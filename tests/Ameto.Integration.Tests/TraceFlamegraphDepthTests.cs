using System.Globalization;
using System.Net;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Integration.Tests;

/// <summary>
/// ISSUE #91: THE FLAME GRAPH OF A TRACE DEEPER THAN 32 LEVELS WAS A 500. The endpoint handed a
/// <c>FlamegraphNode</c> tree to the host's serialiser, whose MaxDepth of 64 JSON levels is 32 tree
/// levels (a node object plus its <c>children</c> array), and the 33rd level threw. Deep call
/// graphs are ordinary — recursion, middleware chains, retries inside retries — so these chains
/// go through the live endpoint and must come back 200 with every level nested where it belongs.
///
/// <para>Past <see cref="TraceFlamegraphJson.MaxLevels"/> the tree is cut, not refused: the node on
/// the last level says <c>"truncated":true</c> over an empty <c>children</c> array.</para>
/// </summary>
public sealed class TraceFlamegraphDepthTests : IClassFixture<AmetoWebAppFactory>
{
    private const long Ms = 1_000_000L;

    /// <summary>2100-01-01T00:05:00Z — clear of every other suite's windows, and of retention.</summary>
    private const long Anchor = 4_102_445_100_000_000_000L;

    private readonly HttpClient         _client;
    private readonly TraceStorageEngine _traces;

    public TraceFlamegraphDepthTests(AmetoWebAppFactory factory)
    {
        _client = factory.CreateClient();
        _traces = factory.Services.GetRequiredService<TraceStorageEngine>();
    }

    /// <summary>A chain of <paramref name="levels"/> spans: level L is span id L, child of L - 1.</summary>
    private TraceId WriteChain(ulong traceLow, int levels)
    {
        var trace = new TraceId(0xF1A3E0DE00000000UL, traceLow);
        long a = Anchor + (long)traceLow * 60_000 * Ms;
        for (int level = 1; level <= levels; level++)
            _traces.WriteSpan(new SpanIngestItem
            {
                TraceId           = trace,
                SpanId            = new SpanId((ulong)level),
                ParentSpanId      = level == 1 ? default : new SpanId((ulong)(level - 1)),
                StartTimeUnixNano = a + level * 1_000L,
                DurationNanos     = (levels - level + 1) * Ms,   // each level 1 ms longer than its child
                Name              = "lvl-" + level.ToString(CultureInfo.InvariantCulture),
                ServiceName       = level % 2 == 0 ? "сервис" : "svc",
                Kind              = SpanKind.Internal,
                Status            = SpanStatusCode.Ok,
            });
        return trace;
    }

    private async Task<JsonDocument> GetFlamegraphAsync(TraceId trace)
    {
        using var resp = await _client.GetAsync($"/api/traces/{trace}/flamegraph");
        byte[] body = await resp.Content.ReadAsByteArrayAsync();
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Equal("application/json; charset=utf-8", resp.Content.Headers.ContentType?.ToString());
        return JsonDocument.Parse(body, new JsonDocumentOptions { MaxDepth = 2 * TraceFlamegraphJson.MaxLevels + 2 });
    }

    /// <summary>
    /// Walks the chain level by level — iteratively, so the check itself has no depth of its own —
    /// and asserts every level's id, name, one child, rounded totals and the leaf at the bottom.
    /// </summary>
    private static void AssertChain(JsonElement root, int levels)
    {
        var node = root;
        for (int level = 1; level <= levels; level++)
        {
            Assert.Equal(((ulong)level).ToString("x16", CultureInfo.InvariantCulture), node.GetProperty("spanId").GetString());
            Assert.Equal("lvl-" + level.ToString(CultureInfo.InvariantCulture), node.GetProperty("name").GetString());
            Assert.Equal(levels - level + 1, node.GetProperty("totalMs").GetDouble());
            Assert.Equal(1, node.GetProperty("selfMs").GetDouble());   // every level 1 ms longer than its child
            Assert.False(node.TryGetProperty("truncated", out _), $"level {level} of {levels} is flagged as cut");

            var children = node.GetProperty("children");
            if (level == levels) { Assert.Equal(0, children.GetArrayLength()); break; }
            Assert.Equal(1, children.GetArrayLength());
            node = children[0];
        }
    }

    [Theory]
    [InlineData(1UL, 33)]    // one past what the serialiser answered
    [InlineData(2UL, 40)]
    [InlineData(3UL, 500)]
    public async Task A_trace_deeper_than_32_levels_draws_every_level(ulong traceLow, int levels)
    {
        var trace = WriteChain(traceLow, levels);

        using (var hot = await GetFlamegraphAsync(trace))
            AssertChain(hot.RootElement, levels);

        _traces.FlushHotTier();
        using var cold = await GetFlamegraphAsync(trace);
        AssertChain(cold.RootElement, levels);
    }

    [Fact]
    public async Task A_trace_past_the_level_limit_is_cut_and_flagged_not_refused()
    {
        const int Max    = TraceFlamegraphJson.MaxLevels;
        const int Levels = Max + 10;
        var trace = WriteChain(4UL, Levels);

        using var doc = await GetFlamegraphAsync(trace);
        var node = doc.RootElement;
        for (int level = 1; level < Max; level++)
        {
            Assert.False(node.TryGetProperty("truncated", out _), $"level {level} is flagged as cut");
            node = node.GetProperty("children")[0];
        }

        // The last level: drawn whole — its totals still count the child that is not drawn — and
        // flagged; nothing of the ten levels below it is on the wire.
        Assert.Equal("lvl-" + Max.ToString(CultureInfo.InvariantCulture), node.GetProperty("name").GetString());
        Assert.Equal(Levels - Max + 1, node.GetProperty("totalMs").GetDouble());
        Assert.Equal(1, node.GetProperty("selfMs").GetDouble());
        Assert.Equal(0, node.GetProperty("children").GetArrayLength());
        Assert.True(node.GetProperty("truncated").GetBoolean());
    }
}
