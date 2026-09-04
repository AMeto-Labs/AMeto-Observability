using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Integration.Tests;

/// <summary>
/// <c>GET /api/traces/index</c> — the numbers nobody had.
///
/// <para>A trace lookup used to consult every cold segment, and inflate every one of their trace
/// indexes to do it. That went unnoticed for as long as it did because the segment count and the
/// weight of those indexes were invisible from outside the process: there was no way to ask an
/// install how wide its fan-out was. This endpoint is the answer to that, and it is also how an
/// operator watches the backfill finish.</para>
///
/// <para>The route is the other thing under test, and it is not a formality: it sits directly
/// under <c>/api/traces/{traceId}</c>, so if literal segments did not outrank parameters this
/// would be parsed as a trace id called "index" and answer 400.</para>
/// </summary>
public sealed class TraceIndexEndpointTests : IClassFixture<AmetoWebAppFactory>
{
    private const long Ms = 1_000_000L;

    private readonly AmetoWebAppFactory _factory;
    private readonly HttpClient         _client;

    public TraceIndexEndpointTests(AmetoWebAppFactory factory)
    {
        _factory = factory;
        _client  = factory.CreateClient();
    }

    private TraceStorageEngine Engine => _factory.Services.GetRequiredService<TraceStorageEngine>();

    private static void WriteSpan(TraceStorageEngine e, ulong trace, ulong span, long startNano)
        => e.WriteSpan(new SpanIngestItem
        {
            TraceId = new TraceId(0xC0FFEE, trace), SpanId = new SpanId(span), ParentSpanId = default,
            StartTimeUnixNano = startNano, DurationNanos = 2 * Ms,
            Name = "GET /orders", ServiceName = "billing",
            Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
        });

    [Fact]
    public async Task It_reports_the_fan_out_width_and_what_the_index_costs()
    {
        var engine = Engine;
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * Ms;

        var before = await _client.GetFromJsonAsync<JsonElement>("/api/traces/index");
        int segsBefore = before.GetProperty("coldSegments").GetInt32();

        for (int s = 0; s < 3; s++)
        {
            for (int t = 0; t < 25; t++)
                for (int k = 0; k < 2; k++)
                    WriteSpan(engine, (ulong)(s * 100 + t), (ulong)(s * 1000 + t * 2 + k),
                              now + (s * 10_000 + t * 10 + k) * Ms);
            engine.FlushHotTier();
        }

        var res = await _client.GetAsync("/api/traces/index");
        Assert.Equal(HttpStatusCode.OK, res.StatusCode);

        var body = await res.Content.ReadFromJsonAsync<JsonElement>();
        int  segments = body.GetProperty("coldSegments").GetInt32();
        int  covered  = body.GetProperty("coveredSegments").GetInt32();
        long onDisk   = body.GetProperty("indexBytesOnDisk").GetInt64();
        long inMemory = body.GetProperty("indexBytesInMemory").GetInt64();
        int  runs     = body.GetProperty("openRuns").GetInt32();

        Assert.Equal(segsBefore + 3, segments);

        // Every segment written since the index existed is covered by the flush that wrote it,
        // so "covered == segments" is what a healthy install looks like once migration is done.
        Assert.Equal(segments, covered);
        Assert.Equal(segments, runs);
        Assert.True(onDisk   > 0, "the runs weigh nothing on disk");
        Assert.True(inMemory > 0, "the runs are holding nothing in memory");

        // The number that decides when per-segment runs stop scaling: RAM per segment. It has to
        // be small enough that hundreds of segments are unremarkable.
        long perSegment = inMemory / Math.Max(1, segments);
        Assert.True(perSegment < 256 * 1024,
            $"each run holds {perSegment} bytes in RAM — per-segment runs will not scale");
    }

    [Fact]
    public async Task The_route_is_not_swallowed_by_the_trace_id_route_beside_it()
    {
        // "index" is a legal path segment and an illegal trace id. If routing preferred the
        // parameter this would be a 400 from TryParseHex instead of a report.
        var res = await _client.GetAsync("/api/traces/index");
        Assert.Equal(HttpStatusCode.OK, res.StatusCode);
        Assert.Contains("coldSegments", await res.Content.ReadAsStringAsync());

        // And the neighbour still behaves: a genuinely malformed id is still refused.
        var bad = await _client.GetAsync("/api/traces/not-a-trace-id");
        Assert.Equal(HttpStatusCode.BadRequest, bad.StatusCode);
    }
}
