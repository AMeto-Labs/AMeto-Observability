using System.Buffers;
using System.Text;
using System.Text.Json;
using Ameto.Tracing;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE FLAME GRAPH WRITER WRITES WHAT THE OLD BUILDER AND SERIALISER WROTE, BYTE FOR BYTE, over
/// seeded random traces — the fuzz half of the flame graph's parity, beside
/// <c>TraceDetailShapeTests</c>' fixed traces through the live endpoint.
///
/// <para>The reference is the builder as it stood before TS#11 (2525d34), copied here verbatim:
/// two dictionaries, a list per span, LINQ per node, its tree serialised under ASP.NET Core's HTTP
/// JSON options — what the endpoint used to answer with. The writer (<c>TraceFlamegraphJson</c>,
/// issue #91) must produce the same bytes. A trace more than 32 levels deep, which that serialiser
/// REFUSED (a 500), is held to the same reference serialised with the depth limit lifted: the
/// bytes the old code would have written had it been allowed to.</para>
///
/// <para>The generator draws the shapes the provider can actually hand over, which is to say
/// span ids unique except the EMPTY id (the provider dedupes the rest) — and inside that: several
/// root candidates, orphans before and after the real root, spans naming a parent that comes later
/// in the list, self-parents and two-span cycles no root reaches, empty-id spans with children
/// pointing at nothing, undefined kinds and statuses, zero and negative durations — and durations
/// with sub-microsecond parts, so that summing the children's ROUNDED totals and summing their raw
/// ones give different <c>selfMs</c>.</para>
/// </summary>
public sealed class TraceFlamegraphParityTests(ITestOutputHelper output)
{
    private static readonly JsonSerializerOptions Host =
        new Microsoft.AspNetCore.Http.Json.JsonOptions().SerializerOptions;

    /// <summary><see cref="Host"/> with room for every level the writer writes — see <see cref="Reference"/>.</summary>
    private static readonly JsonSerializerOptions HostUnbounded =
        new(Host) { MaxDepth = 2 * TraceFlamegraphJson.MaxLevels + 2 };

    // ── The reference: TraceQueryEndpointMapper.BuildFlamegraph at 2525d34 ──

    private static FlamegraphNode? ReferenceBuild(List<SpanRecord> spans)
    {
        var byId       = new Dictionary<SpanId, SpanRecord>(spans.Count);
        var children   = new Dictionary<SpanId, List<SpanRecord>>(spans.Count);

        foreach (var s in spans)
        {
            byId[s.SpanId] = s;
            if (!children.ContainsKey(s.SpanId)) children[s.SpanId] = [];
        }

        SpanRecord? root = null;
        foreach (var s in spans)
        {
            if (s.ParentSpanId.IsEmpty || !byId.ContainsKey(s.ParentSpanId))
            { root = s; continue; }
            children[s.ParentSpanId].Add(s);
        }

        return root is null ? null : ReferenceNode(root, children);
    }

    private static FlamegraphNode ReferenceNode(SpanRecord span, Dictionary<SpanId, List<SpanRecord>> childMap)
    {
        var kids     = childMap.TryGetValue(span.SpanId, out var c) ? c : [];
        var kidNodes = kids.Select(k => ReferenceNode(k, childMap)).ToArray();

        double totalMs = span.DurationNanos / 1_000_000.0;
        double childMs = kidNodes.Sum(n => n.TotalMs);
        double selfMs  = Math.Max(0, totalMs - childMs);

        return new FlamegraphNode
        {
            SpanId   = span.SpanId.ToString(),
            Name     = span.Name,
            Service  = span.ServiceName,
            Kind     = span.Kind.ToString(),
            Status   = span.Status.ToString(),
            TotalMs  = Math.Round(totalMs, 3),
            SelfMs   = Math.Round(selfMs,  3),
            Children = kidNodes,
        };
    }

    // ── The generator ────────────────────────────────────────────────────────

    private static List<SpanRecord> RandomTrace(Random rng)
    {
        bool chain = rng.Next(15) == 0;                                         // deep: past the serialiser's MaxDepth
        int  n     = chain ? rng.Next(20, 60) : rng.Next(1, rng.Next(10) == 0 ? 400 : 40);

        // Unique non-empty ids, some empty ones.
        var ids = new ulong[n];
        var used = new HashSet<ulong>();
        for (int i = 0; i < n; i++)
        {
            if (!chain && rng.Next(12) == 0) { ids[i] = 0; continue; }
            ulong id;
            do { id = (ulong)rng.NextInt64(1, 1 << 20); } while (!used.Add(id));
            ids[i] = id;
        }

        var spans = new List<SpanRecord>(n);
        for (int i = 0; i < n; i++)
        {
            int roll = rng.Next(100);
            ulong parent =
                i == 0 && chain ? 0
              : roll < 6  ? 0                                                    // a root candidate
              : roll < 10 ? (ulong)rng.NextInt64(1 << 21, 1 << 22)              // names nobody: an orphan
              : roll < 12 ? ids[i]                                              // itself
              : chain     ? ids[i - 1]
              : roll < 22 ? ids[rng.Next(n)]                                    // anyone, later ones too
              : i == 0    ? 0
              :             ids[rng.Next(Math.Max(0, i - 6), i)];               // a recent earlier span: depth
            spans.Add(new SpanRecord
            {
                TraceId           = new TraceId(1, 2),
                SpanId            = new SpanId(ids[i]),
                ParentSpanId      = new SpanId(parent),
                StartTimeUnixNano = i,
                DurationNanos     = rng.Next(20) == 0 ? -rng.Next(0, 3_000_000)
                                  : rng.Next(20) == 0 ? 0
                                  : rng.NextInt64(0, 50_000_000) + rng.Next(0, 1000),
                Name              = "op" + i,
                ServiceName       = rng.Next(2) == 0 ? "front" : "сервис <&>",
                Kind              = (SpanKind)rng.Next(0, 8),
                Status            = (SpanStatusCode)rng.Next(0, 4),
            });
        }
        return spans;
    }

    /// <summary>
    /// The reference tree, serialised by the host's options with the depth limit LIFTED — what the
    /// old serialiser would have written for a deep tree had it been allowed to, and so what the
    /// writer must write. For every tree the host's own limit admits, the two serialisations are
    /// asserted to be the same bytes: that is the claim "nothing that worked before changed".
    /// </summary>
    private static string Reference(List<SpanRecord> spans, out bool refusedBefore)
    {
        FlamegraphNode? node = ReferenceBuild(spans);
        string unbounded = Encoding.UTF8.GetString(JsonSerializer.SerializeToUtf8Bytes(node, HostUnbounded));
        try
        {
            Assert.Equal(unbounded, Encoding.UTF8.GetString(JsonSerializer.SerializeToUtf8Bytes(node, Host)));
            refusedBefore = false;
        }
        catch (JsonException) { refusedBefore = true; }   // deeper than 32 levels: a 500 before #91
        return unbounded;
    }

    /// <summary>What the endpoint writes, through the writer settings it uses.</summary>
    private static string Written(List<SpanRecord> spans)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var w = new Utf8JsonWriter(buffer, TraceFlamegraphJson.WithRoom(TraceDetailJson.WriterOptions(Host))))
            TraceFlamegraphJson.Write(w, spans);
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    [Fact]
    public void Every_generated_trace_is_written_as_the_old_builder_and_serialiser_wrote_it()
    {
        const int Traces = 4_000;
        var rng = new Random(20260923);
        int nulls = 0, refusedBefore = 0, roundingMattered = 0;

        for (int t = 0; t < Traces; t++)
        {
            var spans = RandomTrace(rng);
            string want = Reference(spans, out bool refused);
            string got  = Written(spans);
            if (want != got)
                output.WriteLine($"trace {t}: {spans.Count} spans");
            Assert.Equal(want, got);

            if (want == "null") nulls++;
            if (refused) refusedBefore++;
            if (RoundingMatters(spans)) roundingMattered++;
        }

        output.WriteLine($"{Traces:N0} traces identical: {nulls:N0} with no root, {refusedBefore:N0} too deep "
                       + $"for the old serialiser (a 500 before #91, now written), {roundingMattered:N0} where "
                       + "rounded and raw child sums differ");
        Assert.True(nulls > 0 && refusedBefore > 0 && roundingMattered > Traces / 4,
            "the generator stopped producing one of the shapes this test exists for");
    }

    // ── The cut ──────────────────────────────────────────────────────────────

    private static SpanRecord Span(ulong id, ulong parent, long start, long durNanos, string name) => new()
    {
        TraceId           = new TraceId(1, 2),
        SpanId            = new SpanId(id),
        ParentSpanId      = new SpanId(parent),
        StartTimeUnixNano = start,
        DurationNanos     = durNanos,
        Name              = name,
        ServiceName       = "s",
        Kind              = SpanKind.Internal,
        Status            = SpanStatusCode.Ok,
    };

    /// <summary>
    /// A chain longer than <see cref="TraceFlamegraphJson.MaxLevels"/> is CUT on the last level —
    /// the node there written whole, its <c>totalMs</c> and <c>selfMs</c> still counting the child
    /// that is not drawn, <c>"children":[]</c> and <c>"truncated":true</c> — and not a byte of the
    /// subtree below it. No other node carries the flag.
    /// </summary>
    [Fact]
    public void A_chain_past_the_level_limit_is_cut_on_the_last_level_and_says_so()
    {
        const int Max = TraceFlamegraphJson.MaxLevels;
        var spans = new List<SpanRecord>(Max + 5);
        for (int level = 1; level <= Max + 5; level++)
            spans.Add(Span((ulong)level, level == 1 ? 0 : (ulong)(level - 1), level, 3_000_000, "l" + level));

        string json = Written(spans);
        using var doc = JsonDocument.Parse(json, new JsonDocumentOptions { MaxDepth = 2 * Max + 2 });

        var node = doc.RootElement;
        for (int level = 1; level < Max; level++)
        {
            Assert.Equal("l" + level, node.GetProperty("name").GetString());
            Assert.False(node.TryGetProperty("truncated", out _), $"level {level} is not the cut");
            Assert.Equal(1, node.GetProperty("children").GetArrayLength());
            node = node.GetProperty("children")[0];
        }

        Assert.Equal("l" + Max, node.GetProperty("name").GetString());
        Assert.Equal(0, node.GetProperty("children").GetArrayLength());
        Assert.True(node.GetProperty("truncated").GetBoolean());
        Assert.Equal(3, node.GetProperty("totalMs").GetDouble());
        Assert.Equal(0, node.GetProperty("selfMs").GetDouble());        // its child still counted
        Assert.Contains(                                                   // the flag's place: after children
            "\"name\":\"l" + Max + "\",\"service\":\"s\",\"kind\":\"Internal\",\"status\":\"Ok\","
          + "\"totalMs\":3,\"selfMs\":0,\"children\":[],\"truncated\":true}]}", json);
        Assert.DoesNotContain("\"l" + (Max + 1) + "\"", json);          // nothing below the cut
        Assert.Equal(1, CountOf(json, "\"truncated\""));
    }

    /// <summary>
    /// A NON-empty span id repeated so that a span becomes its own descendant — which the
    /// provider's dedupe rules out, and which the old recursive builder would have followed until
    /// the stack overflowed and took the process with it. The walk opens no level deeper than
    /// the trace has spans, so it is cut there instead: a bounded answer, flagged.
    /// </summary>
    [Fact]
    public void A_repeated_span_id_that_would_recurse_for_ever_is_cut_not_followed()
    {
        List<SpanRecord> spans =
        [
            Span(1, 0, 1, 10_000_000, "R"),
            Span(2, 1, 2,  5_000_000, "A"),
            Span(2, 2, 3,  1_000_000, "B"),   // shares A's id and names it as parent: its own child
        ];

        Assert.Equal(
            "{\"spanId\":\"0000000000000001\",\"name\":\"R\",\"service\":\"s\",\"kind\":\"Internal\",\"status\":\"Ok\",\"totalMs\":10,\"selfMs\":5,\"children\":["
          + "{\"spanId\":\"0000000000000002\",\"name\":\"A\",\"service\":\"s\",\"kind\":\"Internal\",\"status\":\"Ok\",\"totalMs\":5,\"selfMs\":4,\"children\":["
          + "{\"spanId\":\"0000000000000002\",\"name\":\"B\",\"service\":\"s\",\"kind\":\"Internal\",\"status\":\"Ok\",\"totalMs\":1,\"selfMs\":0,\"children\":[],\"truncated\":true}]}]}",
            Written(spans));
    }

    /// <summary>
    /// THE CUT BOUNDS SIZE AS WELL AS DEPTH. A repeated NON-empty span id with TWO spans naming it
    /// as parent makes every copy the parent of both, so a walk bounded only by depth writes 2^level
    /// nodes — 1 + 1 + 2 + 4 here, 2^4 096 at the level limit. The walk writes at most one node per
    /// span of the trace — no tree the provider can hand over (span ids deduped) has more — and a
    /// node whose remaining children it had no budget for is closed with <c>"truncated":true</c>.
    /// </summary>
    [Fact]
    public void A_repeated_branching_span_id_writes_no_more_nodes_than_the_trace_has_spans()
    {
        List<SpanRecord> spans =
        [
            Span(1, 0, 1, 10_000_000, "R"),
            Span(2, 1, 2,  5_000_000, "A"),
            Span(2, 2, 3,  1_000_000, "B"),   // both name id 2 as parent: every copy of id 2
            Span(2, 2, 4,  1_000_000, "C"),   // has children B and C
        ];

        string json = Written(spans);
        Assert.Equal(spans.Count, CountOf(json, "\"spanId\""));
        Assert.Equal(
            "{\"spanId\":\"0000000000000001\",\"name\":\"R\",\"service\":\"s\",\"kind\":\"Internal\",\"status\":\"Ok\",\"totalMs\":10,\"selfMs\":5,\"children\":["
          + "{\"spanId\":\"0000000000000002\",\"name\":\"A\",\"service\":\"s\",\"kind\":\"Internal\",\"status\":\"Ok\",\"totalMs\":5,\"selfMs\":3,\"children\":["
          + "{\"spanId\":\"0000000000000002\",\"name\":\"B\",\"service\":\"s\",\"kind\":\"Internal\",\"status\":\"Ok\",\"totalMs\":1,\"selfMs\":0,\"children\":["
          + "{\"spanId\":\"0000000000000002\",\"name\":\"B\",\"service\":\"s\",\"kind\":\"Internal\",\"status\":\"Ok\",\"totalMs\":1,\"selfMs\":0,\"children\":[],\"truncated\":true}"
          + "],\"truncated\":true}],\"truncated\":true}]}",
            json);
    }

    private static int CountOf(string haystack, string needle)
    {
        int n = 0;
        for (int i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
            n++;
        return n;
    }

    /// <summary>
    /// True when some node's <c>selfMs</c> differs between summing its children's rounded totals
    /// and summing their raw ones — the case that proves the builder sums what the old one summed.
    /// </summary>
    private static bool RoundingMatters(List<SpanRecord> spans)
    {
        var byParent = spans.Where(s => !s.ParentSpanId.IsEmpty).GroupBy(s => s.ParentSpanId);
        foreach (var g in byParent)
        {
            var parent = spans.LastOrDefault(s => s.SpanId.Equals(g.Key));
            if (parent is null) continue;
            double total   = parent.DurationNanos / 1_000_000.0;
            double rounded = 0, raw = 0;
            foreach (var k in g) { rounded += Math.Round(k.DurationNanos / 1_000_000.0, 3); raw += k.DurationNanos / 1_000_000.0; }
            if (Math.Round(Math.Max(0, total - rounded), 3) != Math.Round(Math.Max(0, total - raw), 3)) return true;
        }
        return false;
    }
}
