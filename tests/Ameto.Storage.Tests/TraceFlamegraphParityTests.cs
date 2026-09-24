using System.Text;
using System.Text.Json;
using Ameto.Tracing;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE FLAME GRAPH BUILDER IS THE OLD ONE, BYTE FOR BYTE, over seeded random traces — the fuzz half
/// of the flame graph's parity, beside <c>TraceDetailShapeTests</c>' fixed traces through the live
/// endpoint.
///
/// <para>The reference is the builder as it stood before TS#11 (2525d34), copied here verbatim:
/// two dictionaries, a list per span, LINQ per node. Both trees are serialised under ASP.NET Core's
/// HTTP JSON options — what the endpoint writes them with — and must be the same bytes, or both
/// must be refused by the serialiser (a trace more than 32 levels deep is, today).</para>
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

    private static string Outcome(Func<FlamegraphNode?> build)
    {
        FlamegraphNode? node = build();
        try   { return Encoding.UTF8.GetString(JsonSerializer.SerializeToUtf8Bytes(node, Host)); }
        catch (JsonException) { return "<refused by the serialiser: too deep>"; }
    }

    [Fact]
    public void Every_generated_trace_builds_the_tree_the_old_builder_built()
    {
        const int Traces = 4_000;
        var rng = new Random(20260923);
        int nulls = 0, refused = 0, roundingMattered = 0;

        for (int t = 0; t < Traces; t++)
        {
            var spans = RandomTrace(rng);
            string want = Outcome(() => ReferenceBuild(spans));
            string got  = Outcome(() => TraceQueryEndpointMapper.BuildFlamegraph(spans));
            if (want != got)
                output.WriteLine($"trace {t}: {spans.Count} spans");
            Assert.Equal(want, got);

            if (want == "null") nulls++;
            if (want.StartsWith('<')) refused++;
            if (RoundingMatters(spans)) roundingMattered++;
        }

        output.WriteLine($"{Traces:N0} traces identical: {nulls:N0} with no root, {refused:N0} too deep "
                       + $"for the serialiser, {roundingMattered:N0} where rounded and raw child sums differ");
        Assert.True(nulls > 0 && refused > 0 && roundingMattered > Traces / 4,
            "the generator stopped producing one of the shapes this test exists for");
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
