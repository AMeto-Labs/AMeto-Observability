using Ameto.Metrics;
using Ameto.Otel;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// Pins the span-based protobuf metrics parser to the decode-to-DOM-then-map path it
/// replaces: same payload in, identical <see cref="MetricIngestItem"/>s out. Hand-rolled
/// wire parsing earns this — a silently mis-read field would land wrong numbers in
/// storage with nothing to notice it.
/// </summary>
public sealed class OtlpMetricProtoParityTests
{
    private static List<MetricIngestItem> ViaDom(byte[] payload) =>
        OtlpMetricMapper.Map(OtlpProtoDecoder.DecodeMetrics(payload, payload.Length));

    private static List<MetricIngestItem> ViaSpan(byte[] payload) =>
        OtlpMetricProtoParser.Parse(payload);

    [Fact]
    public void MatchesDomPath_OnRealisticBatch()
    {
        byte[] payload = OtlpProtoPayloads.Metrics_Realistic();
        AssertSame(ViaDom(payload), ViaSpan(payload));
    }

    [Fact]
    public void MatchesDomPath_OnEdgeCases()
    {
        // Empty batch, and a batch whose points carry no attributes / no resource at all.
        byte[] empty = OtlpProtoPayloads.EmptyMetrics();
        AssertSame(ViaDom(empty), ViaSpan(empty));

        byte[] bare = OtlpProtoPayloads.BareGauge();
        var dom = ViaDom(bare);
        AssertSame(dom, ViaSpan(bare));
        Assert.Single(dom);
    }

    [Fact]
    public void MatchesDomPath_OnAttributeValueTypes()
    {
        byte[] payload = OtlpProtoPayloads.MixedAttributeTypes();
        var dom  = ViaDom(payload);
        var span = ViaSpan(payload);
        AssertSame(dom, span);

        // Guard the label formatting itself, not just that the two paths agree.
        var labels = span[0].Labels.Pairs.ToDictionary(p => p.Key, p => p.Value);
        Assert.Equal("Etisalat.API", labels["service.name"]);
        Assert.Equal("42",           labels["int.attr"]);
        Assert.Equal("true",         labels["bool.attr"]);
        Assert.Equal("-7",           labels["negative.attr"]);
    }

    [Fact]
    public void MatchesDomPath_OnExemplars()
    {
        byte[] payload = OtlpProtoPayloads.HistogramWithExemplars();
        var dom  = ViaDom(payload);
        var span = ViaSpan(payload);
        AssertSame(dom, span);

        // One exemplar carries a trace id and survives; the other does not and is dropped.
        Assert.Single(span[0].Exemplars!);
        Assert.Equal("0af7651916cd43dd8448eb211c80319c", span[0].Exemplars![0].TraceId);
    }

    private static void AssertSame(List<MetricIngestItem> expected, List<MetricIngestItem> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            var e = expected[i];
            var a = actual[i];
            Assert.Equal(e.Name,              a.Name);
            Assert.Equal(e.Unit,              a.Unit);
            Assert.Equal(e.Kind,              a.Kind);
            Assert.Equal(e.TimestampUnixNano, a.TimestampUnixNano);
            Assert.Equal(e.ScalarValue,       a.ScalarValue);
            Assert.Equal(e.HistogramCount,    a.HistogramCount);
            Assert.Equal(e.HistogramSum,      a.HistogramSum);
            Assert.Equal(e.BucketBounds,      a.BucketBounds);
            Assert.Equal(e.BucketCounts,      a.BucketCounts);
            Assert.Equal(e.Labels, a.Labels);                     // LabelSet equality is order-independent
            Assert.Equal(e.Labels.Pairs.OrderBy(p => p.Key).ToArray(),
                         a.Labels.Pairs.OrderBy(p => p.Key).ToArray());

            if (e.Exemplars is null) { Assert.Null(a.Exemplars); continue; }
            Assert.NotNull(a.Exemplars);
            Assert.Equal(e.Exemplars.Length, a.Exemplars!.Length);
            for (int k = 0; k < e.Exemplars.Length; k++)
            {
                Assert.Equal(e.Exemplars[k].TraceId,           a.Exemplars[k].TraceId);
                Assert.Equal(e.Exemplars[k].SpanId,            a.Exemplars[k].SpanId);
                Assert.Equal(e.Exemplars[k].Value,             a.Exemplars[k].Value);
                Assert.Equal(e.Exemplars[k].TimestampUnixNano, a.Exemplars[k].TimestampUnixNano);
            }
        }
    }
}
