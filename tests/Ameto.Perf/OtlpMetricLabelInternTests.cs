using Ameto.Metrics;
using Ameto.Otel;
using Google.Protobuf;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// The protobuf metrics parser resolves label text through a <see cref="MetricLabelInterner"/>:
/// a series re-sent in the next export gets the SAME strings and the same label set, and an
/// interner past its bounds degrades to fresh strings without costing a point or a character.
/// Each fact uses its own interner, so nothing here depends on what the rest of the assembly
/// has already put into <see cref="MetricLabelInterner.Shared"/>.
/// </summary>
public sealed class OtlpMetricLabelInternTests
{
    [Fact]
    public void A_series_sent_twice_is_decoded_into_the_same_strings_and_label_set()
    {
        byte[] payload  = OtlpProtoPayloads.Metrics_Realistic();
        var    interner = new MetricLabelInterner(MetricLabelInterner.DefaultMaxStrings,
                                                  MetricLabelInterner.DefaultLabelSetSlots);

        var first  = OtlpMetricProtoParser.Parse(payload, interner);
        var second = OtlpMetricProtoParser.Parse(payload, interner);   // the next export interval

        Assert.Equal(first.Count, second.Count);
        for (int i = 0; i < first.Count; i++)
        {
            Assert.Same(first[i].Name,   second[i].Name);
            Assert.Same(first[i].Unit,   second[i].Unit);
            Assert.Same(first[i].Labels, second[i].Labels);
        }

        // And within one export, two series of the same instrument share every key instance.
        var a = first[0].Labels;
        var b = first[1].Labels;
        Assert.NotEqual(a, b);
        Assert.Equal(a.Count, b.Count);
        for (int k = 0; k < a.Count; k++) Assert.Same(a.KeyAt(k), b.KeyAt(k));
    }

    [Fact]
    public void Past_the_interners_bounds_every_point_still_arrives_whole()
    {
        byte[] payload   = OtlpProtoPayloads.Metrics_Realistic();
        var    tiny      = new MetricLabelInterner(maxStrings: 4, labelSetSlots: 2);
        int    exhausted = 0;
        tiny.Strings.PoolExhausted += _ => exhausted++;

        var expected = OtlpMetricMapper.Map(OtlpProtoDecoder.DecodeMetrics(payload, payload.Length),
                                            new MetricLabelInterner(1024, 64));
        var actual   = OtlpMetricProtoParser.Parse(payload, tiny);

        Assert.Equal(1, exhausted);
        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            var e = expected[i];
            var g = actual[i];
            Assert.Equal(e.Name,              g.Name);
            Assert.Equal(e.Unit,              g.Unit);
            Assert.Equal(e.Kind,              g.Kind);
            Assert.Equal(e.TimestampUnixNano, g.TimestampUnixNano);
            Assert.Equal(e.ScalarValue,       g.ScalarValue);
            Assert.Equal(e.BucketCounts,      g.BucketCounts);
            Assert.Equal(e.BucketBounds,      g.BucketBounds);
            Assert.Equal(e.Labels.Pairs.ToArray(), g.Labels.Pairs.ToArray());
            Assert.Equal(e.Labels,            g.Labels);
            foreach (var (k, v) in g.Labels)
            {
                Assert.False(string.IsNullOrEmpty(k));
                Assert.False(string.IsNullOrEmpty(v));
            }
        }
    }

    [Fact]
    public void Histogram_points_of_one_instrument_share_their_bounds_only_when_they_are_bit_equal()
    {
        byte[] payload = OtlpProtoPayloads.Metrics_Realistic();
        var    items   = OtlpMetricProtoParser.Parse(payload, new MetricLabelInterner(1024, 64));

        double[]? previous = null;
        int shared = 0;
        foreach (var item in items)
        {
            if (item.Kind != MetricKind.Histogram) { previous = null; continue; }
            Assert.NotNull(item.BucketBounds);
            if (previous is not null)
            {
                Assert.Same(previous, item.BucketBounds);
                shared++;
            }
            previous = item.BucketBounds;
        }
        Assert.True(shared > 0);

        // 0.0 and -0.0 are Equal as doubles and different as bits — and the stored bounds are
        // bits (the WAL pool, the .mts). A point must keep its own.
        byte[] signed = OtlpProtoPayloads.Msg(req => OtlpProtoPayloads.Nested(req, 1, OtlpProtoPayloads.Msg(rm =>
            OtlpProtoPayloads.Nested(rm, 2, OtlpProtoPayloads.Msg(sm =>
                OtlpProtoPayloads.Nested(sm, 2, OtlpProtoPayloads.Msg(m =>
                {
                    m.WriteTag(1, WireFormat.WireType.LengthDelimited);
                    m.WriteString("signed.zero");
                    OtlpProtoPayloads.Nested(m, 9, OtlpProtoPayloads.Msg(h =>
                    {
                        OtlpProtoPayloads.Nested(h, 1, HistogramPoint(0.0));
                        OtlpProtoPayloads.Nested(h, 1, HistogramPoint(-0.0));
                        OtlpProtoPayloads.Nested(h, 1, HistogramPoint(-0.0));
                    }));
                })))))));

        var z = OtlpMetricProtoParser.Parse(signed, new MetricLabelInterner(1024, 64));
        Assert.Equal(3, z.Count);
        Assert.NotSame(z[0].BucketBounds, z[1].BucketBounds);
        Assert.Same(z[1].BucketBounds, z[2].BucketBounds);
        Assert.False(double.IsNegative(z[0].BucketBounds![0]));
        Assert.True(double.IsNegative(z[1].BucketBounds![0]));
    }

    private static byte[] HistogramPoint(double firstBound) => OtlpProtoPayloads.Msg(c =>
    {
        c.WriteTag(3, WireFormat.WireType.Fixed64); c.WriteFixed64(1_785_300_060_000_000_000UL);
        c.WriteTag(4, WireFormat.WireType.Fixed64); c.WriteFixed64(1);
        byte[] bounds = OtlpProtoPayloads.Msg(b => { b.WriteDouble(firstBound); b.WriteDouble(1.0); });
        c.WriteTag(7, WireFormat.WireType.LengthDelimited); c.WriteBytes(ByteString.CopyFrom(bounds));
    });
}
