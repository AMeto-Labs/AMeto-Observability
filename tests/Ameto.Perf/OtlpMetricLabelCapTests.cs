using System.Diagnostics;
using Ameto.Metrics;
using Ameto.Otel;
using Google.Protobuf;
using Xunit;
using static Ameto.Perf.OtlpProtoPayloads;

namespace Ameto.Perf;

/// <summary>
/// #132: <see cref="MetricLabelSetBuilder.Add"/> scans the keys already present for every attribute, so
/// a data point was O(n²) in its attribute count — 10 000 attributes took 131 ms, 20 000 took 485 ms —
/// and the point budget, which counts points, let an 8 MiB body of such points through. Now a point of
/// more than <see cref="MetricLabelSetBuilder.MaxLabels"/> labels is refused whole, on both encodings,
/// and nothing past the cap is read or interned.
/// </summary>
public sealed class OtlpMetricLabelCapTests
{
    private const string Wide = "labels.wide", Beside = "labels.beside";

    // ── Payloads ──────────────────────────────────────────────────────────────

    private static byte[] StringAttr(string key, string value) => Msg(c =>
    {
        c.WriteTag(1, WireFormat.WireType.LengthDelimited); c.WriteString(key);
        Nested(c, 2, Msg(v => { v.WriteTag(1, WireFormat.WireType.LengthDelimited); v.WriteString(value); }));
    });

    /// <summary>
    /// A resource of <paramref name="service"/> plus <paramref name="resourceAttrs"/> labels, and under it one
    /// gauge with two points: <see cref="Wide"/> carrying <paramref name="pointAttrs"/> attributes — distinct
    /// keys, or one key repeated when <paramref name="repeatKey"/> — and <see cref="Beside"/> carrying three.
    /// </summary>
    private static byte[] Payload(int pointAttrs, int resourceAttrs = 0, string? service = "svc", bool repeatKey = false)
    {
        byte[]? resource = service is null && resourceAttrs == 0 ? null : Msg(res =>
        {
            if (service is not null) Nested(res, 1, StringAttr("service.name", service));
            for (int i = 0; i < resourceAttrs; i++) Nested(res, 1, StringAttr("r" + i, "rv"));
        });

        byte[] scope = Msg(sm =>
        {
            Nested(sm, 2, Msg(metric =>
            {
                metric.WriteTag(1, WireFormat.WireType.LengthDelimited); metric.WriteString(Wide);
                Nested(metric, 5, Msg(g => Nested(g, 1, Msg(dp =>                 // field 5: gauge
                {
                    dp.WriteTag(3, WireFormat.WireType.Fixed64); dp.WriteFixed64(1_785_300_060_000_000_000UL);
                    dp.WriteTag(4, WireFormat.WireType.Fixed64); dp.WriteDouble(0.5);
                    for (int i = 0; i < pointAttrs; i++)
                        Nested(dp, 7, StringAttr(repeatKey ? "k" : "k" + i, "v" + i));   // field 7: attributes
                }))));
            }));
            Nested(sm, 2, Msg(metric =>
            {
                metric.WriteTag(1, WireFormat.WireType.LengthDelimited); metric.WriteString(Beside);
                Nested(metric, 5, Msg(g => Nested(g, 1, Msg(dp =>
                {
                    dp.WriteTag(3, WireFormat.WireType.Fixed64); dp.WriteFixed64(1_785_300_060_000_000_000UL);
                    dp.WriteTag(4, WireFormat.WireType.Fixed64); dp.WriteDouble(1.5);
                    Nested(dp, 7, StringAttr("a", "1"));
                    Nested(dp, 7, StringAttr("b", "2"));
                    Nested(dp, 7, StringAttr("c", "3"));
                }))));
            }));
        });

        return Msg(c => Nested(c, 1, Msg(rm =>
        {
            if (resource is not null) Nested(rm, 1, resource);
            Nested(rm, 2, scope);
        })));
    }

    // ── The two decodes ───────────────────────────────────────────────────────

    private static MetricLabelInterner Interner() => new(4096, 64);   // never Shared: the tests must not fill it

    private static (List<MetricIngestItem> Items, MetricDecodeRefusals Refused) ViaSpan(byte[] payload)
    {
        var items = OtlpMetricProtoParser.Parse(payload, Interner(), out MetricDecodeRefusals refused);
        return (items, refused);
    }

    private static (List<MetricIngestItem> Items, MetricDecodeRefusals Refused) ViaDom(byte[] payload)
    {
        var request = OtlpProtoDecoder.DecodeMetrics(payload, payload.Length);
        var items   = OtlpMetricMapper.Map(request, Interner(), out MetricDecodeRefusals refused);
        return (items, refused);
    }

    private static IEnumerable<Func<byte[], (List<MetricIngestItem> Items, MetricDecodeRefusals Refused)>> BothPaths()
        => [ViaSpan, ViaDom];

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public void A_point_of_20000_attributes_is_refused_and_the_point_beside_it_kept()
    {
        byte[] payload = Payload(pointAttrs: 20_000);
        foreach (var decode in BothPaths())
        {
            var (items, refused) = decode(payload);

            var kept = Assert.Single(items);
            Assert.Equal(Beside, kept.Name);
            Assert.Equal(4, kept.Labels.Count);                   // a, b, c + service.name
            Assert.Equal(new MetricDecodeRefusals(TooManyBuckets: 0, TooManyLabels: 1), refused);
        }
    }

    [Fact]
    public void The_cap_counts_the_service_name_and_the_resource_labels_too()
    {
        // service.name + 2 resource labels + 253 own = 256: at the cap, taken.
        foreach (var decode in BothPaths())
        {
            var (items, refused) = decode(Payload(pointAttrs: MetricLabelSetBuilder.MaxLabels - 3, resourceAttrs: 2));
            Assert.Equal(2, items.Count);
            Assert.Equal(MetricLabelSetBuilder.MaxLabels, items[0].Labels.Count);
            Assert.Equal(0, refused.Total);
        }

        // One more own attribute: 257, refused — the resource labels it would have merged count.
        foreach (var decode in BothPaths())
        {
            var (items, refused) = decode(Payload(pointAttrs: MetricLabelSetBuilder.MaxLabels - 2, resourceAttrs: 2));
            Assert.Equal(Beside, Assert.Single(items).Name);
            Assert.Equal(1, refused.TooManyLabels);
        }
    }

    [Fact]
    public void A_resource_past_the_cap_refuses_every_point_under_it()
    {
        foreach (var decode in BothPaths())
        {
            var (items, refused) = decode(Payload(pointAttrs: 0, resourceAttrs: MetricLabelSetBuilder.MaxLabels + 1));
            Assert.Empty(items);
            Assert.Equal(2, refused.TooManyLabels);
        }
    }

    /// <summary>
    /// The resource's overflow does not carry over to the next resource: two requests concatenated are
    /// one request with resource_metrics repeated, and only the first resource's points are refused.
    /// </summary>
    [Fact]
    public void A_resource_past_the_cap_refuses_only_its_own_points()
    {
        byte[] both = [.. Payload(pointAttrs: 0, resourceAttrs: MetricLabelSetBuilder.MaxLabels + 1),
                       .. Payload(pointAttrs: 0, resourceAttrs: 2)];
        foreach (var decode in BothPaths())
        {
            var (items, refused) = decode(both);
            Assert.Equal(2, refused.TooManyLabels);   // the first resource's two points
            Assert.Equal(2, items.Count);             // the second's two
        }
    }

    /// <summary>
    /// A resource of exactly 256 labels and a service name can carry no point: the merged set is 257 at
    /// the least. It is refused at <see cref="MetricLabelSetBuilder.BeginPoint"/>, before the point's
    /// attributes are read or its ~32 600-compare merge is paid; one of 255 is taken.
    /// </summary>
    [Fact]
    public void A_resource_whose_labels_with_the_service_name_pass_the_cap_refuses_its_points_up_front()
    {
        foreach (var decode in BothPaths())
        {
            var (items, refused) = decode(Payload(pointAttrs: 0, resourceAttrs: MetricLabelSetBuilder.MaxLabels));
            Assert.Empty(items);
            Assert.Equal(2, refused.TooManyLabels);

            // One fewer: the point with no attributes of its own is taken at exactly the cap; the one
            // beside it, with three, is refused in Build.
            (items, refused) = decode(Payload(pointAttrs: 0, resourceAttrs: MetricLabelSetBuilder.MaxLabels - 1));
            var kept = Assert.Single(items);
            Assert.Equal(Wide, kept.Name);
            Assert.Equal(MetricLabelSetBuilder.MaxLabels, kept.Labels.Count);
            Assert.Equal(1, refused.TooManyLabels);
        }
    }

    [Fact]
    public void A_key_repeated_a_thousand_times_is_one_label_and_the_last_value_wins()
    {
        foreach (var decode in BothPaths())
        {
            var (items, refused) = decode(Payload(pointAttrs: 1000, repeatKey: true));
            Assert.Equal(2, items.Count);
            Assert.Equal(0, refused.Total);
            var labels = items[0].Labels.Pairs.ToDictionary(p => p.Key, p => p.Value);
            Assert.Equal("v999", labels["k"]);
            Assert.Equal(2, labels.Count);                        // k + service.name
        }
    }

    [Fact]
    public void Both_encodings_agree_on_what_is_refused()
    {
        foreach (int attrs in new[] { 0, 1, 254, 255, 256, 257, 1000 })
        {
            byte[] payload = Payload(attrs);
            var span = ViaSpan(payload);
            var dom  = ViaDom(payload);
            Assert.Equal(dom.Refused, span.Refused);
            Assert.Equal(dom.Items.Select(i => i.Name), span.Items.Select(i => i.Name));
            // The boundary itself, at Add: 255 own + service.name is taken, 256 own + service.name refused.
            Assert.Equal(attrs + 1 > MetricLabelSetBuilder.MaxLabels ? 1 : 0, span.Refused.TooManyLabels);
        }
    }

    /// <summary>
    /// The work is bounded by the cap, not by the attribute count: a point of 20 000 attributes costs
    /// about what one of 1 000 costs (both stop at the 257th key; the rest is skipping fields). Before,
    /// the 20 000 one cost ~400× the 1 000 one. A ratio, not a time: the box is shared.
    /// </summary>
    [Fact]
    public void Parsing_a_point_of_20000_attributes_costs_about_what_1000_do()
    {
        byte[] small = Payload(1_000), big = Payload(20_000);
        for (int i = 0; i < 3; i++) { ViaSpan(small); ViaSpan(big); }   // warm

        TimeSpan Time(byte[] p)
        {
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < 10; i++) ViaSpan(p);
            return sw.Elapsed;
        }

        var tSmall = Time(small);
        var tBig   = Time(big);
        // 20 000 attributes are 20× the bytes to skip over, so allow 20× plus noise; the quadratic
        // scan of the old code made this ratio ~400.
        Assert.True(tBig < tSmall * 20 + TimeSpan.FromMilliseconds(200),
            $"20 000 attributes: {tBig.TotalMilliseconds:F1} ms for 10 parses; 1 000: {tSmall.TotalMilliseconds:F1} ms");
    }

    /// <summary>
    /// Nothing past the cap is interned: the refused point's attributes do not become pooled strings, on
    /// either encoding. Asserted on the pool itself — an allocation check alone passed with the stop
    /// removed, since a warm-up over the same payload had pooled every one of the 40 000 strings.
    /// </summary>
    [Fact]
    public void Nothing_past_the_cap_is_interned()
    {
        byte[] payload = Payload(20_000);
        // Into a fresh pool: service.name, svc, the two names, a/b/c/1/2/3 and the first 256 keys and
        // values (the 256th own attribute is the 257th label, interned before Add refuses it): 522
        // strings. Interning past the cap would be ~40 000.
        const int MostPooled = 2 * MetricLabelSetBuilder.MaxLabels + 32;

        var span = new MetricLabelInterner(65_536, 64);
        OtlpMetricProtoParser.Parse(payload, span, out MetricDecodeRefusals refused);
        Assert.Equal(1, refused.TooManyLabels);
        Assert.InRange(span.Strings.ClaimedCount, 1, MostPooled);

        var dom = new MetricLabelInterner(65_536, 64);
        OtlpMetricMapper.Map(OtlpProtoDecoder.DecodeMetrics(payload, payload.Length), dom, out _);
        Assert.InRange(dom.Strings.ClaimedCount, 1, MostPooled);

        // Nor allocated: warmed on a point that stops at the same key, so k256…k19999 are NOT pooled by
        // the warm-up, and a parse that interned them would show up here.
        var interner = new MetricLabelInterner(65_536, 64);
        OtlpMetricProtoParser.Parse(Payload(MetricLabelSetBuilder.MaxLabels + 1), interner, out _);
        long before = GC.GetAllocatedBytesForCurrentThread();
        OtlpMetricProtoParser.Parse(payload, interner, out _);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.True(allocated < 256 * 1024, $"allocated {allocated:N0} B parsing a refused point of 20 000 attributes");
    }
}
