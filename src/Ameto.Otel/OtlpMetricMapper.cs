using Ameto.Metrics;
using Ameto.Otel.Models;

namespace Ameto.Otel;

/// <summary>
/// Maps an OTLP <see cref="ExportMetricsServiceRequest"/> (JSON model) to
/// a flat list of <see cref="MetricIngestItem"/> ready for ingestion.
///
/// <para>The JSON twin of <see cref="OtlpMetricProtoParser"/>, and canonicalised the same way:
/// the deserialiser has already paid for its strings, but what the ingest path KEEPS — names,
/// units, label keys and values, label sets — is swapped for the
/// <see cref="MetricLabelInterner"/>'s instances, so a series fed over JSON holds the same
/// objects a protobuf-fed one does and the per-point copies die young.</para>
/// </summary>
public static class OtlpMetricMapper
{
    public static List<MetricIngestItem> Map(ExportMetricsServiceRequest request) =>
        Map(request, MetricLabelInterner.Shared, out MetricDecodeRefusals _);

    /// <summary>
    /// <see cref="Map(ExportMetricsServiceRequest)"/>, and how many points it refused at decode —
    /// histogram points of more than <see cref="MetricIngestItem.MaxBucketCounts"/> buckets (#126
    /// review NEW-0), points of more than <see cref="MetricLabelSetBuilder.MaxLabels"/> labels (#132)
    /// — the count the receiver reports as rejected.
    /// </summary>
    public static List<MetricIngestItem> Map(ExportMetricsServiceRequest request, out int refused) =>
        Map(request, MetricLabelInterner.Shared, out refused);

    /// <summary><see cref="Map(ExportMetricsServiceRequest, out int)"/>, the refusals by reason.</summary>
    public static List<MetricIngestItem> Map(ExportMetricsServiceRequest request, out MetricDecodeRefusals refused) =>
        Map(request, MetricLabelInterner.Shared, out refused);

    public static List<MetricIngestItem> Map(ExportMetricsServiceRequest request, MetricLabelInterner interner) =>
        Map(request, interner, out MetricDecodeRefusals _);

    /// <summary><see cref="Map(ExportMetricsServiceRequest, MetricLabelInterner, out MetricDecodeRefusals)"/>, the refusals as one count.</summary>
    public static List<MetricIngestItem> Map(ExportMetricsServiceRequest request, MetricLabelInterner interner, out int refused)
    {
        var items = Map(request, interner, out MetricDecodeRefusals byReason);
        refused = byReason.Total;
        return items;
    }

    public static List<MetricIngestItem> Map(ExportMetricsServiceRequest request, MetricLabelInterner interner, out MetricDecodeRefusals refused)
    {
        var result = new List<MetricIngestItem>();
        int tooManyBuckets = 0, tooManyLabels = 0;
        // The label rule is the protobuf parser's too — one builder, so the two encodings cannot
        // disagree on a series' identity.
        var labels = new MetricLabelSetBuilder(interner);

        foreach (var rm in request.ResourceMetrics ?? [])
        {
            labels.BeginResource();
            if (ExtractServiceName(rm.Resource?.Attributes) is { } serviceName)
                labels.SetServiceName(labels.Intern(serviceName));
            AddResourceLabels(rm.Resource?.Attributes, labels);
            foreach (var sm in rm.ScopeMetrics ?? [])
            foreach (var metric in sm.Metrics ?? [])
            {
                if (metric.Name is null) continue;
                string name = interner.Intern(metric.Name);
                string unit = interner.Intern(metric.Unit ?? "");

                if (metric.Gauge is not null)
                    MapNumberPoints(name, unit, MetricKind.Gauge, metric.Gauge.DataPoints, labels, result, ref tooManyLabels);

                else if (metric.Sum is not null)
                    MapNumberPoints(name, unit,
                        metric.Sum.IsMonotonic ? MetricKind.Counter : MetricKind.Gauge,
                        metric.Sum.DataPoints, labels, result, ref tooManyLabels);

                else if (metric.Histogram is not null)
                    MapHistogramPoints(name, unit, metric.Histogram.DataPoints, labels, result, ref tooManyBuckets, ref tooManyLabels);
            }
        }

        refused = new MetricDecodeRefusals(tooManyBuckets, tooManyLabels);
        return result;
    }

    /// <summary>
    /// Turns the batch's resource attributes (env, deployment id, …) into label
    /// pairs stamped onto every data point — same behaviour as logs and traces:
    /// whatever the sender chose to put on its resource travels with the data.
    /// Excluded: <c>service.name</c> (already the dedicated label), the
    /// <c>telemetry.sdk.*</c>/<c>telemetry.distro.*</c> self-description the OTel
    /// SDK injects on its own — the sender never asked for those, and an SDK
    /// upgrade would fork every series — and <c>service.instance.id</c>: the SDK
    /// mints a fresh GUID per process start, so keeping it as a label forks every
    /// series of every metric on every restart (the dominant cardinality driver).
    /// Logs and traces keep it — there it annotates records instead of multiplying
    /// series. Point attributes win on key collision (<see cref="MetricLabelSetBuilder.Build"/>).
    /// </summary>
    private static void AddResourceLabels(List<OtlpKeyValue>? attrs, MetricLabelSetBuilder labels)
    {
        if (attrs is null) return;

        // Past MaxLabels the resource is done for: every point under it is refused (#132), so nothing
        // more of it is kept or interned.
        for (int i = 0; i < attrs.Count && !labels.ResourceOverflowed; i++)
        {
            var kv = attrs[i];
            if (kv.Key is null || kv.Value is null) continue;
            if (kv.Key is "service.name" or "service.instance.id") continue;
            if (kv.Key.StartsWith("telemetry.sdk.",    StringComparison.Ordinal) ||
                kv.Key.StartsWith("telemetry.distro.", StringComparison.Ordinal)) continue;
            var sv = FormatLabelValue(kv.Value);
            if (sv is not null)
                labels.AddResourceLabel(labels.Intern(kv.Key), labels.Intern(sv));
        }
    }

    private static string? FormatLabelValue(OtlpAnyValue v) =>
        v.StringValue
        ?? v.IntValue
        ?? v.DoubleValue?.ToString(System.Globalization.CultureInfo.InvariantCulture)
        ?? (v.BoolValue.HasValue ? v.BoolValue.Value.ToString().ToLowerInvariant() : null);

    private static string? ExtractServiceName(List<OtlpKeyValue>? attrs)
    {
        if (attrs is null) return null;
        for (int i = 0; i < attrs.Count; i++)
        {
            var kv = attrs[i];
            if (kv.Key == "service.name" && kv.Value?.StringValue is { } sv)
                return sv;
        }
        return null;
    }

    /// <param name="tooManyLabels">Counts the points refused for more than <see cref="MetricLabelSetBuilder.MaxLabels"/> labels (#132).</param>
    private static void MapNumberPoints(
        string                                   name,
        string                                   unit,
        MetricKind                               kind,
        List<OtlpNumberDataPoint>?               points,
        MetricLabelSetBuilder                    labels,
        List<MetricIngestItem>                   result,
        ref int                                  tooManyLabels)
    {
        foreach (var dp in points ?? [])
        {
            double value = dp.AsDouble
                ?? (dp.AsInt is not null && long.TryParse(dp.AsInt, out var i) ? (double)i : 0);

            var labelSet = ExtractLabels(dp.Attributes, labels);
            if (labels.Overflowed)
            {
                tooManyLabels++;   // refused, not built: the protobuf parser's rule (#132)
                continue;
            }

            result.Add(new MetricIngestItem
            {
                Name              = name,
                Unit              = unit,
                Kind              = kind,
                Labels            = labelSet,
                TimestampUnixNano = OtlpTraceMapper.ParseNanoString(dp.TimeUnixNano),
                ScalarValue       = value,
            });
        }
    }

    /// <param name="tooManyBuckets">Counts the points refused for more than <see cref="MetricIngestItem.MaxBucketCounts"/> buckets.</param>
    /// <param name="tooManyLabels">Counts the points refused for more than <see cref="MetricLabelSetBuilder.MaxLabels"/> labels (#132).</param>
    private static void MapHistogramPoints(
        string                                   name,
        string                                   unit,
        List<OtlpHistogramDataPoint>?            points,
        MetricLabelSetBuilder                    labels,
        List<MetricIngestItem>                   result,
        ref int                                  tooManyBuckets,
        ref int                                  tooManyLabels)
    {
        foreach (var dp in points ?? [])
        {
            // Refused, not built: the protobuf parser's rule (#126 review NEW-0).
            if (dp.BucketCounts is { Count: > MetricIngestItem.MaxBucketCounts })
            {
                tooManyBuckets++;
                continue;
            }

            long count = dp.Count is not null && long.TryParse(dp.Count, out var c) ? c : 0;

            // for loops — no LINQ iterator allocations for bucket data
            long[]? bucketCounts = null;
            if (dp.BucketCounts is { Count: > 0 } bcs)
            {
                bucketCounts = new long[bcs.Count];
                for (int i = 0; i < bcs.Count; i++)
                    bucketCounts[i] = long.TryParse(bcs[i], out var bc) ? bc : 0L;
            }

            double[]? bucketBounds = null;
            if (dp.ExplicitBounds is { Count: > 0 } eb)
            {
                bucketBounds = new double[eb.Count];
                for (int i = 0; i < eb.Count; i++)
                    bucketBounds[i] = eb[i];
            }

            MetricExemplar[]? exemplars = null;
            if (dp.Exemplars is { Count: > 0 } exs)
            {
                var list = new List<MetricExemplar>(exs.Count);
                for (int i = 0; i < exs.Count; i++)
                {
                    var e = exs[i];
                    if (string.IsNullOrEmpty(e.TraceId)) continue; // only exemplars with a trace link are useful
                    double v = e.AsDouble
                        ?? (e.AsInt is not null && long.TryParse(e.AsInt, out var iv) ? iv : 0);
                    list.Add(new MetricExemplar
                    {
                        TimestampUnixNano = OtlpTraceMapper.ParseNanoString(e.TimeUnixNano),
                        Value             = v,
                        TraceId           = e.TraceId!,
                        SpanId            = e.SpanId ?? string.Empty,
                    });
                }
                if (list.Count > 0) exemplars = list.ToArray();
            }

            var labelSet = ExtractLabels(dp.Attributes, labels);
            if (labels.Overflowed)
            {
                tooManyLabels++;   // refused, not built: the protobuf parser's rule (#132)
                continue;
            }

            result.Add(new MetricIngestItem
            {
                Name              = name,
                Unit              = unit,
                Kind              = MetricKind.Histogram,
                Labels            = labelSet,
                TimestampUnixNano = OtlpTraceMapper.ParseNanoString(dp.TimeUnixNano),
                HistogramCount    = count,
                HistogramSum      = dp.Sum ?? 0,
                BucketBounds      = bucketBounds,
                BucketCounts      = bucketCounts,
                Exemplars         = exemplars,
            });
        }
    }

    /// <summary>
    /// The point's label set through the builder the protobuf parser uses too. Stops at the attribute
    /// that takes the point past <see cref="MetricLabelSetBuilder.MaxLabels"/>: the point is refused
    /// (<see cref="MetricLabelSetBuilder.Overflowed"/>, the caller's to count) and the rest of its
    /// attributes are not interned (#132).
    /// </summary>
    private static LabelSet ExtractLabels(List<OtlpKeyValue>? attrs, MetricLabelSetBuilder labels)
    {
        labels.BeginPoint();
        if (attrs is not null)
        for (int i = 0; i < attrs.Count && !labels.Overflowed; i++)
        {
            var kv = attrs[i];
            if (kv.Key is null || kv.Value is null) continue;
            var sv = FormatLabelValue(kv.Value);
            if (sv is not null)
                labels.Add(labels.Intern(kv.Key), labels.Intern(sv));
        }

        return labels.Build();
    }
}
