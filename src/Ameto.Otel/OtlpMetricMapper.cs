using Ameto.Metrics;
using Ameto.Otel.Models;

namespace Ameto.Otel;

/// <summary>
/// Maps an OTLP <see cref="ExportMetricsServiceRequest"/> (JSON model) to
/// a flat list of <see cref="MetricIngestItem"/> ready for ingestion.
/// </summary>
public static class OtlpMetricMapper
{
    public static List<MetricIngestItem> Map(ExportMetricsServiceRequest request)
    {
        var result = new List<MetricIngestItem>();

        foreach (var rm in request.ResourceMetrics ?? [])
        {
            string? serviceName = ExtractServiceName(rm.Resource?.Attributes);
            foreach (var sm in rm.ScopeMetrics ?? [])
            foreach (var metric in sm.Metrics ?? [])
            {
                if (metric.Name is null) continue;

                if (metric.Gauge is not null)
                    MapNumberPoints(metric.Name, metric.Unit ?? "", MetricKind.Gauge,
                        metric.Gauge.DataPoints, serviceName, result);

                else if (metric.Sum is not null)
                    MapNumberPoints(metric.Name, metric.Unit ?? "",
                        metric.Sum.IsMonotonic ? MetricKind.Counter : MetricKind.Gauge,
                        metric.Sum.DataPoints, serviceName, result);

                else if (metric.Histogram is not null)
                    MapHistogramPoints(metric.Name, metric.Unit ?? "",
                        metric.Histogram.DataPoints, serviceName, result);
            }
        }

        return result;
    }

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

    private static void MapNumberPoints(
        string                      name,
        string                      unit,
        MetricKind                  kind,
        List<OtlpNumberDataPoint>?  points,
        string?                     serviceName,
        List<MetricIngestItem>      result)
    {
        foreach (var dp in points ?? [])
        {
            double value = dp.AsDouble
                ?? (dp.AsInt is not null && long.TryParse(dp.AsInt, out var i) ? (double)i : 0);

            result.Add(new MetricIngestItem
            {
                Name              = name,
                Unit              = unit,
                Kind              = kind,
                Labels            = ExtractLabels(dp.Attributes, serviceName),
                TimestampUnixNano = OtlpTraceMapper.ParseNanoString(dp.TimeUnixNano),
                ScalarValue       = value,
            });
        }
    }

    private static void MapHistogramPoints(
        string                          name,
        string                          unit,
        List<OtlpHistogramDataPoint>?   points,
        string?                         serviceName,
        List<MetricIngestItem>          result)
    {
        foreach (var dp in points ?? [])
        {
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

            result.Add(new MetricIngestItem
            {
                Name              = name,
                Unit              = unit,
                Kind              = MetricKind.Histogram,
                Labels            = ExtractLabels(dp.Attributes, serviceName),
                TimestampUnixNano = OtlpTraceMapper.ParseNanoString(dp.TimeUnixNano),
                HistogramCount    = count,
                HistogramSum      = dp.Sum ?? 0,
                BucketBounds      = bucketBounds,
                BucketCounts      = bucketCounts,
                Exemplars         = exemplars,
            });
        }
    }

    private static LabelSet ExtractLabels(List<OtlpKeyValue>? attrs, string? serviceName = null)
    {
        var capacity = (attrs?.Count ?? 0) + (serviceName is not null ? 1 : 0);
        if (capacity == 0) return LabelSet.Empty;

        // for loop — avoids two LINQ iterator object allocations per data point
        var pairs = new List<KeyValuePair<string, string>>(capacity);
        if (serviceName is not null)
            pairs.Add(new KeyValuePair<string, string>("service.name", serviceName));
        if (attrs is not null)
        for (int i = 0; i < attrs.Count; i++)
        {
            var kv = attrs[i];
            if (kv.Key is null || kv.Value is null) continue;
            var sv = kv.Value.StringValue
                ?? kv.Value.IntValue
                ?? kv.Value.DoubleValue?.ToString(System.Globalization.CultureInfo.InvariantCulture)
                ?? (kv.Value.BoolValue.HasValue ? kv.Value.BoolValue.Value.ToString().ToLowerInvariant() : null);
            if (sv is not null)
                pairs.Add(new KeyValuePair<string, string>(kv.Key, sv));
        }
        return pairs.Count == 0 ? LabelSet.Empty : new LabelSet(pairs);
    }
}
