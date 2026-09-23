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
    /// <summary>One point's labels, interleaved, with their interner ids — see
    /// <see cref="MetricLabelInterner.GetLabelSet"/>. Per <see cref="Map"/> call.</summary>
    private sealed class LabelScratch(MetricLabelInterner interner)
    {
        public readonly MetricLabelInterner Interner = interner;
        public string[] Kv  = new string[32];
        public int[]    Ids = new int[32];
        public int      Used;

        public void Add(string key, int keyId, string value, int valueId)
        {
            if (Used + 2 > Kv.Length)
            {
                Array.Resize(ref Kv,  Kv.Length * 2);
                Array.Resize(ref Ids, Ids.Length * 2);
            }
            Kv[Used] = key;   Ids[Used] = keyId;   Used++;
            Kv[Used] = value; Ids[Used] = valueId; Used++;
        }

        public void Add(string key, string value)
        {
            int keyId   = Interner.Intern(key,   out key);
            int valueId = Interner.Intern(value, out value);
            Add(key, keyId, value, valueId);
        }
    }

    public static List<MetricIngestItem> Map(ExportMetricsServiceRequest request) =>
        Map(request, MetricLabelInterner.Shared);

    public static List<MetricIngestItem> Map(ExportMetricsServiceRequest request, MetricLabelInterner interner)
    {
        var result  = new List<MetricIngestItem>();
        var scratch = new LabelScratch(interner);

        foreach (var rm in request.ResourceMetrics ?? [])
        {
            string? serviceName = ExtractServiceName(rm.Resource?.Attributes);
            if (serviceName is not null) serviceName = interner.Intern(serviceName);
            var resLabels       = ExtractResourceLabels(rm.Resource?.Attributes);
            foreach (var sm in rm.ScopeMetrics ?? [])
            foreach (var metric in sm.Metrics ?? [])
            {
                if (metric.Name is null) continue;
                string name = interner.Intern(metric.Name);
                string unit = interner.Intern(metric.Unit ?? "");

                if (metric.Gauge is not null)
                    MapNumberPoints(name, unit, MetricKind.Gauge,
                        metric.Gauge.DataPoints, serviceName, resLabels, scratch, result);

                else if (metric.Sum is not null)
                    MapNumberPoints(name, unit,
                        metric.Sum.IsMonotonic ? MetricKind.Counter : MetricKind.Gauge,
                        metric.Sum.DataPoints, serviceName, resLabels, scratch, result);

                else if (metric.Histogram is not null)
                    MapHistogramPoints(name, unit,
                        metric.Histogram.DataPoints, serviceName, resLabels, scratch, result);
            }
        }

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
    /// series. Point attributes win on key collision.
    /// </summary>
    private static List<KeyValuePair<string, string>>? ExtractResourceLabels(List<OtlpKeyValue>? attrs)
    {
        if (attrs is null || attrs.Count == 0) return null;

        List<KeyValuePair<string, string>>? result = null;
        for (int i = 0; i < attrs.Count; i++)
        {
            var kv = attrs[i];
            if (kv.Key is null || kv.Value is null) continue;
            if (kv.Key is "service.name" or "service.instance.id") continue;
            if (kv.Key.StartsWith("telemetry.sdk.",    StringComparison.Ordinal) ||
                kv.Key.StartsWith("telemetry.distro.", StringComparison.Ordinal)) continue;
            var sv = FormatLabelValue(kv.Value);
            if (sv is not null)
                (result ??= new List<KeyValuePair<string, string>>(attrs.Count))
                    .Add(new KeyValuePair<string, string>(kv.Key, sv));
        }
        return result;
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

    private static void MapNumberPoints(
        string                                   name,
        string                                   unit,
        MetricKind                               kind,
        List<OtlpNumberDataPoint>?               points,
        string?                                  serviceName,
        List<KeyValuePair<string, string>>?      resLabels,
        LabelScratch                             scratch,
        List<MetricIngestItem>                   result)
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
                Labels            = ExtractLabels(dp.Attributes, serviceName, resLabels, scratch),
                TimestampUnixNano = OtlpTraceMapper.ParseNanoString(dp.TimeUnixNano),
                ScalarValue       = value,
            });
        }
    }

    private static void MapHistogramPoints(
        string                                   name,
        string                                   unit,
        List<OtlpHistogramDataPoint>?            points,
        string?                                  serviceName,
        List<KeyValuePair<string, string>>?      resLabels,
        LabelScratch                             scratch,
        List<MetricIngestItem>                   result)
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
                Labels            = ExtractLabels(dp.Attributes, serviceName, resLabels, scratch),
                TimestampUnixNano = OtlpTraceMapper.ParseNanoString(dp.TimeUnixNano),
                HistogramCount    = count,
                HistogramSum      = dp.Sum ?? 0,
                BucketBounds      = bucketBounds,
                BucketCounts      = bucketCounts,
                Exemplars         = exemplars,
            });
        }
    }

    private static LabelSet ExtractLabels(
        List<OtlpKeyValue>? attrs, string? serviceName,
        List<KeyValuePair<string, string>>? resLabels, LabelScratch scratch)
    {
        scratch.Used = 0;
        if (serviceName is not null)
            scratch.Add("service.name", serviceName);
        if (attrs is not null)
        for (int i = 0; i < attrs.Count; i++)
        {
            var kv = attrs[i];
            if (kv.Key is null || kv.Value is null) continue;
            var sv = FormatLabelValue(kv.Value);
            if (sv is not null)
                scratch.Add(kv.Key, sv);
        }

        // Allow-listed resource labels — point attributes win on key collision.
        if (resLabels is not null)
        {
            for (int i = 0; i < resLabels.Count; i++)
            {
                // Against everything added so far, resource labels included: a resource that
                // repeats a key keeps its first value, as the pair-list shape always did.
                bool exists = false;
                for (int j = 0; j < scratch.Used; j += 2)
                    if (scratch.Kv[j] == resLabels[i].Key) { exists = true; break; }
                if (!exists) scratch.Add(resLabels[i].Key, resLabels[i].Value);
            }
        }

        return scratch.Used == 0
            ? LabelSet.Empty
            : scratch.Interner.GetLabelSet(scratch.Kv.AsSpan(0, scratch.Used), scratch.Ids.AsSpan(0, scratch.Used));
    }
}
