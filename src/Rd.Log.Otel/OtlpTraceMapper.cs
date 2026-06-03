using MessagePack;
using System.Buffers;
using Rd.Log.Otel.Models;
using Rd.Log.Tracing;

namespace Rd.Log.Otel;

/// <summary>
/// Maps an OTLP <see cref="ExportTraceServiceRequest"/> (JSON model) to
/// a flat list of <see cref="SpanIngestItem"/> ready for ingestion.
/// </summary>
public static class OtlpTraceMapper
{
    public static List<SpanIngestItem> Map(ExportTraceServiceRequest request)
    {
        // Pre-count to avoid List<T> internal reallocations
        int total = 0;
        foreach (var rs in request.ResourceSpans ?? [])
            foreach (var ss in rs.ScopeSpans ?? [])
                total += ss.Spans?.Count ?? 0;

        var result = new List<SpanIngestItem>(total);

        foreach (var rs in request.ResourceSpans ?? [])
        {
            string serviceName = ExtractServiceName(rs.Resource);

            foreach (var ss in rs.ScopeSpans ?? [])
            foreach (var span in ss.Spans ?? [])
            {
                var item = MapSpan(span, serviceName);
                if (item is not null) result.Add(item);
            }
        }

        return result;
    }

    private static SpanIngestItem? MapSpan(OtlpSpan span, string serviceName)
    {
        if (span.TraceId is null || span.SpanId is null) return null;

        if (!TraceId.TryParseHex(span.TraceId.AsSpan(), out var traceId)) return null;
        if (!SpanId.TryParseHex(span.SpanId.AsSpan(), out var spanId))    return null;

        // Drop outbound HTTP spans that are calls to Rd.Log's own ingestion endpoints.
        // These appear when the app has AddHttpClientInstrumentation() and the Rd.Log
        // Serilog sink (or any OTLP exporter) ships telemetry back to this server.
        if (span.Kind == 3 /* CLIENT */ && IsRdLogInternalSpan(span.Attributes))
            return null;

        SpanId parentId = default;
        if (!string.IsNullOrEmpty(span.ParentSpanId))
            SpanId.TryParseHex(span.ParentSpanId.AsSpan(), out parentId);

        long startNano = ParseNanoString(span.StartTimeUnixNano);
        long endNano   = ParseNanoString(span.EndTimeUnixNano);
        long duration  = endNano > startNano ? endNano - startNano : 0;

        byte[] attrBytes = SerializeAttributes(span.Attributes);

        return new SpanIngestItem
        {
            TraceId           = traceId,
            SpanId            = spanId,
            ParentSpanId      = parentId,
            StartTimeUnixNano = startNano,
            DurationNanos     = duration,
            Name              = span.Name ?? string.Empty,
            ServiceName       = serviceName,
            Kind              = (SpanKind)(span.Kind & 0x07),
            Status            = MapStatus(span.Status),
            AttributesBytes   = attrBytes,
        };
    }

    private static SpanStatusCode MapStatus(OtlpSpanStatus? status) =>
        status?.Code switch
        {
            1 => SpanStatusCode.Ok,
            2 => SpanStatusCode.Error,
            _ => SpanStatusCode.Unset,
        };

    /// <summary>
    /// Returns <c>true</c> for outbound CLIENT spans whose URL targets Rd.Log's own
    /// ingestion or OTLP endpoints (/api/events, /otlp/v1/*).
    /// </summary>
    private static bool IsRdLogInternalSpan(List<OtlpKeyValue>? attrs)
    {
        if (attrs is null) return false;
        foreach (var kv in attrs)
        {
            // Both old (http.url / http.target) and new (url.full / url.path) semconv
            var key = kv.Key;
            if (key is not ("url.full" or "url.path" or "http.url" or "http.target")) continue;
            var val = kv.Value?.StringValue;
            if (val is null) continue;
            if (val.Contains("/api/events", StringComparison.OrdinalIgnoreCase) ||
                val.Contains("/otlp/v1/",   StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    internal static string ExtractServiceName(OtlpResource? resource)
    {
        if (resource?.Attributes is null) return "unknown";
        foreach (var kv in resource.Attributes)
            if (kv.Key == "service.name" && kv.Value?.StringValue is { } s)
                return s;
        return "unknown";
    }

    private static byte[] SerializeAttributes(List<OtlpKeyValue>? attrs)
    {
        if (attrs is null || attrs.Count == 0) return [];

        // Count valid keys first to set correct map header — avoids Dictionary<> + boxing
        int count = 0;
        for (int i = 0; i < attrs.Count; i++)
            if (attrs[i].Key is not null) count++;
        if (count == 0) return [];

        var buf = new ArrayBufferWriter<byte>(count * 32);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(count);
        for (int i = 0; i < attrs.Count; i++)
        {
            var kv = attrs[i];
            if (kv.Key is null) continue;
            w.Write(kv.Key);
            WriteAnyValue(ref w, kv.Value);
        }
        w.Flush();
        return buf.WrittenMemory.ToArray();
    }

    /// <summary>
    /// Writes an <see cref="OtlpAnyValue"/> directly into a <see cref="MessagePackWriter"/>
    /// without boxing value types (bool/double/long) into <c>object?</c>.
    /// </summary>
    private static void WriteAnyValue(ref MessagePackWriter w, OtlpAnyValue? v)
    {
        if (v is null)                { w.WriteNil(); return; }
        if (v.StringValue is not null)  { w.Write(v.StringValue); return; }
        if (v.BoolValue.HasValue)       { w.Write(v.BoolValue.Value); return; }
        if (v.DoubleValue.HasValue)     { w.Write(v.DoubleValue.Value); return; }
        if (v.IntValue is not null && long.TryParse(v.IntValue, out var l)) { w.Write(l); return; }
        if (v.ArrayValue?.Values is { } arr)
        {
            w.WriteArrayHeader(arr.Count);
            for (int i = 0; i < arr.Count; i++)
                WriteAnyValue(ref w, arr[i]);
            return;
        }
        if (v.KvlistValue?.Values is { } kvl)
        {
            int cnt = 0;
            for (int i = 0; i < kvl.Count; i++)
                if (kvl[i].Key is not null) cnt++;
            w.WriteMapHeader(cnt);
            for (int i = 0; i < kvl.Count; i++)
            {
                if (kvl[i].Key is null) continue;
                w.Write(kvl[i].Key!);
                WriteAnyValue(ref w, kvl[i].Value);
            }
            return;
        }
        w.WriteNil();
    }

    internal static object? UnwrapAnyValue(OtlpAnyValue? v)
    {
        if (v is null) return null;
        if (v.StringValue is not null) return v.StringValue;
        if (v.BoolValue.HasValue)      return v.BoolValue.Value;
        if (v.DoubleValue.HasValue)    return v.DoubleValue.Value;
        if (v.IntValue is not null && long.TryParse(v.IntValue, out var l)) return l;
        if (v.ArrayValue?.Values is { } arr)
        {
            // for loop — no LINQ iterator allocation in hot path
            var items = new object?[arr.Count];
            for (int i = 0; i < arr.Count; i++)
                items[i] = UnwrapAnyValue(arr[i]);
            return items;
        }
        if (v.KvlistValue?.Values is { } kvl)
        {
            var d = new Dictionary<string, object?>(kvl.Count);
            foreach (var kv in kvl)
                if (kv.Key is not null) d[kv.Key] = UnwrapAnyValue(kv.Value);
            return d;
        }
        return null;
    }

    internal static long ParseNanoString(string? s)
    {
        if (s is null) return 0;
        return long.TryParse(s, out var v) ? v : 0;
    }
}
