using MessagePack;
using System.Buffers;
using Rd.Log.Core;
using Rd.Log.Otel.Models;

namespace Rd.Log.Otel;

/// <summary>
/// Maps an OTLP <see cref="ExportLogsServiceRequest"/> (JSON model) to
/// <see cref="LogEvent"/> objects ready for ingestion via
/// <see cref="Rd.Log.Ingestion.IngestionEndpoint.IngestEvents"/>.
///
/// Mapping rules:
/// - <c>body.stringValue</c>     → <c>@mt</c> (message template)
/// - <c>severityNumber</c>       → <c>@l</c>  (log level)
/// - <c>attributes</c>           → structured properties
/// - <c>resource.attributes</c>  → added as properties (service.name, etc.)
/// - <c>traceId</c> / <c>spanId</c> → properties <c>TraceId</c> / <c>SpanId</c>
///   (enables log↔trace correlation in the query UI)
/// </summary>
public static class OtlpLogMapper
{
    public static List<LogEvent> Map(ExportLogsServiceRequest request, uint nodeId)
    {
        // Pre-count to avoid List<T> internal reallocations
        int total = 0;
        foreach (var rl in request.ResourceLogs ?? [])
            foreach (var sl in rl.ScopeLogs ?? [])
                total += sl.LogRecords?.Count ?? 0;

        var result = new List<LogEvent>(total);
        uint seq = (uint)Environment.TickCount;

        foreach (var rl in request.ResourceLogs ?? [])
        {
            // Hold OTLP list reference directly — avoids Dictionary<> allocation per resource
            var resourceAttrs = rl.Resource?.Attributes;

            foreach (var sl in rl.ScopeLogs ?? [])
            foreach (var rec in sl.LogRecords ?? [])
            {
                var ev = MapRecord(rec, resourceAttrs, nodeId, seq++);
                if (ev is not null) result.Add(ev);
            }
        }
        return result;
    }

    private static LogEvent? MapRecord(
        OtlpLogRecord rec,
        List<OtlpKeyValue>? resourceAttrs,
        uint nodeId,
        uint seq)
    {
        long nanos = OtlpTraceMapper.ParseNanoString(rec.TimeUnixNano);
        var ts = nanos > 0
            ? DateTimeOffset.FromUnixTimeMilliseconds(nanos / 1_000_000)
            : DateTimeOffset.UtcNow;
        var level = MapSeverity(rec.SeverityNumber, rec.SeverityText);
        string mt = rec.Body?.StringValue ?? string.Empty;

        // Serialize all props directly — no intermediate Dictionary<> clone per record
        var rawProps = SerializeAllProps(resourceAttrs, rec.Attributes, rec.TraceId, rec.SpanId);

        return new LogEvent
        {
            Id              = new EventId(nodeId, seq),
            Timestamp       = ts,
            Level           = level,
            MessageTemplate = mt,
            Properties      = null,   // RawProperties is authoritative; no dict alloc
            RawProperties   = rawProps,
        };
    }

    // ── Serialization ─────────────────────────────────────────────────────────

    /// <summary>
    /// Writes resource + record attributes directly to a msgpack map without
    /// cloning into an intermediate <see cref="Dictionary{TKey,TValue}"/>.
    /// Uses <see cref="ArrayBufferWriter{T}"/> to avoid MemoryStream + ToArray() double-copy.
    /// </summary>
    private static ReadOnlyMemory<byte> SerializeAllProps(
        List<OtlpKeyValue>? resourceAttrs,
        List<OtlpKeyValue>? recordAttrs,
        string? traceId,
        string? spanId)
    {
        int count = CountValidKeys(resourceAttrs) + CountValidKeys(recordAttrs)
                  + (string.IsNullOrEmpty(traceId) ? 0 : 1)
                  + (string.IsNullOrEmpty(spanId)  ? 0 : 1);
        if (count == 0) return ReadOnlyMemory<byte>.Empty;

        var buf = new ArrayBufferWriter<byte>(count * 32);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(count);
        WriteKeyValues(ref w, resourceAttrs);
        WriteKeyValues(ref w, recordAttrs);
        if (!string.IsNullOrEmpty(traceId)) { w.Write("TraceId"); w.Write(traceId); }
        if (!string.IsNullOrEmpty(spanId))  { w.Write("SpanId");  w.Write(spanId);  }
        w.Flush();
        return buf.WrittenMemory;
    }

    private static int CountValidKeys(List<OtlpKeyValue>? kvs)
    {
        if (kvs is null) return 0;
        int c = 0;
        for (int i = 0; i < kvs.Count; i++)
            if (kvs[i].Key is not null) c++;
        return c;
    }

    private static void WriteKeyValues(ref MessagePackWriter w, List<OtlpKeyValue>? kvs)
    {
        if (kvs is null) return;
        for (int i = 0; i < kvs.Count; i++)
        {
            var kv = kvs[i];
            if (kv.Key is null) continue;
            w.Write(kv.Key);
            WriteAnyValue(ref w, kv.Value);
        }
    }

    /// <summary>
    /// Writes an <see cref="OtlpAnyValue"/> directly into a <see cref="MessagePackWriter"/>
    /// without boxing into an intermediate <c>object?</c>.
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
            for (int i = 0; i < arr.Count; i++) // for loop — no LINQ iterator
                WriteAnyValue(ref w, arr[i]);
            return;
        }
        if (v.KvlistValue?.Values is { } kvl)
        {
            int cnt = CountValidKeys(kvl);
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

    /// <summary>
    /// OTLP severity number → Rd.Log level.
    /// Fallback uses <see cref="ReadOnlySpan{T}"/> comparison to avoid
    /// <c>ToUpperInvariant()</c> string allocation.
    /// </summary>
    private static LogLevel MapSeverity(int severityNumber, string? severityText)
    {
        if (severityNumber >= 21) return LogLevel.Fatal;
        if (severityNumber >= 17) return LogLevel.Error;
        if (severityNumber >= 13) return LogLevel.Warning;
        if (severityNumber >= 9)  return LogLevel.Information;
        if (severityNumber >= 5)  return LogLevel.Debug;
        if (severityNumber >= 1)  return LogLevel.Verbose;

        if (severityText is null) return LogLevel.Information;
        var sv = severityText.AsSpan();
        if (sv.StartsWith("FATAL", StringComparison.OrdinalIgnoreCase)) return LogLevel.Fatal;
        if (sv.StartsWith("ERROR", StringComparison.OrdinalIgnoreCase)) return LogLevel.Error;
        if (sv.Equals("WARN",    StringComparison.OrdinalIgnoreCase) ||
            sv.Equals("WARNING", StringComparison.OrdinalIgnoreCase))   return LogLevel.Warning;
        if (sv.Equals("INFO",        StringComparison.OrdinalIgnoreCase) ||
            sv.Equals("INFORMATION", StringComparison.OrdinalIgnoreCase)) return LogLevel.Information;
        if (sv.StartsWith("DEBUG", StringComparison.OrdinalIgnoreCase)) return LogLevel.Debug;
        if (sv.StartsWith("TRACE", StringComparison.OrdinalIgnoreCase)) return LogLevel.Verbose;
        return LogLevel.Information;
    }
}
