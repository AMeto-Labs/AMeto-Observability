using Google.Protobuf;
using Ameto.Otel.Models;

namespace Ameto.Otel;

/// <summary>
/// Decodes OTLP/Protobuf (application/x-protobuf) request payloads into the
/// existing OTLP model types (shared with the JSON path), so the same mappers
/// (OtlpMetricMapper, OtlpTraceMapper, OtlpLogMapper) can be reused unchanged.
///
/// Field numbers and wire types follow the OTLP proto spec v1.x:
///   https://github.com/open-telemetry/opentelemetry-proto
///
/// Nested messages are parsed by reading raw bytes via ReadBytes() and creating
/// a child CodedInputStream — avoids dependency on the removed PushLimit/PopLimit
/// public API (removed from Google.Protobuf ≥ 3.21).
///
/// Only the fields consumed by the existing mappers are decoded; everything
/// else is skipped via SkipLastField().
/// </summary>
internal static class OtlpProtoDecoder
{
    // Convenience: for an embedded-message field (wire type 2), the caller has
    // already read the tag; we read the length-prefixed bytes and parse them.
    private static CodedInputStream SubStream(CodedInputStream cis)
        => new(cis.ReadBytes().ToByteArray());

    // ── Public entry points ───────────────────────────────────────────────────

    public static ExportMetricsServiceRequest DecodeMetrics(byte[] buffer, int length)
    {
        var req = new ExportMetricsServiceRequest { ResourceMetrics = [] };
        var cis = new CodedInputStream(buffer, 0, length);
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            if (tag == 10) // field 1, LEN: resource_metrics (repeated)
                req.ResourceMetrics.Add(ReadResourceMetrics(SubStream(cis)));
            else
                cis.SkipLastField();
        }
        return req;
    }

    public static ExportTraceServiceRequest DecodeTraces(byte[] buffer, int length)
    {
        var req = new ExportTraceServiceRequest { ResourceSpans = [] };
        var cis = new CodedInputStream(buffer, 0, length);
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            if (tag == 10) // field 1, LEN: resource_spans (repeated)
                req.ResourceSpans.Add(ReadResourceSpans(SubStream(cis)));
            else
                cis.SkipLastField();
        }
        return req;
    }

    public static ExportLogsServiceRequest DecodeLogs(byte[] buffer, int length)
    {
        var req = new ExportLogsServiceRequest { ResourceLogs = [] };
        var cis = new CodedInputStream(buffer, 0, length);
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            if (tag == 10) // field 1, LEN: resource_logs (repeated)
                req.ResourceLogs.Add(ReadResourceLogs(SubStream(cis)));
            else
                cis.SkipLastField();
        }
        return req;
    }

    // ── Metrics ───────────────────────────────────────────────────────────────

    private static ResourceMetrics ReadResourceMetrics(CodedInputStream cis)
    {
        var rm = new ResourceMetrics { ScopeMetrics = [] };
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: rm.Resource = ReadResource(SubStream(cis));           break; // field 1
                case 18: rm.ScopeMetrics.Add(ReadScopeMetrics(SubStream(cis))); break; // field 2
                default: cis.SkipLastField(); break;
            }
        }
        return rm;
    }

    private static ScopeMetrics ReadScopeMetrics(CodedInputStream cis)
    {
        var sm = new ScopeMetrics { Metrics = [] };
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: sm.Scope = ReadScope(SubStream(cis));         break; // field 1
                case 18: sm.Metrics.Add(ReadMetric(SubStream(cis)));   break; // field 2
                default: cis.SkipLastField(); break;
            }
        }
        return sm;
    }

    private static OtlpMetric ReadMetric(CodedInputStream cis)
    {
        var m = new OtlpMetric();
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: m.Name        = cis.ReadString();              break; // field 1: name
                case 18: m.Description = cis.ReadString();              break; // field 2: description
                case 26: m.Unit        = cis.ReadString();              break; // field 3: unit
                case 42: m.Gauge       = ReadGauge(SubStream(cis));     break; // field 5: gauge
                case 58: m.Sum         = ReadSum(SubStream(cis));       break; // field 7: sum
                case 74: m.Histogram   = ReadHistogram(SubStream(cis)); break; // field 9: histogram
                default: cis.SkipLastField(); break;
            }
        }
        return m;
    }

    private static OtlpGauge ReadGauge(CodedInputStream cis)
    {
        var g = new OtlpGauge { DataPoints = [] };
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            if (tag == 10) g.DataPoints.Add(ReadNumberDataPoint(SubStream(cis))); // field 1
            else cis.SkipLastField();
        }
        return g;
    }

    private static OtlpSum ReadSum(CodedInputStream cis)
    {
        var s = new OtlpSum { DataPoints = [] };
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: s.DataPoints.Add(ReadNumberDataPoint(SubStream(cis))); break; // field 1
                case 24: s.IsMonotonic = cis.ReadBool(); break; // field 3: is_monotonic
                default: cis.SkipLastField(); break;
            }
        }
        return s;
    }

    private static OtlpHistogram ReadHistogram(CodedInputStream cis)
    {
        var h = new OtlpHistogram { DataPoints = [] };
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            if (tag == 10) h.DataPoints.Add(ReadHistogramDataPoint(SubStream(cis))); // field 1
            else cis.SkipLastField();
        }
        return h;
    }

    private static OtlpNumberDataPoint ReadNumberDataPoint(CodedInputStream cis)
    {
        var dp = new OtlpNumberDataPoint { Attributes = [] };
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 17: cis.ReadFixed64(); break;                                   // field 2: start_time (skip)
                case 25: dp.TimeUnixNano = cis.ReadFixed64().ToString(); break;      // field 3: time_unix_nano
                case 33: dp.AsDouble     = cis.ReadDouble();             break;      // field 4: as_double
                case 49: dp.AsInt        = cis.ReadSFixed64().ToString(); break;     // field 6: as_int
                case 58: dp.Attributes.Add(ReadKeyValue(SubStream(cis))); break;     // field 7: attributes
                default: cis.SkipLastField(); break;
            }
        }
        return dp;
    }

    private static OtlpHistogramDataPoint ReadHistogramDataPoint(CodedInputStream cis)
    {
        var dp = new OtlpHistogramDataPoint { Attributes = [] };
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 17: cis.ReadFixed64(); break;                                   // field 2: start_time (skip)
                case 25: dp.TimeUnixNano = cis.ReadFixed64().ToString(); break;      // field 3: time_unix_nano
                case 33: dp.Count        = cis.ReadFixed64().ToString(); break;       // field 4: count (fixed64, OTel .NET SDK encodes as fixed64 not varint)
                case 41: dp.Sum          = cis.ReadDouble();             break;      // field 5: sum
                case 49: // field 6: bucket_counts (OTel .NET SDK encodes as repeated fixed64, not packed)
                {
                    dp.BucketCounts ??= [];
                    dp.BucketCounts.Add(cis.ReadFixed64().ToString());
                    break;
                }
                case 57: // field 7: explicit_bounds (OTel .NET SDK encodes as repeated double, not packed)
                {
                    dp.ExplicitBounds ??= [];
                    dp.ExplicitBounds.Add(cis.ReadDouble());
                    break;
                }
                case 74: dp.Attributes.Add(ReadKeyValue(SubStream(cis))); break;    // field 9: attributes
                default: cis.SkipLastField(); break;
            }
        }
        return dp;
    }

    // ── Traces ────────────────────────────────────────────────────────────────

    private static ResourceSpans ReadResourceSpans(CodedInputStream cis)
    {
        var rs = new ResourceSpans { ScopeSpans = [] };
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: rs.Resource = ReadResource(SubStream(cis));           break; // field 1
                case 18: rs.ScopeSpans.Add(ReadScopeSpans(SubStream(cis)));    break; // field 2
                default: cis.SkipLastField(); break;
            }
        }
        return rs;
    }

    private static ScopeSpans ReadScopeSpans(CodedInputStream cis)
    {
        var ss = new ScopeSpans { Spans = [] };
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: ss.Scope = ReadScope(SubStream(cis));        break; // field 1
                case 18: ss.Spans.Add(ReadSpan(SubStream(cis)));      break; // field 2
                default: cis.SkipLastField(); break;
            }
        }
        return ss;
    }

    private static OtlpSpan ReadSpan(CodedInputStream cis)
    {
        var span = new OtlpSpan { Attributes = [], Events = [] };
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: // field 1: trace_id (bytes → lowercase hex)
                    span.TraceId = HexFromBytes(cis.ReadBytes()); break;
                case 18: // field 2: span_id (bytes → lowercase hex)
                    span.SpanId = HexFromBytes(cis.ReadBytes()); break;
                case 34: // field 4: parent_span_id (bytes → lowercase hex)
                    span.ParentSpanId = HexFromBytes(cis.ReadBytes()); break;
                case 42: span.Name = cis.ReadString(); break;                   // field 5: name
                case 48: span.Kind = cis.ReadEnum();   break;                   // field 6: kind
                case 57: span.StartTimeUnixNano = cis.ReadFixed64().ToString(); break; // field 7
                case 65: span.EndTimeUnixNano   = cis.ReadFixed64().ToString(); break; // field 8
                case 74:  span.Attributes!.Add(ReadKeyValue(SubStream(cis)));   break; // field 9
                case 90:  span.Events!.Add(ReadSpanEvent(SubStream(cis)));      break; // field 11
                case 122: span.Status = ReadStatus(SubStream(cis));             break; // field 15
                default:  cis.SkipLastField(); break;
            }
        }
        return span;
    }

    private static OtlpSpanStatus ReadStatus(CodedInputStream cis)
    {
        var status = new OtlpSpanStatus();
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 18: status.Message = cis.ReadString(); break; // field 2: message
                case 24: status.Code    = cis.ReadEnum();   break; // field 3: code
                default: cis.SkipLastField(); break;
            }
        }
        return status;
    }

    private static OtlpSpanEvent ReadSpanEvent(CodedInputStream cis)
    {
        var evt = new OtlpSpanEvent { Attributes = [] };
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 9:  evt.TimeUnixNano = cis.ReadFixed64().ToString(); break; // field 1: time (I64 tag=(1<<3)|1=9)
                case 18: evt.Name         = cis.ReadString(); break;             // field 2: name
                case 26: evt.Attributes!.Add(ReadKeyValue(SubStream(cis))); break; // field 3
                default: cis.SkipLastField(); break;
            }
        }
        return evt;
    }

    // ── Logs ──────────────────────────────────────────────────────────────────

    private static ResourceLogs ReadResourceLogs(CodedInputStream cis)
    {
        var rl = new ResourceLogs { ScopeLogs = [] };
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: rl.Resource = ReadResource(SubStream(cis));         break; // field 1
                case 18: rl.ScopeLogs.Add(ReadScopeLogs(SubStream(cis)));    break; // field 2
                default: cis.SkipLastField(); break;
            }
        }
        return rl;
    }

    private static ScopeLogs ReadScopeLogs(CodedInputStream cis)
    {
        var sl = new ScopeLogs { LogRecords = [] };
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: sl.Scope = ReadScope(SubStream(cis));              break; // field 1
                case 18: sl.LogRecords.Add(ReadLogRecord(SubStream(cis)));  break; // field 2
                default: cis.SkipLastField(); break;
            }
        }
        return sl;
    }

    private static OtlpLogRecord ReadLogRecord(CodedInputStream cis)
    {
        var log = new OtlpLogRecord();
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 9:  log.TimeUnixNano   = cis.ReadFixed64().ToString(); break; // field 1: time (I64)
                case 16: log.SeverityNumber = cis.ReadEnum();               break; // field 2: severity_number
                case 26: log.SeverityText   = cis.ReadString();             break; // field 3: severity_text
                case 42: log.Body           = ReadAnyValue(SubStream(cis)); break; // field 5: body
                case 50: // field 6: attributes (repeated KeyValue)
                    log.Attributes ??= [];
                    log.Attributes.Add(ReadKeyValue(SubStream(cis)));
                    break;
                case 74: // field 9: trace_id (bytes)
                {
                    var bs = cis.ReadBytes();
                    log.TraceId = bs.Length > 0 ? HexFromBytes(bs) : null;
                    break;
                }
                case 82: // field 10: span_id (bytes)
                {
                    var bs = cis.ReadBytes();
                    log.SpanId = bs.Length > 0 ? HexFromBytes(bs) : null;
                    break;
                }
                default: cis.SkipLastField(); break;
            }
        }
        return log;
    }

    // ── Shared ────────────────────────────────────────────────────────────────

    private static OtlpResource ReadResource(CodedInputStream cis)
    {
        var r = new OtlpResource { Attributes = [] };
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            if (tag == 10) r.Attributes.Add(ReadKeyValue(SubStream(cis))); // field 1
            else cis.SkipLastField();
        }
        return r;
    }

    private static OtlpInstrumentationScope ReadScope(CodedInputStream cis)
    {
        var scope = new OtlpInstrumentationScope();
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: scope.Name    = cis.ReadString(); break; // field 1: name
                case 18: scope.Version = cis.ReadString(); break; // field 2: version
                default: cis.SkipLastField(); break;
            }
        }
        return scope;
    }

    private static OtlpKeyValue ReadKeyValue(CodedInputStream cis)
    {
        var kv = new OtlpKeyValue();
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: kv.Key   = cis.ReadString();              break; // field 1: key
                case 18: kv.Value = ReadAnyValue(SubStream(cis));  break; // field 2: value
                default: cis.SkipLastField(); break;
            }
        }
        return kv;
    }

    private static OtlpAnyValue ReadAnyValue(CodedInputStream cis)
    {
        var av = new OtlpAnyValue();
        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: av.StringValue = cis.ReadString();           break; // field 1: string_value
                case 16: av.BoolValue   = cis.ReadBool();             break; // field 2: bool_value
                case 24: av.IntValue    = cis.ReadInt64().ToString(); break; // field 3: int_value
                case 33: av.DoubleValue = cis.ReadDouble();           break; // field 4: double_value
                default: cis.SkipLastField(); break;
            }
        }
        return av;
    }

    private static string HexFromBytes(ByteString bs)
        => Convert.ToHexString(bs.Span).ToLowerInvariant();
}
