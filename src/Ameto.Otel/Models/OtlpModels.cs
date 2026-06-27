using System.Text.Json.Serialization;

namespace Ameto.Otel.Models;

// ── OTLP/JSON Trace models ────────────────────────────────────────────────────
// Spec: https://opentelemetry.io/docs/specs/otlp/#json-encoding

public sealed class ExportTraceServiceRequest
{
    [JsonPropertyName("resourceSpans")]
    public List<ResourceSpans>? ResourceSpans { get; set; }
}

public sealed class ResourceSpans
{
    [JsonPropertyName("resource")]
    public OtlpResource? Resource { get; set; }

    [JsonPropertyName("scopeSpans")]
    public List<ScopeSpans>? ScopeSpans { get; set; }
}

public sealed class ScopeSpans
{
    [JsonPropertyName("scope")]
    public OtlpInstrumentationScope? Scope { get; set; }

    [JsonPropertyName("spans")]
    public List<OtlpSpan>? Spans { get; set; }
}

public sealed class OtlpSpan
{
    /// <summary>Hex-encoded 16-byte trace id.</summary>
    [JsonPropertyName("traceId")]
    public string? TraceId { get; set; }

    /// <summary>Hex-encoded 8-byte span id.</summary>
    [JsonPropertyName("spanId")]
    public string? SpanId { get; set; }

    /// <summary>Hex-encoded 8-byte parent span id (absent or empty = root span).</summary>
    [JsonPropertyName("parentSpanId")]
    public string? ParentSpanId { get; set; }

    [JsonPropertyName("name")]
    public string? Name { get; set; }

    /// <summary>
    /// SpanKind: 0=UNSPECIFIED, 1=INTERNAL, 2=SERVER, 3=CLIENT, 4=PRODUCER, 5=CONSUMER
    /// </summary>
    [JsonPropertyName("kind")]
    public int Kind { get; set; }

    /// <summary>Start time, Unix nanoseconds as string (int64 too large for JSON number).</summary>
    [JsonPropertyName("startTimeUnixNano")]
    public string? StartTimeUnixNano { get; set; }

    /// <summary>End time, Unix nanoseconds as string.</summary>
    [JsonPropertyName("endTimeUnixNano")]
    public string? EndTimeUnixNano { get; set; }

    [JsonPropertyName("attributes")]
    public List<OtlpKeyValue>? Attributes { get; set; }

    [JsonPropertyName("status")]
    public OtlpSpanStatus? Status { get; set; }

    [JsonPropertyName("events")]
    public List<OtlpSpanEvent>? Events { get; set; }
}

public sealed class OtlpSpanStatus
{
    /// <summary>0=UNSET, 1=OK, 2=ERROR</summary>
    [JsonPropertyName("code")]
    public int Code { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }
}

public sealed class OtlpSpanEvent
{
    [JsonPropertyName("timeUnixNano")]
    public string? TimeUnixNano { get; set; }

    [JsonPropertyName("name")]
    public string? Name { get; set; }

    [JsonPropertyName("attributes")]
    public List<OtlpKeyValue>? Attributes { get; set; }
}

// ── OTLP/JSON Metrics models ──────────────────────────────────────────────────

public sealed class ExportMetricsServiceRequest
{
    [JsonPropertyName("resourceMetrics")]
    public List<ResourceMetrics>? ResourceMetrics { get; set; }
}

public sealed class ResourceMetrics
{
    [JsonPropertyName("resource")]
    public OtlpResource? Resource { get; set; }

    [JsonPropertyName("scopeMetrics")]
    public List<ScopeMetrics>? ScopeMetrics { get; set; }
}

public sealed class ScopeMetrics
{
    [JsonPropertyName("scope")]
    public OtlpInstrumentationScope? Scope { get; set; }

    [JsonPropertyName("metrics")]
    public List<OtlpMetric>? Metrics { get; set; }
}

public sealed class OtlpMetric
{
    [JsonPropertyName("name")]
    public string? Name { get; set; }

    [JsonPropertyName("description")]
    public string? Description { get; set; }

    [JsonPropertyName("unit")]
    public string? Unit { get; set; }

    // Only one of the following will be set per metric
    [JsonPropertyName("gauge")]
    public OtlpGauge? Gauge { get; set; }

    [JsonPropertyName("sum")]
    public OtlpSum? Sum { get; set; }

    [JsonPropertyName("histogram")]
    public OtlpHistogram? Histogram { get; set; }
}

public sealed class OtlpGauge
{
    [JsonPropertyName("dataPoints")]
    public List<OtlpNumberDataPoint>? DataPoints { get; set; }
}

public sealed class OtlpSum
{
    [JsonPropertyName("dataPoints")]
    public List<OtlpNumberDataPoint>? DataPoints { get; set; }

    [JsonPropertyName("isMonotonic")]
    public bool IsMonotonic { get; set; }
}

public sealed class OtlpHistogram
{
    [JsonPropertyName("dataPoints")]
    public List<OtlpHistogramDataPoint>? DataPoints { get; set; }
}

public sealed class OtlpNumberDataPoint
{
    [JsonPropertyName("attributes")]
    public List<OtlpKeyValue>? Attributes { get; set; }

    [JsonPropertyName("timeUnixNano")]
    public string? TimeUnixNano { get; set; }

    [JsonPropertyName("asDouble")]
    public double? AsDouble { get; set; }

    [JsonPropertyName("asInt")]
    public string? AsInt { get; set; }
}

public sealed class OtlpHistogramDataPoint
{
    [JsonPropertyName("attributes")]
    public List<OtlpKeyValue>? Attributes { get; set; }

    [JsonPropertyName("timeUnixNano")]
    public string? TimeUnixNano { get; set; }

    [JsonPropertyName("count")]
    public string? Count { get; set; }

    [JsonPropertyName("sum")]
    public double? Sum { get; set; }

    [JsonPropertyName("explicitBounds")]
    public List<double>? ExplicitBounds { get; set; }

    [JsonPropertyName("bucketCounts")]
    public List<string>? BucketCounts { get; set; }

    [JsonPropertyName("exemplars")]
    public List<OtlpExemplar>? Exemplars { get; set; }
}

/// <summary>OTLP Exemplar — links a sampled measurement to a trace/span.</summary>
public sealed class OtlpExemplar
{
    [JsonPropertyName("timeUnixNano")]
    public string? TimeUnixNano { get; set; }

    [JsonPropertyName("asDouble")]
    public double? AsDouble { get; set; }

    [JsonPropertyName("asInt")]
    public string? AsInt { get; set; }

    /// <summary>16-char lowercase hex (set by the protobuf decoder).</summary>
    [JsonPropertyName("spanId")]
    public string? SpanId { get; set; }

    /// <summary>32-char lowercase hex (set by the protobuf decoder).</summary>
    [JsonPropertyName("traceId")]
    public string? TraceId { get; set; }
}

// ── OTLP/JSON Logs models ─────────────────────────────────────────────────────

public sealed class ExportLogsServiceRequest
{
    [JsonPropertyName("resourceLogs")]
    public List<ResourceLogs>? ResourceLogs { get; set; }
}

public sealed class ResourceLogs
{
    [JsonPropertyName("resource")]
    public OtlpResource? Resource { get; set; }

    [JsonPropertyName("scopeLogs")]
    public List<ScopeLogs>? ScopeLogs { get; set; }
}

public sealed class ScopeLogs
{
    [JsonPropertyName("scope")]
    public OtlpInstrumentationScope? Scope { get; set; }

    [JsonPropertyName("logRecords")]
    public List<OtlpLogRecord>? LogRecords { get; set; }
}

public sealed class OtlpLogRecord
{
    [JsonPropertyName("timeUnixNano")]
    public string? TimeUnixNano { get; set; }

    /// <summary>
    /// Numeric severity: 1–4=TRACE, 5–8=DEBUG, 9–12=INFO,
    /// 13–16=WARN, 17–20=ERROR, 21–24=FATAL.
    /// </summary>
    [JsonPropertyName("severityNumber")]
    public int SeverityNumber { get; set; }

    [JsonPropertyName("severityText")]
    public string? SeverityText { get; set; }

    [JsonPropertyName("body")]
    public OtlpAnyValue? Body { get; set; }

    [JsonPropertyName("attributes")]
    public List<OtlpKeyValue>? Attributes { get; set; }

    [JsonPropertyName("traceId")]
    public string? TraceId { get; set; }

    [JsonPropertyName("spanId")]
    public string? SpanId { get; set; }
}

// ── Shared OTLP primitives ────────────────────────────────────────────────────

public sealed class OtlpResource
{
    [JsonPropertyName("attributes")]
    public List<OtlpKeyValue>? Attributes { get; set; }
}

public sealed class OtlpInstrumentationScope
{
    [JsonPropertyName("name")]
    public string? Name { get; set; }

    [JsonPropertyName("version")]
    public string? Version { get; set; }
}

public sealed class OtlpKeyValue
{
    [JsonPropertyName("key")]
    public string? Key { get; set; }

    [JsonPropertyName("value")]
    public OtlpAnyValue? Value { get; set; }
}

public sealed class OtlpAnyValue
{
    [JsonPropertyName("stringValue")]
    public string? StringValue { get; set; }

    [JsonPropertyName("boolValue")]
    public bool? BoolValue { get; set; }

    [JsonPropertyName("intValue")]
    public string? IntValue { get; set; }

    [JsonPropertyName("doubleValue")]
    public double? DoubleValue { get; set; }

    [JsonPropertyName("arrayValue")]
    public OtlpArrayValue? ArrayValue { get; set; }

    [JsonPropertyName("kvlistValue")]
    public OtlpKeyValueList? KvlistValue { get; set; }
}

public sealed class OtlpArrayValue
{
    [JsonPropertyName("values")]
    public List<OtlpAnyValue>? Values { get; set; }
}

public sealed class OtlpKeyValueList
{
    [JsonPropertyName("values")]
    public List<OtlpKeyValue>? Values { get; set; }
}
