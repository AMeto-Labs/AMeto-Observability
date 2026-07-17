using System.Text;
using System.Text.Json;
using Ameto.Otel;
using Ameto.Otel.Models;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// The streaming OTLP trace parser must produce items equivalent to the reflection DOM
/// path it replaces (<c>JsonSerializer.Deserialize</c> + <c>OtlpTraceMapper.Map</c>) —
/// field by field, with byte-identical attribute msgpack — including the drop rules
/// (missing/invalid ids, outbound CLIENT spans targeting Ameto's own endpoints).
/// </summary>
public sealed class OtlpTraceStreamingParityTests
{
    private static readonly JsonSerializerOptions DomOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        AllowTrailingCommas  = true,
    };

    [Fact]
    public void Streaming_MatchesDom_ForRepresentativeBatch()
    {
        byte[] utf8 = Encoding.UTF8.GetBytes(SampleBatch);

        var streamed = OtlpTraceStreamParser.Parse(utf8);
        var request  = JsonSerializer.Deserialize<ExportTraceServiceRequest>(utf8, DomOptions)!;
        var dom      = OtlpTraceMapper.Map(request);

        Assert.Equal(dom.Count, streamed.Count);
        for (int i = 0; i < dom.Count; i++)
        {
            var d = dom[i];
            var s = streamed[i];

            Assert.Equal(d.TraceId,           s.TraceId);
            Assert.Equal(d.SpanId,            s.SpanId);
            Assert.Equal(d.ParentSpanId,      s.ParentSpanId);
            Assert.Equal(d.StartTimeUnixNano, s.StartTimeUnixNano);
            Assert.Equal(d.DurationNanos,     s.DurationNanos);
            Assert.Equal(d.Name,              s.Name);
            Assert.Equal(d.ServiceName,       s.ServiceName);
            Assert.Equal(d.Kind,              s.Kind);
            Assert.Equal(d.Status,            s.Status);
            Assert.Equal(d.HttpStatusCode,    s.HttpStatusCode);
            Assert.True(d.AttributesBytes.AsSpan().SequenceEqual(s.AttributesBytes),
                $"attributes msgpack mismatch on span #{i} ({d.Name})");
        }

        // The batch deliberately contains 2 droppable spans (see SampleBatch) —
        // make sure the surviving count proves the drop rules actually ran.
        Assert.Equal(4, streamed.Count);
    }

    /// <summary>
    /// Covers: SERVER span with mixed-type attrs + status Error + old-key http status;
    /// span with new-key http status (string form) overriding an earlier old key;
    /// CLIENT span calling Ameto's own OTLP endpoint (dropped); span with missing traceId
    /// (dropped); span with nested array + kvlist attributes; minimal span without
    /// attributes/parent/status.
    /// </summary>
    private const string SampleBatch = """
    {
      "resourceSpans": [
        {
          "resource": {
            "attributes": [
              { "key": "service.name", "value": { "stringValue": "Wallet.API" } },
              { "key": "host.name",    "value": { "stringValue": "srv-01" } }
            ]
          },
          "scopeSpans": [
            {
              "scope": { "name": "otel.sdk" },
              "spans": [
                {
                  "traceId": "f6f6f098569a7f2ba54f3c734aa563f0",
                  "spanId": "a1b2c3d4e5f60718",
                  "parentSpanId": "0102030405060708",
                  "name": "POST /api/pay",
                  "kind": 2,
                  "startTimeUnixNano": "1783953780000000000",
                  "endTimeUnixNano":   "1783953780250000000",
                  "status": { "code": 2, "message": "boom" },
                  "attributes": [
                    { "key": "http.method",      "value": { "stringValue": "POST" } },
                    { "key": "http.status_code", "value": { "intValue": "500" } },
                    { "key": "retry",            "value": { "boolValue": true } },
                    { "key": "duration_ms",      "value": { "doubleValue": 12.75 } }
                  ]
                },
                {
                  "traceId": "00000000000000010000000000000002",
                  "spanId": "00000000000000aa",
                  "name": "GET /api/status",
                  "kind": 2,
                  "startTimeUnixNano": "1783953781000000000",
                  "endTimeUnixNano":   "1783953781100000000",
                  "attributes": [
                    { "key": "http.status_code",          "value": { "intValue": "301" } },
                    { "key": "http.response.status_code", "value": { "stringValue": "200" } }
                  ]
                },
                {
                  "traceId": "11111111111111111111111111111111",
                  "spanId": "1111111111111111",
                  "name": "POST",
                  "kind": 3,
                  "startTimeUnixNano": "1783953782000000000",
                  "endTimeUnixNano":   "1783953782010000000",
                  "attributes": [
                    { "key": "url.full", "value": { "stringValue": "http://AMETO-HOST:8555/OTLP/v1/traces" } }
                  ]
                },
                {
                  "spanId": "2222222222222222",
                  "name": "no-trace-id",
                  "kind": 1,
                  "startTimeUnixNano": "1783953783000000000",
                  "endTimeUnixNano":   "1783953783001000000"
                },
                {
                  "traceId": "22222222222222222222222222222222",
                  "spanId": "3333333333333333",
                  "name": "nested-attrs",
                  "kind": 1,
                  "startTimeUnixNano": "1783953784000000000",
                  "endTimeUnixNano":   "1783953784000500000",
                  "attributes": [
                    { "key": "tags", "value": { "arrayValue": { "values": [
                        { "stringValue": "a" }, { "intValue": "7" }, { "boolValue": false }
                    ] } } },
                    { "key": "ctx", "value": { "kvlistValue": { "values": [
                        { "key": "region", "value": { "stringValue": "kz" } },
                        { "key": "zone",   "value": { "intValue": "4" } }
                    ] } } }
                  ]
                }
              ]
            }
          ]
        },
        {
          "resource": { "attributes": [] },
          "scopeSpans": [
            {
              "spans": [
                {
                  "traceId": "33333333333333333333333333333333",
                  "spanId": "4444444444444444",
                  "name": "minimal",
                  "kind": 1,
                  "startTimeUnixNano": "1783953785000000000",
                  "endTimeUnixNano":   "1783953784000000000"
                }
              ]
            }
          ]
        }
      ]
    }
    """;
}
