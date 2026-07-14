using System.Text;
using System.Text.Json;
using Ameto.Core;
using Ameto.Core.Serialization;
using Ameto.Ingestion;
using Ameto.Otel;
using Ameto.Otel.Models;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// The zero-alloc streaming OTLP-log parser must produce byte-for-byte equivalent output to
/// the reflection DOM path it replaces (<c>JsonSerializer.Deserialize</c> + OtlpLogMapper.Map).
/// This is the correctness gate for that hot-path rewrite.
/// </summary>
public sealed class OtlpStreamingParityTests
{
    private sealed class CapturingSink : IOtlpLogSink
    {
        public readonly List<(long Ts, byte Level, string Tmpl, byte[] Props, ulong TrHi, ulong TrLo, ulong Sp, string? Svc)> Records = new();

        public bool TryIngestRaw(long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, ReadOnlySpan<byte> msgpackProps,
            ulong traceHi, ulong traceLo, ulong spanId, ReadOnlySpan<byte> serviceUtf8)
        {
            Records.Add((tsTicks, level,
                Encoding.UTF8.GetString(templateUtf8),
                msgpackProps.ToArray(),
                traceHi, traceLo, spanId,
                serviceUtf8.IsEmpty ? null : Encoding.UTF8.GetString(serviceUtf8)));
            return true;
        }

        public void NotifyBatchEnqueued() { }
    }

    [Fact]
    public void Streaming_MatchesDom_ForRepresentativeBatch()
    {
        byte[] utf8 = Encoding.UTF8.GetBytes(SampleBatch);

        var sink = new CapturingSink();
        var (ingested, dropped) = OtlpLogStreamParser.Parse(utf8, sink);

        var req       = JsonSerializer.Deserialize<ExportLogsServiceRequest>(utf8, new JsonSerializerOptions())!;
        var domEvents = OtlpLogMapper.Map(req, NodeId.Local.Value);

        Assert.Equal(domEvents.Count, ingested);
        Assert.Equal(0, dropped);
        Assert.Equal(domEvents.Count, sink.Records.Count);

        for (int i = 0; i < domEvents.Count; i++)
        {
            var d = domEvents[i];
            var s = sink.Records[i];

            Assert.Equal(d.Timestamp.UtcTicks, s.Ts);
            Assert.Equal((byte)d.Level, s.Level);
            Assert.Equal(d.MessageTemplate, s.Tmpl);
            Assert.Equal(d.ServiceName, s.Svc);
            Assert.Equal(d.TraceIdHi, s.TrHi);
            Assert.Equal(d.TraceIdLo, s.TrLo);
            Assert.Equal(d.SpanId, s.Sp);

            var domProps = Decode(d.RawProperties.Span);
            var strProps = Decode(s.Props);
            Assert.Equal(domProps.Count, strProps.Count);
            foreach (var kv in domProps)
            {
                Assert.True(strProps.TryGetValue(kv.Key, out var sv), $"streaming missing key '{kv.Key}'");
                Assert.Equal(kv.Value?.ToString(), sv?.ToString());
            }
        }
    }

    private static Dictionary<string, object?> Decode(ReadOnlySpan<byte> msgpack)
        => msgpack.IsEmpty ? new() : (LogEventSerializer.DeserializePropertiesMap(msgpack) ?? new());

    private const string SampleBatch = """
    {
      "resourceLogs": [
        {
          "resource": {
            "attributes": [
              { "key": "service.name", "value": { "stringValue": "Etisalat.API" } },
              { "key": "host.name",     "value": { "stringValue": "load-gen" } }
            ]
          },
          "scopeLogs": [
            {
              "scope": { "name": "k6.loadtest" },
              "logRecords": [
                {
                  "timeUnixNano": "1783953780000000000",
                  "severityNumber": 9,
                  "severityText": "Information",
                  "body": { "stringValue": "HTTP request handled" },
                  "attributes": [
                    { "key": "orderId",          "value": { "intValue": "12345" } },
                    { "key": "customerId",       "value": { "stringValue": "cust-42" } },
                    { "key": "http.status_code", "value": { "intValue": "200" } },
                    { "key": "duration_ms",      "value": { "doubleValue": 12.5 } },
                    { "key": "cache_hit",        "value": { "boolValue": true } }
                  ]
                },
                {
                  "timeUnixNano": "1783953780500000000",
                  "severityNumber": 17,
                  "severityText": "Error",
                  "body": { "stringValue": "Payment processed" },
                  "traceId": "f6f6f098569a7f2ba54f3c734aa563f0",
                  "spanId": "a1b2c3d4e5f60718",
                  "attributes": [
                    { "key": "region", "value": { "stringValue": "ae-dxb" } },
                    { "key": "amount", "value": { "doubleValue": 999.99 } }
                  ]
                }
              ]
            }
          ]
        }
      ]
    }
    """;
}
