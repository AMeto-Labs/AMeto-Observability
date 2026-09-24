using System.Text;
using System.Text.Json;
using Ameto.Otel;
using Ameto.Otel.Models;
using Ameto.Tracing;
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

    /// <summary>
    /// RETARGETED AT THE RAW SINK (TI#3): the parser streams into <see cref="CapturingSpanSink"/>,
    /// which records exactly what the ring would be handed — name, service and attributes as bytes —
    /// and every one of them is compared with the DOM path byte for byte.
    /// </summary>
    [Fact]
    public void RawSink_MatchesDom_ForRepresentativeBatch()
    {
        byte[] utf8 = Encoding.UTF8.GetBytes(SampleBatch);
        var dom  = OtlpTraceMapper.Map(JsonSerializer.Deserialize<ExportTraceServiceRequest>(utf8, DomOptions)!);

        var sink = new CapturingSpanSink();
        var (ingested, refused) = OtlpTraceStreamParser.Parse(utf8, sink);

        sink.AssertMatches(dom);
        Assert.Equal((4, 0), (ingested, refused));
        // ONCE PER RESOURCE BLOCK that has a span: two blocks, two interns — not one per span.
        Assert.Equal(2, sink.InternCalls);
        Assert.Equal(1, sink.EndBatches);
    }

    /// <summary>
    /// A SINK THAT REFUSES leaves the batch a PREFIX and the counts say so; and a body that throws
    /// part-way still ends the batch (the arena the thread held goes back) with its prefix taken.
    /// </summary>
    [Fact]
    public void RawSink_RefusalsAreCounted_AndTheBatchIsEndedEvenOnAThrow()
    {
        byte[] utf8 = Encoding.UTF8.GetBytes(SampleBatch);
        var refusing = new CapturingSpanSink { RefuseFrom = 1 };
        Assert.Equal((1, 3), OtlpTraceStreamParser.Parse(utf8, refusing));
        Assert.Single(refusing.Spans);

        byte[] truncated = utf8.AsSpan(0, utf8.Length * 3 / 4).ToArray();
        var sink = new CapturingSpanSink();
        Assert.ThrowsAny<Exception>(() => OtlpTraceStreamParser.Parse(truncated, sink));
        Assert.Equal(1, sink.EndBatches);
        Assert.NotEmpty(sink.Spans);                                    // the prefix before the tear
    }

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

/// <summary>
/// THE RAW SINK, RECORDED (TI#3): what a parser hands <see cref="ISpanSink"/> — the name, the service
/// and the attribute blob as BYTES, the ids and times as values — kept so a parity test can compare
/// them with the DOM path byte for byte. The capturing sink of <c>OtlpLogProtoParityTests</c>, for
/// spans.
///
/// <para>It also checks the interning contract as it goes: every span's service index must be one
/// this sink handed out, for EXACTLY the bytes the span carries, and <see cref="InternService"/>
/// must be called once per resource block, not once per span — <see cref="InternCalls"/> is what
/// the tests read to prove it.</para>
/// </summary>
internal sealed class CapturingSpanSink : ISpanSink
{
    internal readonly record struct Span(
        TraceId TraceId, SpanId SpanId, SpanId ParentSpanId, long Start, long Duration,
        byte[] Name, int ServiceIdx, byte[] Service, SpanKind Kind, SpanStatusCode Status,
        short Http, byte[] Attrs);

    public readonly List<Span>   Spans    = [];
    public readonly List<byte[]> Interned = [];
    public int InternCalls;
    public int EndBatches;

    /// <summary>Refuse every span from this one on (0-based), to exercise back-pressure. -1: take all.</summary>
    public int RefuseFrom = -1;
    private int _offered;

    public int InternService(ReadOnlySpan<byte> serviceUtf8)
    {
        InternCalls++;
        Interned.Add(serviceUtf8.ToArray());
        return Interned.Count - 1;
    }

    public bool TryIngestRaw(TraceId traceId, SpanId spanId, SpanId parentSpanId, long startTimeUnixNano,
        long durationNanos, ReadOnlySpan<byte> nameUtf8, int serviceIdx, ReadOnlySpan<byte> serviceUtf8,
        SpanKind kind, SpanStatusCode status, short httpStatusCode, ReadOnlySpan<byte> msgpackAttributes)
    {
        Assert.True(System.Text.Unicode.Utf8.IsValid(nameUtf8),    "the sink was handed a name that is not valid UTF-8");
        Assert.True(System.Text.Unicode.Utf8.IsValid(serviceUtf8), "the sink was handed a service that is not valid UTF-8");
        Assert.InRange(serviceIdx, 0, Interned.Count - 1);
        Assert.True(Interned[serviceIdx].AsSpan().SequenceEqual(serviceUtf8),
            "a span's service index names other bytes than the service it carries");

        if (RefuseFrom >= 0 && _offered++ >= RefuseFrom) return false;
        Spans.Add(new Span(traceId, spanId, parentSpanId, startTimeUnixNano, durationNanos,
                           nameUtf8.ToArray(), serviceIdx, serviceUtf8.ToArray(), kind, status,
                           httpStatusCode, msgpackAttributes.ToArray()));
        return true;
    }

    public void EndBatch() => EndBatches++;

    /// <summary>
    /// Compares every recorded span with the DOM path's item, field by field and the three byte
    /// fields BYTE FOR BYTE, and hands back items built from the recording so a test's own
    /// assertions read what the sink was actually given.
    /// </summary>
    public List<SpanIngestItem> AssertMatches(List<SpanIngestItem> dom)
    {
        Assert.Equal(dom.Count, Spans.Count);
        var items = new List<SpanIngestItem>(Spans.Count);
        for (int i = 0; i < dom.Count; i++)
        {
            var d = dom[i];
            var s = Spans[i];
            Assert.Equal(d.TraceId,           s.TraceId);
            Assert.Equal(d.SpanId,            s.SpanId);
            Assert.Equal(d.ParentSpanId,      s.ParentSpanId);
            Assert.Equal(d.StartTimeUnixNano, s.Start);
            Assert.Equal(d.DurationNanos,     s.Duration);
            Assert.Equal(d.Kind,              s.Kind);
            Assert.Equal(d.Status,            s.Status);
            Assert.Equal(d.HttpStatusCode,    s.Http);
            Assert.True(System.Text.Encoding.UTF8.GetBytes(d.Name).AsSpan().SequenceEqual(s.Name),
                $"span {i} ({d.Name}): name bytes differ");
            Assert.True(System.Text.Encoding.UTF8.GetBytes(d.ServiceName).AsSpan().SequenceEqual(s.Service),
                $"span {i} ({d.Name}): service bytes differ");
            Assert.True(d.AttributesBytes.AsSpan().SequenceEqual(s.Attrs),
                $"span {i} ({d.Name}): attribute bytes differ\n"
              + $"  dom: {Convert.ToHexString(d.AttributesBytes)}\n"
              + $"  new: {Convert.ToHexString(s.Attrs)}");

            items.Add(new SpanIngestItem
            {
                TraceId = s.TraceId, SpanId = s.SpanId, ParentSpanId = s.ParentSpanId,
                StartTimeUnixNano = s.Start, DurationNanos = s.Duration,
                Name = System.Text.Encoding.UTF8.GetString(s.Name),
                ServiceName = System.Text.Encoding.UTF8.GetString(s.Service),
                Kind = s.Kind, Status = s.Status, HttpStatusCode = s.Http, AttributesBytes = s.Attrs,
            });
        }
        return items;
    }
}
