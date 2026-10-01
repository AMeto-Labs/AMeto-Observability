using System.Buffers;
using System.Text;
using System.Text.Json;
using Ameto.Core.Serialization;
using MessagePack;

namespace Ameto.Core.Tests;

/// <summary>
/// The service on the CLEF wire: <c>@service</c> is the header field's own key, and
/// <c>service.name</c> — what the Serilog sink and every Seq-era client send — is still accepted.
/// Both set the header and neither survives as a user property; with both present,
/// <c>@service</c> wins whatever the order (the ServiceTieRule in <see cref="LogEventSerializer"/>).
///
/// <para>Every shape is read by BOTH CLEF readers — the streaming one <c>/api/events</c> runs and
/// the materialising one it was pinned to — and they must agree on the service and on the
/// property bytes, which are what reach the ring.</para>
/// </summary>
public sealed class ClefServiceKeyTests
{
    /// <summary>Shape name → the service keys the event carries, in wire order, and the header expected.</summary>
    private static readonly Dictionary<string, ((string Key, string? Value)[] Keys, string? Expected)> Shapes = new()
    {
        ["@service alone"]                   = ([("@service", "orders-api")],                                 "orders-api"),
        ["service.name alone"]               = ([("service.name", "orders-api")],                             "orders-api"),
        ["both, legacy first"]               = ([("service.name", "legacy-api"), ("@service", "orders-api")], "orders-api"),
        ["both, canonical first"]            = ([("@service", "orders-api"), ("service.name", "legacy-api")], "orders-api"),
        ["empty @service does not erase"]    = ([("@service", ""), ("service.name", "legacy-api")],           "legacy-api"),
        ["nil @service does not erase"]      = ([("@service", null), ("service.name", "legacy-api")],         "legacy-api"),
        ["repeated @service keeps the last"] = ([("@service", "first-api"), ("@service", "orders-api")],      "orders-api"),
        ["neither"]                          = ([],                                                           null),
    };

    [Theory]
    [InlineData("@service alone")]
    [InlineData("service.name alone")]
    [InlineData("both, legacy first")]
    [InlineData("both, canonical first")]
    [InlineData("empty @service does not erase")]
    [InlineData("nil @service does not erase")]
    [InlineData("repeated @service keeps the last")]
    [InlineData("neither")]
    public void Both_readers_take_the_service_into_the_header_and_out_of_the_properties(string shape)
    {
        var (serviceKeys, expected) = Shapes[shape];
        byte[] body = Batch(serviceKeys);

        var streamed = new CaptureSink();
        Assert.Equal(1, LogEventSerializer.StreamBatch(body, streamed, out _));
        var (service, props) = Assert.Single(streamed.Events);

        var events = new List<LogEvent>();
        uint seq   = 1;
        LogEventSerializer.DeserializeBatch(new ReadOnlySequence<byte>(body), 0, ref seq, events);
        var ev = Assert.Single(events);

        // Absent and empty are one state on both readers (an empty service interns to -1).
        Assert.Equal(expected ?? "", service);
        Assert.Equal(expected ?? "", ev.ServiceName ?? "");

        // The property bytes are identical, and they hold the user's property and nothing else.
        Assert.True(ev.RawProperties.Span.SequenceEqual(props), shape);
        var map = LogEventSerializer.DeserializePropertiesMap(props)!;
        Assert.Equal("OrderId", Assert.Single(map.Keys));
    }

    [Fact]
    public void The_json_writer_sends_the_service_as_at_service()
    {
        var ev = new LogEvent
        {
            Id              = new EventId(0u, 7u),
            Timestamp       = new DateTimeOffset(2026, 10, 1, 9, 0, 0, TimeSpan.Zero),
            Level           = LogLevel.Warning,
            MessageTemplate = "Order {OrderId} failed",
            SpanId          = 0xff,
            ServiceName     = "orders-api",
            Properties      = new Dictionary<string, object?> { ["OrderId"] = 4711L },
        };

        var buf = new ArrayBufferWriter<byte>(256);
        using (var w = new Utf8JsonWriter(buf)) LogEventJsonWriter.Write(w, ev);
        string json = Encoding.UTF8.GetString(buf.WrittenSpan);

        // After @sp and before props, where service.name used to be.
        Assert.Contains("\"@sp\":\"00000000000000ff\",\"@service\":\"orders-api\",\"props\":{\"OrderId\":4711}", json);
        Assert.DoesNotContain("service.name", json);

        using var doc = JsonDocument.Parse(json);
        Assert.Equal("orders-api", doc.RootElement.GetProperty("@service").GetString());
    }

    [Fact]
    public void An_event_without_a_service_sends_no_service_key()
    {
        var ev = new LogEvent
        {
            Id              = new EventId(0u, 8u),
            Timestamp       = new DateTimeOffset(2026, 10, 1, 9, 0, 0, TimeSpan.Zero),
            Level           = LogLevel.Information,
            MessageTemplate = "plain",
        };

        var buf = new ArrayBufferWriter<byte>(128);
        using (var w = new Utf8JsonWriter(buf)) LogEventJsonWriter.Write(w, ev);
        string json = Encoding.UTF8.GetString(buf.WrittenSpan);

        Assert.DoesNotContain("@service", json);
        Assert.DoesNotContain("service.name", json);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /// <summary>One CLEF event: the header fields, the given service keys in order, one user property.</summary>
    private static byte[] Batch((string Key, string? Value)[] serviceKeys)
    {
        var buf = new ArrayBufferWriter<byte>(256);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(1);
        w.WriteMapHeader(3 + serviceKeys.Length + 1);
        w.Write("@t");  w.Write("2026-10-01T09:00:00.0000000Z");
        w.Write("@l");  w.Write("Warning");
        w.Write("@mt"); w.Write("Order {OrderId} failed");
        foreach (var (key, value) in serviceKeys)
        {
            w.Write(key);
            if (value is null) w.WriteNil(); else w.Write(value);
        }
        w.Write("OrderId"); w.Write(4711L);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private sealed class CaptureSink : LogEventSerializer.IClefBatchSink
    {
        public readonly List<(string Service, byte[] Props)> Events = [];

        public bool TryIngestClef(
            long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, ExceptionInfo? exception,
            ReadOnlySpan<byte> msgpackProps, ulong traceHi, ulong traceLo, ulong spanId,
            ReadOnlySpan<byte> serviceUtf8)
        {
            Events.Add((Encoding.UTF8.GetString(serviceUtf8), msgpackProps.ToArray()));
            return true;
        }
    }
}
