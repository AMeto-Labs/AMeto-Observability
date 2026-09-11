using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Core.Serialization;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// The streaming CLEF batch reader (<see cref="LogEventSerializer.StreamBatch"/>) must be
/// indistinguishable from the LogEvent-materialising one it replaced on the
/// <c>/api/events</c> hot path — same timestamp ticks, level, template (including the
/// <c>@m</c>→<c>@mt</c> fallback), trace/span ids, service name, structured exception, and
/// above all the EXACT msgpack property bytes that end up in the ring and on disk.
/// </summary>
public sealed class ClefStreamingParityTests
{
    // ── Capturing sink ───────────────────────────────────────────────────────

    private sealed record Captured(
        long TsTicks, byte Level, string Template, ExceptionInfo? Exception,
        byte[] Props, ulong TraceHi, ulong TraceLo, ulong SpanId, string Service);

    private sealed class CaptureSink : LogEventSerializer.IClefBatchSink
    {
        public readonly List<Captured> Events = [];

        public bool TryIngestClef(
            long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, ExceptionInfo? exception,
            ReadOnlySpan<byte> msgpackProps, ulong traceHi, ulong traceLo, ulong spanId,
            ReadOnlySpan<byte> serviceUtf8)
        {
            Events.Add(new Captured(
                tsTicks, level,
                System.Text.Encoding.UTF8.GetString(templateUtf8),
                exception,
                msgpackProps.ToArray(),
                traceHi, traceLo, spanId,
                System.Text.Encoding.UTF8.GetString(serviceUtf8)));
            return true;
        }
    }

    // ── The batch ────────────────────────────────────────────────────────────

    /// <summary>
    /// Everything the recon flagged as a contract, in one body: full events, @m-only,
    /// empty/nil @mt, missing and unparseable @t, all the @t shapes real clients emit,
    /// structured and legacy exceptions, bad ids, nested property values, duplicate and
    /// non-ASCII keys.
    /// </summary>
    private static byte[] BuildBatch()
    {
        const string Trace = "4bf92f3577b34da6a3ce929d0e0e4736";
        const string Span  = "00f067aa0ba902b7";

        var buf = new ArrayBufferWriter<byte>(4096);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(14);

        // 1 — the ordinary shape a .NET Seq sink sends.
        w.WriteMapHeader(8);
        w.Write("@t");           w.Write("2024-03-01T10:20:30.1234567Z");
        w.Write("@l");           w.Write("Warning");
        w.Write("@mt");          w.Write("Order {OrderId} failed for {Customer}");
        w.Write("@tr");          w.Write(Trace);
        w.Write("@sp");          w.Write(Span);
        w.Write("service.name"); w.Write("checkout-api");
        w.Write("OrderId");      w.Write(4711L);
        w.Write("Customer");     w.Write("ACME GmbH");

        // 2 — @m only: the rendered message must become the template.
        w.WriteMapHeader(3);
        w.Write("@t"); w.Write("2024-03-01T10:20:31.0000000Z");
        w.Write("@m"); w.Write("rendered message, no template");
        w.Write("n");  w.Write(1L);

        // 3 — @mt present but empty: still falls back to @m.
        w.WriteMapHeader(3);
        w.Write("@t");  w.Write("2024-03-01T10:20:32.0000000Z");
        w.Write("@mt"); w.Write("");
        w.Write("@m");  w.Write("fallback wins");

        // 4 — nil @mt and nil @l.
        w.WriteMapHeader(4);
        w.Write("@t");  w.Write("2024-03-01T10:20:33.0000000Z");
        w.Write("@mt"); w.WriteNil();
        w.Write("@l");  w.WriteNil();
        w.Write("@m");  w.Write("nil fields");

        // 5 — structured exception with an inner one.
        w.WriteMapHeader(4);
        w.Write("@t");  w.Write("2024-03-01T10:20:34.5000000+05:00");
        w.Write("@l");  w.Write("Error");
        w.Write("@mt"); w.Write("boom");
        w.Write("@x");
        w.WriteMapHeader(4);
        w.Write("type");  w.Write("System.InvalidOperationException");
        w.Write("msg");   w.Write("outer went wrong");
        w.Write("stk");   w.Write("   at Foo.Bar()\n   at Baz.Qux()");
        w.Write("inner");
        w.WriteMapHeader(2);
        w.Write("type");  w.Write("System.IO.IOException");
        w.Write("msg");   w.Write("inner went wrong");

        // 6 — legacy exception as a plain string.
        w.WriteMapHeader(3);
        w.Write("@t");  w.Write("2024-03-01T10:20:35.0000000Z");
        w.Write("@mt"); w.Write("legacy ex");
        w.Write("@x");  w.Write("System.Exception: plain text");

        // 7 — nested property values: map, array, double, bool, nil, binary.
        w.WriteMapHeader(8);
        w.Write("@t");  w.Write("2024-03-01T10:20:36.0000000Z");
        w.Write("@mt"); w.Write("nested {Payload}");
        w.Write("Payload");
        w.WriteMapHeader(2);
        w.Write("inner"); w.Write("value");
        w.Write("count"); w.Write(7L);
        w.Write("Tags");
        w.WriteArrayHeader(3);
        w.Write("a"); w.Write("b"); w.Write(3L);
        w.Write("Ratio");  w.Write(0.25d);
        w.Write("Enabled"); w.Write(true);
        w.Write("Missing"); w.WriteNil();
        w.Write("Blob");    w.Write(new byte[] { 1, 2, 3 });

        // 8 — no @t at all (falls back to "now" on both paths).
        w.WriteMapHeader(2);
        w.Write("@mt"); w.Write("no timestamp");
        w.Write("k");   w.Write("v");

        // 9 — unparseable @t (also falls back to "now").
        w.WriteMapHeader(2);
        w.Write("@t");  w.Write("not-a-timestamp");
        w.Write("@mt"); w.Write("bad timestamp");

        // 10 — @t with 3-digit fractional seconds (non-.NET Seq clients).
        w.WriteMapHeader(2);
        w.Write("@t");  w.Write("2024-03-01T10:20:37.123Z");
        w.Write("@mt"); w.Write("three digits");

        // 11 — @t with no offset at all, and no fraction.
        w.WriteMapHeader(2);
        w.Write("@t");  w.Write("2024-03-01T10:20:38");
        w.Write("@mt"); w.Write("no offset");

        // 12 — malformed trace/span ids (wrong length, non-hex) — both must yield 0.
        w.WriteMapHeader(4);
        w.Write("@t");  w.Write("2024-03-01T10:20:39.0000000Z");
        w.Write("@mt"); w.Write("bad ids");
        w.Write("@tr"); w.Write("zzzz");
        w.Write("@sp"); w.Write("00f067aa0ba902b7ff");

        // 13 — no properties at all: RawProperties must stay empty.
        w.WriteMapHeader(2);
        w.Write("@t");  w.Write("2024-03-01T10:20:40.0000000Z");
        w.Write("@mt"); w.Write("bare");

        // 14 — duplicate keys, non-ASCII key/value, lowercase level alias.
        w.WriteMapHeader(6);
        w.Write("@t");   w.Write("2024-03-01T10:20:41.0000000Z");
        w.Write("@l");   w.Write("info");
        w.Write("@mt");  w.Write("дубликаты {Ключ}");
        w.Write("Ключ"); w.Write("значение");
        w.Write("dup");  w.Write(1L);
        w.Write("dup");  w.Write(2L);

        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    // ── The gate ─────────────────────────────────────────────────────────────

    [Fact]
    public void StreamBatch_MatchesDeserializeBatch_FieldForFieldAndByteForByte()
    {
        byte[] body = BuildBatch();

        var legacy = new List<LogEvent>();
        uint seq   = 0;
        LogEventSerializer.DeserializeBatch(
            new ReadOnlySequence<byte>(body), nodeId: 0, ref seq, legacy);

        var sink = new CaptureSink();
        int ingested = LogEventSerializer.StreamBatch(body, sink, out int dropped);

        Assert.Equal(legacy.Count, ingested);
        Assert.Equal(0, dropped);
        Assert.Equal(legacy.Count, sink.Events.Count);

        // Events 8 and 9 have no usable @t, so both paths stamp their own "now"; every
        // other event must agree to the tick.
        var nowFallback = new HashSet<int> { 7, 8 };

        for (int i = 0; i < legacy.Count; i++)
        {
            LogEvent ev = legacy[i];
            Captured c  = sink.Events[i];
            string  why = $"event #{i + 1}";

            if (nowFallback.Contains(i))
            {
                Assert.True(Math.Abs(c.TsTicks - ev.Timestamp.UtcTicks) < TimeSpan.TicksPerMinute, why);
                Assert.True(Math.Abs(c.TsTicks - DateTimeOffset.UtcNow.UtcTicks) < TimeSpan.TicksPerMinute, why);
            }
            else
            {
                Assert.True(ev.Timestamp.UtcTicks == c.TsTicks,
                    $"{why}: timestamp {ev.Timestamp.UtcTicks} != {c.TsTicks}");
            }

            Assert.Equal((byte)ev.Level, c.Level);
            Assert.Equal(ev.MessageTemplate ?? string.Empty, c.Template);
            Assert.Equal(ev.ServiceName ?? string.Empty, c.Service);
            Assert.Equal(ev.TraceIdHi, c.TraceHi);
            Assert.Equal(ev.TraceIdLo, c.TraceLo);
            Assert.Equal(ev.SpanId, c.SpanId);

            // The bytes the ring copies — the only thing that reaches disk.
            Assert.True(ev.RawProperties.Span.SequenceEqual(c.Props),
                $"{why}: property bytes differ\n  old={Convert.ToHexString(ev.RawProperties.Span)}\n  new={Convert.ToHexString(c.Props)}");

            AssertSameException(ev.Exception, c.Exception, why);
        }

        // Properties is never materialised on the ingest path — RawProperties is the truth,
        // and the streaming path has no dictionary to materialise at all.
        Assert.All(legacy, e => Assert.False(e.PropertiesMaterialised));
    }

    [Fact]
    public void StreamBatch_TruncatedBody_Throws_AfterDeliveringTheIntactPrefix()
    {
        byte[] good = BuildBatch();

        // Truncate mid-way: the first 13 events are intact, the last one is not.
        byte[] truncated = good.AsSpan(0, good.Length - 12).ToArray();

        var sink = new CaptureSink();
        Assert.ThrowsAny<Exception>(() => LogEventSerializer.StreamBatch(truncated, sink, out _));
        Assert.Equal(13, sink.Events.Count);
    }

    [Fact]
    public void StreamBatch_NotAnArray_Throws_WithNothingDelivered()
    {
        var sink = new CaptureSink();
        Assert.ThrowsAny<Exception>(
            () => LogEventSerializer.StreamBatch(new byte[] { 0xa5, 0x68, 0x65, 0x6c, 0x6c, 0x6f }, sink, out _));
        Assert.Empty(sink.Events);
    }

    [Fact]
    public void StreamBatch_GarbageBody_Throws_WithNothingDelivered()
    {
        var sink = new CaptureSink();
        Assert.ThrowsAny<Exception>(
            () => LogEventSerializer.StreamBatch(new byte[] { 0xFF, 0xFE, 0x00, 0x01 }, sink, out _));
        Assert.Empty(sink.Events);
    }

    [Fact]
    public void StreamBatch_DropsCountedSeparatelyFromIngests()
    {
        byte[] body = BuildBatch();
        var    sink = new RejectEverySecond();

        int ingested = LogEventSerializer.StreamBatch(body, sink, out int dropped);
        Assert.Equal(14, ingested + dropped);
        Assert.Equal(7, dropped);
    }

    private sealed class RejectEverySecond : LogEventSerializer.IClefBatchSink
    {
        private int _n;
        public bool TryIngestClef(
            long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, ExceptionInfo? exception,
            ReadOnlySpan<byte> msgpackProps, ulong traceHi, ulong traceLo, ulong spanId,
            ReadOnlySpan<byte> serviceUtf8) => (_n++ % 2) == 0;
    }

    private static void AssertSameException(ExceptionInfo? a, ExceptionInfo? b, string why)
    {
        if (a is null || b is null) { Assert.True(a is null && b is null, $"{why}: exception presence differs"); return; }
        Assert.Equal(a.Type, b.Type);
        Assert.Equal(a.Message, b.Message);
        Assert.Equal(a.StackTrace, b.StackTrace);
        AssertSameException(a.Inner, b.Inner, why + " (inner)");
    }
}
