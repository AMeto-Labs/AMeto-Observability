using System.Buffers;
using System.Reflection;
using System.Text;
using Ameto.Core;
using Ameto.Core.Serialization;
using Ameto.Ingestion;
using Ameto.Otel;
using Ameto.Storage;
using MessagePack;

namespace Ameto.Integration.Tests;

/// <summary>
/// What decoding a log batch holds BESIDE the batch — the parsers' scratch, the intern pool — and
/// the walk that refuses a JSON body before any of it is decoded (round-4 review H3, H4, M2, L9, L10).
/// </summary>
public sealed class LogBatchScratchTests
{
    private const int Limit = 64 * 1024;

    /// <summary>An OTLP sink that keeps what the parser hands it: the maps it assembled, and the sizes of the ones it refused unassembled.</summary>
    private sealed class CapturingSink : IOtlpLogSink, LogEventSerializer.IClefBatchSink
    {
        public readonly List<int>  Assembled = [];
        public readonly List<long> Refused   = [];

        public int MaxEventPayloadBytes => Limit;

        public bool TryIngestRaw(long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, ReadOnlySpan<byte> msgpackProps,
                                 ulong traceHi, ulong traceLo, ulong spanId, ReadOnlySpan<byte> serviceUtf8)
        {
            Assembled.Add(msgpackProps.Length);
            return msgpackProps.Length <= Limit;
        }

        public bool TryIngestOversized(long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, long payloadBytes,
                                       ulong traceHi, ulong traceLo, ulong spanId, ReadOnlySpan<byte> serviceUtf8, int serviceIdx)
        {
            Refused.Add(payloadBytes);
            return false;
        }

        public void NotifyBatchEnqueued() { }

        public bool TryIngestClef(long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, ExceptionInfo? exception,
                                  ReadOnlySpan<byte> msgpackProps, ulong traceHi, ulong traceLo, ulong spanId, ReadOnlySpan<byte> serviceUtf8)
        {
            Assembled.Add(msgpackProps.Length);
            return msgpackProps.Length <= Limit;
        }

        public bool TryIngestOversizedClef(long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, long payloadBytes,
                                           ulong traceHi, ulong traceLo, ulong spanId, ReadOnlySpan<byte> serviceUtf8)
        {
            Refused.Add(payloadBytes);
            return false;
        }
    }

    /// <summary>The capacity of a parser's [ThreadStatic] scratch buffer on this thread, or 0 when it has none.</summary>
    private static int ScratchCapacity(Type parser, string field) =>
        (parser.GetField(field, BindingFlags.NonPublic | BindingFlags.Static)!.GetValue(null) as ArrayBufferWriter<byte>)?.Capacity ?? 0;

    // ── H3: the parsers' scratch ──────────────────────────────────────────────

    /// <summary>
    /// Round-4 review H3, protobuf. A resource attribute of bytes that are not UTF-8 is re-encoded as
    /// U+FFFD, three bytes each — in the thread's resource scratch, and through a pooled char buffer as
    /// long as the attribute — and the resource was copied into every record's map, assembled whole
    /// before the record was refused as oversized: ~9x the attribute held per request, beside the batch
    /// and uncounted (a 7.9 MB attribute charged 8.4 MB held 74 MB), and kept on the thread after. A
    /// map past the limit is counted, not built, now; its records are refused unassembled, and the
    /// scratch is let go.
    /// </summary>
    [Fact]
    public void A_protobuf_resource_past_the_event_limit_is_refused_in_every_record_without_building_it()
    {
        var sink = new CapturingSink();
        OtlpLogProtoParser.Parse(LogBatchHeldMemoryTests.InvalidUtf8ResourceBody(attributeBytes: 8, records: 1), sink);   // warm this thread's scratch
        sink.Assembled.Clear();

        byte[] body = LogBatchHeldMemoryTests.InvalidUtf8ResourceBody(attributeBytes: 1_000_000, records: 50);
        long before = GC.GetAllocatedBytesForCurrentThread();
        var (ingested, dropped) = OtlpLogProtoParser.Parse(body, sink);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal((0, 50), (ingested, dropped));
        Assert.Empty(sink.Assembled);                                   // no map was built
        Assert.Equal(50, sink.Refused.Count);
        Assert.All(sink.Refused, size => Assert.True(size >= 1_000_000, $"refused as {size} B: under the attribute's own bytes"));
        Assert.True(allocated < 2_000_000, $"the parse allocated {allocated:N0} B for a 1 MB attribute");
        Assert.True(ScratchCapacity(typeof(OtlpLogProtoParser), "_tRes") <= 1 << 20, "the resource scratch stayed grown on the thread");
        Assert.True(ScratchCapacity(typeof(OtlpLogProtoParser), "_tOut") <= 1 << 20, "the map scratch stayed grown on the thread");
    }

    /// <summary>
    /// H3 and M2, OTLP/JSON: a resource of 200 KB was copied into each of its records before each was
    /// refused (a 4 MB one over 40 000 records took ~5.6 s before the 413). Refused unassembled now.
    /// </summary>
    [Fact]
    public void A_json_resource_past_the_event_limit_is_refused_in_every_record_without_building_it()
    {
        var sink = new CapturingSink();
        var sb   = new StringBuilder("{\"resourceLogs\":[{\"resource\":{\"attributes\":[{\"key\":\"blob\",\"value\":{\"stringValue\":\"");
        sb.Append('x', 200_000).Append("\"}}]},\"scopeLogs\":[{\"logRecords\":[");
        for (int i = 0; i < 100; i++) sb.Append(i == 0 ? "" : ",").Append("{}");
        sb.Append("]}]}]}");

        var (ingested, dropped) = OtlpLogStreamParser.Parse(Encoding.UTF8.GetBytes(sb.ToString()), sink);

        Assert.Equal((0, 100), (ingested, dropped));
        Assert.Empty(sink.Assembled);
        Assert.Equal(100, sink.Refused.Count);
        Assert.True(ScratchCapacity(typeof(OtlpLogStreamParser), "_tOut") <= 1 << 20, "the map scratch stayed grown on the thread");
    }

    /// <summary>H3, CLEF: an event whose properties pass the limit is refused before they are copied into the thread's scratch, which is let go.</summary>
    [Fact]
    public void A_clef_event_past_the_event_limit_is_refused_before_its_properties_are_copied()
    {
        var buf = new ArrayBufferWriter<byte>();
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(2);
        w.WriteMapHeader(2);
        w.Write("@mt"); w.Write("big {Blob}");
        w.Write("Blob"); w.Write(new string('b', 3_000_000));
        w.WriteMapHeader(2);
        w.Write("@mt"); w.Write("small {N}");
        w.Write("N");   w.Write(1);
        w.Flush();
        var sink = new CapturingSink();

        int ingested = LogEventSerializer.StreamBatch(buf.WrittenMemory, sink, out int dropped);

        Assert.Equal((1, 1), (ingested, dropped));
        Assert.Single(sink.Assembled);                                 // the small one
        Assert.True(sink.Refused.Single() >= 3_000_000);
        Assert.True(ScratchCapacity(typeof(LogEventSerializer), "_tRawPairs") <= 1 << 20, "the pair scratch stayed grown on the thread");
    }

    // ── H4: the intern pool ───────────────────────────────────────────────────

    /// <summary>
    /// Round-4 review H4. The intern pool keeps every string it takes for the life of the process, and
    /// had a cap on their number only: 26 distinct templates of 4 M characters pinned 206 MiB and the
    /// 27th ran the heap out — left behind even by batches refused 413 or 503. It holds at most 16 Mi
    /// UTF-16 units now: past that a miss is answered unpooled, as on a full pool.
    /// </summary>
    [Fact]
    public void The_intern_pool_stops_taking_text_past_its_budget()
    {
        var pool = new StringInternPool();
        int big  = (int)(StringInternPool.DefaultMaxPooledChars / 4);   // four fill it
        for (int i = 0; i < 4; i++)
            Assert.True(pool.Intern(new string((char)('a' + i), big - 8)) >= 0);

        Assert.Equal(-1, pool.Intern(new string('z', big)));             // the fifth is not pooled
        Assert.Equal(4, pool.ClaimedCount);
        Assert.True(pool.Intern("small") >= 0, "a short one still fits");
    }

    /// <summary>
    /// H4: interning text that is not UTF-8 decoded it through the encoding's fallback, which allocated
    /// as it went (~285 MB for one long body of such bytes). Decoded with nothing but the string now.
    /// </summary>
    [Fact]
    public void Interning_bytes_that_are_not_utf8_allocates_no_more_than_the_string()
    {
        var    pool  = new StringInternPool();
        byte[] bytes = new byte[1_000_000];
        bytes.AsSpan().Fill(0xFF);
        pool.Intern(new byte[] { 0xFF, 0xFE });   // warm

        long before = GC.GetAllocatedBytesForCurrentThread();
        int  index  = pool.Intern(bytes, out string text);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(index >= 0);
        Assert.Equal(1_000_000, text.Length);                            // one U+FFFD for each byte
        Assert.True(allocated < 4_000_000, $"interning 1 MB allocated {allocated:N0} B (the string is 2 MB)");
    }

    // ── L9: the JSON walk refuses what is not one JSON object ─────────────────

    /// <summary>
    /// Round-4 review L9. The walk stopped at a first token that was not an object, or at the object's
    /// end: '[', '[1,2' and '{}garbage' were answered 200 while the docs say a malformed JSON logs body
    /// ingests nothing (400). It reads the whole body now, and refuses anything but one object.
    /// </summary>
    [Theory]
    [InlineData("[")]
    [InlineData("[1,2")]
    [InlineData("{}garbage")]
    [InlineData("[]")]
    [InlineData("")]
    public void The_json_walk_refuses_a_body_that_is_not_one_json_object(string body)
    {
        Assert.ThrowsAny<Exception>(() => OtlpLogBatchWeight.WeighJson(Encoding.UTF8.GetBytes(body)));
    }

    [Fact]
    public void The_json_walk_takes_an_object_and_trailing_whitespace()
    {
        Assert.Equal(0, OtlpLogBatchWeight.WeighJson("{}  \n"u8));
        Assert.Equal(IngestionOptions.DecodedLogRecordBytes,
                     OtlpLogBatchWeight.WeighJson("{\"resourceLogs\":[{\"scopeLogs\":[{\"logRecords\":[{}]}]}]}"u8));
    }

    // ── L10: a batch past one array is too heavy, not out of memory ───────────

    /// <summary>
    /// Round-4 review L10. A batch's payloads are one array, and its limit could be int.MaxValue — past
    /// <see cref="Array.MaxLength"/>: a growth past what one array holds threw OutOfMemoryException before
    /// its charge, answered 503 (retried for ever) instead of 413. The charge comes first now, and the
    /// ceiling is below one array.
    /// </summary>
    [Fact]
    public void A_payload_growth_past_one_array_is_refused_as_too_heavy()
    {
        Assert.True(IngestionOptions.MaxDecodedLogBatchBytesCeiling <= Array.MaxLength);

        var budget = new TooHeavyPastLimit(IngestionOptions.MaxDecodedLogBatchBytesCeiling);
        using var batch = new LogWriteBatch(budget: budget);
        batch.Add(new LogEventHeader { TimestampUtcTicks = 1, Level = LogLevel.Information }, [0x80], "one", exception: null);
        // As if the batch already held what one array can: the next payload needs more.
        typeof(LogWriteBatch).GetField("_payloadLength", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(batch, Array.MaxLength - 1);

        Assert.Throws<LogBatchTooHeavyForTest>(() =>
            batch.Add(new LogEventHeader { TimestampUtcTicks = 2, Level = LogLevel.Information }, new byte[16], "two", exception: null));
    }

    private sealed class LogBatchTooHeavyForTest() : Exception("over the limit");

    /// <summary>A budget with room to spare that refuses any charge taking it past <paramref name="limit"/>.</summary>
    private sealed class TooHeavyPastLimit(long limit) : ILogBatchBudget
    {
        private long _charged;
        public long Headroom => limit - _charged;
        public void Charge(long bytes)
        {
            if (_charged + bytes > limit) throw new LogBatchTooHeavyForTest();
            _charged += bytes;
        }
        public void Released() { }
    }
}
