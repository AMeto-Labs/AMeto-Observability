using System.Buffers;
using System.Diagnostics;
using System.Text.Json;
using MessagePack;
using Ameto.Core;
using Ameto.Core.Serialization;
using Ameto.Indexing;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// What a CLEF <c>@x</c> costs on every path an exception-bearing event takes: the ingest decode
/// and its staging, the hot tier's managed retention and its page materialisation, the flush
/// index build, and delivery from a cold segment. One shape throughout — a three-deep chain whose
/// root carries a 14-frame stack trace, ~1.5 KB of msgpack — so the four numbers are comparable.
///
/// <para>Written for the audit's third lever (<c>@x</c> kept in the tier as an
/// <c>ExceptionInfo</c> tree, serialised twice): each fact prints the figure the change moves and
/// guards only the floor under it.</para>
/// </summary>
public sealed class ExceptionBytesProbe
{
    private readonly ITestOutputHelper _out;
    public ExceptionBytesProbe(ITestOutputHelper o) => _out = o;

    private const int Events = 20_000;
    private const int Page   = 50;

    private static readonly string RootStack = string.Join('\n', Enumerable.Repeat(
        "   at Billing.Api.Orders.SettleAsync(OrderId id) in /src/Billing/Orders.cs:line 118", 14));
    private static readonly string InnerStack = string.Join('\n', Enumerable.Repeat(
        "   at System.Net.Sockets.Socket.Receive(Byte[] buffer)", 4));

    private static ExceptionInfo SampleException(int i) => new()
    {
        Type       = "System.InvalidOperationException",
        Message    = "order " + (i % 1000) + " could not be settled",
        StackTrace = RootStack,
        Inner = new ExceptionInfo
        {
            Type       = "System.Net.Sockets.SocketException",
            Message    = "connection reset by peer",
            StackTrace = InnerStack,
            Inner      = new ExceptionInfo { Type = "System.TimeoutException", Message = "the operation timed out" },
        },
    };

    /// <summary>The same exception as the wire carries it: the msgpack map <see cref="ExceptionInfo.Write"/> emits.</summary>
    private static byte[] SampleExceptionBytes(int i) => SampleException(i).ToBytes();

    // ── 1. Ingest: the CLEF decode, then the staging a request does ───────────

    private sealed class CountingSink : LogEventSerializer.IClefBatchSink
    {
        public int  Count;
        public long Touched;
        public bool TryIngestClef(
            long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, ReadOnlySpan<byte> exceptionMsgpack,
            ReadOnlySpan<byte> msgpackProps, ulong traceHi, ulong traceLo, ulong spanId,
            ReadOnlySpan<byte> serviceUtf8)
        {
            Count++;
            Touched += msgpackProps.Length + exceptionMsgpack.Length;
            return true;
        }
    }

    /// <summary>Stages into a <see cref="LogWriteBatch"/> exactly as <c>LogIngestBatch.Add</c> does.</summary>
    private sealed class StagingSink(LogWriteBatch batch, string template) : LogEventSerializer.IClefBatchSink
    {
        public bool TryIngestClef(
            long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, ReadOnlySpan<byte> exceptionMsgpack,
            ReadOnlySpan<byte> msgpackProps, ulong traceHi, ulong traceLo, ulong spanId,
            ReadOnlySpan<byte> serviceUtf8)
        {
            var h = new LogEventHeader { TimestampUtcTicks = tsTicks, Level = (LogLevel)level, MessageTemplatePoolIndex = 0, ServiceNamePoolIndex = 0 };
            batch.Add(h, msgpackProps, template, exceptionMsgpack);
            return true;
        }
    }

    [Fact]
    public void Ingest_ClefWithExceptions_BytesPerEvent()
    {
        const int Batch = 1_000, Iters = 20;
        byte[] body = BuildClefBatch(Batch);
        var    sink = new CountingSink();

        for (int i = 0; i < 5; i++) LogEventSerializer.StreamBatch(body, sink, out _);
        long b0 = GC.GetAllocatedBytesForCurrentThread();
        var  sw = Stopwatch.StartNew();
        for (int i = 0; i < Iters; i++) LogEventSerializer.StreamBatch(body, sink, out _);
        sw.Stop();
        long decodeBytes = (GC.GetAllocatedBytesForCurrentThread() - b0) / Iters;
        double decodeNs  = sw.Elapsed.TotalNanoseconds / Iters / Batch;

        // Staging: one request's batch, rented at the body's size as a request rents it.
        long stageBytes; double stageNs;
        {
            using var warm = new LogWriteBatch(body.Length);
            LogEventSerializer.StreamBatch(body, new StagingSink(warm, "failed {N}"), out _);
            warm.Dispose();
            long s0 = GC.GetAllocatedBytesForCurrentThread();
            sw.Restart();
            for (int i = 0; i < Iters; i++)
            {
                using var batch = new LogWriteBatch(body.Length);
                LogEventSerializer.StreamBatch(body, new StagingSink(batch, "failed {N}"), out _);
            }
            sw.Stop();
            stageBytes = (GC.GetAllocatedBytesForCurrentThread() - s0) / Iters;
            stageNs    = sw.Elapsed.TotalNanoseconds / Iters / Batch;
        }

        _out.WriteLine($"CLEF ingest, {Batch:N0} events each with a 3-deep @x ({body.Length / Batch:N0} B/event on the wire):");
        _out.WriteLine($"  decode into a counting sink : {decodeBytes / (double)Batch,8:F0} B/event  {decodeNs,8:F0} ns/event");
        _out.WriteLine($"  decode + LogWriteBatch stage: {stageBytes / (double)Batch,8:F0} B/event  {stageNs,8:F0} ns/event");
        Assert.Equal(Batch, sink.Count / 25);
    }

    private static byte[] BuildClefBatch(int count)
    {
        var buf = new ArrayBufferWriter<byte>(count * 2048);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(count);
        var ts = new DateTimeOffset(2026, 11, 5, 10, 20, 30, TimeSpan.Zero);
        for (int i = 0; i < count; i++)
        {
            w.WriteMapHeader(7);
            w.Write("@t");           w.Write(ts.AddMilliseconds(i).UtcDateTime.ToString("O"));
            w.Write("@l");           w.Write("Error");
            w.Write("@mt");          w.Write("Order {OrderId} failed for {Customer}");
            w.Write("@x");           w.WriteRaw(SampleExceptionBytes(i));
            w.Write("service.name"); w.Write("billing-api");
            w.Write("OrderId");      w.Write((long)i);
            w.Write("Customer");     w.Write("ACME GmbH (Frankfurt)");
        }
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    // ── 2. Hot tier: what the tier retains, what a page costs ─────────────────

    [Fact]
    public void HotTier_ExceptionEvents_RetainedAndPagedBytes()
    {
        var pool = new StringInternPool();
        using var warm = BuildExceptionTier(pool, 2_000);   // the pooled slot arrays and the JIT
        warm.Dispose();

        long heapBefore = GC.GetTotalMemory(forceFullCollection: true);
        using var hot = BuildExceptionTier(pool, Events);
        long retained = GC.GetTotalMemory(forceFullCollection: true) - heapBefore;
        var frozen = Array.Empty<HotTierSegment>();

        long PageBytes(bool deliver)
        {
            var sink = new ArrayBufferWriter<byte>(256 * 1024);
            using var json = new Utf8JsonWriter(sink);
            long before = GC.GetAllocatedBytesForCurrentThread();
            int n = 0;
            foreach (var ev in HotTierScan.ReadSorted(hot, frozen, pool, long.MinValue, long.MaxValue, null, null, forward: false, levels: null))
            {
                if (deliver)
                {
                    sink.ResetWrittenCount();
                    json.Reset(sink);
                    LogEventJsonWriter.Write(json, ev);
                    json.Flush();
                }
                if (++n >= Page) break;
            }
            return GC.GetAllocatedBytesForCurrentThread() - before;
        }

        PageBytes(false); PageBytes(true);
        long scanned   = PageBytes(false);
        long delivered = PageBytes(true);

        _out.WriteLine($"hot tier, {Events:N0} events each with a 3-deep @x; native payload {hot.AllocatedBytes / 1048576.0:F1} MB committed:");
        _out.WriteLine($"  managed heap retained by the tier : {retained / 1048576.0,8:F1} MB  {retained / (double)Events,8:F0} B/event");
        _out.WriteLine($"  page of {Page}, materialised only   : {scanned / 1024.0,8:F1} KB  {scanned / (double)Page,8:F0} B/row");
        _out.WriteLine($"  page of {Page}, written as JSON      : {delivered / 1024.0,8:F1} KB  {delivered / (double)Page,8:F0} B/row");
        Assert.True(scanned > 0);
    }

    // ── 3. Flush: the index build over an exception-bearing tier ──────────────

    [Fact]
    public void Flush_IndexBuildWithExceptions_BytesPerEvent()
    {
        var pool = new StringInternPool();
        using var hot = BuildExceptionTier(pool, Events);
        hot.Freeze();
        var order = SegmentWriter.ComputeSortOrder(hot);

        for (int i = 0; i < 2; i++) { using var b = new SegmentIndexBuilder(hot.Count); b.Build(hot, pool, order); }

        long b0 = GC.GetAllocatedBytesForCurrentThread();
        var  sw = Stopwatch.StartNew();
        using var builder = new SegmentIndexBuilder(hot.Count);
        builder.Build(hot, pool, order);
        sw.Stop();
        long buildBytes = GC.GetAllocatedBytesForCurrentThread() - b0;

        _out.WriteLine($"index build, {Events:N0} events each with a 3-deep @x:");
        _out.WriteLine($"  build : {buildBytes / 1048576.0:F1} MB  {buildBytes / (double)Events:F0} B/event  {sw.Elapsed.TotalMicroseconds / Events:F2} us/event");
        Assert.True(builder.BloomTermsAdded > 0);
    }

    // ── 4. Cold segment: a candidate row untouched, and one delivered as JSON ─

    [Fact]
    public async Task Segment_ExceptionRows_UntouchedAndDeliveredBytes()
    {
        const int Rows = 4_000;
        string path = Path.Combine(Path.GetTempPath(), $"exc-bytes-{Guid.NewGuid():N}.seg");
        try
        {
            var pool = new StringInternPool();
            using (var hot = BuildExceptionTier(pool, Rows))
            {
                hot.Freeze();
                var order = SegmentWriter.ComputeSortOrder(hot);
                using var w = new SegmentWriter(path);
                w.WriteEvents(hot, pool, order);
                w.Finalise(new NodeId(0), new SegmentId(11UL));
            }

            using var reader = SegmentReader.Open(path);

            async Task<long> Measure(bool deliver)
            {
                var sink = new ArrayBufferWriter<byte>(256 * 1024);
                using var json = new Utf8JsonWriter(sink);
                long before = GC.GetAllocatedBytesForCurrentThread();
                await foreach (var ev in reader.ReadEventsAsync(null, null, null))
                {
                    if (!deliver) continue;
                    sink.ResetWrittenCount();
                    json.Reset(sink);
                    LogEventJsonWriter.Write(json, ev);
                    json.Flush();
                }
                return GC.GetAllocatedBytesForCurrentThread() - before;
            }

            await Measure(false); await Measure(true);
            long untouched = await Measure(false);
            long delivered = await Measure(true);

            _out.WriteLine($"cold segment, {Rows:N0} rows each with a 3-deep @x:");
            _out.WriteLine($"  row read, exception untouched : {untouched / (double)Rows,8:F0} B/row");
            _out.WriteLine($"  row read + written as JSON    : {delivered / (double)Rows,8:F0} B/row");
            // Delivery goes from the bytes to JSON; the tree it used to build is ~3.5 KB a row.
            Assert.True(delivered - untouched < 1024L * Rows,
                $"delivering a row builds the tree again: {(delivered - untouched) / (double)Rows:F0} B/row on top of the read");
        }
        finally { File.Delete(path); }
    }

    // ── The tier ──────────────────────────────────────────────────────────────

    private static HotTierSegment BuildExceptionTier(StringInternPool pool, int events)
    {
        var hot = new HotTierSegment(events + 1, (long)events * 2048 + (1 << 20));
        int    tmplIdx = pool.Intern("Order {OrderId} failed for {Customer}");
        string tmpl    = pool.Get(tmplIdx);
        int    svcIdx  = pool.Intern("billing-api");
        long   baseTicks = DateTimeOffset.UtcNow.UtcTicks;

        var buf = new ArrayBufferWriter<byte>(128);
        for (int i = 0; i < events; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(2);
            w.Write("OrderId");  w.Write((long)i);
            w.Write("Customer"); w.Write("ACME GmbH (Frankfurt)");
            w.Flush();

            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i,
                Level                    = LogLevel.Error,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
            };
            // Its bytes, as ingest hands them on: the tier copies them into its arena.
            Assert.True(hot.TryWrite(h, buf.WrittenSpan, tmpl, SampleExceptionBytes(i)));
        }
        return hot;
    }
}
