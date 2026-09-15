using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Quantifies managed allocations of the columnar segment writer for one flushed tier.
/// The per-block scratch is instance-reused, so writing ~250 blocks must allocate on
/// the order of one block's scratch — not hundreds of MB of per-block arrays/streams
/// (the pre-reuse behaviour that slowed flushes into the back-pressure budget and
/// surfaced as ingest drops at 100k logs/s).
/// </summary>
public sealed class SegmentWriterAllocProbe
{
    private const int Events = 50_000;

    private readonly ITestOutputHelper _out;
    public SegmentWriterAllocProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void WriteEvents_AllocatesConstantScratch()
    {
        var pool = new StringInternPool();
        using var hot = BuildTier(pool);
        var order = SegmentWriter.ComputeSortOrder(hot);

        string warmPath = Path.Combine(Path.GetTempPath(), $"swp-warm-{Guid.NewGuid():N}.seg");
        string path     = Path.Combine(Path.GetTempPath(), $"swp-{Guid.NewGuid():N}.seg");
        try
        {
            // Warm-up (JIT + ArrayPool).
            using (var w = new SegmentWriter(warmPath)) { w.WriteEvents(hot, pool, order); w.Finalise(new NodeId(0), new SegmentId(1UL)); }

            long before = GC.GetAllocatedBytesForCurrentThread();
            var sw = System.Diagnostics.Stopwatch.StartNew();
            using (var w = new SegmentWriter(path))
            {
                w.WriteEvents(hot, pool, order);
                w.Finalise(new NodeId(0), new SegmentId(2UL));
            }
            sw.Stop();
            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            _out.WriteLine($"events={Events}, allocated={allocated / 1024.0:F1} KB, "
                         + $"{sw.Elapsed.TotalMilliseconds:F1} ms, "
                         + $"{sw.Elapsed.TotalMilliseconds * 1000.0 / Events:F3} µs/event");

            // Pre-reuse this was ~100 MB+ per tier (per-block arrays, five MemoryStreams,
            // ToArray of every block, Stream.CopyTo buffers). With reused scratch the
            // whole tier should stay within a few MB.
            Assert.True(allocated < 8 * 1024 * 1024,
                $"segment write allocated {allocated} bytes — per-block scratch is leaking again");
        }
        finally
        {
            File.Delete(warmPath);
            File.Delete(path);
        }
    }

    /// <summary>
    /// The same write on a tier of EXCEPTION-carrying events — the shape a level-split flush
    /// gives the Error segment, which is 100 % of them.
    ///
    /// <para>The hot tier holds an exception as an object graph, so this is the one column the
    /// writer has to serialise itself. Doing it through <c>ExceptionInfo.ToBytes()</c> built an
    /// ArrayBufferWriter per event, grew it (a fresh array per doubling), copied the result out
    /// with ToArray() and copied THAT into the column — four allocations and two copies of a
    /// blob that is mostly stack trace.</para>
    /// </summary>
    [Fact]
    public void WriteEvents_WithExceptions_AllocatesConstantScratch()
    {
        var pool = new StringInternPool();
        using var hot = BuildExceptionTier(pool);
        var order = SegmentWriter.ComputeSortOrder(hot);

        string warmPath = Path.Combine(Path.GetTempPath(), $"swp-exc-warm-{Guid.NewGuid():N}.seg");
        string path     = Path.Combine(Path.GetTempPath(), $"swp-exc-{Guid.NewGuid():N}.seg");
        try
        {
            using (var w = new SegmentWriter(warmPath)) { w.WriteEvents(hot, pool, order); w.Finalise(new NodeId(0), new SegmentId(1UL)); }

            long before = GC.GetAllocatedBytesForCurrentThread();
            var sw = System.Diagnostics.Stopwatch.StartNew();
            using (var w = new SegmentWriter(path))
            {
                w.WriteEvents(hot, pool, order);
                w.Finalise(new NodeId(0), new SegmentId(2UL));
            }
            sw.Stop();
            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            _out.WriteLine($"events={ExceptionEvents}, allocated={allocated / 1024.0:F1} KB, "
                         + $"{sw.Elapsed.TotalMilliseconds:F1} ms, "
                         + $"{allocated / (double)ExceptionEvents:F0} B/event");

            // A per-event serialisation buffer for a ~1 KB exception is ~4 KB/event of garbage.
            // Serialised into the column's own scratch it is none.
            Assert.True(allocated < 8 * 1024 * 1024,
                $"exception-column write allocated {allocated} bytes — the exception blob is being built per event again");
        }
        finally
        {
            File.Delete(warmPath);
            File.Delete(path);
        }
    }

    private const int ExceptionEvents = 20_000;

    private static HotTierSegment BuildExceptionTier(StringInternPool pool)
    {
        var hot = new HotTierSegment(ExceptionEvents + 1, (long)ExceptionEvents * 2048 + 1024 * 1024);

        int    tmplIdx   = pool.Intern("request failed");
        string tmpl      = pool.Get(tmplIdx);
        int    svcIdx    = pool.Intern("Wallet.API");
        long   baseTicks = DateTimeOffset.UtcNow.UtcTicks;

        // A realistic stack: ~20 frames of ~48 chars, i.e. about a kilobyte of UTF-16.
        var frames = new System.Text.StringBuilder(1024);
        for (int f = 0; f < 20; f++)
            frames.Append("   at Ameto.Wallet.Payments.Handler.Step").Append(f).Append("(Int32 n)\n");
        string stack = frames.ToString();

        var buf = new ArrayBufferWriter<byte>(256);
        for (int i = 0; i < ExceptionEvents; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1);
            w.Write("orderId"); w.Write((long)i);
            w.Flush();

            var exc = new ExceptionInfo
            {
                Type       = "System.InvalidOperationException",
                Message    = "payment " + i + " could not be settled",
                StackTrace = stack,
                Inner      = new ExceptionInfo { Type = "System.TimeoutException", Message = "upstream timed out" },
            };

            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i,
                Level                    = Ameto.Core.LogLevel.Error,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
            };
            Assert.True(hot.TryWrite(h, buf.WrittenSpan, tmpl, exc));
        }
        hot.Freeze();
        return hot;
    }

    private static HotTierSegment BuildTier(StringInternPool pool)
    {
        var hot = new HotTierSegment(Events + 1, (long)Events * 512 + 1024 * 1024);

        int    tmplIdx = pool.Intern("HTTP request handled");
        string tmpl    = pool.Get(tmplIdx);
        int    svcIdx  = pool.Intern("Wallet.API");
        long   baseTicks = DateTimeOffset.UtcNow.UtcTicks;

        var buf = new ArrayBufferWriter<byte>(256);
        for (int i = 0; i < Events; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(4);
            w.Write("orderId");    w.Write((long)i);
            w.Write("customerId"); w.Write("cust-" + (i % 500));
            w.Write("route");      w.Write("/api/pay");
            w.Write("duration");   w.Write((i % 400) + 0.5);
            w.Flush();

            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i,
                Level                    = Ameto.Core.LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
                TraceIdHi                = (ulong)i,
                TraceIdLo                = (ulong)i + 1,
                SpanId                   = (ulong)i + 2,
            };
            Assert.True(hot.TryWrite(h, buf.WrittenSpan, tmpl));
        }
        hot.Freeze();
        return hot;
    }
}
