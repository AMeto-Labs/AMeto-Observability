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
            using (var w = new SegmentWriter(path))
            {
                w.WriteEvents(hot, pool, order);
                w.Finalise(new NodeId(0), new SegmentId(2UL));
            }
            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            _out.WriteLine($"events={Events}, allocated={allocated / 1024.0:F1} KB");

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
