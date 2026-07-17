using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Quantifies the allocation win of the header-level hot-tier scan: a page query
/// (limit 50, newest-first — the events list view / live poll) must allocate
/// proportionally to the PAGE, not to the tier size.
/// </summary>
public sealed class HotTierScanAllocProbe
{
    private const int Events = 20_000;
    private const int Page   = 50;

    private readonly ITestOutputHelper _out;
    public HotTierScanAllocProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void PageQuery_AllocatesFractionOfFullMaterialisation()
    {
        var pool = new StringInternPool();
        using var hot = BuildTier(pool);
        var frozen = Array.Empty<HotTierSegment>();

        // Old query path: materialise everything, LINQ-filter + sort, take a page.
        long Old()
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            var page = hot.ReadAll(pool)
                .OrderByDescending(e => e.Timestamp).ThenByDescending(e => e.Id)
                .Take(Page)
                .Count();
            Assert.Equal(Page, page);
            return GC.GetAllocatedBytesForCurrentThread() - before;
        }

        // New path: header scan + sort, lazy materialisation of the page only.
        long New()
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            int n = 0;
            foreach (var ev in HotTierScan.ReadSorted(
                         hot, frozen, pool,
                         long.MinValue, long.MaxValue, null, null, forward: false, levels: null))
            {
                if (++n >= Page) break;
            }
            Assert.Equal(Page, n);
            return GC.GetAllocatedBytesForCurrentThread() - before;
        }

        // Warm-up.
        Old(); New();

        long oldBytes = Old();
        long newBytes = New();

        _out.WriteLine($"events={Events}, page={Page}");
        _out.WriteLine($"old path : {oldBytes / 1024.0:F1} KB allocated");
        _out.WriteLine($"new path : {newBytes / 1024.0:F1} KB allocated  ({(double)oldBytes / newBytes:F1}x less)");

        Assert.True(newBytes * 10 < oldBytes,
            $"expected ≥10x reduction, got old={oldBytes} new={newBytes}");
    }

    private static HotTierSegment BuildTier(StringInternPool pool)
    {
        var hot = new HotTierSegment(Events + 1, (long)Events * 512 + 1024 * 1024);

        int    tmplIdx = pool.Intern("HTTP request handled");
        string tmpl    = pool.Get(tmplIdx);
        int    svcIdx  = pool.Intern("Svc.A");
        long   baseTicks = DateTimeOffset.UtcNow.UtcTicks;

        var buf = new ArrayBufferWriter<byte>(256);
        for (int i = 0; i < Events; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(3);
            w.Write("orderId");    w.Write((long)i);
            w.Write("customerId"); w.Write("cust-" + (i % 500));
            w.Write("route");      w.Write("/api/pay");
            w.Flush();

            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i,
                Level                    = Ameto.Core.LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
            };
            Assert.True(hot.TryWrite(h, buf.WrittenSpan, tmpl));
        }
        hot.Freeze();
        return hot;
    }
}
