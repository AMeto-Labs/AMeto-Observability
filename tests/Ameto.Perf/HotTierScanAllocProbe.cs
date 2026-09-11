using System.Buffers;
using System.Diagnostics;
using MessagePack;
using Ameto.Core;
using Ameto.Query.Filtering;
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

        // Threshold recalibrated from 10x to 4x when materialisation stopped decoding
        // properties into a dictionary (LogEvent.Properties is lazy; decoders carry the
        // msgpack through). That made the BASELINE 2.9x cheaper — 10.7 MB -> 3.7 MB for
        // 20 000 events — while the page path barely moved (653 KB -> 635 KB), so the
        // measured gap fell to ~5.9x without the page path regressing. The guard still
        // catches the thing it was written for: a page query that materialises the tier.
        Assert.True(newBytes * 4 < oldBytes,
            $"expected ≥4x reduction, got old={oldBytes} new={newBytes}");
    }

    // ── Filtered page: header pushdown vs. materialise-then-filter ─────────────

    private const int FilteredEvents = 200_000;

    /// <summary>
    /// A filtered page over a large tier. Without header pushdown every in-window hot
    /// event is materialised (LogEvent + payload copy + pool lookups) only for the
    /// evaluator to reject 99 % of them; with it, a filter the header can answer costs
    /// one header read per rejected event and a materialisation per MATCH.
    /// </summary>
    [Theory]
    [InlineData("@l = 'Error'")]
    [InlineData("@tr = '0123456789abcdef0123456789abcdef'")]
    [InlineData("service.name = 'Svc.B' and @l = 'Error'")]
    [InlineData("@l = 'Error' and Route = '/api/pay'")]
    public void FilteredPage_HeaderPushdown(string expression)
    {
        var pool = new StringInternPool();
        using var hot = BuildFilteredTier(pool, FilteredEvents);
        var frozen = Array.Empty<HotTierSegment>();
        var filter = CompiledFilter.Compile(expression);

        // The hot source exactly as QueryExecutor.HotEventsAsync consumes it: header
        // scan, then the compiled filter per materialised event, first page only.
        (long bytes, double ms, int matched) Run(bool pushdown)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            var  sw     = Stopwatch.StartNew();
            int  n      = 0;
            foreach (var ev in HotTierScan.ReadSorted(
                         hot, frozen, pool,
                         long.MinValue, long.MaxValue, null, null, forward: false, levels: null,
                         headerPredicate: pushdown ? filter.HeaderPredicate : null))
            {
                if (!filter.Matches(ev)) continue;
                if (++n >= Page) break;
            }
            sw.Stop();
            return (GC.GetAllocatedBytesForCurrentThread() - before, sw.Elapsed.TotalMilliseconds, n);
        }

        Run(false); Run(true);   // warm-up

        var old = Run(false);
        var neu = Run(true);
        Assert.Equal(old.matched, neu.matched);

        _out.WriteLine($"filter=\"{expression}\" events={FilteredEvents} page={Page} matched={neu.matched}");
        _out.WriteLine($"no pushdown : {old.bytes / 1024.0:F1} KB, {old.ms:F2} ms");
        _out.WriteLine($"pushdown    : {neu.bytes / 1024.0:F1} KB, {neu.ms:F2} ms  " +
                       $"({(double)old.bytes / Math.Max(1, neu.bytes):F1}x less memory, {old.ms / Math.Max(0.001, neu.ms):F1}x faster)");

        // Allocation must be proportional to the page, not to the rejected candidates.
        Assert.True(neu.bytes * 4 < old.bytes,
            $"expected ≥4x fewer bytes with pushdown, got old={old.bytes} new={neu.bytes}");
    }

    // ── Tail poll: zone map + lazy ordering ────────────────────────────────────

    private const int TailEvents = 300_000;

    /// <summary>
    /// A live-tail poll: forward, cursor at the last ~1 % of the tier, one page. The old
    /// scan walked every header twice and full-sorted the candidates; the zone map lets
    /// it touch the chunks that can hold the window, and the page comes off a heap.
    /// </summary>
    [Fact]
    public void TailPoll_TouchesOnlyChunksInWindow()
    {
        var pool = new StringInternPool();
        using var hot = BuildFilteredTier(pool, TailEvents);
        var frozen = Array.Empty<HotTierSegment>();

        long baseTicks = hot.GetHeader(0).TimestampUtcTicks;
        long cursorTs  = baseTicks + (long)(TailEvents * 0.99);
        ulong cursorId = hot.GetHeader((int)(TailEvents * 0.99)).Id;

        (long bytes, double ms, int n) Poll()
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            var  sw     = Stopwatch.StartNew();
            int  n      = 0;
            foreach (var ev in HotTierScan.ReadSorted(
                         hot, frozen, pool,
                         cursorTs, long.MaxValue, cursorTs, cursorId, forward: true, levels: null))
            {
                if (++n >= Page) break;
            }
            sw.Stop();
            return (GC.GetAllocatedBytesForCurrentThread() - before, sw.Elapsed.TotalMilliseconds, n);
        }

        Poll(); Poll();   // warm-up
        double bestMs = double.MaxValue; long bytes = 0; int n = 0;
        for (int i = 0; i < 5; i++)
        {
            var r = Poll();
            bestMs = Math.Min(bestMs, r.ms); bytes = r.bytes; n = r.n;
        }
        Assert.Equal(Page, n);
        _out.WriteLine($"tail poll: events={TailEvents} page={Page} -> {bytes / 1024.0:F1} KB, best {bestMs:F3} ms");
    }

    /// <summary>1 % Error, three services round-robin, one event with a known trace id.</summary>
    private static HotTierSegment BuildFilteredTier(StringInternPool pool, int events)
    {
        var hot = new HotTierSegment(events + 1, (long)events * 128 + 1024 * 1024);

        int    tmplIdx = pool.Intern("HTTP request handled");
        string tmpl    = pool.Get(tmplIdx);
        int[]  svc     = [pool.Intern("Svc.A"), pool.Intern("Svc.B"), pool.Intern("Svc.C")];
        long   baseTicks = DateTimeOffset.UtcNow.UtcTicks;

        var buf = new ArrayBufferWriter<byte>(128);
        for (int i = 0; i < events; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(2);
            w.Write("OrderId"); w.Write((long)i);
            w.Write("Route");   w.Write("/api/pay");
            w.Flush();

            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i,
                Level                    = i % 100 == 7 ? Ameto.Core.LogLevel.Error : Ameto.Core.LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svc[i % 3],
                TraceIdHi                = i == events / 2 ? 0x0123456789abcdefUL : 0,
                TraceIdLo                = i == events / 2 ? 0x0123456789abcdefUL : 0,
            };
            Assert.True(hot.TryWrite(h, buf.WrittenSpan, tmpl));
        }
        hot.Freeze();
        return hot;
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
