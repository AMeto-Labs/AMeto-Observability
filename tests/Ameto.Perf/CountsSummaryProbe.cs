using System.Buffers;
using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using Ameto.Testing;
using MessagePack;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

// xUnit1031 is off for the same reason as in AggregationPathProbe: the timings below are taken
// around blocking waits on CPU-bound work with no synchronization context.
#pragma warning disable xUnit1031

/// <summary>
/// What the Statistics page's month-wide Logs band costs, on the two roads
/// <c>/api/events/counts</c> can take for a segment wholly inside the window:
///
/// <para>scan — every block of every segment LZ4-decoded for its timestamp, level and service
/// (all there was before 2026-11-09); summary — the segment's counts per 15-minute slot, service
/// and level, read once off the file's counts section and then held in memory; and the road a
/// segment written before summaries takes — scanned once, its summary kept.</para>
///
/// <para>Timings print; what is gated is structural: the answers are equal, and the summary
/// road opens no segment for a scan. On the production stand the scan road is also a month of
/// disk reads, which nothing in-process can show.</para>
/// </summary>
public sealed class CountsSummaryProbe
{
    private const int Events = 180_000;
    private const int Days   = 30;
    private const int FlushEvery = 30_000;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly string[] Services =
        ["checkout", "billing", "gateway", "Office.API", "identity", "ledger", "notify", "search"];

    private readonly ITestOutputHelper _out;
    public CountsSummaryProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void A_month_wide_window_reads_summaries_instead_of_blocks()
    {
        string root = Path.Combine(Path.GetTempPath(), "ameto-countsprobe-" + Guid.NewGuid().ToString("N"));
        string withDir = Path.Combine(root, "with"), legacyDir = Path.Combine(root, "legacy");
        try
        {
            using (var e = Open(withDir))   { Fill(e); }
            using (var e = Open(legacyDir)) { e.Engine._segmentsCarryCounts = false; Fill(e); }

            var from = Base.AddHours(-12);
            var to   = Base.AddDays(Days).AddHours(12);
            const int bucket = 43_200;   // the 1m preset's step

            using var with = Open(withDir);
            int segments = with.Engine.ListSegments().Count;

            // The scan is what the other roads are held against, so it is held against the corpus
            // first: a scan that saw part of the store would otherwise read as the roads disagreeing.
            var scanned = Count(with.Engine, from, to, bucket, summaries: false);
            Assert.Equal(Events, scanned.Total);
            var (scanMs, scanBytes) = Measure(3, () => Count(with.Engine, from, to, bucket, summaries: false));

            int opens = 0;
            with.Engine._beforeHeaderSegmentOpen = _ => Interlocked.Increment(ref opens);
            var (fileMs, fileBytes) = Measure(1, () => Count(with.Engine, from, to, bucket, summaries: true));
            var fromSummaries = Count(with.Engine, from, to, bucket, summaries: true);
            var (warmMs, warmBytes) = Measure(3, () => Count(with.Engine, from, to, bucket, summaries: true));
            Assert.Equal(0, opens);
            AssertSame(scanned, fromSummaries);
            Assert.Equal(Events, fromSummaries.Total);

            using var legacy = Open(legacyDir);
            var (firstMs, firstBytes)  = Measure(1, () => Count(legacy.Engine, from, to, bucket, summaries: true));
            var (againMs, againBytes)  = Measure(3, () => Count(legacy.Engine, from, to, bucket, summaries: true));
            AssertSame(scanned, Count(legacy.Engine, from, to, bucket, summaries: true));

            _out.WriteLine($"{Events:N0} events over {Days} days in {segments} segments; window {from:yyyy-MM-dd} to {to:yyyy-MM-dd}, 12 h buckets");
            _out.WriteLine($"scan, every block decoded       : {scanMs,8:F1} ms | {scanBytes  / 1048576.0,7:F2} MB allocated");
            _out.WriteLine($"summaries, first read off files : {fileMs,8:F1} ms | {fileBytes  / 1048576.0,7:F2} MB allocated");
            _out.WriteLine($"summaries, from memory          : {warmMs,8:F1} ms | {warmBytes  / 1048576.0,7:F2} MB allocated");
            _out.WriteLine($"pre-summary segments, first     : {firstMs,8:F1} ms | {firstBytes / 1048576.0,7:F2} MB allocated (scanned, summaries kept)");
            _out.WriteLine($"pre-summary segments, then      : {againMs,8:F1} ms | {againBytes / 1048576.0,7:F2} MB allocated");
            _out.WriteLine($"gain from memory                : {scanMs / Math.Max(warmMs, 0.001),6:F0}x");
        }
        finally
        {
            // Two stores of 180 000 events each. Not a bare delete under an empty catch: the reopened
            // engines scan their catalogs on a task their dispose does not wait for, and a fact that
            // ends early — a failed assertion — meets that scan with one segment still open.
            TestDataDirectory.Delete(root);
        }
    }

    /// <summary>
    /// What the comparison above stands on: a store this probe reopens is handed over, and so
    /// counted, only once its catalog is whole.
    ///
    /// <para>The engine reads its segments directory in the background and serves whatever that
    /// boot scan has registered so far (<see cref="StorageEngine.CatalogLoaded"/>), so two counts
    /// taken straight off the constructor are counts of two different catalogs. That is how this
    /// probe came to report the block scan at 45 000 and the summaries at 180 000 over the same
    /// 24 files: in the run that was measured, the first count began a millisecond after the
    /// constructor returned, with 3 of the files registered, and the boot scan ended 20 ms later.
    /// Which of the two came first was the machine's to decide: the boot scan on the CI runners,
    /// in every run so far; the count on a developer's machine, in every run on one day and in
    /// three of ten on the next.</para>
    ///
    /// <para>Here the boot scan is held back for half a second, far longer than a reopening
    /// takes, so <see cref="Open"/> returns before the scan has begun on every run unless it
    /// waits the scan out. Without the wait this fails on every run: the store is still
    /// <see cref="QueryAvailability.Loading"/> where <see cref="Open"/> checks it, and its catalog
    /// holds none of the 8 segments. (The half second runs from the hold being made, before the
    /// engine is constructed: a reopening that took all of it would let a missing wait pass. No
    /// timing fails this with the wait in place.)</para>
    ///
    /// <para>It reads the catalog and takes no count, on purpose. xUnit may run this fact first,
    /// and the timings above are of a process as cold as the month-wide fact finds it: a block
    /// scan taken here had that fact print its own scan at 28 ms where it prints 17, and a
    /// summaries count taken here, at 20.</para>
    /// </summary>
    [Fact]
    public void A_reopened_store_is_handed_over_only_once_its_catalog_is_whole()
    {
        const int events = 60_000;   // two flushes: a segment per level from each
        string dir = Path.Combine(Path.GetTempPath(), "ameto-countsprobe-" + Guid.NewGuid().ToString("N"));
        try
        {
            using (var e = Open(dir)) { Fill(e, events); }
            int onDisk = Directory.GetFiles(Path.Combine(dir, "segments"), "*.seg").Length;

            using var reopened = Open(dir, bootScanHeldUntil: Task.Delay(TimeSpan.FromMilliseconds(500)));
            var catalog = reopened.Engine.ListSegments();
            Assert.Equal(onDisk, catalog.Count);
            Assert.Equal(events, catalog.Sum(s => (long)s.EventCount));
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { }
        }
    }

    /// <summary>
    /// What the writer pays for the summary: one dictionary probe per row on the flush and merge
    /// roads, and memory that grows with the segment's slots × services × levels, not its rows.
    /// </summary>
    [Fact]
    public void The_writer_pays_little_for_the_summary()
    {
        const int rows = 130_000;   // a full 64 MB tier's worth of small events
        var pool = new StringInternPool();
        using var hot = Tier(pool, rows);
        string dir = Path.Combine(Path.GetTempPath(), "ameto-countswriter-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            int[] order = SegmentWriter.ComputeSortOrder(hot);
            Write(dir, hot, pool, order, counts: true);    // JIT warm-up, both ways
            Write(dir, hot, pool, order, counts: false);

            var (withMs,    withBytes)    = MeasureOnThread(3, () => Write(dir, hot, pool, order, counts: true));
            var (withoutMs, withoutBytes) = MeasureOnThread(3, () => Write(dir, hot, pool, order, counts: false));
            long section = Write(dir, hot, pool, order, counts: true) - Write(dir, hot, pool, order, counts: false);

            _out.WriteLine($"{rows:N0} rows over {TierSpan.TotalHours:F0} h, {Services.Length} services");
            _out.WriteLine($"write without summary : {withoutMs,7:F1} ms | {withoutBytes / 1024.0,8:F1} KB allocated");
            _out.WriteLine($"write with summary    : {withMs,7:F1} ms | {withBytes / 1024.0,8:F1} KB allocated");
            _out.WriteLine($"section on disk       : {section / 1024.0,7:F1} KB");

            // Structural: the summary's memory is cells, not rows. 130 000 rows in a few hundred
            // cells must not cost anything near a row's worth of bytes each.
            Assert.True(withBytes - withoutBytes < 256 * 1024,
                $"the summary allocated {withBytes - withoutBytes} B on top of the write");
        }
        finally
        {
            TestDataDirectory.Delete(dir);
        }
    }

    private static readonly TimeSpan TierSpan = TimeSpan.FromHours(6);

    private static long Write(string dir, HotTierSegment hot, StringInternPool pool, int[] order, bool counts)
    {
        string path = Path.Combine(dir, Guid.NewGuid().ToString("N") + ".seg");
        using (var w = new SegmentWriter(path) { WritesCountsSummary = counts })
        {
            w.WriteEvents(hot, pool, order);
            w.Finalise(new NodeId(1), new SegmentId(1));
        }
        long length = new FileInfo(path).Length;
        File.Delete(path);
        return length;
    }

    private static HotTierSegment Tier(StringInternPool pool, int rows)
    {
        var hot  = new HotTierSegment(rows + 1, (long)rows * 256 + 1024 * 1024);
        int tmpl = pool.Intern("HTTP {Method} {Route} responded {Status} in {Elapsed} ms");
        var svc  = Services.Select(s => pool.Intern(s)).ToArray();
        long step = TierSpan.Ticks / rows;
        var buf  = new ArrayBufferWriter<byte>(128);
        for (int i = 0; i < rows; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(2);
            w.Write("Status");  w.Write(i % 41 == 0 ? 500 : 200);
            w.Write("Elapsed"); w.Write((i % 400) + 0.25);
            w.Flush();
            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = Base.UtcTicks + i * step,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = tmpl,
                ServiceNamePoolIndex     = svc[i % svc.Length],
            };
            Assert.True(hot.TryWrite(h, buf.WrittenSpan, pool.Get(tmpl), null));
        }
        hot.Freeze();
        return hot;
    }

    /// <summary>Best of <paramref name="runs"/>, allocations on THIS thread: the writer runs on the caller's.</summary>
    private static (double Ms, long Bytes) MeasureOnThread(int runs, Action run)
    {
        double best = double.MaxValue;
        long bytes  = 0;
        for (int r = 0; r < runs; r++)
        {
            long b0 = GC.GetAllocatedBytesForCurrentThread();
            var sw = Stopwatch.StartNew();
            run();
            sw.Stop();
            long b1 = GC.GetAllocatedBytesForCurrentThread();
            if (sw.Elapsed.TotalMilliseconds < best) { best = sw.Elapsed.TotalMilliseconds; bytes = b1 - b0; }
        }
        return (best, bytes);
    }

    private sealed class Store(StorageEngine engine) : IDisposable
    {
        public StorageEngine Engine { get; } = engine;
        public void Dispose() { try { Engine.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { } }
    }

    /// <summary>
    /// A store over <paramref name="dir"/>, handed over once its boot scan has ended. Until then
    /// the catalog holds the segments the scan has reached so far, and every read is of those:
    /// see <see cref="A_reopened_store_is_handed_over_only_once_its_catalog_is_whole"/>, which is
    /// what <paramref name="bootScanHeldUntil"/> is for — the scan starts only when it completes.
    /// The engine is otherwise the public constructor's.
    ///
    /// <para>A scan can also end short: a file it could not open for a reason that is not the
    /// file's bytes stays out of the catalog and leaves the store
    /// <see cref="QueryAvailability.Degraded"/>. That fails here, under its own name, where a
    /// count taken of such a store would fail later as two roads that disagree.</para>
    /// </summary>
    private static Store Open(string dir, Task? bootScanHeldUntil = null)
    {
        Directory.CreateDirectory(dir);
        var opts = new ServerOptions { DataDirectory = dir };
        var engine = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance,
            MemoryBudgets.Current(), bootScanHeldUntil: bootScanHeldUntil);
        engine.IndexSinkFactory = static (c, t) => new SegmentIndexBuilder(c, 5, t);
        var store = new Store(engine);
        try
        {
            engine.CatalogLoaded.GetAwaiter().GetResult();
            Assert.Equal(QueryAvailability.Available, engine.Availability);
        }
        catch { store.Dispose(); throw; }   // a scan that faulted or ended short: the store is not handed over, so it is closed here
        return store;
    }

    /// <summary>A realistic event: a handful of properties, a service, a level mix weighted to Information.</summary>
    private static void Fill(Store store, int events = Events)
    {
        var engine = store.Engine;
        long step = TimeSpan.FromDays(Days).Ticks / events;
        LogLevel[] levels = [LogLevel.Information, LogLevel.Information, LogLevel.Information, LogLevel.Debug,
                             LogLevel.Warning, LogLevel.Information, LogLevel.Error, LogLevel.Information];
        int tmpl = engine.TemplatePool.Intern("HTTP {Method} {Route} responded {Status} in {Elapsed} ms");
        var svc  = Services.Select(s => engine.TemplatePool.Intern(s)).ToArray();
        var buf  = new ArrayBufferWriter<byte>(256);
        for (int i = 0; i < events; i++)
        {
            if (i > 0 && i % FlushEvery == 0) engine.FlushHotTierAsync().GetAwaiter().GetResult();
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(4);
            w.Write("Method");  w.Write(i % 3 == 0 ? "POST" : "GET");
            w.Write("Route");   w.Write("/api/orders/" + (i % 97));
            w.Write("Status");  w.Write(i % 41 == 0 ? 500 : 200);
            w.Write("Elapsed"); w.Write((i % 400) + 0.25);
            w.Flush();
            Assert.True(engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = Base.UtcTicks + i * step,
                Level                    = levels[i % levels.Length],
                MessageTemplatePoolIndex = tmpl,
                ServiceNamePoolIndex     = svc[i % svc.Length],
            }, buf.WrittenSpan.ToArray()));
        }
        engine.FlushHotTierAsync().GetAwaiter().GetResult();
    }

    private static LogVolumeCounts Count(StorageEngine engine, DateTimeOffset from, DateTimeOffset to, int bucket, bool summaries)
    {
        engine._countSummaries = summaries;
        try
        {
            long minBucket = from.ToUnixTimeSeconds() / bucket;
            int  n         = (int)(to.ToUnixTimeSeconds() / bucket - minBucket + 1);
            return engine.AggregateLogVolumeAsync(from, to, minBucket, bucket, n, null).GetAwaiter().GetResult();
        }
        finally { engine._countSummaries = true; }
    }

    private static (double Ms, long Bytes) Measure(int runs, Action run)
    {
        double best = double.MaxValue;
        long bytes  = 0;
        for (int r = 0; r < runs; r++)
        {
            long b0 = GC.GetTotalAllocatedBytes(precise: true);
            var sw = Stopwatch.StartNew();
            run();
            sw.Stop();
            long b1 = GC.GetTotalAllocatedBytes(precise: true);
            if (sw.Elapsed.TotalMilliseconds < best) { best = sw.Elapsed.TotalMilliseconds; bytes = b1 - b0; }
        }
        return (best, bytes);
    }

    private static void AssertSame(LogVolumeCounts a, LogVolumeCounts b)
    {
        Assert.Equal(a.Total, b.Total);
        Assert.Equal(a.Scanned, b.Scanned);
        Assert.Equal(a.Levels.Count, b.Levels.Count);
        for (int i = 0; i < a.Levels.Count; i++)
        {
            Assert.Equal(a.Levels[i].Name, b.Levels[i].Name);
            Assert.Equal(a.Levels[i].Points, b.Levels[i].Points);
        }
        var sa = a.Services.ToDictionary(s => s.Name, s => s.Points, StringComparer.OrdinalIgnoreCase);
        var sb = b.Services.ToDictionary(s => s.Name, s => s.Points, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(sa.Count, sb.Count);
        foreach (var (name, points) in sa) Assert.Equal(points, sb[name]);
    }
}
