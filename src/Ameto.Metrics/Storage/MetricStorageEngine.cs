using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using K4os.Compression.LZ4;
using MessagePack;
using Microsoft.Extensions.Logging;
using Ameto.Core;

namespace Ameto.Metrics.Storage;

/// <summary>
/// In-memory metric storage with periodic flush to <c>.mts</c> files.
///
/// <para>
/// Hot tier: <c>ConcurrentDictionary&lt;SeriesKey, Series&gt;</c> where
/// <see cref="SeriesKey"/> = (name, labels). Points are appended in chronological order.
/// </para>
///
/// <para>
/// Flush policy: every <c>MetricsOptions.FlushCheckInterval</c> (default 60 s)
/// <em>or</em> when the hot tier passes <c>MetricsOptions.HotTierBytes</c> — BYTES, because a
/// 16-bucket histogram point is 4.3x a scalar one and a point count cannot bound memory.
/// Flushed data is written as a <c>.mts</c> LZ4+msgpack file (see <see cref="MetricWriter"/>).
/// </para>
///
/// <para>
/// Rollup: a background pass converts raw-resolution cold files older than 1 hour
/// into 5-minute-granularity aggregates, and files older than 24 hours into
/// 1-hour-granularity aggregates. Raw files are deleted after rollup.
/// </para>
/// </summary>
public sealed class MetricStorageEngine : IMetricIngester, IMetricQuery, IMetricCatalog, IMetricExemplars, IRetentionTarget, IMemoryShedder, IAsyncDisposable
{
    // ── Configuration ─────────────────────────────────────────────────────────
    //
    // EVERY CEILING BELOW USED TO BE A LITERAL, identical on a 512 MB container and a 64 GB
    // host, and nothing in this module consulted MemoryBudgets at all. They are now read from
    // MetricsOptions, whose defaults are min(what they have always been, a share of what this
    // process may use) — so a host large enough for the caps flushes on exactly the cadence it
    // always did, and a 512 MB one gets a tier it can hold. See MetricsOptions.

    private readonly MetricsOptions _options;

    /// <summary>
    /// BYTES, not points, before a flush is forced — see <see cref="EstimatedPointBytes"/>. The
    /// default is 500 000 scalar points restated in bytes, which is what the threshold has always
    /// been; a 16-bucket histogram point is 4.3x a scalar one and now costs 4.3x of it.
    /// </summary>
    private readonly long _hotFlushBytes;

    /// <summary>
    /// The bar a PERIODIC tick clears to write files at all. Below it the tier keeps
    /// accumulating: the points are already durable in the log, and a file per metric name is not
    /// worth writing for a handful of them. The age bound still lands a trickle on disk so it
    /// becomes eligible for rollup and retention.
    /// </summary>
    private readonly long _minFlushBytes;

    /// <summary>
    /// How often a tick asks whether the tier has earned its files. A CHECK interval and not a
    /// flush interval — durability belongs to the log. It used to be a flush interval, sized at
    /// 60 s purely to bound crash loss to about a minute, and since a flush writes one .mts PER
    /// METRIC NAME a 40-instrument deployment paid 40 files a minute for that.
    /// </summary>
    private readonly TimeSpan _flushCheckInterval;

    /// <summary>
    /// How long the tier may hold points before a flush is due whatever its size. One hour
    /// matches the rollup's own first cutoff, so nothing waits longer because of this.
    /// </summary>
    private readonly TimeSpan _maxHotAge;

    /// <summary>Distinct values remembered per label key, per metric — bounds catalog memory.</summary>
    private readonly int _maxLabelValuesPerKey;

    /// <summary>Distinct label-set hashes counted per metric before cardinality stops rising.</summary>
    private readonly int _maxTrackedSeriesPerMetric;

    /// <summary>
    /// How far into the future a point's client-supplied timestamp may reach before it is
    /// dropped instead of stored. Generous on purpose: real clock trouble — NTP drift, a
    /// client stamping local time as UTC — stays under a day, and such points must keep
    /// flowing. What this exists to stop is garbage: the poisoned-WAL incident replayed a
    /// torn entry whose timestamp decoded to the year 2116, and once flushed it became a
    /// cold file's MaxNano — a file the rollup never selects (`MaxNano < cutoff` false for
    /// ninety years), retention never expires (same comparison), and every query scans (its
    /// range overlaps every window). One bad point, three immortalities. The guard sits at
    /// the hot tier's two entrances, because MaxNano is computed from whatever got in.
    /// </summary>
    private static readonly long MaxFutureSkewNanos = (long)TimeSpan.FromHours(24).TotalSeconds * 1_000_000_000L;

    private long _futureDroppedTotal;      // lifetime count, carried in every warning
    private long _futureDropLastLogTicks;  // Environment.TickCount64 of the last warning; 0 = never

    /// <summary>
    /// The refusal boundary, computed in one place: ingest and WAL recovery must agree on it to
    /// the nanosecond, and two hand-expanded copies of the formula were one edited unit away
    /// from quietly disagreeing.
    /// </summary>
    private static long FutureLimitNanos()
        => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L + MaxFutureSkewNanos;

    /// <summary>When the hot tier last went from empty to holding points. Null = empty.</summary>
    private DateTime? _hotSince;

    /// <summary>
    /// Write-ahead log for the hot tier. Every point lands here before it is visible to a
    /// query, so an unflushed tier survives a crash without a file per metric per minute.
    /// </summary>
    private readonly MetricWriteAheadLog _wal;

    /// <summary>The log itself, for tests that need its fault-injection seam. See BeforeResize.</summary>
    internal MetricWriteAheadLog WalForTest => _wal;

    /// <summary>
    /// Makes "log the point, then publish it" atomic against a flush taking its snapshot.
    /// Ingest holds it shared and stays parallel — concurrent ingests are already safe
    /// against each other (a concurrent dictionary plus a per-series lock, which is how this
    /// worked before the log existed); only the drain needs exclusion, and it holds the lock
    /// for the drain alone, never while files are written.
    /// </summary>
    private readonly ReaderWriterLockSlim _snapshotLock = new();

    /// <summary>
    /// One flush at a time, from the snapshot through to the WAL commit. See the long note in
    /// <see cref="FlushHotTierAsync"/> for what a second concurrent flush cost.
    ///
    /// <para>Never disposed, for the same reason <see cref="_snapshotLock"/> is not: flushes
    /// scheduled during shutdown are turned away before they reach it, and a
    /// <see cref="SemaphoreSlim"/> only allocates a finalizable wait handle if
    /// <c>AvailableWaitHandle</c> is read, which nothing here does.</para>
    /// </summary>
    private readonly SemaphoreSlim _flushGate = new(1, 1);

    /// <summary>
    /// Test seam: invoked by <see cref="FlushHotTierAsync"/> after the snapshot and the log's
    /// generation bump, before the .mts files are written. Blocking in it holds a flush inside
    /// exactly the window where an overlapping flush used to reclaim its records. Null in
    /// production. The same shape as <c>StorageEngine.OnMergeManifestWritten</c>.
    /// </summary>
    internal Action? OnSnapshotTakenForTest;

    /// <summary>
    /// Test seam: handed to <see cref="MetricWriter.Write"/> and invoked with each <c>.mts</c>
    /// path the moment that file is complete. Throwing from it is a PARTIAL write — the disk
    /// state a full disk leaves when it fills between two of the files a flush writes — which
    /// nothing else in the suite can produce, since the file names carry a random nonce and the
    /// writer has no other way to fail on the second file but not the first. An instance field
    /// rather than a static on the writer, so parallel test classes cannot see each other's.
    /// Null in production.
    /// </summary>
    internal Action<string>? OnFileWrittenForTest;

    /// <summary>
    /// Test seam: invoked inside the snapshot write lock, after the log's generation is open and
    /// before the drain — the stretch that has no other way to fail on demand. See its call site.
    /// Null in production.
    /// </summary>
    internal Action? OnGenerationOpenedForTest;

    /// <summary>
    /// Test seam: invoked when a flush finds <see cref="_flushGate"/> already held — that is, when
    /// it is about to park rather than walk straight through. Once it has fired, the flush provably
    /// cannot reach its snapshot until the holder releases, which is what lets a test assert the
    /// gate is there without timing anything.
    ///
    /// <para>Deliberately phrased as a question about the wait rather than as a line standing above
    /// it: the wait has to be materialised to ask whether it completed, so a build that drops the
    /// gate drops this with it and will not compile past a test that waits on it. A seam that only
    /// sat above <c>WaitAsync</c> would still fire in such a build, and would report a flush that
    /// sailed straight past the gate as one parked on it. Null in production, and read only on the
    /// contended path.</para>
    /// </summary>
    internal Action? OnFlushGateBlockedForTest;

    // ── Hot tier ─────────────────────────────────────────────────────────────
    private readonly ConcurrentDictionary<SeriesKey, HotSeries> _hot = new();
    private          int  _hotPointCount;

    /// <summary>
    /// The same tier measured in bytes — see <see cref="EstimatedPointBytes"/> for why a point
    /// count cannot be the memory bound. Kept beside the count rather than instead of it: the
    /// count is what every existing test and log line speaks in, and the two are maintained by
    /// the same three statements.
    /// </summary>
    private          long _hotPointBytes;
    // 1 while a threshold-triggered flush is queued/running. Without this gate, every
    // Ingest call past the threshold scheduled ANOTHER flush until the first finally
    // reset the counter — a stampede of concurrent flush tasks under load.
    private          int _thresholdFlushScheduled;

    // Threshold flushes in flight, so shutdown can await them: they run OFF the flush loop
    // and were therefore invisible to it. DisposeAsync awaited _flushTask/_rollupTask and
    // then disposed the WAL and both locks while such a flush could still be draining,
    // writing its .mts and committing.
    //
    // Both failures the orphan can cause are reachable; which one you get depends on whether
    // the hot tier is empty when the loop takes its final pass:
    //   • tier NON-EMPTY (ingest still flowing — the OTLP case): the final flush has real
    //     data, so it opens generation G+1 and commits it. Committing compacts everything at
    //     or below G+1, INCLUDING the orphan's own generation G — its records leave the log
    //     while its file is still being written. Kill the process there and those points are
    //     in neither place. SILENT LOSS, and the bigger of the two: it is the tier that was
    //     large enough to trigger a threshold flush in the first place.
    //   • tier EMPTY (ingest quiesced): the final flush returns before opening a generation,
    //     so G is never superseded, stays uncommitted, and is replayed next start beside the
    //     file the orphan did manage to write — duplicates.
    // Nothing warned about either. MetricWriteAheadLog.CommitFlush returned NOTHING once the
    // log was disposed — indistinguishable from a commit that moved the watermark — so the
    // catch around it never fired; and in the more common interleaving the commit was not even
    // reached, because _coldLock.EnterWriteLock() threw ObjectDisposedException first (the lock
    // is no longer disposed — see _coldClosed — so that particular throw is gone, and a late
    // publish is refused by the fence instead). The silence is gone too: the commit answers
    // MetricWalCommit, and a Refused answer takes the flush's own files back rather than
    // leaving them to be replayed beside (see UnwriteRefusedFlush).
    //
    // Tracking the orphan fixed its LIFETIME — shutdown waits for it. The first bullet's loss
    // needed the other half too, and did not stop at shutdown: any periodic flush overlapping
    // any threshold flush reclaimed the earlier generation the same way. _flushGate closes it
    // by making the two run one after the other; see FlushHotTierAsync.
    //
    // A set rather than one Task field: a field records only the flush published LAST, and
    // publication order is not completion order. A thread preempted between starting its
    // flush and storing the handle can store an ALREADY-COMPLETED task over a live one
    // (its own flush found an empty snapshot and returned while it was descheduled), and
    // shutdown then awaits a completed task and walks straight past the running flush. An
    // entry here removes itself when its flush completes and can never overwrite another.
    private readonly ConcurrentDictionary<Task, byte> _inFlightFlushes = new();

    /// <summary>
    /// Threshold flushes currently inside <see cref="FlushHotTierAsync"/> — that is, holding
    /// or about to hold the locks and the log. Shutdown must leave this at zero, which is
    /// otherwise only assertable by reading the comment on <see cref="DisposeAsync"/>.
    /// </summary>
    private int _runningThresholdFlushes;

    internal int RunningThresholdFlushes => Volatile.Read(ref _runningThresholdFlushes);

    /// <summary>Test hook: hot-tier point count — 0 once a flush has taken its snapshot.</summary>
    internal int HotPointCount => Volatile.Read(ref _hotPointCount);

    /// <summary>Test hook: the same tier in bytes. See <see cref="EstimatedPointBytes"/>.</summary>
    internal long HotByteCount => Volatile.Read(ref _hotPointBytes);

    /// <summary>
    /// Test hook: series NAMED by the tier, points or not. The figure the leak was in — it only
    /// ever grew, because nothing removed a key once a series had been seen.
    /// </summary>
    internal int HotSeriesCount => _hot.Count;

    /// <summary>Test hook: series the stale sweep has evicted since start.</summary>
    internal long StaleSeriesEvicted => Volatile.Read(ref _staleSeriesEvicted);

    private long _staleSeriesEvicted;

    // ── Metadata catalog (maintained at ingestion, survives hot-tier drains) ───
    private readonly ConcurrentDictionary<string, MetricMeta> _meta =
        new(StringComparer.Ordinal);

    // ── Exemplars (recent, in-memory ring per metric — for metric→trace jumps) ──
    //
    // TWO ceilings, where there used to be one. A ring is allocated at FULL capacity the first
    // time a name carries an exemplar — 4 000 slots is ~480 KB with the trace and span id
    // strings — and nothing bounded the number of NAMES, so an instrumentation change could add
    // rings until the heap ran out. An exemplar is a correlation hint, never data: past the cap
    // they are dropped and the metric is unaffected.
    private readonly int _exemplarsPerMetric;
    private readonly int _maxExemplarMetrics;
    private readonly ConcurrentDictionary<string, ExemplarRing> _exemplars =
        new(StringComparer.Ordinal);

    /// <summary>
    /// 1 once <see cref="_exemplars"/> has reached <see cref="_maxExemplarMetrics"/>. Sticky on
    /// purpose: nothing ever removes a ring, so the count is monotone and the cap, once reached,
    /// can never be un-reached. See <see cref="ExemplarRingsFull"/> for what that saves.
    /// </summary>
    private int _exemplarRingsFull;

    /// <summary>Test hook: exemplars refused because the ring cap was reached.</summary>
    internal long ExemplarMetricsRefused => Volatile.Read(ref _exemplarMetricsRefused);

    private long _exemplarMetricsRefused;

    /// <summary>
    /// Test hook: how many times the ring cap has cost a full <c>ConcurrentDictionary.Count</c>.
    /// Bounded by the number of rings plus one for the life of the process — the one that latches
    /// the cap — where it used to be one per refused NAME per batch, forever.
    /// </summary>
    internal long ExemplarCapCounts => Volatile.Read(ref _exemplarCapCounts);

    private long _exemplarCapCounts;

    /// <summary>
    /// Test hook: batches that actually needed the exemplar pass. One increment per BATCH that
    /// carries an exemplar, which is what makes "the pass no longer re-walks every batch"
    /// assertable rather than a claim about a loop nobody can see.
    /// </summary>
    internal long ExemplarPasses => Volatile.Read(ref _exemplarPasses);

    private long _exemplarPasses;

    /// <summary>
    /// Test hook: series whose catalog entry was walked in full — once per series, where the
    /// walk used to run once per POINT.
    /// </summary>
    internal long MetaRegistrations => Volatile.Read(ref _metaRegistrations);

    private long _metaRegistrations;

    // ── Cold tier ─────────────────────────────────────────────────────────────
    private readonly List<MetricSegmentInfo>      _coldSegments = new();
    private readonly ReaderWriterLockSlim          _coldLock     = new();

    /// <summary>
    /// 1 once the cold list is closed for good, set inside <see cref="_coldLock"/>'s WRITE lock
    /// at the very end of the teardown. Every taker of that lock checks it and answers empty.
    ///
    /// <para><b>What this replaces.</b> The teardown used to <c>Dispose</c> the lock, and four
    /// callers take it — <see cref="QueryAsync"/>, <see cref="GetMetricNames"/>,
    /// <c>PerformRollupAsync</c> and <see cref="PruneAsync"/> — of which the first two are
    /// reached from Kestrel, which is STILL SERVING: hosted services stop in reverse
    /// registration order and the HTTP pipeline outlives <c>MetricStorageHostedService.StopAsync</c>.
    /// A query holding the read lock at that moment got an <see cref="ObjectDisposedException"/>
    /// out of the middle of its response. A query WAITING on it was worse:
    /// <see cref="ReaderWriterLockSlim.Dispose"/> throws
    /// <see cref="SynchronizationLockException"/> when a thread is waiting, nothing caught around
    /// that line, and the <c>finally</c> completed <c>_disposeCompleted</c> anyway — so the other
    /// two disposers returned believing the teardown had finished, with the fault still
    /// propagating out of the first.</para>
    ///
    /// <para>So the lock is NOT disposed, for the same reason <see cref="_snapshotLock"/> is not:
    /// it is a process-lifetime singleton whose wait handles are finalizable, and a fence costs
    /// one volatile read per acquisition where disposal costs an exception nobody can prevent.
    /// The fence is what makes "no reader touches the list after this" true rather than likely.</para>
    /// </summary>
    private int _coldClosed;

    /// <summary>
    /// Takes the cold read lock, or answers false because the tier is closed — in which case the
    /// caller must behave as though there were no cold segments. Checked twice on purpose: once
    /// before waiting, so a late caller never queues behind the teardown's write lock, and once
    /// after acquiring, because the fence can be set while this one waits.
    /// </summary>
    private bool TryEnterColdRead()
    {
        if (Volatile.Read(ref _coldClosed) != 0) return false;
        _coldLock.EnterReadLock();
        if (Volatile.Read(ref _coldClosed) == 0) return true;
        _coldLock.ExitReadLock();
        return false;
    }

    /// <summary>The same for the write lock. A refused writer must not publish or unlink.</summary>
    private bool TryEnterColdWrite()
    {
        if (Volatile.Read(ref _coldClosed) != 0) return false;
        _coldLock.EnterWriteLock();
        if (Volatile.Read(ref _coldClosed) == 0) return true;
        _coldLock.ExitWriteLock();
        return false;
    }

    // Cold discovery is deliberately off the startup path (see the constructor), so for a
    // window after construction a query legitimately sees no cold data at all. Without a
    // signal there is no way to tell that window apart from "there is nothing on disk",
    // and a caller that treats the first non-empty answer as the whole answer reads a
    // fraction of the data and cannot know it.
    private readonly TaskCompletionSource _coldLoaded = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Completes once the background cold-segment scan has published its result.</summary>
    internal Task ColdLoadCompleted => _coldLoaded.Task;

    private readonly string                        _dataDir;
    private readonly ILogger<MetricStorageEngine>  _logger;

    /// <summary>
    /// The clock every age in this engine is read from — the hot tier's age, a series' last
    /// append, the flush tick. A seam and not a convenience: the sweep below evicts a series
    /// that has been silent for hours, and a test that has to wait hours, or sleep for a
    /// proportional fraction of them, is a test nobody runs. <see cref="TimeProvider.System"/>
    /// in production.
    /// </summary>
    private readonly TimeProvider _time;

    /// <summary>
    /// Registration with <see cref="MemoryShedRegistry"/>, or null when nothing registered this
    /// engine. See <see cref="RegisterForMemoryPressure"/>.
    /// </summary>
    private readonly System.Threading.Lock _shedLock = new();
    private IDisposable? _shedRegistration;

    // ── Background tasks ──────────────────────────────────────────────────────
    private readonly CancellationTokenSource _cts        = new();
    private readonly Task                    _flushTask;
    private readonly Task                    _rollupTask;

    // 0 = live, 1 = disposed. Guards against the multiple DisposeAsync calls
    // that occur at host shutdown (see DisposeAsync).
    private int _disposed;

    /// <summary>
    /// Completed once the teardown has actually finished. A later caller awaits this rather
    /// than returning on the <see cref="_disposed"/> exchange: being idempotent means not
    /// tearing down twice, not telling the second caller that a teardown it never waited for
    /// is over. Three call sites dispose this engine at host shutdown, and a shutdown timeout
    /// turns that sequence into an overlap — see <see cref="DisposeAsync"/>.
    /// </summary>
    private readonly TaskCompletionSource _disposeCompleted =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// 1 once the log is about to be unmapped. <see cref="Ingest"/> is gated on THIS and not
    /// on <see cref="_disposed"/>, because everything between the two is still fully durable:
    /// the log is open, the final flush has not run, and a point arriving there is written and
    /// replayed like any other. Only the last step of the teardown has to turn callers away.
    /// </summary>
    private int _ingestClosed;

    public MetricStorageEngine(string dataDir, ILogger<MetricStorageEngine> logger,
                               MetricsOptions? options = null, TimeProvider? timeProvider = null)
    {
        _dataDir = dataDir;
        _logger  = logger;
        _time    = timeProvider ?? TimeProvider.System;
        _options = options ?? new MetricsOptions();
        Directory.CreateDirectory(dataDir);

        // ONE MemoryBudgets.Current() for the whole engine: it allocates (the GC's configuration
        // dictionary) and the figures cannot change while the process runs, so reading it per
        // derived cap would be five dictionaries for five identical answers.
        var budgets                = MemoryBudgets.Current();
        _hotFlushBytes             = Math.Max(HotPointBytes, _options.HotTierBytesFor(in budgets));
        _minFlushBytes             = Math.Min(_hotFlushBytes, _options.MinFlushBytesFor(in budgets));
        _flushCheckInterval        = _options.FlushCheckInterval > TimeSpan.Zero
                                        ? _options.FlushCheckInterval : TimeSpan.FromSeconds(60);
        _maxHotAge                 = _options.MaxHotAge > TimeSpan.Zero
                                        ? _options.MaxHotAge : TimeSpan.FromHours(1);
        _staleSeriesAge            = TimeSpan.FromTicks(_maxHotAge.Ticks * 2);
        _maxLabelValuesPerKey      = Math.Max(1, _options.MaxLabelValuesPerKey);
        _maxTrackedSeriesPerMetric = Math.Max(1, _options.MaxTrackedSeriesPerMetric);
        _exemplarsPerMetric        = _options.ExemplarsPerMetricFor(in budgets);
        _maxExemplarMetrics        = Math.Max(1, _options.MaxExemplarMetrics);

        // The WAL, unlike cold-segment discovery, must be open and replayed before the first
        // point is accepted, or a restart would interleave recovered and live data. Replay is
        // a sequential walk of one mmap'd file bounded by the flush thresholds.
        _wal = MetricWriteAheadLog.Open(Path.Combine(dataDir, "metrics.wal"),
                                        _options.WalInitialBytesFor(in budgets), logger);
        RecoverFromWal();

        // Leftover builds from a flush or rollup killed between the write and the rename. HERE,
        // and not beside the cold scan that it was written next to: that scan runs in the flush
        // loop, in the background, while ingest is already being accepted, and a wildcard delete
        // over *.mts.tmp in a directory with live writers unlinks whatever a flush crossing
        // the flush threshold has open at that moment. On Linux the unlink succeeds under the open
        // handle — the writer goes on filling an inode with no name, then FileInfo(tmpPath) or
        // the rename throws — and the flush treats a healthy write as a failed one: the whole
        // snapshot back into the hot tier, the generation abandoned, "Failed to flush metric hot
        // tier" logged against a disk that was fine. The *.seg.tmp sweep this was modelled on is
        // safe because it shares the flush lock with the path that writes those files; the metric
        // threshold flush is scheduled off the ingest path and shares no lock with the scan.
        //
        // Nothing races the constructor. The engine that owns every writer of these files is the
        // one being built, so there is no flush, no rollup and no ingest to collide with, and
        // that is a property of WHERE this runs rather than of what it checks — no age or
        // ownership test would give it.
        foreach (var tmp in Directory.EnumerateFiles(dataDir, "*.mts" + MetricWriter.TempSuffix))
        {
            try { File.Delete(tmp); }
            catch (Exception ex) { _logger.LogWarning(ex, "Failed to delete leftover temp metric segment {File}", tmp); }
        }

        // Cold-segment discovery + catalog seeding read every .mts file — far too
        // heavy for the startup path (it ran before Kestrel bound the port). The
        // flush loop performs it as its first act in the background instead:
        // ingest works from second zero, cold data becomes queryable when done.
        _flushTask  = Task.Run(FlushLoopAsync);
        _rollupTask = Task.Run(RollupLoopAsync);
    }

    /// <summary>
    /// Rebuilds the hot tier from the log left behind by an unclean shutdown. Recovered
    /// points are NOT written back to the log — they are already in it, and it keeps
    /// appending after the last valid entry.
    /// </summary>
    private void RecoverFromWal()
    {
        List<MetricWriteAheadLog.RecoveredPoint> recovered;
        int unresolved;
        try { recovered = _wal.ReadAll(out unresolved); }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Metric WAL replay failed — continuing with an empty hot tier");
            return;
        }

        if (unresolved > 0)
            _logger.LogWarning("Metric WAL: {Count} point(s) referenced a series missing from the pool and were skipped",
                unresolved);
        if (recovered.Count == 0) return;

        long oldestNano    = long.MaxValue;
        long futureLimit   = FutureLimitNanos();
        int  droppedFuture = 0;

        foreach (var r in recovered)
        {
            // See MaxFutureSkewNanos. Recovery replays whatever the log holds, and the log is
            // exactly where the incident's year-2116 point came from — every restart pushed it
            // back into the hot tier, and every flush from there minted another immortal file.
            if (r.Point.TimestampUnixNano > futureLimit) { droppedFuture++; continue; }

            var item = new MetricIngestItem
            {
                Name              = r.Name,
                Kind              = r.Kind,
                Unit              = r.Unit,
                Labels            = r.Labels,
                TimestampUnixNano = r.Point.TimestampUnixNano,
                BucketBounds      = r.Bounds,
                // Exemplars are not logged (see MetricWriteAheadLog) — a flush never
                // persisted them either, so replay restores exactly what a flush would have.
            };
            ApplyToHotTier(item, r.Point);
            if (r.Point.TimestampUnixNano > 0 && r.Point.TimestampUnixNano < oldestNano)
                oldestNano = r.Point.TimestampUnixNano;
        }

        // Date the tier by the data, not by this restart: leaving _hotSince at "now" would
        // restart the MaxHotAge clock on every start, so a crash-restart loop could keep
        // points out of a file indefinitely. Clamped to now, because the timestamp comes
        // from the client and a skewed clock must not push the tier into the future.
        if (oldestNano is > 0 and < long.MaxValue)
        {
            var at = DateTimeOffset.FromUnixTimeMilliseconds(oldestNano / 1_000_000L).UtcDateTime;
            var nowUtc = _time.GetUtcNow().UtcDateTime;
            _hotSince = at < nowUtc ? at : nowUtc;
        }

        if (droppedFuture > 0) ReportFutureDrops(droppedFuture, "WAL recovery");

        _logger.LogInformation("Recovered {Count} metric point(s) from the write-ahead log",
            recovered.Count - droppedFuture);
    }

    /// <summary>
    /// Says that far-future points were refused, without saying it once per batch: a client
    /// with a broken clock sends every batch broken, and ingest handles many batches a second.
    /// One warning a minute, carrying both the increment and the lifetime total, reads the
    /// same and costs nothing.
    /// </summary>
    private void ReportFutureDrops(int dropped, string where)
    {
        long total = Interlocked.Add(ref _futureDroppedTotal, dropped);

        long now  = Environment.TickCount64;
        long last = Volatile.Read(ref _futureDropLastLogTicks);
        if (last != 0 && now - last < 60_000) return;
        if (Interlocked.CompareExchange(ref _futureDropLastLogTicks, now, last) != last) return;

        _logger.LogWarning(
            "Dropped {Dropped} metric point(s) at {Where} with timestamps more than 24 h in the future " +
            "({Total} since start). A point that far ahead makes its cold file immortal for rollup and " +
            "retention; real clock skew stays well under the margin.",
            dropped, where, total);
    }

    // ── IMetricIngester ───────────────────────────────────────────────────────

    public int Ingest(ReadOnlySpan<MetricIngestItem> items)
    {
        long hotBytes = 0;

        long futureLimit   = FutureLimitNanos();
        int  droppedFuture = 0;

        // Whether the exemplar pass below has anything to do. Exemplars are optional in OTLP and
        // most exporters send none, but the pass re-walked the WHOLE batch regardless — re-testing
        // every item's timestamp against the future limit a second time to discover, item by item,
        // that there was nothing there. The main loop already looks at each item; this is what it
        // costs to remember what it saw. (Folding the pass INTO that loop is the change that
        // cannot be made: the loop runs under _snapshotLock, and the exemplar ring must not.)
        bool anyExemplars = false;

        // Logging a point and making it visible must be one step with respect to a flush's
        // snapshot, or a point that lands between the two would be in neither the files nor
        // (after the commit) the log — durable nowhere despite the guarantee above. Held
        // shared: this excludes the drain, not other ingests, which need no exclusion.
        _snapshotLock.EnterReadLock();
        try
        {
            // The log is gone or is about to be. MetricWriteAheadLog.Append returns SILENTLY
            // once disposed, so carrying on would file every remaining point of this batch
            // into a hot tier nobody will flush again and then return normally — the caller is
            // told a batch was accepted that is durable nowhere. Throwing is what an exporter
            // reads as a failed export and retries. Checked under the read lock, which the
            // teardown takes exclusively before it sets this: no Append can straddle the two.
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _ingestClosed) != 0, this);

            foreach (var item in items)
            {
                // See MaxFutureSkewNanos. Before the WAL append, so garbage never becomes the
                // durable copy of anything; counted here, reported once outside the lock.
                if (item.TimestampUnixNano > futureLimit) { droppedFuture++; continue; }

                var point = new MetricDataPoint
                {
                    TimestampUnixNano = item.TimestampUnixNano,
                    Value             = item.Kind == MetricKind.Histogram
                                            ? (item.HistogramCount > 0 ? item.HistogramSum / item.HistogramCount : 0)
                                            : item.ScalarValue,
                    Count             = item.HistogramCount,
                    Sum               = item.HistogramSum,
                    BucketCounts      = item.BucketCounts,   // preserved for real percentiles + heatmap
                };

                _wal.Append(item, in point);
                hotBytes = ApplyToHotTier(item, in point);
                anyExemplars |= item.Exemplars is { Length: > 0 };
            }
        }
        finally { _snapshotLock.ExitReadLock(); }

        if (droppedFuture > 0) ReportFutureDrops(droppedFuture, "ingest");

        if (anyExemplars) AddExemplars(items, futureLimit);

        if (hotBytes >= _hotFlushBytes
            && System.Threading.Interlocked.CompareExchange(ref _thresholdFlushScheduled, 1, 0) == 0)
        {
            // Discarded, necessarily — an ingest call cannot wait on a flush. What the flush
            // has to say about itself is therefore said by the continuation inside, not here.
            _ = ScheduleThresholdFlush();
        }

        return droppedFuture;
    }

    /// <summary>
    /// Files the batch's exemplars into their per-metric rings. Reached only when the ingest loop
    /// saw at least one — see <c>anyExemplars</c> in <see cref="Ingest"/> — because the rings are
    /// optional in OTLP, most exporters send none, and this walk used to run over every batch
    /// regardless, re-testing each item's timestamp to find nothing.
    ///
    /// <para>Outside <c>_snapshotLock</c> on purpose: exemplars are not written to the log and a
    /// ring takes a lock of its own, so they have no business inside the window that excludes the
    /// flush drain.</para>
    /// </summary>
    private void AddExemplars(ReadOnlySpan<MetricIngestItem> items, long futureLimit)
    {
        Interlocked.Increment(ref _exemplarPasses);

        // The ring handle is carried across items: an OTLP batch arrives grouped by instrument,
        // so consecutive items share a name and the reference test replaces a dictionary lookup
        // per point with a pointer comparison.
        string?       lastName = null;
        ExemplarRing? lastRing = null;

        foreach (var item in items)
        {
            // See MaxFutureSkewNanos. A refused point's exemplars are stamped by the same
            // broken clock, and GetExemplars sorts newest-first — an admitted far-future
            // exemplar would sort to the TOP of every answer until the ring rotates it out.
            if (item.TimestampUnixNano > futureLimit) continue;
            if (item.Exemplars is not { Length: > 0 } exs) continue;

            ExemplarRing? ring;
            if (ReferenceEquals(item.Name, lastName))
            {
                ring = lastRing;
                if (ring is null) continue;                 // the same name, refused above
            }
            else if (!_exemplars.TryGetValue(item.Name, out ring))
            {
                // The ring cap is checked before GetOrAdd creates one: past it a NEW name is
                // refused, while names that already have a ring keep working. GetOrAdd's factory
                // can run more than once under contention, so the count is the gate, not the
                // allocation.
                if (ExemplarRingsFull())
                {
                    Interlocked.Increment(ref _exemplarMetricsRefused);
                    lastName = item.Name;
                    lastRing = null;
                    continue;
                }
                ring = _exemplars.GetOrAdd(item.Name, static (_, s) => new ExemplarRing(s), _exemplarsPerMetric);
            }
            lastName = item.Name;
            lastRing = ring;

            foreach (var ex in exs)
            {
                // The exemplar's OWN clock, not the point's: OTLP parses time_unix_nano per
                // exemplar, so a sane point can carry a 2116-stamped exemplar — and the skip
                // above, keyed on the point, would wave it straight through to the top of
                // every newest-first answer.
                if (ex.TimestampUnixNano > futureLimit) continue;
                ring.Add(new ExemplarSample
                {
                    TimestampUnixNano = ex.TimestampUnixNano,
                    Value             = ex.Value,
                    TraceId           = ex.TraceId,
                    SpanId            = ex.SpanId,
                    Labels            = item.Labels,
                });
            }
        }
    }

    /// <summary>
    /// Whether a metric name that has no ring may still take one.
    ///
    /// <para><b>The count is asked at most once more than there are rings.</b>
    /// <c>ConcurrentDictionary.Count</c> acquires EVERY lock in the table — the reason
    /// <see cref="RegisterMeta"/> tests <c>ContainsKey</c> before it counts — and this dictionary
    /// is built with the default <c>growLockArray</c>, so a table holding the 256-name cap
    /// carries 64-128 monitors, taken from lock 0 upwards, on the ingest path. Nothing ever
    /// removes a ring, so the count is monotone and the answer past the cap is permanently yes:
    /// a deployment with more exemplar-carrying instruments than the cap was paying one full
    /// all-locks sweep per REFUSED NAME per batch, for the life of the process, with every
    /// concurrent ingest thread's exemplar pass serialising against the others inside it.</para>
    ///
    /// <para>The latch is deliberately set only here and never cleared. <c>GetOrAdd</c>'s factory
    /// can run more than once under contention and the count is the gate rather than the
    /// allocation, so the table may end a race a ring or two over the cap — which is what the
    /// cap has always allowed, and one more reason the answer cannot come back down.</para>
    /// </summary>
    private bool ExemplarRingsFull()
    {
        if (Volatile.Read(ref _exemplarRingsFull) != 0) return true;

        Interlocked.Increment(ref _exemplarCapCounts);
        if (_exemplars.Count < _maxExemplarMetrics) return false;

        Volatile.Write(ref _exemplarRingsFull, 1);
        return true;
    }

    /// <summary>
    /// Fire-and-forget the threshold flush, tracked so shutdown can await it — the same shape
    /// as <c>Ameto.Storage.StorageEngine.ScheduleFlush</c>, which solved this first.
    ///
    /// <para>The handle is registered BEFORE the flush can run, which <c>Task.Run</c> cannot
    /// do: its argument is evaluated first, so a pool thread is already inside the flush while
    /// the calling thread has yet to publish anything. The body's first act is to await a gate
    /// completed at the end of this method, so registration is not merely likely to win that
    /// race — it precedes the body's first instruction.</para>
    /// </summary>
    private Task ScheduleThresholdFlush()
    {
        var gate  = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var flush = RunThresholdFlushAsync(gate.Task);

        _inFlightFlushes[flush] = 0;
        // Static lambda + state argument: no closure is captured for a path that runs on
        // every threshold crossing. ExecuteSynchronously keeps the bookkeeping on the
        // completing thread rather than queueing a work item just to erase a dictionary entry.
        //
        // Reporting the failure is the second half, and it belongs here because this is the
        // only place that HOLDS the task: the periodic path got its "a tick that throws must
        // cost one tick" wrapper in the loop that awaits it, while the threshold path's only
        // caller is Ingest, which discards it. A throw out of FlushHotTierAsync therefore went
        // nowhere — no log line, no rethrow, nothing but a TaskScheduler.UnobservedTaskException
        // at some later finalisation, which no deployment reads. The tripwire this PR added to
        // BeginFlush is exactly such a throw. (Not, as this comment once claimed, a permanent
        // one: a log dead enough to be tested by CommitFlush refuses every later BeginFlush on
        // its own account, so the order of the clear never decided anything — see the paragraph
        // in MetricWriteAheadLog.CommitFlush, which keeps the clear first as defence, not as
        // repair.) The reporting below is what makes either state visible at all. Silence was
        // the whole cost.
        //
        // Reading t.Exception is also what marks it observed, so the discarded task above is no
        // longer a finaliser-time surprise. The task is still handed back to whoever called,
        // faulted and all — ScheduleThresholdFlushForTest depends on that.
        _ = flush.ContinueWith(
            static (t, s) =>
            {
                var self = (MetricStorageEngine)s!;
                self._inFlightFlushes.TryRemove(t, out _);
                if (t.Exception is { } ex)
                    self._logger.LogError(ex.InnerException ?? ex,
                        "Threshold metric flush failed; the hot tier keeps its points and the next crossing schedules another");
            },
            this, CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

        gate.SetResult();   // registered — the flush may start
        return flush;
    }

    /// <summary>
    /// Test hook: schedules a threshold flush exactly as crossing
    /// the byte threshold does, without the points needed to cross it.
    /// </summary>
    internal Task ScheduleThresholdFlushForTest() => ScheduleThresholdFlush();

    /// <summary>
    /// Test hook: one tick of the PERIODIC path — the flush that is not behind the threshold
    /// CAS, and therefore the one that could overlap a threshold flush. Goes through
    /// <see cref="FlushIfDueAsync"/> rather than straight to the flush, so the tier still has
    /// to earn its files exactly as it does in the loop.
    /// </summary>
    internal Task FlushPeriodicForTest() => FlushIfDueAsync();

    private async Task RunThresholdFlushAsync(Task gate)
    {
        await gate.ConfigureAwait(false);
        try
        {
            // Reached only after this task was registered above, and DisposeAsync sets
            // _disposed before it drains — so the two orders are mutually exclusive. Either
            // the drain saw this task and is awaiting it, or it did not, in which case
            // _disposed was already set when the registration happened and this returns
            // without touching a lock, the log or the disk. There is no third case, which is
            // what leaves an orphan nowhere to start. The points are not lost by returning:
            // they are in the WAL above the last committed generation, so the next start
            // replays them.
            if (Volatile.Read(ref _disposed) != 0) return;

            System.Threading.Interlocked.Increment(ref _runningThresholdFlushes);
            try     { await FlushHotTierAsync().ConfigureAwait(false); }
            finally { System.Threading.Interlocked.Decrement(ref _runningThresholdFlushes); }
        }
        finally { System.Threading.Interlocked.Exchange(ref _thresholdFlushScheduled, 0); }
    }

    /// <summary>
    /// Files one point into its series and the metadata catalog, returning the hot tier's new
    /// size IN BYTES — the figure the flush threshold is spent in. Shared by live ingest and WAL
    /// replay: replay must not write back into the log it is reading from, and must not trigger
    /// a flush from the constructor.
    /// </summary>
    private long ApplyToHotTier(MetricIngestItem item, in MetricDataPoint point)
    {
        var key    = new SeriesKey(item.Name, item.Kind, item.Unit, item.Labels);
        var series = _hot.GetOrAdd(key, static _ => new HotSeries());

        series.Append(point, item.BucketBounds, _time.GetUtcNow().UtcTicks);
        UpdateMeta(item, series);

        long bytes = System.Threading.Interlocked.Add(ref _hotPointBytes, EstimatedPointBytes(in point));
        int  total = System.Threading.Interlocked.Increment(ref _hotPointCount);
        if (total == 1) _hotSince = _time.GetUtcNow().UtcDateTime;   // tier went from empty to holding data
        return bytes;
    }

    /// <summary>
    /// What one point weighs in the tier, in the unit the memory budget is spent in.
    ///
    /// <para>A POINT COUNT IS THE WRONG UNIT and that is the whole reason this exists: a
    /// 16-bucket histogram point carries its own <c>long[]</c> and is 4.3x a scalar point
    /// (measured: 346 B against 185 B resident), so the same 500 000-point ceiling is 20 MB of
    /// gauges or 84 MB of histograms — on a host that was never asked how much it had.</para>
    ///
    /// <para><see cref="HotPointBytes"/> is the marginal cost of a scalar point, not the 40 bytes
    /// <see cref="MetricDataPoint"/> measures: the points live in a <see cref="List{T}"/> that
    /// grows by doubling, so a series holding N points owns between N and 2N slots. Measured at
    /// 300 points a series (capacity 512): 75 B a point including the series' own share. The
    /// bucket array, by contrast, is exact — the reconnaissance's gauge-to-histogram delta was
    /// 153 B a point for 16 buckets against the 152 B this computes.</para>
    /// </summary>
    internal const int HotPointBytes       = 64;
    private  const int BucketArrayOverhead = 24;   // object header + length, on 64-bit

    internal static int EstimatedPointBytes(in MetricDataPoint point) =>
        point.BucketCounts is { Length: > 0 } buckets
            ? HotPointBytes + BucketArrayOverhead + buckets.Length * sizeof(long)
            : HotPointBytes;

    /// <summary>
    /// Keeps the Explore catalog current for one ingested point.
    ///
    /// <para><b>The steady state is "this series is already known", and it now costs one field
    /// read.</b> This used to run <c>_meta.GetOrAdd</c> plus a nested <c>GetOrAdd</c> and a
    /// <c>ContainsKey</c> PER LABEL PER POINT — four to eight concurrent-dictionary lookups on
    /// every data point, on the ingest hot path, in a state where by construction nothing can
    /// have changed: a label set IS the series identity, so a point with different labels is a
    /// different series and lands on a different <see cref="HotSeries"/>. The same argument
    /// covers <c>Kind</c> and <c>Unit</c>, which <c>SeriesKey</c> also carries.</para>
    ///
    /// <para>So the catalog entry is cached on the series the first time it is registered, and
    /// after that only <c>LastSeenMs</c> — the one field that genuinely moves — is touched. A
    /// series evicted by the stale sweep and re-created registers again, which is correct and
    /// idempotent: <c>AddSeries</c> is keyed on the label-set hash and the label values are a
    /// set.</para>
    ///
    /// <para>The race between two threads first seeing the same new series is benign: both do
    /// the full walk, both write the same <c>MetricMeta</c> instance (it comes from a
    /// <c>GetOrAdd</c>), and every step of the walk is idempotent.</para>
    /// </summary>
    private void UpdateMeta(MetricIngestItem item, HotSeries series)
    {
        var meta = series.Meta;
        if (meta is null)
        {
            meta = RegisterMeta(item);
            series.Meta = meta;
        }

        long ms = item.TimestampUnixNano / 1_000_000L;
        if (ms > meta.LastSeenMs) meta.LastSeenMs = ms;
    }

    /// <summary>
    /// The full catalog walk — once per series, not once per point. See <see cref="UpdateMeta"/>.
    /// </summary>
    private MetricMeta RegisterMeta(MetricIngestItem item)
    {
        var meta = _meta.GetOrAdd(item.Name, static (_, cap) => new MetricMeta(cap), _maxTrackedSeriesPerMetric);
        meta.Kind = item.Kind;
        if (!string.IsNullOrEmpty(item.Unit)) meta.Unit = item.Unit;

        foreach (var (k, v) in item.Labels.Pairs)
        {
            var values = meta.LabelValues.GetOrAdd(k, static _ => new ConcurrentDictionary<string, byte>(StringComparer.Ordinal));
            // ContainsKey first: ConcurrentDictionary.Count acquires EVERY lock in the
            // table, and the cap only needs checking for a value that is actually new.
            if (values.ContainsKey(v)) continue;
            if (values.Count < _maxLabelValuesPerKey) values.TryAdd(v, 0);
        }

        meta.AddSeries(item.Labels.GetHashCode());
        Interlocked.Increment(ref _metaRegistrations);
        return meta;
    }

    // ── IMetricCatalog ────────────────────────────────────────────────────────

    public IReadOnlyList<MetricCatalogEntry> GetCatalog(string? search = null)
    {
        var result = new List<MetricCatalogEntry>(_meta.Count);
        foreach (var (name, meta) in _meta)
        {
            if (search is not null && !name.Contains(search, StringComparison.OrdinalIgnoreCase)) continue;
            var keys = meta.LabelValues.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray();
            result.Add(new MetricCatalogEntry
            {
                Name        = name,
                Kind        = meta.Kind,
                Unit        = meta.Unit,
                LabelKeys   = keys,
                Cardinality = meta.Cardinality,
                LastSeenMs  = meta.LastSeenMs,
            });
        }
        result.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));
        return result;
    }

    public IReadOnlyList<string> GetLabelKeys(string metricName) =>
        _meta.TryGetValue(metricName, out var meta)
            ? meta.LabelValues.Keys.OrderBy(k => k, StringComparer.Ordinal).ToList()
            : [];

    public IReadOnlyList<string> GetLabelValues(string metricName, string labelKey) =>
        _meta.TryGetValue(metricName, out var meta) && meta.LabelValues.TryGetValue(labelKey, out var values)
            ? values.Keys.OrderBy(v => v, StringComparer.Ordinal).ToList()
            : [];

    // ── IMetricExemplars ──────────────────────────────────────────────────────

    public IReadOnlyList<ExemplarSample> GetExemplars(
        string metricName, DateTimeOffset? from, DateTimeOffset? to,
        IReadOnlyDictionary<string, string>? filters, int limit = 200)
    {
        if (!_exemplars.TryGetValue(metricName, out var ring)) return [];

        long fromNano = from.HasValue ? from.Value.ToUnixTimeMilliseconds() * 1_000_000L : long.MinValue;
        long toNano   = to.HasValue   ? to.Value.ToUnixTimeMilliseconds()   * 1_000_000L : long.MaxValue;

        var result = new List<ExemplarSample>(Math.Min(limit, 256));
        foreach (var ex in ring.Snapshot())
        {
            if (ex.TimestampUnixNano < fromNano || ex.TimestampUnixNano > toNano) continue;
            if (!MatchesLabels(ex.Labels, filters)) continue;
            result.Add(ex);
        }
        result.Sort(static (a, b) => b.TimestampUnixNano.CompareTo(a.TimestampUnixNano)); // newest first
        return result.Count > limit ? result.GetRange(0, limit) : result;
    }

    // ── IMetricQuery ──────────────────────────────────────────────────────────

    /// <summary>
    /// Every metric name this server can answer for, hot and cold, filtered by
    /// <paramref name="prefix"/> and sorted — the Explore page's first request.
    ///
    /// <para><b>The hot half comes from <c>_meta</c>, not from <c>_hot</c>.</b> Reading
    /// <c>_hot.Keys</c> on a <see cref="ConcurrentDictionary{TKey,TValue}"/> acquires EVERY lock
    /// in its table and materialises a list of every SERIES — at the sandbox's 38 741 series that
    /// is ~1.2 MB allocated and every ingest thread in the process blocked for the duration, per
    /// page load, to produce a few dozen distinct names. <c>_meta</c> is keyed by name, is
    /// maintained on the same ingest path, is seeded from the cold segments at startup and
    /// survives hot-tier drains, so it holds tens of entries where <c>_hot</c> holds tens of
    /// thousands — and is enumerated without taking a lock at all.</para>
    ///
    /// <para>The answer is the same set: every name in <c>_hot</c> got there through
    /// <c>ApplyToHotTier</c>, which registers it in <c>_meta</c> in the same call, and every name
    /// only in <c>_meta</c> is a name the cold segments also carry. The one difference is a
    /// window of a few instructions, on the very first point of a brand-new metric, between the
    /// series being added and its metadata — where a caller could once have seen a name whose
    /// catalog entry did not exist yet.</para>
    ///
    /// <para>The ordering is <c>Comparer&lt;string&gt;.Default</c>, as <c>OrderBy(n =&gt; n)</c>
    /// was — culture-sensitive, and deliberately unchanged: this is the list the UI renders.</para>
    /// </summary>
    public IEnumerable<string> GetMetricNames(string? prefix = null)
    {
        var names = new List<string>(_meta.Count + 16);
        var seen  = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (name, _) in _meta)
            if (Matches(name, prefix) && seen.Add(name)) names.Add(name);

        if (TryEnterColdRead())
        {
            try
            {
                for (int i = 0; i < _coldSegments.Count; i++)
                {
                    string name = _coldSegments[i].MetricName;
                    if (Matches(name, prefix) && seen.Add(name)) names.Add(name);
                }
            }
            finally { _coldLock.ExitReadLock(); }
        }

        names.Sort();
        return names;

        static bool Matches(string name, string? prefix) =>
            prefix is null || name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase);
    }

    public async IAsyncEnumerable<MetricSeries> QueryAsync(
        string             metricName,
        DateTimeOffset?    from         = null,
        DateTimeOffset?    to           = null,
        TimeSpan?          step         = null,
        IReadOnlyDictionary<string, string>? labelMatchers = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        long fromNano = from.HasValue ? from.Value.ToUnixTimeMilliseconds() * 1_000_000L : long.MinValue;
        long toNano   = to.HasValue   ? to.Value.ToUnixTimeMilliseconds()   * 1_000_000L : long.MaxValue;

        // Hot tier
        foreach (var (key, series) in _hot)
        {
            if (!key.Name.Equals(metricName, StringComparison.OrdinalIgnoreCase)) continue;
            if (!MatchesLabels(key.Labels, labelMatchers)) continue;
            ct.ThrowIfCancellationRequested();

            var points = series.GetPoints(fromNano, toNano);
            if (points.Count == 0) continue;

            yield return new MetricSeries
            {
                Name         = key.Name,
                Kind         = key.Kind,
                Unit         = key.Unit,
                Labels       = key.Labels,
                BucketBounds = series.Bounds,
                Points       = step.HasValue ? Downsample(points, step.Value, key.Kind) : points,
            };
        }

        // Cold tier. A closed tier answers empty rather than throwing out of the middle of a
        // response: this enumerator is driven by Kestrel, which serves for a while after the
        // engine's hosted service has stopped.
        List<MetricSegmentInfo> coldCandidates = [];
        if (TryEnterColdRead())
        {
            try
            {
                coldCandidates = _coldSegments
                    .Where(s => s.MetricName.Equals(metricName, StringComparison.OrdinalIgnoreCase)
                             && s.MaxNano >= fromNano && s.MinNano <= toNano)
                    .ToList();
            }
            finally { _coldLock.ExitReadLock(); }
        }

        foreach (var seg in coldCandidates)
        {
            ct.ThrowIfCancellationRequested();
            await foreach (var series in MetricReader.ReadAsync(seg.FilePath, metricName, fromNano, toNano, labelMatchers, ct))
            {
                var points = step.HasValue ? Downsample(series.Points, step.Value, series.Kind) : series.Points;
                yield return new MetricSeries
                {
                    Name         = series.Name,
                    Kind         = series.Kind,
                    Unit         = series.Unit,
                    Labels       = series.Labels,
                    BucketBounds = series.BucketBounds,
                    Points       = points,
                };
            }
        }
    }

    public async IAsyncEnumerable<MetricSeries> GetLatestAsync(
        string            metricName,
        IReadOnlyDictionary<string, string>? labelMatchers = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var (key, series) in _hot)
        {
            if (!key.Name.Equals(metricName, StringComparison.OrdinalIgnoreCase)) continue;
            if (!MatchesLabels(key.Labels, labelMatchers)) continue;
            ct.ThrowIfCancellationRequested();

            var latest = series.GetLatest();
            if (latest is null) continue;

            yield return new MetricSeries
            {
                Name         = key.Name,
                Kind         = key.Kind,
                Unit         = key.Unit,
                Labels       = key.Labels,
                BucketBounds = series.Bounds,
                Points       = [latest.Value],
            };
        }
        await Task.CompletedTask; // satisfy async iterator requirement
    }

    // ── Flush ─────────────────────────────────────────────────────────────────

    private async Task FlushLoopAsync()
    {
        var ct = _cts.Token;

        // Background init (see ctor comment): discover cold segments + seed catalog.
        try { LoadColdSegments(); }
        catch (Exception ex) { _logger.LogError(ex, "Cold metric segment load failed"); }
        finally { _coldLoaded.TrySetResult(); }   // a failed scan must not leave waiters hanging

        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(_flushCheckInterval, _time, ct); }
            catch (OperationCanceledException) { break; }

            // A tick that throws must cost one tick. Bare, this await made any escaping
            // exception terminal: the loop ended, so no periodic flush ran again for the life
            // of the process, the final flush below never ran either, and — because
            // DisposeAsync only catches OperationCanceledException off this task — the fault
            // came back out of shutdown and skipped the ingest fence and the log's close. The
            // hot tier and the log then grew without bound and nothing said why. The flush
            // logs its own failures; this is only here so that the loop outlives them.
            try { await FlushIfDueAsync(); }
            catch (Exception ex) { _logger.LogError(ex, "Periodic metric flush failed; the loop continues"); }
        }
        // Final flush on shutdown — unconditional, so a clean stop leaves nothing to replay.
        try { await FlushHotTierAsync(); }
        catch (Exception ex) { _logger.LogError(ex, "Final metric flush failed during shutdown"); }
    }

    /// <summary>
    /// Flushes only when the hot tier has earned its files: enough points to be worth one
    /// <c>.mts</c> per metric name, or old enough that it should become rollup- and
    /// retention-eligible regardless. Points below both bars stay in memory, durable through
    /// the WAL and fully queryable — every read path consults the hot tier.
    /// </summary>
    private async Task FlushIfDueAsync()
    {
        int points = Volatile.Read(ref _hotPointCount);
        if (points == 0) return;

        bool due = Volatile.Read(ref _hotPointBytes) >= _minFlushBytes
                || (_hotSince is { } since && _time.GetUtcNow().UtcDateTime - since >= _maxHotAge);
        if (due) await FlushHotTierAsync().ConfigureAwait(false);
    }

    private async Task FlushHotTierAsync()
    {
        // Cheap pre-check, deliberately OUTSIDE the gate: an empty tier has nothing to flush,
        // and a periodic tick that queued behind a running flush just to discover that would
        // turn every tick during a long write into a wait. _hot keeps its keys after a drain,
        // so the point count — not _hot.IsEmpty — is what "nothing to flush" means here.
        if (_hot.IsEmpty || Volatile.Read(ref _hotPointCount) == 0) return;

        // One flush at a time, from the snapshot through to the commit.
        //
        // The threshold CAS gates threshold flushes against each other and nothing else, so a
        // PERIODIC flush could take its snapshot, write its (small) files and commit while a
        // threshold flush was still writing its own. The log's watermark is a single u64 and a
        // commit frees everything at or below it, so the later commit reclaimed the earlier
        // flush's records mid-write: from that instant until its last .mts closed, its points
        // were in no file and no log, and if the write then failed they were restored to the
        // tier in memory only. Nothing logged it.
        //
        // Serialising here makes the watermark a prefix boundary BY CONSTRUCTION — at most one
        // generation is ever between Begin and Commit — instead of by an assumption spread
        // across two classes. It also makes the failure path correct rather than accidentally
        // correct: the next flush's drain now provably happens after the restore below, so its
        // snapshot carries the restored points and its commit legitimately covers the
        // abandoned generation.
        //
        // What this does NOT give up is the thing moving the write off the lock bought: the
        // gate is not _snapshotLock, so ingest is still never blocked on disk. What it gives
        // up is flush-versus-flush parallelism, of which there are only ever two candidates —
        // one periodic and one threshold. A SemaphoreSlim rather than System.Threading.Lock
        // because C# forbids await inside a lock body; there is no true suspension point in
        // here (MetricWriter.Write is synchronous), so a holder always runs to completion.
        // Acquire order is gate → _snapshotLock → _coldLock → the log's own lock, and nothing
        // takes the gate while holding any of them.
        // Materialised rather than awaited inline so that "this flush had to queue" is a fact the
        // engine can hand to a test, instead of something a test infers from a stopwatch. An
        // uncontended wait returns the cached completed task, so this costs nothing a plain await
        // did not already cost.
        var gate = _flushGate.WaitAsync();
        if (!gate.IsCompleted) OnFlushGateBlockedForTest?.Invoke();
        await gate.ConfigureAwait(false);
        try
        {
            // The pre-check above describes the tier BEFORE the wait, and the wait is exactly
            // where it goes stale: whoever held the gate was draining the tier, so a tick that
            // queued behind a threshold flush wakes over an empty one by construction, not by
            // chance. Without this it went on to open a generation, bump the counter, write the
            // new value into the mapped header, drain nothing and abandon — correct, but "there
            // was nothing to write" is a normal outcome of crossing a flush, not the rare race
            // the branch below reads as. Repeated per tick for as long as a large flush runs.
            if (Volatile.Read(ref _hotPointCount) == 0) return;

            // Snapshot and clear hot tier. Bounds must travel with the snapshot —
            // without them cold histogram files have no bucket bounds and quantile /
            // heatmap queries over anything older than the hot tier return nothing.
            //
            // The drain, the counter reset and the log's generation bump are one atomic step
            // against ingest: a point that arrives while this runs must land wholly on one
            // side, either inside the snapshot or stamped with the next generation. Writing
            // the files happens after the lock is released, so ingest is never blocked on disk.
            var snapshot = new List<(SeriesKey Key, HotSeries Series)>();
            ulong flushedGeneration = 0;
            bool  generationOpened  = false;
            try
            {
                _snapshotLock.EnterWriteLock();
                try
                {
                    // Opened BEFORE the drain. Order inside the lock is irrelevant to atomicity —
                    // ingest is excluded for all of it — but it decides what a tripwire costs: the
                    // log throws here if a flush is somehow still open, and throwing before the
                    // drain leaves the tier untouched instead of stranding a snapshot nobody holds.
                    flushedGeneration = _wal.BeginFlush();
                    generationOpened  = true;

                    // Test seam standing INSIDE the write lock, between the open generation and
                    // the drain — the one stretch of this method no catch used to reach, and the
                    // one nothing else can throw from on demand: what throws there in production
                    // is the drain's own allocation (a fresh List per series, at up to 500 000
                    // points), so an OutOfMemoryException, which a test cannot ask for. Without
                    // it the finally below would be held up by its comment alone.
                    OnGenerationOpenedForTest?.Invoke();

                    // The stale sweep rides along with the drain: one pass over _hot, and the
                    // eviction happens where it is provably safe — see SweepStaleSeriesLocked.
                    long staleBefore = _time.GetUtcNow().UtcTicks - _staleSeriesAge.Ticks;
                    List<SeriesKey>? stale = null;

                    foreach (var (k, v) in _hot)
                    {
                        var points = v.Drain();
                        if (points.Count > 0)
                            snapshot.Add((k, new HotSeries(points, v.Bounds)));
                        else if (v.LastAppendUtcTicks < staleBefore)
                            (stale ??= []).Add(k);
                    }

                    if (snapshot.Count == 0)
                    {
                        // Nothing to write, so nothing to commit — hand the generation back rather
                        // than leaving it open. The empty generation is covered by the next commit.
                        //
                        // Genuinely rare now, which it was not before the re-check above the lock:
                        // the counter is raised under the read lock after the point is appended, so
                        // a non-zero count seen from inside the WRITE lock means some series holds
                        // points. Kept because the two are separate pieces of state and this costs
                        // one comparison.
                        _wal.AbandonFlush(flushedGeneration);
                        return;
                    }

                    System.Threading.Interlocked.Exchange(ref _hotPointCount, 0);
                    System.Threading.Interlocked.Exchange(ref _hotPointBytes, 0);
                    _hotSince = null;

                    // AFTER the counters are zeroed, as the drain itself is: a series evicted
                    // here holds nothing that either of them still counts.
                    if (stale is not null) SweepStaleSeriesLocked(stale);
                }
                finally { _snapshotLock.ExitWriteLock(); }

                List<MetricSegmentInfo> infos;
                try
                {
                    // Test seam, INSIDE the try on purpose: it stands where the file write does, so
                    // holding in it holds the flush in the window the loss lived in, and throwing
                    // from it is a failed write — restore, abandon, no commit. Outside the try a
                    // throw would skip both and wedge the log. Null in production: one delegate
                    // read per flush, not per point.
                    OnSnapshotTakenForTest?.Invoke();

                    // The write and NOTHING ELSE. What follows it — publishing the files to the
                    // cold list, committing, logging — happens on the far side of the durability
                    // boundary, and treating a failure there as a failed write is how every point
                    // of the snapshot ended up in a complete .mts file AND back in the hot tier AND
                    // in a generation deliberately left replayable: served twice, replayed twice,
                    // every counter and sum over the window doubled. No crash needed.
                    infos = MetricWriter.Write(_dataDir, snapshot, afterFileWritten: OnFileWrittenForTest);
                }
                catch (Exception ex)
                {
                    // The points were drained out of _hot before the write, so a failure here used
                    // to lose them outright. Put them back: the log still holds them, but only a
                    // restart would have brought them back, and a transient disk error is not a
                    // restart. Sound only because the writer is all-or-nothing about what it
                    // leaves on disk — it deletes the files it had already written before it
                    // rethrows — so "the write failed" really does mean no file carries these
                    // points and putting every one of them back cannot duplicate anything.
                    int  restored      = 0;
                    long restoredBytes = 0;
                    _snapshotLock.EnterWriteLock();
                    try
                    {
                        long nowTicks = _time.GetUtcNow().UtcTicks;
                        foreach (var (key, snap) in snapshot)
                        {
                            // The snapshot's list is the one Drain handed over, and `live`'s is the
                            // fresh one it left behind — two different lists, which is what makes
                            // appending into one while reading the other sound. GetOrAdd rather
                            // than a lookup because the stale sweep above may have evicted the key.
                            var live = _hot.GetOrAdd(key, static _ => new HotSeries());
                            foreach (var p in snap.GetPoints(long.MinValue, long.MaxValue))
                            {
                                live.Append(p, snap.Bounds, nowTicks);
                                restoredBytes += EstimatedPointBytes(in p);
                                restored++;
                            }
                        }
                        System.Threading.Interlocked.Add(ref _hotPointCount, restored);
                        System.Threading.Interlocked.Add(ref _hotPointBytes, restoredBytes);
                        if (restored > 0) _hotSince ??= _time.GetUtcNow().UtcDateTime;

                        // Inside the same lock as the restore, and that matters: the generation is
                        // given back only once the points it covers are visible again, so the next
                        // flush cannot drain between the two and miss them. The restored points are
                        // NOT re-logged — deliberately, since the usual cause is a full disk and the
                        // log grows by doubling — so this generation's records are what keeps them
                        // durable until a later flush snapshots them and commits above it.
                        _wal.AbandonFlush(flushedGeneration);
                        generationOpened = false;
                    }
                    finally { _snapshotLock.ExitWriteLock(); }

                    _logger.LogError(ex,
                        "Failed to flush metric hot tier — {Count} point(s) kept in memory for the next attempt",
                        restored);
                    return;
                }

                // ── Past the durability boundary ─────────────────────────────────────────
                // Every .mts file is complete at its final path. The points are durable in
                // files from here, so nothing below may put them back into the tier, and the
                // steps run in descending order of what they cost to skip: the commit is the
                // only one whose omission is a correctness fault (the log would replay points
                // that are already in a file), publishing costs visibility until the next
                // start's LoadColdSegments finds the files anyway, and the log line costs
                // nothing. One catch over all three, because a flush reports failure by
                // logging it, not by throwing at whoever scheduled it.
                try
                {
                    var commit = _wal.CommitFlush(flushedGeneration);
                    generationOpened = false;

                    // The commit could not move the watermark, so the sentence above stops
                    // being true for this flush alone: the log still holds this generation,
                    // intact and uncompacted, and the files just written are a SECOND copy of
                    // points it will replay at the next start. Nothing below may publish them
                    // and this one must take them back — a duplicate needs two copies, and
                    // this is the last moment anything knows which of the two is the spare.
                    if (commit == MetricWalCommit.Refused)
                    {
                        UnwriteRefusedFlush(infos, flushedGeneration);
                        return;
                    }

                    // A closed tier cannot be published to. The files are complete and durable
                    // where they are; the next start's LoadColdSegments finds them, which is the
                    // same cost the catch below already accepts.
                    if (TryEnterColdWrite())
                    {
                        try { _coldSegments.AddRange(infos); }
                        finally { _coldLock.ExitWriteLock(); }
                    }

                    _logger.LogDebug("Flushed {SeriesCount} metric series to {FileCount} .mts files",
                        snapshot.Count, infos.Count);
                }
                catch (Exception ex)
                {
                    // A throw out of CommitFlush cannot mean "not committed" — the log throws
                    // only from past its watermark store, and refuses by returning — so these
                    // files ARE the durable copy and the branch above must not be reached from
                    // here. Keeping them, unpublished, costs visibility until the next start's
                    // LoadColdSegments picks them up off the disk.
                    _logger.LogError(ex,
                        "Metric flush wrote {FileCount} .mts file(s) but failed afterwards; the points are " +
                        "durable in those files and are NOT returned to the hot tier", infos.Count);
                }
            }
            finally
            {
                // The generation does not outlive the flush that opened it, whatever happens in
                // between — including the drain above, which allocates a copy of every series
                // and sat outside every handler. A leaked open flush is not one lost snapshot:
                // BeginFlush refuses every later flush, so the tier never drains again and the
                // log grows by doubling until it cannot. AbandonFlush only releases a generation
                // that is still the open one, so the committed and abandoned paths no-op here.
                if (generationOpened) _wal.AbandonFlush(flushedGeneration);
            }
        }
        finally { _flushGate.Release(); }
    }

    // ── Stale-series sweep ────────────────────────────────────────────────────

    /// <summary>
    /// How long a series may hold no points before the tier stops naming it. Twice
    /// <c>MetricsOptions.MaxHotAge</c>, so a series is only evicted well after the flush that would
    /// have carried its points: a series still reporting at any cadence the tier is built for
    /// is never a candidate, and one that comes back is re-created for free — its cold data is
    /// untouched, and <c>_meta</c> (which the catalog and the names list are fed from) never
    /// forgets it at all.
    /// </summary>
    private readonly TimeSpan _staleSeriesAge;

    /// <summary>
    /// Drops the named series from the hot tier. <b>Call only under <c>_snapshotLock</c>'s WRITE
    /// lock</b>: ingest holds that lock shared across a whole batch and does
    /// <c>_hot.GetOrAdd</c> then <c>Append</c> as two steps, so evicting between them would file
    /// a point into an object no query can reach — the point acknowledged, durable in the log,
    /// and invisible until the next restart replays it.
    ///
    /// <para>The pair-wise <c>TryRemove</c> is a compare-and-remove: it takes the key out only
    /// while it still maps to the very object that was found empty, so a series re-created
    /// between the scan and the removal survives. Under the write lock nothing can do that; the
    /// overload is here because <see cref="Shed"/> holds the same lock on a try-basis and the
    /// cost of being right anyway is one reference comparison per evicted series.</para>
    /// </summary>
    private void SweepStaleSeriesLocked(List<SeriesKey> stale)
    {
        int evicted = 0;
        foreach (var key in stale)
        {
            if (!_hot.TryGetValue(key, out var series) || series.PointCount != 0) continue;
            if (_hot.TryRemove(new KeyValuePair<SeriesKey, HotSeries>(key, series))) evicted++;
        }

        if (evicted == 0) return;
        Interlocked.Add(ref _staleSeriesEvicted, evicted);
        _logger.LogDebug("Hot metric tier dropped {Count} series idle for over {Hours} h ({Named} still named)",
            evicted, _staleSeriesAge.TotalHours, _hot.Count);
    }

    // ── IMemoryShedder ────────────────────────────────────────────────────────

    /// <summary>
    /// What a flush plus a sweep would hand back: the points in the tier, plus what the series
    /// that hold none are still costing. All of it managed, hence
    /// <see cref="ShedableNativeBytes"/> = 0 — the pressure loop already counts the managed heap
    /// through <c>GC.GetGCMemoryInfo</c>, and reporting these bytes there as well would count
    /// them twice.
    /// </summary>
    public long ShedableBytes => Volatile.Read(ref _hotPointBytes) + (long)_hot.Count * EmptySeriesBytes;

    /// <inheritdoc/>
    public long ShedableNativeBytes => 0;

    /// <summary>
    /// A series with no points still costs a <c>HotSeries</c>, its lock, its list, its
    /// <c>SeriesKey</c> + <c>LabelSet</c> + pair array and a concurrent-dictionary node.
    /// Structural, and deliberately not the 12 565 B the reconnaissance measured per series —
    /// that figure WAS the retained point array, which no longer survives a drain.
    /// </summary>
    private const int EmptySeriesBytes = 384;

    /// <summary>
    /// Lets go of what can be let go of without waiting for anybody: the empty series go now,
    /// and the live points leave through a flush, which is the only thing that may move them.
    ///
    /// <para>Try-enter and not enter: <see cref="IMemoryShedder.Shed"/> is called from the RAM
    /// pressure loop and must never park behind a caller of its own, and an ingest batch holds
    /// the snapshot lock shared for a whole OTLP request. Losing the race costs nothing — the
    /// flush this schedules sweeps on its own way through.</para>
    /// </summary>
    public long Shed()
    {
        if (Volatile.Read(ref _disposed) != 0) return 0;

        long released = 0;
        if (_snapshotLock.TryEnterWriteLock(0))
        {
            try
            {
                List<SeriesKey>? empty = null;
                foreach (var (k, v) in _hot)
                    if (v.PointCount == 0) (empty ??= []).Add(k);

                if (empty is not null)
                {
                    int before = _hot.Count;
                    SweepStaleSeriesLocked(empty);      // under pressure, idle is reason enough
                    released = (long)(before - _hot.Count) * EmptySeriesBytes;
                }
            }
            finally { _snapshotLock.ExitWriteLock(); }
        }

        // The points themselves: only a flush moves them, and it must not run under this lock.
        if (Volatile.Read(ref _hotPointCount) > 0
            && System.Threading.Interlocked.CompareExchange(ref _thresholdFlushScheduled, 1, 0) == 0)
            _ = ScheduleThresholdFlush();

        return released;
    }

    /// <summary>
    /// Registers this engine with <see cref="MemoryShedRegistry"/> and returns it, for chaining
    /// from a DI factory. Explicit rather than automatic in the constructor, for the reason
    /// <c>SegmentIndexCache.RegisterForMemoryPressure</c> gives: registration is a process-wide
    /// effect, and an engine built by a test has no business being swept because something else
    /// in the process reported pressure. Idempotent; undone by <see cref="DisposeAsync"/>.
    /// </summary>
    public MetricStorageEngine RegisterForMemoryPressure()
    {
        lock (_shedLock) _shedRegistration ??= MemoryShedRegistry.Register(this);
        return this;
    }

    /// <summary>
    /// Removes the files of a flush whose generation the log could not commit, so the points
    /// they hold are durable exactly ONCE — in the log, which still carries the generation's
    /// records below an unchanged watermark and replays them at the next start.
    ///
    /// <para>The alternative was doing nothing, which is what used to happen and what was
    /// argued to be unavoidable: leave the files and let the replay add the same points a
    /// second time. It is not unavoidable, because a duplicate needs two copies and this one
    /// is deletable — the reclaim never ran (it is on the far side of the watermark store the
    /// commit did not reach), so the log's copy is whole. Nor is it cheap to leave: metric
    /// points are summed, so a doubled counter is not a visible artefact but a wrong number,
    /// and it recurs at every start for as long as the log keeps those records. (No precedent
    /// is claimed for the delete: retention's <see cref="PruneAsync"/> also unlinks files, but
    /// an expired file's points are durable nowhere afterwards — expiry is the point — while
    /// here the log still carries every record the deleted files held.)</para>
    ///
    /// <para>What it costs is visibility until that start: the points have left the hot tier,
    /// their files are gone, and nothing reads the log except recovery. That is the right side
    /// to err on and the window is bounded by the fault itself — a log with no mapping refuses
    /// every append, so ingest is already failing loudly and this process is not long for the
    /// world. Said out loud in the log line, because an operator who sees a gap needs to know
    /// it closes on restart rather than staying a hole.</para>
    /// </summary>
    private void UnwriteRefusedFlush(List<MetricSegmentInfo> infos, ulong flushedGeneration)
    {
        int kept = 0;
        foreach (var info in infos)
        {
            try { File.Delete(info.FilePath); }
            catch (Exception ex)
            {
                kept++;
                _logger.LogError(ex,
                    "Could not remove {File}, whose points the metric log still holds under generation " +
                    "{Generation}; they will be replayed beside it and counted twice", info.FilePath, flushedGeneration);
            }
        }

        if (kept == 0)
            _logger.LogError(
                "Metric log refused to commit generation {Generation}: it is closed or has lost its mapping, " +
                "so the watermark did not move. The {FileCount} .mts file(s) this flush wrote have been " +
                "removed and their points stay durable in the log alone — they are replayed, and visible " +
                "again, at the next start", flushedGeneration, infos.Count);
        else
            _logger.LogError(
                "Metric log refused to commit generation {Generation} and {Kept} of {FileCount} .mts file(s) " +
                "could not be removed; the points those files hold are also in the log and will be counted " +
                "twice from the next start", flushedGeneration, kept, infos.Count);
    }

    // ── Rollup ────────────────────────────────────────────────────────────────

    private async Task RollupLoopAsync()
    {
        var ct = _cts.Token;
        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(TimeSpan.FromMinutes(5), ct); }
            catch (OperationCanceledException) { break; }

            try { await PerformRollupAsync(ct); }
            catch (Exception ex) { _logger.LogError(ex, "Metric rollup error"); }
            // Advance the round-robin so the next pass serves the next metrics.
            _rollupCursor += MaxMetricsPerPass;
        }
    }

    private Task PerformRollupAsync(CancellationToken ct)
    {
        // Compact raw files older than 10 min but newer than the 1-h rollup cutoff
        // (merges many small flush files into one without downsampling)
        var compactCutoff = DateTimeOffset.UtcNow.AddMinutes(-10).ToUnixTimeMilliseconds() * 1_000_000L;

        // Rollup raw files older than 1 hour → 5-min buckets
        var cutoff1h   = DateTimeOffset.UtcNow.AddHours(-1).ToUnixTimeMilliseconds()  * 1_000_000L;
        var cutoff24h  = DateTimeOffset.UtcNow.AddHours(-24).ToUnixTimeMilliseconds() * 1_000_000L;

        List<MetricSegmentInfo> toCompact;
        List<MetricSegmentInfo> toMerge5m;
        List<MetricSegmentInfo> toMerge1h;
        List<MetricSegmentInfo> toRollup5m;
        List<MetricSegmentInfo> toRollup1h;

        // A closed tier has no work: this pass would otherwise rewrite and unlink .mts files
        // after the engine has been torn down.
        if (!TryEnterColdRead()) return Task.CompletedTask;
        try
        {
            toCompact  = _coldSegments
                .Where(s => s.Granularity == MetricGranularity.Raw
                         && s.MaxNano < compactCutoff
                         && s.MaxNano >= cutoff1h)
                .ToList();
            toRollup5m = _coldSegments
                .Where(s => s.Granularity == MetricGranularity.Raw && s.MaxNano < cutoff1h)
                .ToList();
            toRollup1h = _coldSegments
                .Where(s => s.Granularity == MetricGranularity.FiveMin && s.MaxNano < cutoff24h)
                .ToList();
            // Same-granularity merges: each rollup pass emits one small file per
            // metric, so without merging the 5-min and 1-h tiers accumulate
            // hundreds of label-repeating files per metric over the retention
            // window. Also selects lone legacy-v2 files so old data migrates to v3.
            toMerge5m  = _coldSegments
                .Where(s => s.Granularity == MetricGranularity.FiveMin && s.MaxNano >= cutoff24h)
                .ToList();
            toMerge1h  = _coldSegments
                .Where(s => s.Granularity == MetricGranularity.OneHour)
                .ToList();
        }
        finally { _coldLock.ExitReadLock(); }

        // Work budget: every stage below materialises a WHOLE metric (all series,
        // all points — histograms carry a bucket array per point) before writing
        // it back. Processing all metrics in one pass therefore scales with total
        // cardinality: on a 38k-series deployment it allocated ~180 MB/s for
        // minutes and drove the working set past 1 GB every 5 minutes. Take only a
        // few metrics per pass, rotating so every metric is served across passes.
        toCompact  = TakeMetricSlice(toCompact);
        toMerge5m  = TakeMetricSlice(toMerge5m);
        toMerge1h  = TakeMetricSlice(toMerge1h);
        toRollup5m = TakeMetricSlice(toRollup5m);
        toRollup1h = TakeMetricSlice(toRollup1h);

        // Bytes this pass is about to chew through, from catalog metadata (sized once
        // at write/load — no per-pass stat calls). The five lists are disjoint by
        // construction (granularity/cutoff windows don't overlap), so no double count.
        long passBytes = TotalSizeBytes(toCompact) + TotalSizeBytes(toMerge5m) + TotalSizeBytes(toMerge1h)
                       + TotalSizeBytes(toRollup5m) + TotalSizeBytes(toRollup1h);

        if (toCompact.Count >= 2)  CompactSegments(toCompact, MetricGranularity.Raw);
        MergeTier(toMerge5m, MetricGranularity.FiveMin);
        // A merged file expires whole (MaxNano vs the retention cutoff), so its
        // window is also its retention granularity — never let it exceed the TTL
        // itself, or a short retention would be violated several times over.
        var window1h = TimeSpan.FromTicks(Math.Min(TimeSpan.FromDays(7).Ticks,
            Interlocked.Read(ref _lastPruneTtlTicks)));
        MergeTier(toMerge1h, MetricGranularity.OneHour, maxSpan: window1h);
        if (toRollup5m.Count > 0)  Rollup(toRollup5m, MetricGranularity.FiveMin, TimeSpan.FromMinutes(5));
        if (toRollup1h.Count > 0)  Rollup(toRollup1h, MetricGranularity.OneHour, TimeSpan.FromHours(1));

        // Hand the pass's peak back to the OS instead of letting it ratchet up across
        // passes — but only when there WAS a peak. This used to run unconditionally:
        // a silent blocking compacting gen2 every 5 minutes around the clock, even for
        // a pass whose every work list was empty. A day's MEM telemetry showed exactly
        // that shape — the idle CPU sawing 0.2→5% on a drifting 5-minute period with
        // nothing in the log to explain it, 41 of 46 idle spikes unaccounted for.
        // Aggressive mode also raises the memory-pressure signal that drains
        // ArrayPool.Shared, undoing the pooling on the ingest/query hot paths.
        // The gate interval still applies when the floor is met (see AggressiveGcGate);
        // without coordination this call and StorageEngine's maintenance collect once
        // landed 8 s apart mid-load — the two longest pauses in a 7-minute GC trace.
        if (passBytes >= AggressiveGcGate.MaintenancePassBytesFloor)
            AggressiveGcGate.TryCollect(TimeSpan.FromMinutes(2));

        return Task.CompletedTask;
    }

    /// <summary>Metrics processed per rollup pass (rotating) — bounds peak memory.</summary>
    private const int MaxMetricsPerPass = 4;

    private static long TotalSizeBytes(List<MetricSegmentInfo> segments)
    {
        long bytes = 0;
        foreach (var s in segments) bytes += s.SizeBytes;
        return bytes;
    }

    /// <summary>Round-robin cursor over metric names, so no metric is starved.</summary>
    private int _rollupCursor;

    /// <summary>
    /// Restricts a work list to <see cref="MaxMetricsPerPass"/> metric names,
    /// starting where the previous pass stopped. Segments of the chosen metrics
    /// are kept whole — a metric is never split across passes, which would leave
    /// its files half-merged.
    /// </summary>
    private List<MetricSegmentInfo> TakeMetricSlice(List<MetricSegmentInfo> segments)
    {
        if (segments.Count == 0) return segments;
        var names = segments.Select(s => s.MetricName).Distinct().OrderBy(n => n, StringComparer.Ordinal).ToList();
        if (names.Count <= MaxMetricsPerPass) return segments;

        int start = _rollupCursor % names.Count;
        var take  = new HashSet<string>(StringComparer.Ordinal);
        for (int i = 0; i < MaxMetricsPerPass; i++)
            take.Add(names[(start + i) % names.Count]);

        return segments.Where(s => take.Contains(s.MetricName)).ToList();
    }

    /// <summary>
    /// Merges same-granularity cold files per metric: when several have piled up,
    /// or when any is still in the legacy v2 format (so old data migrates to v3
    /// and shrinks). <paramref name="maxSpan"/> windows the merge so one file
    /// never grows beyond that time range (used for the long-lived 1-h tier).
    /// </summary>
    private void MergeTier(List<MetricSegmentInfo> tier, MetricGranularity granularity, TimeSpan? maxSpan = null)
    {
        foreach (var group in tier.GroupBy(s => s.MetricName))
        {
            // Window by maxSpan so a merged file's range stays bounded.
            IEnumerable<List<MetricSegmentInfo>> windows;
            if (maxSpan is { } span)
            {
                long spanNanos = (long)span.TotalMilliseconds * 1_000_000L;
                windows = group
                    .GroupBy(s => s.MinNano / spanNanos)
                    .Select(g => g.ToList());
            }
            else
            {
                windows = [group.ToList()];
            }

            foreach (var window in windows)
            {
                bool hasLegacy = window.Any(s => s.FormatVersion < 3);
                if (window.Count < 4 && !hasLegacy) continue; // not worth a rewrite yet
                CompactSegments(window, granularity);
            }
        }
    }

    /// <summary>
    /// Merges multiple segments of one granularity into one file per metric
    /// without downsampling. Points are de-duplicated by timestamp (last wins)
    /// so a crash between write-new and delete-old can never double data.
    /// </summary>
    private void CompactSegments(List<MetricSegmentInfo> sources, MetricGranularity granularity)
    {
        foreach (var group in sources.GroupBy(s => s.MetricName))
        {
            var segs = group.ToList();
            if (segs.Count < 2 && segs.All(s => s.FormatVersion >= 3)) continue;

            try
            {
                var newInfos = RewriteMetricInChunks(
                    segs, granularity, static (pts, _) => DedupeByTimestamp(pts));

                if (!TryEnterColdWrite()) return;      // closed mid-pass: leave both sets on disk
                try
                {
                    foreach (var s in segs) _coldSegments.Remove(s);
                    _coldSegments.AddRange(newInfos);
                }
                finally { _coldLock.ExitWriteLock(); }

                foreach (var s in segs)
                    try { File.Delete(s.FilePath); } catch { /* best effort */ }

                _logger.LogDebug("Compacted {Count} {Granularity} segment(s) for metric '{Metric}'",
                    segs.Count, granularity, group.Key);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{Granularity} compaction failed for metric '{Metric}'", granularity, group.Key);
            }
        }
    }

    /// <summary>
    /// Series retained in memory at once while rewriting a metric. Matches the
    /// writer's per-file cap so each chunk becomes exactly one output file.
    /// </summary>
    private const int SeriesChunk = 512;

    /// <summary>
    /// Rewrites ONE metric's source files, transforming each series' points, with the
    /// retained <em>point</em> volume bounded by <see cref="SeriesChunk"/> series. That
    /// is the bound that matters — points are what scale with time and dominate the
    /// heap. It is NOT fully independent of cardinality: <c>keys</c>/<c>seen</c>/
    /// <c>bounds</c> below hold one entry per series for the whole rewrite (a key is a
    /// name + kind + unit + <see cref="LabelSet"/>, tens of bytes, so 40k series cost
    /// single-digit MB against the hundreds of MB of points this avoids).
    ///
    /// <para>A metric with more series than the chunk is processed in several passes:
    /// pass 0 collects the key set (its points are decoded one series at a time by
    /// <see cref="MetricReader"/> and dropped immediately), then each chunk re-reads the
    /// sources and keeps only its own series. Chunk boundaries are deliberately NOT
    /// aligned to source files even though <see cref="SeriesChunk"/> equals the writer's
    /// per-file cap: file membership is insertion order at write time, so the same series
    /// lands in different file slots across time windows, and the sources being merged
    /// here are of mixed vintage (pre-cap files carry unbounded series counts). Pairing
    /// by file index would silently split a series across two outputs. Re-reading costs
    /// LZ4 decompression on a background path — cheaper than retaining hundreds of MB and
    /// paying for it in blocking gen2 collections. The reader rents its compressed and
    /// decompressed buffers from <see cref="System.Buffers.ArrayPool{T}"/>, so the extra
    /// passes do not churn the LOH (this process runs workstation GC, which never
    /// compacts it). Metrics that fit in one chunk read each file exactly once.</para>
    /// </summary>
    internal List<MetricSegmentInfo> RewriteMetricInChunks(
        List<MetricSegmentInfo> segs,
        MetricGranularity       target,
        Func<List<MetricDataPoint>, MetricKind, List<MetricDataPoint>> transform)
    {
        // ── Pass 0: key set + bucket bounds (no points retained) ───────────────
        var keys   = new List<SeriesKey>();
        var seen   = new HashSet<SeriesKey>();
        var bounds = new Dictionary<SeriesKey, double[]?>();
        foreach (var seg in segs)
            foreach (var s in MetricReader.ReadAllSync(seg.FilePath))
            {
                var key = new SeriesKey(s.Name, s.Kind, s.Unit, s.Labels);
                if (seen.Add(key)) keys.Add(key);
                if (s.BucketBounds is not null) bounds[key] = s.BucketBounds;
            }
        if (keys.Count == 0) return [];

        var written = new List<MetricSegmentInfo>();
        for (int off = 0; off < keys.Count; off += SeriesChunk)
        {
            int take  = Math.Min(SeriesChunk, keys.Count - off);
            // Single-chunk metric: no filtering needed, one pass over the files.
            var wanted = keys.Count <= SeriesChunk
                ? null
                : new HashSet<SeriesKey>(keys.GetRange(off, take));

            var acc = new Dictionary<SeriesKey, List<MetricDataPoint>>(take);
            foreach (var seg in segs)
                foreach (var s in MetricReader.ReadAllSync(seg.FilePath))
                {
                    var key = new SeriesKey(s.Name, s.Kind, s.Unit, s.Labels);
                    if (wanted is not null && !wanted.Contains(key)) continue;
                    if (!acc.TryGetValue(key, out var pts))
                    {
                        pts = new List<MetricDataPoint>();
                        acc[key] = pts;
                    }
                    pts.AddRange(s.Points);
                }

            var batch = new List<(SeriesKey, HotSeries)>(acc.Count);
            foreach (var (key, pts) in acc)
                batch.Add((key, new HotSeries(transform(pts, key.Kind), bounds.GetValueOrDefault(key))));

            if (batch.Count > 0) written.AddRange(MetricWriter.Write(_dataDir, batch, target));
        }
        return written;
    }

    /// <summary>Sorts by timestamp and drops duplicate-timestamp points (last wins).</summary>
    private static List<MetricDataPoint> DedupeByTimestamp(List<MetricDataPoint> pts)
    {
        pts.Sort(static (a, b) => a.TimestampUnixNano.CompareTo(b.TimestampUnixNano));
        var result = new List<MetricDataPoint>(pts.Count);
        for (int i = 0; i < pts.Count; i++)
        {
            if (i + 1 < pts.Count && pts[i + 1].TimestampUnixNano == pts[i].TimestampUnixNano)
                continue; // superseded by the later entry with the same ts
            result.Add(pts[i]);
        }
        return result;
    }

    private void Rollup(
        List<MetricSegmentInfo> sources,
        MetricGranularity       targetGranularity,
        TimeSpan                bucketSize)
    {
        // Group source files by metric name, aggregate points into buckets
        var groupedByMetric = sources.GroupBy(s => s.MetricName);

        foreach (var group in groupedByMetric)
        {
            try
            {
                // Aggregate into time buckets — type-aware (see Downsample) — in
                // bounded series chunks so a high-cardinality metric can't pin
                // hundreds of MB while it is rewritten.
                var newInfos = RewriteMetricInChunks(
                    group.ToList(), targetGranularity,
                    (pts, kind) => Downsample(
                        pts.OrderBy(p => p.TimestampUnixNano).ToList(), bucketSize, kind).ToList());

                if (!TryEnterColdWrite()) return;      // closed mid-pass: leave both sets on disk
                try
                {
                    foreach (var s in group) _coldSegments.Remove(s);
                    _coldSegments.AddRange(newInfos);
                }
                finally { _coldLock.ExitWriteLock(); }

                // Delete old files
                foreach (var s in group)
                    try { File.Delete(s.FilePath); } catch { /* best effort */ }

                _logger.LogDebug("Rolled up {Count} segments for metric '{Metric}' → {Granularity}",
                    group.Count(), group.Key, targetGranularity);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Rollup failed for metric '{Metric}'", group.Key);
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static bool MatchesLabels(
        LabelSet labels,
        IReadOnlyDictionary<string, string>? matchers)
    {
        if (matchers is null || matchers.Count == 0) return true;
        var pairs = labels.Pairs.ToDictionary(t => t.Key, t => t.Value, StringComparer.Ordinal);
        foreach (var (k, v) in matchers)
        {
            if (!pairs.TryGetValue(k, out var actual)) return false;
            if (!LabelValueMatches(actual, v)) return false;
        }
        return true;
    }

    /// <summary>
    /// Exact match, or OR-match when the matcher value is '|'-delimited
    /// (e.g. <c>service.name=A|B|C</c>) — lets the multi-service filter merge
    /// several series server-side so quantiles aggregate over the union.
    /// </summary>
    private static bool LabelValueMatches(string actual, string matcher)
    {
        if (matcher.IndexOf('|') < 0) return actual == matcher;
        foreach (var opt in matcher.Split('|'))
            if (actual == opt) return true;
        return false;
    }

    /// <summary>
    /// Type-aware downsample into fixed time buckets:
    /// <list type="bullet">
    ///   <item>Counter / Histogram (cumulative): take the LAST point in each bucket so the
    ///   monotonic cumulative series — and the bucket-count snapshot — are preserved for
    ///   later rate/quantile computation. Averaging would corrupt them.</item>
    ///   <item>Gauge: average within the bucket.</item>
    /// </list>
    /// </summary>
    private static IReadOnlyList<MetricDataPoint> Downsample(
        IReadOnlyList<MetricDataPoint> points,
        TimeSpan step,
        MetricKind kind)
    {
        long bucketNanos = (long)step.TotalMilliseconds * 1_000_000L;
        bool takeLast = kind is MetricKind.Counter or MetricKind.Histogram;

        return points
            .GroupBy(p => p.TimestampUnixNano / bucketNanos * bucketNanos)
            .Select(g =>
            {
                if (takeLast)
                {
                    var last = g.OrderBy(p => p.TimestampUnixNano).Last();
                    return new MetricDataPoint
                    {
                        TimestampUnixNano = g.Key,
                        Value             = last.Value,
                        Count             = last.Count,
                        Sum               = last.Sum,
                        BucketCounts      = last.BucketCounts,
                    };
                }
                return new MetricDataPoint
                {
                    TimestampUnixNano = g.Key,
                    Value             = g.Average(p => p.Value),
                    Count             = g.Sum(p => p.Count),
                    Sum               = g.Sum(p => p.Sum),
                };
            })
            .OrderBy(p => p.TimestampUnixNano)
            .ToList();
    }

    private void LoadColdSegments()
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();

        // No *.mts.tmp sweep here, deliberately — see the constructor. This method runs in the
        // flush loop with ingest already live, so the files a wildcard would match include the
        // ones a concurrent flush has open.

        var loaded = new List<MetricSegmentInfo>();
        foreach (var file in Directory.EnumerateFiles(_dataDir, "*.mts").OrderBy(f => f))
        {
            try
            {
                loaded.Add(MetricReader.ReadSegmentInfo(file));
            }
            catch (Exception ex)
            {
                // v1 files (no bucket data) are incompatible with the v2 format — delete them.
                _logger.LogWarning(ex, "Unreadable metric segment {File} — deleting (likely format v1)", file);
                try { File.Delete(file); } catch { /* best effort */ }
            }
        }

        // Runs in the background, so flushes may already have registered new
        // segments — merge, don't overwrite (dedup by path).
        if (!TryEnterColdWrite()) return;   // disposed before the background scan finished
        try
        {
            var known = new HashSet<string>(_coldSegments.Select(s => s.FilePath), StringComparer.Ordinal);
            _coldSegments.InsertRange(0, loaded.Where(s => !known.Contains(s.FilePath)));
        }
        finally { _coldLock.ExitWriteLock(); }

        _logger.LogInformation("Loaded {Count} cold metric segments in {Ms} ms", loaded.Count, sw.ElapsedMilliseconds);
        SeedCatalogFromCold(loaded);
    }

    /// <summary>
    /// Rebuilds the in-memory metric catalog from cold segments on startup so the
    /// Explore catalog / Overview detection work immediately after a restart, instead
    /// of staying blank until the next live export repopulates metadata.
    /// </summary>
    private void SeedCatalogFromCold(List<MetricSegmentInfo> segments)
    {
        int seeded = 0;
        foreach (var seg in segments)
        {
            try
            {
                foreach (var s in MetricReader.ReadAllSync(seg.FilePath))
                {
                    var meta = _meta.GetOrAdd(s.Name, static (_, cap) => new MetricMeta(cap), _maxTrackedSeriesPerMetric);
                    meta.Kind = s.Kind;
                    if (!string.IsNullOrEmpty(s.Unit)) meta.Unit = s.Unit;
                    long lastMs = (s.Points.Count > 0 ? s.Points[^1].TimestampUnixNano : seg.MaxNano) / 1_000_000L;
                    if (lastMs > meta.LastSeenMs) meta.LastSeenMs = lastMs;
                    foreach (var (k, v) in s.Labels.Pairs)
                    {
                        var values = meta.LabelValues.GetOrAdd(k, static _ => new ConcurrentDictionary<string, byte>(StringComparer.Ordinal));
                        if (values.Count < _maxLabelValuesPerKey) values.TryAdd(v, 0);
                    }
                    meta.AddSeries(s.Labels.GetHashCode());
                    seeded++;
                }
            }
            catch (Exception ex) { _logger.LogWarning(ex, "Catalog seed failed for {File}", seg.FilePath); }
        }
        if (seeded > 0) _logger.LogInformation("Seeded metric catalog with {Count} series from cold segments", seeded);
    }

    public async ValueTask DisposeAsync()
    {
        // Idempotent: this engine is disposed more than once at host shutdown —
        // the DI container disposes the singleton IAsyncDisposable, and
        // MetricStorageHostedService additionally calls DisposeAsync from both
        // StopAsync and its own DisposeAsync. Cancelling/disposing the CTS twice
        // throws ObjectDisposedException, so only the first caller tears down.
        //
        // The others WAIT for it. Returning on the exchange made every guarantee below true
        // for the caller that won it and for nobody else, and the three calls are not always
        // a sequence: StopAsync discards its CancellationToken and this method has no timeout
        // of its own, so when the host's shutdown timeout elapses it stops waiting on
        // StopAsync and goes on to dispose the container — a second, CONCURRENT call, which
        // returned in microseconds and let the process exit on top of a running flush.
        if (System.Threading.Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            await _disposeCompleted.Task.ConfigureAwait(false);
            return;
        }

        try
        {
            // First, so the pressure loop cannot start a sweep or schedule a flush against an
            // engine that is on its way down. The registry's weak reference is a backstop for a
            // registrant that was dropped without this; it is not the mechanism.
            lock (_shedLock)
            {
                _shedRegistration?.Dispose();
                _shedRegistration = null;
            }

            _cts.Cancel();
            try { await Task.WhenAll(_flushTask, _rollupTask); }
            catch (OperationCanceledException) { }
            // Everything below this line is the teardown itself — the ingest fence, the log's
            // close, the locks — and a background loop that faulted must not be able to skip
            // it. Catching only the cancellation let one exception out of the flush loop take
            // the whole sequence with it, while the `finally` still completed _disposeCompleted:
            // the host's other two disposers then returned believing the engine was down, with
            // the log still mapped and the OTLP door still open.
            catch (Exception ex) { _logger.LogError(ex, "Metric background loop faulted before shutdown"); }

            // Threshold flushes are scheduled off the ingest path, so they are in neither loop
            // and used to outlive shutdown entirely. Ingest keeps scheduling them throughout
            // this — the engine is disposed by MetricStorageHostedService.StopAsync, hosted
            // services stop in reverse registration order, and Kestrel can still be serving
            // /otlp/v1/metrics for all of it — and ONE drain covers them, because _disposed
            // was set above and a flush is registered before its body can run (see
            // ScheduleThresholdFlush). For any flush: either this snapshot holds it and awaits
            // it, or it was registered after the snapshot, hence after _disposed, and its body
            // returns before reaching a lock, the log or the disk. There is no third case.
            //
            // StorageEngine.DisposeAsync drains twice for a reason that does not carry over:
            // it FLUSHES between its two calls, and that flush can schedule more work. Two
            // back-to-back calls would close only the window between them, which is empty.
            await DrainThresholdFlushesAsync().ConfigureAwait(false);

            // For threshold flushes this is now exact: one already running was awaited above,
            // and one that registers from here on returns without touching anything.
            //
            // Shut the door on ingest, and wait for the callers already through it. Ingest is
            // ungated for the whole shutdown up to this line, deliberately: the log is open
            // until a few instructions below, so a point arriving during the final flush is
            // still written and still replayed, and refusing it early would turn a point that
            // survives into one that does not.
            //
            // What it must not do is keep accepting points once the log is gone. Ingest holds
            // the snapshot lock shared across a WHOLE batch — an OTLP request — and taking it
            // exclusively here is what makes "no Append is in flight" true rather than likely:
            // ReaderWriterLockSlim.Dispose does NOT throw for a read lock held on another
            // thread (it inspects the calling thread's counts and the global waiter counts),
            // so without this fence the unmapping below happened underneath a live batch,
            // every remaining Append hit `if (_disposed) return;` and the caller was told its
            // points had landed. Measured at ~48 000 points per shutdown under load, silently.
            _snapshotLock.EnterWriteLock();
            try { Volatile.Write(ref _ingestClosed, 1); }
            finally { _snapshotLock.ExitWriteLock(); }

            // The log first: nothing durable depends on the locks, and disposing a
            // ReaderWriterLockSlim throws if a thread happens to be WAITING on it, which must
            // not be able to skip the unmap. After the loop's final flush, which commits it.
            _wal.Dispose();
            _cts.Dispose();

            // The cold tier closes behind a FENCE, and the lock is not disposed — see the note
            // on _coldClosed for what disposing it did to a query that was holding or waiting on
            // it while Kestrel was still serving. Taken exclusively so that every reader is
            // either already finished or has yet to start, and the ones yet to start answer
            // empty.
            _coldLock.EnterWriteLock();
            try { Volatile.Write(ref _coldClosed, 1); }
            finally { _coldLock.ExitWriteLock(); }

            // _snapshotLock is deliberately NOT disposed. Its only two users are Ingest and
            // FlushHotTierAsync; both are shut above, and the gate turns a late ingest away by
            // itself, so disposal would buy nothing — while costing something real, because a
            // reader released by the ExitWriteLock a few lines up is still counted as waiting
            // until it wakes, and Dispose throws SynchronizationLockException on a waiter. Its
            // wait handles are finalizable and this engine is a process-lifetime singleton.
        }
        finally { _disposeCompleted.TrySetResult(); }
    }

    /// <summary>Awaits every threshold flush registered as of this call.</summary>
    private async ValueTask DrainThresholdFlushesAsync()
    {
        var pending = _inFlightFlushes.Keys.ToArray();
        if (pending.Length == 0) return;
        try { await Task.WhenAll(pending).ConfigureAwait(false); }
        catch (Exception ex) { _logger.LogWarning(ex, "Metric threshold flush failed during shutdown"); }
    }

    // ── IRetentionTarget ──────────────────────────────────────────────────────

    public string RetentionKey => "metrics";

    /// <summary>Last TTL retention pruned with — bounds the 1-h merge window (default 7 days).</summary>
    private long _lastPruneTtlTicks = TimeSpan.FromDays(7).Ticks;

    public Task<int> PruneAsync(TimeSpan ttl, CancellationToken ct = default)
    {
        // RetentionService holds this engine as an IRetentionTarget and is a hosted service of
        // its own, so its stop order against this one is a registration detail, not a guarantee.
        // Ungated, a prune that arrived after the teardown unlinked .mts files from a directory
        // this process had finished with — and did it through a lock the teardown had disposed.
        if (Volatile.Read(ref _disposed) != 0) return Task.FromResult(0);

        Interlocked.Exchange(ref _lastPruneTtlTicks, ttl.Ticks);
        var cutoffNano = DateTimeOffset.UtcNow.Subtract(ttl).ToUnixTimeMilliseconds() * 1_000_000L;

        List<MetricSegmentInfo> toDelete;
        if (!TryEnterColdWrite()) return Task.FromResult(0);
        try
        {
            toDelete = _coldSegments.Where(s => s.MaxNano < cutoffNano).ToList();
            foreach (var s in toDelete)
                _coldSegments.Remove(s);
        }
        finally { _coldLock.ExitWriteLock(); }

        foreach (var s in toDelete)
            try { File.Delete(s.FilePath); } catch { /* best effort */ }

        if (toDelete.Count > 0)
            _logger.LogInformation("Retention pruned {Count} metric file(s) older than {Days} days",
                toDelete.Count, (int)ttl.TotalDays);

        return Task.FromResult(toDelete.Count);
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

internal readonly record struct SeriesKey(
    string    Name,
    MetricKind Kind,
    string    Unit,
    LabelSet  Labels);

/// <summary>
/// Fixed-capacity circular buffer of exemplars for one metric (newest overwrite oldest).
/// Thread-safe; exemplars are recent correlation hints, not durable history.
/// </summary>
internal sealed class ExemplarRing
{
    private readonly ExemplarSample[] _buf;
    private readonly object _lock = new();
    private int _count;
    private int _head; // next write index

    public ExemplarRing(int capacity) => _buf = new ExemplarSample[capacity];

    public void Add(ExemplarSample s)
    {
        lock (_lock)
        {
            _buf[_head] = s;
            _head = (_head + 1) % _buf.Length;
            if (_count < _buf.Length) _count++;
        }
    }

    public ExemplarSample[] Snapshot()
    {
        lock (_lock)
        {
            var outArr = new ExemplarSample[_count];
            for (int i = 0; i < _count; i++)
            {
                int idx = (_head - _count + i + _buf.Length) % _buf.Length;
                outArr[i] = _buf[idx];
            }
            return outArr;
        }
    }
}

/// <summary>
/// In-memory metadata for one metric name. Survives hot-tier drains so the Explore
/// catalog stays complete. Cardinality is tracked as distinct label-set hashes (capped).
/// </summary>
internal sealed class MetricMeta
{
    /// <summary>
    /// Distinct label-set hashes this metric counts before cardinality stops rising. From
    /// <c>MetricsOptions.MaxTrackedSeriesPerMetric</c>, carried per instance because the engine
    /// that owns the catalog is what was configured — a static would make one host's setting the
    /// process's.
    /// </summary>
    private readonly int _maxTrackedSeries;

    public MetricMeta(int maxTrackedSeries) => _maxTrackedSeries = maxTrackedSeries;

    public MetricKind Kind        { get; set; }
    public string     Unit        { get; set; } = string.Empty;
    public long       LastSeenMs  { get; set; }

    /// <summary>label key → set of observed values (capped per key).</summary>
    public ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> LabelValues { get; } =
        new(StringComparer.Ordinal);

    // Cardinality tracking sits on the ingest hot path — it runs once per data point, on
    // every OTLP request, from every Kestrel thread at once. A HashSet behind a Monitor
    // serialised all of them on one lock per metric name, and a CPU profile of the stand
    // showed the contention (Monitor.Enter_Slowpath under MetricStorageEngine.Ingest).
    // The steady state is "this series was already seen" — exporters re-send the same
    // series every interval — so make that case a lock-free read and pay only for a
    // genuinely new series. Count is tracked separately because ConcurrentDictionary.Count
    // takes every lock in the table.
    private readonly ConcurrentDictionary<int, byte> _seriesHashes = new();
    private int _trackedCount;

    public int Cardinality => Volatile.Read(ref _trackedCount);

    public void AddSeries(int labelSetHash)
    {
        if (_seriesHashes.ContainsKey(labelSetHash)) return;                   // hot path, no lock
        if (Volatile.Read(ref _trackedCount) >= _maxTrackedSeries) return;
        if (_seriesHashes.TryAdd(labelSetHash, 0))
            Interlocked.Increment(ref _trackedCount);
    }
}

internal sealed class HotSeries
{
    /// <summary>
    /// The most slots a drained series carries into its next life. A series that held 500 000
    /// points used to keep a 500 000-slot array until the process died (see <see cref="Drain"/>);
    /// carrying the FULL drained count instead would only shorten that to one flush interval,
    /// and at 2 000 series x 300 points it measured 4 333 B still held per series. So the carry
    /// is capped at a fill a steady exporter actually reaches between flushes — 15-second
    /// scrapes across a 60-second cadence is four points — and a burst re-grows by doubling from
    /// there, which costs a handful of small arrays and no retention at all.
    /// </summary>
    private const int MaxCarriedCapacity = 16;

    private List<MetricDataPoint> _points;
    private readonly object _lock = new();

    /// <summary>
    /// <c>DateTime.UtcNow.Ticks</c> of the last <see cref="Append"/>, from the engine's clock.
    /// Read without the series lock — a sweep only needs to know the series has been idle for
    /// hours, and one tick of staleness in that answer changes nothing.
    /// </summary>
    private long _lastAppendUtcTicks;

    /// <summary>
    /// Histogram bucket upper bounds shared by every point. Set once from the first
    /// histogram point; null for scalar series.
    /// </summary>
    public double[]? Bounds { get; private set; }

    /// <summary>
    /// This series' catalog entry, cached after the first point. The series identity carries the
    /// metric name, kind, unit and label set, so once it is known nothing the catalog records
    /// about this series can change again — see <c>MetricStorageEngine.UpdateMeta</c>, which the
    /// cache turns from four to eight concurrent-dictionary lookups per POINT into one field
    /// read. Not volatile: a thread that misses another's write does the full walk a second time,
    /// which is idempotent, and every path that reads it holds the series' own lock moments
    /// before or after.
    /// </summary>
    public MetricMeta? Meta { get; set; }

    /// <summary>
    /// A new series starts with NO array, not with 64 slots. Sixty-four
    /// <see cref="MetricDataPoint"/>s is 2 560 B reserved the moment a label set is first seen,
    /// and the deployments this round exists for have tens of thousands of series that carry
    /// four points a flush: 38 741 of them is ~99 MB of reservation for ~6 MB of points. The
    /// list grows by doubling from its first add, which is a handful of small arrays per series
    /// per flush and nothing that survives one.
    /// </summary>
    public HotSeries() => _points = [];

    public HotSeries(List<MetricDataPoint> points, double[]? bounds = null)
    {
        _points = points;
        Bounds  = bounds;
    }

    /// <summary>When this series last took a point. See <see cref="_lastAppendUtcTicks"/>.</summary>
    public long LastAppendUtcTicks => Volatile.Read(ref _lastAppendUtcTicks);

    /// <summary>Points held right now — 0 immediately after a <see cref="Drain"/>.</summary>
    public int PointCount { get { lock (_lock) return _points.Count; } }

    public void Append(MetricDataPoint p, double[]? bounds, long nowUtcTicks)
    {
        lock (_lock)
        {
            if (bounds is not null && Bounds is null) Bounds = bounds;
            _points.Add(p);
        }
        Volatile.Write(ref _lastAppendUtcTicks, nowUtcTicks);
    }

    /// <summary>
    /// Hands the flush the list itself and starts this series over on a fresh one.
    ///
    /// <para>It used to copy — <c>new List&lt;MetricDataPoint&gt;(_points)</c> then
    /// <c>_points.Clear()</c> — which paid for the snapshot twice over. Once in allocation: a
    /// second full copy of every point, per flush, on the path that already holds the engine's
    /// write lock. And once, permanently, in retention: <see cref="List{T}.Clear"/> does not
    /// shrink the backing array, so the series kept an array sized to the largest burst it had
    /// ever seen for the life of the process, whether or not it ever reported again. Measured at
    /// 12 565 B retained per series by a tier holding ZERO points — 54 % of the burst's heap
    /// surviving the drain, which on the sandbox's 38 741 series is ~470 MB of gen2 nothing can
    /// reclaim, in a container whose GC heap limit is 384 MB.</para>
    ///
    /// <para>The handover is safe because the caller owns what it is given: the drained list goes
    /// into a snapshot <see cref="HotSeries"/> that nothing else can reach, and the restore path
    /// on a failed write appends into THIS object's new list while reading that one — two
    /// different lists, which is the invariant the copy used to provide by brute force.</para>
    /// </summary>
    public List<MetricDataPoint> Drain()
    {
        lock (_lock)
        {
            var drained = _points;
            _points = new List<MetricDataPoint>(Math.Min(drained.Count, MaxCarriedCapacity));
            return drained;
        }
    }

    public List<MetricDataPoint> GetPoints(long fromNano, long toNano)
    {
        lock (_lock)
            return _points
                .Where(p => p.TimestampUnixNano >= fromNano && p.TimestampUnixNano <= toNano)
                .OrderBy(p => p.TimestampUnixNano)
                .ToList();
    }

    public MetricDataPoint? GetLatest()
    {
        lock (_lock)
            return _points.Count > 0 ? _points[^1] : null;
    }
}

public enum MetricGranularity : byte
{
    Raw     = 0,
    FiveMin = 1,
    OneHour = 2,
}

public sealed class MetricSegmentInfo
{
    public string           FilePath   { get; init; } = string.Empty;
    public string           MetricName { get; init; } = string.Empty;
    public long             MinNano    { get; init; }
    public long             MaxNano    { get; init; }
    public MetricGranularity Granularity { get; init; }
    /// <summary>On-disk format version (2 = legacy per-series blocks, 3 = current). Drives v2→v3 migration.</summary>
    public ushort           FormatVersion { get; init; } = 3;
    /// <summary>On-disk size, captured at write/load time (mirrors <c>SegmentInfo.CompressedBytes</c>) —
    /// lets the rollup pass budget itself without a stat call per file.</summary>
    public long             SizeBytes  { get; init; }
}
