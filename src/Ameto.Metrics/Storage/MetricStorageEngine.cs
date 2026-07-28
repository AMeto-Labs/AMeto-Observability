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
/// Flush policy: every <see cref="FlushIntervalSeconds"/> seconds (default 60 s)
/// <em>or</em> when the hot-tier point count exceeds <see cref="HotFlushThreshold"/>.
/// Flushed data is written as a <c>.mts</c> LZ4+msgpack file (see <see cref="MetricWriter"/>).
/// </para>
///
/// <para>
/// Rollup: a background pass converts raw-resolution cold files older than 1 hour
/// into 5-minute-granularity aggregates, and files older than 24 hours into
/// 1-hour-granularity aggregates. Raw files are deleted after rollup.
/// </para>
/// </summary>
public sealed class MetricStorageEngine : IMetricIngester, IMetricQuery, IMetricCatalog, IMetricExemplars, IRetentionTarget, IAsyncDisposable
{
    // ── Configuration ─────────────────────────────────────────────────────────
    private const int HotFlushThreshold    = 500_000;   // total points before forced flush
    // Durability belongs to the WAL now, so this is a CHECK interval, not a flush interval:
    // a tick decides whether the tier has earned its files. It used to be a flush interval,
    // sized at 60 s purely to bound crash loss to about a minute — and since a flush writes
    // one .mts PER METRIC NAME, a 40-instrument deployment paid 40 files a minute for that.
    private const int FlushCheckIntervalSeconds = 60;
    private const int MaxLabelValuesPerKey  = 2_000;      // cap to bound catalog memory

    // ── Flush policy ──────────────────────────────────────────────────────────
    // Below the minimum the tier keeps accumulating: the points are already durable, and a
    // file per metric name is not worth writing for a handful of them. The age bound still
    // lands a trickle on disk so it becomes eligible for rollup and retention — one hour
    // matches the rollup's own first cutoff, so nothing waits longer because of this.
    private const int MinFlushPoints = 50_000;
    private static readonly TimeSpan MaxHotAge = TimeSpan.FromHours(1);

    /// <summary>When the hot tier last went from empty to holding points. Null = empty.</summary>
    private DateTime? _hotSince;

    /// <summary>
    /// Write-ahead log for the hot tier. Every point lands here before it is visible to a
    /// query, so an unflushed tier survives a crash without a file per metric per minute.
    /// </summary>
    private readonly MetricWriteAheadLog _wal;

    /// <summary>
    /// Makes "log the point, then publish it" atomic against a flush taking its snapshot.
    /// Ingest holds it shared and stays parallel — concurrent ingests are already safe
    /// against each other (a concurrent dictionary plus a per-series lock, which is how this
    /// worked before the log existed); only the drain needs exclusion, and it holds the lock
    /// for the drain alone, never while files are written.
    /// </summary>
    private readonly ReaderWriterLockSlim _snapshotLock = new();

    // ── Hot tier ─────────────────────────────────────────────────────────────
    private readonly ConcurrentDictionary<SeriesKey, HotSeries> _hot = new();
    private          int _hotPointCount;
    // 1 while a threshold-triggered flush is queued/running. Without this gate, every
    // Ingest call past the threshold spawned ANOTHER Task.Run until the flush finally
    // reset the counter — a stampede of concurrent flush tasks under load.
    private          int _thresholdFlushScheduled;

    // ── Metadata catalog (maintained at ingestion, survives hot-tier drains) ───
    private readonly ConcurrentDictionary<string, MetricMeta> _meta =
        new(StringComparer.Ordinal);

    // ── Exemplars (recent, in-memory ring per metric — for metric→trace jumps) ──
    private const int ExemplarsPerMetric = 4_000;
    private readonly ConcurrentDictionary<string, ExemplarRing> _exemplars =
        new(StringComparer.Ordinal);

    // ── Cold tier ─────────────────────────────────────────────────────────────
    private readonly List<MetricSegmentInfo>      _coldSegments = new();
    private readonly ReaderWriterLockSlim          _coldLock     = new();

    private readonly string                        _dataDir;
    private readonly ILogger<MetricStorageEngine>  _logger;

    // ── Background tasks ──────────────────────────────────────────────────────
    private readonly CancellationTokenSource _cts        = new();
    private readonly Task                    _flushTask;
    private readonly Task                    _rollupTask;

    // 0 = live, 1 = disposed. Guards against the multiple DisposeAsync calls
    // that occur at host shutdown (see DisposeAsync).
    private int _disposed;

    public MetricStorageEngine(string dataDir, ILogger<MetricStorageEngine> logger)
    {
        _dataDir = dataDir;
        _logger  = logger;
        Directory.CreateDirectory(dataDir);

        // The WAL, unlike cold-segment discovery, must be open and replayed before the first
        // point is accepted, or a restart would interleave recovered and live data. Replay is
        // a sequential walk of one mmap'd file bounded by the flush thresholds.
        _wal = MetricWriteAheadLog.Open(Path.Combine(dataDir, "metrics.wal"));
        RecoverFromWal();

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

        long oldestNano = long.MaxValue;
        foreach (var r in recovered)
        {
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
            _hotSince = at < DateTime.UtcNow ? at : DateTime.UtcNow;
        }

        _logger.LogInformation("Recovered {Count} metric point(s) from the write-ahead log", recovered.Count);
    }

    // ── IMetricIngester ───────────────────────────────────────────────────────

    public void Ingest(ReadOnlySpan<MetricIngestItem> items)
    {
        int total = 0;

        // Logging a point and making it visible must be one step with respect to a flush's
        // snapshot, or a point that lands between the two would be in neither the files nor
        // (after the commit) the log — durable nowhere despite the guarantee above. Held
        // shared: this excludes the drain, not other ingests, which need no exclusion.
        _snapshotLock.EnterReadLock();
        try
        {
            foreach (var item in items)
            {
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
                total = ApplyToHotTier(item, in point);
            }
        }
        finally { _snapshotLock.ExitReadLock(); }

        // Exemplars live in their own ring and are not logged, so they stay off that path.
        foreach (var item in items)
        {
            if (item.Exemplars is not { Length: > 0 } exs) continue;
            var ring = _exemplars.GetOrAdd(item.Name, static _ => new ExemplarRing(ExemplarsPerMetric));
            foreach (var ex in exs)
                ring.Add(new ExemplarSample
                {
                    TimestampUnixNano = ex.TimestampUnixNano,
                    Value             = ex.Value,
                    TraceId           = ex.TraceId,
                    SpanId            = ex.SpanId,
                    Labels            = item.Labels,
                });
        }

        if (total >= HotFlushThreshold
            && System.Threading.Interlocked.CompareExchange(ref _thresholdFlushScheduled, 1, 0) == 0)
        {
            _ = Task.Run(async () =>
            {
                try { await FlushHotTierAsync().ConfigureAwait(false); }
                finally { System.Threading.Interlocked.Exchange(ref _thresholdFlushScheduled, 0); }
            });
        }
    }

    /// <summary>
    /// Files one point into its series and the metadata catalog, returning the new hot-tier
    /// point count. Shared by live ingest and WAL replay — replay must not write back into
    /// the log it is reading from, and must not trigger a flush from the constructor.
    /// </summary>
    private int ApplyToHotTier(MetricIngestItem item, in MetricDataPoint point)
    {
        var key    = new SeriesKey(item.Name, item.Kind, item.Unit, item.Labels);
        var series = _hot.GetOrAdd(key, static _ => new HotSeries());

        series.Append(point, item.BucketBounds);
        UpdateMeta(item);

        int total = System.Threading.Interlocked.Increment(ref _hotPointCount);
        if (total == 1) _hotSince = DateTime.UtcNow;   // tier went from empty to holding data
        return total;
    }

    private void UpdateMeta(MetricIngestItem item)
    {
        var meta = _meta.GetOrAdd(item.Name, static _ => new MetricMeta());
        meta.Kind = item.Kind;
        if (!string.IsNullOrEmpty(item.Unit)) meta.Unit = item.Unit;

        long ms = item.TimestampUnixNano / 1_000_000L;
        if (ms > meta.LastSeenMs) meta.LastSeenMs = ms;

        foreach (var (k, v) in item.Labels.Pairs)
        {
            var values = meta.LabelValues.GetOrAdd(k, static _ => new ConcurrentDictionary<string, byte>(StringComparer.Ordinal));
            if (values.Count < MaxLabelValuesPerKey) values.TryAdd(v, 0);
        }

        meta.AddSeries(item.Labels.GetHashCode());
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

    public IEnumerable<string> GetMetricNames(string? prefix = null)
    {
        var hotNames = _hot.Keys
            .Select(k => k.Name)
            .Distinct(StringComparer.Ordinal);

        _coldLock.EnterReadLock();
        IEnumerable<string> coldNames;
        try { coldNames = _coldSegments.Select(s => s.MetricName).Distinct(StringComparer.Ordinal).ToList(); }
        finally { _coldLock.ExitReadLock(); }

        return hotNames.Concat(coldNames)
            .Distinct(StringComparer.Ordinal)
            .Where(n => prefix is null || n.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            .OrderBy(n => n);
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

        // Cold tier
        List<MetricSegmentInfo> coldCandidates;
        _coldLock.EnterReadLock();
        try
        {
            coldCandidates = _coldSegments
                .Where(s => s.MetricName.Equals(metricName, StringComparison.OrdinalIgnoreCase)
                         && s.MaxNano >= fromNano && s.MinNano <= toNano)
                .ToList();
        }
        finally { _coldLock.ExitReadLock(); }

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

        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(TimeSpan.FromSeconds(FlushCheckIntervalSeconds), ct); }
            catch (OperationCanceledException) { break; }
            await FlushIfDueAsync();
        }
        // Final flush on shutdown — unconditional, so a clean stop leaves nothing to replay.
        await FlushHotTierAsync();
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

        bool due = points >= MinFlushPoints
                || (_hotSince is { } since && DateTime.UtcNow - since >= MaxHotAge);
        if (due) await FlushHotTierAsync().ConfigureAwait(false);
    }

    private async Task FlushHotTierAsync()
    {
        if (_hot.IsEmpty) return;

        // Snapshot and clear hot tier. Bounds must travel with the snapshot —
        // without them cold histogram files have no bucket bounds and quantile /
        // heatmap queries over anything older than the hot tier return nothing.
        //
        // The drain, the counter reset and the log's generation bump are one atomic step
        // against ingest: a point that arrives while this runs must land wholly on one side,
        // either inside the snapshot or stamped with the next generation. Writing the files
        // happens after the lock is released, so ingest is never blocked on disk.
        var snapshot = new List<(SeriesKey Key, HotSeries Series)>();
        ulong flushedGeneration;
        _snapshotLock.EnterWriteLock();
        try
        {
            foreach (var (k, v) in _hot)
            {
                var points = v.Drain();
                if (points.Count > 0)
                    snapshot.Add((k, new HotSeries(points, v.Bounds)));
            }

            if (snapshot.Count == 0) return;

            System.Threading.Interlocked.Exchange(ref _hotPointCount, 0);
            _hotSince = null;
            flushedGeneration = _wal.BeginFlush();
        }
        finally { _snapshotLock.ExitWriteLock(); }

        try
        {
            var infos = MetricWriter.Write(_dataDir, snapshot);
            _coldLock.EnterWriteLock();
            try { _coldSegments.AddRange(infos); }
            finally { _coldLock.ExitWriteLock(); }

            _logger.LogDebug("Flushed {SeriesCount} metric series to {FileCount} .mts files",
                snapshot.Count, infos.Count);
        }
        catch (Exception ex)
        {
            // The points were drained out of _hot before the write, so a failure here used to
            // lose them outright. Put them back: the log still holds them, but only a restart
            // would have brought them back, and a transient disk error is not a restart.
            int restored = 0;
            _snapshotLock.EnterWriteLock();
            try
            {
                foreach (var (key, snap) in snapshot)
                {
                    var live = _hot.GetOrAdd(key, static _ => new HotSeries());
                    foreach (var p in snap.GetPoints(long.MinValue, long.MaxValue))
                    {
                        live.Append(p, snap.Bounds);
                        restored++;
                    }
                }
                System.Threading.Interlocked.Add(ref _hotPointCount, restored);
                if (restored > 0) _hotSince ??= DateTime.UtcNow;
            }
            finally { _snapshotLock.ExitWriteLock(); }

            _logger.LogError(ex,
                "Failed to flush metric hot tier — {Count} point(s) kept in memory for the next attempt",
                restored);
            // No commit: the snapshot's records stay below an unchanged watermark, so a crash
            // before the retry replays them.
            return;
        }

        // Files are on disk. Committing the flushed generation reclaims its space and leaves
        // everything appended during the write — which is durable nowhere else — untouched.
        try { _wal.CommitFlush(flushedGeneration); }
        catch (Exception ex) { _logger.LogWarning(ex, "Metric WAL commit failed after flush"); }

        await Task.CompletedTask;
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
        _coldLock.EnterReadLock();
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

                _coldLock.EnterWriteLock();
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

                _coldLock.EnterWriteLock();
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
        var sw     = System.Diagnostics.Stopwatch.StartNew();
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
        _coldLock.EnterWriteLock();
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
                    var meta = _meta.GetOrAdd(s.Name, static _ => new MetricMeta());
                    meta.Kind = s.Kind;
                    if (!string.IsNullOrEmpty(s.Unit)) meta.Unit = s.Unit;
                    long lastMs = (s.Points.Count > 0 ? s.Points[^1].TimestampUnixNano : seg.MaxNano) / 1_000_000L;
                    if (lastMs > meta.LastSeenMs) meta.LastSeenMs = lastMs;
                    foreach (var (k, v) in s.Labels.Pairs)
                    {
                        var values = meta.LabelValues.GetOrAdd(k, static _ => new ConcurrentDictionary<string, byte>(StringComparer.Ordinal));
                        if (values.Count < MaxLabelValuesPerKey) values.TryAdd(v, 0);
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
        // throws ObjectDisposedException, so bail out after the first pass.
        if (System.Threading.Interlocked.Exchange(ref _disposed, 1) != 0) return;

        _cts.Cancel();
        try { await Task.WhenAll(_flushTask, _rollupTask); }
        catch (OperationCanceledException) { }
        _cts.Dispose();
        _coldLock.Dispose();
        _snapshotLock.Dispose();
        _wal.Dispose();   // after the loop's final flush, which commits it
    }

    // ── IRetentionTarget ──────────────────────────────────────────────────────

    public string RetentionKey => "metrics";

    /// <summary>Last TTL retention pruned with — bounds the 1-h merge window (default 7 days).</summary>
    private long _lastPruneTtlTicks = TimeSpan.FromDays(7).Ticks;

    public Task<int> PruneAsync(TimeSpan ttl, CancellationToken ct = default)
    {
        Interlocked.Exchange(ref _lastPruneTtlTicks, ttl.Ticks);
        var cutoffNano = DateTimeOffset.UtcNow.Subtract(ttl).ToUnixTimeMilliseconds() * 1_000_000L;

        List<MetricSegmentInfo> toDelete;
        _coldLock.EnterWriteLock();
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
    private const int MaxTrackedSeries = 50_000;

    public MetricKind Kind        { get; set; }
    public string     Unit        { get; set; } = string.Empty;
    public long       LastSeenMs  { get; set; }

    /// <summary>label key → set of observed values (capped per key).</summary>
    public ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> LabelValues { get; } =
        new(StringComparer.Ordinal);

    private readonly HashSet<int> _seriesHashes = new();
    private readonly object       _lock         = new();

    public int Cardinality
    {
        get { lock (_lock) return _seriesHashes.Count; }
    }

    public void AddSeries(int labelSetHash)
    {
        lock (_lock)
        {
            if (_seriesHashes.Count < MaxTrackedSeries)
                _seriesHashes.Add(labelSetHash);
        }
    }
}

internal sealed class HotSeries
{
    private readonly List<MetricDataPoint> _points;
    private readonly object _lock = new();

    /// <summary>
    /// Histogram bucket upper bounds shared by every point. Set once from the first
    /// histogram point; null for scalar series.
    /// </summary>
    public double[]? Bounds { get; private set; }

    public HotSeries() => _points = new List<MetricDataPoint>(64);

    public HotSeries(List<MetricDataPoint> points, double[]? bounds = null)
    {
        _points = points;
        Bounds  = bounds;
    }

    public void Append(MetricDataPoint p, double[]? bounds = null)
    {
        lock (_lock)
        {
            if (bounds is not null && Bounds is null) Bounds = bounds;
            _points.Add(p);
        }
    }

    public List<MetricDataPoint> Drain()
    {
        lock (_lock)
        {
            var copy = new List<MetricDataPoint>(_points);
            _points.Clear();
            return copy;
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
