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
    private const int FlushIntervalSeconds  = 300;        // 5 minutes
    private const int MaxLabelValuesPerKey  = 2_000;      // cap to bound catalog memory

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
        LoadColdSegments();
        _flushTask  = Task.Run(FlushLoopAsync);
        _rollupTask = Task.Run(RollupLoopAsync);
    }

    // ── IMetricIngester ───────────────────────────────────────────────────────

    public void Ingest(ReadOnlySpan<MetricIngestItem> items)
    {
        foreach (var item in items)
        {
            var key    = new SeriesKey(item.Name, item.Kind, item.Unit, item.Labels);
            var series = _hot.GetOrAdd(key, _ => new HotSeries());

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

            series.Append(point, item.BucketBounds);
            UpdateMeta(item);

            if (item.Exemplars is { Length: > 0 } exs)
            {
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

            int total = System.Threading.Interlocked.Increment(ref _hotPointCount);
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
        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(TimeSpan.FromSeconds(FlushIntervalSeconds), ct); }
            catch (OperationCanceledException) { break; }
            await FlushHotTierAsync();
        }
        // Final flush on shutdown
        await FlushHotTierAsync();
    }

    private async Task FlushHotTierAsync()
    {
        if (_hot.IsEmpty) return;

        // Snapshot and clear hot tier
        var snapshot = new List<(SeriesKey Key, HotSeries Series)>();
        foreach (var (k, v) in _hot)
        {
            var points = v.Drain();
            if (points.Count > 0)
                snapshot.Add((k, new HotSeries(points)));
        }

        if (snapshot.Count == 0) return;

        System.Threading.Interlocked.Exchange(ref _hotPointCount, 0);

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
            _logger.LogError(ex, "Failed to flush metric hot tier");
        }

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
        }
        finally { _coldLock.ExitReadLock(); }

        if (toCompact.Count >= 2)  CompactRawSegments(toCompact);
        if (toRollup5m.Count > 0)  Rollup(toRollup5m, MetricGranularity.FiveMin, TimeSpan.FromMinutes(5));
        if (toRollup1h.Count > 0)  Rollup(toRollup1h, MetricGranularity.OneHour, TimeSpan.FromHours(1));

        return Task.CompletedTask;
    }

    /// <summary>
    /// Merges multiple small raw segments for the same metric into one file
    /// without downsampling. Reduces file count for the current-hour window.
    /// </summary>
    private void CompactRawSegments(List<MetricSegmentInfo> sources)
    {
        foreach (var group in sources.GroupBy(s => s.MetricName))
        {
            var segs = group.ToList();
            if (segs.Count < 2) continue;

            try
            {
                var allSeries = new Dictionary<SeriesKey, List<MetricDataPoint>>();
                var bounds    = new Dictionary<SeriesKey, double[]?>();
                foreach (var seg in segs)
                {
                    foreach (var series in MetricReader.ReadAllSync(seg.FilePath))
                    {
                        var key = new SeriesKey(series.Name, series.Kind, series.Unit, series.Labels);
                        if (!allSeries.TryGetValue(key, out var pts))
                        {
                            pts = new List<MetricDataPoint>();
                            allSeries[key] = pts;
                        }
                        pts.AddRange(series.Points);
                        if (series.BucketBounds is not null) bounds[key] = series.BucketBounds;
                    }
                }

                var merged = allSeries
                    .Select(kv => (kv.Key, new HotSeries(
                        kv.Value.OrderBy(p => p.TimestampUnixNano).ToList(),
                        bounds.GetValueOrDefault(kv.Key))))
                    .ToList();

                var newInfos = MetricWriter.Write(_dataDir, merged, MetricGranularity.Raw);

                _coldLock.EnterWriteLock();
                try
                {
                    foreach (var s in segs) _coldSegments.Remove(s);
                    _coldSegments.AddRange(newInfos);
                }
                finally { _coldLock.ExitWriteLock(); }

                foreach (var s in segs)
                    try { File.Delete(s.FilePath); } catch { /* best effort */ }

                _logger.LogDebug("Compacted {Count} raw segments for metric '{Metric}'",
                    segs.Count, group.Key);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Raw compaction failed for metric '{Metric}'", group.Key);
            }
        }
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
                // Read all series from sources for this metric
                var allSeries = new Dictionary<SeriesKey, List<MetricDataPoint>>();
                var bounds    = new Dictionary<SeriesKey, double[]?>();
                foreach (var seg in group)
                {
                    foreach (var series in MetricReader.ReadAllSync(seg.FilePath))
                    {
                        var key = new SeriesKey(series.Name, series.Kind, series.Unit, series.Labels);
                        if (!allSeries.TryGetValue(key, out var pts))
                        {
                            pts = new List<MetricDataPoint>();
                            allSeries[key] = pts;
                        }
                        pts.AddRange(series.Points);
                        if (series.BucketBounds is not null) bounds[key] = series.BucketBounds;
                    }
                }

                // Aggregate into time buckets — type-aware (see Downsample).
                var rolled = new List<(SeriesKey Key, HotSeries Series)>();
                foreach (var (key, pts) in allSeries)
                {
                    var bucketed = Downsample(pts.OrderBy(p => p.TimestampUnixNano).ToList(), bucketSize, key.Kind)
                        .ToList();
                    rolled.Add((key, new HotSeries(bucketed, bounds.GetValueOrDefault(key))));
                }

                var newInfos = MetricWriter.Write(_dataDir, rolled, targetGranularity);

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
        foreach (var file in Directory.EnumerateFiles(_dataDir, "*.mts").OrderBy(f => f))
        {
            try
            {
                var info = MetricReader.ReadSegmentInfo(file);
                _coldSegments.Add(info);
            }
            catch (Exception ex)
            {
                // v1 files (no bucket data) are incompatible with the v2 format — delete them.
                _logger.LogWarning(ex, "Unreadable metric segment {File} — deleting (likely format v1)", file);
                try { File.Delete(file); } catch { /* best effort */ }
            }
        }
        _logger.LogInformation("Loaded {Count} cold metric segments", _coldSegments.Count);
        SeedCatalogFromCold();
    }

    /// <summary>
    /// Rebuilds the in-memory metric catalog from cold segments on startup so the
    /// Explore catalog / Overview detection work immediately after a restart, instead
    /// of staying blank until the next live export repopulates metadata.
    /// </summary>
    private void SeedCatalogFromCold()
    {
        int seeded = 0;
        foreach (var seg in _coldSegments)
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
    }

    // ── IRetentionTarget ──────────────────────────────────────────────────────

    public string RetentionKey => "metrics";

    public Task<int> PruneAsync(TimeSpan ttl, CancellationToken ct = default)
    {
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
}
