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
public sealed class MetricStorageEngine : IMetricIngester, IMetricQuery, IRetentionTarget, IAsyncDisposable
{
    // ── Configuration ─────────────────────────────────────────────────────────
    private const int HotFlushThreshold    = 500_000;   // total points before forced flush
    private const int FlushIntervalSeconds  = 300;        // 5 minutes

    // ── Hot tier ─────────────────────────────────────────────────────────────
    private readonly ConcurrentDictionary<SeriesKey, HotSeries> _hot = new();
    private          int _hotPointCount;

    // ── Cold tier ─────────────────────────────────────────────────────────────
    private readonly List<MetricSegmentInfo>      _coldSegments = new();
    private readonly ReaderWriterLockSlim          _coldLock     = new();

    private readonly string                        _dataDir;
    private readonly ILogger<MetricStorageEngine>  _logger;

    // ── Background tasks ──────────────────────────────────────────────────────
    private readonly CancellationTokenSource _cts        = new();
    private readonly Task                    _flushTask;
    private readonly Task                    _rollupTask;

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
            };

            series.Append(point);

            int total = System.Threading.Interlocked.Increment(ref _hotPointCount);
            if (total >= HotFlushThreshold)
                _ = Task.Run(FlushHotTierAsync);
        }
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
                Name   = key.Name,
                Kind   = key.Kind,
                Unit   = key.Unit,
                Labels = key.Labels,
                Points = step.HasValue ? Downsample(points, step.Value) : points,
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
                var points = step.HasValue ? Downsample(series.Points, step.Value) : series.Points;
                yield return new MetricSeries
                {
                    Name   = series.Name,
                    Kind   = series.Kind,
                    Unit   = series.Unit,
                    Labels = series.Labels,
                    Points = points,
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
                Name   = key.Name,
                Kind   = key.Kind,
                Unit   = key.Unit,
                Labels = key.Labels,
                Points = [latest.Value],
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
                    }
                }

                var merged = allSeries
                    .Select(kv => (kv.Key, new HotSeries(kv.Value.OrderBy(p => p.TimestampUnixNano).ToList())))
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
                    }
                }

                // Aggregate into buckets
                var rolled = new List<(SeriesKey Key, HotSeries Series)>();
                long bucketNanos = (long)bucketSize.TotalMilliseconds * 1_000_000L;
                foreach (var (key, pts) in allSeries)
                {
                    var bucketed = pts
                        .GroupBy(p => p.TimestampUnixNano / bucketNanos * bucketNanos)
                        .Select(g => new MetricDataPoint
                        {
                            TimestampUnixNano = g.Key,
                            Value             = g.Average(p => p.Value),
                            Count             = g.Sum(p => p.Count),
                            Sum               = g.Sum(p => p.Sum),
                        })
                        .OrderBy(p => p.TimestampUnixNano)
                        .ToList();

                    rolled.Add((key, new HotSeries(bucketed)));
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
            if (!pairs.TryGetValue(k, out var actual) || actual != v) return false;
        return true;
    }

    private static IReadOnlyList<MetricDataPoint> Downsample(
        IReadOnlyList<MetricDataPoint> points,
        TimeSpan step)
    {
        long bucketNanos = (long)step.TotalMilliseconds * 1_000_000L;
        return points
            .GroupBy(p => p.TimestampUnixNano / bucketNanos * bucketNanos)
            .Select(g => new MetricDataPoint
            {
                TimestampUnixNano = g.Key,
                Value             = g.Average(p => p.Value),
                Count             = g.Sum(p => p.Count),
                Sum               = g.Sum(p => p.Sum),
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
                _logger.LogWarning(ex, "Could not load cold metric segment {File}", file);
            }
        }
        _logger.LogInformation("Loaded {Count} cold metric segments", _coldSegments.Count);
    }

    public async ValueTask DisposeAsync()
    {
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

internal sealed class HotSeries
{
    private readonly List<MetricDataPoint> _points;
    private readonly object _lock = new();

    public HotSeries() => _points = new List<MetricDataPoint>(64);

    public HotSeries(List<MetricDataPoint> points) => _points = points;

    public void Append(MetricDataPoint p)
    {
        lock (_lock) _points.Add(p);
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
