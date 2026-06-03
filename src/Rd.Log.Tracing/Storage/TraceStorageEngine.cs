using System.Collections.Concurrent;
using MessagePack;
using Microsoft.Extensions.Logging;
using Rd.Log.Core;

namespace Rd.Log.Tracing.Storage;

/// <summary>
/// Coordinates hot-tier span storage and cold-tier flush.
///
/// Hot tier: an in-memory list of <see cref="SpanRecord"/> objects with a
/// <c>TraceId → List&lt;int&gt;</c> inverted index for fast trace assembly.
///
/// Cold tier: flushed as <c>.trc</c> files by <see cref="SpanWriter"/>
/// when the hot segment reaches its size/time threshold.
/// </summary>
public sealed class TraceStorageEngine : ITraceProvider, IRetentionTarget, IDisposable
{
    // ── Hot tier ─────────────────────────────────────────────────────────────
    private readonly List<SpanRecord>                          _hotSpans  = new();
    private readonly Dictionary<TraceId, List<int>>            _traceIdx  = new();
    private readonly ReaderWriterLockSlim                      _lock      = new();

    // ── Cold tier ─────────────────────────────────────────────────────────────
    private readonly string                                    _dataDir;
    private readonly List<SpanSegmentInfo>                     _coldSegments = new();
    private readonly ILogger<TraceStorageEngine>               _logger;

    private const int HotFlushThreshold    = 50_000;  // spans before flush
    private const int CompactionThreshold  = 10_000;  // merge cold segments smaller than this

    public TraceStorageEngine(string dataDir, ILogger<TraceStorageEngine> logger)
    {
        _dataDir = dataDir;
        _logger  = logger;
        Directory.CreateDirectory(dataDir);
        LoadColdSegments();
    }

    // ── Ingestion (called by SpanDrainer) ─────────────────────────────────────

    internal void WriteSpan(SpanIngestItem item)
    {
        var record = new SpanRecord
        {
            TraceId           = item.TraceId,
            SpanId            = item.SpanId,
            ParentSpanId      = item.ParentSpanId,
            StartTimeUnixNano = item.StartTimeUnixNano,
            DurationNanos     = item.DurationNanos,
            Name              = item.Name,
            ServiceName       = item.ServiceName,
            Kind              = item.Kind,
            Status            = item.Status,
            Attributes        = item.AttributesBytes.Length > 0
                                    ? DeserializeAttributes(item.AttributesBytes)
                                    : null,
        };

        _lock.EnterWriteLock();
        try
        {
            int offset = _hotSpans.Count;
            _hotSpans.Add(record);

            if (!_traceIdx.TryGetValue(item.TraceId, out var offsets))
            {
                offsets = new List<int>(4);
                _traceIdx[item.TraceId] = offsets;
            }
            offsets.Add(offset);

            if (_hotSpans.Count >= HotFlushThreshold)
                FlushHotTierLocked();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    // ── Query ─────────────────────────────────────────────────────────────────

    public async IAsyncEnumerable<SpanRecord> GetTraceAsync(
        TraceId traceId,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        // Hot tier
        _lock.EnterReadLock();
        List<SpanRecord>? hotResults = null;
        try
        {
            if (_traceIdx.TryGetValue(traceId, out var offsets))
            {
                hotResults = new List<SpanRecord>(offsets.Count);
                foreach (var o in offsets)
                    hotResults.Add(_hotSpans[o]);
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }

        if (hotResults != null)
            foreach (var r in hotResults.OrderBy(s => s.StartTimeUnixNano))
                yield return r;

        // Cold tier
        foreach (var seg in _coldSegments)
        {
            ct.ThrowIfCancellationRequested();
            await foreach (var r in SpanReader.ReadTraceAsync(seg.FilePath, traceId, ct))
                yield return r;
        }
    }

    public async IAsyncEnumerable<SpanRecord> SearchSpansAsync(
        DateTimeOffset?   from             = null,
        DateTimeOffset?   to               = null,
        string?           serviceName      = null,
        string?           spanName         = null,
        SpanStatusCode?   status           = null,
        long?             minDurationNanos = null,
        long?             maxDurationNanos = null,
        int               limit            = 200,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        long fromNano = from.HasValue ? from.Value.ToUnixTimeMilliseconds() * 1_000_000L : long.MinValue;
        long toNano   = to.HasValue   ? to.Value.ToUnixTimeMilliseconds()   * 1_000_000L : long.MaxValue;

        int yielded = 0;

        // Hot tier (newest first)
        _lock.EnterReadLock();
        List<SpanRecord>? candidates = null;
        try
        {
            candidates = _hotSpans
                .Where(s =>
                    s.StartTimeUnixNano >= fromNano &&
                    s.StartTimeUnixNano <= toNano   &&
                    (serviceName      is null || s.ServiceName.Equals(serviceName, StringComparison.OrdinalIgnoreCase)) &&
                    (spanName         is null || s.Name.Contains(spanName, StringComparison.OrdinalIgnoreCase)) &&
                    (status           is null || s.Status == status.Value) &&
                    (minDurationNanos is null || s.DurationNanos >= minDurationNanos.Value) &&
                    (maxDurationNanos is null || s.DurationNanos <= maxDurationNanos.Value))
                .OrderByDescending(s => s.StartTimeUnixNano)
                .Take(limit)
                .ToList();
        }
        finally
        {
            _lock.ExitReadLock();
        }

        foreach (var r in candidates)
        {
            if (yielded++ >= limit) yield break;
            yield return r;
        }

        if (yielded >= limit) yield break;

        // Cold tier
        foreach (var seg in _coldSegments.OrderByDescending(s => s.MaxStartNano))
        {
            if (seg.MaxStartNano < fromNano || seg.MinStartNano > toNano) continue;
            ct.ThrowIfCancellationRequested();

            await foreach (var r in SpanReader.SearchAsync(seg.FilePath, fromNano, toNano, serviceName, spanName, status, minDurationNanos, maxDurationNanos, ct))
            {
                if (yielded++ >= limit) yield break;
                yield return r;
            }
        }
    }

    // ── Flush ─────────────────────────────────────────────────────────────────

    private void FlushHotTierLocked()
    {
        if (_hotSpans.Count == 0) return;

        try
        {
            var info = SpanWriter.Write(_dataDir, _hotSpans);
            _coldSegments.Add(info);
            _logger.LogInformation("Flushed {Count} spans to {File}", _hotSpans.Count, info.FilePath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to flush hot-tier spans to cold storage");
            return;
        }

        _hotSpans.Clear();
        _traceIdx.Clear();
    }

    // ── Cold segment discovery ─────────────────────────────────────────────────

    private void LoadColdSegments()
    {
        foreach (var file in Directory.EnumerateFiles(_dataDir, "*.trc").OrderBy(f => f))
        {
            try
            {
                var info = SpanReader.ReadSegmentInfo(file);
                _coldSegments.Add(info);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Could not load cold span segment {File}", file);
            }
        }
        _logger.LogInformation("Loaded {Count} cold span segments", _coldSegments.Count);
        CompactSmallSegments();
    }

    internal void CompactSmallSegments()
    {
        var small = _coldSegments.Where(s => s.SpanCount < CompactionThreshold).ToList();
        if (small.Count < 2) return;

        var allSpans = new List<SpanRecord>();
        foreach (var seg in small)
        {
            try   { allSpans.AddRange(SpanReader.ReadAll(seg.FilePath)); }
            catch (Exception ex) { _logger.LogWarning(ex, "Compaction: failed to read {File}", seg.FilePath); }
        }

        if (allSpans.Count == 0) return;

        try
        {
            var merged = SpanWriter.Write(_dataDir, allSpans);
            _logger.LogInformation("Compacted {Count} small segments → {File} ({Spans} spans)",
                small.Count, Path.GetFileName(merged.FilePath), allSpans.Count);

            foreach (var seg in small)
            {
                _coldSegments.Remove(seg);
                try   { File.Delete(seg.FilePath); }
                catch (Exception ex) { _logger.LogWarning(ex, "Compaction: failed to delete {File}", seg.FilePath); }
            }
            _coldSegments.Add(merged);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Compaction: failed to write merged segment");
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static IReadOnlyDictionary<string, object?>? DeserializeAttributes(byte[] bytes)
    {
        try
        {
            return MessagePackSerializer.Deserialize<Dictionary<string, object?>>(bytes);
        }
        catch
        {
            return null;
        }
    }

    public void Dispose()
    {
        _lock.EnterWriteLock();
        try { FlushHotTierLocked(); }
        finally { _lock.ExitWriteLock(); }
        _lock.Dispose();
    }

    // ── IRetentionTarget ───────────────────────────────────────────────────

    public string RetentionKey => "traces";

    public Task<int> PruneAsync(TimeSpan ttl, CancellationToken ct = default)
    {
        var cutoffNano = DateTimeOffset.UtcNow.Subtract(ttl).ToUnixTimeMilliseconds() * 1_000_000L;

        List<SpanSegmentInfo> toDelete;
        _lock.EnterWriteLock();
        try
        {
            toDelete = _coldSegments.Where(s => s.MaxStartNano < cutoffNano).ToList();
            foreach (var s in toDelete)
                _coldSegments.Remove(s);
        }
        finally { _lock.ExitWriteLock(); }

        foreach (var s in toDelete)
            try { File.Delete(s.FilePath); } catch { /* best effort */ }

        if (toDelete.Count > 0)
            _logger.LogInformation("Retention pruned {Count} trace file(s) older than {Days} days",
                toDelete.Count, (int)ttl.TotalDays);

        return Task.FromResult(toDelete.Count);
    }
}

/// <summary>Metadata about a cold-tier span segment file.</summary>
public sealed class SpanSegmentInfo
{
    public string FilePath     { get; init; } = string.Empty;
    public long   MinStartNano { get; init; }
    public long   MaxStartNano { get; init; }
    public int    SpanCount    { get; init; }
}
