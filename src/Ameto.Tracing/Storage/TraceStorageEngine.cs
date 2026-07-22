using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using MessagePack;
using Microsoft.Extensions.Logging;
using Ameto.Core;

namespace Ameto.Tracing.Storage;

/// <summary>
/// Coordinates hot-tier span storage and cold-tier flush.
///
/// Hot tier: an in-memory list of <see cref="SpanRecord"/> objects with a
/// <c>TraceId → List&lt;int&gt;</c> inverted index for fast trace assembly.
///
/// Cold tier: flushed as <c>.trc</c> files by <see cref="SpanWriter"/>
/// when the hot segment reaches its size/time threshold.
/// </summary>
public sealed class TraceStorageEngine : ITraceProvider, ITraceStatsProvider, IServiceGraphProvider, ITraceSummaryProvider, IRetentionTarget, IDisposable
{
    // ── Hot tier ─────────────────────────────────────────────────────────────
    private readonly List<SpanRecord>                          _hotSpans  = new();
    private readonly Dictionary<TraceId, List<int>>            _traceIdx  = new();
    private readonly ReaderWriterLockSlim                      _lock      = new();

    // 0 = live, 1 = disposed. Guards against multiple Dispose calls: the engine
    // is registered as several singleton interfaces, so the DI container captures
    // and disposes the same instance more than once at shutdown.
    private int _disposed;

    // ── Cold tier ─────────────────────────────────────────────────────────────
    private readonly string                                    _dataDir;
    // Immutable snapshot, swapped under _lock's WRITE lock on every mutation
    // (flush/compaction/retention/self-heal). Readers grab the field once and
    // iterate without locks — a concurrent swap can never fault them, and a
    // deleted file surfaces as a per-segment skip, not a request failure.
    private volatile SpanSegmentInfo[]                         _coldSegments = [];
    private readonly ILogger<TraceStorageEngine>               _logger;

    private const int HotFlushThreshold    = 50_000;  // spans before flush
    private const int CompactionThreshold  = 10_000;  // merge cold segments smaller than this
    private const int MaxSegmentsPerPass   = 20;       // merge at most N oldest small segments per run
    private const int MaxSpansPerPass      = 200_000;  // hard cap on spans loaded into memory per run

    public TraceStorageEngine(string dataDir, ILogger<TraceStorageEngine> logger)
    {
        _dataDir = dataDir;
        _logger  = logger;
        Directory.CreateDirectory(dataDir);
        // Cold-segment discovery is deliberately NOT done here: the constructor
        // runs before Kestrel binds, and scanning thousands of .trc files would
        // delay ingest availability. TraceCompactionWorker calls
        // LoadColdSegments() in the background right after startup.
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
            HttpStatusCode    = item.HttpStatusCode,  // promoted — no attrs deserialization
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

        // Cold tier — scan the snapshot in parallel (bounded): a by-id lookup has
        // no time bounds, so every segment must be consulted, and doing that
        // sequentially took whole seconds once small segments piled up. A file
        // that compaction/retention deleted mid-flight is skipped (and healed out
        // of the snapshot) instead of failing the whole request.
        var segs = _coldSegments;
        if (segs.Length == 0) yield break;

        using var gate = new SemaphoreSlim(Math.Clamp(Environment.ProcessorCount / 2, 2, 8));
        var tasks = new Task<List<SpanRecord>?>[segs.Length];
        for (int i = 0; i < segs.Length; i++)
        {
            var seg = segs[i];
            tasks[i] = Task.Run(async () =>
            {
                await gate.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    List<SpanRecord>? found = null;
                    await foreach (var r in SpanReader.ReadTraceAsync(seg.FilePath, traceId, ct).ConfigureAwait(false))
                        (found ??= new List<SpanRecord>()).Add(r);
                    return found;
                }
                catch (OperationCanceledException) { throw; }
                catch (FileNotFoundException)
                {
                    RemoveColdSegment(seg);   // deleted behind our back — heal the snapshot
                    return null;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Trace lookup: skipping unreadable segment {File}", seg.FilePath);
                    return null;
                }
                finally { gate.Release(); }
            }, ct);
        }

        var cold = new List<SpanRecord>();
        foreach (var t in tasks)
            if (await t.ConfigureAwait(false) is { } part)
                cold.AddRange(part);

        foreach (var r in cold.OrderBy(s => s.StartTimeUnixNano))
            yield return r;
    }

    /// <summary>Drops a segment whose file no longer exists from the snapshot.</summary>
    private void RemoveColdSegment(SpanSegmentInfo seg)
    {
        _lock.EnterWriteLock();
        try
        {
            _coldSegments = Array.FindAll(_coldSegments, s => !ReferenceEquals(s, seg));
        }
        finally { _lock.ExitWriteLock(); }
        _logger.LogWarning("Cold span segment {File} vanished from disk — removed from the segment list", seg.FilePath);
    }

    public async IAsyncEnumerable<SpanRecord> SearchSpansAsync(
        DateTimeOffset?   from             = null,
        DateTimeOffset?   to               = null,
        string?           serviceName      = null,
        string?           spanName         = null,
        SpanStatusCode?   status           = null,
        long?             minDurationNanos = null,
        long?             maxDurationNanos = null,
        short?            httpStatusCode   = null,
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
                    (httpStatusCode   is null || s.HttpStatusCode == httpStatusCode.Value) &&
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

        // Cold tier — segment-level service pre-filter, then block-level skip inside SpanReader
        foreach (var seg in _coldSegments.OrderByDescending(s => s.MaxStartNano))
        {
            if (seg.MaxStartNano < fromNano || seg.MinStartNano > toNano) continue;
            // Skip entire segment if it doesn't contain the requested service
            if (serviceName is not null && seg.Services.Length > 0 &&
                !Array.Exists(seg.Services, s => s.Equals(serviceName, StringComparison.OrdinalIgnoreCase)))
                continue;

            ct.ThrowIfCancellationRequested();

            // Manual enumeration so a segment deleted/corrupted mid-scan skips the
            // segment (yield inside try-catch is not allowed by the language).
            await using var e = SpanReader.SearchAsync(
                seg.FilePath, fromNano, toNano,
                serviceName, spanName, status, httpStatusCode,
                minDurationNanos, maxDurationNanos, ct).GetAsyncEnumerator(ct);
            while (true)
            {
                SpanRecord r;
                try
                {
                    if (!await e.MoveNextAsync().ConfigureAwait(false)) break;
                    r = e.Current;
                }
                catch (OperationCanceledException) { throw; }
                catch (FileNotFoundException) { RemoveColdSegment(seg); break; }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Span search: skipping unreadable segment {File}", seg.FilePath);
                    break;
                }
                if (yielded++ >= limit) yield break;
                yield return r;
            }
        }
    }

    // ── Flush ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Flushes the in-memory hot tier to a cold segment. No-op when empty.
    /// Thread-safe — called periodically by <see cref="Ingestion.SpanDrainer"/> so
    /// spans are not stranded in RAM (and lost on restart) under low-traffic loads
    /// that never reach <see cref="HotFlushThreshold"/>.
    /// </summary>
    internal void FlushHotTier()
    {
        _lock.EnterWriteLock();
        try { FlushHotTierLocked(); }
        finally { _lock.ExitWriteLock(); }
    }

    private void FlushHotTierLocked()
    {
        if (_hotSpans.Count == 0) return;

        try
        {
            var info = SpanWriter.Write(_dataDir, _hotSpans);
            _coldSegments = [.. _coldSegments, info];   // under _lock write (see callers)
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

    /// <summary>
    /// Discovers existing cold segments. Runs in the background (see
    /// <c>TraceCompactionWorker</c>) — ingest and queries work from second zero,
    /// cold trace data becomes queryable when this completes. Merges with any
    /// segments flushed while the scan was running.
    /// </summary>
    internal void LoadColdSegments()
    {
        var sw     = System.Diagnostics.Stopwatch.StartNew();
        var loaded = new List<SpanSegmentInfo>();
        foreach (var file in Directory.EnumerateFiles(_dataDir, "*.trc").OrderBy(f => f))
        {
            try
            {
                loaded.Add(SpanReader.ReadSegmentInfo(file));
            }
            catch (Exception ex)
            {
                // v1 files (12-byte footer) will fail with wrong footer magic — delete them.
                _logger.LogWarning(ex, "Unreadable segment {File} — deleting (likely format v1)", file);
                DeleteSegmentFiles(file);
            }
        }

        _lock.EnterWriteLock();
        try
        {
            // Segments flushed while we were scanning are already in the snapshot;
            // keep them and add the discovered ones (dedup by path).
            var known = new HashSet<string>(_coldSegments.Select(s => s.FilePath), StringComparer.Ordinal);
            var next  = new List<SpanSegmentInfo>(loaded.Count + _coldSegments.Length);
            next.AddRange(loaded.Where(s => !known.Contains(s.FilePath)));
            next.AddRange(_coldSegments);
            _coldSegments = next.ToArray();
        }
        finally { _lock.ExitWriteLock(); }

        _logger.LogInformation("Loaded {Count} cold span segments in {Ms} ms",
            _coldSegments.Length, sw.ElapsedMilliseconds);
    }

    /// <summary>
    /// Merges small cold segments until the backlog is drained. Each pass stays
    /// memory-bounded (≤ MaxSegmentsPerPass files, ≤ MaxSpansPerPass spans), but
    /// passes repeat until nothing small remains — one hourly run used to merge a
    /// single batch of 20, which on a busy instance was slower than the flush rate
    /// produced new files, so the backlog only ever grew.
    /// </summary>
    internal void CompactSmallSegments()
    {
        const int MaxPasses = 500;   // safety valve, ~10k merged segments per run
        int passes = 0;
        while (CompactOnePass() && ++passes < MaxPasses) { }
        if (passes > 0)
            _logger.LogInformation("Compaction run finished: {Passes} pass(es), {Count} cold segments remain",
                passes, _coldSegments.Length);
    }

    private bool CompactOnePass()
    {
        // Bounded pass: take only the oldest small segments and cap the spans loaded
        // into memory. Compaction used to merge ALL small segments at once, which on a
        // memory-limited container exhausted the heap (tiny allocations threw OOM) and
        // left the segments un-compacted — so they piled up and every pass failed worse.
        var small = _coldSegments
            .Where(s => s.SpanCount < CompactionThreshold)
            .OrderBy(s => s.MinStartNano)   // oldest first
            .Take(MaxSegmentsPerPass)
            .ToList();
        if (small.Count < 2) return false;

        var allSpans  = new List<SpanRecord>();
        var processed = new List<SpanSegmentInfo>(small.Count);
        foreach (var seg in small)
        {
            if (allSpans.Count >= MaxSpansPerPass) break;   // memory cap reached — stop taking more
            try
            {
                allSpans.AddRange(SpanReader.ReadAll(seg.FilePath));
                processed.Add(seg);
            }
            catch (Exception ex) { _logger.LogWarning(ex, "Compaction: failed to read {File}", seg.FilePath); }
        }

        if (processed.Count < 2 || allSpans.Count == 0) return false;

        try
        {
            var merged = SpanWriter.Write(_dataDir, allSpans);
            _logger.LogInformation("Compacted {Count} small segments → {File} ({Spans} spans)",
                processed.Count, Path.GetFileName(merged.FilePath), allSpans.Count);

            // Swap the snapshot first (readers stop picking the old files up),
            // delete the merged-away files after. An in-flight reader that still
            // holds the old snapshot skips the deleted file gracefully.
            _lock.EnterWriteLock();
            try
            {
                var next = new List<SpanSegmentInfo>(_coldSegments.Length);
                foreach (var s in _coldSegments)
                    if (!processed.Contains(s)) next.Add(s);
                next.Add(merged);
                _coldSegments = next.ToArray();
            }
            finally { _lock.ExitWriteLock(); }

            foreach (var seg in processed)   // delete only the segments we actually merged
                DeleteSegmentFiles(seg.FilePath);   // .trc + all companion sidecars
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Compaction: failed to write merged segment");
            return false;
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

    // ── ITraceStatsProvider ────────────────────────────────────────────────────

    public Task<IReadOnlyList<ServiceSegmentStats>> GetAggregateStatsAsync(
        DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
    {
        long fromNano = from.ToUnixTimeMilliseconds() * 1_000_000L;
        long toNano   = to.ToUnixTimeMilliseconds()   * 1_000_000L;

        // Accumulator: service name → mutable bucket array + counters
        var agg = new Dictionary<string, (uint[] Buckets, uint Spans, uint Errors, long MinDur, long MaxDur)>(
            StringComparer.OrdinalIgnoreCase);

        void Merge(ServiceSegmentStats s)
        {
            if (!agg.TryGetValue(s.ServiceName, out var a))
            {
                var b = new uint[HistogramBuckets.Count];
                a = (b, 0, 0, long.MaxValue, long.MinValue);
            }
            for (int i = 0; i < HistogramBuckets.Count; i++) a.Buckets[i] += s.Buckets[i];
            a.Spans  += s.SpanCount;
            a.Errors += s.ErrorCount;
            if (s.MinDurationNanos < a.MinDur) a.MinDur = s.MinDurationNanos;
            if (s.MaxDurationNanos > a.MaxDur) a.MaxDur = s.MaxDurationNanos;
            agg[s.ServiceName] = a;
        }

        // Hot tier — compute on demand (≤50K spans, fast)
        _lock.EnterReadLock();
        try
        {
            foreach (var s in _hotSpans)
            {
                if (s.StartTimeUnixNano < fromNano || s.StartTimeUnixNano > toNano) continue;
                Merge(new ServiceSegmentStats
                {
                    ServiceName      = s.ServiceName,
                    SpanCount        = 1,
                    ErrorCount       = s.Status == SpanStatusCode.Error ? 1u : 0u,
                    MinDurationNanos = s.DurationNanos,
                    MaxDurationNanos = s.DurationNanos,
                    Buckets          = BucketOf(s.DurationNanos),
                });
            }
        }
        finally { _lock.ExitReadLock(); }

        // Cold tier — read .stats sidecar files only (no span deserialization)
        foreach (var seg in _coldSegments)
        {
            if (seg.MaxStartNano < fromNano || seg.MinStartNano > toNano) continue;
            foreach (var s in SpanReader.ReadStats(seg.FilePath))
                Merge(s);
        }

        var result = new List<ServiceSegmentStats>(agg.Count);
        foreach (var (name, (buckets, spans, errors, minDur, maxDur)) in agg)
            result.Add(new ServiceSegmentStats
            {
                ServiceName      = name,
                SpanCount        = spans,
                ErrorCount       = errors,
                MinDurationNanos = minDur == long.MaxValue ? 0 : minDur,
                MaxDurationNanos = maxDur == long.MinValue ? 0 : maxDur,
                Buckets          = buckets,
            });

        return Task.FromResult<IReadOnlyList<ServiceSegmentStats>>(result);
    }

    // ── IServiceGraphProvider ──────────────────────────────────────────────────

    public Task<ServiceGraphDto> GetServiceGraphAsync(
        DateTimeOffset from, DateTimeOffset to, CancellationToken ct = default)
    {
        long fromNano = from.ToUnixTimeMilliseconds() * 1_000_000L;
        long toNano   = to.ToUnixTimeMilliseconds()   * 1_000_000L;

        // Accumulate edges: (from, to) → (calls, errors, buckets)
        var edgeAgg = new Dictionary<(string, string), (uint Calls, uint Errors, uint[] Buckets)>(32);

        void MergeEdge(string f, string t, uint calls, uint errors, uint[] buckets)
        {
            var key = (f, t);
            if (!edgeAgg.TryGetValue(key, out var acc))
                acc = (0, 0, new uint[HistogramBuckets.Count]);
            acc.Calls  += calls;
            acc.Errors += errors;
            for (int i = 0; i < HistogramBuckets.Count; i++) acc.Buckets[i] += buckets[i];
            edgeAgg[key] = acc;
        }

        // Hot tier: derive edges on-demand (all spans in memory, accurate)
        _lock.EnterReadLock();
        try
        {
            if (_hotSpans.Count > 0)
            {
                var spanSvc = new Dictionary<SpanId, string>(_hotSpans.Count);
                foreach (var s in _hotSpans)
                    if (s.StartTimeUnixNano >= fromNano && s.StartTimeUnixNano <= toNano)
                        spanSvc[s.SpanId] = s.ServiceName;

                foreach (var s in _hotSpans)
                {
                    if (s.StartTimeUnixNano < fromNano || s.StartTimeUnixNano > toNano) continue;
                    if (s.ParentSpanId.IsEmpty) continue;
                    if (!spanSvc.TryGetValue(s.ParentSpanId, out var psvc)) continue;
                    if (string.Equals(psvc, s.ServiceName, StringComparison.Ordinal)) continue;
                    MergeEdge(psvc, s.ServiceName, 1,
                              s.Status == SpanStatusCode.Error ? 1u : 0u,
                              BucketOf(s.DurationNanos));
                }
            }
        }
        finally { _lock.ExitReadLock(); }

        // Cold tier — read .svcgraph sidecars (no span deserialization)
        foreach (var seg in _coldSegments)
        {
            if (seg.MaxStartNano < fromNano || seg.MinStartNano > toNano) continue;
            foreach (var e in ServiceGraphSidecar.ReadEdges(seg.FilePath))
                MergeEdge(e.From, e.To, e.CallCount, e.ErrorCount, e.Buckets);
        }

        // Build edges list
        var edgeDtos = new List<ServiceEdgeDto>(edgeAgg.Count);
        foreach (var ((from2, to2), (calls, errors, buckets)) in edgeAgg)
            edgeDtos.Add(new ServiceEdgeDto
            {
                From      = from2,
                To        = to2,
                CallCount = calls,
                ErrorCount= errors,
                ErrorRate = calls > 0 ? (double)errors / calls : 0,
                P95Ms     = HistogramBuckets.Percentile(buckets, 0.95),
            });

        // Derive nodes from edges + stats provider
        // Union all service names from edges
        var nodeNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var e in edgeDtos) { nodeNames.Add(e.From); nodeNames.Add(e.To); }

        // Load per-service stats for node metrics (reuse existing stats aggregation)
        // We call GetAggregateStatsAsync synchronously since it's Task.FromResult internally
        var statsTask = GetAggregateStatsAsync(from, to, ct);
        var statsMap  = new Dictionary<string, ServiceSegmentStats>(StringComparer.OrdinalIgnoreCase);
        // statsTask is already completed (Task.FromResult)
        foreach (var s in statsTask.Result)
            statsMap[s.ServiceName] = s;

        var nodeDtos = new List<ServiceNodeDto>(nodeNames.Count);
        foreach (var name in nodeNames)
        {
            statsMap.TryGetValue(name, out var st);
            nodeDtos.Add(new ServiceNodeDto
            {
                ServiceName = name,
                SpanCount   = st?.SpanCount ?? 0,
                ErrorRate   = st is { SpanCount: > 0 } ? (double)st.ErrorCount / st.SpanCount : 0,
                P95Ms       = st is not null ? HistogramBuckets.Percentile(st.Buckets, 0.95) : 0,
            });
        }

        return Task.FromResult(new ServiceGraphDto
        {
            Nodes = [.. nodeDtos],
            Edges = [.. edgeDtos],
        });
    }

    // ── ITraceSummaryProvider ──────────────────────────────────────────────────

    private static readonly string[] MethodKeys = { "http.request.method", "http.method" };
    private static readonly string[] PathKeys   = { "url.path", "http.target", "http.route", "url.full", "http.url" };

    /// <summary>
    /// Trace volume + sparkline over [from,to]. Cold tiers are served purely from the
    /// tiny <c>.tracesum</c> volume headers (no span deserialisation); the hot tier is
    /// grouped live. Bounded by (segments × grid-cells) — cheap for any window width.
    /// </summary>
    public async Task<TraceVolume> GetTraceVolumeAsync(
        DateTimeOffset from, DateTimeOffset to, int buckets, CancellationToken ct = default)
    {
        long fromNano  = from.ToUnixTimeMilliseconds() * 1_000_000L;
        long toNano    = to.ToUnixTimeMilliseconds()   * 1_000_000L;
        long rangeNano = Math.Max(1L, toNano - fromNano);
        if (buckets < 1) buckets = 1;

        var total = new double[buckets];
        var error = new double[buckets];
        int totalTraces = 0, errorTraces = 0;

        void Add(long startNano, uint traces, uint errors)
        {
            if (startNano < fromNano || startNano > toNano) return;
            int b = (int)Math.Clamp((startNano - fromNano) * (long)buckets / rangeNano, 0, buckets - 1);
            total[b]    += traces;
            error[b]    += errors;
            totalTraces += (int)traces;
            errorTraces += (int)errors;
        }

        // Snapshot cold segments + aggregate hot tier under one short read-lock.
        SpanSegmentInfo[] segs;
        _lock.EnterReadLock();
        try
        {
            segs = _coldSegments.ToArray();

            if (_hotSpans.Count > 0)
            {
                var hot = new Dictionary<TraceId, HotVolAcc>(_traceIdx.Count);
                foreach (var s in _hotSpans) AccumulateVolume(hot, s);
                foreach (var a in hot.Values) Add(a.HasRoot ? a.RootStart : a.Earliest, 1, a.Err ? 1u : 0u);
            }
        }
        finally { _lock.ExitReadLock(); }

        long half = TraceSummarySidecar.GridNanos / 2;
        foreach (var seg in segs)
        {
            if (seg.MaxStartNano < fromNano || seg.MinStartNano > toNano) continue;
            ct.ThrowIfCancellationRequested();

            var vs = TraceSummarySidecar.ReadVolume(seg.FilePath);
            if (vs is not null)
            {
                foreach (var e in vs.Buckets)
                    Add(e.GridIndex * TraceSummarySidecar.GridNanos + half, e.TraceCount, e.ErrorCount);
            }
            else
            {
                // Legacy segment written before .tracesum existed — derive volume from spans
                // (bounded per segment). Such segments vanish as retention/compaction ages them out.
                var legacy = new Dictionary<TraceId, HotVolAcc>();
                await foreach (var s in SpanReader.SearchAsync(
                    seg.FilePath, fromNano, toNano, null, null, null, null, null, null, ct))
                    AccumulateVolume(legacy, s);
                foreach (var a in legacy.Values) Add(a.HasRoot ? a.RootStart : a.Earliest, 1, a.Err ? 1u : 0u);
            }
        }

        return new TraceVolume
        {
            TotalTraces    = totalTraces,
            ErrorTraces    = errorTraces,
            TotalSparkline = total,
            ErrorSparkline = error,
        };
    }

    private static void AccumulateVolume(Dictionary<TraceId, HotVolAcc> acc, SpanRecord s)
    {
        ref var a = ref CollectionsMarshal.GetValueRefOrAddDefault(acc, s.TraceId, out _);
        if (!a.Init) { a.Init = true; a.Earliest = long.MaxValue; }
        if (s.Status == SpanStatusCode.Error) a.Err = true;
        if (s.StartTimeUnixNano < a.Earliest) a.Earliest = s.StartTimeUnixNano;
        if (s.ParentSpanId.IsEmpty && !a.HasRoot) { a.HasRoot = true; a.RootStart = s.StartTimeUnixNano; }
    }

    /// <summary>
    /// Newest-first, filtered trace rows. Cold tiers are served from <c>.tracesum</c> bodies
    /// (no span deserialisation); the hot tier is grouped live. Traces are merged by id across
    /// tiers, then the cheap filters are applied and the newest <paramref name="limit"/> kept.
    /// </summary>
    public async Task<IReadOnlyList<TraceSummary>> GetTraceListAsync(
        DateTimeOffset   from,
        DateTimeOffset   to,
        string?          serviceName,
        string?          spanName,
        SpanStatusCode?  status,
        long?            minDurationNanos,
        long?            maxDurationNanos,
        int              limit,
        CancellationToken ct = default)
    {
        long fromNano = from.ToUnixTimeMilliseconds() * 1_000_000L;
        long toNano   = to.ToUnixTimeMilliseconds()   * 1_000_000L;
        int  scanCap  = Math.Max(limit * 5, 500);

        var merged = new Dictionary<TraceId, MergedTrace>(scanCap);

        // Hot tier — group live spans (newest data) under read-lock. Snapshot cold too.
        SpanSegmentInfo[] segs;
        _lock.EnterReadLock();
        try
        {
            segs = _coldSegments.ToArray();
            foreach (var s in _hotSpans)
            {
                if (s.StartTimeUnixNano < fromNano || s.StartTimeUnixNano > toNano) continue;
                MergeSpanInto(merged, s);
            }
        }
        finally { _lock.ExitReadLock(); }

        // Cold — newest-first. .tracesum bodies where present, else legacy span read.
        Array.Sort(segs, static (a, b) => b.MaxStartNano.CompareTo(a.MaxStartNano));
        foreach (var seg in segs)
        {
            if (merged.Count >= scanCap) break;
            if (seg.MaxStartNano < fromNano || seg.MinStartNano > toNano) continue;
            if (serviceName is not null && seg.Services.Length > 0 &&
                !Array.Exists(seg.Services, x => x.Equals(serviceName, StringComparison.OrdinalIgnoreCase)))
                continue;

            ct.ThrowIfCancellationRequested();

            if (TraceSummarySidecar.Exists(seg.FilePath))
            {
                foreach (var r in TraceSummarySidecar.ReadSummaries(seg.FilePath))
                {
                    if (r.RootStartNano < fromNano || r.RootStartNano > toNano) continue;
                    MergeSummaryInto(merged, r);
                }
            }
            else
            {
                // Legacy segment — fall back to the span read (bounded by segment + filters).
                await foreach (var s in SpanReader.SearchAsync(
                    seg.FilePath, fromNano, toNano, serviceName, spanName, status, null,
                    minDurationNanos, maxDurationNanos, ct))
                    MergeSpanInto(merged, s);
            }
        }

        // Filter + sort newest-first + take limit.
        var list = new List<TraceSummary>(merged.Count);
        foreach (var m in merged.Values)
        {
            var rowStatus = m.HasError ? SpanStatusCode.Error : m.RootStatus;
            if (status is not null && rowStatus != status.Value) continue;
            if (serviceName is not null && !ServiceMatch(m, serviceName)) continue;
            if (spanName is not null && !m.Name.Contains(spanName, StringComparison.OrdinalIgnoreCase)) continue;
            if (minDurationNanos is not null && m.DurationNanos < minDurationNanos.Value) continue;
            if (maxDurationNanos is not null && m.DurationNanos > maxDurationNanos.Value) continue;
            list.Add(m.ToSummary());
        }

        list.Sort(static (a, b) => b.RootStartNano.CompareTo(a.RootStartNano));
        if (list.Count > limit) list.RemoveRange(limit, list.Count - limit);

        return list;
    }

    private static MergedTrace GetOrAdd(Dictionary<TraceId, MergedTrace> merged, TraceId id)
    {
        if (!merged.TryGetValue(id, out var m)) { m = new MergedTrace { TraceId = id }; merged[id] = m; }
        return m;
    }

    private static void MergeSpanInto(Dictionary<TraceId, MergedTrace> merged, SpanRecord s)
    {
        var m = GetOrAdd(merged, s.TraceId);
        m.SpanCount++;
        if (s.Status == SpanStatusCode.Error) m.HasError = true;
        m.Services.Add(s.ServiceName);
        if (s.StartTimeUnixNano < m.EarliestNano) { m.EarliestNano = s.StartTimeUnixNano; m.EarliestService = s.ServiceName; }
        if (s.ParentSpanId.IsEmpty && !m.HasRoot)
        {
            m.HasRoot        = true;
            m.RootSpanId     = s.SpanId;
            m.RootStartNano  = s.StartTimeUnixNano;
            m.DurationNanos  = s.DurationNanos;
            m.RootStatus     = s.Status;
            m.HttpStatusCode = s.HttpStatusCode;
            m.Name           = s.Name;
            m.ServiceName    = s.ServiceName;
            m.HttpMethod     = GetAttr(s.Attributes, MethodKeys);
            m.HttpPath       = GetAttr(s.Attributes, PathKeys);
        }
    }

    private static void MergeSummaryInto(Dictionary<TraceId, MergedTrace> merged, TraceSummary r)
    {
        var m = GetOrAdd(merged, r.TraceId);
        m.SpanCount += r.SpanCount;
        if (r.HasError) m.HasError = true;
        foreach (var sv in r.Services) m.Services.Add(sv);
        if (r.RootStartNano < m.EarliestNano) { m.EarliestNano = r.RootStartNano; m.EarliestService = r.ServiceName; }
        if (r.HasRoot && !m.HasRoot)
        {
            m.HasRoot        = true;
            m.RootSpanId     = r.RootSpanId;
            m.RootStartNano  = r.RootStartNano;
            m.DurationNanos  = r.DurationNanos;
            m.RootStatus     = r.RootStatus;
            m.HttpStatusCode = r.HttpStatusCode;
            m.Name           = r.Name;
            m.ServiceName    = r.ServiceName;
            m.HttpMethod     = r.HttpMethod;
            m.HttpPath       = r.HttpPath;
        }
    }

    private static bool ServiceMatch(MergedTrace m, string service)
    {
        if (m.ServiceName.Equals(service, StringComparison.OrdinalIgnoreCase)) return true;
        foreach (var sv in m.Services)
            if (sv.Equals(service, StringComparison.OrdinalIgnoreCase)) return true;
        return false;
    }

    private static string GetAttr(IReadOnlyDictionary<string, object?>? attrs, string[] keys)
    {
        if (attrs is null) return string.Empty;
        foreach (var k in keys)
            if (attrs.TryGetValue(k, out var v) && v is not null)
                return v.ToString() ?? string.Empty;
        return string.Empty;
    }

    private struct HotVolAcc
    {
        public bool Init;
        public long Earliest;
        public bool HasRoot;
        public long RootStart;
        public bool Err;
    }

    private sealed class MergedTrace
    {
        public TraceId         TraceId;
        public uint            SpanCount;
        public bool            HasError;
        public long            EarliestNano = long.MaxValue;
        public string          EarliestService = string.Empty;

        public bool            HasRoot;
        public SpanId          RootSpanId;
        public long            RootStartNano;
        public long            DurationNanos;
        public SpanStatusCode  RootStatus;
        public short           HttpStatusCode;
        public string          Name        = string.Empty;
        public string          ServiceName = string.Empty;
        public string          HttpMethod  = string.Empty;
        public string          HttpPath    = string.Empty;

        public readonly HashSet<string> Services = new(2, StringComparer.Ordinal);

        public TraceSummary ToSummary() => new()
        {
            TraceId        = TraceId,
            RootSpanId     = RootSpanId,
            RootStartNano  = HasRoot ? RootStartNano : EarliestNano,
            DurationNanos  = DurationNanos,
            SpanCount      = SpanCount,
            HasRoot        = HasRoot,
            HasError       = HasError,
            RootStatus     = RootStatus,
            HttpStatusCode = HttpStatusCode,
            Name           = Name,
            ServiceName    = HasRoot ? ServiceName : EarliestService,
            HttpMethod     = HttpMethod,
            HttpPath       = HttpPath,
            Services       = [.. Services],
        };
    }

    private static uint[] BucketOf(long durationNanos)
    {
        var b = new uint[HistogramBuckets.Count];
        b[HistogramBuckets.IndexOf(durationNanos)] = 1;
        return b;
    }

    public void Dispose()
    {
        if (System.Threading.Interlocked.Exchange(ref _disposed, 1) != 0) return;

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
            if (toDelete.Count > 0)
                _coldSegments = _coldSegments.Where(s => s.MaxStartNano >= cutoffNano).ToArray();
        }
        finally { _lock.ExitWriteLock(); }

        foreach (var s in toDelete)
            DeleteSegmentFiles(s.FilePath);

        if (toDelete.Count > 0)
            _logger.LogInformation("Retention pruned {Count} trace file(s) older than {Days} days",
                toDelete.Count, (int)ttl.TotalDays);

        return Task.FromResult(toDelete.Count);
    }

    /// <summary>Deletes a cold segment's <c>.trc</c> plus every companion sidecar. Best-effort.</summary>
    private static void DeleteSegmentFiles(string trcPath)
    {
        TryDelete(trcPath);
        TryDelete(Path.ChangeExtension(trcPath, ".stats"));
        TryDelete(Path.ChangeExtension(trcPath, ".svcgraph"));
        TryDelete(Path.ChangeExtension(trcPath, ".tracesum"));

        static void TryDelete(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); }
            catch { /* best effort */ }
        }
    }
}

/// <summary>Metadata about a cold-tier span segment file.</summary>
public sealed class SpanSegmentInfo
{
    public string   FilePath     { get; init; } = string.Empty;
    public long     MinStartNano { get; init; }
    public long     MaxStartNano { get; init; }
    public int      SpanCount    { get; init; }
    /// <summary>Service names present in this segment — enables O(1) cold-tier pre-filter.</summary>
    public string[] Services     { get; init; } = [];
}
