using System.Diagnostics;
using System.Runtime;
using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Server;

/// <summary>
/// Maps diagnostics (vital signs) endpoints:
///   GET /api/diagnostics — current server health snapshot
/// </summary>
public static class DiagnosticsEndpointMapper
{
    /// <summary>
    /// How long one data-directory walk is reused. The dashboard polls every 10 s; on-disk
    /// sizes move on the flush cadence (minutes), so four or five polls per walk costs nothing
    /// an operator can see and takes the walk off the per-request path.
    /// </summary>
    private static readonly DataDirectoryStatsCache DirCache = new(TimeSpan.FromSeconds(45));

    /// <summary>
    /// Process start time never changes, and reading it through a <see cref="Process"/>
    /// instance does not come free on Windows. Formatted once.
    /// </summary>
    private static readonly string StartedAtIso =
        Process.GetCurrentProcess().StartTime.ToUniversalTime().ToString("O");

    public static void MapDiagnosticsEndpoints(this WebApplication app)
    {
        app.MapGet("/api/diagnostics", (
            StorageEngine storage, ServerOptions options, ProcessCpuSampler cpu,
            Ameto.Ingestion.IngestionEndpoint ingest,
            Ameto.Indexing.IndexingWiring indexing, Ameto.Indexing.SegmentIndexCache indexCache,
            HttpContext http) =>
        {
            // Metrics and tracing can each be switched off (Ameto:Metrics:Enabled,
            // Ameto:Tracing:Enabled), and then nothing registers these: looked up rather than
            // injected, so a disabled signal reports nulls instead of failing the endpoint.
            var metrics = http.RequestServices.GetService<Ameto.Metrics.Storage.MetricStorageEngine>();
            var traces  = http.RequestServices.GetService<Ameto.Tracing.TraceDiagnostics>();

            // Disk space for the data directory drive
            long diskFreeBytes  = 0;
            long diskTotalBytes = 0;
            try
            {
                var drive = new DriveInfo(Path.GetFullPath(options.DataDirectory));
                diskFreeBytes  = drive.AvailableFreeSpace;
                diskTotalBytes = drive.TotalSize;
            }
            catch { /* ignore if drive info unavailable */ }

            var segs = storage.GetSegments(null, null);

            // Catalog totals in one pass. LINQ's Sum would walk the list twice and allocate an
            // iterator plus a delegate per call on an endpoint polled every 10 s.
            long totalEvents     = 0;
            long logsSegmentBytes = 0;
            for (int i = 0; i < segs.Count; i++)
            {
                var s = segs[i];
                totalEvents      += s.EventCount;
                logsSegmentBytes += s.CompressedBytes;
            }

            // ── Storage: on-disk size of the whole data directory, broken down by signal.
            // One cached walk (see DataDirectoryStatsCache); the segments directory is not
            // walked at all because CompressedBytes IS the .seg file length.
            var  dataRoot     = Path.GetFullPath(options.DataDirectory);
            var  dir          = DirCache.Get(dataRoot);
            // Quarantined segments are logs storage the catalog no longer lists; they stay on disk
            // until an operator removes them, so leaving them out would put the total under `du`.
            long logsBytes    = logsSegmentBytes + dir.WalBytes + dir.QuarantinedSegmentBytes;
            long metricsBytes = dir.MetricsBytes;
            long tracesBytes  = dir.TracesBytes;
            long dbBytes      = dir.DatabaseBytes;
            long otherBytes   = dir.OtherBytes;
            long dataTotal    = logsBytes + metricsBytes + tracesBytes + dbBytes + otherBytes;

            // Memory attribution: split process RSS into its real consumers so
            // we can stop guessing what holds the working set.
            //   gcHeap      — live managed objects on the GC heap
            //   gcCommitted — address space the GC has committed (≥ heap; the
            //                 part Server GC notoriously keeps reserved)
            //   hotTier     — native (off-heap) NativeMemory chunks
            // Whatever remains of workingSet after these is runtime + mapped
            // code pages (R2R images, ICU, etc.).
            var gcInfo          = GC.GetGCMemoryInfo();
            long hotTierBytes   = storage.HotTierAllocatedBytes;

            return Results.Ok(new
            {
                // Disk
                diskFreeBytes,
                diskTotalBytes,

                // System RAM
                systemRamPercent  = RamPressureService.GetSystemRamPercent(),
                ramTargetPercent  = options.RamTargetPercent,

                // Process. Deliberately not via a Process instance: on Windows each of those
                // reads snapshots EVERY process on the machine (NtQuerySystemInformation), and
                // proc.Threads.Count did so to build a ProcessThread object per thread — the
                // single largest allocation in this endpoint. processThreads now reports the
                // thread-pool thread count, which is the number that actually moves with
                // contention and slow I/O; dedicated threads (drainer, flushers, GC) are not
                // in it, so the figure is smaller than it used to be for the same load.
                processWorkingSetBytes = ProcessMemoryInfo.WorkingSetBytes,
                processPrivateBytes    = ProcessMemoryInfo.PrivateBytes,
                processThreads         = ThreadPool.ThreadCount,
                processStartedAt       = StartedAtIso,

                // CPU. The percentage is the last closed 30-second interval, sampled by
                // RamPressureService — this endpoint deliberately does not sample, because a
                // second reader would shorten that interval and make both figures jitter.
                // -1 means no interval has closed yet (first ~30 s after start).
                // processCpuSeconds is the monotonic total, for callers that would rather
                // difference it across their own polls.
                processCpuPercent      = cpu.LastPercent < 0 ? -1 : Math.Round(cpu.LastPercent, 1),
                processCpuSeconds      = Math.Round(ProcessCpuSampler.TotalProcessorTime.TotalSeconds, 1),
                processorCount         = cpu.Cores,

                // Memory breakdown
                gcMode                 = GCSettings.IsServerGC ? "Server" : "Workstation",
                gcLatencyMode          = GCSettings.LatencyMode.ToString(),
                gcHeapBytes            = gcInfo.HeapSizeBytes,
                gcCommittedBytes       = gcInfo.TotalCommittedBytes,
                gcFragmentedBytes      = gcInfo.FragmentedBytes,
                managedTotalAllocated  = GC.GetTotalAllocatedBytes(),
                gen0Collections        = GC.CollectionCount(0),
                gen1Collections        = GC.CollectionCount(1),
                gen2Collections        = GC.CollectionCount(2),
                hotTierNativeBytes     = hotTierBytes,

                // Decoded segment indexes held across queries: managed postings plus native
                // bloom bits, and the part of RSS that used to ratchet up after the first wide
                // query and never come back down. Retained bytes are what the cache charges
                // against its budget, not what the .seg sections weigh on disk.
                indexCacheEntries      = indexCache.EntryCount,
                indexCacheBytes        = indexCache.TotalBytes,
                // What the cache enforces, not a fresh derivation: recomputing it per poll cost a
                // GC.GetGCMemoryInfo and could disagree with the cache after GC.RefreshMemoryLimit.
                indexCacheBudgetBytes  = indexCache.BudgetBytes,
                indexCacheHits         = indexCache.HitCount,
                indexCacheMisses       = indexCache.MissCount,
                indexCacheIdleEvicted  = indexCache.IdleEvictedCount,
                // The native share and its own ceiling, reported apart from the total because
                // these bytes are NativeMemory: no collection reclaims them, they do not count
                // against the GC's hard limit that indexCacheBudgetBytes is a share of, and on a
                // small host they are the part of this cache that can push the process past its
                // container limit. indexCacheShedEvicted counts entries dropped under RAM
                // pressure — a non-zero value means the server has been giving this cache back.
                indexCacheNativeBytes       = indexCache.NativeBytes,
                indexCacheNativeBudgetBytes = indexCache.NativeBudgetBytes,
                indexCacheShedEvicted       = indexCache.ShedEvictedCount,
                // Evictions the NATIVE ceiling caused while the total budget still had room. A
                // cache capped this way looks healthy in every other figure — it simply sits
                // below its budget and misses — so without this there is nothing to read.
                indexCacheNativeEvicted     = indexCache.NativeEvictedCount,
                // Entries replaced because their path now held different bytes — a segment file
                // replaced under its own name (a re-imported replica). Rare by construction; a
                // count that climbs is worth finding the cause of.
                indexCacheStaleReplaced     = indexCache.StaleReplacedCount,

                // Storage
                segmentCount         = segs.Count,
                totalEventCount      = totalEvents,
                totalCompressedBytes = logsSegmentBytes,

                // On-disk data directory (whole folder, per-signal breakdown)
                dataDirectory        = dataRoot,
                dataTotalBytes       = dataTotal,
                logsStorageBytes     = logsBytes,
                metricsStorageBytes  = metricsBytes,
                tracesStorageBytes   = tracesBytes,
                databaseStorageBytes = dbBytes,
                otherStorageBytes    = otherBytes,
                // Already inside logsStorageBytes; broken out because it is the one part of it
                // that retention will never free — an operator has to inspect or remove it.
                logsQuarantinedBytes = dir.QuarantinedSegmentBytes,

                // Segment counts per signal. Logs come from the engine rather than the
                // directory walk so this figure and the one on the Stats page cannot
                // disagree; metrics/traces are counted by extension in the same walk
                // that already measured their size.
                logsSegmentCount     = segs.Count,
                metricsSegmentCount  = dir.MetricsSegments,
                tracesSegmentCount   = dir.TracesSegments,

                // ── Stores: whether each one's reads are whole (#94) ───────────
                // "loading" in the first seconds after a start, "available" after it; "degraded"
                // when the startup scan left data on disk unread — the store answers from what it
                // has, and alert rules over it are not evaluated, until a restart; "closed" once it
                // has shut down. The HTTP APIs answer a degraded store normally (a part is not an
                // empty), so this is where the state shows. Null when the signal is disabled.
                logsAvailability     = AvailabilityName(storage.Availability),
                metricsAvailability  = metrics is null ? null : AvailabilityName(metrics.Availability),
                tracesAvailability   = traces  is null ? null : AvailabilityName(traces.Availability),

                // ── Ingest (logs) ──────────────────────────────────────────────
                // A request writes its events into the store itself and is answered once they are
                // in a WAL; there is no ring between them any more. What can still go wrong is
                // counted by cause, because each is a different problem: a payload over the
                // per-event limit is a misconfigured client; a batch that found no room within
                // Ingestion.BackPressureWait is a flush that is behind with nowhere left to spill;
                // a WAL append that failed is a volume refusing writes.
                ingestAcceptedTotal      = ingest.AcceptedTotal,
                ingestDroppedOversized   = ingest.DroppedOversized,
                // Events given up on for lack of room (or at shutdown). A request that wrote none
                // of its batch was answered 503 and its client retries; one that wrote part of it
                // reported the rest as dropped.
                ingestNotWritten         = storage.IngestNotWritten,
                // Batches that had to wait for room at all — a full tier between swaps, or a spill
                // file rotating. Climbing steadily with ingestNotWritten at zero is the flush
                // keeping up only just.
                ingestRoomWaits          = storage.IngestRoomWaits,
                ingestRefusedTooLarge    = storage.IngestRefusedTooLarge,
                ingestWalAppendFailures  = storage.IngestWalAppendFailures,
                // The spill (HotTier.SpillEnabled): events taken while every flush slot was busy,
                // into WAL files of their own, written into segments as slots come free. Pending
                // files are that backlog on disk; spilled events are not searchable until then.
                logsSpilling             = storage.IsSpilling,
                logsSpillFilesPending    = storage.SpillFilesPending,
                logsSpillFilesOpened     = storage.SpillFilesOpened,
                logsSpilledEvents        = storage.SpilledEvents,
                logsDespilledEvents      = storage.DespilledEvents,
                // Request bodies parked between requests by IngestBufferPool: CLEF, OTLP/HTTP,
                // OTLP/gRPC and the gzip inflate target all read into it, so on a busy server
                // this is the largest managed thing the ingest path holds. Bounded by
                // MemoryBudgets.IngestBufferBytes and emptied by a pressure trim; until it was
                // reported here, no figure attributed those megabytes to anything.
                ingestBufferPooledBytes  = IngestBufferPool.PooledBytes,
                ingestBufferBudgetBytes  = IngestBufferPool.MaxPooledTotalBytes,

                // ── Index build ────────────────────────────────────────────────
                // Merge rows whose exception column is not a readable exception map: written,
                // but without their @x.* terms, so @x.type filters and free-text search over
                // merged segments miss them. Non-zero means a producer writes maps the index
                // cannot read; it was previously counted nowhere an operator could see.
                indexMalformedExceptionPayloads = indexing.Hints.MalformedExceptionPayloads,
                // Bytes parked in the index build pools — transient after a gen2 trim, but
                // counted in no memory budget.
                indexBuildPooledBytes           = indexing.IndexBuildPooledBytes,

                // ── Metrics: the EFFECTIVE budgets ─────────────────────────────
                // What the engine enforces after the explicit-value, floor and budget rules — the
                // figures config.yml tells an operator tuning HotTierBytes or ExemplarsPerMetric on
                // a small host to read here. Null when metrics are disabled.
                metricsHotTierBudgetBytes       = metrics?.HotTierBudgetBytes,
                metricsMinFlushBytes            = metrics?.MinFlushBytes,
                metricsWalInitialBytes          = metrics?.WalInitialBytes,
                metricsExemplarsPerMetric       = metrics?.ExemplarsPerMetric,
                metricsMaxExemplarMetrics       = metrics?.MaxExemplarMetrics,
                // What one chunk of a metric rewrite may hold (#125) — by default the trace merge
                // pass's share, taken in turn with it.
                metricsRewriteBudgetBytes       = metrics?.RewriteBudgetBytes,
                // Exemplars dropped because MaxExemplarMetrics names already own a ring. A hint,
                // never data — this counter is the only place the refusal shows.
                metricsExemplarMetricsRefused   = metrics?.ExemplarMetricsRefused,
                // The metric label intern pool (#88): its fill, its cap, how many of its epochs
                // filled up and how many times it was reset. A full pool drops nothing — each new
                // label then costs its own string until the next reset (at most one an hour) — so
                // resets climbing hour after hour say the LIVE label set outgrows the pool.
                metricsLabelPoolStrings         = metrics is null ? (int?)null : Ameto.Metrics.MetricLabelInterner.Shared.Strings.ClaimedCount,
                metricsLabelPoolMaxStrings      = metrics is null ? (int?)null : Ameto.Metrics.MetricLabelInterner.Shared.Strings.MaxPoolSize,
                metricsLabelPoolSaturations     = metrics is null ? (int?)null : Ameto.Metrics.MetricLabelInterner.Shared.Saturations,
                metricsLabelPoolResets          = metrics is null ? (int?)null : Ameto.Metrics.MetricLabelInterner.Shared.Resets,

                // ── Traces: the EFFECTIVE budgets, and the ring's back-pressure ─
                // Refusals by cause, because they are different problems: RefusedForBytes is a
                // burst heavier than RingMaxBytes, RefusedNoSlot a drainer that fell behind, and
                // RefusedNoArena a payload mix the 64 KiB chunks pack badly (32–64 KB spans take a
                // chunk each). Null when tracing is disabled.
                tracesHotTierBudgetBytes        = traces?.HotTierBudgetBytes,
                tracesMergeBudgetBytes          = traces?.MergeBudgetBytes,
                tracesRingCapacity              = traces?.RingCapacity,
                tracesRingMaxBytes              = traces?.RingMaxBytes,
                tracesRingBytesInFlight         = traces?.RingBytesInFlight,
                tracesRingRefusedForBytes       = traces?.RingRefusedForBytes,
                tracesRingRefusedNoSlot         = traces?.RingRefusedNoSlot,
                tracesRingRefusedNoArena        = traces?.RingRefusedNoArena,
                // A full intern pool drops nothing — each span then keeps its own string — so
                // these are memory, not loss: a service.name per pod, or span names carrying ids.
                tracesUnpooledSpanNames         = traces?.UnpooledSpanNames,
                tracesUnpooledServiceNames      = traces?.UnpooledServiceNames,
                tracesInternPoolSaturations     = traces?.InternPoolSaturations,
            });
        }).RequireAuthorization();
    }

    /// <summary>A store's state as the API spells it — a constant per state, so a poll formats nothing.</summary>
    private static string AvailabilityName(QueryAvailability availability) => availability switch
    {
        QueryAvailability.Available => "available",
        QueryAvailability.Loading   => "loading",
        QueryAvailability.Degraded  => "degraded",
        QueryAvailability.Closed    => "closed",
        _                           => availability.ToString().ToLowerInvariant(),
    };
}
