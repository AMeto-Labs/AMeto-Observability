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
            Ameto.Ingestion.IngestionRingBuffer ring, Ameto.Ingestion.IngestionDrainer drainer,
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

                // ── Ingest ─────────────────────────────────────────────────────
                // Overload used to be invisible: a client that got a 200 with a "dropped"
                // count had no server-side counterpart, so nobody could see that the ring
                // was filling, let alone WHY. The three drop reasons are different
                // problems — a payload larger than one slab is a misconfigured client, an
                // exhausted arena is a burst the buffer could not absorb, a full ring is a
                // drainer that fell behind — and they are counted apart for that reason.
                ingestAcceptedTotal      = ring.AcceptedTotal,
                ingestDrainedTotal       = ring.DrainedTotal,
                ingestPending            = ring.ApproximateCount,
                ingestCapacity           = ring.Capacity,
                // The binding limit, and the one to watch: a pending event holds a payload
                // slab until the drainer copies it out, and the arena holds far fewer slabs
                // than the ring holds slots — so the slabs run out first, and pending
                // against the SLOT capacity made a saturated buffer look almost idle.
                ingestSlabCapacity       = ring.SlabCapacity,
                ingestSaturationPercent  = ring.SlabCapacity > 0
                                             ? Math.Round(100.0 * ring.ApproximateCount / ring.SlabCapacity, 1)
                                             : 0,
                ingestDroppedOversized   = ring.DroppedOversized,
                ingestDroppedNoSlab      = ring.DroppedNoSlab,
                ingestDroppedRingFull    = ring.DroppedRingFull,
                // A free slab whose pages the OS would not commit: the host is out of commit
                // charge. Not the buffer's limit, so not folded into NoSlab.
                ingestDroppedNoCommit    = ring.DroppedNoCommit,
                // Events the storage write path refused repeatedly and the drainer gave up
                // on — a different failure from a full buffer, and previously silent.
                ingestWriteErrorDrops    = drainer.ErrorDrops,
                // Request bodies parked between requests by IngestBufferPool: CLEF, OTLP/HTTP,
                // OTLP/gRPC and the gzip inflate target all read into it, so on a busy server
                // this is the largest managed thing the ingest path holds. Bounded by
                // MemoryBudgets.IngestBufferBytes and emptied by a pressure trim; until it was
                // reported here, no figure attributed those megabytes to anything.
                ingestBufferPooledBytes  = IngestBufferPool.PooledBytes,
                ingestBufferBudgetBytes  = IngestBufferPool.MaxPooledTotalBytes,
                // The payload arena: what it may reserve, and how far into it the buffer has
                // ever reached (deepest slab x slab size) -- never given back, and the largest
                // single thing the ingest path holds. On Windows the second figure is the
                // arena's commit charge; on Linux it is an upper bound on its resident pages,
                // since a small event touches only the first page of its slab. It used to be
                // reported nowhere: the ring's commit counter is Windows-only by design, so
                // on the Linux container this matters most for, no figure existed at all.
                ingestArenaBytes         = ring.ArenaCapacityBytes,
                ingestArenaResidentBytes = ring.ArenaHighWaterBytes,

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
                // Exemplars dropped because MaxExemplarMetrics names already own a ring. A hint,
                // never data — this counter is the only place the refusal shows.
                metricsExemplarMetricsRefused   = metrics?.ExemplarMetricsRefused,

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
}
