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
    public static void MapDiagnosticsEndpoints(this WebApplication app)
    {
        app.MapGet("/api/diagnostics", (
            StorageEngine storage, ServerOptions options, ProcessCpuSampler cpu,
            Ameto.Ingestion.IngestionRingBuffer ring, Ameto.Ingestion.IngestionDrainer drainer) =>
        {
            var proc = Process.GetCurrentProcess();

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

            // ── Storage: on-disk size of the whole data directory, broken down by
            // signal. Cheap directory walks (metadata only, no file reads).
            var dataRoot     = Path.GetFullPath(options.DataDirectory);
            var logsDir       = DirStats(Path.Combine(dataRoot, "segments"), ".seg");
            var metricsDir    = DirStats(Path.Combine(dataRoot, "metrics"),  ".mts");
            var tracesDir     = DirStats(Path.Combine(dataRoot, "traces"),   ".trc");
            long logsBytes    = logsDir.Bytes + DirStats(Path.Combine(dataRoot, "wal"), null).Bytes;
            long metricsBytes = metricsDir.Bytes;
            long tracesBytes  = tracesDir.Bytes;
            long dbBytes      = FilesSize(dataRoot, "Ameto.db*");
            long dataTotal    = DirStats(dataRoot, null).Bytes;
            long otherBytes   = Math.Max(0, dataTotal - logsBytes - metricsBytes - tracesBytes - dbBytes);

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

                // Process
                processWorkingSetBytes = proc.WorkingSet64,
                processPrivateBytes    = proc.PrivateMemorySize64,
                processThreads         = proc.Threads.Count,
                processStartedAt       = proc.StartTime.ToUniversalTime().ToString("O"),

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

                // Storage
                segmentCount         = segs.Count,
                totalEventCount      = segs.Sum(s => (long)s.EventCount),
                totalCompressedBytes = segs.Sum(s => s.CompressedBytes),

                // On-disk data directory (whole folder, per-signal breakdown)
                dataDirectory        = dataRoot,
                dataTotalBytes       = dataTotal,
                logsStorageBytes     = logsBytes,
                metricsStorageBytes  = metricsBytes,
                tracesStorageBytes   = tracesBytes,
                databaseStorageBytes = dbBytes,
                otherStorageBytes    = otherBytes,

                // Segment counts per signal. Logs come from the engine rather than the
                // directory walk so this figure and the one on the Stats page cannot
                // disagree; metrics/traces are counted by extension in the same walk
                // that already measured their size.
                logsSegmentCount     = segs.Count,
                metricsSegmentCount  = metricsDir.Segments,
                tracesSegmentCount   = tracesDir.Segments,

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
                // Events the storage write path refused repeatedly and the drainer gave up
                // on — a different failure from a full buffer, and previously silent.
                ingestWriteErrorDrops    = drainer.ErrorDrops,
            });
        }).RequireAuthorization();
    }

    /// <summary>
    /// Recursive on-disk size of a directory (0 if it doesn't exist), plus how many of its
    /// files carry <paramref name="segmentExt"/> — pass <c>null</c> to skip counting.
    /// Metadata-only, and one walk for both figures.
    /// </summary>
    /// <remarks>
    /// <see cref="DirectoryInfo.EnumerateFiles(string, SearchOption)"/> yields
    /// <see cref="FileInfo"/> instances whose length is already populated from the OS
    /// enumeration record. The <c>Directory.EnumerateFiles</c> + <c>new FileInfo(path)</c>
    /// shape this replaces paid a fresh stat syscall per file — on a data directory with
    /// thousands of segments that dominated the endpoint's cost.
    /// </remarks>
    private static (long Bytes, int Segments) DirStats(string path, string? segmentExt)
    {
        try
        {
            if (!Directory.Exists(path)) return (0, 0);
            long total    = 0;
            int  segments = 0;
            foreach (var f in new DirectoryInfo(path).EnumerateFiles("*", SearchOption.AllDirectories))
            {
                try
                {
                    total += f.Length;
                    if (segmentExt is not null &&
                        f.Extension.Equals(segmentExt, StringComparison.OrdinalIgnoreCase)) segments++;
                }
                catch { /* transient/locked — skip */ }
            }
            return (total, segments);
        }
        catch { return (0, 0); }
    }

    /// <summary>Sum of files matching a pattern in the top level of a directory.</summary>
    private static long FilesSize(string dir, string pattern)
    {
        try
        {
            if (!Directory.Exists(dir)) return 0;
            long total = 0;
            foreach (var f in Directory.EnumerateFiles(dir, pattern, SearchOption.TopDirectoryOnly))
            {
                try { total += new FileInfo(f).Length; } catch { /* skip */ }
            }
            return total;
        }
        catch { return 0; }
    }
}
