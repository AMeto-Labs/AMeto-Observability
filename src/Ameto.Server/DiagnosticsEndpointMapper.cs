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
        app.MapGet("/api/diagnostics", (StorageEngine storage, ServerOptions options) =>
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
            long logsBytes    = DirSize(Path.Combine(dataRoot, "segments")) + DirSize(Path.Combine(dataRoot, "wal"));
            long metricsBytes = DirSize(Path.Combine(dataRoot, "metrics"));
            long tracesBytes  = DirSize(Path.Combine(dataRoot, "traces"));
            long dbBytes      = FilesSize(dataRoot, "Ameto.db*");
            long dataTotal    = DirSize(dataRoot);
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
            });
        }).RequireAuthorization();
    }

    /// <summary>Recursive on-disk size of a directory (0 if it doesn't exist). Metadata-only.</summary>
    private static long DirSize(string path)
    {
        try
        {
            if (!Directory.Exists(path)) return 0;
            long total = 0;
            foreach (var f in Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories))
            {
                try { total += new FileInfo(f).Length; } catch { /* transient/locked — skip */ }
            }
            return total;
        }
        catch { return 0; }
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
