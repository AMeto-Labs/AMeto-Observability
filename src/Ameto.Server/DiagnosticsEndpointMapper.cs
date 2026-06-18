using System.Diagnostics;
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
                processThreads         = proc.Threads.Count,
                processStartedAt       = proc.StartTime.ToUniversalTime().ToString("O"),

                // Storage
                segmentCount         = segs.Count,
                totalEventCount      = segs.Sum(s => (long)s.EventCount),
                totalCompressedBytes = segs.Sum(s => s.CompressedBytes),
            });
        }).RequireAuthorization();
    }
}
