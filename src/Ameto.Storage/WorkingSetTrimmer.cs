using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace Ameto.Storage;

/// <summary>
/// Best-effort helper that asks the operating system to trim the process
/// working set after a hot-tier flush.
///
/// On Windows this calls <c>SetProcessWorkingSetSizeEx(-1, -1)</c>, the
/// documented way to request that the OS release as many resident pages as
/// possible without affecting the process's virtual address space. On Linux
/// this calls glibc <c>malloc_trim(0)</c>: free()'d hot-tier chunks leave the
/// allocator holding the freed arenas, so without an explicit trim the RSS
/// stays inflated long after a flush.
/// </summary>
internal static class WorkingSetTrimmer
{
    /// <summary>
    /// Full release of resident memory back to the OS — used only after a
    /// pressure-triggered flush + GC. On Windows this empties the working set
    /// (<c>SetProcessWorkingSetSizeEx(-1,-1)</c>); on Linux it also returns freed
    /// allocator arenas (<c>malloc_trim</c>).
    /// </summary>
    public static void TryTrim()
    {
        try
        {
            if (OperatingSystem.IsWindows())    TrimWindows();
            else if (OperatingSystem.IsLinux()) TrimLinux();
        }
        catch
        {
            // best-effort — never break the flush pipeline on a failed trim
        }
    }

    /// <summary>
    /// Cheap per-cycle upkeep: returns free()'d allocator arenas to the OS.
    /// Linux/glibc retains freed hot-tier chunks, so the RSS drifts upward
    /// without a regular <c>malloc_trim</c>. <b>No-op on Windows</b> — emptying the
    /// working set every cycle just evicts pages that get soft-faulted straight
    /// back in (visible as a sawtooth in Task Manager) for no real benefit; the
    /// Windows working set is only trimmed under real RAM pressure via
    /// <see cref="TryTrim"/>.
    /// </summary>
    public static void TrimAllocator()
    {
        try
        {
            if (OperatingSystem.IsLinux()) TrimLinux();
        }
        catch
        {
            // best-effort
        }
    }

    [SupportedOSPlatform("windows")]
    private static void TrimWindows()
    {
        var handle = Process.GetCurrentProcess().Handle;
        SetProcessWorkingSetSizeEx(handle, (IntPtr)(-1), (IntPtr)(-1), 0);
    }

    [SupportedOSPlatform("linux")]
    private static void TrimLinux() => malloc_trim(0);

    [DllImport("kernel32.dll", SetLastError = true)]
    [SupportedOSPlatform("windows")]
    private static extern bool SetProcessWorkingSetSizeEx(
        IntPtr hProcess,
        IntPtr dwMinimumWorkingSetSize,
        IntPtr dwMaximumWorkingSetSize,
        uint   Flags);

    // glibc: returns freed heap memory at the top of the arena to the OS.
    [DllImport("libc", SetLastError = false)]
    [SupportedOSPlatform("linux")]
    private static extern int malloc_trim(nuint pad);
}
