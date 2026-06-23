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
