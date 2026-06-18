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
/// possible without affecting the process's virtual address space. On other
/// platforms this is a no-op — Linux returns pages to the kernel
/// automatically via <c>madvise(MADV_DONTNEED)</c> inside the allocator and
/// no equivalent process-wide API exists.
/// </summary>
internal static class WorkingSetTrimmer
{
    public static void TryTrim()
    {
        try
        {
            if (OperatingSystem.IsWindows()) TrimWindows();
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

    [DllImport("kernel32.dll", SetLastError = true)]
    [SupportedOSPlatform("windows")]
    private static extern bool SetProcessWorkingSetSizeEx(
        IntPtr hProcess,
        IntPtr dwMinimumWorkingSetSize,
        IntPtr dwMaximumWorkingSetSize,
        uint   Flags);
}
