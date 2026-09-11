using System.Runtime.InteropServices;

namespace Ameto.Storage;

/// <summary>
/// Working-set and private-bytes readings for THIS process, without a
/// <see cref="System.Diagnostics.Process"/> instance.
///
/// <para>Why: on Windows every <c>Process.WorkingSet64</c> / <c>PrivateMemorySize64</c> read
/// refreshes the instance through <c>NtQuerySystemInformation(SystemProcessInformation)</c>,
/// which snapshots <em>every</em> process on the machine into a buffer that grows with the
/// process table — hundreds of KB of garbage for two numbers about ourselves. The dashboard
/// polls <c>/api/diagnostics</c> every 10 s, so that cost is permanent while a tab is open.
/// <c>K32GetProcessMemoryInfo</c> answers the same two numbers for one process with a single
/// syscall into a stack struct.</para>
/// </summary>
public static class ProcessMemoryInfo
{
    /// <summary>Resident/working-set bytes. Never throws; 0 when unavailable.</summary>
    public static long WorkingSetBytes => Environment.WorkingSet;

    /// <summary>
    /// Private (non-shared) committed bytes — the figure that exposes an over-committed
    /// native arena on Windows, where the working set alone hides it. Falls back to the
    /// working set when the platform cannot answer. Never throws.
    /// </summary>
    public static long PrivateBytes
    {
        get
        {
            if (OperatingSystem.IsWindows())
            {
                PROCESS_MEMORY_COUNTERS_EX c = default;
                c.cb = (uint)Marshal.SizeOf<PROCESS_MEMORY_COUNTERS_EX>();
                // Pseudo-handle for the current process: no handle to open, none to close.
                if (K32GetProcessMemoryInfo((nint)(-1), ref c, c.cb) != 0)
                    return (long)c.PrivateUsage;
                return Environment.WorkingSet;
            }

            long data = TryReadStatmDataBytes();
            return data > 0 ? data : Environment.WorkingSet;
        }
    }

    /// <summary>
    /// Linux: field 6 ("data") of <c>/proc/self/statm</c>, in pages — private data + stack,
    /// the closest analogue of Windows private bytes. Read into a stack buffer so the 10 s
    /// poll allocates nothing.
    /// </summary>
    private static long TryReadStatmDataBytes()
    {
        try
        {
            using var h = File.OpenHandle("/proc/self/statm");
            Span<byte> buf = stackalloc byte[128];
            int n = RandomAccess.Read(h, buf, 0);
            if (n <= 0) return 0;

            ReadOnlySpan<byte> s = buf[..n];
            // size resident shared text lib data dt — take the 6th (index 5).
            for (int field = 0; ; field++)
            {
                int sp = s.IndexOf((byte)' ');
                ReadOnlySpan<byte> tok = sp < 0 ? s : s[..sp];
                if (field == 5)
                    return long.TryParse(tok, out long pages) ? pages * Environment.SystemPageSize : 0;
                if (sp < 0) return 0;
                s = s[(sp + 1)..];
            }
        }
        catch { return 0; }
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern int K32GetProcessMemoryInfo(
        nint process, ref PROCESS_MEMORY_COUNTERS_EX counters, uint cb);

    [StructLayout(LayoutKind.Sequential)]
    private struct PROCESS_MEMORY_COUNTERS_EX
    {
        public uint  cb;
        public uint  PageFaultCount;
        public nuint PeakWorkingSetSize;
        public nuint WorkingSetSize;
        public nuint QuotaPeakPagedPoolUsage;
        public nuint QuotaPagedPoolUsage;
        public nuint QuotaPeakNonPagedPoolUsage;
        public nuint QuotaNonPagedPoolUsage;
        public nuint PagefileUsage;
        public nuint PeakPagefileUsage;
        public nuint PrivateUsage;
    }
}
