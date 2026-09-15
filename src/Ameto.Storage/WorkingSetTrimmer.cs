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
///
/// <para><c>malloc_trim</c> is a glibc extension. The container image is Alpine (musl) with
/// jemalloc preloaded and neither exports it, so the <c>DllImport</c> this used to make threw
/// <see cref="EntryPointNotFoundException"/> on every single call — a throw plus a stack
/// capture every 30 seconds, forever, on the platform the product actually ships on. The
/// export is now probed once through <see cref="MallocTrim"/> and the address cached; when it is
/// absent the per-cycle call compiles down to a null check. Nothing is lost by not calling it
/// there: jemalloc's background thread purges on its own decay timer.</para>
/// </summary>
internal static class WorkingSetTrimmer
{
    /// <summary>The process-wide <c>malloc_trim</c> binding, resolved on first use.</summary>
    internal static readonly MallocTrimBinding MallocTrim = new(MallocTrimBinding.ResolveFromLoadedLibc);

    /// <summary>Whether a usable <c>malloc_trim</c> was found. Probes on first use.</summary>
    internal static bool HasMallocTrim => MallocTrim.IsAvailable;

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
            else if (OperatingSystem.IsLinux()) MallocTrim.Trim();
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
            if (OperatingSystem.IsLinux()) MallocTrim.Trim();
        }
        catch
        {
            // best-effort
        }
    }

    [SupportedOSPlatform("windows")]
    private static void TrimWindows()
    {
        // Pseudo-handle (-1) for the current process: full access, nothing to open or close,
        // and no Process instance whose finalizable SafeHandle the GC then has to collect.
        SetProcessWorkingSetSizeEx((IntPtr)(-1), (IntPtr)(-1), (IntPtr)(-1), 0);
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    [SupportedOSPlatform("windows")]
    private static extern bool SetProcessWorkingSetSizeEx(
        IntPtr hProcess,
        IntPtr dwMinimumWorkingSetSize,
        IntPtr dwMaximumWorkingSetSize,
        uint   Flags);
}

/// <summary>
/// <c>int malloc_trim(size_t pad)</c>, looked up once and then called through a cached function
/// pointer — or not at all, when the lookup found nothing.
///
/// <para>The lookup is a constructor argument rather than a hard-wired call so the property that
/// matters — ONE lookup, no matter how many trims, and no exception per trim when the export is
/// absent — can be tested on every platform with a lookup that answers "absent", "present" or
/// throws. The real lookup only finds anything on Linux, and CI runs the backend suite on Windows,
/// where going back to a per-call <c>DllImport</c> would have stayed green.</para>
/// </summary>
internal sealed unsafe class MallocTrimBinding
{
    private readonly Func<nint> _resolve;
    private delegate* unmanaged<nuint, int> _fn;
    private bool _probed;

    /// <param name="resolve">Returns the export's address, or 0 when it is absent. May throw; a throw counts as absent.</param>
    internal MallocTrimBinding(Func<nint> resolve) => _resolve = resolve;

    /// <summary>Whether the export was found. Probes on first use.</summary>
    internal bool IsAvailable
    {
        get
        {
            if (!_probed) Probe();
            return _fn is not null;
        }
    }

    /// <summary><c>malloc_trim(0)</c> when the export exists; a null check when it does not.</summary>
    internal void Trim()
    {
        if (!_probed) Probe();
        var fn = _fn;
        if (fn is not null) fn(0);
    }

    /// <summary>
    /// One-shot lookup. Benign if two threads race: both do the same work and write the same
    /// address, and the pointer is only read after <c>_probed</c> is set.
    /// </summary>
    private void Probe()
    {
        nint addr;
        try   { addr = _resolve(); }
        catch { addr = 0; }                 // no libc, no export, no trim — the null below is the answer
        _fn     = (delegate* unmanaged<nuint, int>)addr;
        _probed = true;
    }

    /// <summary>
    /// The production lookup. The main program handle searches everything already loaded — glibc,
    /// and any LD_PRELOADed allocator — which is exactly the set a call would have bound to. 0
    /// anywhere but Linux, without looking.
    /// </summary>
    internal static nint ResolveFromLoadedLibc()
    {
        if (!OperatingSystem.IsLinux()) return 0;
        if (NativeLibrary.TryGetExport(NativeLibrary.GetMainProgramHandle(), "malloc_trim", out nint addr))
            return addr;
        return NativeLibrary.TryLoad("libc.so.6", out nint libc) && NativeLibrary.TryGetExport(libc, "malloc_trim", out addr)
            ? addr
            : 0;
    }
}
