using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Ameto.Ingestion;

/// <summary>
/// The ingest payload arena: one contiguous address range whose pages are paid for only as
/// the buffer actually grows into them.
///
/// <para>Why this exists. The arena is sized to the back-pressure ceiling — 512 MB by default —
/// on the assumption stated in <see cref="IngestionRingBuffer"/> that it is "reserved virtual
/// memory; only pages actually written become resident". That is true of
/// <c>NativeMemory.Alloc</c> on Linux, where a large malloc is an anonymous mmap and pages fault
/// in lazily. It is NOT true on Windows: a block that large goes straight to
/// <c>VirtualAlloc(MEM_COMMIT)</c>, so the process takes a 512 MB commit charge the moment the
/// server starts, before a single event has arrived. On a machine sized for the workload rather
/// than for the ceiling, that is the difference between starting and not.</para>
///
/// <para>So on Windows the range is RESERVED (address space only, no commit charge) and slabs
/// are committed as the high-water mark advances. The free list hands slabs out in increasing
/// index order and reuses them LIFO, so the high-water mark is exactly the deepest the buffer
/// has ever been — a server whose drainer keeps up commits a few slabs and no more. Everywhere
/// else this is a plain allocation, because there the pages were already lazy.</para>
///
/// <para>Lazy per 4 KB page on Linux only while transparent huge pages leave the range alone.
/// With <c>transparent_hugepage/enabled=always</c> (the RHEL default) the first write in a
/// 2 MB-aligned range can fault in a whole 2 MB page, and khugepaged collapses sparsely touched
/// ranges too. A 64 KB slab puts 32 slabs in each 2 MB, so one burst of ~8 192 small events,
/// touching every range, could make most of a 512 MB arena resident instead of ~32 MB. So on
/// Linux the arena is advised <c>MADV_NOHUGEPAGE</c> once, at creation (see
/// <see cref="HugePagesDisabled"/>).</para>
///
/// <para>What is deliberately NOT done: committed slabs are never given back. Decommitting
/// above the high-water mark would need a background sweep — a timer wake on an idle server,
/// which is the thing this work package is removing — to reclaim memory that a LIFO free list
/// will ask for again on the next burst of the same size. The high-water mark IS the residency,
/// and it is the true peak, not the ceiling.</para>
/// </summary>
internal sealed unsafe class SlabArena : IDisposable
{
    private readonly byte* _base;
    private readonly nuint _bytes;
    private readonly nuint _commitChunk;
    private readonly bool  _reserved;       // true => committed on demand, false => already committed
    private readonly Lock  _growGate = new();

    private readonly int   _hugePageOptOut; // NoHugePageOptOut, 0 when madvise succeeded, else its errno

    private nuint _committed;               // bytes committed from _base; only grows
    private bool  _disposed;

    /// <summary><see cref="HugePageOptOutErrno"/> when the arena was never advised: not Linux, or reserved.</summary>
    internal const int NoHugePageOptOut = -1;

    /// <summary><see cref="HugePageOptOutErrno"/> when libc or its <c>madvise</c> export could not be bound.</summary>
    internal const int HugePageOptOutUnbound = -2;

    private SlabArena(byte* @base, nuint bytes, nuint commitChunk, bool reserved, nuint committed, int hugePageOptOut)
    {
        _base           = @base;
        _bytes          = bytes;
        _commitChunk    = commitChunk;
        _reserved       = reserved;
        _committed      = committed;
        _hugePageOptOut = hugePageOptOut;
    }

    public byte* Base => _base;

    /// <summary>
    /// True when the arena is advised <c>MADV_NOHUGEPAGE</c>, so its residency stays per touched
    /// 4 KB page whatever <c>transparent_hugepage/enabled</c> says. Only ever true on Linux, and
    /// only for the plain allocation there. False on Linux means the advice failed (see
    /// <see cref="HugePageOptOutErrno"/>) and the arena behaves as it did before: nothing breaks,
    /// but with THP set to <c>always</c> a burst can make whole 2 MB ranges resident.
    /// </summary>
    public bool HugePagesDisabled => _hugePageOptOut == 0;

    /// <summary>
    /// 0 when <c>madvise(MADV_NOHUGEPAGE)</c> succeeded; its errno when it failed;
    /// <see cref="HugePageOptOutUnbound"/> when it could not be called; <see cref="NoHugePageOptOut"/>
    /// when it was not attempted (not Linux, or a reserved arena).
    /// </summary>
    public int HugePageOptOutErrno => _hugePageOptOut;

    /// <summary>
    /// Bytes committed on demand so far — the ingest high-water mark — or -1 when the arena is a
    /// plain allocation (everything but Windows). There the pages are lazy and nothing is
    /// counted, so the arena size would be an upper bound reported as memory in use: every Linux
    /// container would show 512 MB of arena at idle.
    /// </summary>
    public long CommittedBytes
    {
        get
        {
            if (!_reserved) return -1;
            lock (_growGate) return (long)_committed;
        }
    }

    /// <summary>True when pages are committed on demand rather than up front.</summary>
    public bool IsCommitOnDemand => _reserved;

    /// <summary>
    /// Reserves <paramref name="bytes"/>. Falls back to a plain allocation if the reservation
    /// fails for any reason — a working server matters more than a tidy commit charge.
    /// </summary>
    public static SlabArena Create(nuint bytes, nuint commitChunk) => Create(bytes, commitChunk, reserve: true);

    /// <summary>
    /// <paramref name="reserve"/> false takes the plain-allocation path on every platform — the
    /// one Linux always takes — so a test on Windows can reach it.
    /// </summary>
    internal static SlabArena Create(nuint bytes, nuint commitChunk, bool reserve)
    {
        if (reserve && OperatingSystem.IsWindows())
        {
            nint p = VirtualAlloc(0, bytes, MEM_RESERVE, PAGE_READWRITE);
            if (p != 0)
                return new SlabArena((byte*)p, bytes, Math.Max(commitChunk, (nuint)4096), reserved: true, committed: 0,
                                     hugePageOptOut: NoHugePageOptOut);
        }

        // Linux/macOS: a large NativeMemory.Alloc is already an anonymous mapping whose pages
        // fault in on first touch. On Linux it is also opted out of transparent huge pages, one
        // syscall at creation, so that stays true per 4 KB page (see the class remarks).
        byte* plain = (byte*)NativeMemory.Alloc(bytes);
        int optOut = OperatingSystem.IsLinux() ? DisableHugePages((nuint)plain, bytes) : NoHugePageOptOut;
        return new SlabArena(plain, bytes, bytes, reserved: false, committed: bytes, hugePageOptOut: optOut);
    }

    /// <summary>
    /// <c>madvise(MADV_NOHUGEPAGE)</c> over the whole pages of the allocation. Returns 0, the
    /// errno, or <see cref="HugePageOptOutUnbound"/>; never throws, because a server that cannot
    /// advise its arena still works.
    /// </summary>
    private static int DisableHugePages(nuint address, nuint bytes)
    {
        var (start, length) = PageAlignInward(address, bytes, (nuint)Environment.SystemPageSize);
        if (length == 0) return 0;   // not one whole page: nothing a huge page could back

        try
        {
            return madvise((nint)start, length, MADV_NOHUGEPAGE) == 0 ? 0 : Marshal.GetLastPInvokeError();
        }
        catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException)
        {
            return HugePageOptOutUnbound;
        }
    }

    /// <summary>
    /// The whole pages inside [<paramref name="address"/>, <paramref name="address"/> +
    /// <paramref name="bytes"/>): the start rounded UP and the end rounded DOWN to
    /// <paramref name="pageSize"/>, or length 0 when no whole page fits. <c>madvise</c> takes only
    /// a page-aligned start, and <c>NativeMemory.Alloc</c> promises none (glibc returns an mmap'd
    /// chunk 16 bytes past its page boundary). Rounding inward never advises memory outside the
    /// allocation, and what it leaves out is under one page at either end.
    /// </summary>
    internal static (nuint Start, nuint Length) PageAlignInward(nuint address, nuint bytes, nuint pageSize)
    {
        if (pageSize == 0) return (0, 0);
        nuint end = address + bytes;
        if (end < address) return (0, 0);   // the range wraps the address space: not an allocation

        nuint headGap = (pageSize - address % pageSize) % pageSize;
        if (headGap >= bytes) return (0, 0);

        nuint start      = address + headGap;   // <= end, so it cannot wrap
        nuint alignedEnd = end - end % pageSize;
        return alignedEnd > start ? (start, alignedEnd - start) : (0, 0);
    }

    /// <summary>
    /// Makes the first <paramref name="endOffset"/> bytes writable, or reports that it could
    /// not. On the hot path this is one volatile read and a predicted-true branch; it only
    /// does work when the buffer reaches deeper than it ever has. (A plain allocation starts
    /// with <c>_committed</c> at the whole arena, so it never leaves the fast path.)
    ///
    /// <para>False means the operating system refused the commit — on Windows, the machine's
    /// commit charge is exhausted. That is a transient, host-wide condition, and the caller is
    /// on the ingest path: it must turn this into a counted, refused enqueue, the same
    /// back-pressure an exhausted arena produces. It used to throw OutOfMemoryException out of
    /// TryEnqueue into the HTTP handler, after the slab had already been taken off the free
    /// list, so every failure both failed the request and shrank the arena for good.</para>
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnsureCommitted(nuint endOffset)
    {
        if (endOffset <= Volatile.Read(ref _committed)) return true;
        return TryGrow(endOffset);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private bool TryGrow(nuint endOffset)
    {
        lock (_growGate)
        {
            if (endOffset <= _committed) return true;      // someone else grew past us
            if (_simulateCommitFailure)
            {
                _onSimulatedFailure?.Invoke();
                return false;
            }

            // Round up to a commit chunk so a burst that walks the arena does not make one
            // syscall per slab, and never past the reservation.
            nuint target = endOffset + _commitChunk - 1;
            target -= target % _commitChunk;
            if (target > _bytes) target = _bytes;

            nuint len = target - _committed;
            if (len == 0) return true;

            // Out of commit charge: leave _committed where it is (a write past it would fault)
            // and let the caller refuse the event. The next slab that reaches this deep tries
            // again, so the arena recovers on its own once the pressure is over.
            if (!_reserved || VirtualAlloc((nint)(_base + _committed), len, MEM_COMMIT, PAGE_READWRITE) == 0)
                return false;

            Volatile.Write(ref _committed, target);
            return true;
        }
    }

    // ── Test hook ──────────────────────────────────────────────────────────────

    private bool    _simulateCommitFailure;   // read and written under _growGate only
    private Action? _onSimulatedFailure;      // likewise

    /// <summary>
    /// Test hook: every commit past the current high-water mark fails, as
    /// <c>VirtualAlloc(MEM_COMMIT)</c> does when the commit charge runs out. On a plain
    /// allocation — where nothing is committed on demand — the high-water mark is lowered to
    /// <paramref name="plainHighWaterMark"/> for the duration so the failure path is reachable on
    /// every platform; clearing the hook puts it back.
    /// </summary>
    /// <param name="plainHighWaterMark">
    /// The bytes a plain allocation pretends are committed while the hook is set, so a test can
    /// give it the same committed prefix a reserved arena reached. Ignored by a reserved arena,
    /// which keeps its real mark.
    /// </param>
    /// <param name="onFailure">
    /// Runs under the grow lock each time a simulated commit fails, so a test can hold a producer
    /// inside the failing commit while another thread works the free list.
    /// </param>
    internal void SimulateCommitFailure(bool fail, nuint plainHighWaterMark = 0, Action? onFailure = null)
    {
        lock (_growGate)
        {
            _simulateCommitFailure = fail;
            _onSimulatedFailure    = fail ? onFailure : null;
            if (!_reserved) Volatile.Write(ref _committed, fail ? Math.Min(plainHighWaterMark, _bytes) : _bytes);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        if (_reserved) VirtualFree((nint)_base, 0, MEM_RELEASE);
        else           NativeMemory.Free(_base);
    }

    // ── Windows ────────────────────────────────────────────────────────────────

    private const uint MEM_COMMIT     = 0x1000;
    private const uint MEM_RESERVE    = 0x2000;
    private const uint MEM_RELEASE    = 0x8000;
    private const uint PAGE_READWRITE = 0x04;

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern nint VirtualAlloc(nint address, nuint size, uint allocationType, uint protect);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool VirtualFree(nint address, nuint size, uint freeType);

    // ── Linux ──────────────────────────────────────────────────────────────────

    private const int MADV_NOHUGEPAGE = 15;

    [DllImport("libc", SetLastError = true)]
    private static extern int madvise(nint address, nuint length, int advice);
}
