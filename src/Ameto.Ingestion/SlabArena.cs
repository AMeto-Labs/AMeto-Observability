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

    private nuint _committed;               // bytes committed from _base; only grows
    private bool  _disposed;

    private SlabArena(byte* @base, nuint bytes, nuint commitChunk, bool reserved, nuint committed)
    {
        _base        = @base;
        _bytes       = bytes;
        _commitChunk = commitChunk;
        _reserved    = reserved;
        _committed   = committed;
    }

    public byte* Base => _base;

    /// <summary>Bytes actually backed by memory. Equals the whole arena when not reserved.</summary>
    public long CommittedBytes { get { lock (_growGate) return (long)_committed; } }

    /// <summary>True when pages are committed on demand rather than up front.</summary>
    public bool IsCommitOnDemand => _reserved;

    /// <summary>
    /// Reserves <paramref name="bytes"/>. Falls back to a plain allocation if the reservation
    /// fails for any reason — a working server matters more than a tidy commit charge.
    /// </summary>
    public static SlabArena Create(nuint bytes, nuint commitChunk)
    {
        if (OperatingSystem.IsWindows())
        {
            nint p = VirtualAlloc(0, bytes, MEM_RESERVE, PAGE_READWRITE);
            if (p != 0)
                return new SlabArena((byte*)p, bytes, Math.Max(commitChunk, (nuint)4096), reserved: true, committed: 0);
        }

        // Linux/macOS: a large NativeMemory.Alloc is already an anonymous mapping whose pages
        // fault in on first touch, so there is nothing to improve and no syscalls to add.
        return new SlabArena((byte*)NativeMemory.Alloc(bytes), bytes, bytes, reserved: false, committed: bytes);
    }

    /// <summary>
    /// Guarantees the first <paramref name="endOffset"/> bytes are writable. On the hot path
    /// this is one volatile read and a predicted-false branch; it only does work when the
    /// buffer reaches deeper than it ever has.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void EnsureCommitted(nuint endOffset)
    {
        if (!_reserved) return;
        if (endOffset <= Volatile.Read(ref _committed)) return;
        Grow(endOffset);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Grow(nuint endOffset)
    {
        lock (_growGate)
        {
            if (endOffset <= _committed) return;           // someone else grew past us

            // Round up to a commit chunk so a burst that walks the arena does not make one
            // syscall per slab, and never past the reservation.
            nuint target = endOffset + _commitChunk - 1;
            target -= target % _commitChunk;
            if (target > _bytes) target = _bytes;

            nuint len = target - _committed;
            if (len == 0) return;

            if (VirtualAlloc((nint)(_base + _committed), len, MEM_COMMIT, PAGE_READWRITE) == 0)
            {
                // Out of commit charge. Leave _committed where it is: the caller's write would
                // fault, so tell it plainly rather than corrupting memory.
                throw new OutOfMemoryException(
                    $"Could not commit {len} bytes of the ingest payload arena (committed {_committed} of {_bytes}).");
            }

            Volatile.Write(ref _committed, target);
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
}
