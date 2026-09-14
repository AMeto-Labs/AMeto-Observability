using System.Buffers;

namespace Ameto.Indexing;

/// <summary>
/// A <see cref="MemoryManager{T}"/> over a raw pointer, re-pointed per event, so a
/// <c>MessagePackReader</c> (which wants a <see cref="ReadOnlySequence{T}"/>) can read a payload
/// where it sits — the hot tier's native arena, or a decompressed block on the merge path —
/// instead of after a copy into a scratch array. That copy was ~60 MB of memcpy per flush and
/// one of the six passes every property value's bytes made.
///
/// <para>The owner sets the pointer inside a <c>fixed</c> block over the payload span and
/// clears it before leaving, so the memory is pinned for exactly the walk; nothing keeps the
/// <see cref="Memory{T}"/> beyond it.</para>
/// </summary>
internal sealed unsafe class PinnedSpanMemoryManager : MemoryManager<byte>
{
    private byte* _ptr;
    private int   _len;

    public void Set(byte* ptr, int len) { _ptr = ptr; _len = len; }

    public override Memory<byte> Memory => CreateMemory(_len);

    public override Span<byte> GetSpan() => new(_ptr, _len);

    public override MemoryHandle Pin(int elementIndex = 0) => new(_ptr + elementIndex);

    public override void Unpin() { }

    protected override void Dispose(bool disposing) { }
}
