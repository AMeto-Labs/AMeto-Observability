using System.Buffers;

namespace Ameto.Indexing;

/// <summary>
/// <see cref="IBufferWriter{T}"/> over a caller-owned array of exactly the right size. The
/// section serialisers know their output length before they write it, so the managed-blob path
/// (tests, probes, the legacy <c>Serialise()</c> accessors) allocates once, of that size, and
/// never copies — where an <c>ArrayBufferWriter</c> sized by estimate plus <c>ToArray()</c>
/// made two LOH allocations per section per group.
/// </summary>
internal sealed class FixedBufferWriter(byte[] buffer) : IBufferWriter<byte>
{
    private int _written;

    public int Written => _written;

    public void Advance(int count) => _written += count;

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        if (_written + Math.Max(sizeHint, 1) > buffer.Length) ThrowFull();
        return buffer.AsMemory(_written);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        if (_written + Math.Max(sizeHint, 1) > buffer.Length) ThrowFull();
        return buffer.AsSpan(_written);
    }

    private static void ThrowFull() =>
        throw new InvalidOperationException("Section serialiser wrote past its own computed size");
}
