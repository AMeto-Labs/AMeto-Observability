using System.Buffers;

namespace Ameto.Indexing;

/// <summary>
/// <see cref="IBufferWriter{T}"/> that drains into a <see cref="Stream"/> through one pooled
/// buffer, so a section serialiser writes straight to the segment file instead of into a
/// managed blob that is copied and dropped.
///
/// <para>Per group the old path made a multi-MB <c>ArrayBufferWriter</c>, a second multi-MB
/// <c>ToArray()</c> copy of it, and a managed copy of the bloom's native bits — 70-110 MB of
/// immediately dead LOH per group on a Workstation GC that only compacts the LOH on the forced
/// maintenance collect. This holds one 1 MB rental for the whole group.</para>
///
/// <para>A <see cref="GetSpan"/> hint larger than the buffer is honoured by swapping in a
/// bigger rental (a single dense trigram bucket can ask for a few hundred KB), never by
/// returning a short span: <c>IBufferWriter</c> callers are entitled to at least the hint.</para>
/// </summary>
internal sealed class StreamSectionWriter : IBufferWriter<byte>, IDisposable
{
    private const int DefaultBuffer = 1 << 20;

    private readonly Stream _stream;
    private byte[] _buffer;
    private int    _used;
    private long   _total;

    public StreamSectionWriter(Stream stream, int bufferSize = DefaultBuffer)
    {
        _stream = stream;
        _buffer = IndexBuildPool.Slabs.Rent(bufferSize);
    }

    /// <summary>Bytes written since the last <see cref="ResetCount"/> (flushed or not).</summary>
    public long Total => _total;

    /// <summary>Starts a new section's count. Does not flush.</summary>
    public void ResetCount() => _total = 0;

    public void Advance(int count)
    {
        _used  += count;
        _total += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _buffer.AsMemory(_used);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _buffer.AsSpan(_used);
    }

    /// <summary>Writes what the buffer holds to the stream. The stream is not flushed.</summary>
    public void Flush()
    {
        if (_used == 0) return;
        _stream.Write(_buffer, 0, _used);
        _used = 0;
    }

    private void Ensure(int sizeHint)
    {
        if (sizeHint < 1) sizeHint = 1;
        if (_used + sizeHint <= _buffer.Length) return;
        Flush();
        if (sizeHint <= _buffer.Length) return;
        IndexBuildPool.Slabs.Return(_buffer);
        _buffer = IndexBuildPool.Slabs.Rent(sizeHint);
    }

    public void Dispose()
    {
        Flush();
        IndexBuildPool.Slabs.Return(_buffer);
        _buffer = Array.Empty<byte>();
    }
}
