using System.Buffers;
using K4os.Compression.LZ4;

namespace Ameto.Tracing.Storage;

/// <summary>
/// Pickles LZ4 frames into ONE rented buffer that every block of a segment reuses: the frame
/// <see cref="LZ4Pickler.Pickle(ReadOnlySpan{byte}, LZ4Level)"/> returns, byte for byte, without the
/// array it returns it in.
///
/// <para>WHY NOT THE PICKLER'S OWN OVERLOADS. The array overload allocates its result per call — 140 KB
/// to half a megabyte per span block, on the large-object heap, three to fourteen times a flush — and
/// stages the encode through a pinned buffer of the raw block's size first (native past 1 MiB, with a
/// <c>GC.AddMemoryPressure</c> the runtime counts towards its next gen2). Its <c>IBufferWriter&lt;byte&gt;</c>
/// overload writes into a caller's buffer, but sizes the frame header from the SOURCE length where the
/// array overload sizes it from the length DIFFERENCE: two bytes apart on a 70 000-byte block that
/// shrinks to 11 400 (measured on 1.3.8: 11 400 B from the array overload, 11 402 from the writer one),
/// and apart on every block whose difference needs fewer bytes than its length — the v3 trace-index
/// block, every block of a small flush. That overload moves bytes on disk, and the golden <c>.trc</c>
/// hashes (<c>TraceFlushProbe</c>) say so. So this encodes with
/// <see cref="LZ4Codec.Encode(ReadOnlySpan{byte}, Span{byte}, LZ4Level)"/> straight into the rented
/// buffer, past room for the widest header, and writes the header in front of the payload as the array
/// overload writes it: one byte of flags — pickle version 0 in the low bits, the width of the length
/// difference (1, 2 or 4 bytes; 4 is written as 3) in bits 6–7 — then the difference in that many
/// little-endian bytes; or a single zero byte and the raw bytes when LZ4 did not shrink them.
/// <c>PooledLz4PicklerTests</c> pins the frames to the pickler's own, shape by shape; a reader inflates
/// them with <see cref="LZ4Pickler.Unpickle(ReadOnlySpan{byte})"/> as before.</para>
///
/// <para>THE BUFFER is the engine's between flushes (<see cref="SpanWriteScratch.RentPickle"/>, #90: a
/// thread-pool flush would otherwise miss the shared pool's slot its last flush left on another thread)
/// or the shared pool's, rented at the first block, grown to the largest block seen — a rental of that
/// block's size plus the header, the one it outgrew going back — and given back once, in
/// <see cref="Return"/>. One of these per segment write, used from one thread.</para>
/// </summary>
internal sealed class PooledLz4Pickler
{
    /// <summary>The widest frame header: the flags byte and a four-byte length difference.</summary>
    internal const int MaxHeaderBytes = 5;

    private readonly SpanWriteScratch? _scratch;
    private byte[]? _buffer;

    public PooledLz4Pickler(SpanWriteScratch? scratch) => _scratch = scratch;

    /// <summary>
    /// The frame of <paramref name="raw"/> at <paramref name="level"/>, in this pickler's buffer — valid
    /// until the next call or <see cref="Return"/>, so write it out before either. Empty for an empty
    /// source, as the pickler's is.
    /// </summary>
    public ReadOnlySpan<byte> Pickle(ReadOnlySpan<byte> raw, LZ4Level level)
    {
        int n = raw.Length;
        if (n == 0) return default;

        // A frame is never longer than the raw bytes behind one header byte, so the raw length past
        // the widest header is room for every outcome.
        var buf     = Buffer(MaxHeaderBytes + n).AsSpan();
        int encoded = LZ4Codec.Encode(raw, buf.Slice(MaxHeaderBytes, n), level);
        if (encoded <= 0 || encoded >= n)
        {
            // LZ4 did not shrink it (or could not fit it in n bytes): stored as it is, behind a zero byte.
            buf[MaxHeaderBytes - 1] = 0;
            raw.CopyTo(buf.Slice(MaxHeaderBytes));
            return buf.Slice(MaxHeaderBytes - 1, 1 + n);
        }

        int diff       = n - encoded;
        int sizeOfDiff = diff > 0xFFFF ? 4 : diff > 0xFF ? 2 : 1;
        int start      = MaxHeaderBytes - 1 - sizeOfDiff;
        buf[start] = (byte)((sizeOfDiff == 4 ? 3 : sizeOfDiff) << 6);
        for (int i = 0; i < sizeOfDiff; i++) buf[start + 1 + i] = (byte)(diff >> (8 * i));
        return buf.Slice(start, 1 + sizeOfDiff + encoded);
    }

    /// <summary>Test hook: the buffer in use right now (null before the first block and after <see cref="Return"/>).</summary>
    internal byte[]? BufferForTest => _buffer;

    private byte[] Buffer(int needed)
    {
        var buf = _buffer;
        if (buf is null)
            return _buffer = _scratch?.RentPickle(needed) ?? ArrayPool<byte>.Shared.Rent(needed);
        if (buf.Length >= needed) return buf;

        // Outgrown. The next flush's blocks are the size of this one's, so the larger rental is the one
        // to keep; the one it outgrew goes to the shared pool now rather than displacing it from the
        // engine's slot later.
        ArrayPool<byte>.Shared.Return(buf);
        return _buffer = ArrayPool<byte>.Shared.Rent(needed);
    }

    /// <summary>
    /// Gives the buffer back — to the engine's scratch when this pickler was given one, else to the
    /// shared pool. Idempotent: only the first call has a buffer to give.
    /// </summary>
    public void Return()
    {
        var buf = _buffer;
        if (buf is null) return;
        _buffer = null;
        if (_scratch is not null) _scratch.ReturnPickle(buf);
        else                      ArrayPool<byte>.Shared.Return(buf);
    }
}
