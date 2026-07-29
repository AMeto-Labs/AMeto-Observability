using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;

namespace Ameto.Otel;

/// <summary>
/// Minimal protobuf wire-format reader over a <see cref="ReadOnlySpan{T}"/>.
///
/// <para>Exists because <c>Google.Protobuf.CodedInputStream</c> cannot descend into an
/// embedded message without allocating: <c>PushLimit</c>/<c>PopLimit</c> stopped being
/// public in 3.21, so the only route left is <c>ReadBytes().CreateCodedInput()</c> —
/// a payload copy plus a parser object for EVERY nested message. On OTLP that is once
/// per data point and once per attribute, which is most of the ingest CPU.</para>
///
/// <para>This is a <c>ref struct</c>: it lives on the stack, and
/// <see cref="ReadLengthDelimited"/> returns a slice of the caller's buffer, so
/// descending into a nested message costs nothing at all.</para>
///
/// Only the wire types OTLP uses are supported; groups (wire types 3/4) are obsolete in
/// proto3 and throw rather than being silently mis-skipped.
/// </summary>
internal ref struct ProtoReader
{
    private readonly ReadOnlySpan<byte> _buf;
    private int _pos;

    public ProtoReader(ReadOnlySpan<byte> buf)
    {
        _buf = buf;
        _pos = 0;
    }

    public readonly bool End => _pos >= _buf.Length;

    /// <summary>Next tag, or 0 at end of buffer. Tag = (fieldNumber &lt;&lt; 3) | wireType.</summary>
    public uint ReadTag() => End ? 0u : (uint)ReadVarint();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong ReadVarint()
    {
        ulong result = 0;
        int shift = 0;
        while (shift < 64)
        {
            if (_pos >= _buf.Length) throw new InvalidDataException("Truncated varint");
            byte b = _buf[_pos++];
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
        }
        throw new InvalidDataException("Malformed varint");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong ReadFixed64()
    {
        if (_pos + 8 > _buf.Length) throw new InvalidDataException("Truncated fixed64");
        ulong v = BinaryPrimitives.ReadUInt64LittleEndian(_buf.Slice(_pos, 8));
        _pos += 8;
        return v;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public uint ReadFixed32()
    {
        if (_pos + 4 > _buf.Length) throw new InvalidDataException("Truncated fixed32");
        uint v = BinaryPrimitives.ReadUInt32LittleEndian(_buf.Slice(_pos, 4));
        _pos += 4;
        return v;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public double ReadDouble() => BitConverter.UInt64BitsToDouble(ReadFixed64());

    /// <summary>
    /// Length-prefixed bytes as a slice of the SAME buffer — no copy, no allocation.
    /// Feed it to a nested <see cref="ProtoReader"/> to descend into an embedded message.
    /// </summary>
    public ReadOnlySpan<byte> ReadLengthDelimited()
    {
        int len = checked((int)ReadVarint());
        if (len < 0 || _pos + len > _buf.Length) throw new InvalidDataException("Truncated length-delimited field");
        var slice = _buf.Slice(_pos, len);
        _pos += len;
        return slice;
    }

    public string ReadString()
    {
        var bytes = ReadLengthDelimited();
        return bytes.IsEmpty ? string.Empty : Encoding.UTF8.GetString(bytes);
    }

    /// <summary>Skips the value of a field whose tag was just read.</summary>
    public void SkipField(uint tag)
    {
        switch (tag & 7)
        {
            case 0: ReadVarint(); break;                    // varint
            case 1: ReadFixed64(); break;                   // 64-bit
            case 2: ReadLengthDelimited(); break;           // length-delimited
            case 5: ReadFixed32(); break;                   // 32-bit
            default: throw new InvalidDataException($"Unsupported wire type in tag {tag}");
        }
    }
}
