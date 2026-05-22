using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Rd.Log.Tracing;

/// <summary>
/// W3C-compatible 128-bit trace identifier.
/// </summary>
[StructLayout(LayoutKind.Sequential, Size = 16)]
public readonly struct TraceId : IEquatable<TraceId>
{
    private readonly ulong _hi;
    private readonly ulong _lo;

    public TraceId(ulong hi, ulong lo) { _hi = hi; _lo = lo; }

    public bool IsEmpty => _hi == 0 && _lo == 0;

    public static TraceId Parse(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < 16) return default;
        var hi = System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(bytes);
        var lo = System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(bytes[8..]);
        return new TraceId(hi, lo);
    }

    /// <summary>Parse from 32-char lowercase hex string (OTLP JSON format).</summary>
    public static bool TryParseHex(ReadOnlySpan<char> hex, out TraceId id)
    {
        id = default;
        if (hex.Length != 32) return false;
        if (!TryParseHexU64(hex[..16], out var hi)) return false;
        if (!TryParseHexU64(hex[16..], out var lo)) return false;
        id = new TraceId(hi, lo);
        return true;
    }

    public void WriteTo(Span<byte> dest)
    {
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64BigEndian(dest,       _hi);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt64BigEndian(dest[8..],  _lo);
    }

    public bool Equals(TraceId other) => _hi == other._hi && _lo == other._lo;
    public override bool Equals(object? obj) => obj is TraceId t && Equals(t);
    public override int GetHashCode() => HashCode.Combine(_hi, _lo);
    public override string ToString() => $"{_hi:x16}{_lo:x16}";

    private static bool TryParseHexU64(ReadOnlySpan<char> s, out ulong v)
    {
        v = 0;
        for (int i = 0; i < s.Length; i++)
        {
            int n = HexDigit(s[i]);
            if (n < 0) return false;
            v = (v << 4) | (uint)n;
        }
        return true;
    }
    private static int HexDigit(char c) => c switch
    {
        >= '0' and <= '9' => c - '0',
        >= 'a' and <= 'f' => c - 'a' + 10,
        >= 'A' and <= 'F' => c - 'A' + 10,
        _                 => -1,
    };
}

/// <summary>
/// W3C-compatible 64-bit span identifier.
/// </summary>
[StructLayout(LayoutKind.Sequential, Size = 8)]
public readonly struct SpanId : IEquatable<SpanId>
{
    private readonly ulong _value;

    public SpanId(ulong value) => _value = value;
    public ulong RawValue => _value;
    public bool IsEmpty => _value == 0;

    public static SpanId Parse(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < 8) return default;
        return new SpanId(System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(bytes));
    }

    /// <summary>Parse from 16-char lowercase hex string (OTLP JSON format).</summary>
    public static bool TryParseHex(ReadOnlySpan<char> hex, out SpanId id)
    {
        id = default;
        if (hex.Length != 16) return false;
        ulong v = 0;
        for (int i = 0; i < 16; i++)
        {
            int n = HexDigit(hex[i]);
            if (n < 0) return false;
            v = (v << 4) | (uint)n;
        }
        id = new SpanId(v);
        return true;
    }

    public bool Equals(SpanId other) => _value == other._value;
    public override bool Equals(object? obj) => obj is SpanId s && Equals(s);
    public override int GetHashCode() => _value.GetHashCode();
    public override string ToString() => $"{_value:x16}";

    private static int HexDigit(char c) => c switch
    {
        >= '0' and <= '9' => c - '0',
        >= 'a' and <= 'f' => c - 'a' + 10,
        >= 'A' and <= 'F' => c - 'A' + 10,
        _                 => -1,
    };
}

/// <summary>
/// OpenTelemetry SpanKind.
/// </summary>
public enum SpanKind : byte
{
    Unspecified = 0,
    Internal    = 1,
    Server      = 2,
    Client      = 3,
    Producer    = 4,
    Consumer    = 5,
}

/// <summary>
/// OpenTelemetry SpanStatus code.
/// </summary>
public enum SpanStatusCode : byte
{
    Unset = 0,
    Ok    = 1,
    Error = 2,
}

/// <summary>
/// Fixed-size header stored in the ring buffer and hot-tier NativeMemory array.
/// Total: 72 bytes.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 72)]
public struct SpanHeader
{
    /// <summary>128-bit W3C trace id.</summary>
    public TraceId TraceId;                 // 16 bytes

    /// <summary>64-bit span id.</summary>
    public SpanId  SpanId;                  // 8 bytes

    /// <summary>64-bit parent span id (zero = root span).</summary>
    public SpanId  ParentSpanId;            // 8 bytes

    /// <summary>Span start time, Unix nanoseconds.</summary>
    public long    StartTimeUnixNano;       // 8 bytes

    /// <summary>Span duration in nanoseconds.</summary>
    public long    DurationNanos;           // 8 bytes

    /// <summary>Offset of the span name in the string intern pool.</summary>
    public int     NamePoolIndex;           // 4 bytes

    /// <summary>Offset of the service name in the string intern pool.</summary>
    public int     ServiceNamePoolIndex;    // 4 bytes

    /// <summary>Byte offset of the msgpack attributes blob in the payload arena.</summary>
    public int     AttributesArenaOffset;   // 4 bytes

    /// <summary>Byte length of the msgpack attributes blob.</summary>
    public int     AttributesByteLength;    // 4 bytes

    public SpanKind       Kind;             // 1 byte
    public SpanStatusCode Status;           // 1 byte
    public byte           Flags;            // 1 byte (reserved)
    private byte          _pad;             // 1 byte

    public static int SizeOf => Unsafe.SizeOf<SpanHeader>();
}

/// <summary>
/// Fully materialised span — returned from queries, not stored on the hot path.
/// </summary>
public sealed class SpanRecord
{
    public TraceId        TraceId             { get; init; }
    public SpanId         SpanId              { get; init; }
    public SpanId         ParentSpanId        { get; init; }
    public long           StartTimeUnixNano   { get; init; }
    public long           DurationNanos       { get; init; }
    public string         Name                { get; init; } = string.Empty;
    public string         ServiceName         { get; init; } = string.Empty;
    public SpanKind       Kind                { get; init; }
    public SpanStatusCode Status              { get; init; }

    /// <summary>Decoded key-value attributes (lazy — null until read).</summary>
    public IReadOnlyDictionary<string, object?>? Attributes { get; init; }

    public DateTimeOffset StartTime =>
        DateTimeOffset.FromUnixTimeMilliseconds(StartTimeUnixNano / 1_000_000);

    public TimeSpan Duration => TimeSpan.FromTicks(DurationNanos / 100);
}
