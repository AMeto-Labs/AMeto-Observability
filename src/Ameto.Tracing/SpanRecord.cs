using MessagePack;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Ameto.Tracing;

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

    /// <summary>
    /// The top 64 bits, big-endian — the first eight bytes of the id as it arrived.
    ///
    /// <para>Exposed for the trace-id index, which keys on exactly these bytes rather than all
    /// sixteen: ids are uniformly random, so a 64-bit prefix collides about once in 2e-7 at this
    /// engine's trace counts, and a collision there costs one wasted segment read rather than a
    /// wrong answer — the full id is still checked against the spans. Half the key is half the
    /// index. Sorting by it is also sorting by the id, since it is the most significant half.</para>
    /// </summary>
    public ulong High => _hi;

    /// <summary>
    /// A total order over trace ids. The ordering itself means nothing — it exists so a
    /// sort that ties on timestamp has a deterministic winner, which is what keeps a
    /// paginated boundary stable from one page request to the next.
    /// </summary>
    public int CompareTo(TraceId other)
    {
        int byHi = _hi.CompareTo(other._hi);
        return byHi != 0 ? byHi : _lo.CompareTo(other._lo);
    }

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

    /// <summary>Promoted HTTP response status code (0 = not set). Avoids msgpack attr scan on filter.</summary>
    public short          HttpStatusCode;   // 2 bytes
    private short         _pad2;            // 2 bytes — keeps Size = 76

    public static int SizeOf => Unsafe.SizeOf<SpanHeader>();
}

/// <summary>
/// Fully materialised span — returned from queries, not stored on the hot path.
/// </summary>
public sealed class SpanRecord
{
    private readonly ReadOnlyMemory<byte> _attributesBytes;
    private IReadOnlyDictionary<string, object?>? _attributes;
    private volatile bool _decoded;

    public TraceId        TraceId             { get; init; }
    public SpanId         SpanId              { get; init; }
    public SpanId         ParentSpanId        { get; init; }
    public long           StartTimeUnixNano   { get; init; }
    public long           DurationNanos       { get; init; }
    public string         Name                { get; init; } = string.Empty;
    public string         ServiceName         { get; init; } = string.Empty;
    public SpanKind       Kind                { get; init; }
    public SpanStatusCode Status              { get; init; }

    /// <summary>Promoted HTTP response status code (0 = not extracted from attributes).</summary>
    public short HttpStatusCode { get; init; }

    /// <summary>
    /// THE ATTRIBUTES AS THEY ARRIVED — one msgpack map, the bytes the OTLP mapper produced or the
    /// bytes that sit in the <c>.trc</c> block, and the form the hot tier keeps them in.
    ///
    /// <para>375 bytes for an ordinary eight-attribute OTel span, against the ~987 bytes the
    /// <see cref="Attributes"/> dictionary weighs for the same span — a <c>Dictionary</c>, eight
    /// key strings and eight boxed values, which the ingest path used to build under the engine's
    /// exclusive write lock, for a map the ingest path never reads. Empty when the span carries no
    /// attributes, and empty on a record built from a dictionary directly.</para>
    /// </summary>
    public ReadOnlyMemory<byte> AttributesBytes
    {
        get => _attributesBytes;
        init => _attributesBytes = value;
    }

    /// <summary>
    /// Decoded key-value attributes — lazy, and now actually lazy: decoded from
    /// <see cref="AttributesBytes"/> the first time somebody asks, null when there is nothing to
    /// decode or the blob will not decode.
    ///
    /// <para>A BLOB THAT WILL NOT DECODE ANSWERS NULL FOREVER, never an empty map. TraceQL's
    /// three-valued logic reads null as "this span cannot answer" (issue #74); an empty dictionary
    /// reads as "this span has no such attribute", which is an answer, and
    /// <c>{ !(.foo = "bar") }</c> would then select every span whose attributes were unreadable.
    /// The failure is remembered, so the exception is paid once per record and not once per read.</para>
    ///
    /// <para>The decode is idempotent and its result immutable, so two threads racing here cost at
    /// most one duplicated dictionary and never a torn one: the reference is published by the
    /// volatile store to <c>_decoded</c>, and a second decode simply returns its own copy.</para>
    /// </summary>
    public IReadOnlyDictionary<string, object?>? Attributes
    {
        get
        {
            if (_decoded) return _attributes;

            var decoded = _attributesBytes.IsEmpty ? null : SpanAttributeBlob.Decode(_attributesBytes);
            _attributes = decoded;
            _decoded    = true;   // volatile store — publishes _attributes with it
            return decoded;
        }
        init
        {
            _attributes = value;
            _decoded    = true;   // an explicitly supplied map IS the answer; never decode over it
        }
    }

    public DateTimeOffset StartTime =>
        DateTimeOffset.FromUnixTimeMilliseconds(StartTimeUnixNano / 1_000_000);

    public TimeSpan Duration => TimeSpan.FromTicks(DurationNanos / 100);
}

/// <summary>What one attribute in a msgpack blob turned out to be. See <see cref="SpanAttrValue"/>.</summary>
internal enum SpanAttrKind : byte
{
    /// <summary>The key is not in the map at all.</summary>
    Missing = 0,
    /// <summary>Present, and msgpack-nil.</summary>
    Null,
    Utf8String,
    Integer,
    Float,
    Boolean,
    /// <summary>An array, a nested map, binary or an extension — anything the decoder boxes as null.</summary>
    Other,
}

/// <summary>
/// One attribute value read straight out of the blob. <see cref="Utf8"/> is a WINDOW ONTO the
/// span's own attribute bytes, not a copy: reading a string attribute out of a blob allocates
/// nothing and boxes nothing.
///
/// <para>A <c>Memory</c> rather than a <c>Span</c>, which is the difference between this being a
/// plain struct and being a <c>ref struct</c>. The span it wraps comes from the blob's array and
/// is perfectly safe to hand back, but a <c>ref struct</c> carrying it out of a method that also
/// takes <c>ref MessagePackReader</c> cannot be proved so by the ref-safety rules, and the price
/// of proving it would be a copy.</para>
/// </summary>
internal struct SpanAttrValue
{
    public SpanAttrKind         Kind;
    public ReadOnlyMemory<byte> Utf8;      // Kind == Utf8String
    public long                 Integer;   // Kind == Integer
    public double               Float;     // Kind == Float
    public bool                 Boolean;   // Kind == Boolean
}

/// <summary>
/// THE HTTP SEMCONV KEY LISTS, IN ONE PLACE, because two places is two answers. A trace row's
/// method and path are read on two paths — <c>TraceStorageEngine.MergeSpanInto</c> builds the
/// trace list, <c>TraceQLExecutor.BuildRow</c> builds a TraceQL page — and each kept its own copy
/// of the lists. They had drifted: the engine looked under five path keys and the executor under
/// three, so a span whose path arrived as <c>url.full</c> or <c>http.url</c> showed its path in
/// the trace list and an empty path in a TraceQL row FOR THE SAME TRACE.
///
/// <para>Order is the semantics: the first key present with a non-null value wins, so these are
/// most-specific-first. Adding a key means adding it here, and both readers get it.</para>
///
/// <para>The lists must stay DISJOINT — a key in both would be found at its first rank only, and
/// the second list would read it as absent. <c>TraceHotTierProbe.The_semconv_key_lists_are_disjoint</c>
/// is what holds them so.</para>
/// </summary>
internal static class HttpSemconvKeys
{
    internal static readonly string[] MethodKeys = ["http.request.method", "http.method"];
    internal static readonly string[] PathKeys   = ["url.path", "http.target", "http.route", "url.full", "http.url"];
}

/// <summary>
/// Reads a span's msgpack attribute map — either into a dictionary, or one key at a time with
/// neither a dictionary nor a boxed value in sight.
///
/// <para>THE BOXING RULES HERE ARE A COMPATIBILITY CONTRACT, not a choice. They are the rules
/// <c>SpanReader</c>'s v3 block decoder has always applied — integer to <c>long</c>, float to
/// <c>double</c>, everything it does not understand to <c>null</c> — so that a span read back out
/// of a segment and the same span still sitting in the hot tier answer a TraceQL predicate
/// identically. The hot tier used to go through <c>MessagePackSerializer.Deserialize</c> into a
/// <c>Dictionary</c> instead, whose primitive formatter hands back the NARROWEST integer type
/// (1433 comes back as <c>ushort</c>), and <c>AttributePredicate</c> has no case for <c>ushort</c>
/// — so <c>{ .net.peer.port &gt; 1000 }</c> quietly answered "no" for a hot span and "yes" for the
/// same span once flushed. One decoder, one answer.</para>
///
/// <para>LAST KEY WINS, because the OTLP mapper writes resource attributes first and span
/// attributes second precisely so that a span attribute shadows a resource one of the same name
/// (<c>OtlpTraceMapper.SerializeAttributes</c>). That is a duplicate key in the map, which is also
/// why the hot tier could not use <c>Deserialize</c> honestly: that formatter calls
/// <c>Dictionary.Add</c>, throws on the second copy of the key, and left such a span with NO
/// attributes at all.</para>
/// </summary>
internal static class SpanAttributeBlob
{
    /// <summary>
    /// Decodes the whole map. Null when the bytes are not a readable msgpack map — see
    /// <see cref="SpanRecord.Attributes"/> for why null rather than an empty map.
    /// </summary>
    internal static IReadOnlyDictionary<string, object?>? Decode(ReadOnlyMemory<byte> blob)
    {
        try
        {
            var reader = new MessagePackReader(blob);
            int count  = reader.ReadMapHeader();
            var dict   = new Dictionary<string, object?>(count, StringComparer.Ordinal);
            for (int i = 0; i < count; i++)
            {
                string key = reader.ReadString() ?? string.Empty;
                dict[key]  = ReadBoxedValue(ref reader);   // indexer, not Add: last key wins
            }
            return dict;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// One attribute value, boxed exactly as <c>SpanReader</c>'s block decoder boxes it. Used by
    /// <see cref="Decode"/> and by the flush path that feeds <c>SpanBloom</c>.
    /// </summary>
    internal static object? ReadBoxedValue(ref MessagePackReader r)
    {
        switch (r.NextMessagePackType)
        {
            case MessagePackType.String:  return r.ReadString();
            case MessagePackType.Integer: return r.ReadInt64();
            case MessagePackType.Float:   return r.ReadDouble();
            case MessagePackType.Boolean: return r.ReadBoolean();
            case MessagePackType.Nil:     r.ReadNil(); return null;
            default:                      r.Skip();    return null;
        }
    }

    /// <summary>The most keys <see cref="FindValues"/> will resolve in one walk.</summary>
    internal const int MaxKeyAlternatives = 8;

    /// <summary>
    /// SEVERAL KEYS, ONE WALK OF THE MAP. Fills <paramref name="slots"/> with the value of every
    /// key of <paramref name="keysUtf8"/> the map holds, and returns a bit per filled rank.
    ///
    /// <para>The semconv lookups this exists for ask LISTS, not keys: the HTTP path of a root span
    /// is <c>url.path</c>, else <c>http.target</c>, else <c>http.route</c>, else <c>url.full</c>,
    /// else <c>http.url</c>, and the method is two more. Asking <see cref="TryFind"/> once per key
    /// walks the whole map once per key — seven walks per root span to establish that an ordinary
    /// database span has no HTTP anything — and on the trace-list path those walks happen with the
    /// engine read lock held. One walk answers the whole question.</para>
    ///
    /// <para>The same answers a <see cref="Decode"/>d dictionary gives when it is probed key by
    /// key: the LAST copy of a key wins (the OTLP mapper writes resource attributes before span
    /// attributes precisely so a span attribute shadows a resource one), and a key whose value is
    /// msgpack nil, an array or a nested map is a key whose dictionary value is <c>null</c> — so
    /// its bit stays clear and the caller's next key gets its turn, exactly as a <c>v is not
    /// null</c> test would have let it. Zero means nothing was found, or the map is unreadable:
    /// one answer for both, because a blob that cannot be read cannot say which it is.</para>
    /// </summary>
    internal static int FindValues(
        ReadOnlyMemory<byte> blob, ReadOnlySpan<byte[]> keysUtf8, Span<SpanAttrValue> slots)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(keysUtf8.Length, MaxKeyAlternatives);
        ArgumentOutOfRangeException.ThrowIfLessThan(slots.Length, keysUtf8.Length);

        if (blob.IsEmpty || keysUtf8.Length == 0) return 0;

        int seen = 0;   // bit per rank
        try
        {
            var reader = new MessagePackReader(blob);
            int count  = reader.ReadMapHeader();
            for (int i = 0; i < count; i++)
            {
                // TWO SKIPS IN THE ELSE BRANCH, NOT ONE, and that is the whole of the care this
                // loop needs. TryReadStringSpan is the reader's fast path — a direct slice of the
                // current span, against ReadStringSequence's SequencePosition arithmetic — but it
                // DOES NOT ADVANCE when it declines (a msgpack-nil key; a string spanning
                // segments, which cannot happen over one ReadOnlyMemory but is not this loop's to
                // assume). Falling through to the single Skip below would then consume the KEY and
                // leave the VALUE to be read as the next key, putting every remaining pair one
                // slot out of step — an ordinary `url.path` sitting after such a key vanishes. So
                // the key gets its own Skip here, and the value still gets the one below. A key
                // that is neither a string nor nil throws, exactly as Decode's ReadString does.
                int rank = -1;
                if (reader.TryReadStringSpan(out var k))
                {
                    // Length first: SequenceEqual is an out-of-line call into SpanHelpers and this
                    // inner loop runs once per candidate key per pair, so an eight-attribute map
                    // asks it 56 times. The semconv key lengths are distinctive enough that the
                    // length test leaves ONE of those calls for an ordinary database span. It did
                    // not move the eight-attribute probe below its noise; it is what keeps the
                    // walk linear in the attribute count on a span that carries fifty.
                    for (int j = 0; j < keysUtf8.Length; j++)
                    {
                        var candidate = keysUtf8[j];
                        if (candidate.Length == k.Length && k.SequenceEqual(candidate)) { rank = j; break; }
                    }
                }
                else
                {
                    reader.Skip();   // the key itself
                }

                if (rank < 0) { reader.Skip(); continue; }   // the value

                slots[rank] = default;                  // a later copy of the key replaces the earlier
                ReadValue(ref reader, ref slots[rank]);
                seen |= 1 << rank;
            }
        }
        catch
        {
            return 0;
        }

        // A value a dictionary would hold as null is not an answer — see the summary.
        for (int j = 0; j < keysUtf8.Length; j++)
            if ((seen & (1 << j)) != 0 && slots[j].Kind is SpanAttrKind.Null or SpanAttrKind.Other)
                seen &= ~(1 << j);

        return seen;
    }

    /// <summary>
    /// Finds one key without building anything. False means the key is not in the map, or the map
    /// is not readable — the two cases a caller must treat the same way, because a blob that
    /// cannot be read cannot say whether it holds the key either.
    /// </summary>
    internal static bool TryFind(
        ReadOnlyMemory<byte> blob, ReadOnlySpan<byte> keyUtf8, out SpanAttrValue value)
    {
        value = default;
        if (blob.IsEmpty) return false;

        bool found = false;
        try
        {
            var reader = new MessagePackReader(blob);
            int count  = reader.ReadMapHeader();
            for (int i = 0; i < count; i++)
            {
                var keySeq = reader.ReadStringSequence();
                if (keySeq is { IsSingleSegment: true } ks && ks.First.Span.SequenceEqual(keyUtf8))
                {
                    ReadValue(ref reader, ref value);
                    found = true;   // keep walking: the LAST copy of the key is the one that counts
                }
                else
                {
                    reader.Skip();
                }
            }
        }
        catch
        {
            return false;
        }
        return found;
    }

    /// <summary>
    /// Walks the map and hands every pair to <paramref name="onPair"/>, boxing one value at a time
    /// instead of a whole dictionary. False when the blob is not exactly one well-formed msgpack
    /// map — which is what lets the flush decide whether the bytes are safe to copy through
    /// verbatim.
    /// </summary>
    internal static bool TryWalk<TState>(
        ReadOnlyMemory<byte> blob, TState state, Action<TState, string, object?> onPair)
    {
        try
        {
            var reader = new MessagePackReader(blob);
            int count  = reader.ReadMapHeader();
            for (int i = 0; i < count; i++)
            {
                string key = reader.ReadString() ?? string.Empty;
                onPair(state, key, ReadBoxedValue(ref reader));
            }
            // Trailing bytes would be copied through by a verbatim write and would corrupt the
            // span array around them, so "one map" has to mean the WHOLE blob and nothing after it.
            return reader.End;
        }
        catch
        {
            return false;
        }
    }

    private static void ReadValue(ref MessagePackReader r, ref SpanAttrValue v)
    {
        switch (r.NextMessagePackType)
        {
            case MessagePackType.String:
            {
                var seq = r.ReadStringSequence();
                // The reader is always built over ONE ReadOnlyMemory, so a string is always one
                // segment. A segmented one would need the copy this type exists to avoid; it is
                // reported as Other, which reads as "cannot answer" rather than as a wrong answer.
                if (seq is { IsSingleSegment: true } s) { v.Kind = SpanAttrKind.Utf8String; v.Utf8 = s.First; }
                else                                    { v.Kind = SpanAttrKind.Other; }
                break;
            }
            case MessagePackType.Integer: v.Kind = SpanAttrKind.Integer; v.Integer = r.ReadInt64();   break;
            case MessagePackType.Float:   v.Kind = SpanAttrKind.Float;   v.Float   = r.ReadDouble();  break;
            case MessagePackType.Boolean: v.Kind = SpanAttrKind.Boolean; v.Boolean = r.ReadBoolean(); break;
            case MessagePackType.Nil:     v.Kind = SpanAttrKind.Null;    r.ReadNil();                 break;
            default:                      v.Kind = SpanAttrKind.Other;   r.Skip();                    break;
        }
    }
}

/// <summary>
/// One <see cref="SpanAttrValue"/> slot per candidate key of a
/// <see cref="SpanAttributeBlob.FindValues"/> lookup, inline in the caller's frame. A
/// <c>stackalloc</c> cannot hold these — the type carries a <c>ReadOnlyMemory&lt;byte&gt;</c>
/// window onto the blob, which is exactly what makes reading a string attribute out of it free.
/// </summary>
[System.Runtime.CompilerServices.InlineArray(SpanAttributeBlob.MaxKeyAlternatives)]
internal struct AttrSlots
{
    private SpanAttrValue _element0;
}
