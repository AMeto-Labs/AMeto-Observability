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
/// A span's fixed part — 72 bytes, no references — as the ingest ring carries it (TI#3). Its
/// variable part (name, service and attribute blob, UTF-8 / msgpack) lives in the ring's payload
/// arena at <see cref="PayloadArenaOffset"/>, in that order.
///
/// <para><b>It was dead code until the ring.</b> Written for "the ring buffer and hot-tier
/// NativeMemory array" and referenced nowhere; its first user reshaped the four reserved ints to
/// what a slot actually needs. The NAME travels as bytes, not as a pool index: names can be
/// high-cardinality, their pool is shed at every flush (<see cref="SpanStringPools"/>), and an
/// index carried through a ring past a shed would name another string. The SERVICE travels as
/// both — the pool's index, which the drainer resolves with an array load, and its bytes, which
/// the write-ahead log needs anyway — because the service pool is never shed.</para>
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

    /// <summary>UTF-8 bytes of the span name — the first bytes of the payload.</summary>
    public int     NameByteLength;          // 4 bytes

    /// <summary>The service's index in the service intern pool, or -1 when it has none (a full pool, an empty name).</summary>
    public int     ServiceNamePoolIndex;    // 4 bytes

    /// <summary>Where the payload (name, service, attributes) starts in the ring's arena; -1 when there is none; <c>-2 - n</c> when it is parked apart in place n (larger than a chunk).</summary>
    public int     PayloadArenaOffset;      // 4 bytes

    /// <summary>Byte length of the msgpack attributes blob — the last bytes of the payload.</summary>
    public int     AttributesByteLength;    // 4 bytes

    /// <summary>UTF-8 bytes of the service name — between the name and the attributes.</summary>
    public int     ServiceByteLength;       // 4 bytes

    public SpanKind       Kind;             // 1 byte
    public SpanStatusCode Status;           // 1 byte

    /// <summary>Promoted HTTP response status code (0 = not set). Avoids msgpack attr scan on filter.</summary>
    public short          HttpStatusCode;   // 2 bytes

    /// <summary>The whole payload: name, service and attributes.</summary>
    public readonly int PayloadByteLength => NameByteLength + ServiceByteLength + AttributesByteLength;

    public static int SizeOf => Unsafe.SizeOf<SpanHeader>();
}

/// <summary>Which of <see cref="SpanStringPools"/>' two pools a notice is about.</summary>
internal enum SpanPoolKind : byte { Names, Services }

/// <summary>
/// THE TWO INTERN POOLS SPAN TEXT GOES THROUGH on its way into the hot tier (TI#5): span names and
/// service names, each held by the tier as ONE shared string per distinct value instead of one per
/// span.
///
/// <para><b>Why.</b> A span name is a route template or an operation name — <c>GET
/// /api/v1/orders/{id}</c> — repeated across essentially every span of a batch, and the parser
/// hands every span its own fresh copy: ~56 B/span for an ordinary name, retained for the tier's
/// whole life, gen2 garbage at every flush (<c>TraceSpanInternProbe</c>: 197 → 140 B/span for a
/// record and its strings). A service name is one string per resource block, but a DIFFERENT
/// object per request, so a tier spanning ten thousand requests held ten thousand
/// <c>"Wallet.API"</c>s.</para>
///
/// <para><b>Only the shared INSTANCE is kept, never an index.</b> The tier stores the canonical
/// string reference the pool hands back; no pool index is ever stored where it could outlive the
/// pool that issued it. That is what makes shedding a pool safe at any moment — an index would
/// name some other string after a reset, a reference cannot.</para>
///
/// <para><b>Bounded, and the name pool is shed on flush.</b> Names can be high-cardinality
/// (<c>/api/user/12345</c> when an instrumentation forgets its route template), so the name pool
/// holds at most <see cref="DefaultMaxNames"/> distinct names and is REPLACED by an empty one each
/// time the tier is detached for a flush: it never holds more than one tier's worth, and a
/// high-cardinality burst costs its dictionary once and is gone with its tier. Services are
/// low-cardinality by nature and their pool is bounded and never shed; the span ring's producers
/// intern into it once per resource block.</para>
///
/// <para><b>A full pool never drops a span.</b> <see cref="StringInternPool"/> answers −1 once it
/// has reached its cap; the span then keeps its own string — the argument itself, or a fresh one
/// built from the bytes — exactly as it would have with no pool at all, and the hot tier's byte
/// budget charges that string to the span (see <see cref="UnpooledStringBytes"/>).</para>
/// </summary>
internal sealed class SpanStringPools
{
    /// <summary>Distinct span names one tier may pool. Past it, a span keeps its own name string.</summary>
    internal const int DefaultMaxNames = 16_384;

    /// <summary>Distinct service names the process may pool. Past it, a span keeps its own service string.</summary>
    internal const int DefaultMaxServices = 4_096;

    private readonly int _maxNames;
    private volatile Ameto.Core.StringInternPool _names;
    private long _unpooledNames;
    private long _unpooledServices;
    private long _saturations;

    public SpanStringPools(int maxNames = DefaultMaxNames, int maxServices = DefaultMaxServices)
    {
        _maxNames = maxNames;
        _names    = NewNamePool();
        Services  = new Ameto.Core.StringInternPool(maxServices);
        Services.PoolExhausted += cap => OnSaturated(SpanPoolKind.Services, cap);
    }

    /// <summary>
    /// Raised when a pool fills up (review F4): the service pool once for the life of the process
    /// (it is never shed), the name pool at most once per tier. Past that point every span of the
    /// kind keeps its own string, which used to happen in silence. The ingest endpoint subscribes and
    /// turns it into a rate-limited warning.
    /// </summary>
    public event Action<SpanPoolKind, int>? Saturated;

    /// <summary>How many times a pool has filled up since the process started.</summary>
    public long Saturations => Interlocked.Read(ref _saturations);

    private void OnSaturated(SpanPoolKind kind, int cap)
    {
        Interlocked.Increment(ref _saturations);
        Saturated?.Invoke(kind, cap);
    }

    /// <summary>
    /// Test seam: a name pool is about to be built — the allocation a flush start makes for the next
    /// tier. Throwing from it is that allocation failing (an <see cref="OutOfMemoryException"/> under
    /// the 512 MB stand's heap limit). Null in production.
    /// </summary>
    internal Action? _beforeNewNamePoolForTest;

    private Ameto.Core.StringInternPool NewNamePool()
    {
        _beforeNewNamePoolForTest?.Invoke();
        var pool = new Ameto.Core.StringInternPool(_maxNames);
        pool.PoolExhausted += cap => OnSaturated(SpanPoolKind.Names, cap);
        return pool;
    }

    /// <summary>
    /// A resource block's service interned for the ring — its pool index, or -1 when there is nothing
    /// to intern or the pool is full. Not counted here: the drainer counts the string it then has to
    /// build (<see cref="Service(ReadOnlySpan{byte}, out bool)"/>), once per run of spans.
    /// </summary>
    public int ServiceIndex(ReadOnlySpan<byte> serviceUtf8)
    {
        if (serviceUtf8.IsEmpty) return -1;
        return Services.Intern(serviceUtf8);
    }

    /// <summary>The service pool — bounded, never shed. Its indices are safe to carry through the ring.</summary>
    public Ameto.Core.StringInternPool Services { get; }

    /// <summary>Test hook: the name pool currently in use.</summary>
    internal Ameto.Core.StringInternPool NamesForTest => _names;

    /// <summary>Names that did not fit the pool, since the process started. Each kept its own string.</summary>
    public long UnpooledNames => Interlocked.Read(ref _unpooledNames);

    /// <summary>Services that did not fit the pool, since the process started. Each kept its own string.</summary>
    public long UnpooledServices => Interlocked.Read(ref _unpooledServices);

    /// <summary>
    /// The pool's instance of <paramref name="name"/>, or <paramref name="name"/> itself when the
    /// pool is full — <paramref name="pooled"/> says which. Allocation-free on a hit.
    /// </summary>
    public string Name(string name, out bool pooled)
    {
        if (name.Length == 0) { pooled = true; return string.Empty; }
        pooled = _names.Intern(name, out string canonical) >= 0;
        if (!pooled) Interlocked.Increment(ref _unpooledNames);
        return canonical;
    }

    /// <summary>As <see cref="Name(string, out bool)"/>, from UTF-8 — no string is built on a hit.</summary>
    public string Name(ReadOnlySpan<byte> nameUtf8, out bool pooled)
    {
        if (nameUtf8.IsEmpty) { pooled = true; return string.Empty; }
        pooled = _names.Intern(nameUtf8, out string canonical) >= 0;
        if (!pooled) Interlocked.Increment(ref _unpooledNames);
        return canonical;
    }

    /// <summary>The service pool's instance of <paramref name="service"/>, or the argument when the pool is full.</summary>
    public string Service(string service, out bool pooled)
    {
        if (service.Length == 0) { pooled = true; return string.Empty; }
        pooled = Services.Intern(service, out string canonical) >= 0;
        if (!pooled) Interlocked.Increment(ref _unpooledServices);
        return canonical;
    }

    /// <summary>As <see cref="Service(string, out bool)"/>, from UTF-8.</summary>
    public string Service(ReadOnlySpan<byte> serviceUtf8, out bool pooled)
    {
        if (serviceUtf8.IsEmpty) { pooled = true; return string.Empty; }
        pooled = Services.Intern(serviceUtf8, out string canonical) >= 0;
        if (!pooled) Interlocked.Increment(ref _unpooledServices);
        return canonical;
    }

    /// <summary>
    /// Replaces the name pool with an empty one. The strings already handed out stay valid (they are
    /// references, never indices); the next tier interns afresh, so a high-cardinality burst lives
    /// exactly as long as its tier. The engine does this in two halves — see <see cref="CreateNamePool"/>.
    /// </summary>
    public void ShedNames() => InstallNames(CreateNamePool());

    /// <summary>
    /// Builds the empty name pool the NEXT tier will intern into, without installing it. The half of
    /// <see cref="ShedNames"/> that allocates, split off so a flush start can run it before it touches
    /// anything it would have to undo: the pool is a dictionary, a slot array, a lock and a handler,
    /// and building it after the log had opened its flush window and the tier was detached made an
    /// <see cref="OutOfMemoryException"/> there strand the tier and wedge every later flush.
    /// </summary>
    public Ameto.Core.StringInternPool CreateNamePool() => NewNamePool();

    /// <summary>
    /// Installs a pool <see cref="CreateNamePool"/> built — the half of <see cref="ShedNames"/> that
    /// cannot throw: one reference store.
    /// </summary>
    public void InstallNames(Ameto.Core.StringInternPool pool) => _names = pool;

    /// <summary>What a string the pool could not share costs the tier: the object, header and chars.</summary>
    internal static long UnpooledStringBytes(string s) => (22L + 2L * s.Length + 7) & ~7L;
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
/// THE HTTP SEMCONV KEY LISTS, IN ONE PLACE, because three places is three answers. A trace row's
/// method and path are read on THREE paths — <c>TraceStorageEngine.MergeSpanInto</c> builds the
/// trace list from a hot span, <c>TraceQLExecutor.BuildRow</c> builds a TraceQL page, and
/// <c>TraceSummarySidecar.Write</c> resolves them ONCE at flush time into
/// <c>TraceSummary.RootMethod</c>/<c>RootPath</c>, which is where every later trace-list page reads
/// them back — and each kept its own copy of the lists. They had drifted: the engine looked under
/// five path keys, the executor and the sidecar under three, so a span whose path arrived as
/// <c>url.full</c> or <c>http.url</c> showed its path in the trace list and an empty path in a
/// TraceQL row FOR THE SAME TRACE — and an empty one in the trace list too, from the moment its
/// tier flushed.
///
/// <para>Order is the semantics: the first key present with a non-null value wins, so these are
/// most-specific-first. Adding a key means adding it here, and all three readers get it.</para>
///
/// <para>The lists must stay DISJOINT — a key in both would be found at its first rank only, and
/// the second list would read it as absent. <c>TraceHotTierProbe.The_semconv_key_lists_are_disjoint</c>
/// is what holds them so.</para>
///
/// <para>THE TWO ARE WALKED END TO END, so it is their SUM that must fit
/// <see cref="SpanAttributeBlob.MaxKeyAlternatives"/> — 2 + 5 = 7 of 8, one key of headroom. An
/// eighth is free; a ninth needs that constant raised, or <c>FindValues</c> throws under the engine
/// read lock on every trace-list page. <c>TraceHotTierProbe.The_semconv_key_lists_fit_one_blob_walk</c>
/// says so at build time and names the fix.</para>
/// </summary>
internal static class HttpSemconvKeys
{
    internal static readonly string[] MethodKeys = ["http.request.method", "http.method"];
    internal static readonly string[] PathKeys   = ["url.path", "http.target", "http.route", "url.full", "http.url"];

    /// <summary>
    /// THE DICTIONARY READING OF ONE OF THOSE LISTS: the first key that is present with a
    /// non-null value, as the text a boxed <c>ToString()</c> produces. Empty when no key of the
    /// list is on the span, when every value is null, or when the record has no map at all —
    /// three cases a caller has no reason to tell apart.
    ///
    /// <para><b>It lives here because the lists do.</b> The same nine lines stood in
    /// <c>TraceStorageEngine</c>, <c>TraceSummarySidecar</c> and <c>TraceQLExecutor</c>, byte for
    /// byte, each beside its own alias of <see cref="MethodKeys"/> / <see cref="PathKeys"/>. The
    /// lists were pulled into one place because three copies had already drifted two keys apart
    /// and put a different path on the same trace in the trace list and in a TraceQL row; the
    /// RULE that reads them is the other half of that answer, and leaving it in three copies
    /// leaves the same defect one edit away — first-match becomes last-match, or <c>v is not
    /// null</c> becomes a null-or-empty test, in one reader and not the others.</para>
    ///
    /// <para>A <c>ReadOnlySpan&lt;string&gt;</c> and not <c>params string[]</c>, so the key list
    /// is passed and never built: with a params parameter the literal arguments at a call site
    /// are a fresh array per call, which on the TraceQL page was two arrays on every one of up
    /// to a thousand rows and invisible to the caller (see
    /// <c>TraceQlScanProbe.Reading_the_http_attributes_of_a_row_allocates_nothing</c>).</para>
    /// </summary>
    internal static string GetAttr(IReadOnlyDictionary<string, object?>? attrs, ReadOnlySpan<string> keys)
    {
        if (attrs is null) return string.Empty;
        for (int i = 0; i < keys.Length; i++)
            if (attrs.TryGetValue(keys[i], out var v) && v is not null)
                return v.ToString() ?? string.Empty;
        return string.Empty;
    }

    /// <summary>
    /// BOTH key lists, in one array, as UTF-8 — <see cref="MethodKeys"/> first and then
    /// <see cref="PathKeys"/>, so ranks <c>[0, MethodKeys.Length)</c> answer the method question
    /// and the rest answer the path one. Derived from the string lists at type-init, so the two
    /// spellings of one semconv list cannot drift apart, and one array so that a root span's two
    /// questions cost ONE walk of its attribute map rather than seven.
    ///
    /// <para>The two lists MUST be disjoint: a key in both would be found at its first rank only —
    /// the walk stops comparing at the first match — and the second list would read it as absent.
    /// That is checked by <c>TraceHotTierProbe.The_semconv_key_lists_are_disjoint</c> and NOT
    /// here. A throw from a static field initializer is a <see cref="TypeInitializationException"/>
    /// that kills this whole type for the life of the process — no ingest, no query, no trace
    /// list, and logs and metrics dragged down with the first request that touches tracing — over
    /// two compile-time constants that cannot change after a build. A build-time mistake belongs
    /// in a test.</para>
    /// </summary>
    private static readonly byte[][] HttpKeysUtf8 = Utf8Keys(MethodKeys, PathKeys);

    private static byte[][] Utf8Keys(string[] first, string[] second)
    {
        var utf8 = new byte[first.Length + second.Length][];
        for (int i = 0; i < first.Length;  i++) utf8[i]                = System.Text.Encoding.UTF8.GetBytes(first[i]);
        for (int i = 0; i < second.Length; i++) utf8[first.Length + i] = System.Text.Encoding.UTF8.GetBytes(second[i]);
        return utf8;
    }

    /// <summary>
    /// A ROOT SPAN'S HTTP METHOD AND PATH, READ OUT OF THE BLOB — the one way both readers of a
    /// trace row get them.
    ///
    /// <para>Reaching them through <see cref="SpanRecord.Attributes"/> makes the ask the FIRST
    /// touch of the record's blob, so the lazy decode runs right there: a <c>Dictionary</c>, a key
    /// string and a box per attribute — ~987 B against the blob's 375 B for an ordinary
    /// eight-attribute span — and MEMOISED on the record, so a hot-tier span that has been listed
    /// once stays that much heavier until its tier flushes. The trace list was rewritten off that
    /// path in this round; <c>TraceQLExecutor.BuildRow</c> was still on it, over the very same
    /// records of the very same tier, so every TraceQL page put back what the trace list had
    /// stopped putting there.</para>
    ///
    /// <para>ONE WALK, BOTH QUESTIONS: <see cref="HttpKeysUtf8"/> is the two lists end to end, so
    /// the map is read once and the method answer is picked from the leading ranks and the path
    /// answer from the trailing ones — against seven walks if each key were asked separately, or
    /// one decode plus two dictionary probes as before.</para>
    ///
    /// <para>IDENTICAL ANSWERS to the dictionary path, by construction: the same key order, the
    /// same first-key-present-with-a-non-null-value rule (msgpack nil, arrays and nested maps box
    /// to <c>null</c> on the dictionary path and are skipped here too), the same last-copy-of-a-key
    /// wins, and the same <c>ToString()</c> text for every value shape a dictionary can hold. A
    /// record built from a dictionary rather than from bytes — every test fixture, and the cold
    /// summary rows — takes the dictionary path below, unchanged.</para>
    ///
    /// <para>WHAT IT COSTS, because it is not free: the decode was memoised and this walk is not,
    /// so a second page over the same records pays it again. The trade is the round's stated
    /// order — resident memory first, and the 512 MB stand died of the live set, not of a
    /// millisecond.</para>
    /// </summary>
    internal static void Resolve(SpanRecord s, out string method, out string path)
    {
        var blob = s.AttributesBytes;
        if (blob.IsEmpty)
        {
            method = GetAttr(s.Attributes, MethodKeys);
            path   = GetAttr(s.Attributes, PathKeys);
            return;
        }

        AttrSlots slots = default;
        Span<SpanAttrValue> found = slots;
        int mask = SpanAttributeBlob.FindValues(blob, HttpKeysUtf8, found);

        method = AttrText(found, mask, 0, MethodKeys.Length);
        path   = AttrText(found, mask, MethodKeys.Length, HttpKeysUtf8.Length);
    }

    /// <summary>
    /// The first rank in <c>[lo, hi)</c> the walk found, as the text a boxed <c>ToString()</c>
    /// would have produced. Nothing found is the empty string — what the dictionary path returns
    /// for a key list none of whose keys are on the span.
    /// </summary>
    private static string AttrText(ReadOnlySpan<SpanAttrValue> found, int mask, int lo, int hi)
    {
        for (int j = lo; j < hi; j++)
        {
            if ((mask & (1 << j)) == 0) continue;
            ref readonly var v = ref found[j];
            return v.Kind switch
            {
                SpanAttrKind.Utf8String => System.Text.Encoding.UTF8.GetString(v.Utf8.Span),
                SpanAttrKind.Integer    => v.Integer.ToString(),
                SpanAttrKind.Float      => v.Float.ToString(),
                SpanAttrKind.Boolean    => v.Boolean ? bool.TrueString : bool.FalseString,
                _                       => string.Empty,   // unreachable: FindValues clears these bits
            };
        }
        return string.Empty;
    }
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
