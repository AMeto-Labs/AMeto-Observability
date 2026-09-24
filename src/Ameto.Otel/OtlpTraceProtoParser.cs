using System.Buffers;
using System.Buffers.Text;
using System.Text;
using MessagePack;
using Ameto.Tracing;

namespace Ameto.Otel;

/// <summary>
/// Parses OTLP/protobuf <c>ExportTraceServiceRequest</c> straight into
/// <see cref="SpanIngestItem"/>s — the protobuf counterpart of
/// <see cref="OtlpTraceStreamParser"/>, and the encoding every SDK exporter and the
/// collector actually send.
///
/// <para>It replaces decode-to-DOM-then-map on this route. That route cost, per span: a
/// <c>CodedInputStream</c> plus a payload copy for every nested message — ScopeSpans, Span,
/// Status, and a KeyValue AND an AnyValue per attribute, so roughly 15 parser objects and 45
/// allocations on a six-attribute span (see <see cref="ProtoReader"/>); a throwaway OTLP
/// object graph, <c>events[]</c> and <c>links[]</c> included, which
/// <see cref="OtlpTraceMapper"/> never reads; and the quiet one, a number→string→number round
/// trip, because the DOM models are shared with OTLP/JSON and type wire integers as
/// <c>string</c> — <c>Convert.ToHexString().ToLowerInvariant()</c> per trace, span and parent
/// id re-parsed character by character, <c>ReadFixed64().ToString()</c> per timestamp re-parsed
/// by <c>long.TryParse</c>, and again per int attribute.</para>
///
/// <para>Here the ids are read with <c>BinaryPrimitives.ReadUInt64BigEndian</c> straight off the
/// wire slice — there is no hex anywhere on the trace path, because unlike logs the ids are
/// <em>columns</em>, not <c>@tr</c>/<c>@sp</c> map entries — and every key and string value is a
/// span of the request buffer written once into the <c>[ThreadStatic]</c> msgpack scratch. What
/// used to be left per span — the name string, the attribute blob and the item itself — is gone
/// too since the raw sink (below).</para>
///
/// <para>Attribute order matches <see cref="OtlpTraceMapper"/> byte for byte: resource pairs
/// first, span pairs second, one <c>WriteMapHeader(resCount + spanCount)</c> in front, so a span
/// attribute wins a key collision because it is written last.
/// <c>OtlpTraceProtoParityTests</c> pins that, field by field and blob byte by blob byte.</para>
///
/// <para>Deliberate differences from the DOM oracle, all asserted rather than papered over:</para>
/// <list type="bullet">
///   <item><c>array_value</c> and <c>kvlist_value</c> attributes are ENCODED. The DOM decoder
///   modelled neither (<c>OtlpProtoDecoder.ReadAnyValue</c> handles fields 1–4 only), so both
///   collapsed to nil and protobuf clients silently lost them while JSON clients did not.</item>
///   <item>A value nesting past <see cref="MaxValueDepth"/> is refused. Without a bound a small
///   POST of nested <c>array_value</c> is a stack overflow — process death, no exception to
///   catch. The JSON parser gets this free from <c>Utf8JsonReader</c>; a hand-rolled wire reader
///   does not.</item>
///   <item>The self-ingest URL check uses the UTF-8 overload of
///   <see cref="AmetoIngestEndpoints"/>, which refuses a URL over 512 bytes rather than
///   comparing it. That is the JSON parser's behaviour; the DOM used the char overload, which
///   has no cap. A >512-byte URL naming this server's own receiver therefore stops being
///   dropped on this route, exactly as it is already kept on the JSON one.</item>
///   <item>A literal <c>0x00</c> where a tag is expected ends the message silently, where
///   <c>CodedInputStream</c> threw "invalid tag (zero)". Field 0 is not a legal field number
///   either way; this is <see cref="ProtoReader"/>'s reading, already shared with the log and
///   metric paths.</item>
/// </list>
///
/// <para><b>It streams into a raw sink</b> (<see cref="ISpanSink"/>, TI#3): each span is handed over
/// as slices of the request buffer — the name and service UTF-8 as they arrived (validated), the
/// attribute map from the per-thread scratch — and the sink copies them into the ring's arena. No
/// <see cref="SpanIngestItem"/>, no name string, no attribute array per span; the service is
/// interned once per resource block, at its first span that reaches the sink (<see cref="ISpanSink.InternService"/>). The list-returning
/// <see cref="Parse(ReadOnlySpan{byte})"/> survives for the gRPC receiver and the tests as an
/// adapter over the SAME core (<see cref="SpanItemCollector"/>), so there is one parser and every
/// parity test exercises it.</para>
///
/// <para><b>A malformed tail leaves its prefix in the ring</b> — the receiver still answers 400,
/// which OTLP defines as not retryable, exactly like the log path's streaming parsers. The sink's
/// <see cref="ISpanSink.EndBatch"/> runs in a <c>finally</c> either way: it is what gives back the
/// arena the thread held for the batch. No drainer wake-up is needed; the ring signals per span.</para>
///
/// <para><see cref="OtlpProtoDecoder.DecodeTraces"/> stays in the tree as the parity reference,
/// exactly as <c>DecodeLogs</c> did.</para>
/// </summary>
public static class OtlpTraceProtoParser
{

    // Reused across requests on the same request thread (one request per thread at a time) so a
    // batch allocates no msgpack scratch at all — the whole point of the streaming path.
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tRes;
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tSpan;
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tOut;

    // Where a name or a service whose wire bytes were NOT valid UTF-8 is re-encoded — with U+FFFD
    // for each invalid sequence, the text CodedInputStream.ReadString() produced — so the sink is
    // only ever handed valid UTF-8. Cold: conformant exporters never reach them.
    [ThreadStatic] private static byte[]? _tName;
    [ThreadStatic] private static byte[]? _tService;

    /// <summary>What a resource with no <c>service.name</c> is called, as the sink takes it.</summary>
    private static ReadOnlySpan<byte> UnknownServiceUtf8 => "unknown"u8;

    /// <summary>The block's service has not been interned yet — no span of it has reached the sink.
    /// Not -1, which is the sink's own "not pooled" answer.</summary>
    private const int ServiceNotInterned = -2;

    /// <summary>
    /// How deep an attribute value may nest before the payload is refused.
    ///
    /// <para>A value nests through <c>array_value</c> and <c>kvlist_value</c>, whose writers
    /// recurse into <see cref="WriteAnyValue"/> — so without a bound, 150 KB of
    /// <c>array_value{values{array_value{…}}}</c> is a stack overflow, which is process death
    /// with no exception to catch and no request left to answer.</para>
    ///
    /// <para>64 is the number <c>Utf8JsonReader</c> uses for its default <c>MaxDepth</c>, though
    /// the two are not counting the same thing: the JSON reader counts every object and array
    /// from the root, so it refuses somewhere near eighteen nested values where this refuses at
    /// sixty-four. Both are far past anything an exporter emits, and the point of the number is
    /// that it is small enough to keep the stack.</para>
    /// </summary>
    private const int MaxValueDepth = 64;

    /// <summary>
    /// Above this a scratch writer is dropped at the end of the batch rather than kept.
    ///
    /// <para><c>ResetWrittenCount</c> empties a writer but never shrinks it, so without a ceiling
    /// <see cref="_tSpan"/> (one span's attribute pairs) and <see cref="_tOut"/> (that span's
    /// assembled map) each keep a copy of the largest attribute blob the thread ever saw, for the
    /// life of the process. A single in-limits POST — <c>Ingestion.MaxOtlpBatchBytes</c> is 8 MiB
    /// — whose one span carries a multi-megabyte attribute map pins twice its size on the request
    /// thread, and this is the route every SDK exporter uses, so every thread-pool thread that
    /// serves one keeps its own copy. The JSON parser's escape and nesting scratch already work
    /// this way; these writers were outside every bound in the server.</para>
    /// </summary>
    private const int MaxKeptAttrScratch = 64 * 1024;

    /// <summary>
    /// Per-call state. A ref struct so it can hold spans of the caller's request buffer without
    /// the parser allocating anything per batch.
    /// </summary>
    private ref struct ParseState
    {
        public ArrayBufferWriter<byte> ResBuf;    // resource attrs (msgpack KV pairs), per resourceSpans
        public ArrayBufferWriter<byte> SpanBuf;   // span attrs (msgpack KV pairs), per span
        public ArrayBufferWriter<byte> OutBuf;    // assembled map: header + ResBuf + SpanBuf
        public ISpanSink Sink;
        public int Ingested;
        public int Refused;
        public int ResKeyCount;
        public ReadOnlySpan<byte> Service;        // one per resourceSpans block, valid UTF-8
        public int ServiceIdx;                    // what the sink interned it as, at the block's first span; ServiceNotInterned until then
        public bool ServiceSeen;
        public int Depth;                         // nested array_value / kvlist_value levels open
    }

    private enum AttrScope : byte { Resource, Span, Nested }

    /// <summary>
    /// The <c>finally</c> is the point: a truncated length prefix, a malformed varint and a value
    /// past <see cref="MaxValueDepth"/> all throw out of the middle of a span whose attribute
    /// pairs are already in the scratch, and both receivers catch that and answer
    /// 400 / INVALID_ARGUMENT before the thread goes back into the pool. A refused body must not
    /// be the one shape that keeps its scratch.
    /// </summary>
    public static List<SpanIngestItem> Parse(ReadOnlySpan<byte> payload)
    {
        var items = new SpanItemCollector();
        Parse(payload, items);
        return items.Items;
    }

    /// <summary>
    /// Streams the batch into <paramref name="sink"/> and says how many spans it took and how many
    /// it refused (back-pressure). A malformed body throws, with its prefix already ingested; the
    /// sink's batch is ended either way.
    /// </summary>
    public static (int Ingested, int Refused) Parse(ReadOnlySpan<byte> payload, ISpanSink sink)
    {
        try { return ParseBatch(payload, sink); }
        finally
        {
            sink.EndBatch();
            ReleaseScratch();
        }
    }

    /// <summary>
    /// Drops the scratch writers that grew past <see cref="MaxKeptAttrScratch"/>, so the next
    /// request on this thread starts from the small defaults again.
    ///
    /// <para>Once per batch, not per span: an ordinary span never trips the ceiling and a batch
    /// of them must keep the buffer it has grown to. Safe because everything that leaves the
    /// parser has been copied by then — the assembled map leaves as <c>WrittenSpan.ToArray()</c>
    /// and <c>MessagePackWriter.WriteRaw</c> copies the pairs into it.</para>
    /// </summary>
    private static void ReleaseScratch()
    {
        if (_tRes  is { Capacity: > MaxKeptAttrScratch }) _tRes  = null;
        if (_tSpan is { Capacity: > MaxKeptAttrScratch }) _tSpan = null;
        if (_tOut  is { Capacity: > MaxKeptAttrScratch }) _tOut  = null;
        if (_tName    is { Length: > MaxKeptAttrScratch }) _tName    = null;
        if (_tService is { Length: > MaxKeptAttrScratch }) _tService = null;
    }

    private static (int Ingested, int Refused) ParseBatch(ReadOnlySpan<byte> payload, ISpanSink sink)
    {
        var st = new ParseState
        {
            ResBuf     = _tRes  ??= new ArrayBufferWriter<byte>(4096),
            SpanBuf    = _tSpan ??= new ArrayBufferWriter<byte>(8192),
            OutBuf     = _tOut  ??= new ArrayBufferWriter<byte>(8192),
            Sink       = sink,
            Service    = UnknownServiceUtf8,
            ServiceIdx = ServiceNotInterned,
        };
        // ResetWrittenCount, not Clear: Clear zeroes every byte written last time, and nothing
        // reads past WrittenSpan.
        st.ResBuf.ResetWrittenCount();
        st.SpanBuf.ResetWrittenCount();
        st.OutBuf.ResetWrittenCount();

        var r = new ProtoReader(payload);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag == 10) ReadResourceSpans(r.ReadLengthDelimited(), ref st);   // field 1
            else r.SkipField(tag);
        }
        return (st.Ingested, st.Refused);
    }

    /// <summary>
    /// Resource first, then the spans: the wire format does not guarantee field order, and the
    /// resource attributes are prepended to every span's attribute map below.
    /// </summary>
    private static void ReadResourceSpans(ReadOnlySpan<byte> bytes, ref ParseState st)
    {
        st.ResBuf.ResetWrittenCount();
        st.ResKeyCount = 0;
        st.Service     = UnknownServiceUtf8;
        st.ServiceSeen = false;

        var pass1 = new ProtoReader(bytes);
        uint tag;
        while ((tag = pass1.ReadTag()) != 0)
        {
            if (tag == 10) ReadResource(pass1.ReadLengthDelimited(), ref st);    // field 1
            else pass1.SkipField(tag);
        }

        // ONCE PER BLOCK, not once per span: the service is a property of the resource, and every
        // span under it is handed one index with the same bytes. LAZILY, at the block's first span
        // that reaches the sink (see ReadSpan), as the JSON parser does: the service pool holds
        // 4 096 entries for the life of the process, and an exporter that sends per-pod resource
        // blocks with no spans in them used to take permanent slots over protobuf, never over JSON.
        st.ServiceIdx = ServiceNotInterned;

        var pass2 = new ProtoReader(bytes);
        while ((tag = pass2.ReadTag()) != 0)
        {
            if (tag == 18) ReadScopeSpans(pass2.ReadLengthDelimited(), ref st);  // field 2
            else pass2.SkipField(tag);
        }
    }

    private static void ReadResource(ReadOnlySpan<byte> bytes, ref ParseState st)
    {
        SpanPromotion none = default;                 // resource attributes promote nothing
        var w = new MessagePackWriter(st.ResBuf);
        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag == 10)                                                       // field 1: attributes
            {
                if (TryWriteKeyValue(r.ReadLengthDelimited(), ref w, ref st, ref none, AttrScope.Resource))
                    st.ResKeyCount++;
            }
            else r.SkipField(tag);
        }
        w.Flush();
    }

    private static void ReadScopeSpans(ReadOnlySpan<byte> bytes, ref ParseState st)
    {
        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag == 18) ReadSpan(r.ReadLengthDelimited(), ref st);            // field 2: spans
            else r.SkipField(tag);                                               // 1 scope, 3 schema_url
        }
    }

    // ── one Span → one SpanIngestItem ──────────────────────────────────────────

    private static void ReadSpan(ReadOnlySpan<byte> bytes, ref ParseState st)
    {
        ReadOnlySpan<byte> traceId = default, spanId = default, parentId = default, name = default;
        int   kind = 0, statusCode = 0;
        ulong startRaw = 0, endRaw = 0;
        SpanPromotion promo = default;

        st.SpanBuf.ResetWrittenCount();
        var w = new MessagePackWriter(st.SpanBuf);
        int spanKeyCount = 0;

        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10:  traceId  = r.ReadLengthDelimited(); break;             // 1 trace_id
                case 18:  spanId   = r.ReadLengthDelimited(); break;             // 2 span_id
                case 34:  parentId = r.ReadLengthDelimited(); break;             // 4 parent_span_id
                case 42:  name     = r.ReadLengthDelimited(); break;             // 5 name
                case 48:  kind     = (int)r.ReadVarint();     break;             // 6 kind
                case 57:  startRaw = r.ReadFixed64();         break;             // 7 start_time_unix_nano
                case 65:  endRaw   = r.ReadFixed64();         break;             // 8 end_time_unix_nano
                case 74:                                                         // 9 attributes
                    if (TryWriteKeyValue(r.ReadLengthDelimited(), ref w, ref st, ref promo, AttrScope.Span))
                        spanKeyCount++;
                    break;
                case 122: statusCode = ReadStatusCode(r.ReadLengthDelimited()); break;   // 15 status
                // 3 trace_state, 10/12/14 dropped counts, 11 events, 13 links, 16 flags: the DOM
                // materialised events[] and links[] into objects with their own attribute lists
                // and OtlpTraceMapper never looked at one of them.
                default:  r.SkipField(tag); break;
            }
        }
        w.Flush();

        // The mapper's drop rules. A 16-byte trace id is 32 hex characters and an 8-byte span id
        // is 16, which is exactly what TryParseHex demanded; any other length failed there.
        if (traceId.Length != 16 || spanId.Length != 8) return;
        // kind is compared RAW, before the 3-bit mask, because the mapper compared span.Kind.
        if (kind == 3 /* CLIENT */ && promo.AmetoInternal) return;

        int total = st.ResKeyCount + spanKeyCount;
        ReadOnlySpan<byte> attrBytes = default;
        if (total > 0)
        {
            st.OutBuf.ResetWrittenCount();
            var ow = new MessagePackWriter(st.OutBuf);
            ow.WriteMapHeader(total);
            if (st.ResKeyCount > 0) ow.WriteRaw(st.ResBuf.WrittenSpan);
            if (spanKeyCount  > 0) ow.WriteRaw(st.SpanBuf.WrittenSpan);
            ow.Flush();
            attrBytes = st.OutBuf.WrittenSpan;   // the sink copies it before the next span reuses the buffer
        }

        // A fixed64 past long.MaxValue is not a timestamp: the mapper stringified it and
        // long.TryParse refused it, landing 0. Duration then falls out of end > start.
        long startNano = startRaw <= long.MaxValue ? (long)startRaw : 0;
        long endNano   = endRaw   <= long.MaxValue ? (long)endRaw   : 0;

        // Interned at the block's FIRST span, once: every later span of the block reuses the index.
        if (st.ServiceIdx == ServiceNotInterned) st.ServiceIdx = st.Sink.InternService(st.Service);

        bool taken = st.Sink.TryIngestRaw(
            TraceId.Parse(traceId),
            SpanId.Parse(spanId),
            parentId.Length == 8 ? SpanId.Parse(parentId) : default,
            startNano,
            endNano > startNano ? endNano - startNano : 0,
            ValidUtf8(name, ref _tName),
            st.ServiceIdx,
            st.Service,
            (SpanKind)(kind & 0x07),
            statusCode switch
            {
                1 => SpanStatusCode.Ok,
                2 => SpanStatusCode.Error,
                _ => SpanStatusCode.Unset,
            },
            promo.HttpStatus,
            attrBytes);

        if (taken) st.Ingested++;
        else       st.Refused++;
    }

    /// <summary>
    /// The text a protobuf <c>string</c> field carried, as VALID UTF-8: the wire bytes themselves
    /// when they are valid (every conformant exporter — one vectorised check), else the bytes of
    /// what <c>Encoding.UTF8.GetString</c> made of them (U+FFFD per invalid sequence, the text the
    /// DOM path stored), re-encoded into <paramref name="scratch"/>.
    /// </summary>
    private static ReadOnlySpan<byte> ValidUtf8(ReadOnlySpan<byte> wire, scoped ref byte[]? scratch)
    {
        if (System.Text.Unicode.Utf8.IsValid(wire)) return wire;
        return Sanitize(wire, ref scratch);
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static ReadOnlySpan<byte> Sanitize(ReadOnlySpan<byte> wire, scoped ref byte[]? scratch)
    {
        string text = Encoding.UTF8.GetString(wire);
        int    max  = Encoding.UTF8.GetMaxByteCount(text.Length);
        if (scratch is null || scratch.Length < max) scratch = new byte[Math.Max(256, max)];
        int n = Encoding.UTF8.GetBytes(text, scratch);
        return scratch.AsSpan(0, n);
    }

    /// <summary>Status.code (field 3); status.message (field 2) is never read by the mapper.</summary>
    private static int ReadStatusCode(ReadOnlySpan<byte> bytes)
    {
        int code = 0;
        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag == 24) code = (int)r.ReadVarint();
            else r.SkipField(tag);
        }
        return code;
    }

    // ── KeyValue / AnyValue ────────────────────────────────────────────────────

    /// <summary>
    /// Writes one KeyValue as a msgpack key + value pair. Returns false — writing nothing — for
    /// a message with no key (the mapper's <c>kv.Key is not null</c> test, and the reason the map
    /// header is counted the same way) and for a resource <c>service.name</c>, which is the
    /// dedicated service column and is kept OUT of the pairs on the trace path. That is the
    /// opposite of the logs path, where service.name stays in the property map.
    ///
    /// <para>Two passes over the (tiny) KeyValue rather than one, because the wire format allows
    /// the value to precede the key and the msgpack pair cannot.</para>
    /// </summary>
    private static bool TryWriteKeyValue(
        ReadOnlySpan<byte> bytes, ref MessagePackWriter w, ref ParseState st,
        ref SpanPromotion promo, AttrScope scope)
    {
        ReadOnlySpan<byte> key = default, value = default;
        bool haveKey = false, haveValue = false;

        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: key   = r.ReadLengthDelimited(); haveKey   = true; break;   // field 1: key
                case 18: value = r.ReadLengthDelimited(); haveValue = true; break;   // field 2: value
                default: r.SkipField(tag); break;
            }
        }
        if (!haveKey) return false;

        if (scope == AttrScope.Resource && key.SequenceEqual("service.name"u8))
        {
            // ExtractServiceName returns the first service.name WHOSE VALUE IS A STRING — it
            // skips a non-string one and keeps looking, where the logs path stops at the first
            // key whatever it carries. Every service.name entry is excluded from the pairs,
            // string-valued or not, because SerializeResourcePairs excludes on the key alone.
            if (!st.ServiceSeen && haveValue && TryStringValue(value, out var sv))
            {
                st.ServiceSeen = true;
                st.Service     = ValidUtf8(sv, ref _tService);
            }
            return false;
        }

        var kind = scope == AttrScope.Span ? SpanPromotion.KindOf(key) : SpanKeyKind.Plain;

        WriteUtf8(ref w, key);
        if (haveValue) WriteAnyValue(value, ref w, ref st, ref promo, kind);
        else w.WriteNil();
        return true;
    }

    /// <summary>
    /// The <c>string_value</c> of an AnyValue: true when field 1 was present, which is the
    /// difference between "not a string" and "the empty string" — and
    /// <c>ExtractServiceName</c> accepts the empty string as a service name.
    /// </summary>
    private static bool TryStringValue(ReadOnlySpan<byte> bytes, out ReadOnlySpan<byte> value)
    {
        value = default;
        bool found = false;
        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag == 10) { value = r.ReadLengthDelimited(); found = true; }
            else r.SkipField(tag);
        }
        return found;
    }

    /// <summary>
    /// One AnyValue → one msgpack value. The oneof is scanned first and written afterwards so
    /// that a message with more than one case set resolves in the mapper's order (string, bool,
    /// double, int, array, kvlist) rather than in wire order.
    /// </summary>
    private static void WriteAnyValue(
        ReadOnlySpan<byte> bytes, ref MessagePackWriter w, ref ParseState st,
        ref SpanPromotion promo, SpanKeyKind kind)
    {
        ReadOnlySpan<byte> str = default, array = default, kvlist = default;
        long   intVal  = 0;
        double dblVal  = 0;
        bool   boolVal = false;
        bool haveStr = false, haveBool = false, haveDbl = false, haveInt = false,
             haveArr = false, haveKvl = false;

        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: str     = r.ReadLengthDelimited();  haveStr  = true; break;  // 1 string_value
                case 16: boolVal = r.ReadVarint() != 0;      haveBool = true; break;  // 2 bool_value
                case 24: intVal  = (long)r.ReadVarint();     haveInt  = true; break;  // 3 int_value
                case 33: dblVal  = r.ReadDouble();           haveDbl  = true; break;  // 4 double_value
                case 42: array   = r.ReadLengthDelimited();  haveArr  = true; break;  // 5 array_value
                case 50: kvlist  = r.ReadLengthDelimited();  haveKvl  = true; break;  // 6 kvlist_value
                default: r.SkipField(tag); break;                                     // 7 bytes_value → nil
            }
        }

        // Promotion reads what the mapper read: StringValue ?? IntValue, and neither a bool nor
        // a double ever promoted.
        if (kind != SpanKeyKind.Plain)
        {
            if (haveStr) promo.Capture(kind, str);
            else if (haveInt) promo.Capture(kind, intVal);
        }

        if (haveStr)       WriteUtf8(ref w, str);
        else if (haveBool) w.Write(boolVal);
        else if (haveDbl)  w.Write(dblVal);
        else if (haveInt)  w.Write(intVal);
        else if (haveArr)  WriteArrayValue(array, ref w, ref st, ref promo);
        else if (haveKvl)  WriteKvlistValue(kvlist, ref w, ref st, ref promo);
        else               w.WriteNil();
    }

    /// <summary>
    /// ArrayValue → msgpack array. msgpack needs the element count before the elements, so the
    /// values are counted in one pass and written in a second — a re-walk of a span already in
    /// L1, rather than the JSON parser's buffer-then-splice, which needs scratch because a
    /// <c>Utf8JsonReader</c> cannot be rewound.
    /// </summary>
    private static void WriteArrayValue(
        ReadOnlySpan<byte> bytes, ref MessagePackWriter w, ref ParseState st, ref SpanPromotion promo)
    {
        EnterValue(ref st);

        int n = 0;
        var count = new ProtoReader(bytes);
        uint tag;
        while ((tag = count.ReadTag()) != 0)
        {
            if (tag == 10) { count.ReadLengthDelimited(); n++; }                  // field 1: values
            else count.SkipField(tag);
        }

        w.WriteArrayHeader(n);
        var r = new ProtoReader(bytes);
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag == 10) WriteAnyValue(r.ReadLengthDelimited(), ref w, ref st, ref promo, SpanKeyKind.Plain);
            else r.SkipField(tag);
        }

        st.Depth--;
    }

    /// <summary>KvlistValue → msgpack map, counting only the entries that carry a key.</summary>
    private static void WriteKvlistValue(
        ReadOnlySpan<byte> bytes, ref MessagePackWriter w, ref ParseState st, ref SpanPromotion promo)
    {
        EnterValue(ref st);

        int n = 0;
        var count = new ProtoReader(bytes);
        uint tag;
        while ((tag = count.ReadTag()) != 0)
        {
            if (tag == 10) { if (HasKey(count.ReadLengthDelimited())) n++; }       // field 1: values
            else count.SkipField(tag);
        }

        w.WriteMapHeader(n);
        var r = new ProtoReader(bytes);
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag == 10) TryWriteKeyValue(r.ReadLengthDelimited(), ref w, ref st, ref promo, AttrScope.Nested);
            else r.SkipField(tag);
        }

        st.Depth--;
    }

    /// <summary>
    /// Opens one nesting level, refusing the payload past <see cref="MaxValueDepth"/>.
    ///
    /// <para>Throwing is the point: both receivers catch the wire reader's
    /// <see cref="InvalidDataException"/> already and answer 400 / INVALID_ARGUMENT, so a hostile
    /// value is refused through the same door as a truncated one. Returning quietly would write
    /// a truncated attribute map and call it success.</para>
    /// </summary>
    private static void EnterValue(ref ParseState st)
    {
        if (++st.Depth > MaxValueDepth)
            throw new InvalidDataException(
                $"OTLP attribute value nests deeper than {MaxValueDepth} levels");
    }

    private static bool HasKey(ReadOnlySpan<byte> keyValue)
    {
        var r = new ProtoReader(keyValue);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag == 10) return true;
            r.SkipField(tag);
        }
        return false;
    }

    // ── Small helpers ──────────────────────────────────────────────────────────

    /// <summary>
    /// Writes a protobuf <c>string</c> field as a msgpack str holding the text the DOM path stored.
    ///
    /// <para>Protobuf does not guarantee valid UTF-8 on the wire, and an exporter sending Latin-1
    /// or cp1251 text sends invalid sequences. <c>CodedInputStream.ReadString()</c> decoded
    /// through <see cref="Encoding.UTF8"/>, which replaces each invalid sequence with U+FFFD, and
    /// that is what reached storage. Copied verbatim, the bytes would be stored as invalid UTF-8,
    /// and TraceQL's attribute predicates compare raw bytes.</para>
    ///
    /// <para>Valid input, which is every conformant exporter, costs one vectorised check and the
    /// same raw copy as before.</para>
    /// </summary>
    private static void WriteUtf8(ref MessagePackWriter w, ReadOnlySpan<byte> utf8)
    {
        if (System.Text.Unicode.Utf8.IsValid(utf8)) w.WriteString(utf8);
        else WriteReplacingInvalid(ref w, utf8);
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static void WriteReplacingInvalid(ref MessagePackWriter w, ReadOnlySpan<byte> utf8)
    {
        // Decoding never yields more chars than there are bytes: an invalid sequence becomes one
        // U+FFFD per maximal invalid subsequence, and a valid sequence is never longer in UTF-16.
        char[] chars = ArrayPool<char>.Shared.Rent(utf8.Length);
        try
        {
            int n = Encoding.UTF8.GetChars(utf8, chars);
            w.Write(chars.AsSpan(0, n));
        }
        finally
        {
            ArrayPool<char>.Shared.Return(chars);
        }
    }
}

/// <summary>
/// A sink that builds <see cref="SpanIngestItem"/>s — the parsers' list-returning <c>Parse</c>
/// overloads, which the gRPC receiver and the tests use, over the one streaming core. It rebuilds
/// exactly what the list path always produced: the name decoded from its (valid) UTF-8, ONE service
/// string per resource block shared by the spans under it (the index <see cref="InternService"/>
/// hands out is a position in this collector's own list), and the attribute blob copied out of the
/// parser's scratch — an empty array when the span has none.
/// </summary>
internal sealed class SpanItemCollector : ISpanSink
{
    public readonly List<SpanIngestItem> Items = [];
    private readonly List<string> _services = [];

    public int InternService(ReadOnlySpan<byte> serviceUtf8)
    {
        _services.Add(serviceUtf8.IsEmpty ? string.Empty : Encoding.UTF8.GetString(serviceUtf8));
        return _services.Count - 1;
    }

    public bool TryIngestRaw(
        TraceId traceId, SpanId spanId, SpanId parentSpanId,
        long startTimeUnixNano, long durationNanos,
        ReadOnlySpan<byte> nameUtf8, int serviceIdx, ReadOnlySpan<byte> serviceUtf8,
        SpanKind kind, SpanStatusCode status, short httpStatusCode,
        ReadOnlySpan<byte> msgpackAttributes)
    {
        Items.Add(new SpanIngestItem
        {
            TraceId           = traceId,
            SpanId            = spanId,
            ParentSpanId      = parentSpanId,
            StartTimeUnixNano = startTimeUnixNano,
            DurationNanos     = durationNanos,
            Name              = nameUtf8.IsEmpty ? string.Empty : Encoding.UTF8.GetString(nameUtf8),
            ServiceName       = (uint)serviceIdx < (uint)_services.Count
                                    ? _services[serviceIdx]
                                    : (serviceUtf8.IsEmpty ? string.Empty : Encoding.UTF8.GetString(serviceUtf8)),
            Kind              = kind,
            Status            = status,
            AttributesBytes   = msgpackAttributes.IsEmpty ? [] : msgpackAttributes.ToArray(),
            HttpStatusCode    = httpStatusCode,
        });
        return true;
    }

    public void EndBatch() { }
}
