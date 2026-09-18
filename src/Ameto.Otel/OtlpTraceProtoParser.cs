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
/// is left per span is what the JSON path already pays: the name string, the attribute blob and
/// the item itself.</para>
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
/// <para>The batch is materialised as a list and handed to <c>ISpanIngester.TryIngest</c> whole,
/// exactly as the DOM path did, so a malformed tail still leaves nothing in the ring and the
/// caller answers 400. When the raw span sink lands and this becomes a streaming parser, its
/// prefix will already be in the ring on a throw — and that needs no batch-completion signal,
/// because <c>SpanRingBuffer.TryEnqueue</c> releases the drainer's semaphore per item, not per
/// batch. There is deliberately no <c>NotifyBatchEnqueued</c> equivalent here to add one to.</para>
///
/// <para><see cref="OtlpProtoDecoder.DecodeTraces"/> stays in the tree as the parity reference,
/// exactly as <c>DecodeLogs</c> did.</para>
/// </summary>
public static class OtlpTraceProtoParser
{
    /// <summary>What a resource with no <c>service.name</c> is called (<c>OtlpTraceMapper</c>).</summary>
    private const string UnknownService = "unknown";

    // Reused across requests on the same request thread (one request per thread at a time) so a
    // batch allocates no msgpack scratch at all — the whole point of the streaming path.
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tRes;
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tSpan;
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tOut;

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
    /// Per-call state. A ref struct so it can hold spans of the caller's request buffer without
    /// the parser allocating anything per batch.
    /// </summary>
    private ref struct ParseState
    {
        public ArrayBufferWriter<byte> ResBuf;    // resource attrs (msgpack KV pairs), per resourceSpans
        public ArrayBufferWriter<byte> SpanBuf;   // span attrs (msgpack KV pairs), per span
        public ArrayBufferWriter<byte> OutBuf;    // assembled map: header + ResBuf + SpanBuf
        public List<SpanIngestItem> Result;
        public int ResKeyCount;
        public string Service;                    // one string per resourceSpans block, as the mapper made
        public bool ServiceSeen;
        public int Depth;                         // nested array_value / kvlist_value levels open
    }

    /// <summary>Promotions captured from a span's own attributes (never a resource's).</summary>
    private struct SpanPromo
    {
        public short HttpStatus;
        /// <summary>
        /// Set once a <c>http.response.status_code</c> has parsed. The mapper's loop
        /// <c>break</c>s at that point, so nothing after it — old key or new — can change the
        /// answer; this latch is that <c>break</c>.
        /// </summary>
        public bool HttpLatched;
        public bool AmetoInternal;
    }

    private enum AttrScope : byte { Resource, Span, Nested }

    private enum KeyKind : byte { Plain, HttpStatusNew, HttpStatusOld, Url }

    public static List<SpanIngestItem> Parse(ReadOnlySpan<byte> payload)
    {
        var st = new ParseState
        {
            ResBuf  = _tRes  ??= new ArrayBufferWriter<byte>(4096),
            SpanBuf = _tSpan ??= new ArrayBufferWriter<byte>(8192),
            OutBuf  = _tOut  ??= new ArrayBufferWriter<byte>(8192),
            Result  = [],
            Service = UnknownService,
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
        return st.Result;
    }

    /// <summary>
    /// Resource first, then the spans: the wire format does not guarantee field order, and the
    /// resource attributes are prepended to every span's attribute map below.
    /// </summary>
    private static void ReadResourceSpans(ReadOnlySpan<byte> bytes, ref ParseState st)
    {
        st.ResBuf.ResetWrittenCount();
        st.ResKeyCount = 0;
        st.Service     = UnknownService;
        st.ServiceSeen = false;

        var pass1 = new ProtoReader(bytes);
        uint tag;
        while ((tag = pass1.ReadTag()) != 0)
        {
            if (tag == 10) ReadResource(pass1.ReadLengthDelimited(), ref st);    // field 1
            else pass1.SkipField(tag);
        }

        var pass2 = new ProtoReader(bytes);
        while ((tag = pass2.ReadTag()) != 0)
        {
            if (tag == 18) ReadScopeSpans(pass2.ReadLengthDelimited(), ref st);  // field 2
            else pass2.SkipField(tag);
        }
    }

    private static void ReadResource(ReadOnlySpan<byte> bytes, ref ParseState st)
    {
        SpanPromo none = default;                 // resource attributes promote nothing
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
        SpanPromo promo = default;

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
        byte[] attrBytes;
        if (total == 0)
        {
            attrBytes = [];
        }
        else
        {
            st.OutBuf.ResetWrittenCount();
            var ow = new MessagePackWriter(st.OutBuf);
            ow.WriteMapHeader(total);
            if (st.ResKeyCount > 0) ow.WriteRaw(st.ResBuf.WrittenSpan);
            if (spanKeyCount  > 0) ow.WriteRaw(st.SpanBuf.WrittenSpan);
            ow.Flush();
            attrBytes = st.OutBuf.WrittenSpan.ToArray();
        }

        // A fixed64 past long.MaxValue is not a timestamp: the mapper stringified it and
        // long.TryParse refused it, landing 0. Duration then falls out of end > start.
        long startNano = startRaw <= long.MaxValue ? (long)startRaw : 0;
        long endNano   = endRaw   <= long.MaxValue ? (long)endRaw   : 0;

        st.Result.Add(new SpanIngestItem
        {
            TraceId           = TraceId.Parse(traceId),
            SpanId            = SpanId.Parse(spanId),
            ParentSpanId      = parentId.Length == 8 ? SpanId.Parse(parentId) : default,
            StartTimeUnixNano = startNano,
            DurationNanos     = endNano > startNano ? endNano - startNano : 0,
            Name              = name.IsEmpty ? string.Empty : Encoding.UTF8.GetString(name),
            ServiceName       = st.Service,
            Kind              = (SpanKind)(kind & 0x07),
            Status            = statusCode switch
            {
                1 => SpanStatusCode.Ok,
                2 => SpanStatusCode.Error,
                _ => SpanStatusCode.Unset,
            },
            AttributesBytes   = attrBytes,
            HttpStatusCode    = promo.HttpStatus,
        });
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
        ref SpanPromo promo, AttrScope scope)
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
                st.Service     = Encoding.UTF8.GetString(sv);
            }
            return false;
        }

        var kind = scope == AttrScope.Span ? KindOf(key) : KeyKind.Plain;

        WriteUtf8(ref w, key);
        if (haveValue) WriteAnyValue(value, ref w, ref st, ref promo, kind);
        else w.WriteNil();
        return true;
    }

    /// <summary>The keys the mapper promotes out of a span's attributes into its own columns.</summary>
    private static KeyKind KindOf(ReadOnlySpan<byte> key) =>
        key.SequenceEqual("http.response.status_code"u8) ? KeyKind.HttpStatusNew :
        key.SequenceEqual("http.status_code"u8)          ? KeyKind.HttpStatusOld :
        key.SequenceEqual("url.full"u8) || key.SequenceEqual("url.path"u8) ||
        key.SequenceEqual("http.url"u8) || key.SequenceEqual("http.target"u8)
                                                         ? KeyKind.Url
                                                         : KeyKind.Plain;

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
        ref SpanPromo promo, KeyKind kind)
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
        if (kind != KeyKind.Plain)
        {
            if (haveStr) Promote(kind, str, ref promo);
            else if (haveInt) Promote(kind, intVal, ref promo);
        }

        if (haveStr)       WriteUtf8(ref w, str);
        else if (haveBool) w.Write(boolVal);
        else if (haveDbl)  w.Write(dblVal);
        else if (haveInt)  w.Write(intVal);
        else if (haveArr)  WriteArrayValue(array, ref w, ref st, ref promo);
        else if (haveKvl)  WriteKvlistValue(kvlist, ref w, ref st, ref promo);
        else               w.WriteNil();
    }

    private static void Promote(KeyKind kind, ReadOnlySpan<byte> text, ref SpanPromo promo)
    {
        if (kind == KeyKind.Url)
        {
            // The UTF-8 overload, as the JSON parser uses: it refuses a URL over 512 bytes
            // outright, where the char overload the DOM used compared it. Same choice, same
            // hole, one behaviour across the two streaming parsers.
            if (!promo.AmetoInternal) promo.AmetoInternal = AmetoIngestEndpoints.Matches(text);
            return;
        }
        // short.TryParse of the string the mapper held. Utf8Parser refuses what short.TryParse
        // refused — a non-numeric value, and a value past short — and additionally refuses
        // surrounding whitespace, which no exporter sends and which the JSON parser also refuses.
        if (Utf8Parser.TryParse(text, out long v, out int consumed) && consumed == text.Length)
            PromoteStatus(kind, v, ref promo);
    }

    private static void Promote(KeyKind kind, long value, ref SpanPromo promo)
    {
        if (kind != KeyKind.Url) PromoteStatus(kind, value, ref promo);
    }

    private static void PromoteStatus(KeyKind kind, long value, ref SpanPromo promo)
    {
        if (promo.HttpLatched) return;
        if (value is < short.MinValue or > short.MaxValue) return;   // short.TryParse said no
        promo.HttpStatus = (short)value;
        if (kind == KeyKind.HttpStatusNew) promo.HttpLatched = true; // the mapper's break
    }

    /// <summary>
    /// ArrayValue → msgpack array. msgpack needs the element count before the elements, so the
    /// values are counted in one pass and written in a second — a re-walk of a span already in
    /// L1, rather than the JSON parser's buffer-then-splice, which needs scratch because a
    /// <c>Utf8JsonReader</c> cannot be rewound.
    /// </summary>
    private static void WriteArrayValue(
        ReadOnlySpan<byte> bytes, ref MessagePackWriter w, ref ParseState st, ref SpanPromo promo)
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
            if (tag == 10) WriteAnyValue(r.ReadLengthDelimited(), ref w, ref st, ref promo, KeyKind.Plain);
            else r.SkipField(tag);
        }

        st.Depth--;
    }

    /// <summary>KvlistValue → msgpack map, counting only the entries that carry a key.</summary>
    private static void WriteKvlistValue(
        ReadOnlySpan<byte> bytes, ref MessagePackWriter w, ref ParseState st, ref SpanPromo promo)
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
