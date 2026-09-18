using System.Buffers;
using System.Buffers.Text;
using System.Text.Json;
using MessagePack;
using Ameto.Core;
using Ameto.Tracing;

namespace Ameto.Otel;

/// <summary>
/// Streaming parser for OTLP <c>ExportTraceServiceRequest</c> JSON. Walks the body with a
/// forward-only <see cref="Utf8JsonReader"/> and produces <see cref="SpanIngestItem"/>s
/// directly — no <c>OtlpSpan</c>/<c>OtlpKeyValue</c>/<c>OtlpAnyValue</c> object graph, no
/// intermediate hex/nano strings, attributes serialised to msgpack in one pass. Replaces
/// the reflection <c>JsonSerializer.Deserialize&lt;ExportTraceServiceRequest&gt;</c> +
/// <c>OtlpTraceMapper.Map</c> path for the JSON content type. The protobuf content type has
/// its own span parser, <see cref="OtlpTraceProtoParser"/>, and no longer goes through the DOM
/// either; <c>OtlpProtoDecoder.DecodeTraces</c> survives only as the parity oracle for it.
///
/// Behaviour is pinned to the DOM path by <c>OtlpTraceStreamingParityTests</c>: identical
/// items (including byte-identical attribute msgpack) for the same body, the same drop
/// rules (missing/invalid ids; outbound CLIENT spans targeting Ameto's own endpoints), and
/// the same duplicate-<c>service.name</c> rule as the protobuf parser (first string wins).
///
/// Assumes standard OTLP document order (<c>resource</c> precedes <c>scopeSpans</c>;
/// a KeyValue's <c>key</c> precedes its <c>value</c>) — true for conformant exporters.
/// Out-of-order input degrades gracefully (missing service name), never corrupts.
/// </summary>
public static class OtlpTraceStreamParser
{
    // Thread-reused scratch: span attribute pairs, resource attribute pairs, and
    // the assembled header+pairs blob.
    //
    // All three are reset with ResetWrittenCount rather than Clear. Clear ZEROES everything the
    // last span wrote before the next one starts — about 750 B of memset per span between the
    // three of them — and nothing here ever reads past WrittenSpan.
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tAttr;
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tRes;
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tOut;

    /// <summary>
    /// One scratch writer per open nesting level, reused across spans and requests.
    ///
    /// <para>msgpack needs an array's or a map's element count before its elements, and a
    /// <c>Utf8JsonReader</c> cannot be rewound to count them first — so a nested value is
    /// buffered and spliced. That buffer used to be <c>new ArrayBufferWriter&lt;byte&gt;(256)</c>,
    /// allocated fresh FOR EVERY nested value: two objects and a 256-byte array per array or
    /// kvlist attribute, on every span that carries one. Indexing by depth is what makes reuse
    /// safe — a level's buffer is fully spliced into its parent before the next sibling opens,
    /// and a child is always one level deeper.</para>
    ///
    /// <para>Depth is bounded by <c>Utf8JsonReader</c>'s own MaxDepth of 64, which each attribute
    /// level costs two or three of, so the array settles at a couple of dozen entries at worst.
    /// It still grows on demand rather than assuming that.</para>
    ///
    /// <para>Entries are dropped again past <see cref="MaxKeptNestScratch"/> — see
    /// <see cref="ReleaseNestBuffer"/>.</para>
    /// </summary>
    [ThreadStatic] private static ArrayBufferWriter<byte>?[]? _tNest;

    /// <summary>
    /// Per-thread scratch for unescaping a JSON string, in place of an
    /// <see cref="ArrayPool{T}"/> rent and return per escaped key or value.
    /// </summary>
    [ThreadStatic] private static byte[]? _tEsc;

    /// <summary>
    /// Above this, the unescape scratch is rented rather than kept: one pathological attribute
    /// must not pin a large array to a request thread for the life of the process.
    /// </summary>
    private const int MaxKeptEscapeScratch = 64 * 1024;

    /// <summary>
    /// The same rule for a nesting level's scratch, and it matters more here: every OPEN level
    /// buffers the whole subtree below it before splicing, so one in-limits POST carrying a
    /// multi-megabyte nested attribute would pin a copy of it at EVERY open level — arrays that
    /// <see cref="ArrayBufferWriter{T}.ResetWrittenCount"/> empties but never shrinks, on every
    /// thread-pool thread that ever served such a request.
    /// </summary>
    private const int MaxKeptNestScratch = 64 * 1024;

    /// <summary>The scratch writer for one nesting level, emptied and ready to write.</summary>
    private static ArrayBufferWriter<byte> NestBuffer(int depth)
    {
        var pool = _tNest ??= new ArrayBufferWriter<byte>?[8];
        if (depth >= pool.Length)
        {
            Array.Resize(ref pool, Math.Max(depth + 1, pool.Length * 2));
            _tNest = pool;
        }
        var buf = pool[depth] ??= new ArrayBufferWriter<byte>(256);
        buf.ResetWrittenCount();
        return buf;
    }

    /// <summary>
    /// Releases a nesting level's scratch once its value has been spliced into the parent,
    /// dropping it if it has grown past <see cref="MaxKeptNestScratch"/> so the next value at
    /// that level starts from 256 bytes again.
    ///
    /// <para>Safe to drop here because <c>MessagePackWriter.WriteRaw</c> COPIES: by the time
    /// this runs the bytes are already in the parent's buffer, and the level is closed.</para>
    /// </summary>
    private static void ReleaseNestBuffer(int depth)
    {
        var pool = _tNest;
        if (pool is null || (uint)depth >= (uint)pool.Length) return;
        if (pool[depth] is { } buf && buf.Capacity > MaxKeptNestScratch) pool[depth] = null;
    }

    /// <summary>Scratch of at least <paramref name="needed"/> bytes, kept per thread while it is small.</summary>
    private static byte[] EscapeScratch(int needed)
    {
        var buf = _tEsc;
        if (buf is not null && buf.Length >= needed) return buf;

        // Rounded up so a batch of growing values reallocates a handful of times, not once each.
        int size = Math.Max(256, (int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)needed));
        buf = new byte[size];
        if (size <= MaxKeptEscapeScratch) _tEsc = buf;
        return buf;
    }

    public static List<SpanIngestItem> Parse(ReadOnlySpan<byte> json)
    {
        var reader  = new Utf8JsonReader(json, isFinalBlock: true, state: default);
        var result  = new List<SpanIngestItem>();
        var attrBuf = _tAttr ??= new ArrayBufferWriter<byte>(4096);
        var resBuf  = _tRes  ??= new ArrayBufferWriter<byte>(1024);
        var outBuf  = _tOut  ??= new ArrayBufferWriter<byte>(4096);

        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            return result;

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType == JsonTokenType.PropertyName &&
                reader.ValueTextEquals("resourceSpans"u8) &&
                reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    ParseResourceSpans(ref reader, attrBuf, resBuf, outBuf, result);
            }
            else
            {
                reader.Skip();
            }
        }
        return result;
    }

    // ── resourceSpans[] element ────────────────────────────────────────────────
    private static void ParseResourceSpans(
        ref Utf8JsonReader reader,
        ArrayBufferWriter<byte> attrBuf, ArrayBufferWriter<byte> resBuf, ArrayBufferWriter<byte> outBuf,
        List<SpanIngestItem> result)
    {
        if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); return; }

        // One string per resource batch, shared by every span under it (same as the DOM path).
        // Resource attributes (env, deployment id, …) are likewise serialised once as msgpack
        // pairs and spliced into every span's attribute map.
        string serviceName = "unknown";
        int    resCount    = 0;
        resBuf.ResetWrittenCount();

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }

            if (reader.ValueTextEquals("resource"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                    serviceName = ReadResourceAttributes(ref reader, resBuf, ref resCount) ?? serviceName;
                else reader.Skip();
            }
            else if (reader.ValueTextEquals("scopeSpans"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartArray)
                {
                    while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                        ParseScopeSpans(ref reader, serviceName, attrBuf, resBuf, resCount, outBuf, result);
                }
                else reader.Skip();
            }
            else
            {
                reader.Skip();
            }
        }
    }

    /// <summary>
    /// Walks resource.attributes: captures <c>service.name</c> (returned; kept out of the
    /// pairs — it is the dedicated service column) and writes every other attribute into
    /// <paramref name="resBuf"/> as msgpack key + value pairs, counting them in
    /// <paramref name="resCount"/>.
    /// </summary>
    private static string? ReadResourceAttributes(
        ref Utf8JsonReader reader, ArrayBufferWriter<byte> resBuf, ref int resCount)
    {
        string? service = null;
        var w = new MessagePackWriter(resBuf);
        // Promotion capture is span-level only — dummies for the shared AnyValue writer.
        short dummyStatus = 0; bool dummyNew = false, dummyInternal = false;

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (reader.ValueTextEquals("attributes"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); continue; }
                    bool isService = false, wroteKey = false, wroteValue = false;
                    // Held per ENTRY, folded into `service` with ??= once the entry closes: the
                    // first STRING-valued service.name in the list wins, as OtlpTraceMapper's
                    // ExtractServiceName and OtlpTraceProtoParser do. Within one entry the last
                    // stringValue still wins, because that is a duplicate JSON property and
                    // JsonSerializer — the oracle — overwrites on those.
                    string? entryService = null;
                    while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
                    {
                        if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
                        if (reader.ValueTextEquals("key"u8))
                        {
                            reader.Read();
                            if (reader.TokenType != JsonTokenType.String) continue;
                            if (reader.ValueTextEquals("service.name"u8))
                            {
                                isService = true;
                            }
                            else
                            {
                                WriteJsonStringToMsgpack(ref reader, ref w);
                                wroteKey = true;
                            }
                        }
                        else if (reader.ValueTextEquals("value"u8))
                        {
                            if (isService)
                            {
                                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                                {
                                    while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
                                    {
                                        if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
                                        if (reader.ValueTextEquals("stringValue"u8))
                                        {
                                            reader.Read();
                                            if (reader.TokenType == JsonTokenType.String)
                                                entryService = reader.GetString();
                                        }
                                        else reader.Skip();
                                    }
                                }
                                else reader.Skip();
                            }
                            else if (wroteKey)
                            {
                                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                                    WriteAnyValue(ref reader, ref w, KeyKind.Plain,
                                        ref dummyStatus, ref dummyNew, ref dummyInternal);
                                else
                                    w.WriteNil();
                                wroteValue = true;
                            }
                            else reader.Skip();
                        }
                        else reader.Skip();
                    }
                    if (wroteKey && !wroteValue) w.WriteNil();
                    if (wroteKey) resCount++;
                    if (isService) service ??= entryService;
                }
            }
            else reader.Skip();
        }
        w.Flush();
        return service;
    }

    // ── scopeSpans[] element ───────────────────────────────────────────────────
    private static void ParseScopeSpans(
        ref Utf8JsonReader reader, string serviceName,
        ArrayBufferWriter<byte> attrBuf, ArrayBufferWriter<byte> resBuf, int resCount,
        ArrayBufferWriter<byte> outBuf,
        List<SpanIngestItem> result)
    {
        if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); return; }

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (reader.ValueTextEquals("spans"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    ParseSpan(ref reader, serviceName, attrBuf, resBuf, resCount, outBuf, result);
            }
            else reader.Skip();
        }
    }

    // ── one span → one SpanIngestItem ──────────────────────────────────────────
    private static void ParseSpan(
        ref Utf8JsonReader reader, string serviceName,
        ArrayBufferWriter<byte> attrBuf, ArrayBufferWriter<byte> resBuf, int resCount,
        ArrayBufferWriter<byte> outBuf,
        List<SpanIngestItem> result)
    {
        if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); return; }

        Span<byte> trHex = stackalloc byte[32];
        Span<byte> spHex = stackalloc byte[16];
        Span<byte> paHex = stackalloc byte[16];
        int trLen = 0, spLen = 0, paLen = 0;

        string? name       = null;
        int     kind       = 0;
        int     statusCode = 0;
        long    startNano  = 0, endNano = 0;

        attrBuf.ResetWrittenCount();
        var w = new MessagePackWriter(attrBuf);
        int   attrCount     = 0;
        short httpStatus    = 0;
        bool  httpFromNew   = false;
        bool  ametoInternal = false;

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }

            if      (reader.ValueTextEquals("traceId"u8))           { reader.Read(); trLen = CopyFixed(ref reader, trHex); }
            else if (reader.ValueTextEquals("spanId"u8))            { reader.Read(); spLen = CopyFixed(ref reader, spHex); }
            else if (reader.ValueTextEquals("parentSpanId"u8))      { reader.Read(); paLen = CopyFixed(ref reader, paHex); }
            else if (reader.ValueTextEquals("name"u8))              { reader.Read(); if (reader.TokenType == JsonTokenType.String) name = reader.GetString(); }
            else if (reader.ValueTextEquals("kind"u8))              { reader.Read(); reader.TryGetInt32(out kind); }
            else if (reader.ValueTextEquals("startTimeUnixNano"u8)) { reader.Read(); startNano = ReadUnixNano(ref reader); }
            else if (reader.ValueTextEquals("endTimeUnixNano"u8))   { reader.Read(); endNano   = ReadUnixNano(ref reader); }
            else if (reader.ValueTextEquals("status"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                {
                    while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
                    {
                        if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
                        if (reader.ValueTextEquals("code"u8)) { reader.Read(); reader.TryGetInt32(out statusCode); }
                        else reader.Skip();
                    }
                }
                else reader.Skip();
            }
            else if (reader.ValueTextEquals("attributes"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    if (WriteSpanKeyValue(ref reader, ref w, ref httpStatus, ref httpFromNew, ref ametoInternal))
                        attrCount++;
            }
            else
            {
                reader.Skip();
            }
        }
        w.Flush();

        // Same drop rules as the DOM mapper.
        if (trLen != 32 || !TraceIdHelper.TryParseTraceId(trHex, out ulong trHi, out ulong trLo)) return;
        if (spLen != 16 || !TraceIdHelper.TryParseSpanId(spHex, out ulong spRaw))                 return;
        if (kind == 3 /* CLIENT */ && ametoInternal)                                             return;

        SpanId parentId = default;
        if (paLen == 16 && TraceIdHelper.TryParseSpanId(paHex, out ulong paRaw))
            parentId = new SpanId(paRaw);

        // Final map = resource pairs first + span pairs after (span wins on collision).
        int    totalAttrs = attrCount + resCount;
        byte[] attrBytes;
        if (totalAttrs == 0)
        {
            attrBytes = [];
        }
        else
        {
            outBuf.ResetWrittenCount();
            var ow = new MessagePackWriter(outBuf);
            ow.WriteMapHeader(totalAttrs);
            if (resCount > 0) ow.WriteRaw(resBuf.WrittenSpan);
            ow.WriteRaw(attrBuf.WrittenSpan);
            ow.Flush();
            attrBytes = outBuf.WrittenSpan.ToArray();
        }

        result.Add(new SpanIngestItem
        {
            TraceId           = new TraceId(trHi, trLo),
            SpanId            = new SpanId(spRaw),
            ParentSpanId      = parentId,
            StartTimeUnixNano = startNano,
            DurationNanos     = endNano > startNano ? endNano - startNano : 0,
            Name              = name ?? string.Empty,
            ServiceName       = serviceName,
            Kind              = (SpanKind)(kind & 0x07),
            Status            = statusCode switch { 1 => SpanStatusCode.Ok, 2 => SpanStatusCode.Error, _ => SpanStatusCode.Unset },
            AttributesBytes   = attrBytes,
            HttpStatusCode    = httpStatus,
        });
    }

    // ── span attribute KeyValue with promotion hooks ───────────────────────────

    private enum KeyKind : byte { Plain, HttpStatusNew, HttpStatusOld, Url }

    /// <summary>
    /// Writes one <c>{ "key": …, "value": { AnyValue } }</c> pair as msgpack key + value,
    /// capturing the promoted HTTP status code and the "targets Ameto's own endpoints"
    /// URL check along the way (both old and new semconv key names, like the DOM mapper).
    /// </summary>
    private static bool WriteSpanKeyValue(
        ref Utf8JsonReader reader, ref MessagePackWriter w,
        ref short httpStatus, ref bool httpFromNew, ref bool ametoInternal, int depth = 0)
    {
        if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); return false; }

        bool wroteKey = false, wroteValue = false;
        var  keyKind  = KeyKind.Plain;

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }

            if (reader.ValueTextEquals("key"u8))
            {
                reader.Read();
                if (reader.TokenType == JsonTokenType.String)
                {
                    keyKind =
                        reader.ValueTextEquals("http.response.status_code"u8) ? KeyKind.HttpStatusNew :
                        reader.ValueTextEquals("http.status_code"u8)          ? KeyKind.HttpStatusOld :
                        reader.ValueTextEquals("url.full"u8)   ||
                        reader.ValueTextEquals("url.path"u8)   ||
                        reader.ValueTextEquals("http.url"u8)   ||
                        reader.ValueTextEquals("http.target"u8)               ? KeyKind.Url
                                                                              : KeyKind.Plain;
                    WriteJsonStringToMsgpack(ref reader, ref w);
                    wroteKey = true;
                }
                else reader.Skip();
            }
            else if (reader.ValueTextEquals("value"u8))
            {
                if (!wroteKey) { reader.Skip(); continue; } // value before key (non-standard) — skip
                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                    WriteAnyValue(ref reader, ref w, keyKind, ref httpStatus, ref httpFromNew, ref ametoInternal, depth);
                else
                    w.WriteNil();
                wroteValue = true;
            }
            else reader.Skip();
        }

        if (wroteKey && !wroteValue) w.WriteNil();
        return wroteKey;
    }

    // ── AnyValue → msgpack (with promotion capture) ────────────────────────────
    private static void WriteAnyValue(
        ref Utf8JsonReader reader, ref MessagePackWriter w,
        KeyKind keyKind, ref short httpStatus, ref bool httpFromNew, ref bool ametoInternal, int depth = 0)
    {
        bool wrote = false;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }

            if (reader.ValueTextEquals("stringValue"u8))
            {
                reader.Read();
                CaptureString(ref reader, keyKind, ref httpStatus, ref httpFromNew, ref ametoInternal);
                WriteJsonStringToMsgpack(ref reader, ref w);
                wrote = true;
            }
            else if (reader.ValueTextEquals("intValue"u8))
            {
                reader.Read();
                long v = 0;
                if (reader.TokenType == JsonTokenType.String) Utf8Parser.TryParse(reader.ValueSpan, out v, out _);
                else reader.TryGetInt64(out v);
                CaptureHttpStatus(keyKind, v, ref httpStatus, ref httpFromNew);
                w.Write(v); wrote = true;
            }
            else if (reader.ValueTextEquals("boolValue"u8))
            {
                reader.Read();
                w.Write(reader.TokenType == JsonTokenType.True); wrote = true;
            }
            else if (reader.ValueTextEquals("doubleValue"u8))
            {
                reader.Read();
                reader.TryGetDouble(out double d); w.Write(d); wrote = true;
            }
            else if (reader.ValueTextEquals("arrayValue"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                    WriteArrayValue(ref reader, ref w, depth);
                else reader.Skip();
                wrote = true;
            }
            else if (reader.ValueTextEquals("kvlistValue"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                    WriteKvlistValue(ref reader, ref w, depth);
                else reader.Skip();
                wrote = true;
            }
            else
            {
                reader.Skip();
            }
        }
        if (!wrote) w.WriteNil();
    }

    /// <summary>String value of a promoted key: parse the HTTP status / run the URL check.</summary>
    private static void CaptureString(
        ref Utf8JsonReader reader, KeyKind keyKind,
        ref short httpStatus, ref bool httpFromNew, ref bool ametoInternal)
    {
        if (reader.TokenType != JsonTokenType.String || keyKind == KeyKind.Plain) return;

        if (keyKind is KeyKind.HttpStatusNew or KeyKind.HttpStatusOld)
        {
            if (Utf8Parser.TryParse(reader.ValueSpan, out long v, out _))
                CaptureHttpStatus(keyKind, v, ref httpStatus, ref httpFromNew);
            return;
        }

        // Url: does the value contain one of Ameto's own ingestion endpoints?
        if (ametoInternal) return;
        if (!reader.ValueIsEscaped)
        {
            ametoInternal = ContainsAmetoEndpoint(reader.ValueSpan);
        }
        else
        {
            ametoInternal = EscapedUrlIsAmetoEndpoint(ref reader);
        }
    }

    /// <summary>
    /// The endpoint check for an escaped URL. Its buffer is a <c>stackalloc</c> — unlike the
    /// msgpack writer, <see cref="ContainsAmetoEndpoint"/> is an ordinary static that cannot
    /// keep the span — sized at the longest URL the matcher will look at at all. An escaped
    /// value longer than that can still unescape to something shorter (<c>A</c> is six
    /// bytes for one), so the long case keeps its pooled buffer rather than being skipped.
    ///
    /// <para>Separate method so the stack buffer is only in the frame of a call that takes this
    /// branch, not in every frame of a deeply nested value.</para>
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static bool EscapedUrlIsAmetoEndpoint(ref Utf8JsonReader reader)
    {
        int max = reader.ValueSpan.Length;
        if (max <= MaxMatchableUrlBytes)
        {
            Span<byte> tmp = stackalloc byte[MaxMatchableUrlBytes];
            int len = reader.CopyString(tmp);
            return ContainsAmetoEndpoint(tmp[..len]);
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(max);
        try
        {
            int n = reader.CopyString(rented);
            return ContainsAmetoEndpoint(rented.AsSpan(0, n));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// The longest URL <see cref="AmetoIngestEndpoints.Matches(ReadOnlySpan{byte})"/> compares;
    /// it refuses anything longer outright.
    /// </summary>
    private const int MaxMatchableUrlBytes = 512;

    private static void CaptureHttpStatus(KeyKind keyKind, long value, ref short httpStatus, ref bool httpFromNew)
    {
        if (keyKind is not (KeyKind.HttpStatusNew or KeyKind.HttpStatusOld)) return;
        if (value is < short.MinValue or > short.MaxValue) return;
        // The new semconv key wins over the old one, regardless of document order.
        if (keyKind == KeyKind.HttpStatusNew) { httpStatus = (short)value; httpFromNew = true; }
        else if (!httpFromNew)                { httpStatus = (short)value; }
    }

    /// <summary>
    /// One list, shared with <c>OtlpTraceMapper</c> — see <see cref="AmetoIngestEndpoints"/>.
    /// </summary>
    private static bool ContainsAmetoEndpoint(ReadOnlySpan<byte> url) =>
        AmetoIngestEndpoints.Matches(url);

    /// <summary>ASCII case-insensitive substring search (needle must be lowercase ASCII).</summary>
    private static bool ContainsAsciiIgnoreCase(ReadOnlySpan<byte> haystack, ReadOnlySpan<byte> needleLower)
    {
        if (needleLower.Length == 0 || haystack.Length < needleLower.Length) return false;
        for (int i = 0; i <= haystack.Length - needleLower.Length; i++)
        {
            int j = 0;
            for (; j < needleLower.Length; j++)
            {
                byte c = haystack[i + j];
                if (c is >= (byte)'A' and <= (byte)'Z') c += 32;
                if (c != needleLower[j]) break;
            }
            if (j == needleLower.Length) return true;
        }
        return false;
    }

    // ── Nested array / kvlist (no promotion inside) ────────────────────────────

    private static void WriteArrayValue(ref Utf8JsonReader reader, ref MessagePackWriter w, int depth)
    {
        short s = 0; bool b = false, a = false;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (reader.ValueTextEquals("values"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                var tmp = NestBuffer(depth);
                var tw  = new MessagePackWriter(tmp);
                int n = 0;
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (reader.TokenType == JsonTokenType.StartObject)
                    {
                        WriteAnyValue(ref reader, ref tw, KeyKind.Plain, ref s, ref b, ref a, depth + 1);
                        n++;
                    }
                    else reader.Skip();
                }
                tw.Flush();
                w.WriteArrayHeader(n);
                w.WriteRaw(tmp.WrittenSpan);
                ReleaseNestBuffer(depth);
            }
            else reader.Skip();
        }
    }

    private static void WriteKvlistValue(ref Utf8JsonReader reader, ref MessagePackWriter w, int depth)
    {
        short s = 0; bool b = false, a = false;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (reader.ValueTextEquals("values"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                var tmp = NestBuffer(depth);
                var tw  = new MessagePackWriter(tmp);
                int n = 0;
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    if (WriteSpanKeyValue(ref reader, ref tw, ref s, ref b, ref a, depth + 1)) n++;
                tw.Flush();
                w.WriteMapHeader(n);
                w.WriteRaw(tmp.WrittenSpan);
                ReleaseNestBuffer(depth);
            }
            else reader.Skip();
        }
    }

    // ── Small helpers ──────────────────────────────────────────────────────────

    /// <summary>
    /// Writes the current JSON string token to msgpack as a str, unescaping if needed.
    ///
    /// <para>The unescape buffer is the per-thread scratch, not a pooled rent: renting and
    /// returning per escaped key or value is two interlocked operations and a bucket walk for a
    /// buffer whose life ends four lines later. Anything past
    /// <see cref="MaxKeptEscapeScratch"/> still goes through the pool, so one enormous attribute
    /// cannot pin a large array to a request thread.</para>
    ///
    /// <para>The scratch is an array rather than a <c>stackalloc</c> because
    /// <see cref="MessagePackWriter"/> is a ref struct taken by ref, so ref-safety has to assume
    /// a span handed to it could be stored in it and refuses a stack buffer outright.</para>
    /// </summary>
    private static void WriteJsonStringToMsgpack(ref Utf8JsonReader reader, ref MessagePackWriter w)
    {
        if (reader.TokenType != JsonTokenType.String) { w.WriteNil(); return; }
        if (!reader.ValueIsEscaped) { w.WriteString(reader.ValueSpan); return; }

        // Unescaping never grows the text, so the escaped length bounds the result.
        int max = reader.ValueSpan.Length;
        if (max <= MaxKeptEscapeScratch)
        {
            byte[] scratch = EscapeScratch(max);
            int len = reader.CopyString(scratch);
            w.WriteString(scratch.AsSpan(0, len));
            return;
        }

        byte[] tmp = ArrayPool<byte>.Shared.Rent(max);
        try
        {
            int n = reader.CopyString(tmp);
            w.WriteString(tmp.AsSpan(0, n));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(tmp);
        }
    }

    /// <summary>Copies the current string token (unescaped) into a fixed span; returns length (0 if it doesn't fit).</summary>
    private static int CopyFixed(ref Utf8JsonReader reader, scoped Span<byte> dest)
    {
        if (reader.TokenType != JsonTokenType.String) return 0;
        if (reader.ValueSpan.Length > dest.Length) return 0;
        if (!reader.ValueIsEscaped)
        {
            reader.ValueSpan.CopyTo(dest);
            return reader.ValueSpan.Length;
        }
        return reader.CopyString(dest);
    }

    /// <summary>uint64 nano timestamp — OTLP encodes it as a JSON string; tolerate a raw number.</summary>
    private static long ReadUnixNano(ref Utf8JsonReader reader)
    {
        if (reader.TokenType == JsonTokenType.String)
            return Utf8Parser.TryParse(reader.ValueSpan, out long v, out _) ? v : 0;
        return reader.TryGetInt64(out long n) ? n : 0;
    }
}
