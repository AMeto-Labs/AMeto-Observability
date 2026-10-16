using System.Buffers;
using System.Buffers.Text;
using System.Text;
using System.Text.Json;
using System.Text.Unicode;
using MessagePack;
using Ameto.Core;
using Ameto.Ingestion;

namespace Ameto.Otel;

/// <summary>
/// Zero-alloc streaming parser for OTLP <c>ExportLogsServiceRequest</c> JSON. Walks the body
/// with a forward-only <see cref="Utf8JsonReader"/> and writes each log record (header +
/// msgpack properties) straight into a log batch via
/// <see cref="IOtlpLogSink.TryIngestRaw(long, byte, ReadOnlySpan{byte}, ReadOnlySpan{byte}, ulong, ulong, ulong, ReadOnlySpan{byte})"/> — never materialising the OTLP object graph,
/// per-record <c>LogEvent</c>s, or per-attribute strings. Replaces the reflection-based
/// <c>JsonSerializer.Deserialize&lt;ExportLogsServiceRequest&gt;</c> + <c>OtlpLogMapper.Map</c>
/// path that dominated GC pressure above ~60k events/s.
///
/// Assumes standard OTLP document order (a resourceLogs' <c>resource</c> precedes its
/// <c>scopeLogs</c>; a KeyValue's <c>key</c> precedes its <c>value</c>) — true for every
/// conformant exporter. Out-of-order input degrades gracefully (missing resource attrs),
/// it never corrupts. A nested array or kvlist value is counted on a copy of the reader and then
/// written in place, so nothing is buffered for it.
///
/// <para>The resource's <c>service.name</c> becomes the event's <c>@service</c> header and
/// leaves the property map, exactly as the mapper and the protobuf parser do it — see
/// <see cref="ServiceCapture"/>.</para>
///
/// <para><b>What a record's map would take is counted before it is written</b> (<see cref="Room"/>,
/// round-5 review H-E), as the protobuf parser counts it: exactly, and once the map is past the
/// sink's limit nothing more of it is written — the record is refused unassembled. Attribute values
/// used to be written whole into the scratch, and an escaped one first unescaped into a rent as long
/// as the value: one 7.9 MB attribute took 7.9 MB of scratch (16.3 MB with one escape) against a
/// 2.1 MB charge, and sixteen such requests held 233 MiB the in-flight bound never saw. Escaped
/// strings are unescaped straight into the map now; a template's unescaped copy, which can be as
/// long as the body, is counted against the batch before it is taken
/// (<see cref="IOtlpLogSink.ChargeScratch"/>); a service name is copied no further than the sink
/// keeps it.</para>
///
/// <para><b>Text is stored as the protobuf parser stores it</b> (round-5 L-10): a sequence that is
/// not UTF-8 — raw in the body, or a lone surrogate escape — becomes U+FFFD. Raw invalid bytes were
/// stored as they came, and an escape beside them, or a lone surrogate escape, threw out of the
/// parse part way through the batch, with its prefix already staged.</para>
/// </summary>
public static class OtlpLogStreamParser
{
    // Reused across requests on the same request thread (one request per thread at a time)
    // so a batch allocates no msgpack scratch — the whole point of the streaming path.
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tRes;
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tRec;
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tOut;

    /// <summary>Scratch a request grew past this is let go at its end rather than kept on the thread.</summary>
    private const int KeepScratchBytes = 1 << 20;

    /// <summary>
    /// A template whose JSON string carries escapes is unescaped here when it fits: long enough for a
    /// message, a path or a stack frame. A longer one takes an array of its own, counted against the
    /// batch (<see cref="TemplateScratch"/>). A template with no escape is read where it lies.
    /// </summary>
    private const int TemplateScratchBytes = 4096;

    public static (int Ingested, int Dropped) Parse(ReadOnlySpan<byte> json, IOtlpLogSink sink)
    {
        var reader = new Utf8JsonReader(json, isFinalBlock: true, state: default);

        // Thread-reused scratch, reset per resource/record.
        var resBuf = _tRes ??= new ArrayBufferWriter<byte>(4096);   // resource attrs (msgpack KV pairs)
        var recBuf = _tRec ??= new ArrayBufferWriter<byte>(8192);   // record attrs + @tr/@sp (msgpack KV pairs)
        var outBuf = _tOut ??= new ArrayBufferWriter<byte>(8192);   // assembled map: header + resBuf + recBuf
        resBuf.ResetWrittenCount(); recBuf.ResetWrittenCount(); outBuf.ResetWrittenCount();

        var map  = new MapBudget { Limit = sink.MaxEventPayloadBytes };
        // The resource's service.name, captured per resource, no further than the sink keeps it; its
        // buffer grows (rent, then return) for a long name, so the one to give back is whatever it holds
        // when the parse ends.
        var svc  = new ServiceCapture { Buf = ArrayPool<byte>.Shared.Rent(256), Max = ServiceCaptureBytes(sink.MaxServiceNameBytes) };
        var tmpl = new TemplateScratch { Small = ArrayPool<byte>.Shared.Rent(TemplateScratchBytes) };
        byte[] trBuf = ArrayPool<byte>.Shared.Rent(32);    // traceId bytes (per record)
        byte[] spBuf = ArrayPool<byte>.Shared.Rent(16);    // spanId bytes (per record)

        int ingested = 0, dropped = 0;
        try
        {
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                return (0, 0);

            while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
            {
                if (reader.TokenType == JsonTokenType.PropertyName &&
                    JsonStringText.NameIs(ref reader, "resourceLogs"u8) &&
                    reader.Read() && reader.TokenType == JsonTokenType.StartArray)
                {
                    while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                        ParseResourceLogs(ref reader, sink, resBuf, recBuf, outBuf, ref map,
                            ref svc, ref tmpl, trBuf, spBuf, ref ingested, ref dropped);
                }
                else
                {
                    reader.Skip();
                }
            }

            return (ingested, dropped);
        }
        finally
        {
            // IN A FINALLY for the reason the protobuf parser states: this parser hands records
            // over as it walks, so a malformed tail throws out of the reader with the prefix
            // already in the sink, and a sink that acts on the notification must hear it then too.
            if (ingested > 0) sink.NotifyBatchEnqueued();

            ArrayPool<byte>.Shared.Return(svc.Buf);
            ArrayPool<byte>.Shared.Return(tmpl.Small);
            // The large template scratch is the GC's; what it was counted at goes back. A negative
            // count never throws.
            if (tmpl.Charged > 0) sink.ChargeScratch(-tmpl.Charged);
            ArrayPool<byte>.Shared.Return(trBuf);
            ArrayPool<byte>.Shared.Return(spBuf);

            // Scratch a request grew past this is let go, not kept on the thread for the next one
            // (see the protobuf parser).
            if (_tRes is { Capacity: > KeepScratchBytes }) _tRes = null;
            if (_tRec is { Capacity: > KeepScratchBytes }) _tRec = null;
            if (_tOut is { Capacity: > KeepScratchBytes }) _tOut = null;
        }
    }

    // ── What the map being built would take ─────────────────────────────────────

    /// <summary>
    /// The property map being built, counted as the protobuf parser counts it (its <c>ParseState</c>):
    /// every byte the writes below would put into it, written or not.
    /// </summary>
    private struct MapBudget
    {
        /// <summary>The sink's limit on a record's property map (<see cref="IOtlpLogSink.MaxEventPayloadBytes"/>).</summary>
        public long Limit;
        /// <summary>Bytes of the map already in another buffer: the resource's, while a record's attributes are written.</summary>
        public long Base;
        /// <summary>Bytes counted into the buffer being written — all of them, written or not once <see cref="Over"/>.</summary>
        public long Bytes;
        /// <summary>The map passed <see cref="Limit"/>: nothing more is written into the buffer, only counted.</summary>
        public bool Over;
        /// <summary>The resource's attribute bytes, and whether they alone passed the limit.</summary>
        public long ResBytes;
        public bool ResOver;
    }

    /// <summary>
    /// Counts <paramref name="bytes"/> more of the property map being built; false once the map —
    /// with what is already in another buffer (<see cref="MapBudget.Base"/>) — passes the sink's
    /// limit: the caller then writes nothing more of it, and the record is refused as oversized
    /// before it is assembled. Everything is still counted, for the size the drop reports.
    /// </summary>
    private static bool Room(ref MapBudget map, long bytes)
    {
        map.Bytes += bytes;
        if (!map.Over && map.Base + map.Bytes > map.Limit) map.Over = true;
        return !map.Over;
    }

    // ── resourceLogs[] element ─────────────────────────────────────────────────
    private static void ParseResourceLogs(
        ref Utf8JsonReader reader, IOtlpLogSink sink,
        ArrayBufferWriter<byte> resBuf, ArrayBufferWriter<byte> recBuf, ArrayBufferWriter<byte> outBuf, ref MapBudget map,
        ref ServiceCapture svc, ref TemplateScratch tmpl, byte[] trBuf, byte[] spBuf,
        ref int ingested, ref int dropped)
    {
        if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); return; }

        resBuf.ResetWrittenCount();
        int resKeyCount = 0;
        svc.Len  = 0;       // >0 ⇒ this resource's service.name is in svc.Buf
        svc.Seen = false;
        // The service name is a property of THIS resource, shared by every record under it:
        // intern it once here instead of re-hashing the same bytes per record.
        int svcIdx      = -1;
        // A resource with no attributes adds nothing to its records' maps.
        map.ResBytes = 0;
        map.ResOver  = false;

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }

            if (JsonStringText.NameIs(ref reader, "resource"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                {
                    // Counted as they are written, ONE budget for every resource of the block: past the
                    // limit they stop being written, and every record under them is refused unassembled. A
                    // "resource" key given twice (not OTLP, but JSON) adds its attributes to the first's —
                    // resBuf holds both, as the protobuf parser merges a resource field given twice — so it
                    // is counted on from the first's, never from zero (round-6 review R5-M8). Counted from
                    // zero, a record was assembled past the limit, or with a map cut part way that the store
                    // answered 200 for and gave back with no properties.
                    map.Base  = 0;
                    map.Bytes = map.ResBytes;
                    map.Over  = map.ResOver;
                    ParseResourceAttributes(ref reader, resBuf, ref resKeyCount, ref svc, ref map);
                    map.ResBytes = map.Bytes;
                    map.ResOver  = map.Over;
                    svcIdx = svc.Len > 0 ? sink.InternService(svc.Buf.AsSpan(0, svc.Len)) : -1;
                }
                else
                    reader.Skip();
            }
            else if (JsonStringText.NameIs(ref reader, "scopeLogs"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartArray)
                {
                    while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                        ParseScopeLogs(ref reader, sink, resBuf, resKeyCount, svc.Buf, svc.Len, svcIdx,
                            recBuf, outBuf, ref map, ref tmpl, trBuf, spBuf, ref ingested, ref dropped);
                }
                else reader.Skip();
            }
            else
            {
                reader.Skip();
            }
        }
    }

    private static void ParseResourceAttributes(
        ref Utf8JsonReader reader, ArrayBufferWriter<byte> resBuf, ref int keyCount, ref ServiceCapture svc, ref MapBudget map)
    {
        var w = new MessagePackWriter(resBuf);
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (JsonStringText.NameIs(ref reader, "attributes"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    if (WriteKeyValue(ref reader, ref w, ref svc, captureService: true, ref map)) keyCount++;
            }
            else reader.Skip();
        }
        w.Flush();
    }

    /// <summary>
    /// The resource's <c>service.name</c>, as the mapper reads it: the FIRST attribute of that
    /// name decides (<see cref="Seen"/>), and only a non-empty <c>stringValue</c> becomes the
    /// service (<see cref="Len"/> &gt; 0). That attribute is the event's <c>@service</c> header
    /// and is NOT written to the property map — the header is the one copy. Anything else stays an
    /// ordinary property, as does every later <c>service.name</c>. (This parser used to let the
    /// LAST string one win, unlike the mapper and the protobuf parser; with the attribute now
    /// leaving the map, which one is captured decides which one is missing from it, so all three
    /// agree.)
    /// </summary>
    private struct ServiceCapture
    {
        /// <summary>Pooled; grown (rent, then return) for a long name, returned by <see cref="Parse"/>.</summary>
        public byte[] Buf;
        public int    Len;
        public bool   Seen;
        /// <summary>The most of a name copied (<see cref="ServiceCaptureBytes"/>): the sink cuts the rest.</summary>
        public int    Max;
    }

    /// <summary>
    /// How much of a <c>service.name</c> to copy for a sink that keeps <paramref name="sinkMax"/> bytes of
    /// it: four more, so its cut — at the start of the character that would be split, at most three
    /// bytes back (<c>IngestionEndpoint.ServiceName</c>) — lands where it would on the whole name. A name
    /// of the body's length used to be copied whole, into a rent as long.
    /// </summary>
    private static int ServiceCaptureBytes(int sinkMax) => sinkMax >= int.MaxValue - 4 ? int.MaxValue : sinkMax + 4;

    /// <summary>
    /// The unescaped template of a record whose body string carries escapes. A template with no escape
    /// is read where it lies in the body and needs none of this.
    /// </summary>
    private struct TemplateScratch
    {
        /// <summary>Pooled, <see cref="TemplateScratchBytes"/>: the usual escaped template.</summary>
        public byte[] Small;
        /// <summary>The GC's, grown to the longest escaped template so far; never pooled, so it does not outlive the request.</summary>
        public byte[]? Large;
        /// <summary>What <see cref="Large"/> is counted at against the batch (<see cref="IOtlpLogSink.ChargeScratch"/>).</summary>
        public long Charged;
    }

    // ── scopeLogs[] element ────────────────────────────────────────────────────
    private static void ParseScopeLogs(
        ref Utf8JsonReader reader, IOtlpLogSink sink,
        ArrayBufferWriter<byte> resBuf, int resKeyCount, byte[] svcBuf, int svcLen, int svcIdx,
        ArrayBufferWriter<byte> recBuf, ArrayBufferWriter<byte> outBuf, ref MapBudget map,
        ref TemplateScratch tmpl, byte[] trBuf, byte[] spBuf,
        ref int ingested, ref int dropped)
    {
        if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); return; }

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (JsonStringText.NameIs(ref reader, "logRecords"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    bool ok = ParseLogRecord(ref reader, sink, resBuf, resKeyCount, svcBuf, svcLen, svcIdx,
                        recBuf, outBuf, ref map, ref tmpl, trBuf, spBuf);
                    if (ok) ingested++; else dropped++;
                }
            }
            else reader.Skip();
        }
    }

    // ── one logRecord → one staged event ───────────────────────────────────────
    private static bool ParseLogRecord(
        ref Utf8JsonReader reader, IOtlpLogSink sink,
        ArrayBufferWriter<byte> resBuf, int resKeyCount, byte[] svcBuf, int svcLen, int svcIdx,
        ArrayBufferWriter<byte> recBuf, ArrayBufferWriter<byte> outBuf, ref MapBudget map,
        ref TemplateScratch tmpl, byte[] trBuf, byte[] spBuf)
    {
        if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); return false; }

        long tsTicks   = 0;
        byte level     = (byte)LogLevel.Information;
        int  sevNumber = 0;
        int  trLen     = 0;
        int  spLen     = 0;
        ReadOnlySpan<byte> template = default;

        recBuf.ResetWrittenCount();
        var w = new MessagePackWriter(recBuf);
        int recKeyCount = 0;
        map.Base  = map.ResBytes;
        map.Bytes = 0;
        map.Over  = map.ResOver;

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }

            if (JsonStringText.NameIs(ref reader, "timeUnixNano"u8))
            {
                reader.Read();
                long nanos = ReadUnixNano(ref reader);
                tsTicks = nanos > 0 ? DateTimeOffset.FromUnixTimeMilliseconds(nanos / 1_000_000).UtcTicks
                                    : DateTimeOffset.UtcNow.UtcTicks;
            }
            else if (JsonStringText.NameIs(ref reader, "severityNumber"u8))
            {
                reader.Read();
                if (reader.TokenType == JsonTokenType.Number) reader.TryGetInt32(out sevNumber);
                else reader.Skip();
            }
            else if (JsonStringText.NameIs(ref reader, "severityText"u8))
            {
                reader.Read();
                if (reader.TokenType == JsonTokenType.String) { if (sevNumber == 0) level = (byte)MapSeverityText(reader.ValueSpan); }
                else reader.Skip();
            }
            else if (JsonStringText.NameIs(ref reader, "body"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                    template = ReadBodyString(ref reader, ref tmpl, sink);
                else reader.Skip();
            }
            else if (JsonStringText.NameIs(ref reader, "traceId"u8))
            {
                reader.Read();
                trLen = CopyString(ref reader, trBuf);
            }
            else if (JsonStringText.NameIs(ref reader, "spanId"u8))
            {
                reader.Read();
                spLen = CopyString(ref reader, spBuf);
            }
            else if (JsonStringText.NameIs(ref reader, "attributes"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    if (WriteKeyValue(ref reader, ref w, ref map)) recKeyCount++;   // records never carry the service
            }
            else
            {
                reader.Skip();
            }
        }

        if (sevNumber != 0) level = (byte)MapSeverityNumber(sevNumber);

        // Append @tr / @sp into the record buffer.
        var trSpan = trLen == 32 ? trBuf.AsSpan(0, trLen) : default;
        var spSpan = spLen == 16 ? spBuf.AsSpan(0, spLen) : default;
        if (!trSpan.IsEmpty)
        {
            if (Room(ref map, 4 + OtlpLogProtoParser.StrHeaderBytes(trSpan.Length) + trSpan.Length)) { w.WriteString("@tr"u8); w.WriteString(trSpan); }
            recKeyCount++;
        }
        if (!spSpan.IsEmpty)
        {
            if (Room(ref map, 4 + OtlpLogProtoParser.StrHeaderBytes(spSpan.Length) + spSpan.Length)) { w.WriteString("@sp"u8); w.WriteString(spSpan); }
            recKeyCount++;
        }
        w.Flush();

        int total = resKeyCount + recKeyCount;
        ulong trHi = 0, trLo = 0, spanId = 0;
        if (!trSpan.IsEmpty) TraceIdHelper.TryParseTraceId(trSpan, out trHi, out trLo);
        if (!spSpan.IsEmpty) TraceIdHelper.TryParseSpanId(spSpan, out spanId);
        if (tsTicks == 0) tsTicks = DateTimeOffset.UtcNow.UtcTicks;

        // Past the sink's limit — the resource's attributes alone, or with the record's — the map is
        // not assembled: the record is refused as oversized with the size it would have had, as
        // counted (the buffers hold only what was written before it passed).
        long mapBytes = OtlpLogProtoParser.CollectionHeaderBytes(total) + map.ResBytes + map.Bytes;
        if (map.Over || mapBytes > map.Limit)
            return sink.TryIngestOversized(tsTicks, level, template, mapBytes,
                                           trHi, trLo, spanId, svcLen > 0 ? svcBuf.AsSpan(0, svcLen) : default, svcIdx);

        // Assemble the final msgpack map: header(total) + resource KV bytes + record KV bytes.
        outBuf.ResetWrittenCount();
        var ow = new MessagePackWriter(outBuf);
        ow.WriteMapHeader(total);
        if (resKeyCount > 0) ow.WriteRaw(resBuf.WrittenSpan);
        if (recKeyCount > 0) ow.WriteRaw(recBuf.WrittenSpan);
        ow.Flush();

        return sink.TryIngestRaw(
            tsTicks, level,
            template,
            outBuf.WrittenSpan,
            trHi, trLo, spanId,
            svcLen > 0 ? svcBuf.AsSpan(0, svcLen) : default,
            svcIdx);
    }

    // ── KeyValue { "key": "...", "value": { AnyValue } } → msgpack key + value ──

    /// <summary>A record or kvlist attribute: never the service.</summary>
    private static bool WriteKeyValue(ref Utf8JsonReader reader, ref MessagePackWriter w, ref MapBudget map)
    {
        ServiceCapture none = default;   // untouched: capture is off
        return WriteKeyValue(ref reader, ref w, ref none, captureService: false, ref map);
    }

    /// <summary>
    /// Writes one KeyValue as a msgpack pair and returns whether it wrote one — counted first
    /// (<see cref="Room"/>), and not written once the map is past its limit. With
    /// <paramref name="captureService"/>, the resource's first <c>service.name</c> is decided here
    /// (see <see cref="ServiceCapture"/>): its key is held back until the value is known, and a
    /// value that becomes the service writes nothing at all — the pair is not in the map. One pair:
    /// a second <c>key</c> or <c>value</c> in the same KeyValue (not OTLP) is skipped, as a pair
    /// with two keys was a map whose count no longer matched it.
    /// </summary>
    private static bool WriteKeyValue(
        ref Utf8JsonReader reader, ref MessagePackWriter w, ref ServiceCapture svc, bool captureService, ref MapBudget map)
    {
        if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); return false; }

        bool wroteKey = false, wrote = false;
        bool heldKey  = false;   // the service.name key, not yet written
        bool captured = false;   // …and its value became the service: write neither
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }

            if (JsonStringText.NameIs(ref reader, "key"u8))
            {
                reader.Read();
                if (reader.TokenType == JsonTokenType.String && !wroteKey)
                {
                    if (captureService && !svc.Seen && JsonStringText.NameIs(ref reader, "service.name"u8))
                    {
                        svc.Seen = true;
                        heldKey  = true;
                    }
                    else WriteString(ref reader, ref w, ref map);
                    wroteKey = true;
                }
                else reader.Skip();
            }
            else if (JsonStringText.NameIs(ref reader, "value"u8))
            {
                if (!wroteKey || captured || wrote) { reader.Skip(); continue; } // value before key (non-standard), or a second one — skip
                if (heldKey)
                {
                    heldKey = false;
                    if (TryCaptureService(ref reader, ref svc)) { captured = true; continue; }
                    WriteServiceNameKey(ref w, ref map);   // not the service: an ordinary property after all
                }
                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                    WriteAnyValue(ref reader, ref w, ref map);
                else
                {
                    reader.Skip();
                    if (Room(ref map, 1)) w.WriteNil();
                }
                wrote = true;
            }
            else reader.Skip();
        }

        if (captured) return false;
        if (heldKey) WriteServiceNameKey(ref w, ref map);              // a held key whose value never came
        if (wroteKey && !wrote && Room(ref map, 1)) w.WriteNil();      // key with no value object
        return wroteKey;
    }

    /// <summary>The <c>service.name</c> key, written as an ordinary property's.</summary>
    private static void WriteServiceNameKey(ref MessagePackWriter w, ref MapBudget map)
    {
        if (Room(ref map, 1 + 12)) w.WriteString("service.name"u8);
    }

    /// <summary>
    /// With the reader on a held <c>service.name</c>'s <c>"value"</c> property: when the AnyValue
    /// carries a non-empty <c>stringValue</c>, copies it — no further than <see cref="ServiceCapture.Max"/>
    /// — into <see cref="ServiceCapture.Buf"/>, moves the reader past the value and returns true.
    /// Otherwise leaves the reader where it was and returns false, and the caller writes the pair as
    /// usual. The look-ahead runs on a COPY of the reader — a struct over the whole body, so the copy is
    /// a checkpoint and costs nothing.
    /// </summary>
    private static bool TryCaptureService(ref Utf8JsonReader reader, ref ServiceCapture svc)
    {
        var peek = reader;
        if (!peek.Read() || peek.TokenType != JsonTokenType.StartObject) return false;

        int len = 0;
        while (peek.Read() && peek.TokenType != JsonTokenType.EndObject)
        {
            if (peek.TokenType != JsonTokenType.PropertyName) { peek.Skip(); continue; }
            if (JsonStringText.NameIs(ref peek, "stringValue"u8))
            {
                peek.Read();
                if (peek.TokenType == JsonTokenType.String) len = CopyServiceName(ref peek, ref svc);
                else peek.Skip();
            }
            else peek.Skip();
        }
        if (len == 0) return false;

        svc.Len = len;
        reader.Skip();   // on the "value" property name: past its whole AnyValue
        return true;
    }

    /// <summary>
    /// Copies the current string token's text — unescaped, as the store keeps it — into
    /// <see cref="ServiceCapture.Buf"/>, no further than <see cref="ServiceCapture.Max"/> bytes (at a
    /// character), growing the buffer (rent, then return) as far as that; returns its length.
    /// </summary>
    private static int CopyServiceName(ref Utf8JsonReader reader, ref ServiceCapture svc)
    {
        var  raw     = reader.ValueSpan;
        bool escaped = reader.ValueIsEscaped;
        long full    = escaped ? JsonStringText.UnescapedLength(raw) : raw.Length;
        int  want    = (int)Math.Min(full, svc.Max);
        if (want > svc.Buf.Length)
        {
            // Rent first, then return — same reasoning as SegmentReader.ReadBlockInto. The buffer is
            // the parse's, returned in its finally, so a throw between the return and the reassignment
            // would have it returned twice and the pool would then lease one array to two owners.
            byte[] grown = ArrayPool<byte>.Shared.Rent(want);
            ArrayPool<byte>.Shared.Return(svc.Buf);
            svc.Buf = grown;
        }
        if (!escaped)
        {
            raw[..want].CopyTo(svc.Buf);   // raw bytes: the sink cuts, and decodes what is not UTF-8, as for protobuf
            return want;
        }
        return JsonStringText.Unescape(raw, svc.Buf.AsSpan(0, want));
    }

    // ── AnyValue { stringValue | intValue | boolValue | doubleValue | array | kvlist } ──

    /// <summary>
    /// One AnyValue → one msgpack value, counted first. The first case of the oneof decides; a second
    /// (not OTLP) is skipped — two were written, and the map then had a value too many.
    /// </summary>
    private static void WriteAnyValue(ref Utf8JsonReader reader, ref MessagePackWriter w, ref MapBudget map)
    {
        bool wrote = false;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (wrote) { reader.Skip(); continue; }

            if (JsonStringText.NameIs(ref reader, "stringValue"u8))
            {
                reader.Read();
                WriteString(ref reader, ref w, ref map);
                wrote = true;
            }
            else if (JsonStringText.NameIs(ref reader, "intValue"u8))
            {
                reader.Read();
                // OTLP encodes int64 as a JSON string; tolerate a raw number too.
                long v = 0;
                if (reader.TokenType == JsonTokenType.String)      Utf8Parser.TryParse(reader.ValueSpan, out v, out _);
                else if (reader.TokenType == JsonTokenType.Number) reader.TryGetInt64(out v);
                else reader.Skip();
                if (Room(ref map, OtlpLogProtoParser.IntBytes(v))) w.Write(v);
                wrote = true;
            }
            else if (JsonStringText.NameIs(ref reader, "boolValue"u8))
            {
                reader.Read();
                bool b = reader.TokenType == JsonTokenType.True;
                reader.Skip();
                if (Room(ref map, 1)) w.Write(b);
                wrote = true;
            }
            else if (JsonStringText.NameIs(ref reader, "doubleValue"u8))
            {
                reader.Read();
                double d = 0;
                if (reader.TokenType == JsonTokenType.Number) reader.TryGetDouble(out d);
                else reader.Skip();
                if (Room(ref map, 9)) w.Write(d);
                wrote = true;
            }
            else if (JsonStringText.NameIs(ref reader, "arrayValue"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                    WriteArrayValue(ref reader, ref w, ref map);
                else
                {
                    reader.Skip();
                    if (Room(ref map, 1)) w.WriteArrayHeader(0);
                }
                wrote = true;
            }
            else if (JsonStringText.NameIs(ref reader, "kvlistValue"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                    WriteKvlistValue(ref reader, ref w, ref map);
                else
                {
                    reader.Skip();
                    if (Room(ref map, 1)) w.WriteMapHeader(0);
                }
                wrote = true;
            }
            else
            {
                reader.Skip();
            }
        }
        if (!wrote && Room(ref map, 1)) w.WriteNil();
    }

    /// <summary>
    /// ArrayValue → msgpack array. msgpack needs the element count before the elements: they are
    /// counted on a copy of the reader (a checkpoint, as in <see cref="TryCaptureService"/>) and then
    /// written straight into the map. Each level used to be buffered in a new writer and copied into
    /// the one above it: a 4 MB leaf fifteen levels deep allocated 131 MB. An array value with no
    /// <c>values</c> is an empty array — it was no value at all, and the map lost its count.
    /// </summary>
    private static void WriteArrayValue(ref Utf8JsonReader reader, ref MessagePackWriter w, ref MapBudget map)
    {
        bool header = false;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (JsonStringText.NameIs(ref reader, "values"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                if (header) { reader.Skip(); continue; }   // a second "values" (not OTLP): the first is the array

                int n = 0;
                var peek = reader;
                while (peek.Read() && peek.TokenType != JsonTokenType.EndArray)
                {
                    if (peek.TokenType == JsonTokenType.StartObject) n++;
                    peek.Skip();
                }
                if (Room(ref map, OtlpLogProtoParser.CollectionHeaderBytes(n))) w.WriteArrayHeader(n);
                header = true;

                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (reader.TokenType == JsonTokenType.StartObject) WriteAnyValue(ref reader, ref w, ref map);
                    else reader.Skip();
                }
            }
            else reader.Skip();
        }
        if (!header && Room(ref map, 1)) w.WriteArrayHeader(0);
    }

    /// <summary>KvlistValue → msgpack map, counting only the entries that carry a key, as <see cref="WriteArrayValue"/> counts.</summary>
    private static void WriteKvlistValue(ref Utf8JsonReader reader, ref MessagePackWriter w, ref MapBudget map)
    {
        bool header = false;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (JsonStringText.NameIs(ref reader, "values"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                if (header) { reader.Skip(); continue; }

                int n = 0;
                var peek = reader;
                while (peek.Read() && peek.TokenType != JsonTokenType.EndArray)
                {
                    if (peek.TokenType == JsonTokenType.StartObject) { if (HasKey(ref peek)) n++; }
                    else peek.Skip();
                }
                if (Room(ref map, OtlpLogProtoParser.CollectionHeaderBytes(n))) w.WriteMapHeader(n);
                header = true;

                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    WriteKeyValue(ref reader, ref w, ref map);
            }
            else reader.Skip();
        }
        if (!header && Room(ref map, 1)) w.WriteMapHeader(0);
    }

    /// <summary>
    /// With the reader on a KeyValue's StartObject: whether <see cref="WriteKeyValue"/> would write a
    /// pair for it — a <c>key</c> that is a string. Leaves the reader on its EndObject.
    /// </summary>
    private static bool HasKey(ref Utf8JsonReader reader)
    {
        bool has = false;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (JsonStringText.NameIs(ref reader, "key"u8))
            {
                reader.Read();
                if (reader.TokenType == JsonTokenType.String) has = true;
                else reader.Skip();
            }
            else reader.Skip();
        }
        return has;
    }

    // ── Small helpers ──────────────────────────────────────────────────────────

    /// <summary>
    /// Reads a body <c>{ "stringValue": "..." }</c>: the template's UTF-8 — where it lies in the body
    /// when it has no escape (the sink decodes what is not UTF-8, as for protobuf), or unescaped into
    /// <paramref name="tmpl"/>. Empty for a body with no string. A string the body sent as an object
    /// or an array is skipped whole (its first object used to end the body early).
    /// </summary>
    private static ReadOnlySpan<byte> ReadBodyString(scoped ref Utf8JsonReader reader, scoped ref TemplateScratch tmpl, IOtlpLogSink sink)
    {
        ReadOnlySpan<byte> text = default;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (JsonStringText.NameIs(ref reader, "stringValue"u8))
            {
                reader.Read();
                if (reader.TokenType == JsonTokenType.String) text = TemplateText(ref reader, ref tmpl, sink);
                else { reader.Skip(); text = default; }
            }
            else reader.Skip();
        }
        return text;
    }

    /// <summary>The current string token as a template: see <see cref="ReadBodyString"/>.</summary>
    private static ReadOnlySpan<byte> TemplateText(scoped ref Utf8JsonReader reader, scoped ref TemplateScratch tmpl, IOtlpLogSink sink)
    {
        var raw = reader.ValueSpan;
        if (!reader.ValueIsEscaped) return raw;

        long length = JsonStringText.UnescapedLength(raw);
        byte[] dest = tmpl.Small;
        if (length > dest.Length)
        {
            if (tmpl.Large is null || tmpl.Large.Length < length)
            {
                // Counted against the batch BEFORE it is taken (round-5 H-E): it can be as long as the
                // body, and nothing counted it. A refusal throws here with nothing taken.
                sink.ChargeScratch(length - tmpl.Charged);
                tmpl.Charged = length;
                tmpl.Large   = null;                                            // the old one is garbage first
                tmpl.Large   = GC.AllocateUninitializedArray<byte>((int)length);
            }
            dest = tmpl.Large;
        }
        return dest.AsSpan(0, JsonStringText.Unescape(raw, dest));
    }

    /// <summary>
    /// Writes the current JSON string token to msgpack as a str holding the text the store keeps —
    /// escapes resolved, and U+FFFD for each sequence that is not UTF-8 — counted first
    /// (<see cref="Room"/>), and not written once the map is past the limit. An escaped one is
    /// unescaped straight into the writer: it used to be unescaped into a rent as long as the value,
    /// body-sized for a hostile one. A token that is not a string is a nil.
    /// </summary>
    private static void WriteString(ref Utf8JsonReader reader, ref MessagePackWriter w, ref MapBudget map)
    {
        if (reader.TokenType != JsonTokenType.String)
        {
            reader.Skip();   // an object or array where a string belongs (not OTLP): skipped whole
            if (Room(ref map, 1)) w.WriteNil();
            return;
        }

        var raw = reader.ValueSpan;
        if (!reader.ValueIsEscaped)
        {
            bool valid = Utf8.IsValid(raw);
            // Longer than the limit in bytes, it passes it whatever it decodes to: not decoded at all.
            long length = valid || raw.Length > map.Limit ? raw.Length : OtlpLogProtoParser.ReplacedLength(raw);
            if (!Room(ref map, OtlpLogProtoParser.StrHeaderBytes(length) + length)) return;
            if (valid) w.WriteString(raw);
            else OtlpLogProtoParser.WriteReplacingInvalid(ref w, raw, (int)length);
            return;
        }

        long unescaped = JsonStringText.UnescapedLength(raw);
        if (!Room(ref map, OtlpLogProtoParser.StrHeaderBytes(unescaped) + unescaped)) return;
        w.WriteStringHeader((int)unescaped);
        int written = JsonStringText.Unescape(raw, w.GetSpan((int)unescaped));
        System.Diagnostics.Debug.Assert(written == unescaped, "the unescaped length counted is not the one written");
        w.Advance(written);
    }

    /// <summary>Copies the current string token (unescaped) into a fixed buffer; returns length (0 if it doesn't fit).</summary>
    private static int CopyString(ref Utf8JsonReader reader, byte[] dest)
    {
        if (reader.TokenType != JsonTokenType.String) { reader.Skip(); return 0; }
        var raw = reader.ValueSpan;
        if (!reader.ValueIsEscaped)
        {
            if (raw.Length > dest.Length) return 0;
            raw.CopyTo(dest);
            return raw.Length;
        }
        if (JsonStringText.UnescapedLength(raw) > dest.Length) return 0;
        return JsonStringText.Unescape(raw, dest);
    }

    private static long ReadUnixNano(ref Utf8JsonReader reader)
    {
        if (reader.TokenType == JsonTokenType.String)
            return Utf8Parser.TryParse(reader.ValueSpan, out long v, out _) ? v : 0;
        if (reader.TokenType == JsonTokenType.Number)
            return reader.TryGetInt64(out long n) ? n : 0;
        reader.Skip();
        return 0;
    }

    private static LogLevel MapSeverityNumber(int n) =>
        n >= 21 ? LogLevel.Fatal :
        n >= 17 ? LogLevel.Error :
        n >= 13 ? LogLevel.Warning :
        n >= 9  ? LogLevel.Information :
        n >= 5  ? LogLevel.Debug :
        n >= 1  ? LogLevel.Verbose : LogLevel.Information;

    private static LogLevel MapSeverityText(ReadOnlySpan<byte> t)
    {
        if (t.StartsWith("FATAL"u8) || t.StartsWith("fatal"u8)) return LogLevel.Fatal;
        if (t.StartsWith("ERROR"u8) || t.StartsWith("error"u8)) return LogLevel.Error;
        if (t.StartsWith("WARN"u8)  || t.StartsWith("warn"u8))  return LogLevel.Warning;
        if (t.StartsWith("INFO"u8)  || t.StartsWith("info"u8))  return LogLevel.Information;
        if (t.StartsWith("DEBUG"u8) || t.StartsWith("debug"u8)) return LogLevel.Debug;
        if (t.StartsWith("TRACE"u8) || t.StartsWith("trace"u8)) return LogLevel.Verbose;
        return LogLevel.Information;
    }
}

/// <summary>
/// The text of a JSON string token as the store keeps it (round-5 review H-E, L-10): escapes resolved,
/// and each sequence that is not UTF-8 — raw bytes, or a <c>\u</c> escape of a lone surrogate — as
/// U+FFFD, as the protobuf parser stores a protobuf string (its <c>WriteReplacingInvalid</c>). For
/// valid input, byte for byte what <see cref="Utf8JsonReader.CopyString(Span{byte})"/> writes; that
/// threw on the rest, part way through a batch. Its length is known before anything is written
/// (<see cref="UnescapedLength"/>), so a value is counted against the map's limit first and then
/// written straight into its destination. Allocates nothing.
///
/// <para>The input is a token <see cref="Utf8JsonReader"/> has read: its escapes are well-formed (a
/// backslash, then one of <c>"\/bfnrt</c>, or <c>u</c> and four hex digits) and it holds no control
/// character.</para>
/// </summary>
internal static class JsonStringText
{
    /// <summary>
    /// Whether the reader's current property name — or string — is <paramref name="text"/>, an ASCII name of a few
    /// bytes, read as the store reads text: a <c>\u</c> escape of a lone surrogate is U+FFFD, so such a name is none
    /// of ours (round-6 review, the LOW on L-10). <see cref="Utf8JsonReader.ValueTextEquals(ReadOnlySpan{byte})"/>
    /// threw on it, out of a parse the weighing walk had accepted: a 400 with the records before it written, where a
    /// lone surrogate in a value was stored with U+FFFD. Allocates nothing. The reader is over one span.
    /// </summary>
    public static bool NameIs(scoped ref Utf8JsonReader reader, ReadOnlySpan<byte> text)
    {
        var raw = reader.ValueSpan;
        if (!reader.ValueIsEscaped) return raw.SequenceEqual(text);
        // An escape spells one byte in two at the least, so a name that unescapes to `text` is longer than it.
        if (raw.Length <= text.Length || UnescapedLength(raw) != text.Length) return false;
        Span<byte> unescaped = stackalloc byte[text.Length];
        return Unescape(raw, unescaped) == text.Length && unescaped.SequenceEqual(text);
    }

    /// <summary>The bytes <see cref="Unescape"/> writes for <paramref name="escaped"/>.</summary>
    public static long UnescapedLength(ReadOnlySpan<byte> escaped)
    {
        long length = 0;
        int  i      = 0;
        while (i < escaped.Length)
        {
            int next = escaped[i..].IndexOf((byte)'\\');
            int end  = next < 0 ? escaped.Length : i + next;
            if (end > i)
            {
                var run = escaped[i..end];
                length += Utf8.IsValid(run) ? run.Length : OtlpLogProtoParser.ReplacedLength(run);
                i = end;
                continue;
            }

            // An escape.
            byte e = escaped[i + 1];
            if (e != (byte)'u') { length++; i += 2; continue; }
            int u = Hex4(escaped.Slice(i + 2, 4));
            if (IsPair(escaped, i, u)) { length += 4; i += 12; continue; }
            length += char.IsSurrogate((char)u) ? 3 : u < 0x80 ? 1 : u < 0x800 ? 2 : 3;
            i += 6;
        }
        return length;
    }

    /// <summary>
    /// Writes the text of <paramref name="escaped"/> into <paramref name="dest"/>, at least
    /// <see cref="UnescapedLength"/> long — or, shorter, as much of it as fits, stopping before a
    /// character that does not; returns the bytes written.
    /// </summary>
    public static int Unescape(ReadOnlySpan<byte> escaped, Span<byte> dest)
    {
        int i = 0, o = 0;
        while (i < escaped.Length)
        {
            int next = escaped[i..].IndexOf((byte)'\\');
            int end  = next < 0 ? escaped.Length : i + next;
            if (end > i)
            {
                // A run of raw bytes: copied, each sequence that is not UTF-8 as U+FFFD.
                var run = escaped[i..end];
                if (Utf8.IsValid(run))
                {
                    if (run.Length <= dest.Length - o) { run.CopyTo(dest[o..]); o += run.Length; i = end; continue; }
                }
                while (!run.IsEmpty)
                {
                    var status = Rune.DecodeFromUtf8(run, out var rune, out int consumed);
                    int n = status == OperationStatus.Done ? consumed : 3;
                    if (n > dest.Length - o) return o;
                    if (status == OperationStatus.Done) run[..consumed].CopyTo(dest[o..]);
                    else ReplacementUtf8.CopyTo(dest[o..]);
                    o += n;
                    run = run[consumed..];
                }
                i = end;
                continue;
            }

            byte e = escaped[i + 1];
            if (e != (byte)'u')
            {
                if (o >= dest.Length) return o;
                dest[o++] = e switch
                {
                    (byte)'b' => (byte)'\b',
                    (byte)'f' => (byte)'\f',
                    (byte)'n' => (byte)'\n',
                    (byte)'r' => (byte)'\r',
                    (byte)'t' => (byte)'\t',
                    _         => e,   // " \ /
                };
                i += 2;
                continue;
            }

            int u = Hex4(escaped.Slice(i + 2, 4));
            Rune r;
            int  width;
            if (IsPair(escaped, i, u))
            {
                r     = new Rune(char.ConvertToUtf32((char)u, (char)Hex4(escaped.Slice(i + 8, 4))));
                width = 12;
            }
            else
            {
                r     = char.IsSurrogate((char)u) ? Rune.ReplacementChar : new Rune(u);
                width = 6;
            }
            if (r.Utf8SequenceLength > dest.Length - o) return o;
            o += r.EncodeToUtf8(dest[o..]);
            i += width;
        }
        return o;
    }

    /// <summary>U+FFFD in UTF-8.</summary>
    private static ReadOnlySpan<byte> ReplacementUtf8 => [0xEF, 0xBF, 0xBD];

    /// <summary>Whether the <c>\u</c> escape at <paramref name="i"/> (of <paramref name="u"/>) is a high surrogate with an escaped low one right after it.</summary>
    private static bool IsPair(ReadOnlySpan<byte> escaped, int i, int u) =>
        char.IsHighSurrogate((char)u) && i + 12 <= escaped.Length &&
        escaped[i + 6] == (byte)'\\' && escaped[i + 7] == (byte)'u' &&
        char.IsLowSurrogate((char)Hex4(escaped.Slice(i + 8, 4)));

    private static int Hex4(ReadOnlySpan<byte> h) => (Hex(h[0]) << 12) | (Hex(h[1]) << 8) | (Hex(h[2]) << 4) | Hex(h[3]);

    private static int Hex(byte c) => c <= (byte)'9' ? c - '0' : (c | 0x20) - 'a' + 10;
}
