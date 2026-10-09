using System.Buffers;
using System.Buffers.Text;
using System.Text.Json;
using MessagePack;
using Ameto.Core;
using Ameto.Ingestion;

namespace Ameto.Otel;

/// <summary>
/// Zero-alloc streaming parser for OTLP <c>ExportLogsServiceRequest</c> JSON. Walks the body
/// with a forward-only <see cref="Utf8JsonReader"/> and writes each log record (header +
/// msgpack properties) straight into the ingestion ring via
/// <see cref="IngestionEndpoint.TryIngestRaw"/> — never materialising the OTLP object graph,
/// per-record <c>LogEvent</c>s, or per-attribute strings. Replaces the reflection-based
/// <c>JsonSerializer.Deserialize&lt;ExportLogsServiceRequest&gt;</c> + <c>OtlpLogMapper.Map</c>
/// path that dominated GC pressure above ~60k events/s.
///
/// Assumes standard OTLP document order (a resourceLogs' <c>resource</c> precedes its
/// <c>scopeLogs</c>; a KeyValue's <c>key</c> precedes its <c>value</c>) — true for every
/// conformant exporter. Out-of-order input degrades gracefully (missing resource attrs),
/// it never corrupts. Nested array/kvlist attribute values (rare in logs) take a small
/// pooled buffer; the scalar hot path is allocation-free.
///
/// <para>The resource's <c>service.name</c> becomes the event's <c>@service</c> header and
/// leaves the property map, exactly as the mapper and the protobuf parser do it — see
/// <see cref="ServiceCapture"/>.</para>
/// </summary>
public static class OtlpLogStreamParser
{
    // Reused across requests on the same request thread (one request per thread at a time)
    // so a batch allocates no msgpack scratch — the whole point of the streaming path.
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tRes;
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tRec;
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tOut;
    [ThreadStatic] private static byte[]? _tEsc;

    /// <summary>
    /// Unescape scratch for one attribute value. Long enough that a message, a path or a stack
    /// frame fits — the values that carry escapes at all — so the pool fallback below is for the
    /// genuinely large ones only. One array per request thread, so its size costs nothing.
    /// </summary>
    private const int UnescapeScratchBytes = 1024;

    public static (int Ingested, int Dropped) Parse(ReadOnlySpan<byte> json, IOtlpLogSink sink)
    {
        var reader = new Utf8JsonReader(json, isFinalBlock: true, state: default);

        // Thread-reused scratch, reset per resource/record.
        var resBuf = _tRes ??= new ArrayBufferWriter<byte>(4096);   // resource attrs (msgpack KV pairs)
        var recBuf = _tRec ??= new ArrayBufferWriter<byte>(8192);   // record attrs + @tr/@sp (msgpack KV pairs)
        var outBuf = _tOut ??= new ArrayBufferWriter<byte>(8192);   // assembled map: header + resBuf + recBuf
        resBuf.ResetWrittenCount(); recBuf.ResetWrittenCount(); outBuf.ResetWrittenCount();
        // The resource's service.name, captured per resource; its buffer grows (rent, then return)
        // for a long name, so the one to give back is whatever it holds when the parse ends.
        var    svc     = new ServiceCapture { Buf = ArrayPool<byte>.Shared.Rent(256) };
        byte[] tmplBuf = ArrayPool<byte>.Shared.Rent(4096);  // captured body/template bytes (per record)
        byte[] trBuf   = ArrayPool<byte>.Shared.Rent(32);    // traceId bytes (per record)
        byte[] spBuf   = ArrayPool<byte>.Shared.Rent(16);    // spanId bytes (per record)

        int ingested = 0, dropped = 0;
        try
        {
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                return (0, 0);

            while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
            {
                if (reader.TokenType == JsonTokenType.PropertyName &&
                    reader.ValueTextEquals("resourceLogs"u8) &&
                    reader.Read() && reader.TokenType == JsonTokenType.StartArray)
                {
                    while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                        ParseResourceLogs(ref reader, sink, resBuf, recBuf, outBuf,
                            ref svc, ref tmplBuf, trBuf, spBuf, ref ingested, ref dropped);
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
            ArrayPool<byte>.Shared.Return(tmplBuf);
            ArrayPool<byte>.Shared.Return(trBuf);
            ArrayPool<byte>.Shared.Return(spBuf);
        }
    }

    // ── resourceLogs[] element ─────────────────────────────────────────────────
    private static void ParseResourceLogs(
        ref Utf8JsonReader reader, IOtlpLogSink sink,
        ArrayBufferWriter<byte> resBuf, ArrayBufferWriter<byte> recBuf, ArrayBufferWriter<byte> outBuf,
        ref ServiceCapture svc, ref byte[] tmplBuf, byte[] trBuf, byte[] spBuf,
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

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }

            if (reader.ValueTextEquals("resource"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                {
                    ParseResourceAttributes(ref reader, resBuf, ref resKeyCount, ref svc);
                    svcIdx = svc.Len > 0 ? sink.InternService(svc.Buf.AsSpan(0, svc.Len)) : -1;
                }
                else
                    reader.Skip();
            }
            else if (reader.ValueTextEquals("scopeLogs"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartArray)
                {
                    while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                        ParseScopeLogs(ref reader, sink, resBuf, resKeyCount, svc.Buf, svc.Len, svcIdx,
                            recBuf, outBuf, ref tmplBuf, trBuf, spBuf, ref ingested, ref dropped);
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
        ref Utf8JsonReader reader, ArrayBufferWriter<byte> resBuf, ref int keyCount, ref ServiceCapture svc)
    {
        var w = new MessagePackWriter(resBuf);
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (reader.ValueTextEquals("attributes"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    if (WriteKeyValue(ref reader, ref w, ref svc, captureService: true)) keyCount++;
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
    }

    // ── scopeLogs[] element ────────────────────────────────────────────────────
    private static void ParseScopeLogs(
        ref Utf8JsonReader reader, IOtlpLogSink sink,
        ArrayBufferWriter<byte> resBuf, int resKeyCount, byte[] svcBuf, int svcLen, int svcIdx,
        ArrayBufferWriter<byte> recBuf, ArrayBufferWriter<byte> outBuf,
        ref byte[] tmplBuf, byte[] trBuf, byte[] spBuf,
        ref int ingested, ref int dropped)
    {
        if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); return; }

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (reader.ValueTextEquals("logRecords"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    bool ok = ParseLogRecord(ref reader, sink, resBuf, resKeyCount, svcBuf, svcLen, svcIdx,
                        recBuf, outBuf, ref tmplBuf, trBuf, spBuf);
                    if (ok) ingested++; else dropped++;
                }
            }
            else reader.Skip();
        }
    }

    // ── one logRecord → one ring entry ─────────────────────────────────────────
    private static bool ParseLogRecord(
        ref Utf8JsonReader reader, IOtlpLogSink sink,
        ArrayBufferWriter<byte> resBuf, int resKeyCount, byte[] svcBuf, int svcLen, int svcIdx,
        ArrayBufferWriter<byte> recBuf, ArrayBufferWriter<byte> outBuf,
        ref byte[] tmplBuf, byte[] trBuf, byte[] spBuf)
    {
        if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); return false; }

        long tsTicks   = 0;
        byte level     = (byte)LogLevel.Information;
        int  sevNumber = 0;
        int  tmplLen   = 0;
        int  trLen     = 0;
        int  spLen     = 0;

        recBuf.ResetWrittenCount();
        var w = new MessagePackWriter(recBuf);
        int recKeyCount = 0;

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }

            if (reader.ValueTextEquals("timeUnixNano"u8))
            {
                reader.Read();
                long nanos = ReadUnixNano(ref reader);
                tsTicks = nanos > 0 ? DateTimeOffset.FromUnixTimeMilliseconds(nanos / 1_000_000).UtcTicks
                                    : DateTimeOffset.UtcNow.UtcTicks;
            }
            else if (reader.ValueTextEquals("severityNumber"u8))
            {
                reader.Read();
                reader.TryGetInt32(out sevNumber);
            }
            else if (reader.ValueTextEquals("severityText"u8))
            {
                reader.Read();
                if (sevNumber == 0) level = (byte)MapSeverityText(reader.ValueSpan);
            }
            else if (reader.ValueTextEquals("body"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                    tmplLen = ReadBodyString(ref reader, ref tmplBuf);
                else reader.Skip();
            }
            else if (reader.ValueTextEquals("traceId"u8))
            {
                reader.Read();
                trLen = CopyString(ref reader, trBuf);
            }
            else if (reader.ValueTextEquals("spanId"u8))
            {
                reader.Read();
                spLen = CopyString(ref reader, spBuf);
            }
            else if (reader.ValueTextEquals("attributes"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    if (WriteKeyValue(ref reader, ref w)) recKeyCount++;   // records never carry the service
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
        if (!trSpan.IsEmpty) { w.WriteString("@tr"u8); w.WriteString(trSpan); recKeyCount++; }
        if (!spSpan.IsEmpty) { w.WriteString("@sp"u8); w.WriteString(spSpan); recKeyCount++; }
        w.Flush();

        // Assemble the final msgpack map: header(total) + resource KV bytes + record KV bytes.
        int total = resKeyCount + recKeyCount;
        outBuf.ResetWrittenCount();
        var ow = new MessagePackWriter(outBuf);
        ow.WriteMapHeader(total);
        if (resKeyCount > 0) ow.WriteRaw(resBuf.WrittenSpan);
        if (recKeyCount > 0) ow.WriteRaw(recBuf.WrittenSpan);
        ow.Flush();

        ulong trHi = 0, trLo = 0, spanId = 0;
        if (!trSpan.IsEmpty) TraceIdHelper.TryParseTraceId(trSpan, out trHi, out trLo);
        if (!spSpan.IsEmpty) TraceIdHelper.TryParseSpanId(spSpan, out spanId);

        if (tsTicks == 0) tsTicks = DateTimeOffset.UtcNow.UtcTicks;

        return sink.TryIngestRaw(
            tsTicks, level,
            tmplLen > 0 ? tmplBuf.AsSpan(0, tmplLen) : default,
            outBuf.WrittenSpan,
            trHi, trLo, spanId,
            svcLen > 0 ? svcBuf.AsSpan(0, svcLen) : default,
            svcIdx);
    }

    // ── KeyValue { "key": "...", "value": { AnyValue } } → msgpack key + value ──

    /// <summary>A record or kvlist attribute: never the service.</summary>
    private static bool WriteKeyValue(ref Utf8JsonReader reader, ref MessagePackWriter w)
    {
        ServiceCapture none = default;   // untouched: capture is off
        return WriteKeyValue(ref reader, ref w, ref none, captureService: false);
    }

    /// <summary>
    /// Writes one KeyValue as a msgpack pair and returns whether it wrote one. With
    /// <paramref name="captureService"/>, the resource's first <c>service.name</c> is decided here
    /// (see <see cref="ServiceCapture"/>): its key is held back until the value is known, and a
    /// value that becomes the service writes nothing at all — the pair is not in the map.
    /// </summary>
    private static bool WriteKeyValue(
        ref Utf8JsonReader reader, ref MessagePackWriter w, ref ServiceCapture svc, bool captureService)
    {
        if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); return false; }

        bool wroteKey = false, wrote = false;
        bool heldKey  = false;   // the service.name key, not yet written
        bool captured = false;   // …and its value became the service: write neither
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }

            if (reader.ValueTextEquals("key"u8))
            {
                reader.Read();
                if (reader.TokenType == JsonTokenType.String)
                {
                    if (captureService && !svc.Seen && reader.ValueTextEquals("service.name"u8))
                    {
                        svc.Seen = true;
                        heldKey  = true;
                    }
                    else WriteJsonStringToMsgpack(ref reader, ref w);
                    wroteKey = true;
                }
                else reader.Skip();
            }
            else if (reader.ValueTextEquals("value"u8))
            {
                if (!wroteKey || captured) { reader.Skip(); continue; } // value before key (non-standard) — skip
                if (heldKey)
                {
                    heldKey = false;
                    if (TryCaptureService(ref reader, ref svc)) { captured = true; continue; }
                    w.WriteString("service.name"u8);   // not the service: an ordinary property after all
                }
                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                    WriteAnyValue(ref reader, ref w);
                else
                    w.WriteNil();
                wrote = true;
            }
            else reader.Skip();
        }

        if (captured) return false;
        if (heldKey) w.WriteString("service.name"u8);   // a held key whose value never came
        if (wroteKey && !wrote) w.WriteNil(); // key with no value object
        return wroteKey;
    }

    /// <summary>
    /// With the reader on a held <c>service.name</c>'s <c>"value"</c> property: when the AnyValue
    /// carries a non-empty <c>stringValue</c>, copies it into <see cref="ServiceCapture.Buf"/>,
    /// moves the reader past the value and returns true. Otherwise leaves the reader where it was
    /// and returns false, and the caller writes the pair as usual. The look-ahead runs on a COPY of
    /// the reader — a struct over the whole body, so the copy is a checkpoint and costs nothing.
    /// </summary>
    private static bool TryCaptureService(ref Utf8JsonReader reader, ref ServiceCapture svc)
    {
        var peek = reader;
        if (!peek.Read() || peek.TokenType != JsonTokenType.StartObject) return false;

        int len = 0;
        while (peek.Read() && peek.TokenType != JsonTokenType.EndObject)
        {
            if (peek.TokenType != JsonTokenType.PropertyName) { peek.Skip(); continue; }
            if (peek.ValueTextEquals("stringValue"u8))
            {
                peek.Read();
                if (peek.TokenType == JsonTokenType.String) len = CopyStringGrow(ref peek, ref svc.Buf);
                else peek.Skip();
            }
            else peek.Skip();
        }
        if (len == 0) return false;

        svc.Len = len;
        reader.Skip();   // on the "value" property name: past its whole AnyValue
        return true;
    }

    // ── AnyValue { stringValue | intValue | boolValue | doubleValue | array | kvlist } ──
    private static void WriteAnyValue(ref Utf8JsonReader reader, ref MessagePackWriter w)
    {
        bool wrote = false;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }

            if (reader.ValueTextEquals("stringValue"u8))
            {
                reader.Read();
                WriteJsonStringToMsgpack(ref reader, ref w);
                wrote = true;
            }
            else if (reader.ValueTextEquals("intValue"u8))
            {
                reader.Read();
                // OTLP encodes int64 as a JSON string; tolerate a raw number too.
                long v = 0;
                if (reader.TokenType == JsonTokenType.String) Utf8Parser.TryParse(reader.ValueSpan, out v, out _);
                else reader.TryGetInt64(out v);
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
                    WriteArrayValue(ref reader, ref w);
                else reader.Skip();
                wrote = true;
            }
            else if (reader.ValueTextEquals("kvlistValue"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                    WriteKvlistValue(ref reader, ref w);
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

    // Nested array/kvlist need an element count before the msgpack header ⇒ buffer then splice.
    private static void WriteArrayValue(ref Utf8JsonReader reader, ref MessagePackWriter w)
    {
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (reader.ValueTextEquals("values"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                var tmp = new ArrayBufferWriter<byte>(256);
                var tw  = new MessagePackWriter(tmp);
                int n = 0;
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (reader.TokenType == JsonTokenType.StartObject) { WriteAnyValue(ref reader, ref tw); n++; }
                    else reader.Skip();
                }
                tw.Flush();
                w.WriteArrayHeader(n);
                w.WriteRaw(tmp.WrittenSpan);
            }
            else reader.Skip();
        }
    }

    private static void WriteKvlistValue(ref Utf8JsonReader reader, ref MessagePackWriter w)
    {
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (reader.ValueTextEquals("values"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                var tmp = new ArrayBufferWriter<byte>(256);
                var tw  = new MessagePackWriter(tmp);
                int n = 0;
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    if (WriteKeyValue(ref reader, ref tw)) n++;
                tw.Flush();
                w.WriteMapHeader(n);
                w.WriteRaw(tmp.WrittenSpan);
            }
            else reader.Skip();
        }
    }

    // ── Small helpers ──────────────────────────────────────────────────────────

    /// <summary>Reads a body { "stringValue": "..." } into <paramref name="dest"/>; returns length.</summary>
    private static int ReadBodyString(ref Utf8JsonReader reader, ref byte[] dest)
    {
        int len = 0;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (reader.ValueTextEquals("stringValue"u8))
            {
                reader.Read();
                len = CopyStringGrow(ref reader, ref dest);
            }
            else reader.Skip();
        }
        return len;
    }

    /// <summary>
    /// Writes the current JSON string token to msgpack as a str, unescaping if needed.
    ///
    /// <para>An escaped value has to be unescaped somewhere before it can be written, and that
    /// used to be a rent and a return from the shared pool per value — not an allocation, but a
    /// pool round trip on a path that runs once per attribute, and every quoted message, Windows
    /// path or accented name in a log line takes it. The per-thread buffer below covers every
    /// attribute value anyone writes; longer ones still fall back to the pool, now with the
    /// return in a finally so a malformed token cannot lose the array.</para>
    ///
    /// <para>A <c>stackalloc</c> would be the obvious buffer and is not usable here:
    /// <c>MessagePackWriter</c> is a ref struct taken by ref, so ref-safety has to assume a span
    /// handed to it could be stored in it, and refuses a stack buffer outright.</para>
    /// </summary>
    private static void WriteJsonStringToMsgpack(ref Utf8JsonReader reader, ref MessagePackWriter w)
    {
        if (reader.TokenType != JsonTokenType.String) { w.WriteNil(); return; }
        if (!reader.ValueIsEscaped) { w.WriteString(reader.ValueSpan); return; }

        int max = reader.ValueSpan.Length;
        byte[] scratch = _tEsc ??= new byte[UnescapeScratchBytes];
        if (max <= scratch.Length)
        {
            w.WriteString(scratch.AsSpan(0, reader.CopyString(scratch)));
            return;
        }

        byte[] tmp = ArrayPool<byte>.Shared.Rent(max);
        try { w.WriteString(tmp.AsSpan(0, reader.CopyString(tmp))); }
        finally { ArrayPool<byte>.Shared.Return(tmp); }
    }

    /// <summary>Copies the current string token (unescaped) into a fixed buffer; returns length (0 if it doesn't fit).</summary>
    private static int CopyString(ref Utf8JsonReader reader, byte[] dest)
    {
        if (reader.TokenType != JsonTokenType.String) return 0;
        if (!reader.ValueIsEscaped)
        {
            var s = reader.ValueSpan;
            if (s.Length > dest.Length) return 0;
            s.CopyTo(dest);
            return s.Length;
        }
        if (reader.ValueSpan.Length > dest.Length) return 0;
        return reader.CopyString(dest);
    }

    /// <summary>Copies the current string token (unescaped) into a poolable buffer, growing it if needed.</summary>
    private static int CopyStringGrow(ref Utf8JsonReader reader, ref byte[] dest)
    {
        if (reader.TokenType != JsonTokenType.String) return 0;
        int max = reader.ValueSpan.Length;
        if (max > dest.Length)
        {
            // Rent first, then return — same reasoning as SegmentReader.ReadBlockInto. `dest` is
            // the caller's buffer by ref and the caller returns it in its own finally, so a throw
            // between the return and the reassignment would have it returned twice and the pool
            // would then lease one array to two owners.
            byte[] grown = ArrayPool<byte>.Shared.Rent(max);
            ArrayPool<byte>.Shared.Return(dest);
            dest = grown;
        }
        if (!reader.ValueIsEscaped) { reader.ValueSpan.CopyTo(dest); return reader.ValueSpan.Length; }
        return reader.CopyString(dest);
    }

    private static long ReadUnixNano(ref Utf8JsonReader reader)
    {
        if (reader.TokenType == JsonTokenType.String)
            return Utf8Parser.TryParse(reader.ValueSpan, out long v, out _) ? v : 0;
        return reader.TryGetInt64(out long n) ? n : 0;
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
