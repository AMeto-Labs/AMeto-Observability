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
/// </summary>
public static class OtlpLogStreamParser
{
    // Reused across requests on the same request thread (one request per thread at a time)
    // so a batch allocates no msgpack scratch — the whole point of the streaming path.
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tRes;
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tRec;
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tOut;

    public static (int Ingested, int Dropped) Parse(ReadOnlySpan<byte> json, IOtlpLogSink sink)
    {
        var reader = new Utf8JsonReader(json, isFinalBlock: true, state: default);

        // Thread-reused scratch, reset per resource/record.
        var resBuf = _tRes ??= new ArrayBufferWriter<byte>(4096);   // resource attrs (msgpack KV pairs)
        var recBuf = _tRec ??= new ArrayBufferWriter<byte>(8192);   // record attrs + @tr/@sp (msgpack KV pairs)
        var outBuf = _tOut ??= new ArrayBufferWriter<byte>(8192);   // assembled map: header + resBuf + recBuf
        resBuf.Clear(); recBuf.Clear(); outBuf.Clear();
        byte[] svcBuf  = ArrayPool<byte>.Shared.Rent(256);   // captured service.name bytes (per resource)
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
                            svcBuf, ref tmplBuf, trBuf, spBuf, ref ingested, ref dropped);
                }
                else
                {
                    reader.Skip();
                }
            }

            if (ingested > 0) sink.NotifyBatchEnqueued();
            return (ingested, dropped);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(svcBuf);
            ArrayPool<byte>.Shared.Return(tmplBuf);
            ArrayPool<byte>.Shared.Return(trBuf);
            ArrayPool<byte>.Shared.Return(spBuf);
        }
    }

    // ── resourceLogs[] element ─────────────────────────────────────────────────
    private static void ParseResourceLogs(
        ref Utf8JsonReader reader, IOtlpLogSink sink,
        ArrayBufferWriter<byte> resBuf, ArrayBufferWriter<byte> recBuf, ArrayBufferWriter<byte> outBuf,
        byte[] svcBuf, ref byte[] tmplBuf, byte[] trBuf, byte[] spBuf,
        ref int ingested, ref int dropped)
    {
        if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); return; }

        resBuf.Clear();
        int resKeyCount = 0;
        int svcLen      = 0; // >0 ⇒ service.name captured in svcBuf

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }

            if (reader.ValueTextEquals("resource"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                    ParseResourceAttributes(ref reader, resBuf, ref resKeyCount, svcBuf, ref svcLen);
                else
                    reader.Skip();
            }
            else if (reader.ValueTextEquals("scopeLogs"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartArray)
                {
                    while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                        ParseScopeLogs(ref reader, sink, resBuf, resKeyCount, svcBuf, svcLen,
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
        ref Utf8JsonReader reader, ArrayBufferWriter<byte> resBuf, ref int keyCount,
        byte[] svcBuf, ref int svcLen)
    {
        var w = new MessagePackWriter(resBuf);
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (reader.ValueTextEquals("attributes"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    if (WriteKeyValue(ref reader, ref w, svcBuf, ref svcLen)) keyCount++;
            }
            else reader.Skip();
        }
        w.Flush();
    }

    // ── scopeLogs[] element ────────────────────────────────────────────────────
    private static void ParseScopeLogs(
        ref Utf8JsonReader reader, IOtlpLogSink sink,
        ArrayBufferWriter<byte> resBuf, int resKeyCount, byte[] svcBuf, int svcLen,
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
                    bool ok = ParseLogRecord(ref reader, sink, resBuf, resKeyCount, svcBuf, svcLen,
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
        ArrayBufferWriter<byte> resBuf, int resKeyCount, byte[] svcBuf, int svcLen,
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

        recBuf.Clear();
        var w = new MessagePackWriter(recBuf);
        int recKeyCount = 0;
        int svcDummy = 0; // record attrs never capture service.name (that comes from the resource)

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
                    if (WriteKeyValue(ref reader, ref w, svcBuf: null, svcLen: ref svcDummy)) recKeyCount++;
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
        outBuf.Clear();
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
            svcLen > 0 ? svcBuf.AsSpan(0, svcLen) : default);
    }

    // ── KeyValue { "key": "...", "value": { AnyValue } } → msgpack key + value ──
    private static bool WriteKeyValue(ref Utf8JsonReader reader, ref MessagePackWriter w, byte[]? svcBuf, ref int svcLen)
    {
        if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); return false; }

        bool wroteKey = false, isService = false, wrote = false;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }

            if (reader.ValueTextEquals("key"u8))
            {
                reader.Read();
                if (reader.TokenType == JsonTokenType.String)
                {
                    isService = svcBuf is not null && reader.ValueTextEquals("service.name"u8);
                    WriteJsonStringToMsgpack(ref reader, ref w);
                    wroteKey = true;
                }
                else reader.Skip();
            }
            else if (reader.ValueTextEquals("value"u8))
            {
                if (!wroteKey) { reader.Skip(); continue; } // value before key (non-standard) — skip
                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                    WriteAnyValue(ref reader, ref w, isService ? svcBuf : null, ref svcLen);
                else
                    w.WriteNil();
                wrote = true;
            }
            else reader.Skip();
        }

        if (wroteKey && !wrote) w.WriteNil(); // key with no value object
        return wroteKey;
    }

    // ── AnyValue { stringValue | intValue | boolValue | doubleValue | array | kvlist } ──
    private static void WriteAnyValue(ref Utf8JsonReader reader, ref MessagePackWriter w, byte[]? svcBuf, ref int svcLen)
    {
        bool wrote = false;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }

            if (reader.ValueTextEquals("stringValue"u8))
            {
                reader.Read();
                if (svcBuf is not null) svcLen = CopyString(ref reader, svcBuf);
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
                int n = 0, dummy = 0;
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (reader.TokenType == JsonTokenType.StartObject) { WriteAnyValue(ref reader, ref tw, null, ref dummy); n++; }
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
                int n = 0, dummy = 0;
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    if (WriteKeyValue(ref reader, ref tw, null, ref dummy)) n++;
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

    /// <summary>Writes the current JSON string token to msgpack as a str, unescaping if needed.</summary>
    private static void WriteJsonStringToMsgpack(ref Utf8JsonReader reader, ref MessagePackWriter w)
    {
        if (reader.TokenType != JsonTokenType.String) { w.WriteNil(); return; }
        if (!reader.ValueIsEscaped) { w.WriteString(reader.ValueSpan); return; }

        int max = reader.ValueSpan.Length;
        byte[] tmp = ArrayPool<byte>.Shared.Rent(max);
        int n = reader.CopyString(tmp);
        w.WriteString(tmp.AsSpan(0, n));
        ArrayPool<byte>.Shared.Return(tmp);
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
            ArrayPool<byte>.Shared.Return(dest);
            dest = ArrayPool<byte>.Shared.Rent(max);
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
