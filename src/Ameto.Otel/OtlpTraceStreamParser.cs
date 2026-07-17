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
/// <c>OtlpTraceMapper.Map</c> path for the JSON content type (protobuf keeps the DOM path).
///
/// Behaviour is pinned to the DOM path by <c>OtlpTraceStreamingParityTests</c>: identical
/// items (including byte-identical attribute msgpack) for the same body, the same drop
/// rules (missing/invalid ids; outbound CLIENT spans targeting Ameto's own endpoints).
///
/// Assumes standard OTLP document order (<c>resource</c> precedes <c>scopeSpans</c>;
/// a KeyValue's <c>key</c> precedes its <c>value</c>) — true for conformant exporters.
/// Out-of-order input degrades gracefully (missing service name), never corrupts.
/// </summary>
public static class OtlpTraceStreamParser
{
    // Thread-reused scratch: span attribute pairs, and the assembled header+pairs blob.
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tAttr;
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tOut;

    public static List<SpanIngestItem> Parse(ReadOnlySpan<byte> json)
    {
        var reader  = new Utf8JsonReader(json, isFinalBlock: true, state: default);
        var result  = new List<SpanIngestItem>();
        var attrBuf = _tAttr ??= new ArrayBufferWriter<byte>(4096);
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
                    ParseResourceSpans(ref reader, attrBuf, outBuf, result);
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
        ArrayBufferWriter<byte> attrBuf, ArrayBufferWriter<byte> outBuf,
        List<SpanIngestItem> result)
    {
        if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); return; }

        // One string per resource batch, shared by every span under it (same as the DOM path).
        string serviceName = "unknown";

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }

            if (reader.ValueTextEquals("resource"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                    serviceName = ReadResourceServiceName(ref reader) ?? serviceName;
                else reader.Skip();
            }
            else if (reader.ValueTextEquals("scopeSpans"u8))
            {
                if (reader.Read() && reader.TokenType == JsonTokenType.StartArray)
                {
                    while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                        ParseScopeSpans(ref reader, serviceName, attrBuf, outBuf, result);
                }
                else reader.Skip();
            }
            else
            {
                reader.Skip();
            }
        }
    }

    /// <summary>Walks resource.attributes and returns the service.name string value, if any.</summary>
    private static string? ReadResourceServiceName(ref Utf8JsonReader reader)
    {
        string? service = null;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (reader.ValueTextEquals("attributes"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); continue; }
                    bool isService = false;
                    while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
                    {
                        if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
                        if (reader.ValueTextEquals("key"u8))
                        {
                            reader.Read();
                            isService = reader.TokenType == JsonTokenType.String &&
                                        reader.ValueTextEquals("service.name"u8);
                        }
                        else if (reader.ValueTextEquals("value"u8) && isService &&
                                 reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                        {
                            while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
                            {
                                if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
                                if (reader.ValueTextEquals("stringValue"u8))
                                {
                                    reader.Read();
                                    if (reader.TokenType == JsonTokenType.String)
                                        service = reader.GetString();
                                }
                                else reader.Skip();
                            }
                        }
                        else reader.Skip();
                    }
                }
            }
            else reader.Skip();
        }
        return service;
    }

    // ── scopeSpans[] element ───────────────────────────────────────────────────
    private static void ParseScopeSpans(
        ref Utf8JsonReader reader, string serviceName,
        ArrayBufferWriter<byte> attrBuf, ArrayBufferWriter<byte> outBuf,
        List<SpanIngestItem> result)
    {
        if (reader.TokenType != JsonTokenType.StartObject) { reader.Skip(); return; }

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (reader.ValueTextEquals("spans"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    ParseSpan(ref reader, serviceName, attrBuf, outBuf, result);
            }
            else reader.Skip();
        }
    }

    // ── one span → one SpanIngestItem ──────────────────────────────────────────
    private static void ParseSpan(
        ref Utf8JsonReader reader, string serviceName,
        ArrayBufferWriter<byte> attrBuf, ArrayBufferWriter<byte> outBuf,
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

        attrBuf.Clear();
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

        byte[] attrBytes;
        if (attrCount == 0)
        {
            attrBytes = [];
        }
        else
        {
            outBuf.Clear();
            var ow = new MessagePackWriter(outBuf);
            ow.WriteMapHeader(attrCount);
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
        ref short httpStatus, ref bool httpFromNew, ref bool ametoInternal)
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
                    WriteAnyValue(ref reader, ref w, keyKind, ref httpStatus, ref httpFromNew, ref ametoInternal);
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
        KeyKind keyKind, ref short httpStatus, ref bool httpFromNew, ref bool ametoInternal)
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
            byte[] tmp = ArrayPool<byte>.Shared.Rent(reader.ValueSpan.Length);
            int n = reader.CopyString(tmp);
            ametoInternal = ContainsAmetoEndpoint(tmp.AsSpan(0, n));
            ArrayPool<byte>.Shared.Return(tmp);
        }
    }

    private static void CaptureHttpStatus(KeyKind keyKind, long value, ref short httpStatus, ref bool httpFromNew)
    {
        if (keyKind is not (KeyKind.HttpStatusNew or KeyKind.HttpStatusOld)) return;
        if (value is < short.MinValue or > short.MaxValue) return;
        // The new semconv key wins over the old one, regardless of document order.
        if (keyKind == KeyKind.HttpStatusNew) { httpStatus = (short)value; httpFromNew = true; }
        else if (!httpFromNew)                { httpStatus = (short)value; }
    }

    private static bool ContainsAmetoEndpoint(ReadOnlySpan<byte> url) =>
        ContainsAsciiIgnoreCase(url, "/api/events"u8) ||
        ContainsAsciiIgnoreCase(url, "/otlp/v1/"u8);

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

    private static void WriteArrayValue(ref Utf8JsonReader reader, ref MessagePackWriter w)
    {
        short s = 0; bool b = false, a = false;
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
                    if (reader.TokenType == JsonTokenType.StartObject)
                    {
                        WriteAnyValue(ref reader, ref tw, KeyKind.Plain, ref s, ref b, ref a);
                        n++;
                    }
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
        short s = 0; bool b = false, a = false;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) { reader.Skip(); continue; }
            if (reader.ValueTextEquals("values"u8) && reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            {
                var tmp = new ArrayBufferWriter<byte>(256);
                var tw  = new MessagePackWriter(tmp);
                int n = 0;
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    if (WriteSpanKeyValue(ref reader, ref tw, ref s, ref b, ref a)) n++;
                tw.Flush();
                w.WriteMapHeader(n);
                w.WriteRaw(tmp.WrittenSpan);
            }
            else reader.Skip();
        }
    }

    // ── Small helpers ──────────────────────────────────────────────────────────

    /// <summary>Writes the current JSON string token to msgpack as a str, unescaping if needed.</summary>
    private static void WriteJsonStringToMsgpack(ref Utf8JsonReader reader, ref MessagePackWriter w)
    {
        if (reader.TokenType != JsonTokenType.String) { w.WriteNil(); return; }
        if (!reader.ValueIsEscaped) { w.WriteString(reader.ValueSpan); return; }

        byte[] tmp = ArrayPool<byte>.Shared.Rent(reader.ValueSpan.Length);
        int n = reader.CopyString(tmp);
        w.WriteString(tmp.AsSpan(0, n));
        ArrayPool<byte>.Shared.Return(tmp);
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
