using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using MessagePack;
using Ameto.Core;
using Ameto.Ingestion;

namespace Ameto.Otel;

/// <summary>
/// Parses OTLP/protobuf <c>ExportLogsServiceRequest</c> straight into the ingestion ring —
/// the protobuf counterpart of <see cref="OtlpLogStreamParser"/>, and the encoding SDK
/// exporters and the collector actually send.
///
/// <para>It replaces decode-to-DOM-then-map for this content type. That route cost, per
/// record: a <c>CodedInputStream</c> plus a payload copy for every nested message — body,
/// and a KeyValue AND an AnyValue per attribute, so roughly 20 parser objects and 60
/// allocations on a ten-attribute record (see <see cref="ProtoReader"/>); a throwaway OTLP
/// object graph; a string per key, value, severity text and trace id; and the quiet one, a
/// number→string→number round trip, because the DOM models are shared with OTLP/JSON and
/// type wire integers as <c>string</c> — <c>ReadFixed64().ToString()</c> for the timestamp
/// re-parsed by <c>long.TryParse</c>, and again per int attribute, plus
/// <c>Convert.ToHexString().ToLowerInvariant()</c> per trace and span id, re-parsed back to
/// two ulongs.</para>
///
/// <para>Here every one of those is a span of the request buffer written straight into the
/// <c>[ThreadStatic]</c> msgpack scratch: keys and string values are copied once, from the
/// wire into the properties blob, and nothing else is materialised at all.</para>
///
/// <para>Property order — resource attributes, then record attributes, then <c>@tr</c> and
/// <c>@sp</c> — and the <c>service.name</c> capture (first one wins, string values only,
/// and it stays in the properties map as well) match <c>OtlpLogMapper</c> byte for byte;
/// <c>OtlpLogProtoParityTests</c> pins that. Two deliberate differences, both toward the
/// JSON path: a record with no properties gets an empty msgpack map rather than no bytes,
/// and array / kvlist attribute values are encoded instead of being written as nil — the
/// DOM decoder never modelled those two AnyValue cases, so protobuf clients silently lost
/// them while JSON clients did not.</para>
/// </summary>
public static class OtlpLogProtoParser
{
    // Reused across requests on the same request thread (one request per thread at a time)
    // so a batch allocates no msgpack scratch — the whole point of the streaming path.
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tRes;
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tRec;
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tOut;
    [ThreadStatic] private static byte[]? _tHex;

    /// <summary>
    /// Per-call state. A ref struct so it can hold the service-name span — a slice of the
    /// caller's request buffer — without the parser allocating anything per batch at all.
    /// </summary>
    private ref struct ParseState
    {
        public IOtlpLogSink Sink;
        public ArrayBufferWriter<byte> ResBuf;   // resource attrs (msgpack KV pairs)
        public ArrayBufferWriter<byte> RecBuf;   // record attrs + @tr/@sp (msgpack KV pairs)
        public ArrayBufferWriter<byte> OutBuf;   // assembled map: header + ResBuf + RecBuf
        public byte[] Hex;                       // @tr/@sp hex scratch
        public int ResKeyCount;
        public ReadOnlySpan<byte> Service;       // service.name UTF-8, sliced from the payload
        public bool ServiceSeen;                 // first service.name wins, as the mapper does
        public int Ingested;
        public int Dropped;
    }

    public static (int Ingested, int Dropped) Parse(ReadOnlySpan<byte> payload, IOtlpLogSink sink)
    {
        var st = new ParseState
        {
            Sink   = sink,
            ResBuf = _tRes ??= new ArrayBufferWriter<byte>(4096),
            RecBuf = _tRec ??= new ArrayBufferWriter<byte>(8192),
            OutBuf = _tOut ??= new ArrayBufferWriter<byte>(8192),
            Hex    = _tHex ??= new byte[MaxIdBytes * 2],
        };
        st.ResBuf.ResetWrittenCount();
        st.RecBuf.ResetWrittenCount();
        st.OutBuf.ResetWrittenCount();

        var r = new ProtoReader(payload);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag == 10) ReadResourceLogs(r.ReadLengthDelimited(), ref st);   // field 1
            else r.SkipField(tag);
        }

        if (st.Ingested > 0) sink.NotifyBatchEnqueued();
        return (st.Ingested, st.Dropped);
    }

    /// <summary>
    /// Resource first, then the records: the wire format does not guarantee field order, and
    /// the resource attributes are prepended to every record's property map below.
    /// </summary>
    private static void ReadResourceLogs(ReadOnlySpan<byte> bytes, ref ParseState st)
    {
        st.ResBuf.ResetWrittenCount();
        st.ResKeyCount = 0;
        st.Service     = default;
        st.ServiceSeen = false;

        var pass1 = new ProtoReader(bytes);
        uint tag;
        while ((tag = pass1.ReadTag()) != 0)
        {
            if (tag == 10) ReadResource(pass1.ReadLengthDelimited(), ref st);   // field 1
            else pass1.SkipField(tag);
        }

        var pass2 = new ProtoReader(bytes);
        while ((tag = pass2.ReadTag()) != 0)
        {
            if (tag == 18) ReadScopeLogs(pass2.ReadLengthDelimited(), ref st);  // field 2
            else pass2.SkipField(tag);
        }
    }

    private static void ReadResource(ReadOnlySpan<byte> bytes, ref ParseState st)
    {
        var w = new MessagePackWriter(st.ResBuf);
        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag == 10)                                                       // field 1: attributes
            {
                if (TryWriteKeyValue(r.ReadLengthDelimited(), ref w, ref st, captureService: true))
                    st.ResKeyCount++;
            }
            else r.SkipField(tag);
        }
        w.Flush();
    }

    private static void ReadScopeLogs(ReadOnlySpan<byte> bytes, ref ParseState st)
    {
        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag == 18) ReadLogRecord(r.ReadLengthDelimited(), ref st);       // field 2: log_records
            else r.SkipField(tag);
        }
    }

    // ── one LogRecord → one ring entry ─────────────────────────────────────────

    private static void ReadLogRecord(ReadOnlySpan<byte> bytes, ref ParseState st)
    {
        ulong tsNano  = 0;
        int   sev     = 0;
        ReadOnlySpan<byte> sevText = default;
        ReadOnlySpan<byte> body    = default;
        ReadOnlySpan<byte> traceId = default;
        ReadOnlySpan<byte> spanId  = default;

        st.RecBuf.ResetWrittenCount();
        var w = new MessagePackWriter(st.RecBuf);
        int recKeyCount = 0;

        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 9:  tsNano  = r.ReadFixed64();          break;              // field 1: time_unix_nano
                case 16: sev     = (int)r.ReadVarint();      break;              // field 2: severity_number
                case 26: sevText = r.ReadLengthDelimited();  break;              // field 3: severity_text
                case 42: body    = r.ReadLengthDelimited();  break;              // field 5: body
                case 50:                                                         // field 6: attributes
                    if (TryWriteKeyValue(r.ReadLengthDelimited(), ref w, ref st, captureService: false))
                        recKeyCount++;
                    break;
                case 74: traceId = r.ReadLengthDelimited(); break;               // field 9: trace_id
                case 82: spanId  = r.ReadLengthDelimited(); break;               // field 10: span_id
                default: r.SkipField(tag); break;
            }
        }

        // @tr / @sp, from the raw bytes — the mapper wrote lowercase hex of whatever the
        // client sent, including a non-conformant length, so that is what goes in the map.
        if (!traceId.IsEmpty && traceId.Length <= MaxIdBytes) { WriteHex(ref w, "@tr"u8, traceId, st.Hex); recKeyCount++; }
        if (!spanId.IsEmpty  && spanId.Length  <= MaxIdBytes) { WriteHex(ref w, "@sp"u8, spanId,  st.Hex); recKeyCount++; }
        w.Flush();

        // Assemble the final msgpack map: header(total) + resource KV bytes + record KV bytes.
        st.OutBuf.ResetWrittenCount();
        var ow = new MessagePackWriter(st.OutBuf);
        ow.WriteMapHeader(st.ResKeyCount + recKeyCount);
        if (st.ResKeyCount > 0) ow.WriteRaw(st.ResBuf.WrittenSpan);
        if (recKeyCount   > 0) ow.WriteRaw(st.RecBuf.WrittenSpan);
        ow.Flush();

        // The correlation columns take the ids as numbers; only conformant lengths parse,
        // which is exactly what TryParseTraceId's 32-hex-character check did.
        ulong trHi = 0, trLo = 0, sp = 0;
        if (traceId.Length == 16)
        {
            trHi = BinaryPrimitives.ReadUInt64BigEndian(traceId);
            trLo = BinaryPrimitives.ReadUInt64BigEndian(traceId[8..]);
        }
        if (spanId.Length == 8) sp = BinaryPrimitives.ReadUInt64BigEndian(spanId);

        // A fixed64 past long.MaxValue is not a timestamp; the mapper's long.TryParse of the
        // stringified value failed on it and fell through to "now", so this does too.
        long nanos   = tsNano <= long.MaxValue ? (long)tsNano : 0;
        long tsTicks = nanos > 0
            ? DateTimeOffset.FromUnixTimeMilliseconds(nanos / 1_000_000).UtcTicks
            : DateTimeOffset.UtcNow.UtcTicks;

        bool ok = st.Sink.TryIngestRaw(
            tsTicks, (byte)MapSeverity(sev, sevText),
            ReadBodyString(body),
            st.OutBuf.WrittenSpan,
            trHi, trLo, sp,
            st.Service);

        if (ok) st.Ingested++; else st.Dropped++;
    }

    /// <summary>
    /// The message template: <c>body.string_value</c> and nothing else. A body carrying any
    /// other AnyValue case has no string to be a template, and the mapper's
    /// <c>Body?.StringValue ?? string.Empty</c> produced an empty one for it.
    /// </summary>
    private static ReadOnlySpan<byte> ReadBodyString(ReadOnlySpan<byte> body)
    {
        if (body.IsEmpty) return default;
        var r = new ProtoReader(body);
        ReadOnlySpan<byte> s = default;
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag == 10) s = r.ReadLengthDelimited();                          // field 1: string_value
            else r.SkipField(tag);
        }
        return s;
    }

    // ── KeyValue / AnyValue ───────────────────────────────────────────────────

    /// <summary>
    /// Writes one KeyValue as a msgpack key + value pair. Returns false — writing nothing —
    /// when the message carries no key, which is the mapper's <c>kv.Key is not null</c> test
    /// and the reason the map header is counted the same way.
    ///
    /// <para>Two passes over the (tiny) KeyValue rather than one, because the wire format
    /// allows the value to precede the key and the msgpack pair cannot.</para>
    /// </summary>
    private static bool TryWriteKeyValue(
        ReadOnlySpan<byte> bytes, ref MessagePackWriter w, ref ParseState st, bool captureService)
    {
        ReadOnlySpan<byte> key = default, value = default;
        bool haveKey = false, haveValue = false;

        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: key   = r.ReadLengthDelimited(); haveKey   = true; break;  // field 1: key
                case 18: value = r.ReadLengthDelimited(); haveValue = true; break;  // field 2: value
                default: r.SkipField(tag); break;
            }
        }
        if (!haveKey) return false;

        // service.name: the FIRST one decides, string values only, and it is NOT removed from
        // the property map — all three are the mapper's behaviour, and the JSON path's.
        if (captureService && !st.ServiceSeen && key.SequenceEqual("service.name"u8))
        {
            st.ServiceSeen = true;
            st.Service     = StringValueOf(value);
        }

        w.WriteString(key);
        if (haveValue) WriteAnyValue(value, ref w, ref st);
        else w.WriteNil();
        return true;
    }

    /// <summary>The <c>string_value</c> of an AnyValue, or empty when it is any other case.</summary>
    private static ReadOnlySpan<byte> StringValueOf(ReadOnlySpan<byte> bytes)
    {
        var r = new ProtoReader(bytes);
        ReadOnlySpan<byte> s = default;
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag == 10) s = r.ReadLengthDelimited();
            else r.SkipField(tag);
        }
        return s;
    }

    /// <summary>
    /// One AnyValue → one msgpack value. The oneof is scanned first and written afterwards so
    /// that a message with more than one case set resolves in the mapper's order (string,
    /// bool, double, int, array, kvlist) rather than in wire order.
    /// </summary>
    private static void WriteAnyValue(ReadOnlySpan<byte> bytes, ref MessagePackWriter w, ref ParseState st)
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
                case 10: str     = r.ReadLengthDelimited();  haveStr  = true; break;  // 1: string_value
                case 16: boolVal = r.ReadVarint() != 0;      haveBool = true; break;  // 2: bool_value
                case 24: intVal  = (long)r.ReadVarint();     haveInt  = true; break;  // 3: int_value
                case 33: dblVal  = r.ReadDouble();           haveDbl  = true; break;  // 4: double_value
                case 42: array   = r.ReadLengthDelimited();  haveArr  = true; break;  // 5: array_value
                case 50: kvlist  = r.ReadLengthDelimited();  haveKvl  = true; break;  // 6: kvlist_value
                default: r.SkipField(tag); break;                                     // 7: bytes_value → nil
            }
        }

        if (haveStr)       w.WriteString(str);
        else if (haveBool) w.Write(boolVal);
        else if (haveDbl)  w.Write(dblVal);
        else if (haveInt)  w.Write(intVal);
        else if (haveArr)  WriteArrayValue(array, ref w, ref st);
        else if (haveKvl)  WriteKvlistValue(kvlist, ref w, ref st);
        else               w.WriteNil();
    }

    /// <summary>
    /// ArrayValue → msgpack array. msgpack needs the element count before the elements, so
    /// the values are counted in one pass and written in a second — a re-walk of a span
    /// already in L1, rather than the JSON parser's buffer-then-splice, which needs a
    /// scratch buffer because a <c>Utf8JsonReader</c> cannot be rewound.
    /// </summary>
    private static void WriteArrayValue(ReadOnlySpan<byte> bytes, ref MessagePackWriter w, ref ParseState st)
    {
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
            if (tag == 10) WriteAnyValue(r.ReadLengthDelimited(), ref w, ref st);
            else r.SkipField(tag);
        }
    }

    /// <summary>KvlistValue → msgpack map, counting only the entries that carry a key.</summary>
    private static void WriteKvlistValue(ReadOnlySpan<byte> bytes, ref MessagePackWriter w, ref ParseState st)
    {
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
            if (tag == 10) TryWriteKeyValue(r.ReadLengthDelimited(), ref w, ref st, captureService: false);
            else r.SkipField(tag);
        }
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

    /// <summary>Longest id the stack hex buffer below covers; conformant ones are 16 and 8.</summary>
    private const int MaxIdBytes = 32;

    private static ReadOnlySpan<byte> HexDigits => "0123456789abcdef"u8;

    /// <summary>
    /// Writes <paramref name="key"/> → lowercase hex of <paramref name="raw"/>.
    ///
    /// <para>The scratch is the caller's thread-static array rather than a <c>stackalloc</c>:
    /// <c>MessagePackWriter</c> is a ref struct passed by ref, so ref-safety has to assume a
    /// span handed to it could be stored in it, and a stack buffer is refused outright. A
    /// per-thread array costs nothing per request and satisfies the rule.</para>
    /// </summary>
    private static void WriteHex(ref MessagePackWriter w, ReadOnlySpan<byte> key, ReadOnlySpan<byte> raw, byte[] scratch)
    {
        var digits = HexDigits;
        for (int i = 0; i < raw.Length; i++)
        {
            scratch[i * 2]     = digits[raw[i] >> 4];
            scratch[i * 2 + 1] = digits[raw[i] & 0x0F];
        }
        w.WriteString(key);
        w.WriteString(scratch.AsSpan(0, raw.Length * 2));
    }

    /// <summary>
    /// OTLP severity → Ameto level, matching <c>OtlpLogMapper.MapSeverity</c> exactly:
    /// the number decides whenever it is set, and the text is the fallback — prefix match
    /// for the levels with no synonyms, whole-word for WARN/INFO, which have one each.
    /// </summary>
    private static LogLevel MapSeverity(int severityNumber, ReadOnlySpan<byte> severityText)
    {
        if (severityNumber >= 21) return LogLevel.Fatal;
        if (severityNumber >= 17) return LogLevel.Error;
        if (severityNumber >= 13) return LogLevel.Warning;
        if (severityNumber >= 9)  return LogLevel.Information;
        if (severityNumber >= 5)  return LogLevel.Debug;
        if (severityNumber >= 1)  return LogLevel.Verbose;

        if (severityText.IsEmpty) return LogLevel.Information;
        if (StartsWithFold(severityText, "FATAL"u8)) return LogLevel.Fatal;
        if (StartsWithFold(severityText, "ERROR"u8)) return LogLevel.Error;
        if (Ascii.EqualsIgnoreCase(severityText, "WARN"u8) ||
            Ascii.EqualsIgnoreCase(severityText, "WARNING"u8)) return LogLevel.Warning;
        if (Ascii.EqualsIgnoreCase(severityText, "INFO"u8) ||
            Ascii.EqualsIgnoreCase(severityText, "INFORMATION"u8)) return LogLevel.Information;
        if (StartsWithFold(severityText, "DEBUG"u8)) return LogLevel.Debug;
        if (StartsWithFold(severityText, "TRACE"u8)) return LogLevel.Verbose;
        return LogLevel.Information;
    }

    private static bool StartsWithFold(ReadOnlySpan<byte> text, ReadOnlySpan<byte> prefix)
        => text.Length >= prefix.Length && Ascii.EqualsIgnoreCase(text[..prefix.Length], prefix);
}
