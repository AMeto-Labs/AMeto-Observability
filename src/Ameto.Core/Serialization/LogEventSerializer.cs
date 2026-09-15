using System.Buffers;
using System.Buffers.Text;
using System.Text;
using MessagePack;

namespace Ameto.Core.Serialization;

/// <summary>
/// Serialises and deserialises <see cref="LogEvent"/> objects using the
/// CLEF (Compact Log Event Format) field names over MessagePack binary encoding.
///
/// Each event is a MessagePack map where keys are CLEF field names (strings).
/// This matches the Seq wire format, making ingestion clients compatible.
///
/// Hot path (Deserialize) uses ArrayPool and Span — zero heap allocs for
/// the parsing itself; the resulting LogEvent is the only allocation.
/// </summary>
public static class LogEventSerializer
{
    // CLEF ingest scratch, reused per thread (one request per thread at a time):
    // raw property pair bytes and the msgpack map header. Contents are copied into
    // each event's own array before the next event is parsed.
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tRawPairs;
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tHeader;

    private static readonly MessagePackSerializerOptions _options =
        MessagePackSerializerOptions.Standard
            .WithCompression(MessagePackCompression.None);

    // ── Deserialise a single CLEF/msgpack map into a LogEvent ────────────────

    public static LogEvent Deserialize(ReadOnlySequence<byte> sequence, EventId id)
    {
        var reader = new MessagePackReader(sequence);
        return ReadEvent(ref reader, id);
    }

    public static LogEvent Deserialize(ReadOnlySpan<byte> span, EventId id)
    {
        // MessagePackReader needs a Memory/Sequence, the callers hold spans over pooled
        // segment buffers — a copy is unavoidable, but it can live in the pool instead of
        // being a fresh byte[] per event (~110 MB of gen0 churn on the query path in a
        // 7-minute allocation trace). Safe to return in finally: ReadEvent copies every
        // string, id and raw-property slice into the event's own storage.
        byte[] rented = ArrayPool<byte>.Shared.Rent(span.Length);
        try
        {
            span.CopyTo(rented);
            var reader = new MessagePackReader(rented.AsMemory(0, span.Length));
            return ReadEvent(ref reader, id);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    // ── Deserialise a batch (array of maps) ─────────────────────────────────

    public static int DeserializeBatch(
        ReadOnlySequence<byte> sequence,
        uint nodeId,
        ref uint nextSequence,
        IList<LogEvent> output)
    {
        var reader = new MessagePackReader(sequence);
        int count  = 0;

        // Expect msgpack array at top level
        int arrayCount = reader.ReadArrayHeader();

        for (int i = 0; i < arrayCount; i++)
        {
            var id    = new EventId(nodeId, nextSequence++);
            var evt   = ReadEvent(ref reader, id);
            output.Add(evt);
            count++;
        }

        return count;
    }

    // ── Streaming batch read (no LogEvent per event) ─────────────────────────

    /// <summary>
    /// Receives one CLEF event straight off the wire — no <see cref="LogEvent"/>, no
    /// strings except the structured exception. Every span points into the caller's body
    /// buffer (or into per-thread scratch) and is valid only for the duration of the call.
    /// </summary>
    public interface IClefBatchSink
    {
        /// <returns>True when the event was ingested; false when it was dropped.</returns>
        bool TryIngestClef(
            long tsTicks,
            byte level,
            ReadOnlySpan<byte> templateUtf8,
            ExceptionInfo? exception,
            ReadOnlySpan<byte> msgpackProps,
            ulong traceHi, ulong traceLo, ulong spanId,
            ReadOnlySpan<byte> serviceUtf8);
    }

    // Second scratch for the streaming path: map header + raw pairs, contiguous, so the
    // sink gets exactly the bytes DeserializeBatch would have put in RawProperties —
    // without the per-event byte[] that array used to be.
    [ThreadStatic] private static ArrayBufferWriter<byte>? _tPropsOut;

    /// <summary>
    /// Streaming counterpart of <see cref="DeserializeBatch"/>: walks the CLEF array and
    /// hands each event to <paramref name="sink"/> as spans, with zero managed allocation
    /// per event (the only exception is a structured <c>@x</c>, which has to be an object).
    ///
    /// <para>Takes contiguous memory, not a sequence: the ingest body is always one pooled
    /// array, and contiguity is what lets every string be read as a span.</para>
    ///
    /// <para>A body that is not a CLEF array is rejected before any event is seen (the
    /// array header is read first). A body that turns malformed PART WAY THROUGH throws
    /// with the events before that point already handed to the sink — unlike the old
    /// materialise-the-list-first read, which discarded the whole batch. Validating the
    /// whole body up front to restore that costs a second full msgpack walk (measured at
    /// roughly the price of the decode itself), and the events before the fault are
    /// perfectly good ones the caller would otherwise throw away on a status the client
    /// does not retry.</para>
    /// </summary>
    /// <returns>The number of events the sink accepted.</returns>
    /// <exception cref="MessagePackSerializationException">The body is not a well-formed CLEF array.</exception>
    /// <exception cref="EndOfStreamException">The body ends inside an element.</exception>
    public static int StreamBatch(ReadOnlyMemory<byte> body, IClefBatchSink sink, out int dropped)
    {
        var progress = default(ClefBatchProgress);
        StreamBatch(body, sink, ref progress);
        dropped = progress.Dropped;
        return progress.Ingested;
    }

    /// <summary>
    /// How far a batch got. A struct passed by ref, so the counts SURVIVE the exception a
    /// malformed element throws — the caller needs them: events already handed to the sink
    /// are in the ring and their drainer still has to be woken, and an operator reading the
    /// 400 needs to know that some of the batch landed, and where it stopped.
    /// </summary>
    public struct ClefBatchProgress
    {
        /// <summary>Events the sink accepted.</summary>
        public int Ingested;
        /// <summary>Events the sink refused (oversized, or back-pressure).</summary>
        public int Dropped;
        /// <summary>
        /// Index of the element being read — where a throw happened, if one did. -1 while the
        /// array header itself is being read, so a body that is not a CLEF array at all is not
        /// reported the same as one that failed inside element 0.
        /// </summary>
        public int ElementIndex;
        /// <summary>Elements the array header declared. Zero until the header is read.</summary>
        public int ElementCount;
        /// <summary>
        /// True for exactly the duration of the sink call, and left true when the sink throws.
        /// That is how a caller tells whose fault a throw was WITHOUT listing exception types:
        /// set, it came from the sink (the server's side — ring, pool, logger, shutdown); clear,
        /// it came from reading the body, whatever MessagePackReader chose to throw for it.
        /// </summary>
        public bool InSink;
    }

    /// <inheritdoc cref="StreamBatch(ReadOnlyMemory{byte}, IClefBatchSink, out int)"/>
    /// <summary>
    /// As <see cref="StreamBatch(ReadOnlyMemory{byte}, IClefBatchSink, out int)"/>, but
    /// reporting progress through <paramref name="progress"/> so the counts — and
    /// <see cref="ClefBatchProgress.InSink"/> — are readable after a throw.
    /// </summary>
    public static void StreamBatch(ReadOnlyMemory<byte> body, IClefBatchSink sink, ref ClefBatchProgress progress)
    {
        var reader = new MessagePackReader(body);
        progress.ElementIndex = -1;   // the header read below is not "element 0"
        progress.ElementCount = reader.ReadArrayHeader();

        for (int i = 0; i < progress.ElementCount; i++)
        {
            progress.ElementIndex = i;
            if (StreamEvent(ref reader, sink, ref progress)) progress.Ingested++;
            else                                             progress.Dropped++;
        }
    }

    private static bool StreamEvent(ref MessagePackReader reader, IClefBatchSink sink, ref ClefBatchProgress progress)
    {
        var sourceSequence = reader.Sequence;
        int mapCount       = reader.ReadMapHeader();

        ReadOnlySpan<byte> tsUtf8    = default;
        ReadOnlySpan<byte> tmplUtf8  = default;
        ReadOnlySpan<byte> levelUtf8 = default;
        ReadOnlySpan<byte> msgUtf8   = default;   // CLEF @m — template fallback only
        ReadOnlySpan<byte> svcUtf8   = default;
        ExceptionInfo? exception     = null;
        ulong traceIdHi = 0, traceIdLo = 0, spanId = 0;

        ArrayBufferWriter<byte>? rawPropsBuf = null;
        int                      rawPropsCount = 0;

        for (int i = 0; i < mapCount; i++)
        {
            SequencePosition pairStart = reader.Position;

            ClefField field = reader.TryReadStringSpan(out ReadOnlySpan<byte> keySpan)
                ? ClassifyKey(keySpan)
                : ClassifyKey(reader.ReadString()); // rare: nil / non-contiguous key

            switch (field)
            {
                case ClefField.Timestamp:       tsUtf8    = ReadUtf8Value(ref reader); break;
                case ClefField.MessageTemplate: tmplUtf8  = ReadUtf8Value(ref reader); break;
                case ClefField.Level:           levelUtf8 = ReadUtf8Value(ref reader); break;
                case ClefField.Message:         msgUtf8   = ReadUtf8Value(ref reader); break;
                case ClefField.ServiceName:     svcUtf8   = ReadUtf8Value(ref reader); break;
                case ClefField.Exception:
                    exception = ExceptionInfo.Read(ref reader);
                    break;
                case ClefField.TraceId:
                    TraceIdHelper.TryParseTraceId(ReadUtf8Value(ref reader), out traceIdHi, out traceIdLo);
                    break;
                case ClefField.SpanId:
                    TraceIdHelper.TryParseSpanId(ReadUtf8Value(ref reader), out spanId);
                    break;
                default:
                    // Unrecognised key: copy the (key, value) pair verbatim — the bytes that
                    // end up in the ring must be identical to the non-streaming path's.
                    reader.Skip();
                    SequencePosition pairEnd = reader.Position;
                    if (rawPropsBuf is null)
                    {
                        rawPropsBuf = _tRawPairs ??= new ArrayBufferWriter<byte>(256);
                        rawPropsBuf.ResetWrittenCount();
                    }
                    foreach (var segment in sourceSequence.Slice(pairStart, pairEnd))
                        rawPropsBuf.Write(segment.Span);
                    rawPropsCount++;
                    break;
            }
        }

        LogLevel level = LogLevel.Information;
        if (!levelUtf8.IsEmpty)
            TryParseLevelUtf8(levelUtf8, out level);

        long tsTicks = TryParseTimestampUtf8(tsUtf8, out DateTimeOffset parsed)
            ? parsed.UtcTicks
            : DateTimeOffset.UtcNow.UtcTicks;

        // header(N) + raw pairs, contiguous in per-thread scratch — no per-event byte[].
        ReadOnlySpan<byte> props = default;
        if (rawPropsBuf is not null && rawPropsCount > 0)
        {
            var outBuf = _tPropsOut ??= new ArrayBufferWriter<byte>(512);
            outBuf.ResetWrittenCount();
            var hw = new MessagePackWriter(outBuf);
            hw.WriteMapHeader(rawPropsCount);
            hw.Flush();
            outBuf.Write(rawPropsBuf.WrittenSpan);
            props = outBuf.WrittenSpan;
        }

        // CLEF @m fallback: a client that sent only a rendered message gets it as template.
        ReadOnlySpan<byte> template = tmplUtf8.IsEmpty ? msgUtf8 : tmplUtf8;

        // Every read of the body is behind us: from here to the end of the call, a throw is the
        // sink's. Deliberately not cleared in a finally — the caller reads the flag AFTER the
        // exception, and a sink fault must still say so there.
        progress.InSink = true;
        bool accepted = sink.TryIngestClef(
            tsTicks, (byte)level, template, exception, props,
            traceIdHi, traceIdLo, spanId, svcUtf8);
        progress.InSink = false;
        return accepted;
    }

    /// <summary>
    /// Reads a string value as raw UTF-8 with no allocation. Nil yields an empty span
    /// (matching <c>ReadString()</c> returning null); a non-string value throws, exactly as
    /// <c>ReadString()</c> does, so a malformed field still rejects the batch.
    /// </summary>
    private static ReadOnlySpan<byte> ReadUtf8Value(scoped ref MessagePackReader reader)
    {
        if (reader.TryReadStringSpan(out ReadOnlySpan<byte> span)) return span;
        // Nil, or a truncated string (Skip then throws — the batch is rejected). A string
        // spanning segments cannot occur: StreamBatch reads from contiguous memory.
        reader.Skip();
        return default;
    }

    /// <summary>UTF-8 twin of <see cref="LogLevelExtensions.TryParse(ReadOnlySpan{char}, out LogLevel)"/>.</summary>
    private static bool TryParseLevelUtf8(ReadOnlySpan<byte> value, out LogLevel level)
    {
        if (Ascii.EqualsIgnoreCase(value, "Verbose"u8))     { level = LogLevel.Verbose;     return true; }
        if (Ascii.EqualsIgnoreCase(value, "Debug"u8))       { level = LogLevel.Debug;       return true; }
        if (Ascii.EqualsIgnoreCase(value, "Information"u8)) { level = LogLevel.Information; return true; }
        if (Ascii.EqualsIgnoreCase(value, "Info"u8))        { level = LogLevel.Information; return true; }
        if (Ascii.EqualsIgnoreCase(value, "Warning"u8))     { level = LogLevel.Warning;     return true; }
        if (Ascii.EqualsIgnoreCase(value, "Warn"u8))        { level = LogLevel.Warning;     return true; }
        if (Ascii.EqualsIgnoreCase(value, "Error"u8))       { level = LogLevel.Error;       return true; }
        if (Ascii.EqualsIgnoreCase(value, "Fatal"u8))       { level = LogLevel.Fatal;       return true; }
        level = LogLevel.Information;
        return false;
    }

    /// <summary>
    /// Parses a CLEF <c>@t</c> straight from UTF-8. The round-trip ("O") shape that every
    /// .NET client emits goes through <see cref="Utf8Parser"/> — no string, no culture
    /// machinery, ~10x cheaper than the general parser. Anything else (notably the 3-digit
    /// fractional seconds other Seq clients send) falls back to the exact call the
    /// non-streaming path makes, so the set of accepted inputs is unchanged.
    /// </summary>
    private static bool TryParseTimestampUtf8(ReadOnlySpan<byte> value, out DateTimeOffset result)
    {
        if (value.IsEmpty) { result = default; return false; }

        if (Utf8Parser.TryParse(value, out result, out int consumed, 'O') && consumed == value.Length)
            return true;

        // Non-"O" shape: decode and use the general parser, which is what the batch path
        // has always used. Short and rare, so the transient string is acceptable.
        Span<char> chars = value.Length <= 64 ? stackalloc char[64] : new char[value.Length];
        int written = Encoding.UTF8.GetChars(value, chars);
        return DateTimeOffset.TryParse(
            chars[..written], null, System.Globalization.DateTimeStyles.RoundtripKind, out result);
    }



    // ── Serialise a LogEvent back to msgpack (for replication / export) ──────

    public static byte[] Serialize(LogEvent evt)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        var writer = new MessagePackWriter(buffer);

        int propCount = (evt.Properties?.Count ?? 0)
                      + (evt.Exception is not null ? 1 : 0)
                      + 3; // @t, @mt, @l always present

        writer.WriteMapHeader(propCount);

        writer.Write(ClefFields.Timestamp);
        writer.Write(evt.Timestamp.UtcDateTime.ToString("O"));

        writer.Write(ClefFields.Level);
        writer.Write(evt.Level.ToSeqString());

        writer.Write(ClefFields.MessageTemplate);
        writer.Write(evt.MessageTemplate);

        if (evt.Exception is not null)
        {
            writer.Write(ClefFields.Exception);
            evt.Exception.Write(ref writer);
        }

        if (evt.Properties is not null)
        {
            foreach (var (k, v) in evt.Properties)
            {
                writer.Write(k);
                WriteValue(ref writer, v);
            }
        }

        writer.Flush();
        return buffer.WrittenSpan.ToArray();
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /// <summary>Known CLEF field discriminator — lets the parse loop match keys without allocating.</summary>
    private enum ClefField : byte
    {
        Unknown = 0, Timestamp, MessageTemplate, Level, Message, Exception, TraceId, SpanId, ServiceName,
    }

    /// <summary>Classifies a CLEF key from its raw UTF-8 bytes — zero allocation (hot path).</summary>
    private static ClefField ClassifyKey(ReadOnlySpan<byte> key) =>
        key.SequenceEqual("@t"u8)            ? ClefField.Timestamp       :
        key.SequenceEqual("@mt"u8)           ? ClefField.MessageTemplate :
        key.SequenceEqual("@l"u8)            ? ClefField.Level           :
        key.SequenceEqual("@m"u8)            ? ClefField.Message         :
        key.SequenceEqual("@x"u8)            ? ClefField.Exception       :
        key.SequenceEqual("@tr"u8)           ? ClefField.TraceId         :
        key.SequenceEqual("@sp"u8)           ? ClefField.SpanId          :
        key.SequenceEqual("service.name"u8)  ? ClefField.ServiceName     :
        ClefField.Unknown;

    /// <summary>Fallback classifier for the rare non-contiguous-key path.</summary>
    private static ClefField ClassifyKey(string? key) => key switch
    {
        ClefFields.Timestamp       => ClefField.Timestamp,
        ClefFields.MessageTemplate => ClefField.MessageTemplate,
        ClefFields.Level           => ClefField.Level,
        ClefFields.Message         => ClefField.Message,
        ClefFields.Exception       => ClefField.Exception,
        ClefFields.TraceId         => ClefField.TraceId,
        ClefFields.SpanId          => ClefField.SpanId,
        ClefFields.ServiceName     => ClefField.ServiceName,
        _                          => ClefField.Unknown,
    };

    private static LogEvent ReadEvent(ref MessagePackReader reader, EventId id)
    {
        // Capture the underlying sequence so we can slice raw msgpack bytes for
        // user properties without having to re-encode them later.
        var sourceSequence = reader.Sequence;

        int mapCount = reader.ReadMapHeader();

        string? timestamp       = null;
        string? messageTemplate = null;
        string? levelStr        = null;
        string? messageFallback = null;   // CLEF @m — promoted to @mt only if @mt missing
        ExceptionInfo? exception = null;
        ulong   traceIdHi = 0, traceIdLo = 0, spanId = 0;
        string? serviceName = null;

        ArrayBufferWriter<byte>? rawPropsBuf = null;
        int                      rawPropsCount = 0;

        for (int i = 0; i < mapCount; i++)
        {
            // Remember the position right before the key so we can copy the
            // (key, value) pair as a single msgpack-encoded slice if it turns
            // out to be a user property.
            SequencePosition pairStart = reader.Position;

            // Match the field key as UTF-8 bytes without allocating a string.
            // CLEF events carry ~8 fields each; the old `reader.ReadString()` per
            // key allocated a throwaway string for every field of every event —
            // the dominant managed-allocation source on the ingest hot path.
            ClefField field = reader.TryReadStringSpan(out ReadOnlySpan<byte> keySpan)
                ? ClassifyKey(keySpan)
                : ClassifyKey(reader.ReadString()); // rare: non-contiguous key

            switch (field)
            {
                case ClefField.Timestamp:
                    timestamp = reader.ReadString();
                    break;
                case ClefField.MessageTemplate:
                    messageTemplate = reader.ReadString();
                    break;
                case ClefField.Level:
                    levelStr = reader.ReadString();
                    break;
                case ClefField.Message:
                    // CLEF rendered-message — kept only as a fallback in case @mt is absent.
                    messageFallback = reader.ReadString();
                    break;
                case ClefField.Exception:
                    exception = ExceptionInfo.Read(ref reader);
                    break;
                case ClefField.TraceId:
                {
                    string? hex = reader.ReadString();
                    TraceIdHelper.TryParseTraceId(hex, out traceIdHi, out traceIdLo);
                    break;
                }
                case ClefField.SpanId:
                {
                    string? hex = reader.ReadString();
                    TraceIdHelper.TryParseSpanId(hex, out spanId);
                    break;
                }
                case ClefField.ServiceName:
                    serviceName = reader.ReadString();
                    break;
                default:
                    // Skip the value without decoding it; then copy the raw
                    // (key, value) pair bytes straight into rawPropsBuf. This
                    // avoids boxing values into object?, building a dictionary,
                    // and re-serialising them downstream in IngestionEndpoint.
                    reader.Skip();
                    SequencePosition pairEnd = reader.Position;
                    if (rawPropsBuf is null)
                    {
                        // Thread-reused scratch (one request per thread at a time); the
                        // pair bytes are copied into the event's own array below, so
                        // nothing escapes. Avoids an ArrayBufferWriter per event.
                        rawPropsBuf = _tRawPairs ??= new ArrayBufferWriter<byte>(256);
                        rawPropsBuf.Clear();
                    }
                    foreach (var segment in sourceSequence.Slice(pairStart, pairEnd))
                        rawPropsBuf.Write(segment.Span);
                    rawPropsCount++;
                    break;
            }
        }

        LogLevel level = LogLevel.Information;
        if (levelStr is not null)
            LogLevelExtensions.TryParse(levelStr.AsSpan(), out level);

        DateTimeOffset ts = DateTimeOffset.UtcNow;
        if (timestamp is not null &&
            DateTimeOffset.TryParse(timestamp, null,
                System.Globalization.DateTimeStyles.RoundtripKind, out var parsed))
        {
            ts = parsed;
        }

        // Materialise RawProperties as a single msgpack map: header(N) + raw pairs.
        // The combined array is the event's own storage (it outlives the parse); only
        // the header/pair scratch writers are thread-reused.
        ReadOnlyMemory<byte> rawProps = ReadOnlyMemory<byte>.Empty;
        if (rawPropsBuf is not null && rawPropsCount > 0)
        {
            var headerBuf = _tHeader ??= new ArrayBufferWriter<byte>(8);
            headerBuf.Clear();
            var headerWriter = new MessagePackWriter(headerBuf);
            headerWriter.WriteMapHeader(rawPropsCount);
            headerWriter.Flush();

            var combined = new byte[headerBuf.WrittenCount + rawPropsBuf.WrittenCount];
            headerBuf.WrittenSpan.CopyTo(combined);
            rawPropsBuf.WrittenSpan.CopyTo(combined.AsSpan(headerBuf.WrittenCount));
            rawProps = combined;
        }

        // CLEF @m fallback: if the client only sent a rendered message, treat it
        // as the template so the UI has something meaningful to display.
        string finalTemplate = !string.IsNullOrEmpty(messageTemplate)
            ? messageTemplate
            : (messageFallback ?? string.Empty);

        return new LogEvent
        {
            Id              = id,
            Timestamp       = ts,
            Level           = level,
            MessageTemplate = finalTemplate,
            Exception       = exception,
            Properties      = null,
            RawProperties   = rawProps,
            TraceIdHi       = traceIdHi,
            TraceIdLo       = traceIdLo,
            SpanId          = spanId,
            ServiceName     = serviceName,
        };
    }

    private static object? ReadDynamic(ref MessagePackReader reader)
    {
        return reader.NextMessagePackType switch
        {
            MessagePackType.Nil      => ReadNil(ref reader),
            MessagePackType.Boolean  => (object)reader.ReadBoolean(),
            MessagePackType.Integer  => ReadInteger(ref reader),
            MessagePackType.Float    => reader.ReadDouble(),
            MessagePackType.String   => reader.ReadString(),
            MessagePackType.Binary   => reader.ReadBytes()?.ToArray(),
            MessagePackType.Array    => ReadArray(ref reader),
            MessagePackType.Map      => ReadMap(ref reader),
            _                        => SkipAndReturnNull(ref reader),
        };
    }

    private static object? SkipAndReturnNull(ref MessagePackReader reader)
    {
        reader.Skip();
        return null;
    }

    private static object? ReadNil(ref MessagePackReader reader)
    {
        reader.ReadNil();
        return null;
    }

    private static object ReadInteger(ref MessagePackReader reader)
    {
        // Only use ulong for true uint64 values that exceed long.MaxValue.
        // All other integer encodings (fixint, uint8..uint32, int8..int64, negative fixint)
        // are safely representable as long via ReadInt64(), which handles all msgpack codes.
        if (reader.NextCode == MessagePackCode.UInt64)
        {
            ulong u = reader.ReadUInt64();
            return u <= (ulong)long.MaxValue ? (object)(long)u : u;
        }
        return (object)reader.ReadInt64();
    }

    private static object[] ReadArray(ref MessagePackReader reader)
    {
        int count = reader.ReadArrayHeader();
        var arr   = new object[count];
        for (int i = 0; i < count; i++)
            arr[i] = ReadDynamic(ref reader) ?? "<null>";
        return arr;
    }

    public static Dictionary<string, object?>? DeserializePropertiesMap(ReadOnlySpan<byte> span)
    {
        if (span.IsEmpty) return null;
        // Pooled copy instead of ToArray() — same reasoning as Deserialize(span) above;
        // ReadMap materialises strings/boxes, nothing references the source buffer after it.
        byte[] rented = ArrayPool<byte>.Shared.Rent(span.Length);
        try
        {
            span.CopyTo(rented);
            var reader = new MessagePackReader(rented.AsMemory(0, span.Length));
            return ReadMap(ref reader);
        }
        catch { return null; }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Finds ONE top-level key in a properties map and decodes only its value, leaving
    /// every other entry untouched.
    ///
    /// <para>This is the filter's per-event property read. Going through
    /// <see cref="DeserializePropertiesMap"/> built the WHOLE map for it — a dictionary, a
    /// string per key, a box per value, recursively through nested maps and arrays — for
    /// every event a scan touches, when the predicate wanted one value. Here the map is
    /// walked in place over the event's own buffer (no copy: the reader takes the memory
    /// directly), keys are compared as UTF-8 bytes, and values are skipped until the match.</para>
    ///
    /// <para>Ordinal comparison, matching the dictionary's default comparer, so a probe and
    /// a lookup can never disagree about which key is which.</para>
    /// </summary>
    /// <returns>True when the key was present; <paramref name="value"/> is then its decoded value.</returns>
    public static bool TryReadProperty(ReadOnlyMemory<byte> map, ReadOnlySpan<char> key, out object? value)
        => Probe(map, key, decode: true, out value, out _);

    /// <summary>
    /// Presence of a key WITHOUT decoding its value — used where only "is there anything
    /// under this name" matters, so a nested subtree is never built just to be discarded.
    /// </summary>
    public static bool HasProperty(ReadOnlyMemory<byte> map, ReadOnlySpan<char> key, out bool isNil)
        => Probe(map, key, decode: false, out _, out isNil);

    private static bool Probe(
        ReadOnlyMemory<byte> map, ReadOnlySpan<char> key, bool decode, out object? value, out bool isNil)
    {
        value = null;
        isNil = false;
        if (map.IsEmpty) return false;

        int byteCount = Encoding.UTF8.GetByteCount(key);
        // Stack for the names anyone actually writes; heap only for absurd ones, so the
        // probe always runs and the caller never needs a second code path.
        Span<byte> keyUtf8 = byteCount <= 256 ? stackalloc byte[256] : new byte[byteCount];
        keyUtf8 = keyUtf8[..byteCount];
        Encoding.UTF8.GetBytes(key, keyUtf8);

        try
        {
            var reader = new MessagePackReader(map);
            if (reader.NextMessagePackType != MessagePackType.Map) return false;

            int count = reader.ReadMapHeader();

            // LAST occurrence wins, which is why the walk cannot stop at the first hit:
            // the dictionary is built with dict[key] = value, so a repeated key keeps the
            // LAST one, and the ingest path really does repeat keys — OTLP concatenates
            // resource attributes and record attributes into one flat map with no dedup,
            // so anything set at both levels appears twice. Returning the first would make
            // a probe and a lookup disagree about the same event, and which one ran would
            // depend on whether something else had already materialised the map.
            bool found = false;
            MessagePackReader hit = default;   // positioned at the winning VALUE
            for (int i = 0; i < count; i++)
            {
                bool isMatch;
                if (reader.NextMessagePackType == MessagePackType.String)
                    isMatch = ReadKey(ref reader).SequenceEqual(keyUtf8);
                else
                {
                    reader.Skip();           // a key that is not a string cannot match
                    isMatch = false;
                }

                if (isMatch)
                {
                    found = true;
                    hit   = reader;          // struct copy: a bookmark at this value
                    isNil = reader.NextMessagePackType == MessagePackType.Nil;
                }
                reader.Skip();               // step over the value and try the next entry
            }

            if (found)
            {
                if (decode) value = ReadDynamic(ref hit);
                return true;
            }
        }
        catch { /* malformed map — same answer as the full deserialiser: nothing */ }
        return false;
    }

    private static readonly byte[] EmptyKey = [];

    private static ReadOnlySpan<byte> ReadKey(ref MessagePackReader reader)
    {
        if (reader.TryReadStringSpan(out ReadOnlySpan<byte> span)) return span;
        // Rare: the string spans buffer segments. Properties arrive as one contiguous
        // buffer, so this is defensive rather than reachable.
        var seq = reader.ReadStringSequence();
        return seq.HasValue ? seq.Value.ToArray() : EmptyKey;
    }

    private static Dictionary<string, object?> ReadMap(ref MessagePackReader reader)
    {
        int count = reader.ReadMapHeader();
        var dict  = new Dictionary<string, object?>(count);
        for (int i = 0; i < count; i++)
        {
            string k = reader.ReadString() ?? string.Empty;
            dict[k]  = ReadDynamic(ref reader);
        }
        return dict;
    }

    private static void WriteValue(ref MessagePackWriter writer, object? value)
    {
        switch (value)
        {
            case null:                              writer.WriteNil();              break;
            case bool b:                            writer.Write(b);               break;
            case int i:                             writer.Write(i);               break;
            case long l:                            writer.Write(l);               break;
            case double d:                          writer.Write(d);               break;
            case float f:                           writer.Write(f);               break;
            case string s:                          writer.Write(s);               break;
            case byte[] bytes:                      writer.Write(bytes);           break;
            case ulong u:                           writer.Write(u);               break;
            case Dictionary<string, object?> dict:
                writer.WriteMapHeader(dict.Count);
                foreach (var (k, v) in dict) { writer.Write(k); WriteValue(ref writer, v); }
                break;
            case object[] arr:
                writer.WriteArrayHeader(arr.Length);
                foreach (var item in arr) WriteValue(ref writer, item);
                break;
            default:                                writer.Write(value.ToString()); break;
        }
    }
}
