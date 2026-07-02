using System.Buffers;
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
        var sequence = new ReadOnlySequence<byte>(span.ToArray()); // only alloc here
        return Deserialize(sequence, id);
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
                    rawPropsBuf ??= new ArrayBufferWriter<byte>(64);
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
        ReadOnlyMemory<byte> rawProps = ReadOnlyMemory<byte>.Empty;
        if (rawPropsBuf is not null && rawPropsCount > 0)
        {
            var headerBuf = new ArrayBufferWriter<byte>(8);
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
        try
        {
            var reader = new MessagePackReader(new System.Buffers.ReadOnlySequence<byte>(span.ToArray()));
            return ReadMap(ref reader);
        }
        catch { return null; }
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
