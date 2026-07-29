using System.Buffers;   // BuffersExtensions.ToArray for the rare multi-segment string
using System.Text.Json;
using MessagePack;

namespace Ameto.Core.Serialization;

/// <summary>
/// Writes a msgpack property map straight into a <see cref="Utf8JsonWriter"/>, without
/// building the <c>Dictionary&lt;string, object?&gt;</c> in between.
///
/// <para>The query path used to decode every event's properties into a dictionary of
/// boxed values, nested dictionaries and lists — and then hand it to System.Text.Json,
/// which walked it straight back out as text. A CPU profile of the stand under log
/// scrolling put that round trip at ~25% of the process, and the allocations it produced
/// behind most of the GC polling on top of it. Nothing ever read the dictionary; it
/// existed only to be re-serialised.</para>
///
/// <para>Output must match <c>DynamicObjectConverter</c> exactly, quirks included:
/// a nil INSIDE an array becomes the string <c>"&lt;null&gt;"</c> (the old reader
/// substituted it while materialising), while a nil map VALUE stays JSON null; and a
/// msgpack binary value renders as <c>"System.Byte[]"</c>, because the old path decoded
/// it to <c>byte[]</c> which the converter had no case for and fell through to
/// <c>ToString()</c>. Both are pinned by parity tests — behaviour is preserved here, not
/// corrected, so this stays a pure performance change.</para>
/// </summary>
public static class MsgPackJsonTranscoder
{
    /// <summary>The literal the old materialiser substituted for a nil array element.</summary>
    private static ReadOnlySpan<byte> NullPlaceholder => "<null>"u8;
    /// <summary>What <c>byte[].ToString()</c> produced when the converter fell through.</summary>
    private static ReadOnlySpan<byte> BinaryPlaceholder => "System.Byte[]"u8;

    /// <summary>
    /// Writes <paramref name="msgpack"/> — expected to be a map — as a JSON object.
    /// Writes an empty object when the payload is empty or not a map, which is what the
    /// dictionary path produced for the same input.
    /// </summary>
    public static void WriteMap(Utf8JsonWriter writer, ReadOnlyMemory<byte> msgpack)
    {
        if (msgpack.IsEmpty) { writer.WriteStartObject(); writer.WriteEndObject(); return; }

        // Memory, not Span: MessagePackReader keeps the buffer, and taking a span here
        // would force a defensive copy of the very payload this class exists to avoid.
        var reader = new MessagePackReader(msgpack);
        if (reader.NextMessagePackType != MessagePackType.Map)
        {
            writer.WriteStartObject();
            writer.WriteEndObject();
            return;
        }
        WriteMapCore(writer, ref reader);
    }

    private static void WriteMapCore(Utf8JsonWriter writer, ref MessagePackReader reader)
    {
        int count = reader.ReadMapHeader();
        writer.WriteStartObject();
        for (int i = 0; i < count; i++)
        {
            WritePropertyName(writer, ref reader);
            WriteValue(writer, ref reader, insideArray: false);
        }
        writer.WriteEndObject();
    }

    private static void WritePropertyName(Utf8JsonWriter writer, ref MessagePackReader reader)
    {
        if (reader.NextMessagePackType == MessagePackType.String)
        {
            // Write the UTF-8 bytes through — no string materialised for the key.
            var seq = reader.ReadStringSequence();
            if (seq is { } s)
            {
                if (s.IsSingleSegment) writer.WritePropertyName(s.First.Span);
                else                   writer.WritePropertyName(s.ToArray());
                return;
            }
            writer.WritePropertyName(ReadOnlySpan<byte>.Empty);
            return;
        }

        // Non-string key: msgpack allows it, the dictionary path stringified it.
        object? key = ReadScalarAsObject(ref reader);
        writer.WritePropertyName(key?.ToString() ?? string.Empty);
    }

    private static void WriteValue(Utf8JsonWriter writer, ref MessagePackReader reader, bool insideArray)
    {
        switch (reader.NextMessagePackType)
        {
            case MessagePackType.Nil:
                reader.ReadNil();
                // Array elements were substituted with "<null>" while materialising;
                // map values kept a real null.
                if (insideArray) writer.WriteStringValue(NullPlaceholder);
                else             writer.WriteNullValue();
                break;

            case MessagePackType.Boolean:
                writer.WriteBooleanValue(reader.ReadBoolean());
                break;

            case MessagePackType.Integer:
                // Mirrors LogEventSerializer.ReadInteger: only a true uint64 above
                // long.MaxValue stays unsigned; everything else goes through Int64.
                if (reader.NextCode == MessagePackCode.UInt64)
                {
                    ulong u = reader.ReadUInt64();
                    if (u <= long.MaxValue) writer.WriteNumberValue((long)u);
                    else                    writer.WriteNumberValue(u);
                }
                else writer.WriteNumberValue(reader.ReadInt64());
                break;

            case MessagePackType.Float:
                writer.WriteNumberValue(reader.ReadDouble());
                break;

            case MessagePackType.String:
            {
                var seq = reader.ReadStringSequence();
                if (seq is { } s)
                {
                    if (s.IsSingleSegment) writer.WriteStringValue(s.First.Span);
                    else                   writer.WriteStringValue(s.ToArray());
                }
                else writer.WriteNullValue();
                break;
            }

            case MessagePackType.Binary:
                reader.Skip();
                writer.WriteStringValue(BinaryPlaceholder);
                break;

            case MessagePackType.Array:
            {
                int n = reader.ReadArrayHeader();
                writer.WriteStartArray();
                for (int i = 0; i < n; i++) WriteValue(writer, ref reader, insideArray: true);
                writer.WriteEndArray();
                break;
            }

            case MessagePackType.Map:
                WriteMapCore(writer, ref reader);
                break;

            default:
                // Extension / unknown: the old path skipped it and yielded null.
                reader.Skip();
                if (insideArray) writer.WriteStringValue(NullPlaceholder);
                else             writer.WriteNullValue();
                break;
        }
    }

    /// <summary>Reads one scalar for the rare non-string map key.</summary>
    private static object? ReadScalarAsObject(ref MessagePackReader reader) =>
        reader.NextMessagePackType switch
        {
            MessagePackType.Integer => reader.ReadInt64(),
            MessagePackType.Boolean => reader.ReadBoolean(),
            MessagePackType.Float   => reader.ReadDouble(),
            _                       => SkipNull(ref reader),
        };

    private static object? SkipNull(ref MessagePackReader reader)
    {
        reader.Skip();
        return null;
    }
}
