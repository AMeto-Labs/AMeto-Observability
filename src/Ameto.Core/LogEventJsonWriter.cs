using System.Buffers;
using System.Text.Json;
using Ameto.Core.Serialization;
using MessagePack;

namespace Ameto.Core;

/// <summary>
/// Writes a <see cref="LogEvent"/> to a <see cref="Utf8JsonWriter"/> as the CLEF-shaped
/// object the events SSE stream has always emitted — WITHOUT the DTO in between.
///
/// <para>The old road was <c>LogEventDto.From(ev)</c> followed by reflection-based
/// <c>JsonSerializer.Serialize</c>. Per returned row that cost a DTO, an
/// <c>ExceptionInfoDto</c> tree copy, and five strings that exist only to be copied into
/// the output and dropped: <c>Timestamp.ToString("O")</c>, <c>Id.ToString()</c> and the two
/// interpolated hex ids. Formatting straight into stack buffers removes all of them; the
/// msgpack properties already went out through <see cref="MsgPackJsonTranscoder"/> and
/// still do.</para>
///
/// <para>THE BYTES ARE THE CONTRACT. Field order, the <c>"O"</c> timestamp, the decimal
/// id, lowercase hex trace/span ids, the omission of every absent field, and the
/// <c>type/message/stack/inner</c> exception tree are all exactly what the DTO produced
/// under <c>DefaultIgnoreCondition.WhenWritingNull</c>. Field names carry explicit
/// <c>JsonPropertyName</c>s over there, so the camelCase naming policy never applied to
/// them and does not apply here either. The equality is pinned by
/// <c>LogEventJsonParityTests</c>, which serialises the same events both ways — through a
/// frozen copy of the DTO road, which no stream in the server takes any more — compares the
/// bytes, compares a live tail's whole stream frame for frame, and checks golden frames.</para>
///
/// <para>ONE DELIBERATE MOVE since: the service goes out as <c>@service</c>, not
/// <c>service.name</c> — it became a built-in field with that name (see
/// <see cref="ClefFields.ServiceName"/>). The frozen DTO and the golden frames moved with it, and
/// in the same place: the key still follows <c>@sp</c> and precedes <c>props</c>.</para>
/// </summary>
public static class LogEventJsonWriter
{
    private static readonly JsonEncodedText NameTimestamp   = JsonEncodedText.Encode("@t");
    private static readonly JsonEncodedText NameTemplate    = JsonEncodedText.Encode("@mt");
    private static readonly JsonEncodedText NameLevel       = JsonEncodedText.Encode("@l");
    private static readonly JsonEncodedText NameException   = JsonEncodedText.Encode("@x");
    private static readonly JsonEncodedText NameId          = JsonEncodedText.Encode("id");
    private static readonly JsonEncodedText NameTraceId     = JsonEncodedText.Encode("@tr");
    private static readonly JsonEncodedText NameSpanId      = JsonEncodedText.Encode("@sp");
    private static readonly JsonEncodedText NameServiceName = JsonEncodedText.Encode(ClefFields.ServiceName);   // "@service"
    private static readonly JsonEncodedText NameProps       = JsonEncodedText.Encode("props");

    private static readonly JsonEncodedText NameExType    = JsonEncodedText.Encode("type");
    private static readonly JsonEncodedText NameExMessage = JsonEncodedText.Encode("message");
    private static readonly JsonEncodedText NameExStack   = JsonEncodedText.Encode("stack");
    private static readonly JsonEncodedText NameExInner   = JsonEncodedText.Encode("inner");

    // The six level names are a closed set, so they are escaped once at start-up rather
    // than re-escaped per row. Index is the LogLevel value; anything outside the range
    // falls back to Information, exactly as ToSeqString does.
    private static readonly JsonEncodedText[] LevelNames =
    [
        JsonEncodedText.Encode("Verbose"),
        JsonEncodedText.Encode("Debug"),
        JsonEncodedText.Encode("Information"),
        JsonEncodedText.Encode("Warning"),
        JsonEncodedText.Encode("Error"),
        JsonEncodedText.Encode("Fatal"),
    ];

    /// <summary>Writes one event as a complete JSON object.</summary>
    public static void Write(Utf8JsonWriter writer, LogEvent ev)
    {
        writer.WriteStartObject();

        // "O" on a DateTimeOffset is 33 chars at most ("yyyy-MM-ddTHH:mm:ss.fffffff+hh:mm").
        // It has to be the round-trip format spelled out: WriteString(name, DateTimeOffset)
        // trims trailing zeros out of the fraction, which the client's parser tolerates but
        // the byte-equality contract does not.
        Span<char> scratch = stackalloc char[40];
        ev.Timestamp.TryFormat(scratch, out int written, "O", null);
        writer.WriteString(NameTimestamp, scratch[..written]);

        writer.WriteString(NameTemplate, ev.MessageTemplate);

        int lvl = (int)ev.Level;
        writer.WriteString(NameLevel, (uint)lvl < (uint)LevelNames.Length ? LevelNames[lvl] : LevelNames[2]);

        // From the bytes wherever the event carries them (a segment row, a hot-tier row, an ingest
        // body), and from the tree only where something already built it — or set it directly.
        // Delivery used to build the tree for every returned row to walk it once: the node chain
        // and a UTF-16 copy of every stack frame, 3.5 KB per row, for bytes that go out as they are.
        if (ev.ExceptionMaterialised)
        {
            if (ev.Exception is { } x)
            {
                writer.WritePropertyName(NameException);
                WriteException(writer, x);
            }
        }
        else if (!ev.RawException.IsEmpty)
        {
            var reader = new MessagePackReader(ev.RawException);
            WriteException(writer, ref reader, NameException, depth: 1);
        }

        ev.Id.RawValue.TryFormat(scratch, out written, default, null);
        writer.WriteString(NameId, scratch[..written]);

        if ((ev.TraceIdHi | ev.TraceIdLo) != 0)
        {
            Span<char> hex = stackalloc char[32];
            ev.TraceIdHi.TryFormat(hex,       out _, "x16", null);
            ev.TraceIdLo.TryFormat(hex[16..], out _, "x16", null);
            writer.WriteString(NameTraceId, hex);
        }

        if (ev.SpanId != 0)
        {
            Span<char> hex = stackalloc char[16];
            ev.SpanId.TryFormat(hex, out _, "x16", null);
            writer.WriteString(NameSpanId, hex);
        }

        if (ev.ServiceName is { } service)
            writer.WriteString(NameServiceName, service);

        // Raw first, and only then the dictionary: reading ev.Properties would materialise
        // the map this path exists to avoid. Decoders that produce a dictionary directly
        // (ingest-side tests, hand-built events) still go out the second door.
        if (!ev.RawProperties.IsEmpty)
        {
            writer.WritePropertyName(NameProps);
            MsgPackJsonTranscoder.WriteMap(writer, ev.RawProperties);
        }
        else if (ev.Properties is { } map)
        {
            writer.WritePropertyName(NameProps);
            WriteDynamicMap(writer, map);
        }

        writer.WriteEndObject();
    }

    private static void WriteException(Utf8JsonWriter writer, ExceptionInfo x)
    {
        writer.WriteStartObject();
        writer.WriteString(NameExType, x.Type);
        if (x.Message    is { } m) writer.WriteString(NameExMessage, m);
        if (x.StackTrace is { } s) writer.WriteString(NameExStack,   s);
        if (x.Inner      is { } i)
        {
            writer.WritePropertyName(NameExInner);
            WriteException(writer, i);
        }
        writer.WriteEndObject();
    }

    /// <summary>The type an exception map takes when it carries none — <c>ExceptionInfo.ReadAtDepth</c>'s default.</summary>
    private static readonly JsonEncodedText DefaultExType = JsonEncodedText.Encode("Exception");

    /// <summary>
    /// The exception property straight from its msgpack: byte for byte what
    /// <see cref="WriteException(Utf8JsonWriter, ExceptionInfo)"/> writes for
    /// <see cref="ExceptionInfo.FromBytes(ReadOnlyMemory{byte})"/> of the same bytes, with no tree in
    /// between. Nothing is written — not even <paramref name="name"/> — where the value decodes to
    /// no exception (nil, an empty legacy string, a value that is neither string nor map), which is
    /// how an absent <c>inner</c> stays absent.
    ///
    /// <para>Each branch mirrors one of <c>ExceptionInfo.ReadAtDepth</c>: the fields go out in the
    /// tree's order whatever order the map holds them in, a repeated key keeps its last value, a
    /// <c>type</c> that is absent or nil is <c>"Exception"</c>, a nil <c>msg</c> or <c>stk</c> is
    /// omitted, a legacy string is <c>{type: "Exception", message}</c>, and an <c>inner</c> below
    /// <see cref="ExceptionInfo.MaxDepth"/> is dropped. A value that is not a string where the tree
    /// read one throws what the tree's decode threw, from inside the object — the SSE writer
    /// discards the frame it was composing, and the list endpoint fails the response, as either did
    /// when the throw came out of <see cref="LogEvent.Exception"/>. <c>ExceptionJsonWriterTests</c>
    /// pins the two roads against each other over every shape.</para>
    /// </summary>
    private static void WriteException(Utf8JsonWriter writer, ref MessagePackReader reader, JsonEncodedText name, int depth)
    {
        if (reader.TryReadNil()) return;

        if (reader.NextMessagePackType == MessagePackType.String)
        {
            if (!reader.TryReadStringSpan(out ReadOnlySpan<byte> legacy))
            {
                // A string across segments cannot occur over one memory; kept exact for the tree's sake.
                var seq = reader.ReadStringSequence();
                if (seq is not { IsEmpty: false } s) return;
                legacy = s.ToArray();
            }
            if (legacy.IsEmpty) return;
            writer.WritePropertyName(name);
            writer.WriteStartObject();
            writer.WriteString(NameExType, DefaultExType);
            // Ill-formed UTF-8 goes the tree's road, as in WriteStringField.
            if (System.Text.Unicode.Utf8.IsValid(legacy)) writer.WriteString(NameExMessage, legacy);
            else                                          writer.WriteString(NameExMessage, System.Text.Encoding.UTF8.GetString(legacy));
            writer.WriteEndObject();
            return;
        }

        if (reader.NextMessagePackType != MessagePackType.Map)
        {
            reader.Skip();
            return;
        }

        // One pass to find the LAST value of each key (bookmarks into the same memory), then the
        // fields in the tree's order: the map's own order is the client's and carries no meaning.
        int fields = reader.ReadMapHeader();
        MessagePackReader typeAt = default, msgAt = default, stkAt = default, innerAt = default;
        bool hasType = false, hasMsg = false, hasStk = false, hasInner = false;
        for (int i = 0; i < fields; i++)
        {
            ExceptionInfo.ExcField field = reader.TryReadStringSpan(out ReadOnlySpan<byte> key)
                ? ExceptionInfo.ClassifyKey(key)
                : ExceptionInfo.ClassifyKey(reader.ReadString());
            switch (field)
            {
                case ExceptionInfo.ExcField.Type:    typeAt  = reader; hasType  = true; break;
                case ExceptionInfo.ExcField.Message: msgAt   = reader; hasMsg   = true; break;
                case ExceptionInfo.ExcField.Stack:   stkAt   = reader; hasStk   = true; break;
                case ExceptionInfo.ExcField.Inner:   innerAt = reader; hasInner = true; break;
            }
            reader.Skip();
        }

        writer.WritePropertyName(name);
        writer.WriteStartObject();
        if (hasType && !typeAt.TryReadNil()) WriteStringField(writer, NameExType, ref typeAt);
        else                                 writer.WriteString(NameExType, DefaultExType);
        if (hasMsg && !msgAt.TryReadNil())   WriteStringField(writer, NameExMessage, ref msgAt);
        if (hasStk && !stkAt.TryReadNil())   WriteStringField(writer, NameExStack,   ref stkAt);
        if (hasInner && depth < ExceptionInfo.MaxDepth)
            WriteException(writer, ref innerAt, NameExInner, depth + 1);
        writer.WriteEndObject();
    }

    /// <summary>
    /// A string field from its UTF-8 in place, where the writer escapes it exactly as it escapes the
    /// <see cref="string"/> the tree held — for well-formed UTF-8. Ill-formed bytes take the tree's
    /// own road (<c>ReadString</c>, which replaces them with U+FFFD, then the string overload),
    /// because the writer's UTF-8 overload escapes the replacement it makes (<c>�</c>) where the
    /// relaxed encoder leaves a replacement character in a string as it is, and the bytes on the
    /// wire must not depend on which road a row took. <c>ExceptionBytesContractTests</c> pins both.
    /// A value that is not a string throws out of <c>ReadString</c>, as the tree's decode did.
    /// </summary>
    private static void WriteStringField(Utf8JsonWriter writer, JsonEncodedText name, ref MessagePackReader at)
    {
        if (!at.TryReadStringSpan(out ReadOnlySpan<byte> utf8))
        {
            writer.WriteString(name, at.ReadString());   // nil, a value that is not a string (throws), a string across segments
            return;
        }
        if (System.Text.Unicode.Utf8.IsValid(utf8)) writer.WriteString(name, utf8);
        else                                        writer.WriteString(name, System.Text.Encoding.UTF8.GetString(utf8));
    }

    /// <summary>
    /// The already-materialised property map. Mirrors the DynamicObjectConverter the DTO road
    /// serialised through (gone from the server; its copy is kept beside the parity tests) case
    /// for case — the concrete types <c>LogEventSerializer</c> produces — because both shapes
    /// have to leave the same bytes on the wire.
    /// </summary>
    public static void WriteDynamicMap(Utf8JsonWriter writer, Dictionary<string, object?> map)
    {
        writer.WriteStartObject();
        foreach (var (k, v) in map)
        {
            writer.WritePropertyName(k);
            if (v is null) writer.WriteNullValue();
            else WriteDynamicValue(writer, v);
        }
        writer.WriteEndObject();
    }

    private static void WriteDynamicValue(Utf8JsonWriter writer, object value)
    {
        switch (value)
        {
            case Dictionary<string, object?> d: WriteDynamicMap(writer, d); break;
            case object[] arr:
                writer.WriteStartArray();
                foreach (var item in arr)
                {
                    if (item is null) writer.WriteNullValue();
                    else WriteDynamicValue(writer, item);
                }
                writer.WriteEndArray();
                break;
            case string s:   writer.WriteStringValue(s);    break;
            case bool b:     writer.WriteBooleanValue(b);   break;
            case long l:     writer.WriteNumberValue(l);    break;
            case int i:      writer.WriteNumberValue(i);    break;
            case double dbl: writer.WriteNumberValue(dbl);  break;
            case float f:    writer.WriteNumberValue(f);    break;
            case ulong u:    writer.WriteNumberValue(u);    break;
            default:         writer.WriteStringValue(value.ToString()); break;
        }
    }
}
