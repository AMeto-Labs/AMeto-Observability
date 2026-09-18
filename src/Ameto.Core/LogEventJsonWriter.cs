using System.Text.Json;
using Ameto.Core.Serialization;

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
    private static readonly JsonEncodedText NameServiceName = JsonEncodedText.Encode("service.name");
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

        if (ev.Exception is { } x)
        {
            writer.WritePropertyName(NameException);
            WriteException(writer, x);
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
