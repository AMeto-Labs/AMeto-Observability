using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Ameto.Core;

namespace Ameto.Integration.Tests;

/// <summary>
/// THE ROAD THE LOG STREAMS USED TO TAKE, frozen: <c>LogEventDto.From(ev)</c> serialised through the
/// reflection-based options, exactly as <c>Ameto.Server.EndpointMapper</c> declared them at 42108b4 —
/// the last commit in which the server still sent a stream that way (the live tail). The types below
/// are that code, moved: same names, same attributes, same bodies; only the namespace and the nesting
/// changed.
///
/// <para>It lives here because the bytes it produced are the contract
/// <see cref="LogEventJsonWriter"/> has to keep, and the server no longer has any use for it. Do not
/// "fix" it: an edit here moves the reference the parity tests compare against, not the wire. The
/// golden frames in <see cref="LogEventJsonParityTests"/> pin it from outside, so an edit that moved
/// it shows up there.</para>
/// </summary>
internal static class LegacyDtoRoad
{
    /// <summary>The server's <c>EndpointMapper._json</c>, as it was.</summary>
    internal static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy        = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition      = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented               = false,
        Converters                  = { new DynamicObjectConverter() },
    };

    /// <summary>
    /// The same serialiser as a contract, for <c>SseJsonWriter.WriteEventAsync</c>. The tail called
    /// the writer's reflection overload with <see cref="Json"/>; that overload went with the tail.
    /// <c>JsonSerializer.Serialize(writer, value, options)</c> did exactly this on its first call —
    /// lock the options, fill in the default reflection resolver, resolve the type's info — and then
    /// serialised through it, so the frame is the same frame.
    /// </summary>
    internal static readonly JsonTypeInfo<LogEventDto> Contract = CreateContract();

    private static JsonTypeInfo<LogEventDto> CreateContract()
    {
        Json.MakeReadOnly(populateMissingResolver: true);
        return (JsonTypeInfo<LogEventDto>)Json.GetTypeInfo(typeof(LogEventDto));
    }

    // ── Dynamic object converter ──────────────────────────────────────────────────

    /// <summary>
    /// Serialises <c>object?</c> values stored in property dictionaries.
    /// Handles the concrete types produced by <c>LogEventSerializer</c>:
    /// nested dicts, arrays, primitives. Avoids the default ToString() fallback.
    /// </summary>
    internal sealed class DynamicObjectConverter : JsonConverter<object>
    {
        public override object? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => throw new NotSupportedException();

        public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
        {
            switch (value)
            {
                case Dictionary<string, object?> d:
                    writer.WriteStartObject();
                    foreach (var (k, v) in d)
                    {
                        writer.WritePropertyName(k);
                        if (v is null) writer.WriteNullValue();
                        else Write(writer, v, options);
                    }
                    writer.WriteEndObject();
                    break;
                case object[] arr:
                    writer.WriteStartArray();
                    foreach (var item in arr)
                    {
                        if (item is null) writer.WriteNullValue();
                        else Write(writer, item, options);
                    }
                    writer.WriteEndArray();
                    break;
                case string s:  writer.WriteStringValue(s);     break;
                case bool b:    writer.WriteBooleanValue(b);    break;
                case long l:    writer.WriteNumberValue(l);     break;
                case int i:     writer.WriteNumberValue(i);     break;
                case double d:  writer.WriteNumberValue(d);     break;
                case float f:   writer.WriteNumberValue(f);     break;
                case ulong u:   writer.WriteNumberValue(u);     break;
                default:        writer.WriteStringValue(value.ToString()); break;
            }
        }
    }

    // ── DTO ───────────────────────────────────────────────────────────────────────

    /// <summary>JSON-serialisable view of a <see cref="LogEvent"/>.</summary>
    internal sealed class LogEventDto
    {
        [JsonPropertyName("@t")]            public string Timestamp       { get; init; } = "";
        [JsonPropertyName("@mt")]           public string MessageTemplate { get; init; } = "";
        [JsonPropertyName("@l")]            public string Level           { get; init; } = "";
        [JsonPropertyName("@x")]            public ExceptionInfoDto? Exception { get; init; }
        [JsonPropertyName("id")]            public string Id              { get; init; } = "";
        [JsonPropertyName("@tr")]           public string? TraceId        { get; init; }
        [JsonPropertyName("@sp")]           public string? SpanId         { get; init; }
        [JsonPropertyName("service.name")]  public string? ServiceName    { get; init; }
        [JsonPropertyName("props")]         public EventProps? Properties { get; init; }

        public static LogEventDto From(LogEvent ev) => new()
        {
            Timestamp       = ev.Timestamp.ToString("O"),
            MessageTemplate = ev.MessageTemplate,
            Level           = ev.Level.ToSeqString(),
            Exception       = ExceptionInfoDto.From(ev.Exception),
            Id              = ev.Id.RawValue.ToString(),
            TraceId         = TraceIdHelper.FormatTraceId(ev.TraceIdHi, ev.TraceIdLo),
            SpanId          = TraceIdHelper.FormatSpanId(ev.SpanId),
            ServiceName     = ev.ServiceName,
            // Raw first: touching ev.Properties would materialise the dictionary this
            // exists to avoid. Decoders that produce one directly still work.
            Properties      = !ev.RawProperties.IsEmpty ? new EventProps(ev.RawProperties)
                            : ev.Properties is { } map  ? new EventProps(map)
                            : null,
        };
    }

    /// <summary>
    /// The <c>props</c> payload as it reaches the serialiser: either the msgpack bytes the
    /// decoder carried through (written straight to JSON by <see cref="EventPropsConverter"/>)
    /// or an already-materialised dictionary.
    /// </summary>
    [JsonConverter(typeof(EventPropsConverter))]
    internal readonly struct EventProps
    {
        public readonly ReadOnlyMemory<byte>         Raw;
        public readonly Dictionary<string, object?>? Map;

        public EventProps(ReadOnlyMemory<byte> raw)         { Raw = raw;     Map = null; }
        public EventProps(Dictionary<string, object?> map)  { Raw = default; Map = map;  }
    }

    /// <summary>
    /// Writes <see cref="EventProps"/>. The msgpack branch skips the
    /// dictionary-then-reserialise round trip that dominated the log-scrolling profile;
    /// the dictionary branch delegates to <see cref="DynamicObjectConverter"/> so both
    /// produce identical JSON.
    /// </summary>
    internal sealed class EventPropsConverter : JsonConverter<EventProps>
    {
        public override EventProps Read(ref Utf8JsonReader reader, Type t, JsonSerializerOptions o)
            => throw new NotSupportedException();

        public override void Write(Utf8JsonWriter writer, EventProps value, JsonSerializerOptions options)
        {
            if (!value.Raw.IsEmpty)
            {
                Ameto.Core.Serialization.MsgPackJsonTranscoder.WriteMap(writer, value.Raw);
                return;
            }
            if (value.Map is { } map)
            {
                JsonSerializer.Serialize(writer, (object)map, options);
                return;
            }
            writer.WriteStartObject();
            writer.WriteEndObject();
        }
    }

    /// <summary>JSON-serialisable view of an <see cref="ExceptionInfo"/> tree.</summary>
    internal sealed class ExceptionInfoDto
    {
        [JsonPropertyName("type")]    public string  Type       { get; init; } = "";
        [JsonPropertyName("message")] public string? Message    { get; init; }
        [JsonPropertyName("stack")]   public string? StackTrace { get; init; }
        [JsonPropertyName("inner")]   public ExceptionInfoDto? Inner { get; init; }

        public static ExceptionInfoDto? From(ExceptionInfo? src)
        {
            if (src is null) return null;
            return new ExceptionInfoDto
            {
                Type       = src.Type,
                Message    = src.Message,
                StackTrace = src.StackTrace,
                Inner      = From(src.Inner),
            };
        }
    }
}
