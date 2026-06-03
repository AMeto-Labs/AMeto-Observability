using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Rd.Log.Core;

/// <summary>
/// Bit flags packed into <see cref="LogEventHeader.Flags"/>.
/// </summary>
[Flags]
public enum LogEventFlags : byte
{
    None         = 0,
    /// <summary>The event has a non-null <see cref="LogEvent.Exception"/> payload.</summary>
    HasException = 1 << 0,
    Reserved1    = 1 << 1,
}

/// <summary>
/// Fixed-size header stored contiguously in the ring buffer and hot-tier NativeMemory array.
/// Variable-length data (message template, properties bytes, exception) is stored separately
/// in parallel arrays / payload buffers; offsets below point into the payload arena.
///
/// Total size: 64 bytes — one full cache line.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 64)]
public struct LogEventHeader
{
    /// <summary>Monotonic event id — NodeId (hi 32) + Sequence (lo 32).</summary>
    public ulong  Id;

    /// <summary>UTC ticks (DateTime.UtcNow.Ticks).</summary>
    public long   TimestampUtcTicks;

    /// <summary>Offset of the MessageTemplate string in the string intern pool (or -1).</summary>
    public int    MessageTemplatePoolIndex;

    /// <summary>Byte offset of this event's msgpack properties blob in the payload arena.</summary>
    public int    PropertiesArenaOffset;

    /// <summary>Byte length of the msgpack properties blob.</summary>
    public int    PropertiesByteLength;

    /// <summary>Intern-pool index of the <c>service.name</c> / service identifier string (-1 if absent).</summary>
    public int    ServiceNamePoolIndex;

    /// <summary>Log level.</summary>
    public LogLevel Level;

    /// <summary>Packed bitflags — see <see cref="LogEventFlags"/>.</summary>
    public byte   Flags;

    // 2 bytes padding
    private byte  _pad1;
    private short _pad2;

    /// <summary>High 64 bits of the 128-bit TraceId (0 when absent).</summary>
    public ulong  TraceIdHi;

    /// <summary>Low 64 bits of the 128-bit TraceId (0 when absent).</summary>
    public ulong  TraceIdLo;

    /// <summary>64-bit SpanId (0 when absent).</summary>
    public ulong  SpanId;

    public static int SizeOf => Unsafe.SizeOf<LogEventHeader>();

    public bool HasTraceId => (TraceIdHi | TraceIdLo) != 0;
    public bool HasSpanId  => SpanId != 0;

    /// <summary>Convenience: read / set <see cref="LogEventFlags.HasException"/>.</summary>
    public bool HasException
    {
        get => ((LogEventFlags)Flags & LogEventFlags.HasException) != 0;
        set => Flags = (byte)(value
                              ? (LogEventFlags)Flags | LogEventFlags.HasException
                              : (LogEventFlags)Flags & ~LogEventFlags.HasException);
    }
}

/// <summary>
/// Fully materialised, heap-allocated event used in query results and API responses.
/// Not stored in hot path — only created when an event is decoded for delivery.
///
/// Note: there is no rendered-message field. The UI renders the human-readable
/// message on the fly from <see cref="MessageTemplate"/> + <see cref="Properties"/>.
/// </summary>
public sealed class LogEvent
{
    public required EventId Id                           { get; init; }
    public required DateTimeOffset Timestamp             { get; init; }
    public required LogLevel Level                       { get; init; }
    public required string MessageTemplate               { get; init; }
    public ExceptionInfo? Exception                      { get; init; }
    public Dictionary<string, object?>? Properties       { get; init; }

    /// <summary>Raw msgpack bytes of the properties map (for re-serialization without re-deserializing).</summary>
    public ReadOnlyMemory<byte> RawProperties            { get; init; }

    /// <summary>High 64 bits of the 128-bit distributed TraceId (0 when absent).</summary>
    public ulong TraceIdHi   { get; init; }

    /// <summary>Low 64 bits of the 128-bit distributed TraceId (0 when absent).</summary>
    public ulong TraceIdLo   { get; init; }

    /// <summary>64-bit SpanId (0 when absent).</summary>
    public ulong SpanId      { get; init; }

    /// <summary>Service name (<c>service.name</c> from OTLP resource attributes, or Serilog SourceContext namespace).</summary>
    public string? ServiceName { get; init; }
}

/// <summary>
/// Compact wire format matching Seq CLEF (Compact Log Event Format) over MessagePack.
/// Field names match the CLEF spec: @t, @mt, @l, @x, + arbitrary properties.
///
/// Note on <c>@m</c>: CLEF defines a pre-rendered message field, but Rd.Log
/// renders messages on the client. On ingest, if a payload contains <c>@m</c>
/// without <c>@mt</c>, the value is promoted to <c>@mt</c>; otherwise <c>@m</c>
/// is silently dropped. The server never emits <c>@m</c>.
/// </summary>
public static class ClefFields
{
    /// <summary>
    /// Separator used to join property path segments in index keys.
    /// U+0001 matches the internal encoding used by FilterParser/PropertyPath,
    /// preventing collisions with property names that contain literal dots.
    /// </summary>
    public const char PropertyPathSeparator = '\u0001';

    public const string Timestamp       = "@t";
    public const string MessageTemplate = "@mt";
    public const string Level           = "@l";
    /// <summary>Legacy CLEF rendered message — accepted on ingest as <c>@mt</c> fallback only.</summary>
    public const string Message         = "@m";
    public const string Exception       = "@x";
    public const string TraceId         = "@tr";
    public const string SpanId          = "@sp";
    /// <summary>Per-event 64-bit Snowflake EventId (Rd.Log extension to CLEF, persisted in cold-tier).</summary>
    public const string EventId         = "@i";
    /// <summary>Service/application name — OTLP resource attribute key and Rd.Log canonical property key.</summary>
    public const string ServiceName     = "service.name";
}
