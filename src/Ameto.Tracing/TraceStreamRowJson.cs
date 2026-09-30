using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Ameto.Core;
using Microsoft.AspNetCore.Http;

namespace Ameto.Tracing;

/// <summary>
/// THE TRACE STREAMS' ROW FRAMES IN THE HOST'S ENCODING — each row's <c>data:</c> payload is byte
/// for byte what <c>GET /api/traces</c> and <c>POST /api/traces/query</c> put in their arrays for
/// the same row (issue #93), on any host that does not indent its JSON — this server does not; see
/// <see cref="WriterOptions"/> for one that does. One per stream.
///
/// <para>WHY NOT A CONTEXT WITH THE HOST'S OPTIONS, as <c>MetricJson.Web</c> did for the metrics
/// answers: it does not reach the bytes here. <see cref="JsonSerializer.Serialize{TValue}(Utf8JsonWriter, TValue, JsonTypeInfo{TValue})"/>
/// escapes string VALUES with the encoder of the WRITER it is handed, not the one in the
/// context's options (those only pre-escape property names), and <see cref="SseJsonWriter"/> owns
/// its writer, built with System.Text.Json's default settings. Tried and measured:
/// <c>TraceStreamEncodingTests</c> stayed red with a relaxed-encoder context — Cyrillic still went
/// out as <c>\u0441…</c>, <c>&lt;</c> as <c>\u003C</c>, <c>"</c> as <c>\u0022</c>.</para>
///
/// <para>SO THE ROW IS ENCODED HERE, into a buffer kept for the life of the stream, by the
/// generated <see cref="TraceStreamJson"/> contract through a writer over the host's ENCODER —
/// the one the REST answers are written with — but never its indentation (<see cref="WriterOptions"/>),
/// and handed to <see cref="SseJsonWriter"/> as a
/// value it copies VERBATIM into its frame (<see cref="PreEncodedJson"/>). The frame, its rollback
/// on a failed compose and its send are still the SSE writer's own. Per row this costs one copy of
/// the payload and allocates nothing; per stream, the buffer and the writer.</para>
///
/// <para>SAFE IN A <c>data:</c> LINE, and pinned rather than assumed: every encoder
/// System.Text.Json ships escapes the C0 controls and the row is never indented, so no raw CR or LF
/// can end the line early (asserted per row in Debug builds), and
/// the relaxed one still escapes U+2028, U+2029 and U+0085 (<c>\u2028</c>…), which some SSE and
/// JavaScript parsers treat as line breaks (<c>TraceStreamEncodingTests</c>,
/// <c>TraceDetailShapeTests</c>' 32-level golden). The terminal <c>done</c> / <c>query-error</c>
/// frames and the keepalives are the SSE writer's own and keep its default encoder: they have no
/// REST twin to match.</para>
/// </summary>
internal sealed class TraceStreamRowJson : IDisposable
{
    private readonly ArrayBufferWriter<byte> _row = new(1024);
    private readonly Utf8JsonWriter          _json;

    public TraceStreamRowJson(HttpContext ctx) =>
        _json = new Utf8JsonWriter(_row, WriterOptions(ctx));

    /// <summary>
    /// The host's ENCODER and nothing else of its layout — never <c>Indented</c>, whatever the host
    /// says. An indented row carries raw CR/LF, and the first of them ends the <c>data:</c> line:
    /// the client would get the row in fragments that do not parse. So on a host that indents its
    /// JSON (<c>WriteIndented</c>, common in Development) the rows are no longer the REST answers'
    /// bytes — those are indented — but the same text on one line; on a host that does not, which is
    /// this server's configuration, they are the same bytes.
    /// </summary>
    internal static JsonWriterOptions WriterOptions(HttpContext ctx) => new()
    {
        Encoder  = TraceDetailJson.HostOptions(ctx).Encoder,
        Indented = false,
    };

    /// <summary>One row frame — see the class remarks. Same contract as <see cref="SseJsonWriter.WriteEventAsync{T}"/>.</summary>
    public Task WriteAsync(SseJsonWriter sse, TraceRowDto row, CancellationToken ct)
    {
        _row.ResetWrittenCount();
        _json.Reset(_row);
        JsonSerializer.Serialize(_json, row, TraceStreamJson.Default.TraceRowDto);   // flushes the writer
        System.Diagnostics.Debug.Assert(
            _row.WrittenSpan.IndexOfAny((byte)'\n', (byte)'\r') < 0,
            "a trace row carries a raw line break — it would end its SSE data line early");
        return sse.WriteEventAsync(new PreEncodedJson(_row.WrittenMemory), PreEncodedJson.TypeInfo, ct);
    }

    public void Dispose() => _json.Dispose();
}

/// <summary>
/// One JSON value that is already encoded, written VERBATIM by whatever writer serialises it — no
/// re-escaping, no re-validation. Only for bytes a <see cref="Utf8JsonWriter"/> produced.
/// </summary>
internal readonly struct PreEncodedJson(ReadOnlyMemory<byte> utf8)
{
    public ReadOnlyMemory<byte> Utf8 { get; } = utf8;

    /// <summary>
    /// A generated-style contract over a custom converter — the call source generation emits for a
    /// converter-backed type — on options whose resolver is EMPTY: nothing here can fall back to
    /// reflection metadata.
    /// </summary>
    internal static readonly JsonTypeInfo<PreEncodedJson> TypeInfo =
        JsonMetadataServices.CreateValueInfo<PreEncodedJson>(
            new JsonSerializerOptions { TypeInfoResolver = JsonTypeInfoResolver.Combine() },
            new Converter());

    private sealed class Converter : JsonConverter<PreEncodedJson>
    {
        public override PreEncodedJson Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            throw new NotSupportedException("PreEncodedJson is write-only.");

        public override void Write(Utf8JsonWriter writer, PreEncodedJson value, JsonSerializerOptions options) =>
            writer.WriteRawValue(value.Utf8.Span, skipInputValidation: true);
    }
}
