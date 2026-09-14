using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Ameto.Core;

/// <summary>
/// Per-connection SSE frame writer. Serialises each DTO as UTF-8 directly into a
/// reusable buffer framed as <c>data: {json}\n\n</c> and writes it to the response
/// body. The previous path copied every event three times — a UTF-16 JSON string,
/// an interpolated <c>$"data: {json}\n\n"</c> string, and the UTF-16→UTF-8 transcode
/// inside <c>WriteAsync(string)</c>; all three are gone.
///
/// <para>Lives in Core rather than beside its first caller because the trace and metric
/// endpoint mappers ship in their own assemblies and do not reference Ameto.Server — the
/// reference runs the other way, so a second copy over there was the only alternative.</para>
///
/// <para>Takes a <see cref="Stream"/>, not an <c>HttpResponse</c>: the response body is all it
/// ever touched, and the parameter type was the only thing that made Ameto.Core need
/// <c>&lt;FrameworkReference Include="Microsoft.AspNetCore.App" /&gt;</c>. That reference is
/// transitive — it landed in the runtimeconfig.json of every console tool that links Core
/// (tools/loggen), so loggen refused to start on a host carrying only the .NET runtime.
/// A writer that frames bytes has no business dragging a web server behind it.</para>
/// </summary>
public sealed class SseJsonWriter : IDisposable
{
    private static readonly byte[] DataPrefix     = "data: "u8.ToArray();
    private static readonly byte[] FrameSuffix    = "\n\n"u8.ToArray();
    private static readonly byte[] DoneFrame      = "event: done\ndata: {}\n\n"u8.ToArray();
    private static readonly byte[] DonePrefix     = "event: done\ndata: "u8.ToArray();
    // NOT "event: error": EventSource dispatches its own connection failures under that
    // name, so a client listening for one would receive the other.
    private static readonly byte[] ErrorPrefix    = "event: query-error\ndata: "u8.ToArray();
    private static readonly byte[] KeepaliveFrame = ": keepalive\n\n"u8.ToArray();

    /// <summary>
    /// How much framed output may sit in the buffer before <see cref="WriteLogEventAsync"/>
    /// puts it on the wire. Only the BUFFERED overload consults it; every other frame on this
    /// writer is still sent the moment it is composed.
    ///
    /// <para>Roughly a TCP window's worth. A 500-row page used to cost 500 writes and 500
    /// flushes — a socket send per row — for a payload that coalesces into some tens of
    /// segments; the buffer turns that into one send per ~16 KB while leaving the frame bytes
    /// themselves untouched. Anything that must not wait (the end of a page, a keepalive, a
    /// terminal frame) goes out through a method that sends unconditionally, which is what
    /// keeps the live tail's silence bound a property of the loop rather than of the
    /// buffer.</para>
    /// </summary>
    private const int FlushThresholdBytes = 16 * 1024;

    private readonly ArrayBufferWriter<byte> _buffer = new(4096);
    private readonly Utf8JsonWriter          _json;
    private readonly Stream                  _body;

    /// <param name="body">The response body to frame into — <c>ctx.Response.Body</c>.</param>
    public SseJsonWriter(Stream body)
    {
        _body = body;
        _json = new Utf8JsonWriter(_buffer);
    }

    /// <summary>
    /// Writes whatever frames are still buffered and flushes the body. A no-op when nothing
    /// is pending, so it is safe to call at the end of every page, poll and error path —
    /// and it MUST be called there: a frame left in this buffer is a row the client never
    /// sees.
    /// </summary>
    public ValueTask FlushFramesAsync(CancellationToken ct) => SendAsync(ct);

    /// <summary>
    /// One <c>data:</c> frame carrying a log event, written straight from the event with no
    /// DTO and no reflection (see <see cref="LogEventJsonWriter"/>), and held in the buffer
    /// with its neighbours until <see cref="FlushThresholdBytes"/> is reached.
    ///
    /// <para>Callers own the end of the run: finish with <see cref="FlushFramesAsync"/> (or
    /// any terminal frame, which sends the backlog with it) or the tail of the page stays
    /// here.</para>
    /// </summary>
    public async ValueTask WriteLogEventAsync(LogEvent ev, CancellationToken ct)
    {
        _buffer.Write(DataPrefix);
        _json.Reset(_buffer);
        LogEventJsonWriter.Write(_json, ev);
        _json.Flush();
        _buffer.Write(FrameSuffix);
        if (_buffer.WrittenCount >= FlushThresholdBytes)
            await SendAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Puts the buffered bytes on the wire and empties the buffer, keeping its capacity —
    /// one buffer per connection, for the life of the connection.
    /// </summary>
    private async ValueTask SendAsync(CancellationToken ct)
    {
        if (_buffer.WrittenCount == 0) return;
        await _body.WriteAsync(_buffer.WrittenMemory, ct).ConfigureAwait(false);
        await _body.FlushAsync(ct).ConfigureAwait(false);
        _buffer.ResetWrittenCount();
    }

    /// <summary>
    /// Writes one <c>data:</c> frame with the DTO serialised through a SOURCE-GENERATED contract,
    /// then flushes. Prefer this overload: it is the one that keeps the reflection-based metadata
    /// resolver out of the per-row path, and out of the trimmed output.
    /// </summary>
    public async Task WriteEventAsync<T>(T dto, JsonTypeInfo<T> typeInfo, CancellationToken ct)
    {
        _buffer.Write(DataPrefix);
        _json.Reset(_buffer);
        JsonSerializer.Serialize(_json, dto, typeInfo);
        _buffer.Write(FrameSuffix);
        await SendAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// The reflection door, kept for callers that genuinely have no contract to hand over.
    ///
    /// <para>One does: the log streams serialise a DTO whose attribute values are <c>object</c>,
    /// through a DynamicObjectConverter — a shape a generated contract does not describe, and
    /// converting it is a change to the log path rather than to this one. Everything else should
    /// take the <see cref="JsonTypeInfo{T}"/> overload above, which is why that one exists: when
    /// this writer moved into Core it became the repo-wide SSE contract, and it offered no door
    /// but this one, so every stream was structurally on the reflection resolver.</para>
    /// </summary>
    public async Task WriteEventAsync<T>(T dto, JsonSerializerOptions options, CancellationToken ct)
    {
        _buffer.Write(DataPrefix);
        _json.Reset(_buffer);
        JsonSerializer.Serialize(_json, dto, options);
        _buffer.Write(FrameSuffix);
        await SendAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Terminal <c>event: done</c> frame with an empty payload.</summary>
    public async Task WriteDoneAsync(CancellationToken ct)
    {
        _buffer.Write(DoneFrame);
        await SendAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Terminal <c>event: done</c> frame that also says WHICH ending it was.
    ///
    /// <para>`done` on its own is a single name for two different outcomes: the window was read
    /// to its floor, and the caller's row ceiling was reached with the window still unread. The
    /// Angular client can tell them apart only by counting its own rows against the <c>max</c> it
    /// asked for; every other consumer — a script, a second UI, a future export — cannot tell them
    /// apart at all, which is exactly the conflation the capped/short-page and stalled-cursor
    /// signals exist to remove.</para>
    ///
    /// <para>Backward compatible on purpose: the event NAME is unchanged, so a client that only
    /// listens for <c>done</c> and ignores the payload (which is what the Angular client does)
    /// keeps treating either ending as a normal completion. The distinction is additive, in
    /// fields, for consumers that want it.</para>
    /// </summary>
    /// <param name="complete">True when the whole requested window was read out.</param>
    /// <param name="reason">Machine-readable ending: <c>exhausted</c> or <c>max-rows</c>.</param>
    /// <param name="truncatedBy">
    /// What ELSE went wrong on the way to that ending, or null when nothing did — written as
    /// <c>truncatedBy</c> and omitted entirely when null.
    ///
    /// <para>It exists because "your row ceiling stopped me" and "I could not read part of the
    /// window" are both true at once, routinely, and a caller told only the first has no way to
    /// discover the second: it looks exactly like the ordinary, healthy ending. Its absence is
    /// therefore a positive claim — this page ceiling is the ONLY reason the list is short.</para>
    /// </param>
    public async Task WriteDoneAsync(bool complete, string reason, string? truncatedBy, CancellationToken ct)
    {
        _buffer.Write(DonePrefix);
        _json.Reset(_buffer);
        _json.WriteStartObject();
        _json.WriteBoolean("complete", complete);
        _json.WriteString("reason", reason);
        if (truncatedBy is not null) _json.WriteString("truncatedBy", truncatedBy);
        _json.WriteEndObject();
        _json.Flush();
        _buffer.Write(FrameSuffix);
        await SendAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Terminal <c>event: error</c> frame. The status line is long gone by the time a query
    /// fails mid-stream, so this is the only way to tell the client something went wrong —
    /// without it the stream simply stopped, indistinguishable from "no more results".
    /// </summary>
    /// <param name="truncatedBy">
    /// Optional machine-readable cause, written beside the sentence. A client that reads only
    /// <c>error</c> is unaffected; one that reads this can treat the same fault the same way
    /// whichever terminal frame carried it. Without it the error road offered nothing but English,
    /// so a page could not tell a lost segment from a spent deadline and had to treat every error
    /// alike — which is how one dead file became a blocking banner on one row ceiling and a grey
    /// suffix on another.
    /// </param>
    public async Task WriteErrorAsync(string message, CancellationToken ct, string? truncatedBy = null)
    {
        _buffer.Write(ErrorPrefix);
        _json.Reset(_buffer);
        _json.WriteStartObject();
        _json.WriteString("error", message);
        if (truncatedBy is not null) _json.WriteString("truncatedBy", truncatedBy);
        _json.WriteEndObject();
        _json.Flush();
        _buffer.Write(FrameSuffix);
        await SendAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Comment-only keepalive frame (ignored by EventSource clients).</summary>
    public async Task WriteKeepaliveAsync(CancellationToken ct)
    {
        _buffer.Write(KeepaliveFrame);
        await SendAsync(ct).ConfigureAwait(false);
    }

    public void Dispose() => _json.Dispose();
}
