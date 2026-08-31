using System.Buffers;
using System.Text.Json;

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

    private readonly ArrayBufferWriter<byte> _buffer = new(4096);
    private readonly Utf8JsonWriter          _json;
    private readonly Stream                  _body;

    /// <param name="body">The response body to frame into — <c>ctx.Response.Body</c>.</param>
    public SseJsonWriter(Stream body)
    {
        _body = body;
        _json = new Utf8JsonWriter(_buffer);
    }

    /// <summary>Writes one <c>data:</c> frame with the DTO serialised as JSON, then flushes.</summary>
    public async Task WriteEventAsync<T>(T dto, JsonSerializerOptions options, CancellationToken ct)
    {
        _buffer.ResetWrittenCount();          // keep capacity — one buffer per connection
        _buffer.Write(DataPrefix);
        _json.Reset(_buffer);
        JsonSerializer.Serialize(_json, dto, options);
        _buffer.Write(FrameSuffix);
        await _body.WriteAsync(_buffer.WrittenMemory, ct).ConfigureAwait(false);
        await _body.FlushAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Terminal <c>event: done</c> frame with an empty payload.</summary>
    public async Task WriteDoneAsync(CancellationToken ct)
    {
        await _body.WriteAsync(DoneFrame, ct).ConfigureAwait(false);
        await _body.FlushAsync(ct).ConfigureAwait(false);
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
    public async Task WriteDoneAsync(bool complete, string reason, CancellationToken ct)
    {
        _buffer.ResetWrittenCount();
        _buffer.Write(DonePrefix);
        _json.Reset(_buffer);
        _json.WriteStartObject();
        _json.WriteBoolean("complete", complete);
        _json.WriteString("reason", reason);
        _json.WriteEndObject();
        _json.Flush();
        _buffer.Write(FrameSuffix);
        await _body.WriteAsync(_buffer.WrittenMemory, ct).ConfigureAwait(false);
        await _body.FlushAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Terminal <c>event: error</c> frame. The status line is long gone by the time a query
    /// fails mid-stream, so this is the only way to tell the client something went wrong —
    /// without it the stream simply stopped, indistinguishable from "no more results".
    /// </summary>
    public async Task WriteErrorAsync(string message, CancellationToken ct)
    {
        _buffer.ResetWrittenCount();
        _buffer.Write(ErrorPrefix);
        _json.Reset(_buffer);
        _json.WriteStartObject();
        _json.WriteString("error", message);
        _json.WriteEndObject();
        _json.Flush();
        _buffer.Write(FrameSuffix);
        await _body.WriteAsync(_buffer.WrittenMemory, ct).ConfigureAwait(false);
        await _body.FlushAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Comment-only keepalive frame (ignored by EventSource clients).</summary>
    public async Task WriteKeepaliveAsync(CancellationToken ct)
    {
        await _body.WriteAsync(KeepaliveFrame, ct).ConfigureAwait(false);
        await _body.FlushAsync(ct).ConfigureAwait(false);
    }

    public void Dispose() => _json.Dispose();
}
