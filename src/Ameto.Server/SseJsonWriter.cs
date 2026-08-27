using System.Buffers;
using System.Text.Json;

namespace Ameto.Server;

/// <summary>
/// Per-connection SSE frame writer. Serialises each DTO as UTF-8 directly into a
/// reusable buffer framed as <c>data: {json}\n\n</c> and writes it to the response
/// body. The previous path copied every event three times — a UTF-16 JSON string,
/// an interpolated <c>$"data: {json}\n\n"</c> string, and the UTF-16→UTF-8 transcode
/// inside <c>WriteAsync(string)</c>; all three are gone.
/// </summary>
internal sealed class SseJsonWriter : IDisposable
{
    private static readonly byte[] DataPrefix     = "data: "u8.ToArray();
    private static readonly byte[] FrameSuffix    = "\n\n"u8.ToArray();
    private static readonly byte[] DoneFrame      = "event: done\ndata: {}\n\n"u8.ToArray();
    // NOT "event: error": EventSource dispatches its own connection failures under that
    // name, so a client listening for one would receive the other.
    private static readonly byte[] ErrorPrefix    = "event: query-error\ndata: "u8.ToArray();
    private static readonly byte[] KeepaliveFrame = ": keepalive\n\n"u8.ToArray();

    private readonly ArrayBufferWriter<byte> _buffer = new(4096);
    private readonly Utf8JsonWriter          _json;
    private readonly HttpResponse            _response;

    public SseJsonWriter(HttpResponse response)
    {
        _response = response;
        _json     = new Utf8JsonWriter(_buffer);
    }

    /// <summary>Writes one <c>data:</c> frame with the DTO serialised as JSON, then flushes.</summary>
    public async Task WriteEventAsync<T>(T dto, JsonSerializerOptions options, CancellationToken ct)
    {
        _buffer.ResetWrittenCount();          // keep capacity — one buffer per connection
        _buffer.Write(DataPrefix);
        _json.Reset(_buffer);
        JsonSerializer.Serialize(_json, dto, options);
        _buffer.Write(FrameSuffix);
        await _response.Body.WriteAsync(_buffer.WrittenMemory, ct).ConfigureAwait(false);
        await _response.Body.FlushAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Terminal <c>event: done</c> frame.</summary>
    public async Task WriteDoneAsync(CancellationToken ct)
    {
        await _response.Body.WriteAsync(DoneFrame, ct).ConfigureAwait(false);
        await _response.Body.FlushAsync(ct).ConfigureAwait(false);
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
        await _response.Body.WriteAsync(_buffer.WrittenMemory, ct).ConfigureAwait(false);
        await _response.Body.FlushAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Comment-only keepalive frame (ignored by EventSource clients).</summary>
    public async Task WriteKeepaliveAsync(CancellationToken ct)
    {
        await _response.Body.WriteAsync(KeepaliveFrame, ct).ConfigureAwait(false);
        await _response.Body.FlushAsync(ct).ConfigureAwait(false);
    }

    public void Dispose() => _json.Dispose();
}
