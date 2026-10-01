using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ameto.Core;

namespace Ameto.Core.Tests;

/// <summary>
/// THE TERMINAL FRAMES IN THE HOST'S ENCODING (#94). <c>done</c> and <c>query-error</c> were
/// composed by the writer's own <see cref="Utf8JsonWriter"/>, built with System.Text.Json's
/// defaults, so a parse error naming the user's own literal went out as <c>п…</c> and
/// <c>'</c> while every REST answer of the same host — ASP.NET Core's relaxed encoder — writes
/// that text as UTF-8. The options overload encodes those two frames with the host's settings, never
/// indented (an indented payload would carry the line break that ends its <c>data:</c> line), and
/// leaves every other frame's bytes as they were.
/// </summary>
public sealed class SseJsonWriterEncodingTests
{
    private static readonly JsonWriterOptions Relaxed = new()
    {
        Encoder  = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        Indented = true,                                      // must not reach a data: line
    };

    private const string Message = "TraceQL parse error: Unexpected token String('привет <b>')";

    [Fact]
    public async Task Done_and_query_error_frames_use_the_given_encoder_on_one_line()
    {
        var body = new RecordingStream();
        using var sse = new SseJsonWriter(body, Relaxed);

        await sse.WriteErrorAsync(Message, default, truncatedBy: "unreadable");
        await sse.WriteDoneAsync(false, "max-rows", "ограничение", default);

        Assert.Equal(
            "event: query-error\ndata: {\"error\":\"TraceQL parse error: Unexpected token String('привет <b>')\",\"truncatedBy\":\"unreadable\"}\n\n"
          + "event: done\ndata: {\"complete\":false,\"reason\":\"max-rows\",\"truncatedBy\":\"ограничение\"}\n\n",
            body.All());
    }

    [Fact]
    public async Task Without_options_the_frames_keep_the_default_encoding()
    {
        var body = new RecordingStream();
        using var sse = new SseJsonWriter(body);

        await sse.WriteErrorAsync(Message, default);

        Assert.Equal(
            "event: query-error\ndata: {\"error\":\"TraceQL parse error: Unexpected token String(\\u0027\\u043F\\u0440\\u0438\\u0432\\u0435\\u0442 \\u003Cb\\u003E\\u0027)\"}\n\n",
            body.All());
    }

    [Fact]
    public async Task A_row_frame_is_not_re_encoded_by_the_terminal_options()
    {
        // The rows' bytes are their caller's business (the trace streams encode theirs in the host's
        // encoding themselves): the options change the terminal frames and nothing else.
        var plain   = new RecordingStream();
        var options = new RecordingStream();
        using (var a = new SseJsonWriter(plain))            await a.WriteEventAsync(new Row("привет"), RowJson.Default.Row, default);
        using (var b = new SseJsonWriter(options, Relaxed)) await b.WriteEventAsync(new Row("привет"), RowJson.Default.Row, default);

        Assert.Equal(plain.All(), options.All());
        Assert.Contains("\\u043F", options.All());
    }
}

internal sealed record Row(string Name);

[JsonSerializable(typeof(Row))]
internal sealed partial class RowJson : JsonSerializerContext;
