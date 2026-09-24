using System.Buffers;
using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using MessagePack;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Integration.Tests;

/// <summary>
/// ISSUE #93: ONE TRACE ROW, ONE SPELLING, WHICHEVER ROUTE CARRIES IT. <c>GET /api/traces</c> and
/// <c>POST /api/traces/query</c> answer through the host's JSON options, whose encoder is ASP.NET
/// Core's <c>UnsafeRelaxedJsonEscaping</c>; their SSE twins (<c>/api/traces/stream</c>,
/// <c>/api/traces/query/stream</c>) used System.Text.Json's DEFAULT encoder, so every non-ASCII
/// character of a span name, service or path — and <c>&lt;</c>, <c>&amp;</c>, <c>'</c>, <c>+</c> —
/// went out as a six-byte <c>\uXXXX</c>. The same row after <c>JSON.parse</c>, but three times the
/// bytes for Cyrillic text and no way to compare the two routes' answers byte for byte.
///
/// <para>THE SSE FRAMING IS THE REASON TO LOOK TWICE. A <c>data:</c> line ends at the first raw
/// CR or LF, and some SSE parsers also break at U+2028 / U+2029 (JavaScript's line terminators).
/// The relaxed encoder relaxes a lot — so these rows carry every one of those characters, plus
/// U+0085, in a span name, and the data line is checked at the BYTE level for all of them. (The
/// Angular client uses the browser's native <c>EventSource</c>, whose parser splits on CR, LF
/// and CRLF only, and <c>JSON.parse</c>, which accepts raw U+2028 inside a string — but the
/// encoder escapes those anyway, which is what this file pins rather than assumes.)</para>
/// </summary>
public sealed class TraceStreamEncodingTests : IClassFixture<AmetoWebAppFactory>
{
    private const long Ms = 1_000_000L;

    /// <summary>2100-01-01T02:00:00Z — its own window, and clear of retention.</summary>
    private const long Anchor = 4_102_452_000_000_000_000L;

    private static readonly char Ls  = (char)0x2028;   // LINE SEPARATOR
    private static readonly char Ps  = (char)0x2029;   // PARAGRAPH SEPARATOR
    private static readonly char Nel = (char)0x0085;   // NEXT LINE

    /// <summary>Every character class the two encoders disagree on, and every line breaker.</summary>
    private static readonly string HostileName =
        "GET /заказы?q=<1>&x='y'+z `t` ü 日本 😀 " + Ls + "ls" + Ps + "ps\nlf\rcr" + Nel + "nel \"q\" \\ end";

    private const string Service = "сервис-кодировка";
    private const string HttpPath = "/заказы/<1>&amp;😀";

    private readonly HttpClient         _client;
    private readonly TraceStorageEngine _traces;
    private readonly ITestOutputHelper  _out;

    public TraceStreamEncodingTests(AmetoWebAppFactory factory, ITestOutputHelper output)
    {
        _client = factory.CreateClient();
        _traces = factory.Services.GetRequiredService<TraceStorageEngine>();
        _out    = output;
    }

    private static byte[] HttpAttributes()
    {
        var buf = new ArrayBufferWriter<byte>();
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("http.request.method"); w.Write("GET");
        w.Write("url.path");            w.Write(HttpPath);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>Two root spans: one with an HTTP status, one without (<c>httpStatusCode: null</c>).</summary>
    private void WriteTraces(long baseNano, ulong idBase)
    {
        for (ulong k = 0; k < 2; k++)
            _traces.WriteSpan(new SpanIngestItem
            {
                TraceId           = new TraceId(0x93, idBase + k),
                SpanId            = new SpanId(idBase + k),
                StartTimeUnixNano = baseNano + (long)k * Ms,
                DurationNanos     = 2 * Ms + 345_678,
                Name              = HostileName + " #" + k,
                ServiceName       = Service,
                Kind              = SpanKind.Server,
                Status            = k == 0 ? SpanStatusCode.Ok : SpanStatusCode.Error,
                HttpStatusCode    = k == 0 ? (short)201 : (short)0,
                AttributesBytes   = HttpAttributes(),
            });
    }

    private static (string From, string To) Window(long baseNano)
    {
        var from = DateTimeOffset.FromUnixTimeMilliseconds(baseNano / Ms - 1000);
        var to   = DateTimeOffset.FromUnixTimeMilliseconds(baseNano / Ms + 1000);
        return (from.ToString("O"), to.ToString("O"));
    }

    private static string Q(string s) => Uri.EscapeDataString(s);

    // ── The raw SSE body, at the byte level ─────────────────────────────────

    private static readonly byte[] DataPrefix = "data: "u8.ToArray();

    /// <summary>
    /// The payloads of the untagged (row) frames, as BYTES. Frames are cut at the blank line and
    /// lines at LF — the only line ending the server writes — and every row frame must be exactly
    /// ONE <c>data:</c> line: a raw line break inside the JSON would split it into two, and a
    /// parser that honours CR, U+2028 or U+2029 would split it at those.
    /// </summary>
    private static List<byte[]> RowPayloads(byte[] body, out string terminal)
    {
        var rows = new List<byte[]>();
        terminal = "";
        var rest = body.AsSpan();
        while (!rest.IsEmpty)
        {
            int end = rest.IndexOf("\n\n"u8);
            Assert.True(end >= 0, "the body ends inside a frame");
            var frame = rest[..end];
            rest = rest[(end + 2)..];

            if (frame.StartsWith(": "u8)) continue;                         // keepalive comment
            if (frame.StartsWith("event: "u8))
            {
                terminal = Encoding.UTF8.GetString(frame[7..frame.IndexOf((byte)'\n')]);
                continue;
            }

            Assert.True(frame.StartsWith(DataPrefix), "a row frame that is not a data line");
            Assert.True(frame.IndexOf((byte)'\n') < 0, "a row frame spans more than one line");
            var payload = frame[DataPrefix.Length..];
            AssertNoLineBreaker(payload);
            rows.Add(payload.ToArray());
        }
        return rows;
    }

    private static void AssertNoLineBreaker(ReadOnlySpan<byte> payload)
    {
        Assert.True(payload.IndexOf((byte)'\n') < 0, "raw LF in a data line");
        Assert.True(payload.IndexOf((byte)'\r') < 0, "raw CR in a data line");
        Assert.True(payload.IndexOf([(byte)0xE2, (byte)0x80, (byte)0xA8]) < 0, "raw U+2028 in a data line");
        Assert.True(payload.IndexOf([(byte)0xE2, (byte)0x80, (byte)0xA9]) < 0, "raw U+2029 in a data line");
        Assert.True(payload.IndexOf([(byte)0xC2, (byte)0x85]) < 0,             "raw U+0085 in a data line");
    }

    private async Task<(List<byte[]> Rows, string Terminal)> SseAsync(string url)
    {
        using var cts  = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var resp = await _client.GetAsync(url, cts.Token);
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var rows = RowPayloads(await resp.Content.ReadAsByteArrayAsync(cts.Token), out var terminal);
        return (rows, terminal);
    }

    /// <summary>The REST body is a JSON array of the same rows: <c>[row,row]</c>, compared as bytes.</summary>
    private void AssertSameBytes(byte[] rest, List<byte[]> sseRows, string what)
    {
        var joined = new List<byte>(rest.Length) { (byte)'[' };
        for (int i = 0; i < sseRows.Count; i++)
        {
            if (i > 0) joined.Add((byte)',');
            joined.AddRange(sseRows[i]);
        }
        joined.Add((byte)']');

        string restText = Encoding.UTF8.GetString(rest), sseText = Encoding.UTF8.GetString(joined.ToArray());
        if (restText != sseText) _out.WriteLine($"{what}\nREST: {restText}\nSSE:  {sseText}");

        Assert.Equal(2, sseRows.Count);
        Assert.Equal(restText, sseText);
        Assert.Equal(rest, joined.ToArray());
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task The_filter_stream_sends_each_row_in_the_bytes_GET_api_traces_sends()
    {
        long baseNano = Anchor;
        WriteTraces(baseNano, 0x9301);
        var (from, to) = Window(baseNano);
        string filter = $"service={Q(Service)}&from={Q(from)}&to={Q(to)}";

        byte[] rest = await _client.GetByteArrayAsync($"/api/traces?{filter}&limit=10");
        var (sse, terminal) = await SseAsync($"/api/traces/stream?{filter}&max=10");

        Assert.Equal("done", terminal);
        AssertSameBytes(rest, sse, "GET /api/traces vs /api/traces/stream");
        AssertNoLineBreaker(rest);

        // Raw UTF-8 on both routes — the relaxed encoder's whole point — and the line breakers,
        // the surrogate pair and the quote escaped on both.
        string row = Encoding.UTF8.GetString(sse[0]);
        Assert.Contains("\"serviceName\":\"сервис-кодировка\"", row);
        Assert.Contains("GET /заказы?q=<1>&x='y'+z `t` ü 日本 \\uD83D\\uDE00 \\u2028ls\\u2029ps\\nlf\\rcr\\u0085nel \\\"q\\\" \\\\ end", row);
        Assert.Contains("\"httpPath\":\"/заказы/<1>&amp;\\uD83D\\uDE00\"", row);
    }

    [Fact]
    public async Task The_traceql_stream_sends_each_row_in_the_bytes_POST_api_traces_query_sends()
    {
        long baseNano = Anchor + 60_000 * Ms;
        WriteTraces(baseNano, 0x9311);
        var (from, to) = Window(baseNano);
        string ql = $"{{ service = \"{Service}\" }}";

        using var post = await _client.PostAsJsonAsync("/api/traces/query", new { query = ql, from, to, limit = 10 });
        Assert.Equal(HttpStatusCode.OK, post.StatusCode);
        byte[] rest = await post.Content.ReadAsByteArrayAsync();

        var (sse, terminal) = await SseAsync($"/api/traces/query/stream?ql={Q(ql)}&from={Q(from)}&to={Q(to)}&max=10");

        Assert.Equal("done", terminal);
        AssertSameBytes(rest, sse, "POST /api/traces/query vs /api/traces/query/stream");
    }

    // ── What a row frame costs ───────────────────────────────────────────────

    /// <summary>A body that keeps nothing and completes every write synchronously.</summary>
    private sealed class CountingSink : Stream
    {
        public long Written;
        public override bool CanRead  => false;
        public override bool CanSeek  => false;
        public override bool CanWrite => true;
        public override long Length   => Written;
        public override long Position { get => Written; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override Task FlushAsync(CancellationToken ct) => Task.CompletedTask;
        public override int  Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => Written += count;
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
        {
            Written += buffer.Length;
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// THE ENCODING COSTS A COPY, NOT AN ALLOCATION. Each row is encoded into a buffer the stream
    /// keeps and copied verbatim into the SSE writer's frame. Held against the route it replaced —
    /// the same row through <see cref="Ameto.Core.SseJsonWriter.WriteEventAsync{T}"/> with the
    /// generated contract, on the same writer — rather than against zero, because the SSE writer has
    /// a cost of its own that is not this change's (in a Debug build its async state machines are
    /// classes: one object per call however the call completes). The gate is less than 24 B per row
    /// over that route: the size of the smallest object, so no per-row object can pass it.
    /// Per-thread counter; the sink completes every write synchronously, so nothing leaves the
    /// thread, which is asserted.
    /// </summary>
    [Fact]
    public async Task A_row_frame_in_the_host_encoding_allocates_nothing_per_row_over_the_old_frame()
    {
        var row = new TraceRowDto
        {
            TraceId = "00000000000000930000000000009399", SpanId = "0000000000009399",
            Name = HostileName, ServiceName = Service, Services = [Service, "billing"],
            Status = "Ok", HttpMethod = "GET", HttpPath = HttpPath, HttpStatusCode = 201,
            StartTimeUnixNano = Anchor, DurationNanos = 2 * Ms, SpanCount = 3,
        };
        var ctx  = new Microsoft.AspNetCore.Http.DefaultHttpContext();   // no services: the host's defaults
        var sink = new CountingSink();
        using var sse     = new Ameto.Core.SseJsonWriter(sink);
        using var rowJson = new TraceStreamRowJson(ctx);

        const int Warmup = 200, Rows = 1_000;
        int thread = Environment.CurrentManagedThreadId;

        for (int i = 0; i < Warmup; i++) await sse.WriteEventAsync(row, TraceStreamJson.Default.TraceRowDto, CancellationToken.None);
        long wire   = sink.Written;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < Rows; i++) await sse.WriteEventAsync(row, TraceStreamJson.Default.TraceRowDto, CancellationToken.None);
        long oldFrames = GC.GetAllocatedBytesForCurrentThread() - before;
        long oldWire   = (sink.Written - wire) / Rows;

        for (int i = 0; i < Warmup; i++) await rowJson.WriteAsync(sse, row, CancellationToken.None);
        wire   = sink.Written;
        before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < Rows; i++) await rowJson.WriteAsync(sse, row, CancellationToken.None);
        long newFrames = GC.GetAllocatedBytesForCurrentThread() - before;
        long newWire   = (sink.Written - wire) / Rows;

        Assert.True(thread == Environment.CurrentManagedThreadId, "a write hopped threads — the counter no longer covers it");
        _out.WriteLine($"{Rows:N0} row frames: default encoder (before) {oldFrames:N0} B allocated, {oldWire:N0} B per frame; "
                     + $"host encoder {newFrames:N0} B allocated, {newWire:N0} B per frame");
        Assert.True(newFrames - oldFrames < Rows * 24,
            $"{Rows:N0} row frames in the host encoding allocated {newFrames:N0} B against {oldFrames:N0} B for the "
            + "default-encoder frame — the row is allocating per frame");
    }
}
