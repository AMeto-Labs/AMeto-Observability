using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Google.Protobuf;
using Ameto.Core;
using Ameto.Otel;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using Xunit.Abstractions;
using EventId  = Microsoft.Extensions.Logging.EventId;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Ameto.Integration.Tests;

/// <summary>
/// OTLP/HTTP with <c>Content-Encoding: gzip</c> — issue #82 — through the real pipeline, on all
/// six receiver paths.
///
/// <para>The collector's <c>otlphttp</c> exporter gzips every body by default. The receivers never
/// looked at the header, so the compressed bytes went straight into the protobuf or JSON parser
/// and every batch from a collector left on its defaults was a 400. What is pinned here: a gzip
/// body is ingested and can be read back (logs searched, a trace fetched by id, a metric queried);
/// the ceiling is the INFLATED size, so a batch that only fits compressed is a 413 with nothing
/// ingested, and a bomb never gets a buffer past the limit; any other coding is a 415 that names
/// what would have worked; gzip that does not inflate is a 400 with nothing ingested, never a 500;
/// and gzip over an empty body answers exactly what an empty body does.</para>
///
/// <para>The ceiling is configured down to 1 MiB — a power of two, so the pool's bucket for a
/// limit-sized rent IS the limit, and "no rent past the limit" is a statement about the limit
/// rather than about the pool's rounding.</para>
/// </summary>
public sealed class OtlpHttpGzipTests : IClassFixture<OtlpHttpGzipTests.Factory>
{
    private const int Limit = 1024 * 1024;

    /// <summary>
    /// Process-wide allocation a bomb request may cost end to end — the compressed body in the
    /// test server's pipe, the handler's two buffers, and whatever the host's background loops
    /// allocate meanwhile. Without the ceiling the inflate alone is 256 MiB and more.
    /// </summary>
    private const long BombRequestAllocationBudget = 32L * 1024 * 1024;

    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(15);

    public sealed class Factory : AmetoWebAppFactory
    {
        protected override IngestionOptions ConfiguredIngestion => new() { MaxOtlpBatchBytes = Limit };

        /// <summary>What the host logs — for the one refusal that is logged as well as answered.</summary>
        public CapturedLog Log { get; } = new();

        /// <summary>The clock the too-large warning is throttled by: it moves only when a test says so.</summary>
        internal Ameto.Testing.ManualTimeProvider Clock { get; } = new();

        protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            builder.ConfigureServices(services =>
            {
                services.AddSingleton<ILoggerProvider>(Log);
                services.AddSingleton(sp => new OtlpGzipTooLargeLog(
                    sp.GetRequiredService<ILoggerFactory>().CreateLogger("Ameto.Otel"), Clock));
            });
        }
    }

    /// <summary>
    /// Log entries by event name, with their structured values. The class's tests run one at a
    /// time, so a before/after difference is the test's own.
    /// </summary>
    public sealed class CapturedLog : ILoggerProvider
    {
        private readonly ConcurrentQueue<(string? Event, LogLevel Level, IReadOnlyList<KeyValuePair<string, object?>>? State)> _entries = new();

        public int Count(string eventName, LogLevel level)
        {
            int n = 0;
            foreach (var (name, lvl, _) in _entries) if (name == eventName && lvl == level) n++;
            return n;
        }

        /// <summary>The named values of the latest entry with this event name.</summary>
        public Dictionary<string, object?> Last(string eventName)
        {
            IReadOnlyList<KeyValuePair<string, object?>>? last = null;
            foreach (var (name, _, state) in _entries) if (name == eventName) last = state;
            Assert.NotNull(last);
            var values = new Dictionary<string, object?>();
            foreach (var (key, value) in last) values[key] = value;
            return values;
        }

        public ILogger CreateLogger(string categoryName) => new Sink(this);
        public void Dispose() { }

        private sealed class Sink(CapturedLog log) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => true;
            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                                    Func<TState, Exception?, string> formatter)
                => log._entries.Enqueue((eventId.Name, logLevel, state as IReadOnlyList<KeyValuePair<string, object?>>));
        }
    }

    private readonly Factory           _factory;
    private readonly HttpClient        _client;
    private readonly ITestOutputHelper _out;

    public OtlpHttpGzipTests(Factory factory, ITestOutputHelper output)
    {
        _factory = factory;
        _out     = output;
        // The pool ledger is an AsyncLocal, which TestServer carries into the handler only when
        // asked — and the flag is copied into a client when the client is made, so it goes first.
        factory.Server.PreserveExecutionContext = true;
        _client = factory.CreateClient();
    }

    private static readonly string[] Routes =
        ["/v1/logs", "/otlp/v1/logs", "/v1/traces", "/otlp/v1/traces", "/v1/metrics", "/otlp/v1/metrics"];

    public static TheoryData<string, bool> EveryRouteBothEncodings()
    {
        var data = new TheoryData<string, bool>();
        foreach (string route in Routes)
        {
            data.Add(route, false);
            data.Add(route, true);
        }
        return data;
    }

    public static TheoryData<string> EveryRoute()
    {
        var data = new TheoryData<string>();
        foreach (string route in Routes) data.Add(route);
        return data;
    }

    // ── Accepted ──────────────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(EveryRouteBothEncodings))]
    public async Task A_gzip_body_is_ingested_on_every_route_and_can_be_read_back(string route, bool protobuf)
    {
        var batch = Batch.Of(SignalOf(route), protobuf, count: 3);

        using var response = await PostAsync(route, OtlpGzipTests.Gzip(batch.Message), protobuf, "gzip");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("{\"ingested\":3,\"dropped\":0}", await response.Content.ReadAsStringAsync());
        await batch.AssertReadableAsync(_client);
    }

    /// <summary>
    /// The reproduction in the issue, header for header:
    /// <code>
    /// printf '%s' '&lt;ExportLogsServiceRequest JSON&gt;' | gzip \
    ///   | curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:5341/v1/logs \
    ///       -H 'Content-Type: application/json' -H 'Content-Encoding: gzip' \
    ///       -H 'X-Seq-ApiKey: &lt;key&gt;' --data-binary @-
    /// </code>
    /// "Should be 200 and records in search." It was 400: the gzip bytes went to the JSON parser.
    /// A bare client, so every header on the request is one the curl sends.
    /// </summary>
    [Fact]
    public async Task The_curl_from_issue_82_answers_200_and_the_record_is_in_search()
    {
        string marker = Marker();
        byte[] json   = Encoding.UTF8.GetBytes(Batch.LogsJson(marker, count: 1));   // printf '%s': no newline

        using var client  = _factory.Server.CreateClient();
        using var request = new HttpRequestMessage(HttpMethod.Post, "/v1/logs") { Content = new ByteArrayContent(OtlpGzipTests.Gzip(json)) };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        request.Content.Headers.ContentEncoding.Add("gzip");
        request.Headers.Add("X-Seq-ApiKey", AmetoWebAppFactory.TestApiKey);

        using var response = await client.SendAsync(request);

        Assert.Equal(200, (int)response.StatusCode);                                  // %{http_code}
        var events = await TestHelpers.WaitForEventsAsync(_client, 1, $"@mt = '{marker} record 0'", Patience);
        Assert.Single(events);
    }

    [Fact]
    public async Task A_gzip_body_without_a_content_length_is_ingested_too()
    {
        // The reader's other road: no Content-Length, so the COMPRESSED body arrives by doubling
        // from 64 KiB before it is inflated.
        var batch   = Batch.Of(Signal.Logs, protobuf: true, count: 3);
        var content = new StreamContent(new UndeclaredLengthStream(OtlpGzipTests.Gzip(batch.Message)));
        content.Headers.ContentType = new MediaTypeHeaderValue("application/x-protobuf");
        content.Headers.ContentEncoding.Add("gzip");

        using var response = await _client.PostAsync("/v1/logs", content);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        await batch.AssertReadableAsync(_client);
    }

    /// <summary>
    /// Every spelling that means "gzip" or "nothing": the header is a case-insensitive,
    /// comma-separated list (RFC 9110), <c>identity</c> in it changes nothing, and
    /// <c>x-gzip</c> is gzip's registered alias.
    /// </summary>
    [Theory]
    [InlineData(null,             false)]
    [InlineData("",               false)]
    [InlineData("identity",       false)]
    [InlineData("IDENTITY",       false)]
    [InlineData("gzip",           true)]
    [InlineData("GZip",           true)]
    [InlineData("x-gzip",         true)]
    [InlineData(" gzip ",         true)]
    [InlineData("identity, gzip", true)]
    [InlineData("gzip,identity",  true)]
    public async Task Identity_and_every_spelling_of_gzip_are_accepted(string? encoding, bool compressed)
    {
        var batch = Batch.Of(Signal.Logs, protobuf: true, count: 1);
        byte[] body = compressed ? OtlpGzipTests.Gzip(batch.Message) : batch.Message;

        using var ledger = IngestBufferPoolLedger.Open();
        var ctx = await SendRawAsync("/v1/logs", body, protobuf: true, encoding is null ? default : new StringValues(encoding));

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        Assert.Equal("{\"ingested\":1,\"dropped\":0}", await ReadBodyAsync(ctx));
        // The body, and for gzip the inflate target — both from the pool, both back. Also the
        // control for the 415 test's "no rent at all", which is only worth something if the
        // ledger sees this road's rents in the first place.
        ledger.AssertEveryBufferCameBackOnce(minRents: compressed ? 2 : 1);
    }

    // ── 413: the ceiling is the inflated size ─────────────────────────────────

    /// <summary>
    /// A batch that fits the limit ONLY compressed: a few thousand ordinary records, each small
    /// and each one the sink would take, about 20 KB on the wire and past 1 MiB inflated. 413,
    /// as for a batch over the limit on the wire — and none of it ingested, which is proved by a
    /// second, small batch sent after it: once THAT is readable, anything of the first that had
    /// been ingested would be readable too.
    /// </summary>
    [Theory]
    [InlineData("/v1/logs",    false)]
    [InlineData("/v1/logs",    true)]
    [InlineData("/v1/traces",  false)]
    [InlineData("/v1/traces",  true)]
    [InlineData("/v1/metrics", false)]
    [InlineData("/v1/metrics", true)]
    public async Task A_body_that_inflates_past_the_limit_is_413_and_none_of_it_is_ingested(string route, bool protobuf)
    {
        var over = Batch.Past(Limit, SignalOf(route), protobuf);
        byte[] gz = OtlpGzipTests.Gzip(over.Message);
        Assert.True(gz.Length < Limit, $"the compressed body ({gz.Length:N0} B) must pass the wire check for this to test the inflated one");

        _factory.Clock.Advance(OtlpGzipTooLargeLog.Interval);                      // a new second: this one is written
        int warned = _factory.Log.Count("OtlpGzipTooLarge", LogLevel.Warning);
        using var refused = await PostAsync(route, gz, protobuf, "gzip");

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, refused.StatusCode);
        Assert.Empty(await refused.Content.ReadAsByteArrayAsync());                    // the wire 413's body: none
        // And logged, unlike the wire 413 — once in its second: the client is told, and so is
        // whoever runs the server (the throttle itself is pinned below).
        Assert.Equal(warned + 1, _factory.Log.Count("OtlpGzipTooLarge", LogLevel.Warning));

        var after = Batch.Of(SignalOf(route), protobuf, count: 1);
        using var accepted = await PostAsync(route, OtlpGzipTests.Gzip(after.Message), protobuf, "gzip");
        Assert.Equal(HttpStatusCode.OK, accepted.StatusCode);
        await after.AssertReadableAsync(_client);
        await over.AssertAbsentAsync(_client);
    }

    /// <summary>
    /// PR #96 review, finding 3: the warning for an inflated 413 is written at most once a second,
    /// carries the count since the last one, and names the latest sender — its API key as the key
    /// list shows it (the first eight hex digits of its SHA-256, never the key) and its address.
    /// It used to be one line per refusal, naming nobody, with a logger created per request.
    /// </summary>
    [Fact]
    public async Task The_inflated_413_warning_is_once_a_second_with_the_count_and_the_sender()
    {
        byte[] bomb = GzipBomb.Payload;

        // A new second, and whatever the earlier tests left pending written out with it.
        _factory.Clock.Advance(OtlpGzipTooLargeLog.Interval);
        using (var flush = await PostAsync("/v1/logs", bomb, protobuf: true, "gzip"))
            Assert.Equal(HttpStatusCode.RequestEntityTooLarge, flush.StatusCode);
        int warned = _factory.Log.Count("OtlpGzipTooLarge", LogLevel.Warning);

        // Two more in the same second: refused, counted, not written.
        for (int i = 0; i < 2; i++)
        {
            using var refused = await PostAsync("/v1/traces", bomb, protobuf: true, "gzip");
            Assert.Equal(HttpStatusCode.RequestEntityTooLarge, refused.StatusCode);
        }
        Assert.Equal(warned, _factory.Log.Count("OtlpGzipTooLarge", LogLevel.Warning));

        // The next second: one line, for all three, naming the one that tripped it.
        _factory.Clock.Advance(OtlpGzipTooLargeLog.Interval);
        var last = await SendRawAsync("/v1/metrics", bomb, protobuf: true, new StringValues("gzip"),
                                      from: IPAddress.Parse("203.0.113.7"));
        Assert.Equal(StatusCodes.Status413PayloadTooLarge, last.Response.StatusCode);
        Assert.Equal(warned + 1, _factory.Log.Count("OtlpGzipTooLarge", LogLevel.Warning));

        var line = _factory.Log.Last("OtlpGzipTooLarge");
        Assert.Equal(3L, line["Count"]);
        Assert.Equal(Limit, line["Limit"]);
        Assert.Equal(KeyListPreview(AmetoWebAppFactory.TestApiKey), line["KeyPreview"]);
        Assert.Equal("203.0.113.7", line["RemoteAddress"]);
    }

    /// <summary>What <c>GET /api/auth/keys</c> shows for a key: <c>KeyHash[..8]</c>, the hash being AuthStore's.</summary>
    private static string KeyListPreview(string key)
        => Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(key))).ToLowerInvariant()[..8];

    /// <summary>
    /// THE BOMB, through the receiver: ~250 KB that inflate to 256 MiB, on every route. 413, and
    /// what the request cost is bounded by the limit, not by what the body describes: no rent
    /// past the limit; at most the compressed body plus one limit-sized buffer held at once; and
    /// under <see cref="BombRequestAllocationBudget"/> allocated in the whole process for the
    /// whole request — against the 256 MiB and more the same request allocates without the
    /// ceiling. <c>OtlpGzipTests</c> measures the inflate itself to the byte.
    /// </summary>
    [Theory]
    [MemberData(nameof(EveryRoute))]
    public async Task A_gzip_bomb_is_413_and_costs_at_most_the_limit(string route)
    {
        byte[] bomb = GzipBomb.Payload;
        Assert.True(bomb.Length < Limit / 2, $"the bomb must pass the wire check; it is {bomb.Length:N0} bytes");

        using var ledger = IngestBufferPoolLedger.Open();
        long before = GC.GetTotalAllocatedBytes(precise: true);
        using var response = await PostAsync(route, bomb, protobuf: true, "gzip");
        long allocated = GC.GetTotalAllocatedBytes(precise: true) - before;

        _out.WriteLine($"{route}: {(int)response.StatusCode}, {allocated:N0} B allocated process-wide, largest rent "
                     + $"{ledger.LargestRent:N0} B, peak held {ledger.PeakOutstandingBytes:N0} B");

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
        Assert.True(ledger.LargestRent <= Limit, $"a rent of {ledger.LargestRent:N0} B passed the {Limit:N0} B limit");
        // The compressed body's bucket (under half the limit) and the one inflate buffer.
        Assert.True(ledger.PeakOutstandingBytes <= Limit / 2 + Limit,
            $"the request held {ledger.PeakOutstandingBytes:N0} B of pooled buffers at once");
        ledger.AssertEveryBufferCameBackOnce(minRents: 2);
        Assert.True(allocated < BombRequestAllocationBudget,
            $"the bomb request allocated {allocated:N0} B; the budget is {BombRequestAllocationBudget:N0} B");
    }

    [Fact]
    public async Task A_bomb_whose_trailer_lies_low_still_stops_at_the_limit()
    {
        // ISIZE = 1: the inflate buffer starts small and doubles into the limit instead of being
        // sized once. Two buffers are alive at the last copy, so the bound is a limit and a half
        // plus the compressed body — and still nothing past the limit in any one rent.
        byte[] bomb = (byte[])GzipBomb.Payload.Clone();
        BinaryPrimitives.WriteUInt32LittleEndian(bomb.AsSpan(bomb.Length - 4), 1);

        using var ledger   = IngestBufferPoolLedger.Open();
        using var response = await PostAsync("/v1/traces", bomb, protobuf: true, "gzip");

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
        Assert.True(ledger.LargestRent <= Limit, $"a rent of {ledger.LargestRent:N0} B passed the {Limit:N0} B limit");
        Assert.True(ledger.PeakOutstandingBytes <= Limit / 2 + Limit + Limit / 2,
            $"the request held {ledger.PeakOutstandingBytes:N0} B of pooled buffers at once");
        ledger.AssertEveryBufferCameBackOnce(minRents: 3);
    }

    // ── 415: a coding this receiver does not undo ─────────────────────────────

    /// <summary>
    /// Refused BEFORE the body is read — not a byte rented for it — with the status RFC 9110
    /// gives an unsupported content coding, <c>Accept-Encoding</c> naming what would have
    /// worked, and a body in the OTLP failure shape (a <c>google.rpc.Status</c> with only a
    /// <c>message</c>, encoded like the request) that says it in words. Before, each of these
    /// was a 400 from a parser handed compressed bytes, which reads as "your data is malformed".
    /// </summary>
    [Theory]
    [InlineData("/v1/logs",         "deflate",    false)]
    [InlineData("/v1/logs",         "deflate",    true)]
    [InlineData("/v1/traces",       "zstd",       true)]
    [InlineData("/v1/metrics",      "br",         false)]
    [InlineData("/otlp/v1/logs",    "compress",   true)]
    [InlineData("/otlp/v1/traces",  "gzip, br",   false)]
    [InlineData("/otlp/v1/metrics", "gzip, gzip", true)]
    [InlineData("/v1/logs",         "snappy",     true)]
    public async Task A_coding_other_than_gzip_is_415_before_the_body_is_read(string route, string encoding, bool protobuf)
    {
        var batch = Batch.Of(SignalOf(route), protobuf, count: 1);

        using var ledger = IngestBufferPoolLedger.Open();
        var ctx = await SendRawAsync(route, batch.Message, protobuf, new StringValues(encoding));

        Assert.Equal(StatusCodes.Status415UnsupportedMediaType, ctx.Response.StatusCode);
        Assert.Equal("gzip, identity", ctx.Response.Headers.AcceptEncoding.ToString());
        Assert.Equal(0, ledger.Rents);

        string expected = $"Content-Encoding '{encoding}' is not supported; send gzip or identity";
        byte[] body = await ReadBodyBytesAsync(ctx);
        if (protobuf)
        {
            Assert.Equal("application/x-protobuf", ctx.Response.ContentType);
            Assert.Equal(0x12, body[0]);                                               // Status.message = 2, length-delimited
            Assert.Equal(body.Length - 2, (int)body[1]);
            Assert.Equal(expected, Encoding.UTF8.GetString(body, 2, body.Length - 2));
        }
        else
        {
            Assert.Equal("application/json", ctx.Response.ContentType);
            using var doc = JsonDocument.Parse(body);
            Assert.Equal(expected, doc.RootElement.GetProperty("message").GetString());
        }
    }

    // ── 400: gzip that does not inflate ───────────────────────────────────────

    /// <summary>
    /// Fifty records per body, so that a prefix — which a cut-off stream used to inflate to,
    /// silently — would show up in the read-back. 400 on every kind of damage, never a 500, every
    /// buffer back in the pool, and nothing of the batch ingested.
    /// </summary>
    [Theory]
    [InlineData("/v1/logs",         "cut in half",       true)]
    [InlineData("/v1/logs",         "trailer cut off",   false)]
    [InlineData("/v1/traces",       "cut in half",       false)]
    [InlineData("/v1/traces",       "a flipped byte",    true)]
    [InlineData("/v1/metrics",      "cut in half",       true)]
    [InlineData("/v1/metrics",      "trailer cut off",   false)]
    [InlineData("/otlp/v1/logs",    "not gzip at all",   false)]
    [InlineData("/otlp/v1/traces",  "only the header",   true)]
    [InlineData("/otlp/v1/metrics", "a flipped byte",    false)]
    public async Task Gzip_that_does_not_inflate_is_400_and_none_of_it_is_ingested(string route, string damage, bool protobuf)
    {
        var batch  = Batch.Of(SignalOf(route), protobuf, count: 50);
        byte[] gz  = OtlpGzipTests.Gzip(batch.Message);
        byte[] bad = damage switch
        {
            "cut in half"     => gz[..(gz.Length / 2)],
            "trailer cut off" => gz[..^8],
            "a flipped byte"  => Flipped(gz, gz.Length / 2),
            "not gzip at all" => batch.Message,
            "only the header" => gz[..10],
            _ => throw new ArgumentOutOfRangeException(nameof(damage)),
        };

        using (var ledger = IngestBufferPoolLedger.Open())
        {
            using var response = await PostAsync(route, bad, protobuf, "gzip");
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            ledger.AssertEveryBufferCameBackOnce(minRents: 2);
        }

        var after = Batch.Of(SignalOf(route), protobuf, count: 1);
        using var accepted = await PostAsync(route, OtlpGzipTests.Gzip(after.Message), protobuf, "gzip");
        Assert.Equal(HttpStatusCode.OK, accepted.StatusCode);
        await after.AssertReadableAsync(_client);
        await batch.AssertAbsentAsync(_client);
    }

    // ── Empty ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// A zero-length body labelled gzip holds no member to inflate. It answers exactly what a
    /// zero-length body answers without the label — the status and the bytes — whatever that is
    /// for the route and encoding; for protobuf, where an empty message is a valid empty export,
    /// that is a 200 with nothing ingested.
    /// </summary>
    [Theory]
    [MemberData(nameof(EveryRouteBothEncodings))]
    public async Task Gzip_over_an_empty_body_answers_what_an_empty_body_answers(string route, bool protobuf)
    {
        HttpResponseMessage plain, gzip;
        int plainRents, gzipRents;
        using (var ledger = IngestBufferPoolLedger.Open())
        {
            plain      = await PostAsync(route, [], protobuf, encoding: null);
            plainRents = ledger.Rents;
        }
        using (var ledger = IngestBufferPoolLedger.Open())
        {
            gzip      = await PostAsync(route, [], protobuf, "gzip");
            gzipRents = ledger.Rents;
        }
        using var _ = plain;
        using var __ = gzip;

        // And costs what it does: nothing to inflate is no inflate buffer and no inflater.
        Assert.Equal(plainRents, gzipRents);
        Assert.Equal(plain.StatusCode, gzip.StatusCode);
        Assert.Equal(await plain.Content.ReadAsStringAsync(), await gzip.Content.ReadAsStringAsync());
        if (protobuf)
        {
            Assert.Equal(HttpStatusCode.OK, gzip.StatusCode);
            Assert.Equal("{\"ingested\":0,\"dropped\":0}", await gzip.Content.ReadAsStringAsync());
        }
    }

    // ── The header and the refusal body, without a host ───────────────────────

    [Theory]
    [InlineData("Identity")]
    [InlineData("Identity",    "")]
    [InlineData("Identity",    "identity")]
    [InlineData("Identity",    "identity", "")]
    [InlineData("Identity",    " , identity ,")]
    [InlineData("Gzip",        "gzip")]
    [InlineData("Gzip",        "X-GZIP")]
    [InlineData("Gzip",        "\tgzip\t")]
    [InlineData("Gzip",        "identity", "gzip")]
    [InlineData("Gzip",        "gzip,")]
    [InlineData("Unsupported", "gzip", "gzip")]
    [InlineData("Unsupported", "gzip, gzip")]
    [InlineData("Unsupported", "deflate")]
    [InlineData("Unsupported", "br")]
    [InlineData("Unsupported", "zstd")]
    [InlineData("Unsupported", "gzip;q=1")]
    [InlineData("Unsupported", "gzipx")]
    [InlineData("Unsupported", "gzip", "br")]
    public void The_header_is_read_as_a_list_of_codings(string expected, params string[] values)
        => Assert.Equal(Enum.Parse<OtlpEndpointMapper.ContentCoding>(expected), OtlpEndpointMapper.ClassifyContentEncoding(new StringValues(values)));

    [Fact]
    public void The_refusal_echoes_the_header_capped_and_made_safe_for_a_json_string()
    {
        // The client's own input goes into the body, so it is cut to 64 characters and anything
        // that could end or escape a JSON string — or is not printable ASCII — becomes '?'.
        string hostile = "br\"},\"x\":\"\\ é\n" + new string('z', 200);
        var buffer = new byte[OtlpEndpointMapper.UnsupportedEncodingMaxBytes];

        int n = OtlpEndpointMapper.FormatUnsupportedEncoding(buffer, new StringValues(["deflate", hostile]), protobuf: false);

        using var doc = JsonDocument.Parse(buffer.AsMemory(0, n));
        string message = doc.RootElement.GetProperty("message").GetString()!;
        string echoed  = message["Content-Encoding '".Length..message.IndexOf("' is not", StringComparison.Ordinal)];
        Assert.Equal(OtlpEndpointMapper.EchoMaxChars, echoed.Length);
        Assert.StartsWith("deflate, br?},?x?:?? ??z", echoed, StringComparison.Ordinal);

        // And the same text as a protobuf Status still fits a one-byte length.
        n = OtlpEndpointMapper.FormatUnsupportedEncoding(buffer, new StringValues(["deflate", hostile]), protobuf: true);
        Assert.Equal(n - 2, (int)buffer[1]);
        Assert.True(buffer[1] < 0x80, "the message length must stay a one-byte varint");
    }

    // ── Harness ───────────────────────────────────────────────────────────────

    private enum Signal { Logs, Traces, Metrics }

    private static Signal SignalOf(string route)
        => route.EndsWith("logs", StringComparison.Ordinal)   ? Signal.Logs
         : route.EndsWith("traces", StringComparison.Ordinal) ? Signal.Traces
         :                                                      Signal.Metrics;

    private static string Marker() => "gz" + Guid.NewGuid().ToString("N")[..12];

    private static byte[] Flipped(byte[] bytes, int at)
    {
        byte[] copy = (byte[])bytes.Clone();
        copy[at] ^= 0xFF;
        return copy;
    }

    private Task<HttpResponseMessage> PostAsync(string route, byte[] body, bool protobuf, string? encoding)
    {
        var content = new ByteArrayContent(body);
        content.Headers.ContentType = new MediaTypeHeaderValue(protobuf ? "application/x-protobuf" : "application/json");
        if (encoding is not null) content.Headers.ContentEncoding.Add(encoding);
        return _client.PostAsync(route, content);
    }

    /// <summary>
    /// Straight into the server, for the cases the client would get in the way of: a header
    /// value exactly as written (HttpClient validates Content-Encoding tokens), and a response
    /// header — <c>Accept-Encoding</c> — that HttpClient's typed collections have no place for
    /// on a response.
    /// </summary>
    private async Task<HttpContext> SendRawAsync(string route, byte[] body, bool protobuf, StringValues encoding,
                                                 IPAddress? from = null)
    {
        var ctx = await _factory.Server.SendAsync(c =>
        {
            c.Request.Method        = "POST";
            c.Request.Path          = route;
            c.Request.ContentType   = protobuf ? "application/x-protobuf" : "application/json";
            c.Request.ContentLength = body.Length;
            c.Request.Body          = new MemoryStream(body);
            c.Request.Headers["X-Seq-ApiKey"] = AmetoWebAppFactory.TestApiKey;
            c.Connection.RemoteIpAddress      = from;
            if (encoding.Count > 0)
                c.Request.Headers.ContentEncoding = encoding;
        }).WaitAsync(Patience);
        return ctx;
    }

    private static async Task<byte[]> ReadBodyBytesAsync(HttpContext ctx)
    {
        using var ms = new MemoryStream();
        await ctx.Response.Body.CopyToAsync(ms);
        return ms.ToArray();
    }

    private static async Task<string> ReadBodyAsync(HttpContext ctx) => Encoding.UTF8.GetString(await ReadBodyBytesAsync(ctx));

    /// <summary>
    /// One export request and how to find what it carried: a log body, a trace id, or a metric
    /// name no other test uses, in both encodings — the protobuf written field by field against
    /// the .proto, the JSON as OTLP/JSON spells it.
    /// </summary>
    private sealed class Batch
    {
        public required Signal Signal  { get; init; }
        public required string Marker  { get; init; }
        public required int    Count   { get; init; }
        public required byte[] Message { get; init; }

        private string TraceIdHex => TraceIdOf(Marker);
        private string MetricName => MetricNameOf(Marker);

        /// <summary>The trace every span of a traces batch belongs to: the marker's twelve hex digits, in a 32-digit id.</summary>
        private static string TraceIdOf(string marker) => "0a" + marker[2..] + "000000000000000001";

        private static string MetricNameOf(string marker) => "gzip.probe." + marker;

        public static Batch Of(Signal signal, bool protobuf, int count, int padding = 0)
        {
            string marker = Marker();
            byte[] message = (signal, protobuf) switch
            {
                (Signal.Logs,    true)  => LogsProto(marker, count, padding),
                (Signal.Logs,    false) => Encoding.UTF8.GetBytes(LogsJson(marker, count, padding)),
                (Signal.Traces,  true)  => TracesProto(TraceIdOf(marker), marker, count, padding),
                (Signal.Traces,  false) => Encoding.UTF8.GetBytes(TracesJson(TraceIdOf(marker), marker, count, padding)),
                (Signal.Metrics, true)  => MetricsProto(MetricNameOf(marker), count, padding),
                _                       => Encoding.UTF8.GetBytes(MetricsJson(MetricNameOf(marker), count, padding)),
            };
            return new Batch { Signal = signal, Marker = marker, Count = count, Message = message };
        }

        /// <summary>Records of ordinary size — each one the sink takes — until the message is past <paramref name="limit"/>.</summary>
        public static Batch Past(int limit, Signal signal, bool protobuf)
        {
            for (int count = 256; ; count *= 2)
            {
                var batch = Of(signal, protobuf, count, padding: 200);
                if (batch.Message.Length > limit) return batch;
            }
        }

        public async Task AssertReadableAsync(HttpClient client)
        {
            switch (Signal)
            {
                case Signal.Logs:
                    var events = await TestHelpers.WaitForEventsAsync(
                        client, Count, $"startsWith(@mt, '{Marker} ')", Patience);
                    Assert.Equal(Count, events.Count);
                    // The LAST record is the one a short read or a wrong length loses.
                    Assert.Contains(events, e => e.GetProperty("@mt").GetString() == $"{Marker} record {Count - 1}");
                    break;

                case Signal.Traces:
                    var spans = await PollAsync(() => client.GetAsync($"/api/traces/{TraceIdHex}"),
                                                static a => a.GetArrayLength() > 0, Count);
                    Assert.Equal(Count, spans.GetArrayLength());
                    Assert.Contains(spans.EnumerateArray(), s => s.GetProperty("name").GetString() == $"{Marker} span {Count - 1}");
                    break;

                case Signal.Metrics:
                    var series = await PollAsync(() => client.PostAsync("/api/metrics/query", MetricQuery()),
                                                 static a => a.GetArrayLength() > 0, 1);
                    Assert.Equal(MetricName, series[0].GetProperty("name").GetString());
                    Assert.True(series[0].GetProperty("points").GetArrayLength() > 0, "the series came back with no points");
                    break;
            }
        }

        public async Task AssertAbsentAsync(HttpClient client)
        {
            switch (Signal)
            {
                case Signal.Logs:
                    var events = await TestHelpers.StreamEventsAsync(
                        client, TestHelpers.EventsUrl(count: 1, filter: $"startsWith(@mt, '{Marker} ')"));
                    Assert.Empty(events);
                    break;

                case Signal.Traces:
                    using (var response = await client.GetAsync($"/api/traces/{TraceIdHex}"))
                        Assert.Equal("[]", await response.Content.ReadAsStringAsync());
                    break;

                case Signal.Metrics:
                    using (var response = await client.PostAsync("/api/metrics/query", MetricQuery()))
                        Assert.Equal("[]", await response.Content.ReadAsStringAsync());
                    break;
            }
        }

        private StringContent MetricQuery()
            => new("{\"metric\":\"" + MetricName + "\"}", Encoding.UTF8, "application/json");

        private static async Task<JsonElement> PollAsync(
            Func<Task<HttpResponseMessage>> send, Func<JsonElement, bool> ready, int expected)
        {
            var deadline = DateTime.UtcNow + Patience;
            while (true)
            {
                using (var response = await send())
                {
                    Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                    var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement.Clone();
                    if (ready(json) && json.GetArrayLength() >= expected) return json;
                }
                if (DateTime.UtcNow >= deadline) throw new TimeoutException($"not readable after {Patience.TotalSeconds:F0} s");
                await Task.Delay(20);
            }
        }

        // ── Payloads ──────────────────────────────────────────────────────────

        private static long NowNanos(int i) => (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 60_000 + i) * 1_000_000L;

        private static string Pad(int padding) => new('p', padding);

        private const string JsonResource =
            "\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"gzip-probe\"}}]}";

        private static string JsonAttr(string key, string value)
            => "{\"key\":\"" + key + "\",\"value\":{\"stringValue\":\"" + value + "\"}}";

        public static string LogsJson(string marker, int count, int padding = 0)
        {
            var sb = new StringBuilder("{\"resourceLogs\":[{").Append(JsonResource).Append(",\"scopeLogs\":[{\"logRecords\":[");
            for (int i = 0; i < count; i++)
            {
                if (i > 0) sb.Append(',');
                sb.Append("{\"timeUnixNano\":\"").Append(NowNanos(i).ToString(CultureInfo.InvariantCulture))
                  .Append("\",\"severityNumber\":9,\"body\":{\"stringValue\":\"").Append(marker).Append(" record ")
                  .Append(i.ToString(CultureInfo.InvariantCulture)).Append("\"}");
                if (padding > 0) sb.Append(",\"attributes\":[").Append(JsonAttr("pad", Pad(padding))).Append(']');
                sb.Append('}');
            }
            return sb.Append("]}]}]}").ToString();
        }

        private static string TracesJson(string traceId, string marker, int count, int padding)
        {
            var sb = new StringBuilder("{\"resourceSpans\":[{").Append(JsonResource).Append(",\"scopeSpans\":[{\"spans\":[");
            for (int i = 0; i < count; i++)
            {
                long start = NowNanos(i);
                if (i > 0) sb.Append(',');
                sb.Append("{\"traceId\":\"").Append(traceId).Append("\",\"spanId\":\"").Append(SpanIdHex(i))
                  .Append("\",\"name\":\"").Append(marker).Append(" span ").Append(i.ToString(CultureInfo.InvariantCulture))
                  .Append("\",\"kind\":2,\"startTimeUnixNano\":\"").Append(start.ToString(CultureInfo.InvariantCulture))
                  .Append("\",\"endTimeUnixNano\":\"").Append((start + 1_000_000).ToString(CultureInfo.InvariantCulture)).Append('"');
                if (padding > 0) sb.Append(",\"attributes\":[").Append(JsonAttr("pad", Pad(padding))).Append(']');
                sb.Append('}');
            }
            return sb.Append("]}]}]}").ToString();
        }

        private static string MetricsJson(string name, int count, int padding)
        {
            var sb = new StringBuilder("{\"resourceMetrics\":[{").Append(JsonResource)
                .Append(",\"scopeMetrics\":[{\"metrics\":[{\"name\":\"").Append(name).Append("\",\"gauge\":{\"dataPoints\":[");
            for (int i = 0; i < count; i++)
            {
                if (i > 0) sb.Append(',');
                sb.Append("{\"timeUnixNano\":\"").Append(NowNanos(i).ToString(CultureInfo.InvariantCulture)).Append("\",\"asDouble\":42.5");
                if (padding > 0) sb.Append(",\"attributes\":[").Append(JsonAttr("pad", Pad(padding))).Append(']');
                sb.Append('}');
            }
            return sb.Append("]}}]}]}]}").ToString();
        }

        private static string SpanIdHex(int i) => (0x5A00_0000_0000_0000UL + (ulong)i + 1).ToString("x16", CultureInfo.InvariantCulture);

        private static byte[] Msg(Action<CodedOutputStream> body)
        {
            using var ms = new MemoryStream();
            var o = new CodedOutputStream(ms);
            body(o);
            o.Flush();
            return ms.ToArray();
        }

        private static void Sub(CodedOutputStream o, int field, byte[] payload)
        {
            o.WriteTag(field, WireFormat.WireType.LengthDelimited);
            o.WriteBytes(ByteString.CopyFrom(payload));
        }

        private static byte[] StringAttr(string key, string value) => Msg(c =>
        {
            c.WriteTag(1, WireFormat.WireType.LengthDelimited); c.WriteString(key);
            Sub(c, 2, Msg(v => { v.WriteTag(1, WireFormat.WireType.LengthDelimited); v.WriteString(value); }));
        });

        private static byte[] Resource() => Msg(r => Sub(r, 1, StringAttr("service.name", "gzip-probe")));

        private static byte[] LogsProto(string marker, int count, int padding) => Msg(c =>
            Sub(c, 1, Msg(rl =>                                                      // resource_logs
            {
                Sub(rl, 1, Resource());
                Sub(rl, 2, Msg(sl =>                                                 // scope_logs
                {
                    for (int i = 0; i < count; i++)
                    {
                        int ii = i;
                        Sub(sl, 2, Msg(lr =>                                         // log_records
                        {
                            lr.WriteTag(1, WireFormat.WireType.Fixed64); lr.WriteFixed64((ulong)NowNanos(ii));
                            lr.WriteTag(2, WireFormat.WireType.Varint);  lr.WriteEnum(9);
                            Sub(lr, 5, Msg(b => { b.WriteTag(1, WireFormat.WireType.LengthDelimited); b.WriteString($"{marker} record {ii}"); }));
                            if (padding > 0) Sub(lr, 6, StringAttr("pad", Pad(padding)));
                        }));
                    }
                }));
            })));

        private static byte[] TracesProto(string traceId, string marker, int count, int padding) => Msg(c =>
            Sub(c, 1, Msg(rs =>                                                      // resource_spans
            {
                Sub(rs, 1, Resource());
                Sub(rs, 2, Msg(ss =>                                                 // scope_spans
                {
                    for (int i = 0; i < count; i++)
                    {
                        int ii = i;
                        long start = NowNanos(ii);
                        Sub(ss, 2, Msg(sp =>                                         // spans
                        {
                            sp.WriteTag(1, WireFormat.WireType.LengthDelimited); sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString(traceId)));
                            sp.WriteTag(2, WireFormat.WireType.LengthDelimited); sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString(SpanIdHex(ii))));
                            sp.WriteTag(5, WireFormat.WireType.LengthDelimited); sp.WriteString($"{marker} span {ii}");
                            sp.WriteTag(6, WireFormat.WireType.Varint);          sp.WriteEnum(2);
                            sp.WriteTag(7, WireFormat.WireType.Fixed64);         sp.WriteFixed64((ulong)start);
                            sp.WriteTag(8, WireFormat.WireType.Fixed64);         sp.WriteFixed64((ulong)(start + 1_000_000));
                            if (padding > 0) Sub(sp, 9, StringAttr("pad", Pad(padding)));
                        }));
                    }
                }));
            })));

        private static byte[] MetricsProto(string name, int count, int padding) => Msg(c =>
            Sub(c, 1, Msg(rm =>                                                      // resource_metrics
            {
                Sub(rm, 1, Resource());
                Sub(rm, 2, Msg(sm => Sub(sm, 2, Msg(metric =>                        // scope_metrics.metrics
                {
                    metric.WriteTag(1, WireFormat.WireType.LengthDelimited); metric.WriteString(name);
                    Sub(metric, 5, Msg(gauge =>                                      // gauge
                    {
                        for (int i = 0; i < count; i++)
                        {
                            int ii = i;
                            Sub(gauge, 1, Msg(dp =>                                  // data_points
                            {
                                dp.WriteTag(3, WireFormat.WireType.Fixed64); dp.WriteFixed64((ulong)NowNanos(ii));
                                dp.WriteTag(4, WireFormat.WireType.Fixed64); dp.WriteDouble(42.5);
                                if (padding > 0) Sub(dp, 7, StringAttr("pad", Pad(padding)));
                            }));
                        }
                    }));
                }))));
            })));
    }

    /// <summary>A body that will not state its length, so the client sends it chunked.</summary>
    private sealed class UndeclaredLengthStream(byte[] bytes) : Stream
    {
        private int _position;

        public override bool CanRead  => true;
        public override bool CanSeek  => false;
        public override bool CanWrite => false;
        public override long Length   => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int n = Math.Min(count, bytes.Length - _position);
            Array.Copy(bytes, _position, buffer, offset, n);
            _position += n;
            return n;
        }

        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
