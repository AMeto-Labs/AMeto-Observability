using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Ameto.Otel;
using Ameto.Testing;
using Ameto.Tracing;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using EventId  = Microsoft.Extensions.Logging.EventId;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Ameto.Integration.Tests;

/// <summary>
/// AN OTLP BATCH THE SERVER RAN OUT OF MEMORY TAKING IN IS RETRIED, NOT DROPPED (#125).
///
/// <para>On the 512 MB stand the metric WAL's append ran out of memory while a compaction held
/// the heap. The OutOfMemoryException left the OTLP/HTTP handler and hosting answered 500, which
/// OTLP exporters never retry; over gRPC the decode's catch-all answered INVALID_ARGUMENT, which
/// they never retry either; and over HTTP a parser's catch-all made the same failure in a log or
/// trace batch a 400. Each dropped a valid batch for the server's own failure. Now any
/// OutOfMemoryException while a batch is read, inflated, parsed or stored is answered 503 with
/// <c>Retry-After</c> over HTTP and UNAVAILABLE over gRPC — the answers exporters retry — with
/// every buffer and inflate slot given back, and one throttled error line.</para>
///
/// <para>The faults: each store this host builds — the metric ingester, the span sink, and the log
/// sink's logger, which an oversized record calls — throws OutOfMemoryException when a test arms
/// its signal, and only on a batch of this class's (<see cref="Marker"/>); the read's is an
/// <see cref="IngestBufferPool"/> rent that fails, through <see cref="IngestBufferPoolLedger"/>.</para>
/// </summary>
public sealed class OtlpOutOfMemoryTests : IClassFixture<OtlpOutOfMemoryTests.Factory>
{
    /// <summary>What every batch of this class carries — a metric name, a span name, a log body — and what the faults look for.</summary>
    private const string Marker = "oom-probe";

    private const string Message = "the server ran short of memory taking in this batch; retry";

    public sealed class Factory : AmetoWebAppFactory
    {
        /// <summary>The signal ("logs", "traces", "metrics") whose store throws on this class's batches; null for none.</summary>
        internal volatile string? Failing;

        /// <summary>
        /// The metric and span faults arrive as an <see cref="AggregateException"/> around the
        /// OutOfMemoryException — what Microsoft.Extensions.Logging's logger rethrows when a provider
        /// fails inside a sink's own log call (#126 review F3). The log fault always does: its logger
        /// is built through a <see cref="LoggerFactory"/>, as production's is.
        /// </summary>
        internal volatile bool Aggregate;

        /// <summary>A log provider of the host fails while it writes the OtlpOutOfMemory line itself.</summary>
        internal volatile bool FailingLine;

        /// <summary>What the host logs, with exceptions.</summary>
        public CapturedLog Log { get; } = new();

        /// <summary>The clock the out-of-memory error is throttled by: it moves only when a test says so.</summary>
        internal ManualTimeProvider Clock { get; } = new();

        /// <summary>The log sink's own logger factory: a provider that fails on an oversized record of this class's.</summary>
        private ILoggerFactory? _sinkLoggers;

        internal Exception Fault(string what) =>
            Aggregate ? new AggregateException(new OutOfMemoryException(what)) : new OutOfMemoryException(what);

        protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            _sinkLoggers = LoggerFactory.Create(b => b.AddProvider(new FailingSinkProvider(this)));
            // After the app's own registrations, so each of these is the one resolved.
            builder.ConfigureTestServices(services =>
            {
                services.AddSingleton<ILoggerProvider>(Log);
                services.AddSingleton<ILoggerProvider>(new FailingLineProvider(this));
                services.AddSingleton(sp => new OtlpOutOfMemoryLog(
                    sp.GetRequiredService<ILoggerFactory>().CreateLogger("Ameto.Otel"), Clock));
                services.AddSingleton<IMetricIngester>(sp =>
                    new FailingIngester(this, sp.GetRequiredService<MetricStorageEngine>()));
                services.AddSingleton<ISpanSink>(sp =>
                    new FailingSpanSink(this, sp.GetRequiredService<Ameto.Tracing.Ingestion.SpanIngestionEndpoint>()));
                services.AddSingleton(sp => new IngestionEndpoint(
                    sp.GetRequiredService<IngestionRingBuffer>(),
                    sp.GetRequiredService<StringInternPool>(),
                    sp.GetRequiredService<IngestionDrainer>(),
                    sp.GetRequiredService<ServerOptions>(),
                    _sinkLoggers.CreateLogger<IngestionEndpoint>()));
            });
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing) _sinkLoggers?.Dispose();
        }
    }

    private readonly Factory    _factory;
    private readonly HttpClient _client;

    public OtlpOutOfMemoryTests(Factory factory)
    {
        _factory = factory;
        factory.Server.PreserveExecutionContext = true;                  // the pool ledger is an AsyncLocal
        _client = factory.CreateClient();
    }

    // ── OTLP/HTTP: 503 + Retry-After ──────────────────────────────────────────

    /// <summary>
    /// The store runs out: the metric WAL's append (the stand's failure, a 500 until now), the span
    /// ring, the log sink (each a 400 until now — a parser's catch-all took it for a malformed
    /// payload). 503, <c>Retry-After: 1</c>, the OTLP Status saying why; every pooled buffer back,
    /// and with gzip the inflate slot too. Then the exporter's retry, with the server recovered:
    /// the same bytes, taken.
    /// </summary>
    [Theory]
    [InlineData("/v1/metrics", false)]
    [InlineData("/v1/metrics", true)]
    [InlineData("/v1/traces",  false)]
    [InlineData("/v1/traces",  true)]
    [InlineData("/v1/logs",    false)]
    [InlineData("/v1/logs",    true)]
    public async Task A_store_that_runs_out_of_memory_is_503_with_Retry_After_and_the_retry_is_taken(string route, bool gzip)
    {
        byte[] message = Batch(route);
        byte[] body    = gzip ? OtlpGzipTests.Gzip(message) : message;
        var gate       = _factory.Services.GetRequiredService<OtlpInflateGate>();

        _factory.Failing = SignalOf(route);
        try
        {
            using var ledger  = IngestBufferPoolLedger.Open();
            using var refused = await PostAsync(route, body, protobuf: false, gzip);

            Assert.Equal(HttpStatusCode.ServiceUnavailable, refused.StatusCode);
            Assert.Equal(TimeSpan.FromSeconds(1), refused.Headers.RetryAfter?.Delta);
            Assert.Equal("application/json", refused.Content.Headers.ContentType?.MediaType);
            Assert.Equal(Message, StatusMessage(await refused.Content.ReadAsByteArrayAsync(), protobuf: false));
            ledger.AssertEveryBufferCameBackOnce(minRents: gzip ? 2 : 1);      // the body; with gzip, the inflate too
            Assert.Equal(gate.Capacity, gate.Available);
        }
        finally
        {
            _factory.Failing = null;
        }

        using var retried = await PostAsync(route, body, protobuf: false, gzip);
        Assert.Equal(HttpStatusCode.OK, retried.StatusCode);
    }

    /// <summary>
    /// The same, when the OutOfMemoryException arrives inside an <see cref="AggregateException"/> —
    /// what Microsoft.Extensions.Logging's logger rethrows when a provider fails inside a sink's own
    /// log call, such as the span ring's ring-full warning (#126 review F3). It was answered 400:
    /// the parsers' catch-alls tested for OutOfMemoryException alone. (The log route's fault above
    /// always arrives that way: its sink's logger is built through a LoggerFactory.)
    /// </summary>
    [Theory]
    [InlineData("/v1/metrics")]
    [InlineData("/v1/traces")]
    public async Task A_store_out_of_memory_wrapped_in_an_AggregateException_is_still_503(string route)
    {
        _factory.Failing   = SignalOf(route);
        _factory.Aggregate = true;
        try
        {
            using var refused = await PostAsync(route, Batch(route), protobuf: false, gzip: false);
            Assert.Equal(HttpStatusCode.ServiceUnavailable, refused.StatusCode);
            Assert.Equal(TimeSpan.FromSeconds(1), refused.Headers.RetryAfter?.Delta);
            Assert.Equal(Message, StatusMessage(await refused.Content.ReadAsByteArrayAsync(), protobuf: false));
        }
        finally
        {
            _factory.Failing   = null;
            _factory.Aggregate = false;
        }
    }

    /// <summary>
    /// A log provider that fails while it writes the OtlpOutOfMemory line itself — formatting the
    /// exception's stack is an allocation, and the heap is short — must not cost the exporter its
    /// 503 (#126 review F3). On HTTP the 503 is only buffered when the line is written, and the
    /// logger rethrows a provider's failure as an <see cref="AggregateException"/>, which
    /// <see cref="OtlpOutOfMemoryLog.Note"/> did not catch: hosting then answered 500, with no
    /// Retry-After, and the batch was lost.
    /// </summary>
    [Fact]
    public async Task A_log_provider_that_fails_on_the_line_does_not_cost_the_503()
    {
        _factory.Clock.Advance(OtlpOutOfMemoryLog.Interval);                 // a new second: the line is written
        _factory.Failing     = "metrics";
        _factory.FailingLine = true;
        try
        {
            using var refused = await PostAsync("/v1/metrics", Batch("/v1/metrics"), protobuf: false, gzip: false);
            Assert.Equal(HttpStatusCode.ServiceUnavailable, refused.StatusCode);
            Assert.Equal(TimeSpan.FromSeconds(1), refused.Headers.RetryAfter?.Delta);
            Assert.Equal(Message, StatusMessage(await refused.Content.ReadAsByteArrayAsync(), protobuf: false));
        }
        finally
        {
            _factory.Failing     = null;
            _factory.FailingLine = false;
        }
    }

    [Fact]
    public void Note_never_throws_whatever_a_log_provider_does()
    {
        using var loggers = LoggerFactory.Create(b => b.AddProvider(new ThrowingProvider()));
        var log = new OtlpOutOfMemoryLog(loggers.CreateLogger("Ameto.Otel"), TimeProvider.System);
        var ctx = new DefaultHttpContext();
        ctx.Request.Path = "/v1/metrics";

        log.Note(ctx, new OutOfMemoryException("injected"));                  // AggregateException out of the logger, today
    }

    private sealed class ThrowingProvider : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new Sink();
        public void Dispose() { }

        private sealed class Sink : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => true;
            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                                    Func<TState, Exception?, string> formatter)
                => throw new InvalidOperationException("injected: the provider failed");
        }
    }

    /// <summary>
    /// The metric parse runs out — building a big batch's items is an allocation like any other —
    /// and the parse's catch-all, there for malformed payloads, took it for one: a 400. (A log or
    /// trace parse stores as it goes, so the store faults above are their parse faults too.) Also
    /// when the failure arrives inside an <see cref="AggregateException"/> (#126 review F3).
    /// </summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(true,  false)]
    [InlineData(false, true)]
    public async Task A_metric_parse_that_runs_out_of_memory_is_503_not_400(bool gzip, bool aggregate)
    {
        byte[] message = Batch("/v1/metrics");
        byte[] body    = gzip ? OtlpGzipTests.Gzip(message) : message;
        var gate       = _factory.Services.GetRequiredService<OtlpInflateGate>();

        OtlpEndpointMapper.OnMetricsParsedForTest = points =>
        {
            if (points.Count > 0 && points[0].Name.StartsWith(Marker, StringComparison.Ordinal))
                throw aggregate ? new AggregateException(new OutOfMemoryException("injected: the metric parse ran out"))
                                : new OutOfMemoryException("injected: the metric parse ran out");
        };
        try
        {
            using var ledger  = IngestBufferPoolLedger.Open();
            using var refused = await PostAsync("/v1/metrics", body, protobuf: false, gzip);

            Assert.Equal(HttpStatusCode.ServiceUnavailable, refused.StatusCode);
            Assert.Equal(TimeSpan.FromSeconds(1), refused.Headers.RetryAfter?.Delta);
            Assert.Equal(Message, StatusMessage(await refused.Content.ReadAsByteArrayAsync(), protobuf: false));
            ledger.AssertEveryBufferCameBackOnce(minRents: gzip ? 2 : 1);
            Assert.Equal(gate.Capacity, gate.Available);
        }
        finally
        {
            OtlpEndpointMapper.OnMetricsParsedForTest = null;
        }
    }

    /// <summary>
    /// The read runs out — its first rent from the pool every receiver shares. It escaped the
    /// handler as a 500. The refusal is encoded like the request, as every OTLP refusal here is.
    /// </summary>
    [Theory]
    [InlineData("/v1/metrics", true)]
    [InlineData("/v1/metrics", false)]
    [InlineData("/v1/traces",  true)]
    [InlineData("/v1/logs",    true)]
    public async Task A_body_read_that_runs_out_of_memory_is_503_with_Retry_After(string route, bool protobuf)
    {
        byte[] body = protobuf ? [0x0A, 0x00] : Batch(route);

        using var ledger = IngestBufferPoolLedger.Open();
        ledger.FailRent(1, new OutOfMemoryException("injected: the body read ran out"));
        using var refused = await PostAsync(route, body, protobuf, gzip: false);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, refused.StatusCode);
        Assert.Equal(TimeSpan.FromSeconds(1), refused.Headers.RetryAfter?.Delta);
        Assert.Equal(protobuf ? "application/x-protobuf" : "application/json", refused.Content.Headers.ContentType?.MediaType);
        Assert.Equal(Message, StatusMessage(await refused.Content.ReadAsByteArrayAsync(), protobuf));
        ledger.AssertEveryBufferCameBackOnce(minRents: 0);                    // the one rent failed: nothing was out
    }

    /// <summary>
    /// One error line a second, carrying the latest failure's exception and route and the count
    /// since the last line: it is written when memory is short and the next request is likely to
    /// fail the same way, so one line — with a stack — per request is what the server can least
    /// afford. (Hosting's line for the 500 it replaces was one per request.)
    /// </summary>
    [Fact]
    public async Task The_out_of_memory_error_is_once_a_second_with_the_count_and_the_exception()
    {
        try
        {
            // A new second: this one is written, with whatever earlier tests left pending.
            _factory.Clock.Advance(OtlpOutOfMemoryLog.Interval);
            _factory.Failing = "metrics";
            await AssertRefusedAsync("/v1/metrics");
            int written = _factory.Log.Count("OtlpOutOfMemory");
            var first   = _factory.Log.Last("OtlpOutOfMemory");
            Assert.Equal(LogLevel.Error, first.Level);
            Assert.IsType<OutOfMemoryException>(first.Exception);
            Assert.Equal("/v1/metrics", first.Values["Path"]);

            // Two more in the same second: answered, counted, not written.
            _factory.Failing = "traces";
            await AssertRefusedAsync("/v1/traces");
            await AssertRefusedAsync("/v1/traces");
            Assert.Equal(written, _factory.Log.Count("OtlpOutOfMemory"));

            // The next second: one line for all three, naming the one that tripped it.
            _factory.Clock.Advance(OtlpOutOfMemoryLog.Interval);
            _factory.Failing = "logs";
            await AssertRefusedAsync("/v1/logs");
            Assert.Equal(written + 1, _factory.Log.Count("OtlpOutOfMemory"));
            var line = _factory.Log.Last("OtlpOutOfMemory");
            Assert.Equal(3L, line.Values["Count"]);
            Assert.Equal("/v1/logs", line.Values["Path"]);
            // The log sink's failure came through its logger factory: the line carries what was caught.
            var caught = Assert.IsType<AggregateException>(line.Exception);
            Assert.IsType<OutOfMemoryException>(Assert.Single(caught.InnerExceptions));
        }
        finally
        {
            _factory.Failing = null;
        }

        async Task AssertRefusedAsync(string route)
        {
            using var refused = await PostAsync(route, Batch(route), protobuf: false, gzip: false);
            Assert.Equal(HttpStatusCode.ServiceUnavailable, refused.StatusCode);
        }
    }

    // ── OTLP/gRPC: UNAVAILABLE ────────────────────────────────────────────────

    /// <summary>
    /// The decode — which parses AND stores: the metric decode is where the stand's WAL append
    /// sits — runs out: UNAVAILABLE (14), which exporters retry, where its catch-all answered
    /// INVALID_ARGUMENT (3), which they do not. Anything else the decode throws is still
    /// INVALID_ARGUMENT: the batch's fault, not the server's. Driven over a plain context:
    /// TestServer cannot honestly carry gRPC (see <see cref="OtlpGrpcFramingTests"/>).
    /// </summary>
    [Theory]
    [InlineData(false, "oom",       "14")]
    [InlineData(true,  "oom",       "14")]
    [InlineData(false, "aggregate", "14")]   // a sink's log call that ran out (#126 review F3)
    [InlineData(false, "other",     "3")]
    [InlineData(true,  "other",     "3")]
    public async Task A_grpc_decode_that_runs_out_of_memory_is_UNAVAILABLE_and_any_other_failure_INVALID_ARGUMENT(
        bool compressed, string kind, string expected)
    {
        var gate = new OtlpInflateGate(1, TimeSpan.Zero);
        byte[] message = [0x0A, 0x00];
        var call = GrpcCall(Frame(compressed ? OtlpGzipTests.Gzip(message) : message, compressed));
        Exception fault = kind switch
        {
            "oom"       => new OutOfMemoryException("injected: the metric WAL append ran out"),
            "aggregate" => new AggregateException(new OutOfMemoryException("injected: a sink's logger ran out")),
            _           => new InvalidDataException("injected: a malformed message"),
        };

        using var ledger = IngestBufferPoolLedger.Open();
        await OtlpGrpcEndpointMapper.HandleAsync(call, ApiKeyPermissions.Metrics, gate, NoLog, NoMemoryLog,
            (_, _) => throw fault);

        Assert.Equal(expected, call.Response.Headers["grpc-status"].ToString());
        if (kind != "other")
            Assert.Equal(OtlpGrpcEndpointMapper.IngestMemoryShortMessage, call.Response.Headers["grpc-message"].ToString());
        Assert.Equal(1, gate.Available);
        ledger.AssertEveryBufferCameBackOnce(minRents: compressed ? 2 : 1);
    }

    [Fact]
    public async Task A_grpc_body_read_that_runs_out_of_memory_is_UNAVAILABLE()
    {
        var gate = new OtlpInflateGate(1, TimeSpan.Zero);
        var call = GrpcCall(Frame([0x0A, 0x00], compressed: false));
        int decoded = 0;

        using var ledger = IngestBufferPoolLedger.Open();
        ledger.FailRent(1, new OutOfMemoryException("injected: the body read ran out"));
        await OtlpGrpcEndpointMapper.HandleAsync(call, ApiKeyPermissions.Metrics, gate, NoLog, NoMemoryLog,
            (_, _) => { decoded++; return (true, 0, null); });

        Assert.Equal("14", call.Response.Headers["grpc-status"].ToString());
        Assert.Equal(OtlpGrpcEndpointMapper.IngestMemoryShortMessage, call.Response.Headers["grpc-message"].ToString());
        Assert.Equal(0, decoded);
        Assert.Equal(1, gate.Available);
        ledger.AssertEveryBufferCameBackOnce(minRents: 0);
    }

    // ── The faults ────────────────────────────────────────────────────────────

    private sealed class FailingIngester(Factory host, IMetricIngester inner) : IMetricIngester
    {
        public int Ingest(ReadOnlySpan<MetricIngestItem> points)
        {
            if (host.Failing == "metrics" && points.Length > 0 && points[0].Name.StartsWith(Marker, StringComparison.Ordinal))
                throw host.Fault("injected: the metric WAL append ran out");
            return inner.Ingest(points);
        }
    }

    private sealed class FailingSpanSink(Factory host, ISpanSink inner) : ISpanSink
    {
        public int InternService(ReadOnlySpan<byte> serviceUtf8) => inner.InternService(serviceUtf8);

        public bool TryIngestRaw(TraceId traceId, SpanId spanId, SpanId parentSpanId, long startTimeUnixNano,
                                 long durationNanos, ReadOnlySpan<byte> nameUtf8, int serviceIdx,
                                 ReadOnlySpan<byte> serviceUtf8, SpanKind kind, SpanStatusCode status,
                                 short httpStatusCode, ReadOnlySpan<byte> msgpackAttributes)
        {
            if (host.Failing == "traces" && nameUtf8.StartsWith(Encoding.UTF8.GetBytes(Marker)))
                throw host.Fault("injected: the span ring ran out");
            return inner.TryIngestRaw(traceId, spanId, parentSpanId, startTimeUnixNano, durationNanos, nameUtf8,
                                      serviceIdx, serviceUtf8, kind, status, httpStatusCode, msgpackAttributes);
        }

        public void EndBatch() => inner.EndBatch();
    }

    /// <summary>
    /// A provider of the log sink's logger, which the sink calls for an oversized record — this
    /// class's log batch is one. Behind a <see cref="LoggerFactory"/>, as in production, so what
    /// reaches the receiver is the factory's <see cref="AggregateException"/> around it.
    /// </summary>
    private sealed class FailingSinkProvider(Factory host) : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new Sink(host);
        public void Dispose() { }

        private sealed class Sink(Factory host) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => true;
            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                                    Func<TState, Exception?, string> formatter)
            {
                if (host.Failing == "logs" && formatter(state, exception).Contains(Marker, StringComparison.Ordinal))
                    throw new OutOfMemoryException("injected: the log sink ran out");
            }
        }
    }

    /// <summary>A provider of the host that fails, when armed, on the OtlpOutOfMemory line — formatting a stack is an allocation.</summary>
    private sealed class FailingLineProvider(Factory host) : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new Sink(host);
        public void Dispose() { }

        private sealed class Sink(Factory host) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => true;
            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                                    Func<TState, Exception?, string> formatter)
            {
                if (host.FailingLine && eventId.Name == "OtlpOutOfMemory")
                    throw new OutOfMemoryException("injected: the provider ran out formatting the line");
            }
        }
    }

    /// <summary>Log entries by event name, with their structured values and exception.</summary>
    public sealed class CapturedLog : ILoggerProvider
    {
        public sealed record Entry(string? Event, LogLevel Level, Dictionary<string, object?> Values, Exception? Exception);

        private readonly ConcurrentQueue<Entry> _entries = new();

        public int Count(string eventName) => _entries.Count(e => e.Event == eventName);

        public Entry Last(string eventName) => Assert.IsType<Entry>(_entries.LastOrDefault(e => e.Event == eventName));

        public ILogger CreateLogger(string categoryName) => new Sink(this);
        public void Dispose() { }

        private sealed class Sink(CapturedLog log) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => true;
            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                                    Func<TState, Exception?, string> formatter)
            {
                var values = new Dictionary<string, object?>();
                if (state is IReadOnlyList<KeyValuePair<string, object?>> pairs)
                    foreach (var (key, value) in pairs) values[key] = value;
                log._entries.Enqueue(new Entry(eventId.Name, logLevel, values, exception));
            }
        }
    }

    // ── Harness ───────────────────────────────────────────────────────────────

    private static readonly OtlpGzipTooLargeLog NoLog =
        new(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, TimeProvider.System);

    private static readonly OtlpOutOfMemoryLog NoMemoryLog =
        new(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, TimeProvider.System);

    private static string SignalOf(string route)
        => route.EndsWith("logs", StringComparison.Ordinal) ? "logs"
         : route.EndsWith("traces", StringComparison.Ordinal) ? "traces"
         : "metrics";

    private const string JsonResource =
        "\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"" + Marker + "\"}}]}";

    /// <summary>
    /// One record of the route's signal, as OTLP/JSON: a gauge point, a span, or a log record whose
    /// one attribute is past the sink's 64 KiB event limit — so the sink drops it, and logs that it
    /// did, through the logger this host made fallible.
    /// </summary>
    private static byte[] Batch(string route)
    {
        long now = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 60_000) * 1_000_000L;
        string at = now.ToString(CultureInfo.InvariantCulture);
        string json = SignalOf(route) switch
        {
            "metrics" => "{\"resourceMetrics\":[{" + JsonResource + ",\"scopeMetrics\":[{\"metrics\":[{\"name\":\"" + Marker
                       + ".gauge\",\"gauge\":{\"dataPoints\":[{\"timeUnixNano\":\"" + at + "\",\"asDouble\":42.5}]}}]}]}]}",
            "traces"  => "{\"resourceSpans\":[{" + JsonResource + ",\"scopeSpans\":[{\"spans\":[{\"traceId\":\"0a0b0c0d0e0f0a0b0c0d0e0f00000001\""
                       + ",\"spanId\":\"0a0b0c0d0e0f0001\",\"name\":\"" + Marker + " span\",\"kind\":2,\"startTimeUnixNano\":\"" + at
                       + "\",\"endTimeUnixNano\":\"" + (now + 1_000_000).ToString(CultureInfo.InvariantCulture) + "\"}]}]}]}",
            _         => "{\"resourceLogs\":[{" + JsonResource + ",\"scopeLogs\":[{\"logRecords\":[{\"timeUnixNano\":\"" + at
                       + "\",\"severityNumber\":9,\"body\":{\"stringValue\":\"" + Marker + " record\"},\"attributes\":[{\"key\":\"pad\","
                       + "\"value\":{\"stringValue\":\"" + new string('p', 70_000) + "\"}}]}]}]}]}",
        };
        return Encoding.UTF8.GetBytes(json);
    }

    private Task<HttpResponseMessage> PostAsync(string route, byte[] body, bool protobuf, bool gzip)
    {
        var content = new ByteArrayContent(body);
        content.Headers.ContentType = new MediaTypeHeaderValue(protobuf ? "application/x-protobuf" : "application/json");
        if (gzip) content.Headers.ContentEncoding.Add("gzip");
        return _client.PostAsync(route, content);
    }

    /// <summary>The <c>message</c> of an OTLP Status body, in either encoding.</summary>
    private static string StatusMessage(byte[] body, bool protobuf)
    {
        if (!protobuf) return JsonDocument.Parse(body).RootElement.GetProperty("message").GetString()!;
        Assert.Equal(0x12, body[0]);
        Assert.Equal(body.Length - 2, (int)body[1]);
        return Encoding.UTF8.GetString(body, 2, body.Length - 2);
    }

    private static byte[] Frame(byte[] payload, bool compressed)
    {
        var framed = new byte[OtlpGrpcFraming.HeaderBytes + payload.Length];
        framed[0] = compressed ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteUInt32BigEndian(framed.AsSpan(1), (uint)payload.Length);
        payload.CopyTo(framed.AsSpan(OtlpGrpcFraming.HeaderBytes));
        return framed;
    }

    private DefaultHttpContext GrpcCall(byte[] framed)
    {
        var ctx = new DefaultHttpContext { RequestServices = _factory.Services };
        ctx.Request.Method        = "POST";
        ctx.Request.Protocol      = "HTTP/2";
        ctx.Request.ContentType   = "application/grpc";
        ctx.Request.ContentLength = framed.Length;
        ctx.Request.Body          = new MemoryStream(framed);
        ctx.Request.Headers["X-Seq-ApiKey"]  = AmetoWebAppFactory.TestApiKey;
        ctx.Request.Headers["grpc-encoding"] = "gzip";
        ctx.Response.Body = new MemoryStream();
        return ctx;
    }
}
