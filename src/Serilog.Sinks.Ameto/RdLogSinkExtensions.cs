using Serilog;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;
using Serilog.Sinks.PeriodicBatching;

namespace Serilog.Sinks.Ameto;

/// <summary>
/// Tuning knobs for the Ameto sink, configured through an <c>Action</c> delegate:
/// <code>
/// .WriteTo.Ameto("http://ameto:5341", "api-key", "orders-api", o => {
///     o.BatchSizeLimit = 2000;
/// })
/// </code>
/// </summary>
public sealed class AmetoSinkOptions
{
    // Note: apiKey and serviceName are required and passed as explicit arguments to
    // WriteTo.Ameto(...), not set here — so they can't be accidentally omitted.

    /// <summary>Max events per batch. A batch larger in bytes than <see cref="BatchBytesLimit"/> still goes as several requests.</summary>
    public int                 BatchSizeLimit           { get; set; } = 1000;

    /// <summary>How often batches are flushed (also flushes early when the batch fills).</summary>
    public TimeSpan            Period                   { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Max events buffered in memory; excess is dropped (back-pressure) instead of growing RAM.</summary>
    public int                 QueueLimit               { get; set; } = 100_000;

    /// <summary>Emit the very first event immediately instead of waiting a full <see cref="Period"/>.</summary>
    public bool                EagerlyEmitFirstEvent    { get; set; } = true;

    /// <summary>Static minimum level for this sink.</summary>
    public LogEventLevel       RestrictedToMinimumLevel { get; set; } = LevelAlias.Minimum;

    /// <summary>Runtime-adjustable level override (takes precedence over <see cref="RestrictedToMinimumLevel"/>).</summary>
    public LoggingLevelSwitch? LevelSwitch              { get; set; }

    /// <summary>Bring your own <see cref="HttpClient"/> (shared pool/proxy); null = sink owns one (30 s timeout).</summary>
    public HttpClient?         HttpClient               { get; set; }

    /// <summary>
    /// Max serialised bytes of one HTTP request. A batch that serialises past it is sent as several
    /// requests, each under the limit (an event larger than the limit on its own still goes, alone).
    /// Keep it under the server's <c>Ingestion.MaxBatchBytes</c> (4 MB by default), which answers 413
    /// to a larger body; the sink then halves the request and tries again. 0 = one request per batch.
    /// </summary>
    public int                 BatchBytesLimit          { get; set; } = 3 * 1024 * 1024;

    /// <summary>
    /// Max serialised size of one event's PROPERTIES, in bytes — the map header and its pairs, what
    /// the server measures against its <c>Ingestion.MaxEventPayloadBytes</c> (64 KB by default). An
    /// event over the limit is replaced by a compact Error-level marker (original template, level,
    /// size and trace context preserved) — so a runaway <c>{@Object}</c> is VISIBLE in Ameto instead
    /// of being rejected by the server. The exception has its own limit, <see cref="ExceptionLimitBytes"/>.
    /// 0 = unlimited.
    /// </summary>
    public int                 PropertiesLimitBytes     { get; set; } = 65_536;

    /// <summary>
    /// About the most one event's exception (<c>@x</c>) takes serialised, in bytes. Over it the sink
    /// keeps the exception type, cuts the message and the stack trace and drops the inner exceptions —
    /// the event itself still arrives. A target rather than a hard cap: the cut is by characters, and
    /// a stack trace full of non-ASCII names can land over it. The server takes the exception apart
    /// from the properties and has no comparable cap of its own. 0 = unlimited.
    /// </summary>
    public int                 ExceptionLimitBytes      { get; set; } = 256 * 1024;

    /// <summary>
    /// The 1.0 name of <see cref="PropertiesLimitBytes"/>. Up to 1.0.x it measured the whole serialised
    /// event, exception included; it now sets <see cref="PropertiesLimitBytes"/>, which measures the
    /// properties map only. The exception has <see cref="ExceptionLimitBytes"/>, which a 1.0 setting
    /// meant for whole events — or a 0 meant as "no limit" — should set too.
    /// </summary>
    [Obsolete("Renamed PropertiesLimitBytes, which measures the properties map only (1.0 measured the whole event). The exception has ExceptionLimitBytes.",
        DiagnosticId = "AMETOSINK001")]
    public int                 EventBodyLimitBytes
    {
        get => PropertiesLimitBytes;
        set => PropertiesLimitBytes = value;
    }
}

/// <summary>
/// Ready-made <see cref="AmetoSinkOptions"/> profiles. Pass one directly, or compose then
/// override: <c>o => { AmetoSinkProfiles.HighThroughput(o); o.BatchSizeLimit = 2000; }</c>.
/// </summary>
public static class AmetoSinkProfiles
{
    /// <summary>Balanced production defaults (identical to the option defaults).</summary>
    public static readonly Action<AmetoSinkOptions> Balanced = static _ => { };

    /// <summary>Large batches + big queue, relaxed latency — maximum throughput, higher RAM.</summary>
    public static readonly Action<AmetoSinkOptions> HighThroughput = static o =>
    {
        o.BatchSizeLimit = 5_000;
        o.Period         = TimeSpan.FromSeconds(2);
        o.QueueLimit     = 500_000;
    };

    /// <summary>Tiny batches flushed fast — lowest latency (dev/interactive), more requests.</summary>
    public static readonly Action<AmetoSinkOptions> LowLatency = static o =>
    {
        o.BatchSizeLimit        = 50;
        o.Period                = TimeSpan.FromMilliseconds(250);
        o.QueueLimit            = 20_000;
        o.EagerlyEmitFirstEvent = true;
    };

    /// <summary>Small queue + moderate batch — bounds memory on constrained hosts (drops sooner under load).</summary>
    public static readonly Action<AmetoSinkOptions> MemoryConstrained = static o =>
    {
        o.BatchSizeLimit = 500;
        o.Period         = TimeSpan.FromSeconds(1);
        o.QueueLimit     = 10_000;
    };

    /// <summary>
    /// Large in-memory buffer to ride out short server outages without dropping events; trades RAM
    /// for durability. A batch the server could not take is retried with back-off (one period, then
    /// 10 s doubling to 1 min) for the batching layer's retry time limit of 10 minutes, counted from
    /// the first failure since the last success: in a longer outage every later batch gets one
    /// attempt and is dropped, and after ten dropped batches in a row the queue is emptied.
    /// </summary>
    public static readonly Action<AmetoSinkOptions> Resilient = static o =>
    {
        o.BatchSizeLimit = 1_000;
        o.Period         = TimeSpan.FromSeconds(1);
        o.QueueLimit     = 250_000;
    };
}

/// <summary>
/// <see cref="LoggerSinkConfiguration"/> extensions for the Ameto sink.
/// </summary>
public static class AmetoSinkExtensions
{
    /// <summary>
    /// Sends Serilog events to an Ameto server (<c>POST /api/events</c>).
    /// <paramref name="apiKey"/> and <paramref name="serviceName"/> are required; all other
    /// tuning is optional via the <paramref name="configure"/> delegate (compose with
    /// <see cref="AmetoSinkProfiles"/> for ready-made presets).
    /// </summary>
    /// <param name="serverUrl">Base URL of the Ameto server, e.g. <c>http://ameto:5341</c>.</param>
    /// <param name="apiKey">API key sent as the <c>X-Seq-ApiKey</c> header. Required.</param>
    /// <param name="serviceName">Service identifier written as <c>service.name</c> on every event. Required.</param>
    /// <param name="configure">Optional tuning (batch size, period, queue, limits, level, HttpClient).</param>
    public static LoggerConfiguration Ameto(
        this LoggerSinkConfiguration sinkConfiguration,
        string                       serverUrl,
        string                       apiKey,
        string                       serviceName,
        Action<AmetoSinkOptions>?    configure = null)
    {
        ArgumentNullException.ThrowIfNull(sinkConfiguration);
        if (string.IsNullOrWhiteSpace(serverUrl))
            throw new ArgumentException("serverUrl must be a non-empty URL.", nameof(serverUrl));
        if (string.IsNullOrWhiteSpace(apiKey))
            throw new ArgumentException("apiKey is required.", nameof(apiKey));
        if (string.IsNullOrWhiteSpace(serviceName))
            throw new ArgumentException("serviceName is required.", nameof(serviceName));

        var opts = new AmetoSinkOptions();
        configure?.Invoke(opts);

        var batched = new AmetoBatchedSink(serverUrl, apiKey, serviceName, opts.HttpClient,
            opts.PropertiesLimitBytes, opts.ExceptionLimitBytes, opts.BatchBytesLimit);

        var batchOptions = new PeriodicBatchingSinkOptions
        {
            BatchSizeLimit        = opts.BatchSizeLimit,
            Period                = opts.Period,
            EagerlyEmitFirstEvent = opts.EagerlyEmitFirstEvent,
            QueueLimit            = opts.QueueLimit,
        };
        var sink = new PeriodicBatchingSink(batched, batchOptions);

        return sinkConfiguration.Sink(sink, opts.RestrictedToMinimumLevel, opts.LevelSwitch);
    }
}
