using Serilog;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;
using Serilog.Sinks.PeriodicBatching;

namespace Serilog.Sinks.Ameto;

/// <summary>
/// Tuning knobs for the Ameto sink, configured through an <c>Action</c> delegate:
/// <code>
/// .WriteTo.Ameto("http://ameto:5341", o => {
///     o.ServiceName    = "orders-api";
///     o.ApiKey         = "…";
///     o.BatchSizeLimit = 2000;
/// })
/// </code>
/// </summary>
public sealed class AmetoSinkOptions
{
    // Note: apiKey and serviceName are required and passed as explicit arguments to
    // WriteTo.Ameto(...), not set here — so they can't be accidentally omitted.

    /// <summary>Max events per HTTP POST batch.</summary>
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
}

/// <summary>
/// Ready-made <see cref="AmetoSinkOptions"/> profiles. Pass one directly, or compose then
/// override: <c>o => { AmetoSinkProfiles.HighThroughput(o); o.ServiceName = "api"; }</c>.
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

    /// <summary>Large in-memory buffer to ride out transient server outages without dropping
    /// events (PeriodicBatching retries failed batches with back-off); trades RAM for durability.</summary>
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
    /// <param name="configure">Optional tuning (batch size, period, queue, level, HttpClient).</param>
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

        var batched = new AmetoBatchedSink(serverUrl, apiKey, serviceName, opts.HttpClient);

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
