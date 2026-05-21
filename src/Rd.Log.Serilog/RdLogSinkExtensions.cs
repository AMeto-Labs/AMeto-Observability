using Serilog;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;
using Serilog.Sinks.PeriodicBatching;

namespace Rd.Log.Serilog;

/// <summary>
/// <see cref="LoggerSinkConfiguration"/> extensions for the Rd.Log sink.
/// </summary>
public static class RdLogSinkExtensions
{
    /// <summary>
    /// Sends Serilog events to an Rd.Log server using its native MessagePack CLEF
    /// ingestion endpoint (<c>POST /api/events</c>).
    /// </summary>
    /// <param name="sinkConfiguration">The host configuration object.</param>
    /// <param name="serverUrl">Base URL of the Rd.Log server (e.g. <c>http://localhost:5341</c>).</param>
    /// <param name="apiKey">Optional API key sent in the <c>X-Seq-ApiKey</c> header.</param>
    /// <param name="batchSizeLimit">Maximum number of events sent in a single HTTP request.</param>
    /// <param name="period">Time to wait between batches.</param>
    /// <param name="queueLimit">In-memory queue limit before events are dropped.</param>
    /// <param name="restrictedToMinimumLevel">Events below this level are ignored.</param>
    /// <param name="levelSwitch">Optional level switch for runtime level changes.</param>
    /// <param name="controlLevelSwitch">
    /// Optional level switch updated from server responses (kept for Seq-compatibility;
    /// the current Rd.Log server does not push level updates yet).
    /// </param>
    /// <param name="httpClient">Optional pre-configured <see cref="HttpClient"/>. When null, the sink owns its instance.</param>
    public static LoggerConfiguration RdLog(
        this LoggerSinkConfiguration sinkConfiguration,
        string             serverUrl,
        string?            apiKey                   = null,
        int                batchSizeLimit           = 1000,
        TimeSpan?          period                   = null,
        int                queueLimit               = 100_000,
        LogEventLevel      restrictedToMinimumLevel = LevelAlias.Minimum,
        LoggingLevelSwitch? levelSwitch             = null,
        LoggingLevelSwitch? controlLevelSwitch      = null,
        HttpClient?        httpClient               = null)
    {
        ArgumentNullException.ThrowIfNull(sinkConfiguration);
        if (string.IsNullOrWhiteSpace(serverUrl))
            throw new ArgumentException("serverUrl must be a non-empty URL.", nameof(serverUrl));

        var batched = new RdLogBatchedSink(serverUrl, apiKey, httpClient);

        var options = new PeriodicBatchingSinkOptions
        {
            BatchSizeLimit   = batchSizeLimit,
            Period           = period ?? TimeSpan.FromSeconds(1),
            EagerlyEmitFirstEvent = true,
            QueueLimit       = queueLimit,
        };

        var sink = new PeriodicBatchingSink(batched, options);

        return sinkConfiguration.Sink(sink, restrictedToMinimumLevel, levelSwitch);
    }
}
