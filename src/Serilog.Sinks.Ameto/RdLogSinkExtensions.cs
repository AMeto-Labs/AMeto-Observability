using Serilog;
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Events;
using Serilog.Sinks.PeriodicBatching;

namespace Serilog.Sinks.Ameto;

/// <summary>
/// <see cref="LoggerSinkConfiguration"/> extensions for the Ameto sink.
/// </summary>
public static class AmetoSinkExtensions
{
    /// <summary>
    /// Sends Serilog events to an Ameto server using its native MessagePack CLEF
    /// ingestion endpoint (<c>POST /api/events</c>).
    /// </summary>
    public static LoggerConfiguration Ameto(
        this LoggerSinkConfiguration sinkConfiguration,
        string              serverUrl,
        string?             apiKey                   = null,
        string?             serviceName              = null,
        int                 batchSizeLimit           = 1000,
        TimeSpan?           period                   = null,
        int                 queueLimit               = 100_000,
        LogEventLevel       restrictedToMinimumLevel = LevelAlias.Minimum,
        LoggingLevelSwitch? levelSwitch              = null,
        LoggingLevelSwitch? controlLevelSwitch       = null,
        HttpClient?         httpClient               = null)
    {
        ArgumentNullException.ThrowIfNull(sinkConfiguration);
        if (string.IsNullOrWhiteSpace(serverUrl))
            throw new ArgumentException("serverUrl must be a non-empty URL.", nameof(serverUrl));

        var batched = new AmetoBatchedSink(serverUrl, apiKey, serviceName, httpClient);

        var options = new PeriodicBatchingSinkOptions
        {
            BatchSizeLimit        = batchSizeLimit,
            Period                = period ?? TimeSpan.FromSeconds(1),
            EagerlyEmitFirstEvent = true,
            QueueLimit            = queueLimit,
        };

        var sink = new PeriodicBatchingSink(batched, options);
    

        return sinkConfiguration  
      .Sink(sink, restrictedToMinimumLevel, levelSwitch);
    }
}
