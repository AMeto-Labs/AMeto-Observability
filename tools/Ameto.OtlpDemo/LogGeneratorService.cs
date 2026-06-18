using System.Diagnostics.Metrics;

/// <summary>
/// Background service that emits batches of random structured log entries
/// every 2 seconds. For every error entry generated, there is a 30 % chance
/// it is forwarded to Seq as a signal via <see cref="SeqSignalSender"/>.
/// </summary>
internal sealed class LogGeneratorService(
    ILogger<LogGeneratorService> logger,
    LogStore                     store,
    SeqSignalSender              sender) : BackgroundService
{
    /// <summary>Name used to register this meter with <c>AddMeter()</c> in the OpenTelemetry pipeline.</summary>
    internal const string MeterName = "Ameto.OtlpDemo";

    private static readonly Meter    s_meter    = new(MeterName);
    private static readonly Counter<long> s_generated = s_meter.CreateCounter<long>("demo.logs.generated",  "count", "Total log entries generated.");
    private static readonly Counter<long> s_errors    = s_meter.CreateCounter<long>("demo.logs.errors",     "count", "Total error log entries generated.");
    private static readonly Histogram<double> s_duration = s_meter.CreateHistogram<double>("demo.operation.duration", "ms", "Simulated operation duration.");

    private static readonly string[] s_ops =
    [
        "order.placed",      "order.shipped",       "order.cancelled",
        "user.login",        "user.logout",         "user.registered",
        "payment.processed", "payment.failed",      "payment.refunded",
        "inventory.updated", "report.generated",    "session.expired",
        "cache.miss",        "notification.sent",   "webhook.delivered",
        "auth.token.refresh","search.executed",     "file.uploaded",
    ];

    private static readonly string[] s_users =
    [
        "alice@example.com", "bob@example.com", "carol@example.com",
        "dave@example.com",  "eve@example.com",  "frank@example.com",
    ];

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Let the host finish startup before we begin
        await Task.Delay(300, stoppingToken).ConfigureAwait(false);

        while (!stoppingToken.IsCancellationRequested)
        {
            LogEntry? errorEntry = GenerateBatch(batchSize: 20);

            // Randomly (~30 % when an error occurred) send one entry to Seq
            if (errorEntry is not null && Random.Shared.Next(10) < 3)
                await sender.SendAsync(errorEntry).ConfigureAwait(false);

            await Task.Delay(2_000, stoppingToken).ConfigureAwait(false);
        }
    }

    /// <returns>The first error entry in the batch, or <c>null</c> if none occurred.</returns>
    private LogEntry? GenerateBatch(int batchSize)
    {
        LogEntry? firstError = null;

        for (int i = 0; i < batchSize; i++)
        {
            string op   = s_ops  [Random.Shared.Next(s_ops.Length)];
            string user = s_users[Random.Shared.Next(s_users.Length)];
            bool   fail = Random.Shared.Next(5) == 0;   // 20 % error rate
            int    code = fail ? 500 : 200;
            double ms   = Random.Shared.NextDouble() * 480.0 + 20.0;

            var entry = new LogEntry(
                Timestamp : DateTimeOffset.UtcNow,
                Level     : fail ? "Error" : "Information",
                Operation : op,
                User      : user,
                StatusCode: code,
                DurationMs: Math.Round(ms, 2),
                TraceId   : Guid.NewGuid().ToString("N"),
                Message   : fail
                    ? $"[{op}] failed for {user} — HTTP {code} ({ms:F1}ms)"
                    : $"[{op}] OK for {user} ({ms:F1}ms)");

            store.Add(entry);

            // Emit metrics for every generated entry
            s_generated.Add(1, new KeyValuePair<string, object?>("operation", op));
            s_duration.Record(ms, new KeyValuePair<string, object?>("operation", op));

            if (fail)
            {
                s_errors.Add(1, new KeyValuePair<string, object?>("operation", op));
                logger.LogError(
                    "Operation {Operation} failed for {User} status={StatusCode} duration={DurationMs:F1}ms traceId={TraceId}",
                    op, user, code, ms, entry.TraceId);

                firstError ??= entry;
            }
            else
            {
                logger.LogInformation(
                    "Operation {Operation} succeeded for {User} duration={DurationMs:F1}ms",
                    op, user, ms);
            }
        }

        return firstError;
    }
}
