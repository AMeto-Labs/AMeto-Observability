namespace Rd.Log.LoadTest;

/// <summary>
/// Parameters for a single load-test run.
/// Every field has a sensible default; override via JSON body or query string.
/// </summary>
public sealed class LoadTestConfig
{
    // ── Target ────────────────────────────────────────────────────────────────

    /// <summary>Base URL of the Rd.Log server to target.</summary>
    public string TargetUrl { get; set; } = "http://localhost:5341";

    /// <summary>Optional API key (sent as X-Api-Key header).</summary>
    public string? ApiKey { get; set; }

    // ── Volume ────────────────────────────────────────────────────────────────

    /// <summary>Events per batch (POST /api/events payload size).</summary>
    public int BatchSize { get; set; } = 10;

    /// <summary>
    /// Milliseconds to wait between consecutive batches per worker.
    /// Set to 0 for maximum throughput (no sleep).
    /// </summary>
    public int IntervalMs { get; set; } = 100;

    /// <summary>Number of concurrent HTTP workers sending batches in parallel.</summary>
    public int Concurrency { get; set; } = 1;

    /// <summary>Total duration of the test run in seconds.</summary>
    public int DurationSeconds { get; set; } = 10;

    // ── Log content ───────────────────────────────────────────────────────────

    /// <summary>
    /// Mix of log levels to emit (percentage weights).
    /// Must sum to 100; defaults to 60 % Info / 20 % Warn / 15 % Error / 5 % Debug.
    /// </summary>
    public LevelWeights Levels { get; set; } = new();

    /// <summary>Fixed message templates to cycle through (random pick per event).</summary>
    public string[] Templates { get; set; } =
    [
        "User {UserId} performed action {Action}",
        "HTTP {Method} {Path} responded {StatusCode} in {DurationMs}ms",
        "Database query {QueryName} completed in {DurationMs}ms",
        "Cache {Operation} for key {Key}",
        "Service {ServiceName} health check {Result}",
        "Payment {PaymentId} transitioned to {State}",
        "File {FileName} uploaded by {UserId}",
        "Config reloaded: {Key} = {Value}",
        "Retry attempt {Attempt} for {Operation}",
        "Background job {JobName} finished in {DurationMs}ms",
    ];
}

public sealed class LevelWeights
{
    public int Verbose     { get; set; } = 0;
    public int Debug       { get; set; } = 5;
    public int Information { get; set; } = 60;
    public int Warning     { get; set; } = 20;
    public int Error       { get; set; } = 15;
    public int Fatal       { get; set; } = 0;
}
