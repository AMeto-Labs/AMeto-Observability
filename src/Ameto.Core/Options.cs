namespace Ameto.Core;

/// <summary>
/// Retention rules per log level.
/// </summary>
public sealed class RetentionPolicy
{
    private readonly Dictionary<LogLevel, TimeSpan> _rules;

    public static RetentionPolicy Default { get; } = new(new Dictionary<LogLevel, TimeSpan>
    {
        [LogLevel.Verbose]     = TimeSpan.FromDays(90),
        [LogLevel.Debug]       = TimeSpan.FromDays(3),
        [LogLevel.Information] = TimeSpan.FromDays(90),
        [LogLevel.Warning]     = TimeSpan.FromDays(90),
        [LogLevel.Error]       = TimeSpan.FromDays(90),
        [LogLevel.Fatal]       = TimeSpan.FromDays(90),
    });

    public RetentionPolicy(Dictionary<LogLevel, TimeSpan> rules)
    {
        _rules = rules;
    }

    public TimeSpan GetTtl(LogLevel level) =>
        _rules.TryGetValue(level, out var ttl) ? ttl : TimeSpan.FromDays(90);
}

/// <summary>
/// Hot-tier flush configuration.
/// </summary>
public sealed class HotTierOptions
{
    /// <summary>
    /// Maximum size of the hot-tier in bytes before a flush is triggered. Smaller tiers
    /// mean smaller frozen tiers held in RAM while their cold segment is being written,
    /// so the parallel-flush backlog (see StorageEngine) can be deeper for the same memory
    /// ceiling — smoother back-pressure with fewer drops under bursty ingest.
    /// </summary>
    public long MaxSizeBytes { get; init; } = 64 * 1024 * 1024; // 64 MB

    /// <summary>Maximum age of events in the hot-tier before a flush is triggered.</summary>
    public TimeSpan MaxAge   { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Number of cold-segment flushes (index build + compress + write) allowed to run in
    /// parallel. Higher = more flush throughput (fewer ingest drops under burst) but more
    /// peak RAM (concurrent index builds). 0 = auto (≈ processor count / 2, capped 2–8).
    /// Tune down on memory-constrained hosts, up on many-core hosts chasing throughput.
    /// </summary>
    public int FlushConcurrency { get; init; } = 0;
}

/// <summary>
/// Indexing configuration (segment flush-time index building).
/// </summary>
public sealed class IndexingOptions
{
    /// <summary>
    /// Maximum depth when recursively flattening nested structured properties.
    /// Prevents index explosion for deeply nested objects.
    /// Default: 5. Set to 0 to disable nested flattening (only top-level keys indexed).
    /// </summary>
    public int MaxPropertyFlattenDepth { get; init; } = 5;
}

/// <summary>
/// Ingestion request/size limits. All values are byte counts.
/// </summary>
public sealed class IngestionOptions
{
    /// <summary>
    /// Max HTTP body for <c>POST /api/events</c> (CLEF msgpack batch). A request whose
    /// body exceeds this is rejected with 413 before parsing. Default: 4 MB.
    /// </summary>
    public int MaxBatchBytes { get; init; } = 4 * 1024 * 1024;

    /// <summary>
    /// Max msgpack properties bytes for a single event — also the ring-buffer slab size.
    /// An event whose serialised properties exceed this is dropped (logged with its size),
    /// while the rest of the batch still ingests. Default: 64 KB.
    /// </summary>
    public int MaxEventPayloadBytes { get; init; } = 64 * 1024;

    /// <summary>
    /// Max HTTP body for the OTLP ingest endpoints (<c>/otlp/v1/*</c>). Larger → 413.
    /// Default: 8 MB.
    /// </summary>
    public int MaxOtlpBatchBytes { get; init; } = 8 * 1024 * 1024;
}

/// <summary>
/// Initial per-level retention defaults (days). Used only on first run;
/// after that the values live in SQLite and these are ignored.
/// </summary>
public sealed class RetentionConfig
{
    public int VerboseDays     { get; init; } = 90;
    public int DebugDays       { get; init; } = 3;
    public int InformationDays { get; init; } = 90;
    public int WarningDays     { get; init; } = 90;
    public int ErrorDays       { get; init; } = 90;
    public int FatalDays       { get; init; } = 90;
    public int MetricsDays     { get; init; } = 30;
    public int TracesDays      { get; init; } = 14;
}

/// <summary>
/// Top-level server configuration.
/// </summary>
public sealed class ServerOptions
{
    public NodeId           NodeId           { get; init; } = NodeId.Local;
    public string           DataDirectory    { get; init; } = "data";
    public HotTierOptions   HotTier          { get; init; } = new();
    public IndexingOptions  Indexing         { get; init; } = new();
    public IngestionOptions Ingestion        { get; init; } = new();
    public RetentionConfig  Retention        { get; init; } = new();
    public int              HttpPort         { get; init; } = 5341;
    public string           SslCertPath      { get; init; } = "";
    public string           SslCertPassword  { get; init; } = "";

    /// <summary>
    /// System-wide RAM utilisation target (0–100 %).
    /// When the OS memory load exceeds this threshold the storage engine will
    /// flush the hot tier to disk, releasing the in-memory write buffer.
    /// Default: 85.
    /// </summary>
    public int              RamTargetPercent { get; init; } = 85;
}
