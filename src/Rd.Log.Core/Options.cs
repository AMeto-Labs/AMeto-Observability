namespace Rd.Log.Core;

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
    /// <summary>Maximum size of the hot-tier in bytes before a flush is triggered.</summary>
    public long MaxSizeBytes { get; init; } = 256 * 1024 * 1024; // 256 MB

    /// <summary>Maximum age of events in the hot-tier before a flush is triggered.</summary>
    public TimeSpan MaxAge   { get; init; } = TimeSpan.FromMinutes(5);
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
