using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Ameto.Core;
using CoreLogLevel = Ameto.Core.LogLevel;

namespace Ameto.Storage;

/// <summary>
/// Persists the active <see cref="RetentionPolicy"/> in the shared
/// <c>Ameto.db</c> SQLite database (table <c>retention</c>).
/// Falls back to <see cref="ServerOptions.Retention"/> defaults on first run.
/// Thread-safe — all writes serialize through a lock.
/// </summary>
public sealed class RetentionStore
{
    private readonly string                  _connStr;
    private readonly ILogger<RetentionStore> _logger;
    private readonly object                  _lock = new();
    private volatile RetentionDto            _current;

    public RetentionStore(ServerOptions options, ILogger<RetentionStore> logger)
    {
        _logger  = logger;
        _connStr = $"Data Source={Path.Combine(options.DataDirectory, "Ameto.db")}";
        InitTable();
        var loaded = LoadFromDb();
        var source = loaded is null ? "configuration" : "database";
        var dto    = loaded ?? new RetentionDto
        {
            VerboseDays     = options.Retention.VerboseDays,
            DebugDays       = options.Retention.DebugDays,
            InformationDays = options.Retention.InformationDays,
            WarningDays     = options.Retention.WarningDays,
            ErrorDays       = options.Retention.ErrorDays,
            FatalDays       = options.Retention.FatalDays,
            MetricsDays     = options.Retention.MetricsDays,
            TracesDays      = options.Retention.TracesDays,
        };

        // A window outside the bounds — "forever" typed as 200 000 days, which wrapped the trace
        // cutoff into the future and deleted every trace file every hour (#130) — is clamped here,
        // before any prune sees it, and written back so the next start reads a sound policy.
        // Set() refuses such a window outright; this is for what a database or a config file held
        // before the bounds existed.
        var clamped = dto.Clamped(out bool changed);
        if (changed)
            _logger.LogWarning(
                "Retention windows from the {Source} were outside {Min}…{Max} days and were clamped: {Before} → {After}",
                source, RetentionMath.MinDays, RetentionMath.MaxDays, dto, clamped);
        _current = clamped;
        if (loaded is null || changed) SaveToDb(_current);
    }

    /// <summary>Returns the current retention settings DTO.</summary>
    public RetentionDto Get() => _current;

    /// <summary>Returns the current settings as a <see cref="RetentionPolicy"/>.</summary>
    public RetentionPolicy GetPolicy() => _current.ToPolicy();

    /// <summary>
    /// Updates settings and persists to the database. A window outside
    /// [<see cref="RetentionMath.MinDays"/>, <see cref="RetentionMath.MaxDays"/>] is refused whole, with an
    /// <see cref="ArgumentOutOfRangeException"/> whose <c>ParamName</c> is the field's wire name (#130).
    /// </summary>
    public void Set(RetentionDto dto)
    {
        var field = dto.FirstInvalidField();
        if (field is not null)
            throw new ArgumentOutOfRangeException(field,
                $"{field} must be between {RetentionMath.MinDays} and {RetentionMath.MaxDays} days");

        lock (_lock)
        {
            _current = dto;
            try { SaveToDb(dto); }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to persist retention settings");
            }
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private void InitTable()
    {
        using var conn = OpenConn();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS retention (
                level TEXT PRIMARY KEY,
                days  INTEGER NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();
    }

    private RetentionDto? LoadFromDb()
    {
        using var conn = OpenConn();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "SELECT level, days FROM retention";
        using var r    = cmd.ExecuteReader();

        var dict = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        while (r.Read())
            dict[r.GetString(0)] = r.GetInt32(1);

        if (dict.Count == 0) return null;

        return new RetentionDto
        {
            VerboseDays     = dict.GetValueOrDefault("verbose",     90),
            DebugDays       = dict.GetValueOrDefault("debug",        3),
            InformationDays = dict.GetValueOrDefault("information", 90),
            WarningDays     = dict.GetValueOrDefault("warning",     90),
            ErrorDays       = dict.GetValueOrDefault("error",       90),
            FatalDays       = dict.GetValueOrDefault("fatal",       90),
            MetricsDays     = dict.GetValueOrDefault("metrics",     30),
            TracesDays      = dict.GetValueOrDefault("traces",      14),
        };
    }

    private void SaveToDb(RetentionDto dto)
    {
        var levels = new[]
        {
            ("verbose",     dto.VerboseDays),
            ("debug",       dto.DebugDays),
            ("information", dto.InformationDays),
            ("warning",     dto.WarningDays),
            ("error",       dto.ErrorDays),
            ("fatal",       dto.FatalDays),
            ("metrics",     dto.MetricsDays),
            ("traces",      dto.TracesDays),
        };

        using var conn = OpenConn();
        using var tx   = conn.BeginTransaction();
        foreach (var (level, days) in levels)
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = """
                INSERT INTO retention (level, days) VALUES (@l, @d)
                ON CONFLICT(level) DO UPDATE SET days = excluded.days
                """;
            cmd.Parameters.AddWithValue("@l", level);
            cmd.Parameters.AddWithValue("@d", days);
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
    }

    private SqliteConnection OpenConn()
    {
        var conn = new SqliteConnection(_connStr);
        conn.Open();
        using var wal = conn.CreateCommand();
        wal.CommandText = "PRAGMA journal_mode=WAL;";
        wal.ExecuteNonQuery();
        return conn;
    }
}

/// <summary>Result returned by a manual or background retention enforcement run.</summary>
public sealed record RetentionRunResult(
    int              DeletedSegments,
    long             FreedBytes,
    int              DeletedMetricFiles,
    int              DeletedTraceFiles,
    DateTimeOffset   RanAt);

/// <summary>
/// Flat wire DTO for the retention policy — one integer per log level.
/// </summary>
public sealed class RetentionDto
{
    public int VerboseDays     { get; set; } = 90;
    public int DebugDays       { get; set; } = 3;
    public int InformationDays { get; set; } = 90;
    public int WarningDays     { get; set; } = 90;
    public int ErrorDays       { get; set; } = 90;
    public int FatalDays       { get; set; } = 90;
    public int MetricsDays     { get; set; } = 30;
    public int TracesDays      { get; set; } = 14;

    public static RetentionDto FromPolicy(RetentionPolicy policy) => new()
    {
        VerboseDays     = (int)policy.GetTtl(CoreLogLevel.Verbose).TotalDays,
        DebugDays       = (int)policy.GetTtl(CoreLogLevel.Debug).TotalDays,
        InformationDays = (int)policy.GetTtl(CoreLogLevel.Information).TotalDays,
        WarningDays     = (int)policy.GetTtl(CoreLogLevel.Warning).TotalDays,
        ErrorDays       = (int)policy.GetTtl(CoreLogLevel.Error).TotalDays,
        FatalDays       = (int)policy.GetTtl(CoreLogLevel.Fatal).TotalDays,
    };

    /// <summary>The TTLs, each clamped into the bounds (<see cref="RetentionMath.TtlOf"/>): what the log engine expires by.</summary>
    public RetentionPolicy ToPolicy() => new(new Dictionary<CoreLogLevel, TimeSpan>
    {
        [CoreLogLevel.Verbose]     = RetentionMath.TtlOf(VerboseDays),
        [CoreLogLevel.Debug]       = RetentionMath.TtlOf(DebugDays),
        [CoreLogLevel.Information] = RetentionMath.TtlOf(InformationDays),
        [CoreLogLevel.Warning]     = RetentionMath.TtlOf(WarningDays),
        [CoreLogLevel.Error]       = RetentionMath.TtlOf(ErrorDays),
        [CoreLogLevel.Fatal]       = RetentionMath.TtlOf(FatalDays),
    });

    /// <summary>
    /// The wire name of the first window outside [<see cref="RetentionMath.MinDays"/>, <see cref="RetentionMath.MaxDays"/>],
    /// in wire order, or <c>null</c> when every window is in range.
    /// </summary>
    public string? FirstInvalidField()
    {
        if (!RetentionMath.IsValidDays(VerboseDays))     return "verboseDays";
        if (!RetentionMath.IsValidDays(DebugDays))       return "debugDays";
        if (!RetentionMath.IsValidDays(InformationDays)) return "informationDays";
        if (!RetentionMath.IsValidDays(WarningDays))     return "warningDays";
        if (!RetentionMath.IsValidDays(ErrorDays))       return "errorDays";
        if (!RetentionMath.IsValidDays(FatalDays))       return "fatalDays";
        if (!RetentionMath.IsValidDays(MetricsDays))     return "metricsDays";
        if (!RetentionMath.IsValidDays(TracesDays))      return "tracesDays";
        return null;
    }

    /// <summary>A copy with every window clamped into the bounds; <paramref name="changed"/> says whether any was.</summary>
    public RetentionDto Clamped(out bool changed)
    {
        var c = new RetentionDto
        {
            VerboseDays     = Clamp(VerboseDays),
            DebugDays       = Clamp(DebugDays),
            InformationDays = Clamp(InformationDays),
            WarningDays     = Clamp(WarningDays),
            ErrorDays       = Clamp(ErrorDays),
            FatalDays       = Clamp(FatalDays),
            MetricsDays     = Clamp(MetricsDays),
            TracesDays      = Clamp(TracesDays),
        };
        changed = c.VerboseDays != VerboseDays || c.DebugDays != DebugDays || c.InformationDays != InformationDays
               || c.WarningDays != WarningDays || c.ErrorDays != ErrorDays || c.FatalDays != FatalDays
               || c.MetricsDays != MetricsDays || c.TracesDays != TracesDays;
        return c;
    }

    private static int Clamp(int days) => Math.Clamp(days, RetentionMath.MinDays, RetentionMath.MaxDays);

    public override string ToString()
        => $"verbose {VerboseDays} d, debug {DebugDays} d, information {InformationDays} d, warning {WarningDays} d, "
         + $"error {ErrorDays} d, fatal {FatalDays} d, metrics {MetricsDays} d, traces {TracesDays} d";
}
