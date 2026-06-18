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
        if (loaded is null)
        {
            _current = new RetentionDto
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
            SaveToDb(_current);
        }
        else
        {
            _current = loaded;
        }
    }

    /// <summary>Returns the current retention settings DTO.</summary>
    public RetentionDto Get() => _current;

    /// <summary>Returns the current settings as a <see cref="RetentionPolicy"/>.</summary>
    public RetentionPolicy GetPolicy() => _current.ToPolicy();

    /// <summary>Updates settings and persists to the database.</summary>
    public void Set(RetentionDto dto)
    {
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

    public RetentionPolicy ToPolicy() => new(new Dictionary<CoreLogLevel, TimeSpan>
    {
        [CoreLogLevel.Verbose]     = TimeSpan.FromDays(Math.Max(1, VerboseDays)),
        [CoreLogLevel.Debug]       = TimeSpan.FromDays(Math.Max(1, DebugDays)),
        [CoreLogLevel.Information] = TimeSpan.FromDays(Math.Max(1, InformationDays)),
        [CoreLogLevel.Warning]     = TimeSpan.FromDays(Math.Max(1, WarningDays)),
        [CoreLogLevel.Error]       = TimeSpan.FromDays(Math.Max(1, ErrorDays)),
        [CoreLogLevel.Fatal]       = TimeSpan.FromDays(Math.Max(1, FatalDays)),
    });
}
