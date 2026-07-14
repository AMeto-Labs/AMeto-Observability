using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace Ameto.Alerts;

/// <summary>
/// Durable storage for alert history, silences, and per-rule runtime state in the
/// shared <c>Ameto.db</c> SQLite database. Lets firing history, maintenance silences,
/// and cooldown/state survive a server restart. Thread-safe (writes serialize via lock).
/// </summary>
public sealed class AlertPersistence
{
    private const int HistoryRetentionDays = 30;

    private readonly string                    _connStr;
    private readonly ILogger<AlertPersistence> _logger;
    private readonly object                    _lock = new();

    public AlertPersistence(string dataDirectory, ILogger<AlertPersistence> logger)
    {
        _logger  = logger;
        _connStr = $"Data Source={Path.Combine(dataDirectory, "Ameto.db")}";
        InitTables();
    }

    // ── History ─────────────────────────────────────────────────────────────────

    public void AppendHistory(AlertHistoryEntry e)
    {
        lock (_lock)
        try
        {
            using var conn = Open();
            using var cmd  = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO alert_history (rule_id, rule_name, severity, state, value, threshold, at_ticks)
                VALUES (@id, @name, @sev, @state, @val, @thr, @at)
                """;
            cmd.Parameters.AddWithValue("@id",   e.RuleId);
            cmd.Parameters.AddWithValue("@name", e.RuleName);
            cmd.Parameters.AddWithValue("@sev",  (int)e.Severity);
            cmd.Parameters.AddWithValue("@state",(int)e.State);
            cmd.Parameters.AddWithValue("@val",  e.Value);
            cmd.Parameters.AddWithValue("@thr",  e.Threshold);
            cmd.Parameters.AddWithValue("@at",   e.At.UtcTicks);
            cmd.ExecuteNonQuery();
        }
        catch (Exception ex) { _logger.LogError(ex, "Failed to persist alert history"); }
    }

    public IReadOnlyList<AlertHistoryEntry> LoadHistory(int limit)
    {
        var list = new List<AlertHistoryEntry>(Math.Min(limit, 512));
        try
        {
            using var conn = Open();
            using var cmd  = conn.CreateCommand();
            cmd.CommandText = "SELECT rule_id, rule_name, severity, state, value, threshold, at_ticks FROM alert_history ORDER BY at_ticks DESC LIMIT @n";
            cmd.Parameters.AddWithValue("@n", limit);
            using var r = cmd.ExecuteReader();
            while (r.Read())
                list.Add(new AlertHistoryEntry
                {
                    RuleId = r.GetString(0), RuleName = r.GetString(1),
                    Severity = (AlertSeverity)r.GetInt32(2), State = (AlertState)r.GetInt32(3),
                    Value = r.GetDouble(4), Threshold = r.GetDouble(5),
                    At = new DateTimeOffset(r.GetInt64(6), TimeSpan.Zero),
                });
        }
        catch (Exception ex) { _logger.LogError(ex, "Failed to load alert history"); }
        return list;
    }

    private void PruneHistory()
    {
        try
        {
            using var conn = Open();
            using var cmd  = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM alert_history WHERE at_ticks < @cutoff";
            cmd.Parameters.AddWithValue("@cutoff", DateTimeOffset.UtcNow.AddDays(-HistoryRetentionDays).UtcTicks);
            cmd.ExecuteNonQuery();
        }
        catch (Exception ex) { _logger.LogWarning(ex, "Alert history prune failed"); }
    }

    // ── Silences ──────────────────────────────────────────────────────────────

    public void UpsertSilence(AlertSilence s)
    {
        lock (_lock)
        try
        {
            using var conn = Open();
            using var cmd  = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO alert_silences (id, rule_id, reason, until_ticks, created_ticks)
                VALUES (@id, @rid, @reason, @until, @created)
                ON CONFLICT(id) DO UPDATE SET rule_id=excluded.rule_id, reason=excluded.reason,
                    until_ticks=excluded.until_ticks, created_ticks=excluded.created_ticks
                """;
            cmd.Parameters.AddWithValue("@id",      s.Id);
            cmd.Parameters.AddWithValue("@rid",     s.RuleId);
            cmd.Parameters.AddWithValue("@reason",  (object?)s.Reason ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@until",   s.Until.UtcTicks);
            cmd.Parameters.AddWithValue("@created", s.CreatedAt.UtcTicks);
            cmd.ExecuteNonQuery();
        }
        catch (Exception ex) { _logger.LogError(ex, "Failed to persist silence"); }
    }

    public void DeleteSilence(string id)
    {
        lock (_lock)
        try
        {
            using var conn = Open();
            using var cmd  = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM alert_silences WHERE id = @id";
            cmd.Parameters.AddWithValue("@id", id);
            cmd.ExecuteNonQuery();
        }
        catch (Exception ex) { _logger.LogError(ex, "Failed to delete silence"); }
    }

    public IReadOnlyList<AlertSilence> LoadSilences()
    {
        var list = new List<AlertSilence>();
        try
        {
            using var conn = Open();
            using var cmd  = conn.CreateCommand();
            cmd.CommandText = "SELECT id, rule_id, reason, until_ticks, created_ticks FROM alert_silences";
            using var r = cmd.ExecuteReader();
            while (r.Read())
                list.Add(new AlertSilence
                {
                    Id = r.GetString(0), RuleId = r.GetString(1),
                    Reason = r.IsDBNull(2) ? null : r.GetString(2),
                    Until = new DateTimeOffset(r.GetInt64(3), TimeSpan.Zero),
                    CreatedAt = new DateTimeOffset(r.GetInt64(4), TimeSpan.Zero),
                });
        }
        catch (Exception ex) { _logger.LogError(ex, "Failed to load silences"); }
        return list;
    }

    // ── Maintenance windows ─────────────────────────────────────────────────────

    public void UpsertMaintenance(MaintenanceWindow w)
    {
        lock (_lock)
        try
        {
            using var conn = Open();
            using var cmd  = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO maintenance_windows (id, name, enabled, days_of_week, start_minute, duration_minutes, max_severity)
                VALUES (@id, @name, @en, @dow, @start, @dur, @sev)
                ON CONFLICT(id) DO UPDATE SET name=excluded.name, enabled=excluded.enabled,
                    days_of_week=excluded.days_of_week, start_minute=excluded.start_minute,
                    duration_minutes=excluded.duration_minutes, max_severity=excluded.max_severity
                """;
            cmd.Parameters.AddWithValue("@id",    w.Id);
            cmd.Parameters.AddWithValue("@name",  w.Name);
            cmd.Parameters.AddWithValue("@en",    w.Enabled ? 1 : 0);
            cmd.Parameters.AddWithValue("@dow",   w.DaysOfWeek);
            cmd.Parameters.AddWithValue("@start", w.StartMinuteUtc);
            cmd.Parameters.AddWithValue("@dur",   w.DurationMinutes);
            cmd.Parameters.AddWithValue("@sev",   (object?)(w.MaxSeverity is { } s ? (int)s : (int?)null) ?? DBNull.Value);
            cmd.ExecuteNonQuery();
        }
        catch (Exception ex) { _logger.LogError(ex, "Failed to persist maintenance window"); }
    }

    public void DeleteMaintenance(string id)
    {
        lock (_lock)
        try
        {
            using var conn = Open();
            using var cmd  = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM maintenance_windows WHERE id = @id";
            cmd.Parameters.AddWithValue("@id", id);
            cmd.ExecuteNonQuery();
        }
        catch (Exception ex) { _logger.LogError(ex, "Failed to delete maintenance window"); }
    }

    public IReadOnlyList<MaintenanceWindow> LoadMaintenance()
    {
        var list = new List<MaintenanceWindow>();
        try
        {
            using var conn = Open();
            using var cmd  = conn.CreateCommand();
            cmd.CommandText = "SELECT id, name, enabled, days_of_week, start_minute, duration_minutes, max_severity FROM maintenance_windows";
            using var r = cmd.ExecuteReader();
            while (r.Read())
                list.Add(new MaintenanceWindow
                {
                    Id = r.GetString(0), Name = r.GetString(1), Enabled = r.GetInt32(2) != 0,
                    DaysOfWeek = r.GetInt32(3), StartMinuteUtc = r.GetInt32(4), DurationMinutes = r.GetInt32(5),
                    MaxSeverity = r.IsDBNull(6) ? null : (AlertSeverity)r.GetInt32(6),
                });
        }
        catch (Exception ex) { _logger.LogError(ex, "Failed to load maintenance windows"); }
        return list;
    }

    // ── Per-rule state (cooldown / state continuity across restart) ─────────────

    public void SaveState(string ruleId, AlertState state, double lastValue,
        DateTimeOffset? pendingSince, DateTimeOffset? lastFired)
    {
        lock (_lock)
        try
        {
            using var conn = Open();
            using var cmd  = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO alert_state (rule_id, state, last_value, pending_ticks, fired_ticks)
                VALUES (@id, @state, @val, @pend, @fired)
                ON CONFLICT(rule_id) DO UPDATE SET state=excluded.state, last_value=excluded.last_value,
                    pending_ticks=excluded.pending_ticks, fired_ticks=excluded.fired_ticks
                """;
            cmd.Parameters.AddWithValue("@id",    ruleId);
            cmd.Parameters.AddWithValue("@state", (int)state);
            cmd.Parameters.AddWithValue("@val",   lastValue);
            cmd.Parameters.AddWithValue("@pend",  (object?)pendingSince?.UtcTicks ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@fired", (object?)lastFired?.UtcTicks ?? DBNull.Value);
            cmd.ExecuteNonQuery();
        }
        catch (Exception ex) { _logger.LogError(ex, "Failed to persist alert state"); }
    }

    public IReadOnlyList<(string RuleId, AlertState State, double LastValue, DateTimeOffset? Pending, DateTimeOffset? Fired)> LoadStates()
    {
        var list = new List<(string, AlertState, double, DateTimeOffset?, DateTimeOffset?)>();
        try
        {
            using var conn = Open();
            using var cmd  = conn.CreateCommand();
            cmd.CommandText = "SELECT rule_id, state, last_value, pending_ticks, fired_ticks FROM alert_state";
            using var r = cmd.ExecuteReader();
            while (r.Read())
                list.Add((
                    r.GetString(0), (AlertState)r.GetInt32(1), r.GetDouble(2),
                    r.IsDBNull(3) ? null : new DateTimeOffset(r.GetInt64(3), TimeSpan.Zero),
                    r.IsDBNull(4) ? null : new DateTimeOffset(r.GetInt64(4), TimeSpan.Zero)));
        }
        catch (Exception ex) { _logger.LogError(ex, "Failed to load alert states"); }
        return list;
    }

    // ── Setup ─────────────────────────────────────────────────────────────────

    private void InitTables()
    {
        using var conn = Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS alert_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id TEXT NOT NULL, rule_name TEXT NOT NULL,
                severity INTEGER NOT NULL, state INTEGER NOT NULL,
                value REAL NOT NULL, threshold REAL NOT NULL, at_ticks INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS ix_alert_history_at ON alert_history(at_ticks);
            CREATE TABLE IF NOT EXISTS alert_silences (
                id TEXT PRIMARY KEY, rule_id TEXT NOT NULL, reason TEXT,
                until_ticks INTEGER NOT NULL, created_ticks INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS alert_state (
                rule_id TEXT PRIMARY KEY, state INTEGER NOT NULL, last_value REAL NOT NULL,
                pending_ticks INTEGER, fired_ticks INTEGER);
            CREATE TABLE IF NOT EXISTS maintenance_windows (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, enabled INTEGER NOT NULL,
                days_of_week INTEGER NOT NULL, start_minute INTEGER NOT NULL,
                duration_minutes INTEGER NOT NULL, max_severity INTEGER);
            """;
        cmd.ExecuteNonQuery();
        PruneHistory();
    }

    private SqliteConnection Open()
    {
        var conn = new SqliteConnection(_connStr);
        conn.Open();
        using var wal = conn.CreateCommand();
        wal.CommandText = "PRAGMA journal_mode=WAL;";
        wal.ExecuteNonQuery();
        return conn;
    }
}
