using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Ameto.Core;

namespace Ameto.Alerts;

/// <summary>
/// Persists <see cref="AlertRule"/> definitions to the shared <c>Ameto.db</c>
/// SQLite database (table <c>alert_rules</c>, one row per rule).
///
/// Row-level storage is deliberately robust: each rule is loaded independently,
/// so a single unparseable rule is skipped (and logged) instead of taking the
/// whole rule set down — the failure mode the old single-JSON-file store had.
/// Channel secrets stay encrypted at rest inside each row's payload.
///
/// A one-time migration imports a legacy <c>alerts.json</c> on first start and
/// renames it aside.
/// </summary>
public sealed class AlertRuleStore : IDisposable
{
    private readonly string                    _connStr;
    private readonly string                    _legacyJsonPath;
    private readonly ISecretProtector          _protector;
    private readonly ILogger<AlertRuleStore>   _logger;
    private readonly object                    _lock = new();

    private volatile IReadOnlyList<AlertRule>  _rules = [];

    // Fired whenever the rule list changes.
    public event Action? RulesChanged;

    private static readonly JsonSerializerOptions _jsonOpts = new()
    {
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition      = JsonIgnoreCondition.WhenWritingNull,
        Converters                  = { new AlertChannelConverter() },
    };

    public AlertRuleStore(string dataDirectory, ISecretProtector protector, ILogger<AlertRuleStore> logger)
    {
        _protector      = protector;
        _logger         = logger;
        _connStr        = $"Data Source={Path.Combine(dataDirectory, "Ameto.db")}";
        _legacyJsonPath = Path.Combine(dataDirectory, "alerts.json");

        Directory.CreateDirectory(dataDirectory);
        InitTable();
        MigrateLegacyJsonIfPresent();
        LoadFromDb();
    }

    public IReadOnlyList<AlertRule> GetAll() => _rules;

    public AlertRule? GetById(string id) =>
        _rules.FirstOrDefault(r => r.Id == id);

    /// <summary>Upserts a rule (its own row) and refreshes the in-memory list.</summary>
    public IReadOnlyList<AlertRule> Upsert(AlertRule rule)
    {
        lock (_lock)
        {
            WriteRow(rule);
            var list  = _rules.ToList();
            int index = list.FindIndex(r => r.Id == rule.Id);
            if (index >= 0) list[index] = rule;
            else            list.Add(rule);
            _rules = list.AsReadOnly();
        }
        RulesChanged?.Invoke();
        return _rules;
    }

    /// <summary>Removes a rule's row and refreshes the in-memory list.</summary>
    public bool Delete(string id)
    {
        bool removed;
        lock (_lock)
        {
            using var conn = Open();
            using var cmd  = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM alert_rules WHERE id = @id";
            cmd.Parameters.AddWithValue("@id", id);
            removed = cmd.ExecuteNonQuery() > 0;
            if (removed)
                _rules = _rules.Where(r => r.Id != id).ToList().AsReadOnly();
        }
        if (removed) RulesChanged?.Invoke();
        return removed;
    }

    // ── Persistence (SQLite) ──────────────────────────────────────────────────

    private SqliteConnection Open()
    {
        var conn = new SqliteConnection(_connStr);
        conn.Open();
        using var pragma = conn.CreateCommand();
        pragma.CommandText = "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;";
        pragma.ExecuteNonQuery();
        return conn;
    }

    private void InitTable()
    {
        using var conn = Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS alert_rules (
                id         TEXT PRIMARY KEY,
                data       TEXT NOT NULL,   -- serialized AlertRuleDto (channels + encrypted secrets)
                updated_at TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();
    }

    private void LoadFromDb()
    {
        var rules = new List<AlertRule>();
        try
        {
            using var conn = Open();
            using var cmd  = conn.CreateCommand();
            cmd.CommandText = "SELECT id, data FROM alert_rules";
            using var r = cmd.ExecuteReader();
            while (r.Read())
            {
                var id  = r.GetString(0);
                var raw = r.GetString(1);
                try
                {
                    // Row-level isolation: a single corrupt rule is skipped, not fatal.
                    var dto = JsonSerializer.Deserialize<AlertRuleDto>(raw, _jsonOpts);
                    if (dto is not null) rules.Add(FromDto(dto));
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Skipping unparseable alert rule {Id}", id);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load alert rules from the database");
        }
        _rules = rules.AsReadOnly();
        _logger.LogInformation("Loaded {Count} alert rule(s) from the database", _rules.Count);
    }

    private void WriteRow(AlertRule rule)
    {
        try
        {
            var json = JsonSerializer.Serialize(ToDto(rule), _jsonOpts);
            using var conn = Open();
            using var cmd  = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO alert_rules (id, data, updated_at) VALUES (@id, @data, @ts)
                ON CONFLICT(id) DO UPDATE SET data = @data, updated_at = @ts
                """;
            cmd.Parameters.AddWithValue("@id",   rule.Id);
            cmd.Parameters.AddWithValue("@data", json);
            cmd.Parameters.AddWithValue("@ts",   DateTimeOffset.UtcNow.ToString("O"));
            cmd.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to persist alert rule {Id}", rule.Id);
        }
    }

    /// <summary>
    /// Imports a legacy alerts.json exactly once (when the table is empty and the
    /// file exists), then renames the file aside so it is never re-imported and
    /// the old FileSystemWatcher hot-reload path is fully retired.
    /// </summary>
    private void MigrateLegacyJsonIfPresent()
    {
        if (!File.Exists(_legacyJsonPath)) return;
        try
        {
            using (var conn = Open())
            using (var count = conn.CreateCommand())
            {
                count.CommandText = "SELECT COUNT(*) FROM alert_rules";
                if (Convert.ToInt64(count.ExecuteScalar()) > 0)
                {
                    // Table already populated — just archive the stale file.
                    ArchiveLegacyFile();
                    return;
                }
            }

            var json = File.ReadAllText(_legacyJsonPath);
            var dtos = JsonSerializer.Deserialize<List<AlertRuleDto>>(json, _jsonOpts) ?? [];
            int imported = 0;
            foreach (var dto in dtos)
            {
                try
                {
                    var rule = FromDto(dto);           // decrypts secrets
                    WriteRow(rule);                    // re-encrypts on write
                    imported++;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Skipping unmigratable alert rule '{Name}' from alerts.json", dto.Name);
                }
            }
            _logger.LogInformation("Migrated {Count} alert rule(s) from alerts.json into the database", imported);
            ArchiveLegacyFile();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "alerts.json migration failed — leaving the file in place");
        }
    }

    private void ArchiveLegacyFile()
    {
        try
        {
            var archived = _legacyJsonPath + ".migrated";
            File.Move(_legacyJsonPath, archived, overwrite: true);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not rename alerts.json after migration");
        }
    }

    public void Dispose() { }

    // ── DTO mapping ───────────────────────────────────────────────────────────

    private AlertRule FromDto(AlertRuleDto d) => new()
    {
        Id          = d.Id   ?? Guid.NewGuid().ToString("N")[..8],
        Name        = d.Name ?? "Unnamed",
        Enabled     = d.Enabled,
        Severity    = ParseEnum(d.Severity, AlertSeverity.Warning),
        Source      = ParseEnum(d.Source, AlertSource.Log),
        Comparator  = ParseEnum(d.Comparator, AlertComparator.GreaterOrEqual),
        Threshold   = d.Threshold,
        Window      = TimeSpan.FromSeconds(d.WindowSeconds   > 0 ? d.WindowSeconds   : 300),
        For         = TimeSpan.FromSeconds(d.ForSeconds      > 0 ? d.ForSeconds      : 0),
        Cooldown    = TimeSpan.FromSeconds(d.CooldownSeconds > 0 ? d.CooldownSeconds : 900),
        RepeatInterval = TimeSpan.FromSeconds(d.RepeatSeconds > 0 ? d.RepeatSeconds : 0),
        EscalateAfter  = TimeSpan.FromSeconds(d.EscalateSeconds > 0 ? d.EscalateSeconds : 0),
        Filter      = d.Filter,
        NoData      = d.NoData,
        Metric      = d.Metric,
        Aggregation = d.Aggregation,
        Quantile    = d.Quantile,
        GroupBy     = d.GroupBy,
        Labels      = d.Labels,
        Service     = d.Service,
        TraceMetric = ParseEnum(d.TraceMetric, TraceMetricKind.ErrorRatePct),
        Channels    = (d.Channels ?? []).Select(UnprotectChannel).ToList(),
        Template    = d.Template,
    };

    private AlertRuleDto ToDto(AlertRule r) => new()
    {
        Id              = r.Id,
        Name            = r.Name,
        Enabled         = r.Enabled,
        Severity        = r.Severity.ToString(),
        Source          = r.Source.ToString(),
        Comparator      = r.Comparator.ToString(),
        Threshold       = r.Threshold,
        WindowSeconds   = (int)r.Window.TotalSeconds,
        ForSeconds      = (int)r.For.TotalSeconds,
        CooldownSeconds = (int)r.Cooldown.TotalSeconds,
        RepeatSeconds   = (int)r.RepeatInterval.TotalSeconds,
        EscalateSeconds = (int)r.EscalateAfter.TotalSeconds,
        Filter          = r.Filter,
        NoData          = r.NoData,
        Metric          = r.Metric,
        Aggregation     = r.Aggregation,
        Quantile        = r.Quantile,
        GroupBy         = r.GroupBy,
        Labels          = r.Labels,
        Service         = r.Service,
        TraceMetric     = r.TraceMetric.ToString(),
        Channels        = r.Channels.Select(ProtectChannel).ToList(),
        Template        = r.Template,
    };

    private static T ParseEnum<T>(string? s, T fallback) where T : struct, Enum =>
        Enum.TryParse<T>(s, ignoreCase: true, out var v) ? v : fallback;

    // ── Channel secret encryption (at rest) ──────────────────────────────────────
    // Channel objects are init-only, so protecting/unprotecting produces a fresh copy.

    private AlertChannel ProtectChannel(AlertChannel ch) => Transform(ch, _protector.Protect);
    private AlertChannel UnprotectChannel(AlertChannel ch) => Transform(ch, _protector.Unprotect);

    private static AlertChannel Transform(AlertChannel ch, Func<string?, string> f)
    {
        AlertChannel result = ch switch
        {
            TelegramChannel t => new TelegramChannel { ChatId = t.ChatId, BotToken = f(t.BotToken) },
            SmtpChannel s     => new SmtpChannel
            {
                Host = s.Host, Port = s.Port, UseSsl = s.UseSsl, Username = s.Username,
                Password = string.IsNullOrEmpty(s.Password) ? s.Password : f(s.Password),
                From = s.From, To = s.To,
            },
            WebhookChannel w  => new WebhookChannel
            {
                Url = w.Url,
                Headers = w.Headers?.ToDictionary(kv => kv.Key, kv => f(kv.Value)),
            },
            SlackChannel s     => new SlackChannel     { WebhookUrl = f(s.WebhookUrl) },
            DiscordChannel d   => new DiscordChannel   { WebhookUrl = f(d.WebhookUrl) },
            TeamsChannel tm    => new TeamsChannel     { WebhookUrl = f(tm.WebhookUrl) },
            PagerDutyChannel p => new PagerDutyChannel { RoutingKey = f(p.RoutingKey) },
            HttpFlowChannel hf => new HttpFlowChannel
            {
                Steps   = hf.Steps,   // non-secret (reference the {{secret.*}} placeholders, not the values)
                Secrets = hf.Secrets.ToDictionary(kv => kv.Key, kv => f(kv.Value)),
            },
            _ => ch,
        };
        result.EscalationOnly = ch.EscalationOnly;   // preserve the (non-secret) routing flags across the copy
        result.MinSeverity    = ch.MinSeverity;
        return result;
    }

    private bool HasPlaintextSecret(AlertChannel ch) => ch switch
    {
        TelegramChannel t  => IsPlaintext(t.BotToken),
        SmtpChannel s      => IsPlaintext(s.Password),
        WebhookChannel w   => w.Headers is not null && w.Headers.Values.Any(IsPlaintext),
        SlackChannel s     => IsPlaintext(s.WebhookUrl),
        DiscordChannel d   => IsPlaintext(d.WebhookUrl),
        TeamsChannel tm    => IsPlaintext(tm.WebhookUrl),
        PagerDutyChannel p => IsPlaintext(p.RoutingKey),
        HttpFlowChannel hf => hf.Secrets.Values.Any(IsPlaintext),
        _ => false,
    };
    private bool IsPlaintext(string? v) => !string.IsNullOrEmpty(v) && !_protector.IsProtected(v);
}

// ── DTO types for JSON serialisation ─────────────────────────────────────────

internal sealed class AlertRuleDto
{
    public string?              Id              { get; set; }
    public string?              Name            { get; set; }
    public bool                 Enabled         { get; set; } = true;
    public string?              Severity        { get; set; }
    public string?              Source          { get; set; }
    public string?              Comparator      { get; set; }
    public double               Threshold       { get; set; } = 1;
    public int                  WindowSeconds   { get; set; } = 300;
    public int                  ForSeconds      { get; set; }
    public int                  CooldownSeconds { get; set; } = 900;
    public int                  RepeatSeconds   { get; set; }
    public int                  EscalateSeconds { get; set; }
    public string?              Filter          { get; set; }
    public bool                 NoData          { get; set; }
    public string?              Metric          { get; set; }
    public string?              Aggregation     { get; set; }
    public double?              Quantile        { get; set; }
    public string[]?            GroupBy         { get; set; }
    public Dictionary<string,string>? Labels    { get; set; }
    public string?              Service         { get; set; }
    public string?              TraceMetric     { get; set; }
    public List<AlertChannel>?  Channels        { get; set; }
    public string?              Template        { get; set; }
}

// ── Polymorphic channel JSON converter ────────────────────────────────────────

internal sealed class AlertChannelConverter : JsonConverter<AlertChannel>
{
    public override AlertChannel? Read(ref Utf8JsonReader reader,
        Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc  = JsonDocument.ParseValue(ref reader);
        var root       = doc.RootElement;
        // JsonDocument.TryGetProperty is case-SENSITIVE and ignores the reader's
        // PropertyNameCaseInsensitive — so accept both the camelCase and PascalCase
        // spellings the serializer has produced across versions. Getting this wrong
        // sent every channel down the default branch and broke rule loading.
        var type = (root.TryGetProperty("type", out var tp) || root.TryGetProperty("Type", out tp))
            ? tp.GetString() : null;

        return (type?.ToLowerInvariant()) switch
        {
            "smtp"      => JsonSerializer.Deserialize<SmtpChannel>(root.GetRawText(), options),
            "telegram"  => JsonSerializer.Deserialize<TelegramChannel>(root.GetRawText(), options),
            "webhook"   => JsonSerializer.Deserialize<WebhookChannel>(root.GetRawText(), options),
            "slack"     => JsonSerializer.Deserialize<SlackChannel>(root.GetRawText(), options),
            "discord"   => JsonSerializer.Deserialize<DiscordChannel>(root.GetRawText(), options),
            "teams"     => JsonSerializer.Deserialize<TeamsChannel>(root.GetRawText(), options),
            "pagerduty" => JsonSerializer.Deserialize<PagerDutyChannel>(root.GetRawText(), options),
            "httpflow"  => JsonSerializer.Deserialize<HttpFlowChannel>(root.GetRawText(), options),
            // Unknown/absent type: fail loudly rather than silently coercing to a
            // WebhookChannel (whose required Url then throws a misleading error).
            _ => throw new JsonException(
                $"Alert channel has an unknown or missing 'type' ({type ?? "null"})."),
        };
    }

    public override void Write(Utf8JsonWriter writer,
        AlertChannel value, JsonSerializerOptions options)
    {
        switch (value)
        {
            case WebhookChannel   wh: JsonSerializer.Serialize(writer, wh, options); break;
            case SmtpChannel      sm: JsonSerializer.Serialize(writer, sm, options); break;
            case TelegramChannel  tg: JsonSerializer.Serialize(writer, tg, options); break;
            case SlackChannel     sl: JsonSerializer.Serialize(writer, sl, options); break;
            case DiscordChannel   dc: JsonSerializer.Serialize(writer, dc, options); break;
            case TeamsChannel     tm: JsonSerializer.Serialize(writer, tm, options); break;
            case PagerDutyChannel pd: JsonSerializer.Serialize(writer, pd, options); break;
            case HttpFlowChannel  hf: JsonSerializer.Serialize(writer, hf, options); break;
            default: JsonSerializer.Serialize(writer, value, options); break;
        }
    }
}
