using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;

namespace Ameto.Alerts;

/// <summary>
/// Persists <see cref="AlertRule"/> definitions to a JSON file and hot-reloads
/// them when the file changes on disk.
///
/// File format: JSON array of <c>AlertRuleDto</c> objects.
/// Default path: <c>{DataDirectory}/alerts.json</c>
/// </summary>
public sealed class AlertRuleStore : IDisposable
{
    private readonly string                    _filePath;
    private readonly ILogger<AlertRuleStore>   _logger;
    private readonly FileSystemWatcher         _watcher;
    private readonly object                    _lock = new();

    private volatile IReadOnlyList<AlertRule>  _rules = [];

    // Fired whenever the rule list is reloaded.
    public event Action? RulesChanged;

    private static readonly JsonSerializerOptions _jsonOpts = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented               = true,
        DefaultIgnoreCondition      = JsonIgnoreCondition.WhenWritingNull,
        Converters                  = { new AlertChannelConverter() },
    };

    public AlertRuleStore(string dataDirectory, ILogger<AlertRuleStore> logger)
    {
        _logger   = logger;
        _filePath = Path.Combine(dataDirectory, "alerts.json");

        Directory.CreateDirectory(dataDirectory);

        _watcher = new FileSystemWatcher(dataDirectory, "alerts.json")
        {
            NotifyFilter          = NotifyFilters.LastWrite | NotifyFilters.FileName,
            EnableRaisingEvents   = true,
        };
        _watcher.Changed += OnFileChanged;
        _watcher.Created += OnFileChanged;

        LoadFromDisk();
    }

    public IReadOnlyList<AlertRule> GetAll() => _rules;

    public AlertRule? GetById(string id) =>
        _rules.FirstOrDefault(r => r.Id == id);

    /// <summary>
    /// Upserts a rule and persists the full list.
    /// Returns the updated list.
    /// </summary>
    public IReadOnlyList<AlertRule> Upsert(AlertRule rule)
    {
        lock (_lock)
        {
            var list  = _rules.ToList();
            int index = list.FindIndex(r => r.Id == rule.Id);
            if (index >= 0) list[index] = rule;
            else            list.Add(rule);
            _rules = list.AsReadOnly();
            SaveToDisk(_rules);
            return _rules;
        }
    }

    /// <summary>Removes a rule by id and persists.</summary>
    public bool Delete(string id)
    {
        lock (_lock)
        {
            var list  = _rules.ToList();
            int index = list.FindIndex(r => r.Id == id);
            if (index < 0) return false;
            list.RemoveAt(index);
            _rules = list.AsReadOnly();
            SaveToDisk(_rules);
            return true;
        }
    }

    // ── Persistence ───────────────────────────────────────────────────────────

    private void LoadFromDisk()
    {
        if (!File.Exists(_filePath))
        {
            _rules = [];
            return;
        }

        try
        {
            var json  = File.ReadAllText(_filePath);
            var dtos  = JsonSerializer.Deserialize<List<AlertRuleDto>>(json, _jsonOpts) ?? [];
            _rules    = dtos.Select(FromDto).ToList().AsReadOnly();
            _logger.LogInformation("Loaded {Count} alert rules from {File}", _rules.Count, _filePath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load alert rules from {File}", _filePath);
        }
    }

    private void SaveToDisk(IReadOnlyList<AlertRule> rules)
    {
        try
        {
            var dtos = rules.Select(ToDto).ToList();
            var json = JsonSerializer.Serialize(dtos, _jsonOpts);
            // Write atomically via temp file
            var tmp  = _filePath + ".tmp";
            File.WriteAllText(tmp, json);
            File.Move(tmp, _filePath, overwrite: true);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save alert rules to {File}", _filePath);
        }
    }

    private void OnFileChanged(object _, FileSystemEventArgs __)
    {
        // Debounce: wait briefly then reload
        Task.Delay(200).ContinueWith(_ =>
        {
            lock (_lock) LoadFromDisk();
            RulesChanged?.Invoke();
        });
    }

    public void Dispose() => _watcher.Dispose();

    // ── DTO mapping ───────────────────────────────────────────────────────────

    private static AlertRule FromDto(AlertRuleDto d) => new()
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
        Filter      = d.Filter,
        NoData      = d.NoData,
        Metric      = d.Metric,
        Aggregation = d.Aggregation,
        Quantile    = d.Quantile,
        GroupBy     = d.GroupBy,
        Labels      = d.Labels,
        Service     = d.Service,
        TraceMetric = ParseEnum(d.TraceMetric, TraceMetricKind.ErrorRatePct),
        Channels    = d.Channels ?? [],
        Template    = d.Template,
    };

    private static AlertRuleDto ToDto(AlertRule r) => new()
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
        Filter          = r.Filter,
        NoData          = r.NoData,
        Metric          = r.Metric,
        Aggregation     = r.Aggregation,
        Quantile        = r.Quantile,
        GroupBy         = r.GroupBy,
        Labels          = r.Labels,
        Service         = r.Service,
        TraceMetric     = r.TraceMetric.ToString(),
        Channels        = r.Channels.ToList(),
        Template        = r.Template,
    };

    private static T ParseEnum<T>(string? s, T fallback) where T : struct, Enum =>
        Enum.TryParse<T>(s, ignoreCase: true, out var v) ? v : fallback;
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
        var type       = root.TryGetProperty("type", out var tp) ? tp.GetString() : null;

        return (type?.ToLowerInvariant()) switch
        {
            "smtp"     => JsonSerializer.Deserialize<SmtpChannel>(root.GetRawText(), options),
            "telegram" => JsonSerializer.Deserialize<TelegramChannel>(root.GetRawText(), options),
            "webhook"  => JsonSerializer.Deserialize<WebhookChannel>(root.GetRawText(), options),
            _          => JsonSerializer.Deserialize<WebhookChannel>(root.GetRawText(), options),
        };
    }

    public override void Write(Utf8JsonWriter writer,
        AlertChannel value, JsonSerializerOptions options)
    {
        switch (value)
        {
            case WebhookChannel  wh: JsonSerializer.Serialize(writer, wh, options); break;
            case SmtpChannel     sm: JsonSerializer.Serialize(writer, sm, options); break;
            case TelegramChannel tg: JsonSerializer.Serialize(writer, tg, options); break;
            default: JsonSerializer.Serialize(writer, value, options); break;
        }
    }
}
