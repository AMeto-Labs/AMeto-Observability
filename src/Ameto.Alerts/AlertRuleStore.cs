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
        Id        = d.Id        ?? Guid.NewGuid().ToString("N")[..8],
        Name      = d.Name      ?? "Unnamed",
        Filter    = d.Filter,
        Threshold = d.Threshold > 0 ? d.Threshold : 1,
        Window    = d.WindowSeconds > 0 ? TimeSpan.FromSeconds(d.WindowSeconds) : TimeSpan.FromMinutes(5),
        Cooldown  = d.CooldownSeconds > 0 ? TimeSpan.FromSeconds(d.CooldownSeconds) : TimeSpan.FromMinutes(15),
        Enabled   = d.Enabled,
        Channels  = d.Channels ?? [],
    };

    private static AlertRuleDto ToDto(AlertRule r) => new()
    {
        Id              = r.Id,
        Name            = r.Name,
        Filter          = r.Filter,
        Threshold       = r.Threshold,
        WindowSeconds   = (int)r.Window.TotalSeconds,
        CooldownSeconds = (int)r.Cooldown.TotalSeconds,
        Enabled         = r.Enabled,
        Channels        = r.Channels.ToList(),
    };
}

// ── DTO types for JSON serialisation ─────────────────────────────────────────

internal sealed class AlertRuleDto
{
    public string?              Id              { get; set; }
    public string?              Name            { get; set; }
    public string?              Filter          { get; set; }
    public int                  Threshold       { get; set; } = 1;
    public int                  WindowSeconds   { get; set; } = 300;
    public int                  CooldownSeconds { get; set; } = 900;
    public bool                 Enabled         { get; set; } = true;
    public List<AlertChannel>?  Channels        { get; set; }
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
            "smtp"    => JsonSerializer.Deserialize<SmtpChannel>(root.GetRawText(), options),
            "webhook" => JsonSerializer.Deserialize<WebhookChannel>(root.GetRawText(), options),
            _         => JsonSerializer.Deserialize<WebhookChannel>(root.GetRawText(), options),
        };
    }

    public override void Write(Utf8JsonWriter writer,
        AlertChannel value, JsonSerializerOptions options)
    {
        switch (value)
        {
            case WebhookChannel wh: JsonSerializer.Serialize(writer, wh, options); break;
            case SmtpChannel    sm: JsonSerializer.Serialize(writer, sm, options); break;
            default: JsonSerializer.Serialize(writer, value, options); break;
        }
    }
}
