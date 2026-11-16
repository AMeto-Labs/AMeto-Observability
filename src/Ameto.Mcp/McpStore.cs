using System.Collections.Frozen;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ameto.Core;
using Ameto.Mcp.Masking;
using Ameto.Mcp.Tools;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace Ameto.Mcp;

/// <summary>
/// Everything a call needs to know about the section, as of one moment: read with one volatile
/// load and never changed afterwards, so a call that started under one set of rules finishes
/// under it.
/// </summary>
public sealed class McpSnapshot
{
    public required McpSettings                  Settings { get; init; }
    public required IReadOnlyList<McpProfile>    Profiles { get; init; }
    public required IReadOnlyList<McpMaskRule>   Rules    { get; init; }
    public required MaskingPolicy                Standard { get; init; }
    public required MaskingPolicy                Strict   { get; init; }

    /// <summary>The published tools, drafts included, in name order.</summary>
    public IReadOnlyList<McpPublishedTool> Tools { get; init; } = [];

    internal FrozenDictionary<string, McpProfile> ByKeyHash { get; init; } = FrozenDictionary<string, McpProfile>.Empty;
    internal FrozenDictionary<string, McpProfile> ById      { get; init; } = FrozenDictionary<string, McpProfile>.Empty;

    public MaskingPolicy PolicyFor(McpProfile profile) =>
        profile.MaskingMode == McpMaskingMode.Strict ? Strict : Standard;

    public McpProfile? FindProfile(string id) => ById.GetValueOrDefault(id);

    public McpPublishedTool? FindTool(string name)
    {
        foreach (var tool in Tools)
            if (tool.Name.Equals(name, StringComparison.Ordinal)) return tool;
        return null;
    }
}

/// <summary>
/// The AI section's settings in the shared <c>Ameto.db</c>: profiles, field rules, the section's
/// own settings and the secret the tokens are made with. Like <c>AlertRuleStore</c> it keeps its
/// own connection string to that file and its own tables; nothing here is on a hot path — a call
/// reads <see cref="Current"/>, and only an admin's change writes.
/// </summary>
public sealed class McpStore
{
    private const string KeyPrefix = "mcp_";

    private readonly string            _connStr;
    private readonly ISecretProtector  _protector;
    private readonly ILogger<McpStore> _logger;
    private readonly Lock              _gate = new();

    private byte[]               _secret;
    private DateTimeOffset       _secretCreatedAt;
    private volatile McpSnapshot _snapshot;

    /// <summary>Shared by every policy the store builds, and emptied when the secret changes.</summary>
    public TokenVault Vault { get; } = new();

    public McpSnapshot Current => _snapshot;

    /// <summary>
    /// Raised after a change that can alter what some profile is offered: a tool saved or
    /// deleted, a profile's tools changed, a profile gone. Raised outside the store's lock.
    /// </summary>
    public event Action? ToolListsChanged;

    /// <summary>When the secret the tokens are made with was created or last rotated.</summary>
    public DateTimeOffset SecretCreatedAt { get { lock (_gate) return _secretCreatedAt; } }

    public McpStore(string dataDirectory, ISecretProtector protector, ILogger<McpStore> logger)
    {
        _protector = protector;
        _logger    = logger;
        Directory.CreateDirectory(dataDirectory);
        _connStr = $"Data Source={Path.Combine(dataDirectory, "Ameto.db")}";

        InitSchema();
        (_secret, _secretCreatedAt) = LoadOrCreateSecret();
        SeedDefaultRulesOnce();
        _snapshot = Load();
    }

    // ── Keys ──────────────────────────────────────────────────────────────────

    /// <summary>The profile a raw key belongs to, or null.</summary>
    public McpProfile? Authenticate(ReadOnlySpan<char> rawKey) =>
        rawKey.IsEmpty ? null : _snapshot.ByKeyHash.GetValueOrDefault(HashKey(rawKey));

    internal static string HashKey(ReadOnlySpan<char> rawKey)
    {
        int max = Encoding.UTF8.GetMaxByteCount(rawKey.Length);
        Span<byte> utf8 = max <= 512 ? stackalloc byte[512] : new byte[max];
        Span<byte> digest = stackalloc byte[32];
        SHA256.HashData(utf8[..Encoding.UTF8.GetBytes(rawKey, utf8)], digest);
        return Convert.ToHexStringLower(digest);
    }

    private static string NewKey() =>
        KeyPrefix + Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(24));

    // ── Profiles ──────────────────────────────────────────────────────────────

    /// <summary>Creates a profile and its key. The key is returned here and nowhere else, ever.</summary>
    public (McpProfile Profile, string Key) CreateProfile(McpProfileInput input, string createdBy)
    {
        string key = NewKey();
        var profile = Apply(new McpProfile
        {
            Id        = Guid.NewGuid().ToString("N"),
            Name      = "",
            KeyHash   = HashKey(key),
            // A profile created without a list is given the built-in tools: the common case,
            // and the one the UI starts from. An explicit list, the empty one included, is
            // taken as it is.
            Tools     = [.. Tools.BuiltInTools.All.Select(static t => t.Name)],
            CreatedBy = createdBy,
            CreatedAt = DateTimeOffset.UtcNow,
        }, input);

        lock (_gate)
        {
            Atomically(() =>
            {
                WriteProfile(profile);
                WriteGrants(profile.Id, input.PublishedTools);
            });
            _snapshot = Load();
        }
        return (profile, key);
    }

    /// <summary>
    /// Changes the members <paramref name="input"/> names and leaves the others as they are
    /// (see <see cref="McpProfileInput"/>). Null when there is no such profile.
    /// </summary>
    public McpProfile? UpdateProfile(string id, McpProfileInput input)
    {
        McpProfile updated;
        lock (_gate)
        {
            if (_snapshot.FindProfile(id) is not { } existing) return null;
            updated = Apply(existing, input);
            Atomically(() =>
            {
                WriteProfile(updated);
                WriteGrants(id, input.PublishedTools);
            });
            _snapshot = Load();
        }
        ToolListsChanged?.Invoke();
        return updated;
    }

    /// <summary>
    /// Makes <paramref name="toolIds"/> the published tools that name the profile: each tool
    /// gains or loses it as the list says. An id that is no tool's is nobody's to grant and is
    /// passed over. Under the gate, before the snapshot is reloaded.
    /// </summary>
    private void WriteGrants(string profileId, IReadOnlyList<string>? toolIds)
    {
        if (toolIds is null) return;
        var granted = new HashSet<string>(toolIds, StringComparer.Ordinal);
        foreach (var tool in _snapshot.Tools)
        {
            bool has = tool.Profiles.Contains(profileId, StringComparer.Ordinal);
            if (has == granted.Contains(tool.Id)) continue;
            // The tool's own definition has not changed, so neither has the time it was last edited.
            WriteTool(tool with
            {
                Profiles = has ? [.. tool.Profiles.Where(p => p != profileId)] : [.. tool.Profiles, profileId],
            });
        }
    }

    /// <summary>Replaces the profile's key; the old one stops working with this call.</summary>
    public string? RotateKey(string id)
    {
        lock (_gate)
        {
            if (_snapshot.FindProfile(id) is not { } existing) return null;
            string key = NewKey();
            WriteProfile(existing with { KeyHash = HashKey(key) });
            _snapshot = Load();
            return key;
        }
    }

    public bool DeleteProfile(string id)
    {
        lock (_gate)
        {
            bool removed = false;
            Atomically(() =>
            {
                removed = Execute("DELETE FROM mcp_profiles WHERE id = @id", ("@id", id)) > 0;
                if (!removed) return;
                foreach (var tool in _snapshot.Tools)
                    if (tool.Profiles.Contains(id, StringComparer.Ordinal))
                        WriteTool(tool with { Profiles = [.. tool.Profiles.Where(p => p != id)] });
            });
            if (!removed) return false;
            _snapshot = Load();
        }
        ToolListsChanged?.Invoke();
        return true;
    }

    /// <summary>
    /// Records that a profile was just used. Kept to one write a minute per profile: the column
    /// answers "is this key still in use", which does not need the second.
    /// </summary>
    public void Touch(string profileId, DateTimeOffset now)
    {
        long ms = now.ToUnixTimeMilliseconds();
        if (_snapshot.FindProfile(profileId) is not { } profile || ms - profile.LastCallUnixMs < 60_000) return;
        lock (_gate)
        {
            try
            {
                Execute("UPDATE mcp_profiles SET last_call_ms = @ms WHERE id = @id", ("@ms", ms), ("@id", profileId));
                _snapshot = Load();
            }
            catch (SqliteException e)
            {
                _logger.LogWarning(e, "Could not record the last call of AI profile {ProfileId}", profileId);
            }
        }
    }

    /// <summary>
    /// <paramref name="profile"/> with what <paramref name="input"/> names set on it. A member
    /// the input leaves null keeps the value the profile has — which for a new profile is the
    /// record's own default, and for an existing one is what was stored.
    /// </summary>
    private static McpProfile Apply(McpProfile profile, McpProfileInput input) => profile with
    {
        Name             = input.Name?.Trim() ?? profile.Name,
        Description      = input.Description?.Trim() ?? profile.Description,
        ToolsMode        = (input.ToolsMode ?? profile.ToolsMode) == McpToolsMode.PublishedOnly
                               ? McpToolsMode.PublishedOnly : McpToolsMode.BuiltInAndPublished,
        Tools            = input.Tools is { } tools ? Distinct(tools) : profile.Tools,
        Services         = input.Services is { } services ? Distinct(services) : profile.Services,
        MaskingMode      = (input.MaskingMode ?? profile.MaskingMode) == McpMaskingMode.Strict
                               ? McpMaskingMode.Strict : McpMaskingMode.Standard,
        FreeFilters      = input.FreeFilters ?? profile.FreeFilters,
        MaxEvents        = Math.Clamp(input.MaxEvents ?? profile.MaxEvents, 1, 5_000),
        MaxWindowMinutes = Math.Clamp(input.MaxWindowMinutes ?? profile.MaxWindowMinutes, 1, 366 * 24 * 60),
        CallsPerMinute   = Math.Clamp(input.CallsPerMinute ?? profile.CallsPerMinute, 1, 6_000),
    };

    private static string[] Distinct(IReadOnlyList<string>? values) => values is null
        ? []
        : [.. values.Select(static v => v?.Trim() ?? "").Where(static v => v.Length > 0).Distinct(StringComparer.OrdinalIgnoreCase)];

    // ── Field rules ───────────────────────────────────────────────────────────

    /// <summary>Adds a rule, or replaces the one with <paramref name="id"/>.</summary>
    public McpMaskRule UpsertRule(string? id, string field, IReadOnlyList<string>? services, MaskAction action)
    {
        var rule = new McpMaskRule
        {
            Id       = string.IsNullOrEmpty(id) ? Guid.NewGuid().ToString("N") : id,
            Field    = field.Trim(),
            Services = Distinct(services),
            Action   = Enum.IsDefined(action) ? action : MaskAction.Hash,
        };
        lock (_gate)
        {
            WriteRule(rule);
            _snapshot = Load();
        }
        return rule;
    }

    public bool DeleteRule(string id)
    {
        lock (_gate)
        {
            bool removed = Execute("DELETE FROM mcp_mask_rules WHERE id = @id", ("@id", id)) > 0;
            if (removed) _snapshot = Load();
            return removed;
        }
    }

    // ── Published tools ───────────────────────────────────────────────────────

    /// <summary>Adds a tool, or replaces the one with its id. The caller has validated it.</summary>
    public McpPublishedTool SaveTool(McpPublishedTool tool)
    {
        var saved = tool with
        {
            UpdatedAt = DateTimeOffset.UtcNow,
            Fields    = Distinct(tool.Fields),
            Profiles  = Distinct(tool.Profiles),
        };
        lock (_gate)
        {
            WriteTool(saved);
            _snapshot = Load();
        }
        ToolListsChanged?.Invoke();
        return saved;
    }

    public bool DeleteTool(string id)
    {
        bool removed;
        lock (_gate)
        {
            removed = Execute("DELETE FROM mcp_tools WHERE id = @id", ("@id", id)) > 0;
            if (removed) _snapshot = Load();
        }
        if (removed) ToolListsChanged?.Invoke();
        return removed;
    }

    // ── Settings and the secret ───────────────────────────────────────────────

    public McpSettings UpdateSettings(McpSettings settings)
    {
        settings = settings.Clamped() with { DisabledTools = Distinct(settings.DisabledTools) };
        bool toolsChanged;
        lock (_gate)
        {
            toolsChanged = !_snapshot.Settings.DisabledTools.SequenceEqual(settings.DisabledTools, StringComparer.Ordinal);
            Atomically(() =>
            {
                WriteSetting("enabled",                settings.Enabled ? "1" : "0");
                WriteSetting("detectors",              ((int)settings.Detectors).ToString(CultureInfo.InvariantCulture));
                WriteSetting("response_budget_tokens", settings.ResponseBudgetTokens.ToString(CultureInfo.InvariantCulture));
                WriteSetting("max_window_minutes",     settings.MaxWindowMinutes.ToString(CultureInfo.InvariantCulture));
                WriteSetting("max_events",             settings.MaxEvents.ToString(CultureInfo.InvariantCulture));
                WriteSetting("calls_per_minute",       settings.CallsPerMinute.ToString(CultureInfo.InvariantCulture));
                WriteSetting("max_concurrent",         settings.MaxConcurrent.ToString(CultureInfo.InvariantCulture));
                WriteSetting("disabled_tools",         ListJson(settings.DisabledTools));
            });
            _snapshot = Load();
        }
        // A built-in tool switched off or on changes what every profile is offered.
        if (toolsChanged) ToolListsChanged?.Invoke();
        return settings;
    }

    /// <summary>
    /// Replaces the secret. Every token changes with it: a token from an earlier answer no longer
    /// matches anything, and the vault of remembered values is emptied for the same reason.
    /// </summary>
    public DateTimeOffset RotateSecret()
    {
        lock (_gate)
        {
            // Stored before it is put to use: if the write fails, the secret in force is still
            // the one on disk, and the tokens handed out now are the tokens after a restart.
            byte[] secret = RandomNumberGenerator.GetBytes(32);
            var createdAt = DateTimeOffset.UtcNow;
            Atomically(() => WriteSecret(secret, createdAt));

            _secret          = secret;
            _secretCreatedAt = createdAt;
            Vault.Clear();
            _snapshot = Load();
            return createdAt;
        }
    }

    private (byte[] Secret, DateTimeOffset CreatedAt) LoadOrCreateSecret()
    {
        string? stored = ReadSetting("token_secret");
        if (!string.IsNullOrEmpty(stored))
        {
            // A secret that cannot be decrypted (the master key changed) fails the start loudly:
            // silently minting another would change every token without anyone having asked.
            byte[] secret = Convert.FromBase64String(_protector.Unprotect(stored));
            var createdAt = DateTimeOffset.TryParse(ReadSetting("token_secret_created_at"), CultureInfo.InvariantCulture,
                                                    DateTimeStyles.RoundtripKind, out var at) ? at : DateTimeOffset.UtcNow;
            return (secret, createdAt);
        }

        byte[] fresh = RandomNumberGenerator.GetBytes(32);
        var now = DateTimeOffset.UtcNow;
        WriteSecret(fresh, now);
        return (fresh, now);
    }

    private void WriteSecret(byte[] secret, DateTimeOffset createdAt)
    {
        WriteSetting("token_secret",            _protector.Protect(Convert.ToBase64String(secret)));
        WriteSetting("token_secret_created_at", createdAt.ToString("O", CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// A new installation starts with the three rules nobody argues about. Seeded once, by a
    /// marker rather than by the table being empty: an admin who deletes them has said so.
    /// </summary>
    private void SeedDefaultRulesOnce()
    {
        if (ReadSetting("rules_seeded") is not null) return;
        foreach (string field in (ReadOnlySpan<string>)["*Password*", "*Secret*", "Authorization"])
            WriteRule(new McpMaskRule { Id = Guid.NewGuid().ToString("N"), Field = field, Action = MaskAction.Remove });
        WriteSetting("rules_seeded", "1");
    }

    // ── Loading ───────────────────────────────────────────────────────────────

    private McpSnapshot Load()
    {
        using var conn = Open();

        var settings = new McpSettings();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT key, value FROM mcp_settings";
            using var r = cmd.ExecuteReader();
            while (r.Read())
            {
                string value = r.GetString(1);
                int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int number);
                settings = r.GetString(0) switch
                {
                    "enabled"                => settings with { Enabled = value != "0" },
                    "detectors"              => settings with { Detectors = (MaskDetectors)number },
                    "response_budget_tokens" => settings with { ResponseBudgetTokens = number },
                    "max_window_minutes"     => settings with { MaxWindowMinutes = number },
                    "max_events"             => settings with { MaxEvents = number },
                    "calls_per_minute"       => settings with { CallsPerMinute = number },
                    "max_concurrent"         => settings with { MaxConcurrent = number },
                    "disabled_tools"         => settings with { DisabledTools = List(value) },
                    _                        => settings,
                };
            }
        }
        settings = settings.Clamped();

        var profiles = new List<McpProfile>();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = """
                SELECT id, name, description, key_hash, tools_mode, tools, services, masking_mode, free_filters,
                       max_events, max_window_minutes, calls_per_minute, created_by, created_at, last_call_ms
                FROM mcp_profiles ORDER BY created_at
                """;
            using var r = cmd.ExecuteReader();
            while (r.Read())
            {
                profiles.Add(new McpProfile
                {
                    Id               = r.GetString(0),
                    Name             = r.GetString(1),
                    Description      = r.GetString(2),
                    KeyHash          = r.GetString(3),
                    ToolsMode        = (McpToolsMode)r.GetInt32(4),
                    Tools            = List(r.GetString(5)),
                    Services         = List(r.GetString(6)),
                    MaskingMode      = (McpMaskingMode)r.GetInt32(7),
                    FreeFilters      = r.GetInt32(8) != 0,
                    MaxEvents        = r.GetInt32(9),
                    MaxWindowMinutes = r.GetInt32(10),
                    CallsPerMinute   = r.GetInt32(11),
                    CreatedBy        = r.GetString(12),
                    CreatedAt        = DateTimeOffset.Parse(r.GetString(13), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
                    LastCallUnixMs   = r.GetInt64(14),
                });
            }
        }

        var rules = new List<McpMaskRule>();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT id, field, services, action FROM mcp_mask_rules ORDER BY created_at, field";
            using var r = cmd.ExecuteReader();
            while (r.Read())
                rules.Add(new McpMaskRule
                {
                    Id       = r.GetString(0),
                    Field    = r.GetString(1),
                    Services = List(r.GetString(2)),
                    Action   = (MaskAction)r.GetInt32(3),
                });
        }

        var tools = new List<McpPublishedTool>();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT id, name, published, payload, created_by, created_at, updated_at FROM mcp_tools ORDER BY name";
            using var r = cmd.ExecuteReader();
            while (r.Read())
            {
                // One unreadable row is skipped and said so; it does not take the other tools with it.
                ToolPayload? payload;
                try { payload = JsonSerializer.Deserialize(r.GetString(3), McpStoreJson.Default.ToolPayload); }
                catch (JsonException e)
                {
                    _logger.LogError(e, "The published tool {Name} could not be read and is not offered", r.GetString(1));
                    continue;
                }
                if (payload is null) continue;
                tools.Add(new McpPublishedTool
                {
                    Id                   = r.GetString(0),
                    Name                 = r.GetString(1),
                    Published            = r.GetInt32(2) != 0,
                    Description          = payload.Description ?? "",
                    Parameters           = payload.Parameters ?? [],
                    Filter               = payload.Filter ?? "",
                    Fields               = payload.Fields ?? [],
                    DefaultWindowMinutes = payload.DefaultWindowMinutes,
                    MaxEvents            = payload.MaxEvents,
                    OldestFirst          = payload.OldestFirst,
                    Profiles             = payload.Profiles ?? [],
                    CreatedBy            = r.GetString(4),
                    CreatedAt            = DateTimeOffset.Parse(r.GetString(5), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
                    UpdatedAt            = DateTimeOffset.Parse(r.GetString(6), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
                });
            }
        }

        return new McpSnapshot
        {
            Settings  = settings,
            Tools     = tools,
            Profiles  = profiles,
            Rules     = rules,
            Standard  = new MaskingPolicy(McpMaskingMode.Standard, rules, settings.Detectors, _secret, Vault),
            Strict    = new MaskingPolicy(McpMaskingMode.Strict,   rules, settings.Detectors, _secret, Vault),
            ByKeyHash = profiles.ToFrozenDictionary(static p => p.KeyHash, StringComparer.Ordinal),
            ById      = profiles.ToFrozenDictionary(static p => p.Id, StringComparer.Ordinal),
        };
    }

    private static string ListJson(IReadOnlyList<string> values) =>
        JsonSerializer.Serialize([.. values], McpStoreJson.Default.StringArray);

    private static string[] List(string json)
    {
        try { return JsonSerializer.Deserialize(json, McpStoreJson.Default.StringArray) ?? []; }
        catch (JsonException) { return []; }
    }

    // ── SQLite ────────────────────────────────────────────────────────────────

    private SqliteConnection Open()
    {
        var conn = new SqliteConnection(_connStr);
        conn.Open();
        using var cmd = conn.CreateCommand();
        // The file is shared with the auth and alert stores: wait out a writer of theirs rather
        // than fail on it.
        cmd.CommandText = "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;";
        cmd.ExecuteNonQuery();
        return conn;
    }

    private void InitSchema()
    {
        using var conn = Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS mcp_profiles (
                id                 TEXT    PRIMARY KEY,
                name               TEXT    NOT NULL,
                description        TEXT    NOT NULL DEFAULT '',
                key_hash           TEXT    NOT NULL,
                tools_mode         INTEGER NOT NULL DEFAULT 0,
                tools              TEXT    NOT NULL DEFAULT '[]',
                services           TEXT    NOT NULL DEFAULT '[]',
                masking_mode       INTEGER NOT NULL DEFAULT 0,
                free_filters       INTEGER NOT NULL DEFAULT 1,
                max_events         INTEGER NOT NULL DEFAULT 200,
                max_window_minutes INTEGER NOT NULL DEFAULT 1440,
                calls_per_minute   INTEGER NOT NULL DEFAULT 30,
                created_by         TEXT    NOT NULL DEFAULT '',
                created_at         TEXT    NOT NULL,
                last_call_ms       INTEGER NOT NULL DEFAULT 0
            );
            CREATE UNIQUE INDEX IF NOT EXISTS ux_mcp_profiles_key ON mcp_profiles(key_hash);

            CREATE TABLE IF NOT EXISTS mcp_mask_rules (
                id         TEXT    PRIMARY KEY,
                field      TEXT    NOT NULL,
                services   TEXT    NOT NULL DEFAULT '[]',
                action     INTEGER NOT NULL DEFAULT 0,
                created_at TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS mcp_tools (
                id         TEXT    PRIMARY KEY,
                name       TEXT    NOT NULL,
                published  INTEGER NOT NULL DEFAULT 0,
                payload    TEXT    NOT NULL,
                created_by TEXT    NOT NULL DEFAULT '',
                created_at TEXT    NOT NULL,
                updated_at TEXT    NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS ux_mcp_tools_name ON mcp_tools(name);

            CREATE TABLE IF NOT EXISTS mcp_settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """;
        cmd.ExecuteNonQuery();
    }

    private void WriteProfile(McpProfile p) => Execute("""
        INSERT INTO mcp_profiles (id, name, description, key_hash, tools_mode, tools, services, masking_mode, free_filters,
                                  max_events, max_window_minutes, calls_per_minute, created_by, created_at, last_call_ms)
        VALUES (@id, @name, @desc, @hash, @tmode, @tools, @services, @mmode, @free, @events, @window, @calls, @by, @at, @last)
        ON CONFLICT(id) DO UPDATE SET
            name = excluded.name, description = excluded.description, key_hash = excluded.key_hash,
            tools_mode = excluded.tools_mode, tools = excluded.tools, services = excluded.services,
            masking_mode = excluded.masking_mode, free_filters = excluded.free_filters,
            max_events = excluded.max_events, max_window_minutes = excluded.max_window_minutes,
            calls_per_minute = excluded.calls_per_minute
        """,
        ("@id", p.Id), ("@name", p.Name), ("@desc", p.Description), ("@hash", p.KeyHash),
        ("@tmode", (int)p.ToolsMode), ("@tools", ListJson(p.Tools)),
        ("@services", ListJson(p.Services)), ("@mmode", (int)p.MaskingMode),
        ("@free", p.FreeFilters ? 1 : 0), ("@events", p.MaxEvents), ("@window", p.MaxWindowMinutes),
        ("@calls", p.CallsPerMinute), ("@by", p.CreatedBy),
        ("@at", p.CreatedAt.ToString("O", CultureInfo.InvariantCulture)), ("@last", p.LastCallUnixMs));

    private void WriteRule(McpMaskRule rule) => Execute("""
        INSERT INTO mcp_mask_rules (id, field, services, action, created_at)
        VALUES (@id, @field, @services, @action, @at)
        ON CONFLICT(id) DO UPDATE SET field = excluded.field, services = excluded.services, action = excluded.action
        """,
        ("@id", rule.Id), ("@field", rule.Field), ("@services", ListJson(rule.Services)),
        ("@action", (int)rule.Action), ("@at", DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture)));

    private void WriteTool(McpPublishedTool tool) => Execute("""
        INSERT INTO mcp_tools (id, name, published, payload, created_by, created_at, updated_at)
        VALUES (@id, @name, @published, @payload, @by, @at, @updated)
        ON CONFLICT(id) DO UPDATE SET
            name = excluded.name, published = excluded.published, payload = excluded.payload, updated_at = excluded.updated_at
        """,
        ("@id", tool.Id), ("@name", tool.Name), ("@published", tool.Published ? 1 : 0),
        ("@payload", JsonSerializer.Serialize(new ToolPayload
        {
            Description          = tool.Description,
            Parameters           = [.. tool.Parameters],
            Filter               = tool.Filter,
            Fields               = [.. tool.Fields],
            DefaultWindowMinutes = tool.DefaultWindowMinutes,
            MaxEvents            = tool.MaxEvents,
            OldestFirst          = tool.OldestFirst,
            Profiles             = [.. tool.Profiles],
        }, McpStoreJson.Default.ToolPayload)),
        ("@by", tool.CreatedBy),
        ("@at", tool.CreatedAt.ToString("O", CultureInfo.InvariantCulture)),
        ("@updated", tool.UpdatedAt.ToString("O", CultureInfo.InvariantCulture)));

    private void WriteSetting(string key, string value) => Execute(
        "INSERT INTO mcp_settings (key, value) VALUES (@k, @v) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        ("@k", key), ("@v", value));

    private string? ReadSetting(string key)
    {
        using var conn = Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "SELECT value FROM mcp_settings WHERE key = @k";
        cmd.Parameters.AddWithValue("@k", key);
        return cmd.ExecuteScalar() as string;
    }

    /// <summary>The connection and transaction the writes of one change share; see <see cref="Atomically"/>.</summary>
    private (SqliteConnection Connection, SqliteTransaction Transaction)? _batch;

    /// <summary>
    /// Runs several writes as one change: all of them are stored, or none is. A profile and its
    /// grants, the eight rows of the settings — half of either is a state nobody chose. Called
    /// under the gate, which is what makes the one field safe to share.
    /// </summary>
    private void Atomically(Action writes)
    {
        using var conn = Open();
        using var transaction = conn.BeginTransaction();
        _batch = (conn, transaction);
        try
        {
            writes();
            transaction.Commit();
        }
        finally
        {
            _batch = null;
        }
    }

    private int Execute(string sql, params ReadOnlySpan<(string Name, object Value)> parameters)
    {
        if (_batch is { } batch)
        {
            using var inBatch = batch.Connection.CreateCommand();
            inBatch.Transaction = batch.Transaction;
            inBatch.CommandText = sql;
            foreach (var (name, value) in parameters) inBatch.Parameters.AddWithValue(name, value);
            return inBatch.ExecuteNonQuery();
        }

        using var conn = Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var (name, value) in parameters) cmd.Parameters.AddWithValue(name, value);
        return cmd.ExecuteNonQuery();
    }
}

/// <summary>What of a published tool is kept as one JSON column: everything it is not looked up by.</summary>
internal sealed record ToolPayload
{
    public string?             Description          { get; init; }
    public McpToolParameter[]? Parameters           { get; init; }
    public string?             Filter               { get; init; }
    public string[]?           Fields               { get; init; }
    public int                 DefaultWindowMinutes { get; init; } = 60;
    public int                 MaxEvents            { get; init; } = 200;
    public bool                OldestFirst          { get; init; }
    public string[]?           Profiles             { get; init; }
}

[JsonSerializable(typeof(ToolPayload))]
[JsonSerializable(typeof(string[]))]
internal sealed partial class McpStoreJson : JsonSerializerContext;
