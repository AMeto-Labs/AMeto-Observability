namespace Ameto.Mcp;

/// <summary>Which tools a profile is offered at all.</summary>
public enum McpToolsMode
{
    /// <summary>The built-in log tools and the published ones.</summary>
    BuiltInAndPublished = 0,
    /// <summary>Published tools only: the AI gets no tool that takes a filter of its own.</summary>
    PublishedOnly = 1,
}

/// <summary>What happens to a value no field rule names.</summary>
public enum McpMaskingMode
{
    /// <summary>It goes out as stored, after the value detectors have looked through it.</summary>
    Standard = 0,
    /// <summary>It is hashed. Only fields with a <see cref="MaskAction.Keep"/> rule go out as stored.</summary>
    Strict = 1,
}

/// <summary>What a field rule does to the value of a field it names.</summary>
public enum MaskAction
{
    /// <summary>Replace the value with its token (<c>h:</c> + 12 hex digits).</summary>
    Hash = 0,
    /// <summary>Leave the property out of the answer.</summary>
    Remove = 1,
    /// <summary>Show the last four characters, hide the rest.</summary>
    KeepLast4 = 2,
    /// <summary>Send the value as stored, detectors included: the allow-list of <see cref="McpMaskingMode.Strict"/>.</summary>
    Keep = 3,
}

/// <summary>The value detectors, looked for in every string that no field rule has claimed.</summary>
[Flags]
public enum MaskDetectors
{
    None       = 0,
    CardNumber = 1,
    Email      = 2,
    Phone      = 4,
    /// <summary>Kazakhstan IIN / BIN: twelve digits with a check digit.</summary>
    Iin        = 8,
    /// <summary>JWTs and <c>Bearer …</c> credentials.</summary>
    Secret     = 16,
    Iban       = 32,

    Default = CardNumber | Email | Phone | Iin | Secret,
    All     = Default | Iban,
}

/// <summary>
/// An AI profile: a key and what that key may do. The key itself is never stored — only the
/// SHA-256 of it, as for the ingest keys — and it is accepted on <c>/mcp</c> alone.
/// </summary>
public sealed record McpProfile
{
    public required string Id          { get; init; }
    public required string Name        { get; init; }
    public string          Description { get; init; } = "";

    /// <summary>Lowercase hex of the key's SHA-256.</summary>
    public required string KeyHash     { get; init; }

    public McpToolsMode ToolsMode { get; init; }

    /// <summary>The tools switched on for this profile, by name. A tool not listed is not offered.</summary>
    public IReadOnlyList<string> Tools { get; init; } = [];

    /// <summary>The services whose events the profile sees. Empty: every service.</summary>
    public IReadOnlyList<string> Services { get; init; } = [];

    public McpMaskingMode MaskingMode { get; init; }

    /// <summary>
    /// False withdraws the tools that take a filter or a query written by the AI
    /// (<c>logs_search</c>, <c>logs_aggregate</c>): a filter is a way to ask about a hidden value.
    /// </summary>
    public bool FreeFilters { get; init; } = true;

    public int MaxEvents        { get; init; } = 200;
    public int MaxWindowMinutes { get; init; } = 24 * 60;
    public int CallsPerMinute   { get; init; } = 30;

    public string         CreatedBy { get; init; } = "";
    public DateTimeOffset CreatedAt { get; init; }

    /// <summary>Unix milliseconds of the profile's last call; 0 when it has made none.</summary>
    public long LastCallUnixMs { get; init; }

    public bool SeesService(string? service) =>
        Services.Count == 0 || (service is not null && Services.Contains(service, StringComparer.OrdinalIgnoreCase));
}

/// <summary>What an admin sets on a profile; everything else on it is the server's.</summary>
public sealed record McpProfileInput
{
    public string                 Name             { get; init; } = "";
    public string?                Description      { get; init; }
    public McpToolsMode           ToolsMode        { get; init; }
    public IReadOnlyList<string>? Tools            { get; init; }
    public IReadOnlyList<string>? Services         { get; init; }
    public McpMaskingMode         MaskingMode      { get; init; }
    public bool                   FreeFilters      { get; init; } = true;
    public int?                   MaxEvents        { get; init; }
    public int?                   MaxWindowMinutes { get; init; }
    public int?                   CallsPerMinute   { get; init; }

    /// <summary>
    /// The published tools the profile is granted, by id — the whole list, so that a tool left
    /// out of it is withdrawn. Null leaves the grants as they are. A grant is kept on the tool
    /// (<c>McpPublishedTool.Profiles</c>); this is the same fact, set from the profile's side.
    /// </summary>
    public IReadOnlyList<string>? PublishedTools { get; init; }
}

/// <summary>
/// A field rule. <see cref="Field"/> is a property path as a filter spells it
/// (<c>Customer.Iin</c>), with <c>*</c> for any run of characters. A pattern without a dot is
/// also tried against the last segment alone, so <c>CardNumber</c> hides the field at any depth.
/// </summary>
public sealed record McpMaskRule
{
    public required string Id    { get; init; }
    public required string Field { get; init; }

    /// <summary>The services the rule applies to. Empty: every service.</summary>
    public IReadOnlyList<string> Services { get; init; } = [];

    public MaskAction Action { get; init; }
}

/// <summary>The section's own settings: the switch, the detectors, and the ceilings no profile can raise.</summary>
public sealed record McpSettings
{
    public bool          Enabled              { get; init; } = true;
    public MaskDetectors Detectors            { get; init; } = MaskDetectors.Default;
    public int           ResponseBudgetTokens { get; init; } = 8_000;
    public int           MaxWindowMinutes     { get; init; } = 7 * 24 * 60;
    public int           MaxEvents            { get; init; } = 500;
    public int           CallsPerMinute       { get; init; } = 60;

    /// <summary>AI searches that may run at once, out of the server's search slots.</summary>
    public int MaxConcurrent { get; init; } = 2;

    /// <summary>Built-in tools switched off for every profile, by name.</summary>
    public IReadOnlyList<string> DisabledTools { get; init; } = [];

    /// <summary>The floors and ceilings a stored or posted value is held to.</summary>
    public McpSettings Clamped() => this with
    {
        ResponseBudgetTokens = Math.Clamp(ResponseBudgetTokens, 1_000, 24_000),
        MaxWindowMinutes     = Math.Clamp(MaxWindowMinutes, 5, 366 * 24 * 60),
        MaxEvents            = Math.Clamp(MaxEvents, 10, 5_000),
        CallsPerMinute       = Math.Clamp(CallsPerMinute, 1, 6_000),
        MaxConcurrent        = Math.Clamp(MaxConcurrent, 1, 16),
        Detectors            = Detectors & MaskDetectors.All,
    };
}
