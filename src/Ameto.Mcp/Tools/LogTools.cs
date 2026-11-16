using System.Globalization;
using System.Text;
using System.Text.Json;
using Ameto.Core;
using Ameto.Mcp.Masking;
using Ameto.Query;
using Ameto.Query.Filtering;
using Ameto.Storage;

namespace Ameto.Mcp.Tools;

/// <summary>Why a call was refused: what the record of calls sorts its refusals by.</summary>
internal enum RefusalKind : byte
{
    /// <summary>Not a refusal.</summary>
    None = 0,
    /// <summary>The arguments: a filter that does not parse, an unknown parameter, a bad cursor, time or reference.</summary>
    Input,
    /// <summary>A limit: the profile's calls per minute, the AI searches at once, the server's search slots.</summary>
    Limit,
    /// <summary>The masking rules: a hidden field asked about in a way that would spell it out, or a token nobody was given.</summary>
    Masking,
    /// <summary>What the profile may reach: a tool it is not offered, a service or an event it does not see.</summary>
    Scope,
    /// <summary>The log store is not there to be read.</summary>
    Unavailable,
}

/// <summary>What a tool call came to: the text for the AI, and the figures for the audit record.</summary>
internal sealed class ToolResult
{
    public const string Ok = "OK", Trimmed = "Trimmed", RefusedStatus = "Refused", TimedOut = "Timed out", Failed = "Failed";

    public required string Text { get; init; }
    public bool      IsError { get; init; }
    public string    Status  { get; init; } = Ok;
    public int       Events  { get; init; }
    public MaskStats Stats   { get; init; } = new();

    /// <summary>
    /// The filter the call asked with, AS WRITTEN: for a published tool, its template with the
    /// call's arguments bound in. A token in it is still a token — the text the engine ran,
    /// with the value in the token's place, is never kept.
    /// </summary>
    public string? Filter { get; init; }

    /// <summary>Why the call was refused; <see cref="RefusalKind.None"/> for a call that ran.</summary>
    public RefusalKind Refusal { get; init; }

    /// <summary>
    /// A call that was not run, with the sentence that tells the AI what to change — and, when
    /// it is about a filter, the filter as it was written.
    /// </summary>
    public static ToolResult Refused(RefusalKind kind, string why, string? filter = null) =>
        new() { Text = why, IsError = true, Status = RefusedStatus, Refusal = kind, Filter = filter };
}

/// <summary>One tool call: who makes it, under which rules, with which arguments.</summary>
internal sealed class ToolCall
{
    public required McpProfile  Profile  { get; init; }
    public required McpSnapshot Snapshot { get; init; }
    public required IReadOnlyDictionary<string, JsonElement> Args { get; init; }
    public required DateTimeOffset Now { get; init; }

    /// <summary>True once the call's time budget has run out: what it has found so far is a part.</summary>
    public required Func<bool> TimedOut { get; init; }

    public MaskingPolicy Policy    => Snapshot.PolicyFor(Profile);
    public int           MaxEvents => Math.Min(Profile.MaxEvents, Snapshot.Settings.MaxEvents);
    public TimeSpan      MaxWindow => TimeSpan.FromMinutes(Math.Min(Profile.MaxWindowMinutes, Snapshot.Settings.MaxWindowMinutes));

    /// <summary>The answer's size in characters; about three to a token for this kind of text.</summary>
    public int BudgetChars => Snapshot.Settings.ResponseBudgetTokens * 3;

    // A model does not always send the JSON type the schema names: numbers arrive as strings,
    // a one-item list as a bare string. Read what was plainly meant.

    public string? Str(string name)
    {
        if (!Args.TryGetValue(name, out var value)) return null;
        string? text = value.ValueKind switch
        {
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number or JsonValueKind.True or JsonValueKind.False => value.GetRawText(),
            _ => null,
        };
        return string.IsNullOrWhiteSpace(text) ? null : text.Trim();
    }

    public int? Int(string name)
    {
        if (!Args.TryGetValue(name, out var value)) return null;
        if (value.ValueKind == JsonValueKind.Number) return value.TryGetInt32(out int n) ? n : null;
        return value.ValueKind == JsonValueKind.String
            && int.TryParse(value.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed) ? parsed : null;
    }

    public bool? Bool(string name)
    {
        if (!Args.TryGetValue(name, out var value)) return null;
        return value.ValueKind switch
        {
            JsonValueKind.True  => true,
            JsonValueKind.False => false,
            JsonValueKind.String when bool.TryParse(value.GetString(), out bool b) => b,
            _ => null,
        };
    }

    public List<string>? Strings(string name)
    {
        if (!Args.TryGetValue(name, out var value)) return null;
        var list = new List<string>();
        if (value.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in value.EnumerateArray())
                if (item.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(item.GetString())) list.Add(item.GetString()!.Trim());
        }
        else if (value.ValueKind == JsonValueKind.String)
        {
            foreach (string part in value.GetString()!.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                list.Add(part);
        }
        return list.Count == 0 ? null : list;
    }
}

/// <summary>
/// The five built-in log tools. Each reads through the ordinary query executor — the same
/// filters, indexes and tier merge as the Logs page — and differs from the page only in what it
/// does with the result: narrows it to what the profile may see, hides what the rules hide, and
/// writes it in as few characters as carry the meaning.
/// </summary>
internal sealed class LogTools(IQueryExecutor executor, StorageEngine storage, McpStore store)
{
    private const string Stamp = "yyyy-MM-dd HH:mm:ss";

    public Task<ToolResult> RunAsync(string tool, ToolCall call, CancellationToken ct) => tool switch
    {
        BuiltInTools.Overview  => OverviewAsync(call, ct),
        BuiltInTools.Search    => SearchAsync(call, ct),
        BuiltInTools.Aggregate => AggregateAsync(call, ct),
        BuiltInTools.Context   => ContextAsync(call, ct),
        BuiltInTools.Fields    => FieldsAsync(call, ct),
        _                      => Task.FromResult(ToolResult.Refused(RefusalKind.Input, $"There is no tool named '{tool}'.")),
    };

    // ── logs_search ───────────────────────────────────────────────────────────

    /// <summary>A search worked out and ready to run: what to ask the executor, and how to write the answer.</summary>
    private sealed record SearchSpec
    {
        public string?             Filter      { get; init; }

        /// <summary>The filter before tokens became values and the profile's scope was joined: what the audit keeps.</summary>
        public string?             AsWritten   { get; init; }
        public bool                FreeText    { get; init; }
        public string?             OnlyService { get; init; }
        public HashSet<LogLevel>?  Levels      { get; init; }
        public HashSet<string>?    Fields      { get; init; }
        public int                 Limit       { get; init; }
        public bool                OldestFirst { get; init; }
        public long?               AfterTicks  { get; init; }
        public EventId?            AfterId     { get; init; }
        public DateTimeOffset      From        { get; init; }
        public DateTimeOffset      To          { get; init; }
        public string?             WindowNote  { get; init; }
    }

    private async Task<ToolResult> SearchAsync(ToolCall call, CancellationToken ct)
    {
        if (!TimeWindow.TryResolve(call.Str("from"), call.Str("to"), call.Now, call.MaxWindow, TimeWindow.Default,
                out var from, out var to, out string? windowNote, out string? error))
            return ToolResult.Refused(RefusalKind.Input, error!);

        var prepared = FilterGuard.Prepare(call.Str("filter"), call.Policy, store.Vault);
        if (prepared.Error is not null) return ToolResult.Refused(prepared.Kind, prepared.Error, call.Str("filter"));

        string? service = call.Str("service");
        if (service is not null && !FilterGuard.MaySee(call.Profile, service))
            return ToolResult.Refused(RefusalKind.Scope, $"This profile does not see the service '{service}'.");

        HashSet<LogLevel>? levels = null;
        if (call.Strings("levels") is { } names)
        {
            levels = [];
            foreach (string name in names)
            {
                if (!LogLevelExtensions.TryParse(name, out var level))
                    return ToolResult.Refused(RefusalKind.Input, $"'{name}' is not a level. Levels: Verbose, Debug, Information, Warning, Error, Fatal.");
                levels.Add(level);
            }
            if (levels.Count == 6) levels = null;
        }

        if (!TryCursor(call, out long? afterTicks, out EventId? afterId, out var badCursor)) return badCursor!;

        return await RunSearchAsync(new SearchSpec
        {
            // Free text cannot be joined to a predicate, so its rows are narrowed after the fact.
            Filter      = prepared.FreeText ? prepared.Filter
                        : FilterGuard.Scope(FilterGuard.And(prepared.Filter, service is null ? null : "@service = " + FilterGuard.Quote(service)),
                                            freeText: false, call.Profile),
            AsWritten   = call.Str("filter"),
            FreeText    = prepared.FreeText,
            OnlyService = prepared.FreeText ? service : null,
            Levels      = levels,
            Fields      = call.Strings("fields") is { } f ? new HashSet<string>(f, StringComparer.OrdinalIgnoreCase) : null,
            Limit       = Math.Clamp(call.Int("limit") ?? 50, 1, call.MaxEvents),
            OldestFirst = string.Equals(call.Str("order"), "oldest", StringComparison.OrdinalIgnoreCase),
            AfterTicks  = afterTicks,
            AfterId     = afterId,
            From        = from,
            To          = to,
            WindowNote  = windowNote,
        }, call, ct);
    }

    /// <summary>
    /// A published tool: its filter with the call's arguments bound in as literals, then the
    /// same road as <c>logs_search</c> — the same guard on hidden fields, the same scope, the
    /// same masking, the same answer.
    /// </summary>
    public async Task<ToolResult> RunPublishedAsync(McpPublishedTool tool, ToolCall call, CancellationToken ct)
    {
        if (!TimeWindow.TryResolve(call.Str("from"), call.Str("to"), call.Now, call.MaxWindow,
                TimeSpan.FromMinutes(tool.DefaultWindowMinutes), out var from, out var to, out string? windowNote, out string? error))
            return ToolResult.Refused(RefusalKind.Input, error!);

        var (bound, bindError) = PublishedToolBinder.Bind(tool, call.Args);
        if (bindError is not null) return ToolResult.Refused(RefusalKind.Input, bindError);

        var prepared = FilterGuard.Prepare(bound, call.Policy, store.Vault);
        if (prepared.Error is not null) return ToolResult.Refused(prepared.Kind, prepared.Error, bound);

        if (!TryCursor(call, out long? afterTicks, out EventId? afterId, out var badCursor)) return badCursor!;

        return await RunSearchAsync(new SearchSpec
        {
            Filter      = prepared.FreeText ? prepared.Filter : FilterGuard.Scope(prepared.Filter, freeText: false, call.Profile),
            FreeText    = prepared.FreeText,
            AsWritten   = bound,
            Fields      = tool.Fields.Count == 0 ? null : new HashSet<string>(tool.Fields, StringComparer.OrdinalIgnoreCase),
            Limit       = Math.Clamp(tool.MaxEvents, 1, call.MaxEvents),
            OldestFirst = tool.OldestFirst,
            AfterTicks  = afterTicks,
            AfterId     = afterId,
            From        = from,
            To          = to,
            WindowNote  = windowNote,
        }, call, ct);
    }

    private static bool TryCursor(ToolCall call, out long? afterTicks, out EventId? afterId, out ToolResult? refusal)
    {
        afterTicks = null;
        afterId    = null;
        refusal    = null;
        if (call.Str("cursor") is not { } raw) return true;
        if (!SearchCursor.TryParse(raw, out long ticks, out ulong id))
        {
            refusal = ToolResult.Refused(RefusalKind.Input, "'cursor' is not a cursor from an earlier answer. Pass it exactly as the answer gave it, or leave it out to start again.");
            return false;
        }
        afterTicks = ticks;
        afterId    = new EventId(id);
        return true;
    }

    private async Task<ToolResult> RunSearchAsync(SearchSpec spec, ToolCall call, CancellationToken ct)
    {
        var policy = call.Policy;
        var page = await FetchAsync(spec.Filter, spec.From, spec.To, spec.Levels,
                                    spec.OldestFirst ? QueryDirection.Forward : QueryDirection.Backward,
                                    spec.AfterTicks, spec.AfterId, call.Profile, spec.OnlyService, spec.Limit, ct);

        // Events go into the answer in the order the query returned them, until the budget is
        // spent. Each is masked as it is taken, so nothing is counted that was not sent.
        var stats   = new MaskStats();
        var shown   = new List<MaskedEvent>(page.Events.Count);
        var seen    = new HashSet<string>(StringComparer.Ordinal);
        int used    = EventTable.HeaderCost;
        bool trimmed = false;
        foreach (var ev in page.Events)
        {
            var own    = new MaskStats();
            var masked = policy.Mask(ev, own, spec.Fields);
            bool known = seen.Contains(masked.Template);
            int cost   = EventTable.CostOf(masked, seen);
            if (shown.Count > 0 && used + cost > call.BudgetChars)
            {
                if (!known) seen.Remove(masked.Template);
                trimmed = true;
                break;
            }
            used += cost;
            shown.Add(masked);
            stats.Merge(own);
        }

        bool timedOut = call.TimedOut();
        var sb = new StringBuilder(used + 256);
        sb.Append(shown.Count).Append(shown.Count == 1 ? " event" : " events");
        if (shown.Count > 0) sb.Append(" · ").Append(seen.Count).Append(seen.Count == 1 ? " template" : " templates");
        sb.Append(" · ").Append(spec.From.UtcDateTime.ToString(Stamp, CultureInfo.InvariantCulture))
          .Append(" … ").Append(spec.To.UtcDateTime.ToString(Stamp, CultureInfo.InvariantCulture)).Append(" UTC")
          .Append(spec.OldestFirst ? " · oldest first\n" : " · newest first\n");
        if (spec.WindowNote is not null) sb.Append("note: ").Append(spec.WindowNote).Append('\n');
        if (timedOut) sb.Append("note: the search ran out of its time budget, so this is a part of the result. Narrow the window or the filter.\n");

        EventTable.Write(sb, shown);

        // Where to go on from: the last event shown, or — when a whole page of events was read
        // and none of them was this profile's to see — the last one read.
        (long Ticks, ulong Id)? resume = trimmed || (page.More && shown.Count > 0)
            ? (shown[^1].Source.Timestamp.UtcTicks, shown[^1].Source.Id.RawValue)
            : page.More ? page.Resume : null;

        sb.Append('\n');
        if (resume is { } next)
            sb.Append(trimmed ? "more: the answer reached its size budget. " : "more: ")
              .Append("call again with cursor=").Append(SearchCursor.Format(next.Ticks, next.Id)).Append('\n');
        else if (shown.Count == 0)
            sb.Append("No events match in this window.\n");
        else
            sb.Append("end of results\n");

        return new ToolResult
        {
            Text   = sb.ToString(),
            Status = timedOut ? ToolResult.TimedOut : trimmed ? ToolResult.Trimmed : ToolResult.Ok,
            Events = shown.Count,
            Stats  = stats,
            Filter = spec.AsWritten,
        };
    }

    // ── logs_aggregate ────────────────────────────────────────────────────────

    private async Task<ToolResult> AggregateAsync(ToolCall call, CancellationToken ct)
    {
        if (!TimeWindow.TryResolve(call.Str("from"), call.Str("to"), call.Now, call.MaxWindow, TimeWindow.Default,
                out var from, out var to, out string? windowNote, out string? error))
            return ToolResult.Refused(RefusalKind.Input, error!);

        var policy = call.Policy;
        var (query, refusal, kind) = FilterGuard.PrepareAggregation(call.Str("query"), policy, store.Vault);
        if (query is null) return ToolResult.Refused(kind, refusal!);

        var (result, scopeError) = await RunAggregationAsync(query, call.Profile, from, to, ct);
        if (result is null) return ToolResult.Refused(RefusalKind.Input, scopeError!);

        var stats  = new MaskStats();
        var header = new List<string>(result.KeyColumns.Count + result.ValueColumns.Count);
        header.AddRange(result.KeyColumns);
        header.AddRange(result.ValueColumns);

        var rows = new List<string?[]>(result.Rows.Count);
        foreach (var row in result.Rows)
        {
            var cells = new string?[header.Count];
            for (int k = 0; k < row.Key.Length; k++)
                cells[k] = policy.MaskGroupKey(KeyPath(query.Keys[k].Property), row.Key[k], stats) is { } key ? EventTable.Cell(key) : null;
            for (int v = 0; v < row.Values.Length; v++)
                cells[row.Key.Length + v] = Number(row.Values[v]);
            rows.Add(cells);
        }

        var sb = new StringBuilder();
        sb.Append(from.UtcDateTime.ToString(Stamp, CultureInfo.InvariantCulture)).Append(" … ")
          .Append(to.UtcDateTime.ToString(Stamp, CultureInfo.InvariantCulture)).Append(" UTC · ")
          .Append(result.Scanned).Append(" events read · ").Append(result.GroupsFound).Append(result.GroupsFound == 1 ? " group\n" : " groups\n");
        if (windowNote is not null) sb.Append("note: ").Append(windowNote).Append('\n');
        if (result.Partial)
            sb.Append("note: PARTIAL — ").Append(result.PartialReason ?? "the scan stopped early").Append(". The numbers are floors, not totals.\n");
        if (result.GroupsFound > result.Rows.Count)
            sb.Append("note: ").Append(result.Rows.Count).Append(" of ").Append(result.GroupsFound).Append(" groups shown, largest first.\n");
        sb.Append('\n');
        EventTable.WriteGrid(sb, header, rows);

        return new ToolResult { Text = sb.ToString(), Events = 0, Stats = stats };
    }

    /// <summary>Runs an aggregation narrowed to what the profile may see.</summary>
    private async Task<(AggregationResult? Result, string? Error)> RunAggregationAsync(
        AggregationQuery query, McpProfile profile, DateTimeOffset from, DateTimeOffset to, CancellationToken ct)
    {
        if (!string.IsNullOrWhiteSpace(query.FilterText) && FilterParser.Parse(query.FilterText) is FreeTextNode)
            return (null, "The where-clause is plain words. In an aggregation write it as an expression, for example ci_contains(@mt, 'timeout').");

        var scoped = new AggregationQuery
        {
            Aggregates = query.Aggregates,
            Keys       = query.Keys,
            FilterText = FilterGuard.Scope(query.FilterText, freeText: false, profile),
            Limit      = Math.Clamp(query.Limit, 1, 200),
        };
        var result = await new AggregationExecutor(executor, headerScan: storage).ExecuteAsync(scoped, from, to, ct);
        return (result, null);
    }

    /// <summary>The path a group key is judged by: a property path as a rule spells it, or a built-in's name.</summary>
    private static string KeyPath(string encoded)
    {
        if (!BuiltinFields.TryResolve(encoded, out var field))
            return PropertyPath.ToDisplay(PropertyPath.StripIndexSegments(encoded));
        return field switch
        {
            BuiltinField.MessageTemplate => "@mt",
            BuiltinField.ExceptionMessage or BuiltinField.ExceptionStack or BuiltinField.InnerExceptionMessage => "@x.message",
            _ => "@built-in",
        };
    }

    private static string? Number(double? value) => value is not { } v ? null
        : v == Math.Floor(v) && Math.Abs(v) < 1e15 ? ((long)v).ToString(CultureInfo.InvariantCulture)
        : v.ToString("0.###", CultureInfo.InvariantCulture);

    // ── logs_overview ─────────────────────────────────────────────────────────

    private async Task<ToolResult> OverviewAsync(ToolCall call, CancellationToken ct)
    {
        if (!TimeWindow.TryResolve(call.Str("from"), call.Str("to"), call.Now, call.MaxWindow, TimeWindow.Default,
                out var from, out var to, out string? windowNote, out string? error))
            return ToolResult.Refused(RefusalKind.Input, error!);

        string? service = call.Str("service");
        if (service is not null && !FilterGuard.MaySee(call.Profile, service))
            return ToolResult.Refused(RefusalKind.Scope, $"This profile does not see the service '{service}'.");

        string where   = service is null ? "" : " where @service = " + FilterGuard.Quote(service);
        string andSvc  = service is null ? "" : " and @service = " + FilterGuard.Quote(service);
        var policy     = call.Policy;
        var stats      = new MaskStats();
        var notes      = new List<string>();
        if (windowNote is not null) notes.Add(windowNote);

        async Task<AggregationResult?> Ask(string text)
        {
            if (ct.IsCancellationRequested) return null;
            try
            {
                var (result, _) = await RunAggregationAsync(AggregationParser.Parse(text), call.Profile, from, to, ct);
                if (result is { Partial: true } && notes.TrueForAll(static n => !n.StartsWith("PARTIAL", StringComparison.Ordinal)))
                    notes.Add($"PARTIAL — {result.PartialReason ?? "a scan stopped early"}. The numbers are floors, not totals.");
                return result;
            }
            catch (OperationCanceledException) { return null; }
        }

        var byServiceLevel = await Ask($"select count(*){where} group by @service, @l limit 200");
        var templates      = await Ask($"select count(*){where} group by @mt, @l limit 12");
        var exceptions     = await Ask($"select count(*) where has(@x){andSvc} group by @x.type limit 10");

        var sb = new StringBuilder();
        long total = 0;
        var levels   = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        var services = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        if (byServiceLevel is not null)
            foreach (var row in byServiceLevel.Rows)
            {
                long count = (long)(row.Values[0] ?? 0);
                total += count;
                string svc = row.Key[0] ?? "(no service)", level = row.Key[1] ?? "Information";
                services[svc] = services.GetValueOrDefault(svc) + count;
                levels[level] = levels.GetValueOrDefault(level) + count;
            }

        sb.Append("logs ").Append(from.UtcDateTime.ToString(Stamp, CultureInfo.InvariantCulture)).Append(" … ")
          .Append(to.UtcDateTime.ToString(Stamp, CultureInfo.InvariantCulture)).Append(" UTC (")
          .Append(TimeWindow.Describe(to - from)).Append(") · ").Append(total).Append(" events");
        if (service is not null) sb.Append(" · ").Append(service);
        sb.Append('\n');
        foreach (string note in notes) sb.Append("note: ").Append(note).Append('\n');
        if (call.TimedOut()) sb.Append("note: the overview ran out of its time budget; what is missing below was not counted. Narrow the window.\n");

        if (total > 0)
        {
            sb.Append("levels    ");
            bool first = true;
            foreach (string level in (ReadOnlySpan<string>)["Fatal", "Error", "Warning", "Information", "Debug", "Verbose"])
            {
                if (!levels.TryGetValue(level, out long n) || n == 0) continue;
                if (!first) sb.Append(" · ");
                sb.Append(level).Append(' ').Append(n);
                first = false;
            }
            sb.Append("\nservices  ");
            var ordered = services.OrderByDescending(static s => s.Value).ToList();
            for (int i = 0; i < ordered.Count && i < 12; i++)
            {
                if (i > 0) sb.Append(" · ");
                sb.Append(ordered[i].Key).Append(' ').Append(ordered[i].Value);
            }
            if (ordered.Count > 12) sb.Append(" · +").Append(ordered.Count - 12).Append(" more");
            sb.Append('\n');
        }

        if (templates is { Rows.Count: > 0 })
        {
            var rows = new List<string?[]>(templates.Rows.Count);
            foreach (var row in templates.Rows)
                rows.Add([Number(row.Values[0]), row.Key[1], policy.MaskGroupKey("@mt", row.Key[0], stats) is { } t ? Truncate(t, 200) : null]);
            sb.Append("\nmost frequent message templates\n");
            EventTable.WriteGrid(sb, ["count", "level", "template"], rows);
        }

        if (exceptions is { Rows.Count: > 0 })
        {
            var rows = new List<string?[]>(exceptions.Rows.Count);
            foreach (var row in exceptions.Rows) rows.Add([Number(row.Values[0]), row.Key[0]]);
            sb.Append("\nexception types\n");
            EventTable.WriteGrid(sb, ["count", "type"], rows);
        }

        if (total == 0 && byServiceLevel is not null) sb.Append("No events in this window.\n");

        return new ToolResult
        {
            Text   = sb.ToString(),
            Status = call.TimedOut() ? ToolResult.TimedOut : ToolResult.Ok,
            Stats  = stats,
        };
    }

    // ── logs_context ──────────────────────────────────────────────────────────

    private async Task<ToolResult> ContextAsync(ToolCall call, CancellationToken ct)
    {
        if (call.Str("ref") is not { } raw || !EventRef.TryParse(raw, out ulong id, out long msTicks))
            return ToolResult.Refused(RefusalKind.Input, "'ref' is not an event reference. Pass the ref of a logs_search row exactly as the row gave it.");

        var profile = call.Profile;
        var policy  = call.Policy;

        // The reference names the millisecond the event's own timestamp falls in, so the event
        // is found by reading that one millisecond — no scan, whatever the size of the store.
        var at = new DateTimeOffset(msTicks, TimeSpan.Zero);
        LogEvent? target = null;
        await foreach (var ev in executor.ExecuteAsync(new QueryRequest
        {
            FromUtc   = at,
            ToUtc     = at.AddTicks(TimeSpan.TicksPerMillisecond - 1),
            Count     = 10_000,
            Direction = QueryDirection.Forward,
        }, ct).ConfigureAwait(false))
        {
            if (ev.Id.RawValue != id) continue;
            target = ev;
            break;
        }
        if (target is null || !FilterGuard.InScope(profile, target))
            return ToolResult.Refused(RefusalKind.Scope, "There is no event with this ref among the events this profile sees. A ref comes from a logs_search row; the event may also have passed its retention.");

        var stats = new MaskStats();
        var sb    = new StringBuilder(2_048);
        EventTable.WriteDetail(sb, policy.Mask(target, stats), maxStackChars: Math.Max(2_000, call.BudgetChars / 3));
        int events = 1;

        int before = Math.Clamp(call.Int("before") ?? 5, 0, 50);
        int after  = Math.Clamp(call.Int("after")  ?? 5, 0, 50);
        long ticks = target.Timestamp.UtcTicks;
        if (before + after > 0)
        {
            string? sameService = FilterGuard.Scope(
                target.ServiceName is { } s ? "@service = " + FilterGuard.Quote(s) : null, freeText: false, profile);
            var hour  = TimeSpan.FromHours(1);
            var older = before == 0 ? null : await FetchAsync(sameService, target.Timestamp - hour, target.Timestamp, null,
                QueryDirection.Backward, ticks, target.Id, profile, null, before, ct);
            var newer = after == 0 ? null : await FetchAsync(sameService, target.Timestamp, target.Timestamp + hour, null,
                QueryDirection.Forward, ticks, target.Id, profile, null, after, ct);

            var timeline = new List<MaskedEvent>(before + after + 1);
            if (older is not null)
                for (int i = older.Events.Count - 1; i >= 0; i--) timeline.Add(policy.Mask(older.Events[i], stats));
            timeline.Add(policy.Mask(target, new MaskStats()));   // counted once, above
            if (newer is not null)
                foreach (var ev in newer.Events) timeline.Add(policy.Mask(ev, stats));

            sb.Append("\nbefore and after in ").Append(target.ServiceName ?? "the events this profile sees")
              .Append(" (oldest first; > marks this event)\n");
            EventTable.WriteTimeline(sb, timeline, id, withService: target.ServiceName is null);
            events = timeline.Count;
        }

        if ((call.Bool("trace") ?? true) && TraceIdHelper.FormatTraceId(target.TraceIdHi, target.TraceIdLo) is { } traceId)
        {
            var half  = TimeSpan.FromMinutes(30);
            var trace = await FetchAsync(FilterGuard.Scope($"@tr = '{traceId}'", freeText: false, profile),
                target.Timestamp - half, target.Timestamp + half, null, QueryDirection.Forward, null, null, profile, null, 60, ct);
            if (trace.Events.Count > 1)
            {
                var masked = trace.Events.ConvertAll(ev => policy.Mask(ev, ev.Id.RawValue == id ? new MaskStats() : stats));
                sb.Append("\nsame trace ").Append(traceId).Append(" (oldest first)\n");
                EventTable.WriteTimeline(sb, masked, id, withService: true);
                if (trace.More) sb.Append("… the trace has more events\n");
                events += masked.Count - 1;
            }
        }

        if (call.TimedOut()) sb.Append("\nnote: the call ran out of its time budget; the surroundings above may be incomplete.\n");
        return new ToolResult
        {
            Text   = sb.ToString(),
            Status = call.TimedOut() ? ToolResult.TimedOut : ToolResult.Ok,
            Events = events,
            Stats  = stats,
        };
    }

    // ── logs_fields ───────────────────────────────────────────────────────────

    private sealed class FieldInfo
    {
        public int Count;
        public readonly SortedSet<string> Types = new(StringComparer.Ordinal);
        public readonly List<string> Examples = new(3);
    }

    private async Task<ToolResult> FieldsAsync(ToolCall call, CancellationToken ct)
    {
        if (!TimeWindow.TryResolve(call.Str("from"), call.Str("to"), call.Now, call.MaxWindow, TimeWindow.Default,
                out var from, out var to, out string? windowNote, out string? error))
            return ToolResult.Refused(RefusalKind.Input, error!);

        string? service = call.Str("service");
        if (service is not null && !FilterGuard.MaySee(call.Profile, service))
            return ToolResult.Refused(RefusalKind.Scope, $"This profile does not see the service '{service}'.");

        var policy = call.Policy;
        var page = await FetchAsync(
            FilterGuard.Scope(service is null ? null : "@service = " + FilterGuard.Quote(service), freeText: false, call.Profile),
            from, to, null, QueryDirection.Backward, null, null, call.Profile, null, want: 1_000, ct);

        var stats    = new MaskStats();
        var fields   = new Dictionary<string, FieldInfo>(StringComparer.Ordinal);
        var services = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var ev in page.Events)
        {
            string svc = ev.ServiceName ?? "(no service)";
            services[svc] = services.GetValueOrDefault(svc) + 1;

            var own    = new MaskStats();
            var masked = policy.Mask(ev, own);
            var raw    = ev.Properties;
            bool gaveExample = false;
            foreach (var (key, cell) in masked.Properties)
            {
                if (!fields.TryGetValue(key, out var info)) fields[key] = info = new FieldInfo();
                info.Count++;
                info.Types.Add(TypeName(raw is not null && raw.TryGetValue(key, out object? value) ? value : null));
                if (info.Examples.Count >= 3) continue;
                string example = Truncate(EventTable.Cell(cell), 36);
                if (info.Examples.Contains(example)) continue;
                info.Examples.Add(example);
                gaveExample = true;
            }
            if (gaveExample) stats.Merge(own);
        }

        var sb = new StringBuilder();
        sb.Append("properties in ").Append(page.Events.Count).Append(page.More ? " most recent events" : " events");
        if (service is not null) sb.Append(" of ").Append(service);
        sb.Append(" · ").Append(from.UtcDateTime.ToString(Stamp, CultureInfo.InvariantCulture)).Append(" … ")
          .Append(to.UtcDateTime.ToString(Stamp, CultureInfo.InvariantCulture)).Append(" UTC\n");
        if (windowNote is not null) sb.Append("note: ").Append(windowNote).Append('\n');

        if (fields.Count == 0)
            sb.Append("No properties: there are no events in this window.\n");
        else
        {
            var ordered = fields.OrderByDescending(static f => f.Value.Count).ThenBy(static f => f.Key, StringComparer.Ordinal).ToList();
            var rows = new List<string?[]>(Math.Min(ordered.Count, 60));
            foreach (var (name, info) in ordered.Take(60))
                rows.Add([
                    name,
                    info.Count.ToString(CultureInfo.InvariantCulture),
                    string.Join('|', info.Types),
                    policy.HidesField(name) ? "= only" : "",
                    string.Join(", ", info.Examples),
                ]);
            sb.Append('\n');
            EventTable.WriteGrid(sb, ["property", "events", "type", "filter", "examples"], rows);
            if (ordered.Count > 60) sb.Append("… ").Append(ordered.Count - 60).Append(" rarer properties not listed\n");

            sb.Append("\nservices in these events: ");
            sb.AppendJoin(" · ", services.OrderByDescending(static s => s.Value).Select(static s => $"{s.Key} {s.Value}"));
            sb.Append('\n');
        }

        sb.Append('\n').Append(BuiltInTools.FilterReference);
        if (sb[^1] != '\n') sb.Append('\n');

        return new ToolResult
        {
            Text   = sb.ToString(),
            Status = call.TimedOut() ? ToolResult.TimedOut : ToolResult.Ok,
            Stats  = stats,
        };
    }

    private static string TypeName(object? value) => value switch
    {
        null   => "null",
        string => "string",
        bool   => "bool",
        sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal => "number",
        System.Collections.IDictionary => "object",
        System.Collections.IEnumerable => "array",
        _ => "value",
    };

    private static string Truncate(string text, int max) =>
        text.Length <= max ? text : string.Concat(text.AsSpan(0, max), "…");

    // ── Reading events ────────────────────────────────────────────────────────

    /// <summary>
    /// A page of events: the ones found, whether there are more, and — when a whole page was
    /// read without filling the request — the last event read, to go on from.
    /// </summary>
    private sealed record Page(List<LogEvent> Events, bool More, (long Ticks, ulong Id)? Resume);

    /// <summary>
    /// Reads up to <paramref name="want"/> events the profile may see. The executor is asked for
    /// more than that, because some of what it returns may not be the profile's — its services
    /// are joined to the filter where a filter can carry them, but free text cannot, and the
    /// audit trail is dropped here in every case.
    /// </summary>
    private async Task<Page> FetchAsync(
        string? filter, DateTimeOffset from, DateTimeOffset to, HashSet<LogLevel>? levels, QueryDirection direction,
        long? afterTicks, EventId? afterId, McpProfile profile, string? onlyService, int want, CancellationToken ct)
    {
        int ask = (int)Math.Min(10_000, (long)want * 2 + 64);
        var request = new QueryRequest
        {
            Filter              = filter,
            FromUtc             = from,
            ToUtc               = to,
            Count               = ask,
            Direction           = direction,
            AfterTimestampTicks = afterTicks,
            AfterEventId        = afterId,
            Levels              = levels,
        };

        var events = new List<LogEvent>(Math.Min(want, 256));
        int read = 0;
        bool more = false;
        LogEvent? last = null;
        try
        {
            await foreach (var ev in executor.ExecuteAsync(request, ct).ConfigureAwait(false))
            {
                if (events.Count == want) { more = true; break; }   // one past what was wanted
                read++;
                last = ev;
                if (!FilterGuard.InScope(profile, ev)) continue;
                if (onlyService is not null && !string.Equals(ev.ServiceName, onlyService, StringComparison.OrdinalIgnoreCase)) continue;
                events.Add(ev);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // The budget: what was found before it ran out is the answer, and is said to be a part.
        }

        (long, ulong)? resume = null;
        if (!more && read >= ask && last is not null)
        {
            more   = true;
            resume = (last.Timestamp.UtcTicks, last.Id.RawValue);
        }
        return new Page(events, more, resume);
    }
}
