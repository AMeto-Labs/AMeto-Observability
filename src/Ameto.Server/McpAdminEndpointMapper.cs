using System.Buffers;
using System.Globalization;
using System.Security.Claims;
using System.Text.Json;
using Ameto.Core;
using Ameto.Mcp;
using Ameto.Mcp.Masking;
using Ameto.Mcp.Tools;
using Ameto.Query.Filtering;
using Ameto.Server.Auth;
using Ameto.Storage;

namespace Ameto.Server;

/// <summary>
/// The AI endpoint's search slots are the server's own: an AI call waits in the same queue, for
/// the same slots, under the same time budget as a search from the Logs page.
/// </summary>
internal sealed class QueryGuardSearchGate(QueryGuard guard) : ISearchGate
{
    public TimeSpan Budget => guard.Timeout;

    public async ValueTask<IDisposable?> TryEnterAsync(CancellationToken ct)
    {
        QueryGuard.Lease? lease;
        QueryGuard.Claim? claim = null;
        try { lease = await guard.TryEnterAsync(ct, out claim); }
        catch
        {
            claim?.Abandon();   // a wait whose caller's await threw owes the guard its slot (see /api/events)
            throw;
        }
        return lease is { } taken ? new Slot(taken) : null;
    }

    private sealed class Slot(QueryGuard.Lease lease) : IDisposable
    {
        private int _released;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0) lease.Dispose();
        }
    }
}

/// <summary>
/// The settings API behind Settings → AI / MCP: the section's switch and limits, AI profiles and
/// their keys, the masking rules with a preview of what they do to an event, and the record of
/// the AI's calls. Reading is for managers; every change is an admin's, because each of them
/// widens or narrows what leaves the server.
/// </summary>
internal static class McpAdminEndpointMapper
{
    /// <summary>The detectors as Settings lists them; the id is the one a call's masking breakdown counts under.</summary>
    private static readonly (MaskDetectors Flag, string Id, string Name)[] Detectors =
    [
        Detector(MaskDetectors.CardNumber, "Card number"),
        Detector(MaskDetectors.Email,      "Email address"),
        Detector(MaskDetectors.Phone,      "Phone number"),
        Detector(MaskDetectors.Iin,        "IIN / BIN"),
        Detector(MaskDetectors.Secret,     "Bearer and JWT tokens"),
        Detector(MaskDetectors.Iban,       "IBAN"),
    ];

    private static (MaskDetectors Flag, string Id, string Name) Detector(MaskDetectors flag, string name) =>
        (flag, SensitiveValueDetectors.IdOf(flag), name);

    /// <summary>The most calls one summary reads; past it the figures are of the newest calls only.</summary>
    private const int SummaryCeiling = 50_000;

    public static void MapMcpAdminEndpoints(this WebApplication app)
    {
        var read  = app.MapGroup("/api/mcp").RequireAuthorization(AuthServiceExtensions.PolicyManager);
        var write = app.MapGroup("/api/mcp").RequireAuthorization(AuthServiceExtensions.PolicyAdmin);

        // ── The section ───────────────────────────────────────────────────────
        read.MapGet("", (McpStore store) => Results.Ok(Describe(store)));

        write.MapPut("/settings", (McpSettingsRequest body, McpStore store) =>
        {
            var current = store.Current.Settings;
            var detectors = current.Detectors;
            if (body.Detectors is { } ids)
            {
                detectors = MaskDetectors.None;
                foreach (string id in ids)
                {
                    int index = Array.FindIndex(Detectors, d => d.Id.Equals(id, StringComparison.OrdinalIgnoreCase));
                    if (index < 0) return Results.BadRequest(new { error = $"'{id}' is not a detector.", field = "detectors" });
                    detectors |= Detectors[index].Flag;
                }
            }
            if (body.DisabledTools is { } off)
                foreach (string name in off)
                    if (BuiltInTools.Find(name) is null)
                        return Results.BadRequest(new { error = $"'{name}' is not a built-in tool.", field = "disabledTools" });

            store.UpdateSettings(new McpSettings
            {
                DisabledTools        = body.DisabledTools        ?? current.DisabledTools,
                Enabled              = body.Enabled              ?? current.Enabled,
                Detectors            = detectors,
                ResponseBudgetTokens = body.ResponseBudgetTokens ?? current.ResponseBudgetTokens,
                MaxWindowMinutes     = body.MaxWindowMinutes     ?? current.MaxWindowMinutes,
                MaxEvents            = body.MaxEvents            ?? current.MaxEvents,
                CallsPerMinute       = body.CallsPerMinute       ?? current.CallsPerMinute,
                MaxConcurrent        = body.MaxConcurrent        ?? current.MaxConcurrent,
            });
            return Results.Ok(Describe(store));
        });

        // The last day at a glance, read from the record of calls like everything else here:
        // one pass over the calls of the window, newest first, up to a ceiling. Past the
        // ceiling, or the time budget, the figures are of the newest calls only — and say so.
        read.MapGet("/summary", async (
            HttpContext ctx, IQueryExecutor executor, ISearchGate gate, StorageEngine storage, int hours = 24) =>
        {
            if (LogStoreGate.IsClosed(storage)) return LogStoreGate.Closed;
            using var slot = await gate.TryEnterAsync(ctx.RequestAborted);
            if (slot is null) return Busy(ctx);
            using var deadline = Deadline(gate, ctx.RequestAborted);

            hours = Math.Clamp(hours, 1, 31 * 24);
            var summary = new CallSummary();
            bool partial = false;
            try
            {
                await foreach (var ev in executor.ExecuteAsync(new QueryRequest
                {
                    Filter    = "@service = " + FilterGuard.Quote(FilterGuard.AuditService),
                    FromUtc   = DateTimeOffset.UtcNow.AddHours(-hours),
                    Count     = SummaryCeiling + 1,
                    Direction = QueryDirection.Backward,
                }, deadline.Token))
                {
                    if (summary.Calls == SummaryCeiling) { partial = true; break; }
                    summary.Add(ev);
                }
            }
            catch (OperationCanceledException) when (!ctx.RequestAborted.IsCancellationRequested) { partial = true; }
            catch (ObjectDisposedException) when (LogStoreGate.IsClosed(storage)) { return LogStoreGate.Closed; }

            return Results.Ok(summary.ToDto(hours, partial));
        });

        // Every token changes with the secret, so this is its own deliberate call.
        write.MapPost("/secret/rotate", (McpStore store) =>
            Results.Ok(new { secretCreatedAt = store.RotateSecret().ToString("O") }));

        // ── Profiles ──────────────────────────────────────────────────────────
        read.MapGet("/profiles", (McpStore store) =>
        {
            var snapshot = store.Current;
            return Results.Ok(snapshot.Profiles.Select(p => ProfileDto(p, snapshot)));
        });

        write.MapPost("/profiles", (HttpContext ctx, McpProfileRequest body, McpStore store) =>
        {
            if (ToInput(body, store.Current, creating: true, out var input) is { } invalid) return invalid;
            var (profile, key) = store.CreateProfile(input!, ctx.User.FindFirstValue(ClaimTypes.Name) ?? "unknown");
            // The key is in this answer and nowhere else, ever: only its hash is stored.
            return Results.Ok(new { profile = ProfileDto(profile, store.Current), key });
        });

        // A member the body leaves out keeps its value, as for PUT /settings: an update that
        // names only the name must not also open the profile to every service.
        write.MapPut("/profiles/{id}", (string id, McpProfileRequest body, McpStore store) =>
        {
            if (ToInput(body, store.Current, creating: false, out var input) is { } invalid) return invalid;
            return store.UpdateProfile(id, input!) is { } profile ? Results.Ok(ProfileDto(profile, store.Current)) : Results.NotFound();
        });

        // What the profile's key is offered — the list tools/list answers it with, built by the
        // same code, so that Settings never shows a list the client does not get.
        read.MapGet("/profiles/{id}/tools", (string id, McpStore store) =>
        {
            var snapshot = store.Current;
            if (snapshot.FindProfile(id) is not { } profile) return Results.NotFound();
            return Results.Ok(new
            {
                tools = McpToolHost.Offered(profile, snapshot).Select(static t => new
                {
                    name        = t.Name,
                    description = t.Description,
                    builtIn     = BuiltInTools.Find(t.Name) is not null,
                    inputSchema = t.InputSchema,
                }),
            });
        });

        write.MapPost("/profiles/{id}/rotate-key", (string id, McpStore store) =>
            store.RotateKey(id) is { } key ? Results.Ok(new { key }) : Results.NotFound());

        write.MapDelete("/profiles/{id}", (string id, McpStore store) =>
            store.DeleteProfile(id) ? Results.NoContent() : Results.NotFound());

        // ── Field rules ───────────────────────────────────────────────────────
        read.MapGet("/rules", (McpStore store) => Results.Ok(store.Current.Rules.Select(RuleDto)));

        write.MapPost("/rules", (McpRuleRequest body, McpStore store) =>
            ToRule(body, out var action) is { } invalid
                ? invalid
                : Results.Ok(RuleDto(store.UpsertRule(null, body.Field!, body.Services, action))));

        write.MapPut("/rules/{id}", (string id, McpRuleRequest body, McpStore store) =>
        {
            if (!store.Current.Rules.Any(r => r.Id == id)) return Results.NotFound();
            return ToRule(body, out var action) is { } invalid
                ? invalid
                : Results.Ok(RuleDto(store.UpsertRule(id, body.Field!, body.Services, action)));
        });

        write.MapDelete("/rules/{id}", (string id, McpStore store) =>
            store.DeleteRule(id) ? Results.NoContent() : Results.NotFound());

        // ── Published tools ───────────────────────────────────────────────────
        read.MapGet("/tools", (McpStore store) => Results.Ok(new
        {
            builtIn   = BuiltInTools.All.Select(t => BuiltInDto(t, store.Current.Settings)),
            published = store.Current.Tools.Select(ToolDto),
        }));

        write.MapPost("/tools", (HttpContext ctx, McpToolRequest body, McpStore store) =>
        {
            var tool = ToTool(body, existing: null, ctx.User.FindFirstValue(ClaimTypes.Name) ?? "unknown");
            return CheckTool(tool, store) ?? Results.Ok(ToolDto(store.SaveTool(tool)));
        });

        write.MapPut("/tools/{id}", (string id, McpToolRequest body, McpStore store) =>
        {
            if (store.Current.Tools.FirstOrDefault(x => x.Id == id) is not { } existing) return Results.NotFound();
            var tool = ToTool(body, existing, existing.CreatedBy);
            return CheckTool(tool, store) ?? Results.Ok(ToolDto(store.SaveTool(tool)));
        });

        write.MapDelete("/tools/{id}", (string id, McpStore store) =>
            store.DeleteTool(id) ? Results.NoContent() : Results.NotFound());

        // The editor's test run: a definition as it stands in the form, saved or not, answered
        // exactly as the chosen profile would be answered.
        write.MapPost("/tools/test", async (HttpContext ctx, McpToolTestRequest body, McpStore store, McpToolHost host) =>
        {
            var snapshot = store.Current;
            if (body.Tool is null) return Invalid("tool is required.", "tool");
            if (string.IsNullOrEmpty(body.ProfileId) || snapshot.FindProfile(body.ProfileId) is not { } profile)
                return Invalid("Choose the profile to run the tool as.", "profileId");

            var existing = body.Id is null ? null : snapshot.Tools.FirstOrDefault(x => x.Id == body.Id);
            var tool = ToTool(body.Tool, existing, existing?.CreatedBy ?? "");
            if (CheckTool(tool, store) is { } invalid) return invalid;

            var arguments = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
            if (body.Arguments is { ValueKind: JsonValueKind.Object } sent)
                foreach (var argument in sent.EnumerateObject()) arguments[argument.Name] = argument.Value.Clone();

            long started = System.Diagnostics.Stopwatch.GetTimestamp();
            var result = await host.TestAsync(tool, profile, arguments, ctx.RequestAborted);
            return Results.Ok(new
            {
                text        = result.Text,
                isError     = result.IsError,
                status      = result.Status,
                // The template with these arguments in place; null when the arguments did not bind.
                filter      = result.Filter,
                events      = result.Events,
                masked      = result.Stats.Total,
                maskedBy    = MaskedByDto(result.Stats),
                tokens      = result.Text.Length / 3,
                elapsedMs   = (long)System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalMilliseconds,
                // What the AI is shown of the tool before it ever calls it.
                definition  = new { name = tool.Name, description = tool.Description, inputSchema = PublishedToolBinder.InputSchema(tool) },
            });
        });

        // How often each tool was called, from the record of calls.
        read.MapGet("/tools/usage", async (
            HttpContext ctx, IQueryExecutor executor, ISearchGate gate, StorageEngine storage, int days = 7) =>
        {
            if (LogStoreGate.IsClosed(storage)) return LogStoreGate.Closed;
            using var slot = await gate.TryEnterAsync(ctx.RequestAborted);
            if (slot is null) return Busy(ctx);
            using var deadline = Deadline(gate, ctx.RequestAborted);
            try
            {
                var now    = DateTimeOffset.UtcNow;
                var result = await new Ameto.Query.AggregationExecutor(executor, headerScan: storage).ExecuteAsync(
                    AggregationParser.Parse(
                        $"select count(*) where @service = {FilterGuard.Quote(FilterGuard.AuditService)} group by {McpAudit.Fields.Tool} limit 500"),
                    now.AddDays(-Math.Clamp(days, 1, 90)), now, deadline.Token);
                return Results.Ok(new
                {
                    days,
                    partial = result.Partial,
                    calls   = result.Rows.Where(static r => r.Key[0] is not null)
                                         .ToDictionary(static r => r.Key[0]!, static r => (long)(r.Values[0] ?? 0)),
                });
            }
            catch (OperationCanceledException) when (!ctx.RequestAborted.IsCancellationRequested)
            {
                return Results.Json(
                    new { error = $"The query exceeded its {gate.Budget.TotalSeconds:0}s budget." },
                    statusCode: StatusCodes.Status504GatewayTimeout);
            }
            catch (ObjectDisposedException) when (LogStoreGate.IsClosed(storage)) { return LogStoreGate.Closed; }
        });

        // ── What the AI sees ──────────────────────────────────────────────────
        // One stored event next to the same event as an answer would carry it. It returns the
        // event as stored, which is why it is an admin's and not a manager's.
        write.MapPost("/preview", async (
            HttpContext ctx, McpPreviewRequest body, McpStore store, IQueryExecutor executor, ISearchGate gate) =>
        {
            var snapshot = store.Current;
            MaskingPolicy policy;
            if (!string.IsNullOrEmpty(body.ProfileId))
            {
                if (snapshot.FindProfile(body.ProfileId) is not { } profile) return Results.NotFound();
                policy = snapshot.PolicyFor(profile);
            }
            else
                policy = string.Equals(body.Mode, "strict", StringComparison.OrdinalIgnoreCase) ? snapshot.Strict : snapshot.Standard;

            if (!string.IsNullOrWhiteSpace(body.Filter))
            {
                try { CompiledFilter.Compile(body.Filter); }
                catch (Exception e) { return Results.BadRequest(new { error = $"Invalid filter: {e.Message}", field = "filter" }); }
            }

            using var slot = await gate.TryEnterAsync(ctx.RequestAborted);
            if (slot is null) return Busy(ctx);
            using var deadline = Deadline(gate, ctx.RequestAborted);

            LogEvent? found = null;
            try
            {
                await foreach (var ev in executor.ExecuteAsync(new QueryRequest
                {
                    Filter    = string.IsNullOrWhiteSpace(body.Filter) ? null : body.Filter,
                    FromUtc   = DateTimeOffset.UtcNow.AddDays(-1),
                    Count     = 20,
                    Direction = QueryDirection.Backward,
                }, deadline.Token))
                {
                    // The AI's own audit trail is not what anyone wants to preview a rule on.
                    if (string.Equals(ev.ServiceName, FilterGuard.AuditService, StringComparison.OrdinalIgnoreCase)) continue;
                    found = ev;
                    break;
                }
            }
            catch (OperationCanceledException) when (!ctx.RequestAborted.IsCancellationRequested) { /* the budget: nothing found in time */ }

            var buffer = new ArrayBufferWriter<byte>(1024);
            using (var w = new Utf8JsonWriter(buffer))
            {
                w.WriteStartObject();
                w.WriteBoolean("found", found is not null);
                if (found is not null)
                {
                    var stats  = new MaskStats();
                    var masked = policy.Mask(found, stats);

                    w.WritePropertyName("stored");
                    LogEventJsonWriter.Write(w, found);

                    w.WriteStartObject("sent");
                    w.WriteString("template", masked.Template);
                    w.WriteStartArray("properties");
                    for (int i = 0; i < masked.Properties.Count; i++)
                    {
                        w.WriteStartObject();
                        w.WriteString("name", masked.Properties[i].Key);
                        w.WriteString("value", masked.Properties[i].Value);
                        // The name it is stored under: the same, unless the name itself held a
                        // value and went out as a token. What "stored" and "sent" are paired by.
                        w.WriteString("stored", masked.StoredNames[i]);
                        w.WriteEndObject();
                    }
                    w.WriteEndArray();
                    if (masked.Exception is { } x)
                    {
                        w.WriteStartObject("exception");
                        w.WriteString("type", x.Type);
                        if (x.Message is not null) w.WriteString("message", x.Message);
                        if (x.Stack   is not null) w.WriteString("stack", x.Stack);
                        w.WriteEndObject();
                    }
                    w.WriteEndObject();

                    w.WriteStartObject("masked");
                    w.WriteNumber("total", stats.Total);
                    WriteCounts(w, McpAudit.Fields.Rules,     stats.ByRule);
                    WriteCounts(w, McpAudit.Fields.Strict,    stats.ByStrict);
                    WriteCounts(w, McpAudit.Fields.Detectors, stats.ByDetector);
                    w.WriteEndObject();
                }
                w.WriteEndObject();
            }
            return Results.Bytes(buffer.WrittenMemory, "application/json; charset=utf-8");
        });

        // ── Activity ──────────────────────────────────────────────────────────
        // The AI's calls, read back from the log store where McpAudit writes them.
        read.MapGet("/activity", async (
            HttpContext ctx, IQueryExecutor executor, ISearchGate gate, StorageEngine storage,
            string? profile = null, string? tool = null, string? status = null,
            string? from = null, string? to = null, int count = 50, string? afterTs = null, string? afterId = null) =>
        {
            if (!TryInstant(from, out var fromUtc) || !TryInstant(to, out var toUtc))
                return Results.BadRequest(new { error = "'from' and 'to' are ISO-8601 timestamps." });

            string filter = "@service = " + FilterGuard.Quote(FilterGuard.AuditService);
            if (!string.IsNullOrEmpty(profile)) filter += $" and {McpAudit.Fields.ProfileId} = {FilterGuard.Quote(profile)}";
            if (!string.IsNullOrEmpty(tool))    filter += $" and {McpAudit.Fields.Tool} = {FilterGuard.Quote(tool)}";
            if (!string.IsNullOrEmpty(status))  filter += $" and {McpAudit.Fields.Status} = {FilterGuard.Quote(status)}";

            if (LogStoreGate.IsClosed(storage)) return LogStoreGate.Closed;
            using var slot = await gate.TryEnterAsync(ctx.RequestAborted);
            if (slot is null) return Busy(ctx);
            using var deadline = Deadline(gate, ctx.RequestAborted);

            int take = Math.Clamp(count, 1, 500);
            var calls = new List<object>(take);
            bool more = false;
            try
            {
                await foreach (var ev in executor.ExecuteAsync(new QueryRequest
                {
                    Filter              = filter,
                    FromUtc             = fromUtc ?? DateTimeOffset.UtcNow.AddDays(-1),
                    ToUtc               = toUtc,
                    Count               = take + 1,
                    Direction           = QueryDirection.Backward,
                    AfterTimestampTicks = long.TryParse(afterTs, NumberStyles.Integer, CultureInfo.InvariantCulture, out long ticks) ? ticks : null,
                    AfterEventId        = ulong.TryParse(afterId, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong raw) ? new Ameto.Core.EventId(raw) : null,
                }, deadline.Token))
                {
                    if (calls.Count == take) { more = true; break; }
                    var p = ev.Properties;
                    calls.Add(new
                    {
                        id        = ev.Id.RawValue.ToString(CultureInfo.InvariantCulture),
                        at        = ev.Timestamp.ToString("O", CultureInfo.InvariantCulture),
                        // Ticks pass 2^53, so the cursor travels as text.
                        afterTs   = ev.Timestamp.UtcTicks.ToString(CultureInfo.InvariantCulture),
                        tool      = Text(p, McpAudit.Fields.Tool),
                        profile   = Text(p, McpAudit.Fields.Profile),
                        profileId = Text(p, McpAudit.Fields.ProfileId),
                        status    = Text(p, McpAudit.Fields.Status),
                        events    = Number(p, McpAudit.Fields.Events),
                        masked    = Number(p, McpAudit.Fields.Masked),
                        tokens    = Number(p, McpAudit.Fields.Tokens),
                        elapsedMs = Number(p, McpAudit.Fields.ElapsedMs),
                        arguments = Text(p, McpAudit.Fields.Arguments),
                        refusal   = Text(p, McpAudit.Fields.Refusal),
                        refusalKind = Text(p, McpAudit.Fields.RefusalKind),
                        // For a call refused over the minute's limit: which refusal of that
                        // minute this is. Only the 1st, 2nd, 4th, 8th… are recorded.
                        repeats   = Number(p, McpAudit.Fields.Repeats),
                        filter    = Text(p, McpAudit.Fields.Filter),
                        client    = Text(p, McpAudit.Fields.Client),
                        maskedBy  = p is not null && p.TryGetValue(McpAudit.Fields.MaskedBy, out object? by) ? by : null,
                    });
                }
            }
            catch (OperationCanceledException) when (!ctx.RequestAborted.IsCancellationRequested)
            {
                return Results.Json(
                    new { error = $"The query exceeded its {gate.Budget.TotalSeconds:0}s budget. Narrow the time range." },
                    statusCode: StatusCodes.Status504GatewayTimeout);
            }
            catch (ObjectDisposedException) when (LogStoreGate.IsClosed(storage)) { return LogStoreGate.Closed; }

            return Results.Ok(new { calls, more });
        });
    }

    // ── Shapes ────────────────────────────────────────────────────────────────

    private static object Describe(McpStore store)
    {
        var settings = store.Current.Settings;
        return new
        {
            path            = McpAuth.Path,
            enabled         = settings.Enabled,
            secretCreatedAt = store.SecretCreatedAt.ToString("O"),
            limits          = new
            {
                responseBudgetTokens = settings.ResponseBudgetTokens,
                maxWindowMinutes     = settings.MaxWindowMinutes,
                maxEvents            = settings.MaxEvents,
                callsPerMinute       = settings.CallsPerMinute,
                maxConcurrent        = settings.MaxConcurrent,
            },
            detectors = Detectors.Select(d => new { id = d.Id, name = d.Name, on = (settings.Detectors & d.Flag) != 0 }),
            tools     = BuiltInTools.All.Select(t => BuiltInDto(t, settings)),
        };
    }

    private static object BuiltInDto(ToolDefinition t, McpSettings settings) => new
    {
        name            = t.Name,
        title           = t.Title,
        description     = t.Description,
        takesFreeFilter = t.TakesFreeFilter,
        // Switched off here, a built-in tool is offered to no profile, whatever the profile says.
        enabled         = !settings.DisabledTools.Contains(t.Name, StringComparer.Ordinal),
    };

    private static object ToolDto(McpPublishedTool t) => new
    {
        id          = t.Id,
        name        = t.Name,
        description = t.Description,
        parameters  = t.Parameters.Select(static p => new
        {
            name        = p.Name,
            type        = p.Type,
            required    = p.Required,
            description = p.Description,
            values      = p.Values,
            @default    = p.Default,
        }),
        filter               = t.Filter,
        fields               = t.Fields,
        defaultWindowMinutes = t.DefaultWindowMinutes,
        maxEvents            = t.MaxEvents,
        order                = t.OldestFirst ? "oldest" : "newest",
        published            = t.Published,
        profiles             = t.Profiles,
        createdBy            = t.CreatedBy,
        createdAt            = t.CreatedAt.ToString("O", CultureInfo.InvariantCulture),
        updatedAt            = t.UpdatedAt.ToString("O", CultureInfo.InvariantCulture),
        inputSchema          = PublishedToolBinder.InputSchema(t),
    };

    /// <summary>The tool a request describes; <paramref name="existing"/> keeps its id and its history.</summary>
    private static McpPublishedTool ToTool(McpToolRequest body, McpPublishedTool? existing, string createdBy) => new()
    {
        Id          = existing?.Id ?? Guid.NewGuid().ToString("N"),
        Name        = body.Name?.Trim() ?? "",
        Description = body.Description?.Trim() ?? "",
        Parameters  = [.. (body.Parameters ?? []).Select(static p => new McpToolParameter
        {
            Name        = p.Name?.Trim() ?? "",
            Type        = p.Type?.Trim().ToLowerInvariant() is { Length: > 0 } type ? type : McpToolParameter.String,
            Required    = p.Required ?? false,
            Description = p.Description?.Trim() ?? "",
            Values      = [.. (p.Values ?? []).Select(static v => v.Trim()).Where(static v => v.Length > 0)],
            Default     = string.IsNullOrWhiteSpace(p.Default) ? null : p.Default.Trim(),
        })],
        Filter               = body.Filter?.Trim() ?? "",
        Fields               = body.Fields ?? [],
        DefaultWindowMinutes = Math.Clamp(body.DefaultWindowMinutes ?? 60, 1, 366 * 24 * 60),
        MaxEvents            = Math.Clamp(body.MaxEvents ?? 200, 1, 5_000),
        OldestFirst          = string.Equals(body.Order, "oldest", StringComparison.OrdinalIgnoreCase),
        Published            = body.Published ?? false,
        Profiles             = body.Profiles ?? [],
        CreatedBy            = createdBy,
        CreatedAt            = existing?.CreatedAt ?? DateTimeOffset.UtcNow,
    };

    /// <summary>
    /// The 400 for a definition that cannot be saved: a rule of the definition itself, or a
    /// filter that reads a hidden field in a way no AI call may.
    /// </summary>
    private static IResult? CheckTool(McpPublishedTool tool, McpStore store)
    {
        var snapshot = store.Current;
        if (PublishedToolBinder.Validate(tool, snapshot.Tools) is { } problem) return Invalid(problem.Error, problem.Field);
        foreach (string id in tool.Profiles)
            if (snapshot.FindProfile(id) is null) return Invalid($"There is no profile with the id '{id}'.", "profiles");

        // With sample values in place of the parameters: the question is about the filter's
        // shape, which no argument can change.
        var sample = PublishedToolBinder.Bind(tool, PublishedToolBinder.SampleArguments(tool));
        if (sample.Error is { } unbound) return Invalid(unbound, "filter");
        return FilterGuard.Prepare(sample.Filter, snapshot.Standard, new TokenVault()).Error is { } refusal
            ? Invalid(refusal, "filter")
            : null;
    }

    /// <summary>What an answer hid, by what hid it: the three groups, each by field path or detector id.</summary>
    private static object MaskedByDto(MaskStats stats) => new
    {
        rules     = stats.ByRule,
        strict    = stats.ByStrict,
        detectors = stats.ByDetector,
    };

    private static void WriteCounts(Utf8JsonWriter w, string group, IReadOnlyDictionary<string, int> counts)
    {
        w.WriteStartObject(group);
        foreach (var (what, count) in counts) w.WriteNumber(what, count);
        w.WriteEndObject();
    }

    /// <summary>The calls of a window, added up as they are read.</summary>
    private sealed class CallSummary
    {
        private readonly HashSet<string> _profiles = new(StringComparer.Ordinal);
        private readonly Dictionary<string, long> _statuses  = new(StringComparer.Ordinal);
        private readonly Dictionary<string, long> _refusals  = new(StringComparer.Ordinal);
        private readonly Dictionary<string, long> _rules     = new(StringComparer.Ordinal);
        private readonly Dictionary<string, long> _detectors = new(StringComparer.Ordinal);
        private readonly List<long> _eventsPerCall = [];
        private long _events, _masked, _byRules, _byStrict, _byDetectors;

        /// <summary>
        /// The refusals past a profile's minute, by profile and minute: how many of them were
        /// written down, and the highest number any of those records carries. Only the 1st,
        /// 2nd, 4th, 8th… are recorded (McpAudit.Keeps), so the last record of a minute says
        /// how many there were at least, and the records themselves say far fewer.
        /// </summary>
        private readonly Dictionary<(string Profile, long Minute), (int Records, long Most)> _pastTheMinute = [];

        public int Calls { get; private set; }

        public void Add(LogEvent ev)
        {
            var p = ev.Properties;
            Calls++;
            string? profile = Text(p, McpAudit.Fields.ProfileId);
            if (profile is { Length: > 0 }) _profiles.Add(profile);
            Bump(_statuses, Text(p, McpAudit.Fields.Status) ?? "", 1);
            if (Number(p, McpAudit.Fields.Repeats) is { } nth and > 0)
            {
                var minute = (profile ?? "", ev.Timestamp.UtcTicks / TimeSpan.TicksPerMinute);
                var seen = _pastTheMinute.GetValueOrDefault(minute);
                _pastTheMinute[minute] = (seen.Records + 1, Math.Max(seen.Most, nth));
            }
            else if (Text(p, McpAudit.Fields.RefusalKind) is { Length: > 0 } kind) Bump(_refusals, kind, 1);

            long events = Number(p, McpAudit.Fields.Events) ?? 0;
            _events += events;
            if (events > 0) _eventsPerCall.Add(events);

            _masked += Number(p, McpAudit.Fields.Masked) ?? 0;
            if (p is null || !p.TryGetValue(McpAudit.Fields.MaskedBy, out object? by) || by is not Dictionary<string, object?> groups) return;
            _byRules     += Sum(groups, McpAudit.Fields.Rules,     into: null);
            _byStrict    += Sum(groups, McpAudit.Fields.Strict,    into: null);
            _byDetectors += Sum(groups, McpAudit.Fields.Detectors, into: _detectors);
            Sum(groups, McpAudit.Fields.RuleIds, into: _rules);
        }

        private static long Sum(Dictionary<string, object?> groups, string group, Dictionary<string, long>? into)
        {
            if (!groups.TryGetValue(group, out object? value) || value is not Dictionary<string, object?> counts) return 0;
            long sum = 0;
            foreach (string key in counts.Keys)
            {
                if (Number(counts, key) is not { } n) continue;
                sum += n;
                if (into is not null) Bump(into, key, n);
            }
            return sum;
        }

        private static void Bump(Dictionary<string, long> counts, string key, long by) =>
            counts[key] = counts.GetValueOrDefault(key) + by;

        /// <summary>The middle of the calls that returned events at all: an overview or a table returns none.</summary>
        private long? MedianEvents()
        {
            if (_eventsPerCall.Count == 0) return null;
            _eventsPerCall.Sort();
            return _eventsPerCall[_eventsPerCall.Count / 2];
        }

        public object ToDto(int hours, bool partial)
        {
            // A refusal past the minute is counted by what its minute's last record says, not by
            // how many records the minute left: twenty such refusals are five records.
            long pastTheMinute = 0, unrecorded = 0;
            foreach (var (records, most) in _pastTheMinute.Values)
            {
                pastTheMinute += Math.Max(most, records);
                unrecorded    += Math.Max(most - records, 0);
            }
            if (pastTheMinute > 0) Bump(_refusals, McpAudit.NameOf(RefusalKind.Limit), pastTheMinute);

            return new
            {
                hours,
                partial,
                // The calls that were written down. Beside them, at least `unrecorded` more were
                // refused past a profile's minute and are known of only by the count their minute's
                // records carry — "at least", because the count is the last one recorded.
                calls        = Calls,
                unrecorded,
                // True when any refusal past a profile's minute is in the window. Then every count
                // it is part of is a floor, `unrecorded` 0 included: a minute whose last record is
                // its 2nd refusal may have had a 3rd, which no record holds.
                thinned      = pastTheMinute > 0,
                profiles     = _profiles.Count,
                events       = _events,
                medianEvents = MedianEvents(),
                masked       = _masked,
                maskedBy     = new { rules = _byRules, strict = _byStrict, detectors = _byDetectors },
                statuses     = _statuses,
                refusals     = _refusals,
                // Hidden values by the rule that hid them (rule id) and by detector (detector id).
                rules        = _rules,
                detectors    = _detectors,
            };
        }
    }

    private static object ProfileDto(McpProfile p, McpSnapshot snapshot) => new
    {
        id               = p.Id,
        name             = p.Name,
        description      = p.Description,
        // As for the ingest keys: a prefix of the hash, enough to tell two keys apart.
        keyPreview       = p.KeyHash[..8] + "…",
        toolsMode        = Camel(p.ToolsMode.ToString()),
        tools            = p.Tools,
        services         = p.Services,
        maskingMode      = Camel(p.MaskingMode.ToString()),
        freeFilters      = p.FreeFilters,
        maxEvents        = p.MaxEvents,
        maxWindowMinutes = p.MaxWindowMinutes,
        callsPerMinute   = p.CallsPerMinute,
        // The saved tools that name this profile, drafts included; a draft is granted and not yet offered.
        publishedTools   = snapshot.Tools.Where(t => t.Profiles.Contains(p.Id, StringComparer.Ordinal)).Select(static t => t.Id),
        createdBy        = p.CreatedBy,
        createdAt        = p.CreatedAt.ToString("O", CultureInfo.InvariantCulture),
        lastCallAt       = p.LastCallUnixMs == 0
                               ? null
                               : DateTimeOffset.FromUnixTimeMilliseconds(p.LastCallUnixMs).ToString("O", CultureInfo.InvariantCulture),
    };

    private static object RuleDto(McpMaskRule r) => new
    {
        id       = r.Id,
        field    = r.Field,
        services = r.Services,
        action   = Camel(r.Action.ToString()),
    };

    /// <summary>
    /// The validated input, or the 400 that says which field is wrong. A member the body leaves
    /// out stays null in the input, and the store leaves it as it is; only a new profile has to
    /// be given a name.
    /// </summary>
    private static IResult? ToInput(McpProfileRequest body, McpSnapshot snapshot, bool creating, out McpProfileInput? input)
    {
        input = null;
        if (creating || body.Name is not null)
        {
            if (string.IsNullOrWhiteSpace(body.Name)) return Invalid("Name is required.", "name");
            if (body.Name.Length > 100)               return Invalid("Name is at most 100 characters.", "name");
        }

        McpToolsMode? toolsMode = null;
        if (body.ToolsMode is not null)
        {
            if (!TryName(body.ToolsMode, out McpToolsMode named)) return Invalid("toolsMode is builtInAndPublished or publishedOnly.", "toolsMode");
            toolsMode = named;
        }
        McpMaskingMode? maskingMode = null;
        if (body.MaskingMode is not null)
        {
            if (!TryName(body.MaskingMode, out McpMaskingMode named)) return Invalid("maskingMode is standard or strict.", "maskingMode");
            maskingMode = named;
        }

        if (body.Tools is not null)
            foreach (string tool in body.Tools)
                if (BuiltInTools.Find(tool) is null) return Invalid($"'{tool}' is not a tool.", "tools");

        if (body.PublishedTools is not null)
            foreach (string id in body.PublishedTools)
                if (!snapshot.Tools.Any(t => t.Id == id)) return Invalid($"There is no published tool with the id '{id}'.", "publishedTools");

        input = new McpProfileInput
        {
            Name             = body.Name,
            Description      = body.Description,
            ToolsMode        = toolsMode,
            Tools            = body.Tools,
            Services         = body.Services,
            MaskingMode      = maskingMode,
            FreeFilters      = body.FreeFilters,
            MaxEvents        = body.MaxEvents,
            MaxWindowMinutes = body.MaxWindowMinutes,
            CallsPerMinute   = body.CallsPerMinute,
            PublishedTools   = body.PublishedTools,
        };
        return null;
    }

    private static IResult? ToRule(McpRuleRequest body, out MaskAction action)
    {
        action = MaskAction.Hash;
        if (string.IsNullOrWhiteSpace(body.Field)) return Invalid("Field is required.", "field");
        if (body.Field.Length > 200)               return Invalid("Field is at most 200 characters.", "field");
        if (body.Action is not null && !TryName(body.Action, out action))
            return Invalid("action is hash, remove, keepLast4 or keep.", "action");
        return null;
    }

    /// <summary>
    /// An enum value by its NAME, whatever its case. <c>Enum.TryParse</c> also reads a number —
    /// "2" is a value of every enum as far as it is concerned, defined or not — and a masking
    /// mode that silently became another one is not a typo to pass over.
    /// </summary>
    private static bool TryName<T>(string text, out T value) where T : struct, Enum
    {
        text = text.Trim();
        foreach (string name in Enum.GetNames<T>())
            if (name.Equals(text, StringComparison.OrdinalIgnoreCase))
            {
                value = Enum.Parse<T>(name);
                return true;
            }
        value = default;
        return false;
    }

    private static IResult Invalid(string error, string field) => Results.BadRequest(new { error, field });

    private static IResult Busy(HttpContext ctx)
    {
        ctx.Response.Headers.RetryAfter = "5";
        return Results.Json(new { error = "Too many searches are running. Try again in a moment." },
                            statusCode: StatusCodes.Status503ServiceUnavailable);
    }

    private static CancellationTokenSource Deadline(ISearchGate gate, CancellationToken requestAborted)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(requestAborted);
        if (gate.Budget > TimeSpan.Zero) cts.CancelAfter(gate.Budget);
        return cts;
    }

    private static bool TryInstant(string? raw, out DateTimeOffset? value)
    {
        value = null;
        if (string.IsNullOrEmpty(raw)) return true;
        if (!DateTimeOffset.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed)) return false;
        value = parsed.ToUniversalTime();
        return true;
    }

    private static string? Text(Dictionary<string, object?>? properties, string key) =>
        properties is not null && properties.TryGetValue(key, out object? value) ? value?.ToString() : null;

    private static long? Number(Dictionary<string, object?>? properties, string key) =>
        properties is not null && properties.TryGetValue(key, out object? value) ? value switch
        {
            long l   => l,
            int i    => i,
            ulong u  => (long)u,
            double d => (long)d,
            _        => null,
        } : null;

    private static string Camel(string name) => char.ToLowerInvariant(name[0]) + name[1..];
}

internal sealed record McpSettingsRequest(
    bool? Enabled = null, string[]? Detectors = null, int? ResponseBudgetTokens = null, int? MaxWindowMinutes = null,
    int? MaxEvents = null, int? CallsPerMinute = null, int? MaxConcurrent = null, string[]? DisabledTools = null);

internal sealed record McpProfileRequest(
    string? Name = null, string? Description = null, string? ToolsMode = null, string[]? Tools = null,
    string[]? Services = null, string? MaskingMode = null, bool? FreeFilters = null,
    int? MaxEvents = null, int? MaxWindowMinutes = null, int? CallsPerMinute = null, string[]? PublishedTools = null);

internal sealed record McpRuleRequest(string? Field = null, string[]? Services = null, string? Action = null);

internal sealed record McpPreviewRequest(string? ProfileId = null, string? Mode = null, string? Filter = null);

internal sealed record McpToolParameterRequest(
    string? Name = null, string? Type = null, bool? Required = null, string? Description = null,
    string[]? Values = null, string? Default = null);

internal sealed record McpToolRequest(
    string? Name = null, string? Description = null, McpToolParameterRequest[]? Parameters = null, string? Filter = null,
    string[]? Fields = null, int? DefaultWindowMinutes = null, int? MaxEvents = null, string? Order = null,
    bool? Published = null, string[]? Profiles = null);

internal sealed record McpToolTestRequest(
    McpToolRequest? Tool = null, string? Id = null, string? ProfileId = null, JsonElement? Arguments = null);
