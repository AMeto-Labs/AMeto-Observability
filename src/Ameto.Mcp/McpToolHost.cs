using System.Diagnostics;
using System.Security.Claims;
using System.Text.Json;
using Ameto.Core;
using Ameto.Mcp.Tools;
using Ameto.Storage;
using Microsoft.Extensions.Logging;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Ameto.Mcp;

/// <summary>
/// Answers <c>tools/list</c> and <c>tools/call</c>. The protocol around them — two generations
/// of it — is the SDK's; this class is where a request stops being a protocol message and
/// becomes a profile asking for something.
///
/// <para>The tool list is built per request from the profile the key belongs to. Nothing is
/// registered at start-up and nothing is compiled in per tool, which is what makes a tool
/// published in the UI appear without a restart: the next <c>tools/list</c> simply includes it.
/// The list says how long it may be cached and that it is this caller's own.</para>
/// </summary>
internal sealed class McpToolHost(
    McpStore store, LogTools tools, McpAudit audit, McpCallLimiter limiter, ISearchGate gate,
    StorageEngine storage, McpSessions sessions, ILogger<McpToolHost> logger)
{
    /// <summary>How long a client may keep a tool list before asking again.</summary>
    private static readonly TimeSpan ListLifetime = TimeSpan.FromSeconds(60);

    /// <summary>How long a call waits for one of the AI places before it is refused.</summary>
    private static readonly TimeSpan PlaceWait = TimeSpan.FromSeconds(10);

    /// <summary>The arguments as the client sent them, for the audit record.</summary>
    private static string ArgumentsJson(Dictionary<string, JsonElement> arguments)
    {
        var buffer = new System.Buffers.ArrayBufferWriter<byte>(256);
        using (var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions
               {
                   Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
               }))
        {
            writer.WriteStartObject();
            foreach (var (name, value) in arguments)
            {
                writer.WritePropertyName(name);
                value.WriteTo(writer);
            }
            writer.WriteEndObject();
        }
        return System.Text.Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>
    /// True when the profile is offered this built-in tool: switched on for the server, switched
    /// on for the profile, and not one the profile's own settings withdraw.
    /// </summary>
    internal static bool OffersBuiltIn(McpProfile profile, ToolDefinition tool, McpSettings settings) =>
        profile.ToolsMode != McpToolsMode.PublishedOnly
        && (!tool.TakesFreeFilter || profile.FreeFilters)
        && profile.Tools.Contains(tool.Name, StringComparer.Ordinal)
        && !settings.DisabledTools.Contains(tool.Name, StringComparer.Ordinal);

    /// <summary>
    /// The tools a profile is offered, in the order they are listed: the built-in ones it has
    /// switched on, then the published ones it was granted. Built from the database rows as they
    /// are at this moment — which is all there is to a tool being "created dynamically".
    /// </summary>
    internal static IEnumerable<ToolDefinition> Offered(McpProfile profile, McpSnapshot snapshot)
    {
        foreach (var tool in BuiltInTools.All)
            if (OffersBuiltIn(profile, tool, snapshot.Settings)) yield return tool;

        foreach (var tool in snapshot.Tools)
            if (tool.OfferedTo(profile))
                yield return new ToolDefinition(tool.Name, tool.Name, tool.Description, PublishedToolBinder.InputSchema(tool));
    }

    public ValueTask<ListToolsResult> ListAsync(RequestContext<ListToolsRequestParams> request, CancellationToken ct)
    {
        sessions.Track(request.Server);
        var snapshot = store.Current;
        var profile  = ProfileOf(request.User, snapshot);
        var list = new List<Tool>();
        foreach (var definition in Offered(profile, snapshot))
            list.Add(new Tool
            {
                Name        = definition.Name,
                Title       = definition.Title,
                Description = definition.Description,
                InputSchema = definition.InputSchema,
                Annotations = new ToolAnnotations { Title = definition.Title, ReadOnlyHint = true, OpenWorldHint = false },
            });

        return ValueTask.FromResult(new ListToolsResult
        {
            Tools      = list,
            TimeToLive = ListLifetime,
            CacheScope = CacheScope.Private,
        });
    }

    public async ValueTask<CallToolResult> CallAsync(RequestContext<CallToolRequestParams> request, CancellationToken ct)
    {
        sessions.Track(request.Server);
        var snapshot = store.Current;
        var profile  = ProfileOf(request.User, snapshot);
        string name  = request.Params?.Name ?? "";

        var arguments = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        if (request.Params?.Arguments is { } sent)
            foreach (var (key, value) in sent) arguments[key] = value;

        long started = Stopwatch.GetTimestamp();
        var  now     = DateTimeOffset.UtcNow;

        // A name is a built-in tool the profile has switched on, or a published one it was granted.
        var builtIn   = BuiltInTools.Find(name);
        var published = builtIn is null ? snapshot.FindTool(name) : null;
        bool offered  = builtIn is not null
            ? OffersBuiltIn(profile, builtIn, snapshot.Settings)
            : published?.OfferedTo(profile) == true;

        ToolResult result;
        if (!offered)
            result = ToolResult.Refused(RefusalKind.Scope, $"The tool '{name}' is not offered to this profile.");
        else if (!limiter.TryCount(profile.Id, Math.Min(profile.CallsPerMinute, snapshot.Settings.CallsPerMinute),
                                   now.ToUnixTimeMilliseconds(), out int retryAfter))
            result = ToolResult.Refused(RefusalKind.Limit, $"This profile has made its calls for this minute. Try again in {retryAfter} s.");
        else if (storage.Availability == QueryAvailability.Closed)
            result = ToolResult.Refused(RefusalKind.Unavailable, "The log store has shut down. Logs are available again once the server has restarted.");
        else
            result = await RunAsync(name, published, profile, snapshot, arguments, now, ct).ConfigureAwait(false);

        audit.Record(profile, name, ArgumentsJson(arguments), result, Stopwatch.GetElapsedTime(started), ClientOf(request.Server));
        store.Touch(profile.Id, now);

        return new CallToolResult
        {
            Content = [new TextContentBlock { Text = result.Text }],
            IsError = result.IsError,
        };
    }

    /// <summary>Who called: the client's name and version as it gives them, and the protocol revision it speaks.</summary>
    private static string? ClientOf(McpServer? server)
    {
        if (server is null) return null;
        var info = server.ClientInfo;
        string name = info is null ? "" : string.IsNullOrEmpty(info.Version) ? info.Name : $"{info.Name} {info.Version}";
        string? protocol = server.NegotiatedProtocolVersion;
        return name.Length == 0 ? (protocol is null ? null : "MCP " + protocol)
             : protocol is null ? name : $"{name} · MCP {protocol}";
    }

    /// <summary>
    /// Runs a tool definition that is not saved yet, as <paramref name="profile"/> would get it:
    /// the editor's test run. It takes a search slot and the time budget like any call, and is
    /// neither counted against the profile's minute nor written to the record of calls — it is
    /// the admin's call, not the AI's.
    /// </summary>
    public Task<ToolResult> TestAsync(
        McpPublishedTool tool, McpProfile profile, Dictionary<string, JsonElement> arguments, CancellationToken ct) =>
        RunAsync(tool.Name, tool, profile, store.Current, arguments, DateTimeOffset.UtcNow, ct);

    private async Task<ToolResult> RunAsync(
        string name, McpPublishedTool? published, McpProfile profile, McpSnapshot snapshot,
        Dictionary<string, JsonElement> arguments, DateTimeOffset now, CancellationToken ct)
    {
        if (!await limiter.TryEnterAsync(snapshot.Settings.MaxConcurrent, PlaceWait, ct).ConfigureAwait(false))
            return ToolResult.Refused(RefusalKind.Limit, "The server is running as many AI searches as it allows. Try again in a moment.");
        try
        {
            using var slot = await gate.TryEnterAsync(ct).ConfigureAwait(false);
            if (slot is null) return ToolResult.Refused(RefusalKind.Limit, "Too many searches are running on the server. Try again in a moment.");

            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
            if (gate.Budget > TimeSpan.Zero) deadline.CancelAfter(gate.Budget);
            bool TimedOut() => deadline.IsCancellationRequested && !ct.IsCancellationRequested;

            try
            {
                var call = new ToolCall
                {
                    Profile  = profile,
                    Snapshot = snapshot,
                    Args     = arguments,
                    Now      = now,
                    TimedOut = TimedOut,
                };
                return published is null
                    ? await tools.RunAsync(name, call, deadline.Token).ConfigureAwait(false)
                    : await tools.RunPublishedAsync(published, call, deadline.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (TimedOut())
            {
                return new ToolResult
                {
                    Text    = $"The call ran out of its {gate.Budget.TotalSeconds:0} s budget before it had an answer. Narrow the time window or the filter.",
                    IsError = true,
                    Status  = ToolResult.TimedOut,
                };
            }
            catch (ObjectDisposedException) when (storage.Availability == QueryAvailability.Closed)
            {
                return ToolResult.Refused(RefusalKind.Unavailable, "The log store has shut down. Logs are available again once the server has restarted.");
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;   // the client went away: there is nobody to answer
        }
        catch (Exception e)
        {
            // The exception can name segment paths and internals; the client gets a stable sentence.
            logger.LogError(e, "AI tool {Tool} failed for profile {Profile}", name, profile.Name);
            return new ToolResult
            {
                Text    = "The tool failed on the server. The server log has the details.",
                IsError = true,
                Status  = ToolResult.Failed,
            };
        }
        finally
        {
            limiter.Exit();
        }
    }

    /// <summary>
    /// The profile behind a request. The endpoint's policy has already required an authenticated
    /// profile key, so a request without one here means the profile was deleted between the
    /// check and this line — refused as any unknown key is.
    /// </summary>
    private static McpProfile ProfileOf(ClaimsPrincipal? user, McpSnapshot snapshot)
    {
        string? id = user?.FindFirstValue(ClaimTypes.NameIdentifier);
        return id is not null && snapshot.FindProfile(id) is { } profile
            ? profile
            : throw new McpException("This key does not belong to an AI profile.");
    }
}

/// <summary>
/// Tells the clients that hold a session when their tool list may have changed, so a tool
/// published in Settings shows up without anyone reconnecting.
///
/// <para>Only a client that opened with <c>initialize</c> has a session to be told on. A client
/// on the 2026-07-28 revision has none: its list carries a time to live instead, and it asks
/// again when that runs out.</para>
///
/// <para>Sessions are held weakly — this registry must not be what keeps an ended session's
/// server alive — and a notification that cannot be delivered removes its session from it.</para>
/// </summary>
internal sealed class McpSessions
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, WeakReference<McpServer>> _sessions =
        new(StringComparer.Ordinal);

    public McpSessions(McpStore store) => store.ToolListsChanged += NotifyAll;

    /// <summary>Remembers the session a request came on; a request without one is not tracked.</summary>
    public void Track(McpServer? server)
    {
        if (server?.SessionId is not { } id || _sessions.ContainsKey(id)) return;
        if (_sessions.Count >= SweepAt) Sweep();
        _sessions[id] = new WeakReference<McpServer>(server);
    }

    /// <summary>Entries past which the sessions that have ended are dropped before another is added.</summary>
    private const int SweepAt = 512;

    private void Sweep()
    {
        foreach (var (id, reference) in _sessions)
            if (!reference.TryGetTarget(out _)) _sessions.TryRemove(id, out _);
    }

    private void NotifyAll()
    {
        foreach (var (id, reference) in _sessions)
        {
            if (reference.TryGetTarget(out var server)) _ = NotifyAsync(id, server);
            else _sessions.TryRemove(id, out _);
        }
    }

    private async Task NotifyAsync(string id, McpServer server)
    {
        try
        {
            using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await server.SendNotificationAsync(NotificationMethods.ToolListChangedNotification, limit.Token).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The session has ended, or its client is not listening: it will list again when it reconnects.
            _sessions.TryRemove(id, out _);
        }
    }
}
