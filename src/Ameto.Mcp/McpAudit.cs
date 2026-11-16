using System.Collections.Concurrent;
using Ameto.Core;
using Ameto.Mcp.Masking;
using Ameto.Mcp.Tools;
using Ameto.Storage;
using Microsoft.Extensions.Logging;
using LogLevel = Ameto.Core.LogLevel;

namespace Ameto.Mcp;

/// <summary>
/// Writes every AI call into the log store itself, as an event of the service
/// <c>ameto-mcp</c>: who asked, which tool, with which arguments, how much came back and how
/// much of it was hidden. The Activity tab reads these events back; so does anyone with the
/// Logs page and the filter <c>@service = 'ameto-mcp'</c>, and so do retention, the indexes and
/// the alert rules — none of which had to be written again for a table of its own.
///
/// <para>The arguments are recorded as the AI sent them, and so is the filter they made: a token
/// stays a token, and the filter that actually ran — with the token replaced by its value — is
/// not written anywhere.</para>
/// </summary>
internal sealed class McpAudit(StorageEngine storage, ILogger<McpAudit> logger)
{
    public const string Template = "AI call {Tool} by {Profile}: {Status}";

    /// <summary>
    /// The most JSON one call's arguments may be — and what the record keeps of them, which is
    /// the same number on purpose: a call is refused before it is longer than its record.
    /// A filter has its own, shorter limit (<see cref="FilterGuard.MaxFilterChars"/>).
    /// </summary>
    public const int MaxArgumentChars = 6_000;

    /// <summary>The longest tool name, and the longest client name, the record keeps.</summary>
    public const int MaxNameChars = 64, MaxClientChars = 200;

    /// <summary>
    /// Whether the <paramref name="repeat"/>-th refusal of a spent minute is written down: the
    /// 1st, 2nd, 4th, 8th… — each carrying its number, so the record says how many there were
    /// without holding a line for each. A profile over its limit can ask as fast as the network
    /// carries it; the store is not what should absorb that. Zero is a call that was not such a
    /// refusal, and is always kept.
    /// </summary>
    public static bool Keeps(int repeat) => repeat <= 1 || System.Numerics.BitOperations.IsPow2(repeat);

    /// <summary>Property names of an audit event; the Activity endpoint filters and reads by them.</summary>
    public static class Fields
    {
        public const string Tool = "Tool", Profile = "Profile", ProfileId = "ProfileId", Status = "Status",
                            Events = "Events", Masked = "Masked", MaskedBy = "MaskedBy", Tokens = "Tokens",
                            ElapsedMs = "ElapsedMs", Arguments = "Arguments", Refusal = "Refusal", Client = "Client",
                            Filter = "Filter", RefusalKind = "RefusalKind", Repeats = "Repeats";

        /// <summary>
        /// The groups inside <see cref="MaskedBy"/>: what hid the values. The first three count
        /// by field path or detector id; <see cref="RuleIds"/> counts the rules' own hits.
        /// </summary>
        public const string Rules = "rules", Strict = "strict", Detectors = "detectors", RuleIds = "ruleIds";
    }

    /// <summary>A refusal's kind as the record spells it.</summary>
    public static string NameOf(RefusalKind kind) => kind switch
    {
        RefusalKind.Input       => "input",
        RefusalKind.Limit       => "limit",
        RefusalKind.Masking     => "masking",
        RefusalKind.Scope       => "scope",
        RefusalKind.Unavailable => "unavailable",
        _                       => "",
    };

    /// <summary>What an answer hid, as the record keeps it: only the groups that hold something.</summary>
    private static Dictionary<string, object?> MaskedBy(MaskStats stats)
    {
        var by = new Dictionary<string, object?>(4);
        Put(Fields.Rules,     stats.ByRule);
        Put(Fields.Strict,    stats.ByStrict);
        Put(Fields.Detectors, stats.ByDetector);
        Put(Fields.RuleIds,   stats.RuleHits);
        return by;

        void Put(string group, IReadOnlyDictionary<string, int> counts)
        {
            if (counts.Count > 0) by[group] = counts.ToDictionary(static p => p.Key, static p => (object?)p.Value);
        }
    }

    /// <summary>
    /// Writes one call down. <paramref name="repeats"/> is, for a refusal over the minute's
    /// limit, which one of that minute it is (see <see cref="Keeps"/>); zero otherwise.
    /// </summary>
    public void Record(McpProfile profile, string tool, string argumentsJson, ToolResult result, TimeSpan elapsed, string? client, int repeats = 0)
    {
        try
        {
            var properties = new Dictionary<string, object?>(12)
            {
                [Fields.Tool]      = tool,
                [Fields.Profile]   = profile.Name,
                [Fields.ProfileId] = profile.Id,
                [Fields.Status]    = result.Status,
                [Fields.Events]    = result.Events,
                [Fields.Masked]    = result.Stats.Total,
                [Fields.Tokens]    = result.Text.Length / 3,
                [Fields.ElapsedMs] = (long)elapsed.TotalMilliseconds,
                [Fields.Arguments] = argumentsJson.Length <= MaxArgumentChars
                                         ? argumentsJson
                                         : string.Concat(argumentsJson.AsSpan(0, MaxArgumentChars), "…"),
            };
            if (result.Stats.Total > 0) properties[Fields.MaskedBy] = MaskedBy(result.Stats);
            // A refusal is a sentence about the call as it was written, so it is safe to keep.
            if (result.IsError) properties[Fields.Refusal] = result.Text.Length <= 600 ? result.Text : result.Text[..600];
            if (result.Refusal != RefusalKind.None) properties[Fields.RefusalKind] = NameOf(result.Refusal);
            if (repeats > 1) properties[Fields.Repeats] = repeats;
            if (!string.IsNullOrEmpty(client))
                properties[Fields.Client] = client.Length <= MaxClientChars ? client : string.Concat(client.AsSpan(0, MaxClientChars), "…");
            // The filter the call asked with; for a published tool, its template with this call's
            // arguments in place — which the arguments alone do not show. As written: a token
            // among them is still a token. A filter that ran is never longer than this keeps;
            // one that is longer was refused for it, and its beginning is enough.
            if (!string.IsNullOrEmpty(result.Filter))
                properties[Fields.Filter] = result.Filter.Length <= FilterGuard.MaxFilterChars
                                                ? result.Filter
                                                : string.Concat(result.Filter.AsSpan(0, FilterGuard.MaxFilterChars), "…");

            byte[] payload = PropertyBytes.From(properties);
            int templateIndex = storage.TemplatePool.Intern(Template);
            int serviceIndex  = storage.TemplatePool.Intern(FilterGuard.AuditService);
            var header = new LogEventHeader
            {
                TimestampUtcTicks        = DateTime.UtcNow.Ticks,
                Level                    = result.Status switch
                {
                    ToolResult.Ok or ToolResult.Trimmed => LogLevel.Information,
                    ToolResult.Failed                   => LogLevel.Error,
                    _                                   => LogLevel.Warning,
                },
                MessageTemplatePoolIndex = templateIndex,
                ServiceNamePoolIndex     = serviceIndex,
            };

            // One try, no waiting: an audit line must never hold an answer back. A store with no
            // room this instant says so in the server's own log instead.
            if (!storage.TryWrite(header, payload,
                    template: templateIndex < 0 ? Template : null,
                    service:  serviceIndex  < 0 ? FilterGuard.AuditService : null))
                logger.LogWarning("AI call {Tool} by {Profile} ({Status}) was not written to the log: the store had no room",
                    tool, profile.Name, result.Status);
        }
        catch (Exception e)
        {
            logger.LogWarning(e, "AI call {Tool} by {Profile} could not be written to the log", tool, profile.Name);
        }
    }
}

/// <summary>
/// The two limits a call meets before it reads anything: how many calls a profile may make in a
/// minute, and how many AI calls may run at once. Both are refusals with a sentence — a model
/// reads "try again in 20 seconds" and does.
/// </summary>
internal sealed class McpCallLimiter
{
    private sealed class Minute
    {
        public long Start;
        public int  Calls;
        public int  Refused;
    }

    private readonly ConcurrentDictionary<string, Minute> _minutes = new(StringComparer.Ordinal);
    private int _running;

    /// <summary>
    /// Counts one call against the profile's minute; false when the minute is spent, and then
    /// <paramref name="refused"/> says how many calls of this minute have been turned away so
    /// far, this one included.
    /// </summary>
    public bool TryCount(string profileId, int perMinute, long nowMs, out int retryAfterSeconds, out int refused)
    {
        var minute = _minutes.GetOrAdd(profileId, static _ => new Minute());
        lock (minute)
        {
            long start = nowMs - nowMs % 60_000;
            if (minute.Start != start)
            {
                minute.Start   = start;
                minute.Calls   = 0;
                minute.Refused = 0;
            }
            retryAfterSeconds = (int)((start + 60_000 - nowMs) / 1000) + 1;
            refused = 0;
            if (minute.Calls >= perMinute)
            {
                if (minute.Refused < int.MaxValue) minute.Refused++;
                refused = minute.Refused;
                return false;
            }
            minute.Calls++;
            return true;
        }
    }

    /// <summary>
    /// Takes one of the <paramref name="max"/> places for a running AI call, waiting up to
    /// <paramref name="wait"/> for one: an agent that sends three calls at once should get three
    /// answers, a moment apart, rather than one answer and two refusals.
    /// </summary>
    public async ValueTask<bool> TryEnterAsync(int max, TimeSpan wait, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + (long)wait.TotalMilliseconds;
        while (true)
        {
            int running = Volatile.Read(ref _running);
            if (running < max)
            {
                if (Interlocked.CompareExchange(ref _running, running + 1, running) == running) return true;
                continue;
            }
            if (Environment.TickCount64 >= deadline) return false;
            await Task.Delay(25, ct).ConfigureAwait(false);
        }
    }

    public void Exit() => Interlocked.Decrement(ref _running);
}

/// <summary>
/// The server's search limits, as this assembly sees them: the slots every search on the server
/// shares, and the time one search may take. Implemented by the host over its <c>QueryGuard</c>,
/// so an AI call queues with the Logs page's searches rather than beside them.
/// </summary>
public interface ISearchGate
{
    /// <summary>The longest a search may run; zero for no limit.</summary>
    TimeSpan Budget { get; }

    /// <summary>A search slot to dispose when the search is done, or null when the server is at its limit.</summary>
    ValueTask<IDisposable?> TryEnterAsync(CancellationToken ct);
}
