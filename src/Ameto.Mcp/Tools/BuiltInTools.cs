using System.Text.Json;

namespace Ameto.Mcp.Tools;

/// <summary>A tool as it is offered: its name, what it is for, and the JSON Schema of its arguments.</summary>
public sealed record ToolDefinition(string Name, string Title, string Description, JsonElement InputSchema)
{
    /// <summary>True for a tool that takes a filter or a query the AI writes itself.</summary>
    public bool TakesFreeFilter { get; init; }
}

/// <summary>
/// The log tools every installation has. Five, and deliberately not more: a model picks well
/// among a handful of tools whose purposes do not overlap, and each of these is a step of one
/// investigation — the shape of a window, the events, the numbers, one event up close, the
/// vocabulary.
/// </summary>
public static class BuiltInTools
{
    public const string Overview  = "logs_overview";
    public const string Search    = "logs_search";
    public const string Aggregate = "logs_aggregate";
    public const string Context   = "logs_context";
    public const string Fields    = "logs_fields";

    private const string TimeProperties = """
        "from": { "type": "string", "description": "Start of the window: ISO-8601, or relative such as -15m, -2h, -1d. Default: one hour before 'to'." },
        "to": { "type": "string", "description": "End of the window: ISO-8601, 'now', or relative. Default: now." }
        """;

    public static readonly IReadOnlyList<ToolDefinition> All =
    [
        new(Overview, "Log overview",
            "The shape of a time window before reading any event: how many events, by level and by service, " +
            "the most frequent message templates and the exception types that occurred. Start here.",
            Schema($$"""
                { "type": "object", "properties": {
                    {{TimeProperties}},
                    "service": { "type": "string", "description": "Only this service." }
                } }
                """)),

        new(Search, "Search log events",
            "Events matching a filter, grouped by message template: the template once, then a table of the properties that differ. " +
            "Each row has a 'ref' for logs_context. Values replaced by tokens (h:…) can be filtered on by the token. " +
            "The answer is cut to a budget: when it says 'more', call again with the cursor it gives.",
            Schema($$"""
                { "type": "object", "properties": {
                    "filter": { "type": "string", "description": "Filter expression, e.g. @l = 'Error' and StatusCode >= 500, or plain words: they are looked for in the message template and the exception, not in property values. Empty: every event." },
                    {{TimeProperties}},
                    "service": { "type": "string", "description": "Only this service." },
                    "levels": { "type": "array", "items": { "type": "string", "enum": ["Verbose", "Debug", "Information", "Warning", "Error", "Fatal"] }, "description": "Only these levels." },
                    "fields": { "type": "array", "items": { "type": "string" }, "description": "Only these properties in the answer. Default: all of them." },
                    "limit": { "type": "integer", "minimum": 1, "description": "Most events to return. Default 50." },
                    "order": { "type": "string", "enum": ["newest", "oldest"], "description": "Default: newest first." },
                    "cursor": { "type": "string", "description": "The cursor from the previous answer, to continue after it." }
                } }
                """))
            { TakesFreeFilter = true },

        new(Aggregate, "Count and group log events",
            "Answers with a table instead of events: select count(*) where @l = 'Error' group by @service. " +
            "Aggregates: count(*), count(P), sum(P), min(P), max(P), avg(P). Clauses: where <filter>, group by <property>[, …], limit <n>. " +
            "sum, min, max and avg are made of measurements: a number kept as text is not counted, nor is a number an answer would hide " +
            "(the answer says how many values were left out), and a hidden property can only be counted. " +
            "Rows come largest first. A partial answer says so: its numbers are then floors, not totals.",
            Schema($$"""
                { "type": "object", "required": ["query"], "properties": {
                    "query": { "type": "string", "description": "The select query." },
                    {{TimeProperties}}
                } }
                """))
            { TakesFreeFilter = true },

        new(Context, "One event and what surrounds it",
            "One event in full — every property, the whole exception with its stack trace — and the events of the same service " +
            "just before and after it, plus the events of the same trace when it has one. Takes the 'ref' of a row from logs_search.",
            Schema("""
                { "type": "object", "required": ["ref"], "properties": {
                    "ref": { "type": "string", "description": "The event's ref, exactly as a logs_search row gave it." },
                    "before": { "type": "integer", "minimum": 0, "description": "Events of the same service before it. Default 5." },
                    "after": { "type": "integer", "minimum": 0, "description": "Events of the same service after it. Default 5." },
                    "trace": { "type": "boolean", "description": "Also list the events of the same trace. Default true." }
                } }
                """)),

        new(Fields, "Properties a service logs",
            "The properties found in recent events, each with how often it occurs, its type, sample values, and whether it is hidden " +
            "(a hidden property can only be tested with = in a filter). Ends with a short reference of the filter syntax.",
            Schema($$"""
                { "type": "object", "properties": {
                    "service": { "type": "string", "description": "Only this service." },
                    {{TimeProperties}}
                } }
                """)),
    ];

    /// <summary>What a client is told about the server as a whole, once, before it uses any tool.</summary>
    public const string Instructions =
        """
        Ameto log tools. Events are structured: a message template (@mt) with {placeholders}, a level (@l: Verbose, Debug, Information, Warning, Error, Fatal), a service (@service), a timestamp (@t) and properties. Messages are not rendered: read the template next to the property columns.

        Workflow: logs_overview for the shape of a time window, logs_search to read events, logs_aggregate to count and group, logs_context for one event in full and what happened around it, logs_fields for the properties a service logs.

        Time: 'from' and 'to' take ISO-8601 or relative values (-15m, -2h, -1d, now). The default window is the last hour. All times are UTC.

        Filters use Seq syntax: @l = 'Error', StatusCode >= 500, @service in ['a', 'b'], has(UserId), @mt like '%timeout%', ci_contains(@x.message, 'refused'), Request.Path = '/pay', combined with and / or / not and parentheses. Plain words are looked for in the message template and in the exception, not in property values: to search a property, name it.

        Hidden values: some values are replaced by tokens such as h:91c0e2aa5d17. One value has the same token wherever it is hidden the same way, so tokens can be compared, counted and grouped. To find other events with that value, filter with the token itself: UserId = 'h:91c0e2aa5d17'. A token may only stand as the value in =, != or in, with a property (not a built-in field such as @service); where the value is found it is shown as the token again. A hidden property can only be tested with =, !=, in or has(). Do not try to work out the value behind a token.

        Answers are cut to a budget. When one ends with 'more', call again with the cursor it gives.
        """;

    /// <summary>The filter reference <c>logs_fields</c> ends with.</summary>
    public const string FilterReference =
        """
        filter syntax
          compare      @l = 'Error'   StatusCode >= 500   Elapsed > 2000   Name != 'x'
          lists        @l in ['Error', 'Fatal']   StatusCode not in [200, 204]
          presence     has(UserId)   not has(UserId)   has(@x) (the event carries an exception)
          text         @mt like '%timed out%'   ci_contains(@x.message, 'refused')   startsWith(Path, '/api')
          nested       Request.User.Id = 42   Headers['X-Forwarded-For'] = '10.0.0.1'
          logic        and  or  not  ( )
          built-in     @t  @l  @mt  @service  @tr  @sp  @x.type  @x.message  @x.stack
          functions    has() isDefined() startsWith() contains() endsWith() and their ci_ forms, length(), regexMatch(); no others here
          free text    plain words are looked for in the message template and the exception, not in property values; every word must appear
          hidden       a property marked "= only" takes =, !=, in and has(); a token (h:…) stands only as the value in =, != or in
          aggregation  select count(*) where @l = 'Error' group by @service limit 20   (logs_aggregate)
          strings are in single quotes; a quote inside one is doubled: 'can''t'
        """;

    public static ToolDefinition? Find(string name)
    {
        foreach (var tool in All)
            if (tool.Name.Equals(name, StringComparison.Ordinal)) return tool;
        return null;
    }

    private static JsonElement Schema(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }
}
