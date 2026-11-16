using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Ameto.Query.Filtering;

namespace Ameto.Mcp.Tools;

/// <summary>One parameter of a published tool.</summary>
public sealed record McpToolParameter
{
    public const string String = "string", Number = "number", Boolean = "boolean", Enum = "enum";

    public required string Name { get; init; }

    /// <summary><c>string</c>, <c>number</c>, <c>boolean</c> or <c>enum</c>.</summary>
    public string Type { get; init; } = String;

    public bool Required { get; init; }

    public string Description { get; init; } = "";

    /// <summary>The values an <c>enum</c> parameter may take.</summary>
    public IReadOnlyList<string> Values { get; init; } = [];

    /// <summary>The value an optional parameter takes when the call leaves it out; null for none.</summary>
    public string? Default { get; init; }
}

/// <summary>
/// A published tool: a saved query the AI calls by name. An admin writes the filter and names
/// its parameters; the AI supplies the parameter values and nothing else. A value is bound into
/// the filter as a LITERAL — quoted and escaped by the server — so no value, whatever it
/// contains, can add a condition, close a parenthesis or name another field.
///
/// <para>This is what "tools created dynamically" means here: nothing is compiled or registered
/// per tool. The definition is a row in the database, <c>tools/list</c> is built from the rows a
/// profile is granted, and a tool saved in the UI is offered on the next list a client asks for.</para>
/// </summary>
public sealed record McpPublishedTool
{
    public required string Id   { get; init; }
    public required string Name { get; init; }

    /// <summary>What the tool is for, written for the AI that decides whether to call it.</summary>
    public string Description { get; init; } = "";

    public IReadOnlyList<McpToolParameter> Parameters { get; init; } = [];

    /// <summary>The filter, with <c>{name}</c> where a parameter's value goes.</summary>
    public string Filter { get; init; } = "";

    /// <summary>The properties an answer carries. Empty: all of them.</summary>
    public IReadOnlyList<string> Fields { get; init; } = [];

    public int  DefaultWindowMinutes { get; init; } = 60;
    public int  MaxEvents            { get; init; } = 200;
    public bool OldestFirst          { get; init; }

    /// <summary>False for a draft: kept and editable, offered to nobody.</summary>
    public bool Published { get; init; }

    /// <summary>The ids of the profiles that may call it.</summary>
    public IReadOnlyList<string> Profiles { get; init; } = [];

    public string         CreatedBy { get; init; } = "";
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset UpdatedAt { get; init; }

    public bool OfferedTo(McpProfile profile) => Published && Profiles.Contains(profile.Id, StringComparer.Ordinal);
}

/// <summary>
/// A filter with <c>{name}</c> placeholders, split into its top-level <c>and</c> clauses.
///
/// <para><b>Placeholders are read outside string literals only.</b> A message template is full of
/// braces — <c>@mt = 'Payment {PaymentId} declined'</c> is an ordinary filter — so a brace inside
/// quotes is text, always. A parameter therefore stands where a whole value stands
/// (<c>MerchantId = {merchantId}</c>, <c>startsWith(Path, {prefix})</c>), never inside one.</para>
///
/// <para><b>An optional parameter that is not given takes its clause with it.</b>
/// <c>MerchantId = {merchantId} and Reason = {reason}</c> called without <c>reason</c> runs as
/// <c>MerchantId = …</c>. That works clause by clause, which is why an optional parameter is not
/// allowed in a filter whose top level is an <c>or</c>: dropping "its clause" there would drop
/// the whole filter and return everything.</para>
/// </summary>
internal sealed partial class FilterTemplate
{
    private readonly record struct Piece(string Text, string? Placeholder);

    private readonly List<List<Piece>> _clauses = [];

    public IReadOnlyList<string> Placeholders { get; }

    /// <summary>True when an <c>or</c> stands at the top level, outside every parenthesis.</summary>
    public bool HasTopLevelOr { get; }

    [GeneratedRegex(@"^[A-Za-z][A-Za-z0-9_]{0,31}$", RegexOptions.CultureInvariant)]
    private static partial Regex Identifier();

    public static bool IsIdentifier(string name) => Identifier().IsMatch(name);

    public FilterTemplate(string text)
    {
        var names   = new List<string>();
        var clause  = new List<Piece>();
        var literal = new StringBuilder();
        int depth   = 0;
        bool topOr  = false;

        void FlushText()
        {
            if (literal.Length == 0) return;
            clause.Add(new Piece(literal.ToString(), null));
            literal.Clear();
        }
        void EndClause()
        {
            FlushText();
            if (clause.Count > 0) _clauses.Add(clause);
            clause = [];
        }

        for (int i = 0; i < text.Length; i++)
        {
            char c = text[i];

            if (c == '\'')
            {
                // A string literal, copied whole: '' and \x are the lexer's two escapes.
                int start = i++;
                while (i < text.Length)
                {
                    if (text[i] == '\\' && i + 1 < text.Length) { i += 2; continue; }
                    if (text[i] == '\'' && i + 1 < text.Length && text[i + 1] == '\'') { i += 2; continue; }
                    if (text[i] == '\'') break;
                    i++;
                }
                literal.Append(text, start, Math.Min(i, text.Length - 1) - start + 1);
                continue;
            }

            if (c is '(' or '[') depth++;
            else if (c is ')' or ']') depth = Math.Max(0, depth - 1);

            if (c == '{')
            {
                int close = text.IndexOf('}', i + 1);
                if (close > i && IsIdentifier(text[(i + 1)..close]))
                {
                    string name = text[(i + 1)..close];
                    FlushText();
                    clause.Add(new Piece("", name));
                    if (!names.Contains(name, StringComparer.Ordinal)) names.Add(name);
                    i = close;
                    continue;
                }
            }

            if (depth == 0 && IsKeywordAt(text, i, "and"))
            {
                EndClause();
                i += 2;
                continue;
            }
            if (depth == 0 && IsKeywordAt(text, i, "or")) topOr = true;

            literal.Append(c);
        }
        EndClause();

        Placeholders  = names;
        HasTopLevelOr = topOr;
    }

    private static bool IsKeywordAt(string text, int at, string keyword)
    {
        if (at + keyword.Length > text.Length) return false;
        if (string.Compare(text, at, keyword, 0, keyword.Length, StringComparison.OrdinalIgnoreCase) != 0) return false;
        bool before = at == 0 || char.IsWhiteSpace(text[at - 1]) || text[at - 1] is ')' or ']' or '\'';
        int  end    = at + keyword.Length;
        bool after  = end == text.Length || char.IsWhiteSpace(text[end]) || text[end] is '(' or '[';
        return before && after;
    }

    /// <summary>
    /// The filter with every placeholder replaced by <paramref name="literalOf"/>'s answer. A
    /// clause holding a placeholder it answers null for is left out; with every clause left
    /// out the result is null — no filter at all.
    /// </summary>
    public string? Bind(Func<string, string?> literalOf)
    {
        var bound = new List<string>(_clauses.Count);
        var sb    = new StringBuilder();
        foreach (var clause in _clauses)
        {
            sb.Clear();
            bool dropped = false;
            foreach (var piece in clause)
            {
                if (piece.Placeholder is null) { sb.Append(piece.Text); continue; }
                if (literalOf(piece.Placeholder) is not { } value) { dropped = true; break; }
                sb.Append(value);
            }
            if (dropped) continue;
            string text = sb.ToString().Trim();
            if (text.Length > 0) bound.Add(text);
        }
        return bound.Count == 0 ? null : string.Join(" and ", bound);
    }
}

/// <summary>Checks a published tool before it is saved, and turns a call's arguments into its filter.</summary>
internal static partial class PublishedToolBinder
{
    /// <summary>The arguments every published tool takes besides its own.</summary>
    private static readonly string[] Reserved = ["from", "to", "cursor"];

    [GeneratedRegex(@"^[a-z][a-z0-9_]{2,63}$", RegexOptions.CultureInvariant)]
    private static partial Regex ToolName();

    /// <summary>What is wrong with the definition, and which field it is about; null when nothing is.</summary>
    public static (string Error, string Field)? Validate(McpPublishedTool tool, IEnumerable<McpPublishedTool> others)
    {
        if (!ToolName().IsMatch(tool.Name))
            return ("A tool name is 3 to 64 characters: lowercase letters, digits and underscores, starting with a letter.", "name");
        if (BuiltInTools.Find(tool.Name) is not null)
            return ($"'{tool.Name}' is the name of a built-in tool.", "name");
        if (others.Any(o => o.Id != tool.Id && o.Name.Equals(tool.Name, StringComparison.Ordinal)))
            return ($"There is already a tool named '{tool.Name}'.", "name");
        if (string.IsNullOrWhiteSpace(tool.Description))
            return ("Describe what the tool answers: the AI chooses a tool by its description.", "description");
        if (tool.Description.Length > 2_000)
            return ("The description is at most 2 000 characters.", "description");
        if (tool.Parameters.Count > 16)
            return ("A tool takes at most 16 parameters.", "parameters");

        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var p in tool.Parameters)
        {
            if (!FilterTemplate.IsIdentifier(p.Name))
                return ($"'{p.Name}' is not a parameter name: letters, digits and underscores, starting with a letter.", "parameters");
            if (Reserved.Contains(p.Name, StringComparer.OrdinalIgnoreCase))
                return ($"Every tool already takes '{p.Name}'; choose another name.", "parameters");
            if (!seen.Add(p.Name))
                return ($"The parameter '{p.Name}' is declared twice.", "parameters");
            if (p.Type is not (McpToolParameter.String or McpToolParameter.Number or McpToolParameter.Boolean or McpToolParameter.Enum))
                return ($"The type of '{p.Name}' is string, number, boolean or enum.", "parameters");
            if (p.Type == McpToolParameter.Enum && p.Values.Count == 0)
                return ($"The enum parameter '{p.Name}' needs its allowed values.", "parameters");
            if (p.Default is not null && Literal(p, p.Default) is null)
                return ($"The default of '{p.Name}' is not a {p.Type} it accepts.", "parameters");
        }

        if (string.IsNullOrWhiteSpace(tool.Filter)) return ("The filter is empty.", "filter");
        var template = new FilterTemplate(tool.Filter);
        foreach (string name in template.Placeholders)
            if (!seen.Contains(name)) return ($"The filter uses {{{name}}}, which is not a declared parameter.", "filter");
        foreach (var p in tool.Parameters)
        {
            if (!template.Placeholders.Contains(p.Name, StringComparer.Ordinal))
                return ($"The parameter '{p.Name}' is not used in the filter. A parameter is written as {{{p.Name}}}, outside quotes.", "filter");
            if (template.HasTopLevelOr && !p.Required && p.Default is null)
                return ($"'{p.Name}' is optional, and the filter has an 'or' at its top level: leaving the parameter out would drop the whole filter. " +
                        "Make it required, give it a default, or put the alternatives in parentheses.", "filter");
        }

        // The filter has to be one the engine accepts, both with every parameter given and with
        // only the required ones.
        foreach (bool everything in (ReadOnlySpan<bool>)[true, false])
        {
            string? bound = template.Bind(name =>
            {
                var p = tool.Parameters.First(q => q.Name == name);
                return everything || p.Required || p.Default is not null ? Literal(p, Sample(p)) : null;
            });
            if (bound is null) continue;
            try
            {
                if (AggregationParser.TryParse(bound, out _)) return ("A tool's filter selects events; it cannot be a 'select …' aggregation.", "filter");
                CompiledFilter.Compile(bound);
            }
            catch (Exception e)
            {
                return ($"The filter does not parse{(everything ? "" : " when the optional parameters are left out")}: {e.Message}", "filter");
            }
        }

        if (tool.Fields.Any(string.IsNullOrWhiteSpace)) return ("A returned field has no name.", "fields");
        return null;
    }

    /// <summary>
    /// The tool's filter for one call, or the sentence that tells the AI which argument is wrong.
    /// </summary>
    public static (string? Filter, string? Error) Bind(McpPublishedTool tool, IReadOnlyDictionary<string, JsonElement> arguments)
    {
        foreach (string given in arguments.Keys)
            if (!Reserved.Contains(given, StringComparer.Ordinal) && !tool.Parameters.Any(p => p.Name == given))
                return (null, $"'{given}' is not a parameter of {tool.Name}. Its parameters: {Signature(tool)}.");

        string? error = null;
        string? filter = new FilterTemplate(tool.Filter).Bind(name =>
        {
            var p = tool.Parameters.First(q => q.Name == name);
            string? raw = arguments.TryGetValue(name, out var value) ? Text(value) : null;
            raw ??= p.Default;
            if (raw is null)
            {
                if (p.Required) error ??= $"'{name}' is required. Parameters of {tool.Name}: {Signature(tool)}.";
                return null;
            }
            if (Literal(p, raw) is { } literal) return literal;
            error ??= p.Type == McpToolParameter.Enum
                ? $"'{raw}' is not a value of '{name}'. Its values: {string.Join(", ", p.Values)}."
                : $"'{name}' takes a {p.Type}; '{raw}' is not one.";
            return null;
        });
        return error is null ? (filter, null) : (null, error);
    }

    /// <summary>
    /// An argument for every parameter, of its type: what a definition is tried with where the
    /// question is about the filter's shape rather than about any particular call.
    /// </summary>
    public static Dictionary<string, JsonElement> SampleArguments(McpPublishedTool tool)
    {
        var arguments = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        foreach (var p in tool.Parameters)
        {
            using var document = JsonDocument.Parse(string.Concat("\"", JsonEncodedText.Encode(Sample(p)).ToString(), "\""));
            arguments[p.Name] = document.RootElement.Clone();
        }
        return arguments;
    }

    /// <summary>The JSON Schema of the tool's arguments, as <c>tools/list</c> carries it.</summary>
    public static JsonElement InputSchema(McpPublishedTool tool)
    {
        var buffer = new System.Buffers.ArrayBufferWriter<byte>(512);
        using (var w = new Utf8JsonWriter(buffer))
        {
            w.WriteStartObject();
            w.WriteString("type", "object");
            if (tool.Parameters.Any(static p => p.Required))
            {
                w.WriteStartArray("required");
                foreach (var p in tool.Parameters) if (p.Required) w.WriteStringValue(p.Name);
                w.WriteEndArray();
            }
            w.WriteStartObject("properties");
            foreach (var p in tool.Parameters)
            {
                w.WriteStartObject(p.Name);
                w.WriteString("type", p.Type switch
                {
                    McpToolParameter.Number  => "number",
                    McpToolParameter.Boolean => "boolean",
                    _                        => "string",
                });
                if (p.Type == McpToolParameter.Enum)
                {
                    w.WriteStartArray("enum");
                    foreach (string value in p.Values) w.WriteStringValue(value);
                    w.WriteEndArray();
                }
                string description = p.Default is null ? p.Description : $"{p.Description} Default: {p.Default}.".TrimStart();
                if (description.Length > 0) w.WriteString("description", description);
                w.WriteEndObject();
            }
            Describe(w, "from", $"Start of the window: ISO-8601, or relative such as -15m, -2h, -1d. Default: {TimeWindow.Describe(TimeSpan.FromMinutes(tool.DefaultWindowMinutes))} before 'to'.");
            Describe(w, "to", "End of the window: ISO-8601, 'now', or relative. Default: now.");
            Describe(w, "cursor", "The cursor from the previous answer, to continue after it.");
            w.WriteEndObject();
            w.WriteEndObject();
        }
        using var document = JsonDocument.Parse(buffer.WrittenMemory);
        return document.RootElement.Clone();

        static void Describe(Utf8JsonWriter w, string name, string description)
        {
            w.WriteStartObject(name);
            w.WriteString("type", "string");
            w.WriteString("description", description);
            w.WriteEndObject();
        }
    }

    private static string Signature(McpPublishedTool tool) => tool.Parameters.Count == 0
        ? "none"
        : string.Join(", ", tool.Parameters.Select(static p => p.Required ? p.Name : p.Name + "?"));

    /// <summary>A value as a filter literal of the parameter's type; null when it is not one.</summary>
    private static string? Literal(McpToolParameter p, string raw)
    {
        raw = raw.Trim();
        switch (p.Type)
        {
            case McpToolParameter.Number:
                return double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out double n) && double.IsFinite(n)
                    ? n.ToString("R", CultureInfo.InvariantCulture)
                    : null;
            case McpToolParameter.Boolean:
                return bool.TryParse(raw, out bool b) ? (b ? "true" : "false") : null;
            case McpToolParameter.Enum:
                foreach (string allowed in p.Values)
                    if (allowed.Equals(raw, StringComparison.OrdinalIgnoreCase)) return FilterGuard.Quote(allowed);
                return null;
            default:
                return raw.Length is 0 or > 1_000 ? null : FilterGuard.Quote(raw);
        }
    }

    private static string Sample(McpToolParameter p) => p.Default ?? p.Type switch
    {
        McpToolParameter.Number  => "1",
        McpToolParameter.Boolean => "true",
        McpToolParameter.Enum    => p.Values[0],
        _                        => "x",
    };

    private static string? Text(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.String => value.GetString(),
        JsonValueKind.Number or JsonValueKind.True or JsonValueKind.False => value.GetRawText(),
        _ => null,
    };
}
