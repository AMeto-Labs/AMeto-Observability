using System.Globalization;
using System.Text;
using Ameto.Core;
using Ameto.Mcp.Masking;
using Ameto.Query.Filtering;

namespace Ameto.Mcp.Tools;

/// <summary>
/// What stands between a filter the AI wrote and the query engine.
///
/// <para><b>A filter is a question, and a question can be about a hidden value.</b>
/// <c>CardNumber like '4400%'</c> returns no card number — and tells, by whether anything came
/// back, that one starts with 4400; a few hundred such questions spell the number out. So a
/// field whose value never leaves as stored may be tested for equality only (<c>=</c>,
/// <c>!=</c>, <c>in</c>, <c>has()</c>): against a token from an earlier answer, or against a
/// value the person already has.</para>
///
/// <para><b>The filter that runs is written from a tree, never edited as text.</b> A filter is
/// parsed as it was written; the tree is judged; a token is replaced by the value it stands for
/// only where it is the operand of <c>=</c>, <c>!=</c> or <c>in</c>; and the text the engine is
/// given is written out from that tree and read back, and has to come back as the same tree.
/// An earlier version replaced tokens in the text with a regular expression, after the check:
/// a token inside another string moved the quotes, and what ran was a filter nobody had
/// checked. There is no text substitution here any more, and no place for one.</para>
///
/// <para><b>What a filter may be built from is a list, and the list is short.</b> The engine
/// has several dozen functions, and more than one of them reads a value by a road a field rule
/// does not name — <c>toJson(Customer)</c> reads <c>Customer.Iin</c>, <c>elementAt</c> reads a
/// child by a key given as an argument. A construct not on the list is refused rather than
/// guessed at; adding one means deciding which values it reads.</para>
///
/// <para><b>Plain words search the message template and the exception</b>, not the property
/// values. On the Logs page they search everything; here that would make every hidden value
/// answerable one substring at a time.</para>
/// </summary>
internal static class FilterGuard
{
    /// <summary>The service the AI's own calls are logged under; no AI query returns its events unless a profile names it.</summary>
    public const string AuditService = ReservedServices.AiAudit;

    /// <summary>The longest filter or query a call may carry: what the record of calls keeps whole.</summary>
    public const int MaxFilterChars = 4_000;

    /// <summary>
    /// How deep a filter may nest its parentheses. The parser and everything after it walk a
    /// filter by recursion, and a filter is text from outside: without a limit, a few thousand
    /// opening parentheses are a stack no thread has.
    /// </summary>
    public const int MaxNesting = 32;

    /// <summary>The most plain words one search takes: each is three tests on every event read.</summary>
    public const int MaxWords = 24;

    private const string Service = "@service", Level = "@l", Template = "@mt", TraceId = "@tr";
    private static readonly string ExceptionMessage = "@x" + PropertyPath.Separator + "message";
    private static readonly string ExceptionType    = "@x" + PropertyPath.Separator + "type";

    /// <summary>
    /// A filter as a tree ready to be written out for the engine — tokens resolved, plain words
    /// turned into predicates — or the sentence that says why it will not run. A filter that was
    /// empty is ready and has no tree.
    /// </summary>
    internal sealed record Prepared(FilterNode? Node, string? Error, RefusalKind Kind = RefusalKind.None)
    {
        /// <summary>The filter cannot be read: it does not parse, or it uses what a filter here may not.</summary>
        public static Prepared Unreadable(string error) => new(null, error, RefusalKind.Input);

        /// <summary>The filter reads well and asks what the masking rules do not answer.</summary>
        public static Prepared Hidden(string error) => new(null, error, RefusalKind.Masking);

        public static readonly Prepared Nothing = new(null, null);
    }

    // ── Search filters ────────────────────────────────────────────────────────

    /// <summary>Parses and judges a search filter, and resolves its tokens. An empty filter is ready as it is.</summary>
    public static Prepared Prepare(string? filter, MaskingPolicy policy, TokenVault vault)
    {
        if (string.IsNullOrWhiteSpace(filter)) return Prepared.Nothing;
        filter = filter.Trim();
        if (filter.Length > MaxFilterChars)
            return Prepared.Unreadable($"The filter is {filter.Length} characters long; a call may carry at most {MaxFilterChars}.");
        if (TooDeep(filter))
            return Prepared.Unreadable($"The filter nests parentheses more than {MaxNesting} deep.");

        try
        {
            if (AggregationParser.TryParse(filter, out _))
                return Prepared.Unreadable("This is an aggregation: ask logs_aggregate for it.");
        }
        catch (Exception e)
        {
            return Prepared.Unreadable($"Invalid aggregation: {e.Message}");
        }

        FilterNode root;
        try
        {
            CompiledFilter.Compile(filter);
            root = FilterParser.Parse(filter);
        }
        catch (Exception e)
        {
            return Prepared.Unreadable($"Invalid filter: {e.Message}");
        }

        if (root is MatchAllNode) return Prepared.Nothing;
        var judged = Judged(root, policy, vault);
        if (judged.Node is not { } ready) return judged;

        // Written out once here, so that a filter called ready is one that can be run: a value
        // the language has no spelling for is found now, and said to the caller, rather than
        // when the answer is being put together.
        try { Write(ready); }
        catch (InvalidOperationException)
        {
            return Prepared.Unreadable("The filter holds a name or a value this server cannot pass on exactly as written. Write it more simply.");
        }
        return new Prepared(ready, null);
    }

    /// <summary>
    /// The judgement of a tree that is already parsed: the tree that may run, or the refusal.
    /// Whatever the tree holds that the guard was not taught is refused — which is what the
    /// tests hold every node type of the language to.
    /// </summary>
    internal static Prepared Judged(FilterNode root, MaskingPolicy policy, TokenVault vault)
    {
        var judge = new Judge(policy, vault);
        return judge.Visit(root) is { } ready ? new Prepared(ready, null) : judge.Refusal!;
    }

    /// <summary>True when parentheses or brackets outside string literals nest deeper than <see cref="MaxNesting"/>.</summary>
    internal static bool TooDeep(ReadOnlySpan<char> text)
    {
        const char quote = '\'', escape = '\\';
        int depth = 0;
        for (int i = 0; i < text.Length; i++)
        {
            char c = text[i];
            if (c == quote)
            {
                // A string literal, skipped as the lexer reads one: a backslash takes the next
                // character with it, and two quotes are one.
                for (i++; i < text.Length; i++)
                {
                    if (text[i] == escape && i + 1 < text.Length) { i++; continue; }
                    if (text[i] != quote) continue;
                    if (i + 1 < text.Length && text[i + 1] == quote) { i++; continue; }
                    break;
                }
            }
            else if (c is '(' or '[') { if (++depth > MaxNesting) return true; }
            else if (c is ')' or ']') depth = Math.Max(0, depth - 1);
        }
        return false;
    }

    /// <summary>
    /// The same for a <c>select …</c> query, narrowed to what the profile may see. The columns
    /// and keys are the query's as written; only the where-clause is rebuilt — judged like a
    /// filter and joined to the profile's scope — so nothing a token stands for can reach a
    /// column heading. An aggregate other than a count may not be taken over a hidden field:
    /// <c>min(CardNumber)</c> is the card number.
    /// </summary>
    public static (AggregationQuery? Query, string? Error, RefusalKind Kind) PrepareAggregation(
        string? text, MaskingPolicy policy, TokenVault vault, McpProfile profile)
    {
        if (string.IsNullOrWhiteSpace(text))
            return (null, "'query' is empty. Example: select count(*) where @l = 'Error' group by @service", RefusalKind.Input);
        if (text.Length > MaxFilterChars)
            return (null, $"The query is {text.Length} characters long; a call may carry at most {MaxFilterChars}.", RefusalKind.Input);
        if (TooDeep(text))
            return (null, $"The query nests parentheses more than {MaxNesting} deep.", RefusalKind.Input);

        AggregationQuery asWritten;
        try { asWritten = AggregationParser.Parse(text); }
        catch (Exception e) { return (null, $"Invalid aggregation: {e.Message}", RefusalKind.Input); }

        foreach (var aggregate in asWritten.Aggregates)
            if (aggregate.Property is { } property && aggregate.Kind != AggregateKind.Count && IsHidden(policy, property))
                return (null, $"{PropertyPath.ToDisplay(property)} is a hidden field: it can be counted, not summed, averaged or ranged.",
                        RefusalKind.Masking);

        var where = Prepare(asWritten.FilterText, policy, vault);
        if (where.Error is not null) return (null, where.Error, where.Kind);

        return (new AggregationQuery
        {
            Aggregates = asWritten.Aggregates,
            Keys       = asWritten.Keys,
            FilterText = Compose(where.Node, profile),
            Limit      = Math.Clamp(asWritten.Limit, 1, 200),
            // A sum, a minimum, a maximum or an average is taken over numbers only. A string that
            // reads as a number is, more often than not, an identifier — a phone number, an IIN
            // with its leading zeros — and an aggregate would hand it back as a measurement, its
            // spelling gone and with it everything the detectors know it by.
            NumbersOnly = true,
        }, null, RefusalKind.None);
    }

    // ── What a profile may see ────────────────────────────────────────────────

    /// <summary>
    /// The text the engine runs: the prepared filter, the service the call named, and the
    /// profile's scope — its services, or, for a profile that sees every service, everything
    /// but the AI's own audit trail.
    /// </summary>
    public static string Compose(FilterNode? prepared, McpProfile profile, string? service = null)
    {
        FilterNode scope = profile.Services.Count == 0
            ? new CompareNode(Service, CompareOp.Ne, AuditService)
            : new InNode(Service, [.. profile.Services]);

        // One chain, leaning left: the filter, then the service, then the scope. Written out it
        // is `… and @service = '…' and @service in […]`, with the filter in parentheses when it
        // is an `or` — and that it reads back as exactly this is checked, like everything Write
        // writes: a scope that an `or` could slip out from under would be a different tree.
        FilterNode? node = prepared;
        if (service is not null) node = node is null ? Equal(Service, service) : new AndNode(node, Equal(Service, service));
        return Write(node is null ? scope : new AndNode(node, scope));
    }

    /// <summary><c>property = 'value'</c>, as a node.</summary>
    public static FilterNode Equal(string property, string value) => new CompareNode(property, CompareOp.Eq, value);

    /// <summary>The events of one trace.</summary>
    public static FilterNode InTrace(string traceId) => Equal(TraceId, traceId);

    /// <summary>The events that carry an exception.</summary>
    public static FilterNode HasException() => new HasNode("@x");

    /// <summary>True when the profile may ask about <paramref name="service"/> by name.</summary>
    public static bool MaySee(McpProfile profile, string service) => profile.Services.Count == 0
        ? !service.Equals(AuditService, StringComparison.OrdinalIgnoreCase)
        : profile.SeesService(service);

    /// <summary>True when the profile may be shown this event at all.</summary>
    public static bool InScope(McpProfile profile, LogEvent ev) => profile.Services.Count == 0
        ? !string.Equals(ev.ServiceName, AuditService, StringComparison.OrdinalIgnoreCase)
        : profile.SeesService(ev.ServiceName);

    /// <summary><paramref name="value"/> as a string literal of the filter language.</summary>
    public static string Quote(string value)
    {
        var sb = new StringBuilder(value.Length + 2);
        WriteString(sb, value);
        return sb.ToString();
    }

    /// <summary>
    /// A number as the filter language spells one: digits and a point, never an exponent — the
    /// lexer has no reading of <c>1E-05</c>. Null for a value that is not a finite number.
    /// </summary>
    public static string? NumberLiteral(string text)
    {
        var sb = new StringBuilder(24);
        return TryNumber(text, out object? number) && TryWriteNumber(sb, number!) ? sb.ToString() : null;
    }

    /// <summary>True when a path names a field whose value never leaves as stored. A built-in field is never one.</summary>
    public static bool IsHidden(MaskingPolicy policy, string encodedPath) =>
        !BuiltinFields.TryResolve(encodedPath, out _)
        && policy.HidesField(PropertyPath.ToDisplay(PropertyPath.StripIndexSegments(encodedPath)));

    // ── Judging a tree ────────────────────────────────────────────────────────

    /// <summary>
    /// Walks a filter as it was written and answers with the tree that may run, or with null
    /// and a <see cref="Refusal"/>. A node type it does not know is refused: this is the list of
    /// what an AI's filter may be built from, and each entry is here because it is known which
    /// values it reads — the value at its path, or the elements of a list there, and nothing else.
    /// </summary>
    private sealed class Judge(MaskingPolicy policy, TokenVault vault)
    {
        public Prepared? Refusal { get; private set; }

        public FilterNode? Visit(FilterNode node)
        {
            switch (node)
            {
                case AndNode and:
                    return Visit(and.Left) is { } al && Visit(and.Right) is { } ar ? new AndNode(al, ar) : null;
                case OrNode or:
                    return Visit(or.Left) is { } ol && Visit(or.Right) is { } rr ? new OrNode(ol, rr) : null;
                case NotNode not:
                    return Visit(not.Operand) is { } operand ? new NotNode(operand) : null;

                case CompareNode compare:
                    return Compare(compare);

                case InNode list:
                {
                    // `in` is equality against each of several values: a token is at home in the list.
                    var values = new object?[list.Values.Length];
                    for (int i = 0; i < values.Length; i++)
                        if (!Operand(list.Values[i], out values[i])) return null;
                    return new InNode(list.Property, values);
                }

                // Presence tells nothing an answer does not: a hidden value is shown as a token.
                case HasNode or IsDefinedNode:
                    return node;

                // The bare word `Error`. Written out as the comparison it means, so that what is
                // written reads back as what was meant.
                case LevelNode level:
                    return new CompareNode(Level, CompareOp.Eq, level.Level.ToSeqString());

                case LikeNode like:
                    return Reads(like.Property, like.Pattern) ? like : null;
                case StartsWithNode starts:
                    return Reads(starts.Property, starts.Prefix) ? starts : null;
                case ContainsNode contains:
                    return Reads(contains.Property, contains.Text) ? contains : null;
                case EndsWithNode ends:
                    return Reads(ends.Property, ends.Suffix) ? ends : null;
                case RegexMatchNode regex:
                    return Reads(regex.Property, regex.Pattern) ? regex : null;
                case LengthCompareNode length:
                    return Reads(length.Property, length.Value) ? length : null;

                case FreeTextNode words:
                    return Words(words);

                case MatchAllNode:
                    return Refuse(Prepared.Unreadable("The filter has an empty clause."));

                default:
                    return Refuse(Prepared.Unreadable(
                        $"{Name(node)} is not available here. A filter may use =, !=, <, <=, >, >=, in, like, has(), isDefined(), " +
                        "startsWith(), contains(), endsWith() and their ci_ forms, length(), regexMatch(), and plain words."));
            }
        }

        private FilterNode? Compare(CompareNode compare)
        {
            if (compare.RightProperty is { } right)
            {
                // Two fields compared with each other: whether they are equal is something about
                // both values, so neither may be a hidden one.
                if (IsHidden(policy, compare.Property)) return Refuse(Prepared.Hidden(HiddenField(compare.Property)));
                if (IsHidden(policy, right))            return Refuse(Prepared.Hidden(HiddenField(right)));
                return compare;
            }

            bool equality = compare.Op is CompareOp.Eq or CompareOp.Ne;
            if (IsToken(compare.Value, out _))
            {
                // `Elapsed > 'h:…'` would order the hidden value against a visible one, and a
                // few such questions are a binary search for it.
                if (!equality) return Refuse(Prepared.Hidden(TokenMisplaced(compare.Value!)));
                return Operand(compare.Value, out object? value) ? new CompareNode(compare.Property, compare.Op, value) : null;
            }
            if (!equality && IsHidden(policy, compare.Property)) return Refuse(Prepared.Hidden(HiddenField(compare.Property)));
            return compare;
        }

        /// <summary>A predicate that reads the text or the size of a value: not for a hidden field, and not with a token.</summary>
        private bool Reads(string property, object? operand)
        {
            if (IsToken(operand, out _)) { Refuse(Prepared.Hidden(TokenMisplaced(operand!))); return false; }
            if (IsHidden(policy, property)) { Refuse(Prepared.Hidden(HiddenField(property))); return false; }
            return true;
        }

        /// <summary>The operand of an equality: itself, or — for a token — the value it stands for.</summary>
        private bool Operand(object? written, out object? value)
        {
            value = written;
            if (!IsToken(written, out ulong bits)) return true;
            if (!vault.TryResolve(bits, out var entry))
            {
                Refuse(Prepared.Hidden(
                    $"{written} is not a token this server remembers. Tokens can be searched for a limited time after an answer contained them: " +
                    "run the search that produced it again."));
                return false;
            }
            // A number is put back as a number, so that it equals the number it was read from.
            value = entry.IsNumber && TryNumber(entry.Text, out object? number) ? number : entry.Text;
            return true;
        }

        /// <summary>
        /// Plain words: each has to occur in the message template or in the exception. Not in a
        /// property value — there a word would be a question about every value, hidden ones included.
        /// </summary>
        private FilterNode? Words(FreeTextNode words)
        {
            if (words.Terms.Length > MaxWords)
                return Refuse(Prepared.Unreadable(
                    $"{words.Terms.Length} plain words are more than one search takes ({MaxWords}). " +
                    "Search for the few that matter, or write an expression such as ci_contains(@mt, 'the phrase')."));

            FilterNode? all = null;
            foreach (string term in words.Terms)
            {
                if (IsToken(term, out _)) return Refuse(Prepared.Hidden(TokenMisplaced(term)));
                FilterNode one = new OrNode(
                    new OrNode(new ContainsNode(Template, term, ci: true), new ContainsNode(ExceptionMessage, term, ci: true)),
                    new ContainsNode(ExceptionType, term, ci: true));
                all = all is null ? one : new AndNode(all, one);
            }
            return all ?? Refuse(Prepared.Unreadable("The filter has an empty clause."));
        }

        private FilterNode? Refuse(Prepared refusal)
        {
            Refusal ??= refusal;
            return null;
        }
    }

    private static bool IsToken(object? value, out ulong bits)
    {
        bits = 0;
        return value is string text && MaskToken.TryParse(text, out bits);
    }

    private static string HiddenField(string property) =>
        $"{PropertyPath.ToDisplay(property)} is a hidden field: a filter may only test it with =, !=, in or has(). " +
        "Compare it with a token from an earlier answer (for example 'h:91c0e2aa5d17') or with a value you already have.";

    private static string TokenMisplaced(object token) =>
        $"{token} is a token for a hidden value: it may only stand as the value in =, != or in " +
        "(for example CardNumber = 'h:91c0e2aa5d17'), never in a comparison of order, a pattern or plain words.";

    /// <summary>A node type as a person would name the construct: <c>ToJsonCompareNode</c> is <c>toJson()</c>.</summary>
    private static string Name(FilterNode node)
    {
        string type = node.GetType().Name;
        foreach (string suffix in (ReadOnlySpan<string>)["CompareNode", "PredicateNode", "Node"])
            if (type.EndsWith(suffix, StringComparison.Ordinal)) { type = type[..^suffix.Length]; break; }
        return type.Length == 0 ? "This construct" : char.ToLowerInvariant(type[0]) + type[1..] + "()";
    }

    // ── Writing a tree out ────────────────────────────────────────────────────

    /// <summary>
    /// The tree as text the engine parses — checked by parsing it: the text has to read back as
    /// this tree, value for value. A path is written bare where it can be and in brackets where
    /// a bare spelling would be read as something else (a property called <c>length</c>); if
    /// neither reads back, the tree is not run.
    /// </summary>
    internal static string Write(FilterNode node)
    {
        string wanted = Describe(node);
        foreach (bool bracketed in (ReadOnlySpan<bool>)[false, true])
        {
            var sb = new StringBuilder(128);
            Write(sb, node, bracketed);
            string text = sb.ToString();
            FilterNode back;
            try
            {
                CompiledFilter.Compile(text);
                back = FilterParser.Parse(text);
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                continue;   // this spelling does not read at all; the other may
            }
            if (string.Equals(Describe(back), wanted, StringComparison.Ordinal)) return text;
        }
        throw new InvalidOperationException("A filter could not be written out as the tree it was checked as.");
    }

    private static void Write(StringBuilder sb, FilterNode node, bool bracketed)
    {
        switch (node)
        {
            case AndNode or OrNode: Chain(sb, node, bracketed); break;
            case NotNode not:
                sb.Append("not ");
                Member(sb, not.Operand, bracketed);
                break;

            case CompareNode compare:
                WritePath(sb, compare.Property, bracketed);
                sb.Append(Operator(compare.Op));
                if (compare.RightProperty is { } right) WritePath(sb, right, bracketed);
                else WriteLiteral(sb, compare.Value);
                break;

            case InNode list:
                WritePath(sb, list.Property, bracketed);
                sb.Append(" in [");
                for (int i = 0; i < list.Values.Length; i++)
                {
                    if (i > 0) sb.Append(", ");
                    WriteLiteral(sb, list.Values[i]);
                }
                sb.Append(']');
                break;

            case HasNode has:           Call(sb, "has", has.Property, bracketed).Append(')'); break;
            case IsDefinedNode defined: Call(sb, "isDefined", defined.Property, bracketed).Append(')'); break;

            case LikeNode like:
                WritePath(sb, like.Property, bracketed);
                sb.Append(" like ");
                WriteString(sb, like.Pattern);
                break;

            case StartsWithNode starts:
                WriteString(Call(sb, starts.CaseInsensitive ? "ci_startsWith" : "startsWith", starts.Property, bracketed).Append(", "), starts.Prefix);
                sb.Append(')');
                break;
            case ContainsNode contains:
                WriteString(Call(sb, contains.CaseInsensitive ? "ci_contains" : "contains", contains.Property, bracketed).Append(", "), contains.Text);
                sb.Append(')');
                break;
            case EndsWithNode ends:
                WriteString(Call(sb, ends.CaseInsensitive ? "ci_endsWith" : "endsWith", ends.Property, bracketed).Append(", "), ends.Suffix);
                sb.Append(')');
                break;
            case RegexMatchNode regex:
                WriteString(Call(sb, "regexMatch", regex.Property, bracketed).Append(", "), regex.Pattern);
                sb.Append(')');
                break;

            case LengthCompareNode length:
                Call(sb, "length", length.Property, bracketed).Append(')').Append(Operator(length.Op));
                WriteLiteral(sb, length.Value);
                break;

            default:
                throw new InvalidOperationException($"{node.GetType().Name} is not a node a prepared filter holds.");
        }
    }

    /// <summary>
    /// A run of <c>and</c>s, or of <c>or</c>s, written flat: <c>a and b and c</c>. The parser
    /// reads that as <c>(a and b) and c</c> — a chain that leans left, which is how every chain
    /// it builds leans, and how the scope and the words of a search are joined here — so such a
    /// chain needs no parenthesis of its own. Only a member that is itself an <c>and</c> or an
    /// <c>or</c> gets one.
    ///
    /// <para>What this buys is depth. Every parenthesis is a level of recursion for the parser
    /// that reads the text back and for the engine's own; a filter of three hundred clauses has
    /// to be three hundred clauses long, not three hundred parentheses deep.</para>
    /// </summary>
    private static void Chain(StringBuilder sb, FilterNode node, bool bracketed)
    {
        bool and = node is AndNode;
        var members = new List<FilterNode>(4);          // the left spine, last member first
        for (FilterNode at = node; ; )
        {
            var (left, right) = at is AndNode a ? (a.Left, a.Right) : (((OrNode)at).Left, ((OrNode)at).Right);
            members.Add(right);
            if (and ? left is AndNode : left is OrNode) { at = left; continue; }
            members.Add(left);
            break;
        }
        for (int i = members.Count - 1; i >= 0; i--)
        {
            if (i < members.Count - 1) sb.Append(and ? " and " : " or ");
            Member(sb, members[i], bracketed);
        }
    }

    /// <summary>One side of an <c>and</c>, an <c>or</c> or a <c>not</c>: in parentheses when it is itself a pair.</summary>
    private static void Member(StringBuilder sb, FilterNode node, bool bracketed)
    {
        if (node is not (AndNode or OrNode))
        {
            Write(sb, node, bracketed);
            return;
        }
        sb.Append('(');
        Write(sb, node, bracketed);
        sb.Append(')');
    }

    private static StringBuilder Call(StringBuilder sb, string function, string property, bool bracketed)
    {
        sb.Append(function).Append('(');
        WritePath(sb, property, bracketed);
        return sb;
    }

    private static string Operator(CompareOp op) => op switch
    {
        CompareOp.Eq => " = ",
        CompareOp.Ne => " != ",
        CompareOp.Lt => " < ",
        CompareOp.Le => " <= ",
        CompareOp.Gt => " > ",
        _            => " >= ",
    };

    /// <summary>
    /// An encoded path as the grammar reads one: <c>Customer.Iin</c>, <c>Items[0].Sku</c>,
    /// <c>Headers['X-Forwarded-For']</c>. With <paramref name="bracketed"/> every name is
    /// written in brackets, which no keyword can be mistaken for.
    /// </summary>
    private static void WritePath(StringBuilder sb, string encoded, bool bracketed)
    {
        bool first = true;
        foreach (var range in encoded.AsSpan().Split(PropertyPath.Separator))
        {
            ReadOnlySpan<char> segment = encoded.AsSpan(range);
            if (PropertyPath.IsIndexSegment(segment))
            {
                ReadOnlySpan<char> index = PropertyPath.SegmentValue(segment);
                if (index.IsEmpty || index.ContainsAnyExceptInRange('0', '9'))
                    throw new InvalidOperationException("A path's index is not a whole number.");
                sb.Append('[').Append(index).Append(']');
            }
            else if (!bracketed && IsBareName(segment))
            {
                if (!first) sb.Append('.');
                sb.Append(segment);
            }
            else
            {
                sb.Append('[');
                WriteString(sb, segment);
                sb.Append(']');
            }
            first = false;
        }
    }

    /// <summary>A name the lexer reads as one identifier: a letter, <c>_</c> or <c>@</c>, then letters, digits, <c>_</c> and <c>@</c>.</summary>
    private static bool IsBareName(ReadOnlySpan<char> segment)
    {
        if (segment.IsEmpty || !(segment[0] is '@' or '_' || char.IsLetter(segment[0]))) return false;
        foreach (char c in segment)
            if (!(c is '@' or '_' || char.IsLetterOrDigit(c))) return false;
        return true;
    }

    private static void WriteLiteral(StringBuilder sb, object? value)
    {
        switch (value)
        {
            case null:     sb.Append("null"); break;
            case bool b:   sb.Append(b ? "true" : "false"); break;
            case string s: WriteString(sb, s); break;
            default:
                if (!TryWriteNumber(sb, value))
                    throw new InvalidOperationException($"A {value.GetType().Name} is not a value a filter can hold.");
                break;
        }
    }

    /// <summary>A string literal. The lexer takes a backslash as "the next character as it is".</summary>
    private static void WriteString(StringBuilder sb, ReadOnlySpan<char> value)
    {
        sb.Append('\'');
        foreach (char c in value)
        {
            if (c is '\\' or '\'') sb.Append('\\');
            sb.Append(c);
        }
        sb.Append('\'');
    }

    private static bool TryWriteNumber(StringBuilder sb, object value)
    {
        switch (value)
        {
            case long l: sb.Append(l.ToString(CultureInfo.InvariantCulture)); return true;
            case int i:  sb.Append(i.ToString(CultureInfo.InvariantCulture)); return true;
            case double d when double.IsFinite(d):
                sb.Append(Fixed(d));
                return true;
            default: return false;
        }
    }

    /// <summary>
    /// A double in fixed notation with a point in it — the point is what makes the lexer's
    /// number a double, and it has no reading of an exponent. The digits are the shortest that
    /// read back as the same double, with the exponent spelled out as zeros:
    /// <c>1E-07</c> is <c>0.0000001</c>, <c>1.5E+20</c> is <c>150000000000000000000.0</c>.
    /// </summary>
    private static string Fixed(double d)
    {
        string shortest = d.ToString("R", CultureInfo.InvariantCulture);
        int e = shortest.IndexOf('E');
        if (e < 0) return shortest.Contains('.') ? shortest : shortest + ".0";

        int sign = shortest[0] == '-' ? 1 : 0;
        ReadOnlySpan<char> mantissa = shortest.AsSpan(sign, e - sign);
        int exponent = int.Parse(shortest.AsSpan(e + 1), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
        int dot      = mantissa.IndexOf('.');
        string digits = dot < 0 ? mantissa.ToString() : string.Concat(mantissa[..dot], mantissa[(dot + 1)..]);
        int point     = (dot < 0 ? mantissa.Length : dot) + exponent;   // digits before the point

        var sb = new StringBuilder(digits.Length + Math.Abs(exponent) + 4);
        if (sign == 1) sb.Append('-');
        if (point <= 0)                  sb.Append("0.").Append('0', -point).Append(digits);
        else if (point >= digits.Length) sb.Append(digits).Append('0', point - digits.Length).Append(".0");
        else                             sb.Append(digits, 0, point).Append('.').Append(digits, point, digits.Length - point);
        return sb.ToString();
    }

    /// <summary>A number's text as the value the parser would have made of it: a whole number, else a double.</summary>
    private static bool TryNumber(string text, out object? number)
    {
        number = null;
        text = text.Trim();
        if (long.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out long whole))
        {
            number = whole;
            return true;
        }
        if (double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double real) && double.IsFinite(real))
        {
            number = real;
            return true;
        }
        return false;
    }

    // ── Comparing two trees ───────────────────────────────────────────────────

    /// <summary>
    /// A tree as one line that says everything about it: every node, path, operator and value,
    /// each value with its type and its length. Two trees are the same exactly when their lines
    /// are — which is how the text written for the engine is held to the tree that was judged.
    /// </summary>
    internal static string Describe(FilterNode node)
    {
        var sb = new StringBuilder(128);
        Describe(sb, node, values: true);
        return sb.ToString();
    }

    /// <summary>
    /// The same line with every value left out: the SHAPE of a filter — its nodes, paths and
    /// operators, and where a value stands. Two bindings of one published tool differ in their
    /// values and in nothing else, and this is how that is held to be so.
    /// </summary>
    internal static string Shape(FilterNode? node)
    {
        if (node is null) return "";
        var sb = new StringBuilder(128);
        Describe(sb, node, values: false);
        return sb.ToString();
    }

    private static void Describe(StringBuilder sb, FilterNode node, bool values)
    {
        switch (node)
        {
            case AndNode and: sb.Append("and("); Describe(sb, and.Left, values); sb.Append(','); Describe(sb, and.Right, values); sb.Append(')'); break;
            case OrNode or:   sb.Append("or(");  Describe(sb, or.Left, values);  sb.Append(','); Describe(sb, or.Right, values);  sb.Append(')'); break;
            case NotNode not: sb.Append("not("); Describe(sb, not.Operand, values); sb.Append(')'); break;

            // A path is always part of the shape; only what stands where a value stands is left out.
            case CompareNode compare:
                Text(sb.Append("cmp("), compare.Property).Append(compare.Op).Append(',');
                if (compare.RightProperty is { } right) Text(sb.Append("path:"), right);
                else Value(sb, compare.Value, values);
                sb.Append(')');
                break;

            case InNode list:
                Text(sb.Append("in("), list.Property);
                foreach (object? value in list.Values) Value(sb.Append(','), value, values);
                sb.Append(')');
                break;

            case HasNode has:           Text(sb.Append("has("), has.Property).Append(')'); break;
            case IsDefinedNode defined: Text(sb.Append("defined("), defined.Property).Append(')'); break;
            case LikeNode like:         Value(Text(sb.Append("like("), like.Property), like.Pattern, values); sb.Append(')'); break;
            case StartsWithNode starts: Value(Text(sb.Append("starts("), starts.Property), starts.Prefix, values); sb.Append(starts.CaseInsensitive).Append(')'); break;
            case ContainsNode contains: Value(Text(sb.Append("contains("), contains.Property), contains.Text, values); sb.Append(contains.CaseInsensitive).Append(')'); break;
            case EndsWithNode ends:     Value(Text(sb.Append("ends("), ends.Property), ends.Suffix, values); sb.Append(ends.CaseInsensitive).Append(')'); break;
            case RegexMatchNode regex:  Value(Text(sb.Append("regex("), regex.Property), regex.Pattern, values); sb.Append(regex.IgnoreCase).Append(')'); break;

            case LengthCompareNode length:
                Text(sb.Append("length("), length.Property).Append(length.Op).Append(',');
                Value(sb, length.Value, values);
                sb.Append(')');
                break;

            // Anything else has no line of its own, so no tree that holds one equals another.
            default: sb.Append('?').Append(node.GetType().Name).Append('#').Append(Guid.NewGuid().ToString("N")); break;
        }
    }

    /// <summary>A text with its length in front, so that no text can be read as the end of another.</summary>
    private static StringBuilder Text(StringBuilder sb, string text) => sb.Append(text.Length).Append(':').Append(text).Append(';');

    private static void Value(StringBuilder sb, object? value, bool written)
    {
        if (written) Value(sb, value);
        else sb.Append('?');
    }

    private static void Value(StringBuilder sb, object? value)
    {
        switch (value)
        {
            case null:     sb.Append("null"); break;
            case bool b:   sb.Append(b ? "true" : "false"); break;
            case string s: Text(sb.Append("s:"), s); break;
            case long l:   sb.Append("n:").Append(l.ToString(CultureInfo.InvariantCulture)); break;
            case int i:    sb.Append("n:").Append(i.ToString(CultureInfo.InvariantCulture)); break;
            case double d: sb.Append("d:").Append(d.ToString("R", CultureInfo.InvariantCulture)); break;
            default:       sb.Append('?').Append(value.GetType().Name); break;
        }
    }
}
