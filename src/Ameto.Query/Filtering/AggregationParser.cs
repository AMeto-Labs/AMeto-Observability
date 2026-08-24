namespace Ameto.Query.Filtering;

/// <summary>
/// Reads <c>select … [where …] [group by …] [limit n]</c>.
///
/// <para>None of <c>select</c>, <c>where</c>, <c>group</c>, <c>by</c>, <c>limit</c>, <c>as</c>,
/// <c>count</c>, <c>sum</c>, <c>min</c>, <c>max</c> or <c>avg</c> is a lexer keyword, and that is
/// deliberate. The lexer's keyword table is matched on the whole identifier, case-insensitively
/// and in every position, so promoting a word there takes it away as a property name
/// everywhere — which is exactly why <c>Values = 5</c> already cannot be written without the
/// bracket escape. <c>Count</c> and <c>Min</c> are ordinary names for a property to have. So
/// these words are recognised HERE, by position: <c>select</c> only as the first token,
/// <c>where</c>/<c>group</c>/<c>limit</c> only as clause heads after it, the function names only
/// inside the select list. Anywhere else they stay identifiers.</para>
/// </summary>
public static class AggregationParser
{
    /// <summary>Distinct groups an aggregation may accumulate before it stops adding new ones.</summary>
    public const int MaxGroups = 10_000;

    /// <summary>Columns one aggregation may compute. More than this is a mistake, not a table.</summary>
    public const int MaxAggregates = 16;

    /// <summary>Keys one aggregation may group by.</summary>
    public const int MaxGroupKeys = 4;

    /// <summary>
    /// Does this text ASK to be an aggregation? True only for <c>select</c> followed by an
    /// aggregate name and its opening parenthesis.
    ///
    /// <para>The bar is deliberately higher than the first word. <c>select</c> is not a lexer
    /// keyword and never was, and the search box's contract is that anything which is not an
    /// expression is free text — so <c>select the cheapest plan</c> is a perfectly ordinary
    /// search, and claiming it here would turn it into a parse error. Only the shape that
    /// cannot be anything else is claimed.</para>
    /// </summary>
    public static bool LooksLikeAggregation(string? text)
    {
        if (string.IsNullOrWhiteSpace(text)) return false;
        return LooksLikeAggregation(new Lexer(text).Tokenise());
    }

    private static bool LooksLikeAggregation(List<Token> tokens) =>
        tokens.Count >= 3
        && IsWord(tokens[0], "select")
        && tokens[1].Kind == TokenKind.Ident
        && Parser.IsAggregateName(tokens[1].Raw)
        && tokens[2].Kind == TokenKind.LParen;

    /// <summary>
    /// Parses an aggregation when the text unmistakably is one, for callers whose real job is
    /// something else — the search endpoint deciding where a query belongs, an alert rule
    /// refusing one. Returns false rather than throwing for everything else, so free text stays
    /// free text; once the text HAS claimed to be an aggregation, a mistake inside it is a
    /// <see cref="FormatException"/>.
    /// </summary>
    public static bool TryParse(string? text, out AggregationQuery? query)
    {
        query = null;
        if (string.IsNullOrWhiteSpace(text)) return false;

        var tokens = new Lexer(text).Tokenise();
        if (!LooksLikeAggregation(tokens)) return false;

        query = new Parser(tokens, text!).Parse();
        return true;
    }

    /// <summary>
    /// Parses text the caller has already decided should be an aggregation — the aggregation
    /// endpoint, where anything else is the caller's mistake and deserves to be named. Throws
    /// <see cref="FormatException"/> for everything that is not one, including a <c>select</c>
    /// followed by something that is not an aggregate.
    /// </summary>
    public static AggregationQuery Parse(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            throw new FormatException("Expected an aggregation, for example: select count(*) group by ['service.name'].");

        var tokens = new Lexer(text).Tokenise();
        if (tokens.Count == 0 || !IsWord(tokens[0], "select"))
            throw new FormatException(
                "Not an aggregation — it has to start with 'select', for example: " +
                "select count(*) group by ['service.name'].");

        return new Parser(tokens, text).Parse();
    }

    /// <summary>An identifier spelled <paramref name="word"/>, in any case.</summary>
    private static bool IsWord(Token t, string word) =>
        t.Kind == TokenKind.Ident && string.Equals(t.Raw, word, StringComparison.OrdinalIgnoreCase);

    private sealed class Parser(List<Token> tokens, string source)
    {
        private readonly List<Token> _t = tokens;
        private int _pos;

        private Token     Peek(int ahead = 0) => _pos + ahead < _t.Count ? _t[_pos + ahead] : new Token(TokenKind.Eof, "", source.Length);
        private TokenKind Kind(int ahead = 0) => Peek(ahead).Kind;
        private bool      AtWord(string w, int ahead = 0) => IsWord(Peek(ahead), w);

        public AggregationQuery Parse()
        {
            _pos++;                                  // 'select'
            var aggregates = ReadAggregates();

            // The clause heads, located BEFORE the where-clause is cut out. The filter grammar
            // would otherwise swallow them: a free-text where-clause collects bare words
            // greedily, so `where timeout group by X` would search for "group", "by" and "X".
            int whereStart = -1, whereEnd = _t.Count;
            if (AtWord("where"))
            {
                _pos++;
                whereStart = _pos;
                whereEnd   = FindClauseHead();
                _pos       = whereEnd;
            }

            var keys = ReadGroupBy();
            int limit = ReadLimit();

            if (Kind() != TokenKind.Eof)
                throw new FormatException(
                    $"Unexpected '{Peek().Raw}' at pos {Peek().Pos} — expected 'where', 'group by' or 'limit'.");

            string? filterText = null;
            if (whereStart >= 0)
            {
                filterText = SliceSource(whereStart, whereEnd);
                if (string.IsNullOrWhiteSpace(filterText))
                    throw new FormatException("'where' needs a filter expression after it.");
                CompiledFilter.Compile(filterText);   // report a bad filter here, not mid-scan
            }

            return new AggregationQuery
            {
                Aggregates = aggregates,
                Keys       = keys,
                FilterText = filterText,
                Limit      = limit,
            };
        }

        /// <summary>Index of the next top-level <c>group</c>/<c>limit</c>, or the end.</summary>
        private int FindClauseHead()
        {
            int depth = 0;
            for (int i = _pos; i < _t.Count; i++)
            {
                switch (_t[i].Kind)
                {
                    case TokenKind.LParen or TokenKind.LBracket: depth++; continue;
                    // Clamped at zero. Unclamped, one closer more than the filter opened drives
                    // this permanently negative, every later clause head is skipped, and the
                    // whole tail — `group by`, `limit` and all — is handed to the filter as
                    // text, where bare words read as free text and nothing errors.
                    case TokenKind.RParen or TokenKind.RBracket: if (depth > 0) depth--; continue;
                    case TokenKind.Eof: return i;
                }
                if (depth != 0) continue;
                // A clause head only where it can BE one. `limit` is a plausible property name
                // and an ordinary English word, so it heads a clause only in front of a number:
                // `where limit exceeded` and `where Limit = 10` stay filters.
                if (IsWord(_t[i], "limit") && i + 1 < _t.Count && _t[i + 1].Kind == TokenKind.Number) return i;
                if (IsWord(_t[i], "group") && IsWord(i + 1 < _t.Count ? _t[i + 1] : default, "by")) return i;
            }
            return _t.Count;
        }

        /// <summary>The original text spanned by a token range — the parser never rewrites the filter.</summary>
        private string SliceSource(int firstToken, int endToken)
        {
            if (firstToken >= _t.Count) return "";
            int start = _t[firstToken].Pos;
            int end   = endToken < _t.Count && _t[endToken].Kind != TokenKind.Eof
                      ? _t[endToken].Pos
                      : source.Length;
            return source[start..Math.Max(start, end)].Trim();
        }

        // ── select list ───────────────────────────────────────────────────────

        private List<AggregateSpec> ReadAggregates()
        {
            var list = new List<AggregateSpec>(2);
            while (true)
            {
                list.Add(ReadAggregate());
                // Bounded: every group allocates four arrays sized by the column count, and the
                // group cap is 10 000, so the two multiply. A query string fits about a
                // thousand repetitions of `sum(A),` — enough to commit hundreds of megabytes
                // from one request line.
                if (list.Count > MaxAggregates)
                    throw new FormatException($"At most {MaxAggregates} columns can be selected.");
                if (Kind() == TokenKind.Comma) { _pos++; continue; }
                break;
            }
            return list;
        }

        private AggregateSpec ReadAggregate()
        {
            var nameTok = Peek();
            if (nameTok.Kind != TokenKind.Ident || !TryKind(nameTok.Raw, out var kind))
                throw new FormatException(
                    $"Expected count, sum, min, max or avg at pos {nameTok.Pos}, not '{nameTok.Raw}'.");
            _pos++;

            if (Kind() != TokenKind.LParen)
                throw new FormatException($"'{nameTok.Raw}' needs its argument in parentheses at pos {Peek().Pos}.");
            _pos++;

            string? property = null;
            if (Kind() != TokenKind.RParen)
            {
                int before = _pos;
                property = FilterParser.ReadPathAt(_t, ref _pos);
                if (_pos == before)
                    throw new FormatException($"Expected a property inside {nameTok.Raw}() at pos {Peek().Pos}.");
            }
            // count() and count(*) are the same call: '*' is one of the characters the lexer
            // drops, so by the time we see the token list they are indistinguishable. Both are
            // accepted and mean "every event in the group".
            else if (kind != AggregateKind.Count)
            {
                throw new FormatException($"{nameTok.Raw}() needs a property to work on, at pos {Peek().Pos}.");
            }

            if (Kind() != TokenKind.RParen)
                throw new FormatException($"Expected ')' at pos {Peek().Pos}.");
            _pos++;

            return new AggregateSpec(kind, property, ReadAlias(DefaultAlias(kind, property)));
        }

        internal static bool IsAggregateName(string word) => TryKind(word, out _);

        /// <summary>A token the LEXER turned into one of the language's function keywords.</summary>
        private static bool IsFunctionName(Token t) => t.Kind is not (
            TokenKind.Ident or TokenKind.String or TokenKind.Number
            or TokenKind.True or TokenKind.False or TokenKind.Null
            or TokenKind.And or TokenKind.Or or TokenKind.Not or TokenKind.In or TokenKind.Like
            or TokenKind.Eq or TokenKind.Ne or TokenKind.Lt or TokenKind.Le or TokenKind.Gt or TokenKind.Ge
            or TokenKind.LParen or TokenKind.RParen or TokenKind.LBracket or TokenKind.RBracket
            or TokenKind.Comma or TokenKind.Dot or TokenKind.Eof);

        private static bool TryKind(string word, out AggregateKind kind)
        {
            switch (word.ToLowerInvariant())
            {
                case "count": kind = AggregateKind.Count; return true;
                case "sum":   kind = AggregateKind.Sum;   return true;
                case "min":   kind = AggregateKind.Min;   return true;
                case "max":   kind = AggregateKind.Max;   return true;
                case "avg":   kind = AggregateKind.Avg;   return true;
                default:      kind = default;             return false;
            }
        }

        private static string DefaultAlias(AggregateKind kind, string? property) =>
            property is null
                ? "count"
                : $"{kind.ToString().ToLowerInvariant()}({PropertyPath.ToDisplay(property)})";

        // ── group by ──────────────────────────────────────────────────────────

        private List<GroupKeySpec> ReadGroupBy()
        {
            if (!AtWord("group")) return [];
            if (!AtWord("by", 1))
                throw new FormatException($"Expected 'by' after 'group' at pos {Peek(1).Pos}.");
            _pos += 2;

            var keys = new List<GroupKeySpec>(2);
            while (true)
            {
                var tok = Peek();
                if (tok.Kind is not (TokenKind.Ident or TokenKind.LBracket))
                    // A property named after one of the language's functions — Bucket, Values,
                    // Keys, Length, Replace — lexes as that keyword wherever it appears, which
                    // is the same wart that stops `Values = 5` being written. The bracket form
                    // is the way out of it, and it is worth naming here rather than leaving the
                    // user to discover that their column is unusable.
                    throw new FormatException(
                        IsFunctionName(tok)
                            ? $"'{tok.Raw}' at pos {tok.Pos} is the name of a function in this language. " +
                              $"Write ['{tok.Raw}'] to group by a property of that name."
                            : $"Expected a property to group by at pos {tok.Pos}, not '{tok.Raw}'.");

                string path = FilterParser.ReadPathAt(_t, ref _pos);
                keys.Add(new GroupKeySpec(path, ReadAlias(PropertyPath.ToDisplay(path))));

                if (keys.Count > MaxGroupKeys)
                    throw new FormatException($"At most {MaxGroupKeys} keys can be grouped by.");
                if (Kind() == TokenKind.Comma) { _pos++; continue; }
                break;
            }
            return keys;
        }

        // ── as / limit ────────────────────────────────────────────────────────

        private string ReadAlias(string fallback)
        {
            if (!AtWord("as")) return fallback;
            _pos++;
            var tok = Peek();
            if (tok.Kind is not (TokenKind.Ident or TokenKind.String))
                throw new FormatException($"Expected a name after 'as' at pos {tok.Pos}.");
            _pos++;
            return tok.Raw;
        }

        private int ReadLimit()
        {
            if (!AtWord("limit")) return AggregationQuery.DefaultLimit;
            _pos++;
            var tok = Peek();
            if (tok.Kind != TokenKind.Number || !int.TryParse(tok.Raw, out int n) || n <= 0)
                throw new FormatException($"'limit' needs a positive whole number at pos {tok.Pos}.");
            _pos++;
            return Math.Min(n, AggregationParser.MaxGroups);
        }
    }
}
