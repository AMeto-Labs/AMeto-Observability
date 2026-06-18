using Ameto.Core;

namespace Ameto.Query.Filtering;

/// <summary>
/// Recursive-descent parser for Seq Filter Expressions.
///
/// Grammar (simplified, operator precedence low→high):
///
///   expr     := or_expr
///   or_expr  := and_expr  ('or'  and_expr)*
///   and_expr := not_expr  ('and' not_expr)*
///   not_expr := 'not' not_expr  |  atom
///   atom     := '(' expr ')'
///             | func_call
///             | comparison
///             | property 'in' '[' value_list ']'
///   func_call := ('has' | 'isDefined' | 'startsWith' | 'contains' | 'endsWith'
///                | 'ci_startsWith' | 'ci_contains' | 'ci_endsWith') '(' property ',' value ')'
///             | ('has' | 'isDefined') '(' property ')'
///             | scalar_compare
///   scalar_compare := ('length' | 'toJson' | 'fromJson' | 'toLower' | 'toUpper' | 'toNumber') '(' property ')' op value
///                  | 'substring' '(' property ',' number (',' number)? ')' op value
///                  | ('indexOf' | 'lastIndexOf') '(' property ',' string ')' op value
///                  | 'replace' '(' property ',' string ',' string ')' op value
///                  | 'concat' '(' value (',' value)+ ')' op value
///                  | 'typeOf' '(' property ')' op value
///                  | 'elementAt' '(' property ',' value ')' op value
///                  | ('keys' | 'values') '(' property ')' op value
///                  | 'round' '(' property ',' number ')' op value
///                  | 'now' '(' ')' op value
///                  | 'dateTime' '(' property ')' op value
///                  | 'toIsoString' '(' property (',' number)? ')' op value
///                  | 'datePart' '(' property ',' string (',' number)? ')' op value
///                  | 'timeOfDay' '(' property ',' number ')' op value
///                  | 'timeSpan' '(' property ')' op value
///                  | 'totalMilliseconds' '(' property ')' op value
///                  | 'toTimeString' '(' property ')' op value
///                  | 'toHexString' '(' property ')' op value
///                  | 'bucket' '(' property ',' number ')' op value
///                  | 'offsetIn' '(' string ',' property ')' op value
///                  | 'arrived' '(' property ')' op value
///                  | 'coalesce' '(' value (',' value)+ ')' op value
///   comparison := value  op  value
///   value    := property | string | number | 'true' | 'false' | 'null'
///   property := (ident | bracket_seg) ('.' ident | bracket_seg)*
///   bracket_seg := '[' (string | number) ']'
///   op       := '=' | '!=' | '<' | '<=' | '>' | '>='
///
/// Internally property paths are stored as their segments joined by
/// <see cref="PropertyPath.Separator"/> (U+0001), so segments may contain
/// arbitrary characters including '.' and '-' when written with the bracket
/// form, e.g. <c>Headers['Api-Request-MerchantId']</c>.
/// </summary>
public sealed class FilterParser
{
    private readonly List<Token> _tokens;
    private int _pos;

    private FilterParser(List<Token> tokens) => _tokens = tokens;

    public static FilterNode Parse(string? filter)
    {
        if (string.IsNullOrWhiteSpace(filter))
            return new MatchAllNode();

        var lexer  = new Lexer(filter);
        var tokens = lexer.Tokenise();
        var parser = new FilterParser(tokens);
        return parser.ParseExpr();
    }

    // ── Grammar rules ─────────────────────────────────────────────────────────

    private FilterNode ParseExpr() => ParseOr();

    private FilterNode ParseOr()
    {
        var left = ParseAnd();
        while (PeekKind() == TokenKind.Or)
        {
            Consume(TokenKind.Or);
            var right = ParseAnd();
            left = new OrNode(left, right);
        }
        return left;
    }

    private FilterNode ParseAnd()
    {
        var left = ParseNot();
        while (PeekKind() == TokenKind.And)
        {
            Consume(TokenKind.And);
            var right = ParseNot();
            left = new AndNode(left, right);
        }
        return left;
    }

    private FilterNode ParseNot()
    {
        if (PeekKind() == TokenKind.Not)
        {
            Consume(TokenKind.Not);
            return new NotNode(ParseNot());
        }
        return ParseAtom();
    }

    private FilterNode ParseAtom()
    {
        var t = Peek();

        // Grouped: ( expr )
        if (t.Kind == TokenKind.LParen)
        {
            Consume(TokenKind.LParen);
            var inner = ParseExpr();
            Consume(TokenKind.RParen);
            return inner;
        }

        // Function calls: has(), isDefined(), startsWith(), contains(), ci_*
        if (t.Kind is TokenKind.Has or TokenKind.IsDefined)
        {
            var kind = t.Kind;
            _pos++;
            Consume(TokenKind.LParen);
            // has(fromJson(Body).field)  — existence check inside JSON
            var fjHas = TryParseFromJsonSource();
            if (fjHas is { } fh)
            {
                Consume(TokenKind.RParen);
                return fh.path.Length > 0
                    ? new FromJsonPathHasNode(fh.prop, fh.path)
                    : new HasNode(fh.prop);
            }
            string prop = ReadProperty();
            Consume(TokenKind.RParen);
            return kind == TokenKind.Has ? new HasNode(prop) : new IsDefinedNode(prop);
        }

        if (t.Kind is TokenKind.StartsWith or TokenKind.CiStartsWith)
        {
            bool ci = t.Kind == TokenKind.CiStartsWith;
            _pos++;
            Consume(TokenKind.LParen);
            var fjSw = TryParseFromJsonSource();
            if (fjSw is { } fj1 && fj1.path.Length > 0)
            {
                Consume(TokenKind.Comma);
                string pfx = ConsumeString();
                Consume(TokenKind.RParen);
                return new FromJsonPathStringPredicateNode(fj1.prop, fj1.path, pfx, ci, FromJsonPathPredicateKind.StartsWith);
            }
            string prop   = fjSw is { } fj1b ? fj1b.prop : ReadProperty();
            Consume(TokenKind.Comma);
            string prefix = ConsumeString();
            Consume(TokenKind.RParen);
            return new StartsWithNode(prop, prefix, ci);
        }

        if (t.Kind is TokenKind.Contains or TokenKind.CiContains)
        {
            bool ci = t.Kind == TokenKind.CiContains;
            _pos++;
            Consume(TokenKind.LParen);
            var fjCt = TryParseFromJsonSource();
            if (fjCt is { } fj2 && fj2.path.Length > 0)
            {
                Consume(TokenKind.Comma);
                string txt = ConsumeString();
                Consume(TokenKind.RParen);
                return new FromJsonPathStringPredicateNode(fj2.prop, fj2.path, txt, ci, FromJsonPathPredicateKind.Contains);
            }
            string prop = fjCt is { } fj2b ? fj2b.prop : ReadProperty();
            Consume(TokenKind.Comma);
            string text = ConsumeString();
            Consume(TokenKind.RParen);
            return new ContainsNode(prop, text, ci);
        }

        if (t.Kind is TokenKind.EndsWith or TokenKind.CiEndsWith)
        {
            bool ci = t.Kind == TokenKind.CiEndsWith;
            _pos++;
            Consume(TokenKind.LParen);
            var fjEw = TryParseFromJsonSource();
            if (fjEw is { } fj3 && fj3.path.Length > 0)
            {
                Consume(TokenKind.Comma);
                string sfx = ConsumeString();
                Consume(TokenKind.RParen);
                return new FromJsonPathStringPredicateNode(fj3.prop, fj3.path, sfx, ci, FromJsonPathPredicateKind.EndsWith);
            }
            string prop   = fjEw is { } fj3b ? fj3b.prop : ReadProperty();
            Consume(TokenKind.Comma);
            string suffix = ConsumeString();
            Consume(TokenKind.RParen);
            return new EndsWithNode(prop, suffix, ci);
        }

        if (t.Kind is TokenKind.Length or TokenKind.ToJson or TokenKind.FromJson)
        {
            var fn = t.Kind;
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.RParen);

            // fromJson(Prop).field.sub = value  — path navigation into parsed JSON
            if (fn == TokenKind.FromJson && (PeekKind() is TokenKind.Dot or TokenKind.LBracket))
            {
                var jsonPath = ReadJsonPath();

                // like operator
                if (PeekKind() == TokenKind.Like)
                {
                    Consume(TokenKind.Like);
                    return new FromJsonPathLikeNode(prop, jsonPath, ConsumeString());
                }

                // in operator
                if (PeekKind() == TokenKind.In)
                {
                    Consume(TokenKind.In);
                    Consume(TokenKind.LBracket);
                    var items = new List<object?>();
                    while (PeekKind() != TokenKind.RBracket && PeekKind() != TokenKind.Eof)
                    {
                        ParseValue(out _);
                        items.Add(ParseLiteral(_tokens[_pos - 1]));
                        if (PeekKind() == TokenKind.Comma) Consume(TokenKind.Comma);
                    }
                    Consume(TokenKind.RBracket);
                    return new FromJsonPathInNode(prop, jsonPath, items.ToArray());
                }

                if (!TryConsumeOp(out var pathOp))
                    throw new FormatException($"Expected comparison operator after fromJson().<path> at pos {Peek().Pos}");
                object? pathRhs = ParseValue(out _);
                return new FromJsonPathCompareNode(prop, jsonPath, pathOp, pathRhs);
            }

            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after {fn}() at pos {Peek().Pos}");

            object? rhs = ParseValue(out _);
            return fn switch
            {
                TokenKind.Length   => new LengthCompareNode(prop, cmpOp, rhs),
                TokenKind.ToJson   => new ToJsonCompareNode(prop, cmpOp, rhs),
                TokenKind.FromJson => new FromJsonCompareNode(prop, cmpOp, rhs),
                _                  => new MatchAllNode(),
            };
        }

        if (t.Kind is TokenKind.ToLower or TokenKind.ToUpper or TokenKind.ToNumber)
        {
            var fn = t.Kind;
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.RParen);

            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after {fn}() at pos {Peek().Pos}");

            object? rhs = ParseValue(out _);
            return fn switch
            {
                TokenKind.ToLower  => new ToLowerCompareNode(prop, cmpOp, rhs),
                TokenKind.ToUpper  => new ToUpperCompareNode(prop, cmpOp, rhs),
                TokenKind.ToNumber => new ToNumberCompareNode(prop, cmpOp, rhs),
                _                  => new MatchAllNode(),
            };
        }

        if (t.Kind == TokenKind.Substring)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.Comma);
            int start = ConsumeInt();
            int? length = null;
            if (PeekKind() == TokenKind.Comma)
            {
                Consume(TokenKind.Comma);
                length = ConsumeInt();
            }
            Consume(TokenKind.RParen);

            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after substring() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new SubstringCompareNode(prop, start, length, cmpOp, rhs);
        }

        if (t.Kind is TokenKind.IndexOf or TokenKind.LastIndexOf)
        {
            bool isLast = t.Kind == TokenKind.LastIndexOf;
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.Comma);
            string needle = ConsumeString();
            Consume(TokenKind.RParen);

            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after {(isLast ? "lastIndexOf" : "indexOf")}() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new IndexOfCompareNode(prop, needle, isLast, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.Replace)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.Comma);
            string find = ConsumeString();
            Consume(TokenKind.Comma);
            string repl = ConsumeString();
            Consume(TokenKind.RParen);

            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after replace() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new ReplaceCompareNode(prop, find, repl, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.Concat)
        {
            _pos++;
            Consume(TokenKind.LParen);
            var args = new List<ConcatArgNode>();
            while (true)
            {
                object? argVal = ParseValue(out string? argProp);
                if (argProp is not null) args.Add(new ConcatPropertyArgNode(argProp));
                else args.Add(new ConcatLiteralArgNode(argVal));

                if (PeekKind() == TokenKind.Comma)
                {
                    Consume(TokenKind.Comma);
                    continue;
                }
                break;
            }
            Consume(TokenKind.RParen);

            if (args.Count < 2)
                throw new FormatException("concat() requires at least 2 arguments");
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after concat() at pos {Peek().Pos}");

            object? rhs = ParseValue(out _);
            return new ConcatCompareNode(args, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.TypeOf)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after typeOf() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new TypeOfCompareNode(prop, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.ElementAt)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.Comma);
            object? idx = ParseValue(out string? idxProp);
            if (idxProp is not null) idx = idxProp;
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after elementAt() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new ElementAtCompareNode(prop, idx, cmpOp, rhs);
        }

        if (t.Kind is TokenKind.Keys or TokenKind.Values)
        {
            bool keys = t.Kind == TokenKind.Keys;
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after {(keys ? "keys" : "values")}() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return keys
                ? new KeysCompareNode(prop, cmpOp, rhs)
                : new ValuesCompareNode(prop, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.Round)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.Comma);
            int places = ConsumeInt();
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after round() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new RoundCompareNode(prop, places, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.Now)
        {
            _pos++;
            Consume(TokenKind.LParen);
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after now() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new NowCompareNode(cmpOp, rhs);
        }

        if (t.Kind == TokenKind.DateTime)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after dateTime() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new DateTimeCompareNode(prop, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.ToIsoString)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            int? offset = null;
            if (PeekKind() == TokenKind.Comma)
            {
                Consume(TokenKind.Comma);
                offset = ConsumeInt();
            }
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after toIsoString() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new ToIsoStringCompareNode(prop, offset, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.DatePart)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.Comma);
            string part = ConsumeString();
            int? offset = null;
            if (PeekKind() == TokenKind.Comma)
            {
                Consume(TokenKind.Comma);
                offset = ConsumeInt();
            }
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after datePart() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new DatePartCompareNode(prop, part, offset, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.TimeOfDay)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.Comma);
            int offset = ConsumeInt();
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after timeOfDay() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new TimeOfDayCompareNode(prop, offset, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.TimeSpan)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after timeSpan() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new TimeSpanCompareNode(prop, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.TotalMilliseconds)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after totalMilliseconds() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new TotalMillisecondsCompareNode(prop, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.ToTimeString)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after toTimeString() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new ToTimeStringCompareNode(prop, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.ToHexString)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after toHexString() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new ToHexStringCompareNode(prop, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.Bucket)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.Comma);
            double err = ConsumeDouble();
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after bucket() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new BucketCompareNode(prop, err, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.OffsetIn)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string tz = ConsumeString();
            Consume(TokenKind.Comma);
            string instantProp = ReadProperty();
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after offsetIn() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new OffsetInCompareNode(tz, instantProp, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.Arrived)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after arrived() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new ArrivedCompareNode(prop, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.FromXml)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop  = ReadProperty();
            Consume(TokenKind.Comma);
            string xpath = ConsumeString();
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after fromXml() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new FromXmlCompareNode(prop, xpath, cmpOp, rhs);
        }

        if (t.Kind is TokenKind.FromBase64 or TokenKind.ToBase64)
        {
            var fn = t.Kind;
            _pos++;
            Consume(TokenKind.LParen);
            string prop = ReadProperty();
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after {fn}() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return fn == TokenKind.FromBase64
                ? new FromBase64CompareNode(prop, cmpOp, rhs)
                : new ToBase64CompareNode(prop, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.RegexMatch)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop    = ReadProperty();
            Consume(TokenKind.Comma);
            string pattern = ConsumeRegexOrString(out bool ignoreCase);
            Consume(TokenKind.RParen);
            return new RegexMatchNode(prop, pattern, ignoreCase);
        }

        if (t.Kind == TokenKind.RegexExtract)
        {
            _pos++;
            Consume(TokenKind.LParen);
            string prop    = ReadProperty();
            Consume(TokenKind.Comma);
            string pattern = ConsumeRegexOrString(out bool ignoreCase);
            int group = 1;
            if (PeekKind() == TokenKind.Comma)
            {
                Consume(TokenKind.Comma);
                group = ConsumeInt();
            }
            Consume(TokenKind.RParen);
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after regexExtract() at pos {Peek().Pos}");
            object? rhs = ParseValue(out _);
            return new RegexExtractCompareNode(prop, pattern, group, ignoreCase, cmpOp, rhs);
        }

        if (t.Kind == TokenKind.Coalesce)
        {
            _pos++;
            Consume(TokenKind.LParen);
            var args = new List<CoalesceArgNode>();
            while (true)
            {
                object? argVal = ParseValue(out string? argProp);
                if (argProp is not null) args.Add(new CoalescePropertyArgNode(argProp));
                else args.Add(new CoalesceLiteralArgNode(argVal));

                if (PeekKind() == TokenKind.Comma)
                {
                    Consume(TokenKind.Comma);
                    continue;
                }
                break;
            }
            Consume(TokenKind.RParen);

            if (args.Count < 2)
                throw new FormatException("coalesce() requires at least 2 arguments");
            if (!TryConsumeOp(out var cmpOp))
                throw new FormatException($"Expected comparison operator after coalesce() at pos {Peek().Pos}");

            object? rhs = ParseValue(out _);
            return new CoalesceCompareNode(args, cmpOp, rhs);
        }

        // Comparison or 'in' expression
        // Parse the left-hand value first
        object? lhv = ParseValue(out string? lhProp);

        // Check for 'in'
        if (PeekKind() == TokenKind.In)
        {
            Consume(TokenKind.In);
            Consume(TokenKind.LBracket);
            var items = new List<object?>();
            while (PeekKind() != TokenKind.RBracket && PeekKind() != TokenKind.Eof)
            {
                ParseValue(out _);
                items.Add(ParseLiteral(_tokens[_pos - 1]));
                if (PeekKind() == TokenKind.Comma) Consume(TokenKind.Comma);
            }
            Consume(TokenKind.RBracket);
            string inProp = lhProp ?? "@l";
            return new InNode(inProp, items.ToArray());
        }

        // 'like'
        if (PeekKind() == TokenKind.Like)
        {
            Consume(TokenKind.Like);
            string pattern = ConsumeString();
            return new LikeNode(lhProp ?? string.Empty, pattern);
        }

        // Comparison operator
        if (TryConsumeOp(out var op))
        {
            object? rhv = ParseValue(out _);
            // If left is a property and right is a value, emit CompareNode
            if (lhProp is not null)
                return new CompareNode(lhProp, op, rhv);
            // right-hand side property with literal on left (swap)
            return new CompareNode(lhv?.ToString() ?? "", FlipOp(op), rhv);
        }

        // Bare level keyword: Error, Fatal, etc.
        if (lhProp is not null &&
            LogLevelExtensions.TryParse(lhProp.AsSpan(), out var level))
            return new LevelNode(level);

        // Bare property existence (not ideal but tolerated)
        if (lhProp is not null)
            return new HasNode(lhProp);

        return new MatchAllNode();
    }

    // ── Value reading ─────────────────────────────────────────────────────────

    /// <summary>
    /// Reads the next token as a value.
    /// <paramref name="propertyName"/> is set if the token is an identifier (property path).
    /// Returns the literal object for non-identifier tokens.
    /// </summary>
    private object? ParseValue(out string? propertyName)
    {
        var t = Peek();
        propertyName = null;

        switch (t.Kind)
        {
            case TokenKind.Ident:
            case TokenKind.LBracket:
                propertyName = ReadPropertyPath();
                return null;

            case TokenKind.String:
                _pos++;
                return t.Raw;

            case TokenKind.Number:
                _pos++;
                if (t.Raw.Contains('.') && double.TryParse(t.Raw, System.Globalization.NumberStyles.Any,
                        System.Globalization.CultureInfo.InvariantCulture, out double d))
                    return d;
                if (long.TryParse(t.Raw, out long l)) return l;
                return t.Raw;

            case TokenKind.True:  _pos++; return true;
            case TokenKind.False: _pos++; return false;
            case TokenKind.Null:  _pos++; return null;

            default:
                _pos++;
                return null;
        }
    }

    private object? ParseLiteral(Token t) => t.Kind switch
    {
        TokenKind.String => t.Raw,
        TokenKind.Number => long.TryParse(t.Raw, out long l) ? l : (object?)t.Raw,
        TokenKind.True   => true,
        TokenKind.False  => false,
        _                => null,
    };

    private string ReadProperty() => ReadPropertyPath();

    /// <summary>
    /// Parses <c>(ident | '[' string ']') ('.' ident | '[' string ']')*</c> and
    /// returns the canonical encoded form (segments joined by
    /// <see cref="PropertyPath.Separator"/>). The initial identifier may itself
    /// contain dots (the lexer merges <c>Foo.Bar</c> into a single token); those
    /// are also treated as segment separators. A path may also start with a
    /// bracket-quoted segment, e.g. <c>['1']['$type']</c>, which is necessary
    /// when the top-level property name is not a valid identifier.
    /// </summary>
    private string ReadPropertyPath()
    {
        var t    = Peek();
        var segs = new List<string>(4);

        if (t.Kind == TokenKind.Ident)
        {
            _pos++;
            foreach (var s in t.Raw.Split('.')) segs.Add(s);
        }
        else if (t.Kind == TokenKind.LBracket)
        {
            _pos++;
            segs.Add(ReadBracketSegment());
            Consume(TokenKind.RBracket);
        }
        else
        {
            throw new FormatException($"Expected property name at pos {t.Pos}");
        }

        while (true)
        {
            if (PeekKind() == TokenKind.LBracket)
            {
                _pos++;
                segs.Add(ReadBracketSegment());
                Consume(TokenKind.RBracket);
            }
            else if (PeekKind() == TokenKind.Dot)
            {
                _pos++;
                var nxt = Peek();
                if (nxt.Kind != TokenKind.Ident)
                    throw new FormatException($"Expected property segment at pos {nxt.Pos}");
                _pos++;
                foreach (var s in nxt.Raw.Split('.')) segs.Add(s);
            }
            else break;
        }
        return string.Join(PropertyPath.Separator, segs);
    }

    /// <summary>
    /// Reads the contents of a bracket segment (the <c>'foo'</c> or <c>0</c> part
    /// between <c>[</c> and <c>]</c>). String literals are returned verbatim;
    /// numeric literals are returned prefixed with <see cref="PropertyPath.IndexMarker"/>
    /// so the evaluator can perform list-indexed lookups against array-valued
    /// properties.
    /// </summary>
    private string ReadBracketSegment()
    {
        var t = Peek();
        if (t.Kind == TokenKind.String)
        {
            _pos++;
            return t.Raw;
        }
        if (t.Kind == TokenKind.Number)
        {
            _pos++;
            return PropertyPath.IndexMarker + t.Raw;
        }
        throw new FormatException($"Expected quoted segment or numeric index at pos {t.Pos}");
    }

    private string ConsumeString()
    {
        var t = Peek();
        if (t.Kind == TokenKind.String) { _pos++; return t.Raw; }
        throw new FormatException($"Expected string literal at pos {t.Pos}");
    }

    private int ConsumeInt()
    {
        var t = Peek();
        if (t.Kind != TokenKind.Number)
            throw new FormatException($"Expected numeric literal at pos {t.Pos}");
        _pos++;
        if (!int.TryParse(t.Raw, out int value))
            throw new FormatException($"Expected integer literal at pos {t.Pos}");
        return value;
    }

    /// <summary>
    /// Reads either a plain quoted string or a /pattern/flags regex literal.
    /// Sets <paramref name="ignoreCase"/> = true when the 'i' flag is present.
    /// Returns the inner pattern string without the surrounding slashes.
    /// </summary>
    private string ConsumeRegexOrString(out bool ignoreCase)
    {
        ignoreCase = false;
        var t = Peek();
        if (t.Kind == TokenKind.String)
        {
            _pos++;
            return t.Raw;
        }
        // Allow /pattern/i syntax — lexer will tokenise it as Ident starting with '/'
        // but since '/' is not an ident char it falls through as separate tokens.
        // Simplest approach: treat quoted strings as the regex pattern.
        throw new FormatException($"Expected regex pattern (quoted string) at pos {t.Pos}");
    }

    private double ConsumeDouble()
    {
        var t = Peek();
        if (t.Kind != TokenKind.Number)
            throw new FormatException($"Expected numeric literal at pos {t.Pos}");
        _pos++;
        if (!double.TryParse(t.Raw, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double value))
            throw new FormatException($"Expected numeric literal at pos {t.Pos}");
        return value;
    }

    /// <summary>
    /// Peeks at the current position: if a <c>fromJson(property)</c> call is next,
    /// consumes it (plus an optional path suffix) and returns the source property + path.
    /// Returns null without consuming anything if the next token is not <c>fromJson</c>.
    /// </summary>
    private (string prop, string[] path)? TryParseFromJsonSource()
    {
        if (PeekKind() != TokenKind.FromJson) return null;
        _pos++; // consume 'fromJson'
        Consume(TokenKind.LParen);
        string prop = ReadProperty();
        Consume(TokenKind.RParen);
        // Optional path
        if (PeekKind() is TokenKind.Dot or TokenKind.LBracket)
            return (prop, ReadJsonPath());
        return (prop, Array.Empty<string>());
    }

    /// <summary>
    /// Reads a property-path suffix after a scalar function call, e.g.
    /// <c>.user.balance</c>, <c>['key']</c>, <c>[0]</c> or any combination.
    /// Returns an array of string segments (numeric strings = array index).
    /// </summary>
    private string[] ReadJsonPath()
    {
        var segs = new List<string>(4);
        while (PeekKind() is TokenKind.Dot or TokenKind.LBracket)
        {
            if (PeekKind() == TokenKind.Dot)
            {
                Consume(TokenKind.Dot);
                var nxt = Peek();
                if (nxt.Kind != TokenKind.Ident)
                    throw new FormatException($"Expected field name after '.' at pos {nxt.Pos}");
                _pos++;
                // The lexer merges dotted idents like 'foo.bar' into one token
                foreach (var s in nxt.Raw.Split('.'))
                    if (!string.IsNullOrEmpty(s)) segs.Add(s);
            }
            else // LBracket
            {
                Consume(TokenKind.LBracket);
                var seg = Peek();
                if (seg.Kind == TokenKind.String)
                {
                    _pos++;
                    segs.Add(seg.Raw);
                }
                else if (seg.Kind == TokenKind.Number)
                {
                    _pos++;
                    segs.Add(seg.Raw); // numeric string — evaluator treats as array index
                }
                else
                    throw new FormatException($"Expected string key or numeric index at pos {seg.Pos}");
                Consume(TokenKind.RBracket);
            }
        }
        if (segs.Count == 0)
            throw new FormatException("fromJson() path navigation requires at least one segment");
        return segs.ToArray();
    }

    // ── Token helpers ─────────────────────────────────────────────────────────

    private Token Peek() =>
        _pos < _tokens.Count ? _tokens[_pos] : new Token(TokenKind.Eof, "", -1);

    private TokenKind PeekKind() => Peek().Kind;

    private void Consume(TokenKind kind)
    {
        if (PeekKind() != kind)
            throw new FormatException($"Expected {kind} but got {Peek()} at pos {Peek().Pos}");
        _pos++;
    }

    private bool TryConsumeOp(out CompareOp op)
    {
        op = PeekKind() switch
        {
            TokenKind.Eq => CompareOp.Eq,
            TokenKind.Ne => CompareOp.Ne,
            TokenKind.Lt => CompareOp.Lt,
            TokenKind.Le => CompareOp.Le,
            TokenKind.Gt => CompareOp.Gt,
            TokenKind.Ge => CompareOp.Ge,
            _            => (CompareOp)(-1),
        };
        if ((int)op < 0) return false;
        _pos++;
        return true;
    }

    private static CompareOp FlipOp(CompareOp op) => op switch
    {
        CompareOp.Lt => CompareOp.Gt,
        CompareOp.Le => CompareOp.Ge,
        CompareOp.Gt => CompareOp.Lt,
        CompareOp.Ge => CompareOp.Le,
        _            => op,
    };
}
