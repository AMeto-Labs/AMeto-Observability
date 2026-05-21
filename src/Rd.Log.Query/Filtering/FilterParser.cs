using Rd.Log.Core;

namespace Rd.Log.Query.Filtering;

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
///   func_call := ('has' | 'isDefined' | 'startsWith' | 'contains'
///                | 'ci_startsWith' | 'ci_contains') '(' property ',' value ')'
///             | ('has' | 'isDefined') '(' property ')'
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
            string prop = ReadProperty();
            Consume(TokenKind.RParen);
            return kind == TokenKind.Has ? new HasNode(prop) : new IsDefinedNode(prop);
        }

        if (t.Kind is TokenKind.StartsWith or TokenKind.CiStartsWith)
        {
            bool ci = t.Kind == TokenKind.CiStartsWith;
            _pos++;
            Consume(TokenKind.LParen);
            string prop   = ReadProperty();
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
            string prop = ReadProperty();
            Consume(TokenKind.Comma);
            string text = ConsumeString();
            Consume(TokenKind.RParen);
            return new ContainsNode(prop, text, ci);
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
