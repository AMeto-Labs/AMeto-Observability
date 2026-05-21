namespace Rd.Log.Query.Filtering;

// ── Token types ───────────────────────────────────────────────────────────────

internal enum TokenKind
{
    // Literals
    String, Number, True, False, Null,

    // Identifiers / keywords
    Ident,
    And, Or, Not,
    Like, In, Has, IsDefined, StartsWith, Contains,
    CiStartsWith, CiContains,

    // Operators
    Eq, Ne, Lt, Le, Gt, Ge,

    // Punctuation
    LParen, RParen, LBracket, RBracket, Comma, Dot,

    // Special
    Eof
}

internal readonly struct Token
{
    public readonly TokenKind Kind;
    public readonly string    Raw;    // original source text (unescaped for string literals)
    public readonly int       Pos;    // char offset in source

    public Token(TokenKind kind, string raw, int pos) { Kind = kind; Raw = raw; Pos = pos; }
    public override string ToString() => $"{Kind}({Raw}) @{Pos}";
}

// ── Lexer ─────────────────────────────────────────────────────────────────────

/// <summary>
/// Tokenises a Seq filter expression string.
///
/// Supported tokens:
///   - Single-quoted string literals: 'hello \'world\''
///   - Integer and decimal number literals
///   - Identifiers (including dotted paths like @t, @mt, Props.Inner)
///   - Keywords: and, or, not, like, in, has, isDefined, startsWith, contains,
///               ci_startsWith, ci_contains, true, false, null
///   - Comparison operators: =, !=, &lt;, &lt;=, &gt;, &gt;=
///   - Brackets, parens, comma
/// </summary>
internal sealed class Lexer
{
    private readonly string _src;
    private int _pos;

    public Lexer(string source) { _src = source; _pos = 0; }

    public List<Token> Tokenise()
    {
        var tokens = new List<Token>(32);
        while (true)
        {
            SkipWhitespace();
            if (_pos >= _src.Length)
            {
                tokens.Add(new Token(TokenKind.Eof, "", _pos));
                break;
            }

            char c = _src[_pos];

            if (c == '\'')      { tokens.Add(ReadString());     continue; }
            if (c == '(' )      { tokens.Add(Punct(TokenKind.LParen));  continue; }
            if (c == ')' )      { tokens.Add(Punct(TokenKind.RParen));  continue; }
            if (c == '[' )      { tokens.Add(Punct(TokenKind.LBracket)); continue; }
            if (c == ']' )      { tokens.Add(Punct(TokenKind.RBracket)); continue; }
            if (c == ',' )      { tokens.Add(Punct(TokenKind.Comma));   continue; }
            if (c == '.' && (_pos + 1 >= _src.Length || !char.IsDigit(_src[_pos+1])))
                                { tokens.Add(Punct(TokenKind.Dot));     continue; }

            if (c == '=' )      { tokens.Add(Punct(TokenKind.Eq));      continue; }
            if (c == '!' && Peek(1) == '=')
                                { _pos += 2; tokens.Add(new Token(TokenKind.Ne, "!=", _pos-2)); continue; }
            if (c == '<' && Peek(1) == '=')
                                { _pos += 2; tokens.Add(new Token(TokenKind.Le, "<=", _pos-2)); continue; }
            if (c == '<' )      { tokens.Add(Punct(TokenKind.Lt));      continue; }
            if (c == '>' && Peek(1) == '=')
                                { _pos += 2; tokens.Add(new Token(TokenKind.Ge, ">=", _pos-2)); continue; }
            if (c == '>' )      { tokens.Add(Punct(TokenKind.Gt));      continue; }

            if (c == '-' || char.IsDigit(c) || (c == '.' && _pos + 1 < _src.Length && char.IsDigit(_src[_pos+1])))
                                { tokens.Add(ReadNumber());              continue; }

            if (c == '@' || c == '_' || char.IsLetter(c))
                                { tokens.Add(ReadIdent());               continue; }

            // Unknown char — skip with error tolerance
            _pos++;
        }
        return tokens;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void SkipWhitespace()
    {
        while (_pos < _src.Length && char.IsWhiteSpace(_src[_pos])) _pos++;
    }

    private char Peek(int offset) =>
        (_pos + offset < _src.Length) ? _src[_pos + offset] : '\0';

    private Token Punct(TokenKind kind)
    {
        int start = _pos++;
        return new Token(kind, _src[start].ToString(), start);
    }

    private Token ReadString()
    {
        int start = _pos++;
        var sb    = new System.Text.StringBuilder();
        while (_pos < _src.Length)
        {
            char c = _src[_pos];
            if (c == '\\' && _pos + 1 < _src.Length)
            {
                _pos++;
                sb.Append(_src[_pos++]);
                continue;
            }
            if (c == '\'') { _pos++; break; }
            sb.Append(c);
            _pos++;
        }
        return new Token(TokenKind.String, sb.ToString(), start);
    }

    private Token ReadNumber()
    {
        int start = _pos;
        if (_src[_pos] == '-') _pos++;
        while (_pos < _src.Length && (char.IsDigit(_src[_pos]) || _src[_pos] == '.')) _pos++;
        return new Token(TokenKind.Number, _src[start.._pos], start);
    }

    private Token ReadIdent()
    {
        int start = _pos;
        // Allow @, letters, digits, underscore, dots (for path like Foo.Bar)
        while (_pos < _src.Length &&
               (_src[_pos] == '@' || _src[_pos] == '_' || char.IsLetterOrDigit(_src[_pos]) || _src[_pos] == '.'))
            _pos++;

        string raw = _src[start.._pos];

        TokenKind kind = raw.ToLowerInvariant() switch
        {
            "and"           => TokenKind.And,
            "or"            => TokenKind.Or,
            "not"           => TokenKind.Not,
            "like"          => TokenKind.Like,
            "in"            => TokenKind.In,
            "has"           => TokenKind.Has,
            "isdefined"     => TokenKind.IsDefined,
            "startswith"    => TokenKind.StartsWith,
            "contains"      => TokenKind.Contains,
            "ci_startswith" => TokenKind.CiStartsWith,
            "ci_contains"   => TokenKind.CiContains,
            "true"          => TokenKind.True,
            "false"         => TokenKind.False,
            "null"          => TokenKind.Null,
            _               => TokenKind.Ident,
        };

        return new Token(kind, raw, start);
    }
}
