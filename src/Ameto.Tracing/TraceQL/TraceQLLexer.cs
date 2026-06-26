namespace Ameto.Tracing.TraceQL;

public enum TokenKind
{
    LBrace, RBrace, LParen, RParen,
    And, Or, Not,
    Eq, Neq, Lt, Lte, Gt, Gte,
    Attr,     // .key.sub-key  (leading dot consumed, dots in key kept)
    Ident,    // service / duration / status / name / kind / error / ok / unset / ...
    String,   // "..." or `...`
    Number,   // 123 or 1.5
    Duration, // 1s / 500ms / 1.5m  — Raw holds nanoseconds as long
    Eof,
}

public readonly struct Token
{
    public readonly TokenKind Kind;
    public readonly string    Text;    // original text for diagnostics
    public readonly double    Number;  // valid when Kind == Number or Duration (nanos as double)

    public Token(TokenKind k, string t, double n = 0) { Kind = k; Text = t; Number = n; }
    public override string ToString() => $"{Kind}({Text})";
}

/// <summary>
/// Tokenises a TraceQL span-set filter expression.
/// Supported: <c>{ .attr op value &amp;&amp; intrinsic op value || ... }</c>
/// </summary>
public static class TraceQLLexer
{
    public static List<Token> Tokenize(ReadOnlySpan<char> input)
    {
        var tokens = new List<Token>(16);
        int pos = 0;

        while (pos < input.Length)
        {
            char c = input[pos];

            if (char.IsWhiteSpace(c)) { pos++; continue; }

            switch (c)
            {
                case '{': tokens.Add(new Token(TokenKind.LBrace,  "{")); pos++; break;
                case '}': tokens.Add(new Token(TokenKind.RBrace,  "}")); pos++; break;
                case '(': tokens.Add(new Token(TokenKind.LParen,  "(")); pos++; break;
                case ')': tokens.Add(new Token(TokenKind.RParen,  ")")); pos++; break;
                case '!':
                    if (pos + 1 < input.Length && input[pos + 1] == '=')
                    { tokens.Add(new Token(TokenKind.Neq, "!=")); pos += 2; }
                    else
                    { tokens.Add(new Token(TokenKind.Not, "!")); pos++; }
                    break;
                case '=':
                    if (pos + 1 < input.Length && input[pos + 1] == '~')
                    { tokens.Add(new Token(TokenKind.Eq, "=~")); pos += 2; } // treat regex-eq as eq for MVP
                    else
                    { tokens.Add(new Token(TokenKind.Eq, "=")); pos++; }
                    break;
                case '<':
                    if (pos + 1 < input.Length && input[pos + 1] == '=')
                    { tokens.Add(new Token(TokenKind.Lte, "<=")); pos += 2; }
                    else
                    { tokens.Add(new Token(TokenKind.Lt, "<")); pos++; }
                    break;
                case '>':
                    if (pos + 1 < input.Length && input[pos + 1] == '=')
                    { tokens.Add(new Token(TokenKind.Gte, ">=")); pos += 2; }
                    else
                    { tokens.Add(new Token(TokenKind.Gt, ">")); pos++; }
                    break;
                case '&':
                    if (pos + 1 < input.Length && input[pos + 1] == '&')
                    { tokens.Add(new Token(TokenKind.And, "&&")); pos += 2; }
                    else pos++;
                    break;
                case '|':
                    if (pos + 1 < input.Length && input[pos + 1] == '|')
                    { tokens.Add(new Token(TokenKind.Or, "||")); pos += 2; }
                    else pos++;
                    break;
                case '"': case '\'': case '`':
                    tokens.Add(ReadString(input, ref pos, c));
                    break;
                case '.':
                    tokens.Add(ReadAttr(input, ref pos));
                    break;
                default:
                    if (char.IsDigit(c))
                        tokens.Add(ReadNumberOrDuration(input, ref pos));
                    else if (char.IsLetter(c) || c == '_')
                        tokens.Add(ReadIdent(input, ref pos));
                    else
                        pos++; // skip unknown
                    break;
            }
        }

        tokens.Add(new Token(TokenKind.Eof, ""));
        return tokens;
    }

    // ── Attribute: .key.sub-key ────────────────────────────────────────────────

    private static Token ReadAttr(ReadOnlySpan<char> src, ref int pos)
    {
        pos++; // consume '.'
        int start = pos;
        // Attribute key may contain letters, digits, underscores, hyphens, dots
        while (pos < src.Length && (char.IsLetterOrDigit(src[pos]) || src[pos] is '_' or '-' or '.'))
            pos++;
        return new Token(TokenKind.Attr, src[start..pos].ToString());
    }

    // ── String literal ─────────────────────────────────────────────────────────

    private static Token ReadString(ReadOnlySpan<char> src, ref int pos, char quote)
    {
        pos++; // consume opening quote
        int start = pos;
        while (pos < src.Length && src[pos] != quote)
        {
            if (src[pos] == '\\') pos++; // skip escape
            pos++;
        }
        string val = src[start..pos].ToString();
        if (pos < src.Length) pos++; // consume closing quote
        return new Token(TokenKind.String, val);
    }

    // ── Number or duration ─────────────────────────────────────────────────────

    private static Token ReadNumberOrDuration(ReadOnlySpan<char> src, ref int pos)
    {
        int start = pos;
        while (pos < src.Length && (char.IsDigit(src[pos]) || src[pos] == '.'))
            pos++;

        var numText = src[start..pos];
        if (!double.TryParse(numText, System.Globalization.NumberStyles.Any,
                System.Globalization.CultureInfo.InvariantCulture, out var num))
            return new Token(TokenKind.Number, numText.ToString(), 0);

        // Check for duration suffix
        long nanos = TryParseDurationSuffix(src, ref pos, num);
        if (nanos >= 0)
            return new Token(TokenKind.Duration, src[start..pos].ToString(), nanos);

        return new Token(TokenKind.Number, numText.ToString(), num);
    }

    private static long TryParseDurationSuffix(ReadOnlySpan<char> src, ref int pos, double num)
    {
        if (pos >= src.Length) return -1;

        // ms
        if (pos + 1 < src.Length && src[pos] == 'm' && src[pos + 1] == 's')
        { pos += 2; return (long)(num * 1_000_000); }
        // us
        if (pos + 1 < src.Length && src[pos] == 'u' && src[pos + 1] == 's')
        { pos += 2; return (long)(num * 1_000); }
        // ns
        if (pos + 1 < src.Length && src[pos] == 'n' && src[pos + 1] == 's')
        { pos += 2; return (long)num; }
        // s  (but not followed by a letter — avoids matching "service")
        if (src[pos] == 's' && (pos + 1 >= src.Length || !char.IsLetter(src[pos + 1])))
        { pos += 1; return (long)(num * 1_000_000_000L); }
        // m  (minutes)
        if (src[pos] == 'm' && (pos + 1 >= src.Length || !char.IsLetter(src[pos + 1])))
        { pos += 1; return (long)(num * 60_000_000_000L); }
        // h
        if (src[pos] == 'h' && (pos + 1 >= src.Length || !char.IsLetter(src[pos + 1])))
        { pos += 1; return (long)(num * 3_600_000_000_000L); }

        return -1;
    }

    // ── Identifier ─────────────────────────────────────────────────────────────

    private static Token ReadIdent(ReadOnlySpan<char> src, ref int pos)
    {
        int start = pos;
        while (pos < src.Length && (char.IsLetterOrDigit(src[pos]) || src[pos] == '_'))
            pos++;
        return new Token(TokenKind.Ident, src[start..pos].ToString());
    }
}
