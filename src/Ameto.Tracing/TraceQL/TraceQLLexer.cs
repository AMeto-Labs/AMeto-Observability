namespace Ameto.Tracing.TraceQL;

public enum TokenKind
{
    LBrace, RBrace, LParen, RParen,
    And, Or, Not,
    Eq, Neq, Lt, Lte, Gt, Gte,
    Attr,     // .key.sub-key  (leading dot consumed, dots in key kept)
    Ident,    // service / duration / status / name / kind / error / ok / unset / ...
    String,   // "..." or `...`
    Number,   // 123, 1.5, -3, -1e3
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
                case '-':
                    // A NEGATIVE LITERAL, or nothing this grammar has. There is no binary minus, so
                    // a '-' directly before a digit can only be a sign. It used to fall into "skip
                    // unknown" below, which turned `{ .x = -3 }` into `{ .x = 3 }` — a query that
                    // answered, just not the question asked. Any other '-' is refused for the same
                    // reason: `.x = - 3` silently read as 3 is the same wrong answer.
                    if (pos + 1 < input.Length && char.IsAsciiDigit(input[pos + 1]))
                        tokens.Add(ReadNumberOrDuration(input, ref pos));
                    else
                        throw new TraceQLException(
                            $"'-' at position {pos} is not followed by a number; a negative literal is written '-3', '-0.5' or '-1e3'");
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

    /// <summary>
    /// <c>-?digits[.digits][e[+-]digits][suffix]</c>. The sign and the exponent are part of the
    /// literal — <c>-3</c>, <c>-0.5</c>, <c>-1e3</c> — and a duration keeps its sign so the parser
    /// can refuse <c>-1ms</c> by name rather than read it as something else.
    ///
    /// <para>TEXT THAT DOES NOT PARSE IS AN ERROR, not the number 0 it used to become:
    /// <c>{ .version = 1.2.3 }</c> read as <c>.version = 0</c>.</para>
    /// </summary>
    private static Token ReadNumberOrDuration(ReadOnlySpan<char> src, ref int pos)
    {
        int start = pos;
        if (src[pos] == '-') pos++;
        while (pos < src.Length && (char.IsDigit(src[pos]) || src[pos] == '.'))
            pos++;

        // An exponent only when a digit follows it (after an optional sign): `1e3`, `2.5E-4`. A
        // bare `e` is left for the identifier reader, as before.
        if (pos < src.Length && src[pos] is 'e' or 'E')
        {
            int e = pos + 1;
            if (e < src.Length && src[e] is '+' or '-') e++;
            if (e < src.Length && char.IsAsciiDigit(src[e]))
            {
                pos = e;
                while (pos < src.Length && char.IsAsciiDigit(src[pos])) pos++;
            }
        }

        var numText = src[start..pos];
        if (!double.TryParse(numText, System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out var num)
            || !double.IsFinite(num))
            throw new TraceQLException($"'{numText}' at position {start} is not a number");

        // Check for duration suffix. A bool, not "nanos >= 0": a negative duration is still a
        // duration, and the parser refuses it by name.
        if (TryParseDurationSuffix(src, ref pos, num, out long nanos))
            return new Token(TokenKind.Duration, src[start..pos].ToString(), nanos);

        return new Token(TokenKind.Number, numText.ToString(), num);
    }

    private static bool TryParseDurationSuffix(ReadOnlySpan<char> src, ref int pos, double num, out long nanos)
    {
        nanos = 0;
        if (pos >= src.Length) return false;

        // ms
        if (pos + 1 < src.Length && src[pos] == 'm' && src[pos + 1] == 's')
        { pos += 2; nanos = (long)(num * 1_000_000); return true; }
        // us
        if (pos + 1 < src.Length && src[pos] == 'u' && src[pos + 1] == 's')
        { pos += 2; nanos = (long)(num * 1_000); return true; }
        // ns
        if (pos + 1 < src.Length && src[pos] == 'n' && src[pos + 1] == 's')
        { pos += 2; nanos = (long)num; return true; }
        // s  (but not followed by a letter — avoids matching "service")
        if (src[pos] == 's' && (pos + 1 >= src.Length || !char.IsLetter(src[pos + 1])))
        { pos += 1; nanos = (long)(num * 1_000_000_000L); return true; }
        // m  (minutes)
        if (src[pos] == 'm' && (pos + 1 >= src.Length || !char.IsLetter(src[pos + 1])))
        { pos += 1; nanos = (long)(num * 60_000_000_000L); return true; }
        // h
        if (src[pos] == 'h' && (pos + 1 >= src.Length || !char.IsLetter(src[pos + 1])))
        { pos += 1; nanos = (long)(num * 3_600_000_000_000L); return true; }

        return false;
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
