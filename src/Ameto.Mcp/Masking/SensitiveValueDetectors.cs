using System.Buffers;
using System.Text.RegularExpressions;

namespace Ameto.Mcp.Masking;

/// <summary>
/// Finds values that are confidential by their SHAPE, wherever they sit: a card number in a
/// message template that was built by string interpolation, an email in an exception message,
/// a token in the body of an OTLP log record. A field rule cannot reach those — there is no
/// field name to match.
///
/// <para>Every pattern runs under a match timeout. The text is whatever a service logged, and
/// some of it is written by the outside world (a user agent, a request path); a pattern that
/// could be made to backtrack for seconds would hand that world a way to stall the answer.
/// A timeout is reported to the caller, which then hides the whole string.</para>
/// </summary>
internal static partial class SensitiveValueDetectors
{
    private const int TimeoutMs = 150;

    /// <summary>One value found in a text: where it is, and which detector claimed it.</summary>
    internal readonly record struct Hit(int Start, int Length, MaskDetectors Kind);

    private static readonly SearchValues<char> Digits = SearchValues.Create("0123456789");

    // Thirteen to nineteen digits, each optionally followed by one space or dash.
    [GeneratedRegex(@"(?<!\d)(?:\d[ \-]?){12,18}\d(?!\d)", RegexOptions.CultureInvariant, TimeoutMs)]
    private static partial Regex CardCandidate();

    [GeneratedRegex(@"(?<!\d)\d{12}(?!\d)", RegexOptions.CultureInvariant, TimeoutMs)]
    private static partial Regex TwelveDigits();

    [GeneratedRegex(@"[A-Za-z0-9._%+\-]{1,64}@[A-Za-z0-9\-]{1,63}(?:\.[A-Za-z0-9\-]{1,63}){1,8}", RegexOptions.CultureInvariant, TimeoutMs)]
    private static partial Regex Email();

    // +7 701 555 01 23 and the like: a plus, then ten to fifteen digits with common separators.
    [GeneratedRegex(@"(?<![\w+])\+\d[\d ()\-]{8,20}\d(?!\d)", RegexOptions.CultureInvariant, TimeoutMs)]
    private static partial Regex PhoneInternational();

    // 8 (701) 555-01-23 and 87015550123: the national spelling of a +7 7xx mobile number.
    [GeneratedRegex(@"(?<!\d)8[ (\-]{0,2}7\d{2}[ )\-]{0,2}\d{3}[ \-]?\d{2}[ \-]?\d{2}(?!\d)", RegexOptions.CultureInvariant, TimeoutMs)]
    private static partial Regex PhoneNational();

    [GeneratedRegex(@"eyJ[A-Za-z0-9_\-]{6,}\.[A-Za-z0-9_\-]{6,}\.[A-Za-z0-9_\-]*", RegexOptions.CultureInvariant, TimeoutMs)]
    private static partial Regex Jwt();

    [GeneratedRegex(@"(?<=\b[Bb]earer\s{1,4})[A-Za-z0-9._~+/=\-]{16,}", RegexOptions.CultureInvariant, TimeoutMs)]
    private static partial Regex BearerCredential();

    [GeneratedRegex(@"(?<![A-Za-z0-9])[A-Z]{2}\d{2}[A-Z0-9]{11,30}(?![A-Za-z0-9])", RegexOptions.CultureInvariant, TimeoutMs)]
    private static partial Regex IbanCandidate();

    /// <summary>
    /// Adds to <paramref name="hits"/> every value the enabled detectors find in
    /// <paramref name="text"/>, in order of position, none overlapping another. Throws
    /// <see cref="RegexMatchTimeoutException"/> when a pattern runs out of time.
    /// </summary>
    public static void Find(string text, MaskDetectors enabled, List<Hit> hits)
    {
        if (enabled == MaskDetectors.None || text.Length < 6) return;

        ReadOnlySpan<char> span = text;
        bool hasDigit = span.ContainsAny(Digits);

        // The detectors with a check of their own go first: a run of digits that passes one is
        // that thing, and the looser phone pattern must not claim it first.
        if (hasDigit && (enabled & MaskDetectors.CardNumber) != 0)
            foreach (var m in CardCandidate().EnumerateMatches(span))
                if (IsCardNumber(span.Slice(m.Index, m.Length))) Claim(hits, m.Index, m.Length, MaskDetectors.CardNumber);

        if (hasDigit && (enabled & MaskDetectors.Iin) != 0)
            foreach (var m in TwelveDigits().EnumerateMatches(span))
                if (IsIin(span.Slice(m.Index, m.Length))) Claim(hits, m.Index, m.Length, MaskDetectors.Iin);

        if (hasDigit && (enabled & MaskDetectors.Iban) != 0)
            foreach (var m in IbanCandidate().EnumerateMatches(span))
                if (IsIban(span.Slice(m.Index, m.Length))) Claim(hits, m.Index, m.Length, MaskDetectors.Iban);

        if ((enabled & MaskDetectors.Secret) != 0)
        {
            if (span.Contains("eyJ", StringComparison.Ordinal))
                foreach (var m in Jwt().EnumerateMatches(span)) Claim(hits, m.Index, m.Length, MaskDetectors.Secret);
            if (span.Contains("earer", StringComparison.Ordinal))
                foreach (var m in BearerCredential().EnumerateMatches(span)) Claim(hits, m.Index, m.Length, MaskDetectors.Secret);
        }

        if ((enabled & MaskDetectors.Email) != 0 && span.Contains('@'))
            foreach (var m in Email().EnumerateMatches(span)) Claim(hits, m.Index, m.Length, MaskDetectors.Email);

        if (hasDigit && (enabled & MaskDetectors.Phone) != 0)
        {
            if (span.Contains('+'))
                foreach (var m in PhoneInternational().EnumerateMatches(span))
                    if (CountDigits(span.Slice(m.Index, m.Length)) is >= 10 and <= 15) Claim(hits, m.Index, m.Length, MaskDetectors.Phone);
            foreach (var m in PhoneNational().EnumerateMatches(span)) Claim(hits, m.Index, m.Length, MaskDetectors.Phone);
        }

        if (hits.Count > 1) hits.Sort(static (a, b) => a.Start.CompareTo(b.Start));
    }

    /// <summary>
    /// The form a found value is hashed in, so that one value spelled two ways gets one token:
    /// a card number with or without spaces, an address in either case.
    /// </summary>
    public static string Normalize(MaskDetectors kind, ReadOnlySpan<char> value) => kind switch
    {
        MaskDetectors.CardNumber or MaskDetectors.Iin => OnlyDigits(value, keepPlus: false),
        MaskDetectors.Phone                           => OnlyDigits(value, keepPlus: true),
        MaskDetectors.Email                           => value.ToString().ToLowerInvariant(),
        _                                             => value.ToString(),
    };

    /// <summary>
    /// The detector's id: what the settings API calls it, and what a call's masking breakdown
    /// counts its finds under.
    /// </summary>
    public static string IdOf(MaskDetectors kind) => kind switch
    {
        MaskDetectors.CardNumber => "cardNumber",
        MaskDetectors.Email      => "email",
        MaskDetectors.Phone      => "phone",
        MaskDetectors.Iin        => "iin",
        MaskDetectors.Secret     => "secret",
        MaskDetectors.Iban       => "iban",
        _                        => "detector",
    };

    /// <summary>True for an integer that a detector would hide had it been written as text.</summary>
    public static bool IsSensitiveNumber(long value, MaskDetectors enabled, out MaskDetectors kind)
    {
        kind = MaskDetectors.None;
        if (value < 100_000_000_000) return false;   // under twelve digits: neither a card number nor an IIN
        Span<char> digits = stackalloc char[20];
        value.TryFormat(digits, out int n, default, System.Globalization.CultureInfo.InvariantCulture);
        digits = digits[..n];
        if ((enabled & MaskDetectors.CardNumber) != 0 && n is >= 13 and <= 19 && IsCardNumber(digits)) kind = MaskDetectors.CardNumber;
        else if ((enabled & MaskDetectors.Iin) != 0 && n == 12 && IsIin(digits))                       kind = MaskDetectors.Iin;
        return kind != MaskDetectors.None;
    }

    // ── Checks ────────────────────────────────────────────────────────────────

    /// <summary>Thirteen to nineteen digits that pass the Luhn check.</summary>
    internal static bool IsCardNumber(ReadOnlySpan<char> text)
    {
        int count = 0, sum = 0;
        bool doubled = false;
        for (int i = text.Length - 1; i >= 0; i--)
        {
            char c = text[i];
            if (c is ' ' or '-') continue;
            if (!char.IsAsciiDigit(c)) return false;
            int d = c - '0';
            if (doubled) { d *= 2; if (d > 9) d -= 9; }
            sum += d;
            doubled = !doubled;
            count++;
        }
        return count is >= 13 and <= 19 && sum % 10 == 0;
    }

    /// <summary>
    /// Kazakhstan's IIN and BIN share one check digit: the first eleven digits weighted 1…11,
    /// modulo 11; when that gives 10, weighted 3…11, 1, 2 instead; a second 10 means the
    /// number is not one.
    /// </summary>
    internal static bool IsIin(ReadOnlySpan<char> digits)
    {
        if (digits.Length != 12) return false;
        int first = 0, second = 0;
        for (int i = 0; i < 11; i++)
        {
            if (!char.IsAsciiDigit(digits[i])) return false;
            int d = digits[i] - '0';
            first  += d * (i + 1);
            second += d * ((i + 2) % 11 + 1);
        }
        if (!char.IsAsciiDigit(digits[11])) return false;
        int check = first % 11;
        if (check == 10) check = second % 11;
        return check != 10 && check == digits[11] - '0';
    }

    /// <summary>ISO 13616: move the first four characters to the end, letters as 10…35, modulo 97 is 1.</summary>
    internal static bool IsIban(ReadOnlySpan<char> text)
    {
        if (text.Length is < 15 or > 34) return false;
        int remainder = 0;
        for (int i = 0; i < text.Length; i++)
        {
            char c = text[(i + 4) % text.Length];
            if (char.IsAsciiDigit(c)) remainder = (remainder * 10 + (c - '0')) % 97;
            else if (c is >= 'A' and <= 'Z') remainder = (remainder * 100 + (c - 'A' + 10)) % 97;
            else return false;
        }
        return remainder == 1;
    }

    private static void Claim(List<Hit> hits, int start, int length, MaskDetectors kind)
    {
        foreach (var h in hits)
            if (start < h.Start + h.Length && h.Start < start + length) return;   // an earlier detector has it
        hits.Add(new Hit(start, length, kind));
    }

    private static int CountDigits(ReadOnlySpan<char> text)
    {
        int n = 0;
        foreach (char c in text) if (char.IsAsciiDigit(c)) n++;
        return n;
    }

    private static string OnlyDigits(ReadOnlySpan<char> text, bool keepPlus)
    {
        Span<char> buffer = text.Length <= 64 ? stackalloc char[text.Length] : new char[text.Length];
        int n = 0;
        foreach (char c in text)
            if (char.IsAsciiDigit(c) || (keepPlus && c == '+' && n == 0)) buffer[n++] = c;
        return new string(buffer[..n]);
    }
}
