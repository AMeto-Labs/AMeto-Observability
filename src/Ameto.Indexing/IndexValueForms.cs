using System.Globalization;
using System.Runtime.CompilerServices;

namespace Ameto.Indexing;

/// <summary>
/// The one place that knows what a VALUE is called inside a segment index — the sibling of
/// <c>BuiltinFields</c>, which knows what a property is called.
///
/// <para>Two encodings live here because the index uses both. The inverted index stores a
/// TYPE-TAGGED form (<see cref="Serialise"/>: <c>\0l5</c>, <c>\0d5</c>, <c>\0true</c>, bare
/// text for a string) so two properties with the same digits but different msgpack types stay
/// apart. The bloom filter stores the PLAIN text (<see cref="PlainText"/>) because it is
/// keyless and only ever asked "did any value in this segment look like this".</para>
///
/// <para>The query side has neither luxury: it knows only what the user typed.
/// <c>FilterEvaluator.Compare</c> is deliberately coercing — numbers across types through
/// <c>ToDouble</c>, a stored bool against the literal's text, a quoted numeral parsed into a
/// double — so ONE literal can legitimately match several stored encodings.
/// <see cref="Serialised"/> and <see cref="Plain"/> enumerate them. Probing only the exact
/// encoding let the index answer "absent" for a value the scan matches, and both gates read
/// absence as proof, so the segment was dropped unread.</para>
///
/// <para>Numbers are formatted INVARIANTLY. The grammar parses <c>2.5</c> with
/// <c>CultureInfo.InvariantCulture</c> (<c>FilterParser.ParseValue</c>,
/// <c>FilterEvaluator.ToDouble</c>) while the index used to write it with
/// <c>CurrentCulture</c>: on a <c>ru-KZ</c> host the bloom held <c>2,5</c> and every quoted
/// decimal literal missed it. Worse, the on-disk index silently depended on the host locale,
/// so changing a container's <c>LANG</c> would have orphaned every numeric bucket already
/// written. Segments written by an earlier build under such a locale are still readable: the
/// read-side form lists below add the current-culture spelling whenever it differs.</para>
/// </summary>
public static class IndexValueForms
{
    /// <summary>Upper bound on the encodings one literal can match — enough for the exact
    /// form, the plain text, four numeric tags, one bool bucket and a culture variant.</summary>
    public const int MaxForms = 9;

    /// <summary>Stack scratch for the form lists: managed references cannot be
    /// <c>stackalloc</c>ed, and asking a question should not allocate.</summary>
    [InlineArray(MaxForms)]
    public struct Buffer { private string? _element0; }

    // ── Write side: the single encoding a stored value gets ───────────────────

    /// <summary>The type-tagged form the inverted index files a value under.</summary>
    public static string Serialise(object? value) => value switch
    {
        null     => "\0null",
        bool b   => b ? "\0true" : "\0false",
        int i    => "\0i" + i.ToString(CultureInfo.InvariantCulture),
        long l   => "\0l" + l.ToString(CultureInfo.InvariantCulture),
        double d => "\0d" + d.ToString("R", CultureInfo.InvariantCulture),
        float f  => "\0f" + f.ToString("R", CultureInfo.InvariantCulture),
        string s => s,
        _        => value.ToString() ?? string.Empty,
    };

    /// <summary>
    /// The untagged text the bloom filter holds for a value — and the text
    /// <c>SegmentIndexBuilder</c> must add, so the two agree by construction.
    /// </summary>
    public static string PlainText(object? value) => value switch
    {
        null      => string.Empty,
        bool b    => b ? "True" : "False",
        string s  => s,
        int i     => i.ToString(CultureInfo.InvariantCulture),
        long l    => l.ToString(CultureInfo.InvariantCulture),
        ulong u   => u.ToString(CultureInfo.InvariantCulture),
        short s16 => s16.ToString(CultureInfo.InvariantCulture),
        byte bt   => bt.ToString(CultureInfo.InvariantCulture),
        double d  => d.ToString("R", CultureInfo.InvariantCulture),
        float f   => f.ToString("R", CultureInfo.InvariantCulture),
        _         => value.ToString() ?? string.Empty,
    };

    // ── Read side: every encoding one literal may legitimately match ──────────

    /// <summary>
    /// Every type-tagged encoding a STORED value can have that <c>FilterEvaluator.Compare</c>
    /// would consider equal to <paramref name="value"/>. Returns the count written into
    /// <paramref name="forms"/>.
    /// </summary>
    public static int Serialised(object? value, Span<string?> forms)
    {
        // `X = null` matches a MISSING property, which no bucket records, so the literal nil
        // is the only encoding worth probing — Compare short-circuits every cross-type case
        // the moment either side is null.
        if (value is null) { forms[0] = "\0null"; return 1; }

        int n = 0;
        Append(forms, ref n, Serialise(value));

        // A stored STRING is compared against the literal's text (OrdinalIgnoreCase, which the
        // posting dictionary itself uses), so the untagged spelling always qualifies.
        Append(forms, ref n, PlainText(value));
        if (value is not string) Append(forms, ref n, value.ToString() ?? string.Empty);

        // Stored numbers are compared cross-type through ToDouble, so every numeric tag that
        // formats to the same number qualifies — including from a quoted literal.
        if (TryAsDouble(value, out double d))
        {
            // Only inside 2^53 does the long round-trip exactly; past it a double cannot name
            // a distinct integer anyway.
            if (d == Math.Truncate(d) && Math.Abs(d) <= 9007199254740992d)
            {
                string digits = ((long)d).ToString(CultureInfo.InvariantCulture);
                Append(forms, ref n, "\0l" + digits);
                Append(forms, ref n, "\0i" + digits);
            }
            Append(forms, ref n, "\0d" + d.ToString("R", CultureInfo.InvariantCulture));
            Append(forms, ref n, "\0f" + ((float)d).ToString("R", CultureInfo.InvariantCulture));

            // Back-compat: a pre-invariant build wrote the host's decimal separator.
            if (!CultureInfo.CurrentCulture.Equals(CultureInfo.InvariantCulture))
                Append(forms, ref n, "\0d" + d.ToString("R", CultureInfo.CurrentCulture));
        }

        // A stored BOOL is compared against the literal's TEXT ("true" ⇒ true, anything else
        // ⇒ false), so exactly one bool bucket can match a non-bool literal.
        if (value is not bool)
            Append(forms, ref n,
                string.Equals(PlainText(value), "true", StringComparison.OrdinalIgnoreCase) ? "\0true" : "\0false");

        return n;
    }

    /// <summary>
    /// Every PLAIN text the bloom filter could be holding for a value the scan would accept.
    /// The bloom is keyless, so this is the only question it can be asked correctly.
    /// </summary>
    public static int Plain(object? value, Span<string?> forms)
    {
        int n = 0;
        Append(forms, ref n, PlainText(value));

        if (TryAsDouble(value, out double d))
        {
            if (d == Math.Truncate(d) && Math.Abs(d) <= 9007199254740992d)
                Append(forms, ref n, ((long)d).ToString(CultureInfo.InvariantCulture));
            Append(forms, ref n, d.ToString("R", CultureInfo.InvariantCulture));
            if (!CultureInfo.CurrentCulture.Equals(CultureInfo.InvariantCulture))
                Append(forms, ref n, d.ToString("R", CultureInfo.CurrentCulture));
        }

        // A stored bool reaches the bloom as "True"/"False"; the filter folds case, so a
        // quoted 'true' finds it through the plain form above.
        return n;
    }

    private static void Append(Span<string?> forms, ref int n, string form)
    {
        if (n == MaxForms) return;
        for (int i = 0; i < n; i++)
            if (string.Equals(forms[i], form, StringComparison.Ordinal)) return;
        forms[n++] = form;
    }

    /// <summary>Mirrors <c>FilterEvaluator.ToDouble</c> for the types a filter literal or a
    /// msgpack scalar can be — same invariant-culture string parse, so the two agree.</summary>
    private static bool TryAsDouble(object? value, out double d)
    {
        switch (value)
        {
            case long   l: d = l; return true;
            case int    i: d = i; return true;
            case short s16: d = s16; return true;
            case byte   b: d = b; return true;
            case ulong  u: d = u; return true;
            case double x: d = x; return !double.IsNaN(x);
            case float  f: d = f; return !float.IsNaN(f);
            case string s:
                return double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out d);
            default: d = 0; return false;
        }
    }
}
