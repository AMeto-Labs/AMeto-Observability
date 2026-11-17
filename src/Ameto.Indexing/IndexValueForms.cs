using System.Globalization;
using System.Runtime.CompilerServices;

namespace Ameto.Indexing;

/// <summary>
/// The one place that knows what a VALUE is called inside a segment index — the sibling of
/// <c>BuiltinFields</c>, which knows what a property is called — and, since the two have to agree,
/// what a value MEANS to the scan that the index stands in front of: the text a scalar reads as
/// (<see cref="PlainText"/>) and how its number compares (<see cref="TryCompareNumbers"/>).
/// <c>FilterEvaluator</c> reads values through both, so the index and the scan cannot come to
/// disagree about them again.
///
/// <para>Two encodings live here because the index uses both. The inverted index stores a
/// TYPE-TAGGED form (<see cref="Serialise"/>: <c>\0l5</c>, <c>\0d5</c>, <c>\0true</c>, bare
/// text for a string) so two properties with the same digits but different msgpack types stay
/// apart. The bloom filter stores the PLAIN text (<see cref="PlainText"/>) because it is
/// keyless and only ever asked "did any value in this segment look like this".</para>
///
/// <para>The query side has neither luxury: it knows only what the user typed, and
/// <c>FilterEvaluator</c>'s property comparison is deliberately coercing — a stored number by
/// value, in its own terms, against any literal that reads as one; a stored bool against the text
/// <c>'true'</c> or <c>'false'</c>; a stored string against a number's text — so ONE literal can
/// legitimately match several stored encodings. <see cref="Serialised"/> and <see cref="Plain"/> enumerate
/// them. Probing only the exact encoding let the index answer "absent" for a value the scan
/// matches, and both gates read absence as proof, so the segment was dropped unread.</para>
///
/// <para>Every rule the scan has here must be one the forms can follow with a short list, and
/// one the KEYLESS bloom can follow without passing everything. An index that covers more than
/// the scan costs a re-check, never a row; but "every double within 1e-15" and "every integer
/// that rounds to this double" cannot be listed at all, and "a stored false equals every literal
/// but 'true'" could only be followed by asking the bloom for <c>false</c> on behalf of every
/// literal — which every group holding a false anywhere answers yes to. The scan no longer has
/// those three rules.</para>
///
/// <para>Numbers are formatted INVARIANTLY. The grammar parses <c>2.5</c> with
/// <c>CultureInfo.InvariantCulture</c> (<c>FilterParser.ParseValue</c>, <see cref="TryReadDouble"/>)
/// while the index used to write it with <c>CurrentCulture</c>: on a <c>ru-KZ</c> host the bloom
/// held <c>2,5</c> and every quoted decimal literal missed it. Worse, the on-disk index silently
/// depended on the host locale, so changing a container's <c>LANG</c> would have orphaned every
/// numeric bucket already written. Segments written by an earlier build under such a locale are
/// still readable: the read-side form lists below add the current-culture spelling whenever it
/// differs. The scan renders numbers invariantly too (<see cref="PlainText"/>), so a filter means
/// the same thing on every host.</para>
/// </summary>
public static class IndexValueForms
{
    /// <summary>Room for the encodings one literal can match: the exact form, the plain text, an
    /// integer's tags, a double's and a float's — twice for zero, which also has a negative
    /// spelling — the culture variants of a double, and one bool bucket. A zero literal on a host
    /// whose culture spells numbers differently is the longest list, at eight; the rest is margin,
    /// since running out throws.</summary>
    public const int MaxForms = 12;

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
    /// The untagged text of a value: what the bloom filter holds for it, the text
    /// <c>SegmentIndexBuilder</c> must add so the two agree by construction — and the text the
    /// scan reads a non-string scalar as, for <c>contains</c>, <c>like</c>, <c>in</c> and every
    /// other comparison of text. Invariant, so a filter means the same on every host: <c>12.5</c>,
    /// never <c>12,5</c>, and a minus that is <c>-</c>, never U+2212.
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
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _         => value.ToString() ?? string.Empty,
    };

    // ── What number a value is ────────────────────────────────────────────────
    //
    // A property is compared in its OWN terms, which the value it was written with decides:
    //
    // - an INTEGER property (a msgpack integer: long, or ulong past long.MaxValue) exactly, as the
    //   integer it is. What it is compared with must denote an integer exactly to equal it — a
    //   long literal, an integral double, text such as '1234567890123456789', '1e3' or '5.0'.
    //   Through a double, as the scan used to, every integer that rounds to the same double was
    //   "equal": order 1234567890123456789 was order …790 as well, and no index can list that band.
    // - a FLOATING-POINT property (a msgpack float) as a double, by IEEE equality: the literal is
    //   read as the double nearest it — the reading that gives back the stored double from its own
    //   text, or from the shortest digits a browser shows for it — so -0.0 equals 0 and nothing
    //   else is within a tolerance. The scan's old 1e-15 tolerance was a band no index can list.
    //
    // Either way ONE literal names one integer and one double, which is what makes the forms below
    // a short list.

    /// <summary>2^127: every integral double below it in magnitude converts to <see cref="Int128"/> exactly.</summary>
    private const double TwoTo127 = 170141183460469231731687303715884105728d;

    /// <summary>
    /// The integer <paramref name="value"/> denotes EXACTLY, if it denotes one: any integer type, an
    /// integral double (-0.0 is 0), or numeric text that parses as an integer with
    /// <c>NumberStyles.Any</c> in the invariant culture — <c>'1234567890123456789'</c>, <c>'1e3'</c>,
    /// <c>'5.0'</c>, but not <c>'2.5'</c> and not <c>'1e400'</c>.
    /// </summary>
    public static bool TryReadInteger(object? value, out Int128 integer)
    {
        switch (value)
        {
            case long l:    integer = l;   return true;
            case ulong u:   integer = u;   return true;
            case int i:     integer = i;   return true;
            case uint ui:   integer = ui;  return true;
            case short s16: integer = s16; return true;
            case ushort u16:integer = u16; return true;
            case byte b:    integer = b;   return true;
            case sbyte sb:  integer = sb;  return true;
            case double d:  return TryIntegral(d, out integer);
            case float f:   return TryIntegral(f, out integer);
            case decimal m when decimal.Truncate(m) == m:
                integer = (Int128)m;
                return true;
            case string s:
                return Int128.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out integer);
        }
        integer = 0;
        return false;
    }

    private static bool TryIntegral(double d, out Int128 integer)
    {
        if (double.IsInteger(d) && Math.Abs(d) < TwoTo127)
        {
            integer = (Int128)d;
            return true;
        }
        integer = 0;
        return false;
    }

    /// <summary>
    /// The double <paramref name="value"/> reads as: a number converted (the nearest double), numeric
    /// text parsed with <c>NumberStyles.Any</c> in the invariant culture, as the grammar parses a
    /// literal. NaN is a double too — <c>'NaN'</c> parses — but equals nothing.
    /// </summary>
    public static bool TryReadDouble(object? value, out double d)
    {
        switch (value)
        {
            case double x:  d = x;   return true;
            case float f:   d = f;   return true;
            case long l:    d = l;   return true;
            case ulong u:   d = u;   return true;
            case int i:     d = i;   return true;
            case uint ui:   d = ui;  return true;
            case short s16: d = s16; return true;
            case ushort u16:d = u16; return true;
            case byte b:    d = b;   return true;
            case sbyte sb:  d = sb;  return true;
            case decimal m: d = (double)m; return true;
            case string s:
                return double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out d);
        }
        d = double.NaN;
        return false;
    }

    /// <summary>
    /// Orders <paramref name="value"/>, a property's number, against <paramref name="other"/>, what it
    /// is compared with, in the property's own terms (see above): the sign of <paramref name="order"/>
    /// says which is greater, 0 that they are equal. False — unordered, so neither equal nor unequal —
    /// when <paramref name="value"/> is not a number, <paramref name="other"/> reads as no number of
    /// its kind, or either is NaN.
    /// </summary>
    public static bool TryCompareNumbers(object? value, object? other, out int order)
    {
        order = 0;
        switch (value)
        {
            case double or float:
                if (!TryReadDouble(value, out double v) || !TryReadDouble(other, out double o)
                    || double.IsNaN(v) || double.IsNaN(o))
                    return false;
                order = v.CompareTo(o);           // -0.0 and 0.0 are equal to CompareTo, as to ==
                return true;

            case long or ulong or int or uint or short or ushort or byte or sbyte:
                TryReadInteger(value, out Int128 integer);
                if (TryReadInteger(other, out Int128 exact))
                {
                    order = integer.CompareTo(exact);
                    return true;
                }
                if (!TryReadDouble(other, out double real) || double.IsNaN(real)) return false;

                // Not an integer, so not equal — but its double may be: past 2^52 every double is
                // an integer, and text such as '1234567890123456789.5' rounds onto one. Then the
                // double cannot say which side is greater, and the pair is unordered, like NaN.
                order = CompareIntegerWithReal(integer, real);
                return order != 0;

            default:
                return false;
        }
    }

    /// <summary>
    /// An integer against a double, exactly whenever the answer is not 0. Past 2^127, infinities
    /// included, the double is beyond every integer and its sign decides. Below that, converting the
    /// integer to a double is monotonic, so a strict answer is the true one; 0 means the two doubles
    /// met, which a fraction (always short of 2^52) never does with an integer.
    /// </summary>
    private static int CompareIntegerWithReal(Int128 integer, double real)
        => Math.Abs(real) >= TwoTo127 ? (real > 0 ? -1 : 1) : ((double)integer).CompareTo(real);

    // ── Read side: every encoding one literal may legitimately match ──────────

    /// <summary>
    /// Every type-tagged encoding a STORED value can have that the scan's property comparison
    /// (<c>FilterEvaluator</c>'s <c>=</c>) would consider equal to <paramref name="value"/> —
    /// which also covers <c>in</c>, whose textual match is a subset of it. Returns the count
    /// written into <paramref name="forms"/>.
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
        // posting dictionary itself uses), so the untagged, invariant spelling always qualifies.
        string text = PlainText(value);
        Append(forms, ref n, text);

        // A stored INTEGER equals the one integer the literal denotes exactly, if it denotes one:
        // its digits, under the tag the builder files an integer of that size with — "\0l" for a
        // long (and "\0i" for an int, an earlier build's tag), bare digits for a uint64 past
        // long.MaxValue. At any magnitude: 2^53 is a double's limit, not a long's.
        if (TryReadInteger(value, out Int128 integer))
        {
            string digits = integer.ToString(CultureInfo.InvariantCulture);
            if (integer >= long.MinValue && integer <= long.MaxValue)
            {
                Append(forms, ref n, "\0l" + digits);
                if (integer >= int.MinValue && integer <= int.MaxValue) Append(forms, ref n, "\0i" + digits);
            }
            else if (integer > long.MaxValue && integer <= ulong.MaxValue)
            {
                Append(forms, ref n, digits);
            }
        }

        // A stored DOUBLE equals the one double the literal reads as. Zero is two of them: -0.0
        // equals 0, and the builder files it under its own spelling, "-0". NaN equals nothing,
        // but `in` compares text, and a stored NaN reads "NaN".
        if (TryReadDouble(value, out double d))
        {
            AppendDouble(forms, ref n, d);
            if (d == 0) AppendDouble(forms, ref n, -d);
        }

        // A stored BOOL equals a non-bool literal only when the literal's text names it: 'true'
        // or 'false', in any case. Every other literal equals no bool, so names no bool bucket.
        if (value is not bool)
        {
            if (string.Equals(text, "true", StringComparison.OrdinalIgnoreCase))       Append(forms, ref n, "\0true");
            else if (string.Equals(text, "false", StringComparison.OrdinalIgnoreCase)) Append(forms, ref n, "\0false");
        }

        return n;
    }

    /// <summary>A stored double's bucket, a stored float's (an earlier build's tag) when the float
    /// holds the same number, and the bucket a pre-invariant build filed the double under.</summary>
    private static void AppendDouble(Span<string?> forms, ref int n, double d)
    {
        Append(forms, ref n, "\0d" + d.ToString("R", CultureInfo.InvariantCulture));
        float f = (float)d;
        if ((double)f == d)
            Append(forms, ref n, "\0f" + f.ToString("R", CultureInfo.InvariantCulture));

        // Back-compat: a pre-invariant build wrote the host's decimal separator and minus sign.
        if (!CultureInfo.CurrentCulture.Equals(CultureInfo.InvariantCulture))
            Append(forms, ref n, "\0d" + d.ToString("R", CultureInfo.CurrentCulture));
    }

    /// <summary>
    /// Every PLAIN text the bloom filter could be holding for a value the scan would accept.
    /// The bloom is keyless, so this is the only question it can be asked correctly — and it
    /// has to be asked for every bucket <see cref="Serialised"/> names, or the cheap gate drops
    /// a group the inverted index would have kept.
    /// </summary>
    public static int Plain(object? value, Span<string?> forms)
    {
        int n = 0;
        string text = PlainText(value);
        Append(forms, ref n, text);

        // A stored integer's digits, and a stored double's round-trip text (both zeros for 0).
        if (TryReadInteger(value, out Int128 integer))
            Append(forms, ref n, integer.ToString(CultureInfo.InvariantCulture));
        if (TryReadDouble(value, out double d))
        {
            AppendDoubleText(forms, ref n, d);
            if (d == 0) AppendDoubleText(forms, ref n, -d);
        }

        // A stored bool reaches the bloom as "true"/"false" (folded). The only non-bool literals
        // that equal one are those texts, which the filter finds through the literal's own text
        // above, since it folds case. Nothing else is asked for here, and that is deliberate: the
        // bloom is keyless, and a "false" asked on behalf of every literal would pass every group
        // holding a false anywhere — see the bool rule in FilterEvaluator.Compare.
        return n;
    }

    private static void AppendDoubleText(Span<string?> forms, ref int n, double d)
    {
        Append(forms, ref n, d.ToString("R", CultureInfo.InvariantCulture));
        if (!CultureInfo.CurrentCulture.Equals(CultureInfo.InvariantCulture))
            Append(forms, ref n, d.ToString("R", CultureInfo.CurrentCulture));
    }

    private static void Append(Span<string?> forms, ref int n, string form)
    {
        for (int i = 0; i < n; i++)
            if (string.Equals(forms[i], form, StringComparison.Ordinal)) return;

        // A form dropped here would be a row lost without a trace, so running out of room throws —
        // the query fails and says so. MaxForms counts the longest list the methods above build,
        // and ValueFormsCoverTheScanTests pins it.
        if (n == forms.Length)
            throw new InvalidOperationException($"IndexValueForms: more than {forms.Length} forms for one value.");
        forms[n++] = form;
    }
}
