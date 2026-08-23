using System.Text.RegularExpressions;

namespace Ameto.Query.Filtering;

/// <summary>
/// Builds the <see cref="Regex"/> a filter node runs, ONCE, at parse time.
///
/// <para>Every regex predicate used to call the static <c>Regex.IsMatch(input, pattern,
/// options)</c> per event: a cache probe keyed on the pattern string for each of possibly
/// millions of values, an interpreted engine, and — the part that matters — <b>no
/// timeout</b>. A pattern like <c>(a+)+$</c> against a long non-matching value is the
/// textbook catastrophic backtrack: one query, one core, indefinitely. A user typing that
/// into the search box is not an attacker, and the server should not fall over either way.</para>
///
/// <para>So patterns compile to the NON-BACKTRACKING engine, which is linear in the input
/// and cannot blow up at all. It does not implement backreferences or lookaround; those
/// patterns fall back to the backtracking engine, where a match timeout bounds the damage
/// instead. Either way the compile happens once per query, not once per event, and an
/// invalid pattern is reported when the filter is parsed rather than thrown from the
/// middle of a result stream.</para>
/// </summary>
internal static class FilterRegex
{
    /// <summary>
    /// Per-VALUE budget for the fallback (backtracking) engine. Small on purpose: it is a
    /// bound on pathology, not a work allowance — a healthy pattern over a log value
    /// finishes in microseconds.
    /// </summary>
    private static readonly TimeSpan MatchTimeout = TimeSpan.FromMilliseconds(100);

    public static Regex Compile(string pattern, bool ignoreCase)
    {
        var options = RegexOptions.CultureInvariant
                    | (ignoreCase ? RegexOptions.IgnoreCase : RegexOptions.None);

        try
        {
            return new Regex(pattern, options | RegexOptions.NonBacktracking, MatchTimeout);
        }
        catch (NotSupportedException)
        {
            // Backreferences, lookaround, atomic groups — the non-backtracking engine
            // refuses them at construction. The timeout is what stands in for it.
            return new Regex(pattern, options, MatchTimeout);
        }
    }
}
