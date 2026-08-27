using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace Ameto.Query.Filtering;

/// <summary>
/// Builds the <see cref="Regex"/> a filter node runs, ONCE, and keeps it.
///
/// <para>Every regex predicate used to call the static <c>Regex.IsMatch(input, pattern,
/// options)</c> per event: a cache probe keyed on the pattern string for each of possibly
/// millions of values, an interpreted engine, and — the part that matters — <b>no
/// timeout</b>. A pattern like <c>(a+)+$</c> against a long non-matching value is the
/// textbook catastrophic backtrack: one query, one core, indefinitely. A user typing that
/// into the search box is not an attacker, and the server should not fall over either way.</para>
///
/// <para>So patterns compile to the NON-BACKTRACKING engine, which is linear in the input
/// and cannot blow up. It does not implement backreferences or lookaround; those patterns
/// fall back to the backtracking engine, where a match timeout bounds the damage instead.
/// Only the fallback carries that timeout: the linear engine checks the clock too, and a
/// budget there would turn a big value on a loaded box into a silently missing row.</para>
/// </summary>
internal static class FilterRegex
{
    /// <summary>
    /// Per-VALUE budget for the fallback (backtracking) engine. Small on purpose: it is a
    /// bound on pathology, not a work allowance — a healthy pattern over a log value
    /// finishes in microseconds.
    /// </summary>
    private static readonly TimeSpan MatchTimeout = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Compiled patterns, shared across queries. Building a non-backtracking automaton is
    /// not free, and moving the compile out of the per-event path put it on the per-QUERY
    /// path — where live tail re-compiles the same filter four times a second, per client.
    /// Bounded and cleared wholesale rather than evicted: the population is the set of
    /// distinct patterns a deployment actually searches with, and it is small.
    /// </summary>
    private static readonly ConcurrentDictionary<(string Pattern, bool IgnoreCase), Regex> Cache = new();
    private const int MaxCached = 256;

    /// <exception cref="FormatException">
    /// The pattern is not a valid regex. Thrown as the parser's own error type so a bad
    /// pattern is reported the same way as any other filter syntax error — and at parse
    /// time, rather than from the middle of a result stream, which is where the static
    /// per-event call used to throw it.
    /// </exception>
    public static Regex Compile(string pattern, bool ignoreCase)
    {
        var key = (pattern, ignoreCase);
        if (Cache.TryGetValue(key, out var cached)) return cached;

        var options = RegexOptions.CultureInvariant
                    | (ignoreCase ? RegexOptions.IgnoreCase : RegexOptions.None);

        Regex built;
        try
        {
            // No timeout: this engine is linear, so there is nothing to bound, and a clock
            // here would drop rows on a slow box rather than protect anything.
            built = new Regex(pattern, options | RegexOptions.NonBacktracking);
        }
        catch (NotSupportedException)
        {
            // Backreferences, lookaround, atomic groups — the non-backtracking engine
            // refuses them at construction. The timeout is what stands in for it.
            built = new Regex(pattern, options, MatchTimeout);
        }
        catch (ArgumentException ex)   // RegexParseException derives from this
        {
            throw new FormatException($"Invalid regular expression '{pattern}': {ex.Message}", ex);
        }

        if (Cache.Count >= MaxCached) Cache.Clear();
        Cache[key] = built;
        return built;
    }
}
