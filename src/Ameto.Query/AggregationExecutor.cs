using System.Globalization;
using Ameto.Core;
using Ameto.Query.Filtering;

namespace Ameto.Query;

/// <summary>One row of an aggregation: the group's key values and its computed columns.</summary>
public sealed class AggregationRow
{
    /// <summary>Key values in <c>group by</c> order. Null means the event carried no such property.</summary>
    public required string?[] Key { get; init; }

    /// <summary>Computed values in select order. Null where the group had nothing to compute from.</summary>
    public required double?[] Values { get; init; }
}

/// <summary>The table an aggregation answers with, plus what it had to leave out.</summary>
public sealed class AggregationResult
{
    public required IReadOnlyList<string>         KeyColumns   { get; init; }
    public required IReadOnlyList<string>         ValueColumns { get; init; }
    public required IReadOnlyList<AggregationRow> Rows         { get; init; }

    /// <summary>Events read. Not the number matched — the scan counts what it looked at.</summary>
    public required long Scanned { get; init; }

    /// <summary>Distinct groups seen, which can exceed <see cref="Rows"/> when a limit applied.</summary>
    public required int GroupsFound { get; init; }

    /// <summary>
    /// True when the answer is a floor rather than a count: the scan hit its time budget, its
    /// event budget, or the cap on distinct groups. A partial aggregation that says it is
    /// complete is worse than no aggregation at all — it looks like an answer.
    /// </summary>
    public required bool Partial { get; init; }

    /// <summary>Why, in a sentence, when <see cref="Partial"/>. Null otherwise.</summary>
    public string? PartialReason { get; init; }
}

/// <summary>
/// Runs an <see cref="AggregationQuery"/> over the ordinary scan.
///
/// <para>It deliberately owns no reading of its own: the where-clause goes to
/// <see cref="IQueryExecutor"/> as filter text, so an aggregation gets the same index hints,
/// level pruning, time-bound folding and tier merge as the search it is spelled beside. What
/// this adds is the accumulation, and the accounting for what it could not finish.</para>
/// </summary>
/// <param name="scanBudget">
/// Overrides <see cref="MaxScanned"/>. Exists so a test can reach the cap without writing two
/// million events — the branch that reports a truncated answer is the one that must not be
/// taken on trust.
/// </param>
public sealed class AggregationExecutor(IQueryExecutor executor, int scanBudget = AggregationExecutor.MaxScanned)
{
    /// <summary>
    /// Events one aggregation may read. A group-by has no natural stopping point — it is the
    /// whole window by definition — so this is the difference between a slow answer and a
    /// server one query can occupy. Reaching it marks the result partial rather than
    /// truncating it silently.
    /// </summary>
    public const int MaxScanned = 2_000_000;

    public async Task<AggregationResult> ExecuteAsync(
        AggregationQuery   query,
        DateTimeOffset?    fromUtc,
        DateTimeOffset?    toUtc,
        CancellationToken  ct = default)
    {
        var keys  = query.Keys;
        var aggs  = query.Aggregates;
        var groups = new Dictionary<string, Accumulator>(StringComparer.Ordinal);

        long scanned      = 0;
        bool hitScanCap   = false;
        bool hitGroupCap  = false;

        // A query with no `group by` asks one question and must get one answer, even when the
        // answer is zero. Seeding the single group here rather than discovering it from the
        // first event is the difference between "no errors" and "no result" — and the second
        // reads as a broken query.
        if (keys.Count == 0)
            groups.Add("", new Accumulator([], aggs.Count));

        var request = new QueryRequest
        {
            Filter    = query.FilterText,
            FromUtc   = fromUtc,
            ToUtc     = toUtc,
            // ONE MORE than the budget, so the loop below can SEE that there was more. Asking
            // for exactly the budget makes the two caps disagree by one: QueryExecutor stops at
            // `count >= limit`, emitting the budget and never one event beyond it, so an
            // in-loop test for having exceeded it can never be true. The scan would then end
            // normally, nothing would set the flag, and a result truncated to the newest two
            // million events would be reported as the whole window. AlertEvaluator pairs the
            // same two numbers correctly; this is that pairing.
            Count     = scanBudget + 1,
            Direction = QueryDirection.Backward,
        };

        await foreach (var ev in executor.ExecuteAsync(request, ct).ConfigureAwait(false))
        {
            // Checked BEFORE accounting for this event, so `scanned` ends at the budget rather
            // than one past it, and a window holding exactly the budget is not called partial.
            if (scanned == scanBudget) { hitScanCap = true; break; }
            scanned++;

            // The key is built before the lookup so a group that already exists costs one
            // dictionary probe and no allocation beyond the joined string itself.
            var (composite, parts) = BuildKey(ev, keys);

            if (!groups.TryGetValue(composite, out var acc))
            {
                if (groups.Count >= AggregationParser.MaxGroups) { hitGroupCap = true; continue; }
                acc = new Accumulator(parts, aggs.Count);
                groups.Add(composite, acc);
            }
            acc.Add(ev, aggs);
        }

        bool timedOut = ct.IsCancellationRequested;

        var rows = groups.Values
            .Select(a => new AggregationRow { Key = a.Key, Values = a.Snapshot(aggs) })
            .OrderByDescending(r => r.Values.Length > 0 ? r.Values[0] ?? double.MinValue : 0d)
            .ThenBy(r => string.Concat(r.Key), StringComparer.Ordinal)
            .Take(query.Limit)
            .ToArray();

        string? reason =
            timedOut    ? "the query ran out of time — narrow the window or the filter" :
            hitScanCap  ? $"more than {scanBudget:N0} events matched — narrow the window or the filter" :
            hitGroupCap ? $"more than {AggregationParser.MaxGroups:N0} distinct groups — group by something coarser" :
            null;

        return new AggregationResult
        {
            KeyColumns    = keys.Select(k => k.Alias).ToArray(),
            ValueColumns  = aggs.Select(a => a.Alias).ToArray(),
            Rows          = rows,
            Scanned       = scanned,
            GroupsFound   = groups.Count,
            Partial       = reason is not null,
            PartialReason = reason,
        };
    }

    /// <summary>
    /// The group's identity, as one string for the dictionary and as its parts for the row.
    /// A scalar aggregation has a single empty key, so it takes the same path as everything
    /// else rather than a branch of its own.
    /// </summary>
    private static (string Composite, string?[] Parts) BuildKey(LogEvent ev, IReadOnlyList<GroupKeySpec> keys)
    {
        if (keys.Count == 0) return ("", []);

        var parts = new string?[keys.Count];
        for (int i = 0; i < keys.Count; i++)
            parts[i] = Stringify(FilterEvaluator.ReadProperty(ev, keys[i].Property));

        // Joined with the control characters the path encoding already relies on being absent
        // from msgpack keys and values: U+0001 between parts, so two keys ['a','b'] and
        // ['ab'] stay different groups, and U+0002 for a value the event did not carry, so
        // "absent" does not merge with the group whose value is genuinely the empty string.
        return (string.Join(PropertyPath.Separator,
                            parts.Select(static p => p ?? PropertyPath.IndexMarker.ToString())),
                parts);
    }

    /// <summary>
    /// A group key has to be one value. Anything the decoder hands back as a CONTAINER — an
    /// array, a nested map, a byte string — is reported as absent rather than stringified:
    /// none of those types overrides ToString(), so every array collapsed to the single group
    /// "System.Object[]" and every nested object to a Dictionary type name, merging values
    /// that are not remotely equal and putting a .NET type into the public response. OTLP
    /// array attributes and destructured CLEF objects both arrive here.
    /// </summary>
    private static string? Stringify(object? v) => v switch
    {
        null                                 => null,
        string s                             => s,
        bool b                               => b ? "true" : "false",
        // Arrays, lists and maps — the decoder's object[] / Dictionary / byte[]. Matched by
        // IEnumerable rather than by type so a future decoder shape cannot slip past.
        System.Collections.IEnumerable       => null,
        IFormattable f                       => f.ToString(null, CultureInfo.InvariantCulture),
        _                                    => v.ToString(),
    };

    /// <summary>Per-group running state. One array per aggregate, no boxing of the running values.</summary>
    private sealed class Accumulator(string?[] key, int columns)
    {
        public string?[] Key { get; } = key;

        private readonly long[]   _counts = new long[columns];
        private readonly double[] _sums   = new double[columns];
        private readonly double[] _mins   = Filled(columns, double.MaxValue);
        private readonly double[] _maxs   = Filled(columns, double.MinValue);

        private static double[] Filled(int n, double v)
        {
            var a = new double[n];
            Array.Fill(a, v);
            return a;
        }

        public void Add(LogEvent ev, IReadOnlyList<AggregateSpec> aggs)
        {
            for (int i = 0; i < aggs.Count; i++)
            {
                var spec = aggs[i];
                if (spec.Property is null) { _counts[i]++; continue; }   // count(*)

                object? raw = FilterEvaluator.ReadProperty(ev, spec.Property);
                if (raw is null) continue;                               // absent: contributes nothing

                if (spec.Kind == AggregateKind.Count) { _counts[i]++; continue; }
                if (!TryNumber(raw, out double d)) continue;             // sum('abc') is not an error, just no data

                _counts[i]++;
                _sums[i] += d;
                if (d < _mins[i]) _mins[i] = d;
                if (d > _maxs[i]) _maxs[i] = d;
            }
        }

        public double?[] Snapshot(IReadOnlyList<AggregateSpec> aggs)
        {
            var values = new double?[aggs.Count];
            for (int i = 0; i < aggs.Count; i++)
            {
                // An empty group has no minimum and no average — reporting 0 would be a
                // number the data does not contain.
                values[i] = aggs[i].Kind switch
                {
                    AggregateKind.Count => _counts[i],
                    AggregateKind.Sum   => _counts[i] > 0 ? _sums[i] : null,
                    AggregateKind.Min   => _counts[i] > 0 ? _mins[i] : null,
                    AggregateKind.Max   => _counts[i] > 0 ? _maxs[i] : null,
                    AggregateKind.Avg   => _counts[i] > 0 ? _sums[i] / _counts[i] : null,
                    _                   => null,
                };

                // Belt and braces against a sum that overflowed to infinity on the way: a
                // value JSON cannot express must not reach the serialiser, which throws where
                // the endpoint cannot catch it.
                if (values[i] is { } v && !double.IsFinite(v)) values[i] = null;
            }
            return values;
        }

        private static bool TryNumber(object v, out double d)
        {
            switch (v)
            {
                // Every case BREAKS rather than returning, so all of them go through the finite
                // check below. A `double` straight off the wire is the commonest way NaN gets
                // in, so the arm that returned early was the one that mattered most.
                case long l:    d = l; break;
                case int i:     d = i; break;
                case double x:  d = x; break;
                case float f:   d = f; break;
                case decimal m: d = (double)m; break;
                case bool b:    d = b ? 1 : 0; break;
                // TryParse accepts "NaN", "Infinity" and "-Infinity" — the likelier route in,
                // since a CLEF producer writes whatever its serialiser will emit.
                case string s:  if (!double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out d)) return false;
                                break;
                default:        d = 0; return false;
            }

            // NOT A NUMBER IS NOT A VALUE. NaN compares false against everything, so it would
            // slip past `d < _mins[i]` while still bumping the count — and Snapshot, which
            // gates on the count, would hand back the double.MaxValue/MinValue seeds as if the
            // data contained them, with min above max. Infinity is worse: it poisons the sum,
            // and Utf8JsonWriter refuses to write it, from OUTSIDE the endpoint's try/catch
            // (Results.Json only builds the result; serialisation happens after the handler
            // returns), so the client would get an unlogged 500 with a half-written body.
            return double.IsFinite(d);
        }
    }
}
