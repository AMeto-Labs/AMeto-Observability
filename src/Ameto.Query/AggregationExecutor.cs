using System.Globalization;
using Ameto.Storage;
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

    /// <summary>
    /// Events read. NOT the number matched — this counts what the query looked at, which is why
    /// it can exceed the row counts below and why the UI prints it as "events read".
    ///
    /// <para>What "looked at" means differs by road, and the difference is visible. The event
    /// scan counts events the filter YIELDED, because that is all it ever sees. The header scan
    /// counts every in-window HEADER it walked, before its service filter and before any level
    /// narrowing — so the same question answered the fast way reports a larger number. Both are
    /// honest answers to "how much did this cost"; neither is a count of matches, and no client
    /// should read it as one.</para>
    /// </summary>
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
public sealed class AggregationExecutor(
    IQueryExecutor executor,
    int scanBudget = AggregationExecutor.MaxScanned,
    Ameto.Storage.StorageEngine? headerScan = null)
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
        // A shape the header scan can answer never reaches the event scan at all.
        if (await TryHeaderCountAsync(query, fromUtc, toUtc, ct).ConfigureAwait(false) is { } headerAnswer)
            return headerAnswer;

        var keys  = query.Keys;
        var aggs  = query.Aggregates;
        var groups = new Dictionary<string, Accumulator>(StringComparer.Ordinal);
        var byComposite = groups.GetAlternateLookup<ReadOnlySpan<char>>();
        var keyBuilder  = new GroupKeyBuilder(keys);

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

        // The scan is wrapped because cancellation does NOT arrive uniformly. Most of the
        // executor turns an expired token into a quiet `yield break`, but the cold prefilter
        // runs under Parallel.ForEachAsync with the token in ParallelOptions, and that THROWS.
        // Unwrapped, a timeout landing during the prefilter escaped to the endpoint and became
        // a bare 504 — losing the groups already counted and, worse, replacing the documented
        // `partial: true` answer with something that looks like a different failure entirely.
        try
        {
        await foreach (var ev in executor.ExecuteAsync(request, ct).ConfigureAwait(false))
        {
            // Checked BEFORE accounting for this event, so `scanned` ends at the budget rather
            // than one past it, and a window holding exactly the budget is not called partial.
            if (scanned == scanBudget) { hitScanCap = true; break; }
            scanned++;

            // The key is built before the lookup, and probed as a SPAN, so a group that already
            // exists — which is the overwhelmingly common case — costs one dictionary probe and
            // no allocation at all.
            keyBuilder.Build(ev);

            if (!byComposite.TryGetValue(keyBuilder.Composite, out var acc))
            {
                if (groups.Count >= AggregationParser.MaxGroups) { hitGroupCap = true; continue; }
                acc = new Accumulator(keyBuilder.TakeParts(), aggs.Count);
                groups.Add(new string(keyBuilder.Composite), acc);
            }
            acc.Add(ev, aggs);
        }
        }
        catch (OperationCanceledException) { /* reported as partial below, like every other stop */ }

        bool timedOut = ct.IsCancellationRequested;

        var rows = OrderAndLimit(
            groups.Values.Select(a => new AggregationRow { Key = a.Key, Values = a.Snapshot(aggs) }),
            query.Limit);

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
    /// Biggest first, ties broken by the key so the answer is stable, then the caller's limit.
    /// ONE spelling of the row order, shared by the scan and the header scan — two orderings
    /// that were meant to agree would eventually stop agreeing.
    /// </summary>
    private static AggregationRow[] OrderAndLimit(IEnumerable<AggregationRow> rows, int limit) =>
        rows.OrderByDescending(r => r.Values.Length > 0 ? r.Values[0] ?? double.MinValue : 0d)
            .ThenBy(r => string.Concat(r.Key), StringComparer.Ordinal)
            .Take(limit)
            .ToArray();

    // ── The header-only shortcut ──────────────────────────────────────────────

    /// <summary>
    /// What the header scan can be asked to group by.
    ///
    /// <para>There is no <c>Service</c> member, and its absence is a correctness decision, not
    /// an omission — see <see cref="TryHeaderCountAsync"/>.</para>
    /// </summary>
    private enum HeaderGrouping { None, Level }

    /// <summary>
    /// Answers a <c>count(*)</c> whose grouping and where-clause live entirely in the event
    /// HEADER, without materialising a single <see cref="LogEvent"/>.
    ///
    /// <para><c>select count(*) group by ['service.name']</c> over a wide window used to run the
    /// full ordered k-way merge — every event decoded, its properties copied, its exception
    /// rebuilt — to look at three columns. The header aggregator behind
    /// <c>/api/events/counts</c> already reads exactly those three columns, in parallel across
    /// segments, and the alert evaluator already trusts <c>TryGetHeaderOnlyShape</c> to say when
    /// a filter is expressible that way. This routes the aggregation down the same road.</para>
    ///
    /// <para>DELIBERATELY NARROW; anything unrecognised returns null and the ordinary scan runs.
    /// Every aggregate must be <c>count(*)</c>, there may be at most one group key and it must
    /// be <c>@l</c>, and the filter must reduce to a header-only shape (which excludes any
    /// <c>@t</c> bound — those compile to a TimeCompareNode, which that shape rejects).</para>
    ///
    /// <para>GROUPING BY <c>service.name</c> IS DECLINED, although it is the shape that would
    /// gain most, because the header aggregator cannot reproduce the scan's groups exactly and
    /// an aggregation is read as a fact. It keys services case-INSENSITIVELY and a series keeps
    /// whichever casing reached it first, which the aggregator's own contract admits can flap
    /// between refreshes as parallel workers merge in completion order; the scan road keys
    /// ordinally, so <c>Billing</c> and <c>billing</c> are two rows there and one
    /// nondeterministically-labelled row here. It also cannot tell an event with no service
    /// from one whose service is the empty string or literally <c>(unknown)</c> — all three
    /// collapse together. None of that matters to a volume chart, which is what the aggregator
    /// was built for; all of it matters to a table of counts by service. Grouping by
    /// <c>@l</c> has neither problem: the levels are a closed set of canonical spellings.</para>
    ///
    /// <para>A service EQUALITY in the where-clause is fine and is passed through, because the
    /// evaluator compares service names with <c>OrdinalIgnoreCase</c> too, and
    /// <c>IsUsableServiceLiteral</c> has already refused the empty and <c>(unknown)</c>
    /// literals before this is reached.</para>
    ///
    /// <para>ONE REMAINING DIFFERENCE FROM THE SCAN PATH, deliberate and in the direction of a
    /// better answer: this path reads the WHOLE window rather than the newest
    /// <see cref="MaxScanned"/> events, so a window that the scan would have reported as
    /// partial comes back complete — and it genuinely is.</para>
    /// </summary>
    private async Task<AggregationResult?> TryHeaderCountAsync(
        AggregationQuery query, DateTimeOffset? fromUtc, DateTimeOffset? toUtc, CancellationToken ct)
    {
        if (headerScan is null) return null;

        var aggs = query.Aggregates;
        if (aggs.Count == 0) return null;
        for (int i = 0; i < aggs.Count; i++)
            if (aggs[i].Kind != AggregateKind.Count || aggs[i].Property is not null) return null;

        var keys = query.Keys;
        if (keys.Count > 1) return null;

        var grouping = HeaderGrouping.None;
        if (keys.Count == 1)
        {
            if (!BuiltinFields.TryResolve(keys[0].Property, out var field)) return null;
            if (field != BuiltinField.Level) return null;       // including service.name — see above
            grouping = HeaderGrouping.Level;
        }

        HashSet<LogLevel>? levels;
        string?            service;
        try
        {
            if (!CompiledFilter.Compile(query.FilterText).TryGetHeaderOnlyShape(out levels, out service))
                return null;
        }
        catch { return null; }     // a filter that will not compile is the scan path's error to report

        LogVolumeCounts counts;
        try
        {
            // nBuckets = 1 with a one-second axis: every in-window event lands OUTSIDE the
            // single column, which is exactly right here — the aggregator counts totals before
            // it considers the axis, so the totals are exact and the per-bucket arrays (which
            // this path never reads) stay one long each.
            counts = await headerScan.AggregateLogVolumeAsync(
                fromUtc ?? DateTimeOffset.MinValue,
                toUtc   ?? DateTimeOffset.MaxValue,
                minBucket: 0, bucketSeconds: 1, nBuckets: 1,
                serviceFilter: service, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { return null; }   // the scan path reports the timeout

        var rows = new List<AggregationRow>();
        switch (grouping)
        {
            case HeaderGrouping.Level:
                foreach (var l in counts.Levels)
                {
                    if (levels is not null &&
                        (!LogLevelExtensions.TryParse(l.Name, out var parsed) || !levels.Contains(parsed)))
                        continue;
                    rows.Add(Row([l.Name], l.Count, aggs.Count));
                }
                break;

            default:
            {
                // One row, even when the answer is zero — the same guarantee the scan path
                // makes by seeding its single group up front.
                long total = counts.Total;
                if (levels is not null)
                {
                    total = 0;
                    foreach (var l in counts.Levels)
                        if (LogLevelExtensions.TryParse(l.Name, out var parsed) && levels.Contains(parsed))
                            total += l.Count;
                }
                rows.Add(Row([], total, aggs.Count));
                break;
            }
        }

        bool hitGroupCap = rows.Count > AggregationParser.MaxGroups;
        if (hitGroupCap) rows.RemoveRange(AggregationParser.MaxGroups, rows.Count - AggregationParser.MaxGroups);

        return new AggregationResult
        {
            KeyColumns    = keys.Select(k => k.Alias).ToArray(),
            ValueColumns  = aggs.Select(a => a.Alias).ToArray(),
            Rows          = OrderAndLimit(rows, query.Limit),
            Scanned       = counts.Scanned,
            GroupsFound   = rows.Count,
            Partial       = hitGroupCap,
            PartialReason = hitGroupCap
                ? $"more than {AggregationParser.MaxGroups:N0} distinct groups — group by something coarser"
                : null,
        };

        static AggregationRow Row(string?[] key, long count, int columns)
        {
            var values = new double?[columns];
            for (int i = 0; i < columns; i++) values[i] = count;   // every column is count(*)
            return new AggregationRow { Key = key, Values = values };
        }
    }

    /// <summary>
    /// The group's identity, built ONCE per event into reusable storage.
    ///
    /// <para>The composite used to be a <c>string.Join</c> over a LINQ <c>Select</c> over a
    /// fresh <c>string?[]</c>, with a <c>char.ToString()</c> for every absent part — three to
    /// four heap objects per event, for a lookup that almost always finds a group that already
    /// exists. Here the parts land in a buffer owned by the aggregation, the composite is
    /// written into a second one, and the dictionary is probed through its span alternate
    /// lookup; nothing is allocated until a group is genuinely NEW, and then exactly once.</para>
    ///
    /// <para>The encoding is unchanged and load-bearing: the control characters the path
    /// encoding already relies on being absent from msgpack keys and values — U+0001 between
    /// parts, so <c>['a','b']</c> and <c>['ab']</c> stay different groups, and U+0002 for a
    /// value the event did not carry, so "absent" does not merge with the group whose value is
    /// genuinely the empty string.</para>
    /// </summary>
    private sealed class GroupKeyBuilder
    {
        private readonly IReadOnlyList<GroupKeySpec> _keys;
        private readonly string?[] _parts;
        private char[] _buffer = new char[256];
        private int    _length;

        public GroupKeyBuilder(IReadOnlyList<GroupKeySpec> keys)
        {
            _keys  = keys;
            _parts = keys.Count == 0 ? [] : new string?[keys.Count];
        }

        /// <summary>The composite key of the event last passed to <see cref="Build"/>.</summary>
        public ReadOnlySpan<char> Composite => _buffer.AsSpan(0, _length);

        public void Build(LogEvent ev)
        {
            _length = 0;
            if (_keys.Count == 0) return;

            for (int i = 0; i < _keys.Count; i++)
                _parts[i] = Stringify(FilterEvaluator.ReadProperty(ev, _keys[i].Property));

            int needed = _keys.Count - 1;                       // the separators
            for (int i = 0; i < _parts.Length; i++)
                needed += _parts[i]?.Length ?? 1;                // absent renders as one marker char
            if (needed > _buffer.Length)
                _buffer = new char[Math.Max(needed, _buffer.Length * 2)];

            var dest = _buffer.AsSpan();
            int at = 0;
            for (int i = 0; i < _parts.Length; i++)
            {
                if (i > 0) dest[at++] = PropertyPath.Separator;
                if (_parts[i] is { } p) { p.CopyTo(dest[at..]); at += p.Length; }
                else                      dest[at++] = PropertyPath.IndexMarker;
            }
            _length = at;
        }

        /// <summary>
        /// The parts of the current key, copied out for a group that is being created. The
        /// working array is reused for the next event, so the accumulator cannot hold it.
        /// </summary>
        public string?[] TakeParts() => _keys.Count == 0 ? [] : (string?[])_parts.Clone();
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
