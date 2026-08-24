using System.Runtime.CompilerServices;
using Ameto.Core;
using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

/// <summary>
/// The properties an aggregation must never get wrong quietly. A truncated LIST of events is
/// visibly short; a truncated TOTAL looks exactly like a complete one, so the flag that says
/// which is the load-bearing part of the answer. Driven through a stub executor because the
/// branches that matter are the ones a normal corpus never reaches.
/// </summary>
public sealed class AggregationHonestyTests
{
    /// <summary>
    /// Reproduces <c>QueryExecutor</c>'s emit contract exactly: it stops at
    /// <c>count &gt;= limit</c>, so it yields at most <c>request.Count</c> events and never one
    /// beyond. Everything about the scan cap turns on that "never one beyond".
    /// </summary>
    private sealed class StubExecutor(IEnumerable<LogEvent> events) : IQueryExecutor
    {
        public async IAsyncEnumerable<LogEvent> ExecuteAsync(
            QueryRequest request,
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            int count = 0;
            foreach (var ev in events)
            {
                if (ct.IsCancellationRequested || count >= request.Count) yield break;
                yield return ev;
                count++;
                await Task.Yield();
            }
        }
    }

    private static LogEvent Event(uint seq, params (string Key, object? Value)[] props)
    {
        var map = new Dictionary<string, object?>(props.Length);
        foreach (var (k, v) in props) map[k] = v;
        return new LogEvent
        {
            Id              = new EventId(0u, seq),
            Timestamp       = new DateTimeOffset(2026, 8, 24, 12, 0, 0, TimeSpan.Zero).AddSeconds(seq),
            Level           = LogLevel.Information,
            MessageTemplate = "evt",
            Properties      = map,
        };
    }

    private static AggregationQuery Q(string text) => AggregationParser.Parse(text);

    private static Task<AggregationResult> RunAsync(
        string query, IEnumerable<LogEvent> events, int budget = 1000, CancellationToken ct = default)
        => new AggregationExecutor(new StubExecutor(events), budget).ExecuteAsync(Q(query), null, null, ct);

    // ── The scan cap must be able to fire ─────────────────────────────────────

    [Fact]
    public async Task Reading_the_whole_budget_and_finding_more_is_reported_as_partial()
    {
        // The two caps have to be paired: the executor stops AT its limit, so asking it for
        // exactly the budget makes an in-loop test for exceeding the budget unreachable. The
        // scan then ends normally, nothing sets the flag, and the newest N events are returned
        // as though they were the window.
        var events = Enumerable.Range(0, 250).Select(i => Event((uint)i));

        var result = await RunAsync("select count(*)", events, budget: 100);

        Assert.True(result.Partial, "a truncated aggregation must say so");
        Assert.Contains("more than", result.PartialReason);
        Assert.Equal(100, result.Scanned);
        Assert.Equal(100d, Assert.Single(result.Rows).Values[0]);
    }

    [Fact]
    public async Task A_window_holding_exactly_the_budget_is_complete()
    {
        // The other side of the same boundary: reaching the budget is not evidence of more.
        var result = await RunAsync("select count(*)", Enumerable.Range(0, 100).Select(i => Event((uint)i)), budget: 100);

        Assert.False(result.Partial);
        Assert.Equal(100d, Assert.Single(result.Rows).Values[0]);
    }

    [Fact]
    public async Task A_cancelled_scan_is_reported_as_partial()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var result = await RunAsync("select count(*)", Enumerable.Range(0, 50).Select(i => Event((uint)i)), ct: cts.Token);

        Assert.True(result.Partial);
        Assert.Contains("out of time", result.PartialReason);
    }

    [Fact]
    public async Task Running_out_of_groups_is_reported_as_partial()
    {
        // Every event its own group. The cap is the real one here, so this needs more events
        // than MaxGroups — cheap, since they are never stored.
        int n = AggregationParser.MaxGroups + 50;
        var events = Enumerable.Range(0, n).Select(i => Event((uint)i, ("Shard", (long)i)));

        var result = await RunAsync("select count(*) group by Shard", events, budget: n + 10);

        Assert.True(result.Partial);
        Assert.Contains("distinct groups", result.PartialReason);
        Assert.Equal(AggregationParser.MaxGroups, result.GroupsFound);
    }

    // ── Numbers JSON cannot express ───────────────────────────────────────────

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(double.NegativeInfinity)]
    public async Task A_value_that_is_not_a_finite_number_contributes_nothing(double poison)
    {
        // NaN compares false against everything, so it slipped past `d < min` while still
        // bumping the count — and the snapshot, which gates on the count, handed back the
        // double.MaxValue/MinValue SEEDS as if the data contained them, with min above max.
        // Infinity is worse: Utf8JsonWriter refuses to write it, from outside the endpoint's
        // try/catch, so the client got an unlogged 500 with a half-written body.
        var events = new[] { Event(1, ("V", poison)), Event(2, ("V", poison)) };

        var result = await RunAsync("select min(V), max(V), sum(V), avg(V), count(V)", events);
        var values = Assert.Single(result.Rows).Values;

        Assert.All(values[..4], v => Assert.Null(v));
        // count(V) asks how many events CARRY V, which these do — it counts strings too. Two
        // events with the property and no usable number out of it is the honest report.
        Assert.Equal(2d, values[4]);
    }

    [Theory]
    [InlineData("NaN")]
    [InlineData("Infinity")]
    [InlineData("-Infinity")]
    public async Task The_same_words_written_as_text_are_not_numbers_either(string poison)
    {
        // double.TryParse accepts all three, and JSON has no literal for any of them — which is
        // the likelier route in, since a CLEF producer writes what its serialiser will emit.
        var result = await RunAsync("select min(V), sum(V)", new[] { Event(1, ("V", poison)) });

        Assert.All(Assert.Single(result.Rows).Values, v => Assert.Null(v));
    }

    [Fact]
    public async Task A_finite_number_written_as_text_still_counts()
    {
        var result = await RunAsync("select sum(V), min(V)", new[] { Event(1, ("V", "2.5")), Event(2, ("V", "1.5")) });
        var values = Assert.Single(result.Rows).Values;

        Assert.Equal(4.0d, values[0]!.Value, 6);
        Assert.Equal(1.5d, values[1]!.Value, 6);
    }

    // ── Group keys ────────────────────────────────────────────────────────────

    [Fact]
    public async Task Two_different_container_values_do_not_become_one_group_named_after_a_type()
    {
        // object[] and Dictionary do not override ToString(), so every array stringified to
        // "System.Object[]": values that are not remotely equal merged into one group, and a
        // .NET type name appeared in the public response.
        var events = new[]
        {
            Event(1, ("Tags", new object?[] { "eu-west" })),
            Event(2, ("Tags", new object?[] { "us-east" })),
            Event(3, ("Tags", new Dictionary<string, object?> { ["a"] = 1 })),
            Event(4, ("Tags", "plain")),
        };

        var result = await RunAsync("select count(*) group by Tags", events);

        Assert.DoesNotContain(result.Rows, r => r.Key[0]?.Contains("System.") == true);
        // A container cannot be a key, so those events join the documented "absent" group.
        var absent = Assert.Single(result.Rows.Where(r => r.Key[0] is null));
        Assert.Equal(3d, absent.Values[0]);
        Assert.Contains(result.Rows, r => r.Key[0] == "plain");
    }

    [Fact]
    public async Task Composite_keys_do_not_collide_across_their_boundary()
    {
        var events = new[]
        {
            Event(1, ("A", "a"),  ("B", "b")),
            Event(2, ("A", "ab"), ("B", "")),
        };

        var result = await RunAsync("select count(*) group by A, B", events);
        Assert.Equal(2, result.Rows.Count);
    }

    [Fact]
    public async Task An_absent_key_is_not_the_same_group_as_an_empty_one()
    {
        var events = new[] { Event(1, ("K", "")), Event(2), Event(3) };

        var result = await RunAsync("select count(*) group by K", events);

        Assert.Equal(2, result.Rows.Count);
        Assert.Equal(1d, Assert.Single(result.Rows.Where(r => r.Key[0] == "")).Values[0]);
        Assert.Equal(2d, Assert.Single(result.Rows.Where(r => r.Key[0] is null)).Values[0]);
    }
}
