using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE HOT TIER'S START INDEX ACCOUNTS FOR WHAT IT DESCRIBES (#94, #127) — the one property every
/// reader of it stands on: every span of a captured run either lies inside its block's range or is
/// one of the run's OUTLIERS, listed with its position and its exact start. A range that is too WIDE
/// costs a reader a block it did not need; a span that is neither inside its block's range nor listed
/// is a span no reader will ever return, and that is the failure these facts exist to catch — for
/// spans in any order, for a view captured while appends continue, and across every growth.
/// </summary>
public sealed class SpanStartIndexTests
{
    private const long Second = 1_000_000_000L;

    private static List<SpanRecord> Spans(IEnumerable<long> starts) =>
        [.. starts.Select(static (s, i) => new SpanRecord { SpanId = new SpanId((ulong)i + 1), StartTimeUnixNano = s })];

    private static SpanStartIndex Indexed(IReadOnlyList<long> starts)
    {
        var index = new SpanStartIndex(Spans(starts));
        foreach (long s in starts) index.Append(s);
        return index;
    }

    /// <summary>
    /// Every one of the first <paramref name="count"/> starts is inside its block's range or one of
    /// the view's outliers, which ascend, lie inside the view and carry their span's own start.
    /// Returns the outlier positions.
    /// </summary>
    private static HashSet<int> AssertAccountedFor(SpanStartView view, IReadOnlyList<long> starts, int count)
    {
        var outliers = new HashSet<int>();
        int previous = -1;
        for (int k = 0; k < view.Outliers; k++)
        {
            int p = view.OutlierPosition(k);
            Assert.True(p > previous, $"outlier {k} at {p} does not come after {previous}");
            Assert.True(p < count, $"outlier {k} at {p} lies past the {count} spans the view covers");
            Assert.Equal(starts[p], view.OutlierStart(k));
            outliers.Add(p);
            previous = p;
        }
        for (int i = 0; i < count; i++)
        {
            if (outliers.Contains(i)) continue;
            int b = i >> SpanStartIndex.BlockShift;
            Assert.True(view.MinOf(b) <= starts[i] && starts[i] <= view.MaxOf(b),
                $"span {i} (block {b}) starts at {starts[i]}, outside [{view.MinOf(b)}, {view.MaxOf(b)}], and is not an outlier");
        }
        return outliers;
    }

    /// <summary>A finished index's block ranges are exactly the ranges of their regular spans: no wider than they must be.</summary>
    private static void AssertTight(SpanStartView view, IReadOnlyList<long> starts, HashSet<int> outliers)
    {
        for (int b = 0; b < view.Blocks; b++)
        {
            long min = long.MaxValue, max = long.MinValue;
            for (int i = b * SpanStartIndex.BlockSize; i < Math.Min(view.Spans, (b + 1) * SpanStartIndex.BlockSize); i++)
            {
                if (outliers.Contains(i)) continue;
                min = Math.Min(min, starts[i]);
                max = Math.Max(max, starts[i]);
            }
            Assert.Equal(min, view.MinOf(b));
            Assert.Equal(max, view.MaxOf(b));
        }
    }

    /// <summary>Starts mostly in order, with late, early and negative ones — the arrival order a tier sees.</summary>
    private static List<long> Arrivals(int seed, int count)
    {
        var rnd    = new Random(seed);
        var starts = new List<long>(count);
        long clock = 1_785_000_000_000_000_000L;
        for (int i = 0; i < count; i++)
        {
            clock += rnd.Next(0, 2_000_000);
            starts.Add(rnd.Next(30) switch
            {
                0 => clock - rnd.NextInt64(1, 600_000_000_000L),   // late: a long span reported when it ends
                1 => clock + rnd.NextInt64(1, 60_000_000_000L),    // a clock running ahead
                2 => -rnd.NextInt64(1, 1_000_000),                 // a producer that sent garbage
                _ => clock,
            });
        }
        return starts;
    }

    /// <summary>Starts one millisecond apart from <paramref name="first"/>.</summary>
    private static List<long> InOrder(long first, int count) =>
        [.. Enumerable.Range(0, count).Select(i => first + i * 1_000_000L)];

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 127)]
    [InlineData(3, 128)]
    [InlineData(4, 129)]
    [InlineData(5, 10_000)]
    public void Every_span_is_inside_its_blocks_range_or_listed_and_no_range_is_wider_than_its_spans(int seed, int count)
    {
        var starts = Arrivals(seed, count);
        var index  = Indexed(starts);

        var view = index.View(count);
        Assert.True(view.IsIndexed);
        Assert.Equal(count, index.Count);
        Assert.Equal((count + SpanStartIndex.BlockSize - 1) / SpanStartIndex.BlockSize, view.Blocks);

        AssertTight(view, starts, AssertAccountedFor(view, starts, count));
    }

    /// <summary>
    /// A VIEW OUTLIVES THE APPENDS AFTER IT. A reader captures the view under the read lock and
    /// reads it after the lock is gone, while the drainer keeps appending — into the partial block
    /// the view ends in, into new blocks, past the bound arrays' first size and the outlier list's,
    /// which swap them. Every span the view covers must still be inside its block's range or among
    /// the view's outliers, and the view's outliers may not take in one that came after it.
    /// </summary>
    [Fact]
    public void A_view_still_accounts_for_what_it_captured_after_appends_and_a_growth()
    {
        int initial = SpanStartIndex.InitialBlocks * SpanStartIndex.BlockSize;
        var starts  = Arrivals(7, initial + 3 * SpanStartIndex.BlockSize);
        var index   = new SpanStartIndex(Spans(starts));

        int captured = initial - 50;                      // ends inside a block
        for (int i = 0; i < captured; i++) index.Append(starts[i]);
        var view = index.View(captured);
        int listed = view.Outliers;

        // The rest: into the same block first, then over the arrays' capacity.
        for (int i = captured; i < starts.Count; i++) index.Append(starts[i]);

        Assert.Equal(listed, view.Outliers);
        AssertAccountedFor(view, starts, captured);

        // And the index itself, past the growth, is exact.
        var all = index.View(starts.Count);
        AssertTight(all, starts, AssertAccountedFor(all, starts, starts.Count));
    }

    [Fact]
    public void A_view_of_a_shorter_prefix_lists_only_the_outliers_inside_it()
    {
        var starts = Arrivals(13, 5_000);
        var index  = Indexed(starts);
        foreach (int prefix in (int[])[0, 1, 127, 128, 2_500, 4_999])
            AssertAccountedFor(index.View(prefix), starts, prefix);
    }

    [Fact]
    public void Built_over_a_list_is_the_index_its_appends_would_have_made()
    {
        var starts  = Arrivals(11, 5_000);
        var spans   = Spans(starts);
        var built   = SpanStartIndex.Build(spans);
        var grown   = new SpanStartIndex(spans);
        foreach (long s in starts) grown.Append(s);

        Assert.Same(spans, built.Owner);
        Assert.Equal(spans.Count, built.Count);
        var a = built.View(spans.Count);
        var b = grown.View(spans.Count);
        Assert.Equal(b.Blocks, a.Blocks);
        for (int k = 0; k < a.Blocks; k++)
        {
            Assert.Equal(b.MinOf(k), a.MinOf(k));
            Assert.Equal(b.MaxOf(k), a.MaxOf(k));
        }
        Assert.Equal(b.Outliers, a.Outliers);
        for (int k = 0; k < a.Outliers; k++)
        {
            Assert.Equal(b.OutlierPosition(k), a.OutlierPosition(k));
            Assert.Equal(b.OutlierStart(k), a.OutlierStart(k));
        }
    }

    [Fact]
    public void An_unindexed_view_is_the_default()
    {
        SpanStartView none = default;
        Assert.False(none.IsIndexed);
        Assert.Equal(0, none.Spans);
        Assert.Equal(0, none.Outliers);
    }

    /// <summary>
    /// ONE SPAN FROM A CLOCK RUNNING AHEAD AND ONE REPORTED LATE LEAVE THEIR BLOCK AS THE REST MADE
    /// IT (#127). Without the list, the first lifts its block's maximum 30 s above every other start
    /// in it and the second drops its minimum 10 s below.
    /// </summary>
    [Fact]
    public void A_span_far_outside_its_blocks_range_is_listed_instead_of_widening_it()
    {
        long first  = 1_785_000_000_000_000_000L;
        var  starts = InOrder(first, 3 * SpanStartIndex.BlockSize);
        starts[200] += 30 * Second;
        starts[300] -= 10 * Second;
        var index = Indexed(starts);
        var view  = index.View(starts.Count);

        Assert.Equal(2, view.Outliers);
        Assert.Equal(200, view.OutlierPosition(0));
        Assert.Equal(300, view.OutlierPosition(1));
        Assert.Equal(first + 128 * 1_000_000L, view.MinOf(1));
        Assert.Equal(first + 255 * 1_000_000L, view.MaxOf(1));
        Assert.Equal(first + 256 * 1_000_000L, view.MinOf(2));
        AssertTight(view, starts, AssertAccountedFor(view, starts, starts.Count));
    }

    /// <summary>
    /// A BLOCK WHOSE FIRST SPAN IS THE SKEWED ONE. Judged against its own range alone, that span
    /// would be the range, and the 127 regular spans after it would all be outliers; it is judged
    /// against the range of the block before it instead.
    /// </summary>
    [Fact]
    public void A_skewed_first_span_does_not_make_the_rest_of_its_block_outliers()
    {
        long first  = 1_785_000_000_000_000_000L;
        var  starts = InOrder(first, 3 * SpanStartIndex.BlockSize);
        starts[128] += 30 * Second;
        var view = Indexed(starts).View(starts.Count);

        Assert.Equal(1, view.Outliers);
        Assert.Equal(128, view.OutlierPosition(0));
        Assert.Equal(first + 129 * 1_000_000L, view.MinOf(1));
        Assert.Equal(first + 255 * 1_000_000L, view.MaxOf(1));
    }

    /// <summary>
    /// A GAP IN THE TRAFFIC IS A NEW LEVEL, NOT A FLOOD OF OUTLIERS. Spans resume a minute later and
    /// keep coming: the first few are listed until <see cref="SpanStartIndex.ShiftAfter"/> of them
    /// agree, then the level moves — one block spans the gap, the blocks after it are tight again.
    /// </summary>
    [Fact]
    public void A_gap_in_the_traffic_moves_the_level_after_a_few_outliers()
    {
        long first  = 1_785_000_000_000_000_000L;
        var  starts = InOrder(first, 300);
        starts.AddRange(InOrder(first + 60 * Second, 2_000));
        var view = Indexed(starts).View(starts.Count);

        Assert.Equal(SpanStartIndex.ShiftAfter - 1, view.Outliers);
        int last = (starts.Count - 1) >> SpanStartIndex.BlockShift;
        for (int b = 3; b <= last; b++)
            Assert.True(view.MaxOf(b) - view.MinOf(b) < Second, $"block {b} after the gap still spans it");
        AssertTight(view, starts, AssertAccountedFor(view, starts, starts.Count));
    }

    /// <summary>
    /// PAST THE CAP, BLOCKS WIDEN AGAIN. With every tenth span 30 s ahead, the list fills to its cap
    /// and stops; every skewed span after it widens its block, as before #127 — and is still inside a
    /// range a reader trusts.
    /// </summary>
    [Fact]
    public void Past_its_cap_the_index_widens_blocks_as_before()
    {
        var saved = SpanStartIndex.ShapeForTest;
        SpanStartIndex.ShapeForTest = (SpanStartIndex.ToleranceNanos, 8, SpanStartIndex.ShiftAfter);
        try
        {
            long first  = 1_785_000_000_000_000_000L;
            var  starts = InOrder(first, 5 * SpanStartIndex.BlockSize);
            for (int i = 5; i < starts.Count; i += 10) starts[i] += 30 * Second;
            var view = Indexed(starts).View(starts.Count);

            Assert.Equal(8, view.Outliers);
            Assert.Equal(75, view.OutlierPosition(7));                     // the eighth skewed span
            Assert.True(view.MaxOf(4) - view.MinOf(4) > 29 * Second, "a skewed span past the cap did not widen its block");
            AssertTight(view, starts, AssertAccountedFor(view, starts, starts.Count));
        }
        finally { SpanStartIndex.ShapeForTest = saved; }
    }

    /// <summary>
    /// STARTS AT THE ENDS OF THE 64-BIT RANGE — a producer that sent garbage — neither overflow
    /// into "near" nor leave a span unaccounted for.
    /// </summary>
    [Fact]
    public void Starts_at_the_ends_of_the_range_are_accounted_for()
    {
        long first  = 1_785_000_000_000_000_000L;
        var  starts = InOrder(first, 2 * SpanStartIndex.BlockSize);
        starts[10]  = long.MinValue;
        starts[20]  = long.MaxValue;
        starts[128] = long.MinValue;    // and as a block's first span
        starts[129] = long.MaxValue;
        starts[130] = 0;
        var view = Indexed(starts).View(starts.Count);
        AssertTight(view, starts, AssertAccountedFor(view, starts, starts.Count));
    }
}
