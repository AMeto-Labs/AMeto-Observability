using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE HOT TIER'S START INDEX BOUNDS WHAT IT DESCRIBES (#94) — the one property every reader of it
/// stands on: for every block of a captured run, no start in the block lies outside the block's
/// range. A range that is too WIDE costs a reader a block it did not need; a range that is too
/// NARROW loses rows, and that is the failure these facts exist to catch — for spans in any order,
/// for a view captured while appends continue, and across the arrays' growth.
/// </summary>
public sealed class SpanStartIndexTests
{
    private static List<SpanRecord> Spans(IEnumerable<long> starts) =>
        [.. starts.Select(static (s, i) => new SpanRecord { SpanId = new SpanId((ulong)i + 1), StartTimeUnixNano = s })];

    /// <summary>The exact bounds of each block of the first <paramref name="count"/> starts.</summary>
    private static (long Min, long Max)[] TrueBounds(IReadOnlyList<long> starts, int count)
    {
        int blocks = (count + SpanStartIndex.BlockSize - 1) / SpanStartIndex.BlockSize;
        var bounds = new (long, long)[blocks];
        for (int b = 0; b < blocks; b++)
        {
            long min = long.MaxValue, max = long.MinValue;
            for (int i = b * SpanStartIndex.BlockSize; i < Math.Min(count, (b + 1) * SpanStartIndex.BlockSize); i++)
            {
                min = Math.Min(min, starts[i]);
                max = Math.Max(max, starts[i]);
            }
            bounds[b] = (min, max);
        }
        return bounds;
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

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 127)]
    [InlineData(3, 128)]
    [InlineData(4, 129)]
    [InlineData(5, 10_000)]
    public void Each_block_is_bounded_exactly_by_the_starts_in_it(int seed, int count)
    {
        var starts = Arrivals(seed, count);
        var index  = new SpanStartIndex(Spans(starts));
        foreach (long s in starts) index.Append(s);

        var view = index.View(count);
        Assert.True(view.IsIndexed);
        Assert.Equal(count, index.Count);

        var truth = TrueBounds(starts, count);
        Assert.Equal(truth.Length, view.Blocks);
        for (int b = 0; b < truth.Length; b++)
        {
            Assert.Equal(truth[b].Min, view.MinOf(b));
            Assert.Equal(truth[b].Max, view.MaxOf(b));
        }
    }

    /// <summary>
    /// A VIEW OUTLIVES THE APPENDS AFTER IT. A reader captures the view under the read lock and
    /// reads it after the lock is gone, while the drainer keeps appending — into the partial block
    /// the view ends in, into new blocks, and past the arrays' first size, which swaps them. Every
    /// block the view covers must still bound the spans the view covers.
    /// </summary>
    [Fact]
    public void A_view_still_bounds_what_it_captured_after_appends_and_a_growth()
    {
        int initial = SpanStartIndex.InitialBlocks * SpanStartIndex.BlockSize;
        var starts  = Arrivals(7, initial + 3 * SpanStartIndex.BlockSize);
        var index   = new SpanStartIndex(Spans(starts));

        int captured = initial - 50;                      // ends inside a block
        for (int i = 0; i < captured; i++) index.Append(starts[i]);
        var view = index.View(captured);

        // The rest: into the same block first, then over the arrays' capacity.
        for (int i = captured; i < starts.Count; i++) index.Append(starts[i]);

        var truth = TrueBounds(starts, captured);
        for (int b = 0; b < truth.Length; b++)
        {
            Assert.True(view.MinOf(b) <= truth[b].Min, $"block {b}: a minimum above a captured start");
            Assert.True(view.MaxOf(b) >= truth[b].Max, $"block {b}: a maximum below a captured start");
        }

        // And the index itself, past the growth, is exact.
        var all = index.View(starts.Count);
        var allTruth = TrueBounds(starts, starts.Count);
        for (int b = 0; b < allTruth.Length; b++)
        {
            Assert.Equal(allTruth[b].Min, all.MinOf(b));
            Assert.Equal(allTruth[b].Max, all.MaxOf(b));
        }
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
    }

    [Fact]
    public void An_unindexed_view_is_the_default()
    {
        SpanStartView none = default;
        Assert.False(none.IsIndexed);
        Assert.Equal(0, none.Spans);
    }
}
