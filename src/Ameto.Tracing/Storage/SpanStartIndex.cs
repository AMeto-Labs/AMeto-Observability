namespace Ameto.Tracing.Storage;

/// <summary>
/// WHERE IN TIME EACH STRETCH OF A HOT-TIER LIST LIES (#94): the smallest and the largest start of
/// every block of <see cref="BlockSize"/> consecutive spans, in the list's own ARRIVAL order — a
/// zone map over the unflushed spans. A page whose window misses a block's range skips the block
/// without touching one of its records; a newest-first search visits blocks by their largest start
/// and stops once no block left can hold anything newer than what it already kept.
///
/// <para><b>OUT-OF-ORDER IS THE NORMAL CASE, AND THE BOUNDS ARE WHY IT IS HARMLESS.</b> Spans arrive
/// roughly in start order, never exactly: a long span is exported when it ENDS, an exporter batches,
/// a clock is skewed. A late span lowers its block's minimum and a span from a clock running ahead
/// raises its block's maximum; either way the block's range still contains every start in it, so a
/// reader that trusts the range can only visit a block it did not need — never skip one it did. The
/// cost of disorder is pruning, not correctness.</para>
///
/// <para><b>APPEND IS O(1) AND ALLOCATES NOTHING</b> — two compares and at most two stores, under
/// the engine write lock the span is inserted under. The arrays are sized for a whole tier up front
/// (<see cref="InitialBlocks"/> blocks, 65 536 spans: the 50 000-span flush threshold and the
/// overshoot a flush in progress lets the next tier build) and double past that, which is a tier a
/// flush could not keep up with.</para>
///
/// <para><b>ONE INDEX PER LIST, AND THE LIST'S LIFE IS ITS LIFE.</b> The engine builds a new index
/// with every new tier list, BEFORE the log opens a flush window (nothing after that may allocate):
/// the detached snapshot takes its index with it and the new tier starts empty; a failed flush's
/// rebuilt list gets an index rebuilt for it. <see cref="Owner"/> and <see cref="Count"/> let a
/// reader prove the pairing at capture, and a reader that cannot reads every span instead.</para>
///
/// <para><b>READ WITHOUT THE LOCK, BY THE SAME ARGUMENT AS THE RUNS.</b> A reader captures
/// <see cref="View"/> under the read lock together with the run it describes and uses it after the
/// lock is gone. Every block inside the captured prefix was opened by an append the lock ordered
/// before the capture; what appends do to it afterwards can only WIDEN its range (a later span of
/// the same block), and a widened range is still a range of the captured spans. A growth swaps in
/// new arrays and leaves the captured ones as they were at the copy — which, again, contained every
/// captured span. The elements are read and written as single 64-bit volatile accesses, so no reader
/// ever sees half a bound.</para>
/// </summary>
internal sealed class SpanStartIndex
{
    internal const int BlockShift = 7;
    internal const int BlockSize  = 1 << BlockShift;   // 128 spans: 16 B of index per 128, ~0,13 B a span
    private  const int BlockMask  = BlockSize - 1;

    /// <summary>65 536 spans' worth of blocks — 8 KB of index — before the first growth.</summary>
    internal const int InitialBlocks = 512;

    private long[] _min;
    private long[] _max;
    private int    _count;

    /// <param name="owner">The list this index describes, span for span, in the list's order.</param>
    /// <param name="spans">How many spans to size for; the arrays grow past it.</param>
    internal SpanStartIndex(List<SpanRecord> owner, int spans = InitialBlocks * BlockSize)
    {
        Owner = owner;
        int blocks = Math.Max(InitialBlocks, (spans + BlockMask) >> BlockShift);
        _min = new long[blocks];
        _max = new long[blocks];
    }

    /// <summary>The list whose spans this index bounds. A reader that does not find this list beside it reads unindexed.</summary>
    internal List<SpanRecord> Owner { get; }

    /// <summary>Spans appended. Equal to <see cref="Owner"/>'s count whenever the engine lock is free.</summary>
    internal int Count => _count;

    /// <summary>
    /// The next span of <see cref="Owner"/>, by its start. Called under the engine write lock BEFORE
    /// the span joins the list: a growth that throws then leaves both as they were, and a list add
    /// that throws after it leaves this index one ahead — which <see cref="Count"/> makes every
    /// reader see, and read the list unindexed, until the tier is replaced.
    /// </summary>
    internal void Append(long startNano)
    {
        int i = _count;
        int b = i >> BlockShift;
        if (b == _min.Length) Grow();

        if ((i & BlockMask) == 0)
        {
            Volatile.Write(ref _min[b], startNano);
            Volatile.Write(ref _max[b], startNano);
        }
        else
        {
            if (startNano < _min[b]) Volatile.Write(ref _min[b], startNano);
            if (startNano > _max[b]) Volatile.Write(ref _max[b], startNano);
        }
        _count = i + 1;
    }

    /// <summary>An index over a list that was built some other way (a failed flush's restored tier). O(n), once.</summary>
    internal static SpanStartIndex Build(List<SpanRecord> spans)
    {
        var index = new SpanStartIndex(spans, spans.Count);
        foreach (var s in spans) index.Append(s.StartTimeUnixNano);
        return index;
    }

    /// <summary>The bounds of the first <paramref name="spans"/> spans — taken under the engine read lock with the run they describe.</summary>
    internal SpanStartView View(int spans) => new(_min, _max, spans);

    private void Grow()
    {
        var min = new long[_min.Length * 2];
        var max = new long[_max.Length * 2];
        Array.Copy(_min, min, _min.Length);
        Array.Copy(_max, max, _max.Length);
        _min = min;   // the old arrays stay valid for any reader that captured them
        _max = max;
    }
}

/// <summary>
/// A captured <see cref="SpanStartIndex"/>: the bounds of each block of one run of
/// <see cref="Spans"/> spans. <c>default</c> is an UNINDEXED run, which a reader walks whole.
/// </summary>
internal readonly struct SpanStartView
{
    private readonly long[]? _min;
    private readonly long[]? _max;

    internal SpanStartView(long[] min, long[] max, int spans)
    {
        _min  = min;
        _max  = max;
        Spans = spans;
    }

    /// <summary>The run's length when it was captured.</summary>
    internal readonly int Spans;

    internal bool IsIndexed => _min is not null;

    /// <summary>Blocks covering the run; the last may be partial.</summary>
    internal int Blocks => (Spans + SpanStartIndex.BlockSize - 1) >> SpanStartIndex.BlockShift;

    /// <summary>A lower bound on every start in <paramref name="block"/>.</summary>
    internal long MinOf(int block) => Volatile.Read(ref _min![block]);

    /// <summary>An upper bound on every start in <paramref name="block"/>.</summary>
    internal long MaxOf(int block) => Volatile.Read(ref _max![block]);

    /// <summary>True when <paramref name="block"/> may hold a start in <c>[fromNano, toNano]</c>.</summary>
    internal bool Overlaps(int block, long fromNano, long toNano) =>
        MaxOf(block) >= fromNano && MinOf(block) <= toNano;
}
