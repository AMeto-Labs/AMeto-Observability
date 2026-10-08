namespace Ameto.Tracing.Storage;

/// <summary>
/// WHERE IN TIME EACH STRETCH OF A HOT-TIER LIST LIES (#94): the smallest and the largest start of
/// every block of <see cref="BlockSize"/> consecutive spans, in the list's own ARRIVAL order — a
/// zone map over the unflushed spans. A page whose window misses a block's range skips the block
/// without touching one of its records; a newest-first search visits blocks by their largest start
/// and stops once no block left can hold anything newer than what it already kept.
///
/// <para><b>OUT-OF-ORDER IS THE NORMAL CASE, AND OUTLIERS ARE KEPT OUT OF THE BOUNDS</b> (#127).
/// Spans arrive roughly in start order, never exactly: a long span is exported when it ENDS, an
/// exporter batches, a clock is skewed. Widening a block's range to every such span is always
/// correct, and it is what this index did first — but one span from a clock running ahead lifts
/// its whole block above everything a page keeps, one long span reported late keeps its block in
/// every page reaching back over its duration, and at one span in a hundred nearly every block holds
/// one: a TraceQL page read the whole tier again (#122 review L1). So a span whose start lies more
/// than <see cref="ToleranceNanos"/> outside its block's range does not widen it: its position and
/// start go to an append-only OUTLIER list, and a reader treats it as a block of one span with exact
/// bounds. Every span of a block is therefore either inside the block's range or in the list —
/// the one property readers stand on.</para>
///
/// <para><b>WHAT A BLOCK IS JUDGED AGAINST.</b> Its own range, once it has one; before that — its
/// first span, or every span so far an outlier — the range of the block that last took a regular
/// span, so a block whose first span is the skewed one does not turn its other 127 into outliers.
/// Two things can move the traffic itself, and must not fill the list: a gap (traffic resumes a
/// minute later) and a producer whose batch lands at another level. Neither is a lone span:
/// <see cref="ShiftAfter"/> outliers in a row, each within the tolerance of the one before, are a
/// new level, and the last of them widens its block to it — one wide block per shift, as before
/// this list existed. Past <see cref="MaxOutliers"/> the index widens every block as before.</para>
///
/// <para><b>APPEND IS O(1) AND ALLOCATES NOTHING PER SPAN</b> — a handful of compares and stores,
/// under the engine write lock the span is inserted under. The bound arrays are sized for a whole
/// tier up front (<see cref="InitialBlocks"/> blocks, 65 536 spans: the 50 000-span flush threshold
/// and the overshoot a flush in progress lets the next tier build) and double past that, which is a
/// tier a flush could not keep up with; the outlier list is allocated with its first outlier and
/// doubles up to its cap. Every allocation comes before the first store, so one that throws leaves
/// the index as it was.</para>
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
/// the same block), and a widened range still holds every captured span the range held. The
/// outlier list is append-only and the view carries its length, so the outliers it covers are
/// exactly the captured spans' — later ones lie past the captured prefix. A growth swaps in new
/// arrays and leaves the captured ones as they were. The bounds are read and written as single
/// 64-bit volatile accesses, so no reader ever sees half a bound.</para>
/// </summary>
internal sealed class SpanStartIndex
{
    internal const int BlockShift = 7;
    internal const int BlockSize  = 1 << BlockShift;   // 128 spans: 16 B of index per 128, ~0,13 B a span
    private  const int BlockMask  = BlockSize - 1;

    /// <summary>65 536 spans' worth of blocks — 8 KB of index — before the first growth.</summary>
    internal const int InitialBlocks = 512;

    /// <summary>
    /// How far outside its block's range a start may lie and still widen it — ONE SECOND, the knee of
    /// <c>SpanStartIndexToleranceProbe</c> (49 000 spans, a thousand a second). A skew below it widens
    /// its block by at most itself, which a page pays for in spans read: one span in a hundred a second
    /// ahead costs a TraceQL page 3 048 reads at two seconds against 2 034 at one. A tolerance below the
    /// jitter of ordinary traffic lists that traffic instead: twenty exporters on the SDK's five-second
    /// batches make 891 outliers at two seconds, 1 008 at one and 1 412 at half a second, for reads within
    /// a few percent of each other. Traffic slower than a span a second is listed whole — a tier that
    /// slow is flushed at 500 spans.
    /// </summary>
    internal const long ToleranceNanos = 1_000_000_000L;

    /// <summary>
    /// Outliers an index keeps apart before it widens blocks again: a sixteenth of the 65 536 spans it
    /// is sized for, 48 KB of list at most. One span in twenty skewed, 2 450 of a 49 000-span tier, stays
    /// under it; at one in ten the list fills two thirds of the way in, and a page reads 9 236 spans
    /// instead of the 20 072 every block's widening made it. A reader sorts at most this many one-span
    /// blocks more than an in-order tier makes it.
    /// </summary>
    internal const int MaxOutliers = 4_096;

    /// <summary>
    /// Outliers in a row, each within the tolerance of the one before, that make a new level — FOUR.
    /// A skewed producer's runs shorter than this stay out of their blocks; longer ones re-anchor and
    /// widen their block, as every skewed span did before #127 (2 % of traffic 30 s ahead in runs of
    /// four: a TraceQL page reads 20 584 spans at four, 2 108 at eight; runs of 64 read 2 650 at any
    /// run short of 64). Every step up lists more of ordinary traffic's level changes: twenty exporters
    /// on five-second batches make 640 outliers at two, 1 008 at four, 1 781 at eight and 3 375 at
    /// sixteen, and with no new level at all one gap in the traffic fills the list
    /// (<c>SpanStartIndexToleranceProbe</c>).
    /// </summary>
    internal const int ShiftAfter = 4;

    private const int InitialOutliers = 64;

    /// <summary>
    /// Test seam: the tolerance, cap and level-shift run the NEXT indexes are built with — a probe
    /// compares values with it. Read once, at construction. Production never sets it.
    /// </summary>
    internal static (long ToleranceNanos, int MaxOutliers, int ShiftAfter)? ShapeForTest;

    private long[] _min;
    private long[] _max;
    private int    _count;

    private int[]?  _outPos;
    private long[]? _outStart;
    private int     _outCount;

    private readonly long _tolerance;
    private readonly int  _maxOutliers;
    private readonly int  _shiftAfter;

    // What the next span is judged by: the range of the block that last took a regular span (empty
    // until the first one), the previous span's start, and how many outliers in a row have agreed.
    private long _refMin = long.MaxValue;
    private long _refMax = long.MinValue;
    private long _lastStart;
    private int  _streak;

    /// <param name="owner">The list this index describes, span for span, in the list's order.</param>
    /// <param name="spans">How many spans to size for; the arrays grow past it.</param>
    internal SpanStartIndex(List<SpanRecord> owner, int spans = InitialBlocks * BlockSize)
    {
        Owner = owner;
        int blocks = Math.Max(InitialBlocks, (spans + BlockMask) >> BlockShift);
        _min = new long[blocks];
        _max = new long[blocks];
        var shape    = ShapeForTest;
        _tolerance   = shape?.ToleranceNanos ?? ToleranceNanos;
        _maxOutliers = shape?.MaxOutliers    ?? MaxOutliers;
        _shiftAfter  = shape?.ShiftAfter     ?? ShiftAfter;
    }

    /// <summary>The list whose spans this index bounds. A reader that does not find this list beside it reads unindexed.</summary>
    internal List<SpanRecord> Owner { get; }

    /// <summary>Spans appended. Equal to <see cref="Owner"/>'s count whenever the engine lock is free.</summary>
    internal int Count => _count;

    /// <summary>Spans kept out of their block's range.</summary>
    internal int Outliers => _outCount;

    /// <summary>
    /// The next span of <see cref="Owner"/>, by its start. Called under the engine write lock BEFORE
    /// the span joins the list: a growth that throws then leaves both as they were, and a list add
    /// that throws after it leaves this index one ahead — which <see cref="Count"/> makes every
    /// reader see, and read the list unindexed, until the tier is replaced.
    /// </summary>
    internal void Append(long startNano)
    {
        int  i     = _count;
        int  b     = i >> BlockShift;
        bool opens = (i & BlockMask) == 0;
        if (b == _min.Length) Grow();

        // The range it is judged by: its block's, or — a block with no regular span yet — the last one's.
        long lo = opens ? long.MaxValue : _min[b], hi = opens ? long.MinValue : _max[b];
        if (lo > hi) { lo = _refMin; hi = _refMax; }

        bool regular = lo > hi || Near(startNano, lo, hi, _tolerance);
        int  streak  = 0;
        if (!regular)
        {
            streak  = _streak > 0 && Near(startNano, _lastStart, _lastStart, _tolerance) ? _streak + 1 : 1;
            regular = streak >= _shiftAfter || _outCount == _maxOutliers;
            if (!regular && (_outPos is null || _outCount == _outPos.Length)) GrowOutliers();
        }

        // Nothing below allocates or throws.
        if (opens)
        {
            Volatile.Write(ref _min[b], long.MaxValue);   // no regular span yet: a range that holds nothing
            Volatile.Write(ref _max[b], long.MinValue);
        }
        if (regular)
        {
            if (startNano < _min[b]) Volatile.Write(ref _min[b], startNano);
            if (startNano > _max[b]) Volatile.Write(ref _max[b], startNano);
            _refMin = _min[b];
            _refMax = _max[b];
            _streak = 0;
        }
        else
        {
            _outPos![_outCount]   = i;
            _outStart![_outCount] = startNano;
            _outCount++;
            _streak = streak;
        }
        _lastStart = startNano;
        _count     = i + 1;
    }

    /// <summary>
    /// <paramref name="start"/> lies within <paramref name="tolerance"/> of <c>[lo, hi]</c>. In
    /// unsigned differences, so a start from a broken producer anywhere in the 64-bit range cannot
    /// overflow into "near".
    /// </summary>
    private static bool Near(long start, long lo, long hi, long tolerance) =>
        start < lo ? (ulong)(lo - start) <= (ulong)tolerance
      : start > hi ? (ulong)(start - hi) <= (ulong)tolerance
      :              true;

    /// <summary>An index over a list that was built some other way (a failed flush's restored tier). O(n), once.</summary>
    internal static SpanStartIndex Build(List<SpanRecord> spans)
    {
        var index = new SpanStartIndex(spans, spans.Count);
        foreach (var s in spans) index.Append(s.StartTimeUnixNano);
        return index;
    }

    /// <summary>
    /// The bounds of the first <paramref name="spans"/> spans and the outliers among them — taken
    /// under the engine read lock with the run they describe.
    /// </summary>
    internal SpanStartView View(int spans)
    {
        int outliers = _outCount;
        if (spans < _count)
        {
            // A shorter prefix than the index holds: only the outliers inside it (positions ascend).
            int lo = 0, hi = outliers;
            while (lo < hi)
            {
                int mid = (lo + hi) >>> 1;
                if (_outPos![mid] < spans) lo = mid + 1; else hi = mid;
            }
            outliers = lo;
        }
        return new(_min, _max, spans, _outPos, _outStart, outliers);
    }

    private void Grow()
    {
        var min = new long[_min.Length * 2];
        var max = new long[_max.Length * 2];
        Array.Copy(_min, min, _min.Length);
        Array.Copy(_max, max, _max.Length);
        _min = min;   // the old arrays stay valid for any reader that captured them
        _max = max;
    }

    private void GrowOutliers()
    {
        int size  = Math.Min(_outPos is null ? InitialOutliers : _outPos.Length * 2, _maxOutliers);
        var pos   = new int[size];
        var start = new long[size];
        if (_outPos is not null)
        {
            Array.Copy(_outPos,   pos,   _outCount);
            Array.Copy(_outStart!, start, _outCount);
        }
        _outPos   = pos;     // as with the bounds: a captured pair stays valid, and stays the pair
        _outStart = start;
    }
}

/// <summary>
/// A captured <see cref="SpanStartIndex"/>: the bounds of each block of one run of
/// <see cref="Spans"/> spans, and the run's outliers — its spans kept out of those bounds, by
/// ascending position, each with its exact start. <c>default</c> is an UNINDEXED run, which a
/// reader walks whole.
/// </summary>
internal readonly struct SpanStartView
{
    private readonly long[]? _min;
    private readonly long[]? _max;
    private readonly int[]?  _outPos;
    private readonly long[]? _outStart;

    internal SpanStartView(long[] min, long[] max, int spans, int[]? outPos, long[]? outStart, int outliers)
    {
        _min      = min;
        _max      = max;
        Spans     = spans;
        _outPos   = outPos;
        _outStart = outStart;
        Outliers  = outliers;
    }

    /// <summary>The run's length when it was captured.</summary>
    internal readonly int Spans;

    /// <summary>How many of the run's spans are outliers, every one at a position below <see cref="Spans"/>.</summary>
    internal readonly int Outliers;

    internal bool IsIndexed => _min is not null;

    /// <summary>Blocks covering the run; the last may be partial.</summary>
    internal int Blocks => (Spans + SpanStartIndex.BlockSize - 1) >> SpanStartIndex.BlockShift;

    /// <summary>A lower bound on every start in <paramref name="block"/> but its outliers'. Above <see cref="MaxOf"/> while it holds none.</summary>
    internal long MinOf(int block) => Volatile.Read(ref _min![block]);

    /// <summary>An upper bound on every start in <paramref name="block"/> but its outliers'.</summary>
    internal long MaxOf(int block) => Volatile.Read(ref _max![block]);

    /// <summary>True when <paramref name="block"/>'s spans other than its outliers may hold a start in <c>[fromNano, toNano]</c>.</summary>
    internal bool Overlaps(int block, long fromNano, long toNano) =>
        MaxOf(block) >= fromNano && MinOf(block) <= toNano;

    /// <summary>The run position of outlier <paramref name="k"/>; ascending in <paramref name="k"/>.</summary>
    internal int OutlierPosition(int k) => _outPos![k];

    /// <summary>The exact start of outlier <paramref name="k"/>.</summary>
    internal long OutlierStart(int k) => _outStart![k];
}
