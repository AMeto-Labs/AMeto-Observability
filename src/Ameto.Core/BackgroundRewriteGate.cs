namespace Ameto.Core;

/// <summary>
/// THE TURN THE TWO BACKGROUND REWRITES TAKE: a trace compaction pass and a metric rewrite chunk
/// (#125). One instance per process, shared through DI; an engine built without one (a test) gets
/// its own, so nothing is shared that the composition did not mean to share.
///
/// <para><b>Why a gate and not another share of the heap.</b> Both are the same kind of work — read
/// files back, hold what they decode, write the merged result — and both are bounded by
/// <see cref="MemoryBudgets.TraceMergeBytes"/> (6 % of the managed-heap limit, 24.2 MB on the 512 MB
/// stand): the metric rewrite's budget defaults to that same figure
/// (<c>MetricsOptions.RewriteBudgetBytes</c>). Taking turns is what makes one figure true for both: at
/// most one of them holds its working set at any moment. A separate share would have had to come out
/// of the managed shares <c>MetricBudgetWiringTests</c> holds at 0.58, and every one of them already
/// sits at a documented floor — the index builds at two concurrent flushes, the parked ingest buffers
/// at one bucket set, the cache at the stand's recommended 48 MB, the tiers at their flush cadence,
/// the trace merge at the segment count it buys.</para>
///
/// <para>Held for one unit of work at a time — a trace pass; a metric chunk, a time slice, or the
/// planning walk over one source — and never across an await: both rewrites are synchronous on the
/// thread that runs them. Nothing takes another lock that the other side holds while it waits here,
/// so the wait is bounded by the other side's unit.</para>
///
/// <para>What a turn does not cover: a metric rewrite keeps its block buffers
/// (<c>MetricReader.ReadScratch</c>) for its whole length, between turns as well — up to twice its
/// largest source's block, beside the share rather than inside it.</para>
/// </summary>
public sealed class BackgroundRewriteGate
{
    private readonly Lock _lock = new();

    /// <summary>Waits for the turn and holds it until the scope is disposed.</summary>
    public Lock.Scope Enter() => _lock.EnterScope();
}
