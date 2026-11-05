using Ameto.Core;
using Ameto.Metrics.Storage;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT A METRIC ROLLUP ASKS THE GC FOR (#141 follow-up): the background collection a log merge asks
/// for, not the aggressive blocking compacting one it forced after every pass that rewrote at least
/// <see cref="AggressiveGcGate.MaintenancePassBytesFloor"/> — a stop-the-world pause of 100–250 ms on
/// the stand, on the five-minute cadence, with a decommit the next pass undid. Pinned at the call, as
/// <see cref="MaintenanceCollectShapeTests"/> pins the merge: the gate's own facts are about the gate,
/// and a rollup put back on the aggressive entry point keeps every one of them green. That a pass which
/// rewrote enough reaches this call, and with the same shape, is pinned in <c>MetricRewriteBackoffTests</c>.
///
/// <para>The gate is process-wide; this assembly runs its classes one at a time, so the seam and the
/// reset below cannot reach another test's merge or rollup.</para>
/// </summary>
public sealed class MetricRollupCollectShapeTests : IDisposable
{
    private readonly List<AggressiveGcGate.Shape> _collected = [];

    public MetricRollupCollectShapeTests()
    {
        AggressiveGcGate.ResetForTest();                     // a merge or a rollup earlier in the run may hold the slot
        AggressiveGcGate._collectForTest = _collected.Add;
    }

    public void Dispose()
    {
        AggressiveGcGate._collectForTest = null;
        AggressiveGcGate.ResetForTest();
    }

    [Fact]
    public void A_rollup_asks_for_a_background_collection()
    {
        MetricStorageEngine.ReleaseRollupMemory();

        Assert.Equal(new[] { AggressiveGcGate.Shape.Background }, _collected);
    }

    [Fact]
    public void A_rollup_right_after_a_pressure_collection_asks_for_nothing()
    {
        // A pressure collection a moment ago compacted the heap: there is nothing left for the rollup's
        // sweep to reclaim, so it yields — the one direction the gate lets one shape stand in for another.
        AggressiveGcGate.SetForTest(aggressiveAgo: TimeSpan.FromSeconds(1), backgroundAgo: null);

        MetricStorageEngine.ReleaseRollupMemory();

        Assert.Empty(_collected);
    }

    [Fact]
    public void A_rollup_right_after_a_merges_sweep_asks_for_nothing()
    {
        // The two maintenance passes share the background slot: a merge's sweep a moment ago reclaimed
        // what the rollup's would, so the rollup yields to it, as a merge yields to a rollup's.
        AggressiveGcGate.SetForTest(aggressiveAgo: null, backgroundAgo: TimeSpan.FromSeconds(1));

        MetricStorageEngine.ReleaseRollupMemory();

        Assert.Empty(_collected);
    }
}
