using Ameto.Core;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT A LOG MERGE ASKS THE GC FOR (#141): the background collection, not the aggressive blocking
/// compacting one it used to force after every merge — a stop-the-world pause every two minutes for
/// as long as merges had a backlog. Pinned at the call because nothing else can see it: the gate's own
/// facts are about the gate, and a merge put back on the aggressive entry point keeps every one of
/// them green.
///
/// <para>The gate is process-wide; this assembly runs its classes one at a time, so the seam and the
/// reset below cannot reach another test's merge.</para>
/// </summary>
public sealed class MaintenanceCollectShapeTests : IDisposable
{
    private readonly List<AggressiveGcGate.Shape> _collected = [];

    public MaintenanceCollectShapeTests()
    {
        AggressiveGcGate.ResetForTest();                     // a merge earlier in the run may hold the slot
        AggressiveGcGate._collectForTest = _collected.Add;
    }

    public void Dispose()
    {
        AggressiveGcGate._collectForTest = null;
        AggressiveGcGate.ResetForTest();
    }

    [Fact]
    public void A_merge_asks_for_a_background_collection()
    {
        StorageEngine.ReleaseMaintenanceMemory();

        Assert.Equal(new[] { AggressiveGcGate.Shape.Background }, _collected);
    }

    [Fact]
    public void A_merge_right_after_a_pressure_collection_asks_for_nothing()
    {
        // A pressure collection a moment ago compacted the heap: there is nothing left for the merge's
        // sweep to reclaim, so it yields — the one direction the gate lets one shape stand in for another.
        AggressiveGcGate.SetForTest(aggressiveAgo: TimeSpan.FromSeconds(1), backgroundAgo: null);

        StorageEngine.ReleaseMaintenanceMemory();

        Assert.Empty(_collected);
    }
}
