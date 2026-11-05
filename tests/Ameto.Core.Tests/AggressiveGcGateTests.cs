using Ameto.Core;

namespace Ameto.Core.Tests;

/// <summary>
/// The gate in front of the gen2 collections the server asks for by hand: the aggressive blocking
/// one (RAM pressure) and the background one (a log merge, a metric rollup). The rule under test is
/// the asymmetry: a background request yields to either shape's recent collection, an aggressive
/// request yields only to an aggressive one — a merge's or a rollup's sweep must never cost the
/// pressure path its compaction. Intervals here are an hour or more, and "just inside" means ten seconds short
/// of it, so no tick of <c>Environment.TickCount64</c> can move a test over the line.
///
/// <para>NOTHING HERE COLLECTS FOR REAL. The gate's test seam records the shape it would have
/// collected: this assembly runs its classes in parallel, and an aggressive compaction would stop
/// every one of them — and the recording is what lets each fact say WHICH collection went through,
/// not only that one did.</para>
/// </summary>
public sealed class AggressiveGcGateTests : IDisposable
{
    private static readonly TimeSpan Interval  = TimeSpan.FromHours(1);
    private static readonly TimeSpan Inside    = Interval - TimeSpan.FromSeconds(10);
    private static readonly TimeSpan WellPast  = Interval + TimeSpan.FromSeconds(10);

    private readonly List<AggressiveGcGate.Shape> _collected = [];

    public AggressiveGcGateTests() => AggressiveGcGate._collectForTest = _collected.Add;

    public void Dispose() => AggressiveGcGate._collectForTest = null;

    [Fact]
    public void A_background_request_is_let_through_once_per_interval()
    {
        AggressiveGcGate.ResetForTest();

        Assert.True(AggressiveGcGate.TryCollectInBackground(Interval));
        Assert.False(AggressiveGcGate.TryCollectInBackground(Interval));
        Assert.Equal(new[] { AggressiveGcGate.Shape.Background }, _collected);
    }

    [Fact]
    public void A_recent_background_request_does_not_stand_in_an_aggressive_collections_way()
    {
        // THE PRESSURE PATH MUST WIN. A merge's background sweep compacted nothing and handed
        // nothing back; the collection RamPressureService asks for a moment later has everything
        // still to do.
        AggressiveGcGate.SetForTest(aggressiveAgo: null, backgroundAgo: TimeSpan.FromSeconds(1));

        Assert.True(AggressiveGcGate.TryCollect(Interval));
        Assert.Equal(new[] { AggressiveGcGate.Shape.Aggressive }, _collected);
    }

    [Fact]
    public void A_recent_aggressive_collection_skips_a_background_request_until_the_interval_passes()
    {
        AggressiveGcGate.SetForTest(aggressiveAgo: Inside, backgroundAgo: null);
        Assert.False(AggressiveGcGate.TryCollectInBackground(Interval));

        AggressiveGcGate.SetForTest(aggressiveAgo: WellPast, backgroundAgo: null);
        Assert.True(AggressiveGcGate.TryCollectInBackground(Interval));
        Assert.Equal(new[] { AggressiveGcGate.Shape.Background }, _collected);
    }

    [Fact]
    public void A_background_request_waits_out_its_own_interval_and_no_longer()
    {
        AggressiveGcGate.SetForTest(aggressiveAgo: null, backgroundAgo: Inside);
        Assert.False(AggressiveGcGate.TryCollectInBackground(Interval));

        AggressiveGcGate.SetForTest(aggressiveAgo: null, backgroundAgo: WellPast);
        Assert.True(AggressiveGcGate.TryCollectInBackground(Interval));
        Assert.Equal(new[] { AggressiveGcGate.Shape.Background }, _collected);
    }

    [Fact]
    public void An_aggressive_collection_is_gated_by_the_last_aggressive_one_only()
    {
        AggressiveGcGate.SetForTest(aggressiveAgo: Inside, backgroundAgo: WellPast);
        Assert.False(AggressiveGcGate.TryCollect(Interval));
        Assert.Empty(_collected);
    }

    [Fact]
    public void A_stamp_older_than_the_machines_uptime_still_reads_as_long_past()
    {
        // A CI runner boots minutes before the run, so "an hour ago" lies before its tick count began.
        // Such a stamp must read as old, not as a moment after boot: the clamp that made it read so
        // failed two facts above on the runner and nowhere else.
        AggressiveGcGate.SetForTest(aggressiveAgo: TimeSpan.FromDays(400), backgroundAgo: TimeSpan.FromDays(400));

        Assert.True(AggressiveGcGate.TryCollectInBackground(Interval));
        Assert.True(AggressiveGcGate.TryCollect(Interval));
        Assert.Equal(new[] { AggressiveGcGate.Shape.Background, AggressiveGcGate.Shape.Aggressive }, _collected);
    }

    [Fact]
    public void A_zero_interval_never_gates()
    {
        AggressiveGcGate.ResetForTest();

        Assert.True(AggressiveGcGate.TryCollectInBackground(TimeSpan.Zero));
        Assert.True(AggressiveGcGate.TryCollectInBackground(TimeSpan.Zero));
        Assert.Equal(new[] { AggressiveGcGate.Shape.Background, AggressiveGcGate.Shape.Background }, _collected);
    }

    [Fact]
    public void Each_shape_asks_the_runtime_for_what_its_name_says()
    {
        // The arguments themselves, which no fact above can see through the seam: swapped, the
        // merge loop would stop the world every two minutes again and the pressure path would sweep.
        Assert.Equal((GCCollectionMode.Aggressive, true, true),
                     AggressiveGcGate.ArgumentsOf(AggressiveGcGate.Shape.Aggressive));
        Assert.Equal((GCCollectionMode.Default, false, false),
                     AggressiveGcGate.ArgumentsOf(AggressiveGcGate.Shape.Background));
    }
}
