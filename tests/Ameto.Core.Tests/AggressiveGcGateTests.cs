using Ameto.Core;

namespace Ameto.Core.Tests;

/// <summary>
/// The gate in front of the gen2 collections the server asks for by hand. A log merge asks for a
/// background one (<c>StorageEngine.ReleaseMaintenanceMemory</c>); the pressure loop asks for the
/// aggressive blocking one. Both go through the same timestamp, so neither can land right behind
/// the other — which is the whole point of having one gate rather than one per caller.
/// </summary>
public sealed class AggressiveGcGateTests
{
    [Fact]
    public void A_background_request_is_let_through_once_per_interval_and_shares_the_gate()
    {
        AggressiveGcGate.ResetForTest();

        Assert.True(AggressiveGcGate.TryCollectInBackground(TimeSpan.FromHours(1)));
        Assert.False(AggressiveGcGate.TryCollectInBackground(TimeSpan.FromHours(1)));
        Assert.False(AggressiveGcGate.TryCollect(TimeSpan.FromHours(1)));   // the aggressive shape sees the same claim
    }

    [Fact]
    public void A_zero_interval_never_gates()
    {
        AggressiveGcGate.ResetForTest();

        Assert.True(AggressiveGcGate.TryCollectInBackground(TimeSpan.Zero));
        Assert.True(AggressiveGcGate.TryCollectInBackground(TimeSpan.Zero));
    }
}
