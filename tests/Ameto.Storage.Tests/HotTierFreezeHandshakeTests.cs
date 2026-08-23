using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// <c>Freeze()</c> is the handover from the ingest thread to the flush thread, and everything
/// the flush does afterwards is derived from the <c>Count</c> it reads right after: the sort
/// order, which events go into the cold segment, and — once that segment is published — the
/// decision to unlist the tier, free its memory and delete its WAL.
///
/// <para>So the contract is not "no write may start after Freeze" but "no event may APPEAR
/// after Freeze". Those differ by a writer sitting between the frozen check and the publish:
/// its event would be accepted (<c>TryWrite</c> returns true, the drainer drops it from the
/// ring), served by queries for the length of the flush, and then vanish with the tier — with
/// no WAL copy either, because that WAL is deleted when the flush publishes.</para>
/// </summary>
public sealed class HotTierFreezeHandshakeTests
{
    /// <summary>Enough rounds that a writer landing inside the window is a certainty, not a hope.</summary>
    private const int Rounds = 200;

    /// <summary>How far the writer must get before the freeze, so it is provably mid-loop.</summary>
    private const int WritesBeforeFreeze = 500;

    private static LogEventHeader Header(long ticks) => new()
    {
        TimestampUtcTicks = ticks,
        Level             = LogLevel.Information,
    };

    [Fact]
    public void Every_accepted_event_is_below_the_count_Freeze_hands_to_the_flush()
    {
        for (int round = 0; round < Rounds; round++)
        {
            // Sized so the writer cannot exhaust the tier before the freeze — a full tier
            // rejects writes and the window closes on its own, which would quietly turn this
            // into a test of nothing.
            using var tier = new HotTierSegment(maxEvents: 200_000, maxPayloadBytes: 64 * 1024 * 1024);

            int  accepted = 0;
            bool stop     = false;
            var  running  = new ManualResetEventSlim(false);

            var writer = Task.Factory.StartNew(() =>
            {
                long ticks = DateTimeOffset.UtcNow.UtcTicks;
                running.Set();
                while (!Volatile.Read(ref stop))
                {
                    if (tier.TryWrite(Header(ticks++), ReadOnlySpan<byte>.Empty))
                        Interlocked.Increment(ref accepted);
                }
            }, TaskCreationOptions.LongRunning);

            running.Wait(TimeSpan.FromSeconds(5));
            while (Volatile.Read(ref accepted) < WritesBeforeFreeze)
                Thread.SpinWait(64);

            tier.Freeze();
            int countAtFreeze = tier.Count;   // the number the flush would persist

            Volatile.Write(ref stop, true);
            writer.Wait(TimeSpan.FromSeconds(10));

            // The tier cannot grow behind the flush's back…
            Assert.Equal(countAtFreeze, tier.Count);
            // …and it holds exactly the events the writer was told were stored. An inequality
            // here IS the data loss: the difference is the events ingest acknowledged and the
            // cold segment never received.
            Assert.Equal(countAtFreeze, Volatile.Read(ref accepted));
        }
    }

    [Fact]
    public void A_write_that_loses_the_race_is_refused_rather_than_stranded()
    {
        // The other side of the same contract: once the tier is frozen the answer is false,
        // so the caller (the ingest drainer, holding the event in its pending slot) retries
        // it into the successor tier instead of losing it.
        using var tier = new HotTierSegment(maxEvents: 64, maxPayloadBytes: 64 * 1024);

        Assert.True(tier.TryWrite(Header(1), ReadOnlySpan<byte>.Empty));
        tier.Freeze();

        Assert.False(tier.TryWrite(Header(2), ReadOnlySpan<byte>.Empty));
        Assert.Equal(1, tier.Count);

        // Freeze is idempotent and does not block on a tier with no writer in flight.
        tier.Freeze();
        Assert.Equal(1, tier.Count);
    }
}
