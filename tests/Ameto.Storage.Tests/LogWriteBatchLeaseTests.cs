using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// A <see cref="LogWriteBatch"/> disposed while a write holds it (its lease) gives its arrays and its
/// reservation back when that write ends — exactly once, whichever of the two comes first.
/// </summary>
public sealed class LogWriteBatchLeaseTests
{
    /// <summary>
    /// Round-4 review L2. Dispose wrote "a dispose is asked" with a volatile write and then read the
    /// leases; EndLease drops its lease and then takes the ask, both interlocked. A volatile write is
    /// not a fence: its store can wait in the store buffer while the read of the leases goes ahead,
    /// and racing an EndLease each side read the other's old value — Dispose a lease still held,
    /// EndLease no dispose asked — and neither gave the batch back: its arrays and its reservation
    /// held for the life of the process (measured on the bare shape: 353 in 44 M races in 10 s; 0 with
    /// a fence). The ask is an Interlocked.Exchange now. The two are raced here for three seconds.
    /// </summary>
    [Fact]
    public void A_dispose_racing_the_end_of_the_write_that_holds_the_batch_gives_it_back()
    {
        LogWriteBatch? current = null;
        int go = 0, ended = 0;
        var ender = new Thread(() =>
        {
            int seen = 0;
            while (true)
            {
                int now;
                while ((now = Volatile.Read(ref go)) == seen) Thread.SpinWait(1);
                seen = now;
                if (seen < 0) return;
                Volatile.Read(ref current)!.EndLease();
                Volatile.Write(ref ended, 1);
            }
        }) { IsBackground = true };
        ender.Start();

        long races = 0, kept = 0;
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (sw.Elapsed < TimeSpan.FromSeconds(3))
        {
            var batch = new LogWriteBatch();
            batch.Add(new LogEventHeader { TimestampUtcTicks = 1, Level = LogLevel.Information }, [0x80], "race {n}", exception: null);
            batch.Lease();
            Volatile.Write(ref current, batch);
            Volatile.Write(ref ended, 0);
            Interlocked.Increment(ref go);            // the write ends on the other thread ...
            batch.Dispose();                          // ... as the request disposes the batch on this one
            while (Volatile.Read(ref ended) == 0) Thread.SpinWait(1);
            races++;
            if (batch.ItemCapacity != 0) kept++;      // neither side gave it back
        }
        Volatile.Write(ref go, -1);
        ender.Join();

        Assert.True(races > 10_000, $"only {races} races in three seconds: nothing was measured");
        Assert.True(kept == 0, $"{kept} of {races} batches were never given back");
    }
}
