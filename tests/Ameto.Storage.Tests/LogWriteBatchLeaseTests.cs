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
    ///
    /// <para>Raced closely (round-5 review L-8, which found it caught its revert in 1 run of 5 on two
    /// cores): the batch is reused, and the ender reads it — taking its cache line — before the dispose,
    /// which comes at a sweep of offsets around the ender's. On two cores that are separate physical
    /// cores this catches the revert in every run, as did the version before it; on two that are the
    /// hyperthreads of one core (the 2-core runs here: affinity 0x3), where a store's stay in the
    /// buffer is a few cycles, it caught it in 3 runs of 10 where the version before it caught it in
    /// none.</para>
    /// </summary>
    [Fact]
    public void A_dispose_racing_the_end_of_the_write_that_holds_the_batch_gives_it_back()
    {
        LogWriteBatch? current = null;
        int round = 0, ended = 0, touched = 0;
        var ender = new Thread(() =>
        {
            int seen = 0;
            while (true)
            {
                int now;
                while ((now = Volatile.Read(ref round)) == seen) { }
                seen = now;
                if (seen < 0) return;
                var b = Volatile.Read(ref current)!;
                Volatile.Write(ref touched, b.ItemCapacity);   // the batch's cache line, shared with this core
                for (int k = seen & 7; k > 0; k--) Thread.SpinWait(1);
                b.EndLease();
                Volatile.Write(ref ended, 1);
            }
        }) { IsBackground = true };
        ender.Start();

        long races = 0, kept = 0;
        var batch = new LogWriteBatch();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (sw.Elapsed < TimeSpan.FromSeconds(3))
        {
            batch.Add(new LogEventHeader { TimestampUtcTicks = 1, Level = LogLevel.Information }, [0x80], "race {n}", exception: null);
            batch.Lease();
            Volatile.Write(ref current, batch);
            Volatile.Write(ref ended, 0);
            Volatile.Write(ref touched, -1);
            Interlocked.Increment(ref round);
            while (Volatile.Read(ref touched) < 0) { }                                    // the ender has the line too
            for (int k = (int)(((races + 1) >> 3) & 7); k > 0; k--) Thread.SpinWait(1);   // offsets, both ways
            batch.Dispose();
            while (Volatile.Read(ref ended) == 0) { }
            races++;
            if (batch.ItemCapacity != 0) { kept++; batch = new LogWriteBatch(); }   // neither side gave it back: a fresh one
        }
        Volatile.Write(ref round, -1);
        ender.Join();

        Assert.True(races > 10_000, $"only {races} races in three seconds: nothing was measured");
        Assert.True(kept == 0, $"{kept} of {races} batches were never given back");
    }
}
