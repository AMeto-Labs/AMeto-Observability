namespace Ameto.Core;

/// <summary>
/// Process-wide gate in front of every gen2 collection the server asks for by hand: the aggressive
/// blocking compacting one (<see cref="TryCollect"/>) and the background one
/// (<see cref="TryCollectInBackground"/>).
///
/// <para>Three independent paths used to ask for an aggressive collection — RAM pressure
/// (<c>RamPressureService</c>), log-segment merges (<c>StorageEngine</c>) and metric rollup
/// (<c>MetricStorageEngine</c>). Each is individually a "background" trigger, but the pause is
/// not background: a blocking compacting gen2 stops EVERY thread, ingest and query included. With
/// no coordination, a GC trace caught two of them 8 s apart in the middle of peak load — the two
/// longest pauses in the whole file (246 ms and 219 ms).</para>
///
/// <para>The gate lets the first caller through and turns the pile-up into a no-op, which is
/// correct as well as cheap: immediately after an aggressive compacting collection there is
/// nothing left for a second one to reclaim. The log merge path asks for a background collection
/// now (<see cref="TryCollectInBackground"/>) and still goes through the same timestamp, so it
/// cannot land right behind a pressure collection either, nor one right behind it.</para>
/// </summary>
public static class AggressiveGcGate
{
    /// <summary>
    /// Floor for maintenance passes: below this many source bytes a pass should skip its
    /// aggressive release entirely — the materialised peak is small enough for the next
    /// natural gen2 to absorb.
    ///
    /// <para>Only the metric-rollup loop tests it now. It was shared with the log side's HC
    /// re-compression sweep, whose idle pass rewrote a couple of tiny segments per tick and
    /// paid a stop-the-world gen2 for it; that sweep is gone, and log maintenance releases on
    /// a merge having happened rather than on a byte count. Kept here rather than moved into
    /// the metric engine so a second maintenance loop that ever needs a floor adopts this one
    /// instead of inventing its own.</para>
    /// </summary>
    public const long MaintenancePassBytesFloor = 8L * 1024 * 1024;

    /// <summary><see cref="Environment.TickCount64"/> (ms) of the last collection; 0 = never.</summary>
    private static long _lastCollectMs;

    /// <summary>
    /// Runs an aggressive blocking compacting gen2 collection unless one already ran within
    /// <paramref name="minInterval"/>. Returns whether the collection happened — callers
    /// should skip their working-set trim on false, since without a collection there are
    /// no freshly freed pages to hand back.
    /// </summary>
    public static bool TryCollect(TimeSpan minInterval)
    {
        if (!TryClaim(minInterval)) return false;
        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        return true;
    }

    /// <summary>
    /// Asks for a BACKGROUND gen2 — <c>GC.Collect(2, Default, blocking: false, compacting: false)</c> —
    /// unless any collection went through this gate within <paramref name="minInterval"/>. What a
    /// log merge asks for once it has merged (<c>StorageEngine.ReleaseMaintenanceMemory</c>): the
    /// garbage the pass left is reclaimed and the gen2 callbacks the pools trim on
    /// (<see cref="PoolTrimPolicy.OnGen2Collection"/>) fire, while every other thread keeps running
    /// but for the collection's two short suspensions. It used to be the aggressive collection above:
    /// a stop-the-world mark and compaction of the whole heap — 65 ms on a 20-core box, longer on the
    /// two-core stand — and a decommit of everything freed, which the next flush re-committed page by
    /// page, every two minutes for as long as merges had a backlog. Aggressive mode is for a process
    /// going idle, not one about to allocate the same 40–90 MB again. Returns whether the collection
    /// was asked for.
    /// </summary>
    public static bool TryCollectInBackground(TimeSpan minInterval)
    {
        if (!TryClaim(minInterval)) return false;
        GC.Collect(2, GCCollectionMode.Default, blocking: false, compacting: false);
        return true;
    }

    /// <summary>
    /// Claims the gate for a collection now unless one went through it within
    /// <paramref name="minInterval"/>. The slot is claimed BEFORE collecting, so a caller arriving
    /// mid-collection skips instead of queueing a second one right behind the first.
    /// </summary>
    private static bool TryClaim(TimeSpan minInterval)
    {
        long now = Math.Max(1, Environment.TickCount64);
        while (true)
        {
            long last = Interlocked.Read(ref _lastCollectMs);
            if (last != 0 && now - last < (long)minInterval.TotalMilliseconds)
                return false;
            if (Interlocked.CompareExchange(ref _lastCollectMs, now, last) == last)
                return true;
        }
    }

    /// <summary>Forgets the last collection, so a test sees the gate open whatever ran before it.</summary>
    internal static void ResetForTest() => Interlocked.Exchange(ref _lastCollectMs, 0);
}
