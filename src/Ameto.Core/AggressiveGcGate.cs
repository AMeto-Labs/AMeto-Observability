namespace Ameto.Core;

/// <summary>
/// Process-wide gate in front of <c>GC.Collect(2, Aggressive, blocking, compacting)</c>.
///
/// <para>Three independent paths ask for an aggressive collection — RAM pressure
/// (<c>RamPressureService</c>), log-segment merges (<c>StorageEngine</c>) and metric
/// rollup (<c>MetricStorageEngine</c>). Each is individually a "background" trigger, but
/// the pause is not background: a blocking compacting gen2 stops EVERY thread, ingest and
/// query included. With no coordination, a GC trace caught two of them 8 s apart in the
/// middle of peak load — the two longest pauses in the whole file (246 ms and 219 ms).</para>
///
/// <para>The gate lets the first caller through and turns the pile-up into a no-op, which
/// is correct as well as cheap: immediately after an aggressive compacting collection
/// there is nothing left for a second one to reclaim.</para>
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
        long now = Math.Max(1, Environment.TickCount64);
        while (true)
        {
            long last = Interlocked.Read(ref _lastCollectMs);
            if (last != 0 && now - last < (long)minInterval.TotalMilliseconds)
                return false;
            // Claim the slot BEFORE collecting, so a caller arriving mid-collection
            // skips instead of queueing a second stop-the-world right behind the first.
            if (Interlocked.CompareExchange(ref _lastCollectMs, now, last) == last)
                break;
        }

        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        return true;
    }
}
