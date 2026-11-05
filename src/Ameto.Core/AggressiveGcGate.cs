namespace Ameto.Core;

/// <summary>
/// Process-wide gate in front of every gen2 collection the server asks for by hand. Two shapes
/// go through it: the AGGRESSIVE blocking compacting collection (<see cref="TryCollect"/>) that
/// RAM pressure (<c>RamPressureService</c>) and the metric rollup (<c>MetricStorageEngine</c>) ask
/// for, and the BACKGROUND collection (<see cref="TryCollectInBackground"/>) a log merge asks for
/// (<c>StorageEngine.ReleaseMaintenanceMemory</c>).
///
/// <para>Why a gate at all: each caller is individually a "background" trigger, but an aggressive
/// collection's pause is not background — it stops EVERY thread, ingest and query included. With
/// no coordination, a GC trace caught two of them 8 s apart in the middle of peak load — the two
/// longest pauses in the whole file (246 ms and 219 ms). The gate lets the first through and
/// turns the pile-up into a no-op, which is correct as well as cheap: immediately after an
/// aggressive compacting collection there is nothing left for a second one to reclaim.</para>
///
/// <para>THE TWO SHAPES KEEP SEPARATE TIMESTAMPS, and they yield to each other in one direction
/// only. A background request yields to both: to a recent aggressive collection (nothing left
/// to reclaim) and to a recent background one (its own spacing). An aggressive request yields
/// ONLY to a recent aggressive collection — never to a background one, which compacted nothing
/// and handed nothing back to the OS, so the pressure path's collection, or the rollup's, must
/// not be skipped because a log merge asked for a background sweep a moment ago. With one
/// shared timestamp the merge loop's claims took the pressure path's slot about a quarter of
/// the time during a merge backlog, and the pressure loop still waited out its five-minute
/// cooldown as if it had collected.</para>
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

    /// <summary><see cref="Environment.TickCount64"/> (ms) of the last aggressive collection; 0 = never.</summary>
    private static long _lastAggressiveMs;

    /// <summary><see cref="Environment.TickCount64"/> (ms) of the last background request; 0 = never.</summary>
    private static long _lastBackgroundMs;

    /// <summary>
    /// Runs an aggressive blocking compacting gen2 collection unless one already ran within
    /// <paramref name="minInterval"/>. A background request within the interval does not stand
    /// in its way (see the type). Returns whether the collection happened — callers should skip
    /// their working-set trim on false, since without a collection there are no freshly freed
    /// pages to hand back.
    /// </summary>
    public static bool TryCollect(TimeSpan minInterval)
    {
        long now = Now();
        if (!TryClaim(ref _lastAggressiveMs, now, minInterval)) return false;
        Collect(Shape.Aggressive);
        return true;
    }

    /// <summary>
    /// Asks for a BACKGROUND gen2 — <c>GC.Collect(2, Default, blocking: false, compacting: false)</c>
    /// — unless an aggressive collection ran, or a background one was asked for, within
    /// <paramref name="minInterval"/>. What a log merge asks for once it has merged
    /// (<c>StorageEngine.ReleaseMaintenanceMemory</c>): the garbage the pass left is reclaimed and
    /// the gen2 callbacks the pools trim on (<see cref="PoolTrimPolicy.OnGen2Collection"/>) fire.
    ///
    /// <para>What it costs, and what it does not do. With concurrent GC on (the server's setting)
    /// the runtime runs this as a background collection: two short suspensions around a mark that
    /// runs beside the application. That is the common case, not a guarantee — the runtime decides
    /// the shape, and with <c>GCConserveMemory</c> set and the heap fragmented past its threshold
    /// it may take the chance to compact instead; with concurrent GC off every collection blocks.
    /// It does not decommit what it frees, so it is not what hands pages back to the OS; the
    /// aggressive shape is — RAM pressure's, and a metric rollup's after a pass that rewrote at least
    /// <see cref="MaintenancePassBytesFloor"/>. And the request takes the gate whether or not the
    /// runtime starts a collection for it (one may already be running), which costs at most one
    /// skipped request an interval later. It replaced the aggressive collection on this path: a
    /// stop-the-world mark and compaction of the whole heap — 40–65 ms on a 20-core box, longer on
    /// the two-core stand — and a decommit of everything freed, which the next flush re-committed
    /// page by page, every two minutes for as long as merges had a backlog. Aggressive mode is for
    /// a process going idle, not one about to allocate the same 40–90 MB again.</para>
    /// Returns whether the collection was asked for.
    /// </summary>
    public static bool TryCollectInBackground(TimeSpan minInterval)
    {
        long now        = Now();
        long aggressive = Interlocked.Read(ref _lastAggressiveMs);
        if (aggressive != 0 && now - aggressive < (long)minInterval.TotalMilliseconds) return false;
        if (!TryClaim(ref _lastBackgroundMs, now, minInterval)) return false;
        Collect(Shape.Background);
        return true;
    }

    private static long Now() => Math.Max(1, Environment.TickCount64);

    /// <summary>The two collections the gate fronts — what <see cref="_collectForTest"/> is handed.</summary>
    internal enum Shape { Aggressive, Background }

    /// <summary>
    /// What each shape asks <see cref="GC.Collect(int, GCCollectionMode, bool, bool)"/> for. Data rather
    /// than two call sites, so a fact can pin it: the two argument lists swapped would otherwise pass
    /// every fact about which shape yields to which.
    /// </summary>
    internal static (GCCollectionMode Mode, bool Blocking, bool Compacting) ArgumentsOf(Shape shape) => shape switch
    {
        Shape.Aggressive => (GCCollectionMode.Aggressive, true,  true),
        _                => (GCCollectionMode.Default,    false, false),
    };

    /// <summary>
    /// Test seam: when set, the gate hands it the shape it would collect and collects nothing, so a fact
    /// can see which collection a caller asks for without stopping a parallel test run's world for an
    /// aggressive compaction. Process-wide, like the gate; never assigned in production.
    /// </summary>
    internal static Action<Shape>? _collectForTest;

    private static void Collect(Shape shape)
    {
        if (_collectForTest is { } seam) { seam(shape); return; }
        var (mode, blocking, compacting) = ArgumentsOf(shape);
        GC.Collect(2, mode, blocking, compacting);
    }

    /// <summary>
    /// Claims <paramref name="slot"/> for a collection at <paramref name="now"/> unless one went
    /// through it within <paramref name="minInterval"/>. The slot is claimed BEFORE collecting, so a
    /// caller arriving mid-collection skips instead of queueing a second one right behind the first.
    /// </summary>
    private static bool TryClaim(ref long slot, long now, TimeSpan minInterval)
    {
        while (true)
        {
            long last = Interlocked.Read(ref slot);
            if (last != 0 && now - last < (long)minInterval.TotalMilliseconds)
                return false;
            if (Interlocked.CompareExchange(ref slot, now, last) == last)
                return true;
        }
    }

    /// <summary>
    /// Puts the gate into a known state for a test: each shape's last collection this long ago, or
    /// never (null). Relative to now, so a test can stand just inside or well past an interval
    /// without a clock of its own.
    /// </summary>
    internal static void SetForTest(TimeSpan? aggressiveAgo, TimeSpan? backgroundAgo)
    {
        long now = Now();
        Interlocked.Exchange(ref _lastAggressiveMs, Stamp(now, aggressiveAgo));
        Interlocked.Exchange(ref _lastBackgroundMs, Stamp(now, backgroundAgo));
    }

    /// <summary>
    /// A tick-count stamp this long ago, or 0 for never. NOT clamped to the positive range: a CI runner
    /// boots minutes before the run, so "an hour ago" lies before its tick count started, and a clamp to
    /// 1 put every such stamp at the same instant — "inside the interval" and "well past it" became the
    /// same test, and two of them failed on the runner while passing on a box up for days. Only
    /// differences from now are ever taken, so a negative stamp is simply older.
    /// </summary>
    private static long Stamp(long now, TimeSpan? ago)
    {
        if (ago is not { } a) return 0;
        long stamp = now - (long)a.TotalMilliseconds;
        return stamp == 0 ? -1 : stamp;
    }

    /// <summary>Forgets every collection, so a test sees the gate open whatever ran before it.</summary>
    internal static void ResetForTest() => SetForTest(null, null);
}
