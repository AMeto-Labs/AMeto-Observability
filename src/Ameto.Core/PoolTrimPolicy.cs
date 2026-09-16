using System.Diagnostics;

namespace Ameto.Core;

/// <summary>
/// WHEN A GEN2 COLLECTION SHOULD EMPTY A POOL — one rule, and the gen2 hook that asks it.
///
/// <para>There were two copies of this: one in <see cref="IngestBufferPool"/> and one in the
/// index-build pools, each with its own private <c>Gen2GcCallback</c> and its own unsynchronised
/// 30-second window, so a single collection could run two independent trim policies. Both said
/// the same thing, and both said it with a caveat neither acted on.</para>
///
/// <h3>The caveat, and why it is now a condition</h3>
/// <para><c>GC.GetGCMemoryInfo().MemoryLoadBytes</c> is measured against the container's limit
/// under a cgroup or job object — and against the WHOLE MACHINE without one, which is the case
/// on a bare Windows host and in any container started without <c>--memory</c>. On such a host a
/// neighbouring process pushing the box past the GC's high threshold made every gen2 empty both
/// pools, for ever, once every 30 seconds: the ingest pool's megabytes of request buffers and the
/// build pools' slabs, tables and entry arrays re-allocated on the large object heap because of
/// someone else's memory. That is the churn these pools exist to remove, caused by the pools.
/// The 30-second window bounds how OFTEN it happens; it cannot stop it happening for ever.</para>
///
/// <para>So the same test the RAM-pressure loop already applies one layer up ("the pressure is
/// not ours; skipping"): act on a reading that describes this container, and on a host-wide
/// reading only when giving THIS pool's memory back would actually move the number. A pool
/// holding a couple of megabytes cannot relieve a machine-wide shortage, and emptying it only
/// makes the next refill worse.</para>
/// </summary>
public static class PoolTrimPolicy
{
    /// <summary>
    /// Shortest gap between two pressure trims of one pool.
    ///
    /// <para>Without a gap the trim feeds itself: every gen2 under sustained load empties the
    /// pool, the refill allocates on the large object heap, that brings the next gen2 forward,
    /// and the pool never gets past one refill. Thirty seconds is far longer than the interval
    /// between gen2 collections under load and far shorter than an operator would wait to see
    /// memory come back.</para>
    /// </summary>
    public static readonly TimeSpan MinTrimInterval = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Floor on what a pool must hold before a trim driven by a reading that is NOT ours is worth
    /// taking at all, whatever the machine's size.
    /// </summary>
    public const long MinWorthwhileBytes = 8L * 1024 * 1024;

    /// <summary>
    /// Whether a gen2 collection should empty a pool holding <paramref name="pooledBytes"/>.
    ///
    /// <para>Pure, and taking every input rather than reading any of them, so the branch it takes
    /// can be tested against numbers instead of against a real memory shortage on a real
    /// neighbour's schedule.</para>
    /// </summary>
    /// <param name="pooledBytes">What this pool would release — see <paramref name="readingIsOurs"/>.</param>
    /// <param name="scaleBytes">
    /// The memory the reading is measured against (the container limit, else host RAM). Used only
    /// to size "would this make a difference": one percent of it, as the RAM-pressure loop uses.
    /// </param>
    /// <param name="readingIsOurs">
    /// True when the load reading describes THIS container and a trim therefore relieves pressure
    /// this process is responsible for; false when it describes the whole machine.
    /// </param>
    /// <param name="nowTimestamp">A <see cref="Stopwatch.GetTimestamp"/> reading.</param>
    /// <param name="lastTrimTimestamp">The reading taken at the last pressure trim, or 0 for never.</param>
    public static bool ShouldTrim(
        long pooledBytes, long memoryLoadBytes, long highLoadThresholdBytes, long scaleBytes,
        bool readingIsOurs, long nowTimestamp, long lastTrimTimestamp)
    {
        // A threshold the runtime could not produce is not evidence of anything.
        if (highLoadThresholdBytes <= 0 || memoryLoadBytes < highLoadThresholdBytes) return false;

        if (lastTrimTimestamp != 0 &&
            nowTimestamp - lastTrimTimestamp < (long)(MinTrimInterval.TotalSeconds * Stopwatch.Frequency))
            return false;

        if (readingIsOurs) return true;

        // Host-wide: only if this pool is a meaningful share of what is being asked for.
        long needed = scaleBytes > 0 ? Math.Max(MinWorthwhileBytes, scaleBytes / 100) : MinWorthwhileBytes;
        return pooledBytes >= needed;
    }

    /// <summary>
    /// Whether this process runs under a memory limit of its own, so that the GC's load reading
    /// describes it rather than the machine. Read once — a container's limit does not change
    /// under it, and this is consulted from a finaliser-thread callback.
    ///
    /// <para>Linux cgroups (v2 then v1) are what can be detected portably and are what the
    /// deployments use. A Windows job object is not detected: the honest answer there is "this
    /// reading may not be ours", which costs only the extra test above — exactly the conclusion
    /// wanted on a shared Windows box, and a safe one inside a Windows container, where a pool
    /// holding a meaningful share still trims.</para>
    /// </summary>
    public static bool ReadingIsScopedToThisProcess => _scoped ??= DetectMemoryLimit();

    private static bool? _scoped;

    private static bool DetectMemoryLimit()
    {
        try
        {
            const string v2 = "/sys/fs/cgroup/memory.max";
            if (File.Exists(v2))
            {
                string raw = File.ReadAllText(v2).Trim();
                return raw != "max" && long.TryParse(raw, out long max) && max > 0;
            }

            const string v1 = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
            if (File.Exists(v1))
                return long.TryParse(File.ReadAllText(v1).Trim(), out long limit)
                       && limit > 0 && limit < (1L << 62);      // the huge sentinel means unlimited
        }
        catch { /* unreadable: treat the reading as not ours, which is the cautious answer */ }
        return false;
    }

    /// <summary>
    /// Runs <paramref name="callback"/> at the end of each gen2 collection, the pattern the BCL's
    /// own pools use, until it returns false.
    ///
    /// <para>The object is unreachable from the moment it is constructed, so a collection
    /// finalises it; the finaliser resurrects it with <see cref="GC.ReRegisterForFinalize"/>.
    /// After the first couple of collections it lives in gen2, which is what makes this a gen2
    /// callback rather than a gen0 one. The callback runs on the finaliser thread AFTER the
    /// collection, which is also why <see cref="GC.GetGCMemoryInfo"/> read inside it reports the
    /// one that just ended.</para>
    /// </summary>
    public static void OnGen2Collection(Func<bool> callback) => _ = new Gen2GcCallback(callback);

    private sealed class Gen2GcCallback
    {
        private readonly Func<bool> _callback;

        public Gen2GcCallback(Func<bool> callback) => _callback = callback;

        ~Gen2GcCallback()
        {
            bool again;
            // An unhandled exception in a finaliser tears the process down, and nothing here is
            // worth that.
            try { again = _callback(); } catch { again = false; }
            if (again && !Environment.HasShutdownStarted) GC.ReRegisterForFinalize(this);
        }
    }
}
