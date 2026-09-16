namespace Ameto.Core;

/// <summary>
/// The three RAM ceilings the log path sizes itself against, derived from how much memory
/// this process may actually use.
///
/// <para>They used to be flat constants — 640 MB of concurrent index builds, 512 MB of frozen
/// tiers, a 256 MB index cache — chosen for a machine with room for them. Nothing consulted the
/// host, so on the 512 MB console stand the engine could legally hold 504 MB of frozen tiers
/// plus two 184 MB builds plus a full index cache: over 1 GB of intent on a host with half of
/// that, and the runtime's own managed hard limit (75 % = 384 MB) sits underneath. The failure
/// mode was an OOM kill, which looks like a crash rather than a capacity problem.</para>
///
/// <para>Each ceiling is now <c>min(the old constant, a fraction of a limit)</c>, so a large host
/// behaves exactly as before and a small one gets budgets it can honour.</para>
///
/// <para><b>Two limits, chosen by where the bytes live.</b> A process has two different memory
/// ceilings, and they are not the same number:</para>
/// <list type="bullet">
/// <item><b>The managed-heap limit</b> — <see cref="GCMemoryInfo.TotalAvailableMemoryBytes"/>.
/// Under a container (cgroup on Linux, job object on Windows) the GC sets a heap hard limit of
/// 75 % of the container limit, and this reports THAT, not the container limit: 384 MB in a
/// 512 MB container. An explicit <c>GCHeapHardLimit</c> / <c>GCHeapHardLimitPercent</c> is
/// reported the same way. Index builds and the segment-index cache (expanded postings and
/// dictionaries — managed objects, with a small native bloom share) live on the GC heap, so they
/// are shares of this.</item>
/// <item><b>The physical limit</b> — the container limit, or host RAM without one. Frozen tiers
/// are <c>NativeMemory</c> and are not under the GC hard limit at all, so their share is of this.
/// The runtime does not expose it directly; it is recovered from
/// <see cref="GCMemoryInfo.HighMemoryLoadThresholdBytes"/>, which the GC computes as
/// <c>GCHighMemPercent</c> of exactly that figure (measured: 460 MB = 90 % of a 512 MB job
/// limit, and 90 % of host RAM under a 512 MB <c>GCHeapHardLimit</c>).</item>
/// </list>
/// <para>Using one base for all three — the managed limit — under-provisioned the native budget by
/// a quarter in every container (96 MB instead of 128 MB at 512 MB) and shrank it on a big host
/// that merely capped its managed heap.</para>
///
/// <para>The trade on a tiny host is deliberate: fewer flush slots mean the ingest ring applies
/// back-pressure earlier under a burst, so events are dropped at the door with a counted reason
/// instead of the process being killed with everything in it.</para>
/// </summary>
public readonly struct MemoryBudgets
{
    // ── The absolute ceilings: what the budgets were before, and still are on a big host ──

    /// <summary>Managed index-build state across all concurrent flushes.</summary>
    public const long ManagedBuildCapBytes = 640L * 1024 * 1024;

    /// <summary>Native memory held by frozen-but-not-yet-persisted tiers.</summary>
    public const long NativeTierCapBytes = 512L * 1024 * 1024;

    /// <summary>Cross-query cache of decoded segment indexes.</summary>
    public const long IndexCacheCapBytes = 256L * 1024 * 1024;

    /// <summary>
    /// The NATIVE part of that cache — the segment bloom filters' bits, which are
    /// <c>NativeMemory</c> and therefore sit outside the GC's hard limit entirely.
    ///
    /// <para>A backstop, not a working budget: it is sized so that nothing realistic reaches it,
    /// and so that a pathological mix cannot push native bytes past the limit the rest of the
    /// engine is sized against.</para>
    ///
    /// <para>The figure that sizes it is bloom's share of a cache ENTRY, which is not its share of
    /// the packed sections: the cache charges <c>ApproxRetainedBytes</c>, in which the inverted and
    /// trigram halves are decoded structures 3-4x their sections while these bits are the same
    /// bytes decoded as on disk. Measured by the repo's own <c>BloomSizingProbe</c>, bloom is 4.1 %
    /// of a prop-dense group's entry and 8.3 % of a thin one's — their 15.6 % and 26.6 % of
    /// SECTIONS is the query prefilter's phase-split figure and belongs to that argument alone. At
    /// the worst of those, a full 256 MB cache holds ~21 MB of bloom, so this ceiling first binds
    /// at a configured cache of roughly 1.2 GB.</para>
    /// </summary>
    public const long IndexCacheNativeCapBytes = 96L * 1024 * 1024;

    /// <summary>
    /// Request bodies parked in <c>IngestBufferPool</c> between requests.
    ///
    /// <para>This one has no "what it was before": the pool sized itself from
    /// <c>2 x ProcessorCount</c> alone, on the argument that the containers which cannot afford
    /// the memory are the ones with few cores. That holds only under a CPU quota, and this
    /// project's own deployments set a memory limit and no CPU limit — so a 512 MB container on
    /// a 16-core host took the 32-deep ceiling, and its arithmetic (a full set of buckets is
    /// ~2 x the largest array, so depth x 16 MB) allowed more than the whole container.</para>
    /// </summary>
    public const long IngestBufferCapBytes = 128L * 1024 * 1024;

    /// <summary>
    /// The ingest payload arena — <c>Ingestion.PayloadPoolBytes</c>'s default, which was a flat
    /// 512 MB whatever the host had.
    /// </summary>
    public const long IngestArenaCapBytes = 512L * 1024 * 1024;

    // ── The shares, when that is the smaller number ──
    //
    // Managed builds and the index cache together take 45 % of the managed-heap limit; the rest
    // of the heap is queries, ASP.NET, the drainer and the slack the GC needs to collect at all.
    // Native tiers take 25 % of the physical limit; the rest of the process's native memory is
    // the runtime itself (~60-90 MB of JIT'd code and runtime data on a self-contained build),
    // the ingest ring, the live hot tier and the WAL mapping — and the managed heap, which sits
    // inside the same container. In a 512 MB container that is 115 + 57 MB managed and 128 MB
    // native: 300 MB, 59 % of the container. Native is the largest single share because a frozen
    // tier is bytes already written that cannot be given back until its cold segment is; the
    // index cache is the smallest because losing it costs latency, not correctness.

    /// <summary>Share of the PHYSICAL limit the frozen-tier backlog may hold.</summary>
    public const double NativeTierFraction = 0.25;

    /// <summary>Share of the MANAGED-HEAP limit concurrent index builds may hold.</summary>
    public const double ManagedBuildFraction = 0.30;

    /// <summary>Share of the MANAGED-HEAP limit the segment-index cache may hold.</summary>
    public const double IndexCacheFraction = 0.15;

    /// <summary>
    /// Share of the PHYSICAL limit the segment-index cache's NATIVE bloom bits may hold.
    ///
    /// <para>Of the physical limit, like the frozen tiers and the ingest arena, because that is
    /// where these bytes actually live: <see cref="IndexCacheFraction"/> is a share of the GC's
    /// hard limit, and charging native allocations against it let the cache spend managed
    /// headroom on memory the GC never sees — on a 512 MB stand, in the one component the RAM
    /// pressure path could not reclaim. Five percent is 25.6 MB in that container, where the worst
    /// measured bloom share of an entry (8.3 %) puts a full 57 MB cache at ~4.7 MB of native bits:
    /// room to spare, which is what a backstop is for.</para>
    /// </summary>
    public const double IndexCacheNativeFraction = 0.05;

    /// <summary>
    /// The largest share of a cache ENTRY the bloom bits have been measured at, with headroom —
    /// the factor an explicitly configured <c>Query.IndexCacheBytes</c> scales its native ceiling
    /// by, so that raising the budget cannot silently cap the cache on its native share.
    ///
    /// <para><c>BloomSizingProbe</c> measures 4.1 % of a prop-dense entry and 8.3 % of a thin one.
    /// This sits well above both on purpose: it bounds a BUDGET rather than describing a file, and
    /// a shape nobody has measured must not be the thing that caps a cache an operator deliberately
    /// asked for. The native part stays bounded either way — it is a part of the total budget, so
    /// it can never exceed it.</para>
    /// </summary>
    public const double IndexCacheNativeEntryShare = 0.20;

    /// <summary>
    /// Share of the MANAGED-HEAP limit the ingest body-buffer pool may park. Request bodies are
    /// managed <c>byte[]</c> on the large object heap, so this is a share of the GC's limit like
    /// the two above. It bounds what is PARKED, never what is live: a body larger than the pool
    /// will serve is still read, just allocated and dropped rather than kept.
    /// </summary>
    public const double IngestBufferFraction = 0.10;

    /// <summary>
    /// Share of the PHYSICAL limit the ingest payload arena may reserve — native, like the frozen
    /// tiers, and not under the GC's hard limit.
    ///
    /// <para>The arena is the ring's absorption window: the pages it touches are never given
    /// back, so its high-water mark is a resting level, not a peak. At the flat 512 MB default
    /// that is a bound larger than the whole of a 512 MB container, which is why this is a share
    /// — the trade being the one this class already documents, that a small host applies
    /// back-pressure earlier and drops at the door with a counted reason instead of being killed
    /// with everything in it.</para>
    /// </summary>
    public const double IngestArenaFraction = 0.15;

    /// <summary>
    /// A guard for a runtime that does not report <c>GCHighMemPercent</c>. The .NET 10 runtime
    /// reports the EFFECTIVE percentage, whether configured or chosen by default, including the
    /// higher default at 80 GB of physical memory or more. Measured on 10.0.11: 90 with nothing set,
    /// 70 with <c>GCHighMemPercent=46</c> (hex), and 95 at 100 GB and at 512 GB physical. So this
    /// value is not used today. It is the GC's default below 80 GB.
    /// </summary>
    private const int DefaultHighMemoryLoadPercent = 90;

    // ── Floors, so a pathologically small limit still yields a working engine ──

    private const long MinBuildBytes            = 16L * 1024 * 1024;
    private const long MinNativeBytes           = 16L * 1024 * 1024;
    private const long MinIndexCacheBytes       =  8L * 1024 * 1024;
    private const long MinIndexCacheNativeBytes =  4L * 1024 * 1024;
    private const long MinIngestBufferBytes     =  8L * 1024 * 1024;
    private const long MinIngestArenaBytes      = 16L * 1024 * 1024;   // ~256 slabs at the 64 KB default

    private MemoryBudgets(
        long managedLimit, long physicalLimit, long managed, long native, long indexCache,
        long indexCacheNative, long ingestBuffers, long ingestArena)
    {
        ManagedLimitBytes     = managedLimit;
        PhysicalLimitBytes    = physicalLimit;
        ManagedBuildBytes     = managed;
        NativeTierBytes       = native;
        IndexCacheBytes       = indexCache;
        IndexCacheNativeBytes = indexCacheNative;
        IngestBufferBytes     = ingestBuffers;
        IngestArenaBytes      = ingestArena;
    }

    /// <summary>
    /// The managed-heap limit the managed shares were taken of: the GC hard limit when there is
    /// one (75 % of a container limit by default), else physical memory. 0 when unknown.
    /// </summary>
    public long ManagedLimitBytes { get; }

    /// <summary>
    /// The physical limit the native share was taken of: the container limit, else host RAM.
    /// 0 when unknown.
    /// </summary>
    public long PhysicalLimitBytes { get; }

    /// <summary>Ceiling on managed index-build state across all concurrent flushes.</summary>
    public long ManagedBuildBytes { get; }

    /// <summary>Ceiling on native memory held by frozen tiers awaiting persistence.</summary>
    public long NativeTierBytes { get; }

    /// <summary>Default budget for the cross-query segment-index cache.</summary>
    public long IndexCacheBytes { get; }

    /// <summary>
    /// Ceiling on the NATIVE part of that cache (bloom bits), taken of the physical limit
    /// because that is where those bytes live. See <see cref="IndexCacheNativeFraction"/>.
    /// </summary>
    public long IndexCacheNativeBytes { get; }

    /// <summary>Ceiling on request bodies parked in the ingest buffer pool between requests.</summary>
    public long IngestBufferBytes { get; }

    /// <summary>Default size of the ingest payload arena (the ring's slab budget).</summary>
    public long IngestArenaBytes { get; }

    /// <summary>True when a share of a limit, not the constant, set a ceiling.</summary>
    public bool IsConstrained =>
        ManagedBuildBytes < ManagedBuildCapBytes ||
        NativeTierBytes   < NativeTierCapBytes   ||
        IndexCacheBytes   < IndexCacheCapBytes;

    /// <summary>
    /// One figure for both limits — what a process with no GC hard limit sees, where the heap
    /// may grow to physical memory. Kept for callers that only have one number.
    /// </summary>
    public static MemoryBudgets Derive(long availableBytes) => Derive(availableBytes, availableBytes);

    /// <summary>
    /// Pure function over the two limits, so the arithmetic can be tested at 512 MB, 4 GB and
    /// 64 GB without a machine of each size. A non-positive figure means the runtime could not
    /// tell us, and falls back to the constants for the shares taken of it — the pre-existing
    /// behaviour, which is the right answer when the alternative is guessing low and refusing
    /// work. An unknown physical limit borrows the managed one if that is known.
    /// </summary>
    public static MemoryBudgets Derive(long managedLimitBytes, long physicalLimitBytes)
    {
        long managedBase  = managedLimitBytes  > 0 ? managedLimitBytes  : 0;
        long physicalBase = physicalLimitBytes > 0 ? physicalLimitBytes : managedBase;

        return new MemoryBudgets(
            managedBase,
            physicalBase,
            Share(managedBase,  ManagedBuildFraction, ManagedBuildCapBytes, MinBuildBytes),
            Share(physicalBase, NativeTierFraction,   NativeTierCapBytes,   MinNativeBytes),
            Share(managedBase,  IndexCacheFraction,   IndexCacheCapBytes,   MinIndexCacheBytes),
            Share(physicalBase, IndexCacheNativeFraction, IndexCacheNativeCapBytes, MinIndexCacheNativeBytes),
            Share(managedBase,  IngestBufferFraction, IngestBufferCapBytes, MinIngestBufferBytes),
            Share(physicalBase, IngestArenaFraction,  IngestArenaCapBytes,  MinIngestArenaBytes));

        static long Share(long limit, double fraction, long cap, long floor)
        {
            if (limit <= 0) return cap;
            long share = (long)(limit * fraction);
            return Math.Max(floor, Math.Min(cap, share));
        }
    }

    /// <summary>
    /// Recovers the physical limit (container limit, else host RAM) from the GC's high-memory-load
    /// threshold, which the GC computes as <paramref name="highMemoryLoadPercent"/> of it. Rounded
    /// to a 4 KiB page, since the GC's own multiply truncates. Falls back to
    /// <paramref name="managedLimitBytes"/> when either input is unusable.
    /// </summary>
    public static long PhysicalLimitFrom(long highMemoryLoadThresholdBytes, int highMemoryLoadPercent, long managedLimitBytes)
    {
        if (highMemoryLoadThresholdBytes <= 0 || highMemoryLoadPercent is <= 0 or > 100)
            return managedLimitBytes;

        long raw = (long)Math.Round(highMemoryLoadThresholdBytes * 100.0 / highMemoryLoadPercent);
        return (raw + 2048) & ~4095L;
    }

    /// <summary>
    /// Budgets for THIS process right now. Allocates (the GC's configuration dictionary), so it is
    /// for startup and configuration paths, not for anything polled.
    /// </summary>
    public static MemoryBudgets Current()
    {
        var  info    = GC.GetGCMemoryInfo();
        long managed = info.TotalAvailableMemoryBytes;
        return Derive(managed, PhysicalLimitFrom(info.HighMemoryLoadThresholdBytes, HighMemoryLoadPercent(), managed));
    }

    /// <summary>The GC's effective high-memory-load percentage (configured or default).</summary>
    private static int HighMemoryLoadPercent()
    {
        try
        {
            if (GC.GetConfigurationVariables().TryGetValue("GCHighMemPercent", out object? v))
            {
                long pct = v switch
                {
                    long  l => l,
                    int   i => i,
                    ulong u => (long)Math.Min(u, 100UL),
                    _       => 0,
                };
                if (pct is > 0 and <= 100) return (int)pct;
            }
        }
        catch { /* a runtime that cannot say falls back to the default */ }
        return DefaultHighMemoryLoadPercent;
    }
}
