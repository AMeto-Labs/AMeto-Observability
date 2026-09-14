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
/// <para>Each ceiling is now <c>min(the old constant, a fraction of available memory)</c>, so a
/// large host behaves exactly as before and a small one gets budgets it can honour.
/// <see cref="GC.GetGCMemoryInfo()"/> reports the cgroup limit when there is one, so this reads
/// the container's memory rather than the machine's.</para>
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

    // ── The shares of available memory, when that is the smaller number ──
    //
    // They sum to 70 %, which is not an accident and not a coincidence either: the remaining
    // 30 % is the runtime itself (~60-90 MB of JIT'd code and runtime data on a self-contained
    // build), the ingest ring, the live hot tier, the WAL mapping, and the slack the GC needs
    // to collect at all. Native is the largest share because a frozen tier is bytes already
    // written that cannot be given back until its cold segment is; the index cache is the
    // smallest because losing it costs latency, not correctness.

    /// <summary>Share of available memory the frozen-tier backlog may hold.</summary>
    public const double NativeTierFraction = 0.25;

    /// <summary>Share of available memory concurrent index builds may hold.</summary>
    public const double ManagedBuildFraction = 0.30;

    /// <summary>Share of available memory the segment-index cache may hold.</summary>
    public const double IndexCacheFraction = 0.15;

    // ── Floors, so a pathologically small limit still yields a working engine ──

    private const long MinBuildBytes      = 16L * 1024 * 1024;
    private const long MinNativeBytes     = 16L * 1024 * 1024;
    private const long MinIndexCacheBytes =  8L * 1024 * 1024;

    private MemoryBudgets(long available, long managed, long native, long indexCache)
    {
        AvailableBytes    = available;
        ManagedBuildBytes = managed;
        NativeTierBytes   = native;
        IndexCacheBytes   = indexCache;
    }

    /// <summary>Memory this process may use — the cgroup limit under a container, else host RAM.</summary>
    public long AvailableBytes { get; }

    /// <summary>Ceiling on managed index-build state across all concurrent flushes.</summary>
    public long ManagedBuildBytes { get; }

    /// <summary>Ceiling on native memory held by frozen tiers awaiting persistence.</summary>
    public long NativeTierBytes { get; }

    /// <summary>Default budget for the cross-query segment-index cache.</summary>
    public long IndexCacheBytes { get; }

    /// <summary>True when a share of available memory, not the constant, set a ceiling.</summary>
    public bool IsConstrained =>
        ManagedBuildBytes < ManagedBuildCapBytes ||
        NativeTierBytes   < NativeTierCapBytes   ||
        IndexCacheBytes   < IndexCacheCapBytes;

    /// <summary>
    /// Pure function over an available-memory figure, so the arithmetic can be tested at
    /// 512 MB, 4 GB and 64 GB without a machine of each size. A non-positive figure means the
    /// runtime could not tell us, and falls back to the constants — the pre-existing behaviour,
    /// which is the right answer when the alternative is guessing low and refusing work.
    /// </summary>
    public static MemoryBudgets Derive(long availableBytes)
    {
        if (availableBytes <= 0)
            return new MemoryBudgets(0, ManagedBuildCapBytes, NativeTierCapBytes, IndexCacheCapBytes);

        return new MemoryBudgets(
            availableBytes,
            Share(availableBytes, ManagedBuildFraction, ManagedBuildCapBytes, MinBuildBytes),
            Share(availableBytes, NativeTierFraction,   NativeTierCapBytes,   MinNativeBytes),
            Share(availableBytes, IndexCacheFraction,   IndexCacheCapBytes,   MinIndexCacheBytes));

        static long Share(long available, double fraction, long cap, long floor)
        {
            long share = (long)(available * fraction);
            return Math.Max(floor, Math.Min(cap, share));
        }
    }

    /// <summary>Budgets for the memory THIS process may use right now.</summary>
    public static MemoryBudgets Current() => Derive(GC.GetGCMemoryInfo().TotalAvailableMemoryBytes);
}
