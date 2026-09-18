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
    /// 512 MB whatever the host had, and is still the ceiling on both terms of the default rule
    /// (see <see cref="IngestArenaFraction"/>).
    /// </summary>
    public const long IngestArenaCapBytes = 512L * 1024 * 1024;

    /// <summary>
    /// The metric hot tier between flushes — <b>exactly today's threshold, restated in bytes</b>:
    /// 500 000 points at the 64 B a scalar point costs in the tier (see
    /// <c>MetricStorageEngine.HotPointBytes</c>). So a host large enough for this cap flushes on
    /// the same cadence it always did, which is what keeps the existing flush tests' point counts
    /// meaning what they say, and a smaller one gets a tier it can hold.
    ///
    /// <para>The unit is the whole point of the change. A 16-bucket histogram point carries its
    /// own <c>long[]</c> and is 4.3x a scalar point, so the flat 500 000-POINT threshold was
    /// 20 MB of gauges or 84 MB of histograms — on a 512 MB container whose GC heap hard limit
    /// is 384 MB, decided without asking the host anything.</para>
    /// </summary>
    public const long MetricHotTierCapBytes = 32L * 1000 * 1000;

    /// <summary>
    /// The trace hot tier between flushes — <b>50 000 spans at what a span in the tier now
    /// weighs</b>, the same shape as <see cref="MetricHotTierCapBytes"/>: today's flush cadence,
    /// restated in the only unit that can bound memory. WP8 is what spends it instead of a span
    /// count. Defined here rather than in the traces package because this file is cut once per
    /// round — see the note on <see cref="MetricHotTierFraction"/>.
    ///
    /// <para><b>Re-calibrated, because the unit moved under it.</b> 64 MB was 50 000 x 1 117 B,
    /// the weight of a <c>SpanRecord</c> that had inflated its attributes into a
    /// <c>Dictionary</c> on the ingest path. WP2, in this same wave, made the tier hold the
    /// msgpack blob and decode lazily, and <c>TraceHotTierProbe</c> measures an eight-attribute
    /// span at <b>540 B retained</b> (166 B/span allocated, 148 B with no attributes at all). At
    /// that weight the old ceiling buys 124 000 spans between flushes, not 50 000 — so a large
    /// host would have silently flushed at 2.5x the cadence every trace test names.</para>
    ///
    /// <para>50 000 x 540 B = 27 MB, which is this. The probe's own gate (a span must retain
    /// under 700 B) is the drift guard beneath it, and <c>MetricBudgetWiringTests</c> pins
    /// cap ÷ 540 B to 50 000 ± 10 % so the next change to a span's weight has to move this
    /// constant instead of the cadence.</para>
    /// </summary>
    public const long TraceHotTierCapBytes = 27L * 1000 * 1000;

    /// <summary>
    /// One trace compaction pass's working set — <c>CompactOnePass</c>'s <c>allSpans</c> at
    /// <c>MaxSpansPerPass</c> = 120 000, at what a span read back out of a segment now weighs.
    ///
    /// <para>128 MB was calibrated on 1 740 B retained a span, which made a full pass
    /// <b>199 MB</b> measured — the figure that OOM'd the 512 MB stand, and the reason the
    /// ceiling was deliberately set BELOW a whole pass. WP2's <c>SpanReader</c> yields the blob,
    /// and <c>TraceCompactionMemoryProbe</c> measures <b>607 B/span retained</b>: a full pass is
    /// 69 MB. So 128 MB no longer bounds anything a pass can do — it would admit 221 000 spans,
    /// 1.8x the pass the planner actually builds.</para>
    ///
    /// <para>120 000 x 607 B = 72.8 MB, rounded UP to 73 MB so that a host large enough for the
    /// cap still affords a whole pass rather than 98 % of one. Pinned to ± 10 % of
    /// <c>MaxSpansPerPass</c> in <c>MetricBudgetWiringTests</c> alongside the tier.</para>
    /// </summary>
    public const long TraceMergeCapBytes = 73L * 1000 * 1000;

    // ── The shares, when that is the smaller number ──
    //
    // THE MANAGED CUT, RE-MADE ONCE FOR THE WHOLE ROUND. These fractions were cut for the log
    // path alone — 0.30 builds + 0.15 cache + 0.10 parked buffers = 0.55 — and then the metric
    // tier, the trace tier and the trace merge pass were appended beside them at 0.05 + 0.05 +
    // 0.06. Six shares of 0.71 leave a 384 MB heap 0.29 of itself for every query, every ASP.NET
    // request, the drainer and the slack the GC needs to collect in at all, which is not a heap
    // that can collect. The logs shares are now 0.25 + 0.12 + 0.05 and the six together claim
    // 0.58, leaving 42 % — clear of the 40 % the stand has to keep back.
    //
    // WHAT WAS CUT, AND WHY IT WAS THESE THREE. The distinction that decides it is peak against
    // resting. The three logs ceilings bound bursts; the three tier ceilings bound a level that
    // a flush resets.
    //   * builds  0.30 -> 0.25 (115 -> 96 MB on the stand). A peak of peaks: the budget is what
    //     ALL concurrent flushes may hold AT ONCE, and StorageEngine derives its flush WIDTH
    //     from it, so a smaller share costs a simultaneous flush rather than a smaller flush.
    //     Which is also the hard floor under this share, and it is closer than it looks: the
    //     stand's 16 MB tier costs 43.75 MB of index-build state, so anything below 0.228 of a
    //     384 MB heap limit takes that host from two concurrent builds to ONE and halves its
    //     flush throughput. 0.25 keeps two with ~10 % to spare (a third would need 0.342), and
    //     StorageEngineBudgetWiringTests is what fails when a later cut forgets this — as this
    //     very re-cut did at 0.22, before that test caught it.
    //   * cache   0.15 -> 0.12 (57 -> 46 MB). The one ceiling here whose loss costs latency and
    //     not correctness, and the only one with a path that already hands its bytes back
    //     (IndexCacheIdleEvict, and the RAM-pressure shed). 46 MB on the stand still sits at the
    //     48 MB that CONFIGURATION.md recommends pinning there by hand.
    //   * buffers 0.10 -> 0.05 (38 -> 19 MB). The furthest of all of them from a resting level:
    //     it bounds what is PARKED between requests and never what is live, so a body over the
    //     pool's reach is still read — just allocated and dropped. What a smaller share changes
    //     is how much of a burst's LOH churn the pool absorbs, and nothing else. Its own floor is
    //     one full set of buckets, ~2 x the 8 MB largest array = 16 MB (IngestBufferPool says so
    //     in those words), which 19 MB clears and 0.04 would not.
    // The three tier shares were left where WP3 put them. Each is a ceiling that TRIGGERS A
    // FLUSH when it fills, so it is a resting level by construction, and cutting one buys a file
    // per metric name per minute — the cost the write-ahead logs exist to avoid — rather than
    // memory.
    //
    // Nothing on a host with room moves, because every one of the six is min(cap, share) and the
    // caps bind well below the sizes that matter: the first fraction to stop binding is the
    // largest, and 640 MB of a 16 GB heap limit is 3.9 %. Asserted at 16 GB and 64 GB in
    // MemoryBudgetTests, since "the re-cut is free above the stand" is the claim that makes it
    // safe to make at all.
    //
    // Native tiers take 25 % of the physical limit; the rest of the process's native memory is
    // the runtime itself (~60-90 MB of JIT'd code and runtime data on a self-contained build),
    // the ingest ring, the live hot tier and the WAL mapping — and the managed heap, which sits
    // inside the same container. In a 512 MB container that is 96 + 46 MB managed and 128 MB
    // native: 270 MB, 53 % of the container -- not counting the ingest arena, whose default is
    // floored at 8 192 slabs rather than taken as a share and can reach 512 MB by itself (see
    // IngestArenaFraction). Native is the largest single share because a frozen tier is bytes
    // already written that cannot be given back until its cold segment is; the index cache is
    // smaller because losing it costs latency, not correctness.

    /// <summary>Share of the PHYSICAL limit the frozen-tier backlog may hold.</summary>
    public const double NativeTierFraction = 0.25;

    /// <summary>
    /// Share of the MANAGED-HEAP limit concurrent index builds may hold. <b>0.30 until the
    /// managed cut was re-made</b> — see the note above: this is a peak across all flushes at
    /// once, and the flush width derives from it, so the 5 points came out of how many flushes a
    /// constrained host runs side by side rather than out of any one of them. It cannot go much
    /// further: below 0.228 the 512 MB stand drops from two concurrent builds to one.
    /// </summary>
    public const double ManagedBuildFraction = 0.25;

    /// <summary>
    /// Share of the MANAGED-HEAP limit the segment-index cache may hold. <b>0.15 until the
    /// managed cut was re-made</b>: the cache is the one managed ceiling whose loss costs
    /// latency rather than correctness, and the only one that already gives bytes back on its
    /// own (idle eviction, and the RAM-pressure shed).
    /// </summary>
    public const double IndexCacheFraction = 0.12;

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
    /// asked for. The native part stays bounded twice over — it is a part of the total budget, so
    /// it can never exceed it, and <see cref="IndexCacheNativeMaxFraction"/> bounds it by the host
    /// as well.</para>
    /// </summary>
    public const double IndexCacheNativeEntryShare = 0.20;

    /// <summary>
    /// The most of the PHYSICAL limit those bloom bits may be allowed to pin, however large
    /// <c>Query.IndexCacheBytes</c> is set: the clamp on <see cref="IndexCacheNativeEntryShare"/>.
    ///
    /// <para>A configured budget says how much memory this component may hold; only the host says
    /// how much of it may sit where no collection can reach it. Scaling with no reference to the
    /// host let the managed knob move native bytes without bound — in a 512 MB container a 1 GB
    /// budget asked for 204 MB of bloom bits, 40 % of the box, outside the GC's hard limit and
    /// unreclaimable by the RAM pressure path, which is the class of defect
    /// <see cref="IndexCacheNativeCapBytes"/> exists to prevent.</para>
    ///
    /// <para>Twice the derived backstop, it holds the tiers' 25 % and these bits' 10 % to 35 % of
    /// the physical limit in the worst case. That bounds these two, not the process's native
    /// memory: the ingest arena is no longer a share beside them. Its default is floored at 8 192
    /// slabs (see <see cref="IngestArenaFraction"/>), so at the 64 KB default slab its worst case
    /// is 512 MB, the whole of a 512 MB container on its own: committed on Windows as soon as the
    /// ring has been that deep, resident on Linux only if those events were near the maximum
    /// size. It is a clamp and never a floor: it can only lower a scaled ceiling, never cut into
    /// the backstop a host that configured nothing gets.</para>
    /// </summary>
    public const double IndexCacheNativeMaxFraction = 0.10;

    /// <summary>
    /// Share of the MANAGED-HEAP limit the ingest body-buffer pool may park. Request bodies are
    /// managed <c>byte[]</c> on the large object heap, so this is a share of the GC's limit like
    /// the two above. It bounds what is PARKED, never what is live: a body larger than the pool
    /// will serve is still read, just allocated and dropped rather than kept.
    ///
    /// <para><b>0.10 until the managed cut was re-made.</b> That last sentence is why this share
    /// gave up the largest proportion of itself: what it buys is how much of a burst's LOH churn
    /// the pool absorbs, and a request that outruns it still succeeds. Its floor is one full set
    /// of buckets — about twice the 8 MB largest array, 16 MB — which 0.05 of a 384 MB heap limit
    /// clears at 19 MB.</para>
    /// </summary>
    public const double IngestBufferFraction = 0.05;

    /// <summary>
    /// Share of the PHYSICAL limit the ingest payload arena may reserve — native, like the frozen
    /// tiers, and not under the GC's hard limit. ONE of the two terms of the arena's default, not
    /// the whole of it.
    ///
    /// <para><b>The rule</b> (applied by <c>IngestionOptions.DefaultPayloadPoolBytesFor</c>, which
    /// knows the slab size this class does not): the default arena is the larger of this share,
    /// <c>min(512 MB, 15 %)</c>, and a floor of 8 192 slabs of <c>MaxEventPayloadBytes</c> capped
    /// at <see cref="IngestArenaCapBytes"/>. At the 64 KB default slab the floor is 512 MB, so this
    /// share only sets the size for a lowered slab size on a host where 15 % is more than 8 192
    /// slabs.</para>
    ///
    /// <para><b>Why the floor overrides the share.</b> The share alone gave a 512 MB container
    /// ~76 MB, about 1 200 slabs. A pending event holds a slab whatever its size, and an
    /// OpenTelemetry collector sends 8 192 records a batch by default, so ordinary batches dropped
    /// part way through for want of slabs. That was a real drop at normal load, traded for a
    /// theoretical residency bound.</para>
    ///
    /// <para><b>The residency trade, honestly, and it differs by platform.</b> What the arena
    /// takes is never given back, so its high-water mark is a resting level, not a peak.</para>
    /// <list type="bullet">
    /// <item><b>Linux</b> pages it lazily, so the cost is per touched page, not per slab: a
    /// typical 0.3-2 KB event touches one 4 KB page at the start of its 64 KB slab, and 8 192
    /// slabs of small events rest at about 32 MB. The page stays 4 KB because the arena opts out
    /// of transparent huge pages (<c>MADV_NOHUGEPAGE</c>); under <c>transparent_hugepage=always</c>
    /// without that, each 2 MB range the burst touched could be resident whole, most of 512 MB.
    /// Only events near the maximum size fill their slabs, and that worst case, 512 MB, is the one
    /// the flat default always had.</item>
    /// <item><b>Windows</b> commits it in 1 MB chunks up to the deepest slab ever handed out and
    /// never decommits, whatever the events weigh: one batch that outruns the drainer by ~8 192
    /// events commits ~512 MB even at 300 B an event. The ~32 MB figure is working set there, not
    /// commit, and a job object's memory limit counts commit.</item>
    /// </list>
    /// <para>A host under a container or job memory limit, on either platform, or one that expects
    /// large events, sets <c>Ingestion.PayloadPoolBytes</c> explicitly for a hard ceiling, which
    /// always wins: the Linux ~32 MB is what small events cost, not a bound.</para>
    /// </summary>
    public const double IngestArenaFraction = 0.15;

    /// <summary>
    /// Share of the MANAGED-HEAP limit the metric hot tier may hold between flushes.
    ///
    /// <para><b>Why the metric tier is here at all.</b> Nothing in <c>Ameto.Metrics</c> consulted
    /// this class: every sizing constant was a literal, identical on a 512 MB container and a
    /// 64 GB host, and the tier could legally claim more than the LOG tier is allowed
    /// (<c>HotTier.MaxSizeBytes</c> 16 MB on the stand) while the index cache held 48 MB beside
    /// it.</para>
    ///
    /// <para><b>The re-cut this append owed has been made.</b> The three tier shares went in
    /// beside logs shares of 0.30 + 0.15 + 0.10, for 0.71 of the managed limit; the logs shares
    /// are now 0.25 + 0.12 + 0.05 and the six total 0.58. What paid for the tiers was 5 points
    /// of the index-build peak, 3 of the index cache and 5 of the parked ingest buffers — three
    /// ceilings that bound a burst, where a tier ceiling is a flush trigger and therefore a
    /// resting level. The reasoning is set out in full above the fractions, including the floor
    /// each one stopped at; <c>MemoryBudgetTests</c> spells the new literals and asserts that
    /// nothing on a large host moved, and <c>MetricBudgetWiringTests</c> holds the sum at 0.58
    /// so the next share has to come out of one of these rather than out of the heap's
    /// slack.</para>
    /// </summary>
    public const double MetricHotTierFraction = 0.05;

    /// <summary>
    /// Share of the MANAGED-HEAP limit the trace hot tier may hold between flushes. Consumed by
    /// WP8, defined here because this file gets exactly one owner per round.
    /// </summary>
    public const double TraceHotTierFraction = 0.05;

    /// <summary>
    /// Share of the MANAGED-HEAP limit one trace compaction pass may hold. Larger than a tier's
    /// share because a pass reads whole segments back; still a fraction, because 199 MB of it on
    /// a 384 MB heap limit is how the traces OOM happened.
    /// </summary>
    public const double TraceMergeFraction = 0.06;

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

    /// <summary>
    /// 4 MB is ~62 500 scalar points, and a tier that cannot hold a minute of a small exporter
    /// writes a file per metric name per minute instead — the cost the write-ahead log exists to
    /// avoid. A host too small for this floor has a file-count problem, not a memory one.
    /// </summary>
    private const long MinMetricHotTierBytes    =  4L * 1000 * 1000;
    private const long MinTraceHotTierBytes     =  8L * 1024 * 1024;
    private const long MinTraceMergeBytes       = 16L * 1024 * 1024;

    private MemoryBudgets(
        long managedLimit, long physicalLimit, long managed, long native, long indexCache,
        long indexCacheNative, long ingestBuffers, long ingestArena,
        long metricHotTier, long traceHotTier, long traceMerge)
    {
        ManagedLimitBytes     = managedLimit;
        PhysicalLimitBytes    = physicalLimit;
        ManagedBuildBytes     = managed;
        NativeTierBytes       = native;
        IndexCacheBytes       = indexCache;
        IndexCacheNativeBytes = indexCacheNative;
        IngestBufferBytes     = ingestBuffers;
        IngestArenaBytes      = ingestArena;
        MetricHotTierBytes    = metricHotTier;
        TraceHotTierBytes     = traceHotTier;
        TraceMergeBytes       = traceMerge;
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

    /// <summary>
    /// The byte-share term of the ingest payload arena's default. The arena's actual default also
    /// has a slab-count floor, applied where the slab size is known:
    /// <c>IngestionOptions.DefaultPayloadPoolBytesFor</c>. See <see cref="IngestArenaFraction"/>.
    ///
    /// <para>This term decides the default only when the slab is smaller than 64 KB. It is at
    /// most 512 MB, and the floor is 8 192 slabs capped at 512 MB, which is exactly 512 MB from a
    /// 64 KB slab up; so at the default slab size, or above it, the floor wins on every host and
    /// this figure is not the arena's size. Below 64 KB it wins only where 15 % of the physical
    /// limit exceeds 8 192 slabs.</para>
    /// </summary>
    public long IngestArenaBytes { get; }

    /// <summary>
    /// Ceiling on the metric hot tier between flushes — the budget
    /// <c>MetricsOptions.EffectiveHotTierBytes</c> spends. See
    /// <see cref="MetricHotTierFraction"/>.
    /// </summary>
    public long MetricHotTierBytes { get; }

    /// <summary>Ceiling on the trace hot tier between flushes. Consumed by WP8.</summary>
    public long TraceHotTierBytes { get; }

    /// <summary>Ceiling on one trace compaction pass's working set. Consumed by WP8.</summary>
    public long TraceMergeBytes { get; }

    /// <summary>
    /// True when a share of a limit, not the constant, set a ceiling — what
    /// <c>StorageEngine</c> prints beside the budgets at startup as "host-constrained" rather
    /// than "fixed ceilings".
    ///
    /// <para><b>It has to name every budget a host can cut, and it named three of nine.</b> The
    /// metric tier, the trace tier and the trace merge pass were added to this struct without
    /// being added here. <b>That did not give a wrong answer, and the reason is a coincidence
    /// worth removing:</b> every managed share is taken of the same base, so the budget cut
    /// FIRST and capped LAST is whichever cap is the largest multiple of its own fraction — the
    /// index builds, at 640 MB / 0.25 = 2 560 MB of managed limit, against 610 MB for the metric
    /// tier, 515 MB for the trace tier and 1 160 MB for the merge pass. Any host small enough to
    /// have a tier cut therefore had its build budget cut too, and the three-term form agreed
    /// with this one on every host that exists. It was one fraction change away from not, on a
    /// line whose reader is an operator asking why this install behaves unlike the last one.</para>
    ///
    /// <para>The ingest buffer pool and the payload arena are deliberately still absent: the
    /// arena's default is not this struct's figure at all (a slab-count floor usually decides
    /// it — see <see cref="IngestArenaFraction"/>), and the pool bounds what is PARKED rather
    /// than any ceiling a flush runs into.</para>
    /// </summary>
    public bool IsConstrained =>
        ManagedBuildBytes  < ManagedBuildCapBytes  ||
        NativeTierBytes    < NativeTierCapBytes    ||
        IndexCacheBytes    < IndexCacheCapBytes    ||
        MetricHotTierBytes < MetricHotTierCapBytes ||
        TraceHotTierBytes  < TraceHotTierCapBytes  ||
        TraceMergeBytes    < TraceMergeCapBytes;

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
            Share(physicalBase, IngestArenaFraction,  IngestArenaCapBytes,  MinIngestArenaBytes),
            Share(managedBase,  MetricHotTierFraction, MetricHotTierCapBytes, MinMetricHotTierBytes),
            Share(managedBase,  TraceHotTierFraction,  TraceHotTierCapBytes,  MinTraceHotTierBytes),
            Share(managedBase,  TraceMergeFraction,    TraceMergeCapBytes,    MinTraceMergeBytes));

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
