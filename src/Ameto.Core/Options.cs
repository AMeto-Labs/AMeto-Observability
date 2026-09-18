namespace Ameto.Core;

/// <summary>
/// Retention rules per log level.
/// </summary>
public sealed class RetentionPolicy
{
    private readonly Dictionary<LogLevel, TimeSpan> _rules;

    public static RetentionPolicy Default { get; } = new(new Dictionary<LogLevel, TimeSpan>
    {
        [LogLevel.Verbose]     = TimeSpan.FromDays(90),
        [LogLevel.Debug]       = TimeSpan.FromDays(3),
        [LogLevel.Information] = TimeSpan.FromDays(90),
        [LogLevel.Warning]     = TimeSpan.FromDays(90),
        [LogLevel.Error]       = TimeSpan.FromDays(90),
        [LogLevel.Fatal]       = TimeSpan.FromDays(90),
    });

    public RetentionPolicy(Dictionary<LogLevel, TimeSpan> rules)
    {
        _rules = rules;
    }

    public TimeSpan GetTtl(LogLevel level) =>
        _rules.TryGetValue(level, out var ttl) ? ttl : TimeSpan.FromDays(90);
}

/// <summary>
/// Hot-tier flush configuration.
/// </summary>
public sealed class HotTierOptions
{
    /// <summary>
    /// Maximum size of the hot-tier in bytes before a flush is triggered. Smaller tiers
    /// mean smaller frozen tiers held in RAM while their cold segment is being written,
    /// so the parallel-flush backlog (see StorageEngine) can be deeper for the same memory
    /// ceiling — smoother back-pressure with fewer drops under bursty ingest.
    /// </summary>
    public long MaxSizeBytes { get; init; } = 64 * 1024 * 1024; // 64 MB

    /// <summary>Maximum age of events in the hot-tier before a flush is triggered.</summary>
    public TimeSpan MaxAge   { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// How often the live WAL is msync'd to disk. This is the durability window for
    /// acknowledged events: a power loss forfeits at most this much accepted ingest
    /// (a process crash forfeits nothing — the page cache survives it). Zero or
    /// negative disables the periodic msync, restoring page-cache-only durability.
    /// </summary>
    public TimeSpan WalFlushInterval { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Number of cold-segment flushes (index build + compress + write) allowed to run in
    /// parallel. Higher = more flush throughput (fewer ingest drops under burst) but more
    /// peak RAM (concurrent index builds). 0 = auto (≈ processor count / 2, capped 2–8).
    /// Tune down on memory-constrained hosts, up on many-core hosts chasing throughput.
    /// </summary>
    public int FlushConcurrency { get; init; } = 0;
}

/// <summary>
/// Server log-file configuration.
///
/// Running as a Windows Service there is no console, and the Event Log provider
/// installed by <c>AddWindowsService</c> enforces its own <c>Warning</c> minimum —
/// so without a file sink every Information-level diagnostic is silently discarded
/// on exactly the deployment that most needs it.
/// </summary>
public sealed class LoggingOptions
{
    /// <summary>Write a rolling log file under <c>{DataDirectory}/logs</c>. Default: on.</summary>
    public bool FileEnabled { get; init; } = true;

    /// <summary>
    /// Minimum level for the file sink — one of Trace/Debug/Information/Warning/Error/Critical.
    /// Information keeps the periodic memory-attribution line and flush/merge progress, which
    /// is what any RAM or CPU investigation starts from. A string rather than an enum because
    /// Ameto.Core deliberately carries no Microsoft.Extensions.Logging reference (and its own
    /// <see cref="LogLevel"/> is the CLEF event vocabulary, not the host's).
    /// </summary>
    public string FileMinimumLevel { get; init; } = "Information";

    /// <summary>Daily files retained before the oldest are pruned. Default: 7.</summary>
    public int FileRetainDays { get; init; } = 7;
}

/// <summary>
/// Indexing configuration (segment flush-time index building).
/// </summary>
public sealed class IndexingOptions
{
    /// <summary>
    /// Maximum depth when recursively flattening nested structured properties.
    /// Prevents index explosion for deeply nested objects.
    /// Default: 5. Set to 0 to disable nested flattening (only top-level keys indexed).
    /// </summary>
    public int MaxPropertyFlattenDepth { get; init; } = 5;
}

/// <summary>
/// Ingestion request/size limits. All values are byte counts.
/// </summary>
public sealed class IngestionOptions
{
    /// <summary>
    /// Max HTTP body for <c>POST /api/events</c> (CLEF msgpack batch). A request whose
    /// body exceeds this is rejected with 413 before parsing. Default: 4 MB.
    /// </summary>
    public int MaxBatchBytes { get; init; } = 4 * 1024 * 1024;

    /// <summary>
    /// Max msgpack properties bytes for a single event — also the ring-buffer slab size.
    /// An event whose serialised properties exceed this is dropped (logged with its size),
    /// while the rest of the batch still ingests. Default: 64 KB.
    /// </summary>
    public int MaxEventPayloadBytes { get; init; } = 64 * 1024;

    /// <summary>
    /// Max HTTP body for the OTLP ingest endpoints (<c>/otlp/v1/*</c>). Larger → 413.
    /// Default: 8 MB.
    /// </summary>
    public int MaxOtlpBatchBytes { get; init; } = 8 * 1024 * 1024;

    /// <summary>
    /// Ring-buffer sequencing slots between the HTTP ingest endpoints and the storage
    /// drainer. Rounded up to a power of two. This is the absorption window for hot-tier
    /// flush stalls: at 100k events/s, 65536 slots ≈ 650 ms of headroom before events
    /// drop. Slot memory is ~64 B each (payload slabs are pooled separately and do NOT
    /// scale with this), so the default costs ~4 MB. Default: 65536.
    /// </summary>
    public int RingCapacity { get; init; } = 64 * 1024;

    /// <summary>
    /// Payload slab arena budget for the ring: slabCount = min(RingCapacity, this /
    /// MaxEventPayloadBytes). Slabs — not ring slots — are the true drop threshold when
    /// the drainer stalls: a pending event holds one slab whatever its size.
    ///
    /// <para><b>The default, when unset, is the larger of two terms:</b></para>
    /// <list type="bullet">
    /// <item>a slab floor — <see cref="DefaultArenaMinSlabs"/> (8 192) slabs of
    /// <see cref="MaxEventPayloadBytes"/>, capped at <see cref="MemoryBudgets.IngestArenaCapBytes"/>
    /// (512 MB) so a raised slab size cannot balloon the reservation; and</item>
    /// <item>the byte share — <c>min(512 MB, 15 % of the physical limit)</c>, see
    /// <see cref="MemoryBudgets.IngestArenaFraction"/>.</item>
    /// </list>
    /// <para>At the 64 KB default slab the floor is exactly 512 MB, so every host, the 512 MB
    /// container included, gets 8 192 slabs. The floor is there because an OpenTelemetry collector
    /// sends batches of 8 192 records by default and the parser fills the ring faster than the
    /// drainer empties it: the byte share alone gave a 512 MB container ~1 200 slabs, so one
    /// ordinary batch could run out of slabs part way through (HTTP 200 with a non-zero
    /// <c>dropped</c>, gRPC partial success). The byte share decides the size only when the slab
    /// is small enough that 8 192 of them come to less than it.</para>
    ///
    /// <para><b>What that costs, which depends on the operating system.</b> The arena is reserved
    /// virtual memory, and what it takes is never given back, so its high-water mark is a resting
    /// level, not a peak.</para>
    /// <list type="bullet">
    /// <item><b>Linux</b>: the allocation is lazily paged, so residency is per touched PAGE, not
    /// per slab. The arena opts out of transparent huge pages (<c>MADV_NOHUGEPAGE</c>), so that
    /// page stays 4 KB even where <c>transparent_hugepage</c> is <c>always</c>, as on RHEL; without
    /// it, the first write in each 2 MB range could make the whole 2 MB resident. A typical
    /// 0.3-2 KB event touches one 4 KB page at the start of its slab, so a full default batch of
    /// small events rests at about 32 MB. Only events near the maximum size fill their slabs, which
    /// is the same 512 MB worst case the flat default always had. (If the opt-out fails, the server
    /// logs it once at startup.)</item>
    /// <item><b>Windows</b>: the range is reserved and COMMITTED, in 1 MB chunks, up to the
    /// deepest slab ever handed out, whatever the events in it weigh, and never decommitted. One
    /// batch that outruns the drainer by ~8 192 events therefore commits ~512 MB even at 300 B an
    /// event. The working set still grows only by the pages written, but commit charge is what a
    /// job object's memory limit counts, and what counts against the system commit limit. <b>Under a
    /// Windows job or container memory limit, set this explicitly</b> to what that limit can
    /// carry.</item>
    /// </list>
    /// <para>Under a container memory limit, on EITHER platform, set this explicitly for a hard
    /// ceiling: the ~32 MB on Linux is what small events cost, not a bound, and large events still
    /// fill their slabs. A host that expects large events, or needs a ceiling below 512 MB, likewise
    /// sets it, and accepts that a burst then meets back-pressure earlier (counted as
    /// <c>ingestDroppedNoSlab</c>). <c>/api/diagnostics</c> reports
    /// <c>ingestArenaResidentBytes</c>: the deepest slab ever handed out times the slab size. On
    /// Windows that is the commit charge (to within 1 MB); on Linux it is an upper bound on the
    /// arena's resident memory, not a measurement of it. An explicit value always wins.</para>
    /// </summary>
    public long? PayloadPoolBytes { get; init; }

    /// <summary>
    /// Slabs the DEFAULT arena holds at least: one OpenTelemetry collector batch at its default
    /// size (8 192 records), so an ordinary batch does not run out of slabs before the drainer
    /// catches up. See <see cref="PayloadPoolBytes"/>.
    /// </summary>
    public const int DefaultArenaMinSlabs = 8192;

    /// <summary>The configured arena budget, or the default rule applied to this host when unset.</summary>
    public long EffectivePayloadPoolBytes =>
        PayloadPoolBytes ?? DefaultPayloadPoolBytesFor(MemoryBudgets.Current(), MaxEventPayloadBytes);

    /// <summary>
    /// The default arena rule as a pure function of the host's budgets and the slab size, so it
    /// can be checked at 512 MB and at 64 GB without a machine of each size — the shape
    /// <see cref="MemoryBudgets.Derive(long, long)"/> uses. It lives here, not in
    /// <see cref="MemoryBudgets"/>, because the slab size is an ingestion setting.
    /// </summary>
    public static long DefaultPayloadPoolBytesFor(in MemoryBudgets budgets, int maxEventPayloadBytes)
    {
        long slabFloor = Math.Min(
            MemoryBudgets.IngestArenaCapBytes,
            (long)DefaultArenaMinSlabs * Math.Max(1, maxEventPayloadBytes));

        return Math.Max(slabFloor, budgets.IngestArenaBytes);
    }
}

/// <summary>
/// Initial per-level retention defaults (days). Used only on first run;
/// after that the values live in SQLite and these are ignored.
/// </summary>
public sealed class RetentionConfig
{
    public int VerboseDays     { get; init; } = 90;
    public int DebugDays       { get; init; } = 3;
    public int InformationDays { get; init; } = 90;
    public int WarningDays     { get; init; } = 90;
    public int ErrorDays       { get; init; } = 90;
    public int FatalDays       { get; init; } = 90;
    public int MetricsDays     { get; init; } = 30;
    public int TracesDays      { get; init; } = 14;
}

/// <summary>
/// Software-update check: the server polls the GitHub Releases API and surfaces
/// "new version available" in the UI (Settings → Updates, admin only).
/// The check is a single conditional (ETag) request; 304 responses do not count
/// against the GitHub rate limit. Disable entirely for air-gapped installs.
/// </summary>
public sealed class UpdatesOptions
{
    public bool   Enabled              { get; init; } = true;
    /// <summary>Minutes between checks. Clamped to ≥ 15. Default: 60.</summary>
    public int    CheckIntervalMinutes { get; init; } = 60;
    /// <summary>GitHub "owner/repo" whose Releases are polled.</summary>
    public string GitHubRepository     { get; init; } = "AMeto-Labs/AMeto-Observability";
}

/// <summary>Query-path configuration.</summary>
public sealed class QueryOptions
{
    /// <summary>
    /// Budget for the cross-query cache of decoded segment indexes, charged at each
    /// entry's RETAINED size (expanded postings + dictionaries + bloom bits — several
    /// times the packed sections they decode from). Zero or negative disables it —
    /// every query then re-reads and re-decodes the sections it consults, the
    /// pre-cache behaviour.
    ///
    /// <para>Unset (the default) derives it from memory this process may use:
    /// <c>min(256 MB, 15 % of the managed-heap limit)</c> — the cache is mostly managed postings,
    /// so it is a share of the GC's limit (384 MB in a 512 MB container, giving 57 MB), not of
    /// the container — see <see cref="MemoryBudgets"/>. The flat
    /// 256 MB this replaces was half of a 512 MB host on its own, before the engine's own
    /// tiers and index builds asked for anything. An explicit value always wins, including
    /// a value larger than the derived one.</para>
    /// </summary>
    public long? IndexCacheBytes { get; init; }

    /// <summary>The configured budget, or the one derived from available memory when unset.</summary>
    public long EffectiveIndexCacheBytes => IndexCacheBytes ?? MemoryBudgets.Current().IndexCacheBytes;

    /// <summary>
    /// Ceiling on the NATIVE part of the cache — the segment bloom filters' bits, which are
    /// <c>NativeMemory</c> and so sit outside the GC's hard limit that
    /// <see cref="EffectiveIndexCacheBytes"/> is a share of. Whichever ceiling is reached first
    /// evicts from the LRU tail.
    ///
    /// <para><b>The rule: an explicitly configured <see cref="IndexCacheBytes"/> may raise this
    /// ceiling — to 20 % of the budget set — but never above
    /// <see cref="MemoryBudgets.IndexCacheNativeMaxFraction"/> of the PHYSICAL limit, because a
    /// budget says how much memory this component may hold and only the host says how much of it
    /// may be pinned where no collection can reach it.</b></para>
    ///
    /// <para>Both halves are needed. Held fixed, the ceiling silently capped the cache of anyone
    /// who deliberately raised the budget — at the measured worst-case native share of an entry
    /// (8.3 %) a 96 MB ceiling starts binding at roughly 1.2 GB of configured cache, and past that
    /// every insert evicts the LRU tail while <c>indexCacheBytes</c> sits far below
    /// <c>indexCacheBudgetBytes</c> and the hit rate never improves. Scaled without a reference to
    /// the host, it let the managed knob move NATIVE bytes without bound: in a 512 MB container a
    /// 1 GB budget asked for 204 MB of bloom bits — 40 % of the box, outside the GC's hard limit
    /// and unreclaimable by the RAM pressure path, which is the class of defect the backstop
    /// exists for.</para>
    ///
    /// <para>It never follows the budget DOWN — a small configured cache keeps the derived
    /// backstop — and every figure in the rule is a share of the PHYSICAL limit, because that is
    /// where these bytes live. An eviction this ceiling causes is counted separately
    /// (<c>indexCacheNativeEvicted</c>), since it is otherwise invisible. See
    /// <see cref="MemoryBudgets"/>.</para>
    /// </summary>
    public long EffectiveIndexCacheNativeBytes => IndexCacheNativeBytesFor(MemoryBudgets.Current());

    /// <summary>
    /// That same rule as a pure function of the host's budgets, so it can be checked at 512 MB and
    /// at 64 GB without a machine of each size — the shape
    /// <see cref="MemoryBudgets.Derive(long, long)"/> already uses for the budgets themselves.
    ///
    /// <para>A host that could not report a physical limit gets no scaling at all: with nothing
    /// real to clamp against, the backstop is the only figure anchored to anything.</para>
    /// </summary>
    public long IndexCacheNativeBytesFor(in MemoryBudgets budgets)
    {
        long derived = budgets.IndexCacheNativeBytes;
        if (IndexCacheBytes is not > 0) return derived;

        long scaled = (long)(IndexCacheBytes.Value * MemoryBudgets.IndexCacheNativeEntryShare);
        long host   = budgets.PhysicalLimitBytes > 0
            ? (long)(budgets.PhysicalLimitBytes * MemoryBudgets.IndexCacheNativeMaxFraction)
            : derived;

        // Max last: the clamp may only lower a SCALED ceiling, never cut into the backstop.
        return Math.Max(derived, Math.Min(scaled, host));
    }

    /// <summary>
    /// Drop cached segment indexes that no query has read for this long. Without it the only
    /// thing that ever removes an entry is budget pressure, so a server that answers one wide
    /// query and then goes quiet keeps those postings and native bloom bits resident for the
    /// rest of its life — the single biggest avoidable chunk of steady-state RSS on a small
    /// host. Zero or negative turns it off (budget pressure only). Default: 10 minutes.
    /// </summary>
    public TimeSpan IndexCacheIdleEvict { get; init; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Wall-clock budget for one search. A query that exceeds it is stopped and the client
    /// is told so — rather than the request occupying a core until the browser tab is
    /// closed, which is what an unbounded scan over an unbounded window did. Zero or
    /// negative removes the budget. Default: 60 s.
    /// </summary>
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// How many searches may run at once. Each one memory-maps segments and decompresses
    /// blocks in parallel, so a handful of dashboards refreshing together could take the
    /// whole box; past this limit a request is refused quickly (503 + Retry-After) instead
    /// of everything crawling. 0 = auto (processor count, clamped 2..16), negative =
    /// unlimited.
    /// </summary>
    public int MaxConcurrent { get; init; }

    /// <summary>How long a request waits for a slot before it is refused. Default: 5 s.</summary>
    public TimeSpan QueueWait { get; init; } = TimeSpan.FromSeconds(5);
}

/// <summary>
/// Live-tail (<c>GET /api/events/live</c>) pacing. A tail no longer polls on a timer: it
/// waits to be told that something was written, so these bound how often it MAY look, not
/// how often it does.
/// </summary>
public sealed class LiveTailOptions
{
    /// <summary>
    /// Floor between two polls of one tail. Under load a tail is signalled continuously, and
    /// without a floor it would re-query as fast as the searches complete — each one taking a
    /// search slot. This is the ceiling on that cost, and it doubles as the batching window:
    /// events arriving inside it are delivered together by the next poll. Default: 100 ms.
    /// </summary>
    public TimeSpan MinInterval { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Longest a parked tail waits before looking anyway. It bounds the keepalive interval —
    /// proxies drop idle connections — and is the safety net that limits how long a missed
    /// wake-up could hide an event. Default: 5 s.
    /// </summary>
    public TimeSpan MaxWait { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Events one poll may deliver before the next poll continues from its cursor. Default: 500.</summary>
    public int PageSize { get; init; } = 500;
}

/// <summary>Distributed-tracing settings that an operator may want to touch.</summary>
public sealed class TracesOptions
{
    /// <summary>
    /// What to do about segments written before the trace-id index existed:
    /// <c>Off</c>, <c>Idle</c> (default) or <c>Eager</c>.
    ///
    /// <para>A string rather than the enum itself so the YAML stays readable and an unknown value
    /// degrades to the default instead of failing to bind — this is a performance switch, and a
    /// typo in it must not stop the server from starting.</para>
    /// </summary>
    public string IndexBackfill { get; init; } = "Idle";

    /// <summary>
    /// Write the v4 segment format, which omits the per-segment trace index and is ~40% smaller.
    ///
    /// <para>A ONE-WAY DOOR, and off by default for that reason. Reading v4 costs nothing and is
    /// always on. Writing it means any binary older than this one, meeting those files, deletes
    /// them — it reads an unknown version as corruption. Turn this on only when every node that
    /// might read this data runs this build or newer, and only when you would not roll back.</para>
    /// </summary>
    public bool SegmentFormatV4 { get; init; }

    /// <summary>
    /// The off switch for the trace-id index itself, as opposed to <see cref="IndexBackfill"/>,
    /// which only decides whether OLD segments are migrated into it.
    ///
    /// <para>THE ROLLBACK HAS TO BE REACHABLE BY THE PERSON WHO NEEDS IT. The design's safety
    /// argument is that coverage can be dropped to empty at any moment and the engine goes back to
    /// scanning, which is how it behaved before this feature — but that was only true from a test
    /// seam, so an operator watching a trace return too few spans at three in the morning had no
    /// way to take it. Setting this to false and restarting withdraws every claim in one generation
    /// and closes every run.</para>
    ///
    /// <para>It costs speed and nothing else. No span is rewritten, no <c>.trc</c> is touched, and
    /// the <c>.tix</c> files are left where they are — turning it back on re-covers what the
    /// backfill re-indexes, at whatever pace <see cref="IndexBackfill"/> allows.</para>
    /// </summary>
    public bool IndexEnabled { get; init; } = true;
}

/// <summary>
/// Top-level server configuration.
/// </summary>
public sealed class ServerOptions
{
    public NodeId           NodeId           { get; init; } = NodeId.Local;
    public string           DataDirectory    { get; init; } = "data";
    public HotTierOptions   HotTier          { get; init; } = new();
    public QueryOptions     Query            { get; init; } = new();
    public LiveTailOptions  LiveTail         { get; init; } = new();
    public IndexingOptions  Indexing         { get; init; } = new();
    public IngestionOptions Ingestion        { get; init; } = new();
    public RetentionConfig  Retention        { get; init; } = new();
    public UpdatesOptions   Updates          { get; init; } = new();
    public LoggingOptions   Logging          { get; init; } = new();
    public TracesOptions    Traces           { get; init; } = new();
    public int              HttpPort         { get; init; } = 5341;

    /// <summary>
    /// URL prefix this server is served under — <c>"/ameto"</c> for a deployment reachable at
    /// <c>https://host/ameto</c>. Blank (the default) serves everything at the root, exactly
    /// as before.
    ///
    /// <para>This is applied at <b>runtime</b>, not baked into the client build: the SPA's
    /// <c>&lt;base href&gt;</c> is rewritten as index.html is served, so one build and one
    /// container image work under any prefix. It used to be an <c>ng build --base-href</c>
    /// flag in the Dockerfile, which meant the image and the Windows installer of the same
    /// version disagreed about where they were hosted.</para>
    ///
    /// <para>Accepts <c>ameto</c>, <c>/ameto</c> and <c>/ameto/</c> alike; see
    /// <see cref="UrlBasePath"/> for what is refused and why. Bound once at startup, so a
    /// change needs a restart.</para>
    ///
    /// <para>The prefix is <b>additive</b>: every path keeps answering at the root as well,
    /// because that is what <c>UsePathBase</c> does — a request that does not start with the
    /// prefix is passed through untouched. That is deliberate and load-bearing: the container
    /// health check and any OTLP agent already pointed at the bare address keep working.</para>
    /// </summary>
    public string           BasePath         { get; init; } = "";

    public string           SslCertPath      { get; init; } = "";
    public string           SslCertPassword  { get; init; } = "";

    /// <summary>
    /// Port for OTLP over gRPC — 4317 is the convention. <b>0 disables it</b>, which is the
    /// default: gRPC needs HTTP/2, and without TLS there is no ALPN to negotiate it, so a
    /// plaintext listener has to be told to speak HTTP/2 and then speaks nothing else. That
    /// cannot be the main port — no browser does HTTP/2 without TLS, so the UI, every /api call,
    /// the SSE tail and the container health check would all stop working on it. A second
    /// listener is therefore the only shape this can take, and opening one on every existing
    /// install because the binary was upgraded is not a decision to make on the operator's
    /// behalf. Set it to 4317 to accept collectors.
    /// </summary>
    public int              OtlpGrpcPort     { get; init; }

    /// <summary>
    /// Trust X-Forwarded-Proto/Host/For from a reverse proxy (nginx, traefik).
    /// Required for correct OAuth redirect URIs and generated links when TLS
    /// terminates on the proxy and Kestrel itself serves plain HTTP. Enable
    /// only when the server is reachable exclusively through that proxy —
    /// with it on, any client can spoof its scheme/host via headers.
    /// Default: false.
    /// </summary>
    public bool             TrustForwardedHeaders { get; init; } = false;

    /// <summary>
    /// IPs of the reverse proxies whose forwarded headers are trusted. When set
    /// (with <see cref="TrustForwardedHeaders"/>), only these sources may spoof
    /// scheme/host/for — far safer than trusting every client. Empty keeps the
    /// legacy "trust any proxy" behaviour (a startup warning is logged).
    /// </summary>
    public string[]         KnownProxies { get; init; } = [];

    /// <summary>
    /// System-wide RAM utilisation target (0–100 %).
    /// When the OS memory load exceeds this threshold the storage engine will
    /// flush the hot tier to disk, releasing the in-memory write buffer.
    /// Default: 85.
    /// </summary>
    public int              RamTargetPercent { get; init; } = 85;
}
