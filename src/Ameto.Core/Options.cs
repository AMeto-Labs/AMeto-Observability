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
    /// the drainer stalls. The arena is reserved virtual memory: resident pages track the
    /// bytes actually written (typical events touch one 4 KB page per slab), not the
    /// budget, so a generous value is cheap. Default: 512 MB (= 8192 slabs of 64 KB).
    /// </summary>
    public long PayloadPoolBytes { get; init; } = 512L * 1024 * 1024;
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
    /// pre-cache behaviour. Default: 256 MB.
    /// </summary>
    public long IndexCacheBytes { get; init; } = 256 * 1024 * 1024;

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
