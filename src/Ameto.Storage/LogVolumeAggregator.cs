using System.Runtime.CompilerServices;
using System.Text;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>One time-bucketed series: a service or a level with a dense per-bucket count array.</summary>
public readonly record struct LogSeries(string Name, long Count, long[] Points);

/// <summary>
/// Result of a header-only log-volume scan: per-service and per-level counts bucketed over time.
/// <see cref="Services"/> is sorted by descending total; <see cref="Levels"/> holds only the
/// severity levels that actually occurred, in ascending severity order.
/// </summary>
public sealed class LogVolumeCounts
{
    public required long Total          { get; init; }
    public required long Scanned        { get; init; }
    public required long MinBucket      { get; init; }
    public required int  BucketSeconds  { get; init; }
    public required int  NBuckets       { get; init; }
    public required IReadOnlyList<LogSeries> Services { get; init; }
    public required IReadOnlyList<LogSeries> Levels   { get; init; }
}

/// <summary>
/// Accumulates <c>(bucket, service, level)</c> event counts straight from event <b>headers</b> —
/// timestamp, level and service pool-index — without ever materialising a <see cref="LogEvent"/>,
/// its <c>Properties</c> map, message template or exception. This is the counting core behind
/// <c>GET /api/events/counts</c>.
///
/// <para>Allocation profile: the per-event <c>Add*</c> methods perform only array-index increments
/// plus a dictionary probe for the service; new heap allocation happens once per <i>distinct</i>
/// service (low-cardinality) and never per event. Service cardinality is inherently small, so
/// per-service dense bucket arrays are cheaper and simpler than a sparse <c>(svc,bucket)</c> map.</para>
///
/// <para>Not thread-safe: the owning <see cref="StorageEngine"/> feeds hot then cold events from a
/// single logical thread of control (cold runs after an <c>await</c>, establishing happens-before),
/// so no locking is required on the hot path.</para>
/// </summary>
public sealed class LogVolumeAggregator
{
    private const long   UnixEpochTicks = 621_355_968_000_000_000L; // DateTime(1970,1,1).Ticks
    private const long   TicksPerSecond = TimeSpan.TicksPerSecond;
    private const int    LevelCount     = 6;                          // Verbose..Fatal
    private const string UnknownService = "(unknown)";

    private readonly long   _fromTicks;
    private readonly long   _toTicks;
    private readonly long   _minBucket;
    private readonly int    _bucketSeconds;
    private readonly int    _nBuckets;
    private readonly string? _serviceFilter;      // null => aggregate every service
    private readonly StringInternPool _pool;      // resolves hot-tier ServiceNamePoolIndex

    // Levels are a fixed, tiny enum — dense arrays, allocated once.
    private readonly long[]   _levelTotals  = new long[LevelCount];
    private readonly long[][] _levelBuckets = new long[LevelCount][];

    // Services: shared id-space keyed on name (case-insensitive). Both the hot (pool index)
    // and cold (UTF-8 bytes) paths resolve to the same local id via this dictionary so a
    // service split across tiers merges. Dense per-service bucket arrays grow lazily.
    private readonly Dictionary<string, int> _svcIds = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, int>.AlternateLookup<ReadOnlySpan<char>> _svcByChars;
    private readonly List<string> _svcNames  = new();
    private readonly List<long>   _svcTotals = new();
    private readonly List<long[]> _svcBuckets = new();

    // Hot-path cache: pool index -> local service id (or -1 when filtered out). Avoids
    // re-resolving the interned string for every event of the same service.
    private readonly Dictionary<int, int> _poolIdxToSvc = new();

    private long _total;
    private long _scanned;

    public long Total   => _total;
    public long Scanned => _scanned;

    /// <param name="fromTicks">Inclusive lower bound (UTC ticks); events outside are ignored.</param>
    /// <param name="toTicks">Inclusive upper bound (UTC ticks).</param>
    /// <param name="minBucket">Index of the first column: <c>unixSeconds(from) / bucketSeconds</c>.</param>
    /// <param name="bucketSeconds">Column width in seconds.</param>
    /// <param name="nBuckets">Number of columns.</param>
    /// <param name="serviceFilter">When non-null, only this service is counted (case-insensitive).</param>
    /// <param name="pool">Intern pool that maps hot-tier service pool indices to names.</param>
    public LogVolumeAggregator(
        long fromTicks, long toTicks, long minBucket, int bucketSeconds, int nBuckets,
        string? serviceFilter, StringInternPool pool)
    {
        _fromTicks     = fromTicks;
        _toTicks       = toTicks;
        _minBucket     = minBucket;
        _bucketSeconds = bucketSeconds;
        _nBuckets      = nBuckets;
        _serviceFilter = string.IsNullOrEmpty(serviceFilter) ? null : serviceFilter;
        _pool          = pool;

        for (int i = 0; i < LevelCount; i++)
            _levelBuckets[i] = new long[nBuckets];

        // OrdinalIgnoreCase supports span-keyed alternate lookups, letting the cold path
        // probe by UTF-16 chars without allocating a string until a new service appears.
        _svcByChars = _svcIds.GetAlternateLookup<ReadOnlySpan<char>>();
    }

    // ── Hot-tier entry point (service resolved from the intern pool index) ─────────

    /// <summary>Counts one in-window hot-tier header. <paramref name="servicePoolIndex"/> is
    /// <see cref="LogEventHeader.ServiceNamePoolIndex"/> (-1 when absent).</summary>
    public void AddByPoolIndex(long timestampTicks, LogLevel level, int servicePoolIndex)
    {
        _scanned++;
        int svc = ResolveByPoolIndex(servicePoolIndex);
        if (svc >= 0) Record(timestampTicks, level, svc);
    }

    // ── Cold-tier entry point (service is the raw UTF-8 slice from the seg block) ──

    /// <summary>Counts one in-window cold-tier event given the UTF-8 bytes of its service name
    /// (empty span => "(unknown)"). No string is allocated unless the service is new.</summary>
    public void AddByServiceUtf8(long timestampTicks, LogLevel level, ReadOnlySpan<byte> serviceUtf8)
    {
        _scanned++;
        int svc = ResolveByUtf8(serviceUtf8);
        if (svc >= 0) Record(timestampTicks, level, svc);
    }

    // ── Recording ─────────────────────────────────────────────────────────────────

    private void Record(long timestampTicks, LogLevel level, int svcId)
    {
        _total++;
        int off = BucketOffset(timestampTicks);

        int li = (int)level;
        if ((uint)li < LevelCount)
        {
            _levelTotals[li]++;
            if (off >= 0) _levelBuckets[li][off]++;
        }

        _svcTotals[svcId]++;
        if (off >= 0) _svcBuckets[svcId][off]++;
    }

    /// <summary>Column index for a timestamp, or -1 if it falls outside the bucket axis.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int BucketOffset(long timestampTicks)
    {
        // Matches DateTimeOffset.ToUnixTimeSeconds() (floor division) used by the reference path.
        long unixSeconds = (timestampTicks - UnixEpochTicks) / TicksPerSecond;
        long off = unixSeconds / _bucketSeconds - _minBucket;
        return (ulong)off < (ulong)_nBuckets ? (int)off : -1;
    }

    // ── Service resolution ─────────────────────────────────────────────────────────

    private int ResolveByPoolIndex(int poolIndex)
    {
        if (_poolIdxToSvc.TryGetValue(poolIndex, out int cached))
            return cached;

        string name = poolIndex >= 0 ? _pool.Get(poolIndex) : string.Empty;
        if (string.IsNullOrEmpty(name)) name = UnknownService;

        int id = GetOrAddByName(name);
        _poolIdxToSvc[poolIndex] = id;   // cache the (possibly filtered-out, -1) result
        return id;
    }

    private int ResolveByUtf8(ReadOnlySpan<byte> serviceUtf8)
    {
        if (serviceUtf8.IsEmpty)
            return GetOrAddByName(UnknownService);

        // UTF-8 char count never exceeds byte count, so a byte-length-sized buffer always fits.
        // Keep it on the stack for the common short-name case; fall back to a heap buffer only
        // for pathologically long service names.
        Span<char> buf = serviceUtf8.Length <= 512
            ? stackalloc char[512]
            : new char[serviceUtf8.Length];
        int n = Encoding.UTF8.GetChars(serviceUtf8, buf);
        ReadOnlySpan<char> chars = buf[..n];

        if (_serviceFilter is not null &&
            !chars.Equals(_serviceFilter, StringComparison.OrdinalIgnoreCase))
            return -1;

        if (_svcByChars.TryGetValue(chars, out int existing))
            return existing;

        return AddService(new string(chars));
    }

    private int GetOrAddByName(string name)
    {
        if (_serviceFilter is not null &&
            !name.Equals(_serviceFilter, StringComparison.OrdinalIgnoreCase))
            return -1;

        return _svcIds.TryGetValue(name, out int id) ? id : AddService(name);
    }

    private int AddService(string name)
    {
        int id = _svcNames.Count;
        _svcIds[name] = id;
        _svcNames.Add(name);
        _svcTotals.Add(0);
        _svcBuckets.Add(new long[_nBuckets]);
        return id;
    }

    // ── Result ─────────────────────────────────────────────────────────────────────

    /// <summary>Snapshots the accumulated counts. Services come out sorted by descending total;
    /// levels include only those that occurred, in ascending severity order.</summary>
    public LogVolumeCounts Build()
    {
        int n = _svcNames.Count;
        var services = new LogSeries[n];
        for (int i = 0; i < n; i++)
            services[i] = new LogSeries(_svcNames[i], _svcTotals[i], _svcBuckets[i]);
        Array.Sort(services, static (a, b) => b.Count.CompareTo(a.Count));

        var levels = new List<LogSeries>(LevelCount);
        for (int l = 0; l < LevelCount; l++)
            if (_levelTotals[l] > 0)
                levels.Add(new LogSeries(((LogLevel)l).ToSeqString(), _levelTotals[l], _levelBuckets[l]));

        return new LogVolumeCounts
        {
            Total         = _total,
            Scanned       = _scanned,
            MinBucket     = _minBucket,
            BucketSeconds = _bucketSeconds,
            NBuckets      = _nBuckets,
            Services      = services,
            Levels        = levels,
        };
    }
}
