using Ameto.Metrics.Storage;

namespace Ameto.Metrics;

/// <summary>
/// The numbers behind <c>GET /api/metrics/stats</c>, the Statistics page's Metrics band: what the
/// in-memory catalog and the cold tier's segment LIST answer without opening a file. A segment's
/// bytes are spread over the buckets its time range covers, in proportion to the overlap, so a
/// three-month window costs one pass over the catalogued segments however many points they hold —
/// the only figure a window could want that this cannot give is a point count, and that would mean
/// decompressing every file in the window.
///
/// <para>Not a hot path: a request on a page the operator opens, never a poll. Plain C#.</para>
/// </summary>
internal static class MetricStoreStats
{
    /// <summary>The most columns an answer carries; a finer bucket is widened, as <c>/api/events/counts</c> widens its own.</summary>
    internal const int MaxBuckets = 2_000;

    /// <summary>How many metrics get a series of their own before the rest fold into <c>other</c>.</summary>
    internal const int TopMetrics = 10;

    /// <summary>
    /// A bucket for a window that asked for none: about 96 columns, from the step table the client
    /// picks from (1 d → 15 min, 3 d → 1 h, 7 d → 2 h, 14 d → 4 h, a month → 12 h, 3 months → 1 day),
    /// so a chart drawn without the parameter still lands on a round step.
    /// </summary>
    internal static int AutoBucketSeconds(double rangeSeconds)
    {
        const int target = 96;
        double raw = rangeSeconds / target;
        ReadOnlySpan<int> steps = [60, 300, 900, 1800, 3600, 7200, 14_400, 21_600, 43_200, 86_400, 172_800, 604_800];
        foreach (int s in steps)
            if (s >= raw) return s;
        return (int)Math.Ceiling(raw / 604_800.0) * 604_800;
    }

    /// <summary>
    /// The answer for <c>[from, to]</c> at <paramref name="bucketSeconds"/> (0: <see cref="AutoBucketSeconds"/>).
    /// <paramref name="now"/> decides whether the hot tier's metric names count as "with data": the
    /// hot tier is the present, so they do when the window reaches it.
    /// </summary>
    internal static MetricStatsDto Build(
        MetricStoreSnapshot snapshot, IReadOnlyList<MetricCatalogEntry> catalog,
        DateTimeOffset from, DateTimeOffset to, int bucketSeconds, DateTimeOffset now)
    {
        // ── The grid: whole buckets of bucketSeconds counted from the epoch, as the log counts use ──
        double rangeSec = Math.Max(1, (to - from).TotalSeconds);
        if (bucketSeconds <= 0) bucketSeconds = AutoBucketSeconds(rangeSec);
        long minB = from.ToUnixTimeSeconds() / bucketSeconds;
        long maxB = to.ToUnixTimeSeconds()   / bucketSeconds;
        if (maxB - minB + 1 > MaxBuckets)
        {
            bucketSeconds = AutoBucketSeconds(rangeSec);
            minB = from.ToUnixTimeSeconds() / bucketSeconds;
            maxB = to.ToUnixTimeSeconds()   / bucketSeconds;
        }
        int  n           = (int)(maxB - minB + 1);
        long bucketNanos = bucketSeconds * 1_000_000_000L;
        long gridNano    = minB * bucketNanos;
        long fromNano    = from.ToUnixTimeMilliseconds() * 1_000_000L;
        long toNano      = to.ToUnixTimeMilliseconds()   * 1_000_000L;

        var buckets = new long[n];
        for (int i = 0; i < n; i++) buckets[i] = (minB + i) * bucketSeconds * 1000L;

        // ── Pass 1: the store as a whole, and each metric's share of bytes inside the window ──
        // Names compare as the hot tier files them (OrdinalIgnoreCase): two spellings of one
        // metric are one metric here too, not two rows.
        var store    = new MetricStatsStoreDto();
        var inWindow = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        long oldest = long.MaxValue, newest = long.MinValue;
        foreach (var s in snapshot.ColdSegments)
        {
            store.Segments++;
            store.Bytes += s.SizeBytes;
            switch (s.Granularity)
            {
                case MetricGranularity.Raw:     store.SegmentsRaw++; store.BytesRaw += s.SizeBytes; break;
                case MetricGranularity.FiveMin: store.Segments5m++;  store.Bytes5m  += s.SizeBytes; break;
                default:                        store.Segments1h++;  store.Bytes1h  += s.SizeBytes; break;
            }
            if (s.MinNano < oldest) oldest = s.MinNano;
            if (s.MaxNano > newest) newest = s.MaxNano;

            // The share ranks the metrics; the figures the answer carries come from the columns
            // below, so that raw + rollups, the per-metric bytes and the total all add up exactly.
            // A segment that only touches the window's edge has no bytes in it, and a metric with
            // nothing but such segments is not "with data" for the window.
            if (!Overlaps(s, fromNano, toNano)) continue;
            long share = ShareInside(s, fromNano, toNano);
            if (share <= 0) continue;
            inWindow[s.MetricName] = inWindow.GetValueOrDefault(s.MetricName) + share;
        }
        if (store.Segments > 0)
        {
            store.OldestMs = oldest / 1_000_000L;
            store.NewestMs = newest / 1_000_000L;
        }
        store.HotSeries = snapshot.HotSeries;
        store.HotPoints = snapshot.HotPoints;
        store.HotBytes  = snapshot.HotBytes;

        // ── The catalog: every metric the store has ever seen, by kind ──
        var byName = new Dictionary<string, MetricCatalogEntry>(catalog.Count, StringComparer.OrdinalIgnoreCase);
        var kinds  = new Dictionary<MetricKind, MetricStatsKindDto>();
        foreach (var e in catalog)
        {
            byName[e.Name] = e;
            store.Metrics++;
            store.Series += e.Cardinality;
            if (!kinds.TryGetValue(e.Kind, out var k)) kinds[e.Kind] = k = new MetricStatsKindDto { Kind = e.Kind.ToString() };
            k.Metrics++;
            k.Series += e.Cardinality;
        }

        // ── Pass 2: the top metrics by bytes in the window get a column each; the rest fold into other ──
        var ranked = new List<KeyValuePair<string, long>>(inWindow);
        ranked.Sort(static (a, b) => b.Value != a.Value ? b.Value.CompareTo(a.Value) : string.CompareOrdinal(a.Key, b.Key));
        var top = new Dictionary<string, long[]>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < ranked.Count && i < TopMetrics; i++) top[ranked[i].Key] = new long[n];
        var other = new long[n];
        var all   = new long[n];
        long windowRaw = 0, windowRollup = 0;
        foreach (var s in snapshot.ColdSegments)
        {
            if (!Overlaps(s, fromNano, toNano)) continue;
            long placed = Distribute(s, fromNano, toNano, gridNano, bucketNanos, n, top.GetValueOrDefault(s.MetricName) ?? other, all);
            if (s.Granularity == MetricGranularity.Raw) windowRaw += placed; else windowRollup += placed;
        }

        var byMetric = new List<MetricStatsMetricDto>(top.Count);
        for (int i = 0; i < ranked.Count && i < TopMetrics; i++)
        {
            string name   = ranked[i].Key;
            var    points = top[name];
            var    entry  = byName.GetValueOrDefault(name);
            byMetric.Add(new MetricStatsMetricDto
            {
                Name   = name,
                Kind   = entry?.Kind.ToString() ?? string.Empty,
                Unit   = entry?.Unit ?? string.Empty,
                Series = entry?.Cardinality ?? 0,
                Bytes  = Sum(points),
                Points = points,
            });
        }

        // "With data": a segment overlapping the window, or a hot series when the window is the present.
        var withData = new HashSet<string>(inWindow.Keys, StringComparer.OrdinalIgnoreCase);
        if (to >= now.AddMinutes(-1))
            foreach (var hot in snapshot.HotMetricNames) withData.Add(hot);

        long windowBytes = Sum(all), peak = 0;
        foreach (long v in all) if (v > peak) peak = v;

        var kindList = new List<MetricStatsKindDto>(kinds.Values);
        kindList.Sort(static (a, b) => string.CompareOrdinal(a.Kind, b.Kind));

        return new MetricStatsDto
        {
            From          = from.ToString("O"),
            To            = to.ToString("O"),
            BucketSeconds = bucketSeconds,
            Buckets       = buckets,
            Window        = new MetricStatsWindowDto
            {
                Metrics         = withData.Count,
                BytesWritten    = windowBytes,
                PeakBucketBytes = peak,
                RawBytes        = windowRaw,
                RollupBytes     = windowRollup,
            },
            Store       = store,
            Kinds       = kindList,
            ByMetric    = byMetric,
            OtherBytes  = Sum(other),
            OtherPoints = other,
        };
    }

    private static bool Overlaps(MetricSegmentInfo s, long fromNano, long toNano) =>
        s.MaxNano >= fromNano && s.MinNano <= toNano;

    /// <summary>The part of a segment's bytes that falls inside the window, by time: all of a single-instant segment.</summary>
    private static long ShareInside(MetricSegmentInfo s, long fromNano, long toNano)
    {
        long span = s.MaxNano - s.MinNano;
        if (span <= 0) return s.SizeBytes;
        long lo = Math.Max(s.MinNano, fromNano), hi = Math.Min(s.MaxNano, toNano);
        return (long)((double)s.SizeBytes * Math.Max(0, hi - lo) / span);
    }

    /// <summary>
    /// A segment's bytes over the buckets its range covers inside the window, each bucket taking the
    /// share of the range it holds. A segment is one file for one time range, and nothing short of
    /// opening it says where in that range the bytes fell; the proportional spread is the honest
    /// column for a chart whose buckets are wider than a flush and narrower than a rollup file.
    /// Returns what it placed, which rounding may leave a byte or two under the segment's share.
    /// </summary>
    private static long Distribute(
        MetricSegmentInfo s, long fromNano, long toNano, long gridNano, long bucketNanos, int n, long[] target, long[] all)
    {
        long lo   = Math.Max(s.MinNano, fromNano);
        long hi   = Math.Min(s.MaxNano, toNano);
        long span = s.MaxNano - s.MinNano;
        if (span <= 0)
        {
            int b = BucketOf(lo, gridNano, bucketNanos, n);
            target[b] += s.SizeBytes;
            all[b]    += s.SizeBytes;
            return s.SizeBytes;
        }
        long placed = 0;
        int b0 = BucketOf(lo, gridNano, bucketNanos, n), b1 = BucketOf(hi, gridNano, bucketNanos, n);
        for (int b = b0; b <= b1; b++)
        {
            long bs = gridNano + b * bucketNanos, be = bs + bucketNanos;
            long overlap = Math.Min(be, hi) - Math.Max(bs, lo);
            if (overlap <= 0) continue;
            long part = (long)((double)s.SizeBytes * overlap / span);
            target[b] += part;
            all[b]    += part;
            placed    += part;
        }
        return placed;
    }

    private static int BucketOf(long nano, long gridNano, long bucketNanos, int n) =>
        (int)Math.Clamp((nano - gridNano) / bucketNanos, 0, n - 1);

    private static long Sum(long[] values)
    {
        long total = 0;
        foreach (long v in values) total += v;
        return total;
    }
}

// ── DTOs ────────────────────────────────────────────────────────────────────────

/// <summary>Answer of <c>GET /api/metrics/stats</c>; serialized through <see cref="MetricJson.Web"/>.</summary>
public sealed class MetricStatsDto
{
    public string                   From          { get; set; } = string.Empty;
    public string                   To            { get; set; } = string.Empty;
    public int                      BucketSeconds { get; set; }
    /// <summary>Bucket start timestamps, Unix milliseconds; every per-bucket array below is aligned with it.</summary>
    public long[]                   Buckets       { get; set; } = [];
    public MetricStatsWindowDto     Window        { get; set; } = new();
    public MetricStatsStoreDto      Store         { get; set; } = new();
    public List<MetricStatsKindDto> Kinds         { get; set; } = [];
    /// <summary>The metrics with the most bytes written in the window, most first.</summary>
    public List<MetricStatsMetricDto> ByMetric    { get; set; } = [];
    /// <summary>Every metric outside <see cref="ByMetric"/>, folded.</summary>
    public long                     OtherBytes    { get; set; }
    public long[]                   OtherPoints   { get; set; } = [];
}

/// <summary>What the window holds.</summary>
public sealed class MetricStatsWindowDto
{
    /// <summary>Metrics with a segment in the window, or a hot series when the window reaches now.</summary>
    public int  Metrics         { get; set; }
    /// <summary>Segment bytes attributed to the window; the sum of every per-bucket array.</summary>
    public long BytesWritten    { get; set; }
    public long PeakBucketBytes { get; set; }
    public long RawBytes        { get; set; }
    public long RollupBytes     { get; set; }
}

/// <summary>The store as a whole, window or no window.</summary>
public sealed class MetricStatsStoreDto
{
    /// <summary>Metrics the catalog knows — every name the store has seen since it was created.</summary>
    public int   Metrics     { get; set; }
    /// <summary>Tracked series across those metrics (each metric's cardinality, capped as the catalog caps it).</summary>
    public long  Series      { get; set; }
    public int   Segments    { get; set; }
    public int   SegmentsRaw { get; set; }
    public int   Segments5m  { get; set; }
    public int   Segments1h  { get; set; }
    public long  Bytes       { get; set; }
    public long  BytesRaw    { get; set; }
    public long  Bytes5m     { get; set; }
    public long  Bytes1h     { get; set; }
    public int   HotSeries   { get; set; }
    public int   HotPoints   { get; set; }
    public long  HotBytes    { get; set; }
    /// <summary>The earliest and latest point time any cold segment covers, Unix milliseconds; null with no segments.</summary>
    public long? OldestMs    { get; set; }
    public long? NewestMs    { get; set; }
}

public sealed class MetricStatsKindDto
{
    public string Kind    { get; set; } = string.Empty;
    public int    Metrics { get; set; }
    public long   Series  { get; set; }
}

public sealed class MetricStatsMetricDto
{
    public string Name   { get; set; } = string.Empty;
    public string Kind   { get; set; } = string.Empty;
    public string Unit   { get; set; } = string.Empty;
    public int    Series { get; set; }
    public long   Bytes  { get; set; }
    public long[] Points { get; set; } = [];
}
