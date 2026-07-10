using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// Verifies the header-only counts path (hot <see cref="HotTierSegment.AggregateInto"/> and cold
/// <see cref="SegmentReader.AggregateHeaders"/>) produces exactly the same buckets/services/levels
/// as a full-materialisation reference — the correctness contract for repointing
/// <c>GET /api/events/counts</c> at the cheap path.
/// </summary>
public sealed class LogVolumeAggregatorTests
{
    private const int BucketSeconds = 60;

    // Deterministic base: 2024-01-01T00:00:00Z.
    private static readonly DateTimeOffset Base = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private sealed record Ev(long Ticks, LogLevel Level, string? Service);

    // ── Reference (mirrors LogVolumeAggregator semantics over the raw event list) ──

    private sealed class Reference
    {
        public readonly Dictionary<string, long>   SvcTotals   = new(StringComparer.OrdinalIgnoreCase);
        public readonly Dictionary<string, long[]> SvcPoints   = new(StringComparer.OrdinalIgnoreCase);
        public readonly long[]                     LevelTotals = new long[6];
        public readonly long[][]                   LevelPoints = new long[6][];
        public long Total, Scanned;

        public Reference(int nBuckets)
        {
            for (int i = 0; i < 6; i++) LevelPoints[i] = new long[nBuckets];
        }
    }

    private static Reference BuildReference(
        IEnumerable<Ev> events, long fromTicks, long toTicks,
        long minB, int nBuckets, string? filter)
    {
        var r = new Reference(nBuckets);
        foreach (var e in events)
        {
            if (e.Ticks < fromTicks || e.Ticks > toTicks) continue; // window filter
            r.Scanned++;

            string svc = string.IsNullOrEmpty(e.Service) ? "(unknown)" : e.Service!;
            if (filter is not null && !svc.Equals(filter, StringComparison.OrdinalIgnoreCase)) continue;

            r.Total++;
            int off = (int)(new DateTimeOffset(e.Ticks, TimeSpan.Zero).ToUnixTimeSeconds() / BucketSeconds - minB);

            r.SvcTotals[svc] = r.SvcTotals.GetValueOrDefault(svc) + 1;
            if (!r.SvcPoints.TryGetValue(svc, out var sp)) { sp = new long[nBuckets]; r.SvcPoints[svc] = sp; }
            if ((uint)off < (uint)nBuckets) sp[off]++;

            r.LevelTotals[(int)e.Level]++;
            if ((uint)off < (uint)nBuckets) r.LevelPoints[(int)e.Level][off]++;
        }
        return r;
    }

    private static void AssertMatches(Reference expected, LogVolumeCounts actual)
    {
        Assert.Equal(expected.Total,   actual.Total);
        Assert.Equal(expected.Scanned, actual.Scanned);

        // Services: same set, same totals, same per-bucket points.
        Assert.Equal(expected.SvcTotals.Count, actual.Services.Count);
        foreach (var s in actual.Services)
        {
            Assert.True(expected.SvcTotals.TryGetValue(s.Name, out var expTot), $"unexpected service {s.Name}");
            Assert.Equal(expTot, s.Count);
            Assert.Equal(expected.SvcPoints[s.Name], s.Points);
        }
        // Services must be sorted by descending count.
        for (int i = 1; i < actual.Services.Count; i++)
            Assert.True(actual.Services[i - 1].Count >= actual.Services[i].Count, "services not sorted desc");

        // Levels: exactly the ones that occurred, correct totals + points, ascending severity.
        int expectedLevelCount = 0;
        for (int l = 0; l < 6; l++) if (expected.LevelTotals[l] > 0) expectedLevelCount++;
        Assert.Equal(expectedLevelCount, actual.Levels.Count);

        int prev = -1;
        foreach (var lv in actual.Levels)
        {
            Assert.True(LogLevelExtensions.TryParse(lv.Name, out var level));
            Assert.True((int)level > prev, "levels not ascending"); prev = (int)level;
            Assert.Equal(expected.LevelTotals[(int)level], lv.Count);
            Assert.Equal(expected.LevelPoints[(int)level], lv.Points);
        }
    }

    // ── Fixture generation ─────────────────────────────────────────────────────

    private static List<Ev> MakeEvents(int n, string?[] services)
    {
        var list = new List<Ev>(n);
        for (int i = 0; i < n; i++)
        {
            long ticks = Base.AddSeconds(i).UtcTicks;           // one event per second
            var  level = (LogLevel)(i % 6);
            var  svc   = services[i % services.Length];
            list.Add(new Ev(ticks, level, svc));
        }
        return list;
    }

    private static (long minB, int nBuckets) Axis(long fromTicks, long toTicks)
    {
        long minB = new DateTimeOffset(fromTicks, TimeSpan.Zero).ToUnixTimeSeconds() / BucketSeconds;
        long maxB = new DateTimeOffset(toTicks,   TimeSpan.Zero).ToUnixTimeSeconds() / BucketSeconds;
        return (minB, (int)(maxB - minB + 1));
    }

    private static HotTierSegment FillHot(IReadOnlyList<Ev> events, StringInternPool pool)
    {
        var hot = new HotTierSegment(maxEvents: events.Count + 16, maxPayloadBytes: 16 * 1024 * 1024);
        for (int i = 0; i < events.Count; i++)
        {
            var e = events[i];
            int svcIdx = e.Service is null ? -1 : pool.Intern(e.Service);
            var header = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = e.Ticks,
                Level                    = e.Level,
                MessageTemplatePoolIndex = -1,
                ServiceNamePoolIndex     = svcIdx,
            };
            Assert.True(hot.TryWrite(header, ReadOnlySpan<byte>.Empty));
        }
        return hot;
    }

    // ── Hot tier ────────────────────────────────────────────────────────────────

    [Fact]
    public void HotAggregate_MatchesReference()
    {
        var services = new string?[] { "orders", "billing", "gateway", "orders" };
        var events   = MakeEvents(500, services);
        long fromTicks = events[0].Ticks;
        long toTicks   = events[^1].Ticks;
        var (minB, nBuckets) = Axis(fromTicks, toTicks);

        var pool = new StringInternPool();
        using var hot = FillHot(events, pool);

        var agg = new LogVolumeAggregator(fromTicks, toTicks, minB, BucketSeconds, nBuckets, null, pool);
        hot.AggregateInto(agg, fromTicks, toTicks);

        AssertMatches(BuildReference(events, fromTicks, toTicks, minB, nBuckets, null), agg.Build());
    }

    [Fact]
    public void HotAggregate_UnknownServiceAndFilter()
    {
        // Mix in events with no service.name (pool index -1) → "(unknown)".
        var services = new string?[] { "orders", null, "billing", null };
        var events   = MakeEvents(240, services);
        long fromTicks = events[0].Ticks;
        long toTicks   = events[^1].Ticks;
        var (minB, nBuckets) = Axis(fromTicks, toTicks);

        var pool = new StringInternPool();
        using var hot = FillHot(events, pool);

        // No filter: "(unknown)" is a first-class bucket.
        var aggAll = new LogVolumeAggregator(fromTicks, toTicks, minB, BucketSeconds, nBuckets, null, pool);
        hot.AggregateInto(aggAll, fromTicks, toTicks);
        var all = aggAll.Build();
        AssertMatches(BuildReference(events, fromTicks, toTicks, minB, nBuckets, null), all);
        Assert.Contains(all.Services, s => s.Name == "(unknown)");

        // Filter to one service: only that series survives, totals shrink, scanned stays full.
        var aggFilter = new LogVolumeAggregator(fromTicks, toTicks, minB, BucketSeconds, nBuckets, "billing", pool);
        hot.AggregateInto(aggFilter, fromTicks, toTicks);
        var filtered = aggFilter.Build();
        AssertMatches(BuildReference(events, fromTicks, toTicks, minB, nBuckets, "billing"), filtered);
        Assert.Single(filtered.Services);
        Assert.Equal("billing", filtered.Services[0].Name);
        Assert.Equal(events.Count, filtered.Scanned);
    }

    [Fact]
    public void HotAggregate_WindowExcludesOutOfRange()
    {
        var events   = MakeEvents(600, new string?[] { "orders", "billing" });
        // Narrow window to the middle third of the events.
        long fromTicks = events[200].Ticks;
        long toTicks   = events[399].Ticks;
        var (minB, nBuckets) = Axis(fromTicks, toTicks);

        var pool = new StringInternPool();
        using var hot = FillHot(events, pool);

        var agg = new LogVolumeAggregator(fromTicks, toTicks, minB, BucketSeconds, nBuckets, null, pool);
        hot.AggregateInto(agg, fromTicks, toTicks);
        var result = agg.Build();

        AssertMatches(BuildReference(events, fromTicks, toTicks, minB, nBuckets, null), result);
        Assert.Equal(200, result.Scanned); // exactly events[200..399]
        Assert.Equal(200, result.Total);
    }

    // ── Cold tier (flush → SegmentReader.AggregateHeaders) ──────────────────────

    [Fact]
    public async Task ColdAggregate_MatchesReferenceAndFullScan()
    {
        // Enough events (with several services) to span multiple 64 KB columnar blocks.
        var services = new string?[] { "orders", "billing", "gateway", "search", "auth" };
        var events   = MakeEvents(3000, services);
        long fromTicks = events[0].Ticks;
        long toTicks   = events[^1].Ticks;
        var (minB, nBuckets) = Axis(fromTicks, toTicks);

        var pool = new StringInternPool();
        using var hot = FillHot(events, pool);

        string path = Path.Combine(Path.GetTempPath(), $"ameto-agg-{Guid.NewGuid():N}.seg");
        try
        {
            using (var writer = new SegmentWriter(path))
            {
                writer.WriteEvents(hot, pool);
                writer.WriteInvertedIndex([]);
                writer.WriteTrigramIndex([]);
                writer.WriteBloomFilter([]);
                writer.Finalise(new NodeId(1), new SegmentId(1));
            }

            using var reader = SegmentReader.Open(path);

            // Header path.
            var agg = new LogVolumeAggregator(fromTicks, toTicks, minB, BucketSeconds, nBuckets, null, pool);
            reader.AggregateHeaders(agg, fromTicks, toTicks);
            var headerCounts = agg.Build();

            var expected = BuildReference(events, fromTicks, toTicks, minB, nBuckets, null);
            AssertMatches(expected, headerCounts);

            // Cross-check: the full-materialisation scan (the old path) must yield the SAME triples,
            // proving the header path equals full-scan semantics on real segment bytes.
            var fullScan = new List<Ev>();
            await foreach (var ev in reader.ReadEventsAsync(null, null, null))
                fullScan.Add(new Ev(ev.Timestamp.UtcTicks, ev.Level, ev.ServiceName));

            Assert.Equal(events.Count, fullScan.Count);
            var fullRef = BuildReference(fullScan, fromTicks, toTicks, minB, nBuckets, null);
            AssertMatches(fullRef, headerCounts);
        }
        finally
        {
            try { File.Delete(path); } catch { /* best-effort */ }
        }
    }

    [Fact]
    public async Task ColdAggregate_ServiceFilter()
    {
        var services = new string?[] { "orders", "billing", "gateway" };
        var events   = MakeEvents(1200, services);
        long fromTicks = events[0].Ticks;
        long toTicks   = events[^1].Ticks;
        var (minB, nBuckets) = Axis(fromTicks, toTicks);

        var pool = new StringInternPool();
        using var hot = FillHot(events, pool);

        string path = Path.Combine(Path.GetTempPath(), $"ameto-agg-{Guid.NewGuid():N}.seg");
        try
        {
            using (var writer = new SegmentWriter(path))
            {
                writer.WriteEvents(hot, pool);
                writer.WriteInvertedIndex([]);
                writer.WriteTrigramIndex([]);
                writer.WriteBloomFilter([]);
                writer.Finalise(new NodeId(1), new SegmentId(1));
            }

            using var reader = SegmentReader.Open(path);
            var agg = new LogVolumeAggregator(fromTicks, toTicks, minB, BucketSeconds, nBuckets, "gateway", pool);
            reader.AggregateHeaders(agg, fromTicks, toTicks);
            var result = agg.Build();

            AssertMatches(BuildReference(events, fromTicks, toTicks, minB, nBuckets, "gateway"), result);
            Assert.Single(result.Services);
            Assert.Equal("gateway", result.Services[0].Name);
        }
        finally
        {
            try { File.Delete(path); } catch { /* best-effort */ }
        }
    }

    [Fact]
    public void HotAggregate_Empty_ProducesEmptyResult()
    {
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(16, 64 * 1024);
        var agg = new LogVolumeAggregator(0, long.MaxValue, 0, BucketSeconds, 1, null, pool);
        hot.AggregateInto(agg, 0, long.MaxValue);
        var result = agg.Build();
        Assert.Equal(0, result.Total);
        Assert.Equal(0, result.Scanned);
        Assert.Empty(result.Services);
        Assert.Empty(result.Levels);
    }
}
