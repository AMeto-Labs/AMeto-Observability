using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit.Abstractions;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// Compaction targets ONE MAXIMAL SEGMENT PER (time bucket, level), and these are the four
/// properties that makes it a policy rather than a heuristic:
///
/// <list type="number">
/// <item>a bucket collapses toward a single file;</item>
/// <item>once collapsed it is never rewritten again — the sweep has a terminal state;</item>
/// <item>the bucket's WIDTH is a fraction of the level's own TTL, so a rare level does not get a
///       near-empty file per day;</item>
/// <item>today's bucket keeps working: still-arriving data stays queryable and a flush does not
///       drag the day-so-far through another rewrite.</item>
/// </list>
///
/// <para>Timestamps here are snapped to real bucket boundaries. The grid is aligned (a segment's
/// bucket is <c>floor(MinTimestamp / width)</c>, not a window anchored on whichever file happened
/// to be oldest), so a test that used <c>UtcNow - n days</c> directly would straddle a boundary
/// on some runs and not others.</para>
/// </summary>
public sealed class SegmentBucketCompactionTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-bucket-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private StorageEngine _engine = null!;

    public SegmentBucketCompactionTests(ITestOutputHelper output) => _out = output;

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _engine = NewEngine();
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private StorageEngine NewEngine() => new(
        Options.Create(new ServerOptions { DataDirectory = _dir }),
        new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
        NullLogger<StorageEngine>.Instance)
    {
        _allowIndexlessMerge = true,
    };

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>Bucket width the engine uses for a level under the default retention policy.</summary>
    private static long BucketTicks(LogLevel level) =>
        StorageEngine.MergeBucketTicks(RetentionPolicy.Default.GetTtl(level));

    /// <summary>Start of the bucket <paramref name="ticks"/> falls in, for that level's width.</summary>
    private static long BucketStart(LogLevel level, long ticks)
    {
        long w = BucketTicks(level);
        return ticks / w * w;
    }

    private static byte[] Props(int i, int padBytes = 0)
    {
        var buf = new ArrayBufferWriter<byte>(64 + padBytes);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(padBytes > 0 ? 3 : 2);
        w.Write("n");   w.Write((long)i);
        w.Write("key"); w.Write("wallet:" + i);
        if (padBytes > 0) { w.Write("pad"); w.Write(new string('x', padBytes)); }
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private int _seq;

    private void Write(int count, LogLevel level, long baseTicks, int padBytes = 0)
    {
        for (int i = 0; i < count; i++)
        {
            int n = _seq++;
            Assert.True(_engine.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)n).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerSecond,
                Level                    = level,
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {n}"),
                ServiceNamePoolIndex     = _engine.TemplatePool.Intern("Svc.A"),
            }, Props(n, padBytes)));
        }
    }

    private async Task FlushAsync(int count, LogLevel level, long baseTicks, int padBytes = 0)
    {
        Write(count, level, baseTicks, padBytes);
        await _engine.FlushHotTierAsync();
    }

    private List<RawSegmentEvent> ReadEverything()
    {
        var dedup = new Dictionary<string, string>(StringComparer.Ordinal);
        var all   = new List<RawSegmentEvent>();
        foreach (var seg in _engine.ListSegments())
        {
            using var r = SegmentReader.Open(seg.FilePath);
            all.AddRange(r.ReadAllRaw(dedup));
        }
        return all.OrderBy(e => e.Id).ToList();
    }

    /// <summary>
    /// Runs merge passes until the planner reports nothing left to do, returning how many
    /// batches it merged and how many payload bytes those merges WROTE — the raw material for
    /// the write-amplification claim.
    /// </summary>
    private async Task<(int Merges, long BytesWritten)> CompactToExhaustionAsync()
    {
        int  merges = 0;
        long written = 0;
        while (await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None))
        {
            merges++;
            Assert.True(merges < 40, "compaction did not converge — the planner is churning");
            // The newest segment is the one this pass produced (ids are monotonic).
            written += _engine.ListSegments().OrderByDescending(s => s.Id.Value).First().UncompressedBytes;
        }
        return (merges, written);
    }

    private List<SegmentInfo> SegmentsOf(LogLevel level) =>
        _engine.ListSegments().Where(s => s.MinLevel == level).OrderBy(s => s.MinTimestampTicks).ToList();

    // ── 1. A bucket compacts toward one segment ───────────────────────────────

    /// <summary>
    /// Three settled Debug buckets end as exactly three files, one per bucket. Not "fewer
    /// files": the count is the number of buckets, which is what makes catalog size a function
    /// of the retention window instead of uptime.
    ///
    /// <para>Debug's bucket is SIX HOURS, not a day — its 3-day TTL divided by
    /// <c>MergeSpanTtlDivisor</c>. The old whole-day floor gave Debug rows a fourth day of life
    /// while the divisor advertised 8.3 %, on the level that is usually the largest on
    /// disk.</para>
    /// </summary>
    [Fact]
    public async Task EachBucketCollapsesToOneSegment()
    {
        long width = BucketTicks(LogLevel.Debug);
        Assert.Equal(6 * TimeSpan.TicksPerHour, width);

        long b0 = BucketStart(LogLevel.Debug, DateTime.UtcNow.Ticks - 10 * TimeSpan.TicksPerDay);
        for (int b = 0; b < 3; b++)
            for (int f = 0; f < 5; f++)
                await FlushAsync(60, LogLevel.Debug, b0 + b * width + f * TimeSpan.TicksPerHour);

        Assert.Equal(15, _engine.ListSegments().Count);
        var before = ReadEverything();
        Assert.Equal(900, before.Count);

        var (merges, _) = await CompactToExhaustionAsync();

        var segs = SegmentsOf(LogLevel.Debug);
        Assert.Equal(3, merges);
        Assert.Equal(3, segs.Count);
        for (int b = 0; b < 3; b++)
        {
            Assert.Equal(300u, segs[b].EventCount);
            Assert.Equal(b0 + b * width, BucketStart(LogLevel.Debug, segs[b].MinTimestampTicks));
            Assert.True(segs[b].MaxTimestampTicks - segs[b].MinTimestampTicks < width);
        }

        var after = ReadEverything();
        Assert.Equal(before.Count, after.Count);
        for (int i = 0; i < before.Count; i++)
        {
            Assert.Equal(before[i].Id,      after[i].Id);
            Assert.Equal(before[i].TsTicks, after[i].TsTicks);
            Assert.Equal(before[i].Props ?? [], after[i].Props ?? []);
        }
    }

    // ── 2. A sealed bucket is never rewritten again ───────────────────────────

    /// <summary>
    /// The terminal state has to actually terminate. Once a bucket has collapsed, further passes
    /// must report nothing to do and leave the file byte-identical — a planner that kept
    /// re-selecting its own output would rewrite the same day forever at whatever the maintenance
    /// cadence is.
    /// </summary>
    [Fact]
    public async Task ASealedBucketIsNotRewrittenAgain()
    {
        long day0 = BucketStart(LogLevel.Debug, DateTime.UtcNow.Ticks - 10 * TimeSpan.TicksPerDay);
        for (int f = 0; f < 6; f++)
            await FlushAsync(50, LogLevel.Debug, day0 + f * 30 * TimeSpan.TicksPerMinute);

        var (merges, _) = await CompactToExhaustionAsync();
        Assert.Equal(1, merges);

        var merged   = _engine.ListSegments().Single();
        var stamp    = File.GetLastWriteTimeUtc(merged.FilePath);
        var length   = new FileInfo(merged.FilePath).Length;

        for (int pass = 0; pass < 5; pass++)
            Assert.False(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));

        var still = _engine.ListSegments().Single();
        Assert.Equal(merged.Id.Value, still.Id.Value);
        Assert.Equal(merged.FilePath, still.FilePath);
        Assert.Equal(stamp,  File.GetLastWriteTimeUtc(still.FilePath));
        Assert.Equal(length, new FileInfo(still.FilePath).Length);
    }

    // ── 3. The span bound is a fraction of the level's own TTL ────────────────

    /// <summary>
    /// The same seven daily flushes, at two levels, produce a different number of files — and
    /// the difference is the levels' TTLs, nothing else. A 90-day Fatal is entitled to a 7-day
    /// span (one twelfth of its TTL: its oldest row over-retains by 8.3 %), so seven near-empty
    /// days become ONE file. Debug's 3-day TTL floors at a single day, so it keeps seven.
    ///
    /// <para>This is the whole argument for making the bound a fraction rather than a flat 24 h:
    /// a rare level otherwise pays a full index and catalog entry per day for a handful of
    /// rows. It cuts BOTH ways — Debug's 3-day TTL earns a 6 h bucket, not a whole day, so its
    /// rows die on day 3 as the policy says rather than on day 4.</para>
    /// </summary>
    [Fact]
    public async Task TheSpanBoundIsAFractionOfTheLevelsOwnTtl()
    {
        Assert.Equal(7 * TimeSpan.TicksPerDay,   BucketTicks(LogLevel.Fatal));
        Assert.Equal(6 * TimeSpan.TicksPerHour,  BucketTicks(LogLevel.Debug));

        // One Fatal bucket, comfortably past its grace period.
        long start = BucketStart(LogLevel.Fatal, DateTime.UtcNow.Ticks - 20 * TimeSpan.TicksPerDay);
        for (int d = 0; d < 7; d++)
        {
            long t = start + d * TimeSpan.TicksPerDay + TimeSpan.TicksPerHour;
            Write(4,  LogLevel.Fatal, t);
            Write(20, LogLevel.Debug, t);
            await _engine.FlushHotTierAsync();     // one segment per level, seven rounds
        }
        Assert.Equal(14, _engine.ListSegments().Count);

        await CompactToExhaustionAsync();

        var fatal = SegmentsOf(LogLevel.Fatal);
        var debug = SegmentsOf(LogLevel.Debug);

        Assert.Single(fatal);
        Assert.Equal(28u, fatal[0].EventCount);
        long span = fatal[0].MaxTimestampTicks - fatal[0].MinTimestampTicks;
        Assert.True(span <= BucketTicks(LogLevel.Fatal), $"Fatal segment spans {span / TimeSpan.TicksPerDay} days");
        Assert.True(span <= RetentionPolicy.Default.GetTtl(LogLevel.Fatal).Ticks / 12);

        Assert.Equal(7, debug.Count);   // one flush per bucket, nothing to combine
        Assert.All(debug, s => Assert.True(
            s.MaxTimestampTicks - s.MinTimestampTicks < BucketTicks(LogLevel.Debug)));

        _out.WriteLine($"7 rare-level days: Fatal {fatal.Count} file(s), Debug {debug.Count} file(s)");
    }

    /// <summary>The bound is a HARD one — no merged segment may exceed its level's bucket width.</summary>
    [Fact]
    public async Task NoMergedSegmentOutlivesItsSpanBound()
    {
        long start = BucketStart(LogLevel.Fatal, DateTime.UtcNow.Ticks - 40 * TimeSpan.TicksPerDay);
        var  levels = new[] { LogLevel.Debug, LogLevel.Information, LogLevel.Warning, LogLevel.Error, LogLevel.Fatal };
        for (int d = 0; d < 20; d++)
        {
            long t = start + d * TimeSpan.TicksPerDay + 2 * TimeSpan.TicksPerHour;
            foreach (var lvl in levels) Write(25, lvl, t);
            await _engine.FlushHotTierAsync();
        }

        await CompactToExhaustionAsync();

        foreach (var s in _engine.ListSegments())
        {
            long span  = s.MaxTimestampTicks - s.MinTimestampTicks;
            long bound = BucketTicks(s.MinLevel);
            Assert.True(span <= bound,
                $"{s.MinLevel} segment spans {span / (double)TimeSpan.TicksPerDay:F2} d, bound {bound / TimeSpan.TicksPerDay} d");
        }
    }

    /// <summary>
    /// A bucket with more payload than one file may hold has to be cut, and it is cut by TIME:
    /// the pieces partition the bucket instead of interleaving it. Interleaved pieces each span
    /// the whole bucket, so every query into it opens all of them and every one of them carries
    /// the bucket's newest timestamp — which means the oldest events over-retain by the full
    /// bucket width rather than by one file's share of it.
    /// </summary>
    [Fact]
    public async Task ABucketTooBigForOneFileIsCutByTime()
    {
        _engine._mergeTargetPayloadBytes = 256 * 1024;

        long start = BucketStart(LogLevel.Information, DateTime.UtcNow.Ticks - 20 * TimeSpan.TicksPerDay);
        for (int f = 0; f < 14; f++)
            await FlushAsync(120, LogLevel.Information, start + f * 12 * TimeSpan.TicksPerHour, padBytes: 512);

        await CompactToExhaustionAsync();

        var segs = _engine.ListSegments().OrderBy(s => s.MinTimestampTicks).ToList();
        Assert.True(segs.Count >= 3, $"target too large to force a cut — only {segs.Count} file(s)");
        for (int i = 1; i < segs.Count; i++)
            Assert.True(segs[i].MinTimestampTicks >= segs[i - 1].MaxTimestampTicks,
                $"segments {i - 1} and {i} overlap in time — the bucket was interleaved, not partitioned");
        Assert.Equal(1680, ReadEverything().Count);
    }

    // ── 4. Today's bucket keeps working ───────────────────────────────────────

    /// <summary>
    /// The open bucket is where a naive "one file per day" falls over: it would rewrite the
    /// day-so-far every time a flush lands. A few flushes must therefore merge NOTHING, the merge
    /// that eventually fires must not be followed by another one for the next single flush, and
    /// every event must stay readable the whole way through.
    /// </summary>
    [Fact]
    public async Task TodaysBucketStaysQueryable_AndAFlushDoesNotForceARewrite()
    {
        // THE OPEN BUCKET, by construction. "UtcNow - 2h" is not a position on the grid: the
        // eight flushes cover 420 s of event time, and when a 7-day boundary falls inside that
        // window they split across two buckets. Both are open — the older one ended less than
        // 2 h ago, far inside the 48 h seal grace, so it keeps the fanout of 8 rather than
        // dropping to 2 — and neither side reaches 8, so nothing merges and the assertion below
        // fails. Measured on a real engine at this staging: merge=False, 8 files left.
        long today = MergeBucketGrid.OpenBucketStart(LogLevel.Information);

        for (int f = 0; f < 4; f++)
        {
            await FlushAsync(100, LogLevel.Information, today + f * TimeSpan.TicksPerMinute);
            Assert.False(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        }
        Assert.Equal(4, _engine.ListSegments().Count);
        Assert.Equal(400, ReadEverything().Count);          // queryable while uncompacted

        for (int f = 4; f < 8; f++)
            await FlushAsync(100, LogLevel.Information, today + f * TimeSpan.TicksPerMinute);

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        Assert.Single(_engine.ListSegments());
        Assert.Equal(800, ReadEverything().Count);

        // One more flush must NOT drag the merged file through a rewrite.
        await FlushAsync(100, LogLevel.Information, today + 9 * TimeSpan.TicksPerMinute);
        Assert.False(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        Assert.Equal(2, _engine.ListSegments().Count);
        Assert.Equal(900, ReadEverything().Count);
    }

    /// <summary>
    /// One near-empty flush segment — a quiet minute in an otherwise busy hour — must not gate
    /// its bucket: the nine real files behind it merge on schedule.
    ///
    /// <para>What it must ALSO not do is get dragged into that merge, and that is the change
    /// here. The bucket ends with TWO files, not one, and the second is the one-event segment
    /// left exactly where it was. "A bucket always collapses to exactly one file" is the goal
    /// that made a straggler cost a full bucket rewrite — the same rule that lets a 300-byte
    /// file join a 3.6 MB batch is the rule that let one late row rewrite a collapsed 1.4 MB
    /// bucket five times over. The one-event file is 40 KB of index and catalog overhead; a
    /// rewrite of everything around it is not worth paying to avoid that, and paying it once
    /// does not even end — the rewritten file stays a candidate for the next straggler. So the
    /// tiny file waits for other tiny files, and the bucket's terminal state is "one file per
    /// size tier" rather than "one file".</para>
    /// </summary>
    [Fact]
    public async Task ANearEmptyFlushSegmentDoesNotGateItsBucket()
    {
        // On the grid — see TodaysBucketStaysQueryable. Ten flushes of 400 events at 1 s spacing
        // cover 939 s, and a boundary inside that window splits them across two open buckets.
        // Measured at this staging: the upper bucket's 8 sources merge to 3200 and the lower
        // two are stranded, so the bucket ends with THREE files and the 3600 below is 3200.
        long today = MergeBucketGrid.OpenBucketStart(LogLevel.Information);
        await FlushAsync(1, LogLevel.Information, today);                       // the quiet minute
        for (int f = 1; f < 10; f++)
            await FlushAsync(400, LogLevel.Information, today + f * TimeSpan.TicksPerMinute, padBytes: 256);

        var sizes = _engine.ListSegments().Select(s => s.UncompressedBytes).OrderBy(b => b).ToList();
        Assert.Equal(10, sizes.Count);
        Assert.True(sizes[^1] > sizes[0] * 8, "test is meaningless unless the outlier is beyond the ratio");

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));

        var after = _engine.ListSegments().OrderBy(s => s.EventCount).ToList();
        Assert.Equal(2, after.Count);
        Assert.Equal(1u,    after[0].EventCount);      // the quiet minute, untouched
        Assert.Equal(3600u, after[1].EventCount);      // the nine real flushes, merged
        Assert.Equal(3601, ReadEverything().Count);

        // And it stays that way: a second pass has nothing legal to do.
        Assert.False(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
    }

    /// <summary>
    /// Write amplification, measured rather than asserted in prose: flushes arrive in waves and
    /// compaction runs to exhaustion after each, exactly as the maintenance loop does. The
    /// size-ratio rule keeps a batch's cost proportional to the data it consumes, so the bytes
    /// the merges write stay a small multiple of the bytes ingested — where taking the whole
    /// bucket every time would be quadratic in the day's size.
    /// </summary>
    [Fact]
    public async Task OpenBucketAmplificationStaysBounded()
    {
        // On the grid, so the figure is measured on ONE open bucket every run. A straddle here
        // does not breach the guard — splitting the waves means fewer merges, so the ratio FALLS
        // (measured 0.95x against the 3.0x ceiling) — but an amplification number whose bucket
        // structure depends on the time of day is a weaker measurement than the same number
        // taken on a fixed shape, and the ceiling is only meaningful against a fixed shape.
        long today = MergeBucketGrid.OpenBucketStart(LogLevel.Information);
        int  merges = 0;
        long written = 0, flushed = 0;

        for (int wave = 0; wave < 5; wave++)
        {
            for (int f = 0; f < 8; f++)
                await FlushAsync(200, LogLevel.Information, today + (wave * 8 + f) * TimeSpan.TicksPerMinute, padBytes: 256);
            flushed = _engine.ListSegments().Sum(s => s.UncompressedBytes);   // includes merged files

            var pass = await CompactToExhaustionAsync();
            merges  += pass.Merges;
            written += pass.BytesWritten;
        }

        long ingested = _engine.ListSegments().Sum(s => s.UncompressedBytes);
        double amp = written / (double)ingested;
        _out.WriteLine($"40 flushes, {merges} merges, {written / 1024} KB written for {ingested / 1024} KB of payload — {amp:F2}x");

        Assert.Equal(8000, ReadEverything().Count);
        Assert.True(amp < 3.0, $"write amplification {amp:F2}x — the size-ratio rule is not holding");
        Assert.True(flushed > 0);
    }

    // ── 5. Retention still deletes exactly the right files ────────────────────

    /// <summary>
    /// Compaction must not move any event's deadline. Merged files stay level-pure, so a
    /// 3-day-TTL Debug bucket expires whole and a 90-day Error bucket beside it — same days, same
    /// merge passes — loses nothing.
    /// </summary>
    [Fact]
    public async Task RetentionDeletesExactlyTheExpiredBuckets()
    {
        long width = BucketTicks(LogLevel.Debug);
        long day0  = BucketStart(LogLevel.Debug, DateTime.UtcNow.Ticks - 12 * TimeSpan.TicksPerDay);
        for (int b = 0; b < 3; b++)
            for (int f = 0; f < 4; f++)
            {
                long t = day0 + b * width + f * TimeSpan.TicksPerHour;
                Write(30, LogLevel.Debug, t);
                Write(30, LogLevel.Error, t);
                await _engine.FlushHotTierAsync();
            }

        await CompactToExhaustionAsync();
        var errorsBefore = SegmentsOf(LogLevel.Error);
        Assert.Equal(3, SegmentsOf(LogLevel.Debug).Count);   // 3 Debug buckets, 6 h each
        Assert.NotEmpty(errorsBefore);

        var result = await _engine.EnforceRetentionAsync();

        Assert.Empty(SegmentsOf(LogLevel.Debug));            // 3-day TTL, 12 days old
        Assert.Equal(3, result.DeletedSegments);
        Assert.Equal(errorsBefore.Select(s => s.Id.Value).OrderBy(v => v),
                     SegmentsOf(LogLevel.Error).Select(s => s.Id.Value).OrderBy(v => v));
        Assert.Equal(360, ReadEverything().Count);           // every Error event still served
        foreach (var s in errorsBefore) Assert.True(File.Exists(s.FilePath));
    }
}
