using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit.Abstractions;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// The shapes <see cref="SegmentBucketCompactionTests"/> does not construct, and which the
/// (bucket, level) policy got wrong: a straggler landing in a bucket that has already collapsed,
/// a flush segment whose Min and Max fall either side of a bucket boundary, an open bucket run
/// long enough for its files to outgrow the batch budget, and a long run of all three together.
///
/// <para>Every one of these was a MEASURED failure before the size-tiered run planner:</para>
/// <list type="bullet">
/// <item>a collapsed 1430 KB sealed bucket was rewritten IN FULL for every one-event straggler —
///       5 stragglers, 5 merges, 7151 KB written, and the cost did not decay;</item>
/// <item>six flush segments straddling a bucket boundary produced 0 merges and kept a 32.2-day
///       span against a 7-day bound;</item>
/// <item>an open bucket whose files had outgrown <c>target / MergeMinSources</c> stopped merging
///       entirely — 40 segments, 0 merges;</item>
/// <item>write amplification of the open bucket was 2.06× at 80 flushes, 3.20× at 160 and 3.34×
///       at 400 — a staircase, still climbing, past the policy's own <c>amp &lt; 3.0</c> guard.</item>
/// </list>
/// </summary>
public sealed class SegmentBucketAdversarialProbe : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-adv-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private StorageEngine _engine = null!;

    public SegmentBucketAdversarialProbe(ITestOutputHelper output) => _out = output;

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = _dir }),
            new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance)
        {
            _allowIndexlessMerge = true,
        };
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private static long BucketTicks(LogLevel level) =>
        StorageEngine.MergeBucketTicks(RetentionPolicy.Default.GetTtl(level));

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

    private async Task<(int Merges, long BytesWritten)> CompactToExhaustionAsync(int cap = 500)
    {
        int merges = 0; long written = 0;
        while (await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None))
        {
            merges++;
            Assert.True(merges < cap, "compaction did not converge");
            written += _engine.ListSegments().OrderByDescending(s => s.Id.Value).First().UncompressedBytes;
        }
        return (merges, written);
    }

    private int ServedEvents()
    {
        var dedup = new Dictionary<string, string>(StringComparer.Ordinal);
        int n = 0;
        foreach (var seg in _engine.ListSegments())
        {
            using var r = SegmentReader.Open(seg.FilePath);
            n += r.ReadAllRaw(dedup).Count;
        }
        return n;
    }

    /// <summary>Identity of a file on disk, so "was it rewritten" is answerable byte for byte.</summary>
    private static (ulong Id, long Length, DateTime Written) Fingerprint(SegmentInfo s) =>
        (s.Id.Value, new FileInfo(s.FilePath).Length, File.GetLastWriteTimeUtc(s.FilePath));

    // ── 1. A straggler costs the straggler ────────────────────────────────────

    /// <summary>
    /// THE BLOCKER. A straggler lands in a bucket that collapsed days ago. The bucket is sealed,
    /// and the previous planner dropped the size ratio for a sealed bucket, so a 2-source batch
    /// of "one event plus the bucket's whole collapsed file" was legal — and stayed legal,
    /// because the rewritten file was still under the maximal threshold. Five one-event flushes
    /// cost five merges and 7151 KB of writes.
    ///
    /// <para>The size ratio now applies to sealed buckets too, so the collapsed file is not a
    /// legal partner for anything 800× smaller. The stragglers coalesce among THEMSELVES, which
    /// is the late-arrival segment the design wanted, obtained without a second code path.</para>
    /// </summary>
    [Fact]
    public async Task AStragglerCostsTheStragglerNotTheBucket()
    {
        long day0 = BucketStart(LogLevel.Debug, DateTime.UtcNow.Ticks - 10 * TimeSpan.TicksPerDay);
        for (int f = 0; f < 6; f++)
            await FlushAsync(400, LogLevel.Debug, day0 + f * 30 * TimeSpan.TicksPerMinute, padBytes: 512);

        await CompactToExhaustionAsync();
        var collapsed = _engine.ListSegments().Single();
        var before    = Fingerprint(collapsed);

        long rewritten = 0;
        int  merges    = 0;
        for (int late = 0; late < 5; late++)
        {
            await FlushAsync(1, LogLevel.Debug, day0 + 2 * TimeSpan.TicksPerHour + late);
            var pass = await CompactToExhaustionAsync();
            merges    += pass.Merges;
            rewritten += pass.BytesWritten;
        }

        _out.WriteLine($"collapsed bucket {collapsed.UncompressedBytes / 1024} KB; " +
                       $"5 one-event stragglers => {merges} merge(s), {rewritten} B rewritten");

        // The collapsed file is the same file, byte for byte — never a merge source.
        var still = _engine.ListSegments().Single(s => s.Id.Value == collapsed.Id.Value);
        Assert.Equal(before, Fingerprint(still));

        // And what WAS rewritten is proportional to the stragglers, not to the bucket.
        Assert.True(rewritten < collapsed.UncompressedBytes / 100,
            $"{rewritten} B rewritten for 5 one-event stragglers against a {collapsed.UncompressedBytes} B bucket");
        Assert.Equal(2405, ServedEvents());
    }

    /// <summary>
    /// The same shape without a contrived flush: one late Fatal row inside a live tier of
    /// Information. The level-split flush writes it as a Fatal segment of its own whose Max is
    /// back in the old bucket — which is exactly what used to make it a merge source for that
    /// bucket's collapsed file.
    /// </summary>
    [Fact]
    public async Task OneLateFatalRowDoesNotRewriteTheFatalBucket()
    {
        long day0 = BucketStart(LogLevel.Fatal, DateTime.UtcNow.Ticks - 30 * TimeSpan.TicksPerDay);
        for (int f = 0; f < 8; f++)
            await FlushAsync(300, LogLevel.Fatal, day0 + f * TimeSpan.TicksPerHour, padBytes: 512);
        await CompactToExhaustionAsync();
        var collapsed = _engine.ListSegments().Single();
        var before    = Fingerprint(collapsed);

        long rewritten = 0;
        int  merges    = 0;
        for (int late = 0; late < 4; late++)
        {
            Write(200, LogLevel.Information, DateTime.UtcNow.Ticks - 10 * TimeSpan.TicksPerMinute, padBytes: 256);
            Write(1,   LogLevel.Fatal,       day0 + 3 * TimeSpan.TicksPerHour + late);
            await _engine.FlushHotTierAsync();
            var pass = await CompactToExhaustionAsync();
            merges    += pass.Merges;
            rewritten += pass.BytesWritten;
        }

        _out.WriteLine($"collapsed Fatal bucket {collapsed.UncompressedBytes / 1024} KB; " +
                       $"4 single-row stragglers => {merges} merge(s), {rewritten} B rewritten");

        Assert.Equal(before, Fingerprint(_engine.ListSegments().Single(s => s.Id.Value == collapsed.Id.Value)));
        Assert.True(rewritten < collapsed.UncompressedBytes / 100, $"{rewritten} B rewritten for 4 late rows");
        Assert.Equal(2400 + 4 + 800, ServedEvents());
    }

    // ── 2. A straddler has a terminal state ───────────────────────────────────

    /// <summary>
    /// A flush segment whose oldest row is 30 days late spans from that row to now. Under
    /// Min-bucketing it belonged to the OLD bucket while its span was five times the bucket
    /// width, so the planner's span guard rejected every partner it could have had — including
    /// other straddlers of the same shape. Measured: 6 such segments, 0 merges, 32.2-day spans.
    ///
    /// <para>A segment's bucket is now <c>floor(MaxTimestamp / width)</c>, so a straddler
    /// belongs with the data it was flushed beside and compacts with it normally. Its span is
    /// still 30 days — that is the lateness of the row, which no merge can undo — but the
    /// bucket TERMINATES, and the thing the span guard was protecting (a row's deadline moving)
    /// is now guaranteed directly: every source's Max is inside one width window.</para>
    /// </summary>
    [Fact]
    public async Task StraddlingSegmentsReachATerminalState()
    {
        long w   = BucketTicks(LogLevel.Information);
        long b   = BucketStart(LogLevel.Information, DateTime.UtcNow.Ticks - 30 * TimeSpan.TicksPerDay);
        long now = DateTime.UtcNow.Ticks;

        for (int f = 0; f < 8; f++)
        {
            Write(1,  LogLevel.Information, b + TimeSpan.TicksPerHour + f);
            Write(50, LogLevel.Information, now - (60 - f) * TimeSpan.TicksPerMinute);
            await _engine.FlushHotTierAsync();
        }
        var deadlines = _engine.ListSegments().Select(s => s.MaxTimestampTicks).ToList();

        var pass  = await CompactToExhaustionAsync();
        var after = _engine.ListSegments();
        double maxSpan = after.Max(s => (s.MaxTimestampTicks - s.MinTimestampTicks) / (double)TimeSpan.TicksPerDay);
        _out.WriteLine($"8 straddlers, bucket width {w / TimeSpan.TicksPerDay} d: {pass.Merges} merge(s), " +
                       $"{after.Count} file(s), largest span {maxSpan:F1} d");

        Assert.Equal(1, pass.Merges);
        Assert.Single(after);
        Assert.Equal(408, ServedEvents());

        // The property the span guard was standing in for: no row's expiry moved by more than
        // one bucket width. That holds for a straddler where "span <= width" never could.
        long merged = after[0].MaxTimestampTicks;
        Assert.All(deadlines, d => Assert.True(merged - d < w, $"deadline moved by {merged - d} ticks"));
    }

    /// <summary>
    /// A size outlier in the middle of a bucket. The contiguous planner STOPS at it rather than
    /// stepping over it (see <see cref="MergeRunPlannerTests.AContiguousRunStopsAtTheFirstFileItCannotTake"/>,
    /// which is where that decision stays observable) — but once it is exhausted the tier
    /// fallback merges the small files on both sides, and the file it produces DOES span the
    /// outlier. That overlap is deliberate and it is the price of a terminal state: without it
    /// this bucket, and every bucket a bursty producer writes, keeps one file per flush forever.
    ///
    /// <para>What is bounded is how much overlap: files inside one size tier never overlap each
    /// other, because a tier's run is still time-contiguous, so at any instant a query opens at
    /// most one file PER TIER. Here that is 2, against 5 flushes and against the unbounded count
    /// the contiguous-only rule produced.</para>
    /// </summary>
    [Fact]
    public async Task OverlapCostsOneFilePerSizeTierNotOnePerFlush()
    {
        long b = BucketStart(LogLevel.Information, DateTime.UtcNow.Ticks - 30 * TimeSpan.TicksPerDay);

        // A size outlier in the middle: two small flushes, one 20× larger, two more small.
        await FlushAsync(50,   LogLevel.Information, b + 1 * TimeSpan.TicksPerHour);
        await FlushAsync(50,   LogLevel.Information, b + 2 * TimeSpan.TicksPerHour);
        await FlushAsync(1200, LogLevel.Information, b + 3 * TimeSpan.TicksPerHour, padBytes: 512);
        await FlushAsync(50,   LogLevel.Information, b + 5 * TimeSpan.TicksPerHour);
        await FlushAsync(50,   LogLevel.Information, b + 6 * TimeSpan.TicksPerHour);

        await CompactToExhaustionAsync();

        var segs  = _engine.ListSegments();
        int tiers = segs.Select(s => StorageEngine.SizeTier(Math.Max(s.UncompressedBytes, s.CompressedBytes)))
                        .Distinct().Count();
        _out.WriteLine($"{segs.Count} file(s) in {tiers} size tier(s) after compacting a bucket " +
                       $"with a size outlier in the middle; deepest overlap {DeepestOverlap(segs)}");

        // No two files of one tier overlap: inside a tier the run is contiguous.
        foreach (var group in segs.GroupBy(s => StorageEngine.SizeTier(Math.Max(s.UncompressedBytes, s.CompressedBytes))))
        {
            var inTier = group.OrderBy(s => s.MinTimestampTicks).ToList();
            for (int i = 1; i < inTier.Count; i++)
                Assert.True(inTier[i].MinTimestampTicks >= inTier[i - 1].MaxTimestampTicks,
                    $"two files of tier {group.Key} overlap — a tier's run is not contiguous");
        }
        Assert.True(DeepestOverlap(segs) <= tiers,
            $"{DeepestOverlap(segs)} files cover one instant, above the {tiers}-tier bound");
        Assert.Equal(1400, ServedEvents());
    }

    /// <summary>Most files covering any single instant — what a point query has to open.</summary>
    private static int DeepestOverlap(IReadOnlyList<SegmentInfo> segs)
    {
        int deepest = 0;
        foreach (var probe in segs)
        {
            int n = 0;
            foreach (var s in segs)
                if (s.MinTimestampTicks <= probe.MinTimestampTicks && probe.MinTimestampTicks <= s.MaxTimestampTicks) n++;
            if (n > deepest) deepest = n;
        }
        return deepest;
    }

    // ── 2b. A terminal state under NON-UNIFORM flush sizes ────────────────────

    /// <summary>
    /// Files present, per size tier, so a terminal state can be stated as a bound rather than a
    /// number that happened to come out.
    /// </summary>
    private int DistinctSizeTiers() =>
        _engine.ListSegments()
               .Select(s => StorageEngine.SizeTier(Math.Max(s.UncompressedBytes, s.CompressedBytes)))
               .Distinct().Count();

    /// <summary>
    /// THE SECOND BLOCKER. Every convergence test before this one drove UNIFORM flush volumes,
    /// and uniform sizes are the one distribution under which time-contiguity and the size ratio
    /// never disagree. Give adjacent flushes a 5× size difference — which a producer does by
    /// itself, since the flush fires on a 5-minute timer and its size is simply the traffic in
    /// that window — and the contiguous rule strands every file between two neighbours it cannot
    /// take. MEASURED at 30b0d93: 240 flushes into a bucket sealed 30 days ago produced 240
    /// files and ZERO merges, at fixpoint after every flush. The pre-fix commit 37c4521 produced
    /// 1 file. So the shape went from "converges, too expensively" to "never converges".
    ///
    /// <para>The bound asserted here is the one the size ladder gives, and it holds for ANY size
    /// distribution: at fixpoint a tier holds fewer files than the fanout (three same-tier files
    /// always satisfy the growth rule, since the largest is under
    /// <see cref="StorageEngine.MergeRunSizeRatio"/> of the smallest), so a sealed bucket holds
    /// at most 2 files per tier and the tier count is <c>log₄(bucket bytes / flush bytes)</c> —
    /// a function of the geometry, never of uptime.</para>
    /// </summary>
    [Fact]
    public async Task ASealedBucketConvergesWhateverTheFlushSizeDistribution()
    {
        long b = BucketStart(LogLevel.Information, DateTime.UtcNow.Ticks - 30 * TimeSpan.TicksPerDay);
        var  files = new Dictionary<int, int>();
        int  expected = 0, merges = 0;

        for (int f = 0; f < 240; f++)
        {
            int n = f % 2 == 0 ? 200 : 40;        // 5× between adjacent flushes, above the ratio
            expected += n;
            await FlushAsync(n, LogLevel.Information, b + f * 20 * TimeSpan.TicksPerMinute, padBytes: 256);
            merges += (await CompactToExhaustionAsync()).Merges;
            if (f + 1 is 40 or 80 or 160 or 240) files[f + 1] = _engine.ListSegments().Count;
        }

        _out.WriteLine($"alternating 200/40 events, SEALED bucket, compacted after every flush: " +
                       string.Join(", ", files.Select(kv => $"{kv.Key} flushes -> {kv.Value} file(s)")) +
                       $" in {merges} merges");

        // Was 40 / 80 / 160 / 240 — one file per flush, and 0 merges throughout.
        Assert.True(files[240] <= files[40],
            $"file count grows with uptime: {files[40]} at 40 flushes, {files[240]} at 240");
        Assert.True(files[240] <= MergeSealedFanout * DistinctSizeTiers(),
            $"{files[240]} files across {DistinctSizeTiers()} size tier(s) — above the ladder bound");
        Assert.Equal(expected, ServedEvents());
    }

    private const int MergeSealedFanout = 2;   // StorageEngine.MergeSealedMinSources

    /// <summary>
    /// The same failure on the shape a real producer makes without trying: a quiet baseline with
    /// a burst every twentieth flush — a deploy, an error storm, or just diurnal traffic against
    /// a fixed flush cadence. MEASURED at 30b0d93 in a bucket sealed 30 days ago: 6 / 12 / 24 /
    /// 48 / 96 files at 40 / 80 / 160 / 320 / 640 flushes. Exactly 0.15 files per flush, linear,
    /// forever, with the planner at fixpoint after every one of them — the bucket settled into a
    /// repeating (455 KB, 69 KB, 13 KB) triple whose adjacent ratios, 6.6 and 5.3, both exceed
    /// the size ratio, so no contiguous run of length 2 existed anywhere in it.
    ///
    /// <para>The amplification that shape reported — a flat 1.95× — read well only because
    /// almost nothing merged. That is why file count is guarded here and not just bytes.</para>
    /// </summary>
    [Fact]
    public async Task ABurstyProducerDoesNotGrowTheCatalogWithUptime()
    {
        long b = BucketStart(LogLevel.Information, DateTime.UtcNow.Ticks - 30 * TimeSpan.TicksPerDay);
        var  files = new Dictionary<int, int>();
        int  expected = 0;

        for (int f = 0; f < 640; f++)
        {
            int n = f % 20 == 19 ? 800 : 40;
            expected += n;
            await FlushAsync(n, LogLevel.Information, b + f * 10 * TimeSpan.TicksPerMinute, padBytes: 256);
            await CompactToExhaustionAsync();
            if (f + 1 is 40 or 80 or 160 or 320 or 640) files[f + 1] = _engine.ListSegments().Count;
        }

        _out.WriteLine("bursty 40/800 events, SEALED bucket: " +
                       string.Join(", ", files.Select(kv => $"{kv.Key}->{kv.Value}")) +
                       $"; {DistinctSizeTiers()} size tier(s)");

        // 16× the flushes must not cost 16× the files. Was 6 -> 96.
        Assert.True(files[640] <= files[40] * 2,
            $"file count tracks uptime: {files[40]} at 40 flushes, {files[640]} at 640");
        Assert.True(files[640] <= MergeSealedFanout * DistinctSizeTiers(),
            $"{files[640]} files across {DistinctSizeTiers()} size tier(s) — above the ladder bound");
        Assert.Equal(expected, ServedEvents());
    }

    /// <summary>
    /// And the same in the bucket every deployment always has: the OPEN one, at wall clock.
    /// Sealing was never the mechanism — the size ratio blocked the run, and the fanout, which is
    /// all sealing lowers, was not what stood in the way — so the open bucket failed identically
    /// and had to wait days for a seal that would not have helped. MEASURED at 30b0d93 with
    /// flush volumes strictly alternating 25 and 800 events: 3000 flushes, 0 merges, 3000 files.
    /// </summary>
    [Fact]
    public async Task AnOpenBucketConvergesWhateverTheFlushSizeDistribution()
    {
        long today = DateTime.UtcNow.Ticks - 20 * TimeSpan.TicksPerHour;
        int  expected = 0;

        for (int f = 0; f < 600; f++)
        {
            int n = f % 2 == 0 ? 25 : 800;
            expected += n;
            await FlushAsync(n, LogLevel.Information, today + f * TimeSpan.TicksPerMinute, padBytes: 256);
            await CompactToExhaustionAsync();
        }

        var files = _engine.ListSegments().Count;
        long allowance = StorageEngine.MergeMinSources * (long)DistinctSizeTiers();
        _out.WriteLine($"alternating 25/800 events, OPEN bucket: 600 flushes -> {files} file(s) " +
                       $"across {DistinctSizeTiers()} size tier(s)");

        // An open bucket's fanout is 8, so it may hold up to 7 files per tier in flight — that is
        // the ladder's allowance, and it is a function of the size range, not of the flush count.
        Assert.True(files <= allowance, $"{files} files, above the {allowance}-file ladder allowance");
        Assert.Equal(expected, ServedEvents());
    }

    // ── 3. The batch budget must not stall a bucket ───────────────────────────

    /// <summary>
    /// <c>MergeMinSources</c> used to be tested against the batch AFTER the payload budget had
    /// cut it, so an open bucket whose files had grown past <c>target / 8</c> could never
    /// assemble a legal batch again — measured, 40 segments produced 0 merges. At the shipped
    /// 512 MB target that is any file over 64 MB; the bucket then waited for its seal (up to 9
    /// days for a 90-day level) to consolidate.
    ///
    /// <para>A run that fills the target now merges whatever its source count, because the file
    /// it produces is MAXIMAL — the last rewrite those bytes will ever get.</para>
    /// </summary>
    [Fact]
    public async Task AnOpenBucketKeepsCompactingPastTheBatchBudget()
    {
        long today = DateTime.UtcNow.Ticks - 20 * TimeSpan.TicksPerHour;
        await FlushAsync(200, LogLevel.Information, today, padBytes: 256);
        long one = _engine.ListSegments().Single().UncompressedBytes;
        _engine._mergeTargetPayloadBytes = one * 5;   // five sources fill the budget, eight are required

        for (int f = 1; f < 40; f++)
            await FlushAsync(200, LogLevel.Information, today + f * TimeSpan.TicksPerMinute, padBytes: 256);

        var pass  = await CompactToExhaustionAsync();
        var after = _engine.ListSegments();
        _out.WriteLine($"flush segment {one / 1024} KB, target {one * 5 / 1024} KB: " +
                       $"{pass.Merges} merge(s), {after.Count} of 40 file(s) left");

        Assert.True(pass.Merges >= 8, $"{pass.Merges} merges — the bucket is still stalling");
        Assert.True(after.Count <= 12, $"{after.Count} files left of 40");
        // Each output is at or past maximal, so it is out of the candidate set for good — which
        // is why a run that fills the target is worth merging however few sources it has.
        Assert.All(after, s => Assert.True(s.UncompressedBytes >= one * 5 / 2,
            $"a {s.UncompressedBytes} B file survived below the {one * 5 / 2} B maximal"));
        Assert.Equal(8000, ServedEvents());
    }

    // ── 4. Amplification over a long run, with stragglers ─────────────────────

    /// <summary>
    /// WRITE AMPLIFICATION, measured as a BAND over a long run rather than as a point, because a
    /// point taken at one checkpoint is a sample of an oscillation and the previous one happened
    /// to land at its bottom: the identical workload read 2.92× at 1000 flushes, which was
    /// published as the steady state and guarded at <c>&lt; 3.2</c>, and 3.30× at 4000, which
    /// breached that guard on its own data.
    ///
    /// <para>The claim of 1.40× before that was one step of a staircase: 40 flushes is exactly
    /// one rung of the size ladder. The same shape measured 2.06× at 80, 3.20× at 160 and 3.34×
    /// at 400 and was still climbing, because with a 64 MB target against 69 KB flush segments
    /// no file ever reached maximal and every new rung rewrote everything below it.
    /// Amplification is <c>log_ratio(maximal / flush size)</c>, so it converges only when maximal
    /// is REACHABLE; the target here puts that ratio (~235) near the stand's own (512 MB
    /// maximal-halved against ~1.3 MB flush segments ≈ 394). It is therefore a property of the
    /// DEPLOYMENT'S GEOMETRY, not a constant of the policy — a quiet server with 20 KB flush
    /// segments has two more rungs and pays for them.</para>
    ///
    /// <para>Stragglers are part of the run — a late row every tenth flush into an old sealed
    /// bucket, and a straddling flush every fiftieth — because a policy measured only on
    /// well-behaved input measures the case that was never broken.</para>
    ///
    /// <para>Note the metric counts REWRITES ONLY: the device sees <c>1 + this</c> per ingested
    /// byte, since the flush write itself is not in the numerator.</para>
    /// </summary>
    [Fact]
    public async Task OpenBucketAmplificationConverges()
    {
        var (marginal, files, ingested) = await AmplificationRunAsync(
            flushes: 4000, checkpoints: [80, 160, 400, 1000, 2000, 3000, 4000], sizeSpread: 1);

        // THE BAND, not one sample. Every stretch from 400 flushes on — the point where a file
        // has reached maximal and the ladder is fully formed — must sit inside a 1.25× band.
        // MEASURED over 6000 flushes: 3.13x, 2.81x, 3.05x, 2.98x, 3.05x, 2.98x, 3.05x, a spread
        // of 1.11x across 15× the data. A staircase (the failure this replaced) breaks it at the
        // second checkpoint; the old policy's own 2.73-3.30 range breaks it too.
        AssertAmplificationBand(marginal, from: 400, bandwidth: 1.25, ceiling: 3.6);

        // FILE COUNT is guarded beside the bytes, because the two fail in opposite directions:
        // a policy that merges nothing reports a beautiful amplification. Every file at or past
        // maximal is a permanent, intended resident — 420 MB of ingest cannot occupy fewer than
        // 420/16 files at a 16 MB maximal — so the bound is that floor plus the ladder's own
        // allowance of MergeMinSources per tier still in flight.
        AssertFileCountIsTheLadderNotTheUptime(files[4000], ingested);
    }

    /// <summary>
    /// The same measurement with per-flush volume drawn log-uniformly over 1×..8×, which is the
    /// distribution every amplification and file-count number on this branch was missing.
    ///
    /// <para>Uniform flush sizes are the one case where the size ratio and time-contiguity never
    /// disagree, so they measure the policy at its most co-operative. Under variance the BYTE
    /// figure was never the risk — fewer merges happen, so bytes-written/bytes-ingested falls,
    /// which is exactly why the previous convergence run looked healthy on shapes that were
    /// silently stuck. MEASURED here: 1.00x, 2.91x, 3.19x, 2.96x, 3.01x per stretch and 8 to 27
    /// files over 2000 flushes — the same band as the uniform run, and a file count that tracks
    /// the bytes ingested rather than the flushes.</para>
    /// </summary>
    [Fact]
    public async Task AmplificationAndFileCountSurviveNonUniformFlushSizes()
    {
        var (marginal, files, ingested) = await AmplificationRunAsync(
            flushes: 2000, checkpoints: [80, 160, 400, 1000, 2000], sizeSpread: 8);

        AssertAmplificationBand(marginal, from: 400, bandwidth: 1.25, ceiling: 3.6);
        AssertFileCountIsTheLadderNotTheUptime(files[2000], ingested);
    }

    /// <summary>
    /// One flush per iteration into a live bucket, with a straggler every tenth and a straddler
    /// every fiftieth, compacted to exhaustion after each. Returns the MARGINAL amplification of
    /// each stretch between checkpoints — the cumulative ratio is reported too but is a running
    /// average and lags, which is how 1.40x came to be published while the per-stretch cost was
    /// already 3.34x.
    /// </summary>
    private async Task<(Dictionary<int, double> Marginal, Dictionary<int, int> Files, long Ingested)>
        AmplificationRunAsync(int flushes, int[] checkpoints, int sizeSpread)
    {
        _engine._mergeTargetPayloadBytes = 32L * 1024 * 1024;

        long today = DateTime.UtcNow.Ticks - 20 * TimeSpan.TicksPerHour;
        long stale = BucketStart(LogLevel.Warning, DateTime.UtcNow.Ticks - 30 * TimeSpan.TicksPerDay);
        long written = 0, ingested = 0, lastWritten = 0, lastIngested = 0;
        int  merges  = 0, expected = 0;
        var  marginal = new Dictionary<int, double>();
        var  files    = new Dictionary<int, int>();
        var  marks    = new HashSet<int>(checkpoints);
        var  rnd      = new Random(7);   // fixed, so the curve is reproducible to the byte

        for (int f = 0; f < flushes; f++)
        {
            long before = _engine.ListSegments().Sum(s => s.UncompressedBytes);

            int n = sizeSpread <= 1 ? 200 : (int)(50 * Math.Pow(sizeSpread, rnd.NextDouble()));
            Write(n, LogLevel.Information, today + f * TimeSpan.TicksPerMinute, padBytes: 256);
            expected += n;
            // A late row of its own level lands in a bucket that sealed weeks ago.
            if (f % 10 == 0) { Write(1, LogLevel.Warning, stale + f); expected++; }
            // A late row of the LIVE level makes the flush segment straddle a boundary.
            if (f % 50 == 0) { Write(1, LogLevel.Information, stale + f); expected++; }
            await _engine.FlushHotTierAsync();

            ingested += _engine.ListSegments().Sum(s => s.UncompressedBytes) - before;

            var pass = await CompactToExhaustionAsync(cap: 200);
            merges  += pass.Merges;
            written += pass.BytesWritten;

            if (marks.Contains(f + 1))
            {
                double marg = (written - lastWritten) / (double)(ingested - lastIngested);
                marginal[f + 1] = marg;
                files[f + 1]    = _engine.ListSegments().Count;
                lastWritten = written; lastIngested = ingested;
                _out.WriteLine($"spread {sizeSpread}x, {f + 1,5} flushes: {files[f + 1],3} file(s), {merges,4} merges, " +
                               $"{written / 1024,8} KB written / {ingested / 1024,8} KB ingested = " +
                               $"{written / (double)ingested:F2}x cumulative, {marg:F2}x over this stretch");
            }
        }

        Assert.Equal(expected, ServedEvents());
        return (marginal, files, ingested);
    }

    private void AssertAmplificationBand(Dictionary<int, double> marginal, int from, double bandwidth, double ceiling)
    {
        double lo = double.MaxValue, hi = 0;
        foreach (var (at, amp) in marginal)
        {
            if (at < from) continue;
            lo = Math.Min(lo, amp);
            hi = Math.Max(hi, amp);
        }
        Assert.True(hi <= lo * bandwidth,
            $"amplification has not settled: {lo:F2}x .. {hi:F2}x across the stretches from {from} flushes on");
        // The ceiling is the level, set above the measured 3.13x and below the log₄(maximal /
        // flush size) = 4.1 that every rung costing a full pass would produce.
        Assert.True(hi < ceiling, $"steady-state write amplification peaks at {hi:F2}x");
    }

    private void AssertFileCountIsTheLadderNotTheUptime(int files, long ingested)
    {
        long maximal   = _engine._mergeTargetPayloadBytes / 2;
        long permanent = ingested / maximal;                       // files that are done, by volume
        long allowance = StorageEngine.MergeMinSources * (long)Math.Max(1, DistinctSizeTiers());
        _out.WriteLine($"{files} file(s) against {permanent} maximal + {allowance} in flight " +
                       $"({DistinctSizeTiers()} size tier(s), {ingested / 1024 / 1024} MB ingested)");
        Assert.True(files <= permanent + allowance,
            $"{files} files, above the {permanent} maximal the volume forces plus a {allowance}-file ladder");
    }

    /// <summary>
    /// The bucket widths and the over-retention they buy, per level, under the default policy.
    /// Debug's 3-day TTL earns a 6 h bucket: the old whole-day floor made its over-retention
    /// 33 %, not the 8.3 % <c>MergeSpanTtlDivisor</c> advertises, and its bucket did not seal
    /// until day 2 of a 3-day life.
    /// </summary>
    [Fact]
    public void BucketWidthOverRetentionPerLevel()
    {
        foreach (var lvl in new[] { LogLevel.Verbose, LogLevel.Debug, LogLevel.Information })
        {
            var  ttl   = RetentionPolicy.Default.GetTtl(lvl);
            long w     = BucketTicks(lvl);
            long grace = Math.Min(48L * TimeSpan.TicksPerHour, w);
            _out.WriteLine($"{lvl,-11} ttl {ttl.TotalDays,2} d, width {w / (double)TimeSpan.TicksPerHour:F0} h, " +
                           $"over-retention {100.0 * w / ttl.Ticks:F1} %, seals at bucket start " +
                           $"+{(w + grace) / (double)TimeSpan.TicksPerHour:F0} h, " +
                           $"oldest row dies at +{(w + ttl.Ticks) / (double)TimeSpan.TicksPerDay:F1} d");
            Assert.True(100.0 * w / ttl.Ticks <= 100.0 / 12 + 0.01,
                $"{lvl} over-retains {100.0 * w / ttl.Ticks:F1} %, above the advertised 8.3 %");
        }
        Assert.Equal(7 * TimeSpan.TicksPerDay,  BucketTicks(LogLevel.Information));
        Assert.Equal(6 * TimeSpan.TicksPerHour, BucketTicks(LogLevel.Debug));
    }

    /// <summary>
    /// The whole width ladder, not just the shipped defaults. TTLs are settable at runtime, and
    /// the grid's floor used to be ONE HOUR with no relation to the TTL — the same hidden floor
    /// the whole-day one had been, an order of magnitude down: a 3 h level over-retained by 33 %,
    /// a 1 h level by 100 % and a 30 min level by 200 %, all while the divisor's doc comment
    /// stated 8.3 % as a property of the design. The ladder now runs to one minute, so the
    /// guarantee holds down to a 12-minute TTL, and below that the failure is stated rather than
    /// silent.
    ///
    /// <para>Every width still divides a day, which is what keeps a bucket boundary on every UTC
    /// midnight and the sub-day grid aligned with the whole-day widths above it.</para>
    /// </summary>
    [Fact]
    public void EveryTtlOnTheLadderGetsTheAdvertisedOverRetention()
    {
        foreach (double hours in new[] { 0.2, 0.5, 1, 3, 6, 12, 24, 72, 24 * 30, 24 * 90, 24 * 365 })
        {
            var  ttl = TimeSpan.FromHours(hours);
            long w   = StorageEngine.MergeBucketTicks(ttl);
            double over = 100.0 * w / ttl.Ticks;
            _out.WriteLine($"ttl {hours,8:F1} h -> width {w / (double)TimeSpan.TicksPerMinute,9:F0} min, " +
                           $"over-retention {over,6:F1} %");

            Assert.True(TimeSpan.TicksPerDay % w == 0 || w % TimeSpan.TicksPerDay == 0,
                $"width {w} ticks neither divides a day nor is a whole number of days");
            if (ttl >= TimeSpan.FromMinutes(12))
                Assert.True(over <= 100.0 / 12 + 0.01, $"ttl {hours} h over-retains {over:F1} %");
        }
        // The floor, stated: below a 12-minute TTL the minute grid binds and the fraction does not.
        Assert.Equal(TimeSpan.TicksPerMinute, StorageEngine.MergeBucketTicks(TimeSpan.FromMinutes(1)));
        Assert.Equal(TimeSpan.TicksPerMinute, StorageEngine.MergeBucketTicks(TimeSpan.Zero));
    }
}
