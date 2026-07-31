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
    /// The run planner STOPS at the first source it cannot take instead of stepping over it. The
    /// old span guard <c>continue</c>d, so with an outlier in the middle of a bucket the merged
    /// file spanned right across the source it had skipped and the two overlapped — every query
    /// into that window then opened both.
    /// </summary>
    [Fact]
    public async Task AMergeNeverSpansASourceItSkipped()
    {
        long b = BucketStart(LogLevel.Information, DateTime.UtcNow.Ticks - 30 * TimeSpan.TicksPerDay);

        // A size outlier in the middle: two small flushes, one 20× larger, two more small.
        await FlushAsync(50,   LogLevel.Information, b + 1 * TimeSpan.TicksPerHour);
        await FlushAsync(50,   LogLevel.Information, b + 2 * TimeSpan.TicksPerHour);
        await FlushAsync(1200, LogLevel.Information, b + 3 * TimeSpan.TicksPerHour, padBytes: 512);
        await FlushAsync(50,   LogLevel.Information, b + 5 * TimeSpan.TicksPerHour);
        await FlushAsync(50,   LogLevel.Information, b + 6 * TimeSpan.TicksPerHour);

        await CompactToExhaustionAsync();

        var segs = _engine.ListSegments().OrderBy(s => s.MinTimestampTicks).ToList();
        for (int i = 1; i < segs.Count; i++)
            Assert.True(segs[i].MinTimestampTicks >= segs[i - 1].MaxTimestampTicks,
                $"files {i - 1} and {i} overlap — a merge spanned a source it skipped");
        _out.WriteLine($"{segs.Count} file(s) after compacting a bucket with a size outlier in the middle");
        Assert.Equal(1400, ServedEvents());
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
    /// WRITE AMPLIFICATION, measured as a curve rather than a point, over a run long enough for
    /// files to reach the maximal size and stop being candidates — which is the only thing that
    /// makes the number converge.
    ///
    /// <para>The previous claim of 1.40× was one step of a staircase: 40 flushes is exactly one
    /// rung of the size ladder. The same shape measured 2.06× at 80, 3.20× at 160 and 3.34× at
    /// 400 and was still climbing, because with a 64 MB target against 69 KB flush segments no
    /// file ever reached maximal and every new rung rewrote everything below it. Amplification
    /// is <c>log_ratio(maximal / flush size)</c>, so it converges only when maximal is
    /// REACHABLE; the target here is set to put that ratio (~475) near the stand's own
    /// (512 MB maximal-halved against ~1.3 MB flush segments ≈ 394).</para>
    ///
    /// <para>Stragglers are part of the run — a late row every tenth flush into an old sealed
    /// bucket, and a straddling flush every fiftieth — because a policy measured only on
    /// well-behaved input measures the case that was never broken.</para>
    /// </summary>
    [Fact]
    public async Task OpenBucketAmplificationConverges()
    {
        _engine._mergeTargetPayloadBytes = 32L * 1024 * 1024;

        long today = DateTime.UtcNow.Ticks - 20 * TimeSpan.TicksPerHour;
        long stale = BucketStart(LogLevel.Warning, DateTime.UtcNow.Ticks - 30 * TimeSpan.TicksPerDay);
        long written = 0, ingested = 0, lastWritten = 0, lastIngested = 0;
        int  merges  = 0, expected = 0;
        var  marginal = new Dictionary<int, double>();

        for (int f = 0; f < 1000; f++)
        {
            long before = _engine.ListSegments().Sum(s => s.UncompressedBytes);

            Write(200, LogLevel.Information, today + f * TimeSpan.TicksPerMinute, padBytes: 256);
            expected += 200;
            // A late row of its own level lands in a bucket that sealed weeks ago.
            if (f % 10 == 0) { Write(1, LogLevel.Warning, stale + f); expected++; }
            // A late row of the LIVE level makes the flush segment straddle a boundary.
            if (f % 50 == 0) { Write(1, LogLevel.Information, stale + f); expected++; }
            await _engine.FlushHotTierAsync();

            ingested += _engine.ListSegments().Sum(s => s.UncompressedBytes) - before;

            var pass = await CompactToExhaustionAsync(cap: 100);
            merges  += pass.Merges;
            written += pass.BytesWritten;

            if (f + 1 is 80 or 160 or 400 or 1000)
            {
                // The CUMULATIVE ratio is a running average and lags by construction — it is
                // what made 1.40x look like a result. The number that says whether the policy
                // has settled is the MARGINAL one: what this stretch alone cost.
                double amp  = written / (double)ingested;
                double marg = (written - lastWritten) / (double)(ingested - lastIngested);
                marginal[f + 1] = marg;
                lastWritten = written; lastIngested = ingested;
                _out.WriteLine($"{f + 1,5} flushes: {_engine.ListSegments().Count,3} file(s), {merges,3} merges, " +
                               $"{written / 1024,7} KB written / {ingested / 1024,7} KB ingested = " +
                               $"{amp:F2}x cumulative, {marg:F2}x over this stretch");
            }
        }

        Assert.Equal(expected, ServedEvents());

        // CONVERGED means the last stretch costs what the one before it did. 400 → 1000 flushes
        // is 2.5× the data of 160 → 400, and the whole point of a size ladder whose top rung is
        // REACHABLE (a file at MergeSealedSourceBytes leaves the candidate set for good) is that
        // more data costs no more rewrites per byte. The old policy had no such top — with a
        // 64 MB target against 69 KB segments nothing ever reached it — so every new rung
        // rewrote everything below it and the curve stepped up forever: 2.06x, 3.20x, 3.34x.
        Assert.True(marginal[1000] <= marginal[400] * 1.15,
            $"amplification is still climbing: {marginal[400]:F2}x over 160-400, {marginal[1000]:F2}x over 400-1000");
        // MEASURED 2.92x over 400-1000, against log₄(maximal / flush size) = 4.1 rewrites if
        // every rung cost a full pass. The guard is that number plus room for the run planner
        // to pick a different cut, not a round figure chosen to pass.
        Assert.True(marginal[1000] < 3.2, $"steady-state write amplification is {marginal[1000]:F2}x");
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
}
