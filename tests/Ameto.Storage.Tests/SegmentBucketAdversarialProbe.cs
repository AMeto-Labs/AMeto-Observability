using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit.Abstractions;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// The (bucket, level) policy measured in the shapes <see cref="SegmentBucketCompactionTests"/>
/// does not construct: a straggler landing in a bucket that has already collapsed, a flush
/// segment whose Min and Max fall in different buckets, and an open bucket run long enough for
/// its files to outgrow the batch budget. Each of these prints what the planner actually does;
/// they assert only that no event is lost, because the behaviour they record is the behaviour
/// under review, not a contract.
///
/// <para>MEASURED here (Release, 2026-07-31):</para>
/// <list type="bullet">
/// <item>a collapsed 1430 KB sealed bucket is rewritten IN FULL for every one-event straggler
///       that lands in it — 5 stragglers, 5 merges, 7151 KB written;</item>
/// <item>six flush segments straddling a bucket boundary (one 30-day-late row each) produce
///       0 merges: the bucket never converges and every file keeps a 32-day span, against the
///       7-day bound the policy advertises;</item>
/// <item>an open bucket whose files have outgrown <c>target / MergeMinSources</c> stops merging
///       entirely — 40 flush segments, target = 5 segments' worth, 0 merges.</item>
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

    /// <summary>
    /// A straggler lands in a bucket that collapsed days ago. The bucket is sealed, so the
    /// planner drops the size ratio and takes a 2-source batch — and the second source is the
    /// bucket's whole collapsed file. One event therefore costs a full rewrite, and the cost
    /// does not decay: the merged file is still below <c>MergeTargetPayloadBytes / 2</c>, so it
    /// stays a candidate for the next straggler too.
    /// </summary>
    [Fact]
    public async Task AStragglerRewritesACollapsedSealedBucket()
    {
        long day0 = BucketStart(LogLevel.Debug, DateTime.UtcNow.Ticks - 10 * TimeSpan.TicksPerDay);
        for (int f = 0; f < 6; f++)
            await FlushAsync(400, LogLevel.Debug, day0 + f * 3 * TimeSpan.TicksPerHour, padBytes: 512);

        await CompactToExhaustionAsync();
        var collapsed = _engine.ListSegments().Single();

        long rewritten = 0;
        int  merges    = 0;
        for (int late = 0; late < 5; late++)
        {
            await FlushAsync(1, LogLevel.Debug, day0 + 5 * TimeSpan.TicksPerHour + late);
            var pass = await CompactToExhaustionAsync();
            merges    += pass.Merges;
            rewritten += pass.BytesWritten;
        }

        _out.WriteLine($"collapsed bucket {collapsed.UncompressedBytes / 1024} KB; " +
                       $"5 one-event stragglers => {merges} merge(s), {rewritten / 1024} KB rewritten");
        Assert.Equal(2405, ServedEvents());
    }

    /// <summary>
    /// The same shape without a contrived flush: one late Fatal row inside a live tier of
    /// Information. The level-split flush writes it as a Fatal segment of its own, so its Min
    /// AND Max are both inside the old bucket — which is exactly what makes it a merge source
    /// for that bucket's collapsed file.
    /// </summary>
    [Fact]
    public async Task OneLateFatalRowRewritesTheWholeFatalBucket()
    {
        long day0 = BucketStart(LogLevel.Fatal, DateTime.UtcNow.Ticks - 30 * TimeSpan.TicksPerDay);
        for (int f = 0; f < 8; f++)
            await FlushAsync(300, LogLevel.Fatal, day0 + f * TimeSpan.TicksPerHour, padBytes: 512);
        await CompactToExhaustionAsync();
        var collapsed = _engine.ListSegments().Single();

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
                       $"4 single-row stragglers => {merges} merge(s), {rewritten / 1024} KB rewritten");
        Assert.Equal(2400 + 4 + 800, ServedEvents());
    }

    /// <summary>
    /// A flush segment whose oldest row is 30 days late spans from that row to now. Its bucket is
    /// the OLD one (bucket = floor(Min / width)), but its span is five times the bucket width, so
    /// the planner's span guard rejects every partner it could have — including other straddlers
    /// with the same shape. The bucket has no terminal state and its files keep a span the policy
    /// says is impossible.
    /// </summary>
    [Fact]
    public async Task StraddlingSegmentsNeverCompactAndKeepAnUnboundedSpan()
    {
        long w   = BucketTicks(LogLevel.Information);
        long b   = BucketStart(LogLevel.Information, DateTime.UtcNow.Ticks - 30 * TimeSpan.TicksPerDay);
        long now = DateTime.UtcNow.Ticks;

        for (int f = 0; f < 6; f++)
        {
            Write(1,  LogLevel.Information, b + TimeSpan.TicksPerHour + f);
            Write(50, LogLevel.Information, now - (60 - f) * TimeSpan.TicksPerMinute);
            await _engine.FlushHotTierAsync();
        }

        var pass  = await CompactToExhaustionAsync();
        var after = _engine.ListSegments();
        double maxSpan = after.Max(s => (s.MaxTimestampTicks - s.MinTimestampTicks) / (double)TimeSpan.TicksPerDay);
        _out.WriteLine($"6 straddlers, bucket width {w / TimeSpan.TicksPerDay} d: {pass.Merges} merge(s), " +
                       $"{after.Count} file(s), largest span {maxSpan:F1} d");
        Assert.Equal(306, ServedEvents());
    }

    /// <summary>
    /// The span guard <c>continue</c>s past a source instead of stopping, so the "contiguous
    /// oldest-first run" is not contiguous when a straddler sits in the middle of a bucket: the
    /// merged file spans right across the straddler it skipped, and the two overlap.
    /// </summary>
    [Fact]
    public async Task ASkippedStraddlerLeavesOverlappingFiles()
    {
        long b = BucketStart(LogLevel.Information, DateTime.UtcNow.Ticks - 30 * TimeSpan.TicksPerDay);

        for (int f = 0; f < 4; f++)
            await FlushAsync(50, LogLevel.Information, b + (1 + f) * TimeSpan.TicksPerHour);
        Write(1,  LogLevel.Information, b + 5 * TimeSpan.TicksPerHour);
        Write(50, LogLevel.Information, DateTime.UtcNow.Ticks - TimeSpan.TicksPerHour);
        await _engine.FlushHotTierAsync();
        for (int f = 0; f < 4; f++)
            await FlushAsync(50, LogLevel.Information, b + (6 + f) * TimeSpan.TicksPerHour);

        await CompactToExhaustionAsync();

        var segs = _engine.ListSegments().OrderBy(s => s.MinTimestampTicks).ToList();
        bool overlap = false;
        for (int i = 1; i < segs.Count; i++)
            if (segs[i].MinTimestampTicks < segs[i - 1].MaxTimestampTicks) overlap = true;
        _out.WriteLine($"{segs.Count} file(s) after cutting a bucket that holds a straddler, overlap={overlap}");
        Assert.Equal(451, ServedEvents());
    }

    /// <summary>
    /// <c>MergeMinSources</c> is tested against the batch AFTER the payload budget has cut it, so
    /// an open bucket whose files have grown past <c>target / 8</c> can never assemble a legal
    /// batch again. At the shipped 512 MB target that is any file over 64 MB; the bucket then
    /// waits for its 48 h-past-window seal (up to 9 days for a 90-day level) to consolidate.
    /// </summary>
    [Fact]
    public async Task AnOpenBucketStallsOnceItsFilesOutgrowTheBatchBudget()
    {
        long today = DateTime.UtcNow.Ticks - 20 * TimeSpan.TicksPerHour;
        await FlushAsync(200, LogLevel.Information, today, padBytes: 256);
        long one = _engine.ListSegments().Single().UncompressedBytes;
        _engine._mergeTargetPayloadBytes = one * 5;   // five sources fill the budget, eight are required

        for (int f = 1; f < 40; f++)
            await FlushAsync(200, LogLevel.Information, today + f * TimeSpan.TicksPerMinute, padBytes: 256);

        var pass = await CompactToExhaustionAsync();
        _out.WriteLine($"flush segment {one / 1024} KB, target {one * 5 / 1024} KB: " +
                       $"{pass.Merges} merge(s), {_engine.ListSegments().Count} of 40 file(s) left");
        Assert.Equal(8000, ServedEvents());
    }

    /// <summary>
    /// The bucket widths and the over-retention they buy, per level, under the default policy —
    /// the floor at one whole day means Debug's is 33 %, not the 8.3 % the divisor implies.
    /// </summary>
    [Fact]
    public void BucketWidthOverRetentionPerLevel()
    {
        foreach (var lvl in new[] { LogLevel.Verbose, LogLevel.Debug, LogLevel.Information })
        {
            var  ttl = RetentionPolicy.Default.GetTtl(lvl);
            long w   = BucketTicks(lvl);
            _out.WriteLine($"{lvl,-11} ttl {ttl.TotalDays,2} d, width {w / (double)TimeSpan.TicksPerDay:F0} d, " +
                           $"over-retention {100.0 * w / ttl.Ticks:F1} %, seals at bucket start " +
                           $"+{(w + 48L * TimeSpan.TicksPerHour) / (double)TimeSpan.TicksPerDay:F0} d, " +
                           $"oldest row dies at +{(w + ttl.Ticks) / (double)TimeSpan.TicksPerDay:F0} d");
        }
        Assert.Equal(7 * TimeSpan.TicksPerDay, BucketTicks(LogLevel.Information));
        Assert.Equal(1 * TimeSpan.TicksPerDay, BucketTicks(LogLevel.Debug));
    }
}
