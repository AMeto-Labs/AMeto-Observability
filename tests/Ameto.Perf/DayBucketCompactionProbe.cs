using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit.Abstractions;
using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Perf;

/// <summary>
/// What the (bucket, level) compaction policy does to a real catalog, on the sandbox stand's
/// SHAPE: ~370 MB of log payload a day across six levels, Information dominant, age-flushed on a
/// timer so every flush leaves one small segment per level present in the tier.
///
/// <para>Run at 1/16 of the stand's volume WITH the merged-file target divided by the same 16.
/// How many files a bucket ends with is set by the RATIO of the bucket's payload to that target,
/// so scaling both together reproduces the geometry — a busy level's files still land where the
/// size cap puts them, a rare level's still fill its whole span — at a sixteenth of the bytes.
/// The flush CADENCE is coarser than the stand's five minutes (36 a day, not 288), which is the
/// one thing not to read off this probe: the stand's pre-compaction file count is ~8× what is
/// printed here, the post-compaction one is not.</para>
/// </summary>
public sealed class DayBucketCompactionProbe : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-standshape-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private StorageEngine _engine = null!;

    public DayBucketCompactionProbe(ITestOutputHelper output) => _out = output;

    private const int  Scale         = 16;
    private const int  Days          = 8;
    private const int  FlushesPerDay = 36;
    private const long StandDailyPayloadBytes = 370L * 1024 * 1024;
    private const int  BytesPerEvent = 2048;   // the stand's prop-dense shape

    /// <summary>Share of a day's payload by level — Information dominant, Fatal a rounding error.</summary>
    private static readonly (LogLevel Level, double Share)[] Mix =
    [
        (LogLevel.Verbose,     0.02),
        (LogLevel.Debug,       0.08),
        (LogLevel.Information, 0.70),
        (LogLevel.Warning,     0.12),
        (LogLevel.Error,       0.07),
        (LogLevel.Fatal,       0.01),
    ];

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = _dir }),
            new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance)
        {
            _allowIndexlessMerge     = true,
            _mergeTargetPayloadBytes = 512L * 1024 * 1024 / Scale,
        };
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private static byte[] Props(int n)
    {
        var buf = new ArrayBufferWriter<byte>(BytesPerEvent + 64);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(4);
        w.Write("n");       w.Write((long)n);
        w.Write("wallet");  w.Write("wallet:" + n);
        w.Write("shard");   w.Write(n % 97);
        w.Write("payload"); w.Write(new string((char)('a' + n % 26), BytesPerEvent - 96));
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    [Fact]
    public async Task StandShapeCollapsesToOneFilePerBucketPerLevel()
    {
        long dayPayload = StandDailyPayloadBytes / Scale;
        long width      = StorageEngine.MergeBucketTicks(RetentionPolicy.Default.GetTtl(LogLevel.Fatal));
        // Start far enough back that every bucket, including the last, is past its grace period:
        // this measures the STEADY STATE, not a half-compacted tail.
        long day0 = (DateTime.UtcNow.Ticks - 30 * TimeSpan.TicksPerDay) / width * width;

        int n = 0, ingested = 0;
        for (int d = 0; d < Days; d++)
        {
            for (int f = 0; f < FlushesPerDay; f++)
            {
                long t = day0 + d * TimeSpan.TicksPerDay + f * (TimeSpan.TicksPerDay / FlushesPerDay);
                foreach (var (level, share) in Mix)
                {
                    int events = (int)(dayPayload * share / FlushesPerDay / BytesPerEvent);
                    for (int i = 0; i < events; i++)
                    {
                        _engine.TryWrite(new LogEventHeader
                        {
                            Id                       = new EventId(0u, (uint)n).RawValue,
                            TimestampUtcTicks        = t + i * TimeSpan.TicksPerSecond,
                            Level                    = level,
                            MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {n} on {shard}"),
                            ServiceNamePoolIndex     = _engine.TemplatePool.Intern("Svc." + n % 5),
                        }, Props(n));
                        n++; ingested++;
                    }
                }
                await _engine.FlushHotTierAsync();
            }
        }

        var before      = _engine.ListSegments();
        long beforeMb   = before.Sum(s => s.UncompressedBytes) / 1048576;
        long beforeDisk = before.Sum(s => s.CompressedBytes) / 1048576;

        var  sw = System.Diagnostics.Stopwatch.StartNew();
        int  merges = 0;
        long written = 0;
        while (await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None))
        {
            merges++;
            Assert.True(merges < 400, "compaction did not converge");
            written += _engine.ListSegments().OrderByDescending(s => s.Id.Value).First().UncompressedBytes;
        }
        sw.Stop();

        var after = _engine.ListSegments();
        _out.WriteLine(
            $"stand shape at 1/{Scale}: {Days} days x {FlushesPerDay} flushes, {ingested:N0} events, " +
            $"{beforeMb} MB payload ({beforeDisk} MB on disk)");
        _out.WriteLine($"segments: {before.Count} -> {after.Count} in {merges} merges, {sw.ElapsedMilliseconds} ms");
        _out.WriteLine($"compaction wrote {written / 1048576} MB for {beforeMb} MB ingested " +
                       $"= {written / (double)Math.Max(1, before.Sum(s => s.UncompressedBytes)):F2}x");

        foreach (var (level, _) in Mix)
        {
            var b = before.Where(s => s.MinLevel == level).ToList();
            var a = after .Where(s => s.MinLevel == level).ToList();
            if (b.Count == 0) continue;
            double maxSpanDays = a.Count == 0 ? 0
                : a.Max(s => (s.MaxTimestampTicks - s.MinTimestampTicks) / (double)TimeSpan.TicksPerDay);
            long mb = a.Sum(s => s.UncompressedBytes) / 1048576;
            _out.WriteLine(
                $"  {level,-11} {b.Count,5} -> {a.Count,3} file(s), {mb,4} MB payload, " +
                $"largest span {maxSpanDays * 24:F1} h, bound " +
                $"{StorageEngine.MergeBucketTicks(RetentionPolicy.Default.GetTtl(level)) / (double)TimeSpan.TicksPerHour:F0} h");
        }

        // Every event still served, exactly once.
        int served = 0;
        var dedup = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var s in after)
        {
            using var r = SegmentReader.Open(s.FilePath);
            served += r.ReadAllRaw(dedup).Count;
        }
        Assert.Equal(ingested, served);

        // The headline: file count is a function of buckets, not of uptime. Eight days at six
        // levels is 32 six-hour Debug buckets, two seven-day buckets for each 90-day level, and
        // a handful of size-capped files for the dominant one — MEASURED 1728 -> 45 at 1.00x,
        // because a backfill this settled hits every bucket's terminal state in one pass.
        Assert.True(after.Count < before.Count / 10,
            $"{before.Count} -> {after.Count} segments is not a collapse");
        foreach (var s in after)
            Assert.True(s.MaxTimestampTicks - s.MinTimestampTicks <=
                        StorageEngine.MergeBucketTicks(RetentionPolicy.Default.GetTtl(s.MinLevel)),
                        $"{s.MinLevel} segment outlives its span bound");

        // The dominant level is the one whose bucket the size cap has to cut, so it is the one
        // that shows whether the cut partitions the bucket in time or interleaves it. Picking
        // the batch smallest-first gave 5.72 d per file here — every file spanning the whole
        // bucket — against 1.92 d oldest-first.
        double infoSpan = after.Where(s => s.MinLevel == LogLevel.Information)
                               .Max(s => (s.MaxTimestampTicks - s.MinTimestampTicks) / (double)TimeSpan.TicksPerDay);
        Assert.True(infoSpan < 3.0, $"Information files span {infoSpan:F2} d — the bucket was interleaved, not partitioned");
    }
}
