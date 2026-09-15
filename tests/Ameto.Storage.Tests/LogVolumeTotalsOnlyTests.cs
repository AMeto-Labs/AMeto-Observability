using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// The alert evaluator counts a level-less rule every 15 s over a window that can span every
/// segment on disk, and it used to open and LZ4-decode all of them to do it. A cold segment is
/// immutable and its catalog EventCount is exact, so one lying entirely inside the window needs
/// no decode at all — only the boundary segments and the hot tier do.
///
/// <para>That is only sound if the shortcut agrees with the decode exactly, so these run both
/// over the same data: several flushes (so several cold segments), several services, every
/// level, and a hot tier on top.</para>
/// </summary>
public sealed class LogVolumeTotalsOnlyTests : IDisposable
{
    private static readonly DateTimeOffset Base = new(2026, 8, 23, 9, 0, 0, TimeSpan.Zero);

    private static readonly string[]   Services = ["checkout", "billing", "gateway"];
    private static readonly LogLevel[] Levels   =
        [LogLevel.Verbose, LogLevel.Debug, LogLevel.Information, LogLevel.Warning, LogLevel.Error, LogLevel.Fatal];

    private readonly string        _dir = Path.Combine(Path.GetTempPath(), "Ameto-totalsonly-" + Guid.NewGuid().ToString("N"));
    private readonly StorageEngine _engine;
    private readonly ArrayBufferWriter<byte> _buf = new(128);

    public LogVolumeTotalsOnlyTests()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions { DataDirectory = _dir };
        _engine  = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _engine.IndexSinkFactory = static (c, t) => new SegmentIndexBuilder(c, 5, t);

        // Three flushes, so the catalog holds several cold segments with different spans, then
        // a hot tier that is never a "whole segment" and always has to be walked.
        for (int i = 0;   i < 120; i++) Write(i);
        _engine.FlushHotTierAsync().GetAwaiter().GetResult();
        for (int i = 120; i < 240; i++) Write(i);
        _engine.FlushHotTierAsync().GetAwaiter().GetResult();
        for (int i = 240; i < 360; i++) Write(i);
        _engine.FlushHotTierAsync().GetAwaiter().GetResult();
        for (int i = 360; i < 450; i++) Write(i);
    }

    public void Dispose()
    {
        try { _engine.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { }
        try { Directory.Delete(_dir, true); } catch { }
    }

    /// <summary>One event per second from <see cref="Base"/>, cycling service and level.</summary>
    private void Write(int i)
    {
        _buf.ResetWrittenCount();
        var w = new MessagePackWriter(_buf);
        w.WriteMapHeader(1);
        w.Write("k"); w.Write((long)i);
        w.Flush();

        string service = Services[i % 3];
        var    level   = Levels[(i / 3) % Levels.Length];

        Assert.True(_engine.TryWrite(new LogEventHeader
        {
            TimestampUtcTicks        = Base.UtcTicks + i * TimeSpan.TicksPerSecond,
            Level                    = level,
            MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {k}"),
            ServiceNamePoolIndex     = _engine.TemplatePool.Intern(service),
        }, _buf.WrittenSpan.ToArray()));
    }

    private async Task<LogVolumeCounts> CountAsync(
        DateTimeOffset from, DateTimeOffset to, string? service, bool totalsOnly)
    {
        int bucketSeconds = (int)Math.Max(1, Math.Ceiling((to - from).TotalSeconds));
        long minBucket    = from.ToUnixTimeSeconds() / bucketSeconds;
        return await _engine.AggregateLogVolumeAsync(
            from, to, minBucket, bucketSeconds, nBuckets: 1, serviceFilter: service,
            ct: default, totalsOnly: totalsOnly);
    }

    public static TheoryData<int, int> Windows => new()
    {
        { -60,  600 },   // everything, every segment whole
        {   0,  600 },   // exact lower edge
        {  50,  600 },   // cuts through the first segment
        {  50,  200 },   // cuts through both ends, hot tier excluded
        { 130,  250 },   // entirely inside the middle segments
        { 300, 1000 },   // cuts into the cold tail and takes the whole hot tier
        { 445, 1000 },   // hot tier only
        { 900, 1000 },   // empty
    };

    [Theory]
    [MemberData(nameof(Windows))]
    public async Task The_catalog_shortcut_totals_match_a_full_decode(int fromSec, int toSec)
    {
        var from = Base.AddSeconds(fromSec);
        var to   = Base.AddSeconds(toSec);

        long decoded = (await CountAsync(from, to, null, totalsOnly: false)).Total;
        long fast    = (await CountAsync(from, to, null, totalsOnly: true)).Total;

        Assert.Equal(decoded, fast);
    }

    [Fact]
    public async Task The_corpus_actually_exercises_a_non_empty_answer()
    {
        var counts = await CountAsync(Base.AddSeconds(-60), Base.AddSeconds(600), null, totalsOnly: false);
        Assert.Equal(450, counts.Total);
        Assert.True(counts.Levels.Count > 1 && counts.Services.Count == 3);
    }

    /// <summary>
    /// The catalog cannot say how many of a segment's events belong to one service, so the
    /// shortcut has to turn itself off rather than count the whole file.
    /// </summary>
    [Theory]
    [InlineData("checkout")]
    [InlineData("gateway")]
    [InlineData("absent-service")]
    public async Task A_service_filter_disables_the_shortcut(string service)
    {
        var from = Base.AddSeconds(-60);
        var to   = Base.AddSeconds(600);

        var decoded = await CountAsync(from, to, service, totalsOnly: false);
        var fast    = await CountAsync(from, to, service, totalsOnly: true);

        Assert.Equal(decoded.Total, fast.Total);
        Assert.True(decoded.Total < 450 || service == "absent-service");

        // Still a real, attributed aggregation: the per-service series survives.
        long fromSeries = 0;
        foreach (var s in fast.Services) fromSeries += s.Count;
        Assert.Equal(fast.Total, fromSeries);
    }

    /// <summary>
    /// Without the opt-in nothing changes for /api/events/counts: every series still sums to
    /// the total, which is the invariant the shortcut deliberately gives up.
    /// </summary>
    [Fact]
    public async Task The_default_path_keeps_series_summing_to_the_total()
    {
        var counts = await CountAsync(Base.AddSeconds(-60), Base.AddSeconds(600), null, totalsOnly: false);

        long svc = 0, lvl = 0;
        foreach (var s in counts.Services) svc += s.Count;
        foreach (var s in counts.Levels)   lvl += s.Count;

        Assert.Equal(counts.Total, svc);
        Assert.Equal(counts.Total, lvl);
    }

    /// <summary>
    /// Proof that the file is genuinely not opened, not merely that the numbers agree: with the
    /// segment files renamed out from under the catalog, the decoding path loses them (it logs
    /// and skips an unreadable segment) while the shortcut still answers from the catalog. The
    /// names are put back before the engine is disposed.
    /// </summary>
    [Fact]
    public async Task Whole_segments_are_counted_without_opening_the_file()
    {
        var from = Base.AddSeconds(-60);
        var to   = Base.AddSeconds(600);

        long expected = (await CountAsync(from, to, null, totalsOnly: false)).Total;

        var segs = Directory.GetFiles(Path.Combine(_dir, "segments"), "*.seg");
        Assert.NotEmpty(segs);
        foreach (var p in segs) File.Move(p, p + ".hidden");
        try
        {
            long blindDecode = (await CountAsync(from, to, null, totalsOnly: false)).Total;
            long blindFast   = (await CountAsync(from, to, null, totalsOnly: true)).Total;

            Assert.True(blindDecode < expected, "the decoding path should lose the unreadable segments");
            Assert.Equal(expected, blindFast);   // the shortcut never touched them
        }
        finally
        {
            foreach (var p in segs) File.Move(p + ".hidden", p);
        }
    }
}
