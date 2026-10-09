using System.Globalization;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging;
using EventId  = Microsoft.Extensions.Logging.EventId;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE WRITER NEVER WRITES A BLOCK THE READER WILL NOT OPEN (#125).
///
/// <para>A v3 <c>.mts</c> keeps all its series in one LZ4 block, and <c>MetricReader</c> refuses a block
/// that inflates past <see cref="MetricReader.MaxBlockBytes"/> (64 MiB). The writer had no such line:
/// 512 busy histogram series of 1 600 five-minute points — five and a half days, a size the FiveMin
/// tier reached on the stand by carrying its history forward — made an 87.5 MiB block, written without
/// complaint and then refused by every query, merge and rollup that met it, until retention deleted it.
/// A file now closes before a series that might not fit, and a single series larger than a block goes
/// out in time-ordered runs of its points, one file each. Throwing instead would have been simpler and
/// worse: the hot-tier flush retries a write that throws forever, retracting every other metric's file
/// with it (the #106 wedge).</para>
/// </summary>
public sealed class MetricWriterBlockCapTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mwcap-" + Guid.NewGuid().ToString("N"));

    public MetricWriterBlockCapTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        MetricWriter.MaxSectionBytesForTest = null;
        try { Directory.Delete(_dir, true); } catch { }
    }

    private const long T0  = 1_784_800_020_000_000_000L;
    private const long S   = 1_000_000_000L;
    private const long Min5 = 300 * S;

    [Fact]
    public void A_section_larger_than_the_readers_block_is_written_as_files_the_reader_opens()
    {
        var written = new Dictionary<string, MetricHistogramShapes.SeriesDigest>(StringComparer.Ordinal);
        var files   = MetricHistogramShapes.Write(_dir, MetricGranularity.FiveMin, series: 512, T0, Min5,
                                                  [1600], activeFraction: 1.0, seed: 7, written);

        long section = files.Sum(f => (long)MetricHistogramShapes.BlockSizes(f.FilePath).Raw);
        Assert.True(section > MetricReader.MaxBlockBytes, $"setup: the points make only a {section:N0} B section");
        Assert.True(files.Count >= 2, "one file for more than a block of section");
        Assert.All(files, f => Assert.True(MetricHistogramShapes.BlockSizes(f.FilePath).Raw <= MetricReader.MaxBlockBytes,
                                            $"{Path.GetFileName(f.FilePath)} holds a block the reader refuses"));

        // Every file opens, and between them they hold every point of every series, once.
        var read = MetricHistogramShapes.Digests(files.Select(f => f.FilePath));
        MetricHistogramShapes.AssertSame(written, read);
        Assert.All(read.Values, d => Assert.Equal(1600, d.Points));
    }

    [Fact]
    public void A_series_larger_than_a_block_goes_out_in_time_ordered_runs()
    {
        MetricWriter.MaxSectionBytesForTest = 64 * 1024;

        var big = new List<MetricDataPoint>();
        for (int p = 0; p < 20_000; p++) big.Add(new MetricDataPoint { TimestampUnixNano = T0 + p * 15 * S, Value = p * 0.5 });
        var small = new List<MetricDataPoint> { new() { TimestampUnixNano = T0, Value = 1 } };
        var items = new List<(SeriesKey, HotSeries)>
        {
            (new SeriesKey("cap.gauge", MetricKind.Gauge, "1", new LabelSet([new("pod", "big")])),   new HotSeries(big)),
            (new SeriesKey("cap.gauge", MetricKind.Gauge, "1", new LabelSet([new("pod", "small")])), new HotSeries(small)),
        };

        var files = MetricWriter.Write(_dir, items, MetricGranularity.Raw);

        Assert.All(files, f => Assert.True(MetricHistogramShapes.BlockSizes(f.FilePath).Raw <= 64 * 1024));
        var runs = files.Where(f => MetricReader.ReadAllSync(f.FilePath).Any(s => s.Labels.ValueAt(0) == "big")).ToList();
        Assert.True(runs.Count > 1, "the series larger than a block was written in one");

        // The runs are the series in time order: disjoint, in the order written, every point once.
        var got = new List<long>();
        foreach (var f in runs)
        {
            var s = Assert.Single(MetricReader.ReadAllSync(f.FilePath));   // a run is a file to itself
            Assert.True(got.Count == 0 || s.Points[0].TimestampUnixNano > got[^1]);
            got.AddRange(s.Points.Select(p => p.TimestampUnixNano));
        }
        Assert.Equal(big.Select(p => p.TimestampUnixNano), got);
        Assert.Contains(files, f => MetricReader.ReadAllSync(f.FilePath).Any(s => s.Labels.ValueAt(0) == "small"));
    }

    [Fact]
    public void A_file_closes_before_a_series_that_might_not_fit_its_block()
    {
        MetricWriter.MaxSectionBytesForTest = 256 * 1024;

        var written = new Dictionary<string, MetricHistogramShapes.SeriesDigest>(StringComparer.Ordinal);
        var files   = MetricHistogramShapes.Write(_dir, MetricGranularity.Raw, series: 300, T0, 15 * S,
                                                  [40], activeFraction: 1.0, seed: 8, written);

        Assert.True(files.Count > 1, "300 series that do not fit one block went into one file");
        Assert.All(files, f => Assert.True(MetricHistogramShapes.BlockSizes(f.FilePath).Raw <= 256 * 1024));

        // Input order across the files, every series once, every point intact.
        var order = files.SelectMany(f => MetricReader.ReadAllSync(f.FilePath)).Select(s => s.Labels).ToList();
        Assert.Equal(Enumerable.Range(0, 300).Select(MetricHistogramShapes.LabelsFor), order);
        MetricHistogramShapes.AssertSame(written, MetricHistogramShapes.Digests(files.Select(f => f.FilePath)));
    }

    [Fact]
    public void Below_the_line_a_file_is_still_512_series_whatever_its_size()
    {
        // The cap must not cost a normal flush a file: 1 300 small series are the three files they were.
        var items = new List<(SeriesKey, HotSeries)>();
        for (int s = 0; s < 1_300; s++)
            items.Add((new SeriesKey("cap.small", MetricKind.Gauge, "1",
                                     new LabelSet([new("pod", s.ToString(CultureInfo.InvariantCulture))])),
                       new HotSeries([new MetricDataPoint { TimestampUnixNano = T0 + s * S, Value = s }])));

        var files = MetricWriter.Write(_dir, items, MetricGranularity.Raw);

        Assert.Equal(new[] { 512, 512, 276 }, files.Select(f => MetricReader.ReadAllSync(f.FilePath).Count()));
    }

    /// <summary>
    /// ONE POINT NO BLOCK CAN HOLD NO LONGER STOPS EVERY FLUSH (#126 review NEW-0). A histogram point of
    /// 7 500 000 buckets, about 7 KB as a gzip upload, is larger than the 64 MiB block a reader opens.
    /// The writer threw on it, the flush put every point back, and every later flush threw on the same
    /// point: not one metric was written until a restart's replay cut the point down to the log's
    /// 65 535 buckets, while the tier grew at the ingest rate. The flush now leaves that series out,
    /// with one Error naming it, and writes and commits the rest.
    /// </summary>
    [Fact]
    public async Task A_series_no_block_can_hold_is_dropped_by_the_flush_and_every_other_series_is_written()
    {
        var log = new CapturingLogger();
        await using var engine = new MetricStorageEngine(_dir, log);
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        // The point's size crosses the hot tier's threshold, so the ingest schedules a flush of its
        // own; every flush is awaited, or the one below could find the tier drained and return while
        // that one is still writing.
        var flushes = new List<Task>();
        engine.OnThresholdFlushScheduledForTest = t => { lock (flushes) flushes.Add(t); };
        async Task FlushAll()
        {
            await engine.ScheduleThresholdFlushForTest();
            Task[] scheduled;
            lock (flushes) scheduled = [.. flushes];
            await Task.WhenAll(scheduled);
        }

        Assert.Equal(0, engine.Ingest([Poison(now), Gauge(now, 42)]));
        await FlushAll();

        // The gauge has its file; nothing holds the poison series; the tier is empty, not wedged.
        var series = Directory.GetFiles(_dir, "*.mts").SelectMany(f => MetricReader.ReadAllSync(f)).ToList();
        var gauge  = Assert.Single(series);
        Assert.Equal("cap.healthy", gauge.Name);
        Assert.Equal(42, Assert.Single(gauge.Points).Value);
        Assert.Equal(0, engine.HotPointCount);
        Assert.DoesNotContain(log.Entries, e => e.Text.Contains("Failed to flush", StringComparison.Ordinal));

        // One Error, naming the series and saying why.
        var error = Assert.Single(log.Entries, e => e.Level == LogLevel.Error);
        Assert.Contains("'cap.poison'{pod=\"a\"}", error.Text, StringComparison.Ordinal);
        Assert.Contains("7,500,000 buckets", error.Text, StringComparison.Ordinal);
        Assert.Contains("1 point(s) are lost", error.Text, StringComparison.Ordinal);

        // And the next flush is an ordinary one.
        engine.Ingest([Gauge(now + 1_000_000_000L, 43)]);
        await FlushAll();
        Assert.Equal(2, Directory.GetFiles(_dir, "*.mts").Length);
        Assert.Single(log.Entries, e => e.Level == LogLevel.Error);

        static MetricIngestItem Poison(long at) => new()
        {
            Name = "cap.poison", Kind = MetricKind.Histogram, Unit = "ms", Labels = new LabelSet([new("pod", "a")]),
            TimestampUnixNano = at, HistogramCount = 1, HistogramSum = 1, BucketCounts = new long[7_500_000],
        };

        static MetricIngestItem Gauge(long at, double value) => new()
        {
            Name = "cap.healthy", Kind = MetricKind.Gauge, Unit = "1", Labels = new LabelSet([new("pod", "a")]),
            TimestampUnixNano = at, ScalarValue = value,
        };
    }

    /// <summary>
    /// The writer's two answers to a series no block can hold — a point too large, or labels too large
    /// with a single point: left out and named when the caller passes a list (the flush), a throw that
    /// leaves no file when it does not (a rewrite, which backs off and keeps its sources).
    /// </summary>
    [Fact]
    public void A_series_no_block_can_hold_is_named_and_left_out_or_throws_and_leaves_no_file()
    {
        MetricWriter.MaxSectionBytesForTest = 64 * 1024;

        var items = new List<(SeriesKey, HotSeries)>
        {
            (new SeriesKey("cap.mixed", MetricKind.Gauge, "1", new LabelSet([new("pod", "fine")])),
             new HotSeries([new MetricDataPoint { TimestampUnixNano = T0, Value = 1 }])),
            (new SeriesKey("cap.mixed", MetricKind.Histogram, "ms", new LabelSet([new("pod", "wide")])),
             new HotSeries([new MetricDataPoint { TimestampUnixNano = T0,     Count = 1, BucketCounts = new long[4] },
                            new MetricDataPoint { TimestampUnixNano = T0 + S, Count = 1, BucketCounts = new long[10_000] }])),
            (new SeriesKey("cap.mixed", MetricKind.Gauge, "1", new LabelSet([new("pod", new string('x', 30_000))])),
             new HotSeries([new MetricDataPoint { TimestampUnixNano = T0, Value = 2 }])),
            (new SeriesKey("cap.mixed", MetricKind.Gauge, "1", new LabelSet([new("pod", "also-fine")])),
             new HotSeries([new MetricDataPoint { TimestampUnixNano = T0, Value = 3 }])),
        };

        // No list: the call throws, says why, and leaves no file behind.
        var ex = Assert.Throws<InvalidDataException>(() => MetricWriter.Write(_dir, items, MetricGranularity.Raw));
        Assert.Contains("10,000 buckets", ex.Message, StringComparison.Ordinal);
        Assert.Empty(Directory.GetFiles(_dir));

        // A list: both series are named in it, and every other series is written.
        var unwritable = new List<UnwritableSeries>();
        var files = MetricWriter.Write(_dir, items, MetricGranularity.Raw, unwritable: unwritable);

        Assert.Equal(["wide", new string('x', 30_000)], unwritable.Select(u => u.Key.Labels.ValueAt(0)));
        Assert.Equal([2, 1], unwritable.Select(u => u.Points));
        Assert.Contains("10,000 buckets", unwritable[0].Why, StringComparison.Ordinal);
        Assert.Contains("labels", unwritable[1].Why, StringComparison.Ordinal);
        Assert.Equal(["fine", "also-fine"],
                     files.SelectMany(f => MetricReader.ReadAllSync(f.FilePath)).Select(s => s.Labels.ValueAt(0)));
    }

    private sealed class CapturingLogger : ILogger<MetricStorageEngine>
    {
        public readonly List<(LogLevel Level, string Text)> Entries = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                                Func<TState, Exception?, string> formatter)
        {
            lock (Entries) Entries.Add((logLevel, formatter(state, exception)));
        }
    }
}
