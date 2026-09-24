using System.Globalization;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// The chunked rewrite reads each source once for its identities and goes back only for a later
/// chunk's own series (issue #83 WP7, M#12). Two rules of the old re-read-everything loop that
/// the byte goldens do not exercise — their sources agree on every series' bounds and every
/// series has points — are stated here, for the first chunk and a later one alike: a series'
/// bounds are the LAST non-null ones any source gave it, and a series a source holds with no
/// points is still written.
/// </summary>
public sealed class MetricRewriteChunkTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mrwchunk-" + Guid.NewGuid().ToString("N"));
    private MetricStorageEngine _engine = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(Path.Combine(_dir, "engine"));
        _engine = new MetricStorageEngine(Path.Combine(_dir, "engine"), NullLogger<MetricStorageEngine>.Instance);
        await _engine.ColdLoadCompleted;
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private const long T0 = 1_784_800_020_000_000_000L;
    private const long S  = 1_000_000_000L;

    private static LabelSet Labels(int s) =>
        new([new("replica", "r" + s.ToString("D4", CultureInfo.InvariantCulture))]);

    private List<MetricSegmentInfo> Source(int series, int file, Func<int, double[]?> bounds, Func<int, bool> empty)
    {
        var items = new List<(SeriesKey, HotSeries)>();
        for (int s = 0; s < series; s++)
        {
            var pts = new List<MetricDataPoint>();
            if (!empty(s))
                for (int p = 0; p < 3; p++)
                    pts.Add(new MetricDataPoint
                    {
                        TimestampUnixNano = T0 + (file * 3 + p) * 60 * S, Count = p + 1, Sum = p,
                        BucketCounts = [p, 1, 0, 1],
                    });
            items.Add((new SeriesKey("rw.h", MetricKind.Histogram, "s", Labels(s)), new HotSeries(pts, bounds(s))));
        }
        return MetricWriter.Write(_dir, items, MetricGranularity.Raw);
    }

    // ── A rewrite that fails part-way leaves nothing behind (WP7 review, F3) ──

    private string EngineDir => Path.Combine(_dir, "engine");

    private static string Fingerprint(IEnumerable<MetricSegmentInfo> files) =>
        string.Join("|", files.Select(f => f.FilePath + ":" + Convert.ToHexString(
            System.Security.Cryptography.SHA256.HashData(File.ReadAllBytes(f.FilePath)))));

    private string[] Outputs() =>
        Directory.EnumerateFiles(EngineDir, "*.mts*").Where(p => !p.EndsWith(".wal", StringComparison.Ordinal)).ToArray();

    private List<MetricSegmentInfo> V2Source(int series, Func<int, bool>? corrupt = null)
    {
        var items = new List<(SeriesKey, List<MetricDataPoint>, double[]?)>();
        for (int s = 0; s < series; s++)
            items.Add((new SeriesKey("rw.v2", MetricKind.Gauge, "1", Labels(s)),
                       [new MetricDataPoint { TimestampUnixNano = T0 + s * S, Value = s }], null));
        return [MetricGolden.WriteV2File(Path.Combine(_dir, $"legacy-{Guid.NewGuid():N}.mts"), "rw.v2",
                                         MetricGranularity.Raw, items, corrupt)];
    }

    [Fact]
    public void A_source_corrupt_past_the_first_chunk_fails_the_rewrite_with_no_output_left()
    {
        // Series 700 of 1 100 has a point whose timestamp is a string: sound msgpack, so the first
        // pass walks past it (it is not the first chunk's), and the second chunk's read throws —
        // after the first chunk's output has been written.
        var sources = V2Source(1_100, corrupt: s => s == 700);
        string before = Fingerprint(sources);

        Assert.ThrowsAny<Exception>(() => _engine.RewriteMetricInChunks(sources, MetricGranularity.Raw,
                                           static (pts, _) => MetricStorageEngine.DedupeByTimestamp(pts)));

        Assert.Empty(Outputs());                       // the first chunk's file is taken back
        Assert.Equal(before, Fingerprint(sources));    // and the sources are as they were
    }

    [Fact]
    public void A_source_deleted_mid_rewrite_fails_it_with_no_output_left()
    {
        // Retention unlinking a source while a rollup is between chunks: the next chunk's read finds
        // no file. Two sources, so one survives to be checked.
        var sources = new List<MetricSegmentInfo>();
        sources.AddRange(Source(1_100, 0, _ => [1, 2], _ => false));
        sources.AddRange(Source(1_100, 1, _ => [1, 2], _ => false));
        var survivor = sources[^1];
        string survivorBefore = Fingerprint([survivor]);

        int chunksWritten = 0;
        _engine.OnRewriteChunkWrittenForTest = off =>
        {
            chunksWritten++;
            if (off == 0) foreach (var s in sources) if (s != survivor) File.Delete(s.FilePath);
        };
        try
        {
            Assert.ThrowsAny<IOException>(() => _engine.RewriteMetricInChunks(sources, MetricGranularity.Raw,
                                               static (pts, _) => MetricStorageEngine.DedupeByTimestamp(pts)));
        }
        finally { _engine.OnRewriteChunkWrittenForTest = null; }

        Assert.Equal(1, chunksWritten);                // it failed after an output existed
        Assert.Empty(Outputs());
        Assert.Equal(survivorBefore, Fingerprint([survivor]));
    }

    [Theory]
    [InlineData(300)]    // one chunk
    [InlineData(1_100)]  // three chunks
    public void The_last_bounds_seen_win_and_a_pointless_series_is_still_written(int series)
    {
        double[] first = [1, 2, 3], second = [5, 6, 7];
        var sources = new List<MetricSegmentInfo>();
        sources.AddRange(Source(series, 0, _ => first, s => s % 11 == 3));
        // The second source re-buckets the even series and records no bounds for the odd ones.
        sources.AddRange(Source(series, 1, s => s % 2 == 0 ? second : null, s => s % 11 == 3));

        var outputs = _engine.RewriteMetricInChunks(sources, MetricGranularity.Raw,
                                                    static (pts, _) => MetricStorageEngine.DedupeByTimestamp(pts));

        var seen = new Dictionary<string, MetricSeries>();
        foreach (var o in outputs)
            foreach (var s in MetricReader.ReadAllSync(o.FilePath))
                seen.Add(s.Labels.ValueAt(0), s);

        Assert.Equal(series, seen.Count);
        for (int s = 0; s < series; s++)
        {
            var got = seen["r" + s.ToString("D4", CultureInfo.InvariantCulture)];
            Assert.Equal(s % 2 == 0 ? second : first, got.BucketBounds);
            Assert.Equal(s % 11 == 3 ? 0 : 6, got.Points.Count);
        }
    }
}
