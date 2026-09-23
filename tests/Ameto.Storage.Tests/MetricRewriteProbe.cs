using System.Diagnostics;
using System.Globalization;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// What one metric's chunked rewrite costs — the rollup's and the compaction's unit of work
/// (issue #83 WP7, M#12). It used to decode every source in full once to learn the keys, then
/// re-open, re-inflate and fully decode every source again for EVERY 512-series chunk, labels
/// and all, keeping only the chunk's series. The rewrite is synchronous, so per-thread counters
/// see all of it; best of 3, nothing printed inside a measurement. What it writes is pinned by
/// <c>MetricDownsampleGoldenTests</c>; this measures and bounds the cost.
/// </summary>
public sealed class MetricRewriteProbe : IDisposable
{
    private readonly ITestOutputHelper _out;
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mrwprobe-" + Guid.NewGuid().ToString("N"));

    public MetricRewriteProbe(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_dir);
    }

    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private const long S = 1_000_000_000L;

    /// <summary>Four raw sources of one gauge metric, <paramref name="series"/> series each, 15 points a series.</summary>
    private List<MetricSegmentInfo> Sources(int series)
    {
        var all = new List<MetricSegmentInfo>();
        for (int file = 0; file < 4; file++)
        {
            var items = new List<(SeriesKey, HotSeries)>(series);
            for (int s = 0; s < series; s++)
            {
                var labels = new LabelSet(new Dictionary<string, string>
                {
                    ["service.name"] = "svc-" + (s % 10).ToString(CultureInfo.InvariantCulture),
                    ["http.route"]   = "/api/v1/r" + (s % 40).ToString(CultureInfo.InvariantCulture),
                    ["pod"]          = "pod-" + s.ToString(CultureInfo.InvariantCulture),
                });
                var pts = new List<MetricDataPoint>(15);
                for (int p = 0; p < 15; p++)
                    pts.Add(new MetricDataPoint { TimestampUnixNano = 1_784_800_020_000_000_000L + (file * 15 + p) * 15 * S, Value = s + p });
                items.Add((new SeriesKey("rw.gauge", MetricKind.Gauge, "By", labels), new HotSeries(pts)));
            }
            all.AddRange(MetricWriter.Write(_dir, items, MetricGranularity.Raw));
        }
        return all;
    }

    [Theory]
    [InlineData(400)]     // one chunk
    [InlineData(2_000)]   // four chunks
    public async Task Probe_chunked_rewrite(int series)
    {
        var sources = Sources(series);
        string engineDir = Path.Combine(_dir, "engine");
        Directory.CreateDirectory(engineDir);
        await using var engine = new MetricStorageEngine(engineDir, NullLogger<MetricStorageEngine>.Instance,
                                                         new Ameto.Core.MetricsOptions { HotTierBytes = 1L << 30 });
        await engine.ColdLoadCompleted;

        long bestBytes = long.MaxValue; double bestMs = double.MaxValue; int files = 0;
        for (int run = 0; run < 3; run++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            long t      = Stopwatch.GetTimestamp();
            var outputs = engine.RewriteMetricInChunks(sources, MetricGranularity.Raw, static (pts, _) => MetricStorageEngine.DedupeByTimestamp(pts));
            double ms   = Stopwatch.GetElapsedTime(t).TotalMilliseconds;
            long bytes  = GC.GetAllocatedBytesForCurrentThread() - before;
            bestBytes = Math.Min(bestBytes, bytes);
            bestMs    = Math.Min(bestMs, ms);
            files     = outputs.Count;
            foreach (var o in outputs) File.Delete(o.FilePath);
        }

        long points = 4L * series * 15;
        _out.WriteLine($"REWRITE  {series:N0} series x 4 sources x 15 points = {points:N0} points -> {files} file(s); best of 3");
        _out.WriteLine($"  {bestMs,8:N1} ms | {bestBytes / 1048576.0,7:N2} MB | {(double)bestBytes / points,6:N1} B/point | {(double)bestBytes / series,8:N0} B/series");

        Assert.Equal((series + 511) / 512, files);
    }
}
