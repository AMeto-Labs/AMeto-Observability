using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// <para><c>MetricReader.ReadAllSync</c> used to materialise a whole .mts section as a
/// <c>List&lt;MetricSeries&gt;</c> before yielding anything, so a caller's peak was the
/// FILE's series count no matter how carefully it chunked its own work. That is invisible
/// on files written after the 512-series cap and very visible on the pre-cap files an
/// existing high-cardinality deployment already has on disk.</para>
///
/// <para>This probe measures the live set at the midpoint of an enumeration: streaming
/// (one series decoded at a time) against materialised (what the old reader retained).</para>
/// </summary>
public sealed class MetricReaderStreamingProbe
{
    private readonly ITestOutputHelper _out;
    public MetricReaderStreamingProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void ReadAllSync_RetainedBytes_StreamedVsMaterialised()
    {
        // The sandbox metric that drove the >1 GB rollup: 4,098 series, hourly points.
        const int seriesCount = 4_098, pointsPerSeries = 24;

        string dir = Path.Combine(Path.GetTempPath(), "ameto-rdprobe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var segs  = WriteMetric(dir, seriesCount, pointsPerSeries);
            long disk = segs.Sum(s => new FileInfo(s.FilePath).Length);

            _out.WriteLine($"{seriesCount:N0} series x {pointsPerSeries} points -> " +
                           $"{segs.Count} file(s), {disk / 1024.0:N0} KB on disk");
            _out.WriteLine("");

            long streamed     = LiveAtMidpoint(segs, retain: false);
            long materialised = LiveAtMidpoint(segs, retain: true);

            _out.WriteLine($"  streamed     live@mid = {streamed     / 1048576.0,7:N2} MB");
            _out.WriteLine($"  materialised live@mid = {materialised / 1048576.0,7:N2} MB  (old reader)");
            _out.WriteLine($"  reduction             = {100.0 * (materialised - streamed) / materialised,7:N1} %");

            // The point of the change: retention must not track the file's cardinality.
            Assert.True(streamed < materialised / 2,
                $"streaming retained {streamed} B vs materialised {materialised} B");
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    private static List<MetricSegmentInfo> WriteMetric(string dir, int seriesCount, int pointsPerSeries)
    {
        var items = new List<(SeriesKey, HotSeries)>(seriesCount);
        for (int s = 0; s < seriesCount; s++)
        {
            var labels = new LabelSet(new Dictionary<string, string>
            {
                ["service.name"] = "MintRoute.API",
                ["route"]        = "/api/r" + (s % 211),
                ["pod"]          = "pod-" + s,
            });
            var pts = new List<MetricDataPoint>(pointsPerSeries);
            for (int p = 0; p < pointsPerSeries; p++)
                pts.Add(new MetricDataPoint
                {
                    TimestampUnixNano = 1_784_800_000_000_000_000L + p * 3_600_000_000_000L,
                    Value             = s * 0.5 + p,
                });
            items.Add((new SeriesKey("http.server.duration", MetricKind.Gauge, "ms", labels),
                       new HotSeries(pts, null)));
        }
        return MetricWriter.Write(dir, items, MetricGranularity.OneHour);
    }

    /// <summary>
    /// Live bytes once half the series have been consumed — retention, not allocation rate.
    /// <paramref name="retain"/> holds every decoded series, reproducing exactly what the
    /// old <c>DeserializeSection</c> kept alive while the caller enumerated.
    /// </summary>
    private static long LiveAtMidpoint(List<MetricSegmentInfo> segs, bool retain)
    {
        int total = segs.Sum(s => MetricReader.ReadAllSync(s.FilePath).Count());

        GC.Collect(2, GCCollectionMode.Aggressive, true, true);
        long baseline = GC.GetTotalMemory(true);

        var  held = new List<MetricSeries>();
        long live = 0;
        int  seen = 0;

        foreach (var seg in segs)
            foreach (var s in MetricReader.ReadAllSync(seg.FilePath))
            {
                if (retain) held.Add(s);
                if (++seen == total / 2) live = GC.GetTotalMemory(true) - baseline;
            }

        GC.KeepAlive(held);
        return live;
    }
}
