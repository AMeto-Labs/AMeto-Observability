using System.Buffers.Binary;
using System.Globalization;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Xunit.Abstractions;
using static Ameto.Storage.Tests.MetricGolden;

namespace Ameto.Storage.Tests;

/// <summary>
/// What <see cref="MetricWriter"/> puts on disk, pinned as bytes for the shapes the chunked-rewrite
/// goldens (<c>MetricDownsampleGoldenTests</c>) never write: those are single-metric rewrites whose
/// sections are all far over 64 KB. When the writer moved to a rented buffer and a stack-built
/// header (#94), these were CAPTURED FROM THE WRITER BEFORE IT (7d67eeb, a scratch worktree) and
/// asserted on the new one:
/// <list type="bullet">
/// <item>a flush-shaped call — several metrics interleaved, one over the 512-series cap, histograms,
/// a series with no points and one with a single point, series whose points arrive out of order;</item>
/// <item>sections of exactly 65 535, 65 536 and 65 537 raw bytes — the boundary at which
/// <c>LZ4Pickler</c>'s IBufferWriter overload writes a different header than the span and array
/// overloads (the writer must stay on the latter);</item>
/// <item>a metric name over 482 UTF-8 bytes, whose index and footer no longer fit the writer's
/// stack buffer and go through a rented one.</item>
/// </list>
/// Each hash covers, per output file in order: the name as written less its random nonce, the
/// metric, granularity, format, range, size and every byte.
/// </summary>
public sealed class MetricWriterBytesGoldenTests(ITestOutputHelper output) : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mwgold-" + Guid.NewGuid().ToString("N"));

    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private const long T0 = 1_784_800_000_000_000_000L;
    private const long S  = 1_000_000_000L;
    private static readonly double[] Bounds = [0.005, 0.05, 0.5, 5];

    private static LabelSet Labels(int s, string pad = "") => new(new Dictionary<string, string>
    {
        ["service.name"] = "svc-" + (s % 7).ToString(CultureInfo.InvariantCulture),
        ["route"]        = "/api/v2/items/" + (s % 23).ToString(CultureInfo.InvariantCulture),
        ["replica"]      = s.ToString(CultureInfo.InvariantCulture) + pad,
    });

    /// <summary>Deterministic points: <paramref name="n"/> of them; out of order when asked; histograms with cumulative buckets.</summary>
    private static List<MetricDataPoint> Points(int s, int n, MetricKind kind, bool outOfOrder = false)
    {
        var pts = new List<MetricDataPoint>(n);
        long count = 0;
        var cum = new long[Bounds.Length + 1];
        for (int p = 0; p < n; p++)
        {
            long ts = T0 + (p * 15 + s % 11) * S + s * 1_234_567L;
            long[]? buckets = null;
            if (kind == MetricKind.Histogram && (p + s) % 3 != 0)
            {
                cum[(p + s) % cum.Length]++;
                count++;
                buckets = (long[])cum.Clone();
            }
            double value = kind == MetricKind.Gauge ? (s * 31 + p * 7) % 1000 / 8.0 : s * 100 + p;
            pts.Add(P(ts, value,
                      count:   kind == MetricKind.Histogram ? count : 0,
                      sum:     kind == MetricKind.Histogram ? count * 0.125 : 0,
                      buckets: buckets));
        }
        if (outOfOrder && n > 2) (pts[0], pts[n - 1]) = (pts[n - 1], pts[0]);
        return pts;
    }

    private static (SeriesKey, HotSeries) Series(string metric, MetricKind kind, int s, List<MetricDataPoint> pts, string pad = "") =>
        (new SeriesKey(metric, kind, kind == MetricKind.Gauge ? "By" : "s", Labels(s, pad)),
         new HotSeries(pts, kind == MetricKind.Histogram ? Bounds : null));

    private string Hash(List<MetricSegmentInfo> outputs)
    {
        using var h = NewHash();
        Add(h, (long)outputs.Count);
        foreach (var o in outputs)
        {
            string name = Path.GetFileName(o.FilePath);
            Add(h, name[..^13]);   // all but "-{nonce8}.mts"
            AddFile(h, o);
        }
        return Finish(h);
    }

    private static int RawSectionBytes(string file)
    {
        Span<byte> b = stackalloc byte[4];
        using var fs = File.OpenRead(file);
        fs.Seek(28, SeekOrigin.Begin);
        fs.ReadExactly(b);
        return (int)BinaryPrimitives.ReadUInt32LittleEndian(b);
    }

    // ── The shapes ────────────────────────────────────────────────────────────

    private List<MetricSegmentInfo> Flush()
    {
        string[]     names = ["flush.gauge", "flush.histogram", "flush.counter"];
        MetricKind[] kinds = [MetricKind.Gauge, MetricKind.Histogram, MetricKind.Counter];
        var items = new List<(SeriesKey, HotSeries)>();
        for (int s = 0; s < 1_200; s++)
        {
            int m = s % 3 == 0 || s > 560 ? 0 : s % 3;   // the gauge passes the 512-series cap
            int n = s % 17 == 4 ? 0 : s % 17 == 5 ? 1 : 3 + s % 9;
            items.Add(Series(names[m], kinds[m], s, Points(s, n, kinds[m], outOfOrder: s % 13 == 2)));
        }
        return MetricWriter.Write(_dir, items, MetricGranularity.Raw);
    }

    /// <summary>
    /// One metric whose section is exactly <paramref name="rawBytes"/> long: the first series' last
    /// label is padded until the writer says so (a str16 value, so a character is a byte). Every
    /// attempt but the last is deleted.
    /// </summary>
    private List<MetricSegmentInfo> SectionOf(int rawBytes)
    {
        int pad = 1_000;
        for (int attempt = 0; attempt < 8; attempt++)
        {
            var items = new List<(SeriesKey, HotSeries)>();
            items.Add(Series("edge.gauge", MetricKind.Gauge, 0, Points(0, 40, MetricKind.Gauge), new string('p', pad)));
            for (int s = 1; s < 110; s++)
                items.Add(Series("edge.gauge", MetricKind.Gauge, s, Points(s, 40, MetricKind.Gauge)));
            var outputs = MetricWriter.Write(_dir, items, MetricGranularity.FiveMin);
            int got = RawSectionBytes(outputs[0].FilePath);
            if (got == rawBytes) return outputs;
            foreach (var o in outputs) File.Delete(o.FilePath);
            pad += rawBytes - got;
        }
        throw new InvalidOperationException($"could not land a section on {rawBytes} bytes");
    }

    private List<MetricSegmentInfo> LongName()
    {
        string metric = "long." + new string('€', 165);   // 5 + 495 UTF-8 bytes
        var items = new List<(SeriesKey, HotSeries)>();
        for (int s = 0; s < 5; s++) items.Add(Series(metric, MetricKind.Counter, s, Points(s, 4, MetricKind.Counter)));
        return MetricWriter.Write(_dir, items, MetricGranularity.OneHour);
    }

    [Theory]
    [InlineData("flush",     "3E8C071B2154B3EE")]
    [InlineData("65535",     "5C8499D047341876")]
    [InlineData("65536",     "6C8DC9BAB3E2A33B")]
    [InlineData("65537",     "0636C6BB957DCD53")]
    [InlineData("long-name", "86EDD5157CD060EA")]
    public void The_writer_writes_the_bytes_the_writer_before_it_wrote(string shape, string expected)
    {
        Directory.CreateDirectory(_dir);
        var outputs = shape switch
        {
            "flush"     => Flush(),
            "long-name" => LongName(),
            _           => SectionOf(int.Parse(shape, CultureInfo.InvariantCulture)),
        };

        if (shape is "65535" or "65536" or "65537")
            Assert.Equal(int.Parse(shape, CultureInfo.InvariantCulture), RawSectionBytes(outputs[0].FilePath));
        if (shape == "flush")
            Assert.Equal(["flush.gauge", "flush.gauge", "flush.histogram", "flush.counter"], outputs.Select(o => o.MetricName));
        if (shape == "long-name")
            Assert.True(System.Text.Encoding.UTF8.GetByteCount(outputs[0].MetricName) > 482);

        string actual = Hash(outputs);
        output.WriteLine($"{shape}: {actual} ({outputs.Count} file(s))");
        Assert.Equal(expected, actual);
    }
}
