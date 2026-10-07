using System.Buffers.Binary;
using System.Globalization;
using Ameto.Metrics;
using Ameto.Metrics.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE READER PARKS NO BLOCK OVER 8 MiB IN THE SHARED POOL (#125).
///
/// <para>A v3 block is a whole file's series section, up to 64 MiB. The reader rented the compressed
/// and the inflated block from <c>ArrayPool.Shared</c> whatever their size, and the pool rounds up to a
/// power of two and keeps what it is handed: on the 512 MB stand a 58 MiB block parked two 64 MiB
/// arrays there after the read — and a FiveMin merge failed with an OutOfMemoryException inside the
/// pool's <c>Rent</c>. The line is 8 MiB — <c>IngestBufferPool</c>'s largest array — and not the
/// writer's megabyte: a read happens per query, and at a megabyte every cold query over an ordinary
/// file allocated that file's block (<c>MetricQueryAllocProbe</c>). The compressed copy also goes back
/// before the first series is walked.</para>
/// </summary>
public sealed class MetricReaderPoolTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mrpool-" + Guid.NewGuid().ToString("N"));

    public MetricReaderPoolTests() => Directory.CreateDirectory(_dir);

    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private const long T0 = 1_784_800_020_000_000_000L;
    private const long S  = 1_000_000_000L;

    private static LabelSet Labels(int s) => new([new("pod", "pod-" + s.ToString(CultureInfo.InvariantCulture))]);

    /// <summary>Random doubles do not compress: both the section and its compressed form are large.</summary>
    private MetricSegmentInfo V3File(int series, int points, int seed)
    {
        var rng   = new Random(seed);
        var items = new List<(SeriesKey, HotSeries)>(series);
        for (int s = 0; s < series; s++)
        {
            var pts = new List<MetricDataPoint>(points);
            for (int p = 0; p < points; p++)
                pts.Add(new MetricDataPoint { TimestampUnixNano = T0 + p * 15 * S, Value = rng.NextDouble() * 1e6 });
            items.Add((new SeriesKey("pool.gauge", MetricKind.Gauge, "1", Labels(s)), new HotSeries(pts)));
        }
        return Assert.Single(MetricWriter.Write(_dir, items, MetricGranularity.Raw));
    }

    private static (uint Raw, uint Comp) Sizes(string path)
    {
        using var fs = File.OpenRead(path);
        Span<byte> b = stackalloc byte[8];
        fs.Seek(28, SeekOrigin.Begin);
        fs.ReadExactly(b);
        return (BinaryPrimitives.ReadUInt32LittleEndian(b), BinaryPrimitives.ReadUInt32LittleEndian(b[4..]));
    }

    [Fact]
    public void The_reader_parks_no_block_over_its_line_and_still_parks_an_ordinary_one()
    {
        // Sizes stated as literals, not through the line under test, so moving the line fails the
        // assertions about what was pooled rather than this setup.
        const long MiB = 1024 * 1024;
        var big    = V3File(series: 512, points: 2_400, seed: 1);
        var medium = V3File(series: 512, points: 300,   seed: 2);
        var (raw, comp) = Sizes(big.FilePath);
        Assert.True(comp > 9 * MiB && raw > 9 * MiB, $"setup: the big file's block is {raw:N0} B ({comp:N0} compressed)");
        var (mraw, mcomp) = Sizes(medium.FilePath);
        Assert.True(mraw > MiB && mraw < 4 * MiB && mcomp < 4 * MiB,
                    $"setup: the ordinary file's block is {mraw:N0} B ({mcomp:N0} compressed)");

        // A legacy v2 file whose ONE series block is over the line too.
        var rng = new Random(3);
        var pts = new List<MetricDataPoint>();
        for (int p = 0; p < 600_000; p++) pts.Add(new MetricDataPoint { TimestampUnixNano = T0 + p * S, Value = rng.NextDouble() * 1e6 });
        var v2 = MetricGolden.WriteV2File(Path.Combine(_dir, "legacy.mts"), "pool.v2", MetricGranularity.Raw,
                                          [(new SeriesKey("pool.v2", MetricKind.Gauge, "1", Labels(0)), pts, null)]);
        var (v2raw, v2comp) = Sizes(v2.FilePath);   // a v2 file's first series block sits where a v3 section does
        Assert.True(v2raw > 9 * MiB && v2comp > 9 * MiB, $"setup: the v2 series block is {v2raw:N0} B ({v2comp:N0} compressed)");

        var returned = new List<int>();
        MetricReader.ReturnedToPoolForTest = returned.Add;
        try
        {
            Assert.Equal(512, MetricReader.ReadAllSync(big.FilePath).Count());
            Assert.Equal(512, MetricReader.ReadIdentities(big.FilePath).Count());
            Assert.Equal(600_000, Assert.Single(MetricReader.ReadAllSync(v2.FilePath)).Points.Count);
            Assert.Empty(returned);                                   // every block of those was over the line

            Assert.Equal(512, MetricReader.ReadAllSync(medium.FilePath).Count());
        }
        finally { MetricReader.ReturnedToPoolForTest = null; }

        // The ordinary file's blocks — over the writer's megabyte, under the reader's line — go back,
        // and nothing larger than 8 MiB ever does.
        Assert.Contains(returned, n => n > MiB);
        Assert.All(returned, n => Assert.True(n <= 8 * MiB, $"a {n:N0} B block went back to the shared pool"));
    }

    [Fact]
    public void The_compressed_block_goes_back_before_the_first_series_is_walked()
    {
        var file = V3File(series: 3, points: 10, seed: 4);

        var returned = new List<int>();
        MetricReader.ReturnedToPoolForTest = returned.Add;
        try
        {
            using var e = MetricReader.ReadAllSync(file.FilePath).GetEnumerator();
            Assert.True(e.MoveNext());
            Assert.Single(returned);                                  // the compressed copy, not the block being walked
            while (e.MoveNext()) { }
        }
        finally { MetricReader.ReturnedToPoolForTest = null; }

        Assert.Equal(2, returned.Count);                              // and the block itself once the walk is over
    }
}
