using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Ameto.Metrics;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// The series row writer against the path it replaced (issue #83 WP7, M#9): the old endpoints built
/// <see cref="MetricSeriesDto"/>s — <c>Pairs.ToDictionary</c> for the labels, a
/// <see cref="MetricPointDto"/> per point — and serialized them through reflection over ASP.NET
/// Core's <c>JsonOptions</c> (web defaults + the relaxed encoder). That path is rebuilt here as the
/// ORACLE, and the writer must match it byte for byte on seeded series far wider than the
/// endpoint goldens: every character class the encoder treats differently, every double corner
/// that is finite, long extremes, empty label sets and empty point lists.
/// </summary>
public sealed class MetricSeriesJsonTests
{
    private readonly ITestOutputHelper _out;
    public MetricSeriesJsonTests(ITestOutputHelper output) => _out = output;

    /// <summary>ASP.NET Core's <c>JsonOptions.SerializerOptions</c>, as <c>Results.Json</c> used them.</summary>
    private static readonly JsonSerializerOptions AspNetDefaults = new(JsonSerializerDefaults.Web)
    {
        Encoder          = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        TypeInfoResolver = new DefaultJsonTypeInfoResolver(),
    };

    /// <summary>The endpoint's old ToDto, verbatim.</summary>
    private static MetricSeriesDto ToDto(MetricSeries s) => new()
    {
        Name   = s.Name,
        Kind   = s.Kind.ToString(),
        Unit   = s.Unit,
        Labels = s.Labels.Pairs.ToDictionary(t => t.Key, t => t.Value),
        Points = s.Points.Select(p => new MetricPointDto
        {
            Ts    = p.TimestampUnixNano,
            Value = p.Value,
            Count = p.Count,
            Sum   = p.Sum,
        }).ToList(),
    };

    private static string Oracle(IReadOnlyList<MetricSeries> series) =>
        JsonSerializer.Serialize(series.Select(ToDto).ToList(), AspNetDefaults);

    private static string Written(IReadOnlyList<MetricSeries> series)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var w = new Utf8JsonWriter(buffer, MetricSeriesJson.WriterOptions))
        {
            w.WriteStartArray();
            foreach (var s in series) MetricSeriesJson.Write(w, s);
            w.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    // Characters the relaxed encoder writes raw and ones it escapes: HTML-sensitive, quotes,
    // backslash, controls, non-Latin BMP, a surrogate pair, a lone surrogate, U+2028, U+FFFD.
    private static readonly string[] Pieces =
    [
        "a", "Z", "/", " ", "<", ">", "&", "'", "\"", "\\", "+", "`", C(0x0000), C(0x0001), "\t", "\n", C(0x007F),
        C(0x00E9), C(0x0416), C(0x4E2D), C(0x2028), C(0xFFFD), C(0xD83D) + C(0xDE00), C(0xD800), "service.name", "",
    ];

    /// <summary>One UTF-16 code unit, spelled as a number so the source file holds nothing but ASCII.</summary>
    private static string C(int code) => ((char)code).ToString();

    private static string RandomText(GoldenRng rng)
    {
        var sb = new StringBuilder();
        int n = rng.Next(5);
        for (int i = 0; i < n; i++) sb.Append(Pieces[rng.Next(Pieces.Length)]);
        return sb.ToString();
    }

    private static double RandomDouble(GoldenRng rng) => rng.Next(14) switch
    {
        0  => -0.0,
        1  => 0.1,
        2  => 5e-324,
        3  => double.MaxValue,
        4  => double.MinValue,
        5  => 1e300,
        6  => 123456789.123,
        7  => rng.Next(1_000_000),
        8  => BitConverter.Int64BitsToDouble((long)(rng.NextU64() & 0x7FEF_FFFF_FFFF_FFFFUL)),   // finite, any bits
        9  => -BitConverter.Int64BitsToDouble((long)(rng.NextU64() & 0x7FEF_FFFF_FFFF_FFFFUL)),
        _  => (rng.NextDouble() - 0.5) * Math.Pow(10, rng.Next(40) - 20),
    };

    private static List<MetricSeries> RandomSeries(GoldenRng rng, int count)
    {
        var all = new List<MetricSeries>();
        for (int s = 0; s < count; s++)
        {
            var pairs = new Dictionary<string, string>(StringComparer.Ordinal);
            int n = rng.Next(5);
            for (int i = 0; i < n; i++) pairs[RandomText(rng) + i.ToString(CultureInfo.InvariantCulture)] = RandomText(rng);
            var pts = new List<MetricDataPoint>();
            int np = rng.Next(4) == 0 ? 0 : rng.Next(30);
            for (int i = 0; i < np; i++)
                pts.Add(new MetricDataPoint
                {
                    TimestampUnixNano = rng.Next(10) switch { 0 => long.MaxValue, 1 => long.MinValue, 2 => 0, _ => (long)rng.NextU64() },
                    Value = RandomDouble(rng),
                    Count = rng.Next(10) == 0 ? long.MinValue : (long)rng.NextU64(),
                    Sum   = RandomDouble(rng),
                });
            all.Add(new MetricSeries
            {
                Name   = RandomText(rng) + "m",
                Kind   = (MetricKind)(rng.Next(10) == 0 ? 7 : rng.Next(3)),        // 7: a kind with no name
                Unit   = RandomText(rng),
                Labels = new LabelSet(pairs),
                Points = pts,
            });
        }
        return all;
    }

    [Fact]
    public void The_row_writer_writes_what_the_DTO_serializer_wrote()
    {
        var rng = new GoldenRng(0x5E_71_A1_12);
        for (int round = 0; round < 200; round++)
        {
            var series = RandomSeries(rng, rng.Next(6));
            Assert.Equal(Oracle(series), Written(series));
        }
    }

    [Fact]
    public void It_fails_where_the_DTO_path_failed()
    {
        var dup = new MetricSeries { Name = "m", Labels = new LabelSet([new("k", "1"), new("k", "2")]) };
        Assert.Throws<ArgumentException>(() => Oracle([dup]));
        Assert.Throws<ArgumentException>(() => Written([dup]));

        foreach (double bad in new[] { double.NaN, double.PositiveInfinity, double.NegativeInfinity })
        {
            var valued = new MetricSeries { Name = "m", Points = [new MetricDataPoint { Value = bad }] };
            var summed = new MetricSeries { Name = "m", Points = [new MetricDataPoint { Sum = bad }] };
            Assert.Throws<ArgumentException>(() => Oracle([valued]));
            Assert.Throws<ArgumentException>(() => Written([valued]));
            Assert.Throws<ArgumentException>(() => Oracle([summed]));
            Assert.Throws<ArgumentException>(() => Written([summed]));
        }
    }

    /// <summary>
    /// Probe: what answering 2 000 series x 60 points costs, the DTO way (ToDto + reflection
    /// serialization, the old endpoints) and the row writer's, into the same kind of buffer.
    /// Per-thread bytes, best of 5, nothing printed inside a measurement.
    /// </summary>
    [Fact]
    public void Probe_series_answer_serialization()
    {
        var series = new List<MetricSeries>(2_000);
        for (int s = 0; s < 2_000; s++)
        {
            var pts = new List<MetricDataPoint>(60);
            for (int p = 0; p < 60; p++)
                pts.Add(new MetricDataPoint { TimestampUnixNano = 1_784_800_020_000_000_000L + p * 15_000_000_000L, Value = p * 0.25 + s, Count = p, Sum = p * 1.5 });
            series.Add(new MetricSeries
            {
                Name = "http.server.request.duration", Kind = MetricKind.Histogram, Unit = "s",
                Labels = new LabelSet(new Dictionary<string, string>
                {
                    ["service.name"] = "svc-" + (s % 10).ToString(CultureInfo.InvariantCulture),
                    ["http.route"] = "/api/v1/r" + (s % 20).ToString(CultureInfo.InvariantCulture),
                    ["http.request.method"] = s % 2 == 0 ? "GET" : "POST",
                }),
                Points = pts,
            });
        }

        var sink = new ArrayBufferWriter<byte>(8 << 20);
        (long Bytes, double Ms, int Out) Measure(Action run)
        {
            run();
            long best = long.MaxValue; double bestMs = double.MaxValue;
            for (int r = 0; r < 5; r++)
            {
                sink.ResetWrittenCount();
                long before = GC.GetAllocatedBytesForCurrentThread();
                long t = System.Diagnostics.Stopwatch.GetTimestamp();
                run();
                bestMs = Math.Min(bestMs, System.Diagnostics.Stopwatch.GetElapsedTime(t).TotalMilliseconds);
                best   = Math.Min(best, GC.GetAllocatedBytesForCurrentThread() - before);
            }
            return (best, bestMs, sink.WrittenCount);
        }

        var dto = Measure(() =>
        {
            using var w = new Utf8JsonWriter(sink, new JsonWriterOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping });
            JsonSerializer.Serialize(w, series.Select(ToDto).ToList(), AspNetDefaults);
        });
        var rows = Measure(() =>
        {
            using var w = new Utf8JsonWriter(sink, MetricSeriesJson.WriterOptions);
            w.WriteStartArray();
            foreach (var s in series) MetricSeriesJson.Write(w, s);
            w.WriteEndArray();
        });

        _out.WriteLine($"SERIES ANSWER  2 000 series x 60 points, {rows.Out / 1024.0:N0} KB of JSON; best of 5");
        _out.WriteLine($"  DTO + reflection : {dto.Bytes / 1048576.0,6:N2} MB  {dto.Ms,6:N1} ms");
        _out.WriteLine($"  row writer       : {rows.Bytes / 1048576.0,6:N2} MB  {rows.Ms,6:N1} ms");

        Assert.Equal(dto.Out, rows.Out);
        Assert.True(rows.Bytes < 64 * 1024, $"the row writer allocated {rows.Bytes:N0} B for 120 000 points");
    }
}
