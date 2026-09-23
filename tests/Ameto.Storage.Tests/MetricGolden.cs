using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using K4os.Compression.LZ4;
using MessagePack;

namespace Ameto.Storage.Tests;

/// <summary>
/// Shared machinery for the metric GOLDEN tests: a seeded generator that means the same thing on
/// every machine, and a hash over results that reads every bit a caller could observe.
///
/// <para><b>Why bits and not text.</b> The dev box runs ru-KZ (decimal ','), CI runs en-US, and a
/// golden captured through <c>double.ToString()</c> would be a golden of the culture. Every double
/// is hashed as its IEEE bits — which is also the only form in which 0.0 and -0.0, or two NaN
/// payloads, stay apart — every long as its bytes, every string as UTF-8.</para>
/// </summary>
internal static class MetricGolden
{
    /// <summary>First 16 hex digits of a SHA-256 — ample to tell two answers apart.</summary>
    public static string Finish(IncrementalHash h) => Convert.ToHexString(h.GetHashAndReset())[..16];

    public static IncrementalHash NewHash() => IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

    public static void Add(IncrementalHash h, long v)
    {
        Span<byte> b = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(b, v);
        h.AppendData(b);
    }

    public static void Add(IncrementalHash h, double v) => Add(h, BitConverter.DoubleToInt64Bits(v));

    public static void Add(IncrementalHash h, string? s)
    {
        if (s is null) { Add(h, -1L); return; }
        Add(h, (long)s.Length);
        h.AppendData(Encoding.UTF8.GetBytes(s));
    }

    public static void Add(IncrementalHash h, in MetricDataPoint p)
    {
        Add(h, p.TimestampUnixNano);
        Add(h, p.Value);
        Add(h, p.Count);
        Add(h, p.Sum);
        if (p.BucketCounts is null) Add(h, -1L);
        else
        {
            Add(h, (long)p.BucketCounts.Length);
            foreach (long c in p.BucketCounts) Add(h, c);
        }
    }

    public static void Add(IncrementalHash h, IReadOnlyList<MetricDataPoint> points)
    {
        Add(h, (long)points.Count);
        for (int i = 0; i < points.Count; i++) Add(h, points[i]);
    }

    public static void Add(IncrementalHash h, MetricSeries s)
    {
        Add(h, s.Name);
        Add(h, (long)s.Kind);
        Add(h, s.Unit);
        Add(h, (long)s.Labels.Count);
        for (int i = 0; i < s.Labels.Count; i++) { Add(h, s.Labels.KeyAt(i)); Add(h, s.Labels.ValueAt(i)); }
        if (s.BucketBounds is null) Add(h, -1L);
        else
        {
            Add(h, (long)s.BucketBounds.Length);
            foreach (double d in s.BucketBounds) Add(h, d);
        }
        Add(h, s.Points);
    }

    public static string Hash(IReadOnlyList<MetricDataPoint> points)
    {
        using var h = NewHash();
        Add(h, points);
        return Finish(h);
    }

    public static string Hash(IEnumerable<MetricSeries> series)
    {
        using var h = NewHash();
        long n = 0;
        foreach (var s in series) { Add(h, s); n++; }
        Add(h, n);
        return Finish(h);
    }

    /// <summary>One file as the disk holds it: every byte, plus the catalog facts the writer reports.</summary>
    public static void AddFile(IncrementalHash h, MetricSegmentInfo info)
    {
        Add(h, info.MetricName);
        Add(h, (long)info.Granularity);
        Add(h, (long)info.FormatVersion);
        Add(h, info.MinNano);
        Add(h, info.MaxNano);
        Add(h, info.SizeBytes);
        byte[] bytes = File.ReadAllBytes(info.FilePath);
        Add(h, (long)bytes.Length);
        h.AppendData(bytes);
    }

    public static MetricDataPoint P(long ts, double v, long count = 0, double sum = 0, long[]? buckets = null) =>
        new() { TimestampUnixNano = ts, Value = v, Count = count, Sum = sum, BucketCounts = buckets };

    /// <summary>
    /// A legacy v2 <c>.mts</c> (per-series LZ4 blocks, absolute nanosecond timestamps, always
    /// five-field points) — the pre-v3 writer, as <c>MetricFormatV3Tests</c> keeps it, so a golden
    /// can put the reader's v2 path under the same pin as its v3 one.
    /// </summary>
    public static MetricSegmentInfo WriteV2File(
        string filePath, string metricName, MetricGranularity granularity,
        List<(SeriesKey Key, List<MetricDataPoint> Points, double[]? Bounds)> items)
    {
        long minNano = long.MaxValue, maxNano = long.MinValue;
        foreach (var (_, pts, _) in items)
            foreach (var p in pts)
            {
                minNano = Math.Min(minNano, p.TimestampUnixNano);
                maxNano = Math.Max(maxNano, p.TimestampUnixNano);
            }

        using (var fs = new FileStream(filePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 65536))
        using (var bw = new BinaryWriter(fs))
        {
            bw.Write(0x52_44_4D_54u); // "RDMT"
            bw.Write((ushort)2);
            bw.Write((byte)granularity);
            bw.Write((uint)items.Count);
            bw.Write(minNano);
            bw.Write(maxNano);
            bw.Write((byte)0);

            long firstBlock = fs.Position;
            foreach (var (key, pts, bounds) in items)
            {
                var buf = new System.Buffers.ArrayBufferWriter<byte>();
                var w = new MessagePackWriter(buf);
                w.WriteMapHeader(6);
                w.Write("k");   w.Write((byte)key.Kind);
                w.Write("u");   w.Write(key.Unit);
                w.Write("lbs");
                w.WriteMapHeader(key.Labels.Count);
                for (int i = 0; i < key.Labels.Count; i++) { w.Write(key.Labels.KeyAt(i)); w.Write(key.Labels.ValueAt(i)); }
                w.Write("bnds");
                w.WriteArrayHeader(bounds?.Length ?? 0);
                if (bounds is not null) foreach (var b in bounds) w.Write(b);
                w.Write("pts");
                w.WriteArrayHeader(pts.Count);
                foreach (var p in pts)
                {
                    w.WriteArrayHeader(5);
                    w.Write(p.TimestampUnixNano);
                    w.Write(p.Value);
                    w.Write(p.Count);
                    w.Write(p.Sum);
                    if (p.BucketCounts is { Length: > 0 } bc)
                    {
                        w.WriteArrayHeader(bc.Length);
                        foreach (var c in bc) w.Write(c);
                    }
                    else w.WriteNil();
                }
                w.Write("cnt"); w.Write((uint)pts.Count);
                w.Flush();

                var raw        = buf.WrittenSpan.ToArray();
                var compressed = LZ4Pickler.Pickle(raw);
                bw.Write((uint)raw.Length);
                bw.Write((uint)compressed.Length);
                bw.Write(compressed);
            }

            long nameIdxOffset = fs.Position;
            bw.Write((uint)1);
            var nameBytes = Encoding.UTF8.GetBytes(metricName);
            bw.Write((ushort)nameBytes.Length);
            bw.Write(nameBytes);
            bw.Write((ulong)firstBlock);
            bw.Write((uint)items.Count);
            bw.Write((ulong)nameIdxOffset);
            bw.Write(0x52_44_4D_46u); // "RDMF"
        }

        return MetricReader.ReadSegmentInfo(filePath);
    }
}

/// <summary>
/// xorshift64* — seeded, and the same sequence on every runtime and machine, which
/// <see cref="Random"/> promises only for its legacy seeded algorithm and a golden should not have
/// to lean on.
/// </summary>
internal sealed class GoldenRng(ulong seed)
{
    private ulong _s = seed == 0 ? 0x9E3779B97F4A7C15UL : seed;

    public ulong NextU64()
    {
        _s ^= _s >> 12;
        _s ^= _s << 25;
        _s ^= _s >> 27;
        return _s * 2685821657736338717UL;
    }

    /// <summary>Uniform in [0, max).</summary>
    public int Next(int max) => (int)(NextU64() % (ulong)max);

    /// <summary>Uniform in [0, 1).</summary>
    public double NextDouble() => (NextU64() >> 11) * (1.0 / (1UL << 53));

    public bool Chance(int percent) => Next(100) < percent;

    public void Shuffle<T>(IList<T> list)
    {
        for (int i = list.Count - 1; i > 0; i--)
        {
            int j = Next(i + 1);
            (list[i], list[j]) = (list[j], list[i]);
        }
    }
}
