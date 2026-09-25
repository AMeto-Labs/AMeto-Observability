using System.Diagnostics;
using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT THE LOG'S WRITE LOCK COSTS PER POINT, AND HOW IT SCALES — the log alone, no engine.
///
/// <para>Written for issue #87's placement decision: a per-entry checksum either runs inside
/// <c>_writeLock</c>, hashing each entry from the map, or before it, hashing the point in hand.
/// <c>MetricIngestContentionProbe</c> measures the whole ingest path, where the hot tier's own
/// per-series locks, the snapshot lock and the GC move a per-thread figure by far more than a
/// 48-byte hash does; here the write lock is the ONLY state the threads share, so whatever the
/// checksum adds under it shows as per-thread cost rising with the thread count. Each thread is
/// its own exporter — its own series, resolved outside the lock the way the engine resolves
/// them — and appends OTLP-sized batches into a log big enough never to grow while the clock
/// runs.</para>
///
/// <para>Printed, not asserted: best of five per thread count, per-thread ns/point from each
/// thread's own clock. The CRC32C of one 48-byte header is printed beside it for scale.</para>
/// </summary>
public sealed class MetricWalAppendContentionProbe
{
    private readonly ITestOutputHelper _out;
    public MetricWalAppendContentionProbe(ITestOutputHelper o) => _out = o;

    private const int BatchPoints = 500, Rounds = 100, Repeats = 5;
    private static readonly int[] ThreadCounts = [1, 2, 4, 8];

    [Fact]
    public void Probe_append_cost_per_thread()
    {
        _ = RunSweepPoint(ThreadCounts[^1]);                 // discarded: the parked core's clock step

        _out.WriteLine($"LOG ONLY: {BatchPoints}-point batches of disjoint known series, {Rounds} per thread, best of {Repeats}");
        _out.WriteLine("threads | per-thread ns/point | total M points/s");
        foreach (int threads in ThreadCounts)
        {
            double bestNs = double.MaxValue, bestRate = 0;
            for (int r = 0; r < Repeats; r++)
            {
                var (ns, rate) = RunSweepPoint(threads);
                bestNs   = Math.Min(bestNs, ns);
                bestRate = Math.Max(bestRate, rate);
            }
            _out.WriteLine($"{threads,7} | {bestNs,19:F1} | {bestRate / 1e6,16:F2}");
        }

        // Best of ten passes: the first ones can run Crc32c.Append before tiering has promoted it,
        // which in a process that never hashed anything (the pre-v2 build) is ~50 ns, not ~7.
        var header = new byte[48];
        uint sink = 0;
        double bestCrcNs = double.MaxValue;
        for (int pass = 0; pass < 10; pass++)
        {
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < 1_000_000; i++) sink ^= Crc32c.Append((uint)i, header);
            bestCrcNs = Math.Min(bestCrcNs, sw.Elapsed.TotalNanoseconds / 1_000_000);
        }
        _out.WriteLine($"CRC32C over one 48-byte entry header: {bestCrcNs:F1} ns");
        GC.KeepAlive(sink);
    }

    /// <summary>
    /// WHAT A COMMIT HOLDS THE LOG FOR WHEN ITS TAIL IS FAR LONGER THAN THE PREFIX IT REPLACES — a
    /// flush of a near-empty tier during a burst. The relocation moves the tail in chunks no longer
    /// than the prefix, each with a header store and its checksums, under both of the log's locks;
    /// a one-entry prefix before 160 000 survivors is 160 000 of them. Printed, not asserted: the
    /// commit's wall time for 1- and 16-entry prefixes, and whether it moved anything.
    /// </summary>
    [Fact]
    public void Probe_commit_of_a_long_tail_behind_a_short_prefix()
    {
        foreach (int prefixEntries in new[] { 1, 16, 1 })          // the first 1 warms the JIT
        {
            string dir = Path.Combine(Path.GetTempPath(), "ameto-mwaltail-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(dir);
            try
            {
                using var wal = MetricWriteAheadLog.Open(Path.Combine(dir, "metrics.wal"), 64L * 1024 * 1024);
                var one = new MetricIngestItem[BatchPoints];
                for (int i = 0; i < one.Length; i++)
                    one[i] = new MetricIngestItem
                    {
                        Name = "burst", Kind = MetricKind.Gauge, Labels = new LabelSet([new("s", (i & 7).ToString())]),
                        TimestampUnixNano = 1_785_300_000_000_000_000L + i, ScalarValue = i,
                    };

                wal.Append(one.AsSpan(0, prefixEntries));
                ulong flushing = wal.BeginFlush();
                for (int b = 0; b < 320; b++) wal.Append(one);          // 160 000 survivors, ~8.3 MB
                long before = wal.WrittenBytes;

                var sw = Stopwatch.StartNew();
                wal.CommitFlush(flushing);
                sw.Stop();

                _out.WriteLine($"prefix {prefixEntries,2} entr(ies), tail {before / 1048576.0:F1} MiB: commit {sw.Elapsed.TotalMilliseconds,7:F2} ms, "
                             + $"log after {wal.WrittenBytes / 1048576.0:F2} MiB");
            }
            finally { try { Directory.Delete(dir, true); } catch { } }
        }
    }

    private static (double NsPerPointPerThread, double PointsPerSecond) RunSweepPoint(int threads)
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mwalcontention-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            // 64 MiB: the warm-up and every timed round together stay under its 3/4 mark, so no
            // growth and no pre-grow runs inside the timed window.
            using var wal = MetricWriteAheadLog.Open(Path.Combine(dir, "metrics.wal"), 64L * 1024 * 1024);

            var batches = new MetricIngestItem[threads][];
            for (int t = 0; t < threads; t++)
            {
                batches[t] = new MetricIngestItem[BatchPoints];
                for (int i = 0; i < BatchPoints; i++)
                    batches[t][i] = new MetricIngestItem
                    {
                        Name              = "http.server.request.duration",
                        Kind              = MetricKind.Gauge,
                        Unit              = "ms",
                        Labels            = new LabelSet([new("exporter", t.ToString()), new("s", i.ToString())]),
                        TimestampUnixNano = 1_785_300_000_000_000_000L + i,
                        ScalarValue       = i,
                    };
                wal.Append(batches[t]);                    // registers every series before the clock
            }

            var elapsed = new long[threads];
            var workers = new Thread[threads];
            using var start = new Barrier(threads + 1);
            for (int t = 0; t < threads; t++)
            {
                int me = t;
                workers[t] = new Thread(() =>
                {
                    var batch = batches[me];
                    var index = new uint[batch.Length];
                    start.SignalAndWait();
                    long t0 = Stopwatch.GetTimestamp();
                    for (int r = 0; r < Rounds; r++)
                    {
                        long epoch = wal.SeriesEpoch;       // as MetricStorageEngine.Ingest does
                        for (int i = 0; i < batch.Length; i++) index[i] = wal.ResolveSeries(batch[i]);
                        wal.AppendResolved(batch, index, epoch);
                    }
                    elapsed[me] = Stopwatch.GetTimestamp() - t0;
                }) { IsBackground = true };
                workers[t].Start();
            }

            var wall = Stopwatch.StartNew();
            start.SignalAndWait();
            foreach (var w in workers) w.Join();
            wall.Stop();

            double sumNs = 0;
            for (int t = 0; t < threads; t++)
                sumNs += elapsed[t] * 1e9 / Stopwatch.Frequency / ((double)Rounds * BatchPoints);
            return (sumNs / threads, (double)threads * Rounds * BatchPoints / wall.Elapsed.TotalSeconds);
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }
}
