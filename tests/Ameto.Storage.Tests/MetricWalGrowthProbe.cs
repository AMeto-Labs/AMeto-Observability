using System.Diagnostics;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT A LOG GROWTH COSTS THE THREADS THAT ARE NOT GROWING IT. M#10 (perf half) of issue #83.
///
/// <para><c>Grow()</c> ran inside <c>lock (_writeLock)</c> and unmapped, re-sized and re-mapped
/// the file there, so every ingest thread in the process queued behind one file resize. The
/// probe has several threads append OTLP-sized batches from an 8 MiB log until it has grown
/// four times, and reports, PER THREAD, the slowest append call and how many took over a
/// millisecond — the stall is what the non-growing threads see, so it is read off them.</para>
/// </summary>
public sealed class MetricWalGrowthProbe
{
    private readonly ITestOutputHelper _out;
    public MetricWalGrowthProbe(ITestOutputHelper output) => _out = output;

    private const int Threads        = 4;
    private const int BatchPoints    = 500;
    private const int BatchesPerThread = 1_400;   // 4 x 1 400 x 500 x 48 B = 134 MB: 8 -> 16 -> 32 -> 64 -> 128 -> 256 MiB

    /// <summary>
    /// THE STALL ITSELF, JUDGED BY A SEAM. A batch that does not fit parks inside its growth with
    /// the new mapping built (<c>OnGrowMappedForTest</c>); a batch that DOES fit must go through
    /// meanwhile. Revert to growing inside <c>_writeLock</c> and the small append waits for the
    /// release — the bounded wait below then fails; it bounds a failure, it does not decide one.
    /// Afterwards all three batches replay, in the order they took the log: fill, small, big.
    /// </summary>
    [Fact]
    public void An_append_that_fits_does_not_wait_for_a_growth_in_flight()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mwalgrowseam-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            using var wal = MetricWriteAheadLog.Open(Path.Combine(dir, "metrics.wal"), 64 * 1024);
            wal.Append(Batch("fill", 300));               // 14 400 B of 65 536: below the pre-grow mark

            using var parked  = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            int fired = 0;
            wal.OnGrowMappedForTest = () =>
            {
                if (Interlocked.Increment(ref fired) != 1) return;   // a later pre-grow runs free
                parked.Set();
                release.Wait();
            };

            var grower = Task.Factory.StartNew(() => wal.Append(Batch("big", 2_000)),
                                               TaskCreationOptions.LongRunning);
            try
            {
                Assert.True(parked.Wait(TimeSpan.FromSeconds(30)), "setup: the big batch never reached its growth");

                var small = Task.Factory.StartNew(() => wal.Append(Batch("small", 10)),
                                                  TaskCreationOptions.LongRunning);
                Assert.True(small.Wait(TimeSpan.FromSeconds(30)),
                    "an append that fits waited for another batch's growth");
            }
            finally { release.Set(); }

            grower.Wait();
            wal.OnGrowMappedForTest = null;

            var all = wal.ReadAll(out int unresolved);
            Assert.Equal(0, unresolved);
            Assert.Equal(2_310, all.Count);
            Assert.Equal("fill",  all[0].Name);
            Assert.Equal("fill",  all[299].Name);
            Assert.Equal("small", all[300].Name);
            Assert.Equal("small", all[309].Name);
            Assert.Equal("big",   all[310].Name);
            Assert.Equal("big",   all[^1].Name);
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    private static MetricIngestItem[] Batch(string name, int points)
    {
        var batch = new MetricIngestItem[points];
        for (int i = 0; i < points; i++)
            batch[i] = new MetricIngestItem
            {
                Name              = name,
                Kind              = MetricKind.Gauge,
                Labels            = new LabelSet([new("s", (i & 7).ToString())]),
                TimestampUnixNano = 1_700_000_000_000_000_000L + i,
                ScalarValue       = i,
            };
        return batch;
    }

    [Fact]
    public void Probe_append_latency_across_growths()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mwalgrow-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            using var wal = MetricWriteAheadLog.Open(Path.Combine(dir, "metrics.wal"), 8L * 1024 * 1024);

            var batches = new MetricIngestItem[Threads][];
            for (int t = 0; t < Threads; t++)
            {
                batches[t] = new MetricIngestItem[BatchPoints];
                for (int i = 0; i < BatchPoints; i++)
                    batches[t][i] = new MetricIngestItem
                    {
                        Name              = "grow.metric",
                        Kind              = MetricKind.Gauge,
                        Labels            = new LabelSet([new("t", t.ToString()), new("s", (i & 63).ToString())]),
                        TimestampUnixNano = 1_700_000_000_000_000_000L + i,
                        ScalarValue       = i,
                    };
                wal.Append(batches[t]);   // register every series before the clock starts
            }

            var maxTicks = new long[Threads];
            var slow     = new int[Threads];
            var start    = new Barrier(Threads + 1);
            var workers  = new Thread[Threads];
            for (int t = 0; t < Threads; t++)
            {
                int me = t;
                workers[t] = new Thread(() =>
                {
                    var batch = batches[me];
                    long max = 0; int over = 0;
                    long oneMs = Stopwatch.Frequency / 1000;
                    start.SignalAndWait();
                    for (int b = 0; b < BatchesPerThread; b++)
                    {
                        long t0 = Stopwatch.GetTimestamp();
                        wal.Append(batch);
                        long d = Stopwatch.GetTimestamp() - t0;
                        if (d > max) max = d;
                        if (d > oneMs) over++;
                    }
                    maxTicks[me] = max;
                    slow[me]     = over;
                });
                workers[t].Start();
            }

            var wall = Stopwatch.StartNew();
            start.SignalAndWait();
            foreach (var w in workers) w.Join();
            wall.Stop();

            long points = (long)Threads * BatchesPerThread * BatchPoints;
            _out.WriteLine($"WAL GROWTH  {Threads} threads x {BatchesPerThread} batches x {BatchPoints} points, 8 MiB start, "
                         + $"{wal.WrittenBytes / 1048576.0:F0} MiB written");
            _out.WriteLine($"  wall {wall.Elapsed.TotalMilliseconds:F0} ms, {points / wall.Elapsed.TotalSeconds / 1e6:F2} M points/s");
            for (int t = 0; t < Threads; t++)
                _out.WriteLine($"  thread {t}: slowest append {maxTicks[t] * 1000.0 / Stopwatch.Frequency,8:F2} ms, "
                             + $"{slow[t],4} appends over 1 ms");

            Assert.True(wal.WrittenBytes >= points * 48);
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }
}
