using System.Diagnostics;
using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE EXEMPLAR RING: NO MONITOR PER EXEMPLAR, NO WHOLE-RING COPY PER READ. M#13 of issue #83.
///
/// <para><c>ExemplarRing.Add</c> took a <c>lock</c> per exemplar — every Kestrel thread carrying
/// exemplars for one instrument queued on it — and <c>Snapshot()</c> copied the entire ring
/// (4 000 slots, 32 KB) on every <c>GET /exemplars</c> before the filter threw most of it away,
/// and the answer was then copied again by <c>GetRange</c>.</para>
/// </summary>
public sealed class MetricExemplarRingTests
{
    private readonly ITestOutputHelper _out;
    public MetricExemplarRingTests(ITestOutputHelper output) => _out = output;

    private const int RingSize = 4_000;

    private static MetricStorageEngine Open(string dir) =>
        new(dir, NullLogger<MetricStorageEngine>.Instance, new MetricsOptions
        {
            HotTierBytes       = 256L * 1024 * 1024,
            ExemplarsPerMetric = RingSize,
        });

    /// <summary>
    /// Past a wrap the ring answers with exactly its newest entries, newest first, and the limit
    /// and the time range cut the answer as before.
    /// </summary>
    [Fact]
    public async Task A_wrapped_ring_answers_its_newest_entries_newest_first()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mexring-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var engine = Open(dir);
            long t0 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // 2.5 rings' worth, one exemplar a millisecond, in batches of 1 000 items.
            const int Total = RingSize * 5 / 2;
            for (int done = 0; done < Total; done += 1_000)
                engine.Ingest(Items(t0, done, 1_000));

            var newest = engine.GetExemplars("ex.metric", null, null, null, limit: 50);
            Assert.Equal(50, newest.Count);
            for (int i = 0; i < newest.Count; i++)
                Assert.Equal((t0 + Total - 1 - i) * 1_000_000L, newest[i].TimestampUnixNano);

            // Everything the ring still holds: exactly the last RingSize, none older.
            var all = engine.GetExemplars("ex.metric", null, null, null, limit: int.MaxValue);
            Assert.Equal(RingSize, all.Count);
            Assert.Equal((t0 + Total - RingSize) * 1_000_000L, all[^1].TimestampUnixNano);

            // A range inside what is held.
            var ranged = engine.GetExemplars("ex.metric",
                DateTimeOffset.FromUnixTimeMilliseconds(t0 + Total - 20),
                DateTimeOffset.FromUnixTimeMilliseconds(t0 + Total - 11), null, limit: 200);
            Assert.Equal(10, ranged.Count);
            Assert.Equal((t0 + Total - 11) * 1_000_000L, ranged[0].TimestampUnixNano);
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>
    /// A read that matches nothing costs nothing in proportion to the ring. Revert
    /// <c>GetExemplars</c> to walking <c>ring.Snapshot()</c> and this fails at ~32 KB a call:
    /// the 4 000-slot copy of the ring.
    /// </summary>
    [Fact]
    public async Task A_read_does_not_copy_the_ring()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mexcopy-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var engine = Open(dir);
            long t0 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            for (int done = 0; done < RingSize; done += 1_000) engine.Ingest(Items(t0, done, 1_000));

            var from = DateTimeOffset.FromUnixTimeMilliseconds(t0 - 10_000);
            var to   = DateTimeOffset.FromUnixTimeMilliseconds(t0 - 5_000);   // before every entry
            _ = engine.GetExemplars("ex.metric", from, to, null);

            long b0 = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 100; i++) _ = engine.GetExemplars("ex.metric", from, to, null);
            double perCall = (GC.GetAllocatedBytesForCurrentThread() - b0) / 100.0;

            _out.WriteLine($"GetExemplars over a full {RingSize}-slot ring, nothing in range: {perCall:N0} B/call");
            Assert.True(perCall < 1_024, $"a read that matched nothing allocated {perCall:N0} B — the ring was copied");
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>
    /// Four threads add to one ring at once; afterwards it holds exactly its capacity, every slot
    /// an entry somebody added, none twice.
    /// </summary>
    [Fact]
    public void Concurrent_adders_leave_a_full_ring_of_distinct_entries()
    {
        const int Threads = 4, PerThread = 50_000, Capacity = 1_024;
        var ring  = new ExemplarRing(Capacity);
        var start = new Barrier(Threads);
        var ts    = new Thread[Threads];
        for (int t = 0; t < Threads; t++)
        {
            int me = t;
            ts[t] = new Thread(() =>
            {
                start.SignalAndWait();
                for (int i = 0; i < PerThread; i++)
                    ring.Add(new ExemplarSample { TimestampUnixNano = (long)me * PerThread + i });
            });
            ts[t].Start();
        }
        foreach (var t in ts) t.Join();

        var seen = new HashSet<long>();
        ring.ForEach(seen, static (set, s) => { Assert.True(set.Add(s.TimestampUnixNano)); });
        Assert.Equal(Capacity, seen.Count);
        foreach (long v in seen) Assert.InRange(v, 0, (long)Threads * PerThread - 1);
    }

    /// <summary>
    /// Probe: per-thread ns per <c>ExemplarRing.Add</c> on ONE ring at 1/2/4/8 threads, and the
    /// bytes and ns of one <c>GetExemplars</c> over a full ring.
    /// </summary>
    [Fact]
    public async Task Probe_ring_add_and_read()
    {
        const int PerThread = 400_000;
        _out.WriteLine($"ExemplarRing.Add, one {RingSize}-slot ring, {PerThread:N0} adds per thread, best of 3");
        foreach (int threads in (int[])[1, 2, 4, 8])
        {
            double best = double.MaxValue;
            for (int r = 0; r < 3; r++)
            {
                var ring    = new ExemplarRing(RingSize);
                var sample  = new ExemplarSample { TimestampUnixNano = 1 };
                var start   = new Barrier(threads);
                var ns      = new double[threads];
                var workers = new Thread[threads];
                for (int t = 0; t < threads; t++)
                {
                    int me = t;
                    workers[t] = new Thread(() =>
                    {
                        start.SignalAndWait();
                        long t0 = Stopwatch.GetTimestamp();
                        for (int i = 0; i < PerThread; i++) ring.Add(sample);
                        ns[me] = Stopwatch.GetElapsedTime(t0).TotalNanoseconds / PerThread;
                    });
                    workers[t].Start();
                }
                foreach (var w in workers) w.Join();
                double avg = 0;
                foreach (double v in ns) avg += v;
                avg /= threads;
                if (avg < best) best = avg;
            }
            _out.WriteLine($"  {threads} thread(s): {best,7:F1} ns/add per thread");
        }

        string dir = Path.Combine(Path.GetTempPath(), "ameto-mexprobe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var engine = Open(dir);
            long t0 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            for (int done = 0; done < RingSize; done += 1_000) engine.Ingest(Items(t0, done, 1_000));

            var recent = DateTimeOffset.FromUnixTimeMilliseconds(t0 + RingSize - 100);
            _ = engine.GetExemplars("ex.metric", recent, null, null);
            _ = engine.GetExemplars("ex.metric", null, null, null);

            const int Calls = 500;
            long b0 = GC.GetAllocatedBytesForCurrentThread();
            long c0 = Stopwatch.GetTimestamp();
            for (int i = 0; i < Calls; i++) _ = engine.GetExemplars("ex.metric", recent, null, null);
            double nsNarrow = Stopwatch.GetElapsedTime(c0).TotalNanoseconds / Calls;
            double bNarrow  = (GC.GetAllocatedBytesForCurrentThread() - b0) / (double)Calls;

            b0 = GC.GetAllocatedBytesForCurrentThread();
            c0 = Stopwatch.GetTimestamp();
            for (int i = 0; i < Calls; i++) _ = engine.GetExemplars("ex.metric", null, null, null);
            double nsAll = Stopwatch.GetElapsedTime(c0).TotalNanoseconds / Calls;
            double bAll  = (GC.GetAllocatedBytesForCurrentThread() - b0) / (double)Calls;

            _out.WriteLine($"GetExemplars, full {RingSize}-slot ring:");
            _out.WriteLine($"  last 100 in range, limit 200 : {nsNarrow,9:N0} ns {bNarrow,9:N0} B per call");
            _out.WriteLine($"  all in range,      limit 200 : {nsAll,9:N0} ns {bAll,9:N0} B per call");
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary><paramref name="count"/> items from ordinal <paramref name="first"/>, one exemplar each, 1 ms apart.</summary>
    private static MetricIngestItem[] Items(long t0Ms, int first, int count)
    {
        var items = new MetricIngestItem[count];
        for (int i = 0; i < count; i++)
        {
            long nano = (t0Ms + first + i) * 1_000_000L;
            items[i] = new MetricIngestItem
            {
                Name              = "ex.metric",
                Kind              = MetricKind.Gauge,
                Labels            = new LabelSet([new("s", ((first + i) & 7).ToString())]),
                TimestampUnixNano = nano,
                ScalarValue       = i,
                Exemplars         = [new MetricExemplar { TimestampUnixNano = nano, Value = i, TraceId = "t", SpanId = "s" }],
            };
        }
        return items;
    }
}
