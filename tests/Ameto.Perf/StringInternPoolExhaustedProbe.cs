using System.Diagnostics;
using Ameto.Core;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// WHAT A MISS COSTS ONCE THE POOL IS FULL. Review L3 of issue #83, wave 2.
///
/// <para>The metric label pool (<c>MetricLabelInterner</c>, 16 384 strings) never evicts, and
/// resource labels that churn — <c>k8s.pod.name</c>, <c>container.id</c> — fill it, after
/// which every label it has not seen is a miss for the life of the process. Each miss ended in
/// <c>Interlocked.Exchange</c> on the pool's "exhaustion already reported" flag: a write,
/// even of the 1 already there, to the cache line every other ingest thread reads the pool's
/// fields from. The probe has several threads miss on a full pool, each on its own strings, and
/// reports ns per miss PER THREAD. Asserts pin only the answer: every miss is -1 with the
/// caller's own string, and the pool says it is full exactly once.</para>
/// </summary>
public sealed class StringInternPoolExhaustedProbe
{
    private readonly ITestOutputHelper _out;
    public StringInternPoolExhaustedProbe(ITestOutputHelper output) => _out = output;

    private const int MissesPerThread = 2_000_000;
    private const int KeysPerThread   = 1_024;

    [Fact]
    public void Probe_misses_on_a_full_pool()
    {
        foreach (int threads in (int[])[1, 4, 8])
        {
            var pool  = new StringInternPool(maxPoolSize: 16);
            int fired = 0;
            pool.PoolExhausted += _ => Interlocked.Increment(ref fired);
            for (int i = 0; i < 16; i++) pool.Intern("seed-" + i);

            // Built up front, so the loop times the miss and not the string.
            var keys = new string[threads][];
            for (int t = 0; t < threads; t++)
            {
                keys[t] = new string[KeysPerThread];
                for (int i = 0; i < KeysPerThread; i++) keys[t][i] = $"k8s.pod.name=checkout-7d9f{t:x2}-{i:x5}";
            }

            var nsPerMiss = new double[threads];
            var wrong     = new int[threads];
            var start     = new Barrier(threads + 1);
            var workers   = new Thread[threads];
            for (int t = 0; t < threads; t++)
            {
                int me = t;
                workers[t] = new Thread(() =>
                {
                    var mine = keys[me];
                    int bad  = 0;
                    start.SignalAndWait();
                    long t0 = Stopwatch.GetTimestamp();
                    for (int n = 0; n < MissesPerThread; n++)
                    {
                        string key = mine[n & (KeysPerThread - 1)];
                        if (pool.Intern(key, out string canonical) != -1 || !ReferenceEquals(canonical, key)) bad++;
                    }
                    long d = Stopwatch.GetTimestamp() - t0;
                    nsPerMiss[me] = d * 1e9 / Stopwatch.Frequency / MissesPerThread;
                    wrong[me]     = bad;
                });
                workers[t].Start();
            }
            start.SignalAndWait();
            foreach (var w in workers) w.Join();

            var line = new System.Text.StringBuilder($"FULL POOL  {threads} thread(s), ns per miss:");
            for (int t = 0; t < threads; t++) line.Append($" {nsPerMiss[t],6:F1}");
            _out.WriteLine(line.ToString());

            Assert.All(wrong, w => Assert.Equal(0, w));
            Assert.Equal(1, fired);
        }
    }
}
