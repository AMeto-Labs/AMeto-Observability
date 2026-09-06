using System.Diagnostics;
using Ameto.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// The sampler reports CPU as a share of ALL logical processors, so one fully busy thread on
/// an N-core box must read ~100/N %, not 100 %. That normalisation is the easy thing to get
/// backwards, and a wrong CPU figure is worse than no CPU figure — it sends the next
/// investigation to the wrong place.
/// </summary>
public sealed class ProcessCpuSamplerTests
{
    private readonly ITestOutputHelper _out;
    public ProcessCpuSamplerTests(ITestOutputHelper o) => _out = o;

    /// <summary>Burns CPU on <paramref name="threads"/> threads for <paramref name="ms"/>.</summary>
    private static void Burn(int threads, int ms)
    {
        var tasks = new Task[threads];
        for (int t = 0; t < threads; t++)
            tasks[t] = Task.Factory.StartNew(() =>
            {
                var sw = Stopwatch.StartNew();
                double sink = 0;
                while (sw.ElapsedMilliseconds < ms) sink += Math.Sqrt(sw.ElapsedTicks | 1);
                GC.KeepAlive(sink);
            }, TaskCreationOptions.LongRunning);
        Task.WaitAll(tasks);
    }

    [Fact]
    public void One_busy_thread_reads_about_one_core_worth()
    {
        int cores = Environment.ProcessorCount;
        if (cores < 2) return;                       // the assertion has no headroom on 1 core

        var sampler = new ProcessCpuSampler();
        Assert.Equal(cores, sampler.Cores);

        sampler.Sample();                            // open the interval
        Burn(threads: 1, ms: 1_000);
        double pct = sampler.Sample();

        double expected = 100.0 / cores;
        _out.WriteLine($"cores={cores}  measured={pct:F1} %  expected≈{expected:F1} %");

        // Generous band: the harness itself runs on this process, and a busy CI box adds
        // noise. What must hold is the ORDER OF MAGNITUDE — a missing /cores would land
        // near 100, and a doubled one near expected/2.
        Assert.InRange(pct, expected * 0.5, expected * 2.0);
    }

    [Fact]
    public void A_second_busy_thread_adds_about_a_core_to_the_reading()
    {
        // WHAT THIS GUARDS: that the sampler tracks thread count at all. A sampler that divided by
        // the wrong thing, or reported a constant, would read the same for one thread as for two.
        //
        // MEASURED AS A DIFFERENCE, NOT A RATIO, and that is a fix rather than a loosening. The old
        // form asserted two > one × 1.4, which assumes `one` contains nothing but the burn — and it
        // never does: the runner, the JIT and the GC are burning in this same process, so ambient
        // load lands in BOTH readings and compresses the ratio without touching the signal. Seen on
        // a 4-core CI box: one thread read 32.7 % where a lone core is 25 %, two read 45.4 %, ratio
        // 1.39 — a failure by one hundredth, from noise the ratio had no way to cancel.
        //
        // The increment does cancel it: a constant offset present in both subtracts out, and what
        // is left is what the extra thread actually bought. A sampler that ignores thread count
        // gives an increment near zero and still fails, which is the property worth keeping.
        int cores = Environment.ProcessorCount;
        if (cores < 4) return;

        var sampler = new ProcessCpuSampler();

        sampler.Sample();
        Burn(threads: 1, ms: 800);
        double one = sampler.Sample();

        sampler.Sample();
        Burn(threads: 2, ms: 800);
        double two = sampler.Sample();

        double core  = 100.0 / cores;      // what one fully busy thread is worth
        double gain  = two - one;
        double floor = core * 0.4;         // a contended box will not hand over a whole second core

        _out.WriteLine($"cores={cores}  1 thread={one:F1} %  2 threads={two:F1} %  "
                     + $"gain={gain:F1} pp  (a core is {core:F1} pp, floor {floor:F1})");
        Assert.True(gain > floor,
            $"a second busy thread added {gain:F1} points ({one:F1} % → {two:F1} %), which is under "
          + $"the {floor:F1} expected of a sampler that counts threads at all");
    }

    [Fact]
    public void Percentage_is_unavailable_until_an_interval_closes_and_never_exceeds_100()
    {
        var sampler = new ProcessCpuSampler();
        Assert.Equal(-1, sampler.LastPercent);       // nothing measured yet

        sampler.Sample();
        Burn(threads: Environment.ProcessorCount * 2, ms: 300);
        double pct = sampler.Sample();

        _out.WriteLine($"oversubscribed: {pct:F1} %");
        Assert.InRange(pct, 0, 100);                 // clamped — never a nonsense 800 %
        Assert.Equal(pct, sampler.LastPercent);      // readers see what the sampler computed
    }

    /// <summary>Total CPU time is monotonic — it is what a client differences across polls.</summary>
    [Fact]
    public void Total_processor_time_only_moves_forward()
    {
        var before = ProcessCpuSampler.TotalProcessorTime;
        Burn(threads: 1, ms: 200);
        var after = ProcessCpuSampler.TotalProcessorTime;

        _out.WriteLine($"total before={before.TotalSeconds:F2}s after={after.TotalSeconds:F2}s");
        Assert.True(after > before);
    }
}
