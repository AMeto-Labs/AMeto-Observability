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
    public void Two_busy_threads_read_about_twice_one()
    {
        int cores = Environment.ProcessorCount;
        if (cores < 4) return;

        var sampler = new ProcessCpuSampler();

        sampler.Sample();
        Burn(threads: 1, ms: 800);
        double one = sampler.Sample();

        sampler.Sample();
        Burn(threads: 2, ms: 800);
        double two = sampler.Sample();

        _out.WriteLine($"cores={cores}  1 thread={one:F1} %  2 threads={two:F1} %  ratio={two / one:F2}");
        Assert.True(two > one * 1.4, $"two threads read {two:F1} % against {one:F1} % for one");
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
