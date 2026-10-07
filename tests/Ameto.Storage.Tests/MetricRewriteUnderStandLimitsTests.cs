using System.Diagnostics;
using System.Globalization;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE STAND'S COMPACTION, UNDER THE STAND'S LIMITS (#125).
///
/// <para>The 512 MB console stand (a 384 MiB GC heap hard limit, Workstation GC,
/// <c>GCConserveMemory=5</c>, a resting managed heap of 60–160 MB) failed its metric compaction with an
/// OutOfMemoryException on every pass. Reproduced before the fix, in a process started exactly so:
/// a FiveMin merge of 512 busy histogram series — one file carrying 1 064 five-minute points of each
/// (3.7 days, a 58 MiB block) beside three ordinary one-hour rollup outputs — ran out of heap with a
/// 160 MiB resting load (in <c>MetricReader.ReadBuckets</c>, the stand's own stack) and with none at
/// all. Its chunk was the whole 512 series, ~340 MiB at peak.</para>
///
/// <para>The rewrite now cuts that merge into chunks bounded by <c>RewriteBudgetBytes</c> (the trace
/// merge share, 24.2 MB at this limit), reads every source through one pair of block buffers held for
/// the rewrite (<c>MetricReader.ReadScratch</c>), and must finish, every output readable, every point
/// of every series exactly as it went in. Measured on this shape when it was written: it finishes with
/// up to 230 MiB resting and fails at 260 — ~125–150 MiB needed, ~101 MiB of it the 58 MiB block's
/// compressed and inflated buffers — where it used to fail with none. A child process because the GC
/// reads its limits once, at startup; the test host keeps its own and only waits.</para>
/// </summary>
public sealed class MetricRewriteUnderStandLimitsTests : IDisposable
{
    internal const string Command = "metric-rewrite-under-stand-limits";

    private readonly ITestOutputHelper _out;
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mrwstand-" + Guid.NewGuid().ToString("N"));

    public MetricRewriteUnderStandLimitsTests(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_dir);
    }

    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private const long T0   = 1_784_800_020_000_000_000L;
    private const long Min5 = 300_000_000_000L;
    private const long MiB  = 1024 * 1024;

    [Theory]
    [InlineData(160)]   // the stand's resting heap at its busiest
    [InlineData(0)]     // the rewrite's own peak, measured
    public void A_FiveMin_merge_of_512_busy_histogram_series_finishes_inside_the_stands_heap(int restingMiB)
    {
        string src = Path.Combine(_dir, "src");
        var sources = MetricHistogramShapes.Write(src, MetricGranularity.FiveMin, series: 512, T0, Min5,
                                                  [1064, 12, 12, 12], activeFraction: 1.0, seed: 6);
        long block = sources.Max(s => (long)MetricHistogramShapes.BlockSizes(s.FilePath).Raw);
        Assert.True(block > 48 * MiB, $"setup: the carried-history file's block is only {block:N0} B");

        var child = Run(src, Path.Combine(_dir, "eng"), restingMiB);
        string report = $"FiveMin 512 x 1 100 histogram merge at a 384 MiB limit, {restingMiB} MiB resting: "
                      + string.Join(", ", child.Values.Where(kv => kv.Key != "stack").Select(kv => kv.Key + "=" + kv.Value));
        _out.WriteLine(report);
        foreach (string line in child.Stack) _out.WriteLine("  " + line);
        if (Environment.GetEnvironmentVariable("AMETO_PROBE_OUT") is { Length: > 0 } probe)
            File.AppendAllText(probe, report + Environment.NewLine + string.Join(Environment.NewLine, child.Stack) + Environment.NewLine);

        Assert.Equal(384 * MiB, long.Parse(child["heapLimit"], CultureInfo.InvariantCulture));
        Assert.True(child["result"] == "ok", $"the rewrite failed: {child["result"]}{Environment.NewLine}{string.Join(Environment.NewLine, child.Stack)}");
        Assert.Equal("512", child["series"]);
        Assert.Equal("True", child["same"]);
        Assert.True(int.Parse(child["outputs"], CultureInfo.InvariantCulture) >= 2, "one output for a merge over the budget");
        Assert.True(long.Parse(child["maxOutputBlock"], CultureInfo.InvariantCulture) <= MetricReader.MaxBlockBytes);
    }

    private sealed class ChildResult
    {
        public Dictionary<string, string> Values { get; } = new(StringComparer.Ordinal);
        public List<string> Stack { get; } = [];
        public string this[string key] => Values.TryGetValue(key, out var v) ? v : $"<{key} not printed>";
    }

    /// <summary>
    /// This assembly again, as a child started under the stand's GC settings: a 512 MB physical limit
    /// (the GC takes 75 % of it as its heap hard limit, exactly as under a 512 MB cgroup), Workstation,
    /// <c>GCConserveMemory=5</c> as the stand's Dockerfile sets.
    /// </summary>
    private static ChildResult Run(string sources, string engineDir, int restingMiB)
    {
        string? hostPath = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH");
        var psi = new ProcessStartInfo(hostPath is { Length: > 0 } && File.Exists(hostPath) ? hostPath : "dotnet")
        {
            UseShellExecute        = false,
            RedirectStandardOutput = true,
            RedirectStandardError  = true,
        };
        psi.ArgumentList.Add("exec");
        psi.ArgumentList.Add(typeof(MetricRewriteUnderStandLimitsTests).Assembly.Location);
        psi.ArgumentList.Add(Command);
        psi.ArgumentList.Add(sources);
        psi.ArgumentList.Add(engineDir);
        psi.ArgumentList.Add(restingMiB.ToString(CultureInfo.InvariantCulture));

        // Nothing inherited may pre-empt the settings under test.
        foreach (string prefix in (string[])["DOTNET_", "COMPlus_"])
            foreach (string name in (string[])["GCHeapHardLimit", "GCHeapHardLimitPercent", "GCTotalPhysicalMemory",
                                               "GCHeapHardLimitSOH", "GCHeapHardLimitLOH", "GCHeapHardLimitPOH",
                                               "GCHighMemPercent", "gcServer", "GCgen0size", "GCConserveMemory"])
                psi.Environment.Remove(prefix + name);
        psi.Environment["DOTNET_GCTotalPhysicalMemory"] = "0x20000000";   // 512 MiB
        psi.Environment["DOTNET_gcServer"]              = "0";
        psi.Environment["DOTNET_GCConserveMemory"]      = "5";

        using var proc = Process.Start(psi)!;
        var stderr = proc.StandardError.ReadToEndAsync();
        string stdout = proc.StandardOutput.ReadToEnd();
        if (!proc.WaitForExit(300_000))
        {
            try { proc.Kill(entireProcessTree: true); } catch { /* already gone */ }
            Assert.Fail("the child did not finish the rewrite in 5 minutes");
        }

        var result = new ChildResult();
        foreach (string line in stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            int eq = line.IndexOf('=');
            if (eq <= 0) continue;
            if (line.StartsWith("stack=", StringComparison.Ordinal)) result.Stack.Add(line[6..]);
            else result.Values[line[..eq]] = line[(eq + 1)..];
        }
        if (proc.ExitCode != 0)
            Assert.Fail($"child exited {proc.ExitCode}: {stderr.GetAwaiter().GetResult()}");
        return result;
    }

    /// <summary>
    /// The child's side (<see cref="ChildProcessEntry"/>): digest the sources, take on the resting heap,
    /// run the rewrite the FiveMin merge runs, then release the resting heap and digest what it wrote.
    /// </summary>
    internal static int RunChild(string sources, string engineDir, int restingMiB)
    {
        Console.WriteLine($"heapLimit={GC.GetGCMemoryInfo().TotalAvailableMemoryBytes}");

        var files = Directory.EnumerateFiles(sources, "*.mts").OrderBy(f => f, StringComparer.Ordinal).ToList();
        var input = MetricHistogramShapes.Digests(files);   // a series at a time, before the resting heap
        var infos = files.Select(MetricReader.ReadSegmentInfo).ToList();

        // The stand's resting managed heap, as live 1 MiB arrays.
        var resting = new byte[restingMiB][];
        for (int i = 0; i < resting.Length; i++) { resting[i] = new byte[1 << 20]; resting[i].AsSpan().Fill(1); }
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        Directory.CreateDirectory(engineDir);
        var engine = new MetricStorageEngine(engineDir, NullLogger<MetricStorageEngine>.Instance);
        engine.ColdLoadCompleted.GetAwaiter().GetResult();
        Console.WriteLine($"budget={engine.RewriteBudgetBytes}");

        long baseline = GC.GetTotalMemory(forceFullCollection: true);
        long peak     = baseline;
        int  stop     = 0;
        var sampler = new Thread(() =>
        {
            while (Volatile.Read(ref stop) == 0)
            {
                long now = GC.GetTotalMemory(forceFullCollection: false);
                if (now > Volatile.Read(ref peak)) Volatile.Write(ref peak, now);
                Thread.Sleep(1);
            }
        }) { IsBackground = true };
        sampler.Start();

        string result;
        string? stack = null;
        List<MetricSegmentInfo>? outputs = null;
        var sw = Stopwatch.StartNew();
        try
        {
            outputs = engine.RewriteMetricInChunks(infos, MetricGranularity.FiveMin,
                                                   static (pts, _) => MetricStorageEngine.DedupeByTimestamp(pts));
            result = "ok";
        }
        catch (OutOfMemoryException ex) { result = "OOM"; stack = ex.StackTrace; }
        catch (Exception ex) { result = ex.GetType().Name + ": " + ex.Message.ReplaceLineEndings(" "); stack = ex.StackTrace; }
        sw.Stop();
        Volatile.Write(ref stop, 1);
        sampler.Join();

        Console.WriteLine($"result={result}");
        Console.WriteLine($"restingMiB={restingMiB}");
        Console.WriteLine($"peakAboveRestingMiB={(Volatile.Read(ref peak) - baseline) / (double)(1 << 20):F1}");
        Console.WriteLine($"peakMiB={Volatile.Read(ref peak) / (double)(1 << 20):F1}");
        Console.WriteLine($"ms={sw.ElapsedMilliseconds}");
        if (stack is not null)
            foreach (string line in stack.Split('\n').Where(l => !string.IsNullOrWhiteSpace(l)).Take(8))
                Console.WriteLine("stack=" + line.Trim());

        GC.KeepAlive(resting);
        resting = [];                       // released before the outputs are read back
        GC.Collect();

        if (outputs is not null)
        {
            Console.WriteLine($"outputs={outputs.Count}");
            Console.WriteLine($"maxOutputBlock={outputs.Max(o => (long)MetricHistogramShapes.BlockSizes(o.FilePath).Raw)}");
            var output = MetricHistogramShapes.Digests(outputs.Select(o => o.FilePath));
            bool same  = input.Count == output.Count
                      && input.All(kv => output.TryGetValue(kv.Key, out var d) && d == kv.Value);
            Console.WriteLine($"series={output.Count}");
            Console.WriteLine($"same={same}");
        }

        try { engine.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { /* the verdict is printed */ }
        return 0;
    }
}
