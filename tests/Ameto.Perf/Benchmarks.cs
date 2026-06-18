using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Ameto.Core;
using Ameto.Storage;
using Xunit;

namespace Ameto.Perf;

// ── BenchmarkDotNet runner ────────────────────────────────────────────────────

/// <summary>
/// Entry point: run "dotnet run -c Release" from the Ameto.Perf project directory.
/// Or run individual benchmarks via xUnit [Fact] smoke tests for CI correctness checks.
/// </summary>
public static class Program
{
    public static void Main(string[] args) =>
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
}

// ── HotTierSegment write throughput ─────────────────────────────────────────

/// <summary>
/// Measures raw write throughput into an off-heap HotTierSegment.
/// Target: &gt;= 100,000 events/second per core.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 2, iterationCount: 5)]
public sealed class HotTierWriteBenchmark : IDisposable
{
    private HotTierSegment _segment = null!;
    private LogEventHeader _header;

    [Params(1_000, 10_000, 100_000)]
    public int EventCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _segment = new HotTierSegment(EventCount + 1, (long)EventCount * 256 + 64 * 1024);
        _header  = new LogEventHeader
        {
            TimestampUtcTicks        = DateTimeOffset.UtcNow.UtcTicks,
            Level                    = LogLevel.Information,
            MessageTemplatePoolIndex = -1,
            Flags                    = 0,
        };
    }

    [IterationSetup]
    public void IterSetup()
    {
        _segment.Dispose();
        _segment = new HotTierSegment(EventCount + 1, (long)EventCount * 256 + 64 * 1024);
    }

    [Benchmark]
    public void WriteEvents()
    {
        for (int i = 0; i < EventCount; i++)
        {
            _header.Id                = new EventId(0u, (uint)i).RawValue;
            _header.TimestampUtcTicks = DateTimeOffset.UtcNow.UtcTicks;
            _segment.TryWrite(_header, ReadOnlySpan<byte>.Empty);
        }
    }

    public void Dispose() => _segment?.Dispose();
}

// ── StringInternPool throughput ───────────────────────────────────────────────

[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 2, iterationCount: 5)]
public sealed class StringInternPoolBenchmark
{
    private StringInternPool _pool = null!;
    private string[] _templates = null!;

    [Params(10, 100, 1_000)]
    public int UniqueTemplates { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _pool      = new StringInternPool();
        _templates = Enumerable.Range(0, UniqueTemplates)
                               .Select(i => $"Template {{Arg{i}}} number {i}")
                               .ToArray();

        // Pre-warm
        foreach (var t in _templates) _pool.Intern(t);
    }

    [Benchmark]
    public void InternExistingTemplates()
    {
        // Hot path: all templates already interned → pure dict lookup
        foreach (var t in _templates)
            _pool.Intern(t);
    }
}

// ── Filter evaluation throughput ─────────────────────────────────────────────

[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 2, iterationCount: 5)]
public sealed class FilterEvalBenchmark
{
    private Ameto.Query.Filtering.CompiledFilter _filter = null!;
    private LogEvent[] _events = null!;

    [Params(1_000, 10_000)]
    public int EventCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _filter = Ameto.Query.Filtering.CompiledFilter.Compile(
            "@l = 'Error' and has(UserId)");

        var rng = new Random(42);
        _events = Enumerable.Range(0, EventCount).Select(i =>
        {
            var level = (LogLevel)(i % 6);
            return new LogEvent
            {
                Id              = new EventId(0u, (uint)i),
                Timestamp       = DateTimeOffset.UtcNow,
                Level           = level,
                MessageTemplate = "Request {UserId} completed",
                Properties      = level == LogLevel.Error
                                  ? new Dictionary<string, object?> { ["UserId"] = "user42" }
                                  : null,
            };
        }).ToArray();
    }

    [Benchmark]
    public int EvaluateAllEvents()
    {
        int matches = 0;
        foreach (var ev in _events)
            if (_filter.Matches(ev)) matches++;
        return matches;
    }
}

// ── Smoke tests (xUnit) — run in CI without BenchmarkDotNet overhead ─────────

public sealed class PerfSmokeTests : IDisposable
{
    private readonly HotTierSegment _segment = new(100_001, 100_001L * 256 + 64 * 1024);

    public void Dispose() => _segment.Dispose();

    [Fact]
    public void HotTier_WriteOneHundredThousandEvents_InUnderFiveSeconds()
    {
        var header = new LogEventHeader
        {
            TimestampUtcTicks        = DateTimeOffset.UtcNow.UtcTicks,
            Level                    = LogLevel.Information,
            MessageTemplatePoolIndex = -1,
        };

        var sw = System.Diagnostics.Stopwatch.StartNew();
        for (int i = 0; i < 100_000; i++)
        {
            header.Id = new EventId(0u, (uint)i).RawValue;
            _segment.TryWrite(header, ReadOnlySpan<byte>.Empty);
        }
        sw.Stop();

        Assert.Equal(100_000, _segment.Count);
        // Allow generous 5 s for debug builds / slow CI agents
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5),
            $"100k writes took {sw.Elapsed.TotalMilliseconds:F0} ms — too slow");
    }

    [Fact]
    public void StringInternPool_InternSameTemplate_IsIdempotent()
    {
        var pool = new StringInternPool();
        const string tmpl = "Request {Path} took {Elapsed} ms";
        int idx1 = pool.Intern(tmpl);
        int idx2 = pool.Intern(tmpl);
        Assert.Equal(idx1, idx2);
    }

    [Fact]
    public void FilterEval_MatchesCorrectFraction()
    {
        var filter = Ameto.Query.Filtering.CompiledFilter.Compile("@l = 'Error'");
        int errors = 0;
        for (int i = 0; i < 1_000; i++)
        {
            var ev = new LogEvent
            {
                Id              = new EventId(0u, (uint)i),
                Timestamp       = DateTimeOffset.UtcNow,
                Level           = i % 6 == (int)LogLevel.Error ? LogLevel.Error : LogLevel.Information,
                MessageTemplate = "msg",
            };
            if (filter.Matches(ev)) errors++;
        }
        // 1/6 of events should match
        Assert.InRange(errors, 160, 170);
    }
}
