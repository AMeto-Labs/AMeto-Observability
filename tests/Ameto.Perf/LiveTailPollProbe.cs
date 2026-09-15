using System.Diagnostics;
using Ameto.Core;
using Ameto.Query;
using Ameto.Query.Filtering;
using Ameto.Query.Tests;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// The FIXED cost of one live-tail poll that finds nothing: the filter compile, the
/// catalog walk/sort, the tier snapshot and the merge set-up — paid up to ten times a
/// second per open tab regardless of whether anything was written. Measured over the
/// executor exactly as the tail drives it: forward, cursor at the newest event, one
/// page, the same filter every time, a compiled copy carried on the request.
/// </summary>
public sealed class LiveTailPollProbe
{
    private readonly ITestOutputHelper _out;
    public LiveTailPollProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public async Task IdlePoll_FixedCost()
    {
        string dir = Path.Combine(Path.GetTempPath(), "Ameto-tailpoll-" + Guid.NewGuid().ToString("N"));
        var (engine, query) = await QuerySegmentFixtures.ManySegmentsAsync(dir);
        try
        {
            // Cursor past everything: every poll walks the catalog and the hot tier and
            // finds nothing — the idle tail.
            long cursorTs = new DateTimeOffset(2026, 7, 30, 1, 0, 0, TimeSpan.Zero).UtcTicks;
            const string filterText = "@l = 'Information' and n > 0";
            var prepared = CompiledFilter.Compile(filterText);

            async Task<int> Poll(bool carryPrepared)
            {
                int n = 0;
                await foreach (var ev in query.ExecuteAsync(new QueryRequest
                {
                    Filter              = filterText,
                    FromUtc             = new DateTimeOffset(cursorTs, TimeSpan.Zero),
                    Count               = 50,
                    Direction           = QueryDirection.Forward,
                    AfterTimestampTicks = cursorTs,
                    AfterEventId        = new EventId(0u, uint.MaxValue),
                    Prepared            = carryPrepared ? prepared : null,
                })) n++;
                return n;
            }

            async Task<(long bytes, double us)> Measure(bool carryPrepared, int rounds)
            {
                for (int i = 0; i < 20; i++) await Poll(carryPrepared);   // warm-up
                long before = GC.GetAllocatedBytesForCurrentThread();
                var  sw     = Stopwatch.StartNew();
                for (int i = 0; i < rounds; i++) Assert.Equal(0, await Poll(carryPrepared));
                sw.Stop();
                return ((GC.GetAllocatedBytesForCurrentThread() - before) / rounds, sw.Elapsed.TotalMilliseconds * 1000 / rounds);
            }

            var recompiled = await Measure(carryPrepared: false, 500);
            var carried    = await Measure(carryPrepared: true,  500);
            _out.WriteLine($"idle poll, {QuerySegmentFixtures.ManySegments} cold segments, filter \"{filterText}\":");
            _out.WriteLine($"  filter recompiled per poll : {recompiled.bytes / 1024.0:F1} KB, {recompiled.us:F0} us");
            _out.WriteLine($"  compiled filter carried    : {carried.bytes / 1024.0:F1} KB, {carried.us:F0} us");
        }
        finally
        {
            await engine.DisposeAsync();
            try { Directory.Delete(dir, true); } catch { }
        }
    }
}
