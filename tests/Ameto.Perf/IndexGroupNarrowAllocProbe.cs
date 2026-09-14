using Ameto.Indexing;
using Ameto.Query;
using Ameto.Query.Filtering;
using Ameto.Query.Tests;   // QuerySegmentFixtures, compiled in from the query suite (see .csproj)
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// WHAT NARROWING ONE GROUP COSTS. <c>QueryExecutor.TryNarrowWithIndex</c> runs once per index
/// group of every segment a query touches, eight at a time, and it used to answer in hash sets:
/// one seeded from the first trigram's posting list, one around the inverted result, one around
/// the level union — which for a level-split Error segment is the WHOLE group. All of it built
/// to hash data the codec had already handed over sorted.
///
/// <para>Measured directly rather than through a query, because end to end the block decode
/// swamps it. A ratio between two readings of an allocation counter is not a correctness claim,
/// which is why this is a probe and <c>Ameto.Query.Tests.IndexGroupPrefilterTests</c> is the
/// gate on the answers.</para>
/// </summary>
public sealed class IndexGroupNarrowAllocProbe : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-narrowalloc-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private StorageEngine _engine  = null!;
    private string        _segPath = null!;

    public IndexGroupNarrowAllocProbe(ITestOutputHelper o) => _out = o;

    public async Task InitializeAsync() =>
        (_engine, _, _segPath, _) = await QuerySegmentFixtures.GroupedSegmentAsync(_dir);

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    [Fact]
    public void NarrowingOneGroupIsProportionalToWhatSurvives()
    {
        using var reader = SegmentReader.Open(_segPath);
        int groups = reader.Groups.Length;
        Assert.True(groups >= 2, $"fixture produced {groups} groups");

        // Group 0, with its trigram section — a substring predicate is the shape that makes
        // the trigram intersection run at all.
        uint groupEvents = reader.Groups[0].EventCount;
        using var invSec = reader.RentInvertedIndexBytes(0);
        using var triSec = reader.RentTrigramIndexBytes(0);
        using var bloSec = reader.RentBloomFilterBytes(0);
        using var idx    = SegmentIndexReader.Load(invSec.Span, triSec.Span, bloSec.Span);

        // Every level the fixture wrote, so the union covers the group exactly: the level-pure
        // shape a level-split Error segment has, where the posting list IS the group.
        (string, object?)[][] levelHints = [[(Ameto.Core.ClefFields.Level, "Information")]];

        var like = CompiledFilter.Compile("@mt like '%processed%'");
        var pure = CompiledFilter.Compile(null);

        // Every row of the fixture carries the one template, so '%processed%' is the
        // whole-group shape; an OrderId substring is the selective one.
        var narrow = CompiledFilter.Compile("@mt like '%order-1234%'");

        _out.WriteLine($"{groups} groups, group 0 holds {groupEvents} events");
        long wholeGroup = Report("like '%processed%'             (all rows)", like,   null);
        long withLevels = Report("like '%processed%' + levels    (all rows)", like,   levelHints);
        long selective  = Report("like '%order-1234%'            (a handful)", narrow, null);
        Report("levels only (level-pure group)", pure, levelHints);

        // THE CLAIM: what narrowing costs tracks what SURVIVES, not the size of the group.
        // A hash set seeded from the posting list could not do this — it is sized by the
        // input either way, and at ~26 B an entry a whole-group list alone was several times
        // the sorted array that replaced it.
        Assert.True(selective * 8 < wholeGroup,
            $"narrowing is not proportional to the result: {selective} B for a handful vs {wholeGroup} B for the group");

        // And the whole-group cases stay within a couple of sorted uint[] over the group —
        // 4 B an event each — rather than a hash set's ~26.
        Assert.True(wholeGroup < 12L * groupEvents, $"{wholeGroup} B to narrow a {groupEvents}-event group");
        Assert.True(withLevels < 16L * groupEvents, $"{withLevels} B to narrow a {groupEvents}-event group with levels");

        long Report(string what, CompiledFilter filter, (string, object?)[][]? levels)
        {
            for (int i = 0; i < 20; i++)                      // warm
                QueryExecutor.TryNarrowWithIndex(filter, idx, levels, groupEvents, out _);

            const int Iterations = 200;
            long b0 = GC.GetAllocatedBytesForCurrentThread();
            uint[]? last = null;
            for (int i = 0; i < Iterations; i++)
            {
                Assert.True(QueryExecutor.TryNarrowWithIndex(filter, idx, levels, groupEvents, out var c));
                last = c;
            }
            long perCall = (GC.GetAllocatedBytesForCurrentThread() - b0) / Iterations;

            _out.WriteLine($"  {what,-44} {perCall,7} B/call, {(last is null ? "scan-all" : last.Length + " candidates")}");
            return perCall;
        }
    }

    /// <summary>
    /// The trigram lookup on its own — the same public API before and after, so this is the one
    /// number here that can be read against the parent commit. It used to spend
    /// <c>ToString().ToLowerInvariant()</c> (two strings per call, over a filter literal that
    /// never changes), a <c>HashSet&lt;int&gt;</c> seeded from the first trigram's posting list,
    /// an <c>IntersectWith</c> per further trigram, then <c>ToArray</c> + <c>Array.Sort</c> +
    /// <c>Array.ConvertAll</c>.
    /// </summary>
    [Fact]
    public void TrigramLookupCostsOneArray()
    {
        using var reader = SegmentReader.Open(_segPath);
        uint groupEvents = reader.Groups[0].EventCount;

        using var invSec = reader.RentInvertedIndexBytes(0);
        using var triSec = reader.RentTrigramIndexBytes(0);
        using var bloSec = reader.RentBloomFilterBytes(0);
        using var idx    = SegmentIndexReader.Load(invSec.Span, triSec.Span, bloSec.Span);

        _out.WriteLine($"group of {groupEvents} events");
        long whole     = Report("'processed'   (every row)", "processed");
        long selective = Report("'order-1234'  (one row)",   "order-1234");
        Report("'zzzqqq'      (absent)",  "zzzqqq");

        // One uint[] of the survivors, near enough — the sorted merge writes the answer once.
        Assert.True(whole < 8L * groupEvents, $"{whole} B for a {groupEvents}-row posting list");
        Assert.True(selective < 512, $"{selective} B to look up a term that matches one row");

        long Report(string what, string term)
        {
            for (int i = 0; i < 50; i++) idx.LookupTrigram(term);

            const int Iterations = 500;
            long b0 = GC.GetAllocatedBytesForCurrentThread();
            uint[]? last = null;
            for (int i = 0; i < Iterations; i++) last = idx.LookupTrigram(term);
            long per = (GC.GetAllocatedBytesForCurrentThread() - b0) / Iterations;

            _out.WriteLine($"  {what,-28} {per,6} B/call, {last?.Length.ToString() ?? "no info"} offsets");
            return per;
        }
    }

    /// <summary>
    /// The level-pure shortcut. Every event of the group carries the one allowed level, so the
    /// union of the level posting lists IS the group and intersecting with it cannot remove
    /// anything — but it used to build a HashSet over all of it, per group, per query.
    /// </summary>
    [Fact]
    public void ALevelPureGroupDoesNotPayForTheIdentityIntersection()
    {
        using var reader = SegmentReader.Open(_segPath);
        uint groupEvents = reader.Groups[0].EventCount;

        using var invSec = reader.RentInvertedIndexBytes(0);
        using var triSec = reader.RentTrigramIndexBytes(0);
        using var bloSec = reader.RentBloomFilterBytes(0);
        using var idx    = SegmentIndexReader.Load(invSec.Span, triSec.Span, bloSec.Span);

        (string, object?)[][] levelHints = [[(Ameto.Core.ClefFields.Level, "Information")]];
        var like = CompiledFilter.Compile("@mt like '%processed%'");

        // With the group's true event count the shortcut fires; with 0 ("unknown") it cannot,
        // which is the same code doing the work the shortcut skips.
        long withCount    = Measure(groupEvents, out var a);
        long withoutCount = Measure(0,           out var b);

        Assert.Equal(a, b);                                   // identical answers, both ways
        _out.WriteLine($"group of {groupEvents} events, like + levels");
        _out.WriteLine($"  identity recognised : {withCount} B/call");
        _out.WriteLine($"  identity intersected: {withoutCount} B/call");

        Assert.True(withCount < withoutCount,
            $"the identity intersection is not being skipped: {withCount} B vs {withoutCount} B");

        long Measure(uint count, out uint[]? result)
        {
            for (int i = 0; i < 20; i++) QueryExecutor.TryNarrowWithIndex(like, idx, levelHints, count, out _);

            const int Iterations = 200;
            long b0 = GC.GetAllocatedBytesForCurrentThread();
            uint[]? last = null;
            for (int i = 0; i < Iterations; i++)
            {
                QueryExecutor.TryNarrowWithIndex(like, idx, levelHints, count, out var c);
                last = c;
            }
            long per = (GC.GetAllocatedBytesForCurrentThread() - b0) / Iterations;
            result = last;
            return per;
        }
    }
}
