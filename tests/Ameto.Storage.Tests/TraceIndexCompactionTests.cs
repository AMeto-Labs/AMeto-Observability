using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// MERGING RUNS: fewer blooms in memory, and not one answer different.
///
/// <para>One index run per segment is free at a hundred segments and is not free at ten thousand —
/// each run holds its bloom filter and sparse block map in RAM permanently. Merging is how that
/// stops scaling with the segment count. It buys nothing else: every trace resolvable before a
/// merge must be resolvable after it, to the same segment, at the same offsets.</para>
///
/// <para>The property that makes it safe to run in the background is that it NEVER CHANGES WHAT IS
/// COVERED. The merged run carries the union of its inputs' coverage, so the same segments are
/// vouched for on both sides, and the manifest is touched once, after the rename. These tests push
/// on that from both ends: the happy path, and the two ways a merge can go wrong.</para>
/// </summary>
public sealed class TraceIndexCompactionTests : IDisposable
{
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-tixmerge-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long              _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public TraceIndexCompactionTests(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_root);
    }

    public void Dispose() { try { Directory.Delete(_root, true); } catch { } }

    private string Dir(string name)
    {
        string d = Path.Combine(_root, name);
        Directory.CreateDirectory(d);
        return d;
    }

    private static TraceStorageEngine Engine(string dir) => new(dir, NullLogger<TraceStorageEngine>.Instance);

    private static TraceId Id(int i) => new(
        unchecked((ulong)(i * 6364136223846793005L + 1442695040888963407L)),
        unchecked((ulong)(i * 2862933555777941757L + 3037000493L)));

    private static void WriteSpan(TraceStorageEngine e, TraceId trace, ulong spanId, long startNano)
        => e.WriteSpan(new SpanIngestItem
        {
            TraceId = trace, SpanId = new SpanId(spanId), ParentSpanId = default,
            StartTimeUnixNano = startNano, DurationNanos = 2 * Ms,
            Name = "GET /orders", ServiceName = "billing",
            Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
        });

    /// <summary>One segment per flush, each with its own traces. Returns every planted trace.</summary>
    private List<TraceId> Build(TraceStorageEngine e, int segments, int tracesPer = 20)
    {
        var all = new List<TraceId>();
        ulong span = 1;
        for (int s = 0; s < segments; s++)
        {
            for (int t = 0; t < tracesPer; t++)
            {
                var id = Id(s * 1000 + t);
                all.Add(id);
                for (int k = 0; k < 3; k++)
                    WriteSpan(e, id, span++, _baseNano + (s * 10_000 + t * 10 + k) * Ms);
            }
            e.FlushHotTier();
        }
        return all;
    }

    private static async Task<List<SpanRecord>> Read(TraceStorageEngine e, TraceId id)
    {
        var got = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(id)) got.Add(s);
        return got;
    }

    [Fact]
    public async Task Ten_per_segment_runs_become_one_and_every_trace_still_resolves()
    {
        using var e = Engine(Dir("merge"));
        var all = Build(e, segments: 12);

        Assert.Equal(12, e.IndexStatsForTest.Runs);
        long memBefore = e.IndexStatsForTest.RetainedBytes;

        Assert.True(e.CompactIndexOnce());

        var (runs, memAfter) = e.IndexStatsForTest;
        _out.WriteLine($"runs 12 → {runs}; RAM {memBefore} → {memAfter} B");

        // ALL of them, not ten: once a level is over the ratio the whole level goes, capped at
        // MaxRunsPerMerge. Leaving a remainder behind would mean the next pass merges the leftovers
        // against the big result, which is the accumulator rewrite the levels exist to avoid.
        Assert.Equal(1, runs);
        Assert.True(memAfter < memBefore, $"merging did not reduce memory ({memBefore} → {memAfter})");

        // NOT ONE ANSWER DIFFERENT, which is the only thing merging is allowed to leave unchanged.
        foreach (var id in all)
        {
            var spans = await Read(e, id);
            Assert.Equal(3, spans.Count);
            Assert.All(spans, s => Assert.Equal(id, s.TraceId));
        }

        // And the lookups are still one-segment lookups.
        await Read(e, all[^1]);
        Assert.Equal(1, e.SegmentsOpenedByLastTraceLookup);
    }

    [Fact]
    public void Coverage_is_identical_on_both_sides_of_a_merge()
    {
        // The property that makes this safe to run in the background and safe to interrupt: the
        // merged run carries the union of its inputs' coverage, so no segment gains or loses the
        // index's vouching because a background chore happened to run.
        using var e = Engine(Dir("coverage"));
        Build(e, segments: 11);

        var before = e.IndexCoverage;
        Assert.Equal((11, 11), before);

        Assert.True(e.CompactIndexOnce());

        var after = e.IndexCoverage;
        _out.WriteLine($"coverage {before} → {after}, runs now {e.IndexStatsForTest.Runs}");
        Assert.Equal(before, after);
    }

    [Fact]
    public void Nothing_merges_until_a_level_is_actually_full()
    {
        // Merging early would rewrite the same entries over and over for no memory saved. Nine
        // runs is under the ten-per-level ratio and must be left alone.
        using var e = Engine(Dir("threshold"));
        Build(e, segments: 9);

        Assert.Equal(9, e.IndexStatsForTest.Runs);
        Assert.False(e.CompactIndexOnce());
        Assert.Equal(9, e.IndexStatsForTest.Runs);

        Build(e, segments: 1);
        Assert.Equal(10, e.IndexStatsForTest.Runs);
        Assert.True(e.CompactIndexOnce());
        _out.WriteLine($"9 runs → no merge; 10 runs → merged to {e.IndexStatsForTest.Runs}");
        Assert.Equal(1, e.IndexStatsForTest.Runs);
    }

    [Fact]
    public async Task Entries_of_segments_that_are_gone_are_dropped_by_the_merge()
    {
        // The no-tombstone story's other half. A merged run keeps entries for segments that later
        // expire — harmless, because the read path matches hits against live segments, but they
        // accumulate. The merge is the only place they are physically removed.
        string dir = Dir("dead");
        using var e = Engine(dir);

        long oldNano = DateTimeOffset.UtcNow.AddDays(-30).ToUnixTimeMilliseconds() * Ms;
        var doomed = Id(70_001);
        for (int k = 0; k < 3; k++) WriteSpan(e, doomed, (ulong)(900 + k), oldNano + k * Ms);
        e.FlushHotTier();

        long recent = DateTimeOffset.UtcNow.AddHours(-1).ToUnixTimeMilliseconds() * Ms;
        ulong span = 1;
        var live = new List<TraceId>();
        for (int s = 0; s < 10; s++)
        {
            for (int t = 0; t < 20; t++)
            {
                var id = Id(s * 1000 + t);
                live.Add(id);
                for (int k = 0; k < 3; k++)
                    WriteSpan(e, id, span++, recent + (s * 10_000 + t * 10 + k) * Ms);
            }
            e.FlushHotTier();
        }

        int entriesBefore = e.IndexEntryCountForTest;
        await e.PruneAsync(TimeSpan.FromDays(7));       // the old segment and its L1 run go

        Assert.True(e.CompactIndexOnce());
        int entriesAfter = e.IndexEntryCountForTest;
        _out.WriteLine($"index entries {entriesBefore} → {entriesAfter} after prune + merge");

        // Everything still alive still resolves.
        foreach (var id in live.Take(20))
            Assert.Equal(3, (await Read(e, id)).Count);

        // And the expired trace is gone from the index as well as from disk.
        Assert.Empty(await Read(e, doomed));
    }

    [Fact]
    public void A_merge_whose_input_will_not_open_is_abandoned_rather_than_half_done()
    {
        // THE FAILURE THAT MUST NOT PRODUCE A FILE. Carrying on without an unreadable input would
        // write a merged run claiming its inputs' coverage while missing that input's entries — a
        // covered segment whose traces the index cannot find, which is the one thing this design
        // does not tolerate. Abandoning leaves the old runs in place, still covering, still right.
        string dir = Dir("badinput");
        using var e = Engine(dir);
        Build(e, segments: 10);
        Assert.Equal(10, e.IndexStatsForTest.Runs);
        var coverageBefore = e.IndexCoverage;

        // Destroy one run's header while leaving it named by the manifest.
        var victim = Directory.EnumerateFiles(dir, "*.tix").OrderBy(f => f).First();
        var raw = File.ReadAllBytes(victim);
        raw[0] ^= 0xFF;                                  // wrong magic → Open returns null
        File.WriteAllBytes(victim, raw);

        Assert.False(e.CompactIndexOnce());

        _out.WriteLine($"merge refused; runs still {e.IndexStatsForTest.Runs}, "
                     + $"coverage {e.IndexCoverage} (was {coverageBefore})");
        Assert.Equal(coverageBefore, e.IndexCoverage);
        Assert.Empty(Directory.EnumerateFiles(dir, "tix-L*.tix"));   // nothing was written
    }

    [Fact]
    public void An_orphaned_merged_run_is_swept_at_startup_and_a_live_one_is_not()
    {
        // Merged runs are named tix-L{level}-{guid}.tix, which the startup sweep's "spans-*.tmp"
        // pattern never matched — a per-segment run is spans-….tix and IS caught, a merged one is
        // not. A crash between the merge's rename and its manifest write therefore left a file
        // nothing names and nothing deletes: harmless to correctness, since only runs the manifest
        // names are ever opened, and a slow disk leak all the same.
        string dir = Dir("orphans");
        string liveRun;
        using (var e = Engine(dir))
        {
            Build(e, segments: 10);
            Assert.True(e.CompactIndexOnce());
            liveRun = Directory.EnumerateFiles(dir, "tix-L*.tix").Single();
        }

        // Exactly what a killed merge leaves behind.
        string orphan    = Path.Combine(dir, $"tix-L9-{Guid.NewGuid():N}.tix");
        string orphanTmp = Path.Combine(dir, $"tix-L9-{Guid.NewGuid():N}.tix.tmp");
        File.WriteAllBytes(orphan,    new byte[128]);
        File.WriteAllBytes(orphanTmp, new byte[64]);

        using var reopened = Engine(dir);

        _out.WriteLine($"after restart: {Directory.EnumerateFiles(dir, "tix-L*").Count()} tix-L* file(s) left");
        Assert.False(File.Exists(orphan),    "an orphaned merged run survived the sweep");
        Assert.False(File.Exists(orphanTmp), "an orphaned merge temp survived the sweep");

        // AND THE LIVE ONE IS UNTOUCHED, which is the half a sweep gets wrong. Deleting by name
        // pattern or by age would have taken this too.
        Assert.True(File.Exists(liveRun), "the sweep deleted a run the manifest still names");
        reopened.LoadColdSegments();
        Assert.Equal((10, 10), reopened.IndexCoverage);
    }

    [Fact]
    public async Task A_merged_index_survives_a_restart()
    {
        string dir = Dir("restart");
        List<TraceId> all;
        using (var e = Engine(dir))
        {
            all = Build(e, segments: 11);
            Assert.True(e.CompactIndexOnce());
            Assert.True(e.IndexStatsForTest.Runs < 11);
        }

        using var reopened = Engine(dir);
        reopened.LoadColdSegments();
        _out.WriteLine($"after restart: runs {reopened.IndexStatsForTest.Runs}, "
                     + $"coverage {reopened.IndexCoverage}");
        Assert.Equal((11, 11), reopened.IndexCoverage);

        foreach (var id in all.Take(30))
            Assert.Equal(3, (await Read(reopened, id)).Count);

        await Read(reopened, all[5]);
        Assert.Equal(1, reopened.SegmentsOpenedByLastTraceLookup);
    }

    [Fact]
    public void Levels_climb_rather_than_merging_the_same_run_forever()
    {
        // Without a level the compactor would pick the same big run every pass and rewrite it
        // against each new arrival — the accumulator shape the segment compactor's size tiers
        // exist to avoid. A merged run sits at the next level and is only touched again when THAT
        // level fills.
        using var e = Engine(Dir("levels"));
        Build(e, segments: 10);
        Assert.True(e.CompactIndexOnce());
        Assert.Equal(1, e.IndexStatsForTest.Runs);

        // Nine more L1 runs: still under the ratio at L1, and L2 has one run, so nothing is due.
        Build(e, segments: 9);
        Assert.False(e.CompactIndexOnce());
        _out.WriteLine($"1 L2 run + 9 L1 runs → nothing due; total {e.IndexStatsForTest.Runs}");
        Assert.Equal(10, e.IndexStatsForTest.Runs);

        // The tenth L1 fills that level again.
        Build(e, segments: 1);
        Assert.True(e.CompactIndexOnce());
        _out.WriteLine($"after the second L1 merge: {e.IndexStatsForTest.Runs} runs");
        Assert.Equal(2, e.IndexStatsForTest.Runs);       // two L2 runs, no L1 left
    }

    /// <summary>
    /// THE ENTRY CAP FOLLOWS THE MERGE BUDGET (#94): two million at the cap — the large host keeps
    /// exactly what it had — and that share of it below, down to the share of the smallest budget a
    /// host derives (16 MiB: 459 649 entries). The stand's derived budget (384 MB heap) asks for about
    /// a third.
    /// </summary>
    [Fact]
    public void The_index_merge_entry_cap_scales_with_the_merge_budget()
    {
        long stand = Ameto.Core.MemoryBudgets.Derive(managedLimitBytes: 384L << 20, physicalLimitBytes: 512L << 20).TraceMergeBytes;
        long large = Ameto.Core.MemoryBudgets.Derive(64L << 30, 64L << 30).TraceMergeBytes;
        long least = Ameto.Core.MemoryBudgets.Derive(managedLimitBytes: 256L << 20, physicalLimitBytes: 512L << 20).TraceMergeBytes;
        int  atStand = TraceIndexCompactor.MaxEntriesPerMergeFor(stand);
        _out.WriteLine($"merge budget {stand:N0} B (stand) → {atStand:N0} entries; {large:N0} B (large) → "
                     + $"{TraceIndexCompactor.MaxEntriesPerMergeFor(large):N0}; floor {least:N0} B → "
                     + $"{TraceIndexCompactor.MaxEntriesPerMergeFor(least):N0}");

        Assert.Equal(Ameto.Core.MemoryBudgets.TraceMergeCapBytes, large);
        Assert.Equal(TraceIndexCompactor.MaxEntriesPerMerge, TraceIndexCompactor.MaxEntriesPerMergeFor(large));
        Assert.Equal(TraceIndexCompactor.MaxEntriesPerMerge, TraceIndexCompactor.MaxEntriesPerMergeFor(10 * large));
        Assert.Equal(TraceIndexCompactor.MaxEntriesPerMerge * stand / large, atStand);
        Assert.True(atStand < TraceIndexCompactor.MaxEntriesPerMerge / 2);

        // The floor is MemoryBudgets' own smallest derived budget, and nothing goes below its share.
        Assert.Equal(TraceIndexCompactor.MinMergeBudgetBytes, least);
        Assert.Equal(459_649, TraceIndexCompactor.MaxEntriesPerMergeFor(least));
        Assert.Equal(459_649, TraceIndexCompactor.MaxEntriesPerMergeFor(0));
        Assert.Equal(459_649, TraceIndexCompactor.MaxEntriesPerMergeFor(14_600));
    }

    /// <summary>
    /// …AND THE ENGINE CARRIES ITS OWN BUDGET'S CAP. A run of a stand-sized budget cannot be built
    /// here (it takes 661 895 entries to reach), so the selector is asked with the engine's figure
    /// over runs that only claim their sizes: three of ten 200 000-entry runs fit under the stand's
    /// cap, where the constant took all ten.
    /// </summary>
    [Fact]
    public void The_engine_plans_an_index_merge_by_its_own_merge_budget()
    {
        long stand = Ameto.Core.MemoryBudgets.Derive(managedLimitBytes: 384L << 20, physicalLimitBytes: 512L << 20).TraceMergeBytes;
        using var e = new TraceStorageEngine(Dir("budget-cap"), NullLogger<TraceStorageEngine>.Instance,
                                             writeSegmentFormatV4: false, indexEnabled: true,
                                             new Ameto.Core.TracesOptions { MergeBudgetBytes = stand });
        Assert.Equal(TraceIndexCompactor.MaxEntriesPerMergeFor(stand), e.IndexMergeMaxEntries);

        var runs = new List<TraceIndexRun>();
        for (int i = 0; i < 10; i++)
            runs.Add(new TraceIndexRun(1, $"L1-{i}.tix", 0x1000, 0x9000, 200_000, [(ulong)(i + 1)]));
        Assert.Equal(3,  TraceIndexCompactor.SelectMergeBatch(runs, e.IndexMergeMaxEntries).Count);
        Assert.Equal(10, TraceIndexCompactor.SelectMergeBatch(runs).Count);
    }

    /// <summary>
    /// A TINY EXPLICIT BUDGET DOES NOT STARVE THE INDEX MERGE (review of this branch).
    /// <c>Traces:MergeBudgetBytes</c> has no floor, and 14 600 B used to scale the cap to 400 entries:
    /// ten L1 runs of 100 entries merged four at a time, and under fast flushes the L1 run count
    /// grew without bound. The cap now stops at the smallest derived budget's share, so the level
    /// merges whole. Reverted (no floor): 7 runs left after one merge, not 1.
    /// </summary>
    [Fact]
    public void A_tiny_explicit_merge_budget_still_merges_a_level_whole()
    {
        using var e = new TraceStorageEngine(Dir("budget-tiny"), NullLogger<TraceStorageEngine>.Instance,
                                             writeSegmentFormatV4: false, indexEnabled: true,
                                             new Ameto.Core.TracesOptions { MergeBudgetBytes = 14_600 });
        Assert.Equal(459_649, e.IndexMergeMaxEntries);

        Build(e, segments: 10, tracesPer: 100);
        Assert.Equal(10, e.IndexStatsForTest.Runs);

        Assert.True(e.CompactIndexOnce());
        _out.WriteLine($"cap {e.IndexMergeMaxEntries} entries: 10 runs of 100 → {e.IndexStatsForTest.Runs} run(s)");
        Assert.Equal(1, e.IndexStatsForTest.Runs);
    }
}
