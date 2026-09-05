using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// A RUN THE CATALOG NO LONGER NAMES MUST GO — the second review round on #69.
///
/// <para>Three findings with one shape between them. A merged <c>tix-L*.tix</c> has no segment to
/// derive its path from, so unlike a per-segment run nothing can clean it up by name; the catalog
/// is the only structure that knows it exists. Every place the catalog stopped naming one, the file
/// and its open reader stayed behind — holding a bloom in native memory and a handle on Windows —
/// until a restart swept them. On an install whose entire reason for merging runs is bounded
/// memory, "until a restart" is the wrong bound.</para>
///
/// <para>The un-droppable case was worse than a leak. Liveness was judged against the ids leaving
/// in THAT call, so a run was kept alive by the memory of segments that had expired in some earlier
/// generation, and coverage was withdrawn one segment at a time — which never removed a run naming
/// two. After the first restart the withdrawal returned early (nothing covered any more) and the
/// dead run was named forever.</para>
/// </summary>
public sealed class TraceIndexRunLifetimeTests : IDisposable
{
    private readonly string            _dir;
    private readonly ITestOutputHelper _out;

    public TraceIndexRunLifetimeTests(ITestOutputHelper output)
    {
        _out = output;
        _dir = Path.Combine(Path.GetTempPath(), "ameto-runlife-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
    }

    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private TraceManifest Open() => TraceManifest.Load(_dir, NullLogger.Instance);

    private static TraceSegmentEntry Seg(ulong id) => new(id, $"seg-{id}.trc", 1000, 2000, 100);
    private static TraceIndexRun MergedRun(string path, params ulong[] covers)
        => new(2, path, 0x1000, 0x9000, 999, covers);

    [Fact]
    public void A_merged_run_dies_with_its_LAST_segment_even_across_generations()
    {
        // The leak with no exit. Three hourly segments merged into one L2 run, expiring one per
        // hour as retention catches up with them: each RemoveSegments saw only its own id leaving,
        // found the other two "not in the departing batch", and kept the run. By the third call
        // every segment it covered was gone and it was STILL kept — by the memory of the two that
        // had already left.
        var m   = Open();
        var ids = m.AdoptSegments([Seg(0), Seg(0), Seg(0)]);
        m.MarkCovered(ids[0], MergedRun("L2-0001.tix", [.. ids]));   // adds the run and the claim

        var d1 = m.RemoveSegments([ids[0]]);
        Assert.Empty(d1);                                      // two still live: the run stays
        Assert.Contains(m.Runs, r => r.FilePath == "L2-0001.tix");

        var d2 = m.RemoveSegments([ids[1]]);
        Assert.Empty(d2);
        Assert.Contains(m.Runs, r => r.FilePath == "L2-0001.tix");

        // The last one. Nothing it covers is in the catalog any more, so the run is finished —
        // and the paths come back so the caller can close the reader and delete the file.
        var d3 = m.RemoveSegments([ids[2]]);
        _out.WriteLine($"dropped on the third removal: [{string.Join(", ", d3)}]");
        Assert.Equal(["L2-0001.tix"], d3);
        Assert.Empty(m.Runs);
        Assert.Empty(Open().Runs);                             // and through the file
    }

    [Fact]
    public void A_merged_run_that_will_not_open_is_dropped_by_path_not_segment_by_segment()
    {
        // Sync reports the RUN, not its segments, and DropRuns removes it outright. The old shape
        // withdrew coverage per segment, which kept any run still naming a second one — so the
        // unopenable file survived every withdrawal, and at the next start its segments were no
        // longer covered, the withdrawal returned early, and it was named forever.
        var m   = Open();
        var ids = m.AdoptSegments([Seg(0), Seg(0), Seg(0)]);
        var bad = MergedRun("L2-broken.tix", [.. ids]);
        m.ReplaceRuns([], [bad]);
        foreach (ulong id in ids) m.MarkCovered(id, bad);
        Assert.Equal(3, m.CoveredCount);

        m.DropRuns(["L2-broken.tix"]);

        _out.WriteLine($"runs {m.Runs.Count}, covered {m.CoveredCount}");
        Assert.Empty(m.Runs);
        Assert.Equal(0, m.CoveredCount);                       // all three fall back to scanning
        foreach (ulong id in ids) Assert.False(m.IsCovered(id));
        Assert.True(m.Segments.ContainsKey(ids[0]));           // the SEGMENTS are untouched
        Assert.Empty(Open().Runs);
    }

    [Fact]
    public void Dropping_one_run_leaves_coverage_another_run_still_serves()
    {
        // Coverage is intersected with what survives, never recomputed: a segment two runs vouch
        // for keeps its claim when one of them goes.
        var m   = Open();
        var ids = m.AdoptSegments([Seg(0), Seg(0)]);
        m.ReplaceRuns([], [MergedRun("L2-a.tix", ids[0], ids[1]),
                           MergedRun("L2-b.tix", ids[1])]);
        m.MarkCovered(ids[0], MergedRun("L2-a.tix", ids[0], ids[1]));
        m.MarkCovered(ids[1], MergedRun("L2-b.tix", ids[1]));

        m.DropRuns(["L2-a.tix"]);

        _out.WriteLine($"after dropping L2-a: covered {m.CoveredCount}");
        Assert.False(m.IsCovered(ids[0]));   // only L2-a served it
        Assert.True(m.IsCovered(ids[1]));    // L2-b still does
    }

    [Fact]
    public void Dropping_a_run_never_grants_coverage_back()
    {
        // The other direction, and the reason coverage is INTERSECTED rather than rebuilt from the
        // surviving runs. A claim deliberately taken away while its run stayed named must not
        // reappear because some unrelated run was dropped.
        var m   = Open();
        var ids = m.AdoptSegments([Seg(0), Seg(0)]);
        m.ReplaceRuns([], [MergedRun("L2-a.tix", ids[0], ids[1]), MergedRun("L2-b.tix", ids[0])]);
        m.MarkCovered(ids[0], MergedRun("L2-a.tix", ids[0], ids[1]));
        Assert.False(m.IsCovered(ids[1]));   // never claimed, though L2-a names it

        m.DropRuns(["L2-b.tix"]);

        Assert.True(m.IsCovered(ids[0]));
        Assert.False(m.IsCovered(ids[1]));   // still not claimed
    }

    [Fact]
    public void Adopting_a_thousand_segments_is_one_generation()
    {
        // O(N²) → O(N). A commit rewrites and fsyncs the WHOLE manifest, and adoption was two of
        // them per segment: at a thousand segments that is two thousand rewrites of a file that is
        // itself growing to a thousand entries, on the first start after the upgrade — the start
        // that has the most to adopt. The generation counter is the visible proxy for the fsync
        // count, so it is what this asserts.
        var m = Open();
        var drafts = new List<TraceSegmentEntry>(1000);
        for (int i = 0; i < 1000; i++) drafts.Add(Seg(0) with { FilePath = $"cold-{i}.trc" });

        ulong before = m.Generation;
        var ids = m.AdoptSegments(drafts);
        ulong after = m.Generation;

        _out.WriteLine($"1000 segments adopted in {after - before} generation(s)");
        Assert.Equal(1UL, after - before);
        Assert.Equal(1000, ids.Count);
        Assert.Equal(1000, ids.Distinct().Count());          // and every id is its own
        Assert.Equal(1000, m.Segments.Count);

        // The counter is persisted in the same generation, so a restart cannot reissue any of them.
        Assert.True(Open().AllocateSegmentId() > ids.Max());
    }

    [Fact]
    public async Task An_operator_can_switch_the_index_off_and_still_read_every_trace()
    {
        // THE ROLLBACK THE DESIGN CLAIMS, MADE REACHABLE. "Coverage can be dropped to empty at any
        // moment and the engine goes back to scanning" was true — from a test seam. An operator
        // watching a trace return too few spans had no way to take it. Now it is one setting and a
        // restart, and this test is the proof that the claim survives being exercised.
        string dir = Path.Combine(_dir, "engine");
        Directory.CreateDirectory(dir);

        const long ms = 1_000_000L;
        long baseNano = new DateTimeOffset(2026, 8, 1, 12, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds() * ms;
        TraceId Trace(int i) => new(
            unchecked((ulong)(i * 6364136223846793005L + 1442695040888963407L)),
            unchecked((ulong)(i * 2862933555777941757L + 3037000493L)));

        using (var on = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance))
        {
            for (int t = 0; t < 200; t++)
                for (int k = 0; k < 3; k++)
                    on.WriteSpan(new SpanIngestItem
                    {
                        TraceId = Trace(t), SpanId = new SpanId((ulong)(t * 3 + k + 1)), ParentSpanId = default,
                        StartTimeUnixNano = baseNano + (t * 10 + k) * ms, DurationNanos = 2 * ms,
                        Name = "GET /orders", ServiceName = "billing",
                        Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
                    });
            on.FlushHotTier();
            Assert.Equal(1, on.IndexCoverage.Covered);
            Assert.Single(Directory.EnumerateFiles(dir, "*.tix"));
        }

        // Ameto:Traces:IndexEnabled=false, then a restart.
        using var off = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance,
                                               indexEnabled: false);
        off.LoadColdSegments();

        _out.WriteLine($"index off: coverage {off.IndexCoverage}, runs {off.IndexStatsForTest.Runs}");
        Assert.Equal(0, off.IndexCoverage.Covered);
        Assert.Equal(0, off.IndexStatsForTest.Runs);
        Assert.False(off.BackfillNextSegment());        // and it does not quietly re-cover
        Assert.False(off.CompactIndexOnce());
        Assert.Equal(0, off.IndexCoverage.Covered);

        // Every trace is still there — scanned, as before the index existed. That is the trade.
        var got = new List<SpanRecord>();
        await foreach (var s in off.GetTraceAsync(Trace(137))) got.Add(s);
        Assert.Equal(3, got.Count);
        Assert.Equal(1, off.SegmentsOpenedByLastTraceLookup);

        // The .tix is left where it is: turning the switch back on costs a re-backfill, not data.
        Assert.Single(Directory.EnumerateFiles(dir, "*.tix"));
    }
}
