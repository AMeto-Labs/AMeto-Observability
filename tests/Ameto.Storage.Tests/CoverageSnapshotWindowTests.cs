using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// A STALE COVERAGE SNAPSHOT MUST NOT AUTHORISE A SKIP — the last finding on #69, and the mirror
/// image of the one before it.
///
/// <para>Round 2 moved the coverage sample from inside the per-segment loop to a single snapshot
/// taken BEFORE the lookup, because sampling afterwards let the backfill's <c>MarkCovered</c> flip
/// a segment covered after the fact and skip it on the strength of a run the lookup never saw. That
/// was right, and it was half the answer: the fix turned the hole around rather than closing it.
/// Both writers that SHRINK coverage — retention and segment compaction — withdraw the claim first
/// and close the readers second, so the safe state is "uncovered, run still open", and a snapshot
/// taken before the withdrawal lands squarely in the unsafe one.</para>
///
/// <para>What makes it silent is that a skip is inferred from the mere ABSENCE of a hit.
/// <c>Unanswerable</c> only speaks for a run that was asked and failed; a run already gone from the
/// store is not asked at all, so it never reports that it could not answer. The request sees
/// "covered, no hits" and drops the segment's spans, with no exception and no log line.</para>
///
/// <para>The window is two adjacent lines wide and the loss is transient — the next request answers
/// in full. It is still worth closing: retention runs hourly, compaction more often, and on a busy
/// install this eventually happens and is seen as "the waterfall is sometimes short", which is the
/// hardest kind of report to act on.</para>
/// </summary>
public sealed class CoverageSnapshotWindowTests : IDisposable
{
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-covwin-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long              _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public CoverageSnapshotWindowTests(ITestOutputHelper output)
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

    private static TraceId Id(int i) => new(
        unchecked((ulong)(i * 6364136223846793005L + 1442695040888963407L)),
        unchecked((ulong)(i * 2862933555777941757L + 3037000493L)));

    private void Write(TraceStorageEngine e, TraceId trace, ulong spanId, long startNano) =>
        e.WriteSpan(new SpanIngestItem
        {
            TraceId = trace, SpanId = new SpanId(spanId), ParentSpanId = default,
            StartTimeUnixNano = startNano, DurationNanos = 2 * Ms,
            Name = "GET /orders", ServiceName = "billing",
            Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
        });

    [Fact]
    public async Task Coverage_withdrawn_inside_the_window_does_not_authorise_a_skip()
    {
        // THE FINDING, ISOLATED. The seam fires between the coverage snapshot and the lookup — the
        // two adjacent lines a writer has to fit inside — and takes coverage away there, exactly as
        // PruneAsync and CompactOnePass do, leaving the SEGMENT FILES ALONE.
        //
        // That isolation is the point. Running a whole compaction here also empties the answer, but
        // for a second and older reason: this request holds a snapshot of segment PATHS that
        // compaction has unlinked, and the merged replacement is not in it. That transient predates
        // the index and this branch (see the "deleted mid-flight" note above GetTraceAsync's cold
        // scan) and a test that tripped over it would pass or fail for the wrong reason. Withdrawing
        // coverage alone leaves both files readable, so the only thing that can lose the spans is
        // the stale snapshot authorising a skip.
        string dir = Dir("window");
        var    planted = Id(4_242);

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        // Two segments, each with its own run, and one trace with spans in BOTH — so a skip of
        // either is visible as a SHORT answer, not just an empty one.
        for (int t = 0; t < 300; t++) Write(e, Id(t), (ulong)(t + 1), _baseNano + t * Ms);
        Write(e, planted, 90_001, _baseNano + 400 * Ms);
        e.FlushHotTier();

        for (int t = 300; t < 600; t++) Write(e, Id(t), (ulong)(t + 1), _baseNano + t * Ms);
        Write(e, planted, 90_002, _baseNano + 700 * Ms);
        e.FlushHotTier();

        Assert.Equal(2, e.ColdSegmentCountForTest);
        Assert.Equal((2, 2), e.IndexCoverage);

        // Healthy first, so the assertion below is about the window and not about the fixture.
        var before = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(planted)) before.Add(s);
        _out.WriteLine($"before the window: {before.Count} span(s), "
                     + $"{e.SegmentsSkippedByLastTraceLookup} skipped");
        Assert.Equal(2, before.Count);

        int fired = 0;
        e._betweenCoverageAndLookupForTest = () =>
        {
            if (fired++ > 0) return;
            // Coverage withdrawn and the runs closed — the state both shrinking writers pass
            // through — while the .trc files stay exactly where they are.
            e.DisableTraceIndexForTest();
        };

        var during = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(planted)) during.Add(s);

        _out.WriteLine($"coverage withdrawn inside the window: {during.Count} span(s), "
                     + $"{e.SegmentsOpenedByLastTraceLookup} opened, "
                     + $"{e.SegmentsSkippedByLastTraceLookup} skipped");
        Assert.Equal(1, fired);

        // WITHOUT THE SECOND SAMPLE both segments are skipped on a snapshot that was true a
        // microsecond ago, and this comes back empty with no exception and no log line.
        Assert.Equal(0, e.SegmentsSkippedByLastTraceLookup);
        Assert.Equal(2, during.Count);
        Assert.All(during, s => Assert.Equal(planted, s.TraceId));

        // And the segments are still there afterwards — nothing was destroyed, only un-claimed.
        Assert.Equal(2, e.ColdSegmentCountForTest);
    }

    [Fact]
    public async Task A_partial_withdrawal_reads_the_uncovered_segment_and_still_uses_the_other()
    {
        // The clause must cost only what it has to. One segment loses its claim inside the window
        // and the other keeps it: the first is read, the second is still skipped on its own run,
        // so the answer is whole and the index is still doing its job.
        string dir = Dir("partial");
        var    planted = Id(999_555);   // outside the loop range, so it lives in ONE segment

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        for (int t = 0; t < 300; t++) Write(e, Id(t), (ulong)(t + 1), _baseNano + t * Ms);
        Write(e, planted, 70_001, _baseNano + 400 * Ms);
        e.FlushHotTier();
        for (int t = 300; t < 600; t++) Write(e, Id(t), (ulong)(t + 1), _baseNano + t * Ms);
        e.FlushHotTier();

        Assert.Equal((2, 2), e.IndexCoverage);

        // Nothing moves in the window: the trace is in one segment, the other is skipped on its
        // run. This is the measurement the whole branch exists to produce.
        var got = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(planted)) got.Add(s);
        _out.WriteLine($"steady state: {got.Count} span(s), {e.SegmentsOpenedByLastTraceLookup} opened, "
                     + $"{e.SegmentsSkippedByLastTraceLookup} skipped");
        Assert.Single(got);
        Assert.Equal(1, e.SegmentsOpenedByLastTraceLookup);
        Assert.Equal(1, e.SegmentsSkippedByLastTraceLookup);
    }

    [Fact]
    public async Task The_fix_does_not_give_back_the_skip_it_replaced()
    {
        // THE ROUND-2 PROPERTY, RE-ASSERTED — and the first version of this test could not fail.
        //
        // It disabled the index outright before the lookup, which made `useIndex` false for the
        // whole request: the skip condition was never evaluated, so `skipped == 0` held no matter
        // what the condition said. Reverting the guard it is named for left it green.
        //
        // THE INDEX HAS TO STAY LIVE FOR THE ASSERTION TO MEAN ANYTHING. So one segment keeps its
        // run through the whole request — `useIndex` is true and the skip decision really runs —
        // while a SECOND segment is covered by the backfill inside the window, after
        // coveredAtLookup was taken. That second one must still be read: a claim that appeared
        // after the lookup is a claim about a run the lookup never saw.
        string dir = Dir("backfill");
        var    planted = Id(31_337);

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        // First segment: indexed and left alone, so the index is in use for this request.
        for (int t = 0; t < 200; t++) Write(e, Id(t), (ulong)(t + 1), _baseNano + t * Ms);
        e.FlushHotTier();

        // Second segment, holding the trace, published with no run of its own.
        e.SuppressIndexRunsForTest = true;
        for (int t = 200; t < 400; t++) Write(e, Id(t), (ulong)(t + 1), _baseNano + t * Ms);
        Write(e, planted, 80_001, _baseNano + 500 * Ms);
        e.FlushHotTier();
        e.SuppressIndexRunsForTest = false;

        Assert.Equal(2, e.ColdSegmentCountForTest);
        Assert.Equal(1, e.IndexCoverage.Covered);       // one covered, one not — index live
        Assert.True(e.IndexStatsForTest.Runs > 0, "no run open, so useIndex would be false");

        int fired = 0;
        e._betweenCoverageAndLookupForTest = () =>
        {
            if (fired++ > 0) return;
            e.BackfillNextSegment();      // covers the SECOND segment after the snapshot was taken
        };

        var got = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(planted)) got.Add(s);

        _out.WriteLine($"backfill inside the window: {got.Count} span(s), coverage {e.IndexCoverage}, "
                     + $"{e.SegmentsOpenedByLastTraceLookup} opened, "
                     + $"{e.SegmentsSkippedByLastTraceLookup} skipped");
        Assert.Equal(1, fired);
        Assert.Equal(2, e.IndexCoverage.Covered);       // the backfill really did run

        // The trace survives: its segment was covered only AFTER coveredAtLookup, so it is read.
        Assert.Single(got);
        Assert.Equal(planted, got[0].TraceId);

        // And the index is still doing its job on the segment that was covered all along.
        Assert.Equal(1, e.SegmentsSkippedByLastTraceLookup);
        Assert.Equal(1, e.SegmentsOpenedByLastTraceLookup);
    }

    [Fact]
    public async Task A_compaction_that_completes_mid_request_does_not_cost_the_trace()
    {
        // THE SECOND, OLDER LOSS, and the one this file's first draft tripped over while trying to
        // test the first. A request takes ONE snapshot of _coldSegments and keeps it for its whole
        // life. Compaction publishes the merged segment and unlinks its sources, so a request that
        // started first reads paths that no longer exist and never hears about the file that now
        // holds those spans: every source contributes nothing, the replacement is not in its list,
        // and the trace comes back short. No exception, no log line, HTTP 200.
        //
        // It predates the trace-id index — the same snapshot-versus-compaction shape exists with
        // no index at all — which is why the fix is in the walk rather than in the coverage rule.
        string dir = Dir("overtaken");
        var    planted = Id(4_242);

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        // Two small segments, both compaction candidates, and one trace with a span in each.
        for (int t = 0; t < 300; t++) Write(e, Id(t), (ulong)(t + 1), _baseNano + t * Ms);
        Write(e, planted, 60_001, _baseNano + 400 * Ms);
        e.FlushHotTier();

        for (int t = 300; t < 600; t++) Write(e, Id(t), (ulong)(t + 1), _baseNano + t * Ms);
        Write(e, planted, 60_002, _baseNano + 700 * Ms);
        e.FlushHotTier();

        Assert.Equal(2, e.ColdSegmentCountForTest);

        var before = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(planted)) before.Add(s);
        Assert.Equal(2, before.Count);

        // A WHOLE COMPACTION, at the instant the walk is committed to its snapshot. `segs` is
        // captured before this seam fires, so everything after it reads a list nobody maintains
        // any more: the sources are unlinked and the merged file that replaced them is not in it.
        int fired = 0;
        e._betweenCoverageAndLookupForTest = () =>
        {
            if (Interlocked.Increment(ref fired) > 1) return;
            e.CompactSmallSegments();
        };

        var during = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(planted)) during.Add(s);

        _out.WriteLine($"compaction completed mid-request: {during.Count} span(s), "
                     + $"{e.SegmentsOpenedByLastTraceLookup} opened; "
                     + $"cold segments now {e.ColdSegmentCountForTest}");

        Assert.Equal(1, e.ColdSegmentCountForTest);          // the merge really did happen
        Assert.Equal(2, during.Count);                       // and the trace survived it whole
        Assert.All(during, s => Assert.Equal(planted, s.TraceId));

        // Steady state afterwards: one segment, answered through its own run.
        e._betweenCoverageAndLookupForTest = null;
        var after = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(planted)) after.Add(s);
        _out.WriteLine($"after: {after.Count} span(s), {e.SegmentsOpenedByLastTraceLookup} opened");
        Assert.Equal(2, after.Count);
        Assert.Equal(1, e.SegmentsOpenedByLastTraceLookup);
    }

    [Fact]
    public async Task Real_retention_mid_request_costs_no_extra_reads()
    {
        // THE PRICE OF THE REPAIR, ASSERTED — which the first version of this test did not do. It
        // checked only that the trace came back, and that held with the repair, without it, and
        // with the `vanished > 0` gate deleted entirely, which is the "every deletion became extra
        // reads" bug it was supposed to rule out. Reading the OPENED COUNT is what makes it a
        // statement about cost.
        //
        // And it now runs REAL retention rather than an external unlink. The order matters: prune
        // takes the segment out of _coldSegments FIRST and unlinks its file second, so a request
        // holding the older snapshot meets a path that is gone and no replacement ever appears.
        // Deleting the file behind the engine's back left the segment in the live snapshot and
        // exercised a different path than the one the test is named for.
        string dir = Dir("retire");
        var    planted = Id(999_777);
        long   nowNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * Ms;

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        // Old enough to be past a one-hour TTL, and dated from NOW so prune's cutoff reaches it.
        for (int t = 0; t < 200; t++)
            Write(e, Id(t), (ulong)(t + 1), nowNano - 48L * 3600 * 1_000_000_000L + t * Ms);
        e.FlushHotTier();

        // Fresh, holding the trace we ask for, and comfortably inside the TTL.
        for (int t = 200; t < 400; t++) Write(e, Id(t), (ulong)(t + 1), nowNano - 60L * 1_000_000_000L + t * Ms);
        Write(e, planted, 50_001, nowNano - 30L * 1_000_000_000L);
        e.FlushHotTier();
        Assert.Equal(2, e.ColdSegmentCountForTest);

        int fired = 0;
        e._betweenCoverageAndLookupForTest = () =>
        {
            if (Interlocked.Increment(ref fired) > 1) return;
            e.PruneAsync(TimeSpan.FromHours(1)).GetAwaiter().GetResult();
        };

        var got = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(planted)) got.Add(s);

        _out.WriteLine($"real retention mid-request: {got.Count} span(s), "
                     + $"{e.SegmentsOpenedByLastTraceLookup} opened, "
                     + $"{e.SegmentsSkippedByLastTraceLookup} skipped; "
                     + $"cold segments now {e.ColdSegmentCountForTest}");

        Assert.Equal(1, fired);
        Assert.Equal(1, e.ColdSegmentCountForTest);      // retention really ran
        Assert.Single(got);
        Assert.Equal(planted, got[0].TraceId);

        // THE COST. Two segments were in the snapshot; the retained one contributes nothing and no
        // replacement exists, so the request opens exactly the segments the snapshot named and
        // reads not one file more. A repair that rescanned _coldSegments unconditionally, or that
        // re-read every appeared segment on every lookup, pushes this number up.
        Assert.True(e.SegmentsOpenedByLastTraceLookup <= 2,
            $"the recovery pass read {e.SegmentsOpenedByLastTraceLookup} segments where the snapshot "
          + "named 2 and nothing replaced the retained one");
    }

    [Fact]
    public async Task The_recovery_pass_survives_a_compaction_with_the_index_switched_off()
    {
        // THE CRASH THE REPAIR SHIPPED WITH, and the configuration it shipped it into.
        //
        // The recovery pass guarded its use of the lookup on `SegmentId != 0`, which is orthogonal
        // to whether any run is open: ids are handed out unconditionally by the flush and the
        // compaction, while runs are not written at all when the index is off. So
        // `default(TraceIndexAnswer).Hits` — null — was dereferenced, outside the try below it,
        // and left GetTraceAsync as a NullReferenceException; GET /api/traces/{id} has no wrapper,
        // so it reaches the pipeline as a 500.
        //
        // The reachable state is Ameto:Traces:IndexEnabled=false — the documented operator
        // rollback, whose docstring promises it "costs speed and nothing else". A repair written to
        // stop a silently short answer turned it into a crash for the one person who had already
        // decided the index was suspect.
        string dir = Dir("indexoff");
        var    planted = Id(4_242);

        // Written with the index ON so the segments exist and carry ids, then reopened with it OFF
        // — which is exactly what the operator does: stop, change the setting, start.
        using (var seed = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance))
        {
            for (int t = 0; t < 300; t++) Write(seed, Id(t), (ulong)(t + 1), _baseNano + t * Ms);
            Write(seed, planted, 30_001, _baseNano + 400 * Ms);
            seed.FlushHotTier();
            for (int t = 300; t < 600; t++) Write(seed, Id(t), (ulong)(t + 1), _baseNano + t * Ms);
            Write(seed, planted, 30_002, _baseNano + 700 * Ms);
            seed.FlushHotTier();
        }

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance,
                                             indexEnabled: false);
        e.LoadColdSegments();
        Assert.Equal(2, e.ColdSegmentCountForTest);
        Assert.Equal(0, e.IndexStatsForTest.Runs);                    // no runs open at all
        Assert.All(e.ColdSegmentsForTest, s => Assert.NotEqual(0UL, s.SegmentId));   // but ids kept

        int fired = 0;
        e._betweenCoverageAndLookupForTest = () =>
        {
            if (Interlocked.Increment(ref fired) > 1) return;
            e.CompactSmallSegments();
        };

        // No exception, and the trace is whole: the promise the rollback makes.
        var got = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(planted)) got.Add(s);

        _out.WriteLine($"index off, compaction mid-request: {got.Count} span(s), "
                     + $"{e.SegmentsOpenedByLastTraceLookup} opened; "
                     + $"cold segments now {e.ColdSegmentCountForTest}");
        Assert.Equal(1, e.ColdSegmentCountForTest);
        Assert.Equal(2, got.Count);
        Assert.All(got, s => Assert.Equal(planted, s.TraceId));
    }

    [Fact]
    public async Task The_recovery_pass_skips_a_replacement_the_index_has_cleared()
    {
        // THE COST OF THE REPAIR, WHERE IT IS ACTUALLY OBSERVABLE. The first version of the pass
        // read every appeared segment end to end, on the reasoning that "skipping is what got us
        // here" — which conflated skipping on a STALE snapshot (what lost the trace) with skipping
        // on an answer taken right there, from runs read right there. The second is exactly as
        // sound as the main pass's decision and rests on the same two halves.
        //
        // So: a compaction lands inside the window and the merged segment does NOT hold the trace
        // being asked for. The index can say so with one bloom probe. Reading it anyway would mean
        // walking every block of the file to learn what was already known.
        string dir = Dir("cleared");
        var    planted = Id(777_222);            // lives in NEITHER compacted segment

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        // The segment that holds the trace, published first and left out of the compaction by
        // being far enough away in time that the 24-hour batch window excludes it.
        Write(e, planted, 20_001, _baseNano);
        e.FlushHotTier();

        // Two small segments that WILL merge, holding nothing we ask for.
        long far = _baseNano + 72L * 3600 * 1_000_000_000L;
        for (int t = 0; t < 300; t++) Write(e, Id(t), (ulong)(t + 1), far + t * Ms);
        e.FlushHotTier();
        for (int t = 300; t < 600; t++) Write(e, Id(t), (ulong)(t + 1), far + t * Ms);
        e.FlushHotTier();
        Assert.Equal(3, e.ColdSegmentCountForTest);

        int fired = 0;
        e._betweenCoverageAndLookupForTest = () =>
        {
            if (Interlocked.Increment(ref fired) > 1) return;
            e.CompactSmallSegments();
        };

        var got = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(planted)) got.Add(s);

        _out.WriteLine($"replacement cleared by the index: {got.Count} span(s), "
                     + $"{e.SegmentsOpenedByLastTraceLookup} opened, "
                     + $"{e.SegmentsSkippedByLastTraceLookup} skipped; "
                     + $"cold segments now {e.ColdSegmentCountForTest}");

        Assert.Equal(1, fired);
        Assert.True(e.ColdSegmentCountForTest < 3, "the compaction did not run");
        Assert.Single(got);                       // the answer is still whole

        // AND THE MERGED SEGMENT WAS NOT READ — it is the one skip in this request.
        //
        // The three opens are the main pass: the segment that holds the trace, plus the two sources
        // whose files had already gone (the attempt is counted before the read fails, which is what
        // sets the recovery pass going). The recovery pass then meets the replacement, asks the
        // index, and is told it does not hold this trace. Without the skip rule it would open and
        // walk the merged file instead: four opens and no skips.
        Assert.Equal(3, e.SegmentsOpenedByLastTraceLookup);
        Assert.Equal(1, e.SegmentsSkippedByLastTraceLookup);
    }

    [Fact]
    public async Task The_recovery_pass_stays_asleep_when_nothing_vanished()
    {
        // The gate itself. On a quiet engine the repair must not run at all — no fresh snapshot, no
        // second lookup, no extra reads. Without the `vanished > 0` gate this still ANSWERS
        // correctly, which is why the other tests cannot see it; the cost is the only witness.
        string dir = Dir("quiet");
        var    planted = Id(888_111);

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        for (int t = 0; t < 200; t++) Write(e, Id(t), (ulong)(t + 1), _baseNano + t * Ms);
        Write(e, planted, 40_001, _baseNano + 300 * Ms);
        e.FlushHotTier();
        for (int t = 200; t < 400; t++) Write(e, Id(t), (ulong)(t + 1), _baseNano + t * Ms);
        e.FlushHotTier();

        var got = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(planted)) got.Add(s);

        _out.WriteLine($"quiet engine: {got.Count} span(s), {e.SegmentsOpenedByLastTraceLookup} opened, "
                     + $"{e.SegmentsSkippedByLastTraceLookup} skipped");
        Assert.Single(got);
        Assert.Equal(1, e.SegmentsOpenedByLastTraceLookup);   // one holds it, one is skipped
        Assert.Equal(1, e.SegmentsSkippedByLastTraceLookup);
    }
}
