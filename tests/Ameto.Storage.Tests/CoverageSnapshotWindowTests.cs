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
        // THE ROUND-2 PROPERTY, RE-ASSERTED. The clause added here is "covered now" as well as
        // "covered then" — so it must not be mistaken for a return to sampling coverage only
        // afterwards, which is what let the backfill flip a segment covered after the lookup and
        // skip it on a run the lookup never saw. A segment covered ONLY at the second sample is
        // still read.
        string dir = Dir("backfill");
        var    planted = Id(31_337);

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        for (int t = 0; t < 200; t++) Write(e, Id(t), (ulong)(t + 1), _baseNano + t * Ms);
        Write(e, planted, 80_001, _baseNano + 300 * Ms);
        e.FlushHotTier();

        // Take the coverage away and the runs with it — the state an install has before its
        // backfill has caught up.
        e.DisableTraceIndexForTest();
        Assert.Equal(0, e.IndexCoverage.Covered);

        int fired = 0;
        e._betweenCoverageAndLookupForTest = () =>
        {
            if (fired++ > 0) return;
            e.BackfillNextSegment();      // covers the segment AFTER the snapshot was taken
        };

        var got = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(planted)) got.Add(s);

        _out.WriteLine($"backfill inside the window: {got.Count} span(s), coverage {e.IndexCoverage}, "
                     + $"{e.SegmentsSkippedByLastTraceLookup} skipped");
        Assert.Equal(1, fired);
        Assert.Single(got);
        Assert.Equal(0, e.SegmentsSkippedByLastTraceLookup);
    }
}
