using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHO REMEMBERS THAT A SEGMENT IS GONE, and for how long.
///
/// <para>A cold segment whose file has vanished is dropped from the snapshot by the reader that
/// trips over it — otherwise every later page of every stream would fail on the same dead file.
/// The removal is what makes the fault undiscoverable afterwards: the next request walks a clean
/// list, finds every file it looks for, and reports a window it read out. Through the SSE list
/// that was measured as the SAME request twice returning <c>query-error</c> and then
/// <c>done {"complete":true}</c> over the identical 50 of 100 rows.</para>
///
/// <para>So the memory belongs to the engine. These tests pin the three decisions in it: what is
/// recorded, what is deliberately NOT (the compaction handover, which on a busy install is the
/// common case and would otherwise have a healthy server reporting truncation for ever), and what
/// takes the record away again.</para>
/// </summary>
public sealed class VanishedSegmentMemoryTests : IDisposable
{
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string             _dir = Path.Combine(Path.GetTempPath(), "ameto-vanish-" + Guid.NewGuid().ToString("N"));
    private readonly TraceStorageEngine _engine;
    private readonly long               _baseNano;

    public VanishedSegmentMemoryTests()
    {
        Directory.CreateDirectory(_dir);
        _engine   = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        _baseNano = Base.ToUnixTimeMilliseconds() * 1_000_000L;
    }

    public void Dispose()
    {
        try { _engine.Dispose(); } catch { }
        try { Directory.Delete(_dir, true); } catch { }
    }

    private const long Ms = 1_000_000L;

    private void Write(ulong id, long startNano, string service = "billing")
        => _engine.WriteSpan(new SpanIngestItem
        {
            TraceId           = new TraceId(0, id),
            SpanId            = new SpanId(id),
            ParentSpanId      = default,
            StartTimeUnixNano = startNano,
            DurationNanos     = 2 * Ms,
            Name              = "GET /orders",
            ServiceName       = service,
            Kind              = SpanKind.Server,
            Status            = SpanStatusCode.Ok,
        });

    /// <summary>Flushes the tier and returns the segment that flush created.</summary>
    private SpanSegmentInfo FlushAndTakeNewSegment()
    {
        var before = _engine.ColdSegmentsForTest.Select(s => s.FilePath).ToHashSet(StringComparer.Ordinal);
        _engine.FlushHotTier();
        var added = _engine.ColdSegmentsForTest.Where(s => !before.Contains(s.FilePath)).ToList();
        Assert.Single(added);
        return added[0];
    }

    /// <summary>
    /// Unlinks a segment the way anything outside the engine would: the <c>.trc</c> AND its
    /// companion sidecars. Deleting the <c>.trc</c> alone is not a vanished segment at all — the
    /// trace-list walk is served from <c>.tracesum</c> and would read every row out of a file the
    /// engine's own <c>DeleteSegmentFiles</c> always removes together with it.
    /// </summary>
    private static void UnlinkSegmentFiles(string trcPath)
    {
        File.Delete(trcPath);
        foreach (var ext in new[] { ".tracesum", ".stats", ".svcgraph" })
        {
            var side = Path.ChangeExtension(trcPath, ext);
            if (File.Exists(side)) File.Delete(side);
        }
    }

    private Task<TraceListPage> ListAsync(long fromNano, long toNano) =>
        _engine.GetTraceListAsync(
            DateTimeOffset.FromUnixTimeMilliseconds(fromNano / Ms),
            DateTimeOffset.FromUnixTimeMilliseconds(toNano   / Ms),
            serviceName: null, spanName: null, status: null,
            minDurationNanos: null, maxDurationNanos: null, limit: 1000);

    // ── What IS recorded ──────────────────────────────────────────────────────

    [Fact]
    public async Task A_segment_deleted_behind_the_engines_back_is_reported_on_EVERY_later_read()
    {
        for (int k = 0; k < 20; k++) Write(1_000 + (ulong)k, _baseNano + k * Ms);
        var doomed = FlushAndTakeNewSegment();

        for (int k = 0; k < 20; k++) Write(2_000 + (ulong)k, _baseNano + (200 + k) * Ms);
        FlushAndTakeNewSegment();

        // Nothing in the engine asked for this. An operator clearing space, a half-restored
        // backup, a volume that dropped writes — the spans are not anywhere.
        UnlinkSegmentFiles(doomed.FilePath);

        long from = _baseNano - 1000 * Ms, to = _baseNano + 1000 * Ms;

        var first = await ListAsync(from, to);
        Assert.True(first.Unreadable, "the walk that MET the missing file must report it");
        Assert.Equal(20, first.Rows.Count);

        // THE HEAL: the file is out of the snapshot, so nothing a later read does can rediscover
        // the fault by meeting it. This is the line that makes the rest of the test necessary.
        Assert.DoesNotContain(_engine.ColdSegmentsForTest,
            s => string.Equals(s.FilePath, doomed.FilePath, StringComparison.Ordinal));
        Assert.Equal(1, _engine.VanishedRegionCountForTest);

        // AND EVERY REQUEST AFTER IT. Same window, same rows, same verdict — three times, because
        // a memory that survived one repeat and not the next would pass a two-request test.
        for (int attempt = 2; attempt <= 4; attempt++)
        {
            var again = await ListAsync(from, to);
            Assert.Equal(20, again.Rows.Count);
            Assert.True(again.Unreadable,
                $"attempt {attempt} claimed a clean window over a segment that is gone");
        }

        // AND NOT CAPPED, which is the shape of the claim and not a detail. This walk opened every
        // file that exists and finished all of them, so there is no height a narrower page could
        // settle — the pager must not be sent down through empty bands looking for one.
        var last = await ListAsync(from, to);
        Assert.False(last.Capped);
        Assert.Equal(long.MinValue, last.ScanFloorNano);
    }

    [Fact]
    public async Task The_span_scan_reports_the_same_lost_range_as_the_trace_list()
    {
        // The two cold walks heal the snapshot from their own catch blocks, so both lost the fault
        // after one request, and a fix applied to one of them would have the filter stream and the
        // TraceQL stream disagreeing about the same dead file.
        for (int k = 0; k < 20; k++) Write(3_000 + (ulong)k, _baseNano + k * Ms);
        var doomed = FlushAndTakeNewSegment();

        UnlinkSegmentFiles(doomed.FilePath);

        long from = _baseNano - 1000 * Ms, to = _baseNano + 1000 * Ms;
        var  fromAt = DateTimeOffset.FromUnixTimeMilliseconds(from / Ms);
        var  toAt   = DateTimeOffset.FromUnixTimeMilliseconds(to   / Ms);

        var discover = new SpanScanFloor();
        await foreach (var _ in _engine.SearchSpansAsync(fromAt, toAt, scanFloor: discover)) { }
        Assert.True(discover.Unreadable);

        var later = new SpanScanFloor();
        await foreach (var _ in _engine.SearchSpansAsync(fromAt, toAt, scanFloor: later)) { }
        Assert.True(later.Unreadable, "the span scan forgot the lost range after one request");

        // The bit without a floor: nothing was abandoned part-read, so nothing names a height.
        Assert.False(later.Truncated);
    }

    [Fact]
    public async Task A_window_that_does_not_reach_the_lost_range_is_still_read_out()
    {
        // The record is per RANGE for exactly this reason. One lost file must not turn every
        // query the server is ever asked into a truncation report — that is the same lie in the
        // other direction, and it is the failure mode a global flag would have.
        for (int k = 0; k < 20; k++) Write(4_000 + (ulong)k, _baseNano + k * Ms);
        var doomed = FlushAndTakeNewSegment();

        long elsewhere = _baseNano + 86_400L * 1_000_000_000L;      // a day later
        for (int k = 0; k < 20; k++) Write(5_000 + (ulong)k, elsewhere + k * Ms);
        FlushAndTakeNewSegment();

        UnlinkSegmentFiles(doomed.FilePath);

        Assert.True((await ListAsync(_baseNano - 1000 * Ms, _baseNano + 1000 * Ms)).Unreadable);
        Assert.Equal(1, _engine.VanishedRegionCountForTest);

        var far = await ListAsync(elsewhere - 1000 * Ms, elsewhere + 1000 * Ms);
        Assert.Equal(20, far.Rows.Count);
        Assert.False(far.Unreadable, "a window nowhere near the lost segment was reported as damaged");
        Assert.False(far.Capped);
    }

    // ── What is deliberately NOT recorded ─────────────────────────────────────

    [Fact]
    public async Task A_compaction_handover_is_not_a_loss_and_is_not_remembered()
    {
        // THE CASE THAT WOULD HAVE DOMINATED IN PRODUCTION. CompactOnePass publishes its merged
        // output into the snapshot and unlinks its sources AFTERWARDS, so a reader holding a
        // snapshot from a moment earlier meets missing files on a perfectly healthy server, at
        // whatever rate it compacts — and the spans are not lost at all, they are in the
        // replacement. Recording those would have this install reporting truncation over its own
        // compaction window for ever.
        //
        // Two same-size segments inside one 24 h window is what SelectCompactionBatch takes.
        for (int k = 0; k < 100; k++) Write(6_000 + (ulong)k, _baseNano + k * Ms);
        var a = FlushAndTakeNewSegment();
        for (int k = 0; k < 100; k++) Write(7_000 + (ulong)k, _baseNano + (200 + k) * Ms);
        var b = FlushAndTakeNewSegment();

        _engine.CompactSmallSegments();

        Assert.DoesNotContain(_engine.ColdSegmentsForTest, s => ReferenceEquals(s, a));
        Assert.DoesNotContain(_engine.ColdSegmentsForTest, s => ReferenceEquals(s, b));
        Assert.False(File.Exists(a.FilePath), "the compaction under test must have unlinked its sources");
        Assert.Single(_engine.ColdSegmentsForTest);

        // The race itself, from the reader's side: a scan holding the pre-swap snapshot opens
        // each source and finds no file. This is the ONLY ordering a reader can observe, because
        // the swap always precedes the delete — waiting for it to happen by luck would be a test
        // that usually does not run.
        _engine.MeetMissingSegmentFileForTest(a);
        _engine.MeetMissingSegmentFileForTest(b);

        Assert.Equal(0, _engine.VanishedRegionCountForTest);

        var page = await ListAsync(_baseNano - 1000 * Ms, _baseNano + 1000 * Ms);
        Assert.Equal(200, page.Rows.Count);                 // every row, out of the replacement
        Assert.False(page.Unreadable, "a compaction handover was recorded as data loss");
        Assert.False(page.Capped);
    }

    [Fact]
    public async Task Retention_forgets_a_lost_range_along_with_the_data_it_described()
    {
        // The bound that stops the record being both a leak and a server that never shuts up. A
        // hole in a window whose spans have all expired is not a hole anyone can look through,
        // and explaining a lost file to a user asking about data the operator told it to throw
        // away is describing the wrong event.
        for (int k = 0; k < 20; k++) Write(8_000 + (ulong)k, _baseNano + k * Ms);
        var doomed = FlushAndTakeNewSegment();

        UnlinkSegmentFiles(doomed.FilePath);

        long from = _baseNano - 1000 * Ms, to = _baseNano + 1000 * Ms;
        Assert.True((await ListAsync(from, to)).Unreadable);
        Assert.Equal(1, _engine.VanishedRegionCountForTest);

        // A TTL the fixture's timestamps are already past.
        await _engine.PruneAsync(TimeSpan.FromMinutes(1));

        Assert.Equal(0, _engine.VanishedRegionCountForTest);
        Assert.False((await ListAsync(from, to)).Unreadable);
    }
}

/// <summary>
/// The two bounds on the fault record, at the level they are implemented: it must not grow without
/// limit, and it must never lose coverage while staying inside that limit. Over-reporting is the
/// only error coalescing is allowed to introduce — the silent under-report is the bug the whole
/// mechanism exists to prevent.
/// </summary>
public sealed class VanishedRegionLogTests
{
    private const long Gb = 1_000_000_000L;

    [Fact]
    public void Overlapping_records_merge_instead_of_accumulating()
    {
        var log = new VanishedRegionLog();

        log.Record(100, 200);
        log.Record(150, 300);          // overlaps the first
        log.Record(301, 400);          // adjacent but disjoint — a separate hole
        Assert.Equal(2, log.CountForTest);

        log.Record(250, 350);          // bridges the two
        Assert.Equal(1, log.CountForTest);

        Assert.True(log.Overlaps(100, 100));
        Assert.True(log.Overlaps(400, 400));
        Assert.False(log.Overlaps(401, 500));
        Assert.False(log.Overlaps(0, 99));
    }

    [Fact]
    public void The_window_test_is_an_overlap_not_a_containment()
    {
        var log = new VanishedRegionLog();
        log.Record(1_000, 2_000);

        Assert.True(log.Overlaps(0, 1_000));            // touches the bottom edge
        Assert.True(log.Overlaps(2_000, 9_000));        // touches the top edge
        Assert.True(log.Overlaps(1_400, 1_600));        // entirely inside it
        Assert.True(log.Overlaps(0, 9_000));            // entirely contains it
        Assert.False(log.Overlaps(0, 999));
        Assert.False(log.Overlaps(2_001, 9_000));
    }

    [Fact]
    public void Far_more_losses_than_the_cap_stay_bounded_AND_keep_every_range_reported()
    {
        var log = new VanishedRegionLog();

        // 400 disjoint holes, an hour apart — far past any cap, and deliberately far enough apart
        // that nothing merges on its own.
        const int Losses = 400;
        for (int i = 0; i < Losses; i++)
            log.Record(i * 3600 * Gb, i * 3600 * Gb + 60 * Gb);

        Assert.True(log.CountForTest <= 32,
            $"the record grew to {log.CountForTest} ranges — it is a leak, not a bound");

        // AND NOT ONE OF THEM WAS DROPPED. Coalescing widens; it never forgets. A cap implemented
        // by eviction would pass the assertion above and fail every one of these.
        for (int i = 0; i < Losses; i++)
            Assert.True(log.Overlaps(i * 3600 * Gb, i * 3600 * Gb + 60 * Gb),
                $"loss {i} was evicted rather than coalesced — a window over it now reads as clean");
    }

    [Fact]
    public void Forget_drops_only_what_lies_entirely_below_the_cutoff()
    {
        var log = new VanishedRegionLog();
        log.Record(1_000, 2_000);
        log.Record(5_000, 6_000);
        log.Record(9_000, 10_000);

        Assert.Equal(1, log.Forget(5_000));             // the first only
        Assert.Equal(2, log.CountForTest);
        Assert.False(log.Overlaps(1_000, 2_000));
        Assert.True(log.Overlaps(5_000, 6_000));

        // A range STRADDLING the cutoff is kept: part of it is still inside retention, and a
        // partial hole is a hole.
        Assert.Equal(0, log.Forget(5_500));
        Assert.True(log.Overlaps(5_000, 6_000));
    }
}
