using System.Net;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Integration.Tests;

/// <summary>
/// What a paging cursor owes the list it pages, proved through the live SSE route over COLD
/// SEGMENTS — the tier where the two facts that make cursors hard actually live: segments
/// OVERLAP in time, and the walk over them is BUDGETED, so it can stop with a segment nested
/// inside an already-read one still unopened.
///
/// <para>The cursor is the OLDEST ROW THE PAGE RETURNED. That rule buys exactly two things and
/// they are the two that matter: it descends strictly, so the loop always progresses; and the
/// next page's ceiling is a row already sent, so nothing can arrive NEWER than something the
/// client has already appended (the client does not re-sort — it pushes and re-emits). What it
/// does NOT buy is coverage: when the budget stopped the walk above the cursor, the band between
/// them is read from segments the walk did open and skipped in the ones it did not.</para>
///
/// <para>That skip is the subject of half this file. It is detected — the fetch reports the
/// height above which it settled its window, and a cursor that lands BELOW that height has
/// jumped a gap — and it is REPORTED, never dressed up as an exhausted window. Closing it
/// belongs in the provider (a budget one segment cannot monopolise, plus range pushdown into the
/// .tracesum read) and is deliberately not attempted here.</para>
///
/// <para>Its own host, deliberately: these are the only trace-stream tests that build COLD
/// SEGMENTS, and <c>FlushHotTier</c> takes the whole tier — sharing an engine with tests that
/// expect their rows in memory would silently rewrite their fixtures.</para>
/// </summary>
public sealed class TraceStreamPagingFloorTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly HttpClient         _client;
    private readonly TraceStorageEngine _traces;

    public TraceStreamPagingFloorTests(AmetoWebAppFactory factory)
    {
        _client = factory.CreateClient();
        _traces = factory.Services.GetRequiredService<TraceStorageEngine>();
    }

    private const long Ms  = 1_000_000L;
    private const long Sec = 1_000_000_000L;

    /// <summary>An arbitrary Unix-nanos anchor, exactly on a millisecond boundary.</summary>
    private const long Anchor = 1_700_000_000_000_000_000L;

    private void WriteRootSpan(ulong id, long startNano, string service, SpanStatusCode status)
        => _traces.WriteSpan(new SpanIngestItem
        {
            TraceId           = new TraceId(0, id),
            SpanId            = new SpanId(id),
            ParentSpanId      = default,          // root: no parent
            StartTimeUnixNano = startNano,
            DurationNanos     = 2 * Ms,
            Name              = "GET /orders",
            ServiceName       = service,
            Kind              = SpanKind.Server,
            Status            = status,
        });

    private static string Window(long fromNano, long toNano)
    {
        var from = DateTimeOffset.FromUnixTimeMilliseconds(fromNano / Ms);
        var to   = DateTimeOffset.FromUnixTimeMilliseconds(toNano   / Ms);
        return $"from={Uri.EscapeDataString(from.ToString("O"))}&to={Uri.EscapeDataString(to.ToString("O"))}";
    }

    private static List<long> StartsOf(TestHelpers.SseCapture c)
        => c.Rows.Select(r => r.GetProperty("startTimeUnixNano").GetInt64()).ToList();

    /// <summary>
    /// The ordering guarantee the cursor rule exists to provide, and the one the client cannot
    /// provide for itself: it appends each frame and re-emits the accumulator, so a row that
    /// arrives newer than one already appended renders ABOVE it in a list the UI labels
    /// newest-first. Asserted across the WHOLE stream, page boundaries included — inside one
    /// page the provider's own sort makes it true for free, which is why a per-page check would
    /// prove nothing.
    ///
    /// <para>NON-INCREASING, not strictly decreasing, and the difference is not pedantry. TWO
    /// DISTINCT TRACES CAN START ON THE SAME NANOSECOND — every producer emitting at millisecond
    /// or microsecond resolution does it routinely, and the ingest path neither rejects nor
    /// perturbs it. A 40-shape property fuzz hit exact ties in two shapes where the SERVER'S
    /// ordering was correct and this ASSERTION was what failed: it would have reported a
    /// newest-first violation over a stream that had not committed one, and the natural next move
    /// — "fix" the ordering — is a change to code that was right. Non-increasing is the invariant
    /// the product actually owes its client, because a tie renders correctly in either order.</para>
    /// </summary>
    private static void AssertNewestFirstAcrossPages(List<long> starts, string what)
    {
        for (int i = 1; i < starts.Count; i++)
            Assert.True(starts[i] <= starts[i - 1],
                $"{what}: row {i} starts at {starts[i]}, AFTER the {starts[i - 1]} that preceded it — " +
                "the stream left newest-first order across a page boundary and the client does not re-sort");
    }

    /// <summary>
    /// <c>done</c> is a POSITIVE claim that the window was read out, so it may only be made when
    /// every match actually arrived. Anything less has to say it was truncated — and it has to
    /// say so in a way a reader can tell from an ordinary ending, which is why the terminal EVENT
    /// is asserted and not merely the word.
    /// </summary>
    private static void AssertNoFalseCompleteness(
        TestHelpers.SseCapture c, int delivered, int expected, string what)
    {
        if (delivered >= expected) return;

        Assert.Equal("query-error", c.Terminal);
        Assert.Contains("truncated", c.Error ?? "", StringComparison.OrdinalIgnoreCase);
        Assert.True(c.Terminal != "done",
            $"{what}: {delivered} of {expected} matches arrived and the stream still ended with " +
            $"`done` {c.TerminalPayload?.ToString() ?? "{}"}");
    }

    // ── 1. A narrower segment nested inside a wider one ────────────────────────

    [Fact]
    public async Task FilterStream_ANarrowSegmentNestedInsideAWideOne_IsNeverPagedPast()
    {
        // The wide segment is written and flushed FIRST, so it holds the whole time range; the
        // narrow one lands entirely INSIDE it. The list walk sorts by MaxStartNano descending, so
        // the wide segment is read first, its 3000 traces trip the scan cap (limit*5 = 2500) and
        // the walk breaks with the nested segment unread.
        //
        // WHAT THIS FIXTURE ACTUALLY DISCRIMINATES: the FLOOR-AS-CURSOR branch — the page that
        // comes back with no row to page from — and nothing else. Three pages are needed:
        //   page 1  the wide segment's 100 matches, cursor → the oldest of them;
        //   page 2  comes back holding only the boundary row the cursor was taken from — no NEW
        //           row, so no row to page on. THIS is where the scan floor earns its keep: the
        //           walk settled everything above the nested segment's ceiling, so the ceiling is
        //           a cursor, and it is the only one this page has;
        //   page 3  the window has narrowed enough that the wide segment no longer fills the
        //           budget on its own, the nested segment is finally opened, and its five rows
        //           arrive — below every row already sent, so the list stays ordered.
        //
        // Before the fix this ran out of cursor on page 2 and stopped at 100 of 105 while
        // reporting a "timestamp collision" — a message about milliseconds for a failure that had
        // nothing to do with them.
        //
        // WHAT IT DOES NOT DISCRIMINATE, said plainly because the comment here used to claim it
        // did: the rule that the cursor is the oldest RETURNED ROW rather than the floor. Run
        // with the floor as the cursor throughout, this fixture still passes — 105 rows, no
        // inversions — because its floor sits BELOW its rows, so paging on either lands in the
        // same place. The fixtures that really pin the cursor rule are
        // FilterStream_ALateHotMatch_PagesPastColdSegmentsAndSaysSo (where the floor sits an hour
        // ABOVE the oldest returned row, so paging on it emits a hundred newer rows behind one
        // already sent) and FilterStream_WhenTheUnreadSegmentReachesTheWindowCeiling_StillPages
        // (where the floor is clamped onto the cursor and paging on it never moves at all).
        const string Service = "floor-nested";
        const int    Wide    = 3000;      // > the 2500 scan cap
        const int    WideHit = 100;       // the NEWEST hundred of them match
        const int    Nested  = 5;

        long baseNano = Anchor;

        // Drain whatever the sibling test left in the tier FIRST. A flush takes the whole hot
        // tier, so without this the "wide" segment's extent depends on which test xUnit ran
        // first — and the fixture is entirely about which segment is wider than which.
        _traces.FlushHotTier();

        for (int k = 0; k < Wide; k++)
            WriteRootSpan(20_000_000 + (ulong)k, baseNano + k * Ms, Service,
                          k >= Wide - WideHit ? SpanStatusCode.Error : SpanStatusCode.Ok);
        _traces.FlushHotTier();                                    // → the WIDE segment

        long nestedBase = baseNano + 1500 * Ms + 500_000L;         // inside the wide range
        for (int k = 0; k < Nested; k++)
            WriteRootSpan(20_900_000 + (ulong)k, nestedBase + k * Ms, Service, SpanStatusCode.Error);
        _traces.FlushHotTier();                                    // → the NESTED segment

        Assert.True(_traces.ColdSegmentCountForTest >= 2,
            "the fixture needs two cold segments — one wide, one nested inside it");

        var capture = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/stream?service={Service}&status=Error&max=1000" +
            $"&{Window(baseNano - 1000 * Ms, baseNano + (Wide + 1000) * Ms)}",
            timeout: TimeSpan.FromSeconds(60));

        Assert.Equal(HttpStatusCode.OK, capture.Status);
        Assert.Equal(1, capture.TerminalCount);

        var starts = StartsOf(capture);
        Assert.Equal(starts.Count, starts.Distinct().Count());

        // EVERY match, named. The old assertion accepted any early stop that used the word
        // "truncated" — which every non-`done` ending does — so a run that lost the nested
        // segment's five rows passed it.
        Assert.Equal(WideHit + Nested, starts.Count);
        for (int k = 0; k < Nested; k++)
            Assert.Contains(nestedBase + k * Ms, starts);
        for (int k = Wide - WideHit; k < Wide; k++)
            Assert.Contains(baseNano + k * Ms, starts);

        AssertNewestFirstAcrossPages(starts, "nested segment");
        AssertNoFalseCompleteness(capture, starts.Count, WideHit + Nested, "nested segment");

        // And the positive claim is now EARNED: the walk did reach the nested segment, so the
        // window really was read out.
        Assert.Equal("done", capture.Terminal);
        Assert.True(capture.TerminalPayload!.Value.GetProperty("complete").GetBoolean());
    }

    // ── 2. A late-arriving MATCH in the hot tier, below the cold walk's floor ──

    [Fact]
    public async Task FilterStream_ALateHotMatch_PagesPastColdSegmentsAndSaysSo()
    {
        // THE COST OF THE CURSOR RULE, WRITTEN DOWN. The hot tier is merged over the whole window
        // and is never subject to the cold walk's budget, so a span that arrives late — a
        // backfill, a batch exporter, clock skew, a WAL replay after a restart — comes back
        // however deep it sits. Here it is the ONLY match on page 1, so the oldest returned row
        // is an hour below every cold segment, and the next page's ceiling goes with it:
        // `MinStartNano > toNano` then skips the segment holding the other hundred matches on
        // this page and on every later one.
        //
        // The alternative was to page on the FLOOR — the height the walk settled — which reaches
        // that segment but emits its hundred rows AFTER the older row already sent, i.e. newer
        // rows arriving below older ones in a list the UI calls newest-first and the client never
        // re-sorts. Order is the property that was chosen; coverage is the property that was
        // given up, and the whole point of this test is that giving it up is SAID OUT LOUD.
        //
        // MEASURED, on this fixture, so the size of the trade is on the record: the floor cursor
        // delivered 101 of 101 rows and put 100 of them in the wrong order behind the 101st; the
        // row cursor delivers 1 of 101 in the right order and says the results are truncated.
        // Neither is acceptable and only one of them LIES, which is why this is the one that
        // ships.
        //
        // Closing it for real means a scan budget one segment cannot monopolise plus range
        // pushdown into ReadSummaries — provider work, deliberately not attempted here. When it
        // lands, this test should start delivering all 101 and the completeness assertion below
        // becomes the thing that has to change.
        const string Service   = "floor-latehot";
        const int    ColdHits  = 100;     // in a segment the cursor jumps clean past
        const int    ColdNoise = 3_000;   // > the 2500 scan cap, in a NEWER segment

        long now = Anchor + 100_000 * Sec;          // far from the fixture above

        // A flush takes the WHOLE tier, so drain whatever the sibling test left before building.
        _traces.FlushHotTier();

        // Oldest segment: a hundred matches.
        long coldBase = now - 1800 * Sec;
        for (int k = 0; k < ColdHits; k++)
            WriteRootSpan(21_000_000 + (ulong)k, coldBase + k * Ms, Service, SpanStatusCode.Error);
        _traces.FlushHotTier();

        // Newer segment: enough non-matching traces that the walk's budget is gone before it.
        long noiseBase = now - 600 * Sec;
        for (int k = 0; k < ColdNoise; k++)
            WriteRootSpan(22_000_000 + (ulong)k, noiseBase + k * Ms, Service, SpanStatusCode.Ok);
        _traces.FlushHotTier();

        Assert.True(_traces.ColdSegmentCountForTest >= 2,
            "the fixture needs the matches in an older segment than the noise that hides them");

        // The late arrival: a MATCH, an hour back, still in the hot tier and so merged whatever
        // the cold walk did.
        long lateNano = now - 3600 * Sec;
        WriteRootSpan(22_999_999, lateNano, Service, SpanStatusCode.Error);

        var capture = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/stream?service={Service}&status=Error&max=1000" +
            $"&{Window(now - 7200 * Sec, now + 10 * Sec)}",
            timeout: TimeSpan.FromSeconds(60));

        Assert.Equal(HttpStatusCode.OK, capture.Status);
        Assert.Equal(1, capture.TerminalCount);

        var starts = StartsOf(capture);
        Assert.Equal(starts.Count, starts.Distinct().Count());
        Assert.Contains(lateNano, starts);

        // THE GUARANTEE THAT SURVIVED: whatever arrived, it arrived in order. Paging on the floor
        // instead put a hundred rows an hour NEWER than the one already sent behind it.
        AssertNewestFirstAcrossPages(starts, "late hot match");

        // THE GUARANTEE THAT REPLACED COVERAGE: the stream knows it jumped a gap and refuses to
        // call the window exhausted. This is the assertion that used to read `done`/complete —
        // which was true of the floor cursor and is a lie under this one.
        AssertNoFalseCompleteness(capture, starts.Count, ColdHits + 1, "late hot match");
    }

    // ── 3. An unread segment whose ceiling is the window's own ─────────────────

    [Fact]
    public async Task FilterStream_WhenTheUnreadSegmentReachesTheWindowCeiling_StillPages()
    {
        // The floor is clamped to the window ceiling — nothing above `to` is at stake — and `to`
        // is already millisecond-aligned, so rounding it up returns it unchanged. A cursor taken
        // from that floor is therefore the cursor it already was: the FIRST page reported a
        // "timestamp collision" and stopped, over a window whose floor had simply been pinned to
        // its own ceiling, with a message telling the user to narrow a window that was not the
        // problem.
        //
        // Two segments both reaching to the ceiling is all it takes, and the first one alone
        // exhausts the budget: `visitedAny` guarantees only that ONE segment is opened, and the
        // break then lands on a second whose MaxStartNano is at the ceiling too.
        const string Service = "floor-ceiling";
        const int    Wide    = 3001;      // k = 0..3000, so MaxStartNano = base + 3000 ms
        const int    Peer    = 5;         // ends one millisecond lower, at the window ceiling

        long baseNano = Anchor + 200_000 * Sec;
        long ceiling  = baseNano + 2999 * Ms;

        _traces.FlushHotTier();

        for (int k = 0; k < Wide; k++)
            WriteRootSpan(23_000_000 + (ulong)k, baseNano + k * Ms, Service, SpanStatusCode.Ok);
        _traces.FlushHotTier();                       // MaxStartNano = base + 3000 ms

        for (int k = 0; k < Peer; k++)
            WriteRootSpan(23_900_000 + (ulong)k, ceiling - (Peer - 1 - k) * Ms, Service, SpanStatusCode.Ok);
        _traces.FlushHotTier();                       // MaxStartNano = base + 2999 ms == the ceiling

        Assert.True(_traces.ColdSegmentCountForTest >= 2,
            "the fixture needs a second relevant segment for the walk to break ON");

        // `to` lands EXACTLY on the peer segment's newest row, so the clamp puts the floor on the
        // cursor. max is two internal pages' worth, so a stream that pages at all reaches it.
        var capture = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/stream?service={Service}&max=1000&{Window(baseNano - 1000 * Ms, ceiling)}",
            timeout: TimeSpan.FromSeconds(60));

        Assert.Equal(HttpStatusCode.OK, capture.Status);
        Assert.Equal(1, capture.TerminalCount);

        var starts = StartsOf(capture);
        Assert.Equal(starts.Count, starts.Distinct().Count());
        AssertNewestFirstAcrossPages(starts, "ceiling-pinned floor");

        // The finding: one page and a false stall. Two pages' worth is the proof it moved.
        Assert.Equal(1000, starts.Count);
        Assert.Equal("done", capture.Terminal);
        Assert.False(capture.TerminalPayload!.Value.GetProperty("complete").GetBoolean());
        Assert.Equal("max-rows", capture.TerminalPayload!.Value.GetProperty("reason").GetString());

        // The row ceiling is not the ONLY thing that stopped this stream: a segment reaching the
        // window's own ceiling was never opened either. `done`/`max-rows` alone says nothing
        // about that, and a consumer counting its own rows against `max` would read this as the
        // ordinary, healthy ending.
        Assert.Equal("unread-segment", TruncatedBy(capture));
    }

    // ── 4. A skip over a segment that held nothing this query wanted ───────────

    [Fact]
    public async Task FilterStream_ASkipThatCostNothing_IsReportedTheSameWayAtEveryRowCeiling()
    {
        // The complaint this fixture is built from: a stream that delivers EVERY match and still
        // ends in a red banner. The wide segment holds all 100 matches; the nested segment the
        // budget never reaches holds five traces, none of them matching. Nothing is lost.
        //
        // It stays reported, and that is the decision, not an oversight. The pre-checks the walk
        // has — the segment's time range and its service list — both say that segment COULD have
        // held a matching row; only `status` rules it out, and status is not a segment-level fact
        // (a .stats sidecar carries per-service error counts, but leaning on it would answer this
        // one filter and no other). Reporting a skip that turned out to cost nothing is
        // conservative; the alternative is deciding a segment is irrelevant on evidence the walk
        // does not have.
        //
        // WHAT IS FIXED IS THE INCONSISTENCY. The identical skip used to reach the user as two
        // different endings depending only on the row ceiling: a truncation banner when the loop
        // reached the cursor test, and a bare `done {"reason":"max-rows"}` when `max` was hit on
        // the first page, before the cursor had moved at all. Both ceilings now say it.
        const string Service = "floor-noloss";
        const int    Wide    = 3000;      // > the 2500 scan cap
        const int    WideHit = 100;       // the OLDEST hundred match, so the cursor dives past the nested segment
        const int    Nested  = 5;

        long baseNano = Anchor + 600_000 * Sec;

        _traces.FlushHotTier();

        for (int k = 0; k < Wide; k++)
            WriteRootSpan(24_000_000 + (ulong)k, baseNano + k * Ms, Service,
                          k < WideHit ? SpanStatusCode.Error : SpanStatusCode.Ok);
        _traces.FlushHotTier();                                    // → the WIDE segment

        long nestedBase = baseNano + 1500 * Ms + 500_000L;
        for (int k = 0; k < Nested; k++)
            WriteRootSpan(24_900_000 + (ulong)k, nestedBase + k * Ms, Service, SpanStatusCode.Ok);
        _traces.FlushHotTier();                                    // → the NESTED segment, no matches in it

        string window = Window(baseNano - 1000 * Ms, baseNano + (Wide + 1000) * Ms);

        // (a) Room for every match: all 100 arrive, and the skip is still named.
        var roomy = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/stream?service={Service}&status=Error&max=1000&{window}",
            timeout: TimeSpan.FromSeconds(60));

        Assert.Equal(HttpStatusCode.OK, roomy.Status);
        Assert.Equal(1, roomy.TerminalCount);
        var roomyStarts = StartsOf(roomy);
        Assert.Equal(WideHit, roomyStarts.Count);
        AssertNewestFirstAcrossPages(roomyStarts, "no-loss skip");
        Assert.Equal("query-error", roomy.Terminal);
        Assert.Contains("truncated", roomy.Error ?? "", StringComparison.OrdinalIgnoreCase);

        // The advice has to fit the cause. This segment was not damaged — the walk simply had no
        // budget left to open it — and a narrower window is exactly what fixes that.
        Assert.Contains("ran out of room", roomy.Error ?? "", StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("will not bring them back", roomy.Error ?? "", StringComparison.OrdinalIgnoreCase);

        // (b) A row ceiling that bites on the FIRST page, before the cursor has moved. This used
        // to end `done {"complete":false,"reason":"max-rows"}` and say nothing at all about the
        // segment it never opened.
        var tight = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/stream?service={Service}&status=Error&max=50&{window}",
            timeout: TimeSpan.FromSeconds(60));

        Assert.Equal(HttpStatusCode.OK, tight.Status);
        Assert.Equal(1, tight.TerminalCount);
        Assert.Equal(50, StartsOf(tight).Count);
        Assert.Equal("done", tight.Terminal);
        Assert.False(tight.TerminalPayload!.Value.GetProperty("complete").GetBoolean());
        Assert.Equal("max-rows", tight.TerminalPayload!.Value.GetProperty("reason").GetString());
        Assert.Equal("unread-segment", TruncatedBy(tight));
    }

    // ── 5. The boundary millisecond the cursor rounds up through ───────────────

    [Fact]
    public async Task FilterStream_ARowInsideTheBoundaryMillisecond_NeverArrivesAfterAnOlderOne()
    {
        // THE SUB-MILLISECOND INVERSION. The endpoints take a millisecond-resolution ceiling and
        // rows carry nanoseconds, so the cursor is rounded UP — rounding DOWN would exclude a row
        // inside the boundary millisecond from this page and from every later one, which is a
        // hole no dedupe can close. Rounding up instead makes that millisecond OVERLAP, and the
        // dedupe set handles the ordinary consequence: the SAME trace arriving twice.
        //
        // It does nothing about a DIFFERENT one. A matching trace inside the overlap band that
        // the previous page did not return — because the segment holding it was budget-skipped —
        // used to be emitted on the next page, AFTER rows older than it, in a list the client
        // appends to and never re-sorts. Found by a 40-shape property fuzz and minimised to
        // exactly this: matches at half-milliseconds in a wide segment, one whole-millisecond
        // match in a nested segment the budget never reaches, and 0.5 ms of inversion at the page
        // boundary.
        //
        // Two things close it. The cursor arithmetic is now in NANOSECONDS — only the ASK is
        // rounded up — and a row that comes back above the cursor is withheld rather than emitted
        // out of order. Withholding is not free: that row is now never delivered. It sits inside
        // a band the stream already reports it jumped, which is why the ending below must not be
        // `done`.
        const string Service = "floor-halfms";
        const int    Wide    = 3000;
        const int    WideHit = 100;      // the OLDEST hundred, every one on a HALF millisecond
        const int    Nested  = 5;        // whole milliseconds 1..5 — the overlap band of the cut

        long baseNano = Anchor + 650_000 * Sec;

        _traces.FlushHotTier();

        for (int k = 0; k < Wide; k++)
            WriteRootSpan(25_000_000 + (ulong)k, baseNano + k * Ms + 500_000L, Service,
                          k < WideHit ? SpanStatusCode.Error : SpanStatusCode.Ok);
        _traces.FlushHotTier();

        for (int k = 1; k <= Nested; k++)
            WriteRootSpan(25_900_000 + (ulong)k, baseNano + k * Ms, Service, SpanStatusCode.Error);
        _traces.FlushHotTier();

        var capture = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/stream?service={Service}&status=Error&max=1200" +
            $"&{Window(baseNano - 1000 * Ms, baseNano + (Wide + 1000) * Ms)}",
            timeout: TimeSpan.FromSeconds(60));

        Assert.Equal(HttpStatusCode.OK, capture.Status);
        Assert.Equal(1, capture.TerminalCount);

        var starts = StartsOf(capture);
        Assert.Equal(starts.Count, starts.Distinct().Count());

        // THE INVARIANT. Before the fix this was 101 rows ending +0.5 ms, +1.0 ms — one row
        // 0.5 ms out of order, across the page boundary, every run.
        AssertNewestFirstAcrossPages(starts, "boundary millisecond");

        // The wide segment's matches all arrive; the nested segment's do not, because they sit
        // inside the band the cursor jumped and above the row the client has already been given.
        Assert.Equal(WideHit, starts.Count);
        Assert.Equal(baseNano + 500_000L, starts[^1]);
        for (int k = 1; k <= Nested; k++)
            Assert.DoesNotContain(baseNano + k * Ms, starts);

        // Withheld, therefore said. This is the assertion that stops "keep the order" from being
        // implemented by quietly dropping rows.
        AssertNoFalseCompleteness(capture, starts.Count, WideHit + Nested, "boundary millisecond");
    }

    // ── 6. Two traces on one nanosecond, across a page boundary ───────────────

    [Fact]
    public async Task FilterStream_TracesSharingAStartNanosecond_AllArriveAndStayInOrder()
    {
        // WHY THE ORDERING ASSERTION IS NON-INCREASING. Ties are not a corner case: a producer
        // emitting at millisecond or microsecond resolution puts two traces on one nanosecond all
        // day, and nothing in ingest rejects or perturbs it. A 40-shape property fuzz hit exact
        // ties in two shapes, where the server's ordering was right and the strictly-descending
        // ASSERTION was what failed — so this fixture exists to keep the loosened assertion
        // honest: it MUST see ties, and everything else must still hold over them.
        //
        // The shape is chosen so the page cut lands INSIDE a tie. FilterStreamPageSize is 500;
        // the newest millisecond carries THREE traces and every millisecond below it carries two,
        // so row 500 is the first half of a pair and row 501 is its twin, delivered by the NEXT
        // page, from a ceiling that is its own start nanosecond. That is the exact arrangement a
        // strict assertion calls a violation and a client renders correctly either way.
        const string Service = "seg-tied-starts";
        const int    Pairs   = 300;

        long baseNano = Anchor + 600_000 * Sec;

        _traces.FlushHotTier();

        for (int k = 0; k < Pairs; k++)
        {
            WriteRootSpan(26_000_000 + (ulong)(2 * k),     baseNano + k * Ms, Service, SpanStatusCode.Ok);
            WriteRootSpan(26_000_000 + (ulong)(2 * k + 1), baseNano + k * Ms, Service, SpanStatusCode.Ok);
        }
        // The odd one out, on the NEWEST millisecond, which is what pushes the page cut off the
        // pair boundary it would otherwise land on.
        WriteRootSpan(26_900_000, baseNano + (Pairs - 1) * Ms, Service, SpanStatusCode.Ok);
        _traces.FlushHotTier();

        const int Total = 2 * Pairs + 1;

        var capture = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/stream?service={Service}&max=1000" +
            $"&{Window(baseNano - 1000 * Ms, baseNano + 1000 * Ms)}",
            timeout: TimeSpan.FromSeconds(60));

        Assert.Equal(HttpStatusCode.OK, capture.Status);
        Assert.Equal(1, capture.TerminalCount);

        var starts = StartsOf(capture);
        var ids    = capture.Rows.Select(r => r.GetProperty("traceId").GetString()!).ToList();

        // Nothing lost and nothing repeated. A cursor that treated a tie as "already delivered"
        // would drop the twin; one that failed to move off the millisecond would repeat it.
        Assert.Equal(Total, ids.Count);
        Assert.Equal(Total, ids.Distinct().Count());

        // The fixture really does what it claims. Without this the assertion below is vacuous —
        // a stream with no ties passes a non-increasing check by being strictly decreasing.
        int ties = 0;
        for (int i = 1; i < starts.Count; i++) if (starts[i] == starts[i - 1]) ties++;
        Assert.True(ties >= Pairs - 1,
            $"only {ties} adjacent equal-start pairs arrived out of {Pairs} written — "
          + "the fixture is no longer exercising ties");

        AssertNewestFirstAcrossPages(starts, "tied start nanoseconds");

        // And the ending is the strong one: a tie is not a truncation, and the stream that used
        // to report "more traces share the oldest timestamp than one page can carry" over exactly
        // this shape was describing a page size, not a lost row.
        Assert.Equal("done", capture.Terminal);
        Assert.True(capture.TerminalPayload!.Value.GetProperty("complete").GetBoolean());
        Assert.Equal("exhausted", capture.TerminalPayload!.Value.GetProperty("reason").GetString());
    }

    /// <summary>The <c>truncatedBy</c> field of a terminal <c>done</c>, or null when absent.</summary>
    private static string? TruncatedBy(TestHelpers.SseCapture c) =>
        c.TerminalPayload is { } p && p.TryGetProperty("truncatedBy", out var t) ? t.GetString() : null;
}

/// <summary>
/// A cold segment the walk cannot read, which is not an exotic fault: <c>CompactOnePass</c>
/// publishes its merged output and THEN unlinks its sources, so any scan holding an older
/// snapshot meets deleted files BY DESIGN — and <c>SelectCompactionBatch</c> takes everything
/// under 10 000 spans, which on a quiet install is every freshly flushed segment, often the
/// newest one there is.
///
/// <para>Before this, <c>GetTraceListAsync</c> was the only cold walk in the engine with no
/// per-segment failure handling at all — <c>GetTraceAsync</c> and <c>SearchSpansAsync</c> both
/// have it — and the whole capped/floor contract the stream is built on is derived from it. One
/// buffered GET used to cross this window in milliseconds; a stream now enters the method four
/// to ten times, per connected client, restarted by the client's poll every fifteen seconds.</para>
///
/// <para>Its own host: these tests delete and corrupt files out from under the engine.</para>
/// </summary>
public sealed class TraceStreamSegmentFailureTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly HttpClient         _client;
    private readonly TraceStorageEngine _traces;

    public TraceStreamSegmentFailureTests(AmetoWebAppFactory factory)
    {
        _client = factory.CreateClient();
        _traces = factory.Services.GetRequiredService<TraceStorageEngine>();
    }

    private const long Ms  = 1_000_000L;
    private const long Sec = 1_000_000_000L;
    private const long Anchor = 1_700_000_000_000_000_000L;

    private void WriteRootSpan(ulong id, long startNano, string service)
        => _traces.WriteSpan(new SpanIngestItem
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

    private static string Window(long fromNano, long toNano)
    {
        var from = DateTimeOffset.FromUnixTimeMilliseconds(fromNano / Ms);
        var to   = DateTimeOffset.FromUnixTimeMilliseconds(toNano   / Ms);
        return $"from={Uri.EscapeDataString(from.ToString("O"))}&to={Uri.EscapeDataString(to.ToString("O"))}";
    }

    private static string Ql(string q) => $"ql={Uri.EscapeDataString(q)}";

    /// <summary>Flushes the tier and returns the path of the segment that flush created.</summary>
    private string FlushAndTakeNewSegmentPath()
    {
        var before = _traces.ColdSegmentsForTest.Select(s => s.FilePath).ToHashSet(StringComparer.Ordinal);
        _traces.FlushHotTier();
        var added = _traces.ColdSegmentsForTest.Select(s => s.FilePath)
                           .Where(p => !before.Contains(p)).ToList();
        Assert.Single(added);
        return added[0];
    }

    [Fact]
    public void ColdSegments_AreKeptNewestFirstWhereTheyAreBuilt_NotReSortedByEveryReader()
    {
        // The invariant that replaced a clone-and-sort in TWO read paths — the trace-list walk
        // and the span search — each of which ran it per page, of every stream, over every
        // segment on the box, while the SSE loop runs pages back to back.
        //
        // Asserted through a flush order that is the REVERSE of the required one, because
        // insertion order and MaxStartNano order agree by accident most of the time: a fixture
        // that appends segments in increasing time proves nothing about whether anything sorts.
        // Both read paths break silently without it — they walk BY INDEX to name the segment they
        // stopped before, and an unsorted list makes that name arbitrary.
        const string Service = "seg-order";
        long baseNano = Anchor + 500_000 * Sec;

        _traces.FlushHotTier();

        WriteRootSpan(32_000_000, baseNano + 900 * Ms, Service);   // NEWEST data, flushed FIRST
        _traces.FlushHotTier();
        WriteRootSpan(32_000_001, baseNano + 100 * Ms, Service);   // older, flushed second
        _traces.FlushHotTier();
        WriteRootSpan(32_000_002, baseNano + 500 * Ms, Service);   // in between, flushed last
        _traces.FlushHotTier();

        var segs = _traces.ColdSegmentsForTest;
        Assert.True(segs.Length >= 3, "the fixture needs three cold segments");
        for (int i = 1; i < segs.Length; i++)
            Assert.True(segs[i].MaxStartNano <= segs[i - 1].MaxStartNano,
                $"cold segment {i} ends at {segs[i].MaxStartNano}, ABOVE the {segs[i - 1].MaxStartNano} " +
                "before it — the snapshot is not newest-first and both cold walks read it as if it were");
    }

    /// <summary>
    /// A vanished segment, OLDER than the rows that survive it — and the ending it earns.
    ///
    /// <para>Two failures live in this one fixture and they were fixed in that order. The first
    /// was loud: the legacy branch of the walk called <c>SpanReader.SearchAsync</c> with no
    /// try/catch at all, so the <c>FileNotFoundException</c> escaped <c>GetTraceListAsync</c>,
    /// escaped the <c>Task.Run</c> behind it, and reached the handler's outer catch as "the trace
    /// list failed while streaming results" — a banner, a frozen list, and ZERO rows, including
    /// the rows of every segment that was perfectly readable.</para>
    ///
    /// <para>The second was silent, and it is what the terminal assertions below are for. The
    /// engine HEALS the snapshot on the page that discovers the fault — <c>RemoveColdSegment</c>
    /// drops the file so later reads do not keep failing on it — so the floor was recorded
    /// EXACTLY ONCE. Here the vanished segment is OLDER than the surviving rows, so the cursor
    /// never had to descend past that floor, the sticky skipped-region flag never fired, and page
    /// two found no segment, no fault and nothing to report: <c>done {"complete":true}</c> over
    /// 50 of 100 traces. The same fault with the segment NEWER, or with the file corrupt but
    /// still PRESENT, was reported correctly all along, which is exactly why the difference is
    /// the healing and not the fault.</para>
    ///
    /// <para>This test asserted a row count and the ABSENCE of one string, and therefore passed
    /// under every mutation the fix could have: floor deleted, TryReadSummaries' bool removed,
    /// magic check neutered. The terminal frame is the assertion that has teeth — see its sibling
    /// <see cref="FilterStream_WhenASegmentSummaryWillNotParse_RefusesToCallTheWindowExhausted"/>,
    /// which always had it.</para>
    /// </summary>
    [Fact]
    public async Task FilterStream_WhenASegmentFileVanished_KeepsStreamingInsteadOfFailingTheRequest()
    {
        const string Service = "seg-vanished";
        const int    Newer   = 50;

        long baseNano = Anchor + 300_000 * Sec;

        _traces.FlushHotTier();

        for (int k = 0; k < Newer; k++)                      // the OLDER segment: doomed
            WriteRootSpan(30_000_000 + (ulong)k, baseNano + k * Ms, Service);
        string doomed = FlushAndTakeNewSegmentPath();

        for (int k = 0; k < Newer; k++)                      // the NEWER segment: readable
            WriteRootSpan(30_100_000 + (ulong)k, baseNano + (200 + k) * Ms, Service);
        FlushAndTakeNewSegmentPath();

        // Unlink it the way compaction does — the engine's snapshot still names it.
        File.Delete(doomed);
        var sidecar = Path.ChangeExtension(doomed, ".tracesum");
        if (File.Exists(sidecar)) File.Delete(sidecar);

        var capture = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/stream?service={Service}&max=1000&{Window(baseNano - 1000 * Ms, baseNano + 1000 * Ms)}",
            timeout: TimeSpan.FromSeconds(60));

        Assert.Equal(HttpStatusCode.OK, capture.Status);
        Assert.Equal(1, capture.TerminalCount);
        Assert.DoesNotContain("failed while streaming", capture.Error ?? "",
                              StringComparison.OrdinalIgnoreCase);

        // The readable segment's rows are the whole point: a per-segment fault must cost that
        // segment and nothing else.
        var starts = capture.Rows.Select(r => r.GetProperty("startTimeUnixNano").GetInt64()).ToList();
        Assert.Equal(Newer, starts.Count);
        for (int k = 0; k < Newer; k++)
            Assert.Contains(baseNano + (200 + k) * Ms, starts);

        // THE HEAL HAPPENED — which is what makes the rest of this test necessary rather than
        // hypothetical. After the stream, the snapshot no longer names the deleted file, so no
        // later request can rediscover the fault by meeting it again.
        Assert.DoesNotContain(_traces.ColdSegmentsForTest,
            s => string.Equals(s.FilePath, doomed, StringComparison.Ordinal));

        // AND THE STREAM STILL SAID SO. Half the window fell out of it; `done` is a positive
        // claim that it did not, and no row count in this test can tell the difference.
        Assert.Equal("query-error", capture.Terminal);
        Assert.Contains("truncated", capture.Error ?? "", StringComparison.OrdinalIgnoreCase);
        Assert.Contains("could not be read", capture.Error ?? "", StringComparison.OrdinalIgnoreCase);

        // And it gives the RIGHT advice. A segment that is gone does not come back with a
        // narrower window, so telling the user to narrow one sends them round a loop.
        Assert.Contains("will not bring them back", capture.Error ?? "", StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task FilterStream_WhenTheVanishedSegmentIsTheNEWEST_ReportsTheSameEnding()
    {
        // The mirror image, and the control for the test above: with the vanished segment NEWER
        // than the rows that survive it, the cursor descends past the recorded floor on the very
        // page that recorded it, so the ORIGINAL sticky flag fires and the ending was already
        // honest. Pinning it means the fix above cannot have been a change of behaviour that
        // depends on which side of the surviving rows the fault happens to sit — both orders now
        // produce the same sentence.
        const string Service = "seg-vanished-newer";
        const int    Count   = 50;

        long baseNano = Anchor + 350_000 * Sec;

        _traces.FlushHotTier();

        for (int k = 0; k < Count; k++)                      // the OLDER segment: readable
            WriteRootSpan(33_000_000 + (ulong)k, baseNano + k * Ms, Service);
        FlushAndTakeNewSegmentPath();

        for (int k = 0; k < Count; k++)                      // the NEWER segment: doomed
            WriteRootSpan(33_100_000 + (ulong)k, baseNano + (200 + k) * Ms, Service);
        string doomed = FlushAndTakeNewSegmentPath();

        File.Delete(doomed);
        var sidecar = Path.ChangeExtension(doomed, ".tracesum");
        if (File.Exists(sidecar)) File.Delete(sidecar);

        var capture = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/stream?service={Service}&max=1000&{Window(baseNano - 1000 * Ms, baseNano + 1000 * Ms)}",
            timeout: TimeSpan.FromSeconds(60));

        Assert.Equal(HttpStatusCode.OK, capture.Status);
        Assert.Equal(1, capture.TerminalCount);

        var starts = capture.Rows.Select(r => r.GetProperty("startTimeUnixNano").GetInt64()).ToList();
        Assert.Equal(Count, starts.Count);
        for (int k = 0; k < Count; k++)
            Assert.Contains(baseNano + k * Ms, starts);

        Assert.Equal("query-error", capture.Terminal);
        Assert.Contains("truncated", capture.Error ?? "", StringComparison.OrdinalIgnoreCase);
        Assert.Contains("could not be read", capture.Error ?? "", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// THE FAULT OUTLIVES THE REQUEST THAT FOUND IT — the half of the story the sticky flag in
    /// the paging loop could not carry.
    ///
    /// <para>The loop's <c>unreadableRegion</c> is a LOCAL, so it dies with the stream, while
    /// <c>RemoveColdSegment</c> heals the segment out of the snapshot process-wide and for good.
    /// The fault was therefore recordable exactly ONCE, by whichever request happened to discover
    /// it, and the identical request behind it saw a clean snapshot: measured at 50 of 100 rows
    /// under <c>done {"complete":true}</c>.</para>
    ///
    /// <para>And the production consequence is worse than the second ending looks, because the
    /// control the truncation banner sits next to is REFRESH. That refresh is the second request:
    /// it gets the clean <c>done</c>, the client clears the error, and one click turns "half your
    /// traces are gone" into a list labelled complete, for the rest of that segment's life. The
    /// C1 fix that lets a user-initiated refresh clear the banner is the mechanism that erases
    /// this warning, which is why the memory has to live in the ENGINE and not in the loop.</para>
    ///
    /// <para>Asserted as the SAME request twice — not two different windows — because that is the
    /// shape the user produces, and because any fix scoped to one stream passes a test that only
    /// ever issues one.</para>
    /// </summary>
    [Fact]
    public async Task FilterStream_TheVanishedSegment_IsStillReportedOnTheNextIdenticalRequest()
    {
        const string Service = "seg-vanished-twice";
        const int    Count   = 50;

        long baseNano = Anchor + 320_000 * Sec;

        _traces.FlushHotTier();

        for (int k = 0; k < Count; k++)                      // the OLDER segment: doomed
            WriteRootSpan(34_000_000 + (ulong)k, baseNano + k * Ms, Service);
        string doomed = FlushAndTakeNewSegmentPath();

        for (int k = 0; k < Count; k++)                      // the NEWER segment: readable
            WriteRootSpan(34_100_000 + (ulong)k, baseNano + (200 + k) * Ms, Service);
        FlushAndTakeNewSegmentPath();

        File.Delete(doomed);
        var sidecar = Path.ChangeExtension(doomed, ".tracesum");
        if (File.Exists(sidecar)) File.Delete(sidecar);

        string url = $"/api/traces/stream?service={Service}&max=1000" +
                     $"&{Window(baseNano - 1000 * Ms, baseNano + 1000 * Ms)}";

        var first  = await TestHelpers.CaptureSseAsync(_client, url, timeout: TimeSpan.FromSeconds(60));
        var second = await TestHelpers.CaptureSseAsync(_client, url, timeout: TimeSpan.FromSeconds(60));

        // The heal is what makes the second request the interesting one: by now the snapshot no
        // longer names the file, so nothing the second stream reads can meet the fault.
        Assert.DoesNotContain(_traces.ColdSegmentsForTest,
            s => string.Equals(s.FilePath, doomed, StringComparison.Ordinal));

        foreach (var (capture, which) in new[] { (first, "first"), (second, "second") })
        {
            Assert.Equal(HttpStatusCode.OK, capture.Status);
            Assert.Equal(1, capture.TerminalCount);

            // Half the window is gone on BOTH requests — the row count is identical, which is
            // exactly why the row count cannot be the assertion.
            Assert.Equal(Count, capture.Rows.Count);

            Assert.Equal("query-error", capture.Terminal);
            Assert.Contains("truncated", capture.Error ?? "", StringComparison.OrdinalIgnoreCase);
            Assert.Contains("could not be read", capture.Error ?? "", StringComparison.OrdinalIgnoreCase);

            // The SAME sentence both times, with the same advice. A second request that ended
            // "narrow the time window" would be honest about the truncation and wrong about the
            // cure — a segment that is gone does not come back at any width.
            Assert.Contains("will not bring them back", capture.Error ?? "",
                            StringComparison.OrdinalIgnoreCase);
        }
    }

    /// <summary>
    /// The same memory, through the OTHER reader. <c>SearchSpansAsync</c> heals the snapshot from
    /// its own catch, so the TraceQL stream lost the fault after one request for exactly the same
    /// reason — and the two streams disagreeing about the same dead file is the disagreement this
    /// whole area exists to stop.
    /// </summary>
    [Fact]
    public async Task QlStream_TheVanishedSegment_IsStillReportedOnTheNextIdenticalRequest()
    {
        const string Service = "seg-vanished-ql";
        const int    Count   = 50;

        long baseNano = Anchor + 380_000 * Sec;

        _traces.FlushHotTier();

        for (int k = 0; k < Count; k++)                      // the OLDER segment: doomed
            WriteRootSpan(35_000_000 + (ulong)k, baseNano + k * Ms, Service);
        string doomed = FlushAndTakeNewSegmentPath();

        for (int k = 0; k < Count; k++)                      // the NEWER segment: readable
            WriteRootSpan(35_100_000 + (ulong)k, baseNano + (200 + k) * Ms, Service);
        FlushAndTakeNewSegmentPath();

        File.Delete(doomed);
        foreach (var ext in new[] { ".tracesum", ".stats", ".svcgraph" })
        {
            var side = Path.ChangeExtension(doomed, ext);
            if (File.Exists(side)) File.Delete(side);
        }

        string url = $"/api/traces/query/stream?{Ql($"{{ service = \"{Service}\" }}")}&max=1000" +
                     $"&{Window(baseNano - 1000 * Ms, baseNano + 1000 * Ms)}";

        var first  = await TestHelpers.CaptureSseAsync(_client, url, timeout: TimeSpan.FromSeconds(60));
        var second = await TestHelpers.CaptureSseAsync(_client, url, timeout: TimeSpan.FromSeconds(60));

        Assert.DoesNotContain(_traces.ColdSegmentsForTest,
            s => string.Equals(s.FilePath, doomed, StringComparison.Ordinal));

        foreach (var capture in new[] { first, second })
        {
            Assert.Equal(HttpStatusCode.OK, capture.Status);
            Assert.Equal(1, capture.TerminalCount);
            Assert.Equal(Count, capture.Rows.Count);
            Assert.Equal("query-error", capture.Terminal);
            Assert.Contains("could not be read", capture.Error ?? "", StringComparison.OrdinalIgnoreCase);
        }
    }

    /// <summary>
    /// AND IT MUST NOT SPREAD. The record is per time-range, so a window that does not overlap the
    /// dead segment's range is owed the strong, positive ending it always was — otherwise the fix
    /// turns one lost segment into a server that reports truncation for every query it is ever
    /// asked, which is the same lie in the other direction.
    /// </summary>
    [Fact]
    public async Task FilterStream_AWindowElsewhere_IsStillAllowedToSayItWasReadOut()
    {
        const string Service = "seg-vanished-elsewhere";
        const int    Count   = 20;

        // A window of its own, far from every deleted segment in this class.
        long baseNano = Anchor + 440_000 * Sec;

        _traces.FlushHotTier();

        for (int k = 0; k < Count; k++)
            WriteRootSpan(36_000_000 + (ulong)k, baseNano + k * Ms, Service);
        _traces.FlushHotTier();

        var capture = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/stream?service={Service}&max=1000" +
            $"&{Window(baseNano - 1000 * Ms, baseNano + 1000 * Ms)}",
            timeout: TimeSpan.FromSeconds(60));

        Assert.Equal(HttpStatusCode.OK, capture.Status);
        Assert.Equal(Count, capture.Rows.Count);
        Assert.Equal("done", capture.Terminal);
        Assert.True(capture.TerminalPayload!.Value.GetProperty("complete").GetBoolean());
        Assert.Equal("exhausted", capture.TerminalPayload!.Value.GetProperty("reason").GetString());
    }

    [Fact]
    public async Task FilterStream_WhenASegmentSummaryWillNotParse_RefusesToCallTheWindowExhausted()
    {
        // The quieter half, and the one that produced a WRONG ANSWER rather than an error.
        // TraceSummarySidecar.ReadSummaries ends in `catch { return []; }`, so a sidecar that
        // vanished between the Exists probe and the open — or that a power cut left truncated —
        // merged as an EMPTY list: the walk ran to the end, no floor was recorded, Capped was
        // false, and the stream sent `done {"complete":true}` over a window a whole segment had
        // just fallen out of. The sibling path disagreed with itself about the same two
        // conditions: SearchSpansAsync records them as truncation, so the TraceQL stream called
        // it truncation while the filter stream called it exhausted.
        const string Service = "seg-unreadable";
        const int    Count   = 40;

        long baseNano = Anchor + 400_000 * Sec;

        _traces.FlushHotTier();

        for (int k = 0; k < Count; k++)
            WriteRootSpan(31_000_000 + (ulong)k, baseNano + k * Ms, Service);
        string seg = FlushAndTakeNewSegmentPath();

        // Corrupt the sidecar, leave the .trc alone: Exists() still says yes, the read still
        // fails. Four bytes of wrong magic is exactly what a torn write leaves behind.
        var sidecar = Path.ChangeExtension(seg, ".tracesum");
        Assert.True(File.Exists(sidecar), "the flush must have written a .tracesum to corrupt");
        File.WriteAllBytes(sidecar, [0xDE, 0xAD, 0xBE, 0xEF]);

        var capture = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/stream?service={Service}&max=1000&{Window(baseNano - 1000 * Ms, baseNano + 1000 * Ms)}",
            timeout: TimeSpan.FromSeconds(60));

        Assert.Equal(HttpStatusCode.OK, capture.Status);
        Assert.Equal(1, capture.TerminalCount);
        Assert.DoesNotContain("failed while streaming", capture.Error ?? "",
                              StringComparison.OrdinalIgnoreCase);

        // Nothing could be read, so nothing is claimed. What must NEVER happen is the positive
        // one: `done` over a window the walk could not examine.
        Assert.Equal("query-error", capture.Terminal);
        Assert.Contains("truncated", capture.Error ?? "", StringComparison.OrdinalIgnoreCase);

        // And it says the RIGHT truncation. Every non-`done` ending used to share one message
        // about traces sharing a millisecond, so a segment that would not open was reported to
        // the user as a timestamp collision and the advice — narrow the window — was advice about
        // something that was not the problem.
        Assert.DoesNotContain("millisecond", capture.Error ?? "", StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("timestamp",   capture.Error ?? "", StringComparison.OrdinalIgnoreCase);
    }
}
