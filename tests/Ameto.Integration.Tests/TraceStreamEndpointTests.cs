using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Core;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Integration.Tests;

/// <summary>
/// A trace stream must not go quiet while a page is being fetched.
///
/// <para>This deployment sits behind nginx, whose <c>proxy_read_timeout</c> defaults to 60 s
/// BETWEEN READS from upstream. The request this whole feature exists for — a month-wide scan
/// over cold segments — is precisely the one whose FIRST page can outlast that, and the headers
/// have already been flushed by then: nginx tears the connection down, EventSource reports an
/// anonymous failure, and the user gets a generic error with zero rows and no way to retry,
/// because the SSE ticket is single-use. <c>X-Accel-Buffering: no</c> turns off buffering; it
/// does nothing about the read timeout.</para>
/// </summary>
public sealed class TraceStreamKeepaliveTests
{
    [Fact]
    public async Task A_page_fetch_that_outlasts_the_interval_is_covered_by_keepalives()
    {
        var body = new MemoryStream();
        using var sse = new SseJsonWriter(body);

        // A page that takes ~10 intervals to come back — the shape of a cold month-wide scan,
        // at a speed a test can afford.
        var slowPage = Task.Delay(TimeSpan.FromMilliseconds(300)).ContinueWith(_ => 42);

        int result = await TraceQueryEndpointMapper.AwaitWithKeepaliveAsync(
            slowPage, sse, TimeSpan.FromMilliseconds(30), CancellationToken.None);

        Assert.Equal(42, result);                       // the page still arrives intact

        var frames = Encoding.UTF8.GetString(body.ToArray()).Split(": keepalive\n\n");
        Assert.True(frames.Length - 1 >= 2,
            $"a {300}ms fetch under a 30ms keepalive wrote {frames.Length - 1} keepalives — " +
            "the stream is silent while a page is in flight and a proxy will close it");
    }

    [Fact]
    public async Task A_page_that_returns_at_once_writes_nothing_extra()
    {
        // The keepalive is for silence, not for every page: frames the client has to skip past
        // on every fetch are pure noise on a fast query.
        var body = new MemoryStream();
        using var sse = new SseJsonWriter(body);

        int result = await TraceQueryEndpointMapper.AwaitWithKeepaliveAsync(
            Task.FromResult(7), sse, TimeSpan.FromMilliseconds(30), CancellationToken.None);

        Assert.Equal(7, result);
        Assert.Empty(body.ToArray());
    }
}

/// <summary>
/// The one piece of arithmetic the paging loop cannot recover from getting wrong: turning a
/// nanosecond start into the next page's millisecond boundary.
/// </summary>
public sealed class TraceStreamCursorMathTests
{
    [Fact]
    public void A_start_time_that_cannot_become_a_cursor_is_refused_not_wrapped()
    {
        // Nothing validates the nanosecond start an exporter sends. A value within a millisecond
        // of long.MaxValue — a corrupt field, or a nanosecond column fed seconds-since-epoch
        // multiplied by a billion one time too many — makes `+ 999_999` wrap NEGATIVE, and the
        // ceiling then lands three centuries before the window instead of inside it. The loop's
        // floor test sees a cursor below `from`, calls the window read out, and sends `done` over
        // a list it truncated.
        Assert.True(TraceQueryEndpointMapper.TryCeilToMillisecond(1_700_000_000_123_456_789L, out var ok));
        Assert.Equal(1_700_000_000_124L, ok.ToUnixTimeMilliseconds());   // rounded UP, as the overlap needs

        Assert.False(TraceQueryEndpointMapper.TryCeilToMillisecond(long.MaxValue, out _));
        Assert.False(TraceQueryEndpointMapper.TryCeilToMillisecond(long.MaxValue - 999_998L, out _));

        // The largest value that still rounds without wrapping is accepted, not refused wholesale:
        // a guard that swallowed ordinary far-future timestamps would end streams of its own.
        Assert.True(TraceQueryEndpointMapper.TryCeilToMillisecond(long.MaxValue - 999_999L, out _));
    }
}

/// <summary>
/// The SSE trace list: GET /api/traces/stream and GET /api/traces/query/stream.
///
/// <para>Both drive the existing BOUNDED page computation backwards through the window rather
/// than streaming a scan, so what needs proving is the loop around it — that the cursor moves,
/// that the boundary millisecond it deliberately overlaps is deduped, that it stops when told
/// to, and that it stops AT ALL when a page comes back entirely seen.</para>
///
/// <para>Every test owns a disjoint time window and its own service name: the suite shares one
/// host, so the store is shared too, and a query that leaked into a neighbour's rows would pass
/// or fail for reasons that have nothing to do with what it asserts.</para>
/// </summary>
public sealed class TraceStreamEndpointTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    private readonly HttpClient         _client;
    private readonly TraceStorageEngine _traces;

    public TraceStreamEndpointTests(AmetoWebAppFactory factory)
    {
        _factory = factory;
        _client  = factory.CreateClient();
        _traces  = factory.Services.GetRequiredService<TraceStorageEngine>();
    }

    private const long Ms = 1_000_000L;

    /// <summary>An arbitrary Unix-nanos anchor, exactly on a millisecond boundary.</summary>
    private const long Anchor = 1_700_000_000_000_000_000L;

    /// <summary>Windows 1000 s apart, so no test can see another's traces.</summary>
    private static long BaseOf(int slot) => Anchor + slot * 1_000_000_000_000L;

    // ── Fixtures ──────────────────────────────────────────────────────────────

    private void WriteRootSpan(ulong id, long startNano, string service, short httpStatus = 0)
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
            Status            = SpanStatusCode.Ok,
            HttpStatusCode    = httpStatus,
        });

    /// <summary>The window a test's fixture lives in, widened by a second on each side.</summary>
    private static string Window(long fromNano, long toNano)
    {
        var from = DateTimeOffset.FromUnixTimeMilliseconds(fromNano / Ms - 1000);
        var to   = DateTimeOffset.FromUnixTimeMilliseconds(toNano   / Ms + 1000);
        return $"from={Uri.EscapeDataString(from.ToString("O"))}&to={Uri.EscapeDataString(to.ToString("O"))}";
    }

    private static string Ql(string q) => $"ql={Uri.EscapeDataString(q)}";

    private static List<string> TraceIdsOf(TestHelpers.SseCapture c)
        => c.Rows.Select(r => r.GetProperty("traceId").GetString()!).ToList();

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task FilterStream_OverIngestedSpans_EmitsRowsThenExactlyOneDone()
    {
        const string Service = "stream-basic";
        long baseNano = BaseOf(1);
        for (ulong k = 0; k < 3; k++)
            WriteRootSpan(1_000_000 + k, baseNano + (long)k * Ms, Service);

        var capture = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/stream?service={Service}&max=100&{Window(baseNano, baseNano + 3 * Ms)}");

        Assert.Equal(HttpStatusCode.OK, capture.Status);
        Assert.Equal(3, capture.Rows.Count);
        Assert.Equal("done", capture.Terminal);
        Assert.Equal(1, capture.TerminalCount);

        // The wire shape the client already depends on: camelCase, and httpStatusCode PRESENT
        // and null on a row without one. Serialising with WhenWritingNull would drop the
        // property entirely and the row would arrive a field short.
        var row = capture.Rows[0];
        Assert.Equal(Service, row.GetProperty("serviceName").GetString());
        Assert.True(row.TryGetProperty("httpStatusCode", out var http),
            "httpStatusCode must be on the wire even when null");
        Assert.Equal(JsonValueKind.Null, http.ValueKind);
    }

    [Fact]
    public async Task QlStream_AcrossInternalPages_IsNewestFirstAndFreeOfDuplicates()
    {
        // 250 traces at three per millisecond, so the 200-row internal page boundary lands in
        // the MIDDLE of a millisecond group. That is the shape the cursor rule exists for: the
        // two rows older than the boundary row but inside its millisecond are reachable only
        // because the cursor rounds UP past them. Rounding down would strand them for ever —
        // every later cursor derives from what actually loaded.
        const string Service = "stream-pages";
        const int    Count   = 250;
        long baseNano = BaseOf(2);
        for (int k = 0; k < Count; k++)
        {
            long start = baseNano + (k / 3) * Ms + 100_000L + (k % 3) * 400_000L;
            WriteRootSpan(2_000_000 + (ulong)k, start, Service);
        }

        var capture = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/query/stream?{Ql($"{{ service = \"{Service}\" }}")}&max=1000" +
            $"&{Window(baseNano, baseNano + Count * Ms)}");

        Assert.Equal(HttpStatusCode.OK, capture.Status);
        Assert.Equal("done", capture.Terminal);
        Assert.Equal(1, capture.TerminalCount);

        var ids = TraceIdsOf(capture);
        Assert.Equal(Count, ids.Count);                       // nothing lost at a page boundary
        Assert.Equal(Count, ids.Distinct().Count());          // and the overlap deduped

        var starts = capture.Rows.Select(r => r.GetProperty("startTimeUnixNano").GetInt64()).ToList();
        for (int i = 1; i < starts.Count; i++)
            Assert.True(starts[i] < starts[i - 1],
                $"rows must arrive newest-first: row {i} starts at {starts[i]}, after {starts[i - 1]}");
    }

    [Fact]
    public async Task Streams_EmitExactlyMaxRows_WhenMoreTracesMatch()
    {
        const string Service = "stream-max";
        const int    Count   = 30;
        long baseNano = BaseOf(3);
        for (int k = 0; k < Count; k++)
            WriteRootSpan(3_000_000 + (ulong)k, baseNano + k * Ms, Service);

        string window = Window(baseNano, baseNano + Count * Ms);

        var filtered = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/stream?service={Service}&max=7&{window}");
        Assert.Equal(7, filtered.Rows.Count);
        Assert.Equal("done", filtered.Terminal);

        var ql = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/query/stream?{Ql($"{{ service = \"{Service}\" }}")}&max=7&{window}");
        Assert.Equal(7, ql.Rows.Count);
        Assert.Equal("done", ql.Terminal);
    }

    [Fact]
    public async Task QlStream_BrokenQuery_YieldsQueryErrorFrameOn200()
    {
        // NOT a 400. EventSource never exposes the body of a non-200, so the page would get an
        // anonymous connection failure and the banner would have nothing to show.
        var capture = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/query/stream?{Ql("{ .foo = ")}&max=10");

        Assert.Equal(HttpStatusCode.OK, capture.Status);
        Assert.Equal("query-error", capture.Terminal);
        Assert.Equal(1, capture.TerminalCount);
        Assert.Empty(capture.Rows);

        // The MESSAGE, not merely its presence. The handler's outer catch produces a
        // query-error too, so asserting "some error arrived" passes with the TraceQLException
        // catch deleted — the test would then prove only that broken input fails somehow,
        // while the whole point of catching the parse error separately is that the banner can
        // name the typo instead of saying "the search failed, see the server log".
        Assert.Contains("parse error", capture.Error ?? "", StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task FilterStream_SparseFilter_KeepsPagingPastAPageThatCameBackShort()
    {
        // THE FINDING THIS TEST EXISTS FOR: a short page is not an exhausted window.
        //
        // ?httpStatus= is not a parameter of GetTraceListAsync at all — the endpoint fetches the
        // newest min(1000, limit*3) traces and filters them afterwards. Reading a page that came
        // back short of its 500 rows as "the window is exhausted" ends the stream early and says
        // `done` — a positive claim that the list is complete.
        //
        // THE FIXTURE IS THE TEST. Matches spread evenly through the window placed the oldest
        // ROW EXACTLY ON the provider's own floor row, so every cursor rule anyone could write
        // produced the same number and the test passed against all of them — including against
        // the raise that was added with it, which was a no-op on this shape. So they are grouped
        // instead:
        //   * twelve at the NEWEST end, so the oldest returned row sits a thousand traces ABOVE
        //     the floor. A cursor raised to the floor stalls on page 2 against the boundary row
        //     and the stream stops there;
        //   * two at the OLDEST end, below the first fetch's reach and below the SECOND page's
        //     as well — page 2 comes back with no rows at all, and only the floor can move the
        //     cursor off it.
        const string Service  = "stream-sparse";
        const int    Count    = 1200;
        const int    NewestHits = 12;                     // k = 1188..1199
        long baseNano = BaseOf(6);
        bool IsHit(int k) => k >= Count - NewestHits || k == 0 || k == 100;
        for (int k = 0; k < Count; k++)
            WriteRootSpan(6_000_000 + (ulong)k, baseNano + k * Ms, Service,
                          httpStatus: IsHit(k) ? (short)500 : (short)200);

        var capture = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/stream?service={Service}&httpStatus=500&max=100" +
            $"&{Window(baseNano, baseNano + Count * Ms)}");

        Assert.Equal("done", capture.Terminal);
        Assert.Equal(1, capture.TerminalCount);

        var starts = capture.Rows.Select(r => r.GetProperty("startTimeUnixNano").GetInt64()).ToList();
        Assert.Equal(NewestHits + 2, starts.Count);        // all fourteen

        // Named individually: the twelve the cursor must not stall above, and the two the empty
        // page's floor cursor is the only way to reach.
        for (int k = Count - NewestHits; k < Count; k++)
            Assert.Contains(baseNano + k * Ms, starts);
        Assert.Contains(baseNano,             starts);
        Assert.Contains(baseNano + 100 * Ms,  starts);

        Assert.Equal(starts.Count, starts.Distinct().Count());
        for (int i = 1; i < starts.Count; i++)
            Assert.True(starts[i] < starts[i - 1], "rows must still arrive newest-first");
    }

    [Fact]
    public async Task FilterStream_PastItsDeadline_SaysTheResultsArePartial()
    {
        // The deadline is two minutes in production, which no test can afford to wait out, so it
        // is turned down to nothing here. Assembly-wide DisableTestParallelization (AssemblyInfo)
        // is what makes touching a static safe.
        const string Service = "stream-deadline";
        long baseNano = BaseOf(7);
        for (ulong k = 0; k < 3; k++)
            WriteRootSpan(7_000_000 + k, baseNano + (long)k * Ms, Service);

        var restore = TraceQueryEndpointMapper.StreamDeadline;
        TraceQueryEndpointMapper.StreamDeadline = TimeSpan.Zero;
        try
        {
            var capture = await TestHelpers.CaptureSseAsync(_client,
                $"/api/traces/stream?service={Service}&max=100&{Window(baseNano, baseNano + 3 * Ms)}");

            // Not `done`: a stream that ran out of time has not seen the end of the window, and
            // saying so is the whole difference between a partial list and a wrong one.
            Assert.Equal(HttpStatusCode.OK, capture.Status);
            Assert.Equal("query-error", capture.Terminal);
            Assert.Equal(1, capture.TerminalCount);
            Assert.Contains("partial", capture.Error ?? "", StringComparison.OrdinalIgnoreCase);

            // A sub-second deadline rendered as "0-second limit" — a sentence that reads as a
            // bug report about the server rather than as an explanation to the user.
            Assert.DoesNotContain("0-second", capture.Error ?? "", StringComparison.OrdinalIgnoreCase);
        }
        finally { TraceQueryEndpointMapper.StreamDeadline = restore; }
    }

    [Fact]
    public async Task Streams_AGenuinelyEmptyWindow_YieldsDoneWithNoRows()
    {
        // THE DISTINCTION THIS TEST NOW CARRIES. Zero rows is not by itself an ending — see
        // the two tests below, where a page comes back empty from a window full of matches and
        // the stream must keep going. What ends a stream is the fetch reporting that it ran out
        // of DATA, and this window has none: nothing was ever written into it, so the page is
        // empty AND not capped, and `done` is the honest answer.
        //
        // Asserted through the terminal payload rather than through the row count, because the
        // row count is exactly what cannot tell the two apart.
        long baseNano = BaseOf(4);   // nothing was ever written here
        string window = Window(baseNano, baseNano + 10 * Ms);

        var filtered = await TestHelpers.CaptureSseAsync(_client, $"/api/traces/stream?max=100&{window}");
        Assert.Equal(HttpStatusCode.OK, filtered.Status);
        Assert.Empty(filtered.Rows);
        Assert.Equal("done", filtered.Terminal);
        Assert.Equal(1, filtered.TerminalCount);
        AssertDone(filtered, complete: true, reason: "exhausted");

        var ql = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/query/stream?{Ql("{ }")}&max=100&{window}");
        Assert.Equal(HttpStatusCode.OK, ql.Status);
        Assert.Empty(ql.Rows);
        Assert.Equal("done", ql.Terminal);
        Assert.Equal(1, ql.TerminalCount);
        AssertDone(ql, complete: true, reason: "exhausted");
    }

    [Fact]
    public async Task FilterStream_WhenAWholeCappedPageIsFilteredAway_PagesOnToTheMatchesAtTheFloor()
    {
        // THE FINDING: an EMPTY page is not an ending either — zero is the limiting case of the
        // short page the test above it is about, and it arrives on the single most ordinary
        // reason anyone streams a month of traces: an incident three weeks back.
        //
        // 1200 traces, and the only 500s are the FIVE OLDEST. ?httpStatus= is not a parameter of
        // GetTraceListAsync at all: the endpoint asks for the newest min(1000, 500*3) traces, the
        // provider merges 1200 UNFILTERED, truncates to the newest 1000 and reports Capped — and
        // the post-filter here then matches NOTHING among them. Rows empty, Capped true.
        //
        // Ending there sent `done` with zero rows over a window that holds five matches, and the
        // page rendered "No traces found" and "0 traces" with no qualifier at all.
        const string Service = "stream-emptypage";
        const int    Count   = 1200;
        const int    Hits    = 5;                          // the OLDEST five, below the first fetch
        long baseNano = BaseOf(8);
        for (int k = 0; k < Count; k++)
            WriteRootSpan(8_000_000 + (ulong)k, baseNano + k * Ms, Service,
                          httpStatus: k < Hits ? (short)500 : (short)200);

        var capture = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/stream?service={Service}&httpStatus=500&max=100" +
            $"&{Window(baseNano, baseNano + Count * Ms)}");

        Assert.Equal("done", capture.Terminal);
        Assert.Equal(1, capture.TerminalCount);

        var starts = capture.Rows.Select(r => r.GetProperty("startTimeUnixNano").GetInt64()).ToList();
        Assert.Equal(Hits, starts.Count);
        for (int k = 0; k < Hits; k++)
            Assert.Contains(baseNano + k * Ms, starts);
        for (int i = 1; i < starts.Count; i++)
            Assert.True(starts[i] < starts[i - 1], "rows must still arrive newest-first");
    }

    [Fact]
    public async Task QlStream_WhenAnOrPredicateMatchesNothingItScanned_PagesOnToTheMatchesAtTheFloor()
    {
        // The TraceQL half of the same finding, and it needs no filter at all to reach it.
        // ExtractHints walks the AND-chain only — there is no OrPredicate case — so an OR query
        // pushes NOTHING down: the scan returns the newest limit*10 spans of the window whatever
        // the query said, and the predicate is applied to them afterwards. Here every one of
        // those newest spans belongs to the noise service, the predicate rejects all of them,
        // and the page comes back with zero groups and Capped true while the five traces the
        // query is actually about sit at the floor of the window.
        const string Hit   = "orstream-hit";
        const string Noise = "orstream-noise";
        const int    Hits  = 5;
        const int    Noisy = 2_500;                        // > the 200*10 span scan cap
        long baseNano = BaseOf(9);

        for (int k = 0; k < Hits; k++)                     // oldest
            WriteRootSpan(9_000_000 + (ulong)k, baseNano + k * Ms, Hit);
        for (int k = 0; k < Noisy; k++)                    // everything newer
            WriteRootSpan(9_100_000 + (ulong)k, baseNano + (Hits + k) * Ms, Noise);

        var capture = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/query/stream?{Ql($"{{ service = \"{Hit}\" || service = \"never-ingested\" }}")}" +
            $"&max=1000&{Window(baseNano, baseNano + (Hits + Noisy) * Ms)}",
            timeout: TimeSpan.FromSeconds(60));

        Assert.Equal("done", capture.Terminal);
        Assert.Equal(1, capture.TerminalCount);
        Assert.Equal(Hits, capture.Rows.Count);
        Assert.All(capture.Rows, r => Assert.Equal(Hit, r.GetProperty("serviceName").GetString()));
    }

    [Fact]
    public async Task FilterStream_WritesKeepalivesWhileTheFIRSTPageIsStillBeingFetched()
    {
        // THE FINDING: the keepalive could never fire on a real fetch. It was handed
        // `fetchPage(...)` as an ARGUMENT — evaluated to completion before the helper was even
        // entered — and the whole chain behind it is synchronous anyway (SpanReader has no await
        // tokens; the .tracesum path is a blocking FileStream + LZ4 + parse). `work.IsCompleted`
        // was therefore true on entry every single time and the loop body never ran once, so the
        // stream stayed silent for the entire first page. On a month-wide scan that is exactly
        // the stretch that outlives nginx's 60 s proxy_read_timeout, and the user then gets an
        // anonymous failure with zero rows that is not even retryable — the ticket is single-use.
        //
        // This asserts on the WIRE, from the route, with a fetch made slow the way the real one
        // is slow: BLOCKING, inside the storage engine, before any await. The helper-level test
        // above it feeds Task.Delay to the helper and proves nothing about either.
        const string Service = "stream-keepalive";
        long baseNano = BaseOf(10);
        for (ulong k = 0; k < 3; k++)
            WriteRootSpan(10_000_000 + k, baseNano + (long)k * Ms, Service);

        var restore = TraceQueryEndpointMapper.StreamKeepalive;
        TraceQueryEndpointMapper.StreamKeepalive = TimeSpan.FromMilliseconds(40);
        int calls = 0;
        _traces._beforeTraceListScan = _ =>
        {
            if (Interlocked.Increment(ref calls) == 1) Thread.Sleep(400);   // ~10 intervals
        };
        try
        {
            var capture = await TestHelpers.CaptureSseAsync(_client,
                $"/api/traces/stream?service={Service}&max=100&{Window(baseNano, baseNano + 3 * Ms)}");

            Assert.Equal(3, capture.Rows.Count);            // the page still arrives intact
            Assert.Equal("done", capture.Terminal);
            Assert.True(capture.KeepalivesBeforeFirstRow >= 2,
                $"a 400 ms first page under a 40 ms keepalive put {capture.KeepalivesBeforeFirstRow} " +
                "keepalives on the wire before the first row — the stream is silent while the first " +
                "page is in flight and a proxy will close it");
        }
        finally
        {
            _traces._beforeTraceListScan = null;
            TraceQueryEndpointMapper.StreamKeepalive = restore;
        }
    }

    [Fact]
    public async Task FilterStream_WhenTheDeadlineCutsAScanInFlight_KeepsTheRowsAlreadyEmitted()
    {
        // The deadline has TWO branches and only the cheap one was covered. Setting the deadline
        // to zero trips the wall-clock test at the TOP of the loop, before any fetch runs at all;
        // the branch that matters in production is the token cutting a scan that is ALREADY
        // RUNNING, halfway down a month of cold segments, with rows already on the wire. Those
        // rows must survive — a partial list is the whole point of saying "partial".
        const string Service = "stream-deadline-inflight";
        const int    Count   = 1200;                     // > one 500-row page, so page 1 is capped
        long baseNano = BaseOf(11);
        for (int k = 0; k < Count; k++)
            WriteRootSpan(11_000_000 + (ulong)k, baseNano + k * Ms, Service);

        var restore = TraceQueryEndpointMapper.StreamDeadline;
        TraceQueryEndpointMapper.StreamDeadline = TimeSpan.FromSeconds(2);
        int calls = 0;
        _traces._beforeTraceListScan = ct =>
        {
            if (Interlocked.Increment(ref calls) == 1) return;      // page 1 at full speed
            ct.WaitHandle.WaitOne(TimeSpan.FromSeconds(30));        // page 2 sits in the deadline
            ct.ThrowIfCancellationRequested();
        };
        try
        {
            var capture = await TestHelpers.CaptureSseAsync(_client,
                $"/api/traces/stream?service={Service}&max=2000&{Window(baseNano, baseNano + Count * Ms)}",
                timeout: TimeSpan.FromSeconds(60));

            Assert.Equal(HttpStatusCode.OK, capture.Status);
            Assert.Equal("query-error", capture.Terminal);
            Assert.Equal(1, capture.TerminalCount);
            Assert.Contains("partial", capture.Error ?? "", StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("0-second", capture.Error ?? "", StringComparison.OrdinalIgnoreCase);

            // Page 1's rows are on the wire and stay there. A deadline that threw them away
            // would be indistinguishable from the failure it is meant to replace.
            Assert.Equal(500, capture.Rows.Count);
            Assert.True(calls >= 2, "the deadline fired before a second page was ever attempted");
        }
        finally
        {
            _traces._beforeTraceListScan = null;
            TraceQueryEndpointMapper.StreamDeadline = restore;
        }
    }

    [Fact]
    public async Task QlStream_WhenTheStalledCursorSitsOnTheWindowFLOOR_StillReportsTheStall()
    {
        // The two endings overlap, and the order they are tested in decides which one wins.
        //
        // `next <= from` (the window is read out — `done`) and `next >= cursor` (the cursor
        // cannot move — truncated) can both be true at once, and when they are the answer must
        // be the stall: `done` is a positive claim of completeness, and here rows are demonstrably
        // unread. Both hold whenever the cursor has reached the floor, which a zero-width window
        // does on its very first page — ParseFromTo validates nothing, so `from == to` is an
        // ordinary request the endpoint has to answer honestly.
        //
        // Every start is an exact multiple of 1e6 here, which is not a contrivance: it is what
        // every exporter that emits millisecond precision and converts to nanos produces, and it
        // is what makes ceil(oldest) land exactly on `from` rather than a millisecond above it.
        const string Service = "stream-floorstall";
        const int    Count   = 250;                     // more than one 200-row internal page
        long baseNano = BaseOf(12);
        for (int k = 0; k < Count; k++)
            WriteRootSpan(12_000_000 + (ulong)k, baseNano, Service);   // all on the same exact ms

        var instant = DateTimeOffset.FromUnixTimeMilliseconds(baseNano / Ms).ToString("O");
        string window = $"from={Uri.EscapeDataString(instant)}&to={Uri.EscapeDataString(instant)}";

        var capture = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/query/stream?{Ql($"{{ service = \"{Service}\" }}")}&max=1000&{window}",
            timeout: TimeSpan.FromSeconds(20));

        Assert.Equal(HttpStatusCode.OK, capture.Status);
        Assert.Equal("query-error", capture.Terminal);
        Assert.Equal(1, capture.TerminalCount);
        Assert.Contains("truncated", capture.Error ?? "", StringComparison.OrdinalIgnoreCase);
        Assert.Equal(200, capture.Rows.Count);          // one page's worth, and it stopped
    }

    [Fact]
    public async Task Streams_DoneFrame_SaysWhetherTheWindowWasReadOutOrTheCeilingWasHit()
    {
        // `done` used to be one name for two outcomes. The Angular client compensates by counting
        // its own rows against the max it asked for; nothing else can, and a truncated list that
        // asserts it is complete is the same conflation the capped-page and stalled-cursor
        // signals exist to remove. The event NAME stays `done` either way, so a client that only
        // listens for it keeps treating both as a normal completion.
        const string Service = "stream-doneshape";
        const int    Count   = 30;
        long baseNano = BaseOf(13);
        for (int k = 0; k < Count; k++)
            WriteRootSpan(13_000_000 + (ulong)k, baseNano + k * Ms, Service);

        string window = Window(baseNano, baseNano + Count * Ms);

        var exhausted = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/stream?service={Service}&max=100&{window}");
        Assert.Equal(Count, exhausted.Rows.Count);
        Assert.Equal("done", exhausted.Terminal);
        AssertDone(exhausted, complete: true, reason: "exhausted");

        var ceiling = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/stream?service={Service}&max=7&{window}");
        Assert.Equal(7, ceiling.Rows.Count);
        Assert.Equal("done", ceiling.Terminal);          // still `done` — backward compatible
        AssertDone(ceiling, complete: false, reason: "max-rows");
    }

    private static void AssertDone(TestHelpers.SseCapture c, bool complete, string reason)
    {
        Assert.NotNull(c.TerminalPayload);
        var payload = c.TerminalPayload!.Value;
        Assert.Equal(complete, payload.GetProperty("complete").GetBoolean());
        Assert.Equal(reason,   payload.GetProperty("reason").GetString());
    }

    [Fact]
    public async Task QlStream_WhenTheCursorCannotLeaveAMillisecond_SaysTheResultsAreTruncated()
    {
        // Every trace inside ONE millisecond, more of them than fit in an internal page. The
        // cursor is the oldest row's start rounded up to its millisecond while rows carry
        // nanoseconds, so it lands back where it started: the second page is the first page
        // again, and everything below that millisecond is unreachable.
        //
        // TWO things are asserted, and the first one is why this test was written: the stream
        // TERMINATES. Without the stalled-cursor check the loop spins for ever on the same
        // request, emitting nothing and holding the connection open, and this fails by timeout.
        //
        // The second is that it terminates HONESTLY. This used to end with `done` — a positive
        // claim that the list was complete — after silently dropping the rest of that
        // millisecond and every trace older than it in the window.
        const string Service = "stream-samems";
        const int    Count   = 250;
        long baseNano = BaseOf(5);
        for (int k = 0; k < Count; k++)
            WriteRootSpan(5_000_000 + (ulong)k, baseNano + k * 1_000L, Service);

        var capture = await TestHelpers.CaptureSseAsync(_client,
            $"/api/traces/query/stream?{Ql($"{{ service = \"{Service}\" }}")}&max=1000" +
            $"&{Window(baseNano, baseNano + Ms)}",
            timeout: TimeSpan.FromSeconds(20));

        Assert.Equal("query-error", capture.Terminal);
        Assert.Equal(1, capture.TerminalCount);
        Assert.Contains("truncated", capture.Error ?? "", StringComparison.OrdinalIgnoreCase);

        // One page's worth and no more: the loop cannot reach past a cursor that a
        // millisecond-resolution bound will not let move, and it stops rather than spin.
        Assert.Equal(200, capture.Rows.Count);
        Assert.Equal(capture.Rows.Count, TraceIdsOf(capture).Distinct().Count());
    }
}
