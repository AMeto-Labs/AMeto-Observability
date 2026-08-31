using System.Net;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Integration.Tests;

/// <summary>
/// The two shapes that prove a paging floor is sound, driven through the live SSE route.
///
/// <para>Both exist because a floor derived as a MINIMUM OVER ROWS is not the point down to which
/// a scan looked. Two independent mechanisms pull such a minimum arbitrarily far below anything
/// the scan reached, and past either of them the pager's next cursor lands UNDER data nobody read
/// — where the segment-level range check then skips it on every later page, permanently:</para>
/// <list type="number">
///   <item>cold segments OVERLAP in time and the walk visits them by MaxStartNano DESCENDING, so a
///   WIDE segment walked first can trip the scan cap while a NARROWER one nested inside its range
///   is still unread;</item>
///   <item>the hot tier is merged UNCONDITIONALLY over the whole window and is never subject to the
///   scan cap, so ONE late-arriving span — a backfill, a batch exporter, clock skew, a WAL replay
///   after a restart — drags the reported minimum an hour below the newest thing anyone looked at.</item>
/// </list>
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
    /// The invariant both fixtures share: <c>done</c> is a POSITIVE claim that the window was read
    /// out, so it may only be made when every match actually arrived. Anything less has to say it
    /// was truncated.
    /// </summary>
    private static void AssertNoFalseCompleteness(
        TestHelpers.SseCapture c, int delivered, int expected, string what)
    {
        if (delivered >= expected) return;

        Assert.False(c.Terminal == "done",
            $"{what}: {delivered} of {expected} matches arrived and the stream still ended with " +
            $"`done` {c.TerminalPayload?.ToString() ?? "{}"} — a positive claim that the list is " +
            "complete over rows the pager jumped straight past");
        Assert.Contains("truncated", c.Error ?? "", StringComparison.OrdinalIgnoreCase);
    }

    // ── 1. A narrower segment nested inside a wider one ────────────────────────

    [Fact]
    public async Task FilterStream_ANarrowSegmentNestedInsideAWideOne_IsNeverPagedPast()
    {
        // The wide segment is written and flushed FIRST, so it holds the whole time range; the
        // narrow one lands entirely INSIDE it. The list walk sorts by MaxStartNano descending, so
        // the wide segment is read first, its 3000 traces trip the scan cap (limit*5 = 2500) and
        // the walk breaks with the nested segment unread — while the wide segment's own oldest
        // merged row sits a full three seconds BELOW anything the nested segment holds.
        //
        // A floor taken as the minimum over merged rows therefore reports the wide segment's own
        // floor, the next cursor lands under the nested segment, and `seg.MinStartNano > toNano`
        // then skips it on this and every later page. Its five matches are unreachable for ever,
        // and the stream says `done`.
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

        // The wide segment's own matches are the easy half and must all be there.
        Assert.True(starts.Count >= WideHit,
            $"only {starts.Count} of the wide segment's {WideHit} matches arrived");

        // The nested segment's five are the finding. Either they arrive, or the terminal says the
        // results are truncated — what it must never do is claim the window was read out.
        AssertNoFalseCompleteness(capture, starts.Count, WideHit + Nested, "nested segment");
    }

    // ── 2. A late-arriving MATCH in the hot tier, below the cold walk's floor ──

    [Fact]
    public async Task FilterStream_ALateHotMatch_DoesNotPageThePagerPastAColdSegment()
    {
        // The discriminating shape, and it took a rewrite to find. A floor taken as
        // `Math.Min(scanFloor, oldest RETURNED row)` is harmless while every returned row sits
        // ABOVE the floor — which is the ordinary case, and why an earlier version of this test
        // passed against the very bug it was written for. The minimum only bites when a row is
        // returned from BELOW the floor, and exactly one thing can do that: the hot tier is
        // merged over the whole window and is never subject to the scan cap, so a span that
        // arrives late — a backfill, a batch exporter, clock skew, a WAL replay after a restart —
        // is returned however deep it sits.
        //
        // So: the matches live in the OLDEST cold segment; a newer, larger segment of non-matches
        // fills the scan budget so the walk breaks before reaching it; and one matching trace
        // arrives an hour BELOW both.
        //
        // From the floor, the next cursor is the unread segment's own ceiling and page two reads
        // it. From the minimum, the cursor jumps an hour past that segment, `MinStartNano > to`
        // then skips it on this page and on every later one, and the stream reports `done` having
        // delivered one row of a hundred and one.
        const string Service   = "floor-latehot";
        const int    ColdHits  = 100;     // the answer, in the OLDEST segment
        const int    ColdNoise = 3_000;   // > the 2500 scan cap, in a NEWER segment

        long now = Anchor + 100_000 * Sec;          // far from the fixture above

        // A flush takes the WHOLE tier, so drain whatever the sibling test left before building.
        _traces.FlushHotTier();

        // Oldest segment: the matches.
        long coldBase = now - 1800 * Sec;
        for (int k = 0; k < ColdHits; k++)
            WriteRootSpan(21_000_000 + (ulong)k, coldBase + k * Ms, Service, SpanStatusCode.Error);
        _traces.FlushHotTier();

        // Newer segment: enough non-matching traces that the walk stops before the one above.
        long noiseBase = now - 600 * Sec;
        for (int k = 0; k < ColdNoise; k++)
            WriteRootSpan(22_000_000 + (ulong)k, noiseBase + k * Ms, Service, SpanStatusCode.Ok);
        _traces.FlushHotTier();

        Assert.True(_traces.ColdSegmentCountForTest >= 2,
            "the fixture needs the matches in an older segment than the noise that hides them");

        // The late arrival: a MATCH, an hour back, still in the hot tier and so merged whatever
        // the cold walk did. This is the row that poisons a minimum.
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

        AssertNoFalseCompleteness(capture, starts.Count, ColdHits + 1, "late hot match");

        // And the strong half: the older segment is reachable, so every match must arrive.
        Assert.Equal(ColdHits + 1, starts.Count);
        Assert.Contains(lateNano, starts);
        for (int k = 0; k < ColdHits; k++)
            Assert.Contains(coldBase + k * Ms, starts);

        Assert.Equal("done", capture.Terminal);
        Assert.NotNull(capture.TerminalPayload);
        Assert.True(capture.TerminalPayload!.Value.GetProperty("complete").GetBoolean());
    }
}
