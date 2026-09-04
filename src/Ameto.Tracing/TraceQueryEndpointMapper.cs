using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ameto.Core;
using Ameto.Tracing.Storage;
using Ameto.Tracing.TraceQL;
using HistogramBuckets = Ameto.Tracing.Storage.HistogramBuckets;

namespace Ameto.Tracing;

public static class TraceQueryEndpointMapper
{
    public static void MapTraceEndpoints(this WebApplication app)
    {
        // All trace read endpoints require the Traces view scope (admin bypasses).
        var group = app.MapGroup("").RequireAuthorization(ViewPolicies.Traces);

        // GET /api/traces/stats?from=&to=
        // Fully sidecar-based: percentiles from .stats, volume/sparkline from the .tracesum
        // volume headers. No span deserialization — sub-millisecond for any dataset/window.
        group.MapGet("/api/traces/stats", async (HttpContext ctx) =>
        {
            var statsProvider   = ctx.RequestServices.GetRequiredService<ITraceStatsProvider>();
            var summaryProvider = ctx.RequestServices.GetRequiredService<ITraceSummaryProvider>();
            var (from, to)      = ParseFromTo(ctx);

            var perService    = await statsProvider.GetAggregateStatsAsync(from, to, ctx.RequestAborted);
            var mergedBuckets = new uint[HistogramBuckets.Count];
            foreach (var svc in perService)
                for (int i = 0; i < HistogramBuckets.Count; i++)
                    mergedBuckets[i] += svc.Buckets[i];

            const int Buckets = 20;
            var volume = await summaryProvider.GetTraceVolumeAsync(from, to, Buckets, ctx.RequestAborted);

            double windowSeconds = Math.Max(1, (to - from).TotalSeconds);

            var stats = new TraceStatsDto
            {
                TotalTraces    = volume.TotalTraces,
                ErrorRate      = volume.TotalTraces > 0 ? (double)volume.ErrorTraces / volume.TotalTraces * 100.0 : 0,
                P50LatencyMs   = HistogramBuckets.Percentile(mergedBuckets, 0.50),
                P95LatencyMs   = HistogramBuckets.Percentile(mergedBuckets, 0.95),
                P99LatencyMs   = HistogramBuckets.Percentile(mergedBuckets, 0.99),
                ThroughputRps  = volume.TotalTraces / windowSeconds,
                TotalSparkline = volume.TotalSparkline,
                ErrorSparkline = volume.ErrorSparkline,
            };

            await ctx.Response.WriteAsJsonAsync(stats);
        });

        // GET /api/traces?from=&to=&service=&name=&status=&limit=&minDurationMs=&maxDurationMs=&httpStatus=
        // Served from .tracesum bodies (pre-aggregated per-trace rows) — no span deserialization.
        group.MapGet("/api/traces", async (HttpContext ctx) =>
        {
            var summaryProvider = ctx.RequestServices.GetRequiredService<ITraceSummaryProvider>();
            var (from, to)      = ParseFromTo(ctx);
            var filter          = ParseTraceFilter(ctx);
            int limit           = ParseInt(ctx.Request.Query["limit"], 200, 1, 1000);

            var page = await FetchTracePageAsync(
                summaryProvider, filter, from, to, limit, ctx.RequestAborted);

            await ctx.Response.WriteAsJsonAsync(page.Rows);
        });

        // GET /api/traces/latency?from=&to=&service=
        // Returns per-service duration histograms + p50/p95/p99/p999 from .stats sidecars.
        group.MapGet("/api/traces/latency", async (HttpContext ctx) =>
        {
            var statsProvider = ctx.RequestServices.GetRequiredService<ITraceStatsProvider>();
            var (from, to)    = ParseFromTo(ctx);
            string? service   = NullIfEmpty(ctx.Request.Query["service"]);

            var allStats = await statsProvider.GetAggregateStatsAsync(from, to, ctx.RequestAborted);

            var result = allStats
                .Where(s => service is null || s.ServiceName.Equals(service, StringComparison.OrdinalIgnoreCase))
                .Select(s =>
                {
                    var buckets = s.Buckets;
                    var bounds  = HistogramBuckets.Bounds;
                    var dto = new
                    {
                        service    = s.ServiceName,
                        spanCount  = s.SpanCount,
                        errorCount = s.ErrorCount,
                        p50Ms      = HistogramBuckets.Percentile(buckets, 0.50),
                        p95Ms      = HistogramBuckets.Percentile(buckets, 0.95),
                        p99Ms      = HistogramBuckets.Percentile(buckets, 0.99),
                        p999Ms     = HistogramBuckets.Percentile(buckets, 0.999),
                        buckets    = BuildBucketList(s.Buckets),
                    };
                    return dto;
                })
                .ToList();

            await ctx.Response.WriteAsJsonAsync(result);
        });

        // GET /api/traces/compare?a={traceId}&b={traceId}
        group.MapGet("/api/traces/compare", async (HttpContext ctx) =>
        {
            var provider = ctx.RequestServices.GetRequiredService<ITraceProvider>();
            string? aHex = ctx.Request.Query["a"];
            string? bHex = ctx.Request.Query["b"];

            if (!TraceId.TryParseHex(aHex, out var tidA) || !TraceId.TryParseHex(bHex, out var tidB))
            {
                ctx.Response.StatusCode = 400;
                await ctx.Response.WriteAsync("'a' and 'b' must be valid 32-char hex trace IDs");
                return;
            }

            var taskA = CollectSpansAsync(provider, tidA, ctx.RequestAborted);
            var taskB = CollectSpansAsync(provider, tidB, ctx.RequestAborted);
            await Task.WhenAll(taskA, taskB);

            await ctx.Response.WriteAsJsonAsync(new { traceA = taskA.Result, traceB = taskB.Result });
        });

        // GET /api/traces/service-graph?from=&to=
        group.MapGet("/api/traces/service-graph", async (HttpContext ctx) =>
        {
            var graphProvider = ctx.RequestServices.GetRequiredService<IServiceGraphProvider>();
            var (from, to)    = ParseFromTo(ctx);
            var graph = await graphProvider.GetServiceGraphAsync(from, to, ctx.RequestAborted);
            await ctx.Response.WriteAsJsonAsync(graph);
        });

        // POST /api/traces/query  — TraceQL
        // Body: { "query": "{ .http.status_code = 500 }", "from": "...", "to": "...", "limit": 100 }
        group.MapPost("/api/traces/query", async (HttpContext ctx) =>
        {
            TraceQueryRequest? req = null;
            try { req = await ctx.Request.ReadFromJsonAsync<TraceQueryRequest>(ctx.RequestAborted); }
            catch { ctx.Response.StatusCode = 400; await ctx.Response.WriteAsync("Invalid JSON"); return; }

            if (req is null || string.IsNullOrWhiteSpace(req.Query))
            {
                ctx.Response.StatusCode = 400;
                await ctx.Response.WriteAsync("'query' field is required");
                return;
            }

            SpanPredicate predicate;
            try   { predicate = TraceQLParser.Parse(req.Query); }
            catch (TraceQLException ex)
            {
                ctx.Response.StatusCode = 400;
                await ctx.Response.WriteAsync($"TraceQL parse error: {ex.Message}");
                return;
            }

            var from  = ParseDate(req.From) ?? DateTimeOffset.UtcNow.AddHours(-1);
            var to    = ParseDate(req.To)   ?? DateTimeOffset.UtcNow;
            int limit = Math.Clamp(req.Limit, 1, 1000);

            var provider = ctx.RequestServices.GetRequiredService<ITraceProvider>();
            var page     = await TraceQLExecutor.ExecuteAsync(provider, predicate, from, to, limit, ctx.RequestAborted);
            await ctx.Response.WriteAsJsonAsync(page.Rows);
        });

        // GET /api/traces/query/stream?ql=&from=&to=&max=&ticket=   — TraceQL over SSE
        //
        // GET, and the query text rides in ?ql=, because EventSource can neither POST nor
        // carry a body. Auth is the ?ticket= single-use ticket, which needs nothing here:
        // redemption is a JwtBearerEvents.OnMessageReceived hook on the bearer scheme itself
        // (Ameto.Server/Auth/AuthServiceExtensions.cs), and this group's policy includes it.
        group.MapGet("/api/traces/query/stream", async (HttpContext ctx, ILoggerFactory loggerFactory) =>
        {
            var provider   = ctx.RequestServices.GetRequiredService<ITraceProvider>();
            var (from, to) = ParseFromTo(ctx);
            int max        = ParseInt(ctx.Request.Query["max"], 2000, 1, 5000);
            string ql      = ctx.Request.Query["ql"].ToString();

            await BeginEventStreamAsync(ctx);
            using var sse = new SseJsonWriter(ctx.Response.Body);
            try
            {
                SpanPredicate predicate;
                try { predicate = TraceQLParser.Parse(ql); }
                catch (TraceQLException ex)
                {
                    // A parse error is a query-error FRAME on a 200, never a 400. EventSource
                    // never exposes the body of a non-200 response — the page would get an
                    // anonymous `error` event and the banner would have nothing to show but
                    // "connection failed", throwing away the one diagnostic that names the
                    // typo. The status line is the wrong channel for a browser that cannot
                    // read it.
                    await SafeErrorAsync(sse, $"TraceQL parse error: {ex.Message}", ctx);
                    return;
                }

                var end = await StreamTracePagesAsync(ctx, sse, max, QlStreamPageSize, from, to,
                    async (pageTo, size, ct) =>
                    {
                        var p = await TraceQLExecutor.ExecuteAsync(provider, predicate, from, pageTo, size, ct);
                        return new TracePage(p.Rows, p.ScanFloorNano, p.Unreadable);
                    });

                await FinishStreamAsync(sse, end, ctx);
            }
            catch (OperationCanceledException) { /* client disconnected */ }
            catch (Exception ex)
            {
                loggerFactory.CreateLogger(TraceStreamLogCategory)
                             .LogError(ex, "TraceQL stream failed after the response had opened");
                // A stable sentence for the client; the exception text can name segment paths
                // and internals, and it is already in the log where it belongs.
                await SafeErrorAsync(sse, "The trace search failed while streaming results. See the server log for details.", ctx);
            }
        });

        // GET /api/traces/stream?from=&to=&service=&name=&status=&minDurationMs=&maxDurationMs=&httpStatus=&max=&ticket=
        // The filter list of GET /api/traces, streamed — same filters, same rows, same order.
        group.MapGet("/api/traces/stream", async (HttpContext ctx, ILoggerFactory loggerFactory) =>
        {
            var summaryProvider = ctx.RequestServices.GetRequiredService<ITraceSummaryProvider>();
            var (from, to)      = ParseFromTo(ctx);
            var filter          = ParseTraceFilter(ctx);
            int max             = ParseInt(ctx.Request.Query["max"], 2000, 1, 5000);

            await BeginEventStreamAsync(ctx);
            using var sse = new SseJsonWriter(ctx.Response.Body);
            try
            {
                var end = await StreamTracePagesAsync(ctx, sse, max, FilterStreamPageSize, from, to,
                    (pageTo, size, ct) => FetchTracePageAsync(summaryProvider, filter, from, pageTo, size, ct));

                await FinishStreamAsync(sse, end, ctx);
            }
            catch (OperationCanceledException) { /* client disconnected */ }
            catch (Exception ex)
            {
                loggerFactory.CreateLogger(TraceStreamLogCategory)
                             .LogError(ex, "Trace list stream failed after the response had opened");
                await SafeErrorAsync(sse, "The trace list failed while streaming results. See the server log for details.", ctx);
            }
        });

        // GET /api/traces/{traceId}/flamegraph
        group.MapGet("/api/traces/{traceId}/flamegraph", async (HttpContext ctx, string traceId) =>
        {
            if (!TraceId.TryParseHex(traceId, out var tid))
            {
                ctx.Response.StatusCode = 400;
                return;
            }
            var provider = ctx.RequestServices.GetRequiredService<ITraceProvider>();
            var spans    = await CollectSpansRawAsync(provider, tid, ctx.RequestAborted);

            if (spans.Count == 0) { ctx.Response.StatusCode = 404; return; }

            var flame = BuildFlamegraph(spans);
            await ctx.Response.WriteAsJsonAsync(flame);
        });

        // GET /api/traces/{traceId}
        group.MapGet("/api/traces/{traceId}", async (HttpContext ctx, string traceId) =>
        {
            if (!TraceId.TryParseHex(traceId, out var tid))
            {
                ctx.Response.StatusCode = 400;
                return;
            }
            var provider = ctx.RequestServices.GetRequiredService<ITraceProvider>();
            var spans    = new List<SpanDto>();
            await foreach (var s in provider.GetTraceAsync(tid, ctx.RequestAborted))
                spans.Add(SpanDto.From(s));

            await ctx.Response.WriteAsJsonAsync(spans);
        });
    }

    // ── SSE streaming ─────────────────────────────────────────────────────────

    /// <summary>Rows per internal page — what the Angular client asked for per HTTP round trip.</summary>
    private const int QlStreamPageSize     = 200;
    private const int FilterStreamPageSize = 500;

    /// <summary>Log category for trace-stream failures reported to clients only in summary.</summary>
    private const string TraceStreamLogCategory = "Ameto.Tracing.Stream";

    /// <summary>
    /// Commits the response as an event stream BEFORE any framing: headers first, then an
    /// empty-body flush, so the client's EventSource opens on the status line rather than on
    /// the first row — which on a query that matches nothing would never arrive.
    /// </summary>
    private static async Task BeginEventStreamAsync(HttpContext ctx)
    {
        ctx.Response.ContentType          = "text/event-stream";
        ctx.Response.Headers.CacheControl = "no-cache";
        ctx.Response.Headers.Connection   = "keep-alive";
        // nginx buffers proxied responses by default, and this feature ships behind one — the
        // deployment serves Ameto under a /ameto prefix (see Ameto.Server/config.yml). Buffered,
        // every frame is held until the handler returns and streaming buys nothing at all.
        // NOTE: the log SSE endpoints (GET /api/events and /api/events/live in
        // Ameto.Server/EndpointMapper.cs) do NOT set this header — if the live tail is choppy
        // behind that same proxy, this is why.
        ctx.Response.Headers["X-Accel-Buffering"] = "no";
        await ctx.Response.Body.FlushAsync(ctx.RequestAborted);
    }

    /// <summary>
    /// How long one stream may run before it gives up and says so. A window can be arbitrarily
    /// wide over arbitrarily many cold segments, and neither the page loop nor the scans under
    /// it were otherwise bounded by anything but the client hanging up — a pathological request
    /// could hold a handler and a connection for as long as the process lived.
    ///
    /// <para>Deliberately NOT QueryGuard: the trace endpoints have never taken a slot from it,
    /// and giving them one would change what a trace search costs the log path. This is a wall
    /// clock and nothing else.</para>
    ///
    /// <para>Settable only so a test can reach the deadline branch without spending two minutes
    /// in it; production never assigns it.</para>
    /// </summary>
    internal static TimeSpan StreamDeadline { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// The longest the stream may stay silent. nginx's <c>proxy_read_timeout</c> defaults to
    /// 60 s BETWEEN READS from upstream, and this deployment sits behind one (see
    /// Ameto.Server/config.yml) — so the month-wide cold scan this feature exists for is exactly
    /// the request whose first page outlives the proxy's patience. The connection is then torn
    /// down, EventSource reports an anonymous failure, and the user gets nothing; it is not even
    /// retryable, because the SSE ticket is single-use. <c>X-Accel-Buffering: no</c> does not
    /// help: it turns off BUFFERING, not the read timeout.
    ///
    /// <para>Settable for the same reason as <see cref="StreamDeadline"/>: a test proving that a
    /// keepalive reaches the WIRE cannot wait fifteen seconds for one. Production never assigns
    /// it.</para>
    /// </summary>
    internal static TimeSpan StreamKeepalive { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>One bounded page of trace rows, newest-first, over <c>[from, pageTo]</c>.</summary>
    private delegate Task<TracePage> TracePageFetcher(
        DateTimeOffset pageTo, int pageSize, CancellationToken ct);

    /// <summary>
    /// A page of rows plus the two bits the paging loop cannot infer for itself: whether the
    /// fetch behind it stopped for want of ROOM or for want of DATA, and how deep into the
    /// window it actually looked.
    /// </summary>
    /// <param name="ScanFloorNano">
    /// THE HEIGHT ABOVE WHICH THIS FETCH SETTLED ITS WINDOW: every match STRICTLY ABOVE it was
    /// emitted or deliberately filtered out, and nothing at or below it was necessarily looked
    /// at. <c>long.MinValue</c> means the fetch read the window out to its floor.
    ///
    /// Floors compose by MAXIMUM, never by minimum. Each one names a height above which some
    /// part of the work is settled, so only the highest is a claim all the others sit under.
    ///
    /// IT IS NOT THE CURSOR. See <see cref="StreamTracePagesAsync"/> for what it is instead.
    /// </param>
    /// <param name="Unreadable">
    /// True when the fetch met a segment inside its window it COULD NOT READ, rather than one it
    /// had no room left to open. Carried separately from the floor because the engine heals a
    /// vanished segment out of its snapshot on the page that discovers it — see
    /// <see cref="TraceListPage.Unreadable"/>. The paging loop makes it STICKY for the life of the
    /// stream, which is the only thing that stops a later page reporting the window exhausted.
    /// </param>
    private readonly record struct TracePage(
        List<TraceRowDto> Rows, long ScanFloorNano, bool Unreadable)
    {
        /// <summary>
        /// Derived rather than passed, so the contradictory pair cannot be constructed: capped
        /// with no floor has no honest ending, and a floor with "not capped" invites a caller to
        /// page past it. One fact, one field.
        /// </summary>
        public bool Capped => ScanFloorNano != long.MinValue;
    }

    /// <summary>Why the paging loop stopped. Only the first two earn an <c>event: done</c>.</summary>
    private enum StreamEnd
    {
        /// <summary>The window was read out to its floor. The strong, positive claim.</summary>
        Complete,
        /// <summary>The caller's <c>max</c> was reached with window left — <c>done</c>, but not complete.</summary>
        MaxRows,
        /// <summary>
        /// The loop reached the bottom of the window, but a page had settled its window only
        /// down to a height ABOVE where the cursor then went — the band between was never read
        /// and never will be. Results are truncated.
        /// </summary>
        RegionSkipped,
        /// <summary>
        /// A page met a segment it COULD NOT READ. Separate from <see cref="RegionSkipped"/>
        /// because the advice is the opposite: a budget skip comes back with a narrower window,
        /// a file that will not open does not — see <see cref="FinishStreamAsync"/>.
        /// </summary>
        RegionUnreadable,
        /// <summary>The cursor could not move off a millisecond — results are truncated.</summary>
        TimestampCollision,
        /// <summary>
        /// A page produced neither a row to page from nor a floor below the cursor — a segment
        /// that would not open, or a scan that got no further than the window's own ceiling.
        /// Results are truncated, and NOT for a reason that has anything to do with milliseconds.
        /// </summary>
        NoProgress,
        /// <summary>A row's start time cannot be turned into a cursor at all — results are truncated.</summary>
        UnusableTimestamp,
        /// <summary>The wall clock ran out — results are partial.</summary>
        Deadline,
    }

    /// <summary>
    /// Streams up to <paramref name="max"/> rows by driving the existing BOUNDED page
    /// computation backwards through the window — the very loop the Angular client ran across
    /// HTTP round trips, minus the round trips.
    ///
    /// <para>Neither source can stream: TraceQLExecutor sorts globally and then truncates, and
    /// GetTraceListAsync returns a finished list. Rewriting either into a streaming group-by is
    /// not the answer — the spans of one trace are spread in time, so a row emitted before its
    /// scan window closes can be emitted incomplete.</para>
    ///
    /// <para>WHAT THE PAGING COSTS, honestly. Each page re-enters the fetch over a narrower
    /// window, and only ONE of the three costs actually falls as the cursor descends:</para>
    /// <list type="bullet">
    ///   <item>cold segments ENTIRELY newer than the cursor fail the segment-level range check
    ///   and are never opened again. This is the saving, and it is real;</item>
    ///   <item>a segment that STRADDLES the cursor is reopened, its body decompressed and its
    ///   rows re-parsed, on every page it straddles — and <c>SelectCompactionBatch</c> groups
    ///   sources within a 24-hour window, so a compacted segment can straddle a whole day of
    ///   pagination. The <c>[from, to]</c> bound now pushed into
    ///   <c>TraceSummarySidecar.TryReadSummaries</c> cuts the per-row allocation but not the
    ///   decompression: the body is one LZ4 blob with no index;</item>
    ///   <item>the HOT TIER is walked in full on every page, by both fetchers. A descending
    ///   cursor does not shorten that walk at all — it only makes more spans fail the range test
    ///   inside it, and on the list path each surviving span still costs a MergeSpanInto
    ///   (dictionary probe, HashSet add, field writes) inside the read lock.</item>
    /// </list>
    /// <para>So the cost is bounded by the number of pages, not independent of it, and the
    /// per-page scan budget (<c>max(limit*5, 500)</c> merged summaries) is a budget on the MERGE,
    /// not on the reading: <c>MaxSpansPerPass</c> lets one compacted segment hold 200 000 spans,
    /// which is tens of thousands of summary rows read to fill 2 500 slots. <c>visitedAny</c>
    /// makes reading the first relevant segment unconditional on top of that.</para>
    /// </summary>
    private static async Task<StreamOutcome> StreamTracePagesAsync(
        HttpContext       ctx,
        SseJsonWriter     sse,
        int               max,
        int               pageSize,
        DateTimeOffset    from,
        DateTimeOffset    to,
        TracePageFetcher  fetchPage)
    {
        var ct = ctx.RequestAborted;

        // The scans get their own token so the deadline can cut a page that is taking for ever,
        // while the FRAMES keep being written under the request token alone — a deadline that
        // could fire in the middle of a `data:` line would corrupt the very frame that has to
        // carry the bad news.
        using var deadline = new DeadlineScope(ct, StreamDeadline);
        var scanCt = deadline.Token;

        // NOTHING HERE MAY GROW WITH THE NUMBER OF MATCHES IN THE WINDOW. The dedupe set is
        // capped by `max` and each page by `pageSize`; a month-wide query on a busy service is
        // the shape that killed a 512 MB server when a collection was allowed to track matches
        // instead of results (see 3fc5472).
        var  seen      = new HashSet<string>(StringComparer.Ordinal);
        int  emitted   = 0;
        long startedAt = System.Diagnostics.Stopwatch.GetTimestamp();

        // THE CURSOR IS A NANOSECOND, and the millisecond it rounds up to is only how the fetch
        // is ASKED. The providers take a millisecond-resolution ceiling — GetTraceListAsync and
        // SearchSpansAsync both open with `to.ToUnixTimeMilliseconds() * 1_000_000` — and
        // rounding a nanosecond cursor DOWN to reach one opens a hole that no later page can
        // close, so the ask is rounded UP and the boundary millisecond deliberately OVERLAPS.
        //
        // Doing the ARITHMETIC in that rounded millisecond as well is what put rows out of order.
        // A row inside the overlap band that the previous page did not return — because a segment
        // was budget-skipped — arrived on the next page AFTER rows older than it, and the dedupe
        // set cannot help: it stops the SAME trace recurring, not a DIFFERENT one. Found by a
        // 40-shape property fuzz, minimised to 0.5 ms, and it is exactly the invariant
        // AssertNewestFirstAcrossPages claims. Below, `cursorNano` is the real boundary: the
        // fetch is still asked for the enclosing millisecond, and the rows that come back above
        // the cursor are the overlap — duplicates, or rows in a band this stream has already
        // reported it jumped.
        long fromNano   = from.ToUnixTimeMilliseconds() * 1_000_000L;
        long cursorNano = to.ToUnixTimeMilliseconds()   * 1_000_000L;

        // STICKY, and it is the whole honesty story of this loop. Set the moment a page's cursor
        // lands BELOW the height that page settled its window down to: the band between them was
        // examined by nobody, and every later ceiling is lower still, so nobody ever will. Once
        // set it can never be unset — a later page reading ITS window out says nothing about the
        // band an earlier one jumped, and treating it as if it did is exactly the false `done`
        // this endpoint exists to stop.
        bool skippedRegion = false;

        // STICKY FOR A SECOND, STRONGER REASON. A page that could not READ a segment does not
        // merely leave a band for a later page: the engine REMOVES a vanished segment from its
        // snapshot on the page that discovers it, so no later page can find it, fail on it, or
        // report its floor. The fault is therefore recorded exactly once, and if the cursor never
        // had to descend past that one floor the flag above never fired either — every later page
        // saw a clean window and the stream ended `done {"complete":true}` over half of it.
        // Measured through this route: two segments of 40 traces, the OLDER one unlinked, 40 rows
        // delivered and a positive claim of completeness. Note the shape of the bug — the SAME
        // fault with the segment NEWER, or with the file corrupt but still PRESENT, was reported
        // correctly, because those two keep re-arriving on later pages. This flag is what a
        // healed snapshot costs.
        //
        // THIS FLAG IS NOT THE MEMORY, and it must not be mistaken for one. It is a LOCAL: it
        // dies with the stream, while the removal it compensates for is process-wide and
        // permanent. On its own it therefore only ever moved the bug one request along — the
        // stream that discovered the fault reported it and the IDENTICAL request behind it, which
        // in the product is the refresh button next to the banner, got a clean `done`. The record
        // that survives a request lives in the engine (VanishedRegionLog), reaches this loop as
        // `page.Unreadable` like any other, and is why a page can arrive here NOT capped and still
        // carrying the bit. What this flag does is keep it once it has arrived.
        bool unreadableRegion = false;

        while (true)
        {
            ct.ThrowIfCancellationRequested();
            // On the wall clock as well as through the token: the token cuts a fetch that is
            // already running, this one catches a loop whose pages keep completing, each of
            // them cheap, long after the stream as a whole should have given up.
            if (System.Diagnostics.Stopwatch.GetElapsedTime(startedAt) >= StreamDeadline)
                return new StreamOutcome(StreamEnd.Deadline, skippedRegion, unreadableRegion);

            // The millisecond the fetch is asked for: the lowest boundary the cursor does not sit
            // above, so nothing between the cursor and it can be missed by the ask.
            if (!TryCeilToMillisecond(cursorNano, out var pageTo))
                return new StreamOutcome(StreamEnd.UnusableTimestamp, skippedRegion, unreadableRegion);

            TracePage page;
            try
            {
                // OFF THE HANDLER THREAD, DELIBERATELY. The keepalive below can only fire while
                // this await is pending, and nothing in the fetch chain ever pends: SpanReader
                // contains no await tokens at all, the .tracesum sidecar path is a blocking
                // FileStream + LZ4 + parse, and the async iterators over them hand back
                // synchronously-completed ValueTasks. Passing `fetchPage(...)` straight in as an
                // ARGUMENT was worse still — arguments are evaluated before the callee is
                // entered, so the whole scan ran to completion before the keepalive helper had
                // begun. Either way `work.IsCompleted` was true on entry, the keepalive loop
                // never ran a single iteration, and the stream was silent for the entire first
                // page — the exact stretch on a month-wide scan that outlives nginx's 60 s
                // proxy_read_timeout, which is what the keepalive was added to prevent.
                //
                // Task.Run is therefore the fix and not a workaround: the scan is blocking work,
                // and blocking work belongs on the pool while the handler stays free to write.
                var pageCursor = pageTo;
                page = await AwaitWithKeepaliveAsync(
                    Task.Run(() => fetchPage(pageCursor, pageSize, scanCt), scanCt),
                    sse, StreamKeepalive, ct);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                // Ours, not the client's.
                return new StreamOutcome(StreamEnd.Deadline, skippedRegion, unreadableRegion);
            }

            // Once true, always true: nothing later makes a file that would not open readable,
            // and the engine has already dropped a vanished one from the snapshot so no later
            // page will even try.
            unreadableRegion |= page.Unreadable;

            // Data frames are written HERE and keepalives only inside the await above, so the two
            // can never interleave: one thread of control, and the helper has returned before the
            // first `data:` byte of this page is written.
            //
            // The oldest row the page RETURNED, which is not the oldest it EMITTED: a row an
            // earlier page already sent still bounds where this fetch looked, and it is the
            // bound, not the novelty, that the cursor is made of. Taken as a minimum rather than
            // as `Rows[^1]` because "the fetch sorts newest-first" is a property of two separate
            // implementations and a cursor that silently depends on it is a cursor waiting to be
            // wrong.
            long oldestReturned = long.MaxValue;
            long oldestEmitted  = long.MaxValue;   // how deep this stream has actually delivered
            foreach (var row in page.Rows)
            {
                if (row.StartTimeUnixNano < oldestReturned) oldestReturned = row.StartTimeUnixNano;

                // THE OVERLAP BAND, and the only place rows are held back. `pageTo` is the cursor
                // rounded UP, so this page was asked for up to a millisecond MORE than the cursor
                // allows. A row in that sliver is one of two things:
                //   * one an earlier page already sent — the ordinary case, and the reason the
                //     rounding is safe at all;
                //   * one no page has sent, because the segment holding it was skipped. Emitting
                //     THAT is the sub-millisecond inversion: a row newer than one the client has
                //     already appended, in a list it renders in arrival order and never re-sorts.
                // Neither may be emitted here. The second is a row this stream will never deliver
                // — every later ceiling is lower — so it is reported, exactly as any other jumped
                // band is. In practice the floor test below has already set the flag whenever a
                // skipped segment is the cause; what this catches on its own is a row INGESTED
                // between two pages, which no floor knows about.
                if (row.StartTimeUnixNano > cursorNano)
                {
                    if (!seen.Contains(row.TraceId)) skippedRegion = true;
                    continue;
                }

                if (!seen.Add(row.TraceId)) continue;
                await sse.WriteEventAsync(row, TraceStreamJson.Default.TraceRowDto, ct);
                if (row.StartTimeUnixNano < oldestEmitted) oldestEmitted = row.StartTimeUnixNano;
                // Not Complete: the window was NOT read out, the ceiling was hit. Saying `done`
                // for both makes a truncated list indistinguishable from an exhausted one for
                // every consumer that cannot count its own rows against the max it asked for.
                //
                // Nor is it RegionSkipped, even when a region WAS skipped: `max-rows` already
                // carries `complete: false`, so it makes no positive claim to withdraw, and
                // turning the ordinary "I asked for 100 rows and got them" ending into an error
                // frame would be a regression for every well-behaved query. What it must NOT do
                // is stay SILENT about the skip, which is what returning a bare MaxRows did: the
                // same lost segment reached the user as `done {"reason":"max-rows"}` at one row
                // ceiling and as a red truncation banner at another, purely because of which
                // test the loop reached first. The outcome carries both facts and the terminal
                // frame prints both.
                //
                // AND THE SKIP IS TESTED HERE TOO, not only where the cursor moves. `max` can be
                // reached on the FIRST page, before the cursor has moved at all, so `skippedRegion`
                // may still be false while this page's own floor sits ABOVE everything the stream
                // has delivered — which is exactly a band of unsettled window sitting among the
                // rows the client is looking at. Measured on the M1 fixture: at max=1000 the
                // unopened segment was reported, and at max=50 the identical skip came back as a
                // bare `done {"reason":"max-rows"}` because the loop never reached the cursor test.
                if (++emitted >= max)
                    return new StreamOutcome(
                        StreamEnd.MaxRows,
                        skippedRegion || page.ScanFloorNano > oldestEmitted,
                        unreadableRegion);
            }

            // A SHORT PAGE IS NOT AN ENDING, and `page.Rows.Count < pageSize` must never be
            // reinstated as one. Neither fetcher returns pageSize rows whenever pageSize rows
            // exist — both over-fetch a fixed amount and POST-FILTER what comes back:
            //   * the filter list has no httpStatus parameter at all, so the endpoint fetches
            //     the newest min(1000, limit*3) traces and filters them here. At a 2% error
            //     rate ?httpStatus=5xx&max=2000 fills ~20 rows of a 500-row page;
            //   * GetTraceListAsync merges summaries UNFILTERED up to its own scan cap and only
            //     then applies status/service/duration, so ?status=Error does the same;
            //   * TraceQL asks for limit*10 SPANS and truncates to limit TRACES — an OR
            //     predicate extracts no hints at all, and a span-rich service yields far fewer
            //     traces than spans.
            // Ending there sent `done` — a positive assertion that the list is complete — after
            // 20 of the 2000 error traces the user asked for, which is the exact complaint this
            // feature exists to answer. The honest signal is Capped: the fetch itself says
            // whether it stopped for want of room or for want of data.
            if (!page.Capped) return EndOfWindow();

            // AN EMPTY PAGE IS NOT AN ENDING EITHER — zero is just the limiting case of short,
            // and it arrives for exactly the reasons the essay above lists. `?httpStatus=5xx`
            // over a month whose only 5xx traces are the OLDEST: the provider merges unfiltered,
            // truncates to the newest 1000 and reports Capped, and the post-filter here then
            // matches NOTHING among them. An OR-predicate in TraceQL does the same — no hints
            // are extracted, so the scan returns the newest limit*10 spans whatever the query
            // asked, and none of them may satisfy it. Ending on `page.Rows.Count == 0` sent
            // `done` with zero rows over a window full of matches, and the page rendered
            // "No traces found" for an incident three weeks back.
            //
            // ── THE CURSOR IS THE OLDEST ROW THE PAGE RETURNED ────────────────────────────
            //
            // It is the only candidate that does the two things a cursor must do. It DESCENDS
            // STRICTLY, so the loop progresses; and the next page's ceiling is a row already
            // sent, so nothing can arrive NEWER than something the client has already appended —
            // which matters because the client appends and re-emits, it does not re-sort, so an
            // out-of-order row renders at the top of a list the UI labels newest-first.
            //
            // The SCAN FLOOR is not the cursor, and the version that made it one failed in three
            // independent ways, all of them measured:
            //   * NO FORWARD PROGRESS. The floor is an unvisited segment's ceiling. A wide
            //     segment holding more than the scan budget BELOW that ceiling refills the budget
            //     on every page, breaks on the same nested segment, reports the same floor, and
            //     the cursor lands where it already was — for ever, over a shape as ordinary as
            //     a compacted week-wide segment with an hour-long one inside it;
            //   * ROWS OUT OF ORDER ACROSS PAGES. A floor ABOVE the page's own oldest row makes
            //     the next page's ceiling higher than a row already sent, so that page emits
            //     newer rows behind older ones;
            //   * the floor is clamped to the window ceiling, which is already millisecond
            //     aligned, so an unread segment reaching above `to` collapsed the floor onto the
            //     cursor and stalled the FIRST page — with a message about milliseconds.
            //
            // What the floor is good for is everything below.
            //
            // ALL OF IT IN NANOSECONDS. It used to run on `TryCeilToMillisecond` of both sides,
            // which quietly changed the answers in BOTH directions: a floor 0.1 ms above the
            // cursor rounded to the same millisecond and the skip went unreported, and a floor
            // 0.1 ms below one rounded apart and a skip was reported that had not happened. The
            // rounding belongs on the ASK — `pageTo`, above — and nowhere else.
            long nextNano = oldestReturned;
            bool haveNext = page.Rows.Count > 0 && nextNano < cursorNano;

            if (!haveNext)
            {
                // FLOOR JOB ONE: THE PAGE WITH NO ROW TO PAGE FROM. Either it came back empty, or
                // every row in it was one an earlier page already sent (or sat in the overlap
                // band above the cursor) — the same situation, and the old "the page was entirely
                // duplicates" guard is this branch, not a separate one. The floor is sound HERE
                // and only here: everything above it was examined by this very page, so moving
                // the ceiling down to it skips nothing. INCLUSIVE, because the floor is an
                // exclusive lower bound on what was settled — a row sitting exactly on it was not
                // necessarily returned.
                if (page.ScanFloorNano >= cursorNano)
                    // Two different failures, and they used to share one message about
                    // milliseconds. With rows in hand it really is the timestamp: the page could
                    // not be advanced off the instant it started from. With no rows it is the scan
                    // that could not be advanced — a segment that would not open, or a floor
                    // clamped to the window's own ceiling — and telling that user to narrow their
                    // window is advice about the wrong thing.
                    return new StreamOutcome(
                        page.Rows.Count > 0 ? StreamEnd.TimestampCollision : StreamEnd.NoProgress,
                        skippedRegion, unreadableRegion);
                nextNano = page.ScanFloorNano;
            }
            else if (nextNano < page.ScanFloorNano)
            {
                // FLOOR JOB TWO: HONESTY. The cursor is about to move BELOW the height this page
                // settled down to, so the band between them is read by no page: this one did not
                // reach it, and every later ceiling is lower.
                //
                // THIS IS A REAL AND ACCEPTED LOSS, not a theoretical one. It is what the fixture
                // in TraceStreamPagingFloorTests measures: a hundred matches in a cold segment
                // that a single late-arriving hot span pages the stream straight past. The cure
                // is not a cleverer cursor — the alternatives are the three failures listed above
                // — it is provider work: a scan budget one segment cannot monopolise, plus range
                // pushdown deep enough that a straddling segment costs what it contributes.
                // Deliberately not attempted here. What IS done here is refusing to hide it: the
                // stream can no longer end with `done {"complete":true}` after jumping a gap.
                skippedRegion = true;
            }

            // Walked back to the floor of the window; there is nothing older to ask for. Tested
            // AFTER the stall above, because the two overlap — both hold once the cursor reaches
            // the window's own floor millisecond, which a zero-width window (ParseFromTo
            // validates nothing) reaches on its first page — and a stall reported as `done` is a
            // positive claim of completeness over rows demonstrably still unread.
            if (nextNano <= fromNano) return EndOfWindow();

            cursorNano = nextNano;
        }

        // The three ways a stream can reach the bottom of its window, in the order of what they
        // withdraw. An unreadable segment is the strongest: it is the only one narrowing the
        // window does not fix, so it must not be reported as if it were the budget.
        StreamOutcome EndOfWindow() => new(
            unreadableRegion ? StreamEnd.RegionUnreadable
          : skippedRegion    ? StreamEnd.RegionSkipped
          :                    StreamEnd.Complete,
            skippedRegion, unreadableRegion);
    }

    /// <summary>
    /// How the paging loop ended, plus the two facts that outlive any single page.
    ///
    /// <para>They are carried BESIDE the ending rather than folded into it because they are not
    /// alternatives to it. "I reached your row ceiling" and "I skipped part of the window" are
    /// both true at once, routinely, and the loop used to report whichever test it reached first
    /// — so the SAME lost segment came back as <c>done {"reason":"max-rows"}</c> at one row
    /// ceiling and as a truncation banner at another.</para>
    /// </summary>
    private readonly record struct StreamOutcome(StreamEnd End, bool Skipped, bool Unreadable)
    {
        /// <summary>
        /// Machine-readable cause for the <c>done</c> payload, or null when nothing was lost.
        /// Only meaningful on an ending that is still a <c>done</c>; the error endings say it in
        /// their sentence instead.
        /// </summary>
        public string? TruncatedBy =>
            Unreadable ? "unreadable-segment"
          : Skipped    ? "unread-segment"
          :              null;
    }

    /// <summary>
    /// A linked deadline that CANCELS when it goes out of scope, not merely disposes.
    ///
    /// <para>Plain <c>using var cts = CreateLinkedTokenSource(...)</c> does not do this. Disposing
    /// a linked source stops its <c>CancelAfter</c> timer and unregisters its callback on the outer
    /// token — it leaves its OWN token uncancelled. So when an exception that is not a
    /// cancellation unwinds the paging loop (a write throwing <c>IOException</c> on a socket the
    /// client has already dropped is the realistic one), the scan running on the pool under that
    /// token has nothing left that can ever stop it: it reads segments off disk to completion for
    /// a reader who left minutes ago. It also outlives the request that owns it, which is how an
    /// abandoned scan reaches process-wide state after the code that set it has moved on.</para>
    /// </summary>
    private readonly struct DeadlineScope : IDisposable
    {
        private readonly CancellationTokenSource _cts;

        public DeadlineScope(CancellationToken outer, TimeSpan after)
        {
            _cts = CancellationTokenSource.CreateLinkedTokenSource(outer);
            _cts.CancelAfter(after);
        }

        public CancellationToken Token => _cts.Token;

        public void Dispose()
        {
            // Cancel BEFORE dispose — the other order throws. Callbacks run synchronously here and
            // one of them throwing must not replace the exception that is already unwinding.
            try { _cts.Cancel(); } catch { /* nothing left to tell */ }
            _cts.Dispose();
        }
    }

    /// <summary>
    /// Awaits <paramref name="work"/>, writing a keepalive comment frame every
    /// <paramref name="every"/> until it finishes. The interval is a parameter so a test can
    /// prove the keepalives without waiting a production interval for one.
    /// </summary>
    internal static async Task<T> AwaitWithKeepaliveAsync<T>(
        Task<T> work, SseJsonWriter sse, TimeSpan every, CancellationToken ct)
    {
        try
        {
            while (!work.IsCompleted)
            {
                // Cancelled the moment either side wins, so a finished page does not leave a live
                // 15-second timer behind on every iteration of the paging loop.
                using var tick = CancellationTokenSource.CreateLinkedTokenSource(ct);
                var       done = await Task.WhenAny(work, Task.Delay(every, tick.Token))
                                           .ConfigureAwait(false);
                tick.Cancel();
                if (ReferenceEquals(done, work)) break;

                ct.ThrowIfCancellationRequested();
                await sse.WriteKeepaliveAsync(ct).ConfigureAwait(false);
            }
            // The single await of `work`, so a fetch that threw surfaces its exception here
            // rather than vanishing into a task nobody looked at.
            return await work.ConfigureAwait(false);
        }
        catch
        {
            // Leaving without having awaited `work` — the request was aborted, or writing the
            // keepalive itself failed on a socket that has gone. The fetch is still running on
            // the pool and its own token will stop it, but its exception would then be a Task
            // fault no code ever observes. Attach the observer before unwinding.
            ObserveQuietly(work);
            throw;
        }
    }

    /// <summary>
    /// Swallows the eventual fault of a task this handler has stopped waiting for. Only ever
    /// applied to a fetch whose caller is already unwinding — never to one whose result is
    /// still wanted.
    /// </summary>
    private static void ObserveQuietly(Task work) =>
        _ = work.ContinueWith(static t => _ = t.Exception,
                              CancellationToken.None,
                              TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                              TaskScheduler.Default);

    /// <summary>
    /// Writes the terminal frame the loop earned. <c>done</c> is a positive claim that the list
    /// is complete, so anything that stopped early says so instead — on a 200, as a
    /// <c>query-error</c> frame, because EventSource cannot read the body of anything else.
    /// </summary>
    private static Task FinishStreamAsync(SseJsonWriter sse, StreamOutcome o, HttpContext ctx) => o.End switch
    {
        // Both are `done`, and a client that only listens for the event name (which is what the
        // Angular client does) keeps treating either as a normal completion. The payload is what
        // separates "the window was read out" from "your row ceiling stopped it" — and, on the
        // second, whether something was lost on the way there. `truncatedBy` is the field that
        // stops the same skipped segment being invisible at one row ceiling and a red banner at
        // another; Complete cannot carry it, because Complete is only reachable with neither flag.
        StreamEnd.Complete => sse.WriteDoneAsync(complete: true,  "exhausted", null,          ctx.RequestAborted),
        StreamEnd.MaxRows  => sse.WriteDoneAsync(complete: false, "max-rows",  o.TruncatedBy,  ctx.RequestAborted),

        // The two skip causes are deliberately different sentences, because the ADVICE is
        // opposite. A segment the walk had no budget left to open comes back when the window is
        // narrower; a segment that will not open does not come back at all, and telling that user
        // to narrow their window sends them round a loop that cannot help them.
        StreamEnd.RegionSkipped => SafeErrorAsync(sse,
            "Results are truncated: part of this window sits inside a storage segment the search "
          + "ran out of room to open before it had to move on, so the traces it holds are missing "
          + "from this list. Narrow the time window to bring them back into reach.", ctx, o.TruncatedBy),

        StreamEnd.RegionUnreadable => SafeErrorAsync(sse,
            "Results are truncated: a storage segment inside this window could not be read — it "
          + "was deleted or damaged — so the traces it held are missing from this list. Narrowing "
          + "the time window will not bring them back; the server log names the file.", ctx, o.TruncatedBy),

        // EVERY OTHER ENDING ASKS ABOUT THE LOSS FIRST, and that ordering is the fix, not a
        // flourish. These four used to read o.End alone, so they dropped Skipped and Unreadable
        // entirely — while StreamOutcome's own docstring promised the terminal frame prints both.
        //
        // It was not a corner. A CORRUPT segment is deliberately kept in the snapshot so it fails
        // again on every page, which means it supplies its ceiling to every page too — and that
        // value lands exactly ON the page ceiling as soon as the cursor descends to it, so the
        // no-progress test fires BEFORE the end-of-window test can apply its unreadable-over-
        // skipped-over-complete priority. RegionUnreadable was therefore reachable only for a
        // VANISHED segment, which is removed from the snapshot and so contributes its bit without
        // a ceiling. The user of a damaged file got "narrow the time window" — the one piece of
        // advice that state exists to avoid giving.
        // The cause travels with the sentence. Both terminal roads now carry it, so a page can give
        // one fault one treatment instead of deciding from English prose which fault it was —
        // decided, before this, by nothing more principled than how many rows happened to fit above
        // the loss.
        _ => SafeErrorAsync(sse, LossAwareSentence(o), ctx, o.TruncatedBy),
    };

    /// <summary>
    /// The sentence for an ending that is not a <c>done</c>. A loss outranks the mechanics: which
    /// of the pager's stopping conditions tripped is interesting, but "a segment could not be read"
    /// is what the reader has to act on, and its advice is the opposite of the mechanical one.
    /// </summary>
    private static string LossAwareSentence(StreamOutcome o)
    {
        if (o.Unreadable)
            return "Results are truncated: a storage segment inside this window could not be read — it "
                 + "was deleted or damaged — so the traces it held are missing from this list. Narrowing "
                 + "the time window will not bring them back; the server log names the file.";

        if (o.Skipped)
            return "Results are truncated: part of this window sits inside a storage segment the search "
                 + "ran out of room to open before it had to move on, so the traces it holds are missing "
                 + "from this list. Narrow the time window to bring them back into reach.";

        return o.End switch
        {
            StreamEnd.NoProgress =>
                "Results are truncated: the search could not be advanced past a part of this window "
              + "it was unable to read — a storage segment that would not open, or a scan that got no "
              + "further than the window's own edge. Narrow the time window (or the filter) to see the rest.",

            StreamEnd.TimestampCollision =>
                "Results are truncated: more traces share the oldest timestamp than one page can carry, "
              + "and the search cannot page past a millisecond it cannot move off. "
              + "Narrow the time window (or the filter) to see the rest.",

            StreamEnd.UnusableTimestamp =>
                "Results are truncated: a trace in this window carries a start time that cannot be turned "
              + "into a search boundary, and the search cannot page past it. "
              + "Narrow the time window (or the filter) to see the rest.",

            _ => $"Results are partial: the search hit its {DescribeDeadline(StreamDeadline)} limit "
               + "before reaching the end of the window. Narrow the time window (or the filter) to see the rest.",
        };
    }

    /// <summary>
    /// The deadline in the largest unit that does not round it to zero. <c>{X:N0}-second</c> alone
    /// rendered every sub-second deadline as "0-second limit" — a sentence that reads as a bug
    /// report about the server rather than as an explanation to the user.
    /// </summary>
    private static string DescribeDeadline(TimeSpan d) =>
        d.TotalSeconds >= 60 ? $"{d.TotalMinutes:0.##}-minute"
      : d.TotalSeconds >= 1  ? $"{d.TotalSeconds:0.##}-second"
      :                        $"{d.TotalMilliseconds:0.##}-millisecond";

    /// <summary>
    /// A nanosecond cursor rounded UP to the enclosing millisecond — the window the next page is
    /// ASKED for. False when the value cannot be turned into one at all.
    ///
    /// <para>The providers take millisecond-resolution timestamps while rows carry nanoseconds.
    /// Rounding DOWN would put the ask at or below the boundary row's own millisecond, and since
    /// the bound is inclusive, a trace at ...122.5 ms sitting between a floor-rounded ask and the
    /// oldest loaded row would be excluded by that page and by every later one — a hole no dedupe
    /// can close. Rounding up instead makes the boundary millisecond OVERLAP, which is the whole
    /// reason the caller's dedupe set exists. Identical to the rule the client already used.</para>
    ///
    /// <para>THE OVERLAP IS NOT THE CURSOR, and conflating the two is what put rows out of order.
    /// The caller keeps its cursor in exact nanoseconds and uses this only to widen the ask;
    /// what comes back inside the sliver above the cursor is either a duplicate or a row from a
    /// band the stream has already reported it jumped, and neither may be emitted. See
    /// <see cref="StreamTracePagesAsync"/>.</para>
    ///
    /// <para>The guards are for data the ingest path never validated. A start within a
    /// millisecond of <see cref="long.MaxValue"/> — a corrupt field, or a nanosecond column fed
    /// seconds-since-epoch multiplied by a billion one time too many — makes <c>+ 999_999</c>
    /// wrap NEGATIVE, and the ceiling then lands three centuries before the window instead of
    /// inside it. The caller's floor test sees a cursor below <c>from</c>, calls the window read
    /// out, and sends <c>done</c> over a list it truncated. Refusing the value instead ends the
    /// stream saying it was truncated, which is what happened.</para>
    ///
    /// <para>The range test after it is belt and braces: every value a wrapped nanosecond can
    /// divide down to today lands inside <see cref="DateTimeOffset"/>'s range, so it is the
    /// overflow test above that bites — but the two are one line apart and the day the scale
    /// changes, an <c>ArgumentOutOfRangeException</c> out of
    /// <see cref="DateTimeOffset.FromUnixTimeMilliseconds"/> would reach the handler's outer catch
    /// and be reported as the misleading "failed while streaming results".</para>
    /// </summary>
    internal static bool TryCeilToMillisecond(long startTimeUnixNano, out DateTimeOffset ceiling)
    {
        ceiling = default;
        if (startTimeUnixNano > long.MaxValue - 999_999L) return false;

        long ms = (startTimeUnixNano + 999_999L) / 1_000_000L;
        if (ms < DateTimeOffset.MinValue.ToUnixTimeMilliseconds() ||
            ms > DateTimeOffset.MaxValue.ToUnixTimeMilliseconds()) return false;

        ceiling = DateTimeOffset.FromUnixTimeMilliseconds(ms);
        return true;
    }

    /// <summary>
    /// Best-effort terminal error frame. The stream is already committed and half-written, so a
    /// failure to report the failure must not throw out of the handler — and it is bounded by
    /// its own short deadline rather than by the request token alone, because a client that has
    /// stopped reading would otherwise pin this handler open for as long as it lives.
    /// </summary>
    private static async Task SafeErrorAsync(
        SseJsonWriter sse, string message, HttpContext ctx, string? truncatedBy = null)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ctx.RequestAborted);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        try { await sse.WriteErrorAsync(message, cts.Token, truncatedBy); }
        catch { /* the client is gone, or will not read — nothing left to tell */ }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>The cheap list filters, read off the query string once.</summary>
    private readonly record struct TraceFilter(
        string?         Service,
        string?         SpanName,
        SpanStatusCode? Status,
        long?           MinDurationNanos,
        long?           MaxDurationNanos,
        string          HttpStatus);

    private static TraceFilter ParseTraceFilter(HttpContext ctx)
    {
        SpanStatusCode? status = null;
        if (ctx.Request.Query.TryGetValue("status", out var sv)
            && Enum.TryParse<SpanStatusCode>(sv, ignoreCase: true, out var sParsed))
            status = sParsed;

        return new TraceFilter(
            Service          : NullIfEmpty(ctx.Request.Query["service"]),
            SpanName         : NullIfEmpty(ctx.Request.Query["name"]),
            Status           : status,
            MinDurationNanos : ParseLong(ctx.Request.Query["minDurationMs"]) is long minMs ? minMs * 1_000_000L : null,
            MaxDurationNanos : ParseLong(ctx.Request.Query["maxDurationMs"]) is long maxMs ? maxMs * 1_000_000L : null,
            HttpStatus       : ctx.Request.Query["httpStatus"].ToString());
    }

    /// <summary>
    /// One page of the trace list: newest-first, at most <paramref name="limit"/> rows. Shared
    /// by GET /api/traces and its SSE twin so the two cannot drift into returning different
    /// rows for the same filters.
    ///
    /// <para>The page reports itself CAPPED when the provider's scan was cut short OR when this
    /// method's own httpStatus post-filter stopped at the limit with summaries still unread.
    /// Over-reporting is safe — it costs one more fetch — while under-reporting silently ends
    /// a stream in the middle of the window.</para>
    /// </summary>
    private static async Task<TracePage> FetchTracePageAsync(
        ITraceSummaryProvider provider,
        TraceFilter           filter,
        DateTimeOffset        from,
        DateTimeOffset        to,
        int                   limit,
        CancellationToken     ct)
    {
        // Over-fetch when a post-filter (httpStatus) is active so the page still fills.
        int fetch = string.IsNullOrEmpty(filter.HttpStatus) ? limit : Math.Min(1000, limit * 3);

        var page = await provider.GetTraceListAsync(
            from, to, filter.Service, filter.SpanName, filter.Status,
            filter.MinDurationNanos, filter.MaxDurationNanos, fetch, ct);

        var summaries = page.Rows;

        // The post-filter's OWN stopping point, tracked separately from the provider's. The two
        // are different facts and the raise below depends on which one happened.
        bool postFilterStoppedEarly = false;

        var traces = new List<TraceRowDto>(Math.Min(summaries.Count, limit));
        foreach (var s in summaries)
        {
            int? httpSc = s.HttpStatusCode != 0 ? s.HttpStatusCode : null;
            if (!MatchHttpStatus(httpSc, filter.HttpStatus)) continue;

            traces.Add(new TraceRowDto
            {
                TraceId           = s.TraceId.ToString(),
                SpanId            = s.RootSpanId.ToString(),
                Name              = s.Name,
                ServiceName       = s.ServiceName,
                Services          = s.Services,
                Status            = (s.HasError ? SpanStatusCode.Error : s.RootStatus).ToString(),
                HttpMethod        = s.HttpMethod,
                HttpPath          = s.HttpPath,
                HttpStatusCode    = httpSc,
                StartTimeUnixNano = s.RootStartNano,
                DurationNanos     = s.DurationNanos,
                SpanCount         = (int)s.SpanCount,
            });
            // Stopped with summaries still unexamined — whatever is in them is behind this page.
            if (traces.Count >= limit) { postFilterStoppedEarly = true; break; }
        }

        // The provider's floor, RAISED to the last row this method emitted — but ONLY when this
        // method's own post-filter is what stopped early. That is the condition the raise has
        // always been justified by, and it was not the condition it fired on: `capped` was
        // seeded from the provider's own answer, so the raise ALSO fired when the PROVIDER hit
        // its cap while the post-filter here ran clean to the end of the summaries. It then
        // discarded the provider's deeper, honest floor in favour of a shallower one derived
        // from rows, and threw away real matches doing it: 3 000 traces one per millisecond with
        // an httpStatus match on the first and the 2 501st, asked for at ?max=100, had the floor
        // pushed from trace 2 000 up to trace 2 500 — page two then found only the match it had
        // already sent, and the one at the bottom of the window was never delivered at all.
        //
        // When the post-filter DID break early, the raise is right and necessary: summaries below
        // the break were never shown to the httpStatus filter, so this page decided nothing about
        // them. Floors compose by maximum.
        long scanFloor = page.ScanFloorNano;
        if (postFilterStoppedEarly && traces.Count > 0)
            scanFloor = Math.Max(scanFloor, traces[^1].StartTimeUnixNano);

        return new TracePage(traces, scanFloor, page.Unreadable);
    }

    private static (DateTimeOffset from, DateTimeOffset to) ParseFromTo(HttpContext ctx, double defaultHours = 1)
    {
        var to   = DateTimeOffset.UtcNow;
        var from = to.AddHours(-defaultHours);
        if (ctx.Request.Query.TryGetValue("from", out var fv) && DateTimeOffset.TryParse(fv, out var fp)) from = fp;
        if (ctx.Request.Query.TryGetValue("to",   out var tv) && DateTimeOffset.TryParse(tv, out var tp)) to   = tp;
        return (from, to);
    }

    private static string? NullIfEmpty(string? s) => string.IsNullOrWhiteSpace(s) ? null : s;

    private static int ParseInt(string? s, int def, int min, int max) =>
        int.TryParse(s, out var v) ? Math.Clamp(v, min, max) : def;

    private static long? ParseLong(string? s) =>
        long.TryParse(s, out var v) && v > 0 ? v : null;

    // ── Flamegraph builder ────────────────────────────────────────────────────

    private static FlamegraphNode? BuildFlamegraph(List<SpanRecord> spans)
    {
        // Index for O(1) lookup
        var byId       = new Dictionary<SpanId, SpanRecord>(spans.Count);
        var children   = new Dictionary<SpanId, List<SpanRecord>>(spans.Count);

        foreach (var s in spans)
        {
            byId[s.SpanId] = s;
            if (!children.ContainsKey(s.SpanId)) children[s.SpanId] = [];
        }

        SpanRecord? root = null;
        foreach (var s in spans)
        {
            if (s.ParentSpanId.IsEmpty || !byId.ContainsKey(s.ParentSpanId))
            { root = s; continue; }
            children[s.ParentSpanId].Add(s);
        }

        return root is null ? null : BuildNode(root, children);
    }

    private static FlamegraphNode BuildNode(
        SpanRecord span, Dictionary<SpanId, List<SpanRecord>> childMap)
    {
        var kids    = childMap.TryGetValue(span.SpanId, out var c) ? c : [];
        var kidNodes = kids.Select(k => BuildNode(k, childMap)).ToArray();

        double totalMs = span.DurationNanos / 1_000_000.0;
        double childMs = kidNodes.Sum(n => n.TotalMs);
        double selfMs  = Math.Max(0, totalMs - childMs);

        return new FlamegraphNode
        {
            SpanId   = span.SpanId.ToString(),
            Name     = span.Name,
            Service  = span.ServiceName,
            Kind     = span.Kind.ToString(),
            Status   = span.Status.ToString(),
            TotalMs  = Math.Round(totalMs, 3),
            SelfMs   = Math.Round(selfMs,  3),
            Children = kidNodes,
        };
    }

    // ── Misc helpers ──────────────────────────────────────────────────────────

    private static async Task<List<SpanDto>> CollectSpansAsync(
        ITraceProvider provider, TraceId tid, CancellationToken ct)
    {
        var list = new List<SpanDto>();
        await foreach (var s in provider.GetTraceAsync(tid, ct))
            list.Add(SpanDto.From(s));
        return list;
    }

    private static async Task<List<SpanRecord>> CollectSpansRawAsync(
        ITraceProvider provider, TraceId tid, CancellationToken ct)
    {
        var list = new List<SpanRecord>();
        await foreach (var s in provider.GetTraceAsync(tid, ct))
            list.Add(s);
        return list;
    }

    private static object[] BuildBucketList(uint[] buckets)
    {
        var bounds = Ameto.Tracing.Storage.HistogramBuckets.Bounds;
        var result = new object[buckets.Length];
        for (int i = 0; i < buckets.Length; i++)
        {
            double upperMs = i < bounds.Length ? bounds[i] / 1_000_000.0 : double.MaxValue;
            result[i] = new { upperMs, count = buckets[i] };
        }
        return result;
    }

    private static DateTimeOffset? ParseDate(string? s) =>
        DateTimeOffset.TryParse(s, out var v) ? v : null;

    private static bool MatchHttpStatus(int? code, string filter)
    {
        if (string.IsNullOrEmpty(filter)) return true;
        if (code is null) return false;
        if (filter.Equals("4xx", StringComparison.OrdinalIgnoreCase)) return code >= 400 && code < 500;
        if (filter.Equals("5xx", StringComparison.OrdinalIgnoreCase)) return code >= 500 && code < 600;
        if (filter.Equals("2xx", StringComparison.OrdinalIgnoreCase)) return code >= 200 && code < 300;
        if (filter.Equals("3xx", StringComparison.OrdinalIgnoreCase)) return code >= 300 && code < 400;
        return int.TryParse(filter, out var exact) && code == exact;
    }

}

/// <summary>
/// The generated contract for the row type the trace stream emits. Source-generated rather than
/// reflected: this runs once per ROW, and a stream is the one place in the codebase where the
/// per-item serialisation cost is paid thousands of times for a single request.
/// </summary>
/// <remarks>
/// camelCase, and deliberately WITHOUT <c>WhenWritingNull</c>: the client reads
/// <c>httpStatusCode: null</c> on every row that carries no HTTP status and distinguishes it from
/// a row that was never asked. Dropping the property — which is exactly what EndpointMapper's own
/// options do, so they must not be borrowed here — would turn "no status" into "field missing" on
/// the wire. JsonSourceGenerationOptions leaves DefaultIgnoreCondition at Never, so the property
/// stays; TraceStreamEndpointTests pins it.
/// </remarks>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(TraceRowDto))]
internal partial class TraceStreamJson : JsonSerializerContext;

/// <summary>One row per trace (root span) for the trace list view.</summary>
public sealed class TraceRowDto
{
    public string   TraceId           { get; init; } = string.Empty;
    public string   SpanId            { get; init; } = string.Empty;
    public string   Name              { get; init; } = string.Empty;
    public string   ServiceName       { get; init; } = string.Empty;
    /// <summary>All unique service names across all spans in this trace.</summary>
    public string[] Services          { get; init; } = [];
    public string   Status            { get; init; } = string.Empty;
    public string   HttpMethod        { get; init; } = string.Empty;
    public string   HttpPath          { get; init; } = string.Empty;
    public int?     HttpStatusCode    { get; init; }
    public long     StartTimeUnixNano { get; init; }
    public long     DurationNanos     { get; init; }
    public int      SpanCount         { get; init; }
}

/// <summary>JSON DTO for a single span, returned to the Angular client.</summary>
public sealed class SpanDto
{
    public string                    TraceId           { get; init; } = string.Empty;
    public string                    SpanId            { get; init; } = string.Empty;
    public string                    ParentSpanId      { get; init; } = string.Empty;
    public long                      StartTimeUnixNano { get; init; }
    public long                      DurationNanos     { get; init; }
    public string                    Name              { get; init; } = string.Empty;
    public string                    ServiceName       { get; init; } = string.Empty;
    public string                    Kind              { get; init; } = string.Empty;
    public string                    Status            { get; init; } = string.Empty;
    public int                       HttpStatusCode    { get; init; }
    public Dictionary<string,string> Attributes        { get; init; } = [];

    public static SpanDto From(SpanRecord s) => new()
    {
        TraceId           = s.TraceId.ToString(),
        SpanId            = s.SpanId.ToString(),
        ParentSpanId      = s.ParentSpanId.ToString(),
        StartTimeUnixNano = s.StartTimeUnixNano,
        DurationNanos     = s.DurationNanos,
        Name              = s.Name,
        ServiceName       = s.ServiceName,
        Kind              = s.Kind.ToString(),
        Status            = s.Status.ToString(),
        HttpStatusCode    = s.HttpStatusCode,
        Attributes        = s.Attributes?.ToDictionary(kv => kv.Key, kv => kv.Value?.ToString() ?? string.Empty) ?? [],
    };
}

/// <summary>Single node in a trace flamegraph tree.</summary>
public sealed class FlamegraphNode
{
    public string          SpanId   { get; init; } = string.Empty;
    public string          Name     { get; init; } = string.Empty;
    public string          Service  { get; init; } = string.Empty;
    public string          Kind     { get; init; } = string.Empty;
    public string          Status   { get; init; } = string.Empty;
    public double          TotalMs  { get; init; }
    public double          SelfMs   { get; init; }
    public FlamegraphNode[] Children { get; init; } = [];
}

/// <summary>Request body for POST /api/traces/query.</summary>
public sealed class TraceQueryRequest
{
    public string  Query { get; init; } = string.Empty;
    public string? From  { get; init; }
    public string? To    { get; init; }
    public int     Limit { get; init; } = 100;
}

/// <summary>Aggregate stats for the trace stats cards.</summary>
public sealed class TraceStatsDto
{
    public int      TotalTraces    { get; init; }
    public double   ErrorRate      { get; init; }
    public double   P50LatencyMs   { get; init; }
    public double   P95LatencyMs   { get; init; }
    public double   P99LatencyMs   { get; init; }
    public double   ThroughputRps  { get; init; }
    public double[] TotalSparkline { get; init; } = [];
    public double[] ErrorSparkline { get; init; } = [];
}
