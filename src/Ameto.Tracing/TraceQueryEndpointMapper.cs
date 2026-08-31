using System.Text.Json;
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
                        return new TracePage(p.Rows, p.ScanFloorNano);
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

    /// <summary>camelCase, and deliberately WITHOUT <c>WhenWritingNull</c>.</summary>
    /// <remarks>
    /// The client reads <c>httpStatusCode: null</c> on every row that carries no HTTP status
    /// and distinguishes it from a row that was never asked. Dropping the property — which is
    /// exactly what EndpointMapper's own options do, so they must not be borrowed here —
    /// would turn "no status" into "field missing" on the wire.
    /// </remarks>
    private static readonly JsonSerializerOptions StreamJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented        = false,
    };

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
    /// The height above which this fetch speaks for the window: every match STRICTLY ABOVE it
    /// was emitted or deliberately filtered out, and nothing at or below it was necessarily
    /// looked at. <c>long.MinValue</c> means the fetch read the window out to its floor.
    ///
    /// Floors compose by MAXIMUM, never by minimum. Each one names a height above which some
    /// part of the work is settled, so only the highest is a claim all the others sit under —
    /// and it is the direction that matters, because a floor placed too LOW is a licence for
    /// the pager to jump over rows nobody read.
    /// </param>
    private readonly record struct TracePage(List<TraceRowDto> Rows, long ScanFloorNano)
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
        /// <summary>The cursor could not move off a millisecond — results are truncated.</summary>
        TimestampCollision,
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
    /// <para>Total work does not multiply by the page count: as the cursor moves back, segments
    /// newer than it fail the existing segment-level range check and are never opened.</para>
    /// </summary>
    private static async Task<StreamEnd> StreamTracePagesAsync(
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
        var  cursor    = to;
        int  emitted   = 0;
        long startedAt = System.Diagnostics.Stopwatch.GetTimestamp();

        while (true)
        {
            ct.ThrowIfCancellationRequested();
            // On the wall clock as well as through the token: the token cuts a fetch that is
            // already running, this one catches a loop whose pages keep completing, each of
            // them cheap, long after the stream as a whole should have given up.
            if (System.Diagnostics.Stopwatch.GetElapsedTime(startedAt) >= StreamDeadline)
                return StreamEnd.Deadline;

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
                var pageCursor = cursor;
                page = await AwaitWithKeepaliveAsync(
                    Task.Run(() => fetchPage(pageCursor, pageSize, scanCt), scanCt),
                    sse, StreamKeepalive, ct);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                return StreamEnd.Deadline;   // ours, not the client's
            }

            // Data frames are written HERE and keepalives only inside the await above, so the two
            // can never interleave: one thread of control, and the helper has returned before the
            // first `data:` byte of this page is written.
            foreach (var row in page.Rows)
            {
                if (!seen.Add(row.TraceId)) continue;
                await sse.WriteEventAsync(row, StreamJson, ct);
                // Not Complete: the window was NOT read out, the ceiling was hit. Saying `done`
                // for both makes a truncated list indistinguishable from an exhausted one for
                // every consumer that cannot count its own rows against the max it asked for.
                if (++emitted >= max) return StreamEnd.MaxRows;
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
            if (!page.Capped) return StreamEnd.Complete;

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
            // What blocked the fix was that an empty page has no Rows[^1] to page on — so the
            // cursor comes from the fetch's own SCAN FLOOR, the height above which it speaks for
            // the window.
            //
            // AND THE FLOOR IS THE WHOLE CURSOR. An earlier version took
            // `Math.Min(scanFloor, oldest emitted row)`, reasoning that a capped scan had
            // considered everything down to its floor so the lower of the two was safe and
            // jumped further. That reasoning is false in both tiers and it cost 100 of 103 rows
            // on a measured fixture — while still sending `done {"complete":true}`:
            //   * cold segments OVERLAP, and the walk sorts them by MaxStartNano DESCENDING,
            //     which this codebase documents as non-monotonic. A WIDE segment walked first
            //     can fill the scan budget alone while a NARROWER segment nested inside its
            //     range is still unopened. Paging to the wide segment's oldest row lands UNDER
            //     the nested one, where the range check then skips it on this page and on every
            //     later one, permanently;
            //   * the hot tier is merged unconditionally and is not subject to the budget, so a
            //     single late-arriving span — a backfill, a batch exporter, clock skew, a WAL
            //     replay after restart — drags the oldest returned row an hour below anything
            //     the scan reached.
            // Both make the lower bound a licence to skip. The floor is the only height the
            // fetch can actually vouch for, so it is the only cursor, and it moves the window
            // ceiling DOWN to it rather than past it. Re-reading a band costs a fetch; the
            // dedupe set absorbs the repeats. Skipping one loses rows silently, which is the
            // failure this whole endpoint exists to end.
            if (!TryCeilToMillisecond(page.ScanFloorNano, out var next)) return StreamEnd.UnusableTimestamp;

            // THE CURSOR CANNOT MOVE, and this is tested BEFORE the window floor below. The
            // cursor is a scanned start rounded up to its millisecond while rows carry
            // nanoseconds, so a page that falls entirely inside ONE millisecond asks the next
            // fetch the identical question and gets the identical page, for ever. What is
            // unreachable is not just the rest of that millisecond but everything older than it
            // in the window.
            //
            // The ORDER matters because the two endings overlap. Both are true at once whenever
            // the cursor has reached the window's floor millisecond — `ceil(floor) <= from` and
            // `ceil(floor) >= cursor` together — and with the floor tested first that was
            // reported as `done`: a positive claim of completeness over rows demonstrably still
            // unread inside that millisecond. The shape reaches it on the very first page of a
            // zero-width window, which ParseFromTo accepts without a word of validation, and it
            // needs ceil(floor) to land exactly ON `from` rather than a millisecond above it —
            // rare with true nanosecond starts, ORDINARY for the many exporters that emit
            // millisecond precision converted to nanos, where every start is an exact multiple
            // of 1e6. A stall must never be dressed up as completion.
            //
            // A nanosecond cursor would remove the stall outright, but the whole fetch path is
            // millisecond-bounded (ParseFromTo, SearchSpansAsync) — a deeper change than this.
            // Until then the failure is at least honest.
            //
            // This one condition also subsumes the old "the page was entirely duplicates" guard:
            // a page whose rows are ALL already seen came out of an earlier page, so its floor is
            // no older than the one the current cursor was derived from, and `next` lands right
            // back on `cursor`. There is no second case to distinguish.
            if (next >= cursor) return StreamEnd.TimestampCollision;

            // Walked back to the floor of the window; there is nothing older to ask for.
            if (next <= from) return StreamEnd.Complete;

            cursor = next;
        }
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
    private static Task FinishStreamAsync(SseJsonWriter sse, StreamEnd end, HttpContext ctx) => end switch
    {
        // Both are `done`, and a client that only listens for the event name (which is what the
        // Angular client does) keeps treating either as a normal completion. The payload is what
        // separates "the window was read out" from "your row ceiling stopped it".
        StreamEnd.Complete => sse.WriteDoneAsync(complete: true,  "exhausted", ctx.RequestAborted),
        StreamEnd.MaxRows  => sse.WriteDoneAsync(complete: false, "max-rows",  ctx.RequestAborted),

        StreamEnd.TimestampCollision => SafeErrorAsync(sse,
            "Results are truncated: more traces share the oldest timestamp than one page can carry, "
          + "and the search cannot page past a millisecond it cannot move off. "
          + "Narrow the time window (or the filter) to see the rest.", ctx),

        StreamEnd.UnusableTimestamp => SafeErrorAsync(sse,
            "Results are truncated: a trace in this window carries a start time that cannot be turned "
          + "into a search boundary, and the search cannot page past it. "
          + "Narrow the time window (or the filter) to see the rest.", ctx),

        _ => SafeErrorAsync(sse,
            $"Results are partial: the search hit its {DescribeDeadline(StreamDeadline)} limit "
          + "before reaching the end of the window. Narrow the time window (or the filter) to see the rest.", ctx),
    };

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
    /// A scanned start time rounded UP to the enclosing millisecond — the next page's upper
    /// bound. False when the value cannot be turned into one at all.
    ///
    /// <para>The endpoints take millisecond-resolution timestamps while rows carry nanoseconds.
    /// Rounding DOWN would put the cursor at or below the boundary row's own millisecond, and
    /// since the bound is inclusive that row comes back for ever. Rounding up instead makes the
    /// boundary millisecond OVERLAP — which is the whole reason the caller's dedupe set exists,
    /// and the only variant that cannot open a hole: a trace at ...122.5 ms sitting between a
    /// floor-rounded cursor and the oldest loaded row would be excluded by every later page too,
    /// because every later cursor derives from what actually loaded. Identical to the rule the
    /// client already used.</para>
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
    private static async Task SafeErrorAsync(SseJsonWriter sse, string message, HttpContext ctx)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ctx.RequestAborted);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        try { await sse.WriteErrorAsync(message, cts.Token); }
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
        bool capped   = page.Capped;

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
            if (traces.Count >= limit) { capped = true; break; }
        }

        // The provider's floor, RAISED to the last row this method actually emitted whenever its
        // own post-filter broke early. Summaries below that break were never shown to the
        // httpStatus filter, so the page cannot claim to have decided anything about them — and
        // a floor that claims otherwise is a floor the pager would jump straight past. This is
        // the SECOND place the fetch can stop short, and floors compose by maximum.
        long scanFloor = page.ScanFloorNano;
        if (capped && traces.Count > 0)
            scanFloor = Math.Max(scanFloor, traces[^1].StartTimeUnixNano);

        return new TracePage(traces, scanFloor);
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
