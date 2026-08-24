using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Query;
using Ameto.Server.Auth;
using Ameto.Storage;

namespace Ameto.Server;

/// <summary>Wire all Ameto HTTP endpoints onto the application.</summary>
public static class EndpointMapper
{
    private static readonly JsonSerializerOptions _json = new()
    {
        PropertyNamingPolicy        = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition      = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented               = false,
        Converters                  = { new DynamicObjectConverter() },
    };

    public static void MapAmetoEndpoints(this WebApplication app)
    {        // ── Health ────────────────────────────────────────────────────────────
        app.MapGet("/health", () => Results.Ok(new { status = "ok", utc = DateTimeOffset.UtcNow }));

        // ── Stats ─────────────────────────────────────────────────────────────
        app.MapGet("/api/stats", (StorageEngine storage) =>
        {
            var segs = storage.GetSegments(null, null);
            return Results.Ok(new
            {
                segments        = segs.Count,
                totalEvents     = segs.Sum(s => (long)s.EventCount),
                compressedBytes = segs.Sum(s => s.CompressedBytes),
            });
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewStats);

        // ── Ingest: POST /api/events  (CLEF msgpack batch) ───────────────────
        // Hot path: validated via in-memory ApiKeyCache (no JWT, no DB hit).
        app.MapPost("/api/events", async (HttpContext ctx, IngestionEndpoint ingestion, ApiKeyCache cache) =>
        {
            var key = Ameto.Ingestion.ApiKeyHeader.Extract(ctx.Request);
            if (key is null || !cache.Validate(key.AsSpan(), Ameto.Ingestion.ApiKeyPermissions.Logs))
            {
                ctx.Response.StatusCode  = StatusCodes.Status401Unauthorized;
                ctx.Response.ContentType = "application/json";
                await ctx.Response.WriteAsync("{\"error\":\"Valid API key with Logs permission required.\"}" );
                return;
            }
            await ingestion.HandleAsync(ctx);
        });

        // ── Query: GET /api/events  (SSE stream) ─────────────────────────────
        // Streams matching events as Server-Sent Events (one per data: line),
        // then signals completion with "event: done".
        // Query parameters:
        //   filter — Seq Filter Expression
        //   from   — ISO-8601 lower bound (inclusive)
        //   to     — ISO-8601 upper bound (inclusive)
        //   count  — max results (default 500)
        //   dir    — forward | backward (default backward)
        //   levels — comma-separated level names (omit = all levels)
        app.MapGet("/api/events", async (
            HttpContext           ctx,
            IQueryExecutor        executor,
            QueryGuard            guard,
            ILoggerFactory        loggerFactory,
            string?               filter   = null,
            string?               from     = null,
            string?               to       = null,
            int                   count    = 500,
            string?               dir      = null,
            string?               afterId  = null,
            long?                 afterTs  = null,
            string?               levels   = null) =>
        {
            // EVERYTHING THAT CAN BE REJECTED IS CHECKED BEFORE THE RESPONSE IS COMMITTED.
            // The stream used to open first and parse afterwards, so a malformed date or a
            // filter with a syntax error produced 200 OK followed by a stream that just
            // died — no status, no message, indistinguishable from "no results" — while a
            // 400 with the parser's own diagnostic was sitting right there.
            if (!TryParseWindow(from, to, out var fromUtc, out var toUtc, out string? windowError))
                return Results.BadRequest(new { error = windowError });

            if (!TryCompileFilter(filter, out string? filterError))
                return Results.BadRequest(new { error = filterError });

            // afterId is the raw 64-bit Snowflake EventId; combined with afterTs it forms
            // the (ts, id) cursor used by the keyset pagination in QueryExecutor.
            Ameto.Core.EventId? cursor = null;
            if (!string.IsNullOrEmpty(afterId) && ulong.TryParse(afterId, out var raw))
                cursor = new Ameto.Core.EventId(raw);

            var levelSet = ParseLevels(levels);

            var request = new QueryRequest
            {
                Filter              = filter,
                FromUtc             = fromUtc,
                ToUtc               = toUtc,
                Count               = Math.Clamp(count, 1, 10_000),
                Direction           = "forward".Equals(dir, StringComparison.OrdinalIgnoreCase)
                                          ? QueryDirection.Forward : QueryDirection.Backward,
                AfterEventId        = cursor,
                AfterTimestampTicks = afterTs,
                Levels              = levelSet,
            };

            QueryGuard.Lease? lease;
            try { lease = await guard.TryEnterAsync(ctx.RequestAborted); }
            catch (OperationCanceledException) { return Results.Empty; }   // client left the queue
            if (lease is null)
                return Refused(ctx);

            using (lease)
            {
                ctx.Response.ContentType = "text/event-stream";
                ctx.Response.Headers.CacheControl = "no-cache";
                ctx.Response.Headers.Connection   = "keep-alive";
                await ctx.Response.Body.FlushAsync(ctx.RequestAborted);

                using var deadline = guard.StartDeadline(ctx.RequestAborted);
                using var sse      = new SseJsonWriter(ctx.Response);
                try
                {
                    await foreach (var ev in executor.ExecuteAsync(request, deadline.Token))
                        await sse.WriteEventAsync(LogEventDto.From(ev), _json, deadline.Token);

                    // CHECKED AFTER THE LOOP, not only in a catch filter: the executor turns
                    // cancellation into a normal end-of-stream on its hot paths (a
                    // `yield break` per event and per merge step), so a budget that expires
                    // while rows are actually flowing raises nothing at all — and writing
                    // `done` there would report a truncated result as a complete one, which
                    // is the exact failure this budget exists to make visible.
                    if (deadline.TimedOut) await TimedOutAsync(sse, guard, ctx);
                    else                   await sse.WriteDoneAsync(ctx.RequestAborted);
                }
                catch (OperationCanceledException) when (deadline.TimedOut)
                {
                    await TimedOutAsync(sse, guard, ctx);
                }
                catch (OperationCanceledException) { /* client disconnected */ }
                catch (Exception ex)
                {
                    loggerFactory.CreateLogger(QueryLogCategory)
                                 .LogError(ex, "Search failed after the stream had opened");
                    // The client gets a stable sentence; the exception text can name segment
                    // paths and internals, and it is already in the log where it belongs.
                    await SafeErrorAsync(sse, "The search failed while streaming results. See the server log for details.", ctx);
                }
            }
            return Results.Empty;
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewLogs);

        // ── Aggregation: GET /api/events/aggregate ────────────────────────────
        // `select count(*) where @l = 'Error' group by ['service.name'] limit 20`.
        //
        // A separate endpoint because the answer is a different SHAPE: a table with its own
        // columns, not a stream of events, so it cannot arrive on the SSE channel the search
        // uses. The scan underneath is the ordinary one — same compilation of the where-clause,
        // same index hints, same tier merge — and it is guarded like any other search.
        app.MapGet("/api/events/aggregate", async (
            HttpContext    ctx,
            IQueryExecutor executor,
            QueryGuard     guard,
            ILoggerFactory loggerFactory,
            string?        filter = null,
            string?        from   = null,
            string?        to     = null) =>
        {
            if (!TryParseWindow(from, to, out var fromUtc, out var toUtc, out string? windowError))
                return Results.BadRequest(new { error = windowError });

            // Parse, not TryParse: the caller came to THIS endpoint, so anything that is not an
            // aggregation is their mistake and deserves to be named. TryParse's job is the
            // opposite — to decline quietly so free text stays free text — and it belongs on
            // the search path, not here.
            Ameto.Query.Filtering.AggregationQuery query;
            try
            {
                query = Ameto.Query.Filtering.AggregationParser.Parse(filter);
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = $"Invalid aggregation: {ex.Message}" });
            }

            QueryGuard.Lease? lease;
            try { lease = await guard.TryEnterAsync(ctx.RequestAborted); }
            catch (OperationCanceledException) { return Results.Empty; }
            if (lease is null) return Refused(ctx);

            var now     = DateTimeOffset.UtcNow;
            var toBound = toUtc   ?? now;
            var fromBnd = fromUtc ?? toBound.AddDays(-1);

            using (lease)
            {
                using var deadline = guard.StartDeadline(ctx.RequestAborted);
                try
                {
                    var result = await new Ameto.Query.AggregationExecutor(executor)
                        .ExecuteAsync(query, fromBnd, toBound, deadline.Token);

                    var rows = new AggregationRowDto[result.Rows.Count];
                    for (int i = 0; i < rows.Length; i++)
                        rows[i] = new AggregationRowDto { Key = result.Rows[i].Key, Values = result.Rows[i].Values };

                    return Results.Json(new AggregationResponse
                    {
                        From          = fromBnd.ToString("O"),
                        To            = toBound.ToString("O"),
                        KeyColumns    = [.. result.KeyColumns],
                        ValueColumns  = [.. result.ValueColumns],
                        Rows          = rows,
                        Scanned       = result.Scanned,
                        GroupsFound   = result.GroupsFound,
                        Partial       = result.Partial,
                        PartialReason = result.PartialReason,
                    }, AggregationJsonContext.Default.AggregationResponse);
                }
                catch (OperationCanceledException) when (deadline.TimedOut) { return TimedOutJson(guard); }
                catch (OperationCanceledException) { return Results.Empty; }   // client left
                catch (Exception ex)
                {
                    loggerFactory.CreateLogger(QueryLogCategory).LogError(ex, "Aggregation failed");
                    return Results.Json(
                        new { error = "The aggregation failed. See the server log for details." },
                        statusCode: StatusCodes.Status500InternalServerError);
                }
            }
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewLogs);

        // ── Query validation: GET /api/events/validate ────────────────────────
        // Why this exists: EventSource cannot read the BODY of a non-200 response — a
        // browser sees only "connection failed" — so the 400 the stream endpoints now
        // return is invisible to the one client that most needs the message. The UI asks
        // here when a stream dies before delivering anything, and gets the diagnostic the
        // stream could not hand it. Any other HTTP client just reads the 400 directly.
        app.MapGet("/api/events/validate", (
            string? filter = null,
            string? from   = null,
            string? to     = null) =>
        {
            if (!TryParseWindow(from, to, out _, out _, out string? windowError))
                return Results.BadRequest(new { error = windowError });
            if (!TryCompileFilter(filter, out string? filterError))
                return Results.BadRequest(new { error = filterError });
            return Results.Ok(new { ok = true });
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewLogs);

        // ── Distinct property names: GET /api/events/props ───────────────────
        // Returns sorted unique property keys from the last 24 h (up to 5 000 events sampled).
        app.MapGet("/api/events/props", async (HttpContext ctx, IQueryExecutor executor, QueryGuard guard) =>
        {
            var request = new QueryRequest
            {
                FromUtc   = DateTimeOffset.UtcNow.AddDays(-1),
                Count     = 5_000,
                Direction = QueryDirection.Backward,
            };

            // Guarded like the search it is: this scans up to 5 000 events over a day and
            // competes for exactly the mmap and decompression the limit exists to ration.
            QueryGuard.Lease? lease;
            try { lease = await guard.TryEnterAsync(ctx.RequestAborted); }
            catch (OperationCanceledException) { return Results.Empty; }
            if (lease is null) return Refused(ctx);

            using (lease)
            {
                using var deadline = guard.StartDeadline(ctx.RequestAborted);
                var props = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
                try
                {
                    await foreach (var ev in executor.ExecuteAsync(request, deadline.Token))
                    {
                        if (ev.Properties is null) continue;
                        foreach (var key in ev.Properties.Keys)
                            props.Add(key);
                    }
                }
                catch (OperationCanceledException) when (deadline.TimedOut) { return TimedOutJson(guard); }
                // The unfiltered partner the other guarded endpoints all have. A closed tab
                // cancels the linked token while TimedOut stays FALSE — it is a disconnect, not
                // a budget — so the filter above does not match and there was nothing left to
                // catch it: an ordinary client going away threw out of the delegate.
                catch (OperationCanceledException) { return Results.Empty; }
                if (deadline.TimedOut) return TimedOutJson(guard);
                return Results.Ok(props.ToArray());
            }
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewLogs);

        // ── Distinct services: GET /api/events/services ───────────────────────
        // Returns sorted unique values of ApplicationContext / service.name properties
        // from the last 7 days (up to 10 000 events sampled) — fast index-friendly scan.
        app.MapGet("/api/events/services", async (HttpContext ctx, IQueryExecutor executor, QueryGuard guard,
            int days = 7) =>
        {
            var request = new QueryRequest
            {
                FromUtc   = DateTimeOffset.UtcNow.AddDays(-Math.Clamp(days, 1, 90)),
                Count     = 10_000,
                Direction = QueryDirection.Backward,
            };

            QueryGuard.Lease? lease;
            try { lease = await guard.TryEnterAsync(ctx.RequestAborted); }
            catch (OperationCanceledException) { return Results.Empty; }
            if (lease is null) return Refused(ctx);

            using (lease)
            {
                using var deadline = guard.StartDeadline(ctx.RequestAborted);
                var services = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
                try
                {
                    await foreach (var ev in executor.ExecuteAsync(request, deadline.Token))
                    {
                        // Prefer service.name (OTLP), fall back to ApplicationContext (Serilog)
                        var svc = ev.ServiceName
                            ?? (ev.Properties?.TryGetValue("ApplicationContext", out var v) == true
                                ? v?.ToString() : null);
                        if (!string.IsNullOrWhiteSpace(svc))
                            services.Add(svc);
                    }
                }
                catch (OperationCanceledException) when (deadline.TimedOut) { return TimedOutJson(guard); }
                // The unfiltered partner the other guarded endpoints all have. A closed tab
                // cancels the linked token while TimedOut stays FALSE — it is a disconnect, not
                // a budget — so the filter above does not match and there was nothing left to
                // catch it: an ordinary client going away threw out of the delegate.
                catch (OperationCanceledException) { return Results.Empty; }
                if (deadline.TimedOut) return TimedOutJson(guard);
                return Results.Ok(services.ToArray());
            }
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewLogs);

        // ── Event counts by service + level over time: GET /api/events/counts ─
        // Powers the "Log events" chart / stats. Buckets per-service AND per-level
        // event counts over [from, to] by scanning event HEADERS only — no LogEvent,
        // Properties, message or exception is ever materialised (see
        // StorageEngine.AggregateLogVolumeAsync). Results are dense, chart-ready arrays.
        //   from    — ISO-8601 lower bound (default: now - 24h)
        //   to      — ISO-8601 upper bound (default: now)
        //   bucket  — bucket size in seconds (default: auto from the range)
        //   limit   — accepted for backward compatibility; the header scan is cheap
        //             enough to always cover the full window, so it no longer bounds it.
        //   service — restrict to a single service (case-insensitive)
        app.MapGet("/api/events/counts", async (
            HttpContext           ctx,
            StorageEngine         storage,
            LogVolumeCountsCache  cache,
            string?               from    = null,
            string?               to      = null,
            int?                  bucket  = null,
            int                   limit   = 50_000,
            string?               service = null) =>
        {
            _ = limit; // retained for API compatibility; no longer caps the scan.

            // A malformed date is a 400 with the parameter named, not an unhandled parse
            // exception surfacing as a 500 on a dashboard poll.
            if (!TryParseInstant(to,   "to",   out var toParsed,   out string? toError))   return Results.BadRequest(new { error = toError });
            if (!TryParseInstant(from, "from", out var fromParsed, out string? fromError)) return Results.BadRequest(new { error = fromError });

            var now     = DateTimeOffset.UtcNow;
            var toUtc   = toParsed   ?? now;
            var fromUtc = fromParsed ?? toUtc.AddDays(-1);
            // Rejected rather than silently swapped, like every other query endpoint: a
            // chart that quietly answers a different question than the one asked is the
            // harder bug to notice.
            if (fromUtc > toUtc)
                return Results.BadRequest(new { error = "'from' is later than 'to'." });

            double rangeSec = Math.Max(1, (toUtc - fromUtc).TotalSeconds);
            int bucketSeconds = bucket is > 0 ? bucket.Value : AutoBucketSeconds(rangeSec);

            // Keep the bucket axis manageable; widen the bucket if the requested
            // size would produce more than 2 000 columns.
            long minB = fromUtc.ToUnixTimeSeconds() / bucketSeconds;
            long maxB = toUtc.ToUnixTimeSeconds()   / bucketSeconds;
            if (maxB - minB + 1 > 2_000)
            {
                bucketSeconds = AutoBucketSeconds(rangeSec);
                minB = fromUtc.ToUnixTimeSeconds() / bucketSeconds;
                maxB = toUtc.ToUnixTimeSeconds()   / bucketSeconds;
            }
            int nBuckets = (int)(maxB - minB + 1);

            var svcFilter = string.IsNullOrEmpty(service) ? null : service;

            // Cache keyed on the bucket grid so drifting "now" bounds still hit within a TTL.
            var cacheKey = new CountsCacheKey(minB, maxB, bucketSeconds, svcFilter);
            if (cache.TryGet(cacheKey, out var cached))
                return Results.Json(cached, EventCountsJsonContext.Default.EventCountsResponse);

            var result = await storage.AggregateLogVolumeAsync(
                fromUtc, toUtc, minB, bucketSeconds, nBuckets, svcFilter, ctx.RequestAborted);

            var buckets = new long[nBuckets];
            for (int i = 0; i < nBuckets; i++)
                buckets[i] = (minB + i) * bucketSeconds * 1000L; // bucket start, unix ms

            // Top services by count (cap to 25 to bound payload / chart noise). Services
            // are already sorted descending by the aggregator.
            const int maxServices = 25;
            int svcTake = Math.Min(maxServices, result.Services.Count);
            var servicesOut = new CountSeriesDto[svcTake];
            for (int i = 0; i < svcTake; i++)
            {
                var s = result.Services[i];
                servicesOut[i] = new CountSeriesDto { Service = s.Name, Count = s.Count, Points = s.Points };
            }

            var levelsOut = new CountSeriesDto[result.Levels.Count];
            for (int i = 0; i < result.Levels.Count; i++)
            {
                var l = result.Levels[i];
                levelsOut[i] = new CountSeriesDto { Level = l.Name, Count = l.Count, Points = l.Points };
            }

            var response = new EventCountsResponse
            {
                From          = fromUtc.ToString("O"),
                To            = toUtc.ToString("O"),
                BucketSeconds = bucketSeconds,
                Total         = result.Total,
                Sampled       = result.Scanned,
                Truncated     = false, // full-window header scan — never sampled/truncated
                Buckets       = buckets,
                Services      = servicesOut,
                Levels        = levelsOut,
            };

            cache.Set(cacheKey, response);
            return Results.Json(response, EventCountsJsonContext.Default.EventCountsResponse);
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewLogs);

        // ── Live tail: GET /api/events/live  (SSE) ────────────────────────────
        // Streams new events as Server-Sent Events in CLEF JSON format.
        // Parameters: filter, from (default = now).
        //
        // The tail WAITS to be told that something was written — LiveEventSignal, fed by the
        // storage engine's write hook — instead of re-querying on a timer. The old loop ran a
        // forward catalog scan four times a second per open tab and almost always found
        // nothing, each attempt taking a search slot, while the writer knew the exact moment
        // there was anything to find. What a tail may SEE is unchanged: the same query, the
        // same filter, the same (timestamp, id) cursor. An idle tail now costs one poll per
        // LiveTail.MaxWait instead of four a second, and a new event is delivered as soon as
        // the coalescing floor allows rather than up to 250 ms later.
        app.MapGet("/api/events/live", async (
            HttpContext     ctx,
            IQueryExecutor  executor,
            QueryGuard      guard,
            LiveEventSignal signal,
            ServerOptions   options,
            ILoggerFactory  loggerFactory,
            string?         filter  = null,
            string?         from    = null,
            string?         levels  = null) =>
        {
            // Validated before the stream opens, for the same reason as /api/events: a
            // tail that dies on its first poll because of a typo in the filter looked
            // exactly like a tail with nothing to show.
            if (!TryParseInstant(from, "from", out var fromParsed, out string? windowError))
                return Results.BadRequest(new { error = windowError });
            if (!TryCompileFilter(filter, out string? filterError))
                return Results.BadRequest(new { error = filterError });

            ctx.Response.ContentType = "text/event-stream";
            ctx.Response.Headers.CacheControl = "no-cache";
            ctx.Response.Headers.Connection    = "keep-alive";
            await ctx.Response.Body.FlushAsync(ctx.RequestAborted);

            // Tail starts from 'from' or now, forward direction, unlimited.
            var fromDt = fromParsed ?? DateTimeOffset.UtcNow;

            // Same parameter, same meaning as on the search: the level selector used to apply
            // to the page's history and be dropped the moment the tail started, so switching
            // to live silently widened the view back to every level.
            var levelSet = ParseLevels(levels);

            var tail     = options.LiveTail;
            int pageSize = Math.Clamp(tail.PageSize, 1, 10_000);
            var maxWait  = tail.MaxWait > TimeSpan.Zero ? tail.MaxWait : TimeSpan.FromSeconds(5);

            Ameto.Core.EventId? cursor = null;
            long?                cursorTs = null;
            int                  refusedInARow = 0;
            bool                 behind        = false;   // the last poll came back full
            // Stamped one second back so the first poll runs immediately.
            long lastPollStamp = System.Diagnostics.Stopwatch.GetTimestamp() - System.Diagnostics.Stopwatch.Frequency;
            long lastFrameStamp = System.Diagnostics.Stopwatch.GetTimestamp();

            using var sse = new SseJsonWriter(ctx.Response);
            try
            {
                // One frame up front, so the stream proves itself open immediately. It used
                // to fall out of the 250 ms poll; now a quiet tail says nothing until its
                // first wait expires, and "connected" should not have to wait that long.
                await sse.WriteKeepaliveAsync(ctx.RequestAborted);
                lastFrameStamp = System.Diagnostics.Stopwatch.GetTimestamp();

                while (!ctx.RequestAborted.IsCancellationRequested)
                {
                    // Coalescing floor. On a busy server the signal fires continuously, and
                    // without this a tail would re-query as fast as searches complete —
                    // taking a slot each time. Events arriving inside the window are simply
                    // delivered together by the next poll.
                    var since = System.Diagnostics.Stopwatch.GetElapsedTime(lastPollStamp);
                    if (since < tail.MinInterval)
                        await Task.Delay(tail.MinInterval - since, ctx.RequestAborted);

                    // THE STREAM NEVER GOES QUIET FOR LONGER THAN MaxWait, whichever way the
                    // loop went round. Tying the keepalive to the park alone was wrong: a
                    // refused poll costs the full queue wait and a tail whose filter matches
                    // nothing on a busy server never parks at all, so either could hold a
                    // connection open for tens of seconds without a byte — long enough for a
                    // proxy to tear it down, and the client has no reconnect.
                    if (System.Diagnostics.Stopwatch.GetElapsedTime(lastFrameStamp) >= maxWait)
                    {
                        await sse.WriteKeepaliveAsync(ctx.RequestAborted);
                        lastFrameStamp = System.Diagnostics.Stopwatch.GetTimestamp();
                    }

                    // Read the version BEFORE polling. An event committed while the poll runs
                    // either appears in its own results or leaves this value behind — never
                    // neither. Reading it afterwards would swallow exactly that window and
                    // the tail would sit still until the next unrelated write.
                    long seen = signal.Version;
                    lastPollStamp = System.Diagnostics.Stopwatch.GetTimestamp();

                    // A tail that is behind drains in BIGGER GULPS, not more often. The floor
                    // above bounds how often a tail may take a search slot, so catching up by
                    // polling faster would trade a lagging tail for starved interactive
                    // searches; and the expensive part of a poll is per-POLL, not per-event —
                    // the hot-tier scan walks and sorts the whole post-cursor match set to
                    // yield one page either way. Without this the drain rate is capped at
                    // PageSize/MinInterval however fast the machine is.
                    int wanted = behind ? Math.Min(pageSize * CatchUpPageFactor, 10_000) : pageSize;

                    var request = new QueryRequest
                    {
                        Filter              = filter,
                        FromUtc             = fromDt,
                        Count               = wanted,
                        Direction           = QueryDirection.Forward,
                        AfterEventId        = cursor,
                        AfterTimestampTicks = cursorTs,
                        Levels              = levelSet,
                    };

                    int newCount = 0;
                    // A slot per POLL, never for the life of the connection: a tail is open
                    // for hours and would otherwise hold a search slot the whole time.
                    var lease = await guard.TryEnterAsync(ctx.RequestAborted);
                    if (lease is null)
                    {
                        // The server is at its limit. A tail can wait — but not silently
                        // for ever, or the page shows a live view that stopped being live
                        // without ever saying so. It does NOT park on the signal here:
                        // nothing was read, so whatever it would announce is already waiting.
                        if (++refusedInARow >= RefusalsBeforeGivingUp)
                        {
                            await SafeErrorAsync(sse,
                                "The server is busy and the live tail could not keep up. Reconnect in a moment.", ctx);
                            break;
                        }
                        continue;
                    }

                    refusedInARow = 0;
                    using (lease)
                    {
                        // Bounded like any other search: an unfiltered forward poll over
                        // a wide window is a full-catalog scan, and without a budget it
                        // would hold the slot it took for as long as that takes.
                        using var deadline = guard.StartDeadline(ctx.RequestAborted);
                        await foreach (var ev in executor.ExecuteAsync(request, deadline.Token))
                        {
                            await sse.WriteEventAsync(LogEventDto.From(ev), _json, deadline.Token);
                            cursor   = (Ameto.Core.EventId?)ev.Id;
                            cursorTs = ev.Timestamp.UtcTicks;
                            newCount++;
                        }
                        if (deadline.TimedOut)
                        {
                            await SafeErrorAsync(sse,
                                $"The live tail's poll exceeded its {guard.Timeout.TotalSeconds:0}s budget — narrow the filter.", ctx);
                            break;
                        }
                    }

                    if (newCount > 0) lastFrameStamp = System.Diagnostics.Stopwatch.GetTimestamp();

                    // A full page means the cursor stopped short of the backlog, not that the
                    // backlog ended — go round again rather than parking on a tail that is
                    // behind, and ask for more next time. Same for a write that landed while
                    // this poll was reading.
                    behind = newCount >= wanted;
                    if (behind || signal.Version != seen)
                        continue;

                    // Caught up. Park until the writer says otherwise; the timeout is what
                    // keeps the loop turning over for the keepalive above, and the safety net
                    // should a wake-up ever be missed.
                    await signal.WaitAsync(seen, maxWait, ctx.RequestAborted);
                }
            }
            catch (OperationCanceledException) { /* client disconnected */ }
            catch (Exception ex)
            {
                loggerFactory.CreateLogger(QueryLogCategory)
                             .LogError(ex, "Live tail failed after the stream had opened");
                await SafeErrorAsync(sse, "The live tail failed. See the server log for details.", ctx);
            }
            return Results.Empty;
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewLogs);

        // ── Span logs: GET /api/spans/{spanId}/logs ───────────────────────────
        // Returns up to 500 log events that were emitted within the given span.
        // spanId must be a 16-char lowercase hex string (W3C 64-bit span id).
        app.MapGet("/api/spans/{spanId}/logs", async (
            HttpContext    ctx,
            IQueryExecutor executor,
            QueryGuard     guard,
            string         spanId,
            string?        from  = null,
            string?        to    = null,
            int            count = 500) =>
        {
            if (!Ameto.Core.TraceIdHelper.TryParseSpanId(spanId, out _))
            {
                ctx.Response.StatusCode = 400;
                await ctx.Response.WriteAsync("spanId must be a 16-char hex string");
                return;
            }

            if (!TryParseWindow(from, to, out var fromUtc, out var toUtc, out string? windowError))
            {
                ctx.Response.StatusCode = 400;
                await ctx.Response.WriteAsJsonAsync(new { error = windowError }, ctx.RequestAborted);
                return;
            }

            // Build filter that hits the inverted index on @sp
            string spanFilter = $"@sp = '{spanId}'";

            var request = new QueryRequest
            {
                Filter    = spanFilter,
                FromUtc   = fromUtc,
                ToUtc     = toUtc,
                Count     = Math.Clamp(count, 1, 5_000),
                Direction = QueryDirection.Forward,
            };

            // Guarded and bounded: from/to are optional here, so this is routinely an
            // unbounded-window scan — the shape the budget exists for.
            await WriteGuardedListAsync(ctx, executor, guard, request);
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewLogs);

        // ── Trace logs: GET /api/traces/{traceId}/logs ────────────────────────
        // Returns every log event correlated to the trace (filtered on @tr). This is
        // the primary trace↔logs correlation: logs are written under child spans, so
        // a trace-wide query is what actually surfaces them. The client narrows to a
        // single span by matching @sp on its side.
        // traceId must be a 32-char lowercase hex string (W3C 128-bit trace id).
        app.MapGet("/api/traces/{traceId}/logs", async (
            HttpContext    ctx,
            IQueryExecutor executor,
            QueryGuard     guard,
            string         traceId,
            string?        from  = null,
            string?        to    = null,
            int            count = 2000) =>
        {
            if (!Ameto.Core.TraceIdHelper.TryParseTraceId(traceId, out _, out _))
            {
                ctx.Response.StatusCode = 400;
                await ctx.Response.WriteAsync("traceId must be a 32-char hex string");
                return;
            }

            if (!TryParseWindow(from, to, out var fromUtc, out var toUtc, out string? windowError))
            {
                ctx.Response.StatusCode = 400;
                await ctx.Response.WriteAsJsonAsync(new { error = windowError }, ctx.RequestAborted);
                return;
            }

            // Build filter that hits the inverted index on @tr
            string traceFilter = $"@tr = '{traceId}'";

            var request = new QueryRequest
            {
                Filter    = traceFilter,
                FromUtc   = fromUtc,
                ToUtc     = toUtc,
                Count     = Math.Clamp(count, 1, 5_000),
                Direction = QueryDirection.Forward,
            };

            await WriteGuardedListAsync(ctx, executor, guard, request);
        }).RequireAuthorization(AuthServiceExtensions.PolicyViewLogs);
    }

    /// <summary>
    /// Runs a bounded query under the search limit and the time budget, and writes the
    /// result as a JSON array. Refusal is 503, an expired budget is 504 — a JSON list
    /// cannot admit to being partial the way a stream can, so it does not pretend.
    /// </summary>
    private static async Task WriteGuardedListAsync(
        HttpContext ctx, IQueryExecutor executor, QueryGuard guard, QueryRequest request)
    {
        QueryGuard.Lease? lease;
        try { lease = await guard.TryEnterAsync(ctx.RequestAborted); }
        catch (OperationCanceledException) { return; }
        if (lease is null)
        {
            ctx.Response.StatusCode         = StatusCodes.Status503ServiceUnavailable;
            ctx.Response.Headers.RetryAfter = "5";
            await ctx.Response.WriteAsJsonAsync(
                new { error = "Too many searches are running. Try again in a moment." }, ctx.RequestAborted);
            return;
        }

        using (lease)
        {
            using var deadline = guard.StartDeadline(ctx.RequestAborted);
            var results = new List<LogEventDto>();
            try
            {
                await foreach (var ev in executor.ExecuteAsync(request, deadline.Token))
                    results.Add(LogEventDto.From(ev));
            }
            catch (OperationCanceledException) when (deadline.TimedOut) { }
            catch (OperationCanceledException) { return; }   // client disconnected

            if (deadline.TimedOut)
            {
                ctx.Response.StatusCode = StatusCodes.Status504GatewayTimeout;
                await ctx.Response.WriteAsJsonAsync(
                    new { error = $"The query exceeded its {guard.Timeout.TotalSeconds:0}s budget. Narrow the time range." },
                    ctx.RequestAborted);
                return;
            }

            await ctx.Response.WriteAsJsonAsync(results, _json, ctx.RequestAborted);
        }
    }

    // ── API-key extraction (ingest path only) ─────────────────────────────────

    /// <summary>
    /// Picks a "nice" time-bucket size (in seconds) for a given range so the
    /// resulting chart has ~120 columns. Used by GET /api/events/counts when no
    /// explicit <c>bucket</c> is supplied.
    /// </summary>
    private static int AutoBucketSeconds(double rangeSeconds)
    {
        const int target = 120;
        double raw = rangeSeconds / target;
        int[] steps = { 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14_400, 21_600, 43_200, 86_400, 172_800, 604_800 };
        foreach (var s in steps)
            if (s >= raw) return s;
        return (int)Math.Ceiling(raw / 604_800.0) * 604_800;
    }

    // ── Request validation (runs BEFORE the response is committed) ─────────────

    /// <summary>
    /// Parses the time window, naming the offending parameter. <c>DateTimeOffset.Parse</c>
    /// threw straight out of the handler, which — after the SSE headers had been flushed —
    /// the client saw as a stream that stopped for no reason.
    /// </summary>
    private static bool TryParseWindow(
        string? from, string? to,
        out DateTimeOffset? fromUtc, out DateTimeOffset? toUtc, out string? error)
    {
        fromUtc = toUtc = null;
        error   = null;

        if (!TryParseInstant(from, "from", out fromUtc, out error)) return false;
        if (!TryParseInstant(to,   "to",   out toUtc,   out error)) return false;

        if (fromUtc is { } f && toUtc is { } t && f > t)
        {
            error = "'from' is later than 'to'.";
            return false;
        }
        return true;
    }

    /// <summary>
    /// The <c>levels</c> query parameter — a comma-separated list — as the executor wants it.
    /// Unparseable names are ignored rather than rejected, and both "none of them" and "all
    /// six" mean the same thing as omitting the parameter: no constraint. Shared by the search
    /// and the live tail so the level selector means one thing in both.
    /// </summary>
    private static HashSet<Ameto.Core.LogLevel>? ParseLevels(string? levels)
    {
        if (string.IsNullOrEmpty(levels)) return null;

        var set = new HashSet<Ameto.Core.LogLevel>();
        foreach (var part in levels.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (Ameto.Core.LogLevelExtensions.TryParse(part.AsSpan(), out var lvl))
                set.Add(lvl);
        }
        return set.Count is 0 or 6 ? null : set;
    }

    private static bool TryParseInstant(string? raw, string name, out DateTimeOffset? value, out string? error)
    {
        value = null;
        error = null;
        if (string.IsNullOrEmpty(raw)) return true;

        if (!DateTimeOffset.TryParse(raw, null, System.Globalization.DateTimeStyles.RoundtripKind, out var parsed))
        {
            error = $"'{name}' is not a valid ISO-8601 timestamp: '{raw}'.";
            return false;
        }
        value = parsed.ToUniversalTime();
        return true;
    }

    /// <summary>
    /// Compiles the filter here so a syntax error is a 400 with the parser's own message,
    /// rather than an exception thrown from inside the result stream. The compiled form is
    /// discarded — the executor compiles it again — which costs one parse of a short string
    /// per query and buys the client an answer it can act on.
    /// </summary>
    private static bool TryCompileFilter(string? filter, out string? error)
    {
        error = null;
        if (string.IsNullOrWhiteSpace(filter)) return true;

        // An aggregation is a valid query, just not one this endpoint can answer: its result
        // is a table and this channel carries events. Saying so beats the parser's own report,
        // which would be about a `select` it has never heard of.
        try
        {
            if (Ameto.Query.Filtering.AggregationParser.TryParse(filter, out _))
            {
                error = "This is an aggregation — ask GET /api/events/aggregate for it.";
                return false;
            }
        }
        catch (Exception ex)
        {
            error = $"Invalid aggregation: {ex.Message}";
            return false;
        }

        try
        {
            Ameto.Query.Filtering.CompiledFilter.Compile(filter);
            return true;
        }
        catch (Exception ex)
        {
            error = $"Invalid filter: {ex.Message}";
            return false;
        }
    }

    /// <summary>
    /// Best-effort error frame. The stream may already be half-written or the socket gone;
    /// failing to report a failure must not itself throw out of the handler.
    ///
    /// <para>Bounded by its own short deadline, not by the request token alone: the write
    /// goes to a socket the search may have just timed out ON, and a caller that parks
    /// here goes on holding its search slot for as long as the stuck client lives.</para>
    /// </summary>
    private static async Task SafeErrorAsync(SseJsonWriter sse, string message, HttpContext ctx)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ctx.RequestAborted);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        try { await sse.WriteErrorAsync(message, cts.Token); }
        catch { /* the client is gone, or will not read — nothing left to tell */ }
    }

    /// <summary>The terminal frame for a search stopped by its budget.</summary>
    private static Task TimedOutAsync(SseJsonWriter sse, QueryGuard guard, HttpContext ctx) =>
        SafeErrorAsync(
            sse,
            $"Search exceeded its {guard.Timeout.TotalSeconds:0}s budget — narrow the time range or the filter. Results shown are partial.",
            ctx);

    /// <summary>503 with Retry-After: the server is at its search limit right now.</summary>
    private static IResult Refused(HttpContext ctx)
    {
        ctx.Response.Headers.RetryAfter = "5";
        return Results.Json(
            new { error = "Too many searches are running. Try again in a moment." },
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }

    /// <summary>Log category for query-path failures reported to clients only in summary.</summary>
    private const string QueryLogCategory = "Ameto.Server.Query";

    /// <summary>
    /// Consecutive refused polls after which a live tail stops pretending. A refusal costs
    /// the full queue wait before it is reported, so this is tens of seconds of a genuinely
    /// saturated server — long enough not to fire on a burst, short enough that the page does
    /// not keep showing a live view that is not live.
    /// </summary>
    private const int RefusalsBeforeGivingUp = 8;

    /// <summary>
    /// How much bigger a live tail's page gets while it is behind. Catching up by polling
    /// more often is not an option — the coalescing floor is what stops a tail from holding a
    /// search slot continuously — so it catches up by asking for more per poll instead, which
    /// also amortises the fixed per-poll cost (the hot-tier scan walks and sorts the whole
    /// post-cursor match set whatever the page size).
    /// </summary>
    private const int CatchUpPageFactor = 8;

    /// <summary>504 for the non-streaming endpoints, which cannot report a partial answer.</summary>
    private static IResult TimedOutJson(QueryGuard guard) =>
        Results.Json(
            new { error = $"The query exceeded its {guard.Timeout.TotalSeconds:0}s budget. Narrow the time range." },
            statusCode: StatusCodes.Status504GatewayTimeout);
}

// ── Dynamic object converter ──────────────────────────────────────────────────

/// <summary>
/// Serialises <c>object?</c> values stored in property dictionaries.
/// Handles the concrete types produced by <see cref="LogEventSerializer"/>:
/// nested dicts, arrays, primitives. Avoids the default ToString() fallback.
/// </summary>
internal sealed class DynamicObjectConverter : JsonConverter<object>
{
    public override object? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        => throw new NotSupportedException();

    public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
    {
        switch (value)
        {
            case Dictionary<string, object?> d:
                writer.WriteStartObject();
                foreach (var (k, v) in d)
                {
                    writer.WritePropertyName(k);
                    if (v is null) writer.WriteNullValue();
                    else Write(writer, v, options);
                }
                writer.WriteEndObject();
                break;
            case object[] arr:
                writer.WriteStartArray();
                foreach (var item in arr)
                {
                    if (item is null) writer.WriteNullValue();
                    else Write(writer, item, options);
                }
                writer.WriteEndArray();
                break;
            case string s:  writer.WriteStringValue(s);     break;
            case bool b:    writer.WriteBooleanValue(b);    break;
            case long l:    writer.WriteNumberValue(l);     break;
            case int i:     writer.WriteNumberValue(i);     break;
            case double d:  writer.WriteNumberValue(d);     break;
            case float f:   writer.WriteNumberValue(f);     break;
            case ulong u:   writer.WriteNumberValue(u);     break;
            default:        writer.WriteStringValue(value.ToString()); break;
        }
    }
}

// ── DTO ───────────────────────────────────────────────────────────────────────

/// <summary>JSON-serialisable view of a <see cref="LogEvent"/>.</summary>
internal sealed class LogEventDto
{
    [JsonPropertyName("@t")]            public string Timestamp       { get; init; } = "";
    [JsonPropertyName("@mt")]           public string MessageTemplate { get; init; } = "";
    [JsonPropertyName("@l")]            public string Level           { get; init; } = "";
    [JsonPropertyName("@x")]            public ExceptionInfoDto? Exception { get; init; }
    [JsonPropertyName("id")]            public string Id              { get; init; } = "";
    [JsonPropertyName("@tr")]           public string? TraceId        { get; init; }
    [JsonPropertyName("@sp")]           public string? SpanId         { get; init; }
    [JsonPropertyName("service.name")]  public string? ServiceName    { get; init; }
    [JsonPropertyName("props")]         public EventProps? Properties { get; init; }

    public static LogEventDto From(LogEvent ev) => new()
    {
        Timestamp       = ev.Timestamp.ToString("O"),
        MessageTemplate = ev.MessageTemplate,
        Level           = ev.Level.ToSeqString(),
        Exception       = ExceptionInfoDto.From(ev.Exception),
        Id              = ev.Id.RawValue.ToString(),
        TraceId         = TraceIdHelper.FormatTraceId(ev.TraceIdHi, ev.TraceIdLo),
        SpanId          = TraceIdHelper.FormatSpanId(ev.SpanId),
        ServiceName     = ev.ServiceName,
        // Raw first: touching ev.Properties would materialise the dictionary this
        // exists to avoid. Decoders that produce one directly still work.
        Properties      = !ev.RawProperties.IsEmpty ? new EventProps(ev.RawProperties)
                        : ev.Properties is { } map  ? new EventProps(map)
                        : null,
    };
}

/// <summary>
/// The <c>props</c> payload as it reaches the serialiser: either the msgpack bytes the
/// decoder carried through (written straight to JSON by <see cref="EventPropsConverter"/>)
/// or an already-materialised dictionary.
/// </summary>
[JsonConverter(typeof(EventPropsConverter))]
internal readonly struct EventProps
{
    public readonly ReadOnlyMemory<byte>         Raw;
    public readonly Dictionary<string, object?>? Map;

    public EventProps(ReadOnlyMemory<byte> raw)         { Raw = raw;     Map = null; }
    public EventProps(Dictionary<string, object?> map)  { Raw = default; Map = map;  }
}

/// <summary>
/// Writes <see cref="EventProps"/>. The msgpack branch skips the
/// dictionary-then-reserialise round trip that dominated the log-scrolling profile;
/// the dictionary branch delegates to <see cref="DynamicObjectConverter"/> so both
/// produce identical JSON.
/// </summary>
internal sealed class EventPropsConverter : JsonConverter<EventProps>
{
    public override EventProps Read(ref Utf8JsonReader reader, Type t, JsonSerializerOptions o)
        => throw new NotSupportedException();

    public override void Write(Utf8JsonWriter writer, EventProps value, JsonSerializerOptions options)
    {
        if (!value.Raw.IsEmpty)
        {
            Ameto.Core.Serialization.MsgPackJsonTranscoder.WriteMap(writer, value.Raw);
            return;
        }
        if (value.Map is { } map)
        {
            JsonSerializer.Serialize(writer, (object)map, options);
            return;
        }
        writer.WriteStartObject();
        writer.WriteEndObject();
    }
}

/// <summary>JSON-serialisable view of an <see cref="ExceptionInfo"/> tree.</summary>
internal sealed class ExceptionInfoDto
{
    [JsonPropertyName("type")]    public string  Type       { get; init; } = "";
    [JsonPropertyName("message")] public string? Message    { get; init; }
    [JsonPropertyName("stack")]   public string? StackTrace { get; init; }
    [JsonPropertyName("inner")]   public ExceptionInfoDto? Inner { get; init; }

    public static ExceptionInfoDto? From(ExceptionInfo? src)
    {
        if (src is null) return null;
        return new ExceptionInfoDto
        {
            Type       = src.Type,
            Message    = src.Message,
            StackTrace = src.StackTrace,
            Inner      = From(src.Inner),
        };
    }
}
