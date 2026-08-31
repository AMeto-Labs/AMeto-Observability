namespace Ameto.Tracing.TraceQL;

/// <summary>
/// One page of TraceQL results, plus whether the scan behind it was cut short.
/// </summary>
/// <param name="Rows">Newest-first, at most the requested limit.</param>
/// <param name="Capped">
/// True when the span scan stopped short of the window, or when more traces matched than the
/// limit let through. False means the whole window was read and this is all of it.
/// </param>
/// <param name="ScanFloorNano">
/// Where the work behind this page STOPPED, as an EXCLUSIVE lower bound: every trace whose start
/// is STRICTLY GREATER than this was decided — grouped into <see cref="Rows"/>, rejected by the
/// predicate, or cut by the limit. <c>long.MinValue</c> means the window was read out, which is
/// exactly when <see cref="Capped"/> is false.
///
/// <para>NOT a minimum over the spans that came back. <c>SearchSpansAsync</c> serves the hot tier
/// first and yield-breaks on a GLOBAL <c>yielded &gt;= limit</c> while walking segments ordered by
/// MaxStartNano, so the oldest span it handed over says nothing about how deep it looked: one
/// wide segment, or one late hot span, puts that minimum arbitrarily far below the last thing
/// anyone actually read. It comes from <see cref="SpanScanFloor"/>, which the scan fills in as it
/// stops.</para>
///
/// <para>What it is FOR is the page with no rows at all: an OR-chain extracts no hints, so the
/// scan returns the newest <c>limit*10</c> spans whatever the query says and the predicate can
/// reject every one of them. That page has no row to derive a cursor from, and this is the only
/// cursor it has.</para>
/// </param>
public readonly record struct TraceQueryPage(List<TraceRowDto> Rows, bool Capped, long ScanFloorNano);

/// <summary>Hints extracted from the AST to accelerate <see cref="ITraceProvider.SearchSpansAsync"/>.</summary>
public sealed class SearchHints
{
    public string?          ServiceName      { get; set; }
    public SpanStatusCode?  Status           { get; set; }
    public long?            MinDurationNanos { get; set; }
    public long?            MaxDurationNanos { get; set; }
    public short?           HttpStatusCode   { get; set; }
    /// <summary>Necessary attribute conditions (AND-chain only) — drive per-block bloom skip.</summary>
    public List<AttrHint>?  AttrHints        { get; set; }
}

/// <summary>
/// Executes a parsed TraceQL predicate against <see cref="ITraceProvider"/>.
///
/// Strategy:
///   1. <see cref="ExtractHints"/> walks the AND-chain and extracts indexed predicates
///      (service, status, duration range, http status code).
///   2. These hints are passed to <c>SearchSpansAsync</c> — the storage engine uses its
///      service-name index and block-skip logic to avoid reading irrelevant data.
///   3. Returned spans are post-filtered with the full AST predicate (handles attribute
///      predicates not covered by the index).
///   4. Matching spans are grouped by TraceId and returned as <see cref="TraceRowDto"/> list.
/// </summary>
public static class TraceQLExecutor
{
    // ── Hint extraction ────────────────────────────────────────────────────────

    public static SearchHints ExtractHints(SpanPredicate pred)
    {
        var h = new SearchHints();
        Collect(pred, h);
        return h;
    }

    private static void Collect(SpanPredicate pred, SearchHints h)
    {
        switch (pred)
        {
            // Only AND propagates hints — OR is too broad
            case AndPredicate and:
                Collect(and.Left,  h);
                Collect(and.Right, h);
                break;

            case ServicePredicate svc when svc.Op == TraceQLOp.Eq:
                h.ServiceName ??= svc.Value;
                break;

            case StatusPredicate st when st.Op == TraceQLOp.Eq:
                h.Status ??= st.Value;
                break;

            case DurationPredicate dur:
                if (dur.Op is TraceQLOp.Gt or TraceQLOp.Gte)
                {
                    long min = dur.Op == TraceQLOp.Gt ? dur.Nanos + 1 : dur.Nanos;
                    if (h.MinDurationNanos is null || min > h.MinDurationNanos)
                        h.MinDurationNanos = min;
                }
                else if (dur.Op is TraceQLOp.Lt or TraceQLOp.Lte)
                {
                    long max = dur.Op == TraceQLOp.Lt ? dur.Nanos - 1 : dur.Nanos;
                    if (h.MaxDurationNanos is null || max < h.MaxDurationNanos)
                        h.MaxDurationNanos = max;
                }
                break;

            case HttpStatusCodePredicate hsc when hsc.Op == TraceQLOp.Eq:
                h.HttpStatusCode ??= hsc.Code;
                break;

            // If the query is simply { .http.status_code = 500 } via AttributePredicate fallthrough
            case AttributePredicate attr
                when attr.Op == TraceQLOp.Eq
                  && attr.Key is "http.status_code" or "http.response.status_code"
                  && attr.Value.IsNumber
                  && attr.Value.Number is >= 100 and <= 999:
                h.HttpStatusCode ??= (short)attr.Value.Number;
                AddAttrHint(h, attr);
                break;

            // Every attribute predicate in an AND-chain requires the KEY to exist
            // on the span (a missing attribute never matches, whatever the op);
            // string equality additionally pins the value. Both drive the
            // per-block bloom skip in the cold reader.
            case AttributePredicate anyAttr:
                AddAttrHint(h, anyAttr);
                break;
        }
    }

    private static void AddAttrHint(SearchHints h, AttributePredicate attr)
    {
        // Equality on a string value → key+value probe (bloom stores lowercased
        // values because TraceQL string comparison is OrdinalIgnoreCase). Numeric
        // equality matches across representations (long/double/numeric string),
        // so only the key-presence probe is safe there.
        string? lower = attr.Op == TraceQLOp.Eq && !attr.Value.IsNumber
            ? attr.Value.StringVal?.ToLowerInvariant()
            : null;
        (h.AttrHints ??= new List<AttrHint>(2)).Add(new AttrHint(attr.Key, lower));
    }

    // ── Execution ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns one <see cref="TraceRowDto"/> per trace where at least one span matches
    /// <paramref name="predicate"/>, plus whether the underlying scan was CAPPED.
    ///
    /// <para>A short result does NOT mean the window is exhausted, and a caller paging backwards
    /// must not read it that way. The span fetch is bounded at ten times the trace limit and the
    /// predicate is applied AFTERWARDS: an OR-predicate extracts no hints at all, so the scan
    /// returns the newest spans in the window regardless of the query and most are then thrown
    /// away; and one span-rich service turns those spans into far fewer traces than spans. Both
    /// produce a page far short of <paramref name="limit"/> with plenty more matching traces
    /// deeper in the window.</para>
    /// </summary>
    public static async Task<TraceQueryPage> ExecuteAsync(
        ITraceProvider  provider,
        SpanPredicate   predicate,
        DateTimeOffset  from,
        DateTimeOffset  to,
        int             limit,
        CancellationToken ct)
    {
        var hints = ExtractHints(predicate);

        // Fetch spans using indexed filters; multiply limit for grouping headroom
        int spanLimit = limit * 10;
        var spans     = new List<SpanRecord>();

        // The scan reports where it STOPPED. Nothing here may reconstruct that from the spans it
        // returned — see TraceQueryPage.ScanFloorNano for the two mechanisms that make any such
        // reconstruction wrong, and for what a pager does with a floor that is too low.
        var scanFloor = new SpanScanFloor();
        await foreach (var s in provider.SearchSpansAsync(
            from, to,
            serviceName      : hints.ServiceName,
            status           : hints.Status,
            minDurationNanos : hints.MinDurationNanos,
            maxDurationNanos : hints.MaxDurationNanos,
            httpStatusCode   : hints.HttpStatusCode,
            limit            : spanLimit,
            attrHints        : hints.AttrHints,
            scanFloor        : scanFloor,
            ct               : ct))
            spans.Add(s);

        // `spans.Count >= spanLimit` is deliberately NOT the test any more. It over-reports the
        // one case that has an honest ending — a window holding exactly spanLimit matching spans
        // was read OUT, not cut off — and it under-reports the ones that matter, because a tier
        // can evict a thousand matches and still hand back fewer than spanLimit spans once the
        // dedupe has had them. The scan says which it was.
        long scanFloorNano = scanFloor.FloorNano;
        bool capped        = scanFloor.Truncated;

        // Post-filter + group by trace
        var traces = new Dictionary<TraceId, List<SpanRecord>>(capacity: spans.Count / 4);
        foreach (var s in spans)
        {
            if (!predicate.Evaluate(s)) continue;
            if (!traces.TryGetValue(s.TraceId, out var list))
            {
                list = new List<SpanRecord>(4);
                traces[s.TraceId] = list;
            }
            list.Add(s);
        }

        // Sort newest-first BEFORE truncating, on the cheap key, and only then build rows.
        // The old order — take the first `limit` traces in dictionary-encounter order, then
        // sort — returned a page whose oldest row was the boundary of nothing: encounter
        // order is only roughly newest-first (grouping by trace shuffles it), so a caller
        // paging on "everything older than my oldest row" skipped real traces. Building rows
        // after the cut also keeps BuildRow's allocations (a HashSet, id strings, an array)
        // to the `limit` survivors instead of every matching trace in the window — the
        // difference compounds page by page under the client's load-more.
        //
        // The trace-id tiebreak makes equal-millisecond boundaries deterministic: without it,
        // which of several same-timestamp traces survive the cut is unstable sort luck, and
        // the client's overlapping cursor could then see a different winner on every page.
        var groups = new List<(long StartNano, TraceId Id, List<SpanRecord> Spans)>(traces.Count);
        foreach (var (id, traceSpans) in traces)
        {
            long start = long.MaxValue;
            foreach (var sp in traceSpans)
                if (sp.StartTimeUnixNano < start) start = sp.StartTimeUnixNano;
            groups.Add((start, id, traceSpans));
        }
        groups.Sort(static (a, b) =>
        {
            int byTime = b.StartNano.CompareTo(a.StartNano);
            return byTime != 0 ? byTime : b.Id.CompareTo(a.Id);
        });
        if (groups.Count > limit)
        {
            capped = true;
            // The cut is a SECOND place this page stopped short, and floors compose by MAXIMUM:
            // each names a height above which everything was dealt with, so the highest is the
            // only one all the others sit below. Taken on the group key the sort ran on, not on
            // the row's root start — the two can differ when a matching span predates its own
            // root, and it is the key that decided which side of the cut a trace landed.
            scanFloorNano = Math.Max(scanFloorNano, groups[Math.Max(0, limit - 1)].StartNano);
            groups.RemoveRange(limit, groups.Count - limit);
        }

        var result = new List<TraceRowDto>(groups.Count);
        foreach (var (_, _, traceSpans) in groups)
            result.Add(BuildRow(traceSpans));
        return new TraceQueryPage(result, capped, scanFloorNano);
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static TraceRowDto BuildRow(List<SpanRecord> spans)
    {
        SpanRecord? root = null;
        bool hasErr = false;
        var services = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var s in spans)
        {
            if (s.ParentSpanId.IsEmpty) root = s;
            if (s.Status == SpanStatusCode.Error) hasErr = true;
            services.Add(s.ServiceName);
        }
        root ??= spans.MinBy(s => s.StartTimeUnixNano)!;

        return new TraceRowDto
        {
            TraceId           = root.TraceId.ToString(),
            SpanId            = root.SpanId.ToString(),
            Name              = root.Name,
            ServiceName       = root.ServiceName,
            Services          = [.. services],
            Status            = hasErr ? "Error" : root.Status.ToString(),
            HttpMethod        = GetAttr(root.Attributes, "http.request.method", "http.method"),
            HttpPath          = GetAttr(root.Attributes, "url.path", "http.target", "http.route"),
            HttpStatusCode    = root.HttpStatusCode != 0 ? root.HttpStatusCode : null,
            StartTimeUnixNano = root.StartTimeUnixNano,
            DurationNanos     = root.DurationNanos,
            SpanCount         = spans.Count,
        };
    }

    private static string GetAttr(IReadOnlyDictionary<string, object?>? attrs, params string[] keys)
    {
        if (attrs is null) return string.Empty;
        foreach (var k in keys)
            if (attrs.TryGetValue(k, out var v) && v is not null)
                return v.ToString() ?? string.Empty;
        return string.Empty;
    }
}
