namespace Ameto.Tracing.TraceQL;

/// <summary>Hints extracted from the AST to accelerate <see cref="ITraceProvider.SearchSpansAsync"/>.</summary>
public sealed class SearchHints
{
    public string?          ServiceName      { get; set; }
    public SpanStatusCode?  Status           { get; set; }
    public long?            MinDurationNanos { get; set; }
    public long?            MaxDurationNanos { get; set; }
    public short?           HttpStatusCode   { get; set; }
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
                break;
        }
    }

    // ── Execution ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns one <see cref="TraceRowDto"/> per trace where at least one span matches
    /// <paramref name="predicate"/>.
    /// </summary>
    public static async Task<List<TraceRowDto>> ExecuteAsync(
        ITraceProvider  provider,
        SpanPredicate   predicate,
        DateTimeOffset  from,
        DateTimeOffset  to,
        int             limit,
        CancellationToken ct)
    {
        var hints = ExtractHints(predicate);

        // Fetch spans using indexed filters; multiply limit for grouping headroom
        var spans = new List<SpanRecord>();
        await foreach (var s in provider.SearchSpansAsync(
            from, to,
            serviceName      : hints.ServiceName,
            status           : hints.Status,
            minDurationNanos : hints.MinDurationNanos,
            maxDurationNanos : hints.MaxDurationNanos,
            httpStatusCode   : hints.HttpStatusCode,
            limit            : limit * 10,
            ct               : ct))
        {
            spans.Add(s);
        }

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

        var result = new List<TraceRowDto>(Math.Min(limit, traces.Count));
        foreach (var (_, traceSpans) in traces)
        {
            if (result.Count >= limit) break;
            result.Add(BuildRow(traceSpans));
        }

        // Sort newest-first
        result.Sort(static (a, b) => b.StartTimeUnixNano.CompareTo(a.StartTimeUnixNano));
        return result;
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
