using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Core;

namespace Ameto.Tracing;

/// <summary>
/// The trace read API's answer when the trace store has CLOSED (#95): 503 with a sentence, where it
/// used to be 200 with an empty body — "no traces in this window", "no services", a flame graph that
/// 404s as "trace not found" — about data that is still on disk. Kestrel outlives the hosted
/// services, so a request really does arrive after the engine's teardown; the engine answers it
/// empty so that it cannot throw out of the middle of a response, and this is where the endpoint
/// says what that empty means.
///
/// <para><b>Only CLOSED is refused, not LOADING.</b> A store still scanning its cold tier after a
/// start answers from its hot tier, and that is the design, not an accident of it: ingest and
/// queries work from second zero and cold data joins the answers as soon as the scan completes
/// (<c>TraceCompactionWorker</c>). A page shown for those seconds shows fewer rows, and the next
/// refresh shows all of them; the alert evaluator, which ACTS on an answer, is the caller that must
/// not take a part for the whole, and it skips a loading store itself.</para>
///
/// <para><b>Where it is asked.</b> A buffered endpoint asks AFTER its read, just before it writes:
/// availability only moves forward, so a read that met the closed door is always followed by a
/// Closed answer here, and the check needs no before-and-after pair. A streamed endpoint (the trace
/// detail, the flame graph, compare, the two SSE streams) asks BEFORE it starts — once a status line
/// has gone there is nothing left to change — and a store closing inside one of those is the
/// shutdown race it always was.</para>
///
/// <para>Asking costs a volatile read or two on the engine; the refusal writes a constant body.
/// Neither allocates on the path that answers.</para>
/// </summary>
internal static class TraceStoreGate
{
    /// <summary><c>{"error": …}</c>, the body every refusal on this server carries (see <c>QueryGuard</c>'s 503).</summary>
    private static readonly byte[] ClosedBody =
        """{"error":"The trace store has shut down and cannot answer. Traces are available again once the server has restarted."}"""u8.ToArray();

    /// <summary>
    /// Null when <paramref name="store"/> can answer; otherwise the 503, already begun — the caller
    /// awaits it and returns.
    /// </summary>
    internal static Task? RefuseIfClosed(HttpContext ctx, IQueryAvailability store) =>
        store.Availability == QueryAvailability.Closed ? WriteClosedAsync(ctx) : null;

    /// <summary>The same, for an endpoint reading two stores (they are one engine in the product).</summary>
    internal static Task? RefuseIfClosed(HttpContext ctx, IQueryAvailability first, IQueryAvailability second) =>
        first.Availability == QueryAvailability.Closed || second.Availability == QueryAvailability.Closed
            ? WriteClosedAsync(ctx)
            : null;

    /// <summary>The same, for a handler that has not resolved its store yet.</summary>
    internal static Task? RefuseIfClosed<TStore>(HttpContext ctx) where TStore : class, IQueryAvailability =>
        RefuseIfClosed(ctx, ctx.RequestServices.GetRequiredService<TStore>());

    private static Task WriteClosedAsync(HttpContext ctx)
    {
        ctx.Response.StatusCode         = StatusCodes.Status503ServiceUnavailable;
        ctx.Response.Headers.RetryAfter = "5";
        ctx.Response.ContentType        = "application/json; charset=utf-8";
        return ctx.Response.Body.WriteAsync(ClosedBody, 0, ClosedBody.Length, ctx.RequestAborted);
    }
}
