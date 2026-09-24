using Microsoft.AspNetCore.Http;
using Ameto.Core;

namespace Ameto.Metrics;

/// <summary>
/// The metric query API's answer when the metric store has CLOSED (#95): 503 with a sentence, where
/// it used to be 200 with no series — "no data in this window" about data still on disk. Kestrel
/// outlives the hosted services, so a query really does arrive after the engine's teardown, and the
/// engine answers it empty so as not to throw out of the middle of a response; this is where the
/// endpoint says what that empty means. The trace API's <c>TraceStoreGate</c> is the same rule —
/// two copies because the two assemblies share only Ameto.Core, which deliberately carries no
/// ASP.NET Core reference.
///
/// <para><b>Only CLOSED is refused, not LOADING.</b> Before the background cold scan has run, a
/// query answers from the hot tier; that window is this engine's documented design, and a page shown
/// in it catches up on its next refresh. The alert evaluator, which acts on an answer, skips a
/// loading store itself.</para>
///
/// <para><b>Where it is asked.</b> A buffered endpoint asks AFTER its read — availability only moves
/// forward, so a read that met the closed fence is always followed by a Closed answer — and the raw
/// series stream asks BEFORE it starts, because once it has begun there is no status left to
/// change. The names, catalog, label and exemplar endpoints are not asked: they read in-memory
/// metadata the teardown does not close (the name list's cold half is a subset of it — the catalog
/// is seeded from the cold segments), and they still answer truly.</para>
/// </summary>
internal static class MetricStoreGate
{
    /// <summary>The refusal: one instance, a constant body, nothing allocated to send it.</summary>
    internal static IResult Closed { get; } = new ClosedResult();

    /// <summary>True when <paramref name="store"/> has closed and the endpoint must answer <see cref="Closed"/>.</summary>
    internal static bool IsClosed(IQueryAvailability store) => store.Availability == QueryAvailability.Closed;

    private sealed class ClosedResult : IResult
    {
        /// <summary><c>{"error": …}</c>, the body every refusal on this server carries.</summary>
        private static readonly byte[] Body =
            """{"error":"The metric store has shut down and cannot answer. Metrics are available again once the server has restarted."}"""u8.ToArray();

        public Task ExecuteAsync(HttpContext ctx)
        {
            // No Retry-After: Closed is final for this process — see TraceStoreGate.
            ctx.Response.StatusCode         = StatusCodes.Status503ServiceUnavailable;
            ctx.Response.ContentType        = "application/json; charset=utf-8";
            return ctx.Response.Body.WriteAsync(Body, 0, Body.Length, ctx.RequestAborted);
        }
    }
}
