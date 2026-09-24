using Ameto.Core;

namespace Ameto.Server;

/// <summary>
/// The log search and count API's answer when the log store has CLOSED (#95): 503 with a sentence.
/// Kestrel outlives the hosted services, so a request really does arrive after the log engine's
/// teardown — and past its last step the engine's reader snapshot throws, so the search answered
/// with a stream that died and the counts with a 500, neither saying why. The trace and metric
/// APIs' <c>TraceStoreGate</c> / <c>MetricStoreGate</c> are the same rule.
///
/// <para><b>Only CLOSED is refused, not LOADING:</b> before the catalog scan has ended a search
/// sees the hot tier and the segments registered so far, which is how the engine has always
/// served its first seconds. The alert evaluator, which acts on a count, skips a loading store
/// itself.</para>
/// </summary>
internal static class LogStoreGate
{
    /// <summary>The refusal: one instance, a constant body, nothing allocated to send it.</summary>
    internal static IResult Closed { get; } = new ClosedResult();

    /// <summary>True when <paramref name="store"/> has closed and the endpoint must answer <see cref="Closed"/>.</summary>
    internal static bool IsClosed(IQueryAvailability store) => store.Availability == QueryAvailability.Closed;

    private sealed class ClosedResult : IResult
    {
        /// <summary><c>{"error": …}</c>, the body every refusal on this server carries.</summary>
        private static readonly byte[] Body =
            """{"error":"The log store has shut down and cannot answer. Logs are available again once the server has restarted."}"""u8.ToArray();

        public Task ExecuteAsync(HttpContext ctx)
        {
            ctx.Response.StatusCode         = StatusCodes.Status503ServiceUnavailable;
            ctx.Response.Headers.RetryAfter = "5";
            ctx.Response.ContentType        = "application/json; charset=utf-8";
            return ctx.Response.Body.WriteAsync(Body, 0, Body.Length, ctx.RequestAborted);
        }
    }
}
