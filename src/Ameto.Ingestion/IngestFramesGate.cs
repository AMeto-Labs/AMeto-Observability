using System.Threading.Tasks.Sources;
using Microsoft.AspNetCore.Http;

namespace Ameto.Ingestion;

/// <summary>
/// Lets a receiver go on only once every async frame above it has suspended on it (round-6 review R6-H2).
///
/// <para>A receiver's first suspension used to be a yield (round 5, H-G): its box is made while it owns nothing.
/// But the yield queued the receiver, and the frames above it — ASP.NET Core's <c>EndpointMiddleware</c>, the path
/// base's, the app's own middleware — then suspended on it, each boxing its own state machine and registering its
/// continuation. A frame whose box or registration ran out of memory threw up to Kestrel, which answered 500,
/// disposed the request's <see cref="HttpContext"/> and read the NEXT request on the connection into the same
/// object — while the queued receiver ran on in it: it wrote its batch after the 500 (a Seq client's retry stored
/// it twice), wrote its answer into the next request's response (the wrong counts, two answers concatenated, bytes
/// of a pooled header block), or broke that request's connection.</para>
///
/// <para>The pipeline's first middleware (<see cref="Run"/>, frame-free: it awaits nothing) sets a gate on every
/// POST and calls the rest of the pipeline. The receiver's first suspension is its wait for the gate
/// (<see cref="PassAsync"/>), reached synchronously inside that call — every middleware between is synchronous up
/// to the next one for a receiver's request — so when the call comes back incomplete, every frame on the way down
/// has suspended, registered, on the frame below it, and the gate opens: none of them can fail at its suspension
/// any more. When the call throws, or comes back completed or faulted — a frame failed, or the request was answered
/// without the receiver — the gate closes, and the receiver returns touching nothing of the request.</para>
///
/// <para>What remains, stated (docs/API.md): Kestrel's own await of the pipeline, above the gate, and a request that
/// meets an async hop on its way down — routing's matcher still being built, on the first requests after the start
/// — reaches the receiver after the gate has opened. Out of memory at exactly that registration still answers 500
/// beside a receiver that goes on.</para>
///
/// <para>One allocation per POST: this object, its own value-task source. Its continuation is run on the pool, not
/// in <see cref="Run"/>'s frame, so the receiver goes on after the pipeline's call has returned up to Kestrel.</para>
/// </summary>
public sealed class IngestFramesGate : IValueTaskSource<bool>
{
    private ManualResetValueTaskSourceCore<bool> _core;
    private int _decided;

    private IngestFramesGate() => _core.RunContinuationsAsynchronously = true;

    /// <summary>
    /// The pipeline's first middleware, for a POST: sets the gate, calls the rest of the pipeline, and opens the gate
    /// when the call comes back incomplete — closes it otherwise (see the type). Frame-free: it awaits nothing.
    /// </summary>
    public static Task Run(HttpContext ctx, RequestDelegate next)
    {
        var gate = new IngestFramesGate();
        ctx.Features.Set(gate);
        Task below;
        try { below = next(ctx); }
        catch
        {
            gate.Decide(open: false);
            throw;
        }
        gate.Decide(open: !below.IsCompleted);
        return below;
    }

    /// <summary>
    /// A receiver's first suspension, in place of a yield: true once every frame above it has suspended — it may go
    /// on; false when one of them failed, or the request was answered without it — it must touch nothing of the
    /// request, and return. With no gate (a pipeline without <see cref="Run"/>, such as a test's), a yield and true
    /// (<see cref="Yielded"/>).
    /// </summary>
    public static ValueTask<bool> PassAsync(HttpContext ctx) =>
        ctx.Features.Get<IngestFramesGate>() is { } gate ? new ValueTask<bool>(gate, gate._core.Version) : new ValueTask<bool>(Yielded.Instance, 0);

    /// <summary>
    /// With no gate: a suspension that always happens, as <see cref="Task.Yield"/>'s does — never complete until its
    /// continuation is registered, which queues it to the pool. It was a yield inside an async method of its own, which
    /// is not one: the pool could run that yield's continuation, and complete the method, before the receiver looked,
    /// and the receiver then went on unsuspended — its first suspension its body read, waiting for an inflate slot,
    /// which a failure there left waiting to take a freed slot for nobody (CI, 2 cores). Stateless: one for every use.
    /// </summary>
    private sealed class Yielded : IValueTaskSource<bool>
    {
        internal static readonly Yielded Instance = new();

        public ValueTaskSourceStatus GetStatus(short token) => ValueTaskSourceStatus.Pending;

        public bool GetResult(short token) => true;

        /// <summary>
        /// Queued to the pool, with the execution context when asked to flow it. A scheduling context is not looked for:
        /// ASP.NET Core sets none.
        /// </summary>
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            if ((flags & ValueTaskSourceOnCompletedFlags.FlowExecutionContext) != 0)
                ThreadPool.QueueUserWorkItem(continuation, state, preferLocal: false);
            else
                ThreadPool.UnsafeQueueUserWorkItem(continuation, state, preferLocal: false);
        }
    }

    /// <summary>
    /// Once. A continuation that cannot be queued (out of memory) is not resumed: its request ends at its client's
    /// timeout, and nothing of it was taken.
    /// </summary>
    private void Decide(bool open)
    {
        if (Interlocked.Exchange(ref _decided, 1) != 0) return;
        try { _core.SetResult(open); }
        catch (Exception) { /* see above */ }
    }

    bool IValueTaskSource<bool>.GetResult(short token) => _core.GetResult(token);

    ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource<bool>.OnCompleted(Action<object?> continuation, object? state, short token,
                                            ValueTaskSourceOnCompletedFlags flags) =>
        _core.OnCompleted(continuation, state, token, flags);
}
