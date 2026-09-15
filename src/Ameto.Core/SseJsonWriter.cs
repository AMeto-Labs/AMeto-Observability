using System.Buffers;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Ameto.Core;

/// <summary>
/// Per-connection SSE frame writer. Composes each frame as UTF-8 directly into a reusable
/// buffer — <c>data: {json}\n\n</c> — and puts it on the response body. The path this replaced
/// copied every event three times: a UTF-16 JSON string, an interpolated
/// <c>$"data: {json}\n\n"</c> string, and the UTF-16→UTF-8 transcode inside
/// <c>WriteAsync(string)</c>.
///
/// <para>Log-event frames go through <see cref="WriteLogEventAsync"/>, which writes straight
/// from the event (no DTO, no reflection) and COALESCES: frames accumulate until 16 KB, or until
/// the oldest of them has waited <see cref="MaxFrameHold"/> — a deadline a timer enforces, so it
/// holds while the producer is stuck in a synchronous read. Every other frame here — the typed
/// and reflected DTO overloads, keepalives, and the terminal done/query-error frames — is sent
/// the moment it is composed, and carries any coalesced backlog out with it.</para>
///
/// <para>ONE WRITER TO THE BODY AT A TIME. The hold timer sends from a pool thread, so every
/// method that composes into the buffer or touches the body enters <c>_gate</c> first. The
/// caller still owes the usual SSE discipline — one call at a time from its own side — and gets
/// nothing new to think about: the gate is uncontended except in the instant the timer
/// fires.</para>
///
/// <para>Lives in Core rather than beside its first caller because the trace and metric
/// endpoint mappers ship in their own assemblies and do not reference Ameto.Server — the
/// reference runs the other way, so a second copy over there was the only alternative.</para>
///
/// <para>Takes a <see cref="Stream"/>, not an <c>HttpResponse</c>: the response body is all it
/// ever touched, and the parameter type was the only thing that made Ameto.Core need
/// <c>&lt;FrameworkReference Include="Microsoft.AspNetCore.App" /&gt;</c>. That reference is
/// transitive — it landed in the runtimeconfig.json of every console tool that links Core
/// (tools/loggen), so loggen refused to start on a host carrying only the .NET runtime.
/// A writer that frames bytes has no business dragging a web server behind it.</para>
/// </summary>
public sealed class SseJsonWriter : IDisposable
{
    private static readonly byte[] DataPrefix     = "data: "u8.ToArray();
    private static readonly byte[] FrameSuffix    = "\n\n"u8.ToArray();
    private static readonly byte[] DoneFrame      = "event: done\ndata: {}\n\n"u8.ToArray();
    private static readonly byte[] DonePrefix     = "event: done\ndata: "u8.ToArray();
    // NOT "event: error": EventSource dispatches its own connection failures under that
    // name, so a client listening for one would receive the other.
    private static readonly byte[] ErrorPrefix    = "event: query-error\ndata: "u8.ToArray();
    private static readonly byte[] KeepaliveFrame = ": keepalive\n\n"u8.ToArray();

    /// <summary>
    /// How much framed output may sit in the buffer before <see cref="WriteLogEventAsync"/>
    /// puts it on the wire. Only the BUFFERED overload consults it; every other frame on this
    /// writer is still sent the moment it is composed.
    ///
    /// <para>Roughly a TCP window's worth. A 500-row page used to cost 500 writes and 500
    /// flushes — a socket send per row — for a payload that coalesces into some tens of
    /// segments; the buffer turns that into one send per ~16 KB while leaving the frame bytes
    /// themselves untouched. Anything that must not wait (the end of a page, a keepalive, a
    /// terminal frame) goes out through a method that sends unconditionally, which is what
    /// keeps the live tail's silence bound a property of the loop rather than of the
    /// buffer.</para>
    /// </summary>
    private const int FlushThresholdBytes = 16 * 1024;

    /// <summary>
    /// How long a buffered frame may WAIT, whatever the buffer weighs — measured from the moment
    /// the OLDEST frame still buffered was composed.
    ///
    /// <para>The byte threshold alone has no time bound, and the events endpoint writes nothing
    /// else between the first row and the terminal frame. A sparse cold search — forty matches
    /// found over thirty seconds of scanning — would therefore show the client nothing at all
    /// until <c>done</c>, while the Angular store is built to paint progressively as rows
    /// arrive. Coalescing rows that arrive together is the win; holding a row because the next
    /// one has not been found yet is not.</para>
    ///
    /// <para>ENFORCED BY A TIMER, not only when the next row is written. A check on write bounds
    /// the hold by the gap to the NEXT row, and that gap is exactly what is unbounded: a cold
    /// search that finds two rows a millisecond apart and then scans for thirty seconds kept the
    /// second one here for all thirty, because <c>SegmentReader.ReadEventsAsync</c> is
    /// synchronous and the producer never yields. Flushing when the enumerator is about to
    /// suspend would not help for the same reason. The timer is armed when the buffer goes from
    /// empty to non-empty and sends from a pool thread once that frame is old enough.</para>
    ///
    /// <para>The on-write check stays too, for the two cases it serves without a thread hop: a
    /// row that arrives after the stream has been quiet this long goes out at once, and a
    /// steady trickle is sent by the row that finds the oldest frame overdue.</para>
    /// </summary>
    private static readonly TimeSpan MaxFrameHold = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// How soon the hold timer looks again when it fires while a call is inside the writer.
    /// Short, because that call may be composing a row and about to leave the overdue backlog
    /// behind; the look after it is one uncontended probe.
    /// </summary>
    private static readonly TimeSpan HoldRetry = TimeSpan.FromMilliseconds(20);

    private readonly ArrayBufferWriter<byte> _buffer = new(4096);
    private readonly Utf8JsonWriter          _json;
    private readonly Stream                  _body;

    /// <summary>
    /// Admits one writer at a time — the caller's call, or the hold timer's send. A
    /// <see cref="SemaphoreSlim"/> rather than a <c>Lock</c> because it is held across the
    /// body's awaits; entered through <c>Wait(0)</c> first, which is a counter check that
    /// neither allocates nor blocks, so a row pays for it only in the instant the timer fires.
    /// </summary>
    private readonly SemaphoreSlim _gate = new(1, 1);

    /// <summary>When the buffer last went out, for the quiet-stream rule on write.</summary>
    private long _lastSendStamp = Stopwatch.GetTimestamp();

    /// <summary>
    /// When the OLDEST frame still buffered was composed — stamped as the buffer goes from empty
    /// to non-empty. Meaningful only while the buffer holds something; read under the gate.
    /// </summary>
    private long _oldestFrameStamp;

    /// <summary>
    /// The token of the call that last buffered a row, which is the token the hold timer sends
    /// under: the timer's send is that call's send, deferred. On the events endpoint it is the
    /// search deadline, linked to the request, so a disconnect or an expired budget stops a
    /// timer send exactly as it would have stopped the call's own.
    /// </summary>
    private CancellationToken _holdToken;

    /// <summary>Created on the first held frame; most writers (trace streams, the live tail) never hold one.</summary>
    private Timer? _holdTimer;

    private volatile bool _disposed;

    /// <param name="body">The response body to frame into — <c>ctx.Response.Body</c>.</param>
    public SseJsonWriter(Stream body)
    {
        _body = body;
        _json = new Utf8JsonWriter(_buffer);
    }

    /// <summary>
    /// Writes whatever frames are still buffered and flushes the body. A no-op when nothing
    /// is pending, so it is safe to call at the end of every page, poll and error path —
    /// and it MUST be called there: a frame left in this buffer is a row the client sees only
    /// when the hold timer gets to it.
    /// </summary>
    public async ValueTask FlushFramesAsync(CancellationToken ct)
    {
        await EnterAsync(ct).ConfigureAwait(false);
        try     { await SendAsync(ct).ConfigureAwait(false); }
        finally { _gate.Release(); }
    }

    /// <summary>
    /// One <c>data:</c> frame carrying a log event, written straight from the event with no
    /// DTO and no reflection (see <see cref="LogEventJsonWriter"/>), and held in the buffer
    /// with its neighbours until <see cref="FlushThresholdBytes"/> is reached or the oldest of
    /// them has waited <see cref="MaxFrameHold"/>.
    ///
    /// <para>Callers still own the end of the run: finish with <see cref="FlushFramesAsync"/>
    /// (or any terminal frame, which sends the backlog with it). The timer bounds how long a row
    /// waits; it is not a substitute for ending the stream.</para>
    /// </summary>
    public async ValueTask WriteLogEventAsync(LogEvent ev, CancellationToken ct)
    {
        await EnterAsync(ct).ConfigureAwait(false);
        try
        {
            // Where the frame STARTS, so a writer that throws part-way through composing it does
            // not leave half a `data:` line in front of the terminal query-error frame. A client
            // parsing SSE by blank line would read the fragment and the error frame as one.
            int frameStart = _buffer.WrittenCount;
            try
            {
                _buffer.Write(DataPrefix);
                _json.Reset(_buffer);
                LogEventJsonWriter.Write(_json, ev);
                _json.Flush();
                _buffer.Write(FrameSuffix);
            }
            catch
            {
                _buffer.ResetWrittenCount();
                _buffer.Advance(frameStart);      // keep the whole frames, drop the partial one
                throw;
            }

            _holdToken = ct;
            bool first = frameStart == 0;
            long now   = Stopwatch.GetTimestamp();
            if (first) _oldestFrameStamp = now;

            if (_buffer.WrittenCount >= FlushThresholdBytes
                || Stopwatch.GetElapsedTime(first ? _lastSendStamp : _oldestFrameStamp, now) >= MaxFrameHold)
                await SendAsync(ct).ConfigureAwait(false);
            else if (first)
                ArmHoldTimer(MaxFrameHold);
        }
        finally { _gate.Release(); }
    }

    /// <summary>Takes the gate: the uncontended probe first, the awaiting road only if the timer holds it.</summary>
    private ValueTask EnterAsync(CancellationToken ct) =>
        _gate.Wait(0) ? ValueTask.CompletedTask : new ValueTask(_gate.WaitAsync(ct));

    /// <summary>
    /// Puts the buffered bytes on the wire and empties the buffer, keeping its capacity —
    /// one buffer per connection, for the life of the connection. Called with the gate held.
    ///
    /// <para>The buffer is emptied WHETHER OR NOT the send succeeds. Once the bytes have been
    /// offered to the body they belong to it: Kestrel copies them into its pipe inside
    /// <c>WriteAsync</c>, so a cancellation that lands in the flush, or in the flush that
    /// <c>WriteAsync</c> itself performs, fails a send whose rows are already on their way to
    /// the client. Keeping them here would hand them over a second time with the terminal
    /// <c>query-error</c> frame, and the client would list up to 16 KB of rows twice. Losing a
    /// row the body did NOT take is the lesser fault and an honest one: every road that reaches
    /// here after a failed send ends in a frame that already says the results are partial.</para>
    /// </summary>
    private async ValueTask SendAsync(CancellationToken ct)
    {
        if (_buffer.WrittenCount == 0) return;
        try
        {
            await _body.WriteAsync(_buffer.WrittenMemory, ct).ConfigureAwait(false);
            await _body.FlushAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            _buffer.ResetWrittenCount();
            _lastSendStamp = Stopwatch.GetTimestamp();
        }
    }

    // ── The hold deadline ─────────────────────────────────────────────────────

    /// <summary>
    /// Arms the one-shot hold timer <paramref name="due"/> from now. Called with the gate held
    /// (or, for the contended retry, without it — an earlier look is always harmless).
    /// </summary>
    private void ArmHoldTimer(TimeSpan due)
    {
        if (_disposed) return;
        if (_holdTimer is null)
        {
            // Without the request's ExecutionContext: the timer lives as long as the connection
            // and has no business pinning the request's AsyncLocals (logging scopes, activity)
            // for that long, nor flowing them into a pool thread's send.
            bool suppress = !ExecutionContext.IsFlowSuppressed();
            AsyncFlowControl flow = suppress ? ExecutionContext.SuppressFlow() : default;
            try
            {
                _holdTimer = new Timer(static s => ((SseJsonWriter)s!).OnHoldExpired(), this,
                                       Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            }
            finally { if (suppress) flow.Undo(); }
        }
        _holdTimer.Change(due, Timeout.InfiniteTimeSpan);
    }

    /// <summary>
    /// The hold timer fired: send the backlog if its oldest frame is overdue.
    ///
    /// <para>Every exit leaves one of three things true — the buffer is empty, the timer is
    /// armed again, or the caller's token is already cancelled (its loop is ending, and the
    /// terminal frame it owes carries the backlog, or the client is gone and nothing is owed).
    /// That is what makes the deadline a guarantee rather than a hope.</para>
    ///
    /// <para>A failed send is swallowed here, and nothing is lost by that: the bytes were the
    /// body's either way (see <see cref="SendAsync"/>), a cancelled send means the caller's own
    /// token has fired and its loop will see that, and a broken connection fails the caller's
    /// next write too.</para>
    /// </summary>
    private void OnHoldExpired()
    {
        if (!_gate.Wait(0))
        {
            // A call is inside the writer. It may be composing a row and about to leave the
            // backlog behind, so look again shortly rather than queue behind it.
            try { ArmHoldTimer(HoldRetry); } catch { /* disposed under us */ }
            return;
        }

        bool handedOff = false;
        try
        {
            if (_disposed || _buffer.WrittenCount == 0) return;

            TimeSpan age = Stopwatch.GetElapsedTime(_oldestFrameStamp);
            if (age < MaxFrameHold)
            {
                // Fired early (timer granularity), or for a batch that has since gone out and
                // been replaced by a younger one: wait out the remainder of THIS one.
                ArmHoldTimer(MaxFrameHold - age);
                return;
            }

            CancellationToken ct = _holdToken;
            if (ct.IsCancellationRequested) return;

            ValueTask send = SendAsync(ct);
            if (!send.IsCompleted)
            {
                handedOff = true;
                _ = ReleaseAfterAsync(send);
                return;
            }
            send.GetAwaiter().GetResult();          // observe a synchronous failure
        }
        catch { /* see the remarks: nothing to report, nothing lost */ }
        finally { if (!handedOff) _gate.Release(); }
    }

    /// <summary>The hold timer's send, when the body made it wait: the gate is released only once it is done.</summary>
    private async Task ReleaseAfterAsync(ValueTask send)
    {
        try     { await send.ConfigureAwait(false); }
        catch   { /* see OnHoldExpired */ }
        finally { _gate.Release(); }
    }

    // ── Frames that are sent at once ──────────────────────────────────────────

    /// <summary>
    /// Writes one <c>data:</c> frame with the DTO serialised through a SOURCE-GENERATED contract
    /// and sends it, backlog and all. Prefer this overload for any stream that still goes out as
    /// a DTO: it is the one that keeps the reflection-based metadata resolver out of the per-row
    /// path, and out of the trimmed output.
    /// </summary>
    public async Task WriteEventAsync<T>(T dto, JsonTypeInfo<T> typeInfo, CancellationToken ct)
    {
        await EnterAsync(ct).ConfigureAwait(false);
        try
        {
            _buffer.Write(DataPrefix);
            _json.Reset(_buffer);
            JsonSerializer.Serialize(_json, dto, typeInfo);
            _buffer.Write(FrameSuffix);
            await SendAsync(ct).ConfigureAwait(false);
        }
        finally { _gate.Release(); }
    }

    /// <summary>
    /// The reflection door, kept for callers that genuinely have no contract to hand over.
    ///
    /// <para>One does: the log streams serialise a DTO whose attribute values are <c>object</c>,
    /// through a DynamicObjectConverter — a shape a generated contract does not describe, and
    /// converting it is a change to the log path rather than to this one. Everything else should
    /// take the <see cref="JsonTypeInfo{T}"/> overload above, which is why that one exists: when
    /// this writer moved into Core it became the repo-wide SSE contract, and it offered no door
    /// but this one, so every stream was structurally on the reflection resolver.</para>
    /// </summary>
    public async Task WriteEventAsync<T>(T dto, JsonSerializerOptions options, CancellationToken ct)
    {
        await EnterAsync(ct).ConfigureAwait(false);
        try
        {
            _buffer.Write(DataPrefix);
            _json.Reset(_buffer);
            JsonSerializer.Serialize(_json, dto, options);
            _buffer.Write(FrameSuffix);
            await SendAsync(ct).ConfigureAwait(false);
        }
        finally { _gate.Release(); }
    }

    /// <summary>Terminal <c>event: done</c> frame with an empty payload.</summary>
    public async Task WriteDoneAsync(CancellationToken ct)
    {
        await EnterAsync(ct).ConfigureAwait(false);
        try
        {
            _buffer.Write(DoneFrame);
            await SendAsync(ct).ConfigureAwait(false);
        }
        finally { _gate.Release(); }
    }

    /// <summary>
    /// Terminal <c>event: done</c> frame that also says WHICH ending it was.
    ///
    /// <para>`done` on its own is a single name for two different outcomes: the window was read
    /// to its floor, and the caller's row ceiling was reached with the window still unread. The
    /// Angular client can tell them apart only by counting its own rows against the <c>max</c> it
    /// asked for; every other consumer — a script, a second UI, a future export — cannot tell them
    /// apart at all, which is exactly the conflation the capped/short-page and stalled-cursor
    /// signals exist to remove.</para>
    ///
    /// <para>Backward compatible on purpose: the event NAME is unchanged, so a client that only
    /// listens for <c>done</c> and ignores the payload (which is what the Angular client does)
    /// keeps treating either ending as a normal completion. The distinction is additive, in
    /// fields, for consumers that want it.</para>
    /// </summary>
    /// <param name="complete">True when the whole requested window was read out.</param>
    /// <param name="reason">Machine-readable ending: <c>exhausted</c> or <c>max-rows</c>.</param>
    /// <param name="truncatedBy">
    /// What ELSE went wrong on the way to that ending, or null when nothing did — written as
    /// <c>truncatedBy</c> and omitted entirely when null.
    ///
    /// <para>It exists because "your row ceiling stopped me" and "I could not read part of the
    /// window" are both true at once, routinely, and a caller told only the first has no way to
    /// discover the second: it looks exactly like the ordinary, healthy ending. Its absence is
    /// therefore a positive claim — this page ceiling is the ONLY reason the list is short.</para>
    /// </param>
    public async Task WriteDoneAsync(bool complete, string reason, string? truncatedBy, CancellationToken ct)
    {
        await EnterAsync(ct).ConfigureAwait(false);
        try
        {
            _buffer.Write(DonePrefix);
            _json.Reset(_buffer);
            _json.WriteStartObject();
            _json.WriteBoolean("complete", complete);
            _json.WriteString("reason", reason);
            if (truncatedBy is not null) _json.WriteString("truncatedBy", truncatedBy);
            _json.WriteEndObject();
            _json.Flush();
            _buffer.Write(FrameSuffix);
            await SendAsync(ct).ConfigureAwait(false);
        }
        finally { _gate.Release(); }
    }

    /// <summary>
    /// Terminal <c>event: error</c> frame. The status line is long gone by the time a query
    /// fails mid-stream, so this is the only way to tell the client something went wrong —
    /// without it the stream simply stopped, indistinguishable from "no more results".
    /// </summary>
    /// <param name="truncatedBy">
    /// Optional machine-readable cause, written beside the sentence. A client that reads only
    /// <c>error</c> is unaffected; one that reads this can treat the same fault the same way
    /// whichever terminal frame carried it. Without it the error road offered nothing but English,
    /// so a page could not tell a lost segment from a spent deadline and had to treat every error
    /// alike — which is how one dead file became a blocking banner on one row ceiling and a grey
    /// suffix on another.
    /// </param>
    public async Task WriteErrorAsync(string message, CancellationToken ct, string? truncatedBy = null)
    {
        await EnterAsync(ct).ConfigureAwait(false);
        try
        {
            _buffer.Write(ErrorPrefix);
            _json.Reset(_buffer);
            _json.WriteStartObject();
            _json.WriteString("error", message);
            if (truncatedBy is not null) _json.WriteString("truncatedBy", truncatedBy);
            _json.WriteEndObject();
            _json.Flush();
            _buffer.Write(FrameSuffix);
            await SendAsync(ct).ConfigureAwait(false);
        }
        finally { _gate.Release(); }
    }

    /// <summary>Comment-only keepalive frame (ignored by EventSource clients).</summary>
    public async Task WriteKeepaliveAsync(CancellationToken ct)
    {
        await EnterAsync(ct).ConfigureAwait(false);
        try
        {
            _buffer.Write(KeepaliveFrame);
            await SendAsync(ct).ConfigureAwait(false);
        }
        finally { _gate.Release(); }
    }

    /// <summary>
    /// Stops the hold timer; a frame still buffered is dropped, which is only ever the case on
    /// the client-disconnect road (every other ending writes a terminal frame first, and that
    /// frame waits for any timer send in progress). A timer that fires after this finds the flag
    /// and leaves the body alone — the response may already be complete by then.
    /// </summary>
    public void Dispose()
    {
        _disposed = true;
        _holdTimer?.Dispose();
        _json.Dispose();
    }
}
