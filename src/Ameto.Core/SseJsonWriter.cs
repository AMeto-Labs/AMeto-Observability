using System.Buffers;
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
/// <para>Log-event frames go through <see cref="WriteLogEventAsync"/>, or through
/// <see cref="WriteLogEventsAsync"/> for a whole query's worth. Both write straight from the
/// event (no DTO, no reflection) and COALESCE: frames accumulate until 16 KB, until a row finds
/// the oldest of them <see cref="MaxFrameHold"/> old, or until the source of the rows makes the
/// caller wait. Every other frame here — the DTO overload, keepalives, and the terminal
/// done/query-error frames — is sent the moment it is composed, and carries any coalesced
/// backlog out with it.</para>
///
/// <para>A ROW IS WRITTEN WHEN ITS FRAME IS IN THE BUFFER, not when it reaches the socket: from
/// then on delivering it is this writer's job, and <see cref="RowsWritten"/>,
/// <see cref="LastRowId"/> and <see cref="LastRowTimestampTicks"/> say how far that has got. A
/// caller that pages by keyset (the live tail) continues from there, so a row whose send was
/// refused — still buffered, and on its way out with the next frame — is never asked for, and
/// written, a second time.</para>
///
/// <para>NOTHING TOUCHES THE BODY BUT THE CALLER'S OWN CALL. Every send starts inside a public
/// method and has ended, one way or the other, by the time that method's task completes. There
/// is no timer and no background send, so there is nothing to serialise against the caller,
/// nothing for <see cref="Dispose"/> to cancel or wait for, and no way for a write to reach a
/// response whose request has already completed. The caller owes the usual SSE discipline — one
/// call at a time — and that is the whole of the concurrency contract.</para>
///
/// <para>THE BUFFER HOLDS WHOLE FRAMES ONLY, between any two calls. Every method that composes
/// into it — the row frames, the DTO overload, and the done and query-error frames — cuts a
/// frame that throws part-way back out (<see cref="DiscardFrameFrom"/>), so the frames buffered
/// before it survive and the next frame never lands behind half a line. That is what lets every
/// frame APPEND to the backlog, where each one used to reset the buffer before rows
/// coalesced.</para>
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
    /// puts it on the wire. Only the BUFFERED road consults it; every other frame on this
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
    /// How long a buffered frame may wait before the NEXT ROW WRITTEN sends it: measured from the
    /// moment the oldest frame still buffered was composed, or — for the first frame of a batch —
    /// from the last send.
    ///
    /// <para>The byte threshold alone has no time bound, and the events endpoint writes nothing
    /// else between the first row and the terminal frame. A sparse cold search — forty matches
    /// found over thirty seconds of scanning — would show the client nothing at all until
    /// <c>done</c>, while the Angular store is built to paint progressively as rows arrive.
    /// Coalescing rows that arrive together is the win; holding a row because the next one has
    /// not been found yet is not.</para>
    ///
    /// <para>Two rules on write, and no clock of the writer's own. A row that arrives after the
    /// stream has been quiet this long goes out at once, so a search that finds its rows one at
    /// a time shows each as it is found. A row that finds the oldest buffered frame this old
    /// sends the backlog, so a steady trickle is never more than one row late.</para>
    ///
    /// <para>Neither rule can send a row that nothing is written after. That is
    /// <see cref="WriteLogEventsAsync"/>'s job: it sends the backlog whenever its source makes
    /// it wait. A source that computes its next row SYNCHRONOUSLY gives the writer no moment to
    /// send in, and every stage of the query executor does, so the executor makes its scan wait
    /// on purpose, at least every 50 ms of synchronous work (<c>Ameto.Query.ScanPace</c>). A
    /// source that neither waits nor paces keeps the tail of each burst until the next row, the
    /// 16 KB, or the terminal frame. A timer used to cover that stretch from this side and was
    /// taken out: its send ran on a pool thread as a second writer to the body, so it needed a
    /// gate every row paid for, it could outlive <see cref="Dispose"/> and the request, and it
    /// re-armed every 20 ms through a stalled send.</para>
    /// </summary>
    private static readonly TimeSpan MaxFrameHold = TimeSpan.FromMilliseconds(100);

    private readonly ArrayBufferWriter<byte> _buffer = new(4096);
    private readonly Utf8JsonWriter          _json;
    private readonly Stream                  _body;

    /// <summary>
    /// The writer of the <c>done</c> and <c>query-error</c> payloads: <see cref="_json"/> itself, or
    /// one over the host's encoder when the caller gave its options — see the constructor.
    /// </summary>
    private readonly Utf8JsonWriter          _terminalJson;

    /// <summary>The clock both hold rules read. <see cref="TimeProvider.System"/> outside tests.</summary>
    private readonly TimeProvider _time;

    /// <summary><see cref="MaxFrameHold"/> in <see cref="_time"/>'s timestamp units, so a row compares two longs.</summary>
    private readonly long _maxFrameHoldStamps;

    /// <summary>When the buffer last went out, for the quiet-stream rule on write.</summary>
    private long _lastSendStamp;

    /// <summary>
    /// When the OLDEST frame still buffered was composed — stamped as the buffer goes from empty
    /// to non-empty. Meaningful only while the buffer holds something.
    /// </summary>
    private long _oldestFrameStamp;

    /// <summary>
    /// Log-event rows this writer has ACCEPTED — composed whole into its buffer — since it was
    /// created. A row counts from that moment, whether or not a send has carried it out yet, and
    /// a row that failed to compose (and was cut back out) never counts. The difference across a
    /// query is how many rows that query wrote.
    /// </summary>
    public long RowsWritten { get; private set; }

    /// <summary>
    /// The id of the last row accepted; with <see cref="LastRowTimestampTicks"/>, the
    /// (timestamp, id) keyset position a caller paging forward continues from. Meaningful once
    /// <see cref="RowsWritten"/> is non-zero.
    /// </summary>
    public EventId LastRowId { get; private set; }

    /// <summary>UTC ticks of the last row accepted; see <see cref="LastRowId"/>.</summary>
    public long LastRowTimestampTicks { get; private set; }

    /// <param name="body">The response body to frame into — <c>ctx.Response.Body</c>.</param>
    public SseJsonWriter(Stream body) : this(body, TimeProvider.System) { }

    /// <param name="body">The response body to frame into — <c>ctx.Response.Body</c>.</param>
    /// <param name="terminalFrames">
    /// How the <c>done</c> and <c>query-error</c> payloads are encoded: the host's writer settings,
    /// so an error message carrying the user's own text (a TraceQL literal, a service name) goes
    /// out as the REST answers of the same host write it — under ASP.NET Core's relaxed encoder
    /// Cyrillic, <c>&lt;</c> and <c>'</c> as UTF-8, not <c>\uXXXX</c> (#94). <c>Indented</c> is
    /// IGNORED: a payload is one <c>data:</c> line, and an indented one would carry the raw line
    /// break that ends it. Every other frame keeps this writer's own default encoding; a stream
    /// whose rows must match a REST twin encodes them itself (the trace streams do).
    /// </param>
    public SseJsonWriter(Stream body, JsonWriterOptions terminalFrames)
        : this(body, TimeProvider.System, terminalFrames) { }

    /// <param name="body">The response body to frame into.</param>
    /// <param name="time">
    /// The clock the hold rules read. A test passes one that moves only when it says so: against the
    /// wall clock, "this row is still buffered" held only as long as nothing stalled the test for
    /// 100 ms between two calls, which a loaded two-core CI runner does not promise.
    /// </param>
    /// <param name="terminalFrames">See the public overload; null keeps the default encoding for them too.</param>
    internal SseJsonWriter(Stream body, TimeProvider time, JsonWriterOptions? terminalFrames = null)
    {
        _body               = body;
        _time               = time;
        _maxFrameHoldStamps = MaxFrameHold.Ticks * time.TimestampFrequency / TimeSpan.TicksPerSecond;
        _lastSendStamp      = time.GetTimestamp();
        _json               = new Utf8JsonWriter(_buffer);
        _terminalJson       = terminalFrames is { } o
            ? new Utf8JsonWriter(_buffer, o with { Indented = false })
            : _json;
    }

    /// <summary>
    /// Writes whatever frames are still buffered and flushes the body. A no-op when nothing
    /// is pending, so it is safe to call at the end of every page, poll and error path —
    /// and a run of <see cref="WriteLogEventAsync"/> calls that does not end in a terminal
    /// frame MUST end here: nothing else will send what it left buffered.
    /// </summary>
    public ValueTask FlushFramesAsync(CancellationToken ct) => SendAsync(ct);

    /// <summary>
    /// Writes every event <paramref name="events"/> yields as a <see cref="WriteLogEventAsync"/>
    /// frame, and sends the backlog WHENEVER THE SOURCE MAKES IT WAIT: a <c>MoveNextAsync</c>
    /// that does not complete at once is a stretch in which nothing will be written, so the rows
    /// found so far go out while the source works.
    ///
    /// <para>Rows the source hands over synchronously — one decoded block's matches, a hot-tier
    /// page — keep coalescing up to 16 KB; a sparse search shows each row before the source's
    /// next wait ends, not at <c>done</c>. That is only as good as the source's waits. The query
    /// executor's own stages never wait, so it paces its scan to wait at least every 50 ms of
    /// synchronous work (<c>Ameto.Query.ScanPace</c>), and that is the bound a search gets. The
    /// send runs WHILE the source works, but on this call and against nothing the source
    /// touches, so the body still has one writer.</para>
    ///
    /// <para>The source is never disposed in the middle of a step. A send that fails while the
    /// source is working waits for that step first, bounded by the source's own token (on the
    /// events endpoint the same deadline, linked to the request): an async iterator disposed
    /// mid-step throws <see cref="NotSupportedException"/>, which would replace the send's own
    /// failure — the one the caller's catch filters are written for.</para>
    ///
    /// <para>A REAL FAULT IN THAT STEP IS NOT SWALLOWED. If the step the failed send waited for
    /// throws anything but a cancellation — a corrupt block, a filter node the evaluator has no
    /// arm for — both come out together as an <see cref="AggregateException"/>, the send's failure
    /// first. Reporting the send alone would take the handler down its timeout or disconnect
    /// branch, which logs nothing, and the fault would never be recorded. A step that stopped
    /// because it was cancelled adds nothing to the send's failure and is dropped.</para>
    ///
    /// <para>Ends without a terminal frame and without sending what the last rows left
    /// buffered: the caller writes <c>done</c> or <c>query-error</c> next, and that frame
    /// carries them.</para>
    /// </summary>
    public async ValueTask WriteLogEventsAsync(IAsyncEnumerable<LogEvent> events, CancellationToken ct)
    {
        IAsyncEnumerator<LogEvent> rows = events.GetAsyncEnumerator(ct);
        try
        {
            while (true)
            {
                ValueTask<bool> next = rows.MoveNextAsync();
                if (!next.IsCompleted)
                {
                    try
                    {
                        await SendAsync(ct).ConfigureAwait(false);
                    }
                    catch (Exception sendFault)
                    {
                        try
                        {
                            await next.ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            // The scan stopped for the reason the send did: nothing to add.
                        }
                        catch (Exception scanFault)
                        {
                            throw new AggregateException(sendFault, scanFault);
                        }
                        throw;
                    }
                }

                if (!await next.ConfigureAwait(false)) return;
                await WriteLogEventAsync(rows.Current, ct).ConfigureAwait(false);
            }
        }
        finally
        {
            await rows.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// One <c>data:</c> frame carrying a log event, written straight from the event with no
    /// DTO and no reflection (see <see cref="LogEventJsonWriter"/>), and held in the buffer
    /// with its neighbours until <see cref="FlushThresholdBytes"/> is reached or a row finds the
    /// oldest of them <see cref="MaxFrameHold"/> old.
    ///
    /// <para>Callers own the end of the run: finish with <see cref="FlushFramesAsync"/> (or any
    /// terminal frame, which sends the backlog with it). A caller that has a source rather than
    /// a row should hand the source to <see cref="WriteLogEventsAsync"/>, which also sends
    /// whenever the source makes it wait.</para>
    ///
    /// <para>A send refused before the body took anything leaves this frame buffered; it goes out
    /// with the next frame, so a caller must not write it again after an
    /// <see cref="OperationCanceledException"/>. The row counts in <see cref="RowsWritten"/>
    /// either way.</para>
    /// </summary>
    public async ValueTask WriteLogEventAsync(LogEvent ev, CancellationToken ct)
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
            DiscardFrameFrom(frameStart);
            throw;
        }

        // Accepted: counted BEFORE the send below, which may be refused and leave the frame
        // buffered. Counting after it would put a keyset caller's position in front of a row
        // that is still going out, and its next page would write that row again.
        RowsWritten++;
        LastRowId             = ev.Id;
        LastRowTimestampTicks = ev.Timestamp.UtcTicks;

        bool first = frameStart == 0;
        long now   = _time.GetTimestamp();
        if (first) _oldestFrameStamp = now;

        if (_buffer.WrittenCount >= FlushThresholdBytes
            || now - (first ? _lastSendStamp : _oldestFrameStamp) >= _maxFrameHoldStamps)
            await SendAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Puts the buffered bytes on the wire and empties the buffer, keeping its capacity —
    /// one buffer per connection, for the life of the connection.
    ///
    /// <para>Once the bytes have been OFFERED to the body they belong to it, and the buffer is
    /// emptied whether or not the send then succeeds: Kestrel copies them into its pipe inside
    /// <c>WriteAsync</c>, so a cancellation that lands in the flush, or in the flush that
    /// <c>WriteAsync</c> itself performs, fails a send whose rows are already on their way to
    /// the client. Keeping them here would hand them over a second time with the terminal
    /// <c>query-error</c> frame, and the client would list up to 16 KB of rows twice.</para>
    ///
    /// <para>Bytes NEVER OFFERED stay. A token that is already cancelled when the send starts
    /// fails it here, before the body sees anything — which is exactly what Kestrel's
    /// <c>ValidateState</c> would do one call later, except that there the <c>finally</c> below
    /// would already have thrown the rows away. That start is not rare: a scan step that goes
    /// asynchronous after the search budget ran out makes <see cref="WriteLogEventsAsync"/> send
    /// under the spent token. The rows found within budget then go out with the handler's
    /// query-error, which is sent under a token of its own, instead of vanishing in front of
    /// it.</para>
    /// </summary>
    private async ValueTask SendAsync(CancellationToken ct)
    {
        if (_buffer.WrittenCount == 0) return;
        ct.ThrowIfCancellationRequested();       // not offered, so still ours to deliver
        try
        {
            await _body.WriteAsync(_buffer.WrittenMemory, ct).ConfigureAwait(false);
            await _body.FlushAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            _buffer.ResetWrittenCount();
            _lastSendStamp = _time.GetTimestamp();
        }
    }

    /// <summary>
    /// Cuts a frame that failed part-way back out of the buffer — the one rollback every
    /// composing method uses. Everything before <paramref name="frameStart"/> is whole frames and
    /// stays. The JSON writer's unflushed tail of the failed frame is dropped as well, so a later
    /// flush (<see cref="Dispose"/> flushes) cannot advance the buffer over bytes that are no
    /// longer there.
    /// </summary>
    private void DiscardFrameFrom(int frameStart)
    {
        _buffer.ResetWrittenCount();
        _buffer.Advance(frameStart);      // keep the whole frames, drop the partial one
        _json.Reset(_buffer);
    }

    // ── Frames that are sent at once ──────────────────────────────────────────

    /// <summary>
    /// Writes one <c>data:</c> frame with the DTO serialised through a SOURCE-GENERATED contract
    /// and sends it, backlog and all — the door for any stream that still goes out as a DTO (the
    /// trace streams), and the only one: it keeps the reflection-based metadata resolver out of
    /// the per-row path, and out of the trimmed output.
    ///
    /// <para>A reflection overload used to sit beside this one for the log streams, whose DTO
    /// carried <c>object</c>-typed property values a generated contract does not describe. Every
    /// log stream now writes its rows through <see cref="WriteLogEventAsync"/>, and it went with
    /// the last of them, the live tail.</para>
    ///
    /// <para>A send refused before the body took anything leaves this frame buffered; it goes out
    /// with the next frame, so a caller must not write it again after an
    /// <see cref="OperationCanceledException"/>.</para>
    /// </summary>
    public async Task WriteEventAsync<T>(T dto, JsonTypeInfo<T> typeInfo, CancellationToken ct)
    {
        int frameStart = _buffer.WrittenCount;
        try
        {
            _buffer.Write(DataPrefix);
            _json.Reset(_buffer);
            JsonSerializer.Serialize(_json, dto, typeInfo);
            _buffer.Write(FrameSuffix);
        }
        catch
        {
            DiscardFrameFrom(frameStart);
            throw;
        }
        await SendAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Terminal <c>event: done</c> frame with an empty payload.</summary>
    public async Task WriteDoneAsync(CancellationToken ct)
    {
        _buffer.Write(DoneFrame);
        await SendAsync(ct).ConfigureAwait(false);
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
        int frameStart = _buffer.WrittenCount;
        try
        {
            _buffer.Write(DonePrefix);
            _terminalJson.Reset(_buffer);
            _terminalJson.WriteStartObject();
            _terminalJson.WriteBoolean("complete", complete);
            _terminalJson.WriteString("reason", reason);
            if (truncatedBy is not null) _terminalJson.WriteString("truncatedBy", truncatedBy);
            _terminalJson.WriteEndObject();
            _terminalJson.Flush();
            _buffer.Write(FrameSuffix);
        }
        catch
        {
            DiscardFrameFrom(frameStart);
            throw;
        }
        await SendAsync(ct).ConfigureAwait(false);
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
        int frameStart = _buffer.WrittenCount;
        try
        {
            _buffer.Write(ErrorPrefix);
            _terminalJson.Reset(_buffer);
            _terminalJson.WriteStartObject();
            _terminalJson.WriteString("error", message);
            if (truncatedBy is not null) _terminalJson.WriteString("truncatedBy", truncatedBy);
            _terminalJson.WriteEndObject();
            _terminalJson.Flush();
            _buffer.Write(FrameSuffix);
        }
        catch
        {
            DiscardFrameFrom(frameStart);
            throw;
        }
        await SendAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Comment-only keepalive frame (ignored by EventSource clients).</summary>
    public async Task WriteKeepaliveAsync(CancellationToken ct)
    {
        _buffer.Write(KeepaliveFrame);
        await SendAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Releases the JSON writer. A frame still buffered is dropped, which is only ever the case
    /// on the client-disconnect road: every other ending writes a terminal frame first, and that
    /// frame carries the backlog. There is nothing to stop and nothing to wait for — no send
    /// outlives the call that started it, so once the handler's last call has returned the body
    /// is never touched again.
    /// </summary>
    public void Dispose()
    {
        _json.Dispose();
        if (!ReferenceEquals(_terminalJson, _json)) _terminalJson.Dispose();
    }
}
