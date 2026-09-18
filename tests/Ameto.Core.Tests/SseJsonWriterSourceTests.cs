using Ameto.Core;
using static Ameto.Core.Tests.SseRows;

namespace Ameto.Core.Tests;

/// <summary>
/// <see cref="SseJsonWriter.WriteLogEventsAsync"/> drives the query's enumerator itself so it
/// can send the rows found so far WHENEVER THE SCAN MAKES IT WAIT, on the caller's own call.
/// The on-write rules cannot do that: nothing is written while the scan works, so a row
/// buffered at the end of a burst had nobody to send it until the next row or <c>done</c>.
/// </summary>
public sealed class SseJsonWriterSourceTests
{
    /// <summary>
    /// How long a scan step waits for a send the writer should already have made. Not a timing
    /// bound: the send happens on the writer's own thread the moment the step goes pending, before
    /// the step can even resume, so this only turns a writer that never sends into a failure
    /// instead of a hang.
    ///
    /// <para>No step here goes pending on a fixed delay. A step that waited 50 ms was already
    /// finished when a writer stalled that long before looking at it, so no send happened and a
    /// correct writer failed. Each step waits instead for the event that shows the writer acted on
    /// it: the send reaching the body, or the writer's call coming back waiting on the step.</para>
    /// </summary>
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(10);

    /// <summary>
    /// A SPARSE SEARCH SHOWS EACH ROW BEFORE ITS NEXT WAIT ENDS, NOT AT <c>done</c>. Each burst
    /// is two rows back to back — the second is buffered by every on-write rule — and then the
    /// scan goes asynchronous (a segment open, a prefilter). The wait ends the moment the body
    /// receives the second row; a writer that holds it instead leaves the wait to the hang guard.
    /// </summary>
    [Fact]
    public async Task A_sparse_search_shows_each_row_before_its_next_wait_ends()
    {
        var body = new RecordingStream();
        using var sse = OnFrozenClock(body);          // nothing but the pending step can send the second row
        var seenDuringWait = new List<bool>();

        async IAsyncEnumerable<LogEvent> Sparse()
        {
            for (uint burst = 0; burst < 3; burst++)
            {
                uint first = burst * 10, second = first + 1;
                yield return Event(first);
                yield return Event(second);        // right behind it: buffered, not sent on write

                bool seen = await SentBeforeHangGuard(body.WhenSent(Row(second)));
                seenDuringWait.Add(seen);
                if (!seen) yield break;            // one held row is the failure; do not wait out the rest
            }
        }

        await sse.WriteLogEventsAsync(Sparse(), default);
        await sse.WriteDoneAsync(default);

        Assert.Equal([true, true, true], seenDuringWait);

        string all = body.All();
        uint[] rows = [0, 1, 10, 11, 20, 21];
        int previous = -1;
        foreach (uint row in rows)
        {
            Assert.Equal(1, Count(all, Row(row)));
            int at = all.IndexOf(Row(row), StringComparison.Ordinal);
            Assert.True(at > previous, $"row {row} is out of order");
            previous = at;
        }
        Assert.EndsWith("event: done\ndata: {}\n\n", all);
    }

    /// <summary>
    /// …and rows the source hands over WITHOUT making the writer wait — one decoded block's
    /// matches — still share sends. A send per pending step must not turn into a send per row.
    /// </summary>
    [Fact]
    public async Task Rows_the_source_hands_over_without_waiting_still_share_sends()
    {
        var body = new RecordingStream();
        using var sse = OnFrozenClock(body);

        static async IAsyncEnumerable<LogEvent> Burst()
        {
            await Task.Yield();                    // the prefilter: asynchronous, and nothing is buffered yet
            for (uint i = 0; i < 200; i++)
                yield return Event(i);
        }

        await sse.WriteLogEventsAsync(Burst(), default);
        await sse.WriteDoneAsync(default);

        Assert.True(body.SendCount <= 6, $"200 rows found together should coalesce, saw {body.SendCount} sends");
        string all = body.All();
        Assert.Equal(200, Count(all, "data: {\"@t\""));
        for (uint i = 0; i < 200; i++)
            Assert.Equal(1, Count(all, Row(i)));
    }

    /// <summary>
    /// A SEND THAT FAILS WHILE THE SCAN IS MID-STEP SURFACES AS ITSELF. The handler tells a
    /// spent budget from a disconnect from a real fault by the exception it catches, and an
    /// async iterator disposed mid-step throws <see cref="NotSupportedException"/> instead —
    /// which would turn the budget's query-error into "the search failed" and leave the scan
    /// running with nobody waiting for it. The writer lets the step finish first, and the scan's
    /// own cleanup runs before the call returns.
    ///
    /// <para>The failed batch is not offered again either: row 0 reaches the socket once.</para>
    /// </summary>
    [Fact]
    public async Task A_send_that_fails_while_the_scan_is_mid_step_surfaces_as_itself()
    {
        var body = new RecordingStream();
        using var sse = OnFrozenClock(body);

        // Warm the row road and send, so the source's first row is held by the buffer alone.
        await sse.WriteLogEventAsync(Event(99), default);
        await sse.FlushFramesAsync(default);
        body.Clear();
        body.FlushFailuresLeft = 1;

        bool scanClosed  = false;
        var  writerWaits = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async IAsyncEnumerable<LogEvent> Scan()
        {
            try
            {
                yield return Event(0);
                // Pending until the writer's send of row 0 reaches the body (where its flush fails),
                // AND the writer's call has come back to the test waiting on this step. The second
                // gate keeps the step unfinished at the moment a writer that does not wait for it
                // would dispose it, so that break always shows as its NotSupportedException instead
                // of losing a race to this step's own continuation.
                await Task.WhenAll(body.NextCallEntered(), writerWaits.Task).WaitAsync(HangGuard);
                yield return Event(1);
            }
            finally { scanClosed = true; }
        }

        ValueTask call = sse.WriteLogEventsAsync(Scan(), default);   // back at the writer's first real wait
        writerWaits.SetResult();
        Exception? thrown = await Record.ExceptionAsync(async () => await call);

        Assert.IsAssignableFrom<OperationCanceledException>(thrown);
        Assert.Equal(1, body.FlushFailures);
        Assert.True(scanClosed, "the scan was not closed before the call returned");

        await sse.WriteErrorAsync("Search exceeded its budget. Results shown are partial.", default);
        string all = body.All();
        Assert.Equal(1, Count(all, Row(0)));
        Assert.StartsWith("data: {\"@t\"", all);
        Assert.EndsWith("\n\n", all);
    }

    /// <summary>
    /// ROWS A SEND NEVER OFFERED TO THE BODY GO OUT WITH THE TERMINAL FRAME. The search budget
    /// runs out while the scan is mid-step, and the step then goes asynchronous, so the writer
    /// starts a send under a token that is already cancelled. Kestrel refuses such a token
    /// before it copies a byte, which leaves the rows found within budget still the writer's to
    /// deliver: the handler's query-error, sent under its own short token, must carry them out,
    /// not go out alone in front of rows the client never gets.
    /// </summary>
    [Fact]
    public async Task Rows_a_send_under_a_spent_budget_never_offered_go_out_with_the_terminal_frame()
    {
        var body = new RecordingStream();
        using var sse = OnFrozenClock(body);

        // Warm the row road and send, so the source's row is held by the buffer alone.
        await sse.WriteLogEventAsync(Event(99), default);
        await sse.FlushFramesAsync(default);
        body.Clear();

        using var budget = new CancellationTokenSource();
        var writerWaits = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int sendsWhileRowBuffered = -1;
        async IAsyncEnumerable<LogEvent> Scan()
        {
            yield return Event(0);
            sendsWhileRowBuffered = body.SendCount;
            budget.Cancel();                       // the budget runs out while the scan works…
            // …and the step goes asynchronous after it, until the writer's call has come back to the
            // test waiting on it. Not a bare yield or a delay: either can finish this step before the
            // writer looks, and then there is no wait to send in. Not the body's NextCallEntered: the
            // writer refuses a send under a spent token itself, before the body sees any call.
            await writerWaits.Task;
        }

        ValueTask call = sse.WriteLogEventsAsync(Scan(), budget.Token);   // back at the writer's first real wait
        writerWaits.SetResult();
        Exception? thrown = await Record.ExceptionAsync(async () => await call);

        Assert.IsAssignableFrom<OperationCanceledException>(thrown);
        Assert.Equal(0, sendsWhileRowBuffered);

        const string message = "Search exceeded its budget. Results shown are partial.";
        using (var own = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
            await sse.WriteErrorAsync(message, own.Token);

        string all = body.All();
        Assert.Equal(1, Count(all, Row(0)));
        Assert.StartsWith("data: {\"@t\"", all);
        Assert.EndsWith($"}}\n\nevent: query-error\ndata: {{\"error\":\"{message}\"}}\n\n", all);
    }

    /// <summary>
    /// A SCAN FAULT IN THE STEP A FAILED SEND WAITED FOR COMES OUT WITH THE SEND'S FAILURE. The
    /// client stops reading and the budget fires in the flush while the scan's step is decoding a
    /// block that turns out to be corrupt. The handler picks its branch by the exception: a
    /// cancellation alone takes the timeout or disconnect branch, which logs nothing, so the
    /// corruption would go unrecorded. Both come out, the send's failure first.
    ///
    /// <para>A step that throws a CANCELLATION adds nothing — it stopped for the reason the send
    /// failed — so the send's own failure still surfaces as itself.</para>
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_failed_send_reports_a_fault_in_the_step_it_waited_for_but_not_a_cancellation(bool stepFaults)
    {
        var body = new RecordingStream();
        using var sse = OnFrozenClock(body);

        // Warm the row road and send, so the source's row is held by the buffer alone.
        await sse.WriteLogEventAsync(Event(99), default);
        await sse.FlushFramesAsync(default);
        body.Clear();
        body.FlushFailuresLeft = 1;

        async IAsyncEnumerable<LogEvent> Scan()
        {
            yield return Event(0);
            // Pending until the writer's send of row 0 reaches the body; its flush then fails.
            await body.NextCallEntered().WaitAsync(HangGuard);
            throw stepFaults
                ? new InvalidDataException("block 7 failed its checksum")
                : new OperationCanceledException("the scan saw the budget run out");
        }

        Exception? thrown = await Record.ExceptionAsync(async () => await sse.WriteLogEventsAsync(Scan(), default));

        Assert.Equal(1, body.FlushFailures);
        if (stepFaults)
        {
            var both = Assert.IsType<AggregateException>(thrown);
            Assert.Collection(both.InnerExceptions,
                send => Assert.IsType<TaskCanceledException>(send),
                scan => Assert.IsType<InvalidDataException>(scan));
        }
        else
        {
            Assert.IsType<TaskCanceledException>(thrown);   // the send's, not the scan's
        }
    }

    /// <summary>
    /// THE SCAN ROAD HAS ONE WRITER TOO. A send made while the source works runs beside the
    /// source's step — never beside another send, and never beside a row being composed into the
    /// buffer it is sending. Against a body that completes every call asynchronously, no body
    /// operation starts outside the call, none overlaps another, none is still in flight when the
    /// call returns, and every row arrives exactly once.
    ///
    /// <para>The recording stream cannot show this: a send that completes inline is over before
    /// the loop can move on, so a send the loop did not wait for would look exactly like one it
    /// did. Here each step is a bare yield, short enough that a send left running would still be
    /// on the body when the next rows are composed and the next send starts.</para>
    /// </summary>
    [Fact]
    public async Task Sends_made_while_the_scan_works_stay_inside_the_call_and_never_overlap()
    {
        var body = new ProbeStream();
        using var sse = new SseJsonWriter(body);

        static async IAsyncEnumerable<LogEvent> Scan()
        {
            for (uint burst = 0; burst < 20; burst++)
            {
                yield return Event(burst * 10);
                yield return Event(burst * 10 + 1);
                await Task.Yield();                // the writer sends the burst now
            }
        }

        body.CallerInside = true;
        await sse.WriteLogEventsAsync(Scan(), default);
        await sse.WriteDoneAsync(default);
        body.CallerInside = false;
        Assert.Equal(0, body.InFlight);

        int operations = body.Operations;
        await Task.Delay(200);

        Assert.Equal(operations, body.Operations);
        Assert.Equal(0, body.OutsideCalls);
        Assert.Equal(1, body.MaxConcurrent);

        string all = body.All();
        Assert.Equal(40, Count(all, "data: {\"@t\""));
        for (uint burst = 0; burst < 20; burst++)
        {
            Assert.Equal(1, Count(all, Row(burst * 10)));
            Assert.Equal(1, Count(all, Row(burst * 10 + 1)));
        }
        Assert.EndsWith("event: done\ndata: {}\n\n", all);
    }

    private static async Task<bool> SentBeforeHangGuard(Task sent)
    {
        try
        {
            await sent.WaitAsync(HangGuard);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }
}
