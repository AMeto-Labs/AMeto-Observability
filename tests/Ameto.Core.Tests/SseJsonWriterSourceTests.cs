using System.Diagnostics;
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
    /// A SPARSE SEARCH SHOWS EACH ROW BEFORE ITS NEXT WAIT ENDS, NOT AT <c>done</c>. Each burst
    /// is two rows back to back — the second is buffered by every on-write rule — and then the
    /// scan goes asynchronous (a segment open, a prefilter). The wait ends only once the client
    /// has the second row, or after 3 s if it never gets it.
    /// </summary>
    [Fact]
    public async Task A_sparse_search_shows_each_row_before_its_next_wait_ends()
    {
        var body = new RecordingStream();
        using var sse = new SseJsonWriter(body);
        var seenDuringWait = new List<bool>();

        async IAsyncEnumerable<LogEvent> Sparse()
        {
            for (uint burst = 0; burst < 3; burst++)
            {
                uint first = burst * 10, second = first + 1;
                yield return Event(first);
                yield return Event(second);        // right behind it: buffered, not sent on write

                seenDuringWait.Add(await WaitForAsync(() => body.All().Contains(Row(second)), TimeSpan.FromSeconds(3)));
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
        using var sse = new SseJsonWriter(body);

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
        using var sse = new SseJsonWriter(body);

        // Warm the row road and send, so the source's first row is held by the buffer alone.
        await sse.WriteLogEventAsync(Event(99), default);
        await sse.FlushFramesAsync(default);
        body.Clear();
        body.FlushFailuresLeft = 1;

        bool scanClosed = false;
        async IAsyncEnumerable<LogEvent> Scan()
        {
            try
            {
                yield return Event(0);
                await Task.Delay(50);              // the writer sends row 0 now, and that send fails
                yield return Event(1);
            }
            finally { scanClosed = true; }
        }

        Exception? thrown = await Record.ExceptionAsync(async () => await sse.WriteLogEventsAsync(Scan(), default));

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

    private static async Task<bool> WaitForAsync(Func<bool> condition, TimeSpan limit)
    {
        var clock = Stopwatch.StartNew();
        while (!condition())
        {
            if (clock.Elapsed > limit) return false;
            await Task.Delay(10);
        }
        return true;
    }
}
