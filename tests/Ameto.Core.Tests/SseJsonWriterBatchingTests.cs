using Ameto.Core;
using Ameto.Testing;
using static Ameto.Core.Tests.SseRows;

namespace Ameto.Core.Tests;

/// <summary>
/// The events stream coalesces row frames instead of taking a socket send per row. What has to
/// stay true either side of that: a page's worth of rows arriving together goes out in few
/// sends, a row arriving ALONE does not wait for a neighbour that may be thirty seconds away,
/// and no frame is ever stranded in the buffer or sent twice.
///
/// <para>The last point is the one that makes the first two safe to have. The Angular store
/// paints progressively as rows arrive; a sparse cold search — forty matches found across a
/// long scan — would show nothing at all until <c>done</c> if the only trigger were 16 KB.
/// How rows go out while the scan itself makes the writer wait is in
/// <see cref="SseJsonWriterSourceTests"/>.</para>
/// </summary>
public sealed class SseJsonWriterBatchingTests
{
    /// <summary>
    /// THE REGRESSION THIS EXISTS FOR. Rows found far apart must reach the client as they are
    /// found. Without the time bound all three would sit in the buffer — well under 16 KB —
    /// and appear only when the terminal frame finally pushed them out.
    /// </summary>
    [Fact]
    public async Task Rows_found_far_apart_are_sent_as_they_are_found()
    {
        var body  = new RecordingStream();
        var clock = new ManualTimeProvider();
        using var sse = new SseJsonWriter(body, clock);

        for (uint i = 0; i < 3; i++)
        {
            clock.Advance(TimeSpan.FromMilliseconds(150));
            await sse.WriteLogEventAsync(Event(i), default);
        }

        Assert.Equal(3, body.SendCount);
        for (int i = 0; i < 3; i++)
        {
            Assert.StartsWith("data: {", body.SendAt(i));
            Assert.EndsWith("\n\n", body.SendAt(i));
            Assert.Contains(Row((uint)i), body.SendAt(i));
        }
    }

    /// <summary>
    /// THE HOLD IS 100 MS WHATEVER UNIT THE CLOCK COUNTS IN: a row is held one tick short of it and
    /// sent at it, under both rules. The writer converts the hold into its clock's timestamps once,
    /// and <c>Stopwatch.Frequency</c> is 1e7 per second on Windows but 1e9 on Linux. At 1e7 a
    /// timestamp is a TimeSpan tick, so a conversion with its operands swapped is still exact there
    /// and shows only at 1e9, where the hold shrinks to 10 µs and every row goes out on its own.
    /// </summary>
    [Theory]
    [InlineData(ManualTimeProvider.TickFrequency)]
    [InlineData(ManualTimeProvider.NanosecondFrequency)]
    public async Task A_row_is_held_exactly_the_hold_at_any_timestamp_frequency(long frequency)
    {
        var hold    = TimeSpan.FromMilliseconds(100);
        var oneTick = TimeSpan.FromTicks(1);
        var body    = new RecordingStream();
        var clock   = new ManualTimeProvider(frequency);
        using var sse = new SseJsonWriter(body, clock);

        // Quiet-stream rule: one tick short of the hold since the last send (the writer's start).
        clock.Advance(hold - oneTick);
        await sse.WriteLogEventAsync(Event(0), default);
        Assert.Equal(0, body.SendCount);

        // Backlog rule: the oldest frame one tick short of the hold, then exactly at it.
        clock.Advance(hold - oneTick);
        await sse.WriteLogEventAsync(Event(1), default);
        Assert.Equal(0, body.SendCount);

        clock.Advance(oneTick);
        await sse.WriteLogEventAsync(Event(2), default);
        Assert.Equal(1, body.SendCount);
        Assert.Equal(3, Count(body.SendAt(0), "data: {\"@t\""));

        // Quiet-stream rule again: exactly the hold since that send.
        clock.Advance(hold);
        await sse.WriteLogEventAsync(Event(3), default);
        Assert.Equal(2, body.SendCount);
        Assert.Contains(Row(3), body.SendAt(1));
    }

    /// <summary>
    /// THE BODY HAS ONE WRITER, AND IT IS THE CALLER. Every byte goes out inside a call the
    /// caller is awaiting and none after that call has returned: not while rows sit buffered
    /// past every hold bound, not while the client has stopped reading, and not after Dispose.
    ///
    /// <para>The handler's teardown rests on this. It disposes the writer, then the search
    /// deadline, and returns. Anything still sending by then would be a pool thread writing to
    /// the body of a completed request, under a token that can no longer fire. A hold timer
    /// did exactly that: its send outlived Dispose, it needed a gate to keep off the caller's
    /// writes, and nothing here could see either, because the recording stream completes inline.
    /// This body completes asynchronously and stalls on demand.</para>
    /// </summary>
    [Fact]
    public async Task The_body_is_written_only_inside_the_callers_own_calls()
    {
        var body = new ProbeStream();
        var sse  = new SseJsonWriter(body);

        body.CallerInside = true;
        await sse.WriteLogEventAsync(Event(0), default);
        await sse.WriteLogEventAsync(Event(1), default);
        body.CallerInside = false;

        // Rows buffered, and the caller busy elsewhere for longer than any hold bound.
        await Task.Delay(400);
        Assert.Equal(0, body.OutsideCalls);

        // A client that has stopped reading. The terminal frame's send stalls in the flush and
        // its own token gives up, which is the shape of SafeErrorAsync. The token gives up once
        // the flush is stuck, not after a fixed 300 ms: a runner slow enough to take that long to
        // compose the frame cancelled it before the body had been offered a byte, and the rows
        // then stayed buffered for Dispose to drop.
        body.FlushStall   = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        body.CallerInside = true;
        using (var giveUp = new CancellationTokenSource())
        {
            Task terminal = sse.WriteErrorAsync("the search failed", giveUp.Token);
            await Task.WhenAny(body.FlushStalled.Task, terminal);   // stuck in the flush (or, wrongly, done)
            giveUp.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => terminal);
        }
        body.CallerInside = false;
        Assert.Equal(0, body.InFlight);                  // the send ended before the call did

        sse.Dispose();
        int operationsAtDispose = body.Operations;
        body.FlushStall.SetResult();                     // the client reads again: nothing is waiting to go
        await Task.Delay(300);

        Assert.Equal(operationsAtDispose, body.Operations);
        Assert.Equal(0, body.OutsideCalls);
        Assert.Equal(0, body.InFlight);
        Assert.Equal(1, body.MaxConcurrent);

        string all = body.All();
        Assert.Equal(1, Count(all, Row(0)));
        Assert.Equal(1, Count(all, Row(1)));
        Assert.Equal(1, Count(all, "event: query-error"));
        Assert.EndsWith("\n\n", all);
    }

    /// <summary>
    /// …and rows found TOGETHER still coalesce, which is the whole point of the buffer. A
    /// page of 200 small rows is a few sends, not 200.
    /// </summary>
    [Fact]
    public async Task Rows_found_together_coalesce_into_few_sends()
    {
        var body = new RecordingStream();
        using var sse = OnFrozenClock(body);          // together means together, however slow the runner

        for (uint i = 0; i < 200; i++)
            await sse.WriteLogEventAsync(Event(i), default);
        await sse.FlushFramesAsync(default);

        Assert.True(body.SendCount <= 5, $"200 rows should coalesce, saw {body.SendCount} sends");

        // Every row is there, in order, and the framing is intact.
        string all = body.All();
        Assert.Equal(200, Count(all, "data: {\"@t\""));
        int previous = -1;
        for (uint i = 0; i < 200; i++)
        {
            int at = all.IndexOf(Row(i), StringComparison.Ordinal);
            Assert.True(at > previous, $"row {i} is missing or out of order");
            previous = at;
        }
    }

    public static TheoryData<string> FramesThatSend => new()
    {
        "done", "done-with-ending", "query-error", "keepalive", "event-contract",
    };

    /// <summary>
    /// Nothing is stranded: EVERY frame that sends — the terminal ones, the keepalive, and a
    /// DTO frame — carries the buffered rows out with it, ahead of itself, in one send, and
    /// leaves the buffer empty.
    /// </summary>
    [Theory]
    [MemberData(nameof(FramesThatSend))]
    public async Task Every_frame_that_sends_carries_the_backlog_out_ahead_of_it(string frame)
    {
        var body = new RecordingStream();
        using var sse = OnFrozenClock(body);

        // Warm the row road and send it, so the row below is held by nothing but the buffer:
        // the last send was no time ago on this clock, and it is the only frame waiting.
        await sse.WriteLogEventAsync(Event(0), default);
        await sse.FlushFramesAsync(default);
        body.Clear();

        await sse.WriteLogEventAsync(Event(1), default);
        Assert.Equal(0, body.SendCount);

        string marker;
        switch (frame)
        {
            case "done":
                await sse.WriteDoneAsync(default);
                marker = "event: done\ndata: {}\n\n";
                break;
            case "done-with-ending":
                await sse.WriteDoneAsync(complete: false, "max-rows", null, default);
                marker = "event: done\ndata: {\"complete\":false,\"reason\":\"max-rows\"}\n\n";
                break;
            case "query-error":
                await sse.WriteErrorAsync("the search failed", default);
                marker = "event: query-error\ndata: {\"error\":\"the search failed\"}\n\n";
                break;
            case "keepalive":
                await sse.WriteKeepaliveAsync(default);
                marker = ": keepalive\n\n";
                break;
            case "event-contract":
                await sse.WriteEventAsync(new ProbeDto { X = 1 }, ProbeContract, default);
                marker = "data: {\"X\":1}\n\n";
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(frame), frame, null);
        }

        Assert.Equal(1, body.SendCount);                 // one send: the backlog went WITH the frame
        string sent = body.SendAt(0);
        Assert.StartsWith("data: {\"@t\"", sent);
        Assert.Equal(1, Count(sent, Row(1)));
        Assert.EndsWith(marker, sent);                   // the frame came after the row, whole

        // …and left nothing behind it.
        await sse.FlushFramesAsync(default);
        Assert.Equal(1, body.SendCount);
    }

    /// <summary>
    /// A row that fails half-way through composing must not leave a fragment in front of the
    /// terminal frame — a client splitting SSE on the blank line would read the fragment and
    /// the error as one event.
    ///
    /// <para>The failure is forced with TRUNCATED property bytes: a msgpack map that promises
    /// two pairs and carries one. The transcoder runs off the end, and it does so after the
    /// row's header fields are already in the buffer, which is exactly the shape of the
    /// problem — a real one would be a corrupt segment payload reaching delivery.</para>
    /// </summary>
    [Fact]
    public async Task A_row_that_throws_leaves_no_fragment_behind()
    {
        var body = new RecordingStream();
        using var sse = new SseJsonWriter(body);

        // One good row, sent, so the buffer starts empty and anything left in it afterwards
        // can only be the fragment this test is about.
        await sse.WriteLogEventAsync(Event(0), default);
        await sse.FlushFramesAsync(default);
        body.Clear();

        await Assert.ThrowsAnyAsync<Exception>(async () => await sse.WriteLogEventAsync(Broken(), default));

        await sse.WriteErrorAsync("the search failed", default);

        string all = body.All();
        Assert.StartsWith("event: query-error", all);
        Assert.DoesNotContain("data: {\"@t\"", all);   // no half-written row in front of it
        Assert.DoesNotContain("row 99", all);
    }

    /// <summary>
    /// …and the rollback keeps the WHOLE frames that were already buffered. Losing a row that
    /// had been written successfully, because a later one failed, would be its own bug.
    /// </summary>
    [Fact]
    public async Task A_row_that_throws_does_not_take_its_neighbours_with_it()
    {
        var body = new RecordingStream();
        using var sse = new SseJsonWriter(body);

        await sse.WriteLogEventAsync(Event(0), default);   // stays in the buffer: under 16 KB, under 100 ms

        await Assert.ThrowsAnyAsync<Exception>(async () => await sse.WriteLogEventAsync(Broken(), default));

        await sse.WriteErrorAsync("the search failed", default);

        string all = body.All();
        Assert.Contains(Row(0), all);
        Assert.DoesNotContain("row 99", all);
        // Exactly one ROW frame, not one and a half. ("data: " alone would also count the
        // error frame's own payload line.)
        Assert.Equal(1, Count(all, "data: {\"@t\""));
        Assert.True(all.IndexOf("row 0", StringComparison.Ordinal)
                  < all.IndexOf("event: query-error", StringComparison.Ordinal));
    }

    /// <summary>
    /// A send that fails AFTER the body took the bytes must not offer them again. The search
    /// deadline firing inside the flush reaches the handler's timeout catch, whose query-error
    /// frame goes out through this same writer — and if the failed batch were still buffered,
    /// the client would receive every row in it twice, then the error.
    ///
    /// <para>The failure is allowed to surface from whichever call happened to send — the row
    /// write if the hold bound had already passed, the explicit flush otherwise — so the test
    /// does not depend on timing; <c>FlushFailures</c> proves it did happen.</para>
    /// </summary>
    [Fact]
    public async Task A_batch_whose_flush_failed_is_not_sent_again_with_the_terminal_frame()
    {
        var body = new RecordingStream { FlushFailuresLeft = 1 };
        using var sse = new SseJsonWriter(body);

        try
        {
            await sse.WriteLogEventAsync(Event(7), default);
            await sse.FlushFramesAsync(default);
        }
        catch (OperationCanceledException) { /* the deadline, as the handler would see it */ }
        Assert.Equal(1, body.FlushFailures);

        await sse.WriteErrorAsync("Search exceeded its budget. Results shown are partial.", default);

        string all = body.All();
        int copies = Count(all, Row(7));
        Assert.True(copies == 1, $"row 7 reached the socket {copies} times");
        Assert.True(all.IndexOf("row 7", StringComparison.Ordinal)
                  < all.IndexOf("event: query-error", StringComparison.Ordinal));
        Assert.EndsWith("\n\n", all);
    }

    /// <summary>
    /// A DTO FRAME THAT THROWS PART-WAY LEAVES NO FRAGMENT in front of the error frame. The trace
    /// streams still write a DTO per row, and a double of NaN — an OTLP attribute is stored
    /// unfiltered — makes the serialiser throw with <c>data: </c> already buffered. The handler's
    /// catch then writes query-error through this same writer. Without a rollback the client
    /// receives <c>data: event: query-error</c>: one <c>message</c> event of invalid JSON, a
    /// query-error listener that never fires, and a stream that stops without a word. Before rows
    /// coalesced, every frame reset the buffer first and the error frame went out clean; that
    /// must still be what the client gets.
    /// </summary>
    [Fact]
    public async Task A_dto_frame_that_throws_leaves_no_fragment_in_front_of_the_error_frame()
    {
        var body = new RecordingStream();
        using var sse = new SseJsonWriter(body);

        await Assert.ThrowsAnyAsync<ArgumentException>(async () => await WriteProbeAsync(sse, double.NaN));
        Assert.Equal(0, body.SendCount);

        await sse.WriteErrorAsync("The trace search failed. See the server log for details.", default);

        string all = body.All();
        Assert.StartsWith("event: query-error\ndata: {\"error\":", all);
        Assert.DoesNotContain("\"X\"", all);
        Assert.Equal(1, Count(all, "data: "));
        Assert.EndsWith("\n\n", all);
    }

    /// <summary>
    /// …and it takes only ITSELF out. The rows buffered before the failed DTO frame are whole
    /// frames and go out with the next one, and the DTO frame after the failure is framed as
    /// cleanly as if nothing had happened.
    /// </summary>
    [Fact]
    public async Task A_dto_frame_that_throws_keeps_the_rows_before_it_and_the_frame_after_it_clean()
    {
        var body = new RecordingStream();
        using var sse = OnFrozenClock(body);

        await sse.WriteLogEventAsync(Event(0), default);
        await sse.FlushFramesAsync(default);
        body.Clear();
        await sse.WriteLogEventAsync(Event(1), default);          // buffered

        await Assert.ThrowsAnyAsync<ArgumentException>(async () => await WriteProbeAsync(sse, double.NaN));
        await WriteProbeAsync(sse, 2);

        Assert.Equal(1, body.SendCount);
        string sent = body.SendAt(0);
        Assert.StartsWith("data: {\"@t\"", sent);
        Assert.Equal(1, Count(sent, Row(1)));
        Assert.Equal(2, Count(sent, "data: "));
        Assert.EndsWith("}\n\ndata: {\"X\":2}\n\n", sent);
    }

    private static Task WriteProbeAsync(SseJsonWriter sse, double x)
        => sse.WriteEventAsync(new ProbeDto { X = x }, ProbeContract, default);

    /// <summary>A row whose property bytes promise a two-pair msgpack map and carry one pair.</summary>
    private static LogEvent Broken() => new()
    {
        Id              = new EventId(0u, 99u),
        Timestamp       = new DateTimeOffset(2026, 9, 14, 12, 0, 0, TimeSpan.Zero),
        Level           = LogLevel.Information,
        MessageTemplate = "row 99",
        // 0x82 = fixmap(2); then one key/value pair, and nothing where the second belongs.
        RawProperties   = new byte[] { 0x82, 0xa1, (byte)'a', 0xa1, (byte)'b' },
    };
}
