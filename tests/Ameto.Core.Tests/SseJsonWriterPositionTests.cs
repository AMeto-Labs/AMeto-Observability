using Ameto.Core;
using static Ameto.Core.Tests.SseRows;

namespace Ameto.Core.Tests;

/// <summary>
/// <see cref="SseJsonWriter.RowsWritten"/>, <see cref="SseJsonWriter.LastRowId"/> and
/// <see cref="SseJsonWriter.LastRowTimestampTicks"/> are where a keyset caller — the live tail —
/// continues from. A row is written when its frame is ACCEPTED into the buffer, so the position must
/// move for a row that is only buffered and for a row whose send was refused (still buffered, and
/// going out with the next frame), and must not move for a row that failed to compose (cut back out,
/// gone). Getting the second wrong writes a row twice; getting the third wrong skips one.
/// </summary>
public sealed class SseJsonWriterPositionTests
{
    [Fact]
    public async Task A_row_moves_the_position_as_it_is_accepted()
    {
        var body = new RecordingStream();
        using var sse = new SseJsonWriter(body);
        Assert.Equal(0, sse.RowsWritten);

        await sse.WriteLogEventAsync(Event(0), default);
        await sse.WriteLogEventAsync(Event(1), default);

        Assert.Equal(2, sse.RowsWritten);
        Assert.Equal(Event(1).Id, sse.LastRowId);
        Assert.Equal(Event(1).Timestamp.UtcTicks, sse.LastRowTimestampTicks);
    }

    /// <summary>
    /// THE DUPLICATE THIS EXISTS TO PREVENT. The first page's row is big enough that its own write sends,
    /// and the budget is already spent, so the send is refused before the body takes a byte: the frame
    /// stays buffered and the write throws. The next page continues from the writer's position, as the
    /// tail's next poll does, and must not ask for — and write — that row again: the client would list it
    /// twice, once from the buffer and once from the page.
    /// </summary>
    [Fact]
    public async Task A_row_whose_send_was_refused_counts_as_written_so_the_next_page_does_not_write_it_again()
    {
        var body = new RecordingStream();
        using var sse = new SseJsonWriter(body);

        LogEvent[] log = [Big(0), Event(1), Event(2)];

        using (var spent = new CancellationTokenSource())
        {
            spent.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await sse.WriteLogEventsAsync(PageAfter(log, sse), spent.Token));
        }
        Assert.Equal(0, body.SendCount);                 // refused before the body saw anything
        long writtenAfterRefusal = sse.RowsWritten;
        var  positionAfterRefusal = sse.LastRowId;

        await sse.WriteLogEventsAsync(PageAfter(log, sse), default);
        await sse.FlushFramesAsync(default);

        // The wire first: this is the failure a client would see.
        string all = body.All();
        foreach (var ev in log)
            Assert.True(Count(all, IdOf(ev)) == 1, $"row {ev.Id.RawValue} reached the socket {Count(all, IdOf(ev))} times");
        Assert.True(all.IndexOf(IdOf(log[0]), StringComparison.Ordinal) < all.IndexOf(IdOf(log[1]), StringComparison.Ordinal));

        Assert.Equal(1, writtenAfterRefusal);
        Assert.Equal(log[0].Id, positionAfterRefusal);
        Assert.Equal(3, sse.RowsWritten);
        Assert.Equal(log[2].Id, sse.LastRowId);
    }

    /// <summary>
    /// A row that throws while composing is cut back out of the buffer and never sent, so the position
    /// stays on the row before it. Moving past it would make the next page skip a row that nobody saw.
    /// </summary>
    [Fact]
    public async Task A_row_that_fails_to_compose_does_not_move_the_position()
    {
        var body = new RecordingStream();
        using var sse = new SseJsonWriter(body);

        await sse.WriteLogEventAsync(Event(0), default);
        await Assert.ThrowsAnyAsync<Exception>(async () => await sse.WriteLogEventAsync(Broken(), default));

        Assert.Equal(1, sse.RowsWritten);
        Assert.Equal(Event(0).Id, sse.LastRowId);
        Assert.Equal(Event(0).Timestamp.UtcTicks, sse.LastRowTimestampTicks);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// The rows of <paramref name="log"/> strictly after the writer's (timestamp, id) position — or all of
    /// them before it has written any — handed over without a wait, as a hot-tier page is.
    /// </summary>
    private static async IAsyncEnumerable<LogEvent> PageAfter(LogEvent[] log, SseJsonWriter sse)
    {
        await Task.CompletedTask;
        bool any  = sse.RowsWritten > 0;
        long ts   = sse.LastRowTimestampTicks;
        var  id   = sse.LastRowId;
        foreach (var ev in log)
        {
            long t = ev.Timestamp.UtcTicks;
            if (!any || t > ts || (t == ts && ev.Id.CompareTo(id) > 0))
                yield return ev;
        }
    }

    /// <summary>A row over the writer's 16 KB threshold, so writing it sends.</summary>
    private static LogEvent Big(uint seq) => new()
    {
        Id              = new EventId(0u, seq),
        Timestamp       = Event(seq).Timestamp,
        Level           = LogLevel.Information,
        MessageTemplate = "row " + seq + " " + new string('x', 17 * 1024),
    };

    /// <summary>A row whose property bytes promise a two-pair msgpack map and carry one pair.</summary>
    private static LogEvent Broken() => new()
    {
        Id              = new EventId(0u, 99u),
        Timestamp       = Event(99).Timestamp,
        Level           = LogLevel.Information,
        MessageTemplate = "row 99",
        RawProperties   = new byte[] { 0x82, 0xa1, (byte)'a', 0xa1, (byte)'b' },
    };

    private static string IdOf(LogEvent ev) => $"\"id\":\"{ev.Id.RawValue}\"";
}
