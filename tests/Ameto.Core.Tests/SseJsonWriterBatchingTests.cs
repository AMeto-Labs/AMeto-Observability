using System.Text;
using Ameto.Core;

namespace Ameto.Core.Tests;

/// <summary>
/// The events stream coalesces row frames instead of taking a socket send per row. What has to
/// stay true either side of that: a page's worth of rows arriving together goes out in few
/// sends, a row arriving ALONE does not wait for a neighbour that may be thirty seconds away,
/// and no frame is ever stranded in the buffer.
///
/// <para>The last point is the one that makes the first two safe to have. The Angular store
/// paints progressively as rows arrive; a sparse cold search — forty matches found across a
/// long scan — would show nothing at all until <c>done</c> if the only trigger were 16 KB.</para>
/// </summary>
public sealed class SseJsonWriterBatchingTests
{
    /// <summary>A body that remembers each write as the client's socket would see it.</summary>
    private sealed class RecordingStream : Stream
    {
        public readonly List<string> Sends = [];

        /// <summary>
        /// How many of the coming flushes fail AFTER their write has taken the bytes — the shape
        /// of a search deadline firing inside <c>Body.FlushAsync</c> while Kestrel's pipe already
        /// holds the frame.
        /// </summary>
        public int FlushFailuresLeft;

        /// <summary>How many flushes actually failed, so a test can prove the fault happened.</summary>
        public int FlushFailures;

        public override void Write(ReadOnlySpan<byte> buffer) => Sends.Add(Encoding.UTF8.GetString(buffer));
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
        {
            Sends.Add(Encoding.UTF8.GetString(buffer.Span));
            return ValueTask.CompletedTask;
        }
        public override Task FlushAsync(CancellationToken ct)
        {
            if (FlushFailuresLeft <= 0) return Task.CompletedTask;
            FlushFailuresLeft--;
            FlushFailures++;
            return Task.FromCanceled(new CancellationToken(canceled: true));
        }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => 0; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] b, int o, int c) => throw new NotSupportedException();
        public override long Seek(long o, SeekOrigin r) => throw new NotSupportedException();
        public override void SetLength(long v) => throw new NotSupportedException();
        public override void Write(byte[] b, int o, int c) => Write(b.AsSpan(o, c));
    }

    private static LogEvent Event(uint seq) => new()
    {
        Id              = new EventId(0u, seq),
        Timestamp       = new DateTimeOffset(2026, 9, 14, 12, 0, 0, TimeSpan.Zero).AddSeconds(seq),
        Level           = LogLevel.Information,
        MessageTemplate = "row " + seq,
    };

    /// <summary>
    /// THE REGRESSION THIS EXISTS FOR. Rows found far apart must reach the client as they are
    /// found. Without the time bound all three would sit in the buffer — well under 16 KB —
    /// and appear only when the terminal frame finally pushed them out.
    /// </summary>
    [Fact]
    public async Task Rows_found_far_apart_are_sent_as_they_are_found()
    {
        var body = new RecordingStream();
        using var sse = new SseJsonWriter(body);

        for (uint i = 0; i < 3; i++)
        {
            await Task.Delay(150);
            await sse.WriteLogEventAsync(Event(i), default);
        }

        Assert.Equal(3, body.Sends.Count);
        for (int i = 0; i < 3; i++)
        {
            Assert.StartsWith("data: {", body.Sends[i]);
            Assert.EndsWith("\n\n", body.Sends[i]);
            Assert.Contains($"\"@mt\":\"row {i}\"", body.Sends[i]);
        }
    }

    /// <summary>
    /// …and rows found TOGETHER still coalesce, which is the whole point of the buffer. A
    /// page of 200 small rows is a few sends, not 200.
    /// </summary>
    [Fact]
    public async Task Rows_found_together_coalesce_into_few_sends()
    {
        var body = new RecordingStream();
        using var sse = new SseJsonWriter(body);

        for (uint i = 0; i < 200; i++)
            await sse.WriteLogEventAsync(Event(i), default);
        await sse.FlushFramesAsync(default);

        Assert.True(body.Sends.Count <= 5, $"200 rows should coalesce, saw {body.Sends.Count} sends");

        // Every row is there, in order, and the framing is intact.
        string all = string.Concat(body.Sends);
        Assert.Equal(200, all.Split("data: {\"@t\"").Length - 1);
        int previous = -1;
        for (uint i = 0; i < 200; i++)
        {
            int at = all.IndexOf($"\"@mt\":\"row {i}\"", StringComparison.Ordinal);
            Assert.True(at > previous, $"row {i} is missing or out of order");
            previous = at;
        }
    }

    /// <summary>
    /// Nothing is stranded: the terminal frame carries the backlog out with it, in one send,
    /// and after it the buffer is empty.
    /// </summary>
    [Fact]
    public async Task A_terminal_frame_carries_the_backlog_with_it()
    {
        var body = new RecordingStream();
        using var sse = new SseJsonWriter(body);

        await sse.WriteLogEventAsync(Event(0), default);   // may or may not have gone out yet
        body.Sends.Clear();
        await sse.WriteLogEventAsync(Event(1), default);
        await sse.WriteDoneAsync(default);

        string all = string.Concat(body.Sends);
        Assert.Contains("\"@mt\":\"row 1\"", all);
        Assert.Contains("event: done", all);
        Assert.EndsWith("\n\n", all);

        // The `done` frame did not arrive before the row it followed.
        Assert.True(all.IndexOf("\"@mt\":\"row 1\"", StringComparison.Ordinal)
                  < all.IndexOf("event: done", StringComparison.Ordinal));
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
        body.Sends.Clear();

        // 0x82 = fixmap(2); then one key/value pair, and nothing where the second belongs.
        byte[] truncated = [0x82, 0xa1, (byte)'a', 0xa1, (byte)'b'];
        var broken = new LogEvent
        {
            Id              = new EventId(0u, 99u),
            Timestamp       = new DateTimeOffset(2026, 9, 14, 12, 0, 0, TimeSpan.Zero),
            Level           = LogLevel.Information,
            MessageTemplate = "row 99",
            RawProperties   = truncated,
        };
        await Assert.ThrowsAnyAsync<Exception>(async () => await sse.WriteLogEventAsync(broken, default));

        await sse.WriteErrorAsync("the search failed", default);

        string all = string.Concat(body.Sends);
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

        byte[] truncated = [0x82, 0xa1, (byte)'a', 0xa1, (byte)'b'];
        var broken = new LogEvent
        {
            Id              = new EventId(0u, 99u),
            Timestamp       = new DateTimeOffset(2026, 9, 14, 12, 0, 0, TimeSpan.Zero),
            Level           = LogLevel.Information,
            MessageTemplate = "row 99",
            RawProperties   = truncated,
        };
        await Assert.ThrowsAnyAsync<Exception>(async () => await sse.WriteLogEventAsync(broken, default));

        await sse.WriteErrorAsync("the search failed", default);

        string all = string.Concat(body.Sends);
        Assert.Contains("\"@mt\":\"row 0\"", all);
        Assert.DoesNotContain("row 99", all);
        // Exactly one ROW frame, not one and a half. ("data: " alone would also count the
        // error frame's own payload line.)
        Assert.Equal(1, all.Split("data: {\"@t\"").Length - 1);
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

        string all = string.Concat(body.Sends);
        int copies = all.Split("\"@mt\":\"row 7\"").Length - 1;
        Assert.True(copies == 1, $"row 7 reached the socket {copies} times");
        Assert.True(all.IndexOf("row 7", StringComparison.Ordinal)
                  < all.IndexOf("event: query-error", StringComparison.Ordinal));
        Assert.EndsWith("\n\n", all);
    }
}
