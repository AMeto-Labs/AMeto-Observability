using System.Collections.Concurrent;
using System.Net;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Replication;
using Ameto.Storage;
using MelLogLevel = Microsoft.Extensions.Logging.LogLevel;
using MelEventId = Microsoft.Extensions.Logging.EventId;

namespace Ameto.Integration.Tests;

/// <summary>
/// #48: the receiver learned to answer with terminal statuses — 413 for a body over its limit,
/// 409 with a body naming which of its two conflict causes fired — and the sender kept treating
/// both as one more non-success status: a generic warning naming a code and no reason, with the
/// receiver's one explanatory line discarded. There is no retry loop to stop (a push fires once
/// per flush publication), so what these pin is the REPORTING: the receiver's body reaches the
/// sender's log, on the level the status deserves, and not through the generic branch.
/// </summary>
public sealed class SegmentReplicatorStatusTests : IDisposable
{
    private readonly string _file = Path.Combine(Path.GetTempPath(), "ameto-repl-" + Guid.NewGuid().ToString("N") + ".seg");

    public SegmentReplicatorStatusTests() => File.WriteAllBytes(_file, new byte[128]);
    public void Dispose() { try { File.Delete(_file); } catch { } }

    private sealed class StubHandler(HttpStatusCode code, string body, string? conflictCause = null) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var resp = new HttpResponseMessage(code) { Content = new StringContent(body) };
            if (conflictCause is not null) resp.Headers.Add("X-Ameto-Conflict", conflictCause);
            return Task.FromResult(resp);
        }
    }

    /// <summary>
    /// A body whose stream yields a first chunk and then stalls until the READER's token
    /// cancels — the shape of a peer that sent headers and stopped. The stall must live in the
    /// stream's ReadAsync (which honours the caller's token, as a socket read does), not in
    /// SerializeToStreamAsync: the default ReadAsStreamAsync buffers through the latter with no
    /// token at all, and a stall there hangs the read regardless of any clock the caller set,
    /// which tests the mock rather than the code.
    /// </summary>
    private sealed class StallingContent(string prefix) : HttpContent
    {
        private sealed class StallingStream(byte[] head) : Stream
        {
            private int _served;
            public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct)
            {
                if (_served < head.Length)
                {
                    int n = Math.Min(buffer.Length, head.Length - _served);
                    head.AsMemory(_served, n).CopyTo(buffer);
                    _served += n;
                    return n;
                }
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);   // stalls until the reader's clock fires
                return 0;
            }
            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => false;
            public override long Length => throw new NotSupportedException();
            public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
            public override void Flush() { }
            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        }

        protected override Task<Stream> CreateContentReadStreamAsync() =>
            Task.FromResult<Stream>(new StallingStream(System.Text.Encoding.UTF8.GetBytes(prefix)));
        protected override Task SerializeToStreamAsync(Stream stream, System.Net.TransportContext? context) =>
            throw new NotSupportedException("read via CreateContentReadStreamAsync");
        protected override bool TryComputeLength(out long length) { length = -1; return false; }
    }

    private sealed class StallingHandler(HttpStatusCode code, string prefix) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct) =>
            Task.FromResult(new HttpResponseMessage(code) { Content = new StallingContent(prefix) });
    }

    private sealed class SingleClientFactory(HttpMessageHandler handler) : IHttpClientFactory
    {
        // The buffer cap is DELIBERATE and small: with ResponseHeadersRead the content is
        // never buffered, so the cap must never fire -- while a regression back to a plain
        // buffered SendAsync trips it on the huge-body test and collapses the classified
        // response into the generic failure, which is the exact rejected design.
        public HttpClient CreateClient(string name) =>
            new(handler, disposeHandler: false) { MaxResponseContentBufferSize = 8 * 1024 };
    }

    private sealed class CapturingLogger : ILogger<SegmentReplicator>
    {
        public readonly ConcurrentQueue<(MelLogLevel Level, string Message)> Lines = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(MelLogLevel logLevel) => true;
        public void Log<TState>(MelLogLevel logLevel, MelEventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Lines.Enqueue((logLevel, formatter(state, exception)));
    }

    private static SegmentInfo Segment(string file) => new()
    {
        Id                = new SegmentId(3),
        NodeId            = new NodeId(0),          // matches the replicator's default local id
        FilePath          = file,
        MinTimestampTicks = 1,
        MaxTimestampTicks = 2,
        EventCount        = 1,
        MinLevel          = Ameto.Core.LogLevel.Information,
        CompressedBytes   = 128,
        UncompressedBytes = 128,
    };

    private static (SegmentReplicator Replicator, CapturingLogger Log) Build(
        HttpStatusCode code, string body, string? conflictCause = null, HttpMessageHandler? handler = null)
    {
        var registry = new NodeRegistry();
        registry.Upsert(new PeerPayload { NodeId = 7, Address = "http://peer:5341", Timestamp = DateTimeOffset.UtcNow });
        var log = new CapturingLogger();
        var replicator = new SegmentReplicator(
            Options.Create(new ReplicationOptions { Enabled = true, Secret = "s", PushTimeout = TimeSpan.FromSeconds(2) }),
            registry, log, new SingleClientFactory(handler ?? new StubHandler(code, body, conflictCause)));
        return (replicator, log);
    }

    private static async Task<(MelLogLevel Level, string Message)> WaitForLineAsync(
        CapturingLogger log, Func<string, bool> match)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            foreach (var line in log.Lines)
                if (match(line.Message)) return line;
            await Task.Delay(25);
        }
        throw new TimeoutException("expected log line never appeared; saw: " +
            string.Join(" | ", log.Lines.Select(l => l.Message)));
    }

    /// <summary>
    /// The body is read off the STREAM, bounded — not buffered whole with a cap. A capped
    /// buffered send was tried and rejected: the cap fires inside SendAsync itself, so an
    /// oversized body (a proxy's HTML error page) threw before the status was ever seen and
    /// the classified 409/413 collapsed into a generic "push failed". The marker sits inside
    /// the first 4 KB; the megabyte behind it must cost nothing and lose nothing.
    /// </summary>
    [Fact]
    public async Task A_huge_body_does_not_cost_the_classification()
    {
        string marker = "limit is 1024 bytes: raise Ameto:Replication:MaxSegmentBytes on the receiver";
        var (replicator, log) = Build(HttpStatusCode.RequestEntityTooLarge, marker + new string('x', 1_000_000));
        using (replicator)
        {
            replicator.OnSegmentFlushed(Segment(_file));

            var line = await WaitForLineAsync(log, m => m.Contains("MaxSegmentBytes"));
            Assert.Equal(MelLogLevel.Warning, line.Level);
            Assert.Contains(marker, line.Message);
            Assert.DoesNotContain(log.Lines, l => l.Message.Contains("Push segment") && l.Message.Contains("failed"));
        }
    }

    [Fact]
    public async Task A_413_reaches_the_log_with_the_receivers_body_and_not_as_a_generic_status()
    {
        const string receiverSaid = "limit is 1024 bytes: raise Ameto:Replication:MaxSegmentBytes on the receiver";
        var (replicator, log) = Build(HttpStatusCode.RequestEntityTooLarge, receiverSaid);
        using (replicator)
        {
            replicator.OnSegmentFlushed(Segment(_file));

            var line = await WaitForLineAsync(log, m => m.Contains("MaxSegmentBytes"));
            Assert.Equal(MelLogLevel.Warning, line.Level);
            Assert.Contains(receiverSaid, line.Message);                       // the one explanatory line, kept
            Assert.DoesNotContain(log.Lines, l => l.Message.Contains("returned"));   // not the generic branch
        }
    }

    /// <summary>
    /// A stub, and deliberately so — which makes this a PLUMBING test: it proves the body the
    /// wire carried reaches the log verbatim, and nothing else. Whether the RECEIVER puts the
    /// right words in that body is a property of the endpoint, pinned where the endpoint is
    /// real: <see cref="ReplicationSegmentEndpointTests"/> asserts one 409 body per conflict
    /// cause. The first version of this test claimed the quoting proved the sender "no longer
    /// guesses" — with a stubbed body that claim tested the stub.
    /// </summary>
    [Fact]
    public async Task A_409_reaches_the_log_with_whatever_body_the_wire_carried()
    {
        const string receiverSaid = "names a segment id this node has already allocated for itself";
        var (replicator, log) = Build(HttpStatusCode.Conflict, receiverSaid);
        using (replicator)
        {
            replicator.OnSegmentFlushed(Segment(_file));

            var line = await WaitForLineAsync(log, m => m.Contains("409"));
            Assert.Equal(MelLogLevel.Error, line.Level);
            Assert.Contains(receiverSaid, line.Message);
        }
    }

    /// <summary>
    /// The three 409 causes do not share a framing, and the receiver says which one fired in
    /// X-Ameto-Conflict. One branch used to wrap all three in "two nodes appear to share that
    /// NodeId" at Error — so the unreadable-incumbent body arrived inside a deployment
    /// diagnosis nobody made, and an operator went renumbering nodes over one bad file.
    /// </summary>
    [Fact]
    public async Task A_409_for_an_unreadable_incumbent_is_a_warning_about_a_file_not_an_error_about_nodes()
    {
        const string receiverSaid = "occupied by a file this node could not read";
        var (replicator, log) = Build(HttpStatusCode.Conflict, receiverSaid, conflictCause: "unreadable-incumbent");
        using (replicator)
        {
            replicator.OnSegmentFlushed(Segment(_file));

            var line = await WaitForLineAsync(log, m => m.Contains("409"));
            Assert.Equal(MelLogLevel.Warning, line.Level);
            Assert.Contains(receiverSaid, line.Message);
            Assert.DoesNotContain("two nodes", line.Message);
        }
    }

    /// <summary>
    /// ResponseHeadersRead moved the body read out from under HttpClient.Timeout, and the push
    /// task's token is None — so a peer that sent headers and then held the socket kept the
    /// read hanging for as long as it cared to (measured: thirty seconds against a two-second
    /// client timeout). The read now carries its own clock, and expiry preserves the
    /// classification instead of collapsing it.
    /// </summary>
    [Fact]
    public async Task A_stalled_body_times_out_and_keeps_the_classification()
    {
        var (replicator, log) = Build(HttpStatusCode.RequestEntityTooLarge, "",
            handler: new StallingHandler(HttpStatusCode.RequestEntityTooLarge, "limit is"));
        using (replicator)
        {
            replicator.OnSegmentFlushed(Segment(_file));

            var line = await WaitForLineAsync(log, m => m.Contains("MaxSegmentBytes"));
            Assert.Equal(MelLogLevel.Warning, line.Level);
            // The bytes that DID arrive are kept — the interesting part of a ProblemDetails is
            // at the front — and the marker says the rest never came.
            Assert.Contains("limit is", line.Message);
            Assert.Contains("(body read timed out)", line.Message);
        }
    }
}
