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

    private sealed class StubHandler(HttpStatusCode code, string body) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct) =>
            Task.FromResult(new HttpResponseMessage(code) { Content = new StringContent(body) });
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

    private static (SegmentReplicator Replicator, CapturingLogger Log) Build(HttpStatusCode code, string body)
    {
        var registry = new NodeRegistry();
        registry.Upsert(new PeerPayload { NodeId = 7, Address = "http://peer:5341", Timestamp = DateTimeOffset.UtcNow });
        var log = new CapturingLogger();
        var replicator = new SegmentReplicator(
            Options.Create(new ReplicationOptions { Enabled = true, Secret = "s", PushTimeout = TimeSpan.FromSeconds(5) }),
            registry, log, new SingleClientFactory(new StubHandler(code, body)));
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
}
