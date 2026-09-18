using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Replication;
using Xunit;

namespace Ameto.Integration.Tests;

/// <summary>
/// Replication ships enabled with an empty seed list — the single-node default — so this
/// loop woke every 10 s for the life of every standalone server to build a payload and a LINQ
/// chain over one entry (itself) and await Task.WhenAll of nothing. It must now do nothing at
/// all until a peer actually exists, and must react the moment one does.
/// </summary>
public sealed class PeerProberIdleTests
{
    /// <summary>Counts requests and answers every ping with a peer payload.</summary>
    private sealed class CountingHandler : HttpMessageHandler
    {
        public int Requests;

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Interlocked.Increment(ref Requests);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NoContent));
        }
    }

    private sealed class StubFactory(HttpMessageHandler handler) : IHttpClientFactory
    {
        public HttpClient CreateClient(string name) => new(handler, disposeHandler: false);
    }

    private static PeerProber NewProber(ReplicationOptions opts, NodeRegistry registry, CountingHandler handler) =>
        new(Options.Create(opts), registry, NullLogger<PeerProber>.Instance, new StubFactory(handler));

    [Fact]
    public async Task No_seeds_and_no_peers_means_no_tick_work()
    {
        var handler  = new CountingHandler();
        var registry = new NodeRegistry();
        registry.EnsureKnown(new NodeId(1), "http://localhost:5341");   // the local node, as at startup

        var prober = NewProber(
            new ReplicationOptions { Enabled = true, ProbeInterval = TimeSpan.FromMilliseconds(20) },
            registry, handler);
        prober.SetLocalNodeId(new NodeId(1));                           // what ReplicationWiring does

        await prober.StartAsync(CancellationToken.None);
        await Task.Delay(400);                                          // ~20 probe intervals
        await prober.StopAsync(CancellationToken.None);
        prober.Dispose();

        Assert.Equal(0, prober.ProbeCycles);
        Assert.Equal(0, Volatile.Read(ref handler.Requests));
    }

    /// <summary>
    /// The registry is never empty — the local node is always in it — so "nothing to probe"
    /// cannot be answered by a count.
    /// </summary>
    [Fact]
    public void Registry_does_not_mistake_the_local_node_for_a_peer()
    {
        var registry = new NodeRegistry();
        var local    = new NodeId(1);
        Assert.False(registry.HasPeerOtherThan(local));

        registry.EnsureKnown(local, "http://localhost:5341");
        Assert.False(registry.HasPeerOtherThan(local));

        registry.Upsert(new PeerPayload { NodeId = 2, Address = "http://peer:5341", Timestamp = DateTimeOffset.UtcNow });
        Assert.True(registry.HasPeerOtherThan(local));
    }

    /// <summary>
    /// OnPeerAdded runs inside NodeRegistry.Upsert, on the thread serving an inbound ping. At
    /// shutdown an Upsert can still hold the old delegate while Dispose runs, and Release on the
    /// disposed semaphore escaped Upsert: the peer was registered, and the ping returned 500.
    /// Reproduced deterministically: a handler subscribed ahead of the prober's disposes it
    /// inside the same invocation, which runs over a snapshot of the list.
    /// </summary>
    [Fact]
    public async Task A_peer_announced_while_the_prober_is_disposed_does_not_fail_the_ping()
    {
        var registry = new NodeRegistry();
        registry.EnsureKnown(new NodeId(1), "http://localhost:5341");

        // A seed keeps the loop on its timer rather than parked on the semaphore being disposed,
        // so StopAsync can still end it.
        var prober = NewProber(
            new ReplicationOptions { Enabled = true, SeedNodes = ["http://seed:5341"], ProbeInterval = TimeSpan.FromMilliseconds(20) },
            registry, new CountingHandler());
        prober.SetLocalNodeId(new NodeId(1));

        registry.PeerAdded += prober.Dispose;                   // first in the invocation list
        await prober.StartAsync(CancellationToken.None);        // OnPeerAdded second

        var thrown = Record.Exception(() => registry.Upsert(
            new PeerPayload { NodeId = 2, Address = "http://peer:5341", Timestamp = DateTimeOffset.UtcNow }));

        Assert.Null(thrown);
        Assert.NotNull(registry.Get(new NodeId(2)));
        await prober.StopAsync(CancellationToken.None);
    }

    /// <summary>An inbound ping is the only way a peer can appear here, and it must wake us.</summary>
    [Fact]
    public async Task A_discovered_peer_wakes_the_parked_loop()
    {
        var handler  = new CountingHandler();
        var registry = new NodeRegistry();
        var prober   = NewProber(
            new ReplicationOptions { Enabled = true, ProbeInterval = TimeSpan.FromMilliseconds(20) },
            registry, handler);

        await prober.StartAsync(CancellationToken.None);
        await Task.Delay(150);
        Assert.Equal(0, prober.ProbeCycles);                            // parked

        // What ReplicationEndpointMapper does when a peer pings this node.
        registry.Upsert(new PeerPayload { NodeId = 7, Address = "http://peer:5341", Timestamp = DateTimeOffset.UtcNow });

        // Wait for the CYCLE, not the request. The handler counts a request before the probe has
        // read the response, and the cycle is counted only after it has; stopping in between
        // cancels that read, and the cancellation ends the loop before it counts. A descheduled
        // loop thread in that gap was enough to fail the assertion on a correct prober. A counted
        // cycle implies the request was made, so the second assertion still holds by construction.
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (prober.ProbeCycles == 0 && DateTime.UtcNow < deadline) await Task.Delay(20);

        await prober.StopAsync(CancellationToken.None);
        prober.Dispose();

        Assert.True(prober.ProbeCycles > 0, "the loop should resume as soon as a peer is discovered");
        Assert.True(Volatile.Read(ref handler.Requests) > 0);
    }

    /// <summary>A configured seed still gets probed immediately at startup, as before.</summary>
    [Fact]
    public async Task Seeds_are_probed_without_waiting_for_discovery()
    {
        var handler = new CountingHandler();
        var prober  = NewProber(
            new ReplicationOptions
            {
                Enabled       = true,
                ProbeInterval = TimeSpan.FromMilliseconds(50),
                SeedNodes     = ["http://seed:5341"],
            },
            new NodeRegistry(), handler);

        await prober.StartAsync(CancellationToken.None);

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (Volatile.Read(ref handler.Requests) == 0 && DateTime.UtcNow < deadline) await Task.Delay(10);

        await prober.StopAsync(CancellationToken.None);
        prober.Dispose();

        Assert.True(Volatile.Read(ref handler.Requests) > 0);
    }
}
