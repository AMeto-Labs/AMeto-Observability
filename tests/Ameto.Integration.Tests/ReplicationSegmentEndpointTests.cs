using System.Buffers;
using System.Net;
using System.Net.Http.Headers;
using MessagePack;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Integration.Tests;

/// <summary>
/// The receiving half of replication, through HTTP rather than through the engine: what
/// <c>POST /api/replication/segments/{nodeId}/{segmentId}</c> promises the sender.
///
/// <para>Two promises, and they pull in opposite directions. A re-push of a file this node
/// already holds is NORMAL traffic — the endpoint moves each body into a path derived from the
/// route, with <c>overwrite: true</c> — so it has to keep succeeding, and registering it twice
/// would serve its events twice. A genuinely different file arriving at an occupied (node, id)
/// is the opposite: accepting it drops whatever is already there out of the catalog, and the
/// catalog is the only thing queries, retention and the merge planner read, so the displaced
/// file would go on holding disk while being served by nobody and expired by nothing. It is
/// refused, and the sender is told — a 409 rather than a log line on the receiver, because the
/// only symptom on either side is that everything looks fine.</para>
/// </summary>
public sealed class ReplicationSegmentEndpointTests : IClassFixture<ReplicationWebAppFactory>
{
    private readonly ReplicationWebAppFactory _factory;
    private readonly HttpClient               _client;

    public ReplicationSegmentEndpointTests(ReplicationWebAppFactory factory)
    {
        _factory = factory;
        _client  = factory.CreateClient();
        _client.DefaultRequestHeaders.Add("X-Ameto-Replication", ReplicationWebAppFactory.Secret);
    }

    private Task<HttpResponseMessage> PushAsync(uint node, ulong id, byte[] body)
    {
        var content = new ByteArrayContent(body);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        return _client.PostAsync($"/api/replication/segments/{node}/{id}", content);
    }

    /// <summary>
    /// A peer's segment, and then the same bytes again. Both are accepted and the key holds one
    /// entry: the second push refreshes what the first registered rather than being mistaken for
    /// an intruder, which is what any "one file per key, first wins" rule has to get right before
    /// it is allowed to refuse anything.
    /// </summary>
    [Fact]
    public async Task A_re_push_of_the_same_segment_succeeds_and_registers_once()
    {
        var storage = _factory.Services.GetRequiredService<StorageEngine>();
        var (bytes, id) = ForeignSegment(new NodeId(7), events: 6);

        Assert.Equal(HttpStatusCode.NoContent, (await PushAsync(7, id, bytes)).StatusCode);
        Assert.Equal(HttpStatusCode.NoContent, (await PushAsync(7, id, bytes)).StatusCode);

        // By (node, id), never by id alone: the node under test allocates from its own counter
        // and starts at 1 like every other node, so its segments share ids with this peer's as a
        // matter of course — which is the whole reason the catalog is keyed by the pair.
        var only = Assert.Single(storage.ListSegments(), s => s.NodeId.Value == 7 && s.Id.Value == id);
        Assert.Equal(6u, only.EventCount);
        Assert.Equal(Path.Combine(_factory.SegDir, $"7-{id}.seg"), only.FilePath);
    }

    /// <summary>
    /// The refusal. The body is a real segment written by THIS node, so it carries this node's id
    /// in its header and lands on a key the local flush already holds — which is what two nodes
    /// sharing a NodeId looks like from the receiving end.
    ///
    /// <para>Three things are asserted, and the last is the one a 409 alone would not give: the
    /// status, the local entry still pointing at its own file, and the refused body GONE from the
    /// segments directory. The catalog is rebuilt from that directory on every start, in
    /// enumeration order, so a refused file left lying there would let the next boot pick the
    /// winner by coin toss and undo the refusal — silently, and only after a restart.</para>
    /// </summary>
    [Fact]
    public async Task A_different_file_at_an_occupied_key_is_refused_and_leaves_nothing_behind()
    {
        var storage = _factory.Services.GetRequiredService<StorageEngine>();

        // A segment written by the node under test: {0}-{id}-{min}-{max}.seg, in the catalog,
        // being served.
        var local = WriteAndFlush(storage, events: 8);
        byte[] body = await File.ReadAllBytesAsync(local.FilePath);

        // Same node id, same segment id, different file — the route decides the name, so this
        // arrives as {0}-{id}.seg and cannot overwrite the file it collides with.
        var response = await PushAsync(local.NodeId.Value, local.Id.Value, body);
        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);

        var still = Assert.Single(storage.ListSegments(),
            s => s.NodeId.Value == local.NodeId.Value && s.Id.Value == local.Id.Value);
        Assert.Equal(local.FilePath, still.FilePath);
        Assert.True(File.Exists(local.FilePath), "the file being served was deleted by a push it refused");
        Assert.False(File.Exists(Path.Combine(_factory.SegDir, $"{local.NodeId.Value}-{local.Id.Value}.seg")),
            "the refused body was left in the segments directory, where the next boot's catalog " +
            "scan will pick between it and the file being served in enumeration order");
    }

    /// <summary>
    /// A body that is not a segment is not silently accepted either. The endpoint reports it and
    /// unlinks what it wrote, so the segments directory never carries a file the engine has no
    /// entry for.
    /// </summary>
    [Fact]
    public async Task A_body_that_is_not_a_segment_is_rejected_and_unlinked()
    {
        var response = await PushAsync(9, 4242, [0xDE, 0xAD, 0xBE, 0xEF]);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.False(File.Exists(Path.Combine(_factory.SegDir, "9-4242.seg")));
    }

    // ── What the published contract says ──────────────────────────────────────

    /// <summary>
    /// The 400 above is not the only one, and the other one arrives from somewhere the handler
    /// cannot see. <c>{nodeId}</c> binds as <c>uint</c> and <c>{segmentId}</c> as <c>ulong</c>,
    /// so a URL carrying anything else is refused by model binding with an EMPTY body — before
    /// the handler runs, and therefore before the secret check the 401 row promises.
    ///
    /// <para>Asserted with NO secret header at all, which is what makes the ordering visible:
    /// a request that is unauthenticated AND unroutable comes back 400, not 401. Pinned because
    /// the table in docs/API.md used to explain 400 as "the body did not read as a segment"
    /// alone, which sends anyone debugging one of these looking at their bytes.</para>
    ///
    /// <para>The second assertion is what proves the handler never ran, and it is the one that
    /// carries the ordering: the body could not have been staged, because nothing in this
    /// process ever looked at it. The response body is NOT asserted on — binding failures are
    /// bare under a Production host and carry a framework <c>ProblemDetails</c> under a
    /// Development one, and this harness is the second.</para>
    /// </summary>
    [Fact]
    public async Task A_route_that_does_not_bind_is_refused_before_the_secret_is_checked()
    {
        using var anonymous = _factory.CreateClient();   // deliberately without X-Ameto-Replication

        var content = new ByteArrayContent([0xDE, 0xAD, 0xBE, 0xEF]);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        var response = await anonymous.PostAsync("/api/replication/segments/abc/1", content);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Empty(Directory.GetFiles(_factory.SegDir, "abc-*"));
    }

    /// <summary>
    /// The ceiling the endpoint puts on a body, which is configuration and therefore breakable
    /// in silence. Kestrel's default is 30 MB; a cold segment is routinely larger, and over the
    /// limit the body read throws and used to come back as the 500 the contract marks
    /// RETRYABLE — a peer re-pushing a merged segment forever, with nothing logged anywhere.
    ///
    /// <para>What is asserted is the WIRING: that the value configured for this node reaches
    /// the request before a byte of the body is read. The enforcement itself belongs to Kestrel
    /// and cannot be reached from here at all — <c>TestServer</c> supplies no
    /// <c>IHttpMaxRequestBodySizeFeature</c>, so under this harness there is no limit to
    /// exceed. Hence a stub feature and an assertion on what the endpoint wrote into it: a
    /// build that stopped raising the limit, or raised it to the wrong number, fails here even
    /// though no oversized body can be sent.</para>
    /// </summary>
    [Fact]
    public async Task The_endpoint_raises_the_body_limit_to_the_configured_maximum()
    {
        long configured = _factory.Services
            .GetRequiredService<IOptions<Ameto.Replication.ReplicationOptions>>()
            .Value.MaxSegmentBytes;
        Assert.Equal(ReplicationWebAppFactory.MaxSegmentBytes, configured);   // setup: bound from config

        var limit = new StubBodySizeLimit();
        await _factory.Server.SendAsync(ctx =>
        {
            ctx.Request.Method      = HttpMethods.Post;
            ctx.Request.Path        = "/api/replication/segments/9/4243";
            ctx.Request.ContentType = "application/octet-stream";
            ctx.Request.Headers["X-Ameto-Replication"] = ReplicationWebAppFactory.Secret;
            ctx.Request.Body = new MemoryStream([0xDE, 0xAD, 0xBE, 0xEF]);
            ctx.Features.Set<IHttpMaxRequestBodySizeFeature>(limit);
        });

        Assert.Equal(configured, limit.MaxRequestBodySize);
    }

    /// <summary>
    /// Stands in for the Kestrel feature the endpoint writes its ceiling into. Writable, because
    /// a read-only feature is exactly the case the endpoint must leave alone.
    /// </summary>
    private sealed class StubBodySizeLimit : IHttpMaxRequestBodySizeFeature
    {
        public bool  IsReadOnly          => false;
        public long? MaxRequestBodySize  { get; set; } = 30_000_000;   // the framework's own default
    }

    /// <summary>
    /// The refusal, in the arrangement it was written for and the one the test above cannot
    /// reach: TWO PEERS, neither of them this node, both configured as the same NodeId. Each
    /// allocates segment ids from its own counter, so both produce a segment 1, and the endpoint
    /// derives the file name from the ROUTE — so unlike a local segment (which is
    /// <c>{node}-{id}-{min}-{max}.seg</c> and therefore cannot be landed on) the two arrive at
    /// the SAME path.
    ///
    /// <para>Path equality is not what tells a re-push from an intruder here, and the move used
    /// to run before anything decided anything: the first peer's bytes were already gone when
    /// the import was consulted, and it then compared the path with itself and answered 204.
    /// The second push must be refused with the first peer's file untouched — on disk and in the
    /// catalog — because this node is the only party that can see the two are different, and
    /// each sender otherwise records a successful push.</para>
    /// </summary>
    [Fact]
    public async Task A_second_peer_carrying_the_same_node_id_does_not_overwrite_the_first()
    {
        var storage = _factory.Services.GetRequiredService<StorageEngine>();
        const uint duplicated = 21;

        var (first,  id)  = ForeignSegment(new NodeId(duplicated), events: 6);
        var (second, id2) = ForeignSegment(new NodeId(duplicated), events: 11);
        Assert.Equal(id, id2);   // setup: both peers' counters start at 1, so the ids collide

        Assert.Equal(HttpStatusCode.NoContent, (await PushAsync(duplicated, id, first)).StatusCode);
        string path = Path.Combine(_factory.SegDir, $"{duplicated}-{id}.seg");

        var response = await PushAsync(duplicated, id, second);
        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);

        // The catalog still names the first peer's segment...
        var held = Assert.Single(storage.ListSegments(), s => s.NodeId.Value == duplicated && s.Id.Value == id);
        Assert.Equal(6u, held.EventCount);
        Assert.Equal(path, held.FilePath);

        // ...and so does the disk.
        Assert.Equal(first, await File.ReadAllBytesAsync(path));
        Assert.Empty(Directory.GetFiles(_factory.SegDir, "*.seg.tmp"));
    }

    /// <summary>
    /// TWO PUSHES TO ONE ROUTE, AT ONCE. A peer retrying a request whose response it never saw,
    /// or re-pushing a segment it has just re-compressed, sends the same (nodeId, segmentId)
    /// twice with the two bodies in the air together. Both are legitimate, and the endpoint used
    /// to give them one staging name — <c>filePath + ".tmp"</c>, derived from the route and
    /// nothing else — so the second <c>File.Create</c> landed on the first request's file.
    ///
    /// <para>That was harmless while the endpoint renamed BEFORE calling the engine: the import
    /// read its metadata from the final path, so whatever bytes ended up there were the bytes it
    /// described. Moving the rename into the engine split the read from the rename and opened a
    /// window between them — long enough for the other request to truncate the file that the
    /// first one has already measured and is about to publish. What is registered then describes
    /// one segment while the bytes under it are half of another, and nothing on either node looks
    /// wrong. On Windows the same overlap is a sharing violation on <c>File.Create</c> instead,
    /// which is a 500 on a healthy push; the fault is one bug with two faces, and the assertion
    /// below — both accepted, one entry, and the file on disk equal to what was sent — is failed
    /// by both of them.</para>
    ///
    /// <para>The overlap is arranged rather than hoped for: each body writes its first bytes,
    /// waits until the other has done the same, and only then finishes. Both endpoints therefore
    /// hold their staging file open at the same moment, which is the state a shared name cannot
    /// survive and a per-request name does not notice.</para>
    /// </summary>
    [Fact]
    public async Task Two_pushes_of_one_segment_in_flight_together_are_both_accepted()
    {
        var storage = _factory.Services.GetRequiredService<StorageEngine>();
        var (bytes, id) = ForeignSegment(new NodeId(31), events: 9);

        var overlap = new Rendezvous(parties: 2);
        var first   = PushStreamedAsync(31, id, bytes, overlap);
        var second  = PushStreamedAsync(31, id, bytes, overlap);

        var responses = await Task.WhenAll(first, second);
        Assert.All(responses, r => Assert.Equal(HttpStatusCode.NoContent, r.StatusCode));

        // One entry, describing the segment that was actually sent...
        var only = Assert.Single(storage.ListSegments(), s => s.NodeId.Value == 31 && s.Id.Value == id);
        Assert.Equal(9u, only.EventCount);
        Assert.Equal(Path.Combine(_factory.SegDir, $"31-{id}.seg"), only.FilePath);

        // ...and the bytes under it are that segment, not a body one request truncated while the
        // other was measuring it.
        Assert.Equal(bytes, await File.ReadAllBytesAsync(only.FilePath));
        Assert.Empty(Directory.GetFiles(_factory.SegDir, "*.seg.tmp"));
    }

    // ── Overlapping pushes ────────────────────────────────────────────────────

    /// <summary>
    /// A push whose body is delivered in two parts, pausing in the middle until every other
    /// participant has reached the same point. The pause is what makes the overlap a fact instead
    /// of a hope: a body small enough to leave in one write can be finished, imported and renamed
    /// before the second request has opened anything, and then nothing is concurrent at all.
    /// </summary>
    private Task<HttpResponseMessage> PushStreamedAsync(uint node, ulong id, byte[] body, Rendezvous at)
    {
        var content = new HalfThenWaitContent(body, at);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        return _client.PostAsync($"/api/replication/segments/{node}/{id}", content);
    }

    private sealed class HalfThenWaitContent(byte[] body, Rendezvous at) : HttpContent
    {
        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
        {
            int head = Math.Max(1, body.Length / 8);
            await stream.WriteAsync(body.AsMemory(0, head));
            await stream.FlushAsync();

            // The endpoint has this request's staging file open now, and will keep it open until
            // the rest arrives.
            await at.ArriveAsync();

            await stream.WriteAsync(body.AsMemory(head));
        }

        protected override bool TryComputeLength(out long length)
        {
            length = body.Length;
            return true;
        }
    }

    /// <summary>
    /// Releases everyone once <paramref name="parties"/> have arrived. Bounded, because a harness
    /// that cannot overlap two requests must fail this test rather than hang it.
    /// </summary>
    private sealed class Rendezvous(int parties)
    {
        private readonly TaskCompletionSource _all = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _arrived;

        public Task ArriveAsync()
        {
            if (Interlocked.Increment(ref _arrived) >= parties) _all.TrySetResult();
            return _all.Task.WaitAsync(TimeSpan.FromSeconds(30));
        }
    }

    // ── Segment bodies ────────────────────────────────────────────────────────

    private static byte[] Props(int i)
    {
        var buf = new ArrayBufferWriter<byte>(48);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write((long)i);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>
    /// A segment file produced by a DIFFERENT node, built by running a throwaway engine under
    /// that node's id — the header has to be genuine, because the receiving engine keys the
    /// catalog off what it reads out of the file and not off the route.
    /// </summary>
    private static (byte[] Bytes, ulong Id) ForeignSegment(NodeId node, int events)
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-peer-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        var opts   = new ServerOptions { NodeId = node, DataDirectory = dir };
        var engine = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        try
        {
            var info = WriteAndFlush(engine, events);
            byte[] bytes = File.ReadAllBytes(info.FilePath);
            return (bytes, info.Id.Value);
        }
        finally
        {
            // The dispose used to stand inside the try, below a write and a read that can both
            // throw. The directory delete under it was already guarded — but it cannot succeed
            // with the engine's write-ahead log still mapped, so the guard only made the leak
            // silent: the throw propagated, the swallowed delete failed, and the run left a
            // directory behind. Disposing here is what makes the delete below able to work.
            engine.DisposeAsync().AsTask().GetAwaiter().GetResult();
            try { Directory.Delete(dir, true); } catch { }
        }
    }

    /// <summary>
    /// Writes <paramref name="events"/> events into an engine and flushes them, returning the
    /// segment that appeared. Identified by PATH rather than by id — these tests share one server
    /// and a peer's replica can already be sitting under the same id.
    /// </summary>
    private static SegmentInfo WriteAndFlush(StorageEngine engine, int events)
    {
        long baseTicks = DateTime.UtcNow.Ticks;
        var  known     = engine.ListSegments().Select(static s => s.FilePath).ToHashSet();

        for (int i = 0; i < events; i++)
            Assert.True(engine.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerMillisecond,
                Level                    = Ameto.Core.LogLevel.Information,
                MessageTemplatePoolIndex = engine.TemplatePool.Intern("replicated {n}"),
            }, Props(i)));

        engine.FlushHotTierAsync().GetAwaiter().GetResult();
        return Assert.Single(engine.ListSegments(), s => !known.Contains(s.FilePath));
    }
}

/// <summary>
/// The standard factory runs standalone, so <c>MapReplicationEndpoints</c> never registers
/// anything — the peer routes exist only when <c>Ameto:Replication:Enabled</c> is true. This one
/// turns replication on and sets the shared secret the receive endpoints authenticate against
/// (blank fails closed, so it cannot be left out).
/// </summary>
public sealed class ReplicationWebAppFactory : WebApplicationFactory<Program>
{
    public const string Secret = "replication-integration-secret";

    /// <summary>
    /// Deliberately not the default and not a round number: the assertion on it is only worth
    /// anything if the value could have come from nowhere else.
    /// </summary>
    public const long MaxSegmentBytes = 123_456_789L;

    private readonly string _tempDir =
        Path.Combine(Path.GetTempPath(), "Ameto-repl-" + Guid.NewGuid().ToString("N")[..8]);

    public string SegDir => Path.Combine(_tempDir, "segments");

    protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
    {
        builder.UseSetting("Ameto:DataDirectory", _tempDir);
        builder.UseSetting("Ameto:HttpPort", "0");
        builder.UseSetting("Ameto:Cluster:Enabled", "false");
        builder.UseSetting("Ameto:Replication:Enabled", "true");
        builder.UseSetting("Ameto:Replication:Secret", Secret);
        builder.UseSetting("Ameto:Replication:MaxSegmentBytes", MaxSegmentBytes.ToString());

        string webRoot = Path.Combine(_tempDir, "wwwroot");
        Directory.CreateDirectory(webRoot);
        File.WriteAllText(Path.Combine(webRoot, "index.html"), "<!doctype html><title>stub</title>");
        builder.UseSetting(Microsoft.AspNetCore.Hosting.WebHostDefaults.WebRootKey, webRoot);

        builder.ConfigureServices(services =>
        {
            var opts = new ServerOptions
            {
                NodeId        = NodeId.Local,
                DataDirectory = _tempDir,
                HttpPort      = 0,
                HotTier       = new HotTierOptions
                {
                    MaxSizeBytes = 8 * 1024 * 1024,
                    MaxAge       = TimeSpan.FromMinutes(60),
                },
                Retention = new RetentionConfig(),
            };

            services.AddSingleton(opts);
            services.AddSingleton<IOptions<ServerOptions>>(_ => Options.Create(opts));
        });
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        if (disposing && Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }
}
