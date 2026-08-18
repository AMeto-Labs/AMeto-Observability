using System.Buffers;
using System.Net;
using System.Net.Http.Headers;
using MessagePack;
using Microsoft.AspNetCore.Mvc.Testing;
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
        try
        {
            var opts   = new ServerOptions { NodeId = node, DataDirectory = dir };
            var engine = new StorageEngine(
                Options.Create(opts),
                new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
                NullLogger<StorageEngine>.Instance);

            var info = WriteAndFlush(engine, events);
            byte[] bytes = File.ReadAllBytes(info.FilePath);
            engine.DisposeAsync().AsTask().GetAwaiter().GetResult();
            return (bytes, info.Id.Value);
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
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
