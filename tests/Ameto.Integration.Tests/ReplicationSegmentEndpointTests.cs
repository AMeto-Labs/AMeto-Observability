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

    /// <summary>
    /// A client that STREAMS its request body, for the one test that needs a push to still be in
    /// flight while another runs. <see cref="_client"/> cannot do it: the factory wraps the test
    /// handler in a <c>RedirectHandler</c>, which copies the whole body up front so it could
    /// resend it after a redirect, so nothing is dispatched until the content has been read to
    /// the end. Measured: a body that pauses mid-write leaves the endpoint's <c>File.Create</c>
    /// unreached for 30 s through <see cref="_client"/> and reached within a millisecond through
    /// this one. <c>TestServer.CreateClient</c> is the raw handler with neither wrapper on it.
    /// </summary>
    private readonly HttpClient _streaming;

    public ReplicationSegmentEndpointTests(ReplicationWebAppFactory factory)
    {
        _factory = factory;
        _client  = factory.CreateClient();
        _client.DefaultRequestHeaders.Add("X-Ameto-Replication", ReplicationWebAppFactory.Secret);

        _streaming = factory.Server.CreateClient();
        _streaming.DefaultRequestHeaders.Add("X-Ameto-Replication", ReplicationWebAppFactory.Secret);
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
    /// <para>The STATUS is what carries the ordering, and it carries all of it: this client sends
    /// no secret, so a handler that ran at all would answer 401 on its first line. 400 can only
    /// mean the request never got there. The response body is NOT asserted on — binding failures
    /// are bare under a Production host and carry a framework <c>ProblemDetails</c> under a
    /// Development one, and this harness is the second.</para>
    ///
    /// <para>The second assertion used to be <c>GetFiles(SegDir, "abc-*")</c>, credited in this
    /// docstring with proving the handler never ran — and it could not fail, because the staging
    /// name is built from the BOUND value. Its replacement counts the whole directory, and it
    /// cannot fail either: this client sends no secret, so a handler that ran at all would
    /// answer 401 on its first line and stage nothing whatever the binder allowed. The STATUS
    /// assertion is the entire proof — an executed handler cannot answer 400 — and the count
    /// below is a tidiness check on the shared directory, kept because it costs one line and
    /// credited with nothing.</para>
    /// </summary>
    [Fact]
    public async Task A_route_that_does_not_bind_is_refused_before_the_secret_is_checked()
    {
        using var anonymous = _factory.CreateClient();   // deliberately without X-Ameto-Replication

        Directory.CreateDirectory(_factory.SegDir);
        int before = Directory.GetFiles(_factory.SegDir).Length;

        var content = new ByteArrayContent([0xDE, 0xAD, 0xBE, 0xEF]);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        var response = await anonymous.PostAsync("/api/replication/segments/abc/1", content);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal(before, Directory.GetFiles(_factory.SegDir).Length);
    }

    /// <summary>
    /// The ceiling the endpoint puts on a body, which is configuration and therefore breakable
    /// in silence. Kestrel's default is 30 MB; a pushed level segment can clear it (a flush
    /// starts from a 64 MB budget with LZ4 the only thing between that and the wire), and over
    /// the limit the body read threw and came back as the 500 the contract marks RETRYABLE —
    /// while the sender pushes once per flush and logged a status with no reason, which made it
    /// permanent, silent non-replication rather than the retry storm this docstring once
    /// described.
    ///
    /// <para>What is asserted is the WIRING: that the value configured for this node reaches
    /// the request before a byte of the body is read. The enforcement itself belongs to Kestrel
    /// and cannot be reached from here at all — <c>TestServer</c> supplies no
    /// <c>IHttpMaxRequestBodySizeFeature</c>, so under this harness there is no limit to
    /// exceed. Hence a stub feature and an assertion on what the endpoint wrote into it: a
    /// build that stopped raising the limit, or raised it to the wrong number, fails here even
    /// though no oversized body can be sent.</para>
    ///
    /// <para>The ENFORCEMENT is what is out of reach, not the answer to it. What the endpoint
    /// replies once Kestrel has thrown is reachable and is pinned separately — see
    /// <see cref="A_body_over_the_ceiling_is_refused_as_413_and_not_as_a_retryable_500"/>. Reading
    /// the sentence above as covering both is what left the 413 mapping with no test for two
    /// review rounds.</para>
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
    /// The ANSWER to an oversized body, which is a different thing from the ceiling above and was
    /// pinned by nothing. The test above asserts the number the endpoint hands Kestrel; this one
    /// asserts what the endpoint says when Kestrel enforces it — a distinct terminal status with
    /// its own text and its own retry semantics, published in docs/API.md.
    ///
    /// <para>The comment on the test above says the enforcement "cannot be reached from here at
    /// all", which is true of the enforcement and false of the response mapping. Kestrel signals
    /// an over-long body by throwing <c>BadHttpRequestException</c> with status 413 OUT OF THE
    /// BODY READ, and a body whose read throws exactly that is a body this harness can supply. So
    /// the seam is the same one the ceiling test uses, and what it reaches is the catch filter.</para>
    ///
    /// <para>Worth its own test because the whole content of the fix is which status comes back:
    /// a reordered catch clause, a dropped <c>when</c> filter, or an exception-handling middleware
    /// that intercepts <c>BadHttpRequestException</c> first would put every oversized push back on
    /// the 500 the contract marks RETRYABLE — the exact regression this branch exists to prevent
    /// — with the whole suite still green. Deleting the filter (<c>when (false)</c>) leaves
    /// Ameto.Integration.Tests at Failed 0, Passed 101 without this test, and fails here with
    /// Expected 413 / Actual 500 with it.</para>
    /// </summary>
    [Fact]
    public async Task A_body_over_the_ceiling_is_refused_as_413_and_not_as_a_retryable_500()
    {
        var ctx = await _factory.Server.SendAsync(ctx =>
        {
            ctx.Request.Method      = HttpMethods.Post;
            ctx.Request.Path        = "/api/replication/segments/9/4244";
            ctx.Request.ContentType = "application/octet-stream";
            ctx.Request.Headers["X-Ameto-Replication"] = ReplicationWebAppFactory.Secret;
            ctx.Request.Body = new RefusesToRead();
        });

        Assert.Equal(StatusCodes.Status413PayloadTooLarge, ctx.Response.StatusCode);

        // The staged body is unlinked on this path too. It matters more here than on the disk-error
        // path: the sender is being told not to come back, so nothing else will ever arrive to
        // overwrite what a failed push left in the segments directory.
        Assert.Empty(Directory.GetFiles(_factory.SegDir, "9-4244.*"));
    }

    /// <summary>
    /// A request body that fails the way Kestrel fails one over <c>MaxRequestBodySize</c>: not a
    /// short read and not an <c>IOException</c>, but <c>BadHttpRequestException</c> carrying 413,
    /// thrown out of the read itself. That is the only shape the endpoint's catch filter matches,
    /// so standing in for it is what makes the assertion about the real branch.
    /// </summary>
    private sealed class RefusesToRead : Stream
    {
        public override bool CanRead  => true;
        public override bool CanSeek  => false;
        public override bool CanWrite => false;
        public override long Length   => throw new NotSupportedException();
        public override long Position { get => 0; set => throw new NotSupportedException(); }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default) =>
            throw new BadHttpRequestException(
                "Request body too large.", StatusCodes.Status413PayloadTooLarge);

        public override int Read(byte[] buffer, int offset, int count) =>
            throw new BadHttpRequestException(
                "Request body too large.", StatusCodes.Status413PayloadTooLarge);

        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
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
    /// TWO PUSHES TO ONE ROUTE, OVERLAPPING INSIDE THE ENDPOINT. A peer retrying a request whose
    /// response it never saw, or re-pushing a segment it has just re-compressed, sends the same
    /// (nodeId, segmentId) twice with the two bodies in the air together. Both are legitimate,
    /// and the endpoint used to give them one staging name — <c>filePath + ".tmp"</c>, derived
    /// from the route and nothing else — so the second <c>File.Create</c> landed on the first
    /// request's file.
    ///
    /// <para>That was harmless while the endpoint renamed BEFORE calling the engine: the import
    /// read its metadata from the final path, so whatever bytes ended up there were the bytes it
    /// described. Moving the rename into the engine split the read from the rename and opened a
    /// window between them — long enough for the other request to truncate the file that the
    /// first one has already measured and is about to publish. What is registered then describes
    /// one segment while the bytes under it are half of another, and nothing on either node looks
    /// wrong. On Windows the same overlap is a sharing violation on <c>File.Create</c> instead —
    /// it opens with <c>FileShare.None</c> — which is a 500 on a healthy push; the fault is one
    /// bug with two faces, and the assertions below (both accepted, one entry, the file on disk
    /// equal to what was sent) are failed by both of them.</para>
    ///
    /// <para><b>The overlap used to be hoped for, and this docstring said otherwise.</b> Two
    /// clients each wrote a first slice of body and rendezvoused before writing the rest, which
    /// was described here as arranging the overlap. It arranged nothing, for a blunter reason
    /// than a lost race: the factory's client does not stream at all. Its <c>RedirectHandler</c>
    /// copies the whole body up front so it could resend it, so the pause happened while the
    /// request was still being BUFFERED and neither request had been dispatched. The rendezvous
    /// synchronised two client-side copies into memory; the two handlers then started together
    /// and were left to race for the shared staging name on their own. Measured against the
    /// pre-fix name, Release, whole assembly: 3 red out of 5 — and the same test in isolation,
    /// held open through this client, never reaches the endpoint's <c>File.Create</c> in 30 s.
    /// A control that answers "no defect" two runs in five is not a control.</para>
    ///
    /// <para>So the second request is no longer a peer of the first, it is a probe fired INTO it.
    /// The first push goes through a client that really streams (see <see cref="_streaming"/>)
    /// and stops in the middle of its body: its handler has returned from <c>File.Create</c> and
    /// can neither leave <c>CopyToAsync</c> nor dispose the handle until the rest of the body
    /// arrives, which this test decides. The staging file appearing in the segments directory is
    /// that state, observed rather than timed, and only then is the second push sent — complete,
    /// and awaited to a status it cannot reach without having been through <c>File.Create</c>
    /// itself. The two therefore overlap on every run and every machine: 5 red out of 5 on the
    /// pre-fix name, in 565 ms, naming the 500. Nothing can deadlock — the held request is
    /// released once the second has answered, by a guard that fires even when an assertion above
    /// it does not.</para>
    /// </summary>
    [Fact]
    public async Task Two_pushes_of_one_segment_in_flight_together_are_both_accepted()
    {
        var storage = _factory.Services.GetRequiredService<StorageEngine>();
        var (bytes, id) = ForeignSegment(new NodeId(31), events: 9);

        Directory.CreateDirectory(_factory.SegDir);
        Assert.Empty(Directory.GetFiles(_factory.SegDir, "*.seg.tmp"));   // setup: nothing staged yet

        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var _ = new ReleasedOnExit(release);   // a red assertion below must not strand a request

        var held = PushHeldOpenAsync(31, id, bytes, release.Task);
        await WaitUntilABodyIsStagedAsync();

        // Fired into the window the held request is holding open.
        var second = await PushAsync(31, id, bytes);

        release.SetResult();
        var first = await held;

        Assert.All([first, second], r => Assert.Equal(HttpStatusCode.NoContent, r.StatusCode));

        // One entry, describing the segment that was actually sent...
        var only = Assert.Single(storage.ListSegments(), s => s.NodeId.Value == 31 && s.Id.Value == id);
        Assert.Equal(9u, only.EventCount);
        Assert.Equal(Path.Combine(_factory.SegDir, $"31-{id}.seg"), only.FilePath);

        // ...and the bytes under it are that segment, not a body one request truncated while the
        // other was measuring it.
        Assert.Equal(bytes, await File.ReadAllBytesAsync(only.FilePath));
        Assert.Empty(Directory.GetFiles(_factory.SegDir, "*.seg.tmp"));
    }

    // ── Holding one push open across another ──────────────────────────────────

    /// <summary>
    /// Waits until some request has a staging file open in the segments directory. The file
    /// appearing IS the endpoint's <c>File.Create</c> having returned, and the request that
    /// created it cannot close the handle until its body finishes arriving — so this is an
    /// observation that the overlap is in place, not an estimate of how long it takes to arrive.
    /// The deadline is a backstop that fails the test rather than a window it rests on: no
    /// passing run waits for it, and a harness that stages nothing says so instead of going green
    /// over a test that measured nothing.
    /// </summary>
    private async Task WaitUntilABodyIsStagedAsync()
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (Directory.GetFiles(_factory.SegDir, "*.seg.tmp").Length == 0)
        {
            Assert.True(DateTime.UtcNow < deadline,
                "no push staged a body within 30 s, so the request meant to be held open never " +
                "reached File.Create and nothing below overlaps anything");
            await Task.Delay(10);
        }
    }

    private Task<HttpResponseMessage> PushHeldOpenAsync(uint node, ulong id, byte[] body, Task release)
    {
        var content = new StreamContent(new HeadThenWaitStream(body, release));
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        return _streaming.PostAsync($"/api/replication/segments/{node}/{id}", content);
    }

    /// <summary>
    /// A body whose second half is not readable until <paramref name="release"/> completes. The
    /// endpoint has copied the first half into its staging file by then and cannot leave
    /// <c>CopyToAsync</c> — nor dispose the handle — until the rest arrives, which is the state
    /// the test needs to hold across another push.
    /// </summary>
    private sealed class HeadThenWaitStream(byte[] body, Task release) : Stream
    {
        private readonly int _head = Math.Max(1, body.Length / 8);
        private int _pos;

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            if (_pos == _head) await release;
            if (_pos >= body.Length) return 0;
            int limit = _pos < _head ? _head : body.Length;
            int n = Math.Min(buffer.Length, limit - _pos);
            body.AsSpan(_pos, n).CopyTo(buffer.Span);
            _pos += n;
            return n;
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();

        public override bool CanRead  => true;
        public override bool CanSeek  => false;
        public override bool CanWrite => false;
        public override long Length   => throw new NotSupportedException();
        public override long Position { get => _pos; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long o, SeekOrigin s) => throw new NotSupportedException();
        public override void SetLength(long value)      => throw new NotSupportedException();
        public override void Write(byte[] b, int o, int c) => throw new NotSupportedException();
    }

    /// <summary>
    /// Completes <paramref name="release"/> on the way out of the test, however the test leaves.
    /// A failed assertion between the two pushes would otherwise abandon a request holding a body
    /// half-written, and the fixture's host cannot shut one of those down.
    /// </summary>
    private sealed class ReleasedOnExit(TaskCompletionSource release) : IDisposable
    {
        public void Dispose() => release.TrySetResult();
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
    /// The 409 body names WHICH of the receiver's two conflict causes fired — the sender's log
    /// quotes that body, so before the causes were split it confidently reported the wrong one
    /// in one case of two. This pins the different-segment body.
    /// </summary>
    [Fact]
    public async Task A_409_for_a_different_segment_says_so_in_the_body()
    {
        // Node 71 is used by no other test in this class: the factory is shared, so a node id
        // another test also pushes under would make this one's first push meet that test's
        // segment and 409 before the scenario even starts.
        var (first, id) = ForeignSegment(new NodeId(71), events: 6);
        Assert.Equal(HttpStatusCode.NoContent, (await PushAsync(71, id, first)).StatusCode);

        var (second, _) = ForeignSegment(new NodeId(71), events: 11);
        var response = await PushAsync(71, id, second);

        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        Assert.Contains("already held by a different file", await response.Content.ReadAsStringAsync());
    }

    /// <summary>
    /// And the allocated-locally body: an id this node's own allocator has handed out cannot be
    /// taken by a peer wearing this node's id, and the body says that instead of the
    /// different-file story.
    /// </summary>
    [Fact]
    public async Task A_409_for_a_locally_allocated_id_says_so_in_the_body()
    {
        // The allocator branch needs an id that is HANDED OUT but not PUBLISHED — an id with a
        // catalog entry takes the different-segment branch instead. A flush reserves a block of
        // six ids, one per level, and publishes only the levels it held: WriteAndFlush writes
        // Information only, so the Debug slot right below it (local.Id - 1) is allocated to the
        // local writer and carries no file and no entry. A peer wearing this node's id and
        // pushing that id hits exactly the claim refusal.
        var storage = _factory.Services.GetRequiredService<StorageEngine>();
        var local   = WriteAndFlush(storage, events: 8);
        ulong reservedUnpublished = local.Id.Value - 1;

        byte[] body = PeerSegmentBytes(new NodeId(0), reservedUnpublished, events: 4);
        var response = await PushAsync(0, reservedUnpublished, body);

        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        Assert.Contains("own allocator has already handed out", await response.Content.ReadAsStringAsync());
    }

    /// <summary>
    /// The third 409 cause is NOT a NodeId diagnosis, and its body says so: the path is
    /// occupied by a file the receiver could not read, it kept the file and refused to guess.
    /// The other two bodies both claim a duplicated NodeId — sending either here would have
    /// the sender log a deployment fault nobody established, marked permanent by the contract
    /// when the cause is local and clears with the file.
    /// </summary>
    [Fact]
    public async Task A_409_for_an_unreadable_incumbent_does_not_claim_a_duplicated_node()
    {
        Directory.CreateDirectory(_factory.SegDir);
        string finalPath = Path.Combine(_factory.SegDir, "93-5.seg");
        await File.WriteAllBytesAsync(finalPath, [0xBA, 0xD0, 0xBA, 0xD0, 0xBA, 0xD0, 0xBA, 0xD0]);

        byte[] body = PeerSegmentBytes(new NodeId(93), 5, events: 4);
        var response = await PushAsync(93, 5, body);

        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        string text = await response.Content.ReadAsStringAsync();
        Assert.Contains("could not read", text);
        Assert.DoesNotContain("two nodes", text);
    }

    /// <summary>
    /// A segment body with a CHOSEN header identity — ForeignSegment cannot pick the id, its
    /// engine's allocator does. The header is what the receiver keys on; the route only names
    /// the file.
    /// </summary>
    private static byte[] PeerSegmentBytes(NodeId node, ulong segId, int events)
    {
        string tmp = Path.Combine(Path.GetTempPath(), "ameto-peerbytes-" + Guid.NewGuid().ToString("N")[..8] + ".seg");
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(16, 1L << 20);
        long baseTicks = DateTime.UtcNow.Ticks;
        for (int i = 0; i < events; i++)
            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = new Ameto.Core.EventId(node.Value, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerMillisecond,
                Level                    = Ameto.Core.LogLevel.Information,
                MessageTemplatePoolIndex = pool.Intern("chosen {n}"),
            }, ReadOnlySpan<byte>.Empty, "chosen {n}"));
        hot.Freeze();
        try
        {
            using (var writer = new SegmentWriter(tmp))
            {
                writer.WriteEvents(hot, pool);
                writer.Finalise(node, new SegmentId(segId));
            }
            return File.ReadAllBytes(tmp);
        }
        finally { try { File.Delete(tmp); } catch { } }
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
            // throw — without it the delete below cannot succeed at all, the engine's
            // write-ahead log still being mapped. Necessary is not sufficient, though: a run
            // has left this directory behind even with the dispose in place, so the failure is
            // now SAID rather than swallowed, and a leftover has a line to be found by.
            engine.DisposeAsync().AsTask().GetAwaiter().GetResult();
            try { Directory.Delete(dir, true); }
            catch (Exception ex) { Console.WriteLine($"temp dir left behind: {dir} — {ex.Message}"); }
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
