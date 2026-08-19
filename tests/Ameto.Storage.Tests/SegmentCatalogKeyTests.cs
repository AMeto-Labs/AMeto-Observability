using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// Segment ids are monotonic PER NODE and every node's counter starts at 1, so as soon as a peer
/// replicates anything the two series overlap — collision is the normal state of a cluster, not an
/// edge case. The cold catalog was keyed by the id alone, and both writers into it assign
/// unconditionally, so the second registration EVICTED the first.
///
/// <para>The file did not go anywhere: the names cannot collide (a replica is
/// <c>{node}-{id}.seg</c>, a locally written segment <c>{node}-{id}-{minTs}-{maxTs}.seg</c>), so
/// both stayed on disk while only one was in the catalog. Queries, retention and the merge planner
/// all read the catalog rather than the directory, so the evicted file left all three at once: it
/// was never served, never expired and never compacted, and held disk for the life of the install
/// with nothing logged. <c>LoadSegmentCatalog</c> re-ran the collision on every restart in
/// directory-enumeration order, so which of the two won could change from boot to boot.</para>
/// </summary>
public sealed class SegmentCatalogKeyTests : IAsyncLifetime
{
    /// <summary>A real peer id. The engine under test is NodeId 0, the default.</summary>
    private static readonly NodeId Peer = new(7);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-segkey-" + Guid.NewGuid().ToString("N"));
    private StorageEngine _engine = null!;
    private CapturingLogger _log  = new();

    private string SegDir => Path.Combine(_dir, "segments");

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _engine = NewEngine();
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        try { await _engine.DisposeAsync(); } catch { }
        try { Directory.Delete(_dir, true); } catch { }
    }

    private StorageEngine NewEngine() => new(
        Options.Create(new ServerOptions { DataDirectory = _dir }),
        new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
        _log);

    private static byte[] Props(int i)
    {
        var buf = new ArrayBufferWriter<byte>(48);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write((long)i);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private void Write(int count, long baseTicks, LogLevel level = LogLevel.Information)
    {
        for (int i = 0; i < count; i++)
            Assert.True(_engine.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerMillisecond,
                Level                    = level,
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("local {n}"),
            }, Props(i)));
    }

    /// <summary>
    /// Writes a replicated segment file exactly as the replication endpoint leaves one: named
    /// <c>{node}-{id}.seg</c>, two name segments, with the peer's own node id in the header —
    /// which is what makes a (node, id) key able to tell it apart from a local segment at all.
    /// </summary>
    private string WritePeerSegment(NodeId node, ulong segId, long baseTicks, int events = 4,
                                    LogLevel level = LogLevel.Information, string? path = null)
    {
        const string Template = "peer {n}";
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(16, 1L << 20);
        for (int i = 0; i < events; i++)
            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(node.Value, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerMillisecond,
                Level                    = level,
                MessageTemplatePoolIndex = pool.Intern(Template),
            }, Props(i), Template));
        hot.Freeze();

        Directory.CreateDirectory(SegDir);
        path ??= Path.Combine(SegDir, $"{node.Value}-{segId}.seg");
        using (var writer = new SegmentWriter(path))
        {
            writer.WriteEvents(hot, pool);
            writer.Finalise(node, new SegmentId(segId));
        }
        return path;
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// The collision itself. A peer's segment carrying an id this node has already written must
    /// not displace the local one — both files exist, so both must be in the catalog, which is
    /// the only thing queries, retention and the merge planner ever consult.
    /// </summary>
    [Fact]
    public async Task A_peer_segment_sharing_a_local_id_does_not_evict_it()
    {
        long now = DateTime.UtcNow.Ticks;
        Write(20, now);
        await _engine.FlushHotTierAsync();

        var local = Assert.Single(_engine.ListSegments());
        ulong shared = local.Id.Value;

        _engine.ImportSegment(WritePeerSegment(Peer, shared, now));

        var all = _engine.ListSegments();
        Assert.Equal(2, all.Count);
        Assert.Contains(all, s => s.NodeId.Value == Peer.Value && s.Id.Value == shared);
        Assert.Contains(all, s => s.NodeId.Value == NodeId.Local.Value && s.Id.Value == shared
                                                           && s.FilePath == local.FilePath);

        // And through the reader path a query actually uses, not just the raw list.
        var window = _engine.GetSegments(
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(-5),
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(5));
        Assert.Equal(2, window.Count(s => s.Id.Value == shared));
    }

    /// <summary>
    /// What the eviction actually cost, measured rather than argued: retention enumerates the
    /// catalog, so a segment missing from it is never expired. Its bytes then sit in the segments
    /// directory for the life of the install, past every TTL, with nothing logged anywhere.
    /// </summary>
    [Fact]
    public async Task Retention_expires_both_segments_and_leaves_no_file_behind()
    {
        // Well past the 90-day Information TTL, so both are expired the moment retention runs.
        long old = DateTime.UtcNow.AddDays(-200).Ticks;
        Write(20, old);
        await _engine.FlushHotTierAsync();

        var local = Assert.Single(_engine.ListSegments());
        string peerPath = WritePeerSegment(Peer, local.Id.Value, old);
        _engine.ImportSegment(peerPath);

        var result = await _engine.EnforceRetentionAsync();

        Assert.Equal(2, result.DeletedSegments);
        Assert.Empty(_engine.ListSegments());
        Assert.False(File.Exists(local.FilePath), "the local segment's file survived retention");
        Assert.False(File.Exists(peerPath),       "the peer segment's file survived retention");
        Assert.Empty(Directory.GetFiles(SegDir, "*.seg"));
    }

    /// <summary>
    /// A consumer the write-up does not name, and the one that loses events rather than disk. A
    /// flush reserves a block of <c>LevelSegmentSlots</c> ids for the tier it froze and publishes
    /// them as COVERED so a query does not serve the freshly registered per-level segments and the
    /// still-frozen tier both. That set was matched against segment ids alone — so a replicated
    /// peer segment whose own counter happened to land inside the local block was skipped by every
    /// query for the whole duration of the flush. Perfectly readable file, events silently absent
    /// from results.
    ///
    /// <para>Driven at the publish seam, so the tier is provably still frozen while the count is
    /// taken: <c>_afterLevelPublished</c> fires after a level's file is moved into place and
    /// before the frozen tier is dropped from the list the covered set is built from.</para>
    /// </summary>
    [Fact]
    public async Task A_peer_segment_inside_the_live_flush_block_is_still_served()
    {
        long now = DateTime.UtcNow.Ticks;

        // Inside the block the NEXT flush will reserve — the collision that hides a peer's file
        // behind this node's flush.
        ulong inBlock = _engine.LiveWalSegmentId + 2;
        string peerPath = WritePeerSegment(Peer, inBlock, now, events: 4);
        _engine.ImportSegment(peerPath);

        Write(20, now);

        var reached  = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release  = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        long counted = -1;

        _engine._afterLevelPublished = _ =>
        {
            if (!reached.TrySetResult()) return;
            release.Task.GetAwaiter().GetResult();
        };

        var flushing = Task.Run(() => _engine.FlushHotTierAsync());
        await reached.Task;

        // The tier is frozen, its id block is covered, and the peer's segment shares one of those
        // ids. Its four events must still be counted.
        var counts = await _engine.AggregateLogVolumeAsync(
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(-5),
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(5),
            minBucket: 0, bucketSeconds: 60, nBuckets: 60, serviceFilter: null);
        counted = counts.Total;

        release.SetResult();
        await flushing;

        // 20 local (in the frozen tier) + 4 peer (in the cold segment the covered set was hiding).
        Assert.Equal(24, counted);
    }

    /// <summary>
    /// The SAME covered-set defect at the other consumer, and the one that serves
    /// <c>/api/logs</c>. There are two independent copies of this check —
    /// <c>StorageEngine.AggregateLogVolumeAsync</c> filters the segments it aggregates, and
    /// <c>QueryExecutor.ExecuteAsync</c> filters the segments it merges — and the test above
    /// reaches only the first. Reverting the executor's copy to an id-only match failed nothing
    /// at all: the storage, query and integration suites all stayed green, including the test
    /// whose own documentation describes this defect. The search path was pinned by nothing, so
    /// a later refactor dropping the node component from it would have been invisible.
    ///
    /// <para>What it costs is not a count but the rows themselves: a peer replica whose id falls
    /// inside the block a local flush reserved is filtered out of every log search for the
    /// duration of that flush. A perfectly readable file, events silently missing from results,
    /// nothing logged.</para>
    /// </summary>
    [Fact]
    public async Task A_peer_segment_inside_the_live_flush_block_is_returned_by_a_log_search()
    {
        long now = DateTime.UtcNow.Ticks;

        ulong inBlock = _engine.LiveWalSegmentId + 2;
        string peerPath = WritePeerSegment(Peer, inBlock, now, events: 4);
        _engine.ImportSegment(peerPath);

        Write(20, now);

        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        _engine._afterLevelPublished = _ =>
        {
            if (!reached.TrySetResult()) return;
            release.Task.GetAwaiter().GetResult();
        };

        var flushing = Task.Run(() => _engine.FlushHotTierAsync());
        await reached.Task;

        // Through the executor, not the aggregator: the tier is frozen, its id block is covered,
        // and the peer's segment shares one of those ids.
        var query = new Ameto.Query.QueryExecutor(
            _engine, new Ameto.Indexing.SegmentIndexReaderFactory(),
            NullLogger<Ameto.Query.QueryExecutor>.Instance);

        var hits = new List<LogEvent>();
        await foreach (var ev in query.ExecuteAsync(new QueryRequest
        {
            FromUtc   = new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(-5),
            ToUtc     = new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(5),
            Count     = 500,
            Direction = QueryDirection.Forward,
        }))
            hits.Add(ev);

        release.SetResult();
        await flushing;

        // 20 local (in the frozen tier) + 4 peer (in the cold segment the covered set was hiding).
        Assert.Equal(24, hits.Count);
        Assert.Equal(4, hits.Count(e => e.MessageTemplate.StartsWith("peer ", StringComparison.Ordinal)));
    }

    /// <summary>
    /// THE ALREADY-DAMAGED INSTALL. The eviction was only ever in memory — both files were always
    /// on disk, because the names cannot collide — so a directory written by the pre-fix build is
    /// exactly this: two files sharing an id, one of them absent from the catalog. Recovery has to
    /// need nothing but a restart, since deploying the fix IS a restart, and a fix that only
    /// stopped NEW collisions would leave every existing install's hidden file hidden forever.
    ///
    /// <para><c>LoadSegmentCatalog</c> re-ran the collision on every boot, and in
    /// <c>Directory.EnumerateFiles</c> order, so the pre-fix build did not even hide the same one
    /// each time. Nothing persists the key — the catalog is rebuilt from the files themselves, and
    /// each file's node id comes out of its own header — so the recovery is automatic and there is
    /// no migration to run.</para>
    /// </summary>
    [Fact]
    public async Task An_existing_collided_directory_recovers_on_restart()
    {
        long now = DateTime.UtcNow.Ticks;
        Write(20, now);
        await _engine.FlushHotTierAsync();

        var local = Assert.Single(_engine.ListSegments());
        ulong shared = local.Id.Value;
        string peerPath = WritePeerSegment(Peer, shared, now, events: 4);
        _engine.ImportSegment(peerPath);

        await RestartAsync(expectSegments: 2);

        // Rebuilt from the directory, not from anything the previous process handed over.
        var all = _engine.ListSegments();
        Assert.Equal(2, all.Count);
        Assert.Contains(all, s => s.NodeId.Value == Peer.Value && s.Id.Value == shared
                                                              && s.FilePath == peerPath);
        Assert.Contains(all, s => s.NodeId.Value == NodeId.Local.Value && s.Id.Value == shared
                                                              && s.FilePath == local.FilePath);

        // Visible is not the claim — SERVED is. Both files' events come back through the reader
        // path, which is what the hidden file stopped doing.
        var counts = await _engine.AggregateLogVolumeAsync(
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(-5),
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(5),
            minBucket: 0, bucketSeconds: 60, nBuckets: 60, serviceFilter: null);
        Assert.Equal(24, counts.Total);
    }

    /// <summary>
    /// Retention judges each of the two on its own merits, which is the half of "both are in the
    /// catalog" that matters: the hidden file was not merely unlisted, it was IMMORTAL, sailing
    /// past every TTL because the only thing that expires segments enumerates the catalog.
    ///
    /// <para>Ten days back puts the peer's Debug replica past its 3-day TTL and leaves the local
    /// Information segment 80 days short of its 90 — so exactly one of a pair SHARING AN ID must
    /// go. That also pins the delete to the right path: <c>DeleteSegmentAsync</c> unlinks the file
    /// belonging to the entry it removes, and with one keyspace the entry under an id was
    /// whichever of the two registered last.</para>
    /// </summary>
    [Fact]
    public async Task Each_segment_expires_under_its_own_policy()
    {
        long old = DateTime.UtcNow.AddDays(-10).Ticks;
        Write(20, old, LogLevel.Information);
        await _engine.FlushHotTierAsync();

        var local = Assert.Single(_engine.ListSegments());
        string peerPath = WritePeerSegment(Peer, local.Id.Value, old, events: 4, level: LogLevel.Debug);
        _engine.ImportSegment(peerPath);
        Assert.Equal(2, _engine.ListSegments().Count);

        var result = await _engine.EnforceRetentionAsync();

        Assert.Equal(1, result.DeletedSegments);
        Assert.False(File.Exists(peerPath),
            "the peer's Debug replica is 7 days past its TTL and should have been deleted");
        Assert.True(File.Exists(local.FilePath),
            "retention deleted the local Information segment, which is 80 days short of its TTL");

        var kept = Assert.Single(_engine.ListSegments());
        Assert.Equal(NodeId.Local.Value, kept.NodeId.Value);
    }

    /// <summary>
    /// The direction that must NOT change. The replication endpoint moves a received file into
    /// place with <c>overwrite: true</c> and re-imports it, so a re-push of a segment this node
    /// already holds is normal traffic and has to stay a no-op. Registering it twice would mean
    /// serving its events twice.
    ///
    /// <para>This is the assertion that rules out "renumber the segment on import" as a fix: a
    /// fresh id per import turns each re-push into a permanent duplicate.</para>
    /// </summary>
    [Fact]
    public async Task Re_importing_the_same_file_registers_it_once()
    {
        long now = DateTime.UtcNow.Ticks;
        string peerPath = WritePeerSegment(Peer, 5, now, events: 4);

        // Both REGISTERED, not "second one refused": the refusal below is for a different file
        // arriving at an occupied key, and a re-push is the same file. The endpoint turns
        // anything other than Registered into an error status and deletes the file it wrote, so
        // calling a re-push a conflict would fail every legitimate push after the first and
        // unlink the replica it had just accepted.
        Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(peerPath));
        Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(peerPath));

        var only = Assert.Single(_engine.ListSegments());
        Assert.Equal(Peer.Value, only.NodeId.Value);
        Assert.Equal(peerPath, only.FilePath);

        var counts = await _engine.AggregateLogVolumeAsync(
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(-5),
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(5),
            minBucket: 0, bucketSeconds: 60, nBuckets: 60, serviceFilter: null);
        Assert.Equal(4, counts.Total);
    }

    /// <summary>
    /// The one ambiguity the key cannot resolve — two nodes CONFIGURED with the same NodeId — and
    /// therefore the last place in the engine where registering a segment could still remove one.
    /// The import used to log a warning saying, correctly, that "one of the two files will not be
    /// served or expired", and then perform exactly that eviction one line later.
    ///
    /// <para>What it cost is what the key change cost everywhere else: the evicted file stays on
    /// disk — the names cannot collide — while leaving queries, retention and the merge planner
    /// at once, since all three read the catalog and not the directory. Never served, never
    /// expired, never compacted, holding its bytes for the life of the install, and with the only
    /// evidence a warning in a log nobody reads precisely because everything still looks fine.
    /// The file being served has done nothing wrong, so it is the one that is kept.</para>
    /// </summary>
    [Fact]
    public async Task A_second_file_under_one_key_is_refused_and_the_one_being_served_survives()
    {
        long now = DateTime.UtcNow.Ticks;
        Write(20, now);
        await _engine.FlushHotTierAsync();

        var local = Assert.Single(_engine.ListSegments());

        // Carries THIS engine's node id as well as its segment id — a peer misconfigured with
        // our identity, which is the only way a replica can land on a key we already hold. It
        // still cannot land on our PATH (a replica is {node}-{id}.seg, a local segment
        // {node}-{id}-{min}-{max}.seg), so both files exist and only the catalog can lose one.
        string intruder = WritePeerSegment(NodeId.Local, local.Id.Value, now, events: 4);
        Assert.NotEqual(local.FilePath, intruder);

        Assert.Equal(SegmentImportOutcome.Conflict, _engine.ImportSegment(intruder));

        var kept = Assert.Single(_engine.ListSegments());
        Assert.Equal(local.FilePath, kept.FilePath);
        Assert.Equal(20u, kept.EventCount);

        // Listed is not the claim — SERVED is, and the four intruding events must not appear
        // either: refusing the import means refusing its contents, not hiding its file.
        var counts = await _engine.AggregateLogVolumeAsync(
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(-5),
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(5),
            minBucket: 0, bucketSeconds: 60, nBuckets: 60, serviceFilter: null);
        Assert.Equal(20, counts.Total);
    }

    /// <summary>
    /// <summary>
    /// The case path equality cannot see, and the reason the import owns the rename. The endpoint
    /// names a received file from the ROUTE, so two peers misconfigured with one NodeId — each
    /// allocating segment 3 from its own counter — push two different segments to one path. There
    /// is no local file involved and nothing to tell them apart by name.
    ///
    /// <para>The refusal is asserted where it has to hold: on disk. The move used to happen in
    /// the endpoint before the import was consulted, so by the time anything compared anything
    /// the first peer's bytes were already overwritten, and the comparison — path against itself
    /// — then called it a re-push and refreshed the entry. Nothing was logged and both senders
    /// recorded success.</para>
    /// </summary>
    [Fact]
    public void A_staged_segment_that_differs_from_the_one_at_its_path_is_refused_without_moving()
    {
        long now = DateTime.UtcNow.Ticks;

        string finalPath = WritePeerSegment(Peer, 3, now, events: 4);
        Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(finalPath, finalPath));
        byte[] served = File.ReadAllBytes(finalPath);

        // A DIFFERENT peer wearing the same NodeId: same key, its own events, staged where the
        // endpoint stages a body it has just received.
        string staged = Path.Combine(SegDir, $"{Peer.Value}-3.seg.tmp");
        WritePeerSegment(Peer, 3, now + TimeSpan.TicksPerHour, events: 9, path: staged);

        Assert.Equal(SegmentImportOutcome.Conflict, _engine.ImportSegment(staged, finalPath));

        // Untouched: the entry, the events it serves, and the bytes under it.
        var only = Assert.Single(_engine.ListSegments());
        Assert.Equal(4u, only.EventCount);
        Assert.Equal(finalPath, only.FilePath);
        Assert.Equal(served, File.ReadAllBytes(finalPath));

        // And the refused body is still the caller's — the engine holds no reference to it, so
        // unlinking it is the caller's job and cannot be done for a file that was consumed.
        Assert.True(File.Exists(staged), "a refused import moved the body it refused");
    }

    /// <summary>
    /// The direction that must survive the check above: the SAME segment, staged and pushed
    /// again. Its bytes may differ — a re-compression on the sender rewrites the file without
    /// changing what is in it — so the entry is refreshed and the body takes its place, which is
    /// what makes a re-push normal traffic rather than a conflict.
    /// </summary>
    [Fact]
    public void A_staged_re_push_of_the_same_segment_replaces_the_file_and_registers_once()
    {
        long now = DateTime.UtcNow.Ticks;

        string finalPath = WritePeerSegment(Peer, 4, now, events: 6);
        Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(finalPath, finalPath));

        string staged = Path.Combine(SegDir, $"{Peer.Value}-4.seg.tmp");
        WritePeerSegment(Peer, 4, now, events: 6, path: staged);
        byte[] pushed = File.ReadAllBytes(staged);

        Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(staged, finalPath));

        var only = Assert.Single(_engine.ListSegments());
        Assert.Equal(6u, only.EventCount);
        Assert.Equal(finalPath, only.FilePath);
        Assert.Equal(pushed, File.ReadAllBytes(finalPath));
        Assert.False(File.Exists(staged), "an accepted import left its body at the staged name");
    }

    /// A file that is not a segment gets its own outcome rather than an exception the caller
    /// cannot tell from success. The endpoint that wrote it needs to know: on anything but
    /// <c>Registered</c> the engine holds no reference to that file, so leaving it in the
    /// segments directory leaves the next boot's catalog scan to decide what it is.
    /// </summary>
    [Fact]
    public void A_file_that_is_not_a_segment_is_reported_as_unreadable()
    {
        Directory.CreateDirectory(SegDir);
        string junk = Path.Combine(SegDir, "7-99.seg");
        File.WriteAllBytes(junk, [0xDE, 0xAD, 0xBE, 0xEF]);

        Assert.Equal(SegmentImportOutcome.Unreadable, _engine.ImportSegment(junk));
        Assert.Empty(_engine.ListSegments());
    }

    /// <summary>
    /// The OTHER writer into the catalog. The import refuses a second segment under an occupied
    /// key, but the boot scan rebuilt the catalog by assigning unconditionally, in
    /// <c>Directory.EnumerateFiles</c> order — so a directory that ALREADY holds two files under
    /// one key lost one of them at every start, whichever the order happened to put last, with
    /// nothing logged. That is the whole of bug #43 reached without an import running at all, and
    /// the directory is not exotic: a build that registered the intruder left exactly this, as
    /// does a failed unlink of a refused body, a restore from backup, or an operator copying a
    /// file in from another node.
    ///
    /// <para>Two files, no import — the state, not the route to it. What the scan must do with it
    /// is fixed rather than incidental: keep one, keep the SAME one across restarts, and say so.
    /// The one it keeps is the locally written segment, which this node cannot get back; the
    /// replica's owner still holds it and can push it again.</para>
    /// </summary>
    [Fact]
    public async Task A_directory_holding_two_files_under_one_key_keeps_the_local_one_and_says_so()
    {
        long now = DateTime.UtcNow.Ticks;
        Write(8, now);
        await _engine.FlushHotTierAsync();
        var local = Assert.Single(_engine.ListSegments());

        // A peer misconfigured with THIS node's id, its file already in the directory and never
        // imported: {0}-{id}.seg beside the local {0}-{id}-{min}-{max}.seg.
        string intruder = WritePeerSegment(NodeId.Local, local.Id.Value, now, events: 11);
        Assert.NotEqual(local.FilePath, intruder);

        for (int boot = 1; boot <= 2; boot++)
        {
            await RestartAndAwaitCatalogAsync();

            var kept = Assert.Single(_engine.ListSegments());
            Assert.Equal(local.FilePath, kept.FilePath);
            Assert.Equal(8u, kept.EventCount);

            // Both files are still there: refusing to serve a file is not licence to delete it,
            // and which of the two is the wrong one is not this node's to decide.
            Assert.True(File.Exists(local.FilePath));
            Assert.True(File.Exists(intruder));

            // And the one that is not being served is named, at every start. Silence is what made
            // this survive for the life of an install: the file is on disk, the counts look
            // plausible, and nothing anywhere points at what is missing.
            Assert.Contains(_log.Entries, e =>
                e.Message.Contains(intruder, StringComparison.Ordinal) &&
                e.Message.Contains(local.FilePath, StringComparison.Ordinal));
        }

        // Served, not merely listed: the eight local events and none of the eleven.
        var counts = await _engine.AggregateLogVolumeAsync(
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(-5),
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(5),
            minBucket: 0, bucketSeconds: 60, nBuckets: 60, serviceFilter: null);
        Assert.Equal(8, counts.Total);
    }

    /// <summary>
    /// THE WINDOW BETWEEN THE PROBE AND THE STORE. The import holds <c>_importLock</c>, but that
    /// lock is taken in <c>ImportSegment</c> and nowhere else — flush publication, merge
    /// publication, WAL recovery and the boot catalog scan all write the catalog without it. So a
    /// probe that reports the key free reports it free AS OF THE PROBE, and a store performed on
    /// the strength of it overwrites whatever arrived in between.
    ///
    /// <para>Not a theoretical window. The scan is a BACKGROUND task — ingest and the HTTP
    /// endpoints come up while it is still walking the directory — so a replication POST is live
    /// during a boot that has thousands of segments to open. The import probes before the scan has
    /// reached the local <c>{node}-{id}-{min}-{max}.seg</c>, the scan registers it, and the import
    /// stores over it. The local segment leaves queries, retention and the merge planner at once
    /// while its file stays on disk — bug #43 exactly — and SILENTLY: the import's conflict branch
    /// never fired, because at the moment it looked there was nothing to conflict with, and the
    /// scan's error branch had already been passed for the same reason.</para>
    ///
    /// <para>Driven through the seam because no arrangement of public calls can discriminate: run
    /// the scan first and the import is refused by the probe; run it after and the scan reports
    /// the collision itself. Only a scan landing INSIDE the import's window is silent, and the
    /// only difference a test can see between a store and a compare-and-swap is which of the two
    /// files the catalog is left naming.</para>
    /// </summary>
    [Fact]
    public async Task A_registration_landing_inside_an_imports_window_is_not_overwritten()
    {
        long now = DateTime.UtcNow.Ticks;
        const ulong Shared = 12;

        // In the segments directory and in nobody's catalog: what the boot scan starts from, and
        // what a locally written segment looks like to it.
        string localPath = Path.Combine(SegDir,
            $"{NodeId.Local.Value}-{Shared}-{now}-{now + 7 * TimeSpan.TicksPerMillisecond}.seg");
        WritePeerSegment(NodeId.Local, Shared, now, events: 8, path: localPath);

        // A peer misconfigured with this node's id, pushing ITS segment 12 — the one case the
        // (node, id) key cannot separate. The endpoint names the body from the route, so it
        // arrives as {node}-{id}.seg and cannot land on the local file's name; only the catalog
        // can lose one of the two. Staged outside the segments directory so that the scan's
        // *.seg.tmp sweep is not part of what is being measured (in production the body is staged
        // after that sweep has run, and the scan below is the part that is still going).
        string finalPath = Path.Combine(SegDir, $"{NodeId.Local.Value}-{Shared}.seg");
        string staged    = Path.Combine(_dir, "pushed.body");
        WritePeerSegment(NodeId.Local, Shared, now + TimeSpan.TicksPerHour, events: 4, path: staged);

        var probed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var landed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        _engine._beforeImportPublish = () =>
        {
            if (!probed.TrySetResult()) return;   // only the first pass through the window parks
            landed.Task.GetAwaiter().GetResult();
        };

        var importing = Task.Run(() => _engine.ImportSegment(staged, finalPath));
        await probed.Task;                        // the import has read the catalog: the key is free

        // The scan reaches the local file now, which is the whole point: it takes no lock this
        // import holds, so nothing stops it landing here.
        _engine.LoadSegmentCatalog();
        Assert.Contains(_engine.ListSegments(), s => s.FilePath == localPath);

        landed.SetResult();
        var outcome = await importing;

        // The write decided, so the import loses the exchange, re-reads, and finds what it is
        // actually up against.
        Assert.Equal(SegmentImportOutcome.Conflict, outcome);

        var kept = Assert.Single(_engine.ListSegments());
        Assert.Equal(localPath, kept.FilePath);
        Assert.Equal(8u, kept.EventCount);

        // The rename is the irreversible half and it is now gated on the exchange, not on the
        // probe: a refused import leaves the body where the caller put it and puts nothing in the
        // segments directory for the next boot scan to pick between.
        Assert.True(File.Exists(staged), "a refused import consumed the body it refused");
        Assert.False(File.Exists(finalPath), "a refused import renamed its body into the segments directory");

        // And it is no longer silent. Which file stopped being served is the only thing this node
        // can do about two peers wearing one id, so it has to be in the log.
        Assert.Contains(_log.Entries, e =>
            e.Message.Contains(staged,    StringComparison.Ordinal) &&
            e.Message.Contains(localPath, StringComparison.Ordinal));

        // Served, not merely listed: the eight local events, and none of the peer's four.
        var counts = await _engine.AggregateLogVolumeAsync(
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(-5),
            new DateTimeOffset(now, TimeSpan.Zero).AddHours(2),
            minBucket: 0, bucketSeconds: 60, nBuckets: 240, serviceFilter: null);
        Assert.Equal(8, counts.Total);
    }

    /// <summary>
    /// Restarts on the same directory and waits for the catalog scan to FINISH, rather than for a
    /// count to be reached: what a collision costs is an entry that never appears, so a wait that
    /// stops at the first entry would be waiting for the wrong thing. The scan's closing log line
    /// is the only signal it has published.
    /// </summary>
    private async Task RestartAndAwaitCatalogAsync()
    {
        await _engine.DisposeAsync();
        _log    = new CapturingLogger();
        _engine = NewEngine();

        for (int i = 0; i < 400; i++)
        {
            foreach (var (message, _) in _log.Entries)
                if (message.StartsWith("Loaded ", StringComparison.Ordinal)) return;
            await Task.Delay(25);
        }
        Assert.Fail("the catalog scan did not finish");
    }

    /// <summary>
    /// What the engine logged, so "one of the two files is not being served" is OBSERVABLE. The
    /// scan cannot resolve a duplicate NodeId — nothing on this node can — so saying which file
    /// it stopped keying is the entire remedy available to it.
    /// </summary>
    private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger<StorageEngine>
    {
        private readonly List<(string Message, Exception? Error)> _entries = [];

        public IReadOnlyList<(string Message, Exception? Error)> Entries
        {
            get { lock (_entries) return _entries.ToList(); }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel level) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel level,
                                Microsoft.Extensions.Logging.EventId eventId, TState state,
                                Exception? error, Func<TState, Exception?, string> formatter)
        {
            lock (_entries) _entries.Add((formatter(state, error), error));
        }
    }

    /// <summary>Restarts on the same directory and waits for the background catalog scan.</summary>
    private async Task RestartAsync(int expectSegments)
    {
        await _engine.DisposeAsync();
        _engine = NewEngine();
        for (int i = 0; i < 400 && _engine.ListSegments().Count < expectSegments; i++)
            await Task.Delay(25);
    }
}
