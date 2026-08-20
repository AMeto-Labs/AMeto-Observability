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
                                    LogLevel level = LogLevel.Information, string? path = null,
                                    string template = "peer {n}")
    {
        string Template = template;
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
        using var _  = Seam.ReleasedOnExit(release);   // a throw below must not strand the flush thread
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
        using var _ = Seam.ReleasedOnExit(release);    // a throw below must not strand the flush thread

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

        Assert.Equal(SegmentImportOutcome.ConflictDifferentSegment, _engine.ImportSegment(intruder));

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

        Assert.Equal(SegmentImportOutcome.ConflictDifferentSegment, _engine.ImportSegment(staged, finalPath));

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
    /// changing what is in it — and since #49 the held copy STAYS and the body is dropped: the
    /// header carries no digest, so "same" is five fields, and replacing served bytes on that
    /// comparison was the loss #49 closed. This test's previous shape asserted the replacement
    /// with a byte-identical body, which could not tell the two behaviours apart; the staged
    /// body here differs, so a return to replace-on-match fails it.
    /// </summary>
    [Fact]
    public void A_staged_re_push_of_the_same_segment_registers_once_and_keeps_the_incumbent()
    {
        long now = DateTime.UtcNow.Ticks;

        string finalPath = WritePeerSegment(Peer, 4, now, events: 6);
        Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(finalPath, finalPath));
        byte[] held = File.ReadAllBytes(finalPath);

        string staged = Path.Combine(SegDir, $"{Peer.Value}-4.seg.tmp");
        WritePeerSegment(Peer, 4, now, events: 6, path: staged, template: "peer RECOMPRESSED {n}");
        Assert.NotEqual(held, File.ReadAllBytes(staged));

        Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(staged, finalPath));

        var only = Assert.Single(_engine.ListSegments());
        Assert.Equal(6u, only.EventCount);
        Assert.Equal(finalPath, only.FilePath);
        Assert.Equal(held, File.ReadAllBytes(finalPath));
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
        using var _ = Seam.ReleasedOnExit(landed);     // a red assertion below must not strand the import

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
        Assert.Equal(SegmentImportOutcome.ConflictDifferentSegment, outcome);

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
    /// The route the compare-and-swap above does not cover, and the one that needs no race to
    /// reach. The import decides by exchange, but the three writers that publish segments THIS
    /// node produced — flush publication, merge publication, WAL replay — assign unconditionally,
    /// and they must: their events are in no other file, so refusing a key is not something they
    /// can do. An import that wins its exchange is therefore only registered until the next one
    /// of them reaches the same key.
    ///
    /// <para>Which is not an instant. <c>OpenWal</c> reserves a block of six ids the moment a WAL
    /// opens and the flush publishes into exactly that block, so the block is outstanding for the
    /// whole life of the current hot tier; a merge's id is outstanding while it streams towards a
    /// 512 MB target. Nothing here is timed or parked: the import lands in a live reservation and
    /// then an ORDINARY flush follows. Before the fix that sequence returned <c>Registered</c>,
    /// answered the peer 204, and left the peer's file on disk in no catalog — never served,
    /// never expired, never merged — with nothing logged by either party.</para>
    ///
    /// <para>The remedy is the allocator, used as a verdict rather than as a note: an id it has
    /// already handed out cannot be claimed by a segment arriving with this node's own id. So the
    /// answer is 409 and the body stays the caller's, which is the one thing that IS true — this
    /// node cannot serve both files under one key, and only the sender can tell a refusal from a
    /// healthy push.</para>
    /// </summary>
    [Fact]
    public async Task A_peer_wearing_our_id_cannot_take_an_id_the_allocator_has_handed_out()
    {
        long now = DateTime.UtcNow.Ticks;

        // The id the live WAL's block will flush its Information level into — reserved, spent,
        // and in no catalog, which is exactly what makes it look free to a probe.
        ulong reserved = _engine.LiveWalSegmentId + (ulong)LogLevel.Information;

        string staged    = Path.Combine(_dir, "pushed.body");
        string finalPath = Path.Combine(SegDir, $"{NodeId.Local.Value}-{reserved}.seg");
        WritePeerSegment(NodeId.Local, reserved, now, events: 4, path: staged);

        Assert.Equal(SegmentImportOutcome.ConflictAllocatedLocally, _engine.ImportSegment(staged, finalPath));

        // Refused means refused on disk too: the body is still the caller's to unlink, and the
        // segments directory gains nothing for the next boot scan to pick between.
        Assert.True(File.Exists(staged), "a refused import consumed the body it refused");
        Assert.False(File.Exists(finalPath), "a refused import renamed its body into the segments directory");
        Assert.Contains(_log.Entries, e =>
            e.Message.Contains(staged, StringComparison.Ordinal) &&
            e.Message.Contains($"{NodeId.Local.Value}-{reserved}", StringComparison.Ordinal));

        // And now the flush the reservation belonged to, with no contention of any kind. This is
        // the half that used to run second and win: the local segment lands on the same key.
        Write(20, now + TimeSpan.TicksPerHour);
        await _engine.FlushHotTierAsync();

        var kept = Assert.Single(_engine.ListSegments());
        Assert.Equal(reserved, kept.Id.Value);
        Assert.Equal(20u, kept.EventCount);

        // Served, which is the claim: the twenty local events and none of the peer's four. A
        // build that accepted the import instead answered 204 for four events this assertion
        // cannot find anywhere afterwards.
        var counts = await _engine.AggregateLogVolumeAsync(
            new DateTimeOffset(now, TimeSpan.Zero).AddMinutes(-5),
            new DateTimeOffset(now, TimeSpan.Zero).AddHours(4),
            minBucket: 0, bucketSeconds: 60, nBuckets: 600, serviceFilter: null);
        Assert.Equal(20, counts.Total);
    }

    /// <summary>
    /// The same displacement from the other end, through the one arrangement the allocator cannot
    /// rule out. The catalog scan runs as a BACKGROUND task and WAL replay publishes while it is
    /// still walking the directory, so a peer file already sitting under
    /// <c>{localNode}-{id}.seg</c> — left by an older build, a restore, an endpoint whose unlink
    /// of a refused body failed — can be registered by the scan and then published over by a local
    /// writer that never asked the allocator for anything.
    ///
    /// <para>The local writer WINS, and that is not a compromise: it is holding the only copy of
    /// its own events, so a version of this that kept the peer's entry would trade a silent loss
    /// of replicated data for a silent loss of local data. What changes is that it no longer
    /// happens in silence. Which file stopped being served is the entire remedy this node has for
    /// two peers wearing one NodeId, and the file is left on disk because its owner still holds it
    /// and can push it again once the ids are fixed.</para>
    ///
    /// <para>Both directions are pinned: an unconditional store fails the log assertion, and a
    /// publication that stood down for the incumbent fails the entry and the count.</para>
    /// </summary>
    [Fact]
    public async Task A_local_flush_that_displaces_a_peers_entry_says_which_file_it_stopped_serving()
    {
        long now = DateTime.UtcNow.Ticks;
        ulong reserved = _engine.LiveWalSegmentId + (ulong)LogLevel.Information;

        // In the segments directory before anything scans it, under this node's own id.
        string peerPath = Path.Combine(SegDir, $"{NodeId.Local.Value}-{reserved}.seg");
        WritePeerSegment(NodeId.Local, reserved, now, events: 4, path: peerPath);

        _engine.LoadSegmentCatalog();
        Assert.Contains(_engine.ListSegments(), s => s.FilePath == peerPath);

        Write(20, now + TimeSpan.TicksPerHour);
        await _engine.FlushHotTierAsync();

        var kept = Assert.Single(_engine.ListSegments());
        Assert.NotEqual(peerPath, kept.FilePath);
        Assert.Equal(20u, kept.EventCount);

        Assert.Contains(_log.Entries, e =>
            e.Message.Contains(peerPath,      StringComparison.Ordinal) &&
            e.Message.Contains(kept.FilePath, StringComparison.Ordinal));

        // Displaced, not deleted. Which of two files is the wrong one is not this node's to
        // decide and the cost of being wrong is unrecoverable.
        Assert.True(File.Exists(peerPath), "the displaced file was deleted rather than reported");
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
    /// <summary>
    /// The restart window: a file sits at the final path with NO catalog entry — the scan is a
    /// background task and the endpoint serves before it finishes. A push for that key finds
    /// the key free, and the move used to run with overwrite: true, destroying the bytes on
    /// disk on the strength of a catalog that was still being built. The move refuses now, and
    /// the incumbent on disk is judged the way a catalog incumbent is: a different segment is a
    /// conflict, the file is kept, the entry is withdrawn.
    /// </summary>
    [Fact]
    public void A_push_landing_before_the_scan_reaches_the_file_cannot_overwrite_it()
    {
        long now = DateTime.UtcNow.Ticks;
        string finalPath = WritePeerSegment(Peer, 21, now, events: 5);   // on disk, NOT imported
        byte[] incumbent = File.ReadAllBytes(finalPath);

        string staged = WritePeerSegment(Peer, 21, now + TimeSpan.TicksPerMinute, events: 7,
            path: Path.Combine(SegDir, "7-21.55667788.seg.tmp"));

        Assert.Equal(SegmentImportOutcome.ConflictDifferentSegment, _engine.ImportSegment(staged, finalPath));

        Assert.Equal(incumbent, File.ReadAllBytes(finalPath));
        Assert.True(File.Exists(staged), "a refused import moved the body it refused");
        Assert.DoesNotContain(_engine.ListSegments(), x => x.NodeId.Value == Peer.Value && x.Id.Value == 21ul);
    }

    /// <summary>
    /// And the half that must stay open: the same segment re-pushed into that window — after a
    /// restart, before the scan — is ordinary traffic, answered as the catalog-incumbent branch
    /// answers it: registered, incumbent bytes kept, body dropped.
    /// </summary>
    [Fact]
    public void A_re_push_landing_before_the_scan_keeps_the_incumbent_and_registers()
    {
        long now = DateTime.UtcNow.Ticks;
        string finalPath = WritePeerSegment(Peer, 22, now, events: 5);   // on disk, NOT imported
        byte[] incumbent = File.ReadAllBytes(finalPath);

        string staged = WritePeerSegment(Peer, 22, now, events: 5,
            path: Path.Combine(SegDir, "7-22.99aabbcc.seg.tmp"),
            template: "peer RECOMPRESSED {n}");   // same five fields, different bytes

        Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(staged, finalPath));

        Assert.Equal(incumbent, File.ReadAllBytes(finalPath));
        Assert.False(File.Exists(staged), "the staged body was not cleaned up");
        var entry = Assert.Single(_engine.ListSegments(), x => x.NodeId.Value == Peer.Value && x.Id.Value == 22ul);
        Assert.Equal(finalPath, entry.FilePath);
        // The entry describes the file that SURVIVED, not the body that was deleted: this
        // branch exists because the two may differ in bytes, so an entry built from the staged
        // body carried sizes belonging to a file that no longer exists.
        Assert.Equal(incumbent.LongLength, entry.CompressedBytes);
    }

    /// <summary>
    /// An UNREADABLE file at the final path refuses the push — and says that, rather than a
    /// diagnosis the code never made. This used to come back as ConflictDifferentSegment, and
    /// the endpoint then told the sender "already held by a different file… two nodes appear
    /// to be configured with NodeId N": claims about a file that could not be read and about a
    /// second node for which there was no evidence, quoted at Error by the sender and marked
    /// permanent by the contract — when the real cause is local and clears with the file.
    /// </summary>
    [Fact]
    public void An_unreadable_incumbent_refuses_with_its_own_answer_not_a_guess()
    {
        long now = DateTime.UtcNow.Ticks;
        Directory.CreateDirectory(SegDir);
        string finalPath = Path.Combine(SegDir, $"{Peer.Value}-23.seg");
        File.WriteAllBytes(finalPath, [0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02, 0x03]);

        string staged = WritePeerSegment(Peer, 23, now,
            path: Path.Combine(SegDir, "7-23.44556677.seg.tmp"));

        Assert.Equal(SegmentImportOutcome.ConflictUnreadableIncumbent, _engine.ImportSegment(staged, finalPath));

        Assert.Equal(8, new FileInfo(finalPath).Length);   // the unreadable file was kept, not replaced
        Assert.True(File.Exists(staged), "a refused import moved the body it refused");
        Assert.DoesNotContain(_engine.ListSegments(), x => x.NodeId.Value == Peer.Value && x.Id.Value == 23ul);
    }


    // ── Issues #47, #49, #52 ─────────────────────────────────────────────────

    /// <summary>
    /// #47: the temp-file sweep belongs to the constructor, where nothing can be writing yet —
    /// the catalog scan runs in the background with the replication endpoint and WAL recovery
    /// already live, and both stage files its masks match. Two halves pin both sides: a
    /// leftover from a dead process is swept at construction, and a file that appears AFTER
    /// construction (a live push's staging body) survives the scan untouched.
    /// </summary>
    [Fact]
    public void The_temp_sweep_runs_at_construction_and_never_inside_the_scan()
    {
        Directory.CreateDirectory(SegDir);
        string live = Path.Combine(SegDir, "9-3.aabbccdd.seg.tmp");
        File.WriteAllBytes(live, [1, 2, 3]);

        _engine.LoadSegmentCatalog();   // the background scan, driven by hand

        Assert.True(File.Exists(live),
            "the catalog scan deleted a staging file out from under its writer — the sweep is " +
            "running inside the scan again instead of in the constructor");
        File.Delete(live);

        // And the constructor DOES sweep: a leftover from a previous process is gone before
        // anything can race it.
        string dir2 = Path.Combine(Path.GetTempPath(), "ameto-segkey-" + Guid.NewGuid().ToString("N"));
        string seg2 = Path.Combine(dir2, "segments");
        Directory.CreateDirectory(seg2);
        string leftover = Path.Combine(seg2, "0-1.deadbeef.seg.tmp");
        File.WriteAllBytes(leftover, [1, 2, 3]);
        var engine2 = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = dir2 }),
            new RetentionStore(new ServerOptions { DataDirectory = dir2 }, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        try
        {
            Assert.False(File.Exists(leftover), "a leftover temp file survived construction");
        }
        finally
        {
            engine2.DisposeAsync().AsTask().GetAwaiter().GetResult();
            try { Directory.Delete(dir2, true); }
            catch (Exception ex) { Console.WriteLine($"temp dir left behind: {dir2} — {ex.Message}"); }
        }
    }

    /// <summary>
    /// #49: the same-segment heuristic used to fall THROUGH to File.Move(overwrite: true), so
    /// five coinciding header fields — path, min, max, count, level, with the path route-derived
    /// and therefore equal for every sender — replaced the bytes being served with the incoming
    /// body. The header carries no digest, so the heuristic cannot be hardened; what it can do
    /// is fail toward keeping the file it protects. A re-push now keeps the incumbent's bytes
    /// and drops the staged body, and the sender still hears success, which for an idempotent
    /// push is what success means.
    /// </summary>
    [Fact]
    public void A_body_matching_every_header_field_cannot_replace_the_bytes_being_served()
    {
        long now = DateTime.UtcNow.Ticks;
        string finalPath = WritePeerSegment(Peer, 12, now);
        Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(finalPath));
        byte[] served = File.ReadAllBytes(finalPath);

        // A DIFFERENT file with an identical five-tuple: same ids, same timestamps, same count,
        // same level — different template text, therefore different bytes.
        string staged = WritePeerSegment(Peer, 12, now,
            path: Path.Combine(SegDir, "7-12.11223344.seg.tmp"),
            template: "peer DIFFERENT {n}");
        Assert.NotEqual(served, File.ReadAllBytes(staged));

        Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(staged, finalPath));

        Assert.Equal(served, File.ReadAllBytes(finalPath));   // the incumbent's bytes survived
        Assert.False(File.Exists(staged), "the staged body was not cleaned up");
        Assert.Single(_engine.ListSegments(), s => s.Id.Value == 12ul && s.NodeId.Value == Peer.Value);
    }

    /// <summary>
    /// #52: an import publishes its catalog entry BEFORE its File.Move lands the file — the
    /// reverse order was the earlier bug — so a retention delete landing inside that window
    /// removed the fresh entry, failed to delete a file that was not there yet, and the move
    /// then produced a file no entry names. The delete now serialises with the import: parked
    /// inside the import's lock, it must wait; released, it must run.
    /// </summary>
    [Fact]
    public async Task A_delete_landing_inside_an_imports_window_waits_it_out()
    {
        long now = DateTime.UtcNow.Ticks;
        Write(20, now);
        await _engine.FlushHotTierAsync();
        var local = Assert.Single(_engine.ListSegments());

        var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate   = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _engine._beforeImportPublish = () => { parked.TrySetResult(); gate.Task.GetAwaiter().GetResult(); };

        string peerPath = WritePeerSegment(Peer, 40, now);
        var import = Task.Run(() => _engine.ImportSegment(peerPath));
        await parked.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var delete = Task.Run(() => _engine.DeleteSegmentAsync(SegmentKey.Of(local)));
        Assert.False(delete.Wait(TimeSpan.FromMilliseconds(300)),
            "the delete ran inside the import's publish window instead of waiting for its lock");

        gate.TrySetResult();
        Assert.Equal(SegmentImportOutcome.Registered, await import.WaitAsync(TimeSpan.FromSeconds(10)));
        await delete.WaitAsync(TimeSpan.FromSeconds(10));

        // Both effects, in full: the local segment is gone, file and entry together, and the
        // imported one is present, file and entry together.
        Assert.False(File.Exists(local.FilePath));
        var remaining = Assert.Single(_engine.ListSegments());
        Assert.Equal(Peer.Value, remaining.NodeId.Value);
        Assert.True(File.Exists(remaining.FilePath));
    }

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
