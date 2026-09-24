using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// A merge DELETES ITS SOURCES, so every interruption has to leave the data either wholly in the
/// sources or wholly in the merged file — never split, never both.
///
/// <para>The protocol that guarantees it: write the manifest → write the merged file to
/// <c>.seg.tmp</c> → move it to its final name → publish → delete the sources → drop the
/// manifest. The manifest is the only durable record that a set of sources has been duplicated,
/// so it must exist BEFORE the merged file can be seen by the catalog and survive until the last
/// source is gone. These tests reconstruct the on-disk state at each interruption point and
/// restart the engine on it.</para>
/// </summary>
public sealed class StreamingMergeCrashSafetyTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mergecrash-" + Guid.NewGuid().ToString("N"));
    private string SegDir => Path.Combine(_dir, "segments");
    private StorageEngine _engine = null!;
    private CapturingLogger _log = new();

    /// <summary>
    /// What the engine logged, so "the merge ran and unwound itself" is OBSERVABLE rather than
    /// inferred. The abort path is the only place a merge pass logs a warning carrying an
    /// exception, which is what lets a test tell it apart from a pass that selected no batch.
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

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _engine = NewEngine();
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private StorageEngine NewEngine()
    {
        _log = new CapturingLogger();
        return new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = _dir }),
            new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
            _log)
        {
            // These tests exercise the crash protocol, not the index build; production merges
            // wait until the sink factory is wired.
            _allowIndexlessMerge = true,
        };
    }

    /// <summary>
    /// Asserts the planner really HAD a batch to select from what is currently staged: every
    /// segment in one bucket, that bucket over its own threshold. Without it, a test whose
    /// expected outcome is "the merge returned false and touched nothing" is satisfied just as
    /// well by a planner that selected nothing and never opened a file — the two leave
    /// bit-identical state, and no assertion anywhere would notice the difference.
    /// </summary>
    private void AssertABatchWasSelectable(LogLevel level)
    {
        var staged = _engine.ListSegments();
        long now   = DateTime.UtcNow.Ticks;

        var buckets = staged.Select(s => MergeBucketGrid.BucketOf(s.MaxTimestampTicks, level)).Distinct().ToList();
        Assert.True(buckets.Count == 1,
            $"staging spread over {buckets.Count} buckets — the threshold applies to each separately");

        int needed = MergeBucketGrid.PlannerFanoutFor(buckets[0], level, now);
        Assert.True(staged.Count >= needed,
            $"{staged.Count} source(s) against a fanout of {needed} — the planner would select nothing, " +
            "so anything this test observes afterwards it would observe with the merge path removed");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static byte[] Props(int i)
    {
        var buf = new ArrayBufferWriter<byte>(96);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(3);
        w.Write("n");     w.Write((long)i);
        w.Write("key");   w.Write("wallet:" + i);
        w.Write("shard"); w.Write(i % 7);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>One flush = one small segment (single level, so one file per round).</summary>
    private async Task WriteSegmentAsync(int round, int count, long? baseTicks = null)
    {
        for (int i = 0; i < count; i++)
        {
            int n = round * 1000 + i;
            var header = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)(round * 100_000 + i)).RawValue,
                TimestampUtcTicks        = (baseTicks ?? DateTime.UtcNow.Ticks) + n * TimeSpan.TicksPerMillisecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {n} round " + round % 3),
                ServiceNamePoolIndex     = _engine.TemplatePool.Intern("Svc." + round % 4),
                TraceIdHi                = (ulong)(n + 1),
                TraceIdLo                = (ulong)(n + 2),
                SpanId                   = (ulong)(n + 3),
            };
            var exc = n % 17 == 0
                ? new ExceptionInfo { Type = "System.InvalidOperationException", Message = "boom " + n }
                : null;
            Assert.True(_engine.TryWrite(header, Props(n), exception: exc));
        }
        await _engine.FlushHotTierAsync();
    }

    private static List<RawSegmentEvent> ReadRaw(string path)
    {
        using var r = SegmentReader.Open(path);
        return r.ReadAllRaw(new Dictionary<string, string>(StringComparer.Ordinal));
    }

    /// <summary>Every event the engine can currently serve from cold storage, id-ordered.</summary>
    private List<RawSegmentEvent> ReadEverything()
    {
        var all = new List<RawSegmentEvent>();
        foreach (var seg in _engine.ListSegments()) all.AddRange(ReadRaw(seg.FilePath));
        return all.OrderBy(e => e.Id).ToList();
    }

    private static void AssertSameEvents(List<RawSegmentEvent> expected, List<RawSegmentEvent> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        Assert.Equal(expected.Count, actual.Select(e => e.Id).Distinct().Count());   // exactly once
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Id,      actual[i].Id);
            Assert.Equal(expected[i].TsTicks, actual[i].TsTicks);
            Assert.Equal(expected[i].Props ?? [], actual[i].Props ?? []);
        }
    }

    /// <summary>Copies every .seg out of the segment directory so a "crash" can put them back.</summary>
    private Dictionary<string, byte[]> SnapshotSources()
    {
        var snap = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (var f in Directory.GetFiles(SegDir, "*.seg"))
            snap[Path.GetFileName(f)] = File.ReadAllBytes(f);
        return snap;
    }

    private void Restore(Dictionary<string, byte[]> snap, IEnumerable<string> names)
    {
        foreach (var n in names) File.WriteAllBytes(Path.Combine(SegDir, n), snap[n]);
    }

    private async Task RestartAsync()
    {
        await _engine.DisposeAsync();
        _engine = NewEngine();

        // THE WHOLE SCAN, not the first file out of it. Polling until ListSegments() was non-empty
        // returned as soon as the constructor's background scan had published one of ten, and the
        // callers below assert on all ten — green on an idle machine, 4/6/8-of-10 on a loaded CI
        // runner. The count was never the signal; the scan finishing is.
        await _engine.CatalogLoaded;
    }

    // ── Byte parity ───────────────────────────────────────────────────────────

    /// <summary>
    /// The merged file must be the sources' events, in (timestamp, id) order, unchanged: same
    /// ids, timestamps, levels, templates, services, raw property bytes, exceptions and trace
    /// correlation — each exactly once. Compared in FILE ORDER, because the k-way merge is what
    /// produces that order and a heap bug would show up as a permutation rather than a loss.
    /// </summary>
    [Fact]
    public async Task MergedFileIsByteParityWithItsSources()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 130);

        var expected = new List<RawSegmentEvent>();
        foreach (var seg in _engine.ListSegments()) expected.AddRange(ReadRaw(seg.FilePath));
        expected = expected
            .OrderBy(e => e.TsTicks).ThenBy(e => e.Id)
            .ToList();
        Assert.Equal(1300, expected.Count);

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        var merged = _engine.ListSegments().Single();
        Assert.Equal(1300u, merged.EventCount);

        var actual = ReadRaw(merged.FilePath);   // file order
        Assert.Equal(expected.Count, actual.Count);
        Assert.Equal(expected.Count, actual.Select(e => e.Id).Distinct().Count());

        for (int i = 0; i < expected.Count; i++)
        {
            var e = expected[i];
            var a = actual[i];
            Assert.Equal(e.Id,        a.Id);
            Assert.Equal(e.TsTicks,   a.TsTicks);
            Assert.Equal(e.Level,     a.Level);
            Assert.Equal(e.Template,  a.Template);
            Assert.Equal(e.Service,   a.Service);
            Assert.Equal(e.TraceIdHi, a.TraceIdHi);
            Assert.Equal(e.TraceIdLo, a.TraceIdLo);
            Assert.Equal(e.SpanId,    a.SpanId);
            Assert.Equal(e.Props ?? [], a.Props ?? []);
            Assert.Equal(e.Exception?.Type,    a.Exception?.Type);
            Assert.Equal(e.Exception?.Message, a.Exception?.Message);
        }

        // Ordering is the merge's own output, not an accident of the inputs.
        for (int i = 1; i < actual.Count; i++)
            Assert.True(actual[i - 1].TsTicks < actual[i].TsTicks ||
                        (actual[i - 1].TsTicks == actual[i].TsTicks && actual[i - 1].Id < actual[i].Id),
                        $"merged file is not sorted at ordinal {i}");
    }

    // ── Interruption points ───────────────────────────────────────────────────

    /// <summary>
    /// Killed while writing the merged file, before the manifest existed. Nothing names the
    /// sources as duplicated, so nothing may be deleted — and the half-written .seg.tmp must not
    /// survive as a segment.
    /// </summary>
    [Fact]
    public async Task CrashBeforeTheManifest_LeavesEverySourceIntact()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        Assert.Equal(600, before.Count);

        // A merge killed mid-write leaves exactly this: a partial temp file, no manifest.
        var tmp = Path.Combine(SegDir, "0-999-1-2.seg.tmp");
        File.WriteAllBytes(tmp, new byte[4096]);

        await RestartAsync();

        Assert.False(File.Exists(tmp));
        Assert.Empty(Directory.GetFiles(SegDir, "*.mergemanifest"));
        Assert.Equal(10, _engine.ListSegments().Count);
        AssertSameEvents(before, ReadEverything());
    }

    /// <summary>
    /// Killed after the manifest was written but before the merged file reached its final name.
    /// A manifest whose merged segment does not exist is a merge that never committed: drop the
    /// manifest, keep every source. Deleting on the manifest alone would lose the whole batch.
    /// </summary>
    [Fact]
    public async Task CrashAfterTheManifest_BeforeTheMergedFileLands_KeepsEverySource()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var names  = _engine.ListSegments().Select(s => Path.GetFileName(s.FilePath)).ToList();

        var mergedPath = Path.Combine(SegDir, "0-999-1-2.seg");
        File.WriteAllBytes(mergedPath + ".tmp", new byte[4096]);
        await File.WriteAllLinesAsync(mergedPath + ".mergemanifest", names);

        await RestartAsync();

        Assert.False(File.Exists(mergedPath + ".tmp"));
        Assert.False(File.Exists(mergedPath));
        Assert.Empty(Directory.GetFiles(SegDir, "*.mergemanifest"));
        Assert.Equal(10, _engine.ListSegments().Count);
        AssertSameEvents(before, ReadEverything());
    }

    /// <summary>
    /// Killed after the merged file was in place but before any source was deleted — the window
    /// where the data exists TWICE on disk. Recovery must finish the deletion; serving both would
    /// double every event in the batch.
    /// </summary>
    [Fact]
    public async Task CrashAfterPublication_BeforeDeletion_RemovesTheDuplicatedSources()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        var mergedPath = _engine.ListSegments().Single().FilePath;

        // Rewind to the instant after the Move: sources back on disk, manifest naming them.
        Restore(snap, snap.Keys);
        await File.WriteAllLinesAsync(mergedPath + ".mergemanifest", snap.Keys);

        await RestartAsync();

        foreach (var name in snap.Keys)
            Assert.False(File.Exists(Path.Combine(SegDir, name)), $"{name} survived recovery");
        Assert.Empty(Directory.GetFiles(SegDir, "*.mergemanifest"));
        Assert.Single(_engine.ListSegments());
        AssertSameEvents(before, ReadEverything());
    }

    /// <summary>
    /// Killed between the catalog swap and the first unlink — the window #85 moved the source
    /// deletes into. The catalog already names the output and no source, so the process serves
    /// every event exactly once while the source files sit on disk unnamed; the manifest names
    /// them, and the restart's recovery removes them before its catalog scan can register any.
    /// The crash is a throw from the hook in that window: nothing after it runs, as after a kill.
    /// </summary>
    [Fact]
    public async Task CrashAfterTheSwap_BeforeTheUnlinks_ServesEachEventOnce_AndRecoveryRemovesTheSources()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var before  = ReadEverything();
        var sources = _engine.ListSegments().Select(s => s.FilePath).ToList();
        Assert.Equal(10, sources.Count);

        _engine._afterMergeSwap = static () => throw new IOException("killed between the swap and the unlinks");
        await Assert.ThrowsAsync<IOException>(() => _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        _engine._afterMergeSwap = null;

        // In process: the output alone is served, the sources are on disk and named by nothing
        // but the manifest.
        var output = Assert.Single(_engine.ListSegments());
        Assert.Same(output, Assert.Single(_engine.GetSegments(null, null)));
        foreach (var path in sources) Assert.True(File.Exists(path), $"{path} was unlinked before the crash point");
        var manifest = Assert.Single(Directory.GetFiles(SegDir, "*.mergemanifest"));
        Assert.Equal(output.FilePath + ".mergemanifest", manifest);
        AssertSameEvents(before, ReadEverything());

        // The commit parked the sources, and shutdown makes one last attempt at every parked
        // delete. A killed process makes none, so neither may this one: the restart's recovery is
        // what this test is about.
        _engine._deleteSegmentFile = static _ => throw new IOException("the process is dead");
        await RestartAsync();

        foreach (var path in sources) Assert.False(File.Exists(path), $"{path} survived recovery");
        Assert.Empty(Directory.GetFiles(SegDir, "*.mergemanifest"));
        Assert.Equal(output.FilePath, Assert.Single(_engine.ListSegments()).FilePath);
        AssertSameEvents(before, ReadEverything());
    }

    /// <summary>
    /// A catalog scan that has read a source before the merge commits must not register it after.
    /// The scan registers a file it read unless a delete recorded the path for it or parked it,
    /// and the merge's sources no longer go through <c>DeleteSegmentAsync</c>: its commit has to
    /// record them itself, under the same gate, or the scan puts a source back beside the output
    /// — an entry for a file the commit unlinks, counted on top of the output that holds its
    /// events. The scan is held where it has read and closed the first source and not yet taken
    /// the gate; the whole merge runs there; the scan then finishes.
    /// </summary>
    [Fact]
    public async Task ACatalogScanRunningAcrossTheCommit_RegistersNoSource()
    {
        await _engine.CatalogLoaded;
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var before  = ReadEverything();
        var sources = _engine.ListSegments().Select(s => s.FilePath).ToHashSet(StringComparer.OrdinalIgnoreCase);

        using var atFile   = new ManualResetEventSlim();
        using var released = new ManualResetEventSlim();
        string? held = null;
        // Nothing in this hook may throw: the scan's quarantine catch would swallow it.
        _engine._beforeScanRegistersSegment = file =>
        {
            if (held is not null || !sources.Contains(file)) return;
            held = file;
            atFile.Set();
            released.Wait(TimeSpan.FromSeconds(30));
        };

        var scan = Task.Run(_engine.LoadSegmentCatalog);
        try
        {
            Assert.True(atFile.Wait(TimeSpan.FromSeconds(30)), "setup: the scan never reached a source");
            Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        }
        finally
        {
            released.Set();
            await scan.WaitAsync(TimeSpan.FromSeconds(30));
            _engine._beforeScanRegistersSegment = null;
        }

        Assert.False(File.Exists(held!), "setup: the held source survived the merge");
        var output = Assert.Single(_engine.ListSegments());   // 2: the scan put the source it had read back
        Assert.DoesNotContain(output.FilePath, sources);
        AssertSameEvents(before, ReadEverything());
        Assert.DoesNotContain(_log.Entries, e => e.Message.Contains("Quarantining", StringComparison.Ordinal));
    }

    /// <summary>
    /// A merge, and a whole maintenance pass, started while another merge is in flight do
    /// nothing (#85). The planner is deterministic, so a second merge picks the batch the first
    /// is merging; both commit an output, and the batch's events are on disk twice for good. And
    /// a pass's recovery sweep takes the first merge's manifest, beside an output already at its
    /// final name, for a crashed merge's, and deletes sources the catalog still names.
    ///
    /// <para>Both are started from the first merge's last step before its commit — output moved
    /// into place, sources still in the catalog — each on a pool thread, since a merge that is not
    /// stopped awaits without ConfigureAwait and this hook runs on the test's context.</para>
    /// </summary>
    [Fact]
    public async Task ASecondMergeOrPassDuringAMerge_DoesNothing()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var before  = ReadEverything();
        var sources = _engine.ListSegments().Select(s => s.FilePath).ToList();

        MergeOutcome? second = null, pass = null;
        bool sourcesOnDisk = false;
        int  commits = 0;
        _engine._beforeMergeSwap = () =>
        {
            if (Interlocked.Increment(ref commits) != 1) return;   // a second merge's own commit
            second        = Task.Run(() => _engine.MergeSmallSegmentsOnceAsync(CancellationToken.None)).GetAwaiter().GetResult();
            pass          = Task.Run(() => _engine.RunColdMaintenancePassAsync(CancellationToken.None)).GetAwaiter().GetResult();
            sourcesOnDisk = sources.All(File.Exists);
        };
        MergeOutcome first;
        try     { first = await _engine.MergeSmallSegmentsOnceAsync(CancellationToken.None); }
        finally { _engine._beforeMergeSwap = null; }

        Assert.Equal(MergeOutcome.Merged, first);   // setup
        // Busy, not NothingToMerge: the call learnt nothing about what is left to merge. Merged:
        // it merged the same batch again.
        Assert.Equal(MergeOutcome.Busy, second);
        Assert.Equal(MergeOutcome.Busy, pass);
        Assert.True(sourcesOnDisk, "a sweep deleted the sources of a merge that had not committed");
        Assert.Equal(1, commits);
        Assert.Single(_engine.ListSegments());
        AssertSameEvents(before, ReadEverything());
        Assert.Empty(Directory.GetFiles(SegDir, "*.mergemanifest"));

        // The gate is let go when a merge ends and when a pass ends: after one of each, a fresh
        // batch merges.
        Assert.Equal(MergeOutcome.NothingToMerge, await _engine.RunColdMaintenancePassAsync(CancellationToken.None));   // one segment
        for (int round = 10; round < 20; round++)
            await WriteSegmentAsync(round, 60);
        Assert.Equal(MergeOutcome.Merged, await _engine.MergeSmallSegmentsOnceAsync(CancellationToken.None));   // Busy: the gate was never let go
    }

    /// <summary>
    /// The maintenance loop's pause after each outcome. A pass that lost the gate to a running
    /// merge (a test's, a manual trigger's) takes the backlog pause, not the idle one: the gate
    /// says nothing about whether more is waiting, and read as "nothing to merge" it cost ten
    /// minutes of compaction every time.
    /// </summary>
    [Fact]
    public void TheMaintenanceLoopPausesShortAfterABusyPass()
    {
        Assert.Equal(TimeSpan.FromSeconds(15),  StorageEngine.PauseAfter(MergeOutcome.Merged));
        Assert.Equal(TimeSpan.FromSeconds(15),  StorageEngine.PauseAfter(MergeOutcome.Busy));   // 600 when Busy read as idle
        Assert.Equal(TimeSpan.FromSeconds(600), StorageEngine.PauseAfter(MergeOutcome.NothingToMerge));
    }

    /// <summary>A replicated segment's file, as a peer pushes it: another node's id, four events.</summary>
    private string WritePeerSegment(ulong segId)
    {
        var peer = new NodeId(7);
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(16, 1L << 20);
        long now = DateTime.UtcNow.Ticks;
        for (int i = 0; i < 4; i++)
            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(peer.Value, (uint)i).RawValue,
                TimestampUtcTicks        = now + i * TimeSpan.TicksPerMillisecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = pool.Intern("peer {n}"),
            }, Props(i), "peer {n}"));
        hot.Freeze();

        string path = Path.Combine(SegDir, $"{peer.Value}-{segId}.seg");
        using (var writer = new SegmentWriter(path))
        {
            writer.WriteEvents(hot, pool);
            writer.Finalise(peer, new SegmentId(segId));
        }
        return path;
    }

    /// <summary>
    /// The commit holds <c>_importLock</c> and the scan gate for the swap, not for the unlinks
    /// after it. Held across up to 512 of them, it stalled every replication POST, every
    /// retention delete and every header scan that met a missing file (all three take
    /// <c>_importLock</c>) for the whole of a commit — seconds on NTFS with an on-access scanner,
    /// every 15 s while a backlog drains. From the window after the swap and before the first
    /// unlink, an import and a delete run on another thread and must finish while the merge
    /// waits there. Each unlink still takes <c>_importLock</c> for its one file.
    /// </summary>
    [Fact]
    public async Task BetweenTheSwapAndTheUnlinks_AnImportAndADeleteDoNotWaitForTheCommit()
    {
        await _engine.CatalogLoaded;   // the peer file below is the import's, not the boot scan's
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var peerPath = WritePeerSegment(900);

        SegmentImportOutcome? imported = null;
        bool finishedInside = false, sourcesOnDisk = false;
        var sources = _engine.ListSegments().Select(s => s.FilePath).ToList();
        _engine._afterMergeSwap = () =>
        {
            sourcesOnDisk = sources.All(File.Exists);
            var other = Task.Run(async () =>
            {
                imported = _engine.ImportSegment(peerPath);
                await _engine.DeleteSegmentAsync(new SegmentKey(new NodeId(7), new SegmentId(900)));
            });
            // Bounded, and only the failing case waits it out: blocked, the two cannot finish
            // until this hook returns.
            finishedInside = other.Wait(TimeSpan.FromSeconds(10));
        };
        bool merged;
        try     { merged = await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None); }
        finally { _engine._afterMergeSwap = null; }

        Assert.True(merged, "setup: the merge merged nothing");
        Assert.True(sourcesOnDisk, "setup: the hook did not run before the unlinks");
        Assert.True(finishedInside, "an import and a delete waited for the merge's commit to finish its unlinks");
        Assert.Equal(SegmentImportOutcome.Registered, imported);
        Assert.False(File.Exists(peerPath));
        Assert.Single(_engine.ListSegments());
        foreach (var path in sources) Assert.False(File.Exists(path), $"{path} survived the merge");
    }

    /// <summary>
    /// With the unlinks out of the commit's hold, a catalog scan can START between the two. It
    /// runs merge recovery first, which deletes the sources, except one a reader holds open: that
    /// one it lists, and the park the commit made in its hold is what keeps it from registering
    /// the file beside the output. Windows-only: elsewhere the held file is deleted anyway.
    /// </summary>
    [Fact]
    public async Task ACatalogScanStartingBetweenTheSwapAndTheUnlinks_RegistersNoHeldSource()
    {
        if (!OperatingSystem.IsWindows()) return;
        await _engine.CatalogLoaded;
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var victim = _engine.ListSegments().OrderBy(s => s.MinTimestampTicks).First().FilePath;

        using (new FileStream(victim, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            _engine._afterMergeSwap = _engine.LoadSegmentCatalog;
            bool merged;
            try     { merged = await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None); }
            finally { _engine._afterMergeSwap = null; }

            Assert.True(merged, "setup: the merge merged nothing");
            Assert.True(File.Exists(victim), "setup: the held source should have survived its unlink");
            var output = Assert.Single(_engine.ListSegments());   // 2: the scan registered the held source
            Assert.NotEqual(victim, output.FilePath);
            AssertSameEvents(before, ReadEverything());
            Assert.Equal(1, _engine.PendingSegmentDeleteCount);   // parked for the retry
        }
    }

    /// <summary>
    /// The commit parks every source past <see cref="StorageEngine.PendingSegmentDeleteCap"/>,
    /// because that park guards a scan, not a retry. What is still parked once the unlinks have
    /// been tried is held to the cap again: the rest is let go as a failed delete past the cap is
    /// (logged, left on disk), and the manifest keeps it for the recovery sweep. Here every
    /// source's unlink fails and the cap is 2.
    /// </summary>
    [Fact]
    public async Task SourcesThatCannotBeUnlinked_StayParkedOnlyUpToTheCap()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var sources = _engine.ListSegments().Select(s => s.FilePath).ToHashSet(StringComparer.OrdinalIgnoreCase);
        _engine.PendingSegmentDeleteCap = 2;
        _engine._deleteSegmentFile = path =>
        {
            if (sources.Contains(path)) throw new IOException("the volume refuses deletes");
            File.Delete(path);
        };

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");

        Assert.Equal(2, _engine.PendingSegmentDeleteCount);   // 10: every source parked past the cap for good
        Assert.Equal(8, _log.Entries.Count(e => e.Message.Contains("is not retried: the pending-delete set is full", StringComparison.Ordinal)));
        foreach (var path in sources) Assert.True(File.Exists(path), "setup: an unlink that was meant to fail succeeded");
        Assert.Single(Directory.GetFiles(SegDir, "*.mergemanifest"));   // the recovery sweep's copy of the list
        Assert.Single(_engine.ListSegments());
    }

    /// <summary>
    /// A maintenance pass that throws after taking the merge gate and before its merge takes the
    /// gate over lets go of it. The sweeps catch their own failures but not a throw from the
    /// logging in those catches; left taken, the gate made every later pass and merge report
    /// "nothing merged" for the life of the process, and compaction stopped in silence.
    /// </summary>
    [Fact]
    public async Task APassThatThrowsBeforeItsMerge_LetsGoOfTheGate()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);

        _engine._beforeMaintenanceSweeps = static () => throw new InvalidOperationException("a sweep's logger threw");
        try     { await Assert.ThrowsAsync<InvalidOperationException>(() => _engine.RunColdMaintenancePassAsync(CancellationToken.None)); }
        finally { _engine._beforeMaintenanceSweeps = null; }

        Assert.Equal(MergeOutcome.Merged, await _engine.RunColdMaintenancePassAsync(CancellationToken.None));   // Busy: the gate stayed taken
        Assert.Single(_engine.ListSegments());
    }

    /// <summary>Killed halfway through deleting the sources — the rest must go on restart.</summary>
    [Fact]
    public async Task CrashMidDeletion_FinishesTheRemainingSources()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        var mergedPath = _engine.ListSegments().Single().FilePath;

        // Half the sources deleted, half still there; the manifest still names all ten.
        var survivors = snap.Keys.Take(5).ToList();
        Restore(snap, survivors);
        await File.WriteAllLinesAsync(mergedPath + ".mergemanifest", snap.Keys);

        await RestartAsync();

        foreach (var name in survivors)
            Assert.False(File.Exists(Path.Combine(SegDir, name)), $"{name} survived recovery");
        Assert.Empty(Directory.GetFiles(SegDir, "*.mergemanifest"));
        Assert.Single(_engine.ListSegments());
        AssertSameEvents(before, ReadEverything());
    }

    /// <summary>
    /// A source held open (an in-flight query's mapped view) cannot be deleted on Windows. The
    /// merge must still publish and must still serve each event exactly once — the source is out
    /// of the catalog the moment the merged file is in it, whether or not the file is gone — and
    /// the manifest has to survive so the sweep finishes the deletion later.
    /// </summary>
    [Fact]
    public async Task SourceHeldOpen_PublishesWithoutDuplicates_AndFinishesLater()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var victim = _engine.ListSegments().OrderBy(s => s.MinTimestampTicks).First();

        using (var hold = new FileStream(victim.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));

            Assert.True(File.Exists(victim.FilePath));                    // delete blocked by the handle
            Assert.Single(Directory.GetFiles(SegDir, "*.mergemanifest")); // kept for the sweep
            Assert.Single(_engine.ListSegments());                        // catalog holds only the merged file
            AssertSameEvents(before, ReadEverything());                   // no duplicates while held
        }

        // Handle released → the recovery sweep finishes the interrupted deletion.
        await RestartAsync();

        Assert.False(File.Exists(victim.FilePath));
        Assert.Empty(Directory.GetFiles(SegDir, "*.mergemanifest"));
        AssertSameEvents(before, ReadEverything());
    }

    /// <summary>
    /// A source whose HEADER is intact but whose blocks are corrupt opens cleanly during
    /// planning and blows up mid-stream — after the manifest is on disk. That is the one path
    /// where the manifest-first ordering has to unwind itself: the merged file never reaches its
    /// final name (it is written to .seg.tmp), so dropping the manifest restores the pre-merge
    /// state exactly. Nothing published, nothing deleted, nothing left behind.
    ///
    /// <para>The sealed anchor is load-bearing even though this test asserts FALSE. In an open
    /// bucket four sources are below the fanout, so the planner selects no batch at all and the
    /// merge returns false without ever opening the corrupt file — every assertion below then
    /// holds for a reason that has nothing to do with the abort path. Anchoring on the grid is
    /// what makes that false mean "the merge ran and unwound itself" on every run
    /// (see <see cref="MergeBucketGrid"/>).</para>
    ///
    /// <para>The anchor is not ENOUGH, though, and that is the point of the two positive
    /// assertions below. "Returned false, nothing on disk changed" is bit-for-bit the state a
    /// planner that selected nothing would leave, so the test's meaning rested entirely on an
    /// anchor it never checked: verified by mutation, deleting the abort's manifest unwind AND
    /// moving the anchor to an open bucket makes this test pass. Nothing in it distinguished a
    /// working unwind from a merge that never ran. It now requires that a batch WAS selectable,
    /// and that the merge actually reached the corrupt block and aborted on the whole batch.</para>
    /// </summary>
    [Fact]
    public async Task CorruptBlockMidStream_AbortsTheMerge_AndLeavesNoManifest()
    {
        long old = MergeBucketGrid.SealedBucketStart(LogLevel.Information);
        for (int round = 0; round < 4; round++)
            await WriteSegmentAsync(round, 60, baseTicks: old + round * TimeSpan.TicksPerHour);
        var files = Directory.GetFiles(SegDir, "*.seg").OrderBy(f => f).ToList();
        Assert.Equal(4, files.Count);

        // Scribble over the first block's COMPRESSED payload only. The header (46 B) and the
        // frame's two size fields (offsets 46..53) stay valid, so SegmentReader.Open — which
        // reads the header, footer and block index — still succeeds.
        var victim = _engine.ListSegments().OrderBy(s => s.MinTimestampTicks).First().FilePath;
        var bytes  = File.ReadAllBytes(victim);
        for (int i = 54; i < Math.Min(bytes.Length - 64, 400); i++) bytes[i] = 0xFF;
        File.WriteAllBytes(victim, bytes);
        using (var probe = SegmentReader.Open(victim)) { }   // still opens: the damage is inside a block

        // The planner has a batch to take: four sources in one bucket, and that bucket sealed.
        AssertABatchWasSelectable(LogLevel.Information);

        int loggedBefore = _log.Entries.Count;
        Assert.False(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));

        // …and it took it, streamed into the corrupt block and unwound. The abort is the only
        // thing in a merge pass that logs an exception, and it names the size of the batch it
        // gave up on — four, the whole window, not one skipped file.
        var aborts = _log.Entries.Skip(loggedBefore).Where(e => e.Error is not null).ToList();
        Assert.True(aborts.Count == 1,
            $"expected exactly one aborted merge, saw {aborts.Count}: {string.Join(" | ", aborts.Select(a => a.Message))}");
        Assert.Contains("4 source(s)", aborts[0].Message);

        Assert.Empty(Directory.GetFiles(SegDir, "*.mergemanifest"));
        Assert.Empty(Directory.GetFiles(SegDir, "*.seg.tmp"));
        Assert.Equal(4, Directory.GetFiles(SegDir, "*.seg").Length);   // every source still there
        foreach (var f in files) Assert.True(File.Exists(f));
    }

    /// <summary>
    /// A source that turns unreadable between the catalog scan and the merge must abort the whole
    /// batch: nothing published, nothing deleted, no manifest left behind. The remaining sources
    /// stay exactly as they were.
    /// </summary>
    [Fact]
    public async Task UnreadableSource_AbortsTheBatch_WithoutTouchingAnything()
    {
        // Settled by construction → 2 sources suffice. A fixed offset back from UtcNow lands in
        // the open bucket for two days in every seven, where four sources are below the fanout
        // and the merge never runs at all (see MergeBucketGrid).
        long old = MergeBucketGrid.SealedBucketStart(LogLevel.Information);
        for (int round = 0; round < 4; round++)
            await WriteSegmentAsync(round, 60, baseTicks: old + round * TimeSpan.TicksPerHour);

        AssertABatchWasSelectable(LogLevel.Information);

        var victim = _engine.ListSegments().OrderBy(s => s.MinTimestampTicks).First();
        File.WriteAllBytes(victim.FilePath, [0xDE, 0xAD, 0xBE, 0xEF]);  // magic mismatch

        // The remaining three still merge — the corrupt file is skip-listed, not fatal.
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));

        Assert.True(File.Exists(victim.FilePath));                      // corrupt file untouched
        Assert.Empty(Directory.GetFiles(SegDir, "*.mergemanifest"));    // merge committed cleanly
        var segs = _engine.ListSegments();
        Assert.Contains(segs, s => s.EventCount == 180);                // the three healthy ones
    }
}
