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

    /// <summary>
    /// A fact that needs a file held open with a share mode only Windows enforces: a holder that lets
    /// nothing delete, move or read it. Elsewhere an open file unlinks, moves and reads, so the fact
    /// has nothing to show there; it is reported SKIPPED, not passed by returning early.
    /// </summary>
    public sealed class WindowsFactAttribute : FactAttribute
    {
        public WindowsFactAttribute()
        {
            if (!OperatingSystem.IsWindows())
                Skip = "Windows only: holds a file open with a share mode only Windows enforces";
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

    /// <summary>
    /// <see cref="RestartAsync"/> with the new engine's boot catalog scan held until
    /// <paramref name="seams"/> has set what it needs. The scan is what runs merge recovery, so a
    /// seam set after the constructor returns would race it. No maintenance pass runs on its own.
    /// </summary>
    private async Task RestartWithSeamsAsync(Action<StorageEngine> seams)
    {
        await _engine.DisposeAsync();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var _ = Seam.ReleasedOnExit(release);   // a throw from the seams must not strand the scan
        _log    = new CapturingLogger();
        _engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = _dir }),
            new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
            _log, Timeout.InfiniteTimeSpan, bootScanHeldUntil: release.Task)
        {
            _allowIndexlessMerge = true,
        };
        seams(_engine);
        release.SetResult();
        await _engine.CatalogLoaded;
    }

    /// <summary>An unlink that refuses <paramref name="held"/>, as a volume or a holder without FileShare.Delete does, and deletes anything else.</summary>
    private static Action<string> UnlinkRefusing(params string[] held) => path =>
    {
        if (held.Contains(path, StringComparer.OrdinalIgnoreCase)) throw new IOException("an antivirus scanner holds it");
        File.Delete(path);
    };

    private string[] Manifests() => Directory.GetFiles(SegDir, "*.mergemanifest");

    /// <summary>
    /// Tears a segment where the catalog scan cannot see it: the first block's compressed payload is
    /// overwritten, its frame and the file's footer left whole. The scan's open reads frames, not
    /// payloads, so it registers such a file; only a decode of its rows refuses it. (A lost
    /// block-index count no longer serves for this: the reader refuses a file that lists no block
    /// for the events its header counts, #119.)
    /// </summary>
    private static async Task LoseTheFirstBlockPayloadAsync(string path)
    {
        var bytes = await File.ReadAllBytesAsync(path);
        const int header = 46;   // SegmentFileHeader.Size: the first block's frame follows it
        int compressed = BitConverter.ToInt32(bytes, header + 4);
        for (int i = header + 8; i < header + 8 + Math.Min(compressed, 256); i++) bytes[i] = 0xFF;
        await File.WriteAllBytesAsync(path, bytes);
        using (SegmentReader.Open(path, computeUncompressedBytes: true)) { }   // still opens: the damage is inside a block
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
    /// The scan registers a file it read unless its path is parked or recorded for it, and the
    /// merge's sources no longer go through <c>DeleteSegmentAsync</c>, so the merge has to do one
    /// of the two itself, or the scan puts a source back beside the output — an entry for a file
    /// the merge unlinks, counted on top of the output that holds its events. Three things do it,
    /// any one enough here: the commit parks each source in its hold, the unlink records the path
    /// before the park goes (<c>TryCompletePendingSegmentDelete</c>), and the commit records it
    /// too (<c>ForgetRemovedSegment</c>) — the last redundant with the first two. So this pins the
    /// contract, not the commit's own record: it goes red when the commit neither records nor
    /// parks and unlinks directly, as a <c>DeleteSegmentAsync</c> without its record would. The
    /// scan is held where it has read and closed the first source and not yet taken the gate;
    /// the whole merge runs there; the scan then finishes.
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
        await _engine.CatalogLoaded;   // the outcome, unlike the bool merge, is Busy until the boot scan is done
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

    /// <summary>
    /// A replicated segment shaped like this fixture's own flushes — <paramref name="count"/> events of
    /// round <paramref name="round"/>, now — so the planner takes it into a merge beside them.
    /// </summary>
    private string WritePeerSegment(ulong segId, int round, int count)
    {
        var peer = new NodeId(7);
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(count * 2, 1L << 22);
        long now = DateTime.UtcNow.Ticks;
        for (int i = 0; i < count; i++)
        {
            int n = round * 1000 + i;
            string template = "evt {n} round " + round % 3;
            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(peer.Value, (uint)(round * 100_000 + i)).RawValue,
                TimestampUtcTicks        = now + n * TimeSpan.TicksPerMillisecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = pool.Intern(template),
                ServiceNamePoolIndex     = pool.Intern("Svc." + round % 4),
                TraceIdHi                = (ulong)(n + 1),
                TraceIdLo                = (ulong)(n + 2),
                SpanId                   = (ulong)(n + 3),
            }, Props(n), template));
        }
        hot.Freeze();
        string path = Path.Combine(SegDir, $"{peer.Value}-{segId}.seg");
        using (var writer = new SegmentWriter(path))
        {
            writer.WriteEvents(hot, pool);
            writer.Finalise(peer, new SegmentId(segId));
        }
        return path;
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
    [WindowsFact]
    public async Task ACatalogScanStartingBetweenTheSwapAndTheUnlinks_RegistersNoHeldSource()
    {
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
        await _engine.CatalogLoaded;   // a pass is Busy until the boot scan is done
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);

        _engine._beforeMaintenanceSweeps = static () => throw new InvalidOperationException("a sweep's logger threw");
        try     { await Assert.ThrowsAsync<InvalidOperationException>(() => _engine.RunColdMaintenancePassAsync(CancellationToken.None)); }
        finally { _engine._beforeMaintenanceSweeps = null; }

        Assert.Equal(MergeOutcome.Merged, await _engine.RunColdMaintenancePassAsync(CancellationToken.None));   // Busy: the gate stayed taken
        Assert.Single(_engine.ListSegments());
    }

    /// <summary>
    /// The commit's guard parks do not fill the pending-delete set for a failed delete that lands
    /// among the merge's unlinks. Cap 4, a ten-source merge; from the window after the swap a
    /// peer segment is imported and deleted with its unlink failing, as an open reader makes it
    /// fail. The set then holds the merge's ten guards. Counted, they refused the peer's park as
    /// "set full": nothing in this process retried its file, and after a restart the boot scan
    /// served the expired segment again until the next retention pass.
    /// </summary>
    [Fact]
    public async Task AFailedDeleteAmongTheMergesUnlinks_IsParkedPastItsGuards()
    {
        await _engine.CatalogLoaded;   // the peer file below is the import's, not the boot scan's
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var peerPath = WritePeerSegment(901);
        var peerKey  = new SegmentKey(new NodeId(7), new SegmentId(901));

        _engine.PendingSegmentDeleteCap = 4;
        _engine._deleteSegmentFile = path =>
        {
            if (string.Equals(path, peerPath, StringComparison.OrdinalIgnoreCase)) throw new IOException("a query still maps it");
            File.Delete(path);
        };
        int guardsSeen = -1;
        SegmentImportOutcome? imported = null;
        _engine._afterMergeSwap = () =>
        {
            // Values kept, not asserted: a throw here is the merge's, and ends it.
            guardsSeen = _engine.PendingSegmentDeleteCount;
            imported   = _engine.ImportSegment(peerPath);
            _engine.DeleteSegmentAsync(peerKey).GetAwaiter().GetResult();
        };
        bool merged;
        try     { merged = await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None); }
        finally { _engine._afterMergeSwap = null; }

        Assert.True(merged, "setup: the merge merged nothing");
        Assert.Equal(10, guardsSeen);   // setup: the set was past the cap with the merge's guards alone
        Assert.Equal(SegmentImportOutcome.Registered, imported);
        Assert.True(File.Exists(peerPath), "setup: the peer's unlink was meant to fail");
        Assert.Equal(1, _engine.PendingSegmentDeleteCount);   // 0: refused as "set full", never retried
        Assert.DoesNotContain(_log.Entries, e => e.Message.Contains("is not retried: the pending-delete set is full", StringComparison.Ordinal));

        // Parked, so retried: once the reader is gone the file goes.
        _engine._deleteSegmentFile = File.Delete;
        Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
        Assert.False(File.Exists(peerPath));
    }

    /// <summary>
    /// A commit that throws before its unlinks leaves its guard parks as ordinary parked deletes,
    /// and from then on they count toward the cap like any other: the guard count does not
    /// outlive the commit. Cap 10, ten sources parked by a commit whose hook throws; a delete
    /// failing afterwards finds the set full and is refused, where a guard count left at ten
    /// would have let the set grow past its cap.
    /// </summary>
    [Fact]
    public async Task AfterACommitThatThrows_ItsGuardParksCountTowardTheCap()
    {
        await _engine.CatalogLoaded;   // the peer file below is the import's, not the boot scan's
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var peerPath = WritePeerSegment(902);

        _engine.PendingSegmentDeleteCap = 10;
        _engine._afterMergeSwap = static () => throw new IOException("the commit failed before its unlinks");
        await Assert.ThrowsAsync<IOException>(() => _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        _engine._afterMergeSwap = null;
        Assert.Equal(10, _engine.PendingSegmentDeleteCount);   // setup: the guards stayed parked

        _engine._deleteSegmentFile = path =>
        {
            if (string.Equals(path, peerPath, StringComparison.OrdinalIgnoreCase)) throw new IOException("a query still maps it");
            File.Delete(path);
        };
        Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(peerPath));
        await _engine.DeleteSegmentAsync(new SegmentKey(new NodeId(7), new SegmentId(902)));

        Assert.Equal(10, _engine.PendingSegmentDeleteCount);   // 11: the set grew past its cap
        Assert.Contains(_log.Entries, e => e.Message.Contains("is not retried: the pending-delete set is full", StringComparison.Ordinal));
    }

    /// <summary>
    /// No merge and no pass runs before the boot catalog scan has finished, whoever calls. The
    /// scan does not take the merge gate, and it does what the gate keeps a pass from doing
    /// beside a live merge: the same recovery sweep, and registering every <c>*.seg</c> it lists
    /// — a merge output between its move and its swap among them. Only the maintenance loop's own
    /// wait for the load kept them apart. With the scan held: a merge and a pass are Busy, and
    /// the bool merge waits for the load and then merges.
    /// </summary>
    [Fact]
    public async Task BeforeTheBootScanHasFinished_AMergeOrAPassIsBusy()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var _ = Seam.ReleasedOnExit(release);   // a red assertion below must not strand the scan
        await _engine.DisposeAsync();
        _engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = _dir }),
            new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
            _log, Timeout.InfiniteTimeSpan, bootScanHeldUntil: release.Task)
        {
            _allowIndexlessMerge = true,
        };
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);   // a flush publishes whether or not the scan has run
        Assert.False(_engine.CatalogLoaded.IsCompleted, "setup: the boot scan was not held");

        Assert.Equal(MergeOutcome.Busy, await _engine.MergeSmallSegmentsOnceAsync(CancellationToken.None));
        Assert.Equal(MergeOutcome.Busy, await _engine.RunColdMaintenancePassAsync(CancellationToken.None));
        var merged = _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None);
        Assert.False(merged.IsCompleted, "the bool merge answered before the boot scan had finished");
        Assert.Equal(10, _engine.ListSegments().Count);

        release.SetResult();
        Assert.True(await merged.WaitAsync(TimeSpan.FromSeconds(30)), "the bool merge did not merge once the scan had finished");
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

    // ── Recovery of a committed merge (#98) ───────────────────────────────────

    /// <summary>
    /// Killed halfway through the unlinks, and at the next start one surviving source cannot be
    /// unlinked: on Windows an antivirus scanner or a backup agent holds it, anywhere a volume can
    /// refuse. Recovery used to warn and move on, and the catalog scan right behind it registered the
    /// source beside the output: its events counted twice, until a later pass unlinked the file out
    /// from under the entry. Now it is parked, as a merge's commit parks a source it cannot unlink:
    /// the scan skips it, the delete retry removes it once it is let go, and the manifest stays until
    /// it has. A second start while it is still held is the first start again. The refusal is the
    /// engine's unlink seam, so this runs alike on Windows and on Linux, where an open file unlinks.
    /// </summary>
    [Fact]
    public async Task ASourceThatCannotBeDeletedAtStart_IsParked_NotServedBesideTheOutput()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;

        // Five sources still on disk, the manifest naming all ten.
        var survivors = snap.Keys.Order(StringComparer.Ordinal).Take(5).ToList();
        Restore(snap, survivors);
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);
        var held = Path.Combine(SegDir, survivors[0]);

        for (int start = 1; start <= 2; start++)
        {
            await RestartWithSeamsAsync(e => e._deleteSegmentFile = UnlinkRefusing(held));

            Assert.Equal(1, _engine.PendingSegmentDeleteCount);   // 0: nothing parked it
            Assert.Equal(output, Assert.Single(_engine.ListSegments()).FilePath);   // 2: served beside the output
            AssertSameEvents(before, ReadEverything());
            Assert.True(File.Exists(held), $"start {start}: the held source was unlinked");
            foreach (var name in survivors.Skip(1))
                Assert.False(File.Exists(Path.Combine(SegDir, name)), $"start {start}: {name} survived recovery");
            Assert.Single(Manifests());   // kept while a source it names is on disk
        }

        // Let go: the parked delete goes through, and the next pass drops the manifest.
        _engine._deleteSegmentFile = File.Delete;
        Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
        Assert.False(File.Exists(held), "the parked source outlived its release");
        Assert.Equal(MergeOutcome.NothingToMerge, await _engine.RunColdMaintenancePassAsync(CancellationToken.None));
        Assert.Empty(Manifests());
        Assert.Equal(output, Assert.Single(_engine.ListSegments()).FilePath);
        AssertSameEvents(before, ReadEverything());
    }

    /// <summary>
    /// The same with a real holder, on the platform that has one: a handle opened without
    /// FileShare.Delete, as a scanner or a backup agent takes it, fails the unlink on Windows. Red
    /// before #98 with the double count itself, the held source served beside the output. Linux
    /// unlinks an open file, so there the seam above is the only way to this state.
    /// </summary>
    [WindowsFact]
    public async Task ASourceHeldOpenAcrossARestart_IsNotServedBesideTheOutput()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;

        // Killed after the move, before any unlink.
        Restore(snap, snap.Keys);
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);
        var held = Path.Combine(SegDir, snap.Keys.Order(StringComparer.Ordinal).First());

        using (new FileStream(held, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            await RestartAsync();

            Assert.Equal(output, Assert.Single(_engine.ListSegments()).FilePath);   // 2: the held source too
            AssertSameEvents(before, ReadEverything());
            Assert.True(File.Exists(held), "setup: the handle did not stop the unlink");
            Assert.Equal(1, _engine.PendingSegmentDeleteCount);
            Assert.Single(Manifests());
        }

        Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
        Assert.False(File.Exists(held), "the parked source outlived its holder");
    }

    /// <summary>
    /// Past <see cref="StorageEngine.PendingSegmentDeleteCap"/> a source recovery cannot unlink is
    /// let go rather than parked, as a commit lets one go — and it is still not served: the path is
    /// recorded for the running catalog scan before the park goes. The manifest keeps every one of
    /// them for the next pass. Cap 2, five sources held.
    /// </summary>
    [Fact]
    public async Task PastTheCap_ASourceThatCannotBeDeletedAtStartIsStillNotServed()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;

        var held = snap.Keys.Order(StringComparer.Ordinal).Take(5).ToList();
        Restore(snap, held);
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);

        await RestartWithSeamsAsync(e =>
        {
            e.PendingSegmentDeleteCap = 2;
            e._deleteSegmentFile      = UnlinkRefusing(held.Select(n => Path.Combine(SegDir, n)).ToArray());
        });

        Assert.Equal(2, _engine.PendingSegmentDeleteCount);
        Assert.Equal(output, Assert.Single(_engine.ListSegments()).FilePath);   // the three let go are not served
        AssertSameEvents(before, ReadEverything());
        foreach (var name in held) Assert.True(File.Exists(Path.Combine(SegDir, name)), $"setup: {name} was unlinked");
        Assert.Single(Manifests());
    }

    /// <summary>
    /// Recovery's unlinks are recorded for a catalog scan running beside it, as a commit's and a
    /// retention delete's are, so a scan that has read a source does not register it once recovery has
    /// deleted it. Recovery used to unlink with a bare File.Delete outside the scan gate, and the scan
    /// put back an entry for a file that was gone: every count over its window partial, every merge
    /// that picked it failed. Only the order of start-up kept the two apart (#98). Here a maintenance
    /// pass's recovery runs while a scan run by hand holds a source it has read and closed; the
    /// manifest is written in that window, after the scan's own recovery, so the pass's is the one
    /// that acts.
    /// </summary>
    [Fact]
    public async Task ARecoverySweepBesideACatalogScan_LeavesItNoSourceToRegister()
    {
        await _engine.CatalogLoaded;   // a pass is Busy until the boot scan is done
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output     = Assert.Single(_engine.ListSegments()).FilePath;
        var victimName = snap.Keys.Order(StringComparer.Ordinal).First();
        var victim     = Path.Combine(SegDir, victimName);
        Restore(snap, [victimName]);   // left by a crash: on disk, named by no entry

        MergeOutcome? pass = null;
        // Nothing in this hook may throw: the scan's quarantine catch would swallow it.
        _engine._beforeScanRegistersSegment = file =>
        {
            if (pass is not null || !string.Equals(file, victim, StringComparison.OrdinalIgnoreCase)) return;
            File.WriteAllLines(output + ".mergemanifest", snap.Keys);
            pass = Task.Run(() => _engine.RunColdMaintenancePassAsync(CancellationToken.None)).GetAwaiter().GetResult();
        };
        try     { await Task.Run(_engine.LoadSegmentCatalog).WaitAsync(TimeSpan.FromSeconds(30)); }
        finally { _engine._beforeScanRegistersSegment = null; }

        Assert.Equal(MergeOutcome.NothingToMerge, pass);   // setup: the pass ran beside the scan
        Assert.False(File.Exists(victim), "setup: the pass's recovery left the source on disk");
        Assert.Equal(output, Assert.Single(_engine.ListSegments()).FilePath);   // 2: an entry for the deleted source
        AssertSameEvents(before, ReadEverything());
        Assert.Empty(Manifests());
        Assert.DoesNotContain(_log.Entries, e => e.Message.Contains("Quarantining", StringComparison.Ordinal));
    }

    /// <summary>
    /// An output at its final name is a COMMITTED merge only if it is a whole segment. The merge moves
    /// it there only after the writer's fsync and the event-count check, so no crash of this process
    /// leaves a torn one; a restored backup, or storage that lost a flushed write in a power cut, can.
    /// Read as committed, recovery deleted the sources — the batch's only readable copy — and the scan
    /// then quarantined the output: every event of the batch out of service. At start recovery reads
    /// the output as the scan will, and a torn one is a merge that never committed: the sources stay
    /// and are served, the manifest goes, and the scan quarantines the output as it quarantines every
    /// unreadable segment.
    /// </summary>
    [Fact]
    public async Task ATornOutputAtStart_IsAMergeThatNeverCommitted_ItsSourcesStayServed()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;

        Restore(snap, snap.Keys);
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);
        await _engine.DisposeAsync();   // nothing of the engine's holds the output while it is torn
        var bytes = await File.ReadAllBytesAsync(output);
        await File.WriteAllBytesAsync(output, bytes[..(bytes.Length / 2)]);   // the tail never reached the disk

        await RestartAsync();

        Assert.Equal(10, _engine.ListSegments().Count);   // 0: the sources deleted against an output nobody can read
        AssertSameEvents(before, ReadEverything());
        Assert.Empty(Manifests());
        Assert.False(File.Exists(output));
        Assert.True(File.Exists(output + ".corrupt"), "the torn output was not quarantined");
    }

    /// <summary>
    /// The start's proof of an output is its content, not its framing. With the page holding the
    /// block index's count lost (it reads 0), the output still opens as a segment, one with no
    /// blocks; taken for a whole output, all ten sources were unlinked and the batch served 0 of
    /// 600 events. Every row is decoded now and counted against the header.
    /// </summary>
    [Fact]
    public async Task AnOutputWhoseBlockIndexPageWasLost_IsNotTakenAsCommitted()
    {
        for (int round = 0; round < 10; round++) await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        var output = Assert.Single(_engine.ListSegments()).FilePath;
        Restore(snap, snap.Keys);
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);
        await _engine.DisposeAsync();
        var bytes = await File.ReadAllBytesAsync(output);
        long blockIndexOffset = BitConverter.ToInt64(bytes, bytes.Length - 44 + 24);   // footer slot 3
        Array.Clear(bytes, (int)blockIndexOffset, 4);                                  // the count reads 0
        await File.WriteAllBytesAsync(output, bytes);
        await RestartAsync();
        AssertSameEvents(before, ReadEverything());

        foreach (var name in snap.Keys) Assert.True(File.Exists(Path.Combine(SegDir, name)), $"{name} was unlinked");
        Assert.Empty(Manifests());
        // Quarantined by recovery: the scan opens a block index that reads empty, and registered the
        // output beside the sources it replaced — 11 segments, 1 200 events by their headers.
        Assert.Equal(10, _engine.ListSegments().Count);
        Assert.True(File.Exists(output + ".corrupt"), "the torn output was not quarantined");
        Assert.False(File.Exists(output));
    }

    /// <summary>
    /// The same proof for damage the open cannot see: the first block's payload lost, every frame
    /// and the footer whole. Taken for committed on its open alone, all ten sources were unlinked
    /// against a file that cannot serve its first block; and left to the catalog scan, which opens
    /// it, it was registered beside the sources it had replaced.
    /// </summary>
    [Fact]
    public async Task AnOutputWhoseBlockPayloadWasLost_IsNotTakenAsCommitted()
    {
        for (int round = 0; round < 10; round++) await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;
        Restore(snap, snap.Keys);
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);
        await _engine.DisposeAsync();
        await LoseTheFirstBlockPayloadAsync(output);

        await RestartAsync();

        foreach (var name in snap.Keys) Assert.True(File.Exists(Path.Combine(SegDir, name)), $"{name} was unlinked");
        Assert.Equal(10, _engine.ListSegments().Count);   // 11: the torn output registered beside its sources
        AssertSameEvents(before, ReadEverything());
        Assert.True(File.Exists(output + ".corrupt"), "the torn output was not quarantined");
        Assert.Empty(Manifests());
    }

    /// <summary>
    /// A torn output that cannot be moved aside at the start — on Windows a scanner or a backup
    /// agent holds it without FileShare.Delete — is still kept out of the catalog, recorded for the
    /// scan as a delete is, and its manifest stays, emptied: the torn verdict, kept. The next pass
    /// takes the verdict from it rather than read the output again, and moves the output aside once
    /// it is let go. Windows only: elsewhere an open file moves.
    /// </summary>
    [WindowsFact]
    public async Task ATornOutputHeldAtStart_StaysOutOfService_AndIsQuarantinedByTheNextPass()
    {
        for (int round = 0; round < 10; round++) await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;
        Restore(snap, snap.Keys);
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);
        await _engine.DisposeAsync();
        await LoseTheFirstBlockPayloadAsync(output);   // a tear the scan's open does not see

        using (new FileStream(output, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            await RestartAsync();

            Assert.Equal(10, _engine.ListSegments().Count);   // 11: the torn output registered beside its sources
            AssertSameEvents(before, ReadEverything());
            Assert.True(File.Exists(output), "setup: the holder did not stop the move");
            Assert.Single(Manifests());

            // Still held: the pass keeps the verdict and only retries the move, and its merge takes
            // the ten sources, which are in service like any others.
            await _engine.RunColdMaintenancePassAsync(CancellationToken.None);
            Assert.DoesNotContain(_engine.ListSegments(), s => s.FilePath == output);
            Assert.True(File.Exists(output));
            Assert.Single(Manifests());
            AssertSameEvents(before, ReadEverything());
        }

        await _engine.RunColdMaintenancePassAsync(CancellationToken.None);
        Assert.True(File.Exists(output + ".corrupt"), "the next pass did not quarantine the output");
        Assert.False(File.Exists(output));
        Assert.Empty(Manifests());
        AssertSameEvents(before, ReadEverything());
    }

    /// <summary>
    /// A torn output is kept out of the start's scan before anything that can fail, reading its
    /// manifest included. With the manifest held without sharing at the start, the sweep fails on
    /// it, and the scan used to register the torn output (which it can frame) beside its ten
    /// sources. The next pass, the manifest let go, quarantines the output. Windows only.
    /// </summary>
    [WindowsFact]
    public async Task ATornOutputWhoseManifestCannotBeReadAtStart_IsStillNotServed()
    {
        for (int round = 0; round < 10; round++) await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;
        Restore(snap, snap.Keys);
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);
        await _engine.DisposeAsync();
        await LoseTheFirstBlockPayloadAsync(output);   // a tear the scan's open does not see

        using (new FileStream(output + ".mergemanifest", FileMode.Open, FileAccess.Read, FileShare.None))
        {
            await RestartAsync();
            Assert.Equal(10, _engine.ListSegments().Count);   // 11: the torn output registered beside its sources
            AssertSameEvents(before, ReadEverything());
        }

        await _engine.RunColdMaintenancePassAsync(CancellationToken.None);
        Assert.True(File.Exists(output + ".corrupt"), "the next pass did not quarantine the output");
        Assert.Empty(Manifests());
        AssertSameEvents(before, ReadEverything());
    }

    // ── An output nobody could read is never taken as committed (#120 round 3) ──

    /// <summary>
    /// A torn verdict, once reached, holds. The start found the output torn but could not move it
    /// aside (held without FileShare.Delete), so it left the manifest empty: the marker that keeps
    /// the output out of service. A pass that then could not read the output at all (held without
    /// sharing) took it for committed. The empty manifest went, and the next start served the torn
    /// output beside the segment its sources had been merged into: 1 200 events for 600.
    /// Windows only: elsewhere an open file moves and reads.
    /// </summary>
    [WindowsFact]
    public async Task ATornVerdict_IsNotUndoneByALaterReadFailure()
    {
        for (int round = 0; round < 10; round++) await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;
        Restore(snap, snap.Keys);
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);
        await _engine.DisposeAsync();
        await LoseTheFirstBlockPayloadAsync(output);

        using (new FileStream(output, FileMode.Open, FileAccess.Read, FileShare.Read))   // readable, not movable
            await RestartAsync();
        Assert.Equal(0, new FileInfo(output + ".mergemanifest").Length);   // setup: torn, kept out, the manifest a marker

        using (new FileStream(output, FileMode.Open, FileAccess.Read, FileShare.None))   // now not even readable
        {
            await _engine.RunColdMaintenancePassAsync(CancellationToken.None);
            // Not read again, so not left waiting either: the verdict stands, nothing holds the
            // sources back, and the pass merges them like any others.
            Assert.Single(_engine.ListSegments());
            Assert.Equal(0, new FileInfo(output + ".mergemanifest").Length);
        }

        await RestartAsync();
        Assert.Equal(600, _engine.ListSegments().Sum(s => (long)s.EventCount));   // 1 200: the torn output served beside its sources' merge
        Assert.True(File.Exists(output + ".corrupt"), "the torn output was not quarantined");
        AssertSameEvents(before, ReadEverything());
    }

    /// <summary>
    /// An output a pass cannot read is not taken as committed. The start's sweep proved this one torn
    /// but could not read its manifest (held without sharing), so it kept the output out of service
    /// and its ten sources in. The next pass, unable to open the output, took it for committed and
    /// unlinked every source: 0 of 600 events served. Windows only.
    /// </summary>
    [WindowsFact]
    public async Task AnOutputAPassCannotRead_IsNotTakenAsCommitted()
    {
        for (int round = 0; round < 10; round++) await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;
        Restore(snap, snap.Keys);
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);
        await _engine.DisposeAsync();
        await LoseTheFirstBlockPayloadAsync(output);

        using (new FileStream(output + ".mergemanifest", FileMode.Open, FileAccess.Read, FileShare.None))
            await RestartAsync();
        Assert.Equal(10, _engine.ListSegments().Count);   // setup: the sources in service, the torn output kept out

        using (new FileStream(output, FileMode.Open, FileAccess.Read, FileShare.None))
        {
            await _engine.RunColdMaintenancePassAsync(CancellationToken.None);
            AssertSameEvents(before, ReadEverything());   // 0 of 600: the sources unlinked against an output nobody read
            Assert.Single(Manifests());
        }

        await _engine.RunColdMaintenancePassAsync(CancellationToken.None);   // readable now: torn
        Assert.True(File.Exists(output + ".corrupt"), "the torn output was not quarantined");
        Assert.Empty(Manifests());
        AssertSameEvents(before, ReadEverything());
    }

    /// <summary>
    /// Nor does the start's sweep take an output it cannot read for committed. It used to, on the
    /// ground that a rename is proof of a whole file. The proof exists because a restored backup or
    /// a lost write can tear the file after the rename, and one that could not be read could be
    /// that one: every source unlinked, 0 of 600 served, and the output torn. The verdict waits for
    /// a sweep that can read it. Windows only.
    /// </summary>
    [WindowsFact]
    public async Task AnOutputTheStartCannotRead_IsNotTakenAsCommitted()
    {
        for (int round = 0; round < 10; round++) await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;
        Restore(snap, snap.Keys);
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);
        await _engine.DisposeAsync();
        await LoseTheFirstBlockPayloadAsync(output);

        using (new FileStream(output, FileMode.Open, FileAccess.Read, FileShare.None))
        {
            await RestartAsync();
            AssertSameEvents(before, ReadEverything());   // 0 of 600: every source unlinked against an output nobody read
            Assert.Single(Manifests());
            // Kept out by recovery, recorded for the scan: not a file the scan failed to reach, which
            // would leave the store Degraded — its alert rules unevaluated — for the whole run.
            Assert.Equal(QueryAvailability.Available, _engine.Availability);
        }

        await _engine.RunColdMaintenancePassAsync(CancellationToken.None);   // readable now: torn
        Assert.True(File.Exists(output + ".corrupt"), "the torn output was not quarantined");
        Assert.Empty(Manifests());
        AssertSameEvents(before, ReadEverything());
    }

    /// <summary>
    /// Waiting costs a pass, not the run. An output the start could not read, of a merge killed
    /// halfway through its unlinks, stays out of service while the five sources still on disk serve
    /// their events. The first pass that reads it whole commits it as a merge does: the output goes
    /// in and those five come out in one catalog generation, so every event is served once from
    /// then on. Windows only.
    /// </summary>
    [WindowsFact]
    public async Task AnOutputTheStartCouldNotRead_IsCommittedByThePassThatReadsIt()
    {
        for (int round = 0; round < 10; round++) await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;
        var half   = snap.Keys.Order(StringComparer.Ordinal).Take(5).ToList();
        Restore(snap, half);
        var held   = half.SelectMany(n => ReadRaw(Path.Combine(SegDir, n))).OrderBy(e => e.Id).ToList();
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);

        using (new FileStream(output, FileMode.Open, FileAccess.Read, FileShare.None))
        {
            await RestartAsync();
            Assert.Equal(5, _engine.ListSegments().Count);   // the five on disk in service, the output kept out
            AssertSameEvents(held, ReadEverything());
            Assert.Single(Manifests());
        }

        await _engine.RunColdMaintenancePassAsync(CancellationToken.None);
        Assert.Equal(output, Assert.Single(_engine.ListSegments()).FilePath);   // the output in, its sources out
        AssertSameEvents(before, ReadEverything());
        Assert.Empty(Manifests());
        foreach (var name in half) Assert.False(File.Exists(Path.Combine(SegDir, name)), $"{name} survived the commit");

        await RestartAsync();
        AssertSameEvents(before, ReadEverything());
    }

    /// <summary>
    /// While an output's verdict waits, the planner leaves the output's time range alone. A source
    /// merged meanwhile would put its events in a new output, and the waiting output, committed
    /// later, would serve them a second time. Windows only.
    /// </summary>
    [WindowsFact]
    public async Task WhileAnOutputsVerdictWaits_ItsSourcesAreNotMerged()
    {
        for (int round = 0; round < 10; round++) await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;
        Restore(snap, snap.Keys);                                   // killed after the move, before any unlink
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);

        using (new FileStream(output, FileMode.Open, FileAccess.Read, FileShare.None))
        {
            await RestartAsync();
            Assert.Equal(10, _engine.ListSegments().Count);         // setup: the sources serve, the output waits
            Assert.False(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None),
                         "a source of the waiting output was merged");
        }

        await _engine.RunColdMaintenancePassAsync(CancellationToken.None);
        Assert.Equal(output, Assert.Single(_engine.ListSegments()).FilePath);
        AssertSameEvents(before, ReadEverything());
    }

    // ── Every listed source the catalog names is taken out, a replica too (#120 round 4) ──

    /// <summary>
    /// A merge whose sources include a replica (<c>{node}-{id}.seg</c>, a merge candidate like any
    /// other), killed after the move. The start could not read the output, so it waited, and the
    /// scan registered every source, the replica too. The pass that read the output whole committed
    /// it but took only the sources with local names out: the replica stayed in service as if its
    /// peer had pushed it again, and the manifest went. Its events were served twice, 660 for 600,
    /// for good. Windows only.
    /// </summary>
    [WindowsFact]
    public async Task AReplicaSourceTheWaitKeptInService_IsTakenOutByTheCommit()
    {
        await _engine.CatalogLoaded;
        for (int round = 0; round < 9; round++) await WriteSegmentAsync(round, 60);
        var replica = WritePeerSegment(900, 9, 60);
        Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(replica));
        var before = ReadEverything();
        var snap   = SnapshotSources();
        Assert.Equal(10, snap.Count);                                     // setup: nine local and the replica
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;
        Assert.False(File.Exists(replica), "setup: the replica was not a source");
        await _engine.DisposeAsync();
        Restore(snap, snap.Keys);                                        // killed after the move, before any unlink
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);

        using (new FileStream(output, FileMode.Open, FileAccess.Read, FileShare.None))
            await RestartAsync();
        Assert.Equal(10, _engine.ListSegments().Count);                  // setup: the output waits, all ten sources serve

        await _engine.RunColdMaintenancePassAsync(CancellationToken.None);   // reads the output whole: the commit
        Assert.Equal(output, Assert.Single(_engine.ListSegments()).FilePath);   // 2: the replica beside the output
        AssertSameEvents(before, ReadEverything());
        Assert.False(File.Exists(replica), "the replica source survived the commit");

        await RestartAsync();
        AssertSameEvents(before, ReadEverything());
    }

    /// <summary>
    /// The same replica road through round 2's take-out: the start's sweep could not read the
    /// manifest, so the scan registered the output and all ten sources, and the next pass took out
    /// the nine local names but kept the replica as "pushed again": 660 for 600, for good.
    /// No seam, any OS.
    /// </summary>
    [Fact]
    public async Task AReplicaSourceTheScanRegisteredBesideItsOutput_IsTakenOutByTheNextPass()
    {
        await _engine.CatalogLoaded;
        for (int round = 0; round < 9; round++) await WriteSegmentAsync(round, 60);
        var replica = WritePeerSegment(900, 9, 60);
        Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(replica));
        var before = ReadEverything();
        var snap   = SnapshotSources();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;
        Assert.False(File.Exists(replica), "setup: the replica was not a source");
        Restore(snap, snap.Keys);
        await RestartAsync();                                             // no manifest yet: the scan registers all 11
        Assert.Equal(11, _engine.ListSegments().Count);                   // setup
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);

        await _engine.RunColdMaintenancePassAsync(CancellationToken.None);

        Assert.Equal(output, Assert.Single(_engine.ListSegments()).FilePath);   // 2: the replica beside the output
        AssertSameEvents(before, ReadEverything());
        Assert.Empty(Manifests());
    }

    /// <summary>
    /// A replica its peer pushes AGAIN while the manifest of the merge that took it still lives (another
    /// source held): the same segment, whose events the output holds, so it is taken out like any
    /// source the manifest lists. It used to be left in service as "in service again" and the double
    /// count kept for good; the one segment that would be wrongly taken out is a DIFFERENT one pushed
    /// under the same node id and segment id, a duplicate NodeId — the deployment error no key can
    /// resolve.
    /// </summary>
    [Fact]
    public async Task AReplicaPushedAgainWhileItsManifestLives_IsTakenOut()
    {
        await _engine.CatalogLoaded;   // a pass is Busy until the boot scan is done
        for (int round = 0; round < 9; round++) await WriteSegmentAsync(round, 60);
        var held    = _engine.ListSegments().Select(s => s.FilePath).Order(StringComparer.Ordinal).First();
        var replica = WritePeerSegment(904, 9, 60);
        Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(replica));
        var replicaBytes = await File.ReadAllBytesAsync(replica);
        _engine._deleteSegmentFile = UnlinkRefusing(held);              // keeps the manifest alive

        var before = ReadEverything();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        Assert.False(File.Exists(replica), "setup: the replica was not a source");
        Assert.Single(Manifests());

        var staged = replica + ".push";                                  // the peer pushes it again
        await File.WriteAllBytesAsync(staged, replicaBytes);
        Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(staged, replica));
        Assert.Contains(_engine.ListSegments(), s => s.FilePath == replica);   // setup: back in service, beside the output

        await _engine.RunColdMaintenancePassAsync(CancellationToken.None);
        Assert.DoesNotContain(_engine.ListSegments(), s => s.FilePath == replica);
        AssertSameEvents(before, ReadEverything());
    }

    /// <summary>
    /// An output in a segment format newer than this release reads, every source still on disk:
    /// a version field a bit flip pushed past the newest (v263 = 7 + 256), or a rollback's merge
    /// killed before its unlinks. Taken as committed, every source was unlinked against a file
    /// this release cannot read: 0 of 600. The merge is rolled back instead: the sources stay and
    /// the output goes aside.
    /// </summary>
    [Fact]
    public async Task AnOutputInANewerFormat_WithEverySourceOnDisk_IsRolledBack()
    {
        for (int round = 0; round < 10; round++) await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;
        Restore(snap, snap.Keys);
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);
        await _engine.DisposeAsync();
        var bytes = await File.ReadAllBytesAsync(output);
        bytes[5] ^= 0x01;                                    // the version's high byte: v7 reads as v263
        await File.WriteAllBytesAsync(output, bytes);

        await RestartAsync();

        AssertSameEvents(before, ReadEverything());          // 0: every source unlinked
        Assert.Equal(10, _engine.ListSegments().Count);
        Assert.True(File.Exists(output + ".corrupt"), "the output was not set aside");
        Assert.Empty(Manifests());
    }

    /// <summary>
    /// With a source already unlinked, the merge did commit, and that source's events live only in
    /// the newer-format output. So the output is kept as it is, out of service, for a release that
    /// reads it; the sources still on disk serve their events meanwhile. It is not set aside (#119
    /// keeps a newer-format segment under its name), and not taken as committed either.
    /// </summary>
    [Fact]
    public async Task AnOutputInANewerFormat_WithASourceGone_IsKeptForAReleaseThatReadsIt()
    {
        for (int round = 0; round < 10; round++) await WriteSegmentAsync(round, 60);
        var snap = SnapshotSources();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;
        var half   = snap.Keys.Order(StringComparer.Ordinal).Take(5).ToList();
        Restore(snap, half);
        var expected = half.SelectMany(n => ReadRaw(Path.Combine(SegDir, n))).OrderBy(e => e.Id).ToList();
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);
        await _engine.DisposeAsync();
        var bytes = await File.ReadAllBytesAsync(output);
        bytes[5] ^= 0x01;
        await File.WriteAllBytesAsync(output, bytes);

        await RestartAsync();

        AssertSameEvents(expected, ReadEverything());
        Assert.True(File.Exists(output), "the newer-format output was set aside");
        Assert.False(File.Exists(output + ".corrupt"));
        Assert.Single(Manifests());

        await _engine.RunColdMaintenancePassAsync(CancellationToken.None);   // still waiting, still kept
        Assert.True(File.Exists(output));
        Assert.Single(Manifests());
        AssertSameEvents(expected, ReadEverything());
    }

    /// <summary>
    /// A torn output found after some of its sources were already unlinked: the merge committed,
    /// the crash came in the middle of the unlinks, and the output was damaged afterwards. The
    /// sources still on disk stay in service; the events of the missing ones exist only in the
    /// quarantined file. That is said at Error with how many, and the manifest is kept aside as
    /// {output}.corrupt.sources, out of every sweep's way, as the record of which served sources a
    /// salvage of the .corrupt file would duplicate. It used to be logged as "the sources it lists
    /// stay in service" and the manifest deleted.
    /// </summary>
    [Fact]
    public async Task ATornOutputWithSourcesAlreadyGone_SaysSo_AndKeepsTheManifestAside()
    {
        for (int round = 0; round < 10; round++) await WriteSegmentAsync(round, 60);
        var snap = SnapshotSources();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;

        var half = snap.Keys.Order(StringComparer.Ordinal).Take(5).ToList();   // killed after five of ten unlinks
        Restore(snap, half);
        var expected = half.SelectMany(n => ReadRaw(Path.Combine(SegDir, n))).OrderBy(e => e.Id).ToList();
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);
        await _engine.DisposeAsync();
        var bytes = await File.ReadAllBytesAsync(output);
        await File.WriteAllBytesAsync(output, bytes[..(bytes.Length / 2)]);   // then the output lost its tail

        await RestartAsync();

        Assert.Equal(5, _engine.ListSegments().Count);
        AssertSameEvents(expected, ReadEverything());
        Assert.True(File.Exists(output + ".corrupt"), "the torn output was not quarantined");
        Assert.Empty(Manifests());
        var setAside = output + ".corrupt.sources";
        Assert.True(File.Exists(setAside), "the manifest was not kept aside");
        Assert.Equal(snap.Keys.Order(StringComparer.Ordinal), File.ReadAllLines(setAside).Order(StringComparer.Ordinal));
        Assert.Single(_log.Entries, e => e.Message.Contains("5 of 10 sources", StringComparison.Ordinal));
    }

    /// <summary>
    /// The same, with the torn output held at the start so it cannot be moved aside: the missing
    /// sources are counted and said once, at the first verdict. The manifest then only keeps the
    /// output out of service until a pass can move it, and lists nothing more — the sources it
    /// listed are in service, a pass may merge them, and counted again they would be called
    /// "already deleted" too. Windows only: elsewhere an open file moves.
    /// </summary>
    [WindowsFact]
    public async Task ATornOutputHeldWithSourcesGone_IsReportedOnce_AcrossThePassesThatWaitForIt()
    {
        for (int round = 0; round < 10; round++) await WriteSegmentAsync(round, 60);
        var snap = SnapshotSources();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;

        var half = snap.Keys.Order(StringComparer.Ordinal).Take(5).ToList();
        Restore(snap, half);
        var expected = half.SelectMany(n => ReadRaw(Path.Combine(SegDir, n))).OrderBy(e => e.Id).ToList();
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);
        await _engine.DisposeAsync();
        var bytes = await File.ReadAllBytesAsync(output);
        await File.WriteAllBytesAsync(output, bytes[..(bytes.Length / 2)]);

        int Reports() => _log.Entries.Count(e => e.Message.Contains("were already deleted; their events exist only", StringComparison.Ordinal));
        using (new FileStream(output, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            await RestartAsync();
            Assert.Equal(1, Reports());
            Assert.True(File.Exists(output + ".corrupt.sources"), "the manifest was not kept aside");
            Assert.Equal(0, new FileInfo(output + ".mergemanifest").Length);   // a marker now, listing nothing
            AssertSameEvents(expected, ReadEverything());

            // A pass while it is still held: it merges the five sources, and says nothing more.
            await _engine.RunColdMaintenancePassAsync(CancellationToken.None);
            Assert.Equal(1, Reports());
        }

        await _engine.RunColdMaintenancePassAsync(CancellationToken.None);
        Assert.True(File.Exists(output + ".corrupt"), "the next pass did not quarantine the output");
        Assert.Empty(Manifests());
        Assert.Equal(1, Reports());
        AssertSameEvents(expected, ReadEverything());
    }

    /// <summary>
    /// Killed after the last unlink and before the manifest went: nothing is left to delete, so
    /// recovery drops the manifest and touches nothing else.
    /// </summary>
    [Fact]
    public async Task CrashAfterTheUnlinks_BeforeTheManifestGoes_LeavesTheOutputAlone()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var names  = _engine.ListSegments().Select(s => Path.GetFileName(s.FilePath)).ToList();

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;
        await File.WriteAllLinesAsync(output + ".mergemanifest", names);

        await RestartAsync();

        Assert.Empty(Manifests());
        Assert.Equal(output, Assert.Single(_engine.ListSegments()).FilePath);
        AssertSameEvents(before, ReadEverything());
        Assert.Equal(0, _engine.PendingSegmentDeleteCount);
    }

    /// <summary>
    /// Only a plain <c>*.seg</c> file name in a manifest names a source. A blank line, a path, a name
    /// no file can carry and the output's own name — what a torn or hand-edited manifest can hold —
    /// name nothing recovery may unlink. Parked, the blank line (the segments directory itself)
    /// failed its unlink on every retry and kept the manifest for good; and the output's own name
    /// was unlinked at start, before the scan could serve it, after the sources it had replaced.
    /// </summary>
    [Fact]
    public async Task ManifestLinesThatNameNoSource_AreIgnored_AndTheOutputIsNeverOneOfThem()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;

        Restore(snap, snap.Keys);
        await File.WriteAllLinesAsync(output + ".mergemanifest",
            [.. snap.Keys, "", Path.Combine("sub", "x.seg"), "torn\0name.seg", Path.GetFileName(output)]);

        await RestartAsync();

        Assert.True(File.Exists(output), "recovery unlinked the output its own manifest names");
        Assert.Equal(output, Assert.Single(_engine.ListSegments()).FilePath);
        AssertSameEvents(before, ReadEverything());
        foreach (var name in snap.Keys) Assert.False(File.Exists(Path.Combine(SegDir, name)), $"{name} survived recovery");
        Assert.Equal(0, _engine.PendingSegmentDeleteCount);
        Assert.Empty(Manifests());
    }

    /// <summary>
    /// A source with a name only this node writes, in the catalog beside its output, is taken out
    /// by the next pass's sweep as a commit takes out its sources. It got there because the start's
    /// sweep missed the manifest (it could not read it) and the scan registered all eleven files.
    /// The sweep used to leave it in service as if a peer had pushed it again and drop the
    /// manifest: 1 200 events for 600, for good, and the pass's merge copied the duplicates into a
    /// new output. Seam-free, so it runs alike on every platform.
    /// </summary>
    [Fact]
    public async Task ASourceTheScanRegisteredBesideItsOutput_IsTakenOutByTheNextPass()
    {
        for (int round = 0; round < 10; round++) await WriteSegmentAsync(round, 60);
        var before = ReadEverything();
        var snap   = SnapshotSources();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the merge merged nothing");
        var output = Assert.Single(_engine.ListSegments()).FilePath;
        Restore(snap, snap.Keys);
        await RestartAsync();                                             // no manifest yet: the scan registers all 11 (a boot-sweep miss)
        Assert.Equal(11, _engine.ListSegments().Count);                   // setup
        await File.WriteAllLinesAsync(output + ".mergemanifest", snap.Keys);   // the manifest the boot sweep could not read
        await _engine.RunColdMaintenancePassAsync(CancellationToken.None);
        AssertSameEvents(before, ReadEverything());

        Assert.Equal(output, Assert.Single(_engine.ListSegments()).FilePath);
        Assert.Empty(Manifests());
        foreach (var name in snap.Keys) Assert.False(File.Exists(Path.Combine(SegDir, name)), $"{name} survived the pass");
    }

    /// <summary>
    /// The output of a merge whose manifest waits for a held source is not merged again. Recovery
    /// decides a manifest on its output alone, so an output the planner merged a second time read
    /// as a merge that never committed: the next pass dropped its manifest while the held source
    /// was still on disk, and the next start served that source beside the new output — 540 events
    /// for 480. Sealed bucket, so the planner takes pairs.
    /// </summary>
    [Fact]
    public async Task AnOutputMergedAgainWhileASourceIsHeld_KeepsTheEventsOnceAcrossARestart()
    {
        await _engine.CatalogLoaded;
        long old = MergeBucketGrid.SealedBucketStart(LogLevel.Information);
        for (int round = 0; round < 4; round++)
            await WriteSegmentAsync(round, 60, baseTicks: old + round * TimeSpan.TicksPerHour);
        var held = _engine.ListSegments().Select(s => s.FilePath).Order(StringComparer.Ordinal).First();
        _engine._deleteSegmentFile = UnlinkRefusing(held);            // a scanner or backup agent holds one source

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: merge 1");
        Assert.Single(_engine.ListSegments());
        Assert.Single(Manifests());
        Assert.Equal(1, _engine.PendingSegmentDeleteCount);

        for (int round = 4; round < 8; round++)
            await WriteSegmentAsync(round, 60, baseTicks: old + round * TimeSpan.TicksPerHour);
        var before = ReadEverything();

        int merges = 0;                                               // the size ladder, to a fixpoint
        while (merges < 6 && await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None)) merges++;
        await _engine.RunColdMaintenancePassAsync(CancellationToken.None);

        await RestartAsync();                                         // still held at shutdown: the last retry is refused too
        AssertSameEvents(before, ReadEverything());
    }

    /// <summary>
    /// The same across a restart: the start's sweep keeps the manifest of a source still held and
    /// keeps its output out of the planner, as the merge did. Without that, the first merges after
    /// the start took the output, and with a second manifest surviving to a later start the two
    /// were resolved in whatever order the directory listed them — the one listing the other's
    /// output first, which unlinked it, and the other then read as a merge that never committed.
    /// </summary>
    [Fact]
    public async Task AfterARestart_AnOutputWhoseManifestWaitsForAHeldSource_IsStillNotMergedAgain()
    {
        await _engine.CatalogLoaded;
        long old = MergeBucketGrid.SealedBucketStart(LogLevel.Information);
        for (int round = 0; round < 4; round++)
            await WriteSegmentAsync(round, 60, baseTicks: old + round * TimeSpan.TicksPerHour);
        var held = _engine.ListSegments().Select(s => s.FilePath).Order(StringComparer.Ordinal).First();
        _engine._deleteSegmentFile = UnlinkRefusing(held);

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: merge 1");
        var output = Assert.Single(_engine.ListSegments()).FilePath;

        await RestartWithSeamsAsync(e => e._deleteSegmentFile = UnlinkRefusing(held));
        Assert.Single(Manifests());                                   // setup: kept, the source is still held

        for (int round = 4; round < 8; round++)
            await WriteSegmentAsync(round, 60, baseTicks: old + round * TimeSpan.TicksPerHour);
        var before = ReadEverything();

        int merges = 0;
        while (merges < 6 && await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None)) merges++;
        Assert.True(merges > 0, "setup: the new segments did not merge");
        Assert.Contains(_engine.ListSegments(), s => s.FilePath == output);   // merged again after the start
        await _engine.RunColdMaintenancePassAsync(CancellationToken.None);
        Assert.Single(Manifests());

        await RestartWithSeamsAsync(e => e._deleteSegmentFile = UnlinkRefusing(held));
        AssertSameEvents(before, ReadEverything());

        // Let go: the manifest goes, and with it the hold on the output.
        _engine._deleteSegmentFile = File.Delete;
        Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
        await _engine.RunColdMaintenancePassAsync(CancellationToken.None);
        Assert.Empty(Manifests());
        for (int i = 0; i < 6 && await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None); i++) { }
        Assert.DoesNotContain(_engine.ListSegments(), s => s.FilePath == output);   // a candidate again
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
