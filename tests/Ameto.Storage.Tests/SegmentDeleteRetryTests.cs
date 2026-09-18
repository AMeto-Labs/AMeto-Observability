using System.Buffers;
using System.Diagnostics;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// On Windows a segment file cannot be unlinked while a reader maps it, and a query keeps its
/// readers (prefilter readers included) open for its whole duration. <c>DeleteSegmentAsync</c>
/// removed the catalog entry, <c>File.Delete</c> failed on the open mapping, and the engine
/// logged a warning and never tried again. No entry named the file any more, so retention never
/// saw it again, and after a restart the catalog scan loaded the expired segment back and served
/// it until the next retention pass.
///
/// <para>The entry is still removed at once, so no new query picks the segment. The unlink is
/// now retried: by one background loop with backoff for twice the query timeout after each path
/// was parked, then from every maintenance and retention pass. A retry must never delete a path
/// the catalog names again, the boot catalog scan must never name a parked path again, a file
/// already gone is not parked at all, and the parked set is capped.</para>
///
/// <para>Windows-only behaviour: elsewhere the unlink succeeds with the file still mapped, so
/// there is nothing to retry and the tests that need a held file return early.</para>
/// </summary>
public sealed class SegmentDeleteRetryTests : IAsyncLifetime
{
    private static readonly NodeId Peer = new(7);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-segdelete-" + Guid.NewGuid().ToString("N"));
    private readonly CapturingLogger _log = new();
    private StorageEngine _engine = null!;

    private string SegDir => Path.Combine(_dir, "segments");

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = _dir }),
            new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
            _log)
        {
            // Out of the way by default, so a test drives the retry itself and the background
            // attempt cannot race its assertions. The tests of the background path shorten it.
            SegmentDeleteRetryInitialDelay = TimeSpan.FromHours(1),
        };
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        try { await _engine.DisposeAsync(); } catch { }
        try { Directory.Delete(_dir, true); } catch { }
    }

    private static byte[] Props(int i)
    {
        var buf = new ArrayBufferWriter<byte>(32);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write((long)i);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>A replicated segment, written and imported the way the replication endpoint does it.</summary>
    private (string Path, SegmentKey Key) ImportPeerSegment(ulong segId)
    {
        var (path, key) = WritePeerSegment(segId);
        Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(path));
        return (path, key);
    }

    /// <summary>A replicated segment's file in the segments directory, not registered: what a boot scan finds.</summary>
    private (string Path, SegmentKey Key) WritePeerSegment(ulong segId)
    {
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(16, 1L << 20);
        long now = DateTime.UtcNow.Ticks;
        for (int i = 0; i < 4; i++)
            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(Peer.Value, (uint)i).RawValue,
                TimestampUtcTicks        = now + i * TimeSpan.TicksPerMillisecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = pool.Intern("peer {n}"),
            }, Props(i), "peer {n}"));
        hot.Freeze();

        Directory.CreateDirectory(SegDir);
        string path = Path.Combine(SegDir, $"{Peer.Value}-{segId}.seg");
        using (var writer = new SegmentWriter(path))
        {
            writer.WriteEvents(hot, pool);
            writer.Finalise(Peer, new SegmentId(segId));
        }
        return (path, new SegmentKey(Peer, new SegmentId(segId)));
    }

    private bool InCatalog(SegmentKey key) => _engine.ListSegments().Any(s => SegmentKey.Of(s) == key);

    [Fact]
    public async Task A_delete_that_fails_on_an_open_reader_is_completed_once_the_reader_closes()
    {
        if (!OperatingSystem.IsWindows()) return;

        var (path, key) = ImportPeerSegment(11);

        var reader = SegmentReader.Open(path);   // a query mid-flight
        try
        {
            await _engine.DeleteSegmentAsync(key);

            Assert.False(InCatalog(key), "the entry must go at once, so no new query picks the segment");
            Assert.True(File.Exists(path), "setup: the open mapping should have blocked the unlink");
            Assert.Equal(1, _engine.PendingSegmentDeleteCount);

            // Still held: an attempt fails and keeps the path pending.
            Assert.Equal(1, _engine.RetryPendingSegmentDeletes());
            Assert.True(File.Exists(path));
        }
        finally { reader.Dispose(); }

        // The query finished; the next pass (maintenance or retention) completes the delete.
        Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
        Assert.False(File.Exists(path), "the expired segment's file outlived its reader");
    }

    [Fact]
    public async Task A_pending_delete_does_not_remove_a_path_the_catalog_names_again()
    {
        if (!OperatingSystem.IsWindows()) return;

        var (path, key) = ImportPeerSegment(12);

        using (SegmentReader.Open(path))
        {
            await _engine.DeleteSegmentAsync(key);
            Assert.True(File.Exists(path), "setup: the open mapping should have blocked the unlink");
            Assert.Equal(1, _engine.PendingSegmentDeleteCount);

            // The peer pushes the same segment again, to the same path, before the retry runs.
            Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(path));
        }

        Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
        Assert.True(File.Exists(path), "a stale retry deleted the file of a live catalog entry");
        Assert.True(InCatalog(key));
        Assert.Equal(0, _engine.PendingSegmentDeleteCount);
    }

    [Fact]
    public async Task A_re_import_landing_between_a_retry_s_catalog_check_and_its_unlink_keeps_its_file()
    {
        await _engine.CatalogLoaded;

        var (path, key) = ImportPeerSegment(15);

        // Parked by a failure the unlink seam stages, so this runs on every platform; on Windows a
        // query's open reader parks it the same way.
        _engine._deleteSegmentFile = static p => throw new IOException($"The process cannot access the file '{p}'.");
        await _engine.DeleteSegmentAsync(key);
        Assert.Equal(1, _engine.PendingSegmentDeleteCount);   // setup

        // The peer pushes the same segment again, staged beside the final path as the replication
        // endpoint stages it. The old file is still at the final path, so the import's move fails
        // and its incumbent branch registers that file: the entry then names the parked path.
        string staged = Path.Combine(SegDir, $"{Peer.Value}-15.0badf00d.seg.tmp");
        File.Copy(path, staged);

        using var go        = new ManualResetEventSlim();
        using var published = new ManualResetEventSlim();
        SegmentImportOutcome? outcome     = null;
        Exception?            importError = null;
        _engine._afterImportPublish = published.Set;

        // A thread already running and parked on the signal, not a pool task: when the retry's
        // lock is missing, the import must get through inside the half second below, and a pool
        // under a parallel test run can take longer than that just to start a task.
        var import = new Thread(() =>
        {
            go.Wait();
            try { outcome = _engine.ImportSegment(staged, path); }
            catch (Exception ex) { importError = ex; }
        }) { IsBackground = true };
        import.Start();

        // The reader has closed, and the retry has found no entry naming the path. Between that
        // check and the unlink, the import is let go and given half a second. With the retry
        // holding _importLock across both, the import cannot publish in that time: it runs once
        // the unlink is done, and its move then lands the file on a free path. Without the lock it
        // registers the old file here, and the unlink below removes the file its entry names.
        // (Nothing in this hook may throw: the retry's catch would take it for a failed unlink.)
        //
        // That half depends on the scheduler: under load the import can miss the half second, the
        // unlink runs first, and a retry without the lock passes. The half that does not is what
        // the lock is for, recorded from the retry's own thread: it holds _importLock at the
        // catalog read, still holds it at the unlink, and made exactly one read before that
        // unlink. Removing the lock, or narrowing it to either step, fails that on every run.
        int checks = 0, checkThread = -1;
        bool heldAtCheck = false, heldAtUnlink = false, unlinkFollowsItsCheck = false;
        _engine._beforePendingDeleteCatalogCheck = p =>
        {
            checks++;
            checkThread = Environment.CurrentManagedThreadId;
            heldAtCheck = _engine.ImportLockIsHeldByCurrentThread;
        };
        bool attempted = false, publishedInside = false;
        _engine._deleteSegmentFile = p =>
        {
            if (!attempted)
            {
                attempted = true;
                heldAtUnlink          = _engine.ImportLockIsHeldByCurrentThread;
                unlinkFollowsItsCheck = checks == 1 && checkThread == Environment.CurrentManagedThreadId;
                go.Set();
                publishedInside = published.Wait(TimeSpan.FromMilliseconds(500));
                if (publishedInside) import.Join(TimeSpan.FromSeconds(20));
            }
            File.Delete(p);
        };

        int left = _engine.RetryPendingSegmentDeletes();
        go.Set();
        Assert.True(import.Join(TimeSpan.FromSeconds(20)), "setup: the import never finished");

        Assert.Null(importError);
        Assert.True(attempted, "setup: the retry never reached its unlink");
        Assert.True(unlinkFollowsItsCheck, "setup: the unlink did not follow one catalog read on the same thread");
        Assert.True(heldAtCheck, "the retry read the catalog for the parked path without holding _importLock");
        Assert.True(heldAtUnlink, "the retry unlinked the parked path without holding _importLock");
        Assert.False(publishedInside, "the re-import published its entry between the retry's catalog check and its unlink");
        Assert.Equal(SegmentImportOutcome.Registered, outcome);
        Assert.Equal(0, left);
        Assert.True(InCatalog(key));
        Assert.True(File.Exists(path), "the retry unlinked the file of the entry a re-import had just registered");
        Assert.False(File.Exists(staged));
    }

    [Fact]
    public async Task The_background_retry_deletes_the_file_after_the_reader_closes()
    {
        if (!OperatingSystem.IsWindows()) return;

        _engine.SegmentDeleteRetryInitialDelay = TimeSpan.FromMilliseconds(20);
        var (path, key) = ImportPeerSegment(13);

        using (SegmentReader.Open(path))
        {
            await _engine.DeleteSegmentAsync(key);
            Assert.True(File.Exists(path), "setup: the open mapping should have blocked the unlink");
            await Task.Delay(150);   // a few attempts fail against the open reader
            Assert.True(File.Exists(path));
        }

        var sw = Stopwatch.StartNew();
        while (File.Exists(path) && sw.Elapsed < TimeSpan.FromSeconds(20))
            await Task.Delay(20);

        Assert.False(File.Exists(path), "no background attempt deleted the file after the reader closed");
        Assert.Equal(0, _engine.PendingSegmentDeleteCount);
    }

    [Fact]
    public async Task Dispose_does_not_wait_out_a_pending_retry()
    {
        if (!OperatingSystem.IsWindows()) return;

        // The production schedule: a 120 s retry window ahead of it.
        _engine.SegmentDeleteRetryInitialDelay = TimeSpan.FromMilliseconds(500);
        var (path, key) = ImportPeerSegment(14);

        using (SegmentReader.Open(path))
        {
            await _engine.DeleteSegmentAsync(key);
            Assert.Equal(1, _engine.PendingSegmentDeleteCount);

            var sw = Stopwatch.StartNew();
            await _engine.DisposeAsync();
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10), $"DisposeAsync took {sw.Elapsed}");
        }
    }

    // ── Already gone ──────────────────────────────────────────────────────────

    // These two never reach the engine's already-gone rule. File.Delete returns without throwing
    // for a missing file whose directory exists (the runtime swallows ERROR_FILE_NOT_FOUND and
    // ENOENT), so the engine's unlink simply succeeds. They pin that runtime behaviour, which the
    // engine relies on, and that neither the delete nor the retry parks such a path.

    [Fact]
    public async Task The_runtime_deletes_a_missing_file_silently_and_the_delete_does_not_park_it()
    {
        var (path, key) = ImportPeerSegment(21);

        // The file is gone before the delete (an operator), and its directory is still there.
        File.Delete(path);
        Assert.True(Directory.Exists(SegDir));
        Assert.Null(Record.Exception(() => File.Delete(path)));   // the runtime is silent about it

        await _engine.DeleteSegmentAsync(key);

        Assert.False(InCatalog(key));
        Assert.Equal(0, _engine.PendingSegmentDeleteCount);
    }

    [Fact]
    public async Task The_runtime_deletes_a_missing_file_silently_and_the_retry_settles_it()
    {
        if (!OperatingSystem.IsWindows()) return;

        var (path, key) = ImportPeerSegment(22);
        using (SegmentReader.Open(path))
        {
            await _engine.DeleteSegmentAsync(key);
            Assert.Equal(1, _engine.PendingSegmentDeleteCount);
        }

        File.Delete(path);
        Assert.True(Directory.Exists(SegDir));
        Assert.Null(Record.Exception(() => File.Delete(path)));   // the runtime is silent about it

        Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
    }

    [Fact]
    public async Task A_delete_whose_segments_directory_is_unreachable_is_parked_and_completed_once_it_is_back()
    {
        // The outage below is the Windows one: File.Delete under a missing directory throws
        // DirectoryNotFoundException (ERROR_PATH_NOT_FOUND).
        if (!OperatingSystem.IsWindows()) return;
        await _engine.CatalogLoaded;

        var (path, key) = ImportPeerSegment(23);

        // A volume outage, as a missing drive letter or a broken junction presents it: the
        // segments directory cannot be reached, and the file behind it still exists. Renaming
        // the directory away makes File.Delete throw exactly that.
        string away = SegDir + "-away";
        Directory.Move(SegDir, away);
        try
        {
            Assert.Throws<DirectoryNotFoundException>(() => File.Delete(path));   // setup

            await _engine.DeleteSegmentAsync(key);
            Assert.False(InCatalog(key), "the entry must go at once");
            Assert.Equal(1, _engine.PendingSegmentDeleteCount);   // parked, not taken for gone

            // Still unreachable: the retry keeps the path too.
            Assert.Equal(1, _engine.RetryPendingSegmentDeletes());
        }
        finally { Directory.Move(away, SegDir); }

        // The volume is back, and so is the file; the next pass deletes it.
        Assert.True(File.Exists(path), "setup: the file must have survived the outage");
        Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
        Assert.False(File.Exists(path), "the expired segment's file outlived the outage");
    }

    [Fact]
    public async Task A_delete_whose_directory_is_unreachable_while_its_link_still_exists_is_parked()
    {
        await _engine.CatalogLoaded;

        var (path, key) = ImportPeerSegment(24);

        // The segments directory is a junction or symlink whose target volume went offline. The
        // link still answers Directory.Exists, and File.Delete throws DirectoryNotFoundException
        // for the file behind it. Nothing on disk stages that on every platform, so the unlink
        // seam throws it, with the directory really there.
        _engine._deleteSegmentFile = static p => throw new DirectoryNotFoundException($"Could not find a part of the path '{p}'.");
        Assert.True(Directory.Exists(SegDir));   // setup: the parent "exists"

        await _engine.DeleteSegmentAsync(key);
        Assert.False(InCatalog(key), "the entry must go at once");
        Assert.Equal(1, _engine.PendingSegmentDeleteCount);   // parked, not taken for gone

        // Still unreachable: the retry keeps the path too.
        Assert.Equal(1, _engine.RetryPendingSegmentDeletes());

        // The volume is back, and so is the file; the next pass deletes it.
        _engine._deleteSegmentFile = File.Delete;
        Assert.True(File.Exists(path), "setup: the file must have survived the outage");
        Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
        Assert.False(File.Exists(path), "the expired segment's file outlived the outage");
    }

    // ── One loop, and a cap ───────────────────────────────────────────────────

    [Fact]
    public async Task One_background_loop_serves_every_parked_path()
    {
        if (!OperatingSystem.IsWindows()) return;

        _engine.SegmentDeleteRetryInitialDelay = TimeSpan.FromMilliseconds(20);
        var segments = Enumerable.Range(0, 5).Select(i => ImportPeerSegment(30ul + (ulong)i)).ToList();
        var readers  = segments.Select(s => SegmentReader.Open(s.Path)).ToList();

        Task? loop = null;
        try
        {
            foreach (var (_, key) in segments)
            {
                await _engine.DeleteSegmentAsync(key);
                loop ??= _engine.SegmentDeleteRetryLoop;
                Assert.Same(loop, _engine.SegmentDeleteRetryLoop);   // parking a path starts no task of its own
            }
            Assert.Equal(5, _engine.PendingSegmentDeleteCount);
            Assert.False(loop!.IsCompleted, "the loop stood down while every path was still held and inside its window");
        }
        finally { foreach (var r in readers) r.Dispose(); }

        // The same loop deletes all five once their readers are gone, then stands down.
        await loop.WaitAsync(TimeSpan.FromSeconds(20));
        Assert.All(segments, s => Assert.False(File.Exists(s.Path), $"{s.Path} outlived its reader"));
        Assert.Equal(0, _engine.PendingSegmentDeleteCount);
    }

    [Fact]
    public async Task Past_the_cap_a_failed_delete_is_not_parked_and_the_cap_is_warned_once_per_episode()
    {
        if (!OperatingSystem.IsWindows()) return;

        _engine.PendingSegmentDeleteCap = 2;

        // Four deletes fail at once — a volume that refuses them looks the same.
        var first   = Enumerable.Range(0, 4).Select(i => ImportPeerSegment(40ul + (ulong)i)).ToList();
        var readers = first.Select(s => SegmentReader.Open(s.Path)).ToList();
        try
        {
            foreach (var (_, key) in first) await _engine.DeleteSegmentAsync(key);

            Assert.Equal(2, _engine.PendingSegmentDeleteCount);
            Assert.All(first, s => Assert.False(InCatalog(s.Key), "the entry must go whether or not the path is parked"));
            Assert.Single(CapWarnings());
            Assert.Equal(2, _log.Entries.Count(e => e.Level == Microsoft.Extensions.Logging.LogLevel.Information &&
                                                    e.Message.Contains("not retried", StringComparison.Ordinal)));
        }
        finally { foreach (var r in readers) r.Dispose(); }

        // The two parked files are deleted; the two past the cap stay on disk for the next start.
        Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
        Assert.Equal(2, first.Count(s => File.Exists(s.Path)));

        // Drained, so a new episode is named again — once.
        var second = Enumerable.Range(0, 3).Select(i => ImportPeerSegment(50ul + (ulong)i)).ToList();
        readers    = second.Select(s => SegmentReader.Open(s.Path)).ToList();
        try
        {
            foreach (var (_, key) in second) await _engine.DeleteSegmentAsync(key);
            Assert.Equal(2, _engine.PendingSegmentDeleteCount);
            Assert.Equal(2, CapWarnings().Count());
        }
        finally { foreach (var r in readers) r.Dispose(); }
    }

    [Fact]
    public async Task After_the_window_a_path_stays_parked_and_a_later_pass_deletes_it()
    {
        if (!OperatingSystem.IsWindows()) return;

        _engine.SegmentDeleteRetryInitialDelay   = TimeSpan.FromMilliseconds(20);
        _engine.SegmentDeleteRetryWindowOverride = TimeSpan.FromMilliseconds(150);
        var (path, key) = ImportPeerSegment(60);

        var reader = SegmentReader.Open(path);
        try
        {
            await _engine.DeleteSegmentAsync(key);
            var loop = _engine.SegmentDeleteRetryLoop;

            // The loop gives up on the held path once the window is over, and stands down.
            await loop.WaitAsync(TimeSpan.FromSeconds(20));
            Assert.True(File.Exists(path));
            Assert.Equal(1, _engine.PendingSegmentDeleteCount);
            Assert.Contains(_log.Entries, e => e.Level == Microsoft.Extensions.Logging.LogLevel.Warning &&
                                               e.Message.Contains("no longer retrying in the background", StringComparison.Ordinal));
        }
        finally { reader.Dispose(); }

        // Nothing in the background tries again...
        await Task.Delay(300);
        Assert.True(File.Exists(path), "a background attempt ran after the loop gave the path up");
        Assert.Equal(1, _engine.PendingSegmentDeleteCount);

        // ...but the next maintenance or retention pass does, and the reader is gone now.
        Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
        Assert.False(File.Exists(path));
    }

    // ── The passes that retry ─────────────────────────────────────────────────

    // Past the background loop's window, a maintenance pass and a retention pass are the only
    // retries a parked file gets until shutdown, and every other test here calls the retry itself.
    // These two go through the passes, so a pass that stops calling it fails here rather than
    // leaking files on a stand. The path is parked through the unlink seam, so both run on every
    // platform, and the background loop is an hour away (see InitializeAsync).

    [Fact]
    public async Task A_maintenance_pass_retries_a_parked_delete()
    {
        await _engine.CatalogLoaded;
        var (path, key) = ParkThroughTheSeam(17);

        Assert.False(await _engine.RunColdMaintenancePassAsync(CancellationToken.None));   // setup: nothing to merge

        Assert.Equal(0, _engine.PendingSegmentDeleteCount);
        Assert.False(File.Exists(path), "a maintenance pass left a parked segment file on disk");
        Assert.False(InCatalog(key));
    }

    [Fact]
    public async Task A_retention_pass_retries_a_parked_delete()
    {
        await _engine.CatalogLoaded;
        var (path, key) = ParkThroughTheSeam(18);

        var result = await _engine.EnforceRetentionAsync();
        Assert.Equal(0, result.DeletedSegments);   // setup: nothing expired, so only the retry can delete it

        Assert.Equal(0, _engine.PendingSegmentDeleteCount);
        Assert.False(File.Exists(path), "a retention pass left a parked segment file on disk");
        Assert.False(InCatalog(key));
    }

    /// <summary>
    /// Imports a segment and deletes it with the unlink failing as an open reader makes it fail,
    /// then puts the real unlink back: parked, with nothing holding the file any more.
    /// </summary>
    private (string Path, SegmentKey Key) ParkThroughTheSeam(ulong segId)
    {
        var (path, key) = ImportPeerSegment(segId);
        _engine._deleteSegmentFile = static p => throw new IOException($"The process cannot access the file '{p}'.");
        Assert.True(_engine.DeleteSegmentAsync(key).IsCompletedSuccessfully);
        _engine._deleteSegmentFile = File.Delete;

        Assert.Equal(1, _engine.PendingSegmentDeleteCount);   // setup
        Assert.True(File.Exists(path));
        return (path, key);
    }

    // ── The boot catalog scan ─────────────────────────────────────────────────

    [Fact]
    public async Task The_catalog_scan_does_not_register_a_path_waiting_for_its_delete()
    {
        if (!OperatingSystem.IsWindows()) return;
        await _engine.CatalogLoaded;

        var (path, key) = ImportPeerSegment(70);
        using (SegmentReader.Open(path))
        {
            // Retention expires the segment while a query holds it...
            await _engine.DeleteSegmentAsync(key);
            Assert.Equal(1, _engine.PendingSegmentDeleteCount);

            // ...and the boot scan, still walking the directory, reaches the file.
            _engine.LoadSegmentCatalog();
            Assert.False(InCatalog(key), "the catalog scan put a segment back that retention had just deleted");
        }

        Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
        Assert.False(File.Exists(path), "the retry took the scan's registration for a re-import and kept the file");
        Assert.False(InCatalog(key));
    }

    [Fact]
    public async Task The_catalog_scan_cannot_act_on_a_path_between_its_entry_going_and_its_park()
    {
        if (!OperatingSystem.IsWindows()) return;
        await _engine.CatalogLoaded;

        var (path, key) = ImportPeerSegment(71);
        using var atFile   = new ManualResetEventSlim();
        using var released = new ManualResetEventSlim();
        using var scanned  = new ManualResetEventSlim();
        bool held = false, releasedInside = false;
        bool finishedInside = false, registeredInside = false, loggedInside = false;

        // The delete's step, under _importLock and _scanDeleteGate: remove the entry, record the
        // path for a running scan, unlink the file, park the failed unlink. The scan's check and
        // register take the same gate, so they land wholly before that step or wholly after it,
        // and here, with a reader holding the file, after it the path is parked.
        //
        // The scan starts BEFORE the delete and is held where it has read and closed the file:
        // past its entry lock, before the gate, holding neither. Started from inside the delete,
        // as this test once did, it blocks at that entry lock, which takes the gate too, and runs
        // only once the step is over: the test then passed with the scan's per-file gate removed.
        //
        // Nothing in this hook may throw: the scan's quarantine catch would swallow it.
        _engine._beforeScanRegistersSegment = file =>
        {
            if (held || !string.Equals(file, path, StringComparison.OrdinalIgnoreCase)) return;
            held = true;
            atFile.Set();
            released.Wait(TimeSpan.FromSeconds(20));
        };

        // Inside the delete, after the entry went and was recorded and before the unlink fails and
        // parks, the scan is released and given half a second. Whatever it does about the path in
        // that time, it did inside the step:
        // - finishing (its exit takes the gate): the delete does not hold the gate;
        // - registering: the scan's check did not wait for the gate, and nothing recorded the path;
        // - logging why it skips the path: the scan's check did not wait for the gate, and only
        //   the record kept the path out. Harmless here, where the hook runs after the record. The
        //   scan's gate is what keeps it out of the two places this test cannot land in: the
        //   delete's few instructions between TryRemove and the record, and a read of the record's
        //   HashSet while a delete adds to it.
        _engine._afterSegmentEntryRemoved = () =>
        {
            if (!atFile.IsSet || released.IsSet) return;
            int logged = _log.Entries.Count;
            released.Set();
            releasedInside   = true;
            finishedInside   = scanned.Wait(TimeSpan.FromMilliseconds(500));
            registeredInside = InCatalog(key);
            loggedInside     = _log.Entries.Skip(logged).Any(e => e.Message.Contains(path, StringComparison.OrdinalIgnoreCase));
        };

        using (SegmentReader.Open(path))
        {
            var scan = Task.Run(() => { try { _engine.LoadSegmentCatalog(); } finally { scanned.Set(); } });
            try
            {
                Assert.True(atFile.Wait(TimeSpan.FromSeconds(20)), "setup: the scan never reached the file");
                await _engine.DeleteSegmentAsync(key);
            }
            finally
            {
                released.Set();
                await scan.WaitAsync(TimeSpan.FromSeconds(20));
            }

            Assert.True(releasedInside, "setup: the delete did not release the scan from inside its step");
            Assert.False(finishedInside, "the scan ran to completion inside the delete's remove-to-park step");
            Assert.False(registeredInside, "the catalog scan registered the path inside the delete's remove-to-park step");
            Assert.False(loggedInside, "the catalog scan decided about the path inside the delete's remove-to-park step");

            Assert.Equal(1, _engine.PendingSegmentDeleteCount);
            Assert.False(InCatalog(key), "the catalog scan registered the path the delete was about to park");

            // It did decide, once the delete was over: skipped the path, and did not quarantine it.
            var entries = _log.Entries;
            Assert.Contains(entries, e => e.Level == Microsoft.Extensions.Logging.LogLevel.Debug &&
                                          e.Message.Contains("skips it", StringComparison.Ordinal) &&
                                          e.Message.Contains(path, StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(entries, e => e.Message.Contains("Quarantining unreadable segment", StringComparison.Ordinal));
        }

        Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
        Assert.False(File.Exists(path));
    }

    [Fact]
    public async Task The_catalog_scan_does_not_register_a_file_deleted_after_it_read_it()
    {
        await _engine.CatalogLoaded;

        var (path, key) = ImportPeerSegment(72);
        bool deleted = false, deleteCompleted = false;
        int  recordedInsideScan = -1;

        // Retention deletes the segment after the scan has read the file and before it registers
        // it. Nothing holds the file, so the unlink succeeds and nothing is parked: the scan has
        // no park to find, only the delete's record of the path.
        //
        // Nothing in the hook may throw: the scan's quarantine catch would swallow it and keep the
        // key out of the catalog for the wrong reason. Outcomes are kept and asserted afterwards.
        _engine._beforeScanRegistersSegment = file =>
        {
            if (deleted || !string.Equals(file, path, StringComparison.OrdinalIgnoreCase)) return;
            deleted            = true;
            deleteCompleted    = _engine.DeleteSegmentAsync(key).IsCompletedSuccessfully;
            recordedInsideScan = _engine.DeletedDuringCatalogScanCount;
        };

        _engine.LoadSegmentCatalog();

        Assert.True(deleted, "setup: the scan never reached the file");
        Assert.True(deleteCompleted, "setup: the delete did not complete synchronously");
        Assert.Equal(1, recordedInsideScan);   // setup: the delete recorded its path for the scan
        Assert.False(File.Exists(path), "setup: the delete should have unlinked the file (is the scan still holding it open?)");
        Assert.Equal(0, _engine.PendingSegmentDeleteCount);
        Assert.False(InCatalog(key), "the catalog scan registered a segment whose file a delete had just removed");

        // Skipped because of the delete, not quarantined: an exception inside the scan's step
        // also keeps the key out, and says so with an Error and a .seg.corrupt file.
        AssertSkippedAsDeleted(path);

        // Recording ends with the scan, and what it recorded goes with it.
        Assert.Equal(0, _engine.DeletedDuringCatalogScanCount);
    }

    [Fact]
    public async Task The_catalog_scan_does_not_quarantine_a_file_deleted_before_it_opened_it()
    {
        await _engine.CatalogLoaded;

        var (path, key) = ImportPeerSegment(76);
        bool deleted = false, deleteCompleted = false;
        int  recordedInsideScan = -1;

        // Retention deletes the segment after the scan has listed the directory and before it opens
        // the file. Nothing holds the file, so the unlink succeeds and nothing is parked. The scan's
        // open then throws for the missing file, and only the delete's record tells that apart from
        // a segment nothing can read. (Nothing in the hook may throw: see the tests above.)
        _engine._beforeScanOpensSegment = file =>
        {
            if (deleted || !string.Equals(file, path, StringComparison.OrdinalIgnoreCase)) return;
            deleted            = true;
            deleteCompleted    = _engine.DeleteSegmentAsync(key).IsCompletedSuccessfully;
            recordedInsideScan = _engine.DeletedDuringCatalogScanCount;
        };

        _engine.LoadSegmentCatalog();

        Assert.True(deleted, "setup: the scan never reached the file");
        Assert.True(deleteCompleted, "setup: the delete did not complete synchronously");
        Assert.Equal(1, recordedInsideScan);   // setup: the delete recorded its path for the scan
        Assert.False(File.Exists(path), "setup: the delete should have unlinked the file");
        Assert.Equal(0, _engine.PendingSegmentDeleteCount);
        Assert.False(InCatalog(key));

        // Skipped at Debug, not quarantined at Error with a Warning for the rename of a missing file.
        AssertSkippedAsDeleted(path);
    }

    [Fact]
    public async Task The_catalog_scan_does_not_register_a_parked_file_its_retry_deleted_after_the_scan_read_it()
    {
        await _engine.CatalogLoaded;

        // Parked before the scan began, so the delete recorded nothing for it: the park alone keeps
        // it out, until a retry settles the path and takes the park away.
        var (path, key) = ParkThroughTheSeam(78);
        bool retried = false;
        int  left    = -1;

        // The retry lands after the scan has read and closed the file and before it registers it.
        // Nothing holds the file any more, so the unlink succeeds on every platform and the path is
        // unparked. (Nothing in the hook may throw: see the tests above.)
        _engine._beforeScanRegistersSegment = file =>
        {
            if (retried || !string.Equals(file, path, StringComparison.OrdinalIgnoreCase)) return;
            retried = true;
            left    = _engine.RetryPendingSegmentDeletes();
        };

        _engine.LoadSegmentCatalog();

        Assert.True(retried, "setup: the scan never reached the file");
        Assert.Equal(0, left);   // setup: the retry settled the path
        Assert.False(File.Exists(path), "setup: the retry should have unlinked the file");
        Assert.False(InCatalog(key), "the catalog scan registered a segment whose parked delete a retry had just completed");
        AssertSkippedAsDeleted(path);
        Assert.Equal(0, _engine.DeletedDuringCatalogScanCount);
    }

    [Fact]
    public async Task The_catalog_scan_does_not_quarantine_a_parked_file_its_retry_deleted_before_the_scan_opened_it()
    {
        await _engine.CatalogLoaded;

        var (path, key) = ParkThroughTheSeam(79);
        bool retried = false;
        int  left    = -1;

        // The same retry, landing between the listing and the open, and running to its end: the
        // path is unparked by the time the scan's open fails on the missing file.
        _engine._beforeScanOpensSegment = file =>
        {
            if (retried || !string.Equals(file, path, StringComparison.OrdinalIgnoreCase)) return;
            retried = true;
            left    = _engine.RetryPendingSegmentDeletes();
        };

        _engine.LoadSegmentCatalog();

        Assert.True(retried, "setup: the scan never reached the file");
        Assert.Equal(0, left);   // setup: the retry settled the path
        Assert.False(File.Exists(path), "setup: the retry should have unlinked the file");
        Assert.False(InCatalog(key));
        AssertSkippedAsDeleted(path);
    }

    [Fact]
    public async Task The_catalog_scan_does_not_quarantine_a_parked_file_its_retry_unlinked_before_the_scan_opened_it()
    {
        await _engine.CatalogLoaded;

        var (path, key) = ParkThroughTheSeam(77);
        using var atFile   = new ManualResetEventSlim();
        using var released = new ManualResetEventSlim();
        using var scanned  = new ManualResetEventSlim();
        bool held = false, scannedInside = false;

        // The scan is held where it has listed the parked file and not opened it. The delete was
        // parked before the scan began, so the scan has no record of it, only the park.
        _engine._beforeScanOpensSegment = file =>
        {
            if (held || !string.Equals(file, path, StringComparison.OrdinalIgnoreCase)) return;
            held = true;
            atFile.Set();
            released.Wait(TimeSpan.FromSeconds(20));
        };

        // The reader has closed and a retry unlinks the file. Right after the unlink, while the path
        // is still parked, the scan is released and runs to its end: its open fails on the missing
        // file, and the park is what says why.
        _engine._deleteSegmentFile = p =>
        {
            File.Delete(p);
            released.Set();
            scannedInside = scanned.Wait(TimeSpan.FromSeconds(20));
        };

        var scan = Task.Run(() => { try { _engine.LoadSegmentCatalog(); } finally { scanned.Set(); } });
        try
        {
            Assert.True(atFile.Wait(TimeSpan.FromSeconds(20)), "setup: the scan never reached the file");
            Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
        }
        finally
        {
            released.Set();
            await scan.WaitAsync(TimeSpan.FromSeconds(20));
        }

        Assert.True(scannedInside, "setup: the scan did not finish between the retry's unlink and the end of its attempt");
        Assert.False(File.Exists(path));
        Assert.False(InCatalog(key));

        var entries = _log.Entries;
        Assert.Contains(entries, e => e.Level == Microsoft.Extensions.Logging.LogLevel.Debug &&
                                      e.Message.Contains("is waiting for its delete to complete", StringComparison.Ordinal) &&
                                      e.Message.Contains(path, StringComparison.OrdinalIgnoreCase));
        AssertNotQuarantined(path);
    }

    [Fact]
    public async Task The_catalog_scan_skips_a_deleted_segment_by_the_delete_s_record_not_by_the_file_and_registers_one_nobody_deleted()
    {
        await _engine.CatalogLoaded;

        // Two files the scan reaches, in this order. The first is in the catalog and deleted under
        // the scan. The second is a file nobody deleted and the catalog does not hold yet, as at boot.
        var (deletedPath, deletedKey) = ImportPeerSegment(73);
        var (livePath, liveKey)       = WritePeerSegment(74);
        Assert.False(InCatalog(liveKey));   // setup

        string     backup    = Path.Combine(_dir, "73.bak");
        bool       deleted   = false, unlinked = false, restored = false;
        Exception? hookError = null;

        // The engine deletes the first segment, and its file is back on disk before the scan
        // decides. A probe of the filesystem finds a file there, just as it does for the live
        // second one, and on a flaky NFS or SMB mount it can find neither. Only the delete's record
        // tells the two apart. (Nothing here may throw: see the test above.)
        _engine._beforeScanRegistersSegment = file =>
        {
            if (deleted || !string.Equals(file, deletedPath, StringComparison.OrdinalIgnoreCase)) return;
            deleted = true;
            try
            {
                File.Copy(deletedPath, backup);
                _engine.DeleteSegmentAsync(deletedKey);
                unlinked = !File.Exists(deletedPath);
                File.Move(backup, deletedPath);
                restored = File.Exists(deletedPath);
            }
            catch (Exception ex) { hookError = ex; }
        };

        _engine.LoadSegmentCatalog();

        Assert.Null(hookError);
        Assert.True(deleted && unlinked && restored, $"setup: deleted={deleted} unlinked={unlinked} restored={restored}");
        Assert.Equal(0, _engine.PendingSegmentDeleteCount);   // setup: skipped by the record, not by a park

        Assert.True(File.Exists(deletedPath));
        Assert.False(InCatalog(deletedKey), "the catalog scan registered a segment the engine had deleted because its file was on disk");
        AssertSkippedAsDeleted(deletedPath);

        Assert.True(InCatalog(liveKey), "the catalog scan skipped a file nobody deleted");
        Assert.Equal(0, _engine.DeletedDuringCatalogScanCount);
    }

    [Fact]
    public async Task A_delete_once_the_catalog_scan_has_finished_records_nothing()
    {
        await _engine.CatalogLoaded;

        var (path, key) = ImportPeerSegment(75);
        await _engine.DeleteSegmentAsync(key);

        Assert.False(InCatalog(key));
        Assert.False(File.Exists(path));
        Assert.Equal(0, _engine.DeletedDuringCatalogScanCount);
    }

    /// <summary>The scan skipped <paramref name="path"/> as deleted: it said so, and did not quarantine it.</summary>
    private void AssertSkippedAsDeleted(string path)
    {
        var entries = _log.Entries;
        Assert.Contains(entries, e => e.Level == Microsoft.Extensions.Logging.LogLevel.Debug &&
                                      e.Message.Contains("was deleted while the catalog scan was running", StringComparison.Ordinal) &&
                                      e.Message.Contains(path, StringComparison.OrdinalIgnoreCase));
        AssertNotQuarantined(path);
    }

    /// <summary>The scan neither quarantined <paramref name="path"/> nor tried to.</summary>
    private void AssertNotQuarantined(string path)
    {
        Assert.DoesNotContain(_log.Entries, e => e.Message.Contains("Quarantining unreadable segment", StringComparison.Ordinal) ||
                                                 e.Message.Contains("Failed to quarantine corrupt segment", StringComparison.Ordinal));
        Assert.False(File.Exists(path + ".corrupt"), "the scan quarantined the file instead of skipping it");
    }

    private IEnumerable<(Microsoft.Extensions.Logging.LogLevel Level, string Message, Exception? Error)> CapWarnings() =>
        _log.Entries.Where(e => e.Level == Microsoft.Extensions.Logging.LogLevel.Warning &&
                                e.Message.Contains("the most that are retried", StringComparison.Ordinal));

    private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger<StorageEngine>
    {
        private readonly List<(Microsoft.Extensions.Logging.LogLevel Level, string Message, Exception? Error)> _entries = [];

        public IReadOnlyList<(Microsoft.Extensions.Logging.LogLevel Level, string Message, Exception? Error)> Entries
        {
            get { lock (_entries) return _entries.ToList(); }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel level) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel level,
                                Microsoft.Extensions.Logging.EventId eventId, TState state,
                                Exception? error, Func<TState, Exception?, string> formatter)
        {
            lock (_entries) _entries.Add((level, formatter(state, error), error));
        }
    }
}
