using System.Buffers;
using System.Runtime.CompilerServices;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Testing;
using MsLogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Ameto.Storage.Tests;

/// <summary>
/// The boot catalog scan ends WITH its engine. The constructor starts it as a background task, and
/// <see cref="StorageEngine.DisposeAsync"/> used to return without it: a scan still walking the
/// directory went on opening segments — a handle and a mapping each — registering them, finishing
/// interrupted merges, renaming torn files aside and logging, in an engine whose host believed
/// storage was down. On Windows the segment it had open could not be deleted: that is how a test
/// which reopened a store and ended early left one <c>.seg</c> in TEMP, and how a new engine opened
/// on the same directory ran beside the old one's scan.
///
/// <para>Shutdown now cancels the scan — it stops between two files — and waits for it, inside the
/// budget its other waits share. A scan stopped that way is not a scan that failed (no Error, and
/// nothing set aside for a failure met after the stop) and never a catalog that loaded: the store
/// stays Loading until the teardown closes it. A constructor that throws after it has started the
/// scan ends it the same way.</para>
///
/// <para>The facts that hold the scan at a seam are judged by which comes first —
/// <c>DisposeAsync</c> returning, or its wait for the scan beginning. On the engine this replaces
/// <c>DisposeAsync</c> returns first, with the scan still parked: the fact fails an assertion, and
/// the park is released on the way out so nothing is stranded.</para>
///
/// <para>A clock appears in four kinds of place, none of which can fail a right engine: two short
/// holds (250 and 300 ms) that only give a wrong engine the time to show itself; the budget facts,
/// whose subject is a timeout; the pause's own length, read on the clock the pause reads; and the
/// reopen-and-dispose fact, which races on purpose and holds whichever way the race goes.</para>
/// </summary>
public sealed class CatalogScanShutdownTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "ameto-scanshut-" + Guid.NewGuid().ToString("N"));

    /// <summary>
    /// Bound on a step that, on a correct engine, a signal ends at once. Only a hang reaches it, so
    /// it decides nothing but how long a hung test takes to report.
    /// </summary>
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The same for a wait on <c>DisposeAsync</c> itself: longer than the engine's own 30 s budget,
    /// so that a teardown which waits its whole budget for a scan comes back and fails the fact on
    /// what it then finds, not on this guard.
    /// </summary>
    private static readonly TimeSpan TeardownGuard = TimeSpan.FromSeconds(90);

    public void Dispose() => TestDataDirectory.Delete(_root);

    // ── A scan that never started ─────────────────────────────────────────────

    /// <summary>
    /// An engine built with its scan held and disposed without a release: the hold ends with the
    /// engine. <c>DisposeAsync</c> used to return with the scan's task still pending, and a release
    /// after it — a test's <c>finally</c>, typically — STARTED the scan, in a disposed engine, over
    /// a directory the test was already deleting.
    ///
    /// <para>What is left behind is a scan that never ran, and nothing may take it for one that
    /// did: the store went from Loading to Closed, and a merge is still refused. And a scan run by
    /// hand afterwards does nothing at all — it does not so much as list a directory that is, by
    /// then, gone: listing it would fail, and say at Error that the scan had.</para>
    /// </summary>
    [Fact]
    public async Task A_scan_still_held_at_shutdown_never_starts_and_a_late_release_opens_nothing()
    {
        var (dir, _) = await StoreAsync(flushes: 2);
        var log    = new CapturingLogger();
        var hold   = new TaskCompletionSource();   // released only once the engine is gone
        var engine = OpenHeld(dir, hold.Task, log);
        int opens  = 0;
        bool endedWithTheEngine;
        QueryAvailability whileHeld;
        try
        {
            engine._beforeScanOpensSegment = _ => Interlocked.Increment(ref opens);
            whileHeld = engine.Availability;
        }
        finally
        {
            await engine.DisposeAsync().AsTask().WaitAsync(TeardownGuard);
            endedWithTheEngine = engine.CatalogLoaded.IsCompleted;
            hold.SetResult();   // the engine this replaces started its scan here
        }

        Assert.Equal(QueryAvailability.Loading, whileHeld);
        Assert.True(endedWithTheEngine,
            "DisposeAsync returned with the held scan still pending — a release after it starts the scan in a disposed engine");
        Assert.True(engine.CatalogLoaded.IsCanceled, "a scan shutdown stopped ends Canceled: that is what its readers go by");
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => engine.CatalogLoaded);
        Assert.Equal(QueryAvailability.Closed, engine.Availability);

        // Not a scanned catalog for anyone: a merge finds the gate shut.
        Assert.Equal(MergeOutcome.Busy, await engine.MergeSmallSegmentsOnceAsync(CancellationToken.None));
        Assert.Equal(0, Volatile.Read(ref opens));
        Assert.Empty(engine.ListSegments());
        DeleteAtOnce(dir);

        // And a scan run now stops before it looks at anything: the directory is not there to list.
        Assert.ThrowsAny<OperationCanceledException>(engine.LoadSegmentCatalog);
        Assert.Equal(0, Volatile.Read(ref opens));
        Assert.DoesNotContain(log.Snapshot(), static l => l.Level >= MsLogLevel.Error);
    }

    /// <summary>
    /// Canceled is the record that the ENGINE'S END stopped the scan, and nothing else may write it:
    /// a store reads Loading for it, a merge Busy, both until a Closed that then never comes. A hold
    /// cancelled by its test — any cancellation that is not shutdown's — is a scan that failed: the
    /// task faults, and the store is Degraded, as after any scan that did not run to its end.
    /// </summary>
    [Fact]
    public async Task A_scan_cancelled_by_anything_but_the_engines_end_is_a_scan_that_failed()
    {
        var (dir, _) = await StoreAsync(flushes: 1);
        var hold   = new TaskCompletionSource();
        var engine = OpenHeld(dir, hold.Task, new CapturingLogger());
        try
        {
            hold.SetCanceled();

            await Assert.ThrowsAsync<InvalidOperationException>(() => engine.CatalogLoaded.WaitAsync(HangGuard));
            Assert.True(engine.CatalogLoaded.IsFaulted);
            Assert.Equal(QueryAvailability.Degraded, engine.Availability);
        }
        finally { await engine.DisposeAsync(); }
    }

    // ── A scan stopped on its way ─────────────────────────────────────────────

    /// <summary>
    /// THE DEFECT, held open: the scan is parked INSIDE a segment it has open — the handle and the
    /// mapping that keep a file from being deleted on Windows. <c>DisposeAsync</c> used to return
    /// right there; a recursive delete of the data directory then failed on that one file and left
    /// it. It waits now: it starts its wait for the scan and does not return until the scan is let
    /// out of the segment, the scan stops at the next file, and the directory goes with ONE plain
    /// delete.
    ///
    /// <para>And the stretch between the scan's stop and the close of the write path — the final
    /// flush, here with ten events to write — reads Loading: the catalog holds two of four
    /// segments, and Available there would hand a count over half a store to whoever asked.</para>
    /// </summary>
    [Fact]
    public async Task Shutdown_waits_for_a_scan_that_has_a_segment_open_and_the_directory_then_goes_at_once()
    {
        var (dir, segments) = await StoreAsync(flushes: 2);   // four segments; the scan is held in the second
        string target = segments[1];
        var log    = new CapturingLogger();
        var hold   = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var engine = OpenHeld(dir, hold.Task, log);

        var inside = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var opened = new List<string>();
        engine._beforeScanOpensSegment    = file => { lock (opened) opened.Add(file); };
        engine._whileScanHoldsSegmentOpen = file =>
        {
            if (!Same(file, target)) return;
            inside.TrySetResult();
            release.Wait(HangGuard);
        };
        QueryAvailability? duringFinalFlush = null;
        bool? scanCanceledByThen = null;
        engine._beforeSwap = () =>
        {
            duringFinalFlush   ??= engine.Availability;
            scanCanceledByThen ??= engine.CatalogLoaded.IsCanceled;
        };

        Task? dispose = null;
        bool heldOpen = false, waited = false, gaveUp = true, endedAtReturn = false;
        string[] openedAtReturn = [];
        try
        {
            hold.SetResult();
            await inside.Task.WaitAsync(HangGuard);
            heldOpen = IsHeldOpen(target);
            Write(engine, 10, LogLevel.Information);   // a final flush: the stretch between the scan's stop and the close

            (dispose, waited) = await DisposeUpToTheScanAsync(engine);
            // A teardown that gave up on the scan — a wait shorter than the budget — would be back well
            // within this. It can only give a wrong engine the time to show itself: the right one is
            // held by the park, and from here only the release lets it on.
            gaveUp = await Task.WhenAny(dispose, Task.Delay(TimeSpan.FromMilliseconds(250))) == dispose;
            release.Set();
            await dispose.WaitAsync(TeardownGuard);
            endedAtReturn = engine.CatalogLoaded.IsCompleted;
            lock (opened) openedAtReturn = [.. opened];
        }
        finally { await LetGoAsync(release, dispose, engine); }

        Assert.True(heldOpen, "setup: the scan was not parked inside an open segment");
        Assert.True(waited, "DisposeAsync returned — or hung — instead of waiting for the scan that had a segment open");
        Assert.False(gaveUp, "DisposeAsync began to wait for the scan and returned while the scan still had the segment open");
        Assert.True(endedAtReturn, "DisposeAsync returned with the boot scan still running");
        Assert.True(engine.CatalogLoaded.IsCanceled, "a scan shutdown stopped ends Canceled, not completed and not faulted");

        // Stopped BETWEEN two files: the one it was in is read and registered like any other, the next
        // is never opened. (Distinct: a file something else held for a moment is opened twice, by the
        // scan's own retry — that is not this fact's.)
        Assert.Equal(new[] { segments[0], target }, openedAtReturn.Distinct());
        var served = engine.ListSegments().Select(static s => s.FilePath).ToHashSet(StringComparer.OrdinalIgnoreCase);
        Assert.Contains(target, served);
        Assert.DoesNotContain(segments[2], served);
        Assert.DoesNotContain(segments[3], served);

        // Never a catalog that loaded: Loading from the scan's stop to the close, then Closed.
        Assert.Equal(QueryAvailability.Loading, duringFinalFlush);
        Assert.True(scanCanceledByThen, "setup: the final flush ran before the scan had been stopped");
        Assert.Equal(QueryAvailability.Closed, engine.Availability);

        // Stopped, not failed: one line saying so, no Error, and not the line of a scan that ran to its end.
        var lines = log.Snapshot();
        Assert.DoesNotContain(lines, static l => l.Level >= MsLogLevel.Error);
        Assert.Single(lines, static l => l.Level == MsLogLevel.Information && l.Message.Contains("was stopped by shutdown", StringComparison.Ordinal));
        Assert.DoesNotContain(lines, static l => l.Message.StartsWith("Loaded ", StringComparison.Ordinal));

        DeleteAtOnce(dir);
        lock (opened) Assert.Equal(openedAtReturn.Length, opened.Count);   // and nothing was opened after DisposeAsync returned
    }

    /// <summary>
    /// The same through the PUBLIC constructor, whose scan is the one a server runs: started with
    /// the shutdown token, so that stopped it ends Canceled — without the token <c>Task.Run</c> ends
    /// it Faulted, which is a store that reads Degraded on its way down, with an Error owed.
    ///
    /// <para>That constructor leaves no time to set a seam, so the scan is parked by the logger it
    /// was given: on the Error line of a torn first segment, between deciding to set the file aside
    /// and the rename. The file is then one the scan had BEGUN before shutdown: it is finished —
    /// set aside — and the scan stops at the next.</para>
    /// </summary>
    [Fact]
    public async Task The_constructors_own_scan_stopped_on_its_way_ends_canceled_and_finishes_the_file_it_had_begun()
    {
        var (dir, segments) = await StoreAsync(flushes: 2);
        string torn = segments[0];
        Halve(torn);

        var atTorn = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        int parks = 0;
        var log = new CapturingLogger();
        log.OnLine = line =>
        {
            if (!line.Contains("Quarantining unreadable segment", StringComparison.Ordinal) || Interlocked.Exchange(ref parks, 1) != 0) return;
            atTorn.TrySetResult();
            release.Wait(HangGuard);
        };
        var engine = Open(dir, log);

        Task? dispose = null;
        bool waited = false;
        try
        {
            await atTorn.Task.WaitAsync(HangGuard);
            (dispose, waited) = await DisposeUpToTheScanAsync(engine);
            release.Set();
            await dispose.WaitAsync(TeardownGuard);
        }
        finally { await LetGoAsync(release, dispose, engine); }

        Assert.True(waited, "DisposeAsync returned — or hung — instead of waiting for the scan that was setting a file aside");
        Assert.True(engine.CatalogLoaded.IsCanceled, "the constructor's scan, stopped by shutdown, did not end Canceled");

        Assert.False(File.Exists(torn), "the file the scan had begun to set aside was left half way");
        Assert.True(File.Exists(torn + ".corrupt"));
        Assert.Empty(engine.ListSegments());   // the three files after it were never reached

        var lines = log.Snapshot();
        Assert.Single(lines, static l => l.Level >= MsLogLevel.Error);   // the torn file's own, and no "scan failed"
        Assert.Single(lines, static l => l.Message.Contains("was stopped by shutdown", StringComparison.Ordinal));
        DeleteAtOnce(dir);
    }

    /// <summary>
    /// A failure met once shutdown has begun is nobody's verdict. The scan is parked just before
    /// it opens a torn segment; the teardown starts; the open then fails as it would on any day —
    /// and the scan, already asked to stop, sets nothing aside and says nothing at Error (it says
    /// at Debug what it dropped). The next start reads the file and decides, as it would have.
    /// </summary>
    [Fact]
    public async Task A_torn_segment_met_once_shutdown_has_begun_is_left_for_the_next_start()
    {
        var (dir, segments) = await StoreAsync(flushes: 2);
        string torn = segments[1];
        Halve(torn);
        var log    = new CapturingLogger();
        var hold   = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var engine = OpenHeld(dir, hold.Task, log);

        var atTorn = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        engine._beforeScanOpensSegment = file =>
        {
            if (!Same(file, torn)) return;
            atTorn.TrySetResult();
            release.Wait(HangGuard);
        };

        Task? dispose = null;
        bool waited = false;
        try
        {
            hold.SetResult();
            await atTorn.Task.WaitAsync(HangGuard);
            (dispose, waited) = await DisposeUpToTheScanAsync(engine);
            release.Set();
            await dispose.WaitAsync(TeardownGuard);
        }
        finally { await LetGoAsync(release, dispose, engine); }

        Assert.True(waited, "DisposeAsync returned — or hung — instead of waiting for the scan parked at a file");
        Assert.True(engine.CatalogLoaded.IsCanceled);
        Assert.True(File.Exists(torn), "a scan that was ending set a file aside");
        Assert.False(File.Exists(torn + ".corrupt"));
        var lines = log.Snapshot();
        Assert.DoesNotContain(lines, static l => l.Level >= MsLogLevel.Error);
        Assert.Single(lines, l => l.Level == MsLogLevel.Debug
                                  && l.Message.Contains("while the engine was shutting down", StringComparison.Ordinal)
                                  && l.Message.Contains(torn, StringComparison.OrdinalIgnoreCase));

        // Nothing was lost by leaving it: the next start's scan sets it aside, and serves the other three.
        var next = Open(dir);
        try
        {
            await next.CatalogLoaded.WaitAsync(HangGuard);
            Assert.True(File.Exists(torn + ".corrupt"), "the next start did not judge the file the stopped scan had left");
            Assert.Equal(3, next.ListSegments().Count);
        }
        finally { await next.DisposeAsync(); }
    }

    /// <summary>
    /// A segment that would not open is tried again after a pause, up to six times. Shutdown ends
    /// the pause and there is no attempt after it — and no verdict either: the file is not called
    /// unreachable, and the store is not marked short by a scan that is ending.
    /// </summary>
    [Fact]
    public async Task A_scan_pausing_before_another_attempt_makes_none_once_shutdown_has_begun()
    {
        var (dir, segments) = await StoreAsync(flushes: 2);
        string busy = segments[1];
        var log    = new CapturingLogger();
        var hold   = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var engine = OpenHeld(dir, hold.Task, log);

        var inPause = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        int attempts = 0, pauses = 0;
        bool lastAttemptWasAtBusy = false;   // read and written by the scan alone: one thread, its hooks in order
        engine._beforeScanOpensSegment = file =>
        {
            lastAttemptWasAtBusy = Same(file, busy);
            if (!lastAttemptWasAtBusy) return;
            Interlocked.Increment(ref attempts);
            throw new IOException("test: another process has the segment");
        };
        engine._catalogScanWaitForTest = wait =>
        {
            // Only the pause after an attempt at `busy`. A file something else held for a moment is
            // retried by the same rule, after the same pause, and is not this fact's: it is slept.
            if (!lastAttemptWasAtBusy) { Thread.Sleep(wait); return; }
            if (Interlocked.Increment(ref pauses) != 1) return;   // a scan that went on would not park again
            inPause.TrySetResult();
            release.Wait(HangGuard);
        };

        Task? dispose = null;
        bool waited = false;
        try
        {
            hold.SetResult();
            await inPause.Task.WaitAsync(HangGuard);
            (dispose, waited) = await DisposeUpToTheScanAsync(engine);
            release.Set();
            await dispose.WaitAsync(TeardownGuard);
        }
        finally { await LetGoAsync(release, dispose, engine); }

        Assert.True(waited, "DisposeAsync returned — or hung — instead of waiting for the scan in its pause");
        Assert.True(engine.CatalogLoaded.IsCanceled);
        Assert.Equal(1, Volatile.Read(ref attempts));
        Assert.Equal(1, Volatile.Read(ref pauses));
        Assert.True(File.Exists(busy));
        Assert.DoesNotContain(log.Snapshot(), static l => l.Level >= MsLogLevel.Error);
    }

    /// <summary>
    /// The scan begins with the sweep that finishes interrupted merges, one manifest at a time.
    /// Shutdown stops it between two manifests: the one it is in is recovered to its end — here a
    /// merge that never committed, whose manifest goes — and the next is left on disk for the next
    /// start, with no segment opened at all.
    /// </summary>
    [Fact]
    public async Task The_sweep_of_interrupted_merges_stops_between_two_manifests()
    {
        var (dir, segments) = await StoreAsync(flushes: 1);
        string segDir = Path.Combine(dir, "segments");
        // Two merges that never committed: a manifest each, and no output beside it.
        string[] name = Path.GetFileName(segments[0]).Split('-');   // {node}-{id}-{min}-{max}.seg
        string[] manifests =
        [
            Path.Combine(segDir, $"{name[0]}-900001-{name[2]}-{name[3]}.mergemanifest"),
            Path.Combine(segDir, $"{name[0]}-900002-{name[2]}-{name[3]}.mergemanifest"),
        ];
        foreach (var m in manifests) File.WriteAllLines(m, [Path.GetFileName(segments[0])]);

        var log    = new CapturingLogger();
        var hold   = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var engine = OpenHeld(dir, hold.Task, log);

        var atManifest = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        int deletes = 0, opens = 0;
        string? first = null;
        engine._beforeScanOpensSegment = _ => Interlocked.Increment(ref opens);
        engine._deleteMergeManifest = path =>
        {
            if (Interlocked.Increment(ref deletes) == 1)
            {
                first = path;
                atManifest.TrySetResult();
                release.Wait(HangGuard);
            }
            File.Delete(path);
        };

        Task? dispose = null;
        bool waited = false;
        try
        {
            hold.SetResult();
            await atManifest.Task.WaitAsync(HangGuard);
            (dispose, waited) = await DisposeUpToTheScanAsync(engine);
            release.Set();
            await dispose.WaitAsync(TeardownGuard);
        }
        finally { await LetGoAsync(release, dispose, engine); }

        Assert.True(waited, "DisposeAsync returned — or hung — instead of waiting for the scan inside a manifest's recovery");
        Assert.True(engine.CatalogLoaded.IsCanceled);
        Assert.Equal(1, Volatile.Read(ref deletes));
        Assert.False(File.Exists(first!), "the manifest the sweep was in was not recovered to its end");
        Assert.Equal(manifests.Where(m => !Same(m, first!)), Directory.GetFiles(segDir, "*.mergemanifest"));
        Assert.Equal(0, Volatile.Read(ref opens));
        Assert.True(File.Exists(segments[0]));
        Assert.DoesNotContain(log.Snapshot(), static l =>
            l.Level >= MsLogLevel.Error || l.Message.Contains("Merge recovery failed", StringComparison.Ordinal));
    }

    /// <summary>
    /// Between the sweep and the listing of the directory the scan asks once more. A stop that came
    /// while the sweep was in its LAST manifest used to be met only at the first listed segment:
    /// the directory was listed and sorted first, and a store with no segment in it — nothing left
    /// to stop the scan at — read as scanned to its end, with the line that says so.
    /// </summary>
    [Fact]
    public async Task A_stop_during_the_sweeps_last_manifest_ends_the_scan_before_it_lists_the_directory()
    {
        var (dir, _) = await StoreAsync(flushes: 0);   // no segment: a scan that went on to list finds nothing to stop it
        string manifest = Path.Combine(dir, "segments", "0-900001-1-2.seg.mergemanifest");   // a merge that never committed
        File.WriteAllLines(manifest, ["0-1-1-2.seg"]);

        var log    = new CapturingLogger();
        var hold   = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var engine = OpenHeld(dir, hold.Task, log);

        var atManifest = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        int deletes = 0;
        engine._deleteMergeManifest = path =>
        {
            if (Interlocked.Increment(ref deletes) == 1)
            {
                atManifest.TrySetResult();
                release.Wait(HangGuard);
            }
            File.Delete(path);
        };

        Task? dispose = null;
        bool waited = false;
        try
        {
            hold.SetResult();
            await atManifest.Task.WaitAsync(HangGuard);
            (dispose, waited) = await DisposeUpToTheScanAsync(engine);
            release.Set();
            await dispose.WaitAsync(TeardownGuard);
        }
        finally { await LetGoAsync(release, dispose, engine); }

        Assert.True(waited, "DisposeAsync returned — or hung — instead of waiting for the scan inside its last manifest");
        Assert.False(File.Exists(manifest), "the manifest the sweep was in was not recovered to its end");
        Assert.True(engine.CatalogLoaded.IsCanceled, "the scan went on to list the directory after it had been told to stop");
        var lines = log.Snapshot();
        Assert.DoesNotContain(lines, static l => l.Message.StartsWith("Loaded ", StringComparison.Ordinal));
        Assert.Single(lines, static l => l.Message.Contains("was stopped by shutdown", StringComparison.Ordinal));
    }

    /// <summary>
    /// Why the teardown waits for the scan BEFORE its last attempts at the parked deletes (which is
    /// what puts the wait before the final flush too: those attempts come first): a segment the
    /// scan has open cannot be unlinked on Windows, and they are the last this process makes. The
    /// delete of a segment the scan is inside is parked behind the scan's reader; the teardown
    /// waits the scan out of it, and its last attempt then removes the file. Waited for any later,
    /// the attempt met the reader and the file stayed.
    /// </summary>
    [StreamingMergeCrashSafetyTests.WindowsFact]
    public async Task The_last_attempts_at_parked_deletes_come_once_the_scan_has_let_go_of_its_segment()
    {
        var (dir, _) = await StoreAsync(flushes: 1);
        var hold   = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var engine = OpenHeld(dir, hold.Task, new CapturingLogger());

        var inside = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        Task? dispose = null;
        bool waited = false;
        int parked = -1;
        string? path = null;
        try
        {
            // A segment the catalog names before the scan reaches it: flushed by this engine while
            // its scan is held, then listed by that scan like the two an earlier run left.
            Write(engine, 10, LogLevel.Warning);
            await engine.FlushHotTierAsync();
            var fresh = Assert.Single(engine.ListSegments());
            path = fresh.FilePath;
            engine._whileScanHoldsSegmentOpen = file =>
            {
                if (!Same(file, path)) return;
                inside.TrySetResult();
                release.Wait(HangGuard);
            };

            hold.SetResult();
            await inside.Task.WaitAsync(HangGuard);
            await engine.DeleteSegmentAsync(SegmentKey.Of(fresh));   // the entry goes; the unlink meets the scan's reader
            parked = engine.PendingSegmentDeleteCount;

            (dispose, waited) = await DisposeUpToTheScanAsync(engine);
            release.Set();
            await dispose.WaitAsync(TeardownGuard);
        }
        finally { await LetGoAsync(release, dispose, engine); }

        Assert.Equal(1, parked);   // setup: the delete was parked behind the scan's open segment
        Assert.True(waited, "DisposeAsync returned — or hung — instead of waiting for the scan that had the segment open");
        Assert.False(File.Exists(path!),
            "the teardown made its last attempt at the parked delete while the scan still had the segment open: the file outlived the engine");
        Assert.Equal(0, engine.PendingSegmentDeleteCount);
        Assert.DoesNotContain(engine.ListSegments(), s => Same(s.FilePath, path!));   // and the scan did not put it back
    }

    // ── What may not stop the teardown, or turn a stop into a failure ─────────

    /// <summary>
    /// The scan says two things from inside its unwinding: at Debug, the failure it drops because
    /// shutdown had begun, and at Information, that it was stopped. A sink that throws on either
    /// used to replace the cancellation — the scan's task faulted, and a store on its way down read
    /// Degraded, with the Error of a scan that failed. Both lines are lost here, to a sink that
    /// throws on each; the stop is still a stop.
    /// </summary>
    [Fact]
    public async Task A_stop_whose_lines_cannot_be_said_is_still_a_stop()
    {
        var (dir, segments) = await StoreAsync(flushes: 2);
        string torn = segments[1];
        Halve(torn);   // its read fails once shutdown has begun: the failure the scan drops, and says so
        int thrown = 0;
        var log = new CapturingLogger();
        log.OnLine = line =>
        {
            if (!line.Contains("was stopped by shutdown", StringComparison.Ordinal)
                && !line.Contains("while the engine was shutting down", StringComparison.Ordinal)) return;
            Interlocked.Increment(ref thrown);
            throw new InvalidOperationException("test: the log sink threw");
        };
        var hold   = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var engine = OpenHeld(dir, hold.Task, log);

        var atFile = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        engine._beforeScanOpensSegment = file =>
        {
            if (!Same(file, torn)) return;
            atFile.TrySetResult();
            release.Wait(HangGuard);
        };

        Task? dispose = null;
        bool waited = false;
        try
        {
            hold.SetResult();
            await atFile.Task.WaitAsync(HangGuard);
            (dispose, waited) = await DisposeUpToTheScanAsync(engine);
            release.Set();
            await dispose.WaitAsync(TeardownGuard);
        }
        finally { await LetGoAsync(release, dispose, engine); }

        Assert.True(waited, "DisposeAsync returned — or hung — instead of waiting for the scan parked at a file");
        Assert.Equal(2, Volatile.Read(ref thrown));   // setup: both lines were written, and their sink threw on each
        Assert.True(engine.CatalogLoaded.IsCanceled, "a sink that threw on one of the stop's lines turned the stop into a scan that failed");
        Assert.DoesNotContain(log.Snapshot(), static l => l.Level >= MsLogLevel.Error);
        Assert.True(File.Exists(torn));
    }

    /// <summary>
    /// Past the budget the teardown says, at Error, that it leaves the scan behind — before its
    /// final flush. A sink that throws on that line used to end the teardown right there: the tier
    /// was not flushed, the write path never closed, and the log and <c>counts.db</c> stayed open.
    /// </summary>
    [Fact]
    public async Task A_sink_that_throws_on_the_line_about_a_scan_left_behind_does_not_end_the_teardown()
    {
        var (dir, segments) = await StoreAsync(flushes: 2);
        string stuck = segments[1];
        var log = new CapturingLogger();
        log.OnLine = static line =>
        {
            if (line.Contains("had not ended after", StringComparison.Ordinal)) throw new InvalidOperationException("test: the log sink threw");
        };
        var hold   = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var engine = OpenHeld(dir, hold.Task, log);
        engine._shutdownWaitBudget = TimeSpan.FromMilliseconds(200);

        var atFile = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        engine._beforeScanOpensSegment = file =>
        {
            if (!Same(file, stuck)) return;
            atFile.TrySetResult();
            release.Wait(HangGuard);
        };
        int swaps = 0;
        engine._beforeSwap = () => Interlocked.Increment(ref swaps);

        Task? dispose = null;
        try
        {
            hold.SetResult();
            await atFile.Task.WaitAsync(HangGuard);
            Write(engine, 10, LogLevel.Information);   // something for the final flush

            dispose = engine.DisposeAsync().AsTask();
            await dispose.WaitAsync(TeardownGuard);    // rethrows what the teardown threw

            Assert.Contains(log.Snapshot(), static l =>
                l.Level == MsLogLevel.Error && l.Message.Contains("had not ended after", StringComparison.Ordinal));   // setup: said, and its sink threw
            Assert.Equal(1, Volatile.Read(ref swaps));   // the final flush ran
            Assert.Equal(QueryAvailability.Closed, engine.Availability);
        }
        finally { await LetGoAsync(release, dispose, engine); }
    }

    /// <summary>
    /// What CAN end the teardown at its wait for the scan is the wait's own registration running
    /// out of memory — and it is let to. Caught there, the method went on with its later
    /// finallys unrun until some await suspended and resumed (the owner builders' remarks): through
    /// the writer fence and two lock statements, whose exits were among them while they were in the
    /// teardown's own body (they are methods of their own now: <see cref="ShutdownLoopJoinTests"/>).
    /// With nothing in the live tier no final flush came to put that right, the teardown reported the
    /// store closed, and a write after it blocked for good where it should be answered. So the
    /// failure ends the teardown, as it does at the <c>CancelAsync</c> that begins it, and nothing is
    /// left locked.
    ///
    /// <para>The seam fails the teardown's first later suspension on a configured task awaiter,
    /// which is the wait for the scan. THE ENGINE IS LEFT HALF TORN DOWN — the teardown is claimed,
    /// a second <c>DisposeAsync</c> returns at once — and what it still holds is closed here.</para>
    /// </summary>
    [Fact]
    public async Task An_out_of_memory_at_the_wait_for_the_scan_ends_the_teardown_and_leaves_nothing_locked()
    {
        var (dir, segments) = await StoreAsync(flushes: 2);
        string stuck = segments[1];
        var hold   = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var engine = OpenHeld(dir, hold.Task, new CapturingLogger());

        var atFile = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        engine._beforeScanOpensSegment = file =>
        {
            if (!Same(file, stuck)) return;
            atFile.TrySetResult();
            release.Wait(HangGuard);
        };

        try
        {
            hold.SetResult();
            await atFile.Task.WaitAsync(HangGuard);   // nothing is written: no final flush would resume the method

            int asked = 0;
            OwnerAsync.LaterSuspensionFailsForTest.Value = (stateMachine, awaiter) =>
                stateMachine.Name.Contains("<DisposeCoreAsync>", StringComparison.Ordinal)
                && awaiter == typeof(ConfiguredTaskAwaitable.ConfiguredTaskAwaiter)
                && Interlocked.Increment(ref asked) == 1;
            Task dispose;
            try { dispose = engine.DisposeAsync().AsTask(); }
            finally { OwnerAsync.LaterSuspensionFailsForTest.Value = null; }

            var failure = await Record.ExceptionAsync(() => dispose.WaitAsync(TeardownGuard));
            Assert.Equal(1, Volatile.Read(ref asked));   // setup: the wait for the scan was a later suspension, and the seam failed it
            Assert.IsType<OutOfMemoryException>(failure);

            // Nothing is left locked: a write is ANSWERED, and a reader opens. (Answered how is not this
            // fact's: today the write is taken, since a teardown that ended here never closed the write
            // path — the engine is half torn down, as the summary says.)
            var write = Task.Run(() => engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = DateTime.UtcNow.Ticks,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = engine.TemplatePool.Intern("evt {n}"),
            }, Props(0)));
            await write.WaitAsync(HangGuard);
            await Task.Run(() => engine.OpenHotTierReader().Dispose()).WaitAsync(HangGuard);
        }
        finally
        {
            release.Set();
            try { await engine.DisposeAsync(); } catch { /* the teardown's own failure is the fact's, above */ }
            await Task.WhenAny(engine.CatalogLoaded, Task.Delay(HangGuard));
            engine.LiveWalForTest?.Dispose();
            engine.CountsStoreForTest?.Dispose();
        }
    }

    // ── The budget ────────────────────────────────────────────────────────────

    /// <summary>
    /// The wait for the scan is inside the shutdown budget, like the waits for flushes and readers:
    /// a scan that does not come back from a file — a volume that has stopped answering — cannot
    /// hold the host's stop for good. Past the budget the teardown goes on and says so at Error;
    /// the scan stops as soon as the call it is in returns, at the very next file, reading the
    /// shutdown token of a source the teardown has disposed by then.
    /// </summary>
    [Fact]
    public async Task A_scan_that_does_not_come_back_from_a_file_is_left_behind_past_the_budget_and_stops_when_it_does()
    {
        var (dir, segments) = await StoreAsync(flushes: 2);
        string stuck = segments[1];
        var log    = new CapturingLogger();
        var hold   = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var engine = OpenHeld(dir, hold.Task, log);
        engine._shutdownWaitBudget = TimeSpan.FromMilliseconds(200);

        var atFile = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var opened = new List<string>();
        engine._beforeScanOpensSegment = file =>
        {
            lock (opened) opened.Add(file);
            if (!Same(file, stuck)) return;
            atFile.TrySetResult();
            release.Wait(HangGuard);
        };

        Task? dispose = null;
        try
        {
            hold.SetResult();
            await atFile.Task.WaitAsync(HangGuard);

            dispose = engine.DisposeAsync().AsTask();
            await dispose.WaitAsync(TeardownGuard);

            Assert.False(engine.CatalogLoaded.IsCompleted, "setup: the scan ended while it was parked");
            Assert.Equal(QueryAvailability.Closed, engine.Availability);
            Assert.Contains(log.Snapshot(), static l =>
                l.Level == MsLogLevel.Error && l.Message.Contains("boot catalog scan", StringComparison.Ordinal)
                                            && l.Message.Contains("had not ended after", StringComparison.Ordinal));

            release.Set();   // the call comes back
            await Task.WhenAny(engine.CatalogLoaded, Task.Delay(HangGuard));
            Assert.True(engine.CatalogLoaded.IsCanceled, "the scan went on past the file it was in when shutdown gave up waiting");
            lock (opened) Assert.Equal(new[] { segments[0], stuck }, opened.Distinct());
            Assert.Single(log.Snapshot(), static l => l.Message.Contains("was stopped by shutdown", StringComparison.Ordinal));
        }
        finally { await LetGoAsync(release, dispose, engine); }
    }

    // ── A start that fails ────────────────────────────────────────────────────

    /// <summary>
    /// A constructor that throws after it has started the scan — here before its boot log — leaves
    /// an engine nobody holds, so nobody disposes it. Its scan used to stay: pending for good if a
    /// test held it, and started by a release that came later, in an engine that never was. The
    /// failing constructor ends it now.
    ///
    /// <para>The release is made on a pool thread, where a continuation runs inside the call that
    /// releases it: an engine that left its scan pending scans right there, to the line that says
    /// it loaded.</para>
    /// </summary>
    [Fact]
    public async Task A_start_that_fails_takes_the_scan_it_held_with_it()
    {
        var (dir, _) = await StoreAsync(flushes: 2);
        var log   = new CapturingLogger();
        var hold  = new TaskCompletionSource();
        var crash = new IOException("test: the start fails before its boot log");
        StorageEngine.BeforeBootWalOpenForTest.Value = () => throw crash;
        Exception? thrown;
        try { thrown = Record.Exception(() => OpenHeld(dir, hold.Task, log)); }
        finally { StorageEngine.BeforeBootWalOpenForTest.Value = null; }

        await Task.Run(hold.SetResult);

        Assert.Same(crash, thrown);
        Assert.DoesNotContain(log.Snapshot(), static l => l.Message.StartsWith("Loaded ", StringComparison.Ordinal));
        DeleteAtOnce(dir);
    }

    /// <summary>
    /// And one already RUNNING is waited for, as the teardown waits: when the constructor's failure
    /// reaches its caller, the scan has ended. The scan is parked by its logger, in the middle of
    /// setting a torn segment aside, when the start fails; it is let out 300 ms later, by a clock.
    /// A constructor that did not wait is back with its failure long before that, the scan still
    /// parked and neither of its two last lines written.
    ///
    /// <para>Either line will do: stopped is what a waiting constructor's scan says, and should
    /// this thread stall for those 300 ms between the park's release being set up and the cancel,
    /// the scan ends by reading the rest — also before the failure is out.</para>
    /// </summary>
    [Fact]
    public async Task A_start_that_fails_waits_for_the_scan_it_had_started()
    {
        var (dir, segments) = await StoreAsync(flushes: 2);
        string torn = segments[0];
        Halve(torn);

        using var parked  = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        int parks = 0;
        var log = new CapturingLogger();
        log.OnLine = line =>
        {
            if (!line.Contains("Quarantining unreadable segment", StringComparison.Ordinal) || Interlocked.Exchange(ref parks, 1) != 0) return;
            parked.Set();
            release.Wait(HangGuard);
        };
        var crash = new IOException("test: the start fails before its boot log");
        StorageEngine.BeforeBootWalOpenForTest.Value = () =>
        {
            if (!parked.Wait(HangGuard)) throw new InvalidOperationException("test: the scan never reached the torn segment");
            _ = Task.Delay(TimeSpan.FromMilliseconds(300)).ContinueWith(_ => release.Set(), TaskScheduler.Default);
            throw crash;
        };
        Exception? thrown;
        (MsLogLevel Level, string Message)[] atTheFailure;
        try
        {
            thrown       = Record.Exception(() => Open(dir, log));
            atTheFailure = log.Snapshot();
        }
        finally
        {
            StorageEngine.BeforeBootWalOpenForTest.Value = null;
            release.Set();
        }

        Assert.Same(crash, thrown);
        Assert.Contains(atTheFailure, static l =>
            l.Message.Contains("was stopped by shutdown", StringComparison.Ordinal) || l.Message.StartsWith("Loaded ", StringComparison.Ordinal));
        Assert.True(File.Exists(torn + ".corrupt"), "the file the scan had begun to set aside was left half way");
        DeleteAtOnce(dir);
    }

    /// <summary>
    /// …and one that does not end within the budget is left behind, as the teardown leaves one: the
    /// failing constructor says so at Error — on a sink that may throw — and its own failure is
    /// still what goes on to its caller. Through a constructor the budget is the 30 s an engine is
    /// born with, so the stop is run here by hand, on an engine that was built, with the budget
    /// shortened and the scan parked at a file.
    /// </summary>
    [Fact]
    public async Task A_start_that_fails_says_so_when_the_scan_it_started_outlasts_the_budget()
    {
        var (dir, segments) = await StoreAsync(flushes: 2);
        string stuck = segments[1];
        var log = new CapturingLogger();
        log.OnLine = static line =>
        {
            if (line.Contains("failed to start", StringComparison.Ordinal)) throw new InvalidOperationException("test: the log sink threw");
        };
        var hold   = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var engine = OpenHeld(dir, hold.Task, log);
        engine._shutdownWaitBudget = TimeSpan.FromMilliseconds(200);

        var atFile = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        engine._beforeScanOpensSegment = file =>
        {
            if (!Same(file, stuck)) return;
            atFile.TrySetResult();
            release.Wait(HangGuard);
        };

        try
        {
            hold.SetResult();
            await atFile.Task.WaitAsync(HangGuard);

            // Returns, having waited the budget and not the scan — and without the sink's throw.
            await Task.Run(engine.StopCatalogScanOfAFailedStart).WaitAsync(HangGuard);

            Assert.False(engine.CatalogLoaded.IsCompleted, "setup: the scan ended while it was parked");
            Assert.Single(log.Snapshot(), static l =>
                l.Level == MsLogLevel.Error && l.Message.Contains("failed to start", StringComparison.Ordinal)
                                            && l.Message.Contains("had not ended", StringComparison.Ordinal));

            release.Set();   // the call the scan was in comes back
            await Task.WhenAny(engine.CatalogLoaded, Task.Delay(HangGuard));
            Assert.True(engine.CatalogLoaded.IsCanceled, "the scan of a start that failed went on past the file it was in");
        }
        finally
        {
            release.Set();
            await engine.DisposeAsync();   // this engine was built: its own teardown runs to its end
            await Task.WhenAny(engine.CatalogLoaded, Task.Delay(HangGuard));
        }
        Assert.Equal(QueryAvailability.Closed, engine.Availability);
    }

    // ── As a host meets it ────────────────────────────────────────────────────

    /// <summary>
    /// The sentence a host — or a test's teardown — relies on, with nothing held: a store reopened
    /// and disposed at once can be removed at once, by one plain <c>Directory.Delete</c>. The scan
    /// is wherever the teardown finds it, round after round: not started, among the six segments,
    /// or done.
    /// </summary>
    [Fact]
    public async Task A_store_reopened_and_disposed_at_once_can_be_removed_at_once()
    {
        var (_, segments) = await StoreAsync(flushes: 3);
        for (int round = 0; round < 8; round++)
        {
            string dir    = NewDir();
            string segDir = Path.Combine(dir, "segments");
            Directory.CreateDirectory(segDir);
            foreach (var s in segments) File.Copy(s, Path.Combine(segDir, Path.GetFileName(s)));

            var engine = Open(dir);
            await engine.DisposeAsync().AsTask().WaitAsync(TeardownGuard);

            Assert.True(engine.CatalogLoaded.IsCompleted, $"round {round}: DisposeAsync returned with the boot scan still running");
            DeleteAtOnce(dir);
        }
    }

    // ── The pause itself ──────────────────────────────────────────────────────

    /// <summary>
    /// The scan's pause between two attempts lasts its length, and not a slice longer than shutdown
    /// lets it — also when the teardown has already disposed the token's source, which a scan that
    /// outlasted the budget meets (a wait on the token's handle throws there).
    /// </summary>
    [Fact]
    public async Task The_scans_pause_lasts_its_length_and_ends_when_shutdown_begins()
    {
        long from = Environment.TickCount64;
        StorageEngine.SleepUnlessStopped(TimeSpan.FromMilliseconds(120), CancellationToken.None);
        Assert.True(Environment.TickCount64 - from >= 120, "a pause nothing interrupted was cut short");

        var cts   = new CancellationTokenSource();
        var token = cts.Token;
        var pause = Task.Run(() => StorageEngine.SleepUnlessStopped(TimeSpan.FromMinutes(10), token));
        cts.Cancel();
        await pause.WaitAsync(HangGuard);   // times out if the pause went on after shutdown had begun

        cts.Dispose();
        var late = Task.Run(() => StorageEngine.SleepUnlessStopped(TimeSpan.FromMinutes(10), token));
        await late.WaitAsync(HangGuard);    // nor does one begun after the source was disposed
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Starts <c>DisposeAsync</c> with the scan parked at a seam, and returns once the teardown has
    /// either returned or started waiting for the scan. <c>Waited</c>: it started waiting, and had
    /// not returned — from there only the park's release moves it on, so the caller releases next.
    /// </summary>
    private static async Task<(Task Dispose, bool Waited)> DisposeUpToTheScanAsync(StorageEngine engine)
    {
        var waiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        engine._onWaitingForCatalogScan = () => waiting.TrySetResult();
        var dispose = engine.DisposeAsync().AsTask();
        await Task.WhenAny(waiting.Task, dispose, Task.Delay(HangGuard));
        return (dispose, waiting.Task.IsCompleted && !dispose.IsCompleted);
    }

    /// <summary>
    /// A test's way out, green or red: the park is released, the teardown awaited, and the scan
    /// given time to end — on an engine that does not wait for it, it is still running here, and
    /// the directory is about to be deleted.
    /// </summary>
    private static async Task LetGoAsync(ManualResetEventSlim release, Task? dispose, StorageEngine engine)
    {
        release.Set();
        try { await (dispose ?? engine.DisposeAsync().AsTask()).WaitAsync(TeardownGuard); }
        catch (Exception) { /* the assertions report it */ }
        await Task.WhenAny(engine.CatalogLoaded, Task.Delay(HangGuard));
    }

    /// <summary>
    /// One plain <c>Directory.Delete</c>, and the directory gone after it — what a host, or any
    /// teardown, may do the moment <c>DisposeAsync</c> has returned. No retry and no pool clearing:
    /// a file the scan still had open fails the delete with a sharing violation naming it (the
    /// scan's reader shares its file for reading only), and a retry would wait out the very handle
    /// these facts are here to rule out.
    ///
    /// <para>Somebody else's handle that shares as little fails it the same way: a third party
    /// reading a file written a moment ago. One that shares for delete, as a virus scanner's does,
    /// does not fail the delete — measured in review on Windows 11 (NTFS, .NET 10.0.15), where
    /// the entry is gone at once. Where such an entry lingered instead, the delete would still
    /// return, since the runtime does not mind a directory that is not yet empty: the last
    /// assertion below is the line that would say so. CI's runners scan in real time, and their
    /// runs so far have left nothing behind from several hundred bare deletes each.</para>
    /// </summary>
    private static void DeleteAtOnce(string dir)
    {
        try { Directory.Delete(dir, recursive: true); }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            Assert.Fail($"The data directory could not be removed the moment DisposeAsync had returned: {ex.Message} Left: {Left(dir)}");
        }
        Assert.False(Directory.Exists(dir), $"Directory.Delete returned, and {dir} is still there. Left: {Left(dir)}");

        static string Left(string dir)
        {
            try
            {
                return Directory.Exists(dir)
                    ? string.Join(", ", Directory.GetFileSystemEntries(dir, "*", SearchOption.AllDirectories).Select(e => Path.GetRelativePath(dir, e)))
                    : "nothing";
            }
            catch (Exception ex) { return "entries that cannot be listed (" + ex.Message + ")"; }
        }
    }

    /// <summary>Whether somebody has <paramref name="path"/> open: an open that shares nothing is refused.</summary>
    private static bool IsHeldOpen(string path)
    {
        try
        {
            using var exclusive = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.None);
            return false;
        }
        catch (IOException) { return true; }
    }

    private static bool Same(string a, string b) => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);

    /// <summary>Cuts a segment to half its length: the torn file a lost write leaves, whose footer is gone.</summary>
    private static void Halve(string path)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Write);
        fs.SetLength(fs.Length / 2);
    }

    private string NewDir()
    {
        string dir = Path.Combine(_root, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    /// <summary>
    /// A closed store of <c>2 × flushes</c> segments — a flush writes one per level — and their
    /// files in the order the scan reads them.
    /// </summary>
    private async Task<(string Dir, string[] Segments)> StoreAsync(int flushes)
    {
        string dir = NewDir();
        var engine = Open(dir);
        try
        {
            await engine.CatalogLoaded.WaitAsync(HangGuard);
            for (int f = 0; f < flushes; f++)
            {
                Write(engine, 20, LogLevel.Information);
                Write(engine, 20, LogLevel.Error);
                await engine.FlushHotTierAsync();
            }
        }
        finally { await engine.DisposeAsync(); }

        var files = Directory.GetFiles(Path.Combine(dir, "segments"), "*.seg");
        Array.Sort(files, StringComparer.Ordinal);   // the scan's order
        Assert.Equal(2 * flushes, files.Length);
        return (dir, files);
    }

    private static ServerOptions OptionsFor(string dir) => new()
    {
        DataDirectory = dir,
        HotTier       = new HotTierOptions { MaxSizeBytes = 8 * 1024 * 1024 },
    };

    /// <summary>The engine a server gets: the public constructor, its scan started at once.</summary>
    private static StorageEngine Open(string dir, Microsoft.Extensions.Logging.ILogger<StorageEngine>? logger = null)
    {
        var opts = OptionsFor(dir);
        return new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            logger ?? NullLogger<StorageEngine>.Instance);
    }

    /// <summary>An engine whose boot scan starts only once <paramref name="heldUntil"/> completes — after the test has set its seams.</summary>
    private static StorageEngine OpenHeld(string dir, Task heldUntil, Microsoft.Extensions.Logging.ILogger<StorageEngine> logger)
    {
        var opts = OptionsFor(dir);
        return new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            logger, maintenanceStartDelay: TimeSpan.FromHours(1), bootScanHeldUntil: heldUntil);
    }

    private static byte[] Props(int i)
    {
        var buf = new ArrayBufferWriter<byte>(32);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write((long)i);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private static void Write(StorageEngine engine, int count, LogLevel level)
    {
        for (int i = 0; i < count; i++)
            Assert.True(engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = DateTime.UtcNow.Ticks,
                Level                    = level,
                MessageTemplatePoolIndex = engine.TemplatePool.Intern("evt {n}"),
            }, Props(i)));
    }

    /// <summary>Every line at every level, formatted; <see cref="OnLine"/> runs on the thread that wrote it, so blocking there parks that thread at its line.</summary>
    private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger<StorageEngine>
    {
        private readonly List<(MsLogLevel Level, string Message)> _lines = [];

        public Action<string>? OnLine;

        public (MsLogLevel Level, string Message)[] Snapshot() { lock (_lines) return [.. _lines]; }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(MsLogLevel logLevel) => true;

        public void Log<TState>(
            MsLogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
            TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            string message = formatter(state, exception);
            lock (_lines) _lines.Add((logLevel, message));
            OnLine?.Invoke(message);
        }
    }
}
