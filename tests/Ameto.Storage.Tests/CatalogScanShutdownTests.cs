using System.Buffers;
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
/// nothing set aside by a scan that is ending) and never a catalog that loaded: the store stays
/// Loading until the teardown closes it.</para>
///
/// <para>Every test here holds the scan at a seam instead of racing it, and its verdict is which
/// comes first — <c>DisposeAsync</c> returning, or its wait for the scan beginning — not a timer.
/// On the engine this replaces <c>DisposeAsync</c> returns first, with the scan still parked: the
/// test fails an assertion, and the park is released on the way out so nothing is stranded.</para>
/// </summary>
public sealed class CatalogScanShutdownTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "ameto-scanshut-" + Guid.NewGuid().ToString("N"));

    /// <summary>
    /// Bound on a step that, on a correct engine, a signal ends at once. Only a hang reaches it, so
    /// it decides nothing but how long a hung test takes to report.
    /// </summary>
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

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
        engine._beforeScanOpensSegment = _ => Interlocked.Increment(ref opens);
        Assert.Equal(QueryAvailability.Loading, engine.Availability);

        await engine.DisposeAsync().AsTask().WaitAsync(HangGuard);
        bool endedWithTheEngine = engine.CatalogLoaded.IsCompleted;

        hold.SetResult();   // the engine this replaces started its scan here

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
            await dispose.WaitAsync(HangGuard);
            endedAtReturn = engine.CatalogLoaded.IsCompleted;
            lock (opened) openedAtReturn = [.. opened];
        }
        finally { await LetGoAsync(release, dispose, engine); }

        Assert.True(heldOpen, "setup: the scan was not parked inside an open segment");
        Assert.True(waited, "DisposeAsync returned — or hung — instead of waiting for the scan that had a segment open");
        Assert.False(gaveUp, "DisposeAsync began to wait for the scan and returned while the scan still had the segment open");
        Assert.True(endedAtReturn, "DisposeAsync returned with the boot scan still running");
        Assert.True(engine.CatalogLoaded.IsCanceled, "a scan shutdown stopped ends Canceled, not completed and not faulted");

        // Stopped BETWEEN two files: the one it was in is read and registered like any other, the next is never opened.
        Assert.Equal(new[] { segments[0], target }, openedAtReturn);
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
            await dispose.WaitAsync(HangGuard);
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
    /// and the scan, already asked to stop, sets nothing aside and says nothing at Error. The next
    /// start reads the file and decides, as it would have.
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
            await dispose.WaitAsync(HangGuard);
        }
        finally { await LetGoAsync(release, dispose, engine); }

        Assert.True(waited, "DisposeAsync returned — or hung — instead of waiting for the scan parked at a file");
        Assert.True(engine.CatalogLoaded.IsCanceled);
        Assert.True(File.Exists(torn), "a scan that was ending set a file aside");
        Assert.False(File.Exists(torn + ".corrupt"));
        Assert.DoesNotContain(log.Snapshot(), static l => l.Level >= MsLogLevel.Error);

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
        engine._beforeScanOpensSegment = file =>
        {
            if (!Same(file, busy)) return;
            Interlocked.Increment(ref attempts);
            throw new IOException("test: another process has the segment");
        };
        engine._catalogScanWaitForTest = _ =>
        {
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
            await dispose.WaitAsync(HangGuard);
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
            await dispose.WaitAsync(HangGuard);
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
            await dispose.WaitAsync(HangGuard);

            Assert.False(engine.CatalogLoaded.IsCompleted, "setup: the scan ended while it was parked");
            Assert.Equal(QueryAvailability.Closed, engine.Availability);
            Assert.Contains(log.Snapshot(), static l =>
                l.Level == MsLogLevel.Error && l.Message.Contains("boot catalog scan", StringComparison.Ordinal)
                                            && l.Message.Contains("still running", StringComparison.Ordinal));

            release.Set();   // the call comes back
            await Task.WhenAny(engine.CatalogLoaded, Task.Delay(HangGuard));
            Assert.True(engine.CatalogLoaded.IsCanceled, "the scan went on past the file it was in when shutdown gave up waiting");
            lock (opened) Assert.Equal(new[] { segments[0], stuck }, opened.ToArray());
            Assert.Single(log.Snapshot(), static l => l.Message.Contains("was stopped by shutdown", StringComparison.Ordinal));
        }
        finally { await LetGoAsync(release, dispose, engine); }
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
            await engine.DisposeAsync().AsTask().WaitAsync(HangGuard);

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
    public void The_scans_pause_lasts_its_length_and_ends_when_shutdown_begins()
    {
        long from = Environment.TickCount64;
        StorageEngine.SleepUnlessStopped(TimeSpan.FromMilliseconds(120), CancellationToken.None);
        Assert.True(Environment.TickCount64 - from >= 120, "a pause nothing interrupted was cut short");

        var cts   = new CancellationTokenSource();
        var token = cts.Token;
        var pause = Task.Run(() => StorageEngine.SleepUnlessStopped(TimeSpan.FromMinutes(10), token));
        cts.Cancel();
        Assert.True(pause.Wait(HangGuard), "the pause went on after shutdown had begun");

        cts.Dispose();
        var late = Task.Run(() => StorageEngine.SleepUnlessStopped(TimeSpan.FromMinutes(10), token));
        Assert.True(late.Wait(HangGuard), "a pause begun after the source was disposed did not end at once");
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
        try { await (dispose ?? engine.DisposeAsync().AsTask()).WaitAsync(HangGuard); }
        catch (Exception) { /* the assertions report it */ }
        await Task.WhenAny(engine.CatalogLoaded, Task.Delay(HangGuard));
    }

    /// <summary>
    /// One plain <c>Directory.Delete</c>: no retry and no pool clearing — what a host, or any
    /// teardown, may do the moment <c>DisposeAsync</c> has returned.
    /// </summary>
    private static void DeleteAtOnce(string dir)
    {
        try { Directory.Delete(dir, recursive: true); }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            string left = Directory.Exists(dir)
                ? string.Join(", ", Directory.GetFiles(dir, "*", SearchOption.AllDirectories).Select(f => Path.GetRelativePath(dir, f)))
                : "nothing";
            Assert.Fail($"The data directory could not be removed the moment DisposeAsync had returned: {ex.Message} Left: {left}");
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
