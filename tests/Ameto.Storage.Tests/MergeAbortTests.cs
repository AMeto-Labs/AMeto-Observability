using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// What a merge does when it does NOT finish.
///
/// <para>A merge takes ownership of up to <c>MergeMaxSources</c> = 512 open segment readers and
/// puts a manifest on disk before it streams a byte, so the abort path decides two things that
/// outlive the pass: whether those mapped views are closed, and whether the batch is allowed to
/// be selected again. Both used to be answered by one <c>catch (Exception)</c> that could not
/// tell a shutdown from a full disk from a corrupt file.</para>
/// </summary>
public sealed class MergeAbortTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mergeabort-" + Guid.NewGuid().ToString("N"));
    private string SegDir => Path.Combine(_dir, "segments");
    private StorageEngine _engine = null!;

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = _dir }),
            new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance)
        {
            _allowIndexlessMerge = true,
        };
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static byte[] Props(int i)
    {
        var buf = new ArrayBufferWriter<byte>(96);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("n");   w.Write((long)i);
        w.Write("key"); w.Write("wallet:" + i);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>
    /// One flush = one small segment. Anchored on the sealed side of the bucket grid, because in
    /// an OPEN bucket the planner declines four sources outright and every assertion here would
    /// hold for a reason that has nothing to do with the abort path.
    /// </summary>
    private async Task WriteSegmentAsync(int round, int count, long baseTicks)
    {
        for (int i = 0; i < count; i++)
        {
            int n = round * 1000 + i;
            Assert.True(_engine.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)(round * 100_000 + i)).RawValue,
                TimestampUtcTicks        = baseTicks + n * TimeSpan.TicksPerMillisecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {n} round " + round % 3),
                ServiceNamePoolIndex     = _engine.TemplatePool.Intern("Svc." + round % 4),
            }, Props(n), exception: null));
        }
        await _engine.FlushHotTierAsync();
    }

    private async Task<List<string>> WriteFourSourcesAsync()
    {
        long old = MergeBucketGrid.SealedBucketStart(LogLevel.Information);
        for (int round = 0; round < 4; round++)
            await WriteSegmentAsync(round, 60, old + round * TimeSpan.TicksPerHour);
        var files = Directory.GetFiles(SegDir, "*.seg").OrderBy(static f => f).ToList();
        Assert.Equal(4, files.Count);
        return files;
    }

    /// <summary>
    /// Proves nothing still has the file open. A <see cref="SegmentReader"/> maps its segment, and
    /// the mapping's own handle is opened without <c>FileShare.Delete</c> — which is why a leaked
    /// reader is not merely wasted address space: on Windows retention cannot unlink the file and
    /// compaction cannot rewrite it, for as long as the process lives.
    /// </summary>
    private static void AssertNotHeldOpen(string path)
    {
        using var exclusive = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
    }

    private static void AssertUntouched(string segDir, List<string> sources)
    {
        Assert.Empty(Directory.GetFiles(segDir, "*.mergemanifest"));
        Assert.Empty(Directory.GetFiles(segDir, "*.seg.tmp"));
        Assert.Equal(sources.Count, Directory.GetFiles(segDir, "*.seg").Length);
        foreach (var f in sources) Assert.True(File.Exists(f), $"{Path.GetFileName(f)} was consumed by an aborted merge");
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// SHUTDOWN IS NOT A VERDICT ON THE BATCH. Cancellation used to be caught with everything
    /// else: the maintenance loop got a quiet <c>false</c> instead of the
    /// <see cref="OperationCanceledException"/> it breaks on, a WARNING was logged on every stop,
    /// and — the part that outlived the process's next start — the whole window was skip-listed,
    /// so the segments a fresh run had the most reason to compact were the ones it would not look
    /// at again.
    /// </summary>
    [Fact]
    public async Task AShutdownDuringAMergePropagates_AndLeavesTheBatchMergeable()
    {
        var sources = await WriteFourSourcesAsync();

        using var cts = new CancellationTokenSource();
        _engine._beforeMergeStream = cts.Cancel;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => _engine.TryMergeSmallSegmentsOnceAsync(cts.Token));

        AssertUntouched(SegDir, sources);

        // The batch is still a candidate: this is the next start doing what the previous one was
        // interrupted doing.
        _engine._beforeMergeStream = null;
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        Assert.Single(_engine.ListSegments());
        Assert.Equal(240u, _engine.ListSegments()[0].EventCount);
    }

    /// <summary>
    /// A cancelled merge PASS closes the readers its planner opened, whichever of the two owners
    /// gets there first. This is the caller's half of the contract — <c>TryMergeSmallSegmentsOnceAsync</c>
    /// closing them on the way out — and it is deliberately the weaker of the two assertions here,
    /// because it holds even when the merge task itself never runs. The merge task's own half is
    /// <see cref="AMergeTaskAlwaysEntersItsDelegate_AndClosesWhatItWasHanded"/>; this test cannot
    /// see it and must not be read as covering it.
    /// </summary>
    [Fact]
    public async Task AMergePassThatIsCancelled_ClosesEverySourceItOpened()
    {
        var sources = await WriteFourSourcesAsync();

        using var cts = new CancellationTokenSource();
        _engine._beforeMergeStream = cts.Cancel;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => _engine.TryMergeSmallSegmentsOnceAsync(cts.Token));

        foreach (var f in sources) AssertNotHeldOpen(f);
    }

    /// <summary>
    /// THE MERGE TASK'S DELEGATE ALWAYS RUNS. <c>Task.Run(delegate, ct)</c> with an already
    /// cancelled token transitions the task straight to Canceled without ever invoking the
    /// delegate — and the delegate is where the <c>finally</c> that closes the readers lives, so
    /// a shutdown landing between the flush-slot wait returning and the task being scheduled left
    /// up to <c>MergeMaxSources</c> = 512 mapped views open for the life of the process. On
    /// Windows those files can then be neither unlinked by retention nor rewritten by compaction.
    ///
    /// <para>Driven directly, because through a whole merge pass this is invisible: the caller
    /// closes the readers on its own abort paths too, so the pass-level test above passes
    /// identically whether the delegate ran or not — VERIFIED by putting the token back on
    /// <c>Task.Run</c>, at which point all four of this file's tests stayed green.</para>
    ///
    /// <para>The two assertions pin the two halves separately. The exception must be an
    /// <see cref="OperationCanceledException"/> and not a <see cref="DirectoryNotFoundException"/>:
    /// the output path is inside a directory that does not exist, so a delegate that skipped its
    /// opening <c>ct.ThrowIfCancellationRequested()</c> and went on to open a writer would fail
    /// there instead, and say so. And every source must be closed, which is the part that fails
    /// the moment the token goes back on <c>Task.Run</c> — the delegate not having run, nothing
    /// closed them.</para>
    /// </summary>
    [Fact]
    public async Task AMergeTaskAlwaysEntersItsDelegate_AndClosesWhatItWasHanded()
    {
        var sources = await WriteFourSourcesAsync();

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var  readers = sources.Select(static f => SegmentReader.Open(f)).ToList();
        long expect  = readers.Sum(static r => (long)r.Info.EventCount);
        // Nothing may be written before cancellation is observed, and this is what proves it: the
        // writer's first act is to create its .seg.tmp, which cannot succeed here.
        string segPath = Path.Combine(SegDir, "no-such-directory", "merged.seg");
        try
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => _engine.MergeToColdAsync(readers, new SegmentId(9_999UL), segPath, expect, cts.Token));

            foreach (var f in sources) AssertNotHeldOpen(f);
        }
        finally { foreach (var r in readers) r.Dispose(); }
    }

    /// <summary>
    /// A TRANSIENT failure costs one pass, not the batch. Skip-listing lasts until the process
    /// restarts, so applying it to a disk that filled up or a volume that blinked stopped
    /// compaction on that bucket indefinitely — and the small-file backlog it stops clearing is
    /// the entire reason the sweep exists.
    /// </summary>
    [Fact]
    public async Task ATransientIoFailure_DoesNotQuarantineTheBatch()
    {
        var sources = await WriteFourSourcesAsync();

        _engine._beforeMergeStream = static () => throw new IOException("There is not enough space on the disk.");
        Assert.False(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        AssertUntouched(SegDir, sources);

        _engine._beforeMergeStream = null;
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        Assert.Single(_engine.ListSegments());
        Assert.Equal(240u, _engine.ListSegments()[0].EventCount);
    }

    /// <summary>
    /// The other half of the same discrimination: a structural failure IS the batch's fault. It
    /// will fail identically on every future pass, and since the sources are read interleaved
    /// there is no telling which of them is the bad one — so the window is retired until restart
    /// rather than re-selected forever.
    /// </summary>
    [Fact]
    public async Task ACorruptSource_QuarantinesTheBatchUntilRestart()
    {
        var sources = await WriteFourSourcesAsync();

        _engine._beforeMergeStream = static () => throw new InvalidDataException("Corrupt block at 46");
        Assert.False(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        AssertUntouched(SegDir, sources);

        _engine._beforeMergeStream = null;
        Assert.False(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        Assert.Equal(4, _engine.ListSegments().Count);
    }
}
