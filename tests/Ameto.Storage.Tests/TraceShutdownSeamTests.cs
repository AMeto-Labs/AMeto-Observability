using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE TRACE ENGINE'S TEARDOWN, which had none. <c>grep '_disposed'</c> found three hits in three
/// and a half thousand lines, and all of the following were true at once:
///
/// <list type="bullet">
/// <item>The engine is registered under six singleton interfaces, so the container disposes it six
/// times. Callers 2-6 returned on the <c>Interlocked.Exchange</c> — while caller 1 was still inside
/// a segment build measured at 599 ms per 50 000 spans. Nothing that observed "the engine is
/// disposed" could conclude the files were on disk.</item>
/// <item>Compaction, retention, the index backfill, the index merge and the cold scan checked
/// nothing. A pass mid-flight went on to <c>EnterWriteLock</c> on a lock the teardown had freed,
/// reopened index runs (native bloom memory with nothing left to retire them) and unlinked
/// <c>.trc</c> files after shutdown.</item>
/// <item><c>SpanWriteAheadLog.Append</c> answered a post-dispose append with <c>return;</c> while
/// the very next line still added the span to the hot tier — unrecoverable and queryable at the
/// same instant.</item>
/// </list>
///
/// <para>JUDGED BY SEAMS, NOT BY TIMERS, the way <c>StorageEngineShutdownTests</c> was rebuilt at
/// <c>de6b682</c>. Every fact here drives the engine to a defined point — the heavy-phase wait, the
/// reader wait, a parked segment build — and then asserts what the next call does. The only
/// durations in the file are hang guards, which decide nothing but how long a hung test takes to
/// report.</para>
/// </summary>
public sealed class TraceShutdownSeamTests : IDisposable
{
    /// <summary>Bound on a step a correct engine ends at once. Only a hang reaches it.</summary>
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset From = Base.AddMinutes(-5);
    private static readonly DateTimeOffset To   = Base.AddMinutes(+5);

    private readonly string _root = Path.Combine(Path.GetTempPath(), "ameto-trcshut-" + Guid.NewGuid().ToString("N"));
    private readonly List<TraceStorageEngine> _engines = [];

    public void Dispose()
    {
        foreach (var e in _engines)
            try { e.Dispose(); } catch { /* a test may have left one frozen on purpose */ }
        try { Directory.Delete(_root, recursive: true); } catch { /* best-effort */ }
    }

    private TraceStorageEngine NewEngine(out string dir)
    {
        dir = Path.Combine(_root, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        _engines.Add(e);
        return e;
    }

    private static SpanIngestItem Span(int i, SpanId parent = default) => new()
    {
        TraceId           = new TraceId(0x9E3779B97F4A7C15UL, (ulong)(i / 2 + 1)),
        SpanId            = new SpanId((ulong)(i + 1)),
        ParentSpanId      = parent,
        StartTimeUnixNano = Base.ToUnixTimeMilliseconds() * 1_000_000L + i * 1_000_000L,
        DurationNanos     = 4_000_000,
        Name              = "GET /api/pay",
        ServiceName       = i % 2 == 0 ? "gateway" : "billing",
        Kind              = i % 2 == 0 ? SpanKind.Server : SpanKind.Client,
        Status            = SpanStatusCode.Unset,
        HttpStatusCode    = 200,
        AttributesBytes   = [],
    };

    private static void Fill(TraceStorageEngine engine, int spans)
    {
        for (int i = 0; i < spans; i++)
            Assert.True(engine.WriteSpan(Span(i)), "setup: the live engine refused a span");
    }

    // ── The second disposer ───────────────────────────────────────────────────

    /// <summary>
    /// Six disposers, one teardown. The first is parked inside the final flush's segment build;
    /// the other five must still be waiting there, because the whole point of the exchange
    /// returning early was that a caller could conclude "disposed" over a flush still writing.
    /// </summary>
    [Fact]
    public async Task A_second_disposer_awaits_the_first_instead_of_returning_over_a_running_flush()
    {
        var engine = NewEngine(out _);
        Fill(engine, 600);                      // over MinSegmentSpans, so the final flush builds

        var parked   = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var released = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseOnExit = Seam.ReleasedOnExit(released);
        engine._beforeSegmentWrite = () =>
        {
            parked.TrySetResult();
            released.Task.GetAwaiter().GetResult();
        };

        var first = engine.DisposeAsync().AsTask();
        Assert.Same(parked.Task, await Task.WhenAny(parked.Task, first, Task.Delay(HangGuard)));

        // The five the container would make. None may complete while the build is parked.
        var others = new Task[5];
        for (int i = 0; i < others.Length; i++) others[i] = engine.DisposeAsync().AsTask();

        var all = Task.WhenAll(others);
        var raced = await Task.WhenAny(all, first, Task.Delay(TimeSpan.FromMilliseconds(200)));
        Assert.False(all.IsCompleted, "a later disposer returned while the first was still flushing");
        Assert.False(first.IsCompleted);
        Assert.NotSame(all, raced);

        released.TrySetResult();
        await first.WaitAsync(HangGuard);
        await all.WaitAsync(HangGuard);
        Assert.True(engine.ResourcesFreedForTest);
    }

    // ── The write gate ────────────────────────────────────────────────────────

    /// <summary>
    /// THE HEADLINE BUG, at the seam. With the teardown parked on the heavy-phase wait the write
    /// path is closed but the log is still mapped, so the old failure is reachable and visible:
    /// <c>Append</c> would return silently and the very next line would put the span in the hot
    /// tier. The refusal has to happen ABOVE both halves — the span must be in neither.
    /// </summary>
    [Fact]
    public async Task A_span_arriving_after_the_close_lands_in_neither_the_log_nor_the_hot_tier()
    {
        var engine = NewEngine(out _);
        Fill(engine, 20);

        var wedge    = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var released = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseOnExit = Seam.ReleasedOnExit(released);
        engine._inCompactionRunForTest = () =>
        {
            wedge.TrySetResult();
            released.Task.GetAwaiter().GetResult();
        };
        var compaction = Task.Run(engine.CompactSmallSegments);
        await wedge.Task.WaitAsync(HangGuard);
        Assert.Equal(1, engine.HeavyPhasesInFlight);

        bool accepted = true;
        long walBefore = -1, walAfter = -1;
        int lateTraceSpans = -1;
        var atWait = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        engine._onWaitingForHeavyPhases = () =>
        {
            Assert.True(engine.WritesClosedForTest);
            walBefore = engine.WalWrittenBytesForTest;
            accepted  = engine.WriteSpan(Span(9_999));
            walAfter  = engine.WalWrittenBytesForTest;
            lateTraceSpans = engine.GetTraceAsync(Span(9_999).TraceId).ToBlockingEnumerable().Count();
            atWait.TrySetResult();
        };
        engine._shutdownWaitBudget = TimeSpan.FromMilliseconds(250);

        var dispose = engine.DisposeAsync().AsTask();
        await atWait.Task.WaitAsync(HangGuard);

        Assert.False(accepted);                     // refused, and the caller is told
        Assert.Equal(walBefore, walAfter);          // nothing appended
        Assert.Equal(0, lateTraceSpans);            // and nothing queryable

        released.TrySetResult();
        await compaction.WaitAsync(HangGuard);
        await dispose.WaitAsync(HangGuard);
    }

    // ── The heavy-phase wait ──────────────────────────────────────────────────

    /// <summary>
    /// A compaction wedged on a slow volume. Shutdown must WAIT for it — the pass is holding the
    /// engine lock, the index and the manifest — and must then give up rather than hang the host,
    /// leaving all three allocated instead of freeing them underneath it.
    /// </summary>
    [Fact]
    public async Task A_wedged_compaction_is_waited_for_and_then_left_frozen_rather_than_freed()
    {
        var engine = NewEngine(out _);
        Fill(engine, 20);

        var wedge    = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var released = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseOnExit = Seam.ReleasedOnExit(released);
        engine._inCompactionRunForTest = () =>
        {
            wedge.TrySetResult();
            released.Task.GetAwaiter().GetResult();
        };
        var compaction = Task.Run(engine.CompactSmallSegments);
        await wedge.Task.WaitAsync(HangGuard);

        var atWait = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        engine._onWaitingForHeavyPhases = () => atWait.TrySetResult();
        engine._shutdownWaitBudget = TimeSpan.FromMilliseconds(250);

        var dispose = engine.DisposeAsync().AsTask();

        // The seam is the verdict: an engine that counts nothing never reaches the wait at all.
        Assert.Same(atWait.Task, await Task.WhenAny(atWait.Task, dispose, Task.Delay(HangGuard)));
        Assert.False(dispose.IsCompleted, "the teardown did not wait for the running compaction");

        // The budget runs out and the teardown returns — a wedged pass must not hang the host.
        await dispose.WaitAsync(HangGuard);
        Assert.False(engine.ResourcesFreedForTest,
            "the lock, the index and the WAL were freed while a compaction was still inside them");
        Assert.Equal(1, engine.HeavyPhasesInFlight);

        released.TrySetResult();
        await compaction.WaitAsync(HangGuard);
    }

    /// <summary>
    /// The same wedge, released before the budget expires: the teardown resumes the instant the
    /// count reaches zero and frees everything. Without this, the test above would also pass on an
    /// engine whose wait never ends.
    /// </summary>
    [Fact]
    public async Task A_compaction_that_ends_lets_the_teardown_finish_and_free()
    {
        var engine = NewEngine(out _);
        Fill(engine, 20);

        var wedge    = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var released = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseOnExit = Seam.ReleasedOnExit(released);
        engine._inCompactionRunForTest = () =>
        {
            wedge.TrySetResult();
            released.Task.GetAwaiter().GetResult();
        };
        var compaction = Task.Run(engine.CompactSmallSegments);
        await wedge.Task.WaitAsync(HangGuard);

        var atWait = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        engine._onWaitingForHeavyPhases = () => atWait.TrySetResult();

        var dispose = engine.DisposeAsync().AsTask();
        await atWait.Task.WaitAsync(HangGuard);
        Assert.False(dispose.IsCompleted);

        released.TrySetResult();
        await compaction.WaitAsync(HangGuard);
        await dispose.WaitAsync(HangGuard);
        Assert.True(engine.ResourcesFreedForTest);
        Assert.Equal(0, engine.HeavyPhasesInFlight);
    }

    /// <summary>
    /// A heavy phase that starts after the close starts nothing. The compaction worker runs its
    /// passes through <c>Task.Run(..., ct)</c> and <c>ct</c> does not stop one that has begun, so
    /// "refuse to begin" is the only half of that contract this engine controls.
    /// </summary>
    [Fact]
    public async Task No_heavy_phase_begins_after_the_close()
    {
        var engine = NewEngine(out string dir);
        Fill(engine, 600);
        await engine.DisposeAsync();

        bool compactionRan = false;
        engine._inCompactionRunForTest = () => compactionRan = true;

        engine.CompactSmallSegments();
        engine.LoadColdSegments();
        engine.AdoptUnnamedSegments();
        Assert.False(engine.BackfillNextSegment());
        Assert.False(engine.CompactIndexOnce());
        engine.FlushHotTier();

        Assert.False(compactionRan);
        Assert.Equal(0, engine.HeavyPhasesInFlight);
        // The segment the final flush wrote is still there: nothing ran that could unlink it.
        Assert.NotEmpty(Directory.GetFiles(dir, "*.trc"));
    }

    // ── Retention ─────────────────────────────────────────────────────────────

    /// <summary>
    /// <c>RetentionService</c> holds the engine as an <c>IRetentionTarget</c> and can call at any
    /// time. After the teardown a prune that would expire everything must unlink nothing — it used
    /// to delete <c>.trc</c> files through a disposed lock. The live half is asserted first, so the
    /// fact cannot pass on an engine whose retention never worked.
    /// </summary>
    [Fact]
    public async Task Retention_cannot_unlink_segments_after_the_engine_is_down()
    {
        var engine = NewEngine(out string dir);
        Fill(engine, 600);
        engine.FlushHotTier();
        engine.LoadColdSegments();

        int live = Directory.GetFiles(dir, "*.trc").Length;
        Assert.True(live > 0, "setup: the flush must have written a segment");
        Assert.Equal(live, await engine.PruneAsync(TimeSpan.Zero));
        Assert.Empty(Directory.GetFiles(dir, "*.trc"));

        Fill(engine, 600);
        await engine.DisposeAsync();                  // the final flush writes another segment

        int after = Directory.GetFiles(dir, "*.trc").Length;
        Assert.True(after > 0, "setup: the final flush must have written a segment");
        Assert.Equal(0, await engine.PruneAsync(TimeSpan.Zero));
        Assert.Equal(after, Directory.GetFiles(dir, "*.trc").Length);
    }

    // ── The reader wait ───────────────────────────────────────────────────────

    /// <summary>
    /// A cold walk parked between its snapshot and its first segment open, which is where a query
    /// really is when Kestrel — still serving, because it outlives the hosted services — hands one
    /// in during shutdown. The teardown must reach the reader wait and stay there.
    /// </summary>
    [Fact]
    public async Task A_query_in_flight_holds_the_teardown_at_the_reader_wait()
    {
        var engine = NewEngine(out _);
        Fill(engine, 600);
        engine.FlushHotTier();
        engine.LoadColdSegments();
        Assert.True(engine.ColdSegmentCountForTest > 0, "setup: the walk needs a cold segment to park before");

        var parked   = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var released = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseOnExit = Seam.ReleasedOnExit(released);
        engine._beforeColdSegmentRead = _ =>
        {
            parked.TrySetResult();
            released.Task.GetAwaiter().GetResult();
        };

        var query = Task.Run(() => engine.SearchSpansAsync(From, To, limit: 5).ToBlockingEnumerable().Count());
        await parked.Task.WaitAsync(HangGuard);
        Assert.Equal(1, engine.ActiveReadersForTest);

        var atWait = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        engine._onWaitingForReaders = () => atWait.TrySetResult();

        var dispose = engine.DisposeAsync().AsTask();
        Assert.Same(atWait.Task, await Task.WhenAny(atWait.Task, dispose, Task.Delay(HangGuard)));
        Assert.False(dispose.IsCompleted, "the teardown did not wait for the open reader");

        released.TrySetResult();
        await query.WaitAsync(HangGuard);
        await dispose.WaitAsync(HangGuard);
        Assert.True(engine.ResourcesFreedForTest);
        Assert.Equal(0, engine.ActiveReadersForTest);
    }

    /// <summary>
    /// After the teardown every read path answers empty. An <see cref="ObjectDisposedException"/>
    /// out of the middle of a response is the failure — the lock, the index and the log are gone
    /// by then, and Kestrel is still routing requests at them.
    /// </summary>
    [Fact]
    public async Task Every_read_path_answers_empty_after_the_teardown_instead_of_throwing()
    {
        var engine = NewEngine(out _);
        Fill(engine, 600);
        await engine.DisposeAsync();
        Assert.True(engine.ResourcesFreedForTest);

        var fault = await Record.ExceptionAsync(async () =>
        {
            Assert.Empty(engine.GetTraceAsync(Span(0).TraceId).ToBlockingEnumerable());
            Assert.Empty(engine.SearchSpansAsync(From, To, limit: 50).ToBlockingEnumerable());
            Assert.Empty(await engine.GetAggregateStatsAsync(From, To));

            var graph = await engine.GetServiceGraphAsync(From, To);
            Assert.Empty(graph.Edges);
            Assert.Empty(graph.Nodes);

            var page = await engine.GetTraceListAsync(From, To, null, null, null, null, null, 50);
            Assert.Empty(page.Rows);
            Assert.False(page.Capped);

            var volume = await engine.GetTraceVolumeAsync(From, To, 10);
            Assert.Equal(0, volume.TotalTraces);
        });

        Assert.Null(fault);
        Assert.Equal(0, engine.ActiveReadersForTest);
    }
}
