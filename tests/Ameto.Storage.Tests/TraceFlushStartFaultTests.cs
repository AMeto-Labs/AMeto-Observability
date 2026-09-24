using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;
using EventId = Microsoft.Extensions.Logging.EventId;
using Ameto.Core;
using Ameto.Tracing;
using Ameto.Tracing.Ingestion;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// A FLUSH THAT CANNOT START MUST LEAVE THE ENGINE AS IT FOUND IT (PR #84 review, #1). Starting a
/// flush allocates — the next tier's list, its name pool, the task — and the name pool used to be
/// built AFTER the log had opened its flush window and the tier had been detached. An
/// <see cref="OutOfMemoryException"/> there (the 512 MB stand runs a 384 MB heap hard limit, and has
/// run out) stranded the snapshot where no query could see it, left the window open so that every
/// later flush met "already open", leaked a heavy-phase slot per attempt (the teardown then spent its
/// whole budget and left the engine frozen), and, from the write path, ended the drained batch after
/// the hold that crossed the threshold — the drainer released the other 384 of its 512 spans unwritten.
///
/// <para>Each fault point is a seam that throws where the real failure would: building the name pool
/// (<see cref="SpanStringPools._beforeNewNamePoolForTest"/>), opening the log's window
/// (<see cref="SpanWriteAheadLog._beforeBeginFlushForTest"/> — BeginFlush's own "already open"
/// throws from the same place), and starting the task
/// (<see cref="TraceStorageEngine._beforeFlushTaskStartForTest"/>). No timers: every fact drives the
/// engine synchronously to the fault and reads the state it left.</para>
/// </summary>
public sealed class TraceFlushStartFaultTests : IDisposable
{
    private static readonly DateTimeOffset Base = new(2026, 9, 1, 12, 0, 0, TimeSpan.Zero);

    /// <summary>Bound on a step a correct engine ends at once. Only a hang reaches it.</summary>
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    private readonly string _root = Path.Combine(Path.GetTempPath(), "ameto-flushstart-" + Guid.NewGuid().ToString("N"));
    private readonly List<TraceStorageEngine> _engines = [];

    public TraceFlushStartFaultTests() => Directory.CreateDirectory(_root);

    public void Dispose()
    {
        foreach (var e in _engines)
            try { e.Dispose(); } catch { /* best-effort */ }
        try { Directory.Delete(_root, recursive: true); } catch { /* best-effort */ }
    }

    public enum FaultPoint { NamePool, BeginFlush, TaskStart }

    /// <summary>What the seams throw — distinct from anything the engine could throw on its own.</summary>
    private sealed class InjectedFault(FaultPoint point) : Exception($"injected at {point}");

    private TraceStorageEngine NewEngine(TracesOptions? options = null, SpanStringPools? pools = null,
                                         ILogger<TraceStorageEngine>? logger = null)
    {
        string dir = Path.Combine(_root, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var e = new TraceStorageEngine(dir, logger ?? NullLogger<TraceStorageEngine>.Instance,
                                       false, true, options, pools);
        _engines.Add(e);
        return e;
    }

    /// <summary>Arms the seam at <paramref name="point"/>; it throws while <paramref name="armed"/> says so.</summary>
    private static void Arm(TraceStorageEngine engine, FaultPoint point, Func<bool> armed)
    {
        Action fault = () => { if (armed()) throw new InjectedFault(point); };
        switch (point)
        {
            case FaultPoint.NamePool:   engine.PoolsForTest._beforeNewNamePoolForTest = fault; break;
            case FaultPoint.BeginFlush: engine.WalForTest._beforeBeginFlushForTest    = fault; break;
            case FaultPoint.TaskStart:  engine._beforeFlushTaskStartForTest            = fault; break;
        }
    }

    private static TraceId Trace(int i) => new(0xF1A5, (ulong)(i / 2 + 1));

    private static SpanIngestItem Span(int i) => new()
    {
        TraceId           = Trace(i),
        SpanId            = new SpanId((ulong)(i + 1)),
        StartTimeUnixNano = Base.ToUnixTimeMilliseconds() * 1_000_000L + i * 1_000L,
        DurationNanos     = 1_000_000,
        Name              = "GET /api/pay",
        ServiceName       = i % 2 == 0 ? "gateway" : "billing",
        Kind              = SpanKind.Server,
        Status            = SpanStatusCode.Unset,
        AttributesBytes   = [],
    };

    private static async Task<int> SpansOfTrace(TraceStorageEngine engine, TraceId id)
    {
        int n = 0;
        await foreach (var _ in engine.GetTraceAsync(id)) n++;
        return n;
    }

    /// <summary>
    /// The due-check path (<see cref="TraceStorageEngine.FlushIfDue"/>, the drainer's tick). The start
    /// throws once: the fault reaches the caller — the drainer logs it — and nothing else moves. The
    /// slot is back, every span is still in the tier AND findable by trace id (the trace index was not
    /// cleared), the log holds exactly what it held, and the very next due check flushes normally.
    ///
    /// <para>Reverted (the pool built after <c>BeginFlush</c>, the slot released only by the task,
    /// the task started after the detach): every fault point leaves <see cref="TraceStorageEngine.HeavyPhasesInFlight"/>
    /// at 1, and the name-pool and task-start points also leave the tier empty and the log's window
    /// open, so the "next flush" throws "A span-WAL flush is already open."</para>
    /// </summary>
    [Theory]
    [InlineData(FaultPoint.NamePool)]
    [InlineData(FaultPoint.BeginFlush)]
    [InlineData(FaultPoint.TaskStart)]
    public async Task A_flush_start_that_throws_leaves_the_tier_the_log_and_the_slot_as_they_were(FaultPoint point)
    {
        var engine = NewEngine();
        for (int i = 0; i < 600; i++)                                    // over MinSegmentSpans: due
            Assert.True(engine.WriteSpan(Span(i)), "setup: the live engine refused a span");
        long walBefore = engine.WalWrittenBytesForTest;
        Assert.Equal(0, engine.ColdSegmentCountForTest);

        int shots = 1;
        Arm(engine, point, () => shots-- > 0);

        Assert.Throws<InjectedFault>(engine.FlushIfDue);

        Assert.Equal(0, engine.HeavyPhasesInFlight);
        Assert.Equal(600, engine.HotSpansForTest.Count);
        Assert.Equal(walBefore, engine.WalWrittenBytesForTest);
        Assert.Equal(0, engine.ColdSegmentCountForTest);
        Assert.Equal(2, await SpansOfTrace(engine, Trace(10)));        // through the trace index

        // The next due check is an ordinary flush: the window was closed, nothing is stranded.
        engine.FlushIfDue();
        engine.WaitForFlushForTest();

        // The task gives its slot back in its own finally, a step after it clears _flushTask, so the
        // join above can return just ahead of it. A leaked slot never comes back; the guard only
        // bounds how long that failure takes to report.
        Assert.True(SpinWait.SpinUntil(() => engine.HeavyPhasesInFlight == 0, HangGuard));
        Assert.Equal(1, engine.ColdSegmentCountForTest);
        Assert.Empty(engine.HotSpansForTest);
        Assert.Equal(0, engine.WalWrittenBytesForTest);                 // committed: the log is empty
        Assert.Equal(2, await SpansOfTrace(engine, Trace(10)));        // now from the segment
    }

    /// <summary>
    /// The write path, under the failure that made the wedge cost data: EVERY start throws, and the
    /// tier's byte budget is one byte, so every 128-span hold of a drained 512-span batch crosses the
    /// threshold and tries. The batch must be stored whole — its spans are already in the log and the
    /// tier when the start is tried — and the four failures said once (rate-limited), not four times.
    /// Once the fault clears, one flush carries all 512.
    ///
    /// <para>Reverted (the call site not catching): <c>DrainOnce</c> throws out of the first hold,
    /// 128 spans are in the tier and the drainer has released the other 384.</para>
    /// </summary>
    [Theory]
    [InlineData(FaultPoint.NamePool)]
    [InlineData(FaultPoint.BeginFlush)]
    [InlineData(FaultPoint.TaskStart)]
    public async Task A_drained_batch_is_stored_whole_when_every_flush_start_throws(FaultPoint point)
    {
        const int BatchSpans = 512;                                      // SpanDrainer.BatchSize
        var pools  = new SpanStringPools();
        var log    = new StartFailureLog();
        var engine = NewEngine(new TracesOptions { HotTierMaxBytes = 1 }, pools, log);
        using var ring = new SpanRingBuffer(capacity: 1_024, maxBytes: 8 * 1024 * 1024, pools);
        var drainer = new SpanDrainer(ring, engine, NullLogger<SpanDrainer>.Instance, startLoop: false);

        bool faulty = true;
        Arm(engine, point, () => faulty);

        for (int i = 0; i < BatchSpans; i++)
        {
            var h = new SpanHeader
            {
                TraceId           = Trace(i),
                SpanId            = new SpanId((ulong)(i + 1)),
                StartTimeUnixNano = Base.ToUnixTimeMilliseconds() * 1_000_000L + i * 1_000L,
                DurationNanos     = 1_000_000,
                Kind              = SpanKind.Server,
            };
            Assert.True(ring.TryEnqueueRaw(in h, "GET /api/pay"u8, -1, "gateway"u8, []));
        }
        ring.EndBatch();

        int drained = drainer.DrainOnce(out int taken);

        Assert.Equal(BatchSpans, drained);
        Assert.Equal(BatchSpans, taken);
        Assert.Equal(BatchSpans, engine.HotSpansForTest.Count);
        Assert.Equal(0, engine.HeavyPhasesInFlight);
        Assert.Equal(BatchSpans / TraceStorageEngine.MaxSpansPerWriteHold, engine.FlushStartFailuresForTest);
        Assert.Equal(1, log.StartFailures);                              // said once, not per hold
        Assert.Equal(0, engine.ColdSegmentCountForTest);

        faulty = false;
        engine.FlushHotTier();

        Assert.Equal(0, engine.HeavyPhasesInFlight);
        Assert.Equal(1, engine.ColdSegmentCountForTest);
        Assert.Empty(engine.HotSpansForTest);
        Assert.Equal(2, await SpansOfTrace(engine, Trace(BatchSpans - 1)));
    }

    /// <summary>Counts the rate-limited "could not start a flush" errors, and nothing else.</summary>
    private sealed class StartFailureLog : ILogger<TraceStorageEngine>
    {
        private int _startFailures;
        public int StartFailures => Volatile.Read(ref _startFailures);

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel level) => true;

        public void Log<TState>(LogLevel level, EventId eventId, TState state, Exception? error,
                                Func<TState, Exception?, string> formatter)
        {
            if (level == LogLevel.Error && error is InjectedFault
                && formatter(state, error).StartsWith("Could not start a hot-tier flush", StringComparison.Ordinal))
                Interlocked.Increment(ref _startFailures);
        }
    }
}
