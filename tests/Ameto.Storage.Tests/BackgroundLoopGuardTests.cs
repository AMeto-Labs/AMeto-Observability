using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Ameto.Tracing;
using Ameto.Tracing.Ingestion;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// #137: a background loop whose only <c>catch</c> logged without protection died when the log call
/// itself threw — an <see cref="OutOfMemoryException"/> on a 512 MB stand under load — and never ran
/// again: the span drainer (the ring then fills and every request with spans is refused), the metric
/// flush and rollup loops, the segment-delete retry loop (which also kept its "running" flag, so no
/// other loop could start). The log engine's own loops were given this guard in e6fe109; these are
/// the loops outside it. Each fact here makes the error line throw and then shows the loop still turning.
/// </summary>
public sealed class BackgroundLoopGuardTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-loopguard-" + Guid.NewGuid().ToString("N"));

    public BackgroundLoopGuardTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        // The pool keeps the retention store's Ameto.db open after the engine is gone; on Windows the
        // delete then fails and the directory stays in TEMP for ever.
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    private string Sub(string name)
    {
        string d = Path.Combine(_dir, name);
        Directory.CreateDirectory(d);
        return d;
    }

    /// <summary>
    /// A logger that throws an <see cref="OutOfMemoryException"/> from the lines a predicate picks —
    /// the deterministic stand-in for a log call that runs out of memory at a chosen point.
    /// </summary>
    private sealed class ThrowingLogger<T>(Func<string, bool> throwOn) : Microsoft.Extensions.Logging.ILogger<T>
    {
        private int _thrown;

        public int Thrown => Volatile.Read(ref _thrown);

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
                                TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!throwOn(formatter(state, exception))) return;
            Interlocked.Increment(ref _thrown);
            throw new OutOfMemoryException("test: this log line ran out of memory");
        }
    }

    private static async Task WaitUntil(Func<bool> condition, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) Assert.Fail($"timed out waiting for {what}");
            await Task.Delay(25);
        }
    }

    // ── Span drainer ──────────────────────────────────────────────────────────

    private static void Publish(SpanRingBuffer ring, TraceId trace, ulong spanId)
    {
        var h = new SpanHeader
        {
            TraceId           = trace,
            SpanId            = new SpanId(spanId),
            StartTimeUnixNano = 1_785_000_000_000_000_000L + (long)spanId,
            DurationNanos     = 1_000,
            Kind              = SpanKind.Server,
        };
        Assert.True(ring.TryEnqueueRaw(in h, "GET /guarded"u8, -1, "gateway"u8, []));
        ring.EndBatch();
    }

    /// <summary>
    /// The first batch's write faults and the error line that reports it throws: the loop used to end
    /// right there, and a span published afterwards stayed in the ring, acknowledged and never written.
    /// </summary>
    [Fact]
    public async Task The_span_drain_loop_goes_round_when_its_error_line_throws()
    {
        var pools = new SpanStringPools();
        using var ring   = new SpanRingBuffer(capacity: 1_024, maxBytes: 8 * 1024 * 1024, pools);
        using var engine = new TraceStorageEngine(Sub("drainer"), NullLogger<TraceStorageEngine>.Instance, false, true, null, pools);
        var logger = new ThrowingLogger<SpanDrainer>(static line => line.Contains("error writing a drained batch"));
        var first  = new TraceId(0xD137, 1);
        var second = new TraceId(0xD137, 2);

        int writes = 0;
        engine._afterWriteHoldForTest = _ =>
        {
            if (Interlocked.Increment(ref writes) == 1) throw new InvalidOperationException("write fault (test seam)");
        };
        // `await using` after the ring and the engine: the loop is joined before the ring is freed.
        await using var drainer = new SpanDrainer(ring, engine, logger, startLoop: true);

        Publish(ring, first, 1);
        await WaitUntil(() => logger.Thrown >= 1, TimeSpan.FromSeconds(15), "the error line to throw");

        // A live loop drains this one — in its next pass, or in the final drain the disposal runs.
        // A loop that had died leaves it in the ring: nothing drains a ring whose loop is gone.
        Publish(ring, second, 2);
        var disposal = await Record.ExceptionAsync(() => drainer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Null(disposal);

        int found = 0;
        await foreach (var _ in engine.GetTraceAsync(second)) found++;
        Assert.Equal(1, found);
    }

    // ── Metric engine loops ───────────────────────────────────────────────────

    /// <summary>Two ticks whose flush fails and whose error line throws; a third tick shows the loop alive.</summary>
    [Fact]
    public async Task The_metric_flush_loop_goes_round_when_its_error_line_throws()
    {
        var logger = new ThrowingLogger<MetricStorageEngine>(static line => line.Contains("Periodic metric flush failed"));
        var engine = new MetricStorageEngine(Sub("flush"), logger,
            new MetricsOptions { FlushCheckInterval = TimeSpan.FromMilliseconds(50) });
        await engine.ColdLoadCompleted;

        int ticks = 0;
        engine._beforePeriodicFlushForTest = () =>
        {
            if (Interlocked.Increment(ref ticks) <= 2) throw new InvalidOperationException("flush fault (test seam)");
        };

        Exception? disposal;
        try
        {
            await WaitUntil(() => logger.Thrown >= 2, TimeSpan.FromSeconds(20), "two error lines to throw");
            await WaitUntil(() => Volatile.Read(ref ticks) >= 3, TimeSpan.FromSeconds(20), "a third flush tick");
            Assert.False(engine.FlushLoopForTest.IsCompleted, "the flush loop ended");
        }
        finally   // a fact that fails must not leave a 50 ms loop and an open metric log in the test host
        {
            disposal = await Record.ExceptionAsync(() => engine.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30)));
        }
        Assert.Null(disposal);
    }

    [Fact]
    public async Task The_metric_rollup_loop_goes_round_when_its_error_line_throws()
    {
        var logger = new ThrowingLogger<MetricStorageEngine>(static line => line.Contains("Metric rollup error"));
        MetricStorageEngine engine;
        MetricStorageEngine.RollupIntervalForTest.Value = TimeSpan.FromMilliseconds(50);
        try { engine = new MetricStorageEngine(Sub("rollup"), logger); }
        finally { MetricStorageEngine.RollupIntervalForTest.Value = null; }
        await engine.ColdLoadCompleted;

        int passes = 0;
        engine._beforeRollupForTest = () =>
        {
            if (Interlocked.Increment(ref passes) <= 2) throw new InvalidOperationException("rollup fault (test seam)");
        };

        Exception? disposal;
        try
        {
            await WaitUntil(() => logger.Thrown >= 2, TimeSpan.FromSeconds(20), "two error lines to throw");
            await WaitUntil(() => Volatile.Read(ref passes) >= 3, TimeSpan.FromSeconds(20), "a third rollup pass");
            Assert.False(engine.RollupLoopForTest.IsCompleted, "the rollup loop ended");
        }
        finally
        {
            disposal = await Record.ExceptionAsync(() => engine.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30)));
        }
        Assert.Null(disposal);
    }

    // ── Segment-delete retry loop ─────────────────────────────────────────────

    private static readonly NodeId Peer = new(7);

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
    private static (string Path, SegmentKey Key) ImportPeerSegment(StorageEngine engine, string segDir, ulong segId)
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

        Directory.CreateDirectory(segDir);
        string path = Path.Combine(segDir, $"{Peer.Value}-{segId}.seg");
        using (var writer = new SegmentWriter(path))
        {
            writer.WriteEvents(hot, pool);
            writer.Finalise(Peer, new SegmentId(segId));
        }
        Assert.Equal(SegmentImportOutcome.Registered, engine.ImportSegment(path));
        return (path, new SegmentKey(Peer, new SegmentId(segId)));
    }

    /// <summary>
    /// A pass fails and its warning throws: the loop used to end with its running flag still set, so
    /// no loop ever retried the parked delete again — the file stayed until the next maintenance or
    /// retention pass. Windows only: elsewhere the unlink succeeds under an open reader and nothing is parked.
    /// </summary>
    [Fact]
    public async Task The_segment_delete_retry_loop_goes_round_when_its_warning_throws()
    {
        if (!OperatingSystem.IsWindows()) return;

        string dir    = Sub("retry");
        var    logger = new ThrowingLogger<StorageEngine>(static line => line.Contains("Deferred segment delete pass failed"));
        var engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = dir }),
            new RetentionStore(new ServerOptions { DataDirectory = dir }, NullLogger<RetentionStore>.Instance),
            logger)
        {
            SegmentDeleteRetryInitialDelay = TimeSpan.FromMilliseconds(50),
        };
        try
        {
            await engine.CatalogLoaded;
            var (path, key) = ImportPeerSegment(engine, Path.Combine(dir, "segments"), 11);

            int passes = 0;
            engine._beforePendingDeleteCatalogCheck = _ =>
            {
                if (Interlocked.Increment(ref passes) == 1) throw new InvalidOperationException("pass fault (test seam)");
            };

            var reader = SegmentReader.Open(path);   // a query mid-flight: the unlink fails and the path is parked
            try
            {
                await engine.DeleteSegmentAsync(key);
                Assert.Equal(1, engine.PendingSegmentDeleteCount);
                await WaitUntil(() => logger.Thrown >= 1, TimeSpan.FromSeconds(15), "the warning to throw");
            }
            finally { reader.Dispose(); }

            // The reader is gone: a loop still turning completes the delete within a few passes. The
            // loop that died with its flag set never did — and nothing started another.
            await WaitUntil(() => engine.PendingSegmentDeleteCount == 0, TimeSpan.FromSeconds(20), "the parked delete to complete");
            Assert.False(File.Exists(path));
        }
        finally
        {
            await engine.DisposeAsync();
        }
    }
}
