using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// The write path that replaced the ingest ring: a batch is written into the hot tier AND its WAL
/// before <see cref="StorageEngine.WriteBatchAsync"/> returns, so what a request acknowledges
/// survives the death of the process; when the flush is behind and no flush slot is free the
/// engine SPILLS into WAL files of their own and despills them into segments later; and when it
/// cannot do even that, a batch waits for room and then says what it could not write.
///
/// <para>The flush is held behind on purpose, not raced: the engine gets one flush slot (a
/// 64 MB host's budgets and a width of one), and the first tier's flush parks at the seam after
/// its first level is published, holding that slot for as long as a test needs.</para>
/// </summary>
public sealed class IngestWriteAndSpillTests : IDisposable
{
    private const long MB = 1024 * 1024;

    /// <summary>One chunk: 16 384 events fill it by count, so "tier full" is a number the test controls.</summary>
    private const long TierBytes   = 8 * MB;
    private const int  TierEvents  = HotTierSegment.ChunkEventCapacity;

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(60);

    private readonly string _root = Path.Combine(Path.GetTempPath(), "ameto-ingest-spill-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch { /* best-effort */ }
    }

    private string NewDir()
    {
        string dir = Path.Combine(_root, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static ServerOptions Options(string dir, bool spill = true, long tierBytes = TierBytes) => new()
    {
        DataDirectory = dir,
        HotTier = new HotTierOptions
        {
            MaxSizeBytes          = tierBytes,
            FlushConcurrency      = 1,
            SpillEnabled          = spill,
            SpillMinFreeDiskBytes = 0,
        },
    };

    /// <summary>An engine with exactly one flush slot: a 64 MB host's 16 MB native floor holds one 9 MB tier.</summary>
    private static StorageEngine OneSlotEngine(ServerOptions opts, Microsoft.Extensions.Logging.ILogger<StorageEngine>? logger = null)
    {
        var engine = new StorageEngine(
            Microsoft.Extensions.Options.Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            logger ?? NullLogger<StorageEngine>.Instance,
            MemoryBudgets.Derive(managedLimitBytes: 64 * MB, physicalLimitBytes: 64 * MB));
        Assert.Equal(1, engine.FlushSlots);
        return engine;
    }

    /// <summary>How long a test waits for something the fix makes happen at once, so a regression fails in seconds, not a minute.</summary>
    private static readonly TimeSpan ShortGuard = TimeSpan.FromSeconds(15);

    /// <summary>
    /// A logger that throws an <see cref="OutOfMemoryException"/> from the lines a predicate picks —
    /// the deterministic stand-in for a log call that runs out of memory at a chosen point.
    /// </summary>
    private sealed class ThrowingLogger(Func<string, bool> throwOn) : Microsoft.Extensions.Logging.ILogger<StorageEngine>
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

    private static StorageEngine Engine(ServerOptions opts) => new(
        Microsoft.Extensions.Options.Options.Create(opts),
        new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
        NullLogger<StorageEngine>.Instance);

    /// <summary>Parks the FIRST flush after its first level is published — holding its slot — until released.</summary>
    private static ManualResetEventSlim HoldFirstFlush(StorageEngine engine)
    {
        var gate  = new ManualResetEventSlim(false);
        int armed = 1;
        engine._afterLevelPublished = _ =>
        {
            if (Interlocked.Exchange(ref armed, 0) == 1) gate.Wait(HangGuard);
        };
        return gate;
    }

    private static byte[] Props(long n)
    {
        var buf = new ArrayBufferWriter<byte>(32);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write(n);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private static readonly long T0 = DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond * TimeSpan.TicksPerMillisecond;

    /// <summary>
    /// Events <paramref name="from"/>..<paramref name="from"/>+<paramref name="count"/>, one per
    /// millisecond, each carrying its number, a trace context derived from it and one service.
    /// </summary>
    private static LogWriteBatch Batch(StorageEngine engine, int from, int count)
    {
        var batch = new LogWriteBatch();
        int tmpl  = engine.TemplatePool.Intern("event {n}");
        int svc   = engine.TemplatePool.Intern("spill.svc");
        for (int i = from; i < from + count; i++)
            batch.Add(new LogEventHeader
            {
                TimestampUtcTicks        = T0 + i * TimeSpan.TicksPerMillisecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = tmpl,
                ServiceNamePoolIndex     = svc,
                TraceIdHi                = 0xA000_0000_0000_0000UL | (uint)i,
                TraceIdLo                = (ulong)i * 7 + 1,
                SpanId                   = (ulong)i * 13 + 1,
            }, Props(i), "event {n}", exception: null);
        return batch;
    }

    /// <summary>Every event in the catalog's segments, by its number, after asserting none is there twice.</summary>
    private static Dictionary<long, RawSegmentEvent> ReadAll(StorageEngine engine)
    {
        var dedup = new Dictionary<string, string>(StringComparer.Ordinal);
        var byN   = new Dictionary<long, RawSegmentEvent>();
        foreach (var seg in engine.ListSegments())
        {
            using var r = SegmentReader.Open(seg.FilePath);
            foreach (var ev in r.ReadAllRaw(dedup))
            {
                long n = (ev.TsTicks - T0) / TimeSpan.TicksPerMillisecond;
                Assert.True(byN.TryAdd(n, ev), $"event {n} is in the segments twice");
            }
        }
        return byN;
    }

    private static void AssertIsEvent(long n, RawSegmentEvent ev)
    {
        Assert.Equal("event {n}", ev.Template);
        Assert.Equal("spill.svc", ev.Service);
        Assert.Equal(0xA000_0000_0000_0000UL | (ulong)n, ev.TraceIdHi);
        Assert.Equal((ulong)n * 7 + 1,  ev.TraceIdLo);
        Assert.Equal((ulong)n * 13 + 1, ev.SpanId);
    }

    private static void WaitFor(Func<bool> condition, string what, TimeSpan? within = null)
    {
        Assert.True(SpinWait.SpinUntil(condition, within ?? HangGuard), $"timed out waiting for {what}");
    }

    // ── Acknowledged means durable ────────────────────────────────────────────

    /// <summary>
    /// A batch is in the WAL when WriteBatchAsync returns — no drainer, no buffer in between. Killed
    /// right after (the WAL kept, the shutdown flush undone), the next start replays every event,
    /// with its trace context: WAL v6 keeps it, where every earlier format came back without one.
    /// </summary>
    [Fact]
    public async Task A_written_batch_survives_a_kill_with_its_trace_context()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        var engine  = Engine(opts);
        try
        {
            using var batch = Batch(engine, 0, 500);
            var r = await engine.WriteBatchAsync(batch, TimeSpan.FromSeconds(5));
            Assert.Equal(new LogBatchWriteResult(500, 0, 0), r);

            // kill -9: keep the live WAL's bytes, let shutdown flush and unlink it, then undo that flush.
            ulong  walId  = engine.LiveWalSegmentId;
            string walDir = Path.Combine(dir, "wal");
            string wal    = Directory.GetFiles(walDir, "*.wal").Single();
            File.Copy(wal, wal + ".crash", overwrite: true);
            File.Copy(wal + ".pool", wal + ".pool.crash", overwrite: true);
            await engine.DisposeAsync();
            foreach (var f in Directory.GetFiles(Path.Combine(dir, "segments"), "*.seg"))
            {
                var parts = Path.GetFileNameWithoutExtension(f).Split('-');
                if (ulong.TryParse(parts[1], out var id) && id >= walId && id < walId + 6) File.Delete(f);
            }
            File.Move(wal + ".crash", wal, overwrite: true);
            File.Move(wal + ".pool.crash", wal + ".pool", overwrite: true);
        }
        finally { await engine.DisposeAsync(); }

        await using var restarted = Engine(opts);
        await restarted.CatalogLoaded;
        var events = ReadAll(restarted);
        Assert.Equal(500, events.Count);
        foreach (var (n, ev) in events) AssertIsEvent(n, ev);
    }

    /// <summary>
    /// A WAL holds what the tier that wrote it held, and a restart may run a smaller tier. Replayed
    /// into a tier sized from the configuration alone, the WAL's tail was refused, and a refused
    /// event was simply not counted as replayed. The recovery tier is sized from the WAL.
    /// </summary>
    [Fact]
    public async Task A_wal_written_under_a_larger_tier_replays_whole_under_a_smaller_one()
    {
        string dir    = NewDir();
        const int Many = TierEvents + 3_000;            // more than the smaller tier takes
        var engine    = Engine(Options(dir, tierBytes: 2 * TierBytes));
        try
        {
            using var batch = Batch(engine, 0, Many);
            Assert.Equal(Many, (await engine.WriteBatchAsync(batch, TimeSpan.FromSeconds(5))).Written);

            ulong  walId  = engine.LiveWalSegmentId;
            string wal    = Directory.GetFiles(Path.Combine(dir, "wal"), "*.wal").Single();
            File.Copy(wal, wal + ".crash", overwrite: true);
            File.Copy(wal + ".pool", wal + ".pool.crash", overwrite: true);
            await engine.DisposeAsync();
            foreach (var f in Directory.GetFiles(Path.Combine(dir, "segments"), "*.seg"))
            {
                var parts = Path.GetFileNameWithoutExtension(f).Split('-');
                if (ulong.TryParse(parts[1], out var id) && id >= walId && id < walId + 6) File.Delete(f);
            }
            File.Move(wal + ".crash", wal, overwrite: true);
            File.Move(wal + ".pool.crash", wal + ".pool", overwrite: true);
        }
        finally { await engine.DisposeAsync(); }

        await using var restarted = Engine(Options(dir, tierBytes: TierBytes));
        await restarted.CatalogLoaded;
        Assert.Equal(Many, ReadAll(restarted).Count);
    }

    // ── Spill ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// The flush is behind (its one slot held), the live tier full: the batch does not wait and is
    /// not dropped — it goes on into spill files, rotating as each takes a tier's worth. Once the
    /// slot is free the spill ends, every spilled block is written into segments, and every event
    /// is there exactly once, with its template, service and trace context.
    /// </summary>
    [Fact]
    public async Task With_every_flush_slot_busy_a_batch_spills_and_every_event_reaches_the_segments_once()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir));
        var    gate   = HoldFirstFlush(engine);
        const int Total = 4 * TierEvents + 1_000;        // A, B in tiers; two spill files and a bit
        try
        {
            using (var batch = Batch(engine, 0, Total))
            {
                var r = await engine.WriteBatchAsync(batch, HangGuard);
                Assert.Equal(new LogBatchWriteResult(Total, 0, 0), r);
            }

            Assert.True(engine.IsSpilling, "the batch was written without the engine spilling");
            Assert.True(engine.SpilledEvents >= 2 * TierEvents, $"only {engine.SpilledEvents} event(s) spilled");
            Assert.True(engine.SpillFilesOpened >= 2, "a spill file that took a tier's worth was not rotated");

            gate.Set();
            WaitFor(() => !engine.IsSpilling && engine.SpillFilesPending == 0
                       && engine.DespilledEvents == engine.SpilledEvents,
                    "the spill to end and every spilled block to be despilled");
            Assert.Empty(Directory.GetFiles(Path.Combine(dir, "wal"), "*" + StorageEngine.SpillExtension));

            await engine.FlushHotTierAsync();
            var events = ReadAll(engine);
            Assert.Equal(Total, events.Count);
            foreach (var (n, ev) in events) AssertIsEvent(n, ev);
        }
        finally
        {
            gate.Set();
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// A spill file the process died with is despilled by the next start, in the background, and a
    /// block an earlier life had already written out in full (its completion marker on disk) is
    /// deleted rather than written twice.
    /// </summary>
    [Fact]
    public async Task Spill_files_an_earlier_run_left_are_despilled_at_the_next_start()
    {
        string dir     = NewDir();
        var    opts    = Options(dir);
        string walDir  = Path.Combine(dir, "wal");
        Directory.CreateDirectory(walDir);

        // As a spill writes them: a WAL of its own, its strings in its own pool file, numbered by
        // a pool this process never had.
        const ulong Block = 9_000, FlushedBlock = 9_006;
        string spill = Path.Combine(walDir, $"{opts.NodeId.Value}-{Block}{StorageEngine.SpillExtension}");
        using (var wal = WriteAheadLog.Open(spill, opts.NodeId, new SegmentId(Block), 1 << 20))
        {
            for (int i = 0; i < 300; i++)
                wal.Append(T0 + i * TimeSpan.TicksPerMillisecond, LogLevel.Information, 40, "event {n}", Props(i),
                           serviceIndex: 41, service: "spill.svc",
                           traceIdHi: 0xA000_0000_0000_0000UL | (uint)i, traceIdLo: (ulong)i * 7 + 1, spanId: (ulong)i * 13 + 1);
        }
        string flushed = Path.Combine(walDir, $"{opts.NodeId.Value}-{FlushedBlock}{StorageEngine.SpillExtension}");
        using (var wal = WriteAheadLog.Open(flushed, opts.NodeId, new SegmentId(FlushedBlock), 1 << 20))
            wal.Append(T0 + 999 * TimeSpan.TicksPerMillisecond, LogLevel.Information, 1, "already in segments", Props(999));
        File.WriteAllText(Path.Combine(walDir, $"{opts.NodeId.Value}-{FlushedBlock}.flushed"), "");

        await using var engine = Engine(opts);
        await engine.CatalogLoaded;
        WaitFor(() => engine.DespilledEvents == 300 && engine.SpillFilesPending == 0, "both spill files to be settled");
        WaitFor(() => !File.Exists(flushed) && !File.Exists(spill), "both spill files to be deleted");

        var events = ReadAll(engine);
        Assert.Equal(300, events.Count);
        foreach (var (n, ev) in events) AssertIsEvent(n, ev);
    }

    /// <summary>
    /// The swap that ends a spill commits the frozen tier to its heavy phase: once the tier is
    /// frozen, counted and listed, the flush owns the slot and the count. Ending the spill used to
    /// run inside the try whose catch means "nothing was swapped", so a throw there — the close's
    /// Task.Run out of memory, or the line that says the spill ended — gave the slot back and left
    /// the frozen tier counted, unflushed and unfreed for good. The throw is a logger's here.
    /// </summary>
    [Fact]
    public async Task A_throw_while_ending_a_spill_does_not_orphan_the_tier_the_swap_froze()
    {
        string dir    = NewDir();
        var    logger = new ThrowingLogger(line => line.StartsWith("Spill ended", StringComparison.Ordinal));
        var    engine = OneSlotEngine(Options(dir), logger);
        engine._shutdownWaitBudget = TimeSpan.FromSeconds(5);   // a regression must not hold the test for 30 s
        var    gate   = HoldFirstFlush(engine);
        const int Total = 2 * TierEvents + 1_000;              // A, B in tiers; the rest spilled
        try
        {
            using (var batch = Batch(engine, 0, Total))
                Assert.Equal(new LogBatchWriteResult(Total, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
            Assert.True(engine.IsSpilling, "the batch was written without the engine spilling");

            gate.Set();
            WaitFor(() => logger.Thrown > 0, "the swap that ends the spill to reach the line that throws");
            WaitFor(() => engine.HeavyPhasesInFlight == 0 && !engine.IsSpilling && engine.SpillFilesPending == 0
                       && engine.DespilledEvents == engine.SpilledEvents,
                    "the tier the swap froze to be flushed and the spilled block despilled", ShortGuard);

            await engine.FlushHotTierAsync();
            Assert.Equal(Total, ReadAll(engine).Count);
        }
        finally
        {
            gate.Set();
            await engine.DisposeAsync();
        }
    }

    // ── A WAL that refuses an append ──────────────────────────────────────────

    /// <summary>
    /// An append to the live WAL fails: that event is in the tier, durable only once flushed. Every
    /// later one used to be too — the swap that replaces the WAL was asked for once, the busy slot
    /// dropped the request, and each write after it was answered as written from memory alone until
    /// the tier filled or MaxAge came round. Now the faulted tier takes nothing more: with the only
    /// slot busy the batch waits (spilling off) or spills (on), and the swap that frees the slot
    /// gives the next tier a fresh WAL.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task After_a_wal_append_fails_no_later_event_is_acknowledged_from_memory_alone(bool spill)
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir, spill: spill));
        var    gate   = HoldFirstFlush(engine);
        try
        {
            // Tier A full and swapped, its flush holding the only slot; B is live.
            using (var a = Batch(engine, 0, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            WaitFor(() => engine.LiveHotTier.Count == 0 && engine.HeavyPhasesInFlight == 1, "tier A to be swapped and held");

            engine.LiveWalForTest!.Dispose();   // B's next append throws

            using (var b = Batch(engine, TierEvents, 10))
            {
                var r = await engine.WriteBatchAsync(b, spill ? HangGuard : TimeSpan.FromMilliseconds(300));
                Assert.Equal(1, engine.IngestWalAppendFailures);   // the first event only: in the tier, not its WAL
                Assert.Equal(1, engine.LiveHotTier.Count);
                if (spill)
                {
                    Assert.Equal(new LogBatchWriteResult(10, 0, 0), r);
                    Assert.Equal(9, engine.SpilledEvents);           // the rest went to a WAL of their own
                }
                else
                    Assert.Equal(new LogBatchWriteResult(1, 0, 9), r);   // the rest waited, and were not written
            }

            gate.Set();
            if (!spill)
            {
                using var rest = Batch(engine, TierEvents + 1, 9);
                Assert.Equal(new LogBatchWriteResult(9, 0, 0), await engine.WriteBatchAsync(rest, HangGuard));
            }
            else
                WaitFor(() => !engine.IsSpilling && engine.SpillFilesPending == 0 && engine.DespilledEvents == 9,
                        "the spill to end and its block to be despilled");

            WaitFor(() => engine.HeavyPhasesInFlight == 0, "tier B's flush to give the only slot back");
            await engine.FlushHotTierAsync();
            var events = ReadAll(engine);
            Assert.Equal(TierEvents + 10, events.Count);
            foreach (var (n, ev) in events) AssertIsEvent(n, ev);
        }
        finally
        {
            gate.Set();
            await engine.DisposeAsync();
        }
    }

    // ── No room at all ────────────────────────────────────────────────────────

    /// <summary>
    /// Spilling off and the one slot busy: there is nowhere for the events after the full tier to go.
    /// The batch waits out its wait and then reports them as not written — a prefix written, the
    /// rest not, nothing skipped — and they are written once the slot is free.
    /// </summary>
    [Fact]
    public async Task With_spilling_off_and_no_slot_a_batch_waits_then_reports_what_it_could_not_write()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir, spill: false));
        var    gate   = HoldFirstFlush(engine);
        try
        {
            using (var batch = Batch(engine, 0, 2 * TierEvents + 10))
            {
                var r = await engine.WriteBatchAsync(batch, TimeSpan.FromMilliseconds(300));
                Assert.Equal(new LogBatchWriteResult(2 * TierEvents, 0, 10), r);
            }
            Assert.False(engine.IsSpilling);
            Assert.Equal(10, engine.IngestNotWritten);

            gate.Set();
            using (var rest = Batch(engine, 2 * TierEvents, 10))
                Assert.Equal(new LogBatchWriteResult(10, 0, 0), await engine.WriteBatchAsync(rest, HangGuard));
        }
        finally
        {
            gate.Set();
            await engine.DisposeAsync();
        }
    }

    /// <summary>A batch waiting for room goes on the moment a slot frees: it is woken, not left to time out.</summary>
    [Fact]
    public async Task A_batch_waiting_for_room_goes_on_as_soon_as_a_slot_frees()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir, spill: false));
        var    gate   = HoldFirstFlush(engine);
        try
        {
            using var batch = Batch(engine, 0, 2 * TierEvents + 10);
            var writing = engine.WriteBatchAsync(batch, HangGuard).AsTask();

            WaitFor(() => engine.IngestRoomWaits > 0 && engine.LiveHotTier.Count == TierEvents, "the batch to fill both tiers and wait");
            Assert.False(writing.IsCompleted);

            gate.Set();
            var r = await writing.WaitAsync(TimeSpan.FromSeconds(20));
            Assert.Equal(new LogBatchWriteResult(2 * TierEvents + 10, 0, 0), r);
        }
        finally
        {
            gate.Set();
            await engine.DisposeAsync();
        }
    }

    /// <summary>A closed engine writes nothing and says so, so the caller answers "retry later".</summary>
    [Fact]
    public async Task A_closed_engine_writes_nothing_and_reports_it_as_not_written()
    {
        var engine = Engine(Options(NewDir()));
        await engine.DisposeAsync();

        using var batch = Batch(engine, 0, 5);
        Assert.Equal(new LogBatchWriteResult(0, 0, 5), await engine.WriteBatchAsync(batch, TimeSpan.FromSeconds(5)));
    }
}
