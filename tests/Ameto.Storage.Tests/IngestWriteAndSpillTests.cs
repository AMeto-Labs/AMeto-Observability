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

    /// <summary>Counts the lines a predicate picks.</summary>
    private sealed class CountingLogger(Func<string, bool> counts) : Microsoft.Extensions.Logging.ILogger<StorageEngine>
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
                                TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (counts(formatter(state, exception))) Interlocked.Increment(ref _count);
        }
    }

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
    /// A spill file is never larger than the space its floor check counted: when its entries weigh
    /// more than the tier's admission does — large exceptions, which it does not weigh — the file
    /// fills first, and the spill rotates to a new one, checked against the floor before it opens.
    /// It used to grow by doubling with no check on the way: a storm of large stack traces took one
    /// block far past the space the floor had allowed for it.
    /// </summary>
    [Fact]
    public async Task A_spill_file_rotates_rather_than_grow_past_the_space_its_floor_check_counted()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir));
        var    gate   = HoldFirstFlush(engine);
        long   cap    = engine.SpillFileCapacityBytes;
        const int First = 2 * TierEvents, Heavy = 150;        // ~100 KB each: ~15 MB, against a ~9 MB file
        var    trace  = new ExceptionInfo { Type = "System.InvalidOperationException", StackTrace = new string('x', 100_000) };
        try
        {
            using (var tiers = Batch(engine, 0, First))
                Assert.Equal(new LogBatchWriteResult(First, 0, 0), await engine.WriteBatchAsync(tiers, HangGuard));
            using (var heavy = new LogWriteBatch())
            {
                int tmpl = engine.TemplatePool.Intern("event {n}");
                for (int i = First; i < First + Heavy; i++)
                    heavy.Add(new LogEventHeader
                    {
                        TimestampUtcTicks = T0 + i * TimeSpan.TicksPerMillisecond, Level = LogLevel.Error,
                        MessageTemplatePoolIndex = tmpl, ServiceNamePoolIndex = -1,
                    }, Props(i), "event {n}", trace);
                Assert.Equal(new LogBatchWriteResult(Heavy, 0, 0), await engine.WriteBatchAsync(heavy, HangGuard));
            }

            Assert.True(engine.SpillFilesOpened >= 2, $"{engine.SpillFilesOpened} spill file(s) for ~15 MB of entries in ~{cap >> 20} MB files");
            foreach (var f in Directory.GetFiles(Path.Combine(dir, "wal"), "*" + StorageEngine.SpillExtension))
                Assert.True(new FileInfo(f).Length <= 32 + cap, $"{Path.GetFileName(f)} grew to {new FileInfo(f).Length:N0} B past its {cap:N0} B");

            gate.Set();
            WaitFor(() => !engine.IsSpilling && engine.SpillFilesPending == 0 && engine.DespilledEvents == Heavy,
                    "the spill to end and its blocks to be despilled");
            WaitFor(() => engine.HeavyPhasesInFlight == 0, "the flushes to end");
            await engine.FlushHotTierAsync();
            var events = ReadAll(engine);
            Assert.Equal(First + Heavy, events.Count);
            Assert.Equal(trace.StackTrace, events[First + Heavy - 1].Exception?.StackTrace);
        }
        finally
        {
            gate.Set();
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// A spill file that cannot be opened — a full volume the floor check cannot see (a UNC path, a
    /// mount folder, a junction) — is reported once and tried again at most once a second. Every
    /// swap attempt used to try again, each waiting writer asking ten times a second, and each
    /// failure was a Warning with its stack.
    /// </summary>
    [Fact]
    public async Task A_spill_open_that_fails_is_reported_once_and_tried_again_at_most_once_a_second()
    {
        string dir    = NewDir();
        var    lines  = new CountingLogger(line => line.StartsWith("Could not open a spill file", StringComparison.Ordinal));
        var    engine = OneSlotEngine(Options(dir), lines);
        var    gate   = HoldFirstFlush(engine);
        int    opens  = 0;
        engine._beforeSpillOpenForTest = () =>
        {
            Interlocked.Increment(ref opens);
            throw new IOException("test: there is not enough space on the disk");
        };
        try
        {
            using (var a = Batch(engine, 0, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            WaitFor(() => engine.LiveHotTier.Count == 0 && engine.HeavyPhasesInFlight == 1, "tier A to be swapped and held");

            engine._roomRecheckMs = 10;   // the waiting writer asks for the swap again a hundred times a second
            using (var b = Batch(engine, TierEvents, TierEvents + 10))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 10), await engine.WriteBatchAsync(b, TimeSpan.FromMilliseconds(500)));

            Assert.Equal(1, lines.Count);
            Assert.True(Volatile.Read(ref opens) <= 2, $"{opens} attempts to open a spill file in half a second");
        }
        finally
        {
            engine._beforeSpillOpenForTest = null;
            gate.Set();
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// A closed spill file is cut to what it holds. It is pre-sized to a tier's worth and closed far
    /// earlier — here at the admission's cap of small events, ~1 MB in a ~9 MB file — and NTFS
    /// allocates the whole pre-size: the closed files of one long spill held several times the disk
    /// their data needed, and reached the spill floor that much sooner.
    /// </summary>
    [Fact]
    public async Task A_closed_spill_file_is_cut_to_what_it_holds()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir));
        var    gate   = HoldFirstFlush(engine);
        long   cap    = engine.SpillFileCapacityBytes;
        const int Total = 3 * TierEvents + 100;               // A, B in tiers; one block closed; a second open
        try
        {
            using (var batch = Batch(engine, 0, Total))
                Assert.Equal(new LogBatchWriteResult(Total, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
            WaitFor(() => engine.SpillFilesPending == 1, "the first spill block to be closed and queued");

            var lengths = Directory.GetFiles(Path.Combine(dir, "wal"), "*" + StorageEngine.SpillExtension)
                                   .Select(f => new FileInfo(f).Length).Order().ToArray();
            Assert.Equal(2, lengths.Length);
            Assert.True(lengths[0] < cap / 4, $"the closed spill file kept {lengths[0]:N0} B of its {cap:N0} B pre-size");
            Assert.Equal(32 + cap, lengths[1]);                // the open one is still pre-sized

            gate.Set();
            WaitFor(() => !engine.IsSpilling && engine.SpillFilesPending == 0 && engine.DespilledEvents == engine.SpilledEvents,
                    "the trimmed blocks to be despilled");
            WaitFor(() => engine.HeavyPhasesInFlight == 0, "the flushes to end");
            await engine.FlushHotTierAsync();
            Assert.Equal(Total, ReadAll(engine).Count);
        }
        finally
        {
            gate.Set();
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// A spilled event's only copy is its spill file, so its template has to be in it — also one
    /// with no pool row: outside a saturated pool (index -1, its text attached, as the receivers
    /// stage it), or longer than a row's 65 535 bytes. Those used to be despilled with an empty
    /// template, or one cut at 65 535 bytes, while the same events that reached the live tier kept
    /// theirs — for OTLP, whose log body is the template, the whole message lost.
    /// </summary>
    [Fact]
    public async Task A_spilled_event_keeps_a_template_that_has_no_pool_row()
    {
        string dir      = NewDir();
        var    engine   = OneSlotEngine(Options(dir));
        var    gate     = HoldFirstFlush(engine);
        string unpooled = "user 4711 logged in from 10.0.0.7";
        string longTmpl = new string('é', 40_000) + " {n}";   // 80 004 UTF-8 bytes: more than a pool row holds
        int    longIdx  = engine.TemplatePool.Intern(longTmpl);
        const int First = 2 * TierEvents;                      // after A and B, in the tiers
        try
        {
            using (var tiers = Batch(engine, 0, First))
                Assert.Equal(new LogBatchWriteResult(First, 0, 0), await engine.WriteBatchAsync(tiers, HangGuard));
            using (var spilled = new LogWriteBatch())
            {
                spilled.Add(new LogEventHeader
                {
                    TimestampUtcTicks = T0 + First * TimeSpan.TicksPerMillisecond, Level = LogLevel.Information,
                    MessageTemplatePoolIndex = -1, ServiceNamePoolIndex = -1,
                }, Props(First), unpooled, exception: null);
                spilled.Add(new LogEventHeader
                {
                    TimestampUtcTicks = T0 + (First + 1) * TimeSpan.TicksPerMillisecond, Level = LogLevel.Information,
                    MessageTemplatePoolIndex = longIdx, ServiceNamePoolIndex = -1,
                }, Props(First + 1), longTmpl, exception: null);
                // The same long template again: the spill file holds it once, in the entry above, and
                // this one's entry names its index alone.
                spilled.Add(new LogEventHeader
                {
                    TimestampUtcTicks = T0 + (First + 2) * TimeSpan.TicksPerMillisecond, Level = LogLevel.Information,
                    MessageTemplatePoolIndex = longIdx, ServiceNamePoolIndex = -1,
                }, Props(First + 2), longTmpl, exception: null);
                Assert.Equal(new LogBatchWriteResult(3, 0, 0), await engine.WriteBatchAsync(spilled, HangGuard));
            }
            Assert.Equal(3, engine.SpilledEvents);

            gate.Set();
            WaitFor(() => !engine.IsSpilling && engine.SpillFilesPending == 0 && engine.DespilledEvents == 3,
                    "the spill to end and its block to be despilled");

            var events = ReadAll(engine);
            Assert.Equal(unpooled, events[First].Template);
            Assert.Equal(longTmpl, events[First + 1].Template);
            Assert.Equal(longTmpl, events[First + 2].Template);
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

    /// <summary>
    /// The swap that ends a spill is asked for once, by the flush that frees a slot, and a swap lock
    /// held at that instant used to drop the request. Here the swap that opens the spill is still in
    /// its floor check, holding the lock, when tier A's flush frees the only slot. While spilling
    /// every write succeeds, so no writer asks again, and the engine went on spilling beside an idle
    /// slot (reproduced in review: still spilling 12 s later). The request is asked again when the
    /// lock is released. The writer's re-check is lengthened so that no re-poll of its own can stand
    /// in for the request.
    /// </summary>
    [Fact]
    public async Task A_slot_freed_while_the_swap_lock_is_held_still_ends_the_spill()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir));
        var    gate   = HoldFirstFlush(engine);
        using var inFloorCheck    = new ManualResetEventSlim(false);
        using var leaveFloorCheck = new ManualResetEventSlim(false);
        try
        {
            using (var a = Batch(engine, 0, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            WaitFor(() => engine.LiveHotTier.Count == 0 && engine.HeavyPhasesInFlight == 1, "tier A to be swapped and held");

            engine._roomRecheckMs = (int)HangGuard.TotalMilliseconds;
            // The swap that opens the spill parks in its floor check, holding the swap lock.
            engine._spillFreeDiskForTest = () =>
            {
                inFloorCheck.Set();
                leaveFloorCheck.Wait(HangGuard);
                return long.MaxValue;
            };
            using var b = Batch(engine, TierEvents, TierEvents + 100);
            var writing = engine.WriteBatchAsync(b, HangGuard).AsTask();
            Assert.True(inFloorCheck.Wait(HangGuard), "tier B's swap never reached the spill's floor check");

            // A's flush frees the only slot, and the swap it asks for meets the held lock.
            gate.Set();
            WaitFor(() => engine.HeavyPhasesInFlight == 0 && engine.InFlightFlushTasks().Count(t => !t.IsCompleted) == 1,
                    "A's flush to end, and the swap it asked for to find the lock held");

            leaveFloorCheck.Set();
            Assert.Equal(new LogBatchWriteResult(TierEvents + 100, 0, 0), await writing.WaitAsync(HangGuard));
            WaitFor(() => !engine.IsSpilling, "the spill to end, the only slot being free", ShortGuard);
        }
        finally
        {
            gate.Set();
            leaveFloorCheck.Set();
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// A slot a despill gives back goes to the live tier when the live tier is refusing writes and
    /// cannot spill (here: the disk at the spill floor). The despill loop used to yield only to an
    /// installed spill, and after each despill it took the next slot inline, ahead of the swap the
    /// release had queued: the waiting batch sat behind the whole backlog. The swap is slowed a
    /// little here so that the loop, if it does not yield, certainly gets there first.
    /// </summary>
    [Fact]
    public async Task A_slot_a_despill_frees_goes_to_a_live_tier_that_is_refusing_writes()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir));
        var    gate   = HoldFirstFlush(engine);
        using var firstDespill = new ManualResetEventSlim(false);
        using var laterDespills = new ManualResetEventSlim(false);
        using var despillHeld  = new ManualResetEventSlim(false);
        engine._roomRecheckMs = (int)HangGuard.TotalMilliseconds;   // only a real swap gets the writer going
        try
        {
            // A held in its flush; B full; one spill block closed and queued, a second one open.
            using (var a = Batch(engine, 0, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            WaitFor(() => engine.LiveHotTier.Count == 0 && engine.HeavyPhasesInFlight == 1, "tier A to be swapped and held");
            using (var b = Batch(engine, TierEvents, 2 * TierEvents + 100))
                Assert.Equal(new LogBatchWriteResult(2 * TierEvents + 100, 0, 0), await engine.WriteBatchAsync(b, HangGuard));
            WaitFor(() => engine.SpillFilesPending == 1, "the first spill block to be closed and queued");

            int despills = 0;
            engine._beforeLoadSpillForTest = _ =>
            {
                despillHeld.Set();
                (Interlocked.Increment(ref despills) == 1 ? firstDespill : laterDespills).Wait(HangGuard);
            };
            engine._spillFreeDiskForTest = () => 0;   // at the floor from here: no new spill file

            // The slot frees: the swap ends the spill (B frozen), B's flush ends, and the first despill
            // takes the slot and holds it.
            gate.Set();
            Assert.True(despillHeld.Wait(HangGuard), "the first despill never started");

            // C fills and refuses: no slot, no spill. The writer waits.
            using var c = Batch(engine, 3 * TierEvents + 100, TierEvents + 10);
            var writing = engine.WriteBatchAsync(c, HangGuard).AsTask();
            WaitFor(() => engine.LiveHotTierRefused && engine.LiveHotTier.Count == TierEvents, "tier C to refuse the writer");

            engine._beforeSwap = () => Thread.Sleep(100);
            firstDespill.Set();   // its slot comes free: the swap must get it, not the next despill

            Assert.Equal(new LogBatchWriteResult(TierEvents + 10, 0, 0), await writing.WaitAsync(ShortGuard));
        }
        finally
        {
            gate.Set();
            firstDespill.Set();
            laterDespills.Set();
            await engine.DisposeAsync();
        }
    }

    // ── A swap that fails ─────────────────────────────────────────────────────

    /// <summary>One event outside every batch here, for a write that only has to be refused.</summary>
    private static bool TryWriteOne(StorageEngine engine, int n) => engine.TryWrite(new LogEventHeader
    {
        TimestampUtcTicks        = T0 + n * TimeSpan.TicksPerMillisecond,
        Level                    = LogLevel.Information,
        MessageTemplatePoolIndex = engine.TemplatePool.Intern("event {n}"),
        ServiceNamePoolIndex     = engine.TemplatePool.Intern("spill.svc"),
    }, Props(n), "event {n}");

    /// <summary>
    /// The despill does not yield its slot to a swap that cannot install. It yields to a live tier
    /// that refuses writes, so the swap gets the slot first — but the swap here keeps failing (the
    /// stand-in for an NTFS volume too full for the next 64 MB WAL), so the slot sat idle and the
    /// backlog, whose despill is what would free that disk, stayed pending for as long as it failed.
    /// </summary>
    [Fact]
    public async Task A_despill_is_not_starved_by_a_swap_that_keeps_failing()
    {
        string dir    = NewDir();
        var    opts   = Options(dir, spill: false);
        string walDir = Path.Combine(dir, "wal");
        Directory.CreateDirectory(walDir);

        // The backlog: a block an earlier run spilled.
        const ulong Block = 9_000;
        const int   Spilled = 300, From = 100_000;
        string spill = Path.Combine(walDir, $"{opts.NodeId.Value}-{Block}{StorageEngine.SpillExtension}");
        using (var wal = WriteAheadLog.Open(spill, opts.NodeId, new SegmentId(Block), 1 << 20))
            for (int i = From; i < From + Spilled; i++)
                wal.Append(T0 + i * TimeSpan.TicksPerMillisecond, LogLevel.Information, 40, "event {n}", Props(i),
                           serviceIndex: 41, service: "spill.svc");

        var hold = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        StorageEngine engine;
        try { engine = OneSlotEngine(opts); }
        finally { StorageEngine.HoldDespillForTest.Value = null; }
        int swaps = 0;
        engine._beforeSwap = () =>
        {
            Interlocked.Increment(ref swaps);
            throw new IOException("test: no room on the volume for the next WAL");
        };
        try
        {
            using (var a = Batch(engine, 0, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            Assert.False(TryWriteOne(engine, TierEvents));   // the full tier refuses it
            WaitFor(() => Volatile.Read(ref swaps) >= 1 && engine.LiveHotTierRefused, "the swap to fail");

            hold.SetResult();   // the despill loop starts, with the only slot free and the swap failing
            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == Spilled,
                    "the backlog to be despilled while the swap fails", ShortGuard);
        }
        finally
        {
            hold.TrySetResult();
            engine._beforeSwap = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// A swap that failed asks for itself again. It used to be asked for again only by a writer that
    /// came back to find the tier still refusing — so once the requests had given up (503), nothing
    /// did, and the tier went on refusing after the failure had cleared, until MaxAge.
    /// </summary>
    [Fact]
    public async Task A_swap_that_failed_is_tried_again_with_no_write_asking()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir, spill: false));
        engine._swapRetryDelay = TimeSpan.FromMilliseconds(100);
        int failures = 2;
        engine._beforeSwap = () =>
        {
            if (Interlocked.Decrement(ref failures) >= 0) throw new IOException("test: no room on the volume for the next WAL");
        };
        try
        {
            using (var a = Batch(engine, 0, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            Assert.False(TryWriteOne(engine, TierEvents));   // refused; nothing writes from here on

            WaitFor(() => !engine.LiveHotTierRefused && engine.LiveHotTier.Count == 0,
                    "the failed swap to be tried again, and install", ShortGuard);
            Assert.True(TryWriteOne(engine, TierEvents));
        }
        finally
        {
            engine._beforeSwap = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// The same for the swap that ends a spill: asked for once, by the flush that freed the slot, and
    /// lost when it failed — the spill went on beside an idle slot until its block filled or MaxAge.
    /// </summary>
    [Fact]
    public async Task A_swap_that_would_end_a_spill_and_fails_is_tried_again()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir));
        var    gate   = HoldFirstFlush(engine);
        engine._swapRetryDelay = TimeSpan.FromMilliseconds(100);
        const int Total = 2 * TierEvents + 1_000;   // A, B in tiers; the rest spilled
        try
        {
            using (var batch = Batch(engine, 0, Total))
                Assert.Equal(new LogBatchWriteResult(Total, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
            Assert.True(engine.IsSpilling);

            int failures = 1;
            engine._beforeSwap = () =>
            {
                if (Interlocked.Decrement(ref failures) >= 0) throw new IOException("test: no room on the volume for the next WAL");
            };
            gate.Set();   // A's flush ends; the swap that would end the spill fails once

            WaitFor(() => Volatile.Read(ref failures) <= 0, "the swap that ends the spill to fail");
            WaitFor(() => !engine.IsSpilling, "the spill to end on the retried swap", ShortGuard);
            Assert.True(Volatile.Read(ref failures) < 0, "the spill ended without the swap being tried again");
        }
        finally
        {
            gate.Set();
            engine._beforeSwap = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// A swap that failed is not tried again by every waiting writer's re-check — ten a second per
    /// writer, each one creating a 64 MB WAL on the disk that has just refused one — but about once
    /// per retry delay.
    /// </summary>
    [Fact]
    public async Task A_failing_swap_is_tried_about_once_per_retry_delay_not_by_every_waiting_writer()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir, spill: false));
        engine._swapRetryDelay = TimeSpan.FromMilliseconds(250);
        engine._roomRecheckMs  = 10;
        int attempts = 0;
        engine._beforeSwap = () =>
        {
            Interlocked.Increment(ref attempts);
            throw new IOException("test: no room on the volume for the next WAL");
        };
        try
        {
            using (var a = Batch(engine, 0, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            using (var b = Batch(engine, TierEvents, 10))
                Assert.Equal(new LogBatchWriteResult(0, 0, 10), await engine.WriteBatchAsync(b, TimeSpan.FromSeconds(1)));

            // About 1 s of waiting at a 10 ms re-check: a hundred requests for the swap. At most one
            // attempt per 250 ms gets through, plus the first and the one each retry makes.
            Assert.InRange(Volatile.Read(ref attempts), 1, 12);
        }
        finally
        {
            engine._beforeSwap = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// A despill whose read of the spilled block fails — out of memory materialising it, the file
    /// held by a scanner — is tried again, and its block counts as pending meanwhile. The loop used
    /// to take the block off the queue before reading it, so a failed read left the block in no
    /// queue: its acknowledged events unsearchable, and counted nowhere, until the next start.
    /// </summary>
    [Fact]
    public async Task A_despill_whose_read_fails_is_tried_again_and_stays_pending_meanwhile()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir));
        var    gate   = HoldFirstFlush(engine);
        engine._despillRetryDelay = TimeSpan.FromSeconds(1);
        int reads = 0;
        engine._beforeLoadSpillForTest = _ =>
        {
            if (Interlocked.Increment(ref reads) == 1) throw new OutOfMemoryException("test: no memory to materialise the block");
        };
        const int Total = 2 * TierEvents + 1_000;   // A, B in tiers; the rest spilled
        try
        {
            using (var batch = Batch(engine, 0, Total))
                Assert.Equal(new LogBatchWriteResult(Total, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
            Assert.True(engine.IsSpilling);

            gate.Set();
            WaitFor(() => Volatile.Read(ref reads) >= 1, "the first despill to fail its read");
            Assert.Equal(1, engine.SpillFilesPending);   // still on the books while it waits to be tried again

            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == engine.SpilledEvents,
                    "the block to be despilled on its second try", ShortGuard);
            Assert.Equal(2, Volatile.Read(ref reads));

            WaitFor(() => engine.HeavyPhasesInFlight == 0, "the flushes to end");
            await engine.FlushHotTierAsync();
            Assert.Equal(Total, ReadAll(engine).Count);
        }
        finally
        {
            gate.Set();
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// A despill whose read keeps failing is tried less and less often — the delay doubles with each
    /// failure in a row, up to a cap — and says so with one Error, not one per attempt. Every attempt
    /// reads the whole block again, and a block that cannot fit the heap is an out-of-memory for the
    /// whole process each time: it came every 15 s for ever (the review's probe: 28 attempts in 3 s
    /// at a 100 ms delay), each an Error with its stack. Once the read works the block is despilled,
    /// and the line that says so counts the failures.
    /// </summary>
    [Fact]
    public async Task A_despill_whose_read_keeps_failing_backs_off_and_logs_one_error()
    {
        string dir    = NewDir();
        var    log    = new RecordingLogger();
        var    engine = OneSlotEngine(Options(dir), log);
        var    gate   = HoldFirstFlush(engine);
        engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
        engine._despillRetryCap   = TimeSpan.FromMilliseconds(400);
        int reads = 0, failing = 1;
        engine._beforeLoadSpillForTest = _ =>
        {
            Interlocked.Increment(ref reads);
            if (Volatile.Read(ref failing) == 1) throw new OutOfMemoryException("test: the block does not fit the heap");
        };
        const int Total = 2 * TierEvents + 1_000;   // A, B in tiers; the rest spilled
        try
        {
            using (var batch = Batch(engine, 0, Total))
                Assert.Equal(new LogBatchWriteResult(Total, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
            Assert.True(engine.IsSpilling);

            gate.Set();
            WaitFor(() => Volatile.Read(ref reads) >= 1, "the first despill to fail its read");
            await Task.Delay(TimeSpan.FromSeconds(2));

            // Two seconds at a fixed 50 ms is some forty attempts; doubling from 50 ms to 400 ms is about eight.
            Assert.InRange(Volatile.Read(ref reads), 2, 12);
            Assert.Single(log.Lines, l => l.Level == Microsoft.Extensions.Logging.LogLevel.Error
                                          && l.Line.StartsWith("Despill of", StringComparison.Ordinal));
            Assert.Equal(1, engine.SpillFilesPending);

            Volatile.Write(ref failing, 0);
            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == engine.SpilledEvents,
                    "the block to be despilled once its read works", ShortGuard);
            Assert.Contains(log.Lines, l => l.Level == Microsoft.Extensions.Logging.LogLevel.Information
                                            && l.Line.StartsWith("Despilled block", StringComparison.Ordinal)
                                            && l.Line.Contains("failed attempt(s)", StringComparison.Ordinal));
        }
        finally
        {
            gate.Set();
            await engine.DisposeAsync();
        }
    }

    /// <summary>Every line, with its level.</summary>
    private sealed class RecordingLogger : Microsoft.Extensions.Logging.ILogger<StorageEngine>
    {
        private readonly System.Collections.Concurrent.ConcurrentQueue<(Microsoft.Extensions.Logging.LogLevel Level, string Line)> _lines = new();

        public IReadOnlyList<(Microsoft.Extensions.Logging.LogLevel Level, string Line)> Lines => _lines.ToArray();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
                                TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => _lines.Enqueue((logLevel, formatter(state, exception)));
    }

    /// <summary>
    /// A block being despilled is still pending: logsSpillFilesPending is what the rollback note
    /// tells an operator to wait out, and it read 0 while the last block was still being replayed
    /// and flushed, because the loop took the block off the queue before reading it.
    /// </summary>
    [Fact]
    public async Task A_block_being_despilled_is_still_counted_as_pending()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir));
        var    gate   = HoldFirstFlush(engine);
        using var inDespill = new ManualResetEventSlim(false);
        using var release   = new ManualResetEventSlim(false);
        engine._beforeLoadSpillForTest = _ => { inDespill.Set(); release.Wait(HangGuard); };
        const int Total = 2 * TierEvents + 1_000;
        try
        {
            using (var batch = Batch(engine, 0, Total))
                Assert.Equal(new LogBatchWriteResult(Total, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));

            gate.Set();
            Assert.True(inDespill.Wait(HangGuard), "the despill never started");
            Assert.Equal(1, engine.SpillFilesPending);

            release.Set();
            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == engine.SpilledEvents,
                    "the block to be despilled");
        }
        finally
        {
            gate.Set();
            release.Set();
            await engine.DisposeAsync();
        }
    }

    // ── A write that throws ───────────────────────────────────────────────────

    /// <summary>
    /// A write that throws — the tier out of memory for a chunk — wrote nothing, and is waited out
    /// and tried again within the wait. It used to escape WriteBatchAsync: the request was answered
    /// 500 (gRPC UNKNOWN), which no exporter retries, and the count of what had been written was
    /// lost. The throw here is the write gate's, before anything is written.
    /// </summary>
    [Fact]
    public async Task A_write_that_throws_wrote_nothing_and_is_tried_again_within_the_wait()
    {
        var engine = Engine(Options(NewDir()));
        try
        {
            int calls = 0;
            engine._afterWriteGate = () =>
            {
                if (Interlocked.Increment(ref calls) == 4) throw new OutOfMemoryException("test: no memory for a tier chunk");
            };
            using (var batch = Batch(engine, 0, 10))
                Assert.Equal(new LogBatchWriteResult(10, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
            engine._afterWriteGate = null;
            Assert.Equal(1, engine.IngestWriteFaults);
            Assert.Equal(0, engine.IngestNotWritten);

            await engine.FlushHotTierAsync();
            Assert.Equal(10, ReadAll(engine).Count);   // each once
        }
        finally
        {
            engine._afterWriteGate = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// A write that keeps throwing stops its batch there once the wait has run out: the prefix is
    /// reported written and the rest not written — counted, never thrown — so a request that wrote
    /// nothing is answered 503 and its retry duplicates nothing, and one that wrote a prefix does
    /// not have it written twice by a client retrying a 500.
    /// </summary>
    [Fact]
    public async Task A_write_that_keeps_throwing_stops_its_batch_there_and_reports_the_rest_not_written()
    {
        var engine = Engine(Options(NewDir()));
        try
        {
            int calls = 0;
            engine._afterWriteGate = () =>
            {
                if (Interlocked.Increment(ref calls) >= 4) throw new OutOfMemoryException("test: no memory for a tier chunk");
            };
            using (var batch = Batch(engine, 0, 10))
                Assert.Equal(new LogBatchWriteResult(3, 0, 7), await engine.WriteBatchAsync(batch, TimeSpan.FromMilliseconds(300)));
            Assert.True(engine.IngestWriteFaults >= 2, $"tried {engine.IngestWriteFaults} time(s); it should be tried again within the wait");
            Assert.Equal(7, engine.IngestNotWritten);
        }
        finally
        {
            engine._afterWriteGate = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// A write that keeps faulting slowly — each attempt blocked, as a failing large rent blocks on
    /// the full collection it forces — ends within about its wait plus one attempt, after a few
    /// attempts. Only the pauses between attempts used to count, so here a 500 ms wait took six
    /// 150 ms attempts and ~1.4 s; on a heap at its limit it was ~20 attempts and ~7 s a request.
    /// </summary>
    [Fact]
    public async Task A_write_that_keeps_faulting_slowly_ends_within_its_wait_after_a_few_attempts()
    {
        var engine = Engine(Options(NewDir()));
        try
        {
            int calls = 0, faulted = 0;
            engine._afterWriteGate = () =>
            {
                if (Interlocked.Increment(ref calls) < 4) return;
                Interlocked.Increment(ref faulted);
                Thread.Sleep(150);   // the collection a failing 128 KB rent waits for
                throw new OutOfMemoryException("test: no memory for the template slots");
            };
            var sw = System.Diagnostics.Stopwatch.StartNew();
            using (var batch = Batch(engine, 0, 10))
                Assert.Equal(new LogBatchWriteResult(3, 0, 7), await engine.WriteBatchAsync(batch, TimeSpan.FromMilliseconds(500)));
            sw.Stop();

            Assert.True(Volatile.Read(ref faulted) <= 4, $"{faulted} faulting attempts in a 500 ms wait");
            Assert.True(sw.Elapsed < TimeSpan.FromMilliseconds(500 + 150 + 300), $"the batch took {sw.Elapsed.TotalMilliseconds:F0} ms");
        }
        finally
        {
            engine._afterWriteGate = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// Once an event is in the tier it is written, whatever its bookkeeping does next: a throw
    /// after the commit (here the log line of a failed WAL append) must not be taken for "nothing
    /// written", or the event would be written a second time.
    /// </summary>
    [Fact]
    public async Task A_throw_after_the_commit_is_not_taken_for_a_write_that_failed()
    {
        string dir    = NewDir();
        var    logger = new ThrowingLogger(line => line.StartsWith("WAL append failed", StringComparison.Ordinal));
        var    engine = OneSlotEngine(Options(dir), logger);
        try
        {
            engine.LiveWalForTest!.Dispose();   // the next append fails, and the line that says so throws
            using (var batch = Batch(engine, 0, 5))
                Assert.Equal(new LogBatchWriteResult(5, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
            Assert.Equal(1, logger.Thrown);
            Assert.Equal(0, engine.IngestWriteFaults);

            WaitFor(() => engine.HeavyPhasesInFlight == 0, "the faulted tier's flush");
            await engine.FlushHotTierAsync();
            Assert.Equal(5, ReadAll(engine).Count);   // ReadAll fails on an event written twice
        }
        finally { await engine.DisposeAsync(); }
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
    ///
    /// <para>Tier A is filled and swapped before the timed batch starts, so its 300 ms covers the
    /// wait for a slot and nothing else: writing 16 384 events in a Debug build and tier A's own
    /// swap (a Task.Run that creates a WAL) used to come out of the same budget, and a swap started
    /// late under a loaded test run would have stopped the batch at tier A.</para>
    /// </summary>
    [Fact]
    public async Task With_spilling_off_and_no_slot_a_batch_waits_then_reports_what_it_could_not_write()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir, spill: false));
        var    gate   = HoldFirstFlush(engine);
        try
        {
            using (var a = Batch(engine, 0, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            WaitFor(() => engine.LiveHotTier.Count == 0 && engine.HeavyPhasesInFlight == 1, "tier A to be swapped and held");

            using (var batch = Batch(engine, TierEvents, TierEvents + 10))
            {
                var r = await engine.WriteBatchAsync(batch, TimeSpan.FromMilliseconds(300));
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 10), r);
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

    /// <summary>
    /// The wait is for room, and only the time spent waiting for it counts. A batch whose writing
    /// had already taken longer than its whole wait — here its first write is held up past it, as a
    /// long batch interleaved with others, or a long queue for the writer lock, would be — used to
    /// give up at the first full tier without waiting at all. Now it waits its full wait there.
    /// </summary>
    [Fact]
    public async Task Time_spent_writing_does_not_use_up_the_wait_for_room()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir, spill: false));
        var    gate   = HoldFirstFlush(engine);
        var    wait   = TimeSpan.FromMilliseconds(200);
        try
        {
            using (var a = Batch(engine, 0, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            WaitFor(() => engine.LiveHotTier.Count == 0 && engine.HeavyPhasesInFlight == 1, "tier A to be swapped and held");
            Assert.Equal(0, engine.IngestRoomWaits);

            int first = 1;
            engine._afterWriteGate = () => { if (Interlocked.Exchange(ref first, 0) == 1) Thread.Sleep(wait * 2); };

            using var b = Batch(engine, TierEvents, TierEvents + 10);
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var r  = await engine.WriteBatchAsync(b, wait);
            sw.Stop();

            Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 10), r);   // B full, no slot, no spill
            Assert.Equal(1, engine.IngestRoomWaits);                        // ... and it waited for room first
            Assert.True(sw.Elapsed >= wait * 2 + wait * 0.8, $"gave up after {sw.Elapsed.TotalMilliseconds:F0} ms");
        }
        finally
        {
            gate.Set();
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// A batch waiting for room goes on the moment a slot frees: the freed slot asks for the swap,
    /// and the swap wakes the writer. Its own re-check is lengthened past the test's patience, so
    /// that a lost request (the slot release's) or a lost wake-up (the swap's) fails the test
    /// instead of being covered by a re-poll; and the slot is freed only once tier B has refused
    /// the writer — with tier A merely waiting for its ordinary swap, the slot was never contended.
    /// </summary>
    [Fact]
    public async Task A_batch_waiting_for_room_goes_on_as_soon_as_a_slot_frees()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir, spill: false));
        var    gate   = HoldFirstFlush(engine);
        engine._roomRecheckMs = (int)HangGuard.TotalMilliseconds;
        try
        {
            using var batch = Batch(engine, 0, 2 * TierEvents + 10);
            var writing = engine.WriteBatchAsync(batch, HangGuard).AsTask();

            WaitFor(() => engine.HeavyPhasesInFlight == 1 && engine.LiveHotTier.Count == TierEvents && engine.LiveHotTierRefused,
                    "tier A to be held in its flush and tier B to refuse the writer");
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
