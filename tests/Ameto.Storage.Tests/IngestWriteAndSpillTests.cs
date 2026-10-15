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

    /// <summary>The event's number; and, with <paramref name="pad"/>, that many bytes of padding beside it.</summary>
    private static byte[] Props(long n, int pad = 0)
    {
        var buf = new ArrayBufferWriter<byte>(32 + pad);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(pad > 0 ? 2 : 1);
        w.Write("n"); w.Write(n);
        if (pad > 0) { w.Write("pad"); w.Write(new string((char)('a' + n % 26), pad)); }
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private static readonly long T0 = DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond * TimeSpan.TicksPerMillisecond;

    /// <summary>
    /// Events <paramref name="from"/>..<paramref name="from"/>+<paramref name="count"/>, one per
    /// millisecond, each carrying its number, a trace context derived from it and one service —
    /// and <paramref name="pad"/> bytes of padding in its properties (<see cref="Props"/>).
    /// </summary>
    private static LogWriteBatch Batch(StorageEngine engine, int from, int count, int pad = 0, LogLevel level = LogLevel.Information)
    {
        var batch = new LogWriteBatch();
        int tmpl  = engine.TemplatePool.Intern("event {n}");
        int svc   = engine.TemplatePool.Intern("spill.svc");
        for (int i = from; i < from + count; i++)
            batch.Add(new LogEventHeader
            {
                TimestampUtcTicks        = T0 + i * TimeSpan.TicksPerMillisecond,
                Level                    = level,
                MessageTemplatePoolIndex = tmpl,
                ServiceNamePoolIndex     = svc,
                TraceIdHi                = 0xA000_0000_0000_0000UL | (uint)i,
                TraceIdLo                = (ulong)i * 7 + 1,
                SpanId                   = (ulong)i * 13 + 1,
            }, Props(i, pad), "event {n}", exception: null);
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
    /// A spilled event keeps a <c>@service</c> that has no pool index — the intern pool was full when
    /// it arrived — and a block despilled into a live pool that is full keeps the services its rows
    /// name. A full pool answers -1 for a service; the spill logged such an event with no service,
    /// and the despill dropped the name of every row it could not intern.
    /// </summary>
    [Fact]
    public async Task A_spilled_event_keeps_its_service_when_the_pool_is_full()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir));
        var    gate   = HoldFirstFlush(engine);
        const int First = 2 * TierEvents;   // after A and B, in the tiers
        try
        {
            using (var tiers = Batch(engine, 0, First))
                Assert.Equal(new LogBatchWriteResult(First, 0, 0), await engine.WriteBatchAsync(tiers, HangGuard));

            for (int i = 0; engine.TemplatePool.Intern("filler " + i) >= 0; i++) { }   // the pool is full from here
            using (var spilled = new LogWriteBatch())
            {
                spilled.Add(new LogEventHeader
                {
                    TimestampUtcTicks = T0 + First * TimeSpan.TicksPerMillisecond, Level = LogLevel.Information,
                    MessageTemplatePoolIndex = engine.TemplatePool.Intern("event {n}"), ServiceNamePoolIndex = -1,
                }, Props(First), "event {n}", exception: null, service: "svc.past.saturation");
                spilled.Add(new LogEventHeader
                {
                    TimestampUtcTicks = T0 + (First + 1) * TimeSpan.TicksPerMillisecond, Level = LogLevel.Information,
                    MessageTemplatePoolIndex = engine.TemplatePool.Intern("event {n}"),
                    ServiceNamePoolIndex     = engine.TemplatePool.Intern("spill.svc"),   // pooled before the pool filled
                }, Props(First + 1), "event {n}", exception: null);
                Assert.Equal(new LogBatchWriteResult(2, 0, 0), await engine.WriteBatchAsync(spilled, HangGuard));
            }
            Assert.Equal(2, engine.SpilledEvents);

            gate.Set();
            WaitFor(() => !engine.IsSpilling && engine.SpillFilesPending == 0 && engine.DespilledEvents == 2,
                    "the spill to end and its block to be despilled");

            var events = ReadAll(engine);
            Assert.Equal("svc.past.saturation", events[First].Service);
            Assert.Equal("spill.svc", events[First + 1].Service);
        }
        finally
        {
            gate.Set();
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// A block an earlier run spilled, despilled into a live pool that is full: its services come
    /// from its own pool file's rows, and with no room to intern them they are kept beside the events.
    /// </summary>
    [Fact]
    public async Task A_block_despilled_into_a_full_pool_keeps_its_rows_services()
    {
        string dir    = NewDir();
        var    opts   = Options(dir);
        string walDir = Path.Combine(dir, "wal");
        Directory.CreateDirectory(walDir);
        const ulong Block = 9_000;
        string spill = Path.Combine(walDir, $"{opts.NodeId.Value}-{Block}{StorageEngine.SpillExtension}");
        using (var wal = WriteAheadLog.Open(spill, opts.NodeId, new SegmentId(Block), 1 << 20))
            for (int i = 0; i < 10; i++)
                wal.Append(T0 + i * TimeSpan.TicksPerMillisecond, LogLevel.Information, 40, "event {n}", Props(i),
                           serviceIndex: 41, service: "row.svc");

        var hold = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        StorageEngine engine;
        try { engine = Engine(opts); }
        finally { StorageEngine.HoldDespillForTest.Value = null; }
        try
        {
            for (int i = 0; engine.TemplatePool.Intern("filler " + i) >= 0; i++) { }   // full before the despill
            hold.SetResult();
            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == 10, "the block to be despilled");

            var events = ReadAll(engine);
            Assert.Equal(10, events.Count);
            Assert.All(events.Values, e => Assert.Equal("row.svc", e.Service));
        }
        finally
        {
            hold.TrySetResult();
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
    /// A flushed tier's WAL that cannot be deleted — held by a scanner, on Windows — keeps its
    /// completion marker beside it. The marker used to go regardless, and the next start found the
    /// WAL with none: it replayed the block and wrote again whatever level a merge had since taken
    /// into another segment — the same events twice, for good. With the marker, that start only
    /// deletes the WAL.
    /// </summary>
    [Fact]
    public async Task A_wal_that_cannot_be_deleted_after_its_flush_keeps_its_marker_for_the_next_start()
    {
        string dir    = NewDir();
        var    opts   = Options(dir);
        string walDir = Path.Combine(dir, "wal");
        var    engine = Engine(opts);
        string wal, marker;
        try
        {
            using (var a = Batch(engine, 0, 100))
                Assert.Equal(new LogBatchWriteResult(100, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            ulong block = engine.LiveWalSegmentId;
            wal    = Path.Combine(walDir, $"{opts.NodeId.Value}-{block}.wal");
            marker = Path.Combine(walDir, $"{opts.NodeId.Value}-{block}.flushed");
            engine._deleteWalFile = p =>
            {
                if (p == wal) throw new IOException("test: the file is held by a scanner");
                File.Delete(p);
            };

            await engine.FlushHotTierAsync();

            Assert.True(File.Exists(wal), "the WAL was deleted after all");
            Assert.True(File.Exists(marker), "the marker went though its WAL stayed");
        }
        finally
        {
            engine._deleteWalFile = File.Delete;
            await engine.DisposeAsync();
        }

        await using var next = Engine(opts);
        await next.CatalogLoaded;
        Assert.False(File.Exists(wal));
        Assert.False(File.Exists(marker));
        Assert.Equal(100, ReadAll(next).Count);
    }

    /// <summary>The same for a despilled block whose spill file cannot be deleted.</summary>
    [Fact]
    public async Task A_spill_file_that_cannot_be_deleted_after_its_despill_keeps_its_marker_for_the_next_start()
    {
        string dir    = NewDir();
        var    opts   = Options(dir);
        var    engine = OneSlotEngine(opts);
        var    gate   = HoldFirstFlush(engine);
        engine._deleteWalFile = p =>
        {
            if (p.EndsWith(StorageEngine.SpillExtension, StringComparison.Ordinal)) throw new IOException("test: the file is held by a scanner");
            File.Delete(p);
        };
        const int Total = 2 * TierEvents + 1_000;   // A, B in tiers; the rest spilled
        try
        {
            using (var batch = Batch(engine, 0, Total))
                Assert.Equal(new LogBatchWriteResult(Total, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
            gate.Set();
            WaitFor(() => !engine.IsSpilling && engine.SpillFilesPending == 0 && engine.DespilledEvents == engine.SpilledEvents,
                    "the spilled block to be despilled");

            string spill = Assert.Single(Directory.GetFiles(Path.Combine(dir, "wal"), "*" + StorageEngine.SpillExtension));
            Assert.True(File.Exists(Path.ChangeExtension(spill, ".flushed")), "the marker went though its spill file stayed");
        }
        finally
        {
            gate.Set();
            engine._deleteWalFile = File.Delete;
            await engine.DisposeAsync();
        }

        await using var next = Engine(opts);
        await next.CatalogLoaded;
        WaitFor(() => Directory.GetFiles(Path.Combine(dir, "wal"), "*" + StorageEngine.SpillExtension).Length == 0,
                "the next start to delete the spill file against its marker");
        Assert.Equal(0, next.DespilledEvents);    // deleted, not despilled again
        Assert.Equal(Total, ReadAll(next).Count); // and no event twice
    }

    /// <summary>
    /// The server's engine despills a block an earlier run left only once the indexing layer has
    /// wired its index sink. The despill loop starts with the engine, and the sink is wired by a
    /// hosted service's StartAsync, after every hosted service has been built: a block despilled in
    /// between went into segments with no index sections. (The wait is bounded, and is the server
    /// registration's; an engine built with none despills at once, as every other test here does.)
    /// </summary>
    [Fact]
    public async Task The_servers_engine_despills_a_block_from_an_earlier_run_only_once_the_index_sink_is_wired()
    {
        string dir    = NewDir();
        var    opts   = Options(dir);
        string walDir = Path.Combine(dir, "wal");
        Directory.CreateDirectory(walDir);
        const ulong Block = 9_000;
        const int   Spilled = 300;
        string spill = Path.Combine(walDir, $"{opts.NodeId.Value}-{Block}{StorageEngine.SpillExtension}");
        using (var wal = WriteAheadLog.Open(spill, opts.NodeId, new SegmentId(Block), 1 << 20))
            for (int i = 0; i < Spilled; i++)
                wal.Append(T0 + i * TimeSpan.TicksPerMillisecond, LogLevel.Information, 40, "event {n}", Props(i),
                           serviceIndex: 41, service: "spill.svc");

        var engine = new StorageEngine(
            Microsoft.Extensions.Options.Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance,
            MemoryBudgets.Current(),
            indexSinkWait: HangGuard);
        try
        {
            await engine.CatalogLoaded;
            // A despill that did not wait would be done well within this.
            SpinWait.SpinUntil(() => engine.SpillFilesPending == 0, TimeSpan.FromSeconds(1));
            Assert.Equal(1, engine.SpillFilesPending);

            int sinks = 0;
            engine.IndexSinkFactory = (events, terms) =>
            {
                Interlocked.Increment(ref sinks);
                return new Ameto.Indexing.SegmentIndexBuilder(events, 5, terms);
            };
            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == Spilled,
                    "the block to be despilled once the sink is wired", ShortGuard);
            Assert.True(Volatile.Read(ref sinks) > 0, "the despilled block's segments were written without an index");
        }
        finally
        {
            await engine.DisposeAsync();
        }
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

    /// <summary>
    /// A wait for room that cannot even be set up — WaitAsync allocates, and a heap at its limit
    /// refuses that too — does not escape the write. It used to, after the batch's prefix was
    /// written: the request was answered 500 and the count of what was written lost. It counts as
    /// a slice waited, so the batch still ends within its wait, and says what it did not write.
    /// </summary>
    [Fact]
    public async Task A_wait_for_room_that_cannot_be_set_up_does_not_escape_the_write()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir, spill: false));
        var    gate   = HoldFirstFlush(engine);
        int    waits  = 0;
        try
        {
            using (var a = Batch(engine, 0, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            WaitFor(() => engine.LiveHotTier.Count == 0 && engine.HeavyPhasesInFlight == 1, "tier A to be swapped and held");

            engine._beforeRoomWaitForTest = () =>
            {
                Interlocked.Increment(ref waits);
                throw new OutOfMemoryException("test: no memory to set up the wait");
            };
            using var b = Batch(engine, TierEvents, TierEvents + 10);   // B takes a tier's worth, then has no room
            var result = await engine.WriteBatchAsync(b, TimeSpan.FromMilliseconds(500)).AsTask().WaitAsync(ShortGuard);

            Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 10), result);
            Assert.InRange(Volatile.Read(ref waits), 1, 10);   // a slice counted for each, so the 500 ms ran out
        }
        finally
        {
            engine._beforeRoomWaitForTest = null;
            gate.Set();
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

    /// <summary>A block an earlier run spilled: <paramref name="count"/> events from <paramref name="from"/>.</summary>
    private static void EarlierRunSpill(ServerOptions opts, string dir, ulong block, int from, int count)
    {
        string walDir = Path.Combine(dir, "wal");
        Directory.CreateDirectory(walDir);
        string spill = Path.Combine(walDir, $"{opts.NodeId.Value}-{block}{StorageEngine.SpillExtension}");
        using var wal = WriteAheadLog.Open(spill, opts.NodeId, new SegmentId(block), 1 << 20);
        for (int i = from; i < from + count; i++)
            wal.Append(T0 + i * TimeSpan.TicksPerMillisecond, LogLevel.Information, 40, "event {n}", Props(i),
                       serviceIndex: 41, service: "spill.svc");
    }

    /// <summary>
    /// R4-3. One transient swap failure, spilling off: the despill does not take the free slot (for a
    /// whole block), and the writers waiting are written once the swap installs at its first retry, a
    /// re-check interval later. The despill took the slot at the first failure while the writers were
    /// held back a fixed second: (0, 0, 10) after ~2 s, while the swap would have installed. It takes
    /// the slot once failures persist — three in a row — or when a spill can take the writers.
    /// The longest pause (_swapRetryDelay) is far past the writers' wait, so the hold-back's START is
    /// pinned too, not only the slot: a first pause of that fixed length passed while the pause was 2 s
    /// and the wait 4 (round-3 review of a272a17).
    /// </summary>
    [Fact]
    public async Task One_transient_swap_failure_leaves_the_slot_to_the_swap_and_the_writers_are_written()
    {
        string dir  = NewDir();
        var    opts = Options(dir, spill: false);
        EarlierRunSpill(opts, dir, 9_000, 100_000, 50);

        var hold = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        StorageEngine engine;
        try { engine = OneSlotEngine(opts); }
        finally { StorageEngine.HoldDespillForTest.Value = null; }
        using var despillGate = new ManualResetEventSlim(false);
        int failing = 1, failures = 0;
        engine._beforeSwap = () =>
        {
            if (Volatile.Read(ref failing) == 1)
            {
                Interlocked.Increment(ref failures);
                throw new IOException("test: a transient failure creating the next WAL");
            }
        };
        engine._beforeLoadSpillForTest = _ => despillGate.Wait(HangGuard);   // a despill that took the slot keeps it
        engine._roomRecheckMs  = 500;         // the swap's first retry half a second after its failure
        engine._swapRetryDelay = HangGuard;   // and not this, which would hold the writers past their wait
        try
        {
            using (var a = Batch(engine, 0, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            Assert.False(TryWriteOne(engine, TierEvents));   // the full tier refuses it
            WaitFor(() => Volatile.Read(ref failures) >= 1 && engine.LiveHotTierRefused, "the swap to fail");

            Volatile.Write(ref failing, 0);   // transient: the next attempt installs
            hold.SetResult();                  // the despill loop starts, the only slot free
            using (var b = Batch(engine, TierEvents + 1, 10))
                Assert.Equal(new LogBatchWriteResult(10, 0, 0), await engine.WriteBatchAsync(b, ShortGuard));
            Assert.Equal(1, Volatile.Read(ref failures));
        }
        finally
        {
            despillGate.Set();
            hold.TrySetResult();
            engine._beforeSwap = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// Round-4 review L25: R4-3's case with spilling on. The despill takes the slot at a swap's first
    /// failure only when a spill can take the writers, and it judged that by HasSpillRoom alone, which
    /// says yes for a volume that cannot say how full it is (DriveInfo threw on every UNC path). Here
    /// the volume cannot say and no spill file can be created: a despill that took the slot held it
    /// for a whole block while the writers could neither swap nor spill, and they were answered 503
    /// though the swap installed at its first retry. It leaves the slot to the swap.
    /// </summary>
    [Fact]
    public async Task A_despill_does_not_count_on_a_spill_where_the_volume_cannot_say_how_full_it_is()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        EarlierRunSpill(opts, dir, 9_000, 100_000, 50);

        var hold = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        StorageEngine engine;
        try { engine = OneSlotEngine(opts); }
        finally { StorageEngine.HoldDespillForTest.Value = null; }
        using var despillGate = new ManualResetEventSlim(false);
        int failing = 1, failures = 0;
        engine._beforeSwap = () =>
        {
            if (Volatile.Read(ref failing) == 1)
            {
                Interlocked.Increment(ref failures);
                throw new IOException("test: a transient failure creating the next WAL");
            }
        };
        engine._spillFreeDiskForTest   = () => throw new ArgumentException("test: DriveInfo takes no UNC path");
        engine._beforeSpillOpenForTest = () => throw new IOException("test: no spill file can be created");
        engine._beforeLoadSpillForTest = _ => despillGate.Wait(HangGuard);   // a despill that took the slot keeps it
        engine._roomRecheckMs  = 500;          // the swap's first retry half a second after its failure
        engine._swapRetryDelay = HangGuard;
        try
        {
            using (var a = Batch(engine, 0, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            Assert.False(TryWriteOne(engine, TierEvents));   // the full tier refuses it
            WaitFor(() => Volatile.Read(ref failures) >= 1 && engine.LiveHotTierRefused, "the swap to fail");

            Volatile.Write(ref failing, 0);   // transient: the next attempt installs
            hold.SetResult();                  // the despill loop starts, the only slot free
            using (var b = Batch(engine, TierEvents + 1, 10))
                Assert.Equal(new LogBatchWriteResult(10, 0, 0), await engine.WriteBatchAsync(b, ShortGuard));
            Assert.Equal(1, Volatile.Read(ref failures));
        }
        finally
        {
            despillGate.Set();
            hold.TrySetResult();
            engine._beforeSwap             = null;
            engine._beforeSpillOpenForTest = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// Round-4 review L25, the other half: a spill open that has failed, with none opened since, is no
    /// spill to count on either — the despill ignored SpillWithoutSlot's hold, and a volume that keeps
    /// refusing spill files refuses the next one too. An open fails while the first flush holds the
    /// only slot; the volume says it has room throughout. A later swap fails once: the despill leaves
    /// the slot to its retry, and the writers are written.
    /// </summary>
    [Fact]
    public async Task A_despill_does_not_count_on_a_spill_once_a_spill_open_has_failed()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        EarlierRunSpill(opts, dir, 9_000, 100_000, 50);

        var hold = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        StorageEngine engine;
        try { engine = OneSlotEngine(opts); }
        finally { StorageEngine.HoldDespillForTest.Value = null; }
        var gate = HoldFirstFlush(engine);
        using var despillGate = new ManualResetEventSlim(false);
        int opens = 0, failing = 0, failures = 0;
        engine._beforeSpillOpenForTest = () =>
        {
            Interlocked.Increment(ref opens);
            throw new IOException("test: no spill file can be created");
        };
        engine._beforeSwap = () =>
        {
            if (Volatile.Read(ref failing) == 1)
            {
                Interlocked.Increment(ref failures);
                throw new IOException("test: a transient failure creating the next WAL");
            }
        };
        engine._beforeLoadSpillForTest = _ => despillGate.Wait(HangGuard);   // a despill that took the slot keeps it
        engine._roomRecheckMs  = 500;
        engine._swapRetryDelay = HangGuard;
        try
        {
            // Tier 1 swaps, and its flush parks holding the only slot; tier 2 fills and refuses, and its
            // swap, finding no slot, tries to spill: the open fails.
            using (var a = Batch(engine, 0, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            Assert.False(TryWriteOne(engine, TierEvents));
            WaitFor(() => engine.FrozenTierCountForTest == 1 && engine.LiveHotTier.Count == 0, "tier 1 to swap");
            using (var b = Batch(engine, TierEvents, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(b, HangGuard));
            Assert.False(TryWriteOne(engine, 2 * TierEvents));
            WaitFor(() => Volatile.Read(ref opens) >= 1, "the spill open to fail");

            // The slot comes free: tier 2 swaps and is flushed. No spill file has opened since.
            gate.Set();
            WaitFor(() => engine.FrozenTierCountForTest == 0 && engine.HeavyPhasesInFlight == 0 && !engine.LiveHotTierRefused,
                    "tiers 1 and 2 to be flushed", ShortGuard);

            // Tier 3 fills, and its swap fails once.
            Volatile.Write(ref failing, 1);
            using (var c = Batch(engine, 2 * TierEvents, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(c, HangGuard));
            Assert.False(TryWriteOne(engine, 3 * TierEvents));
            WaitFor(() => Volatile.Read(ref failures) >= 1 && engine.LiveHotTierRefused, "tier 3's swap to fail");

            Volatile.Write(ref failing, 0);
            hold.SetResult();   // the despill loop starts, the only slot free
            using (var d = Batch(engine, 3 * TierEvents + 1, 10))
                Assert.Equal(new LogBatchWriteResult(10, 0, 0), await engine.WriteBatchAsync(d, ShortGuard));
            Assert.Equal(1, Volatile.Read(ref failures));
            Assert.Equal(1, Volatile.Read(ref opens));
        }
        finally
        {
            gate.Set();
            despillGate.Set();
            hold.TrySetResult();
            engine._beforeSwap             = null;
            engine._beforeSpillOpenForTest = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// Round-4 review L25: the floor is measured on the volume that holds the WAL directory, asked by
    /// the directory's own path. DriveInfo takes a drive's root alone: it threw on a UNC path — the
    /// floor never held there, and the despill counted on spills no volume had vouched for — and
    /// measured the root volume for a directory behind a mount folder or a junction. The path here is
    /// the directory's extended-length form, which DriveInfo refuses as it refuses a UNC name; this
    /// machine's admin share is measured too where it can be reached.
    /// </summary>
    [Fact]
    public void The_spill_floor_measures_the_volume_of_a_path_DriveInfo_refuses()
    {
        const long GB = 1024 * MB;
        string full   = Path.GetFullPath(NewDir());
        long   direct = new DriveInfo(full).AvailableFreeSpace;
        Assert.InRange(StorageEngine.VolumeAvailableBytes(full), direct - GB, direct + GB);
        if (!OperatingSystem.IsWindows()) return;

        string extended = @"\\?\" + full;
        Assert.ThrowsAny<ArgumentException>(() => new DriveInfo(extended));
        Assert.InRange(StorageEngine.VolumeAvailableBytes(extended), direct - GB, direct + GB);

        string share = $@"\\localhost\{full[0]}$\{full[3..]}";
        if (Directory.Exists(share))
            Assert.InRange(StorageEngine.VolumeAvailableBytes(share), direct - GB, direct + GB);
    }

    /// <summary>
    /// R4-6. A slot coming free gives a swap that failed the first go at it: what it failed on may have
    /// cleared. Two queued blocks, a spill to take the writers, a swap that failed: the despill takes
    /// the free slot for the first block (a spill can take the writers), and when that slot comes free
    /// the swap installs before the second block's despill starts — though the swap is slow to reach
    /// the slot, and the despill loop is already there. Unpinned until now: a mutant that left the
    /// failure standing passed every test.
    /// </summary>
    [Fact]
    public async Task A_slot_a_despill_frees_goes_to_the_swap_that_failed_before_the_next_block()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        EarlierRunSpill(opts, dir, 9_000, 100_000, 50);
        EarlierRunSpill(opts, dir, 9_006, 200_000, 50);

        var hold = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        StorageEngine engine;
        try { engine = OneSlotEngine(opts); }
        finally { StorageEngine.HoldDespillForTest.Value = null; }
        engine._roomRecheckMs  = (int)HangGuard.TotalMilliseconds;   // no retry of the swap's own: only a freed slot asks
        engine._swapRetryDelay = HangGuard;
        var order   = new System.Collections.Concurrent.ConcurrentQueue<string>();
        int failing = 1, failures = 0;
        engine._beforeSwap = () =>
        {
            if (Volatile.Read(ref failing) == 1)
            {
                Interlocked.Increment(ref failures);
                throw new IOException("test: a failure creating the next WAL");
            }
            Thread.Sleep(300);   // the swap is slow to reach the slot: a despill loop that did not yield would take it
        };
        engine._beforeFrozenListGrowForTest = () => order.Enqueue("swap");   // installing: it has the slot
        engine._beforeLoadSpillForTest      = segId => order.Enqueue($"despill {segId}");
        try
        {
            using (var a = Batch(engine, 0, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            Assert.False(TryWriteOne(engine, TierEvents));   // the full tier refuses it
            // The swap that refusal asked for has failed — not merely been asked for: cleared before it
            // ran, it installed at its first go and there was no failure to clear (13 of 15 runs on two
            // cores).
            WaitFor(() => engine.LiveHotTierRefused && Volatile.Read(ref failures) >= 1, "the full tier's swap to fail");

            Volatile.Write(ref failing, 0);
            hold.SetResult();
            WaitFor(() => order.Count >= 3, "both despills and the swap", ShortGuard);
            Assert.Equal(new[] { "despill 9000", "swap", "despill 9006" }, order.Take(3));
        }
        finally
        {
            hold.TrySetResult();
            engine._beforeSwap = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// Round-4 review L27: failures a quiet spell apart are not in a row. Two swaps nobody waits on
    /// fail (explicit flushes of a tier with room: their retry finds nothing to swap); a quiet spell
    /// passes; then a full tier's swap fails once, transiently. The count the two had left stood for
    /// good and made that the third failure in a row: the despill took the only slot at once, for a
    /// whole block, and with spilling off the writers were answered 503 though the swap installed at
    /// its first retry — R4-3's case again.
    /// </summary>
    [Fact]
    public async Task Swap_failures_a_quiet_spell_apart_are_not_in_a_row()
    {
        string dir  = NewDir();
        var    opts = Options(dir, spill: false);
        EarlierRunSpill(opts, dir, 9_000, 100_000, 50);

        var hold = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        StorageEngine engine;
        try { engine = OneSlotEngine(opts); }
        finally { StorageEngine.HoldDespillForTest.Value = null; }
        using var despillGate = new ManualResetEventSlim(false);
        int toFail = 2, failures = 0;
        engine._beforeSwap = () =>
        {
            if (Interlocked.Decrement(ref toFail) < 0) return;
            Interlocked.Increment(ref failures);
            throw new IOException("test: a failure creating the next WAL");
        };
        engine._beforeLoadSpillForTest = _ => despillGate.Wait(HangGuard);   // a despill that took the slot keeps it
        engine._roomRecheckMs  = 300;                                // the hold-back after one failure
        engine._swapRetryDelay = TimeSpan.FromMilliseconds(300);    // so failures over 3 s apart are not in a row
        try
        {
            Assert.True(TryWriteOne(engine, 0));
            await FailExplicitFlushesUntil(engine, () => Volatile.Read(ref failures), 2);
            Thread.Sleep(3_500);   // the quiet spell

            Volatile.Write(ref toFail, 1);   // the full tier's swap fails once
            using (var a = Batch(engine, 1, TierEvents - 1))
                Assert.Equal(new LogBatchWriteResult(TierEvents - 1, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            Assert.False(TryWriteOne(engine, TierEvents));   // the full tier refuses it
            WaitFor(() => Volatile.Read(ref failures) >= 3 && engine.LiveHotTierRefused, "the full tier's swap to fail");

            hold.SetResult();   // the despill loop starts, the only slot free
            using (var b = Batch(engine, TierEvents + 1, 10))
                Assert.Equal(new LogBatchWriteResult(10, 0, 0), await engine.WriteBatchAsync(b, ShortGuard));
            Assert.Equal(3, Volatile.Read(ref failures));
        }
        finally
        {
            despillGate.Set();
            hold.TrySetResult();
            engine._beforeSwap = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// Explicit flushes until the test's count of failed swaps reaches <paramref name="target"/>. A
    /// flush that finds the swap lock held — a request a release re-asked — returns without trying.
    /// </summary>
    private static async Task FailExplicitFlushesUntil(StorageEngine engine, Func<int> failures, int target)
    {
        long until = Environment.TickCount64 + (long)ShortGuard.TotalMilliseconds;
        while (failures() < target)
        {
            Assert.True(Environment.TickCount64 < until, $"the swap failed {failures()} of {target} times");
            try { await engine.FlushHotTierAsync(); }
            catch (IOException) { /* the failure counted */ }
        }
    }

    /// <summary>
    /// Round-4 review L27, the other half of the decision: a slot coming free does not end a run of
    /// failures. It forgets the last failure, so the swap tries first; but when that try fails too,
    /// the run goes on — here it is the third, and the backlog's despill takes a slot at once, the
    /// despill that frees what the swap keeps failing on (M4). Counted from 1 after each release, the
    /// despill went on yielding to a swap that failed after every flush that ended. No retry of the
    /// swap's own runs here: the attempts are the test's and the released slot's.
    /// </summary>
    [Fact]
    public async Task A_slot_coming_free_does_not_end_a_run_of_swap_failures()
    {
        string dir  = NewDir();
        var    opts = new ServerOptions
        {
            DataDirectory = dir,
            HotTier = new HotTierOptions
            {
                MaxSizeBytes          = TierBytes,
                FlushConcurrency      = 2,
                SpillEnabled          = false,
                SpillMinFreeDiskBytes = 0,
            },
        };
        EarlierRunSpill(opts, dir, 9_000, 100_000, 50);

        var hold = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        StorageEngine engine;
        try
        {
            engine = new StorageEngine(
                Microsoft.Extensions.Options.Options.Create(opts),
                new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
                NullLogger<StorageEngine>.Instance,
                MemoryBudgets.Derive(managedLimitBytes: 64 * MB, physicalLimitBytes: 64 * MB));
        }
        finally { StorageEngine.HoldDespillForTest.Value = null; }
        Assert.Equal(2, engine.FlushSlots);
        var gate = HoldFirstFlush(engine);
        using var despilling = new ManualResetEventSlim(false);
        int failing = 0, failures = 0;
        engine._beforeSwap = () =>
        {
            if (Volatile.Read(ref failing) == 0) return;
            Interlocked.Increment(ref failures);
            throw new IOException("test: no room on the volume for the next WAL");
        };
        engine._beforeLoadSpillForTest = _ => despilling.Set();
        engine._roomRecheckMs  = (int)HangGuard.TotalMilliseconds;   // no retry of the swap's own
        engine._swapRetryDelay = HangGuard;
        try
        {
            // Tier 1 swaps, and its flush parks holding one of the two slots.
            using (var a = Batch(engine, 0, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            Assert.False(TryWriteOne(engine, TierEvents));
            WaitFor(() => engine.FrozenTierCountForTest == 1 && engine.LiveHotTier.Count == 0, "tier 1 to swap");

            // Tier 2 fills, and its swap fails twice: the refusal's request, then an explicit flush.
            Volatile.Write(ref failing, 1);
            using (var b = Batch(engine, TierEvents, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(b, HangGuard));
            Assert.False(TryWriteOne(engine, 2 * TierEvents));
            await FailExplicitFlushesUntil(engine, () => Volatile.Read(ref failures), 2);
            Assert.Equal(2, Volatile.Read(ref failures));

            hold.SetResult();   // the despill loop starts: two failures, so it yields the free slot
            Assert.False(despilling.Wait(300), "the despill took the slot at the second failure");

            // Tier 1's flush ends: its slot comes free, the swap tries first and fails a third time.
            gate.Set();
            Assert.True(despilling.Wait(ShortGuard), "the despill went on yielding after the third failure in a row");
            Assert.Equal(3, Volatile.Read(ref failures));
        }
        finally
        {
            gate.Set();
            Volatile.Write(ref failing, 0);
            hold.TrySetResult();
            engine._beforeSwap = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// A swap whose install fails after it built its successor gives the successor back — its tier
    /// freed, its WAL deleted. The catch that gave the slot back left it behind: an open 64 MB WAL
    /// with its mapping and id block, and a tier's first chunk, at every attempt a request made.
    /// </summary>
    [Fact]
    public async Task A_swap_whose_install_fails_gives_back_the_successor_it_built()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir, spill: false));
        string walDir = Path.Combine(dir, "wal");
        int    armed  = 1;
        engine._beforeFrozenListGrowForTest = () =>
        {
            if (Interlocked.Exchange(ref armed, 0) == 1) throw new OutOfMemoryException("test: no room to list the frozen tier");
        };
        try
        {
            Assert.True(TryWriteOne(engine, 0));
            await Assert.ThrowsAsync<OutOfMemoryException>(() => engine.FlushHotTierAsync());

            Assert.Single(Directory.GetFiles(walDir, "*.wal"));   // the live WAL alone: the successor's is gone
            Assert.Equal(1, engine.LiveHotTier.Count);            // nothing was swapped
            Assert.True(TryWriteOne(engine, 1));

            await engine.FlushHotTierAsync();                     // the next swap installs
            Assert.Equal(2, ReadAll(engine).Count);
        }
        finally
        {
            engine._beforeFrozenListGrowForTest = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// The swap's own allocations — the room signal it wakes the writers with, room in the frozen
    /// list for its tier — are made before it freezes anything, so one that runs out of memory swaps
    /// nothing. In the shape before the review, the signal was allocated after the install: a throw
    /// there was taken for "nothing swapped", and the tier the swap had frozen, counted and listed was
    /// never flushed (its heavy phase counted for ever, shutdown waiting its whole budget for it).
    /// </summary>
    [Fact]
    public async Task A_swap_whose_room_signal_cannot_be_allocated_swaps_nothing_and_orphans_nothing()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir, spill: false));
        engine._shutdownWaitBudget = TimeSpan.FromSeconds(5);
        int armed = 1;
        engine._beforeRoomSignalAllocForTest = () =>
        {
            if (Interlocked.Exchange(ref armed, 0) == 1) throw new OutOfMemoryException("test: no memory for the room signal");
        };
        try
        {
            Assert.True(TryWriteOne(engine, 0));
            try { await engine.FlushHotTierAsync(); }
            catch (OutOfMemoryException) { /* nothing swapped */ }

            await engine.FlushHotTierAsync();   // the allocation works again
            WaitFor(() => engine.HeavyPhasesInFlight == 0, "every heavy phase to end", ShortGuard);
            Assert.Equal(1, ReadAll(engine).Count);
        }
        finally
        {
            engine._beforeRoomSignalAllocForTest = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// The same for room in the frozen list: in the shape before the review it was found after the
    /// freeze and the count, and a list that could not grow left the live tier frozen — every write
    /// refused for good — and its heavy phase counted with nothing to end it.
    /// </summary>
    [Fact]
    public async Task A_swap_whose_frozen_list_cannot_grow_leaves_the_live_tier_writable()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir, spill: false));
        engine._shutdownWaitBudget = TimeSpan.FromSeconds(5);
        int armed = 1;
        engine._beforeFrozenListGrowForTest = () =>
        {
            if (Interlocked.Exchange(ref armed, 0) == 1) throw new OutOfMemoryException("test: no room to list the frozen tier");
        };
        try
        {
            Assert.True(TryWriteOne(engine, 0));
            try { await engine.FlushHotTierAsync(); }
            catch (OutOfMemoryException) { /* nothing swapped */ }

            Assert.True(TryWriteOne(engine, 1), "the live tier refuses writes after a swap that failed");
            await engine.FlushHotTierAsync();
            WaitFor(() => engine.HeavyPhasesInFlight == 0, "every heavy phase to end", ShortGuard);
            Assert.Equal(2, ReadAll(engine).Count);
        }
        finally
        {
            engine._beforeFrozenListGrowForTest = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// A write's request for a swap that a swap has answered already does not swap again: the tier
    /// it finds is the fresh one, with no reason to go. Acted on, it froze that tier — a segment of
    /// a few events, for nothing — whenever a second flush slot was free. Here the request is the
    /// one a refused write made while the swap lock was held, asked again when the lock came free.
    /// </summary>
    [Fact]
    public async Task A_request_for_a_swap_answered_already_leaves_the_fresh_tier_alone()
    {
        string dir  = NewDir();
        var    opts = new ServerOptions
        {
            DataDirectory = dir,
            HotTier = new HotTierOptions
            {
                MaxSizeBytes          = TierBytes,
                FlushConcurrency      = 2,   // two slots: a stale request finds one free
                SpillEnabled          = false,
                SpillMinFreeDiskBytes = 0,
            },
        };
        var engine = new StorageEngine(
            Microsoft.Extensions.Options.Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance,
            MemoryBudgets.Derive(managedLimitBytes: 64 * MB, physicalLimitBytes: 64 * MB));
        Assert.Equal(2, engine.FlushSlots);
        using var inFirst    = new ManualResetEventSlim(false);
        using var firstGoes  = new ManualResetEventSlim(false);
        using var inSecond   = new ManualResetEventSlim(false);
        using var secondGoes = new ManualResetEventSlim(false);
        int swaps = 0;
        engine._beforeSwap = () =>
        {
            switch (Interlocked.Increment(ref swaps))
            {
                case 1: inFirst.Set();  firstGoes.Wait(HangGuard);  break;
                case 2: inSecond.Set(); secondGoes.Wait(HangGuard); break;
            }
        };
        try
        {
            using (var a = Batch(engine, 0, TierEvents))   // A fills: its swap is asked for, and held at the seam
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            Assert.True(inFirst.Wait(HangGuard), "A's swap never started");
            Assert.False(TryWriteOne(engine, TierEvents));   // refused: asks again, and finds the swap lock held

            firstGoes.Set();   // A swaps; releasing the lock asks again for the request it held
            Assert.True(inSecond.Wait(HangGuard), "the request held behind A's swap was never asked again");
            for (int i = 1; i <= 10; i++)
                Assert.True(TryWriteOne(engine, TierEvents + i));   // the fresh tier takes a few
            secondGoes.Set();

            WaitFor(() => engine.InFlightFlushTasks().All(t => t.IsCompleted), "the request to be answered");
            Assert.Equal(10, engine.LiveHotTier.Count);   // not swapped for a request A's swap answered
        }
        finally
        {
            firstGoes.Set();
            secondGoes.Set();
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
    /// Round-4 review H1. The bookkeeping of a failed swap runs before the swap lock is released (so
    /// the release's re-ask is held back like any other), and it allocated outside its try: the
    /// retry's closure, hoisted to the method's entry, and the despill's wake (SemaphoreSlim.Release
    /// allocates with a timed waiter parked). An out-of-memory there skipped the release: the swap
    /// lock was held for good — no swap, spill end, despill or merge ever again. Whichever of the two
    /// fails, the lock is released and the next swap installs.
    /// </summary>
    [Theory]
    [InlineData("closure")]
    [InlineData("wake")]
    public async Task A_failed_swap_whose_bookkeeping_runs_out_of_memory_still_releases_the_swap_lock(string where)
    {
        var engine = Engine(Options(NewDir()));
        int fail = 1;
        engine._beforeSwap = () =>
        {
            if (Interlocked.Exchange(ref fail, 0) == 1) throw new IOException("test: no room on the volume for the next WAL");
        };
        if (where == "closure") engine._swapRetryClosureForTest = () => throw new OutOfMemoryException("test: the retry's closure");
        else                    engine._wakeDespillForTest      = () => throw new OutOfMemoryException("test: waking the despill");
        try
        {
            using (var batch = Batch(engine, 0, 10))
                Assert.Equal(new LogBatchWriteResult(10, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
            var failed = await Record.ExceptionAsync(() => engine.FlushHotTierAsync());   // the swap fails

            engine._swapRetryClosureForTest = null;
            engine._wakeDespillForTest      = null;
            await engine.FlushHotTierAsync().WaitAsync(ShortGuard);   // the next swap has the lock
            Assert.True(engine.LiveHotTier.Count == 0, "the next swap did not run: the swap lock is still held");
            Assert.NotEmpty(engine.ListSegments());
            Assert.IsType<IOException>(failed);   // the swap's own failure, not its bookkeeping's
        }
        finally
        {
            engine._beforeSwap              = null;
            engine._swapRetryClosureForTest = null;
            engine._wakeDespillForTest      = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// The failed swap's bookkeeping runs before the swap lock is released, so a request a writer made
    /// while the failing swap held the lock — which the release asks again — is held back like any
    /// other after a failure. Released first, the request went round at once: a second attempt at what
    /// had failed a moment before, and a second count of one failure in the run that decides when the
    /// despill may take the slot. The R4-3 test saw that only when its write's request happened to land
    /// while the swap held the lock, and even then the bookkeeping is quicker than a re-ask's thread
    /// hop (round-3 review of a272a17). Here a request lands in the failing swap for certain, and the
    /// seam stretches the bookkeeping, so a release that came first would be seen.
    /// </summary>
    [Fact]
    public async Task A_swap_request_made_while_a_swap_fails_is_held_back_like_any_other()
    {
        var engine = OneSlotEngine(Options(NewDir(), spill: false));
        engine._roomRecheckMs  = 500;         // the hold-back after one failure
        engine._swapRetryDelay = HangGuard;
        var  attempts = new System.Collections.Concurrent.ConcurrentQueue<long>();
        long failedAt = 0;
        int  failing  = 1;
        engine._beforeSwap = () =>
        {
            attempts.Enqueue(Environment.TickCount64);
            if (Interlocked.Exchange(ref failing, 0) == 0) return;
            Assert.False(TryWriteOne(engine, TierEvents + 1));   // a writer asks while this swap holds the lock
            WaitFor(() => engine.SwapRequestedForTest, "the request to find the swap lock held");
            throw new IOException("test: a transient failure creating the next WAL");
        };
        engine._onSwapFailedForTest = () =>
        {
            Volatile.Write(ref failedAt, Environment.TickCount64);
            Thread.Sleep(300);
        };
        try
        {
            using (var a = Batch(engine, 0, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            Assert.False(TryWriteOne(engine, TierEvents));   // the full tier refuses it: the swap is asked for, and fails
            WaitFor(() => attempts.Count >= 2, "the swap to be tried again", ShortGuard);

            long after = attempts.ToArray()[1] - Volatile.Read(ref failedAt);
            Assert.True(after >= 400, $"the swap was tried again {after} ms after its failure: the request was not held back");
        }
        finally
        {
            engine._beforeSwap          = null;
            engine._onSwapFailedForTest = null;
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
        // The second read waits for the test to have looked: the pending count is asserted between
        // the failure and the retry, and a test thread held up for the retry's one second found the
        // block despilled already.
        using var looked = new ManualResetEventSlim(false);
        engine._beforeLoadSpillForTest = _ =>
        {
            if (Interlocked.Increment(ref reads) == 1) throw new OutOfMemoryException("test: no memory to materialise the block");
            looked.Wait(HangGuard);
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
            looked.Set();

            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == engine.SpilledEvents,
                    "the block to be despilled on its second try", ShortGuard);
            Assert.Equal(2, Volatile.Read(ref reads));

            WaitFor(() => engine.HeavyPhasesInFlight == 0, "the flushes to end");
            await engine.FlushHotTierAsync();
            Assert.Equal(Total, ReadAll(engine).Count);
        }
        finally
        {
            looked.Set();
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

    /// <summary>
    /// A despill whose flush fails hands its tier, slot and heavy phase to the flush retry — even when
    /// the line that says so throws. The throw left the catch before the hand-over: the finally gave
    /// the slot back, ended the heavy phase and queued the block again, so the tier stayed listed as
    /// frozen for good (its native memory held, its events served beside the block's second despill).
    /// </summary>
    [Fact]
    public async Task A_despill_whose_flush_fails_hands_its_tier_to_the_retry_even_when_its_error_line_throws()
    {
        string dir    = NewDir();
        var    logger = new ThrowingLogger(line => line.Contains("its tier stays frozen", StringComparison.Ordinal));
        var    engine = OneSlotEngine(Options(dir), logger);
        var    gate   = HoldFirstFlush(engine);
        engine._flushRetryDelay   = TimeSpan.FromMilliseconds(50);
        engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
        const int Total = 2 * TierEvents + 1_000;   // A, B in tiers; the rest spilled
        try
        {
            using (var batch = Batch(engine, 0, Total))
                Assert.Equal(new LogBatchWriteResult(Total, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
            Assert.True(engine.IsSpilling);

            // The despill's flush fails once, after its first level.
            int despilling = 0, failed = 0;
            engine._beforeLoadSpillForTest = _ => Volatile.Write(ref despilling, 1);
            var parkFirst = engine._afterLevelPublished;
            engine._afterLevelPublished = level =>
            {
                parkFirst?.Invoke(level);
                if (Volatile.Read(ref despilling) == 1 && Interlocked.Exchange(ref failed, 1) == 0)
                    throw new IOException("test: the despill's flush fails after a level");
            };

            gate.Set();
            WaitFor(() => logger.Thrown > 0, "the despill's error line to throw");
            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == engine.SpilledEvents
                       && engine.HeavyPhasesInFlight == 0,
                    "the block to be despilled and every heavy phase to end", ShortGuard);

            Assert.Equal(0, engine.FrozenTierCountForTest);   // no tier left listed
            await engine.FlushHotTierAsync();
            Assert.Equal(Total, ReadAll(engine).Count);
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
    /// Ma. A faulting attempt holds the writer lock for as long as the collection a failing rent
    /// forces, and the requests behind it queued for the lock with that time uncounted: six to eight
    /// concurrent requests took three to four times a 500 ms wait (forty would take ~50 s, past a
    /// Collector's 5 s timeout). While the store is faulting a request's wall time is its wait, its
    /// queueing for the lock included, so each ends within about its wait and one attempt.
    /// </summary>
    [Fact]
    public async Task Concurrent_requests_whose_writes_keep_faulting_each_end_within_about_their_wait()
    {
        var engine  = Engine(Options(NewDir()));
        var batches = new List<LogWriteBatch>();
        // Six requests each block a pool thread (asleep under the lock, or waiting for it): on two cores
        // the pool's floor is two, and threads injected late were counted as the requests' own time
        // (2 of 30 runs over the bound). The floor is raised for the test; the measure is the engine's.
        ThreadPool.GetMinThreads(out int minWorkers, out int minIo);
        ThreadPool.SetMinThreads(Math.Max(minWorkers, 16), minIo);
        try
        {
            engine._afterWriteGate = () =>
            {
                Thread.Sleep(150);   // the collection a failing rent waits for, the writer lock held
                throw new OutOfMemoryException("test: no memory for the template slots");
            };
            for (int i = 0; i < 6; i++) batches.Add(Batch(engine, i * 10, 10));

            var done = await Task.WhenAll(batches.Select(b => Task.Run(async () =>
            {
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var r  = await engine.WriteBatchAsync(b, TimeSpan.FromMilliseconds(500));
                return (Result: r, Took: sw.Elapsed);
            }))).WaitAsync(HangGuard);

            foreach (var (r, took) in done)
            {
                Assert.Equal(new LogBatchWriteResult(0, 0, 10), r);
                Assert.True(took < TimeSpan.FromMilliseconds(1_300), $"a request took {took.TotalMilliseconds:F0} ms of a 500 ms wait");
            }
        }
        finally
        {
            ThreadPool.SetMinThreads(minWorkers, minIo);
            engine._afterWriteGate = null;
            foreach (var b in batches) b.Dispose();
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// Round-4 review M1. While the store was faulting (any fault in the last second), a request was
    /// charged its queueing for the writer lock — a healthy one too, run after run: under contention a
    /// healthy batch gave up part way, a 200 with its tail dropped that no client retries. Only a
    /// request in a run of faults of its own, or one that has written nothing yet (whose giving up is
    /// a clean 503), is charged now. Here a healthy batch of three runs queues 350 ms behind another
    /// request after each of its first two, the store faulting: with a 500 ms wait it gave up at its
    /// third run; it writes all of it now.
    /// </summary>
    [Fact]
    public async Task A_healthy_batch_that_has_written_is_not_charged_its_queueing_while_the_store_faults()
    {
        var engine = Engine(Options(NewDir()));
        engine._roomRecheckMs = 20;
        using var inGate = new SemaphoreSlim(0);
        int failFirst = 1, holdNext = 0, hThread = 0, hRuns = 0;
        engine._afterWriteGate = () =>
        {
            if (Interlocked.Exchange(ref failFirst, 0) == 1) throw new OutOfMemoryException("test: another request's write faults");
            if (Interlocked.Exchange(ref holdNext, 0) == 1)
            {
                inGate.Release();
                Thread.Sleep(350);   // another request's write holds the writer lock
            }
        };
        var others = new System.Collections.Concurrent.ConcurrentBag<Task>();
        engine._afterWriteRunForTest = () =>
        {
            // After the healthy batch's first and second runs, another request takes the lock first.
            if (Environment.CurrentManagedThreadId != Volatile.Read(ref hThread) || Interlocked.Increment(ref hRuns) > 2) return;
            Volatile.Write(ref holdNext, 1);
            int n = 100_000 + Volatile.Read(ref hRuns);
            others.Add(Task.Run(async () =>
            {
                using var g = Batch(engine, n, 1);
                await engine.WriteBatchAsync(g, HangGuard);
            }));
            Assert.True(inGate.Wait(HangGuard), "the other request never took the lock");
        };
        try
        {
            using (var f = Batch(engine, 50_000, 1))   // faults once, then writes: the store is faulting
                Assert.Equal(new LogBatchWriteResult(1, 0, 0), await engine.WriteBatchAsync(f, HangGuard));

            using var h = Batch(engine, 0, 3 * 256);
            var result = await Task.Run(async () =>
            {
                Volatile.Write(ref hThread, Environment.CurrentManagedThreadId);
                return await engine.WriteBatchAsync(h, TimeSpan.FromMilliseconds(500));
            }).WaitAsync(HangGuard);

            Assert.Equal(new LogBatchWriteResult(3 * 256, 0, 0), result);
            await Task.WhenAll(others).WaitAsync(HangGuard);
        }
        finally
        {
            engine._afterWriteGate      = null;
            engine._afterWriteRunForTest = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// Round-4 review L1. The writer lock was waited for in 50 ms slices, each a TryEnter that gave up
    /// and came back as a new waiter — so none ever reached System.Threading.Lock's 100 ms, after which
    /// a waiter is handed the lock instead of barged past: a lone small request behind a busy writer
    /// waited up to 462 ms (at most ~110 before the slices). The slices are 200 ms now: here it gets
    /// the lock while the busy writer is still in its runs.
    /// </summary>
    [Fact]
    public async Task A_lone_writer_behind_a_busy_one_is_handed_the_lock_within_the_locks_bound()
    {
        var engine = Engine(Options(NewDir(), tierBytes: 64 * MB));
        int busyThread = 0;
        long eventTicks = System.Diagnostics.Stopwatch.Frequency / 25_000;   // 40 µs an event: ~10 ms a run
        engine._afterWriteGate = () =>
        {
            if (Environment.CurrentManagedThreadId != Volatile.Read(ref busyThread)) return;
            long until = System.Diagnostics.Stopwatch.GetTimestamp() + eventTicks;
            while (System.Diagnostics.Stopwatch.GetTimestamp() < until) Thread.SpinWait(20);
        };
        try
        {
            using var busy = Batch(engine, 0, 400 * 256);   // ~4 s of runs, the lock taken again at once after each
            var busyWrite  = Task.Run(async () =>
            {
                Volatile.Write(ref busyThread, Environment.CurrentManagedThreadId);
                return await engine.WriteBatchAsync(busy, HangGuard);
            });
            WaitFor(() => Volatile.Read(ref busyThread) != 0, "the busy writer to start");
            await Task.Delay(100);

            long worst = 0;
            for (int i = 0; i < 8; i++)   // one lone writer at a time, each measured
            {
                using var lone = Batch(engine, 1_000_000 + i, 1);
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var r  = await Task.Run(async () => await engine.WriteBatchAsync(lone, HangGuard));
                sw.Stop();
                Assert.Equal(new LogBatchWriteResult(1, 0, 0), r);
                worst = Math.Max(worst, sw.ElapsedMilliseconds);
            }
            Assert.False(busyWrite.IsCompleted, "the busy writer finished first: nothing was measured");
            Assert.True(worst < 250, $"a lone writer waited {worst} ms behind the busy one");
            await busyWrite.WaitAsync(HangGuard);
        }
        finally
        {
            engine._afterWriteGate = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// Round-4 review L7. The pause after each fault in a row doubles; when the pause could not be set
    /// up (out of memory) the fallback slept the re-check interval every time instead, so a request
    /// that kept faulting went round twenty times in a 2 s wait where five would do. It sleeps the
    /// pause it would have waited now.
    /// </summary>
    [Fact]
    public async Task A_faulting_write_whose_pause_cannot_be_set_up_still_pauses_twice_as_long_each_time()
    {
        var engine = Engine(Options(NewDir()));
        engine._roomRecheckMs = 100;
        int attempts = 0;
        engine._afterWriteGate = () =>
        {
            Interlocked.Increment(ref attempts);
            throw new OutOfMemoryException("test: every write faults");
        };
        engine._beforeRoomWaitForTest = () => throw new OutOfMemoryException("test: no memory to set up the pause");
        try
        {
            using var batch = Batch(engine, 0, 1);
            var r = await Task.Run(async () => await engine.WriteBatchAsync(batch, TimeSpan.FromSeconds(2)));
            Assert.Equal(new LogBatchWriteResult(0, 0, 1), r);
            // 100 + 200 + 400 + 800 ms of pauses spend the 2 s wait in about six attempts.
            Assert.InRange(Volatile.Read(ref attempts), 3, 8);
        }
        finally
        {
            engine._afterWriteGate        = null;
            engine._beforeRoomWaitForTest = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// Ma, its second half: a request queued for the writer lock sees its cancellation. It blocked
    /// in Enter until the holder let go — behind a holder stuck in a collection, a client long gone
    /// kept its thread and its batch. Cancelled while another write holds the lock, it stops within
    /// a slice, having written nothing.
    /// </summary>
    [Fact]
    public async Task A_request_queued_for_the_writer_lock_stops_when_it_is_cancelled()
    {
        var engine = Engine(Options(NewDir()));
        using var entered = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        int calls = 0;
        engine._afterWriteGate = () =>
        {
            if (Interlocked.Increment(ref calls) != 1) return;
            entered.Set();
            release.Wait(HangGuard);   // the first write holds the writer lock until the test lets go
        };
        using var holder = Batch(engine, 0, 1);
        using var queued = Batch(engine, 1, 5);
        using var cts    = new CancellationTokenSource();
        Task<LogBatchWriteResult>? held = null;
        try
        {
            held = Task.Run(async () => await engine.WriteBatchAsync(holder, HangGuard));
            Assert.True(entered.Wait(HangGuard), "the first write never took the lock");

            var waiting = Task.Run(async () => await engine.WriteBatchAsync(queued, HangGuard, cts.Token));
            await Task.Delay(100);   // at the lock (a request cancelled before it gets there stops the same way)
            cts.Cancel();

            var r = await waiting.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(new LogBatchWriteResult(0, 0, 5), r);
            Assert.False(held.IsCompleted);   // the holder still has the lock: the queued request did not wait it out
        }
        finally
        {
            release.Set();
            if (held is not null) await held.WaitAsync(HangGuard);
            engine._afterWriteGate = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// R1a-4. The pause after a fault doubles with each fault IN A ROW; a write that lands between
    /// them ends the row. The count used to be reset only by a faulting run that had written first,
    /// so a fault after a run that wrote everything doubled a stale count (806 ms measured where 100
    /// was right). Three faults, each after a run that landed whole: three first pauses.
    /// </summary>
    [Fact]
    public async Task A_fault_after_a_run_that_landed_pauses_as_a_first_fault_not_a_doubled_one()
    {
        const int Run = 256;   // StorageEngine.WriteRunEvents: the faults below each open a run
        var engine = Engine(Options(NewDir()));
        engine._roomRecheckMs = 200;
        try
        {
            int calls = 0;
            engine._afterWriteGate = () =>
            {
                int n = Interlocked.Increment(ref calls);
                if (n == 1 || n == 2 + Run || n == 3 + 2 * Run)
                    throw new OutOfMemoryException("test: a fault between runs that land");
            };
            var sw = System.Diagnostics.Stopwatch.StartNew();
            using (var batch = Batch(engine, 0, 3 * Run))
                Assert.Equal(new LogBatchWriteResult(3 * Run, 0, 0), await engine.WriteBatchAsync(batch, TimeSpan.FromSeconds(10)));
            sw.Stop();

            // 3 x 200 ms; a stale count paused 200 + 400 + 800.
            Assert.True(sw.Elapsed < TimeSpan.FromMilliseconds(1_100), $"the batch took {sw.Elapsed.TotalMilliseconds:F0} ms");
        }
        finally
        {
            engine._afterWriteGate = null;
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// R1a-5. A wait for room that cannot even be set up (WaitAsync allocates) was counted as a slice
    /// that took no time: a request went round twenty times in 6 ms and gave up before the swap it was
    /// waiting for landed. It waits for real now, with nothing to allocate, and takes the room when it
    /// comes.
    /// </summary>
    [Fact]
    public async Task A_wait_for_room_that_cannot_be_set_up_still_waits_and_takes_the_room_that_comes()
    {
        var engine = OneSlotEngine(Options(NewDir(), spill: false));
        var gate   = HoldFirstFlush(engine);
        try
        {
            using (var a = Batch(engine, 0, TierEvents))
                Assert.Equal(new LogBatchWriteResult(TierEvents, 0, 0), await engine.WriteBatchAsync(a, HangGuard));
            WaitFor(() => engine.LiveHotTier.Count == 0 && engine.HeavyPhasesInFlight == 1, "tier A to be swapped and held");

            engine._beforeRoomWaitForTest = () => throw new OutOfMemoryException("test: no memory to set up the wait");
            using var b = Batch(engine, TierEvents, TierEvents + 10);   // B takes a tier's worth, then has no room
            // Off this thread: with no wait that can be set up, the write waits on its own thread.
            var writing = Task.Run(() => engine.WriteBatchAsync(b, TimeSpan.FromSeconds(3)).AsTask());

            await Task.Delay(500);
            Assert.False(writing.IsCompleted, "the batch gave up within its wait, while the room it waited for was coming");
            gate.Set();
            Assert.Equal(new LogBatchWriteResult(TierEvents + 10, 0, 0), await writing.WaitAsync(ShortGuard));
        }
        finally
        {
            engine._beforeRoomWaitForTest = null;
            gate.Set();
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// The same wait, failing where its state machine is boxed — this method's first real suspension
    /// — caught and slept out as above. But the compiler guards every finally of the method with the
    /// state that suspension had set, so each one after it was skipped: the next run's release of the
    /// writer lock (held for good: every other writer waited for ever) and the lease's end (the batch
    /// never given back). The lock is released in a method with no await now, and the lease by plain
    /// code.
    /// </summary>
    [Fact]
    public async Task A_write_whose_first_suspension_cannot_be_boxed_releases_the_writer_lock_and_its_batch()
    {
        var engine = Engine(Options(NewDir()));
        engine._roomRecheckMs = 50;
        int calls = 0;
        engine._afterWriteGate = () =>
        {
            if (Interlocked.Increment(ref calls) == 1) throw new OutOfMemoryException("test: a fault, so the write waits");
        };
        var batch = Batch(engine, 0, 10);
        try
        {
            LogBatchWriteResult result;
            OwnerAsync.FirstSuspensionFailsForTest.Value = FailFirstSuspensionOnce("<WriteBatchAsync>");
            try { result = await engine.WriteBatchAsync(batch, TimeSpan.FromSeconds(5)); }
            finally { OwnerAsync.FirstSuspensionFailsForTest.Value = null; }
            Assert.Equal(new LogBatchWriteResult(10, 0, 0), result);

            using var other  = Batch(engine, 10, 5);
            var       second = Task.Run(async () => await engine.WriteBatchAsync(other, TimeSpan.FromSeconds(5)));
            Assert.True(await Task.WhenAny(second, Task.Delay(ShortGuard)) == second, "the writer lock was never released");
            Assert.Equal(new LogBatchWriteResult(5, 0, 0), await second);

            batch.Dispose();
            Assert.True(batch.ItemCapacity == 0, "the batch was not given back: its write never ended its lease");
        }
        finally
        {
            engine._afterWriteGate = null;
            batch.Dispose();
            // Off this thread and bounded: with the writer lock held for good (before the fix), shutdown's
            // fence waits for it for ever.
            var disposing = Task.Run(async () => await engine.DisposeAsync());
            await Task.WhenAny(disposing, Task.Delay(ShortGuard));
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

    /// <summary>
    /// R1a-3. A request whose own await throws — its state machine's box, out of memory, once the
    /// write it awaits has suspended — leaves through its finally, which disposes the batch while the
    /// write is still waiting for room on it. The arrays went back to their pools at once, and the
    /// write, woken, read a batch emptied under it: here it wrote nothing and said so; with a pool
    /// handing the arrays to another request first, a zeroed event or another request's. A dispose
    /// is now deferred to the end of the write that holds the batch.
    /// </summary>
    [Fact]
    public async Task A_batch_disposed_while_its_write_waits_for_room_is_given_back_only_when_the_write_ends()
    {
        var engine = OneSlotEngine(Options(NewDir(), spill: false));
        var gate   = HoldFirstFlush(engine);
        engine._roomRecheckMs = (int)HangGuard.TotalMilliseconds;   // the write waits for the signal, not a timer
        try
        {
            using (var fill = Batch(engine, 0, 2 * TierEvents))   // A held in its flush, B full
                Assert.Equal(2 * TierEvents, (await engine.WriteBatchAsync(fill, HangGuard)).Written);

            var batch   = Batch(engine, 2 * TierEvents, 10);
            var writing = engine.WriteBatchAsync(batch, HangGuard).AsTask();
            WaitFor(() => engine.LiveHotTierRefused && engine.IngestRoomWaits > 0, "the write to wait for room");
            Assert.False(writing.IsCompleted);

            batch.Dispose();                  // the request gave up on it
            Assert.Equal(10, batch.Count);    // still whole: the write holds it

            gate.Set();
            Assert.Equal(new LogBatchWriteResult(10, 0, 0), await writing.WaitAsync(TimeSpan.FromSeconds(20)));
            Assert.Equal(0, batch.Count);     // given back as the write ended

            WaitFor(() => engine.HeavyPhasesInFlight == 0, "the held flush and the one after it to end", ShortGuard);
            await engine.FlushHotTierAsync();
            WaitFor(() => engine.HeavyPhasesInFlight == 0, "the last flush to end", ShortGuard);
            var events = ReadAll(engine);
            for (int n = 2 * TierEvents; n < 2 * TierEvents + 10; n++) AssertIsEvent(n, events[n]);
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

    // ── A flush retry and its hand-over ───────────────────────────────────────

    /// <summary>A scheduler that queues nothing: a flush retry started on it is a start that failed.</summary>
    private sealed class RefusingScheduler : TaskScheduler
    {
        protected override void QueueTask(Task task) => throw new OutOfMemoryException("test: the retry could not be queued");
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;
        protected override IEnumerable<Task> GetScheduledTasks() => [];
    }

    /// <summary>
    /// The despill's flush fails once, after its first level; the despill hands its tier to a retry.
    /// Returns the spilled engine with its first flush released.
    /// </summary>
    private async Task<(StorageEngine Engine, ManualResetEventSlim Gate)> DespillWhoseFlushFailsOnceAsync(
        string dir, Action<StorageEngine> arm, Microsoft.Extensions.Logging.ILogger<StorageEngine>? logger = null)
    {
        var engine = OneSlotEngine(Options(dir), logger);
        var gate   = HoldFirstFlush(engine);
        engine._flushRetryDelay   = TimeSpan.FromMilliseconds(50);
        engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
        using (var batch = Batch(engine, 0, 2 * TierEvents + 1_000))   // A, B in tiers; the rest spilled
            Assert.Equal(new LogBatchWriteResult(2 * TierEvents + 1_000, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
        Assert.True(engine.IsSpilling);

        int despilling = 0, failed = 0;
        engine._beforeLoadSpillForTest = _ => Volatile.Write(ref despilling, 1);
        var parkFirst = engine._afterLevelPublished;
        engine._afterLevelPublished = level =>
        {
            parkFirst?.Invoke(level);
            if (Volatile.Read(ref despilling) == 1 && Interlocked.Exchange(ref failed, 1) == 0)
                throw new IOException("test: the despill's flush fails after a level");
        };
        arm(engine);
        gate.Set();
        return (engine, gate);
    }

    /// <summary>
    /// H2. A retry that has started owns the tier, its slot and its heavy phase, whatever happens next
    /// in the call that started it. Task.Run allocated its promise after queueing the work, and the
    /// bookkeeping came after that: an out-of-memory there reached the despill as "no retry", and the
    /// despill gave the tier up — unlisted, retired, freed — while the retry went on to flush it every
    /// 15 s (an access violation), or, if a reader kept it alive, raced the block's second despill and
    /// unlinked a level it had published. The throw here comes right after the start; the retry is
    /// held at its first attempt while the test looks.
    /// </summary>
    [Fact]
    public async Task A_throw_right_after_a_despills_retry_has_started_does_not_take_the_tier_from_the_retry()
    {
        int thrown = 0, tearDown = 0;
        using var attempt = new ManualResetEventSlim(false);
        var (engine, gate) = await DespillWhoseFlushFailsOnceAsync(NewDir(), e =>
        {
            e._afterFlushRetryStartForTest = () =>
            {
                if (Interlocked.Exchange(ref thrown, 1) == 0) throw new OutOfMemoryException("test: right after the retry's start");
            };
            e._beforeFlushRetryAttemptForTest = () =>
            {
                attempt.Wait(HangGuard);
                if (Volatile.Read(ref tearDown) == 1) throw new OperationCanceledException("test: torn down before the attempt");
            };
        });
        try
        {
            WaitFor(() => Volatile.Read(ref thrown) == 1 && engine.DespillsCompletedForTest == 1,
                    "the despill to hand its tier over and return", ShortGuard);

            // The retry has the tier: still listed (queries serve it), its block still pending, its
            // heavy phase still counted — none of it given back by the despill.
            Assert.Equal(1, engine.FrozenTierCountForTest);
            Assert.Equal(1, engine.SpillFilesPending);
            Assert.Equal(1, engine.HeavyPhasesInFlight);

            attempt.Set();
            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == engine.SpilledEvents
                       && engine.HeavyPhasesInFlight == 0,
                    "the retry to publish the block", ShortGuard);
            Assert.Equal(0, engine.FrozenTierCountForTest);
            Assert.Equal(1, engine.DespillsCompletedForTest);   // the block was not despilled a second time
            await engine.FlushHotTierAsync();
            Assert.Equal(2 * TierEvents + 1_000, ReadAll(engine).Count);
        }
        finally
        {
            Volatile.Write(ref tearDown, 1);   // a retry still parked leaves without touching its tier
            attempt.Set();
            gate.Set();
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// H2, the other side. A retry that cannot be started — out of memory before the start, here the
    /// scheduler refusing it — leaves the tier with the despill, which gives it up exactly once: the
    /// block is read from its file again and despilled, once. The retry's entry, taken before the
    /// start, does not outlive it (shutdown awaits every entry).
    /// </summary>
    [Fact]
    public async Task A_despill_whose_retry_cannot_start_gives_its_tier_up_once_and_despills_the_block_again()
    {
        var (engine, gate) = await DespillWhoseFlushFailsOnceAsync(NewDir(), e => e._flushRetrySchedulerForTest = new RefusingScheduler());
        try
        {
            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == engine.SpilledEvents
                       && engine.HeavyPhasesInFlight == 0,
                    "the block to be despilled again from its file", ShortGuard);
            Assert.Equal(2, engine.DespillsCompletedForTest);
            Assert.Equal(0, engine.FrozenTierCountForTest);
            Assert.Empty(engine.InFlightFlushTasks());
            await engine.FlushHotTierAsync();
            Assert.Equal(2 * TierEvents + 1_000, ReadAll(engine).Count);
        }
        finally
        {
            gate.Set();
            var disposing = engine.DisposeAsync().AsTask();
            Assert.True(await Task.WhenAny(disposing, Task.Delay(ShortGuard)) == disposing, "shutdown waited for a retry that never started");
        }
    }

    /// <summary>
    /// The swap's own flush hands its tier to a retry the same way, and its error line stood
    /// unguarded before the hand-over (the despill's was guarded in d6399f5): a throw from it left
    /// the tier listed and never flushed, while the slot and the heavy phase went back.
    /// </summary>
    [Fact]
    public async Task A_swapped_tier_whose_flush_fails_reaches_its_retry_even_when_the_error_line_throws()
    {
        var logger = new ThrowingLogger(line => line.Contains("Segment flush failed", StringComparison.Ordinal));
        var engine = OneSlotEngine(Options(NewDir()), logger);
        engine._flushRetryDelay = TimeSpan.FromMilliseconds(50);
        int failed = 0;
        engine._afterLevelPublished = _ =>
        {
            if (Interlocked.Exchange(ref failed, 1) == 0) throw new IOException("test: the flush fails after a level");
        };
        try
        {
            using (var batch = Batch(engine, 0, 500))
                Assert.Equal(500, (await engine.WriteBatchAsync(batch, HangGuard)).Written);
            try { await engine.FlushHotTierAsync(); }
            catch (OutOfMemoryException) { /* the unguarded line, before the fix */ }

            Assert.Equal(1, logger.Thrown);
            WaitFor(() => engine.FrozenTierCountForTest == 0 && engine.HeavyPhasesInFlight == 0,
                    "the failed tier to be flushed by its retry", ShortGuard);
            Assert.Equal(500, ReadAll(engine).Count);
        }
        finally { await engine.DisposeAsync(); }
    }

    /// <summary>
    /// The swap's tier whose retry cannot be started (the new failure path of the hand-over): its slot
    /// and heavy phase go back once, and the tier stays listed — queries go on serving it — until
    /// shutdown frees it; its WAL stays, and the next start brings every event back once: the level
    /// the failed attempt had moved into place as it is, the other replayed from the WAL. Two levels,
    /// because with one the failed attempt had already published everything, the replay had nothing to
    /// do, and a WAL deleted at shutdown passed (round-3 review of 9275894). Either way it cannot start:
    /// nothing of it set up ("before"), and the caller gives the tier up; or set up and refused by the
    /// scheduler, where its continuation gives it up — and only it: with the caller too, the slot and
    /// the heavy phase went back twice, the count negative and shutdown waiting its whole budget.
    /// </summary>
    [Theory]
    [InlineData("before")]
    [InlineData("scheduler")]
    public async Task A_swapped_tier_whose_retry_cannot_start_stays_served_and_comes_back_once_at_the_next_start(string how)
    {
        string dir    = NewDir();
        var    opts   = Options(dir);
        var    engine = OneSlotEngine(opts);
        int failed = 0;
        engine._afterLevelPublished = _ =>
        {
            // After the first level (Information): the second (Warning) is not written.
            if (Interlocked.Exchange(ref failed, 1) == 0) throw new IOException("test: the flush fails after a level");
        };
        if (how == "before") engine._beforeFlushRetryStartForTest = () => throw new OutOfMemoryException("test: no retry can be started");
        else                 engine._flushRetrySchedulerForTest   = new RefusingScheduler();
        try
        {
            using (var info = Batch(engine, 0, 250))
                Assert.Equal(250, (await engine.WriteBatchAsync(info, HangGuard)).Written);
            using (var warn = Batch(engine, 250, 250, level: LogLevel.Warning))
                Assert.Equal(250, (await engine.WriteBatchAsync(warn, HangGuard)).Written);
            try { await engine.FlushHotTierAsync(); }
            catch (OutOfMemoryException) { /* before the fix the start's throw escaped */ }

            Assert.Equal(0, engine.HeavyPhasesInFlight);
            Assert.Equal(1, engine.FrozenTierCountForTest);
            Assert.Empty(engine.InFlightFlushTasks());
            using (var reader = engine.OpenHotTierReader())
                Assert.Equal(500, reader.ReadAll().Count());
            Assert.Empty(ReadAll(engine));   // in no segment yet
        }
        finally { await engine.DisposeAsync(); }

        // A WAL whose block has a level on disk is despilled once the boot scan has read that level
        // (9cd8b06): the Warning level comes back in the background, after the catalog has loaded.
        await using var restarted = OneSlotEngine(opts);
        await restarted.CatalogLoaded;
        WaitFor(() => restarted.SpillFilesPending == 0, "the WAL's other level to be written by the despill", ShortGuard);
        var events = ReadAll(restarted);
        Assert.Equal(500, events.Count);
        foreach (var (n, ev) in events)
        {
            AssertIsEvent(n, ev);
            Assert.Equal((byte)(n < 250 ? LogLevel.Information : LogLevel.Warning), ev.Level);
        }
    }

    /// <summary>
    /// A retry that has published its tier is done, whatever its success line does. The line sat in
    /// the attempt, and a throw from it sent the loop round again: the next attempt unlinked the level
    /// files just published — registered segments — and flushed the tier PublishFlushedTier had just
    /// handed to be freed (a reader here keeps it alive, or that is an access violation). The block's
    /// level is written twice where once is right.
    /// </summary>
    [Fact]
    public async Task A_flush_retry_whose_success_line_throws_does_not_write_its_block_again()
    {
        var logger = new ThrowingLogger(line => line.Contains("Frozen-tier flush retry succeeded", StringComparison.Ordinal));
        var engine = OneSlotEngine(Options(NewDir()), logger);
        engine._flushRetryDelay = TimeSpan.FromMilliseconds(50);
        int levelWrites = 0;
        engine._afterLevelPublished = _ =>
        {
            if (Interlocked.Increment(ref levelWrites) == 1) throw new IOException("test: the flush fails after a level");
        };
        try
        {
            using (var batch = Batch(engine, 0, 500))
                Assert.Equal(500, (await engine.WriteBatchAsync(batch, HangGuard)).Written);
            using (engine.OpenHotTierReader())   // keeps the tier's memory alive whatever the retry does
            {
                await engine.FlushHotTierAsync();
                WaitFor(() => logger.Thrown == 1 && engine.HeavyPhasesInFlight == 0 && engine.InFlightFlushTasks().Length == 0,
                        "the retry to publish the block and end", ShortGuard);
            }

            Assert.Equal(2, Volatile.Read(ref levelWrites));   // the failed attempt's level, and the retry's
            foreach (var seg in engine.ListSegments())
                Assert.True(File.Exists(seg.FilePath), $"{seg.FilePath} is in the catalog and not on disk");
            Assert.Equal(500, ReadAll(engine).Count);
        }
        finally { await engine.DisposeAsync(); }
    }

    /// <summary>
    /// Publishing a flushed tier is done once its segments are registered and the tier unlisted:
    /// what follows — the log line, the SegmentFlushed subscribers (replication), the WAL's delete,
    /// the tier's retirement — is best-effort and throws nothing. A subscriber that threw used to
    /// skip the WAL's delete and the tier's retirement, and reached the flush's caller as a failure
    /// of a flush that had published (in the retry loop, as a reason to write the block again).
    /// </summary>
    [Fact]
    public async Task A_segment_flushed_subscriber_that_throws_does_not_undo_a_published_flush()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir));
        int calls = 0;
        engine.SegmentFlushed = _ =>
        {
            Interlocked.Increment(ref calls);
            throw new OutOfMemoryException("test: the subscriber runs out of memory");
        };
        try
        {
            using (var batch = Batch(engine, 0, 500))
                Assert.Equal(500, (await engine.WriteBatchAsync(batch, HangGuard)).Written);
            ulong walId = engine.LiveWalSegmentId;
            await engine.FlushHotTierAsync();   // threw the subscriber's exception before

            Assert.Equal(1, calls);
            Assert.Equal(0, engine.FrozenTierCountForTest);
            Assert.NotEqual(walId, engine.LiveWalSegmentId);
            Assert.DoesNotContain(Directory.GetFiles(Path.Combine(dir, "wal"), "*.wal"),
                f => Path.GetFileNameWithoutExtension(f).EndsWith($"-{walId}", StringComparison.Ordinal));   // the published tier's WAL is gone
            Assert.Equal(500, ReadAll(engine).Count);
        }
        finally { await engine.DisposeAsync(); }
    }

    // ── The despill loop survives its own failures ────────────────────────────

    /// <summary>
    /// R4-4. The despill loop caught only cancellation around its own steps: an out-of-memory setting
    /// up its wait, walking its queue or writing a line ended it, and no block was despilled again in
    /// this process — and shutdown's await of the dead loop threw before its final flush. It goes on
    /// after a pause now, and after a pause it cannot even set up (a blocked thread instead).
    /// </summary>
    [Fact]
    public async Task The_despill_loop_survives_a_failure_of_its_own_and_despills_the_block_after_it()
    {
        string dir    = NewDir();
        var    opts   = Options(dir);
        string walDir = Path.Combine(dir, "wal");
        Directory.CreateDirectory(walDir);
        string spill = Path.Combine(walDir, $"{opts.NodeId.Value}-9000{StorageEngine.SpillExtension}");
        using (var wal = WriteAheadLog.Open(spill, opts.NodeId, new SegmentId(9_000), 1 << 20))
            for (int i = 0; i < 10; i++)
                wal.Append(T0 + i * TimeSpan.TicksPerMillisecond, LogLevel.Information, 40, "event {n}", Props(i),
                           serviceIndex: 41, service: "spill.svc",
                           traceIdHi: 0xA000_0000_0000_0000UL | (uint)i, traceIdLo: (ulong)i * 7 + 1, spanId: (ulong)i * 13 + 1);

        var hold = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        StorageEngine engine;
        try { engine = Engine(opts); }
        finally { StorageEngine.HoldDespillForTest.Value = null; }

        int rounds = 0, pauses = 0;
        engine._despillLoopIterationForTest = () =>
        {
            if (Interlocked.Increment(ref rounds) == 1) throw new OutOfMemoryException("test: the loop's own round ran out");
        };
        engine._despillLoopPauseForTest = () =>
        {
            if (Interlocked.Increment(ref pauses) == 1) throw new OutOfMemoryException("test: no memory for the pause's timer");
        };
        await using (engine)
        {
            hold.SetResult();
            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == 10, "the block to be despilled after the loop's failure", ShortGuard);
            Assert.True(Volatile.Read(ref rounds) >= 2);
            Assert.Equal(10, ReadAll(engine).Count);
        }
    }

    /// <summary>
    /// R4-4, its second half: shutdown goes on past a despill loop that died. The loop survives its
    /// own failures now, so only a loop that died anyway reaches this — here its start hold faults.
    /// Shutdown's await of it threw out of DisposeAsync before the final flush, the live tier left in
    /// its WAL. Unpinned until now: the loop no longer dies in the test above, so a mutant without
    /// this guard passed it.
    /// </summary>
    [Fact]
    public async Task Shutdown_goes_on_to_its_final_flush_past_a_despill_loop_that_died()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        StorageEngine.HoldDespillForTest.Value = Task.FromException(new OutOfMemoryException("test: the despill loop died"));
        StorageEngine engine;
        try { engine = Engine(opts); }
        finally { StorageEngine.HoldDespillForTest.Value = null; }

        using (var batch = Batch(engine, 0, 10))
            Assert.Equal(new LogBatchWriteResult(10, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));

        await engine.DisposeAsync();

        Assert.NotEmpty(Directory.GetFiles(Path.Combine(dir, "segments"), "*.seg"));   // the final flush ran
        await using var reopened = Engine(opts);
        await reopened.CatalogLoaded;   // the boot scan registers the segment, in the background
        Assert.Equal(10, ReadAll(reopened).Count);
    }

    // ── A first suspension whose box cannot be allocated ──────────────────────

    /// <summary>
    /// OwnerAsync's seam, armed for one first suspension of the method whose state machine's name
    /// carries <paramref name="method"/> (as "&lt;TryFlushAsync&gt;"): it then throws where the runtime
    /// boxes the state machine, every finally of the method skipped as a real failure skips them.
    /// </summary>
    private static Func<Type, bool> FailFirstSuspensionOnce(string method)
    {
        int armed = 1;
        return t => t.Name.Contains(method, StringComparison.Ordinal) && Interlocked.Exchange(ref armed, 0) == 1;
    }

    /// <summary>
    /// Round-4 review H5, the swap. TryFlushAsync's first real suspension was the heavy phase's await
    /// of the flush, which always suspends: a box that could not be allocated there was caught as a
    /// FAILED flush while the flush ran on — handed to a retry — and the finallys that give the
    /// heavy-phase permit back were skipped. With one permit, every later heavy phase waited for ever.
    /// The method is boxed before it owns anything now: that failure fails the swap before it swaps,
    /// and the next one flushes everything once.
    /// </summary>
    [Fact]
    public async Task A_swap_whose_first_suspension_cannot_be_boxed_owns_nothing_and_every_tier_is_flushed_once()
    {
        var engine = Engine(Options(NewDir()));   // one heavy-phase permit, slots to spare
        engine._flushRetryDelay = TimeSpan.FromMilliseconds(50);
        try
        {
            using (var a = Batch(engine, 0, 10))
                Assert.Equal(new LogBatchWriteResult(10, 0, 0), await engine.WriteBatchAsync(a, HangGuard));

            OwnerAsync.FirstSuspensionFailsForTest.Value = FailFirstSuspensionOnce("<TryFlushAsync>");
            Exception? first;
            try { first = await Record.ExceptionAsync(() => engine.FlushHotTierAsync()); }
            finally { OwnerAsync.FirstSuspensionFailsForTest.Value = null; }

            using (var b = Batch(engine, 10, 10))
                Assert.Equal(new LogBatchWriteResult(10, 0, 0), await engine.WriteBatchAsync(b, HangGuard));
            var next = engine.FlushHotTierAsync();   // a heavy phase that needs the one permit
            Assert.True(await Task.WhenAny(next, Task.Delay(ShortGuard)) == next,
                        "the next heavy phase never got its permit: the failed one kept it");
            await next;

            WaitFor(() => engine.FrozenTierCountForTest == 0 && engine.HeavyPhasesInFlight == 0, "every tier flushed and published", ShortGuard);
            Assert.Equal(20, ReadAll(engine).Count);
            Assert.IsType<OutOfMemoryException>(first);   // it failed owning nothing, before it swapped
        }
        finally { await engine.DisposeAsync(); }
    }

    /// <summary>
    /// H5, the despill: its first real suspension was the wait for the swap lock or the permit, or the
    /// await of the flush — a box that could not be allocated there skipped every finally, and the
    /// permit, the slot and the block stayed taken (the block never despilled in this process). It is
    /// boxed first now, owning only the slot the loop took for it, which its catch gives back with
    /// the block queued again; the next despill writes it.
    /// </summary>
    [Fact]
    public async Task A_despill_whose_first_suspension_cannot_be_boxed_gives_its_slot_back_and_the_block_is_despilled_later()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        EarlierRunSpill(opts, dir, 9_000, 100_000, 50);

        var hold = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value       = hold.Task;
        OwnerAsync.FirstSuspensionFailsForTest.Value = FailFirstSuspensionOnce("<DespillAsync>");
        StorageEngine engine;
        try { engine = OneSlotEngine(opts); }
        finally
        {
            StorageEngine.HoldDespillForTest.Value       = null;
            OwnerAsync.FirstSuspensionFailsForTest.Value = null;
        }
        await using (engine)
        {
            engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
            engine._flushRetryDelay   = TimeSpan.FromMilliseconds(50);
            hold.SetResult();
            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == 50,
                    "the block to be despilled after the despill that could not be boxed", ShortGuard);
            Assert.Equal(50, ReadAll(engine).Count);
            Assert.Equal(0, engine.HeavyPhasesInFlight);
        }
    }

    /// <summary>
    /// Round-4 review L21. A started flush retry owns its tier's slot and heavy-phase count; its first
    /// suspension — the wait before its first attempt, where it is boxed — could run out of memory,
    /// which skipped the finally that gives them back: the slot and the count held for good, the tier
    /// never flushed, shutdown waiting its budget. The continuation registered before the retry
    /// started gives them back now (once); the tier stays listed, served, until shutdown, and its WAL
    /// brings every event back once at the next start.
    /// </summary>
    [Fact]
    public async Task A_flush_retry_whose_first_suspension_cannot_be_boxed_gives_its_slot_and_count_back()
    {
        string dir    = NewDir();
        var    opts   = Options(dir);
        var    engine = OneSlotEngine(opts);
        engine._flushRetryDelay = TimeSpan.FromMilliseconds(50);
        int failed = 0;
        engine._afterLevelPublished = _ =>
        {
            if (Interlocked.Exchange(ref failed, 1) == 0) throw new IOException("test: the flush fails after a level");
        };
        try
        {
            using (var batch = Batch(engine, 0, 500))
                Assert.Equal(500, (await engine.WriteBatchAsync(batch, HangGuard)).Written);
            OwnerAsync.FirstSuspensionFailsForTest.Value = FailFirstSuspensionOnce("<FlushRetryLoopAsync>");
            try { await engine.FlushHotTierAsync(); }   // the flush fails; its retry starts, and is not boxed
            finally { OwnerAsync.FirstSuspensionFailsForTest.Value = null; }

            WaitFor(() => engine.HeavyPhasesInFlight == 0, "the dead retry's slot and heavy phase to be given back", ShortGuard);
            Assert.Equal(1, engine.FrozenTierCountForTest);   // listed, and served, until shutdown
        }
        finally
        {
            engine._afterLevelPublished = null;
            var sw = System.Diagnostics.Stopwatch.StartNew();
            await engine.DisposeAsync();
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10), $"shutdown waited {sw.Elapsed.TotalSeconds:F1}s for a heavy phase");
        }

        await using var reopened = Engine(opts);
        await reopened.CatalogLoaded;
        WaitFor(() => reopened.SpillFilesPending == 0 && ReadAll(reopened).Count == 500, "the tier's WAL to bring its events back", ShortGuard);
        Assert.Equal(500, ReadAll(reopened).Count);
    }

    /// <summary>
    /// Round-4 review L22. DespillAsync's callback for its retry captured the block, so the closure was
    /// allocated on its entry, before any of its try blocks: an out-of-memory there left the slot the
    /// loop took for it held, and the block in flight, for good. The callbacks are built where they
    /// are handed over now; failing there, no retry starts and the block is despilled again.
    /// </summary>
    [Fact]
    public async Task A_despill_whose_retry_callbacks_cannot_be_allocated_still_despills_its_block()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        EarlierRunSpill(opts, dir, 9_000, 100_000, 50);

        var hold = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        StorageEngine engine;
        try { engine = OneSlotEngine(opts); }
        finally { StorageEngine.HoldDespillForTest.Value = null; }
        int callbacks = 0, failed = 0;
        engine._despillRetryCallbacksForTest = () =>
        {
            if (Interlocked.Increment(ref callbacks) == 1) throw new OutOfMemoryException("test: the retry's callbacks");
        };
        engine._afterLevelPublished = _ =>
        {
            if (Interlocked.Exchange(ref failed, 1) == 0) throw new IOException("test: the despill's flush fails after a level");
        };
        await using (engine)
        {
            engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
            hold.SetResult();
            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == 50, "the block to be despilled", ShortGuard);
            Assert.Equal(50, ReadAll(engine).Count);
            Assert.Equal(0, engine.HeavyPhasesInFlight);
        }
    }

    // ── A flush's own first suspension (round-5 H-B) ──────────────────────────

    /// <summary>
    /// A flush whose own first suspension cannot be boxed (OwnerAsync's seam, <see cref="FailsFirstSuspension"/>),
    /// and the level writes around it (StorageEngine.LevelWriteStartingForTest, on each write's thread before
    /// it reads its tier). The box fails once a level write the flush queued has begun — or after
    /// <see cref="Bound"/>, when it queued none — so a write that began before the failure is one the
    /// failure left running: <see cref="LeftWriteBehind"/>, whatever the threads' order. Such a write is
    /// then held, before it reads anything, until its tier is freed or a second write of it begins (both
    /// recorded, and the write stopped there: in production the first is a use-after-free), or the bound
    /// passes. A write that begins on a tier already freed is recorded and stopped too.
    /// </summary>
    private sealed class LeftBehindWriteWatch
    {
        private static readonly TimeSpan Bound = TimeSpan.FromSeconds(1);

        private readonly ManualResetEventSlim _began = new(false), _boxFailed = new(false);
        private int _armed = 1;
        private HotTierSegment? _leftBehind;
        private volatile bool _leftWriteBehind, _freedUnderIt, _writtenBesideIt;

        /// <summary>A level write had begun when the flush's first suspension failed: it ran on after the flush's fault.</summary>
        public bool LeftWriteBehind => _leftWriteBehind;

        /// <summary>A tier was freed under a level write, or a write began on a freed tier.</summary>
        public bool FreedUnderIt => _freedUnderIt;

        /// <summary>A second write of a tier began while the one left behind was running.</summary>
        public bool WrittenBesideIt => _writtenBesideIt;

        /// <summary>For OwnerAsync.FirstSuspensionFailsForTest: FlushTierByLevelAsync's first suspension fails, once.</summary>
        public bool FailsFirstSuspension(Type stateMachine)
        {
            if (!stateMachine.Name.Contains("<FlushTierByLevelAsync>", StringComparison.Ordinal) || Interlocked.Exchange(ref _armed, 0) == 0)
                return false;
            _began.Wait(Bound);   // a write queued before this suspension begins at once; none queued, none begins
            _boxFailed.Set();
            return true;
        }

        public void OnLevelWrite(HotTierSegment tier)
        {
            if (tier.IsDisposed)
            {
                _freedUnderIt = true;
                throw new InvalidOperationException("test: a level write began on a freed tier; stopped before it reads");
            }
            if (!_boxFailed.IsSet)
            {
                Volatile.Write(ref _leftBehind, tier);
                try
                {
                    _began.Set();
                    if (!_boxFailed.Wait(Bound)) return;   // no flush failed meanwhile: an ordinary write
                    _leftWriteBehind = true;
                    SpinWait.SpinUntil(() => tier.IsDisposed || _writtenBesideIt, Bound + Bound);
                    if (tier.IsDisposed) _freedUnderIt = true;
                    throw new InvalidOperationException("test: the level write the failed flush left running is stopped before it reads");
                }
                finally { Volatile.Write(ref _leftBehind, null); }   // a write of the tier after this one is no longer beside it
            }
            if (ReferenceEquals(Volatile.Read(ref _leftBehind), tier))
            {
                _writtenBesideIt = true;
                throw new InvalidOperationException("test: a second write of the tier began beside the one left running");
            }
        }
    }

    /// <summary>
    /// Round-5 review H-B, the swap. FlushTierByLevelAsync was boxed at its first real suspension, the
    /// await of its first level's write — a Task.Run already reading the tier — and a box that could not
    /// be allocated faulted the flush while that write ran on. The heavy phase takes the flush's own fault
    /// for "it has stopped" and hands the tier to a retry, which wrote it beside the orphan: two writers of
    /// one tier. The flush is boxed before any level write is queued now, so its failure leaves no write
    /// behind, and the retry is the tier's only writer.
    /// </summary>
    [Fact]
    public async Task A_swapped_tiers_flush_that_cannot_be_boxed_leaves_no_level_write_behind_for_its_retry_to_meet()
    {
        var engine = OneSlotEngine(Options(NewDir()));
        engine._flushRetryDelay = TimeSpan.FromMilliseconds(50);
        var watch = new LeftBehindWriteWatch();
        try
        {
            using (var batch = Batch(engine, 0, 500))
                Assert.Equal(new LogBatchWriteResult(500, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
            OwnerAsync.FirstSuspensionFailsForTest.Value  = watch.FailsFirstSuspension;
            StorageEngine.LevelWriteStartingForTest.Value = watch.OnLevelWrite;   // the retry the swap starts carries both
            try { await engine.FlushHotTierAsync(); }
            finally
            {
                OwnerAsync.FirstSuspensionFailsForTest.Value  = null;
                StorageEngine.LevelWriteStartingForTest.Value = null;
            }

            WaitFor(() => engine.FrozenTierCountForTest == 0 && engine.HeavyPhasesInFlight == 0, "the tier to be published by its retry", ShortGuard);
            Assert.False(watch.LeftWriteBehind, "the flush that could not be boxed had queued a level write, which ran on after its fault");
            Assert.False(watch.WrittenBesideIt, "the retry wrote the tier beside the level write the unboxed flush had left running");
            Assert.False(watch.FreedUnderIt, "a tier was freed under a level write");
            Assert.Equal(500, ReadAll(engine).Count);
        }
        finally { await engine.DisposeAsync(); }
    }

    /// <summary>
    /// H-B, the despill whose retry cannot be started: it gives its tier up — unlisted and retired, freed —
    /// which it did under the level write its unboxed flush had left running (a use-after-free).
    /// </summary>
    [Fact]
    public async Task A_despills_flush_that_cannot_be_boxed_and_whose_retry_cannot_start_frees_no_tier_under_a_write()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        EarlierRunSpill(opts, dir, 9_000, 100_000, 50);
        var watch = new LeftBehindWriteWatch();
        var hold  = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value        = hold.Task;
        OwnerAsync.FirstSuspensionFailsForTest.Value  = watch.FailsFirstSuspension;
        StorageEngine.LevelWriteStartingForTest.Value = watch.OnLevelWrite;   // the despill loop carries them
        StorageEngine engine;
        try { engine = OneSlotEngine(opts); }
        finally
        {
            StorageEngine.HoldDespillForTest.Value        = null;
            OwnerAsync.FirstSuspensionFailsForTest.Value  = null;
            StorageEngine.LevelWriteStartingForTest.Value = null;
        }
        int refused = 0;
        engine._beforeFlushRetryStartForTest = () =>
        {
            if (Interlocked.Exchange(ref refused, 1) == 0) throw new OutOfMemoryException("test: no retry can be set up");
        };
        await using (engine)
        {
            engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
            engine._flushRetryDelay   = TimeSpan.FromMilliseconds(50);
            hold.SetResult();
            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == 50, "the block to be despilled", ShortGuard);
            Assert.Equal(1, Volatile.Read(ref refused));
            Assert.False(watch.LeftWriteBehind, "the despill's flush that could not be boxed had queued a level write, which ran on after its fault");
            Assert.False(watch.FreedUnderIt, "the despill's tier was freed under the level write its unboxed flush had left running");
            Assert.False(watch.WrittenBesideIt, "the despill's tier was written twice at once");
            Assert.Equal(50, ReadAll(engine).Count);
        }
    }

    /// <summary>
    /// H-B, the startup replay: its flush's fault reached GetResult, and the replay's `using` freed the
    /// recovery tier under the level write the unboxed flush had left running.
    /// </summary>
    [Fact]
    public async Task A_replays_flush_that_cannot_be_boxed_frees_no_recovery_tier_under_a_write()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        Directory.CreateDirectory(Path.Combine(dir, "wal"));
        WriteThreeEventLog(Path.Combine(dir, "wal", $"{opts.NodeId.Value}-9000.wal"), opts, 9_000);
        var watch = new LeftBehindWriteWatch();
        OwnerAsync.FirstSuspensionFailsForTest.Value  = watch.FailsFirstSuspension;
        StorageEngine.LevelWriteStartingForTest.Value = watch.OnLevelWrite;
        StorageEngine engine;
        try { engine = Engine(opts); }
        finally
        {
            OwnerAsync.FirstSuspensionFailsForTest.Value  = null;
            StorageEngine.LevelWriteStartingForTest.Value = null;
        }
        await using (engine)
        {
            await engine.CatalogLoaded;
            WaitFor(() => engine.SpillFilesPending == 0, "the WAL to be written", ShortGuard);
            Assert.False(watch.LeftWriteBehind, "the replay's flush that could not be boxed had queued a level write, which ran on after its fault");
            Assert.False(watch.FreedUnderIt, "the recovery tier was freed under the level write the unboxed flush had left running");
            AssertThreeEventsWhole(ReadAll(engine));
        }
    }

    // ── Publishing a flushed tier is all or nothing ───────────────────────────

    /// <summary>A tier of two levels: 300 Information events, then 200 Error ones.</summary>
    private static async Task WriteTwoLevelsAsync(StorageEngine engine)
    {
        using (var info = Batch(engine, 0, 300))
            Assert.Equal(new LogBatchWriteResult(300, 0, 0), await engine.WriteBatchAsync(info, HangGuard));
        using (var error = Batch(engine, 300, 200, level: LogLevel.Error))
            Assert.Equal(new LogBatchWriteResult(200, 0, 0), await engine.WriteBatchAsync(error, HangGuard));
    }

    /// <summary>
    /// Round-4 review L23. PublishFlushedTier registers a flushed tier's levels under the frozen lock,
    /// one dictionary node each: one that ran out of memory part way left the levels before it
    /// registered, while the retry took the throw for "not published" — and its next attempt unlinked
    /// their files as leftovers: registered segments, gone from disk under the catalog until the rewrite
    /// put them back. What a failed registration got in is taken back now, so nothing registered is
    /// ever a leftover.
    /// </summary>
    [Fact]
    public async Task A_retry_whose_publish_fails_part_way_takes_back_what_it_registered_and_unlinks_nothing_registered()
    {
        var engine = OneSlotEngine(Options(NewDir()));
        engine._flushRetryDelay = TimeSpan.FromMilliseconds(50);
        int flushFailed = 0, registrations = 0, gone = 0;
        engine._afterLevelPublished = _ =>
        {
            if (Interlocked.Exchange(ref flushFailed, 1) == 0) throw new IOException("test: the swap's flush fails after a level");
        };
        engine._beforeSegmentPublishForTest = _ =>
        {
            // The retry's first publish: its second level's registration runs out of memory.
            if (Interlocked.Increment(ref registrations) == 2) throw new OutOfMemoryException("test: a dictionary node for the second level");
        };
        engine._beforeLevelWriteForTest = _ =>
        {
            foreach (var seg in engine.ListSegments())
                if (!File.Exists(seg.FilePath)) Interlocked.Increment(ref gone);
        };
        try
        {
            await WriteTwoLevelsAsync(engine);
            await engine.FlushHotTierAsync();   // fails after its first level: a retry takes the tier

            WaitFor(() => engine.FrozenTierCountForTest == 0 && engine.HeavyPhasesInFlight == 0, "the retry to publish the tier", ShortGuard);
            Assert.True(Volatile.Read(ref registrations) >= 4, "the retry's publish never failed part way");
            Assert.True(Volatile.Read(ref gone) == 0, "a registered segment's file was unlinked by the next attempt");
            Assert.Equal(500, ReadAll(engine).Count);
        }
        finally { await engine.DisposeAsync(); }
    }

    /// <summary>
    /// L23, the swap's own publish: one that failed was left to the heavy phase's finally — the tier
    /// listed until shutdown (and served twice where a level had got in), its block never published in
    /// this process. Nothing registered now, it goes to a retry as a failed flush does.
    /// </summary>
    [Fact]
    public async Task A_swap_whose_publish_fails_hands_its_tier_to_a_retry_and_it_is_published_once()
    {
        var engine = OneSlotEngine(Options(NewDir()));
        engine._flushRetryDelay = TimeSpan.FromMilliseconds(50);
        int registrations = 0;
        engine._beforeSegmentPublishForTest = _ =>
        {
            if (Interlocked.Increment(ref registrations) == 2) throw new OutOfMemoryException("test: a dictionary node for the second level");
        };
        try
        {
            await WriteTwoLevelsAsync(engine);
            try { await engine.FlushHotTierAsync(); }
            catch (OutOfMemoryException) { /* before the fix the publish's throw escaped the heavy phase */ }

            WaitFor(() => engine.FrozenTierCountForTest == 0 && engine.HeavyPhasesInFlight == 0, "the tier to be published by a retry", ShortGuard);
            Assert.Equal(500, ReadAll(engine).Count);
        }
        finally { await engine.DisposeAsync(); }
    }

    /// <summary>
    /// L23, the despill's publish: one that failed queued the block again while its tier stayed listed
    /// for good, served beside the block's second despill. It goes to a retry now.
    /// </summary>
    [Fact]
    public async Task A_despill_whose_publish_fails_hands_its_tier_to_a_retry_and_the_block_is_published_once()
    {
        string dir    = NewDir();
        var    opts   = Options(dir);
        string walDir = Path.Combine(dir, "wal");
        Directory.CreateDirectory(walDir);
        using (var spill = WriteAheadLog.Open(Path.Combine(walDir, $"{opts.NodeId.Value}-9000{StorageEngine.SpillExtension}"),
                                              opts.NodeId, new SegmentId(9_000), 1 << 20))
            for (int i = 0; i < 100; i++)
                spill.Append(T0 + i * TimeSpan.TicksPerMillisecond, i < 60 ? LogLevel.Information : LogLevel.Error, 40, "event {n}", Props(i),
                             serviceIndex: 41, service: "spill.svc");

        var hold = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        StorageEngine engine;
        try { engine = OneSlotEngine(opts); }
        finally { StorageEngine.HoldDespillForTest.Value = null; }
        int registrations = 0;
        engine._beforeSegmentPublishForTest = _ =>
        {
            if (Interlocked.Increment(ref registrations) == 2) throw new OutOfMemoryException("test: a dictionary node for the second level");
        };
        await using (engine)
        {
            engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
            engine._flushRetryDelay   = TimeSpan.FromMilliseconds(50);
            hold.SetResult();
            WaitFor(() => engine.SpillFilesPending == 0 && engine.FrozenTierCountForTest == 0 && engine.HeavyPhasesInFlight == 0,
                    "the block to be published once, its tier unlisted", ShortGuard);
            Assert.Equal(100, ReadAll(engine).Count);
            Assert.Equal(100, engine.DespilledEvents);
        }
    }

    // ── The text a tier's events carry of their own (round-5 H-D) ─────────────

    /// <summary>
    /// Flushes until nothing is left in memory or in a spill file: a flush request that found no slot, or
    /// the frozen tiers at their share of text of their events' own, did nothing, and is asked again.
    /// </summary>
    private static async Task FlushEverythingAsync(StorageEngine engine, TimeSpan within)
    {
        var until = DateTime.UtcNow + within;
        while (true)
        {
            await engine.FlushHotTierAsync();
            if (engine.LiveTierCountForTest == 0 && engine.FrozenTierCountForTest == 0 && !engine.IsSpilling &&
                engine.SpillFilesPending == 0 && engine.HeavyPhasesInFlight == 0)
                return;
            Assert.True(DateTime.UtcNow < until,
                        $"not flushed within {within}: {engine.LiveTierCountForTest} live event(s), {engine.FrozenTierCountForTest} frozen tier(s), " +
                        $"spilling {engine.IsSpilling}, {engine.SpillFilesPending} spill file(s) pending");
            await Task.Delay(20);
        }
    }

    /// <summary>
    /// Round-5 review H-D. Past the intern pool's budget — or past the longest text it takes — a template
    /// is a string of the event's own, and the tier held every one until it flushed: its fullness counted
    /// events and payload bytes only, and the batch that had been charged for the string let go of it at
    /// its commit. 41 requests of a distinct 4 M-character template ran a 384 MiB heap out while the tier's
    /// native memory sat at 9 MiB; a CLEF exception did the same with no pool involved. The tier counts that
    /// text against its size now, and a full one is swapped like any other: 600 events of 40 KB of text
    /// each (24 MB) go into at least three 8 MB tiers — one used to take them all.
    /// </summary>
    [Theory]
    [InlineData("template")]
    [InlineData("exception")]
    public async Task A_tier_counts_the_text_its_events_carry_of_their_own_and_is_swapped_when_that_reaches_its_size(string carried)
    {
        const int Events = 600, Chars = 20_000;
        var engine = Engine(Options(NewDir(), tierBytes: 8 * MB));
        try
        {
            int tmpl = engine.TemplatePool.Intern("event {n}");
            for (int i = 0; i < Events; i++)
            {
                string own = new string((char)('a' + i % 26), Chars) + i;   // a fresh string of the event's own, as past the budget
                using var batch = new LogWriteBatch();
                if (carried == "template")
                    batch.Add(new LogEventHeader
                    {
                        TimestampUtcTicks = T0 + i * TimeSpan.TicksPerMillisecond, Level = LogLevel.Information,
                        MessageTemplatePoolIndex = -1, ServiceNamePoolIndex = -1,
                    }, Props(i), own, exception: null);
                else
                    batch.Add(new LogEventHeader
                    {
                        TimestampUtcTicks = T0 + i * TimeSpan.TicksPerMillisecond, Level = LogLevel.Information,
                        MessageTemplatePoolIndex = tmpl, ServiceNamePoolIndex = -1,
                    }, Props(i), "event {n}", new ExceptionInfo { Type = "E", Message = "m", StackTrace = own });
                Assert.Equal(1, (await engine.WriteBatchAsync(batch, HangGuard)).Written);
            }
            // A tier of text that found the frozen ones at their share of it spilled, or waited (see the test below).
            await FlushEverythingAsync(engine, ShortGuard);

            int tiers = engine.ListSegments().Count;   // one level: one segment a tier or spilled block
            Assert.True(tiers >= 3, $"{Events} events with {Events * (long)Chars * 2 / MB} MB of their own text went into {tiers} tier(s) of 8 MB");
            Assert.Equal(Events, ReadAll(engine).Count);
        }
        finally { await engine.DisposeAsync(); }
    }

    /// <summary>
    /// Round-5 review H-D, the frozen backlog's half. Each tier bounds the text its events carry of their
    /// own, but the frozen tiers were bounded only in number, by their native footprint against the
    /// physical limit — on a 384 MiB heap limit over a host's RAM, 30 frozen tiers of one 8 MB CLEF stack
    /// each ran the heap out. They hold at most as many tiers' worth of that text as flushes run at once
    /// now: with one flush, and the first tier's held, the second tier of text does not freeze beside it —
    /// its events spill — and every event is there once the flush goes on.
    /// </summary>
    [Fact]
    public async Task The_frozen_tiers_hold_no_more_of_their_events_own_text_than_their_flushes_run_at_once()
    {
        // 40 KB of text each: four 8 MB tiers' worth — one frozen and held, one live and full, and more
        // than a tier's worth spilled.
        const int Events = 900, Chars = 20_000;
        var engine = Engine(Options(NewDir(), tierBytes: 8 * MB));   // one flush at a time (FlushConcurrency 1)
        var gate   = HoldFirstFlush(engine);
        try
        {
            Assert.True(engine.FlushWidth == 1 && engine.FlushSlots > 2,
                        $"setup: width {engine.FlushWidth}, {engine.FlushSlots} slot(s) — the slots alone must leave room for more frozen tiers");
            for (int i = 0; i < Events; i++)
            {
                using var batch = new LogWriteBatch();
                batch.Add(new LogEventHeader
                {
                    TimestampUtcTicks = T0 + i * TimeSpan.TicksPerMillisecond, Level = LogLevel.Information,
                    MessageTemplatePoolIndex = -1, ServiceNamePoolIndex = -1,
                }, Props(i), new string((char)('a' + i % 26), Chars) + i, exception: null);
                Assert.Equal(1, (await engine.WriteBatchAsync(batch, HangGuard)).Written);
            }

            Assert.True(engine.FrozenTierCountForTest == 1 && engine.SpilledEvents > 0,
                        $"with the first tier's flush held: {engine.FrozenTierCountForTest} frozen tier(s), " +
                        $"{engine.FrozenTextBytesForTest / MB} MB of their events' own text, {engine.SpilledEvents} event(s) spilled");

            gate.Set();
            await FlushEverythingAsync(engine, HangGuard);
            Assert.Equal(Events, ReadAll(engine).Count);
            Assert.Equal(0, engine.FrozenTextBytesForTest);

            // And a spilled block holds no more of that text than a tier: its despill's tier counts none,
            // so the spill's admission does (one segment a block or tier: one level).
            foreach (var s in engine.ListSegments())
                Assert.True(s.EventCount <= 8 * MB / 40_000 + 1,
                            $"segment {s.Id} holds {s.EventCount} events of 40 KB of text each — past an 8 MB tier's");
        }
        finally { gate.Set(); await engine.DisposeAsync(); }
    }

    /// <summary>
    /// Round-5 review H-D: the frozen text goes back with the tier's slot whichever owner gives it — here a
    /// flush retry, after a first attempt that failed. Kept, it would hold the next tier of text out of the
    /// frozen backlog for good: every event after it spilled, and none was despilled.
    /// </summary>
    [Fact]
    public async Task A_flush_retry_gives_its_tiers_text_back_with_its_slot()
    {
        var engine = Engine(Options(NewDir(), tierBytes: 8 * MB));
        engine._flushRetryDelay = TimeSpan.FromMilliseconds(50);
        int failures = 1;
        engine._beforeLevelWriteForTest = _ =>
        {
            if (Interlocked.Exchange(ref failures, 0) == 1) throw new IOException("test: the first level write fails");
        };
        try
        {
            for (int i = 0; i < 4; i++)
            {
                using var batch = new LogWriteBatch();
                batch.Add(new LogEventHeader
                {
                    TimestampUtcTicks = T0 + i * TimeSpan.TicksPerMillisecond, Level = LogLevel.Information,
                    MessageTemplatePoolIndex = -1, ServiceNamePoolIndex = -1,
                }, Props(i), new string((char)('a' + i), 1_000_000) + i, exception: null);   // 2 MB each
                Assert.Equal(1, (await engine.WriteBatchAsync(batch, HangGuard)).Written);
            }
            await engine.FlushHotTierAsync();
            WaitFor(() => engine.FrozenTierCountForTest == 0 && engine.HeavyPhasesInFlight == 0, "the tier to be flushed by its retry", ShortGuard);
            Assert.Equal(0, Volatile.Read(ref failures));   // the first attempt did fail
            Assert.Equal(0, engine.FrozenTextBytesForTest);
            Assert.Equal(4, ReadAll(engine).Count);
        }
        finally { await engine.DisposeAsync(); }
    }

    /// <summary>
    /// Round-5 review M-E. The intern pool runs out by the count of its entries or by the text they hold,
    /// and the warning that says so named only the count ("exhausted at 65536 entries … templates built by
    /// interpolation"), so a pool that a few long texts had spent read as one built by interpolation. It
    /// names both now, and the longest text a pool takes.
    /// </summary>
    [Fact]
    public async Task The_pool_exhausted_warning_names_the_text_the_pool_holds_as_well_as_its_entries()
    {
        var log    = new RecordingLogger();
        var opts   = Options(NewDir());
        var engine = new StorageEngine(Microsoft.Extensions.Options.Options.Create(opts),
                                       new RetentionStore(opts, NullLogger<RetentionStore>.Instance), log);
        try
        {
            // Texts the pool takes, until its text is spent: about a thousand.
            int each = StringInternPool.DefaultMaxPooledTextChars, i = 0;
            while (engine.TemplatePool.Intern(new string((char)('a' + i % 26), each - 8) + i.ToString("D8")) >= 0)
                Assert.True(++i < 4_096, "setup: the pool never ran out of text");

            var line = Assert.Single(log.Lines, l => l.Line.Contains("intern pool exhausted")).Line;
            Assert.True(line.Contains($" of {StringInternPool.DefaultMaxPoolSize} entries") &&
                        line.Contains($"of {StringInternPool.DefaultMaxPooledChars} characters of text") &&
                        line.Contains($"over {StringInternPool.DefaultMaxPooledTextChars} characters is never pooled"),
                        $"the warning: {line}");
        }
        finally { await engine.DisposeAsync(); }
    }

    // ── A publish that fails keeps its block (round-5 H-A) ────────────────────

    /// <summary>A spill file an earlier run left: block 9 000, 100 events — 60 Information, then 40 Error.</summary>
    private static void TwoLevelSpillBlock(ServerOptions opts, string dir)
    {
        string walDir = Path.Combine(dir, "wal");
        Directory.CreateDirectory(walDir);
        using var spill = WriteAheadLog.Open(Path.Combine(walDir, $"{opts.NodeId.Value}-9000{StorageEngine.SpillExtension}"),
                                             opts.NodeId, new SegmentId(9_000), 1 << 20);
        for (int i = 0; i < 100; i++)
            spill.Append(T0 + i * TimeSpan.TicksPerMillisecond, i < 60 ? LogLevel.Information : LogLevel.Error, 40, "event {n}", Props(i),
                         serviceIndex: 41, service: "spill.svc",
                         traceIdHi: 0xA000_0000_0000_0000UL | (uint)i, traceIdLo: (ulong)i * 7 + 1, spanId: (ulong)i * 13 + 1);
    }

    /// <summary>A one-slot engine whose despill waits for <paramref name="hold"/>, so a test can set its seams first.</summary>
    private static StorageEngine HeldDespillEngine(ServerOptions opts, TaskCompletionSource hold)
    {
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        try { return OneSlotEngine(opts); }
        finally { StorageEngine.HoldDespillForTest.Value = null; }
    }

    /// <summary>The second registration of a flushed tier's levels runs out of memory, once: a publish that fails part way.</summary>
    private static Func<int> FailSecondRegistration(StorageEngine engine)
    {
        int registrations = 0;
        engine._beforeSegmentPublishForTest = _ =>
        {
            if (Interlocked.Increment(ref registrations) == 2) throw new OutOfMemoryException("test: a dictionary node for the second level");
        };
        return () => Volatile.Read(ref registrations);
    }

    /// <summary>
    /// Round-5 H-A, the despill's give-up. A despill whose flush wrote its levels — and its completion
    /// marker, last, before the publish — and whose publish then failed, with no retry to be started,
    /// gave its tier up and unlinked the levels with the marker left: the block's next despill took the
    /// marker for a block an earlier life had written and deleted the spill file unread. Every event of
    /// the block lost, in this run and at every start after. The marker goes first now, and the next
    /// despill writes the block.
    /// </summary>
    [Fact]
    public async Task A_despill_whose_publish_fails_and_whose_retry_cannot_start_writes_its_block_again()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        TwoLevelSpillBlock(opts, dir);
        var hold   = new TaskCompletionSource();
        var engine = HeldDespillEngine(opts, hold);
        FailSecondRegistration(engine);
        int refused = 0;
        engine._beforeFlushRetryStartForTest = () =>
        {
            if (Interlocked.Exchange(ref refused, 1) == 0) throw new OutOfMemoryException("test: no retry can be set up");
        };
        await using (engine)
        {
            engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
            engine._flushRetryDelay   = TimeSpan.FromMilliseconds(50);
            hold.SetResult();
            WaitFor(() => engine.SpillFilesPending == 0 && engine.FrozenTierCountForTest == 0 && engine.HeavyPhasesInFlight == 0,
                    "the block to settle", ShortGuard);

            Assert.Equal(1, Volatile.Read(ref refused));
            var events = ReadAll(engine);
            Assert.True(events.Count == 100,
                $"{events.Count} of the block's 100 events are in segments: its next despill took the marker for a written block and deleted the spill file");
            foreach (var (n, ev) in events) AssertIsEvent(n, ev);
        }
    }

    /// <summary>
    /// H-A, the swap's retry. A swapped tier whose publish failed went to a retry, whose attempt unlinked
    /// the levels — under the marker the flush had written — before writing them again. Stopped before
    /// a rewrite landed (here every rewrite fails, as the publish did), the next start found the marker,
    /// took the WAL for flushed and deleted it unread. The marker goes before the levels now: the next
    /// start replays the WAL.
    /// </summary>
    [Fact]
    public async Task A_swapped_tier_whose_publish_fails_and_whose_retry_stops_before_its_rewrite_comes_back_at_the_next_start()
    {
        string dir    = NewDir();
        var    opts   = Options(dir);
        var    engine = OneSlotEngine(opts);
        engine._flushRetryDelay = TimeSpan.FromMilliseconds(50);
        var registrations = FailSecondRegistration(engine);
        int rewrites = 0;
        engine._beforeLevelWriteForTest = _ =>
        {
            if (registrations() < 2) return;   // the swap's own flush writes; the retry's rewrites fail
            Interlocked.Increment(ref rewrites);
            throw new IOException("test: the retry's rewrite fails as the publish did");
        };
        try
        {
            await WriteTwoLevelsAsync(engine);
            await engine.FlushHotTierAsync();   // its publish fails: a retry takes the tier
            WaitFor(() => Volatile.Read(ref rewrites) >= 2, "the retry's rewrite to fail twice", ShortGuard);
        }
        finally { await engine.DisposeAsync(); }

        await using var restarted = OneSlotEngine(opts);
        await restarted.CatalogLoaded;
        WaitFor(() => restarted.SpillFilesPending == 0, "the start's despills to finish", ShortGuard);
        var events = ReadAll(restarted);
        Assert.True(events.Count == 500,
            $"{events.Count} of the tier's 500 events came back: the start took the WAL for flushed and deleted it unread");
        foreach (var (n, ev) in events) AssertIsEvent(n, ev);
    }

    /// <summary>
    /// H-A, the despill's retry: as the swap's, its attempt unlinked the levels under the marker before
    /// writing them again, and a stop before the rewrite left the next start's despill to find the marker
    /// and delete the spill file unread.
    /// </summary>
    [Fact]
    public async Task A_despill_whose_publish_fails_and_whose_retry_stops_before_its_rewrite_writes_its_block_at_the_next_start()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        TwoLevelSpillBlock(opts, dir);
        var hold   = new TaskCompletionSource();
        var engine = HeldDespillEngine(opts, hold);
        var registrations = FailSecondRegistration(engine);
        int rewrites = 0;
        engine._beforeLevelWriteForTest = _ =>
        {
            if (registrations() < 2) return;
            Interlocked.Increment(ref rewrites);
            throw new IOException("test: the retry's rewrite fails as the publish did");
        };
        try
        {
            engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
            engine._flushRetryDelay   = TimeSpan.FromMilliseconds(50);
            hold.SetResult();
            WaitFor(() => Volatile.Read(ref rewrites) >= 2, "the retry's rewrite to fail twice", ShortGuard);
        }
        finally { await engine.DisposeAsync(); }

        await using var restarted = OneSlotEngine(opts);
        await restarted.CatalogLoaded;
        WaitFor(() => restarted.SpillFilesPending == 0, "the block to be despilled", ShortGuard);
        var events = ReadAll(restarted);
        Assert.True(events.Count == 100,
            $"{events.Count} of the block's 100 events came back: the start's despill found the marker and deleted the spill file unread");
        foreach (var (n, ev) in events) AssertIsEvent(n, ev);
    }

    /// <summary>
    /// H-A, a despill's retry that ends without finishing (its first suspension cannot be boxed): its
    /// continuation gives the tier up as the despill does (RetryEnded's onAbandoned) — and unlinked the
    /// levels under the marker the same way, the block's next despill deleting the spill file unread.
    /// </summary>
    [Fact]
    public async Task A_despill_whose_publish_fails_and_whose_retry_dies_unboxed_writes_its_block_again()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        TwoLevelSpillBlock(opts, dir);
        var hold = new TaskCompletionSource();
        OwnerAsync.FirstSuspensionFailsForTest.Value = FailFirstSuspensionOnce("<FlushRetryLoopAsync>");
        StorageEngine engine;
        try { engine = HeldDespillEngine(opts, hold); }   // the despill loop, and the retry it starts, carry the seam
        finally { OwnerAsync.FirstSuspensionFailsForTest.Value = null; }
        FailSecondRegistration(engine);
        await using (engine)
        {
            engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
            engine._flushRetryDelay   = TimeSpan.FromMilliseconds(50);
            hold.SetResult();
            WaitFor(() => engine.SpillFilesPending == 0 && engine.FrozenTierCountForTest == 0 && engine.HeavyPhasesInFlight == 0,
                    "the block to settle", ShortGuard);

            var events = ReadAll(engine);
            Assert.True(events.Count == 100,
                $"{events.Count} of the block's 100 events are in segments: the dead retry's give-up unlinked the levels under the marker");
            foreach (var (n, ev) in events) AssertIsEvent(n, ev);
        }
    }

    /// <summary>
    /// H-A, a marker that cannot be removed: the despill's give-up keeps it and all it vouches for — the
    /// levels, the tier (listed, served, until shutdown) and the block (in flight, despilled by nobody in
    /// this run) — and the next start finishes the block. Giving the tier up with the marker kept left
    /// the block's next despill to delete the spill file under the marker while the levels it vouches for
    /// were in no catalog: the events served by nobody until a restart.
    /// </summary>
    [Fact]
    public async Task A_despill_give_up_whose_marker_cannot_be_removed_keeps_its_tier_served_and_the_next_start_finishes_the_block()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        TwoLevelSpillBlock(opts, dir);
        var hold   = new TaskCompletionSource();
        var engine = HeldDespillEngine(opts, hold);
        FailSecondRegistration(engine);
        engine._beforeFlushRetryStartForTest = () => throw new OutOfMemoryException("test: no retry can be set up");
        int removals = 0;
        engine._removeFlushMarkerForTest = _ =>
        {
            Interlocked.Increment(ref removals);
            throw new IOException("test: the marker cannot be removed");
        };
        try
        {
            engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
            hold.SetResult();
            WaitFor(() => engine.DespillsCompletedForTest == 1 && engine.HeavyPhasesInFlight == 0, "the despill to give up", ShortGuard);

            Assert.True(Volatile.Read(ref removals) >= 1, "the give-up never tried to remove the marker");
            Assert.True(engine.FrozenTierCountForTest == 1, "the tier was given up under a marker that stays");
            Assert.Equal(1, engine.SpillFilesPending);
            using (var reader = engine.OpenHotTierReader())
                Assert.Equal(100, reader.ReadAll().Count());   // served, from the tier
            Assert.Empty(ReadAll(engine));                       // and from nothing else

            await Task.Delay(TimeSpan.FromMilliseconds(10 * 50));   // ten of the despill's retry delays
            Assert.True(engine.DespillsCompletedForTest == 1 && engine.SpillFilesPending == 1,
                        "the block was despilled again beside the tier kept for it");
        }
        finally { await engine.DisposeAsync(); }

        await using var restarted = OneSlotEngine(opts);
        await restarted.CatalogLoaded;
        WaitFor(() => restarted.SpillFilesPending == 0, "the next start to finish the block", ShortGuard);
        var events = ReadAll(restarted);
        Assert.Equal(100, events.Count);
        foreach (var (n, ev) in events) AssertIsEvent(n, ev);
    }

    // ── A block id past the last usable one ───────────────────────────────────

    /// <summary>
    /// R2-2. A WAL or spill file whose name or header says a block near the end of the id space cannot
    /// be one this store wrote. Reserving ulong.MaxValue − 2 wrapped the allocator's floor to 3, and the
    /// next blocks were handed out again; one a little lower left the next allocation to wrap; replaying
    /// it wrote levels at ids that wrap onto other blocks. Such a file (past long.MaxValue) is left in
    /// place, with an Error, and the allocator untouched.
    /// </summary>
    [Fact]
    public async Task A_log_naming_a_block_past_the_last_usable_id_is_left_alone_and_wraps_nothing()
    {
        const ulong Past = ulong.MaxValue - 2;
        string dir    = NewDir();
        var    opts   = Options(dir);
        string walDir = Path.Combine(dir, "wal");
        Directory.CreateDirectory(walDir);
        string wal   = Path.Combine(walDir, $"{opts.NodeId.Value}-{Past}.wal");
        string spill = Path.Combine(walDir, $"{opts.NodeId.Value}-{Past - 6}{StorageEngine.SpillExtension}");
        foreach (var (path, block) in new[] { (wal, Past), (spill, Past - 6) })
            using (var log = WriteAheadLog.Open(path, opts.NodeId, new SegmentId(block), 1 << 20))
                log.Append(T0, LogLevel.Fatal, 40, "event {n}", Props(0));
        Directory.CreateDirectory(Path.Combine(dir, "segments"));   // and a segment file named past it: the allocator's seed
        File.WriteAllBytes(Path.Combine(dir, "segments", $"{opts.NodeId.Value}-{Past}-0-0.seg"), [1, 2, 3]);

        ulong fresh;
        await using (var empty = Engine(Options(NewDir()))) fresh = empty.LiveWalSegmentId;

        await using var engine = Engine(opts);
        await engine.CatalogLoaded;

        Assert.Equal(fresh, engine.LiveWalSegmentId);   // the allocator did not take a wrapped floor
        Assert.True(File.Exists(wal), "the WAL past the last block id was replayed and deleted");
        Assert.True(File.Exists(spill));
        Assert.Equal(0, engine.SpillFilesPending);       // not queued for a despill into wrapping ids
        Assert.Empty(ReadAll(engine));
    }

    // ── A recovery walk that fails part way ───────────────────────────────────

    private static readonly ExceptionInfo WalkedException = new()
    {
        Type = "System.InvalidOperationException", Message = "the middle event's exception", StackTrace = "   at Orders.Handle()",
    };

    /// <summary>A log of three events, the middle one with an exception; the first and last use row 40, the middle row 41.</summary>
    private static void WriteThreeEventLog(string path, ServerOptions opts, ulong block)
    {
        using var wal = WriteAheadLog.Open(path, opts.NodeId, new SegmentId(block), 1 << 20);
        for (int i = 0; i < 3; i++)
            wal.Append(T0 + i * TimeSpan.TicksPerMillisecond, i == 1 ? LogLevel.Error : LogLevel.Information,
                       i == 1 ? 41 : 40, i == 1 ? "failed {n}" : "event {n}", Props(i),
                       exception: i == 1 ? WalkedException : null, serviceIndex: 42, service: "spill.svc",
                       traceIdHi: 0xA000_0000_0000_0000UL | (uint)i, traceIdLo: (ulong)i * 7 + 1, spanId: (ulong)i * 13 + 1);
    }

    private static void AssertThreeEventsWhole(Dictionary<long, RawSegmentEvent> events)
    {
        Assert.Equal(3, events.Count);
        Assert.Equal("event {n}",  events[0].Template);
        Assert.Equal("failed {n}", events[1].Template);
        Assert.Equal("event {n}",  events[2].Template);
        Assert.Equal(WalkedException.Message, events[1].Exception?.Message);
        Assert.All(events.Values, e => Assert.Equal("spill.svc", e.Service));
    }

    /// <summary>
    /// Mc. Decoding a checksummed entry's exception is not reading a torn entry: an out-of-memory
    /// there — a long stack trace on a tight heap — was taken for the end of the durable log, and the
    /// replay published the event before it and deleted the WAL with the rest. It propagates now: the
    /// replay fails ("WAL recovery failed") and the WAL stays. And (R2-3) the walk that failed part way
    /// frees its tier and its mapping: the file can be opened with no sharing at all.
    ///
    /// <para>Round-4 L14: and the WAL is read again in the same run, by the despill — held here until
    /// the checks above are made — once the heap allows: all three events come back whole and the WAL
    /// goes, without a restart. It used to wait for the next start.</para>
    /// </summary>
    [Fact]
    public async Task A_replay_that_runs_out_of_memory_decoding_an_exception_keeps_its_wal_until_the_despill_reads_it()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        Directory.CreateDirectory(Path.Combine(dir, "wal"));
        string wal  = Path.Combine(dir, "wal", $"{opts.NodeId.Value}-9000.wal");
        WriteThreeEventLog(wal, opts, 9_000);

        HotTierSegment? tier = null;
        int outOfMemory      = 1;   // the despill's context sees these seams too: they are switched off, not unset
        var hold             = new TaskCompletionSource();
        WriteAheadLog.BeforeExceptionDecodeForTest.Value = () =>
        {
            if (Volatile.Read(ref outOfMemory) == 1) throw new OutOfMemoryException("test: decoding the exception");
        };
        StorageEngine.RecoveryTierCreatedForTest.Value = t => tier ??= t;
        StorageEngine.HoldDespillForTest.Value         = hold.Task;
        StorageEngine run1;
        try { run1 = Engine(opts); }
        finally
        {
            WriteAheadLog.BeforeExceptionDecodeForTest.Value = null;
            StorageEngine.RecoveryTierCreatedForTest.Value   = null;
            StorageEngine.HoldDespillForTest.Value           = null;
        }
        await using (run1)
        {
            await run1.CatalogLoaded;
            Assert.True(File.Exists(wal), "the WAL was deleted after a replay that stopped part way");
            Assert.Empty(ReadAll(run1));
            Assert.NotNull(tier);
            Assert.True(tier!.IsDisposed, "the tier the failed walk filled was not freed");
            using (new FileStream(wal, FileMode.Open, FileAccess.ReadWrite, FileShare.None)) { }   // no mapping left on it
            Assert.Equal(1, run1.SpillFilesPending);                                                // counted until it is in segments

            Volatile.Write(ref outOfMemory, 0);
            hold.SetResult();
            WaitFor(() => run1.SpillFilesPending == 0, "the WAL to be read again by the despill", ShortGuard);
            AssertThreeEventsWhole(ReadAll(run1));
            Assert.False(File.Exists(wal), "the WAL was kept after its events reached segments");
        }

        await using var run2 = Engine(opts);
        await run2.CatalogLoaded;
        AssertThreeEventsWhole(ReadAll(run2));   // once each: nothing replayed a second time
    }

    /// <summary>
    /// Round-4 L14. A WAL the startup replay could not read — held by a scanner, here — is read again
    /// in the same run by the despill, with its backoff, and its events reach segments without a
    /// restart. The replay used to leave it for the next start: its events served by nobody, however
    /// soon the scanner let go.
    /// </summary>
    [Fact]
    public async Task A_wal_the_replay_cannot_open_is_read_again_by_the_despill_in_the_same_run()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        Directory.CreateDirectory(Path.Combine(dir, "wal"));
        string wal  = Path.Combine(dir, "wal", $"{opts.NodeId.Value}-9000.wal");
        WriteThreeEventLog(wal, opts, 9_000);

        var log  = new RecordingLogger();
        var hold = new TaskCompletionSource();
        StorageEngine.BeforeWalReplayReadForTest.Value = path =>
        {
            if (path == wal) throw new IOException("test: the file is held by a scanner");
        };
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        StorageEngine engine;
        try { engine = OneSlotEngine(opts, log); }
        finally
        {
            StorageEngine.BeforeWalReplayReadForTest.Value = null;
            StorageEngine.HoldDespillForTest.Value         = null;
        }
        await using (engine)
        {
            await engine.CatalogLoaded;
            Assert.Equal(1, engine.SpillFilesPending);
            Assert.Single(log.Lines, l => l.Level == Microsoft.Extensions.Logging.LogLevel.Error
                                          && l.Line.StartsWith($"WAL recovery failed for {wal} — the log is kept and read again", StringComparison.Ordinal));

            hold.SetResult();
            WaitFor(() => engine.SpillFilesPending == 0, "the WAL to be read again by the despill", ShortGuard);
            AssertThreeEventsWhole(ReadAll(engine));
            Assert.False(File.Exists(wal), "the WAL was kept after its events reached segments");
        }
    }

    /// <summary>
    /// Round-4 L18. A pool file whose log is gone is swept at the start. A log's pool is deleted with
    /// it, best-effort, and on Windows that delete fails while a handle to the file is open — a tick's
    /// fsync of the pool, off the pool's lock, holds one past the log's dispose — and the .pool stayed
    /// for good: nothing lists them. A spill file's own pool is not touched.
    /// </summary>
    [Fact]
    public async Task Pool_files_whose_log_is_gone_are_swept_at_the_start()
    {
        string dir    = NewDir();
        var    opts   = Options(dir);
        string walDir = Path.Combine(dir, "wal");
        Directory.CreateDirectory(walDir);
        string walPool   = Path.Combine(walDir, $"{opts.NodeId.Value}-120.wal.pool");
        string spillPool = Path.Combine(walDir, $"{opts.NodeId.Value}-126{StorageEngine.SpillExtension}.pool");
        File.WriteAllBytes(walPool,   [1, 0, 1, 0, (byte)'x']);
        File.WriteAllBytes(spillPool, [1, 0, 1, 0, (byte)'x']);
        string spill = Path.Combine(walDir, $"{opts.NodeId.Value}-9000{StorageEngine.SpillExtension}");
        WriteThreeEventLog(spill, opts, 9_000);   // with its own pool, which stays

        var hold = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        StorageEngine engine;
        try { engine = Engine(opts); }
        finally { StorageEngine.HoldDespillForTest.Value = null; }
        await using (engine)
        {
            Assert.False(File.Exists(walPool),   "a log's pool outlived its log");
            Assert.False(File.Exists(spillPool), "a spill file's pool outlived its spill file");
            Assert.True(File.Exists(spill + ".pool"), "the pool of a spill file still waiting was swept");

            hold.SetResult();
            WaitFor(() => engine.SpillFilesPending == 0, "the spill file to be despilled", ShortGuard);
            AssertThreeEventsWhole(ReadAll(engine));
        }
    }

    /// <summary>
    /// Round-4 L14, a flush that fails part way: the replay's flush moves one level into place and
    /// cannot write the next (a directory stands where its file goes). The level it moved — registered
    /// by nobody — is deleted, the WAL goes to the despill, and once the way is clear the despill writes
    /// the whole block: every event once. Left there, the level would stop the despill's own flush,
    /// which never moves a level over a file.
    /// </summary>
    [Fact]
    public async Task A_wal_whose_replay_flush_fails_part_way_is_written_whole_by_the_despill()
    {
        string dir    = NewDir();
        var    opts   = Options(dir);
        string segDir = Path.Combine(dir, "segments");
        Directory.CreateDirectory(Path.Combine(dir, "wal"));
        Directory.CreateDirectory(segDir);
        string wal    = Path.Combine(dir, "wal", $"{opts.NodeId.Value}-9000.wal");
        WriteThreeEventLog(wal, opts, 9_000);
        // Where the Error level (block + 4) of the middle event goes: taken by a directory.
        long   errorAt = T0 + TimeSpan.TicksPerMillisecond;
        string blocker = Path.Combine(segDir, $"{opts.NodeId.Value}-{9_000 + (int)LogLevel.Error}-{errorAt}-{errorAt}.seg");
        Directory.CreateDirectory(blocker);

        var hold = new TaskCompletionSource();
        StorageEngine.HoldDespillForTest.Value = hold.Task;
        StorageEngine engine;
        try { engine = Engine(opts); }
        finally { StorageEngine.HoldDespillForTest.Value = null; }
        await using (engine)
        {
            await engine.CatalogLoaded;
            Assert.Equal(1, engine.SpillFilesPending);
            Assert.Empty(Directory.GetFiles(segDir, $"{opts.NodeId.Value}-{9_000 + (int)LogLevel.Information}-*.seg"));   // the level it moved, gone

            Directory.Delete(blocker);
            hold.SetResult();
            WaitFor(() => engine.SpillFilesPending == 0, "the WAL to be written by the despill", ShortGuard);
            AssertThreeEventsWhole(ReadAll(engine));
            Assert.False(File.Exists(wal), "the WAL was kept after its events reached segments");
        }
    }

    /// <summary>
    /// Round-5 H-C. The boot scan runs beside the startup replay, and can register a level the replay
    /// moved into place before the replay failed on a later one. The hand-off to the despill deleted
    /// that level and left the scan's entry: the catalog named a file that was gone, and once the
    /// despill or its retry had put the same level back at that path, a merge could take the stale
    /// entry, and the block's publication then stored the level's events twice. The hand-off takes out
    /// what the scan registered now. Here the scan runs, whole, between the failure and the hand-off.
    /// </summary>
    [Fact]
    public async Task A_level_the_boot_scan_registered_goes_out_of_the_catalog_with_the_failed_replays_hand_off()
    {
        string dir    = NewDir();
        var    opts   = Options(dir);
        string segDir = Path.Combine(dir, "segments");
        Directory.CreateDirectory(Path.Combine(dir, "wal"));
        Directory.CreateDirectory(segDir);
        WriteThreeEventLog(Path.Combine(dir, "wal", $"{opts.NodeId.Value}-9000.wal"), opts, 9_000);
        long   errorAt = T0 + TimeSpan.TicksPerMillisecond;
        string blocker = Path.Combine(segDir, $"{opts.NodeId.Value}-{9_000 + (int)LogLevel.Error}-{errorAt}-{errorAt}.seg");
        Directory.CreateDirectory(blocker);   // the replay fails on its Error level, after the Information one

        var     scan    = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var     logger  = new CountingLogger(line => line.StartsWith("Loaded ", StringComparison.Ordinal) && line.Contains(" segments from ", StringComparison.Ordinal));
        var     despill = new TaskCompletionSource();
        string? moved   = null;
        bool    scanned = false;
        StorageEngine.BeforeFailedReplayHandOffForTest.Value = _ =>
        {
            moved = Directory.GetFiles(segDir, $"{opts.NodeId.Value}-{9_000 + (int)LogLevel.Information}-*.seg").SingleOrDefault();
            scan.SetResult();   // the boot scan lists the directory and registers the level the replay moved
            scanned = SpinWait.SpinUntil(() => logger.Count == 1, HangGuard);
        };
        StorageEngine.HoldDespillForTest.Value = despill.Task;
        StorageEngine engine;
        try
        {
            engine = new StorageEngine(Microsoft.Extensions.Options.Options.Create(opts),
                                       new RetentionStore(opts, NullLogger<RetentionStore>.Instance), logger,
                                       maintenanceStartDelay: TimeSpan.FromHours(1), bootScanHeldUntil: scan.Task);
        }
        finally
        {
            StorageEngine.BeforeFailedReplayHandOffForTest.Value = null;
            StorageEngine.HoldDespillForTest.Value               = null;
        }
        await using (engine)
        {
            await engine.CatalogLoaded;
            Assert.True(scanned, "setup: the boot scan did not finish before the hand-off");
            Assert.NotNull(moved);                                                      // setup: the replay had moved its Information level
            Assert.False(File.Exists(moved), "setup: the hand-off did not delete the level the replay moved");
            Assert.Equal(1, engine.SpillFilesPending);
            var stale = engine.ListSegments().Where(s => !File.Exists(s.FilePath)).Select(s => Path.GetFileName(s.FilePath)).ToList();
            Assert.True(stale.Count == 0, $"the catalog names a level file the failed replay's hand-off deleted: {string.Join(", ", stale)}");

            Directory.Delete(blocker);
            despill.SetResult();
            WaitFor(() => engine.SpillFilesPending == 0, "the WAL to be written by the despill", ShortGuard);
            AssertThreeEventsWhole(ReadAll(engine));
        }
    }

    /// <summary>
    /// H-C's other half, and round-5 L-19: the merge planner took entries of a block a flush still
    /// owned. A swapped tier's flush fails after its Information level is in place, and a catalog scan
    /// registers that level — as the boot scan does with a level it lists in its first minutes — and a
    /// merge took it with a neighbour; the retry then wrote the level again and published it, and its
    /// events were stored twice. (A publish being rolled back left the planner the same kind of entry.)
    /// The planner leaves the blocks of frozen tiers and queued spills to their own flushes now.
    /// </summary>
    [Fact]
    public async Task A_merge_leaves_alone_a_level_of_a_block_whose_flush_is_being_retried()
    {
        var  engine = Engine(Options(NewDir()));
        long old    = DateTime.UtcNow.AddDays(-20).Date.Ticks;   // a sealed bucket: two small files are a batch
        void Write(int from, int count, LogLevel level)
        {
            for (int i = from; i < from + count; i++)
                Assert.True(engine.TryWrite(new LogEventHeader
                {
                    TimestampUtcTicks        = old + i * TimeSpan.TicksPerMillisecond,
                    Level                    = level,
                    MessageTemplatePoolIndex = engine.TemplatePool.Intern("event {n}"),
                }, Props(i), "event {n}"));
        }
        using var attempt = new ManualResetEventSlim(false);
        try
        {
            Write(0, 10, LogLevel.Information);
            await engine.FlushHotTierAsync();   // the neighbour: block A's Information level, published

            int failed = 0;
            engine._flushRetryDelay = TimeSpan.FromMilliseconds(50);
            engine._afterLevelPublished = _ =>
            {
                if (Interlocked.Exchange(ref failed, 1) == 0) throw new IOException("test: the flush fails after its first level");
            };
            engine._beforeFlushRetryAttemptForTest = () => attempt.Wait(HangGuard);
            Write(100, 10, LogLevel.Information);
            Write(200, 10, LogLevel.Error);
            await engine.FlushHotTierAsync();   // block B: its Information level in place, its Error one not; a retry has it
            Assert.Equal(1, engine.FrozenTierCountForTest);

            engine.LoadSegmentCatalog();        // a catalog scan registers what it lists, block B's level among it
            Assert.Equal(2, engine.ListSegments().Count(s => s.MinLevel == LogLevel.Information));   // setup

            engine._allowIndexlessMerge = true;
            await engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None);

            attempt.Set();
            WaitFor(() => engine.FrozenTierCountForTest == 0 && engine.HeavyPhasesInFlight == 0, "the retry to publish block B", ShortGuard);
            var dedup = new Dictionary<string, string>(StringComparer.Ordinal);
            var seen  = new Dictionary<long, int>();
            foreach (var seg in engine.ListSegments())
            {
                using var r = SegmentReader.Open(seg.FilePath);
                foreach (var ev in r.ReadAllRaw(dedup))
                    seen[ev.TsTicks] = seen.GetValueOrDefault(ev.TsTicks) + 1;
            }
            int twice = seen.Count(kv => kv.Value > 1);
            Assert.True(twice == 0, $"{twice} event(s) are stored twice: a merge took a level of the block its retry wrote again");
            Assert.Equal(30, seen.Count);
        }
        finally
        {
            attempt.Set();
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// Mc, for the pool. A pool file that could not be read part way — out of memory, here — came back
    /// as the rows read so far: the replay resolved the missing row's index through the LIVE pool and
    /// published the middle event with another template (or none), then deleted the WAL. The failure
    /// propagates now and the WAL stays; the next start replays every event with its own template.
    /// </summary>
    [Fact]
    public async Task A_replay_whose_pool_read_fails_part_way_keeps_its_wal_for_the_next_start()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        Directory.CreateDirectory(Path.Combine(dir, "wal"));
        string wal  = Path.Combine(dir, "wal", $"{opts.NodeId.Value}-9000.wal");
        WriteThreeEventLog(wal, opts, 9_000);

        WriteAheadLog.BeforePoolRowForTest.Value = index =>
        {
            if (index == 41) throw new OutOfMemoryException("test: reading the middle event's row");
        };
        StorageEngine run1;
        try { run1 = Engine(opts); }
        finally { WriteAheadLog.BeforePoolRowForTest.Value = null; }
        await using (run1)
        {
            await run1.CatalogLoaded;
            Assert.True(File.Exists(wal), "the WAL was deleted after a replay that read part of its pool");
        }

        await using var run2 = Engine(opts);
        await run2.CatalogLoaded;
        AssertThreeEventsWhole(ReadAll(run2));
    }

    /// <summary>
    /// Mc, for the despill: the read that ran out of memory decoding an exception despilled the event
    /// before it and deleted the spill file with the rest. It fails now, is backed off and read again
    /// (M5), and the block comes back whole; the tier the failed read filled is freed (R2-3).
    /// </summary>
    [Fact]
    public async Task A_despill_that_runs_out_of_memory_decoding_an_exception_reads_the_block_again()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        Directory.CreateDirectory(Path.Combine(dir, "wal"));
        string spill = Path.Combine(dir, "wal", $"{opts.NodeId.Value}-9000{StorageEngine.SpillExtension}");
        WriteThreeEventLog(spill, opts, 9_000);

        int thrown = 0;
        var tiers  = new System.Collections.Concurrent.ConcurrentQueue<HotTierSegment>();
        var hold   = new TaskCompletionSource();
        WriteAheadLog.BeforeExceptionDecodeForTest.Value = () =>
        {
            if (Interlocked.Exchange(ref thrown, 1) == 0) throw new OutOfMemoryException("test: decoding the exception");
        };
        StorageEngine.RecoveryTierCreatedForTest.Value = tiers.Enqueue;
        StorageEngine.HoldDespillForTest.Value         = hold.Task;
        StorageEngine engine;
        try { engine = Engine(opts); }
        finally
        {
            WriteAheadLog.BeforeExceptionDecodeForTest.Value = null;
            StorageEngine.RecoveryTierCreatedForTest.Value   = null;
            StorageEngine.HoldDespillForTest.Value           = null;
        }
        await using (engine)
        {
            engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
            hold.SetResult();
            WaitFor(() => engine.SpillFilesPending == 0, "the block to be despilled", ShortGuard);

            Assert.Equal(1, Volatile.Read(ref thrown));
            Assert.Equal(3, engine.DespilledEvents);
            Assert.True(tiers.TryPeek(out var first) && first.IsDisposed, "the tier the failed read filled was not freed");
            AssertThreeEventsWhole(ReadAll(engine));
        }
    }

    /// <summary>The three events of <see cref="WriteThreeEventLog"/>, the middle one without its exception.</summary>
    private static void AssertThreeEventsTheMiddleWithoutItsException(Dictionary<long, RawSegmentEvent> events)
    {
        Assert.Equal(3, events.Count);
        Assert.Equal("event {n}",  events[0].Template);
        Assert.Equal("failed {n}", events[1].Template);
        Assert.Equal("event {n}",  events[2].Template);
        Assert.Null(events[1].Exception);
        Assert.Equal((byte)Ameto.Core.LogLevel.Error, events[1].Level);
        Assert.All(events.Values, e => Assert.Equal("spill.svc", e.Service));
    }

    /// <summary>
    /// Round-4 L13, the poison pill, at the startup replay. A checksummed entry whose exception the
    /// decoder refuses — not out of memory: the bytes themselves — failed the replay at every start,
    /// the WAL kept and its events served by nobody, and a rollback waiting for a start that logs
    /// neither replay line waited for ever. The event is written without its exception now, the others
    /// whole, the WAL removed, and an Error names the log and the event.
    /// </summary>
    [Fact]
    public async Task A_replay_whose_exception_cannot_be_decoded_writes_every_event_without_it_and_says_so()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        Directory.CreateDirectory(Path.Combine(dir, "wal"));
        string wal  = Path.Combine(dir, "wal", $"{opts.NodeId.Value}-9000.wal");
        WriteThreeEventLog(wal, opts, 9_000);

        var log = new RecordingLogger();
        WriteAheadLog.BeforeExceptionDecodeForTest.Value = () => throw new InvalidDataException("test: the exception's bytes do not decode");
        StorageEngine engine;
        try { engine = OneSlotEngine(opts, log); }
        finally { WriteAheadLog.BeforeExceptionDecodeForTest.Value = null; }
        await using (engine)
        {
            await engine.CatalogLoaded;
            Assert.False(File.Exists(wal), "the WAL was kept");
            AssertThreeEventsTheMiddleWithoutItsException(ReadAll(engine));
            Assert.Single(log.Lines, l => l.Level == Microsoft.Extensions.Logging.LogLevel.Error
                                          && l.Line.Contains("1 event(s) are written to segments without their exception", StringComparison.Ordinal)
                                          && l.Line.Contains(wal, StringComparison.Ordinal));
            Assert.DoesNotContain(log.Lines, l => l.Line.Contains("WAL recovery failed", StringComparison.Ordinal));
        }
    }

    /// <summary>The same poison pill in a spill file: the despill retried it for ever, and the block never reached segments.</summary>
    [Fact]
    public async Task A_despill_whose_exception_cannot_be_decoded_writes_every_event_without_it()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        Directory.CreateDirectory(Path.Combine(dir, "wal"));
        string spill = Path.Combine(dir, "wal", $"{opts.NodeId.Value}-9000{StorageEngine.SpillExtension}");
        WriteThreeEventLog(spill, opts, 9_000);

        var log  = new RecordingLogger();
        var hold = new TaskCompletionSource();
        WriteAheadLog.BeforeExceptionDecodeForTest.Value = () => throw new InvalidDataException("test: the exception's bytes do not decode");
        StorageEngine.HoldDespillForTest.Value           = hold.Task;
        StorageEngine engine;
        try { engine = OneSlotEngine(opts, log); }
        finally
        {
            WriteAheadLog.BeforeExceptionDecodeForTest.Value = null;
            StorageEngine.HoldDespillForTest.Value           = null;
        }
        await using (engine)
        {
            engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
            hold.SetResult();
            WaitFor(() => engine.SpillFilesPending == 0, "the block to be despilled", ShortGuard);

            Assert.Equal(3, engine.DespilledEvents);
            AssertThreeEventsTheMiddleWithoutItsException(ReadAll(engine));
            Assert.Single(log.Lines, l => l.Level == Microsoft.Extensions.Logging.LogLevel.Error
                                          && l.Line.Contains("1 event(s) are written to segments without their exception", StringComparison.Ordinal));
        }
    }

    /// <summary>
    /// Round-4 L13: a flushed WAL — its completion marker beside it — is removed without a read. The
    /// marker was asked after the log's first entry had been read, and a first entry that could not be
    /// read (out of memory decoding its exception, here) failed, at every start, the replay of a log that
    /// only had to be deleted.
    /// </summary>
    [Fact]
    public async Task A_flushed_wal_is_removed_without_reading_its_first_entry()
    {
        string dir    = NewDir();
        var    opts   = Options(dir);
        string walDir = Path.Combine(dir, "wal");
        Directory.CreateDirectory(walDir);
        string wal    = Path.Combine(walDir, $"{opts.NodeId.Value}-9000.wal");
        string marker = Path.Combine(walDir, $"{opts.NodeId.Value}-9000.flushed");
        using (var log = WriteAheadLog.Open(wal, opts.NodeId, new SegmentId(9_000), 1 << 20))
            log.Append(T0, LogLevel.Error, 41, "failed {n}", Props(0), exception: WalkedException);
        File.WriteAllText(marker, "");

        int decodes = 0;
        WriteAheadLog.BeforeExceptionDecodeForTest.Value = () =>
        {
            Interlocked.Increment(ref decodes);
            throw new OutOfMemoryException("test: decoding the first entry's exception");
        };
        StorageEngine engine;
        try { engine = Engine(opts); }
        finally { WriteAheadLog.BeforeExceptionDecodeForTest.Value = null; }
        await using (engine)
        {
            await engine.CatalogLoaded;
            Assert.Equal(0, Volatile.Read(ref decodes));
            Assert.False(File.Exists(wal), "the flushed WAL was kept");
            Assert.False(File.Exists(marker), "the marker of a removed WAL was kept");
        }
    }

    // ── Recovery reads the log where it lies ──────────────────────────────────

    /// <summary>
    /// Padding per event in the tests below: properties enough that a copy of the log in the heap
    /// stands out from everything else a read allocates, and few enough (410 B, under the 512 B a
    /// chunk's arena gives each slot) that a tier's worth fills a chunk by count.
    /// </summary>
    private const int RecoveryPad = 400;

    /// <summary>
    /// The startup replay reads an orphaned WAL one entry at a time, where it lies in the file's
    /// mapping, straight into the recovery tier. It used to collect every entry first, each payload
    /// copied into a heap array, and so held the log twice — that list beside the tier it then
    /// filled — at the one moment, the start after a crash, a host has least to spare. Measured on
    /// the thread that reads, between the seams either side of the read: well under a quarter of
    /// the properties the log holds, where the copy alone was all of them. Every event comes back
    /// whole, its properties byte for byte, though the bytes it was written from were the mapping's.
    /// </summary>
    [Fact]
    public async Task The_startup_replay_reads_the_wal_where_it_lies_with_no_copy_of_it_in_the_heap()
    {
        string dir  = NewDir();
        var    opts = Options(dir, tierBytes: 4 * TierBytes);   // one tier, and so one WAL, takes them all
        const int Many = 2 * TierEvents;
        var engine = Engine(opts);
        try
        {
            using var batch = Batch(engine, 0, Many, RecoveryPad);
            Assert.Equal(Many, (await engine.WriteBatchAsync(batch, TimeSpan.FromSeconds(30))).Written);
            await KillKeepingTheWalAsync(engine, dir);
        }
        finally { await engine.DisposeAsync(); }

        // Paired by path: a clean stop leaves its successor's empty WAL beside the one restored, and
        // the replay looks at that one too — the before seam only, as an empty log is not read.
        var  before    = new Dictionary<string, long>();
        long allocated = -1;
        int  reads     = 0;
        StorageEngine.BeforeWalReplayReadForTest.Value = wal => before[wal] = GC.GetAllocatedBytesForCurrentThread();
        StorageEngine.AfterWalReplayReadForTest.Value  = wal =>
        {
            allocated = GC.GetAllocatedBytesForCurrentThread() - before[wal];
            reads++;
        };
        StorageEngine restarted;
        try { restarted = Engine(opts); }
        finally
        {
            StorageEngine.BeforeWalReplayReadForTest.Value = null;
            StorageEngine.AfterWalReplayReadForTest.Value  = null;
        }
        await using (restarted)
        {
            await restarted.CatalogLoaded;
            Assert.Equal(1, reads);   // the restored WAL, read whole on one thread

            long properties = (long)Many * RecoveryPad;
            Assert.True(allocated < properties / 4,
                $"reading a WAL of {properties:N0} bytes of properties allocated {allocated:N0} bytes in the heap — a copy of the log");

            var events = ReadAll(restarted);
            Assert.Equal(Many, events.Count);
            foreach (var (n, ev) in events)
            {
                AssertIsEvent(n, ev);
                Assert.Equal(Props(n, RecoveryPad), ev.Props);
            }
        }
    }

    /// <summary>
    /// The despill reads a spill file the same way, one entry at a time from its mapping into the
    /// tier it flushes, where it used to copy the whole block into the heap first: a backlog of
    /// spilled blocks came back at twice the memory each block takes — during an overload, which is
    /// when there are spilled blocks at all. Measured between the seams either side of each read,
    /// summed over the blocks.
    /// </summary>
    [Fact]
    public async Task A_despill_reads_the_spill_file_where_it_lies_with_no_copy_of_it_in_the_heap()
    {
        string dir    = NewDir();
        var    engine = OneSlotEngine(Options(dir));
        var    gate   = HoldFirstFlush(engine);
        const int Total = 3 * TierEvents + 1_000;          // A and B in tiers, the rest spilled
        using var started   = new ThreadLocal<long>();
        long      allocated = 0;
        int       reads     = 0;
        engine._beforeLoadSpillForTest = _ => started.Value = GC.GetAllocatedBytesForCurrentThread();
        engine._afterLoadSpillForTest  = _ =>
        {
            Interlocked.Add(ref allocated, GC.GetAllocatedBytesForCurrentThread() - started.Value);
            Interlocked.Increment(ref reads);
        };
        try
        {
            using (var batch = Batch(engine, 0, Total, RecoveryPad))
                Assert.Equal(new LogBatchWriteResult(Total, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
            Assert.True(engine.SpilledEvents >= TierEvents, $"only {engine.SpilledEvents} event(s) spilled");

            gate.Set();
            WaitFor(() => !engine.IsSpilling && engine.SpillFilesPending == 0
                       && engine.DespilledEvents == engine.SpilledEvents,
                    "the spill to end and every spilled block to be despilled");

            long properties = engine.SpilledEvents * RecoveryPad;
            Assert.True(Volatile.Read(ref reads) > 0, "no spill file was read");
            Assert.True(Volatile.Read(ref allocated) < properties / 4,
                $"reading {Volatile.Read(ref reads)} spill file(s), {properties:N0} bytes of properties, allocated "
              + $"{Volatile.Read(ref allocated):N0} bytes in the heap — a copy of the blocks");

            await engine.FlushHotTierAsync();
            var events = ReadAll(engine);
            Assert.Equal(Total, events.Count);
            foreach (var (n, ev) in events)
            {
                AssertIsEvent(n, ev);
                Assert.Equal(Props(n, RecoveryPad), ev.Props);
            }
        }
        finally
        {
            gate.Set();
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// kill -9 with <paramref name="engine"/>'s live WAL left behind: its bytes kept, the shutdown's
    /// flush of it undone, so the next start finds it orphaned and replays it.
    /// </summary>
    private static async Task KillKeepingTheWalAsync(StorageEngine engine, string dir)
    {
        ulong  walId = engine.LiveWalSegmentId;
        string wal   = Directory.GetFiles(Path.Combine(dir, "wal"), "*.wal").Single();
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
}
