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
    private static LogWriteBatch Batch(StorageEngine engine, int from, int count, int pad = 0)
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
            engine._afterWriteGate = null;
            foreach (var b in batches) b.Dispose();
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
    /// shutdown frees it; its WAL stays, and the next start replays every event once, though the failed
    /// attempt had moved a level file into place.
    /// </summary>
    [Fact]
    public async Task A_swapped_tier_whose_retry_cannot_start_stays_served_and_comes_back_once_at_the_next_start()
    {
        string dir    = NewDir();
        var    opts   = Options(dir);
        var    engine = OneSlotEngine(opts);
        int failed = 0;
        engine._afterLevelPublished = _ =>
        {
            if (Interlocked.Exchange(ref failed, 1) == 0) throw new IOException("test: the flush fails after a level");
        };
        engine._beforeFlushRetryStartForTest = () => throw new OutOfMemoryException("test: no retry can be started");
        try
        {
            using (var batch = Batch(engine, 0, 500))
                Assert.Equal(500, (await engine.WriteBatchAsync(batch, HangGuard)).Written);
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

        await using var restarted = OneSlotEngine(opts);
        await restarted.CatalogLoaded;
        var events = ReadAll(restarted);
        Assert.Equal(500, events.Count);
        foreach (var (n, ev) in events) AssertIsEvent(n, ev);
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
    /// replay fails ("WAL recovery failed"), the WAL stays for the next start, which replays all three.
    /// And (R2-3) the walk that failed part way frees its tier and its mapping: the file can be opened
    /// with no sharing at all.
    /// </summary>
    [Fact]
    public async Task A_replay_that_runs_out_of_memory_decoding_an_exception_keeps_its_wal_for_the_next_start()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        Directory.CreateDirectory(Path.Combine(dir, "wal"));
        string wal  = Path.Combine(dir, "wal", $"{opts.NodeId.Value}-9000.wal");
        WriteThreeEventLog(wal, opts, 9_000);

        HotTierSegment? tier = null;
        WriteAheadLog.BeforeExceptionDecodeForTest.Value = () => throw new OutOfMemoryException("test: decoding the exception");
        StorageEngine.RecoveryTierCreatedForTest.Value   = t => tier = t;
        StorageEngine run1;
        try { run1 = Engine(opts); }
        finally
        {
            WriteAheadLog.BeforeExceptionDecodeForTest.Value = null;
            StorageEngine.RecoveryTierCreatedForTest.Value   = null;
        }
        await using (run1)
        {
            await run1.CatalogLoaded;
            Assert.True(File.Exists(wal), "the WAL was deleted after a replay that stopped part way");
            Assert.Empty(ReadAll(run1));
            Assert.NotNull(tier);
            Assert.True(tier!.IsDisposed, "the tier the failed walk filled was not freed");
            using (new FileStream(wal, FileMode.Open, FileAccess.ReadWrite, FileShare.None)) { }   // no mapping left on it
        }

        await using var run2 = Engine(opts);
        await run2.CatalogLoaded;
        AssertThreeEventsWhole(ReadAll(run2));
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
