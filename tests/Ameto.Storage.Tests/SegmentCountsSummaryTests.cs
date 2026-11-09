using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// The counts summary answers <c>/api/events/counts</c> for a segment wholly inside the window
/// without decoding it (2026-11-09: the Statistics page's 1m band decoded a month of segments per
/// request on the 512 MB production stand). It is only worth having if it is EXACT, so most of
/// these hold it against the header scan it replaces, on the same data, event for event.
/// </summary>
public sealed class SegmentCountsSummaryTests
{
    private static readonly StringInternPool NoPool = new();

    // ── The builder against the per-event road ─────────────────────────────────────

    /// <summary>
    /// The arithmetic the summary rests on: a slot's bucket is the bucket of every event in it,
    /// for every grid that is a whole number of slots — before 1970 too, where the truncating
    /// division the aggregator uses is not a floor — and the service and level of a cell take the
    /// same road an event does (case folding, "(unknown)", bytes that are not UTF-8, levels the
    /// enum does not name, buckets off the axis).
    /// </summary>
    [Theory]
    [InlineData(900)]
    [InlineData(3_600)]
    [InlineData(7_200)]
    [InlineData(43_200)]
    [InlineData(86_400)]
    public void A_summary_adds_up_exactly_as_its_events_do(int bucketSeconds)
    {
        var events = Corpus();
        var builder = new SegmentCountsBuilder();
        foreach (var e in events) builder.Add(e.Ticks, e.Level, e.Service);
        long min = events.Min(e => e.Ticks), max = events.Max(e => e.Ticks);
        var summary = builder.Build((uint)events.Count, min, max);
        Assert.NotNull(summary);

        foreach (string? filter in new[] { null, "checkout", "CHECKOUT", "(unknown)", "absent" })
        {
            // An axis that starts inside the corpus and stops short of its end, so some events
            // fall off it at both ends and count toward the totals only.
            long minBucket = UnixSeconds(min) / bucketSeconds + 2;
            int  nBuckets  = 9;
            var perEvent = new LogVolumeAggregator(long.MinValue, long.MaxValue, minBucket, bucketSeconds, nBuckets, filter, NoPool);
            var fromSum  = new LogVolumeAggregator(long.MinValue, long.MaxValue, minBucket, bucketSeconds, nBuckets, filter, NoPool);

            foreach (var e in events) perEvent.AddByServiceUtf8(e.Ticks, (LogLevel)e.Level, e.Service);
            summary!.AddTo(fromSum, bucketSeconds);

            AssertSameCounts(perEvent.Build(), fromSum.Build());
        }
    }

    [Fact]
    public void Events_out_of_time_order_still_make_one_sorted_cell_per_slot_service_and_level()
    {
        var b = new SegmentCountsBuilder();
        long t0 = new DateTimeOffset(2026, 3, 1, 10, 0, 0, TimeSpan.Zero).UtcTicks;
        long q  = TimeSpan.FromMinutes(15).Ticks;
        b.Add(t0 + 2 * q, 2, "a"u8);
        b.Add(t0,         2, "a"u8);
        b.Add(t0 + 2 * q, 2, "a"u8);
        b.Add(t0 + q,     3, "b"u8);
        b.Add(t0,         2, "a"u8);

        var s = b.Build(5, t0, t0 + 2 * q);
        Assert.NotNull(s);
        var cells = s!.Cells.ToArray();
        Assert.Equal([2u, 1u, 2u], cells.Select(c => c.Count));
        Assert.True(cells[0].Slot < cells[1].Slot && cells[1].Slot < cells[2].Slot);
    }

    [Fact]
    public void Past_its_service_limit_the_builder_gives_up_rather_than_grow()
    {
        var b = new SegmentCountsBuilder();
        long t = new DateTimeOffset(2026, 3, 1, 10, 0, 0, TimeSpan.Zero).UtcTicks;
        for (int i = 0; i <= SegmentCountsBuilder.MaxServices; i++)
            b.Add(t, 2, Encoding.UTF8.GetBytes("svc-" + i));

        Assert.True(b.Abandoned);
        Assert.Null(b.Build((uint)SegmentCountsBuilder.MaxServices + 1, t, t));
    }

    [Fact]
    public void A_count_or_a_time_the_header_does_not_record_is_no_summary()
    {
        long t = new DateTimeOffset(2026, 3, 1, 10, 0, 0, TimeSpan.Zero).UtcTicks;
        var b = new SegmentCountsBuilder();
        b.Add(t, 2, "a"u8);
        b.Add(t + 10, 2, "a"u8);

        Assert.Null(b.Build(3, t, t + 10));        // one event the builder never saw
    }

    [Fact]
    public void An_event_outside_the_header_bounds_is_no_summary()
    {
        long t = new DateTimeOffset(2026, 3, 1, 10, 0, 0, TimeSpan.Zero).UtcTicks;
        var b = new SegmentCountsBuilder();
        b.Add(t, 2, "a"u8);
        b.Add(t + 10, 2, "a"u8);

        Assert.Null(b.Build(2, t + 1, t + 10));    // the header claims a later first event
    }

    // ── The section ─────────────────────────────────────────────────────────────────

    [Fact]
    public void A_section_reads_back_as_the_summary_it_was_written_from()
    {
        var events  = Corpus();
        var summary = BuildFrom(events);
        var info    = Info((uint)events.Count);

        var bytes = Section(summary);
        Assert.Equal(summary.SectionBytes, bytes.Length);

        var back = SegmentCountsSummary.Parse(bytes, info);
        Assert.NotNull(back);
        Assert.Equal(summary.EventCount, back!.EventCount);
        Assert.Equal(summary.Cells.ToArray(), back.Cells.ToArray());
        Assert.Equal(summary.Services.Select(Convert.ToHexString), back.Services.Select(Convert.ToHexString));
    }

    [Fact]
    public void A_section_that_does_not_check_out_is_no_summary()
    {
        var events  = Corpus();
        var summary = BuildFrom(events);
        var info    = Info((uint)events.Count);
        byte[] good = Section(summary);

        // Any flipped byte: the CRC.
        for (int at = 0; at < good.Length; at += 7)
        {
            byte[] bad = (byte[])good.Clone();
            bad[at] ^= 0x40;
            Assert.Null(SegmentCountsSummary.Parse(bad, info));
        }

        // Short.
        Assert.Null(SegmentCountsSummary.Parse(good.AsSpan(0, good.Length - 1), info));

        // A segment that is not the one it was written for.
        Assert.Null(SegmentCountsSummary.Parse(good, Info((uint)events.Count + 1)));

        // Consistent bytes with a correct CRC that still lie: cells out of order.
        var cells = summary.Cells.ToArray();
        Array.Reverse(cells);
        var liar = new SegmentCountsSummary(summary.Services.ToArray(), cells, summary.EventCount);
        Assert.Null(SegmentCountsSummary.Parse(Section(liar), info));
    }

    // ── The writer ──────────────────────────────────────────────────────────────────

    [Fact]
    public void A_written_segment_carries_its_summary_and_a_scan_of_it_agrees()
    {
        using var dir = new TempDir();
        string path = Path.Combine(dir.Path, "a.seg");
        var pool = new StringInternPool();
        SegmentInfo info;
        using (var hot = Tier(pool, Corpus()))
        using (var w = new SegmentWriter(path))
        {
            w.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot));
            info = w.Finalise(new NodeId(1), new SegmentId(9));
        }

        Assert.NotEqual(0, FooterSlot(path, 1));
        var summary = SegmentCountsSummary.Read(path, info);
        Assert.NotNull(summary);
        Assert.Equal(info.EventCount, summary!.EventCount);

        // The summary against a header scan of the very file.
        foreach (int bucket in new[] { 900, 3_600, 86_400 })
        {
            // The corpus spans 1969 to 2026; an axis over its first 2 000 buckets keeps the arrays
            // small and leaves the 2026 events off it, counted in the totals only.
            long minBucket = UnixSeconds(info.MinTimestampTicks) / bucket;
            int  n         = (int)Math.Min(2_000, UnixSeconds(info.MaxTimestampTicks) / bucket - minBucket + 1);
            var scanned = new LogVolumeAggregator(long.MinValue, long.MaxValue, minBucket, bucket, n, null, pool);
            var summed  = new LogVolumeAggregator(long.MinValue, long.MaxValue, minBucket, bucket, n, null, pool);
            using (var r = SegmentReader.Open(path)) r.AggregateHeaders(scanned, long.MinValue, long.MaxValue);
            summary.AddTo(summed, bucket);
            AssertSameCounts(scanned.Build(), summed.Build());
        }
    }

    /// <summary>
    /// The section is named by footer slot 1, which v7 zeroed and which the reader as of this
    /// change — and every v7 reader before it — never looks at. So a file that carries it reads,
    /// event for event, exactly as the same events written without it.
    /// </summary>
    [Fact]
    public async Task A_reader_reads_a_segment_with_a_summary_exactly_as_one_without()
    {
        using var dir = new TempDir();
        var pool = new StringInternPool();
        var events = Corpus();
        string with = Path.Combine(dir.Path, "with.seg"), without = Path.Combine(dir.Path, "without.seg");
        using (var hot = Tier(pool, events))
        {
            using (var w = new SegmentWriter(with)) { w.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot)); w.Finalise(new NodeId(1), new SegmentId(9)); }
            using (var w = new SegmentWriter(without) { WritesCountsSummary = false }) { w.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot)); w.Finalise(new NodeId(1), new SegmentId(9)); }
        }

        Assert.Equal(0, FooterSlot(without, 1));
        Assert.True(new FileInfo(with).Length > new FileInfo(without).Length);

        using var a = SegmentReader.Open(with,    computeUncompressedBytes: true);
        using var b = SegmentReader.Open(without, computeUncompressedBytes: true);
        Assert.Equal(b.Info.EventCount,        a.Info.EventCount);
        Assert.Equal(b.Info.MinTimestampTicks, a.Info.MinTimestampTicks);
        Assert.Equal(b.Info.MaxTimestampTicks, a.Info.MaxTimestampTicks);
        Assert.Equal(b.Info.UncompressedBytes, a.Info.UncompressedBytes);
        Assert.Equal(b.BlockIndexCrc,          a.BlockIndexCrc);

        var ea = new List<string>();
        var eb = new List<string>();
        await foreach (var e in a.ReadEventsAsync(null, null, null)) ea.Add(Describe(e));
        await foreach (var e in b.ReadEventsAsync(null, null, null)) eb.Add(Describe(e));
        Assert.Equal(eb, ea);
        Assert.Equal(events.Count, ea.Count);
    }

    [Fact]
    public void A_segment_past_the_service_limit_is_written_without_a_summary()
    {
        using var dir = new TempDir();
        string path = Path.Combine(dir.Path, "wide.seg");
        var pool = new StringInternPool();
        long t = new DateTimeOffset(2026, 3, 1, 10, 0, 0, TimeSpan.Zero).UtcTicks;
        var events = Enumerable.Range(0, SegmentCountsBuilder.MaxServices + 1)
            .Select(i => new Ev(t + i, 2, Encoding.UTF8.GetBytes("svc-" + i))).ToList();
        SegmentInfo info;
        using (var hot = Tier(pool, events))
        using (var w = new SegmentWriter(path))
        {
            w.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot));
            info = w.Finalise(new NodeId(1), new SegmentId(9));
        }

        Assert.Equal(0, FooterSlot(path, 1));
        Assert.Null(SegmentCountsSummary.Read(path, info));
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────────

    internal readonly record struct Ev(long Ticks, byte Level, byte[] Service);

    /// <summary>
    /// Every awkward case at once, in a fixed order: slot edges to the tick, both sides of 1970,
    /// service spellings that fold together, none at all, a name that is not UTF-8, levels past
    /// Fatal, and a run that arrives out of order.
    /// </summary>
    internal static List<Ev> Corpus()
    {
        byte[][] services =
        [
            "checkout"u8.ToArray(), "Checkout"u8.ToArray(), "billing"u8.ToArray(), [],
            Encoding.UTF8.GetBytes("Zahlung-ü"), [0xFF, 0x41], "(unknown)"u8.ToArray(),
        ];
        byte[] levels = [0, 1, 2, 3, 4, 5, 2, 2, 4, 7];
        var list = new List<Ev>();
        long epoch = DateTimeOffset.UnixEpoch.UtcTicks;
        long q     = TimeSpan.FromMinutes(15).Ticks;
        long[] anchors =
        [
            epoch - 3 * q - 1, epoch - q, epoch - 1, epoch, epoch + 1, epoch + q - 1,
            new DateTimeOffset(2026, 3, 8, 6, 59, 59, TimeSpan.Zero).UtcTicks,
            new DateTimeOffset(2026, 11, 1, 23, 45, 0, TimeSpan.Zero).UtcTicks,
        ];
        int k = 0;
        foreach (long a in anchors)
            for (int i = 0; i < 160; i++)
            {
                long ticks = a + (long)i * (q / 7) + (i % 5 == 0 ? q - 1 : 0);
                list.Add(new Ev(ticks, levels[k % levels.Length], services[k % services.Length]));
                k++;
            }
        // A run out of order inside the last anchor.
        for (int i = 0; i < 40; i++)
            list.Add(new Ev(anchors[^1] - (long)i * (q / 3), levels[i % levels.Length], services[(i * 3) % services.Length]));
        return list;
    }

    private static SegmentCountsSummary BuildFrom(List<Ev> events)
    {
        var b = new SegmentCountsBuilder();
        foreach (var e in events) b.Add(e.Ticks, e.Level, e.Service);
        return b.Build((uint)events.Count, events.Min(e => e.Ticks), events.Max(e => e.Ticks))!;
    }

    private static byte[] Section(SegmentCountsSummary s)
    {
        using var ms = new MemoryStream();
        s.WriteSection(ms);
        return ms.ToArray();
    }

    private static SegmentInfo Info(uint events) => new()
    {
        Id = new SegmentId(1), NodeId = new NodeId(1), FilePath = "x.seg",
        MinTimestampTicks = 0, MaxTimestampTicks = 0, EventCount = events,
        MinLevel = LogLevel.Verbose, CompressedBytes = 0, UncompressedBytes = 0,
    };

    private static long UnixSeconds(long ticks) => (ticks - DateTimeOffset.UnixEpoch.UtcTicks) / TimeSpan.TicksPerSecond;

    /// <summary>A hot tier holding <paramref name="events"/>; the writer's input on the flush road.</summary>
    internal static HotTierSegment Tier(StringInternPool pool, IReadOnlyList<Ev> events)
    {
        var hot = new HotTierSegment(events.Count + 1, (long)events.Count * 512 + 1024 * 1024);
        int tmpl = pool.Intern("evt {n}");
        var buf = new ArrayBufferWriter<byte>(64);
        for (int i = 0; i < events.Count; i++)
        {
            var e = events[i];
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1);
            w.Write("n"); w.Write((long)i);
            w.Flush();
            // The tier interns names; bytes that are not UTF-8 go in as the text they decode to,
            // which is also what the column then holds.
            int svc = e.Service.Length == 0 ? -1 : pool.Intern(Encoding.UTF8.GetString(e.Service));
            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = e.Ticks,
                Level                    = (LogLevel)e.Level,
                MessageTemplatePoolIndex = tmpl,
                ServiceNamePoolIndex     = svc,
            };
            Assert.True(hot.TryWrite(h, buf.WrittenSpan, pool.Get(tmpl), null));
        }
        hot.Freeze();
        return hot;
    }

    internal static long FooterSlot(string path, int slot)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        Span<byte> b = stackalloc byte[8];
        fs.Seek(fs.Length - 44 + slot * 8, SeekOrigin.Begin);
        fs.ReadExactly(b);
        return BinaryPrimitives.ReadInt64LittleEndian(b);
    }

    private static string Describe(LogEvent e) =>
        $"{e.Id}|{e.Timestamp.UtcTicks}|{e.Level}|{e.ServiceName}|{e.MessageTemplate}";

    /// <summary>Two answers are the same answer: totals, and every series point for point (names folded, as the aggregator folds them).</summary>
    internal static void AssertSameCounts(LogVolumeCounts expected, LogVolumeCounts actual)
    {
        Assert.Equal(expected.Total,              actual.Total);
        Assert.Equal(expected.Scanned,            actual.Scanned);
        Assert.Equal(expected.SkippedSegments,    actual.SkippedSegments);
        Assert.Equal(expected.MergedAwaySegments, actual.MergedAwaySegments);
        Assert.Equal(Series(expected.Levels),     Series(actual.Levels));
        Assert.Equal(Series(expected.Services),   Series(actual.Services));
    }

    private static SortedDictionary<string, string> Series(IReadOnlyList<LogSeries> series)
    {
        var d = new SortedDictionary<string, string>(StringComparer.Ordinal);
        foreach (var s in series) d[s.Name.ToUpperInvariant()] = s.Count + ":" + string.Join(',', s.Points);
        return d;
    }

    internal sealed class TempDir : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "ameto-counts-" + Guid.NewGuid().ToString("N"));
        public TempDir() => Directory.CreateDirectory(Path);
        public void Dispose() { try { Directory.Delete(Path, true); } catch { } }
    }
}

/// <summary>
/// The engine end to end: several flushes (so several level-pure segments per flush), a hot tier
/// on top, services that fold together, every level — and the same question asked with the
/// summary road on and off.
/// </summary>
public sealed class SegmentCountsEngineTests : IDisposable
{
    private static readonly DateTimeOffset Base = new(2026, 8, 23, 0, 0, 0, TimeSpan.Zero);
    private const int Events = 1_800;
    private const int StepSeconds = 97;   // ~48.5 h of events, crossing slot, hour, half-day and day edges

    private static readonly string[]   Services = ["checkout", "Checkout", "billing", "", "Zahlung-ü"];
    private static readonly LogLevel[] Levels   =
        [LogLevel.Verbose, LogLevel.Debug, LogLevel.Information, LogLevel.Warning, LogLevel.Error, LogLevel.Fatal];

    private readonly SegmentCountsSummaryTests.TempDir _dir = new();
    private StorageEngine _engine;

    public SegmentCountsEngineTests() => _engine = Populate(carryCounts: true);

    public void Dispose()
    {
        try { _engine.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { }
        _dir.Dispose();
    }

    private StorageEngine Populate(bool carryCounts, string? sub = null)
    {
        string dir = sub is null ? _dir.Path : Path.Combine(_dir.Path, sub);
        Directory.CreateDirectory(dir);
        var opts = new ServerOptions { DataDirectory = dir };
        var engine = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        engine.IndexSinkFactory   = static (c, t) => new SegmentIndexBuilder(c, 5, t);
        engine._segmentsCarryCounts = carryCounts;

        var buf = new ArrayBufferWriter<byte>(64);
        for (int i = 0; i < Events; i++)
        {
            if (i > 0 && i % 450 == 0 && i < 1_700) engine.FlushHotTierAsync().GetAwaiter().GetResult();
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1);
            w.Write("n"); w.Write((long)i);
            w.Flush();
            string svc = Services[i % Services.Length];
            Assert.True(engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = Base.UtcTicks + (long)i * StepSeconds * TimeSpan.TicksPerSecond,
                Level                    = Levels[(i / 5) % Levels.Length],
                MessageTemplatePoolIndex = engine.TemplatePool.Intern("evt {n}"),
                ServiceNamePoolIndex     = svc.Length == 0 ? -1 : engine.TemplatePool.Intern(svc),
            }, buf.WrittenSpan.ToArray()));
        }
        return engine;
    }

    private Task<LogVolumeCounts> CountAsync(DateTimeOffset from, DateTimeOffset to, int bucket, string? service) =>
        CountAsync(_engine, from, to, bucket, service);

    private static async Task<LogVolumeCounts> CountAsync(StorageEngine engine, DateTimeOffset from, DateTimeOffset to, int bucket, string? service)
    {
        long minBucket = from.ToUnixTimeSeconds() / bucket;
        int  n         = (int)Math.Min(2_000, to.ToUnixTimeSeconds() / bucket - minBucket + 1);
        return await engine.AggregateLogVolumeAsync(from, to, minBucket, bucket, n, service);
    }

    private static async Task<LogVolumeCounts> ScanAsync(StorageEngine engine, DateTimeOffset from, DateTimeOffset to, int bucket, string? service)
    {
        engine._countSummaries = false;
        try     { return await CountAsync(engine, from, to, bucket, service); }
        finally { engine._countSummaries = true; }
    }

    public static TheoryData<double, double, int, string?> Questions()
    {
        var d = new TheoryData<double, double, int, string?>();
        (double, double)[] windows = [(-12, 72), (0, 48.5), (5.25, 30), (10, 12.25), (47.9, 72)];
        int[] buckets = [900, 3_600, 43_200, 86_400, 60, 1_000];
        string?[] filters = [null, "checkout", "CHECKOUT", "billing", "Zahlung-ü", "absent"];
        foreach (var (f, t) in windows)
            foreach (int b in buckets)
                foreach (var s in filters)
                    d.Add(f, t, b, s);
        return d;
    }

    [Theory]
    [MemberData(nameof(Questions))]
    public async Task The_summaries_answer_every_window_grid_and_filter_exactly_as_the_scan(
        double fromHours, double toHours, int bucket, string? service)
    {
        var from = Base.AddHours(fromHours);
        var to   = Base.AddHours(toHours);

        var scanned = await ScanAsync(_engine, from, to, bucket, service);
        var first   = await CountAsync(from, to, bucket, service);   // summaries read off the files
        var again   = await CountAsync(from, to, bucket, service);   // and from memory

        SegmentCountsSummaryTests.AssertSameCounts(scanned, first);
        SegmentCountsSummaryTests.AssertSameCounts(scanned, again);
    }

    /// <summary>
    /// THE POINT OF THE CHANGE. A window that covers every cold segment whole — the Statistics
    /// band's 1m and 3m on a store of any size — opened every one of them for a header scan and
    /// LZ4-decoded all of its blocks; now it opens none of them.
    /// </summary>
    [Fact]
    public async Task A_window_that_covers_segments_whole_decodes_none_of_them()
    {
        Assert.True(_engine.ListSegments().Count >= 10, "setup: several flushes, several levels each");
        int opens = 0;
        _engine._beforeHeaderSegmentOpen = _ => Interlocked.Increment(ref opens);
        try
        {
            var counts = await CountAsync(Base.AddHours(-12), Base.AddDays(3), 43_200, null);
            Assert.Equal(Events, counts.Total);
            Assert.Equal(0, opens);
        }
        finally { _engine._beforeHeaderSegmentOpen = null; }
    }

    [Fact]
    public async Task A_boundary_segment_is_still_decoded_and_only_it()
    {
        int opens = 0;
        _engine._beforeHeaderSegmentOpen = _ => Interlocked.Increment(ref opens);
        try
        {
            var from = Base.AddHours(6);
            var to   = Base.AddDays(3);
            int straddling = _engine.ListSegments().Count(s =>
                s.MinTimestampTicks < from.UtcTicks && s.MaxTimestampTicks >= from.UtcTicks);
            Assert.True(straddling > 0, "setup: the window's start cuts a segment");

            var counts = await CountAsync(from, to, 3_600, null);
            Assert.Equal(straddling, opens);
            SegmentCountsSummaryTests.AssertSameCounts(await ScanAsync(_engine, from, to, 3_600, null), counts);
        }
        finally { _engine._beforeHeaderSegmentOpen = null; }
    }

    [Fact]
    public async Task A_grid_that_is_not_whole_slots_is_still_scanned()
    {
        int opens = 0;
        _engine._beforeHeaderSegmentOpen = _ => Interlocked.Increment(ref opens);
        try
        {
            await CountAsync(Base.AddHours(-12), Base.AddDays(3), 1_000, null);
            Assert.Equal(_engine.ListSegments().Count, opens);
        }
        finally { _engine._beforeHeaderSegmentOpen = null; }
    }

    /// <summary>
    /// A store written before summaries existed — every segment on a stand the day this ships.
    /// The first wide window decodes them (it had to anyway) and keeps their summaries; the
    /// next one decodes nothing.
    /// </summary>
    [Fact]
    public async Task Segments_written_before_summaries_are_decoded_once_then_answered_from_memory()
    {
        await using var legacy = Populate(carryCounts: false, sub: "legacy");
        var segments = legacy.ListSegments();
        Assert.All(segments, s => Assert.Equal(0, SegmentCountsSummaryTests.FooterSlot(s.FilePath, 1)));

        var from = Base.AddHours(-12);
        var to   = Base.AddDays(3);
        var scanned = await ScanAsync(legacy, from, to, 43_200, null);

        int opens = 0;
        legacy._beforeHeaderSegmentOpen = _ => Interlocked.Increment(ref opens);
        var first = await CountAsync(legacy, from, to, 43_200, null);
        Assert.Equal(segments.Count, opens);

        opens = 0;
        var second = await CountAsync(legacy, from, to, 86_400, "checkout");
        Assert.Equal(0, opens);

        SegmentCountsSummaryTests.AssertSameCounts(scanned, first);
        SegmentCountsSummaryTests.AssertSameCounts(await ScanAsync(legacy, from, to, 86_400, "checkout"), second);
        Assert.Equal(segments.Count, legacy.CountsCacheForTest.CountForTest);
    }

    [Fact]
    public async Task A_damaged_counts_section_is_ignored_and_its_segment_decoded()
    {
        var victim = _engine.ListSegments()[0];
        long section = SegmentCountsSummaryTests.FooterSlot(victim.FilePath, 1);
        Assert.NotEqual(0, section);
        using (var fs = new FileStream(victim.FilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
        {
            fs.Seek(section + SegmentCountsSummary.SectionHeaderBytes + 1, SeekOrigin.Begin);
            int b = fs.ReadByte();
            fs.Seek(-1, SeekOrigin.Current);
            fs.WriteByte((byte)(b ^ 0x5A));
        }

        var from = Base.AddHours(-12);
        var to   = Base.AddDays(3);
        var opened = new List<string>();
        _engine._beforeHeaderSegmentOpen = s => { lock (opened) opened.Add(s.FilePath); };
        try
        {
            var counts = await CountAsync(from, to, 3_600, null);
            Assert.Equal([victim.FilePath], opened);
            SegmentCountsSummaryTests.AssertSameCounts(await ScanAsync(_engine, from, to, 3_600, null), counts);
        }
        finally { _engine._beforeHeaderSegmentOpen = null; }
    }

    /// <summary>A merge's output is written by the same writer, so it carries a summary of its own.</summary>
    [Fact]
    public async Task A_merged_segment_carries_a_summary_of_its_sources()
    {
        string dir = Path.Combine(_dir.Path, "merge");
        Directory.CreateDirectory(dir);
        var opts = new ServerOptions { DataDirectory = dir };
        await using var engine = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        engine.IndexSinkFactory = static (c, t) => new SegmentIndexBuilder(c, 5, t);

        // Two Error segments in a long-sealed bucket: the planner merges such a pair.
        var buf = new ArrayBufferWriter<byte>(64);
        for (int i = 0; i < 120; i++)
        {
            if (i == 60) await engine.FlushHotTierAsync();
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1); w.Write("n"); w.Write((long)i); w.Flush();
            Assert.True(engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = Base.UtcTicks + i * 61 * TimeSpan.TicksPerSecond,
                Level                    = LogLevel.Error,
                MessageTemplatePoolIndex = engine.TemplatePool.Intern("evt {n}"),
                ServiceNamePoolIndex     = engine.TemplatePool.Intern(i % 2 == 0 ? "billing" : "checkout"),
            }, buf.WrittenSpan.ToArray()));
        }
        await engine.FlushHotTierAsync();
        Assert.Equal(2, engine.ListSegments().Count);
        Assert.True(await engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None), "setup: the pair did not merge");

        var output = Assert.Single(engine.ListSegments());
        var summary = SegmentCountsSummary.Read(output.FilePath, output);
        Assert.NotNull(summary);
        Assert.Equal(120u, summary!.EventCount);

        var from = Base.AddHours(-1);
        var to   = Base.AddHours(4);
        SegmentCountsSummaryTests.AssertSameCounts(
            await ScanAsync(engine, from, to, 900, null), await CountAsync(engine, from, to, 900, null));
    }
}
