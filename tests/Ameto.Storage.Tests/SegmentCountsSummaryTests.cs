using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using MessagePack;
using Microsoft.Data.Sqlite;
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
        var info    = Info(events);

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
        var info    = Info(events);
        byte[] good = Section(summary);
        Assert.NotNull(SegmentCountsSummary.Parse(good, info));

        // Every flipped byte: the CRC.
        for (int at = 0; at < good.Length; at++)
        {
            byte[] bad = (byte[])good.Clone();
            bad[at] ^= 0x40;
            Assert.Null(SegmentCountsSummary.Parse(bad, info));
        }

        // Short.
        Assert.Null(SegmentCountsSummary.Parse(good.AsSpan(0, good.Length - 1), info));

        // A segment that is not the one it was written for.
        Assert.Null(SegmentCountsSummary.Parse(good, Info(events, eventsDelta: 1)));
    }

    /// <summary>
    /// Bytes a CRC vouches for can still be bytes no writer of this format produced. Each lie here
    /// is internally consistent and would put events where the segment has none.
    /// </summary>
    [Fact]
    public void A_well_formed_section_that_lies_is_no_summary()
    {
        var events  = Corpus();
        var summary = BuildFrom(events);
        var info    = Info(events);

        // Cells out of order.
        var cells = summary.Cells.ToArray();
        Array.Reverse(cells);
        Assert.Null(SegmentCountsSummary.Parse(Section(new SegmentCountsSummary(summary.Services.ToArray(), cells, summary.EventCount)), info));

        // A slot past the segment's last event: the header says the segment ends one slot sooner.
        long q = TimeSpan.FromMinutes(15).Ticks;
        Assert.Null(SegmentCountsSummary.Parse(Section(summary), Info(events, maxDelta: -q)));

        // A slot before its first event.
        Assert.Null(SegmentCountsSummary.Parse(Section(summary), Info(events, minDelta: q)));
    }

    /// <summary>
    /// A 90-day level merges into week-scale segments (an expiry bucket is a twelfth of the
    /// retention): 672 slots. Sixty services active in every one of them is 40 320 cells, which
    /// the first cap (32 768, sized for day-scale segments) gave up on — and that segment was then
    /// decoded on every wide request for as long as retention kept it.
    /// </summary>
    [Fact]
    public void A_week_scale_segment_with_sixty_services_has_a_summary()
    {
        const int slots = 672, services = 60;
        long t0 = new DateTimeOffset(2026, 3, 2, 0, 0, 0, TimeSpan.Zero).UtcTicks;
        long q  = TimeSpan.FromMinutes(15).Ticks;
        var names = Enumerable.Range(0, services).Select(i => Encoding.UTF8.GetBytes("service-" + i)).ToArray();
        var b = new SegmentCountsBuilder();
        for (int s = 0; s < slots; s++)
            for (int v = 0; v < services; v++)
                b.Add(t0 + s * q, 2, names[v]);

        var summary = b.Build(slots * services, t0, t0 + (slots - 1) * q);
        Assert.NotNull(summary);
        Assert.Equal(slots * services, summary!.CellCount);
    }

    [Fact]
    public void Past_its_cell_limit_the_builder_gives_up_rather_than_grow()
    {
        long t0 = new DateTimeOffset(2026, 3, 2, 0, 0, 0, TimeSpan.Zero).UtcTicks;
        long q  = TimeSpan.FromMinutes(15).Ticks;
        const int services = 64;
        int slots = SegmentCountsBuilder.MaxCells / services + 1;
        var names = Enumerable.Range(0, services).Select(i => Encoding.UTF8.GetBytes("s" + i)).ToArray();
        var b = new SegmentCountsBuilder();
        for (int s = 0; s < slots; s++)
            for (int v = 0; v < services; v++)
                b.Add(t0 + s * q, 2, names[v]);

        Assert.True(b.Abandoned);
        Assert.Null(b.Build((uint)(slots * services), t0, t0 + (slots - 1) * q));
    }

    [Fact]
    public void Past_its_service_bytes_limit_the_builder_gives_up()
    {
        long t = new DateTimeOffset(2026, 3, 1, 10, 0, 0, TimeSpan.Zero).UtcTicks;
        int n = SegmentCountsBuilder.MaxServiceBytesTotal / SegmentCountsBuilder.MaxServiceBytes + 1;
        var b = new SegmentCountsBuilder();
        for (int i = 0; i < n; i++)
        {
            byte[] name = new byte[SegmentCountsBuilder.MaxServiceBytes];
            Array.Fill(name, (byte)'a');
            Encoding.ASCII.GetBytes(i.ToString("D6")).CopyTo(name, 0);
            b.Add(t, 2, name);
        }

        Assert.True(b.Abandoned);
    }

    // ── Coarser resolutions and the store's form ───────────────────────────────────

    /// <summary>
    /// An hour's or a day's summary is the 15-minute one divided down: the same truncating nesting,
    /// so it answers every grid of whole hours or days exactly as the events do — before 1970 too.
    /// </summary>
    [Theory]
    [InlineData(3_600, 3_600)]
    [InlineData(3_600, 43_200)]
    [InlineData(86_400, 86_400)]
    [InlineData(86_400, 604_800)]
    public void A_coarse_summary_adds_up_exactly_as_its_events_do(int resolution, int bucketSeconds)
    {
        var events  = Corpus();
        var coarse  = BuildFrom(events).Coarsen(resolution);
        Assert.Equal(resolution, coarse.ResolutionSeconds);
        long min = events.Min(e => e.Ticks);

        foreach (string? filter in new[] { null, "checkout", "(unknown)" })
        {
            long minBucket = UnixSeconds(min) / bucketSeconds + 1;
            var perEvent = new LogVolumeAggregator(long.MinValue, long.MaxValue, minBucket, bucketSeconds, 9, filter, NoPool);
            var fromSum  = new LogVolumeAggregator(long.MinValue, long.MaxValue, minBucket, bucketSeconds, 9, filter, NoPool);
            foreach (var e in events) perEvent.AddByServiceUtf8(e.Ticks, (LogLevel)e.Level, e.Service);
            coarse.AddTo(fromSum, bucketSeconds);
            AssertSameCounts(perEvent.Build(), fromSum.Build());
        }
    }

    [Theory]
    [InlineData(900)]
    [InlineData(3_600)]
    [InlineData(86_400)]
    public void A_store_section_reads_back_and_adds_from_its_bytes_as_the_summary_does(int resolution)
    {
        var events  = Corpus();
        var summary = BuildFrom(events).Coarsen(resolution);
        var info    = Info(events);
        byte[] bytes = summary.ToStoreBytes();
        Assert.Equal(SegmentCountsSummary.StoreVersion, BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(4)));

        var back = SegmentCountsSummary.Parse(bytes, info);
        Assert.NotNull(back);
        Assert.Equal(resolution, back!.ResolutionSeconds);
        Assert.Equal(summary.Cells.ToArray(), back.Cells.ToArray());

        int bucket = resolution * 4;
        long minBucket = UnixSeconds(events.Min(e => e.Ticks)) / bucket + 1;
        var a = new LogVolumeAggregator(long.MinValue, long.MaxValue, minBucket, bucket, 9, null, NoPool);
        var b = new LogVolumeAggregator(long.MinValue, long.MaxValue, minBucket, bucket, 9, null, NoPool);
        summary.AddTo(a, bucket);
        Assert.True(SegmentCountsSummary.TryAddFrom(bytes, info, b, bucket));
        AssertSameCounts(a.Build(), b.Build());

        // A grid that is not a whole number of its slots adds nothing.
        if (resolution > 900)
        {
            var c = new LogVolumeAggregator(long.MinValue, long.MaxValue, minBucket, 900, 9, null, NoPool);
            Assert.False(SegmentCountsSummary.TryAddFrom(bytes, info, c, 900));
            Assert.Equal(0, c.Build().Scanned);
        }
    }

    [Fact]
    public void A_file_section_stays_v1_so_the_build_before_the_store_still_reads_it()
    {
        var summary = BuildFrom(Corpus());
        byte[] file = Section(summary);
        Assert.Equal(SegmentCountsSummary.FileVersion, BinaryPrimitives.ReadUInt16LittleEndian(file.AsSpan(4)));
        Assert.Equal(900, BinaryPrimitives.ReadUInt16LittleEndian(file.AsSpan(6)));
        Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(file.AsSpan(24)));
        Assert.Throws<InvalidOperationException>(() => Section(summary.Coarsen(3_600)));
    }

    /// <summary>A section that checks out byte for byte but names a resolution nothing writes is no summary.</summary>
    [Fact]
    public void A_store_section_of_an_unknown_resolution_is_no_summary()
    {
        var events = Corpus();
        byte[] bytes = BuildFrom(events).Coarsen(3_600).ToStoreBytes();
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(24), 1_800);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(bytes.Length - 4), Crc32c.Append(0, bytes.AsSpan(0, bytes.Length - 4)));

        Assert.Null(SegmentCountsSummary.Parse(bytes, Info(events)));
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

    internal static SegmentCountsSummary BuildFrom(List<Ev> events)
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

    /// <summary>The catalog entry of a segment holding <paramref name="events"/>, its bounds moved by the deltas to make it lie.</summary>
    private static SegmentInfo Info(List<Ev> events, int eventsDelta = 0, long minDelta = 0, long maxDelta = 0) => new()
    {
        Id = new SegmentId(1), NodeId = new NodeId(1), FilePath = "x.seg",
        MinTimestampTicks = events.Min(e => e.Ticks) + minDelta,
        MaxTimestampTicks = events.Max(e => e.Ticks) + maxDelta,
        EventCount = (uint)(events.Count + eventsDelta),
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
    /// A grid no summary can answer — the aggregate road's one-second axis, a service-filtered
    /// alert's own window, a short custom window's 60 s step — never reads one, so it must not pay
    /// for building one either: on top of its scan that was a hash and a probe per event and a
    /// builder per segment, thrown away (review of #157, S1). Nor does it write to the store.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(60)]
    [InlineData(1_000)]
    public async Task A_grid_that_is_not_whole_slots_builds_no_summary(int bucket)
    {
        await CountAsync(Base.AddHours(-12), Base.AddDays(3), bucket, null);
        await CountAsync(Base.AddHours(-12), Base.AddDays(3), bucket, "billing");

        Assert.Equal(0, _engine.SummaryBuildsForTest);
        Assert.Empty(_engine.CountsStoreForTest!.ReadFingerprints());
    }

    /// <summary>
    /// THE POINT OF counts.db. A store written before summaries existed — every segment on a stand
    /// the day this ships. The first wide window decodes them (it had to anyway) and writes their
    /// summaries to disk; the next window decodes nothing, and neither does the first window of the
    /// NEXT process. Kept in memory (#157), the summaries died with every restart, and every deploy
    /// began with a month decoded again.
    /// </summary>
    [Fact]
    public async Task Segments_written_before_summaries_are_decoded_once_ever_not_once_per_restart()
    {
        var from = Base.AddHours(-12);
        var to   = Base.AddDays(3);
        LogVolumeCounts scanned, first, second;
        int segmentCount;

        await using (var legacy = Populate(carryCounts: false, sub: "legacy"))
        {
            var segments = legacy.ListSegments();
            segmentCount = segments.Count;
            Assert.All(segments, s => Assert.Equal(0, SegmentCountsSummaryTests.FooterSlot(s.FilePath, 1)));
            await legacy.FlushHotTierAsync();   // everything cold, so the restart below sees it all
            segmentCount = legacy.ListSegments().Count;
            scanned = await ScanAsync(legacy, from, to, 43_200, null);

            int opens = 0;
            legacy._beforeHeaderSegmentOpen = _ => Interlocked.Increment(ref opens);
            first = await CountAsync(legacy, from, to, 43_200, null);
            Assert.Equal(segmentCount, opens);
            Assert.Equal(segmentCount, legacy.CountsStoreForTest!.ReadFingerprints().Count);
        }

        await using var restarted = Reopen("legacy");
        int reopened = 0;
        restarted._beforeHeaderSegmentOpen = _ => Interlocked.Increment(ref reopened);
        second = await CountAsync(restarted, from, to, 43_200, null);

        Assert.Equal(0, reopened);
        Assert.Equal(segmentCount, restarted.ListSegments().Count);
        SegmentCountsSummaryTests.AssertSameCounts(scanned, first);
        SegmentCountsSummaryTests.AssertSameCounts(scanned, second);
    }

    /// <summary>
    /// The background pass does it before anyone asks: after one pass over a pre-summary store,
    /// no window decodes a whole segment — in this process or the next — and a second pass finds
    /// nothing to do.
    /// </summary>
    [Fact]
    public async Task One_backfill_pass_summarises_every_older_segment_and_the_next_pass_has_nothing_to_do()
    {
        var from = Base.AddHours(-12);
        var to   = Base.AddDays(3);
        await using (var legacy = Populate(carryCounts: false, sub: "backfill"))
        {
            await legacy.FlushHotTierAsync();
            int n = legacy.ListSegments().Count;

            var pass = await legacy.BackfillCountsOnceAsync(CancellationToken.None, dutyCycle: 1);
            Assert.Equal(n, pass.Segments);
            Assert.Equal(n, pass.Scanned);
            Assert.Equal(0, pass.FromFileSections + pass.Failed + pass.AlreadyStored);
            Assert.Equal(new CountsSummaryProgress(n, n, legacy.CountsSummaries.AsOf), legacy.CountsSummaries);

            var again = await legacy.BackfillCountsOnceAsync(CancellationToken.None, dutyCycle: 1);
            Assert.Equal(n, again.AlreadyStored);
            Assert.Equal(0, again.Written);
        }

        await using var restarted = Reopen("backfill");
        int[] buckets = [900, 3_600, 43_200, 86_400];
        var answers = new LogVolumeCounts[buckets.Length];
        int opens = 0;
        restarted._beforeHeaderSegmentOpen = _ => Interlocked.Increment(ref opens);
        for (int i = 0; i < buckets.Length; i++) answers[i] = await CountAsync(restarted, from, to, buckets[i], null);
        restarted._beforeHeaderSegmentOpen = null;

        Assert.Equal(0, opens);
        for (int i = 0; i < buckets.Length; i++)
            SegmentCountsSummaryTests.AssertSameCounts(await ScanAsync(restarted, from, to, buckets[i], null), answers[i]);
    }

    /// <summary>
    /// A segment no summary can be built for — here more service-name bytes than a summary keeps —
    /// is remembered as such: the backfill stores "none", and a counting request then scans it
    /// plainly instead of building, and dropping, a summary on every request.
    /// </summary>
    [Fact]
    public async Task A_segment_with_no_possible_summary_is_scanned_without_building_one_again()
    {
        string dir = Path.Combine(_dir.Path, "wide-names");
        Directory.CreateDirectory(dir);
        var opts = new ServerOptions { DataDirectory = dir };
        await using var engine = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        engine.IndexSinkFactory = static (c, t) => new SegmentIndexBuilder(c, 5, t);
        int names = SegmentCountsBuilder.MaxServiceBytesTotal / 1_000 + 2;
        var buf = new ArrayBufferWriter<byte>(64);
        for (int i = 0; i < names; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1); w.Write("n"); w.Write((long)i); w.Flush();
            Assert.True(engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = Base.UtcTicks + i * TimeSpan.TicksPerSecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = engine.TemplatePool.Intern("evt {n}"),
                ServiceNamePoolIndex     = engine.TemplatePool.Intern(i.ToString("D4") + new string('s', 996)),
            }, buf.WrittenSpan.ToArray()));
        }
        await engine.FlushHotTierAsync();
        var segment = Assert.Single(engine.ListSegments());
        Assert.Equal(0, SegmentCountsSummaryTests.FooterSlot(segment.FilePath, 1));   // the writer gave up too

        var pass = await engine.BackfillCountsOnceAsync(CancellationToken.None, dutyCycle: 1);
        Assert.Equal(1, pass.Unsummarisable);

        var from = Base.AddHours(-1);
        var to   = Base.AddHours(1);
        var counts = await CountAsync(engine, from, to, 900, null);
        await CountAsync(engine, from, to, 3_600, null);

        Assert.Equal(0, engine.SummaryBuildsForTest);
        Assert.Equal(names, counts.Total);
    }

    /// <summary>New segments carry their section, so the backfill copies it into the store without a scan.</summary>
    [Fact]
    public async Task The_backfill_copies_new_segments_sections_without_scanning_them()
    {
        await _engine.FlushHotTierAsync();
        int n = _engine.ListSegments().Count;

        var pass = await _engine.BackfillCountsOnceAsync(CancellationToken.None, dutyCycle: 1);

        Assert.Equal(n, pass.FromFileSections);
        Assert.Equal(0, pass.Scanned);
        Assert.Equal(n, _engine.CountsStoreForTest!.ReadFingerprints().Count);
    }

    /// <summary>
    /// A pass stopped half way — a shutdown, a deploy — keeps every row it wrote, and the next pass
    /// starts from there. A pass stops between segments: the one under way when the stop came is
    /// finished and kept.
    /// </summary>
    [Fact]
    public async Task A_stopped_pass_keeps_what_it_wrote_and_the_next_one_resumes()
    {
        await using var legacy = Populate(carryCounts: false, sub: "resume");
        await legacy.FlushHotTierAsync();
        int n = legacy.ListSegments().Count;
        using var cts = new CancellationTokenSource();
        int seen = 0;
        legacy._beforeBackfillSegment = _ => { if (++seen == 4) cts.Cancel(); };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => legacy.BackfillCountsOnceAsync(cts.Token, dutyCycle: 1));
        Assert.Equal(4, legacy.CountsStoreForTest!.ReadFingerprints().Count);

        legacy._beforeBackfillSegment = null;
        var rest = await legacy.BackfillCountsOnceAsync(CancellationToken.None, dutyCycle: 1);
        Assert.Equal(4, rest.AlreadyStored);
        Assert.Equal(n - 4, rest.Scanned);
    }

    /// <summary>
    /// Rows outlive nothing: a segment that leaves the catalog — deleted by retention, merged away —
    /// stops matching, is never used, and the next pass clears its row.
    /// </summary>
    [Fact]
    public async Task A_row_whose_segment_left_the_catalog_is_cleared_by_the_next_pass()
    {
        await _engine.FlushHotTierAsync();
        await _engine.BackfillCountsOnceAsync(CancellationToken.None, dutyCycle: 1);
        var victim = _engine.ListSegments()[0];

        await _engine.DeleteSegmentAsync(SegmentKey.Of(victim));
        var pass = await _engine.BackfillCountsOnceAsync(CancellationToken.None, dutyCycle: 1);

        Assert.Equal(1, pass.Swept);
        Assert.False(_engine.CountsStoreForTest!.ReadFingerprints().ContainsKey(SegmentKey.Of(victim)));
    }

    /// <summary>
    /// The store's rows, at every resolution a grid can take, against the scan — with the store
    /// filled by a backfill pass, so every wholly-inside segment is answered from counts.db.
    /// </summary>
    [Theory]
    [InlineData(900)]
    [InlineData(3_600)]
    [InlineData(7_200)]
    [InlineData(43_200)]
    [InlineData(86_400)]
    public async Task The_store_answers_every_resolution_exactly_as_the_scan(int bucket)
    {
        await _engine.FlushHotTierAsync();
        await _engine.BackfillCountsOnceAsync(CancellationToken.None, dutyCycle: 1);
        int opens = 0;
        _engine._beforeHeaderSegmentOpen = _ => Interlocked.Increment(ref opens);
        try
        {
            foreach (var (f, t) in new[] { (-12.0, 72.0), (5.25, 30.0), (24.0, 48.5) })
                foreach (string? service in new[] { null, "checkout", "Zahlung-ü", "absent" })
                {
                    var from = Base.AddHours(f);
                    var to   = Base.AddHours(t);
                    var counts = await CountAsync(from, to, bucket, service);
                    SegmentCountsSummaryTests.AssertSameCounts(await ScanAsync(_engine, from, to, bucket, service), counts);
                }
        }
        finally { _engine._beforeHeaderSegmentOpen = null; }
        Assert.True(opens < _engine.ListSegments().Count * 12, "only boundary segments were decoded");
    }

    private StorageEngine Reopen(string sub)
    {
        var opts = new ServerOptions { DataDirectory = Path.Combine(_dir.Path, sub) };
        var engine = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        engine.IndexSinkFactory = static (c, t) => new SegmentIndexBuilder(c, 5, t);
        engine.CatalogLoaded.GetAwaiter().GetResult();
        return engine;
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

/// <summary>
/// counts.db on its own: a row answers as its summary does at every resolution, only for the file
/// it was written for; a segment with no possible summary is remembered as such; rows of segments
/// that have gone are cleared; the store outlives its process; and a file that is not a store is
/// set aside rather than stopping the server.
/// </summary>
public sealed class SegmentCountsStoreTests : IDisposable
{
    private static readonly StringInternPool NoPool = new();
    private readonly SegmentCountsSummaryTests.TempDir _dir = new();

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        _dir.Dispose();
    }

    private SegmentCountsStore OpenStore() => SegmentCountsStore.Open(_dir.Path, NullLogger.Instance)!;

    private static SegmentInfo Info(List<SegmentCountsSummaryTests.Ev> events, ulong id = 7, long bytes = 12_345) => new()
    {
        Id = new SegmentId(id), NodeId = new NodeId(1), FilePath = $"{id}.seg",
        MinTimestampTicks = events.Min(e => e.Ticks), MaxTimestampTicks = events.Max(e => e.Ticks),
        EventCount = (uint)events.Count, MinLevel = LogLevel.Verbose,
        CompressedBytes = bytes, UncompressedBytes = 0,
    };

    [Theory]
    [InlineData(900, 900)]
    [InlineData(900, 7_200)]
    [InlineData(3_600, 3_600)]
    [InlineData(3_600, 43_200)]
    [InlineData(86_400, 86_400)]
    public void A_row_adds_exactly_what_its_events_add(int resolution, int bucketSeconds)
    {
        var events = SegmentCountsSummaryTests.Corpus();
        var info   = Info(events);
        using (var store = OpenStore())
            Assert.True(store.TryUpsert(info, SegmentCountsSummaryTests.BuildFrom(events)));

        using var reopened = OpenStore();   // and it is still there in the next process
        long minBucket = (events.Min(e => e.Ticks) - DateTimeOffset.UnixEpoch.UtcTicks) / TimeSpan.TicksPerSecond / bucketSeconds + 1;
        foreach (string? filter in new[] { null, "checkout", "absent" })
        {
            var perEvent = new LogVolumeAggregator(long.MinValue, long.MaxValue, minBucket, bucketSeconds, 9, filter, NoPool);
            var fromRow  = new LogVolumeAggregator(long.MinValue, long.MaxValue, minBucket, bucketSeconds, 9, filter, NoPool);
            foreach (var e in events) perEvent.AddByServiceUtf8(e.Ticks, (LogLevel)e.Level, e.Service);

            Assert.Equal(SegmentCountsStore.Lookup.Added, reopened.TryAddTo(info, resolution, fromRow, bucketSeconds));
            SegmentCountsSummaryTests.AssertSameCounts(perEvent.Build(), fromRow.Build());
        }
    }

    [Fact]
    public void A_row_answers_only_for_the_file_it_was_written_for()
    {
        var events = SegmentCountsSummaryTests.Corpus();
        using var store = OpenStore();
        store.TryUpsert(Info(events, bytes: 12_345), SegmentCountsSummaryTests.BuildFrom(events));

        var agg = new LogVolumeAggregator(long.MinValue, long.MaxValue, 0, 900, 1, null, NoPool);
        Assert.Equal(SegmentCountsStore.Lookup.Missing, store.TryAddTo(Info(events, bytes: 99_999), 900, agg, 900));
        Assert.Equal(SegmentCountsStore.Lookup.Missing, store.TryAddTo(Info(events, id: 8), 900, agg, 900));
        Assert.Equal(0, agg.Build().Scanned);
    }

    [Fact]
    public void A_segment_with_no_possible_summary_is_remembered_as_such()
    {
        var events = SegmentCountsSummaryTests.Corpus();
        using var store = OpenStore();
        Assert.True(store.TryUpsert(Info(events), summary: null));

        var agg = new LogVolumeAggregator(long.MinValue, long.MaxValue, 0, 3_600, 1, null, NoPool);
        Assert.Equal(SegmentCountsStore.Lookup.Unsummarisable, store.TryAddTo(Info(events), 3_600, agg, 3_600));
        Assert.True(store.ReadFingerprints()[SegmentKey.Of(Info(events))].Matches(Info(events)));
    }

    [Fact]
    public void Rows_of_segments_the_catalog_no_longer_serves_as_that_file_are_cleared()
    {
        var events = SegmentCountsSummaryTests.Corpus();
        var summary = SegmentCountsSummaryTests.BuildFrom(events);
        var gone = Info(events, id: 1); var kept = Info(events, id: 2); var replaced = Info(events, id: 3);
        using var store = OpenStore();
        foreach (var info in new[] { gone, kept, replaced }) store.TryUpsert(info, summary);

        int cleared = store.DeleteStale(key =>
            key.Id.Value == 2 ? kept
          : key.Id.Value == 3 ? Info(events, id: 3, bytes: 1)   // same key, another file
          : null);

        Assert.Equal(2, cleared);
        Assert.Equal([SegmentKey.Of(kept)], store.ReadFingerprints().Keys);
    }

    [Fact]
    public void A_file_that_is_not_a_store_is_set_aside_and_a_new_one_started()
    {
        string path = Path.Combine(_dir.Path, SegmentCountsStore.FileName);
        File.WriteAllText(path, "this is not a database, and never was one");

        using var store = SegmentCountsStore.Open(_dir.Path, NullLogger.Instance);

        Assert.NotNull(store);
        Assert.True(File.Exists(path + ".corrupt"));
        Assert.Empty(store!.ReadFingerprints());
        var events = SegmentCountsSummaryTests.Corpus();
        Assert.True(store.TryUpsert(Info(events), SegmentCountsSummaryTests.BuildFrom(events)));
    }

    [Fact]
    public void A_disposed_store_has_no_rows_and_takes_none()
    {
        var events = SegmentCountsSummaryTests.Corpus();
        var store = OpenStore();
        store.TryUpsert(Info(events), SegmentCountsSummaryTests.BuildFrom(events));
        store.Dispose();

        var agg = new LogVolumeAggregator(long.MinValue, long.MaxValue, 0, 900, 1, null, NoPool);
        Assert.Equal(SegmentCountsStore.Lookup.Missing, store.TryAddTo(Info(events), 900, agg, 900));
        Assert.False(store.TryUpsert(Info(events), SegmentCountsSummaryTests.BuildFrom(events)));
        Assert.Empty(store.ReadFingerprints());
    }
}
