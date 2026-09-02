using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// ONE TORN 8-BYTE HEADER FIELD, AND WHERE THE ANSWER TO IT BELONGS.
///
/// <para><c>minNano</c>/<c>maxNano</c> are copied out of a <c>.trc</c> header by
/// <c>ReadSegmentInfo</c>, and four things treat them as fact: the window skip, the snapshot's sort
/// order, retention (<c>MaxStartNano &lt; cutoff</c>), and — once <c>VanishedRegionLog</c> existed —
/// the process-wide memory of what can no longer be served. A <c>maxNano</c> torn to
/// <c>long.MaxValue</c> therefore wrote a truncation claim over the rest of time that
/// <c>PruneAsync</c> was STRUCTURALLY UNABLE to withdraw: measured on a 20-trace segment with the 8
/// bytes at offset 18 overwritten and the file then deleted, a window 400 days later holding ten
/// healthy fresh traces came back <c>Unreadable=True</c> and stayed so after retention
/// (<c>pruned=0 regions=1</c>) — every query on the install, for the life of the process.</para>
///
/// <para>THE POISON WAS THE RECORDED REGION, NOT THE SEGMENT SCAN, and an earlier round answered it
/// in the wrong place: a REPAIR at load time that clamped <c>max</c> down to
/// <c>min(mtime, now) + 24 h</c>. Two ordinary, undamaged installs then answered ordinary queries
/// with a CLEAN, COMPLETE, EMPTY page over data that was sitting on the disk:</para>
/// <list type="bullet">
///   <item>files whose mtime is older than their spans — <c>rsync -at</c>, <c>tar -xp</c>,
///   <c>cp -p</c>, a snapshot restore, a host whose clock was behind while writing. Measured: 20
///   spans written an hour ago, every mtime set 5 days back, and the last 15 min / 1 h / 24 h /
///   3 days all returned <c>rows=0 Unreadable=False Capped=False</c>. Only a 7-day window found
///   them;</item>
///   <item>a producer whose clock is AHEAD. 20 spans at <c>now+25h</c> answered <c>rows=20</c>
///   until the process restarted and <c>rows=0</c> after it, because the reload clamped
///   <c>max</c> to <c>now+24h</c> and both cold walks skip on
///   <c>seg.MaxStartNano &lt; fromNano</c>. The boundary was exactly the slack: +23 h → 20 rows,
///   +25 h → none.</item>
/// </list>
///
/// <para>A positive claim of completeness over data on disk is the ONE failure this endpoint exists
/// to prevent, so the header is read RAW again, as <c>origin/main</c> read it. The header range
/// decides which segments a walk opens, and a value invented at load time can only ever close a
/// door the data is behind. What is bounded instead is the RECORD — see
/// <c>VanishedRegionLog.Record</c> and <c>TraceStorageEngine.RemoveColdSegment</c> — which is where
/// an unforgettable number actually did the damage.</para>
/// </summary>
public sealed class SegmentHeaderRangeTests : IDisposable
{
    private const long Ms      = 1_000_000L;
    private const long Hour    = 3_600L * 1_000_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    /// <summary>Byte offset of <c>maxNano</c>: magic(4) + version(2) + spanCount(4) + minNano(8).</summary>
    private const int MaxNanoOffset = 18;

    /// <summary>Byte offset of <c>minNano</c>.</summary>
    private const int MinNanoOffset = 10;

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-hdrrange-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long              _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public SegmentHeaderRangeTests(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_root);
    }

    public void Dispose() { try { Directory.Delete(_root, true); } catch { } }

    private static long NowNano() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * Ms;

    private static void Write(TraceStorageEngine e, ulong id, long startNano)
        => e.WriteSpan(new SpanIngestItem
        {
            TraceId  = new TraceId(0, id), SpanId = new SpanId(id), ParentSpanId = default,
            StartTimeUnixNano = startNano, DurationNanos = 2 * Ms,
            Name = "GET /orders", ServiceName = "billing",
            Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
        });

    private static Task<TraceListPage> ListAsync(TraceStorageEngine e, long fromNano, long toNano) =>
        e.GetTraceListAsync(
            DateTimeOffset.FromUnixTimeMilliseconds(fromNano / Ms),
            DateTimeOffset.FromUnixTimeMilliseconds(toNano   / Ms),
            serviceName: null, spanName: null, status: null,
            minDurationNanos: null, maxDurationNanos: null, limit: 1000);

    private static void Tear(string trc, int offset, long value)
    {
        using var fs = new FileStream(trc, FileMode.Open, FileAccess.Write, FileShare.None);
        using var bw = new BinaryWriter(fs);
        fs.Seek(offset, SeekOrigin.Begin);
        bw.Write(value);
    }

    private static void UnlinkSegmentFiles(string trc)
    {
        File.Delete(trc);
        foreach (var ext in new[] { ".tracesum", ".stats", ".svcgraph" })
        {
            var side = Path.ChangeExtension(trc, ext);
            if (File.Exists(side)) File.Delete(side);
        }
    }

    /// <summary>Backdates every file in the directory, the way <c>cp -p</c> or a restore does.</summary>
    private static void BackdateEveryFile(string dir, DateTime whenUtc)
    {
        foreach (var f in Directory.GetFiles(dir))
            File.SetLastWriteTimeUtc(f, whenUtc);
    }

    /// <summary>Writes <paramref name="count"/> traces into a fresh engine, flushes, returns the .trc.</summary>
    private string SegmentAt(string name, ulong idBase, long startNano, int count = 20)
    {
        string dir = Path.Combine(_root, name);
        Directory.CreateDirectory(dir);
        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        for (int k = 0; k < count; k++) Write(e, idBase + (ulong)k, startNano + k * Ms);
        e.FlushHotTier();
        return e.ColdSegmentsForTest.Single().FilePath;
    }

    // ── The reader hands the header over exactly as written ───────────────────

    [Fact]
    public void An_honest_header_is_returned_exactly_as_written()
    {
        string trc = SegmentAt("honest", 1_000, _baseNano);

        var info = SpanReader.ReadSegmentInfo(trc);
        Assert.Equal(_baseNano, info.MinStartNano);
        Assert.Equal(_baseNano + 19 * Ms, info.MaxStartNano);
    }

    [Fact]
    public void A_max_torn_to_long_MaxValue_is_still_read_back_RAW()
    {
        // The reader's job is to report what the file says. Rewriting the range here was the
        // regression: the same code runs on every segment at every start, and there is no test it
        // can apply that distinguishes "impossible" from "unusual" without closing a door on data
        // that is present. The bound belongs where the number does damage — see the region tests
        // below — not where a walk decides which files to open.
        string trc = SegmentAt("rawmax", 2_000, _baseNano);
        Tear(trc, MaxNanoOffset, long.MaxValue);

        var info = SpanReader.ReadSegmentInfo(trc);
        _out.WriteLine($"min={info.MinStartNano} max={info.MaxStartNano}");

        Assert.Equal(long.MaxValue, info.MaxStartNano);
        Assert.Equal(_baseNano, info.MinStartNano);
    }

    [Fact]
    public void A_torn_segment_is_KEPT_and_stays_queryable()
    {
        // LoadColdSegments DELETES any file ReadSegmentInfo throws on — "Unreadable segment {File}
        // — deleting (likely format v1)", sidecars and all. So refusing a header here would answer
        // one torn byte by destroying every span behind it. The file is kept, and its range is
        // believed: an over-wide range costs re-read work, a narrowed one is a silent gap.
        string dir = Path.Combine(_root, "kept");
        Directory.CreateDirectory(dir);
        string trc;
        using (var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance))
        {
            for (int k = 0; k < 20; k++) Write(e, 3_000 + (ulong)k, _baseNano + k * Ms);
            e.FlushHotTier();
            trc = e.ColdSegmentsForTest.Single().FilePath;
        }
        Tear(trc, MaxNanoOffset, long.MaxValue);

        using var e2 = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e2.LoadColdSegments();

        Assert.True(File.Exists(trc), "the segment file was deleted over one torn header field");
        var seg = Assert.Single(e2.ColdSegmentsForTest);
        Assert.Equal(long.MaxValue, seg.MaxStartNano);
    }

    // ── The two clean-complete-and-empty answers a load-time repair produced ───

    [Fact]
    public async Task Files_restored_with_an_older_mtime_still_answer_ordinary_windows()
    {
        // rsync -at, tar -xp, cp -p, a filesystem snapshot restore, a host whose clock was behind
        // while it was writing: the mtime is BELOW the spans, and it is not evidence about them.
        // Measured against the load-time repair: last 15 min / 1 h / 24 h / 3 days all returned
        // rows=0 Unreadable=False Capped=False over 20 spans that were sitting right there.
        string dir = Path.Combine(_root, "backdated");
        Directory.CreateDirectory(dir);

        long anHourAgo = NowNano() - Hour;
        using (var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance))
        {
            for (int k = 0; k < 20; k++) Write(e, 4_000 + (ulong)k, anHourAgo + k * Ms);
            e.FlushHotTier();
        }
        BackdateEveryFile(dir, DateTime.UtcNow.AddDays(-5));

        using var e2 = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e2.LoadColdSegments();

        long now = NowNano();
        // 15 min is deliberately included: the spans are an hour old, so it is the one window that
        // SHOULD be empty — and it must be empty because the spans are outside it, not because the
        // segment was skipped. Which is why it expects zero and the other three expect all twenty:
        // a blanket "20 everywhere" would have been the assertion contradicting its own comment,
        // and it would fail on the honest answer.
        foreach (var (label, span, expected) in new (string, long, int)[]
                 {
                     ("last 15 min", 15 * 60L * 1_000_000_000L, 0),
                     ("last 1 hour", Hour,                      20),
                     ("last 24 h",   24 * Hour,                 20),
                     ("last 3 days", 72 * Hour,                 20),
                 })
        {
            var page = await ListAsync(e2, now - span - 5 * 60L * 1_000_000_000L, now);
            _out.WriteLine($"{label,-12} rows={page.Rows.Count} Unreadable={page.Unreadable} Capped={page.Capped}");
            Assert.Equal(expected, page.Rows.Count);
            Assert.False(page.Unreadable);
            Assert.False(page.Capped);
        }
    }

    [Fact]
    public async Task A_producer_clock_25_hours_ahead_survives_the_restart_that_reloads_the_header()
    {
        // The same regression from the other side, and the boundary was exactly the slack the
        // repair allowed: +23 h answered 20 rows, +25 h answered none — but only AFTER a restart,
        // because the header is only re-read on load. So the same query answered differently
        // before and after a process bounce, over spans nothing had touched.
        string dir = Path.Combine(_root, "clockahead");
        Directory.CreateDirectory(dir);

        long ahead = NowNano() + 25 * Hour;
        using (var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance))
        {
            for (int k = 0; k < 20; k++) Write(e, 5_000 + (ulong)k, ahead + k * Ms);
            e.FlushHotTier();

            var before = await ListAsync(e, ahead - 60_000 * Ms, ahead + 60_000 * Ms);
            _out.WriteLine($"BEFORE restart rows={before.Rows.Count}");
            Assert.Equal(20, before.Rows.Count);
        }

        using var e2 = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e2.LoadColdSegments();

        var seg = Assert.Single(e2.ColdSegmentsForTest);
        _out.WriteLine($"reload min={seg.MinStartNano} max={seg.MaxStartNano} realMax={ahead + 19 * Ms}");
        Assert.Equal(ahead + 19 * Ms, seg.MaxStartNano);

        var after = await ListAsync(e2, ahead - 60_000 * Ms, ahead + 60_000 * Ms);
        _out.WriteLine($"AFTER  restart rows={after.Rows.Count} Unreadable={after.Unreadable} Capped={after.Capped}");
        Assert.Equal(20, after.Rows.Count);
        Assert.False(after.Unreadable);
    }

    // ── The record, which is where the torn number actually did damage ─────────

    [Fact]
    public async Task A_torn_header_can_no_longer_poison_every_window_for_ever()
    {
        string dir = Path.Combine(_root, "poison");
        Directory.CreateDirectory(dir);

        string trc;
        using (var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance))
        {
            for (int k = 0; k < 20; k++) Write(e, 6_000 + (ulong)k, _baseNano + k * Ms);
            e.FlushHotTier();
            trc = e.ColdSegmentsForTest.Single().FilePath;
        }
        Tear(trc, MaxNanoOffset, long.MaxValue);

        using var e2 = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e2.LoadColdSegments();

        // Now lose the file for real, so a range IS recorded — the point is which range.
        UnlinkSegmentFiles(trc);
        var discovering = await ListAsync(e2, _baseNano - 1000 * Ms, _baseNano + 1000 * Ms);
        Assert.True(discovering.Unreadable, "the walk that met the missing file must still report it");
        Assert.Equal(1, e2.VanishedRegionCountForTest);

        // 400 days later: ten perfectly healthy traces, in a window nothing was ever lost from.
        long far = _baseNano + 400L * 86_400 * 1_000_000_000L;
        for (int k = 0; k < 10; k++) Write(e2, 9_000 + (ulong)k, far + k * Ms);
        e2.FlushHotTier();

        var page = await ListAsync(e2, far - 1000 * Ms, far + 1000 * Ms);
        _out.WriteLine($"far-future window rows={page.Rows.Count} Unreadable={page.Unreadable}");

        Assert.Equal(10, page.Rows.Count);
        Assert.False(page.Unreadable,
            "a window 400 days from the loss reported truncation — the torn max is still in the record");
    }

    [Fact]
    public async Task The_range_recorded_for_a_lost_segment_stops_at_the_file_it_came_from()
    {
        // THE CONSEQUENCE OF MOVING THE BOUND, and the reason the recorded ceiling is the FILE's
        // own last-write time rather than "now" or "now plus a day". A segment written a month ago
        // with a torn max, then lost, must not put a region over LIVE traffic: measured with a
        // now-anchored ceiling, a now-15m window holding none of the lost data still came back
        // rows=0 Unreadable=True, and Forget could not reach it because it keys on Max.
        string dir = Path.Combine(_root, "livewindow");
        Directory.CreateDirectory(dir);

        string trc;
        using (var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance))
        {
            for (int k = 0; k < 20; k++) Write(e, 7_000 + (ulong)k, _baseNano + k * Ms);
            e.FlushHotTier();
            trc = e.ColdSegmentsForTest.Single().FilePath;
        }
        Tear(trc, MaxNanoOffset, long.MaxValue);
        // The file was written when its spans were — a month ago — which is the ordinary case and
        // the only honest ceiling available once the file itself is gone.
        BackdateEveryFile(dir, Base.UtcDateTime.AddMinutes(1));

        using var e2 = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e2.LoadColdSegments();
        UnlinkSegmentFiles(trc);

        Assert.True((await ListAsync(e2, _baseNano - 1000 * Ms, _baseNano + 1000 * Ms)).Unreadable);
        Assert.Equal(1, e2.VanishedRegionCountForTest);

        long now  = NowNano();
        var  live = await ListAsync(e2, now - 15 * 60L * 1_000_000_000L, now);
        _out.WriteLine($"live 15m window rows={live.Rows.Count} Unreadable={live.Unreadable}");
        Assert.False(live.Unreadable,
            "a segment lost a month ago is claiming a hole in the last fifteen minutes");
    }

    [Fact]
    public async Task A_recorded_range_is_never_narrowed_off_the_data_it_described()
    {
        // The other direction of the same clamp, and the failure it must not become. When the
        // mtime is BELOW the segment's own spans — the restored-backup case above — a ceiling
        // applied blindly would pull the recorded Max under the recorded Min and leave a region
        // that misses every span it is supposed to describe: a silent complete over lost data,
        // which is the one answer this whole mechanism exists to prevent.
        string dir = Path.Combine(_root, "narrowed");
        Directory.CreateDirectory(dir);

        long anHourAgo = NowNano() - Hour;
        string trc;
        using (var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance))
        {
            for (int k = 0; k < 20; k++) Write(e, 8_000 + (ulong)k, anHourAgo + k * Ms);
            e.FlushHotTier();
            trc = e.ColdSegmentsForTest.Single().FilePath;
        }
        BackdateEveryFile(dir, DateTime.UtcNow.AddDays(-5));

        using var e2 = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e2.LoadColdSegments();
        UnlinkSegmentFiles(trc);

        long now  = NowNano();
        var  page = await ListAsync(e2, anHourAgo - 1000 * Ms, now);
        _out.WriteLine($"after loss rows={page.Rows.Count} Unreadable={page.Unreadable} "
                     + $"regions={e2.VanishedRegionCountForTest}");

        Assert.True(page.Unreadable, "the window that lost the segment was told nothing");
        Assert.Equal(1, e2.VanishedRegionCountForTest);

        // And on every later request, over the band the spans actually occupied.
        for (int attempt = 2; attempt <= 4; attempt++)
            Assert.True((await ListAsync(e2, anHourAgo - 1000 * Ms, anHourAgo + 1000 * Ms)).Unreadable,
                $"attempt {attempt} forgot a loss whose file carried an older mtime");
    }

    [Fact]
    public async Task Retention_can_reach_a_range_recorded_from_a_torn_header()
    {
        // The half PruneAsync was structurally unable to do: with an unforgettable Max the pass
        // reported "pruned=0, regions=1" for ever.
        string dir = Path.Combine(_root, "forgettable");
        Directory.CreateDirectory(dir);

        string trc;
        using (var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance))
        {
            for (int k = 0; k < 20; k++) Write(e, 10_000 + (ulong)k, _baseNano + k * Ms);
            e.FlushHotTier();
            trc = e.ColdSegmentsForTest.Single().FilePath;
        }
        Tear(trc, MaxNanoOffset, long.MaxValue);
        // The premise this test is about: a segment written in the PAST. Without backdating, the
        // file's write time is this instant, the recorded ceiling is this instant, and no TTL
        // shorter than the test's own runtime could ever pass it — the assertion below would be
        // demanding that retention age out a loss that just happened.
        BackdateEveryFile(dir, DateTime.UtcNow.AddHours(-1));

        using var e2 = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e2.LoadColdSegments();
        UnlinkSegmentFiles(trc);
        Assert.True((await ListAsync(e2, _baseNano - 1000 * Ms, _baseNano + 1000 * Ms)).Unreadable);
        Assert.Equal(1, e2.VanishedRegionCountForTest);

        // One minute of TTL is enough: the recorded ceiling is the file's own last-write time, so
        // it is in the PAST. That is the whole difference between a record retention can age out
        // and one it can never pass at all.
        await e2.PruneAsync(TimeSpan.FromMinutes(1));

        Assert.Equal(0, e2.VanishedRegionCountForTest);
    }

    [Fact]
    public async Task Tearing_the_MIN_alone_was_always_survivable_and_still_is()
    {
        // The asymmetry pinned, because it is what explains where the fix belongs. Forget's test
        // is `Max < cutoff`, so a torn Min never blocked retention — measured on the pre-fix build
        // as regions 1 → 0 on the next pass. It only ever cost an over-wide window.
        string dir = Path.Combine(_root, "minonly");
        Directory.CreateDirectory(dir);

        string trc;
        using (var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance))
        {
            for (int k = 0; k < 20; k++) Write(e, 11_000 + (ulong)k, _baseNano + k * Ms);
            e.FlushHotTier();
            trc = e.ColdSegmentsForTest.Single().FilePath;
        }
        Tear(trc, MinNanoOffset, 0L);

        using var e2 = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e2.LoadColdSegments();
        Assert.Equal(0, e2.ColdSegmentsForTest.Single().MinStartNano);   // zero is a legal instant

        UnlinkSegmentFiles(trc);
        Assert.True((await ListAsync(e2, _baseNano - 1000 * Ms, _baseNano + 1000 * Ms)).Unreadable);

        await e2.PruneAsync(TimeSpan.FromMinutes(1));
        Assert.Equal(0, e2.VanishedRegionCountForTest);
    }

    // ── The log's own door ────────────────────────────────────────────────────

    [Fact]
    public void Record_clamps_an_impossible_range_so_retention_can_always_reach_it()
    {
        // The structural guard, and the one that holds however a caller reaches this method: no
        // range whose Max is above NOW is storable, so no record is ever unforgettable.
        var log = new VanishedRegionLog();
        log.Record(0, long.MaxValue, long.MaxValue);

        Assert.Equal(1, log.CountForTest);

        long inAMinute = DateTimeOffset.UtcNow.AddMinutes(1).ToUnixTimeMilliseconds() * Ms;
        Assert.Equal(1, log.Forget(inAMinute));
        Assert.Equal(0, log.CountForTest);
    }

    [Fact]
    public void Record_clamps_to_NOW_and_not_to_a_day_past_it()
    {
        // A day of slack on the RECORD costs a day of false truncation on windows that hold none
        // of the lost data, and it is a day retention cannot shorten because Forget keys on Max.
        // Slack belongs in what is BELIEVED about a live file, not in what is remembered about a
        // dead one: nothing was ingested after the moment the loss was noticed.
        var log = new VanishedRegionLog();
        log.Record(0, long.MaxValue, long.MaxValue);

        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * Ms;
        Assert.False(log.Overlaps(now + Hour, now + 25 * Hour),
            "the recorded range reaches into tomorrow — every live window is inside it until then");
        Assert.True(log.Overlaps(now - Hour, now), "the range stopped covering the loss itself");
    }

    [Fact]
    public void Forget_trims_the_range_that_straddles_the_cutoff()
    {
        // The asymmetry answered at its source. Forget looked only at the TOP of a range, so the
        // part of a survivor lying BELOW the cutoff — spans retention has now deleted on purpose —
        // went on being reported as a loss.
        var log = new VanishedRegionLog();
        log.Record(1_000, 9_000, long.MaxValue);

        Assert.Equal(0, log.Forget(5_000));      // straddles: kept, not dropped
        Assert.Equal(1, log.CountForTest);

        Assert.False(log.Overlaps(1_000, 4_999),
            "a band retention has already deleted is still being reported as a lost segment");
        Assert.True(log.Overlaps(5_000, 9_000), "the part still inside retention stopped being reported");
    }

    [Fact]
    public void Trimming_keeps_the_list_disjoint_and_sorted()
    {
        // The invariant the trim could plausibly break, asserted rather than argued: only the
        // FIRST surviving range can have a Min below the cutoff, because every later one starts
        // above its predecessor's Max, which is itself at or above the cutoff.
        var log = new VanishedRegionLog();
        log.Record(1_000, 6_000, long.MaxValue);
        log.Record(8_000, 9_000, long.MaxValue);
        log.Record(11_000, 12_000, long.MaxValue);

        Assert.Equal(0, log.Forget(5_000));
        Assert.Equal(3, log.CountForTest);

        Assert.False(log.Overlaps(0, 4_999));
        Assert.True(log.Overlaps(5_000, 6_000));
        Assert.False(log.Overlaps(6_001, 7_999));   // the gap between the first two is still a gap
        Assert.True(log.Overlaps(8_000, 9_000));
        Assert.True(log.Overlaps(11_000, 12_000));
    }
}
