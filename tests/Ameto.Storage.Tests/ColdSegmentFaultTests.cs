using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT A COLD WALK IS ALLOWED TO CONCLUDE FROM A SEGMENT IT COULD NOT READ.
///
/// <para><c>VanishedSegmentMemoryTests</c> pins the case where the answer is "the data is gone":
/// the file was unlinked by something outside the engine, and every later read whose window
/// overlaps it has to be told, for ever. That mechanism was built first, and building it turned
/// two OTHER faults into permanent data-loss claims because they arrive through the same catch
/// blocks and nothing distinguished them:</para>
/// <list type="bullet">
///   <item>a MOUNT BLIP. The <c>DirectoryNotFoundException</c> catch treated a missing directory
///   as proof of permanent loss and healed every segment out of the snapshot. Nothing rescans —
///   <c>LoadColdSegments</c> runs once, at startup — so an SMB, iSCSI or bind-mount hiccup cost
///   the whole cold tier for the life of the process AND claimed a data loss that had not
///   happened. On <c>origin/main</c> that path had no catch at all, so the request failed
///   transiently and the next one recovered: the guard made it strictly worse;</item>
///   <item>a COMPACTION HANDOVER. <c>RemoveColdSegment</c> already knew this one was not a loss
///   and recorded nothing — and then the caller set its own per-request fault bit anyway, so the
///   MEMORY was right and the REQUEST said "deleted or damaged" on a completely healthy server.</item>
/// </list>
///
/// <para>The distinction the fault bit needs is the one the memory already makes, plus one more
/// piece of evidence nobody was looking at: a directory that is missing is not the same evidence
/// as a file that is missing while its directory is intact.</para>
/// </summary>
public sealed class ColdSegmentFaultTests : IDisposable
{
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-coldfault-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long              _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public ColdSegmentFaultTests(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_root);
    }

    public void Dispose() { try { Directory.Delete(_root, true); } catch { } }

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

    private static void UnlinkSegmentFiles(string trc)
    {
        File.Delete(trc);
        foreach (var ext in new[] { ".tracesum", ".stats", ".svcgraph" })
        {
            var side = Path.ChangeExtension(trc, ext);
            if (File.Exists(side)) File.Delete(side);
        }
    }

    // ── P1b: a mount blip ─────────────────────────────────────────────────────

    /// <summary>Creates an NTFS junction. Returns false when the platform will not make one.</summary>
    private static bool TryJunction(string link, string target)
    {
        try
        {
            var psi = new ProcessStartInfo("cmd.exe", $"/c mklink /J \"{link}\" \"{target}\"")
            {
                RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false,
            };
            using var p = Process.Start(psi)!;
            p.WaitForExit();
            return p.ExitCode == 0;
        }
        catch { return false; }
    }

    [Fact]
    public async Task A_directory_that_blips_costs_ONE_REQUEST_and_not_the_cold_tier()
    {
        // The engine is rooted on a junction so the DIRECTORY can be taken away and put back with
        // every file untouched — which is what a remount is, and what no amount of deleting files
        // can simulate. Deleting the junction removes the reparse point, not the contents.
        string real = Path.Combine(_root, "real");
        string link = Path.Combine(_root, "link");
        Directory.CreateDirectory(real);
        if (!TryJunction(link, real)) return;   // no junction support — nothing to assert

        using var e = new TraceStorageEngine(link, NullLogger<TraceStorageEngine>.Instance);
        for (int k = 0; k < 50; k++) Write(e, 1_000 + (ulong)k, _baseNano + k * Ms);
        e.FlushHotTier();
        for (int k = 0; k < 50; k++) Write(e, 2_000 + (ulong)k, _baseNano + (200 + k) * Ms);
        e.FlushHotTier();

        long from = _baseNano - 1000 * Ms, to = _baseNano + 1000 * Ms;

        var healthy = await ListAsync(e, from, to);
        Assert.Equal(100, healthy.Rows.Count);
        Assert.False(healthy.Unreadable);
        Assert.Equal(2, e.ColdSegmentCountForTest);

        // THE BLIP.
        Directory.Delete(link);

        var during = await ListAsync(e, from, to);
        _out.WriteLine($"during   rows={during.Rows.Count} Unreadable={during.Unreadable} "
                     + $"Capped={during.Capped} segs={e.ColdSegmentCountForTest} regions={e.VanishedRegionCountForTest}");

        // The request could not read the cold tier and says so — as TRUNCATION, which is
        // recoverable and which the client answers with "narrow the window / try again", not as a
        // data loss, which raises the red banner and freezes the list.
        Assert.True(during.Capped, "a request that read none of the cold tier claimed to be complete");
        Assert.False(during.Unreadable,
            "a mount blip was reported as data loss — the files are all still on the disk");

        // AND NOTHING WAS THROWN AWAY. These two are what made the old behaviour permanent.
        Assert.Equal(2, e.ColdSegmentCountForTest);
        Assert.Equal(0, e.VanishedRegionCountForTest);

        // THE REMOUNT. Nothing rescans — LoadColdSegments runs once at startup — so a snapshot
        // healed during the blip could never come back on its own.
        Assert.True(TryJunction(link, real));
        Assert.Equal(2, Directory.GetFiles(link, "*.trc").Length);

        var after = await ListAsync(e, from, to);
        _out.WriteLine($"after    rows={after.Rows.Count} Unreadable={after.Unreadable} "
                     + $"Capped={after.Capped} segs={e.ColdSegmentCountForTest} regions={e.VanishedRegionCountForTest}");

        Assert.Equal(100, after.Rows.Count);
        Assert.False(after.Unreadable);
        Assert.False(after.Capped);
        Assert.Equal(0, e.VanishedRegionCountForTest);
    }

    [Fact]
    public async Task The_span_scan_answers_a_directory_blip_the_same_way_the_trace_list_does()
    {
        // The asymmetry the two walks used to have: the list caught DirectoryNotFoundException
        // explicitly (and removed the segment), the span search caught only FileNotFoundException
        // and let the directory fault fall into its generic catch, where it was reported as a
        // corrupt file. One blip, two streams, two different stories.
        string real = Path.Combine(_root, "real2");
        string link = Path.Combine(_root, "link2");
        Directory.CreateDirectory(real);
        if (!TryJunction(link, real)) return;

        using var e = new TraceStorageEngine(link, NullLogger<TraceStorageEngine>.Instance);
        for (int k = 0; k < 40; k++) Write(e, 3_000 + (ulong)k, _baseNano + k * Ms);
        e.FlushHotTier();

        var fromAt = DateTimeOffset.FromUnixTimeMilliseconds((_baseNano - 1000 * Ms) / Ms);
        var toAt   = DateTimeOffset.FromUnixTimeMilliseconds((_baseNano + 1000 * Ms) / Ms);

        Directory.Delete(link);

        var floor = new SpanScanFloor();
        await foreach (var _ in e.SearchSpansAsync(fromAt, toAt, scanFloor: floor)) { }

        _out.WriteLine($"span scan during blip: Truncated={floor.Truncated} Unreadable={floor.Unreadable} "
                     + $"segs={e.ColdSegmentCountForTest} regions={e.VanishedRegionCountForTest}");

        Assert.True(floor.Truncated, "the span scan read none of the cold tier and reported no floor");
        Assert.False(floor.Unreadable, "the span scan called a mount blip a data loss");
        Assert.Equal(1, e.ColdSegmentCountForTest);
        Assert.Equal(0, e.VanishedRegionCountForTest);

        Assert.True(TryJunction(link, real));

        int n = 0;
        var later = new SpanScanFloor();
        await foreach (var _ in e.SearchSpansAsync(fromAt, toAt, scanFloor: later)) n++;
        Assert.Equal(40, n);
        Assert.False(later.Truncated);
        Assert.False(later.Unreadable);
    }

    // ── P1c: a healthy compaction handover ────────────────────────────────────

    /// <summary>
    /// Retires and unlinks a segment at the instant a walk is about to open it — the compaction
    /// handover race, run deliberately instead of waited for. <c>CompactSmallSegments</c> is what
    /// publishes the replacement and unlinks the sources; the seam puts the walk exactly between
    /// the two, which is where it lands by luck on any install that compacts.
    /// </summary>
    private static Action<SpanSegmentInfo> RetireOnFirstRead(TraceStorageEngine e)
    {
        bool done = false;
        return _ =>
        {
            if (done) return;
            done = true;
            e.CompactSmallSegments();   // publishes the merged output, then unlinks its sources
        };
    }

    [Fact]
    public async Task A_compaction_handover_caps_the_page_that_races_it_but_does_not_call_it_damaged()
    {
        string dir = Path.Combine(_root, "handover");
        Directory.CreateDirectory(dir);
        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        // Two same-size segments inside one 24 h window is what SelectCompactionBatch takes.
        for (int k = 0; k < 100; k++) Write(e, 6_000 + (ulong)k, _baseNano + k * Ms);
        e.FlushHotTier();
        for (int k = 0; k < 100; k++) Write(e, 7_000 + (ulong)k, _baseNano + (200 + k) * Ms);
        e.FlushHotTier();
        Assert.Equal(2, e.ColdSegmentCountForTest);

        long from = _baseNano - 1000 * Ms, to = _baseNano + 1000 * Ms;

        e._beforeColdSegmentRead = RetireOnFirstRead(e);
        var raced = await ListAsync(e, from, to);
        e._beforeColdSegmentRead = null;

        _out.WriteLine($"raced    rows={raced.Rows.Count} Unreadable={raced.Unreadable} "
                     + $"Capped={raced.Capped} regions={e.VanishedRegionCountForTest}");

        // THE MEMORY WAS ALWAYS RIGHT — the segments were retired on purpose, so nothing is lost.
        Assert.Equal(0, e.VanishedRegionCountForTest);

        // AND NOW THE REQUEST AGREES WITH IT. This is the assertion that used to fail: the page
        // reported a permanent data loss on a server whose compaction had just worked perfectly,
        // which on the client is a red banner over a frozen list.
        Assert.False(raced.Unreadable,
            "a healthy compaction handover was reported to the caller as deleted or damaged data");

        // It IS still a short page, and saying so is the whole point of keeping the floor: this
        // walk held the pre-swap snapshot, so the rows it missed are in the replacement and the
        // caller has to come back for them.
        Assert.True(raced.Capped, "the page that raced the handover claimed to have read the window out");

        // And the very next request, on the post-swap snapshot, is whole.
        var next = await ListAsync(e, from, to);
        Assert.Equal(200, next.Rows.Count);
        Assert.False(next.Unreadable);
        Assert.False(next.Capped);
    }

    [Fact]
    public async Task The_span_scan_makes_the_same_distinction_on_the_same_race()
    {
        string dir = Path.Combine(_root, "handover-span");
        Directory.CreateDirectory(dir);
        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        for (int k = 0; k < 100; k++) Write(e, 8_000 + (ulong)k, _baseNano + k * Ms);
        e.FlushHotTier();
        for (int k = 0; k < 100; k++) Write(e, 9_000 + (ulong)k, _baseNano + (200 + k) * Ms);
        e.FlushHotTier();

        var fromAt = DateTimeOffset.FromUnixTimeMilliseconds((_baseNano - 1000 * Ms) / Ms);
        var toAt   = DateTimeOffset.FromUnixTimeMilliseconds((_baseNano + 1000 * Ms) / Ms);

        e._beforeColdSegmentRead = RetireOnFirstRead(e);
        var floor = new SpanScanFloor();
        await foreach (var _ in e.SearchSpansAsync(fromAt, toAt, limit: 1000, scanFloor: floor)) { }
        e._beforeColdSegmentRead = null;

        _out.WriteLine($"span race: Truncated={floor.Truncated} Unreadable={floor.Unreadable} "
                     + $"regions={e.VanishedRegionCountForTest}");

        Assert.Equal(0, e.VanishedRegionCountForTest);
        Assert.False(floor.Unreadable,
            "the span scan called a healthy compaction handover a data loss");
    }

    // ── The loss itself still reports, which is what all of this is for ───────

    [Fact]
    public async Task A_file_that_really_did_vanish_is_still_a_permanent_fault()
    {
        // The control for every relaxation above. Three verdicts were separated; exactly one of
        // them is a data loss, and it must still behave as VanishedSegmentMemoryTests requires —
        // otherwise this round has quietly reintroduced the silent under-report it inherited.
        string dir = Path.Combine(_root, "reallost");
        Directory.CreateDirectory(dir);
        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        for (int k = 0; k < 20; k++) Write(e, 4_000 + (ulong)k, _baseNano + k * Ms);
        e.FlushHotTier();
        var doomed = e.ColdSegmentsForTest.Single();

        for (int k = 0; k < 20; k++) Write(e, 5_000 + (ulong)k, _baseNano + (200 + k) * Ms);
        e.FlushHotTier();

        // The directory is intact and the engine retired nothing: the file is simply gone.
        UnlinkSegmentFiles(doomed.FilePath);

        long from = _baseNano - 1000 * Ms, to = _baseNano + 1000 * Ms;

        var page = await ListAsync(e, from, to);
        Assert.True(page.Unreadable, "a genuinely lost segment stopped being reported");
        Assert.Equal(20, page.Rows.Count);
        Assert.Equal(1, e.VanishedRegionCountForTest);

        // And on every later request, which is the property the memory exists for.
        for (int attempt = 2; attempt <= 4; attempt++)
            Assert.True((await ListAsync(e, from, to)).Unreadable, $"attempt {attempt} forgot the loss");
    }

    [Fact]
    public async Task A_volume_that_came_back_EMPTY_is_reported_as_loss_ON_PURPOSE()
    {
        // THE GUARD THAT USED TO BE HERE IS GONE, and this test is where that decision is pinned.
        //
        // Three attempts tried to recognise an unpopulated volume from the filesystem, and each
        // was defeated by this engine's own files: 'no directory entries at all' by spans.wal,
        // which lives in the data directory; 'two segments gone and no .trc left' by CompleteFlush,
        // which writes a fresh .trc into whatever the data directory currently is, within seconds
        // on the busy install the whole branch exists for. And each bought its rare true positive
        // by suppressing the ordinary true negative — measured with the last guard in place, a
        // wholesale delete on an install with two segments reported Unreadable=False on three
        // consecutive requests, permanently.
        //
        // So an empty remount is now reported as what it looks like from inside a query: the files
        // this window needed are not there. That over-reports for a volume that later comes back —
        // and over-reporting is the recoverable direction, because the region is bounded and
        // retention ages it out, while a suppressed loss is a window quietly called whole.
        //
        // The BLIP is a different question with real evidence behind it, and it is still answered
        // transiently — see A_directory_that_blips_costs_ONE_REQUEST_and_not_the_cold_tier, where
        // the directory itself is gone rather than merely empty.
        string real = Path.Combine(_root, "realvol");
        string stub = Path.Combine(_root, "emptyvol");
        string link = Path.Combine(_root, "mount");
        Directory.CreateDirectory(real);
        Directory.CreateDirectory(stub);
        if (!TryJunction(link, real)) return;   // needs a junction; skipped where it cannot be made

        using var e = new TraceStorageEngine(link, NullLogger<TraceStorageEngine>.Instance);
        for (int k = 0; k < 10; k++) Write(e, 6_000 + (ulong)k, _baseNano + k * Ms);
        e.FlushHotTier();
        for (int k = 10; k < 20; k++) Write(e, 6_000 + (ulong)k, _baseNano + k * Ms);
        e.FlushHotTier();

        long from = _baseNano - 1000 * Ms, to = _baseNano + 1000 * Ms;
        Assert.Equal(20, (await ListAsync(e, from, to)).Rows.Count);

        Directory.Delete(link);
        if (!TryJunction(link, stub)) return;

        var during = await ListAsync(e, from, to);
        _out.WriteLine($"empty mount rows={during.Rows.Count} Unreadable={during.Unreadable} "
                     + $"regions={e.VanishedRegionCountForTest}");

        // Reported, not swallowed. The window really cannot be served, and saying so is the whole
        // contract — the alternative that was tried here said nothing and meant it permanently.
        Assert.True(during.Unreadable,
            "a window whose segments are all unreachable came back without a fault");

        // And bounded: what is recorded is the range those segments covered, nothing wider, so
        // retention can age it out.
        Assert.True(e.VanishedRegionCountForTest >= 1);
    }

    [Fact]
    public async Task Two_walks_that_meet_ONE_lost_file_give_ONE_verdict()
    {
        // RemoveColdSegment is atomic, so of two readers meeting the same genuinely deleted file
        // exactly one wins the removal and is told "Lost"; the loser saw a segment already off the
        // snapshot and would call it a compaction handover — the same fault, two verdicts, and the
        // loser advises "narrow the window and retry" for data no window will return. The memory
        // settles it: a range already recorded is a loss somebody else just proved.
        string dir = Path.Combine(_root, "tworeaders");
        Directory.CreateDirectory(dir);
        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        for (int k = 0; k < 20; k++) Write(e, 7_000 + (ulong)k, _baseNano + k * Ms);
        e.FlushHotTier();
        var doomed = e.ColdSegmentsForTest.Single();
        for (int k = 0; k < 20; k++) Write(e, 7_500 + (ulong)k, _baseNano + (200 + k) * Ms);
        e.FlushHotTier();

        UnlinkSegmentFiles(doomed.FilePath);

        long from = _baseNano - 1000 * Ms, to = _baseNano + 1000 * Ms;

        // The first walk discovers it and de-lists it — that is the "winner".
        Assert.True((await ListAsync(e, from, to)).Unreadable);

        // Now replay the loser exactly: a reader still holding the pre-removal snapshot meets the
        // same file. It must reach the same verdict, not "handover".
        Assert.Equal(TraceStorageEngine.ColdReadFault.Lost, e.MeetMissingSegmentFileVerdictForTest(doomed));
    }
}