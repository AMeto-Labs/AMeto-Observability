using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT THE STARTUP SCAN OWES THE QUERY PATH WHEN IT DESTROYS A SEGMENT.
///
/// <para><c>VanishedSegmentMemoryTests</c> and <c>ColdSegmentFaultTests</c> between them pin the
/// QUERY path's honesty: a file that is gone leaves a region behind, so no later page over that
/// band can answer <c>done {"complete":true}</c>. Both were built around losses caused by
/// something OUTSIDE the engine.</para>
///
/// <para>This is the loss the engine causes itself. <c>LoadColdSegments</c> ends in a catch that
/// deletes a segment whose content will not parse — the v1 migration path, and the only place in
/// the engine that destroys data it has not replaced. It recorded nothing, so the window went from
/// "on disk" to "on no disk" and every read over it went on making the strong positive claim. The
/// query path was taught this contract in the same branch; the startup path was not.</para>
/// </summary>
public sealed class ColdSegmentStartupDeleteTests : IDisposable
{
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-startupdel-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long              _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public ColdSegmentStartupDeleteTests(ITestOutputHelper output)
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

    /// <summary>
    /// Writes one real segment, then breaks it the way a v1 file is broken: the 27-byte header is
    /// untouched and correct, everything past it will not parse. This is the shape the deleting
    /// catch was written for, and the shape whose range is still perfectly readable.
    /// </summary>
    private string WriteThenCorruptTail(string dir, ulong idBase, long firstNano)
    {
        using (var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance))
        {
            for (int k = 0; k < 40; k++) Write(e, idBase + (ulong)k, firstNano + k * Ms);
            e.FlushHotTier();
        }

        string trc = Directory.EnumerateFiles(dir, "*.trc").Single();
        using (var fs = new FileStream(trc, FileMode.Open, FileAccess.Write))
        {
            // The footer magic, which is what a v1 file fails on. Header untouched.
            fs.Seek(-4, SeekOrigin.End);
            fs.Write([0xDE, 0xAD, 0xBE, 0xEF]);
        }
        return trc;
    }

    [Fact]
    public async Task A_segment_deleted_at_startup_leaves_a_region_behind()
    {
        string dir = Path.Combine(_root, "recorded");
        Directory.CreateDirectory(dir);

        string doomed = WriteThenCorruptTail(dir, 1_000, _baseNano);

        // Reopen: this is the startup the deletion happens on.
        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e.LoadColdSegments();

        _out.WriteLine($"after load: exists={File.Exists(doomed)} "
                     + $"segments={e.ColdSegmentCountForTest} regions={e.VanishedRegionCountForTest}");

        // The migration behaviour is unchanged — the file that will not parse is still removed.
        Assert.False(File.Exists(doomed), "the unparseable segment was left on disk");
        Assert.Equal(0, e.ColdSegmentCountForTest);

        // THE PART THAT WAS MISSING. Deleting is a decision about disk AND about every later
        // answer, and only one of the two was being made.
        Assert.Equal(1, e.VanishedRegionCountForTest);

        // And the claim it buys: the window the deleted file covered can no longer be called whole.
        var page = await ListAsync(e, _baseNano - 1000 * Ms, _baseNano + 1000 * Ms);
        _out.WriteLine($"page rows={page.Rows.Count} Unreadable={page.Unreadable}");
        Assert.Empty(page.Rows);
        Assert.True(page.Unreadable,
            "a window whose only segment the engine deleted at startup answered as complete");
    }

    [Fact]
    public async Task A_window_the_deleted_segment_never_covered_is_still_whole()
    {
        // The control. A region that swallows the whole timeline would pass the test above and be
        // useless in production — the banner would sit over every query on the install for ever.
        // The recorded band has to be the band that was lost, so a window well clear of it must
        // still answer complete.
        string dir = Path.Combine(_root, "narrow");
        Directory.CreateDirectory(dir);

        WriteThenCorruptTail(dir, 2_000, _baseNano);

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e.LoadColdSegments();
        Assert.Equal(1, e.VanishedRegionCountForTest);

        // Rows the survivor holds, an hour clear of the deleted band.
        long farNano = _baseNano + 3_600_000 * Ms;
        for (int k = 0; k < 10; k++) Write(e, 2_500 + (ulong)k, farNano + k * Ms);
        e.FlushHotTier();

        var far = await ListAsync(e, farNano - 1000 * Ms, farNano + 1000 * Ms);
        _out.WriteLine($"far page rows={far.Rows.Count} Unreadable={far.Unreadable}");
        Assert.Equal(10, far.Rows.Count);
        Assert.False(far.Unreadable,
            "the region recorded for a deleted segment covers time that segment never held");
    }

    [Fact]
    public void A_segment_whose_range_cannot_be_read_is_not_deleted_at_all()
    {
        // The other half of the contract, and the one the fix would be dishonest without: being
        // unable to RECORD a loss is not a licence to CAUSE one. With the header gone there is no
        // band to record, so a deletion here would be unreportable by construction — no region to
        // overlap, no path to classify, every later window silently whole.
        string dir = Path.Combine(_root, "unnameable");
        Directory.CreateDirectory(dir);

        string trc = WriteThenCorruptTail(dir, 3_000, _baseNano);
        using (var fs = new FileStream(trc, FileMode.Open, FileAccess.Write))
        {
            fs.Seek(0, SeekOrigin.Begin);
            fs.Write([0x00, 0x00, 0x00, 0x00]);   // the magic too — now nothing can be believed
        }
        long sizeBefore = new FileInfo(trc).Length;

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e.LoadColdSegments();

        _out.WriteLine($"exists={File.Exists(trc)} incomplete={e.ColdTierIncompleteForTest} "
                     + $"regions={e.VanishedRegionCountForTest}");

        Assert.True(File.Exists(trc), "a segment whose loss could not be recorded was deleted anyway");
        Assert.Equal(sizeBefore, new FileInfo(trc).Length);

        // Loud instead: the cold tier is short for this process, and every query says so. That is
        // recoverable — move the file aside and restart — which a deletion is not.
        Assert.True(e.ColdTierIncompleteForTest);
    }

    [Fact]
    public void The_header_range_survives_a_file_the_full_read_refuses()
    {
        // The measurement the fix rests on: ReadSegmentInfo fails on this file, TryReadHeaderRange
        // does not. If that ever stops holding, the fix above silently degrades to the "cannot
        // name the loss" branch and v1 segments stop being migrated — so it is pinned directly.
        string dir = Path.Combine(_root, "headeronly");
        Directory.CreateDirectory(dir);

        string trc = WriteThenCorruptTail(dir, 4_000, _baseNano);

        Assert.ThrowsAny<Exception>(() => SpanReader.ReadSegmentInfo(trc));

        Assert.True(SpanReader.TryReadHeaderRange(trc, out long min, out long max));
        _out.WriteLine($"header range [{min}, {max}] vs written [{_baseNano}, {_baseNano + 39 * Ms}]");
        Assert.Equal(_baseNano, min);
        Assert.Equal(_baseNano + 39 * Ms, max);
    }
}
