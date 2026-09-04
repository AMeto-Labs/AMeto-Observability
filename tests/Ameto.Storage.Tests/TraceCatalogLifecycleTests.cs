using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// IDENTITY THROUGH THE WHOLE LIFE OF A SEGMENT — written, merged, expired, and found again after
/// a restart.
///
/// <para>An id is only worth having if it means one file for as long as anything might reference
/// it. Every transition below is a place that could break that: a flush that names a segment
/// nobody has seen yet, a compaction that replaces several files with one, retention that unlinks
/// them, and a start that meets files written before any of this existed. The catalog has to come
/// out of each of them agreeing with the directory — and where it cannot, it has to lose rather
/// than the directory, because the directory is what actually holds the spans.</para>
/// </summary>
public sealed class TraceCatalogLifecycleTests : IDisposable
{
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-catalog-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long              _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public TraceCatalogLifecycleTests(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_root);
    }

    public void Dispose() { try { Directory.Delete(_root, true); } catch { } }

    private TraceStorageEngine Engine(string dir) =>
        new(dir, NullLogger<TraceStorageEngine>.Instance);

    private static void Write(TraceStorageEngine e, ulong id, long startNano)
        => e.WriteSpan(new SpanIngestItem
        {
            TraceId  = new TraceId(0, id), SpanId = new SpanId(id), ParentSpanId = default,
            StartTimeUnixNano = startNano, DurationNanos = 2 * Ms,
            Name = "GET /orders", ServiceName = "billing",
            Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
        });

    private string Dir(string name)
    {
        string d = Path.Combine(_root, name);
        Directory.CreateDirectory(d);
        return d;
    }

    [Fact]
    public void A_flushed_segment_is_named_before_any_reader_can_see_it()
    {
        string dir = Dir("flush");
        using var e = Engine(dir);

        for (int k = 0; k < 20; k++) Write(e, 1_000 + (ulong)k, _baseNano + k * Ms);
        e.FlushHotTier();

        var seg = e.ColdSegmentsForTest.Single();
        _out.WriteLine($"segment {seg.SegmentId} ← {Path.GetFileName(seg.FilePath)}");

        // The id is on the snapshot entry itself, not merely in the file: a reader holding the
        // snapshot is what asks whether the index covers this segment.
        Assert.NotEqual(0UL, seg.SegmentId);
        Assert.Equal((1, 0), e.CatalogCountsForTest);   // named, and vouched for by nothing yet
    }

    [Fact]
    public void Ids_are_stable_across_a_restart_and_never_reissued()
    {
        string dir = Dir("restart");
        ulong first;
        using (var e = Engine(dir))
        {
            for (int k = 0; k < 20; k++) Write(e, 2_000 + (ulong)k, _baseNano + k * Ms);
            e.FlushHotTier();
            first = e.ColdSegmentsForTest.Single().SegmentId;
        }

        using var reopened = Engine(dir);
        reopened.LoadColdSegments();
        var seg = reopened.ColdSegmentsForTest.Single();
        _out.WriteLine($"before restart: {first}; after: {seg.SegmentId}");

        // The SAME id, not a fresh one: an index entry written against it before the restart has
        // to still name this file after it.
        Assert.Equal(first, seg.SegmentId);

        // And the next allocation is above it.
        for (int k = 0; k < 20; k++) Write(reopened, 2_500 + (ulong)k, _baseNano + (500 + k) * Ms);
        reopened.FlushHotTier();
        var second = reopened.ColdSegmentsForTest.Select(s => s.SegmentId).Max();
        Assert.True(second > first, $"id {second} was issued at or below the existing {first}");
    }

    [Fact]
    public void A_segment_written_before_the_catalog_existed_is_adopted()
    {
        // The migration path for every install that already has data: the files are there, the
        // manifest is not. Nothing may be lost, and nothing may be rewritten — they just get names.
        string dir = Dir("adopt");
        using (var e = Engine(dir))
        {
            for (int k = 0; k < 20; k++) Write(e, 3_000 + (ulong)k, _baseNano + k * Ms);
            e.FlushHotTier();
            for (int k = 0; k < 20; k++) Write(e, 3_500 + (ulong)k, _baseNano + (500 + k) * Ms);
            e.FlushHotTier();
        }

        File.Delete(Path.Combine(dir, "traces.manifest"));
        Assert.Equal(2, Directory.EnumerateFiles(dir, "*.trc").Count());

        using var reopened = Engine(dir);
        Assert.Equal((0, 0), reopened.CatalogCountsForTest);   // nothing known yet
        reopened.LoadColdSegments();

        var ids = reopened.ColdSegmentsForTest.Select(s => s.SegmentId).ToList();
        _out.WriteLine($"adopted ids: {string.Join(", ", ids)}");
        Assert.Equal(2, ids.Count);
        Assert.DoesNotContain(0UL, ids);
        Assert.Equal(2, ids.Distinct().Count());
        Assert.Equal((2, 0), reopened.CatalogCountsForTest);
    }

    [Fact]
    public void An_entry_whose_file_is_gone_is_dropped_on_load()
    {
        string dir = Dir("stale");
        using (var e = Engine(dir))
        {
            for (int k = 0; k < 20; k++) Write(e, 4_000 + (ulong)k, _baseNano + k * Ms);
            e.FlushHotTier();
        }

        // Something outside the engine took the file — the case VanishedRegionLog is about. The
        // catalog must not go on naming it, and must not go on vouching for it.
        foreach (var f in Directory.EnumerateFiles(dir, "*.trc").ToList()) File.Delete(f);

        using var reopened = Engine(dir);
        reopened.LoadColdSegments();
        _out.WriteLine($"catalog after load: {reopened.CatalogCountsForTest}");
        Assert.Equal((0, 0), reopened.CatalogCountsForTest);
        Assert.Empty(reopened.ColdSegmentsForTest);
    }

    [Fact]
    public void A_compaction_retires_the_source_ids_and_names_the_merged_file()
    {
        string dir = Dir("compact");
        using var e = Engine(dir);

        // Two same-size segments inside one 24 h window is what SelectCompactionBatch takes.
        for (int k = 0; k < 100; k++) Write(e, 5_000 + (ulong)k, _baseNano + k * Ms);
        e.FlushHotTier();
        for (int k = 0; k < 100; k++) Write(e, 6_000 + (ulong)k, _baseNano + (200 + k) * Ms);
        e.FlushHotTier();

        var before = e.ColdSegmentsForTest.Select(s => s.SegmentId).ToHashSet();
        Assert.Equal(2, before.Count);

        e.CompactSmallSegments();

        var after = e.ColdSegmentsForTest;
        _out.WriteLine($"before: [{string.Join(", ", before)}] → after: "
                     + $"[{string.Join(", ", after.Select(s => s.SegmentId))}]");

        Assert.Single(after);
        ulong mergedId = after[0].SegmentId;
        Assert.NotEqual(0UL, mergedId);

        // A RETIRED ID IS NEVER REUSED. Reissuing 1 for the merged file would silently repoint
        // every index entry that named the source.
        Assert.DoesNotContain(mergedId, before);
        Assert.Equal((1, 0), e.CatalogCountsForTest);
    }

    [Fact]
    public async Task Retention_drops_the_catalog_entry_with_the_file()
    {
        string dir = Dir("prune");
        using var e = Engine(dir);

        // Old enough to be past any TTL the test asks for.
        long oldNano = DateTimeOffset.UtcNow.AddDays(-30).ToUnixTimeMilliseconds() * Ms;
        for (int k = 0; k < 20; k++) Write(e, 7_000 + (ulong)k, oldNano + k * Ms);
        e.FlushHotTier();
        Assert.Equal((1, 0), e.CatalogCountsForTest);

        int pruned = await e.PruneAsync(TimeSpan.FromDays(7));

        _out.WriteLine($"pruned {pruned}; catalog now {e.CatalogCountsForTest}");
        Assert.Equal(1, pruned);
        Assert.Equal((0, 0), e.CatalogCountsForTest);
        Assert.Empty(e.ColdSegmentsForTest);
    }

    [Fact]
    public void A_manifest_that_will_not_parse_costs_names_and_nothing_else()
    {
        // The degradation promise, end to end rather than at the manifest's own seam: the engine
        // has to start, the spans have to be readable, and the only thing missing is the fast path.
        string dir = Dir("damaged");
        using (var e = Engine(dir))
        {
            for (int k = 0; k < 30; k++) Write(e, 8_000 + (ulong)k, _baseNano + k * Ms);
            e.FlushHotTier();
        }

        string manifest = Path.Combine(dir, "traces.manifest");
        var raw = File.ReadAllBytes(manifest);
        for (int i = 0; i < raw.Length; i++) raw[i] ^= 0x5A;      // total nonsense, valid file
        File.WriteAllBytes(manifest, raw);

        using var reopened = Engine(dir);
        Assert.Equal((0, 0), reopened.CatalogCountsForTest);      // loaded as empty, did not throw
        reopened.LoadColdSegments();

        // The spans are all still there — the catalog never held any of them.
        var seg = reopened.ColdSegmentsForTest.Single();
        _out.WriteLine($"after a destroyed manifest: {seg.SpanCount} spans, id {seg.SegmentId}");
        Assert.Equal(30, seg.SpanCount);
        Assert.NotEqual(0UL, seg.SegmentId);                     // re-adopted under a fresh name
    }
}
