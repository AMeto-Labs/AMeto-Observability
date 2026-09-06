using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// A COVERAGE CLAIM MUST NEVER OUTLIVE THE RUN BEHIND IT — the review findings on #69.
///
/// <para>Coverage is the one thing that lets a segment be SKIPPED. Everywhere it is granted, the
/// run that justifies it has to already be open, because the two states in between are both
/// wrong in the same direction: the manifest says "covered", the store holds nothing, and a lookup
/// finds hits, finds coverage, finds no offsets for that segment, and omits its spans. No error is
/// raised and nothing re-derives coverage from the runs, so it lasts until a restart — or, for the
/// compaction case, past one.</para>
///
/// <para>The trigger is mundane rather than exotic: on Windows an antivirus or backup agent holding
/// a handle on the just-renamed <c>.tix</c> is a sharing violation inside
/// <c>TraceIndexReader.Open</c>, which catches everything and answers null.</para>
/// </summary>
public sealed class TraceIndexCoverageClaimTests : IDisposable
{
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-claim-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long              _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public TraceIndexCoverageClaimTests(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_root);
    }

    public void Dispose() { try { Directory.Delete(_root, true); } catch { } }

    private string Dir(string name)
    {
        string d = Path.Combine(_root, name);
        Directory.CreateDirectory(d);
        return d;
    }

    private static TraceStorageEngine Engine(string dir) => new(dir, NullLogger<TraceStorageEngine>.Instance);

    private static TraceId Id(int i) => new(
        unchecked((ulong)(i * 6364136223846793005L + 1442695040888963407L)),
        unchecked((ulong)(i * 2862933555777941757L + 3037000493L)));

    private static void WriteSpan(TraceStorageEngine e, TraceId trace, ulong spanId, long startNano)
        => e.WriteSpan(new SpanIngestItem
        {
            TraceId = trace, SpanId = new SpanId(spanId), ParentSpanId = default,
            StartTimeUnixNano = startNano, DurationNanos = 2 * Ms,
            Name = "GET /orders", ServiceName = "billing",
            Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
        });

    private List<TraceId> Build(TraceStorageEngine e, int segments, int tracesPer = 20, long? from = null)
    {
        long at = from ?? _baseNano;
        var all = new List<TraceId>();
        ulong span = 1;
        for (int s = 0; s < segments; s++)
        {
            for (int t = 0; t < tracesPer; t++)
            {
                var id = Id(s * 1000 + t);
                all.Add(id);
                for (int k = 0; k < 3; k++)
                    WriteSpan(e, id, span++, at + (s * 10_000 + t * 10 + k) * Ms);
            }
            e.FlushHotTier();
        }
        return all;
    }

    private static async Task<List<SpanRecord>> Read(TraceStorageEngine e, TraceId id)
    {
        var got = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(id)) got.Add(s);
        return got;
    }

    // ── Finding 1: the claim must not survive a failed open ────────────────────

    [Fact]
    public async Task A_flush_whose_run_will_not_open_leaves_the_segment_uncovered()
    {
        // The run file is made unopenable the instant it is written, standing in for the sharing
        // violation an antivirus produces on Windows. The engine must notice and NOT claim coverage
        // — a claim with nothing behind it costs the segment's spans on every lookup.
        string dir = Dir("flushfail");
        using var e = Engine(dir);
        Build(e, segments: 2);                                   // healthy neighbours, so runs exist

        e._afterIndexRunWrittenForTest = path =>
        {
            // Wrong magic: Open() refuses it and answers null, exactly as it does for a file it
            // cannot read for any other reason.
            var raw = File.ReadAllBytes(path);
            raw[0] ^= 0xFF;
            File.WriteAllBytes(path, raw);
        };

        var doomed = Id(777_001);
        for (int k = 0; k < 3; k++) WriteSpan(e, doomed, (ulong)(9000 + k), _baseNano + (500_000 + k) * Ms);
        e.FlushHotTier();
        e._afterIndexRunWrittenForTest = null;

        var (covered, total) = e.IndexCoverage;
        _out.WriteLine($"after a flush whose run will not open: coverage {covered}/{total}");

        // THE ASSERTION. Covered would mean the manifest vouching for a segment nothing serves.
        Assert.Equal(total - 1, covered);

        // And the proof that it matters: the spans are still there.
        var spans = await Read(e, doomed);
        _out.WriteLine($"the doomed segment's trace: {spans.Count} spans, "
                     + $"opened {e.SegmentsOpenedByLastTraceLookup} segment(s)");
        Assert.Equal(3, spans.Count);
    }

    [Fact]
    public async Task A_backfill_whose_run_will_not_open_leaves_the_segment_uncovered()
    {
        string dir = Dir("backfillfail");
        List<TraceId> all;
        using (var seed = Engine(dir)) { all = Build(seed, segments: 3); }

        // A pre-index install: segments on disk, no runs, no catalog.
        foreach (var f in Directory.EnumerateFiles(dir, "*.tix").ToList()) File.Delete(f);
        File.Delete(Path.Combine(dir, "traces.manifest"));

        using var e = Engine(dir);
        e.LoadColdSegments();
        e._afterIndexRunWrittenForTest = path =>
        {
            var raw = File.ReadAllBytes(path);
            raw[0] ^= 0xFF;
            File.WriteAllBytes(path, raw);
        };

        while (e.BackfillNextSegment()) { }
        e._afterIndexRunWrittenForTest = null;

        _out.WriteLine($"backfill with every run unopenable: coverage {e.IndexCoverage}");
        Assert.Equal(0, e.IndexCoverage.Covered);

        // Every trace still resolves, by scanning.
        foreach (var id in all.Take(5))
            Assert.Equal(3, (await Read(e, id)).Count);
    }

    // ── Finding 2: liveness comes from the catalog, not the snapshot ───────────

    [Fact]
    public async Task A_merge_racing_a_flush_does_not_strand_the_new_segment()
    {
        // THE ONE THAT DOES NOT HEAL AT STARTUP, so it gets the real window rather than a proxy.
        //
        // A flush publishes into Segments / Runs / Covered and only then swaps _coldSegments.
        // CompactIndexOnce takes no engine lock — the backfill worker calls it straight through —
        // so it can run in between. Judging liveness by the snapshot made it call the freshly
        // published segment dead: every entry of its run dropped as garbage, the segment left out
        // of the merged run's CoveredSegments, and ReplaceRuns then deleting its run. When the swap
        // landed, that segment was covered with no run anywhere holding its entries, and
        // GetTraceAsync's covered/no-hit branch skipped it. Every trace in it returned empty, for
        // good, because nothing re-derives coverage from the runs.
        string dir = Dir("liveness");
        using var e = Engine(dir);
        Build(e, segments: 10);                        // level 1 is full, so a merge is due

        var straddler = Id(555_001);
        bool merged = false;
        e._inCatalogNotYetInSnapshotForTest = () =>
        {
            if (merged) return;
            merged = true;
            e.CompactIndexOnce();                      // exactly in the window
        };

        for (int k = 0; k < 3; k++) WriteSpan(e, straddler, (ulong)(8100 + k), _baseNano + (700_000 + k) * Ms);
        e.FlushHotTier();
        e._inCatalogNotYetInSnapshotForTest = null;
        Assert.True(merged, "the merge never ran inside the window — the test proved nothing");

        var covered = e.CoveredSegmentIdsForTest;
        var named   = e.IndexRunCoverageForTest;
        _out.WriteLine($"after the race: covered {covered.Count}, named by runs {named.Count}, "
                     + $"runs {e.IndexStatsForTest.Runs}");

        // No segment may be vouched for by a catalog that has no run holding its entries.
        foreach (ulong sid in covered)
            Assert.Contains(sid, named);

        // And the trace that was mid-flush comes back whole.
        var spans = await Read(e, straddler);
        _out.WriteLine($"the straddling trace: {spans.Count} spans, "
                     + $"opened {e.SegmentsOpenedByLastTraceLookup} segment(s)");
        Assert.Equal(3, spans.Count);
    }

    [Fact]
    public async Task Every_covered_segment_is_named_by_a_run_after_a_flush_and_a_merge()
    {
        // The end-to-end form of the same invariant, across the operations that grant and move
        // coverage: flush, backfill, merge.
        string dir = Dir("invariant");
        using var e = Engine(dir);
        var all = Build(e, segments: 11);
        Assert.True(e.CompactIndexOnce());
        Build(e, segments: 3, from: _baseNano + 5_000_000L * Ms);

        var covered = e.CoveredSegmentIdsForTest;
        var named   = e.IndexRunCoverageForTest;
        _out.WriteLine($"covered {covered.Count}, named by runs {named.Count}, "
                     + $"runs {e.IndexStatsForTest.Runs}");

        foreach (ulong sid in covered)
            Assert.Contains(sid, named);

        foreach (var id in all.Take(10))
            Assert.Equal(3, (await Read(e, id)).Count);
    }

    // ── Finding 3: a truncated input must not be read as an exhausted one ──────

    [Fact]
    public void A_merge_refuses_an_input_whose_interior_block_is_torn()
    {
        // Open() validates the header, footer, sparse index and bloom — not the blocks. So a run
        // with a torn INTERIOR block opens perfectly and enumerates right up to it. Reading that
        // short answer as an end wrote a survivor claiming the union of its inputs' coverage while
        // missing the tail it never copied, then deleted the sources: silent, and permanent.
        string dir = Dir("torninput");
        using var e = Engine(dir);
        Build(e, segments: 10, tracesPer: 400);        // big enough that a run has several blocks

        var runs = Directory.EnumerateFiles(dir, "*.tix").OrderBy(f => f).ToList();
        var victim = runs.First(f => new FileInfo(f).Length > 6_000);

        // Damage well past the header and well before the footer: an interior block.
        var raw = File.ReadAllBytes(victim);
        for (int i = 200; i < Math.Min(600, raw.Length - 200); i++) raw[i] ^= 0xA5;
        File.WriteAllBytes(victim, raw);
        Assert.NotNull(TraceIndexReader.Open(victim));   // the premise: it still OPENS

        var coverageBefore = e.IndexCoverage;
        int runsBefore     = e.IndexStatsForTest.Runs;

        Assert.False(e.CompactIndexOnce());

        _out.WriteLine($"merge refused a torn input; runs {runsBefore} → {e.IndexStatsForTest.Runs}, "
                     + $"coverage {coverageBefore} → {e.IndexCoverage}");
        Assert.Equal(coverageBefore, e.IndexCoverage);
        Assert.Equal(runsBefore, e.IndexStatsForTest.Runs);
        Assert.Empty(Directory.EnumerateFiles(dir, "tix-L*.tix"));
    }
}
