using System.Buffers.Binary;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE INDEX MAY ONLY VOUCH FOR A SEGMENT IT FULLY ACCOUNTED FOR — the second review round on #69.
///
/// <para>Every walk over a <c>.trc</c> ends at <c>traceIdxOffset</c>, read from the footer. A
/// footer torn by a crash between the block writes and the final fsync can hold a value that is
/// merely SMALLER than the truth: it still lands inside the file, the byte it points at still
/// decodes, and the walk simply stops early. Nothing throws, and the map that comes back is
/// well-formed and short. The backfill would turn that into a run and claim COVERAGE — permission
/// to skip the segment.</para>
///
/// <para>WHAT THAT DOES AND DOES NOT COST TODAY, measured rather than assumed, because the finding
/// as filed overstates it. The uncovered fallback is <c>EnumerateTraceIdsInOrder</c>, which stops
/// at the SAME boundary, so the tail past a torn offset is invisible to the index and to the scan
/// alike: claiming coverage does not change any answer. What the guard buys is the invariant
/// itself. Coverage is the one thing in this design that turns "I did not find it" into "it is not
/// there", and a claim built on a read that could not account for every span is a claim resting on
/// something nobody checked — true today only by a coincidence of two code paths sharing one
/// boundary, and false the moment either grows a second way to end early.</para>
///
/// <para>The guard is a count, not a parse: exactly one index offset exists per span, and the
/// header records the span count at the FRONT of the file. It lives in <c>ReadTraceIndex</c> and
/// deliberately NOT in <c>ReadFooter</c> — throwing there reaches the cold-tier scan, which reads a
/// throwing footer as corruption and DELETES the segment with its sidecars. Losing the index costs
/// speed; losing the segment costs the spans.</para>
/// </summary>
public sealed class TruncatedFooterCoverageTests : IDisposable
{
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-torn-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long              _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public TruncatedFooterCoverageTests(ITestOutputHelper output)
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

    private static TraceId Id(int i) => new(
        unchecked((ulong)(i * 6364136223846793005L + 1442695040888963407L)),
        unchecked((ulong)(i * 2862933555777941757L + 3037000493L)));

    /// <summary>Three blocks' worth, so a truncation can land on a block boundary.</summary>
    private List<SpanRecord> Corpus(int traces, int spansPer) =>
        [.. Enumerable.Range(0, traces).SelectMany(t => Enumerable.Range(0, spansPer).Select(k =>
            new SpanRecord
            {
                TraceId = Id(t), SpanId = new SpanId((ulong)(t * spansPer + k + 1)), ParentSpanId = default,
                StartTimeUnixNano = _baseNano + (t * 10 + k) * Ms, DurationNanos = 3 * Ms,
                Name = "GET /api/orders/{id}", ServiceName = "gateway",
                Kind = SpanKind.Server, Status = SpanStatusCode.Ok, HttpStatusCode = 200,
            }))];

    /// <summary>
    /// Rewinds the footer's traceIdxOffset to the end of the FIRST block — what a torn 8-byte
    /// write can leave behind, and the only damaged form that decodes cleanly instead of throwing.
    /// </summary>
    private static long TruncateTraceIdxOffset(string trcPath)
    {
        byte[] raw = File.ReadAllBytes(trcPath);

        // Header is 27 bytes; a block is [uncompSize:4][compSize:4][payload].
        uint firstComp = BinaryPrimitives.ReadUInt32LittleEndian(raw.AsSpan(27 + 4));
        long shortened = 27 + 8 + firstComp;

        int at = raw.Length - 28;                 // v3+ footer: traceIdx, svcIdx, bloomIdx, magic
        BinaryPrimitives.WriteUInt64LittleEndian(raw.AsSpan(at), (ulong)shortened);
        File.WriteAllBytes(trcPath, raw);
        return shortened;
    }

    /// <summary>A v4 segment: no index block of its own, so the whole index comes from the scan.</summary>
    private string WriteV4(string dir, List<SpanRecord> corpus)
    {
        var info = SpanWriter.Write(dir, corpus, version: SpanWriter.NewestVersion);
        return info.FilePath;
    }

    [Fact]
    public void A_v4_footer_pointing_short_of_the_last_block_yields_no_index_at_all()
    {
        // THE SILENT CASE, and the only one. v4 has no index block, so the map is built by walking
        // the spans — and that walk ends at traceIdxOffset. A torn-short offset therefore produces
        // a perfectly well-formed index over the first block and nothing else, with no exception
        // anywhere. The count is what notices.
        string dir    = Dir("v4");
        var    corpus = Corpus(traces: 4_000, spansPer: 3);   // 12 000 spans → three 4096 blocks
        string path   = WriteV4(dir, corpus);

        var whole = SpanReader.ReadTraceIndexForTest(path);
        Assert.NotNull(whole);
        Assert.Equal(4_000, whole.Count);

        long shortened = TruncateTraceIdxOffset(path);
        _out.WriteLine($"traceIdxOffset rewound to {shortened} of {new FileInfo(path).Length} bytes");

        var torn = SpanReader.ReadTraceIndexForTest(path);
        Assert.Null(torn);
    }

    [Fact]
    public void A_v3_torn_offset_is_refused_too_by_throwing_rather_than_by_counting()
    {
        // The other half of the shape, recorded because it is the reason the v3 path was never the
        // dangerous one. v3 keeps its own index block AT traceIdxOffset, so a torn offset seeks
        // into the middle of a span block and reads a compressed payload as an entry count —
        // caught by the bounds guard long before it could become a short index. Either way no
        // coverage is claimed; only the route there differs.
        string dir    = Dir("v3");
        var    corpus = Corpus(traces: 4_000, spansPer: 3);
        string path   = Path.Combine(dir, "seg.trc");
        OddGeometryV3Writer.Write(path, corpus, blockSpans: 4096);

        Assert.NotNull(SpanReader.ReadTraceIndexForTest(path));
        TruncateTraceIdxOffset(path);

        var ex = Record.Exception(() => SpanReader.ReadTraceIndexForTest(path));
        _out.WriteLine($"v3 torn offset: {ex?.GetType().Name} — {ex?.Message[..Math.Min(90, ex.Message.Length)]}");
        Assert.IsType<InvalidDataException>(ex);
    }

    [Fact]
    public async Task The_backfill_claims_nothing_for_a_segment_it_could_not_account_for()
    {
        // THE FINDING, END TO END — and the honest version of its consequence. The backfill looks
        // at the segment, cannot account for 8 000 of its 12 000 spans, and claims nothing: no
        // coverage, no run, no .tix.
        string dir    = Dir("engine");
        var    corpus = Corpus(traces: 4_000, spansPer: 3);
        string path   = WriteV4(dir, corpus);
        string named  = Path.Combine(dir, "spans-20260801-120000.trc");
        File.Move(path, named);

        long shortened = TruncateTraceIdxOffset(named);
        Assert.True(shortened < new FileInfo(named).Length);

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e.LoadColdSegments();
        Assert.Equal(1, e.CatalogCountsForTest.Segments);

        Assert.True(e.BackfillNextSegment());                  // it did look, and it decided
        _out.WriteLine($"after backfill: coverage {e.IndexCoverage}, runs {e.IndexStatsForTest.Runs}");
        Assert.Equal(0, e.IndexCoverage.Covered);
        Assert.Equal(0, e.IndexStatsForTest.Runs);
        Assert.Empty(Directory.EnumerateFiles(dir, "*.tix"));
        Assert.False(e.BackfillNextSegment());                 // and does not churn on it

        // Traces in the first block are still served. Ones past the torn offset are not — by the
        // scan either, which is the measurement that keeps this test honest about what the guard
        // is for. The damage is the footer's; refusing coverage is refusing to make it permanent
        // in a .tix that would outlive it.
        var early = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(Id(0))) early.Add(s);
        Assert.Equal(3, early.Count);

        var late = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(Id(3_999))) late.Add(s);
        _out.WriteLine($"past the torn offset: {late.Count} span(s) — the scan cannot see them either");
        Assert.Empty(late);
    }

    [Fact]
    public void A_healthy_segment_is_still_backfilled()
    {
        // The guard must not be a blanket refusal: the same corpus, untouched, indexes normally.
        string dir    = Dir("healthy");
        var    corpus = Corpus(traces: 4_000, spansPer: 3);
        string path   = WriteV4(dir, corpus);
        File.Move(path, Path.Combine(dir, "spans-20260801-120000.trc"));

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e.LoadColdSegments();

        Assert.True(e.BackfillNextSegment());
        _out.WriteLine($"healthy segment: coverage {e.IndexCoverage}");
        Assert.Equal(1, e.IndexCoverage.Covered);
        Assert.Single(Directory.EnumerateFiles(dir, "*.tix"));
    }

    [Fact]
    public void A_torn_footer_does_not_cost_the_segment_at_startup()
    {
        // WHY THE CHECK IS NOT IN ReadFooter. The cold scan reads a throwing footer as corruption
        // and deletes the .trc with its sidecars, so a guard that threw there would turn a lost
        // index into lost spans — the trade this whole branch exists to refuse.
        string dir    = Dir("startup");
        var    corpus = Corpus(traces: 4_000, spansPer: 3);
        string path   = WriteV4(dir, corpus);
        string named  = Path.Combine(dir, "spans-20260801-120000.trc");
        File.Move(path, named);
        TruncateTraceIdxOffset(named);

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e.LoadColdSegments();

        _out.WriteLine($"segment still present after the cold scan: {File.Exists(named)}");
        Assert.True(File.Exists(named));
        Assert.Equal(1, e.CatalogCountsForTest.Segments);
    }
}
