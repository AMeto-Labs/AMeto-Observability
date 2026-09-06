using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// v4: THE TRACE INDEX LEAVES THE SEGMENT, AND THE SEGMENT STAYS READABLE WITHOUT IT.
///
/// <para>The per-segment trace index was 38% of every <c>.trc</c> and it answered one question —
/// where in this file is trace X — that the global trace-id index now answers for one 4 KB read
/// instead of reading and inflating all of it, per segment, on every lookup. v4 stops writing
/// it.</para>
///
/// <para>That is a real bet, and the tests below are about the losing side of it. A v4 segment
/// whose index run is missing has no cheap way to find a trace, and if the answer in that state
/// were "not found" the format change would be a data-loss machine. It is not: the spans carry
/// their own trace ids, so the reader falls back to scanning, the backfill rebuilds the run from
/// the same scan, and the degraded state heals. Slow, correct, temporary — in that order of
/// importance.</para>
/// </summary>
public sealed class SpanFormatV4Tests : IDisposable
{
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-v4-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long              _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public SpanFormatV4Tests(ITestOutputHelper output)
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

    /// <summary>Realistic spans: a few per trace, with the attributes real segments carry.</summary>
    private List<SpanRecord> Corpus(int traces, int spansPer)
    {
        var list = new List<SpanRecord>(traces * spansPer);
        ulong span = 1;
        for (int t = 0; t < traces; t++)
            for (int k = 0; k < spansPer; k++)
                list.Add(new SpanRecord
                {
                    TraceId = Id(t), SpanId = new SpanId(span++), ParentSpanId = default,
                    StartTimeUnixNano = _baseNano + (t * 10 + k) * Ms, DurationNanos = 3 * Ms,
                    Name = "GET /api/orders/{id}", ServiceName = k % 2 == 0 ? "gateway" : "billing",
                    Kind = SpanKind.Server, Status = SpanStatusCode.Ok, HttpStatusCode = 200,
                    Attributes = new Dictionary<string, object?>(3, StringComparer.Ordinal)
                    {
                        ["http.method"] = "GET",
                        ["http.route"]  = "/api/orders/{id}",
                        ["db.system"]   = "mssql",
                    },
                });
        return list;
    }

    private static async Task<List<SpanRecord>> ReadTrace(string path, TraceId id)
    {
        var got = new List<SpanRecord>();
        await foreach (var s in SpanReader.ReadTraceAsync(path, id, default)) got.Add(s);
        return got;
    }

    [Fact]
    public void A_v4_segment_is_smaller_than_the_v3_of_the_same_spans()
    {
        // The claim the whole stage rests on, measured rather than asserted from the design.
        string dir = Dir("size");
        var corpus = Corpus(traces: 2_000, spansPer: 3);

        var v4 = SpanWriter.Write(dir, corpus, version: SpanWriter.NewestVersion);
        Assert.Equal(4, v4.FormatVersion);

        string v3Path = Path.Combine(dir, "legacy-v3.trc");
        OddGeometryV3Writer.Write(v3Path, corpus, blockSpans: 4096);

        long v4Bytes = new FileInfo(v4.FilePath).Length;
        long v3Bytes = new FileInfo(v3Path).Length;
        double saved = 1.0 - (double)v4Bytes / v3Bytes;

        _out.WriteLine($"{corpus.Count} spans / 2000 traces: v3 {v3Bytes:N0} B → v4 {v4Bytes:N0} B "
                     + $"({saved:P1} smaller)");

        Assert.True(v4Bytes < v3Bytes, "v4 is not smaller than v3");
        Assert.True(saved > 0.20,
            $"v4 saved only {saved:P1} — the trace index was supposed to be a third of the file");
    }

    [Fact]
    public async Task A_v4_segment_with_no_index_run_still_finds_its_traces()
    {
        // THE LOSING SIDE OF THE BET. No .tix, no per-segment index: if this came back empty the
        // format change would silently lose every trace in every segment whose run went missing.
        string dir = Dir("noindex");
        var corpus = Corpus(traces: 300, spansPer: 3);
        var info   = SpanWriter.Write(dir, corpus, version: SpanWriter.NewestVersion);

        // Written straight through SpanWriter, so no .tix exists at all — the state a crash between
        // the segment rename and the run write leaves behind.
        Assert.Empty(Directory.EnumerateFiles(dir, "*.tix"));

        foreach (int t in new[] { 0, 17, 299 })
        {
            var spans = await ReadTrace(info.FilePath, Id(t));
            Assert.Equal(3, spans.Count);
            Assert.All(spans, s => Assert.Equal(Id(t), s.TraceId));
        }

        // And a trace that is genuinely absent is still absent, not "everything".
        Assert.Empty(await ReadTrace(info.FilePath, Id(999_999)));
        _out.WriteLine("v4 with no run: every trace found by scanning, absent trace still absent");
    }

    [Fact]
    public async Task The_engine_heals_a_v4_segment_whose_run_was_deleted()
    {
        // The degraded state must be temporary. The backfill rebuilds the run from the same scan
        // the reader falls back to, so an install that loses its .tix files recovers by itself.
        string dir = Dir("heal");
        TraceId planted = Id(42);

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance,
                                            writeSegmentFormatV4: true);
        for (int t = 0; t < 200; t++)
            for (int k = 0; k < 3; k++)
                e.WriteSpan(new SpanIngestItem
                {
                    TraceId = Id(t), SpanId = new SpanId((ulong)(t * 3 + k + 1)), ParentSpanId = default,
                    StartTimeUnixNano = _baseNano + (t * 10 + k) * Ms, DurationNanos = 2 * Ms,
                    Name = "GET /orders", ServiceName = "billing",
                    Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
                });
        e.FlushHotTier();
        Assert.Equal((1, 1), e.CatalogCountsForTest);

        // Take the run away and the claim with it — the state after a lost or unreadable .tix.
        e.DisableTraceIndexForTest();
        foreach (var f in Directory.EnumerateFiles(dir, "*.tix").ToList()) File.Delete(f);
        Assert.Equal(0, e.IndexCoverage.Covered);

        // Still correct while degraded.
        var got = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(planted)) got.Add(s);
        Assert.Equal(3, got.Count);

        // And it heals.
        Assert.True(e.BackfillNextSegment());
        _out.WriteLine($"after backfill: coverage {e.IndexCoverage}, runs {e.IndexStatsForTest.Runs}");
        Assert.Equal((1, 1), e.IndexCoverage);
        Assert.Single(Directory.EnumerateFiles(dir, "*.tix"));

        got.Clear();
        await foreach (var s in e.GetTraceAsync(planted)) got.Add(s);
        Assert.Equal(3, got.Count);
        Assert.Equal(1, e.SegmentsOpenedByLastTraceLookup);
    }

    [Fact]
    public async Task A_v3_segment_still_reads_exactly_as_it_did()
    {
        // Every install this ships to is full of v3 files, and nothing rewrites them for this
        // change alone. They keep their own trace index and must keep working through it.
        string dir = Dir("v3");
        var corpus = Corpus(traces: 400, spansPer: 3);
        string v3Path = Path.Combine(dir, "legacy-v3.trc");
        OddGeometryV3Writer.Write(v3Path, corpus, blockSpans: 4096);

        var info = SpanReader.ReadSegmentInfo(v3Path);
        Assert.Equal(3, info.FormatVersion);

        foreach (int t in new[] { 0, 200, 399 })
        {
            var spans = await ReadTrace(v3Path, Id(t));
            Assert.Equal(3, spans.Count);
            Assert.All(spans, s => Assert.Equal(Id(t), s.TraceId));
        }

        // And its own index is what answered — a v3 file is never scanned for this.
        var map = SpanReader.ReadTraceIndexForTest(v3Path);
        Assert.NotNull(map);   // a healthy file accounts for every span, or it is not indexed at all
        _out.WriteLine($"v3 index read back: {map.Count} traces");
        Assert.Equal(400, map.Count);
    }

    [Fact]
    public void A_v4_segment_reports_its_traces_by_scanning_when_asked_for_the_whole_index()
    {
        // What the backfill calls. For v3 it reads the index block; for v4 there is none, so it
        // walks the spans — and the two must agree, because the run built from either has to point
        // at the same offsets.
        string dir = Dir("wholeindex");
        var corpus = Corpus(traces: 500, spansPer: 3);

        var v4 = SpanWriter.Write(dir, corpus, version: SpanWriter.NewestVersion);
        string v3Path = Path.Combine(dir, "legacy-v3.trc");
        OddGeometryV3Writer.Write(v3Path, corpus, blockSpans: 4096);

        var fromScan  = SpanReader.ReadTraceIndexForTest(v4.FilePath);
        var fromIndex = SpanReader.ReadTraceIndexForTest(v3Path);
        Assert.NotNull(fromScan);
        Assert.NotNull(fromIndex);

        _out.WriteLine($"v4 by scan: {fromScan.Count} traces; v3 by index: {fromIndex.Count}");
        Assert.Equal(fromIndex.Count, fromScan.Count);

        foreach (var (id, offsets) in fromIndex)
        {
            Assert.True(fromScan.TryGetValue(id, out var scanned), $"scan lost trace {id}");
            Assert.Equal(offsets.Order(), scanned!.Order());
        }
    }
}
