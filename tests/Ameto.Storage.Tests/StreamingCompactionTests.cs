using System.Buffers;
using Ameto.Core;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE STREAMING MERGE (<c>SpanWriter.WriteMerged</c>, <c>TraceStorageEngine.CompactStreamingPass</c>).
///
/// <para>A materialising pass held every span it merged — ~607 B each — so on the 512 MB stand, whose
/// pass budget is 24 MB, a full flush (~22 MB read back) was never a candidate and a production install
/// sat at 2.6k flush-sized segments. The streaming merge holds a block per source it is reading. These
/// facts pin that it writes the same spans, that its sidecars answer what the sources answered, that it
/// holds blocks and not its output, how it plans, and that a source it cannot read is treated the way
/// the materialising loader treats one.</para>
/// </summary>
public sealed class StreamingCompactionTests : IDisposable
{
    private const long MB = 1024 * 1024;
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly string[] Services = ["gateway", "billing", "ledger", "auth"];

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-tstream-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long              _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public StreamingCompactionTests(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_root);
    }

    public void Dispose()
    {
        SpanWriter._afterMergedBlockForTest = null;
        try { Directory.Delete(_root, true); } catch { }
    }

    private string Dir(string name)
    {
        string d = Path.Combine(_root, name);
        Directory.CreateDirectory(d);
        return d;
    }

    private static MemoryBudgets Stand => MemoryBudgets.Derive(managedLimitBytes: 384 * MB, physicalLimitBytes: 512 * MB);

    // ── The corpus ──────────────────────────────────────────────────────────

    /// <summary>
    /// <paramref name="segments"/> flush-shaped sources, each <paramref name="traces"/> five-span traces:
    /// a root in <c>gateway</c> and four children in the other services, one in seven an error. The LAST
    /// trace of every source but the last has its final child in the NEXT source — a trace that
    /// straddled a flush, the case no source's own sidecars can see whole.
    /// </summary>
    private List<SpanRecord> WriteSources(string dir, int segments, int traces, out List<SpanSegmentInfo> written,
                                          int blobBytes = 0, ushort version = SpanWriter.DefaultVersion)
    {
        var all     = new List<SpanRecord>();
        var batches = new List<List<SpanRecord>>();
        for (int s = 0; s < segments; s++) batches.Add([]);

        long t = _baseNano;
        ulong spanId = 1;
        for (int s = 0; s < segments; s++)
        {
            for (int tr = 0; tr < traces; tr++)
            {
                var  traceId = new TraceId(0x5EED0000UL + (ulong)s, (ulong)tr + 1);
                var  rootId  = new SpanId(spanId++);
                bool error   = (s * traces + tr) % 7 == 0;
                for (int k = 0; k < 5; k++)
                {
                    var rec = new SpanRecord
                    {
                        TraceId           = traceId,
                        SpanId            = k == 0 ? rootId : new SpanId(spanId++),
                        ParentSpanId      = k == 0 ? default : rootId,
                        StartTimeUnixNano = t + k * Ms,
                        DurationNanos     = (k + 1) * 3 * Ms,
                        Name              = k == 0 ? "POST /api/pay" : "SELECT payments",
                        ServiceName       = k == 0 ? "gateway" : Services[1 + (k - 1) % (Services.Length - 1)],
                        Kind              = k == 0 ? SpanKind.Server : SpanKind.Client,
                        Status            = error && k == 4 ? SpanStatusCode.Error : SpanStatusCode.Ok,
                        HttpStatusCode    = (short)(k == 0 ? 200 : 0),
                        AttributesBytes   = Blob(k == 0, blobBytes),
                    };
                    bool straddles = tr == traces - 1 && k == 4 && s < segments - 1;
                    (straddles ? batches[s + 1] : batches[s]).Add(rec);
                    all.Add(rec);
                }
                t += 10 * Ms;
            }
        }

        written = [];
        foreach (var b in batches) written.Add(SpanWriter.Write(dir, b, version: version));
        return all;
    }

    /// <summary>A realistic attribute map: the HTTP keys on a root, a database client's on a child.</summary>
    private static byte[] Blob(bool root, int padTo)
    {
        var buf = new ArrayBufferWriter<byte>(256);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(root ? 4 : 5);
        if (root)
        {
            w.Write("http.request.method"); w.Write("POST");
            w.Write("url.path");            w.Write("/api/pay");
            w.Write("http.response.status_code"); w.Write(200L);
            w.Write("pad");                 w.Write(new string('p', Math.Max(0, padTo - 80)));
        }
        else
        {
            w.Write("db.system");     w.Write("mssql");
            w.Write("db.name");       w.Write("payments");
            w.Write("net.peer.name"); w.Write("10.220.0.17");
            w.Write("net.peer.port"); w.Write(1433L);
            w.Write("pad");           w.Write(new string('q', Math.Max(0, padTo - 80)));
        }
        w.Flush();
        return buf.WrittenMemory.ToArray();
    }

    private TraceStorageEngine Engine(string dir, TracesOptions? options = null) =>
        new(dir, NullLogger<TraceStorageEngine>.Instance,
            options: options ?? new TracesOptions { CompactionMinAge = TimeSpan.Zero });

    private static (string, ulong) Key(SpanRecord s) => (s.TraceId.ToString(), s.SpanId.RawValue);

    // ── What the merge writes ───────────────────────────────────────────────

    [Fact]
    public void The_merged_segment_holds_exactly_the_spans_of_its_sources_in_start_order()
    {
        string dir = Dir("spans");
        var all = WriteSources(dir, segments: 5, traces: 900, out _, blobBytes: 200);

        using var e = Engine(dir);
        e.LoadColdSegments();
        Assert.Equal(5, e.ColdSegmentCountForTest);
        e.CompactSmallSegments();

        var merged = Assert.Single(e.ColdSegmentsForTest);
        var back   = SpanReader.ReadAll(merged.FilePath);
        _out.WriteLine($"5 sources, {all.Count:N0} spans → 1 segment of {back.Count:N0}, {merged.FilePath}");

        Assert.Equal(all.Count, back.Count);
        Assert.Equal(all.Count, merged.SpanCount);
        for (int i = 1; i < back.Count; i++)
            Assert.True(back[i].StartTimeUnixNano >= back[i - 1].StartTimeUnixNano, $"span {i} is out of start order");

        var byKey = all.ToDictionary(Key);
        foreach (var r in back)
        {
            var src = byKey[Key(r)];
            Assert.Equal(src.ParentSpanId, r.ParentSpanId);
            Assert.Equal(src.StartTimeUnixNano, r.StartTimeUnixNano);
            Assert.Equal(src.DurationNanos, r.DurationNanos);
            Assert.Equal(src.Name, r.Name);
            Assert.Equal(src.ServiceName, r.ServiceName);
            Assert.Equal(src.Kind, r.Kind);
            Assert.Equal(src.Status, r.Status);
            Assert.Equal(src.HttpStatusCode, r.HttpStatusCode);
            Assert.True(src.AttributesBytes.Span.SequenceEqual(r.AttributesBytes.Span), "an attribute blob changed in the merge");
        }

        Assert.Equal(all.Min(s => s.StartTimeUnixNano), merged.MinStartNano);
        Assert.Equal(all.Max(s => s.StartTimeUnixNano), merged.MaxStartNano);
        Assert.Equal(Services.Order(), merged.Services.Order());
        Assert.True(merged.WeightBytes > 0, "the merged segment was not weighed");
        Assert.Equal(3, merged.FormatVersion);
    }

    [Fact]
    public void The_merged_sidecars_answer_what_the_spans_answer()
    {
        string dir = Dir("sidecars");
        var all = WriteSources(dir, segments: 4, traces: 700, out var sources);

        // What a flush of the WHOLE set would have said — the reference the sidecars are held to.
        var sourceEdges = sources.Sum(s => ServiceGraphSidecar.ReadEdges(s.FilePath).Sum(e => (long)e.CallCount));

        using var e = Engine(dir);
        e.LoadColdSegments();
        e.CompactSmallSegments();
        var merged = Assert.Single(e.ColdSegmentsForTest);

        // .stats — from the spans, exactly.
        var stats = SpanReader.ReadStats(merged.FilePath).ToDictionary(s => s.ServiceName);
        foreach (var g in all.GroupBy(s => s.ServiceName))
        {
            var st = stats[g.Key];
            Assert.Equal((uint)g.Count(), st.SpanCount);
            Assert.Equal((uint)g.Count(s => s.Status == SpanStatusCode.Error), st.ErrorCount);
            Assert.Equal(g.Min(s => s.DurationNanos), st.MinDurationNanos);
            Assert.Equal(g.Max(s => s.DurationNanos), st.MaxDurationNanos);
            var buckets = new uint[HistogramBuckets.Count];
            foreach (var s in g) buckets[HistogramBuckets.IndexOf(s.DurationNanos)]++;
            Assert.Equal(buckets, st.Buckets);
        }

        // .tracesum — one row per trace here (every trace sits well inside the window), read the
        // way the cold walks read rows: merged by trace id.
        var rows = TraceSummarySidecar.ReadSummaries(merged.FilePath);
        var traces = all.GroupBy(s => s.TraceId).ToDictionary(g => g.Key, g => g.ToList());
        Assert.Equal(traces.Count, rows.Count);
        foreach (var r in rows)
        {
            var spans = traces[r.TraceId];
            var root  = spans.Single(s => s.ParentSpanId.IsEmpty);
            Assert.Equal((uint)spans.Count, r.SpanCount);
            Assert.Equal(spans.Any(s => s.Status == SpanStatusCode.Error), r.HasError);
            Assert.True(r.HasRoot);
            Assert.Equal(root.SpanId, r.RootSpanId);
            Assert.Equal(root.StartTimeUnixNano, r.RootStartNano);
            Assert.Equal(root.DurationNanos, r.DurationNanos);
            Assert.Equal("gateway", r.ServiceName);
            Assert.Equal("POST", r.HttpMethod);
            Assert.Equal("/api/pay", r.HttpPath);
            Assert.Equal(spans.Select(s => s.ServiceName).Distinct().Order(), r.Services.Order());
        }
        var volume = TraceSummarySidecar.ReadVolume(merged.FilePath)!;
        Assert.Equal(traces.Count, volume.Buckets.Sum(b => (long)b.TraceCount));
        Assert.Equal(traces.Values.Count(t => t.Any(s => s.Status == SpanStatusCode.Error)),
                     volume.Buckets.Sum(b => (long)b.ErrorCount));

        // .svcgraph — every edge over the whole set: the sources' own, plus the three a straddling
        // trace's last child draws across a source boundary, which no source could see.
        var byId = all.ToDictionary(s => s.SpanId);
        var expected = all.Where(s => !s.ParentSpanId.IsEmpty && byId.ContainsKey(s.ParentSpanId)
                                   && byId[s.ParentSpanId].ServiceName != s.ServiceName)
                          .GroupBy(s => (byId[s.ParentSpanId].ServiceName, s.ServiceName))
                          .ToDictionary(g => g.Key, g => (Calls: (uint)g.Count(), Errors: (uint)g.Count(s => s.Status == SpanStatusCode.Error)));
        var edges = ServiceGraphSidecar.ReadEdges(merged.FilePath).ToDictionary(x => (x.From, x.To));
        Assert.Equal(expected.Count, edges.Count);
        foreach (var (k, v) in expected)
        {
            Assert.Equal(v.Calls,  edges[k].CallCount);
            Assert.Equal(v.Errors, edges[k].ErrorCount);
        }
        long mergedCalls = edges.Values.Sum(x => (long)x.CallCount);
        _out.WriteLine($"edges: sources {sourceEdges}, merged {mergedCalls} (+{mergedCalls - sourceEdges} across sources); "
                     + $"rows {rows.Count}; last merge {e.LastStreamedMergeForTest}");
        Assert.Equal(3, mergedCalls - sourceEdges);
        Assert.Equal(3, e.LastStreamedMergeForTest!.Value.CrossSourceCalls);
    }

    [Fact]
    public void The_merged_v3_trace_index_is_the_one_its_spans_give()
    {
        string dir = Dir("tix");
        WriteSources(dir, segments: 3, traces: 1_200, out _);

        using var e = Engine(dir);
        e.LoadColdSegments();
        e.CompactSmallSegments();
        var merged = Assert.Single(e.ColdSegmentsForTest);

        var spans = SpanReader.ReadAll(merged.FilePath);
        var fromSpans = new Dictionary<TraceId, List<uint>>();
        for (int i = 0; i < spans.Count; i++)
        {
            if (!fromSpans.TryGetValue(spans[i].TraceId, out var l)) fromSpans[spans[i].TraceId] = l = [];
            l.Add((uint)i);
        }
        var inFile = SpanReader.ReadTraceIndexForTest(merged.FilePath)!;
        Assert.Equal(fromSpans.Count, inFile.Count);
        foreach (var (id, offsets) in fromSpans) Assert.Equal(offsets, inFile[id]);
    }

    [Fact]
    public async Task A_trace_split_across_sources_reads_back_whole_and_lists_once()
    {
        string dir = Dir("lookup");
        var all = WriteSources(dir, segments: 3, traces: 50, out _);
        var split = new TraceId(0x5EED0000UL, 50);   // source 0's last trace: its last child is in source 1
        Assert.Equal(5, all.Count(s => s.TraceId.Equals(split)));

        using var e = Engine(dir);
        e.LoadColdSegments();
        e.CompactSmallSegments();
        Assert.Equal(1, e.ColdSegmentCountForTest);

        var spans = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(split)) spans.Add(s);
        Assert.Equal(5, spans.Count);

        var page = await e.GetTraceListAsync(Base.AddMinutes(-1), Base.AddHours(1), null, null, null, null, null, 1_000);
        var row  = Assert.Single(page.Rows, r => r.TraceId.Equals(split));
        Assert.Equal(5u, row.SpanCount);
        Assert.Equal(150, page.Rows.Count);
    }

    // ── How a merged segment is read ────────────────────────────────────────

    private static async Task<List<SpanRecord>> Search(string path, long from, long to, List<uint>? decoded = null)
    {
        SpanReader._searchBlockDecodedForTest = decoded is null ? null : b => decoded.Add(b);
        try
        {
            var got = new List<SpanRecord>();
            await foreach (var s in SpanReader.SearchAsync(path, from, to, null, null, null, null, null, null, null, CancellationToken.None))
                got.Add(s);
            return got;
        }
        finally { SpanReader._searchBlockDecodedForTest = null; }
    }

    /// <summary>
    /// A MERGED SEGMENT IS BIG, AND A WINDOW INSIDE IT NEED NOT READ ALL OF IT. The merge vouches for
    /// its order (the header flag), so a search seeks to the block its window starts in and stops after
    /// the one it ends in — and still finds every span a full read finds, at every window, including
    /// windows that begin or end exactly on a span, a block's first span, or the file's edges.
    /// </summary>
    [Fact]
    public async Task A_window_inside_a_merged_segment_reads_only_its_blocks_and_finds_every_span()
    {
        string dir = Dir("window");
        WriteSources(dir, segments: 6, traces: 2_000, out _);
        using var e = Engine(dir);
        e.LoadColdSegments();
        e.CompactSmallSegments();
        var merged = Assert.Single(e.ColdSegmentsForTest);
        var all    = SpanReader.ReadAll(merged.FilePath);
        int blocks = (all.Count + 4095) / 4096;

        var rng = new Random(20261009);
        var windows = new List<(long From, long To)>
        {
            (merged.MinStartNano, merged.MaxStartNano),
            (merged.MinStartNano - 1, merged.MinStartNano),
            (merged.MaxStartNano, merged.MaxStartNano + 1),
            (all[4096].StartTimeUnixNano, all[4096].StartTimeUnixNano),        // exactly a block's first span
            (all[4095].StartTimeUnixNano, all[8192].StartTimeUnixNano),        // a block's last to the next-but-one's first
        };
        for (int w = 0; w < 30; w++)
        {
            long a = all[rng.Next(all.Count)].StartTimeUnixNano + rng.Next(-2, 3);
            long b = a + rng.Next(0, 2_000) * Ms;
            windows.Add((a, b));
        }

        foreach (var (from, to) in windows)
        {
            var expected = all.Where(s => s.StartTimeUnixNano >= from && s.StartTimeUnixNano <= to).Select(Key).ToList();
            var got      = (await Search(merged.FilePath, from, to)).Select(Key).ToList();
            Assert.Equal(expected, got);
        }

        // And it reads a block or two of the file for a narrow window in the middle, not all of it.
        var decoded = new List<uint>();
        long mid = all[all.Count / 2].StartTimeUnixNano;
        var  hit = await Search(merged.FilePath, mid, mid + 50 * Ms, decoded);
        _out.WriteLine($"{blocks} blocks; a 50 ms window in the middle decoded {decoded.Count}: [{string.Join(", ", decoded)}], {hit.Count} spans");
        Assert.NotEmpty(hit);
        Assert.InRange(decoded.Count, 1, 2);
    }

    /// <summary>
    /// TIES ACROSS A BLOCK BOUNDARY. Spans with one start time can end one block and begin the next,
    /// so the block a window must start in is the last one that starts STRICTLY before it.
    /// </summary>
    [Fact]
    public async Task Spans_tied_across_a_block_boundary_are_all_found()
    {
        string dir = Dir("ties");
        long t0 = _baseNano;
        for (int s = 0; s < 2; s++)
        {
            var spans = new List<SpanRecord>();
            for (int i = 0; i < 6_000; i++)
                spans.Add(new SpanRecord
                {
                    TraceId = new TraceId(0x7135UL, (ulong)(s * 10_000 + i + 1)), SpanId = new SpanId((ulong)(s * 10_000 + i + 1)),
                    StartTimeUnixNano = t0 + (s * 6_000 + i) / 1_000 * Ms,     // runs of 1 000 equal starts
                    DurationNanos = Ms, Name = "op", ServiceName = "billing", Kind = SpanKind.Server,
                });
            SpanWriter.Write(dir, spans);
        }
        using var e = Engine(dir);
        e.LoadColdSegments();
        e.CompactSmallSegments();
        var merged = Assert.Single(e.ColdSegmentsForTest);

        for (int k = 0; k < 12; k++)
        {
            long at = t0 + k * Ms;
            Assert.Equal(1_000, (await Search(merged.FilePath, at, at)).Count);
        }
    }

    /// <summary>A segment the merge did not write is read whole, as it always was — nothing vouches for its order.</summary>
    [Fact]
    public async Task A_flushed_segment_is_still_read_whole()
    {
        string dir = Dir("flush-whole");
        WriteSources(dir, segments: 1, traces: 2_000, out var sources);
        var all = SpanReader.ReadAll(sources[0].FilePath);
        long mid = all[all.Count / 2].StartTimeUnixNano;

        var decoded = new List<uint>();
        await Search(sources[0].FilePath, mid, mid + 50 * Ms, decoded);
        Assert.Equal((all.Count + 4095) / 4096, decoded.Count);
    }

    // ── What the merge holds ────────────────────────────────────────────────

    /// <summary>
    /// THE POINT OF THE WHOLE CHANGE, as a number. Eight sources of 20 000 ordinary spans: a
    /// materialising pass would hold all 160 000 read back — ~97 MB at 607 B a span. The streaming
    /// merge's live set, sampled after every block it writes, stays a few blocks deep.
    /// </summary>
    [Fact]
    public void A_streamed_merge_holds_a_few_blocks_not_its_output()
    {
        const long Budget = 32 * MB;   // the stand's 24 MB, with room for 160 000 spans in ONE output
        string dir = Dir("memory");
        WriteSources(dir, segments: 8, traces: 4_000, out _, blobBytes: 300);

        using var e = Engine(dir, new TracesOptions { CompactionMinAge = TimeSpan.Zero, MergeBudgetBytes = Budget });
        e.LoadColdSegments();
        Assert.Equal(8, e.ColdSegmentCountForTest);

        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        long baseline = GC.GetTotalMemory(forceFullCollection: true);
        long peak = 0;
        int  samples = 0;
        SpanWriter._afterMergedBlockForTest = written =>
        {
            if (written % (4 * 4096) != 0) return;               // every fourth block
            peak = Math.Max(peak, GC.GetTotalMemory(forceFullCollection: true) - baseline);
            samples++;
        };
        try { e.CompactSmallSegments(); }
        finally { SpanWriter._afterMergedBlockForTest = null; }

        var merged = Assert.Single(e.ColdSegmentsForTest);
        long materialised = merged.WeightBytes;
        _out.WriteLine($"STREAMED MERGE  8 sources → {merged.SpanCount:N0} spans; live set peak {peak / (double)MB:F1} MB "
                     + $"over {samples} samples; a materialising pass would hold {materialised / (double)MB:F1} MB; "
                     + $"read {e.LastStreamedMergeForTest!.Value.MaxOpenSources} source(s) at once");

        Assert.Equal(160_000, merged.SpanCount);
        Assert.True(samples >= 5, $"only {samples} samples were taken");
        Assert.True(peak < Budget, $"the merge held {peak / (double)MB:F1} MB live, past its {Budget / (double)MB:F0} MB budget — it is materialising its output again");
        Assert.True(peak * 3 < materialised, $"the merge held {peak:N0} B against the {materialised:N0} B its spans weigh");
    }

    // ── How it plans ────────────────────────────────────────────────────────

    private static SpanSegmentInfo Seg(int spans, long minNano, long maxNano, int id = 0) => new()
    {
        FilePath      = $"spans-{minNano}-{maxNano}-{spans}-{id:x8}.trc",
        MinStartNano  = minNano,
        MaxStartNano  = maxNano,
        SpanCount     = spans,
        Services      = ["billing"],
        FormatVersion = 3,
    };

    /// <summary>
    /// THE STAND, PLANNED. A full stand tier is ~37 600 ordinary spans, which the materialising planner
    /// never took (a pair needs 46 MB of a 24 MB pass). The streaming planner takes a run of them —
    /// several flushes into one segment — inside the same budget.
    /// </summary>
    [Fact]
    public void On_the_stand_budget_full_flushes_are_merged_several_at_a_time()
    {
        long budget = new TracesOptions().MergeBudgetBytesFor(Stand);
        long hour   = 3_600_000L * Ms;
        var flushes = Enumerable.Range(0, 30)
            .Select(i => Seg(37_600, _baseNano + i * 4 * 60_000L * Ms, _baseNano + (i * 4 + 4) * 60_000L * Ms - 1, i + 1))
            .ToArray();
        long now = _baseNano + 2 * 24 * hour;

        Assert.Empty(TraceStorageEngine.SelectCompactionBatch(flushes, budget));      // the old planner: nothing

        var batch = TraceStorageEngine.SelectStreamingBatch(flushes, budget, now, minAgeNanos: hour,
                                                             rowsOf: s => s.SpanCount / 10);
        _out.WriteLine($"stand pass {budget / (double)MB:F1} MB: {batch.Count} full flushes per merge "
                     + $"({batch.Sum(s => (long)s.SpanCount):N0} spans) — 30 flushes become ~{(30 + batch.Count - 1) / batch.Count}");
        Assert.True(batch.Count >= 3, $"only {batch.Count} full flushes fit one streamed merge on the stand");
        Assert.Equal(flushes.Take(batch.Count), batch);                                // oldest first, consecutive

        // The output is settled: more than half a pass by itself, so it is never merged again.
        var output = Seg(batch.Sum(s => s.SpanCount), batch[0].MinStartNano, batch[^1].MaxStartNano, 99);
        Assert.Empty(TraceStorageEngine.SelectStreamingBatch([output, output], budget, now, hour,
                                                             rowsOf: s => s.SpanCount / 10));
    }

    /// <summary>
    /// WHAT RAISING THE BUDGET BUYS — the figures CONFIGURATION.md quotes: ~4 full stand flushes per
    /// merged segment at the derived 24 MB, ~14 at 48 MB, ~20 at 64 MB, for flushes that follow one
    /// another in time and carry ten spans a trace.
    /// </summary>
    [Theory]
    [InlineData(0L,          4)]
    [InlineData(48_000_000L, 14)]
    [InlineData(64_000_000L, 20)]
    public void A_larger_budget_buys_larger_merged_segments(long budget, int flushesPerMerge)
    {
        if (budget == 0) budget = new TracesOptions().MergeBudgetBytesFor(Stand);
        long hour = 3_600_000L * Ms;
        var flushes = Enumerable.Range(0, 40)
            .Select(i => Seg(37_631, _baseNano + i * 4 * 60_000L * Ms, _baseNano + (i * 4 + 4) * 60_000L * Ms - 1, i + 1))
            .ToArray();
        var batch = TraceStorageEngine.SelectStreamingBatch(flushes, budget, _baseNano + 48 * hour, hour,
                                                             rowsOf: s => s.SpanCount / 10);
        _out.WriteLine($"{budget / 1e6:F1} MB pass: {batch.Count} flushes, {batch.Sum(s => (long)s.SpanCount):N0} spans");
        Assert.Equal(flushesPerMerge, batch.Count);
    }

    [Fact]
    public void A_segment_younger_than_the_minimum_age_waits()
    {
        long hour = 3_600_000L * Ms;
        long now  = _baseNano + 10 * hour;
        var young = new[] { Seg(1_000, now - 50 * 60_000L * Ms, now - 40 * 60_000L * Ms, 1),
                            Seg(1_000, now - 39 * 60_000L * Ms, now - 30 * 60_000L * Ms, 2) };
        var old   = new[] { Seg(1_000, now - 3 * hour, now - 2 * hour, 3), Seg(1_000, now - 2 * hour + 1, now - 90 * 60_000L * Ms, 4) };

        Assert.Empty(TraceStorageEngine.SelectStreamingBatch(young, 64 * MB, now, hour));
        Assert.Equal(2, TraceStorageEngine.SelectStreamingBatch([.. young, .. old], 64 * MB, now, hour).Count);
        Assert.Equal(2, TraceStorageEngine.SelectStreamingBatch(young, 64 * MB, now, minAgeNanos: 0).Count);
    }

    [Fact]
    public void A_batch_keeps_to_its_window_its_span_cap_and_its_format()
    {
        long hour = 3_600_000L * Ms;
        long now  = _baseNano + 100 * hour;

        // Twenty-four hours from the oldest start to the newest span, and not a nanosecond more.
        var spread = Enumerable.Range(0, 40).Select(i => Seg(500, _baseNano + i * hour, _baseNano + i * hour + 1, i + 1)).ToArray();
        var day = TraceStorageEngine.SelectStreamingBatch(spread, 64 * MB, now, hour);
        Assert.Equal(24, day.Count);   // +0 h … +23 h: the one starting at +24 h ends a nanosecond past the day

        // A million spans at most per output.
        var big = Enumerable.Range(0, 10).Select(i => Seg(300_000, _baseNano + i * 60_000L * Ms, _baseNano + (i + 1) * 60_000L * Ms - 1, i + 1)).ToArray();
        var capped = TraceStorageEngine.SelectStreamingBatch(big, 4_096 * MB, now, hour, rowsOf: static _ => 0);
        Assert.True(capped.Sum(s => (long)s.SpanCount) <= TraceStorageEngine.MaxSpansPerStreamedSegment);
        Assert.Equal(3, capped.Count);

        // v2 is the materialising merge's: it migrates by being rewritten.
        var legacy = new[] { Seg(500, _baseNano, _baseNano + 1, 1), Seg(500, _baseNano + 2, _baseNano + 3, 2) }
            .Select(s => new SpanSegmentInfo { FilePath = s.FilePath, MinStartNano = s.MinStartNano, MaxStartNano = s.MaxStartNano,
                                               SpanCount = s.SpanCount, Services = s.Services, FormatVersion = 2 })
            .ToArray();
        Assert.Empty(TraceStorageEngine.SelectStreamingBatch(legacy, 64 * MB, now, hour));
    }

    [Fact]
    public void Overlapping_sources_are_priced_as_read_together()
    {
        long hour = 3_600_000L * Ms;
        long now  = _baseNano + 100 * hour;
        long budget = 24 * MB;

        // Ten one-minute flushes back to back, against ten that all span the same ten minutes: the
        // second set is read ten blocks deep at once, so it buys a smaller batch from one budget.
        var sequential = Enumerable.Range(0, 10).Select(i => Seg(20_000, _baseNano + i * 60_000L * Ms, _baseNano + (i + 1) * 60_000L * Ms - 1, i + 1)).ToArray();
        var stacked    = Enumerable.Range(0, 10).Select(i => Seg(20_000, _baseNano + i, _baseNano + 10 * 60_000L * Ms, i + 1)).ToArray();

        int seqCount   = TraceStorageEngine.SelectStreamingBatch(sequential, budget, now, hour, rowsOf: s => s.SpanCount / 5).Count;
        int stackCount = TraceStorageEngine.SelectStreamingBatch(stacked,    budget, now, hour, rowsOf: s => s.SpanCount / 5).Count;
        _out.WriteLine($"24 MB pass: {seqCount} sequential sources, {stackCount} stacked");
        Assert.True(stackCount < seqCount, $"stacked {stackCount}, sequential {seqCount}");
    }

    // ── A source it cannot read ─────────────────────────────────────────────

    /// <summary>The four sources <see cref="Damaged"/> writes hold this many spans together; the first, damaged one, <see cref="DamagedSpans"/>.</summary>
    private const int Total = 400, DamagedSpans = 99;

    /// <summary>Four consecutive sources (<see cref="WriteSources"/>), the oldest one passed to <paramref name="damage"/>.</summary>
    private (string Damaged, List<SpanSegmentInfo> All) Damaged(string dir, Action<string> damage)
    {
        WriteSources(dir, segments: 4, traces: 20, out var sources);
        damage(sources[0].FilePath);
        return (sources[0].FilePath, sources);
    }

    [Fact]
    public void A_source_that_reads_back_empty_is_quarantined_and_kept_and_the_rest_merge()
    {
        string dir = Dir("empty");
        var (damaged, _) = Damaged(dir, path =>
        {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite);
            fs.Seek(-28, SeekOrigin.End);
            fs.Write(BitConverter.GetBytes(27UL));        // the trace-index offset at the first block
        });

        using var e = Engine(dir);
        e.LoadColdSegments();
        e.CompactSmallSegments();

        var after = e.ColdSegmentsForTest;
        _out.WriteLine($"after: {string.Join(", ", after.Select(s => $"{s.SpanCount}"))}");
        Assert.Equal(2, after.Length);
        Assert.Contains(after, s => s.FilePath == damaged);
        Assert.True(File.Exists(damaged));
        Assert.Contains(after, s => s.SpanCount == Total - DamagedSpans);  // the other three, merged
        e.CompactSmallSegments();
        Assert.Equal(0, e.LastCompactionPassesForTest);
    }

    [Fact]
    public void A_source_that_will_not_decode_is_quarantined_and_kept_and_the_rest_merge()
    {
        string dir = Dir("undecodable");
        var (damaged, _) = Damaged(dir, path =>
        {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite);
            fs.Seek(27, SeekOrigin.Begin);
            fs.Write(BitConverter.GetBytes(0x7FFF_FFFFu));   // the first block's length, impossible
        });

        using var e = Engine(dir);
        e.LoadColdSegments();
        e.CompactSmallSegments();

        var after = e.ColdSegmentsForTest;
        Assert.Equal(2, after.Length);
        Assert.True(File.Exists(damaged));
        Assert.Contains(after, s => s.FilePath == damaged);
        Assert.Contains(after, s => s.SpanCount == Total - DamagedSpans);
        Assert.Empty(Directory.EnumerateFiles(dir, "*.tmp"));
    }

    [Fact]
    public void Running_out_of_memory_mid_merge_is_retried_not_quarantined()
    {
        string dir = Dir("oom");
        WriteSources(dir, segments: 3, traces: 20, out var sources);

        using var e = Engine(dir);
        e.LoadColdSegments();
        bool oom = true;
        e._beforeCompactionReadForTest = seg =>
        {
            if (oom && seg.FilePath == sources[1].FilePath)
                throw SpanReader.OutOfMemoryReading(0, 4_096, new OutOfMemoryException());
        };

        e.CompactSmallSegments();
        Assert.Equal(3, e.ColdSegmentCountForTest);                 // nothing merged, nothing lost
        Assert.Empty(Directory.EnumerateFiles(dir, "*.tmp"));

        oom = false;
        e.CompactSmallSegments();
        Assert.Equal(300, Assert.Single(e.ColdSegmentsForTest).SpanCount);
    }

    [Fact]
    public void A_source_whose_service_graph_will_not_read_is_merged_by_rebuilding_from_spans()
    {
        string dir = Dir("svcgraph");
        WriteSources(dir, segments: 3, traces: 20, out var sources);
        File.WriteAllBytes(Path.ChangeExtension(sources[0].FilePath, ".svcgraph"), [1, 2, 3, 4, 5]);

        using var e = Engine(dir);
        e.LoadColdSegments();
        e.CompactSmallSegments();

        // The damaged one is refused by the streamed merge (its edges cannot be summed) and rewritten
        // ALONE by the materialising one, which rebuilds every sidecar from the spans; the rewrite is
        // a segment a stream takes, so the same run ends with everything in one segment. Reverted
        // (no lone rewrite), the damaged segment is left as it is for good: two segments.
        var after = e.ColdSegmentsForTest;
        _out.WriteLine($"after: {string.Join(", ", after.Select(s => $"{s.SpanCount}"))}");
        var merged = Assert.Single(after);
        Assert.Equal(300, merged.SpanCount);
        Assert.True(ServiceGraphSidecar.TryReadEdges(merged.FilePath, out var edges));
        Assert.Equal(20 * 4 * 3, edges.Sum(x => (long)x.CallCount));   // four children a trace, three sources
    }

    /// <summary>
    /// A source whose header claims a minimum ABOVE its first span. The writer never produces one; it
    /// is what a hand-edited or torn header looks like, and it would break the lazy open's order — the
    /// source would join the merge after spans later than its own were written.
    /// </summary>
    private static void ClaimMinimumAboveFirstSpan(string trcPath)
    {
        var info = SpanReader.ReadSegmentInfo(trcPath);
        using var fs = new FileStream(trcPath, FileMode.Open, FileAccess.ReadWrite);
        fs.Seek(10, SeekOrigin.Begin);
        fs.Write(BitConverter.GetBytes(info.MinStartNano + 1));
    }

    [Fact]
    public void A_source_out_of_start_order_is_refused_with_nothing_published()
    {
        string dir = Dir("unsorted");
        WriteSources(dir, segments: 2, traces: 10, out var sources);
        ClaimMinimumAboveFirstSpan(sources[0].FilePath);
        var claimed = SpanReader.ReadSegmentInfo(sources[0].FilePath);

        var ex = Assert.Throws<MergeSourceException>(() =>
            SpanWriter.WriteMerged(dir, [claimed, sources[1]], TraceStorageEngine.MergeLimitsFor(24 * MB)));
        _out.WriteLine(ex.Message);
        Assert.Equal(claimed.FilePath, ex.FilePath);
        Assert.True(SegmentOrderException.Is(ex.InnerException), ex.InnerException?.ToString());
        Assert.Equal(2, Directory.EnumerateFiles(dir, "*.trc").Count());
        Assert.Empty(Directory.EnumerateFiles(dir, "*.tmp"));
    }

    /// <summary>
    /// …AND THE ENGINE REPAIRS IT RATHER THAN QUARANTINING IT: order is damage a rewrite fixes — the
    /// materialising merge sorts what it reads and writes a true header — so the source is handed to
    /// it, rewritten alone, and merged by a stream in the same run. Quarantined instead, it would sit
    /// out every merge until retention took it.
    /// </summary>
    [Fact]
    public void A_source_out_of_start_order_is_rewritten_in_order_and_then_merged()
    {
        string dir = Dir("unsorted-engine");
        WriteSources(dir, segments: 3, traces: 20, out var sources);
        ClaimMinimumAboveFirstSpan(sources[1].FilePath);

        using var e = Engine(dir);
        e.LoadColdSegments();
        e.CompactSmallSegments();

        var merged = Assert.Single(e.ColdSegmentsForTest);
        Assert.Equal(300, merged.SpanCount);
        Assert.False(File.Exists(sources[1].FilePath));
    }

    /// <summary>
    /// A FAULT OF THE MACHINE AROUND ONE FILE does not hold up the rest of the run: the file sits the
    /// run out, the others merge without it, and the next run takes it. The materialising loader read
    /// past such a file the same way.
    /// </summary>
    [Fact]
    public void A_source_that_cannot_be_opened_sits_out_the_run_and_the_rest_merge()
    {
        string dir = Dir("locked");
        WriteSources(dir, segments: 4, traces: 20, out var sources);

        using var e = Engine(dir);
        e.LoadColdSegments();
        bool locked = true;
        e._beforeCompactionReadForTest = seg =>
        {
            if (locked && seg.FilePath == sources[1].FilePath)
                throw new IOException("The process cannot access the file because it is being used by another process.");
        };

        e.CompactSmallSegments();
        var after = e.ColdSegmentsForTest;
        _out.WriteLine($"locked run: {string.Join(", ", after.Select(s => $"{s.SpanCount}"))}");
        Assert.Equal(2, after.Length);
        Assert.Contains(after, s => s.FilePath == sources[1].FilePath);

        locked = false;
        e.CompactSmallSegments();
        Assert.Equal(Total, Assert.Single(e.ColdSegmentsForTest).SpanCount);
    }

    [Fact]
    public void With_streaming_off_full_flushes_on_the_stand_stay_as_they_are()
    {
        long budget = new TracesOptions().MergeBudgetBytesFor(Stand);
        string dir = Dir("off");
        WriteSources(dir, segments: 3, traces: 30, out _);

        using var e = Engine(dir, new TracesOptions { StreamingCompaction = false, CompactionMinAge = TimeSpan.Zero, MergeBudgetBytes = budget });
        e.LoadColdSegments();
        e.CompactSmallSegments();
        Assert.Null(e.LastStreamedMergeForTest);                // the materialising pass ran, not this one
        Assert.Equal(450, Assert.Single(e.ColdSegmentsForTest).SpanCount);
    }

    [Fact]
    public void The_minimum_age_defaults_to_an_hour_and_zero_is_honoured()
    {
        Assert.Equal(TimeSpan.FromHours(1), new TracesOptions().EffectiveCompactionMinAge);
        Assert.Equal(TimeSpan.Zero, new TracesOptions { CompactionMinAge = TimeSpan.Zero }.EffectiveCompactionMinAge);
        Assert.Equal(TimeSpan.FromHours(1), new TracesOptions { CompactionMinAge = TimeSpan.FromSeconds(-5) }.EffectiveCompactionMinAge);
        Assert.True(new TracesOptions().StreamingCompaction);
    }
}
