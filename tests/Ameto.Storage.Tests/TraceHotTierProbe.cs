using System.Diagnostics;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT ONE SPAN COSTS TO INGEST, AND WHAT THE HOT TIER THEN HOLDS — the gate for the traces half
/// of issue #83, because the 512 MB stand did not die of CPU. It died of a `List&lt;SpanRecord&gt;`
/// weighing 1 117 bytes per span at a 50 000-span flush threshold, doubled for the whole of every
/// flush by the detached snapshot the readers keep seeing, plus 199 MB for a compaction pass built
/// out of the same records.
///
/// <para>RETAINED, NOT ALLOCATED, is the number this file exists for. Allocation is gen0 churn and
/// the collector deals with it; what killed the container is the live set, so the measurement is
/// <c>GC.GetTotalMemory(forceFullCollection: true)</c> across an ingest whose engine is still
/// alive and holding everything it took in.</para>
///
/// <para>Two span shapes, because they bracket the cost: an attribute-less span is the floor a
/// <c>SpanRecord</c> cannot go below, and the eight-attribute SqlClient span is what an
/// instrumented service actually emits. The distance between them IS the attribute map.</para>
///
/// <para>Printed, not asserted, except for one bound: a span in the tier must stay well under the
/// kilobyte that the dictionary cost. A printed figure that drifts is information; an asserted one
/// that drifts is a machine-dependent test.</para>
/// </summary>
public sealed class TraceHotTierProbe : IDisposable
{
    /// <summary>Just under <c>HotFlushThreshold</c> (50 000), so the tier is never flushed out from under the measurement.</summary>
    private const int Spans        = 49_000;
    private const int SpansPerTrace = 10;

    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly List<string> _dirs = [];
    private readonly ITestOutputHelper _out;

    public TraceHotTierProbe(ITestOutputHelper output) => _out = output;

    public void Dispose()
    {
        foreach (var d in _dirs)
            try { Directory.Delete(d, true); } catch { }
    }

    [Fact]
    public void Ingest_cost_and_retained_bytes_per_span()
    {
        // Warm: JIT the engine, the WAL and the msgpack paths so the first measured run is not
        // paying for all three.
        Measure(attributes: true, spans: 2_000, label: null);

        var zero = Measure(attributes: false, spans: Spans, label: "0 attrs");
        var eight = Measure(attributes: true, spans: Spans, label: "8 attrs");

        _out.WriteLine("");
        _out.WriteLine($"attribute map costs {eight.RetainedPerSpan - zero.RetainedPerSpan:N0} B/span retained, "
                     + $"{eight.MicrosPerSpan - zero.MicrosPerSpan:N2} us/span");
        _out.WriteLine($"at the 50 000-span flush threshold the tier is "
                     + $"{eight.RetainedPerSpan * 50_000 / 1048576.0:N0} MB, and twice that across a flush");

        // THE GATE. main retained 1 117 B/span for this shape; the dictionary alone was 987 B of
        // it. A tier that holds the blob has no dictionary, no eight key strings and no eight
        // boxes, so it cannot come near that number — if this trips, the ingest path has started
        // decoding attributes again.
        Assert.True(eight.RetainedPerSpan < 700,
            $"an eight-attribute span retains {eight.RetainedPerSpan:N0} B in the hot tier — the "
            + "attribute map is being decoded on the ingest path again");
    }

    /// <summary>
    /// WHAT THE FIRST TRACE-LIST PAGE OVER A FRESH TIER COSTS, and the reason the number belongs in
    /// this file rather than in the query lens: every byte of it is allocated INSIDE
    /// <c>_lock.EnterReadLock()</c>, which the drainer's <c>WriteSpan</c> has to wait out, and what
    /// it leaves behind is attached to the tier for the rest of the tier's life.
    ///
    /// <para><c>MergeSpanInto</c> asks the first root span of every trace for two HTTP attributes.
    /// While the hot tier inflated every span on ingest that ask was a dictionary probe; once the
    /// tier holds the blob, reaching it through <c>SpanRecord.Attributes</c> makes it the FIRST
    /// touch of that blob and the lazy decode runs under the read lock — a <c>Dictionary</c>, eight
    /// key strings and eight boxes per root span, memoised onto the record. At the 50 000-span
    /// threshold and ten spans a trace that is 5 000 decodes and 9,0 MB the tier never gives back,
    /// on the first page after every flush, i.e. twice a second at 100 k spans/s.</para>
    ///
    /// <para>ALLOCATED IS THE ASSERTION, retained is printed beside it. Every byte of the
    /// allocation is spent while the read lock is held, and it is counted PER THREAD — the
    /// process-wide counter carries xUnit's own output drain, measured at 6 288 B per printed line
    /// on a thread of its own, and this class prints twenty-odd lines before this test runs.
    /// Reverting <c>MergeSpanInto</c> to <c>GetAttr(s.Attributes, …)</c> fails it at 2 023 B per
    /// root span against 623. The retained figure says the same thing louder (1 888 against 498)
    /// but cannot be gated here; the comment on it says why.</para>
    ///
    /// <para>PAGE 2 IS PRINTED AND NOT ASSERTED, and it is the half of the trade that is a cost
    /// rather than a saving: the decode was memoised on the record and the blob walk is not, so a
    /// second page over the same tier pays the walk again. Release, this shape: 2,1 ms with no
    /// attributes at all, 3,0 ms through the memoised dictionary, 5,1 ms through the blob — ≈ 1 µs
    /// per root span of read-lock hold, per page, bought for 1 390 B per root span the tier no
    /// longer keeps. Memoising the two strings on the record is the SSE hot-tier re-walk, which
    /// the plan gives to WP9; the number is here so that decision has something to stand on.</para>
    /// </summary>
    [Fact]
    public async Task A_trace_list_page_does_not_inflate_the_hot_tier_it_walks()
    {
        const int PageSpans = 20_000;
        const int Roots     = PageSpans / SpansPerTrace;

        string dir = Path.Combine(Path.GetTempPath(), "ameto-hotprobe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);

        using var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        long baseNano = Base.ToUnixTimeMilliseconds() * 1_000_000L;
        for (int i = 0; i < PageSpans; i++)
            engine.WriteSpan(new SpanIngestItem
            {
                TraceId           = new TraceId(0x9E3779B97F4A7C15UL, (ulong)(i / SpansPerTrace + 1)),
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = i % SpansPerTrace == 0 ? default : new SpanId((ulong)(i / SpansPerTrace * SpansPerTrace + 1)),
                StartTimeUnixNano = baseNano + i * 1_000_000L,
                DurationNanos     = 1_000_000L * (1 + i % 2000),
                Name              = "SELECT payments",
                ServiceName       = "billing",
                Kind              = SpanKind.Client,
                Status            = SpanStatusCode.Unset,
                HttpStatusCode    = 0,
                AttributesBytes   = SqlClientBlob(i),
            });

        var from = Base.AddMinutes(-1);
        var to   = Base.AddDays(1);

        // JIT THE WHOLE PAGE PATH FIRST, on a tier of the same shape. A trace list is several
        // hundred lines of first-call-in-the-process code and its jitting is milliseconds — enough
        // on its own to make the more-code side of a comparison look like the slower one.
        await WarmTraceListAsync(from, to);

        // Both samples are taken the same way — a compacting gen2 collect either side. A 10 MB
        // tier compacted once and then measured after a NON-compacting collect reports its own
        // fragmentation as the page's retention, which is most of a megabyte of nothing.
        long liveBefore  = LiveBytes();

        // THIS THREAD'S BYTES, NOT THE PROCESS'S, for the reason measured in TraceQlScanProbe:
        // GC.GetTotalAllocatedBytes counts every thread, and the thread that pollutes it here is
        // xUnit's own — it drains each ITestOutputHelper.WriteLine on a thread of its own at
        // 6 288 B a line, and this class prints twenty-odd lines before this test runs. A window
        // with no queued output behind it reads 40 B of other-thread allocation; one that follows
        // a backlog of 22 lines measured 148 240 B, which over 2 000 root spans is 74 B/root span
        // of pure reporting noise on a gate whose whole signal is 623 against 2 023.
        //
        // Per thread the page reads the same value to the byte over twelve consecutive runs, and
        // the same value again with DOTNET_PROCESSOR_COUNT=2. The checks below are what make that
        // claim checkable: the figure is only the whole page if the page completed SYNCHRONOUSLY,
        // which holds here by construction — this tier is hot-only, nothing has been flushed, so
        // GetTraceListAsync never reaches its `await foreach` over SpanReader.SearchAsync. Thread
        // identity would be the wrong question: a page that yields can resume on the thread it
        // left and pass such a check with half its work billed elsewhere.
        long allocBefore = GC.GetAllocatedBytesForCurrentThread();

        var  sw       = Stopwatch.StartNew();
        var  pageTask = engine.GetTraceListAsync(from, to, null, null, null, null, null, 100);
        bool ranHere1 = pageTask.IsCompleted;   // read BEFORE the await: nothing has resumed yet
        var  page     = await pageTask;
        sw.Stop();
        long allocated = GC.GetAllocatedBytesForCurrentThread() - allocBefore;

        // A SECOND PAGE OVER THE SAME TIER, because the two paths differ in kind and not only in
        // size. The dictionary is MEMOISED on the record, so the decode is paid on the first page
        // and never again — and it is the retention that never goes away. The blob walk is paid
        // per page and retains nothing. An SSE client pages this tier twice a second, so both
        // columns belong in the report.
        var  page2  = await engine.GetTraceListAsync(from, to, null, null, null, null, null, 100);
        long alloc2 = GC.GetAllocatedBytesForCurrentThread();
        var  sw2    = Stopwatch.StartNew();
        var  task2  = engine.GetTraceListAsync(from, to, null, null, null, null, null, 100);
        bool ranHere2 = task2.IsCompleted;
        page2 = await task2;
        sw2.Stop();
        long allocated2 = GC.GetAllocatedBytesForCurrentThread() - alloc2;

        // The parity check runs on the rows, but AFTER the numbers are taken and BEFORE they are
        // printed is the wrong order for a probe: a failing parity assert would hide the figures
        // the run exists to report. Rows first into locals, print, then assert.
        string[] paths   = [.. page.Rows.Select(static r => r.HttpPath)];
        string[] methods = [.. page.Rows.Select(static r => r.HttpMethod)];

        int rows = page.Rows.Count;
        page  = default;   // the rows are the page's, not the tier's — they must not be measured
        page2 = default;

        long retained = LiveBytes() - liveBefore;
        GC.KeepAlive(engine);


        _out.WriteLine("");
        _out.WriteLine($"TRACE LIST  GetTraceListAsync(100) over a {PageSpans:N0}-span hot tier "
                     + $"({Roots:N0} traces, {rows} rows)");
        _out.WriteLine($"  wall        {sw.Elapsed.TotalMilliseconds,10:N1} ms   INSIDE the engine read lock");
        _out.WriteLine($"  allocated   {allocated / 1048576.0,10:N1} MB   {allocated / Roots,8:N0} B/root span");
        _out.WriteLine($"  LEFT ON THE TIER {retained / 1048576.0,5:N1} MB   {retained / Roots,8:N0} B/root span");
        _out.WriteLine($"  at 50 000 spans  {retained / (double)Roots * (50_000 / SpansPerTrace) / 1048576.0,6:N1} MB "
                     + "per flush cycle, on the first page after every flush");
        _out.WriteLine($"  page 2      {sw2.Elapsed.TotalMilliseconds,10:N1} ms   "
                     + $"{allocated2 / Roots,8:N0} B/root span   (the SSE re-walk)");

        // PARITY: the blob scan has to give the same two strings the dictionary gave. The
        // SqlClient span carries `http.route` (third key of PathKeys, so the walk has to skip
        // `url.path` and `http.target` to reach it) and no method key at all. Every shape the two
        // paths could disagree on is in
        // The_trace_list_reads_the_same_two_attributes_out_of_the_blob_as_out_of_the_map.
        Assert.NotEmpty(paths);
        Assert.All(paths,   static p => Assert.Equal("/api/v1/tenants/{tenantId}/payments", p));
        Assert.All(methods, static m => Assert.Equal(string.Empty, m));

        // THE ALLOCATION GATE. Every byte of it is allocated while the read lock is held. Measured
        // on this machine, Release: 527 B per root span over an attribute-less tier of this shape
        // (the page's own MergedTrace, service set and summary rows — the floor), 623 B for the
        // blob scan, 2 023 B when MergeSpanInto reaches the same two keys through
        // SpanRecord.Attributes. The decoded map is all of the difference.
        Assert.True(ranHere1 && ranHere2,
            "a trace-list page did not complete synchronously, so the per-thread figures above are "
            + "only the part of it that ran on this thread. This tier is HOT-ONLY and nothing has "
            + "been flushed, so the page has no yielding await; if the hot-tier path has gained "
            + "one, measure it with GC.GetTotalAllocatedBytes minus a baseline idle sample rather "
            + "than trusting these numbers — and take that baseline the same way, because "
            + "ITestOutputHelper.WriteLine costs 6 288 B on another thread per printed line.");

        Assert.True(allocated / Roots < 1_000,
            $"a trace-list page allocated {allocated / Roots:N0} B per root span — MergeSpanInto is "
            + "decoding whole attribute maps under the engine read lock again");

        // AND WHAT IT LEAVES BEHIND, PRINTED AND NOT ASSERTED. This is the half that outlives the
        // request — a decode is memoised on the record, so a tier listed once stays that much
        // heavier until it flushes — and it is also the half that cannot carry a gate at this
        // denominator. GC.GetTotalMemory is the whole process's live set, so whatever the class
        // that happened to run before this one left behind lands on the figure, and 2 000 root
        // spans divide a fixed offset into a large per-row number: measured over two full Release
        // suites, the SAME code read 493 and 1 372 B per root span, while run alone it reads 498
        // every time. An 900-B gate over that is a coin toss, and a gate loose enough to survive
        // the offset no longer separates the blob scan from the decode it replaced.
        //
        // The figures, taken alone: attribute-less tier 392, blob scan 498, decode 1 888 — at the
        // 50 000-span threshold, 2,4 MB against 9,0 MB per flush cycle. The gate above is the
        // allocation, counted per thread, which carries no other thread's work at all and reads
        // the same value to the byte in every context measured.
        _out.WriteLine($"  (retained is printed, not gated — see the comment: it carries the live "
                     + $"set of whatever ran before this test)");
    }

    /// <summary>A throwaway tier of the same shape, listed once, so nothing below is jitting.</summary>
    private async Task WarmTraceListAsync(DateTimeOffset from, DateTimeOffset to)
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-hotprobe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);

        using var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        long baseNano = Base.ToUnixTimeMilliseconds() * 1_000_000L;
        for (int i = 0; i < 500; i++)
            engine.WriteSpan(new SpanIngestItem
            {
                TraceId           = new TraceId(0x51ED270BUL, (ulong)(i / SpansPerTrace + 1)),
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = i % SpansPerTrace == 0 ? default : new SpanId((ulong)(i / SpansPerTrace * SpansPerTrace + 1)),
                StartTimeUnixNano = baseNano + i * 1_000_000L,
                DurationNanos     = 1_000_000L,
                Name              = "SELECT payments",
                ServiceName       = "billing",
                Kind              = SpanKind.Client,
                Status            = SpanStatusCode.Unset,
                HttpStatusCode    = 0,
                AttributesBytes   = SqlClientBlob(i),
            });

        _ = await engine.GetTraceListAsync(from, to, null, null, null, null, null, 100);
    }

    /// <summary>
    /// THE BLOB SCAN ANSWERS WHAT THE DICTIONARY ANSWERED — every map shape, not just the one the
    /// probe above lists. The probe measures the saving; this measures nothing and is the reason
    /// the saving is allowed to be taken: <c>MergeSpanInto</c> stopped asking
    /// <c>SpanRecord.Attributes</c> for the row's HTTP method and path and now walks the msgpack
    /// blob for both, so every rule the dictionary probe got for free has to be reproduced by hand
    /// — key order, first-key-present-with-a-non-null-value, last copy of a duplicated key wins,
    /// arrays and nested maps and msgpack nil all reading as "no value here, try the next key",
    /// and <c>ToString()</c> text for integers, floats and booleans.
    ///
    /// <para>The oracle is the dictionary itself, read through the engine's own key lists, so the
    /// theory cannot drift away from the code it guards: rename either list and this stops
    /// compiling rather than stops checking.</para>
    ///
    /// <para>The nil-key row is the one that is not about semconv at all. A map whose key is
    /// msgpack nil is a map <c>Decode</c> reads happily (the key becomes <c>""</c>), and a walker
    /// that reaches for the key with <c>MessagePackReader.TryReadStringSpan</c> — which does NOT
    /// advance when it declines — skips the KEY instead of the value and reads every remaining
    /// pair one slot out of step, so a perfectly ordinary <c>url.path</c> sitting after it
    /// disappears. It is pinned here because nothing else in the suite would notice.</para>
    /// </summary>
    [Fact]
    public async Task The_trace_list_reads_the_same_two_attributes_out_of_the_blob_as_out_of_the_map()
    {
        var shapes = AttrShapes();

        string dir = Path.Combine(Path.GetTempPath(), "ameto-hotprobe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);

        using var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        long baseNano = Base.ToUnixTimeMilliseconds() * 1_000_000L;
        for (int i = 0; i < shapes.Count; i++)
            engine.WriteSpan(new SpanIngestItem
            {
                TraceId           = new TraceId((ulong)(i + 1), 0xA11CE),   // High indexes the shape back
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = default,                     // root: the only span MergeSpanInto asks
                StartTimeUnixNano = baseNano + i * 1_000_000L,
                DurationNanos     = 5_000_000L,
                Name              = "GET /payments",
                ServiceName       = "billing",
                Kind              = SpanKind.Server,
                Status            = SpanStatusCode.Unset,
                HttpStatusCode    = 200,
                AttributesBytes   = shapes[i].Blob,
            });

        var page = await engine.GetTraceListAsync(
            Base.AddMinutes(-1), Base.AddDays(1), null, null, null, null, null, shapes.Count + 10);

        Assert.Equal(shapes.Count, page.Rows.Count);

        string[] methodKeys = EngineKeys("MethodKeys");
        string[] pathKeys   = EngineKeys("PathKeys");

        foreach (var row in page.Rows)
        {
            var (label, blob) = shapes[(int)row.TraceId.High - 1];
            string method = Oracle(blob, methodKeys);
            string path   = Oracle(blob, pathKeys);

            _out.WriteLine($"{label,-46} method={Quote(row.HttpMethod),-10} path={Quote(row.HttpPath)}");

            Assert.Equal(method, row.HttpMethod);
            Assert.Equal(path,   row.HttpPath);
        }
    }

    private static string Quote(string s) => s.Length == 0 ? "(empty)" : "\"" + s + "\"";

    /// <summary>What a dictionary probe of the same key list would have returned. The four lines
    /// <c>TraceStorageEngine.GetAttr</c> still runs for a span that carries no blob.</summary>
    private static string Oracle(byte[] blob, string[] keys)
    {
        var attrs = blob.Length == 0 ? null : SpanAttributeBlob.Decode(blob);
        if (attrs is null) return string.Empty;
        foreach (var k in keys)
            if (attrs.TryGetValue(k, out var v) && v is not null)
                return v.ToString() ?? string.Empty;
        return string.Empty;
    }

    /// <summary>The engine's own semconv key list, so the oracle asks exactly what the code asks.</summary>
    private static string[] EngineKeys(string field) =>
        (string[])typeof(TraceStorageEngine)
            .GetField(field, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .GetValue(null)!;

    /// <summary>Every attribute-map shape the two lookups have to survive, worst ones first.</summary>
    private static List<(string Label, byte[] Blob)> AttrShapes()
    {
        const string M1 = "http.request.method", M2 = "http.method";
        const string P1 = "url.path", P2 = "http.target", P3 = "http.route", P4 = "url.full", P5 = "http.url";

        return
        [
            ("a nil KEY before the answer",        Map(null, "shadow", P1, "/api/after-a-nil-key")),
            ("first path key",                     Map(P1, "/api/a")),
            ("second path key only",               Map(P2, "/api/b?x=1")),
            ("third path key only",                Map(P3, "/api/{id}")),
            ("fourth path key only",               Map(P4, "https://h/api/d")),
            ("fifth path key only",                Map(P5, "https://h/api/e")),
            ("both method keys, first wins",       Map(M2, "POST", M1, "GET")),
            ("second method key only",             Map(M2, "PUT")),
            ("method and path together",           Map(M1, "PATCH", P1, "/api/f")),
            ("earlier key nil, later key answers", Map(P1, null, P2, "/api/g")),
            ("earlier key an array",               Map(P1, Shape.Array, P2, "/api/h")),
            ("earlier key a nested map",           Map(P1, Shape.NestedMap, P2, "/api/i")),
            ("duplicate key, last copy wins",      Map(P1, "/api/first", P1, "/api/last")),
            ("duplicate key, last copy nil",       Map(P1, "/api/first", P1, null, P2, "/api/j")),
            ("integer value",                      Map(M1, 8080L, P1, "/api/k")),
            ("float value",                        Map(M1, 0.5, P1, "/api/l")),
            ("boolean value",                      Map(M1, true, P1, "/api/m")),
            ("multi-byte UTF-8 value",             Map(P1, "/api/Ünïcödé")),
            ("no HTTP attributes at all",          Map("db.system", "mssql")),
            ("no attributes at all",               []),
            ("a blob that will not decode",        Map(P1, "/api/n")[..6]),
        ];
    }

    private enum Shape { Array, NestedMap }

    /// <summary>One msgpack map. A null KEY is written as msgpack nil; a null VALUE likewise.</summary>
    private static byte[] Map(params object?[] kv)
    {
        var buf = new System.Buffers.ArrayBufferWriter<byte>(256);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(kv.Length / 2);
        for (int i = 0; i < kv.Length; i += 2)
        {
            if (kv[i] is string key) w.Write(key); else w.WriteNil();
            switch (kv[i + 1])
            {
                case null:              w.WriteNil();   break;
                case string s:          w.Write(s);     break;
                case long l:            w.Write(l);     break;
                case double d:          w.Write(d);     break;
                case bool b:            w.Write(b);     break;
                case Shape.Array:       w.WriteArrayHeader(2); w.Write("a"); w.Write("b");      break;
                case Shape.NestedMap:   w.WriteMapHeader(1);   w.Write("host"); w.Write("h1");  break;
                default: throw new InvalidOperationException($"unhandled value {kv[i + 1]}");
            }
        }
        w.Flush();
        return buf.WrittenMemory.ToArray();
    }

    /// <summary>The live set, sampled the same way every time it is sampled.</summary>
    private static long LiveBytes()
    {
        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        return GC.GetTotalMemory(forceFullCollection: true);
    }

    private readonly record struct Result(double MicrosPerSpan, long AllocPerSpan, long RetainedPerSpan);

    private Result Measure(bool attributes, int spans, string? label)
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-hotprobe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);

        // THE LIVE-SET BASELINE IS TAKEN BEFORE THE CORPUS EXISTS, and that is not fussiness. The
        // attribute blob the corpus hands over is the SAME ARRAY the tier ends up holding, so a
        // baseline taken after the corpus was built charges the tier nothing for it: the corpus
        // being dropped at the end frees the SpanIngestItem wrappers and nothing else, and the
        // probe reports 36 B/span for a span carrying 375 B of attributes. Measuring from before
        // means the delta is everything ingest made live, blobs included.
        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        long liveBefore = GC.GetTotalMemory(forceFullCollection: true);

        // The whole corpus is built and handed over BEFORE the clock starts: the mapper's work is
        // the ingest lens's (WP1), not this one's, and leaving it inside would put a byte[] per
        // span on this probe's allocation figure.
        var items = new SpanIngestItem[spans];
        long baseNano = Base.ToUnixTimeMilliseconds() * 1_000_000L;
        for (int i = 0; i < spans; i++)
            items[i] = new SpanIngestItem
            {
                TraceId           = new TraceId(0x9E3779B97F4A7C15UL, (ulong)(i / SpansPerTrace + 1)),
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = i % SpansPerTrace == 0 ? default : new SpanId((ulong)(i / SpansPerTrace * SpansPerTrace + 1)),
                StartTimeUnixNano = baseNano + i * 1_000_000L,
                DurationNanos     = 1_000_000L * (1 + i % 2000),
                Name              = "SELECT payments",
                ServiceName       = "billing",
                Kind              = SpanKind.Client,
                Status            = SpanStatusCode.Unset,
                HttpStatusCode    = 0,
                AttributesBytes   = attributes ? SqlClientBlob(i) : [],
            };

        var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        long allocBefore = GC.GetTotalAllocatedBytes(precise: true);
        int  g2Before    = GC.CollectionCount(2);

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < spans; i++) engine.WriteSpan(items[i]);
        sw.Stop();

        long allocated = GC.GetTotalAllocatedBytes(precise: true) - allocBefore;
        int  g2        = GC.CollectionCount(2) - g2Before;

        // The corpus is as heavy as the tier; drop it before sampling the live set or it is
        // counted twice.
        Array.Clear(items);
        long retained = GC.GetTotalMemory(forceFullCollection: true) - liveBefore;
        GC.KeepAlive(engine);

        long walBytes = Directory.EnumerateFiles(dir, "*.wal").Sum(static f => new FileInfo(f).Length);

        var r = new Result(sw.Elapsed.TotalMicroseconds / spans, allocated / spans, retained / spans);

        if (label is not null)
        {
            _out.WriteLine("");
            _out.WriteLine($"HOT TIER  {spans:N0} spans through TraceStorageEngine.WriteSpan   [{label}]");
            _out.WriteLine($"  wall        {sw.Elapsed.TotalMilliseconds,10:N1} ms   {r.MicrosPerSpan,8:N2} us/span");
            _out.WriteLine($"  allocated   {allocated / 1048576.0,10:N1} MB   {r.AllocPerSpan,8:N0} B/span");
            _out.WriteLine($"  RETAINED    {retained / 1048576.0,10:N1} MB   {r.RetainedPerSpan,8:N0} B/span");
            _out.WriteLine($"  at 50 000   {r.RetainedPerSpan * 50_000 / 1048576.0,10:N0} MB   (+ the same again while a flush builds)");
            _out.WriteLine($"  g2 during ingest {g2}");
            _out.WriteLine($"  WAL file    {walBytes / 1048576.0,10:N1} MB   {walBytes / spans,8:N0} B/span");
        }

        engine.Dispose();
        return r;
    }

    /// <summary>
    /// The msgpack map an OpenTelemetry SqlClient instrumentation emits — the same eight
    /// attributes <c>ColdSpanSegmentFixture</c> uses, written the way <c>OtlpTraceMapper</c> writes
    /// them, so the blob is the 375-byte one every figure in the recon report is quoted against.
    /// </summary>
    internal static byte[] SqlClientBlob(int i)
    {
        var buf = new System.Buffers.ArrayBufferWriter<byte>(512);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(8);
        w.Write("db.system");         w.Write("mssql");
        w.Write("db.name");           w.Write("payments");
        w.Write("db.statement");      w.Write("SELECT TOP 100 Id, TenantId, Amount, CreatedUtc FROM dbo.Payments "
                                            + "WHERE TenantId = @p0 AND CreatedUtc >= @p1 ORDER BY CreatedUtc DESC");
        w.Write("net.peer.name");     w.Write("sql-prod-03.svc.cluster.local");
        w.Write("net.peer.port");     w.Write(1433L);
        w.Write("http.route");        w.Write("/api/v1/tenants/{tenantId}/payments");
        w.Write("thread.id");         w.Write((long)(i % 64));
        w.Write("otel.library.name"); w.Write("OpenTelemetry.Instrumentation.SqlClient");
        w.Flush();
        return buf.WrittenMemory.ToArray();
    }
}
