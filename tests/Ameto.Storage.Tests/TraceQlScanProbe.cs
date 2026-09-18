using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Ameto.Tracing.TraceQL;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT ONE TRACEQL PAGE COSTS PER SPAN IT SCANNED, and the proof that reading the answer out of
/// the msgpack blob gives the same answer as reading it out of a dictionary.
///
/// <para>The query is the repro's own: <c>{ .db.system = "mssql" &amp;&amp; duration &gt; 1s }</c>
/// over one 50 000-span segment for 200 rows. At main it allocated 43,6 MB — 913 B per span
/// scanned — to evaluate a predicate that reads ONE key, because every span the scalar filter
/// admitted was inflated into a <c>Dictionary</c> with eight key strings and eight boxes first.
/// A page is not allowed to cost a megabyte per row.</para>
///
/// <para>The parity theory below is the other half, and it is the load-bearing half: a predicate
/// that scans bytes and a predicate that reads a dictionary must agree on every value shape, or a
/// span in the hot tier and the same span after a flush answer the same query differently.</para>
/// </summary>
public sealed class TraceQlScanProbe : IClassFixture<ColdSpanSegmentFixture>, IDisposable
{
    private readonly ColdSpanSegmentFixture _fx;
    private readonly ITestOutputHelper      _out;
    private readonly List<string>           _dirs = [];

    public TraceQlScanProbe(ColdSpanSegmentFixture fx, ITestOutputHelper output)
    {
        _fx  = fx;
        _out = output;
    }

    public void Dispose()
    {
        foreach (var d in _dirs)
            try { Directory.Delete(d, true); } catch { }
    }

    // ── The probe ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task Allocated_bytes_per_span_scanned()
    {
        string dir = _fx.CopySegmentToPrivateDir();
        _dirs.Add(dir);

        using var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        engine.LoadColdSegments();   // the constructor does not rescan; the background worker would

        var from = ColdSpanSegmentFixture.Base.AddMinutes(-1);
        var to   = ColdSpanSegmentFixture.Base.AddDays(1);
        var pred = TraceQLParser.Parse("{ .db.system = \"mssql\" && duration > 1s }");

        await TraceQLExecutor.ExecuteAsync(engine, pred, from, to, 200, CancellationToken.None); // warm

        // PROCESS-WIDE ON PURPOSE, unlike the per-row gate below: this page reaches cold segments
        // and so has a yielding await, which would bill part of itself to another thread. The
        // other-thread noise it therefore admits is xUnit's output drain — 6 288 B a printed line,
        // and even the whole suite's backlog (~24 lines ≈ 150 kB) is ~3 B per span scanned against
        // a 700 B gate.
        long a0   = GC.GetTotalAllocatedBytes(precise: true);
        long loh0 = GC.GetGCMemoryInfo().GenerationInfo[^1].SizeAfterBytes;
        var  sw   = Stopwatch.StartNew();
        var  page = await TraceQLExecutor.ExecuteAsync(engine, pred, from, to, 200, CancellationToken.None);
        sw.Stop();
        long allocated = GC.GetTotalAllocatedBytes(precise: true) - a0;
        long loh       = GC.GetGCMemoryInfo().GenerationInfo[^1].SizeAfterBytes - loh0;

        int scanned = ColdSpanSegmentFixture.Spans;

        _out.WriteLine($"TRACEQL  {{ .db.system = \"mssql\" && duration > 1s }} limit 200 "
                     + $"over a {scanned:N0}-span segment");
        _out.WriteLine($"  wall        {sw.Elapsed.TotalMilliseconds,10:N1} ms   "
                     + $"{sw.Elapsed.TotalMicroseconds / scanned,6:N2} us/span scanned");
        _out.WriteLine($"  allocated   {allocated / 1048576.0,10:N1} MB   {allocated / scanned,6:N0} B/span scanned");
        _out.WriteLine($"  LOH delta   {loh / 1048576.0,10:N1} MB");
        _out.WriteLine($"  rows        {page.Rows.Count}, capped={page.Capped}");

        Assert.NotEmpty(page.Rows);

        // THE GATE. main allocated 913 B per span scanned; the dictionary was ~1,5 KB of it and
        // the scan's own SpanRecord and blob copy are the rest. A predicate that reads one key out
        // of the bytes cannot come near the old figure.
        Assert.True(allocated / scanned < 700,
            $"a TraceQL page allocated {allocated / scanned:N0} B per span scanned — the attribute "
            + "dictionary is being built for spans the predicate only reads one key from");
    }

    // ── The parity theory ─────────────────────────────────────────────────────

    /// <summary>
    /// Every value shape a msgpack attribute map can hold, asked the same question twice: once of a
    /// span carrying the blob (what storage hands out now) and once of a span carrying the decoded
    /// dictionary (what every test fixture builds, and what a legacy caller still gets). The two
    /// must return the SAME three-valued answer, including the nulls.
    ///
    /// <para>Delete the blob branch in <c>AttributePredicate.Evaluate</c> — or let it answer
    /// <c>false</c> where the dictionary answers <c>null</c> — and the absent, nil, array and
    /// nested-map rows fail here. Leaving the boxing rules of <c>SpanAttributeBlob</c> alone is what
    /// keeps the numeric rows passing: decode <c>1433</c> to <c>ushort</c> instead of <c>long</c>
    /// and <c>.net.peer.port &gt; 1000</c> answers <c>false</c> on one side and <c>true</c> on the
    /// other.</para>
    /// </summary>
    [Theory]
    [InlineData("{ .db.system = \"mssql\" }",          true)]
    [InlineData("{ .db.system = \"MSSQL\" }",          true)]   // ordinal ignore case
    [InlineData("{ .db.system != \"pgsql\" }",         true)]
    [InlineData("{ .db.system < \"n\" }",              true)]   // ordering, not just equality
    [InlineData("{ .db.system > \"n\" }",              false)]
    [InlineData("{ .name.unicode = \"Ünïcödé\" }",     true)]   // multi-byte UTF-8 both sides
    [InlineData("{ .net.peer.port = 1433 }",           true)]   // uint16 encoding
    [InlineData("{ .net.peer.port > 1000 }",           true)]
    [InlineData("{ .net.peer.port < 1000 }",           false)]
    [InlineData("{ .thread.id = 63 }",                 true)]   // positive fixint
    [InlineData("{ .retry.delta < 0 }",                true)]   // int8, negative
    [InlineData("{ .cpu.ratio > 0.05 }",               true)]   // float64
    [InlineData("{ .net.peer.port = \"1433\" }",       true)]   // a number met by a string query: long.ToString()
    [InlineData("{ .cpu.ratio = \"banana\" }",         false)]  // a double met by a string query it cannot be
    [InlineData("{ .cache.hit = \"true\" }",           true)]   // bool.ToString() is "True"
    [InlineData("{ .cache.hit > 0 }",                  false)]  // present but incomparable stays false
    [InlineData("{ .numeric.text > 41 }",              true)]   // a numeric string parses
    [InlineData("{ .numeric.text > 43 }",              false)]
    [InlineData("{ .missing = \"x\" }",                null)]   // absent: unknown
    [InlineData("{ .tenant.id = \"x\" }",              null)]   // msgpack nil: unknown
    [InlineData("{ .tags = \"x\" }",                   null)]   // an array boxes to null: unknown
    [InlineData("{ .peer.info = \"x\" }",              null)]   // a nested map, likewise
    [InlineData("{ !(.missing = \"x\") }",             null)]   // and unknown negates to unknown
    [InlineData("{ .db.system != nil }",               true)]   // presence
    [InlineData("{ .tenant.id != nil }",               false)]
    [InlineData("{ .tags != nil }",                    false)]  // absent by the dictionary's reckoning
    [InlineData("{ .missing = nil }",                  true)]
    [InlineData("{ .shadowed = \"from-span\" }",       true)]   // last key wins, as the mapper intends
    public void A_predicate_answers_the_same_from_the_blob_as_from_the_dictionary(string query, bool? expected)
    {
        var blob = EveryValueShape();
        var pred = TraceQLParser.Parse(query);

        bool? fromBlob = pred.Evaluate(SpanWith(blob, dictionary: false));
        bool? fromDict = pred.Evaluate(SpanWith(blob, dictionary: true));

        _out.WriteLine($"{query,-38} blob={Show(fromBlob)}  dict={Show(fromDict)}");

        Assert.Equal(expected, fromBlob);
        Assert.Equal(fromBlob, fromDict);
    }

    /// <summary>
    /// TS#13: THE KEY LIST IS A CONSTANT, SO IT MUST NOT BE AN ALLOCATION. <c>BuildRow</c> asks for
    /// the HTTP method and path of every row it returns, and each ask went through a
    /// <c>params string[]</c> parameter with literal arguments — a fresh <c>string[]</c> per call,
    /// two per row, on a page that clamps at a thousand rows, and invisible to every caller because
    /// the array is the callee's signature and not the caller's expression.
    ///
    /// <para>MEASURED THROUGH <c>ExecuteAsync</c>, WHICH IS THE POINT. The first version of this
    /// test called <c>GetAttr</c> directly with the new static lists, so it measured the new call
    /// SHAPE and not the allocation the item removed: restore <c>params string[]</c> on the helper
    /// and hand it those same static arrays and a params parameter passes an existing array
    /// through untouched — nothing allocates, the test stays green, and <c>BuildRow</c>'s literal
    /// arguments go on building two arrays a row with no one watching. The only caller that can
    /// tell the difference is the real one.</para>
    ///
    /// <para>THE THIRD PAGE IS THE MEASURED ONE. <c>BuildRow</c> reaches the row's attributes
    /// through <c>SpanRecord.Attributes</c>, whose decode is memoised ON THE RECORD, and the hot
    /// tier hands out the same records to every query — so the first page pays a decode per row
    /// and the 104 B this test exists for would be noise inside it. Two warm pages leave a page
    /// whose per-row cost is <c>BuildRow</c>'s own objects and nothing else.</para>
    ///
    /// <para>Restore the <c>params string[]</c> signature and its literal arguments at
    /// <c>TraceQLExecutor.cs:BuildRow</c> and this fails at 104 B a row above the gate.</para>
    /// </summary>
    [Fact]
    public async Task Reading_the_http_attributes_of_a_row_allocates_nothing()
    {
        const int Rows = 1_000;   // the clamp POST /api/traces/query applies

        string dir = Path.Combine(Path.GetTempPath(), "ameto-qlrows-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);

        using var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        var  baseAt   = ColdSpanSegmentFixture.Base;
        long baseNano = baseAt.ToUnixTimeMilliseconds() * 1_000_000L;
        var  attrs    = HttpRootBlob();

        // One root span per trace, so a row is a row is a span: the page's per-row figure is not
        // diluted by the spans of a trace that did not produce one.
        for (int i = 0; i < Rows; i++)
            engine.WriteSpan(new SpanIngestItem
            {
                TraceId           = new TraceId(0x5EED, (ulong)(i + 1)),
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = default,
                StartTimeUnixNano = baseNano + i * 1_000_000L,
                DurationNanos     = 5_000_000_000L,
                Name              = "GET /payments",
                ServiceName       = "billing",
                Kind              = SpanKind.Server,
                Status            = SpanStatusCode.Unset,
                HttpStatusCode    = 200,
                AttributesBytes   = attrs,
            });

        var from = baseAt.AddMinutes(-1);
        var to   = baseAt.AddDays(1);
        var pred = TraceQLParser.Parse("{ duration > 1s }");

        _ = await TraceQLExecutor.ExecuteAsync(engine, pred, from, to, Rows, CancellationToken.None);
        _ = await TraceQLExecutor.ExecuteAsync(engine, pred, from, to, Rows, CancellationToken.None);

        // THIS THREAD'S BYTES, NOT THE PROCESS'S — and the source of the spread that forces it is
        // THIS PROBE'S OWN PRINTING, not a parallel suite. AssemblyInfo.cs:17 disables test
        // parallelisation, so no other class is allocating here at all; what lands in a
        // process-wide reading is ITestOutputHelper.WriteLine. xUnit queues each line and drains it
        // on its own thread, and that drain is what the counter sees. Measured: a window with no
        // WriteLine before it carries 40 B of other-thread allocation (the Stopwatch, on this
        // thread); every window that follows one WriteLine carries 6 288 B, and identically so for
        // a 0,1 ms window and a 32 ms one — it is per line, not per millisecond. A backlog of 22
        // queued lines drained into a single 1 ms idle window measured 148 240 B. Two consecutive
        // precise readings with nothing between them differ by 0, so the counter itself is exact.
        //
        // That IS the ~175 B/row spread: 1 192 256 − 1 018 368 = 173 888 B ≈ 27 lines of this
        // class's own output landing inside the measured page, and 1 089 328 − 1 018 368 = 70 960
        // ≈ 11 of them. The engine is not the source — the same 6 288 B appears in an idle window
        // with an engine alive and no query running, and with no engine at all.
        //
        // GC.GetAllocatedBytesForCurrentThread cannot see any of it: the drain is another thread.
        // Over twelve consecutive pages it reads the SAME value to the byte — 1 018 368 B Release,
        // 1 018 976 B Debug — run alone, run inside the whole suite, and with
        // DOTNET_PROCESSOR_COUNT=2. The check below is what makes that claim checkable.
        //
        // THE PRECONDITION THE FIGURE RESTS ON, asserted and not assumed: the page must complete
        // SYNCHRONOUSLY, because only then is every byte it allocated this thread's. It holds here
        // by construction — the directory is fresh and 1 000 spans is far under the 50 000-span
        // flush threshold, so there are no cold segments and ExecuteAsync never reaches its
        // `await foreach` over SpanReader.SearchAsync — and IsCompleted, read before the await, is
        // what says so. Thread identity was the old check and it asks the wrong question twice
        // over: a page that yields can resume on the very thread it left, passing the check with
        // half its work billed elsewhere, and its failure message blames threads for what is
        // really a changed execution shape.
        long before   = GC.GetAllocatedBytesForCurrentThread();
        var  pageTask = TraceQLExecutor.ExecuteAsync(engine, pred, from, to, Rows, CancellationToken.None);
        bool ranHere  = pageTask.IsCompleted;   // read BEFORE the await: nothing has resumed yet
        var  page     = await pageTask;
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(ranHere,
            "the TraceQL page did not complete synchronously, so the per-thread figure below is "
            + "only the part of it that ran on this thread. This probe measures a HOT-TIER page, "
            + "which has no yielding await; if the hot-tier path has gained one, measure the page "
            + "with GC.GetTotalAllocatedBytes minus a baseline idle sample rather than widening "
            + "the gate — and note that ITestOutputHelper.WriteLine costs 6 288 B on another "
            + "thread per printed line, so that baseline has to be taken the same way.");

        _out.WriteLine($"TRACEQL PAGE  {Rows:N0} rows, one root span each, warm: "
                     + $"{allocated:N0} B ({allocated / (double)Rows:N0} B/row)");

        Assert.Equal(Rows, page.Rows.Count);
        Assert.Equal("GET",              page.Rows[0].HttpMethod);
        Assert.Equal("/api/v1/payments", page.Rows[0].HttpPath);

        // WHAT THE DEFECT WEIGHS, MEASURED ON THIS RUNTIME AND AGAINST TODAY'S KEY LISTS. Restoring
        // `params string[]` on GetAttr puts two key arrays back on every row; this loop allocates
        // exactly those two arrays a thousand times and weighs them the same way the page above was
        // weighed. On x64 it is 104 B a row — a string[2] (24 B header + 2×8 = 40) and a string[5]
        // (24 + 5×8 = 64) — and that is precisely the 104 000 B separating the two measured pages:
        // restoring the params signature and BuildRow's literal arguments measures 1 122 976 B
        // Debug and 1 122 368 B Release against this file's 1 018 976 / 1 018 368.
        //
        // IT USED TO SAY 88, AND THE SECOND LIST WAS THE REASON. The calibration wrote its key
        // lists out as literals and kept the executor's OLD three-key path list, so it weighed a
        // string[3] against the five-key list BuildRow actually passes — 88 B where the defect is
        // 104, and a claim of "88 000 B separating the pages" that was never measured. Understating
        // the defect does not loosen the gate, it thins the margin ABOVE the baseline: 44 B of
        // headroom where 52 was earned. Reading the lengths off HttpSemconvKeys is what stops the
        // next key added to PathKeys from thinning it again, silently.
        double defectPerRow = MeasureTwoKeyArraysPerRow(Rows);
        _out.WriteLine($"              two params key arrays weigh {defectPerRow:N0} B/row here");
        Assert.InRange(defectPerRow, 40, 200);   // the calibration itself must have measured something

        // THE GATE: the measured page plus half of the measured defect. Both halves of the margin
        // are numbers this run took, and the only constant is the baseline — which is sound to the
        // BYTE and not merely stable, because it is one thread's allocation over a deterministic
        // code path. Measured with the per-thread counter: 1 018,368 B/row in Release and
        // 1 018,976 B/row in Debug, the SAME figure run alone, run beside its own class, run inside
        // the whole suite and with DOTNET_PROCESSOR_COUNT=2 (which is what CI gives it). The row's
        // own TraceRowDto, service set, id strings and services array are that 1 019. Re-measured
        // for this calibration: ten consecutive Debug runs and ten Release runs, every one of them
        // 1 018 976 B and 1 018 368 B to the byte.
        //
        // So the gate is 1 071, sitting 52 B above the worst measured figure and 52 B below the
        // defect page's 1 123 B/row (measured: 1 122 976 B Debug, 1 122 368 B Release), and CI's
        // Debug 2-core run is the configuration the baseline was taken in.
        const double Baseline = 1_019;   // the Debug figure, rounded up to the byte
        double gate    = Baseline + defectPerRow / 2;
        double perRow  = allocated / (double)Rows;

        Assert.True(perRow < gate,
            $"a returned row cost {perRow:N0} B against a gate of {gate:N0} — BuildRow is building "
            + $"its semconv key lists per row again (two params string[] is {defectPerRow:N0} B a row)");
    }

    /// <summary>
    /// ONE TRACE, TWO READERS, ONE ANSWER. The trace list reaches a root span's HTTP path through
    /// <c>TraceStorageEngine.MergeSpanInto</c> and a TraceQL row reaches the same span's through
    /// <c>TraceQLExecutor.BuildRow</c>, and each used to carry its own copy of the semconv key
    /// list. The copies had drifted: the engine looked under five path keys and the executor under
    /// three, so a span whose path arrived as <c>url.full</c> or <c>http.url</c> — which is what an
    /// HTTP CLIENT span emits, semconv has no <c>url.path</c> for it — showed a path in the trace
    /// list and an empty one in a TraceQL row FOR THE SAME TRACE, on the same screen.
    ///
    /// <para>Shorten <c>HttpSemconvKeys.PathKeys</c> back to its first three entries and the
    /// second assert fails with an empty string against the URL; give either reader its own list
    /// again and it fails the same way.</para>
    /// </summary>
    [Theory]
    [InlineData("url.full", "https://api.example.com/v1/payments?id=7")]
    [InlineData("http.url", "https://api.example.com/v1/refunds")]
    public async Task The_trace_list_and_a_traceql_row_read_the_same_path_key(string key, string url)
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-keyparity-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);

        using var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        var  at       = ColdSpanSegmentFixture.Base;
        long baseNano = at.ToUnixTimeMilliseconds() * 1_000_000L;

        engine.WriteSpan(new SpanIngestItem
        {
            TraceId           = new TraceId(0xC11E27, 1),
            SpanId            = new SpanId(1),
            ParentSpanId      = default,
            StartTimeUnixNano = baseNano,
            DurationNanos     = 5_000_000_000L,
            Name              = "GET",
            ServiceName       = "checkout",
            Kind              = SpanKind.Client,
            Status            = SpanStatusCode.Unset,
            HttpStatusCode    = 200,
            AttributesBytes   = OneAttr(key, url),
        });

        var from = at.AddMinutes(-1);
        var to   = at.AddDays(1);

        var list = await engine.GetTraceListAsync(from, to, null, null, null, null, null, 10);
        var page = await TraceQLExecutor.ExecuteAsync(
            engine, TraceQLParser.Parse("{ duration > 1s }"), from, to, 10, CancellationToken.None);

        string fromList = Assert.Single(list.Rows).HttpPath;
        string fromQl   = Assert.Single(page.Rows).HttpPath;
        _out.WriteLine($"{key,-10} trace list \"{fromList}\"   traceql \"{fromQl}\"");

        Assert.Equal(url, fromList);
        Assert.Equal(fromList, fromQl);
    }

    /// <summary>
    /// THE SAME TRACE, BEFORE AND AFTER ITS TIER FLUSHED. The sibling above holds the two HOT
    /// readers together; this one holds the COLD one. A flushed trace no longer answers out of
    /// <c>MergeSpanInto</c> at all: <c>SpanWriter.Write</c> calls <c>TraceSummarySidecar.Write</c>,
    /// which resolves the method and path ONCE at flush time into <c>TraceSummary.RootMethod</c>
    /// and <c>RootPath</c>, and every later page reads those strings back through
    /// <c>TraceStorageEngine.MergeSummaryInto</c>. The sidecar kept a THIRD private copy of the
    /// semconv key lists, so a client span whose path arrived as <c>url.full</c> or <c>http.url</c>
    /// had a path in the trace list while its tier was hot and an empty one from the flush onward —
    /// the same row, on the same screen, changing its mind on a background timer.
    ///
    /// <para>Give <c>TraceSummarySidecar</c> its own three-key path list back and the second assert
    /// fails with an empty string against the URL. The <c>.tracesum</c> precondition is what keeps
    /// that honest: with no sidecar the cold read falls back to scanning spans through
    /// <c>MergeSpanInto</c>, which shares the lists already and would pass either way.</para>
    /// </summary>
    [Theory]
    [InlineData("url.full", "https://api.example.com/v1/payments?id=7")]
    [InlineData("http.url", "https://api.example.com/v1/refunds")]
    public async Task The_trace_list_reports_the_same_path_once_the_tier_has_flushed(string key, string url)
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-flushparity-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);

        using var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        var  at       = ColdSpanSegmentFixture.Base;
        long baseNano = at.ToUnixTimeMilliseconds() * 1_000_000L;

        engine.WriteSpan(new SpanIngestItem
        {
            TraceId           = new TraceId(0xF105ED, 1),
            SpanId            = new SpanId(1),
            ParentSpanId      = default,
            StartTimeUnixNano = baseNano,
            DurationNanos     = 5_000_000_000L,
            Name              = "GET",
            ServiceName       = "checkout",
            Kind              = SpanKind.Client,
            Status            = SpanStatusCode.Unset,
            HttpStatusCode    = 200,
            AttributesBytes   = OneAttr(key, url),
        });

        var from = at.AddMinutes(-1);
        var to   = at.AddDays(1);

        var    hotList = await engine.GetTraceListAsync(from, to, null, null, null, null, null, 10);
        string hot     = Assert.Single(hotList.Rows).HttpPath;

        engine.FlushHotTier();   // the segment and its .tracesum, written on this thread

        // THE PRECONDITION, ASSERTED AND NOT ASSUMED: a segment with no sidecar is read span by
        // span through MergeSpanInto, which is the hot reader again and would prove nothing here.
        Assert.NotEmpty(Directory.GetFiles(dir, "*.tracesum", SearchOption.AllDirectories));

        var    coldList = await engine.GetTraceListAsync(from, to, null, null, null, null, null, 10);
        string cold     = Assert.Single(coldList.Rows).HttpPath;

        _out.WriteLine($"{key,-10} hot \"{hot}\"   flushed \"{cold}\"");

        Assert.Equal(url, hot);
        Assert.Equal(hot, cold);
    }

    /// <summary>A one-key attribute map.</summary>
    private static byte[] OneAttr(string key, string value)
    {
        var buf = new ArrayBufferWriter<byte>(128);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write(key); w.Write(value);
        w.Flush();
        return buf.WrittenMemory.ToArray();
    }

    /// <summary>
    /// What two per-row semconv key arrays cost — the allocation TS#13 removed, weighed on the
    /// runtime the gate is about to run on and against the key lists as they stand TODAY.
    ///
    /// <para><c>Escape</c> is not inlined, so the two arrays escape the loop exactly as
    /// <c>BuildRow</c>'s did into <c>GetAttr</c>; a runtime that stack-allocates a non-escaping
    /// <c>string[]</c> would otherwise weigh nothing and the calibration would read zero. The
    /// <c>Assert.InRange</c> at the call site is what catches that if it ever does.</para>
    /// </summary>
    private static double MeasureTwoKeyArraysPerRow(int rows)
    {
        // THE LENGTHS ARE READ, NOT WRITTEN. A `params string[]` restored on GetAttr would take
        // BuildRow's literal arguments — which are the semconv lists — so the arrays it built per
        // row are exactly these two lengths, whatever they are today. Written out as literals this
        // measured a string[3] path array against the real five-key list and understated the
        // defect by 16 B a row; grow PathKeys again and a hard-coded calibration understates it
        // again, silently, in the direction that loosens the gate. Only the element COUNT is the
        // allocation, so nothing here needs the key strings themselves.
        int methodLen = HttpSemconvKeys.MethodKeys.Length;   // 2
        int pathLen   = HttpSemconvKeys.PathKeys.Length;     // 5

        Escape(new string[methodLen], new string[pathLen]);   // type handles and this frame, not the figure

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < rows; i++)
            Escape(new string[methodLen], new string[pathLen]);
        return (GC.GetAllocatedBytesForCurrentThread() - before) / (double)rows;
    }

    private static int _sink;

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void Escape(string[] method, string[] path) => _sink = method.Length + path.Length;

    /// <summary>An HTTP server root span's attribute map: the two keys <c>BuildRow</c> asks for.</summary>
    private static byte[] HttpRootBlob()
    {
        var buf = new ArrayBufferWriter<byte>(64);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("http.request.method"); w.Write("GET");
        w.Write("url.path");            w.Write("/api/v1/payments");
        w.Flush();
        return buf.WrittenMemory.ToArray();
    }

    /// <summary>A blob that will not decode answers UNKNOWN, never "no" — issue #74's rule, on bytes.</summary>
    [Fact]
    public void An_unreadable_blob_cannot_answer_either_way()
    {
        var torn = EveryValueShape()[..12];
        var span = SpanWith(torn, dictionary: false);

        Assert.Null(span.Attributes);
        Assert.Null(TraceQLParser.Parse("{ .db.system = \"mssql\" }").Evaluate(span));
        Assert.Null(TraceQLParser.Parse("{ !(.db.system = \"mssql\") }").Evaluate(span));

        // Presence is the one question a blob can still be wrong about cheaply, so it is pinned:
        // an unreadable map holds nothing anybody can name.
        Assert.False(TraceQLParser.Parse("{ .db.system != nil }").Evaluate(span));
    }

    private static string Show(bool? b) => b?.ToString() ?? "unknown";

    /// <summary>One map holding every shape a msgpack attribute value can take.</summary>
    private static byte[] EveryValueShape()
    {
        var buf = new ArrayBufferWriter<byte>(512);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(11);
        w.Write("db.system");    w.Write("mssql");
        w.Write("name.unicode"); w.Write("Ünïcödé");
        w.Write("net.peer.port"); w.Write(1433L);         // uint16 encoding
        w.Write("thread.id");    w.Write(63L);            // positive fixint
        w.Write("retry.delta");  w.Write(-100L);          // int8
        w.Write("cpu.ratio");    w.Write(0.1);
        w.Write("cache.hit");    w.Write(true);
        w.Write("tenant.id");    w.WriteNil();
        w.Write("numeric.text"); w.Write("42");
        w.Write("tags");         w.WriteArrayHeader(2); w.Write("a"); w.Write("b");
        w.Write("peer.info");    w.WriteMapHeader(1); w.Write("host"); w.Write("sql-03");
        w.Flush();

        // The resource-then-span shadowing the OTLP mapper relies on: the same key twice, and the
        // SECOND one is the answer.
        var outer = new ArrayBufferWriter<byte>(buf.WrittenCount + 64);
        var ow    = new MessagePackWriter(outer);
        ow.WriteMapHeader(13);
        ow.WriteRaw(buf.WrittenSpan[1..]);   // drop the inner map header, keep its 11 pairs
        ow.Write("shadowed"); ow.Write("from-resource");
        ow.Write("shadowed"); ow.Write("from-span");
        ow.Flush();
        return outer.WrittenMemory.ToArray();
    }

    private static SpanRecord SpanWith(byte[] blob, bool dictionary) => dictionary
        ? new SpanRecord
        {
            TraceId = new TraceId(1, 2), SpanId = new SpanId(3),
            Name = "SELECT payments", ServiceName = "billing",
            Kind = SpanKind.Client, Status = SpanStatusCode.Ok,
            Attributes = SpanAttributeBlob.Decode(blob),
        }
        : new SpanRecord
        {
            TraceId = new TraceId(1, 2), SpanId = new SpanId(3),
            Name = "SELECT payments", ServiceName = "billing",
            Kind = SpanKind.Client, Status = SpanStatusCode.Ok,
            AttributesBytes = blob,
        };
}
