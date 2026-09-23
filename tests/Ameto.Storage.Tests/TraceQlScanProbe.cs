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

    /// <summary>
    /// How many times every per-thread allocation figure in this class is taken. The verdict is the
    /// SMALLEST of them: the counter's only noise term is a GC landing inside the window, which
    /// adds this thread's unused allocation context to the reading and can never subtract.
    /// </summary>
    private const int MeasuredPasses = 3;

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
        //
        // NO SAME-RUN CONTROL HERE, AND THE DENOMINATOR IS WHY. The row-building term the gate
        // below has to subtract — a trace id and a span id per RETURNED ROW, 144 B or 216 B
        // depending on whether the runtime is running an optimised body of
        // DefaultInterpolatedStringHandler.AppendFormatted<ulong> — arrives on this figure divided
        // by the 50 000 spans SCANNED for 200 rows: 0,29 B per span of swing, next to xUnit's
        // ~3 B/span of output drain and a 320 B margin. Measured 380 and 381 B/span scanned with
        // and without DOTNET_ReadyToRun=0.
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
    /// <para>THE THIRD PAGE IS THE FIRST MEASURED ONE. <c>BuildRow</c> reaches the row's attributes
    /// through <c>SpanRecord.Attributes</c>, whose decode is memoised ON THE RECORD, and the hot
    /// tier hands out the same records to every query — so the first page pays a decode per row
    /// and the 104 B this test exists for would be noise inside it. Two warm pages leave a page
    /// whose per-row cost is <c>BuildRow</c>'s own objects and nothing else.</para>
    ///
    /// <para>NOTHING IN THE VERDICT IS INHERITED FROM THE RUN. Two terms of the raw figure are the
    /// process's history and not the code's, and each is dealt with where it lives: a GC inside the
    /// measured window (worth +384 B, and it landed in one pass of every eight-pass Debug suite run
    /// it was measured over) can only ADD, so every figure is the best of
    /// <see cref="MeasuredPasses"/>; and the row's two id strings cost
    /// 144 B/row or 216 B/row depending on whether the runtime has an optimised body for the
    /// interpolated-string handler (worth +72 000 B, seen in 1 full Debug suite run in 5 and in
    /// every measured pass of it), so they are measured IMMEDIATELY BEFORE AND AFTER EVERY PASS
    /// and subtracted from that pass — and a pass across which the runtime changed its mind is not
    /// read at all. The gate's own comment carries the measurements.</para>
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
        // Over twelve consecutive pages run ALONE it reads the SAME value to the byte —
        // 1 018 368 B Release, 1 018 976 B Debug — and with DOTNET_PROCESSOR_COUNT=2 as well.
        //
        // IT IS NOT, HOWEVER, INDEPENDENT OF THE RUN, and an earlier version of this comment said
        // it was. Inside the whole suite the raw figure has two further terms, both of them the
        // process's history rather than the page's code; the two blocks below are what removes
        // them, and each carries what it was measured at.
        //
        // THREE MEASURED PAGES AND THE SMALLEST IS THE READING, because the per-thread counter has
        // one noise term of its own and it is strictly additive. A GC that lands INSIDE the window
        // makes the counter jump by whatever was left unused in this thread's allocation context:
        // the remainder is turned into a free object and the context zeroed, so the
        // `alloc_bytes − (alloc_limit − alloc_ptr)` the counter computes loses its subtrahend.
        // Measured here: every pass with `GC.CollectionCount(0)` unchanged reads 1 018 976 B to the
        // byte, and a pass that took one gen0 collection read 1 019 360 — +384 B, once in each of
        // three eight-pass full Debug suite runs and in a different pass every time. It cannot be
        // predicted and it cannot be subtracted, but it can only ever ADD, so a minimum over a few
        // passes is exactly the reading with no GC in it. The pass that took one is printed as
        // such, so a run whose minimum is not a clean pass says so rather than hiding it.
        //
        // A MINIMUM IS ONLY HONEST FOR A COST THAT IS PAID ON EVERY PAGE, and this one is: two
        // `params string[]` per row are built afresh by every call of BuildRow. Its sibling
        // TraceHotTierProbe.A_trace_list_page_does_not_inflate_the_hot_tier_it_walks must NOT be
        // given this treatment for the opposite reason — the decode it gates is memoised on the
        // record, so it is paid on the FIRST page only and a minimum over pages would read a later
        // page and see nothing.
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
        //
        // EVERY PASS CARRIES ITS OWN ID-STRING CONTROL, taken immediately before it and immediately
        // after it, and a pass is read only when the two agree. Why the row's two id strings are
        // subtracted at all is the long note above the gate below; why the control brackets EACH
        // pass instead of being taken once after all of them is that the term it cancels is the
        // runtime's CURRENT choice of code for the interpolation handler, and the runtime can change
        // that choice at any moment — a promotion is decided and installed off this thread. A
        // control taken after the pages cancelled what the handler cost when the CONTROL ran, not
        // what it cost when the page ran: promote the handler between the two and the unchanged
        // tree read 1 035 B/row against a gate of 1 015 (forced under DOTNET_ReadyToRun=0 by
        // spinning TraceId.ToString until the boxes stopped, right after the page loop: every page
        // pass boxed at 1 179 408 B, the control unboxed at 144 B/row), and the message blamed
        // BuildRow's key lists for it.
        //
        // Bracketed, a promotion can only land INSIDE one pass's brackets or BETWEEN passes. Inside,
        // the two brackets disagree — by 72 B/row when it lands in the page — and that pass is not
        // read; between, both neighbours are consistent. No sequence of promotions takes the handler
        // from boxing to not boxing and back (tier 1 is final), so no pass can see it boxed on both
        // sides and unboxed in the middle: a pass that is read can over-state its row, never
        // under-state it. BracketTolerance absorbs what a GC landing inside a bracket adds to it;
        // the smaller bracket is the one without that GC, and it is the one subtracted. (A GC in
        // BOTH brackets of one pass could under-state that pass by at most the tolerance — 8 B
        // against a 52 B margin and a 104 B defect.)
        const int    MaxPasses        = MeasuredPasses + 3;   // a promotion spoils at most one pass
        const double BracketTolerance = 8;                    // B/row: 20× the measured +384 B of a GC in a window
        long           allocated = long.MaxValue;
        double         perRow    = double.MaxValue;
        double         idsPerRow = double.NaN;
        int            steady    = 0;
        bool           ranHere   = true;
        TraceQueryPage page      = default;
        for (int pass = 0; pass < MaxPasses && steady < MeasuredPasses; pass++)
        {
            double idsBefore = MeasureRowIdStringsPerRow(Rows);
            int    gen0      = GC.CollectionCount(0);
            long   before    = GC.GetAllocatedBytesForCurrentThread();
            var    pageTask  = TraceQLExecutor.ExecuteAsync(engine, pred, from, to, Rows, CancellationToken.None);
            ranHere         &= pageTask.IsCompleted;   // read BEFORE the await: nothing has resumed yet
            page             = await pageTask;
            long   a         = GC.GetAllocatedBytesForCurrentThread() - before;
            int    gcs       = GC.CollectionCount(0) - gen0;
            double idsAfter  = MeasureRowIdStringsPerRow(Rows);

            bool   agree = Math.Abs(idsAfter - idsBefore) <= BracketTolerance;
            double ids   = Math.Min(idsBefore, idsAfter);
            double net   = a / (double)Rows - ids;
            _out.WriteLine($"              pass {pass}: {a:N0} B ({a / (double)Rows:N0} B/row), id strings "
                         + $"{idsBefore:N0} -> {idsAfter:N0} B/row, row {net:N0} B"
                         + (gcs > 0 ? $"   ({gcs} gen0 collection(s) inside the window)" : "")
                         + (agree ? "" : "   (the handler changed code across this pass: not read)"));
            if (!agree) continue;

            steady++;
            if (net < perRow) { perRow = net; allocated = a; idsPerRow = ids; }
        }

        Assert.True(ranHere,
            "the TraceQL page did not complete synchronously, so the per-thread figure below is "
            + "only the part of it that ran on this thread. This probe measures a HOT-TIER page, "
            + "which has no yielding await; if the hot-tier path has gained one, measure the page "
            + "with GC.GetTotalAllocatedBytes minus a baseline idle sample rather than widening "
            + "the gate — and note that ITestOutputHelper.WriteLine costs 6 288 B on another "
            + "thread per printed line, so that baseline has to be taken the same way.");

        Assert.True(steady > 0,
            $"the id strings' cost changed across every one of {MaxPasses} measured passes, so no pass "
            + "can be read net of them. A promotion of the interpolation handler spoils one pass; "
            + "every pass spoiled means the control itself is unstable — find out why before "
            + "touching the gate.");

        _out.WriteLine($"TRACEQL PAGE  {Rows:N0} rows, one root span each, warm, best of "
                     + $"{steady} steady pass(es): {allocated:N0} B ({allocated / (double)Rows:N0} B/row)");

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

        // WHAT THE ROW'S TWO ID STRINGS COST, MEASURED AROUND THE SAME PASS, because that is the one
        // part of the figure the RUNTIME gets to choose — and it chose differently often enough to
        // turn this gate red on an unchanged tree.
        //
        // THE FAILURE. One full Debug suite run in five read 1 090 976 B where every other run,
        // and every run of this class alone, read 1 018 976 B to the byte: +72 000 B, exactly
        // 72 B on every one of the thousand rows, present in EVERY measured pass of that run and
        // in none of another's. The 72 is three boxed `ulong`s at 24 B each, and the three are
        // BuildRow's `root.TraceId.ToString()` (two holes) and `root.SpanId.ToString()` (one):
        // both are `$"{_hi:x16}{_lo:x16}"`, and `DefaultInterpolatedStringHandler.AppendFormatted<T>`
        // tests `value is IFormattable` / `is ISpanFormattable` on its generic argument. Compiled
        // OPTIMISED — the ReadyToRun body in System.Private.CoreLib, or a tier-1 rejit — the JIT
        // folds those type tests against the known `ulong` and the box disappears. Compiled
        // UNOPTIMISED — tier-0, which is where an instantiation lives until the runtime promotes
        // it — the box is a real 24-byte allocation per hole.
        //
        // Measured on this tree, same binaries, same machine, per-thread counter:
        //   optimised body     TraceId.ToString 88 B/row   SpanId.ToString 56 B/row   page 1 018 976 B
        //   unoptimised body   TraceId.ToString 136 B/row  SpanId.ToString 80 B/row   page 1 090 976 B
        // (Pinned deterministically with DOTNET_ReadyToRun=0, which denies the instantiation its
        // precompiled body; the second column reproduces the failing run's figure to the byte, and
        // does so in EVERY pass, which is why the minimum above cannot help here — the term is
        // state, not warm-up. It also arrives unprompted: 1 of the 10 full Debug suite runs this
        // change was verified over read the raw 1 090 976 B with the control at 216 B/row, and the
        // verdict below still came out at 874,976 B/row and green.)
        //
        // SO THE GATE MEASURES THE ROW WITHOUT THEM. The page builds exactly one TraceId string and
        // one SpanId string per row, so subtracting what that pair costs AROUND THE PASS removes both
        // the strings and whatever the runtime decided to box around them (the loop above says why
        // around the pass and not merely in the same run). What is left is the
        // row's own TraceRowDto, service set and services array: 874,976 B/row in Debug and
        // 874,368 B/row in Release — and 874,976 again with the boxes present, identical to the
        // byte, which is the check that the two ToString calls are the whole of the variable term.
        //
        // The margin is unchanged by the subtraction, because both ends move together: the gate is
        // the baseline + 52, sitting 52 B above the measured baseline and 52 B below the defect
        // page, which is that baseline plus the 104 B the two params arrays weigh.
        //
        // THE BASELINE MOVED FROM 875 TO 963 WHEN BuildRow LEFT THE MEMOISED DICTIONARY, and the
        // 88 B is the whole of the move — the two strings a blob walk builds per page
        // (Encoding.UTF8.GetString of "GET" and of "/api/v1/payments", 24 + 56 = 80 B, plus the
        // walk's own rounding), where the memoised dictionary handed back the SAME two instances
        // it had decoded on page 1. That is the stated half of the trade and the reason the gate
        // for this item is an ALLOCATION gate while the one for that item is a RETENTION gate:
        // A_traceql_page_does_not_inflate_the_hot_tier_it_paged_over measures 368 B per root span
        // left on the tier where the dictionary left 1 545, i.e. 1 177 B of permanent tier memory
        // bought for 88 B of per-page allocation.
        _out.WriteLine($"              a row's two id strings weigh {idsPerRow:N0} B/row around the pass read "
                     + $"({(idsPerRow >= 200 ? "boxed: the handler is running unoptimised" : "unboxed")})");
        Assert.InRange(idsPerRow, 144, 400);   // 144 is the two strings themselves; anything less is a mis-measurement

        const double Baseline = 963;   // the Debug figure less the id strings, rounded up to the byte
        double gate   = Baseline + defectPerRow / 2;

        Assert.True(perRow < gate,
            $"a returned row cost {perRow:N0} B beyond its two id strings, against a gate of "
            + $"{gate:N0} — BuildRow is building its semconv key lists per row again (two params "
            + $"string[] is {defectPerRow:N0} B a row)");
    }

    /// <summary>
    /// A TRACEQL PAGE DOES NOT INFLATE THE HOT TIER IT PAGED OVER.
    ///
    /// <para><c>BuildRow</c> read its row's method and path through
    /// <see cref="SpanRecord.Attributes"/>, which on a hot-tier record is the FIRST touch of its
    /// msgpack blob: the lazy decode runs right there and builds a <c>Dictionary</c>, a key
    /// string and a box per attribute — ~987 B against the blob's 375 B for an ordinary
    /// eight-attribute span — and MEMOISES it on the record. The records belong to the TIER, not
    /// to the page, so the page's dictionaries outlive it and the tier stays that much heavier
    /// until it flushes. This is the very thing <c>TraceStorageEngine.SetHttpAttrs</c> was
    /// rewritten to stop doing for the trace list (<c>TraceHotTierProbe.A_trace_list_page_does_
    /// not_inflate_the_hot_tier_it_walks</c>) — over the same records of the same tier, so every
    /// TraceQL page put straight back what that item had taken away.</para>
    ///
    /// <para>RETAINED IS THE ASSERTION HERE, and it is the right one BECAUSE the cost is
    /// memoised: the decode is paid on the first page and never again, so a per-page allocation
    /// figure reads whatever page it happens to measure while the retention is permanent. It is
    /// measured against a baseline taken with the tier already built and already warmed — the
    /// spans, their blobs and the whole page path are live on both sides — so the delta is what
    /// the PAGE left behind and nothing else. Both samples take a compacting gen2 collect, or a
    /// tier's own fragmentation reads as the page's retention.</para>
    ///
    /// <para>Revert <c>BuildRow</c> to <c>GetAttr(root.Attributes, …)</c> and this fails: 368 B
    /// per root span becomes 1 545 B, which is 1 000 memoised eight-entry dictionaries the tier
    /// cannot give back until it flushes.</para>
    /// </summary>
    [Fact]
    public async Task A_traceql_page_does_not_inflate_the_hot_tier_it_paged_over()
    {
        const int Rows = 1_000;

        string dir = Path.Combine(Path.GetTempPath(), "ameto-qlretain-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);

        using var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        var  baseAt   = ColdSpanSegmentFixture.Base;
        long baseNano = baseAt.ToUnixTimeMilliseconds() * 1_000_000L;
        var  attrs    = SqlRootBlob();

        for (int i = 0; i < Rows; i++)
            engine.WriteSpan(new SpanIngestItem
            {
                TraceId           = new TraceId(0xC0FFEE, (ulong)(i + 1)),
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

        // WARM ON A DIFFERENT TIER, so the jitting of several hundred lines of first-call-in-the-
        // process page code is not billed to the measured tier's retention.
        await WarmPageAsync(pred);

        long liveBefore = LiveBytes();

        var page = await TraceQLExecutor.ExecuteAsync(engine, pred, from, to, Rows, CancellationToken.None);

        int    rows   = page.Rows.Count;
        string method = page.Rows[0].HttpMethod, path = page.Rows[0].HttpPath;
        page = default;                       // the rows belong to the caller, not to the tier

        long retained = LiveBytes() - liveBefore;
        GC.KeepAlive(engine);

        _out.WriteLine("");
        _out.WriteLine($"TRACEQL PAGE RETENTION  {Rows:N0} root spans, 8 attributes each, one page");
        _out.WriteLine($"  left on the tier  {retained / 1024.0,10:N1} KB   {retained / (double)Rows,8:N0} B/root span");

        Assert.Equal(Rows, rows);
        Assert.Equal("GET", method);
        Assert.Equal("/api/v1/payments", path);

        // THE GATE, AND THE RESIDUAL IT SITS ABOVE. A page over the blob does not retain NOTHING:
        // measured here at 368 B a root span, which is the same order as the 498 B
        // TraceHotTierProbe reports for the trace list on the same path (the tier's own lazily
        // built per-span state, not a decoded map) and is what a blob page costs. Reverting
        // BuildRow to `GetAttr(root.Attributes, …)` measures 1 545 B a root span in the same run
        // — the 987 B memoised dictionary on top of that residual. 700 B sits between the two
        // with ~90 % headroom above today's figure and better than 2x below the defect.
        Assert.True(retained < Rows * 700L,
            $"a TraceQL page left {retained / (double)Rows:N0} B per root span on the hot tier — "
          + "BuildRow is decoding and memoising the attribute dictionary again");
    }

    /// <summary>A throwaway tier of the same shape, purely to jit the page path.</summary>
    private async Task WarmPageAsync(SpanPredicate pred)
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-qlwarm-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);

        using var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        var  at       = ColdSpanSegmentFixture.Base;
        long baseNano = at.ToUnixTimeMilliseconds() * 1_000_000L;
        var  attrs    = SqlRootBlob();

        for (int i = 0; i < 200; i++)
            engine.WriteSpan(new SpanIngestItem
            {
                TraceId           = new TraceId(0xBEEF, (ulong)(i + 1)),
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

        for (int i = 0; i < 2; i++)
            _ = await TraceQLExecutor.ExecuteAsync(
                engine, pred, at.AddMinutes(-1), at.AddDays(1), 200, CancellationToken.None);
    }

    private static long LiveBytes()
    {
        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        return GC.GetTotalMemory(forceFullCollection: false);
    }

    /// <summary>
    /// The eight-attribute SqlClient span this round is measured on, with the two semconv keys a
    /// row reads sitting where a real server span puts them — LAST, after the resource
    /// attributes, which is also what makes the walk do its whole job.
    /// </summary>
    private static byte[] SqlRootBlob()
    {
        var buf = new ArrayBufferWriter<byte>(512);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(8);
        w.Write("db.system");            w.Write("mssql");
        w.Write("db.name");              w.Write("payments");
        w.Write("db.statement");         w.Write("SELECT id, amount, status FROM payments WHERE tenant = @p0");
        w.Write("net.peer.name");        w.Write("sql-primary.internal");
        w.Write("net.peer.port");        w.Write(1433);
        w.Write("server.address");       w.Write("node-3");
        w.Write("http.request.method");  w.Write("GET");
        w.Write("url.path");             w.Write("/api/v1/payments");
        w.Flush();
        return buf.WrittenMemory.ToArray();
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
        // Best of MeasuredPasses, for the same reason the page above is: a gen0 collection inside
        // the window adds this thread's unused allocation context to the reading and can only add.
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

        long best = long.MaxValue;
        for (int pass = 0; pass < MeasuredPasses; pass++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < rows; i++)
                Escape(new string[methodLen], new string[pathLen]);
            long a = GC.GetAllocatedBytesForCurrentThread() - before;
            if (a < best) best = a;
        }
        return best / (double)rows;
    }

    /// <summary>
    /// WHAT A ROW'S TWO ID STRINGS COST ON THIS RUNTIME, IN THIS PROCESS, RIGHT NOW — the 32-char
    /// trace id and the 16-char span id <c>BuildRow</c> writes into every <c>TraceRowDto</c>.
    ///
    /// <para>88 + 56 = 144 B of string, plus 0 or 72 B of boxing depending on whether the runtime
    /// is running an optimised body of <c>DefaultInterpolatedStringHandler.AppendFormatted&lt;ulong&gt;</c>
    /// — see the gate's comment for the measurement and for why that is not this test's business to
    /// judge.</para>
    ///
    /// <para>ONE PASS, NOT A BEST OF SEVERAL: it is a bracket, and what a bracket must report is the
    /// handler's code at the edge of the page it sits next to. A minimum over several passes would
    /// read whichever end of a promotion was cheaper. A gen0 collection inside it can only inflate
    /// it, which the caller handles by comparing the two brackets and subtracting the smaller.</para>
    ///
    /// <para><c>Escape</c> keeps the two strings alive past the loop body, exactly as the row they
    /// are written into would.</para>
    /// </summary>
    private static double MeasureRowIdStringsPerRow(int rows)
    {
        var trace = new TraceId(0x5EED, 1);
        var span  = new SpanId(1);
        Escape(trace.ToString(), span.ToString());   // jit, and the interpolation handler's pooled buffer

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < rows; i++)
            Escape(trace.ToString(), span.ToString());
        return (GC.GetAllocatedBytesForCurrentThread() - before) / (double)rows;
    }

    private static int _sink;

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void Escape(string[] method, string[] path) => _sink = method.Length + path.Length;

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void Escape(string traceId, string spanId) => _sink = traceId.Length + spanId.Length;

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
