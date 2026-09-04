using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// ONE cold segment on disk, with the exact geometry and the exact span shape the OOM was
/// about, built once for the whole class.
///
/// <para>Written straight through <see cref="SpanWriter"/> rather than by pushing spans at the
/// engine and flushing. The engine's own flush scheduler decides segment boundaries — it trips
/// at <c>HotFlushThreshold</c> = 50,000 and a background LZ4-HC flush may or may not have
/// finished by any given write — so a fixture built that way has a segment count and a
/// per-segment size that depend on a race. Here the segment is exactly
/// <see cref="Spans"/> spans, exactly <c>ceil(Spans / 4096)</c> blocks, every run.</para>
///
/// <para>The span shape is the one that costs: a SqlClient client span with eight ordinary OTel
/// attributes. A record read out of this file weighs about 1.7 kilobytes — the dictionary, its
/// eight keys, its eight values — against the ~96 bytes an attribute-less record weighs. A
/// fixture that writes <c>AttributesBytes = []</c> defends a shape twenty times lighter than the
/// one that killed the server.</para>
///
/// <para>The per-span figure is MEASURED by the tests below and printed, not asserted from a
/// constant; 1,749 B is what it reads on the reference machine. Every megabyte quoted anywhere in
/// this file derives from it, so if the numbers here and in <c>SpanReader</c> ever disagree, the
/// test output is the one that is right.</para>
/// </summary>
public sealed class ColdSpanSegmentFixture : IDisposable
{
    /// <summary>Exactly <c>HotFlushThreshold</c>: the size of an ordinary flushed segment.</summary>
    public const int Spans = 50_000;

    /// <summary><c>SpanWriter.BlockSize</c>. The unit the streamed reader is allowed to hold.</summary>
    public const int BlockSize = 4096;

    public static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    /// <summary>Start time of span <c>i</c> — strictly increasing, 1 ms apart.</summary>
    public static long StartNano(int i) => Base.ToUnixTimeMilliseconds() * 1_000_000L + i * 1_000_000L;

    /// <summary>
    /// Duration of span <c>i</c>: 1 ms … 2 s, so <c>duration &gt; 1s</c> — the repro's own
    /// predicate — splits the segment almost exactly in half instead of matching everything.
    /// </summary>
    public static long DurationNanos(int i) => 1_000_000L * (1 + i % 2000);

    /// <summary>How many spans satisfy <c>duration &gt; 1s</c>.</summary>
    public static int OverOneSecond
    {
        get
        {
            int n = 0;
            for (int i = 0; i < Spans; i++) if (DurationNanos(i) >= 1_000_000_000L) n++;
            return n;
        }
    }

    public string Dir         { get; }
    public string SegmentPath { get; }

    public ColdSpanSegmentFixture()
    {
        Dir = Path.Combine(Path.GetTempPath(), "ameto-trcbound-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Dir);

        var corpus = new List<SpanRecord>(Spans);
        for (int i = 0; i < Spans; i++)
            corpus.Add(new SpanRecord
            {
                TraceId           = new TraceId(0x9E3779B97F4A7C15UL, (ulong)(i + 1)),
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = default,
                StartTimeUnixNano = StartNano(i),
                DurationNanos     = DurationNanos(i),
                Name              = "SELECT payments",
                ServiceName       = "billing",
                Kind              = SpanKind.Client,
                Status            = SpanStatusCode.Unset,
                HttpStatusCode    = 0,
                Attributes        = SqlClientAttributes(i),
            });

        SegmentPath = SpanWriter.Write(Dir, corpus).FilePath;

        // The corpus is as heavy as everything the tests are about to measure. Let it go
        // before any of them takes a baseline.
        corpus.Clear();
        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
    }

    /// <summary>
    /// The attributes an OpenTelemetry SqlClient instrumentation actually emits — the shape
    /// <c>{ .db.system = "mssql" &amp;&amp; duration &gt; 1s }</c> was asked about. Keys and
    /// values repeat across spans on purpose: they compress away on disk and are still
    /// allocated fresh per span on the way back, which is exactly what production does.
    /// </summary>
    public static IReadOnlyDictionary<string, object?> SqlClientAttributes(int i) =>
        new Dictionary<string, object?>(8, StringComparer.Ordinal)
        {
            ["db.system"]         = "mssql",
            ["db.name"]           = "payments",
            ["db.statement"]      = "SELECT TOP 100 Id, TenantId, Amount, CreatedUtc FROM dbo.Payments "
                                  + "WHERE TenantId = @p0 AND CreatedUtc >= @p1 ORDER BY CreatedUtc DESC",
            ["net.peer.name"]     = "sql-prod-03.svc.cluster.local",
            ["net.peer.port"]     = 1433L,
            ["http.route"]        = "/api/v1/tenants/{tenantId}/payments",
            ["thread.id"]         = (long)(i % 64),
            ["otel.library.name"] = "OpenTelemetry.Instrumentation.SqlClient",
        };

    /// <summary>Copies the segment and its sidecars into a private directory, so a test that
    /// stands an engine over them cannot disturb the shared fixture.</summary>
    public string CopySegmentToPrivateDir()
    {
        string dst  = Path.Combine(Path.GetTempPath(), "ameto-trcbound-e-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dst);
        string stem = Path.GetFileNameWithoutExtension(SegmentPath);
        foreach (var f in Directory.EnumerateFiles(Dir, stem + ".*"))
            File.Copy(f, Path.Combine(dst, Path.GetFileName(f)));
        return dst;
    }

    public void Dispose()
    {
        try { Directory.Delete(Dir, true); } catch { }
    }
}

/// <summary>
/// A span search must cost memory in proportion to what it RETURNS, not to what it matches —
/// and not to what the segment it is walking HOLDS.
///
/// <para>The two requirements pull against each other, which is how the bug got in. A segment
/// file is written oldest-first, so a search that simply streams it and stops at the limit
/// keeps the oldest matches and drops the newest — the caller then pages "newest first" over a
/// pool that is quietly the wrong end of the data. Ordering the segment's matches fixes that,
/// and the first version of the fix ordered them by buffering all of them. The second version
/// replaced the buffer with a bounded heap, which capped what a search RETAINED ACROSS segments
/// and left the peak INSIDE one segment exactly where it was: <c>SearchAsync</c> opened by
/// materialising every span of every admitted block into a <c>List</c> before the first
/// predicate ran.</para>
///
/// <para>A record read out of the fixture below weighs about 1.7 kilobytes: two strings, and an
/// attribute dictionary that the decoder built for EVERY span, not — as the comments then
/// claimed — whenever the query touched an attribute. At the 1,749 B the fixture measures, an
/// ordinary 50,000-span segment is therefore ~83 MB live and a compacted one
/// (<c>MaxSpansPerPass</c> = 200,000) ~334 MB. On a 512 MB server, on exactly the query this
/// bound was written for, one segment was still enough.</para>
///
/// <para>WHERE THE MEASUREMENTS ARE TAKEN. The peak lives inside <c>SpanReader.SearchAsync</c>,
/// and it is gone before <c>TraceStorageEngine.SearchSpansAsync</c> yields anything: the engine
/// drives the whole segment enumerator to completion into its bounded heap and only then hands
/// the first span up. Measuring at the engine's first yielded span therefore samples the heap
/// AFTER the peak has been released, which is why a test that did exactly that read 0.08 MB
/// against a reader that materialises whole files. These tests drive the reader directly and
/// sample repeatedly DURING the walk.</para>
/// </summary>
public sealed class SpanSearchBoundTests : IClassFixture<ColdSpanSegmentFixture>
{
    private const int Limit = 50;

    /// <summary>Samples are taken every this many yielded spans — twice per 4096-span block, so
    /// no block can be decoded and drained between two of them.</summary>
    private const int SampleEvery = 2048;

    private readonly ColdSpanSegmentFixture _fx;
    private readonly ITestOutputHelper      _out;

    public SpanSearchBoundTests(ColdSpanSegmentFixture fx, ITestOutputHelper output)
    {
        _fx  = fx;
        _out = output;
    }

    private IAsyncEnumerable<SpanRecord> ReaderSearch(long? minDurationNanos = null) =>
        SpanReader.SearchAsync(
            _fx.SegmentPath,
            fromNano:         ColdSpanSegmentFixture.StartNano(0) - 1,
            toNano:           ColdSpanSegmentFixture.StartNano(ColdSpanSegmentFixture.Spans) + 1,
            serviceName:      "billing",
            spanName:         null,
            status:           null,
            httpStatusCode:   null,
            minDurationNanos: minDurationNanos,
            maxDurationNanos: null,
            // The repro's attribute hint. It reaches the reader as a per-block bloom skip, so
            // this also keeps that path under the measurement rather than beside it.
            attrHints:        new[] { new AttrHint("db.system", "mssql") },
            ct:               CancellationToken.None);

    // ── The bound ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task The_peak_inside_one_segment_is_one_block_not_the_segment()
    {
        // Warm-up pass: JIT, and — the one that would otherwise be charged to the reading —
        // ArrayPool. A block buffer is rented on the first decode and stays in the pool
        // afterwards, so an unwarmed first pass reports the pool's growth as the reader's
        // live set.
        int warm = 0;
        await foreach (var _ in ReaderSearch()) warm++;
        Assert.Equal(ColdSpanSegmentFixture.Spans, warm);

        // What holding every match costs — the retention of the reader this replaced, measured
        // here rather than asserted from a constant, so the thresholds below are in this
        // machine's bytes and this fixture's span shape.
        long materialised = await LiveBytesHoldingEverything();

        // What the streamed reader's peak is, sampled throughout the walk into a bounded sink
        // shaped like the engine's.
        var (peak, matched) = await PeakLiveBytesWithBoundedSink(Limit);

        long perSpan  = materialised / ColdSpanSegmentFixture.Spans;
        long oneBlock = perSpan * ColdSpanSegmentFixture.BlockSize;

        _out.WriteLine($"segment          = {ColdSpanSegmentFixture.Spans:N0} spans, "
                     + $"{new FileInfo(_fx.SegmentPath).Length / 1048576.0:N1} MB on disk, "
                     + $"{(ColdSpanSegmentFixture.Spans + ColdSpanSegmentFixture.BlockSize - 1) / ColdSpanSegmentFixture.BlockSize} blocks");
        _out.WriteLine($"per span         = {perSpan:N0} B");
        _out.WriteLine($"one block        = {oneBlock / 1048576.0,8:N2} MB");
        _out.WriteLine($"whole segment    = {materialised / 1048576.0,8:N2} MB   <- peak of the materialising reader");
        _out.WriteLine($"streamed peak    = {peak / 1048576.0,8:N2} MB   <- peak of this one");
        _out.WriteLine($"                 = {(double)peak / oneBlock:N2} blocks, "
                     + $"{100.0 * peak / materialised:N1} % of the segment");

        Assert.Equal(ColdSpanSegmentFixture.Spans, matched);

        // THE SHAPE ASSERTION. Not "under N megabytes" — under a small multiple of ONE BLOCK,
        // in bytes this fixture measured. A reader that materialises the file peaks at
        // Spans/BlockSize = 12.2 blocks and fails here; one that streams peaks at one block
        // plus the page. The headroom is for the page, the pooled buffers and the enumerator,
        // not for a second block.
        Assert.True(peak < 3 * oneBlock,
            $"peak retention was {peak / 1048576.0:N2} MB = {(double)peak / oneBlock:N2} blocks " +
            $"({peak * 100.0 / materialised:N1}% of the whole segment) — the segment scan is " +
            "materialising more than one block at a time");

        // And the same fact from the other side, so a change in block geometry cannot quietly
        // turn "one block" into "the file".
        Assert.True(peak < materialised / 4,
            $"peak retention was {peak * 100.0 / materialised:N1}% of the whole segment");
    }

    [Fact]
    public async Task A_span_the_filter_rejects_does_not_get_an_attribute_dictionary()
    {
        // Allocation counters, not heap sampling: exact, and they see work that is freed again
        // before any GC could be asked about it. Retention cannot see this at all — the
        // dictionaries the old decoder built for rejected spans became garbage immediately.
        //
        // Both scans admit and fully decode EVERY block (same service, same bloom hint); they
        // differ only in how many spans survive the duration test.
        await Drain(ReaderSearch());                                   // warm

        long a0 = GC.GetTotalAllocatedBytes(precise: true);
        int all = await Drain(ReaderSearch());
        long allocAll = GC.GetTotalAllocatedBytes(precise: true) - a0;

        long b0 = GC.GetTotalAllocatedBytes(precise: true);
        int none = await Drain(ReaderSearch(minDurationNanos: 3_600_000_000_000L)); // an hour: nothing
        long allocNone = GC.GetTotalAllocatedBytes(precise: true) - b0;

        _out.WriteLine($"all {all:N0} matched : {allocAll  / 1048576.0,8:N2} MB allocated");
        _out.WriteLine($"none matched      : {allocNone / 1048576.0,8:N2} MB allocated");
        _out.WriteLine($"                    {100.0 * allocNone / allocAll:N1} % of the matching scan");

        Assert.Equal(ColdSpanSegmentFixture.Spans, all);
        Assert.Equal(0, none);

        // The decoder used to build the dictionary before anything could reject the span, so
        // the two scans allocated within a few percent of each other.
        Assert.True(allocNone < allocAll / 3,
            $"a scan that matched nothing allocated {allocNone / 1048576.0:N2} MB against the " +
            $"{allocAll / 1048576.0:N2} MB of one that matched everything — attributes are " +
            "still being decoded for spans the filter has already rejected");
    }

    // ── What the bound must not have cost ─────────────────────────────────────

    [Fact]
    public async Task Streaming_returns_exactly_what_reading_the_whole_file_and_filtering_would()
    {
        // The equivalence the peak fix has to preserve, checked against the one reader that
        // still materialises whole files. Same spans, same order, no exceptions for the
        // deferred attribute decode to hide behind.
        long floor = 1_000_000_000L; // duration >= 1s — the repro's predicate

        var expected = SpanReader.ReadAll(_fx.SegmentPath)
            .Where(s => s.ServiceName.Equals("billing", StringComparison.OrdinalIgnoreCase)
                     && s.DurationNanos >= floor)
            .Select(s => (s.TraceId, s.SpanId, s.StartTimeUnixNano, s.DurationNanos))
            .ToList();

        var actual = new List<(TraceId, SpanId, long, long)>();
        var kept   = new List<SpanRecord>();
        await foreach (var s in ReaderSearch(minDurationNanos: floor))
        {
            actual.Add((s.TraceId, s.SpanId, s.StartTimeUnixNano, s.DurationNanos));
            if (kept.Count < 4) kept.Add(s);
        }

        Assert.Equal(ColdSpanSegmentFixture.OverOneSecond, expected.Count);
        Assert.Equal(expected, actual);

        // A span that SURVIVES the filter still carries its attributes out — the skip must
        // apply to rejects only.
        Assert.NotEmpty(kept);
        foreach (var s in kept)
        {
            Assert.NotNull(s.Attributes);
            Assert.Equal(8, s.Attributes!.Count);
            Assert.Equal("mssql", s.Attributes["db.system"]);
            Assert.Equal(1433L, s.Attributes["net.peer.port"]);
        }
    }

    [Fact]
    public async Task A_page_is_the_NEWEST_matches_even_when_the_segment_holds_far_more()
    {
        string dir = _fx.CopySegmentToPrivateDir();
        try
        {
            using var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
            engine.LoadColdSegments();

            var page = new List<SpanRecord>();
            await foreach (var s in Search(engine, Limit)) page.Add(s);

            Assert.Equal(Limit, page.Count);

            // The newest `Limit` spans are the last written, and they must come back newest-first.
            long newest = ColdSpanSegmentFixture.StartNano(ColdSpanSegmentFixture.Spans - 1);
            for (int i = 0; i < Limit; i++)
                Assert.Equal(newest - i * 1_000_000L, page[i].StartTimeUnixNano);
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    [Fact]
    public async Task The_page_does_not_grow_with_the_number_of_matches()
    {
        string dir = _fx.CopySegmentToPrivateDir();
        try
        {
            using var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
            engine.LoadColdSegments();

            // The limit is the contract. Without it holding, "load more" walks the client into
            // the same allocation the server just died on.
            int one = 0;
            await foreach (var _ in Search(engine, 1)) one++;
            Assert.Equal(1, one);

            int seven = 0;
            await foreach (var _ in Search(engine, 7)) seven++;
            Assert.Equal(7, seven);
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    private static IAsyncEnumerable<SpanRecord> Search(TraceStorageEngine engine, int limit) =>
        engine.SearchSpansAsync(
            from:        ColdSpanSegmentFixture.Base.AddMinutes(-5),
            to:          ColdSpanSegmentFixture.Base.AddDays(7),
            serviceName: "billing",
            limit:       limit);

    // ── Measurement ───────────────────────────────────────────────────────────

    private static async Task<int> Drain(IAsyncEnumerable<SpanRecord> src)
    {
        int n = 0;
        await foreach (var _ in src) n++;
        return n;
    }

    /// <summary>
    /// Live bytes with every yielded record held — what the materialising reader retained while
    /// the caller enumerated, and the yardstick the assertions are expressed in.
    /// </summary>
    private async Task<long> LiveBytesHoldingEverything()
    {
        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        long baseline = GC.GetTotalMemory(forceFullCollection: true);

        var held = new List<SpanRecord>(ColdSpanSegmentFixture.Spans);
        await foreach (var s in ReaderSearch()) held.Add(s);

        long live = GC.GetTotalMemory(forceFullCollection: true) - baseline;
        GC.KeepAlive(held);
        return live;
    }

    /// <summary>
    /// The MAXIMUM live set observed during the walk, with the records draining into a bounded
    /// top-K exactly as <c>TraceStorageEngine</c>'s segment loop drains them.
    ///
    /// <para>Sampled repeatedly, because a single reading anywhere after the walk finishes is
    /// worthless here: the whole point is that a materialising reader is only expensive WHILE
    /// it is being enumerated.</para>
    /// </summary>
    private async Task<(long Peak, int Matched)> PeakLiveBytesWithBoundedSink(int limit)
    {
        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        long baseline = GC.GetTotalMemory(forceFullCollection: true);

        var  top  = new PriorityQueue<SpanRecord, long>(limit);
        long peak = 0;
        int  n    = 0;

        await foreach (var s in ReaderSearch())
        {
            if (top.Count < limit) top.Enqueue(s, s.StartTimeUnixNano);
            else if (top.TryPeek(out _, out long oldest) && s.StartTimeUnixNano > oldest)
                top.EnqueueDequeue(s, s.StartTimeUnixNano);

            if (++n % SampleEvery == 0)
            {
                long live = GC.GetTotalMemory(forceFullCollection: true) - baseline;
                if (live > peak) peak = live;
            }
        }

        GC.KeepAlive(top);
        return (peak, n);
    }
}
