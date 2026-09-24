using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// TI#5: THE HOT TIER HOLDS ONE STRING PER DISTINCT SPAN NAME AND SERVICE, and the pools that make
/// it so are bounded, shed with every flush, and never the reason a span is lost.
///
/// <para>Every span used to carry the fresh string the parser built for it — a route template
/// repeated across essentially every span of a batch — and every request its own copy of the
/// service name, all retained for the tier's life. <c>TraceSpanInternProbe</c> prices it; these
/// facts hold the three properties the reviewer has to be able to rely on.</para>
/// </summary>
public sealed class TraceSpanInternTests : IDisposable
{
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-tintern-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long              _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public TraceSpanInternTests(ITestOutputHelper output)
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

    [Fact]
    public void Spans_that_arrive_with_their_own_copies_share_one_string_per_name_and_service()
    {
        using var e = new TraceStorageEngine(Dir("share"), NullLogger<TraceStorageEngine>.Instance);

        // A fresh instance per span for the name, and per 50 spans for the service — the shape a
        // parser hands over (one service string per resource block, a new block per request).
        string service = Fresh("billing");
        for (int i = 0; i < 500; i++)
        {
            if (i % 50 == 0) service = Fresh("billing");
            e.WriteSpan(Span(i, Fresh(i % 2 == 0 ? "GET /orders/{id}" : "SELECT orders"), service));
        }

        var hot = e.HotSpansForTest;
        Assert.Equal(500, hot.Count);

        var even = hot[0].Name;
        var odd  = hot[1].Name;
        var svc  = hot[0].ServiceName;
        for (int i = 0; i < hot.Count; i++)
        {
            Assert.Same(i % 2 == 0 ? even : odd, hot[i].Name);
            Assert.Same(svc, hot[i].ServiceName);
        }
        Assert.Equal("GET /orders/{id}", even);
        Assert.Equal("SELECT orders", odd);
        Assert.Equal(0, e.PoolsForTest.UnpooledNames);
    }

    /// <summary>
    /// A FULL POOL NEVER DROPS A SPAN. Eight names fit the pool; the other forty-two distinct names
    /// keep their own strings, every span is in the tier under its own name, and the tier's byte
    /// budget is charged for the strings the pool could not share.
    /// </summary>
    [Fact]
    public void A_saturated_name_pool_keeps_every_span_under_its_own_name()
    {
        var pools = new SpanStringPools(maxNames: 8, maxServices: 2);
        using var e = new TraceStorageEngine(Dir("saturated"), NullLogger<TraceStorageEngine>.Instance,
                                             writeSegmentFormatV4: false, indexEnabled: true, options: null, pools);

        const int Spans = 100, Names = 50, Services = 5;
        long expectedBytes = 0;
        var  seenNames     = new HashSet<string>(StringComparer.Ordinal);
        for (int i = 0; i < Spans; i++)
        {
            string name = $"GET /api/user/{i % Names}";
            string svc  = $"svc-{i % Services}";
            e.WriteSpan(Span(i, name, svc));
            expectedBytes += TraceStorageEngine.HotSpanBytes(0);
        }

        var hot = e.HotSpansForTest;
        _out.WriteLine($"{hot.Count} spans in the tier, {e.PoolsForTest.UnpooledNames} names and "
                     + $"{e.PoolsForTest.UnpooledServices} services kept their own strings");

        Assert.Equal(Spans, hot.Count);                               // nothing dropped
        for (int i = 0; i < Spans; i++)
        {
            Assert.Equal($"GET /api/user/{i % Names}", hot[i].Name);   // each under its OWN name
            Assert.Equal($"svc-{i % Services}",       hot[i].ServiceName);
        }
        Assert.True(e.PoolsForTest.UnpooledNames    > 0, "the name pool never saturated — the fact tests nothing");
        Assert.True(e.PoolsForTest.UnpooledServices > 0, "the service pool never saturated — the fact tests nothing");

        // The budget pays for what the pool could not share — exactly the unshared strings.
        long unshared = 0;
        var  poolNames    = new HashSet<string>(StringComparer.Ordinal);
        var  poolServices = new HashSet<string>(StringComparer.Ordinal);
        foreach (var r in hot)
        {
            if (poolNames.Count < 8 || poolNames.Contains(r.Name)) poolNames.Add(r.Name);
            else unshared += SpanStringPools.UnpooledStringBytes(r.Name);
            if (poolServices.Count < 2 || poolServices.Contains(r.ServiceName)) poolServices.Add(r.ServiceName);
            else unshared += SpanStringPools.UnpooledStringBytes(r.ServiceName);
        }
        Assert.Equal(expectedBytes + unshared, e.HotBytesForTest);
    }

    /// <summary>
    /// SHED ON FLUSH. The name pool a tier interned into is gone once the tier is detached: the next
    /// tier gets an empty pool, so a high-cardinality burst lives exactly as long as its tier and the
    /// pool never holds more than one tier's names.
    /// </summary>
    [Fact]
    public void The_name_pool_is_replaced_when_the_tier_is_flushed()
    {
        using var e = new TraceStorageEngine(Dir("shed"), NullLogger<TraceStorageEngine>.Instance);

        e.WriteSpan(Span(0, Fresh("GET /orders/{id}"), "billing"));
        var poolBefore = e.PoolsForTest.NamesForTest;
        var nameBefore = e.HotSpansForTest[0].Name;

        e.FlushHotTier();

        e.WriteSpan(Span(1, Fresh("GET /orders/{id}"), "billing"));
        Assert.NotSame(poolBefore, e.PoolsForTest.NamesForTest);
        // The old instance is not what the new tier gets — the old pool, and what it held, is gone.
        Assert.NotSame(nameBefore, e.HotSpansForTest[0].Name);
        Assert.Equal(nameBefore, e.HotSpansForTest[0].Name);
    }

    /// <summary>
    /// REPLAY GOES THROUGH THE SAME DOOR. A tier rebuilt from the write-ahead log after an unclean
    /// stop is interned exactly as live ingest is — one insert path, not two.
    /// </summary>
    [Fact]
    public void A_tier_replayed_from_the_log_is_interned_too()
    {
        string dir = Dir("replay");
        var wal = SpanWriteAheadLog.Open(Path.Combine(dir, "spans.wal"));
        for (int i = 0; i < 20; i++) wal.Append(Span(i, Fresh("SELECT orders"), Fresh("billing")));
        wal.Dispose();   // unclean stop: the log is all there is

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        var hot = e.HotSpansForTest;
        Assert.Equal(20, hot.Count);
        Assert.All(hot, r => Assert.Same(hot[0].Name, r.Name));
        Assert.All(hot, r => Assert.Same(hot[0].ServiceName, r.ServiceName));
    }

    private static string Fresh(string s) => new(s.AsSpan());

    private SpanIngestItem Span(int i, string name, string service) => new()
    {
        TraceId           = new TraceId(0x1A7E3UL, (ulong)(i / 10 + 1)),
        SpanId            = new SpanId((ulong)(i + 1)),
        StartTimeUnixNano = _baseNano + i * Ms,
        DurationNanos     = Ms,
        Name              = name,
        ServiceName       = service,
        Kind              = SpanKind.Server,
    };
}
