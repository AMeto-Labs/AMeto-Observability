using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// The HOT half of what <see cref="SpanSearchBoundTests"/> proves about the cold half: a search
/// serves the in-memory tier first, and it must serve the NEWEST <c>limit</c> spans from it
/// without sorting the tier to find them.
///
/// <para>The hot tier holds up to ~100k spans and every ingested span takes the WRITE side of the
/// same lock the scan reads under. The SSE trace list runs one search per internal page with no
/// pacing — up to max/pageSize of them back to back on one connection, where the old client paced
/// the same passes by human scrolling — so an ordering buffer over the whole tier inside the read
/// lock is 25 full sorts of 100k spans per stream, each of them stalling ingest.</para>
/// </summary>
public sealed class SpanSearchHotTierBoundTests : IDisposable
{
    /// <summary>Far more than any page asks for, and small enough to write in a moment.</summary>
    private const int Spans = 40_000;

    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-trchot-" + Guid.NewGuid().ToString("N"));
    private readonly TraceStorageEngine _engine;
    private readonly long _baseNano;

    public SpanSearchHotTierBoundTests()
    {
        Directory.CreateDirectory(_dir);
        _engine   = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        _baseNano = Base.ToUnixTimeMilliseconds() * 1_000_000L;

        for (int i = 0; i < Spans; i++)
        {
            _engine.WriteSpan(new SpanIngestItem
            {
                TraceId           = new TraceId(0x9E3779B97F4A7C15UL, (ulong)(i + 1)),
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = default,
                StartTimeUnixNano = _baseNano + i * 1_000_000L,   // 1 ms apart, oldest written first
                DurationNanos     = 4_000_000,
                Name              = "SELECT payments",
                ServiceName       = "billing",
                Kind              = SpanKind.Client,
                Status            = SpanStatusCode.Unset,
                HttpStatusCode    = 0,
                AttributesBytes   = [],
            });
        }

        // NOT flushed, unlike the cold-path tests: everything under test must still be in memory.
        Assert.Equal(Spans, HotCount);
    }

    /// <summary>Live hot-tier span count — the tier has no public size.</summary>
    private int HotCount
    {
        get
        {
            var f = typeof(TraceStorageEngine).GetField("_hotSpans",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
            return ((System.Collections.ICollection)f.GetValue(_engine)!).Count;
        }
    }

    public void Dispose()
    {
        try { _engine.Dispose(); } catch { }
        try { Directory.Delete(_dir, true); } catch { }
    }

    [Fact]
    public async Task The_hot_tier_returns_the_NEWEST_limit_spans_newest_first()
    {
        const int Limit = 50;

        var page = new List<SpanRecord>();
        await foreach (var s in _engine.SearchSpansAsync(
            from: Base.AddMinutes(-5), to: Base.AddDays(7), serviceName: "billing", limit: Limit))
            page.Add(s);

        Assert.Equal(Limit, page.Count);

        // The hot tier is stored oldest-first, so a bounded selection that keeps the wrong end —
        // or drains its heap without reversing — fails here and nowhere else.
        long newest = _baseNano + (Spans - 1) * 1_000_000L;
        for (int i = 0; i < Limit; i++)
            Assert.Equal(newest - i * 1_000_000L, page[i].StartTimeUnixNano);
    }

    [Fact]
    public async Task Selecting_the_newest_limit_does_not_allocate_an_ordering_over_the_whole_tier()
    {
        // WHAT THIS LOCKS, and why the two tests around it do not lock it: they assert the
        // RESULT — the newest `limit` spans, newest-first — and `Where().OrderByDescending()
        // .Take(limit).ToList()` produces exactly that result too. A revert to it is green
        // against them, and the property the change was made FOR (the tier is not ordered inside
        // the read lock that every ingested span needs the write side of) goes unguarded.
        //
        // Measured as ALLOCATION rather than as live bytes, which is where this departs from
        // SpanSearchBoundTests. That suite can use GC.GetTotalMemory because its buffer is still
        // ROOTED at the moment it measures: the iterator is suspended inside the segment loop and
        // whatever the loop accumulated cannot have been collected. Here the ordering buffer dies
        // at `.ToList()`, before any point a test can observe, so a live-bytes probe reads the
        // same number for both implementations and cannot fail. Cumulative allocation sees the
        // transient. The precondition is the same one — [assembly: CollectionBehavior
        // (DisableTestParallelization = true)] in AssemblyInfo.cs — because the counter is
        // process-wide.
        const int Limit = 50;

        // Warm first: JIT, the reader's one-off statics and this test's own delegates would
        // otherwise land in the measurement as a constant that has nothing to do with the tier.
        await Drain(Limit);

        long before = GC.GetTotalAllocatedBytes(precise: true);
        int  count  = await Drain(Limit);
        long grew   = GC.GetTotalAllocatedBytes(precise: true) - before;

        Assert.Equal(Limit, count);

        // Ordering 40k matches to keep 50 allocates the buffer by doubling (~1 MB of element
        // arrays) plus a long[40_000] of keys and an int[40_000] map — north of 1.4 MB. The
        // bounded heap allocates a 50-slot queue, a 50-row list and two small identity sets:
        // tens of kilobytes. 256 KB sits an order of magnitude above what the bounded path needs
        // and far below what an ordering pass costs.
        Assert.True(grew < 256L * 1024,
            $"selecting the newest {Limit} of {Spans:N0} hot spans allocated {grew / 1024.0:F0} KB — " +
            "the tier is being ordered to find them, inside the read lock ingest needs");
    }

    private async Task<int> Drain(int limit)
    {
        int n = 0;
        await foreach (var _ in _engine.SearchSpansAsync(
            from: Base.AddMinutes(-5), to: Base.AddDays(7), serviceName: "billing", limit: limit))
            n++;
        return n;
    }

    [Fact]
    public async Task A_filtered_hot_scan_still_keeps_only_the_newest_limit()
    {
        // The filters run before the selection, so a scan that matched everything and one that
        // matches a subset must both come back bounded and newest-first.
        const int Limit = 10;
        long cut = _baseNano + 1_000L * 1_000_000L;   // the oldest 1000 spans only

        var page = new List<SpanRecord>();
        await foreach (var s in _engine.SearchSpansAsync(
            from: Base.AddMinutes(-5), to: DateTimeOffset.FromUnixTimeMilliseconds(cut / 1_000_000L),
            serviceName: "billing", limit: Limit))
            page.Add(s);

        Assert.Equal(Limit, page.Count);
        for (int i = 0; i < Limit; i++)
            Assert.Equal(cut - i * 1_000_000L, page[i].StartTimeUnixNano);
    }
}
