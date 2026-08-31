using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// A span search must cost memory in proportion to what it RETURNS, not to what it matches.
///
/// The two requirements pull against each other, which is how the bug got in. A segment file
/// is written oldest-first, so a search that simply streams it and stops at the limit keeps
/// the oldest matches and drops the newest — the caller then pages "newest first" over a
/// pool that is quietly the wrong end of the data. Ordering the segment's matches fixes
/// that, and the first version of the fix ordered them by buffering all of them.
///
/// On a month-wide query against a busy service that is hundreds of thousands of spans, each
/// carrying its strings and — whenever the query touches an attribute — a decoded attribute
/// dictionary. A 512 MB server died on exactly that. Both properties are asserted here
/// together, because a fix for either one alone is available and wrong.
/// </summary>
public sealed class SpanSearchBoundTests : IDisposable
{
    // Large enough that buffering every match is a heap cost no ambient noise can imitate,
    // small enough to write and flush in a couple of seconds.
    private const int Spans = 120_000;
    private const int Limit = 50;

    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-trcbound-" + Guid.NewGuid().ToString("N"));
    private readonly TraceStorageEngine _engine;
    private readonly long _baseNano;

    public SpanSearchBoundTests()
    {
        Directory.CreateDirectory(_dir);
        _engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);

        _baseNano = Base.ToUnixTimeMilliseconds() * 1_000_000L;

        // One span per trace, strictly increasing start times, all matching the same filter.
        // Names and service are shared literals on purpose: distinct strings would land in the
        // segment's intern pool, which is live during the scan either way, and would inflate
        // both sides of the memory comparison instead of separating them.
        for (int i = 0; i < Spans; i++)
        {
            _engine.WriteSpan(new SpanIngestItem
            {
                TraceId           = new TraceId(0x9E3779B97F4A7C15UL, (ulong)(i + 1)),
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = default,
                StartTimeUnixNano = _baseNano + i * 1_000_000L,   // 1 ms apart
                DurationNanos     = 4_000_000,
                Name              = "SELECT payments",
                ServiceName       = "billing",
                Kind              = SpanKind.Client,
                Status            = SpanStatusCode.Unset,
                HttpStatusCode    = 0,
                AttributesBytes   = [],
            });
        }

        // Onto disk, so the search runs the cold path this is about — and ALL of it, because
        // the search serves the hot tier first and stops as soon as the limit is met. One
        // flush caps its snapshot, so a single call leaves tens of thousands of spans in
        // memory, the page fills from those, and the cold scan under test never runs at all.
        for (int guard = 0; guard < 20 && HotCount > 0; guard++)
            _engine.FlushHotTier();
        Assert.Equal(0, HotCount);
    }

    /// <summary>Live hot-tier span count — the tier has no public size, and these tests are
    /// void unless it is empty when they run.</summary>
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

    private IAsyncEnumerable<SpanRecord> Search(int limit) =>
        _engine.SearchSpansAsync(
            from:        Base.AddMinutes(-5),
            to:          Base.AddDays(7),
            serviceName: "billing",
            limit:       limit);

    [Fact]
    public async Task A_page_is_the_NEWEST_matches_even_when_the_segment_holds_far_more()
    {
        var page = new List<SpanRecord>();
        await foreach (var s in Search(Limit)) page.Add(s);

        Assert.Equal(Limit, page.Count);

        // The newest `Limit` spans are the last written, and they must come back newest-first.
        long newest = _baseNano + (Spans - 1) * 1_000_000L;
        for (int i = 0; i < Limit; i++)
            Assert.Equal(newest - i * 1_000_000L, page[i].StartTimeUnixNano);
    }

    [Fact]
    public async Task Reading_a_huge_segment_does_not_retain_it()
    {
        // The reader's own buffers, the segment's intern pool and the page itself are all live
        // at the moment of measurement and are all fine. What must NOT be live is one object
        // per MATCH — that is the difference between a bounded page and 120k retained records.
        //
        // Measured at the first yielded span rather than at the end: the iterator is suspended
        // inside the segment loop there, so whatever that loop accumulated is still rooted by
        // the state machine and cannot have been collected out from under the reading.
        long before = GC.GetTotalMemory(forceFullCollection: true);

        await using var e = Search(Limit).GetAsyncEnumerator();
        Assert.True(await e.MoveNextAsync(), "the search returned nothing to measure");

        long grew = GC.GetTotalMemory(forceFullCollection: true) - before;

        // Buffering every match costs ~100 bytes per SpanRecord plus a dedupe-set entry each:
        // north of 15 MB at this size, and unbounded in production, where the same shape of
        // query matches more and each record also carries a decoded attribute dictionary.
        // The bounded scan keeps `Limit` of them. 6 MB sits an order of magnitude above what
        // the bounded path needs and well under what the unbounded one takes.
        Assert.True(grew < 6L * 1024 * 1024,
            $"reading {Spans:N0} matches for a {Limit}-span page retained {grew / (1024.0 * 1024.0):F1} MB — " +
            "the segment scan is buffering matches instead of keeping only the newest `limit`");
    }

    [Fact]
    public async Task The_page_does_not_grow_with_the_number_of_matches()
    {
        // The limit is the contract. Without it holding, "load more" walks the client into the
        // same allocation the server just died on.
        await foreach (var _ in Search(1)) { }

        int count = 0;
        await foreach (var _ in Search(7)) count++;
        Assert.Equal(7, count);
    }
}
