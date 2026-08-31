using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// A duplicate must not COST a result slot.
///
/// <para>The bounded top-K that <see cref="SpanSearchBoundTests"/> and
/// <see cref="SpanSearchHotTierBoundTests"/> exist to protect checked the dedupe set on the way
/// IN (against what had already been yielded) and recorded on the way OUT. Two copies of one
/// (TraceId, SpanId) arriving inside a single tier or segment were therefore both admitted —
/// neither was in the yielded set yet — and between them evicted a distinct, older span to make
/// room. The second copy was then dropped at the drain, and the scan returned FEWER than `limit`
/// distinct spans although more existed.</para>
///
/// <para>Both duplicate sources are ordinary rather than exotic: the reader is served the hot
/// tier concatenated with the in-flight flush snapshot, and a segment can hold spans that a WAL
/// replay put back after a crash. Rewriting the same span is how this reproduces them without
/// one.</para>
///
/// <para>The fix must not reintroduce the growth 3fc5472 removed: the identity set is bounded by
/// what the heap HOLDS, never by what the scan reads — which is why
/// <see cref="SpanSearchBoundTests.Reading_a_huge_segment_does_not_retain_it"/> has to stay green
/// alongside these.</para>
/// </summary>
public sealed class SpanSearchDuplicateSlotTests : IDisposable
{
    private const int  Spans = 100;
    private const long Ms    = 1_000_000L;

    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-trcdup-" + Guid.NewGuid().ToString("N"));
    private readonly TraceStorageEngine _engine;
    private readonly long _baseNano;

    public SpanSearchDuplicateSlotTests()
    {
        Directory.CreateDirectory(_dir);
        _engine   = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        _baseNano = Base.ToUnixTimeMilliseconds() * 1_000_000L;

        // One span per trace, a millisecond apart, oldest written first.
        for (int i = 1; i <= Spans; i++) Write((ulong)i, _baseNano + i * Ms);

        // …then the newest THREE again, byte for byte. Same TraceId, same SpanId, same start:
        // exactly what a WAL replay or a flush-snapshot overlap puts in front of a reader.
        Write(Spans,     _baseNano + Spans * Ms);
        Write(Spans - 1, _baseNano + (Spans - 1) * Ms);
        Write(Spans - 2, _baseNano + (Spans - 2) * Ms);
    }

    private void Write(ulong id, long startNano) => _engine.WriteSpan(new SpanIngestItem
    {
        TraceId           = new TraceId(0x9E3779B97F4A7C15UL, id),
        SpanId            = new SpanId(id),
        ParentSpanId      = default,
        StartTimeUnixNano = startNano,
        DurationNanos     = 4_000_000,
        Name              = "SELECT payments",
        ServiceName       = "billing",
        Kind              = SpanKind.Client,
        Status            = SpanStatusCode.Unset,
        HttpStatusCode    = 0,
        AttributesBytes   = [],
    });

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

    private async Task<List<SpanRecord>> PageAsync(int limit)
    {
        var page = new List<SpanRecord>();
        await foreach (var s in _engine.SearchSpansAsync(
            from: Base.AddMinutes(-5), to: Base.AddDays(7), serviceName: "billing", limit: limit))
            page.Add(s);
        return page;
    }

    /// <summary>The newest <paramref name="n"/> DISTINCT spans, newest-first.</summary>
    private long[] NewestStarts(int n) =>
        Enumerable.Range(0, n).Select(i => _baseNano + (Spans - i) * Ms).ToArray();

    [Fact]
    public async Task A_duplicate_in_the_hot_tier_does_not_cost_a_slot()
    {
        Assert.Equal(Spans + 3, HotCount);          // the duplicates really are in there

        var page = await PageAsync(limit: 3);

        // Three asked for, three DISTINCT spans back. With the duplicates consuming heap slots
        // this returns two: the second copy of the newest span occupies the place the third
        // distinct span should have held, and is then discarded at the yield.
        Assert.Equal(3, page.Count);
        Assert.Equal(NewestStarts(3), page.Select(s => s.StartTimeUnixNano).ToArray());
        Assert.Equal(3, page.Select(s => s.SpanId).Distinct().Count());
    }

    [Fact]
    public async Task A_duplicate_inside_one_cold_segment_does_not_cost_a_slot()
    {
        // Onto disk, and ALL of it: the search serves the hot tier first and stops at the limit,
        // so a tier left with anything in it means the cold path under test never runs.
        for (int guard = 0; guard < 20 && HotCount > 0; guard++) _engine.FlushHotTier();
        Assert.Equal(0, HotCount);

        var page = await PageAsync(limit: 3);

        Assert.Equal(3, page.Count);
        Assert.Equal(NewestStarts(3), page.Select(s => s.StartTimeUnixNano).ToArray());
        Assert.Equal(3, page.Select(s => s.SpanId).Distinct().Count());
    }
}
