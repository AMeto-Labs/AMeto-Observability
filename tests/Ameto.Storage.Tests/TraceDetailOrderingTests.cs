using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// <c>ITraceProvider.GetTraceAsync</c> promises spans "ordered by StartTimeUnixNano", and the
/// engine used to sort each TIER and then concatenate: every hot span, then every cold one.
///
/// <para>The hot tier holds the newest spans by construction, so a trace straddling the two came
/// back inverted — and every trace crosses that boundary for the whole minute after its oldest
/// spans flush, which is exactly the minute a user is looking at it. Measured through the live
/// endpoint on a seven-span trace, five flushed and two hot: <c>hot(+20 ms), hot(+21 ms),
/// cold(+1), cold(+3), cold(+5), cold(+12), cold(+14)</c>. The waterfall and the flamegraph draw
/// what they are handed, so the root arrived last.</para>
///
/// <para>The tests below are ordered by what they defend: the contract itself, then the two
/// things merging the tiers must not have broken — the dedupe that covers the flush handover,
/// and the trace that lives in one tier only.</para>
/// </summary>
public sealed class TraceDetailOrderingTests : IDisposable
{
    private static readonly DateTimeOffset Base = new(2026, 8, 3, 9, 0, 0, TimeSpan.Zero);

    private readonly string             _dir = Path.Combine(Path.GetTempPath(), "ameto-trcorder-" + Guid.NewGuid().ToString("N"));
    private readonly TraceStorageEngine _engine;
    private readonly long               _baseNano;

    public TraceDetailOrderingTests()
    {
        Directory.CreateDirectory(_dir);
        _engine   = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        _baseNano = Base.ToUnixTimeMilliseconds() * 1_000_000L;
    }

    public void Dispose()
    {
        try { _engine.Dispose(); } catch { }
        try { Directory.Delete(_dir, true); } catch { }
    }

    private const long Ms = 1_000_000L;

    private static readonly TraceId Straddler = new(0xA11CE, 0x7);

    private void Write(ulong spanId, long offsetMs, TraceId? trace = null, string name = "step")
        => _engine.WriteSpan(new SpanIngestItem
        {
            TraceId           = trace ?? Straddler,
            SpanId            = new SpanId(spanId),
            ParentSpanId      = spanId == 1 ? default : new SpanId(1),
            StartTimeUnixNano = _baseNano + offsetMs * Ms,
            DurationNanos     = 2 * Ms,
            Name              = name,
            ServiceName       = "billing",
            Kind              = SpanKind.Server,
            Status            = SpanStatusCode.Ok,
        });

    private async Task<List<SpanRecord>> ReadTrace(TraceId id)
    {
        var got = new List<SpanRecord>();
        await foreach (var s in _engine.GetTraceAsync(id)) got.Add(s);
        return got;
    }

    private static void AssertAscendingByStart(List<SpanRecord> spans)
    {
        for (int i = 1; i < spans.Count; i++)
            Assert.True(spans[i].StartTimeUnixNano >= spans[i - 1].StartTimeUnixNano,
                $"span {i} starts at {spans[i].StartTimeUnixNano}, BEFORE the "
              + $"{spans[i - 1].StartTimeUnixNano} handed over ahead of it — "
              + "GetTraceAsync promises start-time order and the waterfall draws what it is given");
    }

    [Fact]
    public async Task A_trace_straddling_both_tiers_arrives_in_start_order()
    {
        // Five spans in TWO cold segments — one tier's worth of sorting is not enough on its own,
        // and a fix that merged the segments but not the tiers would pass a one-segment fixture.
        Write(1, 1);
        Write(2, 3);
        Write(3, 5);
        _engine.FlushHotTier();

        Write(4, 12);
        Write(5, 14);
        _engine.FlushHotTier();

        // And two still in memory: the NEWEST spans of the trace, which is why concatenating the
        // tiers inverted it rather than merely shuffling it.
        Write(6, 20);
        Write(7, 21);

        var spans = await ReadTrace(Straddler);

        Assert.Equal(7, spans.Count);
        AssertAscendingByStart(spans);
        Assert.Equal(
            new[] { 1L, 3L, 5L, 12L, 14L, 20L, 21L }.Select(m => _baseNano + m * Ms).ToList(),
            spans.Select(s => s.StartTimeUnixNano).ToList());
    }

    [Fact]
    public async Task The_hot_tier_alone_and_the_cold_tier_alone_are_both_still_ordered()
    {
        // The two degenerate shapes the merge must not have cost: a trace that never flushed, and
        // one with nothing left in memory. The second is the branch where the hot list is null,
        // which is the one a careless merge drops on the floor.
        var hotOnly  = new TraceId(0xB0B, 0x1);
        var coldOnly = new TraceId(0xB0B, 0x2);

        Write(11, 9,  coldOnly);
        Write(12, 2,  coldOnly);
        Write(13, 6,  coldOnly);
        _engine.FlushHotTier();

        Write(21, 8, hotOnly);
        Write(22, 4, hotOnly);

        var cold = await ReadTrace(coldOnly);
        Assert.Equal(3, cold.Count);
        AssertAscendingByStart(cold);

        var hot = await ReadTrace(hotOnly);
        Assert.Equal(2, hot.Count);
        AssertAscendingByStart(hot);

        Assert.Empty(await ReadTrace(new TraceId(0xDEAD, 0xBEEF)));
    }

    [Fact]
    public async Task A_span_that_reaches_the_reader_from_both_tiers_is_yielded_once()
    {
        // The flush handover and a WAL replay both put the same (trace, span) in front of this
        // reader twice, and the dedupe that covers it used to run across two separate yield loops.
        // Merging them into one sequence is exactly the change that could have let a duplicate
        // through — the waterfall would draw the span twice, and the flamegraph would double its
        // self time.
        Write(31, 5);
        _engine.FlushHotTier();          // the cold copy

        Write(31, 5);                    // the same span id, still in memory
        Write(32, 7);

        var spans = await ReadTrace(Straddler);

        Assert.Equal(2, spans.Count);
        Assert.Equal(new[] { 31UL, 32UL }, spans.Select(s => s.SpanId.RawValue).ToArray());
        AssertAscendingByStart(spans);
    }

    [Fact]
    public async Task The_HOT_copy_of_a_duplicated_span_is_the_one_that_survives()
    {
        // WHICH COPY, not just how many — and the comment on the merge used to answer this
        // question wrongly. It said "OrderBy is a STABLE sort and the hot spans are added first,
        // so the HOT copy is the one that survives the dedupe below", and the dedupe ran AFTER the
        // sort, so what actually survived was the EARLIER-STARTING copy whatever tier it came
        // from. Stability orders EQUAL keys; two copies of a span with different start times are
        // not equal keys, so it never applied.
        //
        // Every other test in this file writes both copies at the SAME offset, which is exactly
        // the case stability does cover — so the whole suite passed over a claim that was false.
        // Here the two copies differ, and the difference is the assertion.
        Write(61, 1, name: "COLD-copy");
        _engine.FlushHotTier();

        Write(61, 30, name: "HOT-copy");     // same span id, re-written, still in memory
        Write(62, 40, name: "later");

        var spans = await ReadTrace(Straddler);

        Assert.Equal(2, spans.Count);
        AssertAscendingByStart(spans);

        var survivor = spans.Single(s => s.SpanId.RawValue == 61);
        Assert.Equal("HOT-copy", survivor.Name);
        Assert.Equal(_baseNano + 30 * Ms, survivor.StartTimeUnixNano);
    }

    [Fact]
    public async Task A_span_in_two_cold_segments_is_yielded_once_and_in_place()
    {
        // The other duplicate source, entirely inside the cold tier: a crash between a
        // compaction's merge write and its source deletion leaves one span in two files.
        Write(41, 2);
        Write(42, 9);
        _engine.FlushHotTier();

        Write(41, 2);                    // the survivor of the interrupted compaction
        _engine.FlushHotTier();

        var spans = await ReadTrace(Straddler);

        Assert.Equal(2, spans.Count);
        AssertAscendingByStart(spans);
        Assert.Equal(_baseNano + 2 * Ms, spans[0].StartTimeUnixNano);
        Assert.Equal(_baseNano + 9 * Ms, spans[1].StartTimeUnixNano);
    }

    [Fact]
    public async Task Spans_sharing_a_start_time_are_all_returned()
    {
        // Ties are ordinary — a producer at millisecond resolution emits them all day — and the
        // ordering claim is NON-DECREASING, not strictly increasing. What must not happen is one
        // of them being folded away by the sort or the dedupe.
        Write(51, 3);
        Write(52, 3);
        _engine.FlushHotTier();
        Write(53, 3);

        var spans = await ReadTrace(Straddler);

        Assert.Equal(3, spans.Count);
        AssertAscendingByStart(spans);
        Assert.Equal(new[] { 51UL, 52UL, 53UL }, spans.Select(s => s.SpanId.RawValue).Order().ToArray());
    }
}
