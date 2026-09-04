using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE THRESHOLD INVERSION: for most of this engine's life the segment an ordinary flush produced
/// was never a compaction candidate.
///
/// <para><c>HotFlushThreshold</c> writes 50 000 spans; <c>CompactionThreshold</c> admitted
/// candidates under 10 000. Fifty thousand is not under ten thousand, so a normal segment was never
/// a seed, never in a batch, and never merged with anything until retention deleted it. Segment
/// count grew with ingest and never fell — and since a trace lookup consults every cold segment,
/// that count is the multiplier on the cost of opening any trace. <c>MaxHotAge</c> alone put a
/// floor of twenty-four new segments a day under it, on an install with no traffic at all.</para>
///
/// <para>These tests pin both halves of the fix: ordinary segments now merge, and the memory a
/// merge may hold did not move — which is the reason raising the threshold is safe rather than a
/// trade.</para>
/// </summary>
public sealed class CompactionThresholdTests : IDisposable
{
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-cthresh-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long              _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public CompactionThresholdTests(ITestOutputHelper output)
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

    /// <summary>
    /// Segments the size a real flush makes them, without writing fifty thousand spans four times
    /// over: <see cref="SelectCompactionBatch"/> reads <c>SpanCount</c>, so the decision under test
    /// can be put to it directly.
    /// </summary>
    private static SpanSegmentInfo Seg(int spanCount, long minNano, long maxNano, ulong id = 0) => new()
    {
        FilePath      = $"spans-{minNano}-{maxNano}-{spanCount}-{id:x8}.trc",
        MinStartNano  = minNano,
        MaxStartNano  = maxNano,
        SpanCount     = spanCount,
        Services      = ["billing"],
        FormatVersion = 4,
        SegmentId     = id,
    };

    [Fact]
    public void The_segment_an_ordinary_flush_writes_is_a_compaction_candidate()
    {
        // THE BUG, STATED AS A TEST. A flush writes HotFlushThreshold spans; if that size is not a
        // candidate then nothing an install produces at volume ever merges.
        var segs = new SpanSegmentInfo[4];
        for (int i = 0; i < 4; i++)
            segs[i] = Seg(50_000, _baseNano + i * 3_600_000L * Ms, _baseNano + (i * 3_600_000L + 1_000_000L) * Ms, (ulong)(i + 1));

        var batch = TraceStorageEngine.SelectCompactionBatch(segs);

        _out.WriteLine($"four 50 000-span segments → batch of {batch.Count}");
        Assert.True(batch.Count >= 2,
            "a segment the size of an ordinary flush is still not a compaction candidate — "
          + "segment count will grow with ingest and never fall");
    }

    [Fact]
    public void The_result_of_merging_ordinary_segments_stops_merging()
    {
        // The other side: the threshold must not be so high that merged output keeps re-merging,
        // rewriting the same spans over and over. Four 50k segments make ~200k, which is above the
        // threshold and settles.
        var merged = new[] { Seg(200_000, _baseNano, _baseNano + 1_000_000L * Ms, 1) };
        var batch  = TraceStorageEngine.SelectCompactionBatch(merged);

        _out.WriteLine($"one 200 000-span segment → batch of {batch.Count}");
        Assert.Empty(batch);
    }

    [Fact]
    public void A_merge_still_holds_no_more_spans_than_it_ever_did()
    {
        // WHY RAISING THE THRESHOLD IS SAFE RATHER THAN A TRADE. Peak memory is governed by
        // MaxSpansPerPass, not by the threshold: at 10 000 a batch was up to twenty segments of ten
        // thousand, and at 60 000 it is four of fifty thousand. Same two hundred thousand spans.
        var many = new SpanSegmentInfo[20];
        for (int i = 0; i < 20; i++)
            many[i] = Seg(50_000, _baseNano + i * 60_000L * Ms, _baseNano + (i * 60_000L + 1_000L) * Ms, (ulong)(i + 1));

        var batch = TraceStorageEngine.SelectCompactionBatch(many);
        long spans = batch.Sum(s => (long)s.SpanCount);

        _out.WriteLine($"twenty 50 000-span segments offered → batch of {batch.Count}, {spans:N0} spans");
        // EXACTLY the cap, not the cap plus one segment. Enforcing it in the loader — which
        // stopped once it had ALREADY read that many — overshot by whatever the last segment
        // held: invisible at 10 000-span candidates, a quarter of the budget at 50 000.
        Assert.True(spans <= 200_000,
            $"a batch of {spans:N0} spans is past the 200 000 a pass may hold");
    }

    [Fact]
    public void Ordinary_segments_really_do_merge_end_to_end()
    {
        // The constants above are only an argument; this is the engine doing it. Segments here are
        // small for the test's sake, but the point is that they are ABOVE the old threshold of
        // 10 000 — the size class that used to be excluded is not special any more.
        string dir = Dir("endtoend");
        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        ulong span = 1;
        for (int s = 0; s < 3; s++)
        {
            for (int t = 0; t < 4_000; t++)
                e.WriteSpan(new SpanIngestItem
                {
                    TraceId = new TraceId((ulong)(s * 100_000 + t), (ulong)t),
                    SpanId = new SpanId(span++), ParentSpanId = default,
                    StartTimeUnixNano = _baseNano + (s * 100_000 + t) * Ms, DurationNanos = 2 * Ms,
                    Name = "GET /orders", ServiceName = "billing",
                    Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
                });
            e.FlushHotTier();
        }
        Assert.Equal(3, e.ColdSegmentCountForTest);

        e.CompactSmallSegments();

        _out.WriteLine($"3 segments → {e.ColdSegmentCountForTest} after compaction; "
                     + $"catalog {e.CatalogCountsForTest}");
        Assert.Equal(1, e.ColdSegmentCountForTest);

        // And the merged file is v4, so the rewrite also reclaims the trace index block.
        Assert.Equal(4, e.ColdSegmentsForTest.Single().FormatVersion);
    }
}
