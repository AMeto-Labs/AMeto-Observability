using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE POINT OF THE WHOLE THING: a trace lookup that opens one segment instead of all of them —
/// and never opens fewer than it is entitled to.
///
/// <para>A correct answer is not the property under test. The engine already returned correct
/// answers; it returned them by reading and inflating every segment's trace index, 38% of every
/// <c>.trc</c>, to find five spans. So these tests assert on WORK — how many segments the lookup
/// actually opened — because an index that is right and still scanning is invisible from the
/// result and is exactly the failure this branch exists to prevent.</para>
///
/// <para>The other half is the safety direction. A segment may be skipped only when the catalog
/// says the index covers it AND no run named the trace in it; every test that turns coverage off
/// asserts that the scan comes back, because a skip taken without that proof is a trace that has
/// silently stopped existing.</para>
/// </summary>
public sealed class TraceIndexLookupTests : IDisposable
{
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-tixlookup-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long              _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public TraceIndexLookupTests(ITestOutputHelper output)
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

    private static TraceStorageEngine Engine(string dir) => new(dir, NullLogger<TraceStorageEngine>.Instance);

    /// <summary>A trace id with no structure a lookup could exploit — as OTel produces them.</summary>
    private static TraceId Id(int i) => new(
        unchecked((ulong)(i * 6364136223846793005L + 1442695040888963407L)),
        unchecked((ulong)(i * 2862933555777941757L + 3037000493L)));

    private static void WriteSpan(TraceStorageEngine e, TraceId trace, ulong spanId, long startNano)
        => e.WriteSpan(new SpanIngestItem
        {
            TraceId = trace, SpanId = new SpanId(spanId), ParentSpanId = default,
            StartTimeUnixNano = startNano, DurationNanos = 2 * Ms,
            Name = "GET /orders", ServiceName = "billing",
            Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
        });

    /// <summary>N segments, each holding its own traces. Returns the trace planted in segment k.</summary>
    private TraceId[] BuildSegments(TraceStorageEngine e, int segments, int tracesPer = 40, long? from = null)
    {
        long at = from ?? _baseNano;
        var planted = new TraceId[segments];
        ulong span = 1;
        for (int s = 0; s < segments; s++)
        {
            for (int t = 0; t < tracesPer; t++)
            {
                var id = Id(s * 1000 + t);
                if (t == 0) planted[s] = id;
                for (int k = 0; k < 3; k++)
                    WriteSpan(e, id, span++, at + (s * 10_000 + t * 10 + k) * Ms);
            }
            e.FlushHotTier();
        }
        return planted;
    }

    private static async Task<List<SpanRecord>> Read(TraceStorageEngine e, TraceId id)
    {
        var got = new List<SpanRecord>();
        await foreach (var s in e.GetTraceAsync(id)) got.Add(s);
        return got;
    }

    [Fact]
    public async Task A_lookup_opens_the_one_segment_that_holds_the_trace()
    {
        using var e = Engine(Dir("one"));
        var planted = BuildSegments(e, segments: 12);
        Assert.Equal(12, e.ColdSegmentCountForTest);
        Assert.Equal((12, 12), e.CatalogCountsForTest);

        var spans = await Read(e, planted[7]);

        _out.WriteLine($"opened {e.SegmentsOpenedByLastTraceLookup}, "
                     + $"skipped {e.SegmentsSkippedByLastTraceLookup} of 12; "
                     + $"index holds {e.IndexStatsForTest.RetainedBytes} B over "
                     + $"{e.IndexStatsForTest.Runs} runs");

        Assert.Equal(3, spans.Count);
        Assert.All(spans, s => Assert.Equal(planted[7], s.TraceId));

        // THE ASSERTION THIS FILE EXISTS FOR. Before the index every one of the twelve was opened
        // and its whole trace index inflated.
        Assert.Equal(1,  e.SegmentsOpenedByLastTraceLookup);
        Assert.Equal(11, e.SegmentsSkippedByLastTraceLookup);
    }

    [Fact]
    public async Task A_trace_that_does_not_exist_opens_nothing_at_all()
    {
        using var e = Engine(Dir("absent"));
        BuildSegments(e, segments: 10);

        var spans = await Read(e, Id(999_999));

        _out.WriteLine($"opened {e.SegmentsOpenedByLastTraceLookup} of 10 for an absent trace");
        Assert.Empty(spans);
        Assert.Equal(0,  e.SegmentsOpenedByLastTraceLookup);
        Assert.Equal(10, e.SegmentsSkippedByLastTraceLookup);
    }

    [Fact]
    public async Task Every_planted_trace_is_still_found_in_full()
    {
        // The control for all the skipping above: an index that skipped one segment too many
        // would show up here as a trace that came back short or empty.
        using var e = Engine(Dir("all"));
        var planted = BuildSegments(e, segments: 8);

        for (int s = 0; s < planted.Length; s++)
        {
            var spans = await Read(e, planted[s]);
            Assert.Equal(3, spans.Count);
            Assert.All(spans, x => Assert.Equal(planted[s], x.TraceId));
        }
        _out.WriteLine($"{planted.Length} traces, each complete, each costing one segment open");
    }

    [Fact]
    public async Task A_trace_split_across_two_segments_opens_both()
    {
        // NOT A CORNER CASE. Spans arrive over time and a flush lands between them, so a trace
        // really does straddle two segments. Skipping the second would draw a waterfall missing
        // half its spans — and it would do it silently.
        using var e = Engine(Dir("split"));
        var straddler = Id(4242);

        WriteSpan(e, straddler, 1, _baseNano + 1 * Ms);
        WriteSpan(e, straddler, 2, _baseNano + 2 * Ms);
        e.FlushHotTier();

        BuildSegments(e, segments: 6);           // noise in between

        WriteSpan(e, straddler, 3, _baseNano + 900_000 * Ms);
        e.FlushHotTier();

        var spans = await Read(e, straddler);
        _out.WriteLine($"straddling trace: {spans.Count} spans, opened "
                     + $"{e.SegmentsOpenedByLastTraceLookup} of {e.ColdSegmentCountForTest}");

        Assert.Equal(3, spans.Count);
        Assert.Equal(2, e.SegmentsOpenedByLastTraceLookup);
    }

    [Fact]
    public async Task An_uncovered_segment_is_always_opened()
    {
        // The safety direction. Coverage is what makes a miss believable; without it the segment
        // has to be read, however confident the index sounds.
        string dir = Dir("uncovered");
        TraceId[] planted;
        using (var e = Engine(dir))
        {
            planted = BuildSegments(e, segments: 6);
        }

        // Take the runs away, exactly as an operator or a failed disk would. The catalog still
        // names the segments; nothing vouches for them any more.
        foreach (var f in Directory.EnumerateFiles(dir, "*.tix").ToList()) File.Delete(f);

        using var reopened = Engine(dir);
        reopened.LoadColdSegments();
        _out.WriteLine($"after deleting every .tix: catalog {reopened.CatalogCountsForTest}, "
                     + $"runs {reopened.IndexStatsForTest.Runs}");
        Assert.Equal(0, reopened.CatalogCountsForTest.Covered);

        var spans = await Read(reopened, planted[3]);
        _out.WriteLine($"opened {reopened.SegmentsOpenedByLastTraceLookup} of 6 with no index");

        Assert.Equal(3, spans.Count);                                   // still correct
        Assert.Equal(0, reopened.SegmentsSkippedByLastTraceLookup);     // and nothing was skipped
    }

    [Fact]
    public async Task Coverage_survives_a_restart()
    {
        string dir = Dir("restart");
        TraceId[] planted;
        using (var e = Engine(dir))
        {
            planted = BuildSegments(e, segments: 9);
        }

        using var reopened = Engine(dir);
        reopened.LoadColdSegments();
        _out.WriteLine($"reopened: catalog {reopened.CatalogCountsForTest}, "
                     + $"runs {reopened.IndexStatsForTest.Runs}");
        Assert.Equal(9, reopened.CatalogCountsForTest.Covered);

        var spans = await Read(reopened, planted[5]);
        Assert.Equal(3, spans.Count);
        Assert.Equal(1, reopened.SegmentsOpenedByLastTraceLookup);
    }

    [Fact]
    public async Task Retention_takes_the_run_with_the_segment_and_the_rest_keep_working()
    {
        string dir = Dir("prune");
        using var e = Engine(dir);

        long oldNano = DateTimeOffset.UtcNow.AddDays(-30).ToUnixTimeMilliseconds() * Ms;
        var doomed = Id(70_001);
        for (int k = 0; k < 3; k++) WriteSpan(e, doomed, (ulong)(900 + k), oldNano + k * Ms);
        e.FlushHotTier();

        // Inside the TTL, unlike _baseNano which is fixed in the past — the point of this test
        // is that ONE segment expires, not that they all do.
        long recent = DateTimeOffset.UtcNow.AddHours(-1).ToUnixTimeMilliseconds() * Ms;
        var planted = BuildSegments(e, segments: 4, from: recent);
        Assert.Equal(5, e.ColdSegmentCountForTest);

        int pruned = await e.PruneAsync(TimeSpan.FromDays(7));
        _out.WriteLine($"pruned {pruned}; catalog {e.CatalogCountsForTest}, runs {e.IndexStatsForTest.Runs}");

        Assert.Equal(1, pruned);
        Assert.Equal(4, e.CatalogCountsForTest.Segments);
        Assert.Equal(4, e.CatalogCountsForTest.Covered);
        Assert.Equal(4, e.IndexStatsForTest.Runs);
        Assert.Empty(Directory.EnumerateFiles(dir, "*.tix").Where(f => !File.Exists(Path.ChangeExtension(f, ".trc"))));

        // The survivors still resolve in one open.
        var spans = await Read(e, planted[2]);
        Assert.Equal(3, spans.Count);
        Assert.Equal(1, e.SegmentsOpenedByLastTraceLookup);
    }

    [Fact]
    public async Task A_compaction_leaves_one_run_that_answers_for_everything_it_merged()
    {
        string dir = Dir("compact");
        using var e = Engine(dir);

        // Two same-size segments in one 24 h window is what SelectCompactionBatch takes.
        var a = Id(11); var b = Id(22);
        for (int k = 0; k < 3; k++) WriteSpan(e, a, (ulong)(1 + k), _baseNano + k * Ms);
        for (int t = 0; t < 40; t++) for (int k = 0; k < 2; k++)
            WriteSpan(e, Id(5000 + t), (ulong)(100 + t * 2 + k), _baseNano + (50 + t) * Ms);
        e.FlushHotTier();
        for (int k = 0; k < 3; k++) WriteSpan(e, b, (ulong)(500 + k), _baseNano + (200 + k) * Ms);
        for (int t = 0; t < 40; t++) for (int k = 0; k < 2; k++)
            WriteSpan(e, Id(6000 + t), (ulong)(700 + t * 2 + k), _baseNano + (250 + t) * Ms);
        e.FlushHotTier();
        Assert.Equal(2, e.ColdSegmentCountForTest);

        e.CompactSmallSegments();
        Assert.Equal(1, e.ColdSegmentCountForTest);
        _out.WriteLine($"after compaction: catalog {e.CatalogCountsForTest}, runs {e.IndexStatsForTest.Runs}");

        // ONE run, covering the merged segment — the sources' runs went with their files.
        Assert.Equal(1, e.IndexStatsForTest.Runs);
        Assert.Equal(1, e.CatalogCountsForTest.Covered);
        Assert.Single(Directory.EnumerateFiles(dir, "*.tix"));

        // And traces from BOTH sources still resolve through it.
        foreach (var id in new[] { a, b })
        {
            var spans = await Read(e, id);
            Assert.Equal(3, spans.Count);
            Assert.Equal(1, e.SegmentsOpenedByLastTraceLookup);
        }
    }
}
