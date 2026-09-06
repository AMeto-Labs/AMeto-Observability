using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE MIGRATION: data that already exists gets the fast path too, one segment at a time, and only
/// if the operator wants it.
///
/// <para>Every install that this ships to already has segments, and they were written before there
/// was an index to write beside them. Without a backfill the feature would only ever help data
/// recorded after the upgrade — the traces people actually go looking for are the older ones. So
/// the segments are brought in afterwards, in the background, at a pace the operator sets.</para>
///
/// <para>What must stay true throughout is that a half-finished migration is not a broken one. At
/// every point — none indexed, some indexed, one that refuses to be indexed — the answers have to
/// be identical to the answers before any of this existed, and the only thing that changes is how
/// much work they cost. That is what these tests check: correctness at each step, and the work
/// dropping as coverage grows.</para>
/// </summary>
public sealed class TraceIndexBackfillTests : IDisposable
{
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-backfill-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long              _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public TraceIndexBackfillTests(ITestOutputHelper output)
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

    private TraceId[] Build(TraceStorageEngine e, int segments, int tracesPer = 30)
    {
        var planted = new TraceId[segments];
        ulong span = 1;
        for (int s = 0; s < segments; s++)
        {
            for (int t = 0; t < tracesPer; t++)
            {
                var id = Id(s * 1000 + t);
                if (t == 0) planted[s] = id;
                for (int k = 0; k < 3; k++)
                    WriteSpan(e, id, span++, _baseNano + (s * 10_000 + t * 10 + k) * Ms);
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

    /// <summary>An install as it exists before the upgrade: segments on disk, no index anywhere.</summary>
    private (string Dir, TraceId[] Planted) LegacyInstall(string name, int segments)
    {
        string dir = Dir(name);
        TraceId[] planted;
        using (var e = Engine(dir)) { planted = Build(e, segments); }

        // Strip everything the index put there. What is left is exactly a pre-upgrade install.
        foreach (var f in Directory.EnumerateFiles(dir, "*.tix").ToList()) File.Delete(f);
        File.Delete(Path.Combine(dir, "traces.manifest"));
        return (dir, planted);
    }

    [Fact]
    public async Task Old_segments_are_brought_in_one_at_a_time_and_the_work_drops_as_they_are()
    {
        var (dir, planted) = LegacyInstall("progress", segments: 6);

        using var e = Engine(dir);
        e.LoadColdSegments();
        Assert.Equal((0, 6), e.IndexCoverage);           // adopted, named, indexed by nothing

        // Before any backfill the lookup is what it always was: every segment opened.
        var before = await Read(e, planted[4]);
        Assert.Equal(3, before.Count);
        Assert.Equal(6, e.SegmentsOpenedByLastTraceLookup);
        _out.WriteLine($"coverage 0/6 → opened {e.SegmentsOpenedByLastTraceLookup}");

        // ONE SEGMENT PER CALL. The pace belongs to the caller, not to this method.
        Assert.True(e.BackfillNextSegment());
        Assert.Equal(1, e.IndexCoverage.Covered);

        while (e.BackfillNextSegment()) { }
        Assert.Equal((6, 6), e.IndexCoverage);

        var after = await Read(e, planted[4]);
        _out.WriteLine($"coverage 6/6 → opened {e.SegmentsOpenedByLastTraceLookup}, "
                     + $"skipped {e.SegmentsSkippedByLastTraceLookup}");

        // Same answer, a fraction of the work.
        Assert.Equal(before.Select(s => s.SpanId), after.Select(s => s.SpanId));
        Assert.Equal(1, e.SegmentsOpenedByLastTraceLookup);
        Assert.Equal(5, e.SegmentsSkippedByLastTraceLookup);
    }

    [Fact]
    public async Task Half_a_migration_answers_exactly_like_none_of_it()
    {
        // The state the server is actually in for most of the backfill, and the one a bug would
        // hide in: some segments covered, some not, on the same lookup.
        var (dir, planted) = LegacyInstall("half", segments: 8);

        using var e = Engine(dir);
        e.LoadColdSegments();
        for (int i = 0; i < 4; i++) Assert.True(e.BackfillNextSegment());
        Assert.Equal(4, e.IndexCoverage.Covered);
        _out.WriteLine($"mid-migration coverage: {e.IndexCoverage}");

        // EVERY trace, on both sides of the line, complete.
        for (int s = 0; s < planted.Length; s++)
        {
            var spans = await Read(e, planted[s]);
            Assert.Equal(3, spans.Count);
            Assert.All(spans, x => Assert.Equal(planted[s], x.TraceId));
        }

        // And an absent trace still costs the uncovered segments and nothing more.
        await Read(e, Id(987_654));
        _out.WriteLine($"absent trace mid-migration: opened {e.SegmentsOpenedByLastTraceLookup}, "
                     + $"skipped {e.SegmentsSkippedByLastTraceLookup}");
        Assert.Equal(4, e.SegmentsOpenedByLastTraceLookup);
        Assert.Equal(4, e.SegmentsSkippedByLastTraceLookup);
    }

    [Fact]
    public void The_backfill_reports_when_there_is_nothing_left_to_do()
    {
        // The worker's stop condition. Returning "worked" forever would have it re-reading the
        // whole cold tier at its pause interval for the life of the process.
        var (dir, _) = LegacyInstall("done", segments: 3);

        using var e = Engine(dir);
        e.LoadColdSegments();

        int passes = 0;
        while (e.BackfillNextSegment()) { if (++passes > 50) break; }
        _out.WriteLine($"{passes} passes for 3 segments, then false");

        Assert.Equal(3, passes);
        Assert.False(e.BackfillNextSegment());
        Assert.Equal((3, 3), e.IndexCoverage);
    }

    [Fact]
    public async Task A_segment_that_cannot_be_indexed_is_tried_once_and_left_readable()
    {
        var (dir, planted) = LegacyInstall("bad", segments: 4);

        // Destroy one segment's trace index — the part the backfill reads — while leaving the
        // spans themselves alone. The block walk does not need the index; only the backfill does.
        var victim = Directory.EnumerateFiles(dir, "*.trc").OrderBy(f => f).Skip(1).First();
        var raw = File.ReadAllBytes(victim);
        // The footer's first eight bytes are traceIdxOffset; point it at nonsense.
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(raw.AsSpan(raw.Length - 28), long.MaxValue);
        File.WriteAllBytes(victim, raw);

        using var e = Engine(dir);
        e.LoadColdSegments();

        int passes = 0;
        while (e.BackfillNextSegment()) { if (++passes > 50) break; }
        _out.WriteLine($"{passes} passes over 4 segments; coverage {e.IndexCoverage}");

        // IT MUST NOT SPIN. A segment whose index will not parse will not parse next time either,
        // so it is recorded and skipped rather than picked again every pass for ever.
        Assert.True(passes <= 4, $"the backfill retried a hopeless segment ({passes} passes)");
        Assert.Equal(3, e.IndexCoverage.Covered);

        // And the traces in the OTHER segments are unaffected.
        foreach (int s in new[] { 0, 2, 3 })
        {
            var spans = await Read(e, planted[s]);
            Assert.Equal(3, spans.Count);
        }
    }

    [Fact]
    public async Task Coverage_earned_by_the_backfill_survives_a_restart()
    {
        var (dir, planted) = LegacyInstall("persist", segments: 5);

        using (var e = Engine(dir))
        {
            e.LoadColdSegments();
            while (e.BackfillNextSegment()) { }
            Assert.Equal((5, 5), e.IndexCoverage);
        }

        using var reopened = Engine(dir);
        reopened.LoadColdSegments();
        _out.WriteLine($"after restart: coverage {reopened.IndexCoverage}, "
                     + $"runs {reopened.IndexStatsForTest.Runs}");
        Assert.Equal((5, 5), reopened.IndexCoverage);

        var spans = await Read(reopened, planted[1]);
        Assert.Equal(3, spans.Count);
        Assert.Equal(1, reopened.SegmentsOpenedByLastTraceLookup);
    }

    [Fact]
    public async Task Turning_the_index_off_leaves_the_data_untouched()
    {
        // The rollback, and the reason this is safe to enable: withdrawing every claim of coverage
        // costs speed and nothing else. No span is rewritten and no .trc is opened.
        var (dir, planted) = LegacyInstall("rollback", segments: 5);

        using var e = Engine(dir);
        e.LoadColdSegments();
        while (e.BackfillNextSegment()) { }
        Assert.Equal((5, 5), e.IndexCoverage);

        var withIndex = await Read(e, planted[2]);
        Assert.Equal(1, e.SegmentsOpenedByLastTraceLookup);

        e.DisableTraceIndexForTest();

        var withoutIndex = await Read(e, planted[2]);
        _out.WriteLine($"index off → opened {e.SegmentsOpenedByLastTraceLookup} of 5, "
                     + $"same {withoutIndex.Count} spans");

        Assert.Equal(withIndex.Select(s => s.SpanId), withoutIndex.Select(s => s.SpanId));
        Assert.Equal(5, e.SegmentsOpenedByLastTraceLookup);
        Assert.Equal(0, e.SegmentsSkippedByLastTraceLookup);
    }
}
