using System.Globalization;
using Ameto.Core;
using Ameto.Tracing;
using Ameto.Tracing.Ingestion;
using Ameto.Tracing.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// TS#3 (= TI#4): THE TRACE CAPS IN BYTES, FROM THE HOST.
///
/// <para><c>HotFlushThreshold</c> 50 000, <c>CompactionThreshold</c> 60 000,
/// <c>MaxSpansPerPass</c> 120 000 and the ring's 65 536 slots were constants, and
/// <c>TracesOptions</c> had three settings, none about memory. A span count is the wrong unit — the
/// same 50 000 is 27 MB of ordinary spans and 250-500 MB of spans carrying a SQL statement — and a
/// 512 MB stand got exactly what a 64 GB box got. These facts pin what the budgets resolve to on
/// the stand and on a large host, that the tier flushes on bytes, that compaction plans and loads
/// against bytes, and that the ring's capacity is reachable from configuration at all.</para>
/// </summary>
public sealed class TraceMemoryBudgetTests : IDisposable
{
    private const long MB = 1024 * 1024;
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-tbudget-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long              _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public TraceMemoryBudgetTests(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_root);
    }

    public void Dispose() { try { Directory.Delete(_root, true); } catch { } }

    /// <summary>The stand as a real 512 MB container presents itself: 384 MB heap limit, 512 MB physical.</summary>
    private static MemoryBudgets Stand => MemoryBudgets.Derive(managedLimitBytes: 384 * MB, physicalLimitBytes: 512 * MB);

    /// <summary>A CI runner or a developer box: the caps bind, not the fractions.</summary>
    private static MemoryBudgets Large => MemoryBudgets.Derive(64 * 1024 * MB, 64 * 1024 * MB);

    /// <summary>The ordinary span every figure in the plan is quoted against: eight attributes, a 375-byte blob.</summary>
    private const int OrdinaryBlob = 375;

    private string Dir(string name)
    {
        string d = Path.Combine(_root, name);
        Directory.CreateDirectory(d);
        return d;
    }

    private static string N(double v) => v.ToString("N0", CultureInfo.InvariantCulture);

    [Fact]
    public void The_derived_traces_caps_on_a_384_MB_managed_limit_are_what_the_plan_says()
    {
        var o = new TracesOptions();

        long tier      = o.HotTierMaxBytesFor(Stand);
        long pass      = o.MergeBudgetBytesFor(Stand);
        long threshold = TraceStorageEngine.CompactionThresholdBytesFor(pass);

        long ordinaryHot  = TraceStorageEngine.HotSpanBytes(OrdinaryBlob);
        long ordinaryBack = TraceStorageEngine.ReadBackSpanBytes(OrdinaryBlob);

        _out.WriteLine($"stand  tier {tier / (double)MB:F1} MB = {N(tier / (double)ordinaryHot)} ordinary spans; "
                     + $"pass {pass / (double)MB:F1} MB = {N(pass / (double)ordinaryBack)} spans read back; "
                     + $"candidate below {threshold / (double)MB:F1} MB = {N(threshold / (double)ordinaryBack)} spans");

        // THE PLAN'S "≈ 20 MB, then byte-budgeted" FOR THE TIER: 5 % of the 384 MB heap limit.
        Assert.Equal((long)(384 * MB * MemoryBudgets.TraceHotTierFraction), tier);
        Assert.InRange(tier, 20 * 1000 * 1000, 21 * 1000 * 1000);
        // What that buys in ordinary spans — three quarters of the 50 000 a large host holds.
        Assert.InRange(tier / ordinaryHot, 37_000, 38_000);

        // THE PASS: 6 % of the heap limit, 24 MB — against the 199 MB a pass of 120 000 spans cost
        // at main, and the 73 MB the cap still allows a large host.
        Assert.Equal((long)(384 * MB * MemoryBudgets.TraceMergeFraction), pass);
        Assert.InRange(pass, 24 * 1000 * 1000, 25 * 1000 * 1000);

        // AND THE CONSEQUENCE, PINNED SO NOBODY DISCOVERS IT: a full tier read back (~22.8 MB) is
        // heavier than half a pass, so on the stand a full flush is NOT a compaction candidate. The
        // pass can afford 1.2 tiers, not the two a pair needs; what merges there is the small
        // segments a quiet hour's timed flushes leave. See CompactionThresholdBytesFor.
        long fullTierReadBack = tier / ordinaryHot * ordinaryBack;
        Assert.True(fullTierReadBack >= threshold,
            $"a full stand tier weighs {fullTierReadBack:N0} B read back, under the {threshold:N0} B threshold");
    }

    [Fact]
    public void On_a_large_host_the_trace_caps_are_what_they_always_were()
    {
        var o = new TracesOptions();

        long tier = o.HotTierMaxBytesFor(Large);
        long pass = o.MergeBudgetBytesFor(Large);

        Assert.Equal(MemoryBudgets.TraceHotTierCapBytes, tier);
        Assert.Equal(MemoryBudgets.TraceMergeCapBytes,   pass);

        // The count cap still binds first for the ordinary span: 27 MB is a touch MORE than 50 000
        // of them, so the flush cadence every trace test names is unchanged.
        Assert.True(tier / TraceStorageEngine.HotSpanBytes(OrdinaryBlob) >= 50_000);

        // And the byte threshold is the old 60 000 spans, to the span, for a segment of unknown
        // weight — EstimatedSegmentBytes prices it at the cap's own 73 MB / 120 000.
        long threshold = TraceStorageEngine.CompactionThresholdBytesFor(pass);
        Assert.True(TraceStorageEngine.EstimatedSegmentBytes(Seg(59_999, 0, 1)) <  threshold);
        Assert.True(TraceStorageEngine.EstimatedSegmentBytes(Seg(60_000, 0, 1)) >= threshold);
        Assert.True(TraceStorageEngine.EstimatedSegmentBytes(Seg(120_000, 0, 1)) <= pass);
        Assert.True(TraceStorageEngine.EstimatedSegmentBytes(Seg(120_001, 0, 1)) >  pass);

        // The ring: 65 536 slots unless told otherwise.
        Assert.Equal(TracesOptions.DefaultRingCapacity, o.EffectiveRingCapacity);
    }

    [Fact]
    public void An_explicit_setting_wins_over_the_host()
    {
        var o = new TracesOptions { HotTierMaxBytes = 3 * MB, MergeBudgetBytes = 7 * MB, RingCapacity = 5_000 };

        Assert.Equal(3 * MB, o.HotTierMaxBytesFor(Stand));
        Assert.Equal(7 * MB, o.MergeBudgetBytesFor(Large));
        Assert.Equal(8_192,  o.EffectiveRingCapacity);            // rounded up to a power of two
        Assert.Equal(1_024,  new TracesOptions { RingCapacity = 3 }.EffectiveRingCapacity);   // clamped
    }

    /// <summary>
    /// THE BYTE HALF OF THE FLUSH TRIGGER. Spans carrying 10 KB of attributes each, a 1 MB tier:
    /// the flush has to start near a hundred of them, where the count half would have waited for
    /// fifty thousand — 500 MB.
    /// </summary>
    [Fact]
    public void A_tier_of_heavy_spans_flushes_on_its_bytes_long_before_its_count()
    {
        using var e = new TraceStorageEngine(Dir("heavy"), NullLogger<TraceStorageEngine>.Instance,
                                             options: new TracesOptions { HotTierMaxBytes = MB });
        byte[] blob = Blob(10_000);
        long   per  = TraceStorageEngine.HotSpanBytes(blob.Length);
        int    fits = (int)(MB / per);                      // spans the budget holds before it is reached

        for (int i = 0; i < fits; i++) e.WriteSpan(Span(i, blob));
        e.WaitForFlushForTest();
        Assert.Equal(0, e.ColdSegmentCountForTest);          // just under the budget: nothing yet
        Assert.Equal(fits * per, e.HotBytesForTest);

        e.WriteSpan(Span(fits, blob));                       // this one reaches it
        e.WaitForFlushForTest();

        _out.WriteLine($"{fits + 1} spans of {blob.Length:N0} B attributes flushed a {MB:N0} B tier "
                     + $"(the count half waits for 50 000)");
        Assert.Equal(1, e.ColdSegmentCountForTest);
        Assert.Equal(0, e.HotBytesForTest);                  // the tier starts over, and so does its count
    }

    /// <summary>
    /// A FAILED FLUSH PUTS ITS BYTES BACK WITH ITS SPANS. The snapshot's bytes leave the tier when it
    /// is detached; if the build throws, the spans are restored in front of whatever arrived since,
    /// and the budget must see them again — or the next flush of that tier comes a whole snapshot
    /// late.
    /// </summary>
    [Fact]
    public void A_failed_flush_returns_its_bytes_to_the_tier_with_its_spans()
    {
        using var e = new TraceStorageEngine(Dir("restore"), NullLogger<TraceStorageEngine>.Instance);
        byte[] blob = Blob(1_000);
        for (int i = 0; i < 10; i++) e.WriteSpan(Span(i, blob));
        long before = e.HotBytesForTest;
        Assert.Equal(10 * TraceStorageEngine.HotSpanBytes(blob.Length), before);

        e._beforeSegmentWrite = static () => throw new IOException("the disk said no");
        e.FlushHotTier();                                    // fails, restores
        e._beforeSegmentWrite = null;

        Assert.Equal(0, e.ColdSegmentCountForTest);
        Assert.Equal(before, e.HotBytesForTest);
    }

    /// <summary>
    /// THE PLANNER PRICES A SEGMENT BY WHAT IT HOLDS. A 5 000-span segment is nothing by its count
    /// (3 MB at the cap's per-span figure) — and 20 MB when this process weighed it as it wrote it.
    /// Four of them: by count all four fit a 73 MB pass; by weight three do. At 40 MB each none is
    /// a candidate at all, however few spans it has.
    /// </summary>
    [Fact]
    public void The_planner_prices_a_segment_this_process_weighed_by_its_weight_not_its_count()
    {
        const long Twenty = 20 * 1000 * 1000, Forty = 40 * 1000 * 1000;
        var light = new[] { Seg(5_000, 0, 1, Twenty), Seg(5_000, 2, 3, Twenty), Seg(5_000, 4, 5, Twenty), Seg(5_000, 6, 7, Twenty) };
        var heavy = new[] { Seg(5_000, 0, 1, Forty),  Seg(5_000, 2, 3, Forty) };
        var unweighed = new[] { Seg(5_000, 0, 1), Seg(5_000, 2, 3), Seg(5_000, 4, 5), Seg(5_000, 6, 7) };

        Assert.Equal(4, TraceStorageEngine.SelectCompactionBatch(unweighed, MemoryBudgets.TraceMergeCapBytes).Count);
        Assert.Equal(3, TraceStorageEngine.SelectCompactionBatch(light,     MemoryBudgets.TraceMergeCapBytes).Count);
        Assert.Empty(TraceStorageEngine.SelectCompactionBatch(heavy, MemoryBudgets.TraceMergeCapBytes));
    }

    [Fact]
    public void On_the_stand_budget_small_segments_pair_and_a_full_tier_is_left_as_it_is()
    {
        long pass = new TracesOptions().MergeBudgetBytesFor(Stand);
        long full = new TracesOptions().HotTierMaxBytesFor(Stand) / TraceStorageEngine.HotSpanBytes(OrdinaryBlob);

        var fullTiers = new[] { Seg((int)full, 0, 1), Seg((int)full, 2, 3) };
        var quietHour = new[] { Seg(4_000, 0, 1), Seg(4_000, 2, 3), Seg(4_000, 4, 5) };

        Assert.Empty(TraceStorageEngine.SelectCompactionBatch(fullTiers, pass));
        Assert.Equal(3, TraceStorageEngine.SelectCompactionBatch(quietHour, pass).Count);
    }

    /// <summary>
    /// THE LOADER'S BYTE GUARD, for the case the plan cannot see: segments found on disk at startup
    /// carry no weight, so they are priced from their span count and their file — and 200 spans of
    /// 10 KB that compress to nothing are 2 MB read back, not the 120 KB their count says. Three of
    /// them: the plan admits all three.
    ///
    /// <para>Under a 5 MB pass two fit (4.1 MB) and merge; the third, read, would take the pass to
    /// 6.2 MB and is PUT BACK, weighed. Under a 3 MB pass not even two fit: nothing merges, all
    /// three are weighed, and the planner never proposes them again. What a pass writes never weighs
    /// more than a pass may hold — it used to overshoot by the last segment it read.</para>
    /// </summary>
    [Theory]
    [InlineData(5, 2)]
    [InlineData(3, 3)]
    public void A_pass_keeps_to_its_byte_budget_when_the_plan_underestimates_it(int budgetMb, int segmentsAfter)
    {
        string dir  = Dir("loader" + budgetMb);
        byte[] blob = Blob(10_000);

        using (var writer = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance))
        {
            for (int s = 0; s < 3; s++)
            {
                for (int t = 0; t < 200; t++) writer.WriteSpan(Span(s * 1_000 + t, blob));
                writer.FlushHotTier();
            }
        }

        // A restart: the segments come back from disk with no weight, so the plan is the estimate's.
        long budget = budgetMb * MB;
        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance,
                                             options: new TracesOptions { MergeBudgetBytes = budget });
        e.LoadColdSegments();
        Assert.Equal(3, e.ColdSegmentCountForTest);
        Assert.All(e.ColdSegmentsForTest, static s => Assert.Equal(0, s.WeightBytes));
        Assert.Equal(3, TraceStorageEngine.SelectCompactionBatch(e.ColdSegmentsForTest, budget).Count);

        e.CompactSmallSegments();

        var after = e.ColdSegmentsForTest;
        _out.WriteLine($"3 segments of 200 x 10 KB under a {budgetMb} MB pass -> {after.Length} "
                     + $"({string.Join(", ", after.Select(static s => $"{s.SpanCount}:{s.WeightBytes:N0} B"))})");
        Assert.Equal(segmentsAfter, after.Length);
        // Nothing a pass wrote weighs more than a pass may hold…
        Assert.All(after.Where(static s => s.SpanCount > 200), s => Assert.True(s.WeightBytes <= budget,
            $"a merged segment of {s.SpanCount} spans weighs {s.WeightBytes:N0} B, past the {budget:N0} B budget"));
        // …and the planner proposes nothing more: what it read is weighed, and what it did not read
        // (a segment with no peer left) it has no reason to read.
        Assert.Empty(TraceStorageEngine.SelectCompactionBatch(after, budget));
    }

    /// <summary>
    /// F1 — THE COMPACTION LIVELOCK AFTER A RESTART. The oldest segment on disk is priced from its
    /// span count (a thousand spans: 0.6 MB, under the 1 MB threshold of a 2 MB pass) but weighs
    /// 3.2 MB read back — its attributes compress to almost nothing, so its file says nothing either.
    /// It is the oldest candidate, so it seeds the first batch; the loader reads it, is past its
    /// budget, and stops with one segment — and a pass that merged nothing used to return false with
    /// the measured weight thrown away. Every later run re-planned the same seed, re-read it in full,
    /// and merged nothing; the two ordinary segments behind it waited for retention.
    ///
    /// <para>The queue must advance: the heavy seed is weighed once, drops out of the candidates,
    /// and its peers merge — in the same run, and on no later run is it re-read.</para>
    /// </summary>
    [Fact]
    public void After_a_restart_one_oversized_seed_does_not_stall_the_queue_behind_it()
    {
        string dir = Dir("livelock");
        using (var writer = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance))
        {
            byte[] heavy = Blob(3_000);                                   // 'x' repeated: LZ4 crushes it
            for (int t = 0; t < 1_000; t++) writer.WriteSpan(Span(t, heavy));
            writer.FlushHotTier();                                        // the oldest: the seed
            for (int s = 1; s < 3; s++)
            {
                for (int t = 0; t < 1_000; t++) writer.WriteSpan(Span(s * 10_000 + t, []));
                writer.FlushHotTier();                                    // two ordinary peers, same tier
            }
        }

        // The restart: every segment comes back unweighed.
        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance,
                                             options: new TracesOptions { MergeBudgetBytes = 2 * MB });
        e.LoadColdSegments();
        Assert.All(e.ColdSegmentsForTest, static s => Assert.Equal(0, s.WeightBytes));
        var plan = TraceStorageEngine.SelectCompactionBatch(e.ColdSegmentsForTest, 2 * MB);
        Assert.Equal(3, plan.Count);                                      // the heavy one seeds it
        long seedFile = new FileInfo(plan[0].FilePath).Length;
        _out.WriteLine($"seed: {plan[0].SpanCount} spans, file {seedFile:N0} B, priced "
                     + $"{TraceStorageEngine.EstimatedSegmentBytes(plan[0]):N0} B, weighs "
                     + $"{1_000 * TraceStorageEngine.ReadBackSpanBytes(Blob(3_000).Length):N0} B read back");

        for (int run = 0; run < 3; run++) e.CompactSmallSegments();      // the hourly worker, three times

        var after = e.ColdSegmentsForTest;
        _out.WriteLine($"after three runs: {after.Length} segments ({string.Join(", ", after.Select(static s => $"{s.SpanCount}:{s.WeightBytes:N0} B"))})");
        Assert.Equal(2, after.Length);                                    // the peers merged
        Assert.Contains(after, static s => s.SpanCount == 2_000);
        var seed = Assert.Single(after, static s => s.SpanCount == 1_000);
        Assert.True(seed.WeightBytes >= 2 * MB, "the seed's measured weight was not kept, so it will be re-read every run");
        Assert.Empty(TraceStorageEngine.SelectCompactionBatch(after, 2 * MB));
    }

    /// <summary>
    /// L1 — A SEGMENT THAT READS BACK EMPTY IS WEIGHED ONCE, NOT ON EVERY PASS. The writer never makes
    /// an empty segment, but a damaged footer does: its trace-index offset pointing at the first
    /// block, the reader walks no blocks and hands back no spans, without throwing. Two such
    /// segments: a pass reads both, measures 0 bytes, merges nothing and writes the weights back.
    /// A weight of 0 is the "never measured" value, so every pass saw a change, re-planned the same
    /// two segments and read them again — until the run's 500-pass safety valve, on every run.
    /// </summary>
    [Fact]
    public void Segments_that_read_back_empty_are_weighed_once_and_the_run_ends()
    {
        string dir = Dir("empty");
        for (int s = 0; s < 2; s++)
        {
            var spans = new List<SpanRecord>();
            for (int t = 0; t < 3; t++)
                spans.Add(new SpanRecord
                {
                    TraceId = new TraceId(0xE3, (ulong)(s * 10 + t + 1)), SpanId = new SpanId((ulong)(s * 10 + t + 1)),
                    StartTimeUnixNano = _baseNano + (s * 10 + t) * Ms, DurationNanos = Ms,
                    Name = "op", ServiceName = "billing", Kind = SpanKind.Server,
                });
            var info = SpanWriter.Write(dir, spans);
            // The footer's trace-index offset (its first 8 of 28 bytes) moved back to the first
            // block (offset 27, just past the header): the reader now finds no blocks before it.
            using var fs = new FileStream(info.FilePath, FileMode.Open, FileAccess.ReadWrite);
            fs.Seek(-28, SeekOrigin.End);
            fs.Write(BitConverter.GetBytes(27UL));
        }

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e.LoadColdSegments();
        Assert.Equal(2, e.ColdSegmentCountForTest);
        Assert.All(e.ColdSegmentsForTest, static s => Assert.Empty(SpanReader.ReadAll(s.FilePath)));
        Assert.Equal(2, TraceStorageEngine.SelectCompactionBatch(e.ColdSegmentsForTest).Count);

        e.CompactSmallSegments();
        _out.WriteLine($"two empty segments: {e.LastCompactionPassesForTest} pass(es)");
        Assert.True(e.LastCompactionPassesForTest <= 2,
            $"the run made {e.LastCompactionPassesForTest} passes over two empty segments");
        Assert.All(e.ColdSegmentsForTest, static s => Assert.True(s.WeightBytes > 0, "an empty segment was left unweighed"));

        e.CompactSmallSegments();                                          // the next run: nothing new to learn
        Assert.Equal(0, e.LastCompactionPassesForTest);
    }

    /// <summary>
    /// SEGMENTS THAT READ BACK EMPTY DO NOT STARVE THE BATCH BEHIND THEM (#94). Weighing an empty
    /// segment once (L1, above) ended the run quickly, but the planner still chose it: two empty
    /// segments of one tier and 24 h window are the OLDEST batch, so every pass of every run read
    /// them, merged nothing, returned "no change" — and a real batch of the next tier, an hour later,
    /// was never reached; it waited for retention. Now a segment that reads back empty leaves the
    /// plan (and stays on disk: its header claims spans a repair might still recover).
    ///
    /// <para>Reverted (no quarantine): after three runs the two real segments are still unmerged —
    /// four cold segments instead of three.</para>
    /// </summary>
    [Fact]
    public void Segments_that_read_back_empty_do_not_hold_up_the_batch_behind_them()
    {
        string dir = Dir("empty-ahead");
        var empties = new List<string>(2);
        for (int s = 0; s < 2; s++)
        {
            var spans = new List<SpanRecord>();
            for (int t = 0; t < 3; t++)                                    // 3 spans: tier 0
                spans.Add(new SpanRecord
                {
                    TraceId = new TraceId(0xE4, (ulong)(s * 10 + t + 1)), SpanId = new SpanId((ulong)(s * 10 + t + 1)),
                    StartTimeUnixNano = _baseNano + (s * 10 + t) * Ms, DurationNanos = Ms,
                    Name = "op", ServiceName = "billing", Kind = SpanKind.Server,
                });
            var info = SpanWriter.Write(dir, spans);
            // The damaged footer of the L1 test: the reader walks no blocks, and throws nothing.
            using var fs = new FileStream(info.FilePath, FileMode.Open, FileAccess.ReadWrite);
            fs.Seek(-28, SeekOrigin.End);
            fs.Write(BitConverter.GetBytes(27UL));
            empties.Add(info.FilePath);
        }
        for (int s = 0; s < 2; s++)
        {
            var spans = new List<SpanRecord>();
            for (int t = 0; t < 20; t++)                                   // 20 spans: tier 2, an hour later
                spans.Add(new SpanRecord
                {
                    TraceId = new TraceId(0xE5, (ulong)(s * 100 + t + 1)), SpanId = new SpanId((ulong)(s * 100 + t + 1)),
                    StartTimeUnixNano = _baseNano + 3_600_000 * Ms + (s * 100 + t) * Ms, DurationNanos = Ms,
                    Name = "op", ServiceName = "billing", Kind = SpanKind.Server,
                });
            SpanWriter.Write(dir, spans);
        }

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        e.LoadColdSegments();
        Assert.Equal(4, e.ColdSegmentCountForTest);
        var first = TraceStorageEngine.SelectCompactionBatch(e.ColdSegmentsForTest);
        Assert.Equal(empties.Order(StringComparer.Ordinal), first.Select(static s => s.FilePath).Order(StringComparer.Ordinal));  // the empties plan first

        for (int run = 0; run < 3; run++) e.CompactSmallSegments();      // the hourly worker, three times

        var after = e.ColdSegmentsForTest;
        _out.WriteLine($"after three runs: {after.Length} segments ({string.Join(", ", after.Select(static s => $"{s.SpanCount} spans"))})");
        Assert.Equal(3, after.Length);                                    // the real pair merged…
        Assert.Contains(after, static s => s.SpanCount == 40);
        foreach (string path in empties)                                  // …and the empties stayed, on disk and listed
        {
            Assert.True(File.Exists(path));
            Assert.Contains(after, s => s.FilePath == path);
        }

        e.CompactSmallSegments();                                          // nothing left to plan
        Assert.Equal(0, e.LastCompactionPassesForTest);
    }

    /// <summary>
    /// AN UNWEIGHED SEGMENT IS NEVER PRICED BELOW ITS OWN FILE. Five thousand spans are 3 MB by the
    /// count; a 40 MB file of them cannot weigh less than 40 MB read back, so it is not a candidate —
    /// it is kept out of the plan before anything has had to read it to find that out. The file floor
    /// only ever raises an estimate: two segments of ordinary spans, whose files are far lighter than
    /// their count, still pair.
    /// </summary>
    [Fact]
    public void An_unweighed_segment_is_priced_at_no_less_than_its_file()
    {
        const long Forty = 40L * 1000 * 1000;
        var heavy    = new[] { Seg(5_000, 0, 1, fileBytes: Forty), Seg(5_000, 2, 3, fileBytes: Forty) };
        var ordinary = new[] { Seg(5_000, 0, 1, fileBytes: 200_000), Seg(5_000, 2, 3, fileBytes: 200_000) };

        Assert.Equal(Forty, TraceStorageEngine.EstimatedSegmentBytes(heavy[0]));
        Assert.Empty(TraceStorageEngine.SelectCompactionBatch(heavy, MemoryBudgets.TraceMergeCapBytes));
        Assert.Equal(2, TraceStorageEngine.SelectCompactionBatch(ordinary, MemoryBudgets.TraceMergeCapBytes).Count);
    }

    /// <summary>
    /// A SEGMENT THIS PROCESS FLUSHED IS WEIGHED AS IT IS WRITTEN — so the planner prices it by what
    /// it holds without reading it again.
    /// </summary>
    [Fact]
    public void A_flushed_segment_carries_its_read_back_weight()
    {
        using var e = new TraceStorageEngine(Dir("weigh"), NullLogger<TraceStorageEngine>.Instance);
        byte[] blob = Blob(2_000);
        for (int i = 0; i < 50; i++) e.WriteSpan(Span(i, blob));
        e.FlushHotTier();

        var seg = Assert.Single(e.ColdSegmentsForTest);
        Assert.Equal(50 * TraceStorageEngine.ReadBackSpanBytes(blob.Length), seg.WeightBytes);
        Assert.Equal(seg.WeightBytes, TraceStorageEngine.EstimatedSegmentBytes(seg));
    }

    /// <summary>
    /// <c>Traces:RingCapacity</c> REACHES THE RING. The ring used to be registered with
    /// <c>AddSingleton&lt;SpanRingBuffer&gt;()</c> — the parameterless constructor — so no setting
    /// could have sized it.
    /// </summary>
    [Fact]
    public void The_ring_capacity_is_read_from_configuration()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance);
        services.AddSingleton(typeof(ILogger<>), typeof(Logger<>));
        services.AddSingleton(new ServerOptions { Traces = new TracesOptions { RingCapacity = 5_000 } });
        services.AddAmetoTracing(Dir("di"));

        using var sp = services.BuildServiceProvider();
        var ring = sp.GetRequiredService<SpanRingBuffer>();

        Assert.Equal(8_192, ring.Capacity);
    }

    // ── fixtures ────────────────────────────────────────────────────────────

    private SpanIngestItem Span(int i, byte[] blob) => new()
    {
        TraceId           = new TraceId(0xB0D6E7UL, (ulong)(i / 10 + 1)),
        SpanId            = new SpanId((ulong)(i + 1)),
        StartTimeUnixNano = _baseNano + i * Ms,
        DurationNanos     = 2 * Ms,
        Name              = "SELECT payments",
        ServiceName       = "billing",
        Kind              = SpanKind.Client,
        AttributesBytes   = blob,
    };

    /// <summary>A one-entry msgpack map whose value is <paramref name="bytes"/> long in total.</summary>
    private static byte[] Blob(int bytes)
    {
        var buf = new System.Buffers.ArrayBufferWriter<byte>(bytes + 16);
        var w   = new MessagePack.MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("db.statement");
        w.Write(new string('x', Math.Max(0, bytes - 20)));
        w.Flush();
        return buf.WrittenMemory.ToArray();
    }

    private static SpanSegmentInfo Seg(int spanCount, long minNano, long maxNano, long weight = 0, long fileBytes = 0) => new()
    {
        FilePath      = $"spans-{minNano}-{maxNano}-{spanCount}-{weight}.trc",
        MinStartNano  = minNano,
        MaxStartNano  = maxNano,
        SpanCount     = spanCount,
        Services      = ["billing"],
        FormatVersion = 4,
        WeightBytes   = weight,
        FileBytes     = fileBytes,
    };
}
