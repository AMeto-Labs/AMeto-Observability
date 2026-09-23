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
    /// carry no weight, so they are priced from their span count — and 200 spans of 10 KB each are
    /// 2 MB, not the 120 KB their count says. Three of them under a 3 MB pass: the plan admits all
    /// three, and the pass must stop after the second, whose read already spent the budget.
    /// </summary>
    [Fact]
    public void A_pass_stops_loading_at_its_byte_budget_when_the_plan_underestimates_it()
    {
        string dir  = Dir("loader");
        byte[] blob = Blob(10_000);

        using (var writer = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance))
        {
            for (int s = 0; s < 3; s++)
            {
                for (int t = 0; t < 200; t++) writer.WriteSpan(Span(s * 1_000 + t, blob));
                writer.FlushHotTier();
            }
        }

        // A restart: the segments come back from disk with no weight, so the plan is the count's.
        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance,
                                             options: new TracesOptions { MergeBudgetBytes = 3 * MB });
        e.LoadColdSegments();
        Assert.Equal(3, e.ColdSegmentCountForTest);
        Assert.All(e.ColdSegmentsForTest, static s => Assert.Equal(0, s.WeightBytes));
        Assert.Equal(3, TraceStorageEngine.SelectCompactionBatch(e.ColdSegmentsForTest, 3 * MB).Count);

        e.CompactSmallSegments();

        var after = e.ColdSegmentsForTest;
        _out.WriteLine($"3 segments of 200 x 10 KB under a 3 MB pass -> {after.Length} "
                     + $"({string.Join(", ", after.Select(static s => s.SpanCount))} spans)");
        // Two merged (the second read took the pass past its budget), the third left for later.
        Assert.Equal(2, after.Length);
        Assert.Contains(after, static s => s.SpanCount == 400);
        Assert.Contains(after, static s => s.SpanCount == 200);
        // And the merged segment now carries the weight its pass measured.
        Assert.True(after.Single(static s => s.SpanCount == 400).WeightBytes
                    >= 400 * TraceStorageEngine.ReadBackSpanBytes(blob.Length));
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

    private static SpanSegmentInfo Seg(int spanCount, long minNano, long maxNano, long weight = 0) => new()
    {
        FilePath      = $"spans-{minNano}-{maxNano}-{spanCount}-{weight}.trc",
        MinStartNano  = minNano,
        MaxStartNano  = maxNano,
        SpanCount     = spanCount,
        Services      = ["billing"],
        FormatVersion = 4,
        WeightBytes   = weight,
    };
}
