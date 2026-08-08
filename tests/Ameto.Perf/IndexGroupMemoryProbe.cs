using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// The point of index groups, measured.
///
/// <para>Index-build state is the ceiling that kept segments small: the trigram accumulator
/// costs ~7.6 B per posting and scales with INDEXED TEXT BYTES, so a whole day of one level
/// would retain ~610 MB of managed dictionaries in a single build. v7 seals a group at a
/// payload budget and builds its indexes from a fresh builder, so what one build retains is
/// bounded by the GROUP, not by the file.</para>
///
/// <para>This probe measures the live GC heap at the moment each group's accumulators are at
/// their fullest — the instant before serialisation, with everything from earlier groups
/// already unreachable — and shows the peak staying flat while the segment quadruples.</para>
/// </summary>
public sealed class IndexGroupMemoryProbe : IDisposable
{
    /// <summary>Small enough to seal several groups out of a test-sized segment.</summary>
    private const long GroupBudget = 4L * 1024 * 1024;

    private readonly ITestOutputHelper _out;
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-groupmem-" + Guid.NewGuid().ToString("N"));

    public IndexGroupMemoryProbe(ITestOutputHelper o)
    {
        _out = o;
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, true); } catch { }
        // This probe deliberately grows the heap to tens of MB. Hand it back before the next
        // test class runs — Ameto.Perf disables parallelisation, so a fat gen2 left behind
        // lands squarely on whichever wall-clock probe happens to run next.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    [Fact]
    public void PeakIndexBuildMemoryIsBoundedByTheGroupNotTheFile()
    {
        const double MB = 1048576.0;

        // Warm every path once so JIT and pool growth do not land inside a measurement.
        Measure(4_000, GroupBudget, "warm");

        var g1 = Measure(12_000, GroupBudget, "grouped 1x");
        var g2 = Measure(24_000, GroupBudget, "grouped 2x");
        var g4 = Measure(48_000, GroupBudget, "grouped 4x");
        var un = Measure(48_000, long.MaxValue, "ungrouped 4x");

        _out.WriteLine("  events | groups |  peak live build state |  bytes/event");
        _out.WriteLine("  -------+--------+------------------------+-------------");
        foreach (var r in new[] { g1, g2, g4, un })
            _out.WriteLine($"  {r.Events,6:N0} | {r.Groups,6} | {r.PeakBytes / MB,18:F1} MB | {r.PeakBytes / (double)r.Events,10:F0} B");

        Assert.True(g4.Groups >= 4, $"budget did not cut the file: {g4.Groups} group(s)");
        Assert.Equal(1, un.Groups);

        // The claim: 4x the data does not cost 4x the peak. A group's accumulators are the
        // only thing live, so the peak is flat up to noise — allow 1.6x for heap slack and
        // the last group's serialised blobs.
        Assert.True(g4.PeakBytes < g1.PeakBytes * 1.6,
            $"peak grew with the file: {g1.PeakBytes / MB:F1} MB at {g1.Events:N0} events " +
            $"vs {g4.PeakBytes / MB:F1} MB at {g4.Events:N0} — groups are not bounding the build");
        Assert.True(g4.PeakBytes < g2.PeakBytes * 1.6);

        // Same events, one group: this is what the peak looked like before v7 and what a
        // day-scale segment would have cost.
        Assert.True(g4.PeakBytes * 2 < un.PeakBytes,
            $"grouping saved nothing: {g4.PeakBytes / MB:F1} MB grouped vs {un.PeakBytes / MB:F1} MB ungrouped");
    }

    private readonly record struct Result(int Events, int Groups, long PeakBytes);

    private Result Measure(int events, long groupBudget, string name)
    {
        var pool = new StringInternPool();
        using var hot = BuildTier(events, pool);
        int[] order = SegmentWriter.ComputeSortOrder(hot);

        string path = Path.Combine(_dir, $"{name.Replace(' ', '-')}-{events}.seg");
        int  groups = 0;
        long peak   = 0;

        using (var writer = new SegmentWriter(path, groupBudget))
        {
            // Baseline with the tier, the order array and the writer already alive, so what
            // we attribute to the build is only what the BUILDER retains.
            long baseline = GC.GetTotalMemory(forceFullCollection: true);

            // A FRESH builder per group — the mechanism under test. The previous group's
            // accumulators are unreachable by the time the next one seals, so the forced
            // collection inside the probe leaves only the current group's state live.
            writer.WriteEvents(hot, pool, order, (count, termsPerEvent) => new PeakProbeSink(
                new SegmentIndexBuilder(count, 5, termsPerEvent),
                () =>
                {
                    long live = GC.GetTotalMemory(forceFullCollection: true) - baseline;
                    if (live > peak) peak = live;
                    groups++;
                }));
            writer.Finalise(new NodeId(0), new SegmentId((ulong)events));
        }

        return new Result(events, groups, peak);
    }

    /// <summary>
    /// Wraps the real builder and samples the live heap at the instant a group seals — the
    /// group's accumulators at their fullest, everything from earlier groups already garbage.
    /// </summary>
    private sealed class PeakProbeSink(SegmentIndexBuilder inner, Action onSeal) : ISegmentIndexSink
    {
        public void Add(uint fileOrdinal, in SegmentEventRef ev) => inner.Add(fileOrdinal, in ev);

        // Forwarded, not stubbed to 0: this is what the writer sizes the NEXT group's filter
        // from, and what it seals the CURRENT one on when the filter fills before the payload
        // budget does. A wrapper that swallowed either would make the probe measure a blind
        // writer rather than the one production runs.
        public long BloomTermsAdded   => inner.BloomTermsAdded;
        public long BloomTermCapacity => inner.BloomTermCapacity;

        public (byte[] Inverted, byte[] Trigram, byte[] Bloom) Serialise()
        {
            onSeal();
            return inner.Serialise();
        }

        public void Dispose() => inner.Dispose();
    }

    /// <summary>Trace-carrying, prop-dense events — the shape that makes the trigram
    /// accumulator expensive (every TraceId/SpanId and every value is a distinct term).</summary>
    private static HotTierSegment BuildTier(int events, StringInternPool pool)
    {
        var hot = new HotTierSegment(events + 1, (long)events * 768 + (16L << 20));
        int tmplIdx = pool.Intern("HTTP {Method} {Route} responded {Status} in {Elapsed} ms");
        string tmpl = pool.Get(tmplIdx);
        int svcIdx  = pool.Intern("Etisalat.API");

        var rng = new Random(11);
        string[] methods = ["GET", "POST", "PUT", "DELETE"];
        string[] routes  = ["/api/pay", "/api/topup", "/api/status", "/api/balance"];

        long baseTicks = new DateTimeOffset(2026, 7, 30, 0, 0, 0, TimeSpan.Zero).UtcTicks;
        var buf = new ArrayBufferWriter<byte>(512);
        for (int i = 0; i < events; i++)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(8);
            w.Write("orderId");          w.Write(rng.Next(0, 10_000_000));
            w.Write("customerId");       w.Write("cust-" + rng.Next(0, 100_000));
            w.Write("http.method");      w.Write(methods[rng.Next(methods.Length)]);
            w.Write("http.route");       w.Write(routes[rng.Next(routes.Length)]);
            w.Write("http.status_code"); w.Write(new[] { 200, 201, 400, 404, 500 }[rng.Next(5)]);
            w.Write("duration_ms");      w.Write(Math.Round(rng.NextDouble() * 500, 2));
            w.Write("region");           w.Write("ae-dxb");
            w.Write("RequestId");        w.Write("0HN" + rng.Next().ToString("x"));
            w.Flush();

            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerMillisecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
                TraceIdHi                = (ulong)rng.NextInt64(),
                TraceIdLo                = (ulong)rng.NextInt64(),
                SpanId                   = (ulong)rng.NextInt64(),
            };
            // template: null keeps the tier's per-chunk string[] unallocated, so the
            // baseline does not drift with event count.
            if (!hot.TryWrite(h, buf.WrittenSpan)) break;
        }
        hot.Freeze();
        _ = tmpl;
        return hot;
    }
}
