using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// The point of the streaming merge, measured.
///
/// <para>Compaction used to read every source with <c>ReadAllRaw</c> into a managed
/// <c>List&lt;RawSegmentEvent&gt;</c> (one <c>byte[]</c> per properties payload), copy that into
/// a <see cref="HotTierSegment"/> and index the batch whole — peak ≈ 3× the batch. That is why
/// the merge budgets were 32 MB / 100k events: they were a MEMORY bound wearing a policy hat.</para>
///
/// <para>The writer now pulls from a k-way merged stream and holds one block plus one index
/// group. This probe merges the same source shape at 1×, 2× and 4× the size and shows the peak
/// live heap staying flat, against the cost of merely MATERIALISING the same events — which is
/// the first and cheapest of the three stages the old pipeline paid for.</para>
/// </summary>
public sealed class StreamingMergeMemoryProbe : IDisposable
{
    /// <summary>Small enough to seal several index groups out of a test-sized merge.</summary>
    private const long GroupBudget = 4L * 1024 * 1024;
    private const int  Sources     = 12;

    private readonly ITestOutputHelper _out;
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mergemem-" + Guid.NewGuid().ToString("N"));

    public StreamingMergeMemoryProbe(ITestOutputHelper o)
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
    public void PeakMergeMemoryIsFlatInTheMergedSegmentSize()
    {
        const double MB = 1048576.0;

        Measure(250, "warm");   // JIT + ArrayPool growth out of the way

        var m1 = Measure(1_000, "merge 1x");
        var m2 = Measure(2_000, "merge 2x");
        var m4 = Measure(4_000, "merge 4x");
        var m8 = Measure(8_000, "merge 8x");

        _out.WriteLine("  events | groups | peak live merge state | materialising the same |  bytes/event");
        _out.WriteLine("  -------+--------+-----------------------+------------------------+-------------");
        foreach (var r in new[] { m1, m2, m4, m8 })
            _out.WriteLine($"  {r.Events,6:N0} | {r.Groups,6} | {r.PeakBytes / MB,17:F1} MB | " +
                           $"{r.MaterialisedBytes / MB,18:F1} MB | {r.PeakBytes / (double)r.Events,10:F0} B");

        Assert.True(m8.Groups >= 6, $"budget did not cut the merged file: {m8.Groups} group(s)");
        Assert.Equal(m1.Events * 8, m8.Events);

        // The claim: 8× the merged data does not cost 8× the peak. Only one block per source
        // plus the open group's accumulators is live, so the peak is flat up to heap slack.
        Assert.True(m8.PeakBytes < m1.PeakBytes * 1.6,
            $"peak grew with the merged file: {m1.PeakBytes / MB:F1} MB at {m1.Events:N0} events " +
            $"vs {m8.PeakBytes / MB:F1} MB at {m8.Events:N0} — the merge is not streaming");
        Assert.True(m8.PeakBytes < m4.PeakBytes * 1.6);

        // Materialising the batch — the FIRST stage of the old pipeline, before the tier copy
        // and the whole-batch index build — already scales with the merged size, and by 8× it
        // has overtaken the entire streaming peak on its own.
        Assert.True(m8.MaterialisedBytes > m1.MaterialisedBytes * 5,
            "the materialised baseline is not scaling — the probe is measuring nothing");
        Assert.True(m8.PeakBytes * 1.5 < m8.MaterialisedBytes,
            $"streaming peak {m8.PeakBytes / MB:F1} MB is no better than materialising " +
            $"{m8.MaterialisedBytes / MB:F1} MB");
    }

    private readonly record struct Result(int Events, int Groups, long PeakBytes, long MaterialisedBytes);

    private Result Measure(int eventsPerSource, string name)
    {
        string sub = Path.Combine(_dir, name.Replace(' ', '-'));
        Directory.CreateDirectory(sub);

        var paths = new List<string>(Sources);
        for (int s = 0; s < Sources; s++)
            paths.Add(WriteSource(sub, s, eventsPerSource));

        int totalEvents = Sources * eventsPerSource;
        long peak = 0;
        int  groups = 0;

        string mergedPath = Path.Combine(sub, "merged.seg");
        long baseline = GC.GetTotalMemory(forceFullCollection: true);
        using (var source = MergingSegmentEventSource.Open(paths))
        using (var writer = new SegmentWriter(mergedPath, GroupBudget))
        {
            writer.WriteEvents(source, count => new PeakSink(
                new SegmentIndexBuilder(count),
                () =>
                {
                    // Sampled at the seal: the group's accumulators at their fullest, with
                    // everything from earlier groups already unreachable.
                    long live = GC.GetTotalMemory(forceFullCollection: true) - baseline;
                    if (live > peak) peak = live;
                    groups++;
                }));
            var info = writer.Finalise(new NodeId(0), new SegmentId(1UL));
            Assert.Equal((uint)totalEvents, info.EventCount);
        }

        // What the old pipeline's FIRST stage costs: every event of the batch in managed
        // memory, properties as a byte[] each. The tier copy and the batch-wide index build
        // came on top of this.
        long materialised = MeasureMaterialised(paths);

        return new Result(totalEvents, groups, peak, materialised);
    }

    private static long MeasureMaterialised(List<string> paths)
    {
        long baseline = GC.GetTotalMemory(forceFullCollection: true);
        var  held     = new List<List<LogEvent>>(paths.Count);
        foreach (var p in paths)
        {
            using var reader = SegmentReader.Open(p);
            held.Add(Drain(reader.ReadEventsAsync(null, null, null)));
        }
        long live = GC.GetTotalMemory(forceFullCollection: true) - baseline;
        GC.KeepAlive(held);
        return live;
    }

    private static List<LogEvent> Drain(IAsyncEnumerable<LogEvent> src)
    {
        var list = new List<LogEvent>();
        var e = src.GetAsyncEnumerator();
        try { while (e.MoveNextAsync().GetAwaiter().GetResult()) list.Add(e.Current); }
        finally { e.DisposeAsync().GetAwaiter().GetResult(); }
        return list;
    }

    /// <summary>Samples the live heap the instant a group seals, then delegates.</summary>
    private sealed class PeakSink(SegmentIndexBuilder inner, Action onSeal) : ISegmentIndexSink
    {
        public void Add(uint fileOrdinal, in SegmentEventRef ev) => inner.Add(fileOrdinal, in ev);

        public (byte[] Inverted, byte[] Trigram, byte[] Bloom) Serialise()
        {
            onSeal();
            return inner.Serialise();
        }

        public void Dispose() => inner.Dispose();
    }

    /// <summary>
    /// One source segment: trace-carrying, prop-dense events, disjoint time ranges per source so
    /// the heap actually has to interleave them (sources overlap by half a range).
    /// </summary>
    private static string WriteSource(string dir, int sourceIndex, int events)
    {
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(events + 1, (long)events * 768 + (16L << 20));
        int tmplIdx = pool.Intern("HTTP {Method} {Route} responded {Status} in {Elapsed} ms");
        int svcIdx  = pool.Intern("Etisalat.API");

        var rng = new Random(1000 + sourceIndex);
        string[] methods = ["GET", "POST", "PUT", "DELETE"];
        string[] routes  = ["/api/pay", "/api/topup", "/api/status", "/api/balance"];

        long baseTicks = new DateTimeOffset(2026, 7, 30, 0, 0, 0, TimeSpan.Zero).UtcTicks
                       + sourceIndex * (events / 2L) * TimeSpan.TicksPerMillisecond;

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
                Id                       = new EventId((uint)sourceIndex, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerMillisecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
                TraceIdHi                = (ulong)rng.NextInt64(),
                TraceIdLo                = (ulong)rng.NextInt64(),
                SpanId                   = (ulong)rng.NextInt64(),
            };
            if (!hot.TryWrite(h, buf.WrittenSpan)) break;
        }
        hot.Freeze();

        string path = Path.Combine(dir, $"src-{sourceIndex}.seg");
        using var writer = new SegmentWriter(path);
        writer.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot));
        writer.Finalise(new NodeId(0), new SegmentId((ulong)sourceIndex + 1));
        return path;
    }
}
