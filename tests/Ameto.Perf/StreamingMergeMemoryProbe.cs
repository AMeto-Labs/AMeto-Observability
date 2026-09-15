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
/// group. This probe merges the same source shape at 1×, 2×, 4×, 8× and 16× the size and shows
/// what the merge keeps reachable at its peak staying flat — stated as an absolute ceiling on
/// that state at every size, an absolute bound on what each further merged event may add, and
/// ratios against the smaller merges.</para>
///
/// <para>Beside it, for scale, the cost of merely MATERIALISING the same events: the first and
/// cheapest of the three stages the old pipeline paid for. It is checked only for scaling with
/// the data. It used to be the yardstick (the peak had to undercut it), which tied a merge guard
/// to the query decoder's cost: the decoder then got cheaper (template and service strings
/// shared per enumeration, a lazy exception column), materialising 96k events fell from 44.9 to
/// 29.5 MB, and the guard failed with the merge unchanged.</para>
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

    /// <summary>
    /// The k-way merge's string-dedup table is the one thing in the pipeline that is NOT
    /// O(block + group): it holds an entry per distinct template or service name for the whole
    /// merge. With structured templates that is a handful of strings; with a RENDERED message —
    /// a template built by interpolation, which is what <see cref="StringInternPool"/>'s
    /// exhaustion warning exists for — it is one per event, and unbounded it would put back the
    /// memory ceiling the streaming merge removed, at a merged file that may now hold millions
    /// of rows.
    ///
    /// <para>Measured on the SOURCE alone — no writer, no index sink — because that is where the
    /// table lives and the writer's own flat cost would drown it. Ten times the events must not
    /// cost ten times the retention.</para>
    /// </summary>
    [Fact]
    public void TheMergeStreamsStringDedupIsBounded()
    {
        const double MB = 1048576.0;

        MeasureSourceRetention(200, "hc-warm");
        var small = MeasureSourceRetention(2_000,  "hc-small");   // 24k events — inside the cap
        var large = MeasureSourceRetention(20_000, "hc-large");   // 240k events — well past it

        _out.WriteLine(
            $"  dedup retention with a distinct template per event: {small.Events:N0} events " +
            $"{small.Bytes / MB:F1} MB ({small.Bytes / (double)small.Events:F0} B/event) -> " +
            $"{large.Events:N0} events {large.Bytes / MB:F1} MB " +
            $"({large.Bytes / (double)large.Events:F0} B/event)");

        Assert.Equal(small.Events * 10, large.Events);
        Assert.True(large.Bytes < small.Bytes * 5,
            $"10x the events retained {large.Bytes / (double)small.Bytes:F1}x the memory — the dedup table is unbounded");
    }

    /// <summary>Live bytes the merge SOURCE still holds after draining it — the dedup table.</summary>
    private (int Events, long Bytes) MeasureSourceRetention(int eventsPerSource, string name)
    {
        string sub = Path.Combine(_dir, name);
        Directory.CreateDirectory(sub);

        var paths = new List<string>(Sources);
        for (int s = 0; s < Sources; s++)
            paths.Add(WriteSource(sub, s, eventsPerSource, distinctTemplates: true));

        long baseline = SettledBaseline();
        using var source = MergingSegmentEventSource.Open(paths);
        int n = 0;
        while (source.TryReadNext(out var ev)) { if (ev.Id != 0) n++; else n++; }
        long live = GC.GetTotalMemory(forceFullCollection: true) - baseline;
        GC.KeepAlive(source);
        return (n, live);
    }

    /// <summary>
    /// What one further merged event may add to the merge's peak reachable state, over the
    /// 168 000 events from the 2× merge to the 16× one.
    ///
    /// <para>What legitimately grows with the merged size is per BLOCK or per GROUP (the block
    /// index, the group directory): well under a byte an event here. The measured slope is about
    /// 1 B. What moves it is group shape, not events: the state at the peak seal differs by up to
    /// about 1 MB from one size to the next (21.2 MB at 24 000 events and 21.7 MB at 48 000; with a
    /// 20 MB array held through the merge, 41.4 MB at 24 000 and 42.4 MB at 96 000), which over
    /// this span is up to 6 B an event. Over 24 000 to 96 000 events, the span this was first
    /// measured on, the same noise read up to 14.6 B. That is why the span is 16× and not 8×.</para>
    ///
    /// <para>The floor, measured with throwaway retention in <c>SegmentWriter.WriteEvents</c>, one
    /// entry per merged event held for the whole merge. An 8 B id in an array sized up front reads
    /// 8.9 B and passes. The same id appended to a <c>List</c>, which over-allocates as it
    /// doubles, reads 12.6 B: right at the bound, so it can go either way. A 16 B record in an
    /// array sized up front reads 17.3 B and fails; a 24 B one reads 24.5 B. So a leak of 8 B an
    /// event gets through, 16 B does not, and what lies between depends on how the leak
    /// allocates. Under the previous span and bound (32 B over 24 000 to 96 000 events) a 16 B
    /// record read 16.8 to 19.8 B and a 24 B one 24.6 to 27.6 B, and both passed.</para>
    /// </summary>
    private const double MaxMarginalBytesPerEvent = 12;

    /// <summary>
    /// Ceiling on what <see cref="IndexBuildPool"/> may hold parked at a seal of these 4 MB groups:
    /// the previous group's returned arrays and this one's growth leftovers, about one group's
    /// worth. The pool's own caps are size-independent and far larger; this is the workload's.
    /// </summary>
    private const long MaxParkedBytes = 32L << 20;

    /// <summary>
    /// Ceiling on what the merge keeps reachable at a group seal, at every size.
    ///
    /// <para>What that is, measured from 12 000 to 192 000 events: the index build state of the
    /// group being sealed, as <see cref="SegmentIndexBuilder.BuildRetainedBytes"/> counts it,
    /// 19.5 to 20.5 MB (the table prints it); and 0.6 to 1.3 MB of everything else the merge holds
    /// beside it — the source cursors, the writer's buffers, the group's bloom filter. 20.1 to
    /// 21.8 MB in all. The ceiling is about 1.5× the largest, the slack the ratios below allow.</para>
    ///
    /// <para>This is the absolute half of the guard, the role the old crossover played: the peak
    /// had to stay under two thirds of what materialising the 96 000 events cost (29.9 MB at base,
    /// 19.7 MB once the query decoder got cheaper). The slope and the ratios compare this probe's
    /// merges with each other, so a cost that is flat in the merged size passes them however large
    /// it is. Simulated with a 20 MB array held for the whole merge (throwaway, in
    /// <c>SegmentWriter.WriteEvents</c>): 40.7 to 42.5 MB at every size, a slope of 3.2 B/event,
    /// 1.03× from 1× to 16×. Only this ceiling fails it.</para>
    /// </summary>
    private const long MaxMergeStateBytes = 32L << 20;

    [Fact]
    public void PeakMergeMemoryIsFlatInTheMergedSegmentSize()
    {
        const double MB = 1048576.0;

        // One for every merge of the run, as the storage engine's sink factory shares one: each
        // builder pre-sizes from the group sealed before it, which is how production builds.
        var hints = new IndexBuildHints();

        Measure(250, "warm", hints);   // JIT + ArrayPool growth out of the way

        var m1  = Measure(1_000,  "merge 1x",  hints);
        var m2  = Measure(2_000,  "merge 2x",  hints);
        var m4  = Measure(4_000,  "merge 4x",  hints);
        var m8  = Measure(8_000,  "merge 8x",  hints);
        var m16 = Measure(16_000, "merge 16x", hints);

        _out.WriteLine("   events | groups | merge state at peak | of it index build | parked in pool | gross heap at peak | materialising the same");
        _out.WriteLine("  --------+--------+---------------------+-------------------+----------------+--------------------+-----------------------");
        foreach (var r in new[] { m1, m2, m4, m8, m16 })
            _out.WriteLine($"  {r.Events,7:N0} | {r.Groups,6} | {r.PeakBytes / MB,16:F1} MB | {r.BuildStateAtPeakBytes / MB,14:F1} MB | " +
                           $"{r.ParkedBytes / MB,11:F1} MB | {r.GrossPeakBytes / MB,15:F1} MB | {r.MaterialisedBytes / MB,18:F1} MB");

        double mergePerEvent = (m16.PeakBytes - m2.PeakBytes) / (double)(m16.Events - m2.Events);
        double matPerEvent   = (m16.MaterialisedBytes - m2.MaterialisedBytes) / (double)(m16.Events - m2.Events);
        _out.WriteLine($"  each merged event from {m2.Events:N0} to {m16.Events:N0}: merge state {mergePerEvent:F1} B " +
                       $"(bound {MaxMarginalBytesPerEvent} B), materialising {matPerEvent:F1} B");

        Assert.True(m16.Groups >= 12, $"budget did not cut the merged file: {m16.Groups} group(s)");
        Assert.Equal(m1.Events * 16, m16.Events);

        // The sources must hold data that scales, or a flat merge proves nothing.
        Assert.True(m16.MaterialisedBytes > m1.MaterialisedBytes * 10,
            "the materialised baseline is not scaling — the probe is measuring nothing");

        // The absolute half: however flat, the merge's working state has a size. A cost that does
        // not grow with the merged file passes the slope and the ratios below, and this catches it.
        foreach (var r in new[] { m1, m2, m4, m8, m16 })
            Assert.True(r.PeakBytes < MaxMergeStateBytes,
                $"the {r.Events:N0}-event merge held {r.PeakBytes / MB:F1} MB at a seal (ceiling {MaxMergeStateBytes / MB:F0} MB; " +
                $"the group's index build state was {r.BuildStateAtPeakBytes / MB:F1} MB of it)");

        // The claim, as a slope: between two multi-group merges, 8× the events apart, each further
        // event adds at most a few bytes to what the merge holds at its peak. The bound is
        // absolute, so a change to another code path's cost cannot move it.
        Assert.True(mergePerEvent < MaxMarginalBytesPerEvent,
            $"each merged event adds {mergePerEvent:F1} B to the merge's peak (bound {MaxMarginalBytesPerEvent} B; " +
            $"materialising adds {matPerEvent:F1} B) — the merge is not streaming");

        // And as ratios: 16× the merged data does not cost 16× the peak.
        Assert.True(m16.PeakBytes < m1.PeakBytes * 1.6,
            $"peak grew with the merged file: {m1.PeakBytes / MB:F1} MB at {m1.Events:N0} events " +
            $"vs {m16.PeakBytes / MB:F1} MB at {m16.Events:N0} — the merge is not streaming");
        Assert.True(m16.PeakBytes < m4.PeakBytes * 1.6,
            $"peak grew with the merged file: {m4.PeakBytes / MB:F1} MB at {m4.Events:N0} events " +
            $"vs {m16.PeakBytes / MB:F1} MB at {m16.Events:N0} — the merge is not streaming");

        // The pool is bounded apart: reusable, but memory the process holds all the same.
        Assert.True(m16.ParkedBytes < MaxParkedBytes,
            $"{m16.ParkedBytes / MB:F1} MB parked in IndexBuildPool at a seal of the {m16.Events:N0}-event merge");
    }

    /// <param name="PeakBytes">The most the merge kept reachable at any group seal, above a settled baseline.</param>
    /// <param name="ParkedBytes">The most <see cref="IndexBuildPool"/> held parked at any seal.</param>
    /// <param name="GrossPeakBytes">The most of the two together at one seal: what a heap sample blind to the pool reads.</param>
    /// <param name="BuildStateAtPeakBytes">The sealing group's index build state at the seal <paramref name="PeakBytes"/> was read at, as the builder counts it.</param>
    private readonly record struct Result(int Events, int Groups, long PeakBytes, long ParkedBytes, long GrossPeakBytes,
                                          long MaterialisedBytes, long BuildStateAtPeakBytes);

    /// <summary>
    /// A heap baseline nothing is about to drop.
    ///
    /// <para><see cref="IndexBuildPool"/> trims itself from a gen2 callback on the finaliser
    /// thread. A plain <c>GC.GetTotalMemory(true)</c> baseline can count parked arrays that a
    /// collection in the NEXT sample frees, and the delta comes out short: below zero on the
    /// materialising side, after a merge had left 20 MB parked.</para>
    /// </summary>
    private static long SettledBaseline()
    {
        IndexBuildPool.TrimAll();
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        return GC.GetTotalMemory(forceFullCollection: true);
    }

    private Result Measure(int eventsPerSource, string name, IndexBuildHints hints, bool distinctTemplates = false)
    {
        string sub = Path.Combine(_dir, name.Replace(' ', '-'));
        Directory.CreateDirectory(sub);

        var paths = new List<string>(Sources);
        for (int s = 0; s < Sources; s++)
            paths.Add(WriteSource(sub, s, eventsPerSource, distinctTemplates));

        int totalEvents = Sources * eventsPerSource;
        long peak = 0, parkedPeak = 0, grossPeak = 0, buildAtPeak = 0;
        int  groups = 0;

        string mergedPath = Path.Combine(sub, "merged.seg");
        long baseline = SettledBaseline();
        using (var source = MergingSegmentEventSource.Open(paths))
        using (var writer = new SegmentWriter(mergedPath, GroupBudget))
        {
            writer.WriteEvents(source, (count, termsPerEvent) => new PeakSink(
                new SegmentIndexBuilder(count, 5, termsPerEvent, hints),
                buildState =>
                {
                    // Sampled at the seal: the group's accumulators at their fullest, with
                    // everything from earlier groups already unreachable.
                    //
                    // What IndexBuildPool has parked for the next builder is read first and then
                    // dropped, so the heap sample holds exactly what the merge can reach. Left in,
                    // it is counted or not depending on whether the pool's gen2 hook runs inside
                    // this collection: 0 MB at one size, 7 MB at the next. Dropping it here does
                    // not starve the next group, because this group's arrays go back to the pool
                    // after the seal and the next group rents those.
                    long parked = IndexBuildPool.PooledBytes;
                    IndexBuildPool.TrimAll();
                    long live = GC.GetTotalMemory(forceFullCollection: true) - baseline;
                    if (live          > peak)       { peak = live; buildAtPeak = buildState; }
                    if (parked        > parkedPeak) parkedPeak = parked;
                    if (live + parked > grossPeak)  grossPeak  = live + parked;
                    groups++;
                }));
            var info = writer.Finalise(new NodeId(0), new SegmentId(1UL));
            Assert.Equal((uint)totalEvents, info.EventCount);
        }

        // What the old pipeline's FIRST stage costs: every event of the batch in managed
        // memory. The tier copy and the batch-wide index build came on top of this.
        long materialised = MeasureMaterialised(paths);

        return new Result(totalEvents, groups, peak, parkedPeak, grossPeak, materialised, buildAtPeak);
    }

    private static long MeasureMaterialised(List<string> paths)
    {
        long baseline = SettledBaseline();
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

    /// <summary>Samples the live heap, and the builder's own count of its state, the instant a group seals, then delegates.</summary>
    private sealed class PeakSink(SegmentIndexBuilder inner, Action<long> onSeal) : ISegmentIndexSink
    {
        public void Add(uint fileOrdinal, in SegmentEventRef ev) => inner.Add(fileOrdinal, in ev);

        // See IndexGroupMemoryProbe.PeakProbeSink — forwarded so the writer sizes and seals as
        // it does in production.
        public long BloomTermsAdded   => inner.BloomTermsAdded;
        public long BloomTermCapacity => inner.BloomTermCapacity;

        public void WriteSections(Stream destination, out long invertedOffset, out long trigramOffset, out long bloomOffset)
        {
            onSeal(inner.BuildRetainedBytes);
            inner.WriteSections(destination, out invertedOffset, out trigramOffset, out bloomOffset);
        }

        public void Dispose() => inner.Dispose();
    }

    /// <summary>
    /// One source segment: trace-carrying, prop-dense events, disjoint time ranges per source so
    /// the heap actually has to interleave them (sources overlap by half a range).
    /// </summary>
    /// <param name="distinctTemplates">
    /// Give every event its own message template — a rendered message rather than a structured
    /// one. It is what the merge's string dedup has to stay bounded against, and the shared
    /// template below hides it completely.
    /// </param>
    private static string WriteSource(string dir, int sourceIndex, int events, bool distinctTemplates = false)
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
            string? tmpl = distinctTemplates
                ? $"HTTP GET /api/pay/{sourceIndex}/{i} responded 200 in {i % 500} ms for cust-{i}"
                : null;
            if (!hot.TryWrite(h, buf.WrittenSpan, tmpl)) break;
        }
        hot.Freeze();

        string path = Path.Combine(dir, $"src-{sourceIndex}.seg");
        using var writer = new SegmentWriter(path);
        writer.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot));
        writer.Finalise(new NodeId(0), new SegmentId((ulong)sourceIndex + 1));
        return path;
    }
}
