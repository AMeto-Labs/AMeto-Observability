using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// What a group's bloom section costs, per group, against what that group's terms actually need.
///
/// <para>The bloom filter is the one section sized by FORECAST rather than by content: it is
/// allocated before the group's first event is indexed and cannot be resized. So its cost is
/// whatever the forecast said, and the forecast used to be a fixed 64 terms per event — 10 bits
/// a term, 80 bytes an event, for every event shape there is. This probe reports the two numbers
/// that decide whether that is right: BYTES PER EVENT (what the file pays) and BITS PER TERM
/// (whether the filter is selective at that price).</para>
///
/// <para>~10 bits/term is the design point — about 1 % false positives. Materially below it the
/// filter saturates and the phase-1 prefilter stops rejecting, which is the failure the fixed 64
/// was raised to eliminate. Materially above it, the bits are simply wasted.</para>
///
/// <para>MEASURED over an 8-source merge, 48 000 events, 4 MB groups. "before" is the fixed
/// 64 terms/event; "after" is each group sized from the measured terms of the groups before it,
/// with 2× headroom and an absolute ceiling (see <c>SegmentWriter.EnsureSink</c>). Figures are
/// for the groups AFTER the first, which is the only place the change applies — group 0 is
/// forecast before an event has been indexed and deliberately keeps the assumption:</para>
/// <code>
///   shape      terms/ev   bloom B/event      bits/term        section, one group
///                         before   after   before   after    before      after
///   Thin          7.0      80.0    17.5     91.4    20.0     1.54 MB    0.34 MB
///   PropDense    21.1      80.1    52.6     30.3    19.9     0.97 MB    0.64 MB
/// </code>
///
/// <para>Thin events are the case the fixed estimate got worst and they are the ones a log store
/// actually holds most of: 91 bits a term is nine times the design point, i.e. eight of every
/// nine bloom bytes in the file bought nothing. PropDense was already within 3× of it, which is
/// why the constant looked defensible when it was set — it was set against events like those.
/// Both shapes land at ~20 bits/term after, which is the 2× headroom and nothing else.</para>
///
/// <para>Scaled to the 64 MB group budget production runs, one full group of thin events goes
/// from 34.0 MB of filter to 7.4 MB, and one of prop-dense events from 15.5 MB to 10.2 MB.</para>
///
/// <para>The probe also reports what the query prefilter's phase split is trading, since that is
/// decided by the same numbers: phase 1 reads the bloom SECTION alone, phase 2 the inverted and
/// trigram sections. Bloom is 15.6 % of a prop-dense group's three sections and 26.6 % of a thin
/// one's, so rejecting in phase 1 is 6.4× and 3.8× cheaper. At the fixed 64 it was 62 % of a thin
/// group's index and 1.6× — the split had nearly stopped paying for itself.</para>
///
/// <para><b>That sections ratio is not the cache's ratio, and the two were being quoted for each
/// other.</b> <c>SegmentIndexCache</c> charges an entry its <c>ApproxRetainedBytes</c> and bounds
/// the native part of it separately, and an entry is not its sections: the inverted and trigram
/// halves are DECODED into dictionaries and <c>int[]</c>, 3-4× their packed sections, while the
/// bloom's bits are the same bytes decoded as on disk. So bloom is 4.1 % of a prop-dense ENTRY and
/// 8.3 % of a thin one, against 15.6 % and 26.6 % of those shapes' SECTIONS. Sizing a host from
/// the sections figure budgets 3-4× too much off-heap memory for this cache. Both are printed and
/// both are asserted, each excluding the blind first group so they compare like for like.</para>
/// </summary>
public sealed class BloomSizingProbe : IDisposable
{
    private const long GroupBudget = 4L * 1024 * 1024;
    private const int  Sources     = 8;

    private readonly ITestOutputHelper _out;
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-bloom-" + Guid.NewGuid().ToString("N"));

    public BloomSizingProbe(ITestOutputHelper o)
    {
        _out = o;
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, true); } catch { }
    }

    public enum Shape
    {
        /// <summary>
        /// What most of a log store is: a level, a template, a service name and two short
        /// properties. Seven bloom terms an event and ~90 bytes of payload — the shape the fixed
        /// 64-terms forecast over-sized by an order of magnitude.
        /// </summary>
        Thin,

        /// <summary>
        /// Trace-carrying, eight properties, an exception on one in twenty: ~21 terms an event.
        /// The densest realistic shape, and the one the fixed estimate was tuned against.
        /// </summary>
        PropDense,
    }

    /// <summary>
    /// Every group of a merged file: what its filter cost and what its terms needed.
    ///
    /// <para>The merge path is where this binds. On a flush the source's remaining-event hint
    /// caps the forecast at the tier's own event count; on a merge the hint is the sum of every
    /// source's events, so the forecast is set by the group's PAYLOAD budget divided by the row
    /// cost — and for thin rows that quotient is enormous.</para>
    /// </summary>
    [Theory]
    [InlineData(Shape.Thin)]
    [InlineData(Shape.PropDense)]
    public void EveryGroupsFilterIsSizedForTheTermsItHolds(Shape shape)
    {
        var (groups, payloadBytes) = MergeAndMeasure(shape);

        _out.WriteLine($"  {shape}: {groups.Count} groups, {GroupBudget / 1048576} MB budget each");
        _out.WriteLine("   grp |  events |    terms | terms/ev |  bloom   | B/event | bits/term");
        _out.WriteLine("  -----+---------+----------+----------+----------+---------+----------");

        long totalTerms = 0, totalBloom = 0, totalEvents = 0;
        foreach (var g in groups)
        {
            _out.WriteLine(
                $"  {g.Index,4} | {g.Events,7:N0} | {g.Terms,8:N0} | {g.Terms / (double)g.Events,8:F1} | " +
                $"{g.BloomBytes / 1048576.0,5:F2} MB | {g.BloomBytes / (double)g.Events,7:F1} | " +
                $"{g.BloomBytes * 8.0 / g.Terms,9:F1}");
            totalTerms  += g.Terms;
            totalBloom  += g.BloomBytes;
            totalEvents += g.Events;
        }

        double termsPerEvent   = totalTerms / (double)totalEvents;
        double bytesPerEvent   = totalBloom / (double)totalEvents;
        double payloadPerEvent = payloadBytes / (double)totalEvents;
        _out.WriteLine(
            $"  file: {termsPerEvent:F1} terms/event, {payloadPerEvent:F0} payload B/event, " +
            $"{bytesPerEvent:F1} bloom B/event, {totalBloom * 8.0 / totalTerms:F1} bits/term");

        // What ONE FULL group costs at the default 64 MB budget production runs — the number the
        // group budget's own documentation is expressed in.
        //
        // Taken from the groups AFTER the first, not from the file average. Group 0 is sized
        // blind and is deliberately left that way, and in a two-group probe file it is over half
        // the events — averaging it in would report a figure no real merged file pays, since a
        // day-scale file has one blind group and many measured ones.
        long steadyBloom = 0, steadyEvents = 0;
        for (int i = 1; i < groups.Count; i++) { steadyBloom += groups[i].BloomBytes; steadyEvents += groups[i].Events; }
        double steadyBytesPerEvent = steadyEvents > 0 ? steadyBloom / (double)steadyEvents : bytesPerEvent;
        double eventsInFullGroup   = SegmentWriter.DefaultGroupPayloadBudgetBytes / payloadPerEvent;
        _out.WriteLine($"  a full {SegmentWriter.DefaultGroupPayloadBudgetBytes / 1048576} MB group of this shape: " +
                       $"{eventsInFullGroup:N0} events, " +
                       $"{eventsInFullGroup * steadyBytesPerEvent / 1048576.0:F1} MB of filter " +
                       $"({eventsInFullGroup * 80 / 1048576.0:F1} MB at a fixed 64 terms/event)");

        // What the query prefilter's two-phase split is actually trading. Phase 1 reads the bloom
        // section alone; phase 2 additionally reads inverted, and trigram when the filter has a
        // substring predicate. The ratio between them is the whole justification for having two
        // phases, and it was documented as "a few KB against MB" — off by orders of magnitude.
        long inv = 0, tri = 0, steadyInv = 0, steadyTri = 0;
        for (int i = 0; i < groups.Count; i++)
        {
            inv += groups[i].InvertedBytes;
            tri += groups[i].TrigramBytes;
            if (i == 0) continue;
            steadyInv += groups[i].InvertedBytes;
            steadyTri += groups[i].TrigramBytes;
        }
        _out.WriteLine($"  index sections: bloom {totalBloom / 1048576.0:F2} MB, " +
                       $"inverted {inv / 1048576.0:F2} MB, trigram {tri / 1048576.0:F2} MB " +
                       $"— phase 1 reads {totalBloom * 100.0 / (totalBloom + inv + tri):F1}% of the index");
        // Again without the blind first group, which is the share a day-scale file's groups pay.
        long steadyIndex = steadyBloom + steadyInv + steadyTri;
        if (steadyIndex > 0)
            _out.WriteLine($"  excluding the blind first group: phase 1 reads " +
                           $"{steadyBloom * 100.0 / steadyIndex:F1}% of the index, " +
                           $"so rejecting there is {steadyIndex / (double)steadyBloom:F1}x cheaper than phase 2");

        // And what the CACHE is charged for holding those same groups decoded, which is the other
        // number the bloom share gets quoted for and is NOT the same number. SegmentIndexCache
        // charges ApproxRetainedBytes per entry and bounds the native part of it separately; the
        // sections ratio above belongs to the phase split and nothing else.
        long retained = 0, nativeRetained = 0, steadyRetained = 0, steadyNative = 0;
        for (int i = 0; i < groups.Count; i++)
        {
            retained       += groups[i].RetainedBytes;
            nativeRetained += groups[i].NativeRetainedBytes;
            if (i == 0) continue;
            steadyRetained += groups[i].RetainedBytes;
            steadyNative   += groups[i].NativeRetainedBytes;
        }
        long sections = totalBloom + inv + tri;
        _out.WriteLine($"  cache entries: retained {retained / 1048576.0:F2} MB " +
                       $"(managed {(retained - nativeRetained) / 1048576.0:F2} MB, native {nativeRetained / 1048576.0:F2} MB) " +
                       $"— expansion {retained / (double)sections:F1}x over the sections");
        _out.WriteLine($"  bloom is {nativeRetained * 100.0 / retained:F1}% of an ENTRY " +
                       $"against {totalBloom * 100.0 / sections:F1}% of the SECTIONS");

        // The like-for-like figure, and the one worth quoting: the sections share is always
        // reported without the blind first group, so the entry share has to be too. Group 0's
        // filter is forecast before an event has been indexed, and on a two-group probe file its
        // over-sized bloom is over half the file's — averaging it in flatters the native share.
        if (steadyRetained > 0 && steadyIndex > 0)
            _out.WriteLine($"  excluding the blind first group: bloom is " +
                           $"{steadyNative * 100.0 / steadyRetained:F1}% of an ENTRY " +
                           $"against {steadyBloom * 100.0 / steadyIndex:F1}% of the SECTIONS " +
                           $"— entries expand {steadyRetained / (double)steadyIndex:F1}x, and only the " +
                           $"managed half of them expands at all");

        // The design point, as a two-sided bound rather than a printed number.
        //
        // Group 0 is excluded and only from the LOWER-cost side of the argument: it is forecast
        // before a single event has been indexed, so it has nothing measured to size from and
        // deliberately keeps the generous assumption. Every group after it is sized from the
        // file's own measured terms, and those are the ones this asserts on.
        Assert.True(groups.Count >= 2,
            $"{shape}: {groups.Count} group(s) — the probe needs a second group to measure carry-over sizing");

        for (int i = 1; i < groups.Count; i++)
        {
            var g = groups[i];
            double bitsPerTerm = g.BloomBytes * 8.0 / g.Terms;

            // Under-sizing is the failure that matters: below ~10 bits/term the filter saturates
            // and the prefilter stops rejecting. Bound at 8 so headroom loss shows up as a
            // failure well before selectivity does.
            Assert.True(bitsPerTerm >= 8.0,
                $"{shape} group {i}: {bitsPerTerm:F1} bits/term — the filter is saturating and the prefilter stops rejecting");

            // Over-sizing is the finding. 40 is 2× the sizing's own 2× headroom over the 10-bit
            // design point, so a forecast that goes back to assuming a fixed terms-per-event
            // (91 bits/term on thin events) fails here.
            Assert.True(bitsPerTerm <= 40.0,
                $"{shape} group {i}: {bitsPerTerm:F1} bits/term — the filter is sized for terms this group does not hold");
        }

        // The share the CACHE is sized by, pinned as a band rather than a printed number, because
        // it is the figure MemoryBudgets, SegmentIndexCache, SegmentIndexReader and both operator
        // docs quote when they justify the native ceiling.
        //
        // The upper bound is what fails if the sections ratio is written back in its place: 15.6 %
        // and 26.6 % are both outside it. The lower bound fails if it drifts the other way, to the
        // "native is about 1 %, ignore it" premise an earlier review worked from — these bytes are
        // a real share of an entry and no collection returns any of them.
        double entryShare = steadyNative * 100.0 / steadyRetained;
        Assert.InRange(entryShare, 2.0, 12.0);

        // And the structural reason the two differ, which is what makes quoting one for the other
        // a mistake rather than a rounding difference: decoding expands the managed half only.
        Assert.True(steadyBloom * 100.0 / steadyIndex > entryShare * 1.5,
            $"{shape}: bloom is {entryShare:F1}% of an entry and {steadyBloom * 100.0 / steadyIndex:F1}% of the " +
            "sections — if these have converged, the entry is no longer expanding over its sections and every " +
            "sizing argument built on the difference needs re-deriving");
    }

    private readonly record struct GroupCost(
        int Index, long Events, long Terms, long BloomBytes, long InvertedBytes, long TrigramBytes,
        long RetainedBytes, long NativeRetainedBytes);

    private (List<GroupCost> Groups, long PayloadBytes) MergeAndMeasure(Shape shape)
    {
        var paths  = BuildSources(6_000, shape);
        string p   = Path.Combine(_dir, $"merged-{shape}.seg");

        // The sink factory is what the writer drives, so wrapping it is the only place the
        // per-group term count can be captured — the builder is disposed at the group boundary.
        var built = new List<SegmentIndexBuilder>();
        SegmentInfo info;
        using (var source = MergingSegmentEventSource.Open(paths))
        using (var writer = new SegmentWriter(p, GroupBudget))
        {
            writer.WriteEvents(source, (count, termsPerEvent) =>
            {
                var b = new SegmentIndexBuilder(count, 5, termsPerEvent);
                built.Add(b);
                return b;
            }, CancellationToken.None);
            info = writer.Finalise(new NodeId(0), new SegmentId(1UL));
        }

        var costs = new List<GroupCost>();
        using (var reader = SegmentReader.Open(p))
        {
            var groups = reader.Groups;
            for (int g = 0; g < groups.Length; g++)
            {
                using var bloom = reader.RentBloomFilterBytes(g);
                using var inv   = reader.RentInvertedIndexBytes(g);
                using var tri   = reader.RentTrigramIndexBytes(g);

                // What a CACHE ENTRY for this group weighs — a different question from what the
                // sections weigh, and the one SegmentIndexCache's budgets are denominated in. Its
                // inverted and trigram halves are DECODED into dictionaries and int[] several
                // times their packed sections; the bloom's bits are the same bytes decoded as on
                // disk. So the bloom's share of an entry is much smaller than its share of the
                // sections, and the two must not be quoted for each other.
                using var entry = SegmentIndexReader.Load(inv.Span, tri.Span, bloom.Span);

                // Readable after Dispose by contract — only the filter's bits are native.
                costs.Add(new GroupCost(g, groups[g].EventCount,
                                        g < built.Count ? built[g].BloomTermsAdded : 0,
                                        bloom.Span.Length, inv.Span.Length, tri.Span.Length,
                                        entry.ApproxRetainedBytes, entry.ApproxNativeBytes));
            }
        }
        return (costs, info.UncompressedBytes);
    }

    private List<string> BuildSources(int eventsPerSource, Shape shape)
    {
        string sub = Path.Combine(_dir, $"src-{shape}");
        if (Directory.Exists(sub))
            return [.. Directory.GetFiles(sub, "*.seg").Order()];

        Directory.CreateDirectory(sub);
        var paths = new List<string>(Sources);
        for (int s = 0; s < Sources; s++) paths.Add(WriteSource(sub, s, eventsPerSource, shape));
        return paths;
    }

    private static string WriteSource(string dir, int sourceIndex, int events, Shape shape)
    {
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(events + 1, (long)events * 1024 + (32L << 20));
        int tmplIdx = pool.Intern(shape == Shape.Thin
            ? "Request {Route} finished with {Status}"
            : "Settlement {Stage} for wallet {Wallet} finished with {Status} in {Elapsed} ms");
        int svcIdx = pool.Intern("Etisalat.Payments.Settlement");

        var rng = new Random(1717 + sourceIndex);
        string[] stages   = ["authorize", "capture", "reconcile", "refund"];
        string[] routes   = ["/api/pay", "/api/topup", "/api/status", "/api/balance"];
        string[] regions  = ["ae-dxb", "ae-auh", "sa-ruh"];
        string[] statuses = ["Succeeded", "Retried", "Declined"];

        long baseTicks = new DateTimeOffset(2026, 8, 1, 0, 0, 0, TimeSpan.Zero).UtcTicks
                       + sourceIndex * (events / 2L) * TimeSpan.TicksPerMillisecond;

        var buf = new ArrayBufferWriter<byte>(1024);
        for (int i = 0; i < events; i++)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            if (shape == Shape.Thin)
            {
                w.WriteMapHeader(2);
                w.Write("Route");  w.Write(routes[rng.Next(routes.Length)]);
                w.Write("Status"); w.Write(new[] { 200, 201, 404 }[rng.Next(3)]);
            }
            else
            {
                w.WriteMapHeader(8);
                w.Write("Stage");            w.Write(stages[rng.Next(stages.Length)]);
                w.Write("Wallet");           w.Write("wallet-" + rng.Next(0, 2_000_000).ToString("D9"));
                w.Write("Status");           w.Write(statuses[rng.Next(statuses.Length)]);
                w.Write("http.route");       w.Write(routes[rng.Next(routes.Length)]);
                w.Write("http.status_code"); w.Write(new[] { 200, 201, 400, 404, 500 }[rng.Next(5)]);
                w.Write("Elapsed");          w.Write(Math.Round(rng.NextDouble() * 500, 2));
                w.Write("region");           w.Write(regions[rng.Next(regions.Length)]);
                w.Write("RequestId");        w.Write("0HN" + rng.NextInt64().ToString("x16"));
            }
            w.Flush();

            var h = new LogEventHeader
            {
                Id                       = new EventId((uint)sourceIndex, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerMillisecond,
                Level                    = (i % 20) == 0 ? LogLevel.Error : LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
                TraceIdHi                = shape == Shape.Thin ? 0 : (ulong)rng.NextInt64(),
                TraceIdLo                = shape == Shape.Thin ? 0 : (ulong)rng.NextInt64(),
                SpanId                   = shape == Shape.Thin ? 0 : (ulong)rng.NextInt64(),
            };
            var exc = shape == Shape.PropDense && (i % 20) == 0
                ? new ExceptionInfo
                  {
                      Type    = "System.InvalidOperationException",
                      Message = "wallet " + i + " is not in a state that permits settlement",
                      Inner   = new ExceptionInfo { Type = "System.TimeoutException", Message = "ledger did not answer in 30s" },
                  }
                : null;
            if (!hot.TryWrite(h, buf.WrittenSpan, null, exc)) break;
        }
        hot.Freeze();

        string path = Path.Combine(dir, $"src-{sourceIndex:D2}.seg");
        using var writer = new SegmentWriter(path);
        writer.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot));
        writer.Finalise(new NodeId(0), new SegmentId((ulong)sourceIndex + 1));
        return path;
    }
}
