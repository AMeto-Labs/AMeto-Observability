using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// What a group's bloom filter costs when the events STOP LOOKING LIKE the ones before them.
///
/// <para><c>BloomSizingProbe</c> measures a file whose event shape never changes, which is the
/// easy case for a forecast: every group looks like the last one, so sizing the next group from
/// what the previous ones held is right by construction. This probe measures the case the
/// forecast exists to survive — a file whose term density steps UP partway through, which on the
/// merge path is an ordinary thing for a bucket to contain: sources are read in timestamp order,
/// so a deployment that starts emitting more structured context puts the change on a group
/// boundary and everything after it is denser than everything before it.</para>
///
/// <para>The number that decides whether the forecast survived is BITS PER TERM, per group. ~10
/// is the design point (~1 % false positives); materially below it the filter saturates and the
/// query's phase-1 prefilter stops rejecting, which is the entire reason the section exists.</para>
///
/// <para>MEASURED, 20 sources merged at a 1 MB group budget, events going from two 96-character
/// properties to twenty-four 4-character ones — 7.0 terms/event to 51.0, and 20 terms per payload
/// kB to 143. Forecasting from the FILE's average terms-per-event, which is what the sizing
/// change first did, the step lands like this:</para>
/// <code>
///   group 15 (the step)   3.2 bits/term      group 20   6.5
///   group 16              3.5                group 21   7.0
///   group 17              4.4                group 22   7.6
///   group 18              5.2                group 23   8.2
///   group 19              5.9                group 24   8.5   ← ten groups under the design point
/// </code>
/// <para>Ten consecutive groups below the design point, recovering by about half a bit each,
/// because a cumulative average moves by 1/n and the step is a factor of seven. Forecasting from
/// the LAST group instead follows it in one, and sealing a group when its filter fills bounds
/// even that one: the same file now runs 10.5 at the step and 19-20 after it.</para>
/// </summary>
public sealed class BloomStepSizingProbe : IDisposable
{
    /// <summary>
    /// Small on purpose. Production runs at 64 MB and the shape of the failure is the same at
    /// any budget — it is a ratio between two forecasts — so the budget is set to whatever gives
    /// a file enough groups to show a step being absorbed over several of them.
    /// </summary>
    private const long GroupBudget = 1L * 1024 * 1024;

    private readonly ITestOutputHelper _out;
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-bloomstep-" + Guid.NewGuid().ToString("N"));

    public BloomStepSizingProbe(ITestOutputHelper o)
    {
        _out = o;
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, true); } catch { }
    }

    /// <summary>Property shape of one source: how many properties an event carries and how long
    /// their values are. Together these fix both terms per event (3 + 2 per property) and payload
    /// bytes per event, which is what makes term DENSITY — terms per payload byte — settable.</summary>
    private readonly record struct PropShape(int Count, int ValueChars);

    /// <summary>
    /// A file whose later groups hold more terms per byte than its earlier ones.
    ///
    /// <para>Two bands. The first is the shape step the sizing fix's own probe defines — events
    /// gaining properties, from two to eight. The second is the adversarial one: properties that
    /// were few and long become many and short, which raises terms per byte without raising
    /// payload much, and payload is what seals a group. That second band is where a forecast
    /// expressed as terms-per-EVENT, applied to an event count derived from payload, comes apart.</para>
    /// </summary>
    [Theory]
    [InlineData(2, 24, 8, 8)]
    [InlineData(2, 96, 24, 4)]
    public void AStepUpInTermDensity_StillSizesEachGroupForItsOwnTerms(
        int propsBefore, int valueCharsBefore, int propsAfter, int valueCharsAfter)
    {
        var before = new PropShape(propsBefore, valueCharsBefore);
        var after  = new PropShape(propsAfter,  valueCharsAfter);

        var groups = MergeAndMeasure(before, after, sourcesBefore: 12, sourcesAfter: 8, eventsPerSource: 4_000);

        _out.WriteLine($"  {propsBefore}x{valueCharsBefore} -> {propsAfter}x{valueCharsAfter}, " +
                       $"{GroupBudget / 1024} KB groups, {groups.Count} groups");
        _out.WriteLine("   grp |  events |    terms | terms/ev | terms/kB |  bloom B | bits/term");
        _out.WriteLine("  -----+---------+----------+----------+----------+----------+----------");
        foreach (var g in groups)
            _out.WriteLine(
                $"  {g.Index,4} | {g.Events,7:N0} | {g.Terms,8:N0} | {g.Terms / (double)g.Events,8:F1} | " +
                $"{g.Terms * 1024.0 / g.PayloadBytes,8:F1} | {g.BloomBytes,8:N0} | " +
                $"{g.BloomBytes * 8.0 / g.Terms,9:F1}");

        // Group 0 is forecast before a single event has been indexed and deliberately keeps the
        // generous assumption, so it is excluded — from this side of the argument only.
        Assert.True(groups.Count >= 4,
            $"{groups.Count} group(s) — this probe needs a file long enough for a step to land inside it");

        for (int i = 1; i < groups.Count; i++)
        {
            double bitsPerTerm = groups[i].BloomBytes * 8.0 / groups[i].Terms;

            // Under-sizing, which is the failure: below ~10 bits/term the filter saturates and
            // the prefilter stops rejecting. Bounded at 8 so the loss of headroom fails here
            // before selectivity does.
            Assert.True(bitsPerTerm >= 8.0,
                $"group {i}: {bitsPerTerm:F1} bits/term over {groups[i].Terms:N0} terms — the filter is " +
                "saturating and the prefilter stops rejecting");

            // And the other side, so this cannot be satisfied by making every filter enormous.
            // 40 is 2x the sizing's own 2x headroom over the 10-bit design point, the same bound
            // BloomSizingProbe holds a steady-shape file to — a step in the data must not buy the
            // file that was already sized correctly a larger filter.
            Assert.True(bitsPerTerm <= 40.0,
                $"group {i}: {bitsPerTerm:F1} bits/term — the filter is sized for terms this group does not hold");
        }
    }

    /// <summary>
    /// The floor under the forecast, at the only shape that can reach it: a level and a message
    /// template and nothing else — no service name, no properties — which is two bloom terms an
    /// event, four after the headroom, under <see cref="SegmentWriter.MinBloomTermsPerEvent"/>.
    ///
    /// <para>Left unfloored, a file like this teaches the writer to forecast one or two terms an
    /// event, and the first group of normal traffic after it gets cut to a sliver by the seal —
    /// each sliver carrying its own inverted and trigram sections. The floor is what bounds that,
    /// and it costs bits only on files this degenerate.</para>
    /// </summary>
    [Fact]
    public void ADegenerateGroupsForecastIsFlooredAtTheMinimumTermsPerEvent()
    {
        var bare   = new PropShape(0, 0);
        var groups = MergeAndMeasure(bare, bare, sourcesBefore: 8, sourcesAfter: 8, eventsPerSource: 4_000,
                                     withService: false, name: "bare");

        _out.WriteLine($"  bare events, {groups.Count} groups");
        foreach (var g in groups)
            _out.WriteLine($"  {g.Index,4} | {g.Events,7:N0} events | {g.Terms / (double)g.Events,4:F1} terms/ev | " +
                           $"{g.BloomBytes * 8.0 / g.Events,5:F1} filter bits/event");

        Assert.True(groups.Count >= 3, $"{groups.Count} group(s) — not enough to have a forecast carried into one");

        for (int i = 1; i < groups.Count; i++)
        {
            double termsPerEvent = groups[i].Terms / (double)groups[i].Events;
            double bitsPerEvent  = groups[i].BloomBytes * 8.0 / groups[i].Events;

            // The floor is what is being tested, so first prove the measurement would have gone
            // under it: doubled, this shape is still below the floor.
            Assert.True(termsPerEvent * 2 < SegmentWriter.MinBloomTermsPerEvent,
                $"group {i} holds {termsPerEvent:F1} terms/event — not degenerate enough for the floor to bind, " +
                "so this test proves nothing about it");

            // The last group is short (it holds whatever is left), which makes its forecast
            // generous rather than tight — so the bound is one-sided and only ever too weak here.
            Assert.True(bitsPerEvent >= SegmentWriter.MinBloomTermsPerEvent * 10 * 0.9,
                $"group {i} was sized at {bitsPerEvent:F1} filter bits/event, under the " +
                $"{SegmentWriter.MinBloomTermsPerEvent * 10} the floor buys — the forecast followed the " +
                "degenerate measurement down");
        }
    }

    private readonly record struct GroupCost(int Index, long Events, long Terms, long BloomBytes, long PayloadBytes);

    private List<GroupCost> MergeAndMeasure(
        PropShape before, PropShape after, int sourcesBefore, int sourcesAfter, int eventsPerSource,
        bool withService = true, string name = "merged")
    {
        string sub = Path.Combine(_dir, $"src-{name}-{before.Count}x{before.ValueChars}-{after.Count}x{after.ValueChars}");
        Directory.CreateDirectory(sub);

        // Non-overlapping in time, and the merge reads in timestamp order, so every "before"
        // event precedes every "after" one and the change lands on a group boundary.
        long origin = new DateTimeOffset(2026, 8, 1, 0, 0, 0, TimeSpan.Zero).UtcTicks;
        var  paths  = new List<string>(sourcesBefore + sourcesAfter);
        for (int s = 0; s < sourcesBefore; s++)
            paths.Add(WriteSource(sub, s, eventsPerSource, before, withService,
                                  origin + (long)s * eventsPerSource * TimeSpan.TicksPerMillisecond));
        for (int s = 0; s < sourcesAfter; s++)
            paths.Add(WriteSource(sub, sourcesBefore + s, eventsPerSource, after, withService,
                                  origin + (long)(sourcesBefore + s) * eventsPerSource * TimeSpan.TicksPerMillisecond));

        string p     = Path.Combine(_dir, name + ".seg");
        var    built = new List<SegmentIndexBuilder>();
        using (var source = MergingSegmentEventSource.Open(paths))
        using (var writer = new SegmentWriter(p, GroupBudget))
        {
            writer.WriteEvents(source, (count, termsPerEvent) =>
            {
                var b = new SegmentIndexBuilder(count, 5, termsPerEvent);
                built.Add(b);
                return b;
            }, CancellationToken.None);
            writer.Finalise(new NodeId(0), new SegmentId(1UL));
        }

        var costs = new List<GroupCost>();
        using (var reader = SegmentReader.Open(p))
        {
            var    groups = reader.Groups;
            byte[] blk    = [];
            for (int g = 0; g < groups.Length; g++)
            {
                using var bloom = reader.RentBloomFilterBytes(g);
                // Payload per group, from the blocks themselves — terms per PAYLOAD BYTE is the
                // density that decides saturation, because payload is what seals a group.
                long payload = 0;
                for (int b = 0; b < groups[g].BlockCount; b++)
                    payload += reader.ReadBlockInto(groups[g].FirstBlock + b, ref blk);
                costs.Add(new GroupCost(g, groups[g].EventCount,
                                        g < built.Count ? built[g].BloomTermsAdded : 0,
                                        bloom.Span.Length, payload));
            }
        }
        return costs;
    }

    private static string WriteSource(string dir, int sourceIndex, int events, PropShape shape,
                                      bool withService, long baseTicks)
    {
        string path = Path.Combine(dir, $"src-{sourceIndex:D2}.seg");
        if (File.Exists(path)) return path;

        var pool = new StringInternPool();
        using var hot = new HotTierSegment(events + 1, (long)events * 2048 + (32L << 20));
        int tmplIdx = pool.Intern("Settlement {Stage} for wallet {Wallet} finished with {Status}");
        int svcIdx  = withService ? pool.Intern("Etisalat.Payments.Settlement") : -1;

        var rng = new Random(4242 + sourceIndex);
        var buf = new ArrayBufferWriter<byte>(4096);
        for (int i = 0; i < events; i++)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(shape.Count);
            for (int k = 0; k < shape.Count; k++)
            {
                w.Write("p" + k.ToString("D2"));
                // Distinct values, so terms are terms and not one entry the filter sets once.
                w.Write(rng.NextInt64().ToString("x16")[..Math.Min(16, shape.ValueChars)]
                        .PadRight(shape.ValueChars, 'z'));
            }
            w.Flush();

            var h = new LogEventHeader
            {
                Id                       = new EventId((uint)sourceIndex, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerMillisecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
            };
            if (!hot.TryWrite(h, buf.WrittenSpan, null, null)) break;
        }
        hot.Freeze();

        using var writer = new SegmentWriter(path);
        writer.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot));
        writer.Finalise(new NodeId(0), new SegmentId((ulong)sourceIndex + 1));
        return path;
    }
}
