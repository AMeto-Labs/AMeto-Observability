using System.Buffers;
using System.Diagnostics;
using Ameto.Core;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// DIAGNOSTIC PROBE. The two things a flush does to a frozen tier BEFORE a single byte is
/// written — decide the file order, then partition that order by log level — measured on their
/// own, because they happen on the flush thread while the ingest path is still filling the
/// successor tier.
///
/// <para>A = what this cost before: <c>Array.Sort(int[], Comparison&lt;int&gt;)</c> over a
/// closure that did two random <c>GetHeader</c> reads per comparison, then a
/// <c>List&lt;int&gt;</c> per level grown by doubling and copied out with <c>ToArray()</c>.
/// B = what it costs now: one sequential extraction pass with an identity fast path and a
/// struct comparer, then count-then-fill into pooled arrays.</para>
///
/// <para>Both routes must produce the SAME order — the probe asserts it — because the order is
/// the file's byte order and segment files are pinned byte-for-byte by the storage tests.</para>
/// </summary>
public sealed class FlushSortSplitProbe
{
    private readonly ITestOutputHelper _out;
    public FlushSortSplitProbe(ITestOutputHelper o) => _out = o;

    private const int Events = 200_000;
    private const int LevelSlots = 6;
    private const double KB = 1024.0;

    [Fact]
    public void SortOrderAndLevelSplit_Cost()
    {
        var pool = new StringInternPool();
        using var arrived = BuildTier(pool, shuffleTimestamps: false);
        using var jittered = BuildTier(pool, shuffleTimestamps: true);

        // FIRST, on a cold array pool — which is what a server's first flush after start meets,
        // and the only place the sort's own buffers are visible at all. The steady-state figures
        // below cannot see them: a rented buffer is allocated once and then reused for the life
        // of the process, so it stops being counted while still being RETAINED, per core, for as
        // long as the pool holds it. An arrival-ordered tier takes the identity path and must
        // therefore rent nothing: its cost is the caller's int[] order and not a byte more.
        long coldBefore = GC.GetAllocatedBytesForCurrentThread();
        int[] coldOrder = SegmentWriter.ComputeSortOrder(arrived);
        long coldAlloc  = GC.GetAllocatedBytesForCurrentThread() - coldBefore;
        _out.WriteLine($"cold pool, arrival-ordered tier: first ComputeSortOrder allocates "
                     + $"{coldAlloc / KB:N0} KB for {Events:N0} events "
                     + $"({coldAlloc / (double)Events:F1} B/event; the int[] order alone is "
                     + $"{coldOrder.Length * 4 / KB:N0} KB)\n");
        Assert.True(coldAlloc < coldOrder.Length * 4 + 4096,
            $"the identity path allocated {coldAlloc} B for a {coldOrder.Length}-entry order — "
            + "it is renting sort keys before it knows whether it needs them");

        // Warm the JIT and the array pool so the first measured run is steady state.
        Measure(arrived, baseline: true);
        Measure(arrived, baseline: false);

        _out.WriteLine($"sort order + level split, {Events:N0} events, {LevelSlots} level slots\n");

        foreach (var (name, hot) in new[] { ("arrival-ordered tier", arrived), ("jittered tier", jittered) })
        {
            // Each variant is warmed IMMEDIATELY before it is measured, not both up front. The
            // first rent of an array-pool bucket allocates it, and a gen2 collection — which the
            // other variant's megabytes of list garbage can trigger — trims those buckets again.
            // Interleaving the two would charge whichever ran second for the other's collection.
            Measure(hot, baseline: true);
            var a = Measure(hot, baseline: true);
            Measure(hot, baseline: false);
            var b = Measure(hot, baseline: false);

            Assert.Equal(a.Order, b.Order);
            Assert.Equal(a.Partitions, b.Partitions);

            _out.WriteLine($"  {name}");
            _out.WriteLine($"    A  delegate sort + List<int> split   {a.SortMs,7:F1} ms sort  {a.SplitMs,7:F1} ms split  {a.AllocBytes / KB,10:N0} KB");
            _out.WriteLine($"    B  struct-key sort + pooled split    {b.SortMs,7:F1} ms sort  {b.SplitMs,7:F1} ms split  {b.AllocBytes / KB,10:N0} KB");
            _out.WriteLine($"    ⇒  {a.SortMs / Math.Max(0.001, b.SortMs),5:F1}× sort, {a.SplitMs / Math.Max(0.001, b.SplitMs),5:F1}× split, "
                         + $"{(a.AllocBytes - b.AllocBytes) / KB:N0} KB less garbage\n");
        }
    }

    private readonly record struct Result(double SortMs, double SplitMs, long AllocBytes, string Order, string Partitions);

    private static Result Measure(HotTierSegment hot, bool baseline)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();

        var sw = Stopwatch.StartNew();
        int[] order = baseline ? BaselineComputeSortOrder(hot) : SegmentWriter.ComputeSortOrder(hot);
        sw.Stop();
        double sortMs = sw.Elapsed.TotalMilliseconds;

        string orderHash = Hash(order, order.Length);
        string partitions;

        sw.Restart();
        if (baseline)
        {
            var perLevel = new List<int>[LevelSlots];
            for (int oi = 0; oi < order.Length; oi++)
            {
                int lvl = (int)hot.GetHeader(order[oi]).Level;
                if ((uint)lvl >= LevelSlots) lvl = (int)LogLevel.Information;
                (perLevel[lvl] ??= new List<int>()).Add(order[oi]);
            }
            var subsets = new string[LevelSlots];
            for (int l = 0; l < LevelSlots; l++)
            {
                var idx = perLevel[l];
                if (idx is null || idx.Count == 0) { subsets[l] = ""; continue; }
                var subset = idx.ToArray();               // the copy the writer was handed
                subsets[l] = Hash(subset, subset.Length);
            }
            sw.Stop();
            partitions = string.Join('|', subsets);
        }
        else
        {
            var perLevel = new int[LevelSlots][];
            var counts   = new int[LevelSlots];
            StorageEngine.SplitOrderByLevel(hot, order, perLevel, counts);
            sw.Stop();
            var subsets = new string[LevelSlots];
            for (int l = 0; l < LevelSlots; l++)
                subsets[l] = counts[l] == 0 ? "" : Hash(perLevel[l]!, counts[l]);
            for (int l = 0; l < LevelSlots; l++)
                if (perLevel[l] is { } arr) ArrayPool<int>.Shared.Return(arr);
            partitions = string.Join('|', subsets);
        }

        long alloc = GC.GetAllocatedBytesForCurrentThread() - before;
        return new Result(sortMs, sw.Elapsed.TotalMilliseconds, alloc, orderHash, partitions);
    }

    /// <summary>The pre-change implementation, kept here so the comparison stays runnable.</summary>
    private static int[] BaselineComputeSortOrder(HotTierSegment hot)
    {
        int count = hot.Count;
        var order = new int[count];
        for (int i = 0; i < count; i++) order[i] = i;
        Array.Sort(order, (a, b) =>
        {
            ref var ha = ref hot.GetHeader(a);
            ref var hb = ref hot.GetHeader(b);
            int c = ha.TimestampUtcTicks.CompareTo(hb.TimestampUtcTicks);
            return c != 0 ? c : ha.Id.CompareTo(hb.Id);
        });
        return order;
    }

    private static string Hash(int[] values, int count)
    {
        // FNV-1a over the permutation — a fingerprint, not a collection to compare element-wise
        // (Assert.Equal over 200k ints prints a 200k-element diff on failure).
        ulong h = 14695981039346656037UL;
        for (int i = 0; i < count; i++)
        {
            uint v = (uint)values[i];
            for (int b = 0; b < 4; b++) { h ^= (byte)(v >> (b * 8)); h *= 1099511628211UL; }
        }
        return count + ":" + h.ToString("x16");
    }

    private static HotTierSegment BuildTier(StringInternPool pool, bool shuffleTimestamps)
    {
        var hot = new HotTierSegment(Events + 1, (long)Events * 256 + 1024 * 1024);
        int tmplIdx  = pool.Intern("HTTP request handled");
        string tmpl  = pool.Get(tmplIdx);
        int svcIdx   = pool.Intern("Wallet.API");
        long baseTicks = DateTimeOffset.UtcNow.UtcTicks;
        var rng = new Random(7);

        // Level mix of a real tier: mostly Information, a tail of Warning/Error/Debug.
        LogLevel[] levels =
        [
            LogLevel.Information, LogLevel.Information, LogLevel.Information, LogLevel.Information,
            LogLevel.Information, LogLevel.Information, LogLevel.Information, LogLevel.Debug,
            LogLevel.Warning, LogLevel.Error,
        ];

        Span<byte> props = stackalloc byte[1];
        props[0] = 0x80;   // empty msgpack map
        for (int i = 0; i < Events; i++)
        {
            // Arrival order is the tier's write order; "jittered" models concurrent writers
            // stamping timestamps a few ticks out of the order they reserve slots in.
            long ticks = baseTicks + i * 10 + (shuffleTimestamps ? rng.Next(-40, 41) : 0);
            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = ticks,
                Level                    = levels[i % levels.Length],
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
                TraceIdHi                = (ulong)i,
                TraceIdLo                = (ulong)i + 1,
                SpanId                   = (ulong)i + 2,
            };
            Assert.True(hot.TryWrite(h, props, tmpl));
        }
        hot.Freeze();
        return hot;
    }
}
