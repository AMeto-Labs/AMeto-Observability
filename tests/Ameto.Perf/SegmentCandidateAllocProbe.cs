using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Quantifies the allocation win of candidate-driven segment reads (v5): a selective
/// filter over a segment should allocate proportionally to the MATCHES, not to the
/// segment size — rejected rows must not materialise a Dictionary/strings.
/// </summary>
public sealed class SegmentCandidateAllocProbe
{
    private const int Events = 20_000;

    private readonly ITestOutputHelper _out;
    public SegmentCandidateAllocProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public async Task CandidateScan_AllocatesFractionOfFullScan()
    {
        var pool = new StringInternPool();
        using var hot = BuildHotTier(pool);

        var order   = SegmentWriter.ComputeSortOrder(hot);
        var builder = new SegmentIndexBuilder(hot.Count);
        builder.Build(hot, pool, order);

        string path = Path.Combine(Path.GetTempPath(), $"cand-alloc-{Guid.NewGuid():N}.seg");
        try
        {
            using (var w = new SegmentWriter(path))
            {
                w.WriteEvents(hot, pool, order);
                w.WriteInvertedIndex(builder.SerialisedInvertedIndex);
                w.WriteTrigramIndex(builder.SerialisedTrigramIndex);
                w.WriteBloomFilter(builder.SerialisedBloomFilter);
                w.Finalise(new NodeId(0), new SegmentId(3UL));
            }

            using var reader = SegmentReader.Open(path);
            var idx = SegmentIndexReader.Load(
                reader.ReadInvertedIndexBytes(),
                reader.ReadTrigramIndexBytes(),
                reader.ReadBloomFilterBytes());
            var candidates = idx.LookupIntersect(
                new List<(string property, object? value)> { ("bucket", "needle") })!;

            // Warm-up (JIT + ArrayPool).
            await DrainAsync(reader, null);
            await DrainAsync(reader, candidates);

            long fullScan = await MeasureAsync(reader, null);
            long candScan = await MeasureAsync(reader, candidates);

            _out.WriteLine($"events={Events}, candidates={candidates.Length}");
            _out.WriteLine($"full scan : {fullScan / 1024.0:F1} KB allocated");
            _out.WriteLine($"cand scan : {candScan / 1024.0:F1} KB allocated  ({(double)fullScan / candScan:F1}x less)");

            // Candidate scan materialises ~1% of the rows — allocations must drop
            // by at least an order of magnitude.
            Assert.True(candScan * 10 < fullScan,
                $"expected ≥10x reduction, got full={fullScan} cand={candScan}");
        }
        finally { File.Delete(path); }
    }

    private static async Task<long> MeasureAsync(SegmentReader reader, uint[]? candidates)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        await DrainAsync(reader, candidates);
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static async Task DrainAsync(SegmentReader reader, uint[]? candidates)
    {
        await foreach (var _ in reader.ReadEventsAsync(candidates, null, null)) { }
    }

    private static HotTierSegment BuildHotTier(StringInternPool pool)
    {
        var hot = new HotTierSegment(Events + 1, (long)Events * 512 + 1024 * 1024);

        int    tmplIdx = pool.Intern("evt");
        string tmpl    = pool.Get(tmplIdx);
        int    svcIdx  = pool.Intern("Svc.A");
        long   baseTicks = DateTimeOffset.UtcNow.UtcTicks;

        var buf = new ArrayBufferWriter<byte>(256);
        for (int i = 0; i < Events; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(3);
            w.Write("orderId");    w.Write((long)i);
            w.Write("customerId"); w.Write("cust-" + (i % 500));
            w.Write("bucket");     w.Write(i % 97 == 0 ? "needle" : "hay");
            w.Flush();

            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i,
                Level                    = Ameto.Core.LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
            };
            Assert.True(hot.TryWrite(h, buf.WrittenSpan, tmpl));
        }
        hot.Freeze();
        return hot;
    }
}
