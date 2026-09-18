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
            using var idx = SegmentIndexReader.Load(
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

    /// <summary>
    /// The shape finding #3 is about: an Error segment, where every candidate row carries a
    /// stack trace and a template drawn from a small vocabulary, and the filter rejects most of
    /// what the index handed over. What a REJECTED candidate costs is the whole question —
    /// so the measurement reads the rows without ever touching <c>Exception</c>, which is what
    /// an evaluator does for any predicate that is not about the exception.
    /// </summary>
    [Fact]
    public async Task RejectedCandidate_DoesNotPayForItsExceptionTree()
    {
        string path = Path.Combine(Path.GetTempPath(), $"cand-exc-{Guid.NewGuid():N}.seg");
        try
        {
            BuildExceptionSegment(path);
            using var reader = SegmentReader.Open(path);

            await DrainAsync(reader, null);                 // warm (JIT + ArrayPool)
            await DrainDecodingExceptionsAsync(reader);

            long scanned  = await MeasureAsync(reader, null);
            long decoded  = await MeasureDecodingAsync(reader);

            _out.WriteLine($"events={ExcEvents} exception-bearing rows, {Templates} distinct templates");
            _out.WriteLine($"scan, exception untouched : {scanned / 1024.0:F1} KB  ({scanned / (double)ExcEvents:F0} B/row)");
            _out.WriteLine($"scan + decode every one   : {decoded / 1024.0:F1} KB  ({decoded / (double)ExcEvents:F0} B/row)");
            _out.WriteLine($"deferred                  : {(decoded - scanned) / 1024.0:F1} KB  ({(double)decoded / scanned:F1}x)");

            // A row the filter will reject must not pay for the tree it never reads.
            Assert.True(scanned * 2 < decoded,
                $"the exception tree is not being deferred: untouched={scanned} B, decoded={decoded} B");
        }
        finally { File.Delete(path); }
    }

    private static async Task<long> MeasureDecodingAsync(SegmentReader reader)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        await DrainDecodingExceptionsAsync(reader);
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static async Task DrainDecodingExceptionsAsync(SegmentReader reader)
    {
        await foreach (var ev in reader.ReadEventsAsync(null, null, null))
            _ = ev.Exception?.Type;
    }

    private const int ExcEvents = 4_000;
    private const int Templates = 20;

    private static void BuildExceptionSegment(string path)
    {
        var pool   = new StringInternPool();
        int svcIdx = pool.Intern("Billing.Api");

        var tmplIdx = new int[Templates];
        for (int t = 0; t < Templates; t++) tmplIdx[t] = pool.Intern($"order {{OrderId}} failed at stage {t}");

        using var hot = new HotTierSegment(ExcEvents + 1, (long)ExcEvents * 4096 + (1 << 20));
        long baseTicks = DateTimeOffset.UtcNow.UtcTicks;

        var buf = new ArrayBufferWriter<byte>(256);
        for (int i = 0; i < ExcEvents; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1);
            w.Write("orderId"); w.Write((long)i);
            w.Flush();

            int t = i % Templates;
            var exc = new ExceptionInfo
            {
                Type       = "System.InvalidOperationException",
                Message    = "order could not be settled",
                StackTrace = string.Join('\n', Enumerable.Repeat(
                    "   at Billing.Api.Orders.SettleAsync(OrderId id) in /src/Orders.cs:line 118", 14)),
                Inner      = new ExceptionInfo { Type = "System.FormatException", Message = "bad amount" },
            };

            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i,
                Level                    = Ameto.Core.LogLevel.Error,
                MessageTemplatePoolIndex = tmplIdx[t],
                ServiceNamePoolIndex     = svcIdx,
                HasException             = true,
            };
            Assert.True(hot.TryWrite(h, buf.WrittenSpan, pool.Get(tmplIdx[t]), exc));
        }
        hot.Freeze();

        var order = SegmentWriter.ComputeSortOrder(hot);
        using var sw = new SegmentWriter(path);
        sw.WriteEvents(hot, pool, order);
        sw.Finalise(new NodeId(0), new SegmentId(7UL));
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
