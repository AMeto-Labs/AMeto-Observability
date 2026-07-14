using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Measures managed bytes allocated while building a segment's indexes (inverted/trigram/bloom)
/// from a realistic hot tier — the flush-path allocator that balloons the GC heap under load.
/// Baseline probe for the zero-alloc-flush work.
/// </summary>
public sealed class IndexBuildAllocProbe
{
    private readonly ITestOutputHelper _out;
    public IndexBuildAllocProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void IndexBuild_BytesPerSegment()
    {
        const int events = 50_000;
        var pool = new StringInternPool();
        int svcIdx = pool.Intern("Etisalat.API");
        using var hot = BuildRealisticSegment(events, pool, svcIdx);

        // Warm up.
        for (int i = 0; i < 2; i++) { var b = new SegmentIndexBuilder(hot.Count); b.Build(hot, pool); }

        long b0 = GC.GetAllocatedBytesForCurrentThread();
        var builder = new SegmentIndexBuilder(hot.Count);
        builder.Build(hot, pool);
        long buildBytes = GC.GetAllocatedBytesForCurrentThread() - b0;

        long s0 = GC.GetAllocatedBytesForCurrentThread();
        _ = builder.SerialisedInvertedIndex;
        _ = builder.SerialisedTrigramIndex;
        _ = builder.SerialisedBloomFilter;
        long serialiseBytes = GC.GetAllocatedBytesForCurrentThread() - s0;

        string report = $"index — {events:N0} events:  build={buildBytes / 1048576.0:F1} MB  " +
                        $"serialise={serialiseBytes / 1048576.0:F1} MB  total={(buildBytes + serialiseBytes) / 1048576.0:F1} MB";
        _out.WriteLine(report);
        Assert.True(hot.Count > 0);
    }

    private static HotTierSegment BuildRealisticSegment(int events, StringInternPool pool, int svcIdx)
    {
        var hot = new HotTierSegment(events + 1, (long)events * 512 + 1024 * 1024);
        long baseTicks = DateTimeOffset.UtcNow.UtcTicks;
        var rng = new Random(5);
        string[] methods = { "GET", "POST", "PUT", "DELETE" };
        string[] routes  = { "/api/pay", "/api/topup", "/api/status", "/api/balance" };
        string tmpl = pool.Get(pool.Intern("HTTP request handled"));

        var buf = new ArrayBufferWriter<byte>(512);
        for (int i = 0; i < events; i++)
        {
            buf.Clear();
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
                TimestampUtcTicks        = baseTicks + i,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = pool.Intern("HTTP request handled"),
                ServiceNamePoolIndex     = svcIdx,
            };
            hot.TryWrite(h, buf.WrittenSpan, tmpl);
        }
        return hot;
    }
}
