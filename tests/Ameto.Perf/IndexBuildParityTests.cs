using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// The zero-alloc streaming index build (<see cref="SegmentIndexBuilder.Build"/>) must be
/// byte-for-byte identical to the old dictionary path (<see cref="SegmentIndexBuilder.BuildReference"/>).
/// Identical serialised index bytes ⇒ queries behave identically. Correctness gate for the rewrite.
/// </summary>
public sealed class IndexBuildParityTests
{
    [Fact]
    public void StreamingBuild_MatchesDictionaryBuild_ByteForByte()
    {
        var pool = new StringInternPool();
        int svcIdx = pool.Intern("Etisalat.API");
        using var hot = BuildSegment(pool, svcIdx);

        var streaming = new SegmentIndexBuilder(hot.Count);
        streaming.Build(hot, pool);

        var reference = new SegmentIndexBuilder(hot.Count);
        reference.BuildReference(hot, pool);

        Assert.Equal(reference.SerialisedInvertedIndex, streaming.SerialisedInvertedIndex);
        Assert.Equal(reference.SerialisedTrigramIndex,  streaming.SerialisedTrigramIndex);
        Assert.Equal(reference.SerialisedBloomFilter,   streaming.SerialisedBloomFilter);
    }

    private static HotTierSegment BuildSegment(StringInternPool pool, int svcIdx)
    {
        const int events = 2_000;
        var hot = new HotTierSegment(events + 1, (long)events * 1024 + 1024 * 1024);
        long baseTicks = DateTimeOffset.UtcNow.UtcTicks;
        var rng = new Random(11);
        string[] methods = { "GET", "POST", "PUT", "DELETE" };
        string[] routes  = { "/api/pay", "/api/topup", "/api/status" };
        string tmpl = pool.Get(pool.Intern("HTTP request handled"));
        var buf = new ArrayBufferWriter<byte>(1024);

        for (int i = 0; i < events; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            bool withNested = i % 7 == 0;
            bool withNull   = i % 11 == 0;
            int fields = 7 + (withNested ? 1 : 0) + (withNull ? 1 : 0);
            w.WriteMapHeader(fields);
            w.Write("orderId");          w.Write((long)rng.Next(0, 10_000_000)); // long
            w.Write("customerId");       w.Write("cust-" + rng.Next(0, 1000));   // string
            w.Write("http.method");      w.Write(methods[rng.Next(methods.Length)]);
            w.Write("http.route");       w.Write(routes[rng.Next(routes.Length)]);
            w.Write("http.status_code"); w.Write((long)new[] { 200, 404, 500 }[rng.Next(3)]);
            w.Write("duration_ms");      w.Write(Math.Round(rng.NextDouble() * 500, 3)); // double
            w.Write("cache_hit");        w.Write(rng.Next(2) == 0);                       // bool
            if (withNested)
            {
                w.Write("user");
                w.WriteMapHeader(2);
                w.Write("name"); w.Write("alice");
                w.Write("age");  w.Write((long)rng.Next(18, 90));
            }
            if (withNull) { w.Write("optional"); w.WriteNil(); }
            w.Flush();

            bool withTrace = i % 3 == 0;
            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i,
                Level                    = (LogLevel)(i % 6),
                MessageTemplatePoolIndex = pool.Intern("HTTP request handled"),
                ServiceNamePoolIndex     = svcIdx,
                TraceIdHi                = withTrace ? 0xF6F6F098569A7F2BUL : 0,
                TraceIdLo                = withTrace ? 0xA54F3C734AA563F0UL : 0,
                SpanId                   = withTrace ? 0xA1B2C3D4E5F60718UL : 0,
            };
            hot.TryWrite(h, buf.WrittenSpan, tmpl);
        }
        return hot;
    }
}
