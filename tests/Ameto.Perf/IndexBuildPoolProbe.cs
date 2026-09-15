using System.Buffers;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using MessagePack;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// What the index-build pools park between groups, for the two tier sizes that matter: the
/// 16 MB tier the 512 MB stand runs, and the 64 MB default. Groups are built and released in
/// sequence as the writer would, with the hints carried from one to the next, and the pooled
/// bytes read after each; the number that matters is the resting level after the last one.
/// </summary>
public sealed class IndexBuildPoolProbe
{
    private readonly ITestOutputHelper _out;
    public IndexBuildPoolProbe(ITestOutputHelper o) => _out = o;

    [Theory]
    [InlineData(16, 4)]
    [InlineData(64, 2)]
    public void PooledBytes_AfterGroups(int groupMb, int groups)
    {
        var pool   = new StringInternPool();
        int svcIdx = pool.Intern("Etisalat.API");
        int perGroup = groupMb * 2_000;                              // ~500 B of row per prop-dense event
        using var hot = Tier(perGroup * groups, pool, svcIdx);
        var hints = new IndexBuildHints();

        IndexBuildPool.TrimAll();
        _out.WriteLine($"{groupMb} MB groups x {groups} ({perGroup:N0} events each):");
        for (int g = 0; g < groups; g++)
        {
            using var b = new SegmentIndexBuilder(perGroup, 5, 0, hints);
            b.Build(hot, pool, null, g * perGroup, perGroup);
            long held = b.BuildRetainedBytes;
            b.WriteSections(Stream.Null, out _, out _, out _);
            b.Dispose();
            _out.WriteLine($"  group {g}: held {held >> 20,4} MB during the build, {IndexBuildPool.PooledBytes >> 20,4} MB parked after it (hints: {hints.LastTerms:N0} terms / {hints.LastTrigrams:N0} trigrams)");
        }
        IndexBuildPool.TrimIdle();
        _out.WriteLine($"  after one idle trim (a gen2 with nothing rented since): {IndexBuildPool.PooledBytes >> 20} MB");
        IndexBuildPool.TrimIdle();
        _out.WriteLine($"  after a second:                                        {IndexBuildPool.PooledBytes >> 20} MB");
        IndexBuildPool.TrimAll();
    }

    private static HotTierSegment Tier(int events, StringInternPool pool, int svcIdx)
    {
        var hot = new HotTierSegment(2_000_000, 256L * 1024 * 1024);
        long baseTicks = DateTimeOffset.UtcNow.UtcTicks;
        var rng = new Random(5);
        string[] methods = { "GET", "POST", "PUT", "DELETE" };
        string[] routes  = { "/api/pay", "/api/topup", "/api/status", "/api/balance" };
        int tmplIdx = pool.Intern("HTTP {Method} {Route} responded {Status} in {Elapsed} ms");
        string tmpl = pool.Get(tmplIdx);
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
            if (!hot.TryWrite(new LogEventHeader
                {
                    Id = new EventId(0u, (uint)i).RawValue, TimestampUtcTicks = baseTicks + i,
                    Level = LogLevel.Information, MessageTemplatePoolIndex = tmplIdx, ServiceNamePoolIndex = svcIdx,
                    TraceIdHi = (ulong)rng.NextInt64(), TraceIdLo = (ulong)rng.NextInt64(), SpanId = (ulong)rng.NextInt64(),
                }, buf.WrittenSpan, tmpl)) break;
        }
        return hot;
    }
}
