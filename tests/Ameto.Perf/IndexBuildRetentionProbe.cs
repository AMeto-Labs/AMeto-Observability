using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// DIAGNOSTIC PROBE (temporary — added while investigating the "RSS climbs to ~1 GB,
/// CPU spikes then drops to 0" report).
///
/// <para><see cref="IndexBuildAllocProbe"/> measures allocation CHURN (bytes passed through
/// the allocator). Churn is cheap when it dies in gen0. What actually drives process RSS is
/// the RETAINED build-phase state: <c>SegmentInvertedIndex</c> holds
/// <c>Dictionary&lt;string, Dictionary&lt;string, List&lt;int&gt;&gt;&gt;</c> and
/// <c>SegmentTrigramIndex</c> holds <c>Dictionary&lt;(char,char,char), HashSet&lt;int&gt;&gt;</c>
/// alive for the whole flush — and up to <c>HotTier.FlushConcurrency</c> of these exist at once.</para>
///
/// <para>This probe measures live-heap growth held by ONE builder, and contrasts a
/// low-cardinality tier (what the existing probe / the k6 load test produce) with a
/// trace-carrying tier (what a real service fleet produces, where TraceId and SpanId are
/// unique per event).</para>
/// </summary>
public sealed class IndexBuildRetentionProbe
{
    private readonly ITestOutputHelper _out;
    public IndexBuildRetentionProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void IndexBuild_RetainedBytesPerSegment()
    {
        // A 64 MB hot tier (HotTierOptions.MaxSizeBytes default) at ~480 B of properties
        // per event flushes at roughly this many events.
        const int events = 130_000;

        _out.WriteLine($"LogEventHeader.SizeOf = {LogEventHeader.SizeOf} B");
        _out.WriteLine("");

        Measure("low-cardinality  (no trace ids — what k6-100k.js sends)", events, withTraceIds: false);
        _out.WriteLine("");
        Measure("trace-carrying   (unique TraceId+SpanId — real OTLP fleet)", events, withTraceIds: true);
    }

    /// <summary>
    /// StorageEngine sizes its in-flight-tier budget as <c>MaxSizeBytes * 1.4</c>. That factor
    /// only holds when the average event fills its share of a chunk's 8 MB payload area
    /// (~512 B). Chunks are claimed by SLOT index (16384 events/chunk) but the flush is
    /// triggered by PAYLOAD bytes — so small events claim chunks far faster than they fill
    /// them, and the native footprint per tier overshoots the assumed 1.4x badly.
    /// </summary>
    [Fact]
    public void HotTier_NativeFootprint_VsAssumed1_4x()
    {
        const long maxSizeBytes = 64L * 1024 * 1024;
        _out.WriteLine($"HotTier.MaxSizeBytes = {maxSizeBytes / 1048576} MB");
        _out.WriteLine($"StorageEngine assumes a frozen tier costs {maxSizeBytes * 1.4 / 1048576:F0} MB native (1.4x)");
        _out.WriteLine("");
        _out.WriteLine("  avg payload |  events to fill |  chunks |  native MB |  actual factor");
        _out.WriteLine("  ------------+-----------------+---------+------------+---------------");

        foreach (int payloadBytes in new[] { 512, 300, 150, 64, 16 })
        {
            using var hot = FillWithFixedPayload(payloadBytes, maxSizeBytes);
            long native = hot.AllocatedBytes;
            _out.WriteLine(
                $"  {payloadBytes,10} B | {hot.Count,15:N0} | {native / 9437184,7} | {native / 1048576.0,9:F0} MB | {native / (double)maxSizeBytes,13:F2}x");
        }
    }

    private static HotTierSegment FillWithFixedPayload(int payloadBytes, long maxSizeBytes)
    {
        var hot = new HotTierSegment(2_000_000, maxSizeBytes);
        var payload = new byte[payloadBytes];
        var h = new LogEventHeader { Level = LogLevel.Information, TimestampUtcTicks = DateTimeOffset.UtcNow.UtcTicks };
        // Write until the tier reports full — exactly the condition StorageEngine flushes on.
        while (!hot.IsFull && hot.TryWrite(h, payload)) { }
        return hot;
    }

    private void Measure(string label, int events, bool withTraceIds)
    {
        var pool = new StringInternPool();
        int svcIdx = pool.Intern("Etisalat.API");

        using var hot = BuildSegment(events, pool, svcIdx, withTraceIds);

        long tierNative = hot.AllocatedBytes;

        // Settle the heap, then take the live-set baseline with the tier already alive
        // so we attribute only what the BUILDER retains.
        long before = GC.GetTotalMemory(forceFullCollection: true);

        var builder = new SegmentIndexBuilder(hot.Count);
        builder.Build(hot, pool);

        // Live set with the builder's dictionaries still reachable == what one in-flight
        // flush pins in the GC heap.
        long afterBuild = GC.GetTotalMemory(forceFullCollection: true);

        var inverted = builder.SerialisedInvertedIndex;
        var trigram  = builder.SerialisedTrigramIndex;
        var bloom    = builder.SerialisedBloomFilter;

        long afterSerialise = GC.GetTotalMemory(forceFullCollection: true);

        GC.KeepAlive(builder);

        const double MB = 1048576.0;
        _out.WriteLine($"── {label}");
        _out.WriteLine($"   events                       {events:N0}");
        _out.WriteLine($"   hot tier native (off-heap)   {tierNative / MB,8:F1} MB   ({hot.AllocatedBytes / 9437184} chunks x 9 MB)");
        _out.WriteLine($"   RETAINED by index build      {(afterBuild - before) / MB,8:F1} MB   <-- x FlushConcurrency concurrent flushes");
        _out.WriteLine($"   + serialised index blobs     {(afterSerialise - afterBuild) / MB,8:F1} MB   (LOH: {inverted.Length / MB:F1} inv / {trigram.Length / MB:F1} tri / {bloom.Length / MB:F1} bloom)");
        _out.WriteLine($"   PEAK per in-flight flush     {(afterSerialise - before) / MB,8:F1} MB");

        Assert.True(hot.Count > 0);
    }

    private static HotTierSegment BuildSegment(int events, StringInternPool pool, int svcIdx, bool withTraceIds)
    {
        // Mirror CreateHotTier(): 2 M slot cap, payload budget = HotTier.MaxSizeBytes.
        var hot = new HotTierSegment(2_000_000, 64L * 1024 * 1024);
        long baseTicks = DateTimeOffset.UtcNow.UtcTicks;
        var rng = new Random(5);
        string[] methods = { "GET", "POST", "PUT", "DELETE" };
        string[] routes  = { "/api/pay", "/api/topup", "/api/status", "/api/balance" };
        string tmpl = pool.Get(pool.Intern("HTTP {Method} {Route} responded {Status} in {Elapsed} ms"));
        int tmplIdx = pool.Intern("HTTP {Method} {Route} responded {Status} in {Elapsed} ms");

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
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
                // Real OTLP / Serilog-with-tracing traffic carries a distinct trace and
                // span id on every single event.
                TraceIdHi                = withTraceIds ? (ulong)rng.NextInt64() : 0,
                TraceIdLo                = withTraceIds ? (ulong)rng.NextInt64() : 0,
                SpanId                   = withTraceIds ? (ulong)rng.NextInt64() : 0,
            };
            if (!hot.TryWrite(h, buf.WrittenSpan, tmpl))
                break; // tier full — this is exactly where StorageEngine triggers a flush
        }
        return hot;
    }
}
