using System.Diagnostics;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT ONE <c>GET /api/traces/{id}</c> AND ONE FLAME GRAPH COST, for a 2 000-span trace of ordinary
/// eight-attribute SqlClient spans — the request the span pane and the flame graph tab make every
/// time somebody opens a trace.
///
/// <para>Lives in Ameto.Storage.Tests, not in tests/Ameto.Perf where the plan put it: Ameto.Perf
/// does not reference Ameto.Tracing and nobody edits its project file this round (plan, "Ownership
/// rules"). It drives the handler itself — <c>TraceQueryEndpointMapper.WriteTraceDetailAsync</c> —
/// over a <c>DefaultHttpContext</c> whose body is a counting sink, against a real engine holding the
/// trace in its hot tier.</para>
///
/// <para>PER-THREAD COUNTERS, and they are exact here: a hot-tier trace is served without a
/// yielding await (no cold segment, so no fan-out onto the pool), and the sink completes every
/// write synchronously, so the whole request runs on the calling thread — which each measurement
/// asserts rather than assumes. Every figure is the best of <see cref="Passes"/>, taken after a
/// warm-up long enough for the tiered JIT to settle, and nothing is printed until all of them are
/// in: xUnit drains test output on a thread of its own.</para>
///
/// <para>THREE ARMS, so the endpoint's own share can be read apart from the engine's. The engine
/// arm enumerates <c>GetTraceAsync</c> and writes nothing — what the provider costs whoever calls
/// it. The detail and flame graph arms are the handlers end to end; their difference from the
/// engine arm is what the endpoint itself costs, which is the part this probe exists to watch.</para>
/// </summary>
public sealed class TraceDetailAllocProbe : IDisposable
{
    private const int Spans   = 2_000;
    private const int Warmups = 40;
    private const int Passes  = 7;

    private readonly ITestOutputHelper _out;
    private readonly string            _dir;

    public TraceDetailAllocProbe(ITestOutputHelper output)
    {
        _out = output;
        _dir = Path.Combine(Path.GetTempPath(), "ameto-detailprobe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, true); } catch { }
    }

    private static readonly TraceId Trace = new(0x5DE7A11000000001UL, 0x0000000000002000UL);

    /// <summary>
    /// A four-way tree, so the flame graph has real structure (seven levels) and stays well under
    /// the depth at which today's serialiser refuses it.
    /// </summary>
    private static void WriteTrace(TraceStorageEngine engine)
    {
        long baseNano = new DateTimeOffset(2026, 9, 1, 12, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds() * 1_000_000L;
        for (int i = 0; i < Spans; i++)
            engine.WriteSpan(new SpanIngestItem
            {
                TraceId           = Trace,
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = i == 0 ? default : new SpanId((ulong)((i - 1) / 4 + 1)),
                StartTimeUnixNano = baseNano + i * 10_000L,
                DurationNanos     = 1_000_000L * (1 + i % 2000) + 123_456,
                Name              = "SELECT payments",
                ServiceName       = "billing",
                Kind              = i == 0 ? SpanKind.Server : SpanKind.Client,
                Status            = i % 97 == 0 ? SpanStatusCode.Error : SpanStatusCode.Unset,
                HttpStatusCode    = i == 0 ? (short)200 : (short)0,
                AttributesBytes   = TraceHotTierProbe.SqlClientBlob(i),
            });
    }

    /// <summary>A response body that keeps nothing and completes every write synchronously.</summary>
    private sealed class CountingSink : Stream
    {
        public long Written;
        public override bool CanRead  => false;
        public override bool CanSeek  => false;
        public override bool CanWrite => true;
        public override long Length   => Written;
        public override long Position { get => Written; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override Task FlushAsync(CancellationToken ct) => Task.CompletedTask;
        public override int  Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => Written += count;
        public override void Write(ReadOnlySpan<byte> buffer) => Written += buffer.Length;
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
        {
            Written += buffer.Length;
            return ValueTask.CompletedTask;
        }
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            Written += count;
            return Task.CompletedTask;
        }
    }

    private readonly record struct Reading(long Bytes, double Ms, long BodyBytes);

    private delegate Task Handler(HttpContext ctx, string traceId);

    private static async Task<Reading> MeasureHandlerAsync(IServiceProvider services, Handler handler, string id)
    {
        var sink = new CountingSink();
        var ctx  = new DefaultHttpContext { RequestServices = services };
        ctx.Response.Body = sink;

        int  thread = Environment.CurrentManagedThreadId;
        long a0     = GC.GetAllocatedBytesForCurrentThread();
        long t0     = Stopwatch.GetTimestamp();
        await handler(ctx, id);
        long t1     = Stopwatch.GetTimestamp();
        long a1     = GC.GetAllocatedBytesForCurrentThread();

        Assert.True(thread == Environment.CurrentManagedThreadId,
            "the handler hopped threads — the per-thread counter no longer covers it");
        Assert.Equal(200, ctx.Response.StatusCode);
        return new Reading(a1 - a0, Stopwatch.GetElapsedTime(t0, t1).TotalMilliseconds, sink.Written);
    }

    private static async Task<Reading> MeasureEngineAsync(TraceStorageEngine engine)
    {
        int  thread = Environment.CurrentManagedThreadId;
        int  n      = 0;
        long a0     = GC.GetAllocatedBytesForCurrentThread();
        long t0     = Stopwatch.GetTimestamp();
        await foreach (var _ in engine.GetTraceAsync(Trace)) n++;
        long t1     = Stopwatch.GetTimestamp();
        long a1     = GC.GetAllocatedBytesForCurrentThread();

        Assert.True(thread == Environment.CurrentManagedThreadId,
            "the enumeration hopped threads — the per-thread counter no longer covers it");
        Assert.Equal(Spans, n);
        return new Reading(a1 - a0, Stopwatch.GetElapsedTime(t0, t1).TotalMilliseconds, 0);
    }

    private static Reading Best(Reading a, Reading b) =>
        new(Math.Min(a.Bytes, b.Bytes), Math.Min(a.Ms, b.Ms), Math.Max(a.BodyBytes, b.BodyBytes));

    [Fact]
    public async Task Trace_detail_and_flamegraph_of_a_2000_span_trace()
    {
        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        WriteTrace(engine);

        using var services = new ServiceCollection()
            .AddSingleton<ITraceProvider>(engine)
            .BuildServiceProvider();
        string id = Trace.ToString();

        for (int i = 0; i < Warmups; i++)
        {
            await MeasureEngineAsync(engine);
            await MeasureHandlerAsync(services, TraceQueryEndpointMapper.WriteTraceDetailAsync, id);
            await MeasureHandlerAsync(services, TraceQueryEndpointMapper.WriteFlamegraphAsync, id);
        }

        Reading eng = new(long.MaxValue, double.MaxValue, 0), det = eng, flame = eng;
        for (int p = 0; p < Passes; p++)
        {
            eng   = Best(eng,   await MeasureEngineAsync(engine));
            det   = Best(det,   await MeasureHandlerAsync(services, TraceQueryEndpointMapper.WriteTraceDetailAsync, id));
            flame = Best(flame, await MeasureHandlerAsync(services, TraceQueryEndpointMapper.WriteFlamegraphAsync, id));
        }

        long detOwn   = det.Bytes   - eng.Bytes;
        long flameOwn = flame.Bytes - eng.Bytes;

        _out.WriteLine($"TRACE DETAIL + FLAME GRAPH, one {Spans:N0}-span hot-tier trace, 8 SqlClient attributes/span "
                     + $"(best of {Passes}, per-thread)");
        _out.WriteLine($"  engine GetTraceAsync only   {eng.Bytes,12:N0} B  {eng.Bytes / Spans,7:N0} B/span  {eng.Ms,8:N2} ms");
        _out.WriteLine($"  GET /api/traces/{{id}}        {det.Bytes,12:N0} B  {det.Bytes / Spans,7:N0} B/span  {det.Ms,8:N2} ms  body {det.BodyBytes:N0} B");
        _out.WriteLine($"    of which the endpoint      {detOwn,12:N0} B  {detOwn / Spans,7:N0} B/span");
        _out.WriteLine($"  GET .../flamegraph          {flame.Bytes,12:N0} B  {flame.Bytes / Spans,7:N0} B/span  {flame.Ms,8:N2} ms  body {flame.BodyBytes:N0} B");
        _out.WriteLine($"    of which the endpoint      {flameOwn,12:N0} B  {flameOwn / Spans,7:N0} B/span");

        // THE GATE ON THE DETAIL. Before TS#11 the endpoint's own share was 978-1 074 B per span
        // (1 957 336-2 149 336 B per request, Release): a SpanDto, three id strings, a
        // Dictionary<string,string> and eight value strings per span, the list holding them all,
        // and the reflection serialiser. Written from the record it is a writer and a context,
        // ~1.2 KB per REQUEST. The gate is 16 B per span — far above that, far below the defect.
        Assert.True(detOwn < Spans * 16,
            $"GET /api/traces/{{id}} allocated {detOwn:N0} B of its own for {Spans:N0} spans — the "
            + "detail is building per-span objects again (a DTO, a dictionary, value strings)");

        // THE GATE ON THE FLAME GRAPH. Before TS#11: 495-519 B per span of its own (991 120-1 039 120
        // B per request) — two dictionaries, a List per span, LINQ per node, an id string and two
        // boxed enums. What must stay is what the response is made of: a FlamegraphNode (80 B), its
        // id string (56 B), children arrays and the span list the builder reads — 168 B per span.
        Assert.True(flameOwn < Spans * 256,
            $"GET .../flamegraph allocated {flameOwn:N0} B of its own for {Spans:N0} spans — the builder "
            + "is indexing the trace through dictionaries and per-span lists again");
    }

    private static long LiveBytes()
    {
        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        return GC.GetTotalMemory(forceFullCollection: false);
    }

    /// <summary>
    /// ONE LOOK AT A TRACE MUST NOT MAKE THE HOT TIER HEAVIER. <see cref="SpanRecord.Attributes"/>
    /// decodes a hot-tier record's blob on first touch and MEMOISES the dictionary on the record,
    /// and the records belong to the tier — so a detail view that reaches the attributes through
    /// it leaves every span of the trace carrying a decoded map (a Dictionary, a string per key, a
    /// box per value) until the tier flushes: the shape <c>TraceQlScanProbe.A_traceql_page_does_
    /// not_inflate_the_hot_tier_it_paged_over</c> closed for TraceQL rows.
    ///
    /// <para>Measured the way that probe measures it: the tier built, the handler warmed on ANOTHER
    /// trace of the same engine, then live bytes with a compacting gen2 collect on both sides of one
    /// request, so the delta is what the request left behind.</para>
    /// </summary>
    [Fact]
    public async Task A_trace_detail_does_not_inflate_the_hot_tier_it_read()
    {
        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        WriteTrace(engine);

        var warmTrace = new TraceId(0x5DE7A11000000002UL, 1);
        engine.WriteSpan(new SpanIngestItem
        {
            TraceId = warmTrace, SpanId = new SpanId(1), StartTimeUnixNano = 1, DurationNanos = 1,
            Name = "warm", ServiceName = "warm", AttributesBytes = TraceHotTierProbe.SqlClientBlob(0),
        });

        using var services = new ServiceCollection()
            .AddSingleton<ITraceProvider>(engine)
            .BuildServiceProvider();

        for (int i = 0; i < 3; i++)
            await MeasureHandlerAsync(services, TraceQueryEndpointMapper.WriteTraceDetailAsync, warmTrace.ToString());

        long before = LiveBytes();
        var  r      = await MeasureHandlerAsync(services, TraceQueryEndpointMapper.WriteTraceDetailAsync, Trace.ToString());
        long after  = LiveBytes();

        long perSpan = (after - before) / Spans;
        _out.WriteLine($"ONE GET /api/traces/{{id}} over a {Spans:N0}-span hot-tier trace: body {r.BodyBytes:N0} B, "
                     + $"left on the tier {after - before:N0} B = {perSpan:N0} B/span");

        // Before TS#11: 1 484-1 489 B per span left on the tier — the memoised decode of every span
        // of the trace, ~3 MB for one look at a 2 000-span trace, held until the tier flushed.
        Assert.True(perSpan < 256,
            $"one trace-detail request left {perSpan:N0} B per span on the hot tier — the handler is "
            + "reading attributes through SpanRecord.Attributes, which memoises its decode on the record");
        GC.KeepAlive(engine);
    }
}
