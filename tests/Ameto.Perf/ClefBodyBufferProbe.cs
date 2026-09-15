using System.Buffers;
using MessagePack;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// What one <c>/api/events</c> request allocates while sixteen of them carry 1.4 MB CLEF bodies
/// at once, measured through <see cref="IngestionEndpoint.HandleAsync"/> itself: with a
/// Content-Length (what Serilog.Sinks.Seq sends) and without one (the doubling read).
///
/// <para>The endpoint runs over a real ring and intern pool. Its drainer is disposed before the
/// first request, so nothing downstream of the ring is charged to a request; the probe empties the
/// ring itself between rounds, outside the measured window, and the ring is sized so a whole round
/// is accepted rather than dropped. Each of the sixteen slots reuses one DefaultHttpContext across
/// rounds, and the harness alone (a Task.Run per request, nothing else) is printed alongside so
/// its share can be read off. Bodies are MemoryStreams, so reads complete synchronously and the
/// handler's own async overhead does not appear; the body buffer is what this is about.</para>
///
/// <para>Printed, not asserted, for the reason <see cref="OtlpBodyBufferProbe"/> gives at length:
/// warm thread-pool threads are ArrayPool.Shared's best case (one array per thread on top of its
/// per-core stacks), and the misses the dedicated pool removes come from thread churn, depth and
/// Shared's own trimming, which a tight loop on a many-core box does not reproduce. What IS
/// asserted is that every request took the path being measured: 200, every event accepted.</para>
/// </summary>
public sealed class ClefBodyBufferProbe : IAsyncLifetime
{
    private const int Concurrency = 16;
    private const int Rounds      = 20;
    private const int BodyBytes   = 1_400_000;
    private const int SlabBytes   = 1024;       // these events carry ~100 B of properties
    private const int RingSlots   = 1 << 17;    // 16 bodies of ~5,400 events fit in one round

    private readonly ITestOutputHelper _out;
    private readonly byte[] _body;
    private readonly int    _eventsPerBody;
    private readonly byte[] _drainBuf = new byte[SlabBytes];
    private readonly Task[] _tasks    = new Task[Concurrency];

    private IngestionRingBuffer _ring     = null!;
    private IngestionEndpoint   _endpoint = null!;

    public ClefBodyBufferProbe(ITestOutputHelper o)
    {
        _out  = o;
        _body = BuildBatch(BodyBytes, out _eventsPerBody);
    }

    public async Task InitializeAsync()
    {
        var options = new ServerOptions { Ingestion = new IngestionOptions { MaxEventPayloadBytes = SlabBytes } };
        _ring = new IngestionRingBuffer(RingSlots, SlabBytes, (long)RingSlots * SlabBytes);

        // Storage is never reached: the drainer is disposed while the ring is still empty, so its
        // loop and its final drain both find nothing to write. NotifyEnqueued on it afterwards only
        // releases a semaphore nobody waits on.
        var drainer = new IngestionDrainer(_ring, storage: null!, options, NullLogger<IngestionDrainer>.Instance);
        await drainer.DisposeAsync();

        _endpoint = new IngestionEndpoint(_ring, new StringInternPool(), drainer, options,
                                          NullLogger<IngestionEndpoint>.Instance);
    }

    public Task DisposeAsync()
    {
        _ring.Dispose();
        return Task.CompletedTask;
    }

    [Fact]
    public async Task ConcurrentLargeBodies_AllocatedBytesPerRequest()
    {
        DefaultHttpContext[] withLength = Contexts(declareLength: true);
        DefaultHttpContext[] chunked    = Contexts(declareLength: false);

        long harnessFlat = await Measure(withLength, harnessOnly: true,  gen2: false);
        long lengthFlat  = await Measure(withLength, harnessOnly: false, gen2: false);
        long chunkedFlat = await Measure(chunked,    harnessOnly: false, gen2: false);
        long harnessGen2 = await Measure(withLength, harnessOnly: true,  gen2: true);
        long lengthGen2  = await Measure(withLength, harnessOnly: false, gen2: true);
        long chunkedGen2 = await Measure(chunked,    harnessOnly: false, gen2: true);

        _out.WriteLine($"{Concurrency} concurrent {_body.Length / 1024.0 / 1024.0:F2} MB CLEF bodies "
                     + $"({_eventsPerBody:N0} events each) x {Rounds} rounds = {Concurrency * Rounds} requests, "
                     + $"{Environment.ProcessorCount} cores; IngestBufferPool depth {IngestBufferPool.ArraysPerBucket}");
        _out.WriteLine("                                   bytes/request   bytes/request, gen2 before each round");
        _out.WriteLine($"  harness alone (Task.Run)       : {harnessFlat,13:N0}   {harnessGen2,13:N0}");
        _out.WriteLine($"  Content-Length body            : {lengthFlat,13:N0}   {lengthGen2,13:N0}");
        _out.WriteLine($"  chunked body (doubling read)   : {chunkedFlat,13:N0}   {chunkedGen2,13:N0}");
    }

    private DefaultHttpContext[] Contexts(bool declareLength)
    {
        var contexts = new DefaultHttpContext[Concurrency];
        for (int i = 0; i < Concurrency; i++)
        {
            var ctx = new DefaultHttpContext();
            ctx.Request.Method      = "POST";
            ctx.Request.ContentType = "application/octet-stream";
            if (declareLength) ctx.Request.ContentLength = _body.Length;
            ctx.Request.Body        = new MemoryStream(_body, writable: false);
            ctx.Response.Body       = new MemoryStream(256);
            contexts[i] = ctx;
        }
        return contexts;
    }

    /// <summary>Allocated bytes per request over <see cref="Rounds"/> measured rounds, after one unmeasured one.</summary>
    private async Task<long> Measure(DefaultHttpContext[] contexts, bool harnessOnly, bool gen2)
    {
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);

        // Unmeasured: what is compared is steady-state reuse, not whichever pool was filled last.
        Reset(contexts);
        await Round(contexts, harnessOnly);
        Settle(contexts, harnessOnly);

        long total = 0;
        for (int r = 0; r < Rounds; r++)
        {
            Reset(contexts);
            if (gen2) GC.Collect(2, GCCollectionMode.Forced, blocking: true);

            long before = GC.GetTotalAllocatedBytes(precise: true);
            await Round(contexts, harnessOnly);
            total += GC.GetTotalAllocatedBytes(precise: true) - before;

            Settle(contexts, harnessOnly);
        }
        return total / (Rounds * Concurrency);
    }

    private async Task Round(DefaultHttpContext[] contexts, bool harnessOnly)
    {
        for (int i = 0; i < Concurrency; i++)
        {
            DefaultHttpContext ctx = contexts[i];
            _tasks[i] = harnessOnly ? Task.Run(() => Nothing(ctx)) : Task.Run(() => _endpoint.HandleAsync(ctx));
        }
        await Task.WhenAll(_tasks);
    }

    private static Task Nothing(HttpContext ctx) => Task.CompletedTask;

    private static void Reset(DefaultHttpContext[] contexts)
    {
        foreach (var ctx in contexts)
        {
            ctx.Request.Body.Position = 0;
            ctx.Response.Body.SetLength(0);
            ctx.Response.StatusCode = 0;
        }
    }

    /// <summary>Outside the window: every request answered 200, and the ring gives back every event it took.</summary>
    private void Settle(DefaultHttpContext[] contexts, bool harnessOnly)
    {
        if (harnessOnly) return;

        foreach (var ctx in contexts)
            Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);

        long drained = 0;
        while (_ring.TryDequeue(out _, out _, out _, out _, out _, _drainBuf, out _, out _, out _, out _, out _))
            drained++;
        Assert.Equal((long)Concurrency * _eventsPerBody, drained);
    }

    /// <summary>
    /// At least <paramref name="minBytes"/> of events shaped like a real Serilog/Seq post (8 CLEF
    /// fields + 4 properties, as <see cref="ClefAllocProbe"/>), every event the same encoded size.
    /// </summary>
    private static byte[] BuildBatch(int minBytes, out int count)
    {
        int oneEvent = Encode(1).Length - 1;              // a one-element array header is one byte
        count = minBytes / oneEvent + 1;
        return Encode(count);
    }

    private static byte[] Encode(int count)
    {
        var buf = new ArrayBufferWriter<byte>(Math.Max(count, 1) * 320);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(count);

        var ts = new DateTimeOffset(2024, 3, 1, 10, 20, 30, TimeSpan.Zero);
        for (int i = 0; i < count; i++)
        {
            w.WriteMapHeader(10);
            w.Write("@t");           w.Write(ts.AddTicks(1 + i % 1000).UtcDateTime.ToString("O"));
            w.Write("@l");           w.Write((i % 7) == 0 ? "Warning" : "Information");
            w.Write("@mt");          w.Write("Order {OrderId} for {Customer} moved to {State} in {Region}");
            w.Write("@tr");          w.Write("4bf92f3577b34da6a3ce929d0e0e4736");
            w.Write("@sp");          w.Write("00f067aa0ba902b7");
            w.Write("service.name"); w.Write("checkout-api");
            w.Write("OrderId");      w.Write((long)(i % 100));
            w.Write("Customer");     w.Write("ACME GmbH (Frankfurt)");
            w.Write("State");        w.Write("AwaitingPayment");
            w.Write("Region");       w.Write("eu-central-1");
        }

        w.Flush();
        return buf.WrittenSpan.ToArray();
    }
}
