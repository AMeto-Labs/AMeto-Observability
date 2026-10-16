using System.Buffers.Binary;
using System.Text;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Otel;
using Ameto.Storage;
using Google.Protobuf;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Integration.Tests;

/// <summary>
/// Nothing below a receiver waits out, on the request's thread, what an await of its own could not wait
/// for (round-6 review R5-M4: M-D's class one frame down). Such a wait-out held a pool thread that what it
/// waited for needed — on two cores a burst of them stalled the whole pool for seconds — and past its bound
/// it gave up on something still running.
///
/// <para><b>The commit</b> (R5-M4). An OTLP logs receiver awaits its batch's write itself, on the state
/// machine it boxed before the batch was begun; the CLEF receiver's case is IngestStoreBusyTests'. The commit
/// stood between the two: an async method whose first real suspension, where its state machine is boxed,
/// was its await of the write — after the write began. A box that could not be allocated threw there with the
/// write going on; the commit waited it out for the write's wait and a second more, and then let the failure
/// go up: OTLP/HTTP answered 500 and the gRPC call failed, while the write went on and stored the batch the
/// exporter was told had failed. Driven against a host whose BackPressureWait is 100 ms: every first
/// suspension of a frame of the ingest endpoint below the receivers fails, as the runtime's box allocation
/// would; the store faults once, so the write waits, and its next attempt lands only when the test says — two
/// seconds on, past the write's wait and the second a commit used to wait it out for.</para>
///
/// <para><b>The inflate gate's wait</b> (R5-M12, and the LOW on its guard). A receiver whose await of a pending
/// wait for a slot throws abandons the wait's claim (OtlpInflateGate.Claim) rather than waiting the wait out,
/// and the slot the wait takes later goes back.</para>
/// </summary>
public sealed class ReceiverWaitOutTests : IClassFixture<ReceiverWaitOutTests.Factory>
{
    /// <summary>
    /// A host whose writes wait 100 ms for room, so the commit's old wait-out gave up after about a second; and
    /// whose inflate gate has one slot and five seconds of patience, so a wait for it that a request waited out
    /// held its thread that long.
    /// </summary>
    public sealed class Factory : AmetoWebAppFactory
    {
        protected override IngestionOptions ConfiguredIngestion => new() { BackPressureWait = TimeSpan.FromMilliseconds(100) };

        internal OtlpInflateGate Gate { get; } = new(1, TimeSpan.FromSeconds(5));

        protected override void ConfigureWebHost(Microsoft.AspNetCore.Hosting.IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            builder.ConfigureServices(services => services.AddSingleton(Gate));
        }
    }

    private static readonly OtlpGzipTooLargeLog NoLog       = new(NullLogger.Instance, TimeProvider.System);
    private static readonly OtlpOutOfMemoryLog  NoMemoryLog = new(NullLogger.Instance, TimeProvider.System);

    private readonly Factory _factory;

    public ReceiverWaitOutTests(Factory factory)
    {
        _factory = factory;
        using var seeded = factory.CreateClient();   // creating a client is what seeds the test API key
    }

    [Theory]
    [InlineData("otlp-http")]
    [InlineData("otlp-grpc")]
    public async Task An_otlp_receiver_whose_write_outlives_its_wait_is_answered_from_what_the_write_did(string receiver)
    {
        string template = "commit probe " + receiver + " " + Guid.NewGuid().ToString("N");
        var storage  = _factory.Services.GetRequiredService<StorageEngine>();
        var endpoint = _factory.Services.GetRequiredService<IngestionEndpoint>();
        var previous = storage.EventWritten;
        int written = 0, attempts = 0, failed = 0;
        using var release = new ManualResetEventSlim(false);
        storage.EventWritten = (header, t) =>
        {
            previous?.Invoke(header, t);
            if (t == template) Interlocked.Increment(ref written);
        };
        storage._afterWriteGate = () =>
        {
            int n = Interlocked.Increment(ref attempts);
            if (n == 1) throw new OutOfMemoryException("test: a fault, so the write waits");
            if (n == 2) release.Wait(TimeSpan.FromSeconds(30));   // the next attempt lands only when the test says
        };
        long acceptedBefore = endpoint.AcceptedTotal;
        try
        {
            bool grpc = receiver == "otlp-grpc";
            var  ctx  = grpc
                ? Context("application/grpc", Frame(LogsProto(template, 3)))
                : Context("application/json", Encoding.UTF8.GetBytes(LogsJson(template, 3)));
            if (grpc) ctx.Request.Protocol = "HTTP/2";

            Task handled;
            OwnerAsync.FirstSuspensionFailsForTest.Value = t =>
            {
                if (!IngestStoreBusyTests.BelowTheHandler(t)) return false;
                Interlocked.Increment(ref failed);
                return true;
            };
            try
            {
                handled = grpc
                    ? OtlpGrpcEndpointMapper.HandleLogsAsync(ctx, new OtlpInflateGate(1, TimeSpan.Zero), NoLog, NoMemoryLog)
                    : Route("/v1/logs")(ctx);
            }
            finally { OwnerAsync.FirstSuspensionFailsForTest.Value = null; }

            bool   endedEarly = await Task.WhenAny(handled, Task.Delay(2_000)) == handled;
            string how        = endedEarly ? Ended(handled, ctx, grpc) : "";
            release.Set();
            try { await handled.WaitAsync(TimeSpan.FromSeconds(30)); }
            catch (OutOfMemoryException) when (endedEarly) { /* told below */ }
            var until = DateTime.UtcNow.AddSeconds(30);
            while (Volatile.Read(ref written) < 3 && DateTime.UtcNow < until) await Task.Delay(10);
            await Task.Delay(100);   // nothing more comes

            int stored = Volatile.Read(ref written);
            Assert.True(!endedEarly, $"the request ended ({how}) while its write was still going, and the write then stored {stored} event(s) of a batch the exporter was told had failed");
            if (grpc) Assert.Equal("0", ctx.Response.Headers["grpc-status"].ToString());   // OK
            else      Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
            Assert.Equal(3, stored);
            Assert.Equal(3, endpoint.AcceptedTotal - acceptedBefore);
            Assert.True(Volatile.Read(ref failed) == 0, "a frame of the ingest endpoint was boxed after its write began");
        }
        finally
        {
            release.Set();
            storage._afterWriteGate = null;
            storage.EventWritten    = previous;
        }
    }

    // ── The inflate gate's wait (R5-M12) ──────────────────────────────────────

    /// <summary>
    /// Round-6 review R5-M12. The body read's await of its wait for a slot is its first real suspension when the
    /// body was read whole without suspending; a box that could not be allocated there threw with the wait
    /// pending, and round 5 waited that wait out on the request's thread — a pool thread the wait's own doorbell
    /// and timer needed — until it ended: here the gate's five seconds of patience, with every slot taken
    /// (concurrent failures stalled the whole pool for seconds, 6.6 s at sixteen in the review's P12b). The
    /// wait's claim is abandoned instead: the request is answered at once, and the slot the wait takes when one
    /// comes free goes back to the gate.
    /// </summary>
    [Fact]
    public async Task A_body_read_whose_wait_for_a_slot_cannot_be_boxed_is_answered_at_once_and_loses_no_slot()
    {
        var gate = _factory.Gate;
        Assert.True(await gate.TryEnterAsync(default));   // the one slot, taken
        var ctx = Context("application/json", OtlpGzipTests.Gzip(Encoding.UTF8.GetBytes(LogsJson("gate probe", 1))));
        ctx.Request.Headers.ContentEncoding = "gzip";

        int fired = 0;
        var sinceFailure = new System.Diagnostics.Stopwatch();
        Task handled;
        OwnerAsync.FirstSuspensionFailsForTest.Value = t =>
        {
            if (!t.Name.Contains("<ReadBodyAsync>", StringComparison.Ordinal) || Interlocked.Exchange(ref fired, 1) == 1) return false;
            sinceFailure.Start();
            return true;
        };
        try { handled = Route("/v1/logs")(ctx); }
        finally { OwnerAsync.FirstSuspensionFailsForTest.Value = null; }
        long heldMs;
        try
        {
            await handled.WaitAsync(TimeSpan.FromSeconds(30));
            heldMs = sinceFailure.ElapsedMilliseconds;
        }
        finally { gate.Exit(); }                           // the slot comes free: a wait still pending takes it...
        await Task.Delay(300);                             // ...and gives it back, if it was abandoned

        Assert.Equal(1, Volatile.Read(ref fired));         // setup: the read's wait for a slot was its first suspension
        Assert.True(heldMs < 2_000, $"the request was answered {heldMs} ms after its read's wait for a slot could not be boxed: its thread waited the wait out");
        Assert.Equal(StatusCodes.Status503ServiceUnavailable, ctx.Response.StatusCode);
        Assert.True(gate.Available == 1, "a slot the gate freed was taken by a wait the failed read had left behind");
    }

    /// <summary>
    /// Round-6 review, the LOW on R5-M12's guard. The gRPC handler's await of its wait for a slot is not its first
    /// suspension (the yield before it is), but registering the continuation can still throw with the wait pending
    /// — the TPL event source's wrapper, a wait that completed meanwhile queued to a pool queue that must grow. Its
    /// catch answered UNAVAILABLE with no guard at all, and the wait went on to take the next freed slot for
    /// nobody. The claim gives it back. The gate's seam throws as that registration would, once the wait began.
    /// </summary>
    [Fact]
    public async Task A_grpc_receiver_whose_await_of_a_wait_for_a_slot_throws_leaves_no_wait_to_take_a_freed_slot()
    {
        var gate = new OtlpInflateGate(1, TimeSpan.FromSeconds(5));
        Assert.True(await gate.TryEnterAsync(default));   // the one slot, taken
        int fired = 0;
        gate.AfterWaitBegunForTest = () =>
        {
            Interlocked.Increment(ref fired);
            throw new OutOfMemoryException("test: the continuation of a pending wait could not be registered");
        };
        var ctx = Context("application/grpc", Frame(OtlpGzipTests.Gzip(LogsProto("gate probe", 1)), compressed: true));
        ctx.Request.Protocol = "HTTP/2";
        ctx.Request.Headers["grpc-encoding"] = "gzip";

        try { await OtlpGrpcEndpointMapper.HandleLogsAsync(ctx, gate, NoLog, NoMemoryLog).WaitAsync(TimeSpan.FromSeconds(30)); }
        finally { gate.AfterWaitBegunForTest = null; }

        Assert.Equal(1, Volatile.Read(ref fired));         // setup: the handler's wait for a slot began, and its await threw
        Assert.Equal("14", ctx.Response.Headers["grpc-status"].ToString());   // UNAVAILABLE, which exporters retry
        gate.Exit();                                       // the slot comes free...
        await Task.Delay(300);                             // ...and a wait left behind would have it by now
        Assert.True(gate.Available == 1, "a slot the gate freed was taken by a wait the gRPC handler had left behind");
    }

    /// <summary>How a request that has already ended ended: faulted, or the answer it gave.</summary>
    private static string Ended(Task handled, HttpContext ctx, bool grpc) =>
        handled.IsFaulted ? "faulted: " + handled.Exception!.InnerException!.GetType().Name
        : grpc            ? "grpc-status " + ctx.Response.Headers["grpc-status"]
        :                   "HTTP " + ctx.Response.StatusCode;

    private RequestDelegate Route(string pattern) =>
        ((RouteEndpoint)_factory.Services.GetRequiredService<EndpointDataSource>().Endpoints
            .Single(e => e is RouteEndpoint r && r.RoutePattern.RawText == pattern)).RequestDelegate!;

    private DefaultHttpContext Context(string contentType, byte[] body)
    {
        var ctx = new DefaultHttpContext { RequestServices = _factory.Services };
        ctx.Request.Method        = "POST";
        ctx.Request.ContentType   = contentType;
        ctx.Request.ContentLength = body.Length;
        ctx.Request.Body          = new MemoryStream(body);
        ctx.Request.Headers["X-Seq-ApiKey"] = AmetoWebAppFactory.TestApiKey;
        ctx.Response.Body = new MemoryStream();
        return ctx;
    }

    private static string LogsJson(string body, int records)
    {
        var sb = new StringBuilder("{\"resourceLogs\":[{\"scopeLogs\":[{\"logRecords\":[");
        for (int i = 0; i < records; i++)
            sb.Append(i == 0 ? "" : ",").Append("{\"body\":{\"stringValue\":\"").Append(body).Append("\"}}");
        return sb.Append("]}]}]}").ToString();
    }

    private static byte[] Frame(byte[] message, bool compressed = false)
    {
        var framed = new byte[OtlpGrpcFraming.HeaderBytes + message.Length];
        framed[0] = compressed ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteUInt32BigEndian(framed.AsSpan(1), (uint)message.Length);
        message.CopyTo(framed.AsSpan(OtlpGrpcFraming.HeaderBytes));
        return framed;
    }

    private static byte[] Msg(Action<CodedOutputStream> body)
    {
        using var ms = new MemoryStream();
        var o = new CodedOutputStream(ms);
        body(o);
        o.Flush();
        return ms.ToArray();
    }

    private static void Sub(CodedOutputStream o, int field, byte[] payload)
    {
        o.WriteTag(field, WireFormat.WireType.LengthDelimited);
        o.WriteBytes(ByteString.CopyFrom(payload));
    }

    private static byte[] LogsProto(string body, int records) => Msg(c =>
        Sub(c, 1, Msg(rl => Sub(rl, 2, Msg(sl =>
        {
            for (int i = 0; i < records; i++)
                Sub(sl, 2, Msg(lr =>
                {
                    lr.WriteTag(2, WireFormat.WireType.Varint); lr.WriteEnum(9);
                    Sub(lr, 5, Msg(b =>
                    {
                        b.WriteTag(1, WireFormat.WireType.LengthDelimited);
                        b.WriteString(body);
                    }));
                }));
        })))));
}
