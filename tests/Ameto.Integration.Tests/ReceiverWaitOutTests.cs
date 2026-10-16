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
/// </summary>
public sealed class ReceiverWaitOutTests : IClassFixture<ReceiverWaitOutTests.Factory>
{
    /// <summary>A host whose writes wait 100 ms for room, so the commit's old wait-out gave up after about a second.</summary>
    public sealed class Factory : AmetoWebAppFactory
    {
        protected override IngestionOptions ConfiguredIngestion => new() { BackPressureWait = TimeSpan.FromMilliseconds(100) };
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

    private static byte[] Frame(byte[] message)
    {
        var framed = new byte[OtlpGrpcFraming.HeaderBytes + message.Length];
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
