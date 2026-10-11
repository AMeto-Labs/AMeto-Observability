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
/// Round-4 review L4: the log receivers' own await of the commit.
///
/// <para>A receiver's handler is boxed at its first real suspension. When its body was read whole
/// without suspending, that is its await of the commit — after the write began — and a box that
/// could not be allocated there threw while the commit ran on: a 500 (gRPC UNKNOWN) for a batch the
/// store went on to write, and every finally of the handler skipped, the batch's dispose among
/// them — its reservation against the batches held decoded at once held for good. The handlers wait
/// the commit out now (allocating nothing), answer from it, and give the batch back themselves.</para>
///
/// <para>Driven over plain contexts, the bodies read from memory: the handler has not suspended
/// before the commit. The store's first write faults, so the commit waits; OwnerAsync's seam fails
/// the handler's first suspension as the runtime's box allocation would.</para>
/// </summary>
public sealed class ReceiverCommitBoxTests : IClassFixture<AmetoWebAppFactory>
{
    private static readonly OtlpGzipTooLargeLog NoLog       = new(NullLogger.Instance, TimeProvider.System);
    private static readonly OtlpOutOfMemoryLog  NoMemoryLog = new(NullLogger.Instance, TimeProvider.System);

    private readonly AmetoWebAppFactory _factory;

    public ReceiverCommitBoxTests(AmetoWebAppFactory factory)
    {
        _factory = factory;
        using var seeded = factory.CreateClient();   // creating a client is what seeds the test API key
    }

    [Fact]
    public async Task An_otlp_http_receiver_whose_await_of_the_commit_cannot_suspend_answers_200_and_gives_the_batch_back()
    {
        var logs = ((RouteEndpoint)_factory.Services.GetRequiredService<EndpointDataSource>().Endpoints
                       .Single(e => e is RouteEndpoint r && r.RoutePattern.RawText == "/v1/logs")).RequestDelegate!;
        var ctx  = Context("application/json", Encoding.UTF8.GetBytes(
            "{\"resourceLogs\":[{\"scopeLogs\":[{\"logRecords\":[" +
            "{\"body\":{\"stringValue\":\"box probe 1\"}},{\"body\":{\"stringValue\":\"box probe 2\"}}]}]}]}"));

        await RunWithTheHandlerUnboxedAsync("MapOtlpEndpoints", () => logs(ctx));

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        Assert.Equal(0, _factory.Services.GetRequiredService<IngestionEndpoint>().DecodedBytesInFlight);
    }

    [Fact]
    public async Task An_otlp_grpc_receiver_whose_await_of_the_commit_cannot_suspend_answers_ok_and_gives_the_batch_back()
    {
        var ctx = Context("application/grpc", Frame(LogsRequest(2)));
        ctx.Request.Protocol = "HTTP/2";

        await RunWithTheHandlerUnboxedAsync("<HandleCoreAsync>",
            () => OtlpGrpcEndpointMapper.HandleLogsAsync(ctx, new OtlpInflateGate(1, TimeSpan.Zero), NoLog, NoMemoryLog));

        Assert.Equal("0", ctx.Response.Headers["grpc-status"].ToString());
        Assert.Equal(0, _factory.Services.GetRequiredService<IngestionEndpoint>().DecodedBytesInFlight);
    }

    /// <summary>
    /// Runs a handler with the store's first write faulting (so its commit waits) and the handler's
    /// first suspension — its await of that commit — failing where its state machine is boxed.
    /// </summary>
    private async Task RunWithTheHandlerUnboxedAsync(string handler, Func<Task> run)
    {
        var storage = _factory.Services.GetRequiredService<StorageEngine>();
        int failing = 1, armed = 1, fired = 0;
        storage._afterWriteGate = () =>
        {
            if (Volatile.Read(ref failing) == 1) throw new OutOfMemoryException("test: a fault, so the write waits");
        };
        OwnerAsync.FirstSuspensionFailsForTest.Value = t =>
        {
            if (!t.Name.Contains(handler, StringComparison.Ordinal) || Interlocked.Exchange(ref armed, 0) == 0) return false;
            Volatile.Write(ref failing, 0);   // the write's next attempt lands
            Interlocked.Increment(ref fired);
            return true;
        };
        try { await run(); }
        finally
        {
            OwnerAsync.FirstSuspensionFailsForTest.Value = null;
            storage._afterWriteGate = null;
        }
        Assert.Equal(1, Volatile.Read(ref fired));   // the handler's first suspension was its await of the commit
    }

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

    private static byte[] Frame(byte[] message)
    {
        var framed = new byte[OtlpGrpcFraming.HeaderBytes + message.Length];
        framed[0] = 0;   // identity
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

    private static byte[] LogsRequest(int records) => Msg(c =>
        Sub(c, 1, Msg(rl => Sub(rl, 2, Msg(sl =>
        {
            for (int i = 0; i < records; i++)
            {
                int ii = i;
                Sub(sl, 2, Msg(lr =>
                {
                    lr.WriteTag(2, WireFormat.WireType.Varint); lr.WriteEnum(9);
                    Sub(lr, 5, Msg(b =>
                    {
                        b.WriteTag(1, WireFormat.WireType.LengthDelimited);
                        b.WriteString($"grpc box probe {ii}");
                    }));
                }));
            }
        })))));
}
