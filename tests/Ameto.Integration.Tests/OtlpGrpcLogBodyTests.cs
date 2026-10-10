using System.Buffers.Binary;
using Ameto.Core;
using Ameto.Otel;
using Ameto.Storage;
using Google.Protobuf;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Integration.Tests;

/// <summary>
/// A log batch over OTLP/gRPC is written into the store, and the call answered, only once its
/// records are in a WAL — a write that may wait for room up to <c>Ingestion.BackPressureWait</c>.
/// It waits without its request body: the batch holds its own copy of everything it keeps, and the
/// body of an identity frame IS the message, which the request used to hold until its outer finally,
/// its message in memory twice for the whole wait. Driven over a plain context, as the other gRPC
/// tests are; the store's first write is held so the test can look while the commit waits.
/// </summary>
public sealed class OtlpGrpcLogBodyTests : IClassFixture<AmetoWebAppFactory>
{
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(60);

    private static readonly OtlpGzipTooLargeLog NoLog       = new(NullLogger.Instance, TimeProvider.System);
    private static readonly OtlpOutOfMemoryLog  NoMemoryLog = new(NullLogger.Instance, TimeProvider.System);

    private readonly AmetoWebAppFactory _factory;

    public OtlpGrpcLogBodyTests(AmetoWebAppFactory factory)
    {
        _factory = factory;
        using var seeded = factory.CreateClient();   // creating a client is what seeds the test API key
    }

    [Fact]
    public async Task An_identity_framed_log_batch_gives_its_body_back_before_it_waits_for_the_store()
    {
        var storage = _factory.Services.GetRequiredService<StorageEngine>();
        var gate    = new OtlpInflateGate(1, TimeSpan.Zero);
        using var writing = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        int writes = 0;
        storage._afterWriteGate = () =>
        {
            if (Interlocked.Increment(ref writes) != 1) return;
            writing.Set();
            release.Wait(HangGuard);
        };
        try
        {
            using var ledger = IngestBufferPoolLedger.Open();
            var call = GrpcCall(Frame(LogsRequest(3)));

            // On its own thread: the held write blocks the thread that commits. The ledger, an
            // AsyncLocal, flows into it.
            var handling = Task.Run(() => OtlpGrpcEndpointMapper.HandleLogsAsync(call, gate, NoLog, NoMemoryLog));
            Assert.True(writing.Wait(HangGuard), "the batch never reached the store");

            Assert.True(ledger.Rents >= 2, "the request body and the batch's payloads did not both come from IngestBufferPool");
            // Held while the store is written: the batch's own copy of its records, and nothing of
            // the request. The body used to be the second.
            Assert.Equal(1, ledger.Outstanding);

            release.Set();
            await handling.WaitAsync(HangGuard);
            Assert.Equal("0", call.Response.Headers["grpc-status"].ToString());
        }
        finally
        {
            storage._afterWriteGate = null;
            release.Set();
        }
    }

    private static byte[] Frame(byte[] message)
    {
        var framed = new byte[OtlpGrpcFraming.HeaderBytes + message.Length];
        framed[0] = 0;   // identity
        BinaryPrimitives.WriteUInt32BigEndian(framed.AsSpan(1), (uint)message.Length);
        message.CopyTo(framed.AsSpan(OtlpGrpcFraming.HeaderBytes));
        return framed;
    }

    private DefaultHttpContext GrpcCall(byte[] framed)
    {
        var ctx = new DefaultHttpContext { RequestServices = _factory.Services };
        ctx.Request.Method        = "POST";
        ctx.Request.Protocol      = "HTTP/2";
        ctx.Request.ContentType   = "application/grpc";
        ctx.Request.ContentLength = framed.Length;
        ctx.Request.Body          = new MemoryStream(framed);
        ctx.Request.Headers["X-Seq-ApiKey"] = AmetoWebAppFactory.TestApiKey;
        ctx.Response.Body = new MemoryStream();
        return ctx;
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
                    lr.WriteTag(1, WireFormat.WireType.Fixed64);
                    lr.WriteFixed64(1_785_300_060_000_000_000UL + (ulong)ii);
                    lr.WriteTag(2, WireFormat.WireType.Varint); lr.WriteEnum(9);
                    Sub(lr, 5, Msg(b =>
                    {
                        b.WriteTag(1, WireFormat.WireType.LengthDelimited);
                        b.WriteString($"grpc body probe {ii}");
                    }));
                }));
            }
        })))));
}
