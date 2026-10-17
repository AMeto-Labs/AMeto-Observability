using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Storage;
using MessagePack;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace Ameto.Integration.Tests;

/// <summary>
/// Round-6 review R6-H2: the frames above a receiver. A receiver's first suspension was a yield, which queued it before
/// the frames above it suspended on it; a frame of theirs whose box or registration ran out of memory threw up to
/// Kestrel, which answered 500 and reused the request's HttpContext for the next request on the connection — while the
/// queued receiver ran on in it, reading the body, writing the batch after the 500, and writing its answer into the
/// next request's response. Each pipeline here is the production gate (IngestFramesGate.Run), a frame above the
/// receiver built with the owner builder — whose first suspension, on the receiver, the seam fails as a box that cannot
/// be allocated — and the receiver's own route.
/// </summary>
public sealed class ReceiverFramesAboveTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;

    public ReceiverFramesAboveTests(AmetoWebAppFactory factory)
    {
        _factory = factory;
        using var seeded = factory.CreateClient();   // creating a client is what seeds the test API key
    }

    public static TheoryData<string, string, string> Receivers => new()
    {
        { "/api/events", "application/octet-stream", "clef" },
        { "/v1/logs",    "application/json",
          "{\"resourceLogs\":[{\"scopeLogs\":[{\"logRecords\":[{\"body\":{\"stringValue\":\"frames above probe\"}}]}]}]}" },
    };

    /// <summary>The body of a test case: a MessagePack CLEF batch of three events for "clef", the text otherwise.</summary>
    private static byte[] Body(string body)
    {
        if (body != "clef") return Encoding.UTF8.GetBytes(body);
        var buf = new ArrayBufferWriter<byte>(256);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(3);
        for (int i = 0; i < 3; i++)
        {
            w.WriteMapHeader(2);
            w.Write("@mt"); w.Write("frames above probe {i}");
            w.Write("i");   w.Write((long)i);
        }
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    [Theory]
    [MemberData(nameof(Receivers))]
    public async Task A_receiver_whose_frame_above_fails_at_its_suspension_touches_nothing_of_the_request(
        string route, string contentType, string body)
    {
        var storage = _factory.Services.GetRequiredService<StorageEngine>();
        int writes = 0, armed = 1;
        storage._afterWriteGate = () => Interlocked.Increment(ref writes);
        var reads = new CountingStream(Body(body));
        var ctx   = Context(contentType, reads);
        Task? received = null;
        var pipeline = Pipeline(route, r => received = r);
        try
        {
            Task run;
            OwnerAsync.FirstSuspensionFailsForTest.Value = t =>
                t.Name.Contains("<FrameAbove>", StringComparison.Ordinal) && Interlocked.Exchange(ref armed, 0) == 1;
            try { run = pipeline(ctx); }
            finally { OwnerAsync.FirstSuspensionFailsForTest.Value = null; }

            await Assert.ThrowsAsync<OutOfMemoryException>(() => run);   // hosting answers it 500
            Assert.NotNull(received);
            await received!.WaitAsync(TimeSpan.FromSeconds(10));        // the receiver has ended, and before that:
            Assert.Equal(0, reads.Reads);                                // nothing of the body read,
            Assert.Equal(0, Volatile.Read(ref writes));                  // nothing written to the store,
            Assert.Equal(0, ctx.Response.Body.Length);                   // and nothing written into the response
        }
        finally { storage._afterWriteGate = null; }
    }

    /// <summary>With every frame suspended the gate opens, and the receiver does its work as before.</summary>
    [Theory]
    [MemberData(nameof(Receivers))]
    public async Task A_receiver_behind_the_gate_goes_on_once_the_frames_above_have_suspended(
        string route, string contentType, string body)
    {
        var storage = _factory.Services.GetRequiredService<StorageEngine>();
        int writes = 0;
        storage._afterWriteGate = () => Interlocked.Increment(ref writes);
        var ctx = Context(contentType, new CountingStream(Body(body)));
        try
        {
            await Pipeline(route, static _ => { })(ctx).WaitAsync(TimeSpan.FromSeconds(10));
            Assert.True(ctx.Response.StatusCode is StatusCodes.Status200OK or StatusCodes.Status201Created,
                        $"answered {ctx.Response.StatusCode}");
            Assert.True(Volatile.Read(ref writes) > 0, "the batch was not written");
        }
        finally { storage._afterWriteGate = null; }
    }

    private RequestDelegate Pipeline(string route, Action<Task> received)
    {
        var receiver = ((RouteEndpoint)_factory.Services.GetRequiredService<EndpointDataSource>().Endpoints
            .Single(e => e is RouteEndpoint r && r.RoutePattern.RawText == route && IsPost(r))).RequestDelegate!;
        var app = new ApplicationBuilder(_factory.Services);
        app.Use(static (ctx, next) => IngestFramesGate.Run(ctx, next));
        app.Use(FrameAbove);
        app.Run(ctx =>
        {
            var task = receiver(ctx);
            received(task);
            return task;
        });
        return app.Build();
    }

    private static bool IsPost(RouteEndpoint r) =>
        r.Metadata.GetMetadata<HttpMethodMetadata>() is not { } methods || methods.HttpMethods.Contains("POST");

    /// <summary>A frame above the receiver, as EndpointMiddleware's or the path base's is: it awaits what is below it.</summary>
    [AsyncMethodBuilder(typeof(OwnerTaskMethodBuilder))]
    private static async Task FrameAbove(HttpContext ctx, RequestDelegate next) => await next(ctx);

    private DefaultHttpContext Context(string contentType, Stream body)
    {
        var ctx = new DefaultHttpContext { RequestServices = _factory.Services };
        ctx.Request.Method        = "POST";
        ctx.Request.ContentType   = contentType;
        ctx.Request.ContentLength = body.Length;
        ctx.Request.Body          = body;
        ctx.Request.Headers["X-Seq-ApiKey"] = AmetoWebAppFactory.TestApiKey;
        ctx.Response.Body = new MemoryStream();
        return ctx;
    }

    /// <summary>A request body that counts its reads.</summary>
    private sealed class CountingStream(byte[] data) : MemoryStream(data)
    {
        private int _reads;
        public int Reads => Volatile.Read(ref _reads);

        public override int Read(byte[] buffer, int offset, int count)
        {
            Interlocked.Increment(ref _reads);
            return base.Read(buffer, offset, count);
        }

        public override int Read(Span<byte> buffer)
        {
            Interlocked.Increment(ref _reads);
            return base.Read(buffer);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _reads);
            return base.ReadAsync(buffer, offset, count, cancellationToken);
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _reads);
            return base.ReadAsync(buffer, cancellationToken);
        }
    }
}
