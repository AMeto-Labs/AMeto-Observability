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
/// Round-5 review H-G and M-D: the OTLP receivers are boxed before they own anything.
///
/// <para>A handler's state machine is boxed at its first real suspension, and a box that cannot be
/// allocated throws there with every finally of the handler skipped while what it awaited runs on.
/// That was the body read, which went on to wait for an inflate slot and take it for nobody — on the
/// stand's two slots, two such events and every compressed batch, logs, traces and metrics, HTTP and
/// gRPC, was refused until a restart — or, for a body read whole without suspending, the commit, which
/// the handler then waited out on a pool thread the commit itself needed (ce9e57c; a pool-wide stall
/// of seconds on two cores). Each handler yields first now, and a box that fails there is answered with
/// the retry code, owning nothing. Two awaits below a handler's own were first suspensions of their
/// methods as well, and are guarded where they stand: the body read's wait for a slot, and the read of
/// the body itself.</para>
///
/// <para>Driven over plain contexts; OwnerAsync's seam fails a first suspension as the runtime's box
/// allocation would.</para>
/// </summary>
public sealed class ReceiverFirstSuspensionTests : IClassFixture<AmetoWebAppFactory>
{
    private static readonly OtlpGzipTooLargeLog NoLog       = new(NullLogger.Instance, TimeProvider.System);
    private static readonly OtlpOutOfMemoryLog  NoMemoryLog = new(NullLogger.Instance, TimeProvider.System);

    private readonly AmetoWebAppFactory _factory;

    public ReceiverFirstSuspensionTests(AmetoWebAppFactory factory)
    {
        _factory = factory;
        using var seeded = factory.CreateClient();   // creating a client is what seeds the test API key
    }

    // ── The commit: nothing is waited out on a thread (M-D) ───────────────────

    [Fact]
    public async Task An_otlp_http_receiver_whose_first_suspension_cannot_be_boxed_takes_nothing_and_holds_no_thread()
    {
        var ctx = Context("application/json", Encoding.UTF8.GetBytes(
            "{\"resourceLogs\":[{\"scopeLogs\":[{\"logRecords\":[" +
            "{\"body\":{\"stringValue\":\"box probe 1\"}},{\"body\":{\"stringValue\":\"box probe 2\"}}]}]}]}"));

        var (heldMs, writes) = await RunWithTheStoreFaultingAsync("MapOtlpEndpoints", () => Route("/v1/logs")(ctx));

        Assert.True(heldMs < 1_000, $"the handler held its caller's thread {heldMs} ms, waiting a commit out");
        Assert.Equal(StatusCodes.Status503ServiceUnavailable, ctx.Response.StatusCode);
        Assert.Equal(IngestionEndpoint.RetryAfterSeconds, ctx.Response.Headers.RetryAfter.ToString());
        Assert.Equal(0, writes);   // nothing was decoded, so nothing was written
        Assert.Equal(0, _factory.Services.GetRequiredService<IngestionEndpoint>().DecodedBytesInFlight);
    }

    [Fact]
    public async Task An_otlp_grpc_receiver_whose_first_suspension_cannot_be_boxed_takes_nothing_and_holds_no_thread()
    {
        var ctx = Context("application/grpc", Frame(LogsRequest(2), compressed: false));
        ctx.Request.Protocol = "HTTP/2";

        var (heldMs, writes) = await RunWithTheStoreFaultingAsync("<HandleCoreAsync>",
            () => OtlpGrpcEndpointMapper.HandleLogsAsync(ctx, new OtlpInflateGate(1, TimeSpan.Zero), NoLog, NoMemoryLog));

        Assert.True(heldMs < 1_000, $"the handler held its caller's thread {heldMs} ms, waiting a commit out");
        Assert.Equal("14", ctx.Response.Headers["grpc-status"].ToString());   // UNAVAILABLE, which exporters retry
        Assert.Equal(0, writes);
        Assert.Equal(0, _factory.Services.GetRequiredService<IngestionEndpoint>().DecodedBytesInFlight);
    }

    /// <summary>
    /// Runs a handler with the store faulting throughout — a commit would wait its whole BackPressureWait —
    /// and the handler's first suspension failing where its state machine is boxed. Returns how long the
    /// caller's thread was held before it had the handler's task back, and how many writes were tried.
    /// </summary>
    private async Task<(long HeldMs, int Writes)> RunWithTheStoreFaultingAsync(string handler, Func<Task> run)
    {
        var storage = _factory.Services.GetRequiredService<StorageEngine>();
        int writes = 0, armed = 1, fired = 0;
        storage._afterWriteGate = () =>
        {
            Interlocked.Increment(ref writes);
            throw new OutOfMemoryException("test: a fault, so a write waits");
        };
        long heldMs;
        try
        {
            var  sw = System.Diagnostics.Stopwatch.StartNew();
            Task handled;
            OwnerAsync.FirstSuspensionFailsForTest.Value = t =>
            {
                if (!t.Name.Contains(handler, StringComparison.Ordinal) || Interlocked.Exchange(ref armed, 0) == 0) return false;
                Interlocked.Increment(ref fired);
                return true;
            };
            try { handled = run(); }
            finally { OwnerAsync.FirstSuspensionFailsForTest.Value = null; }
            heldMs = sw.ElapsedMilliseconds;
            await handled;
        }
        finally { storage._afterWriteGate = null; }
        Assert.Equal(1, Volatile.Read(ref fired));   // setup: the handler's first suspension failed
        return (heldMs, Volatile.Read(ref writes));
    }

    // ── The inflate gate: no wait is left to take a slot for nobody (H-G) ────

    /// <summary>
    /// P11, gRPC: the message read whole, the gate full. The handler's first suspension was its wait for a
    /// slot: answered UNAVAILABLE, and the wait went on to take the next freed slot for nobody.
    /// </summary>
    [Fact]
    public async Task A_grpc_receiver_whose_first_suspension_cannot_be_boxed_at_a_full_gate_leaves_no_wait_to_take_a_freed_slot()
    {
        var gate = new OtlpInflateGate(1, TimeSpan.FromSeconds(2));
        Assert.True(await gate.TryEnterAsync(default));   // the one slot taken
        var ctx = Context("application/grpc", Frame(OtlpGzipTests.Gzip(LogsRequest(2)), compressed: true));
        ctx.Request.Protocol = "HTTP/2";
        ctx.Request.Headers["grpc-encoding"] = "gzip";

        int fired = await FailFirstSuspensionAsync("<HandleCoreAsync>", () => OtlpGrpcEndpointMapper.HandleLogsAsync(ctx, gate, NoLog, NoMemoryLog));

        Assert.Equal(1, fired);
        Assert.Equal("14", ctx.Response.Headers["grpc-status"].ToString());
        gate.Exit();                                       // the slot comes free...
        await Task.Delay(300);                             // ...and a wait left behind would have it by now
        Assert.True(gate.Available == 1, "a slot the gate freed was taken by a wait the failed handler had left behind");
    }

    /// <summary>
    /// P12, HTTP: every slot taken, a gzip body. The handler's first suspension was its await of the body
    /// read, which was waiting for a slot: answered 503, and the read went on to take a slot, inflate the
    /// body and hand both to nobody. Logs, traces and metrics alike.
    /// </summary>
    [Theory]
    [InlineData("/v1/logs",    "{\"resourceLogs\":[{\"scopeLogs\":[{\"logRecords\":[{\"body\":{\"stringValue\":\"p12\"}}]}]}]}")]
    [InlineData("/v1/traces",  "{\"resourceSpans\":[]}")]
    [InlineData("/v1/metrics", "{\"resourceMetrics\":[]}")]
    public async Task An_http_receiver_whose_first_suspension_cannot_be_boxed_at_a_full_gate_leaves_no_wait_to_take_a_freed_slot(string route, string json)
    {
        var gate = _factory.Services.GetRequiredService<OtlpInflateGate>();
        int took = await TakeEverySlotAsync(gate);
        var ctx  = Context("application/json", OtlpGzipTests.Gzip(Encoding.UTF8.GetBytes(json)));
        ctx.Request.Headers.ContentEncoding = "gzip";

        int fired;
        try { fired = await FailFirstSuspensionAsync("MapOtlpEndpoints", () => Route(route)(ctx)); }
        finally
        {
            for (int i = 0; i < took; i++) gate.Exit();    // every slot this test took comes free...
        }
        await Task.Delay(300);                             // ...and a read left behind would have one by now

        Assert.Equal(1, fired);
        Assert.Equal(StatusCodes.Status503ServiceUnavailable, ctx.Response.StatusCode);
        Assert.True(gate.Available == took, $"{took - gate.Available} slot(s) taken by a read the failed handler had left behind");
    }

    /// <summary>
    /// Takes every slot of the gate (the fast path, none waited for); how many. A slot an earlier case left
    /// taken for nobody (before the fix, each of these did) is given back first, so a case counts only its own.
    /// </summary>
    private static async Task<int> TakeEverySlotAsync(OtlpInflateGate gate)
    {
        while (gate.Available < gate.Capacity) gate.Exit();
        int took = 0;
        while (gate.Available > 0)
        {
            Assert.True(await gate.TryEnterAsync(default));
            took++;
        }
        Assert.True(took > 0, "setup: the gate had no slot free");
        return took;
    }

    /// <summary>
    /// The body read's own wait for a slot: the handler is boxed, but the read (ReadBodyAsync) is a method of
    /// its own, and with the body read whole its first suspension is that wait. A box that failed there threw
    /// with the wait pending, and the wait took the next freed slot for nobody. It is waited out now, and the
    /// slot it took given back.
    /// </summary>
    [Fact]
    public async Task A_body_read_whose_wait_for_a_slot_cannot_be_boxed_gives_back_the_slot_the_wait_takes()
    {
        var gate = _factory.Services.GetRequiredService<OtlpInflateGate>();
        int took = await TakeEverySlotAsync(gate);
        var ctx  = Context("application/json", OtlpGzipTests.Gzip(Encoding.UTF8.GetBytes(
            "{\"resourceLogs\":[{\"scopeLogs\":[{\"logRecords\":[{\"body\":{\"stringValue\":\"read\"}}]}]}]}")));
        ctx.Request.Headers.ContentEncoding = "gzip";

        using var failed = new ManualResetEventSlim(false);
        int armed = 1;
        Task handled;
        OwnerAsync.FirstSuspensionFailsForTest.Value = t =>
        {
            if (!t.Name.Contains("<ReadBodyAsync>", StringComparison.Ordinal) || Interlocked.Exchange(ref armed, 0) == 0) return false;
            failed.Set();
            return true;
        };
        try { handled = Task.Run(() => Route("/v1/logs")(ctx)); }   // a read waited out holds its thread
        finally { OwnerAsync.FirstSuspensionFailsForTest.Value = null; }
        try
        {
            Assert.True(failed.Wait(TimeSpan.FromSeconds(10)), "setup: the read's wait for a slot was not its first suspension");
            gate.Exit();                                   // one slot comes free while the wait is pending: it takes it
            await handled.WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally
        {
            for (int i = 1; i < took; i++) gate.Exit();
        }
        await Task.Delay(300);

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, ctx.Response.StatusCode);
        Assert.True(gate.Available == took, $"{took - gate.Available} slot(s) taken for nobody by a wait the failed read had left behind");
    }

    // ── The body read: a buffer is not given back under a read (found with H-G) ─

    /// <summary>
    /// The read of the body (OtlpBodyReader) is a method of its own too, and its first suspension can be
    /// the read itself, still filling the buffer. A box that failed there gave the buffer back to the pool
    /// while the read went on to write into it — inside whichever request rented it next. It is left to the
    /// read now.
    /// </summary>
    [Fact]
    public async Task A_body_read_whose_await_cannot_be_boxed_leaves_its_buffer_to_the_read_still_writing_into_it()
    {
        byte[] framed = Frame(LogsRequest(1), compressed: false);
        var pendingBody = new PendingReadStream();
        var ctx = Context("application/grpc", framed);
        ctx.Request.Protocol = "HTTP/2";
        ctx.Request.Body     = pendingBody;

        using var ledger = IngestBufferPoolLedger.Open();
        int fired = await FailFirstSuspensionAsync("<ReadAsync>",
            () => OtlpGrpcEndpointMapper.HandleLogsAsync(ctx, new OtlpInflateGate(1, TimeSpan.Zero), NoLog, NoMemoryLog),
            declaringType: typeof(OtlpBodyReader));

        Assert.Equal(1, fired);
        Assert.Equal("14", ctx.Response.Headers["grpc-status"].ToString());
        Assert.True(pendingBody.IsPending, "setup: the read was not pending when the box failed");
        Assert.True(ledger.Outstanding == 1, "the body buffer went back to the pool while a read was still writing into it");
        pendingBody.Complete(framed);                      // the read lands in a buffer nobody else holds
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>Runs <paramref name="run"/> with the first suspension of the method <paramref name="method"/> names failing, once; how many times it fired.</summary>
    private static async Task<int> FailFirstSuspensionAsync(string method, Func<Task> run, Type? declaringType = null)
    {
        int armed = 1, fired = 0;
        OwnerAsync.FirstSuspensionFailsForTest.Value = t =>
        {
            if (!t.Name.Contains(method, StringComparison.Ordinal) || (declaringType is not null && t.DeclaringType != declaringType)
                || Interlocked.Exchange(ref armed, 0) == 0) return false;
            Interlocked.Increment(ref fired);
            return true;
        };
        try { await run(); }
        finally { OwnerAsync.FirstSuspensionFailsForTest.Value = null; }
        return Volatile.Read(ref fired);
    }

    private RequestDelegate Route(string pattern) =>
        ((RouteEndpoint)_factory.Services.GetRequiredService<EndpointDataSource>().Endpoints
            .Single(e => e is RouteEndpoint r && r.RoutePattern.RawText == pattern)).RequestDelegate!;

    /// <summary>A request body whose read stays pending until <see cref="Complete"/> writes into the buffer it was given.</summary>
    private sealed class PendingReadStream : Stream
    {
        private readonly TaskCompletionSource<int> _read = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private Memory<byte> _into;
        private int _reads;

        public bool IsPending => Volatile.Read(ref _reads) == 1 && !_read.Task.IsCompleted;

        public void Complete(byte[] data)
        {
            int n = Math.Min(data.Length, _into.Length);
            data.AsSpan(0, n).CopyTo(_into.Span);
            _read.TrySetResult(n);
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref _reads) > 1) return ValueTask.FromResult(0);
            _into = buffer;
            return new ValueTask<int>(_read.Task);
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
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

    private static byte[] Frame(byte[] message, bool compressed)
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
                        b.WriteString($"first suspension probe {ii}");
                    }));
                }));
            }
        })))));
}
