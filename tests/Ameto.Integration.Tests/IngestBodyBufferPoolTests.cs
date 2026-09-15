using System.Buffers;
using System.Text.Json;
using MessagePack;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Storage;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Integration.Tests;

/// <summary>
/// <c>POST /api/events</c> reads each body into a buffer from <see cref="IngestBufferPool"/>, the
/// pool the OTLP receivers read into, and the reason that pool exists: ArrayPool.Shared is shallow,
/// and past its depth every concurrent 1.4 MB batch got a fresh 2 MB array on the large object heap.
///
/// <para>What these pin is the bookkeeping, which is where a pool change goes wrong without a
/// sound: every buffer the handler rents comes from that pool, and every one goes back to it
/// exactly once — on 200, on both kinds of 400, on 413, on a client abort, and on a fault that
/// escapes the handler. A buffer rented here and returned to Shared, or the reverse, throws
/// nowhere; the pool just quietly stops pooling.</para>
///
/// <para>The counting is <see cref="IngestBufferPoolLedger"/>, scoped to each test's own async
/// flow, so the OTLP suites renting from the same pool in parallel are not in it.</para>
/// </summary>
public sealed class IngestBodyBufferPoolTests : IClassFixture<AmetoWebAppFactory>
{
    private const int Concurrency = 16;
    private const int BodyBytes   = 1_400_000;
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(60);

    private readonly AmetoWebAppFactory _factory;
    public IngestBodyBufferPoolTests(AmetoWebAppFactory factory) => _factory = factory;

    // ── Through the real pipeline, sixteen at once ───────────────────────────

    [Fact]
    public async Task ConcurrentLargeBodies_AreReadIntoIngestBufferPool_AndEveryBufferGoesBack()
    {
        _factory.CreateClient().Dispose();                 // makes the factory seed its API key
        // The ledger is an AsyncLocal; TestServer carries it into the handler only when asked.
        _factory.Server.PreserveExecutionContext = true;

        byte[] body = LargeBatch(BodyBytes, out int events);
        var    gate = new ReadGate(Concurrency);

        using var ledger = IngestBufferPoolLedger.Open();

        var sends = new Task<HttpContext>[Concurrency];
        for (int i = 0; i < Concurrency; i++)
        {
            bool declareLength = i % 2 == 0;               // half Content-Length, half the doubling read
            sends[i] = _factory.Server.SendAsync(ctx =>
            {
                ctx.Request.Method      = "POST";
                ctx.Request.Path        = "/api/events";
                ctx.Request.ContentType = "application/octet-stream";
                if (declareLength) ctx.Request.ContentLength = body.Length;
                ctx.Request.Headers["X-Seq-ApiKey"] = AmetoWebAppFactory.TestApiKey;
                ctx.Request.Body        = new GatedBody(body, gate);
            });
        }

        foreach (HttpContext done in await Task.WhenAll(sends).WaitAsync(Patience))
        {
            Assert.Equal(StatusCodes.Status200OK, done.Response.StatusCode);

            // Read to the end: the response body completes only once the handler has returned,
            // its finally included, so the ledger below is not racing a return still under way.
            using var reader = new StreamReader(done.Response.Body);
            using var counts = JsonDocument.Parse(await reader.ReadToEndAsync().WaitAsync(Patience));
            Assert.Equal(events, counts.RootElement.GetProperty("ingested").GetInt32()
                               + counts.RootElement.GetProperty("dropped").GetInt32());
        }

        // Every body held its first read until all sixteen had one, and each handler rents
        // before it reads — so sixteen buffers from this pool were out at the same moment.
        Assert.True(ledger.PeakOutstanding >= Concurrency,
            $"expected {Concurrency} buffers out of IngestBufferPool at once, peak was {ledger.PeakOutstanding}");
        Assert.True(ledger.LargestRent >= body.Length,
            $"no buffer from IngestBufferPool could hold a {body.Length:N0}-byte body (largest rent {ledger.LargestRent:N0})");
        ledger.AssertEveryBufferCameBackOnce(minRents: Concurrency);
    }

    // ── Each way out of the handler, one at a time ───────────────────────────

    [Fact]
    public async Task Success_WithContentLength_RentsOnce_AndReturnsIt()
    {
        byte[] body = LargeBatch(BodyBytes, out _);
        var ctx = Post(new MemoryStream(body), declaredLength: body.Length);

        var (thrown, ledger) = await DriveAsync(Endpoint(), ctx);

        Assert.Null(thrown);
        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        Assert.Equal(1, ledger.Rents);                     // sized from Content-Length, never grown
        Assert.True(ledger.LargestRent >= body.Length);
        ledger.AssertEveryBufferCameBackOnce(minRents: 1);
    }

    [Fact]
    public async Task Success_Chunked_ReturnsEveryBufferItOutgrew()
    {
        byte[] body = LargeBatch(BodyBytes, out _);
        var ctx = Post(new MemoryStream(body), declaredLength: null);

        var (thrown, ledger) = await DriveAsync(Endpoint(), ctx);

        Assert.Null(thrown);
        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        // 64 KB doubling to the 2 MB bucket: every step is a rent, and every step but the last
        // is returned inside the loop rather than in the finally.
        Assert.True(ledger.Rents >= 2, $"expected the chunked read to grow its buffer, saw {ledger.Rents} rent(s)");
        ledger.AssertEveryBufferCameBackOnce(minRents: 2);
    }

    [Fact]
    public async Task MalformedBody_AfterAnIngestedPrefix_Answers400_AndReturnsItsBuffer()
    {
        var ctx = Post(new MemoryStream(BatchWithBadTail(good: 5)), declaredLength: null, fixLength: true);

        var (thrown, ledger) = await DriveAsync(Endpoint(), ctx);

        Assert.Null(thrown);
        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
        Assert.Equal(5, ReplyCount(ctx, "ingested"));      // the partial-ingest 400, not the header one
        ledger.AssertEveryBufferCameBackOnce(minRents: 1);
    }

    [Fact]
    public async Task TruncatedBody_Answers400_AndReturnsItsBuffer()
    {
        byte[] full = LargeBatch(BodyBytes, out _);
        var ctx = Post(new MemoryStream(full, 0, full.Length / 2), declaredLength: full.Length);

        var (thrown, ledger) = await DriveAsync(Endpoint(), ctx);

        Assert.Null(thrown);
        Assert.Equal(StatusCodes.Status400BadRequest, ctx.Response.StatusCode);
        Assert.Equal(0, ReplyCount(ctx, "ingested"));
        ledger.AssertEveryBufferCameBackOnce(minRents: 1);
    }

    [Fact]
    public async Task ChunkedBodyOverTheLimit_Answers413_AndReturnsEveryBufferItOutgrew()
    {
        int max = Options.Ingestion.MaxBatchBytes;
        var ctx = Post(new MemoryStream(new byte[max + 1]), declaredLength: null);

        var (thrown, ledger) = await DriveAsync(Endpoint(), ctx);

        Assert.Null(thrown);
        Assert.Equal(StatusCodes.Status413PayloadTooLarge, ctx.Response.StatusCode);
        Assert.True(ledger.LargestRent > max, "the 413 fires only once the body has outgrown the limit");
        ledger.AssertEveryBufferCameBackOnce(minRents: 2);
    }

    [Fact]
    public async Task DeclaredLengthOverTheLimit_Answers413_WithoutRentingAnything()
    {
        var ctx = Post(Stream.Null, declaredLength: Options.Ingestion.MaxBatchBytes + 1L);

        var (thrown, ledger) = await DriveAsync(Endpoint(), ctx);

        Assert.Null(thrown);
        Assert.Equal(StatusCodes.Status413PayloadTooLarge, ctx.Response.StatusCode);
        Assert.Equal(0, ledger.Rents);
        ledger.AssertEveryBufferCameBackOnce(minRents: 0);
    }

    /// <summary>
    /// The client stops sending part way through and the connection goes: the read throws out of
    /// the handler on RequestAborted. Chunked, the buffer has already been outgrown twice by then.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ClientAbortMidBody_ThrowsOutOfTheHandler_AndReturnsEveryBuffer(bool declareLength)
    {
        byte[] body = LargeBatch(BodyBytes, out _);
        var stalling = new StallingBody(body, served: 256 * 1024);
        var ctx      = Post(stalling, declaredLength: declareLength ? body.Length : null);
        using var aborted = new CancellationTokenSource();
        ctx.RequestAborted = aborted.Token;

        using var ledger = IngestBufferPoolLedger.Open();
        Task handler = Endpoint().HandleAsync(ctx);
        await stalling.Stalled.Task.WaitAsync(Patience);
        await aborted.CancelAsync();
        Exception? thrown = await Record.ExceptionAsync(() => handler.WaitAsync(Patience));

        Assert.IsAssignableFrom<OperationCanceledException>(thrown);
        ledger.AssertEveryBufferCameBackOnce(minRents: declareLength ? 1 : 2);
    }

    [Fact]
    public async Task SinkFault_EscapingTheHandler_StillReturnsItsBuffer()
    {
        // One event over the 64 KB per-event limit sends the sink down its warning path, and the
        // logger there throws: a server fault, which leaves the handler rather than answering 400.
        var ctx = Post(new MemoryStream(OversizedEventBatch()), declaredLength: null, fixLength: true);

        var (thrown, ledger) = await DriveAsync(Endpoint(new ThrowingLogger()), ctx);

        Assert.IsType<InvalidOperationException>(thrown);
        ledger.AssertEveryBufferCameBackOnce(minRents: 1);
    }

    /// <summary>
    /// A ceiling raised past <see cref="IngestBufferPool.MaxPooledBytes"/>: the pool hands out an
    /// exact-length array it does not own, and must take it back without throwing — a throw from
    /// the finally would turn every such request into a 500.
    /// </summary>
    [Theory]
    [InlineData(true)]    // 9 MB with Content-Length: one exact-length rent, 200
    [InlineData(false)]   // chunked past 12 MB: grows into a 16 MB array, 413
    public async Task BodiesAboveThePoolCeiling_GoBackWithoutThrowing(bool declareLength)
    {
        const int max = 12 * 1024 * 1024;
        var options = new ServerOptions { Ingestion = new IngestionOptions { MaxBatchBytes = max } };

        byte[] body = declareLength ? LargeBatch(9 * 1024 * 1024, out _) : new byte[max + 1];
        var ctx = Post(new MemoryStream(body), declaredLength: declareLength ? body.Length : null);

        var (thrown, ledger) = await DriveAsync(Endpoint(options: options), ctx);

        Assert.Null(thrown);
        Assert.Equal(declareLength ? StatusCodes.Status200OK : StatusCodes.Status413PayloadTooLarge,
                     ctx.Response.StatusCode);
        Assert.True(ledger.LargestRent > IngestBufferPool.MaxPooledBytes);
        ledger.AssertEveryBufferCameBackOnce(minRents: 1);
    }

    // ── Harness ──────────────────────────────────────────────────────────────

    private ServerOptions Options => _factory.Services.GetRequiredService<ServerOptions>();

    private IngestionEndpoint Endpoint(ILogger<IngestionEndpoint>? logger = null, ServerOptions? options = null)
    {
        var sp = _factory.Services;
        return new IngestionEndpoint(
            sp.GetRequiredService<IngestionRingBuffer>(),
            sp.GetRequiredService<StringInternPool>(),
            sp.GetRequiredService<IngestionDrainer>(),
            options ?? sp.GetRequiredService<ServerOptions>(),
            logger ?? NullLogger<IngestionEndpoint>.Instance);
    }

    private static async Task<(Exception? Thrown, IngestBufferPoolLedger Ledger)> DriveAsync(
        IngestionEndpoint endpoint, DefaultHttpContext ctx)
    {
        using var ledger = IngestBufferPoolLedger.Open();
        Exception? thrown = await Record.ExceptionAsync(() => endpoint.HandleAsync(ctx).WaitAsync(Patience));
        return (thrown, ledger);
    }

    /// <param name="fixLength">Declare the stream's own length — for the small bodies whose path does not depend on it.</param>
    private static DefaultHttpContext Post(Stream body, long? declaredLength, bool fixLength = false)
    {
        var ctx = new DefaultHttpContext();
        ctx.Request.Method        = "POST";
        ctx.Request.ContentType   = "application/octet-stream";
        ctx.Request.ContentLength = fixLength ? body.Length : declaredLength;
        ctx.Request.Body          = body;
        ctx.Response.Body         = new MemoryStream();
        return ctx;
    }

    private static int ReplyCount(DefaultHttpContext ctx, string name)
    {
        ctx.Response.Body.Position = 0;
        using var doc = JsonDocument.Parse(ctx.Response.Body);
        return doc.RootElement.GetProperty(name).GetInt32();
    }

    /// <summary>
    /// A CLEF batch of at least <paramref name="minBytes"/>, in ~4 KB events: the body's SIZE is
    /// what is under test, and a few hundred events per megabyte keeps the storage it lands in cheap.
    /// </summary>
    private static byte[] LargeBatch(int minBytes, out int events)
    {
        string filler = new('x', 4000);
        events = minBytes / filler.Length + 1;

        var buf = new ArrayBufferWriter<byte>(minBytes + 64 * 1024);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(events);
        for (int i = 0; i < events; i++)
        {
            w.WriteMapHeader(3);
            w.Write("@t");      w.Write("2024-03-01T10:20:30.1234567Z");
            w.Write("@mt");     w.Write("body buffer pool probe");
            w.Write("Payload"); w.Write(filler);
        }
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary><paramref name="good"/> complete events, then one whose map claims a pair it never supplies.</summary>
    private static byte[] BatchWithBadTail(int good)
    {
        var buf = new ArrayBufferWriter<byte>(1024);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(good + 1);
        for (int i = 0; i < good; i++)
        {
            w.WriteMapHeader(2);
            w.Write("@mt"); w.Write("pool bookkeeping {i}");
            w.Write("i");   w.Write((long)i);
        }
        w.WriteMapHeader(2);
        w.Write("@mt"); w.Write("never completed");
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>One event whose ~100 KB property is over the 64 KB per-event limit.</summary>
    private static byte[] OversizedEventBatch()
    {
        var buf = new ArrayBufferWriter<byte>(128 * 1024);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(1);
        w.WriteMapHeader(2);
        w.Write("@mt"); w.Write("pool bookkeeping sink fault");
        w.Write("Big"); w.Write(new string('x', 100_000));
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>Opens once <c>parties</c> readers have arrived, so their buffers are provably out together.</summary>
    private sealed class ReadGate(int parties)
    {
        private int _arrived;
        private readonly TaskCompletionSource _open = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Arrive()
        {
            if (Interlocked.Increment(ref _arrived) == parties) _open.TrySetResult();
            return _open.Task;
        }
    }

    /// <summary>A body whose first read waits at the gate; after that, 32 KB at a time, like a socket.</summary>
    private sealed class GatedBody(byte[] bytes, ReadGate gate) : ReadOnlyBody
    {
        private int  _pos;
        private bool _arrived;

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            if (!_arrived)
            {
                _arrived = true;
                await gate.Arrive().WaitAsync(Patience, ct);
            }
            int n = Math.Min(Math.Min(buffer.Length, 32 * 1024), bytes.Length - _pos);
            bytes.AsSpan(_pos, n).CopyTo(buffer.Span);
            _pos += n;
            return n;
        }
    }

    /// <summary>Serves the first <c>served</c> bytes, then waits on the request's abort token: a client gone mid-body.</summary>
    private sealed class StallingBody(byte[] bytes, int served) : ReadOnlyBody
    {
        private int _pos;
        public TaskCompletionSource Stalled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            if (_pos < served)
            {
                int n = Math.Min(buffer.Length, served - _pos);
                bytes.AsSpan(_pos, n).CopyTo(buffer.Span);
                _pos += n;
                return n;
            }
            Stalled.TrySetResult();
            await Task.Delay(Timeout.Infinite, ct);
            return 0;
        }
    }

    /// <summary>Async-read-only stream plumbing; a synchronous read throws, so it cannot bypass a gate unnoticed.</summary>
    private abstract class ReadOnlyBody : Stream
    {
        public abstract override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default);
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct)
            => ReadAsync(buffer.AsMemory(offset, count), ct).AsTask();

        public override bool CanRead  => true;
        public override bool CanSeek  => false;
        public override bool CanWrite => false;
        public override long Length   => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int  Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private sealed class ThrowingLogger : ILogger<IngestionEndpoint>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;
        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
                                TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => throw new InvalidOperationException("sink fault: the logger under TryIngestClef failed");
    }
}

/// <summary>
/// Every <see cref="IngestBufferPool"/> rent and return made by the async flow that opened the
/// ledger, and by nothing else: the hook is process-wide, the ledger is an AsyncLocal, so a test
/// renting from the same pool on another flow at the same moment is not counted here.
/// </summary>
internal sealed class IngestBufferPoolLedger : IDisposable
{
    private static readonly AsyncLocal<IngestBufferPoolLedger?> Current = new();

    /// <summary>Installed on first use and never removed: with no ledger open it is one AsyncLocal read per call.</summary>
    private sealed class Hook : IIngestBufferPoolObserver
    {
        public void Rented(byte[] array)    => Current.Value?.OnRented(array);
        public void Returning(byte[] array) => Current.Value?.OnReturning(array);
    }

    private static readonly Hook Installed = new();

    private readonly Lock            _gate   = new();
    private readonly HashSet<byte[]> _out    = new(ReferenceEqualityComparer.Instance);
    private readonly List<string>    _faults = [];

    public int Rents           { get { lock (_gate) return _rents; } }
    public int PeakOutstanding { get { lock (_gate) return _peak; } }
    public int LargestRent     { get { lock (_gate) return _largest; } }
    private int _rents, _returns, _peak, _largest;

    public static IngestBufferPoolLedger Open()
    {
        var existing = Interlocked.CompareExchange(ref IngestBufferPool.Observer, Installed, null);
        if (existing is not null && existing != Installed)
            throw new InvalidOperationException("another IngestBufferPool observer is installed");

        var ledger = new IngestBufferPoolLedger();
        Current.Value = ledger;
        return ledger;
    }

    public void Dispose() => Current.Value = null;

    private void OnRented(byte[] array)
    {
        lock (_gate)
        {
            _rents++;
            _largest = Math.Max(_largest, array.Length);
            if (!_out.Add(array))
                _faults.Add($"a {array.Length:N0}-byte buffer was handed out while still out: it had been returned twice");
            _peak = Math.Max(_peak, _out.Count);
        }
    }

    private void OnReturning(byte[] array)
    {
        lock (_gate)
        {
            _returns++;
            if (!_out.Remove(array))
                _faults.Add($"a {array.Length:N0}-byte buffer came back that IngestBufferPool did not hand out (rented elsewhere, or returned twice)");
        }
    }

    /// <summary>At least <paramref name="minRents"/> rents from the pool; each one back in it, once; nothing foreign put in.</summary>
    public void AssertEveryBufferCameBackOnce(int minRents)
    {
        lock (_gate)
        {
            Assert.True(_rents >= minRents,
                $"expected at least {minRents} rent(s) from IngestBufferPool, saw {_rents}");
            Assert.True(_faults.Count == 0, string.Join(Environment.NewLine, _faults));
            Assert.True(_out.Count == 0,
                $"{_out.Count} buffer(s) rented from IngestBufferPool never came back to it: "
              + string.Join(", ", _out.Select(static a => $"{a.Length:N0} B")));
            Assert.Equal(_rents, _returns);
        }
    }
}
