using System.Buffers;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Ameto.Core;
using Ameto.Storage;
using MessagePack;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// What one live-tail POLL costs to put on the wire, per frame: a full page (LiveTail.PageSize, 500
/// rows) through <see cref="SseJsonWriter"/>, in two shapes — the fat Office.API rows of
/// <see cref="LogPageJsonProbe"/> with half of them carrying an exception tree, and small rows (a short
/// template and one property).
///
/// <para>old — one DTO frame per row, as the tail wrote them until it moved:
/// <c>ProbeLogEventDto.From(ev)</c> serialised through the reflection resolver by
/// <c>WriteEventAsync</c>, each frame written and flushed on its own;<br/>
/// new — the poll handed to <c>WriteLogEventsAsync</c> and flushed at its end:
/// <c>LogEventJsonWriter</c> straight from the event, frames coalesced into ~16 KB sends.</para>
///
/// <para>The body is an inline sink, so the time and the allocations are the server's own work —
/// serialise, frame, hand to the body — and not a socket's. The writes and flushes per poll are
/// reported beside them, because on a real connection each one is a Kestrel write and flush.</para>
/// </summary>
public sealed class LiveTailFrameProbe
{
    private const int PollRows = 500;
    private const int Polls    = 100;

    /// <summary>The server's old reflection options, spelled out because Ameto.Perf cannot see the frozen copy.</summary>
    private static readonly JsonSerializerOptions DtoOptions = new()
    {
        PropertyNamingPolicy   = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
        WriteIndented          = false,
        Converters             = { new TestDynamicObjectConverter() },
    };

    /// <summary>
    /// The options resolved to a contract the way <c>JsonSerializer.Serialize(writer, value, options)</c>
    /// resolves them — the writer's reflection overload went with the tail.
    /// </summary>
    private static readonly JsonTypeInfo<ProbeLogEventDto> DtoContract = CreateContract();

    private static JsonTypeInfo<ProbeLogEventDto> CreateContract()
    {
        DtoOptions.MakeReadOnly(populateMissingResolver: true);
        return (JsonTypeInfo<ProbeLogEventDto>)DtoOptions.GetTypeInfo(typeof(ProbeLogEventDto));
    }

    private readonly ITestOutputHelper _out;
    public LiveTailFrameProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public async Task LiveTailPoll_RowWriterAgainstDtoFrames()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-tailframes-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            string path = LogPageJsonProbe.BuildSegment(dir);

            var decoded = new List<LogEvent>(PollRows);
            using (var reader = SegmentReader.Open(path))
            {
                await foreach (var ev in reader.ReadEventsAsync(null, null, null, reversed: false, default))
                {
                    decoded.Add(ev);
                    if (decoded.Count >= PollRows) break;
                }
            }
            Assert.Equal(PollRows, decoded.Count);

            Weigh("fat rows, half with an exception tree", FatRows(decoded));
            Weigh("small rows", SmallRows());
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { }
        }
    }

    private void Weigh(string shape, List<LogEvent> rows)
    {
        var poll = new InlineRows(rows);

        // Same bytes out, or the comparison is meaningless.
        var dtoCapture = new SinkStream(capture: true);
        var rowCapture = new SinkStream(capture: true);
        using (var sse = new SseJsonWriter(dtoCapture)) RunInline(DtoPollAsync(sse, poll));
        using (var sse = new SseJsonWriter(rowCapture)) RunInline(RowPollAsync(sse, poll));
        Assert.Equal(dtoCapture.Captured(), rowCapture.Captured());
        long frameBytes = rowCapture.Bytes;

        var dtoSink = new SinkStream(capture: false);
        var rowSink = new SinkStream(capture: false);
        using var dtoSse = new SseJsonWriter(dtoSink);
        using var rowSse = new SseJsonWriter(rowSink);
        Func<ValueTask> dto = () => DtoPollAsync(dtoSse, poll);
        Func<ValueTask> row = () => RowPollAsync(rowSse, poll);

        for (int i = 0; i < 20; i++) { RunInline(dto()); RunInline(row()); }

        var (dtoUs, dtoBytes) = Measure(dto);
        var (rowUs, rowBytes) = Measure(row);

        dtoSink.Reset(); RunInline(dto());
        rowSink.Reset(); RunInline(row());

        _out.WriteLine($"{shape}: {PollRows}-row poll, {frameBytes / 1024.0:F0} KB of frames ({frameBytes / (double)PollRows:F0} B/frame)");
        _out.WriteLine($"  DTO frame per row  : {dtoUs * 1000 / PollRows,6:F0} ns/frame | {dtoBytes / (double)PollRows,6:F1} B/frame | {dtoUs / 1000,6:F2} ms/poll | {dtoBytes / 1024.0,7:F1} KB/poll | {dtoSink.Writes} writes + {dtoSink.Flushes} flushes");
        _out.WriteLine($"  row writer + flush : {rowUs * 1000 / PollRows,6:F0} ns/frame | {rowBytes / (double)PollRows,6:F1} B/frame | {rowUs / 1000,6:F2} ms/poll | {rowBytes / 1024.0,7:F1} KB/poll | {rowSink.Writes} writes + {rowSink.Flushes} flushes");
        _out.WriteLine($"  gain               : {dtoUs / rowUs:F1}x faster, {(double)dtoBytes / Math.Max(rowBytes, 1):F0}x less allocated, {dtoSink.Writes / (double)Math.Max(rowSink.Writes, 1):F0}x fewer sends");

        // GC.GetAllocatedBytesForCurrentThread is deterministic, so this holds on any machine: the DTO
        // road allocates a DTO, an exception-tree copy and four strings per row, the row writer nothing
        // per row. The timings are reported, not asserted.
        Assert.True(rowBytes * 10 < dtoBytes,
            $"the row writer should allocate ~nothing per frame: dto={dtoBytes} B/poll, row writer={rowBytes} B/poll");
    }

    private static async ValueTask DtoPollAsync(SseJsonWriter sse, IAsyncEnumerable<LogEvent> poll)
    {
        await foreach (var ev in poll)
            await sse.WriteEventAsync(ProbeLogEventDto.From(ev), DtoContract, default);
    }

    private static async ValueTask RowPollAsync(SseJsonWriter sse, IAsyncEnumerable<LogEvent> poll)
    {
        await sse.WriteLogEventsAsync(poll, default);
        await sse.FlushFramesAsync(default);
    }

    /// <summary>
    /// A poll over an inline body and an inline source completes inline — which is what keeps every
    /// byte it allocates on this thread's counter.
    /// </summary>
    private static void RunInline(ValueTask poll)
    {
        if (!poll.IsCompletedSuccessfully)
            throw new InvalidOperationException("the poll did not complete inline; the thread's allocation count would miss part of it");
        poll.GetAwaiter().GetResult();
    }

    private static (double UsPerPoll, long BytesPerPoll) Measure(Func<ValueTask> poll)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        long b0 = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < Polls; i++) RunInline(poll());
        sw.Stop();
        return (sw.Elapsed.TotalMilliseconds * 1000 / Polls,
                (GC.GetAllocatedBytesForCurrentThread() - b0) / Polls);
    }

    private static List<LogEvent> FatRows(List<LogEvent> decoded)
    {
        var rows = new List<LogEvent>(decoded.Count);
        for (int i = 0; i < decoded.Count; i++)
        {
            var e = decoded[i];
            rows.Add(new LogEvent
            {
                Id              = e.Id,
                Timestamp       = e.Timestamp,
                Level           = e.Level,
                MessageTemplate = e.MessageTemplate,
                ServiceName     = e.ServiceName,
                RawProperties   = e.RawProperties.ToArray(),
                TraceIdHi       = 0x0123456789abcdefUL,
                TraceIdLo       = (ulong)(i + 1),
                SpanId          = (ulong)(i + 1),
                Exception       = (i % 2) == 0 ? null : new ExceptionInfo
                {
                    Type       = "System.InvalidOperationException",
                    Message    = "the handler refused the command",
                    StackTrace = "   at Common.MediatR.LoggingBehavior.Handle()\n   at Office.API.Controller.Post()",
                    Inner      = new ExceptionInfo { Type = "System.TimeoutException", Message = "timed out" },
                },
            });
        }
        return rows;
    }

    private static List<LogEvent> SmallRows()
    {
        var  rows      = new List<LogEvent>(PollRows);
        long baseTicks = new DateTimeOffset(2026, 9, 15, 12, 0, 0, TimeSpan.Zero).UtcTicks;
        var  buf       = new ArrayBufferWriter<byte>(16);
        for (int i = 0; i < PollRows; i++)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1);
            w.Write("n"); w.Write((long)i);
            w.Flush();

            rows.Add(new LogEvent
            {
                Id              = new EventId(0u, (uint)i),
                Timestamp       = new DateTimeOffset(baseTicks + i, TimeSpan.Zero),
                Level           = Ameto.Core.LogLevel.Information,
                MessageTemplate = "tailed {n}",
                RawProperties   = buf.WrittenSpan.ToArray(),
            });
        }
        return rows;
    }

    /// <summary>A poll's rows, handed over inline; re-enumerable without allocating.</summary>
    private sealed class InlineRows(List<LogEvent> rows) : IAsyncEnumerable<LogEvent>, IAsyncEnumerator<LogEvent>
    {
        private int _next = -1;

        public IAsyncEnumerator<LogEvent> GetAsyncEnumerator(CancellationToken ct = default)
        {
            _next = -1;
            return this;
        }

        public LogEvent Current => rows[_next];
        public ValueTask<bool> MoveNextAsync() => new(++_next < rows.Count);
        public ValueTask DisposeAsync() => default;
    }

    /// <summary>Takes every write and flush inline and counts them; keeps the bytes only when asked.</summary>
    private sealed class SinkStream(bool capture) : Stream
    {
        private readonly MemoryStream? _capture = capture ? new MemoryStream() : null;

        public long Bytes;
        public int  Writes;
        public int  Flushes;

        public void   Reset()    { Bytes = 0; Writes = 0; Flushes = 0; }
        public string Captured() => System.Text.Encoding.UTF8.GetString(_capture!.ToArray());

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
        {
            Bytes += buffer.Length;
            Writes++;
            _capture?.Write(buffer.Span);
            return default;
        }

        public override Task FlushAsync(CancellationToken ct)
        {
            Flushes++;
            return Task.CompletedTask;
        }

        public override bool CanRead  => false;
        public override bool CanSeek  => false;
        public override bool CanWrite => true;
        public override long Length   => throw new NotSupportedException();
        public override long Position { get => 0; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int  Read(byte[] b, int o, int c) => throw new NotSupportedException();
        public override long Seek(long o, SeekOrigin r) => throw new NotSupportedException();
        public override void SetLength(long v) => throw new NotSupportedException();
        public override void Write(byte[] b, int o, int c) => WriteAsync(b.AsMemory(o, c));
    }
}
