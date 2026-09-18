using System.Buffers;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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

        var (dtoUs, dtoBytes, _)        = Measure(dto, dtoSink);
        var (rowUs, rowBytes, rowSends) = Measure(row, rowSink);

        dtoSink.Reset(); RunInline(dto());
        rowSink.Reset(); RunInline(row());

        // COUNTED, NOT ASSUMED. One WriteLogEventAsync a row is how WriteLogEventsAsync is written
        // today, not something the probe can take on trust: a refactor that composed the rows inline
        // and left WriteLogEventAsync public for its other callers would leave 500 × 88 B subtracted in
        // Debug for state machines the poll no longer builds — room for a real 88 B allocation on every
        // row, hidden in the only build CI gates. The calls are counted on a poll of their own because
        // counting walks the stack, which allocates; how many a poll makes does not depend on timing.
        int rowCalls = CountCalls(row, Method(typeof(SseJsonWriter), nameof(SseJsonWriter.WriteLogEventAsync)));

        // Every async call on the row road, weighed as the build compiled it: the poll, the source
        // loop, each WriteLogEventAsync the count above saw, and one SendAsync a send the sink counted.
        // A closing FlushFramesAsync that finds the last row already sent calls SendAsync without
        // writing, and is the one call this misses — at most one a poll.
        double asyncBoxes = AsyncBoxBytes(typeof(LiveTailFrameProbe), nameof(RowPollAsync))
                          + AsyncBoxBytes(typeof(SseJsonWriter), nameof(SseJsonWriter.WriteLogEventsAsync))
                          + AsyncBoxBytes(typeof(SseJsonWriter), nameof(SseJsonWriter.WriteLogEventAsync)) * (double)rowCalls
                          + AsyncBoxBytes(typeof(SseJsonWriter), "SendAsync") * rowSends;
        double ownPerRow  = (rowBytes - asyncBoxes) / PollRows;

        _out.WriteLine($"{shape}: {PollRows}-row poll, {frameBytes / 1024.0:F0} KB of frames ({frameBytes / (double)PollRows:F0} B/frame)");
        _out.WriteLine($"  DTO frame per row  : {dtoUs * 1000 / PollRows,6:F0} ns/frame | {dtoBytes / PollRows,6:F1} B/frame | {dtoUs / 1000,6:F2} ms/poll | {dtoBytes / 1024.0,7:F1} KB/poll | {dtoSink.Writes} writes + {dtoSink.Flushes} flushes");
        _out.WriteLine($"  row writer + flush : {rowUs * 1000 / PollRows,6:F0} ns/frame | {rowBytes / PollRows,6:F1} B/frame | {rowUs / 1000,6:F2} ms/poll | {rowBytes / 1024.0,7:F1} KB/poll | {rowSink.Writes} writes + {rowSink.Flushes} flushes");
        _out.WriteLine($"    async state machines built as classes: {asyncBoxes / PollRows,6:F1} B/frame ({rowCalls} WriteLogEventAsync calls a poll); the writer's own: {ownPerRow,6:F1} B/frame");
        _out.WriteLine($"  gain               : {dtoUs / rowUs:F1}x faster, {dtoBytes / Math.Max(rowBytes, 1):F0}x less allocated, {dtoSink.Writes / (double)Math.Max(rowSink.Writes, 1):F0}x fewer sends");

        // GC.GetAllocatedBytesForCurrentThread is deterministic, so this holds on any machine, and with
        // the compiler's state machines taken out (AsyncBoxBytes) it holds in any build. The DTO road
        // allocates a DTO, an exception-tree copy and four strings per row; the row writer, nothing.
        //
        // A BOUND PER ROW, NOT A RATIO TO THE DTO ROAD. The ratio this replaced compared raw totals, and
        // it went red on CI's Debug build over code that allocates nothing: 101.9 B/frame, every byte of
        // it state machines. A ratio is also not the claim — it lets the row road allocate anything up
        // to a tenth of whatever the old road happened to cost.
        //
        // 2 B a row: the smallest object on a 64-bit heap is 24 B, so one allocation on every row is
        // twelve times over it and one on every tenth row is still over, while the call the count above
        // can miss is 80 B a poll in Debug — 0.16 B a row. The timings are reported, not asserted.
        //
        // BOUNDED BELOW AS WELL. The poll cannot allocate less than nothing, so a figure under -2 B a
        // row means the accounting subtracts state machines the build did not allocate — a call the
        // list above names that the road no longer makes, or makes fewer times than counted. Left
        // unbounded, that surplus would cancel a real allocation of the same size.
        Assert.True(ownPerRow > -2,
            $"the async accounting is stale: {ownPerRow:F1} B/frame of the writer's own means it subtracts " +
            $"state machines the poll no longer builds ({rowBytes:F0} B/poll measured, {asyncBoxes:F0} B " +
            $"subtracted); make the call list above follow the row road");
        Assert.True(ownPerRow < 2,
            $"the row writer should allocate nothing per row: {ownPerRow:F1} B/frame of its own " +
            $"({rowBytes:F0} B/poll, of which {asyncBoxes:F0} B async state machines built as classes)");
    }

    private static MethodInfo Method(Type owner, string method) =>
        owner.GetMethod(method, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static)
        ?? throw new InvalidOperationException($"{owner.Name}.{method} is gone; the probe's async accounting must follow it");

    /// <summary>
    /// How many rows one <paramref name="poll"/> composes inside <paramref name="method"/> — a row whose
    /// property bytes are read while that method is on the stack (<see cref="WatchedBytes"/>), counted
    /// once however often it is read.
    ///
    /// <para>A frame of the method's own async state machine counts as the method: that is where its
    /// body runs. A row composed anywhere else is not counted, which leaves its state machine in the
    /// writer's own bytes and fails the bound loudly rather than hiding anything.</para>
    /// </summary>
    private static int CountCalls(Func<ValueTask> poll, MethodInfo method)
    {
        WatchedBytes.Begin(method);
        try
        {
            RunInline(poll());
            return WatchedBytes.Composed;
        }
        finally
        {
            WatchedBytes.End();
        }
    }

    /// <summary>
    /// The heap bytes one call of <paramref name="method"/> costs before its body runs: its async
    /// state machine when the compiler built that as a CLASS, and nothing when it built a struct.
    ///
    /// <para>The build decides, not the code. An optimising build makes the state machine a struct on
    /// the caller's stack, boxed only if the method really suspends, so a call that completes inline
    /// allocates nothing. A Debug build makes it a class, for Edit and Continue, and every call
    /// allocates one whether it suspends or not: 88 B for WriteLogEventAsync, 80 B for SendAsync and
    /// 152 B for WriteLogEventsAsync when this was written — which on the fat rows was all of CI's
    /// 101.9 B/frame.</para>
    ///
    /// <para>Read from the method's own <see cref="AsyncStateMachineAttribute"/> and weighed by
    /// allocating one, so it follows the build it runs in and the method as it is now. A method that
    /// stops being async weighs nothing here, which only makes the upper bound stricter; one that is
    /// renamed fails loudly rather than quietly weighing nothing. One the road stops CALLING still
    /// weighs what it did, which is why the row writer's calls are counted and the bound has a floor.</para>
    /// </summary>
    private static long AsyncBoxBytes(Type owner, string method)
    {
        Type? stateMachine = Method(owner, method).GetCustomAttribute<AsyncStateMachineAttribute>()?.StateMachineType;
        if (stateMachine is null || stateMachine.IsValueType) return 0;

        // The first call fills the runtime's allocator cache for the type; the second is the object alone.
        RuntimeHelpers.GetUninitializedObject(stateMachine);
        long b0 = GC.GetAllocatedBytesForCurrentThread();
        RuntimeHelpers.GetUninitializedObject(stateMachine);
        return GC.GetAllocatedBytesForCurrentThread() - b0;
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

    /// <summary>
    /// Time, bytes and sends per poll over <see cref="Polls"/> polls. The sends are counted over the
    /// same polls as the bytes, not taken from a poll of their own: the writer's quiet-stream rule can
    /// add a send the first time a poll follows a pause, and each send is an async call whose state
    /// machine the bytes include.
    /// </summary>
    private static (double UsPerPoll, double BytesPerPoll, double SendsPerPoll) Measure(Func<ValueTask> poll, SinkStream sink)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        sink.Reset();
        long b0 = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < Polls; i++) RunInline(poll());
        sw.Stop();
        return (sw.Elapsed.TotalMilliseconds * 1000 / Polls,
                (GC.GetAllocatedBytesForCurrentThread() - b0) / (double)Polls,
                sink.Writes / (double)Polls);
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
                RawProperties   = new WatchedBytes(e.RawProperties.ToArray()).Memory,
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
                RawProperties   = new WatchedBytes(buf.WrittenSpan.ToArray()).Memory,
            });
        }
        return rows;
    }

    /// <summary>
    /// A row's property bytes behind a <see cref="MemoryManager{T}"/>, so the probe can see WHERE a row
    /// is composed: every writer reads them through <see cref="GetSpan"/>, and while a count is running
    /// (<see cref="CountCalls"/>) that read looks at the stack.
    ///
    /// <para>Outside a count the read is one static check and the array — nothing allocated, so the
    /// measured polls weigh what they did over plain arrays. The frames are identical either way; the
    /// capture comparison in <see cref="Weigh"/> is over these rows.</para>
    /// </summary>
    private sealed class WatchedBytes(byte[] bytes) : MemoryManager<byte>
    {
        private static MethodInfo? s_method;
        private static Type?       s_stateMachine;
        private static int         s_count;

        /// <summary>The count this row was last seen in, so a row read twice counts once.</summary>
        private int _seenIn;

        public static int Composed { get; private set; }

        public static void Begin(MethodInfo method)
        {
            s_method       = method;
            s_stateMachine = method.GetCustomAttribute<AsyncStateMachineAttribute>()?.StateMachineType;
            s_count++;
            Composed = 0;
        }

        public static void End() => s_method = null;

        public override Span<byte> GetSpan()
        {
            if (s_method is not null && _seenIn != s_count && OnStack())
            {
                _seenIn = s_count;
                Composed++;
            }
            return bytes;
        }

        private static bool OnStack()
        {
            foreach (StackFrame frame in new StackTrace(1, false).GetFrames())
            {
                MethodBase? m = frame.GetMethod();
                if (m is null) continue;
                if (m.Equals(s_method) || (s_stateMachine is not null && m.DeclaringType == s_stateMachine)) return true;
            }
            return false;
        }

        public override unsafe MemoryHandle Pin(int elementIndex = 0)
        {
            GCHandle handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
            return new MemoryHandle((byte*)handle.AddrOfPinnedObject() + elementIndex, handle, this);
        }

        public override void Unpin() { }
        protected override void Dispose(bool disposing) { }
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
