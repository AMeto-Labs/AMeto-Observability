using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Ameto.Core;
using Ameto.Testing;

namespace Ameto.Core.Tests;

/// <summary>
/// A body that remembers each write as the client's socket would see it, and completes every
/// call inline. Locked, because <see cref="SseJsonWriter.WriteLogEventsAsync"/> sends while its
/// source is still working, and a test source may read the body from that other side.
/// </summary>
internal sealed class RecordingStream : Stream
{
    private readonly List<string> _sends = [];

    /// <summary>
    /// How many of the coming flushes fail AFTER their write has taken the bytes — the shape
    /// of a search deadline firing inside <c>Body.FlushAsync</c> while Kestrel's pipe already
    /// holds the frame.
    /// </summary>
    public int FlushFailuresLeft;

    /// <summary>How many flushes actually failed, so a test can prove the fault happened.</summary>
    public int FlushFailures;

    private readonly List<(string Needle, TaskCompletionSource Sent)> _waiters = [];

    /// <summary>Armed by <see cref="NextCallEntered"/>; guarded by <c>_sends</c>.</summary>
    private TaskCompletionSource? _nextCall;

    public int    SendCount   { get { lock (_sends) return _sends.Count; } }
    public string SendAt(int i) { lock (_sends) return _sends[i]; }
    public string All()         { lock (_sends) return string.Concat(_sends); }
    public void   Clear()       { lock (_sends) _sends.Clear(); }

    /// <summary>
    /// Completes when a write carrying <paramref name="needle"/> reaches this body — at once if one
    /// already has. Completed from inside that write, so a test waiting on it learns of the send the
    /// moment it happens instead of polling for it against a clock.
    /// </summary>
    public Task WhenSent(string needle)
    {
        lock (_sends)
        {
            foreach (string s in _sends)
                if (s.Contains(needle, StringComparison.Ordinal)) return Task.CompletedTask;
            var sent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Add((needle, sent));
            return sent.Task;
        }
    }

    /// <summary>
    /// Completes when the NEXT <c>WriteAsync</c> or <c>FlushAsync</c> is entered, a refused or failing
    /// call included: entering is what shows a send has started. Armed when asked, so calls made
    /// before do not count. A scan step that awaits it stays pending until the writer's send reaches
    /// the body, however long the writer takes to look at the step, where a fixed delay ended the
    /// step without a send whenever the writer stalled longer. Its continuation runs asynchronously,
    /// never inside the writer's call.
    /// </summary>
    public Task NextCallEntered()
    {
        lock (_sends)
            return (_nextCall ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
    }

    private void Entered()
    {
        TaskCompletionSource? entered;
        lock (_sends)
        {
            entered   = _nextCall;
            _nextCall = null;
        }
        entered?.TrySetResult();
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        string s = Encoding.UTF8.GetString(buffer);
        lock (_sends)
        {
            _sends.Add(s);
            for (int i = _waiters.Count - 1; i >= 0; i--)
            {
                if (!s.Contains(_waiters[i].Needle, StringComparison.Ordinal)) continue;
                _waiters[i].Sent.TrySetResult();
                _waiters.RemoveAt(i);
            }
        }
    }
    // Both refuse a token that is ALREADY cancelled before they take anything, as Kestrel's
    // response pipe does (HttpResponsePipeWriter.ValidateState). A body that took the bytes
    // anyway would hide a writer that throws away rows it never offered.
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
    {
        Entered();
        if (ct.IsCancellationRequested) return ValueTask.FromCanceled(ct);
        Write(buffer.Span);
        return ValueTask.CompletedTask;
    }
    public override Task FlushAsync(CancellationToken ct)
    {
        Entered();
        if (ct.IsCancellationRequested) return Task.FromCanceled(ct);
        lock (_sends)
        {
            if (FlushFailuresLeft <= 0) return Task.CompletedTask;
            FlushFailuresLeft--;
            FlushFailures++;
        }
        return Task.FromCanceled(new CancellationToken(canceled: true));
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => 0; set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override int Read(byte[] b, int o, int c) => throw new NotSupportedException();
    public override long Seek(long o, SeekOrigin r) => throw new NotSupportedException();
    public override void SetLength(long v) => throw new NotSupportedException();
    public override void Write(byte[] b, int o, int c) => Write(b.AsSpan(o, c));
}

/// <summary>
/// A body that behaves like a socket under a slow client — every write and flush completes
/// ASYNCHRONOUSLY, and a flush can be made to stall until released or cancelled — and that
/// watches who touches it: operations started while no writer call was in progress, operations
/// still in flight, and the most that ever overlapped.
///
/// <para>The recording stream cannot show any of that. A body that completes inline finishes
/// every send before the call that started it can even observe it, so a send that outlives its
/// call, or overlaps another, never has the chance to.</para>
/// </summary>
internal sealed class ProbeStream : Stream
{
    private readonly Lock          _lock = new();
    private readonly StringBuilder _all  = new();
    private int _inFlight;
    private int _operations;
    private int _outsideCalls;
    private int _maxConcurrent;

    /// <summary>Set by the test around every writer call it awaits.</summary>
    public volatile bool CallerInside;

    /// <summary>When set, every flush waits for it, or for its own token — a client that has stopped reading.</summary>
    public volatile TaskCompletionSource? FlushStall;

    /// <summary>
    /// Completed by a flush the moment it starts waiting on <see cref="FlushStall"/>: the send's bytes
    /// have been offered and it is now stuck, so a test can cancel it THERE rather than after a guess
    /// at how long getting there takes.
    /// </summary>
    public TaskCompletionSource FlushStalled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public int Operations    => Volatile.Read(ref _operations);
    public int OutsideCalls  => Volatile.Read(ref _outsideCalls);
    public int InFlight      => Volatile.Read(ref _inFlight);
    public int MaxConcurrent => Volatile.Read(ref _maxConcurrent);

    public string All() { lock (_lock) return _all.ToString(); }

    private void Enter()
    {
        Interlocked.Increment(ref _operations);
        if (!CallerInside) Interlocked.Increment(ref _outsideCalls);
        int now = Interlocked.Increment(ref _inFlight);
        int seen;
        while (now > (seen = Volatile.Read(ref _maxConcurrent))
               && Interlocked.CompareExchange(ref _maxConcurrent, now, seen) != seen) { }
    }

    private void Exit() => Interlocked.Decrement(ref _inFlight);

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
    {
        Enter();
        try
        {
            string s = Encoding.UTF8.GetString(buffer.Span);   // taken at once, as Kestrel's pipe takes it
            lock (_lock) _all.Append(s);
            await Task.Yield();
        }
        finally { Exit(); }
    }

    public override async Task FlushAsync(CancellationToken ct)
    {
        Enter();
        try
        {
            await Task.Yield();
            if (FlushStall is { } stall)
            {
                FlushStalled.TrySetResult();
                await stall.Task.WaitAsync(ct);
            }
        }
        finally { Exit(); }
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => 0; set => throw new NotSupportedException(); }
    public override void Flush() => throw new InvalidOperationException("the writer flushes asynchronously");
    public override int Read(byte[] b, int o, int c) => throw new NotSupportedException();
    public override long Seek(long o, SeekOrigin r) => throw new NotSupportedException();
    public override void SetLength(long v) => throw new NotSupportedException();
    public override void Write(byte[] b, int o, int c) => throw new InvalidOperationException("the writer writes asynchronously");
}

/// <summary>A DTO for the frames that still go out as one.</summary>
internal sealed class ProbeDto
{
    public double X { get; set; }
}

internal static class SseRows
{
    /// <summary>
    /// A writer whose hold rules read a clock that never moves, so a row the rules would buffer is
    /// buffered however long the test takes between two calls. On the wall clock a 100 ms stall
    /// anywhere between a send and the next row — a GC, a first JIT, a descheduled thread on a
    /// loaded two-core runner — sent that row on write instead, and every assertion about what was
    /// still buffered went with it.
    /// </summary>
    public static SseJsonWriter OnFrozenClock(Stream body) => new(body, new ManualTimeProvider());

    public static LogEvent Event(uint seq) => new()
    {
        Id              = new EventId(0u, seq),
        Timestamp       = new DateTimeOffset(2026, 9, 14, 12, 0, 0, TimeSpan.Zero).AddSeconds(seq),
        Level           = LogLevel.Information,
        MessageTemplate = "row " + seq,
    };

    /// <summary>The marker a row frame carries; the closing quote keeps row 1 from matching row 10.</summary>
    public static string Row(uint seq) => $"\"@mt\":\"row {seq}\"";

    public static int Count(string haystack, string needle) => haystack.Split(needle).Length - 1;

    /// <summary>A contract for <see cref="ProbeDto"/>, standing in for a source-generated one.</summary>
    public static JsonTypeInfo<ProbeDto> ProbeContract { get; } =
        (JsonTypeInfo<ProbeDto>)JsonSerializerOptions.Default.GetTypeInfo(typeof(ProbeDto));
}
