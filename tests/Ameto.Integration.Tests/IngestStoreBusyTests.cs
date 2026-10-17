using System.Buffers;
using System.Text.Json;
using MessagePack;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Storage;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Integration.Tests;

/// <summary>
/// A request is answered once its events are in the store; when the store could take none of them
/// — no room within <c>Ingestion.BackPressureWait</c>, or the engine shutting down — the request is
/// answered 503 with <c>Retry-After</c> and the counts, never a 200 with a "dropped" count no client
/// retries. Nothing was written, so the retry the 503 asks for writes the batch once.
///
/// <para>Driven with a closed engine: the deterministic way to an engine that takes nothing. The
/// road through a full tier with every flush slot busy is <c>IngestWriteAndSpillTests</c>'s.</para>
/// </summary>
public sealed class IngestStoreBusyTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-ingest-busy-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task A_batch_the_store_takes_none_of_is_answered_503_with_retry_after_and_the_counts()
    {
        Directory.CreateDirectory(_dir);
        var opts    = new ServerOptions { DataDirectory = _dir };
        var storage = new StorageEngine(
            Microsoft.Extensions.Options.Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        await storage.DisposeAsync();

        var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);
        var ctx      = Post(Clef(3));

        await endpoint.HandleAsync(ctx);

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, ctx.Response.StatusCode);
        Assert.Equal(IngestionEndpoint.RetryAfterSeconds, ctx.Response.Headers.RetryAfter.ToString());
        ctx.Response.Body.Position = 0;
        using var doc = JsonDocument.Parse(ctx.Response.Body);
        Assert.Equal(0, doc.RootElement.GetProperty("ingested").GetInt32());
        Assert.Equal(3, doc.RootElement.GetProperty("dropped").GetInt32());
        Assert.Equal(0, endpoint.AcceptedTotal);
    }

    /// <summary>
    /// The drop markers are written AFTER the client's events. A batch whose first event is over the
    /// size limit used to have its marker staged in that event's place, so a store with room for one
    /// event took the marker and none of the client's records: the request was answered 503 although
    /// something was written, and the retry wrote the Error marker again. The store's one event is
    /// now the client's first record, and the answer a partial 200, as for any batch the store ran
    /// out of room part way through.
    /// </summary>
    [Fact]
    public async Task A_store_with_room_for_one_event_takes_the_clients_first_record_not_its_drop_marker()
    {
        var opts = Options(backPressureWait: TimeSpan.FromMilliseconds(100));
        await using var storage = Engine(opts);
        var written = CaptureWrites(storage);
        TakeOnly(storage, events: 1);   // then each write fails, and is retried until the wait runs out
        try
        {
            var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);
            var ctx      = Post(ClefOversizedFirst(3));

            await endpoint.HandleAsync(ctx);

            Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
            var (ingested, dropped) = Counts(ctx);
            Assert.Equal(1, ingested);
            Assert.Equal(3, dropped);                     // the oversized one, and the two the store had no room for
            Assert.Equal(new[] { "busy probe {i}" }, written);   // the client's record — no drop marker
            Assert.Equal(1, endpoint.DroppedOversized);   // a 200 ends the request: counted
        }
        finally { storage._afterWriteGate = null; }
    }

    /// <summary>
    /// A batch of nothing but events over the size limit is answered for good — a 200 with every
    /// one dropped — however much room the store had. No retry could store any of its events, only
    /// send them again, each over the limit, to write the markers. It was a 503 whenever a marker
    /// was left unwritten, the one already written included (then written again by the retry).
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task A_batch_of_nothing_but_events_over_the_size_limit_is_answered_200_whatever_the_store_took(int room)
    {
        var opts = Options(backPressureWait: TimeSpan.FromMilliseconds(100));
        await using var storage = Engine(opts);
        var written = CaptureWrites(storage);
        TakeOnly(storage, events: room);
        try
        {
            var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);
            var ctx      = Post(ClefOversizedOnly(2));

            await endpoint.HandleAsync(ctx);

            Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
            var (ingested, dropped) = Counts(ctx);
            Assert.Equal(0, ingested);
            Assert.Equal(2, dropped);
            Assert.Equal(room, written.Count);            // the markers there was room for
            Assert.Equal(2, endpoint.DroppedOversized);
        }
        finally { storage._afterWriteGate = null; }
    }

    /// <summary>
    /// The promise end to end: a store that takes none of a batch with an event over the size limit
    /// writes nothing of it — not its drop marker either — and answers 503; the retry, with room,
    /// writes the client's records and the marker once each.
    /// </summary>
    [Fact]
    public async Task A_503_wrote_nothing_and_its_retry_writes_the_drop_marker_once()
    {
        var opts = Options(backPressureWait: TimeSpan.FromMilliseconds(100));
        await using var storage = Engine(opts);
        var written  = CaptureWrites(storage);
        var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);

        TakeOnly(storage, events: 0);
        var refused = Post(ClefOversizedFirst(2));
        try { await endpoint.HandleAsync(refused); }
        finally { storage._afterWriteGate = null; }

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, refused.Response.StatusCode);
        Assert.Empty(written);

        var retried = Post(ClefOversizedFirst(2));
        await endpoint.HandleAsync(retried);

        Assert.Equal(StatusCodes.Status200OK, retried.Response.StatusCode);
        Assert.Equal((2, 1), Counts(retried));
        Assert.Equal(1, written.Count(t => t.Contains("dropped an oversized log event", StringComparison.Ordinal)));
        Assert.Equal(3, written.Count);
    }

    /// <summary>
    /// An event over the size limit is counted once, by the answer that ends its request — not once
    /// per attempt. A batch answered 503 is staged again by every retry, so one oversized event in a
    /// batch retried each second for three minutes read as ~180 drops. (Its Warning, which names the
    /// sender, is still logged as each attempt is read: a log call that runs out of memory there is
    /// answered 503 before anything is written, which OtlpOutOfMemoryTests pins.)
    /// </summary>
    [Fact]
    public async Task An_event_over_the_size_limit_is_counted_by_the_answer_that_ends_its_request_not_by_each_retry()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions
        {
            DataDirectory = _dir,
            Ingestion     = new IngestionOptions { MaxEventPayloadBytes = 64 },
        };
        var storage = new StorageEngine(
            Microsoft.Extensions.Options.Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        await storage.DisposeAsync();   // takes nothing: every attempt is answered 503

        var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);
        for (int attempt = 0; attempt < 3; attempt++)
        {
            var ctx = Post(ClefOversizedFirst(2));
            await endpoint.HandleAsync(ctx);
            Assert.Equal(StatusCodes.Status503ServiceUnavailable, ctx.Response.StatusCode);
        }
        Assert.Equal(0, endpoint.DroppedOversized);

        // The attempt that is answered for good counts it, once.
        await using var open = new StorageEngine(
            Microsoft.Extensions.Options.Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        var accepting = new IngestionEndpoint(open, open.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);
        var last      = Post(ClefOversizedFirst(2));
        await accepting.HandleAsync(last);
        Assert.Equal(StatusCodes.Status200OK, last.Response.StatusCode);
        Assert.Equal(1, accepting.DroppedOversized);
    }

    /// <summary>
    /// The Debug line that says what the store did not write runs after the write, and is no reason
    /// to fail the request: one that ran out of memory there (its arguments box) made the answer a
    /// 500 — for a batch the store had partly written, with the count of that part lost, and a Seq
    /// client's retry writing it twice. Here the store takes nothing, so the answer is the 503.
    /// </summary>
    [Fact]
    public async Task A_log_line_that_runs_out_of_memory_after_the_write_does_not_cost_the_answer()
    {
        var opts    = Options(backPressureWait: TimeSpan.FromMilliseconds(100));
        var storage = Engine(opts);
        await storage.DisposeAsync();   // takes nothing

        var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, new OutOfMemoryOnDebug());
        var ctx      = Post(Clef(3));

        await endpoint.HandleAsync(ctx);

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, ctx.Response.StatusCode);
        Assert.Equal((0, 3), Counts(ctx));
    }

    /// <summary>
    /// Round-6 review R5-M4. This test pinned the opposite — "a 500, then written" — as accepted. The
    /// commit's first real suspension was its await of the write, after the write began; a box that
    /// could not be allocated there (round-3 R1a-3) was waited out on the request's thread for the
    /// write's wait and a second more, and then the failure went up — a 500 — while the write went on
    /// and stored the batch, which a Seq client's retry stored a second time. The handler awaits the
    /// write itself now, on the box it made first, and nothing between the write's start and its end
    /// can fail but the write. Here every first suspension of a frame of the endpoint below the
    /// handler fails, as the box allocation would; the store faults once, so the write waits, and its
    /// next attempt lands only when the test says — two seconds on, past the write's 100 ms wait and
    /// the second a commit used to wait it out for.
    /// </summary>
    [Fact]
    public async Task A_commit_whose_write_outlives_its_wait_is_answered_from_what_the_write_did()
    {
        var opts = Options(backPressureWait: TimeSpan.FromMilliseconds(100));
        await using var storage = Engine(opts);
        var written = CaptureWrites(storage);
        using var release = new ManualResetEventSlim(false);
        int attempts = 0;
        storage._afterWriteGate = () =>
        {
            int n = Interlocked.Increment(ref attempts);
            if (n == 1) throw new OutOfMemoryException("test: a fault, so the write waits");
            if (n == 2) release.Wait(TimeSpan.FromSeconds(30));   // the next attempt lands only when the test says
        };
        var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);
        try
        {
            var  ctx    = Post(Clef(3));
            int  failed = 0;
            Task handled;
            OwnerAsync.FirstSuspensionFailsForTest.Value = t =>
            {
                if (!BelowTheHandler(t)) return false;
                Interlocked.Increment(ref failed);
                return true;
            };
            try { handled = endpoint.HandleAsync(ctx); }
            finally { OwnerAsync.FirstSuspensionFailsForTest.Value = null; }

            bool   endedEarly = await Task.WhenAny(handled, Task.Delay(2_000)) == handled;
            string how        = !endedEarly ? "" : handled.IsFaulted ? "a 500: " + handled.Exception!.InnerException!.GetType().Name
                                                                     : ctx.Response.StatusCode.ToString();
            release.Set();
            try { await handled.WaitAsync(TimeSpan.FromSeconds(30)); }
            catch (OutOfMemoryException) when (endedEarly) { /* told below */ }
            var until = DateTime.UtcNow.AddSeconds(30);
            while (DateTime.UtcNow < until)
            {
                lock (written) if (written.Count >= 3) break;
                await Task.Delay(10);
            }
            await Task.Delay(100);   // nothing more comes
            int stored;
            lock (written) stored = written.Count;

            Assert.True(!endedEarly, $"the request ended ({how}) while its write was still going, and the write then stored {stored} event(s): a Seq client's retry stores them a second time");
            Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
            Assert.Equal((3, 0), Counts(ctx));
            Assert.Equal(3, stored);
            Assert.Equal(3, endpoint.AcceptedTotal);
            Assert.True(Volatile.Read(ref failed) == 0, "a frame of the endpoint below the handler was boxed after its write began");
        }
        finally
        {
            release.Set();
            storage._afterWriteGate = null;
        }
    }

    /// <summary>
    /// A frame of the ingest endpoint below the CLEF handler, whose first suspension the seam fails: one that
    /// stands between the request and the store (the commit's own, before round-6 R5-M4) is boxed there.
    /// </summary>
    internal static bool BelowTheHandler(Type stateMachine) =>
        (stateMachine.DeclaringType == typeof(IngestionEndpoint) && !stateMachine.Name.Contains("<HandleAsync>", StringComparison.Ordinal))
        || stateMachine.DeclaringType == typeof(LogIngestBatch);

    /// <summary>
    /// OwnerAsync's seam, armed for one first suspension of the method whose state machine's name
    /// carries <paramref name="method"/> (as "&lt;HandleAsync&gt;"): it throws there as a box that cannot
    /// be allocated does, every finally of the method skipped. <paramref name="onFail"/> runs as it fires.
    /// </summary>
    private static Func<Type, bool> FirstSuspensionFailsOnce(string method, Action? onFail = null)
    {
        int armed = 1;
        return t =>
        {
            if (!t.Name.Contains(method, StringComparison.Ordinal) || Interlocked.Exchange(ref armed, 0) == 0) return false;
            onFail?.Invoke();
            return true;
        };
    }

    /// <summary>
    /// Round-5 review M-D and H-G. The handler's first real suspension, where its state machine is boxed,
    /// was its await of the commit when the body was read without suspending (here, from memory) — after
    /// the batch was decoded and its write had begun. A box that could not be allocated there was waited
    /// out on the caller's thread (ce9e57c): a pool thread the commit itself needed, so a burst on two
    /// cores stalled the whole pool for seconds and the bound then expired into a 500. The handler is boxed
    /// before it owns anything now: that failure fails it at once, nothing decoded, reserved or written —
    /// a 500, which a Seq client retries — and no thread is held. The store faults throughout here, so a
    /// commit waits its whole BackPressureWait.
    /// </summary>
    [Fact]
    public async Task A_receiver_whose_first_suspension_cannot_be_boxed_takes_nothing_and_holds_no_thread()
    {
        var opts = Options(backPressureWait: TimeSpan.FromSeconds(5));
        await using var storage = Engine(opts);
        var written  = CaptureWrites(storage);
        int attempts = 0;
        storage._afterWriteGate = () =>
        {
            Interlocked.Increment(ref attempts);
            throw new OutOfMemoryException("test: a fault, so a write waits");
        };
        var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);
        try
        {
            var  ctx = Post(Clef(3));
            var  sw  = System.Diagnostics.Stopwatch.StartNew();
            Task handled;
            OwnerAsync.FirstSuspensionFailsForTest.Value = FirstSuspensionFailsOnce("<HandleAsync>");
            try { handled = endpoint.HandleAsync(ctx); }   // what the caller's thread spends before it has the task back
            finally { OwnerAsync.FirstSuspensionFailsForTest.Value = null; }
            long heldMs = sw.ElapsedMilliseconds;

            Assert.True(heldMs < 1_000, $"the handler held its caller's thread {heldMs} ms, waiting a commit out");
            await Assert.ThrowsAsync<OutOfMemoryException>(() => handled);
            Assert.Equal(0, Volatile.Read(ref attempts));   // nothing was decoded, so nothing was written
            Assert.Empty(written);
            Assert.Equal(0, endpoint.DecodedBytesInFlight);
        }
        finally { storage._afterWriteGate = null; }
    }

    /// <summary>
    /// Round-4 review L3. The commit read the batch AFTER its write — the marker accounting from its
    /// event count — while the write's lease ends inside the write, where a dispose asked meanwhile
    /// (deferred to that end) zeroes it: the client's events read as none, every marker as written, the
    /// answer's counts wrong. It reads what it needs before the write now.
    /// </summary>
    [Fact]
    public async Task A_batch_disposed_while_its_write_waits_is_still_answered_with_the_right_counts()
    {
        var opts = Options(backPressureWait: TimeSpan.FromMilliseconds(500));
        await using var storage = Engine(opts);
        var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);
        var batch    = endpoint.BeginBatch();
        for (int i = 0; i < 3; i++)
            Assert.True(batch.TryIngestRaw(DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information, "client {N}"u8, [0x80], 0, 0, 0, "svc"u8));
        Assert.False(batch.TryIngestRaw(DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information, "big {N}"u8, new byte[100], 0, 0, 0, "svc"u8));

        // The first attempt faults (so the write waits, and the dispose is asked meanwhile); the three
        // client events then go in; the marker after them finds no room until the wait runs out.
        int calls = 0;
        storage._afterWriteGate = () =>
        {
            int n = Interlocked.Increment(ref calls);
            if (n == 1 || n > 4) throw new OutOfMemoryException("test: no room");
        };
        storage._beforeRoomWaitForTest = () => batch.Dispose();   // asked while the write holds it: deferred to its end
        try
        {
            var result = await batch.CommitAsync();
            Assert.Equal(new LogIngestResult(3, 1, Busy: false), result);   // three in; the oversized one dropped, its marker not written
        }
        finally
        {
            storage._afterWriteGate        = null;
            storage._beforeRoomWaitForTest = null;
            batch.Dispose();
        }
    }

    /// <summary>
    /// Round-4 review L5. A drop whose marker the store had no room for is named whatever the
    /// once-a-second limit says — and a body of thousands of them wrote thousands of Warnings, a flood
    /// of the bounded file-log queue and the Event Log. A request names its first twenty, then one line
    /// with the rest's count.
    /// </summary>
    [Fact]
    public async Task A_request_names_its_first_unwritten_markers_and_sums_up_the_rest_in_one_line()
    {
        var opts   = Options(backPressureWait: TimeSpan.FromMilliseconds(100));
        await using var storage = Engine(opts);
        var logger = new CountingLogger();
        var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, logger);
        using var batch = endpoint.BeginBatch();
        Assert.True(batch.TryIngestRaw(DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information, "client {N}"u8, [0x80], 0, 0, 0, "svc"u8));
        for (int i = 0; i < 100; i++)
            Assert.False(batch.TryIngestRaw(DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information, "big {N}"u8, new byte[100], 0, 0, 0, "svc"u8));

        int writes = 0;
        storage._afterWriteGate = () =>
        {
            if (Interlocked.Increment(ref writes) > 1) throw new OutOfMemoryException("test: no room after the client's event");
        };
        try
        {
            var result = await batch.CommitAsync();
            Assert.Equal(new LogIngestResult(1, 100, Busy: false), result);
        }
        finally { storage._afterWriteGate = null; }

        Assert.Equal(IngestionEndpoint.UnwrittenMarkerWarningsPerRequest + 1, logger.Warnings);
        Assert.Contains("80 more oversized", logger.Last);
    }

    /// <summary>
    /// Round-5 review L-8: the same when written and unwritten markers mix, which no test reached. The
    /// twenty a request names are its first twenty UNWRITTEN ones, counted from the first marker the store
    /// had no room for — here thirty are written (named at most once a second, as their markers name them)
    /// and seventy are not: twenty named, then one line with fifty.
    /// </summary>
    [Fact]
    public async Task A_request_whose_markers_are_partly_written_names_its_first_unwritten_ones_and_sums_up_the_rest()
    {
        var opts   = Options(backPressureWait: TimeSpan.FromMilliseconds(100));
        await using var storage = Engine(opts);
        var logger = new CountingLogger();
        var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, logger);
        using var batch = endpoint.BeginBatch();
        Assert.True(batch.TryIngestRaw(DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information, "client {N}"u8, [0x80], 0, 0, 0, "svc"u8));
        for (int i = 0; i < 100; i++)
            Assert.False(batch.TryIngestRaw(DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information, "big {N}"u8, new byte[100], 0, 0, 0, "svc"u8));

        int writes = 0;
        storage._afterWriteGate = () =>
        {
            if (Interlocked.Increment(ref writes) > 31) throw new OutOfMemoryException("test: no room after thirty markers");
        };
        try
        {
            var result = await batch.CommitAsync();
            Assert.Equal(new LogIngestResult(1, 100, Busy: false), result);
        }
        finally { storage._afterWriteGate = null; }

        Assert.Equal(IngestionEndpoint.UnwrittenMarkerWarningsPerRequest,
                     logger.Lines.Count(l => l.Contains("the store had no room for its marker", StringComparison.Ordinal)));
        Assert.Contains("50 more oversized", logger.Last);
    }

    /// <summary>Counts the Warnings written, and keeps the last one.</summary>
    private sealed class CountingLogger : Microsoft.Extensions.Logging.ILogger<IngestionEndpoint>
    {
        public int     Warnings;
        public string? Last;
        public readonly System.Collections.Concurrent.ConcurrentQueue<string> Lines = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
                                TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel != Microsoft.Extensions.Logging.LogLevel.Warning) return;
            Interlocked.Increment(ref Warnings);
            Last = formatter(state, exception);
            Lines.Enqueue(Last);
        }
    }

    /// <summary>Throws an <see cref="OutOfMemoryException"/> from every Debug line.</summary>
    private sealed class OutOfMemoryOnDebug : Microsoft.Extensions.Logging.ILogger<IngestionEndpoint>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
                                TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == Microsoft.Extensions.Logging.LogLevel.Debug)
                throw new OutOfMemoryException("test: the Debug line ran out of memory");
        }
    }

    private ServerOptions Options(TimeSpan backPressureWait)
    {
        Directory.CreateDirectory(_dir);
        return new ServerOptions
        {
            DataDirectory = _dir,
            Ingestion     = new IngestionOptions { MaxEventPayloadBytes = 64, BackPressureWait = backPressureWait },
        };
    }

    /// <summary>
    /// Round-6 review R6-M5. The receiver's own await of the write can fail at its registration — out of memory,
    /// past the runtime's own catch — while the write runs on and lands. The request is answered by the throw (a
    /// 500; API.md names it), but the finally that gives the batch back was skipped on that road, and the batch's
    /// reservation of the decoded budget with it: a batch's worth of the budget lost for good, per such request. A
    /// catch around the await gives the batch back, at the write's end (its lease defers the dispose).
    /// </summary>
    [Fact]
    public async Task A_receiver_whose_await_of_the_write_fails_gives_its_batch_back_when_the_write_ends()
    {
        var opts = Options(backPressureWait: TimeSpan.FromSeconds(2));
        await using var storage = Engine(opts);
        int faults = 0;
        storage._afterWriteGate = () =>
        {
            if (Interlocked.Increment(ref faults) <= 2) throw new OutOfMemoryException("test: a fault, so the write waits");
        };
        try
        {
            var endpoint = new IngestionEndpoint(storage, storage.TemplatePool, opts, NullLogger<IngestionEndpoint>.Instance);
            var ctx      = Post(Clef(3));
            int armed    = 1;
            Task handled;
            OwnerAsync.LaterSuspensionFailsForTest.Value = (sm, awaiter) =>
                sm.Name.Contains("<HandleAsync>", StringComparison.Ordinal)
                && awaiter == typeof(System.Runtime.CompilerServices.ValueTaskAwaiter<LogBatchWriteResult>)
                && Interlocked.Exchange(ref armed, 0) == 1;
            try { handled = endpoint.HandleAsync(ctx); }
            finally { OwnerAsync.LaterSuspensionFailsForTest.Value = null; }

            await Assert.ThrowsAsync<OutOfMemoryException>(() => handled);   // answered by the throw: a 500
            Assert.Equal(0, Volatile.Read(ref armed));
            // The write runs on and lands; then the batch is given back.
            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (endpoint.DecodedBytesInFlight != 0 && sw.Elapsed < TimeSpan.FromSeconds(15)) await Task.Delay(20);
            Assert.True(endpoint.DecodedBytesInFlight == 0,
                        $"{endpoint.DecodedBytesInFlight} B of the decoded budget still reserved: the batch was never given back");
            Assert.True(Volatile.Read(ref faults) > 2, "the write did not go on after the await failed");
        }
        finally { storage._afterWriteGate = null; }
    }

    private static StorageEngine Engine(ServerOptions opts) => new(
        Microsoft.Extensions.Options.Options.Create(opts),
        new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
        NullLogger<StorageEngine>.Instance);

    /// <summary>The templates of the events the store writes, in the order it writes them.</summary>
    private static List<string> CaptureWrites(StorageEngine storage)
    {
        var written = new List<string>();
        storage.EventWritten = (_, template) => { lock (written) written.Add(template); };
        return written;
    }

    /// <summary>The store takes <paramref name="events"/> more events, then fails every write — retried until the wait runs out.</summary>
    private static void TakeOnly(StorageEngine storage, int events)
    {
        int writes = 0;
        storage._afterWriteGate = () =>
        {
            if (Interlocked.Increment(ref writes) > events) throw new OutOfMemoryException("test: no room");
        };
    }

    private static (int Ingested, int Dropped) Counts(DefaultHttpContext ctx)
    {
        ctx.Response.Body.Position = 0;
        using var doc = JsonDocument.Parse(ctx.Response.Body);
        return (doc.RootElement.GetProperty("ingested").GetInt32(), doc.RootElement.GetProperty("dropped").GetInt32());
    }

    /// <summary>A CLEF batch of <paramref name="events"/> events, each with properties over a 64-byte limit.</summary>
    private static byte[] ClefOversizedOnly(int events)
    {
        var buf = new ArrayBufferWriter<byte>(512);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(events);
        for (int i = 0; i < events; i++)
        {
            w.WriteMapHeader(2);
            w.Write("@mt");  w.Write("oversized probe");
            w.Write("Blob"); w.Write(new string('x', 200));
        }
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>A CLEF batch whose first event's properties are over a 64-byte limit, then <paramref name="events"/> ordinary ones.</summary>
    private static byte[] ClefOversizedFirst(int events)
    {
        var buf = new ArrayBufferWriter<byte>(512);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(events + 1);
        w.WriteMapHeader(2);
        w.Write("@mt");  w.Write("oversized probe");
        w.Write("Blob"); w.Write(new string('x', 200));
        for (int i = 0; i < events; i++)
        {
            w.WriteMapHeader(2);
            w.Write("@mt"); w.Write("busy probe {i}");
            w.Write("i");   w.Write((long)i);
        }
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private static DefaultHttpContext Post(byte[] body)
    {
        var ctx = new DefaultHttpContext();
        ctx.Request.Method        = "POST";
        ctx.Request.ContentType   = "application/octet-stream";
        ctx.Request.ContentLength = body.Length;
        ctx.Request.Body          = new MemoryStream(body);
        ctx.Response.Body         = new MemoryStream();
        return ctx;
    }

    private static byte[] Clef(int events)
    {
        var buf = new ArrayBufferWriter<byte>(256);
        var w   = new MessagePackWriter(buf);
        w.WriteArrayHeader(events);
        for (int i = 0; i < events; i++)
        {
            w.WriteMapHeader(2);
            w.Write("@mt"); w.Write("busy probe {i}");
            w.Write("i");   w.Write((long)i);
        }
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }
}
