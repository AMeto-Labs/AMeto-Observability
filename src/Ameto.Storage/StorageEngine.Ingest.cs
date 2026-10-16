using Ameto.Core;
using Microsoft.Extensions.Logging;

namespace Ameto.Storage;

// ── THE WRITE PATH: AN EVENT IS ACKNOWLEDGED ONCE IT IS IN A WAL ────────────────────────────────
//
// Logs used to reach this engine through an ingest ring: a request copied each event into a 64 KB
// slab of a native arena, answered 200, and a single drainer thread moved the events into the hot
// tier and its WAL later. Two things followed from that, and both are why the ring is gone.
//
//   - A 200 meant "in process memory". An OOM kill, or any crash, between the answer and the drain
//     lost acknowledged events, as many as the ring held.
//   - The ring's limit was a COUNT of 64 KB slabs (8 192 by default), whatever the events weighed,
//     and an event that found no slab was dropped on the spot — reported to an OTLP exporter as a
//     200 with a "dropped" count, which no exporter retries.
//
// A request now writes its own events, under one writer lock (the hot tier is single-writer), into
// the hot tier and its WAL, and is answered after that. The WAL is mapped, so the write is a copy
// into the page cache — durable against the death of the process, which is what the page cache
// survives; power loss is still bounded by HotTier.WalFlushInterval, as it always was.
//
// When the tier is full the request WAITS for room rather than dropping anything:
//   - the swap that freezes a full tier and installs a fresh one takes milliseconds;
//   - when no flush slot is free — the frozen tiers already hold all the RAM their budget allows,
//     because the flush is behind — the engine SPILLS: writes go to a WAL of their own with no tier
//     in memory (see SpillTarget), acknowledged as durably as any other, and the block is written
//     into segments once a slot is free (Despill). The disk, not a slab count, is the limit then;
//   - only when nothing can take events at all (spilling off, or the disk at its floor) does a
//     request give up, after Ingestion.BackPressureWait, and say so with a retryable 503.

public sealed partial class StorageEngine
{
    /// <summary>
    /// Serialises every write into the live tier, its WAL and a spill target. The hot tier is
    /// single-writer by design (<see cref="HotTierSegment"/>), and with the drainer gone the
    /// writers are the request threads themselves. Also taken — briefly, never across I/O — by a
    /// swap installing a new <see cref="WriteState"/> and by shutdown's fence, so a writer never
    /// sees a tier without the WAL that journals it, and nothing is installed or freed under a
    /// write in progress.
    ///
    /// <para>Lock order: <c>_flushLock</c> (the swap), then this, then <c>_frozenLock</c>. A writer
    /// holding this takes no other lock: <see cref="ScheduleFlush"/> only queues work.</para>
    /// </summary>
    private readonly System.Threading.Lock _writerLock = new();

    /// <summary>What one write came to.</summary>
    private enum WriteOutcome : byte
    {
        /// <summary>In the live tier and its WAL, or in a spill target's WAL.</summary>
        Written,
        /// <summary>No room right now; a swap has been asked for. The same event may be written once there is.</summary>
        NoRoom,
        /// <summary>The write path is closed for shutdown.</summary>
        Closed,
        /// <summary>No tier can ever hold it: a payload larger than one hot-tier chunk.</summary>
        TooLarge,
        /// <summary>
        /// The write threw — out of memory allocating a tier chunk or its template slots, a swap
        /// request that could not be queued — and wrote nothing: <see cref="WriteLocked"/> commits
        /// last and nothing after its commit can throw. Waited out and tried again like
        /// <see cref="NoRoom"/>.
        /// </summary>
        Faulted,
    }

    /// <summary>
    /// Events one writer writes per take of <see cref="_writerLock"/>. A batch of thousands lets
    /// the other requests in between runs rather than holding them for its whole length; at about
    /// a microsecond an event a run holds the lock for a fraction of a millisecond.
    /// </summary>
    private const int WriteRunEvents = 256;

    /// <summary>
    /// How long a writer waiting for room waits on the signal before it writes again — the safety
    /// net under the wake-ups (<see cref="SignalRoom"/>, and the swap requests that lead to them).
    /// Internal so a test can lengthen it until only a real wake-up gets a writer going.
    /// </summary>
    internal int _roomRecheckMs = 100;

    /// <summary>
    /// Writes one event — the single-event form of <see cref="WriteBatchAsync"/>, for callers with
    /// one event and no wish to wait. False when it was not written: no room right now (a swap has
    /// been asked for), the write path is closed, or no tier can hold an event that large. A throw
    /// (out of memory in the tier) also means nothing was written.
    /// </summary>
    public bool TryWrite(in LogEventHeader header, ReadOnlySpan<byte> propertiesPayload, string? template = null, ExceptionInfo? exception = null,
                         string? service = null)
    {
        lock (_writerLock) return WriteLocked(header, propertiesPayload, template, exception, service) == WriteOutcome.Written;
    }

    /// <summary>
    /// Writes <paramref name="batch"/> in order, waiting for room when there is none, and returns
    /// once every event is written or the wait has run out. An event written is in the live tier
    /// and its WAL, or in a spill target's WAL — acknowledged-durable either way.
    ///
    /// <para>Stops at the first event it could not write within <paramref name="maxWait"/> (or at
    /// shutdown): what follows it is reported as not written, never skipped over, so a caller
    /// answering 503 for a batch that wrote nothing knows that a retry duplicates nothing. An event
    /// no tier can hold is skipped and counted apart (see <see cref="LogBatchWriteResult"/>).</para>
    ///
    /// <para>NEVER THROWS for a write that fails. A write that throws — out of memory allocating a
    /// tier chunk, say — wrote nothing (<see cref="WriteLocked"/> commits last), and is waited out
    /// and tried again like one that found no room — its own attempt counted against the wait, and
    /// the pause after each fault in a row doubled — then reported as not written with everything
    /// after it. It used to escape: the request was answered 500 (gRPC UNKNOWN), which no OTLP
    /// exporter retries, the count of what was already written was lost, and a Seq client, which
    /// does retry a 500, wrote the batch's prefix a second time.</para>
    /// </summary>
    /// <param name="maxWait">
    /// How long the batch may wait, in all, for room: the time actually spent waiting, summed over
    /// its waits. It used to be a deadline from the call's start, so writing a long batch and
    /// queueing for the writer lock behind other requests spent it too: a 40 000-event batch
    /// interleaved with a burst of others could be past it when the next tier filled, and gave up
    /// at that routine swap — milliseconds from room — without waiting at all, its tail reported
    /// as dropped in a 200 no client retries.
    /// </param>
    /// <param name="ct">The request's: a client that has gone stops the wait, and nothing after the
    /// events already written is written — it is looked at before every run of writes, so a pause that a
    /// starved pool ended late, past the client's going, writes nothing more either.</param>
    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(OwnerValueTaskMethodBuilder<>))]
    public async ValueTask<LogBatchWriteResult> WriteBatchAsync(LogWriteBatch batch, TimeSpan maxWait, CancellationToken ct = default)
    {
        // Held until this method ends — in this method, not a wrapper, whose own await could throw
        // while this one is still suspended: a dispose meanwhile (the request's own await threw, and
        // its finally disposed the batch) is deferred to the end of the write (see LogWriteBatch).
        //
        // And given back by plain code, not a finally — nor is the writer lock released in one here
        // (WriteRunLocked). This method's first real suspension, the wait for room, boxes its state
        // machine, and a box that cannot be allocated is caught below and waited out with a sleep; but
        // the compiler guards every finally of the method with the state that suspension had already
        // set, so from then on each was skipped: the next run's release of the writer lock — held for
        // good, every writer after it waiting for ever — and the lease's end, the batch never given back.
        batch.Lease();
        LogBatchWriteResult result;
        try
        {
            int  next     = 0, written = 0, refused = 0;
            bool waited   = false;
            long budgetMs = (long)Math.Max(0, maxWait.TotalMilliseconds);
            long waitedMs = 0;    // spent of the wait: its waits, and while the store is faulting all of its time
            int  faults   = 0;    // its own faulted attempts in a row: the pause after each one doubles
            // While the store is faulting — this request's own attempt failed, or another's did in the
            // last second — the request's wall time is its wait, the time it queues for the writer lock
            // included, until a write of its lands: from countFrom (TickCount64), on top of countWaited.
            // Behind other requests' faulting attempts — each holding the lock through the full
            // collection a failing rent forces — six to eight requests took three to four times a
            // 500 ms wait, and forty would take ~50 s, past a Collector's 5 s timeout, whose resend writes
            // the prefix twice. A healthy store keeps the rule above: only a batch's waits count.
            long countFrom   = -1;
            long countWaited = 0;
            // Within that counting: when its own first fault was (-1: none — it is counting the store's
            // episode alone), and what its room waits took. A write that lands keeps only those charged
            // (round-5 review L-6).
            long ownFrom     = -1;
            long countSlices = 0;
            // False once anything was caught at the await for room below: from then on this call never
            // awaits again (round-5 review M-C). A suspension whose box was made and whose registration
            // then threw — the TPL event source, async debugging, a second continuation — left the box in
            // the builder with a copy of this method's state taken at that suspension; the method went on
            // in its own copy (a Release build's struct state machine), and the next suspension that
            // succeeded reused the box and resumed that stale copy: every event written since, written
            // again (16 384 events twice for a batch of 16 784, in the review's Release probe).
            bool awaitable = true;

            while (next < batch.Count)
            {
                int runFrom = next;

                // A client that has gone has nothing more written (round-6 review R5-M4, with the §4 item "a
                // room-wait slice that overruns on a starved pool is counted only after it"). After a pause for
                // room the writer used to go round and write before it looked at anything: a slice that timed
                // out, then a client that went — its own timeout, while a starved pool had not yet resumed the
                // writer — and the batch was written for nobody, and stored again by the client's retry. Looked
                // at before every run, the first included: one volatile read.
                if (ct.IsCancellationRequested) break;

                // While the store is faulting the lock is taken within what is left of the wait, its own
                // wait counted from when the request came to it (even when the first fault is only seen
                // part way through that wait), and a cancelled request stops queueing.
                if (!_writerLock.TryEnter() &&
                    !WaitForWriterLock(Environment.TickCount64, budgetMs, waitedMs, nothingWritten: written == 0, ref countFrom, ref countWaited, ct))
                    break;
                var outcome = WriteRunLocked(batch, ref next, ref written, ref refused, out long runStart, out var full, out var room);
                _afterWriteRunForTest?.Invoke();

                // A write landed: the time up to this attempt counted, and the request is a healthy
                // one again — and the pause after its next fault starts from the first. The count of
                // faults in a row used to be reset only by a faulting run that had written first, and
                // a later fault doubled a stale count.
                if (next > runFrom)
                {
                    // What stays charged of the counting: its own run of faults, from the first — and of a
                    // count that was the store's episode alone (it queued while others faulted), only the
                    // waits it did. The whole of its queueing behind the episode stayed charged, and a
                    // healthy request then gave up at a routine wait for room later in its batch: a 200
                    // with its tail dropped, which no client retries (round-5 review L-6).
                    if (countFrom >= 0)
                        waitedMs = countWaited + (ownFrom >= 0 ? Math.Max(0, runStart - ownFrom) : countSlices);
                    countFrom   = -1;
                    ownFrom     = -1;
                    countSlices = 0;
                    faults      = 0;
                }

                if (outcome is WriteOutcome.Written or WriteOutcome.TooLarge) continue;   // the run is done; let others in
                if (outcome == WriteOutcome.Closed) break;

                if (outcome == WriteOutcome.Faulted)
                {
                    // A faulted attempt is part of the wait, not free time between waits: on a heap at its
                    // limit the attempt itself — a failing 128 KB rent and the full collection it forces —
                    // can take longer than the slice after it. Counted only the slices, a request retried
                    // about twenty times in a 2 s wait and took ~7 s; so the request's time counts from its
                    // fault on (above), and each fault in a row doubles the pause after it: a request that
                    // keeps faulting ends within about BackPressureWait plus one attempt.
                    if (countFrom < 0)
                    {
                        countFrom   = runStart;
                        countWaited = waitedMs;
                    }
                    if (ownFrom < 0) ownFrom = runStart;
                    faults++;
                    waitedMs = countWaited + (Environment.TickCount64 - countFrom);
                    if (budgetMs - waitedMs <= 0) break;
                }

                if (!ReferenceEquals(_write, full)) continue;

                long left = budgetMs - waitedMs;
                if (left <= 0) break;
                if (!waited && outcome == WriteOutcome.NoRoom)
                {
                    waited = true;
                    Interlocked.Increment(ref _ingestRoomWaits);
                }
                long slice = outcome == WriteOutcome.Faulted
                    ? (long)_roomRecheckMs << Math.Min(faults - 1, 6)
                    : _roomRecheckMs;
                long began     = Environment.TickCount64;
                long thisSlice = Math.Min(left, slice);
                try
                {
                    // In slices: a slice that ends without a signal goes round and writes again, and the
                    // write that finds no room asks for the swap again. A request is no longer dropped
                    // when the swap lock is held (ReleaseFlushLock asks again), so this is the net under
                    // the wake-ups, not what they rest on.
                    _beforeRoomWaitForTest?.Invoke();
                    if (awaitable) await room.WaitAsync(TimeSpan.FromMilliseconds(thisSlice), ct).ConfigureAwait(false);
                    else           room.Wait((int)thisSlice, ct);   // past a failed await: a blocked thread, never a suspension
                }
                catch (TimeoutException) { /* write again */ }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
                catch (Exception)
                {
                    // The wait itself could not be set up: WaitAsync allocates, and so does this method's
                    // box at its first suspension, which is here; a heap at its limit refuses both. It
                    // used to escape here, after events may already have been written; then it was
                    // counted as a slice that took no time, and a request went round twenty times in 6 ms
                    // and gave up before the swap it waited for landed. It waits for real now, with
                    // nothing to allocate (a blocked thread, but only on a heap that cannot set up a
                    // timer), and counts what it waited — the slice it would have waited, so the pause
                    // after each fault in a row still doubles (it slept the re-check interval, every time).
                    // And it does not await again (see awaitable).
                    awaitable = false;
                    Thread.Sleep((int)thisSlice);
                }
                _afterRoomWaitForTest?.Invoke();

                // A slice that ends late — a pool too starved to resume the writer on time — is counted here,
                // after it, and the writer goes round and writes once more before it looks at its wait: the
                // request can pass its wait by that slice. Kept (round-6 review R5-M4): the attempt after the
                // last slice is the one the wait was for, and the receiver awaits the write to its end, so its
                // answer is what the write did — the commit no longer gave up on a write that then landed
                // (a 500, then written). A client that went meanwhile has nothing more written (above).
                long elapsed = Environment.TickCount64 - began;
                if (countFrom < 0) waitedMs += elapsed;
                else
                {
                    countSlices += elapsed;
                    waitedMs     = countWaited + (Environment.TickCount64 - countFrom);
                }
            }

            int notWritten = batch.Count - next;
            if (notWritten > 0) Interlocked.Add(ref _ingestNotWritten, notWritten);
            result = new LogBatchWriteResult(written, refused, notWritten);
        }
        catch
        {
            batch.EndLease();
            throw;
        }
        batch.EndLease();
        return result;
    }

    /// <summary>
    /// One run of a batch's writes — up to <see cref="WriteRunEvents"/> events — under
    /// <see cref="_writerLock"/>, which the caller has taken and which is released here, in a finally
    /// of a method with no await: in <see cref="WriteBatchAsync"/> that finally was skipped after a
    /// suspension whose box could not be allocated. Moves <paramref name="next"/> past what it wrote
    /// or refused, and says why it stopped. <paramref name="full"/> and <paramref name="room"/> are read
    /// under the lock, in the order that cannot lose the wake-up: a state change stores <c>_write</c>
    /// BEFORE it swaps the signal (SignalRoom), so either the signal read here is the one about to
    /// complete, or <c>_write</c> has already moved on and the caller sees it.
    /// </summary>
    private WriteOutcome WriteRunLocked(LogWriteBatch batch, ref int next, ref int written, ref int refused,
                                        out long runStart, out WriteState full, out Task room)
    {
        try
        {
            // Read once a run, not once an event: on a fault, the run's time is the attempt's (at
            // most a few hundred events before it, microseconds — next to a full GC, nothing).
            runStart = Environment.TickCount64;
            var outcome = WriteOutcome.Written;
            int end     = Math.Min(batch.Count, next + WriteRunEvents);
            while (next < end)
            {
                try
                {
                    outcome = WriteLocked(batch.HeaderAt(next), batch.PayloadAt(next), batch.TemplateAt(next), batch.ExceptionAt(next),
                                          batch.ServiceAt(next));
                }
                catch (Exception ex)
                {
                    outcome = WriteOutcome.Faulted;   // nothing of the event was written: see WriteLocked
                    NoteWriteFaultLocked(ex);
                }
                if (outcome == WriteOutcome.Written)       written++;
                else if (outcome == WriteOutcome.TooLarge) { refused++; Interlocked.Increment(ref _ingestRefusedTooLarge); }
                else break;
                next++;
            }
            full = _write;
            room = Volatile.Read(ref _roomSignal).Task;
            return outcome;
        }
        finally { _writerLock.Exit(); }
    }

    /// <summary>
    /// What an event brings that no other event shares, which a tier — and a spilled block, for the tier
    /// its despill makes — counts against its size (round-5 H-D): a template or a service the intern pool
    /// did not take (past its budget, or longer than it takes), and an exception. A pooled one is the
    /// pool's instance, shared by every event of it, and so is the empty string. Allocates nothing.
    /// </summary>
    private static long OwnManagedBytes(in LogEventHeader h, string? template, ExceptionInfo? exception, string? service) =>
        (h.MessageTemplatePoolIndex < 0 && !string.IsNullOrEmpty(template) ? HotTierSegment.ManagedBytesOf(template) : 0)
        + (h.ServiceNamePoolIndex < 0 && !string.IsNullOrEmpty(service) ? HotTierSegment.ManagedBytesOf(service) : 0)
        + HotTierSegment.ManagedBytesOf(exception);

    /// <summary>The body of every write; the caller holds <see cref="_writerLock"/>.</summary>
    private WriteOutcome WriteLocked(in LogEventHeader header, ReadOnlySpan<byte> payload, string? template, ExceptionInfo? exception,
                                     string? service = null)
    {
        // Shut by DisposeAsync. Refused like a full tier — the caller keeps the event — and before
        // anything else, so a refused write neither takes an id nor schedules a flush. Under the
        // writer lock, so shutdown's fence (which takes it after setting the flag) is the last
        // moment a write can land.
        if (Volatile.Read(ref _writesClosed) != 0)
            return WriteOutcome.Closed;

        _afterWriteGate?.Invoke();

        // Never admitted by any tier, empty or not: waiting for room would wait for ever.
        if (payload.Length > HotTierSegment.ChunkPayloadBytes)
            return WriteOutcome.TooLarge;

        // ONE capture: tier, WAL and spill target are used from the same state, so the event can
        // never land in a tier whose WAL this call does not also hold.
        var w = _write;
        if (w.Spill is { } spill)
            return SpillLocked(spill, header, payload, template, exception, service);

        // The live WAL refused an append (below): until a swap replaces it, the tier takes nothing
        // more. An event written into it now would be acknowledged as durable and be in memory
        // only. The writer waits like one that found the tier full, and asks for the swap again —
        // so a request that a held swap lock dropped is made again on every re-check.
        if (w.WalFaulted)
        {
            ScheduleFlush();
            return WriteOutcome.NoRoom;
        }

        // Assign time-sortable, monotonic event id.
        // Time component is derived from the event's own @t (TimestampUtcTicks), not
        // server ingest time, so sorting by Id matches the timestamp shown in the UI.
        // The generator clamps to prevMs+1 for late-arriving events, preserving
        // strict per-node monotonicity (cursor pagination by Id remains correct).
        var h = header;
        h.Id  = _idGen.Next(header.TimestampUtcTicks);

        if (!w.Hot.TryWrite(h, payload, template, exception, service, OwnManagedBytes(h, template, exception, service)))
        {
            // Full (or wedged on a chunk an event does not fit): ask for a swap — or, with no flush
            // slot free, for a spill — and tell the caller to wait. The flag is what lets the swap
            // tell this from a timed flush of a tier with room.
            w.HotRefused = true;
            ScheduleFlush();
            return WriteOutcome.NoRoom;
        }

        // ── The event is COMMITTED from here on. Nothing below may throw out of this method: the
        //    caller takes a throw for "nothing written" (WriteBatchAsync) and would write the event
        //    again, a second copy. What can fail below is bookkeeping that a later write repeats —
        //    a log line, a swap request (a faulted or full tier refuses the next write, and every
        //    refused write asks again) — so the whole of it is guarded, and a failure swallowed.
        try
        {
            // The WAL entry's index is 16 bits and all 65 536 values are real pool ids, so an event
            // outside the pool (-1 once it is full, or a claim at/past 65 536) is passed through
            // as-is and Append logs it with the Unpooled flag instead of an index. It writes NO
            // pool row for it: a row 0 carrying the unpooled text would be force-interned by
            // recovery and become every genuine index-0 event's template. And without the flag,
            // recovery gave the unpooled event itself index 0's template whenever the pool file
            // held any row. The text goes into the entry itself instead (v6 InlineTemplate), as
            // does a template longer than a pool row holds, so the replay gets it back.
            string tmplStr = template
                             ?? (h.MessageTemplatePoolIndex >= 0 ? TemplatePool.Get(h.MessageTemplatePoolIndex) : string.Empty);
            // The service is logged the way the header holds it, by pool index, and its text goes into
            // the WAL's pool file the first time that WAL logs the index — resolved here exactly as the
            // flush resolves it, so a replayed event carries the service the flushed one would have.
            // One with no index (the pool was full) is logged by its text, in the entry (v6
            // InlineService), as the tier keeps it.
            string? svcStr = h.ServiceNamePoolIndex >= 0 ? TemplatePool.Get(h.ServiceNamePoolIndex) : service;
            try
            {
                w.Wal?.Append(h.TimestampUtcTicks, h.Level, h.MessageTemplatePoolIndex, tmplStr, payload, exception,
                              h.ServiceNamePoolIndex, svcStr, h.TraceIdHi, h.TraceIdLo, h.SpanId);
            }
            catch (Exception ex)
            {
                // Disk full growing the WAL, a wedged mapping after a failed grow, a pool-file write
                // error. THIS event is in the tier, and its flush persists it, but it is not
                // crash-durable until then: the one event per fault that its acknowledgement
                // over-promises. No later one is: the state is marked, the tier refuses from the next
                // write on (above), and the swap that replaces tier and WAL is asked for — and asked
                // again by every writer that finds the tier refusing. It used to be asked for once per
                // episode, a request a held swap lock or a busy slot could drop, and every write after
                // it was answered 200 from memory alone until the tier filled or MaxAge came round.
                // HotRefused is what lets a swap that finds no flush slot spill instead. Marked
                // before anything that can throw.
                Interlocked.Increment(ref _ingestWalAppendFailures);
                w.WalFaulted = true;
                w.HotRefused = true;
                _logger.LogError(ex,
                    "WAL append failed — the event is in the hot tier but not crash-durable until it is flushed; " +
                    "further writes wait for the WAL to be replaced");
                ScheduleFlush();
            }

            NotifyEventWritten(in h, tmplStr);

            // Check size threshold
            if (w.Hot.IsFull)
                ScheduleFlush();
        }
        catch (Exception) { /* see above: the event is written, and says so */ }

        return WriteOutcome.Written;
    }

    // ── Write faults ──────────────────────────────────────────────────────────

    private long _ingestWriteFaults;
    /// <summary>When the last write-fault line was logged (<see cref="Environment.TickCount64"/>); under <see cref="_writerLock"/>.</summary>
    private long _writeFaultLoggedAt;
    /// <summary><see cref="_ingestWriteFaults"/> as that line reported it; under <see cref="_writerLock"/>.</summary>
    private long _writeFaultsLogged;

    /// <summary>
    /// A write threw (and wrote nothing). Counted, and logged at most once a second with the count
    /// since the last line: a store out of memory fails every waiting writer's every re-check, and
    /// a line apiece would bury the one that says why. The caller holds <see cref="_writerLock"/>.
    /// </summary>
    private void NoteWriteFaultLocked(Exception ex)
    {
        long n   = Interlocked.Increment(ref _ingestWriteFaults);
        long now = Environment.TickCount64;
        Volatile.Write(ref _lastWriteFaultAt, now);
        if (_writeFaultLoggedAt != 0 && now - _writeFaultLoggedAt < 1000) return;
        long since = n - _writeFaultsLogged;
        _writeFaultLoggedAt = now;
        _writeFaultsLogged  = n;
        try
        {
            _logger.LogError(ex,
                "Writing a log event into the store failed ({Count} time(s) since the last report) — nothing of it " +
                "was written; its batch waits and tries again, up to Ingestion.BackPressureWait", since);
        }
        catch (Exception) { /* counted; the line is lost */ }
    }

    /// <summary>
    /// Tells <see cref="EventWritten"/>'s subscribers (the live-tail doorbell, the alert evaluator)
    /// that an event is visible — fast, and unable to fault the write: a throwing subscriber used to
    /// kill the drain task outright.
    /// </summary>
    private void NotifyEventWritten(in LogEventHeader header, string template)
    {
        var hook = EventWritten;
        if (hook is null) return;
        try { hook(header, template); }
        catch (Exception ex)
        {
            long n = Interlocked.Increment(ref _hookFaults);
            if (n == 1 || n % 10_000 == 0)
                _logger.LogWarning(ex, "EventWritten subscriber threw ({Count} total) — subscriber faults are ignored", n);
        }
    }

    // ── Waiting for room ──────────────────────────────────────────────────────

    /// <summary>
    /// Completed, and replaced, whenever <see cref="_write"/> changes — a swap, a spill opening or
    /// rotating — and when the write path closes. A writer with no room awaits the one it read under
    /// <see cref="_writerLock"/>; see <see cref="WriteBatchAsync"/> for why that cannot miss a change.
    /// </summary>
    private TaskCompletionSource _roomSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Wakes every writer waiting for room. Call AFTER storing the new <see cref="_write"/>.
    ///
    /// <para>Never throws. It runs after a state change has been made, and a swap's caller takes a
    /// throw for "nothing was swapped" (see <see cref="InstallSwap"/>). A wake-up lost to an
    /// out-of-memory costs a waiting writer one <see cref="_roomRecheckMs"/> slice, no more: it then
    /// writes again whatever the signal did.</para>
    /// </summary>
    /// <param name="next">The signal to install in its place, allocated by a caller that cannot
    /// afford to fail after its change; null allocates one here.</param>
    private void SignalRoom(TaskCompletionSource? next = null)
    {
        try
        {
            if (next is null) _beforeRoomSignalAllocForTest?.Invoke();
            var old = Interlocked.Exchange(ref _roomSignal,
                next ?? new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
            old.TrySetResult();
        }
        catch (Exception) { /* see above: the writers' own re-check covers a lost wake-up */ }
    }

    /// <summary>
    /// Test hook: the signal a state change wakes the writers with is about to be allocated —
    /// throwing from it is that allocation running out of memory. Null in production.
    /// </summary>
    internal Action? _beforeRoomSignalAllocForTest;

    /// <summary>
    /// Test hook: the swap is about to give the frozen-tier list room for its tier — throwing from it
    /// is that growth running out of memory. Null in production.
    /// </summary>
    internal Action? _beforeFrozenListGrowForTest;

    /// <summary>
    /// Installs <paramref name="next"/> as the write state under <see cref="_writerLock"/> — so no
    /// write is in progress against the state it replaces — and wakes the writers waiting for room.
    /// For a change that keeps the live tier (a spill opening, rotating, or ending beside an empty
    /// tier); a swap that freezes it is <see cref="InstallSwap"/>.
    /// </summary>
    private void InstallWriteState(WriteState next)
    {
        EnterWriterLock();
        try { _write = next; }
        finally { _writerLock.Exit(); }
        SignalRoom();
    }

    /// <summary>
    /// The swap's one step: freeze <paramref name="frozen"/>, count its heavy phase, list it as
    /// frozen under <see cref="_frozenLock"/> and install <paramref name="next"/> — all in one hold
    /// of <see cref="_writerLock"/> — then wake the writers waiting for room.
    ///
    /// <para>Under the writer lock, no write is in progress, so the freeze returns at once and no
    /// event can land in the tier after it — nor in its WAL after the tier has gone: the old
    /// "captured the state just before the rotation" window, in which an event reached the frozen
    /// tier but not the WAL the swap was disposing, is closed. Counted in the step that publishes the
    /// tier to the frozen list, and under <c>_flushLock</c> (the caller's): from here on the heavy
    /// phase owns reading this tier, and the decrement at its end is what lets shutdown free it.
    /// Listed and installed under <see cref="_frozenLock"/>, the lock queries snapshot from, so a
    /// query sees the tier exactly once — as current before the store, as frozen after it — and
    /// skips its reserved cold segment ids. A tier flushes to ONE SEGMENT PER LEVEL, and the block
    /// of ids for exactly that was reserved when its WAL was opened — the level's segment is always
    /// <c>firstId + (byte)level</c>; levels absent from the tier never become files.</para>
    ///
    /// <para>THROWS ONLY BEFORE THE FREEZE. The caller's catch takes a throw for "nothing was
    /// swapped" and gives the flush slot back, so a throw after the count would orphan the frozen
    /// tier: never flushed, never freed, and a heavy phase shutdown waits its whole budget for. So
    /// what can fail is done first — the signal the writers are woken with is allocated, and the
    /// frozen list is given room for the entry — and from the freeze on nothing allocates.</para>
    /// </summary>
    private void InstallSwap(WriteState next, HotTierSegment frozen, ulong frozenSegId)
    {
        _beforeRoomSignalAllocForTest?.Invoke();
        var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        EnterWriterLock();
        try
        {
            lock (_frozenLock)
            {
                _beforeFrozenListGrowForTest?.Invoke();
                _frozenHot.EnsureCapacity(_frozenHot.Count + 1);
                frozen.Freeze();
                Interlocked.Increment(ref _heavyPhases);
                _frozenHot.Add((frozen, frozenSegId));
                _write = next;
            }
        }
        finally { _writerLock.Exit(); }
        SignalRoom(signal);
    }

    /// <summary>
    /// Takes <see cref="_writerLock"/>, calling <see cref="_onWaitingForWriterLock"/> first when a
    /// writer holds it — the seam a test parks a write on to see that a swap or shutdown waits it out.
    /// </summary>
    private void EnterWriterLock()
    {
        if (_writerLock.TryEnter()) return;
        _onWaitingForWriterLock?.Invoke();
        _writerLock.Enter();
    }

    /// <summary>Test hook: a swap or shutdown found <see cref="_writerLock"/> held and is about to wait for it.</summary>
    internal Action? _onWaitingForWriterLock;

    /// <summary>
    /// Waits for <see cref="_writerLock"/>, held by another writer, for a batch writer that came to it
    /// at <paramref name="lockFrom"/>. In slices: while the writer is in a run of faults of its own
    /// (<paramref name="countFrom"/> set) — or, with <paramref name="nothingWritten"/>, while the store
    /// is faulting (<see cref="StoreFaulting"/>) — its wall time counts against its
    /// <paramref name="budgetMs"/>, from <paramref name="lockFrom"/> when the faulting is only seen
    /// during this wait, and it gives up when that is spent. A cancelled request stops waiting either
    /// way. False when it gave up: the batch is done, with nothing more written.
    ///
    /// <para>Not a healthy writer that has written part of its batch: charged for its queueing behind
    /// other requests' faults, run after run, a healthy batch gave up part way under contention — a
    /// 200 with its tail dropped, which no client retries (10+4 and 6+6 of 96 requests, where none
    /// did before). One that has written nothing gives up cleanly, with a 503 its client retries.</para>
    /// </summary>
    private bool WaitForWriterLock(long lockFrom, long budgetMs, long waitedMs, bool nothingWritten, ref long countFrom,
                                   ref long countWaited, CancellationToken ct)
    {
        while (true)
        {
            if (countFrom < 0 && nothingWritten && StoreFaulting())
            {
                countFrom   = lockFrom;
                countWaited = waitedMs;
            }
            long sliceMs = LockWaitSliceMs;
            if (countFrom >= 0)
            {
                long left = budgetMs - (countWaited + (Environment.TickCount64 - countFrom));
                if (left <= 0) return _writerLock.TryEnter();
                sliceMs = Math.Min(sliceMs, left);
            }
            if (_writerLock.TryEnter((int)sliceMs)) return true;
            _onWriterLockSliceForTest?.Invoke();
            if (ct.IsCancellationRequested) return false;
        }
    }

    /// <summary>
    /// How long a writer waits for the lock before it looks at its token (and, counting, at its wait)
    /// again. Over <see cref="System.Threading.Lock"/>'s 100 ms: a waiter that has waited that long is
    /// handed the lock rather than barged past, and a waiter that gave up every 50 ms and came back as
    /// a new one never got there — a lone small request waited up to 462 ms behind busy writers.
    /// </summary>
    private const int LockWaitSliceMs = 200;

    /// <summary>Test seam: a writer's wait for the lock ran out one slice without it (see <see cref="LockWaitSliceMs"/>).</summary>
    internal Action? _onWriterLockSliceForTest;

    /// <summary>Test hook: runs after each run of a batch's writes, outside the writer lock.</summary>
    internal Action? _afterWriteRunForTest;

    /// <summary>When a write last faulted (<see cref="Environment.TickCount64"/>); written under <see cref="_writerLock"/>.</summary>
    private long _lastWriteFaultAt = long.MinValue / 2;

    /// <summary>How long after a write faulted the store counts as faulting for the writers coming to the lock.</summary>
    private const long WriteFaultEpisodeMs = 1000;

    /// <summary>
    /// A write faulted in the last second: the writer lock may be held through failing attempts, so a
    /// writer coming to it counts the time it waits there (see <see cref="WriteBatchAsync"/>).
    /// </summary>
    private bool StoreFaulting() => Environment.TickCount64 - Volatile.Read(ref _lastWriteFaultAt) <= WriteFaultEpisodeMs;

    /// <summary>Test hook: a writer with no room is about to wait for it; throwing from it is a wait that could not be set up.</summary>
    internal Action? _beforeRoomWaitForTest;

    /// <summary>Test hook: a writer's pause for room has ended, however it ended, and the writer is about to go round.</summary>
    internal Action? _afterRoomWaitForTest;

    // ── Ingest counters (/api/diagnostics) ────────────────────────────────────

    private long _ingestRoomWaits;
    private long _ingestNotWritten;
    private long _ingestRefusedTooLarge;
    private long _ingestWalAppendFailures;

    /// <summary>
    /// Batches that found no room and waited for it, once per batch. Every tier swap makes the
    /// batches writing at that moment wait for it, so this climbs with ordinary traffic.
    /// </summary>
    public long IngestRoomWaits => Interlocked.Read(ref _ingestRoomWaits);

    /// <summary>
    /// Events a batch gave up on: no room within <c>Ingestion.BackPressureWait</c> (or writes that
    /// kept failing for that long — <see cref="IngestWriteFaults"/>), or shutdown.
    /// </summary>
    public long IngestNotWritten => Interlocked.Read(ref _ingestNotWritten);

    /// <summary>Events refused because no tier can hold them (a payload over one hot-tier chunk).</summary>
    public long IngestRefusedTooLarge => Interlocked.Read(ref _ingestRefusedTooLarge);

    /// <summary>
    /// Events written to the live tier whose WAL append failed: acknowledged, persisted by their
    /// flush, but not crash-durable until it. Non-zero means the WAL volume refused writes. At most
    /// one per WAL: after it, writes wait for the swap that replaces the WAL (or spill).
    /// </summary>
    public long IngestWalAppendFailures => Interlocked.Read(ref _ingestWalAppendFailures);

    /// <summary>
    /// Writes that threw — out of memory in the tier, most likely — and wrote nothing; each was
    /// waited out and tried again, and what had still not been written when the wait ran out is in
    /// <see cref="IngestNotWritten"/>. The log says why, at most once a second.
    /// </summary>
    public long IngestWriteFaults => Interlocked.Read(ref _ingestWriteFaults);
}
