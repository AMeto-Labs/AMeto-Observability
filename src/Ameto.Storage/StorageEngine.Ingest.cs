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
    }

    /// <summary>
    /// Events one writer writes per take of <see cref="_writerLock"/>. A batch of thousands lets
    /// the other requests in between runs rather than holding them for its whole length; at about
    /// a microsecond an event a run holds the lock for a fraction of a millisecond.
    /// </summary>
    private const int WriteRunEvents = 256;

    /// <summary>How long a writer waiting for room waits on the signal before it writes again.</summary>
    private const int RoomRecheckMs = 100;

    /// <summary>
    /// Writes one event — the single-event form of <see cref="WriteBatchAsync"/>, for callers with
    /// one event and no wish to wait. False when it was not written: no room right now (a swap has
    /// been asked for), the write path is closed, or no tier can hold an event that large.
    /// </summary>
    public bool TryWrite(in LogEventHeader header, ReadOnlySpan<byte> propertiesPayload, string? template = null, ExceptionInfo? exception = null)
    {
        lock (_writerLock) return WriteLocked(header, propertiesPayload, template, exception) == WriteOutcome.Written;
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
    /// </summary>
    /// <param name="maxWait">How long the batch may wait, in all, for room.</param>
    /// <param name="ct">The request's: a client that has gone stops the wait, and nothing after the
    /// events already written is written.</param>
    public async ValueTask<LogBatchWriteResult> WriteBatchAsync(LogWriteBatch batch, TimeSpan maxWait, CancellationToken ct = default)
    {
        int  next     = 0, written = 0, refused = 0;
        bool waited   = false;
        long deadline = Environment.TickCount64 + (long)Math.Max(0, maxWait.TotalMilliseconds);

        while (next < batch.Count)
        {
            WriteState full;
            Task       room;
            lock (_writerLock)
            {
                int end = Math.Min(batch.Count, next + WriteRunEvents);
                var outcome = WriteOutcome.Written;
                while (next < end)
                {
                    outcome = WriteLocked(batch.HeaderAt(next), batch.PayloadAt(next), batch.TemplateAt(next), batch.ExceptionAt(next));
                    if (outcome == WriteOutcome.Written)       written++;
                    else if (outcome == WriteOutcome.TooLarge) { refused++; Interlocked.Increment(ref _ingestRefusedTooLarge); }
                    else break;
                    next++;
                }
                if (outcome is WriteOutcome.Written or WriteOutcome.TooLarge) continue;   // the run is done; let others in
                if (outcome == WriteOutcome.Closed) break;

                // No room. Captured under the lock, in this order, so the wake-up cannot be lost: a
                // state change stores _write BEFORE it swaps the signal (SignalRoom), so either the
                // signal read here is the one about to complete, or _write has already moved on and
                // the check below sees it.
                full = _write;
                room = Volatile.Read(ref _roomSignal).Task;
            }

            if (!ReferenceEquals(_write, full)) continue;

            long left = deadline - Environment.TickCount64;
            if (left <= 0) break;
            if (!waited)
            {
                waited = true;
                Interlocked.Increment(ref _ingestRoomWaits);
            }
            try
            {
                // In slices: a slice that ends without a signal goes round and writes again, and the
                // write that finds no room asks for the swap again. A request for a swap can be
                // dropped — ScheduleFlush gives way to a swap lock already held, and the holder may be
                // doing something else with it (a despill registering) — and without the retry a
                // writer would sit out its whole wait on a request nobody acted on.
                await room.WaitAsync(TimeSpan.FromMilliseconds(Math.Min(left, RoomRecheckMs)), ct).ConfigureAwait(false);
            }
            catch (TimeoutException) { /* write again */ }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
        }

        int notWritten = batch.Count - next;
        if (notWritten > 0) Interlocked.Add(ref _ingestNotWritten, notWritten);
        return new LogBatchWriteResult(written, refused, notWritten);
    }

    /// <summary>The body of every write; the caller holds <see cref="_writerLock"/>.</summary>
    private WriteOutcome WriteLocked(in LogEventHeader header, ReadOnlySpan<byte> payload, string? template, ExceptionInfo? exception)
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
            return SpillLocked(spill, header, payload, template, exception);

        // Assign time-sortable, monotonic event id.
        // Time component is derived from the event's own @t (TimestampUtcTicks), not
        // server ingest time, so sorting by Id matches the timestamp shown in the UI.
        // The generator clamps to prevMs+1 for late-arriving events, preserving
        // strict per-node monotonicity (cursor pagination by Id remains correct).
        var h = header;
        h.Id  = _idGen.Next(header.TimestampUtcTicks);

        if (!w.Hot.TryWrite(h, payload, template, exception))
        {
            // Full (or wedged on a chunk an event does not fit): ask for a swap — or, with no flush
            // slot free, for a spill — and tell the caller to wait. The flag is what lets the swap
            // tell this from a timed flush of a tier with room.
            w.HotRefused = true;
            ScheduleFlush();
            return WriteOutcome.NoRoom;
        }

        // ── The event is COMMITTED from here on. Nothing below may throw out of this method: the
        //    caller would take the event for not written and write it again, a second copy.
        // The WAL entry's index is 16 bits and all 65 536 values are real pool ids, so an event
        // outside the pool (-1 once it is full, or a claim at/past 65 536) is passed through
        // as-is and Append logs it with the Unpooled flag instead of an index. It writes NO
        // pool row for it: a row 0 carrying the unpooled text would be force-interned by
        // recovery and become every genuine index-0 event's template. And without the flag,
        // recovery gave the unpooled event itself index 0's template whenever the pool file
        // held any row. The hook still gets the attached text; the WAL never stores it.
        string tmplStr = template
                         ?? (h.MessageTemplatePoolIndex >= 0 ? TemplatePool.Get(h.MessageTemplatePoolIndex) : string.Empty);
        // The service is logged the way the header holds it, by pool index, and its text goes into
        // the WAL's pool file the first time that WAL logs the index — resolved here exactly as the
        // flush resolves it, so a replayed event carries the service the flushed one would have.
        string? svcStr = h.ServiceNamePoolIndex >= 0 ? TemplatePool.Get(h.ServiceNamePoolIndex) : null;
        try
        {
            w.Wal?.Append(h.TimestampUtcTicks, h.Level, h.MessageTemplatePoolIndex, tmplStr, payload, exception,
                          h.ServiceNamePoolIndex, svcStr, h.TraceIdHi, h.TraceIdLo, h.SpanId);
            _walFaulted = false;
        }
        catch (Exception ex)
        {
            // Disk full growing the WAL, a wedged mapping after a failed grow, a pool-file write
            // error. The event is in the tier and the flush persists it regardless — but until the
            // WAL rotates it is not crash-durable, which the acknowledgement promises. Counted,
            // rotation forced, and said once per episode (the age loop keeps re-attempting the
            // swap until the disk recovers).
            Interlocked.Increment(ref _ingestWalAppendFailures);
            if (!_walFaulted)
            {
                _walFaulted = true;
                _logger.LogError(ex,
                    "WAL append failed — events acknowledged until the WAL rotates are in memory only, " +
                    "not crash-durable; rotation forced");
                ScheduleFlush();
            }
        }

        NotifyEventWritten(in h, tmplStr);

        // Check size threshold
        if (w.Hot.IsFull)
            ScheduleFlush();

        return WriteOutcome.Written;
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

    /// <summary>Wakes every writer waiting for room. Call AFTER storing the new <see cref="_write"/>.</summary>
    private void SignalRoom()
    {
        var old = Interlocked.Exchange(ref _roomSignal, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        old.TrySetResult();
    }

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
    /// </summary>
    private void InstallSwap(WriteState next, HotTierSegment frozen, ulong frozenSegId)
    {
        EnterWriterLock();
        try
        {
            frozen.Freeze();
            Interlocked.Increment(ref _heavyPhases);
            lock (_frozenLock)
            {
                _frozenHot.Add((frozen, frozenSegId));
                _write = next;
            }
        }
        finally { _writerLock.Exit(); }
        SignalRoom();
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

    // ── Ingest counters (/api/diagnostics) ────────────────────────────────────

    private long _ingestRoomWaits;
    private long _ingestNotWritten;
    private long _ingestRefusedTooLarge;
    private long _ingestWalAppendFailures;

    /// <summary>Times a writer found no room and waited for it.</summary>
    public long IngestRoomWaits => Interlocked.Read(ref _ingestRoomWaits);

    /// <summary>Events a batch gave up on: no room within <c>Ingestion.BackPressureWait</c>, or shutdown.</summary>
    public long IngestNotWritten => Interlocked.Read(ref _ingestNotWritten);

    /// <summary>Events refused because no tier can hold them (a payload over one hot-tier chunk).</summary>
    public long IngestRefusedTooLarge => Interlocked.Read(ref _ingestRefusedTooLarge);

    /// <summary>
    /// Events written to the live tier whose WAL append failed: acknowledged, persisted by their
    /// flush, but not crash-durable until it. Non-zero means the WAL volume refused writes.
    /// </summary>
    public long IngestWalAppendFailures => Interlocked.Read(ref _ingestWalAppendFailures);
}
