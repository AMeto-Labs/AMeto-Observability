using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// Round 7, the registration class. At any await after an async method's first real suspension, a registration of its
/// continuation that runs out of memory throws into the method after the compiler has recorded the suspension in the
/// state: every finally is skipped from there until a suspension resumes, while the catches run — and what it awaited
/// runs on. The flush's owners — the swap's heavy phase, a despill, a flush retry, the startup replay — awaited the
/// flush, and the flush awaited each level's write; whichever of those awaits failed left a level write reading a tier
/// that its owner, or the owner's give-up, went on to free, to write a second time, or to give its slot, its count
/// and its permit back under. The flush runs in its owner's own frame now (FlushTierByLevel), so no await is left
/// to fail while a write runs. These tests fail such an await at the parent through the LATER-suspension seam
/// (OwnerAsync.LaterSuspensionFailsForTest) while a write is held, and see what it frees.
/// </summary>
public sealed partial class IngestWriteAndSpillTests
{
    /// <summary>
    /// Holds the <c>holdWrite</c>-th level write a flush begins (StorageEngine.LevelWriteStartingForTest, on the write's
    /// thread, before it reads its tier), and answers the later-suspension seam for the state machines it names: the
    /// first of their later suspensions after it is armed waits, bounded, for the held write to begin (a flush that
    /// queued its write and awaited it suspended before the write began), and one that comes while the write is held is
    /// recorded (<see cref="SuspendedWhileWriting"/>, its state machine) and failed, as a registration that runs out of
    /// memory under the TPL event source fails. The held write is then kept, before it reads, until its tier is freed or
    /// another level write begins beside it — both recorded, and the write stopped there: in production the first is a
    /// use-after-free — or a bound passes; with nothing failed it goes on after the bound. A write that begins on a freed
    /// tier, or beside the held one, is recorded and stopped too. And the write records the engine's locks as its thread
    /// sees them (<see cref="Engine"/>): a flush in its owner's frame holds none, and finds the swap lock free.
    /// </summary>
    private sealed class HeldWriteWatch(int holdWrite, params string[] suspends)
    {
        private static readonly TimeSpan Bound = TimeSpan.FromSeconds(1);

        private readonly ManualResetEventSlim _began = new(false), _failed = new(false);
        private int _writes, _asked, _armed = 1, _writesBeside;
        private HotTierSegment? _held;
        private volatile string? _suspendedWhileWriting, _lockHeld;
        private volatile bool _freedUnderIt;

        /// <summary>The engine whose locks a level write looks at; none for a replay in the constructor.</summary>
        public StorageEngine? Engine { get; set; }

        /// <summary>The state machine of a flush owner that suspended while its level write was held; null for none.</summary>
        public string? SuspendedWhileWriting => _suspendedWhileWriting;

        /// <summary>A tier was freed under a level write, or a write began on a freed tier.</summary>
        public bool FreedUnderIt => _freedUnderIt;

        /// <summary>Level writes that began while the held one was running.</summary>
        public int WritesBesideIt => Volatile.Read(ref _writesBeside);

        /// <summary>The held write has begun.</summary>
        public bool WriteHeld => _began.IsSet;

        /// <summary>Sets the two seams on this flow, and every flow started from it; the result clears them.</summary>
        public IDisposable Arm()
        {
            OwnerAsync.LaterSuspensionFailsForTest.Value  = FailsLaterSuspension;
            StorageEngine.LevelWriteStartingForTest.Value = OnLevelWrite;
            return new Disarm();
        }

        private sealed class Disarm : IDisposable
        {
            public void Dispose()
            {
                OwnerAsync.LaterSuspensionFailsForTest.Value  = null;
                StorageEngine.LevelWriteStartingForTest.Value = null;
            }
        }

        /// <summary>Nothing was left running by a failed await, nothing freed or written beside a write, and no write held a lock.</summary>
        public void AssertNothingLeftRunning()
        {
            var wrong = new List<string>();
            if (SuspendedWhileWriting is { } owner)
                wrong.Add($"{owner} suspended while its flush's level write ran, and its await failed there with the write running on");
            if (FreedUnderIt) wrong.Add("a tier was freed under a level write");
            if (WritesBesideIt > 0) wrong.Add($"{WritesBesideIt} level write(s) began beside the one left running");
            if (_lockHeld is { } held) wrong.Add($"a level write ran holding {held}");
            Assert.True(wrong.Count == 0, string.Join("; ", wrong));
        }

        private bool FailsLaterSuspension(Type stateMachine, Type awaiter)
        {
            string name = stateMachine.Name;
            bool named = false;
            foreach (string s in suspends) named |= name.Contains(s, StringComparison.Ordinal);
            if (!named) return false;
            if (Interlocked.Exchange(ref _asked, 1) == 0) _began.Wait(Bound);
            if (Volatile.Read(ref _held) is null || Interlocked.Exchange(ref _armed, 0) == 0) return false;
            _suspendedWhileWriting = name;
            _failed.Set();
            return true;
        }

        private void OnLevelWrite(HotTierSegment tier)
        {
            if (tier.IsDisposed)
            {
                _freedUnderIt = true;
                throw new InvalidOperationException("test: a level write began on a freed tier; stopped before it reads");
            }
            if (Engine is { } engine)
            {
                var (writer, frozen, swap) = engine.LocksAsSeenHereForTest;
                if (writer) _lockHeld = "the writer lock";
                else if (frozen) _lockHeld = "the frozen-tier list's lock";
                else if (swap) _lockHeld = "the swap lock (or it was taken meanwhile)";
            }
            if (Volatile.Read(ref _held) is not null)
            {
                Interlocked.Increment(ref _writesBeside);
                throw new InvalidOperationException("test: a level write began beside the held one; stopped before it reads");
            }
            if (Interlocked.Increment(ref _writes) != holdWrite) return;
            Volatile.Write(ref _held, tier);
            _began.Set();
            try
            {
                if (!_failed.Wait(Bound)) return;   // no owner suspended meanwhile: the write goes on
                SpinWait.SpinUntil(() => tier.IsDisposed || Volatile.Read(ref _writesBeside) > 0, Bound + Bound);
                if (tier.IsDisposed) _freedUnderIt = true;
                throw new InvalidOperationException("test: the level write a failed await left running is stopped before it reads");
            }
            finally { Volatile.Write(ref _held, null); }
        }
    }

    /// <summary>
    /// R6-H3 (c42b5d4, round-6 review and ultra review #1), a despill's retry. The retry loop's await of its attempt's
    /// flush failed at its registration while the flush ran: c42b5d4's filter let the throw out of the loop with every
    /// finally skipped, and RetryEnded took the faulted task for a retry that ended — it gave the slot, the count and the
    /// text back and gave the despill's tier up: unlisted and retired (freed at once, no query reading it) under the
    /// running flush, its levels unlinked, its block queued for a second despill beside it. The attempt's flush runs in
    /// the loop's own frame now: when an attempt ends, nothing of it runs.
    /// </summary>
    [Fact]
    public async Task A_despill_retrys_flush_is_not_left_running_and_its_tier_is_not_freed_under_it()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        EarlierRunSpill(opts, dir, 9_000, 100_000, 400);
        var watch = new HeldWriteWatch(holdWrite: 1, "<FlushRetryLoopAsync>");
        var hold  = new TaskCompletionSource();
        StorageEngine engine;
        using (watch.Arm())   // the despill loop carries it, and the retry the despill starts
            engine = HeldOneSlotEngine(opts, hold);
        watch.Engine = engine;
        int levelCalls = 0;
        await using (engine)
        {
            engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
            engine._flushRetryDelay   = TimeSpan.FromMilliseconds(50);
            // The despill's own flush fails before any level write: its tier goes to a retry, whose first write is held.
            engine._beforeLevelWriteForTest = _ =>
            {
                if (Interlocked.Increment(ref levelCalls) == 1) throw new IOException("test: the despill's flush fails");
            };
            hold.SetResult();
            WaitFor(() => watch.WriteHeld, "the retry's level write to begin", ShortGuard);
            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == 400, "the block to be despilled", ShortGuard);
            watch.AssertNothingLeftRunning();
            Assert.Equal(0, engine.HeavyPhasesInFlight);
            Assert.Equal(400, ReadAll(engine).Count);
        }
    }

    /// <summary>
    /// R6-H3, a swapped tier's retry (and c42b5d4's own test, which threw from BEFORE the await, so the loop's finally ran
    /// and the road it was about went untested). The same failed await: RetryEnded gave the heavy-phase count back while
    /// the orphaned flush ran, and a shutdown then, its count of heavy phases at zero, freed the still-listed tier under
    /// the write; and the next attempt, had the loop gone round, voided the marker and unlinked the levels the orphan
    /// was writing (21eb088's LOW). The attempt runs in the loop's frame now: the heavy phase stays counted while it
    /// writes, no second attempt begins beside it, and shutdown waits for it. The block comes back once.
    /// </summary>
    [Fact]
    public async Task A_swap_retrys_flush_stays_counted_while_it_writes_and_a_shutdown_frees_nothing_under_it()
    {
        string dir    = NewDir();
        var    opts   = Options(dir);
        var    engine = Engine(opts);
        var    watch  = new HeldWriteWatch(holdWrite: 2, "<FlushRetryLoopAsync>") { Engine = engine };
        int failed = 0;
        Task? dispose = null;
        try
        {
            using (var batch = Batch(engine, 0, 10))
                Assert.Equal(new LogBatchWriteResult(10, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
            engine._flushRetryDelay = TimeSpan.FromMilliseconds(50);
            // The swap's flush fails after its level is in place: a retry has the tier, and its first write is held.
            engine._afterLevelPublished = _ =>
            {
                if (Interlocked.Exchange(ref failed, 1) == 0) throw new IOException("test: the swap's flush fails after its level");
            };
            using (watch.Arm())
                await engine.FlushHotTierAsync();

            WaitFor(() => watch.WriteHeld, "the retry's level write to begin", ShortGuard);
            int countedWhileHeld = engine.HeavyPhasesInFlight;   // the retry's, while its flush writes
            dispose = engine.DisposeAsync().AsTask();            // shutdown, while the write is held
            bool ended = await Task.WhenAny(dispose, Task.Delay(ShortGuard)) == dispose;
            watch.AssertNothingLeftRunning();
            Assert.Equal(1, countedWhileHeld);
            Assert.True(ended, "the shutdown did not end");
        }
        finally
        {
            await (dispose ?? engine.DisposeAsync().AsTask());
        }

        await using var next = Engine(opts);
        await next.CatalogLoaded;
        await FlushEverythingAsync(next, ShortGuard);
        Assert.Equal(10, ReadAll(next).Count);   // ReadAll asserts no event is there twice
    }

    /// <summary>
    /// R6-M6 (ultra review #2), the swap's heavy phase. Its await of the flush failed at the registration with the flush
    /// running: the finally that gives the heavy-phase permit back was skipped (the compiler guards it with the
    /// suspension's state), and so were the publish and the slot's release. With one permit — every 2-core host — no
    /// flush, despill or merge ran again: every later swap took a slot and polled for the permit for ever, ingest spilled
    /// down to the floor and then answered 503, and shutdown polled the same permit with no deadline. The flush runs in
    /// the heavy phase's frame now, and its permit goes back in a finally whose try holds no await.
    /// </summary>
    [Fact]
    public async Task A_swaps_heavy_phase_is_not_left_by_a_failed_await_and_gives_its_permit_back()
    {
        var engine = OneSlotEngine(Options(NewDir()));
        var watch  = new HeldWriteWatch(holdWrite: 1, "<TryFlushAsync>") { Engine = engine };
        try
        {
            using (var batch = Batch(engine, 0, 500))
                Assert.Equal(new LogBatchWriteResult(500, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
            using (watch.Arm())
            {
                try { await engine.FlushHotTierAsync(); }
                catch (OutOfMemoryException) { /* the parent's road: the heavy phase's await failed */ }
            }

            WaitFor(() => engine.FrozenTierCountForTest == 0 && engine.HeavyPhasesInFlight == 0
                          && engine.FlushConcurrencyForTest.CurrentCount == engine.FlushWidth,
                    "the tier to be published and its heavy-phase permit to come back", ShortGuard);
            watch.AssertNothingLeftRunning();

            // And the next heavy phase runs: the permit is there for it.
            using (var more = Batch(engine, 500, 100))
                Assert.Equal(new LogBatchWriteResult(100, 0, 0), await engine.WriteBatchAsync(more, HangGuard));
            await engine.FlushHotTierAsync().WaitAsync(ShortGuard);
            Assert.Equal(600, ReadAll(engine).Count);
        }
        finally { await engine.DisposeAsync(); }
    }

    /// <summary>
    /// R6-M6's despill twin. A despill's await of its flush failed at the registration: its finally — the permit, the
    /// slot, the heavy-phase count, the frozen text and the block's settling — was skipped, so the block stayed in flight
    /// for the life of the process, unserved once its tier was gone, and the permit was lost. In the despill's own frame
    /// now.
    /// </summary>
    [Fact]
    public async Task A_despills_heavy_phase_is_not_left_by_a_failed_await_and_settles_its_block()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        EarlierRunSpill(opts, dir, 9_000, 100_000, 400);
        var watch = new HeldWriteWatch(holdWrite: 1, "<DespillAsync>");
        var hold  = new TaskCompletionSource();
        StorageEngine engine;
        using (watch.Arm())
            engine = HeldOneSlotEngine(opts, hold);
        watch.Engine = engine;
        await using (engine)
        {
            engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
            hold.SetResult();
            WaitFor(() => watch.WriteHeld, "the despill's level write to begin", ShortGuard);
            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == 400 && engine.HeavyPhasesInFlight == 0
                          && engine.FlushConcurrencyForTest.CurrentCount == engine.FlushWidth
                          && engine.FlushSlotsForTest.CurrentCount == engine.FlushSlots,
                    "the block to be despilled, and its permit and slot to come back", ShortGuard);
            watch.AssertNothingLeftRunning();
            Assert.Equal(400, ReadAll(engine).Count);
        }
    }

    /// <summary>
    /// Round 7 (the coordinator's check on the synchronous flush): the flush now runs in its owner's frame, so that frame
    /// must never be the request's whose write asked for the swap, nor one holding the writer lock, the frozen-tier
    /// list's lock or the swap lock. A write that fills the tier asks for the swap (ScheduleFlush, a Task.Run; the swap
    /// yields first), waits for the successor, and returns while the swapped tier's flush is still writing — its level
    /// write held here, its locks looked at on its thread.
    /// </summary>
    [Fact]
    public async Task A_write_that_asks_for_a_swap_returns_while_the_swaps_flush_is_writing()
    {
        var engine = OneSlotEngine(Options(NewDir()));
        using var gate = new ManualResetEventSlim(false);
        using var began = new ManualResetEventSlim(false);
        string? lockHeld = null;
        try
        {
            StorageEngine.LevelWriteStartingForTest.Value = _ =>
            {
                var (writer, frozen, swap) = engine.LocksAsSeenHereForTest;
                lockHeld = writer ? "the writer lock" : frozen ? "the frozen-tier list's lock" : swap ? "the swap lock" : null;
                began.Set();
                gate.Wait(HangGuard);
            };
            Task<LogBatchWriteResult> write;
            using (var batch = Batch(engine, 0, TierEvents + 100))
            {
                // On a thread of its own, as a request is: a flush run in its frame holds that thread, not this one.
                try { write = Task.Run(() => engine.WriteBatchAsync(batch, HangGuard).AsTask()); }
                finally { StorageEngine.LevelWriteStartingForTest.Value = null; }
                WaitFor(() => began.IsSet, "the swap's flush to begin its level write", ShortGuard);
                Assert.True(await Task.WhenAny(write, Task.Delay(ShortGuard)) == write,
                            "the write that asked for the swap did not return while the swap's flush was writing: the flush ran in its frame");
                Assert.Equal(new LogBatchWriteResult(TierEvents + 100, 0, 0), await write);
            }
            Assert.True(lockHeld is null, $"the swap's level write ran holding {lockHeld}");
            gate.Set();
            await FlushEverythingAsync(engine, ShortGuard);
            Assert.Equal(TierEvents + 100, ReadAll(engine).Count);
        }
        finally
        {
            gate.Set();
            await engine.DisposeAsync();
        }
    }

    /// <summary>
    /// Ultra review #7. Shutdown's final flush polled the heavy-phase permit with no deadline and a token nothing
    /// cancelled: a permit another heavy phase held past the shutdown budget — a slow disk, a large tier, or the permit
    /// R6-M6 lost — held DisposeAsync for good, and the Windows service's stop timed out. It polls until the budget's
    /// deadline now, and past it gives the tier up to the WAL's replay at the next start.
    /// </summary>
    [Fact]
    public async Task The_final_flush_waits_for_the_heavy_phase_permit_only_within_the_shutdown_budget()
    {
        string dir    = NewDir();
        var    opts   = Options(dir);
        var    engine = OneSlotEngine(opts);
        engine._shutdownWaitBudget = TimeSpan.FromSeconds(2);
        using (var batch = Batch(engine, 0, 100))
            Assert.Equal(new LogBatchWriteResult(100, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
        Assert.True(engine.FlushConcurrencyForTest.Wait(0));   // the only permit, held past the budget
        Task dispose = engine.DisposeAsync().AsTask();
        bool ended;
        try { ended = await Task.WhenAny(dispose, Task.Delay(TimeSpan.FromSeconds(12))) == dispose; }
        finally { engine.FlushConcurrencyForTest.Release(); }
        await dispose;
        Assert.True(ended, "the shutdown did not end within its budget: its final flush waited for the heavy-phase permit past it");

        await using var next = Engine(opts);
        await next.CatalogLoaded;
        await FlushEverythingAsync(next, ShortGuard);
        Assert.Equal(100, ReadAll(next).Count);
    }

    // ── The pauses: what a frame holds while it waits (ultra review #3; round 7) ──

    /// <summary>For OwnerAsync.LaterSuspensionFailsForTest: the first later suspension of <paramref name="method"/>'s state machine fails, once.</summary>
    private static Func<Type, Type, bool> FailLaterSuspensionOnce(string method, Action? asFailing = null)
    {
        int armed = 1;
        return (t, _) =>
        {
            if (!t.Name.Contains(method, StringComparison.Ordinal) || Interlocked.Exchange(ref armed, 0) == 0) return false;
            asFailing?.Invoke();
            return true;
        };
    }

    /// <summary>A block an earlier run spilled whose events carry a template no pool takes (inline, longer than a pooled text): its despilled tier holds text of its own.</summary>
    private static void EarlierRunTextSpill(ServerOptions opts, string dir, ulong block, int from, int count, int chars)
    {
        string walDir = Path.Combine(dir, "wal");
        Directory.CreateDirectory(walDir);
        string spill = Path.Combine(walDir, $"{opts.NodeId.Value}-{block}{StorageEngine.SpillExtension}");
        using var wal = WriteAheadLog.Open(spill, opts.NodeId, new SegmentId(block), 1 << 20);
        for (int i = from; i < from + count; i++)
            wal.Append(T0 + i * TimeSpan.TicksPerMillisecond, LogLevel.Information, -1,
                       new string((char)('a' + i % 26), chars) + i, Props(i));
    }

    /// <summary>
    /// Ultra review #3, the swap's heavy phase. The pause in its wait for the heavy-phase permit fails at its
    /// registration, and no retry can be started (out of memory). The catch handed the tier to a retry, found none,
    /// and returned through the finally that gives the slot and the count back — skipped, the compiler guarding it with
    /// the failed suspension's state: one slot fewer for good, and shutdown waited its whole budget for a heavy phase
    /// that would never end. The catch gives them back itself now.
    /// </summary>
    [Fact]
    public async Task A_heavy_phase_whose_permit_wait_fails_with_no_retry_gives_its_slot_and_count_back()
    {
        var engine = OneSlotEngine(Options(NewDir()));
        try
        {
            using (var batch = Batch(engine, 0, 500))
                Assert.Equal(new LogBatchWriteResult(500, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
            Assert.True(engine.FlushConcurrencyForTest.Wait(0));   // held: the heavy phase pauses for it
            engine._beforeFlushRetryStartForTest = static () => throw new OutOfMemoryException("test: no retry can be set up");
            OwnerAsync.LaterSuspensionFailsForTest.Value = FailLaterSuspensionOnce("<TryFlushAsync>");
            try { await engine.FlushHotTierAsync(); }
            finally
            {
                OwnerAsync.LaterSuspensionFailsForTest.Value = null;
                engine.FlushConcurrencyForTest.Release();
            }

            Assert.True(engine.FlushSlotsForTest.CurrentCount == engine.FlushSlots && engine.HeavyPhasesInFlight == 0,
                        $"slots free {engine.FlushSlotsForTest.CurrentCount}/{engine.FlushSlots}, heavy phases {engine.HeavyPhasesInFlight}: " +
                        "the heavy phase that could not wait for its permit kept them");
            Assert.Equal(1, engine.FrozenTierCountForTest);   // listed, its WAL the next start's: no retry could start
        }
        finally { await engine.DisposeAsync(); }
    }

    /// <summary>
    /// Round 7, the final flush's wait for a slot, which it makes holding the swap lock. A pause there whose continuation
    /// could not be registered threw with the finally that releases the lock skipped: the lock stayed taken, and the
    /// teardown waited its whole budget to take it, then left every tier allocated. A catch releases it now.
    /// </summary>
    [Fact]
    public async Task A_final_flush_whose_wait_for_a_slot_fails_gives_the_swap_lock_back()
    {
        var engine = OneSlotEngine(Options(NewDir()));
        engine._shutdownWaitBudget = TimeSpan.FromSeconds(3);
        using (var batch = Batch(engine, 0, 100))
            Assert.Equal(new LogBatchWriteResult(100, 0, 0), await engine.WriteBatchAsync(batch, HangGuard));
        Assert.True(engine.FlushSlotsForTest.Wait(0));   // the only slot, held: the final flush pauses for one
        bool waitedForTheLock = false;
        engine._onWaitingForFlushLock = () => waitedForTheLock = true;
        OwnerAsync.LaterSuspensionFailsForTest.Value = FailLaterSuspensionOnce("<TryFlushAsync>");
        Task dispose;
        try { dispose = engine.DisposeAsync().AsTask(); }
        finally { OwnerAsync.LaterSuspensionFailsForTest.Value = null; }
        try { await dispose.WaitAsync(HangGuard); }
        finally { engine.FlushSlotsForTest.Release(); }
        Assert.False(waitedForTheLock, "the teardown found the swap lock taken by the final flush whose wait for a slot had failed");
    }

    /// <summary>
    /// Ultra review #3, a despill's wait for the swap lock. A pause there whose continuation could not be registered
    /// threw with the despill's finally skipped: the tier it had read was never freed, its slot never given back, and its
    /// block never queued again — in flight, read by nobody, for the life of the process. Its catch ends the despill now.
    /// </summary>
    [Fact]
    public async Task A_despill_whose_wait_for_the_swap_lock_fails_frees_its_tier_and_queues_its_block_again()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        EarlierRunSpill(opts, dir, 9_000, 100_000, 300);
        var hold  = new TaskCompletionSource();
        var tiers = new System.Collections.Concurrent.ConcurrentQueue<HotTierSegment>();
        StorageEngine.RecoveryTierCreatedForTest.Value = tiers.Enqueue;
        OwnerAsync.LaterSuspensionFailsForTest.Value   = FailLaterSuspensionOnce("<DespillAsync>");
        StorageEngine engine;
        try { engine = HeldOneSlotEngine(opts, hold); }
        finally
        {
            StorageEngine.RecoveryTierCreatedForTest.Value = null;
            OwnerAsync.LaterSuspensionFailsForTest.Value   = null;
        }
        await using (engine)
        {
            engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
            Assert.True(engine.FlushLockForTest.Wait(0));   // held: the despill pauses for it
            try
            {
                hold.SetResult();
                WaitFor(() => engine.DespillsCompletedForTest >= 1, "the despill whose wait failed to end", ShortGuard);
            }
            finally { engine.FlushLockForTest.Release(); }

            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == 300, "the block to be despilled", ShortGuard);
            Assert.True(tiers.TryPeek(out var first) && first.IsDisposed, "the tier the failed despill had read was not freed");
            Assert.Equal(engine.FlushSlots, engine.FlushSlotsForTest.CurrentCount);
            Assert.Equal(300, ReadAll(engine).Count);
        }
    }

    /// <summary>
    /// Ultra review #3, a despill's wait for the heavy-phase permit, its tier listed and counted and its text charged.
    /// The catch gave the tier up and rethrew through the finally that gives the slot, the count and the text back and
    /// queues the block again — skipped: shutdown waited its whole budget for the count, the frozen tiers stayed closed to
    /// that much text for the life of the process, and the block was read by nobody. Its catch ends the despill now.
    /// </summary>
    [Fact]
    public async Task A_despill_whose_wait_for_its_permit_fails_gives_its_slot_count_and_text_back()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        EarlierRunTextSpill(opts, dir, 9_000, 100_000, 20, chars: 20_000);
        var hold = new TaskCompletionSource();
        OwnerAsync.LaterSuspensionFailsForTest.Value = FailLaterSuspensionOnce("<DespillAsync>");
        StorageEngine engine;
        try { engine = HeldOneSlotEngine(opts, hold); }
        finally { OwnerAsync.LaterSuspensionFailsForTest.Value = null; }
        await using (engine)
        {
            engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
            Assert.True(engine.FlushConcurrencyForTest.Wait(0));   // held: the despill pauses for it
            long chargedMeanwhile = 0;
            try
            {
                hold.SetResult();
                WaitFor(() => engine.DespillsCompletedForTest >= 1, "the despill whose wait failed to end", ShortGuard);
                chargedMeanwhile = engine.FrozenTextBytesForTest;
                Assert.True(engine.HeavyPhasesInFlight == 0 && engine.FlushSlotsForTest.CurrentCount == engine.FlushSlots,
                            $"heavy phases {engine.HeavyPhasesInFlight}, slots free {engine.FlushSlotsForTest.CurrentCount}/{engine.FlushSlots}, " +
                            $"text charged {chargedMeanwhile} B: the despill whose wait for its permit failed kept them");
            }
            finally { engine.FlushConcurrencyForTest.Release(); }

            Assert.Equal(0, chargedMeanwhile);
            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == 20, "the block to be despilled", ShortGuard);
            Assert.Equal(20, ReadAll(engine).Count);
        }
    }

    /// <summary>
    /// Round 7: a guarded finally is skipped not only at the await whose registration fails, but for the rest of that
    /// run of the method once a catch has taken the throw and the method goes on — until a suspension resumes, or until
    /// the method re-enters the try that holds the failed await, whose dispatch resumes it. The flush retry's attempt
    /// catches such a failure at its wait for the permit and goes round; with no real pause before the next attempt (a
    /// zero retry delay, the permit free by then) only that dispatch resets the state. The retry still publishes the
    /// block once, keeps every level it published, and gives its slot, its count and its permit back — a regression
    /// check of that road: the owners and the permit go back by plain code now, which does not depend on the dispatch
    /// (a finally there passes this test as well: the dispatch resets the state first).
    /// </summary>
    [Fact]
    public async Task A_retry_that_publishes_after_a_caught_failed_wait_gives_its_owners_back_and_unlinks_nothing()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        EarlierRunSpill(opts, dir, 9_000, 100_000, 200);
        var hold = new TaskCompletionSource();
        StorageEngine engine;
        int calls = 0;
        // The retry's first wait for the permit fails at its registration; the permit is let go as it fails, so the
        // next attempt — at once, with a zero delay — finds it free and suspends nowhere before it publishes.
        SemaphoreSlim? permit = null;
        OwnerAsync.LaterSuspensionFailsForTest.Value = FailLaterSuspensionOnce("<FlushRetryLoopAsync>", () => permit?.Release());
        try { engine = HeldOneSlotEngine(opts, hold); }
        finally { OwnerAsync.LaterSuspensionFailsForTest.Value = null; }
        permit = engine.FlushConcurrencyForTest;
        await using (engine)
        {
            engine._despillRetryDelay = TimeSpan.FromMilliseconds(50);
            engine._flushRetryDelay   = TimeSpan.FromMilliseconds(500);   // the retry's first pause, where its state machine is boxed
            // The despill's own flush fails before any write: its tier goes to a retry.
            engine._beforeLevelWriteForTest = _ =>
            {
                if (Interlocked.Increment(ref calls) == 1) throw new IOException("test: the despill's flush fails");
            };
            hold.SetResult();
            // The despill gives its permit back as it hands over: taken here, so the retry's attempt waits for it.
            WaitFor(() => Volatile.Read(ref calls) >= 1 && engine.FlushConcurrencyForTest.Wait(0), "the permit the despill gives back", ShortGuard);
            engine._flushRetryDelay = TimeSpan.Zero;   // and no real pause after the wait that fails
            WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == 200, "the retry to publish the block", ShortGuard);
            await Task.Delay(300);   // RetryEnded's give-up, had there been one, has run by now
            Assert.Equal(200, ReadAll(engine).Count);   // every published level still there, each event once
            Assert.Equal(0, engine.HeavyPhasesInFlight);
            Assert.Equal(engine.FlushSlots, engine.FlushSlotsForTest.CurrentCount);
            // And its permit: a finally around the flush was skipped on this road as well (round 7) — every finally of
            // an async method is guarded by the state, one whose try holds no await too.
            Assert.Equal(engine.FlushWidth, engine.FlushConcurrencyForTest.CurrentCount);
        }
    }

    /// <summary>
    /// Round 7: the despill loop's await of a despill fails at its registration — past the owner builder's tries, the road
    /// the later-suspension seam stands for — and the loop's catch takes it and goes on, the compiler's state still that
    /// suspension's. Every finally the loop then leaves is skipped until a suspension resumes, a lock statement's exit
    /// with them: the catch asked the queue for the block's failures under <c>_spillQueueLock</c>, and the loop's pool
    /// thread kept the monitor for good — the running despill then blocked settling its block, and so did every spill,
    /// despill and settle after it. The loop walks the queue in methods of its own now.
    /// </summary>
    [Fact]
    public async Task A_despill_loop_whose_await_of_a_despill_fails_keeps_no_lock_and_the_despill_settles()
    {
        string dir  = NewDir();
        var    opts = Options(dir);
        EarlierRunSpill(opts, dir, 9_000, 100_000, 50);
        var hold = new TaskCompletionSource();
        StorageEngine engine;
        int failed = 0, pastTheHold = 0;
        // The loop's await of the despill: its first later suspension on a Task (the signal's wait is a Task<bool>) — or,
        // when the loop reached its hold only once it was let go, and so never suspended there, its first suspension of
        // all, past the hold. Either throws into the loop the same way: a failed box or a failed registration.
        OwnerAsync.LaterSuspensionFailsForTest.Value = (sm, awaiter) =>
            sm.Name.Contains("<RunDespillLoopAsync>", StringComparison.Ordinal)
            && awaiter == typeof(System.Runtime.CompilerServices.ConfiguredTaskAwaitable.ConfiguredTaskAwaiter)
            && Interlocked.Exchange(ref failed, 1) == 0;
        OwnerAsync.FirstSuspensionFailsForTest.Value = sm =>
            sm.Name.Contains("<RunDespillLoopAsync>", StringComparison.Ordinal)
            && Volatile.Read(ref pastTheHold) == 1 && Interlocked.Exchange(ref failed, 1) == 0;
        try { engine = HeldOneSlotEngine(opts, hold); }
        finally
        {
            OwnerAsync.LaterSuspensionFailsForTest.Value = null;
            OwnerAsync.FirstSuspensionFailsForTest.Value = null;
        }
        engine._despillLoopIterationForTest = () => Volatile.Write(ref pastTheHold, 1);   // the top of a round: past the hold

        // Not disposed on failure: with the lock kept, the shutdown waits on it for good. And lock-free first — anything
        // that takes the lock (SpillFilesPending too) blocked as well — so the test fails rather than hangs.
        var read = new ManualResetEventSlim();
        engine._beforeLoadSpillForTest = _ => read.Wait(TimeSpan.FromSeconds(5));   // the despill suspends the loop's await
        hold.SetResult();
        WaitFor(() => Volatile.Read(ref failed) == 1, "the loop's await of the despill to fail", ShortGuard);
        read.Set();
        WaitFor(() => engine.DespillsCompletedForTest >= 1, "the despill to end (it settles its block under the queue's lock)", ShortGuard);
        Assert.True(Monitor.TryEnter(engine.SpillQueueLockForTest, TimeSpan.FromSeconds(5)), "the spill queue's lock was kept");
        Monitor.Exit(engine.SpillQueueLockForTest);
        WaitFor(() => engine.SpillFilesPending == 0 && engine.DespilledEvents == 50, "the despill to settle its block", ShortGuard);
        Assert.Equal(50, ReadAll(engine).Count);
        await engine.DisposeAsync();
    }
}
