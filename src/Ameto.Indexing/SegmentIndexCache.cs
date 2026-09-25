using Ameto.Core;

namespace Ameto.Indexing;

/// <summary>
/// Cross-query LRU cache of per-group segment indexes.
///
/// <para>Without it, every query re-read every surviving group's sections and re-derived what
/// the query before it had derived. Segments are immutable, and entries for files deleted by merge
/// or retention are simply never requested again and age out of the LRU. A PATH, though, can come
/// back with different bytes — a replicated segment re-imported under the name retention
/// unlinked — so the query path's entries carry an <see cref="IndexGroupFingerprint"/> of the bytes
/// they learned from, and one opened over different bytes is replaced, not trusted.</para>
///
/// <para><b>What an entry is.</b> On the query path (<see cref="AcquireOrAdd"/>, through
/// <see cref="SegmentIndexView"/>) an entry is a MEMO: what queries worked out about the group —
/// the postings of the buckets and trigrams they asked for, what was absent, the bloom's
/// verdicts — kilobytes, grown as queries ask new things and charged as it grows. It holds no
/// section and no native memory; the sections stay in the segment file, which the asking query
/// has mapped anyway. Until #80 an entry was the group's index decoded whole — 128 MB for a
/// prop-dense group against a 256 MB budget, which is why the cache held two groups and hit 2 %
/// of the time. <see cref="Insert"/> still takes any reader, and <see cref="TryAcquire"/> still
/// finds it, for callers and tests that build a reader themselves; neither ever hands out a memo,
/// which to them is a miss, and an inserted reader replaces one.</para>
///
/// <para>Ownership: a <see cref="SegmentIndexReader"/> made by <c>Load</c> holds NATIVE memory
/// (the bloom bits), so an entry is disposed exactly once — on eviction if unreferenced,
/// otherwise by the last <see cref="Lease"/> to release it. Callers interact only through leases;
/// a leased reader is guaranteed alive until the lease is released. A memo has nothing native to
/// free, and follows the same rule.</para>
///
/// <para>An inserted reader may lack its trigram section (the caller had no substring
/// predicate); a <see cref="TryAcquire"/> that needs trigrams treats it as a miss, and an insert
/// of the full reader replaces it (upgrade). A memo never lacks anything: it reads the trigram
/// section through the view the moment a query needs it.</para>
///
/// <para>Budget pressure is the only thing that used to remove an entry, so one wide
/// dashboard query filled the cache and the process held those bytes — managed postings AND
/// native bloom bits — until something else needed the room. On a server that then goes idle
/// nothing ever does, which is the "RSS ratchets after the first big query" shape. An
/// optional idle age (<see cref="IdleEvict"/>) drops entries nothing has read for that long;
/// because the LRU is ordered by last touch, the sweep stops at the first entry that is still
/// young and is therefore O(evicted), not O(entries).</para>
///
/// <para><b>Two budgets, because an entry can live in two places.</b> A reader's sections and
/// memo are managed; a loaded reader's bloom bits are <c>NativeMemory</c> — 15.6 % of a prop-dense
/// group's sections and 26.6 % of a thin one's by the repo's own <c>BloomSizingProbe</c>, and so
/// of a loaded reader, which keeps its sections packed. (Before #80 a reader decoded its sections
/// 3-4x, and the bits were 4.1-8.3 % of it.) One budget charged the whole thing against a share
/// of the GC's hard limit, so the native part spent managed headroom on memory the GC never sees.
/// The native share now has its own ceiling, taken of the PHYSICAL limit, and whichever is
/// reached first evicts.
/// Both figures are reported (<c>/api/diagnostics</c>) rather than merged into one, and so is the
/// eviction the native ceiling causes while the total still has room — the one an operator cannot
/// otherwise see. Since #80 the query path's memos keep the bloom's verdicts and not its bits, so
/// on a server the native figure stays at zero and this ceiling is a backstop for readers
/// inserted whole.</para>
///
/// <para><b>Sheddable.</b> Neither budget helps when the pressure is elsewhere: the RAM-pressure
/// loop flushes the hot tier, forces a collection and trims the working set, and none of that
/// touches native bloom bits. As an <see cref="IMemoryShedder"/> the cache can be told to let go
/// of everything — which it does on the same ownership rule as eviction, so a query holding a
/// lease keeps its reader until it is done.</para>
/// </summary>
public sealed class SegmentIndexCache : IDisposable, IMemoryShedder
{
    private readonly object                                   _lock = new();
    private readonly Dictionary<(string Path, int Group), Entry> _map  = new();
    private readonly LinkedList<Entry>                        _lru  = new(); // head = most recent
    private readonly long                                     _budgetBytes;
    private readonly long                                     _nativeBudgetBytes; // 0 = no separate ceiling
    private readonly long                                     _idleTicks;   // 0 = no idle eviction; in _time's units
    private readonly TimeProvider                             _time;
    private readonly ITimer?                                  _sweepTimer;
    private          long                                     _totalBytes;
    private          long                                     _nativeBytes;
    private          long                                     _hits, _misses;
    private          long                                     _idleEvicted;
    private          long                                     _shedEvicted;
    private          long                                     _nativeEvicted;
    private          long                                     _staleReplaced;
    private          IDisposable?                             _shedRegistration;

    public SegmentIndexCache(long budgetBytes) : this(budgetBytes, 0, default) { }

    /// <inheritdoc cref="SegmentIndexCache(long, long, TimeSpan)"/>
    public SegmentIndexCache(long budgetBytes, TimeSpan idleEvict) : this(budgetBytes, 0, idleEvict) { }

    /// <param name="budgetBytes">
    /// Total retained bytes the cache may hold — managed postings and native bloom bits together.
    /// Zero or negative disables the cache.
    /// </param>
    /// <param name="nativeBudgetBytes">
    /// Ceiling on the NATIVE part of that total (bloom bits). Zero or negative means no separate
    /// ceiling, which is what the plain constructors give a caller that has only one number.
    /// </param>
    /// <param name="idleEvict">
    /// Drop entries that nothing has acquired for this long. <see cref="TimeSpan.Zero"/> or
    /// less turns it off, which is the pre-existing behaviour (budget pressure only).
    /// </param>
    public SegmentIndexCache(long budgetBytes, long nativeBudgetBytes, TimeSpan idleEvict)
        : this(budgetBytes, nativeBudgetBytes, idleEvict, TimeProvider.System) { }

    /// <summary>The public constructor with its clock made explicit.</summary>
    /// <param name="time">
    /// The clock behind idle eviction: each entry's last-touch stamp, the sweep's cutoff and the
    /// sweep timer. <see cref="TimeProvider.System"/> outside tests. A test passes a clock it moves by
    /// hand, because against the wall clock a sweep timer ticking every few milliseconds raced every
    /// assertion, on a CI runner that can deschedule a thread for longer than the whole idle age.
    /// </param>
    internal SegmentIndexCache(long budgetBytes, long nativeBudgetBytes, TimeSpan idleEvict, TimeProvider time)
    {
        _budgetBytes       = budgetBytes;
        _nativeBudgetBytes = nativeBudgetBytes;
        _time              = time;
        // Past MaxIdleEvict the age is "never": treated as off, which is what it means, and which
        // keeps the timestamp conversion below inside a long on every platform.
        IdleEvict    = idleEvict > TimeSpan.Zero && idleEvict <= MaxIdleEvict ? idleEvict : TimeSpan.Zero;
        _idleTicks   = (long)(IdleEvict.TotalSeconds * time.TimestampFrequency);

        if (_idleTicks <= 0 || !Enabled) return;

        // A sweep only ever runs if the cache is enabled AND an idle age is configured. The
        // cadence is a quarter of the idle age so an entry is released within 1.25× of it;
        // at the 10-minute default that is one wake every 2.5 minutes, which on an idle
        // server reads one timestamp under the lock and returns. The callback is static and
        // takes its state through the timer, so the timer holds no closure.
        //
        // Capped at MaxSweepPeriod: System.Threading.Timer rejects a period above 0xFFFFFFFE ms
        // (~49.7 days), so a quarter of any idle age from ~199 days up used to throw out of the
        // DI factory and take every query endpoint down with it. Past four days an entry is
        // therefore released within a day of its idle age rather than within a quarter of it.
        long periodTicks = Math.Clamp(IdleEvict.Ticks / 4, TimeSpan.TicksPerMillisecond, MaxSweepPeriod.Ticks);
        var  period      = TimeSpan.FromTicks(periodTicks);
        _sweepTimer = time.CreateTimer(static s => ((SegmentIndexCache)s!).Sweep(), this, period, period);
    }

    /// <summary>
    /// Longest idle age taken literally. Anything longer — <see cref="TimeSpan.MaxValue"/> included —
    /// is "never" and turns idle eviction off. A century in Stopwatch ticks still fits a long at
    /// Linux's 1e9 ticks per second; TimeSpan.MaxValue does not.
    /// </summary>
    private static readonly TimeSpan MaxIdleEvict = TimeSpan.FromDays(100 * 366);

    /// <summary>Longest sweep cadence: far inside Timer's ~49.7-day limit, and a day late at worst.</summary>
    private static readonly TimeSpan MaxSweepPeriod = TimeSpan.FromDays(1);

    /// <summary>Idle age after which an untouched entry is evicted; <see cref="TimeSpan.Zero"/> = off.</summary>
    public TimeSpan IdleEvict { get; }

    /// <summary>Entries dropped by idle age (not by budget pressure) since start.</summary>
    public long IdleEvictedCount => Interlocked.Read(ref _idleEvicted);

    /// <summary>False when the budget is zero or negative — every acquire misses and inserts are not retained.</summary>
    public bool Enabled => _budgetBytes > 0;

    /// <summary>
    /// The budget this cache enforces — the one it was built with, not what configuration would
    /// derive now (a <c>GC.RefreshMemoryLimit</c> after a container resize changes the latter only).
    /// </summary>
    public long BudgetBytes => _budgetBytes;

    /// <summary>
    /// Group uses served without reading a section of the group — on the query path, a
    /// <see cref="SegmentIndexView"/> whose memo answered everything; through
    /// <see cref="TryAcquire"/>, an entry found.
    /// </summary>
    public long HitCount   => Interlocked.Read(ref _hits);

    /// <summary>Group uses that had to read a section: a question the memo had not seen, or an
    /// entry evicted since. Through <see cref="TryAcquire"/>, an entry not found.</summary>
    public long MissCount  => Interlocked.Read(ref _misses);
    public long TotalBytes { get { lock (_lock) return _totalBytes; } }
    public int  EntryCount { get { lock (_lock) return _map.Count; } }

    /// <summary>
    /// The part of <see cref="TotalBytes"/> held in <c>NativeMemory</c> (bloom bits) — bytes no
    /// collection can reclaim and that do not count against the GC's hard limit.
    /// </summary>
    public long NativeBytes { get { lock (_lock) return _nativeBytes; } }

    /// <summary>Ceiling on <see cref="NativeBytes"/>; 0 when there is no separate native ceiling.</summary>
    public long NativeBudgetBytes => _nativeBudgetBytes;

    /// <summary>Entries dropped by <see cref="Shed"/> (memory pressure) since start.</summary>
    public long ShedEvictedCount => Interlocked.Read(ref _shedEvicted);

    /// <summary>
    /// Entries dropped because the NATIVE ceiling was reached while the total budget still had
    /// room. Counted apart from every other eviction because it is the one nothing else reveals:
    /// the cache then sits far below its total budget for ever, evicting on every insert, and the
    /// only other clue is noticing that <see cref="NativeBytes"/> is pinned to
    /// <see cref="NativeBudgetBytes"/>. A number that climbs means this cache is bounded by its
    /// bloom bits rather than by the budget an operator set.
    /// </summary>
    public long NativeEvictedCount => Interlocked.Read(ref _nativeEvicted);

    /// <summary>Entries replaced because their path now holds different bytes (see
    /// <see cref="IndexGroupFingerprint"/>). Rare by construction; a count that climbs means
    /// segment files are being replaced under their own names.</summary>
    public long StaleReplacedCount => Interlocked.Read(ref _staleReplaced);

    internal sealed class Entry
    {
        public required (string Path, int Group) Key;
        public required SegmentIndexReader       Reader;
        public required bool                     HasTrigram;
        public required long                     Size;
        public          long                     NativeSize;  // the part of Size that is NativeMemory
        public          long                     Charged;     // Reader.ApproxRetainedBytes when Size last caught up with it
        public          IndexGroupFingerprint    Fingerprint; // the bytes the reader answers for; default for Insert
        public int  RefCount;                    // guarded by the cache lock
        public bool Doomed;                      // evicted/replaced — dispose at RefCount 0
        public long LastTouched;                 // the cache clock's timestamp of the last acquire
        public LinkedListNode<Entry>? Node;      // null once off the LRU
    }

    /// <summary>
    /// Keeps the underlying reader alive until released. Release exactly once: by
    /// <see cref="Dispose"/>, or — on the query path, where the lease decides the hit rate — by
    /// <see cref="Complete"/>.
    /// </summary>
    public readonly struct Lease : IDisposable
    {
        private readonly SegmentIndexCache _owner;
        private readonly Entry             _entry;
        internal Lease(SegmentIndexCache owner, Entry entry) { _owner = owner; _entry = entry; }
        public SegmentIndexReader Index => _entry.Reader;
        public void Dispose() => _owner.Release(_entry, Outcome.None);

        /// <summary>
        /// Releases a lease taken by <see cref="AcquireOrAdd"/> and counts the use: a HIT when the
        /// memo answered everything, a MISS when a section of the group had to be read.
        /// </summary>
        internal void Complete(bool readSections) =>
            _owner.Release(_entry, readSections ? Outcome.Miss : Outcome.Hit);
    }

    private enum Outcome : byte { None, Hit, Miss }

    /// <summary>
    /// What the cache itself keeps per query-path entry, beyond the memo: the entry, its LRU node
    /// and its share of the map. MEASURED (Release, 5 000 entries): an empty memo entry retains
    /// ~453 B, of which the memo is 176 B. With entries of a few hundred bytes to a few KB this is
    /// no longer a rounding error, so it is charged. <see cref="Insert"/> keeps charging exactly
    /// what its caller passes.
    /// </summary>
    internal const long EntryOverheadBytes = 280;

    /// <summary>
    /// The query path's entry point: a lease on the group's memo, created empty when the group has
    /// none. Never a miss by itself — whether the memo could answer is only known once the query
    /// is done with the group, so the hit or miss is counted by <see cref="Lease.Complete"/>.
    ///
    /// <para>A new memo is a few hundred bytes. Charged at that and inserted at the LRU head, it
    /// can only push out the tail; what it grows to is charged when the lease is released.</para>
    ///
    /// <para><paramref name="fingerprint"/> names the bytes the caller is about to lend the memo.
    /// An entry that learned from different bytes under the same path — the file was replaced —
    /// is unlisted like an eviction (a query still holding it keeps it until released) and a fresh
    /// memo takes its place. See <see cref="IndexGroupFingerprint"/>.</para>
    /// </summary>
    internal Lease AcquireOrAdd(string path, int group, in IndexGroupFingerprint fingerprint)
    {
        List<SegmentIndexReader>? toDispose = null;
        Lease lease;
        lock (_lock)
        {
            var key = (path, group);
            if (_map.TryGetValue(key, out var e))
            {
                if (e.Fingerprint == fingerprint)
                {
                    e.RefCount++;
                    e.LastTouched = _time.GetTimestamp();
                    _lru.Remove(e.Node!);
                    _lru.AddFirst(e.Node!);
                    return new Lease(this, e);
                }
                RemoveLocked(e, toDispose = []);                 // other bytes behind this path now
                Interlocked.Increment(ref _staleReplaced);
            }

            var  reader = SegmentIndexReader.CreateMemo();
            long charged = reader.ApproxRetainedBytes;
            long size    = charged + EntryOverheadBytes;
            e = new Entry
            {
                Key = key, Reader = reader, HasTrigram = true,   // a memo reads any section it needs
                Size = size, Charged = charged, RefCount = 1, LastTouched = _time.GetTimestamp(),
                Fingerprint = fingerprint,
            };
            e.Node       = _lru.AddFirst(e);
            _map[key]    = e;
            _totalBytes += size;
            EvictLocked(toDispose ??= []);
            lease = new Lease(this, e);
        }
        foreach (var r in toDispose) r.Dispose();
        return lease;
    }

    /// <summary>
    /// Acquires the cached reader for the group, or null on a miss — including the case
    /// where the cached entry lacks the trigram index the caller needs.
    /// </summary>
    public Lease? TryAcquire(string path, int group, bool needTrigram)
    {
        if (!Enabled) return null;
        lock (_lock)
        {
            // A memo (the query path's entry) owns no section and answers only through a view: to
            // this API it is not there.
            if (!_map.TryGetValue((path, group), out var e) || !e.Reader.OwnsSections || (needTrigram && !e.HasTrigram))
            {
                Interlocked.Increment(ref _misses);
                return null;
            }
            e.RefCount++;
            e.LastTouched = _time.GetTimestamp();
            _lru.Remove(e.Node!);
            _lru.AddFirst(e.Node!);
            Interlocked.Increment(ref _hits);
            return new Lease(this, e);
        }
    }

    /// <summary>
    /// Inserts a freshly built reader and returns a lease over the WINNING entry: a
    /// concurrent insert of the same group may have landed first, in which case the
    /// offered reader is disposed and the existing entry served — unless ours carries
    /// the trigram index the existing one lacks, which replaces it (upgrade). The
    /// caller must not touch <paramref name="reader"/> after this call except through
    /// the returned lease.
    /// </summary>
    public Lease Insert(string path, int group, bool hasTrigram, SegmentIndexReader reader, long sizeBytes)
    {
        if (!Enabled)
        {
            // Uncached mode: a doomed, unlisted entry — the lease's dispose is the reader's.
            return new Lease(this, new Entry
            {
                Key = (path, group), Reader = reader, HasTrigram = hasTrigram,
                Size = sizeBytes, RefCount = 1, Doomed = true,
            });
        }

        List<SegmentIndexReader>? toDispose = null;
        Lease lease;
        lock (_lock)
        {
            var key = (path, group);
            // An existing memo is never the better entry: it cannot answer through this API, and the
            // caller's reader can answer through a view as well.
            if (_map.TryGetValue(key, out var existing) && existing.Reader.OwnsSections && (existing.HasTrigram || !hasTrigram))
            {
                // Lost the race to an equal-or-better entry — serve that one, drop ours.
                (toDispose ??= []).Add(reader);
                existing.RefCount++;
                existing.LastTouched = _time.GetTimestamp();
                _lru.Remove(existing.Node!);
                _lru.AddFirst(existing.Node!);
                lease = new Lease(this, existing);
            }
            else
            {
                if (existing is not null)
                    RemoveLocked(existing, toDispose ??= []); // trigram upgrade replaces it

                var e = new Entry
                {
                    Key = key, Reader = reader, HasTrigram = hasTrigram,
                    Size = sizeBytes, RefCount = 1, LastTouched = _time.GetTimestamp(),
                    // Read off the reader rather than passed in: the caller charges one number,
                    // and only the reader knows how much of it the GC cannot see.
                    NativeSize = reader.ApproxNativeBytes,
                    Charged    = reader.ApproxRetainedBytes,
                };
                e.Node        = _lru.AddFirst(e);
                _map[key]     = e;
                _totalBytes  += sizeBytes;
                _nativeBytes += e.NativeSize;
                EvictLocked(toDispose ??= []);
                lease = new Lease(this, e);
            }
        }
        if (toDispose is not null)
            foreach (var r in toDispose) r.Dispose();
        return lease;
    }

    /// <summary>Unlists an entry; native memory is freed here only when nothing holds a lease.</summary>
    private void RemoveLocked(Entry e, List<SegmentIndexReader> toDispose)
    {
        _map.Remove(e.Key);
        if (e.Node is not null) { _lru.Remove(e.Node); e.Node = null; }
        _totalBytes  -= e.Size;
        _nativeBytes -= e.NativeSize;
        if (e.RefCount == 0) toDispose.Add(e.Reader);
        else e.Doomed = true;
    }

    /// <summary>
    /// Evicts from the LRU tail until BOTH budgets are met. A leased tail entry is still
    /// unlisted — its bytes stop counting and its last lease frees it — so one oversized group
    /// can never wedge either budget.
    /// </summary>
    private void EvictLocked(List<SegmentIndexReader> toDispose)
    {
        int nativeDriven = 0;
        while (OverBudgetLocked() && _lru.Last is { } tail)
        {
            // Attribute the removal before it happens. The native ceiling is the reason exactly
            // when the total budget still has room — the case an operator cannot otherwise see,
            // because the symptom is a cache that stays far below its budget and never improves
            // its hit rate. The two causes have different remedies, so one counter for both would
            // not be a diagnosis.
            if (_totalBytes <= _budgetBytes) nativeDriven++;
            RemoveLocked(tail.Value, toDispose);
        }
        if (nativeDriven > 0) Interlocked.Add(ref _nativeEvicted, nativeDriven);
    }

    /// <summary>
    /// Over the total budget, or over the native one. The native ceiling can bite while the
    /// total has room to spare: a cache of thin-event groups is a quarter bloom by weight, and
    /// those bytes are the ones outside the GC's limit.
    /// </summary>
    private bool OverBudgetLocked() =>
        _totalBytes > _budgetBytes ||
        (_nativeBudgetBytes > 0 && _nativeBytes > _nativeBudgetBytes);

    /// <summary>
    /// Drops every entry nothing has acquired for <see cref="IdleEvict"/>, and returns how
    /// many. Ownership is the same as budget eviction: the entry is unlisted and its bytes
    /// stop counting immediately, but the native bloom bits are freed by the LAST lease to
    /// be released — an idle sweep can never pull memory out from under a running query.
    /// Public so a test can drive it without waiting for the timer.
    /// </summary>
    public int Sweep()
    {
        if (_idleTicks <= 0) return 0;

        List<SegmentIndexReader>? toDispose = null;
        int evicted = 0;
        long cutoff = _time.GetTimestamp() - _idleTicks;
        lock (_lock)
        {
            // The LRU tail is the least recently touched entry, so the first young one ends
            // the sweep: everything ahead of it is younger still.
            while (_lru.Last is { } tail && tail.Value.LastTouched <= cutoff)
            {
                RemoveLocked(tail.Value, toDispose ??= []);
                evicted++;
            }
        }
        if (toDispose is not null)
            foreach (var r in toDispose) r.Dispose();
        if (evicted > 0) Interlocked.Add(ref _idleEvicted, evicted);
        return evicted;
    }

    // ── IMemoryShedder ────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public long ShedableBytes => TotalBytes;

    /// <inheritdoc/>
    public long ShedableNativeBytes => NativeBytes;

    /// <summary>
    /// Drops every entry and returns the retained bytes that releases.
    ///
    /// <para>Ownership is eviction's, not something stricter: an entry under lease is unlisted
    /// and stops counting immediately, and its reader — including the native bloom bits — is
    /// freed by the LAST lease to be released. A shed can therefore never pull memory out from
    /// under a running query, which is what makes it safe to call from the pressure loop while
    /// queries are in flight.</para>
    ///
    /// <para>The cost is latency, never correctness: the next query re-reads and re-decodes the
    /// sections it needs. That is the right trade when the alternative is the OOM killer, and it
    /// is why nothing sheds on a schedule — only under real pressure.</para>
    /// </summary>
    public long Shed()
    {
        List<SegmentIndexReader>? toDispose = null;
        long released = 0;
        int  dropped  = 0;
        lock (_lock)
        {
            while (_lru.Last is { } tail)
            {
                released += tail.Value.Size;
                RemoveLocked(tail.Value, toDispose ??= []);
                dropped++;
            }
        }
        if (toDispose is not null)
            foreach (var r in toDispose) r.Dispose();
        if (dropped > 0) Interlocked.Add(ref _shedEvicted, dropped);
        return released;
    }

    /// <summary>
    /// Registers this cache with <see cref="MemoryShedRegistry"/> so the RAM-pressure loop can
    /// shed it, and returns it for chaining from a DI factory. Idempotent; undone by
    /// <see cref="Dispose"/>.
    ///
    /// <para>Explicit rather than automatic in the constructor, because registration is a
    /// process-wide effect and a cache built by a test — or by a second host inside one process
    /// — has no business being shed when something else reports pressure.</para>
    /// </summary>
    public SegmentIndexCache RegisterForMemoryPressure()
    {
        lock (_lock) _shedRegistration ??= MemoryShedRegistry.Register(this);
        return this;
    }

    /// <summary>
    /// Drops a lease, and charges the entry for whatever its reader learned while leased.
    ///
    /// <para>A reader decodes lazily and remembers what it decoded, so it grows while a query
    /// holds it (<see cref="SegmentIndexReader.ApproxRetainedBytes"/>). The growth is charged
    /// HERE, once the query is done with it, rather than per lookup: the reader's memo takes its
    /// own lock and the cache's lock must never be held under it. Until then a leased entry may
    /// run ahead of its charge by what one group's lookups decode. An entry the growth pushes over
    /// budget is evicted like any other, from the LRU tail — which is only this entry when it no
    /// longer fits by itself. An entry already unlisted is not charged: its bytes left the total
    /// when it was evicted, and its last lease frees it.</para>
    /// </summary>
    private void Release(Entry e, Outcome outcome)
    {
        if      (outcome == Outcome.Hit)  Interlocked.Increment(ref _hits);
        else if (outcome == Outcome.Miss) Interlocked.Increment(ref _misses);

        List<SegmentIndexReader>? toDispose = null;
        lock (_lock)
        {
            e.RefCount--;
            if (e.Node is not null)
            {
                long now   = e.Reader.ApproxRetainedBytes;
                long grown = now - e.Charged;
                if (grown != 0)
                {
                    e.Charged   = now;
                    e.Size     += grown;
                    _totalBytes += grown;
                    if (grown > 0) EvictLocked(toDispose ??= []);
                }
            }
            else if (e.Doomed && e.RefCount == 0)
            {
                (toDispose ??= []).Add(e.Reader);
            }
        }
        if (toDispose is not null)
            foreach (var r in toDispose) r.Dispose();
    }

    /// <summary>
    /// Stops the sweep timer and gives up the shed registration. Cached readers are NOT disposed
    /// here: a lease may still be open on one, and the process is going away anyway — the same
    /// reasoning that lets an unreferenced entry sit in the LRU until something evicts it.
    ///
    /// <para>The registration must go even though the registry holds it weakly: a host disposed
    /// inside a still-running process (every integration test) would otherwise leave a cache to
    /// be shed on behalf of a server that no longer exists.</para>
    /// </summary>
    public void Dispose()
    {
        IDisposable? registration;
        lock (_lock) { registration = _shedRegistration; _shedRegistration = null; }
        registration?.Dispose();
        _sweepTimer?.Dispose();
    }
}
