namespace Ameto.Indexing;

/// <summary>
/// Cross-query LRU cache of deserialised per-group segment indexes.
///
/// <para>Without it, every query fully re-read and re-decoded every surviving group's
/// inverted (and trigram) sections — multi-MB managed structures, hundreds of thousands
/// of strings and arrays per dashboard refresh — because the reader was created and
/// disposed inside the prefilter loop. Segments are immutable and their file names carry
/// never-reused ids, so a (path, group) key can never serve stale data; entries for
/// files deleted by merge or retention are simply never requested again and age out of
/// the LRU under budget pressure.</para>
///
/// <para>Ownership: <see cref="SegmentIndexReader"/> holds NATIVE memory (the bloom
/// bits), so an entry is disposed exactly once — on eviction if unreferenced, otherwise
/// by the last <see cref="Lease"/> to release it. Callers interact only through leases;
/// a leased reader is guaranteed alive until the lease is disposed.</para>
///
/// <para>Trigram sections are the largest thing in a segment (~43% of the file) and many
/// queries never consult them, so an entry may be cached WITHOUT its trigram index. A
/// query that needs trigrams treats such an entry as a miss and re-inserts the full
/// reader in its place (upgrade); one that does not is happy with either.</para>
/// </summary>
public sealed class SegmentIndexCache
{
    private readonly object                                   _lock = new();
    private readonly Dictionary<(string Path, int Group), Entry> _map  = new();
    private readonly LinkedList<Entry>                        _lru  = new(); // head = most recent
    private readonly long                                     _budgetBytes;
    private          long                                     _totalBytes;
    private          long                                     _hits, _misses;

    public SegmentIndexCache(long budgetBytes) => _budgetBytes = budgetBytes;

    /// <summary>False when the budget is zero or negative — every acquire misses and inserts are not retained.</summary>
    public bool Enabled => _budgetBytes > 0;

    public long HitCount   => Interlocked.Read(ref _hits);
    public long MissCount  => Interlocked.Read(ref _misses);
    public long TotalBytes { get { lock (_lock) return _totalBytes; } }
    public int  EntryCount { get { lock (_lock) return _map.Count; } }

    internal sealed class Entry
    {
        public required (string Path, int Group) Key;
        public required SegmentIndexReader       Reader;
        public required bool                     HasTrigram;
        public required long                     Size;
        public int  RefCount;                    // guarded by the cache lock
        public bool Doomed;                      // evicted/replaced — dispose at RefCount 0
        public LinkedListNode<Entry>? Node;      // null once off the LRU
    }

    /// <summary>Keeps the underlying reader alive until disposed. Dispose exactly once.</summary>
    public readonly struct Lease : IDisposable
    {
        private readonly SegmentIndexCache _owner;
        private readonly Entry             _entry;
        internal Lease(SegmentIndexCache owner, Entry entry) { _owner = owner; _entry = entry; }
        public SegmentIndexReader Index => _entry.Reader;
        public void Dispose() => _owner.Release(_entry);
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
            if (!_map.TryGetValue((path, group), out var e) || (needTrigram && !e.HasTrigram))
            {
                Interlocked.Increment(ref _misses);
                return null;
            }
            e.RefCount++;
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
            if (_map.TryGetValue(key, out var existing) && (existing.HasTrigram || !hasTrigram))
            {
                // Lost the race to an equal-or-better entry — serve that one, drop ours.
                (toDispose ??= []).Add(reader);
                existing.RefCount++;
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
                    Size = sizeBytes, RefCount = 1,
                };
                e.Node       = _lru.AddFirst(e);
                _map[key]    = e;
                _totalBytes += sizeBytes;
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
        _totalBytes -= e.Size;
        if (e.RefCount == 0) toDispose.Add(e.Reader);
        else e.Doomed = true;
    }

    /// <summary>
    /// Evicts from the LRU tail down to budget. A leased tail entry is still unlisted —
    /// its bytes stop counting and its last lease frees it — so one oversized group can
    /// never wedge the budget.
    /// </summary>
    private void EvictLocked(List<SegmentIndexReader> toDispose)
    {
        while (_totalBytes > _budgetBytes && _lru.Last is { } tail)
            RemoveLocked(tail.Value, toDispose);
    }

    private void Release(Entry e)
    {
        SegmentIndexReader? dispose = null;
        lock (_lock)
        {
            e.RefCount--;
            if (e.Doomed && e.RefCount == 0) dispose = e.Reader;
        }
        dispose?.Dispose();
    }
}
