using System.Collections.Concurrent;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// The engine's cold-segment catalog: a concurrent map plus a cached, immutable,
/// time-ordered snapshot of it for readers.
///
/// <para>Every query — and every live-tail poll, up to ten a second per open tab — asked
/// <c>GetSegments</c>, which walked the whole map, filtered with LINQ, sorted with a
/// keyed comparer and built a list, to arrive at the same answer as the poll before it:
/// the catalog changes a few times a minute, the question is asked thousands of times.
/// The snapshot is built once per catalog version and handed out until a mutation bumps
/// the version. Readers never take the lock.</para>
///
/// <para>The mutating surface is exactly the one <c>ConcurrentDictionary</c> offered the
/// engine (TryAdd / TryRemove / TryUpdate), with the same signatures, so the sites that
/// call them are unchanged; each bumps the version on success. A mutation that lands
/// while a snapshot is being built bumps the version past the one that build is tagged
/// with, so the next reader rebuilds — the stale build is served for at most that one
/// read, exactly as a per-call walk racing the same mutation would have been.</para>
/// </summary>
internal sealed class SegmentCatalog
{
    private readonly ConcurrentDictionary<SegmentKey, SegmentInfo> _map = new();
    private          int                                           _version;
    private readonly Lock                                          _buildLock = new();
    private volatile Snapshot?                                     _snapshot;

    /// <summary>Segments sorted by MaxTimestampTicks descending, tagged with the version they reflect.</summary>
    private sealed class Snapshot(int version, SegmentInfo[] byMaxTsDesc)
    {
        public readonly int           Version     = version;
        public readonly SegmentInfo[] ByMaxTsDesc = byMaxTsDesc;
    }

    // ── Map surface (unchanged signatures) ────────────────────────────────────

    public int  Count                                                  => _map.Count;
    public ICollection<SegmentInfo> Values                             => _map.Values;
    public bool ContainsKey(SegmentKey key)                            => _map.ContainsKey(key);
    public bool TryGetValue(SegmentKey key, out SegmentInfo info)      => _map.TryGetValue(key, out info!);

    public bool TryAdd(SegmentKey key, SegmentInfo info)
    {
        if (!_map.TryAdd(key, info)) return false;
        Interlocked.Increment(ref _version);
        return true;
    }

    public bool TryRemove(SegmentKey key, out SegmentInfo info)
    {
        if (!_map.TryRemove(key, out info!)) return false;
        Interlocked.Increment(ref _version);
        return true;
    }

    public bool TryRemove(KeyValuePair<SegmentKey, SegmentInfo> item)
    {
        if (!_map.TryRemove(item)) return false;
        Interlocked.Increment(ref _version);
        return true;
    }

    public bool TryUpdate(SegmentKey key, SegmentInfo newValue, SegmentInfo comparisonValue)
    {
        if (!_map.TryUpdate(key, newValue, comparisonValue)) return false;
        Interlocked.Increment(ref _version);
        return true;
    }

    // ── Read side ─────────────────────────────────────────────────────────────

    /// <summary>
    /// The segments overlapping [<paramref name="fromTicks"/>, <paramref name="toTicks"/>],
    /// newest MaxTs first — the order the executor's backward priming wants. An exact-sized
    /// array per call (count, then fill); the sort itself is paid once per catalog version.
    /// </summary>
    public IReadOnlyList<SegmentInfo> GetOverlapping(long fromTicks, long toTicks)
    {
        var sorted = Sorted();
        if (fromTicks == long.MinValue && toTicks == long.MaxValue) return sorted;

        int n = 0;
        for (int i = 0; i < sorted.Length; i++)
        {
            var s = sorted[i];
            if (s.MaxTimestampTicks >= fromTicks && s.MinTimestampTicks <= toTicks) n++;
        }
        if (n == 0) return Array.Empty<SegmentInfo>();

        var result = new SegmentInfo[n];
        int j = 0;
        for (int i = 0; i < sorted.Length; i++)
        {
            var s = sorted[i];
            if (s.MaxTimestampTicks >= fromTicks && s.MinTimestampTicks <= toTicks) result[j++] = s;
        }
        return result;
    }

    private SegmentInfo[] Sorted()
    {
        var snap = _snapshot;
        int v    = Volatile.Read(ref _version);
        if (snap is not null && snap.Version == v) return snap.ByMaxTsDesc;

        lock (_buildLock)
        {
            snap = _snapshot;
            v    = Volatile.Read(ref _version);      // re-read UNDER the lock, before the walk
            if (snap is not null && snap.Version == v) return snap.ByMaxTsDesc;

            var arr = new SegmentInfo[_map.Count];
            int n   = 0;
            foreach (var s in _map.Values)
            {
                if (n == arr.Length) Array.Resize(ref arr, arr.Length * 2 + 1);   // grew under us
                arr[n++] = s;
            }
            if (n != arr.Length) Array.Resize(ref arr, n);
            arr.AsSpan().Sort(default(ByMaxTsDescStable));
            _snapshot = new Snapshot(v, arr);
            return arr;
        }
    }

    /// <summary>
    /// MaxTs descending, ties broken by key so the order is deterministic across builds —
    /// the LINQ OrderByDescending this replaces was stable on the map's enumeration order,
    /// which is itself not deterministic across mutations, so no caller can have relied on
    /// a tie order; this one is at least the same every time.
    /// </summary>
    private struct ByMaxTsDescStable : IComparer<SegmentInfo>
    {
        public int Compare(SegmentInfo? a, SegmentInfo? b)
        {
            int c = b!.MaxTimestampTicks.CompareTo(a!.MaxTimestampTicks);
            if (c != 0) return c;
            c = a.NodeId.Value.CompareTo(b.NodeId.Value);
            return c != 0 ? c : a.Id.Value.CompareTo(b.Id.Value);
        }
    }
}
