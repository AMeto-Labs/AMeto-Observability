using System.Diagnostics;

namespace Ameto.Server;

/// <summary>
/// One TTL-cached walk of the data directory, classified by subdirectory.
///
/// <para>What this replaces: <c>/api/diagnostics</c> used to run five recursive directory
/// walks per call — <c>segments</c>, <c>metrics</c>, <c>traces</c>, <c>wal</c> and then the
/// whole data root again, which re-visits all four — so every file was stat'd twice and the
/// biggest directory (segments: one file per cold segment) was the bulk of it. The Settings
/// dashboard polls this endpoint every 10 s, so on a server with thousands of segments that
/// was thousands of <see cref="FileInfo"/> objects and syscalls per poll, forever.</para>
///
/// <para>Two changes: the <c>segments</c> directory is not walked — the size of its live
/// segments is the sum of <c>SegmentInfo.CompressedBytes</c> over the catalog, which is the .seg
/// file length (<c>SegmentWriter</c>/<c>SegmentReader</c> set it from the file size) and is
/// already in memory; and what remains is walked once and cached for <see cref="Ttl"/>. Sizes on
/// disk move slowly — a flush every few minutes — so a staleness window of under a minute is
/// invisible on a dashboard that rounds to MB.</para>
///
/// <para>The one thing in <c>segments</c> the catalog cannot answer for is a QUARANTINED segment,
/// <c>*.seg.corrupt</c>: unreadable at some start, set aside, excluded from the catalog, and kept
/// for an operator to inspect or remove. That is permanent storage an operator has to act on, and
/// StorageEngine warns about it at every start precisely so disk usage does not silently disagree
/// with retention — so it is counted, from a top-level, pattern-filtered enumeration of
/// <c>segments</c> that usually matches nothing (see <see cref="Snapshot.QuarantinedSegmentBytes"/>).</para>
///
/// <para>What is knowingly left out: transient <c>*.seg.tmp</c> and <c>*.mergemanifest</c> files in
/// <c>segments</c>, which exist only during a flush, merge or replication transfer and are not
/// storage the operator can act on. And the live-segment figure comes from the catalog, which
/// loads in the background after start, so for the first moments after a restart it under-reports
/// until the scan has published every segment.</para>
///
/// <para>One cache, one root at a time. The endpoint's instance is process-wide, and several
/// servers with different data directories can share a process (the integration tests do), so a
/// snapshot records the root it describes and a request for another root walks rather than being
/// handed a different directory's sizes. Alternating roots therefore defeats the cache; a server
/// has one root, so that costs nothing outside a test process.</para>
/// </summary>
public sealed class DataDirectoryStatsCache
{
    /// <summary>An immutable snapshot. Published by reference so readers can never tear.</summary>
    public sealed class Snapshot
    {
        public long MetricsBytes;
        public long TracesBytes;
        public long WalBytes;
        public long DatabaseBytes;
        /// <summary>Everything under the root that is not logs, metrics, traces, WAL or the DB.</summary>
        public long OtherBytes;
        /// <summary>
        /// <c>segments/*.seg.corrupt</c>: quarantined segments the catalog no longer lists. Logs
        /// storage, and not in any other figure here.
        /// </summary>
        public long QuarantinedSegmentBytes;
        public int  MetricsSegments;
        public int  TracesSegments;
        /// <summary>The data root this snapshot describes.</summary>
        public string Root = "";
        /// <summary>Stopwatch timestamp the walk finished at.</summary>
        public long TakenAt;
    }

    private readonly long _ttlTicks;
    private readonly Lock _firstFill = new();

    private Snapshot? _current;
    private int       _refreshing;   // 0 = idle, 1 = a walk is in flight

    /// <summary>How long a snapshot is served before the next caller refreshes it.</summary>
    public TimeSpan Ttl { get; }

    public DataDirectoryStatsCache(TimeSpan ttl)
    {
        Ttl       = ttl;
        _ttlTicks = (long)(ttl.TotalSeconds * Stopwatch.Frequency);
    }

    /// <summary>Walks performed since start — the test hook that proves the TTL holds.</summary>
    public int WalkCount => Volatile.Read(ref _walks);
    private int _walks;

    /// <summary>
    /// Test hook: runs on a caller that has just read an EXPIRED snapshot of its root, before it
    /// competes to refresh it — the window in which another caller's refresh can finish.
    /// </summary>
    internal Action? OnExpiredRead;

    /// <summary>
    /// The current breakdown, refreshing it first if it has aged past the TTL. The refresh
    /// runs on the calling thread but only ever on ONE thread: a second caller that arrives
    /// mid-walk is served the previous snapshot rather than starting a duplicate walk.
    /// </summary>
    public Snapshot Get(string dataRoot)
    {
        var cur = Volatile.Read(ref _current);

        if (cur is not null && IsOf(cur, dataRoot))
        {
            if (IsFresh(cur)) return cur;

            // Expired: one thread refreshes, everyone else keeps reading the stale snapshot
            // instead of queueing behind a directory walk on a request thread.
            OnExpiredRead?.Invoke();
            if (Interlocked.CompareExchange(ref _refreshing, 1, 0) != 0) return cur;
            try
            {
                // Winning the flag does not mean the snapshot is still expired: a refresh can
                // have finished, and released the flag, between our read above and the exchange.
                // Walking again would repeat it microseconds later.
                var latest = Volatile.Read(ref _current);
                if (latest is not null && IsOf(latest, dataRoot) && IsFresh(latest)) return latest;
                return Refresh(dataRoot);
            }
            finally { Volatile.Write(ref _refreshing, 0); }
        }

        // Nothing yet, or a snapshot of a different root: serving it would be a wrong answer
        // rather than a stale one, so block. Only one thread walks; the rest wait and take its
        // result.
        lock (_firstFill)
        {
            cur = Volatile.Read(ref _current);
            if (cur is not null && IsOf(cur, dataRoot) && IsFresh(cur)) return cur;
            return Refresh(dataRoot);
        }
    }

    private bool IsFresh(Snapshot s) => Stopwatch.GetTimestamp() - s.TakenAt < _ttlTicks;

    private static bool IsOf(Snapshot s, string dataRoot) => string.Equals(s.Root, dataRoot, StringComparison.Ordinal);

    private Snapshot Refresh(string dataRoot)
    {
        var fresh = Walk(dataRoot);
        fresh.Root    = dataRoot;
        fresh.TakenAt = Stopwatch.GetTimestamp();
        Volatile.Write(ref _current, fresh);
        Interlocked.Increment(ref _walks);
        return fresh;
    }

    /// <summary>
    /// Visits every file under the root exactly once, except the <c>segments</c> subtree, whose
    /// live segments the catalog already accounts for and whose quarantined ones are matched by
    /// name at its top level only.
    /// </summary>
    private static Snapshot Walk(string dataRoot)
    {
        var s = new Snapshot();
        try
        {
            var root = new DirectoryInfo(dataRoot);
            if (!root.Exists) return s;

            foreach (var f in root.EnumerateFiles("*", SearchOption.TopDirectoryOnly))
            {
                long len = Length(f);
                if (f.Name.StartsWith("Ameto.db", StringComparison.OrdinalIgnoreCase)) s.DatabaseBytes += len;
                else                                                                   s.OtherBytes    += len;
            }

            foreach (var d in root.EnumerateDirectories())
            {
                if (Is(d.Name, "segments"))
                {
                    // Live segments: from the catalog. Quarantined ones: here, because nothing
                    // else lists them and they stay on disk until an operator acts.
                    s.QuarantinedSegmentBytes = QuarantinedBytes(d);
                    continue;
                }

                if (Is(d.Name, "metrics"))
                {
                    (s.MetricsBytes, s.MetricsSegments) = DirStats(d, ".mts");
                }
                else if (Is(d.Name, "traces"))
                {
                    (s.TracesBytes, s.TracesSegments) = DirStats(d, ".trc");
                }
                else if (Is(d.Name, "wal"))
                {
                    s.WalBytes += DirStats(d, null).Bytes;
                }
                else
                {
                    s.OtherBytes += DirStats(d, null).Bytes;
                }
            }
        }
        catch { /* raced with a delete, or the root vanished — report what we counted */ }
        return s;

        static bool Is(string name, string expected) =>
            name.Equals(expected, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Bytes of <c>*.seg.corrupt</c> directly in <paramref name="segments"/> — where StorageEngine
    /// sets a segment aside. The name filter runs over the directory listing, so the .seg files
    /// around them cost a name comparison each and no <see cref="FileInfo"/>; there is usually
    /// nothing to match.
    /// </summary>
    private static long QuarantinedBytes(DirectoryInfo segments)
    {
        try
        {
            long total = 0;
            foreach (var f in segments.EnumerateFiles("*.seg.corrupt", SearchOption.TopDirectoryOnly))
                total += Length(f);
            return total;
        }
        catch { return 0; }
    }

    /// <summary>
    /// Recursive size of one directory, plus how many of its files carry
    /// <paramref name="segmentExt"/> (pass <c>null</c> to skip counting).
    /// </summary>
    /// <remarks>
    /// <see cref="DirectoryInfo.EnumerateFiles(string, SearchOption)"/> yields
    /// <see cref="FileInfo"/> instances whose length is already populated from the OS
    /// enumeration record, so this is one stat per file, not two.
    /// </remarks>
    private static (long Bytes, int Segments) DirStats(DirectoryInfo dir, string? segmentExt)
    {
        try
        {
            long total    = 0;
            int  segments = 0;
            foreach (var f in dir.EnumerateFiles("*", SearchOption.AllDirectories))
            {
                total += Length(f);
                if (segmentExt is not null &&
                    f.Extension.Equals(segmentExt, StringComparison.OrdinalIgnoreCase)) segments++;
            }
            return (total, segments);
        }
        catch { return (0, 0); }
    }

    private static long Length(FileInfo f)
    {
        try { return f.Length; } catch { return 0; }   // deleted between enumeration and read
    }
}
