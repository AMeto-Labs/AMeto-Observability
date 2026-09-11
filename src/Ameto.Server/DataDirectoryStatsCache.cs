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
/// <para>Two changes: the <c>segments</c> directory is not walked at all — its size is the sum
/// of <c>SegmentInfo.CompressedBytes</c> over the catalog, which is the .seg file length
/// (<c>SegmentWriter</c>/<c>SegmentReader</c> set it from the file size) and is already in
/// memory; and what remains is walked once and cached for <see cref="Ttl"/>. Sizes on disk move
/// slowly — a flush every few minutes — so a staleness window of under a minute is invisible on
/// a dashboard that rounds to MB.</para>
///
/// <para>Not walking <c>segments</c> means transient <c>*.seg.tmp</c> and
/// <c>*.mergemanifest</c> files there are no longer counted in the total. They exist only
/// during a flush, merge or replication transfer, and are not storage the operator can act on.</para>
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
        public int  MetricsSegments;
        public int  TracesSegments;
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
    /// The current breakdown, refreshing it first if it has aged past the TTL. The refresh
    /// runs on the calling thread but only ever on ONE thread: a second caller that arrives
    /// mid-walk is served the previous snapshot rather than starting a duplicate walk.
    /// </summary>
    public Snapshot Get(string dataRoot)
    {
        long now = Stopwatch.GetTimestamp();
        var  cur = Volatile.Read(ref _current);

        if (cur is not null && now - cur.TakenAt < _ttlTicks) return cur;

        // First call after start: block, because serving zeroes would be a wrong answer
        // rather than a stale one. Only one thread walks; the rest wait and take its result.
        if (cur is null)
        {
            lock (_firstFill)
            {
                cur = Volatile.Read(ref _current);
                if (cur is not null) return cur;
                return Refresh(dataRoot);
            }
        }

        // Expired: one thread refreshes, everyone else keeps reading the stale snapshot
        // instead of queueing behind a directory walk on a request thread.
        if (Interlocked.CompareExchange(ref _refreshing, 1, 0) != 0) return cur;
        try     { return Refresh(dataRoot); }
        finally { Volatile.Write(ref _refreshing, 0); }
    }

    private Snapshot Refresh(string dataRoot)
    {
        var fresh = Walk(dataRoot);
        fresh.TakenAt = Stopwatch.GetTimestamp();
        Volatile.Write(ref _current, fresh);
        Interlocked.Increment(ref _walks);
        return fresh;
    }

    /// <summary>
    /// Visits every file under the root exactly once, except the <c>segments</c> subtree,
    /// which the catalog already accounts for.
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
                if (Is(d.Name, "segments")) continue;              // logs: from the catalog

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
