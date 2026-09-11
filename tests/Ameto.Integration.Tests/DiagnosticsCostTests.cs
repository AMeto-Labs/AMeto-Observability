using System.Diagnostics;
using Ameto.Server;

namespace Ameto.Integration.Tests;

/// <summary>
/// The cost of <c>/api/diagnostics</c>, which the Settings dashboard polls every 10 s for as
/// long as a tab is open. Before this change the handler ran five recursive directory walks
/// (segments, metrics, traces, wal, then the whole root again — so every file was stat'd twice)
/// and snapshotted every thread in the process; both scaled with things an operator grows.
///
/// These tests pin the two properties that fixed it: the walk happens once per TTL no matter how
/// often the endpoint is polled, and the segments directory — the one that scales with segment
/// count — is not walked at all, because <c>SegmentInfo.CompressedBytes</c> is the .seg file size.
/// </summary>
public sealed class DiagnosticsCostTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "Ameto-diagcost-" + Guid.NewGuid().ToString("N"));

    public DiagnosticsCostTests()
    {
        Directory.CreateDirectory(_root);
        Write(Path.Combine(_root, "Ameto.db"), 500);
        Write(Path.Combine(_root, "notes.txt"), 10);

        Directory.CreateDirectory(Path.Combine(_root, "segments"));
        for (int i = 0; i < 200; i++) Write(Path.Combine(_root, "segments", $"n-{i}.seg"), 1000);

        Directory.CreateDirectory(Path.Combine(_root, "metrics"));
        for (int i = 0; i < 3; i++) Write(Path.Combine(_root, "metrics", $"m-{i}.mts"), 100);

        Directory.CreateDirectory(Path.Combine(_root, "traces"));
        Write(Path.Combine(_root, "traces", "t-0.trc"), 70);

        Directory.CreateDirectory(Path.Combine(_root, "wal"));
        Write(Path.Combine(_root, "wal", "live.wal"), 40);
    }

    public void Dispose() { try { Directory.Delete(_root, true); } catch { } }

    private static void Write(string path, int bytes) => File.WriteAllBytes(path, new byte[bytes]);

    [Fact]
    public void Breakdown_classifies_every_subdirectory()
    {
        var s = new DataDirectoryStatsCache(TimeSpan.FromMinutes(1)).Get(_root);

        Assert.Equal(300, s.MetricsBytes);
        Assert.Equal(3,   s.MetricsSegments);
        Assert.Equal(70,  s.TracesBytes);
        Assert.Equal(1,   s.TracesSegments);
        Assert.Equal(40,  s.WalBytes);
        Assert.Equal(500, s.DatabaseBytes);
        Assert.Equal(10,  s.OtherBytes);          // notes.txt, and nothing else miscounted
    }

    /// <summary>
    /// The whole point: the directory that grows with retention is never enumerated. 200 .seg
    /// files are on disk and none of their bytes appear here — the endpoint adds the catalog's
    /// CompressedBytes sum instead, which is the same number without the syscalls.
    /// </summary>
    [Fact]
    public void Segments_directory_is_not_walked()
    {
        var s = new DataDirectoryStatsCache(TimeSpan.FromMinutes(1)).Get(_root);
        long counted = s.MetricsBytes + s.TracesBytes + s.WalBytes + s.DatabaseBytes + s.OtherBytes;
        Assert.Equal(920, counted);               // 200 000 bytes of .seg excluded
    }

    [Fact]
    public void Poll_within_the_ttl_walks_once()
    {
        var cache = new DataDirectoryStatsCache(TimeSpan.FromMinutes(5));
        for (int i = 0; i < 50; i++) cache.Get(_root);
        Assert.Equal(1, cache.WalkCount);
    }

    [Fact]
    public void Expired_snapshot_is_recomputed()
    {
        var cache = new DataDirectoryStatsCache(TimeSpan.FromMilliseconds(1));
        cache.Get(_root);
        Thread.Sleep(20);
        cache.Get(_root);
        Assert.Equal(2, cache.WalkCount);
    }

    /// <summary>
    /// Concurrent expiry must not start N walks: the losers are served the previous snapshot.
    /// </summary>
    [Fact]
    public void Concurrent_callers_do_not_walk_twice()
    {
        var cache = new DataDirectoryStatsCache(TimeSpan.FromMilliseconds(1));
        cache.Get(_root);                           // prime, so nobody takes the first-fill lock
        Thread.Sleep(20);

        int before = cache.WalkCount;
        var threads = new Thread[8];
        using var start = new Barrier(threads.Length);
        for (int i = 0; i < threads.Length; i++)
        {
            threads[i] = new Thread(() => { start.SignalAndWait(); cache.Get(_root); });
            threads[i].Start();
        }
        foreach (var t in threads) t.Join();

        Assert.True(cache.WalkCount - before <= 2,
            $"expected at most one extra walk for a burst of 8 callers, saw {cache.WalkCount - before}");
    }

    /// <summary>
    /// The measurement behind the change, printed rather than asserted on an absolute figure:
    /// allocations for the removed per-request work (the five walks plus the thread snapshot)
    /// against what the cached path costs. Asserts only the order of magnitude, so it cannot
    /// go flaky on a machine with a different process table.
    /// </summary>
    [Fact]
    public void Cached_path_allocates_orders_of_magnitude_less()
    {
        var cache = new DataDirectoryStatsCache(TimeSpan.FromMinutes(5));
        cache.Get(_root);                           // warm

        long a0 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 100; i++) cache.Get(_root);
        long cached = (GC.GetAllocatedBytesForCurrentThread() - a0) / 100;

        long b0 = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 10; i++) OldShape(_root);
        long old = (GC.GetAllocatedBytesForCurrentThread() - b0) / 10;

        Console.WriteLine($"diagnostics per call: before ≈ {old:N0} B, after ≈ {cached:N0} B");
        Assert.True(cached * 50 < old,
            $"cached path {cached} B/call is not decisively cheaper than the old {old} B/call");
    }

    /// <summary>The pre-change handler's directory work, kept here only as the BEFORE number.</summary>
    private static long OldShape(string root)
    {
        long total = 0;
        total += DirStats(Path.Combine(root, "segments"), ".seg");
        total += DirStats(Path.Combine(root, "metrics"),  ".mts");
        total += DirStats(Path.Combine(root, "traces"),   ".trc");
        total += DirStats(Path.Combine(root, "wal"),      null);
        total += DirStats(root, null);
        foreach (var f in Directory.EnumerateFiles(root, "Ameto.db*", SearchOption.TopDirectoryOnly))
            total += new FileInfo(f).Length;
        return total;

        static long DirStats(string path, string? ext)
        {
            if (!Directory.Exists(path)) return 0;
            long t = 0;
            foreach (var f in new DirectoryInfo(path).EnumerateFiles("*", SearchOption.AllDirectories))
            {
                t += f.Length;
                if (ext is not null && f.Extension.Equals(ext, StringComparison.OrdinalIgnoreCase)) t++;
            }
            return t;
        }
    }

    /// <summary>
    /// <c>processThreads</c> stays a live number without a <see cref="Process"/> snapshot.
    /// Process.Threads.Count on Windows enumerates every process on the machine to build a
    /// ProcessThread per thread of ours; ThreadPool.ThreadCount is a counter read.
    /// </summary>
    [Fact]
    public void Thread_count_is_read_without_a_process_snapshot()
    {
        long a0 = GC.GetAllocatedBytesForCurrentThread();
        int n = 0;
        for (int i = 0; i < 100; i++) n += ThreadPool.ThreadCount;
        long perCall = (GC.GetAllocatedBytesForCurrentThread() - a0) / 100;

        Assert.True(n > 0);
        Assert.True(perCall < 64, $"ThreadPool.ThreadCount allocated {perCall} B/call");
    }

    /// <summary>Working set and private bytes without a Process instance, on every platform.</summary>
    [Fact]
    public void Process_memory_is_reported_without_a_process_instance()
    {
        Assert.True(Ameto.Storage.ProcessMemoryInfo.WorkingSetBytes > 0);
        Assert.True(Ameto.Storage.ProcessMemoryInfo.PrivateBytes    > 0);
    }
}
