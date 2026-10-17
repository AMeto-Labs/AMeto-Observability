using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using LogLevel = Ameto.Core.LogLevel;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE THREE WRITE-AHEAD LOGS HAVE THEIR WHOLE FILE'S BLOCKS FROM EVERY OPEN AND EVERY GROWTH, AND A FAILED
/// OPEN LEAVES THE FILE AS IT FOUND IT (round-6 review R5-H1, R5-H2).
///
/// <para>Each log is written through a mapping over a file sized with SetLength — on Linux ftruncate, a
/// sparse file — and a write into a page of a full volume is SIGBUS. Round 5 reserved the logs' WAL and
/// only what the metric WAL's open added. The span WAL reserved nothing: trace ingest killed the process
/// (exit 135) after ~600 spans once the volume filled. A log reopened at its size — every metric and
/// span WAL at every start, an earlier build's sparse file, the sized file a failed reservation left —
/// was mapped over its holes: the metric engine's Ingest died with AccessViolation (exit 134). A
/// failed open left a sized, unreserved file for the next start to map.</para>
///
/// <para>The platform-independent facts watch <see cref="FileSpace.Reserve"/> through its seam
/// (<see cref="FileSpace.ReserveForTest"/>), which sees every call on every platform and refuses one as a
/// full Linux volume would. The On_linux facts read the blocks a file has on disk (stat) and return early
/// elsewhere; the backend-linux CI job runs this class and turns an early return into a failure.</para>
/// </summary>
public sealed class WalReservationTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-walres-" + Guid.NewGuid().ToString("N"));

    public WalReservationTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        FileSpace.ReserveForTest.Value = null;
        try { Directory.Delete(_dir, true); } catch { }
    }

    private string SpanPath   => Path.Combine(_dir, "spans.wal");
    private string MetricPath => Path.Combine(_dir, "metrics.wal");
    private string LogPath    => Path.Combine(_dir, "3-42.wal");

    private readonly record struct Reservation(long Offset, long Length, long FileLength);

    /// <summary>Records every reservation of this flow; <paramref name="refuse"/> says which to refuse, as a full volume.</summary>
    private static List<Reservation> Watch(Func<long, long, bool>? refuse = null)
    {
        var calls = new List<Reservation>();
        FileSpace.ReserveForTest.Value = (handle, offset, length) =>
        {
            lock (calls) calls.Add(new Reservation(offset, length, RandomAccess.GetLength(handle)));
            return refuse is not null && refuse(offset, length)
                ? new IOException($"test: could not reserve {length:N0} bytes at {offset:N0}: the volume is full")
                : null;
        };
        return calls;
    }

    private static void StopWatching() => FileSpace.ReserveForTest.Value = null;

    /// <summary>The open reserved the whole file it mapped: [0, its length then).</summary>
    private static void AssertWholeFileReserved(List<Reservation> calls, string path, string what)
    {
        long length = new FileInfo(path).Length;
        Assert.True(calls.Exists(static c => c.Offset == 0 && c.Length == c.FileLength),
            calls.Count == 0
                ? $"{what} reserved nothing of its {length:N0}-byte file"
                : $"{what} reserved {string.Join(", ", calls.ConvertAll(static c => $"[{c.Offset:N0}, {c.Offset + c.Length:N0}) of {c.FileLength:N0}"))}, not the whole file");
    }

    private static SpanIngestItem Span(int i, int attrBytes = 0) => new()
    {
        TraceId           = new TraceId((ulong)(i + 1) * 0x9E3779B97F4A7C15UL, (ulong)(i + 7)),
        SpanId            = new SpanId((ulong)(i + 100)),
        ParentSpanId      = default,
        StartTimeUnixNano = 1_785_000_000_000_000_000L + i,
        DurationNanos     = 1_000_000L,
        Name              = $"GET /api/thing/{i}",
        ServiceName       = "MintRoute.API",
        Kind              = SpanKind.Server,
        Status            = SpanStatusCode.Unset,
        AttributesBytes   = new byte[attrBytes],
    };

    private static MetricIngestItem Gauge(int i) => new()
    {
        Name              = "cpu",
        Kind              = MetricKind.Gauge,
        Unit              = "ms",
        Labels            = new LabelSet([new("service.name", "MintRoute.API")]),
        TimestampUnixNano = 1_785_300_000_000_000_000L + i,
        ScalarValue       = i,
    };

    private static List<SpanIngestItem> ReadSpans(string path)
    {
        using var wal = SpanWriteAheadLog.Open(path, initialCapacity: 8 * 1024);
        return wal.ReadAll();
    }

    private static int CountPoints(string path)
    {
        using var wal = MetricWriteAheadLog.Open(path, 64 * 1024);
        return wal.ReadAll(out _).Count;
    }

    // ── The span WAL (R5-H1) ──────────────────────────────────────────────────

    /// <summary>
    /// A new span log and a reopened one each reserve the whole file before anything is written through
    /// the mapping. It reserved nothing at all.
    /// </summary>
    [Fact]
    public void Every_open_of_the_span_log_reserves_its_whole_file()
    {
        try
        {
            var calls = Watch();
            using (var wal = SpanWriteAheadLog.Open(SpanPath, initialCapacity: 64 * 1024))
            {
                AssertWholeFileReserved(calls, SpanPath, "the span log's first open");
                wal.Append(Span(1));
            }

            calls.Clear();
            using (SpanWriteAheadLog.Open(SpanPath, initialCapacity: 64 * 1024))
                AssertWholeFileReserved(calls, SpanPath, "the span log's reopen");
        }
        finally { StopWatching(); }
    }

    /// <summary>A growth reserves what it adds — [the old size, the new size) — before the re-map writes into it.</summary>
    [Fact]
    public void A_span_log_growth_reserves_what_it_adds()
    {
        try
        {
            using var wal = SpanWriteAheadLog.Open(SpanPath, initialCapacity: 8 * 1024);
            long before   = new FileInfo(SpanPath).Length;
            long capacity = wal.CapacityForTest;
            var  calls    = Watch();
            for (int i = 0; wal.CapacityForTest == capacity; i++)
            {
                Assert.True(i < 100, "setup: the span log did not grow");
                wal.Append(Span(i, attrBytes: 900));
            }
            long after = new FileInfo(SpanPath).Length;
            Assert.True(calls.Exists(c => c.Offset == before && c.Length == after - before),
                $"the span log grew from {before:N0} to {after:N0} bytes and reserved " +
                (calls.Count == 0 ? "nothing" : string.Join(", ", calls.ConvertAll(static c => $"[{c.Offset:N0}, {c.Offset + c.Length:N0})"))));
        }
        finally { StopWatching(); }
    }

    /// <summary>
    /// A growth the volume cannot hold fails the append that needed it, with the IOException the trace
    /// engine answers, and is undone: the file back at its old size, the old mapping in place, every span
    /// already appended kept, and the log taking appends again once there is room. The extension was
    /// written into unreserved — SIGBUS on a full Linux volume.
    /// </summary>
    [Fact]
    public void A_span_log_growth_the_volume_cannot_hold_fails_its_append_and_is_undone()
    {
        try
        {
            int appended = 0;
            long sized;
            using (var wal = SpanWriteAheadLog.Open(SpanPath, initialCapacity: 8 * 1024))
            {
                sized = new FileInfo(SpanPath).Length;
                long capacity = wal.CapacityForTest;
                Watch(refuse: static (offset, _) => offset > 0);   // a growth's, not an open's
                Exception? refused = null;
                for (int i = 0; i < 100 && refused is null; i++)
                {
                    refused = Record.Exception(() => wal.Append(Span(i, attrBytes: 900)));
                    if (refused is null) appended++;
                }

                Assert.True(refused is IOException,
                    refused is null
                        ? $"the growth went on past a reservation that failed: {appended} spans appended, the log grown to {new FileInfo(SpanPath).Length:N0} bytes"
                        : $"the refused growth threw {refused.GetType().Name}, not an IOException");
                Assert.Equal(sized, new FileInfo(SpanPath).Length);
                Assert.Equal(capacity, wal.CapacityForTest);

                StopWatching();
                wal.Append(Span(1_000, attrBytes: 900));   // room again: it grows, and takes the span
            }

            var replayed = ReadSpans(SpanPath);
            Assert.Equal(appended + 1, replayed.Count);
            Assert.Equal(1_785_000_000_000_000_000L + 1_000, replayed[^1].StartTimeUnixNano);
        }
        finally { StopWatching(); }
    }

    /// <summary>
    /// Round-6 review R6-H1. A growth step the volume cannot hold is tried again at what the append needs. The whole step
    /// — doubling, then up to a hot tier's 27 MB — was reserved or nothing, and a volume with room for the next spans
    /// refused them: the drainer dropped them after the ring had acknowledged them (835 of 850 stored with 10.5 MB free
    /// in the review's probe). Here the step's reservation is refused, as a nearly full volume refuses it, and the
    /// smaller one that follows is not.
    /// </summary>
    [Fact]
    public void A_span_log_growth_step_the_volume_cannot_hold_grows_by_what_the_append_needs()
    {
        try
        {
            int appended = 0, growths = 0;
            using (var wal = SpanWriteAheadLog.Open(SpanPath, initialCapacity: 8 * 1024))
            {
                long capacity = wal.CapacityForTest;
                Watch(refuse: (offset, _) => offset > 0 && Interlocked.Increment(ref growths) == 1);   // the first growth's
                while (wal.CapacityForTest == capacity)
                {
                    Assert.True(appended < 100, "setup: the span log did not grow");
                    wal.Append(Span(appended, attrBytes: 900));   // the IOException, before: the step was all or nothing
                    appended++;
                }
                Assert.True(Volatile.Read(ref growths) >= 2, $"{growths} growth reservation(s): the step was not refused and tried again");
                Assert.True(wal.CapacityForTest < 2 * capacity,
                            $"grown from {capacity:N0} to {wal.CapacityForTest:N0} bytes: the refused step, not what the append needed");
                StopWatching();
            }
            Assert.Equal(appended, ReadSpans(SpanPath).Count);
        }
        finally { StopWatching(); }
    }

    /// <summary>
    /// R6-H1's class, the metric log: a growth step the volume cannot hold is tried again at what the append needs.
    /// The append that needed the step threw — the points refused with it — though the volume had room for them.
    /// </summary>
    [Fact]
    public void A_metric_log_growth_step_the_volume_cannot_hold_grows_by_what_the_append_needs()
    {
        try
        {
            int steps = 0, fits = 0;
            using (var wal = MetricWriteAheadLog.Open(MetricPath, 64 * 1024))
            {
                // Every step refused — the log pre-grows off the append, and a pre-growth that fails is let go — and
                // what an append needs held: a few KB at a time.
                Watch(refuse: (offset, length) =>
                {
                    if (offset == 0) return false;   // an open's
                    if (length >= 32 * 1024) { Interlocked.Increment(ref steps); return true; }
                    Interlocked.Increment(ref fits);
                    return false;
                });
                for (int i = 0; i < 3_000; i++) wal.Append([Gauge(i)]);   // grows it; the IOException, before
                Assert.True(Volatile.Read(ref steps) > 0 && Volatile.Read(ref fits) > 0,
                            $"{steps} step(s) refused, {fits} growth(s) by what an append needed");
                StopWatching();
            }
            Assert.Equal(3_000, CountPoints(MetricPath));
        }
        finally { StopWatching(); }
    }

    /// <summary>
    /// R6-H1's class, the logs' WAL: a doubling the volume cannot hold is tried again at what the entry needs. The
    /// append that needed the doubling threw, and the write behind it faulted, though the volume had room for the entry.
    /// </summary>
    [Fact]
    public void A_log_growth_the_volume_cannot_double_grows_by_what_the_entry_needs()
    {
        try
        {
            int growths = 0, appended = 0;
            using var wal = WriteAheadLog.Open(LogPath, new NodeId(3), new SegmentId(42UL), initialCapacity: 64 * 1024);
            long sized = new FileInfo(LogPath).Length;
            Watch(refuse: (offset, _) => offset > 0 && Interlocked.Increment(ref growths) == 1);   // the first growth's
            while (new FileInfo(LogPath).Length == sized)
            {
                Assert.True(appended < 1_000, "setup: the log did not grow");
                Assert.True(wal.Append(1_000 + appended, LogLevel.Information, 1, "t", new byte[1_000]));   // threw, before
                appended++;
            }
            long grown = new FileInfo(LogPath).Length;
            Assert.True(Volatile.Read(ref growths) >= 2, $"{growths} growth reservation(s): the doubling was not refused and tried again");
            Assert.True(grown - sized < sized - 4096,
                        $"grown from {sized:N0} to {grown:N0} bytes: the refused doubling, not what the entry needed");
        }
        finally { StopWatching(); }
    }

    /// <summary>An open whose reservation fails throws, and removes the file it created. It opened over the sparse file.</summary>
    [Fact]
    public void A_span_log_open_the_volume_cannot_hold_throws_and_removes_the_file_it_created()
    {
        try
        {
            Watch(refuse: static (_, _) => true);
            SpanWriteAheadLog? opened = null;
            var thrown = Record.Exception(() => opened = SpanWriteAheadLog.Open(SpanPath, initialCapacity: 64 * 1024));
            opened?.Dispose();

            Assert.True(thrown is IOException, thrown is null ? "the open went on past a reservation that failed" : $"the open threw {thrown.GetType().Name}");
            Assert.False(File.Exists(SpanPath), $"the failed open left a {(File.Exists(SpanPath) ? new FileInfo(SpanPath).Length : 0):N0}-byte file behind");
        }
        finally { StopWatching(); }
    }

    /// <summary>
    /// An open of an existing log whose reservation fails leaves the file as it was — its length and every
    /// byte — and the next open replays every span. Asked for more capacity than the file has, the open
    /// extends it first: the extension used to stay, sized and unreserved, for the next start to map.
    /// </summary>
    [Fact]
    public void A_span_log_open_the_volume_cannot_hold_leaves_an_existing_log_as_it_found_it()
    {
        try
        {
            using (var wal = SpanWriteAheadLog.Open(SpanPath, initialCapacity: 8 * 1024))
                for (int i = 0; i < 3; i++) wal.Append(Span(i));
            byte[] found = File.ReadAllBytes(SpanPath);

            Watch(refuse: static (_, _) => true);
            SpanWriteAheadLog? opened = null;
            var thrown = Record.Exception(() => opened = SpanWriteAheadLog.Open(SpanPath, initialCapacity: 256 * 1024));
            opened?.Dispose();
            StopWatching();

            Assert.True(thrown is IOException, thrown is null ? "the open went on past a reservation that failed" : $"the open threw {thrown.GetType().Name}");
            long left = new FileInfo(SpanPath).Length;
            Assert.True(left == found.Length, $"the failed open left the log at {left:N0} bytes, where it found {found.Length:N0}");
            Assert.Equal(found, File.ReadAllBytes(SpanPath));
            Assert.Equal(3, ReadSpans(SpanPath).Count);
        }
        finally { StopWatching(); }
    }

    // ── The metric WAL (R5-H2) ────────────────────────────────────────────────

    /// <summary>
    /// A reopened metric log reserves its whole file, not only what the open added — at its full size it
    /// added nothing, and was mapped over whatever holes it had. So does a reopen after a growth.
    /// </summary>
    [Fact]
    public void Every_open_of_the_metric_log_reserves_its_whole_file()
    {
        try
        {
            var calls = Watch();
            using (var wal = MetricWriteAheadLog.Open(MetricPath, 64 * 1024))
            {
                AssertWholeFileReserved(calls, MetricPath, "the metric log's first open");
                wal.Append([Gauge(1)]);
            }

            calls.Clear();
            using (var wal = MetricWriteAheadLog.Open(MetricPath, 64 * 1024))
            {
                AssertWholeFileReserved(calls, MetricPath, "the metric log's reopen");
                for (int i = 0; i < 3_000; i++) wal.Append([Gauge(i)]);   // grows it: the reopen below finds it grown
            }
            Assert.True(new FileInfo(MetricPath).Length > 64 * 1024 + 64, "setup: the metric log did not grow");

            calls.Clear();
            using (MetricWriteAheadLog.Open(MetricPath, 64 * 1024))
                AssertWholeFileReserved(calls, MetricPath, "the metric log's reopen after a growth");
        }
        finally { StopWatching(); }
    }

    /// <summary>
    /// An open whose reservation fails throws and removes the files it created. It only disposed: the
    /// sized, unreserved file stayed, and the next start mapped it — Ingest then died with AccessViolation
    /// once the volume was full.
    /// </summary>
    [Fact]
    public void A_metric_log_open_the_volume_cannot_hold_throws_and_removes_the_files_it_created()
    {
        try
        {
            Watch(refuse: static (_, _) => true);
            MetricWriteAheadLog? opened = null;
            var thrown = Record.Exception(() => opened = MetricWriteAheadLog.Open(MetricPath, 64 * 1024));
            opened?.Dispose();

            Assert.True(thrown is IOException, thrown is null ? "the open went on past a reservation that failed" : $"the open threw {thrown.GetType().Name}");
            Assert.False(File.Exists(MetricPath), $"the failed open left a {(File.Exists(MetricPath) ? new FileInfo(MetricPath).Length : 0):N0}-byte metrics.wal behind");
            Assert.False(File.Exists(MetricPath + ".pool"), "the failed open left a pool file behind");
        }
        finally { StopWatching(); }
    }

    /// <summary>
    /// An open of an existing metric log whose reservation fails leaves it as it found it — its length and
    /// every byte — and the next open replays every point. The extension the open made used to stay.
    /// </summary>
    [Fact]
    public void A_metric_log_open_the_volume_cannot_hold_leaves_an_existing_log_as_it_found_it()
    {
        try
        {
            using (var wal = MetricWriteAheadLog.Open(MetricPath, 64 * 1024))
                for (int i = 0; i < 3; i++) wal.Append([Gauge(i)]);
            byte[] found = File.ReadAllBytes(MetricPath);

            Watch(refuse: static (_, _) => true);
            MetricWriteAheadLog? opened = null;
            var thrown = Record.Exception(() => opened = MetricWriteAheadLog.Open(MetricPath, 1024 * 1024));
            opened?.Dispose();
            StopWatching();

            Assert.True(thrown is IOException, thrown is null ? "the open went on past a reservation that failed" : $"the open threw {thrown.GetType().Name}");
            long left = new FileInfo(MetricPath).Length;
            Assert.True(left == found.Length, $"the failed open left the metric log at {left:N0} bytes, where it found {found.Length:N0}");
            Assert.Equal(found, File.ReadAllBytes(MetricPath));
            Assert.Equal(3, CountPoints(MetricPath));
        }
        finally { StopWatching(); }
    }

    // ── The logs' WAL (R5-H2's class) ─────────────────────────────────────────

    /// <summary>A reopened log reserves its whole file too. It reserved only what an open added: nothing, at its size.</summary>
    [Fact]
    public void Every_open_of_the_log_reserves_its_whole_file()
    {
        try
        {
            var calls = Watch();
            using (var wal = WriteAheadLog.Open(LogPath, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024))
            {
                AssertWholeFileReserved(calls, LogPath, "the log's first open");
                Assert.True(wal.Append(1_000, LogLevel.Information, 1, "t", new byte[100]));
            }

            calls.Clear();
            using (WriteAheadLog.Open(LogPath, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024))
                AssertWholeFileReserved(calls, LogPath, "the log's reopen");
        }
        finally { StopWatching(); }
    }

    /// <summary>
    /// An open of an existing log whose reservation fails leaves it as it found it, its entries readable.
    /// It kept an existing file, at the size the open had extended it to.
    /// </summary>
    [Fact]
    public void A_log_open_the_volume_cannot_hold_leaves_an_existing_log_as_it_found_it()
    {
        try
        {
            using (var wal = WriteAheadLog.Open(LogPath, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024))
            {
                Assert.True(wal.Append(1_000, LogLevel.Information, 1, "t", new byte[100]));
                Assert.True(wal.Append(2_000, LogLevel.Information, 1, "t", new byte[100]));
            }
            byte[] found = File.ReadAllBytes(LogPath);

            Watch(refuse: static (_, _) => true);
            WriteAheadLog? opened = null;
            var thrown = Record.Exception(() => opened = WriteAheadLog.Open(LogPath, new NodeId(3), new SegmentId(42UL), initialCapacity: 4 * 1024 * 1024));
            opened?.Dispose();
            StopWatching();

            Assert.True(thrown is IOException, thrown is null ? "the open went on past a reservation that failed" : $"the open threw {thrown.GetType().Name}");
            long left = new FileInfo(LogPath).Length;
            Assert.True(left == found.Length, $"the failed open left the log at {left:N0} bytes, where it found {found.Length:N0}");
            Assert.Equal(found, File.ReadAllBytes(LogPath));
            Assert.Equal(2, WriteAheadLog.ReadForRecovery(LogPath).Entries.Count);
        }
        finally { StopWatching(); }
    }

    /// <summary>
    /// The twin that was already right, pinned: a new log whose reservation fails is removed with its pool.
    /// What changed is how the open knows it created them — off its own handle, not File.Exists before it.
    /// </summary>
    [Fact]
    public void A_log_open_the_volume_cannot_hold_throws_and_removes_the_files_it_created()
    {
        try
        {
            Watch(refuse: static (_, _) => true);
            WriteAheadLog? opened = null;
            var thrown = Record.Exception(() => opened = WriteAheadLog.Open(LogPath, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024));
            opened?.Dispose();

            Assert.True(thrown is IOException, thrown is null ? "the open went on past a reservation that failed" : $"the open threw {thrown.GetType().Name}");
            Assert.False(File.Exists(LogPath), "the failed open left the log file behind");
            Assert.False(File.Exists(LogPath + ".pool"), "the failed open left a pool file behind");
        }
        finally { StopWatching(); }
    }

    // ── Linux: the blocks on disk ─────────────────────────────────────────────

    /// <summary>
    /// The span log has its blocks from its open and from each growth (the log WAL's and the metric WAL's
    /// facts are in their own classes). It was sparse from its open on.
    /// </summary>
    [Fact]
    public void On_linux_the_span_log_has_its_blocks_from_its_open_and_from_each_growth()
    {
        if (!OperatingSystem.IsLinux()) return;   // NTFS allocates on SetLength; this is Linux's road
        using var wal = SpanWriteAheadLog.Open(SpanPath, initialCapacity: 64 * 1024);
        long sized = new FileInfo(SpanPath).Length;
        Assert.True(FileAllocation.AllocatedBytes(SpanPath) >= sized,
            $"the span log is sparse from its open: {FileAllocation.AllocatedBytes(SpanPath):N0} of {sized:N0} bytes on disk");

        long capacity = wal.CapacityForTest;
        for (int i = 0; wal.CapacityForTest == capacity; i++)
        {
            Assert.True(i < 1_000, "setup: the span log did not grow");
            wal.Append(Span(i, attrBytes: 900));
        }
        long grown = new FileInfo(SpanPath).Length;
        Assert.True(FileAllocation.AllocatedBytes(SpanPath) >= grown,
            $"the span log is sparse past its growth: {FileAllocation.AllocatedBytes(SpanPath):N0} of {grown:N0} bytes on disk");
    }

    /// <summary>
    /// Writes <paramref name="path"/> again as an earlier build left it: the same length and the same first
    /// page — the header and the few entries the test wrote — with everything after it a hole.
    /// </summary>
    private static void MakeSparse(string path)
    {
        byte[] bytes = File.ReadAllBytes(path);
        File.Delete(path);
        using var fs = new FileStream(path, FileMode.CreateNew, FileAccess.Write);
        fs.Write(bytes, 0, 4096);
        fs.SetLength(bytes.Length);
        fs.Flush(flushToDisk: true);
    }

    /// <summary>
    /// The span log reopened over a file an earlier build left sparse — its length all there, its blocks
    /// not — has every block once it is open, and replays what it held. The span log never reserved.
    /// </summary>
    [Fact]
    public void On_linux_the_span_log_reopened_over_a_sparse_file_has_its_blocks()
    {
        if (!OperatingSystem.IsLinux()) return;
        using (var wal = SpanWriteAheadLog.Open(SpanPath, initialCapacity: 64 * 1024))
            for (int i = 0; i < 3; i++) wal.Append(Span(i));
        MakeSparse(SpanPath);
        Assert.True(FileAllocation.AllocatedBytes(SpanPath) < new FileInfo(SpanPath).Length, "setup: the span log is not sparse");

        using var reopened = SpanWriteAheadLog.Open(SpanPath, initialCapacity: 64 * 1024);
        Assert.True(FileAllocation.AllocatedBytes(SpanPath) >= new FileInfo(SpanPath).Length,
            $"the reopened span log is sparse: {FileAllocation.AllocatedBytes(SpanPath):N0} of {new FileInfo(SpanPath).Length:N0} bytes on disk");
        Assert.Equal(3, reopened.ReadAll().Count);
    }

    /// <summary>
    /// The metric log reopened over a file an earlier build left sparse has every block once it is open,
    /// and replays what it held. The reopen reserved nothing — the open added nothing — and the metric
    /// engine's Ingest then wrote into the holes of a full volume and died with AccessViolation.
    /// </summary>
    [Fact]
    public void On_linux_the_metric_log_reopened_over_a_sparse_file_has_its_blocks()
    {
        if (!OperatingSystem.IsLinux()) return;
        using (var wal = MetricWriteAheadLog.Open(MetricPath, 64 * 1024))
            for (int i = 0; i < 3; i++) wal.Append([Gauge(i)]);
        MakeSparse(MetricPath);
        Assert.True(FileAllocation.AllocatedBytes(MetricPath) < new FileInfo(MetricPath).Length, "setup: the metric log is not sparse");

        using var reopened = MetricWriteAheadLog.Open(MetricPath, 64 * 1024);
        Assert.True(FileAllocation.AllocatedBytes(MetricPath) >= new FileInfo(MetricPath).Length,
            $"the reopened metric log is sparse: {FileAllocation.AllocatedBytes(MetricPath):N0} of {new FileInfo(MetricPath).Length:N0} bytes on disk");
        Assert.Equal(3, reopened.ReadAll(out _).Count);
    }

    /// <summary>The log reopened over a file an earlier build left sparse has every block once it is open.</summary>
    [Fact]
    public void On_linux_the_log_reopened_over_a_sparse_file_has_its_blocks()
    {
        if (!OperatingSystem.IsLinux()) return;
        using (var wal = WriteAheadLog.Open(LogPath, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024))
            for (int i = 0; i < 3; i++) Assert.True(wal.Append(1_000 + i, LogLevel.Information, 1, "t", new byte[100]));
        MakeSparse(LogPath);
        Assert.True(FileAllocation.AllocatedBytes(LogPath) < new FileInfo(LogPath).Length, "setup: the log is not sparse");

        using (WriteAheadLog.Open(LogPath, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024))
            Assert.True(FileAllocation.AllocatedBytes(LogPath) >= new FileInfo(LogPath).Length,
                $"the reopened log is sparse: {FileAllocation.AllocatedBytes(LogPath):N0} of {new FileInfo(LogPath).Length:N0} bytes on disk");
        Assert.Equal(3, WriteAheadLog.ReadForRecovery(LogPath).Entries.Count);
    }
}
