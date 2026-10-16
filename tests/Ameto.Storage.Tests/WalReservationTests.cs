using Ameto.Core;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE SPAN WAL HAS ITS WHOLE FILE'S BLOCKS FROM EVERY OPEN AND EVERY GROWTH, AND A FAILED OPEN LEAVES THE
/// FILE AS IT FOUND IT (round-6 review R5-H1).
///
/// <para>The span log is written through a mapping over a file sized with SetLength — on Linux ftruncate,
/// a sparse file — and a write into a page of a full volume is SIGBUS. Round 5 reserved the logs' WAL and
/// the metric WAL; the span WAL reserved nothing, and trace ingest killed the process (exit 135) after
/// ~600 spans once the volume filled.</para>
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

    private string SpanPath => Path.Combine(_dir, "spans.wal");

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

    private static List<SpanIngestItem> ReadSpans(string path)
    {
        using var wal = SpanWriteAheadLog.Open(path, initialCapacity: 8 * 1024);
        return wal.ReadAll();
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
}
