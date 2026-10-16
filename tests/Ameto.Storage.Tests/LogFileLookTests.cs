using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using LogLevel = Ameto.Core.LogLevel;

namespace Ameto.Storage.Tests;

/// <summary>
/// A LOG IS GONE ONLY WHEN THE FILE SYSTEM SAYS SO, AND A READ THAT FAILED IS NOT AN EMPTY LOG (round-6 review
/// R5-M6, round-5 L-18's class).
///
/// <para><c>File.Exists</c> answers false for a file it could not look at — a share that did not answer, an
/// access check — as well as for one that is gone. The recovery reads took that false for "no log" and "no
/// pool", and their callers then deleted a log that read as empty, or replayed its events with no templates
/// and deleted it. The logs' opens took it for "a new file" and wrote a fresh header over an existing log,
/// and a failed open deleted the existing log it believed it had created.</para>
///
/// <para><see cref="FileRefusal"/> answers a look or an open of a path with the exception the file system
/// would throw, on every platform; round 5's seam reached only the look.</para>
/// </summary>
public sealed class LogFileLookTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-filelook-" + Guid.NewGuid().ToString("N"));

    public LogFileLookTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        FileRefusal.ForTest.Value = null;
        try { Directory.Delete(_dir, true); } catch { }
    }

    private string LogPath    => Path.Combine(_dir, "3-42.wal");
    private string SpanPath   => Path.Combine(_dir, "spans.wal");
    private string MetricPath => Path.Combine(_dir, "metrics.wal");

    /// <summary>What a share that does not answer says, to a look or an open.</summary>
    private static IOException NotAnswering(string path) => new($"test: the network path of {path} did not answer");

    /// <summary>Refuses the accesses <paramref name="refused"/> picks, as the file system would.</summary>
    private static void Refuse(Func<string, FileRefusal.Access, bool> refused) =>
        FileRefusal.ForTest.Value = (path, access) => refused(path, access) ? NotAnswering(path) : null;

    private static void Answer() => FileRefusal.ForTest.Value = null;

    private void WriteLog(int events)
    {
        using var wal = WriteAheadLog.Open(LogPath, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024);
        for (int i = 0; i < events; i++)
            Assert.True(wal.Append(1_000 + i, LogLevel.Information, 7, "event {n}", new byte[40]));
    }

    // ── The recovery reads ────────────────────────────────────────────────────

    /// <summary>
    /// A WAL the file system will not open is a read that failed — the replay and the despill keep the log
    /// and read it again — not a log of no entries, which both delete. One that is gone still reads empty.
    /// </summary>
    [Fact]
    public void A_wal_the_file_system_will_not_open_is_a_failed_read_not_an_empty_log()
    {
        WriteLog(3);
        Refuse((p, _) => p == LogPath);
        try
        {
            WriteAheadLog.RecoveryReader? reader = null;
            var thrown = Record.Exception(() => reader = WriteAheadLog.RecoveryReader.Open(LogPath));
            using (reader)
                Assert.True(thrown is IOException,
                    thrown is null
                        ? $"a WAL the file system would not open read as a log of {(reader!.SegmentId == 0 ? "no" : "some")} entries"
                        : $"the open threw {thrown.GetType().Name}");
        }
        finally { Answer(); }

        using (var reader = WriteAheadLog.RecoveryReader.Open(Path.Combine(_dir, "gone.wal")))
            Assert.Equal(0UL, reader.SegmentId);                                  // gone is gone
        Assert.Equal(3, WriteAheadLog.ReadForRecovery(LogPath).Entries.Count);   // and the log is as it was
    }

    /// <summary>
    /// A pool the file system will not open is a read that failed, not a pool of no rows: the replay went
    /// on with none, every event of the block lost its template and its service, and the log was deleted.
    /// </summary>
    [Fact]
    public void A_pool_the_file_system_will_not_open_is_a_failed_read_not_an_empty_pool()
    {
        WriteLog(3);
        string pool = LogPath + ".pool";
        Refuse((p, _) => p == pool);
        try
        {
            Dictionary<ushort, string>? rows = null;
            var thrown = Record.Exception(() => rows = WriteAheadLog.LoadPool(pool));
            Assert.True(thrown is IOException,
                thrown is null ? $"a pool the file system would not open read as {rows!.Count} row(s)" : $"the open threw {thrown.GetType().Name}");
        }
        finally { Answer(); }

        Assert.Empty(WriteAheadLog.LoadPool(Path.Combine(_dir, "gone.wal.pool")));   // gone is gone
        Assert.Equal("event {n}", WriteAheadLog.LoadPool(pool)[7]);
    }

    // ── The logs' opens (fixed in c07dafc and 0abd598) ────────────────────────

    /// <summary>
    /// A log the file system will not LOOK at, and does open, is opened as the log it is. Its open asked
    /// File.Exists whether there was a log, and took the false for a new file: a fresh header over the
    /// existing one, the write offset back at the start, every entry behind it unreachable.
    /// </summary>
    [Fact]
    public void A_log_the_file_system_will_not_look_at_opens_as_the_log_it_is()
    {
        WriteLog(3);
        Refuse((p, a) => a == FileRefusal.Access.Look && p == LogPath);
        try
        {
            using (WriteAheadLog.Open(LogPath, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024)) { }
        }
        finally { Answer(); }
        int left = WriteAheadLog.ReadForRecovery(LogPath).Entries.Count;
        Assert.True(left == 3, $"the open took a log it could not look at for a new one: {left} of 3 entries left");
    }

    /// <summary>
    /// A log the file system will neither look at nor open is not deleted by the open that failed. The open
    /// asked File.Exists before it whether the file existed, and deleted it in its catch when the answer was
    /// no — the existing log, for a look that failed.
    /// </summary>
    [Fact]
    public void A_log_the_file_system_will_not_open_is_not_deleted_by_the_open_that_failed()
    {
        WriteLog(3);
        Refuse((p, _) => p == LogPath);
        Exception? thrown;
        try
        {
            WriteAheadLog? opened = null;
            thrown = Record.Exception(() => opened = WriteAheadLog.Open(LogPath, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024));
            opened?.Dispose();
        }
        finally { Answer(); }
        Assert.True(thrown is IOException, thrown is null ? "the open went on" : $"the open threw {thrown.GetType().Name}");
        Assert.True(File.Exists(LogPath), "the failed open deleted the log it could not open");
        Assert.Equal(3, WriteAheadLog.ReadForRecovery(LogPath).Entries.Count);
    }

    /// <summary>
    /// A span log the file system will not look at, and does open, is opened as the log it is. Read as a
    /// new file, it was re-initialised in place: every span it held, gone.
    /// </summary>
    [Fact]
    public void A_span_log_the_file_system_will_not_look_at_opens_as_the_log_it_is()
    {
        using (var wal = SpanWriteAheadLog.Open(SpanPath, initialCapacity: 64 * 1024))
            for (int i = 0; i < 3; i++)
                wal.Append(new SpanIngestItem
                {
                    TraceId = new TraceId(1, (ulong)i + 1), SpanId = new SpanId((ulong)i + 1), StartTimeUnixNano = 1_785_000_000_000_000_000L + i,
                    DurationNanos = 1_000, Name = "GET /x", ServiceName = "svc", Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
                    AttributesBytes = [],
                });

        Refuse((p, a) => a == FileRefusal.Access.Look && p == SpanPath);
        int replayed;
        try
        {
            using var wal = SpanWriteAheadLog.Open(SpanPath, initialCapacity: 64 * 1024);
            replayed = wal.ReadAll().Count;
        }
        finally { Answer(); }
        Assert.True(replayed == 3, $"the open took a span log it could not look at for a new one: {replayed} of 3 spans replayed");
    }

    /// <summary>
    /// A metric log the file system will not look at, and does open, is opened as the log it is. Read as a
    /// new file, it was re-initialised in place: every point it held, gone.
    /// </summary>
    [Fact]
    public void A_metric_log_the_file_system_will_not_look_at_opens_as_the_log_it_is()
    {
        using (var wal = MetricWriteAheadLog.Open(MetricPath, 64 * 1024))
            for (int i = 0; i < 3; i++)
                wal.Append([new MetricIngestItem
                {
                    Name = "cpu", Kind = MetricKind.Gauge, Unit = "ms", Labels = new LabelSet([new("service.name", "svc")]),
                    TimestampUnixNano = 1_785_300_000_000_000_000L + i, ScalarValue = i,
                }]);

        Refuse((p, a) => a == FileRefusal.Access.Look && p == MetricPath);
        int replayed;
        try
        {
            using var wal = MetricWriteAheadLog.Open(MetricPath, 64 * 1024);
            replayed = wal.ReadAll(out _).Count;
        }
        finally { Answer(); }
        Assert.True(replayed == 3, $"the open took a metric log it could not look at for a new one: {replayed} of 3 points replayed");
    }

    // ── The metric WAL's pool ─────────────────────────────────────────────────

    /// <summary>
    /// A metric log whose pool the file system will not read fails its open, and the pool is not cut. The
    /// walk asked File.Exists first and swallowed every failure; both ended it at offset 0, and the open
    /// then cut the pool there as a torn tail: every series the log's points name, gone with the file, and
    /// the points replayed as unresolved.
    /// </summary>
    [Fact]
    public void A_metric_pool_the_open_cannot_read_fails_the_open_and_is_not_cut()
    {
        string pool = MetricPath + ".pool";
        using (var wal = MetricWriteAheadLog.Open(MetricPath, 64 * 1024))
            for (int i = 0; i < 3; i++)
                wal.Append([new MetricIngestItem
                {
                    Name = "cpu." + i, Kind = MetricKind.Gauge, Unit = "ms", Labels = new LabelSet([new("service.name", "svc")]),
                    TimestampUnixNano = 1_785_300_000_000_000_000L + i, ScalarValue = i,
                }]);
        long poolLength = new FileInfo(pool).Length;
        Assert.True(poolLength > 0, "setup: the pool is empty");

        // The open opens the pool twice: its own handle, then the walk's read — the one refused.
        int opens = 0;
        FileRefusal.ForTest.Value = (p, a) => a == FileRefusal.Access.Open && p == pool && ++opens == 2 ? NotAnswering(p) : null;
        Exception? thrown;
        try
        {
            MetricWriteAheadLog? opened = null;
            thrown = Record.Exception(() => opened = MetricWriteAheadLog.Open(MetricPath, 64 * 1024));
            opened?.Dispose();
        }
        finally { Answer(); }

        long left = new FileInfo(pool).Length;
        Assert.True(thrown is IOException,
            thrown is null ? $"the open went on past a pool it could not read, which it cut from {poolLength:N0} to {left:N0} bytes"
                           : $"the open threw {thrown.GetType().Name}");
        Assert.Equal(poolLength, left);
        using var reopened = MetricWriteAheadLog.Open(MetricPath, 64 * 1024);
        var points = reopened.ReadAll(out int unresolved);
        Assert.Equal(0, unresolved);
        Assert.Equal(3, points.Count);
    }
}
