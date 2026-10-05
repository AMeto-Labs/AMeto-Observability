using System.Buffers.Binary;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MsLogLevel = Microsoft.Extensions.Logging.LogLevel;
using QueryAvailability = Ameto.Core.QueryAvailability;

namespace Ameto.Storage.Tests;

/// <summary>
/// A METRIC SEGMENT THE STARTUP SCAN CANNOT READ IS DELETED ONLY WHEN ITS BYTES ARE WRONG (#108).
///
/// <para>The cold scan deleted every <c>.mts</c> whose header read threw — the v1 migration path —
/// whatever the throw said. A segment an antivirus held open for a moment, one read through a share
/// that blinked, one the process ran out of handles for: each was a segment gone for good, at a start.
/// Now the failure is classified: a data error (<see cref="InvalidDataException"/>,
/// <see cref="EndOfStreamException"/>) still deletes; a file gone between the listing and the open is
/// skipped; anything else is retried a few times and then KEPT, out of this run's cold tier, with one
/// Error naming it and the store <see cref="QueryAvailability.Degraded"/> until the next start reads it.</para>
///
/// <para>The faults come through <see cref="MetricStorageEngine.ColdScanIo"/> — one file's read fails
/// as a busy disk fails it, and the pauses between attempts are recorded instead of slept — except in
/// the fact that holds a real file open, which proves the same of a real sharing violation.</para>
/// </summary>
public sealed class MetricColdScanFaultTests : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    /// <summary>The scan's pauses between attempts at an unreachable file — <c>ColdReadRetryDelays</c>.</summary>
    private static readonly TimeSpan[] Delays =
    [
        TimeSpan.FromMilliseconds(25), TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(100),
        TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(400),
    ];

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mtsfault-" + Guid.NewGuid().ToString("N"));

    public MetricColdScanFaultTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    // ── Not about the bytes: kept ─────────────────────────────────────────────────────────────

    /// <summary>
    /// THE ISSUE'S TEST. An <see cref="IOException"/> on one file, on every attempt: after the start
    /// the file is on disk, untouched; the cold tier is without it — its metric answers nothing — and
    /// with the file next to it, which the scan went on to load; one Error names it; the store is
    /// Degraded; and the pauses were the schedule's, every one of them. The next start, with nothing
    /// in the way, loads the file and is Available. Before, the first start deleted it.
    /// </summary>
    [Fact]
    public async Task An_io_error_on_one_file_keeps_it_out_of_the_catalog_and_the_next_start_loads_it()
    {
        var files  = await WriteSegmentsAsync("busy.metric", "other.metric");
        string busy = files["busy.metric"];
        long   size = new FileInfo(busy).Length;
        var    sharingViolation = new IOException("The process cannot access the file because it is being used by another process.");

        var log   = new Entries();
        var waits = new List<TimeSpan>();
        var io = new MetricStorageEngine.ColdScanIo
        {
            ReadSegmentInfo = path => path == busy ? throw sharingViolation : MetricReader.ReadSegmentInfo(path),
            Wait            = waits.Add,
        };
        await using (var engine = await StartAsync(log, io))
        {
            Assert.True(File.Exists(busy), "a file the scan could not reach was deleted");
            Assert.Equal(size, new FileInfo(busy).Length);
            Assert.Equal(0, await PointsOfAsync(engine, "busy.metric"));
            Assert.Equal(3, await PointsOfAsync(engine, "other.metric"));
            Assert.Equal(QueryAvailability.Degraded, engine.Availability);

            var error = Assert.Single(log.Snapshot(), e => e.Level >= MsLogLevel.Error);
            Assert.Same(sharingViolation, error.Error);
            Assert.Contains(busy, error.Message);
            Assert.Contains("after 6 attempt(s)", error.Message);
            Assert.Contains("reports itself Degraded", error.Message);
            Assert.Equal(Delays, waits);
        }

        var next = new Entries();
        await using (var engine = await StartAsync(next, io: null))
        {
            Assert.Equal(3, await PointsOfAsync(engine, "busy.metric"));
            Assert.Equal(3, await PointsOfAsync(engine, "other.metric"));
            Assert.Equal(QueryAvailability.Available, engine.Availability);
            Assert.DoesNotContain(next.Snapshot(), e => e.Level >= MsLogLevel.Error);
        }
    }

    /// <summary>
    /// The rest of what says nothing about the bytes: access denied (a remounting volume, an ACL), the
    /// data directory gone from under the scan (a volume, not a delete — the engine never removes its
    /// directory), a process out of file handles. Each keeps the file and leaves the store Degraded.
    /// </summary>
    [Theory]
    [InlineData("access denied")]
    [InlineData("directory gone")]
    [InlineData("too many open files")]
    public async Task Every_failure_that_is_not_about_the_bytes_keeps_the_file(string failure)
    {
        var files  = await WriteSegmentsAsync("busy.metric");
        string busy = files["busy.metric"];
        Exception fault = failure switch
        {
            "access denied"  => new UnauthorizedAccessException($"Access to the path '{busy}' is denied."),
            "directory gone" => new DirectoryNotFoundException($"Could not find a part of the path '{busy}'."),
            _                => new IOException("Too many open files"),
        };

        var log = new Entries();
        var io = new MetricStorageEngine.ColdScanIo
        {
            ReadSegmentInfo = path => throw fault,
            Wait            = static _ => { },
        };
        await using var engine = await StartAsync(log, io);

        Assert.True(File.Exists(busy), $"{failure}: the file was deleted");
        Assert.Equal(QueryAvailability.Degraded, engine.Availability);
        Assert.Same(fault, Assert.Single(log.Snapshot(), e => e.Level >= MsLogLevel.Error).Error);
    }

    /// <summary>
    /// What the retries are for: a file busy on its first two attempts and readable on the third is
    /// loaded — after the schedule's first two pauses — and costs a Warning, not a Degraded store.
    /// </summary>
    [Fact]
    public async Task A_file_busy_for_its_first_attempts_is_loaded_and_the_store_is_available()
    {
        var files  = await WriteSegmentsAsync("busy.metric");
        string busy = files["busy.metric"];

        int attempts = 0;
        var log   = new Entries();
        var waits = new List<TimeSpan>();
        var io = new MetricStorageEngine.ColdScanIo
        {
            ReadSegmentInfo = path => ++attempts <= 2 ? throw new IOException("busy") : MetricReader.ReadSegmentInfo(path),
            Wait            = waits.Add,
        };
        await using var engine = await StartAsync(log, io);

        Assert.Equal(3, await PointsOfAsync(engine, "busy.metric"));
        Assert.Equal(QueryAvailability.Available, engine.Availability);
        Assert.Equal(Delays[..2], waits);
        Assert.DoesNotContain(log.Snapshot(), e => e.Level >= MsLogLevel.Error);
        Assert.Single(log.Snapshot(), e => e.Level == MsLogLevel.Warning && e.Message.Contains("read on attempt 3"));
    }

    /// <summary>
    /// THE RETRIES ARE BOUNDED FOR THE WHOLE SCAN, not per file: four files a backup agent holds would
    /// cost 3.1 s of pauses at the schedule's 0.775 s each — at every start, with every metric alert
    /// held in Loading — and the budget stops them at 2 s. The fourth file gets its one attempt, and
    /// all four are kept.
    /// </summary>
    [Fact]
    public async Task The_pauses_of_one_scan_stop_at_its_budget()
    {
        var files = await WriteSegmentsAsync("a.metric", "b.metric", "c.metric", "d.metric");

        var log   = new Entries();
        var waits = new List<TimeSpan>();
        var io = new MetricStorageEngine.ColdScanIo
        {
            ReadSegmentInfo = static _ => throw new IOException("held by a backup agent"),
            Wait            = waits.Add,
        };
        await using var engine = await StartAsync(log, io);

        Assert.Equal(TimeSpan.FromSeconds(2), waits.Aggregate(TimeSpan.Zero, static (a, b) => a + b));
        Assert.All(files.Values, f => Assert.True(File.Exists(f)));
        var errors = log.Snapshot().Where(e => e.Level >= MsLogLevel.Error).ToList();
        Assert.Equal(4, errors.Count);
        Assert.Contains("after 1 attempt(s)", errors[^1].Message);   // the last file, after the budget ran out
        Assert.Equal(QueryAvailability.Degraded, engine.Availability);
    }

    /// <summary>
    /// A REAL sharing violation, not a seam's: the file is held open without sharing for the whole
    /// scan — a sharing violation on Windows, .NET's advisory lock on Linux — and the scan keeps it.
    /// Let go, the next start loads it.
    /// </summary>
    [Fact]
    public async Task A_file_held_open_by_another_handle_through_the_retries_is_kept()
    {
        var files  = await WriteSegmentsAsync("held.metric");
        string held = files["held.metric"];
        var io = new MetricStorageEngine.ColdScanIo { Wait = static _ => { } };   // the real read, no sleeping

        var log = new Entries();
        using (new FileStream(held, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        await using (var engine = await StartAsync(log, io))
        {
            Assert.Equal(0, await PointsOfAsync(engine, "held.metric"));
            Assert.Equal(QueryAvailability.Degraded, engine.Availability);
            Assert.IsAssignableFrom<IOException>(Assert.Single(log.Snapshot(), e => e.Level >= MsLogLevel.Error).Error);
        }
        Assert.True(File.Exists(held));

        await using var restarted = await StartAsync(new Entries(), io: null);
        Assert.Equal(3, await PointsOfAsync(restarted, "held.metric"));
        Assert.Equal(QueryAvailability.Available, restarted.Availability);
    }

    // ── Gone: skipped ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A file that was listed and is gone when opened has nothing to load and nothing to delete: it is
    /// skipped with a Warning, and the store is not Degraded over it. (The seam says "gone" while the
    /// file is still there — which is how this proves nothing deletes it.)
    /// </summary>
    [Fact]
    public async Task A_file_gone_by_the_time_it_is_opened_is_skipped_not_deleted_and_degrades_nothing()
    {
        var files  = await WriteSegmentsAsync("gone.metric");
        string gone = files["gone.metric"];

        var log = new Entries();
        var io = new MetricStorageEngine.ColdScanIo
        {
            ReadSegmentInfo = path => throw new FileNotFoundException("gone", path),
            Wait            = static _ => throw new InvalidOperationException("a vanished file is not retried"),
        };
        await using var engine = await StartAsync(log, io);

        Assert.True(File.Exists(gone));
        Assert.Equal(QueryAvailability.Available, engine.Availability);
        Assert.DoesNotContain(log.Snapshot(), e => e.Level >= MsLogLevel.Error);
        Assert.Single(log.Snapshot(), e => e.Level == MsLogLevel.Warning && e.Message.Contains("was gone"));
    }

    // ── About the bytes: deleted, as before ───────────────────────────────────────────────────

    /// <summary>
    /// Today's handling, kept: a file whose bytes the reader refuses — a foreign magic, a file too
    /// short to hold a header — is deleted at once, without a retry, and does not make the store
    /// Degraded: it is the store's data from then on, not a load left unfinished.
    /// </summary>
    [Theory]
    [InlineData("foreign")]
    [InlineData("short")]
    public async Task A_file_whose_bytes_are_refused_is_deleted_and_degrades_nothing(string shape)
    {
        string bad = Path.Combine(_dir, $"metrics-{shape}.mts");
        File.WriteAllBytes(bad, shape == "short" ? [0x54, 0x4D, 0x44] : [.. Enumerable.Repeat((byte)0xEE, 64)]);

        var log = new Entries();
        var io = new MetricStorageEngine.ColdScanIo
        {
            Wait = static _ => throw new InvalidOperationException("damage is not retried"),
        };
        await using var engine = await StartAsync(log, io);

        Assert.False(File.Exists(bad), "a file the reader refuses was kept");
        Assert.Equal(QueryAvailability.Available, engine.Availability);
        Assert.DoesNotContain(log.Snapshot(), e => e.Level >= MsLogLevel.Error);
        Assert.Single(log.Snapshot(), e => e.Level == MsLogLevel.Warning && e.Message.Contains("deleting"));
    }

    /// <summary>
    /// The one data error the reader used to throw as a plain <see cref="IOException"/>: a footer
    /// whose name-index offset points before the start of the file. Classified by its type, that
    /// would have been "unreachable" — the torn file kept, and the store Degraded at every start, for
    /// ever. It is an <see cref="InvalidDataException"/> now, and the file is deleted like any other
    /// the reader refuses.
    /// </summary>
    [Fact]
    public async Task A_name_index_offset_before_the_file_is_refused_as_data_not_as_a_busy_file()
    {
        string torn = Path.Combine(_dir, "metrics-torn.mts");
        var bytes = new byte[27 + 12];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, 0x52_44_4D_54);                // "RDMT"
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(4), 3);                  // a version this build reads
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(27), 0x8000_0000_0000_0000UL);   // negative as a long
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(35), 0x52_44_4D_46);     // "RDMF"
        File.WriteAllBytes(torn, bytes);

        Assert.Throws<InvalidDataException>(() => MetricReader.ReadSegmentInfo(torn));

        var log = new Entries();
        await using var engine = await StartAsync(log, new MetricStorageEngine.ColdScanIo
        {
            Wait = static _ => throw new InvalidOperationException("damage is not retried"),
        });

        Assert.False(File.Exists(torn));
        Assert.Equal(QueryAvailability.Available, engine.Availability);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────────────────────

    /// <summary>One .mts per metric, three gauge points each in the last minute — what a clean stop's final flush leaves.</summary>
    private async Task<Dictionary<string, string>> WriteSegmentsAsync(params string[] metrics)
    {
        var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);
        await engine.ColdLoadCompleted.WaitAsync(Timeout);
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        foreach (var metric in metrics)
        {
            MetricIngestItem[] points =
            [
                new() { Name = metric, Kind = MetricKind.Gauge, TimestampUnixNano = now - 30_000_000_000L, ScalarValue = 1 },
                new() { Name = metric, Kind = MetricKind.Gauge, TimestampUnixNano = now - 20_000_000_000L, ScalarValue = 2 },
                new() { Name = metric, Kind = MetricKind.Gauge, TimestampUnixNano = now - 10_000_000_000L, ScalarValue = 3 },
            ];
            Assert.Equal(0, engine.Ingest(points));
        }
        await engine.DisposeAsync();

        var files = Directory.GetFiles(_dir, "*.mts").ToDictionary(static f => MetricReader.ReadSegmentInfo(f).MetricName);
        Assert.Equal(metrics.Order(), files.Keys.Order());
        return files;
    }

    /// <summary>An engine over the directory whose cold scan reads through <paramref name="io"/>, once that scan has ended.</summary>
    private async Task<MetricStorageEngine> StartAsync(Entries log, MetricStorageEngine.ColdScanIo? io)
    {
        MetricStorageEngine engine;
        MetricStorageEngine.ColdScanIoForTest.Value = io;
        try { engine = new MetricStorageEngine(_dir, log); }
        finally { MetricStorageEngine.ColdScanIoForTest.Value = null; }
        await engine.ColdLoadCompleted.WaitAsync(Timeout);
        return engine;
    }

    private static async Task<int> PointsOfAsync(MetricStorageEngine engine, string metric)
    {
        int points = 0;
        await foreach (var series in engine.QueryAsync(metric, DateTimeOffset.UtcNow.AddHours(-1), DateTimeOffset.UtcNow.AddMinutes(1)))
            points += series.Points.Count;
        return points;
    }

    /// <summary>Every entry at every level, formatted.</summary>
    private sealed class Entries : ILogger<MetricStorageEngine>
    {
        private readonly List<(MsLogLevel Level, string Message, Exception? Error)> _entries = [];

        public List<(MsLogLevel Level, string Message, Exception? Error)> Snapshot() { lock (_entries) return [.. _entries]; }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(MsLogLevel level) => true;

        public void Log<TState>(MsLogLevel level, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? error,
                                Func<TState, Exception?, string> formatter)
        {
            lock (_entries) _entries.Add((level, formatter(state, error), error));
        }
    }
}
