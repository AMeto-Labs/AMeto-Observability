using System.Buffers.Binary;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using QueryAvailability = Ameto.Core.QueryAvailability;

namespace Ameto.Storage.Tests;

/// <summary>
/// A TRACE COLD SCAN THAT FAILS AS A WHOLE SAYS, ONCE, WHAT IT LEFT BEHIND (#94) — the trace side of
/// what the metric and log engines log for theirs. The scan used to throw out to the compaction
/// worker, which logged "cold-segment load failed": nothing about the segments staying unserved until
/// a restart, and nothing about the alert evaluator reading the missing window as a quiet one. The
/// store now also says it is <see cref="QueryAvailability.Degraded"/>, which the evaluator skips.
///
/// <para>At the seam: the scan throws before the directory is listed. Reverted (no catch in the
/// engine): <c>LoadColdSegments</c> throws, and no Error of this shape is logged.</para>
/// </summary>
public sealed class TraceColdScanFailureLogTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-coldscan-" + Guid.NewGuid().ToString("N"));

    public TraceColdScanFailureLogTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    [Fact]
    public void A_failed_cold_scan_logs_one_error_naming_the_alert_rules_and_leaves_the_store_degraded()
    {
        var logger = new CapturingLogger();
        using var e = new TraceStorageEngine(_dir, logger);
        var fault = new IOException("injected: the data directory would not list");
        e._failColdScanForTest = fault;

        e.LoadColdSegments();                                            // does not throw

        var errors = logger.Entries.Where(static x => x.Level == LogLevel.Error).ToList();
        var line   = Assert.Single(errors);
        Assert.Same(fault, line.Error);
        Assert.Contains(_dir, line.Message, StringComparison.Ordinal);
        Assert.Contains("trace store reports itself Degraded", line.Message, StringComparison.Ordinal);
        Assert.Contains("trace ALERT RULES are not evaluated on that partial data", line.Message, StringComparison.Ordinal);
        Assert.True(e.ColdTierIncompleteForTest);                        // reads now report the window short
        Assert.Equal(QueryAvailability.Degraded, e.Availability);        // and the evaluator skips the store
    }

    [Fact]
    public void A_scan_that_succeeds_logs_no_error()
    {
        var logger = new CapturingLogger();
        using var e = new TraceStorageEngine(_dir, logger);
        e.LoadColdSegments();
        Assert.DoesNotContain(logger.Entries, static x => x.Level == LogLevel.Error);
        Assert.False(e.ColdTierIncompleteForTest);
        Assert.Equal(QueryAvailability.Available, e.Availability);
    }

    /// <summary>
    /// A SEGMENT STILL HELD BY ANOTHER PROCESS AFTER THE SCAN'S RETRIES IS A LOAD LEFT UNFINISHED
    /// (#94): kept on disk, missing from this run's cold tier, one Error naming it — and the store is
    /// Degraded until the restart that reads it. Held here by a handle opened without sharing, which
    /// fails the scan's open on Windows (a sharing violation) and on Linux (.NET's advisory lock)
    /// alike, for as long as the test holds it; the scan's retries (~0.6 s) run out against it. The
    /// next start, with the handle gone, loads the segment and is Available.
    /// </summary>
    [Fact]
    public void A_segment_held_open_through_the_retries_is_kept_and_leaves_the_store_degraded_until_a_restart()
    {
        string trc = WriteOneSegment();

        var logger = new CapturingLogger();
        using (new FileStream(trc, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        using (var held = new TraceStorageEngine(_dir, logger))
        {
            held.LoadColdSegments();

            Assert.True(File.Exists(trc), "a segment the scan could not open was deleted");
            Assert.Equal(0, held.ColdSegmentCountForTest);
            var error = Assert.Single(logger.Entries, static x => x.Level == LogLevel.Error);
            Assert.Contains(trc, error.Message, StringComparison.Ordinal);
            Assert.Contains("reports itself Degraded", error.Message, StringComparison.Ordinal);
            Assert.Equal(QueryAvailability.Degraded, held.Availability);
        }

        using var restarted = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        restarted.LoadColdSegments();
        Assert.Equal(1, restarted.ColdSegmentCountForTest);
        Assert.Equal(QueryAvailability.Available, restarted.Availability);
    }

    /// <summary>
    /// BUSY, THEN GONE, IS A HANDOVER (#119 review). On Windows the compactor's delete-pending source
    /// refuses the scan's first open with access denied, and is gone by the retry: the merge that
    /// holds its spans was published. The retry used to swallow that FileNotFoundException and answer
    /// "still unreadable", so the store went Degraded — every trace alert rule skipped until a
    /// restart — over a handover that lost nothing. The last failure decides now: one retry, then
    /// skipped as vanished; the store is Available and nothing is short.
    /// </summary>
    [Fact]
    public void A_segment_busy_and_then_gone_is_a_handover_not_a_degraded_store()
    {
        string trc   = WriteOneSegment();
        var logger   = new CapturingLogger();
        using var e  = new TraceStorageEngine(_dir, logger);
        int attempts = 0;
        e._readColdSegmentInfoForTest = path =>
        {
            if (++attempts == 1) throw new UnauthorizedAccessException($"Access to the path '{path}' is denied.");
            throw new FileNotFoundException("gone", path);
        };

        e.LoadColdSegments();

        Assert.Equal(2, attempts);                                       // one retry: gone is not retried again
        Assert.Equal(QueryAvailability.Available, e.Availability);
        Assert.False(e.ColdTierIncompleteForTest);
        Assert.DoesNotContain(logger.Entries, static x => x.Level == LogLevel.Error);
        Assert.True(File.Exists(trc));                                   // the seam's "gone": nothing deleted it
    }

    /// <summary>
    /// BUSY, THEN DAMAGED, IS DAMAGE (#119 review). A segment held on its first open whose bytes the
    /// reader refuses once it can open it takes the damage path — here, a readable header: deleted,
    /// with its window recorded as unreadable — and does not leave the store Degraded, which no
    /// restart would end.
    /// </summary>
    [Fact]
    public void A_segment_busy_and_then_damaged_takes_the_damage_path_not_the_degraded_one()
    {
        string trc = WriteOneSegment();
        using (var fs = new FileStream(trc, FileMode.Open, FileAccess.Write))
        {
            fs.Seek(-4, SeekOrigin.End);
            fs.Write([0xDE, 0xAD, 0xBE, 0xEF]);   // the footer magic, which a v1 file also fails on; header intact
        }

        var logger   = new CapturingLogger();
        using var e  = new TraceStorageEngine(_dir, logger);
        int attempts = 0;
        e._readColdSegmentInfoForTest = path => ++attempts == 1 ? throw new IOException("busy") : SpanReader.ReadSegmentInfo(path);

        e.LoadColdSegments();

        Assert.Equal(2, attempts);
        Assert.False(File.Exists(trc), "a segment whose bytes the reader refused was kept as merely unreachable");
        Assert.Equal(1, e.VanishedRegionCountForTest);                   // the damage path's record of the window
        Assert.Equal(QueryAvailability.Available, e.Availability);
    }

    /// <summary>
    /// A DATA DIRECTORY THAT DROPS AFTER THE LISTING IS A VOLUME, NOT A HANDOVER (#119 review). Every
    /// open of a listed segment then throws DirectoryNotFoundException, which the scan filed with
    /// FileNotFoundException as "vanished": skipped at Debug, no flag, and the store Available over
    /// a cold tier it had not read. Nothing in the engine removes its data directory, so it is now
    /// the busy path: retried through the budget (four attempts), then kept, one Error, the tier
    /// short and the store Degraded.
    /// </summary>
    [Fact]
    public void A_data_directory_gone_after_the_listing_leaves_the_store_degraded_not_available()
    {
        string trc   = WriteOneSegment();
        var logger   = new CapturingLogger();
        using var e  = new TraceStorageEngine(_dir, logger);
        int attempts = 0;
        e._readColdSegmentInfoForTest = path =>
        {
            attempts++;
            throw new DirectoryNotFoundException($"Could not find a part of the path '{path}'.");
        };

        e.LoadColdSegments();

        Assert.Equal(4, attempts);                                       // the first and three retries
        Assert.Equal(QueryAvailability.Degraded, e.Availability);
        Assert.True(e.ColdTierIncompleteForTest);
        var error = Assert.Single(logger.Entries, static x => x.Level == LogLevel.Error);
        Assert.IsType<DirectoryNotFoundException>(error.Error);
        Assert.Contains(trc, error.Message, StringComparison.Ordinal);
        Assert.True(File.Exists(trc));
    }

    /// <summary>
    /// A .TRC TOO SHORT FOR ITS FOOTER IS DAMAGE (#119 review F1). A 26-byte v3 file holds a header and
    /// nothing else; the reader's seek to the footer threw IOException, which the scan took for a busy
    /// file — retried, then the store Degraded, at every start, over bytes no restart reads. It is an
    /// InvalidDataException now, and takes the damage path: the header is readable, so the window is
    /// recorded and the file deleted; the store is Available and not short.
    /// </summary>
    [Fact]
    public void A_trc_too_short_for_its_footer_is_damage_not_a_busy_file()
    {
        string trc = Path.Combine(_dir, "spans-torn.trc");
        var bytes  = new byte[26];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, 0x52_44_54_43);        // "RDTC"
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(4), 3);           // a version this build reads
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(6), 1);           // span count
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(10), 1);           // min start
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(18), 2);           // max start
        File.WriteAllBytes(trc, bytes);

        Assert.Throws<InvalidDataException>(() => SpanReader.ReadSegmentInfo(trc));

        var logger  = new CapturingLogger();
        using var e = new TraceStorageEngine(_dir, logger);
        e.LoadColdSegments();

        Assert.False(File.Exists(trc), "a torn .trc was kept as a busy file");
        Assert.Equal(QueryAvailability.Available, e.Availability);
        Assert.False(e.ColdTierIncompleteForTest);
        Assert.DoesNotContain(logger.Entries, static x => x.Level == LogLevel.Error);
    }

    /// <summary>One segment of one span, written and flushed by an engine that is then closed; its path.</summary>
    private string WriteOneSegment()
    {
        using var writer = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        writer.LoadColdSegments();
        writer.WriteSpan(new SpanIngestItem
        {
            TraceId = new TraceId(0, 1), SpanId = new SpanId(1), ParentSpanId = default,
            StartTimeUnixNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L, DurationNanos = 1_000_000L,
            Name = "GET /orders", ServiceName = "billing", Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
        });
        writer.FlushHotTier();
        return Assert.Single(Directory.GetFiles(_dir, "*.trc"));
    }

    private sealed class CapturingLogger : ILogger<TraceStorageEngine>
    {
        private readonly List<(LogLevel Level, string Message, Exception? Error)> _entries = [];

        public IReadOnlyList<(LogLevel Level, string Message, Exception? Error)> Entries
        {
            get { lock (_entries) return [.. _entries]; }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel level) => true;

        public void Log<TState>(LogLevel level, EventId eventId, TState state, Exception? error,
                                Func<TState, Exception?, string> formatter)
        {
            lock (_entries) _entries.Add((level, formatter(state, error), error));
        }
    }
}
