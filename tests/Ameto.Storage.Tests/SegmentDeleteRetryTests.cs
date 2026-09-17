using System.Buffers;
using System.Diagnostics;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// On Windows a segment file cannot be unlinked while a reader maps it, and a query keeps its
/// readers (prefilter readers included) open for its whole duration. <c>DeleteSegmentAsync</c>
/// removed the catalog entry, <c>File.Delete</c> failed on the open mapping, and the engine
/// logged a warning and never tried again. No entry named the file any more, so retention never
/// saw it again, and after a restart the catalog scan loaded the expired segment back and served
/// it until the next retention pass.
///
/// <para>The entry is still removed at once, so no new query picks the segment. The unlink is
/// now retried: in the background with backoff for twice the query timeout, then from every
/// maintenance and retention pass. A retry must never delete a path the catalog names again.</para>
///
/// <para>Windows-only behaviour: elsewhere the unlink succeeds with the file still mapped, so
/// there is nothing to retry and each test returns early.</para>
/// </summary>
public sealed class SegmentDeleteRetryTests : IAsyncLifetime
{
    private static readonly NodeId Peer = new(7);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-segdelete-" + Guid.NewGuid().ToString("N"));
    private StorageEngine _engine = null!;

    private string SegDir => Path.Combine(_dir, "segments");

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = _dir }),
            new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance)
        {
            // Out of the way by default, so a test drives the retry itself and the background
            // attempt cannot race its assertions. The tests of the background path shorten it.
            SegmentDeleteRetryInitialDelay = TimeSpan.FromHours(1),
        };
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        try { await _engine.DisposeAsync(); } catch { }
        try { Directory.Delete(_dir, true); } catch { }
    }

    private static byte[] Props(int i)
    {
        var buf = new ArrayBufferWriter<byte>(32);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write((long)i);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>A replicated segment, written and imported the way the replication endpoint does it.</summary>
    private (string Path, SegmentKey Key) ImportPeerSegment(ulong segId)
    {
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(16, 1L << 20);
        long now = DateTime.UtcNow.Ticks;
        for (int i = 0; i < 4; i++)
            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(Peer.Value, (uint)i).RawValue,
                TimestampUtcTicks        = now + i * TimeSpan.TicksPerMillisecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = pool.Intern("peer {n}"),
            }, Props(i), "peer {n}"));
        hot.Freeze();

        Directory.CreateDirectory(SegDir);
        string path = Path.Combine(SegDir, $"{Peer.Value}-{segId}.seg");
        using (var writer = new SegmentWriter(path))
        {
            writer.WriteEvents(hot, pool);
            writer.Finalise(Peer, new SegmentId(segId));
        }

        Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(path));
        return (path, new SegmentKey(Peer, new SegmentId(segId)));
    }

    private bool InCatalog(SegmentKey key) => _engine.ListSegments().Any(s => SegmentKey.Of(s) == key);

    [Fact]
    public async Task A_delete_that_fails_on_an_open_reader_is_completed_once_the_reader_closes()
    {
        if (!OperatingSystem.IsWindows()) return;

        var (path, key) = ImportPeerSegment(11);

        var reader = SegmentReader.Open(path);   // a query mid-flight
        try
        {
            await _engine.DeleteSegmentAsync(key);

            Assert.False(InCatalog(key), "the entry must go at once, so no new query picks the segment");
            Assert.True(File.Exists(path), "setup: the open mapping should have blocked the unlink");
            Assert.Equal(1, _engine.PendingSegmentDeleteCount);

            // Still held: an attempt fails and keeps the path pending.
            Assert.Equal(1, _engine.RetryPendingSegmentDeletes());
            Assert.True(File.Exists(path));
        }
        finally { reader.Dispose(); }

        // The query finished; the next pass (maintenance or retention) completes the delete.
        Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
        Assert.False(File.Exists(path), "the expired segment's file outlived its reader");
    }

    [Fact]
    public async Task A_pending_delete_does_not_remove_a_path_the_catalog_names_again()
    {
        if (!OperatingSystem.IsWindows()) return;

        var (path, key) = ImportPeerSegment(12);

        using (SegmentReader.Open(path))
        {
            await _engine.DeleteSegmentAsync(key);
            Assert.True(File.Exists(path), "setup: the open mapping should have blocked the unlink");
            Assert.Equal(1, _engine.PendingSegmentDeleteCount);

            // The peer pushes the same segment again, to the same path, before the retry runs.
            Assert.Equal(SegmentImportOutcome.Registered, _engine.ImportSegment(path));
        }

        Assert.Equal(0, _engine.RetryPendingSegmentDeletes());
        Assert.True(File.Exists(path), "a stale retry deleted the file of a live catalog entry");
        Assert.True(InCatalog(key));
        Assert.Equal(0, _engine.PendingSegmentDeleteCount);
    }

    [Fact]
    public async Task The_background_retry_deletes_the_file_after_the_reader_closes()
    {
        if (!OperatingSystem.IsWindows()) return;

        _engine.SegmentDeleteRetryInitialDelay = TimeSpan.FromMilliseconds(20);
        var (path, key) = ImportPeerSegment(13);

        using (SegmentReader.Open(path))
        {
            await _engine.DeleteSegmentAsync(key);
            Assert.True(File.Exists(path), "setup: the open mapping should have blocked the unlink");
            await Task.Delay(150);   // a few attempts fail against the open reader
            Assert.True(File.Exists(path));
        }

        var sw = Stopwatch.StartNew();
        while (File.Exists(path) && sw.Elapsed < TimeSpan.FromSeconds(20))
            await Task.Delay(20);

        Assert.False(File.Exists(path), "no background attempt deleted the file after the reader closed");
        Assert.Equal(0, _engine.PendingSegmentDeleteCount);
    }

    [Fact]
    public async Task Dispose_does_not_wait_out_a_pending_retry()
    {
        if (!OperatingSystem.IsWindows()) return;

        // The production schedule: a 120 s retry window ahead of it.
        _engine.SegmentDeleteRetryInitialDelay = TimeSpan.FromMilliseconds(500);
        var (path, key) = ImportPeerSegment(14);

        using (SegmentReader.Open(path))
        {
            await _engine.DeleteSegmentAsync(key);
            Assert.Equal(1, _engine.PendingSegmentDeleteCount);

            var sw = Stopwatch.StartNew();
            await _engine.DisposeAsync();
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10), $"DisposeAsync took {sw.Elapsed}");
        }
    }
}
