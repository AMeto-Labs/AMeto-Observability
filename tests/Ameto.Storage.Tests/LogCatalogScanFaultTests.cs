using System.Buffers.Binary;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using MsLogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE LOG CATALOG SCAN SETS A SEGMENT ASIDE ONLY FOR ITS BYTES (#119 review — #108's rule, which the
/// metric and trace scans of the same PR follow).
///
/// <para>The boot catalog scan renamed to <c>.seg.corrupt</c> every segment whose open threw,
/// whatever the throw said: a segment an antivirus or a backup agent held for a moment, one on a
/// share that blinked, was set aside, served by nobody until an operator renamed it back — and the
/// store read Available over the hole. Now the failure is classified: content
/// (<see cref="FileBounds.DescribesContent"/>) is quarantined as before; a delete's file is skipped
/// as before; anything else is retried a few times and then left where it is, out of this run's
/// catalog, with one Error naming it and the store <see cref="QueryAvailability.Degraded"/> until the
/// next start reads it.</para>
///
/// <para>The engines here are built with their boot scan held, so the seams — the scan's open hook,
/// the pause between attempts — are in place before it runs, and the scan is the real one whose
/// task <see cref="StorageEngine.Availability"/> reads.</para>
/// </summary>
public sealed class LogCatalogScanFaultTests : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    /// <summary>The scan's pauses between attempts at a segment it could not open — <c>CatalogReadRetryDelays</c>.</summary>
    private static readonly TimeSpan[] Delays =
    [
        TimeSpan.FromMilliseconds(25), TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(100),
        TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(400),
    ];

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-logscanfault-" + Guid.NewGuid().ToString("N"));

    private string SegDir => Path.Combine(_dir, "segments");

    public LogCatalogScanFaultTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    // ── Not about the bytes: kept where it is ─────────────────────────────────────────────────

    /// <summary>
    /// THE REVIEW'S CASE. A sharing violation on one segment on every attempt: after the start the
    /// file is under its own name — not <c>.seg.corrupt</c> — and out of the catalog, while the segment
    /// beside it is in; one Error names it; the store is Degraded; the pauses were the schedule's. The
    /// next start, with nothing in the way, serves it and is Available. Before, the first start set it
    /// aside for good and read Available.
    /// </summary>
    [Fact]
    public async Task A_segment_held_by_another_process_is_kept_unread_and_the_next_start_serves_it()
    {
        var segs = await WriteSegmentsAsync(2);
        string busy = segs[0];
        var sharing = new IOException("The process cannot access the file because it is being used by another process.");
        var log   = new Entries();
        var waits = new List<TimeSpan>();

        await using (var engine = await StartAsync(log, e =>
        {
            e._beforeScanOpensSegment = path => { if (path == busy) throw sharing; };
            e._catalogScanWaitForTest = waits.Add;
        }))
        {
            Assert.True(File.Exists(busy), "the segment was moved from its name");
            Assert.False(File.Exists(busy + ".corrupt"), "a segment held by another process was set aside as corrupt");
            Assert.DoesNotContain(engine.ListSegments(), s => s.FilePath == busy);
            Assert.Contains(engine.ListSegments(), s => s.FilePath == segs[1]);
            Assert.Equal(QueryAvailability.Degraded, engine.Availability);

            var error = Assert.Single(log.Snapshot(), e => e.Level >= MsLogLevel.Error);
            Assert.Same(sharing, error.Error);
            Assert.Contains(busy, error.Message);
            Assert.Contains("not set aside as .corrupt", error.Message);
            Assert.Equal(Delays, waits);
        }

        var next = new Entries();
        await using var restarted = await StartAsync(next, static _ => { });
        Assert.Contains(restarted.ListSegments(), s => s.FilePath == busy);
        Assert.Equal(QueryAvailability.Available, restarted.Availability);
        Assert.DoesNotContain(next.Snapshot(), e => e.Level >= MsLogLevel.Error);
    }

    /// <summary>
    /// What the retries are for: a segment refused twice and readable on the third attempt is
    /// registered, after the schedule's first two pauses, with a Warning and no Error; Available.
    /// </summary>
    [Fact]
    public async Task A_segment_busy_for_its_first_attempts_is_registered_and_the_store_is_available()
    {
        var segs = await WriteSegmentsAsync(1);
        int attempts = 0;
        var log   = new Entries();
        var waits = new List<TimeSpan>();

        await using var engine = await StartAsync(log, e =>
        {
            e._beforeScanOpensSegment = _ => { if (++attempts <= 2) throw new IOException("busy"); };
            e._catalogScanWaitForTest = waits.Add;
        });

        Assert.Contains(engine.ListSegments(), s => s.FilePath == segs[0]);
        Assert.Equal(QueryAvailability.Available, engine.Availability);
        Assert.Equal(Delays[..2], waits);
        Assert.DoesNotContain(log.Snapshot(), e => e.Level >= MsLogLevel.Error);
        Assert.Single(log.Snapshot(), e => e.Level == MsLogLevel.Warning && e.Message.Contains("read on attempt 3"));
    }

    /// <summary>
    /// A file the open finds gone, with no delete of ours recorded for it, is NOT skipped quietly —
    /// the scan's own rule: a failing mount answers "not there" for a live segment, and nobody would
    /// be told. Retried, then reported, and the store Degraded; nothing is set aside.
    /// </summary>
    [Fact]
    public async Task A_segment_gone_with_no_delete_recorded_is_reported_and_degrades_the_store()
    {
        var segs = await WriteSegmentsAsync(1);
        var log  = new Entries();

        await using var engine = await StartAsync(log, e =>
        {
            e._beforeScanOpensSegment = path => throw new FileNotFoundException("not there", path);
            e._catalogScanWaitForTest = static _ => { };
        });

        Assert.False(File.Exists(segs[0] + ".corrupt"));
        Assert.Equal(QueryAvailability.Degraded, engine.Availability);
        Assert.IsType<FileNotFoundException>(Assert.Single(log.Snapshot(), e => e.Level >= MsLogLevel.Error).Error);
    }

    // ── About the bytes: set aside, as before ─────────────────────────────────────────────────

    /// <summary>
    /// Today's handling, kept — and the handling #98's merge recovery relies on for a torn merge
    /// output: a segment whose bytes the reader refuses (here, cut to half its length) is set aside as
    /// <c>.seg.corrupt</c> at once, without a retry, and does not make the store Degraded.
    /// </summary>
    [Fact]
    public async Task A_torn_segment_is_set_aside_without_a_retry_and_degrades_nothing()
    {
        var segs = await WriteSegmentsAsync(1);
        Halve(segs[0]);
        var log = new Entries();

        await using var engine = await StartAsync(log, e =>
            e._catalogScanWaitForTest = static _ => throw new InvalidOperationException("damage is not retried"));

        Assert.False(File.Exists(segs[0]));
        Assert.True(File.Exists(segs[0] + ".corrupt"));
        Assert.Equal(QueryAvailability.Available, engine.Availability);
        Assert.Single(log.Snapshot(), e => e.Level >= MsLogLevel.Error && e.Message.Contains("Quarantining unreadable segment"));
    }

    /// <summary>
    /// THE LAST ATTEMPT DECIDES. A segment held on its first open whose bytes turn out torn once it
    /// can be opened is set aside after one pause — damage, not a load left unfinished.
    /// </summary>
    [Fact]
    public async Task A_busy_segment_whose_bytes_turn_out_torn_is_set_aside_not_left_unread()
    {
        var segs = await WriteSegmentsAsync(1);
        Halve(segs[0]);
        int attempts = 0;
        var log   = new Entries();
        var waits = new List<TimeSpan>();

        await using var engine = await StartAsync(log, e =>
        {
            e._beforeScanOpensSegment = _ => { if (++attempts == 1) throw new IOException("busy"); };
            e._catalogScanWaitForTest = waits.Add;
        });

        Assert.Equal(2, attempts);
        Assert.Equal(Delays[..1], waits);
        Assert.True(File.Exists(segs[0] + ".corrupt"));
        Assert.Equal(QueryAvailability.Available, engine.Availability);
    }

    /// <summary>
    /// A COUNT THE FILE CANNOT HOLD IS DAMAGE. A block-index or group-directory count torn to two
    /// billion failed the reader's allocation with <see cref="OutOfMemoryException"/> — a word about
    /// the machine, which the scan now keeps as "unreachable" at every start. The reader names it as
    /// the bytes it is (<see cref="InvalidDataException"/>), and the scan sets the file aside.
    /// </summary>
    [Theory]
    [InlineData("block index")]
    [InlineData("group directory")]
    public async Task A_torn_count_is_named_as_damage_and_the_segment_set_aside(string which)
    {
        var segs = await WriteSegmentsAsync(1);
        TearCount(segs[0], which);

        Assert.Throws<InvalidDataException>(() => SegmentReader.Open(segs[0]));

        await using var engine = await StartAsync(new Entries(), e =>
            e._catalogScanWaitForTest = static _ => throw new InvalidOperationException("damage is not retried"));

        Assert.True(File.Exists(segs[0] + ".corrupt"));
        Assert.Equal(QueryAvailability.Available, engine.Availability);
    }

    /// <summary>
    /// A LOST INDEX PAGE IS DAMAGE, NOT AN EMPTY SEGMENT (#119 review F3). A flushed ten-event
    /// segment whose block-index count is zeroed opened "whole" — the header's ten events, no block —
    /// and served none of them, registered, the store Available, nothing said. The reader names it as
    /// damage now, and the scan sets it aside.
    /// </summary>
    [Fact]
    public async Task A_segment_whose_index_lists_no_block_for_its_events_is_damage_and_set_aside()
    {
        var segs = await WriteSegmentsAsync(1);
        WriteInt32At(segs[0], FooterSlot(segs[0], BlockIndexSlot), 0);   // the block-index count, zeroed

        Assert.Throws<InvalidDataException>(() => SegmentReader.Open(segs[0]));

        await using var engine = await StartAsync(new Entries(), e =>
            e._catalogScanWaitForTest = static _ => throw new InvalidOperationException("damage is not retried"));

        Assert.True(File.Exists(segs[0] + ".corrupt"), "a segment that serves none of its events was kept in service");
        Assert.DoesNotContain(engine.ListSegments(), s => s.FilePath == segs[0]);
        Assert.Equal(QueryAvailability.Available, engine.Availability);
    }

    /// <summary>
    /// AN OFFSET TORN INTO THE FILE'S LAST BYTES IS DAMAGE (#119 review F1). The reader read its count
    /// or frame AT the offset, and a position inside the view's last few bytes failed with a plain
    /// ArgumentException, which no content classifier counts: the scan kept the segment as
    /// unreachable, and the store was Degraded at every start, its alert rules skipped for good. Each
    /// offset the bytes give is checked first now: InvalidDataException, set aside once, Available.
    /// A torn BLOCK offset did not even fail the open — only a read of that block, later.
    /// </summary>
    [Theory]
    [InlineData("block index offset")]
    [InlineData("group directory offset")]
    [InlineData("block offset")]
    public async Task An_offset_torn_into_the_last_bytes_is_damage_and_the_segment_set_aside(string which)
    {
        var segs = await WriteSegmentsAsync(1);
        string seg  = segs[0];
        long length = new FileInfo(seg).Length;
        long slot   = which switch
        {
            "block index offset"     => length - 44 + BlockIndexSlot,
            "group directory offset" => length - 44 + GroupDirectorySlot,
            _                        => FooterSlot(seg, BlockIndexSlot) + 4,   // the first block's entry
        };
        WriteInt64At(seg, slot, length - 2);

        Assert.Throws<InvalidDataException>(() => SegmentReader.Open(seg));

        await using var engine = await StartAsync(new Entries(), e => e._catalogScanWaitForTest = static _ => { });

        Assert.True(File.Exists(seg + ".corrupt"), $"{which}: the torn segment was kept as unreachable");
        Assert.Equal(QueryAvailability.Available, engine.Availability);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────────────────────

    /// <summary>Footer slot offsets (the footer is a file's last 44 bytes): the v7 group directory, and the block index.</summary>
    private const int GroupDirectorySlot = 0, BlockIndexSlot = 24;

    /// <summary>The int64 in footer slot <paramref name="slot"/> — an offset into the file.</summary>
    private static long FooterSlot(string path, int slot)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read);
        Span<byte> v = stackalloc byte[8];
        fs.Seek(fs.Length - 44 + slot, SeekOrigin.Begin);
        fs.ReadExactly(v);
        return BinaryPrimitives.ReadInt64LittleEndian(v);
    }

    private static void WriteInt32At(string path, long at, int value)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Write);
        Span<byte> v = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(v, value);
        fs.Seek(at, SeekOrigin.Begin);
        fs.Write(v);
    }

    private static void WriteInt64At(string path, long at, long value)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Write);
        Span<byte> v = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(v, value);
        fs.Seek(at, SeekOrigin.Begin);
        fs.Write(v);
    }

    /// <summary><paramref name="count"/> segments of ten events each, ten minutes ago, written by an engine that is then closed.</summary>
    private async Task<string[]> WriteSegmentsAsync(int count)
    {
        var opts   = new ServerOptions { DataDirectory = _dir };
        var engine = new StorageEngine(
            Options.Create(opts), new RetentionStore(opts, NullLogger<RetentionStore>.Instance), NullLogger<StorageEngine>.Instance);
        engine.IndexSinkFactory = static (c, t) => new SegmentIndexBuilder(c, 5, t);
        try
        {
            await engine.CatalogLoaded.WaitAsync(Timeout);
            long   baseTicks = DateTimeOffset.UtcNow.AddMinutes(-10).UtcTicks;
            byte[] payload   = [0x81, 0xA1, (byte)'k', 0x00];   // msgpack {"k": 0}
            for (int s = 0; s < count; s++)
            {
                for (int i = 0; i < 10; i++)
                    Assert.True(engine.TryWrite(new LogEventHeader
                    {
                        TimestampUtcTicks        = baseTicks + (s * 10 + i) * TimeSpan.TicksPerSecond,
                        Level                    = Ameto.Core.LogLevel.Information,
                        MessageTemplatePoolIndex = engine.TemplatePool.Intern("evt {k}"),
                        ServiceNamePoolIndex     = engine.TemplatePool.Intern("checkout"),
                    }, payload));
                await engine.FlushHotTierAsync();
            }
        }
        finally { await engine.DisposeAsync(); }

        var files = Directory.GetFiles(SegDir, "*.seg");
        Array.Sort(files, StringComparer.Ordinal);
        Assert.Equal(count, files.Length);
        return files;
    }

    /// <summary>An engine over the directory whose boot scan runs only once <paramref name="arm"/> has set its seams; returned when the scan has ended.</summary>
    private async Task<StorageEngine> StartAsync(Entries log, Action<StorageEngine> arm)
    {
        var opts = new ServerOptions { DataDirectory = _dir };
        var hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var engine = new StorageEngine(
            Options.Create(opts), new RetentionStore(opts, NullLogger<RetentionStore>.Instance), log,
            maintenanceStartDelay: TimeSpan.FromHours(1), bootScanHeldUntil: hold.Task);
        engine.IndexSinkFactory = static (c, t) => new SegmentIndexBuilder(c, 5, t);
        arm(engine);
        hold.SetResult();
        await engine.CatalogLoaded.WaitAsync(Timeout);
        return engine;
    }

    /// <summary>Cuts a segment to half its length: the torn file a lost write leaves, whose footer is gone.</summary>
    private static void Halve(string path)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Write);
        fs.SetLength(fs.Length / 2);
    }

    /// <summary>Overwrites the block index's count, or the v7 group directory's, with two billion.</summary>
    private static void TearCount(string path, string which)
    {
        long offset = FooterSlot(path, which == "block index" ? BlockIndexSlot : GroupDirectorySlot);
        Assert.InRange(offset, 1, new FileInfo(path).Length - 4);   // setup: a v7 segment, whose footer names both
        WriteInt32At(path, offset, 0x7FFF_FFF0);
    }

    /// <summary>Every entry at every level, formatted.</summary>
    private sealed class Entries : ILogger<StorageEngine>
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
