using System.Buffers;
using System.Buffers.Binary;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// A log WAL in a format this build does not know reaches one of its starts in one way: an operator
/// rolled back from the release that wrote it. Every release before log WAL v5 deletes such a file at
/// start, unread and without a word — the rollback cost docs/CONFIGURATION.md documents for v5 — and
/// so did this one. It now leaves the file for the release that can read it, with its segment-id
/// block reserved and an Error saying so, which makes v5 the last log WAL format a rollback has to
/// plan around.
/// </summary>
public sealed class WalFromALaterReleaseTests : IDisposable
{
    private const ulong KeptBlock = 5_000;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-wallater-" + Guid.NewGuid().ToString("N"));

    private string WalDir => Path.Combine(_dir, "wal");

    public void Dispose()
    {
        try { Directory.Delete(_dir, true); } catch { }
    }

    private StorageEngine NewEngine(Microsoft.Extensions.Logging.ILogger<StorageEngine>? logger = null) => new(
        Options.Create(new ServerOptions { DataDirectory = _dir }),
        new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
        logger ?? NullLogger<StorageEngine>.Instance);

    [Fact]
    public async Task A_wal_of_a_later_format_is_kept_with_its_block_reserved_and_replayed_once_it_can_be_read()
    {
        Directory.CreateDirectory(WalDir);
        string walPath = Path.Combine(WalDir, $"0-{KeptBlock}.wal");
        long t = DateTime.UtcNow.Ticks;

        // A v5 log whose header claims v6: what a later release that only ADDED to the format would
        // leave behind, with one acknowledged event in it.
        using (var wal = WriteAheadLog.Open(walPath, NodeId.Local, new SegmentId(KeptBlock), 64 * 1024))
            wal.Append(t, LogLevel.Information, 0, "later {N}", Props(1), serviceIndex: 1, service: "Later.Api");
        SetVersion(walPath, (ushort)(WriteAheadLog.FormatVersion + 1));
        byte[] walBytes  = File.ReadAllBytes(walPath);
        byte[] poolBytes = File.ReadAllBytes(walPath + ".pool");

        var logger = new CapturingLogger();
        var engine = NewEngine(logger);
        try
        {
            await engine.CatalogLoaded;

            Assert.Equal(walBytes,  File.ReadAllBytes(walPath));              // neither deleted nor rewritten
            Assert.Equal(poolBytes, File.ReadAllBytes(walPath + ".pool"));
            Assert.Empty(engine.ListSegments());                               // nor replayed as something else
            Assert.True(engine.LiveWalSegmentId >= KeptBlock + 6,
                $"the live WAL took block {engine.LiveWalSegmentId}, inside the kept log's {KeptBlock}..{KeptBlock + 5}");
            Assert.Contains(logger.Entries, e =>
                e.Level == Microsoft.Extensions.Logging.LogLevel.Error
                && e.Message.Contains(Path.GetFileName(walPath), StringComparison.Ordinal)
                && e.Message.Contains($"format v{WriteAheadLog.FormatVersion + 1}", StringComparison.Ordinal));
        }
        finally { await engine.DisposeAsync(); }

        // The release that wrote it starts again — here, the same bytes under the version this build
        // reads — and replays it like any orphaned log.
        SetVersion(walPath, WriteAheadLog.FormatVersion);
        await using var again = NewEngine();
        await again.CatalogLoaded;

        var seg = Assert.Single(again.ListSegments());
        using var reader = SegmentReader.Open(seg.FilePath);
        var ev = Assert.Single(reader.ReadAllRaw(new Dictionary<string, string>(StringComparer.Ordinal)).ToList());
        Assert.Equal(t, ev.TsTicks);
        Assert.Equal("later {N}", ev.Template);
        Assert.Equal("Later.Api", ev.Service);
        Assert.False(File.Exists(walPath));
    }

    /// <summary>
    /// The most common rollback: the later release stopped CLEANLY. Its stop leaves one log in
    /// <c>wal/</c>, empty — the final flush hands its events to segments and opens a successor
    /// that nothing is written to — and that log is the later format too. It is kept and its block
    /// reserved like any other, but it is no Error: an Error at every start saying it held events
    /// sent anyone alerting on Error-level self-logs after a file a hex dump would show was empty.
    /// The file is this release's own clean-stop leftover, stamped one version up.
    /// </summary>
    [Fact]
    public async Task The_empty_log_a_later_releases_clean_stop_leaves_is_kept_without_an_error()
    {
        Directory.CreateDirectory(_dir);
        var engine = NewEngine();
        await engine.CatalogLoaded;
        Assert.True(engine.TryWrite(new LogEventHeader
        {
            TimestampUtcTicks        = DateTime.UtcNow.Ticks,
            Level                    = LogLevel.Information,
            MessageTemplatePoolIndex = engine.TemplatePool.Intern("t {n}"),
            ServiceNamePoolIndex     = engine.TemplatePool.Intern("Svc.A"),
        }, Props(1), "t {n}"));
        await engine.DisposeAsync();                                                     // clean stop

        string wal = Assert.Single(Directory.GetFiles(WalDir, "*.wal"));
        Assert.Equal(32L, BinaryPrimitives.ReadInt64LittleEndian(ReadHeader(wal).AsSpan(24)));   // WriteOffset: empty
        ulong block = BinaryPrimitives.ReadUInt64LittleEndian(ReadHeader(wal).AsSpan(16));
        SetVersion(wal, (ushort)(WriteAheadLog.FormatVersion + 1));                      // what a later release's clean stop leaves

        for (int start = 1; start <= 2; start++)
        {
            var logger = new CapturingLogger();
            var e      = NewEngine(logger);
            try
            {
                await e.CatalogLoaded;
                Assert.True(e.LiveWalSegmentId >= block + 6, $"start {start}: the live WAL took block {e.LiveWalSegmentId}, inside the kept log's");
            }
            finally { await e.DisposeAsync(); }

            Assert.True(File.Exists(wal), $"start {start} deleted the later release's log");
            Assert.DoesNotContain(logger.Entries, x => x.Level == Microsoft.Extensions.Logging.LogLevel.Error);
            Assert.Contains(logger.Entries, x =>
                x.Level == Microsoft.Extensions.Logging.LogLevel.Information
                && x.Message.Contains(Path.GetFileName(wal), StringComparison.Ordinal)
                && x.Message.Contains("records no entries", StringComparison.Ordinal));
        }
    }

    private static byte[] ReadHeader(string walPath)
    {
        var header = new byte[32];
        using var fs = new FileStream(walPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        fs.ReadExactly(header);
        return header;
    }

    private static void SetVersion(string walPath, ushort version)
    {
        using var fs = new FileStream(walPath, FileMode.Open, FileAccess.ReadWrite);
        Span<byte> v = stackalloc byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(v, version);
        fs.Seek(4, SeekOrigin.Begin);     // the file header's Version field
        fs.Write(v);
    }

    private static byte[] Props(int n)
    {
        var buf = new ArrayBufferWriter<byte>(32);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("N"); w.Write((long)n);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger<StorageEngine>
    {
        private readonly List<(Microsoft.Extensions.Logging.LogLevel Level, string Message)> _entries = [];

        public IReadOnlyList<(Microsoft.Extensions.Logging.LogLevel Level, string Message)> Entries
        {
            get { lock (_entries) return _entries.ToList(); }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel level) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel level,
                                Microsoft.Extensions.Logging.EventId eventId, TState state,
                                Exception? error, Func<TState, Exception?, string> formatter)
        {
            lock (_entries) _entries.Add((level, formatter(state, error)));
        }
    }
}
