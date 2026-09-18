using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// A WAL entry carries no service name — there is no field for one. So every event replayed
/// out of an orphaned WAL has no service, and the header the replay builds must SAY so.
///
/// <para>It said the opposite. <see cref="LogEventHeader.ServiceNamePoolIndex"/> is a plain int
/// on a struct and the replay initialiser never set it, so it took its default of 0 — which is
/// not the "absent" sentinel (<c>-1</c>) every reader tests for, but a valid pool index. The
/// intern pool is shared between message templates and service names, and recovery
/// force-interns the WAL's own pool rows into it before replaying, so slot 0 holds that WAL's
/// first template. Every recovered event was therefore stamped with a message template as its
/// service name, and the recovery flush wrote that string into the segment's <c>@svc</c>
/// column, where it is permanent: those rows answer <c>service.name = 'Starting {App}'</c> and
/// are attributed to it by the per-service counts.</para>
///
/// <para>The sibling suite <c>WalUnpooledTemplateTests</c> covers the same replay path for the
/// TEMPLATE index and asserts only on templates, which is why this went unseen.</para>
/// </summary>
public sealed class WalRecoveredServiceTests : IAsyncLifetime
{
    private const string IndexZeroTemplate = "Starting {App}";

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-walsvc-" + Guid.NewGuid().ToString("N"));
    private StorageEngine _engine = null!;

    private string SegDir => Path.Combine(_dir, "segments");
    private string WalDir => Path.Combine(_dir, "wal");

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _engine = NewEngine();
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private StorageEngine NewEngine() => new(
        Options.Create(new ServerOptions { DataDirectory = _dir }),
        new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
        NullLogger<StorageEngine>.Instance);

    private static byte[] Props(int n)
    {
        var buf = new ArrayBufferWriter<byte>(32);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write((long)n);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>One event with NO service — the ordinary shape for a CLEF batch that sets none.</summary>
    private void WriteServiceless(long ticks, int n) =>
        Assert.True(_engine.TryWrite(new LogEventHeader
        {
            TimestampUtcTicks        = ticks,
            Level                    = LogLevel.Information,
            MessageTemplatePoolIndex = 0,
            ServiceNamePoolIndex     = -1,
        }, Props(n), IndexZeroTemplate));

    [Fact]
    public async Task EventsReplayedFromAnOrphanedWal_HaveNoService_NotPoolSlotZero()
    {
        Assert.Equal(0, _engine.TemplatePool.Intern(IndexZeroTemplate));
        long t = DateTime.UtcNow.Ticks;

        // One event and a flush, so the WAL rotates and the events below land in a successor
        // whose pool file will carry row 0 — the row recovery force-interns.
        WriteServiceless(t, 1);
        await _engine.FlushHotTierAsync();

        long firstTicks  = t + TimeSpan.TicksPerSecond;
        long secondTicks = t + 2 * TimeSpan.TicksPerSecond;
        WriteServiceless(firstTicks, 2);
        WriteServiceless(secondTicks, 3);

        // kill -9: keep the WAL bytes, let shutdown flush and unlink them, then undo that flush
        // so the WAL is orphaned and the next start replays it.
        ulong  walId = _engine.LiveWalSegmentId;
        string wal   = Directory.GetFiles(WalDir, "*.wal").Single();
        File.Copy(wal, wal + ".crash", overwrite: true);
        File.Copy(wal + ".pool", wal + ".pool.crash", overwrite: true);

        await _engine.DisposeAsync();
        foreach (var f in Directory.GetFiles(SegDir, "*.seg"))
        {
            var parts = Path.GetFileNameWithoutExtension(f).Split('-');
            if (ulong.TryParse(parts[1], out var id) && id >= walId && id < walId + 6) File.Delete(f);
        }
        File.Move(wal + ".crash", wal, overwrite: true);
        File.Move(wal + ".pool.crash", wal + ".pool", overwrite: true);

        _engine = NewEngine();
        await _engine.CatalogLoaded;

        // Slot 0 is a message template, restored by the replay itself — the string an unset
        // ServiceNamePoolIndex resolves to.
        Assert.Equal(IndexZeroTemplate, _engine.TemplatePool.Get(0));

        var dedup     = new Dictionary<string, string>(StringComparer.Ordinal);
        var recovered = new List<RawSegmentEvent>();
        int total     = 0;
        foreach (var seg in _engine.ListSegments())
        {
            using var r = SegmentReader.Open(seg.FilePath);
            foreach (var ev in r.ReadAllRaw(dedup))
            {
                total++;
                if (ev.TsTicks == firstTicks || ev.TsTicks == secondTicks) recovered.Add(ev);
            }
        }

        Assert.Equal(3, total);              // the pre-rotation event, plus both replayed
        Assert.Equal(2, recovered.Count);
        foreach (var ev in recovered)
            Assert.True(string.IsNullOrEmpty(ev.Service),
                $"a replayed event was stamped with pool slot 0 as its service: '{ev.Service}'");
    }
}
