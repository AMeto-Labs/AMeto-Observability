using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// The WAL entry has a 16-bit template index and no "not pooled" value, so an event
/// whose header says -1 is logged as index 0. That alias is harmless only while such an
/// event never WRITES a pool row. The pool file maps index to text, and recovery
/// force-interns every row it holds, so a row 0 carrying an unpooled event's text becomes
/// the template of every genuine index-0 event in that WAL.
///
/// <para>Past pool saturation the receivers attach the materialised template to a -1
/// event, so that is exactly what TryWrite was handed. The first such event into a fresh
/// WAL claimed row 0, and after a kill -9 every index-0 event from that WAL came back with
/// its text. An index at or past 65 536 is the same collision: its (ushort) cast is 0.</para>
/// </summary>
public sealed class WalUnpooledTemplateTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-walunpooled-" + Guid.NewGuid().ToString("N"));
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

    private void Write(long ticks, int poolIndex, string template, int n) =>
        Assert.True(_engine.TryWrite(new LogEventHeader
        {
            TimestampUtcTicks        = ticks,
            Level                    = LogLevel.Information,
            MessageTemplatePoolIndex = poolIndex,
        }, Props(n), template));

    [Theory]
    [InlineData(-1)]       // the pool is full: Intern answered -1 and the text is attached
    [InlineData(65_536)]   // a claim past the cap: (ushort) truncates it to 0
    public async Task AnEventOutsideThePool_DoesNotRewriteTemplateZero_ForTheEventsRecoveredWithIt(int outsideIndex)
    {
        const string IndexZeroTemplate = "Starting {App}";
        const string UnpooledTemplate  = "user 4711 logged in";

        Assert.Equal(0, _engine.TemplatePool.Intern(IndexZeroTemplate));
        long t = DateTime.UtcNow.Ticks;

        // One event, so the flush has a tier to swap and the WAL rotates: the successor
        // starts with no pool row saved, which is the state the first event into it meets.
        Write(t, 0, IndexZeroTemplate, 1);
        await _engine.FlushHotTierAsync();

        for (int i = 1; i < 65_536; i++) _engine.TemplatePool.Intern("filler " + i);
        Assert.Equal(-1, _engine.TemplatePool.Intern("proof the pool is full"));

        // The first event into the fresh WAL lies outside the pool; a pooled index-0 event follows.
        long unpooledTicks = t + TimeSpan.TicksPerSecond;
        long pooledTicks   = t + 2 * TimeSpan.TicksPerSecond;
        Write(unpooledTicks, outsideIndex, UnpooledTemplate, 2);
        Write(pooledTicks,   0,            _engine.TemplatePool.Get(0), 3);

        // kill -9: keep the WAL bytes, let shutdown flush and unlink them, then undo that flush.
        ulong walId = _engine.LiveWalSegmentId;
        string wal  = Directory.GetFiles(WalDir, "*.wal").Single();
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

        var dedup = new Dictionary<string, string>(StringComparer.Ordinal);
        RawSegmentEvent? recovered = null;
        int total = 0;
        foreach (var seg in _engine.ListSegments())
        {
            using var r = SegmentReader.Open(seg.FilePath);
            foreach (var ev in r.ReadAllRaw(dedup))
            {
                total++;
                if (ev.TsTicks == pooledTicks) recovered = ev;
            }
        }

        Assert.Equal(3, total);   // the pre-rotation event, plus both replayed from the WAL
        Assert.NotNull(recovered);
        Assert.Equal(IndexZeroTemplate, recovered.Value.Template);
    }
}
