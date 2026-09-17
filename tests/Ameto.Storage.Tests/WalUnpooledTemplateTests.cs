using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// The WAL entry has a 16-bit template index, and all 65 536 of its values are real pool ids,
/// so no index can mean "not pooled". Events outside the pool went wrong on both sides of
/// that alias.
///
/// <para>The pool file maps index to text, and recovery force-interns every row it holds. Past
/// pool saturation the receivers attach the materialised template to a -1 event, so that is
/// exactly what TryWrite was handed. The first such event into a fresh WAL claimed row 0, and
/// after a kill -9 every genuine index-0 event from that WAL came back with its text. An index
/// at or past 65 536 is the same collision: its (ushort) cast is 0.</para>
///
/// <para>Writing no pool row fixed that side and moved the bug to the other one. The unpooled
/// event was still logged as index 0, so a replay whose pool file held any row resolved it and
/// gave the unpooled event index 0's template. The entry now carries an Unpooled flag in the
/// header byte that used to be padding, and recovery gives a flagged event no template: its
/// text was never stored.</para>
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

    /// <summary>Every event in the catalog's segments, keyed by timestamp.</summary>
    private Dictionary<long, RawSegmentEvent> ReadAllByTicks()
    {
        var dedup  = new Dictionary<string, string>(StringComparer.Ordinal);
        var byTick = new Dictionary<long, RawSegmentEvent>();
        foreach (var seg in _engine.ListSegments())
        {
            using var r = SegmentReader.Open(seg.FilePath);
            foreach (var ev in r.ReadAllRaw(dedup))
                byTick.Add(ev.TsTicks, ev);
        }
        return byTick;
    }

    [Theory]
    [InlineData(-1)]       // the pool is full: Intern answered -1 and the text is attached
    [InlineData(65_536)]   // a claim past the cap: (ushort) truncates it to 0
    public async Task AnEventOutsideThePool_AndAnIndexZeroEvent_EachRecoverTheirOwnTemplate(int outsideIndex)
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

        var events = ReadAllByTicks();
        Assert.Equal(3, events.Count);   // the pre-rotation event, plus both replayed from the WAL

        // The genuine index-0 event keeps its template: the unpooled event wrote no row 0.
        Assert.Equal(IndexZeroTemplate, events[pooledTicks].Template);

        // The unpooled event gets NO template: the WAL never stored its text, and its logged
        // index of 0 must not be resolved against the pool rows that did survive.
        Assert.True(string.IsNullOrEmpty(events[unpooledTicks].Template),
            $"the unpooled event was recovered with template '{events[unpooledTicks].Template}'");
    }

    /// <summary>
    /// A WAL written by the build before the Unpooled flag. Byte 13 of every entry header was
    /// padding that build never wrote, so it is zero in its files. Assembled here by hand from
    /// the documented v4 layout rather than through Append, so the test pins the bytes on disk
    /// and not whatever the current writer produces: an index-0 entry and an index-1 entry, a
    /// pool file naming both, replayed at startup, each with its own template.
    /// </summary>
    [Fact]
    public async Task A_wal_written_before_the_flag_existed_replays_unchanged()
    {
        await _engine.DisposeAsync();

        const ulong SegId = 1_000;
        long t = DateTime.UtcNow.Ticks;
        string walPath = Path.Combine(WalDir, $"0-{SegId}.wal");

        var entries = new List<byte[]>
        {
            PreviousBuildEntry(t,                           LogLevel.Information, 0, Props(10)),
            PreviousBuildEntry(t + TimeSpan.TicksPerSecond, LogLevel.Warning,     1, Props(11)),
        };
        long dataBytes = entries.Sum(e => e.Length);

        var file = new byte[32 + 64 * 1024];
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(0),  0x52_44_57_41);   // "RDWA"
        BinaryPrimitives.WriteUInt16LittleEndian(file.AsSpan(4),  4);               // v4
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(8),  0);               // node
        BinaryPrimitives.WriteUInt64LittleEndian(file.AsSpan(16), SegId);
        BinaryPrimitives.WriteInt64LittleEndian (file.AsSpan(24), 32 + dataBytes);  // WriteOffset
        int pos = 32;
        foreach (var e in entries) { e.CopyTo(file, pos); pos += e.Length; }
        File.WriteAllBytes(walPath, file);

        using (var pool = File.Create(walPath + ".pool"))
        {
            WritePoolRow(pool, 0, "Starting {App}");
            WritePoolRow(pool, 1, "Stopping {App}");
        }

        // The format as recovery parses it first, then the replay the engine does with it.
        var (segId, parsed) = WriteAheadLog.ReadForRecovery(walPath);
        Assert.Equal(SegId, segId);
        Assert.Equal(2, parsed.Count);
        Assert.All(parsed, e => Assert.False(e.Unpooled));
        Assert.Equal(new ushort[] { 0, 1 }, parsed.Select(e => e.TemplateIndex));

        _engine = NewEngine();
        await _engine.CatalogLoaded;

        var events = ReadAllByTicks();
        Assert.Equal(2, events.Count);
        Assert.Equal("Starting {App}", events[t].Template);
        Assert.Equal((byte)LogLevel.Information, events[t].Level);
        Assert.Equal("Stopping {App}", events[t + TimeSpan.TicksPerSecond].Template);
        Assert.Equal((byte)LogLevel.Warning, events[t + TimeSpan.TicksPerSecond].Level);
    }

    /// <summary>
    /// payloadLen u32 | ticks i64 | level u8 | byte 13 (never written, so 0) | templateIndex u16 |
    /// exceptionLen u32 | crc32c u32 over bytes [0, 20) + payload.
    /// </summary>
    private static byte[] PreviousBuildEntry(long ticks, LogLevel level, ushort templateIndex, byte[] payload)
    {
        var e = new byte[24 + payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(0),  (uint)payload.Length);
        BinaryPrimitives.WriteInt64LittleEndian (e.AsSpan(4),  ticks);
        e[12] = (byte)level;
        e[13] = 0;
        BinaryPrimitives.WriteUInt16LittleEndian(e.AsSpan(14), templateIndex);
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(16), 0);
        uint crc = Crc32c.Append(0, e.AsSpan(0, 20));
        crc      = Crc32c.Append(crc, payload);
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(20), crc);
        payload.CopyTo(e, 24);
        return e;
    }

    private static void WritePoolRow(Stream s, ushort index, string text)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(text);
        Span<byte> hdr = stackalloc byte[4];
        BinaryPrimitives.WriteUInt16LittleEndian(hdr, index);
        BinaryPrimitives.WriteUInt16LittleEndian(hdr[2..], (ushort)bytes.Length);
        s.Write(hdr);
        s.Write(bytes);
    }
}
