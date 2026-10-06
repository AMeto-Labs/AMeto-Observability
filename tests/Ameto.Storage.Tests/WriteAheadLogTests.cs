using System.Buffers.Binary;
using System.Text;
using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// WAL append → recovery round-trip: the exception path (exception bytes are serialised through a
/// thread-reused scratch buffer, so consecutive appends with different exceptions must not bleed
/// into each other, and exception-free entries in between must stay exception-free), the service
/// a v5 entry carries, the checksum's stop at a torn or rotted entry, and the on-disk layout of
/// every format recovery still reads, pinned byte by byte.
/// </summary>
public sealed class WriteAheadLogTests
{
    // The documented layout (WriteAheadLog's class doc). Spelled out here, not taken from the
    // class, so a change to the format has to change these lines too.
    private const int FileHeader    = 32;
    private const int EntryHeader   = 26;   // v5
    private const int EntryHeaderV4 = 24;
    private const int EntryHeaderV3 = 20;
    private const int WriteOffsetAt = 24;   // the file header's WriteOffset field

    private const string NonAscii     = "Платежи.Шлюз 💳";
    private static readonly string Long = "Svc." + new string('q', 3_000);
    [Fact]
    public void Append_WithExceptions_RoundTripsThroughRecovery()
    {
        string path = Path.Combine(Path.GetTempPath(), $"wal-{Guid.NewGuid():N}.wal");
        var payloadA = new byte[] { 1, 2, 3 };
        var payloadB = new byte[] { 9, 8 };

        var excA = new ExceptionInfo
        {
            Type       = "System.InvalidOperationException",
            Message    = "first failure",
            StackTrace = "at A.B()",
            Inner      = new ExceptionInfo { Type = "System.IO.IOException", Message = "disk" },
        };
        var excB = new ExceptionInfo { Type = "System.TimeoutException", Message = "second — different and longer than the first one" };

        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(0), new SegmentId(1UL), initialCapacity: 1024 * 1024))
            {
                wal.Append(100, Ameto.Core.LogLevel.Error,       0, "tmpl-a", payloadA, excA);
                wal.Append(200, Ameto.Core.LogLevel.Information, 1, "tmpl-b", payloadB, exception: null);
                wal.Append(300, Ameto.Core.LogLevel.Fatal,       2, "tmpl-c", ReadOnlySpan<byte>.Empty, excB);
            }

            var (segId, entries) = WriteAheadLog.ReadForRecovery(path);

            Assert.Equal(1UL, segId);
            Assert.Equal(3, entries.Count);

            Assert.Equal(100, entries[0].TimestampTicks);
            Assert.Equal(payloadA, entries[0].Payload);
            Assert.NotNull(entries[0].Exception);
            Assert.Equal(excA.Type,          entries[0].Exception!.Type);
            Assert.Equal(excA.Message,       entries[0].Exception!.Message);
            Assert.Equal(excA.StackTrace,    entries[0].Exception!.StackTrace);
            Assert.Equal(excA.Inner!.Type,   entries[0].Exception!.Inner?.Type);

            Assert.Equal(payloadB, entries[1].Payload);
            Assert.Null(entries[1].Exception);          // scratch reuse must not leak excA here

            Assert.Empty(entries[2].Payload);
            Assert.Equal(excB.Type,    entries[2].Exception?.Type);
            Assert.Equal(excB.Message, entries[2].Exception?.Message);

            Assert.All(entries, e => Assert.Equal(-1, e.ServiceIndex));   // none was given
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".pool");
        }
    }

    /// <summary>
    /// The service rides in the entry by pool index, its text in the pool file once per index:
    /// index 0 (so "has a service" is a flag, not a nonzero index) and the last pool id, non-ASCII
    /// and long text, services changing from entry to entry, and the three ways to have none —
    /// -1, an index past the pool's 65 536 ids, and a template-only event.
    /// </summary>
    [Fact]
    public void Append_WithServices_RoundTripsThroughRecovery()
    {
        string path = NewWalPath();
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(0), new SegmentId(1UL), initialCapacity: 1024 * 1024))
            {
                wal.Append(100, LogLevel.Information, 7, "tmpl", [1], serviceIndex: 0,      service: "Orders.Api");
                wal.Append(200, LogLevel.Information, 7, "tmpl", [2], serviceIndex: 1,      service: NonAscii);
                wal.Append(300, LogLevel.Information, 7, "tmpl", [3], serviceIndex: -1,     service: null);
                wal.Append(400, LogLevel.Information, 7, "tmpl", [4], serviceIndex: 0,      service: "Orders.Api");
                wal.Append(500, LogLevel.Information, 7, "tmpl", [5], serviceIndex: 2,      service: Long);
                wal.Append(600, LogLevel.Information, 7, "tmpl", [6], serviceIndex: 65_535, service: "Last.Id");
                wal.Append(700, LogLevel.Information, 7, "tmpl", [7], serviceIndex: 65_536, service: "Past.The.Cap");
                wal.Append(800, LogLevel.Information, 7, "tmpl", [8]);
            }

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal([0, 1, -1, 0, 2, 65_535, -1, -1], entries.Select(e => e.ServiceIndex));
            Assert.All(entries, e => Assert.Equal(7, e.TemplateIndex));

            // One row per index, whichever of template or service it was written for, and the text
            // exact — UTF-8 both ways, no truncation at 3 000 bytes. Nothing for the index past the cap.
            var pool = WriteAheadLog.LoadPool(path + ".pool");
            Assert.Equal(
                [(0, "Orders.Api"), (1, NonAscii), (2, Long), (7, "tmpl"), (65_535, "Last.Id")],
                pool.OrderBy(kv => kv.Key).Select(kv => ((int)kv.Key, kv.Value)));
            Assert.Equal(5, CountPoolRows(path + ".pool"));
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// An index logged without its text — the intern pool answers <see cref="string.Empty"/> for a
    /// slot whose claim another thread has not finished storing — still flags the service and
    /// writes no row. The next entry that brings the text writes the row, and it serves both.
    /// </summary>
    [Fact]
    public void A_service_logged_before_its_text_is_known_is_resolved_by_a_later_row()
    {
        string path = NewWalPath();
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(0), new SegmentId(1UL), initialCapacity: 1024 * 1024))
            {
                wal.Append(100, LogLevel.Information, 0, "tmpl", [1], serviceIndex: 4, service: "");
                Assert.DoesNotContain(4, WriteAheadLog.LoadPool(path + ".pool").Keys.Select(k => (int)k));
                wal.Append(200, LogLevel.Information, 0, "tmpl", [2], serviceIndex: 4, service: "Late.Text");
            }

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            var pool = WriteAheadLog.LoadPool(path + ".pool");
            Assert.Equal([4, 4], entries.Select(e => e.ServiceIndexIn(pool)));
            Assert.Equal("Late.Text", pool[4]);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// The clean stop's dispose: a log nothing was appended to goes, with its pool; a log with one
    /// entry stays, readable; an append after either throws as it does after a plain dispose; a
    /// second call does nothing.
    /// </summary>
    [Fact]
    public void DisposeDeletingIfEmpty_deletes_only_a_log_nothing_was_appended_to()
    {
        string empty = NewWalPath(), kept = NewWalPath();
        try
        {
            var a = WriteAheadLog.Open(empty, new NodeId(0), new SegmentId(1UL), initialCapacity: 64 * 1024);
            Assert.True(a.DisposeDeletingIfEmpty());
            Assert.False(File.Exists(empty));
            Assert.False(File.Exists(empty + ".pool"));
            Assert.Throws<ObjectDisposedException>(() => a.Append(1, LogLevel.Information, 0, "tmpl", [1]));
            Assert.False(a.DisposeDeletingIfEmpty());

            var b = WriteAheadLog.Open(kept, new NodeId(0), new SegmentId(1UL), initialCapacity: 64 * 1024);
            b.Append(100, LogLevel.Information, 0, "tmpl", [1], serviceIndex: 1, service: "Svc.A");
            Assert.False(b.DisposeDeletingIfEmpty());
            Assert.True(File.Exists(kept));
            Assert.Throws<ObjectDisposedException>(() => b.Append(2, LogLevel.Information, 0, "tmpl", [2]));

            var (_, entries) = WriteAheadLog.ReadForRecovery(kept);
            Assert.Equal(1, Assert.Single(entries).ServiceIndex);
            Assert.Equal("Svc.A", WriteAheadLog.LoadPool(kept + ".pool")[1]);
        }
        finally
        {
            foreach (var p in new[] { empty, empty + ".pool", kept, kept + ".pool" })
                try { File.Delete(p); } catch { }
        }
    }

    /// <summary>
    /// An entry's service resolves only against its own WAL's pool rows: an index whose row did not
    /// survive is no service, whatever a live pool holds at that slot.
    /// </summary>
    [Fact]
    public void ServiceIndexIn_answers_the_index_only_when_the_wals_own_pool_has_its_row()
    {
        var rows = new Dictionary<ushort, string> { [3] = "Orders.Api" };
        Assert.Equal(3,  new WalEntry { ServiceIndex = 3  }.ServiceIndexIn(rows));
        Assert.Equal(-1, new WalEntry { ServiceIndex = 4  }.ServiceIndexIn(rows));
        Assert.Equal(-1, new WalEntry { ServiceIndex = -1 }.ServiceIndexIn(rows));
        Assert.Equal(-1, new WalEntry { ServiceIndex = 3  }.ServiceIndexIn(new Dictionary<ushort, string>()));
    }

    // ── The layouts on disk ───────────────────────────────────────────────────

    /// <summary>
    /// What Append writes is, byte for byte, the documented v5 layout: a 32-byte file header saying
    /// version 5, then per entry payloadLen u32 | ticks i64 | level u8 | flags u8 (bit 0 Unpooled,
    /// bit 1 Service) | templateIndex u16 | exceptionLen u32 | serviceIndex u16 | crc32c u32 over
    /// [0, 22) + payload + exception, then the payload.
    /// </summary>
    [Fact]
    public void Append_writes_the_documented_v5_layout()
    {
        string path = NewWalPath();
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024))
            {
                wal.Append(1_000, LogLevel.Warning,     5,  "tmpl", [0xA1, 0xA2], serviceIndex: 9, service: "Svc");
                wal.Append(2_000, LogLevel.Information, -1, "unpooled text", [0xB1]);
            }

            byte[] file = File.ReadAllBytes(path);
            Assert.Equal(0x52_44_57_41u, BinaryPrimitives.ReadUInt32LittleEndian(file));       // "RDWA"
            Assert.Equal(5, BinaryPrimitives.ReadUInt16LittleEndian(file.AsSpan(4)));           // v5
            Assert.Equal(42UL, BinaryPrimitives.ReadUInt64LittleEndian(file.AsSpan(16)));

            byte[] first  = V5Entry(1_000, LogLevel.Warning,     flags: 0x02, templateIndex: 5, serviceIndex: 9, [0xA1, 0xA2]);
            byte[] second = V5Entry(2_000, LogLevel.Information, flags: 0x01, templateIndex: 0, serviceIndex: 0, [0xB1]);
            Assert.Equal(first,  file.AsSpan(FileHeader, first.Length).ToArray());
            Assert.Equal(second, file.AsSpan(FileHeader + first.Length, second.Length).ToArray());
            Assert.Equal(FileHeader + first.Length + second.Length,
                         BinaryPrimitives.ReadInt64LittleEndian(file.AsSpan(WriteOffsetAt)));
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// A v4 file — what every release before v5 wrote, and what a node upgraded mid-crash leaves —
    /// still replays: checksummed at [20, 24), no service whatever its flag byte says (bits past 0
    /// were reserved in v4), and still cut at its first entry that does not verify.
    /// </summary>
    [Fact]
    public void A_v4_file_replays_without_services_and_stops_at_its_first_bad_checksum()
    {
        string path = NewWalPath();
        try
        {
            byte[] a = V4Entry(100, flags: 0x00, templateIndex: 1, [1, 2, 3]);
            byte[] b = V4Entry(200, flags: 0x03, templateIndex: 2, [4, 5]);      // Unpooled + a bit v4 never had
            byte[] c = V4Entry(300, flags: 0x00, templateIndex: 3, [6]);
            WriteFile(path, version: 4, a, b, c);

            var (segId, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal(1UL, segId);
            Assert.Equal([100L, 200L, 300L], entries.Select(e => e.TimestampTicks));
            Assert.Equal([false, true, false], entries.Select(e => e.Unpooled));
            Assert.All(entries, e => Assert.Equal(-1, e.ServiceIndex));
            Assert.Equal([4, 5], entries[1].Payload);

            // Rot in the second entry's payload: replay keeps the first and stops.
            Corrupt(path, FileHeader + a.Length + EntryHeaderV4);
            (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal(100L, Assert.Single(entries).TimestampTicks);
        }
        finally { File.Delete(path); }
    }

    /// <summary>A v3 file (20-byte header, no checksum, no flags) still replays as it always did.</summary>
    [Fact]
    public void A_v3_file_replays_without_services()
    {
        string path = NewWalPath();
        try
        {
            WriteFile(path, version: 3,
                V3Entry(100, templateIndex: 1, [1, 2, 3]),
                V3Entry(200, templateIndex: 2, [4]));

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal([100L, 200L], entries.Select(e => e.TimestampTicks));
            Assert.Equal([(ushort)1, (ushort)2], entries.Select(e => e.TemplateIndex));
            Assert.All(entries, e => Assert.False(e.Unpooled));
            Assert.All(entries, e => Assert.Equal(-1, e.ServiceIndex));
            Assert.Equal([4], entries[1].Payload);
        }
        finally { File.Delete(path); }
    }

    private static int CountPoolRows(string poolPath)
    {
        byte[] bytes = File.ReadAllBytes(poolPath);
        int rows = 0;
        for (int pos = 0; pos + 4 <= bytes.Length; rows++)
            pos += 4 + BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pos + 2));
        return rows;
    }

    private static byte[] V5Entry(long ticks, LogLevel level, byte flags, ushort templateIndex, ushort serviceIndex, byte[] payload)
    {
        var e = new byte[EntryHeader + payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(0),  (uint)payload.Length);
        BinaryPrimitives.WriteInt64LittleEndian (e.AsSpan(4),  ticks);
        e[12] = (byte)level;
        e[13] = flags;
        BinaryPrimitives.WriteUInt16LittleEndian(e.AsSpan(14), templateIndex);
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(16), 0);                  // no exception
        BinaryPrimitives.WriteUInt16LittleEndian(e.AsSpan(20), serviceIndex);
        uint crc = Crc32c.Append(0, e.AsSpan(0, 22));
        crc      = Crc32c.Append(crc, payload);
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(22), crc);
        payload.CopyTo(e, EntryHeader);
        return e;
    }

    private static byte[] V4Entry(long ticks, byte flags, ushort templateIndex, byte[] payload)
    {
        var e = new byte[EntryHeaderV4 + payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(0),  (uint)payload.Length);
        BinaryPrimitives.WriteInt64LittleEndian (e.AsSpan(4),  ticks);
        e[12] = (byte)LogLevel.Information;
        e[13] = flags;
        BinaryPrimitives.WriteUInt16LittleEndian(e.AsSpan(14), templateIndex);
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(16), 0);
        uint crc = Crc32c.Append(0, e.AsSpan(0, 20));
        crc      = Crc32c.Append(crc, payload);
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(20), crc);
        payload.CopyTo(e, EntryHeaderV4);
        return e;
    }

    private static byte[] V3Entry(long ticks, ushort templateIndex, byte[] payload)
    {
        var e = new byte[EntryHeaderV3 + payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(0),  (uint)payload.Length);
        BinaryPrimitives.WriteInt64LittleEndian (e.AsSpan(4),  ticks);
        e[12] = (byte)LogLevel.Information;
        BinaryPrimitives.WriteUInt16LittleEndian(e.AsSpan(14), templateIndex);
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(16), 0);
        payload.CopyTo(e, EntryHeaderV3);
        return e;
    }

    private static void WriteFile(string path, ushort version, params byte[][] entries)
    {
        var file = new byte[FileHeader + 64 * 1024];
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(0),  0x52_44_57_41);   // "RDWA"
        BinaryPrimitives.WriteUInt16LittleEndian(file.AsSpan(4),  version);
        BinaryPrimitives.WriteUInt64LittleEndian(file.AsSpan(16), 1UL);
        BinaryPrimitives.WriteInt64LittleEndian (file.AsSpan(WriteOffsetAt), FileHeader + entries.Sum(e => e.Length));
        int pos = FileHeader;
        foreach (var e in entries) { e.CopyTo(file, pos); pos += e.Length; }
        File.WriteAllBytes(path, file);
    }

    /// <summary>Flips every bit of the byte at <paramref name="offset"/>.</summary>
    private static void Corrupt(string path, long offset)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite);
        fs.Seek(offset, SeekOrigin.Begin);
        int b = fs.ReadByte();
        fs.Seek(-1, SeekOrigin.Current);
        fs.WriteByte((byte)(b ^ 0xFF));
    }

    /// <summary>
    /// An index outside the pool (-1, or past its 65 536 ids) is logged with the Unpooled flag
    /// and writes no pool row, and a genuine index 0 beside it is not flagged. The WAL-level
    /// half of WalUnpooledTemplateTests.
    /// </summary>
    [Fact]
    public void Append_OutsideThePool_IsFlaggedUnpooled_AndWritesNoPoolRow()
    {
        string path = NewWalPath();
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(0), new SegmentId(1UL), initialCapacity: 1024 * 1024))
            {
                wal.Append(100, Ameto.Core.LogLevel.Information, -1,     "unpooled text", new byte[] { 1 });
                wal.Append(200, Ameto.Core.LogLevel.Information, 0,      "Starting {App}", new byte[] { 2 });
                wal.Append(300, Ameto.Core.LogLevel.Information, 65_536, "past the cap",  new byte[] { 3 });
            }

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal(3, entries.Count);
            Assert.True(entries[0].Unpooled);
            Assert.False(entries[1].Unpooled);
            Assert.Equal(0, entries[1].TemplateIndex);
            Assert.True(entries[2].Unpooled);

            var pool = WriteAheadLog.LoadPool(path + ".pool");
            Assert.Equal("Starting {App}", Assert.Single(pool).Value);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    // ── Torn and rotted entries (v5) ──────────────────────────────────────────
    //
    // WriteThreeEntries writes payloads of 3, 2 and 4 bytes behind 26-byte headers, so the
    // entries sit at file offsets 32, 61 and 89 and the header's WriteOffset field (byte 24)
    // reads 89 + 30 = 119. Each names a service, so a tear or a flipped bit in the service
    // index is covered by the checksum exactly as one in the payload is.

    private const long Entry2 = FileHeader + EntryHeader + 3;                       // 61
    private const long LogEnd = Entry2 + (EntryHeader + 2) + (EntryHeader + 4);     // 119

    private static string NewWalPath() => Path.Combine(Path.GetTempPath(), $"wal-{Guid.NewGuid():N}.wal");

    private static void WriteThreeEntries(string path)
    {
        using var wal = WriteAheadLog.Open(path, new NodeId(0), new SegmentId(1UL), initialCapacity: 1024 * 1024);
        wal.Append(100, Ameto.Core.LogLevel.Error,       0, "tmpl-a", new byte[] { 1, 2, 3 },    serviceIndex: 10, service: "Svc.A");
        wal.Append(200, Ameto.Core.LogLevel.Information, 1, "tmpl-b", new byte[] { 9, 8 },       serviceIndex: 11, service: NonAscii);
        wal.Append(300, Ameto.Core.LogLevel.Warning,     2, "tmpl-c", new byte[] { 7, 6, 5, 4 }, serviceIndex: 10, service: "Svc.A");
    }

    [Fact]
    public void Recovery_ReadsEveryEntryOfAnUndamagedLog()
    {
        string path = NewWalPath();
        try
        {
            WriteThreeEntries(path);

            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read))
            {
                Span<byte> header = stackalloc byte[FileHeader];
                fs.ReadExactly(header);
                Assert.Equal(LogEnd, BinaryPrimitives.ReadInt64LittleEndian(header[WriteOffsetAt..]));   // the offsets above hold
            }

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal([100L, 200L, 300L], entries.Select(e => e.TimestampTicks));
            Assert.Equal([10, 11, 10], entries.Select(e => e.ServiceIndex));
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    [Fact]
    public void Recovery_StopsAtEntryWhoseChecksumFails()
    {
        string path = NewWalPath();
        try
        {
            WriteThreeEntries(path);

            // Flip one payload byte of entry 2 — a torn page in the middle of the log.
            Corrupt(path, Entry2 + EntryHeader + 1);

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Single(entries);                 // entry 1 survives, replay stops at the tear
            Assert.Equal(100, entries[0].TimestampTicks);
            Assert.Equal(10, entries[0].ServiceIndex);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// A bit gone bad in the service index (byte 20) or in the flag that says there is one
    /// (byte 13) fails the entry's checksum: replay stops there rather than handing an event
    /// another service, or none, while the entries before it keep theirs.
    /// </summary>
    [Theory]
    [InlineData(20)]   // serviceIndex, low byte
    [InlineData(21)]   // serviceIndex, high byte
    [InlineData(13)]   // flags
    public void Recovery_StopsAtEntryWhoseServiceRotted(int headerByte)
    {
        string path = NewWalPath();
        try
        {
            WriteThreeEntries(path);
            Corrupt(path, Entry2 + headerByte);

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            var entry = Assert.Single(entries);
            Assert.Equal(100, entry.TimestampTicks);
            Assert.Equal(10, entry.ServiceIndex);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    [Fact]
    public void Recovery_StopsAtZeroedTail()
    {
        string path = NewWalPath();
        try
        {
            WriteThreeEntries(path);

            // Zero everything after entry 1 while WriteOffset still covers it: the shape a
            // crash leaves when the header page reached disk but the payload pages did not
            // (the file is created sparse). Pre-checksum, a zero region parsed as an endless
            // run of "valid" zero-length entries at year 0001.
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite))
            {
                fs.Seek(Entry2, SeekOrigin.Begin);
                fs.Write(new byte[LogEnd - Entry2]);
            }

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Single(entries);
            Assert.Equal(100, entries[0].TimestampTicks);
            Assert.Equal(10, entries[0].ServiceIndex);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// The last entry torn at its very end — its header and most of its payload reached disk, its
    /// last bytes did not — is dropped whole; the two before it replay with their services.
    /// </summary>
    [Fact]
    public void Recovery_DropsATornLastEntry()
    {
        string path = NewWalPath();
        try
        {
            WriteThreeEntries(path);
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite))
            {
                fs.Seek(LogEnd - 2, SeekOrigin.Begin);
                fs.Write(new byte[2]);
            }

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal([100L, 200L], entries.Select(e => e.TimestampTicks));
            Assert.Equal([10, 11], entries.Select(e => e.ServiceIndex));
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    [Fact]
    public void Recovery_ClampsGarbageWriteOffset()
    {
        string path = NewWalPath();
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(0), new SegmentId(1UL), initialCapacity: 1024 * 1024))
                wal.Append(100, Ameto.Core.LogLevel.Error, 0, "tmpl-a", new byte[] { 1, 2, 3 });

            using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite))
            {
                // Sanity-check the field position before poisoning it.
                fs.Seek(WriteOffsetAt, SeekOrigin.Begin);
                Span<byte> cur = stackalloc byte[8];
                fs.ReadExactly(cur);
                Assert.Equal(FileHeader + EntryHeader + 3L, BitConverter.ToInt64(cur));

                fs.Seek(WriteOffsetAt, SeekOrigin.Begin);
                fs.Write(BitConverter.GetBytes(long.MaxValue - 100));
            }

            // Unclamped, the entry walk runs off the mapped view: an uncatchable
            // AccessViolation in the engine constructor, i.e. a crash-looping node.
            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Single(entries);
            Assert.Equal(100, entries[0].TimestampTicks);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    [Fact]
    public void Reopen_WithGarbageWriteOffset_ResetsAndAppends()
    {
        string path = NewWalPath();
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(0), new SegmentId(1UL), initialCapacity: 1024 * 1024))
                wal.Append(100, Ameto.Core.LogLevel.Error, 0, "tmpl-a", new byte[] { 1, 2, 3 });

            using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite))
            {
                fs.Seek(24, SeekOrigin.Begin);
                fs.Write(BitConverter.GetBytes(long.MaxValue - 100));
            }

            // The live open path shares the clamp: a garbage offset resets the log instead
            // of letting the first Append write past the mapping.
            using (var wal = WriteAheadLog.Open(path, new NodeId(0), new SegmentId(1UL), initialCapacity: 1024 * 1024))
                wal.Append(500, Ameto.Core.LogLevel.Information, 3, "tmpl-x", new byte[] { 42 });

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Single(entries);
            Assert.Equal(500, entries[0].TimestampTicks);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    [Fact]
    public void Flush_BetweenAppends_KeepsAllEntriesRecoverable()
    {
        string path = NewWalPath();
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(0), new SegmentId(1UL), initialCapacity: 1024 * 1024))
            {
                wal.Append(100, Ameto.Core.LogLevel.Error, 0, "tmpl-a", new byte[] { 1 });
                wal.Flush(); // the periodic msync the engine drives
                wal.Append(200, Ameto.Core.LogLevel.Error, 0, "tmpl-a", new byte[] { 2 });
            }

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal(2, entries.Count);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }
}
