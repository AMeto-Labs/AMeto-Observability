using System.Buffers.Binary;
using System.Text;
using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// WAL append → recovery round-trip: the exception path (exception bytes are serialised through a
/// thread-reused scratch buffer, so consecutive appends with different exceptions must not bleed
/// into each other, and exception-free entries in between must stay exception-free), the service
/// a v5 entry carries, the trace context a v6 entry carries, the checksum's stop at a torn or rotted
/// entry, and the on-disk layout of every format recovery still reads, pinned byte by byte.
/// </summary>
public sealed class WriteAheadLogTests
{
    // The documented layout (WriteAheadLog's class doc). Spelled out here, not taken from the
    // class, so a change to the format has to change these lines too.
    private const int FileHeader    = 32;
    private const int EntryHeader   = 26;   // v5 and v6
    private const int TraceContext  = 24;   // v6, with the Trace flag
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
    /// What Append writes is, byte for byte, the documented v6 layout: a 32-byte file header saying
    /// version 6, then per entry payloadLen u32 | ticks i64 | level u8 | flags u8 (bit 0 Unpooled,
    /// bit 1 Service, bit 2 Trace, bit 3 InlineTemplate) | templateIndex u16 | exceptionLen u32 |
    /// serviceIndex u16 | crc32c u32 over [0, 22) + trace context + inline template + payload +
    /// exception; with the Trace flag the trace context, traceIdHi u64 | traceIdLo u64 | spanId
    /// u64; with the InlineTemplate flag the template's byte length u32 and its UTF-8 text; then
    /// the payload. An entry with neither is a v5 entry byte for byte.
    /// </summary>
    [Fact]
    public void Append_writes_the_documented_v6_layout()
    {
        string path = NewWalPath();
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024))
            {
                wal.Append(1_000, LogLevel.Warning,     5,  "tmpl", [0xA1, 0xA2], serviceIndex: 9, service: "Svc");
                wal.Append(2_000, LogLevel.Information, -1, "unpooled text", [0xB1]);
                wal.Append(3_000, LogLevel.Error,       5,  "tmpl", [0xC1], serviceIndex: 9, service: "Svc",
                           traceIdHi: 0x0102030405060708UL, traceIdLo: 0x1112131415161718UL, spanId: 0x2122232425262728UL);
                wal.Append(4_000, LogLevel.Debug,       5,  "tmpl", [0xD1], spanId: 7);   // a span id alone is context too
            }

            byte[] file = File.ReadAllBytes(path);
            Assert.Equal(0x52_44_57_41u, BinaryPrimitives.ReadUInt32LittleEndian(file));       // "RDWA"
            Assert.Equal(6, BinaryPrimitives.ReadUInt16LittleEndian(file.AsSpan(4)));           // v6
            Assert.Equal(42UL, BinaryPrimitives.ReadUInt64LittleEndian(file.AsSpan(16)));

            byte[][] expected =
            [
                V5Entry(1_000, LogLevel.Warning,     flags: 0x02, templateIndex: 5, serviceIndex: 9, [0xA1, 0xA2]),
                V6InlineEntry(2_000, LogLevel.Information, flags: 0x09, "unpooled text", [0xB1]),
                V6Entry(3_000, LogLevel.Error, flags: 0x06, templateIndex: 5, serviceIndex: 9,
                        0x0102030405060708UL, 0x1112131415161718UL, 0x2122232425262728UL, [0xC1]),
                V6Entry(4_000, LogLevel.Debug, flags: 0x04, templateIndex: 5, serviceIndex: 0, 0, 0, 7, [0xD1]),
            ];
            int pos = FileHeader;
            foreach (var e in expected)
            {
                Assert.Equal(e, file.AsSpan(pos, e.Length).ToArray());
                pos += e.Length;
            }
            Assert.Equal(pos, BinaryPrimitives.ReadInt64LittleEndian(file.AsSpan(WriteOffsetAt)));

            // And it reads back: the context where there was one, zeros where there was none.
            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal([0UL, 0UL, 0x0102030405060708UL, 0UL], entries.Select(e => e.TraceIdHi));
            Assert.Equal([0UL, 0UL, 0x1112131415161718UL, 0UL], entries.Select(e => e.TraceIdLo));
            Assert.Equal([0UL, 0UL, 0x2122232425262728UL, 7UL], entries.Select(e => e.SpanId));
            Assert.Equal([0xC1], entries[2].Payload);
            Assert.Equal(9, entries[2].ServiceIndex);
            // The unpooled template's text, from the entry itself; none for the pooled ones.
            Assert.Equal([null, "unpooled text", null, null], entries.Select(e => e.Template));
            Assert.Equal([0xB1], entries[1].Payload);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// A template no pool row can hold whole — a row's length is 16 bits — travels in its entry, and
    /// comes back whole: a row used to keep it cut at 65 535 bytes (or at a U+FFFD, the cut falling
    /// inside a character). No row is written for it, so its index cannot resolve to the cut text.
    /// </summary>
    [Fact]
    public void A_template_longer_than_a_pool_row_comes_back_whole()
    {
        string path = NewWalPath();
        string longTemplate = new string('é', 40_000) + " {n}";   // 80 004 UTF-8 bytes, 40 004 UTF-16 units
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024))
            {
                wal.Append(1_000, LogLevel.Information, 7, longTemplate, [0xA1]);
                wal.Append(2_000, LogLevel.Information, 8, "short {n}",  [0xB1]);
            }

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal(2, entries.Count);
            Assert.True(entries[0].Unpooled);
            Assert.Equal(longTemplate, entries[0].Template);
            Assert.False(entries[1].Unpooled);
            Assert.Null(entries[1].Template);
            Assert.Equal("short {n}", Assert.Single(WriteAheadLog.LoadPool(path + ".pool")).Value);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// R5-3. Templates and services share the pool, so one text can be both. A pooled SERVICE too long
    /// for a row was written to a row cut at 65 535 bytes: the service replayed cut, and when the same
    /// text came next as a template, its index already had a row — so its entry was taken for carried
    /// by it, and the template replayed cut too (or with no text). A text no row can hold is now carried
    /// in the entry whichever it is used as, and gets no row.
    /// </summary>
    [Fact]
    public void A_text_too_long_for_a_row_used_as_a_service_and_then_as_a_template_comes_back_whole_as_both()
    {
        string path     = NewWalPath();
        string longText = new string('é', 40_000) + " {n}";   // 80 004 UTF-8 bytes
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024))
            {
                wal.Append(1_000, LogLevel.Information, 7, "short {n}", [0xA1], serviceIndex: 9, service: longText);
                wal.Append(2_000, LogLevel.Information, 9, longText,    [0xB1]);
                wal.Append(3_000, LogLevel.Information, 9, longText,    [0xC1], serviceIndex: 9, service: longText);
            }

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal(3, entries.Count);
            Assert.Equal(longText, entries[0].Service);
            Assert.Equal(longText, entries[1].Template);
            Assert.Equal(longText, entries[2].Template);
            Assert.Equal(longText, entries[2].Service);
            Assert.DoesNotContain((ushort)9, WriteAheadLog.LoadPool(path + ".pool").Keys);   // no row cut short
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// A log that cannot be opened — here its pre-size is more than any disk has — leaves nothing
    /// behind. It used to leave a 0-byte file, its FileStream open until finalisation: on a full
    /// disk, one per attempt to open a spill file or the next live WAL.
    /// </summary>
    [Fact]
    public void A_log_that_cannot_be_opened_leaves_no_file_behind()
    {
        string path = NewWalPath();
        try
        {
            Assert.ThrowsAny<Exception>(() => WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 1L << 50));
            Assert.False(File.Exists(path), "the log's file was left behind");
            Assert.False(File.Exists(path + ".pool"), "the log's pool file was left behind");
        }
        finally { try { File.Delete(path); File.Delete(path + ".pool"); } catch { /* the assertion says what is wrong */ } }
    }

    /// <summary>
    /// Round-5 review H-F. On Linux a log is sized with ftruncate, which leaves the file sparse: its blocks
    /// were taken by the first write into each page, through the mapping, and on a volume that had filled
    /// meanwhile that write was SIGBUS — the process killed, where NTFS, which allocates on SetLength, fails
    /// the open or the grow with an IOException the engine answers 503 with. The blocks are reserved where
    /// the size is set now, at the open and at every grow: the file has them before anything is written.
    /// </summary>
    [Fact]
    public void On_linux_a_log_has_its_blocks_from_its_open_and_from_each_grow()
    {
        if (!OperatingSystem.IsLinux()) return;   // NTFS allocates on SetLength; this is Linux's road
        string path    = NewWalPath();
        byte[] payload = new byte[600 * 1024];
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024))
            {
                long sized = new FileInfo(path).Length;
                Assert.True(FileAllocation.AllocatedBytes(path) >= sized,
                    $"the log is sparse from its open: {FileAllocation.AllocatedBytes(path):N0} of {sized:N0} bytes on disk");

                // The second entry does not fit: the log doubles to 2 MB, and its upper half is written by nothing
                // yet — the pages a later append would have met unallocated.
                Assert.True(wal.Append(1_000, LogLevel.Information, 1, "t", payload));
                Assert.True(wal.Append(2_000, LogLevel.Information, 1, "t", payload));
                long grown = new FileInfo(path).Length;
                Assert.True(grown > sized, "setup: the log did not grow");
                Assert.True(FileAllocation.AllocatedBytes(path) >= grown,
                    $"the log is sparse past its grow: {FileAllocation.AllocatedBytes(path):N0} of {grown:N0} bytes on disk");
            }
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// An entry larger than twice the log's capacity grows the log to fit it. Grow doubled once and
    /// assumed that was enough, so such an entry was copied past the end of the mapping — reachable
    /// in a spill log, which can be as small as 1 MB and takes its first entry whatever its size.
    /// (Not run against the old code: it writes past the mapping, which takes the test host down.)
    /// </summary>
    [Fact]
    public void An_entry_larger_than_twice_the_capacity_grows_the_log_to_fit()
    {
        string path    = NewWalPath();
        byte[] payload = new byte[3 * 1024 * 1024];
        Random.Shared.NextBytes(payload);
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024, spill: true))
            {
                Assert.True(wal.Append(1_000, LogLevel.Information, 1, "t", payload));
                Assert.False(wal.Append(2_000, LogLevel.Information, 1, "t", payload));   // a spill log grows for its first entry only
            }

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal(payload, Assert.Single(entries).Payload);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// A POOLED template longer than a pool row is written once per log, into the first entry that
    /// uses its index, and every later entry of that index gets it back — the same string. It used to
    /// be carried by every entry: 1 000 events of one 70 KB template made a 70 MB log, and a replay
    /// that built a 70 KB string per event (~17 GiB for a full tier of them), ran out of memory, and
    /// left the log unreplayed at every start.
    /// </summary>
    [Fact]
    public void A_long_pooled_template_is_written_once_per_log_and_every_entry_of_its_index_gets_it_back()
    {
        string path         = NewWalPath();
        string longTemplate = new string('x', 70_000) + " {n}";   // 70 004 bytes: more than a row holds
        try
        {
            long written;
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024))
            {
                for (int i = 0; i < 1_000; i++)
                    wal.Append(1_000 + i, LogLevel.Information, 7, longTemplate, [(byte)i]);
                wal.Append(5_000, LogLevel.Information, 8, "short {n}", [0xB1]);
                wal.Append(6_000, LogLevel.Information, -1, "unpooled {n}", [0xC1]);
                written = wal.WrittenBytes;
            }

            Assert.True(written < 2 * longTemplate.Length, $"{written:N0} B logged for 1 000 events of one template");

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal(1_002, entries.Count);
            for (int i = 0; i < 1_000; i++)
            {
                Assert.True(entries[i].Unpooled);                          // no row to resolve: the text is attached
                Assert.Same(entries[0].Template, entries[i].Template);     // one string, not one per entry
                Assert.Equal((byte)i, Assert.Single(entries[i].Payload));
            }
            Assert.Equal(longTemplate, entries[0].Template);
            Assert.False(entries[1_000].Unpooled);
            Assert.Null(entries[1_000].Template);
            Assert.Equal("unpooled {n}", entries[1_001].Template);
            Assert.Equal("short {n}", Assert.Single(WriteAheadLog.LoadPool(path + ".pool")).Value);   // no row for the long one
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// Round-4 H2. A POOLED service longer than a pool row is written once per log, as a long pooled
    /// template is: by the first entry that uses its index, and every later entry of that index gets it
    /// back — the same string, and by the index of a template too, the pool being one. It was carried by
    /// every entry: 1 000 events of one 70 KB service made a 70 MB log (the review's probe), and 3 500 of
    /// a 65 600-character one a log whose replay ran a 384 MiB heap out at every start.
    /// </summary>
    [Fact]
    public void A_long_pooled_service_is_written_once_per_log_and_every_entry_of_its_index_gets_it_back()
    {
        string path        = NewWalPath();
        string longService = "Svc." + new string('s', 70_000);   // 70 004 bytes: more than a row holds
        try
        {
            long written;
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024))
            {
                for (int i = 0; i < 1_000; i++)
                    wal.Append(1_000 + i, LogLevel.Information, 7, "short {n}", [(byte)i], serviceIndex: 9, service: longService);
                wal.Append(5_000, LogLevel.Information, 9, longService, [0xB1]);                               // its index as a template
                wal.Append(6_000, LogLevel.Information, 7, "short {n}", [0xC1], serviceIndex: 8, service: "svc.short");
                written = wal.WrittenBytes;
            }

            Assert.True(written < 2 * longService.Length, $"{written:N0} B logged for 1 000 events of one service");

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal(1_002, entries.Count);
            Assert.Equal(longService, entries[0].Service);
            for (int i = 0; i < 1_000; i++)
            {
                Assert.Same(entries[0].Service, entries[i].Service);   // one string, not one per entry
                Assert.Equal(-1, entries[i].ServiceIndex);              // no row to resolve: the text is attached
                Assert.Equal((byte)i, Assert.Single(entries[i].Payload));
            }
            Assert.True(entries[1_000].Unpooled);
            Assert.Same(entries[0].Service, entries[1_000].Template);
            Assert.Equal(8, entries[1_001].ServiceIndex);
            Assert.Null(entries[1_001].Service);

            var rows = WriteAheadLog.LoadPool(path + ".pool");
            Assert.DoesNotContain((ushort)9, rows.Keys);                // no row cut short
            Assert.Equal("svc.short", rows[8]);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// Round-4 H2. A walk of a log makes ONE string per distinct inline text. A template or service
    /// outside the pool is in every entry that uses it, and the walk built a string per entry, which
    /// the tier a replay fills held per event: a log of one 64 KB unpooled service in every entry held
    /// a copy per event. Distinct texts stay distinct, and a text met again after others is still the
    /// first string.
    /// </summary>
    [Fact]
    public void A_walk_makes_one_string_per_distinct_inline_text()
    {
        string path    = NewWalPath();
        string serviceA = "A." + new string('a', 64 * 1024);
        string serviceB = "B." + new string('a', 64 * 1024);      // as long as A: only the bytes tell them apart
        string template = "unpooled " + new string('t', 64 * 1024) + " {n}";
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024))
            {
                for (int i = 0; i < 200; i++)
                    wal.Append(1_000 + i, LogLevel.Information, -1, template, [(byte)i], serviceIndex: -1, service: i % 3 == 2 ? serviceB : serviceA);
            }

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal(200, entries.Count);
            var services  = new HashSet<string>(ReferenceEqualityComparer.Instance);
            var templates = new HashSet<string>(ReferenceEqualityComparer.Instance);
            for (int i = 0; i < 200; i++)
            {
                Assert.Equal(i % 3 == 2 ? serviceB : serviceA, entries[i].Service);
                Assert.Equal(template, entries[i].Template);
                services.Add(entries[i].Service!);
                templates.Add(entries[i].Template!);
            }
            Assert.Equal(2, services.Count);
            Assert.Single(templates);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// Round-4 L15. A pool file whose tail a power loss zero-filled — NTFS can keep a file's size and
    /// lose its data — ends its rows at the first row of length 0: no row is written empty. Read as
    /// rows, the zeros were (0, "") over and over, and the first of them overwrote row 0, ordinarily the
    /// log's first template, which its events then replayed without.
    /// </summary>
    [Fact]
    public void A_pool_files_zero_filled_tail_ends_its_rows_and_leaves_row_0_alone()
    {
        string path = NewWalPath();
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 64 * 1024))
            {
                wal.Append(1_000, LogLevel.Information, 0, "first {n}",  [1]);
                wal.Append(2_000, LogLevel.Information, 1, "second {n}", [2]);
            }
            using (var pool = new FileStream(path + ".pool", FileMode.Append))
                pool.Write(new byte[64]);   // the zero-filled tail

            var rows = WriteAheadLog.LoadPool(path + ".pool");
            Assert.Equal(2, rows.Count);
            Assert.Equal("first {n}",  rows[0]);
            Assert.Equal("second {n}", rows[1]);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// Round-5 review L-1 (L15's residual). A zero-fill that starts inside a row's TEXT, the row's 4-byte
    /// header intact, read as that text plus NULs, and the replay stamped it on every event of the index:
    /// a service "Orders" and thirteen NULs. A row whose text ends in zeros with only zeros after it in
    /// the file is the torn tail now.
    /// </summary>
    [Fact]
    public void A_zero_fill_from_inside_a_rows_text_ends_the_rows_there()
    {
        string path = NewWalPath();
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 64 * 1024))
            {
                wal.Append(1_000, LogLevel.Information, 0, "first {n}", [1]);
                wal.Append(2_000, LogLevel.Information, 1, "Orders.Payments.Api", [2]);
            }
            byte[] pool = File.ReadAllBytes(path + ".pool");
            int    from = 4 + "first {n}".Length + 4 + 6;   // six bytes into the last row's text
            Array.Clear(pool, from, pool.Length - from);     // the power loss: the size kept, the data not
            File.WriteAllBytes(path + ".pool", pool);

            var rows = WriteAheadLog.LoadPool(path + ".pool");
            Assert.Equal("first {n}", Assert.Single(rows).Value);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// Round-5 review L-17. The append encodes an event's exception into a per-thread scratch buffer, which
    /// kept the largest exception its thread ever wrote — megabytes on each writer thread, for the life of
    /// the thread. Grown past a megabyte, it is let go once the entry is written; the entry is whole.
    /// </summary>
    [Fact]
    public void A_writer_thread_keeps_no_exception_scratch_past_a_megabyte()
    {
        string path  = NewWalPath();
        var    stack = new string('s', 4_000_000);
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024))
            {
                wal.Append(1_000, LogLevel.Error, 0, "failed {n}", [1],
                           new ExceptionInfo { Type = "System.Exception", Message = "boom", StackTrace = stack });

                var scratch = typeof(WriteAheadLog).GetField("_tExc", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
                                                   .GetValue(null) as System.Buffers.ArrayBufferWriter<byte>;
                Assert.True((scratch?.Capacity ?? 0) <= 1 << 20, $"the thread kept {scratch?.Capacity:N0} B of exception scratch");
            }

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal(stack, Assert.Single(entries).Exception!.StackTrace);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>Round-5 review L-1, the other side: a text that ends in a NUL of its own, with rows after it, is a row like any other.</summary>
    [Fact]
    public void A_pool_row_that_ends_in_a_nul_with_rows_after_it_is_kept()
    {
        string path = NewWalPath();
        try
        {
            // Written by hand: from v6 on the log writes no such row (R5-M5), and this pins the reader.
            byte[] a = Encoding.UTF8.GetBytes("ends in a NUL\0"), b = Encoding.UTF8.GetBytes("second {n}");
            var pool = new byte[4 + a.Length + 4 + b.Length];
            BinaryPrimitives.WriteUInt16LittleEndian(pool.AsSpan(0), 0);
            BinaryPrimitives.WriteUInt16LittleEndian(pool.AsSpan(2), (ushort)a.Length);
            a.CopyTo(pool, 4);
            BinaryPrimitives.WriteUInt16LittleEndian(pool.AsSpan(4 + a.Length), 1);
            BinaryPrimitives.WriteUInt16LittleEndian(pool.AsSpan(6 + a.Length), (ushort)b.Length);
            b.CopyTo(pool, 8 + a.Length);
            File.WriteAllBytes(path + ".pool", pool);

            var rows = WriteAheadLog.LoadPool(path + ".pool");
            Assert.Equal(2, rows.Count);
            Assert.Equal("ends in a NUL\0", rows[0]);
            Assert.Equal("second {n}", rows[1]);
        }
        finally { File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// Round-6 review R5-M5. A pool row whose text ends in a zero byte with only zeros after it reads as the
    /// tail a power loss zero-filled (round-5 L-1) — and that is exactly what an intact pool looks like when
    /// its NEWEST row is a client's NUL-ending service or template: every despill, and every replay after a
    /// kill, dropped that row, and the events came back without their service. Such a text is never a row
    /// now: the first entry of the log that uses its index carries it, as a text too long for a row is, and
    /// recovery gives every entry of the index that text.
    /// </summary>
    [Fact]
    public void A_newest_text_that_ends_in_a_nul_comes_back_whole()
    {
        string path = NewWalPath();
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 64 * 1024))
                for (int i = 0; i < 10; i++)
                    Assert.True(wal.Append(1_000 + i, LogLevel.Information, 40, "event {n}", [1], serviceIndex: 41, service: "row.svc\0"));

            // What the replay and the despill do: the entry's own text, else its index's row.
            var rows    = WriteAheadLog.LoadPool(path + ".pool");
            var entries = WriteAheadLog.ReadForRecovery(path).Entries;
            Assert.Equal(10, entries.Count);
            var services = entries.ConvertAll(e => e.Service ?? (e.ServiceIndex >= 0 && rows.TryGetValue((ushort)e.ServiceIndex, out var s) ? s : null));
            int lost = services.Count(s => s != "row.svc\0");
            Assert.True(lost == 0, $"{lost} of 10 entries came back without their service \"row.svc\\0\", the pool's newest text: rows {string.Join(", ", rows.Keys)}");
            Assert.Equal("event {n}", rows[40]);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// Round-4 L13, the poison pill. An entry that verifies — its bytes are the ones written — but whose
    /// exception the decoder refuses is read WITHOUT its exception, the rest of it whole, and the record
    /// says why; the walk goes on to the entries after it. The decode's failure propagated, and failed the
    /// walk at every reading: the log replayed at no start and was despilled by no retry, for good. Here
    /// the exception's map claims fifteen fields and holds two, and the checksum is recomputed over that.
    /// </summary>
    [Fact]
    public void An_entry_whose_exception_cannot_be_decoded_is_read_without_it_and_the_walk_goes_on()
    {
        string path = NewWalPath();
        var    exc  = new ExceptionInfo { Type = "System.InvalidOperationException", Message = "the middle event's exception" };
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 64 * 1024))
            {
                wal.Append(1_000, LogLevel.Information, 1, "event {n}",  [0xA1]);
                wal.Append(2_000, LogLevel.Error,       2, "failed {n}", [0xB1], exception: exc);
                wal.Append(3_000, LogLevel.Information, 1, "event {n}",  [0xC1], exception: exc);
            }
            PoisonException(path, entry: 1);

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal(3, entries.Count);
            Assert.Null(entries[1].Exception);
            Assert.NotNull(entries[1].ExceptionDecodeError);
            Assert.Equal([0xB1], entries[1].Payload);
            Assert.Equal(LogLevel.Error, entries[1].Level);
            Assert.Equal(2_000, entries[1].TimestampTicks);
            Assert.Null(entries[0].ExceptionDecodeError);
            Assert.Null(entries[2].ExceptionDecodeError);
            Assert.Equal(exc.Message, entries[2].Exception?.Message);   // the walk went on, and decoded the next one
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// Makes entry <paramref name="entry"/>'s exception undecodable — its map header claims fifteen
    /// fields — and recomputes the entry's checksum, so the entry still verifies. Entries without the
    /// trace context and inline texts only: header, payload, exception.
    /// </summary>
    private static void PoisonException(string path, int entry)
    {
        byte[] file = File.ReadAllBytes(path);
        long   pos  = FileHeader;
        for (int i = 0; i < entry; i++)
            pos += EntryHeader + BinaryPrimitives.ReadUInt32LittleEndian(file.AsSpan((int)pos))
                               + BinaryPrimitives.ReadUInt32LittleEndian(file.AsSpan((int)pos + 16));
        int at         = (int)pos;
        int payloadLen = (int)BinaryPrimitives.ReadUInt32LittleEndian(file.AsSpan(at));
        int excLen     = (int)BinaryPrimitives.ReadUInt32LittleEndian(file.AsSpan(at + 16));
        Assert.Equal(0, file[at + 13] & ~0x03);                   // no trace context or inline text in the way
        Assert.True(excLen > 0, "the entry has no exception to poison");
        int excAt      = at + EntryHeader + payloadLen;
        Assert.Equal(0x80, file[excAt] & 0xF0);                   // a fixmap …
        file[excAt] = 0x8F;                                       // … that now claims 15 fields

        uint crc = Crc32c.Append(0, file.AsSpan(at, EntryHeader - 4));
        crc      = Crc32c.Append(crc, file.AsSpan(at + EntryHeader, payloadLen));
        crc      = Crc32c.Append(crc, file.AsSpan(excAt, excLen));
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(at + EntryHeader - 4), crc);
        File.WriteAllBytes(path, file);
    }

    /// <summary>
    /// A long pooled template counts as carried only once an entry that carries it is in the log: an
    /// append refused for want of room (a spill log) leaves the next one to carry it, so no entry can
    /// name the index ahead of the text. And each log carries it for itself — the one a spill rotates
    /// to carries it again.
    /// </summary>
    [Fact]
    public void A_long_pooled_template_is_carried_by_each_log_and_not_counted_carried_by_a_refused_append()
    {
        string spill = NewWalPath(), next = NewWalPath();
        string longTemplate = new string('y', 70_000);
        try
        {
            using (var wal = WriteAheadLog.Open(spill, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024, spill: true))
            {
                Assert.True(wal.Append(1_000, LogLevel.Information, 0, "first {n}", [0]));
                Assert.False(wal.Append(2_000, LogLevel.Information, 7, longTemplate, [1]));   // refused: it does not fit
                // Still the carrier, so still refused. An entry naming the index without the text
                // would fit, and would replay with no template.
                Assert.False(wal.Append(3_000, LogLevel.Information, 7, longTemplate, [2]));
            }
            using (var wal = WriteAheadLog.Open(next, new NodeId(3), new SegmentId(48UL), initialCapacity: 1024 * 1024))
            {
                Assert.True(wal.Append(2_000, LogLevel.Information, 7, longTemplate, [1]));
                Assert.True(wal.Append(3_000, LogLevel.Information, 7, longTemplate, [2]));
            }

            Assert.Null(Assert.Single(WriteAheadLog.ReadForRecovery(spill).Entries).Template);   // the pooled first event: a row
            var (_, entries) = WriteAheadLog.ReadForRecovery(next);
            Assert.Equal(2, entries.Count);
            Assert.All(entries, e => Assert.Equal(longTemplate, e.Template));
        }
        finally
        {
            foreach (var p in new[] { spill, next }) { File.Delete(p); File.Delete(p + ".pool"); }
        }
    }

    /// <summary>
    /// A spill log grows for a first entry larger than it to that entry's size and no further, so it
    /// takes nothing after it. It doubled, like a live WAL: a first entry between one and two
    /// capacities left it at twice the size its floor check had counted, taking entries until that
    /// was full (the review's probe: 2 050 468 B written where 1 048 576 were counted).
    /// </summary>
    [Fact]
    public void A_spill_log_grows_for_its_first_entry_only_to_that_entrys_size()
    {
        string path = NewWalPath();
        byte[] big  = new byte[1_500_000];
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024, spill: true))
            {
                Assert.True(wal.Append(1_000, LogLevel.Information, 1, "t", big));       // larger than the log: taken, being the first
                Assert.False(wal.Append(2_000, LogLevel.Information, 1, "t", [1]));      // and nothing after it
            }

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal(big.Length, Assert.Single(entries).Payload.Length);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// A spill log's pool file counts against the log's capacity. Its rows — up to 64 KB each, written
    /// before the entry that needs them is checked for room — were counted by no capacity and no floor:
    /// a hundred events with distinct long templates and tiny payloads filled a 256 KB spill log's
    /// pool to 6 MB while the log itself took every one of them.
    /// </summary>
    [Fact]
    public void A_spill_logs_pool_rows_count_against_its_capacity()
    {
        string     path     = NewWalPath();
        const long Capacity = 256 * 1024;
        try
        {
            int taken = 0;
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: Capacity, spill: true))
            {
                for (int i = 0; i < 100; i++)
                {
                    string template = new string((char)('a' + i % 26), 60_000) + i;   // a pooled row of ~60 KB each
                    if (!wal.Append(1_000 + i, LogLevel.Information, i, template, [(byte)i])) break;
                    taken++;
                }
            }

            long onDisk = new FileInfo(path).Length + new FileInfo(path + ".pool").Length;
            Assert.InRange(taken, 1, 5);
            Assert.True(onDisk <= 32 + Capacity + WriteAheadLog.MaxRowsPastCapacity,
                        $"{onDisk:N0} B on disk for a {Capacity:N0} B spill log");
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// A service outside the pool — a full pool answers -1 — travels in its entry (v6 InlineService),
    /// after the inline template and under the checksum, and comes back whole; a pooled one is still
    /// named by its index and row, and an entry given none has none. It used to be logged with no
    /// service at all, and replayed and despilled so.
    /// </summary>
    [Fact]
    public void A_service_outside_the_pool_travels_in_its_entry()
    {
        string path = NewWalPath();
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024))
            {
                wal.Append(1_000, LogLevel.Information, -1, "unpooled {n}", [1], serviceIndex: -1, service: "svc.outside");
                wal.Append(2_000, LogLevel.Information, 5,  "pooled {n}",   [2], serviceIndex: 9,  service: "svc.pooled");
                wal.Append(3_000, LogLevel.Information, 5,  "pooled {n}",   [3], serviceIndex: -1, service: null);
            }

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal(3, entries.Count);
            Assert.Equal("svc.outside", entries[0].Service);
            Assert.Equal(-1, entries[0].ServiceIndex);
            Assert.Equal("unpooled {n}", entries[0].Template);
            Assert.Equal([1], entries[0].Payload);
            Assert.Null(entries[1].Service);
            Assert.Equal(9, entries[1].ServiceIndex);
            Assert.Null(entries[2].Service);
            Assert.Equal(-1, entries[2].ServiceIndex);

            // A rotted byte in the first entry's service fails that entry, like any other field.
            Corrupt(path, FileHeader + EntryHeader + 4 + "unpooled {n}".Length + 4 + 2);
            Assert.Empty(WriteAheadLog.ReadForRecovery(path).Entries);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>A rotted byte in an inline template fails its entry like any other field: the checksum covers it.</summary>
    [Fact]
    public void A_rotted_inline_template_stops_replay_at_its_entry()
    {
        string path = NewWalPath();
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024))
            {
                wal.Append(1_000, LogLevel.Information, -1, "first", [1]);
                wal.Append(2_000, LogLevel.Information, -1, "second", [2]);
            }
            int first = EntryHeader + 4 + "first".Length + 1;
            Corrupt(path, FileHeader + first + EntryHeader + 4 + 2);   // inside the second entry's text

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal("first", Assert.Single(entries).Template);
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// A rotted byte in the trace context fails the entry like any other field: the checksum covers it.
    /// </summary>
    [Fact]
    public void A_rotted_trace_context_stops_replay_at_its_entry()
    {
        string path = NewWalPath();
        try
        {
            using (var wal = WriteAheadLog.Open(path, new NodeId(3), new SegmentId(42UL), initialCapacity: 1024 * 1024))
            {
                wal.Append(1_000, LogLevel.Information, 1, "t", [1], traceIdHi: 1, traceIdLo: 2, spanId: 3);
                wal.Append(2_000, LogLevel.Information, 1, "t", [2], traceIdHi: 4, traceIdLo: 5, spanId: 6);
            }
            int first = EntryHeader + TraceContext + 1;
            Corrupt(path, FileHeader + first + EntryHeader + 9);   // inside the second entry's trace id

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            var only = Assert.Single(entries);
            Assert.Equal((1UL, 2UL, 3UL), (only.TraceIdHi, only.TraceIdLo, only.SpanId));
        }
        finally { File.Delete(path); File.Delete(path + ".pool"); }
    }

    /// <summary>
    /// A v5 file, what every release before v6 wrote, still replays: with its services, and with no
    /// trace context whatever its flag byte says. Bit 2 was reserved in v5, and a v5 entry has no
    /// bytes for it to name.
    /// </summary>
    [Fact]
    public void A_v5_file_replays_with_services_and_without_trace_context()
    {
        string path = NewWalPath();
        try
        {
            WriteFile(path, version: 5,
                V5Entry(100, LogLevel.Information, flags: 0x02, templateIndex: 1, serviceIndex: 4, [1, 2, 3]),
                V5Entry(200, LogLevel.Warning,     flags: 0x06, templateIndex: 2, serviceIndex: 5, [4, 5]));   // + a bit v5 never had

            var (_, entries) = WriteAheadLog.ReadForRecovery(path);
            Assert.Equal([100L, 200L], entries.Select(e => e.TimestampTicks));
            Assert.Equal([4, 5], entries.Select(e => e.ServiceIndex));
            Assert.All(entries, e => Assert.Equal((0UL, 0UL, 0UL), (e.TraceIdHi, e.TraceIdLo, e.SpanId)));
            Assert.Equal([4, 5], entries[1].Payload);
        }
        finally { File.Delete(path); }
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

    private static byte[] V6Entry(long ticks, LogLevel level, byte flags, ushort templateIndex, ushort serviceIndex,
                                  ulong traceHi, ulong traceLo, ulong spanId, byte[] payload)
    {
        var e = new byte[EntryHeader + TraceContext + payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(0),  (uint)payload.Length);
        BinaryPrimitives.WriteInt64LittleEndian (e.AsSpan(4),  ticks);
        e[12] = (byte)level;
        e[13] = flags;
        BinaryPrimitives.WriteUInt16LittleEndian(e.AsSpan(14), templateIndex);
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(16), 0);                  // no exception
        BinaryPrimitives.WriteUInt16LittleEndian(e.AsSpan(20), serviceIndex);
        BinaryPrimitives.WriteUInt64LittleEndian(e.AsSpan(EntryHeader),      traceHi);
        BinaryPrimitives.WriteUInt64LittleEndian(e.AsSpan(EntryHeader + 8),  traceLo);
        BinaryPrimitives.WriteUInt64LittleEndian(e.AsSpan(EntryHeader + 16), spanId);
        uint crc = Crc32c.Append(0, e.AsSpan(0, 22));
        crc      = Crc32c.Append(crc, e.AsSpan(EntryHeader, TraceContext));
        crc      = Crc32c.Append(crc, payload);
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(22), crc);
        payload.CopyTo(e, EntryHeader + TraceContext);
        return e;
    }

    /// <summary>A v6 entry with no trace context that carries its template's text: header, length u32, UTF-8 text, payload.</summary>
    private static byte[] V6InlineEntry(long ticks, LogLevel level, byte flags, string template, byte[] payload)
    {
        byte[] text = System.Text.Encoding.UTF8.GetBytes(template);
        var e = new byte[EntryHeader + 4 + text.Length + payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(0),  (uint)payload.Length);
        BinaryPrimitives.WriteInt64LittleEndian (e.AsSpan(4),  ticks);
        e[12] = (byte)level;
        e[13] = flags;
        BinaryPrimitives.WriteUInt16LittleEndian(e.AsSpan(14), 0);                  // unpooled: no index
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(16), 0);                  // no exception
        BinaryPrimitives.WriteUInt16LittleEndian(e.AsSpan(20), 0);                  // no service
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(EntryHeader), (uint)text.Length);
        text.CopyTo(e, EntryHeader + 4);
        uint crc = Crc32c.Append(0, e.AsSpan(0, 22));
        crc      = Crc32c.Append(crc, e.AsSpan(EntryHeader, 4 + text.Length));
        crc      = Crc32c.Append(crc, payload);
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(22), crc);
        payload.CopyTo(e, EntryHeader + 4 + text.Length);
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
