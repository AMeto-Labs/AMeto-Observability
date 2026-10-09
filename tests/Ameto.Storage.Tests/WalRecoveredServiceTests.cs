using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query;

namespace Ameto.Storage.Tests;

/// <summary>
/// An event's service is a header field — a pool index, <see cref="LogEventHeader.ServiceNamePoolIndex"/> —
/// and a WAL entry before format v5 had no field for it. Every event replayed out of an orphaned WAL
/// therefore came back with no service: an OTLP event's resource <c>service.name</c>, a CLEF event's
/// <c>@service</c>, gone from the <c>@svc</c> column, from <c>@service</c> filters, from the service
/// index a merge builds and from the event JSON, although the same event flushed normally carries it
/// (#111). A v5 entry logs the index and the WAL's pool file the text, and the replay gives the event
/// its service back.
///
/// <para>The replay must still say "no service" when there is none — the first test below. The field
/// is a plain int on a struct, its default of 0 is a valid pool index rather than the -1 every reader
/// tests for, and slot 0 is ordinarily the WAL's first template: a replay that left the field unset
/// stamped that template on every recovered event as its service, permanently.</para>
///
/// <para>The sibling suite <c>WalUnpooledTemplateTests</c> covers the same replay path for the
/// TEMPLATE index; <c>WriteAheadLogTests</c> pins the entry layout byte by byte.</para>
/// </summary>
public sealed class WalRecoveredServiceTests : IAsyncLifetime
{
    private const string IndexZeroTemplate = "Starting {App}";

    // The services the replay must hand back exactly: plain ASCII; Cyrillic with an emoji (a
    // surrogate pair in UTF-16, four bytes in UTF-8); and two long ones, past the 512 chars the
    // intern pool decodes on the stack and in the kilobytes in UTF-8.
    private const string Ascii    = "Orders.Api";
    private const string NonAscii = "Платежи.Шлюз 💳";
    private static readonly string LongAscii    = "Svc." + new string('q', 3_000);
    private static readonly string LongNonAscii = string.Concat(Enumerable.Repeat("сервис-платежей-", 120));

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

    /// <summary>
    /// One event as the drainer hands it over: the service already interned in the engine's pool
    /// (the OTLP parsers once per resource block, CLEF ingest per event), or -1 for none.
    /// </summary>
    private void Write(long ticks, string? service, int n, LogLevel level = LogLevel.Information)
    {
        const string Template = "order {n} handled";
        Assert.True(_engine.TryWrite(new LogEventHeader
        {
            TimestampUtcTicks        = ticks,
            Level                    = level,
            MessageTemplatePoolIndex = _engine.TemplatePool.Intern(Template),
            ServiceNamePoolIndex     = service is null ? -1 : _engine.TemplatePool.Intern(service),
        }, Props(n), Template));
    }

    /// <summary>
    /// kill -9: keep the live WAL's bytes, let shutdown flush and unlink them, then undo that flush so
    /// the WAL is orphaned and the next start replays it. <paramref name="tamper"/> gets the restored
    /// WAL's path before the restart, to damage it the way a crash can.
    /// </summary>
    private async Task CrashAndRestartAsync(Action<string>? tamper = null)
    {
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
        tamper?.Invoke(wal);

        _engine = NewEngine();
        await _engine.CatalogLoaded;
    }

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

    private static string? NullIfEmpty(string? s) => string.IsNullOrEmpty(s) ? null : s;

    private static async Task<List<LogEvent>> QueryAsync(QueryExecutor q, string filter, int count = 1_000)
    {
        var res = new List<LogEvent>();
        await foreach (var ev in q.ExecuteAsync(new QueryRequest
        {
            Filter = filter, Count = count, Direction = QueryDirection.Backward,
        }))
        {
            res.Add(ev);
            if (res.Count >= count) break;
        }
        return res;
    }

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

        await CrashAndRestartAsync();

        // Slot 0 is a message template, restored by the replay itself — the string an unset
        // ServiceNamePoolIndex resolves to.
        Assert.Equal(IndexZeroTemplate, _engine.TemplatePool.Get(0));

        var events = ReadAllByTicks();
        Assert.Equal(3, events.Count);              // the pre-rotation event, plus both replayed
        foreach (long ticks in new[] { firstTicks, secondTicks })
            Assert.True(string.IsNullOrEmpty(events[ticks].Service),
                $"a replayed event was stamped with pool slot 0 as its service: '{events[ticks].Service}'");
    }

    /// <summary>
    /// The issue itself. Services that change from event to event — the drainer interleaves batches
    /// from concurrent senders — including none at all, one at pool index 0 (so the entry must say
    /// "has a service" with a flag, not with an index), non-ASCII and long ones. After a kill -9 each
    /// event comes back with its own service: in the segment's <c>@svc</c> column, to an
    /// <c>@service</c> filter, and as the <see cref="LogEvent.ServiceName"/> the event JSON writes as
    /// <c>@service</c>.
    /// </summary>
    [Fact]
    public async Task Every_replayed_event_gets_its_own_service_back()
    {
        Assert.Equal(0, _engine.TemplatePool.Intern(Ascii));   // a service at index 0
        string?[] rotation = [Ascii, NonAscii, null, LongAscii, Ascii, LongNonAscii, NonAscii];

        long t = DateTime.UtcNow.Ticks;
        var expected = new Dictionary<long, string?>();
        for (int n = 0; n < 5 * rotation.Length; n++)
        {
            long ticks = t + n * TimeSpan.TicksPerMillisecond;
            Write(ticks, rotation[n % rotation.Length], n);
            expected[ticks] = rotation[n % rotation.Length];
        }

        await CrashAndRestartAsync();

        var events = ReadAllByTicks();
        Assert.Equal(expected.Count, events.Count);
        foreach (var (ticks, service) in expected)
            Assert.True(service == NullIfEmpty(events[ticks].Service),
                $"event at +{(ticks - t) / TimeSpan.TicksPerMillisecond} ms came back with service " +
                $"'{Shorten(events[ticks].Service)}', expected '{Shorten(service)}'");

        var query = new QueryExecutor(_engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);
        foreach (string service in rotation.OfType<string>().Distinct())
        {
            var found = await QueryAsync(query, $"@service = '{service}'");
            Assert.Equal(expected.Count(kv => kv.Value == service), found.Count);
            Assert.All(found, ev => Assert.Equal(service, ev.ServiceName));
        }
        var none = await QueryAsync(query, "not has(@service)");
        Assert.Equal(expected.Count(kv => kv.Value is null), none.Count);
    }

    /// <summary>
    /// A recovery segment is written without an index (the replay runs in the constructor, before
    /// the indexing service has wired its sink), so the service index a recovered event belongs to
    /// is built by the merge that later folds the segment into its neighbours. Here a merge does
    /// that, with the production index builder, and a filter served through the merged file's index
    /// finds the replayed events beside the ones the same service flushed normally — the property
    /// "indistinguishable from a normal flush" has to have for the index, not only for the column.
    /// </summary>
    [Fact]
    public async Task A_replayed_event_is_in_the_service_index_a_merge_builds()
    {
        const string Other = "Billing.Worker";
        long old = MergeBucketGrid.SealedBucketStart(LogLevel.Information);

        // Flushed normally, indexed, before the crash.
        _engine.IndexSinkFactory = static (events, terms) => new SegmentIndexBuilder(events, 5, terms);
        for (int n = 0; n < 20; n++) Write(old + n * TimeSpan.TicksPerSecond, NonAscii, n);
        await _engine.FlushHotTierAsync();

        // Logged, then lost to the crash, then replayed.
        for (int n = 20; n < 60; n++) Write(old + n * TimeSpan.TicksPerSecond, n % 2 == 0 ? NonAscii : Other, n);
        await CrashAndRestartAsync();
        Assert.Equal(2, _engine.ListSegments().Count);

        _engine.IndexSinkFactory = static (events, terms) => new SegmentIndexBuilder(events, 5, terms);
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        var merged = Assert.Single(_engine.ListSegments());
        using (var reader = SegmentReader.Open(merged.FilePath))
            Assert.True(reader.Groups.Length > 0);     // the filter below goes through an index

        var query = new QueryExecutor(_engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);
        Assert.Equal(20 + 20, (await QueryAsync(query, $"@service = '{NonAscii}'")).Count);
        Assert.Equal(20,      (await QueryAsync(query, $"@service = '{Other}'")).Count);
    }

    /// <summary>
    /// A torn tail: the crash wrote the header's WriteOffset but not every byte of the last entry.
    /// Replay stops at the entry whose checksum fails — the v5 checksum covers the service index —
    /// and every entry before it keeps its service.
    /// </summary>
    [Fact]
    public async Task A_torn_last_entry_is_dropped_and_the_entries_before_it_keep_their_services()
    {
        long t = DateTime.UtcNow.Ticks;
        for (int n = 0; n < 6; n++) Write(t + n * TimeSpan.TicksPerSecond, n % 2 == 0 ? Ascii : NonAscii, n);

        await CrashAndRestartAsync(tamper: static wal =>
        {
            using var fs = new FileStream(wal, FileMode.Open, FileAccess.ReadWrite);
            Span<byte> header = stackalloc byte[32];
            fs.ReadExactly(header);
            long writeOffset = BinaryPrimitives.ReadInt64LittleEndian(header[24..]);
            fs.Seek(writeOffset - 3, SeekOrigin.Begin);   // the last payload bytes never reached disk
            fs.Write(new byte[3]);
        });

        var events = ReadAllByTicks();
        Assert.Equal(5, events.Count);
        for (int n = 0; n < 5; n++)
            Assert.Equal(n % 2 == 0 ? Ascii : NonAscii, events[t + n * TimeSpan.TicksPerSecond].Service);
    }

    /// <summary>
    /// The pool file is fsynced on the same tick as the WAL, but a power loss can still keep an entry
    /// and lose the pool row its service needed. The entry then replays with NO service — not with
    /// whatever the restarted process's pool holds at that index. Here that is a stale string: an
    /// orphaned WAL left by an earlier process, whose pool numbered its strings differently, is
    /// replayed first and force-interns its own text at the same index. Resolved through the live
    /// pool, the lost row would become that string, in the recovery segment, for good.
    /// </summary>
    [Fact]
    public async Task A_service_whose_pool_row_was_lost_replays_as_no_service_not_as_another_wals_string()
    {
        const string Stale    = "Stale.Service";
        const ulong  StaleSeg = 1_000;
        long t = DateTime.UtcNow.Ticks;
        Write(t, Ascii, 0);
        Write(t + TimeSpan.TicksPerSecond, NonAscii, 1);
        int lost = _engine.TemplatePool.Intern(NonAscii);

        await CrashAndRestartAsync(tamper: wal =>
        {
            // The template's row and the first service's reached disk; the second service's did not.
            var rows = WriteAheadLog.LoadPool(wal + ".pool");
            Assert.Equal(["order {n} handled", Ascii, NonAscii], rows.OrderBy(kv => kv.Key).Select(kv => kv.Value));
            using (var pool = File.Create(wal + ".pool"))
                foreach (var (index, text) in rows)
                    if (text != NonAscii) WritePoolRow(pool, index, text);

            CreateListedBefore(wal, path =>
            {
                using var stale = WriteAheadLog.Open(path, NodeId.Local, new SegmentId(StaleSeg), 64 * 1024);
                stale.Append(t - TimeSpan.TicksPerSecond, LogLevel.Information, 0, "order {n} handled", Props(99),
                             serviceIndex: lost, service: Stale);
            });
        });

        var events = ReadAllByTicks();
        Assert.Equal(3, events.Count);
        Assert.Equal(Stale, events[t - TimeSpan.TicksPerSecond].Service);   // the other WAL's event keeps its own
        Assert.Equal(Ascii, events[t].Service);
        Assert.True(string.IsNullOrEmpty(events[t + TimeSpan.TicksPerSecond].Service),
            $"a service with no pool row was resolved anyway: '{events[t + TimeSpan.TicksPerSecond].Service}'");
        Assert.All(events.Values, e => Assert.Equal("order {n} handled", e.Template));
    }

    /// <summary>
    /// Creates a WAL (through <paramref name="create"/>) under a name the directory lists BEFORE
    /// <paramref name="other"/>, so the engine's replay, which walks that listing, replays it first.
    /// The order is the file system's — by name on NTFS, by hash on ext4, newest first on tmpfs — so
    /// the listing is asked rather than assumed, and a test built on the order cannot pass by luck.
    /// </summary>
    private static void CreateListedBefore(string other, Action<string> create)
    {
        string dir = Path.GetDirectoryName(other)!;
        for (int k = 0; k < 64; k++)
        {
            string path = Path.Combine(dir, $"!stale-{k}.wal");
            create(path);
            var listing = Directory.EnumerateFiles(dir, "*.wal").ToList();
            if (listing.IndexOf(path) < listing.IndexOf(other)) return;
            File.Delete(path);
            File.Delete(path + ".pool");
        }
        Assert.Fail($"no WAL name the file system lists before {Path.GetFileName(other)}");
    }

    /// <summary>
    /// A WAL the previous release left behind is format v4: its entries have no service field, so
    /// they replay as they always did, with no service — whatever the pool file holds and whatever
    /// the reserved bits of the flag byte say (v4 bytes 20-23 are the checksum, not a service index).
    /// Assembled by hand from the documented v4 layout, so this pins the bytes on disk and not
    /// whatever the current writer produces.
    /// </summary>
    [Fact]
    public async Task A_v4_wal_from_the_previous_release_replays_without_a_service()
    {
        await _engine.DisposeAsync();

        const ulong SegId = 2_000;
        long t = DateTime.UtcNow.Ticks;
        string walPath = Path.Combine(WalDir, $"0-{SegId}.wal");

        var entries = new List<byte[]>
        {
            V4Entry(t,                           flags: 0,    templateIndex: 0, Props(1)),
            V4Entry(t + TimeSpan.TicksPerSecond, flags: 0x02, templateIndex: 0, Props(2)),   // bit 1 means nothing in v4
        };
        var file = new byte[32 + 64 * 1024];
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(0),  0x52_44_57_41);   // "RDWA"
        BinaryPrimitives.WriteUInt16LittleEndian(file.AsSpan(4),  4);               // v4
        BinaryPrimitives.WriteUInt64LittleEndian(file.AsSpan(16), SegId);
        BinaryPrimitives.WriteInt64LittleEndian (file.AsSpan(24), 32 + entries.Sum(e => e.Length));
        int pos = 32;
        foreach (var e in entries) { e.CopyTo(file, pos); pos += e.Length; }
        File.WriteAllBytes(walPath, file);
        using (var pool = File.Create(walPath + ".pool"))
        {
            WritePoolRow(pool, 0, IndexZeroTemplate);
            WritePoolRow(pool, 1, Ascii);   // a service row the v4 entries cannot point at
        }

        var (_, parsed) = WriteAheadLog.ReadForRecovery(walPath);
        Assert.Equal(2, parsed.Count);
        Assert.All(parsed, e => Assert.Equal(-1, e.ServiceIndex));

        _engine = NewEngine();
        await _engine.CatalogLoaded;

        var events = ReadAllByTicks();
        Assert.Equal(2, events.Count);
        Assert.All(events.Values, e => Assert.True(string.IsNullOrEmpty(e.Service), $"service '{e.Service}'"));
        Assert.All(events.Values, e => Assert.Equal(IndexZeroTemplate, e.Template));
    }

    /// <summary>
    /// v4: payloadLen u32 | ticks i64 | level u8 | flags u8 | templateIndex u16 | exceptionLen u32 |
    /// crc32c u32 over bytes [0, 20) + payload.
    /// </summary>
    private static byte[] V4Entry(long ticks, byte flags, ushort templateIndex, byte[] payload)
    {
        var e = new byte[24 + payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(0),  (uint)payload.Length);
        BinaryPrimitives.WriteInt64LittleEndian (e.AsSpan(4),  ticks);
        e[12] = (byte)LogLevel.Information;
        e[13] = flags;
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

    private static string? Shorten(string? s) => s is { Length: > 40 } ? s[..40] + "…" : s;
}
