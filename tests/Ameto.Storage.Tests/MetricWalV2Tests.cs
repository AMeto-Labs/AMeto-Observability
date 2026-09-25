using System.Buffers.Binary;
using System.Text;
using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Ameto.Storage.Tests;

/// <summary>
/// METRIC WAL v2: a CRC32C per entry and per pool record, and the v1 logs the previous release
/// left behind still replay. Issue #87, the residual of incident #56.
///
/// <para>v1 protected an entry only by judgement — the generation margin, the series-index cap,
/// the header's claim checked against the walk — so a torn entry whose fields decoded as
/// plausible replayed as a point, and a flush then wrote it into a permanent <c>.mts</c>. Every
/// fact below is about the first entry that does not verify ENDING the replay, about where the
/// checksum is stored relative to the header's claim, and about the one thing a format change
/// must never do: drop the log the process before it acknowledged. All judged at seams; no
/// timers.</para>
/// </summary>
public sealed class MetricWalV2Tests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mwal2-" + Guid.NewGuid().ToString("N"));
    private readonly List<MetricWriteAheadLog> _wals    = [];
    private readonly List<MetricStorageEngine> _engines = [];

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        for (int i = _wals.Count - 1; i >= 0; i--)
            try { _wals[i].Dispose(); } catch { }
        for (int i = _engines.Count - 1; i >= 0; i--)
            try { await _engines[i].DisposeAsync(); } catch { }
        try { Directory.Delete(_dir, true); } catch { }
    }

    private string WalPath  => Path.Combine(_dir, "metrics.wal");
    private string PoolPath => WalPath + ".pool";
    private string TmpPath  => WalPath + MetricWriteAheadLog.UpgradeSuffix;

    private const int  FileHeader   = 64;    // v2: v1's 32 bytes + the relocation record
    private const int  FileHeaderV1 = 32;
    private const int  V1Entry    = 48;          // the header, no checksum
    private const int  V2Entry    = 52;          // the same 48 bytes + the CRC
    private const long Capacity   = 64 * 1024;
    private const long BaseNano   = 1_785_300_000_000_000_000L;

    private MetricWriteAheadLog Open(ILogger? logger = null, string? path = null)
    {
        var wal = MetricWriteAheadLog.Open(path ?? WalPath, Capacity, logger);
        _wals.Add(wal);
        return wal;
    }

    private MetricWriteAheadLog OpenWith(MetricWriteAheadLog.UpgradeIo io, ILogger? logger = null)
    {
        var wal = MetricWriteAheadLog.Open(WalPath, Capacity, logger, beforeResize: null, interner: null, io);
        _wals.Add(wal);
        return wal;
    }

    private static readonly double[] Bounds = [1.0, 5.0, 10.0];

    private static MetricIngestItem Gauge(string name, int i, double value) => new()
    {
        Name              = name,
        Kind              = MetricKind.Gauge,
        Unit              = "ms",
        Labels            = new LabelSet([new("service.name", "MintRoute.API")]),
        TimestampUnixNano = BaseNano + i,
        ScalarValue       = value,
    };

    private static MetricIngestItem Histo(string name, int i, long[] buckets) => new()
    {
        Name              = name,
        Kind              = MetricKind.Histogram,
        Unit              = "s",
        Labels            = new LabelSet([new("service.name", "KioskAgent.API")]),
        TimestampUnixNano = BaseNano + i,
        HistogramCount    = buckets.Sum(),
        HistogramSum      = 2.5 * buckets.Sum(),
        BucketBounds      = Bounds,
        BucketCounts      = buckets,
    };

    /// <summary>Five entries — gauge, gauge, gauge, a 4-bucket histogram, gauge — returning each one's FILE offset.</summary>
    private long[] AppendFive(MetricWriteAheadLog wal)
    {
        var at = new long[5];
        for (int i = 0; i < 5; i++)
        {
            at[i] = FileHeader + wal.WrittenBytes;
            wal.Append([i == 3 ? Histo("latency", i, [3, 9, 4, 1]) : Gauge("cpu", i, i * 1.5)]);
        }
        return at;
    }

    private static byte[] ReadShared(string path)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        var bytes = new byte[fs.Length];
        fs.ReadExactly(bytes);
        return bytes;
    }

    private void Xor(string path, long offset, byte mask)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite);
        fs.Position = offset;
        int b = fs.ReadByte();
        fs.Position = offset;
        fs.WriteByte((byte)(b ^ mask));
    }

    // ── Hand-built files: the layouts, byte for byte ─────────────────────────

    private readonly record struct RawEntry(ulong Generation, uint Series, long Nano, double Value,
                                            long Count = 0, double Sum = 0, long[]? Buckets = null);

    /// <summary>One entry in the v1 (48-byte header) or v2 (header + CRC) layout; <paramref name="crc"/> overrides the checksum.</summary>
    private static byte[] EntryBytes(RawEntry e, bool v2, uint? crc = null)
    {
        int buckets = e.Buckets?.Length ?? 0;
        int head    = v2 ? V2Entry : V1Entry;
        var b       = new byte[head + buckets * 8];
        BinaryPrimitives.WriteUInt64LittleEndian(b.AsSpan(0),  e.Generation);
        BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(8),  e.Series);
        BinaryPrimitives.WriteInt64LittleEndian (b.AsSpan(12), e.Nano);
        BinaryPrimitives.WriteDoubleLittleEndian(b.AsSpan(20), e.Value);
        BinaryPrimitives.WriteInt64LittleEndian (b.AsSpan(28), e.Count);
        BinaryPrimitives.WriteDoubleLittleEndian(b.AsSpan(36), e.Sum);
        BinaryPrimitives.WriteUInt16LittleEndian(b.AsSpan(44), (ushort)buckets);
        for (int k = 0; k < buckets; k++)
            BinaryPrimitives.WriteInt64LittleEndian(b.AsSpan(head + k * 8), e.Buckets![k]);
        if (v2)
            BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(48),
                crc ?? Crc32c.Append(Crc32c.Append(0, b.AsSpan(0, 48)), b.AsSpan(V2Entry)));
        return b;
    }

    /// <summary>A pool record body: kind, name, unit, labels, bounds — strings as u16 length + UTF-8.</summary>
    private static byte[] PoolBody(string name, MetricKind kind, string unit, (string K, string V)[] labels, double[]? bounds)
    {
        using var ms = new MemoryStream();
        using var w  = new BinaryWriter(ms);
        void Str(string s) { var u = Encoding.UTF8.GetBytes(s); w.Write((ushort)u.Length); w.Write(u); }
        w.Write((byte)kind);
        Str(name);
        Str(unit);
        w.Write((ushort)labels.Length);
        foreach (var (k, v) in labels) { Str(k); Str(v); }
        w.Write((ushort)(bounds?.Length ?? 0));
        foreach (var d in bounds ?? []) w.Write(d);
        w.Flush();
        return ms.ToArray();
    }

    /// <summary>A pool record in the v1 shape (index, length, body) or the v2 one (index, tagged length, crc, body).</summary>
    private static byte[] PoolRecord(uint index, byte[] body, bool v2)
    {
        var r = new byte[(v2 ? 12 : 8) + body.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(r.AsSpan(0), index);
        BinaryPrimitives.WriteUInt32LittleEndian(r.AsSpan(4), v2 ? (0xC5u << 24) | (uint)body.Length : (uint)body.Length);
        if (v2)
            BinaryPrimitives.WriteUInt32LittleEndian(r.AsSpan(8), Crc32c.Append(Crc32c.Append(0, r.AsSpan(0, 8)), body));
        body.CopyTo(r, v2 ? 12 : 8);
        return r;
    }

    /// <summary>Writes a whole log by hand at exactly <see cref="Capacity"/>, so an open neither grows nor shrinks it.</summary>
    private void WriteLog(ushort version, ulong generation, ulong committed, byte[][] entries, byte[][] poolRecords)
    {
        long written = entries.Sum(static e => (long)e.Length);
        int  head = version == 1 ? FileHeaderV1 : FileHeader;
        var file = new byte[head + Capacity];
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(0), 0x52_44_4D_57);          // "RDMW"
        BinaryPrimitives.WriteUInt16LittleEndian(file.AsSpan(4), version);
        BinaryPrimitives.WriteInt64LittleEndian (file.AsSpan(8), head + written);
        BinaryPrimitives.WriteUInt64LittleEndian(file.AsSpan(16), generation);
        BinaryPrimitives.WriteUInt64LittleEndian(file.AsSpan(24), committed);
        if (version != 1) SealHeader(file);
        long at = head;
        foreach (var e in entries) { e.CopyTo(file, at); at += e.Length; }
        File.WriteAllBytes(WalPath, file);
        File.WriteAllBytes(PoolPath, poolRecords.SelectMany(static r => r).ToArray());
    }

    /// <summary>The v2 header checksum: CRC32C over bytes [0, 8) and [16, 56), stored at 56.</summary>
    private static void SealHeader(byte[] file) =>
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(56),
            Crc32c.Append(Crc32c.Append(0, file.AsSpan(0, 8)), file.AsSpan(16, 40)));

    private static readonly byte[] CpuBody     = PoolBody("cpu",     MetricKind.Gauge,     "ms", [("service.name", "MintRoute.API")], null);
    private static readonly byte[] LatencyBody = PoolBody("latency", MetricKind.Histogram, "s",  [("service.name", "KioskAgent.API")], Bounds);

    /// <summary>
    /// The v1 log a previous release leaves: header generation 7 over watermark 5, one committed
    /// entry (dead), four live ones — a histogram among them — and its v1 pool.
    /// </summary>
    private void WriteV1Log() => WriteLog(1, generation: 7, committed: 5,
        [
            EntryBytes(new RawEntry(5, 0, BaseNano,     100.0), v2: false),                       // committed: dead
            EntryBytes(new RawEntry(6, 0, BaseNano + 1, 1.0),   v2: false),
            EntryBytes(new RawEntry(6, 1, BaseNano + 2, 2.5, 17, 42.5, [3, 9, 4, 1]), v2: false),
            EntryBytes(new RawEntry(7, 0, BaseNano + 3, 3.0),   v2: false),
            EntryBytes(new RawEntry(7, 0, BaseNano + 4, 4.0),   v2: false),
        ],
        [PoolRecord(0, CpuBody, v2: false), PoolRecord(1, LatencyBody, v2: false)]);

    private static void AssertTheV1Points(List<MetricWriteAheadLog.RecoveredPoint> replayed, int unresolved)
    {
        Assert.Equal(0, unresolved);
        Assert.Equal([1.0, 2.5, 3.0, 4.0], replayed.Select(static r => r.Point.Value));
        Assert.Equal(["cpu", "latency", "cpu", "cpu"], replayed.Select(static r => r.Name));
        var h = replayed[1];
        Assert.Equal(MetricKind.Histogram, h.Kind);
        Assert.Equal(Bounds, h.Bounds);
        Assert.Equal([3L, 9, 4, 1], h.Point.BucketCounts!);
        Assert.Equal(17, h.Point.Count);
        Assert.Equal(42.5, h.Point.Sum);
        Assert.Equal(BaseNano + 4, replayed[3].Point.TimestampUnixNano);
        Assert.Contains(("service.name", "MintRoute.API"), replayed[0].Labels.Pairs);
    }

    private static ushort VersionOnDisk(string path) => BinaryPrimitives.ReadUInt16LittleEndian(ReadShared(path).AsSpan(4));

    // ── The checksum ─────────────────────────────────────────────────────────

    /// <summary>
    /// ONE FLIPPED BIT ANYWHERE IN AN ENTRY ENDS THE REPLAY AT THAT ENTRY. Every field of the
    /// 48-byte header, its reserved bytes, the CRC itself and the bucket payload — each flip chosen
    /// so that nothing BUT the checksum can see it: the generation stays inside the margin, the
    /// series index lands on the other real series, the bucket count shrinks inside the bounds.
    /// The entries before the damaged one come back intact. With the checksum not verified, the
    /// damaged histogram replays with the corruption in it (or under the wrong series), and the
    /// entry after it with it.
    /// </summary>
    [Theory]
    [InlineData("generation",   1,        0x01)]    // 1 → 257: inside the margin
    [InlineData("series index", 8,        0x01)]    // 1 → 0: the OTHER real series
    [InlineData("timestamp",    12,       0x01)]
    [InlineData("value",        27,       0x40)]
    [InlineData("count",        28,       0x01)]
    [InlineData("sum",          43,       0x01)]
    [InlineData("bucket count", 44,       0x01)]    // 4 → 5: still inside the claimed end
    [InlineData("reserved",     46,       0x01)]
    [InlineData("the crc",      48,       0x01)]
    [InlineData("a bucket",     52 + 8,   0x01)]
    public void A_flipped_bit_in_any_field_ends_the_replay_at_the_entry_it_is_in(string field, int offsetInEntry, byte mask)
    {
        long[] at;
        using (var wal = Open()) at = AppendFive(wal);
        Xor(WalPath, at[3] + offsetInEntry, mask);

        var logger   = new CapturingLogger();
        var replayed = Open(logger).ReadAll(out int unresolved);

        Assert.True(3 == replayed.Count, $"{field}: replayed {replayed.Count} point(s), expected the three before the damage");
        Assert.Equal(0, unresolved);
        Assert.Equal([0.0, 1.5, 3.0], replayed.Select(static r => r.Point.Value));
        Assert.All(replayed, static r => Assert.Equal("cpu", r.Name));
        Assert.Contains(logger.Entries, static e => e.Level == LogLevel.Warning && e.Text.Contains("fails its checksum"));
    }

    /// <summary>
    /// A TORN TAIL — the last entry's bucket page never reached the disk, the header's claim did —
    /// is cut at open, at the first entry that does not verify, and replays as the entries before
    /// it; v1 replayed that histogram with zeroed buckets. The cut is real, not a replay-time skip:
    /// the next append lands where the torn entry was, and the following open reads both.
    /// </summary>
    [Fact]
    public void A_torn_tail_is_cut_at_the_first_checksum_mismatch_and_the_next_append_takes_its_place()
    {
        using (var wal = Open())
        {
            wal.Append([Gauge("cpu", 0, 1.0), Gauge("cpu", 1, 2.0), Histo("latency", 2, [3, 9, 4, 1])]);
            long tornBuckets = FileHeader + 2 * V2Entry + V2Entry;           // the histogram's buckets
            wal.Dispose();
            using var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite);
            fs.Position = tornBuckets;
            fs.Write(new byte[4 * 8]);                                        // never written back
        }

        using (var wal = Open())
        {
            Assert.Equal(2 * V2Entry, wal.WrittenBytes);                      // cut at open
            Assert.Equal([1.0, 2.0], wal.ReadAll(out _).Select(static r => r.Point.Value));
            wal.Append([Gauge("cpu", 9, 9.0)]);
        }

        using var again = Open();
        Assert.Equal([1.0, 2.0, 9.0], again.ReadAll(out _).Select(static r => r.Point.Value));
    }

    /// <summary>
    /// A TORN ENTRY IS A TORN WRITE, WHATEVER ITS FIELDS DECODE TO; AN ENTRY THAT VERIFIES AND IS
    /// STILL IMPOSSIBLE IS CORRUPTION. The first entry's generation is set to the incident's
    /// ≈155e9. Left with its old checksum it is what a random tear looks like — a Warning, and no
    /// 4 MiB quarantine copy of an ordinary unclean stop. With the checksum recomputed over it,
    /// the bytes were written whole and are still wrong: the Error and the quarantine are for
    /// that. Judging the generation before the checksum reported every random tear as corruption
    /// and only in-margin garbage as a torn write.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_torn_entry_is_reported_as_a_torn_write_and_only_a_verifying_impossible_one_as_corruption(bool verifies)
    {
        using (var wal = Open()) wal.Append([Gauge("cpu", 0, 1.0), Gauge("cpu", 1, 2.0)]);
        byte[] file = File.ReadAllBytes(WalPath);
        BinaryPrimitives.WriteUInt64LittleEndian(file.AsSpan(FileHeader), 155_000_000_000UL);
        if (verifies)
            BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(FileHeader + 48),
                Crc32c.Append(0, file.AsSpan(FileHeader, 48)));
        File.WriteAllBytes(WalPath, file);

        var logger = new CapturingLogger();
        using (var wal = Open(logger))
        {
            Assert.Empty(wal.ReadAll(out _));
            Assert.Equal(0, wal.WrittenBytes);
        }

        Assert.Equal(verifies, logger.Entries.Any(static e => e.Level == LogLevel.Error));
        Assert.Equal(!verifies, logger.Entries.Any(static e => e.Level == LogLevel.Warning && e.Text.Contains("fails its checksum")));
        Assert.Equal(verifies, File.Exists(WalPath + ".quarantine"));
    }

    /// <summary>
    /// THE INCIDENT #56 HEAD, AS A v2 LOG TEARS, DOES NOT REPLAY. Reconstructed from the stand's
    /// forensics (the bytes themselves were not kept in the repo): header generation 3 705 over
    /// watermark 3 704, the first entry torn — its bucket count reading 8, so the stride lands on
    /// an all-zero slot — and series 0 registered in the pool. This is the incident's SHAPE written
    /// the way a v2 append tears: the fields landed, the checksum slot did not (zeros). v1 caught
    /// the incident's own generation (≈155e9) with the margin; the residual it could not catch is
    /// the same tear with a PLAUSIBLE generation, here the header's own. Its timestamp is the
    /// year-2116 value the incident minted immortal files with, or — the case no timestamp guard
    /// can see — a sane one, and its value is garbage. v2 replays neither; the engine built over it
    /// comes up with an empty tier. A v1 log carrying the same head is the next fact.
    /// </summary>
    [Theory]
    [InlineData(true)]                                // the incident's year-2116 MaxNano
    [InlineData(false)]                               // a minute ago: invisible to every guard but the CRC
    public void The_incident_56_head_as_a_v2_log_tears_replays_no_point(bool year2116)
    {
        long stamp = year2116 ? 4_610_746_851_722_254_905L : DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L - 60_000_000_000L;
        var torn = new RawEntry(3_705, 0, stamp, BitConverter.Int64BitsToDouble(0x4B3F_A97A_8424_3FA9),
                                Count: 0x243F_A97A_84, Sum: 6.02e23, Buckets: new long[8]);
        WriteLog(2, generation: 3_705, committed: 3_704,
                 [EntryBytes(torn, v2: true, crc: 0)],                        // zeros: the slot never written
                 [PoolRecord(0, PoolBody("process_memory_usage", MetricKind.Gauge, "By", [], null), v2: true)]);
        byte[] head = File.ReadAllBytes(WalPath), headPool = File.ReadAllBytes(PoolPath);

        var logger = new CapturingLogger();
        using (var wal = Open(logger))
        {
            Assert.Empty(wal.ReadAll(out int unresolved));
            Assert.Equal(0, unresolved);
            Assert.Equal(0, wal.WrittenBytes);
        }
        Assert.Contains(logger.Entries, static e => e.Level == LogLevel.Warning && e.Text.Contains("fails its checksum"));

        // One level up: the constructor a host start makes replays nothing into the tier.
        File.WriteAllBytes(WalPath, head);
        File.WriteAllBytes(PoolPath, headPool);
        var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);
        _engines.Add(engine);
        Assert.Equal(0, engine.HotPointCount);
    }

    /// <summary>
    /// THE SAME HEAD IN A v1 LOG — the log the incident actually had — AND WHAT THE UPGRADE DOES
    /// WITH IT. A v1 entry carries no checksum, and the upgrade computes one over the bytes as it
    /// finds them, so whatever it copies it vouches for. With the incident's year-2116 timestamp the
    /// entry fails v1's own far-future judgement and is DROPPED by the upgrade (a Warning counts
    /// it): nothing replays. Before, it was copied, checksummed, and replayed from the v2 log as a
    /// garbage point. With a SANE timestamp and a garbage value nothing in a v1 entry can tell it
    /// from a real point — the upgrade carries it over and it replays: that limit is pinned here
    /// on purpose, and docs/CONFIGURATION.md states it.
    /// </summary>
    [Theory]
    [InlineData(true,  0)]                            // the incident: dropped
    [InlineData(false, 1)]                            // indistinguishable from data: carried, and replayed
    public void The_incident_56_head_in_a_v1_log_is_dropped_by_the_upgrade_when_v1_can_tell(bool year2116, int replays)
    {
        double garbage = BitConverter.Int64BitsToDouble(0x4B3F_A97A_8424_3FA9);
        long   stamp   = year2116 ? 4_610_746_851_722_254_905L : BaseNano;
        WriteLog(1, generation: 3_705, committed: 3_704,
                 [EntryBytes(new RawEntry(3_705, 0, stamp, garbage, Buckets: new long[8]), v2: false)],
                 [PoolRecord(0, PoolBody("process_memory_usage", MetricKind.Gauge, "By", [], null), v2: false)]);

        var logger   = new CapturingLogger();
        var replayed = Open(logger).ReadAll(out _);

        Assert.Equal((ushort)2, VersionOnDisk(WalPath));
        Assert.Equal(replays, replayed.Count);
        if (replays == 1) Assert.Equal(garbage, replayed[0].Point.Value);
        Assert.Equal(year2116, logger.Entries.Any(static e => e.Level == LogLevel.Warning && e.Text.Contains("stamped more than a day")));
    }

    // ── Where the checksum lands relative to the claim ───────────────────────

    /// <summary>
    /// EVERY CHECKSUM OF A BATCH IS DOWN BEFORE THE HEADER CLAIMS THE BATCH. At the seam that fires
    /// between the last entry and the header's write-offset store, the file is copied as it stands
    /// — what a process death there leaves. That copy replays none of the batch (the claim has not
    /// moved: all-or-nothing), and the same bytes with the claim moved over the batch — what a
    /// power loss that kept the header page and the data pages could leave — replay all of it,
    /// every entry verifying. Storing the header before the checksums fails the first half; storing
    /// a checksum after the header fails the second.
    /// </summary>
    [Fact]
    public void Every_checksum_of_a_batch_is_down_before_the_header_claims_it()
    {
        using var wal = Open();
        wal.Append([Gauge("cpu", 0, 1.0), Histo("latency", 1, [1, 2, 3, 4])]);   // registers both series
        long claimed = wal.WrittenBytes;

        byte[]? atSeam = null;
        byte[]? poolAtSeam = null;
        long batchEnd = 0;
        wal.OnBatchWrittenForTest = end =>
        {
            atSeam     = ReadShared(WalPath);
            poolAtSeam = ReadShared(PoolPath);
            batchEnd   = end;
        };
        wal.Append([Gauge("cpu", 2, 2.0), Histo("latency", 3, [5, 6, 7, 8]), Gauge("cpu", 4, 3.0)]);
        wal.OnBatchWrittenForTest = null;

        Assert.NotNull(atSeam);
        Assert.Equal(FileHeader + claimed, BinaryPrimitives.ReadInt64LittleEndian(atSeam.AsSpan(8)));   // not yet claimed
        Assert.Equal(claimed + 3 * V2Entry + 4 * 8, batchEnd);

        string death = Path.Combine(_dir, "death");
        Directory.CreateDirectory(death);
        File.WriteAllBytes(Path.Combine(death, "metrics.wal"), atSeam!);
        File.WriteAllBytes(Path.Combine(death, "metrics.wal.pool"), poolAtSeam!);
        Assert.Equal([1.0, 2.5],
                     Open(path: Path.Combine(death, "metrics.wal")).ReadAll(out _).Select(static r => r.Point.Value));

        string power = Path.Combine(_dir, "power");
        Directory.CreateDirectory(power);
        BinaryPrimitives.WriteInt64LittleEndian(atSeam.AsSpan(8), FileHeader + batchEnd);
        File.WriteAllBytes(Path.Combine(power, "metrics.wal"), atSeam);
        File.WriteAllBytes(Path.Combine(power, "metrics.wal.pool"), poolAtSeam!);
        var replayed = Open(path: Path.Combine(power, "metrics.wal")).ReadAll(out int unresolved);
        Assert.Equal(0, unresolved);
        Assert.Equal([1.0, 2.5, 2.0, 2.5, 3.0], replayed.Select(static r => r.Point.Value));
    }

    /// <summary>
    /// A CHECKSUM HASHED BEFORE THE LOCK IS NOT STORED FOR BYTES IT WAS NOT HASHED OVER. The batch
    /// path hashes each entry outside the write lock, against the generation it reads then and the
    /// series index it resolved; the seam opens the window between that and the lock. A flush that
    /// begins in it stamps the batch with the NEXT generation, and a commit that empties the log
    /// re-issues the series indices — either way the precomputed values are wrong for what lands,
    /// and every entry must still verify. Storing them regardless leaves a batch the next replay
    /// stops at the first entry of (a Debug build also trips the assertion in the write).
    /// </summary>
    [Theory]
    [InlineData(false)]      // a flush begins: the generation moves
    [InlineData(true)]       // a flush commits and empties the log: the generation AND the series indices move
    public void A_flush_between_the_hashing_and_the_lock_does_not_leave_stale_checksums(bool commit)
    {
        using var wal = Open();
        wal.Append([Gauge("cpu", 0, 1.0), Histo("latency", 1, [1, 1, 1, 1])]);

        bool fired = false;
        ulong opened = 0;
        wal.OnSeriesResolvedForTest = () =>
        {
            if (fired) return;
            fired  = true;
            opened = wal.BeginFlush();
            if (commit) Assert.Equal(MetricWalCommit.Committed, wal.CommitFlush(opened));
        };
        wal.Append([Gauge("cpu", 2, 2.0), Histo("latency", 3, [2, 2, 2, 2]), Gauge("cpu", 4, 3.0)]);
        wal.OnSeriesResolvedForTest = null;
        Assert.True(fired, "the seam never ran; the test proved nothing");
        if (!commit) wal.AbandonFlush(opened);
        wal.Dispose();

        var replayed = Open().ReadAll(out int unresolved);
        Assert.Equal(0, unresolved);
        double[] expected = commit ? [2.0, 2.5, 3.0] : [1.0, 2.5, 2.0, 2.5, 3.0];
        Assert.Equal(expected, replayed.Select(static r => r.Point.Value));
    }

    // ── A commit's relocation, killed half-way ───────────────────────────────

    /// <summary>
    /// A PROCESS KILLED ANYWHERE INSIDE A COMMIT'S MOVE OF THE SURVIVORS LOSES NONE OF THEM. The
    /// commit stores the watermark, then moves the surviving tail to the front. The file is copied
    /// at every point of that move a process can die at: once the record is armed — sealed, or
    /// between the arming store and its checksum; after each chunk — sealed, or between its
    /// <c>MoveDone</c> and the checksum, including the last chunk (all moved, nothing stored
    /// after); after the new end is stored with the record still armed; between disarming and its
    /// checksum; and after disarming, before the end marker. Where a chunk can be in flight the
    /// kill is completed by hand the way a forward memmove leaves it — <c>intoNextChunk</c> bytes
    /// of the next chunk copied. Some rows also rot the header's generation, so the header fails
    /// its checksum WITH a record armed. Opening each file must replay every survivor exactly once
    /// and no committed point, leave the log finished (the tail's length), and leave nothing for
    /// the next open to finish or rebuild. Without the finish at open, the moved copies verify, the
    /// entry straddling the frontier fails its checksum, and every survivor past it is lost.
    /// </summary>
    [Theory]
    [InlineData(6, 4, "armed",          0,   0,  false)]   // one chunk (the tail fits in the gap)
    [InlineData(6, 4, "armed",          0,   78, false)]   // …1.5 entries into it
    [InlineData(6, 4, "armed",          0,   78, true)]    // …and the header rotted
    [InlineData(6, 4, "armed-unsealed", 0,   0,  false)]   // between arming and its checksum
    [InlineData(6, 4, "armed-unsealed", 0,   0,  true)]
    [InlineData(6, 4, "chunk",          208, 0,  false)]   // all moved: done == tail
    [InlineData(6, 4, "chunk-unsealed", 208, 0,  false)]
    [InlineData(6, 4, "end-stored",     208, 0,  false)]   // the new end stored, the record armed
    [InlineData(6, 4, "end-stored",     208, 0,  true)]
    [InlineData(6, 4, "clear-unsealed", 208, 0,  false)]   // between disarming and its checksum
    [InlineData(6, 4, "clear-unsealed", 208, 0,  true)]
    [InlineData(6, 4, "cleared",        208, 0,  false)]   // disarmed, the end marker not planted
    [InlineData(2, 5, "chunk",          104, 30, false)]   // a tail longer than the prefix: three chunks
    [InlineData(2, 5, "chunk",          104, 30, true)]
    [InlineData(2, 5, "chunk-unsealed", 104, 0,  false)]
    [InlineData(2, 5, "chunk-unsealed", 104, 0,  true)]
    [InlineData(2, 5, "chunk",          208, 51, false)]   // inside the last, one byte short of it
    [InlineData(2, 5, "chunk",          260, 0,  false)]   // done == tail
    [InlineData(2, 5, "end-stored",     260, 0,  false)]
    [InlineData(2, 5, "cleared",        260, 0,  false)]
    public void A_process_killed_anywhere_inside_a_commits_relocation_loses_no_survivor(
        int committedEntries, int survivors, string point, long done, int intoNextChunk, bool rotHeader)
    {
        long tail = survivors * (long)V2Entry;
        string killed  = KilledRelocation(committedEntries, survivors, point, done, intoNextChunk);
        string walPath = Path.Combine(killed, "metrics.wal");
        if (rotHeader)
        {
            byte[] file = File.ReadAllBytes(walPath);
            BinaryPrimitives.WriteUInt64LittleEndian(file.AsSpan(16), 0xDEAD_BEEF_1234_5678UL);      // Generation
            File.WriteAllBytes(walPath, file);
        }

        double[] expected = Enumerable.Range(0, survivors).Select(static j => 200.0 + j).ToArray();
        var logger   = new CapturingLogger();
        var reopened = Open(logger, walPath);
        Assert.Equal(expected, reopened.ReadAll(out int unresolved).Select(static r => r.Point.Value));
        Assert.Equal(0, unresolved);
        Assert.Equal(tail, reopened.WrittenBytes);                                // finished, not just read past
        reopened.Dispose();
        Assert.Equal(rotHeader, logger.Entries.Any(static e => e.Level == LogLevel.Error && e.Text.Contains("header does not verify")));
        Assert.DoesNotContain(logger.Entries, static e => e.Text.Contains("cannot vouch for"));

        // And the finish is durable in the file: the next open has nothing to finish or rebuild.
        var quiet = new CapturingLogger();
        var again = Open(quiet, walPath);
        Assert.Equal(expected, again.ReadAll(out _).Select(static r => r.Point.Value));
        Assert.DoesNotContain(quiet.Entries, static e => e.Level >= LogLevel.Warning);
    }

    /// <summary>
    /// A DEAD PREFIX FAR SHORTER THAN THE TAIL BEHIND IT IS LEFT IN PLACE, NOT MOVED IN THOUSANDS OF
    /// SEALED CHUNKS. One committed entry before twenty survivors: moving them would take twenty
    /// one-entry chunks, each a header store and two checksums under both locks — at 8 MiB of tail,
    /// 160 000 of them. The commit leaves the log as it is (no relocation step fires, the length
    /// is unchanged), replay skips the dead entry, and the next commit reclaims everything.
    /// </summary>
    [Fact]
    public void A_dead_prefix_far_shorter_than_its_tail_is_left_in_place_until_the_next_commit()
    {
        long steps = 0;
        using (var wal = Open())
        {
            wal.Append([Gauge("cpu", 0, 100.0)]);
            ulong first = wal.BeginFlush();
            for (int j = 0; j < 20; j++) wal.Append([Gauge("cpu", 1 + j, 200.0 + j)]);

            wal.OnRelocationStepForTest = (_, _) => steps++;
            Assert.Equal(MetricWalCommit.Committed, wal.CommitFlush(first));
            Assert.Equal(21L * V2Entry, wal.WrittenBytes);                       // nothing moved
            Assert.Equal(0, steps);
        }

        using (var wal = Open())
        {
            Assert.Equal(Enumerable.Range(0, 20).Select(static j => 200.0 + j), wal.ReadAll(out _).Select(static r => r.Point.Value));
            Assert.Equal(MetricWalCommit.Committed, wal.CommitFlush(wal.BeginFlush()));
            Assert.Equal(0, wal.WrittenBytes);                                    // reclaimed with the rest
        }
    }

    /// <summary>
    /// Builds, in a directory of its own, the file a process killed inside a commit's relocation
    /// leaves: <paramref name="committedEntries"/> points (100…) flushed, <paramref name="survivors"/>
    /// (200…) appended during the flush, and the file copied at <paramref name="point"/> — a
    /// <see cref="MetricWriteAheadLog.RelocationStep"/> ("armed", "chunk", "end-stored", "cleared"),
    /// or the state between a covered store and its checksum ("armed-unsealed", "chunk-unsealed",
    /// "clear-unsealed") — with <paramref name="done"/> bytes moved; then <paramref name="intoNextChunk"/>
    /// bytes of the next chunk copied by hand the way a forward memmove leaves them. Returns the
    /// directory.
    /// </summary>
    private string KilledRelocation(int committedEntries, int survivors, string point, long done, int intoNextChunk,
                                    string name = "killed")
    {
        long prefix = committedEntries * (long)V2Entry, tail = survivors * (long)V2Entry;
        string source = Path.Combine(_dir, name + "-source.wal");
        byte[]? atStep = null, poolAtStep = null;
        void Take()
        {
            if (atStep is not null) return;
            atStep     = ReadShared(source);
            poolAtStep = ReadShared(source + ".pool");
        }

        using (var wal = Open(path: source))
        {
            for (int i = 0; i < committedEntries; i++) wal.Append([Gauge("cpu", i, 100 + i)]);
            ulong flushing = wal.BeginFlush();
            for (int j = 0; j < survivors; j++) wal.Append([Gauge("cpu", 50 + j, 200 + j)]);

            wal.OnRelocationStepForTest = (step, moved) =>
            {
                if (moved != done) return;
                if ((point, step) is ("armed", MetricWriteAheadLog.RelocationStep.Armed)
                                  or ("chunk", MetricWriteAheadLog.RelocationStep.Chunk)
                                  or ("end-stored", MetricWriteAheadLog.RelocationStep.EndStored)
                                  or ("cleared", MetricWriteAheadLog.RelocationStep.Cleared)) Take();
            };
            wal.OnCoveredStoreForTest = (field, value) =>
            {
                if ((point, field) is ("armed-unsealed", "MoveLength") && value != 0) Take();
                if ((point, field) is ("chunk-unsealed", "MoveDone") && (long)value == done) Take();
                if ((point, field) is ("clear-unsealed", "MoveLength") && value == 0) Take();
            };
            Assert.Equal(MetricWalCommit.Committed, wal.CommitFlush(flushing));
            wal.OnRelocationStepForTest = null;
            wal.OnCoveredStoreForTest   = null;
            Assert.Equal(tail, wal.WrittenBytes);                               // the live log finished it
        }
        Assert.True(atStep is not null, $"the kill point {point} at {done} was never reached");

        long chunk = Math.Min(prefix, tail - done);
        if (intoNextChunk > 0)
            Array.Copy(atStep!, FileHeader + prefix + done, atStep!, FileHeader + done, Math.Min(intoNextChunk, chunk));

        string killed = Path.Combine(_dir, name);
        Directory.CreateDirectory(killed);
        File.WriteAllBytes(Path.Combine(killed, "metrics.wal"), atStep!);
        File.WriteAllBytes(Path.Combine(killed, "metrics.wal.pool"), poolAtStep!);
        return killed;
    }

    /// <summary>
    /// A RELOCATION RECORD IN A HEADER THAT DOES NOT VERIFY IS ACTED ON ONLY IF IT DESCRIBES THE MOVE
    /// A COMMIT MADE. The kill of the three-chunk case, inside the second chunk, with the header's
    /// generation rotted as well, so the header fails. The record still matches what the commit left
    /// — the claim at the old end, <c>done</c> a chunk boundary — so the move is finished and every
    /// survivor replays. With <c>done</c> rotted too (not a chunk boundary) the record is not trusted:
    /// an Error says so, nothing is moved by it, and the walk keeps what the copy frontier left —
    /// the two survivors moved before it.
    /// </summary>
    [Theory]
    [InlineData(false, new[] { 200.0, 201.0, 202.0, 203.0, 204.0 })]
    [InlineData(true,  new[] { 200.0, 201.0 })]
    public void A_relocation_record_in_a_header_that_does_not_verify_must_match_the_move_it_names(bool rotDone, double[] expected)
    {
        string killed = KilledRelocation(committedEntries: 2, survivors: 5, "chunk", done: 2 * V2Entry, intoNextChunk: 30);
        string walPath = Path.Combine(killed, "metrics.wal");
        byte[] file = File.ReadAllBytes(walPath);
        BinaryPrimitives.WriteUInt64LittleEndian(file.AsSpan(16), 0xDEAD_BEEF_1234_5678UL);          // Generation: rot
        if (rotDone) BinaryPrimitives.WriteInt64LittleEndian(file.AsSpan(48), 50);                   // MoveDone: rot
        File.WriteAllBytes(walPath, file);

        var logger = new CapturingLogger();
        using (var wal = Open(logger, walPath))
            Assert.Equal(expected, wal.ReadAll(out _).Select(static r => r.Point.Value));
        Assert.Contains(logger.Entries, static e => e.Level == LogLevel.Error && e.Text.Contains("header does not verify"));
        Assert.Equal(rotDone, logger.Entries.Any(static e => e.Level == LogLevel.Error && e.Text.Contains("cannot vouch for")));
        Assert.Equal(2UL, BinaryPrimitives.ReadUInt64LittleEndian(File.ReadAllBytes(walPath).AsSpan(16)));   // rebuilt
    }

    /// <summary>
    /// AN OPEN KILLED WHILE IT FINISHES A RELOCATION UNDER A HEADER THAT DOES NOT VERIFY LEAVES IT
    /// NOT VERIFYING. The open redoes the move chunk by chunk, storing its progress; if those stores
    /// sealed the header, the rotted generation would be sealed with them, and the open after a
    /// second kill would find a header that verifies and skip the rebuild. The file is copied at the
    /// open's own relocation seam; opening that copy still rebuilds, and every survivor replays.
    /// </summary>
    [Fact]
    public void An_open_killed_while_it_finishes_a_relocation_under_a_rotted_header_still_rebuilds()
    {
        string killed = KilledRelocation(committedEntries: 2, survivors: 5, "chunk", done: 2 * V2Entry, intoNextChunk: 30);
        string walPath = Path.Combine(killed, "metrics.wal");
        byte[] file = File.ReadAllBytes(walPath);
        BinaryPrimitives.WriteUInt64LittleEndian(file.AsSpan(16), 0xDEAD_BEEF_1234_5678UL);
        File.WriteAllBytes(walPath, file);

        byte[]? midOpen = null, midOpenPool = null;
        MetricWriteAheadLog.t_relocationStepForNextOpenForTest = (_, _) =>
        {
            if (midOpen is not null) return;
            midOpen     = ReadShared(walPath);
            midOpenPool = ReadShared(walPath + ".pool");
        };
        try { Open(path: walPath).Dispose(); }
        finally { MetricWriteAheadLog.t_relocationStepForNextOpenForTest = null; }
        Assert.NotNull(midOpen);

        string twice = Path.Combine(_dir, "twice");
        Directory.CreateDirectory(twice);
        File.WriteAllBytes(Path.Combine(twice, "metrics.wal"), midOpen!);
        File.WriteAllBytes(Path.Combine(twice, "metrics.wal.pool"), midOpenPool!);

        var logger = new CapturingLogger();
        using (var wal = Open(logger, Path.Combine(twice, "metrics.wal")))
            Assert.Equal([200.0, 201.0, 202.0, 203.0, 204.0], wal.ReadAll(out _).Select(static r => r.Point.Value));
        Assert.Contains(logger.Entries, static e => e.Level == LogLevel.Error && e.Text.Contains("header does not verify"));
    }

    // ── The header's own checksum ────────────────────────────────────────────

    /// <summary>
    /// A HEADER THAT DOES NOT VERIFY HAS ITS COUNTERS REBUILT FROM THE ENTRIES, AND HIDES NONE OF
    /// THEM. The watermark is written into the header without its checksum. A ROTTED watermark at
    /// or above every entry's generation hid them all — replay skips what the watermark covers, and
    /// the crossed-header repair then raised the counter above it — acknowledged points dropped by
    /// eight bytes of header, silently; now the watermark is rebuilt below the oldest entry and all
    /// of them replay, with an Error. A rotted watermark BELOW the newest entry hides at most a
    /// prefix and is kept: a committed prefix still in the file stays dead and the survivors replay
    /// once. The next open finds a header that verifies. (A STOP between a watermark store and its
    /// checksum is not this: the header verifies by its pending checksum — the next fact.)
    /// </summary>
    [Theory]
    [InlineData(1_000UL, new[] { 1.0, 2.0, 3.0 })]   // rotted above every entry — rebuilt, all replay
    [InlineData(1UL,     new[] { 4.0, 5.0 })]        // rotted below the newest — kept, survivors only
    public void A_header_that_does_not_verify_has_its_counters_rebuilt_from_the_entries(ulong watermark, double[] expected)
    {
        using (var wal = Open())
        {
            wal.Append([Gauge("cpu", 0, 1.0), Gauge("cpu", 1, 2.0), Gauge("cpu", 2, 3.0)]);   // generation 1
            if (expected.Length == 2)
            {
                wal.BeginFlush();                                                          // generation 2 from here
                wal.Append([Gauge("cpu", 3, 4.0), Gauge("cpu", 4, 5.0)]);
            }
        }
        byte[] file = File.ReadAllBytes(WalPath);
        BinaryPrimitives.WriteUInt64LittleEndian(file.AsSpan(24), watermark);             // CommittedGeneration, unsealed
        File.WriteAllBytes(WalPath, file);

        var logger = new CapturingLogger();
        using (var wal = Open(logger))
            Assert.Equal(expected, wal.ReadAll(out _).Select(static r => r.Point.Value));
        Assert.Contains(logger.Entries, static e => e.Level == LogLevel.Error && e.Text.Contains("header does not verify"));

        var quiet = new CapturingLogger();
        using (var wal = Open(quiet))
            Assert.Equal(expected, wal.ReadAll(out _).Select(static r => r.Point.Value));
        Assert.DoesNotContain(quiet.Entries, static e => e.Level >= LogLevel.Warning);
    }

    /// <summary>
    /// A COMMIT KILLED BETWEEN ITS WATERMARK STORE AND THE HEADER'S CHECKSUM REPLAYS NOTHING IT
    /// FLUSHED. The usual periodic flush: nothing arrived while it wrote its files, so the commit
    /// stores the watermark over every entry in the log and then compacts it to nothing. The file
    /// is copied at the seam between the watermark store and the checksum that covers it. The
    /// header must verify — by the checksum written for its new state before the field — so the
    /// watermark stands and nothing replays. When the store came first and the seal after it, that
    /// header failed, the rebuild dropped a watermark covering every entry, and the whole flushed
    /// generation replayed beside the .mts files it had already been written to.
    /// </summary>
    [Fact]
    public void A_commit_killed_between_its_watermark_store_and_the_checksum_replays_nothing_it_flushed()
    {
        byte[]? atStore = null, poolAtStore = null;
        using (var wal = Open())
        {
            wal.Append([Gauge("cpu", 0, 1.0), Gauge("cpu", 1, 2.0), Gauge("cpu", 2, 3.0)]);
            ulong flushing = wal.BeginFlush();
            wal.OnCoveredStoreForTest = (field, _) =>
            {
                if (field != "Committed") return;
                atStore     = ReadShared(WalPath);
                poolAtStore = ReadShared(PoolPath);
            };
            Assert.Equal(MetricWalCommit.Committed, wal.CommitFlush(flushing));
            wal.OnCoveredStoreForTest = null;
        }
        Assert.NotNull(atStore);
        Assert.Equal(1UL, BinaryPrimitives.ReadUInt64LittleEndian(atStore.AsSpan(24)));       // the watermark landed
        Assert.Equal(3L * V2Entry, BinaryPrimitives.ReadInt64LittleEndian(atStore.AsSpan(8)) - FileHeader);

        string killed = Path.Combine(_dir, "killed");
        Directory.CreateDirectory(killed);
        File.WriteAllBytes(Path.Combine(killed, "metrics.wal"), atStore!);
        File.WriteAllBytes(Path.Combine(killed, "metrics.wal.pool"), poolAtStore!);

        var logger = new CapturingLogger();
        using (var wal = Open(logger, Path.Combine(killed, "metrics.wal")))
            Assert.Empty(wal.ReadAll(out _));
        Assert.DoesNotContain(logger.Entries, static e => e.Level >= LogLevel.Error);
    }

    // ── The pool ─────────────────────────────────────────────────────────────

    /// <summary>
    /// A POOL RECORD THAT FAILS ITS CHECKSUM IS SKIPPED, NOT DECODED — AND ONLY IT. One flipped bit
    /// inside "beta" would have decoded as a series named "bdta" with the real point under it — the
    /// pool's version of the garbage replay. Its points come back as UNRESOLVED instead; the
    /// records around it, before AND after, resolve as usual, because its framing held and the
    /// walk steps over it; the file is not cut (a Warning names the skip); and the index is not
    /// handed out again — the log's own entry still pins it, so "delta", registered after the
    /// restart, does not replay under beta's index. Ending the walk at the bad record, as the
    /// first cut of v2 did, left gamma unresolved too and cut it off the file at the next open.
    /// </summary>
    [Fact]
    public void A_pool_record_that_fails_its_checksum_is_skipped_and_the_records_after_it_resolve()
    {
        using (var wal = Open())
        {
            wal.Append([Gauge("alpha", 0, 1.0)]);
            wal.Append([Gauge("beta",  1, 2.0)]);
            wal.Append([Gauge("gamma", 2, 3.0)]);
        }
        byte[] pool = File.ReadAllBytes(PoolPath);
        long poolLength = pool.Length;
        int alphaEnd = 12 + (int)(BinaryPrimitives.ReadUInt32LittleEndian(pool.AsSpan(4)) & 0x00FF_FFFF);
        Assert.Equal((byte)'e', pool[alphaEnd + 12 + 1 + 2 + 1]);                // kind, u16 length, 'b', then 'e'
        Xor(PoolPath, alphaEnd + 12 + 1 + 2 + 1, 0x01);                          // "beta" → "bdta"

        var logger = new CapturingLogger();
        using (var wal = Open(logger))
        {
            Assert.Equal(poolLength, new FileInfo(PoolPath).Length);             // nothing cut
            wal.Append([Gauge("delta", 3, 4.0)]);                                // must not take beta's index
        }
        Assert.Contains(logger.Entries, static e => e.Level == LogLevel.Warning && e.Text.Contains("fail their checksum"));

        var replayed = Open().ReadAll(out int unresolved);
        Assert.Equal(1, unresolved);                                             // beta, honestly lost
        Assert.Equal(["alpha", "gamma", "delta"], replayed.Select(static r => r.Name));
        Assert.Equal([1.0, 3.0, 4.0], replayed.Select(static r => r.Point.Value));
    }

    // ── v1 still replays, and is upgraded ────────────────────────────────────

    /// <summary>
    /// A v1 LOG LEFT BY THE PREVIOUS RELEASE REPLAYS — every field, under the same watermark (the
    /// committed entry stays dead) — and is a v2 log from then on: a second open replays the same
    /// points, and a point appended after the upgrade survives, its NEW series recorded in the v2
    /// pool shape right behind the v1 records, both read back. Treating version 1 as unknown, the
    /// way a foreign file is, re-initialises it and replays nothing: every point the old process
    /// acknowledged and never flushed, silently gone.
    /// </summary>
    [Fact]
    public void A_v1_log_opens_replays_and_is_upgraded_in_place()
    {
        WriteV1Log();

        using (var wal = Open())
        {
            Assert.Equal((ushort)2, VersionOnDisk(WalPath));
            var first = wal.ReadAll(out int unresolved);
            AssertTheV1Points(first, unresolved);
            wal.Append([Gauge("fresh", 9, 9.0)]);
        }

        byte[] pool = File.ReadAllBytes(PoolPath);
        int v1Records = 8 + CpuBody.Length + 8 + LatencyBody.Length;
        Assert.Equal(0xC5, pool[v1Records + 7]);                                 // the new record is v2

        var again = Open().ReadAll(out int unresolvedAgain);
        Assert.Equal(0, unresolvedAgain);
        Assert.Equal([1.0, 2.5, 3.0, 4.0, 9.0], again.Select(static r => r.Point.Value));
        Assert.Equal("fresh", again[4].Name);
        Assert.False(File.Exists(TmpPath));
    }

    /// <summary>
    /// AN UPGRADE THAT CANNOT WRITE ITS COPY LEAVES THE v1 LOG EXACTLY AS IT WAS — and OPENS IT, as
    /// v1: it neither re-initialises the file to make the open succeed nor throws out of it, which
    /// would be a throw out of the metric engine's constructor. The copy is blocked by a directory
    /// squatting on its path (the full disk of production, reproducibly). Once the obstacle is gone
    /// the next open upgrades and replays everything.
    /// </summary>
    [Fact]
    public void A_failed_upgrade_copy_leaves_the_v1_log_untouched_and_opens_it_as_v1()
    {
        WriteV1Log();
        byte[] walBefore  = File.ReadAllBytes(WalPath);
        byte[] poolBefore = File.ReadAllBytes(PoolPath);
        Directory.CreateDirectory(TmpPath);

        var logger = new CapturingLogger();
        using (var wal = Open(logger))
        {
            var replayed = wal.ReadAll(out int unresolved);
            AssertTheV1Points(replayed, unresolved);
        }
        Assert.Contains(logger.Entries, static e => e.Level == LogLevel.Error && e.Text.Contains("could not be written"));

        Assert.Equal(walBefore,  File.ReadAllBytes(WalPath));                   // byte for byte
        Assert.Equal(poolBefore, File.ReadAllBytes(PoolPath));

        Directory.Delete(TmpPath);
        using (var wal = Open())
        {
            Assert.Equal((ushort)2, VersionOnDisk(WalPath));
            AssertTheV1Points(wal.ReadAll(out int unresolved), unresolved);
        }
    }

    /// <summary>
    /// THE SERVER COMES UP. The same blocked upgrade, one level up: the metric engine's constructor
    /// — the call a host start makes — succeeds, replays the v1 points into the hot tier, and takes
    /// new points.
    /// </summary>
    [Fact]
    public void An_engine_over_a_v1_log_that_cannot_be_upgraded_still_starts_and_replays_it()
    {
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        WriteLog(1, generation: 3, committed: 2,
                 [EntryBytes(new RawEntry(3, 0, now - 2_000_000_000L, 1.0), v2: false),
                  EntryBytes(new RawEntry(3, 0, now - 1_000_000_000L, 2.0), v2: false)],
                 [PoolRecord(0, CpuBody, v2: false)]);
        Directory.CreateDirectory(TmpPath);

        var engine = new MetricStorageEngine(_dir, NullLogger<MetricStorageEngine>.Instance);
        _engines.Add(engine);

        Assert.Equal(2, engine.HotPointCount);
        engine.Ingest([Gauge("cpu", 0, 3.0)]);
        Assert.Equal(3, engine.HotPointCount);
        Assert.Equal((ushort)1, VersionOnDisk(WalPath));
    }

    /// <summary>
    /// A MOVE THAT NEVER SUCCEEDS — the antivirus scanner that will not let go of the fresh copy —
    /// is retried a bounded number of times and then given up on: the log opens AS v1, in place,
    /// replays every live point, and takes appends in the v1 layout — entries AND pool records,
    /// which is what the release a rollback goes back to can read. The copy is removed and the
    /// failure logged as an Error. The next plain open upgrades, and the v1-era append comes along.
    /// </summary>
    [Fact]
    public void A_v1_log_whose_move_keeps_failing_opens_as_v1_and_loses_nothing()
    {
        WriteV1Log();
        int moves = 0, waits = 0;
        var io = new MetricWriteAheadLog.UpgradeIo
        {
            Move = (_, _) => { moves++; throw new IOException("The process cannot access the file because it is being used by another process."); },
            Wait = _ => waits++,
        };
        var logger = new CapturingLogger();

        using (var wal = OpenWith(io, logger))
        {
            Assert.Equal((ushort)1, VersionOnDisk(WalPath));
            AssertTheV1Points(wal.ReadAll(out int unresolved), unresolved);
            wal.Append([Gauge("fresh", 9, 9.0)]);
        }

        Assert.Equal(6, moves);                                                  // bounded: five pauses, six attempts
        Assert.Equal(5, waits);
        Assert.False(File.Exists(TmpPath));
        Assert.Contains(logger.Entries, static e => e.Level == LogLevel.Error && e.Error is IOException);

        // Appended as v1: a 48-byte entry, and a pool record a v1 reader can walk.
        byte[] wal1 = File.ReadAllBytes(WalPath);
        Assert.Equal(FileHeaderV1 + 5 * V1Entry + 4 * 8 + V1Entry, BinaryPrimitives.ReadInt64LittleEndian(wal1.AsSpan(8)));
        byte[] pool = File.ReadAllBytes(PoolPath);
        int v1Records = 8 + CpuBody.Length + 8 + LatencyBody.Length;
        Assert.Equal(0x00, pool[v1Records + 7]);

        // The next start, with the scanner gone: upgraded, and the point appended as v1 came along.
        using (var wal = Open())
        {
            Assert.Equal((ushort)2, VersionOnDisk(WalPath));
            Assert.Equal([1.0, 2.5, 3.0, 4.0, 9.0], wal.ReadAll(out _).Select(static r => r.Point.Value));
        }
    }

    /// <summary>
    /// A MOVE THAT FAILS TWICE AND THEN SUCCEEDS is an upgrade, not a v1 fallback: the retry is the
    /// point. No timer decides it — the seam's pause is a counter.
    /// </summary>
    [Fact]
    public void A_v1_log_whose_move_fails_transiently_is_upgraded_on_a_retry()
    {
        WriteV1Log();
        int moves = 0, waits = 0;
        var io = new MetricWriteAheadLog.UpgradeIo
        {
            Move = (from, to) =>
            {
                if (++moves <= 2) throw new UnauthorizedAccessException("Access to the path is denied.");
                File.Move(from, to, overwrite: true);
            },
            Wait = _ => waits++,
        };
        var logger = new CapturingLogger();

        using (var wal = OpenWith(io, logger))
        {
            Assert.Equal((ushort)2, VersionOnDisk(WalPath));
            AssertTheV1Points(wal.ReadAll(out int unresolved), unresolved);
        }

        Assert.Equal(3, moves);
        Assert.Equal(2, waits);
        Assert.False(File.Exists(TmpPath));
        Assert.DoesNotContain(logger.Entries, static e => e.Level >= LogLevel.Warning);
    }

    /// <summary>
    /// A STALE COPY from an upgrade that died before its move is removed. Beside a v2 log it is
    /// garbage that nothing else would ever delete; beside a v1 log the upgrade overwrites it.
    /// </summary>
    [Fact]
    public void A_stale_upgrade_copy_from_an_earlier_crash_is_removed()
    {
        using (var wal = Open()) wal.Append([Gauge("cpu", 0, 1.0)]);
        File.WriteAllBytes(TmpPath, new byte[4096]);

        Assert.Single(Open().ReadAll(out _));
        Assert.False(File.Exists(TmpPath));

        // …and beside a v1 log, a half-written copy does not stop the upgrade.
        foreach (var w in _wals) w.Dispose();
        WriteV1Log();
        File.WriteAllBytes(TmpPath, [1, 2, 3]);
        using (var wal = Open())
        {
            Assert.Equal((ushort)2, VersionOnDisk(WalPath));
            AssertTheV1Points(wal.ReadAll(out int unresolved), unresolved);
        }
        Assert.False(File.Exists(TmpPath));
    }

    /// <summary>
    /// THE UPGRADE'S COMMIT POINT REPLACES THE LOG, AND FAILS THE WAY ITS RETRY EXPECTS. On Windows
    /// the default move is MoveFileEx with MOVEFILE_WRITE_THROUGH — a durable rename, which
    /// File.Move cannot ask for; elsewhere it is File.Move and the directory is fsynced after it.
    /// Durability itself is not observable from a test; what is pinned is the contract the upgrade
    /// leans on: the file is replaced, a destination held open fails as IOException or
    /// UnauthorizedAccessException (the two the retry filters on, where a Win32Exception would
    /// escape it and fail the start), and the directory sync succeeds.
    /// </summary>
    [Fact]
    public void The_durable_move_replaces_the_log_and_fails_the_way_the_retry_expects()
    {
        string from = Path.Combine(_dir, "a.tmp"), to = Path.Combine(_dir, "b.wal");
        File.WriteAllBytes(from, [1, 2, 3]);
        File.WriteAllBytes(to,   [9]);

        MetricWriteAheadLog.DurableMove(from, to);
        Assert.Equal([1, 2, 3], File.ReadAllBytes(to));
        Assert.False(File.Exists(from));
        Assert.True(MetricWriteAheadLog.SyncDirectory(_dir));

        var missing = Record.Exception(() => MetricWriteAheadLog.DurableMove(from, to));
        Assert.True(missing is IOException, $"a missing source threw {missing?.GetType().Name}");

        if (OperatingSystem.IsWindows())
        {
            File.WriteAllBytes(from, [4]);
            using var held = new FileStream(to, FileMode.Open, FileAccess.Read, FileShare.None);
            var locked = Record.Exception(() => MetricWriteAheadLog.DurableMove(from, to));
            Assert.True(locked is IOException or UnauthorizedAccessException,
                $"a held destination threw {locked?.GetType().Name}");
        }
    }

    private sealed class CapturingLogger : ILogger
    {
        public readonly List<(LogLevel Level, string Text, Exception? Error)> Entries = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception,
                                Func<TState, Exception?, string> formatter)
        {
            lock (Entries) Entries.Add((logLevel, formatter(state, exception), exception));
        }
    }
}
