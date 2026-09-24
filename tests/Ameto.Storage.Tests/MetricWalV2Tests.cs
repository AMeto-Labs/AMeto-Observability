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

    private const int  FileHeader = 32;
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
        var file = new byte[FileHeader + Capacity];
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(0), 0x52_44_4D_57);          // "RDMW"
        BinaryPrimitives.WriteUInt16LittleEndian(file.AsSpan(4), version);
        BinaryPrimitives.WriteInt64LittleEndian (file.AsSpan(8), FileHeader + written);
        BinaryPrimitives.WriteUInt64LittleEndian(file.AsSpan(16), generation);
        BinaryPrimitives.WriteUInt64LittleEndian(file.AsSpan(24), committed);
        long at = FileHeader;
        foreach (var e in entries) { e.CopyTo(file, at); at += e.Length; }
        File.WriteAllBytes(WalPath, file);
        File.WriteAllBytes(PoolPath, poolRecords.SelectMany(static r => r).ToArray());
    }

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
    /// INCIDENT #56's HEAD DOES NOT REPLAY. Reconstructed from the stand's forensics (the bytes
    /// themselves were not kept in the repo): header generation 3 705 over watermark 3 704, the
    /// first entry torn — its bucket count reading 8, so the stride lands on an all-zero slot at
    /// the old byte 112 — and series 0 registered in the pool. v1 caught the incident's own
    /// generation (≈155e9) with the margin; the residual it could not catch is the same tear
    /// with a PLAUSIBLE generation, here the header's own. Its timestamp is the year-2116 value
    /// the incident minted immortal files with, or — the case no timestamp guard can see — a
    /// sane one, and its value is garbage. v2 replays neither, because its checksum slot holds
    /// what a tear leaves there; the engine built over it comes up with an empty tier.
    /// </summary>
    [Theory]
    [InlineData(true)]                                // the incident's year-2116 MaxNano
    [InlineData(false)]                               // a minute ago: invisible to every guard but the CRC
    public async Task The_incident_56_head_replays_no_point(bool year2116)
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
    /// What the same head did in v1 — and still does in a log that is v1 on disk: a v1 entry
    /// carries no checksum, so the torn head replays as a point with a garbage value. This is the
    /// fact the v2 facts above are the fix for, kept so that fix is measured against the old
    /// behaviour rather than asserted into existence.
    /// </summary>
    [Fact]
    public void The_same_head_in_a_v1_log_replays_as_a_garbage_point()
    {
        double garbage = BitConverter.Int64BitsToDouble(0x4B3F_A97A_8424_3FA9);
        WriteLog(1, generation: 3_705, committed: 3_704,
                 [EntryBytes(new RawEntry(3_705, 0, BaseNano, garbage, Buckets: new long[8]), v2: false)],
                 [PoolRecord(0, PoolBody("process_memory_usage", MetricKind.Gauge, "By", [], null), v2: false)]);

        var replayed = Open().ReadAll(out _);
        Assert.Equal(garbage, Assert.Single(replayed).Point.Value);
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

    // ── The pool ─────────────────────────────────────────────────────────────

    /// <summary>
    /// A POOL RECORD THAT FAILS ITS CHECKSUM IS NOT DECODED INTO A SERIES. One flipped bit inside
    /// "beta" would have decoded as a series named "bdta" with the real point under it — the pool's
    /// version of the garbage replay. Its points come back as UNRESOLVED instead, the pool is cut
    /// back to the last record that verifies (so the next record appends on a clean boundary), and
    /// the index is not handed out again: the log's own entry still pins it.
    /// </summary>
    [Fact]
    public void A_pool_record_that_fails_its_checksum_is_not_decoded_into_a_series()
    {
        using (var wal = Open())
        {
            wal.Append([Gauge("alpha", 0, 1.0)]);
            wal.Append([Gauge("beta", 1, 2.0)]);
        }
        byte[] pool = File.ReadAllBytes(PoolPath);
        int alphaEnd = 12 + (int)(BinaryPrimitives.ReadUInt32LittleEndian(pool.AsSpan(4)) & 0x00FF_FFFF);
        Assert.Equal((byte)'e', pool[alphaEnd + 12 + 1 + 2 + 1]);                // kind, u16 length, 'b', then 'e'
        Xor(PoolPath, alphaEnd + 12 + 1 + 2 + 1, 0x01);                          // "beta" → "bdta"

        using (var wal = Open())
        {
            Assert.Equal(alphaEnd, new FileInfo(PoolPath).Length);               // cut to the last verified record
            wal.Append([Gauge("charlie", 2, 3.0)]);                              // must not take beta's index
        }

        var replayed = Open().ReadAll(out int unresolved);
        Assert.Equal(1, unresolved);                                             // beta, honestly lost
        Assert.Equal(["alpha", "charlie"], replayed.Select(static r => r.Name));
        Assert.Equal([1.0, 3.0], replayed.Select(static r => r.Point.Value));
    }

    // ── v1 still replays ─────────────────────────────────────────────────────

    /// <summary>
    /// A v1 LOG LEFT BY THE PREVIOUS RELEASE REPLAYS — every field, under the same watermark (the
    /// committed entry stays dead) — and stays v1: a point appended to it goes down as a 48-byte
    /// entry with no checksum and its new series as a v1 pool record, both of which the release a
    /// rollback goes back to can read, and a second open replays all of it. Treating version 1 as
    /// unknown, the way a foreign file is, re-initialises it and replays nothing: every point the
    /// old process acknowledged and never flushed, silently gone.
    /// </summary>
    [Fact]
    public void A_v1_log_opens_replays_and_keeps_its_own_layout()
    {
        WriteV1Log();

        using (var wal = Open())
        {
            AssertTheV1Points(wal.ReadAll(out int unresolved), unresolved);
            wal.Append([Gauge("fresh", 9, 9.0)]);
        }

        byte[] file = File.ReadAllBytes(WalPath);
        Assert.Equal((ushort)1, VersionOnDisk(WalPath));
        Assert.Equal(FileHeader + 5 * V1Entry + 4 * 8 + V1Entry, BinaryPrimitives.ReadInt64LittleEndian(file.AsSpan(8)));
        byte[] pool = File.ReadAllBytes(PoolPath);
        int v1Records = 8 + CpuBody.Length + 8 + LatencyBody.Length;
        Assert.Equal(0x00, pool[v1Records + 7]);                                 // the new record is v1 too

        var again = Open().ReadAll(out int unresolvedAgain);
        Assert.Equal(0, unresolvedAgain);
        Assert.Equal([1.0, 2.5, 3.0, 4.0, 9.0], again.Select(static r => r.Point.Value));
        Assert.Equal("fresh", again[4].Name);
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
