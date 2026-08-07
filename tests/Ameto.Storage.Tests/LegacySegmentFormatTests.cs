using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Ameto.Core;
using K4os.Compression.LZ4;
using MessagePack;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// v7 moves the index sections into the middle of the file and repurposes footer slot 0, and
/// the level split changes what a segment CONTAINS. Both are upgrade-in-place changes: an
/// existing install's segments were written by an older build, and
/// <c>StorageEngine.LoadSegmentCatalog</c> DELETES any <c>.seg</c> it cannot open. So "the
/// reader still accepts v4-v6" is not a nicety here — it is the only thing between a deploy
/// and a wiped data directory.
///
/// <para>These tests build v4, v5 and v6 files byte by byte, in the exact on-disk shape those
/// writers produced (the block payload has been unchanged since v4; what differs is the
/// block-index stride and what its second slot means), and drive the CURRENT reader, merge and
/// writer over them. A round trip through the current <see cref="SegmentWriter"/> would prove
/// nothing — it can only emit v7.</para>
/// </summary>
public sealed class LegacySegmentFormatTests : IDisposable
{
    private const int Rows          = 300;
    private const int RowsPerBlock  = 50;    // ⇒ 6 blocks, so the block-index STRIDE is load-bearing

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-legacy-" + Guid.NewGuid().ToString("N"));
    private readonly long   _base = new DateTimeOffset(2026, 6, 1, 0, 0, 0, TimeSpan.Zero).UtcTicks;

    public LegacySegmentFormatTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    // ── 1. Every version this build claims to support actually opens ──────────

    [Theory]
    [InlineData((ushort)4)]
    [InlineData((ushort)5)]
    [InlineData((ushort)6)]
    public void ALegacySegmentOpensAndReturnsEveryEventUnchanged(ushort version)
    {
        var rows = BuildRows(idBase: 1000, tsStep: TimeSpan.TicksPerSecond);
        string path = WriteLegacySegment(version, rows, segId: 42);

        using var reader = SegmentReader.Open(path, computeUncompressedBytes: true);

        Assert.Equal(42UL,          reader.Info.Id.Value);
        Assert.Equal((uint)Rows,    reader.Info.EventCount);
        Assert.Equal(rows[0].Ticks, reader.Info.MinTimestampTicks);
        Assert.Equal(rows[^1].Ticks, reader.Info.MaxTimestampTicks);
        Assert.Equal(LogLevel.Debug, reader.Info.MinLevel);   // the rows cycle Debug..Error

        var read = Drain(reader.ReadEventsAsync(null, null, null, false, default));
        Assert.Equal(Rows, read.Count);
        for (int i = 0; i < Rows; i++)
        {
            var e = rows[i];
            var a = read[i];
            Assert.Equal(e.Id,       a.Id.RawValue);
            Assert.Equal(e.Ticks,    a.Timestamp.UtcTicks);
            Assert.Equal(e.Level,    a.Level);
            Assert.Equal(e.Template, a.MessageTemplate);
            Assert.Equal(e.Service,  a.ServiceName);
            Assert.Equal(e.TraceHi,  a.TraceIdHi);
            Assert.Equal(e.SpanId,   a.SpanId);
            Assert.True(e.Props.AsSpan().SequenceEqual(a.RawProperties.Span), $"properties differ at row {i}");
            Assert.Equal(e.Exception?.Type, a.Exception?.Type);
        }
    }

    /// <summary>
    /// A pre-v7 file must present as ONE implicit group whose section offsets are the footer's
    /// — that synthetic entry is the only thing that lets the group-shaped prefilter read an
    /// old file at all, and a group whose bounds were wrong would drop rows silently rather
    /// than fail.
    /// </summary>
    [Theory]
    [InlineData((ushort)4)]
    [InlineData((ushort)5)]
    [InlineData((ushort)6)]
    public void ALegacySegmentIsOneImplicitGroupCoveringTheWholeFile(ushort version)
    {
        var rows = BuildRows(idBase: 7000, tsStep: TimeSpan.TicksPerSecond);
        string path = WriteLegacySegment(version, rows, segId: 43);

        using var reader = SegmentReader.Open(path);
        Assert.Equal(1, reader.Groups.Length);

        var g = reader.Groups[0];
        Assert.Equal(0,             g.FirstBlock);
        Assert.Equal(0u,            g.FirstOrdinal);
        Assert.Equal((uint)Rows,    g.EventCount);
        Assert.Equal(reader.Info.MinTimestampTicks, g.MinTs);
        Assert.Equal(reader.Info.MaxTimestampTicks, g.MaxTs);
        Assert.True(g.BlockCount >= Rows / RowsPerBlock, $"{g.BlockCount} blocks for {Rows} rows");

        // The sections have to come back byte-for-byte: the footer slots v7 repurposes are
        // where an old file keeps them.
        Assert.Equal(InvertedStub, reader.ReadInvertedIndexBytes());
        Assert.Equal(TrigramStub,  reader.ReadTrigramIndexBytes());
        Assert.Equal(BloomStub,    reader.ReadBloomFilterBytes());
    }

    /// <summary>
    /// A time window over a pre-v6 file has no zone map to prune with, so every block is
    /// decoded and the per-event check decides. The result must still equal a brute-force
    /// scan — a v6 skip rule leaking onto a file whose block index holds FirstEventId in that
    /// slot would silently drop whole blocks.
    /// </summary>
    [Theory]
    [InlineData((ushort)4)]
    [InlineData((ushort)5)]
    [InlineData((ushort)6)]
    public void WindowedReadsOverALegacySegmentMatchABruteForceScan(ushort version)
    {
        var rows = BuildRows(idBase: 9000, tsStep: TimeSpan.TicksPerSecond);
        string path = WriteLegacySegment(version, rows, segId: 44);

        (int From, int To)[] windows = [(0, 0), (0, Rows - 1), (49, 51), (120, 200), (Rows - 1, Rows - 1)];
        foreach (var (from, to) in windows)
        {
            long f = rows[from].Ticks, t = rows[to].Ticks;
            var expect = rows.Where(r => r.Ticks >= f && r.Ticks <= t).Select(r => r.Id).ToList();

            using var reader = SegmentReader.Open(path);
            var got = Drain(reader.ReadEventsAsync(
                    null, new DateTimeOffset(f, TimeSpan.Zero), new DateTimeOffset(t, TimeSpan.Zero), false, default))
                .Select(e => e.Id.RawValue).ToList();
            Assert.Equal(expect, got);
        }
    }

    /// <summary>
    /// v5 posting lists are file ordinals, so candidates select rows. v4's are not, and the
    /// reader has no block→ordinal map for a v4 file — so candidates must be IGNORED and the
    /// whole segment scanned. A future "optimisation" that applied them anyway would return a
    /// subset of the matching rows with no error anywhere, which is why this is asserted
    /// rather than left to the comment in <c>SegmentReader</c>.
    /// </summary>
    [Fact]
    public void CandidateOrdinalsNarrowAV5SegmentAndAreIgnoredForV4()
    {
        var rows = BuildRows(idBase: 11_000, tsStep: TimeSpan.TicksPerSecond);
        uint[] cands = [0, 1, 137, (uint)(Rows - 1)];

        using (var v5 = SegmentReader.Open(WriteLegacySegment(5, rows, 45, "v5-cand.seg")))
        {
            var got = Drain(v5.ReadEventsAsync(cands, null, null, false, default));
            Assert.Equal(cands.Length, got.Count);
            for (int i = 0; i < cands.Length; i++)
                Assert.Equal(rows[(int)cands[i]].Id, got[i].Id.RawValue);
        }

        using (var v4 = SegmentReader.Open(WriteLegacySegment(4, rows, 46, "v4-cand.seg")))
        {
            var got = Drain(v4.ReadEventsAsync(cands, null, null, false, default));
            Assert.Equal(Rows, got.Count);
        }
    }

    // ── 2. The footer stays 44 bytes in every version this build can meet ─────

    /// <summary>
    /// The reader parses the footer BEFORE it knows the version — it seeks to
    /// <c>length - 44</c> and reads the magic at <c>+40</c> — so 44 is a cross-format
    /// constant, not a v7 detail. Checked on every version this build can encounter,
    /// v7 included, from the file rather than from the writer's constants.
    /// </summary>
    [Fact]
    public void TheFooterIsFortyFourBytesInEveryVersionThisBuildCanOpen()
    {
        var rows  = BuildRows(idBase: 13_000, tsStep: TimeSpan.TicksPerSecond);
        var paths = new List<string>
        {
            WriteLegacySegment(4, rows, 47, "foot-v4.seg"),
            WriteLegacySegment(5, rows, 48, "foot-v5.seg"),
            WriteLegacySegment(6, rows, 49, "foot-v6.seg"),
            WriteCurrentSegment(rows, 50, "foot-v7.seg"),
        };

        foreach (var path in paths)
        {
            var bytes = File.ReadAllBytes(path);
            ushort version = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(4));

            // Magic sits at footerStart + 40, i.e. the last four bytes of a 44-byte footer.
            Assert.Equal(0x52_44_46_54u, BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(bytes.Length - 4)));
            // Slot 4 is the footer's own offset — it must name length - 44 exactly.
            long footerOffset = BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(bytes.Length - 12));
            Assert.Equal(bytes.Length - 44, footerOffset);
            Assert.InRange(version, (ushort)4, (ushort)7);
        }
    }

    // ── 3. Mixed-level old segments ──────────────────────────────────────────

    /// <summary>
    /// Everything written before the level split is MIXED-LEVEL, and its header MinLevel is
    /// the lowest severity VALUE inside it. Retention (<c>MaxTimestamp + Ttl(MinLevel)</c>)
    /// and the merge planner's bucket key both read that field, so a build that quietly
    /// assumed one level per segment would mis-file every old file. Nothing may filter a
    /// segment's ROWS by it.
    /// </summary>
    [Fact]
    public void AMixedLevelLegacySegmentKeepsEveryLevelAndReportsTheLowestValue()
    {
        var rows = BuildRows(idBase: 15_000, tsStep: TimeSpan.TicksPerSecond);
        string path = WriteLegacySegment(5, rows, segId: 51);

        using var reader = SegmentReader.Open(path);
        Assert.Equal(LogLevel.Debug, reader.Info.MinLevel);

        var byLevel = Drain(reader.ReadEventsAsync(null, null, null, false, default))
            .GroupBy(e => e.Level).ToDictionary(g => g.Key, g => g.Count());
        Assert.Equal(4, byLevel.Count);
        foreach (var lvl in new[] { LogLevel.Debug, LogLevel.Information, LogLevel.Warning, LogLevel.Error })
            Assert.True(byLevel.GetValueOrDefault(lvl) > 0, $"{lvl} rows disappeared");

        // Expiry is a pure function of (MaxTimestamp, MinLevel) and this build does not
        // change it — the mixed segment keeps exactly the deadline it had before the upgrade.
        var policy = RetentionPolicy.Default;
        var info   = reader.Info;
        var deadline = info.MaxTimestamp.Add(policy.GetTtl(info.MinLevel));
        Assert.False(info.IsExpired(policy, deadline.AddTicks(-1)));
        Assert.True(info.IsExpired(policy, deadline.AddTicks(1)));
    }

    // ── 4. Compaction consumes a mixed-level legacy file ─────────────────────

    /// <summary>
    /// The merge planner selects on catalog metadata alone — id, payload size, MinLevel,
    /// timestamps — and nothing in it looks at the format version, so an old v4/v5 file WILL
    /// be picked as a source for a v7 output. This drives that path end to end: two
    /// mixed-level legacy files of different versions in, one v7 file out, every row present
    /// and byte-identical, and the output's MinLevel equal to the sources' minimum (which is
    /// what keeps its retention deadline the same as theirs).
    /// </summary>
    [Fact]
    public void AMixedLevelV4AndV5MergeIntoAV7FileWithoutLosingARow()
    {
        var a = BuildRows(idBase: 20_000, tsStep: TimeSpan.TicksPerSecond);
        var b = BuildRows(idBase: 40_000, tsStep: TimeSpan.TicksPerSecond, tsOffset: TimeSpan.TicksPerSecond / 2);
        string pa = WriteLegacySegment(4, a, 60, "merge-v4.seg");
        string pb = WriteLegacySegment(5, b, 61, "merge-v5.seg");

        string outPath = Path.Combine(_dir, "0-62-0-0.seg");
        using (var source = MergingSegmentEventSource.Open([pa, pb]))
        using (var writer = new SegmentWriter(outPath, SegmentWriter.DefaultGroupPayloadBudgetBytes,
                                              SegmentCompression.High))
        {
            writer.WriteEvents(source, null);
            var info = writer.Finalise(new NodeId(0), new SegmentId(62));
            Assert.Equal((uint)(a.Count + b.Count), info.EventCount);
            Assert.Equal(LogLevel.Debug, info.MinLevel);
        }

        Assert.Equal(7, BinaryPrimitives.ReadUInt16LittleEndian(File.ReadAllBytes(outPath).AsSpan(4)));

        var expected = a.Concat(b)
            .OrderBy(r => r.Ticks).ThenBy(r => r.Id)
            .ToList();

        using var reader = SegmentReader.Open(outPath);
        var read = Drain(reader.ReadEventsAsync(null, null, null, false, default));
        Assert.Equal(expected.Count, read.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Id,       read[i].Id.RawValue);
            Assert.Equal(expected[i].Ticks,    read[i].Timestamp.UtcTicks);
            Assert.Equal(expected[i].Level,    read[i].Level);
            Assert.Equal(expected[i].Template, read[i].MessageTemplate);
            Assert.Equal(expected[i].Service,  read[i].ServiceName);
            Assert.True(expected[i].Props.AsSpan().SequenceEqual(read[i].RawProperties.Span),
                $"properties differ at merged row {i}");
            Assert.Equal(expected[i].Exception?.Type, read[i].Exception?.Type);
        }
    }

    /// <summary>An unsupported version must be REPORTED, not guessed at — the catalog deletes
    /// what it cannot open, so a silent mis-parse would be worse than the exception.</summary>
    [Theory]
    [InlineData((ushort)3)]
    [InlineData((ushort)8)]
    public void AnUnsupportedVersionIsRejected(ushort version)
    {
        var rows = BuildRows(idBase: 30_000, tsStep: TimeSpan.TicksPerSecond);
        string path = WriteLegacySegment(5, rows, 70, $"bad-{version}.seg");
        using (var fs = new FileStream(path, FileMode.Open, FileAccess.Write))
        {
            Span<byte> v = stackalloc byte[2];
            BinaryPrimitives.WriteUInt16LittleEndian(v, version);
            fs.Seek(4, SeekOrigin.Begin);
            fs.Write(v);
        }
        Assert.Throws<InvalidDataException>(() => SegmentReader.Open(path));
    }

    // ── Fixtures ─────────────────────────────────────────────────────────────

    private sealed record Row(
        ulong Id, long Ticks, LogLevel Level, string Template, string Service,
        ulong TraceHi, ulong TraceLo, ulong SpanId, byte[] Props, ExceptionInfo? Exception);

    private static readonly byte[] InvertedStub = Encoding.UTF8.GetBytes("legacy-inverted-section");
    private static readonly byte[] TrigramStub  = Encoding.UTF8.GetBytes("legacy-trigram-section-longer");
    private static readonly byte[] BloomStub    = Encoding.UTF8.GetBytes("legacy-bloom");

    private List<Row> BuildRows(ulong idBase, long tsStep, long tsOffset = 0)
    {
        var levels = new[] { LogLevel.Debug, LogLevel.Information, LogLevel.Warning, LogLevel.Error };
        var rows   = new List<Row>(Rows);
        for (int i = 0; i < Rows; i++)
        {
            var w = new ArrayBufferWriter<byte>(512);
            var mp = new MessagePackWriter(w);
            mp.WriteMapHeader(2);
            mp.Write("n");   mp.Write((long)i);
            mp.Write("pad"); mp.Write(new string((char)('a' + i % 26), 380));
            mp.Flush();

            rows.Add(new Row(
                Id:        idBase + (ulong)i,
                Ticks:     _base + tsOffset + i * tsStep,
                Level:     levels[i % levels.Length],
                Template:  "legacy event {n} on {service}",
                Service:   i % 3 == 0 ? "Svc.Alpha" : "Svc.Beta",
                TraceHi:   0x1122334455667788UL + (ulong)i,
                TraceLo:   0x99AABBCCDDEEFF00UL + (ulong)i,
                SpanId:    0xFEEDFACE00000000UL + (ulong)i,
                Props:     w.WrittenSpan.ToArray(),
                Exception: i % 7 == 0
                    ? new ExceptionInfo { Type = "System.InvalidOperationException", Message = $"boom {i}" }
                    : null));
        }
        return rows;
    }

    /// <summary>Writes <paramref name="rows"/> through the CURRENT writer (v7), for contrast.</summary>
    private string WriteCurrentSegment(List<Row> rows, ulong segId, string name)
    {
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(rows.Count + 1, (long)rows.Count * 1024 + (8L << 20));
        foreach (var r in rows)
        {
            int tmpl = pool.Intern(r.Template);
            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = r.Id,
                TimestampUtcTicks        = r.Ticks,
                Level                    = r.Level,
                MessageTemplatePoolIndex = tmpl,
                ServiceNamePoolIndex     = pool.Intern(r.Service),
                TraceIdHi                = r.TraceHi,
                TraceIdLo                = r.TraceLo,
                SpanId                   = r.SpanId,
            }, r.Props, r.Template, r.Exception));
        }
        hot.Freeze();

        string path = Path.Combine(_dir, name);
        using var writer = new SegmentWriter(path);
        writer.WriteEvents(hot, pool);
        writer.Finalise(new NodeId(0), new SegmentId(segId));
        return path;
    }

    /// <summary>
    /// Writes a segment in the on-disk shape of <paramref name="version"/>.
    ///
    /// <para>The BLOCK payload is identical in v4-v7 (nine columns since v4), so it is written
    /// once. What the version changes is the block-index entry:</para>
    /// <list type="bullet">
    ///   <item>v4 — 16 B: <c>int64 offset, uint64 firstEventId</c>.</item>
    ///   <item>v5 — 20 B: the same plus <c>uint32 firstOrdinal</c>.</item>
    ///   <item>v6 — 20 B: <c>int64 offset, int64 blockMinTs, uint32 firstOrdinal</c> — the
    ///         second slot becomes the time zone map.</item>
    /// </list>
    /// <para>The footer is 44 B in all of them and names the three FILE-level sections in
    /// slots 0-2, which is exactly what v7 reuses for its group directory.</para>
    /// </summary>
    private string WriteLegacySegment(ushort version, List<Row> rows, ulong segId, string? name = null)
    {
        string path = Path.Combine(_dir, name ?? $"0-{segId}-0-0-v{version}.seg");
        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None);
        using var bw = new BinaryWriter(fs);

        const int headerSize = 46;
        fs.Seek(headerSize, SeekOrigin.Begin);

        var blocks   = new List<(long Offset, ulong FirstId, long MinTs, uint FirstOrdinal)>();
        uint ordinal = 0;
        for (int start = 0; start < rows.Count; start += RowsPerBlock)
        {
            int count = Math.Min(RowsPerBlock, rows.Count - start);
            var slice = rows.GetRange(start, count);
            byte[] raw = EncodeBlock(slice);

            byte[] comp = new byte[LZ4Codec.MaximumOutputSize(raw.Length)];
            int compLen = LZ4Codec.Encode(raw, 0, raw.Length, comp, 0, comp.Length, LZ4Level.L00_FAST);

            blocks.Add((fs.Position, slice.Min(r => r.Id), slice.Min(r => r.Ticks), ordinal));
            bw.Write((uint)raw.Length);
            bw.Write((uint)compLen);
            bw.Write(comp, 0, compLen);
            ordinal += (uint)count;
        }

        long invOff = fs.Position; bw.Write((uint)InvertedStub.Length); bw.Write(InvertedStub);
        long triOff = fs.Position; bw.Write((uint)TrigramStub.Length);  bw.Write(TrigramStub);
        long bloOff = fs.Position; bw.Write((uint)BloomStub.Length);    bw.Write(BloomStub);

        long blockIdxOff = fs.Position;
        bw.Write((uint)blocks.Count);
        foreach (var (offset, firstId, minTs, firstOrdinal) in blocks)
        {
            bw.Write(offset);
            if (version >= 6) bw.Write(minTs); else bw.Write(firstId);
            if (version >= 5) bw.Write(firstOrdinal);
        }

        long footerOffset = fs.Position;
        bw.Write(invOff);
        bw.Write(triOff);
        bw.Write(bloOff);
        bw.Write(blockIdxOff);
        bw.Write(footerOffset);
        bw.Write(0x52_44_46_54u);   // "RDFT"

        fs.Seek(0, SeekOrigin.Begin);
        bw.Write(0x52_44_4C_47u);                              // "RDLG"
        bw.Write(version);
        bw.Write(0u);                                          // nodeId
        bw.Write(segId);
        bw.Write(rows.Min(r => r.Ticks));
        bw.Write(rows.Max(r => r.Ticks));
        bw.Write((uint)rows.Count);
        bw.Write((byte)rows.Min(r => (byte)r.Level));          // MinLevel
        bw.Write((byte)0x01);                                  // FlagCompressed
        bw.Write((byte)0);
        bw.Write((byte)0);
        return path;
    }

    /// <summary>
    /// The uncompressed columnar block, unchanged since v4:
    /// <c>uint32 n, int64 minTs, uint64 minId, byte colCount</c> then
    /// <c>{ byte id, uint32 byteLen, bytes }</c> for columns 1-9.
    /// </summary>
    private static byte[] EncodeBlock(List<Row> rows)
    {
        int n = rows.Count;
        long  minTs = rows.Min(r => r.Ticks);
        ulong minId = rows.Min(r => r.Id);

        using var blk = new MemoryStream(64 * 1024);
        WriteU32(blk, (uint)n);
        WriteI64(blk, minTs);
        WriteU64(blk, minId);
        blk.WriteByte(9);

        var ts = new byte[n * 8];
        var lv = new byte[n];
        var id = new byte[n * 8];
        var tr = new byte[n * 16];
        var sp = new byte[n * 8];
        for (int i = 0; i < n; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian (ts.AsSpan(i * 8),      rows[i].Ticks - minTs);
            lv[i] = (byte)rows[i].Level;
            BinaryPrimitives.WriteUInt64LittleEndian(id.AsSpan(i * 8),      rows[i].Id - minId);
            BinaryPrimitives.WriteUInt64LittleEndian(tr.AsSpan(i * 16),     rows[i].TraceHi);
            BinaryPrimitives.WriteUInt64LittleEndian(tr.AsSpan(i * 16 + 8), rows[i].TraceLo);
            BinaryPrimitives.WriteUInt64LittleEndian(sp.AsSpan(i * 8),      rows[i].SpanId);
        }

        WriteColumn(blk, 1, ts);
        WriteColumn(blk, 2, lv);
        WriteColumn(blk, 3, id);
        WriteStringColumn(blk, 4, rows.Select(r => Encoding.UTF8.GetBytes(r.Template)).ToList());
        WriteStringColumn(blk, 5, rows.Select(r => r.Exception?.ToBytes() ?? []).ToList());
        WriteStringColumn(blk, 6, rows.Select(r => r.Props).ToList());
        WriteColumn(blk, 7, tr);
        WriteColumn(blk, 8, sp);
        WriteStringColumn(blk, 9, rows.Select(r => Encoding.UTF8.GetBytes(r.Service)).ToList());
        return blk.ToArray();
    }

    private static void WriteColumn(MemoryStream dst, byte id, byte[] payload)
    {
        dst.WriteByte(id);
        WriteU32(dst, (uint)payload.Length);
        dst.Write(payload, 0, payload.Length);
    }

    /// <summary>A string column: <c>(n+1)</c> uint32 offsets, then the concatenated payloads.</summary>
    private static void WriteStringColumn(MemoryStream dst, byte id, List<byte[]> values)
    {
        int total = values.Sum(v => v.Length);
        dst.WriteByte(id);
        WriteU32(dst, (uint)((values.Count + 1) * 4 + total));

        uint off = 0;
        foreach (var v in values) { WriteU32(dst, off); off += (uint)v.Length; }
        WriteU32(dst, off);
        foreach (var v in values) dst.Write(v, 0, v.Length);
    }

    private static void WriteU32(MemoryStream s, uint v)
    { Span<byte> b = stackalloc byte[4]; BinaryPrimitives.WriteUInt32LittleEndian(b, v); s.Write(b); }

    private static void WriteI64(MemoryStream s, long v)
    { Span<byte> b = stackalloc byte[8]; BinaryPrimitives.WriteInt64LittleEndian(b, v); s.Write(b); }

    private static void WriteU64(MemoryStream s, ulong v)
    { Span<byte> b = stackalloc byte[8]; BinaryPrimitives.WriteUInt64LittleEndian(b, v); s.Write(b); }

    private static List<LogEvent> Drain(IAsyncEnumerable<LogEvent> src)
    {
        var list = new List<LogEvent>();
        var e = src.GetAsyncEnumerator();
        try { while (e.MoveNextAsync().GetAwaiter().GetResult()) list.Add(e.Current); }
        finally { e.DisposeAsync().GetAwaiter().GetResult(); }
        return list;
    }
}
