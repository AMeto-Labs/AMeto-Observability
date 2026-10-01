using System.Buffers.Binary;
using System.Text;
using Ameto.Core;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Microsoft.Extensions.Logging;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Ameto.Storage.Tests;

/// <summary>
/// SPAN WAL v3: a commit's move of the surviving tail is recorded in the header and finished at the
/// next open (#103), and the v2 and v1 logs earlier releases left behind still replay.
///
/// <para>v2's commit moved the spans appended during a flush to the front of the log with one
/// <c>Buffer.MemoryCopy</c>. A process killed inside it left the copies up to the copy front, one
/// entry torn by the front and the untouched originals behind it — under the old header, whose
/// claim covered all of it. The replay stopped at the torn entry, truncated the log there, and the
/// originals behind the front — spans acknowledged to the exporter and in no segment — were gone.
/// Every fact below copies the file at a point a process can die at and opens the copy; all judged
/// at seams, no timers. The metric WAL's <c>MetricWalV2Tests</c> is the same matrix for the same
/// protocol (a057ca6, b93caba).</para>
/// </summary>
public sealed class SpanWalV3Tests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-swal3-" + Guid.NewGuid().ToString("N"));
    private readonly List<SpanWriteAheadLog> _wals = [];

    public SpanWalV3Tests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        for (int i = _wals.Count - 1; i >= 0; i--)
            try { _wals[i].Dispose(); } catch { }
        try { Directory.Delete(_dir, true); } catch { }
    }

    private string WalPath => Path.Combine(_dir, "spans.wal");
    private string TmpPath => WalPath + SpanWriteAheadLog.UpgradeSuffix;

    private const int  FileHeader   = 64;    // v3: v2's 32 bytes + the relocation record
    private const int  FileHeaderV2 = 32;    // v1 and v2
    private const int  EntryHead    = 68;    // the 64 checksummed bytes + the CRC (v2, v3)
    private const int  EntryHeadV1  = 64;
    private const int  Entry        = 100;   // every span of Span(): a 68-byte head + 32 payload bytes
    private const long BaseNano     = 1_785_300_000_000_000_000L;

    private SpanWriteAheadLog Open(string? path = null, ILogger? logger = null)
    {
        var wal = SpanWriteAheadLog.Open(path ?? WalPath, 64 * 1024, 1 << 20, logger);
        _wals.Add(wal);
        return wal;
    }

    private SpanWriteAheadLog OpenWith(SpanWriteAheadLog.UpgradeIo io, ILogger? logger = null)
    {
        var wal = SpanWriteAheadLog.Open(WalPath, 64 * 1024, 1 << 20, logger, io);
        _wals.Add(wal);
        return wal;
    }

    /// <summary>
    /// A span whose log entry is exactly <see cref="Entry"/> bytes — a 10-byte name, a 6-byte service and
    /// 16 attribute bytes — so a chunk boundary is an entry boundary and the rows below can name them.
    /// </summary>
    private static SpanIngestItem Span(int id)
    {
        var attrs = new byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(attrs, id * 31L);
        BinaryPrimitives.WriteInt64LittleEndian(attrs.AsSpan(8), ~(long)id);
        return new SpanIngestItem
        {
            TraceId           = new TraceId((ulong)id * 0x9E3779B97F4A7C15UL + 1, (ulong)id),
            SpanId            = new SpanId((ulong)id),
            ParentSpanId      = new SpanId((ulong)id + 7),
            StartTimeUnixNano = BaseNano + id,
            DurationNanos     = 1_000 + id,
            Name              = $"op-{id:D7}",
            ServiceName       = "svc-01",
            Kind              = SpanKind.Server,
            Status            = SpanStatusCode.Unset,
            HttpStatusCode    = 200,
            AttributesBytes   = attrs,
        };
    }

    private static ulong[] Ids(int first, int count) => Enumerable.Range(first, count).Select(static i => (ulong)i).ToArray();

    private static ulong[] IdsOf(List<SpanIngestItem> spans) => spans.Select(static s => s.SpanId.RawValue).ToArray();

    private static byte[] ReadShared(string path)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        var bytes = new byte[fs.Length];
        fs.ReadExactly(bytes);
        return bytes;
    }

    // ── A commit, killed anywhere ────────────────────────────────────────────

    /// <summary>
    /// Builds, in a directory of its own, the file a process killed inside a commit leaves:
    /// <paramref name="flushed"/> spans (ids 100…) flushed, <paramref name="tail"/> (200…) appended
    /// while the segment was written, and the file copied at <paramref name="point"/> — "before" the
    /// commit, a <see cref="SpanWriteAheadLog.RelocationStep"/> ("armed", "chunk", "end-stored",
    /// "cleared") with <paramref name="done"/> bytes moved, the data "barrier" (its msync, before
    /// the drive flush and the stamp), or "committed" — then <paramref name="intoNextChunk"/> bytes
    /// of the next chunk copied by hand, the way a forward memmove killed part-way leaves them.
    /// Returns the copy's path.
    /// </summary>
    private string KilledCommit(int flushed, int tail, string point, long done, int intoNextChunk, string name = "killed")
    {
        long prefix = flushed * (long)Entry, length = tail * (long)Entry;
        string source = Path.Combine(_dir, name + "-source.wal");
        byte[]? atPoint = null;
        void Take() => atPoint ??= ReadShared(source);

        using (var wal = SpanWriteAheadLog.Open(source, 64 * 1024, 1 << 20))
        {
            for (int i = 0; i < flushed; i++) wal.Append(Span(100 + i));
            wal.BeginFlush();
            for (int j = 0; j < tail; j++) wal.Append(Span(200 + j));
            Assert.Equal(prefix + length, wal.WrittenBytes);

            if (point == "before") Take();
            wal.OnRelocationStepForTest = (step, moved) =>
            {
                if (moved != done) return;
                if ((point, step) is ("armed",      SpanWriteAheadLog.RelocationStep.Armed)
                                  or ("chunk",      SpanWriteAheadLog.RelocationStep.Chunk)
                                  or ("end-stored", SpanWriteAheadLog.RelocationStep.EndStored)
                                  or ("cleared",    SpanWriteAheadLog.RelocationStep.Cleared)) Take();
            };
            wal.RangeFlushedForTest = (_, _) => { if (point == "barrier") Take(); };   // the first is the data barrier's
            wal.CommitFlush();
            wal.OnRelocationStepForTest = null;
            wal.RangeFlushedForTest     = null;
            if (point == "committed") Take();
            Assert.Equal(length, wal.WrittenBytes);                                    // the live log finished it
        }
        Assert.True(atPoint is not null, $"the kill point {point} at {done} was never reached");

        long chunk = Math.Min(prefix, length - done);
        if (intoNextChunk > 0)
            Array.Copy(atPoint!, FileHeader + prefix + done, atPoint!, FileHeader + done, Math.Min(intoNextChunk, chunk));

        string killed = Path.Combine(_dir, name);
        Directory.CreateDirectory(killed);
        string path = Path.Combine(killed, "spans.wal");
        File.WriteAllBytes(path, atPoint!);
        return path;
    }

    /// <summary>
    /// A PROCESS KILLED ANYWHERE INSIDE A COMMIT LOSES NO SPAN, AND REPLAYS NONE TWICE. The file is
    /// copied at every point of the commit a process can die at: before it recorded anything; once
    /// the record is armed; after each chunk of the move, including the last (all moved, nothing
    /// stored after); after the new end is stored with the record still armed; after the record is
    /// cleared, before the end marker; at the data barrier; and after the stamp. Where a chunk can
    /// be in flight the kill is completed by hand the way a forward memmove leaves it —
    /// <c>intoNextChunk</c> bytes of the next chunk copied. Two shapes: a tail that fits in the
    /// flushed prefix (one chunk) and one longer than it (three chunks: 200, 200 and 100 bytes).
    ///
    /// <para>Opening each copy must replay every span appended during the flush exactly once, leave
    /// the log finished (its length the tail's), say so with a Warning when it had a move to finish,
    /// and leave nothing for the next open to finish. The flushed spans come back only from the
    /// kill BEFORE the record: nothing had moved, and the old header still accepts the flushed
    /// generation — the one window in which a commit duplicates spans of the segment it follows
    /// (the read paths drop the repeat by span id). From the record on, they are gone from the log.</para>
    ///
    /// <para>Without the finish at open (v2), the copies verify, the entry straddling the copy front
    /// does not, and every tail span behind it is lost — the rows killed inside a chunk; the rows
    /// killed between chunks replay the flushed spans the move had not yet overwritten.</para>
    /// </summary>
    [Theory]
    [InlineData(6, 4, "before",     0,   0)]    // nothing recorded: both generations replay
    [InlineData(6, 4, "armed",      0,   0)]    // one chunk (the tail fits in the flushed prefix)
    [InlineData(6, 4, "armed",      0,   150)]  // …killed 1.5 entries into it
    [InlineData(6, 4, "chunk",      400, 0)]    // all moved: done == tail
    [InlineData(6, 4, "end-stored", 400, 0)]    // the new end stored, the record still armed
    [InlineData(6, 4, "cleared",    400, 0)]    // disarmed, the end marker not planted
    [InlineData(6, 4, "barrier",    400, 0)]    // marker planted, data msynced, not stamped
    [InlineData(6, 4, "committed",  400, 0)]    // stamped
    [InlineData(2, 5, "before",     0,   0)]
    [InlineData(2, 5, "armed",      0,   50)]   // a tail longer than the prefix: chunks of 200, 200, 100
    [InlineData(2, 5, "chunk",      200, 0)]    // after the first chunk, on its boundary
    [InlineData(2, 5, "chunk",      200, 130)]  // …and 1.3 entries into the second
    [InlineData(2, 5, "chunk",      400, 99)]   // inside the last, one byte short of it
    [InlineData(2, 5, "chunk",      500, 0)]    // after the last: done == tail
    [InlineData(2, 5, "end-stored", 500, 0)]
    [InlineData(2, 5, "cleared",    500, 0)]
    [InlineData(2, 5, "barrier",    500, 0)]
    [InlineData(2, 5, "committed",  500, 0)]
    public void A_process_killed_anywhere_inside_a_commit_replays_every_surviving_span_once(
        int flushed, int tail, string point, long done, int intoNextChunk)
    {
        string killed = KilledCommit(flushed, tail, point, done, intoNextChunk);
        bool before   = point == "before";
        bool finishes = point is "armed" or "chunk" or "end-stored";
        ulong[] expected = [.. before ? Ids(100, flushed) : [], .. Ids(200, tail)];

        var logger = new CapturingLogger();
        using (var wal = Open(killed, logger))
        {
            Assert.Equal(expected, IdsOf(wal.ReadAll()));
            Assert.Equal(Entry * (long)(before ? flushed + tail : tail), wal.WrittenBytes);   // finished, not just read past
        }
        Assert.Equal(finishes, logger.Entries.Any(static e => e.Level == LogLevel.Warning && e.Text.Contains("finishing the relocation")));
        Assert.DoesNotContain(logger.Entries, static e => e.Level >= LogLevel.Error);

        // And the finish is in the file: the next open has nothing left to finish.
        var quiet = new CapturingLogger();
        using (var again = Open(killed, quiet))
            Assert.Equal(expected, IdsOf(again.ReadAll()));
        Assert.DoesNotContain(quiet.Entries, static e => e.Level >= LogLevel.Warning);
    }

    /// <summary>
    /// A FINISHED RELOCATION IS FORCED TO DISK BEFORE THE OPEN RETURNS. The open that finishes a move
    /// a stop interrupted issues the barrier the commit never reached — the relocated range from the
    /// file's first byte, then the drive flush — so a power loss right after the restart cannot mix
    /// what the killed process moved with what the open redid. An open with nothing to finish
    /// forces nothing.
    /// </summary>
    [Fact]
    public void The_open_that_finishes_a_relocation_forces_it_to_disk()
    {
        string killed = KilledCommit(2, 5, "chunk", 200, 130);
        using (var wal = Open(killed))
        {
            Assert.Equal(1, wal.RangeFlushCount);
            Assert.Equal(1, wal.HandleFlushCount);
            Assert.Equal(0, wal.RangeFlushFailures);
        }

        using var quiet = Open(killed);
        Assert.Equal(0, quiet.RangeFlushCount);
        Assert.Equal(0, quiet.HandleFlushCount);
    }

    /// <summary>
    /// A RECORD THAT CANNOT DESCRIBE A MOVE INSIDE THE FILE MOVES NOTHING. A relocation record whose
    /// source runs past the mapping (a copied, truncated or damaged file) is cleared with an Error
    /// naming its numbers, and the walk decides where the data ends — as it did before the record
    /// existed. Acting on it would copy bytes from outside the log over the spans at its front.
    /// </summary>
    [Fact]
    public void A_record_that_does_not_fit_the_file_is_cleared_and_moves_nothing()
    {
        string killed = KilledCommit(6, 4, "before", 0, 0);
        byte[] file = File.ReadAllBytes(killed);
        BinaryPrimitives.WriteInt64LittleEndian(file.AsSpan(32), 600);                     // MoveFrom: the flush boundary
        BinaryPrimitives.WriteInt64LittleEndian(file.AsSpan(40), file.Length);              // MoveLength: past the file
        File.WriteAllBytes(killed, file);

        var logger = new CapturingLogger();
        using (var wal = Open(killed, logger))
            Assert.Equal([.. Ids(100, 6), .. Ids(200, 4)], IdsOf(wal.ReadAll()));
        Assert.Contains(logger.Entries, static e => e.Level == LogLevel.Error && e.Text.Contains("cannot vouch for"));
        Assert.Equal(0L, BinaryPrimitives.ReadInt64LittleEndian(ReadShared(killed).AsSpan(40)));   // cleared
    }

    // ── The format: v3, and the logs before it ───────────────────────────────

    /// <summary>One entry as a hand-built log carries it: v1 (no checksum) or v2/v3 (the CRC after the 64 header bytes).</summary>
    private static byte[] EntryBytes(SpanIngestItem s, uint generation, bool checksummed)
    {
        byte[] name = Encoding.UTF8.GetBytes(s.Name);
        byte[] svc  = Encoding.UTF8.GetBytes(s.ServiceName);
        int head    = checksummed ? EntryHead : EntryHeadV1;
        var e       = new byte[head + name.Length + svc.Length + s.AttributesBytes.Length];
        s.TraceId.WriteTo(e.AsSpan(0, 16));
        BinaryPrimitives.WriteUInt64LittleEndian(e.AsSpan(16), s.SpanId.RawValue);
        BinaryPrimitives.WriteUInt64LittleEndian(e.AsSpan(24), s.ParentSpanId.RawValue);
        BinaryPrimitives.WriteInt64LittleEndian (e.AsSpan(32), s.StartTimeUnixNano);
        BinaryPrimitives.WriteInt64LittleEndian (e.AsSpan(40), s.DurationNanos);
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(48), (uint)s.AttributesBytes.Length);
        BinaryPrimitives.WriteUInt16LittleEndian(e.AsSpan(52), (ushort)name.Length);
        BinaryPrimitives.WriteUInt16LittleEndian(e.AsSpan(54), (ushort)svc.Length);
        BinaryPrimitives.WriteInt16LittleEndian (e.AsSpan(56), s.HttpStatusCode);
        e[58] = (byte)s.Kind;
        e[59] = (byte)s.Status;
        BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(60), generation);
        name.CopyTo(e, head);
        svc.CopyTo(e, head + name.Length);
        s.AttributesBytes.CopyTo(e, head + name.Length + svc.Length);
        if (checksummed)
            BinaryPrimitives.WriteUInt32LittleEndian(e.AsSpan(64), Crc32c.Append(Crc32c.Append(0, e.AsSpan(0, 64)), e.AsSpan(EntryHead)));
        return e;
    }

    /// <summary>Writes a v1 or v2 log by hand — a 32-byte header, then the entries — at 64 KB of capacity.</summary>
    private void WriteLegacyLog(ushort version, uint headerGeneration, params byte[][] entries)
    {
        long written = entries.Sum(static e => (long)e.Length);
        var file = new byte[FileHeaderV2 + 64 * 1024];
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(0), 0x52_44_53_57);               // "RDSW"
        BinaryPrimitives.WriteUInt16LittleEndian(file.AsSpan(4), version);
        BinaryPrimitives.WriteInt64LittleEndian (file.AsSpan(8), FileHeaderV2 + written);
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(16), headerGeneration);
        long at = FileHeaderV2;
        foreach (var e in entries) { e.CopyTo(file, at); at += e.Length; }
        File.WriteAllBytes(WalPath, file);
    }

    /// <summary>The v2 log the previous release leaves: header generation 7, one dead entry, two of 7, two of 8 (mid-flush).</summary>
    private void WriteV2Log() => WriteLegacyLog(2, 7,
        EntryBytes(Span(100), 6, checksummed: true),                                       // an older generation: dead
        EntryBytes(Span(101), 7, checksummed: true), EntryBytes(Span(102), 7, checksummed: true),
        EntryBytes(Span(103), 8, checksummed: true), EntryBytes(Span(104), 8, checksummed: true));

    private static ushort VersionOnDisk(string path) => BinaryPrimitives.ReadUInt16LittleEndian(ReadShared(path).AsSpan(4));

    /// <summary>
    /// A v2 LOG — THE 32-BYTE HEADER THE PREVIOUS RELEASE WROTE — IS UPGRADED, NOT DROPPED: every live
    /// span replays, field for field, under the same generation rule; the file is v3 from then on, its
    /// data 32 bytes further in; a span appended after the upgrade survives; and a commit on the
    /// upgraded log records its move. Treating version 2 as unknown, the way a foreign file is,
    /// re-initialises the log and replays nothing — every span the old process acknowledged and
    /// never flushed, silently gone.
    /// </summary>
    [Fact]
    public void A_v2_log_is_upgraded_to_v3_and_replays_every_span()
    {
        WriteV2Log();

        using (var wal = Open())
        {
            Assert.Equal((ushort)3, wal.HeaderForTest.Version);
            Assert.Equal(7u, wal.HeaderForTest.Generation);
            Assert.Equal(FileHeader + 5L * Entry, wal.HeaderForTest.WriteOffset);   // every entry the v2 walk reaches, the dead one too
            var replayed = wal.ReadAll();
            Assert.Equal(Ids(101, 4), IdsOf(replayed));
            var want = Span(103);
            var got  = replayed[2];
            Assert.Equal(want.TraceId, got.TraceId);
            Assert.Equal(want.ParentSpanId, got.ParentSpanId);
            Assert.Equal(want.StartTimeUnixNano, got.StartTimeUnixNano);
            Assert.Equal(want.DurationNanos, got.DurationNanos);
            Assert.Equal(want.Name, got.Name);
            Assert.Equal(want.ServiceName, got.ServiceName);
            Assert.Equal(want.HttpStatusCode, got.HttpStatusCode);
            Assert.Equal(want.AttributesBytes, got.AttributesBytes);
            wal.Append(Span(105));
        }
        Assert.Equal((ushort)3, VersionOnDisk(WalPath));
        Assert.False(File.Exists(TmpPath));

        using (var wal = Open())
        {
            Assert.Equal(Ids(101, 5), IdsOf(wal.ReadAll()));
            long steps = 0;
            wal.BeginFlush();
            wal.Append(Span(106));
            wal.OnRelocationStepForTest = (_, _) => steps++;
            wal.CommitFlush();
            Assert.True(steps > 0, "the upgraded log committed without recording its move");
        }
        using (var wal = Open())
            Assert.Equal([106UL], IdsOf(wal.ReadAll()));
    }

    /// <summary>
    /// THE UPGRADE COPIES WHAT v2 WOULD HAVE REPLAYED, AND NOTHING PAST IT. A v2 log whose third entry
    /// fails its checksum (a torn write under the old release) replayed two spans; the upgrade carries
    /// those two and stops, so it can never vouch for the torn entry or the bytes behind it — the
    /// v3 copy's own checksums would otherwise bless whatever they hold.
    /// </summary>
    [Fact]
    public void A_v2_log_is_copied_up_to_its_first_entry_that_does_not_verify()
    {
        byte[] torn = EntryBytes(Span(102), 7, checksummed: true);
        torn[EntryHead + 3] ^= 0x5A;                                                       // a name byte
        WriteLegacyLog(2, 7,
            EntryBytes(Span(100), 7, checksummed: true), EntryBytes(Span(101), 7, checksummed: true),
            torn, EntryBytes(Span(103), 7, checksummed: true));

        using (var wal = Open())
        {
            Assert.Equal((ushort)3, wal.HeaderForTest.Version);
            Assert.Equal(FileHeader + 2L * Entry, wal.HeaderForTest.WriteOffset);
            Assert.Equal(Ids(100, 2), IdsOf(wal.ReadAll()));
        }
    }

    /// <summary>
    /// A v2 LOG WHOSE UPGRADE CANNOT COMPLETE STAYS v2 FOR THE RUN AND LOSES NOTHING. The move is
    /// refused every time (the antivirus scanner that will not let go): the log opens in its own
    /// layout, in place — a 32-byte header, v2 entries — replays every live span, takes appends AND
    /// commits in that layout (the single copy a v2 log always made: it has no room for a record),
    /// and the next start, with the scanner gone, upgrades it with everything it gained meanwhile.
    /// </summary>
    [Fact]
    public void A_v2_log_whose_upgrade_cannot_complete_stays_v2_and_loses_nothing()
    {
        WriteV2Log();
        int moves = 0;
        var io = new SpanWriteAheadLog.UpgradeIo
        {
            Move = (_, _) => { moves++; throw new IOException("The process cannot access the file because it is being used by another process."); },
            Wait = static _ => { },
        };
        var logger = new CapturingLogger();

        using (var wal = OpenWith(io, logger))
        {
            Assert.Equal((ushort)2, wal.HeaderForTest.Version);
            Assert.Equal(Ids(101, 4), IdsOf(wal.ReadAll()));
            wal.BeginFlush();
            wal.Append(Span(105));
            wal.Append(Span(106));
            wal.CommitFlush();                                                             // the v2 single copy
            wal.Append(Span(107));
            Assert.Equal(FileHeaderV2 + 3L * Entry, wal.HeaderForTest.WriteOffset);       // v2 offsets: 32 lower
        }
        Assert.Equal(6, moves);
        Assert.Contains(logger.Entries, static e => e.Level == LogLevel.Error && e.Error is IOException);
        Assert.False(File.Exists(TmpPath));
        Assert.Equal((ushort)2, VersionOnDisk(WalPath));

        using (var wal = Open())
        {
            Assert.Equal((ushort)3, wal.HeaderForTest.Version);
            Assert.Equal(Ids(105, 3), IdsOf(wal.ReadAll()));
        }
    }

    /// <summary>
    /// A v1 LOG IS UPGRADED STRAIGHT TO v3: its entries get the checksum v1 never had, its header the
    /// record, and the live spans replay. (SpanWalV2Tests holds the v1 facts; this is the one hop.)
    /// </summary>
    [Fact]
    public void A_v1_log_is_upgraded_straight_to_v3()
    {
        WriteLegacyLog(1, 3,
            EntryBytes(Span(100), 2, checksummed: false),
            EntryBytes(Span(101), 3, checksummed: false), EntryBytes(Span(102), 4, checksummed: false));

        using (var wal = Open())
        {
            Assert.Equal((ushort)3, wal.HeaderForTest.Version);
            Assert.Equal(FileHeader + 3L * Entry, wal.HeaderForTest.WriteOffset);   // 96-byte v1 entries, 100 bytes each now
            Assert.Equal(Ids(101, 2), IdsOf(wal.ReadAll()));
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
