using System.Buffers.Binary;
using System.Text;
using Ameto.Core;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;
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

    private readonly ITestOutputHelper _out;

    public SpanWalV3Tests(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_dir);
    }

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
    private static SpanIngestItem Span(int id, string? name = null)
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
            Name              = name ?? $"op-{id:D7}",
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
    /// commit; a <see cref="SpanWriteAheadLog.RelocationStep"/> ("armed", "chunk", "end-stored",
    /// "cleared") with <paramref name="done"/> bytes moved; a state between a covered store's
    /// pending checksum and its field ("armed-pending", "stamp-pending") or between the field and the
    /// checksum that vouches for it ("armed-unsealed", "chunk-unsealed", "clear-unsealed",
    /// "stamp-unsealed"); the data "barrier" (its msync, before the drive flush and the stamp); or
    /// "committed" — then <paramref name="intoNextChunk"/> bytes of the next chunk copied by hand,
    /// the way a forward memmove killed part-way leaves them. Returns the copy's path.
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
            wal.OnPendingStoredForTest = (field, value) =>
            {
                if ((point, field) is ("armed-pending", "MoveLength") && value != 0) Take();
                if ((point, field) is ("stamp-pending", "Generation")) Take();
            };
            wal.OnCoveredStoreForTest = (field, value) =>
            {
                if ((point, field) is ("armed-unsealed", "MoveLength") && value != 0) Take();
                if ((point, field) is ("chunk-unsealed", "MoveDone") && (long)value == done) Take();
                if ((point, field) is ("clear-unsealed", "MoveLength") && value == 0) Take();
                if ((point, field) is ("stamp-unsealed", "Generation")) Take();
            };
            wal.RangeFlushedForTest = (_, _) => { if (point == "barrier") Take(); };   // the first is the data barrier's
            wal.CommitFlush();
            wal.OnRelocationStepForTest = null;
            wal.OnPendingStoredForTest  = null;
            wal.OnCoveredStoreForTest   = null;
            wal.RangeFlushedForTest     = null;
            if (point == "committed") Take();
            // The live log finished it: the tail moved to the front — or, a prefix under an eighth
            // of it (dead by construction here: one generation, below the stamp), stayed behind it.
            Assert.Equal(prefix * 8 < length ? prefix + length : length, wal.WrittenBytes);
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

    /// <summary>Rots the header's generation (bytes 16..20), unsealed: the header no longer verifies.</summary>
    private static void RotGeneration(string path)
    {
        byte[] file = File.ReadAllBytes(path);
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(16), 0xDEAD_BEEFu);
        File.WriteAllBytes(path, file);
    }

    /// <summary>The v3 header checksum over a file's first 64 bytes, into both slots: CRC32C over [0, 8) and [16, 56), the record's From/Done read as 0 while its Length is 0.</summary>
    private static void Seal(byte[] file)
    {
        byte[] h = file.AsSpan(0, FileHeader).ToArray();
        if (BinaryPrimitives.ReadInt64LittleEndian(h.AsSpan(40)) == 0) { h.AsSpan(32, 8).Clear(); h.AsSpan(48, 8).Clear(); }
        uint crc = Crc32c.Append(Crc32c.Append(0, h.AsSpan(0, 8)), h.AsSpan(16, 40));
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(56), crc);
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(60), crc);
    }

    /// <summary>
    /// A PROCESS KILLED ANYWHERE INSIDE A COMMIT LOSES NO SPAN, AND REPLAYS NONE TWICE. The file is
    /// copied at every point of the commit a process can die at: before it recorded anything; once
    /// the record is armed — sealed, or on either side of the arming store's field; after each chunk
    /// of the move — sealed, or between its <c>MoveDone</c> and the checksum — including the last
    /// (all moved, nothing stored after); after the new end is stored with the record still armed;
    /// between the disarming and its checksum; after the record is cleared, before the end marker;
    /// at the data barrier; on either side of the stamp's field; and after the stamp. Where a chunk
    /// can be in flight the kill is completed by hand the way a forward memmove leaves it —
    /// <c>intoNextChunk</c> bytes of the next chunk copied. Two shapes: a tail that fits in the
    /// flushed prefix (one chunk) and one longer than it (three chunks: 200, 200 and 100 bytes).
    /// Some rows also rot the header's generation, so the header fails its checksum — with a record
    /// armed, or without one.
    ///
    /// <para>Opening each copy must replay every span appended during the flush exactly once, leave
    /// the log finished (its length the tail's), say so with a Warning when it had a move to finish,
    /// log an Error only for a rotted header (and never "cannot vouch for": every record a commit
    /// leaves matches its claim), and leave nothing for the next open to finish or rebuild. The
    /// flushed spans come back only from a kill BEFORE the record (the pending checksum of the
    /// arming is not the arming): nothing had moved, and the old header still accepts the flushed
    /// generation — the one window in which a commit duplicates spans of the segment it follows
    /// (the read paths drop the repeat by span id). From the record on, they are gone from the log.</para>
    ///
    /// <para>Without the finish at open (v2), the copies verify, the entry straddling the copy front
    /// does not, and every tail span behind it is lost — the rows killed inside a chunk; the rows
    /// killed between chunks replay the flushed spans the move had not yet overwritten. Without the
    /// pending checksum, every unsealed row reads as rot (an Error and a rebuild of a header that was
    /// right); without the rebuild, every rotted row replays nothing.</para>
    /// </summary>
    [Theory]
    [InlineData(6, 4, "before",         0,   0,   false)]   // nothing recorded: both generations replay
    [InlineData(6, 4, "before",         0,   0,   true)]
    [InlineData(6, 4, "armed-pending",  0,   0,   false)]   // the arming's checksum stored, not the arming
    [InlineData(6, 4, "armed",          0,   0,   false)]   // one chunk (the tail fits in the flushed prefix)
    [InlineData(6, 4, "armed",          0,   150, false)]   // …killed 1.5 entries into it
    [InlineData(6, 4, "armed",          0,   150, true)]    // …and the header rotted
    [InlineData(6, 4, "armed-unsealed", 0,   0,   false)]   // between arming and its checksum
    [InlineData(6, 4, "armed-unsealed", 0,   0,   true)]
    [InlineData(6, 4, "chunk",          400, 0,   false)]   // all moved: done == tail
    [InlineData(6, 4, "chunk-unsealed", 400, 0,   false)]
    [InlineData(6, 4, "end-stored",     400, 0,   false)]   // the new end stored, the record still armed
    [InlineData(6, 4, "end-stored",     400, 0,   true)]
    [InlineData(6, 4, "clear-unsealed", 400, 0,   false)]   // between disarming and its checksum
    [InlineData(6, 4, "clear-unsealed", 400, 0,   true)]
    [InlineData(6, 4, "cleared",        400, 0,   false)]   // disarmed, the end marker not planted
    [InlineData(6, 4, "barrier",        400, 0,   false)]   // marker planted, data msynced, not stamped
    [InlineData(6, 4, "stamp-pending",  400, 0,   false)]   // the stamp's checksum stored, not the stamp
    [InlineData(6, 4, "stamp-unsealed", 400, 0,   false)]   // the stamp stored, its checksum not
    [InlineData(6, 4, "stamp-unsealed", 400, 0,   true)]
    [InlineData(6, 4, "committed",      400, 0,   false)]   // stamped
    [InlineData(6, 4, "committed",      400, 0,   true)]
    [InlineData(2, 5, "before",         0,   0,   false)]
    [InlineData(2, 5, "armed",          0,   50,  false)]   // a tail longer than the prefix: chunks of 200, 200, 100
    [InlineData(2, 5, "armed",          0,   50,  true)]
    [InlineData(2, 5, "chunk",          200, 0,   false)]   // after the first chunk, on its boundary
    [InlineData(2, 5, "chunk",          200, 130, false)]   // …and 1.3 entries into the second
    [InlineData(2, 5, "chunk",          200, 130, true)]
    [InlineData(2, 5, "chunk-unsealed", 200, 0,   false)]
    [InlineData(2, 5, "chunk-unsealed", 200, 0,   true)]
    [InlineData(2, 5, "chunk",          400, 99,  false)]   // inside the last, one byte short of it
    [InlineData(2, 5, "chunk",          500, 0,   false)]   // after the last: done == tail
    [InlineData(2, 5, "end-stored",     500, 0,   false)]
    [InlineData(2, 5, "cleared",        500, 0,   false)]
    [InlineData(2, 5, "barrier",        500, 0,   false)]
    [InlineData(2, 5, "committed",      500, 0,   false)]
    public void A_process_killed_anywhere_inside_a_commit_replays_every_surviving_span_once(
        int flushed, int tail, string point, long done, int intoNextChunk, bool rotHeader)
    {
        string killed = KilledCommit(flushed, tail, point, done, intoNextChunk);
        if (rotHeader) RotGeneration(killed);

        bool before   = point is "before" or "armed-pending";
        bool finishes = point is "armed" or "armed-unsealed" or "chunk" or "chunk-unsealed" or "end-stored";
        ulong[] expected = [.. before ? Ids(100, flushed) : [], .. Ids(200, tail)];

        var logger = new CapturingLogger();
        using (var wal = Open(killed, logger))
        {
            Assert.Equal(expected, IdsOf(wal.ReadAll()));
            Assert.Equal(Entry * (long)(before ? flushed + tail : tail), wal.WrittenBytes);   // finished, not just read past
        }
        Assert.Equal(finishes, logger.Entries.Any(static e => e.Level == LogLevel.Warning && e.Text.Contains("finishing the relocation")));
        Assert.Equal(rotHeader, logger.Entries.Any(static e => e.Level == LogLevel.Error && e.Text.Contains("header does not verify")));
        Assert.DoesNotContain(logger.Entries, static e => e.Text.Contains("cannot vouch for"));

        // And the finish is in the file: the next open has nothing left to finish or rebuild.
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
    /// A RECORD THAT CANNOT DESCRIBE A MOVE INSIDE THE FILE MOVES NOTHING, EVEN IN A HEADER THAT
    /// VERIFIES. A relocation record whose source runs past the mapping (a copied or truncated file;
    /// sealed here, so it is the checksum's blind spot and not rot) is cleared with an Error naming
    /// its numbers, and the walk decides where the data ends — as it did before the record existed.
    /// Acting on it would copy bytes from outside the log over the spans at its front.
    /// </summary>
    [Fact]
    public void A_record_that_does_not_fit_the_file_is_cleared_and_moves_nothing()
    {
        string killed = KilledCommit(6, 4, "before", 0, 0);
        byte[] file = File.ReadAllBytes(killed);
        BinaryPrimitives.WriteInt64LittleEndian(file.AsSpan(32), 600);                     // MoveFrom: the flush boundary
        BinaryPrimitives.WriteInt64LittleEndian(file.AsSpan(40), file.Length);              // MoveLength: past the file
        Seal(file);
        File.WriteAllBytes(killed, file);

        var logger = new CapturingLogger();
        using (var wal = Open(killed, logger))
            Assert.Equal([.. Ids(100, 6), .. Ids(200, 4)], IdsOf(wal.ReadAll()));
        Assert.Contains(logger.Entries, static e => e.Level == LogLevel.Error && e.Text.Contains("cannot vouch for"));
        Assert.DoesNotContain(logger.Entries, static e => e.Text.Contains("header does not verify"));
        Assert.Equal(0L, BinaryPrimitives.ReadInt64LittleEndian(ReadShared(killed).AsSpan(40)));   // cleared
    }

    /// <summary>
    /// A RELOCATION RECORD IN A HEADER THAT DOES NOT VERIFY IS ACTED ON ONLY IF IT DESCRIBES THE MOVE
    /// A COMMIT MADE. The three-chunk kill inside the second chunk, with the header's generation
    /// rotted as well, so the header fails. The record still matches what the commit left — the claim
    /// at the old end, <c>done</c> a chunk boundary — so the move is finished and every tail span
    /// replays, under the generation rebuilt from the entries. With <c>done</c> rotted too (not a
    /// chunk boundary) the record is not trusted: an Error says so, nothing is moved by it, and the
    /// walk keeps what the copy front left — the three tail spans moved whole before it. Moving by
    /// the rotted record would have read a source the second chunk had already begun to overwrite.
    /// </summary>
    [Theory]
    [InlineData(false, new ulong[] { 200, 201, 202, 203, 204 })]
    [InlineData(true,  new ulong[] { 200, 201, 202 })]
    public void A_relocation_record_in_a_header_that_does_not_verify_must_match_the_move_it_names(bool rotDone, ulong[] expected)
    {
        string killed = KilledCommit(2, 5, "chunk", 200, 130);
        byte[] file = File.ReadAllBytes(killed);
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(16), 0xDEAD_BEEFu);              // Generation: rot
        if (rotDone) BinaryPrimitives.WriteInt64LittleEndian(file.AsSpan(48), 50);            // MoveDone: rot
        File.WriteAllBytes(killed, file);

        var logger = new CapturingLogger();
        using (var wal = Open(killed, logger))
            Assert.Equal(expected, IdsOf(wal.ReadAll()));
        Assert.Contains(logger.Entries, static e => e.Level == LogLevel.Error && e.Text.Contains("header does not verify"));
        Assert.Equal(rotDone, logger.Entries.Any(static e => e.Level == LogLevel.Error && e.Text.Contains("cannot vouch for")));
        Assert.Equal(1u, BinaryPrimitives.ReadUInt32LittleEndian(ReadShared(killed).AsSpan(16)));   // rebuilt: the flushed generation
    }

    /// <summary>
    /// A RECORD THAT SAYS THE NEW END IS STORED BUT THE MOVE IS NOT DONE IS NO STATE A COMMIT LEAVES.
    /// The three-chunk move killed after its new end was stored (the record still armed, all of it
    /// done), then the header's generation rotted and MoveDone rotted to a lower chunk boundary. The
    /// claim equals the move's length and <c>done</c> is a multiple of <c>from</c>, so a check that
    /// took them separately would trust it — and redoing from there reads source bytes the later
    /// chunks have already overwritten, corrupting spans that were whole and in place (203 lost, 204
    /// twice). The record is refused; the five tail spans, already in place, replay.
    /// </summary>
    [Fact]
    public void A_record_claiming_the_new_end_with_the_move_unfinished_is_not_redone()
    {
        string killed = KilledCommit(2, 5, "end-stored", 500, 0);
        byte[] file = File.ReadAllBytes(killed);
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(16), 0xDEAD_BEEFu);              // Generation: rot
        BinaryPrimitives.WriteInt64LittleEndian (file.AsSpan(48), 200);                       // MoveDone: rot, a boundary
        File.WriteAllBytes(killed, file);

        var logger = new CapturingLogger();
        using (var wal = Open(killed, logger))
            Assert.Equal(Ids(200, 5), IdsOf(wal.ReadAll()));
        Assert.Contains(logger.Entries, static e => e.Level == LogLevel.Error && e.Text.Contains("cannot vouch for"));
    }

    /// <summary>
    /// THE DOUBLE STOP. The first kill leaves a header that verifies only by its pending checksum, or
    /// a record the open has to finish; the open that finishes it is killed in turn, inside its first
    /// covered store, after it wrote that store's pending checksum and before the field. Unless the
    /// open sealed the header before storing anything, that file matches neither slot and the next
    /// open reports rot that is not there. It must verify, finish, and replay every tail span once.
    /// </summary>
    [Theory]
    [InlineData(6, 4, "armed-unsealed", 0)]
    [InlineData(2, 5, "chunk-unsealed", 200)]
    [InlineData(6, 4, "end-stored",     400)]
    public void A_second_stop_inside_the_open_that_finishes_a_first_leaves_a_header_that_verifies(
        int flushed, int tail, string point, long done)
    {
        string killed = KilledCommit(flushed, tail, point, done, intoNextChunk: 0);

        byte[]? secondStop = null;
        SpanWriteAheadLog.t_pendingStoredForNextOpenForTest = (_, _) => secondStop ??= ReadShared(killed);
        try { Open(killed).Dispose(); }
        finally { SpanWriteAheadLog.t_pendingStoredForNextOpenForTest = null; }
        Assert.NotNull(secondStop);

        string twice = Path.Combine(_dir, "twice");
        Directory.CreateDirectory(twice);
        File.WriteAllBytes(Path.Combine(twice, "spans.wal"), secondStop!);

        var logger = new CapturingLogger();
        using (var wal = Open(Path.Combine(twice, "spans.wal"), logger))
            Assert.Equal(Ids(200, tail), IdsOf(wal.ReadAll()));
        Assert.DoesNotContain(logger.Entries, static e => e.Level >= LogLevel.Error);
    }

    /// <summary>
    /// AN OPEN KILLED WHILE IT FINISHES A RELOCATION UNDER A HEADER THAT DOES NOT VERIFY LEAVES IT NOT
    /// VERIFYING. The open redoes the move chunk by chunk, storing its progress; if those stores
    /// sealed the header, the rotted generation would be sealed with them, and the open after a second
    /// kill would find a header that verifies, skip the rebuild — and replay nothing, the rotted
    /// generation accepting no entry. The file is copied at the open's own relocation seam; opening
    /// that copy still rebuilds, and every tail span replays.
    /// </summary>
    [Fact]
    public void An_open_killed_while_it_finishes_a_relocation_under_a_rotted_header_still_rebuilds()
    {
        string killed = KilledCommit(2, 5, "chunk", 200, 130);
        RotGeneration(killed);

        byte[]? midOpen = null;
        SpanWriteAheadLog.t_relocationStepForNextOpenForTest = (_, _) => midOpen ??= ReadShared(killed);
        try { Open(killed).Dispose(); }
        finally { SpanWriteAheadLog.t_relocationStepForNextOpenForTest = null; }
        Assert.NotNull(midOpen);

        string twice = Path.Combine(_dir, "twice");
        Directory.CreateDirectory(twice);
        File.WriteAllBytes(Path.Combine(twice, "spans.wal"), midOpen!);

        var logger = new CapturingLogger();
        using (var wal = Open(Path.Combine(twice, "spans.wal"), logger))
            Assert.Equal(Ids(200, 5), IdsOf(wal.ReadAll()));
        Assert.Contains(logger.Entries, static e => e.Level == LogLevel.Error && e.Text.Contains("header does not verify"));
    }

    // ── The header's own checksum ────────────────────────────────────────────

    /// <summary>
    /// ONE FLIPPED BIT IN ANY FIELD OF THE HEADER LOSES NO SPAN. The log is mid-flush — three spans
    /// of the header's generation, two of its successor appended after BeginFlush — and one bit of
    /// the header is flipped on disk. Every field the checksum covers fails it and has the generation
    /// rebuilt from the entries (an Error), except the record's From and Done while no record is
    /// armed, which the checksum reads as zero and nothing reads at all; a flip in either checksum
    /// leaves the other slot verifying. The claim is not covered — every append moves it — and a
    /// claim flipped past the data is cut back by the walk to where the entries end. Every row
    /// replays all five spans and leaves a header the next open finds quiet.
    ///
    /// <para>Without the checksum the generation row replays NOTHING: a generation two or more away
    /// from the entries' accepts none of them (#103's second half — "a garbage generation hides every
    /// entry"). Magic and version are not rows: they identify the file before any checksum is read,
    /// and one that does not read as a span WAL is a foreign file, re-initialised, as in every
    /// version of this log.</para>
    /// </summary>
    [Theory]
    [InlineData("pad",            6,  0x01, true)]
    [InlineData("generation",     17, 0x40, true)]    // 1 → 0x4001: accepts no entry
    [InlineData("reserved",       20, 0x01, true)]
    [InlineData("reserved, high", 31, 0x80, true)]
    [InlineData("move from",      33, 0x01, false)]   // no record armed: read as 0
    [InlineData("move length",    40, 0x08, true)]    // a record armed by rot: from 0, refused
    [InlineData("move done",      48, 0x10, false)]
    [InlineData("crc",            57, 0x01, false)]   // the pending slot still verifies
    [InlineData("pending crc",    62, 0x01, false)]
    [InlineData("claim",          13, 0x01, false)]   // not covered: 2^40 bytes past the data
    public void A_flipped_bit_in_any_header_field_loses_no_span(string field, int offset, byte mask, bool rebuilds)
    {
        using (var wal = Open())
        {
            for (int i = 0; i < 3; i++) wal.Append(Span(100 + i));
            wal.BeginFlush();
            for (int i = 3; i < 5; i++) wal.Append(Span(100 + i));
        }
        byte[] file = File.ReadAllBytes(WalPath);
        file[offset] ^= mask;
        File.WriteAllBytes(WalPath, file);

        var logger = new CapturingLogger();
        ulong[] replayed;
        using (var wal = Open(logger: logger)) replayed = IdsOf(wal.ReadAll());
        Assert.True(Ids(100, 5).SequenceEqual(replayed), $"{field}: replayed [{string.Join(", ", replayed)}]");
        Assert.Equal(rebuilds, logger.Entries.Any(static e => e.Level == LogLevel.Error && e.Text.Contains("header does not verify")));

        var quiet = new CapturingLogger();
        using (var wal = Open(logger: quiet))
            Assert.Equal(Ids(100, 5), IdsOf(wal.ReadAll()));
        Assert.DoesNotContain(quiet.Entries, static e => e.Level >= LogLevel.Warning);
    }

    /// <summary>Writes a v3 log by hand — a 64-byte header with the given generation, sealed unless rotted — then the entries.</summary>
    private void WriteV3Log(uint headerGeneration, bool seal, params byte[][] entries)
    {
        long written = entries.Sum(static e => (long)e.Length);
        var file = new byte[FileHeader + 64 * 1024];
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(0), 0x52_44_53_57);               // "RDSW"
        BinaryPrimitives.WriteUInt16LittleEndian(file.AsSpan(4), 3);
        BinaryPrimitives.WriteInt64LittleEndian (file.AsSpan(8), FileHeader + written);
        BinaryPrimitives.WriteUInt32LittleEndian(file.AsSpan(16), headerGeneration);
        if (seal) Seal(file);
        long at = FileHeader;
        foreach (var e in entries) { e.CopyTo(file, at); at += e.Length; }
        File.WriteAllBytes(WalPath, file);
    }

    /// <summary>
    /// THE REBUILD TAKES THE NEWEST GENERATION ON THE CYCLE, NOT THE LAST ONE IN THE FILE AND NOT THE
    /// LARGEST NUMBER. Two logs whose header rotted: a restart after a crash mid-flush, which appended
    /// under the header's generation (4) BEHIND entries of its successor (5) — the last entry is the
    /// older one; and a flush in flight across the wrap, uint.MaxValue followed by 1 — the largest
    /// number is the older one. Either wrong reading rebuilds a window that drops the newer entries;
    /// both logs must replay every entry.
    /// </summary>
    [Theory]
    [InlineData(5u, 4u)]                  // the last entry is the older generation
    [InlineData(uint.MaxValue, 1u)]       // the newer generation is the smaller number
    public void The_rebuild_takes_the_newest_generation_on_the_cycle(uint first, uint then)
    {
        WriteV3Log(0xDEAD_BEEFu, seal: false,
            EntryBytes(Span(100), first, checksummed: true), EntryBytes(Span(101), first, checksummed: true),
            EntryBytes(Span(102), then,  checksummed: true), EntryBytes(Span(103), then,  checksummed: true));

        var logger = new CapturingLogger();
        using (var wal = Open(logger: logger))
            Assert.Equal(Ids(100, 4), IdsOf(wal.ReadAll()));
        Assert.Contains(logger.Entries, static e => e.Level == LogLevel.Error && e.Text.Contains("header does not verify"));
    }

    // ── A short prefix stays: at most eight chunks ───────────────────────────

    /// <summary>
    /// A DEAD PREFIX FAR SHORTER THAN THE TAIL BEHIND IT IS LEFT IN PLACE, NOT MOVED IN THOUSANDS OF
    /// SEALED CHUNKS. One flushed span before nine appended during the flush: moving them would take
    /// nine one-entry chunks, each a header store and two checksums under the append lock — at 8 MiB
    /// of minimal spans, 120 000 of them. The commit leaves the log as it is (no relocation step
    /// fires, the length is unchanged), the replay skips the dead span by its generation, and the
    /// next commit reclaims everything. Without the rule the tail moves (eleven steps) and the log
    /// is nine spans long.
    /// </summary>
    [Fact]
    public void A_dead_prefix_far_shorter_than_its_tail_is_left_in_place_until_the_next_commit()
    {
        long steps = 0;
        using (var wal = Open())
        {
            wal.Append(Span(100));
            wal.BeginFlush();
            for (int j = 0; j < 9; j++) wal.Append(Span(200 + j));

            wal.OnRelocationStepForTest = (_, _) => steps++;
            wal.CommitFlush();
            Assert.Equal(0, steps);
            Assert.Equal(10L * Entry, wal.WrittenBytes);                                   // nothing moved
            Assert.Equal(Ids(200, 9), IdsOf(wal.ReadAll()));
        }

        using (var wal = Open())
        {
            Assert.Equal(Ids(200, 9), IdsOf(wal.ReadAll()));
            wal.BeginFlush();
            wal.CommitFlush();
            Assert.Equal(0, wal.WrittenBytes);                                            // reclaimed with the rest
        }
        using (var wal = Open())
            Assert.Empty(wal.ReadAll());
    }

    /// <summary>
    /// THE BOUND IS EIGHT CHUNKS, NOT NINE. A one-entry prefix (100 B) before eight spans and one a
    /// byte longer (801 B): `prefix &lt; tail / 8` floors 801 / 8 to 100 and lets the move run — nine
    /// chunks. Compared multiplied (100 × 8 = 800 &lt; 801), the prefix stays. A tail of exactly eight
    /// prefixes still moves, in exactly eight chunks.
    /// </summary>
    [Theory]
    [InlineData(true,  0)]      // 801 B: stays
    [InlineData(false, 8)]      // 800 B: moves, eight chunks
    public void A_tail_just_over_eight_prefixes_is_not_moved(bool oneByteMore, int chunks)
    {
        long chunkSteps = 0;
        using var wal = Open();
        wal.Append(Span(100));
        wal.BeginFlush();
        for (int j = 0; j < 7; j++) wal.Append(Span(200 + j));
        wal.Append(Span(207, oneByteMore ? "op-00000207" : null));
        wal.OnRelocationStepForTest = (step, _) => { if (step == SpanWriteAheadLog.RelocationStep.Chunk) chunkSteps++; };
        wal.CommitFlush();
        Assert.Equal(chunks, chunkSteps);
        Assert.Equal(Ids(200, 8), IdsOf(wal.ReadAll()));
    }

    /// <summary>
    /// A RETRIED FLUSH MOVES ITS TAIL HOWEVER LONG IT IS: ITS PREFIX HOLDS THE GENERATION IT STAMPS.
    /// The first attempt is abandoned (the segment write failed); a span arrives before the retry's
    /// Begin, which reuses the bumped generation — so the retry's prefix holds a span of the very
    /// generation the commit then stamps, and the replay accepts it. The tail is more than eight
    /// prefixes long, which would leave the prefix in place by length alone; left there, span 101 —
    /// in the retry's segment — replays beside it on every restart. The commit must move.
    /// </summary>
    [Fact]
    public void A_retried_flush_moves_its_tail_however_long_since_its_prefix_holds_the_stamps_generation()
    {
        long steps = 0;
        using (var wal = Open())
        {
            wal.Append(Span(100));                                                       // generation 1
            wal.BeginFlush();
            wal.AbandonFlush();                                                          // the write failed
            wal.Append(Span(101));                                                       // generation 2, before the retry
            wal.BeginFlush();                                                            // the retry: no bump, 2 is reused
            for (int j = 0; j < 17; j++) wal.Append(Span(200 + j));                      // 1 700 B behind a 200 B prefix

            wal.OnRelocationStepForTest = (_, _) => steps++;
            wal.CommitFlush();
        }
        Assert.True(steps > 0, "the retry's prefix was left in front of its tail");

        using var reopened = Open();
        Assert.Equal(Ids(200, 17), IdsOf(reopened.ReadAll()));
    }

    /// <summary>
    /// A FLUSH AFTER A RESTART MID-FLUSH MOVES ITS TAIL: ITS PREFIX HOLDS THE SUCCESSOR'S ENTRIES. The
    /// process died between a Begin and its commit — two spans of generation 1, one of 2 — and the
    /// restart replays all three and appends under the header's generation, 1. Its first flush bumps
    /// to 2 and stamps 2: the pre-crash span of 2, in that flush's segment, sits in its prefix, live
    /// under the stamp. A long tail would leave it there by length alone. The commit must move.
    /// </summary>
    [Fact]
    public void A_flush_after_a_restart_mid_flush_moves_since_its_prefix_holds_the_successors_entries()
    {
        using (var wal = Open())
        {
            wal.Append(Span(100));
            wal.Append(Span(101));
            wal.BeginFlush();
            wal.Append(Span(102));                                                       // generation 2
        }                                                                                 // killed before the commit

        long steps = 0;
        using (var wal = Open())
        {
            Assert.Equal(Ids(100, 3), IdsOf(wal.ReadAll()));                            // the tier gets all three back
            wal.BeginFlush();                                                            // they are in this flush's segment
            for (int j = 0; j < 25; j++) wal.Append(Span(200 + j));                      // 2 500 B behind a 300 B prefix
            wal.OnRelocationStepForTest = (_, _) => steps++;
            wal.CommitFlush();
        }
        Assert.True(steps > 0, "the prefix holding the successor's span was left in front of the tail");

        using var reopened = Open();
        Assert.Equal(Ids(200, 25), IdsOf(reopened.ReadAll()));
    }

    /// <summary>
    /// A PREFIX THAT DOES NOT VERIFY IS NOT LEFT IN FRONT OF THE TAIL. A log opened over an entry
    /// that fails its checksum and never replayed (so its claim was never cut back) flushes a short
    /// prefix before a long tail. Kept in place, that entry would stand in front of the tail, and the
    /// replay — which stops at the first entry that does not verify — would never reach a span the
    /// commit kept. The commit must move the tail over it.
    /// </summary>
    [Fact]
    public void A_prefix_that_does_not_verify_is_not_left_in_front_of_the_tail()
    {
        byte[] torn = EntryBytes(Span(100), 1, checksummed: true);
        torn[EntryHead + 2] ^= 0x5A;
        WriteV3Log(1, seal: true, torn);

        using (var wal = Open())                                                         // no ReadAll: the claim stands
        {
            wal.BeginFlush();
            for (int j = 0; j < 9; j++) wal.Append(Span(200 + j));
            wal.CommitFlush();
            Assert.Equal(9L * Entry, wal.WrittenBytes);                                  // moved
        }
        using var reopened = Open();
        Assert.Equal(Ids(200, 9), IdsOf(reopened.ReadAll()));
    }

    /// <summary>
    /// A COMMIT THAT LEAVES ITS PREFIX IN PLACE, KILLED ANYWHERE. Nothing moves, so the stamp is the
    /// commit: killed before it — the data barrier, the stamp's pending checksum — the old header
    /// replays both generations (the documented duplicate window); killed after its field, the
    /// header verifies by its pending checksum and replays the tail alone — read as rot instead, the
    /// rebuild would accept the dead prefix's generation and replay it beside its segment. A rotted
    /// header over a kept prefix does exactly that: duplicates, never loss.
    /// </summary>
    [Theory]
    [InlineData("before",         false, true)]
    [InlineData("barrier",        false, true)]
    [InlineData("stamp-pending",  false, true)]
    [InlineData("stamp-unsealed", false, false)]
    [InlineData("stamp-unsealed", true,  true)]
    [InlineData("committed",      false, false)]
    [InlineData("committed",      true,  true)]
    public void A_commit_that_keeps_its_prefix_killed_anywhere_loses_nothing(string point, bool rotHeader, bool prefixReplays)
    {
        string killed = KilledCommit(1, 9, point, done: 0, intoNextChunk: 0);
        if (rotHeader) RotGeneration(killed);

        var logger = new CapturingLogger();
        using (var wal = Open(killed, logger))
        {
            Assert.Equal([.. prefixReplays ? Ids(100, 1) : [], .. Ids(200, 9)], IdsOf(wal.ReadAll()));
            Assert.Equal(10L * Entry, wal.WrittenBytes);
        }
        Assert.Equal(rotHeader, logger.Entries.Any(static e => e.Level == LogLevel.Error && e.Text.Contains("header does not verify")));
        Assert.DoesNotContain(logger.Entries, static e => e.Text.Contains("finishing the relocation"));
    }

    /// <summary>
    /// WHAT THE BOUND SAVES, printed: a one-entry prefix before ~8 MiB of minimal spans (68 B, 120 000
    /// of them) — the shape the rule keeps in place — committed with the rule, and the same tail behind
    /// a prefix as long as it, which moves in one chunk. Asserted only that the first moved nothing.
    /// Measured on the development box (Release): the many-chunk move it replaces held the append
    /// lock 10-12.5 ms against 3.3 ms for the one-chunk move, its drive flush 2 ms.
    /// </summary>
    [Fact]
    public void Probe_commit_of_a_long_tail_behind_a_short_prefix()
    {
        static SpanIngestItem Minimal(int id) => new()
        {
            TraceId = new TraceId((ulong)id + 1, 7), SpanId = new SpanId((ulong)id), StartTimeUnixNano = BaseNano + id,
            DurationNanos = 1, Name = "", ServiceName = "", AttributesBytes = [],
        };

        foreach (int prefix in (int[])[1, 120_000])
        {
            string path = Path.Combine(_dir, $"probe-{prefix}.wal");
            using var wal = SpanWriteAheadLog.Open(path, 32L * 1024 * 1024, 1L << 26);
            for (int i = 0; i < prefix; i++) wal.Append(Minimal(i));
            wal.BeginFlush();
            for (int j = 0; j < 120_000; j++) wal.Append(Minimal(1_000_000 + j));

            long steps = 0;
            wal.OnRelocationStepForTest = (_, _) => steps++;
            long t0 = System.Diagnostics.Stopwatch.GetTimestamp();
            wal.CommitFlush();
            double ms = System.Diagnostics.Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
            _out.WriteLine($"prefix {prefix} x 68 B before 120 000 x 68 B: {steps} relocation step(s), commit {ms:N2} ms");
            if (prefix == 1) Assert.Equal(0, steps);
        }
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
