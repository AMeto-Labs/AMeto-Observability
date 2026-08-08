using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// A flush DELETES THE WAL, so the decision "this WAL was already flushed" has to be exact.
///
/// <para>A tier is written as one segment PER LEVEL, published one <c>File.Move</c> at a time, so
/// between the first move and the last the segment directory holds a PARTIAL set. Reading that as
/// "flushed" — which "some segment inside the WAL's reserved block exists" does, for a set of one
/// — deletes the only copy of every level that had not been published yet. These tests reconstruct
/// each interruption point and restart the engine on it.</para>
///
/// <para>The protocol they pin: build every level at <c>.seg.tmp</c> → move them all → write and
/// fsync <c>{nodeId}-{firstSegId}.flushed</c> → delete the WAL → drop the marker. Only a present
/// marker means flushed; a markerless WAL is replayed, and the replay writes only the levels whose
/// reserved id carries no file, which is what keeps it idempotent.</para>
/// </summary>
public sealed class FlushCrashSafetyTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-flushcrash-" + Guid.NewGuid().ToString("N"));
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
        _engine._afterLevelPublished = null;
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private StorageEngine NewEngine() => new(
        Options.Create(new ServerOptions { DataDirectory = _dir }),
        new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
        NullLogger<StorageEngine>.Instance)
    {
        _allowIndexlessMerge = true,
    };

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static byte[] Props(int i)
    {
        var buf = new ArrayBufferWriter<byte>(48);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write((long)i);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>Writes <paramref name="count"/> events of one level, ids <c>tag * 1000 + i</c>.</summary>
    private void Write(int tag, int count, LogLevel level, long baseTicks)
    {
        for (int i = 0; i < count; i++)
        {
            int n = tag * 1000 + i;
            Assert.True(_engine.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)n).RawValue,
                TimestampUtcTicks        = baseTicks + n * TimeSpan.TicksPerMillisecond,
                Level                    = level,
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {n}"),
            }, Props(n)));
        }
    }

    /// <summary>Every event the engine can serve from cold storage.</summary>
    private List<RawSegmentEvent> ColdEvents()
    {
        var all = new List<RawSegmentEvent>();
        var dedup = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var seg in _engine.ListSegments())
        {
            using var r = SegmentReader.Open(seg.FilePath);
            all.AddRange(r.ReadAllRaw(dedup));
        }
        return all;
    }

    /// <summary>Segment ids present as FILES, independent of how far the catalog scan has got.</summary>
    private HashSet<ulong> SegmentIdsOnDisk()
    {
        var ids = new HashSet<ulong>();
        foreach (var f in Directory.GetFiles(SegDir, "*.seg"))
        {
            var parts = Path.GetFileNameWithoutExtension(f).Split('-');
            if (parts.Length >= 2 && ulong.TryParse(parts[1], out var id)) ids.Add(id);
        }
        return ids;
    }

    /// <summary>Copies the live WAL and its pool aside — the on-disk image a kill -9 would leave.</summary>
    private (string Wal, string Pool) SnapshotWal()
    {
        var wal = Directory.GetFiles(WalDir, "*.wal").Single();
        File.Copy(wal, wal + ".crash", overwrite: true);
        if (File.Exists(wal + ".pool")) File.Copy(wal + ".pool", wal + ".pool.crash", overwrite: true);
        return (wal, wal + ".pool");
    }

    private static void RestoreWal((string Wal, string Pool) snap)
    {
        File.Copy(snap.Wal + ".crash", snap.Wal, overwrite: true);
        if (File.Exists(snap.Pool + ".crash")) File.Copy(snap.Pool + ".crash", snap.Pool, overwrite: true);
        File.Delete(snap.Wal + ".crash");
        if (File.Exists(snap.Pool + ".crash")) File.Delete(snap.Pool + ".crash");
    }

    /// <summary>Restarts and waits for the catalog scan to register at least <paramref name="expectSegments"/>.</summary>
    private async Task RestartAsync(int expectSegments)
    {
        _engine._afterLevelPublished = null;
        await _engine.DisposeAsync();
        _engine = NewEngine();
        for (int i = 0; i < 400 && _engine.ListSegments().Count < expectSegments; i++)
            await Task.Delay(25);
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// THE LOSS. A flush that dies between two level moves leaves one level on disk and the rest
    /// only in the WAL. The existential predicate reads that single file as proof of a completed
    /// flush and deletes the WAL, taking every unpublished level with it.
    ///
    /// <para>Measured against the predicate this replaces: 80 events in, one Information segment
    /// published, the WAL deleted on restart, 40 events recoverable. The 40 Errors were gone —
    /// and with <c>SegmentWriter.Finalise</c> now fsyncing, the partial state that triggers it
    /// survives a power loss and not merely a process kill.</para>
    ///
    /// <para>The kill is real, not reconstructed: <c>_afterLevelPublished</c> throws at the seam
    /// between the two moves, which is also the in-process failure branch — that branch KEEPS the
    /// WAL for replay, and the already-moved level file then answered the predicate on the next
    /// restart anyway.</para>
    /// </summary>
    [Fact]
    public async Task AFlushKilledBetweenTwoLevelMovesRecoversEveryEvent()
    {
        long baseTicks = DateTime.UtcNow.Ticks;
        Write(0, 40, LogLevel.Information, baseTicks);
        Write(1, 40, LogLevel.Error, baseTicks);

        ulong walId = _engine.LiveWalSegmentId;
        ulong infoId = walId + (ulong)LogLevel.Information;
        ulong errId  = walId + (ulong)LogLevel.Error;

        int published = 0;
        _engine._afterLevelPublished = _ =>
        {
            if (++published == 1) throw new IOException("killed between two level moves");
        };

        await _engine.FlushHotTierAsync();   // the flush path logs and returns; it does not throw

        // The state a crash at that seam leaves: Information published, Error not, the WAL that
        // holds BOTH still on disk, and no completion marker to say the block is done.
        var onDisk = SegmentIdsOnDisk();
        Assert.Contains(infoId, onDisk);
        Assert.DoesNotContain(errId, onDisk);
        Assert.Single(Directory.GetFiles(WalDir, $"*-{walId}.wal"));
        Assert.Empty(Directory.GetFiles(WalDir, "*.flushed"));

        await RestartAsync(2);

        var events = ColdEvents();
        Assert.Equal(80, events.Count);
        Assert.Equal(80, events.Select(e => e.Id).Distinct().Count());
        Assert.Equal(40, events.Count(e => e.Level == (byte)LogLevel.Error));
        Assert.Equal(40, events.Count(e => e.Level == (byte)LogLevel.Information));

        // The level that DID get published was kept, not rewritten — recovery fills the holes.
        Assert.Contains(infoId, SegmentIdsOnDisk());
        Assert.Contains(errId,  SegmentIdsOnDisk());
    }

    /// <summary>
    /// The complementary direction, on the interruption that matters most for an upgrade: a WAL
    /// whose flush COMPLETED but which carries no marker, because the build that flushed it never
    /// wrote one. Recovery must not read the missing marker as "never flushed" and replay it into
    /// duplicates.
    ///
    /// <para>It does not, and the reason is that the replay publishes only the levels whose
    /// reserved id has no file: a level is one move of one whole file, so a segment sitting at
    /// <c>walId + level</c> means that level is fully persisted. An old directory's complete
    /// flush therefore replays to zero new segments and the WAL is simply dropped.</para>
    /// </summary>
    [Fact]
    public async Task AWalFlushedByThePreviousBuildIsNotReplayedIntoDuplicates()
    {
        long baseTicks = DateTime.UtcNow.Ticks;
        Write(0, 30, LogLevel.Information, baseTicks);
        Write(1, 30, LogLevel.Error, baseTicks);

        ulong walId = _engine.LiveWalSegmentId;
        var   snap  = SnapshotWal();

        await _engine.FlushHotTierAsync();
        Assert.Equal(2, _engine.ListSegments().Count);

        // The image the previous build leaves: both level segments on disk, the WAL still there
        // because the crash beat its unlink — and no marker anywhere, ever.
        foreach (var m in Directory.GetFiles(WalDir, "*.flushed")) File.Delete(m);
        RestoreWal(snap);
        Assert.Empty(Directory.GetFiles(WalDir, "*.flushed"));

        await RestartAsync(2);

        var events = ColdEvents();
        Assert.Equal(60, events.Count);
        Assert.Equal(60, events.Select(e => e.Id).Distinct().Count());
        Assert.Equal(2, SegmentIdsOnDisk().Count);
        Assert.Empty(Directory.GetFiles(WalDir, $"*-{walId}.wal"));
    }

    /// <summary>
    /// The marker is deleted with the WAL it authorises, so a healthy directory accumulates
    /// none — and one left behind by a crash in the gap between the two unlinks is swept at
    /// startup rather than kept forever.
    /// </summary>
    [Fact]
    public async Task TheMarkerIsDroppedWithItsWalAndAStaleOneIsSwept()
    {
        ulong walId = _engine.LiveWalSegmentId;
        Write(0, 20, LogLevel.Information, DateTime.UtcNow.Ticks);
        await _engine.FlushHotTierAsync();

        Assert.Empty(Directory.GetFiles(WalDir, "*.flushed"));
        Assert.Empty(Directory.GetFiles(WalDir, $"*-{walId}.wal"));

        // A crash between "WAL unlinked" and "marker unlinked".
        var stale = Path.Combine(WalDir, $"{NodeId.Local.Value}-{walId}.flushed");
        File.WriteAllText(stale, "");

        await RestartAsync(1);

        Assert.False(File.Exists(stale));
        Assert.Equal(20, ColdEvents().Count);
    }
}
