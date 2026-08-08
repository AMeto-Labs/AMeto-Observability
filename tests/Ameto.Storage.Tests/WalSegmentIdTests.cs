using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// The WAL is named from the block of segment ids ITS events will flush into, and startup
/// decides "this WAL was already flushed" by looking for a segment carrying one of them. That
/// makes the reservation load-bearing: anything else allowed to take an id out of the live
/// WAL's block turns the restart check into a delete of un-flushed events.
///
/// <para>Compaction was exactly that. It reserved <c>_nextSegmentId</c> — the very id the open
/// WAL had been named from — so the first merge after any flush published a segment that
/// answered the check on the WAL's behalf. Measured before the fix: WAL id 25, merged segment
/// id 25, 30 events in the WAL, 0 recovered, on 3 of 3 runs.</para>
/// </summary>
public sealed class WalSegmentIdTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-walid-" + Guid.NewGuid().ToString("N"));
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
        NullLogger<StorageEngine>.Instance)
    {
        _allowIndexlessMerge = true,
    };

    private static byte[] Props(int i)
    {
        var buf = new ArrayBufferWriter<byte>(48);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write((long)i);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private void Write(int round, int count, long baseTicks)
    {
        for (int i = 0; i < count; i++)
        {
            int n = round * 1000 + i;
            Assert.True(_engine.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)(round * 100_000 + i)).RawValue,
                TimestampUtcTicks        = baseTicks + n * TimeSpan.TicksPerMillisecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {n}"),
            }, Props(n)));
        }
    }

    private async Task WriteSegmentAsync(int round, int count, long baseTicks)
    {
        Write(round, count, baseTicks);
        await _engine.FlushHotTierAsync();
    }

    private int ColdEventCount()
    {
        int total = 0;
        foreach (var seg in _engine.ListSegments())
        {
            using var r = SegmentReader.Open(seg.FilePath);
            total += r.ReadAllRaw(new Dictionary<string, string>(StringComparer.Ordinal)).Count;
        }
        return total;
    }

    /// <summary>Copies the live WAL and its pool aside — the on-disk image a kill -9 would leave.</summary>
    private (string Wal, string Pool) SnapshotWal()
    {
        var wal  = Directory.GetFiles(WalDir, "*.wal").Single();
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

    private async Task RestartAsync()
    {
        await _engine.DisposeAsync();
        _engine = NewEngine();
        for (int i = 0; i < 200 && _engine.ListSegments().Count == 0; i++) await Task.Delay(25);
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// The one-line invariant: after compaction, no segment carries an id out of the live WAL's
    /// reserved block. This assertion alone fails on the pre-fix planner.
    ///
    /// <para>The four sources sit in a SEALED bucket, so the merge runs on the 2-source
    /// threshold. Anchored on the grid rather than at <c>UtcNow - 5d</c>, which lands in the
    /// open bucket — and its fanout of 8 — for two days in every seven
    /// (see <see cref="MergeBucketGrid"/>).</para>
    /// </summary>
    [Fact]
    public async Task AMergeNeverTakesAnIdOutOfTheLiveWalsBlock()
    {
        long old = MergeBucketGrid.SealedBucketStart(LogLevel.Information);
        for (int round = 0; round < 4; round++)
            await WriteSegmentAsync(round, 50, old + round * TimeSpan.TicksPerHour);

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));

        ulong wal = _engine.LiveWalSegmentId;
        foreach (var seg in _engine.ListSegments())
            Assert.False(seg.Id.Value >= wal && seg.Id.Value < wal + 6,
                $"segment {seg.Id.Value} sits inside the live WAL's reserved block [{wal}, {wal + 6})");
    }

    /// <summary>
    /// The loss itself, end to end: events that exist only in the WAL when the process dies must
    /// come back after a merge has run. The crash is reconstructed by keeping the WAL bytes,
    /// letting shutdown flush and remove them, then deleting the segments that flush produced and
    /// putting the WAL back — precisely the on-disk state a kill -9 leaves.
    /// </summary>
    [Fact]
    public async Task EventsLivingOnlyInTheWalSurviveACrashAfterAMerge()
    {
        long old = MergeBucketGrid.SealedBucketStart(LogLevel.Information);
        for (int round = 0; round < 4; round++)
            await WriteSegmentAsync(round, 50, old + round * TimeSpan.TicksPerHour);

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        Assert.Equal(200, ColdEventCount());

        // 30 events that live ONLY in the WAL.
        ulong walId = _engine.LiveWalSegmentId;
        Write(99, 30, DateTime.UtcNow.Ticks);
        var snap = SnapshotWal();

        // Shutdown flushes those 30 events and removes the WAL. Undo both: delete every segment
        // the WAL's own block produced, restore the WAL, and the tree reads "never flushed".
        await _engine.DisposeAsync();
        foreach (var f in Directory.GetFiles(SegDir, "*.seg"))
        {
            var parts = Path.GetFileNameWithoutExtension(f).Split('-');
            if (ulong.TryParse(parts[1], out var id) && id >= walId && id < walId + 6) File.Delete(f);
        }
        RestoreWal(snap);

        _engine = NewEngine();
        for (int i = 0; i < 200 && _engine.ListSegments().Count == 0; i++) await Task.Delay(25);

        Assert.Equal(230, ColdEventCount());
    }

    /// <summary>
    /// The complementary direction: a WAL whose flush DID complete must not be replayed, or its
    /// events are served twice. A tier of pure Information events writes segment
    /// <c>walId + 2</c> and nothing at <c>walId</c>, so the check has to probe the whole reserved
    /// block — and read the segment DIRECTORY, since the catalog it used to consult is still
    /// being loaded on a background thread at that moment.
    /// </summary>
    [Fact]
    public async Task AFlushedWalIsNotReplayed_EvenWhenItsFirstLevelSlotIsEmpty()
    {
        ulong walId = _engine.LiveWalSegmentId;
        Write(0, 60, DateTime.UtcNow.Ticks);
        var snap = SnapshotWal();

        await _engine.FlushHotTierAsync();
        var flushed = _engine.ListSegments().Single();
        Assert.Equal(walId + (ulong)LogLevel.Information, flushed.Id.Value);   // nothing at walId itself

        // A crash between "segment written" and "WAL deleted" leaves the old WAL behind.
        RestoreWal(snap);
        await RestartAsync();

        Assert.Equal(60, ColdEventCount());   // a replay would serve 120
    }
}
