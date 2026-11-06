using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// #131: the empty log a clean stop leaves is the only record on disk of the last segment-id block
/// that run handed out (a run that flushed nothing left no segment file to seed the allocator from).
/// The startup replay deleted it as soon as it had read it, and the boot log that takes its place was
/// created later, after every other orphan was replayed and the spill files discovered. A crash — or
/// a throw — in that window left no log at all, and the next start handed the same block out again:
/// a replica holding a segment of that (node, id) answered 409 different-segment, and the segment
/// written under the old id could meet a log not yet replayed. The spent log now goes only once the
/// boot log exists.
/// </summary>
public sealed class BootWalMarkWindowTests : IDisposable
{
    private const int Slots = 6;   // StorageEngine.LevelSegmentSlots: a live log's block

    private readonly string _root = Path.Combine(Path.GetTempPath(), "ameto-bootmark-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch { /* best-effort */ }
    }

    private string Dir(string name) => Path.Combine(_root, name);

    private static StorageEngine NewEngine(string dir)
    {
        Directory.CreateDirectory(dir);
        var opts = new ServerOptions { DataDirectory = dir, NodeId = new NodeId(0) };
        return new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
    }

    /// <summary>
    /// Run one stops clean, leaving its empty boot log. Run two dies the moment it is about to open its
    /// own boot log — after the replay has read run one's log, which it used to delete right there.
    /// Run three must still know how far run one got.
    /// </summary>
    [Fact]
    public async Task A_crash_between_the_spent_log_and_the_new_boot_log_keeps_the_mark()
    {
        string dir = Dir("window");
        ulong handedOut;
        await using (var first = NewEngine(dir))
        {
            await first.CatalogLoaded;
            handedOut = first.LiveWalSegmentId + Slots - 1;     // the last id of its boot block
        }
        Assert.Single(Directory.GetFiles(Path.Combine(dir, "wal"), "*.wal"));   // the clean stop's empty log

        StorageEngine.BeforeBootWalOpenForTest.Value = static () => throw new IOException("crash before the boot log (test seam)");
        try
        {
            var crash = Record.Exception(() => NewEngine(dir));
            Assert.IsType<IOException>(crash);
        }
        finally { StorageEngine.BeforeBootWalOpenForTest.Value = null; }

        // Before: the replay had deleted run one's log, run two died before writing its own, and this
        // directory held no log — run three seeded from segment names alone, of which there are none.
        Assert.Single(Directory.GetFiles(Path.Combine(dir, "wal"), "*.wal"));

        await using var third = NewEngine(dir);
        await third.CatalogLoaded;
        Assert.True(third.LiveWalSegmentId > handedOut,
            $"the restart handed out block {third.LiveWalSegmentId} again; the first run had reached {handedOut}");
        Assert.True(third.AllocateSegmentId() > handedOut);
    }

    /// <summary>
    /// The ordinary start: the spent log is gone once the boot log is open — the deferral leaves no
    /// litter behind, and the two never coexist past the constructor.
    /// </summary>
    [Fact]
    public async Task A_normal_start_still_removes_the_spent_log_once_its_boot_log_exists()
    {
        string dir = Dir("normal");
        string firstLog;
        await using (var first = NewEngine(dir))
        {
            await first.CatalogLoaded;
            firstLog = Assert.Single(Directory.GetFiles(Path.Combine(dir, "wal"), "*.wal"));
        }

        await using var next = NewEngine(dir);
        await next.CatalogLoaded;
        Assert.False(File.Exists(firstLog), "the spent log of the previous run was not removed");
        Assert.False(File.Exists(firstLog + ".pool"), "its pool was not removed");
        Assert.Single(Directory.GetFiles(Path.Combine(dir, "wal"), "*.wal"));   // the new boot log alone
    }

    /// <summary>
    /// A log whose header cannot be read (its first 32 bytes zeroed: magic, version, block id) still
    /// reserves the block its NAME carries — and is cleaned up once the boot log exists, not before.
    /// </summary>
    [Fact]
    public async Task A_log_whose_header_cannot_be_read_still_reserves_the_block_its_name_carries()
    {
        string dir    = Dir("corrupt-header");
        string walDir = Path.Combine(dir, "wal");
        Directory.CreateDirectory(walDir);
        string wal = Path.Combine(walDir, "0-5000.wal");
        WriteAheadLog.Open(wal, new NodeId(0), new SegmentId(5000)).Dispose();
        using (var fs = new FileStream(wal, FileMode.Open, FileAccess.ReadWrite))
            fs.Write(new byte[32]);

        ulong boot;
        await using (var engine = NewEngine(dir))
        {
            await engine.CatalogLoaded;
            boot = engine.LiveWalSegmentId;
            Assert.True(boot >= 5000 + (ulong)Slots, $"the boot block {boot} overlaps the block the unreadable log is named for");
            Assert.False(File.Exists(wal), "the unreadable log was not cleaned up once the boot log existed");
        }

        // And the boot log now carries the mark forward on its own.
        await using var next = NewEngine(dir);
        await next.CatalogLoaded;
        Assert.True(next.LiveWalSegmentId >= boot + (ulong)Slots);
    }
}
