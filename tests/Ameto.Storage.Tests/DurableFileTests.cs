using Ameto.Core;
using Ameto.Metrics.Storage;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE COMMIT POINT OF BOTH WRITE-AHEAD LOGS' FORMAT UPGRADES: <see cref="DurableFile"/>. Moved to
/// Ameto.Core from the metric WAL (44797aa, bedba1b) when the span WAL's upgrade (#103) needed the
/// same rename; these facts moved with it from <c>MetricWalV2Tests</c>. The backend-linux CI job runs
/// this class: on Linux the rename is <c>File.Move</c> and the directory fsync is the path under test.
/// </summary>
public sealed class DurableFileTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-durable-" + Guid.NewGuid().ToString("N"));

    public DurableFileTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    /// <summary>
    /// THE UPGRADE'S COMMIT POINT REPLACES THE LOG, AND FAILS THE WAY ITS RETRY EXPECTS. On Windows
    /// the move is MoveFileEx with MOVEFILE_WRITE_THROUGH — a durable rename, which File.Move cannot
    /// ask for; elsewhere it is File.Move and the directory is fsynced after it. Durability itself is
    /// not observable from a test; what is pinned is the contract the upgrades lean on: the file is
    /// replaced, a destination held open fails as IOException or UnauthorizedAccessException (the two
    /// the retry filters on, where a Win32Exception would escape it and fail the start), and the
    /// directory sync succeeds.
    /// </summary>
    [Fact]
    public void The_durable_move_replaces_the_log_and_fails_the_way_the_retry_expects()
    {
        string from = Path.Combine(_dir, "a.tmp"), to = Path.Combine(_dir, "b.wal");
        File.WriteAllBytes(from, [1, 2, 3]);
        File.WriteAllBytes(to,   [9]);

        DurableFile.Replace(from, to);
        Assert.Equal([1, 2, 3], File.ReadAllBytes(to));
        Assert.False(File.Exists(from));
        Assert.True(DurableFile.SyncDirectory(_dir));

        var missing = Record.Exception(() => DurableFile.Replace(from, to));
        Assert.True(missing is IOException, $"a missing source threw {missing?.GetType().Name}");

        if (OperatingSystem.IsWindows())
        {
            File.WriteAllBytes(from, [4]);
            using var held = new FileStream(to, FileMode.Open, FileAccess.Read, FileShare.None);
            var locked = Record.Exception(() => DurableFile.Replace(from, to));
            Assert.True(locked is IOException or UnauthorizedAccessException,
                $"a held destination threw {locked?.GetType().Name}");
        }
    }

    /// <summary>
    /// THE DURABLE MOVE WORKS PAST MAX_PATH. MoveFileExW without the <c>\\?\</c> form is limited to
    /// 260 characters where File.Move is not, so a data directory deep enough failed every upgrade
    /// on Windows — six attempts, then the log left in its old version at every start. The move is
    /// made into a directory whose path alone is over 260 characters.
    /// </summary>
    [Fact]
    public void The_durable_move_works_past_max_path()
    {
        string deep = _dir;
        while (deep.Length < 300) deep = Path.Combine(deep, "a-directory-name-of-forty-characters-xx");
        Directory.CreateDirectory(deep);
        string from = Path.Combine(deep, "spans.wal.upgrade.tmp"), to = Path.Combine(deep, "spans.wal");
        File.WriteAllBytes(from, [1, 2, 3]);
        File.WriteAllBytes(to,   [9]);

        DurableFile.Replace(from, to);

        Assert.Equal([1, 2, 3], File.ReadAllBytes(to));
        Assert.False(File.Exists(from));
        Assert.True(DurableFile.SyncDirectory(deep));
    }

    /// <summary>
    /// BOTH LOGS' UPGRADES COMMIT WITH IT. The span WAL's upgrade used to rename with a plain
    /// File.Move — atomic, not durable: until the directory entry reached the disk, a power loss could
    /// bring the old log back under the name, with the generation it had at the upgrade, and every
    /// span logged to the new file since would be gone.
    /// </summary>
    [Fact]
    public void Both_write_ahead_logs_upgrade_through_the_durable_move()
    {
        Action<string, string> durable = DurableFile.Replace;
        Assert.Equal(durable, SpanWriteAheadLog.UpgradeIo.Default.Move);
        Assert.Equal(durable, MetricWriteAheadLog.UpgradeIo.Default.Move);
    }
}
