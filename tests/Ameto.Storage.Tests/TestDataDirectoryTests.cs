using System.Diagnostics;
using Microsoft.Data.Sqlite;
using Ameto.Testing;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE TEARDOWN'S DELETE, HELD TO WHAT IT PROMISES. <see cref="TestDataDirectory.Delete(string)"/>
/// replaced a <c>Directory.Delete</c> under an empty <c>catch</c>, whose failure nobody could see:
/// one green run of the suite left 885 directories in TEMP. A helper that went back to returning
/// in silence would pass every test that uses it, so what it promises are facts of their own: it
/// fails over a file somebody holds and names it; it lets a pooled SQLite connection go; it waits
/// out a handle that is closing; it gets past the junction .NET's recursive delete trips over; it
/// does not replace the failure of a fact that has already failed; and it stops waiting ten
/// seconds a time once a run is leaving directories behind wholesale.
///
/// <para>Most of them are Windows facts: they hold a file the way an undisposed reader does, with
/// a share mode only Windows enforces — elsewhere an open file unlinks and there is nothing to
/// show — or make an NTFS junction.</para>
/// </summary>
public sealed class TestDataDirectoryTests
{
    private static string NewDirectory()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-testdir-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    /// <summary>A file held the way <c>SegmentReader</c> holds a segment: others may read it, nobody may delete it.</summary>
    private static FileStream Hold(string path) => new(path, FileMode.Create, FileAccess.ReadWrite, FileShare.Read);

    [Fact]
    public void A_directory_nobody_holds_is_removed_and_one_that_is_not_there_is_no_error()
    {
        string dir = NewDirectory();
        Directory.CreateDirectory(Path.Combine(dir, "segments"));
        File.WriteAllText(Path.Combine(dir, "segments", "0-1.seg"), "x");

        TestDataDirectory.Delete(dir);
        Assert.False(Directory.Exists(dir));

        TestDataDirectory.Delete(dir);   // a teardown that runs twice, or after a test that never created it
    }

    [StreamingMergeCrashSafetyTests.WindowsFact]
    public void A_file_somebody_still_holds_fails_the_delete_and_is_named()
    {
        string dir  = NewDirectory();
        string held = Path.Combine(dir, "segments", "0-7.seg");
        Directory.CreateDirectory(Path.Combine(dir, "segments"));
        File.WriteAllText(Path.Combine(dir, "free.txt"), "x");

        using (Hold(held))
        {
            var ex = Assert.Throws<TestDataDirectoryLeftBehindException>(
                () => TestDataDirectory.Delete(dir, TimeSpan.FromMilliseconds(300)));

            Assert.Contains(Path.Combine("segments", "0-7.seg"), ex.Message);   // what is held, by name
            Assert.DoesNotContain("free.txt", ex.Message);                      // what nobody held is gone
            Assert.IsAssignableFrom<IOException>(ex.InnerException);            // and the delete's own words are kept
            Assert.True(File.Exists(held));
        }

        TestDataDirectory.Delete(dir);
        Assert.False(Directory.Exists(dir));
    }

    /// <summary>
    /// The stores that pool on purpose — <c>AuthDatabase</c>, <c>AlertRuleStore</c>,
    /// <c>AlertPersistence</c> — open <c>Ameto.db</c> exactly like this, and the pool keeps the
    /// file open after the <c>using</c>: the plain delete below is the proof, and the reason 151
    /// directories of the integration project stayed in TEMP after every run.
    /// </summary>
    [StreamingMergeCrashSafetyTests.WindowsFact]
    public void A_connection_the_sqlite_pool_parked_is_let_go()
    {
        string dir = NewDirectory();
        using (var conn = new SqliteConnection($"Data Source={Path.Combine(dir, "Ameto.db")}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "PRAGMA journal_mode=WAL; CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);";
            cmd.ExecuteNonQuery();
        }
        Assert.ThrowsAny<IOException>(() => Directory.Delete(dir, recursive: true));   // setup: parked, and held

        TestDataDirectory.Delete(dir);

        Assert.False(Directory.Exists(dir));
    }

    /// <summary>
    /// The handle the retries are for: one that closes by itself a moment later, as the boot
    /// catalog scan's reader does. The holder is closed only once the delete has been seen to
    /// start — it removes the file nobody holds first — so the first attempt meets it.
    /// </summary>
    [StreamingMergeCrashSafetyTests.WindowsFact]
    public async Task A_handle_that_closes_a_moment_later_is_waited_out()
    {
        string dir    = NewDirectory();
        string canary = Path.Combine(dir, "a-first.txt");   // deleted first: directory order
        File.WriteAllText(canary, "x");
        var holder = Hold(Path.Combine(dir, "b-held.seg"));

        var release = Task.Run(async () =>
        {
            try { while (File.Exists(canary)) await Task.Delay(1); }
            finally { holder.Dispose(); }
        });

        var sw = Stopwatch.StartNew();
        TestDataDirectory.Delete(dir);
        sw.Stop();
        await release;

        Assert.False(Directory.Exists(dir));
        Assert.True(sw.Elapsed < TestDataDirectory.RetryLimit, $"the delete took {sw.Elapsed}, the whole retry limit");
    }

    /// <summary>
    /// .NET's recursive delete (10.0.15) throws "Access to the path is denied" for a directory
    /// junction after it has removed the junction and everything else, and leaves the emptied
    /// directory: four facts of <c>ColdSegmentFaultTests</c> left one each, every run.
    /// </summary>
    [StreamingMergeCrashSafetyTests.WindowsFact]
    public void A_junction_inside_does_not_leave_the_emptied_directory_behind()
    {
        string dir  = NewDirectory();
        string real = Path.Combine(dir, "real");
        string link = Path.Combine(dir, "link");
        Directory.CreateDirectory(real);
        File.WriteAllText(Path.Combine(real, "spans.wal"), "x");
        Assert.True(TryJunction(link, real), "setup: mklink /J made no junction");

        TestDataDirectory.Delete(dir);

        Assert.False(Directory.Exists(dir));
    }

    // ── A fact that has already failed keeps its own failure ──────────────────

    /// <summary>
    /// The teardown of a fact that failed half way, with what it opened still open: a
    /// <c>finally</c> (or a <c>using</c>) whose delete cannot succeed. The exception that comes
    /// out must be the fact's own, and soon — not a left-behind directory after ten seconds, which
    /// is what a report showed in place of the assertion that had failed.
    /// </summary>
    [StreamingMergeCrashSafetyTests.WindowsFact]
    public void A_delete_in_the_finally_of_a_fact_that_failed_does_not_replace_its_failure()
    {
        string dir  = NewDirectory();
        string held = Path.Combine(dir, "wal", "0-1.wal");
        Directory.CreateDirectory(Path.Combine(dir, "wal"));
        using var holder = Hold(held);

        // An Action by name: a lambda that only throws converts to Func<Task> as well, and xunit refuses that overload.
        Action failingFact = () =>
        {
            try { throw new InvalidOperationException("the fact's own failure"); }
            finally { TestDataDirectory.Delete(dir); }
        };

        var sw = Stopwatch.StartNew();
        var ex = Assert.Throws<InvalidOperationException>(failingFact);
        sw.Stop();

        Assert.Equal("the fact's own failure", ex.Message);
        Assert.True(File.Exists(held), "best effort: what is held stays, and nothing is said about it");
        Assert.True(sw.Elapsed < TestDataDirectory.RetryLimit / 2, $"it waited {sw.Elapsed} for a fact that had already failed");

        holder.Dispose();
        TestDataDirectory.Delete(dir);
        Assert.False(Directory.Exists(dir));
    }

    /// <summary>
    /// What the fact above stands on, held to the runtime it was measured on (.NET 10.0.15): an
    /// exception that is unwinding can be seen from the <c>finally</c> and the <c>Dispose</c> it
    /// runs through, and from nowhere else. A runtime that always answered "yes" would make every
    /// teardown silent again; one that always answered "no" would bring the replaced failures
    /// back. Either turns this red instead.
    /// </summary>
    [StreamingMergeCrashSafetyTests.WindowsFact]
    public async Task An_exception_on_its_way_is_seen_from_the_finally_it_runs_through_and_nowhere_else()
    {
        Assert.False(TestDataDirectory.FailureInFlight);                       // plain code

        bool inNormalFinally = true;
        try { }
        finally { inNormalFinally = TestDataDirectory.FailureInFlight; }
        Assert.False(inNormalFinally);                                         // a finally reached normally

        bool inUnwindingFinally = false, inCatch = false;
        try
        {
            try { throw new InvalidOperationException("x"); }
            finally { inUnwindingFinally = TestDataDirectory.FailureInFlight; }
        }
        catch (InvalidOperationException) { inCatch = TestDataDirectory.FailureInFlight; }
        Assert.True(inUnwindingFinally);                                       // a finally an exception runs through
        Assert.True(inCatch);                                                  // and the catch block that handles it
        Assert.False(TestDataDirectory.FailureInFlight);                       // over once that block has ended

        bool afterInnerCatch = true;
        try { try { throw new InvalidOperationException("caught below"); } catch (InvalidOperationException) { } }
        finally { afterInnerCatch = TestDataDirectory.FailureInFlight; }
        Assert.False(afterInnerCatch);                                         // an exception caught deeper down is not one on its way

        // A `using` in an async method whose body throws after an await: the shape of
        // `using var dir = new TempDir();` in an async fact.
        var seen = new Probe();
        await Assert.ThrowsAsync<InvalidOperationException>(() => ThrowsAfterAnAwait(seen));
        Assert.True(seen.InFlightAtDispose);

        var quiet = new Probe();
        await CompletesAfterAnAwait(quiet);
        Assert.False(quiet.InFlightAtDispose);
    }

    private sealed class Probe : IDisposable
    {
        public bool InFlightAtDispose { get; private set; }
        public void Dispose() => InFlightAtDispose = TestDataDirectory.FailureInFlight;
    }

    private static async Task ThrowsAfterAnAwait(Probe probe)
    {
        using var _ = probe;
        await Task.Yield();
        throw new InvalidOperationException("y");
    }

    private static async Task CompletesAfterAnAwait(Probe probe)
    {
        using var _ = probe;
        await Task.Yield();
    }

    // ── A run that leaves directories behind wholesale ────────────────────────

    /// <summary>
    /// Ten seconds of retries in each of a few hundred teardowns is longer than CI's job limit, and
    /// a regression that leaves a handle in every test would end as a cancelled job with no test
    /// named. After the third directory left behind in a process the wait is half a second.
    /// </summary>
    [Fact]
    public void After_three_directories_left_behind_the_retries_are_short()
    {
        Assert.Equal(TestDataDirectory.RetryLimit, TestDataDirectory.RetryLimitAfter(0));
        Assert.Equal(TestDataDirectory.RetryLimit, TestDataDirectory.RetryLimitAfter(TestDataDirectory.FailuresBeforeShortRetries - 1));
        Assert.Equal(TestDataDirectory.ShortRetryLimit, TestDataDirectory.RetryLimitAfter(TestDataDirectory.FailuresBeforeShortRetries));
        Assert.Equal(TestDataDirectory.ShortRetryLimit, TestDataDirectory.RetryLimitAfter(500));

        Assert.True(TestDataDirectory.ShortRetryLimit <= TimeSpan.FromSeconds(1));
        Assert.True(TestDataDirectory.RetryLimit >= TimeSpan.FromSeconds(10), "a slow runner's closing handle needs the long wait while failures are few");
    }

    private static bool TryJunction(string link, string target)
    {
        var psi = new ProcessStartInfo("cmd.exe", $"/c mklink /J \"{link}\" \"{target}\"")
        {
            RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false,
        };
        using var p = Process.Start(psi)!;
        p.WaitForExit();
        return p.ExitCode == 0 && Directory.Exists(link);
    }
}
