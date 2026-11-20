using System.Diagnostics;
using Ameto.Testing;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE TEARDOWN'S DELETE, HELD TO WHAT IT PROMISES. <see cref="TestDataDirectory.Delete(string)"/>
/// replaced a <c>Directory.Delete</c> under an empty <c>catch</c>, whose failure nobody could see:
/// one green run of the suite left 885 directories in TEMP. A helper that went back to returning
/// in silence would pass every test that uses it, so its three promises are facts of their own —
/// it fails over a file somebody holds, it waits out a handle that is closing, and it gets past the
/// junction .NET's recursive delete trips over.
///
/// <para>Three of the four are Windows facts. Two hold a file the way an undisposed reader does,
/// with a share mode only Windows enforces — elsewhere an open file unlinks and there is nothing
/// to show — and one makes an NTFS junction.</para>
/// </summary>
public sealed class TestDataDirectoryTests
{
    private static string NewDirectory()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-testdir-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

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

        using (new FileStream(held, FileMode.Create, FileAccess.ReadWrite, FileShare.Read))   // as SegmentReader holds a segment
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
    /// The handle the retries are for: one that closes by itself a moment later, as the boot
    /// catalog scan's reader does. The holder is closed only once the delete has been seen to
    /// start — it removes the file nobody holds first — so the first attempt meets it.
    /// </summary>
    [StreamingMergeCrashSafetyTests.WindowsFact]
    public async Task A_handle_that_closes_a_moment_later_is_waited_out()
    {
        string dir    = NewDirectory();
        string canary = Path.Combine(dir, "a-first.txt");   // deleted first: directory order
        string held   = Path.Combine(dir, "b-held.seg");
        File.WriteAllText(canary, "x");
        var holder = new FileStream(held, FileMode.Create, FileAccess.ReadWrite, FileShare.Read);

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
