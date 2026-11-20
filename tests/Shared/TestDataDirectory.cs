using Microsoft.Data.Sqlite;

namespace Ameto.Testing;

/// <summary>
/// Removes a test's data directory, and FAILS THE TEST WHEN IT CANNOT. Linked into the test
/// projects that build engines, stores or hosts over a directory under TEMP.
///
/// <para>WHY NOT <c>try { Directory.Delete(dir, true); } catch { }</c>. On Windows a file somebody
/// still holds open cannot be deleted, a recursive delete removes everything else and throws for
/// that one, and the empty <c>catch</c> made the throw say nothing: the directory stayed in TEMP
/// for ever and no run was red. Measured on one developer machine in November 2026: 885
/// <c>ameto-*</c> directories and 101 MB left by ONE green run of the suite, 36 472 directories
/// and 5.4 GB by the time anyone looked. All but six of the 885 held only <c>Ameto.db</c>, which
/// <c>RetentionStore</c> left open in Microsoft.Data.Sqlite's connection pool until that store
/// stopped pooling. The rest are the reasons a delete can still fail, and what this helper is for:</para>
/// <list type="bullet">
///   <item>A STORE THAT POOLS ON PURPOSE. <c>AuthDatabase</c>, <c>AlertRuleStore</c> and
///   <c>AlertPersistence</c> open <c>Ameto.db</c> through the pool, which is right for a server:
///   one data directory, kept until the process exits. A test that removes the directory is the
///   one that has to let the pool go, so it is cleared here first.</item>
///   <item>A HANDLE THAT IS ABOUT TO CLOSE. The boot catalog scan of an engine is a task its
///   <c>DisposeAsync</c> does not wait for, and it holds one segment open at a time: a delete that
///   meets it leaves that one <c>.seg</c>. An antivirus or an indexer reading files written a
///   moment ago may do the same. Both let go by themselves, so the delete is retried, backing off
///   from 10 ms, for <see cref="RetryLimit"/> in all. A delete that succeeds at once, the normal
///   case, costs nothing extra.</item>
///   <item>A DIRECTORY JUNCTION INSIDE. .NET's recursive delete (10.0.15) throws "Access to the
///   path is denied" for the junction after it has removed the junction and everything else, and
///   leaves the emptied directory. The second attempt finds no junction.</item>
///   <item>A HANDLE NOBODY WILL CLOSE: an engine, a reader or a mapping that was not disposed.
///   That is a defect — in the test, or in what it tests, where the same handle blocks a
///   retention delete or a merge on a Windows server — and it outlasts any retry. This is the
///   case that throws, naming the files. (One such engine, left half torn down on purpose by a
///   single test, kept a 64 MB log: 2.6 of those 5.4 GB.)</item>
/// </list>
///
/// <para>WHERE THE FAILURE SHOWS, measured with xunit 2.9.3 under <c>dotnet test</c>. From the
/// <c>Dispose</c> or <c>DisposeAsync</c> of a test class the test fails with the message below,
/// beside its own failure if it had one. From a class fixture the tests stay Passed, the run
/// exits 1, and at <c>-v minimal</c> — CI's verbosity — the log holds one line naming the class
/// and the exception's TYPE, with no message: hence a type of its own,
/// <see cref="TestDataDirectoryLeftBehindException"/>, so that line says what happened. In a
/// <c>finally</c> the throw REPLACES the test's own failure, C# keeping one exception; the
/// message says so.</para>
/// </summary>
internal static class TestDataDirectory
{
    /// <summary>How long <see cref="Delete(string)"/> keeps retrying a delete that a handle blocks.</summary>
    public static readonly TimeSpan RetryLimit = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Deletes <paramref name="dir"/> and everything in it; does nothing when it is not there.
    /// Call it once everything the test opened in the directory has been disposed.
    /// </summary>
    /// <exception cref="TestDataDirectoryLeftBehindException">The directory is still there after <see cref="RetryLimit"/>.</exception>
    public static void Delete(string dir) => Delete(dir, RetryLimit);

    /// <summary><see cref="Delete(string)"/> with a retry limit of the caller's: for the facts about this helper.</summary>
    internal static void Delete(string dir, TimeSpan retryLimit)
    {
        SqliteConnection.ClearAllPools();

        long      started = Environment.TickCount64;
        Exception last;
        for (int backoffMs = 10; ; backoffMs = Math.Min(backoffMs * 2, 500))
        {
            try
            {
                Directory.Delete(dir, recursive: true);
                return;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // Gone, whoever removed it. A DirectoryNotFoundException with the directory still
                // there is a child that vanished under the walk, and is retried like the rest.
                if (!Directory.Exists(dir)) return;
                last = ex;
                if (Environment.TickCount64 - started >= (long)retryLimit.TotalMilliseconds) break;
                Thread.Sleep(backoffMs);
            }
        }

        throw new TestDataDirectoryLeftBehindException(
            $"The test data directory {dir} was left behind: after {retryLimit.TotalSeconds:0.#} s of retries "
          + $"it still holds {Describe(dir)}. Something in this process keeps a handle there — an engine, a "
          + "reader or a mapping that was not disposed, or a background task that never ends. (If this "
          + "replaced the test's own failure, which a throw from a finally block does, look for that first: "
          + "a test that stops half way can leave its handles open.) The last delete said: " + last.Message,
            last);
    }

    /// <summary>
    /// What is still in the directory: its files — what a handle holds is a file — up to eight of
    /// them, relative, and how many more; its directories when no file is left.
    /// </summary>
    private static string Describe(string dir)
    {
        const int shown = 8;
        try
        {
            var entries = Directory.GetFiles(dir, "*", SearchOption.AllDirectories);
            string what = "";
            if (entries.Length == 0)
            {
                entries = Directory.GetDirectories(dir, "*", SearchOption.AllDirectories);
                if (entries.Length == 0) return "nothing, yet cannot be removed itself";
                what = "no file, only the directories ";
            }
            Array.Sort(entries, StringComparer.OrdinalIgnoreCase);
            var names = entries.Take(shown).Select(e => Path.GetRelativePath(dir, e));
            return what + string.Join(", ", names) + (entries.Length > shown ? $" and {entries.Length - shown} more" : "");
        }
        catch (Exception ex)
        {
            return "entries that cannot be listed (" + ex.Message + ")";
        }
    }
}

/// <summary>
/// A test's data directory outlived the test: see <see cref="TestDataDirectory.Delete(string)"/>. A type of
/// its own because its name is all a class fixture's failed cleanup shows at <c>-v minimal</c>.
/// </summary>
internal sealed class TestDataDirectoryLeftBehindException(string message, Exception inner)
    : IOException(message, inner);
