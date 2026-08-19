namespace Ameto.Storage.Tests;

/// <summary>
/// Several tests here hold a background thread inside an engine — parked on
/// <c>seam.Task.GetAwaiter().GetResult()</c> in a test hook — so that the rest of the test can
/// observe a window that is otherwise instantaneous. The thread is released by a
/// <c>SetResult()</c> further down the test.
///
/// <para>Further down is the problem. An assertion above that line ends the test by throwing, the
/// <c>SetResult()</c> never runs, and the parked thread stays parked for the life of the process:
/// no <c>DisposeAsync</c>, an engine still holding its write-ahead log mapped, and a fixture whose
/// <c>Directory.Delete</c> then fails and swallows saying so. One such run of the metric WAL suite
/// leaves 32 MB behind, because the log grows by doubling. The failure mode is therefore worst
/// exactly when it is least welcome: a test that leaks ONLY when it fails accumulates litter on
/// the days someone is debugging, and enough of it fills the disk, after which runs stop failing
/// on the code and start failing because there is nowhere to write.</para>
///
/// <para>So the release is a scope rather than a statement. <c>using var _ =
/// Seam.ReleasedOnExit(held);</c> at the top of a test releases the seam however the test leaves —
/// returning, throwing, or failing an assertion — and the ordinary mid-test <c>SetResult()</c>
/// still reads as it did, since <c>TrySetResult</c> on an already-completed source does
/// nothing.</para>
/// </summary>
internal static class Seam
{
    /// <summary>Releases <paramref name="seam"/> when the enclosing scope exits, however it exits.</summary>
    public static Scope ReleasedOnExit(TaskCompletionSource seam) => new(seam);

    /// <summary>A struct, so guarding a seam allocates nothing.</summary>
    internal readonly struct Scope(TaskCompletionSource seam) : IDisposable
    {
        public void Dispose() => seam.TrySetResult();
    }
}
