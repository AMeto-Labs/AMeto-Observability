using Xunit;

namespace Ameto.Query.Tests;

/// <summary>
/// THE SUITE MUST NOT LEAVE ITS DATA DIRECTORIES BEHIND.
///
/// <para>Every query test builds a segment fixture under TEMP and deletes it in teardown. The
/// delete used to fail on Windows: <c>RetentionStore</c> opened <c>Ameto.db</c> through
/// Microsoft.Data.Sqlite, whose connection pool kept the handle parked after the store was gone,
/// and the <c>catch { }</c> that every teardown wrapped the delete in swallowed the failure in
/// silence. Nothing was red and the directory stayed — one per test class, per run, holding a
/// database and forty segments. The store no longer pools, and the teardown's delete
/// (<see cref="QuerySegmentFixtures.DeleteDataDirectory"/>, which is
/// <c>TestDataDirectory.Delete</c>, the helper the test projects share) now fails its test
/// instead of returning over a directory that is still there.</para>
///
/// <para>This asserts the outcome rather than the mechanism, because the mechanism is what
/// changes: build a real fixture, dispose the engine, hand the directory to the helper, and
/// require that it is gone. The helper throws by itself now when the directory stays, so the
/// call is the fact and the assertion after it only states the outcome.
/// <c>RetentionStoreFileLifetimeTests</c> (Ameto.Storage.Tests) holds the store itself to it,
/// with a plain delete that no pool clear helps.</para>
/// </summary>
public sealed class FixtureCleanupTests
{
    [Fact]
    public async Task A_fixture_directory_is_gone_once_its_engine_is_disposed()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-cleanup-" + Guid.NewGuid().ToString("N"));

        var (engine, _) = await QuerySegmentFixtures.ManySegmentsAsync(dir);
        await engine.DisposeAsync();

        QuerySegmentFixtures.DeleteDataDirectory(dir);

        Assert.False(Directory.Exists(dir),
            $"{dir} survived teardown — something still holds a handle inside it, and every run of "
          + "this suite leaves one of these behind");
    }
}
