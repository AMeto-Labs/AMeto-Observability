using Xunit;

namespace Ameto.Query.Tests;

/// <summary>
/// THE SUITE MUST NOT LEAVE ITS DATA DIRECTORIES BEHIND.
///
/// <para>Every query test builds a segment fixture under TEMP and deletes it in teardown. The
/// delete used to fail on Windows: <c>RetentionStore</c> opens <c>Ameto.db</c> through
/// Microsoft.Data.Sqlite, whose connection pool keeps the handle parked after the store is gone,
/// and the <c>catch { }</c> that every teardown wrapped the delete in swallowed the failure in
/// silence. Nothing was red and the directory stayed — one per test class, per run, holding a
/// database and forty segments.</para>
///
/// <para>This asserts the outcome rather than the mechanism, because the mechanism is what will
/// change: build a real fixture, dispose the engine, hand the directory to
/// <see cref="QuerySegmentFixtures.DeleteDataDirectory"/>, and require that it is gone. Take the
/// pool clear out of that helper and this goes red on Windows — which is the platform CI runs.</para>
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
