using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE RETENTION STORE HOLDS NOTHING OPEN ONCE A CALL HAS RETURNED.
///
/// <para>It used to open <c>Ameto.db</c> through Microsoft.Data.Sqlite's connection pool, and the
/// pool parked the connection — the database, its <c>-wal</c>, its <c>-shm</c> and a mapping of the
/// last — past every <c>using</c> in the store and past the store itself. On Windows a file held
/// like that cannot be deleted, so the data directory of every test that built a store, which is
/// nearly every test of the engine, survived its own teardown: the delete threw, an empty
/// <c>catch</c> said nothing, and TEMP grew by about a thousand directories with every run of the
/// suite. A process-wide count of file handles would be the direct measurement and Windows offers
/// no cheap one, so these facts ask the question the way the suite does: can the directory be
/// removed, now, with one plain call?</para>
///
/// <para>NO POOL CLEAR AND NO RETRY IN HERE, on purpose. <c>TestDataDirectory.Delete</c> does both,
/// and either would turn these green again over a store that had gone back to pooling. On Linux an
/// open file does not block its unlink, so they pass there whatever the store does; the whole
/// suite runs on Windows, which is where the difference exists.</para>
/// </summary>
public sealed class RetentionStoreFileLifetimeTests
{
    private static string NewDirectory()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-retfile-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public void A_directory_can_be_removed_while_its_store_is_still_alive()
    {
        string dir = NewDirectory();
        var store  = new RetentionStore(new ServerOptions { DataDirectory = dir }, NullLogger<RetentionStore>.Instance);
        store.Set(new RetentionDto { DebugDays = 7 });   // the write road, not only the constructor's
        Assert.True(File.Exists(Path.Combine(dir, "Ameto.db")), "setup: the store must have created its database");

        Directory.Delete(dir, recursive: true);

        Assert.False(Directory.Exists(dir));
        Assert.Equal(7, store.Get().DebugDays);          // and the store goes on answering from memory
    }

    /// <summary>
    /// What every test of the engine does: a store built inline, an engine over it, the engine
    /// disposed, the directory deleted. The engine closes what it opened (its WAL, its segments,
    /// <c>counts.db</c>); the store was the one thing left.
    /// </summary>
    [Fact]
    public async Task A_directory_can_be_removed_as_soon_as_its_engine_is_disposed()
    {
        string dir  = NewDirectory();
        var options = new ServerOptions { DataDirectory = dir };
        var engine  = new StorageEngine(
            Options.Create(options),
            new RetentionStore(options, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        await engine.CatalogLoaded;      // the boot scan is a task the dispose does not wait for
        await engine.DisposeAsync();

        Directory.Delete(dir, recursive: true);

        Assert.False(Directory.Exists(dir));
    }

    /// <summary>
    /// Unpooled, a second store is a second real connection to the same file — as the server's
    /// other stores are — and must read what the first one wrote.
    /// </summary>
    [Fact]
    public void A_second_store_over_the_same_directory_reads_what_the_first_wrote()
    {
        string dir = NewDirectory();
        try
        {
            var options = new ServerOptions { DataDirectory = dir };
            var first   = new RetentionStore(options, NullLogger<RetentionStore>.Instance);
            first.Set(new RetentionDto { TracesDays = 21, MetricsDays = 45 });

            var second = new RetentionStore(options, NullLogger<RetentionStore>.Instance);
            Assert.Equal(21, second.Get().TracesDays);
            Assert.Equal(45, second.Get().MetricsDays);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
        Assert.False(Directory.Exists(dir));
    }
}
