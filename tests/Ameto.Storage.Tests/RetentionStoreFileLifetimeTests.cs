using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE RETENTION STORE HOLDS NOTHING OPEN ONCE A CALL HAS RETURNED, AND ASKS NO MORE OF THE SHARED
/// FILE THAN IT NEEDS.
///
/// <para>It used to open <c>Ameto.db</c> through Microsoft.Data.Sqlite's connection pool, and the
/// pool parked the connection — the database, its <c>-wal</c>, its <c>-shm</c> and a mapping of the
/// last — past every <c>using</c> in the store and past the store itself. On Windows a file held
/// like that cannot be deleted, so the data directory of every test that built a store and an
/// engine survived its own teardown: the delete threw, an empty <c>catch</c> said nothing, and one
/// run of the suite left 728 such directories in TEMP. A process-wide count of file handles would
/// be the direct measurement and Windows offers no cheap one, so these facts ask the question the
/// way the suite does: can the directory be removed, now, with a plain delete?</para>
///
/// <para>NO POOL CLEAR IN HERE, on purpose: <c>TestDataDirectory.Delete</c> clears the pools, and
/// that would turn these green again over a store that had gone back to pooling. The plain delete
/// is retried for two seconds and no longer — a scanner may have the file it just saw created open
/// for a moment — which hides nothing: the pool keeps a parked connection for minutes (381 s in
/// one measurement). On Linux an open file does not block its unlink, so these pass there whatever
/// the store does; the whole suite runs on Windows, which is where the difference exists.</para>
/// </summary>
public sealed class RetentionStoreFileLifetimeTests
{
    private static string NewDirectory()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-retfile-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    /// <summary><c>Directory.Delete</c> and nothing else, for two seconds at most; its last failure is the fact's.</summary>
    private static void DeletePlainly(string dir)
    {
        long until = Environment.TickCount64 + 2_000;
        while (true)
        {
            try
            {
                Directory.Delete(dir, recursive: true);
                return;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException && Environment.TickCount64 < until)
            {
                Thread.Sleep(20);
            }
        }
    }

    [Fact]
    public void A_directory_can_be_removed_while_its_store_is_still_alive()
    {
        string dir = NewDirectory();
        var store  = new RetentionStore(new ServerOptions { DataDirectory = dir }, NullLogger<RetentionStore>.Instance);
        store.Set(new RetentionDto { DebugDays = 7 });   // the write road, not only the constructor's
        Assert.True(File.Exists(Path.Combine(dir, "Ameto.db")), "setup: the store must have created its database");

        DeletePlainly(dir);

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

        DeletePlainly(dir);

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
            DeletePlainly(dir);
        }
        Assert.False(Directory.Exists(dir));
    }

    /// <summary>
    /// A start over a database that already holds a policy — every start but the first — only
    /// READS it. <c>Ameto.db</c> is the auth and alert stores' file too, and one of them can be in
    /// the middle of a write when this store is built: held here by a connection that has begun a
    /// write transaction and not ended it. A constructor that asked for the write lock as well
    /// would wait that writer out for the library's thirty seconds and then fail the start with
    /// "database is locked".
    /// </summary>
    [Fact]
    public void A_start_over_a_stored_policy_reads_it_while_another_store_holds_the_write_lock()
    {
        string dir = NewDirectory();
        try
        {
            var options = new ServerOptions { DataDirectory = dir };
            new RetentionStore(options, NullLogger<RetentionStore>.Instance).Set(new RetentionDto { ErrorDays = 11 });

            string unpooled = new SqliteConnectionStringBuilder
            {
                DataSource = Path.Combine(dir, "Ameto.db"),
                Pooling    = false,
            }.ToString();
            using (var writer = new SqliteConnection(unpooled))
            {
                writer.Open();
                using var held = writer.BeginTransaction();   // BEGIN IMMEDIATE: the write lock, taken and kept
                using (var cmd = writer.CreateCommand())
                {
                    cmd.Transaction = held;
                    cmd.CommandText = "CREATE TABLE IF NOT EXISTS other_store (a INTEGER); INSERT INTO other_store VALUES (1);";
                    cmd.ExecuteNonQuery();
                }

                var store = new RetentionStore(options, NullLogger<RetentionStore>.Instance);

                Assert.Equal(11, store.Get().ErrorDays);
                held.Rollback();
            }
        }
        finally
        {
            DeletePlainly(dir);
        }
        Assert.False(Directory.Exists(dir));
    }
}
