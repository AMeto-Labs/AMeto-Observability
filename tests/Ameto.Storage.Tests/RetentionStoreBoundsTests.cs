using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// #130: the retention windows were bounded only from below, so "forever" was typed as 200 000 days
/// and overflowed the trace cutoff. The store now refuses a window outside
/// [<see cref="RetentionMath.MinDays"/>, <see cref="RetentionMath.MaxDays"/>] naming the field, and
/// clamps what an older database or a config file already holds.
/// </summary>
public sealed class RetentionStoreBoundsTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-retstore-" + Guid.NewGuid().ToString("N"));

    public RetentionStoreBoundsTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        // The pool keeps Ameto.db open after the store is gone, and on Windows the delete then fails and
        // the directory stays in TEMP for ever (see QuerySegmentFixtures.DeleteDataDirectory).
        SqliteConnection.ClearAllPools();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private RetentionStore Store(ServerOptions? options = null)
        => new(options ?? new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance);

    [Fact]
    public void Set_refuses_a_window_outside_the_bounds_and_names_the_field()
    {
        var store = Store();
        var before = store.Get();

        var tooLong = new RetentionDto { TracesDays = 200_000 };
        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => store.Set(tooLong));
        Assert.Equal("tracesDays", ex.ParamName);

        var zero = new RetentionDto { DebugDays = 0 };
        Assert.Equal("debugDays", Assert.Throws<ArgumentOutOfRangeException>(() => store.Set(zero)).ParamName);

        Assert.Same(before, store.Get());   // nothing of a refused request is kept
    }

    [Fact]
    public void Set_accepts_the_bounds_themselves()
    {
        var store = Store();
        store.Set(new RetentionDto { VerboseDays = RetentionMath.MinDays, TracesDays = RetentionMath.MaxDays });

        Assert.Equal(RetentionMath.MinDays, store.Get().VerboseDays);
        Assert.Equal(RetentionMath.MaxDays, store.Get().TracesDays);
        Assert.Equal(TimeSpan.FromDays(RetentionMath.MinDays), store.GetPolicy().GetTtl(LogLevel.Verbose));
    }

    [Fact]
    public void A_stored_window_of_centuries_is_clamped_on_load_and_healed_in_the_database()
    {
        // An install that typed 200 000 days before the bounds existed. The first start after the
        // change must not run the prune with that TTL even once.
        Store();   // creates the table and the defaults
        using (var conn = new SqliteConnection($"Data Source={Path.Combine(_dir, "Ameto.db")}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE retention SET days = 200000 WHERE level = 'traces'; UPDATE retention SET days = 0 WHERE level = 'debug';";
            cmd.ExecuteNonQuery();
        }

        var store = Store();
        Assert.Equal(RetentionMath.MaxDays, store.Get().TracesDays);
        Assert.Equal(RetentionMath.MinDays, store.Get().DebugDays);
        Assert.Equal(TimeSpan.FromDays(RetentionMath.MaxDays), RetentionMath.TtlOf(store.Get().TracesDays));

        // Healed where it was stored: the rows now hold the bounds, so a rollback to a build without the
        // clamp reads 36 500 (a cutoff in 1926, nothing deleted), not 200 000 (a cutoff in the future,
        // every trace file deleted every hour). An in-memory clamp alone would pass the asserts above.
        using (var conn = new SqliteConnection($"Data Source={Path.Combine(_dir, "Ameto.db")}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT days FROM retention WHERE level = 'traces'";
            Assert.Equal((long)RetentionMath.MaxDays, (long)cmd.ExecuteScalar()!);
            cmd.CommandText = "SELECT days FROM retention WHERE level = 'debug'";
            Assert.Equal((long)RetentionMath.MinDays, (long)cmd.ExecuteScalar()!);
        }
        Assert.Equal(RetentionMath.MaxDays, Store().Get().TracesDays);
    }

    [Fact]
    public void A_configured_window_of_centuries_is_clamped_on_first_run()
    {
        var options = new ServerOptions
        {
            DataDirectory = _dir,
            Retention     = new RetentionConfig { MetricsDays = 500_000, ErrorDays = -3 },
        };

        var store = Store(options);
        Assert.Equal(RetentionMath.MaxDays, store.Get().MetricsDays);
        Assert.Equal(RetentionMath.MinDays, store.Get().ErrorDays);
    }

    [Fact]
    public void ToPolicy_clamps_as_well()
    {
        // The policy is what EnforceRetentionAsync asks; a DTO built elsewhere must not reach it unbounded.
        var policy = new RetentionDto { FatalDays = 800_000, VerboseDays = 0 }.ToPolicy();
        Assert.Equal(TimeSpan.FromDays(RetentionMath.MaxDays), policy.GetTtl(LogLevel.Fatal));
        Assert.Equal(TimeSpan.FromDays(RetentionMath.MinDays), policy.GetTtl(LogLevel.Verbose));
    }
}
