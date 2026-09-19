using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE COLD TIER'S HALF OF THE TEARDOWN, which nothing covered.
///
/// <para>The flush half is sound and was verified as such: cancel, await both loops (the flush
/// loop's last act is an unconditional final flush), drain the threshold flushes, fence ingest
/// under the snapshot write lock. The cold list had no equivalent. <c>_coldLock.Dispose()</c>
/// stood at the end of the teardown while four callers take that lock, and two of them —
/// <c>QueryAsync</c> and <c>GetMetricNames</c> — are reached from Kestrel, which is STILL
/// SERVING: hosted services stop in reverse registration order and the HTTP pipeline outlives
/// <c>MetricStorageHostedService.StopAsync</c>.</para>
///
/// <para>A query holding the read lock at that moment got an <see cref="ObjectDisposedException"/>
/// out of the middle of its response. A query WAITING on it made
/// <see cref="ReaderWriterLockSlim.Dispose"/> throw <see cref="SynchronizationLockException"/>,
/// which escaped <c>DisposeAsync</c> — while the <c>finally</c> completed <c>_disposeCompleted</c>
/// regardless, so the host's other two disposers returned believing the teardown had finished.
/// And <c>PruneAsync</c>, which <c>RetentionService</c> holds as an <c>IRetentionTarget</c> and
/// can call at any time, was gated on nothing at all: it unlinked <c>.mts</c> files through a
/// disposed lock.</para>
///
/// <para>Judged by seams and not by timers: every fact here drives the engine to a defined point
/// and then asserts what the next call does.</para>
/// </summary>
public sealed class MetricShutdownSeamTests
{
    /// <summary>
    /// The enumerator is parked between the hot tier and the cold tier — it has yielded the hot
    /// series and has not yet taken the cold read lock — when the engine is disposed underneath
    /// it. Before the fence, the next MoveNext threw ObjectDisposedException at whoever was
    /// reading the response.
    /// </summary>
    [Fact]
    public async Task A_query_enumerator_survives_a_dispose_that_happens_mid_flight()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mshut-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

            // A cold segment on disk, so the enumerator has a cold half to reach at all.
            var seeder = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance);
            seeder.Ingest([Point("shutdown.metric", baseNano)]);
            await seeder.DisposeAsync();

            var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance);
            await engine.ColdLoadCompleted;
            engine.Ingest([Point("shutdown.metric", baseNano + 1_000_000L, 2.0)]);

            await using var e = engine.QueryAsync("shutdown.metric").GetAsyncEnumerator();

            // The hot tier is walked first, so one MoveNext leaves the enumerator exactly at the
            // seam: past the hot series, before the cold read lock.
            Assert.True(await e.MoveNextAsync());
            Assert.Equal(2.0, e.Current.Points[0].Value);

            await engine.DisposeAsync();

            // The cold half answers empty rather than throwing. The enumeration ENDS, which is
            // the contract: a response that stops early is a response, and an
            // ObjectDisposedException out of the middle of one is not.
            var afterDispose = await Record.ExceptionAsync(async () => { while (await e.MoveNextAsync()) { } });
            Assert.Null(afterDispose);
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>
    /// The read paths Kestrel can still reach after the teardown. None of them may throw, and
    /// none of them may claim data the engine no longer stands behind.
    /// </summary>
    [Fact]
    public async Task The_read_paths_answer_empty_after_a_dispose_instead_of_throwing()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mshutread-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
            var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance);
            engine.Ingest([Point("shutdown.metric", baseNano)]);
            await engine.DisposeAsync();

            // _meta is not part of the cold tier and is not torn down, so the catalog still
            // answers — the fence is about the segment LIST, not about forgetting the metric.
            Assert.Contains("shutdown.metric", engine.GetMetricNames());

            var series = new List<MetricSeries>();
            await foreach (var s in engine.QueryAsync("shutdown.metric")) series.Add(s);
            Assert.Empty(series);                 // the hot tier was drained by the final flush

            // Retention: gated, and it must not unlink the files the final flush just wrote.
            int filesBefore = Directory.GetFiles(dir, "*.mts").Length;
            Assert.True(filesBefore > 0, "setup: the final flush must have written a segment");
            Assert.Equal(0, await engine.PruneAsync(TimeSpan.Zero));
            Assert.Equal(filesBefore, Directory.GetFiles(dir, "*.mts").Length);

            // And the second and third disposers — the DI container and the hosted service —
            // return without a fault of their own.
            await engine.DisposeAsync();
            await engine.DisposeAsync();
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>
    /// A prune with a TTL that would expire everything, arriving after the teardown. Gated on
    /// <c>_disposed</c>, it is a no-op; ungated it deleted files through a disposed lock.
    /// </summary>
    [Fact]
    public async Task Retention_cannot_unlink_segments_after_the_engine_is_down()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-mshutprune-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
            var engine = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance);
            engine.Ingest([Point("shutdown.metric", baseNano)]);

            // While it is LIVE the same call does prune — otherwise this fact would pass on an
            // engine whose retention never worked at all.
            await engine.ScheduleThresholdFlushForTest();
            Assert.True(Directory.GetFiles(dir, "*.mts").Length > 0);
            Assert.Equal(1, await engine.PruneAsync(TimeSpan.Zero));
            Assert.Empty(Directory.GetFiles(dir, "*.mts"));

            engine.Ingest([Point("shutdown.metric", baseNano + 1_000_000L)]);
            await engine.DisposeAsync();          // the final flush writes another segment

            int after = Directory.GetFiles(dir, "*.mts").Length;
            Assert.True(after > 0, "setup: the final flush must have written a segment");
            Assert.Equal(0, await engine.PruneAsync(TimeSpan.Zero));
            Assert.Equal(after, Directory.GetFiles(dir, "*.mts").Length);
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    private static MetricIngestItem Point(string name, long nano, double value = 1.0) => new()
    {
        Name              = name,
        Kind              = MetricKind.Gauge,
        Labels            = new LabelSet(new Dictionary<string, string> { ["series"] = "s" }),
        TimestampUnixNano = nano,
        ScalarValue       = value,
    };
}
