using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// The index cache may only ever SKIP section reads — never change what a query
/// returns. These tests run the same queries against one executor with the cache and
/// one without, over the same segments, and require byte-identical result sets: on
/// cold cache, on hot cache, and across the trigram upgrade (an equality query seeds a
/// trigram-less entry; the contains query that follows must not be narrowed by it).
/// </summary>
public sealed class IndexCacheEquivalenceTests : IDisposable
{
    private const int Events = 400;

    private readonly string            _dir = Path.Combine(Path.GetTempPath(), "ameto-idxcache-" + Guid.NewGuid().ToString("N"));
    private readonly StorageEngine     _engine;
    private readonly QueryExecutor     _plain;
    private readonly QueryExecutor     _cached;
    private readonly SegmentIndexCache _cache = new(64 * 1024 * 1024);

    public IndexCacheEquivalenceTests()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions { DataDirectory = _dir };
        _engine = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _engine.IndexSinkFactory = static (estimatedEventCount, termsPerEvent) =>
            new SegmentIndexBuilder(estimatedEventCount, 5, termsPerEvent);

        _plain  = new QueryExecutor(_engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);
        _cached = new QueryExecutor(_engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance, _cache);

        long baseTicks = new DateTimeOffset(2026, 8, 3, 0, 0, 0, TimeSpan.Zero).UtcTicks;
        var  buf       = new ArrayBufferWriter<byte>(128);
        for (int i = 0; i < Events; i++)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(2);
            w.Write("k");        w.Write((long)i);
            w.Write("Customer"); w.Write("cust-" + i % 10);
            w.Flush();
            Assert.True(_engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerSecond,
                // 7 is coprime with the Customer cycle (10): every customer value occurs at
                // BOTH levels, so both level-pure segments pass the bloom gate for any
                // `Customer = ...` filter and every segment's memo learns postings, not only a
                // bloom verdict. (Since #80 a bloom-rejected group is cached too — its memo keeps
                // the verdict — so this is no longer what makes the no-new-misses assertion
                // below hold; it keeps that assertion about postings.)
                Level                    = i % 7 == 0 ? LogLevel.Error : LogLevel.Information,
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("widget {k} shipped to {Customer}"),
                ServiceNamePoolIndex     = _engine.TemplatePool.Intern("Svc.Ship"),
            }, buf.WrittenSpan.ToArray()));
        }
        _engine.FlushHotTierAsync().GetAwaiter().GetResult();
        Assert.NotEmpty(_engine.ListSegments());
    }

    public void Dispose()
    {
        try { _engine.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { }
        QuerySegmentFixtures.DeleteDataDirectory(_dir);
    }

    private static async Task<List<long>> RunAsync(QueryExecutor q, string? filter, HashSet<LogLevel>? levels = null)
    {
        var keys = new List<long>();
        await foreach (var ev in q.ExecuteAsync(new QueryRequest
        {
            Filter    = filter,
            Count     = 10_000,
            Direction = QueryDirection.Forward,
            Levels    = levels,
        }))
        {
            keys.Add((long)ev.Properties!["k"]!);
        }
        return keys;
    }

    [Fact]
    public async Task Repeated_queries_hit_the_cache_and_return_identical_results()
    {
        var expected = await RunAsync(_plain, "Customer = 'cust-3'");

        var cold = await RunAsync(_cached, "Customer = 'cust-3'");
        Assert.Equal(expected, cold);
        Assert.True(_cache.EntryCount > 0, "the miss should have populated the cache");

        long missesAfterCold = _cache.MissCount;
        var hot = await RunAsync(_cached, "Customer = 'cust-3'");
        Assert.Equal(expected, hot);
        Assert.True(_cache.HitCount > 0, "the second run should have hit the cache");
        Assert.Equal(missesAfterCold, _cache.MissCount); // and missed nothing new
    }

    [Fact]
    public async Task Trigram_query_after_equality_query_upgrades_and_stays_correct()
    {
        // Seeds trigram-less entries: no substring predicate in the filter.
        var eq = await RunAsync(_cached, "Customer = 'cust-3'");
        Assert.Equal(await RunAsync(_plain, "Customer = 'cust-3'"), eq);

        // The contains query needs trigram postings the cached entries lack — it must
        // rebuild (upgrade), not narrow through the trigram-less reader.
        var contains = await RunAsync(_cached, "contains(@mt, 'shipped')");
        Assert.Equal(await RunAsync(_plain, "contains(@mt, 'shipped')"), contains);
        Assert.Equal(Events, contains.Count); // every event's template contains it

        // And the upgraded full entry now serves both shapes.
        Assert.Equal(eq, await RunAsync(_cached, "Customer = 'cust-3'"));
    }

    /// <summary>
    /// Shedding the cache under RAM pressure is a memory decision and must never be a
    /// correctness one. Pressure can empty it between queries — or, as here, while an executor
    /// is live over the same segments — and everything after that must still return exactly what
    /// the uncached executor returns, on the cold query that follows and on the hot one after it.
    /// </summary>
    [Fact]
    public async Task Pressure_shedding_the_cache_does_not_change_any_result()
    {
        var expected = await RunAsync(_plain, "Customer = 'cust-3'");
        Assert.Equal(expected, await RunAsync(_cached, "Customer = 'cust-3'"));
        Assert.True(_cache.EntryCount > 0, "the query should have populated the cache");

        long shed = _cache.Shed();

        Assert.True(shed > 0, "a populated cache should have had bytes to give back");
        Assert.Equal(0, _cache.EntryCount);
        Assert.Equal(0L, _cache.TotalBytes);
        Assert.Equal(0L, _cache.NativeBytes);

        // Cold again — every section re-read and re-decoded — then hot again.
        Assert.Equal(expected, await RunAsync(_cached, "Customer = 'cust-3'"));
        Assert.Equal(expected, await RunAsync(_cached, "Customer = 'cust-3'"));

        // And a shape that needs the trigram sections the shed also dropped.
        Assert.Equal(await RunAsync(_plain,  "contains(@mt, 'shipped')"),
                     await RunAsync(_cached, "contains(@mt, 'shipped')"));
    }

    [Fact]
    public async Task Levels_pruning_through_the_cache_matches_the_uncached_result()
    {
        var expected = await RunAsync(_plain, filter: null, levels: [LogLevel.Error]);
        var cold     = await RunAsync(_cached, filter: null, levels: [LogLevel.Error]);
        var hot      = await RunAsync(_cached, filter: null, levels: [LogLevel.Error]);
        Assert.Equal(expected, cold);
        Assert.Equal(expected, hot);
        Assert.Equal((Events + 6) / 7, expected.Count); // i = 0, 7, ... — 58 of 400
    }
}
