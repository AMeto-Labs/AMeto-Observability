using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// The <c>levels</c> parameter prunes cold groups through the inverted index (union of
/// the allowed levels' posting lists) and the bloom filter. The invariant these tests
/// pin: pruning may only ever SKIP work — the result set must be byte-identical to the
/// pre-pruning full scan, for indexed segments, for index-less segments (WAL-recovery
/// style, where the index answers "no information"), and across both tiers.
/// </summary>
public sealed class LevelPruneTests : IDisposable
{
    private const int ColdEvents = 30;
    private const int HotEvents  = 6;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-lvlprune-" + Guid.NewGuid().ToString("N"));
    private readonly List<StorageEngine> _engines = [];

    public void Dispose()
    {
        foreach (var e in _engines)
            try { e.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { }
        try { Directory.Delete(_dir, true); } catch { }
    }

    private static LogLevel LevelOf(int i) => (i % 3) switch
    {
        0 => LogLevel.Information,
        1 => LogLevel.Warning,
        _ => LogLevel.Error,
    };

    private async Task<(StorageEngine Engine, QueryExecutor Query, long BaseTicks)> BuildAsync(bool withIndex)
    {
        string dir = Path.Combine(_dir, withIndex ? "idx" : "noidx");
        Directory.CreateDirectory(dir);
        var opts   = new ServerOptions { DataDirectory = dir };
        var engine = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _engines.Add(engine);
        if (withIndex)
            engine.IndexSinkFactory = static (estimatedEventCount, termsPerEvent) =>
                new SegmentIndexBuilder(estimatedEventCount, 5, termsPerEvent);

        var query = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);
        long baseTicks = new DateTimeOffset(2026, 8, 2, 0, 0, 0, TimeSpan.Zero).UtcTicks;

        var buf = new ArrayBufferWriter<byte>(128);
        void Write(StorageEngine e, int i)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1);
            w.Write("k"); w.Write((long)i);
            w.Flush();
            Assert.True(e.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerSecond,
                Level                    = LevelOf(i),
                MessageTemplatePoolIndex = e.TemplatePool.Intern("evt {k}"),
                ServiceNamePoolIndex     = e.TemplatePool.Intern("Svc.A"),
            }, buf.WrittenSpan.ToArray()));
        }

        for (int i = 0; i < ColdEvents; i++) Write(engine, i);
        await engine.FlushHotTierAsync();
        Assert.NotEmpty(engine.ListSegments());

        for (int i = ColdEvents; i < ColdEvents + HotEvents; i++) Write(engine, i);

        return (engine, query, baseTicks);
    }

    private static async Task<List<long>> RunAsync(QueryExecutor query, HashSet<LogLevel>? levels, bool forward = true)
    {
        var keys = new List<long>();
        await foreach (var ev in query.ExecuteAsync(new QueryRequest
        {
            Count     = 1000,
            Direction = forward ? QueryDirection.Forward : QueryDirection.Backward,
            Levels    = levels,
        }))
        {
            keys.Add((long)ev.Properties!["k"]!);
        }
        return keys;
    }

    private static List<long> Expected(Func<int, bool> match, bool forward)
    {
        var keys = Enumerable.Range(0, ColdEvents + HotEvents).Where(match).Select(i => (long)i).ToList();
        if (!forward) keys.Reverse();
        return keys;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Single_level_returns_exactly_that_levels_events(bool withIndex)
    {
        var (_, query, _) = await BuildAsync(withIndex);
        var got = await RunAsync(query, [LogLevel.Error]);
        Assert.Equal(Expected(static i => LevelOf(i) == LogLevel.Error, forward: true), got);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Level_set_returns_the_union_in_order(bool withIndex)
    {
        var (_, query, _) = await BuildAsync(withIndex);
        var got = await RunAsync(query, [LogLevel.Error, LogLevel.Warning], forward: false);
        Assert.Equal(
            Expected(static i => LevelOf(i) is LogLevel.Error or LogLevel.Warning, forward: false),
            got);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Null_levels_returns_everything(bool withIndex)
    {
        var (_, query, _) = await BuildAsync(withIndex);
        var got = await RunAsync(query, levels: null);
        Assert.Equal(Expected(static _ => true, forward: true), got);
    }
}
