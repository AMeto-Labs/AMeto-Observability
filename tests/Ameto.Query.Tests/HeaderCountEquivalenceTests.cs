using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query.Filtering;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// The alert evaluator counts a header-only rule from event headers instead of scanning.
/// That is only sound if the two agree exactly, so this runs both over the same data:
/// the header aggregator the fast path uses, and the full scan it replaces. Data spans
/// both tiers, several services (one of them absent from some levels) and every level.
/// </summary>
public sealed class HeaderCountEquivalenceTests : IDisposable
{
    private static readonly DateTimeOffset Base = new(2026, 8, 23, 9, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset From = Base.AddMinutes(-1);
    private static readonly DateTimeOffset To   = Base.AddHours(2);

    private static readonly string[]   Services = ["checkout", "billing", "gateway"];
    private static readonly LogLevel[] Levels   =
        [LogLevel.Verbose, LogLevel.Debug, LogLevel.Information, LogLevel.Warning, LogLevel.Error, LogLevel.Fatal];

    private readonly string        _dir = Path.Combine(Path.GetTempPath(), "ameto-hdrcount-" + Guid.NewGuid().ToString("N"));
    private readonly StorageEngine _engine;
    private readonly QueryExecutor _query;

    public HeaderCountEquivalenceTests()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions { DataDirectory = _dir };
        _engine  = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _engine.IndexSinkFactory = static (c, t) => new SegmentIndexBuilder(c, 5, t);
        _query   = new QueryExecutor(_engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);

        var buf = new ArrayBufferWriter<byte>(128);
        void Write(int i)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1);
            w.Write("k"); w.Write((long)i);
            w.Flush();

            // Strides chosen so level and service do NOT move together — otherwise whole
            // (level, service) pairs vanish from the corpus and the comparison passes on
            // empty sets. 'gateway' is the deliberate exception: it only ever logs
            // Information, so at least one pair IS empty.
            string service = Services[i % 3];
            var    level   = service == "gateway" ? LogLevel.Information : Levels[(i / 3) % Levels.Length];

            Assert.True(_engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = Base.UtcTicks + i * TimeSpan.TicksPerSecond,
                Level                    = level,
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {k}"),
                ServiceNamePoolIndex     = _engine.TemplatePool.Intern(service),
            }, buf.WrittenSpan.ToArray()));
        }

        for (int i = 0; i < 200; i++) Write(i);
        _engine.FlushHotTierAsync().GetAwaiter().GetResult();   // cold tier
        for (int i = 200; i < 320; i++) Write(i);               // …and hot tier
    }

    public void Dispose()
    {
        try { _engine.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { }
        try { Directory.Delete(_dir, true); } catch { }
    }

    /// <summary>What the alert evaluator's fast path computes.</summary>
    private async Task<double> HeaderCountAsync(HashSet<LogLevel>? levels, string? service)
    {
        int bucketSeconds = (int)Math.Max(1, Math.Ceiling((To - From).TotalSeconds));
        long minBucket    = From.ToUnixTimeSeconds() / bucketSeconds;

        var counts = await _engine.AggregateLogVolumeAsync(
            From, To, minBucket, bucketSeconds, nBuckets: 1, serviceFilter: service);

        if (levels is null) return counts.Total;
        if (levels.Count == 0) return 0;

        double total = 0;
        foreach (var s in counts.Levels)
            if (LogLevelExtensions.TryParse(s.Name.AsSpan(), out var lvl) && levels.Contains(lvl))
                total += s.Count;
        return total;
    }

    /// <summary>What it replaces.</summary>
    private async Task<double> ScanCountAsync(string? filter)
    {
        int count = 0;
        await foreach (var _ in _query.ExecuteAsync(new QueryRequest
        {
            Filter    = filter,
            FromUtc   = From,
            ToUtc     = To,
            Count     = 1_000_000,
            Direction = QueryDirection.Backward,
        }))
        {
            count++;
        }
        return count;
    }

    [Theory]
    [InlineData(null)]
    [InlineData("@l = 'Error'")]
    [InlineData("Fatal")]
    [InlineData("@l in ['Error', 'Fatal']")]
    [InlineData("@l = 'Error' or @l = 'Warning'")]
    [InlineData("service.name = 'checkout'")]
    [InlineData("@l = 'Error' and service.name = 'checkout'")]
    [InlineData("service.name = 'gateway' and @l = 'Error'")]     // a pair with no events
    [InlineData("@l = 'Information' and service.name = 'gateway'")]
    [InlineData("@l in ['Error', 'Fatal'] and @l = 'Fatal'")]
    public async Task The_header_count_equals_the_scan_count(string? filter)
    {
        Assert.True(CompiledFilter.Compile(filter).TryGetHeaderOnlyShape(out var levels, out var service),
            "this filter is supposed to be answerable from headers");

        double expected = await ScanCountAsync(filter);
        double actual   = await HeaderCountAsync(levels, service);

        Assert.Equal(expected, actual);
        Assert.True(expected > 0 || filter!.Contains("gateway"), "the corpus should exercise a non-empty case");
    }

    [Theory]
    // The shapes the ALERT path actually takes to the aggregator: no level constraint, so
    // Total (already service-filtered) is the whole answer. A level-constrained rule goes
    // back to the scan, where the index and level-pure segments make it cheaper.
    [InlineData(null)]
    [InlineData("service.name = 'checkout'")]
    [InlineData("service.name = 'gateway'")]
    public async Task The_alert_fast_path_total_equals_the_scan_count(string? filter)
    {
        var shape = CompiledFilter.Compile(filter);
        Assert.True(shape.TryGetHeaderOnlyShape(out var levels, out var service));
        Assert.Null(levels);                       // this is what routes it to the aggregator

        int bucketSeconds = (int)Math.Max(1, Math.Ceiling((To - From).TotalSeconds));
        var counts = await _engine.AggregateLogVolumeAsync(
            From, To, From.ToUnixTimeSeconds() / bucketSeconds, bucketSeconds, nBuckets: 1, serviceFilter: service);

        Assert.Equal(await ScanCountAsync(filter), counts.Total);
        Assert.True(counts.Total > 0);
    }
}
