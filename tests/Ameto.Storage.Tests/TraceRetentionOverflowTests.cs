using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// #130: the trace prune computed its cutoff as <c>UtcNow.Subtract(ttl).ToUnixTimeMilliseconds() * 1e6</c>.
/// A TTL of some centuries (200 000 days, typed for "forever") wrapped the product into a cutoff in
/// the FUTURE and the prune deleted every trace file, every hour; a TTL past year 1 (800 000 days)
/// threw out of the prune instead. The cutoff now saturates: a TTL reaching past the epoch keeps everything.
/// </summary>
public sealed class TraceRetentionOverflowTests : IDisposable
{
    private const long Ms = 1_000_000L;

    private readonly string _root = Path.Combine(Path.GetTempPath(), "ameto-ret130-" + Guid.NewGuid().ToString("N"));

    public TraceRetentionOverflowTests() => Directory.CreateDirectory(_root);

    public void Dispose() { try { Directory.Delete(_root, true); } catch { } }

    private static TraceId Id(int i) => new(
        unchecked((ulong)(i * 6364136223846793005L + 1442695040888963407L)),
        unchecked((ulong)(i * 2862933555777941757L + 3037000493L)));

    private static void Write(TraceStorageEngine e, TraceId trace, ulong spanId, long startNano) =>
        e.WriteSpan(new SpanIngestItem
        {
            TraceId = trace, SpanId = new SpanId(spanId), ParentSpanId = default,
            StartTimeUnixNano = startNano, DurationNanos = 2 * Ms,
            Name = "GET /orders", ServiceName = "billing",
            Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
        });

    /// <summary>One cold segment of spans two days old, dated from now so a sane cutoff reaches it.</summary>
    private TraceStorageEngine EngineWithTwoDayOldSegment(string name)
    {
        string dir = Path.Combine(_root, name);
        Directory.CreateDirectory(dir);
        var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        long nowNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * Ms;
        for (int t = 0; t < 50; t++)
            Write(e, Id(t), (ulong)(t + 1), nowNano - 48L * 3600 * 1_000_000_000L + t * Ms);
        e.FlushHotTier();
        Assert.Equal(1, e.ColdSegmentCountForTest);
        return e;
    }

    [Theory]
    [InlineData(200_000)]
    [InlineData(800_000)]
    public async Task A_retention_of_centuries_deletes_nothing(int days)
    {
        using var e = EngineWithTwoDayOldSegment("centuries-" + days);

        Assert.Equal(0, await e.PruneAsync(TimeSpan.FromDays(days)));
        Assert.Equal(1, e.ColdSegmentCountForTest);
    }

    [Fact]
    public async Task The_longest_possible_TTL_deletes_nothing()
    {
        using var e = EngineWithTwoDayOldSegment("max");

        Assert.Equal(0, await e.PruneAsync(TimeSpan.MaxValue));
        Assert.Equal(1, e.ColdSegmentCountForTest);
    }

    [Fact]
    public async Task A_sane_TTL_still_prunes()
    {
        // The control: the saturation did not turn the prune off.
        using var e = EngineWithTwoDayOldSegment("sane");

        Assert.Equal(1, await e.PruneAsync(TimeSpan.FromDays(1)));
        Assert.Equal(0, e.ColdSegmentCountForTest);
    }
}
