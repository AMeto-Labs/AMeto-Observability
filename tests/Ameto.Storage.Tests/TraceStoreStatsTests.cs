using Ameto.Core;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// <see cref="TraceStorageEngine.GetStoreStats"/> — the Statistics page's "Store" column for traces —
/// over a store that has flushed: the cold segments' spans, file bytes, services and start bounds,
/// with the hot tier's count on top. The integration test covers the hot-only store; this one needs
/// a segment on disk, and a file the catalogue carries no length for, so the stat-and-remember path
/// is the one measured.
/// </summary>
public sealed class TraceStoreStatsTests : IDisposable
{
    private const long Ms = 1_000_000L;
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-tstats-" + Guid.NewGuid().ToString("N"));

    public TraceStoreStatsTests() => Directory.CreateDirectory(_dir);

    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private static SpanIngestItem Span(int i, string service, long startNano) => new()
    {
        TraceId = new TraceId(0x57A7, (ulong)(i / 5 + 1)), SpanId = new SpanId((ulong)(i + 1)),
        StartTimeUnixNano = startNano, DurationNanos = Ms, Name = "GET /orders", ServiceName = service, Kind = SpanKind.Server,
    };

    [Fact]
    public void A_flushed_store_reports_its_segments_bytes_services_and_bounds_with_the_hot_tier_on_top()
    {
        // The old cadence (TraceFlushCadenceTests): a tier of 500 spans is written on the next due check.
        using var e = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance,
            options: new TracesOptions { HotTierFlushAge = TimeSpan.Zero });

        var empty = e.GetStoreStats();
        Assert.Equal(0, empty.Segments);
        Assert.Equal(0, empty.Spans);
        Assert.Null(empty.OldestStartNano);

        long now    = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * Ms;
        long oldest = now - 3_600_000 * Ms;                                 // an hour of two services
        for (int i = 0; i < 600; i++)
            Assert.True(e.WriteSpan(Span(i, i % 2 == 0 ? "billing" : "checkout", oldest + i * Ms)));
        e.FlushIfDue();
        e.WaitForFlushForTest();
        var files = Directory.GetFiles(_dir, "*.trc");
        Assert.Single(files);

        var cold = e.GetStoreStats();
        Assert.Equal(1, cold.Segments);
        Assert.Equal(600, cold.Spans);
        Assert.Equal(0, cold.HotSpans);
        Assert.Equal(new FileInfo(files[0]).Length, cold.FileBytes);     // measured now: a flushed segment carries no length
        Assert.Equal(["billing", "checkout"], cold.Services.Order().ToArray());
        Assert.Equal(oldest, cold.OldestStartNano);
        Assert.Equal(oldest + 599 * Ms, cold.NewestStartNano);

        // Three more spans stay hot: counted in Spans and HotSpans, but their service has no cold
        // header to be listed in — the endpoint adds the window's services on top (see the mapper).
        for (int i = 0; i < 3; i++) Assert.True(e.WriteSpan(Span(5_000 + i, "gateway", now - i * Ms)));
        var mixed = e.GetStoreStats();
        Assert.Equal(1, mixed.Segments);
        Assert.Equal(603, mixed.Spans);
        Assert.Equal(3, mixed.HotSpans);
        Assert.Equal(cold.FileBytes, mixed.FileBytes);
        Assert.Equal(2, mixed.Services.Count);
        Assert.Equal(cold.NewestStartNano, mixed.NewestStartNano);       // the bounds are the cold tier's
    }
}
