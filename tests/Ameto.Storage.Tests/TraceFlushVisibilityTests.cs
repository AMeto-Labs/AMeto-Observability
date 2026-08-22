using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// Once the segment build moved off the engine lock, a flush's spans belong to neither
/// tier for its whole duration: they left the hot tier and their segment is not registered
/// yet. Every read path has to bridge that window — at load, flushes run back to back, so
/// a path that reads only the hot tier shows a rolling hole right behind the live edge.
/// These tests park a flush in exactly that window and require the answers to match what
/// the same queries return once it has published.
/// </summary>
public sealed class TraceFlushVisibilityTests : IDisposable
{
    private const int Traces = 300;                       // ×2 spans = 600, over MinSegmentSpans

    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset From = Base.AddMinutes(-5);
    private static readonly DateTimeOffset To   = Base.AddMinutes(+5);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-trcvis-" + Guid.NewGuid().ToString("N"));
    private readonly TraceStorageEngine _engine;

    public TraceFlushVisibilityTests()
    {
        Directory.CreateDirectory(_dir);
        _engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);

        long baseNano = Base.ToUnixTimeMilliseconds() * 1_000_000L;
        for (int t = 0; t < Traces; t++)
        {
            var trace = new TraceId(0x9E3779B97F4A7C15UL, (ulong)(t + 1));
            long start = baseNano + t * 1_000_000L;
            var root = new SpanIngestItem
            {
                TraceId = trace, SpanId = new SpanId((ulong)(t * 2 + 1)), ParentSpanId = default,
                StartTimeUnixNano = start, DurationNanos = 4_000_000,
                Name = "GET /api/pay", ServiceName = "gateway",
                Kind = SpanKind.Server, Status = SpanStatusCode.Unset, HttpStatusCode = 200,
                AttributesBytes = [],
            };
            var child = new SpanIngestItem
            {
                TraceId = trace, SpanId = new SpanId((ulong)(t * 2 + 2)), ParentSpanId = root.SpanId,
                StartTimeUnixNano = start + 100_000L, DurationNanos = 2_000_000,
                Name = "SELECT payments", ServiceName = "billing",
                Kind = SpanKind.Client, Status = SpanStatusCode.Unset, HttpStatusCode = 0,
                AttributesBytes = [],
            };
            _engine.WriteSpan(root);
            _engine.WriteSpan(child);
        }
    }

    public void Dispose()
    {
        try { _engine.Dispose(); } catch { }
        try { Directory.Delete(_dir, true); } catch { }
    }

    private sealed record Snapshot(int Spans, int Traces, int Edges, int VolumeTraces, int OneTraceSpans);

    private async Task<Snapshot> ReadEverythingAsync()
    {
        var stats  = await _engine.GetAggregateStatsAsync(From, To);
        var list   = await _engine.GetTraceListAsync(From, To, null, null, null, null, null, 1000);
        var graph  = await _engine.GetServiceGraphAsync(From, To);
        var volume = await _engine.GetTraceVolumeAsync(From, To, 20);
        var one    = _engine.GetTraceAsync(new TraceId(0x9E3779B97F4A7C15UL, 1)).ToBlockingEnumerable().ToList();

        return new Snapshot(
            Spans:         (int)stats.Sum(s => s.SpanCount),
            Traces:        list.Count,
            Edges:         graph.Edges.Length,
            VolumeTraces:  volume.TotalTraces,
            OneTraceSpans: one.Count);
    }

    [Fact]
    public async Task Every_read_path_sees_the_spans_of_a_flush_that_is_still_building()
    {
        using var parked   = new ManualResetEventSlim(false);
        using var released = new ManualResetEventSlim(false);
        _engine._beforeSegmentWrite = () => { parked.Set(); released.Wait(TimeSpan.FromSeconds(30)); };

        _engine.FlushIfDue();                                   // detaches the tier, parks in the seam
        Assert.True(parked.Wait(TimeSpan.FromSeconds(30)), "the flush never reached the segment build");

        // The spans are in neither tier right now. Every path must still report them.
        var midFlush = await ReadEverythingAsync();
        Assert.Equal(Traces * 2, midFlush.Spans);
        Assert.Equal(Traces,     midFlush.Traces);
        Assert.Equal(1,          midFlush.Edges);               // gateway → billing, one edge
        Assert.Equal(Traces,     midFlush.VolumeTraces);
        Assert.Equal(2,          midFlush.OneTraceSpans);

        released.Set();
        _engine.WaitForFlushForTest();

        // …and the same answers after publication — no double counting across the handover.
        Assert.Equal(midFlush, await ReadEverythingAsync());
    }
}
