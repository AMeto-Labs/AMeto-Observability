using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// The hot tier must be an entrant of the same (ts, id) k-way merge as the cold
/// segments. It used to be emitted wholesale BEFORE the cold stream, which broke the
/// global order whenever the tiers interleave in time (late-arriving events with @t
/// inside an already-flushed window) — and, through the keyset cursor, made the
/// not-yet-served cold events on the wrong side of the boundary permanently
/// unreachable under pagination.
/// </summary>
public sealed class TierMergeOrderTests : IAsyncLifetime
{
    private const int PerTier = 20;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-tiermerge-" + Guid.NewGuid().ToString("N"));
    private StorageEngine _engine = null!;
    private QueryExecutor _query  = null!;
    private long          _base;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions { DataDirectory = _dir };
        _engine = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _query = new QueryExecutor(_engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);
        _base  = new DateTimeOffset(2026, 8, 1, 0, 0, 0, TimeSpan.Zero).UtcTicks;

        // Cold: one event per minute at :00, flushed to a segment.
        WriteBatch(secondOffset: 0, tag: "cold");
        await _engine.FlushHotTierAsync();
        Assert.NotEmpty(_engine.ListSegments());

        // Hot: late arrivals at :30 of the SAME minutes — strictly inside the flushed
        // segment's [MinTs, MaxTs] window, never flushed.
        WriteBatch(secondOffset: 30, tag: "hot");
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private void WriteBatch(int secondOffset, string tag)
    {
        var buf = new ArrayBufferWriter<byte>(128);
        for (int i = 0; i < PerTier; i++)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1);
            w.Write("k"); w.Write(tag + "-" + i);
            w.Flush();

            Assert.True(_engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = _base + i * TimeSpan.TicksPerMinute + secondOffset * TimeSpan.TicksPerSecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {k}"),
                ServiceNamePoolIndex     = _engine.TemplatePool.Intern("Svc.A"),
            }, buf.WrittenSpan.ToArray()));
        }
    }

    private static string KeyOf(LogEvent ev) => ev.Properties?["k"] as string ?? "<none>";

    private async Task<List<LogEvent>> PageAsync(bool forward, int count, LogEvent? after)
    {
        var res = new List<LogEvent>(count);
        await foreach (var ev in _query.ExecuteAsync(new QueryRequest
        {
            Count               = count,
            Direction           = forward ? QueryDirection.Forward : QueryDirection.Backward,
            AfterEventId        = after?.Id,
            AfterTimestampTicks = after?.Timestamp.UtcTicks,
        }))
        {
            res.Add(ev);
            if (res.Count >= count) break;
        }
        return res;
    }

    private static void AssertStrictOrder(IReadOnlyList<LogEvent> events, bool forward)
    {
        for (int i = 1; i < events.Count; i++)
        {
            var (pTs, pId) = (events[i - 1].Timestamp.UtcTicks, events[i - 1].Id.RawValue);
            var (cTs, cId) = (events[i].Timestamp.UtcTicks, events[i].Id.RawValue);
            bool ok = forward
                ? cTs > pTs || (cTs == pTs && cId > pId)
                : cTs < pTs || (cTs == pTs && cId < pId);
            Assert.True(ok, $"order broken at {i}: {KeyOf(events[i - 1])} then {KeyOf(events[i])} ({(forward ? "forward" : "backward")})");
        }
    }

    [Fact]
    public async Task Forward_interleaves_tiers_in_global_order()
    {
        var all = await PageAsync(forward: true, count: PerTier * 2, after: null);

        Assert.Equal(PerTier * 2, all.Count);
        AssertStrictOrder(all, forward: true);
        // cold-0 (:00), hot-0 (:30), cold-1, hot-1, ... — the exact interleaving.
        for (int i = 0; i < all.Count; i++)
            Assert.Equal((i % 2 == 0 ? "cold-" : "hot-") + i / 2, KeyOf(all[i]));
    }

    [Fact]
    public async Task Backward_interleaves_tiers_in_global_order()
    {
        var all = await PageAsync(forward: false, count: PerTier * 2, after: null);

        Assert.Equal(PerTier * 2, all.Count);
        AssertStrictOrder(all, forward: false);
        Assert.Equal("hot-" + (PerTier - 1),  KeyOf(all[0]));
        Assert.Equal("cold-" + (PerTier - 1), KeyOf(all[1]));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Keyset_pagination_reaches_every_event_exactly_once(bool forward)
    {
        // Page size 7 deliberately puts page boundaries on both tiers. Before the merge
        // fix, page 1 was served entirely from the hot tier going forward, and the next
        // cursor then excluded every cold event older than it — forever.
        var seen = new List<LogEvent>();
        LogEvent? cursor = null;
        for (int guard = 0; guard < 20; guard++)
        {
            var page = await PageAsync(forward, count: 7, after: cursor);
            if (page.Count == 0) break;
            seen.AddRange(page);
            cursor = page[^1];
        }

        Assert.Equal(PerTier * 2, seen.Count);
        AssertStrictOrder(seen, forward);
        Assert.Equal(PerTier * 2, seen.Select(KeyOf).Distinct().Count());
    }
}
