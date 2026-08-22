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
/// <c>@t</c> comparisons are rewritten at compile time into chronological tick compares
/// (<see cref="TimeCompareNode"/>), and the AND-chain's bounds feed the executor's scan
/// window. Two invariants: the bounds must be exactly what the operators say (they prune
/// segments and blocks — a wrong bound is silent row loss, not slowness), and the
/// evaluator must order timestamps by TIME, not by the bytes of their ISO renderings —
/// under the old ordinal compare a literal ending in 'Z' against the event's '+00:00'
/// rendering rejected sub-second events of the same second.
/// </summary>
public sealed class TimeBoundsTests : IDisposable
{
    // ── Compile-time bounds ───────────────────────────────────────────────────

    private static readonly long T0 = new DateTimeOffset(2026, 8, 22, 10, 0, 0, TimeSpan.Zero).UtcTicks;

    [Fact]
    public void Operators_produce_the_exact_bounds()
    {
        Assert.Equal((T0, (long?)null),    Bounds("@t >= '2026-08-22T10:00:00Z'"));
        Assert.Equal((T0 + 1, (long?)null), Bounds("@t > '2026-08-22T10:00:00Z'"));
        Assert.Equal(((long?)null, T0),    Bounds("@t <= '2026-08-22T10:00:00Z'"));
        Assert.Equal(((long?)null, T0 - 1), Bounds("@t < '2026-08-22T10:00:00Z'"));
        Assert.Equal((T0, (long?)T0),      Bounds("@t = '2026-08-22T10:00:00Z'"));
    }

    [Fact]
    public void And_chain_tightens_and_or_not_bound_nothing()
    {
        long t1 = T0 + TimeSpan.TicksPerHour;
        Assert.Equal((T0, (long?)t1),
            Bounds("@t >= '2026-08-22T10:00:00Z' and @t <= '2026-08-22T11:00:00Z' and Customer = 'x'"));

        // A bound from one OR branch would exclude the other branch's matches.
        Assert.Equal(((long?)null, (long?)null),
            Bounds("@t >= '2026-08-22T10:00:00Z' or Customer = 'x'"));
        Assert.Equal(((long?)null, (long?)null),
            Bounds("not (@t >= '2026-08-22T10:00:00Z')"));
    }

    [Fact]
    public void Unparseable_literal_keeps_the_old_node_and_no_bounds()
    {
        var f = CompiledFilter.Compile("@t >= 'not a date'");
        Assert.Equal(((long?)null, (long?)null), (f.MinTimestampTicks, f.MaxTimestampTicks));
    }

    [Fact]
    public void Offset_and_dateonly_literals_normalise_to_utc()
    {
        // +05:00 → 05:00 UTC that day; a bare date is midnight UTC.
        Assert.Equal((new DateTimeOffset(2026, 8, 22, 5, 0, 0, TimeSpan.Zero).UtcTicks, (long?)null),
            Bounds("@t >= '2026-08-22T10:00:00+05:00'"));
        Assert.Equal((new DateTimeOffset(2026, 8, 22, 0, 0, 0, TimeSpan.Zero).UtcTicks, (long?)null),
            Bounds("@t >= '2026-08-22'"));
    }

    private static (long?, long?) Bounds(string expr)
    {
        var f = CompiledFilter.Compile(expr);
        return (f.MinTimestampTicks, f.MaxTimestampTicks);
    }

    // ── End-to-end over both tiers ────────────────────────────────────────────

    private const int Events = 120; // 60 cold (flushed) + 60 hot, half with a .5s fraction

    private readonly string        _dir = Path.Combine(Path.GetTempPath(), "ameto-tbounds-" + Guid.NewGuid().ToString("N"));
    private StorageEngine?         _engine;
    private QueryExecutor?         _query;
    private readonly List<(long Ticks, long K)> _all = [];

    public void Dispose()
    {
        try { _engine?.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { }
        try { Directory.Delete(_dir, true); } catch { }
    }

    private async Task BuildAsync()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions { DataDirectory = _dir };
        _engine  = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _engine.IndexSinkFactory = static (c, t) => new SegmentIndexBuilder(c, 5, t);
        _query   = new QueryExecutor(_engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);

        var buf = new ArrayBufferWriter<byte>(64);
        void Write(int i)
        {
            long ticks = T0 + i * TimeSpan.TicksPerSecond
                       + (i % 2 == 1 ? TimeSpan.TicksPerSecond / 2 : 0); // odd events at x.5s
            _all.Add((ticks, i));
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1);
            w.Write("k"); w.Write((long)i);
            w.Flush();
            Assert.True(_engine!.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = ticks,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {k}"),
                ServiceNamePoolIndex     = _engine.TemplatePool.Intern("Svc.A"),
            }, buf.WrittenSpan.ToArray()));
        }

        for (int i = 0; i < Events / 2; i++) Write(i);
        await _engine!.FlushHotTierAsync();
        Assert.NotEmpty(_engine.ListSegments());
        for (int i = Events / 2; i < Events; i++) Write(i);
    }

    private async Task<List<long>> RunAsync(string filter)
    {
        var keys = new List<long>();
        await foreach (var ev in _query!.ExecuteAsync(new QueryRequest
        {
            Filter    = filter,
            Count     = 10_000,
            Direction = QueryDirection.Forward,
        }))
        {
            keys.Add((long)ev.Properties!["k"]!);
        }
        return keys;
    }

    private List<long> Expected(Func<long, bool> ticksMatch) =>
        _all.Where(e => ticksMatch(e.Ticks)).OrderBy(e => e.Ticks).Select(e => e.K).ToList();

    [Fact]
    public async Task Time_filter_prunes_but_returns_exactly_the_chronological_matches()
    {
        await BuildAsync();

        // Boundary in the middle of the COLD tier, Z-suffixed, whole-second: the sub-second
        // events of later seconds must be included (the old ordinal compare dropped x.5s
        // events whose second equalled the literal's).
        string mid = new DateTimeOffset(T0 + 20 * TimeSpan.TicksPerSecond, TimeSpan.Zero)
            .UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'");
        long midTicks = T0 + 20 * TimeSpan.TicksPerSecond;

        Assert.Equal(Expected(t => t >= midTicks), await RunAsync($"@t >= '{mid}'"));
        Assert.Equal(Expected(t => t <  midTicks), await RunAsync($"@t < '{mid}'"));

        // A window straddling the flush boundary, via the AND-chain.
        long lo = T0 + 50 * TimeSpan.TicksPerSecond;
        long hi = T0 + 70 * TimeSpan.TicksPerSecond;
        // Kind=Utc "O" already carries the trailing Z.
        string loS = new DateTimeOffset(lo, TimeSpan.Zero).UtcDateTime.ToString("O");
        string hiS = new DateTimeOffset(hi, TimeSpan.Zero).UtcDateTime.ToString("O");
        Assert.Equal(
            Expected(t => t >= lo && t <= hi),
            await RunAsync($"@t >= '{loS}' and @t <= '{hiS}'"));

        // OR keeps per-event semantics with no pruning: both sides fully served.
        Assert.Equal(
            Expected(t => t < T0 + 5 * TimeSpan.TicksPerSecond || t >= T0 + 115 * TimeSpan.TicksPerSecond),
            await RunAsync(
                $"@t < '{new DateTimeOffset(T0 + 5 * TimeSpan.TicksPerSecond, TimeSpan.Zero).UtcDateTime:O}'" +
                $" or @t >= '{new DateTimeOffset(T0 + 115 * TimeSpan.TicksPerSecond, TimeSpan.Zero).UtcDateTime:O}'"));
    }
}
