using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query;
using Ameto.Storage;
using Xunit;

namespace Ameto.Query.Tests;

/// <summary>
/// On-disk fixtures for the query tests, shared with <c>Ameto.Perf</c> by link rather than
/// copied (see that project's .csproj).
///
/// <para>The split is deliberate: the assertions about WHAT a query returns live in this
/// suite, where the whole functional suite runs, and the allocation-ratio probes over the
/// same segments live in Ameto.Perf, where a threshold on a process-wide counter belongs.
/// Both need identical segments to be talking about the same thing, so the setup is one
/// file with one copy of the numbers.</para>
/// </summary>
internal static class QuerySegmentFixtures
{
    // ── Grouped single segment ────────────────────────────────────────────────

    /// <summary>Events in the multi-group fixture.</summary>
    public const int  GroupedEvents = 6_000;

    /// <summary>Index-group payload budget — several groups out of one small segment.</summary>
    public const long GroupBudget   = 512 * 1024;

    /// <summary>
    /// One flushed segment spanning several index groups, written through the SAME wiring
    /// production uses: a fresh <see cref="SegmentIndexBuilder"/> per group, posting offsets
    /// based at the group's first file ordinal.
    ///
    /// <para>Property shapes matter to what can be asserted: <c>OrderId</c> is unique per
    /// event (so a value lives in exactly one group), <c>Customer</c> repeats every 40 (so a
    /// predicate spans every group), and the padding is what pushes the payload past the
    /// budget often enough to produce groups at all.</para>
    /// </summary>
    public static async Task<(StorageEngine Engine, QueryExecutor Query, string SegPath, long BaseTicks)>
        GroupedSegmentAsync(string dir)
    {
        Directory.CreateDirectory(dir);
        var engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = dir }),
            new RetentionStore(new ServerOptions { DataDirectory = dir }, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        engine._groupPayloadBudgetBytes = GroupBudget;
        engine.IndexSinkFactory = static (estimatedEventCount, termsPerEvent) =>
            new SegmentIndexBuilder(estimatedEventCount, 5, termsPerEvent);

        var query = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);

        long baseTicks = new DateTimeOffset(2026, 7, 30, 0, 0, 0, TimeSpan.Zero).UtcTicks;
        int  tmplIdx   = engine.TemplatePool.Intern("order {OrderId} processed for {Customer}");
        int  svcIdx    = engine.TemplatePool.Intern("Svc.Orders");

        var buf = new ArrayBufferWriter<byte>(512);
        for (int i = 0; i < GroupedEvents; i++)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(3);
            w.Write("OrderId");  w.Write("order-" + i);                 // unique per event
            w.Write("Customer"); w.Write("cust-" + (i % 40));           // low cardinality
            w.Write("pad");      w.Write(new string((char)('a' + i % 26), 220));
            w.Flush();

            Assert.True(engine.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerSecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
            }, buf.WrittenSpan.ToArray()));
        }
        await engine.FlushHotTierAsync();

        var segs = engine.ListSegments();
        Assert.Single(segs);
        return (engine, query, segs[0].FilePath, baseTicks);
    }

    /// <summary>Groups the flushed fixture segment actually ended up with.</summary>
    public static int GroupCountOf(string segPath)
    {
        using var reader = SegmentReader.Open(segPath);
        return reader.Groups.Length;
    }

    // ── Many small segments ───────────────────────────────────────────────────

    public const int ManySegments     = 40;
    public const int EventsPerSegment = 25;

    /// <summary>
    /// Non-overlapping segments, oldest first: segment k covers minute k. The point of the
    /// shape is that a page of 5 can be served from one of them, so opening the other 39 is
    /// pure waste and shows up as such.
    /// </summary>
    public static async Task<(StorageEngine Engine, QueryExecutor Query)> ManySegmentsAsync(string dir)
    {
        Directory.CreateDirectory(dir);
        var engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = dir }),
            new RetentionStore(new ServerOptions { DataDirectory = dir }, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        var query = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);

        long baseTicks = new DateTimeOffset(2026, 7, 30, 0, 0, 0, TimeSpan.Zero).UtcTicks;
        var  buf       = new ArrayBufferWriter<byte>(128);
        for (int s = 0; s < ManySegments; s++)
        {
            for (int i = 0; i < EventsPerSegment; i++)
            {
                buf.ResetWrittenCount();
                var w = new MessagePackWriter(buf);
                w.WriteMapHeader(1);
                w.Write("n"); w.Write((long)(s * EventsPerSegment + i));
                w.Flush();

                Assert.True(engine.TryWrite(new LogEventHeader
                {
                    Id                       = new EventId(0u, (uint)(s * EventsPerSegment + i)).RawValue,
                    TimestampUtcTicks        = baseTicks + s * TimeSpan.TicksPerMinute + i * TimeSpan.TicksPerSecond,
                    Level                    = LogLevel.Information,
                    MessageTemplatePoolIndex = engine.TemplatePool.Intern("evt {n}"),
                    ServiceNamePoolIndex     = engine.TemplatePool.Intern("Svc.A"),
                }, buf.WrittenSpan.ToArray()));
            }
            await engine.FlushHotTierAsync();
        }
        Assert.Equal(ManySegments, engine.ListSegments().Count);
        return (engine, query);
    }

    // ── Shared query helper ───────────────────────────────────────────────────

    /// <summary>Drains a query, stopping at <paramref name="count"/>.</summary>
    public static async Task<List<LogEvent>> RunAsync(
        QueryExecutor query, string? filter, int count,
        DateTimeOffset? from = null, DateTimeOffset? to = null, bool forward = false)
    {
        var res = new List<LogEvent>(Math.Min(count, 1024));
        await foreach (var ev in query.ExecuteAsync(new QueryRequest
        {
            Filter    = filter,
            Count     = count,
            FromUtc   = from,
            ToUtc     = to,
            Direction = forward ? QueryDirection.Forward : QueryDirection.Backward,
        }))
        {
            res.Add(ev);
            if (res.Count >= count) break;
        }
        return res;
    }

    /// <summary>Identity by payload, not by EventId — the engine assigns ids itself.</summary>
    public static string OrderIdOf(LogEvent ev) => ev.Properties?["OrderId"] as string ?? "<none>";
}
