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

    // ── Many small INDEXED segments ───────────────────────────────────────────

    /// <summary>Events in <see cref="ManyIndexedSegmentsAsync"/>: one more in every fifth segment.</summary>
    public const int ManyIndexedEvents = ManySegments * EventsPerSegment + ManySegments / 5;

    /// <summary>
    /// The many-segments shape again, written through the production index wiring so a filter
    /// goes through the index prefilter, with a property shape for each thing a lazy prefilter
    /// has to get right:
    /// <list type="bullet">
    /// <item><c>Customer</c> cycles through four values in every segment, so the newest segment
    /// alone fills a small page and everything older is work the page does not need.</item>
    /// <item><c>Batch = 'first'</c> holds in the OLDEST segment only, so a newest-first page has to
    /// check the whole window to find it, and must still find it.</item>
    /// <item>Every fifth segment carries one more event three minutes past its own, so time ranges
    /// overlap and the priming order (by MaxTs) is not the catalog order.</item>
    /// </list>
    /// <paramref name="engineLog"/> is the engine's logger, for a test that asserts what the
    /// storage side says while queries run over these segments; null logs nowhere.
    /// </summary>
    public static async Task<(StorageEngine Engine, QueryExecutor Query)> ManyIndexedSegmentsAsync(
        string dir, Microsoft.Extensions.Logging.ILogger<StorageEngine>? engineLog = null)
    {
        Directory.CreateDirectory(dir);
        var engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = dir }),
            new RetentionStore(new ServerOptions { DataDirectory = dir }, NullLogger<RetentionStore>.Instance),
            engineLog ?? NullLogger<StorageEngine>.Instance);
        engine.IndexSinkFactory = static (estimatedEventCount, termsPerEvent) =>
            new SegmentIndexBuilder(estimatedEventCount, 5, termsPerEvent);
        var query = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);

        long baseTicks = new DateTimeOffset(2026, 7, 30, 0, 0, 0, TimeSpan.Zero).UtcTicks;
        int  tmplIdx   = engine.TemplatePool.Intern("evt {n}");
        int  svcIdx    = engine.TemplatePool.Intern("Svc.A");
        var  buf       = new ArrayBufferWriter<byte>(128);

        void Write(int segment, long n, long ticks)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(3);
            w.Write("n");        w.Write(n);
            w.Write("Customer"); w.Write("cust-" + (n % 4));
            w.Write("Batch");    w.Write(segment == 0 ? "first" : "later");
            w.Flush();

            Assert.True(engine.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)n).RawValue,
                TimestampUtcTicks        = ticks,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
            }, buf.WrittenSpan.ToArray()));
        }

        for (int s = 0; s < ManySegments; s++)
        {
            for (int i = 0; i < EventsPerSegment; i++)
                Write(s, s * EventsPerSegment + i, baseTicks + s * TimeSpan.TicksPerMinute + i * TimeSpan.TicksPerSecond);
            if (s % 5 == 0)
                Write(s, 100_000 + s, baseTicks + (s + 3) * TimeSpan.TicksPerMinute + 30 * TimeSpan.TicksPerSecond);
            await engine.FlushHotTierAsync();
        }
        Assert.Equal(ManySegments, engine.ListSegments().Count);
        return (engine, query);
    }

    // ── Error segments, some of whose rows carry an exception ─────────────────

    /// <summary>Segments in <see cref="ErrorSegmentsAsync"/>.</summary>
    public const int ErrorSegments = 8;

    /// <summary>The <c>Lane</c> of the even rows of <see cref="ErrorSegmentsAsync"/>, and of the odd ones.</summary>
    public const string EvenLane = "even", OddLane = "odd";

    /// <summary>
    /// What a level-split store's Error level looks like to a search for exceptions: eight
    /// one-group segments of <paramref name="rowsPerSegment"/> Error events each, written through
    /// the production index wiring, in which <paramref name="percentWithException"/> rows of
    /// every hundred carry an exception of ordinary size (a ~1.3 KB stack trace) and the rest
    /// carry none. Eight because that is one prefilter batch
    /// (<c>QueryExecutor.PrefilterParallelism</c> at its cap), which is what a first page pays
    /// for before it returns a row.
    ///
    /// <para><c>Row</c> is unique and ascending, so a result can be checked for which rows it
    /// holds and in what order. <c>Lane</c> alternates between <see cref="EvenLane"/> and
    /// <see cref="OddLane"/> by row, so an equality on it names every other row of every group:
    /// the ordinary indexed equality, neither a lookup of one row nor the whole group.
    /// <paramref name="cache"/> is the index cache production runs with; null answers every query
    /// from the sections.</para>
    /// </summary>
    /// <param name="segments">
    /// How many flushes the rows are written in, <see cref="ErrorSegments"/> unless said otherwise.
    /// </param>
    /// <param name="groupPayloadBudgetBytes">
    /// When set, the index-group payload budget, so that one large flush comes out as ONE segment
    /// of several index groups — the shape compaction leaves a day's Error level in — where the
    /// default budget makes each segment here a single group.
    /// </param>
    public static async Task<(StorageEngine Engine, QueryExecutor Query)> ErrorSegmentsAsync(
        string dir, int rowsPerSegment, int percentWithException, SegmentIndexCache? cache = null,
        int segments = ErrorSegments, long? groupPayloadBudgetBytes = null)
    {
        Directory.CreateDirectory(dir);
        var options = new ServerOptions
        {
            DataDirectory = dir,
            HotTier       = new HotTierOptions { MaxSizeBytes = 256L * 1024 * 1024 },
        };
        var engine = new StorageEngine(
            Options.Create(options),
            new RetentionStore(options, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        if (groupPayloadBudgetBytes is { } budget) engine._groupPayloadBudgetBytes = budget;
        engine.IndexSinkFactory = static (estimatedEventCount, termsPerEvent) =>
            new SegmentIndexBuilder(estimatedEventCount, 5, termsPerEvent);
        var query = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance, cache);

        byte[] exception = new ExceptionInfo
        {
            Type       = "System.InvalidOperationException",
            Message    = "gateway timeout on settle",
            StackTrace = string.Join('\n', Enumerable.Repeat("   at Billing.Api.Orders.SettleAsync(OrderId id) in /src/Orders.cs:line 118", 16)),
        }.ToBytes();

        long baseTicks = new DateTimeOffset(2026, 7, 30, 0, 0, 0, TimeSpan.Zero).UtcTicks;
        int  tmplIdx   = engine.TemplatePool.Intern("settle {Row} failed");
        int  svcIdx    = engine.TemplatePool.Intern("Billing.API");
        var  buf       = new ArrayBufferWriter<byte>(64);

        int row = 0;
        for (int s = 0; s < segments; s++)
        {
            for (int i = 0; i < rowsPerSegment; i++, row++)
            {
                buf.ResetWrittenCount();
                var w = new MessagePackWriter(buf);
                w.WriteMapHeader(2);
                w.Write("Row");  w.Write(row.ToString("D7", System.Globalization.CultureInfo.InvariantCulture));
                w.Write("Lane"); w.Write(row % 2 == 0 ? EvenLane : OddLane);
                w.Flush();

                Assert.True(engine.TryWrite(new LogEventHeader
                {
                    Id                       = new EventId(0u, (uint)row).RawValue,
                    TimestampUtcTicks        = baseTicks + row * TimeSpan.TicksPerSecond,
                    Level                    = LogLevel.Error,
                    MessageTemplatePoolIndex = tmplIdx,
                    ServiceNamePoolIndex     = svcIdx,
                }, buf.WrittenSpan, "settle {Row} failed", i % 100 < percentWithException ? exception : default));
            }
            await engine.FlushHotTierAsync();
        }
        Assert.Equal(segments, engine.ListSegments().Count);
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

    // ── Teardown ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Removes a test's data directory once its engine is disposed. Use this rather than a bare
    /// <c>Directory.Delete</c>, everywhere in this suite.
    ///
    /// <para>It is <see cref="Ameto.Testing.TestDataDirectory.Delete(string)"/>, which the test projects
    /// share: retried while a handle that is about to close still blocks it — an antivirus or an
    /// indexer reading the fresh <c>.seg</c> files — and a failure of the
    /// test when the directory is there at the end of that, where this method used to return in
    /// silence. Its history is why the helper exists. Disposing the engine did not close
    /// <c>Ameto.db</c>: <see cref="RetentionStore"/> opened it through Microsoft.Data.Sqlite's
    /// connection pool, which parked the handle past the store, so on Windows the delete failed,
    /// the <c>catch { }</c> every teardown wrapped it in swallowed that, and each run left an
    /// <c>ameto-*</c> directory in TEMP for ever. This method cleared the pool first; the store no
    /// longer pools.</para>
    ///
    /// <para><c>FixtureCleanupTests</c> asserts the outcome over a real fixture.</para>
    /// </summary>
    public static void DeleteDataDirectory(string dir) => Ameto.Testing.TestDataDirectory.Delete(dir);
}
