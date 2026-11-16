using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// A client may send a property whose NAME is one of the inverted buckets the segment index
/// files for itself from the event header — <c>@l</c>, <c>@service</c>, <c>@tr</c>, <c>@sp</c>,
/// <c>@x.type</c>, <c>@x.inner.type</c> and <c>@x.exists</c>, the marker "this row carries an
/// exception". Its values then share that bucket, and a string that spells a value the header
/// files there shares the header's own TERM.
///
/// <para>In the marker's bucket that used to cost the query rows. The builder adds a value to
/// the group's bloom filter only when the inverted index reports it as new, and the marker's
/// posting had already made <c>"true"</c> an existing value of that bucket — while what the
/// marker gives the bloom is the bucket's name, never the value. So the client's <c>"true"</c>
/// was never added, and the bloom gate dropped the group for a filter on it:
/// <c>['@x.exists'][0] = 'true'</c> returned the row while it was hot and nothing once the tier
/// was flushed. Only with an exception indexed first, which is how it hid: a row written before
/// any exception made the value new, and it was added.</para>
///
/// <para>The other six buckets are asked the same question here and always answered it: their
/// header fields give the bloom the very text they file, so a client's string finds it there
/// whichever came first. They are pinned so that the next field filed marker-fashion — a
/// posting whose bloom term is not its value — fails a test instead of a query.</para>
///
/// <para>Each test asks the hot tier, flushes (or merges), and asks again — through the real
/// <see cref="StorageEngine"/> and <see cref="QueryExecutor"/>, without the index cache and
/// through it. The invariant is the usual one: the index may cost a query its acceleration,
/// never its rows.</para>
/// </summary>
public sealed class ClientPropertyInAHeaderBucketTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-headerbucket-" + Guid.NewGuid().ToString("N"));
    private readonly List<StorageEngine> _engines = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var engine in _engines) { try { await engine.DisposeAsync(); } catch { } }
        QuerySegmentFixtures.DeleteDataDirectory(_dir);
    }

    // ── Every bucket the builder files from the header ────────────────────────

    /// <summary>
    /// The bucket, the value a header files in it, and the order the rows are written in:
    /// <c>header</c> is the row whose HEADER files that value, <c>client</c> the row that carries
    /// it as a property of the bucket's name, and <c>both</c> one row that does both — its own
    /// header filed a moment before its own property, so it needs no other row to go missing.
    /// </summary>
    public static TheoryData<string, string, string> BucketsAndOrders
    {
        get
        {
            var data = new TheoryData<string, string, string>();
            foreach (var (bucket, value) in HeaderBuckets)
            foreach (string order in (string[])["header,client,plain", "client,header,plain", "plain,header,client", "both,plain"])
                data.Add(bucket, value, order);
            return data;
        }
    }

    [Theory]
    [MemberData(nameof(BucketsAndOrders))]
    public async Task AClientValueInABucketTheHeaderFiles_IsReturnedFromTheSegmentAsFromTheHotTier(
        string bucket, string value, string order)
    {
        string[] rows  = order.Split(',');
        var      store = Store($"{bucket.Replace('.', '-').TrimStart('@')}-{order.Replace(',', '-')}");
        for (int i = 0; i < rows.Length; i++)
            Write(store.Engine, i, rows[i], bucket, value);

        string[] expected = [.. rows.Where(static row => row is "client" or "both")];
        await AssertHotAsync(store, AskForTheClientValue(bucket, value, expected[0]), expected);

        await store.Engine.FlushHotTierAsync();
        AssertOneGroupOf(store.Engine, rows.Length);

        await AssertFlushedAsync(store, AskForTheClientValue(bucket, value, expected[0]), expected, rows.Length);
    }

    // ── The marker's bucket, the other ways in ────────────────────────────────

    /// <summary>
    /// The built-in keeps answering from the header beside such a property, in both tiers. The
    /// client's row IS filed in the marker's posting list — the two share the term — which makes
    /// that list a superset of the rows with an exception; the scan decides, as it does for
    /// every candidate, so the list costs a read and never an answer.
    /// </summary>
    [Fact]
    public async Task TheBuiltInFieldStillAnswersFromTheHeader_BesideAClientPropertyOfItsName()
    {
        string[] filters = ["@x.exists = true", "['@x.exists'] = true", "@x.exists = 'true'", "has(@x)", "has(@x) and @l = 'Warning'"];
        string[] rows    = ["header", "client", "plain"];

        var store = Store("builtin");
        for (int i = 0; i < rows.Length; i++)
            Write(store.Engine, i, rows[i], Marker, "true");

        await AssertHotAsync(store, filters, ["header"]);
        await store.Engine.FlushHotTierAsync();
        AssertOneGroupOf(store.Engine, rows.Length);
        await AssertFlushedAsync(store, filters, ["header"], rows.Length);

        await AssertFlushedAsync(store, ["@x.exists = false", "not has(@x)"], ["client", "plain"], rows.Length);
    }

    /// <summary>
    /// A merge builds its groups with the same builder, so it was the other moment a row could go
    /// missing: flushed apart, the exception and the property are in two segments that each
    /// answer correctly, and the merge files them into one group — the exception first, by its
    /// timestamp. It is also what repairs a group written before this: its index is rebuilt
    /// from its rows.
    /// </summary>
    [Fact]
    public async Task ARowHoldingTheMarkersValue_SurvivesTheMergeThatPutsAnExceptionBeforeIt()
    {
        string[] filters = AskForTheClientValue(Marker, "true", "client");

        var store = Store("merge");
        Write(store.Engine, 0, "header", Marker, "true");
        Write(store.Engine, 1, "plain", Marker, "true");
        await store.Engine.FlushHotTierAsync();
        Write(store.Engine, 2, "client", Marker, "true");
        Write(store.Engine, 3, "plain", Marker, "true");
        await store.Engine.FlushHotTierAsync();

        Assert.Equal(2, store.Engine.ListSegments().Count);
        await AssertFlushedAsync(store, filters, ["client"], rows: 4, "two segments, before the merge");

        while (await store.Engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None)) { }
        AssertOneGroupOf(store.Engine, 4);       // or nothing was merged, and the test proves nothing

        await AssertFlushedAsync(store, filters, ["client"], rows: 4, "one segment, after the merge");
    }

    // ── Assertions ────────────────────────────────────────────────────────────

    private sealed record TestStore(StorageEngine Engine, QueryExecutor Query);

    /// <summary>
    /// Filters on the client's value. The array form is the one spelling that reaches the
    /// property at all: the bare name is the built-in field, which shadows a property of its
    /// name in both tiers (see <c>ExceptionExistsFieldTests</c>).
    ///
    /// <para>The marker's value is asked for as a bool literal as well. A stored string compares
    /// with the literal's text, so the hot tier matches it — and the bloom gate then probes the
    /// text a stored bool would have left, <c>True</c>, which folds to the same term.</para>
    /// </summary>
    private static string[] AskForTheClientValue(string bucket, string value, string row)
    {
        string[] filters =
        [
            $"['{bucket}'][0] = '{value}'",
            $"['{bucket}'][0] = '{value.ToUpperInvariant()}'",
            $"['{bucket}'][0] in ['{value}']",
            $"['{bucket}'][0] = '{value}' and @l = 'Warning'",
            $"Row = '{row}' and ['{bucket}'][0] = '{value}'",
        ];
        return value == "true" ? [.. filters, $"['{bucket}'][0] = true"] : filters;
    }

    private static async Task AssertHotAsync(TestStore store, string[] filters, string[] expected)
    {
        Assert.Empty(store.Engine.ListSegments());
        foreach (string filter in filters)
            await AssertRowsAsync(store.Query, filter, expected, "hot tier");
    }

    /// <summary>
    /// Asks every filter of the flushed store: uncached, then twice through the index cache,
    /// whose second answer comes from each group's memo.
    /// </summary>
    private static async Task AssertFlushedAsync(
        TestStore store, string[] filters, string[] expected, int rows, string state = "flushed")
    {
        using var cache = new SegmentIndexCache(16L * 1024 * 1024);
        var cached = new QueryExecutor(store.Engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance, cache);

        foreach (string filter in filters)
        {
            await AssertRowsAsync(store.Query, filter, expected, state);
            await AssertRowsAsync(cached, filter, expected, state + ", through the index cache");
            await AssertRowsAsync(cached, filter, expected, state + ", from the cache's memo");
        }

        // And the store is all there for a query that asks the index nothing.
        Assert.Equal(rows, (await RowsAsync(store.Query, null)).Length);
    }

    private static async Task AssertRowsAsync(QueryExecutor query, string filter, string[] expected, string tier)
    {
        string[] rows = await RowsAsync(query, filter);
        Assert.True(expected.AsSpan().SequenceEqual(rows),
            $"{tier}, `{filter}`: expected [{string.Join(", ", expected)}], got [{string.Join(", ", rows)}]");
    }

    /// <summary>The premise of every test here: all the rows share ONE index group, so the
    /// header's posting and the client's value really did meet in one bucket.</summary>
    private static void AssertOneGroupOf(StorageEngine engine, int rows)
    {
        var segment = Assert.Single(engine.ListSegments());
        using var reader = SegmentReader.Open(segment.FilePath);
        Assert.Equal(1, reader.Groups.Length);
        Assert.Equal((uint)rows, reader.Groups[0].EventCount);
    }

    /// <summary>The <c>Row</c> of every event a filter returns, oldest first — the order written.</summary>
    private static async Task<string[]> RowsAsync(QueryExecutor query, string? filter)
    {
        var events = await QuerySegmentFixtures.RunAsync(query, filter, int.MaxValue, forward: true);
        return [.. events.Select(static ev => (string)ev.Properties!["Row"]!)];
    }

    // ── Fixture ───────────────────────────────────────────────────────────────

    private const string Marker   = ClefFields.ExceptionExists;
    private const string Template = "settle {Row}";
    private const string Service  = "Billing.API";
    private const ulong  TraceHi  = 0x0123456789ABCDEFUL, TraceLo = 0xFEDCBA9876543210UL, Span = 0x00FF00FF00FF00FFUL;

    private static readonly ExceptionInfo Boom = new()
    {
        Type       = "System.InvalidOperationException",
        Message    = "gateway timeout on settle",
        StackTrace = "   at Billing.Settle()",
        Inner      = new ExceptionInfo { Type = "System.IO.IOException", Message = "disk" },
    };

    private static readonly byte[] BoomBytes = Boom.ToBytes();

    /// <summary>
    /// Every bucket <c>SegmentIndexBuilder</c> files from the header, with the value the
    /// <c>header</c> row of this fixture puts in it (see <see cref="Write"/>).
    /// </summary>
    private static readonly (string Bucket, string Value)[] HeaderBuckets =
    [
        (ClefFields.Level,              LogLevel.Warning.ToSeqString()),
        (ClefFields.ServiceName,        Service),
        (ClefFields.TraceId,            TraceIdHelper.FormatTraceId(TraceHi, TraceLo)!),
        (ClefFields.SpanId,             TraceIdHelper.FormatSpanId(Span)!),
        (ClefFields.ExceptionType,      Boom.Type),
        (ClefFields.ExceptionInnerType, Boom.Inner!.Type),
        (ClefFields.ExceptionExists,    "true"),
    ];

    /// <summary>An engine wired to the production index builder, and an executor with no index cache.</summary>
    private TestStore Store(string name)
    {
        string dir = Path.Combine(_dir, name);
        Directory.CreateDirectory(dir);
        var options = new ServerOptions { DataDirectory = dir };
        var engine  = new StorageEngine(
            Options.Create(options),
            new RetentionStore(options, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _engines.Add(engine);
        engine.IndexSinkFactory = static (events, terms) => new SegmentIndexBuilder(events, 5, terms);
        return new TestStore(engine, new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance));
    }

    /// <summary>
    /// Writes one row, named by its kind, a second after the one before it — ordinals in a
    /// segment follow the timestamps, so the order written is the order indexed.
    ///
    /// <para>A <c>header</c> or <c>both</c> row carries, in its HEADER, everything the builder
    /// files from one: the service, a trace and span id, an exception with an inner one. A
    /// <c>client</c> or <c>both</c> row carries the property <paramref name="bucket"/> holding
    /// <c>[<paramref name="value"/>]</c>. Every row is a Warning: a flush splits by level, and
    /// the rows have to meet in one segment (so in the level's bucket the header of each row
    /// files the value itself, <c>client</c> rows included).</para>
    /// </summary>
    private static void Write(StorageEngine engine, int ordinal, string row, string bucket, string value)
    {
        bool header = row is "header" or "both", client = row is "client" or "both";

        var buf = new ArrayBufferWriter<byte>(128);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(client ? 2 : 1);
        w.Write("Row"); w.Write(row);
        if (client)
        {
            w.Write(bucket);
            w.WriteArrayHeader(1);
            w.Write(value);
        }
        w.Flush();

        // Well in the past, so a merge treats the level's bucket as sealed and takes the pair.
        long baseTicks = new DateTimeOffset(2026, 8, 30, 9, 0, 0, TimeSpan.Zero).UtcTicks;
        Assert.True(engine.TryWrite(new LogEventHeader
        {
            Id                       = new EventId(0u, (uint)ordinal).RawValue,
            TimestampUtcTicks        = baseTicks + ordinal * TimeSpan.TicksPerSecond,
            Level                    = LogLevel.Warning,
            MessageTemplatePoolIndex = engine.TemplatePool.Intern(Template),
            ServiceNamePoolIndex     = engine.TemplatePool.Intern(header ? Service : "Ledger.API"),
            TraceIdHi                = header ? TraceHi : 0,
            TraceIdLo                = header ? TraceLo : 0,
            SpanId                   = header ? Span : 0,
        }, buf.WrittenSpan, Template, header ? BoomBytes : default));
    }
}
