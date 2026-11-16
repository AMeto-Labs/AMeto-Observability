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
/// A search for exceptions — <c>has(@x)</c>, <c>@x.exists = true</c> — answered from the segment
/// index instead of by reading every row of the window. It used to scan everything: the
/// predicate gave the index nothing to look up, so a week of Information segments holding no
/// exception at all was decoded row by row to say so.
///
/// <para>What is pinned here, over real flushed segments and through the real executor:</para>
/// <list type="bullet">
///   <item>THE ROWS DO NOT CHANGE — for the predicates that ask the marker and for the ones that
///   still scan (<c>not has(@x)</c>, <c>@x.exists = false</c>), including the row whose exception
///   map the index could not read and the row whose exception column holds nil.</item>
///   <item>WHAT THE INDEX SAYS PER GROUP: a group with no exception is dropped unread, a group
///   where every row has one contributes every row without naming them, a mixed one names its
///   rows.</item>
///   <item>A STORE WRITTEN BEFORE THIS loses nothing. Its groups carry no word that their marker
///   is complete, so they answer "no information" and are scanned, exactly as before — and once
///   a merge has rewritten them they answer from the marker like any other.</item>
/// </list>
/// </summary>
public sealed class ExceptionPresenceIndexTests : IAsyncLifetime
{
    /// <summary>Rows of each level in each of the two flushes.</summary>
    private const int PerLevel = 60;

    private static readonly LogLevel[] Levels = [LogLevel.Information, LogLevel.Warning, LogLevel.Error];

    private static readonly string[] AskForAnException =
    [
        "has(@x)", "isDefined(@x)", "has(Exception)", "has(@x.type)",
        "@x.exists = true", "@x.exists = 'TRUE'", "@x.exists != false", "@x.exists in [true]",
        "['@x.exists'] = true", "Exception.Exists = true",
    ];

    private static readonly string[] AskForNone =
    [
        "not has(@x)", "@x.exists = false", "@x.exists != true", "@x.exists in [false]",
    ];

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-xpresence-" + Guid.NewGuid().ToString("N"));
    private readonly List<StorageEngine> _engines = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var engine in _engines) { try { await engine.DisposeAsync(); } catch { } }
        QuerySegmentFixtures.DeleteDataDirectory(_dir);
    }

    // ── A store this build wrote ──────────────────────────────────────────────

    [Fact]
    public async Task AStoreWrittenByThisBuild_AnswersFromTheMarker_AndReturnsTheSameRows()
    {
        var store = await StoreAsync("vouched", static (events, terms) => new SegmentIndexBuilder(events, 5, terms));

        await AssertAnswersAsync(store);

        // Six segments of one group each: an Information, a Warning and an Error one per flush.
        // Information holds no exception; every Error row has one; a third of Warning does,
        // plus the row whose map cannot be read.
        string   mixed    = $"{WarningRowsWithAnException} of {PerLevel}";
        string[] expected = [mixed, mixed, "dropped", "dropped", "every row", "every row"];
        foreach (string filter in AskForAnException)
            Assert.Equal(expected, GroupVerdicts(store.Engine, filter));

        // The predicates that are not that question still ask the index nothing.
        foreach (string filter in AskForNone)
            Assert.All(GroupVerdicts(store.Engine, filter), static verdict => Assert.Equal("no information", verdict));
    }

    // ── A store written before the marker was complete ────────────────────────

    [Fact]
    public async Task AStoreWrittenBeforeThis_IsScanned_LosesNothing_AndAnswersFromTheMarkerOnceMerged()
    {
        var store = await StoreAsync("older", static (events, terms) => new PreVouchingIndexSink(events, terms));

        // Its groups hold a marker — without the unreadable row — and no word. Narrowing by that
        // list would lose the row; dropping a group for lacking one would lose a Warning segment.
        foreach (string filter in AskForAnException)
            Assert.All(GroupVerdicts(store.Engine, filter), static verdict => Assert.Equal("no information", verdict));
        await AssertAnswersAsync(store);

        // The next build's compaction rewrites them, index and all.
        store.Engine.IndexSinkFactory = static (events, terms) => new SegmentIndexBuilder(events, 5, terms);
        int before = store.Engine.ListSegments().Count;
        while (await store.Engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None)) { }
        Assert.True(store.Engine.ListSegments().Count < before, "setup: nothing was merged — the test proves nothing");

        foreach (string filter in AskForAnException)
            Assert.DoesNotContain("no information", GroupVerdicts(store.Engine, filter));
        await AssertAnswersAsync(store);
    }

    // ── Assertions ────────────────────────────────────────────────────────────

    private sealed record Store(StorageEngine Engine, QueryExecutor Query, string[] WithAnException, string[] Without);

    private static async Task AssertAnswersAsync(Store store)
    {
        foreach (string filter in AskForAnException)
            Assert.Equal(store.WithAnException, await RowsAsync(store.Query, filter));

        foreach (string filter in AskForNone)
            Assert.Equal(store.Without, await RowsAsync(store.Query, filter));

        // Beside a conjunct of its own, in either order, and beside the level the UI adds.
        string[] warnings = [.. store.WithAnException.Where(static row => row.StartsWith("W", StringComparison.Ordinal))];
        Assert.Equal(warnings, await RowsAsync(store.Query, "has(@x) and @l = 'Warning'"));
        Assert.Equal(warnings, await RowsAsync(store.Query, "@l = 'Warning' and @x.exists = true"));
        string[] first = [warnings[0]];
        Assert.Equal(first, await RowsAsync(store.Query, $"@x.exists = true and Row = '{warnings[0]}'"));

        // And the whole store is still there for a query that asks nothing.
        Assert.Equal(store.WithAnException.Length + store.Without.Length, (await RowsAsync(store.Query, null)).Length);
    }

    /// <summary>The <c>Row</c> of every event a filter returns, oldest first — the order written.</summary>
    private static async Task<string[]> RowsAsync(QueryExecutor query, string? filter)
    {
        var events = await QuerySegmentFixtures.RunAsync(query, filter, int.MaxValue, forward: true);
        return [.. events.Select(static ev => (string)ev.Properties!["Row"]!)];
    }

    /// <summary>
    /// What the production narrowing says about <paramref name="filter"/> for every index group
    /// of every segment, sorted: "dropped" (proven to hold no match), "every row", "n of m" (the
    /// rows it names) or "no information" (scan).
    /// </summary>
    private static string[] GroupVerdicts(StorageEngine engine, string filter)
    {
        var compiled = CompiledFilter.Compile(filter);
        var verdicts = new List<string>();
        foreach (var segment in engine.ListSegments())
        {
            using var reader = SegmentReader.Open(segment.FilePath);
            for (int g = 0; g < reader.Groups.Length; g++)
            {
                using var inverted = reader.RentInvertedIndexBytes(g);
                using var trigram  = reader.RentTrigramIndexBytes(g);
                using var bloom    = reader.RentBloomFilterBytes(g);
                using var index    = SegmentIndexReader.Load(inverted.Span, trigram.Span, bloom.Span);
                uint rows = reader.Groups[g].EventCount;

                if (!QueryExecutor.TryNarrowWithIndex(compiled, index, null, rows, out uint[]? candidates, out bool everyRow))
                    verdicts.Add("dropped");
                else if (everyRow)
                    verdicts.Add("every row");
                else
                    verdicts.Add(candidates is null ? "no information" : $"{candidates.Length} of {rows}");
            }
        }
        verdicts.Sort(StringComparer.Ordinal);
        return [.. verdicts];
    }

    // ── Fixture ───────────────────────────────────────────────────────────────

    /// <summary>A third of the Warning rows carry a readable exception, and row 1 an unreadable one.</summary>
    private const int WarningRowsWithAnException = PerLevel / 3 + 1;

    private static readonly byte[] Readable =
        new ExceptionInfo
        {
            Type       = "System.InvalidOperationException",
            Message    = "gateway timeout on settle",
            StackTrace = "   at Billing.Settle()",
            Inner      = new ExceptionInfo { Type = "System.IO.IOException", Message = "disk" },
        }.ToBytes();

    /// <summary>
    /// A map whose <c>type</c> is not a string. <c>LogEvent.HasException</c> answers from the lead
    /// byte and says yes; the index cannot read its fields.
    /// </summary>
    private static readonly byte[] Unreadable = Pack(static (ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(1);
        w.Write("type");
        w.Write(7);
    });

    /// <summary>msgpack nil: an exception column that is not empty and holds no exception.</summary>
    private static readonly byte[] Nil = [0xC0];

    /// <summary>What row <paramref name="i"/> of a level carries in its exception column.</summary>
    private static byte[] ExceptionOf(LogLevel level, int i) => level switch
    {
        LogLevel.Error                  => Readable,
        LogLevel.Warning when i == 1    => Unreadable,
        LogLevel.Warning when i == 2    => Nil,
        LogLevel.Warning when i % 3 == 0 => Readable,
        _                               => [],
    };

    /// <summary>
    /// Two flushes of <see cref="PerLevel"/> rows per level. A flush splits by level, so each
    /// makes three segments — one that holds no exception, one where every row has one, and a
    /// mixed one — and two of each is what a merge needs to have something to do.
    /// </summary>
    private async Task<Store> StoreAsync(string name, SegmentIndexSinkFactory indexSink)
    {
        string dir = Path.Combine(_dir, name);
        Directory.CreateDirectory(dir);
        var options = new ServerOptions { DataDirectory = dir };
        var engine  = new StorageEngine(
            Options.Create(options),
            new RetentionStore(options, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _engines.Add(engine);
        engine.IndexSinkFactory = indexSink;
        var query = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);

        // Well in the past, so the merge treats each level's bucket as sealed and takes a pair.
        long baseTicks = new DateTimeOffset(2026, 8, 30, 9, 0, 0, TimeSpan.Zero).UtcTicks;
        int  template  = engine.TemplatePool.Intern("settle {Row}");
        int  service   = engine.TemplatePool.Intern("Billing.API");
        var  buf       = new ArrayBufferWriter<byte>(64);
        var  with      = new List<string>();
        var  without   = new List<string>();
        int  written   = 0;

        for (int flush = 0; flush < 2; flush++)
        {
            foreach (var level in Levels)
            for (int i = 0; i < PerLevel; i++)
            {
                // Named so that ordinal order is write order, and the level reads off the name.
                string row       = $"{level.ToString()[0]}-{flush}-{i:D3}";
                byte[] exception = ExceptionOf(level, i);
                (ExceptionInfo.IsPresent(exception) ? with : without).Add(row);

                buf.ResetWrittenCount();
                var w = new MessagePackWriter(buf);
                w.WriteMapHeader(1);
                w.Write("Row"); w.Write(row);
                w.Flush();

                Assert.True(engine.TryWrite(new LogEventHeader
                {
                    Id                       = new EventId(0u, (uint)written).RawValue,
                    TimestampUtcTicks        = baseTicks + written * TimeSpan.TicksPerSecond,
                    Level                    = level,
                    MessageTemplatePoolIndex = template,
                    ServiceNamePoolIndex     = service,
                }, buf.WrittenSpan, "settle {Row}", exception));
                written++;
            }
            await engine.FlushHotTierAsync();
        }

        Assert.Equal(2 * Levels.Length, engine.ListSegments().Count);
        return new Store(engine, query, [.. with], [.. without]);
    }

    private delegate void PackAction(ref MessagePackWriter writer);

    private static byte[] Pack(PackAction write)
    {
        var buf = new ArrayBufferWriter<byte>(32);
        var w   = new MessagePackWriter(buf);
        write(ref w);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }
}
