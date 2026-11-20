using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// The inverted index files a typed scalar under a TYPE-TAGGED form — the long 5 as <c>\0l5</c>,
/// the double 2.5 as <c>\0d2.5</c>, a bool as <c>\0true</c> / <c>\0false</c>, a nil as
/// <c>\0null</c> (<see cref="IndexValueForms.Serialise"/>) — and a string under its own bytes. So
/// a string that begins with NUL can spell a typed value's form exactly, and under one property
/// the two are then ONE term, for which the bloom filter is given two different texts: the
/// string's own bytes, and the value's plain text (<c>5</c>).
///
/// <para>That used to cost rows. The builder gives a value's text to the bloom only on its term's
/// first sight, so whichever of the two was filed second never reached the bloom, and the bloom
/// gate dropped the group for a filter on it: beside the string <c>"\0l5"</c>, <c>P = 5</c>
/// returned the long's row from the hot tier and nothing once the tier was flushed; the other
/// way round <c>P = '\0l5'</c> lost the string's row. The header files three strings a client
/// controls as well — the service, and the exception's type and inner type — so a NUL-led one
/// meets a typed value of a client property named like its bucket in the same way.</para>
///
/// <para>Each test names the row it is about and asserts that the hot tier returns it; flushes
/// (or merges); checks that the two really did meet in one term of one index group; and asks
/// again, through the real <see cref="StorageEngine"/> and <see cref="QueryExecutor"/>, without
/// the index cache and through it. The flushed answer must be the hot one: the index may cost a
/// query its acceleration, never its rows. (The hot answer is the reference rather than a list
/// written here because one of these filters also matches a stored <c>false</c>, by the scan's
/// rule that a stored <c>false</c> equals any literal but <c>'true'</c>.)</para>
/// </summary>
public sealed class StringSpellingATypedTermTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-taggedterm-" + Guid.NewGuid().ToString("N"));
    private readonly List<StorageEngine> _engines = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var engine in _engines) { try { await engine.DisposeAsync(); } catch { } }
        QuerySegmentFixtures.DeleteDataDirectory(_dir);
    }

    // ── A string and a typed value under one property ────────────────────────

    /// <summary>
    /// Every typed form the walk files, and the ways the two filers can meet: in rows apart,
    /// either first, or as the two elements of one array, either first.
    /// </summary>
    public static TheoryData<string, string> KindsAndArrangements
    {
        get
        {
            var data = new TheoryData<string, string>();
            foreach (string kind in Kinds.Keys)
            foreach (string arrangement in (string[])["string,typed", "typed,string", "[string,typed]", "[typed,string]"])
                data.Add(kind, arrangement);
            return data;
        }
    }

    [Theory]
    [MemberData(nameof(KindsAndArrangements))]
    public async Task AStringAndATypedValueSharingATerm_AreBothFoundInTheSegment(string kind, string arrangement)
    {
        var    k      = Kinds[kind];
        bool   oneRow = arrangement.StartsWith('[');
        string[] order = arrangement.Trim('[', ']').Split(',');

        var store = Store($"{kind}-{(oneRow ? "array" : "rows")}-{order[0]}");
        int rows;
        if (oneRow)
        {
            Write(store.Engine, 0, "both", k, order);
            Write(store.Engine, 1, "plain", k);
            rows = 2;
        }
        else
        {
            Write(store.Engine, 0, order[0], k, order[0]);
            Write(store.Engine, 1, order[1], k, order[1]);
            Write(store.Engine, 2, "plain", k);
            rows = 3;
        }

        // In an array the element has to be named: `P = 5` compares the whole array.
        string stringPath = oneRow ? $"P[{Array.IndexOf(order, "string")}]" : "P";
        string typedPath  = oneRow ? $"P[{Array.IndexOf(order, "typed")}]"  : "P";
        List<(string Filter, string Subject)> asks = [($"{stringPath} = '{k.Spelling}'", oneRow ? "both" : "string")];
        foreach (string filter in k.TypedFilters(typedPath))
            asks.Add((filter, oneRow ? "both" : "typed"));

        var hot = await AskTheHotTierAsync(store, asks);
        await store.Engine.FlushHotTierAsync();
        AssertOneGroupOf(store.Engine, rows);
        AssertTheyShareATerm(store.Engine, "P", k.Value, k.Spelling, oneRow ? [0u] : [0u, 1u]);
        await AssertFlushedAsync(store, hot, rows);
    }

    // ── A header string and a typed value of a client property ────────────────

    /// <summary>
    /// The three buckets the header files a client's string in, and the ways the header's string
    /// and the client's typed value can meet: rows apart, either first, or on one row — where the
    /// header's is always first.
    /// </summary>
    public static TheoryData<string, string> HeaderBucketsAndArrangements
    {
        get
        {
            var data = new TheoryData<string, string>();
            foreach (string bucket in (string[])[ClefFields.ServiceName, ClefFields.ExceptionType, ClefFields.ExceptionInnerType])
            foreach (string arrangement in (string[])["header,client", "client,header", "both"])
                data.Add(bucket, arrangement);
            return data;
        }
    }

    [Theory]
    [MemberData(nameof(HeaderBucketsAndArrangements))]
    public async Task AHeaderStringAndATypedValueSharingATerm_AreBothFoundInTheSegment(string bucket, string arrangement)
    {
        string[] rows   = [.. arrangement.Split(','), "plain"];
        bool     oneRow = rows[0] == "both";
        var      store  = Store($"{bucket.TrimStart('@').Replace('.', '-')}-{arrangement.Replace(',', '-')}");
        for (int i = 0; i < rows.Length; i++)
            WriteHeaderRow(store.Engine, i, rows[i], bucket);

        List<(string Filter, string Subject)> asks =
        [
            ($"{bucket} = '{HeaderSpelling}'",  oneRow ? "both" : "header"),   // the built-in: the header's string
            ($"['{bucket}'][0] = 5",           oneRow ? "both" : "client"),   // the client's property of that name
            ($"['{bucket}'][0] = '5'",         oneRow ? "both" : "client"),
        ];

        var hot = await AskTheHotTierAsync(store, asks);
        await store.Engine.FlushHotTierAsync();
        AssertOneGroupOf(store.Engine, rows.Length);
        AssertTheyShareATerm(store.Engine, bucket, 5L, HeaderSpelling, oneRow ? [0u] : [0u, 1u]);
        await AssertFlushedAsync(store, hot, rows.Length);
    }

    // ── A merge ───────────────────────────────────────────────────────────────

    /// <summary>
    /// A merge indexes with the same builder, so it is the other moment the two can meet: flushed
    /// apart they are in two segments that each answer correctly, and the merge files them into
    /// one group. It is also what repairs a group written before this: its index is rebuilt
    /// from its rows.
    /// </summary>
    [Fact]
    public async Task AStringAndATypedValueFlushedApart_AreBothFoundAfterTheMergeThatPutsThemInOneGroup()
    {
        var k = Kinds["long"];
        var store = Store("merge");
        Write(store.Engine, 0, "string", k, "string");
        Write(store.Engine, 1, "plain-a", k);
        await store.Engine.FlushHotTierAsync();
        Write(store.Engine, 2, "typed", k, "typed");
        Write(store.Engine, 3, "plain-b", k);
        await store.Engine.FlushHotTierAsync();

        List<(string Filter, string[] Rows)> asks =
        [
            ($"P = '{k.Spelling}'", ["string"]),
            ("P = 5",               ["typed"]),
            ("P in [5]",            ["typed"]),
            ("P = '5'",             ["typed"]),
        ];

        Assert.Equal(2, store.Engine.ListSegments().Count);
        await AssertFlushedAsync(store, asks, rows: 4, "two segments, before the merge");

        while (await store.Engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None)) { }
        AssertOneGroupOf(store.Engine, 4);       // or nothing was merged, and the test proves nothing
        AssertTheyShareATerm(store.Engine, "P", 5L, k.Spelling, [0u, 2u]);

        await AssertFlushedAsync(store, asks, rows: 4, "one segment, after the merge");
    }

    // ── Assertions ────────────────────────────────────────────────────────────

    private sealed record TestStore(StorageEngine Engine, QueryExecutor Query);

    /// <summary>
    /// Asks the hot tier every filter, asserting that it returns the row each one is about, and
    /// hands back what it returned — the answer the flushed segment has to give.
    /// </summary>
    private static async Task<List<(string Filter, string[] Rows)>> AskTheHotTierAsync(
        TestStore store, List<(string Filter, string Subject)> asks)
    {
        Assert.Empty(store.Engine.ListSegments());
        var answers = new List<(string, string[])>(asks.Count);
        foreach (var (filter, subject) in asks)
        {
            string[] rows = await RowsAsync(store.Query, filter);
            Assert.True(rows.Contains(subject),
                $"hot tier, `{Show(filter)}`: the premise is that it returns {subject}, and it returned [{string.Join(", ", rows)}]");
            answers.Add((filter, rows));
        }
        return answers;
    }

    /// <summary>
    /// Asks every filter of the flushed store: uncached, then twice through the index cache,
    /// whose second answer comes from each group's memo.
    /// </summary>
    private static async Task AssertFlushedAsync(
        TestStore store, List<(string Filter, string[] Rows)> asks, int rows, string state = "flushed")
    {
        using var cache = new SegmentIndexCache(16L * 1024 * 1024);
        var cached = new QueryExecutor(store.Engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance, cache);

        foreach (var (filter, expected) in asks)
        {
            await AssertRowsAsync(store.Query, filter, expected, state);
            await AssertRowsAsync(cached, filter, expected, state + ", through the index cache");
            await AssertRowsAsync(cached, filter, expected, state + ", from the cache's memo");
        }

        // And the store is all there for a query that asks the index nothing.
        Assert.Equal(rows, (await RowsAsync(store.Query, null)).Length);
    }

    private static async Task AssertRowsAsync(QueryExecutor query, string filter, string[] expected, string state)
    {
        string[] rows = await RowsAsync(query, filter);
        Assert.True(expected.AsSpan().SequenceEqual(rows),
            $"{state}, `{Show(filter)}`: expected [{string.Join(", ", expected)}], got [{string.Join(", ", rows)}]");
    }

    /// <summary>The premise of every test here: all the rows share ONE index group.</summary>
    private static void AssertOneGroupOf(StorageEngine engine, int rows)
    {
        var segment = Assert.Single(engine.ListSegments());
        using var reader = SegmentReader.Open(segment.FilePath);
        Assert.Equal(1, reader.Groups.Length);
        Assert.Equal((uint)rows, reader.Groups[0].EventCount);
    }

    /// <summary>
    /// The other premise, read off the flushed group's index: the typed value and the string were
    /// filed as ONE term — asked for either, the bucket names the same rows, both filers' among
    /// them. Without it a passing test would be passing for want of a meeting.
    /// </summary>
    private static void AssertTheyShareATerm(StorageEngine engine, string bucket, object? typed, string spelling, uint[] rows)
    {
        var segment = Assert.Single(engine.ListSegments());
        using var reader   = SegmentReader.Open(segment.FilePath);
        using var inverted = reader.RentInvertedIndexBytes(0);
        using var trigram  = reader.RentTrigramIndexBytes(0);
        using var bloom    = reader.RentBloomFilterBytes(0);
        using var index    = SegmentIndexReader.Load(inverted.Span, trigram.Span, bloom.Span);

        uint[]? byValue    = index.Lookup(bucket, typed);
        uint[]? bySpelling = index.Lookup(bucket, spelling);
        Assert.True(byValue is not null && bySpelling is not null
                    && byValue.AsSpan().SequenceEqual(rows) && bySpelling.AsSpan().SequenceEqual(rows),
            $"({bucket}, {typed ?? "null"}) names [{string.Join(", ", byValue ?? [])}] and ({bucket}, `{Show(spelling)}`) " +
            $"names [{string.Join(", ", bySpelling ?? [])}]; one term would name [{string.Join(", ", rows)}] for both");
    }

    /// <summary>The <c>Row</c> of every event a filter returns, oldest first — the order written.</summary>
    private static async Task<string[]> RowsAsync(QueryExecutor query, string? filter)
    {
        var events = await QuerySegmentFixtures.RunAsync(query, filter, int.MaxValue, forward: true);
        return [.. events.Select(static ev => (string)ev.Properties!["Row"]!)];
    }

    /// <summary>A filter as a message can carry it: the NUL these are made of, made visible.</summary>
    private static string Show(string text) => text.Replace("\0", "\\0", StringComparison.Ordinal);

    // ── Fixture ───────────────────────────────────────────────────────────────

    private delegate void PackValue(ref MessagePackWriter writer);

    /// <summary>
    /// A typed value the walk files, the string that spells its term, and the filters that find
    /// the typed value at a path — each one a filter the bloom gate is asked about.
    /// </summary>
    private sealed record Kind(string Spelling, object? Value, PackValue Typed, Func<string, string[]> TypedFilters);

    /// <summary>
    /// Every typed form the property walk files. A nil is asked for by no filter the index sees
    /// (<c>P = null</c> is never hinted), so its own text is never probed; its term is still
    /// shared, and the string beside it is what can go missing.
    /// </summary>
    private static readonly Dictionary<string, Kind> Kinds = new()
    {
        ["long"]   = new("\0l5",    5L,    static (ref MessagePackWriter w) => w.Write(5L),    static p => [$"{p} = 5", $"{p} in [5]", $"{p} = '5'"]),
        ["double"] = new("\0d2.5",  2.5d,  static (ref MessagePackWriter w) => w.Write(2.5d),  static p => [$"{p} = 2.5", $"{p} = '2.5'"]),
        ["true"]   = new("\0true",  true,  static (ref MessagePackWriter w) => w.Write(true),  static p => [$"{p} = true", $"{p} = 'true'"]),
        ["false"]  = new("\0false", false, static (ref MessagePackWriter w) => w.Write(false), static p => [$"{p} = false"]),
        ["nil"]    = new("\0null",  null,  static (ref MessagePackWriter w) => w.WriteNil(),   static _ => []),
    };

    /// <summary>The string a header field carries in the header tests: the long 5's term.</summary>
    private const string HeaderSpelling = "\0l5";

    private const string Template = "settle {Row}";

    private static long BaseTicks => new DateTimeOffset(2026, 8, 30, 9, 0, 0, TimeSpan.Zero).UtcTicks;

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
    /// Writes one Warning row named <paramref name="row"/>, a second after the one before it, in
    /// which <c>P</c> holds <paramref name="values"/> — <c>string</c>, the kind's spelling, and
    /// <c>typed</c>, its typed value: as a scalar when there is one, as an array when there are
    /// two, and not at all when there are none. One level and one service, so a flush leaves the
    /// rows in one segment, and the timestamps far enough back for a merge to take them.
    /// </summary>
    private static void Write(StorageEngine engine, int ordinal, string row, Kind kind, params string[] values)
    {
        var buf = new ArrayBufferWriter<byte>(64);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(values.Length == 0 ? 1 : 2);
        w.Write("Row"); w.Write(row);
        if (values.Length > 0)
        {
            w.Write("P");
            if (values.Length > 1) w.WriteArrayHeader(values.Length);
            foreach (string value in values)
            {
                if (value == "string") w.Write(kind.Spelling);
                else                   kind.Typed(ref w);
            }
        }
        w.Flush();

        Assert.True(engine.TryWrite(new LogEventHeader
        {
            Id                       = new EventId(0u, (uint)ordinal).RawValue,
            TimestampUtcTicks        = BaseTicks + ordinal * TimeSpan.TicksPerSecond,
            Level                    = LogLevel.Warning,
            MessageTemplatePoolIndex = engine.TemplatePool.Intern(Template),
            ServiceNamePoolIndex     = engine.TemplatePool.Intern("Ledger.API"),
        }, buf.WrittenSpan, Template));
    }

    /// <summary>
    /// Writes one Warning row for the header tests. A <c>header</c> or <c>both</c> row carries
    /// <see cref="HeaderSpelling"/> in the header field <paramref name="bucket"/> is filed from —
    /// the service, or an exception's type or inner type. A <c>client</c> or <c>both</c> row
    /// carries a property named <paramref name="bucket"/> holding <c>[5]</c>. Every other row has
    /// the service <c>Ledger.API</c> and no exception.
    /// </summary>
    private static void WriteHeaderRow(StorageEngine engine, int ordinal, string row, string bucket)
    {
        bool header = row is "header" or "both", client = row is "client" or "both";

        var buf = new ArrayBufferWriter<byte>(64);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(client ? 2 : 1);
        w.Write("Row"); w.Write(row);
        if (client)
        {
            w.Write(bucket);
            w.WriteArrayHeader(1);
            w.Write(5L);
        }
        w.Flush();

        string service = header && bucket == ClefFields.ServiceName ? HeaderSpelling : "Ledger.API";
        byte[] exception = !header || bucket == ClefFields.ServiceName ? []
            : bucket == ClefFields.ExceptionType
                ? new ExceptionInfo { Type = HeaderSpelling, Message = "gateway timeout" }.ToBytes()
                : new ExceptionInfo
                  {
                      Type    = "System.InvalidOperationException",
                      Message = "gateway timeout",
                      Inner   = new ExceptionInfo { Type = HeaderSpelling, Message = "disk" },
                  }.ToBytes();

        Assert.True(engine.TryWrite(new LogEventHeader
        {
            Id                       = new EventId(0u, (uint)ordinal).RawValue,
            TimestampUtcTicks        = BaseTicks + ordinal * TimeSpan.TicksPerSecond,
            Level                    = LogLevel.Warning,
            MessageTemplatePoolIndex = engine.TemplatePool.Intern(Template),
            ServiceNamePoolIndex     = engine.TemplatePool.Intern(service),
        }, buf.WrittenSpan, Template, exception));
    }
}
