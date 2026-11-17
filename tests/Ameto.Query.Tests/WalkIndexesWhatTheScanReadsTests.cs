using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Core.Serialization;
using Ameto.Indexing;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// The property walk that builds a flushed group's index (<c>SegmentIndexBuilder.WalkMap</c> /
/// <c>WalkValue</c> / <c>AddScalar</c>) has to file every value the way the per-event scan READS
/// it. The scan is what a filter is matched against, in the hot tier and after the index has let
/// a group through; the two gates in front of it — the bloom filter and the inverted and trigram
/// indexes — read "not filed" as "not there" and drop the group unread. So a value filed under
/// anything but the text the scan reads is a row the hot tier returns and the flushed segment
/// does not. Four shapes were filed otherwise:
///
/// <list type="number">
/// <item>A map key that is not a string ENDED the walk of its row: the walk read every key as a
/// string, a non-string one threw, and the throw was swallowed, so every property after that key
/// was never indexed. The scan's probe skips such a pair and reads on. (Its dictionary, which free
/// text and nested paths read, threw on the key and lost the whole map; it skips the pair now too,
/// which is why those filters find these rows hot at all.) The project's own Serilog sink writes
/// such a key — a <c>Dictionary&lt;int, …&gt;</c> property's keys go out as integers — and CLEF
/// ingest keeps the pair as it came.</item>
/// <item>A nil ELEMENT of an array, which the reader turns into the string <c>"&lt;null&gt;"</c>
/// (<c>LogEventSerializer.ReadArray</c>) — what the scan compares, what the events API shows, and
/// so what the UI's "filter by this value" builds a filter from (<c>Tags[1] = '&lt;null&gt;'</c>).
/// The walk filed a nil.</item>
/// <item>An extension element of an array (a msgpack timestamp, say), which the reader turns into
/// <c>"&lt;null&gt;"</c> the same way. The walk filed nothing.</item>
/// <item>A binary value, which the scan's text predicates read as <c>"System.Byte[]"</c> — the
/// text clients are shown for it. The walk filed nothing.</item>
/// </list>
///
/// <para>Each test writes rows to the hot tier, asks it every filter (asserting the row the filter
/// is about is among what it returns), flushes, checks that all the rows share ONE index group,
/// and asks again through the real <see cref="StorageEngine"/> and <see cref="QueryExecutor"/> —
/// without the index cache, and twice through it. The flushed answer must be the hot one: the
/// index may cost a query its acceleration, never its rows.</para>
/// </summary>
public sealed class WalkIndexesWhatTheScanReadsTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-walkscan-" + Guid.NewGuid().ToString("N"));
    private readonly List<StorageEngine> _engines = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var engine in _engines) { try { await engine.DisposeAsync(); } catch { } }
        QuerySegmentFixtures.DeleteDataDirectory(_dir);
    }

    public static TheoryData<string> ShapeNames => Names(Shapes.Keys);

    [Theory]
    [MemberData(nameof(ShapeNames))]
    public async Task EveryShape_IsFoundInTheFlushedSegment_AsInTheHotTier(string shape)
    {
        var s     = Shapes[shape];
        var store = Store(shape);
        for (int i = 0; i < s.Rows.Length; i++) Write(store.Engine, i, s.Rows[i]);

        var (hot, wrong) = await AskTheHotTierAsync(store, s.Asks);
        await store.Engine.FlushHotTierAsync();
        AssertOneGroupOf(store.Engine, s.Rows.Length);
        await AssertFlushedAsync(store, hot, s.Rows.Length, wrong);
    }

    // ── 1. Keys that are not strings, of every kind ───────────────────────────

    /// <summary>
    /// msgpack lets any value be a key. Whatever the key is, the pair is skipped and the walk
    /// carries on to the properties after it — top level, and one map down.
    /// </summary>
    public static TheoryData<string> KeyKinds => Names(NonStringKeys.Keys);

    private static TheoryData<string> Names(IEnumerable<string> names)
    {
        var data = new TheoryData<string>();
        foreach (string name in names) data.Add(name);
        return data;
    }

    [Theory]
    [MemberData(nameof(KeyKinds))]
    public async Task APairWhoseKeyIsNotAString_IsSkipped_AndThePropertiesAfterItAreFound(string keyKind)
    {
        var key   = NonStringKeys[keyKind];
        var store = Store("key-" + keyKind);
        Write(store.Engine, 0, new Row("subject", 4, (ref MessagePackWriter w) =>
        {
            key(ref w); w.Write("x");
            w.Write("P"); w.Write("needle");
            w.Write("Ctx"); w.WriteMapHeader(2); key(ref w); w.Write("y"); w.Write("Tenant"); w.Write("acme");
            w.Write("N"); w.Write(4242L);
        }));
        Write(store.Engine, 1, new Row("plain", 3, static (ref MessagePackWriter w) =>
        {
            w.Write("P"); w.Write("other");
            w.Write("Ctx"); w.WriteMapHeader(1); w.Write("Tenant"); w.Write("globex");
            w.Write("N"); w.Write(1L);
        }));

        List<(string Filter, string[] Premise)> asks =
        [
            ("P = 'needle'",          ["subject"]),
            ("contains(P, 'needl')",  ["subject"]),
            ("N = 4242",              ["subject"]),
            ("Ctx.Tenant = 'acme'",   ["subject"]),
            ("needle",                ["subject"]),
        ];

        var (hot, wrong) = await AskTheHotTierAsync(store, asks);
        await store.Engine.FlushHotTierAsync();
        AssertOneGroupOf(store.Engine, 2);
        await AssertFlushedAsync(store, hot, 2, wrong);
    }

    /// <summary>
    /// The way such a key really arrives: a CLEF batch on the wire, decoded by the ingest path,
    /// which keeps every pair that is not a header field exactly as it came — here a nested map
    /// with an integer key, as the Serilog sink writes a <c>Dictionary&lt;int, int&gt;</c>.
    /// </summary>
    [Fact]
    public async Task AnIntegerKeyFromTheWire_DoesNotHideThePropertiesAfterIt()
    {
        var store = Store("clef-wire");
        foreach (var (ordinal, row, r, s) in new[] { (0, "subject", "needle", 4242L), (1, "plain", "other", 1L) })
        {
            byte[] body = Pack((ref MessagePackWriter w) =>
            {
                w.WriteArrayHeader(1);
                w.WriteMapHeader(6);
                w.Write("@t");     w.Write("2026-08-30T09:00:00.0000000Z");
                w.Write("@mt");    w.Write(Template);
                w.Write("Row");    w.Write(row);
                w.Write("Counts"); w.WriteMapHeader(2); w.Write(500); w.Write(3); w.Write(404); w.Write(1);
                w.Write("R");      w.Write(r);
                w.Write("S");      w.Write(s);
            });
            uint seq = 1;
            var decoded = new List<LogEvent>();
            LogEventSerializer.DeserializeBatch(new ReadOnlySequence<byte>(body), 0u, ref seq, decoded);
            WriteRaw(store.Engine, ordinal, Assert.Single(decoded).RawProperties.ToArray());
        }

        List<(string Filter, string[] Premise)> asks =
        [
            ("R = 'needle'",         ["subject"]),
            ("S = 4242",             ["subject"]),
            ("R in ['needle']",      ["subject"]),
            ("contains(R, 'needl')", ["subject"]),
            ("needle",               ["subject"]),
        ];

        var (hot, wrong) = await AskTheHotTierAsync(store, asks);
        await store.Engine.FlushHotTierAsync();
        AssertOneGroupOf(store.Engine, 2);
        await AssertFlushedAsync(store, hot, 2, wrong);
    }

    // ── A merge ───────────────────────────────────────────────────────────────

    /// <summary>
    /// A merge indexes with the same walk, from the rows the segments kept — the client's bytes,
    /// untouched — so it is the other moment these shapes are filed, and the one that rebuilds a
    /// group written before this change. Every shape, flushed into two segments and merged into
    /// one group, answers as a twin store that was never flushed.
    /// </summary>
    [Fact]
    public async Task EveryShape_IsFoundAfterTheMergeThatPutsThemInOneGroup()
    {
        // Every row of every shape, named after its shape so that no two rows share a name.
        var rows = new List<Row>();
        var asks = new List<(string Filter, string[] Premise)>();
        foreach (var (name, shape) in Shapes)
        {
            foreach (var row in shape.Rows) rows.Add(row with { Name = $"{name}/{row.Name}" });
            foreach (var (filter, premise) in shape.Asks) asks.Add((filter, [.. premise.Select(p => $"{name}/{p}")]));
        }

        var truth = Store("merge-truth");
        for (int i = 0; i < rows.Count; i++) Write(truth.Engine, i, rows[i]);
        var (answers, wrong) = await AskTheHotTierAsync(truth, asks);

        var store = Store("merge");
        int half = rows.Count / 2;
        for (int i = 0; i < half; i++) Write(store.Engine, i, rows[i]);
        await store.Engine.FlushHotTierAsync();
        for (int i = half; i < rows.Count; i++) Write(store.Engine, i, rows[i]);
        await store.Engine.FlushHotTierAsync();

        Assert.Equal(2, store.Engine.ListSegments().Count);
        await AssertFlushedAsync(store, answers, rows.Count, wrong, "two segments, before the merge");

        while (await store.Engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None)) { }
        AssertOneGroupOf(store.Engine, rows.Count);   // or nothing was merged, and the test proves nothing
        await AssertFlushedAsync(store, answers, rows.Count, wrong, "one segment, after the merge");
    }

    // ── The shapes ────────────────────────────────────────────────────────────

    private delegate void PackPairs(ref MessagePackWriter writer);

    /// <summary>One row: its name (the <c>Row</c> property, written first) and the pairs after it.</summary>
    private sealed record Row(string Name, int Pairs, PackPairs Rest);

    /// <summary>
    /// Rows, and what to ask of them: each filter with the rows the hot tier must return for it —
    /// the premise that makes the flushed answer worth comparing. Empty for a control.
    /// </summary>
    private sealed record Shape(Row[] Rows, (string Filter, string[] Premise)[] Asks);

    /// <summary>A msgpack timestamp (fixext 4, type -1) and an ext 8 of three bytes, type 7.</summary>
    private static readonly byte[] TimestampExt = [0xD6, 0xFF, 0x00, 0x00, 0x00, 0x01];
    private static readonly byte[] Ext8         = [0xC7, 0x03, 0x07, 0x01, 0x02, 0x03];

    private static readonly Dictionary<string, Shape> Shapes = new()
    {
        // 1. A key that is not a string, at the top level and one map down.
        ["key-int"] = new(
        [
            new("subject", 3, static (ref MessagePackWriter w) => { w.Write(1); w.Write("x"); w.Write("P"); w.Write("needle"); w.Write("N"); w.Write(4242L); }),
            new("plain",   2, static (ref MessagePackWriter w) => { w.Write("P"); w.Write("other"); w.Write("N"); w.Write(1L); }),
        ],
        [
            ("P = 'needle'",          ["subject"]),
            ("N = 4242",              ["subject"]),
            ("P in ['needle']",       ["subject"]),
            ("contains(P, 'needl')",  ["subject"]),
            ("P like 'needl%'",       ["subject"]),
            ("startsWith(P, 'need')", ["subject"]),
            ("needle",                ["subject"]),
        ]),

        // The shape the project's Serilog sink writes for a Dictionary<int, int> property.
        ["key-int-nested"] = new(
        [
            new("subject", 4, static (ref MessagePackWriter w) =>
            {
                w.Write("Before"); w.Write("early");
                w.Write("Counts"); w.WriteMapHeader(1); w.Write(500); w.Write(3);
                w.Write("R");      w.Write("needle");
                w.Write("S");      w.Write(4242L);
            }),
            new("plain", 3, static (ref MessagePackWriter w) => { w.Write("Before"); w.Write("late"); w.Write("R"); w.Write("other"); w.Write("S"); w.Write(1L); }),
        ],
        [
            ("Before = 'early'",     ["subject"]),   // before the key: filed either way
            ("R = 'needle'",         ["subject"]),
            ("S = 4242",             ["subject"]),
            ("R in ['needle']",      ["subject"]),
            ("contains(R, 'needl')", ["subject"]),
            ("needle",               ["subject"]),
        ]),

        // 2. A nil element of an array: the string "<null>" to the scan and to every client.
        ["nil-element"] = new(
        [
            new("subject",    1, static (ref MessagePackWriter w) => { w.Write("Tags"); w.WriteArrayHeader(2); w.Write("a"); w.WriteNil(); }),
            new("scalar-nil", 1, static (ref MessagePackWriter w) => { w.Write("Tags"); w.WriteNil(); }),
            new("plain",      1, static (ref MessagePackWriter w) => { w.Write("Tags"); w.WriteArrayHeader(1); w.Write("b"); }),
        ],
        [
            ("Tags[1] = '<null>'",        ["subject"]),   // what the UI's "filter by this value" builds
            ("Tags = '<null>'",           ["subject"]),
            ("Tags = '<NULL>'",           ["subject"]),
            ("Tags in ['<null>']",        ["subject"]),
            ("contains(Tags, 'null')",    ["subject"]),
            ("contains(Tags[1], 'null')", ["subject"]),
            ("Tags like '%null%'",        ["subject"]),
            ("startsWith(Tags, '<nu')",   ["subject"]),
            ("'<null>'",                  ["subject"]),
            ("Tags = 'a'",                ["subject"]),
            ("Tags = null",               ["scalar-nil"]),   // never hinted; a control
        ]),

        // ... in an array one map down, and in an array of arrays.
        ["nil-element-nested"] = new(
        [
            new("subject", 1, static (ref MessagePackWriter w) => { w.Write("Ctx"); w.WriteMapHeader(1); w.Write("Tags"); w.WriteArrayHeader(1); w.WriteNil(); }),
            new("deep",    1, static (ref MessagePackWriter w) => { w.Write("M"); w.WriteArrayHeader(1); w.WriteArrayHeader(2); w.WriteNil(); w.Write("z"); }),
            new("plain",   1, static (ref MessagePackWriter w) => { w.Write("Other"); w.Write("x"); }),
        ],
        [
            ("Ctx.Tags = '<null>'",           ["subject"]),
            ("Ctx.Tags[0] = '<null>'",        ["subject"]),
            ("contains(Ctx.Tags, '<null>')",  ["subject"]),
            ("'<null>'",                      ["subject", "deep"]),
        ]),

        // 3. An extension element of an array: "<null>" to the scan as well; and one as a map
        //    value, which the scan reads as a nil, with a property after it.
        ["ext-element"] = new(
        [
            new("subject",   1, static (ref MessagePackWriter w) => { w.Write("Tags"); w.WriteArrayHeader(1); w.WriteRaw(TimestampExt); }),
            new("ext8",      1, static (ref MessagePackWriter w) => { w.Write("Marks"); w.WriteArrayHeader(2); w.Write("m"); w.WriteRaw(Ext8); }),
            new("ext-value", 2, static (ref MessagePackWriter w) => { w.Write("Stamp"); w.WriteRaw(TimestampExt); w.Write("After"); w.Write("later"); }),
            new("plain",     1, static (ref MessagePackWriter w) => { w.Write("Tags"); w.WriteArrayHeader(1); w.Write("b"); }),
        ],
        [
            ("Tags = '<null>'",        ["subject"]),
            ("Tags[0] = '<null>'",     ["subject"]),
            ("Tags in ['<null>']",     ["subject"]),
            ("contains(Tags, 'null')", ["subject"]),
            ("Marks[1] = '<null>'",    ["ext8"]),
            ("'<null>'",               ["subject", "ext8"]),
            ("After = 'later'",        ["ext-value"]),
        ]),

        // 4. A binary value: "System.Byte[]" to the scan's text predicates and to every client.
        ["binary"] = new(
        [
            new("subject",  1, static (ref MessagePackWriter w) => { w.Write("Bin"); w.Write((byte[])[1, 2, 3]); }),
            new("in-array", 1, static (ref MessagePackWriter w) => { w.Write("Bins"); w.WriteArrayHeader(2); w.Write("x"); w.Write((byte[])[4]); }),
            new("plain",    2, static (ref MessagePackWriter w) => { w.Write("Bin"); w.Write("other"); w.Write("Bins"); w.WriteArrayHeader(1); w.Write("y"); }),
        ],
        [
            ("contains(Bin, 'Byte')",     ["subject"]),
            ("Bin like 'System.Byte%'",   ["subject"]),
            ("startsWith(Bin, 'System')", ["subject"]),
            ("Bin in ['System.Byte[]']",  ["subject"]),
            ("contains(Bins, 'Byte')",    ["in-array"]),
            ("Bins like '%byte[]'",       ["in-array"]),
            ("Bin = 'System.Byte[]'",     []),            // `=` does not read a byte[] as text; a control
        ]),
    };

    /// <summary>Keys that are not strings, one of each msgpack kind.</summary>
    private static readonly Dictionary<string, PackPairs> NonStringKeys = new()
    {
        ["int"]     = static (ref MessagePackWriter w) => w.Write(7),
        ["negative"] = static (ref MessagePackWriter w) => w.Write(-40),
        ["uint64"]  = static (ref MessagePackWriter w) => w.Write(ulong.MaxValue),
        ["double"]  = static (ref MessagePackWriter w) => w.Write(2.5d),
        ["float"]   = static (ref MessagePackWriter w) => w.Write(1.5f),
        ["bool"]    = static (ref MessagePackWriter w) => w.Write(true),
        ["nil"]     = static (ref MessagePackWriter w) => w.WriteNil(),
        ["binary"]  = static (ref MessagePackWriter w) => w.Write((byte[])[9, 9]),
        ["array"]   = static (ref MessagePackWriter w) => { w.WriteArrayHeader(2); w.Write(1); w.Write("k"); },
        ["map"]     = static (ref MessagePackWriter w) => { w.WriteMapHeader(1); w.Write("k"); w.Write(1); },
        ["ext"]     = static (ref MessagePackWriter w) => w.WriteRaw(TimestampExt),
    };

    // ── Assertions ────────────────────────────────────────────────────────────

    private sealed record TestStore(StorageEngine Engine, QueryExecutor Query);

    /// <summary>
    /// Asks the hot tier every filter and hands back what it returned — the answer the flushed
    /// segment has to give — with every premise it broke: a row a filter is about and the hot tier
    /// did not return. Those are reported with the flushed differences, not on their own, so that
    /// one run shows everything.
    /// </summary>
    private static async Task<(List<(string Filter, string[] Rows)> Answers, List<string> Wrong)> AskTheHotTierAsync(
        TestStore store, IEnumerable<(string Filter, string[] Premise)> asks)
    {
        Assert.Empty(store.Engine.ListSegments());
        var answers = new List<(string, string[])>();
        var wrong   = new List<string>();
        foreach (var (filter, premise) in asks)
        {
            string[] rows = await RowsAsync(store.Query, filter);
            foreach (string row in premise)
                if (!rows.Contains(row))
                    wrong.Add($"hot tier, `{filter}`: the premise is that it returns {row}, and it returned [{string.Join(", ", rows)}]");
            answers.Add((filter, rows));
        }
        return (answers, wrong);
    }

    /// <summary>
    /// Asks every filter of the flushed store: uncached, then twice through the index cache,
    /// whose second answer comes from each group's memo. Every disagreement is reported at once,
    /// after the hot tier's broken premises (<paramref name="hotWrong"/>).
    /// </summary>
    private static async Task AssertFlushedAsync(
        TestStore store, List<(string Filter, string[] Rows)> asks, int rows, List<string> hotWrong, string state = "flushed")
    {
        using var cache = new SegmentIndexCache(16L * 1024 * 1024);
        var cached = new QueryExecutor(store.Engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance, cache);

        var wrong = new List<string>(hotWrong);
        foreach (var (filter, expected) in asks)
        {
            await CompareAsync(store.Query, filter, expected, state, wrong);
            await CompareAsync(cached, filter, expected, state + ", through the index cache", wrong);
            await CompareAsync(cached, filter, expected, state + ", from the cache's memo", wrong);
        }
        Assert.True(wrong.Count == 0, string.Join(Environment.NewLine, wrong));

        // And the store is all there for a query that asks the index nothing.
        Assert.Equal(rows, (await RowsAsync(store.Query, null)).Length);
    }

    private static async Task CompareAsync(QueryExecutor query, string filter, string[] expected, string state, List<string> wrong)
    {
        string[] rows = await RowsAsync(query, filter);
        if (!expected.AsSpan().SequenceEqual(rows))
            wrong.Add($"{state}, `{filter}`: expected [{string.Join(", ", expected)}], got [{string.Join(", ", rows)}]");
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
    /// The <c>Row</c> of every event a filter returns, oldest first — the order written. Read
    /// through the probe, which reads a row's top-level string whatever else the map holds.
    /// </summary>
    private static async Task<string[]> RowsAsync(QueryExecutor query, string? filter)
    {
        var events = await QuerySegmentFixtures.RunAsync(query, filter, int.MaxValue, forward: true);
        return [.. events.Select(static ev => ev.TryGetProperty("Row", out object? row) ? (string)row! : "?")];
    }

    // ── Fixture ───────────────────────────────────────────────────────────────

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

    private static byte[] Pack(PackPairs pack)
    {
        var buf = new ArrayBufferWriter<byte>(64);
        var w   = new MessagePackWriter(buf);
        pack(ref w);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>Writes <paramref name="row"/> as <c>{Row: name, …its pairs}</c>.</summary>
    private static void Write(StorageEngine engine, int ordinal, Row row) => WriteRaw(engine, ordinal, Pack((ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(1 + row.Pairs);
        w.Write("Row"); w.Write(row.Name);
        row.Rest(ref w);
    }));

    /// <summary>
    /// Writes one Warning row, a second after the one before it: one level and one service, so a
    /// flush leaves the rows in one segment, and the timestamps far enough back for a merge.
    /// </summary>
    private static void WriteRaw(StorageEngine engine, int ordinal, byte[] properties)
    {
        Assert.True(engine.TryWrite(new LogEventHeader
        {
            Id                       = new EventId(0u, (uint)ordinal).RawValue,
            TimestampUtcTicks        = BaseTicks + ordinal * TimeSpan.TicksPerSecond,
            Level                    = LogLevel.Warning,
            MessageTemplatePoolIndex = engine.TemplatePool.Intern(Template),
            ServiceNamePoolIndex     = engine.TemplatePool.Intern("Ledger.API"),
        }, properties, Template), $"TryWrite refused row {ordinal}");
    }
}
