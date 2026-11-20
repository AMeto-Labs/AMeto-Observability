using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query.Filtering;
using Ameto.Storage;
using Xunit.Abstractions;

namespace Ameto.Query.Tests;

/// <summary>
/// CASE FOLDING MAY COST A QUERY ITS ACCELERATION, NEVER ITS ROWS. The scan compares strings
/// <see cref="StringComparison.OrdinalIgnoreCase"/> (<c>=</c>, <c>in</c>, free text,
/// <c>ci</c> functions) and lower-case char by char (<c>like</c>); the segment's bloom filter and
/// trigram index used to fold with <c>ToLowerInvariant</c>, which keeps apart characters both of
/// those call equal — <c>ς</c>/<c>σ</c>, <c>µ</c>/<c>μ</c>, the archaic Cyrillic <c>ᲀ</c> against
/// <c>в</c>, and on ICU hosts every astral case pair ICU does not know (Garay). A row the hot tier
/// returned for <c>Place = 'ΟΔΟΣ'</c> was gone the moment it was flushed.
///
/// <para>Every filter here is asked of the hot tier and of the flushed segment through the real
/// <see cref="StorageEngine"/> and <see cref="QueryExecutor"/>, with and without the index cache,
/// and the answers must agree with each other and with the expectation spelled next to the
/// filter — so a hot tier that stopped matching cannot make the comparison vacuous. The same
/// corpus is also served from a segment the PREVIOUS build wrote (<see cref="LegacyFoldSegment"/>),
/// whose sections are folded the old way and say so by not saying otherwise.</para>
/// </summary>
public sealed class CaseFoldTierParityTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _out;
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-casefold-" + Guid.NewGuid().ToString("N"));
    private readonly List<StorageEngine> _engines = [];

    public CaseFoldTierParityTests(ITestOutputHelper output) => _out = output;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var engine in _engines) { try { await engine.DisposeAsync(); } catch { /* best effort */ } }
        QuerySegmentFixtures.DeleteDataDirectory(_dir);
    }

    // ── The corpus ────────────────────────────────────────────────────────────
    //
    // FROZEN: LegacyFoldSegment holds these rows as the previous build flushed them. A change here
    // is a change to what that segment is compared against, and the comparison would fail on the
    // corpus, not on the index.

    internal delegate void Props(ref MessagePackWriter w);

    internal sealed record Row(string Name, int PropCount, Props Write,
                               string Template = "settle {Row}", string Service = "Ledger.API",
                               ExceptionInfo? Exception = null);

    internal static readonly Row[] Corpus =
    [
        new("gr",        1, static (ref MessagePackWriter w) => { w.Write("Place");  w.Write("οδος"); }),            // final ς
        new("gr-other",  1, static (ref MessagePackWriter w) => { w.Write("Place");  w.Write("αθηνα"); }),
        new("micro",     1, static (ref MessagePackWriter w) => { w.Write("Unit");   w.Write("µs"); }),         // MICRO SIGN
        new("milli",     1, static (ref MessagePackWriter w) => { w.Write("Unit");   w.Write("ms"); }),
        new("logos",     0, static (ref MessagePackWriter w) => { }, Template: "λογος {Row}"),                      // template
        new("account",   0, static (ref MessagePackWriter w) => { }, Service: "Λογαριασμος"),                       // service
        new("odos-ex",   0, static (ref MessagePackWriter w) => { },
            Exception: new ExceptionInfo { Type = "System.Οδος.Exception", Message = "νομος violated", StackTrace = "   at Ledger.Settle()" }),
        new("ve",        1, static (ref MessagePackWriter w) => { w.Write("Word");   w.Write("ᲀода"); }),       // ROUNDED VE → В
        new("garay",     1, static (ref MessagePackWriter w) => { w.Write("Script"); w.Write("\U00010D50\U00010D51\U00010D52"); }),
        new("kelvin",    1, static (ref MessagePackWriter w) => { w.Write("Temp");   w.Write("Kelvin"); }),     // KELVIN SIGN
        new("strasse",   1, static (ref MessagePackWriter w) => { w.Write("Street"); w.Write("STRAẞE"); }),     // CAPITAL SHARP S
        new("cyr",       1, static (ref MessagePackWriter w) => { w.Write("Msg");    w.Write("Ошибка платежа"); }),
        new("ascii",     1, static (ref MessagePackWriter w) => { w.Write("Msg");    w.Write("Connection refused"); }),
        new("emoji",     1, static (ref MessagePackWriter w) => { w.Write("Mood");   w.Write("\U0001F525 hot"); }),
    ];

    /// <summary>
    /// The filters and the rows the SCAN returns for them. What each one reaches in the index is
    /// noted; "bloom" is the phase-1 gate on an equality hint, "trigram" the substring hint.
    /// </summary>
    public static TheoryData<string, string[]> Filters() => new()
    {
        // OrdinalIgnoreCase equates ς and σ (both Σ); lowercase keeps them apart.
        { "Place = 'ΟΔΟΣ'",                        ["gr"] },               // bloom, property value
        { "Place in ['ΟΔΟΣ']",                     ["gr"] },
        { "ΟΔΟΣ",                                  ["gr", "odos-ex"] },    // trigram: value, exception type
        { "ΛΟΓΟΣ",                                 ["logos"] },            // trigram: template
        { "ΝΟΜΟΣ",                                 ["odos-ex"] },          // trigram: exception message
        { "@x.type = 'SYSTEM.ΟΔΟΣ.EXCEPTION'",     ["odos-ex"] },          // bloom: exception type
        { "@service = 'ΛΟΓΑΡΙΑΣΜΟΣ'",              ["account"] },          // bloom: service
        // µ (U+00B5) uppercases to Μ, as μ (U+03BC) does.
        { "Unit = 'μs'",                           ["micro"] },
        { "Unit in ['μs']",                        ["micro"] },
        // ᲀ (U+1C80) uppercases to В; lowercase leaves it alone.
        { "Word = 'ВОДА'",                         ["ve"] },
        { "ВОДА",                                  ["ve"] },
        // Garay (Unicode 16): .NET's comparer folds the pairs, ICU on a Windows host does not.
        { "Script = '\U00010D70\U00010D71\U00010D72'", ["garay"] },
        // LIKE lowercases: the Kelvin sign is k, the capital sharp s is ß. OrdinalIgnoreCase
        // says neither, so the equality next to each is empty in both tiers.
        { "Temp like '%kelvin%'",                  ["kelvin"] },
        { "Temp = 'kelvin'",                       [] },
        { "Street like 'straße'",                  ["strasse"] },
        { "Street = 'straße'",                     [] },
        // Ordinary text, which the old fold got right.
        { "Msg = 'ОШИБКА ПЛАТЕЖА'",                ["cyr"] },
        { "ПЛАТЕЖА",                               ["cyr"] },
        { "Msg = 'connection REFUSED'",            ["ascii"] },
        { "REFUSED",                               ["ascii"] },
        { "Mood = '\U0001F525 HOT'",               ["emoji"] },
        // Absent values: no rows anywhere.
        { "Place = 'ΑΘΗΝΑΣ'",                      [] },
        { "ΑΘΗΝΑΣ",                                [] },
        { "Msg = 'nothing here'",                  [] },
        { "nothing",                               [] },
    };

    // ── A segment this build flushes ──────────────────────────────────────────

    [Theory]
    [MemberData(nameof(Filters))]
    public async Task The_hot_tier_and_a_flushed_segment_return_the_same_rows(string filter, string[] expected)
    {
        var (engine, query) = Store("new");
        WriteCorpus(engine);
        Assert.Empty(engine.ListSegments());

        string[] hot = await RowsAsync(query, filter);

        await engine.FlushHotTierAsync();
        var segment = Assert.Single(engine.ListSegments());
        using (var reader = SegmentReader.Open(segment.FilePath))
            Assert.Equal(1, reader.Groups.Length);

        string[] cold = await RowsAsync(query, filter);
        using var cache = new SegmentIndexCache(16L * 1024 * 1024);
        var cached = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance, cache);
        string[] coldCached = await RowsAsync(cached, filter);

        _out.WriteLine($"{filter}: hot [{string.Join(", ", hot)}], flushed [{string.Join(", ", cold)}], cached [{string.Join(", ", coldCached)}]");
        Assert.Equal(expected, hot);
        Assert.Equal(hot, cold);
        Assert.Equal(hot, coldCached);
    }

    // ── A segment the previous build flushed ──────────────────────────────────

    /// <summary>
    /// THE SEGMENT THE PREVIOUS BUILD WROTE. <see cref="Corpus"/>, written through
    /// <see cref="WriteCorpus"/> into a <see cref="StorageEngine"/> with the production index
    /// wiring (<c>new SegmentIndexBuilder(events, 5, terms)</c>) and flushed with
    /// <see cref="StorageEngine.FlushHotTierAsync"/> — by <c>main</c> at <c>bc182d60</c>, a build whose
    /// bloom and trigram sections fold with <c>ToLowerInvariant</c> (as every build before the
    /// canonical fold does), on a Windows host with ICU. Byte-identical over two runs; SHA-256
    /// <c>738556e4f97372c069032739f2472d4e715eaebf47ad64e027131b643a4e1b23</c>. One index group of
    /// fourteen events. Nothing in this build can write it again: that is what makes it the old
    /// format rather than a description of it.
    /// </summary>
    internal const string LegacyFoldSegment = "0-4-639236772000000000-639236772130000000.seg";

    internal static string LegacyFoldSegmentSource =>
        Path.Combine(AppContext.BaseDirectory, "Fixtures", "LegacyFoldSegment", LegacyFoldSegment);

    [Theory]
    [MemberData(nameof(Filters))]
    public async Task A_segment_the_previous_build_flushed_returns_the_rows_the_hot_tier_does(string filter, string[] expected)
    {
        var (hotEngine, hotQuery) = Store("hot");
        WriteCorpus(hotEngine);
        string[] hot = await RowsAsync(hotQuery, filter);

        var (engine, query) = await StoreOverLegacySegmentAsync("legacy");
        string[] cold = await RowsAsync(query, filter);
        using var cache = new SegmentIndexCache(16L * 1024 * 1024);
        var cached = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance, cache);
        string[] coldCached = await RowsAsync(cached, filter);

        _out.WriteLine($"{filter}: hot [{string.Join(", ", hot)}], old segment [{string.Join(", ", cold)}], cached [{string.Join(", ", coldCached)}]");
        Assert.Equal(expected, hot);
        Assert.Equal(hot, cold);
        Assert.Equal(hot, coldCached);
    }

    // ── What each kind of segment still prunes ────────────────────────────────

    /// <summary>
    /// A literal the group does not hold, asked of the executor's own decisions
    /// (<see cref="QueryExecutor.PassesBloomGate"/>, then <see cref="QueryExecutor.TryNarrowWithIndex(CompiledFilter, ISegmentIndex, out uint[])"/>)
    /// over the group's real sections. "bloom" — phase 1 dropped the group; "inverted" — phase 2
    /// did; "trigram" — the substring hint did; "scan" — nothing could, so every block of the group
    /// is read. A present literal must narrow to its row and nothing may drop it.
    /// </summary>
    public static TheoryData<string, string, string> Pruning() => new()
    {
        // A segment this build writes proves absence in its cheapest section, whatever the script.
        { "new",    "Place = 'ΑΘΗΝΑΣ'",      "bloom" },
        { "new",    "ΑΘΗΝΑΣ",                "trigram" },
        { "new",    "Msg = 'nothing here'",  "bloom" },
        { "new",    "nothing",               "trigram" },
        // An old segment keeps its pruning for ASCII literals...
        { "legacy", "Msg = 'nothing here'",  "bloom" },
        { "legacy", "nothing",               "trigram" },
        // ...and loses it in the bloom and the trigram for anything else, because the old fold of
        // a non-ASCII value was the WRITER's lowercase, which nobody recorded and which is not the
        // comparer's equivalence anyway. Equality still prunes one phase later, in the inverted
        // index; a substring with no ASCII window is answered by reading the group.
        { "legacy", "Place = 'ΑΘΗΝΑΣ'",      "inverted" },
        { "legacy", "ΑΘΗΝΑΣ",                "scan" },
    };

    [Theory]
    [MemberData(nameof(Pruning))]
    public async Task An_absent_literal_is_pruned_where_the_section_can_prove_it(string kind, string filter, string expected)
    {
        string path = await SegmentPathAsync(kind);
        using var segment = SegmentReader.Open(path);
        Assert.Equal(1, segment.Groups.Length);

        var compiled = CompiledFilter.Compile(filter);
        using var view = SegmentIndexView.Open(null, path, 0, segment);
        string verdict =
              compiled.TryGetIndexHint(out _, out _) && !QueryExecutor.PassesBloomGate(compiled, view) ? "bloom"
            : !QueryExecutor.TryNarrowWithIndex(compiled, view, out uint[]? candidates)
                ? (compiled.GetTrigramHints().Count > 0 ? "trigram" : "inverted")
            : candidates is null ? "scan"
            : $"narrowed to [{string.Join(", ", candidates)}]";

        _out.WriteLine($"{kind} {filter}: {verdict}");
        Assert.Equal(expected, verdict);
    }

    /// <summary>
    /// A literal the group holds narrows to its row in either kind of segment — on an old one
    /// through the inverted index (equality) or through the term's ASCII windows (substring).
    /// </summary>
    [Theory]
    [InlineData("new",    "Place = 'ΟΔΟΣ'",          "gr")]
    [InlineData("new",    "ΟΔΟΣ",                    "gr,odos-ex")]
    [InlineData("new",    "Unit = 'μs'",             "micro")]
    [InlineData("new",    "Msg = 'connection REFUSED'", "ascii")]
    [InlineData("legacy", "Place = 'ΟΔΟΣ'",          "gr")]
    [InlineData("legacy", "Unit = 'μs'",             "micro")]
    [InlineData("legacy", "Msg = 'connection REFUSED'", "ascii")]
    [InlineData("legacy", "REFUSED",                 "ascii")]
    [InlineData("legacy", "ΟΔΟΣ.EXCEPTION",          "odos-ex")]   // the ASCII windows of a mixed term
    public async Task A_present_literal_narrows_to_its_rows(string kind, string filter, string rows)
    {
        string path = await SegmentPathAsync(kind);
        using var segment = SegmentReader.Open(path);

        var compiled = CompiledFilter.Compile(filter);
        using var view = SegmentIndexView.Open(null, path, 0, segment);
        if (compiled.TryGetIndexHint(out _, out _))
            Assert.True(QueryExecutor.PassesBloomGate(compiled, view), "the bloom gate dropped a group holding the value");
        Assert.True(QueryExecutor.TryNarrowWithIndex(compiled, view, out uint[]? candidates), "the index dropped a group holding the value");

        uint[] expected = [.. rows.Split(',').Select(static r => (uint)Array.FindIndex(Corpus, c => c.Name == r))];
        Assert.Equal(expected, candidates);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>The one segment of a store holding <see cref="Corpus"/>: flushed by this build,
    /// or the previous build's (<see cref="LegacyFoldSegment"/>).</summary>
    private async Task<string> SegmentPathAsync(string kind)
    {
        if (kind == "legacy")
        {
            var (legacy, _) = await StoreOverLegacySegmentAsync("legacy-" + Guid.NewGuid().ToString("N")[..8]);
            return Assert.Single(legacy.ListSegments()).FilePath;
        }
        var (engine, _) = Store("new-" + Guid.NewGuid().ToString("N")[..8]);
        WriteCorpus(engine);
        await engine.FlushHotTierAsync();
        return Assert.Single(engine.ListSegments()).FilePath;
    }

    /// <summary>A store whose data directory holds <see cref="LegacyFoldSegment"/> when it starts,
    /// as an upgraded node's would.</summary>
    private async Task<(StorageEngine Engine, QueryExecutor Query)> StoreOverLegacySegmentAsync(string name)
    {
        string segments = Path.Combine(_dir, name, "segments");
        Directory.CreateDirectory(segments);
        File.Copy(LegacyFoldSegmentSource, Path.Combine(segments, LegacyFoldSegment));

        var (engine, query) = Store(name);
        await engine.CatalogLoaded.WaitAsync(TimeSpan.FromSeconds(60));
        var info = Assert.Single(engine.ListSegments());
        using var reader = SegmentReader.Open(info.FilePath);
        Assert.Equal(1, reader.Groups.Length);
        Assert.Equal((uint)Corpus.Length, reader.Groups[0].EventCount);
        return (engine, query);
    }

    /// <summary>
    /// The fixture is what its comment says: the bytes the previous build wrote, and sections that
    /// carry the old fold — a regenerated one would test the new path twice and the old one never.
    /// </summary>
    [Fact]
    public void The_previous_builds_segment_is_the_old_format()
    {
        byte[] file = File.ReadAllBytes(LegacyFoldSegmentSource);
        Assert.Equal("738556e4f97372c069032739f2472d4e715eaebf47ad64e027131b643a4e1b23",
                     Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(file)));

        using var reader = SegmentReader.Open(LegacyFoldSegmentSource);
        using var bloom  = SegmentBloomFilter.Deserialise(reader.ReadBloomFilterBytes(0));
        Assert.Equal(SegmentBloomFilter.TextFold.LowerInvariant, bloom.Fold);
        var trigram = SegmentTrigramIndex.ReadPackedHeader(reader.ReadTrigramIndexBytes(0));
        Assert.Equal(SegmentTrigramIndex.TrigramFold.Lower, trigram.Fold);
        Assert.True(trigram.Count > 0);
    }

    private (StorageEngine Engine, QueryExecutor Query) Store(string name)
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
        return (engine, new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance));
    }

    internal static readonly long BaseTicks = new DateTimeOffset(2026, 8, 30, 9, 0, 0, TimeSpan.Zero).UtcTicks;

    internal static void WriteCorpus(StorageEngine engine)
    {
        for (int i = 0; i < Corpus.Length; i++)
        {
            var row = Corpus[i];
            var buf = new ArrayBufferWriter<byte>(128);
            var w   = new MessagePackWriter(buf);
            w.WriteMapHeader(1 + row.PropCount);
            w.Write("Row"); w.Write(row.Name);
            row.Write(ref w);
            w.Flush();

            Assert.True(engine.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = BaseTicks + i * TimeSpan.TicksPerSecond,
                Level                    = LogLevel.Warning,
                MessageTemplatePoolIndex = engine.TemplatePool.Intern(row.Template),
                ServiceNamePoolIndex     = engine.TemplatePool.Intern(row.Service),
            }, buf.WrittenSpan, row.Template, row.Exception?.ToBytes() ?? default));
        }
    }

    internal static async Task<string[]> RowsAsync(QueryExecutor query, string filter)
    {
        var events = await QuerySegmentFixtures.RunAsync(query, filter, int.MaxValue, forward: true);
        return [.. events.Select(static ev => (string)ev.Properties!["Row"]!)];
    }
}
