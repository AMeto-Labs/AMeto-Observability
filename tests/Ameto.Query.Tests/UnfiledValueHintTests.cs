using System.Buffers;
using MessagePack;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query.Filtering;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// Filters that handed the segment index a hint for something its builder never filed. The hot
/// tier, which consults no index, returned the row; the moment the row was flushed the index
/// answered "not here" and the row was gone. Three ways in:
/// <list type="bullet">
///   <item>THE EMPTY VALUE OF A HEADER FIELD. <c>SegmentIndexBuilder</c> files the template, the
///   service and the exception types only when they are not empty, while the scan matches
///   <c>@mt = ''</c> against an event that has no template — the filter the client's "find all
///   like this" builds for such an event.</item>
///   <item>THE TEMPLATE UNDER ITS OWN SPELLING. It has no inverted bucket, and its hint went out
///   keyed by the filter's spelling (<c>Message</c>, <c>@mt</c>, …): in a group where a client
///   property of that name exists, that property's bucket answered for the template.</item>
///   <item>FLATTEN DEPTH. The builder flattens nested properties to a depth; a substring hint
///   on a deeper path, and — with <c>Indexing.MaxPropertyFlattenDepth</c> set lower — any hint on
///   a nested path or free text, asked the index for values it never held.</item>
/// </list>
///
/// <para>Each engine-level test writes rows into a real <see cref="StorageEngine"/>, asks the hot
/// tier, flushes, and asks again through <see cref="QueryExecutor"/> — without a cache, and twice
/// through a <see cref="SegmentIndexCache"/> (the miss that fills the memo, then the memo). The
/// invariant: the flushed store returns exactly what the hot tier did.</para>
/// </summary>
public sealed class UnfiledValueHintTests : IAsyncLifetime
{
    private const string Template = "settle {Row}";
    private const string Service  = "Ledger.API";

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-unfiled-" + Guid.NewGuid().ToString("N"));
    private readonly List<StorageEngine> _engines = [];
    private readonly List<IAsyncDisposable> _disposables = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var d in _disposables) { try { await d.DisposeAsync(); } catch { } }
        foreach (var engine in _engines) { try { await engine.DisposeAsync(); } catch { } }
        QuerySegmentFixtures.DeleteDataDirectory(_dir);
    }

    // ── 1. The empty value of a header field ──────────────────────────────────

    private static readonly byte[] Boom =
        new ExceptionInfo { Type = "System.InvalidOperationException", Message = "gateway timeout" }.ToBytes();

    [Theory]
    [InlineData("@mt = ''")]                         // what findLikeFilter builds for a template-less event
    [InlineData("MessageTemplate = ''")]
    [InlineData("@m = ''")]
    [InlineData("Message = ''")]
    [InlineData("@mt in ['']")]
    [InlineData("@mt = '' and @l = 'Warning'")]     // a conjunct the index can answer still narrows
    public Task EmptyTemplate(string filter) => AssertTiersAgreeAsync(filter, ["blank"],
    [
        new Row("blank", Template: ""),
        new Row("other"),
    ]);

    /// <summary>
    /// The service has no empty value to lose: ingest interns a service name only when it is not
    /// empty, so an event logged without one carries none — null in the hot tier, and a
    /// zero-length column, read back as null, in a segment — and <c>@service = ''</c> matches it
    /// in neither tier. (The review's probe interned <c>""</c> as a service directly, which made
    /// the hot tier answer <c>""</c>; no ingest path does that.) Its hint goes all the same — no
    /// header field files an empty value — so this pins only that the tiers agree.
    /// </summary>
    [Theory]
    [InlineData("@service = ''")]
    [InlineData("ServiceName = ''")]
    [InlineData("service.name = ''")]
    [InlineData("['service.name'] = ''")]
    [InlineData("@service in ['']")]
    public Task EmptyService(string filter) => AssertTiersAgreeAsync(filter, [],
    [
        new Row("none", Service: null),
        new Row("named"),
    ]);

    [Theory]
    [InlineData("@x.type = ''")]
    [InlineData("@x = ''")]
    [InlineData("Exception = ''")]
    [InlineData("Exception.Type = ''")]
    [InlineData("['@x.type'] = ''")]
    [InlineData("@x.type in ['']")]
    public Task EmptyExceptionType(string filter) => AssertTiersAgreeAsync(filter, ["blank"],
    [
        new Row("blank", Exception: new ExceptionInfo { Type = "", Message = "gateway timeout" }.ToBytes()),
        new Row("typed", Exception: Boom),
    ]);

    [Theory]
    [InlineData("@x.inner.type = ''")]
    [InlineData("Exception.Inner.Type = ''")]
    public Task EmptyInnerExceptionType(string filter) => AssertTiersAgreeAsync(filter, ["blank"],
    [
        new Row("blank", Exception: new ExceptionInfo
        {
            Type = "System.Net.Http.HttpRequestException", Message = "outer",
            Inner = new ExceptionInfo { Type = "", Message = "inner" },
        }.ToBytes()),
        new Row("typed", Exception: new ExceptionInfo
        {
            Type = "System.Net.Http.HttpRequestException", Message = "outer",
            Inner = new ExceptionInfo { Type = "System.IO.IOException", Message = "inner" },
        }.ToBytes()),
    ]);

    /// <summary>
    /// The case that always worked, and why it did: a nil property gives the bloom the empty
    /// text, so the group passed the gate for an empty template by luck.
    /// </summary>
    [Fact]
    public Task EmptyTemplate_BesideANilPropertyValue() => AssertTiersAgreeAsync("@mt = ''", ["blank"],
    [
        new Row("blank", Template: ""),
        new Row("other", Props: static (ref MessagePackWriter w) => { w.Write("Note"); w.WriteNil(); }),
    ]);

    /// <summary>A client property's empty string IS filed, so its hint stays and still answers.</summary>
    [Fact]
    public Task EmptyUserPropertyValue() => AssertTiersAgreeAsync("Note = ''", ["empty"],
    [
        new Row("empty", Props: static (ref MessagePackWriter w) => { w.Write("Note"); w.Write(""); }),
        new Row("full",  Props: static (ref MessagePackWriter w) => { w.Write("Note"); w.Write("x"); }),
        new Row("none"),
    ]);

    [Theory]
    [InlineData("@mt = ''")]
    [InlineData("@m = ''")]
    [InlineData("@service = ''")]
    [InlineData("@x.type = ''")]
    [InlineData("@x = ''")]
    [InlineData("@x.inner.type = ''")]
    [InlineData("@l = ''")]
    [InlineData("@tr = ''")]
    [InlineData("@sp = ''")]
    [InlineData("@mt in ['']")]
    [InlineData("@service in ['']")]
    public void EmptyLiteralOnABuiltIn_AsksTheIndexNothing(string filter)
    {
        var compiled = CompiledFilter.Compile(filter);
        Assert.False(compiled.TryGetIndexHint(out _, out _));
        Assert.Empty(compiled.GetInvertedHints());
    }

    [Fact]
    public void EmptyLiteral_TheOtherConjunctStillHints()
    {
        var compiled = CompiledFilter.Compile("@mt = '' and @service = 'Auth.API'");
        Assert.True(compiled.TryGetIndexHint(out string key, out object? value));
        Assert.Equal((ClefFields.ServiceName, "Auth.API"), (key, (string?)value));
        Assert.Equal([(ClefFields.ServiceName, (object?)"Auth.API")], compiled.GetInvertedHints());
    }

    [Fact]
    public void NonEmptyLiteralOnABuiltIn_AndEmptyOnAUserProperty_StillHint()
    {
        var service = CompiledFilter.Compile("@service = 'Auth.API'");
        Assert.True(service.TryGetIndexHint(out _, out _));
        Assert.Equal([(ClefFields.ServiceName, (object?)"Auth.API")], service.GetInvertedHints());

        var note = CompiledFilter.Compile("Note = ''");
        Assert.True(note.TryGetIndexHint(out string key, out object? value));
        Assert.Equal(("Note", ""), (key, (string?)value));
        Assert.Equal([("Note", (object?)"")], note.GetInvertedHints());
    }

    // ── 2. The template under its own spelling ────────────────────────────────

    [Theory]
    [InlineData("Message")]
    [InlineData("MessageTemplate")]
    [InlineData("@mt")]
    [InlineData("@m")]
    public Task Template_BesideAClientPropertyOfTheSameName(string alias) =>
        AssertTiersAgreeAsync($"{alias} = 'Connection refused'", ["tmpl"],
        [
            new Row("tmpl", Template: "Connection refused"),
            new Row("prop", Template: "Failed: {Reason}", Props: (ref MessagePackWriter w) => { w.Write(alias); w.Write("timeout"); }),
        ]);

    /// <summary>
    /// The template's hint carries no bucket name — an empty one — and a client may send a
    /// property whose name IS empty. The executor must not ask that bucket either.
    /// </summary>
    [Theory]
    [InlineData("Message")]
    [InlineData("@mt")]
    public Task Template_BesideAClientPropertyWithAnEmptyName(string alias) =>
        AssertTiersAgreeAsync($"{alias} = 'Connection refused'", ["tmpl"],
        [
            new Row("tmpl", Template: "Connection refused"),
            new Row("prop", Template: "Failed: {Reason}", Props: static (ref MessagePackWriter w) => { w.Write(""); w.Write("timeout"); }),
        ]);

    [Theory]
    [InlineData("Message")]
    [InlineData("@mt")]
    public Task TemplateIn_BesideAClientPropertyOfTheSameName(string alias) =>
        AssertTiersAgreeAsync($"{alias} in ['Connection refused']", ["tmpl"],
        [
            new Row("tmpl", Template: "Connection refused"),
            new Row("prop", Template: "Failed: {Reason}", Props: (ref MessagePackWriter w) => { w.Write(alias); w.Write("timeout"); }),
        ]);

    /// <summary>
    /// The template's hint is the bloom's alone — no bucket to ask — and the bloom still drops a
    /// group that never logged the template: what the fix takes away is the wrong question, not
    /// the acceleration.
    /// </summary>
    [Theory]
    [InlineData("Message")]
    [InlineData("MessageTemplate")]
    [InlineData("@mt")]
    [InlineData("@m")]
    public async Task TemplateHint_IsTheBloomsAlone_AndStillDropsAGroupWithoutIt(string alias)
    {
        var compiled = CompiledFilter.Compile($"{alias} = 'Connection refused'");
        Assert.True(compiled.TryGetIndexHint(out string key, out object? value));
        Assert.Equal("", key);
        Assert.Equal("Connection refused", value);
        Assert.Empty(compiled.GetInvertedHints());

        var (engine, _) = Store("bloom-" + alias.TrimStart('@'));
        Write(engine,
        [
            new Row("tmpl", Template: "Connection refused"),
            new Row("prop", Template: "Failed: {Reason}", Props: (ref MessagePackWriter w) => { w.Write(alias); w.Write("timeout"); }),
        ]);
        await engine.FlushHotTierAsync();

        string path = Assert.Single(engine.ListSegments()).FilePath;
        using var reader = SegmentReader.Open(path);
        using var index  = SegmentIndexView.Open(null, path, 0, reader);   // what the prefilter opens per group

        Assert.True(QueryExecutor.PassesBloomGate(compiled, index));
        Assert.True(QueryExecutor.TryNarrowWithIndex(compiled, index, out _));
        Assert.False(QueryExecutor.PassesBloomGate(CompiledFilter.Compile($"{alias} = 'never logged here'"), index),
            "the bloom must still drop a group that never logged the template");
    }

    // ── 3a. A substring on a path deeper than the index flattens ──────────────

    [Theory]
    [InlineData("contains(a.b.c.d.e.f.g, 'needle')")]
    [InlineData("ci_contains(a.b.c.d.e.f.g, 'NEEDLE')")]
    [InlineData("a.b.c.d.e.f.g like '%needle%'")]
    [InlineData("startsWith(a.b.c.d.e.f.g, 'needle')")]
    [InlineData("a.b.c.d.e.f.g = 'needle-value'")]          // equality was always capped
    public Task SubstringOnASevenLevelPath(string filter) => AssertTiersAgreeAsync(filter, ["deep"],
    [
        new Row("deep", Props: static (ref MessagePackWriter w) => Nest(ref w, ["a", "b", "c", "d", "e", "f", "g"], "needle-value")),
        new Row("plain"),
    ]);

    /// <summary>An array on the way down adds no level to the index, and none to the path.</summary>
    [Fact]
    public Task SubstringOnASevenLevelPath_ThroughAnArray() => AssertTiersAgreeAsync("contains(a.b.c[0].d.e.f.g, 'needle')", ["deep"],
    [
        new Row("deep", Props: static (ref MessagePackWriter w) =>
        {
            w.Write("a"); w.WriteMapHeader(1);
            w.Write("b"); w.WriteMapHeader(1);
            w.Write("c"); w.WriteArrayHeader(1); w.WriteMapHeader(1);
            w.Write("d"); w.WriteMapHeader(1);
            w.Write("e"); w.WriteMapHeader(1);
            w.Write("f"); w.WriteMapHeader(1);
            w.Write("g"); w.Write("needle-value");
        }),
        new Row("plain"),
    ]);

    [Theory]
    [InlineData("contains(a.b.c.d.e.f, 'needle')")]
    [InlineData("a.b.c.d.e.f like '%needle%'")]
    public Task SubstringOnASixLevelPath(string filter) => AssertTiersAgreeAsync(filter, ["deep"],
    [
        new Row("deep", Props: static (ref MessagePackWriter w) => Nest(ref w, ["a", "b", "c", "d", "e", "f"], "needle-value")),
        new Row("plain"),
    ]);

    [Theory]
    [InlineData("contains(a.b.c.d.e.f.g, 'needle')", false)]
    [InlineData("a.b.c.d.e.f.g like '%needle%'", false)]
    [InlineData("startsWith(a.b.c.d.e.f.g, 'needle')", false)]
    [InlineData("contains(a.b.c[0].d.e.f.g, 'needle')", false)]
    [InlineData("contains(a.b.c.d.e.f, 'needle')", true)]          // six levels: the index holds it
    [InlineData("contains(a.b[0].c.d.e.f, 'needle')", true)]
    [InlineData("contains(['a.b.c.d.e.f.g'], 'needle')", true)]    // one key with dots in it: top level
    [InlineData("contains(@mt, 'needle')", true)]
    public void SubstringHint_StopsWhereTheIndexStopsFlattening(string filter, bool hinted) =>
        Assert.Equal(hinted, CompiledFilter.Compile(filter).GetTrigramHints().Count > 0);

    // ── 3b. Indexing.MaxPropertyFlattenDepth reaches the query side ───────────

    private static readonly Row[] NestedRows =
    [
        new("nested", Props: static (ref MessagePackWriter w) =>
        {
            w.Write("Ctx");
            w.WriteMapHeader(2);
            w.Write("Tenant"); w.Write("acmecorp");
            w.Write("Org"); w.WriteMapHeader(1); w.Write("Unit"); w.Write("ops");
        }),
        // The same name as ONE key with a dot in it, beside the nested map: a hint for the path
        // asks the union of both buckets, and the nested one is absent from a shallow index.
        new("flat", Props: static (ref MessagePackWriter w) => { w.Write("Ctx.Tenant"); w.Write("acmecorp"); }),
        new("top",  Props: static (ref MessagePackWriter w) => { w.Write("Region"); w.Write("eu-west"); }),
        new("plain"),
    ];

    [Theory]
    [InlineData(0,  "Ctx.Tenant = 'acmecorp'",            new[] { "nested", "flat" })]
    [InlineData(0,  "Ctx.Tenant in ['acmecorp']",         new[] { "nested", "flat" })]
    [InlineData(0,  "contains(Ctx.Tenant, 'corp')",       new[] { "nested", "flat" })]
    [InlineData(0,  "acmecorp",                           new[] { "nested", "flat" })]
    [InlineData(0,  "Ctx.Org.Unit = 'ops'",               new[] { "nested" })]
    [InlineData(0,  "ops",                                new[] { "nested" })]
    [InlineData(0,  "Region = 'eu-west'",                 new[] { "top" })]
    [InlineData(1,  "Ctx.Tenant = 'acmecorp'",            new[] { "nested", "flat" })]
    [InlineData(1,  "Ctx.Org.Unit = 'ops'",               new[] { "nested" })]
    [InlineData(1,  "contains(Ctx.Org.Unit, 'ops')",      new[] { "nested" })]
    [InlineData(1,  "ops",                                new[] { "nested" })]
    [InlineData(-1, "Region = 'eu-west'",                 new[] { "top" })]
    [InlineData(-1, "Ctx.Tenant = 'acmecorp'",            new[] { "nested", "flat" })]
    [InlineData(-1, "eu-west",                            new[] { "top" })]
    [InlineData(5,  "Ctx.Org.Unit = 'ops'",               new[] { "nested" })]
    [InlineData(5,  "acmecorp",                           new[] { "nested", "flat" })]
    public async Task ConfiguredFlattenDepth_ReachesTheQuerySide(int depth, string filter, string[] expected)
    {
        var (engine, hosted, uncached) = await WiredStoreAsync($"depth{depth}-{(uint)filter.GetHashCode():x8}", depth);

        Write(engine, NestedRows);
        string[] hot = await RowsAsync(hosted, filter);
        Assert.Equal(expected, hot);

        await engine.FlushHotTierAsync();
        AssertOneGroup(engine, NestedRows.Length);

        string[] cold    = await RowsAsync(hosted, filter);
        string[] again   = await RowsAsync(hosted, filter);
        string[] direct  = await RowsAsync(uncached, filter);

        Assert.Equal(hot, cold);
        Assert.Equal(hot, again);
        Assert.Equal(hot, direct);
    }

    /// <summary>
    /// The two other roads a filter takes to the index, below the default depth: a PREPARED
    /// filter — the live tail compiles one, for the default depth, and carries it on every
    /// request — and a group whose rows the prefilter leaves for the scan to name (late naming).
    /// <c>Region = 'eu'</c> names 400 rows of 600: more than the prefilter names up front, fewer
    /// than "nearly all", so the group is named late. Half the tenants are a nested map and half
    /// a flat dotted key; an index flattened to 0 holds only the flat half, and a hint on
    /// <c>Ctx.Tenant</c> would name those alone.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PreparedFilterAndLateNaming_BelowTheDefaultDepth(bool prepared)
    {
        const string filter = "Region = 'eu' and Ctx.Tenant = 'acmecorp'";
        var (engine, hosted, uncached) = await WiredStoreAsync("late-" + prepared, depth: 0);
        var compiled = prepared ? CompiledFilter.Compile(filter) : null;

        var rows = new Row[600];
        for (int i = 0; i < rows.Length; i++)
        {
            bool eu = i < 400, nested = i % 2 == 0;
            rows[i] = new Row($"r{i:D3}", Props: nested
                ? (eu ? static (ref MessagePackWriter w) => RegionAndTenant(ref w, "eu", nested: true)
                      : static (ref MessagePackWriter w) => RegionAndTenant(ref w, "us", nested: true))
                : (eu ? static (ref MessagePackWriter w) => RegionAndTenant(ref w, "eu", nested: false)
                      : static (ref MessagePackWriter w) => RegionAndTenant(ref w, "us", nested: false)),
                PropCount: 2);
        }
        Write(engine, rows);

        string[] hot = await PageAsync(hosted, filter, compiled);
        Assert.Equal([.. Enumerable.Range(0, 50).Select(static i => $"r{i:D3}")], hot);

        await engine.FlushHotTierAsync();
        AssertOneGroup(engine, rows.Length);

        long namedLate = QueryExecutor.GroupsNamedLate;
        string[] cold   = await PageAsync(hosted, filter, compiled);
        Assert.True(QueryExecutor.GroupsNamedLate > namedLate, "the group was meant to be left for the scan to name");
        string[] direct = await PageAsync(uncached, filter, compiled);

        Assert.Equal(hot, cold);
        Assert.Equal(hot, direct);
    }

    private static void RegionAndTenant(ref MessagePackWriter w, string region, bool nested)
    {
        w.Write("Region"); w.Write(region);
        if (nested) { w.Write("Ctx"); w.WriteMapHeader(1); w.Write("Tenant"); w.Write("acmecorp"); }
        else        { w.Write("Ctx.Tenant"); w.Write("acmecorp"); }
    }

    private static async Task<string[]> PageAsync(QueryExecutor query, string filter, CompiledFilter? prepared)
    {
        var page = new List<string>(50);
        await foreach (var ev in query.ExecuteAsync(new QueryRequest
        {
            Filter = filter, Prepared = prepared, Count = 50, Direction = QueryDirection.Forward,
        }))
            page.Add((string)ev.Properties!["Row"]!);
        return [.. page];
    }

    /// <summary>
    /// A store in the production wiring: one <see cref="ServerOptions"/>, the builder wired by
    /// <see cref="IndexingWiring"/> and the executor resolved from <c>AddAmetoQuery</c> (with its
    /// index cache), both reading the option — and an uncached executor given the same options.
    /// </summary>
    private async Task<(StorageEngine Engine, QueryExecutor Hosted, QueryExecutor Uncached)> WiredStoreAsync(string name, int depth)
    {
        var options = new ServerOptions
        {
            DataDirectory = Path.Combine(_dir, name),
            Indexing      = new IndexingOptions { MaxPropertyFlattenDepth = depth },
        };
        Directory.CreateDirectory(options.DataDirectory);
        var engine = new StorageEngine(
            Options.Create(options),
            new RetentionStore(options, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _engines.Add(engine);

        var provider = new ServiceCollection()
            .AddLogging()
            .AddSingleton(Options.Create(options))
            .AddSingleton(engine)
            .AddSingleton<ISegmentProvider>(engine)
            .AddAmetoIndexing()
            .AddAmetoQuery()
            .BuildServiceProvider();
        _disposables.Add(provider);
        await provider.GetRequiredService<IndexingWiring>().StartAsync(CancellationToken.None);

        var hosted   = (QueryExecutor)provider.GetRequiredService<IQueryExecutor>();
        var uncached = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance,
                                         indexCache: null, options: Options.Create(options));
        return (engine, hosted, uncached);
    }

    [Theory]
    [InlineData(5)]
    [InlineData(7)]
    public void AtTheDefaultDepthOrDeeper_TheFilterIsItsOwnAnswer(int depth)
    {
        var compiled = CompiledFilter.Compile("Ctx.Tenant = 'acmecorp' and acmecorp and contains(a.b.c.d.e.f, 'x')");
        Assert.Same(compiled, compiled.ForIndexDepth(depth));
    }

    /// <summary>
    /// A request's filter is compiled at the server's depth in one go; a prepared one is copied
    /// to it. Both give the same hints, and the copy shares what does not depend on the depth.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-1)]
    [InlineData(5)]
    [InlineData(9)]
    public void CompiledAtADepth_IsTheSameFilterAsACopyAtIt(int depth)
    {
        const string text = "Ctx.Tenant = 'acmecorp' and Region = 'eu' and @l = 'Warning' "
                          + "and @t >= '2026-08-30T00:00:00Z' and contains(Ctx.Org.Unit, 'ops') and acmecorp";
        var once   = CompiledFilter.Compile(text, depth);
        var copied = CompiledFilter.Compile(text).ForIndexDepth(depth);

        Assert.Equal(copied.GetInvertedHints(), once.GetInvertedHints());
        Assert.Equal(copied.GetTrigramHints(), once.GetTrigramHints());
        Assert.Equal(copied.TryGetIndexHint(out string copiedKey, out object? copiedValue),
                     once.TryGetIndexHint(out string onceKey, out object? onceValue));
        Assert.Equal((copiedKey, copiedValue), (onceKey, onceValue));
        Assert.Equal(copied.MinTimestampTicks, once.MinTimestampTicks);
        Assert.Equal(copied.DerivedLevels, once.DerivedLevels);
        Assert.Same(once, once.ForIndexDepth(depth));
    }

    [Fact]
    public void BelowTheDefaultDepth_DeeperHintsGo_AndTheRestStay()
    {
        var compiled = CompiledFilter.Compile(
            "Ctx.Tenant = 'acmecorp' and Region = 'eu' and @service = 'Auth.API' and contains(Ctx.Tenant, 'acme') "
          + "and contains(Region, 'eu-') and contains(@mt, 'settle') and acmecorp");
        Assert.Equal(4, compiled.GetTrigramHints().Count);    // at the default depth, every one of them

        var flat = compiled.ForIndexDepth(0);
        Assert.NotSame(compiled, flat);
        Assert.Same(flat, compiled.ForIndexDepth(0));    // made once, then kept
        Assert.Equal(compiled.Expression, flat.Expression);

        Assert.Equal([("Region", (object?)"eu"), (ClefFields.ServiceName, (object?)"Auth.API")], flat.GetInvertedHints());
        Assert.Equal([("Region", "eu-"), ("@mt", "settle")], flat.GetTrigramHints());
        Assert.True(flat.TryGetIndexHint(out string key, out _));
        Assert.Equal("Region", key);

        // No property is indexed at all: only the header fields keep their hints.
        var none = compiled.ForIndexDepth(-1);
        Assert.Equal([(ClefFields.ServiceName, (object?)"Auth.API")], none.GetInvertedHints());
        Assert.Equal([("@mt", "settle")], none.GetTrigramHints());
        Assert.True(none.TryGetIndexHint(out key, out _));
        Assert.Equal(ClefFields.ServiceName, key);

        // And the rows a filter matches do not depend on the index it is asked against.
        var ev = new LogEvent
        {
            Id              = new EventId(0u, 1u),
            Timestamp       = DateTimeOffset.UnixEpoch,
            Level           = LogLevel.Warning,
            MessageTemplate = "settle {Row}",
            ServiceName     = "Auth.API",
            Properties      = new Dictionary<string, object?>
            {
                ["Region"] = "eu-west",
                ["Ctx"]    = new Dictionary<string, object?> { ["Tenant"] = "acmecorp" },
            },
        };
        var matching = CompiledFilter.Compile("acmecorp and Ctx.Tenant = 'acmecorp'");
        Assert.True(matching.Matches(ev));
        Assert.True(matching.ForIndexDepth(0).Matches(ev));
        Assert.True(matching.ForIndexDepth(-1).Matches(ev));
    }

    // ── Fixture ───────────────────────────────────────────────────────────────

    private delegate void Props(ref MessagePackWriter w);

    /// <summary>One event: its <c>Row</c> property names it; the rest is what the case needs.</summary>
    /// <param name="Service">Null is an event logged without one, as ingest stages it: no pool index.</param>
    /// <param name="PropCount">How many properties <paramref name="Props"/> writes besides <c>Row</c>.</param>
    private sealed record Row(
        string Name, string Template = Template, string? Service = Service, byte[]? Exception = null, Props? Props = null,
        int PropCount = 1);

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
        engine.IndexSinkFactory = static (events, terms) => new SegmentIndexBuilder(events, IndexingOptions.DefaultMaxPropertyFlattenDepth, terms);
        return (engine, new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance));
    }

    /// <summary>
    /// Writes <paramref name="rows"/>, asks the hot tier, flushes them into one index group and asks
    /// again: uncached, then twice through an index cache.
    /// </summary>
    private async Task AssertTiersAgreeAsync(string filter, string[] expected, Row[] rows)
    {
        var (engine, query) = Store($"{(uint)(filter + rows.Length).GetHashCode():x8}");
        Write(engine, rows);

        Assert.Empty(engine.ListSegments());
        string[] hot = await RowsAsync(query, filter);
        Assert.Equal(expected, hot);

        await engine.FlushHotTierAsync();
        AssertOneGroup(engine, rows.Length);

        string[] cold = await RowsAsync(query, filter);
        using var cache = new SegmentIndexCache(16L * 1024 * 1024);
        var cached = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance, cache);
        string[] filled = await RowsAsync(cached, filter);
        string[] memo   = await RowsAsync(cached, filter);

        Assert.Equal(hot, cold);
        Assert.Equal(hot, filled);
        Assert.Equal(hot, memo);
        Assert.Equal(rows.Length, (await RowsAsync(query, null)).Length);
    }

    private static void AssertOneGroup(StorageEngine engine, int rows)
    {
        var segment = Assert.Single(engine.ListSegments());
        using var reader = SegmentReader.Open(segment.FilePath);
        Assert.Equal(1, reader.Groups.Length);
        Assert.Equal((uint)rows, reader.Groups[0].EventCount);
    }

    private static void Write(StorageEngine engine, Row[] rows)
    {
        long baseTicks = new DateTimeOffset(2026, 8, 30, 9, 0, 0, TimeSpan.Zero).UtcTicks;
        var  buf       = new ArrayBufferWriter<byte>(128);
        for (int i = 0; i < rows.Length; i++)
        {
            var row = rows[i];
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(row.Props is null ? 1 : 1 + row.PropCount);
            w.Write("Row"); w.Write(row.Name);
            row.Props?.Invoke(ref w);
            w.Flush();

            // One level, so one segment and one index group: the rows meet in one index.
            Assert.True(engine.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerSecond,
                Level                    = LogLevel.Warning,
                MessageTemplatePoolIndex = engine.TemplatePool.Intern(row.Template),
                ServiceNamePoolIndex     = row.Service is null ? -1 : engine.TemplatePool.Intern(row.Service),
            }, buf.WrittenSpan, row.Template, row.Exception ?? default));
        }
    }

    /// <summary>A value <paramref name="keys"/>.Length map levels down: <c>{a: {b: … {g: value}}}</c>.</summary>
    private static void Nest(ref MessagePackWriter w, string[] keys, string value)
    {
        for (int i = 0; i < keys.Length; i++)
        {
            w.Write(keys[i]);
            if (i < keys.Length - 1) w.WriteMapHeader(1);
        }
        w.Write(value);
    }

    private static async Task<string[]> RowsAsync(QueryExecutor query, string? filter)
    {
        var events = await QuerySegmentFixtures.RunAsync(query, filter, int.MaxValue, forward: true);
        return [.. events.Select(static ev => (string)ev.Properties!["Row"]!)];
    }
}
