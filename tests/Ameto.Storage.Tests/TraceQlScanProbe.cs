using System.Buffers;
using System.Diagnostics;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Ameto.Tracing.TraceQL;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT ONE TRACEQL PAGE COSTS PER SPAN IT SCANNED, and the proof that reading the answer out of
/// the msgpack blob gives the same answer as reading it out of a dictionary.
///
/// <para>The query is the repro's own: <c>{ .db.system = "mssql" &amp;&amp; duration &gt; 1s }</c>
/// over one 50 000-span segment for 200 rows. At main it allocated 43,6 MB — 913 B per span
/// scanned — to evaluate a predicate that reads ONE key, because every span the scalar filter
/// admitted was inflated into a <c>Dictionary</c> with eight key strings and eight boxes first.
/// A page is not allowed to cost a megabyte per row.</para>
///
/// <para>The parity theory below is the other half, and it is the load-bearing half: a predicate
/// that scans bytes and a predicate that reads a dictionary must agree on every value shape, or a
/// span in the hot tier and the same span after a flush answer the same query differently.</para>
/// </summary>
public sealed class TraceQlScanProbe : IClassFixture<ColdSpanSegmentFixture>, IDisposable
{
    private readonly ColdSpanSegmentFixture _fx;
    private readonly ITestOutputHelper      _out;
    private readonly List<string>           _dirs = [];

    public TraceQlScanProbe(ColdSpanSegmentFixture fx, ITestOutputHelper output)
    {
        _fx  = fx;
        _out = output;
    }

    public void Dispose()
    {
        foreach (var d in _dirs)
            try { Directory.Delete(d, true); } catch { }
    }

    // ── The probe ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task Allocated_bytes_per_span_scanned()
    {
        string dir = _fx.CopySegmentToPrivateDir();
        _dirs.Add(dir);

        using var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        engine.LoadColdSegments();   // the constructor does not rescan; the background worker would

        var from = ColdSpanSegmentFixture.Base.AddMinutes(-1);
        var to   = ColdSpanSegmentFixture.Base.AddDays(1);
        var pred = TraceQLParser.Parse("{ .db.system = \"mssql\" && duration > 1s }");

        await TraceQLExecutor.ExecuteAsync(engine, pred, from, to, 200, CancellationToken.None); // warm

        long a0   = GC.GetTotalAllocatedBytes(precise: true);
        long loh0 = GC.GetGCMemoryInfo().GenerationInfo[^1].SizeAfterBytes;
        var  sw   = Stopwatch.StartNew();
        var  page = await TraceQLExecutor.ExecuteAsync(engine, pred, from, to, 200, CancellationToken.None);
        sw.Stop();
        long allocated = GC.GetTotalAllocatedBytes(precise: true) - a0;
        long loh       = GC.GetGCMemoryInfo().GenerationInfo[^1].SizeAfterBytes - loh0;

        int scanned = ColdSpanSegmentFixture.Spans;

        _out.WriteLine($"TRACEQL  {{ .db.system = \"mssql\" && duration > 1s }} limit 200 "
                     + $"over a {scanned:N0}-span segment");
        _out.WriteLine($"  wall        {sw.Elapsed.TotalMilliseconds,10:N1} ms   "
                     + $"{sw.Elapsed.TotalMicroseconds / scanned,6:N2} us/span scanned");
        _out.WriteLine($"  allocated   {allocated / 1048576.0,10:N1} MB   {allocated / scanned,6:N0} B/span scanned");
        _out.WriteLine($"  LOH delta   {loh / 1048576.0,10:N1} MB");
        _out.WriteLine($"  rows        {page.Rows.Count}, capped={page.Capped}");

        Assert.NotEmpty(page.Rows);

        // THE GATE. main allocated 913 B per span scanned; the dictionary was ~1,5 KB of it and
        // the scan's own SpanRecord and blob copy are the rest. A predicate that reads one key out
        // of the bytes cannot come near the old figure.
        Assert.True(allocated / scanned < 700,
            $"a TraceQL page allocated {allocated / scanned:N0} B per span scanned — the attribute "
            + "dictionary is being built for spans the predicate only reads one key from");
    }

    // ── The parity theory ─────────────────────────────────────────────────────

    /// <summary>
    /// Every value shape a msgpack attribute map can hold, asked the same question twice: once of a
    /// span carrying the blob (what storage hands out now) and once of a span carrying the decoded
    /// dictionary (what every test fixture builds, and what a legacy caller still gets). The two
    /// must return the SAME three-valued answer, including the nulls.
    ///
    /// <para>Delete the blob branch in <c>AttributePredicate.Evaluate</c> — or let it answer
    /// <c>false</c> where the dictionary answers <c>null</c> — and the absent, nil, array and
    /// nested-map rows fail here. Leaving the boxing rules of <c>SpanAttributeBlob</c> alone is what
    /// keeps the numeric rows passing: decode <c>1433</c> to <c>ushort</c> instead of <c>long</c>
    /// and <c>.net.peer.port &gt; 1000</c> answers <c>false</c> on one side and <c>true</c> on the
    /// other.</para>
    /// </summary>
    [Theory]
    [InlineData("{ .db.system = \"mssql\" }",          true)]
    [InlineData("{ .db.system = \"MSSQL\" }",          true)]   // ordinal ignore case
    [InlineData("{ .db.system != \"pgsql\" }",         true)]
    [InlineData("{ .db.system < \"n\" }",              true)]   // ordering, not just equality
    [InlineData("{ .db.system > \"n\" }",              false)]
    [InlineData("{ .name.unicode = \"Ünïcödé\" }",     true)]   // multi-byte UTF-8 both sides
    [InlineData("{ .net.peer.port = 1433 }",           true)]   // uint16 encoding
    [InlineData("{ .net.peer.port > 1000 }",           true)]
    [InlineData("{ .net.peer.port < 1000 }",           false)]
    [InlineData("{ .thread.id = 63 }",                 true)]   // positive fixint
    [InlineData("{ .retry.delta < 0 }",                true)]   // int8, negative
    [InlineData("{ .cpu.ratio > 0.05 }",               true)]   // float64
    [InlineData("{ .net.peer.port = \"1433\" }",       true)]   // a number met by a string query: long.ToString()
    [InlineData("{ .cpu.ratio = \"banana\" }",         false)]  // a double met by a string query it cannot be
    [InlineData("{ .cache.hit = \"true\" }",           true)]   // bool.ToString() is "True"
    [InlineData("{ .cache.hit > 0 }",                  false)]  // present but incomparable stays false
    [InlineData("{ .numeric.text > 41 }",              true)]   // a numeric string parses
    [InlineData("{ .numeric.text > 43 }",              false)]
    [InlineData("{ .missing = \"x\" }",                null)]   // absent: unknown
    [InlineData("{ .tenant.id = \"x\" }",              null)]   // msgpack nil: unknown
    [InlineData("{ .tags = \"x\" }",                   null)]   // an array boxes to null: unknown
    [InlineData("{ .peer.info = \"x\" }",              null)]   // a nested map, likewise
    [InlineData("{ !(.missing = \"x\") }",             null)]   // and unknown negates to unknown
    [InlineData("{ .db.system != nil }",               true)]   // presence
    [InlineData("{ .tenant.id != nil }",               false)]
    [InlineData("{ .tags != nil }",                    false)]  // absent by the dictionary's reckoning
    [InlineData("{ .missing = nil }",                  true)]
    [InlineData("{ .shadowed = \"from-span\" }",       true)]   // last key wins, as the mapper intends
    public void A_predicate_answers_the_same_from_the_blob_as_from_the_dictionary(string query, bool? expected)
    {
        var blob = EveryValueShape();
        var pred = TraceQLParser.Parse(query);

        bool? fromBlob = pred.Evaluate(SpanWith(blob, dictionary: false));
        bool? fromDict = pred.Evaluate(SpanWith(blob, dictionary: true));

        _out.WriteLine($"{query,-38} blob={Show(fromBlob)}  dict={Show(fromDict)}");

        Assert.Equal(expected, fromBlob);
        Assert.Equal(fromBlob, fromDict);
    }

    /// <summary>
    /// TS#13: THE KEY LIST IS A CONSTANT, SO IT MUST NOT BE AN ALLOCATION. <c>BuildRow</c> asks for
    /// the HTTP method and path of every row it returns, and each ask went through a
    /// <c>params string[]</c> parameter with literal arguments — a fresh <c>string[]</c> per call,
    /// two per row, on a page that may return a thousand rows, and invisible to every caller.
    ///
    /// <para>Measured directly on the helper, because the arrays are 80 B per row against a page
    /// that allocates megabytes: at this call count they are unmistakable, and at page scale they
    /// would be noise. Restore the <c>params string[]</c> signature and its literal arguments and
    /// this fails at about 80 B per call instead of 0.</para>
    /// </summary>
    [Fact]
    public void Reading_the_http_attributes_of_a_row_allocates_nothing()
    {
        const int Calls = 200_000;

        var attrs = new Dictionary<string, object?>(2, StringComparer.Ordinal)
        {
            ["http.request.method"] = "GET",
            ["url.path"]            = "/api/v1/payments",
        };

        // Warm the JIT before the counter is read.
        for (int i = 0; i < 1_000; i++)
        {
            _ = TraceQLExecutor.GetAttr(attrs, TraceQLExecutor.MethodKeys);
            _ = TraceQLExecutor.GetAttr(attrs, TraceQLExecutor.PathKeys);
        }

        long before = GC.GetTotalAllocatedBytes(precise: true);
        string method = string.Empty, path = string.Empty;
        for (int i = 0; i < Calls; i++)
        {
            method = TraceQLExecutor.GetAttr(attrs, TraceQLExecutor.MethodKeys);
            path   = TraceQLExecutor.GetAttr(attrs, TraceQLExecutor.PathKeys);
        }
        long allocated = GC.GetTotalAllocatedBytes(precise: true) - before;

        _out.WriteLine($"{Calls:N0} rows x 2 attribute lookups: {allocated:N0} B "
                     + $"({(double)allocated / Calls:N1} B/row)");

        Assert.Equal("GET", method);
        Assert.Equal("/api/v1/payments", path);
        Assert.True(allocated < 8 * Calls,
            $"{allocated / (double)Calls:N1} B allocated per row to read two constant key lists");
    }

    /// <summary>A blob that will not decode answers UNKNOWN, never "no" — issue #74's rule, on bytes.</summary>
    [Fact]
    public void An_unreadable_blob_cannot_answer_either_way()
    {
        var torn = EveryValueShape()[..12];
        var span = SpanWith(torn, dictionary: false);

        Assert.Null(span.Attributes);
        Assert.Null(TraceQLParser.Parse("{ .db.system = \"mssql\" }").Evaluate(span));
        Assert.Null(TraceQLParser.Parse("{ !(.db.system = \"mssql\") }").Evaluate(span));

        // Presence is the one question a blob can still be wrong about cheaply, so it is pinned:
        // an unreadable map holds nothing anybody can name.
        Assert.False(TraceQLParser.Parse("{ .db.system != nil }").Evaluate(span));
    }

    private static string Show(bool? b) => b?.ToString() ?? "unknown";

    /// <summary>One map holding every shape a msgpack attribute value can take.</summary>
    private static byte[] EveryValueShape()
    {
        var buf = new ArrayBufferWriter<byte>(512);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(11);
        w.Write("db.system");    w.Write("mssql");
        w.Write("name.unicode"); w.Write("Ünïcödé");
        w.Write("net.peer.port"); w.Write(1433L);         // uint16 encoding
        w.Write("thread.id");    w.Write(63L);            // positive fixint
        w.Write("retry.delta");  w.Write(-100L);          // int8
        w.Write("cpu.ratio");    w.Write(0.1);
        w.Write("cache.hit");    w.Write(true);
        w.Write("tenant.id");    w.WriteNil();
        w.Write("numeric.text"); w.Write("42");
        w.Write("tags");         w.WriteArrayHeader(2); w.Write("a"); w.Write("b");
        w.Write("peer.info");    w.WriteMapHeader(1); w.Write("host"); w.Write("sql-03");
        w.Flush();

        // The resource-then-span shadowing the OTLP mapper relies on: the same key twice, and the
        // SECOND one is the answer.
        var outer = new ArrayBufferWriter<byte>(buf.WrittenCount + 64);
        var ow    = new MessagePackWriter(outer);
        ow.WriteMapHeader(13);
        ow.WriteRaw(buf.WrittenSpan[1..]);   // drop the inner map header, keep its 11 pairs
        ow.Write("shadowed"); ow.Write("from-resource");
        ow.Write("shadowed"); ow.Write("from-span");
        ow.Flush();
        return outer.WrittenMemory.ToArray();
    }

    private static SpanRecord SpanWith(byte[] blob, bool dictionary) => dictionary
        ? new SpanRecord
        {
            TraceId = new TraceId(1, 2), SpanId = new SpanId(3),
            Name = "SELECT payments", ServiceName = "billing",
            Kind = SpanKind.Client, Status = SpanStatusCode.Ok,
            Attributes = SpanAttributeBlob.Decode(blob),
        }
        : new SpanRecord
        {
            TraceId = new TraceId(1, 2), SpanId = new SpanId(3),
            Name = "SELECT payments", ServiceName = "billing",
            Kind = SpanKind.Client, Status = SpanStatusCode.Ok,
            AttributesBytes = blob,
        };
}
