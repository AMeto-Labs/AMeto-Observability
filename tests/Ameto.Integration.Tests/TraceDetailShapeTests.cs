using System.Buffers;
using System.Globalization;
using System.Net;
using System.Text;
using MessagePack;
using Microsoft.Extensions.DependencyInjection;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Integration.Tests;

/// <summary>
/// THE WIRE SHAPE OF <c>GET /api/traces/{id}</c> AND <c>GET /api/traces/{id}/flamegraph</c>, BYTE
/// FOR BYTE, as the unchanged endpoint produced it — captured before either response was taken off
/// its <c>Dictionary</c>-per-span DTO and its reflection serialiser, so that rewrite has something
/// exact to be held to.
///
/// <para>The Angular client reads these two bodies directly (the waterfall, the span details pane,
/// the flame graph), and nothing else on the server pins them: every attribute value arrives as a
/// STRING — the text <c>object.ToString()</c> gave the value the msgpack map decoded to — and a
/// transcoder that emitted numbers as numbers would change what the client renders without a
/// single existing test noticing.</para>
///
/// <para>WHAT "THE TEXT <c>ToString()</c> GAVE" MEANS, EXACTLY, and each of these is a row of the
/// fixture below:</para>
/// <list type="bullet">
///   <item>strings: as decoded, invalid UTF-8 becoming U+FFFD, then escaped by the encoder the
///   host's JSON options carry — ASP.NET Core's <c>UnsafeRelaxedJsonEscaping</c>, NOT the
///   System.Text.Json default: <c>&lt;</c>, <c>&amp;</c>, <c>'</c> and every non-ASCII character
///   of the BMP pass through as UTF-8, and only control characters, <c>"</c>, <c>\</c> and
///   characters outside the BMP (a surrogate pair, as two <c>\uXXXX</c>) are escaped. The SSE rows
///   of <c>TraceStreamJson</c> use the default encoder instead, which is a different shape;</item>
///   <item>every msgpack integer width: <c>long.ToString()</c>, so a uint64 at or below
///   <c>long.MaxValue</c> reads like any other integer;</item>
///   <item>a uint64 ABOVE <c>long.MaxValue</c>, at the top level of the map: the decode throws, so
///   the WHOLE map is reported as <c>{}</c> — not the one key. The same for a non-string key, a
///   torn map, and a payload that is not a map at all;</item>
///   <item>float32 and float64: <c>double.ToString()</c> — shortest round-trip, and IN THE CURRENT
///   CULTURE (issue #86). That is why every body here is captured twice: under the invariant
///   culture, and under a synthetic culture whose decimal separator, minus sign, exponent plus
///   sign, NaN and infinity symbols are all different — built by hand rather than taken from ICU
///   so the expected bytes cannot move with the ICU data of whichever machine runs this;</item>
///   <item>bool: <c>"True"</c> / <c>"False"</c>; nil, arrays, nested maps, binary and extensions:
///   <c>""</c>;</item>
///   <item>a key that appears twice: ONE property, at the position of the FIRST copy, holding the
///   value of the LAST (the decode is a dictionary indexer); a nil key is the empty-string key, and
///   collides with a real one.</item>
/// </list>
///
/// <para>Both tiers, because they reach the endpoint through different code: the hot tier hands
/// over the blob the ingest path stored, the cold tier the one <c>SpanReader</c> copies out of a
/// block — after <c>SpanWriter</c> re-encoded every blob it could not copy through verbatim. The
/// bodies are identical, and asserting that is part of the pin.</para>
///
/// <para>The flame graph's shape is its own set of rules and all of them are pinned: the root is
/// the LAST span, in start order, whose parent is empty or not in the trace — so a later orphan
/// hides the real root, and an earlier one does not; children in start order; <c>selfMs</c> is
/// the span's rounded total minus the sum of its children's ROUNDED totals, floored at zero; a
/// trace with no root at all is the JSON literal <c>null</c>.</para>
///
/// <para>Timestamps sit in the year 2100 so the retention pass this host starts after a minute
/// can never prune the cold segment out from under the second half of a test.</para>
/// </summary>
public sealed class TraceDetailShapeTests : IClassFixture<AmetoWebAppFactory>
{
    private const long Ms = 1_000_000L;

    /// <summary>2100-01-01T00:00:00Z in Unix nanoseconds — see the class summary for why.</summary>
    private const long Anchor = 4_102_444_800_000_000_000L;

    private readonly HttpClient         _client;
    private readonly TraceStorageEngine _traces;
    private readonly ITestOutputHelper  _out;

    public TraceDetailShapeTests(AmetoWebAppFactory factory, ITestOutputHelper output)
    {
        // The request culture is the TEST's: TestServer carries the caller's ExecutionContext —
        // and CultureInfo.CurrentCulture with it — into the handler only when asked. Without it
        // the handler formats under the machine's culture, and these bodies would differ between
        // this dev box (ru-KZ) and CI (en-US).
        //
        // BEFORE CreateClient, not after: the flag is COPIED into the client's handler when the
        // client is made. Set after, the class's FIRST test ran on a client that did not carry the
        // culture and passed or failed depending on the machine's culture and on which test xUnit
        // happened to run first — every later test got a client made after the flag was set.
        factory.Server.PreserveExecutionContext = true;

        _client = factory.CreateClient();
        _traces = factory.Services.GetRequiredService<TraceStorageEngine>();
        _out    = output;
    }

    // ── The two cultures ─────────────────────────────────────────────────────

    /// <summary>
    /// Every culture-dependent symbol a <c>long</c> or <c>double</c> can print, changed — the
    /// separator ru-KZ changes, plus the four that no common culture changes all at once.
    /// </summary>
    private static readonly CultureInfo Odd = BuildOdd();

    private static CultureInfo BuildOdd()
    {
        var c  = (CultureInfo)CultureInfo.InvariantCulture.Clone();
        var nf = c.NumberFormat;
        nf.NumberDecimalSeparator = ",";
        nf.NegativeSign           = "−";      // U+2212 MINUS SIGN
        nf.PositiveSign           = "⁺";      // U+207A SUPERSCRIPT PLUS, used by the exponent
        nf.NaNSymbol              = "не-число";
        nf.PositiveInfinitySymbol = "∞";
        nf.NegativeInfinitySymbol = "−∞";
        return c;
    }

    // ── Fixture: the detail trace ────────────────────────────────────────────

    private static readonly TraceId DetailTrace = new(0xABCDEF0012345678UL, 0x9ABCDEF000000001UL);

    private delegate void BlobWriter(ref MessagePackWriter w);

    private static byte[] Blob(BlobWriter write)
    {
        var buf = new ArrayBufferWriter<byte>();
        var w   = new MessagePackWriter(buf);
        write(ref w);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>One map holding every value shape a key can carry without killing the map.</summary>
    private static byte[] EveryShape() => Blob(static (ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(37);

        w.Write("str");          w.Write("plain");
        w.Write("str.empty");    w.Write("");
        w.Write("str.escape");   w.Write("quote\" backslash\\ <tag> & 'apos' +plus `tick` \n\t\u0001 end");
        w.Write("str.unicode");  w.Write("Ünïcödé кириллица 日本 😀");
        w.Write("str.badutf8");  w.WriteString([0x61, 0xFF, 0x62, 0xC3]);   // invalid, then truncated

        w.Write("int.fix");      w.Write(63L);                 // positive fixint
        w.Write("int.u8");       w.Write(200L);                // uint8
        w.Write("int.u16");      w.Write(1433L);               // uint16
        w.Write("int.neg");      w.Write(-5L);                 // negative fixint
        w.Write("int.neg16");    w.Write(-200L);               // int16
        w.Write("int.min");      w.Write(long.MinValue);       // int64
        w.Write("int.max");      w.Write(long.MaxValue);       // uint64 code, fits a long
        w.Write("u64.forced");   w.WriteUInt64(12345UL);       // uint64 code for a small value

        w.Write("f64");          w.Write(0.375);
        w.Write("f64.neg");      w.Write(-2.5);
        w.Write("f64.big");      w.Write(1e20);
        w.Write("f64.tiny");     w.Write(1.5e-7);
        w.Write("f64.pi");       w.Write(Math.PI);
        w.Write("f64.nan");      w.Write(double.NaN);
        w.Write("f64.inf");      w.Write(double.PositiveInfinity);
        w.Write("f64.ninf");     w.Write(double.NegativeInfinity);
        w.Write("f64.negzero");  w.Write(-0.0);
        w.Write("f32");          w.Write(0.1f);                // float32, widened to double

        w.Write("bool.t");       w.Write(true);
        w.Write("bool.f");       w.Write(false);
        w.Write("nil");          w.WriteNil();

        w.Write("arr");          w.WriteArrayHeader(3); w.Write(1L); w.Write("a"); w.WriteUInt64(ulong.MaxValue);
        w.Write("map");          w.WriteMapHeader(1); w.Write("k"); w.Write("v");
        ReadOnlySpan<byte> bin = [1, 2, 3];
        w.Write("bin");          w.Write(bin);
        w.Write("ext");          w.WriteExtensionFormat(new ExtensionResult(5, new byte[] { 1, 2 }));

        w.Write("dup");          w.Write("first");
        w.WriteNil();            w.Write("nil-key-value");     // a nil key is the "" key...
        w.Write("");             w.Write("empty-key-value");   // ...so this one replaces it
        w.Write("ключ<&>");      w.Write("v");
        w.WriteString([0x6B, 0xFE]); w.Write("badkey");        // invalid UTF-8 in a KEY
        w.Write("dup");          w.Write("second");            // last copy wins, first position
        w.Write("tail");         w.Write("after the duplicate");
    });

    private static readonly byte[] BigUnsigned = Blob(static (ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(2);
        w.Write("ok");  w.Write("x");
        w.Write("big"); w.WriteUInt64(ulong.MaxValue);          // ReadInt64 throws: whole map {}
    });

    private static readonly byte[] IntegerKey = Blob(static (ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(2);
        w.Write("fine"); w.Write("y");
        w.Write(7L);     w.Write("seven");                     // ReadString throws: whole map {}
    });

    private static readonly byte[] Torn = Blob(static (ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(3);                                   // promises three pairs
        w.Write("a"); w.Write("b");                            // delivers one
    });

    private static readonly byte[] NotAMap = Blob(static (ref MessagePackWriter w) =>
    {
        w.WriteArrayHeader(2); w.Write(1L); w.Write(2L);
    });

    private static readonly byte[] Trailing = Blob(static (ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(1); w.Write("a"); w.Write("b");
        w.WriteNil();                                          // a byte after the map
    });

    private static readonly byte[] EmptyMap = Blob(static (ref MessagePackWriter w) => w.WriteMapHeader(0));

    private static byte[] OneKey(string key, bool value) => Blob((ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(1); w.Write(key); w.Write(value);
    });

    private static byte[] OneKey(string key, long value) => Blob((ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(1); w.Write(key); w.Write(value);
    });

    private void Write(TraceId trace, ulong span, ulong parent, long startNano, long durNano,
                       string name, string service, SpanKind kind, SpanStatusCode status,
                       short http, byte[] attrs)
        => _traces.WriteSpan(new SpanIngestItem
        {
            TraceId           = trace,
            SpanId            = new SpanId(span),
            ParentSpanId      = new SpanId(parent),
            StartTimeUnixNano = startNano,
            DurationNanos     = durNano,
            Name              = name,
            ServiceName       = service,
            Kind              = kind,
            Status            = status,
            HttpStatusCode    = http,
            AttributesBytes   = attrs,
        });

    private void WriteDetailTrace()
    {
        long a = Anchor;
        const ulong Root = 0xA1A1A1A1A1A1A1A1UL;
        Write(DetailTrace, Root,                   0,        a,           100 * Ms, "GET /orders?id=<1>&x='y'", "сервис-α",
              SpanKind.Server,      SpanStatusCode.Ok,    200, EveryShape());
        Write(DetailTrace, 0xB2B2B2B2B2B2B2B2UL,   Root,     a +  1 * Ms,  10 * Ms, "child-bigint",    "billing",
              SpanKind.Client,      SpanStatusCode.Error, 503, BigUnsigned);
        Write(DetailTrace, 0xC3C3C3C3C3C3C3C3UL,   Root,     a +  2 * Ms,  10 * Ms, "child-intkey",    "billing",
              SpanKind.Producer,    SpanStatusCode.Unset,   0, IntegerKey);
        Write(DetailTrace, 0xD4D4D4D4D4D4D4D4UL,   Root,     a +  3 * Ms,  10 * Ms, "child-torn",      "billing",
              SpanKind.Consumer,    SpanStatusCode.Ok,      0, Torn);
        Write(DetailTrace, 0xE5E5E5E5E5E5E5E5UL,   Root,     a +  4 * Ms,  10 * Ms, "child-notmap",    "billing",
              SpanKind.Internal,    SpanStatusCode.Ok,      0, NotAMap);
        Write(DetailTrace, 0xF6F6F6F6F6F6F6F6UL,   Root,     a +  5 * Ms,  10 * Ms, "child-none",      "billing",
              SpanKind.Unspecified, SpanStatusCode.Ok,      0, []);
        Write(DetailTrace, 0x0707070707070707UL,   Root,     a +  6 * Ms,      -1,  "child-trailing",  "billing",
              (SpanKind)9,          (SpanStatusCode)7,     -1, Trailing);
        Write(DetailTrace, 0x1818181818181818UL,   Root,     a +  7 * Ms,       0,  "child-emptymap",  "billing",
              SpanKind.Internal,    SpanStatusCode.Ok,      0, EmptyMap);
        Write(DetailTrace, 0x2929292929292929UL,   0xDEADUL, a +  8 * Ms,   3 * Ms, "orphan \"quoted\" \\ name", "ünï",
              SpanKind.Server,      SpanStatusCode.Error,   0, OneKey("n", 1L));
        Write(DetailTrace, 0,                      Root,     a +  9 * Ms,   1 * Ms, "no-span-id",      "billing",
              SpanKind.Internal,    SpanStatusCode.Ok,      0, OneKey("z", true));
    }

    // ── Fixture: the flame graph traces ──────────────────────────────────────

    /// <summary>A later orphan is the root: the real root and its children are not drawn.</summary>
    private static readonly TraceId FlameLateOrphan  = new(0xF1A3E00000000001UL, 0x00000000000000AAUL);
    /// <summary>An earlier orphan is not: the real root, which comes after it, wins.</summary>
    private static readonly TraceId FlameEarlyOrphan = new(0xF1A3E00000000002UL, 0x00000000000000BBUL);
    /// <summary>No span qualifies as a root at all.</summary>
    private static readonly TraceId FlameNoRoot      = new(0xF1A3E00000000003UL, 0x00000000000000CCUL);

    private void WriteFlameTraces()
    {
        long a = Anchor + 60_000 * Ms;   // a minute after the detail trace, so the two never tie
        var  t = FlameLateOrphan;
        byte[] none = [];

        Write(t, 0x10, 0,      a,          100 * Ms,   "root",   "front", SpanKind.Server,   SpanStatusCode.Ok,    0, none);
        Write(t, 0x11, 0x10,   a + 1 * Ms, 1_234_567,  "c1",     "front", SpanKind.Internal, SpanStatusCode.Ok,    0, none);
        Write(t, 0x12, 0x10,   a + 2 * Ms, 2_000_500,  "c2",     "front", SpanKind.Client,   SpanStatusCode.Unset, 0, none);
        Write(t, 0x20, 0xDEAD, a + 3 * Ms, 50 * Ms,    "orphan", "back",  SpanKind.Server,   SpanStatusCode.Error, 0, none);
        Write(t, 0x21, 0x20,   a + 4 * Ms, 30 * Ms,    "o1",     "back",  SpanKind.Internal, SpanStatusCode.Ok,    0, none);
        Write(t, 0x22, 0x20,   a + 5 * Ms, 333_333_333, "o2",    "db",    SpanKind.Client,   SpanStatusCode.Ok,    0, none);
        Write(t, 0x23, 0x21,   a + 6 * Ms, 0,          "o1.zero", "back", SpanKind.Internal, SpanStatusCode.Ok,    0, none);
        Write(t, 0x24, 0x21,   a + 7 * Ms, -5 * Ms,    "o1.neg", "back",  (SpanKind)9,       (SpanStatusCode)7,    0, none);
        Write(t, 0,    0x21,   a + 8 * Ms, 7_777_777,  "o1.noid.a", "back", SpanKind.Producer, SpanStatusCode.Ok,  0, none);
        Write(t, 0,    0x21,   a + 9 * Ms, 1_000_001,  "o1.noid.b", "back", SpanKind.Consumer, SpanStatusCode.Ok,  0, none);

        t = FlameEarlyOrphan;
        Write(t, 0x30, 0xBEEF, a + 10 * Ms, 5 * Ms,    "early orphan", "x", SpanKind.Server,   SpanStatusCode.Ok,    0, none);
        Write(t, 0x31, 0,      a + 11 * Ms, 20 * Ms,   "real root <&>", "сервис", SpanKind.Server, SpanStatusCode.Ok, 0, none);
        Write(t, 0x32, 0x31,   a + 12 * Ms, 12_345_678, "only child", "x", SpanKind.Client,  SpanStatusCode.Error, 0, none);

        t = FlameNoRoot;
        Write(t, 0x40, 0x40,   a + 13 * Ms, 1 * Ms,    "self-parent", "x", SpanKind.Internal, SpanStatusCode.Ok,  0, none);
    }

    // ── Plumbing ─────────────────────────────────────────────────────────────

    private async Task<(HttpStatusCode Status, string? ContentType, byte[] Body)> GetAsync(
        string path, CultureInfo culture)
    {
        var saved = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = culture;
        try
        {
            using var resp = await _client.GetAsync(path);
            return (resp.StatusCode,
                    resp.Content.Headers.ContentType?.ToString(),
                    await resp.Content.ReadAsByteArrayAsync());
        }
        finally { CultureInfo.CurrentCulture = saved; }
    }

    /// <summary>
    /// The expected body is written one JSON element per source line for review; the line breaks
    /// are not part of it. Nothing in a JSON body produced here contains a raw line break — the
    /// encoder escapes every control character — so removing them is exact.
    /// </summary>
    private static string Joined(string multiLine) => multiLine.ReplaceLineEndings("");

    private async Task AssertBodyAsync(string path, CultureInfo culture, string expected, string what)
    {
        var (status, contentType, body) = await GetAsync(path, culture);
        string actual = Encoding.UTF8.GetString(body);
        if (actual != expected)
            _out.WriteLine($"{what} — ACTUAL:\n{actual}");

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal("application/json; charset=utf-8", contentType);
        Assert.Equal(expected, actual);
        Assert.Equal(Encoding.UTF8.GetBytes(expected), body);   // the bytes, not only the text
    }

    private static string Id(TraceId t) => t.ToString();

    private static string Label(CultureInfo c) => ReferenceEquals(c, Odd) ? "odd culture" : "invariant";

    // ── The detail ───────────────────────────────────────────────────────────

    [Fact]
    public async Task The_trace_detail_is_byte_for_byte_what_it_was_hot_and_cold()
    {
        WriteDetailTrace();
        string path = $"/api/traces/{Id(DetailTrace)}";

        await AssertBodyAsync(path, CultureInfo.InvariantCulture, Joined(DetailInvariant), "hot, invariant");
        await AssertBodyAsync(path, Odd,                          Joined(DetailOdd),       "hot, odd culture");

        _traces.FlushHotTier();
        Assert.True(_traces.ColdSegmentCountForTest > 0, "the flush wrote no segment");

        await AssertBodyAsync(path, CultureInfo.InvariantCulture, Joined(DetailInvariant), "cold, invariant");
        await AssertBodyAsync(path, Odd,                          Joined(DetailOdd),       "cold, odd culture");
    }

    [Fact]
    public async Task An_unknown_trace_is_an_empty_array_and_a_malformed_id_is_a_bare_400()
    {
        var (status, contentType, body) = await GetAsync(
            $"/api/traces/{Id(new TraceId(0x0123456789ABCDEFUL, 0x0123456789ABCDEFUL))}", CultureInfo.InvariantCulture);
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal("application/json; charset=utf-8", contentType);
        Assert.Equal("[]"u8.ToArray(), body);

        (status, _, body) = await GetAsync("/api/traces/not-a-trace-id", CultureInfo.InvariantCulture);
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Empty(body);
    }

    // ── The flame graph ──────────────────────────────────────────────────────

    [Fact]
    public async Task The_flamegraph_is_byte_for_byte_what_it_was_hot_and_cold()
    {
        WriteFlameTraces();

        async Task AllThree(string tier)
        {
            // No culture-dependent text in a flame graph: its numbers are JSON numbers. Both
            // cultures anyway, so a future change that routes one through ToString() is caught.
            foreach (var c in new[] { CultureInfo.InvariantCulture, Odd })
            {
                await AssertBodyAsync($"/api/traces/{Id(FlameLateOrphan)}/flamegraph",  c, Joined(FlameLateOrphanBody),  $"{tier}, late orphan, {Label(c)}");
                await AssertBodyAsync($"/api/traces/{Id(FlameEarlyOrphan)}/flamegraph", c, Joined(FlameEarlyOrphanBody), $"{tier}, early orphan, {Label(c)}");
                await AssertBodyAsync($"/api/traces/{Id(FlameNoRoot)}/flamegraph",      c, "null",                       $"{tier}, no root, {Label(c)}");
            }
        }

        await AllThree("hot");
        _traces.FlushHotTier();
        Assert.True(_traces.ColdSegmentCountForTest > 0, "the flush wrote no segment");
        await AllThree("cold");
    }

    [Fact]
    public async Task The_flamegraph_of_an_unknown_trace_is_a_bare_404_and_a_malformed_id_a_bare_400()
    {
        var (status, _, body) = await GetAsync(
            $"/api/traces/{Id(new TraceId(0x0123456789ABCDEFUL, 0xFEDCBA9876543210UL))}/flamegraph", CultureInfo.InvariantCulture);
        Assert.Equal(HttpStatusCode.NotFound, status);
        Assert.Empty(body);

        (status, _, body) = await GetAsync("/api/traces/zz/flamegraph", CultureInfo.InvariantCulture);
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Empty(body);
    }

    // ── The captured bodies ──────────────────────────────────────────────────

    private const string DetailInvariant = """
        [{"traceId":"abcdef00123456789abcdef000000001","spanId":"a1a1a1a1a1a1a1a1","parentSpanId":"0000000000000000","startTimeUnixNano":4102444800000000000,"durationNanos":100000000,"name":"GET /orders?id=<1>&x='y'","serviceName":"сервис-α","kind":"Server","status":"Ok","httpStatusCode":200,"attributes":{
        "str":"plain",
        "str.empty":"",
        "str.escape":"quote\" backslash\\ <tag> & 'apos' +plus `tick` \n\t\u0001 end",
        "str.unicode":"Ünïcödé кириллица 日本 \uD83D\uDE00",
        "str.badutf8":"a�b�",
        "int.fix":"63",
        "int.u8":"200",
        "int.u16":"1433",
        "int.neg":"-5",
        "int.neg16":"-200",
        "int.min":"-9223372036854775808",
        "int.max":"9223372036854775807",
        "u64.forced":"12345",
        "f64":"0.375",
        "f64.neg":"-2.5",
        "f64.big":"1E+20",
        "f64.tiny":"1.5E-07",
        "f64.pi":"3.141592653589793",
        "f64.nan":"NaN",
        "f64.inf":"Infinity",
        "f64.ninf":"-Infinity",
        "f64.negzero":"-0",
        "f32":"0.10000000149011612",
        "bool.t":"True",
        "bool.f":"False",
        "nil":"",
        "arr":"",
        "map":"",
        "bin":"",
        "ext":"",
        "dup":"second",
        "":"empty-key-value",
        "ключ<&>":"v",
        "k�":"badkey",
        "tail":"after the duplicate"}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"b2b2b2b2b2b2b2b2","parentSpanId":"a1a1a1a1a1a1a1a1","startTimeUnixNano":4102444800001000000,"durationNanos":10000000,"name":"child-bigint","serviceName":"billing","kind":"Client","status":"Error","httpStatusCode":503,"attributes":{}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"c3c3c3c3c3c3c3c3","parentSpanId":"a1a1a1a1a1a1a1a1","startTimeUnixNano":4102444800002000000,"durationNanos":10000000,"name":"child-intkey","serviceName":"billing","kind":"Producer","status":"Unset","httpStatusCode":0,"attributes":{}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"d4d4d4d4d4d4d4d4","parentSpanId":"a1a1a1a1a1a1a1a1","startTimeUnixNano":4102444800003000000,"durationNanos":10000000,"name":"child-torn","serviceName":"billing","kind":"Consumer","status":"Ok","httpStatusCode":0,"attributes":{}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"e5e5e5e5e5e5e5e5","parentSpanId":"a1a1a1a1a1a1a1a1","startTimeUnixNano":4102444800004000000,"durationNanos":10000000,"name":"child-notmap","serviceName":"billing","kind":"Internal","status":"Ok","httpStatusCode":0,"attributes":{}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"f6f6f6f6f6f6f6f6","parentSpanId":"a1a1a1a1a1a1a1a1","startTimeUnixNano":4102444800005000000,"durationNanos":10000000,"name":"child-none","serviceName":"billing","kind":"Unspecified","status":"Ok","httpStatusCode":0,"attributes":{}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"0707070707070707","parentSpanId":"a1a1a1a1a1a1a1a1","startTimeUnixNano":4102444800006000000,"durationNanos":-1,"name":"child-trailing","serviceName":"billing","kind":"9","status":"7","httpStatusCode":-1,"attributes":{
        "a":"b"}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"1818181818181818","parentSpanId":"a1a1a1a1a1a1a1a1","startTimeUnixNano":4102444800007000000,"durationNanos":0,"name":"child-emptymap","serviceName":"billing","kind":"Internal","status":"Ok","httpStatusCode":0,"attributes":{}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"2929292929292929","parentSpanId":"000000000000dead","startTimeUnixNano":4102444800008000000,"durationNanos":3000000,"name":"orphan \"quoted\" \\ name","serviceName":"ünï","kind":"Server","status":"Error","httpStatusCode":0,"attributes":{
        "n":"1"}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"0000000000000000","parentSpanId":"a1a1a1a1a1a1a1a1","startTimeUnixNano":4102444800009000000,"durationNanos":1000000,"name":"no-span-id","serviceName":"billing","kind":"Internal","status":"Ok","httpStatusCode":0,"attributes":{
        "z":"True"}}]
        """;

    private const string DetailOdd = """
        [{"traceId":"abcdef00123456789abcdef000000001","spanId":"a1a1a1a1a1a1a1a1","parentSpanId":"0000000000000000","startTimeUnixNano":4102444800000000000,"durationNanos":100000000,"name":"GET /orders?id=<1>&x='y'","serviceName":"сервис-α","kind":"Server","status":"Ok","httpStatusCode":200,"attributes":{
        "str":"plain",
        "str.empty":"",
        "str.escape":"quote\" backslash\\ <tag> & 'apos' +plus `tick` \n\t\u0001 end",
        "str.unicode":"Ünïcödé кириллица 日本 \uD83D\uDE00",
        "str.badutf8":"a�b�",
        "int.fix":"63",
        "int.u8":"200",
        "int.u16":"1433",
        "int.neg":"−5",
        "int.neg16":"−200",
        "int.min":"−9223372036854775808",
        "int.max":"9223372036854775807",
        "u64.forced":"12345",
        "f64":"0,375",
        "f64.neg":"−2,5",
        "f64.big":"1E⁺20",
        "f64.tiny":"1,5E−07",
        "f64.pi":"3,141592653589793",
        "f64.nan":"не-число",
        "f64.inf":"∞",
        "f64.ninf":"−∞",
        "f64.negzero":"−0",
        "f32":"0,10000000149011612",
        "bool.t":"True",
        "bool.f":"False",
        "nil":"",
        "arr":"",
        "map":"",
        "bin":"",
        "ext":"",
        "dup":"second",
        "":"empty-key-value",
        "ключ<&>":"v",
        "k�":"badkey",
        "tail":"after the duplicate"}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"b2b2b2b2b2b2b2b2","parentSpanId":"a1a1a1a1a1a1a1a1","startTimeUnixNano":4102444800001000000,"durationNanos":10000000,"name":"child-bigint","serviceName":"billing","kind":"Client","status":"Error","httpStatusCode":503,"attributes":{}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"c3c3c3c3c3c3c3c3","parentSpanId":"a1a1a1a1a1a1a1a1","startTimeUnixNano":4102444800002000000,"durationNanos":10000000,"name":"child-intkey","serviceName":"billing","kind":"Producer","status":"Unset","httpStatusCode":0,"attributes":{}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"d4d4d4d4d4d4d4d4","parentSpanId":"a1a1a1a1a1a1a1a1","startTimeUnixNano":4102444800003000000,"durationNanos":10000000,"name":"child-torn","serviceName":"billing","kind":"Consumer","status":"Ok","httpStatusCode":0,"attributes":{}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"e5e5e5e5e5e5e5e5","parentSpanId":"a1a1a1a1a1a1a1a1","startTimeUnixNano":4102444800004000000,"durationNanos":10000000,"name":"child-notmap","serviceName":"billing","kind":"Internal","status":"Ok","httpStatusCode":0,"attributes":{}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"f6f6f6f6f6f6f6f6","parentSpanId":"a1a1a1a1a1a1a1a1","startTimeUnixNano":4102444800005000000,"durationNanos":10000000,"name":"child-none","serviceName":"billing","kind":"Unspecified","status":"Ok","httpStatusCode":0,"attributes":{}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"0707070707070707","parentSpanId":"a1a1a1a1a1a1a1a1","startTimeUnixNano":4102444800006000000,"durationNanos":-1,"name":"child-trailing","serviceName":"billing","kind":"9","status":"7","httpStatusCode":-1,"attributes":{
        "a":"b"}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"1818181818181818","parentSpanId":"a1a1a1a1a1a1a1a1","startTimeUnixNano":4102444800007000000,"durationNanos":0,"name":"child-emptymap","serviceName":"billing","kind":"Internal","status":"Ok","httpStatusCode":0,"attributes":{}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"2929292929292929","parentSpanId":"000000000000dead","startTimeUnixNano":4102444800008000000,"durationNanos":3000000,"name":"orphan \"quoted\" \\ name","serviceName":"ünï","kind":"Server","status":"Error","httpStatusCode":0,"attributes":{
        "n":"1"}},
        {"traceId":"abcdef00123456789abcdef000000001","spanId":"0000000000000000","parentSpanId":"a1a1a1a1a1a1a1a1","startTimeUnixNano":4102444800009000000,"durationNanos":1000000,"name":"no-span-id","serviceName":"billing","kind":"Internal","status":"Ok","httpStatusCode":0,"attributes":{
        "z":"True"}}]
        """;

    private const string FlameLateOrphanBody = """
        {"spanId":"0000000000000020","name":"orphan","service":"back","kind":"Server","status":"Error","totalMs":50,"selfMs":0,"children":[
        {"spanId":"0000000000000021","name":"o1","service":"back","kind":"Internal","status":"Ok","totalMs":30,"selfMs":26.222,"children":[
        {"spanId":"0000000000000023","name":"o1.zero","service":"back","kind":"Internal","status":"Ok","totalMs":0,"selfMs":0,"children":[]},
        {"spanId":"0000000000000024","name":"o1.neg","service":"back","kind":"9","status":"7","totalMs":-5,"selfMs":0,"children":[]},
        {"spanId":"0000000000000000","name":"o1.noid.a","service":"back","kind":"Producer","status":"Ok","totalMs":7.778,"selfMs":7.778,"children":[]},
        {"spanId":"0000000000000000","name":"o1.noid.b","service":"back","kind":"Consumer","status":"Ok","totalMs":1,"selfMs":1,"children":[]}]},
        {"spanId":"0000000000000022","name":"o2","service":"db","kind":"Client","status":"Ok","totalMs":333.333,"selfMs":333.333,"children":[]}]}
        """;

    private const string FlameEarlyOrphanBody = """
        {"spanId":"0000000000000031","name":"real root <&>","service":"сервис","kind":"Server","status":"Ok","totalMs":20,"selfMs":7.654,"children":[
        {"spanId":"0000000000000032","name":"only child","service":"x","kind":"Client","status":"Error","totalMs":12.346,"selfMs":12.346,"children":[]}]}
        """;
}
