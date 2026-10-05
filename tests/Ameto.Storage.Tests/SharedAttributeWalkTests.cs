using System.Buffers;
using System.Text;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Ameto.Tracing.TraceQL;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// ONE WALK OF A SPAN'S ATTRIBUTE MAP FOR ALL OF A FILTER'S PREDICATES (#94) — and the same answer
/// as every predicate walking it for itself.
///
/// <para>The first test is the load-bearing one: <see cref="SpanPredicateEvaluator"/> and the AST
/// it was built from are asked the same question about the same span, over random maps — duplicate
/// keys, keys a filter names and keys it does not, every value shape, non-ASCII, torn and
/// garbage-tailed maps, and values that step over cleanly but will not decode — and random filters,
/// deep and shallow, wider than one walk takes. The AST's per-predicate evaluation is the reference
/// because it is what every page ran before this change, and what the existing three-valued tests
/// pin.</para>
///
/// <para>The rest pin what the evaluator is FOR — the walk count — with a counter rather than a
/// clock, and the corners the random test reaches only by luck.</para>
/// </summary>
public sealed class SharedAttributeWalkTests : IDisposable
{
    private readonly ITestOutputHelper _out;
    private readonly List<string>      _dirs = [];

    public SharedAttributeWalkTests(ITestOutputHelper output) => _out = output;

    public void Dispose()
    {
        foreach (var d in _dirs)
            try { Directory.Delete(d, true); } catch { }
    }

    // ── The equivalence ───────────────────────────────────────────────────────

    /// <summary>
    /// Keys the maps and the filters draw from: twice what one walk takes, so a filter often needs
    /// two; two that differ only in case (the walk is ordinal: "Tenant" is not "tenant"); non-ASCII;
    /// and the empty key, which <c>{ . = 1 }</c> asks for.
    /// </summary>
    private static readonly string[] Keys =
    [
        "db.system", "db.name", "http.route", "net.peer.port", "peer.service", "cache.hit",
        "ratio", "tenant", "Tenant", "ключ", "naïve", "日本", "k-1", "a_b", "x", "",
    ];

    /// <summary>The keys the lexer also reads as a bare word — <c>tenant = "x"</c> names the attribute as <c>.tenant</c> does.</summary>
    private static readonly HashSet<string> BareWordKeys = ["ratio", "tenant", "Tenant", "x", "a_b"];

    private static readonly string[] Ops = ["=", "!=", "<", "<=", ">", ">="];

    private static readonly string[] Literals =
    [
        "\"mssql\"", "\"MSSQL\"", "\"x\"", "\"\"", "\"42\"", "\"nil\"", "\"true\"", "\"v\"",
        "\"Ünïcödé\"", "\"ключ-значение\"", "\"0.1\"", "\"1433\"", "\"-3.5\"",
        "0", "1", "-1", "7", "42", "63", "1433", "-100", "0.1", "1e3", "-3.5", "9223372036854775807",
        "1s", "5ms", "-5ms", "100ns",
        "true", "error",   // bare words: text, not a presence test
    ];

    private static readonly string[] Intrinsics =
    [
        "duration > 1s", "duration <= 5ms", "status = error", "status != ok",
        "name = \"SELECT payments\"", "kind = client", "service = \"billing\"",
        ".http.status_code = 200", ".http.status_code >= 500",
        ".http.status_code = nil", ".http.status_code != nil",
    ];

    private static readonly string[] Texts =
    [
        "mssql", "MSSQL", "x", "", "42", "nil", "true", "True", "v", "Ünïcödé", "ÜNÏCÖDÉ",
        "ключ-значение", "0.1", "1433", "-3.5", " 7 ", "1e3", "-0", "NaN", "Infinity", "0x10", "1,5",
    ];

    private static readonly long[] Integers =
        [0, 1, -1, 7, 42, 63, 1433, -100, 1000, long.MaxValue, long.MinValue, 1L << 40, -(1L << 40)];

    private static readonly double[] Doubles =
        [0.1, -3.5, 1433.0, 1e3, 42.0, double.NaN, double.PositiveInfinity, double.NegativeInfinity, -0.0];

    [Fact]
    public void The_shared_walk_answers_what_each_predicate_answers_alone()
    {
        const int Seed    = 94;
        const int Filters = 600;
        const int Maps    = 300;
        var rnd = new Random(Seed);

        // ONE POOL OF SPANS, asked of every filter in the same order, so each evaluator meets one
        // span after another exactly as a page does: what one span's walk found must never answer
        // for the next one, and a span with no map must not disturb the state of the next one that
        // has.
        var spans = new SpanRecord[Maps];
        for (int i = 0; i < spans.Length; i++)
            spans[i] = RandomSpan(rnd, RandomMap(rnd));

        int compared = 0, unknown = 0, selected = 0, rejected = 0, wide = 0;
        for (int f = 0; f < Filters; f++)
        {
            string query = f % 4 == 3 ? WideChain(rnd) : "{ " + Expr(rnd, depth: 4) + " }";
            var    pred  = TraceQLParser.Parse(query);   // the generator writes only valid TraceQL
            var    eval  = new SpanPredicateEvaluator(pred);
            if (DistinctKeys(pred) > SpanAttributeBlob.MaxKeyAlternatives) wide++;

            foreach (var span in spans)
            {
                bool? alone  = pred.Evaluate(span);
                bool? shared = eval.Evaluate(span);
                if (alone != shared)
                    Assert.Fail($"seed {Seed}, filter {f}: {query}\n"
                              + $"  per predicate: {Show(alone)}, shared walk: {Show(shared)}\n"
                              + $"  span: duration={span.DurationNanos} status={span.Status} kind={span.Kind} "
                              + $"name=\"{span.Name}\" service=\"{span.ServiceName}\" http={span.HttpStatusCode}\n"
                              + $"  map: {Convert.ToHexString(span.AttributesBytes.Span)}");

                compared++;
                if (alone is null) unknown++;
                else if (alone == true) selected++;
                else rejected++;
            }
        }

        _out.WriteLine($"seed {Seed}: {Filters} filters x {Maps} spans = {compared:N0} answers compared — "
                     + $"{selected:N0} true, {rejected:N0} false, {unknown:N0} unknown; "
                     + $"{wide} filters name more keys than one walk takes");

        // THE GENERATOR MUST REACH WHAT THE TEST IS FOR, or it passes by asking nothing.
        Assert.True(wide >= Filters / 8, $"only {wide} filters were wider than one walk");
        Assert.True(selected >= compared / 50, $"only {selected} answers were true");
        Assert.True(rejected >= compared / 50, $"only {rejected} answers were false");
        Assert.True(unknown  >= compared / 50, $"only {unknown} answers were unknown");
    }

    // ── What it is for: the walk count ────────────────────────────────────────

    /// <summary>
    /// THE POINT OF THE CHANGE, COUNTED. Every predicate true for every span, so the conjunction never
    /// short-circuits and the per-predicate evaluation walks each span's map once per predicate; the
    /// shared walk makes one, whatever the count up to what one walk takes.
    ///
    /// <para>Let the evaluator walk per predicate again and this reads <c>predicates × spans</c>.</para>
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(6)]
    [InlineData(SpanAttributeBlob.MaxKeyAlternatives)]
    public void A_span_is_walked_once_for_all_the_predicates_of_a_filter(int predicates)
    {
        const int Spans = 100;
        string[]  keys  = KeyNames(predicates);
        var       eval  = new SpanPredicateEvaluator(TraceQLParser.Parse(AllEqual(keys, "&&")));
        var       span  = WithMap(MapOf(keys, "v"));

        for (int i = 0; i < Spans; i++)
            Assert.True(eval.Evaluate(span));

        _out.WriteLine($"{predicates} predicates: {eval.WalksForTest} walks over {Spans} spans");
        Assert.Equal(Spans, eval.WalksForTest);
    }

    /// <summary>
    /// MORE KEYS THAN ONE WALK TAKES: batched, one walk per batch per span — and an answer, never an
    /// exception. <c>FindValues</c> throws above <see cref="SpanAttributeBlob.MaxKeyAlternatives"/>
    /// keys, so handing it a wide filter's keys in one go fails every span of the page.
    /// </summary>
    [Theory]
    [InlineData(SpanAttributeBlob.MaxKeyAlternatives + 1)]
    [InlineData(SpanAttributeBlob.MaxKeyAlternatives * 2)]
    [InlineData(SpanAttributeBlob.MaxKeyAlternatives * 2 + 1)]
    public void A_filter_naming_more_keys_than_one_walk_takes_walks_once_per_batch(int predicates)
    {
        const int Spans   = 50;
        int       batches = (predicates + SpanAttributeBlob.MaxKeyAlternatives - 1) / SpanAttributeBlob.MaxKeyAlternatives;
        string[]  keys    = KeyNames(predicates);
        var       pred    = TraceQLParser.Parse(AllEqual(keys, "&&"));
        var       eval    = new SpanPredicateEvaluator(pred);

        var all     = WithMap(MapOf(keys, "v"));
        var lastOff = WithMap(MapOf(keys, "v", lastValue: "w"));   // the last key, alone in the last batch, differs

        for (int i = 0; i < Spans; i++)
        {
            Assert.Equal(pred.Evaluate(all),     eval.Evaluate(all));
            Assert.Equal(pred.Evaluate(lastOff), eval.Evaluate(lastOff));
        }

        Assert.True(eval.Evaluate(all));
        Assert.False(eval.Evaluate(lastOff));
        _out.WriteLine($"{predicates} keys = {batches} batches: {eval.WalksForTest} walks over {2 * Spans + 2} spans");
        Assert.Equal((2 * Spans + 2) * batches, eval.WalksForTest);
    }

    /// <summary>
    /// THE WALK IS LAZY: taken when the first attribute predicate is reached, so a span an intrinsic
    /// has already decided costs none, and a batch no reached predicate needs is never walked. Walk
    /// every span up front and the first count reads 100.
    /// </summary>
    [Fact]
    public void A_short_circuit_still_saves_the_walk()
    {
        var fast = WithMap(MapOf(["a", "b"], "v"), durationNanos: 5_000_000);

        var byDuration = new SpanPredicateEvaluator(
            TraceQLParser.Parse("{ duration > 1s && .a = \"v\" && .b = \"v\" }"));
        for (int i = 0; i < 100; i++) Assert.False(byDuration.Evaluate(fast));
        Assert.Equal(0, byDuration.WalksForTest);

        // k0 is in the first batch and decides the disjunction, so the batch holding the last key
        // is never walked.
        string[] keys = KeyNames(SpanAttributeBlob.MaxKeyAlternatives + 1);
        var firstDecides = new SpanPredicateEvaluator(TraceQLParser.Parse(
            $"{{ .{keys[0]} = \"v\" || ({string.Join(" && ", keys[1..].Select(static k => $".{k} = \"v\""))}) }}"));
        var span = WithMap(MapOf(keys, "v"));
        for (int i = 0; i < 100; i++) Assert.True(firstDecides.Evaluate(span));
        Assert.Equal(100, firstDecides.WalksForTest);
    }

    /// <summary>
    /// WHAT ONE SPAN'S WALK FOUND NEVER ANSWERS FOR THE NEXT. The evaluator keeps the found values in
    /// the instance, and a page hands it one span after another: a key present in the first span and
    /// absent from the second must read as absent in the second. Drop the per-span invalidation and
    /// the second answer is the first span's <c>true</c>.
    /// </summary>
    [Fact]
    public void The_values_one_span_held_never_answer_for_the_next()
    {
        var eval = new SpanPredicateEvaluator(TraceQLParser.Parse("{ .a = \"x\" }"));

        Assert.True (eval.Evaluate(WithMap(Map(("a", "x")))));
        Assert.Null (eval.Evaluate(WithMap(Map(("b", "x")))));      // no .a here: unknown, not the last span's true
        Assert.False(eval.Evaluate(WithMap(Map(("a", "y")))));
        Assert.Null (eval.Evaluate(WithMap(Map(("a", null)))));     // nil: unknown
        Assert.True (eval.Evaluate(WithMap(Map(("a", "y"), ("a", "x")))));   // the last copy counts
        Assert.Equal(5, eval.WalksForTest);
    }

    /// <summary>
    /// A VALUE THAT STEPS OVER CLEANLY BUT WILL NOT DECODE COSTS ITS OWN KEY, NOT ITS NEIGHBOURS'.
    /// <c>ReadInt64</c> refuses a uint64 above <c>long.MaxValue</c>. Each predicate walking alone
    /// decodes only its own key's values and steps over that one, so <c>.a</c> is found; one walk
    /// for both keys decodes it for <c>.big</c> and the throw used to cost <c>.a</c> as well — this
    /// filter answered unknown where every page before it answered true. The evaluator asks such a
    /// map again one key at a time, which is the old answer at the old price.
    /// </summary>
    [Fact]
    public void A_value_that_will_not_decode_costs_only_its_own_key()
    {
        var blob = Map(("a", "x"), ("big", ulong.MaxValue));
        var span = WithMap(blob);

        var either = TraceQLParser.Parse("{ .a = \"x\" || .big != nil }");
        var eval   = new SpanPredicateEvaluator(either);

        Assert.True(either.Evaluate(span));
        Assert.True(eval.Evaluate(span));
        Assert.Equal(1 + 2, eval.WalksForTest);   // the shared walk, then one per key of its batch

        // And the key under the value itself is unknown either way, as it always was.
        var big = TraceQLParser.Parse("{ .big > 0 }");
        Assert.Null(big.Evaluate(span));
        Assert.Null(new SpanPredicateEvaluator(big).Evaluate(span));
    }

    /// <summary>
    /// TWO KEYS ARE ONE WHEN THEIR BYTES ARE, because the walk compares bytes. Two strings holding
    /// different lone surrogates encode to the same UTF-8 (each becomes U+FFFD), and each predicate
    /// alone finds the map's one key. As two slots, the walk would fill the first and leave the
    /// second empty — it stops comparing at the first match — and the conjunction would answer
    /// unknown. The parser cannot produce such a key; a predicate built in code can.
    /// </summary>
    [Fact]
    public void Two_keys_that_encode_to_the_same_bytes_are_one_key_to_the_walk()
    {
        var first  = new AttributePredicate("k\uD800", TraceQLOp.Eq, TraceQLValue.FromString("x"));
        var second = new AttributePredicate("k\uDC00", TraceQLOp.Eq, TraceQLValue.FromString("x"));
        Assert.Equal(first.KeyUtf8, second.KeyUtf8);

        var buf = new ArrayBufferWriter<byte>(32);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.WriteString(first.KeyUtf8);
        w.Write("x");
        w.Flush();
        var span = WithMap(buf.WrittenSpan.ToArray());

        var both = new AndPredicate(first, second);
        Assert.True(both.Evaluate(span));
        Assert.True(new SpanPredicateEvaluator(both).Evaluate(span));
    }

    /// <summary>
    /// A record with no map — built from a dictionary, as every fixture and the legacy migration
    /// build them — is answered by the AST's dictionary path, and nothing is walked.
    /// </summary>
    [Fact]
    public void A_record_built_from_a_dictionary_is_answered_by_the_dictionary()
    {
        var span = new SpanRecord
        {
            TraceId = new TraceId(1, 2), SpanId = new SpanId(3), Name = "op", ServiceName = "svc",
            Attributes = new Dictionary<string, object?> { ["a"] = "x", ["n"] = 7L },
        };

        foreach (string q in (string[])["{ .a = \"x\" && .n > 5 }", "{ .a = \"x\" && .b != nil }", "{ !(.b = \"x\") }"])
        {
            var pred = TraceQLParser.Parse(q);
            var eval = new SpanPredicateEvaluator(pred);
            Assert.Equal(pred.Evaluate(span), eval.Evaluate(span));
            Assert.Equal(0, eval.WalksForTest);
        }
    }

    // ── The page ──────────────────────────────────────────────────────────────

    /// <summary>
    /// THE PAGE'S POST-FILTER IS THE SHARED WALK. Three hundred hot spans, each its own trace, every
    /// one carrying all three keys a three-predicate filter names; two in three match. The page's
    /// scan hands back all three hundred (its span limit is ten times the row limit) and each is
    /// walked ONCE. Post-filter with the predicate again and the evaluator walks nothing — this reads
    /// 0, and the rows are still right, which is exactly why the count has to be asserted.
    /// </summary>
    [Fact]
    public async Task A_traceql_page_walks_each_span_once_for_all_its_attribute_predicates()
    {
        const int Spans = 300;

        string dir = Path.Combine(Path.GetTempPath(), "ameto-qlwalk-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);

        using var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        var  at       = ColdSpanSegmentFixture.Base;
        long baseNano = at.ToUnixTimeMilliseconds() * 1_000_000L;
        var  match    = MapOf(["a", "b", "c"], "v");
        var  miss     = MapOf(["a", "b", "c"], "v", lastValue: "w");

        for (int i = 0; i < Spans; i++)
            engine.WriteSpan(new SpanIngestItem
            {
                TraceId           = new TraceId(0xA11, (ulong)(i + 1)),
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = default,
                StartTimeUnixNano = baseNano + i * 1_000_000L,
                DurationNanos     = 5_000_000L,
                Name              = "SELECT payments",
                ServiceName       = "billing",
                Kind              = SpanKind.Client,
                Status            = SpanStatusCode.Unset,
                AttributesBytes   = i % 3 == 2 ? miss : match,
            });

        var eval = new SpanPredicateEvaluator(TraceQLParser.Parse("{ .a = \"v\" && .b = \"v\" && .c = \"v\" }"));
        var page = await TraceQLExecutor.ExecuteAsync(
            engine, eval, at.AddMinutes(-1), at.AddDays(1), limit: 1_000, CancellationToken.None);

        _out.WriteLine($"{page.Rows.Count} rows, {eval.WalksForTest} walks over {Spans} spans");
        Assert.Equal(Spans * 2 / 3, page.Rows.Count);
        Assert.Equal(Spans, eval.WalksForTest);
    }

    // ── Generators ────────────────────────────────────────────────────────────

    private static string Expr(Random rnd, int depth)
    {
        if (depth <= 0 || rnd.Next(3) == 0) return Leaf(rnd);
        return rnd.Next(6) switch
        {
            0 => $"({Expr(rnd, depth - 1)} && {Expr(rnd, depth - 1)})",
            1 => $"({Expr(rnd, depth - 1)} || {Expr(rnd, depth - 1)})",
            2 => $"!({Expr(rnd, depth - 1)})",
            3 => $"!{Leaf(rnd)}",
            4 => $"{Expr(rnd, depth - 1)} && {Expr(rnd, depth - 1)}",   // precedence decides the shape
            _ => $"{Expr(rnd, depth - 1)} || {Expr(rnd, depth - 1)}",
        };
    }

    private static string Leaf(Random rnd) => rnd.Next(10) switch
    {
        0 => Intrinsics[rnd.Next(Intrinsics.Length)],
        1 => $"{KeyText(rnd, Keys[rnd.Next(Keys.Length)])} {(rnd.Next(2) == 0 ? "=" : "!=")} nil",
        _ => Comparison(rnd, Keys[rnd.Next(Keys.Length)]),
    };

    private static string Comparison(Random rnd, string key) =>
        $"{KeyText(rnd, key)} {Ops[rnd.Next(Ops.Length)]} {Literals[rnd.Next(Literals.Length)]}";

    /// <summary>A filter naming more distinct keys than one walk takes, chained with random connectives.</summary>
    private static string WideChain(Random rnd)
    {
        var keys = Keys.OrderBy(_ => rnd.Next()).Take(rnd.Next(SpanAttributeBlob.MaxKeyAlternatives + 1, Keys.Length + 1)).ToArray();
        var sb   = new StringBuilder("{ ");
        for (int i = 0; i < keys.Length; i++)
        {
            if (i > 0) sb.Append(rnd.Next(3) == 0 ? " || " : " && ");
            sb.Append(rnd.Next(5) == 0 ? $"{KeyText(rnd, keys[i])} != nil" : Comparison(rnd, keys[i]));
        }
        return sb.Append(" }").ToString();
    }

    /// <summary><c>.key</c>, or the bare word where the lexer reads one; the empty key is <c>.</c> before a space.</summary>
    private static string KeyText(Random rnd, string key) =>
        BareWordKeys.Contains(key) && rnd.Next(4) == 0 ? key : "." + key;

    private static SpanRecord RandomSpan(Random rnd, byte[] blob) => new()
    {
        TraceId           = new TraceId(1, 2),
        SpanId            = new SpanId(3),
        Name              = rnd.Next(2) == 0 ? "SELECT payments" : "GET /orders",
        ServiceName       = rnd.Next(2) == 0 ? "billing" : "gateway",
        Kind              = (SpanKind)rnd.Next(6),
        Status            = (SpanStatusCode)rnd.Next(3),
        DurationNanos     = rnd.Next(3) switch { 0 => 1_000_000L, 1 => 5_000_000L, _ => 2_000_000_000L },
        HttpStatusCode    = (short)(rnd.Next(3) switch { 0 => 0, 1 => 200, _ => 503 }),
        AttributesBytes   = blob,
    };

    /// <summary>
    /// A map the way the OTLP mappers write one — and the ways they do not: a header that lies about
    /// its count, a key that is nil or not text, and one map in eight torn, bit-flipped or tailed
    /// with garbage.
    /// </summary>
    private static byte[] RandomMap(Random rnd)
    {
        var buf = new ArrayBufferWriter<byte>(256);
        var w   = new MessagePackWriter(buf);

        int pairs  = rnd.Next(0, 16);
        int header = rnd.Next(25) == 0 ? Math.Max(0, pairs + rnd.Next(-2, 3)) : pairs;
        w.WriteMapHeader(header);
        for (int i = 0; i < pairs; i++)
        {
            switch (rnd.Next(100))
            {
                case 0:          w.WriteNil();                   break;   // no key at all: steps over
                case 1:          w.Write(rnd.Next(100));         break;   // not text: the map will not read
                case < 6:        w.Write("other." + rnd.Next(5)); break;   // a key no filter names
                default:         w.Write(Keys[rnd.Next(Keys.Length)]); break;
            }
            WriteValue(ref w, rnd, nested: 0);
        }
        w.Flush();

        byte[] map = buf.WrittenSpan.ToArray();
        if (rnd.Next(8) != 0 || map.Length == 0) return map;

        switch (rnd.Next(3))
        {
            case 0:
                return map[..rnd.Next(map.Length)];                                   // torn
            case 1:
            {
                var flipped = (byte[])map.Clone();
                flipped[rnd.Next(flipped.Length)] = (byte)rnd.Next(256);              // a flipped byte
                return flipped;
            }
            default:
            {
                var tail = new byte[rnd.Next(1, 8)];
                rnd.NextBytes(tail);
                return [.. map, .. tail];                                             // garbage after the map
            }
        }
    }

    private static void WriteValue(ref MessagePackWriter w, Random rnd, int nested)
    {
        switch (rnd.Next(nested > 0 ? 9 : 14))
        {
            case 0:  w.Write(Texts[rnd.Next(Texts.Length)]);           break;
            case 1:  w.Write(LongText(rnd));                           break;   // past the comparer's 256-char stack buffer
            case 2:  w.Write(Integers[rnd.Next(Integers.Length)]);     break;
            case 3:  w.Write((long)rnd.Next(-200, 2000));              break;
            case 4:  w.Write(Doubles[rnd.Next(Doubles.Length)]);       break;
            case 5:  w.Write((float)Doubles[rnd.Next(Doubles.Length)]); break;   // float32 on the wire
            case 6:  w.Write(rnd.Next(2) == 0);                        break;
            case 7:  w.WriteNil();                                     break;
            case 8:  w.Write(ulong.MaxValue - (ulong)rnd.Next(1000));  break;   // steps over, will not decode
            case 9:
            {
                int n = rnd.Next(0, 4);
                w.WriteArrayHeader(n);
                for (int i = 0; i < n; i++) WriteValue(ref w, rnd, nested + 1);
                break;
            }
            case 10:
            {
                int n = rnd.Next(0, 3);
                w.WriteMapHeader(n);
                for (int i = 0; i < n; i++) { w.Write(Keys[rnd.Next(Keys.Length)]); WriteValue(ref w, rnd, nested + 1); }
                break;
            }
            case 11:
            {
                var bin = new byte[rnd.Next(0, 5)];
                rnd.NextBytes(bin);
                w.Write((ReadOnlySpan<byte>)bin);
                break;
            }
            case 12: w.WriteExtensionFormat(new ExtensionResult(5, new byte[rnd.Next(1, 4)])); break;
            default: w.Write(Texts[rnd.Next(Texts.Length)]);           break;
        }
    }

    private static string LongText(Random rnd)
    {
        const string Alphabet = "abcXYZ019 .-ÜïжЖ日本€";
        var chars = new char[rnd.Next(100, 320)];
        for (int i = 0; i < chars.Length; i++) chars[i] = Alphabet[rnd.Next(Alphabet.Length)];
        return new string(chars);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>The distinct keys a filter names, told apart by their bytes as the walk tells them.</summary>
    private static int DistinctKeys(SpanPredicate pred)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        void Visit(SpanPredicate p)
        {
            switch (p)
            {
                case AndPredicate a:               Visit(a.Left); Visit(a.Right); break;
                case OrPredicate o:                Visit(o.Left); Visit(o.Right); break;
                case NotPredicate n:               Visit(n.Inner); break;
                case AttributePredicate attr:      seen.Add(Convert.ToHexString(attr.KeyUtf8)); break;
                case AttributePresencePredicate x: seen.Add(Convert.ToHexString(x.KeyUtf8)); break;
            }
        }
        Visit(pred);
        return seen.Count;
    }

    private static string[] KeyNames(int count) => [.. Enumerable.Range(0, count).Select(static i => $"k{i}")];

    private static string AllEqual(string[] keys, string connective) =>
        "{ " + string.Join($" {connective} ", keys.Select(static k => $".{k} = \"v\"")) + " }";

    /// <summary>Every key mapped to <paramref name="value"/> — the last one to <paramref name="lastValue"/> when given.</summary>
    private static byte[] MapOf(string[] keys, string value, string? lastValue = null)
    {
        var pairs = new (string, object?)[keys.Length];
        for (int i = 0; i < keys.Length; i++)
            pairs[i] = (keys[i], i == keys.Length - 1 && lastValue is not null ? lastValue : value);
        return Map(pairs);
    }

    private static byte[] Map(params (string Key, object? Value)[] pairs)
    {
        var buf = new ArrayBufferWriter<byte>(128);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(pairs.Length);
        foreach (var (key, value) in pairs)
        {
            w.Write(key);
            switch (value)
            {
                case null:     w.WriteNil(); break;
                case string s: w.Write(s);   break;
                case long l:   w.Write(l);   break;
                case ulong u:  w.Write(u);   break;
                case double d: w.Write(d);   break;
                case bool b:   w.Write(b);   break;
                default: throw new ArgumentException($"no msgpack shape for {value.GetType()}", nameof(pairs));
            }
        }
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private static SpanRecord WithMap(byte[] blob, long durationNanos = 5_000_000L) => new()
    {
        TraceId         = new TraceId(1, 2),
        SpanId          = new SpanId(3),
        Name            = "SELECT payments",
        ServiceName     = "billing",
        Kind            = SpanKind.Client,
        Status          = SpanStatusCode.Ok,
        DurationNanos   = durationNanos,
        AttributesBytes = blob,
    };

    private static string Show(bool? b) => b?.ToString() ?? "unknown";
}
