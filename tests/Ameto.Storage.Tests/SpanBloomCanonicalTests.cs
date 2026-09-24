using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Ameto.Tracing.TraceQL;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE SPAN BLOOM IS A PROMISE ABOUT THE EVALUATOR — issue #86 and TS#12 of #90.
///
/// <para>A block bloom may keep a block that holds no answer; it may never drop one that does. The
/// pre-#86 bloom broke that in two ways, both silent: it hashed <c>value.ToString()</c> in the
/// PROCESS culture, so a segment flushed on a ru-KZ box held <c>0,375</c> and an en-US query for
/// <c>"0.375"</c> skipped the block; and it folded case by lowercasing, which is not the equivalence
/// <c>OrdinalIgnoreCase</c> uses (<c>ς</c>/<c>σ</c>, <c>µ</c>/<c>μ</c>, the archaic Cyrillic
/// letters). The canonical bloom hashes an invariant text, folds upper-of-lower, and is fed from the
/// blob's bytes with nothing allocated per value.</para>
///
/// <para>WHAT IS PROVED HERE, AND HOW:</para>
/// <list type="bullet">
///   <item>the fold agrees with <c>OrdinalIgnoreCase</c> over EVERY code point, found through the
///     comparer's own hash contract rather than a list of letters somebody thought of;</item>
///   <item>for every value in a fuzzed space — integers, doubles by raw bit pattern (NaN payloads,
///     ±∞, −0, subnormals, 1E+21), booleans, nil, strings with Cyrillic, Greek, digits, astral
///     characters and ill-formed UTF-8 — under ru-KZ, en-US, sv-SE and the invariant culture, the
///     writer's hash equals the hash of the hint the matching query produces, and the evaluator
///     matches that query;</item>
///   <item>a segment written before #86 is still answered completely, the #86 scenario included,
///     and an older build reading a new segment skips nothing;</item>
///   <item>the byte walk accepts and rejects exactly what the decode it replaced did;</item>
///   <item>it allocates nothing per value.</item>
/// </list>
/// </summary>
public sealed class SpanBloomCanonicalTests : IDisposable
{
    private const int  Block    = 4096;
    private const long BaseNano = 1_754_049_600_000_000_000L;   // 2025-08-01T12:00:00Z

    private static readonly string[] Cultures = ["ru-KZ", "en-US", "sv-SE", ""];

    private readonly ITestOutputHelper _out;
    private readonly string _root = Path.Combine(Path.GetTempPath(), "ameto-bloom86-" + Guid.NewGuid().ToString("N"));

    public SpanBloomCanonicalTests(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_root);
    }

    public void Dispose() { try { Directory.Delete(_root, recursive: true); } catch { /* best effort */ } }

    // ── The fold, over every code point ───────────────────────────────────────────────────────

    /// <summary>
    /// EVERY PAIR OF SCALARS <c>OrdinalIgnoreCase</c> CALLS EQUAL HASHES EQUAL — from the blob on the
    /// writer's side, and from the lowercased hint on the query's. The pairs are found through the
    /// comparer's hash-code contract (equal under a comparer ⇒ equal hash under it): every scalar is
    /// bucketed by <c>string.GetHashCode(s, OrdinalIgnoreCase)</c> and every pair inside a bucket is
    /// asked, so no pair can hide in a list somebody forgot to write.
    ///
    /// <para>It also holds the LEGACY probe to the same standard: where a pre-#86 bloom's lowercase
    /// disagrees for a pair, <c>LegacyValueProbeIsExact</c> must say so, or the reader would trust a
    /// legacy bloom that cannot hold the answer.</para>
    ///
    /// <para>Make the fold a plain lowercase — the pre-#86 rule — and this fails on <c>ς</c>/<c>σ</c>
    /// first; let <c>LegacyValueProbeIsExact</c> trust a non-ASCII literal and it fails on the same
    /// pair from the legacy side.</para>
    /// </summary>
    [Fact]
    public void Every_pair_OrdinalIgnoreCase_calls_equal_hashes_equal()
    {
        UnderCulture("ru-KZ", () =>
        {
            // (hash << 32 | scalar), sorted: a bucket is a run of equal high halves.
            var keyed = new List<ulong>(1_112_064);
            Span<char> u16 = stackalloc char[2];
            for (int cp = 0; cp <= 0x10FFFF; cp++)
            {
                if (cp is >= 0xD800 and <= 0xDFFF) continue;
                int n = new Rune(cp).EncodeToUtf16(u16);
                uint h = (uint)string.GetHashCode(u16[..n], StringComparison.OrdinalIgnoreCase);
                keyed.Add(((ulong)h << 32) | (uint)cp);
            }
            keyed.Sort();

            int pairs = 0, legacyDisagreements = 0;
            var failures = new List<string>();
            for (int i = 0; i < keyed.Count; )
            {
                int j = i + 1;
                while (j < keyed.Count && keyed[j] >> 32 == keyed[i] >> 32) j++;
                for (int a = i; a < j; a++)
                for (int b = a + 1; b < j; b++)
                {
                    string sa = char.ConvertFromUtf32((int)(uint)keyed[a]);
                    string sb = char.ConvertFromUtf32((int)(uint)keyed[b]);
                    if (!string.Equals(sa, sb, StringComparison.OrdinalIgnoreCase)) continue;
                    pairs++;
                    foreach (var (value, literal) in (ReadOnlySpan<(string, string)>)[(sa, sb), (sb, sa)])
                    {
                        string lower = literal.ToLowerInvariant();   // what TraceQLExecutor hands the reader
                        if (WriterValueHash(OneAttr("k", value), "k") != SpanBloom.HashKeyValue("k", lower)
                            && SpanBloom.CanonicalValueProbeIsExact(lower, sameFold: true))   // a probe the reader would trust
                            failures.Add($"canonical: value U+{char.ConvertToUtf32(value, 0):X4} literal U+{char.ConvertToUtf32(literal, 0):X4}");

                        if (SpanBloom.LegacyHashKeyValue("k", value) == SpanBloom.LegacyHashKeyValue("k", lower)) continue;
                        legacyDisagreements++;
                        if (SpanBloom.LegacyValueProbeIsExact(lower))
                            failures.Add($"legacy trusted: value U+{char.ConvertToUtf32(value, 0):X4} literal U+{char.ConvertToUtf32(literal, 0):X4}");
                    }
                }
                i = j;
            }

            _out.WriteLine($"{keyed.Count:N0} scalars, {pairs:N0} OrdinalIgnoreCase pairs, "
                         + $"{legacyDisagreements} directed pairs the pre-#86 lowercase disagreed on, "
                         + $"{SpanBloomFold.HostDisagreementCount} characters this host folds differently from the table");
            foreach (var f in failures.Take(40)) _out.WriteLine(f);

            Assert.True(pairs > 1_200, $"only {pairs} case pairs found — the bucketing stopped seeing them");
            Assert.True(legacyDisagreements > 0, "the legacy fold agrees everywhere — then this test is not looking");
            Assert.Empty(failures);
        });
    }

    /// <summary>
    /// A scalar folds the same whether the literal arrives as typed or lowercased — the reader only
    /// ever sees <c>ToLowerInvariant(literal)</c>. Over every BMP scalar and every astral one with a
    /// case mapping; the rest fold to themselves.
    /// </summary>
    [Fact]
    public void A_literal_and_its_lowercase_probe_the_same_bits()
    {
        UnderCulture("sv-SE", () =>
        {
            int checkedScalars = 0;
            for (int cp = 0; cp <= 0x10FFFF; cp++)
            {
                if (cp is >= 0xD800 and <= 0xDFFF) continue;
                var r = new Rune(cp);
                if (cp > 0xFFFF && Rune.ToLowerInvariant(r) == r && Rune.ToUpperInvariant(r) == r) continue;
                string s = r.ToString();
                ulong writer = WriterValueHash(OneAttr("k", s), "k");
                string lower = s.ToLowerInvariant();
                Assert.True(writer == SpanBloom.HashKeyValue("k", lower) || !SpanBloom.CanonicalValueProbeIsExact(lower, sameFold: true),
                            $"U+{cp:X4} lowercased, and the reader would trust the probe");
                Assert.True(writer == SpanBloom.HashKeyValue("k", s),                    $"U+{cp:X4} as typed");
                checkedScalars++;
            }
            _out.WriteLine($"{checkedScalars:N0} scalars");
        });
    }

    // ── The value space, under four cultures ──────────────────────────────────────────────────

    /// <summary>
    /// THE FUZZ PROOF THE PLAN ASKED FOR. For every value, under the row's culture: the writer's hash
    /// from the blob, the writer's hash from the equivalent dictionary, and the writer's hash under
    /// the INVARIANT culture are one and the same; the query that spells the value's text — as the
    /// invariant <c>ToString</c>/<c>"R"</c> gives it, independently of the product's own formatting,
    /// and in upper and lower case — is matched by the evaluator from the blob and from the
    /// dictionary; and the hint that query produces probes exactly the writer's bits.
    ///
    /// <para>Put <c>value.ToString()</c> back into the writer and the ru-KZ and sv-SE rows fail on
    /// the first double and the first negative integer; format the evaluator with the ambient
    /// culture and they fail on the evaluator's answer instead.</para>
    /// </summary>
    [Theory]
    [InlineData("ru-KZ")]
    [InlineData("en-US")]
    [InlineData("sv-SE")]
    [InlineData("")]
    public void Every_value_hashes_the_same_from_the_writer_and_from_the_query_that_matches_it(string culture)
    {
        int values = 0, probes = 0;
        UnderCulture(culture, () =>
        {
            foreach (var (value, text) in ValueSpace())
            {
                byte[] blob      = OneAttr("x", value);
                ulong  writer    = WriterValueHash(blob, "x");
                ulong  invariant = UnderCulture("", () => WriterValueHash(blob, "x"));
                Assert.True(writer == invariant, $"{Show(value)}: the writer's hash depends on the culture");

                if (value is not byte[])
                {
                    var set = new HashSet<ulong>();
                    SpanBloom.AddAttr(set, "x", value);
                    set.Remove(SpanBloom.HashKey("x"));
                    Assert.True(set.Count == 1 && set.Contains(writer), $"{Show(value)}: the dictionary feed hashes differently");
                }

                var fromBlob = new SpanRecord { TraceId = new TraceId(1, 2), SpanId = new SpanId(3), Name = "op", ServiceName = "svc", AttributesBytes = blob };
                var fromDict = new SpanRecord { TraceId = new TraceId(1, 2), SpanId = new SpanId(3), Name = "op", ServiceName = "svc", Attributes = SpanAttributeBlob.Decode(blob) };

                foreach (string literal in (string[])[text, text.ToUpperInvariant(), text.ToLowerInvariant()])
                {
                    var pred = new AttributePredicate("x", TraceQLOp.Eq, TraceQLValue.FromString(literal));
                    bool? b = pred.Evaluate(fromBlob), d = pred.Evaluate(fromDict);
                    Assert.True(b == d, $"{Show(value)} = \"{literal}\": blob {b}, dictionary {d}");
                    if (ReferenceEquals(literal, text))
                        Assert.True(b == true, $"{Show(value)} = \"{literal}\" did not match its own invariant text");
                    if (b != true) continue;   // a case variant OrdinalIgnoreCase does not equate (ſ against S)

                    var hint = Assert.Single(TraceQLExecutor.ExtractHints(pred).AttrHints!);
                    Assert.True(SpanBloom.HashKeyValue(hint.Key, hint.LowerValue!) == writer,
                        $"{Show(value)} = \"{literal}\": the hint probes bits the writer did not set");
                    probes++;
                }
                values++;
            }

            // nil: a key and nothing else — there is no text a query could spell.
            var nilSet = new HashSet<ulong>();
            Assert.True(SpanBloom.TryAddBlob(nilSet, OneAttr("x", null)));
            Assert.True(nilSet.Count == 1 && nilSet.Contains(SpanBloom.HashKey("x")), "nil must add the key and nothing else");
        });

        _out.WriteLine($"{culture,-6} {values:N0} values, {probes:N0} matching probes, all on the writer's bits");
        Assert.True(values > 10_000);
    }

    /// <summary>
    /// THE BEHAVIOUR CHANGE, PINNED. A number is compared as its INVARIANT text now: under ru-KZ
    /// <c>{ .ratio = "0.375" }</c> matches and <c>{ .ratio = "0,375" }</c> no longer does, and under
    /// sv-SE <c>"-3"</c> matches a negative integer that the culture writes with U+2212.
    /// </summary>
    [Fact]
    public void A_number_is_compared_as_its_invariant_text_under_every_culture()
    {
        byte[] ratio = OneAttr("ratio", 0.375);
        byte[] skew  = OneAttr("skew", -3L);
        foreach (string culture in Cultures)
        {
            UnderCulture(culture, () =>
            {
                Assert.True (Eval("{ .ratio = \"0.375\" }", ratio), culture);
                Assert.False(Eval("{ .ratio = \"0,375\" }", ratio), culture);
                Assert.True (Eval("{ .skew = \"-3\" }",     skew),  culture);
                Assert.False(Eval("{ .skew = \"−3\" }", skew), culture);
                Assert.True (Eval("{ .ratio = 0.375 }",     ratio), culture);   // numeric: unchanged
            });
        }

        static bool? Eval(string q, byte[] blob) => TraceQLParser.Parse(q).Evaluate(
            new SpanRecord { TraceId = new TraceId(1, 2), SpanId = new SpanId(3), Name = "op", ServiceName = "svc", AttributesBytes = blob });
    }

    // ── Segments: the #86 scenario, old and new ───────────────────────────────────────────────

    /// <summary>
    /// ISSUE #86 AS IT HAPPENED, AND THE SEGMENTS ALREADY ON DISK. A segment is flushed on a ru-KZ box
    /// — once by today's writer, once by the pre-#86 writer (<see cref="LegacySpanBloom"/>, certified
    /// byte-exact by <c>TraceFlushProbe</c>) — and read under en-US and sv-SE. For every query the
    /// bloom-filtered answer must equal the unfiltered one: string and numeric equality on a double,
    /// a float32, a negative integer, a float that only ever reached the writer as a dictionary, an
    /// ordinary string, and Greek whose final sigma the old lowercase fold kept apart.
    ///
    /// <para>And the skip must still be worth having: on the canonical segment every string equality
    /// reads one block of three; on the legacy one the exact literal (<c>"mssql"</c>) still does, and
    /// only the literals whose legacy text could differ fall back to the key probe (three blocks).
    /// Before #86 the legacy rows for <c>"0.375"</c>, <c>"-3"</c>, the float32 and the Greek read
    /// ZERO blocks — the reported defect. Make <c>LegacyValueProbeIsExact</c> answer true and those
    /// rows fail by name.</para>
    /// </summary>
    [Theory]
    [InlineData("en-US")]
    [InlineData("sv-SE")]
    [InlineData("ru-KZ")]
    public async Task A_segment_flushed_under_ru_KZ_answers_in_full_under_another_culture(string readCulture)
    {
        var corpus    = ThreeBlockCorpus();
        string canon  = UnderCulture("ru-KZ", () => SpanWriter.Write(NewDir("canon"), corpus).FilePath);
        string legacy = UnderCulture("ru-KZ", () => SpanWriter.Write(NewDir("legacy"), ThreeBlockCorpus()).FilePath);
        LegacySpanBloom.Rewrite(legacy, ThreeBlockCorpus(), culture: "ru-KZ");

        (string Query, int CanonBlocks, int LegacyBlocks)[] cases =
        [
            ("{ .sampling.ratio = \"0.375\" }",               1, 3),
            ("{ .sampling.ratio = 0.375 }",                   3, 3),   // numeric: key probe everywhere
            ("{ .clock.skew = \"-3\" }",                      1, 3),
            ("{ .clock.skew < 0 }",                           3, 3),   // (the lexer has no negative literal)
            ("{ .jitter = \"0.10000000149011612\" }",         1, 3),   // float32 on the wire
            ("{ .backoff.seconds = \"0.375\" }",              1, 3),   // float via the dictionary path
            ("{ .db.system = \"mssql\" }",                    1, 1),   // exact on both
            ("{ .db.system = \"MSSQL\" }",                    1, 1),
            ("{ .place = \"οδός\" }",                         1, 3),   // value "ΟΔΌΣ": σ against ς
        ];

        await UnderCultureAsync(readCulture, async () =>
        {
            foreach (var (query, canonBlocks, legacyBlocks) in cases)
            {
                var pred = TraceQLParser.Parse(query);
                foreach (var (label, path, blocks) in new[] { ("canonical", canon, canonBlocks), ("legacy", legacy, legacyBlocks) })
                {
                    int truth = 0;
                    foreach (var s in SpanReader.ReadAll(path)) if (pred.Evaluate(s) == true) truth++;

                    var (admitted, found) = await Search(path, pred);
                    _out.WriteLine($"{readCulture} {label,-9} {query,-40} truth {truth,5}  found {found,5}  read {admitted / Block} block(s)");

                    Assert.True(truth > 0, $"{label} {query}: the fixture holds no answer");
                    Assert.True(found == truth, $"{label} {query} under {readCulture}: the bloom lost {truth - found} of {truth} rows");
                    Assert.True(admitted == blocks * Block, $"{label} {query}: read {admitted / (double)Block:N2} blocks, expected {blocks}");
                }
            }
        });
    }

    /// <summary>
    /// AN OLDER BUILD READING A NEW SEGMENT SKIPS NOTHING — the rollback half of the format change.
    /// Parsed the way every pre-#86 reader parses the bloom index: a count, then that many
    /// length-prefixed bitsets. Each one is EMPTY, which every such reader treats as "no bloom, never
    /// skip"; builds since <c>36c0c81</c> additionally find the section does not end at the footer
    /// and answer "no usable bloom index". Both read the whole segment — slower, and complete. Probing
    /// the canonical bits with the old hash instead would have been a false-negative source on
    /// exactly the values #86 is about.
    /// </summary>
    [Fact]
    public void An_older_reader_finds_only_empty_blooms_in_a_new_segment()
    {
        string path = UnderCulture("en-US", () => SpanWriter.Write(NewDir("old-reader"), ThreeBlockCorpus()).FilePath);

        using var fs = File.OpenRead(path);
        using var br = new BinaryReader(fs);
        fs.Position = fs.Length - 12;
        long bloomAt  = (long)br.ReadUInt64();
        long footerAt = fs.Length - 28;

        fs.Position = bloomAt;
        uint blocks = br.ReadUInt32();
        Assert.Equal(3u, blocks);
        for (uint b = 0; b < blocks; b++)
        {
            uint len = br.ReadUInt32();
            Assert.Equal(0u, len);                                        // "0 bytes ⇒ no bloom, never skip"
            Assert.True(SpanBloom.MayContain([], SpanBloom.LegacyHashKeyValue("db.system", "mssql")));
        }
        Assert.NotEqual(footerAt, fs.Position);                           // 36c0c81+: not an index it knows
        Assert.Equal(SpanBloom.CanonicalMarker, br.ReadUInt32());         // this build: the canonical half
        Assert.Equal(SpanBloomFold.Fingerprint, br.ReadUInt64());         // …folded by this build's table
    }

    /// <summary>
    /// A CANONICAL SECTION THIS BUILD CANNOT RECOGNISE SKIPS NOTHING. A torn marker is not a legacy
    /// bloom and not a canonical one; the answer is the reader's usual "no usable index" — every block
    /// read — rather than a guess at which hash the bits were built with.
    /// </summary>
    [Fact]
    public async Task A_torn_bloom_marker_reads_every_block()
    {
        string path = UnderCulture("en-US", () => SpanWriter.Write(NewDir("torn"), ThreeBlockCorpus()).FilePath);
        byte[] file = File.ReadAllBytes(path);
        long bloomAt = (long)BinaryPrimitives.ReadUInt64LittleEndian(file.AsSpan(file.Length - 12));
        int markerAt = (int)bloomAt + 4 + 3 * 4;
        Assert.Equal(SpanBloom.CanonicalMarker, BinaryPrimitives.ReadUInt32LittleEndian(file.AsSpan(markerAt)));
        file[markerAt] ^= 0xFF;
        File.WriteAllBytes(path, file);

        await UnderCultureAsync("en-US", async () =>
        {
            var (admitted, found) = await Search(path, TraceQLParser.Parse("{ .db.system = \"mssql\" }"));
            Assert.Equal(3 * Block, admitted);
            Assert.True(found > 0);
        });
    }

    /// <summary>
    /// The whole path, through the engine: spans ingested and FLUSHED under ru-KZ, the TraceQL page
    /// asked under en-US and sv-SE. The issue's own repro — <c>{ .sampling.ratio = "0.375" }</c>
    /// answered "0 blocks instead of 1".
    /// </summary>
    [Fact]
    public async Task Issue_86_repro_through_the_engine()
    {
        string dir = NewDir("engine");
        using var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        var corpus = ThreeBlockCorpus();

        await UnderCultureAsync("ru-KZ", () =>
        {
            foreach (var s in corpus)
            {
                if (s.AttributesBytes.IsEmpty) continue;   // the engine ingests wire blobs only
                engine.WriteSpan(new SpanIngestItem
                {
                    TraceId = s.TraceId, SpanId = s.SpanId, ParentSpanId = default,
                    StartTimeUnixNano = s.StartTimeUnixNano, DurationNanos = s.DurationNanos,
                    Name = s.Name, ServiceName = s.ServiceName, Kind = s.Kind, Status = s.Status,
                    AttributesBytes = s.AttributesBytes.ToArray(),
                });
            }
            engine.FlushHotTier();
            return Task.CompletedTask;
        });
        Assert.NotEmpty(engine.ColdSegmentsForTest);

        var from = DateTimeOffset.FromUnixTimeMilliseconds(BaseNano / 1_000_000).AddMinutes(-1);
        var to   = from.AddDays(1);
        foreach (string culture in (string[])["en-US", "sv-SE"])
        {
            await UnderCultureAsync(culture, async () =>
            {
                foreach (string q in (string[])["{ .sampling.ratio = \"0.375\" }", "{ .clock.skew = \"-3\" }"])
                {
                    var page = await TraceQLExecutor.ExecuteAsync(engine, TraceQLParser.Parse(q), from, to, 10, CancellationToken.None);
                    _out.WriteLine($"{culture} {q} → {page.Rows.Count} rows");
                    Assert.Equal(10, page.Rows.Count);
                }
            });
        }
    }

    // ── The byte walk against the decode it replaced ──────────────────────────────────────────

    /// <summary>
    /// <c>TryAddBlob</c> SAYS YES AND NO EXACTLY WHERE <c>TryWalk</c> DID — its answer decides whether
    /// a span's blob is copied into the block verbatim or re-encoded from a dictionary, so a
    /// different verdict is different bytes on disk, not only a different bloom. Every rejection
    /// shape by name (truncated at every length, trailing bytes, not a map, a nil key, an integer
    /// key, a uint64 past <c>long.MaxValue</c>, ill-formed UTF-8, an extension), then 20 000 seeded
    /// mutations of a map that holds every value shape.
    /// </summary>
    [Fact]
    public void The_byte_walk_accepts_exactly_what_the_decode_accepted()
    {
        byte[] rich = RichBlob();
        var shapes = new List<(string, byte[])>
        {
            ("rich", rich),
            ("empty map", [0x80]),
            ("nil", [0xC0]),
            ("array", [0x91, 0x01]),
            ("trailing byte", [.. rich, 0x00]),
            ("nil key", [0x81, 0xC0, 0x01]),
            ("int key", [0x81, 0x01, 0x01]),
            ("uint64 past long.MaxValue", [0x81, 0xA1, (byte)'x', 0xCF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),
            ("ill-formed UTF-8 key and value", [0x81, 0xA2, 0xC3, 0x28, 0xA2, 0xED, 0xA0]),
            ("fixext1 value", [0x81, 0xA1, (byte)'x', 0xD4, 0x05, 0x01]),
            ("float32 value", [0x81, 0xA1, (byte)'x', 0xCA, 0x3E, 0x80, 0x00, 0x00]),
        };
        for (int cut = 0; cut < rich.Length; cut++) shapes.Add(($"cut at {cut}", rich[..cut]));

        foreach (var (name, blob) in shapes)
            Assert.True(Walk(blob) == Bytes(blob), $"{name}: TryWalk {Walk(blob)}, TryAddBlob {Bytes(blob)}");

        var rnd = new Random(86);
        int accepted = 0;
        for (int i = 0; i < 20_000; i++)
        {
            byte[] m = (byte[])rich.Clone();
            int edits = 1 + rnd.Next(3);
            for (int e = 0; e < edits; e++) m[rnd.Next(m.Length)] = (byte)rnd.Next(256);
            if (rnd.Next(4) == 0) m = m[..rnd.Next(m.Length)];
            bool w = Walk(m);
            Assert.True(w == Bytes(m), $"mutation {i}: TryWalk {w}, TryAddBlob {!w}");
            if (w) accepted++;
        }
        _out.WriteLine($"{shapes.Count} named shapes; 20 000 mutations, {accepted:N0} accepted by both");
        Assert.True(accepted is > 100 and < 19_900, "the mutations never exercised one of the two verdicts");

        static bool Walk(byte[] b)  => SpanAttributeBlob.TryWalk<object?>(b, null, static (_, _, _) => { });
        static bool Bytes(byte[] b) => SpanBloom.TryAddBlob(new HashSet<ulong>(), b);
    }

    /// <summary>
    /// A VALUE msgpack HAS NO TYPE FOR ANSWERS THE SAME HOT AND FLUSHED — review F2. A dictionary-built
    /// record may carry a <c>DateTime</c>, a <c>decimal</c>, a <c>ulong</c> past <c>long.MaxValue</c>, a
    /// negative <c>sbyte</c>, a <c>uint</c>; the writer stores each as a string, the bloom hashes that
    /// string, and the hot tier compares the boxed value's text. All three now use the invariant text
    /// (<c>SpanAttributeBlob.InvariantText</c>). Under ru-KZ the writer used to store <c>0.5m</c> as
    /// "0,5" and a date as "24.09.2026 12:00:00", while the hot tier compared the invariant text — so
    /// the query below matched the span in the hot tier and nothing once it was flushed. Put
    /// <c>ToString()</c> back in <c>WriteAttributes</c>' default branch and the flushed half finds 0.
    /// </summary>
    [Theory]
    [InlineData("ru-KZ")]
    [InlineData("sv-SE")]
    public async Task A_value_msgpack_has_no_type_for_answers_the_same_hot_and_flushed(string culture)
    {
        (string Key, object Hit, object Miss)[] values =
        [
            ("when",   new DateTime(2026, 9, 24, 12, 0, 0, DateTimeKind.Utc), new DateTime(2025, 1, 2, 3, 4, 5, DateTimeKind.Utc)),
            ("amount", 0.5m,                                                   1.5m),
            ("big",    ulong.MaxValue,                                         1UL),
            ("delta",  (sbyte)-5,                                              (sbyte)5),
            ("count",  7u,                                                     9u),
        ];

        var corpus = new List<SpanRecord>(2 * Block);
        for (int i = 0; i < 2 * Block; i++)
        {
            var attrs = new Dictionary<string, object?>(StringComparer.Ordinal);
            foreach (var (key, hit, miss) in values) attrs[key] = i < Block ? hit : miss;
            corpus.Add(new SpanRecord
            {
                TraceId = new TraceId(0xF2, (ulong)i + 1), SpanId = new SpanId((ulong)i + 1),
                StartTimeUnixNano = BaseNano + i * 1_000L, DurationNanos = 1_000_000,
                Name = "op", ServiceName = "svc", Attributes = attrs,
            });
        }

        await UnderCultureAsync(culture, async () =>
        {
            string path = SpanWriter.Write(NewDir("f2"), corpus).FilePath;
            var flushed = SpanReader.ReadAll(path);

            foreach (var (key, hit, _) in values)
            {
                // The literal a user would type: the BCL's invariant text, not the product's helper.
                string literal = ((IFormattable)hit).ToString(null, CultureInfo.InvariantCulture);
                var pred = new AttributePredicate(key, TraceQLOp.Eq, TraceQLValue.FromString(literal));

                int hot = 0, cold = 0;
                foreach (var s in corpus)  if (pred.Evaluate(s) == true) hot++;
                foreach (var s in flushed) if (pred.Evaluate(s) == true) cold++;
                var (admitted, found) = await Search(path, pred);
                _out.WriteLine($"{culture} .{key} = \"{literal}\": hot {hot}, flushed {cold}, bloom-filtered {found}, read {admitted / Block} block(s)");

                Assert.Equal(Block, hot);
                Assert.True(cold == hot, $".{key} = \"{literal}\": {hot} hot, {cold} once flushed");
                Assert.True(found == cold, $".{key} = \"{literal}\": the bloom lost {cold - found} of {cold}");
                Assert.Equal(Block, admitted);
            }
        });
    }

    // ── The fold is the build's, not the host's ───────────────────────────────────────────────

    /// <summary>
    /// The fold table's fingerprint, pinned: it is written into every segment, and a build whose
    /// fingerprint differs probes other builds' segments by key for every non-ASCII literal. A
    /// change here must be a decision — regenerate the table, bump nothing by accident.
    /// </summary>
    private const ulong FoldFingerprint = 0xE8EC_90FE_163C_9514UL;

    /// <summary>
    /// THE COMPILED TABLE IS THE RUNTIME'S INVARIANT FOLD, and the host-drift set is empty where it
    /// must be. Under invariant globalization — the Docker image's mode, and the mode CI's Linux job
    /// runs this class in (<c>AMETO_REQUIRE_INVARIANT_GLOBALIZATION=1</c> turns "not invariant here"
    /// into a failure there instead of a silent pass) — every BMP scalar's
    /// <c>ToUpperInvariant(ToLowerInvariant(c))</c> must equal the table, U+017F excepted. The day a
    /// .NET update brings new Unicode casing this goes red, and that is the day to regenerate
    /// <c>SpanBloomFold</c>: until then the reader already degrades safely on such a host, because the
    /// drift set is non-empty there.
    /// </summary>
    [Fact]
    public void The_embedded_fold_is_the_runtimes_invariant_fold()
    {
        _out.WriteLine($"fold fingerprint 0x{SpanBloomFold.Fingerprint:X16}, "
                     + $"{SpanBloomFold.HostDisagreementCount} characters this host folds differently");
        Assert.Equal(FoldFingerprint, SpanBloomFold.Fingerprint);

        bool invariant = InvariantGlobalization();
        if (!invariant)
        {
            Assert.True(Environment.GetEnvironmentVariable("AMETO_REQUIRE_INVARIANT_GLOBALIZATION") != "1",
                "AMETO_REQUIRE_INVARIANT_GLOBALIZATION=1 but this process runs under ICU");
            _out.WriteLine("ICU globalization: the table is checked against the runtime under invariant globalization (CI, Linux)");
            return;
        }

        int differences = 0;
        for (int c = 0; c < 65_536; c++)
        {
            if (c is >= 0xD800 and <= 0xDFFF) continue;
            var r = new Rune(c);
            int expected = c == 0x017F ? c : Rune.ToUpperInvariant(Rune.ToLowerInvariant(r)).Value;
            int actual   = SpanBloom.Fold(r).Value;
            if (expected != actual && differences++ < 20)
                _out.WriteLine($"U+{c:X4}: runtime {expected:X4}, table {actual:X4}");
        }
        Assert.Equal(0, differences);
        Assert.Equal(0, SpanBloomFold.HostDisagreementCount);
    }

    /// <summary>
    /// F1 AS IT WOULD HAPPEN: a segment written by a process in the OTHER globalization mode — the
    /// Docker image is invariant, a Windows host is ICU — read here. On this box the two modes fold
    /// Unicode 16's pairs differently (<c>ɤ</c> has an uppercase, U+A7CB, only under invariant), and
    /// before the fold was a compiled table the child's bits for <c>"ɤ-report"</c> were not the bits
    /// this process probed: the block was skipped and the rows lost, the value byte-identical on both
    /// sides. The child is this test assembly's own entry point (<see cref="ChildProcessEntry"/>).
    /// </summary>
    [Fact]
    public async Task A_segment_written_under_the_other_globalization_mode_loses_no_row()
    {
        bool invariant = InvariantGlobalization();
        string dir     = NewDir("other-mode");
        string path    = RunFoldChild(dir, childInvariant: !invariant);
        _out.WriteLine($"this process {(invariant ? "invariant" : "ICU")}, writer {(invariant ? "ICU" : "invariant")}: {path}");

        await UnderCultureAsync("en-US", async () =>
        {
            foreach (string q in FoldProbeQueries)
            {
                var pred = TraceQLParser.Parse(q);
                int truth = 0;
                foreach (var s in SpanReader.ReadAll(path)) if (pred.Evaluate(s) == true) truth++;
                var (admitted, found) = await Search(path, pred);
                _out.WriteLine($"{q,-32} truth {truth,5}  found {found,5}  read {admitted / Block} block(s)");
                Assert.True(truth > 0, $"{q}: the fixture holds no answer");
                Assert.True(found == truth, $"{q}: the bloom lost {truth - found} of {truth} rows");
                // One block: the same table on both sides. (A host whose comparer knows casing the
                // table does not probes such literals by key — correct, and three blocks.)
                if (SpanBloomFold.HostDisagreementCount == 0)
                    Assert.True(admitted == Block, $"{q}: read {admitted / (double)Block:N2} blocks — the same table on both sides should read one");
            }
        });
    }

    /// <summary>
    /// A SEGMENT FOLDED BY ANOTHER BUILD'S TABLE LOSES NO ROW — the fingerprint's reason to exist.
    /// The writer here folds with a table that keeps <c>ɤ</c>, <c>Ɤ</c>, <c>ё</c> and <c>Ё</c> apart from
    /// their other case (an older Unicode's view, or a build before a table update); the reader
    /// folds with this build's. The fingerprints differ, so every non-ASCII literal probes its key
    /// alone and finds its rows, while an ASCII literal still reads one block of three. Ignore the
    /// fingerprint and the Cyrillic and Latin-extended queries find nothing.
    /// </summary>
    [Fact]
    public async Task A_segment_folded_by_another_table_loses_no_row()
    {
        var other = SpanBloomFold.CopyOfTable();
        foreach (char c in "ɤꟋёЁ") other[c] = c;

        string path;
        using (SpanBloomFold.UseTableForTest(other))
            path = UnderCulture("ru-KZ", () => SpanWriter.Write(NewDir("other-table"), FoldProbeCorpus()).FilePath);

        await UnderCultureAsync("en-US", async () =>
        {
            foreach (var (q, blocks) in (ValueTuple<string, int>[])
                     [("{ .x = \"ɤ-report\" }", 3), ("{ .y = \"ЁЛКА\" }", 3), ("{ .y = \"ёлка\" }", 3), ("{ .db = \"MSSQL\" }", 1)])
            {
                var pred = TraceQLParser.Parse(q);
                int truth = 0;
                foreach (var s in SpanReader.ReadAll(path)) if (pred.Evaluate(s) == true) truth++;
                var (admitted, found) = await Search(path, pred);
                _out.WriteLine($"{q,-24} truth {truth,5}  found {found,5}  read {admitted / Block} block(s)");
                Assert.True(truth > 0, $"{q}: the fixture holds no answer");
                Assert.True(found == truth, $"{q}: the bloom lost {truth - found} of {truth} rows");
                Assert.True(admitted == blocks * Block, $"{q}: read {admitted / (double)Block:N2} blocks, expected {blocks}");
            }
        });
    }

    internal static readonly string[] FoldProbeQueries =
        ["{ .x = \"ɤ-report\" }", "{ .y = \"ЁЛКА\" }", "{ .y = \"ёлка\" }", "{ .db = \"mssql\" }"];

    /// <summary>
    /// Three blocks; block 0 holds <c>x = "ɤ-report"</c>, <c>y = "ёлка"</c>, <c>db = "mssql"</c>, the
    /// other two different values under the same keys. Deterministic — the child process builds it
    /// too.
    /// </summary>
    internal static List<SpanRecord> FoldProbeCorpus()
    {
        var spans = new List<SpanRecord>(3 * Block);
        var buf   = new ArrayBufferWriter<byte>(128);
        for (int i = 0; i < 3 * Block; i++)
        {
            bool first = i < Block;
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(3);
            w.Write("x");  w.Write(first ? "ɤ-report" : "other-report");
            w.Write("y");  w.Write(first ? "ёлка" : "сосна");
            w.Write("db"); w.Write(first ? "mssql" : "pgsql");
            w.Flush();
            spans.Add(new SpanRecord
            {
                TraceId = new TraceId(0xF1, (ulong)i + 1), SpanId = new SpanId((ulong)i + 1),
                StartTimeUnixNano = BaseNano + i * 1_000L, DurationNanos = 1_000_000,
                Name = "op", ServiceName = "svc", AttributesBytes = buf.WrittenSpan.ToArray(),
            });
        }
        return spans;
    }

    /// <summary>Writes <see cref="FoldProbeCorpus"/> into <paramref name="dir"/> from a child process.</summary>
    private static string RunFoldChild(string dir, bool childInvariant)
    {
        string? hostPath = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH");
        var psi = new System.Diagnostics.ProcessStartInfo(hostPath is { Length: > 0 } && File.Exists(hostPath) ? hostPath : "dotnet")
        {
            UseShellExecute        = false,
            RedirectStandardOutput = true,
            RedirectStandardError  = true,
        };
        psi.ArgumentList.Add("exec");
        psi.ArgumentList.Add(typeof(SpanBloomCanonicalTests).Assembly.Location);
        psi.ArgumentList.Add(ChildProcessEntry.WriteFoldSegmentCommand);
        psi.ArgumentList.Add(dir);
        psi.Environment["DOTNET_SYSTEM_GLOBALIZATION_INVARIANT"] = childInvariant ? "1" : "0";
        psi.Environment["DOTNET_SYSTEM_GLOBALIZATION_PREDEFINED_CULTURES_ONLY"] = "false";

        using var proc = System.Diagnostics.Process.Start(psi)!;
        var stderr = proc.StandardError.ReadToEndAsync();
        string stdout = proc.StandardOutput.ReadToEnd();
        Assert.True(proc.WaitForExit(60_000), "child process did not exit");
        if (proc.ExitCode != 0)
            Assert.Fail($"child exited {proc.ExitCode}: {stderr.GetAwaiter().GetResult()}");

        string path = stdout.Trim();
        Assert.True(File.Exists(path), $"the child wrote no segment: '{path}'");
        return path;
    }

    private static bool InvariantGlobalization() =>
        Environment.GetEnvironmentVariable("DOTNET_SYSTEM_GLOBALIZATION_INVARIANT") is { } v
        && (v == "1" || v.Equals("true", StringComparison.OrdinalIgnoreCase));

    // ── TS#12: nothing allocated per value ────────────────────────────────────────────────────

    /// <summary>
    /// TS#12's POINT, AS A GATE: feeding a block's worth of ordinary spans to the bloom from their
    /// blobs allocates NOTHING — no key string, no boxed value, no number text — once the hash set
    /// has its capacity (the flush clears and reuses one set per file). The pre-#86 feed, which
    /// decoded every pair to a string key and a boxed value and formatted every number, is measured
    /// beside it as the before figure. Best of three passes: a GC landing in the window can only add.
    /// </summary>
    [Fact]
    public void Feeding_the_bloom_from_the_blob_allocates_nothing_per_value()
    {
        UnderCulture("ru-KZ", () =>
        {
            var blobs = new byte[Block][];
            for (int i = 0; i < Block; i++) blobs[i] = OrdinarySpanBlob(i);
            var set = new HashSet<ulong>();

            long after  = BestOfThree(() => { foreach (var b in blobs) SpanBloom.TryAddBlob(set, b); });
            long before = BestOfThree(() =>
            {
                foreach (var b in blobs)
                    SpanAttributeBlob.TryWalk(b, set, static (h, k, v) => LegacySpanBloom.AddAttr(h, k, v));
            });

            _out.WriteLine($"bloom feed over {Block:N0} ordinary spans (10 attributes): "
                         + $"before (decode + ToString) {before / (double)Block,8:N1} B/span, "
                         + $"after (from the bytes) {after / (double)Block,6:N1} B/span");
            Assert.True(before > 0, "the control measured nothing — the instrument is not reading");
            Assert.Equal(0, after);

            long BestOfThree(Action feed)
            {
                set.Clear(); feed(); set.Clear(); feed();          // JIT, and the set's capacity
                long best = long.MaxValue;
                for (int pass = 0; pass < 3; pass++)
                {
                    set.Clear();
                    long a0 = GC.GetAllocatedBytesForCurrentThread();
                    feed();
                    best = Math.Min(best, GC.GetAllocatedBytesForCurrentThread() - a0);
                }
                return best;
            }
        });
    }

    /// <summary>
    /// WHAT FOLDING NON-ASCII TEXT COSTS THE FLUSH — a probe, not a gate. A block of spans whose
    /// string values are Cyrillic (the ru-KZ deployment's ordinary data), fed to the bloom from the
    /// blob; printed as nanoseconds per non-ASCII character, best of five passes.
    /// </summary>
    [Fact]
    public void Folding_non_ASCII_text_costs()
    {
        UnderCulture("ru-KZ", () =>
        {
            const string text = "Платёж принят: Алматы, ул. Абая — квитанция №";
            var blobs = new byte[Block][];
            var buf   = new ArrayBufferWriter<byte>(512);
            long nonAscii = 0;
            for (int i = 0; i < Block; i++)
            {
                buf.ResetWrittenCount();
                var w = new MessagePackWriter(buf);
                w.WriteMapHeader(4);
                for (int k = 0; k < 4; k++)
                {
                    string v = text + (i * 4 + k);
                    w.Write("поле." + k);
                    w.Write(v);
                    foreach (char c in v) if (c >= 0x80) nonAscii++;
                }
                w.Flush();
                blobs[i] = buf.WrittenSpan.ToArray();
            }

            var set = new HashSet<ulong>();
            foreach (var b in blobs) SpanBloom.TryAddBlob(set, b);   // warm
            double best = double.MaxValue;
            for (int pass = 0; pass < 5; pass++)
            {
                set.Clear();
                var sw = System.Diagnostics.Stopwatch.StartNew();
                foreach (var b in blobs) SpanBloom.TryAddBlob(set, b);
                best = Math.Min(best, sw.Elapsed.TotalNanoseconds);
            }
            _out.WriteLine($"bloom feed over {Block:N0} spans, {nonAscii:N0} non-ASCII value chars: "
                         + $"{best / 1e6:N2} ms, {best / nonAscii:N1} ns per non-ASCII char");
            Assert.True(best > 0);
        });
    }

    // ── Fixtures ──────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Three blocks of 4096 spans in start-time order, so block b is exactly spans [4096b, 4096b+4096).
    /// Block 0 holds the values the queries ask for; blocks 1 and 2 hold others under the same keys,
    /// so a key-only probe reads all three and a value probe one. Every 512th span has no blob — the
    /// dictionary path — and carries a float and an int.
    /// </summary>
    private static List<SpanRecord> ThreeBlockCorpus()
    {
        var spans = new List<SpanRecord>(3 * Block);
        var buf   = new ArrayBufferWriter<byte>(256);
        for (int i = 0; i < 3 * Block; i++)
        {
            bool first = i < Block;
            var  id    = new TraceId(0x86, (ulong)i + 1);
            long start = BaseNano + i * 1_000L;
            if (i % 512 == 7)
            {
                spans.Add(new SpanRecord
                {
                    TraceId = id, SpanId = new SpanId((ulong)i + 1), StartTimeUnixNano = start,
                    DurationNanos = 1_000_000, Name = "op", ServiceName = "svc",
                    Attributes = new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["backoff.seconds"] = first ? 0.375f : 0.5f,
                        ["clock.skew"]      = first ? -3 : 7,
                    },
                });
                continue;
            }

            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(6);
            w.Write("sampling.ratio");  w.Write(first ? 0.375 : 0.5);
            w.Write("clock.skew");      w.Write(first ? -3L : 7L);
            w.Write("jitter");          w.Write(first ? 0.1f : 0.5f);      // float32 on the wire
            w.Write("backoff.seconds"); w.Write(first ? 0.375 : 0.5);
            w.Write("db.system");       w.Write(first ? "mssql" : "postgresql");
            w.Write("place");           w.Write(first ? "ΟΔΌΣ" : "Αθήνα");
            w.Flush();
            spans.Add(new SpanRecord
            {
                TraceId = id, SpanId = new SpanId((ulong)i + 1), StartTimeUnixNano = start,
                DurationNanos = 1_000_000, Name = "op", ServiceName = "svc",
                AttributesBytes = buf.WrittenSpan.ToArray(),
            });
        }
        return spans;
    }

    /// <summary>Spans the reader hands back for the query's hints, and how many of them match it.</summary>
    private static async Task<(int Admitted, int Found)> Search(string path, SpanPredicate pred)
    {
        var hints = TraceQLExecutor.ExtractHints(pred).AttrHints;
        int admitted = 0, found = 0;
        await foreach (var s in SpanReader.SearchAsync(path, long.MinValue, long.MaxValue,
                           null, null, null, null, null, null, hints, CancellationToken.None))
        {
            admitted++;
            if (pred.Evaluate(s) == true) found++;
        }
        return (admitted, found);
    }

    /// <summary>
    /// The fuzzed value space, each with the query text that must match it — computed with the BCL's
    /// invariant formatting, not the product's, so the product is checked against something it did
    /// not write.
    /// </summary>
    private static IEnumerable<(object Value, string Text)> ValueSpace()
    {
        var rnd = new Random(86);

        long[] longs = [0, 1, -1, -3, 7, 42, 1433, 1_000_000, -1_000_000, int.MinValue, int.MaxValue,
                        long.MinValue, long.MaxValue, long.MinValue + 1];
        foreach (long l in longs) yield return (l, l.ToString(CultureInfo.InvariantCulture));
        for (int i = 0; i < 2_000; i++)
        {
            long l = rnd.NextInt64(long.MinValue, long.MaxValue) >> rnd.Next(64);
            yield return (l, l.ToString(CultureInfo.InvariantCulture));
        }

        double[] doubles =
        [
            0.0, -0.0, 0.375, -0.375, 0.1, 1.0 / 3, 0.5, 2.5, 12.5, 1e21, -1e21, 1e-21, 1e-7, 1e15, 1e16,
            123456789.125, 1433.0, double.Epsilon, -double.Epsilon,
            BitConverter.Int64BitsToDouble(0x000F_FFFF_FFFF_FFFF),    // the largest subnormal
            BitConverter.Int64BitsToDouble(0x0010_0000_0000_0000),    // the smallest normal
            double.MaxValue, double.MinValue, double.NaN, double.PositiveInfinity, double.NegativeInfinity,
            BitConverter.Int64BitsToDouble(0x7FF8_0000_0000_0001),    // a NaN with a payload
            BitConverter.Int64BitsToDouble(unchecked((long)0xFFF8_0000_0000_0000)),   // a negative NaN
            (double)0.1f, (double)0.25f, (double)float.MaxValue, (double)float.Epsilon,
        ];
        foreach (double d in doubles) yield return (d, d.ToString("R", CultureInfo.InvariantCulture));
        for (int i = 0; i < 5_000; i++)
        {
            double d = BitConverter.Int64BitsToDouble(rnd.NextInt64(long.MinValue, long.MaxValue));
            yield return (d, d.ToString("R", CultureInfo.InvariantCulture));
        }
        for (int i = 0; i < 2_000; i++)
        {
            double d = rnd.Next(-1_000_000, 1_000_000) / Math.Pow(10, rnd.Next(0, 9));
            yield return (d, d.ToString("R", CultureInfo.InvariantCulture));
        }

        yield return (true,  "True");
        yield return (false, "False");

        string[] strings =
        [
            "", "mssql", "MSSQL", "Москва", "МОСКВА", "ΟΔΌΣ", "οδός", "Σίσυφος", "µs", "μs", "12345", "-3",
            "0,375", "0.375", "−3", "NaN", "Infinity", "∞", "Ünïcödé", "日本語", "😀 emoji", "İstanbul", "ısı",
            "ǅemal", "straße", "ﬀ", "K", "ſ", "ᲂ", "�", "tab\tnew\nline", "quote\"back\\slash",
        ];
        foreach (string s in strings) yield return (s, s);

        const string pool = "abcXYZ0123456789-.,+eE ёЁжЖщЩоОсСтТвВдДъЪσΣςμµβϐθϑπϖκϰρϱφϕεϵſsSİıiIkKKßẞǄǅǆ";
        string[] astral = ["😀", "𐐀", "𐐨", "𞤀", "𞤢", "𑢠", "𑣀"];
        var sb = new StringBuilder();
        for (int i = 0; i < 3_000; i++)
        {
            sb.Clear();
            int len = rnd.Next(0, 16);
            for (int k = 0; k < len; k++)
            {
                if (rnd.Next(10) == 0) sb.Append(astral[rnd.Next(astral.Length)]);
                else sb.Append(pool[rnd.Next(pool.Length)]);
            }
            string s = sb.ToString();
            yield return (s, s);
        }

        // Ill-formed UTF-8: the evaluator decodes it with U+FFFD, and so must the bloom's fold.
        for (int i = 0; i < 1_000; i++)
        {
            var bytes = new byte[rnd.Next(1, 12)];
            rnd.NextBytes(bytes);
            yield return (bytes, Encoding.UTF8.GetString(bytes));
        }
    }

    /// <summary>A one-attribute map; a <c>byte[]</c> value is written as a msgpack str of those raw bytes.</summary>
    private static byte[] OneAttr(string key, object? value)
    {
        var buf = new ArrayBufferWriter<byte>(64);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write(key);
        switch (value)
        {
            case null:       w.WriteNil();          break;
            case long l:     w.Write(l);            break;
            case double d:   w.Write(d);            break;
            case bool b:     w.Write(b);            break;
            case string s:   w.Write(s);            break;
            case byte[] raw: w.WriteString(raw);    break;
            default: throw new ArgumentOutOfRangeException(nameof(value), value, null);
        }
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>The value half of what <c>TryAddBlob</c> puts in the bloom for a one-attribute blob.</summary>
    private static ulong WriterValueHash(byte[] blob, string key)
    {
        var set = new HashSet<ulong>();
        Assert.True(SpanBloom.TryAddBlob(set, blob));
        set.Remove(SpanBloom.HashKey(key));
        return Assert.Single(set);
    }

    /// <summary>An ordinary instrumented span's attributes: ten pairs, every value shape the mapper emits.</summary>
    private static byte[] OrdinarySpanBlob(int i)
    {
        var buf = new ArrayBufferWriter<byte>(512);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(10);
        w.Write("db.system");        w.Write(i % 3 == 0 ? "mssql" : "postgresql");
        w.Write("db.statement");     w.Write("SELECT TOP 100 * FROM Orders WHERE CustomerId = @p" + i % 17);
        w.Write("net.peer.port");    w.Write(1433L + i % 7);
        w.Write("db.rows");          w.Write((long)(i % 4096) - 2048);
        w.Write("sampling.ratio");   w.Write(0.25 + i % 4 * 0.125);
        w.Write("latency.ms");       w.Write(i / 7.0);
        w.Write("db.cached");        w.Write(i % 2 == 0);
        w.Write("город");            w.Write("Алматы-" + i % 11);
        w.Write("trace.flags");      w.WriteNil();
        w.Write("peer.tags");        w.WriteArrayHeader(2); w.Write("a"); w.Write("b");
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>One map holding every shape, a duplicate key, a nested map, binary and a float32.</summary>
    private static byte[] RichBlob()
    {
        var buf = new ArrayBufferWriter<byte>(256);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(10);
        w.Write("s");      w.Write("Москва");
        w.Write("i");      w.Write(-100L);
        w.Write("u");      w.Write(70_000L);
        w.Write("d");      w.Write(0.375);
        w.Write("f");      w.Write(0.1f);
        w.Write("b");      w.Write(true);
        w.Write("n");      w.WriteNil();
        w.Write("bin");    w.Write(new byte[] { 1, 2, 3 });
        w.Write("m");      w.WriteMapHeader(1); w.Write("k"); w.WriteArrayHeader(2); w.Write(1L); w.Write("v");
        w.Write("s");      w.Write("shadow");
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private static string Show(object value) => value switch
    {
        byte[] b => "utf8[" + Convert.ToHexString(b) + "]",
        double d => $"double {BitConverter.DoubleToInt64Bits(d):X16} ({d.ToString("R", CultureInfo.InvariantCulture)})",
        string s => $"\"{s}\"",
        _        => $"{value.GetType().Name} {Convert.ToString(value, CultureInfo.InvariantCulture)}",
    };

    private string NewDir(string label)
    {
        string dir = Path.Combine(_root, label + "-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static void UnderCulture(string culture, Action body) => UnderCulture(culture, () => { body(); return 0; });

    private static T UnderCulture<T>(string culture, Func<T> body)
    {
        var saved = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
        try { return body(); }
        finally { CultureInfo.CurrentCulture = saved; }
    }

    /// <summary>CurrentCulture flows through the awaits of <paramref name="body"/> (it is async-local).</summary>
    private static async Task UnderCultureAsync(string culture, Func<Task> body)
    {
        var saved = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
        try { await body(); }
        finally { CultureInfo.CurrentCulture = saved; }
    }
}
