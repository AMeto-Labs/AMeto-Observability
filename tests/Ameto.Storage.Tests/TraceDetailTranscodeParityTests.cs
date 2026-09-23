using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using MessagePack;
using Ameto.Tracing;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// <c>TraceDetailJson.WriteSpan</c> IS <c>SpanDto.From</c> + THE HOST'S SERIALISER, BYTE FOR BYTE,
/// over seeded random attribute blobs — the fuzz half of the trace-detail parity, beside
/// <c>TraceDetailShapeTests</c>' fixed fixture through the live endpoint.
///
/// <para>The reference is the old code itself, still in the tree because <c>/api/traces/compare</c>
/// uses it: <see cref="SpanDto.From"/> decodes the blob into boxed values and stringifies each with
/// <c>ToString()</c>, and the serialiser writes that dictionary under ASP.NET Core's HTTP JSON
/// options (relaxed encoder, camelCase). The hand-written path must match it for every blob the
/// generator can build: every msgpack value family, keys drawn from a small pool so duplicates are
/// common (first position, last value), nil and empty keys that collide, ill-formed UTF-8 in keys
/// and values, characters the relaxed encoder treats specially, integers past <c>long.MaxValue</c>
/// (whole map <c>{}</c>), torn and non-map blobs, trailing bytes, maps past the fast path's size
/// limit — and records built from a dictionary of arbitrary CLR values, under five cultures whose
/// number symbols differ.</para>
/// </summary>
public sealed class TraceDetailTranscodeParityTests(ITestOutputHelper output)
{
    private static readonly JsonSerializerOptions Host =
        new Microsoft.AspNetCore.Http.Json.JsonOptions().SerializerOptions;

    private static readonly string[] KeyPool =
    [
        "db.system", "http.route", "net.peer.port", "", "a", "dup", "ключ<&>", "日本", "emoji😀",
        "quote\"key", "back\\slash", "tab\tkey",
    ];

    private static readonly string[] Tricky =
    [
        "plain", "", "Ünïcödé", "кириллица", "😀 pair", "\u2028\u2029", "\uFFFD", "\uFEFF bom",
        "\u00A0nbsp", "\u0085nel", "\u007Fdel", "\uFFFF", "\uFDD0", "<script>&'\"", "\n\r\t\b\f\u0001",
        "a\\b/c", "+`~",
    ];

    private static CultureInfo Odd()
    {
        var c  = (CultureInfo)CultureInfo.InvariantCulture.Clone();
        var nf = c.NumberFormat;
        nf.NumberDecimalSeparator = ",";
        nf.NegativeSign           = "\u2212";
        nf.PositiveSign           = "\u207A";
        nf.NaNSymbol              = "не-число";
        nf.PositiveInfinitySymbol = "\u221E";
        nf.NegativeInfinitySymbol = "\u2212\u221E";
        return c;
    }

    private static IEnumerable<CultureInfo> Cultures()
    {
        yield return CultureInfo.InvariantCulture;
        yield return Odd();
        // Real cultures, for their real symbols: whatever the ICU data on the machine says, both
        // sides read the same NumberFormatInfo, so these are parity checks and not golden values.
        foreach (var name in new[] { "ru-KZ", "sv-SE", "ar-SA" })
        {
            CultureInfo? c = null;
            try { c = CultureInfo.GetCultureInfo(name); } catch (CultureNotFoundException) { }
            if (c is not null) yield return c;
        }
    }

    // ── The generator ────────────────────────────────────────────────────────

    private static void WriteKey(ref MessagePackWriter w, Random rng, bool illFormed)
    {
        int roll = rng.Next(100);
        if (roll < 4)      { w.WriteNil(); return; }                                  // the "" key, again
        if (illFormed && roll < 7)  { w.WriteString([(byte)'k', 0xFF]); return; }  // ill-formed: the fast path declines the map
        if (illFormed && roll < 9)  { w.WriteString([(byte)'k', 0xFE]); return; }  // ...decodes to the SAME key
        if (illFormed && roll < 11) { w.WriteString([0xC3]); return; }                  // truncated sequence
        w.Write(KeyPool[rng.Next(KeyPool.Length)]);
    }

    private static void WriteValue(ref MessagePackWriter w, Random rng, bool allowFatal, int depth = 0)
    {
        switch (rng.Next(allowFatal ? 22 : 21))
        {
            case 0:  w.Write(Tricky[rng.Next(Tricky.Length)]); break;
            case 1:  w.Write(rng.Next(-40, 200)); break;                           // fixints, uint8, int8
            case 2:  w.Write((long)rng.Next(int.MinValue, int.MaxValue) * rng.Next(1, 1 << 30)); break;
            case 3:  w.Write(rng.Next(2) == 0 ? long.MinValue : long.MaxValue); break;
            case 4:  w.WriteUInt64((ulong)rng.NextInt64(0, long.MaxValue)); break;   // uint64 code, fits
            case 5:  w.WriteInt64(rng.Next(-5, 5)); break;                         // int64 code, small
            case 6:  w.Write(BitConverter.Int64BitsToDouble(rng.NextInt64())); break;   // any double, NaNs included
            case 7:  w.Write(Math.Round(rng.NextDouble() * 1000 - 500, rng.Next(0, 6))); break;
            case 8:  w.Write(new[] { double.NaN, double.PositiveInfinity, double.NegativeInfinity, -0.0, 0.0,
                                     double.Epsilon, double.MaxValue, double.MinValue, 1e20, 1.5e-7, 0.1 }[rng.Next(11)]); break;
            case 9:  w.Write((float)(rng.NextDouble() * 100)); break;             // float32
            case 10: w.Write(BitConverter.Int32BitsToSingle(rng.Next())); break;
            case 11: w.Write(rng.Next(2) == 0); break;
            case 12: w.WriteNil(); break;
            case 13:
                w.WriteArrayHeader(2); w.Write(1L); w.WriteUInt64(ulong.MaxValue);   // Skip does not range-check
                break;
            case 14:
                if (depth > 1) { w.WriteNil(); break; }
                w.WriteMapHeader(1); w.Write("k"); WriteValue(ref w, rng, allowFatal: true, depth + 1);
                break;
            case 15: w.Write((ReadOnlySpan<byte>)[1, 2, 3]); break;                // bin
            case 16: w.WriteExtensionFormat(new ExtensionResult((sbyte)rng.Next(-1, 10), new byte[rng.Next(0, 5)])); break;
            case 17: w.WriteString([0x61, 0xFF, 0x62]); break;                      // ill-formed value
            case 18: w.WriteString([0xE2, 0x82]); break;                            // truncated value
            case 19: w.Write(new string((char)rng.Next(0x20, 0xD7FF), rng.Next(0, 4))); break;
            case 20: w.Write(Tricky[rng.Next(Tricky.Length)] + Tricky[rng.Next(Tricky.Length)]); break;
            default: w.WriteUInt64(ulong.MaxValue - (ulong)rng.Next(1000)); break;  // FATAL at the top level
        }
    }

    private static byte[] RandomBlob(Random rng)
    {
        var buf = new ArrayBufferWriter<byte>();
        var w   = new MessagePackWriter(buf);

        int shape = rng.Next(100);
        if (shape < 3)
        {
            w.WriteArrayHeader(1); w.Write("not a map");                              // not a map
        }
        else
        {
            int pairs = shape < 6 ? rng.Next(257, 300) : rng.Next(0, 40);          // past the fast path's limit
            bool fatal  = rng.Next(20) == 0;
            int  intKey = rng.Next(25) == 0 && pairs > 0 ? rng.Next(pairs) : -1;   // one non-string key: whole map {}
            bool illFormedKeys = rng.Next(10) == 0;
            w.WriteMapHeader(pairs);
            for (int i = 0; i < pairs; i++)
            {
                if (i == intKey) { w.Write(7L); w.Write("int key"); continue; }
                WriteKey(ref w, rng, illFormedKeys);
                WriteValue(ref w, rng, allowFatal: fatal);
            }
        }
        if (rng.Next(15) == 0) w.WriteNil();                                          // trailing byte
        w.Flush();

        var bytes = buf.WrittenSpan.ToArray();
        if (bytes.Length > 1 && rng.Next(15) == 0)
            bytes = bytes[..rng.Next(1, bytes.Length)];                               // torn
        return bytes;
    }

    private static IReadOnlyDictionary<string, object?> RandomDictionary(Random rng)
    {
        var d = new Dictionary<string, object?>(StringComparer.Ordinal);
        int n = rng.Next(0, 8);
        for (int i = 0; i < n; i++)
            d[KeyPool[rng.Next(KeyPool.Length)]] = rng.Next(9) switch
            {
                0 => Tricky[rng.Next(Tricky.Length)],
                1 => (ushort)rng.Next(ushort.MaxValue),
                2 => rng.Next(int.MinValue, int.MaxValue),
                3 => rng.NextDouble() * 1e6 - 5e5,
                4 => (float)rng.NextDouble(),
                5 => rng.Next(2) == 0,
                6 => null,
                7 => (decimal)rng.NextDouble() * 100m,
                _ => new DateTime(2026, 9, 23, 12, 0, 0, DateTimeKind.Utc),
            };
        return d;
    }

    private static SpanRecord RandomRecord(Random rng, int i)
    {
        var  traceId        = new TraceId((ulong)rng.NextInt64(), (ulong)rng.NextInt64());
        var  spanId         = new SpanId(rng.Next(10) == 0 ? 0 : (ulong)rng.NextInt64());
        var  parentId       = new SpanId(rng.Next(4) == 0 ? 0 : (ulong)rng.NextInt64());
        long start          = rng.NextInt64(long.MinValue, long.MaxValue);
        long duration       = rng.NextInt64(-1_000, 10_000_000_000);
        string name         = Tricky[rng.Next(Tricky.Length)] + i;
        string service      = Tricky[rng.Next(Tricky.Length)];
        var  kind           = (SpanKind)rng.Next(0, 9);
        var  status         = (SpanStatusCode)rng.Next(0, 5);
        var  http           = (short)rng.Next(short.MinValue, short.MaxValue);

        // Attributes is NEVER assigned on a record that carries a blob — not even to null. An
        // explicitly supplied map is the answer and the blob is never decoded over it, so
        // `Attributes = null` would make the reference read `{}` for every blob. Production builds
        // blob-only records (engine, reader); test fixtures build dictionary-only ones.
        return rng.Next(12) == 0
            ? new SpanRecord
            {
                TraceId = traceId, SpanId = spanId, ParentSpanId = parentId, StartTimeUnixNano = start,
                DurationNanos = duration, Name = name, ServiceName = service, Kind = kind, Status = status,
                HttpStatusCode = http, Attributes = RandomDictionary(rng),
            }
            : new SpanRecord
            {
                TraceId = traceId, SpanId = spanId, ParentSpanId = parentId, StartTimeUnixNano = start,
                DurationNanos = duration, Name = name, ServiceName = service, Kind = kind, Status = status,
                HttpStatusCode = http, AttributesBytes = RandomBlob(rng),
            };
    }

    // ── The comparison ───────────────────────────────────────────────────────

    private static byte[] Reference(SpanRecord s) => JsonSerializer.SerializeToUtf8Bytes(SpanDto.From(s), Host);

    private static byte[] HandWritten(SpanRecord s)
    {
        var buf = new ArrayBufferWriter<byte>();
        using (var w = new Utf8JsonWriter(buf, TraceDetailJson.WriterOptions(Host)))
            TraceDetailJson.WriteSpan(w, s);
        return buf.WrittenSpan.ToArray();
    }

    [Fact]
    public void Every_generated_span_is_written_exactly_as_the_dto_serialised_it()
    {
        const int PerCulture = 3_000;
        var saved = CultureInfo.CurrentCulture;
        int compared = 0, emptyMaps = 0, cultures = 0;
        try
        {
            foreach (var culture in Cultures())
            {
                cultures++;
                CultureInfo.CurrentCulture = culture;
                var rng = new Random(20260923 + cultures);
                for (int i = 0; i < PerCulture; i++)
                {
                    // Two records with the same content: SpanDto.From memoises a decode on the one it
                    // reads, and the hand-written path must never see a record that has one.
                    int seed = rng.Next();
                    var mine = RandomRecord(new Random(seed), i);
                    var old  = RandomRecord(new Random(seed), i);

                    byte[] got  = HandWritten(mine);
                    byte[] want = Reference(old);
                    if (!got.AsSpan().SequenceEqual(want))
                    {
                        output.WriteLine($"culture {culture.Name} seed {seed} blob "
                                       + Convert.ToHexString(mine.AttributesBytes.Span));
                        Assert.Equal(Encoding.UTF8.GetString(want), Encoding.UTF8.GetString(got));
                        Assert.Equal(want, got);
                    }
                    compared++;
                    if (want.AsSpan().EndsWith("\"attributes\":{}}"u8)) emptyMaps++;
                }
            }
        }
        finally { CultureInfo.CurrentCulture = saved; }

        output.WriteLine($"{compared:N0} spans over {cultures} cultures byte-identical; {emptyMaps:N0} with an empty map");
        Assert.True(emptyMaps > compared / 50,  "the generator stopped producing maps that decode to nothing");
        Assert.True(emptyMaps < compared / 2,   "the generator stopped producing maps that decode");
    }
}
