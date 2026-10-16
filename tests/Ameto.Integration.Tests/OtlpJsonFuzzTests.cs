using System.Text;
using Ameto.Ingestion;
using Ameto.Otel;
using MessagePack;

namespace Ameto.Integration.Tests;

/// <summary>
/// The JSON log parser's count of a record's property map is exact, and every map it hands over reads as
/// the pairs it counts — over bodies a generator makes: escapes of every kind, nested arrays and kvlists,
/// and the shapes the round-6 review found it wrong on. Each record's map at no limit is its true size L;
/// at a limit of L it must be taken, byte for byte that map, and at L - 1 refused with size L. Seeded, so a
/// failure names the seed that makes its body.
///
/// <para>Round-6 review R5-M8: a <c>"resource"</c> key given more than once in one resourceLogs element —
/// 2 756 records assembled past the limit and 2 999 refused with a wrong size over the review's 1 000
/// seeds.</para>
/// </summary>
public sealed class OtlpJsonFuzzTests
{
    private const int Seeds = 200;

    [Flags]
    public enum Shape
    {
        None        = 0,
        Escapes     = 1,
        Nested      = 2,
        DupResource = 4,
        /// <summary>Lone-surrogate escapes in string values.</summary>
        Surrogates     = 8,
        /// <summary>Lone-surrogate escapes in attribute keys.</summary>
        SurrogateKeys  = 16,
        /// <summary>Properties no parser knows, named with a lone-surrogate escape, at every level.</summary>
        SurrogateNames = 32,
    }

    [Theory]
    [InlineData(Shape.Escapes | Shape.Nested | Shape.DupResource)]
    [InlineData(Shape.Escapes | Shape.Nested | Shape.Surrogates | Shape.SurrogateKeys | Shape.SurrogateNames)]
    public void A_record_map_is_counted_to_the_byte_and_reads_as_its_pairs(Shape shape)
    {
        var failures = new List<string>();
        int records = 0;
        for (int seed = 0; seed < Seeds && failures.Count < 10; seed++)
        {
            byte[] body = new Generator(new Random(seed), shape).Request();
            var whole = new Sink(int.MaxValue);
            OtlpLogStreamParser.Parse(body, whole);
            records += whole.Calls.Count;

            for (int i = 0; i < whole.Calls.Count; i++)
                if (whole.Calls[i].Map is { } map && Malformed(map) is { } why)
                    failures.Add($"seed {seed}, record {i}: its map does not read as its pairs ({why})");

            foreach (int limit in whole.Calls.SelectMany(c => new[] { (int)Math.Min(int.MaxValue, c.Size), (int)Math.Min(int.MaxValue, c.Size) - 1 }).Where(l => l >= 0).Distinct())
            {
                var at = new Sink(limit);
                OtlpLogStreamParser.Parse(body, at);
                for (int i = 0; i < whole.Calls.Count; i++)
                {
                    long size = whole.Calls[i].Size;
                    var  got  = at.Calls[i];
                    if (size <= limit && got.Map is null)
                        failures.Add($"seed {seed}, record {i}: its {size} B map was refused at a limit of {limit} B, as {got.Size} B");
                    else if (size <= limit && !got.Map.AsSpan().SequenceEqual(whole.Calls[i].Map))
                        failures.Add($"seed {seed}, record {i}: its map at a limit of {limit} B is not the one it is with none");
                    else if (size > limit && got.Map is not null)
                        failures.Add($"seed {seed}, record {i}: its {size} B map was assembled at a limit of {limit} B, as {got.Size} B");
                    else if (size > limit && got.Size != size)
                        failures.Add($"seed {seed}, record {i}: its {size} B map was refused at a limit of {limit} B as {got.Size} B");
                }
            }
        }
        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));   // the first ten
        Assert.True(records > Seeds, $"setup: {records} records over {Seeds} seeds");
    }

    /// <summary>Why a msgpack map does not read as one value — or null when it does.</summary>
    private static string? Malformed(byte[] map)
    {
        try
        {
            var r = new MessagePackReader(map);
            if (r.NextMessagePackType != MessagePackType.Map) return "not a map";
            r.Skip();
            return r.End ? null : $"{r.Sequence.Length - r.Consumed} B after the map";
        }
        catch (Exception e) { return e.GetType().Name; }
    }

    /// <summary>A sink that keeps what it is handed: a map (assembled) or a size (refused).</summary>
    private sealed class Sink(int limit) : IOtlpLogSink
    {
        public readonly List<(long Size, byte[]? Map)> Calls = [];
        public int  MaxEventPayloadBytes => limit;
        public int  MaxServiceNameBytes  => int.MaxValue;
        public void ChargeScratch(long bytes) { }
        public void NotifyBatchEnqueued() { }

        public bool TryIngestRaw(long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, ReadOnlySpan<byte> msgpackProps,
                                 ulong traceHi, ulong traceLo, ulong spanId, ReadOnlySpan<byte> serviceUtf8)
        {
            Calls.Add((msgpackProps.Length, msgpackProps.ToArray()));
            return true;
        }

        public bool TryIngestOversized(long tsTicks, byte level, ReadOnlySpan<byte> templateUtf8, long payloadBytes,
                                       ulong traceHi, ulong traceLo, ulong spanId, ReadOnlySpan<byte> serviceUtf8, int serviceIdx)
        {
            Calls.Add((payloadBytes, null));
            return false;
        }
    }

    /// <summary>OTLP/JSON logs bodies, byte by byte, from a seed (after the round-5 review's decode fuzz).</summary>
    private sealed class Generator(Random rng, Shape shape)
    {
        private readonly List<byte> _o = [];
        private static readonly int[] Lengths = [0, 1, 30, 31, 32, 33, 254, 255, 256, 257, 4095, 4096, 4097, 65534, 65535, 65536];

        private void A(string s) => _o.AddRange(Encoding.UTF8.GetBytes(s));
        private void B(params byte[] b) => _o.AddRange(b);

        private void Str() => Str((shape & Shape.Surrogates) != 0);

        private void Key() => Str((shape & Shape.SurrogateKeys) != 0);

        /// <summary>A property no parser knows, named with a lone-surrogate escape (round-6 review, the LOW on L-10).</summary>
        private void UnknownName()
        {
            A(rng.Next(4) switch { 0 => "\"\\ud800", 1 => "\"\\udc00", 2 => "\"\\ud800\\ud800\\ud800", _ => "\"x\\udfff" });
            for (int i = rng.Next(0, 14); i > 0; i--) B((byte)('a' + rng.Next(26)));
            A("\":1");
        }

        /// <summary>Now and then, such a property, and a comma after it.</summary>
        private void MaybeUnknown()
        {
            if ((shape & Shape.SurrogateNames) != 0 && rng.Next(4) == 0) { UnknownName(); A(","); }
        }

        private void Str(bool surrogates)
        {
            int length = rng.Next(32) == 0 ? Lengths[rng.Next(Lengths.Length)] : rng.Next(0, 40);   // now and then across a str header's bounds
            A("\"");
            for (int i = 0; i < length; i++)
            {
                int k = rng.Next(20);
                if (k < 10) { B((byte)('a' + rng.Next(26))); continue; }
                if ((shape & Shape.Escapes) != 0 && k < 14)
                {
                    A(rng.Next(9) switch
                    {
                        0 => "\\n", 1 => "\\\"", 2 => "\\\\", 3 => "\\/", 4 => "\\u00e9", 5 => "\\u20ac",
                        6 => "\\ud83d\\ude00", 7 => "\\u0000", _ => "\\t",
                    });
                    continue;
                }
                if (surrogates && k < 16)
                {
                    A(rng.Next(4) switch { 0 => "\\ud800", 1 => "\\udfff", 2 => "\\ud83dx", _ => "\\ude00\\ud83d" });
                    continue;
                }
                switch (rng.Next(3)) { case 0: B(0xC3, 0xA9); break; case 1: B(0xE2, 0x82, 0xAC); break; default: B(0xF0, 0x9F, 0x98, 0x80); break; }
            }
            A("\"");
        }

        private void Any(int depth)
        {
            switch (rng.Next((shape & Shape.Nested) != 0 && depth < 4 ? 8 : 6))
            {
                case 0: case 5: A("{"); MaybeUnknown(); A("\"stringValue\":"); Str(); A("}"); break;
                case 1: A(rng.Next(2) == 0 ? "{\"boolValue\":true}" : "{\"boolValue\":false}"); break;
                case 2: A(rng.Next(2) == 0 ? $"{{\"intValue\":\"{rng.NextInt64(long.MinValue, long.MaxValue)}\"}}" : $"{{\"intValue\":{rng.Next(-200000, 200000)}}}"); break;
                case 3: A($"{{\"doubleValue\":{(rng.NextDouble() * 1e6).ToString(System.Globalization.CultureInfo.InvariantCulture)}}}"); break;
                case 4: A("{}"); break;
                case 6:
                {
                    int n = depth < 2 && rng.Next(3) == 0 ? 15 + rng.Next(3) : rng.Next(0, 4);   // past a fixarray or fixmap, near the top
                    A("{\"arrayValue\":{\"values\":[");
                    for (int i = 0; i < n; i++) { if (i > 0) A(","); Any(depth + 1); }
                    A("]}}");
                    break;
                }
                default:
                {
                    int n = depth < 2 && rng.Next(3) == 0 ? 15 + rng.Next(3) : rng.Next(0, 4);   // past a fixarray or fixmap, near the top
                    A("{\"kvlistValue\":{\"values\":[");
                    for (int i = 0; i < n; i++) { if (i > 0) A(","); KeyValue(depth + 1); }
                    A("]}}");
                    break;
                }
            }
        }

        private void KeyValue(int depth)
        {
            if (rng.Next(12) == 0) { A("{"); MaybeUnknown(); A("\"key\":"); Key(); A("}"); return; }   // a key with no value
            A("{"); MaybeUnknown(); A("\"key\":"); Key(); A(",\"value\":"); Any(depth); A("}");
        }

        private void Attributes(int n)
        {
            A("[");
            for (int i = 0; i < n; i++) { if (i > 0) A(","); KeyValue(0); }
            A("]");
        }

        private void Resource()
        {
            A("\"resource\":{"); MaybeUnknown(); A("\"attributes\":[");
            int n = rng.Next(4) == 0 ? 14 + rng.Next(4) : rng.Next(0, 4);
            bool first = true;
            if (rng.Next(2) == 0) { A("{\"key\":\"service.name\",\"value\":{\"stringValue\":"); Str(); A("}}"); first = false; }
            for (int i = 0; i < n; i++) { if (!first) A(","); first = false; KeyValue(0); }
            A("]}");
        }

        private void Record()
        {
            var parts = new List<Action>();
            if (rng.Next(2) == 0) parts.Add(() => A($"\"timeUnixNano\":\"{rng.NextInt64(1, long.MaxValue)}\""));
            if (rng.Next(2) == 0) parts.Add(() => A($"\"severityNumber\":{rng.Next(0, 25)}"));
            if (rng.Next(2) == 0) parts.Add(() => { A("\"body\":{\"stringValue\":"); Str(); A("}"); });
            int n = rng.Next(4) == 0 ? 14 + rng.Next(4) : rng.Next(0, 5);
            if (n > 0) parts.Add(() => { A("\"attributes\":"); Attributes(n); });
            if (rng.Next(2) == 0) parts.Add(() => A($"\"traceId\":\"{Convert.ToHexString(Bytes(16)).ToLowerInvariant()}\""));
            if (rng.Next(2) == 0) parts.Add(() => A($"\"spanId\":\"{Convert.ToHexString(Bytes(8)).ToLowerInvariant()}\""));
            if ((shape & Shape.SurrogateNames) != 0 && rng.Next(3) == 0) parts.Add(UnknownName);
            for (int i = parts.Count - 1; i > 0; i--) { int j = rng.Next(i + 1); (parts[i], parts[j]) = (parts[j], parts[i]); }
            A("{");
            for (int i = 0; i < parts.Count; i++) { if (i > 0) A(","); parts[i](); }
            A("}");
        }

        private byte[] Bytes(int n) { var b = new byte[n]; rng.NextBytes(b); return b; }

        public byte[] Request()
        {
            _o.Clear();
            A("{\"resourceLogs\":[");
            for (int block = 0, blocks = rng.Next(1, 3); block < blocks; block++)
            {
                if (block > 0) A(",");
                A("{");
                MaybeUnknown();
                Resource(); A(",");
                // A "resource" key given again (or more), before the records: JSON allows it, OTLP does not send it.
                if ((shape & Shape.DupResource) != 0)
                    for (int again = rng.Next(3) == 0 ? rng.Next(1, 4) : 0; again > 0; again--) { Resource(); A(","); }
                A("\"scopeLogs\":[{\"logRecords\":[");
                for (int r = 0, n = rng.Next(1, 4); r < n; r++) { if (r > 0) A(","); Record(); }
                A("]}]}");
            }
            A("]}");
            return _o.ToArray();
        }
    }
}
