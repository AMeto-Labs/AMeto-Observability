using System.Globalization;
using System.Runtime.InteropServices;
using Ameto.Metrics;

namespace Ameto.Otel;

/// <summary>
/// Parses OTLP/protobuf metrics straight into <see cref="MetricIngestItem"/>s — the
/// protobuf counterpart of the streaming JSON parsers, and the path real OTel SDK
/// exporters actually use.
///
/// <para>Replaces decode-to-DOM-then-map for this content type. The old route cost three
/// avoidable things per data point: a <c>CodedInputStream</c> plus a payload copy for every
/// nested message (data point, attribute, value — see <see cref="ProtoReader"/>), a full
/// OTLP object graph that existed only to be walked once, and — the quiet one — a
/// number→string→number round trip, because the shared DOM models type wire integers as
/// <c>string</c> for OTLP/JSON. <c>ReadFixed64().ToString()</c> in the decoder became
/// <c>long.TryParse</c> in the mapper, once per timestamp, per int value, per histogram
/// count and per bucket.</para>
///
/// <para><b>No label string is materialised twice.</b> Keys, values, metric names and units are
/// resolved from their UTF-8 bytes through a <see cref="MetricLabelInterner"/>, which answers a
/// string the process already holds with that instance and allocates nothing; and a point whose
/// labels are all pooled gets the label set built the first time those labels were seen. A
/// 500-point batch decoded 8 000 label strings of which 26 were distinct — 99.7 % of them copies,
/// ~900 B a point, retained into gen2 by every structure that keys on a label set. Past the
/// interner's bounds a string or a label set is simply built fresh, as before; nothing is
/// refused.</para>
///
/// Semantics are identical to <c>OtlpProtoDecoder</c> + <c>OtlpMetricMapper</c>;
/// <c>OtlpMetricProtoParityTests</c> pins that.
/// </summary>
public static class OtlpMetricProtoParser
{
    /// <summary>Per-call scratch state — keeps the recursive readers to two parameters.</summary>
    private sealed class ParseState
    {
        public readonly List<MetricIngestItem> Result = [];

        /// <summary>
        /// The point's labels and the resource's — the builder the JSON mapper uses too, so the rule
        /// that decides a series' identity is written once for both encodings.
        /// </summary>
        public readonly MetricLabelSetBuilder Labels;

        /// <summary>Histogram scratch, so a point's arrays are allocated once at their exact size.</summary>
        public readonly List<long>   Counts = new(32);
        public readonly List<double> Bounds = new(32);

        /// <summary>
        /// The bounds array of the previous histogram point in this batch. Every point of an
        /// instrument carries the same bounds and nothing downstream mutates them (the hot tier
        /// keeps its series' first array, the log and the writer only read), so a point whose
        /// bounds match bit for bit shares the array instead of allocating its own.
        /// </summary>
        public double[]? LastBounds;

        public ParseState(MetricLabelInterner interner) => Labels = new MetricLabelSetBuilder(interner);

        public InternedText Intern(ReadOnlySpan<byte> utf8) => Labels.Intern(utf8);
    }

    public static List<MetricIngestItem> Parse(ReadOnlySpan<byte> payload) =>
        Parse(payload, MetricLabelInterner.Shared);

    /// <summary>
    /// <see cref="Parse(ReadOnlySpan{byte})"/> against a given interner — the process-wide
    /// <see cref="MetricLabelInterner.Shared"/> in production; tests pass a small one to reach its
    /// bounds.
    /// </summary>
    public static List<MetricIngestItem> Parse(ReadOnlySpan<byte> payload, MetricLabelInterner interner)
    {
        var st = new ParseState(interner);
        var r  = new ProtoReader(payload);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag == 10) ReadResourceMetrics(r.ReadLengthDelimited(), st);   // field 1
            else r.SkipField(tag);
        }
        return st.Result;
    }

    private static void ReadResourceMetrics(ReadOnlySpan<byte> bytes, ParseState st)
    {
        // Resource first: the wire format does not guarantee field order, and the service
        // name / resource labels are stamped onto every point below.
        st.Labels.BeginResource();

        var pass1 = new ProtoReader(bytes);
        uint tag;
        while ((tag = pass1.ReadTag()) != 0)
        {
            if (tag == 10) ReadResource(pass1.ReadLengthDelimited(), st);
            else pass1.SkipField(tag);
        }

        var pass2 = new ProtoReader(bytes);
        while ((tag = pass2.ReadTag()) != 0)
        {
            if (tag == 18) ReadScopeMetrics(pass2.ReadLengthDelimited(), st);
            else pass2.SkipField(tag);
        }
    }

    /// <summary>
    /// Splits the resource attributes into the dedicated service name and the label set
    /// stamped onto every point. Exclusions mirror <c>OtlpMetricMapper</c>: the SDK's own
    /// <c>telemetry.*</c> self-description, and <c>service.instance.id</c>, which is a fresh
    /// GUID per process start and would fork every series on every restart. The key is
    /// decided on before the value is resolved, so an excluded value — the per-restart GUID
    /// above all — never takes a slot in the interner.
    /// </summary>
    private static void ReadResource(ReadOnlySpan<byte> bytes, ParseState st)
    {
        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag != 10) { r.SkipField(tag); continue; }                       // field 1: attributes
            if (!TryReadKeyValue(r.ReadLengthDelimited(), out var keyUtf8, out var value)) continue;

            if (keyUtf8.SequenceEqual("service.name"u8))
            {
                // Mapper parity: a string only, and the first one (see SetServiceName).
                if (value.IsString && !st.Labels.HasServiceName) st.Labels.SetServiceName(st.Intern(value.Utf8));
                continue;
            }
            if (keyUtf8.SequenceEqual("service.instance.id"u8)) continue;
            if (keyUtf8.StartsWith("telemetry.sdk."u8) ||
                keyUtf8.StartsWith("telemetry.distro."u8)) continue;

            if (value.TryFormat(st, out var sv))
                st.Labels.AddResourceLabel(st.Intern(keyUtf8), sv);
        }
    }

    private static void ReadScopeMetrics(ReadOnlySpan<byte> bytes, ParseState st)
    {
        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag == 18) ReadMetric(r.ReadLengthDelimited(), st);              // field 2
            else r.SkipField(tag);
        }
    }

    private static void ReadMetric(ReadOnlySpan<byte> bytes, ParseState st)
    {
        // Name/unit precede the data in every OTLP encoder, but the wire format allows any
        // order — collect the scalars first, then walk the point sets with them in hand.
        // Both are interned: every point of the metric and every series key built from it
        // holds them for the series' life.
        string? name = null;
        string  unit = string.Empty;

        var pass1 = new ProtoReader(bytes);
        uint tag;
        while ((tag = pass1.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: name = st.Intern(pass1.ReadLengthDelimited()).Text; break;   // field 1: name
                case 26: unit = st.Intern(pass1.ReadLengthDelimited()).Text; break;   // field 3: unit
                default: pass1.SkipField(tag); break;
            }
        }
        if (name is null) return;

        var pass2 = new ProtoReader(bytes);
        while ((tag = pass2.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 42: ReadNumberPoints(pass2.ReadLengthDelimited(), name, unit, MetricKind.Gauge, st); break; // gauge
                case 58: ReadSum(pass2.ReadLengthDelimited(), name, unit, st);                            break; // sum
                case 74: ReadHistogramPoints(pass2.ReadLengthDelimited(), name, unit, st);                break; // histogram
                default: pass2.SkipField(tag); break;
            }
        }
    }

    private static void ReadSum(ReadOnlySpan<byte> bytes, string name, string unit, ParseState st)
    {
        bool monotonic = false;
        var pass1 = new ProtoReader(bytes);
        uint tag;
        while ((tag = pass1.ReadTag()) != 0)
        {
            if (tag == 24) monotonic = pass1.ReadVarint() != 0;                  // field 3: is_monotonic
            else pass1.SkipField(tag);
        }

        ReadNumberPoints(bytes, name, unit, monotonic ? MetricKind.Counter : MetricKind.Gauge, st);
    }

    private static void ReadNumberPoints(
        ReadOnlySpan<byte> bytes, string name, string unit, MetricKind kind, ParseState st)
    {
        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag != 10) { r.SkipField(tag); continue; }                       // field 1: data_points

            var dp = r.ReadLengthDelimited();
            long   ts = 0, asInt = 0;
            double value = 0;
            bool   haveDouble = false, haveInt = false;

            var p = new ProtoReader(dp);
            uint t;
            while ((t = p.ReadTag()) != 0)
            {
                switch (t)
                {
                    case 25: ts = (long)p.ReadFixed64(); break;                    // field 3: time_unix_nano
                    case 33: value = p.ReadDouble(); haveDouble = true; break;     // field 4: as_double
                    case 49: asInt = (long)p.ReadFixed64(); haveInt = true; break; // field 6: as_int (sfixed64)
                    default: p.SkipField(t); break;
                }
            }

            st.Result.Add(new MetricIngestItem
            {
                Name              = name,
                Unit              = unit,
                Kind              = kind,
                Labels            = BuildLabels(dp, 7, st),
                TimestampUnixNano = ts,
                ScalarValue       = haveDouble ? value : (haveInt ? asInt : 0),
            });
        }
    }

    private static void ReadHistogramPoints(ReadOnlySpan<byte> bytes, string name, string unit, ParseState st)
    {
        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag != 10) { r.SkipField(tag); continue; }                       // field 1: data_points

            var dp = r.ReadLengthDelimited();
            long   ts = 0, count = 0;
            double sum = 0;

            // The arrays are collected into reused scratch lists and allocated once, at their
            // exact length: growing a fresh List by doubling and then copying it out cost four
            // arrays per field per point. The flags keep "the field was present but empty" (an
            // empty array) apart from "absent" (null), as the lists-on-demand shape did.
            var counts = st.Counts;
            var bounds = st.Bounds;
            counts.Clear();
            bounds.Clear();
            bool haveCounts = false, haveBounds = false;
            List<MetricExemplar>? exemplars = null;

            var p = new ProtoReader(dp);
            uint t;
            while ((t = p.ReadTag()) != 0)
            {
                switch (t)
                {
                    case 25: ts    = (long)p.ReadFixed64(); break;               // field 3: time_unix_nano
                    case 33: count = (long)p.ReadFixed64(); break;               // field 4: count (SDK emits fixed64)
                    case 41: sum   = p.ReadDouble();        break;               // field 5: sum
                    case 48: haveCounts = true; counts.Add((long)p.ReadVarint());  break;  // field 6 unpacked varint
                    case 49: haveCounts = true; counts.Add((long)p.ReadFixed64()); break;  // field 6 unpacked fixed64
                    case 50:                                                     // field 6 packed
                    {
                        var packed = p.ReadLengthDelimited();
                        haveCounts = true;
                        // N x fixed64 is always a multiple of 8; varints almost never are —
                        // the same auto-detection the DOM decoder used.
                        bool asFixed = packed.Length % 8 == 0;
                        var pr = new ProtoReader(packed);
                        while (!pr.End) counts.Add((long)(asFixed ? pr.ReadFixed64() : pr.ReadVarint()));
                        break;
                    }
                    case 57: haveBounds = true; bounds.Add(p.ReadDouble()); break;         // field 7 unpacked
                    case 58:                                                     // field 7 packed double
                    {
                        var packed = p.ReadLengthDelimited();
                        haveBounds = true;
                        var pr = new ProtoReader(packed);
                        while (!pr.End) bounds.Add(pr.ReadDouble());
                        break;
                    }
                    case 66:                                                     // field 8: exemplars
                    {
                        var ex = ReadExemplar(p.ReadLengthDelimited());
                        if (ex is not null) (exemplars ??= []).Add(ex);
                        break;
                    }
                    default: p.SkipField(t); break;
                }
            }

            st.Result.Add(new MetricIngestItem
            {
                Name              = name,
                Unit              = unit,
                Kind              = MetricKind.Histogram,
                Labels            = BuildLabels(dp, 9, st),
                TimestampUnixNano = ts,
                HistogramCount    = count,
                HistogramSum      = sum,
                BucketBounds      = haveBounds ? SharedBounds(st) : null,
                BucketCounts      = haveCounts ? counts.ToArray() : null,
                Exemplars         = exemplars?.ToArray(),
            });
        }
    }

    /// <summary>
    /// This point's bounds: the previous point's array when the values match BIT FOR BIT (so
    /// <c>-0.0</c> never stands in for <c>0.0</c>, nor one NaN payload for another), else a new
    /// exact-length array that becomes the one the next point is compared with.
    /// </summary>
    private static double[] SharedBounds(ParseState st)
    {
        var scratch = CollectionsMarshal.AsSpan(st.Bounds);
        var last    = st.LastBounds;
        if (last is not null &&
            MemoryMarshal.Cast<double, long>(last.AsSpan()).SequenceEqual(MemoryMarshal.Cast<double, long>(scratch)))
            return last;

        var fresh = scratch.ToArray();
        st.LastBounds = fresh;
        return fresh;
    }

    /// <summary>Only exemplars carrying a trace link are useful — the rest are dropped (mapper parity).</summary>
    private static MetricExemplar? ReadExemplar(ReadOnlySpan<byte> bytes)
    {
        long   ts = 0, asInt = 0;
        double value = 0;
        bool   haveDouble = false, haveInt = false;
        string? traceId = null, spanId = null;

        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 17: ts = (long)r.ReadFixed64(); break;                      // field 2: time_unix_nano
                case 25: value = r.ReadDouble(); haveDouble = true; break;       // field 3: as_double
                case 34: spanId  = Hex(r.ReadLengthDelimited()); break;          // field 4: span_id
                case 42: traceId = Hex(r.ReadLengthDelimited()); break;          // field 5: trace_id
                case 49: asInt = (long)r.ReadFixed64(); haveInt = true; break;   // field 6: as_int
                default: r.SkipField(tag); break;
            }
        }

        if (string.IsNullOrEmpty(traceId)) return null;
        return new MetricExemplar
        {
            TimestampUnixNano = ts,
            Value             = haveDouble ? value : (haveInt ? asInt : 0),
            TraceId           = traceId,
            SpanId            = spanId ?? string.Empty,
        };
    }

    /// <summary>
    /// Builds the point's label set through the shared <see cref="MetricLabelSetBuilder"/>: service
    /// name, the point's own attributes, then any resource label not already present. Re-walks the
    /// data point for just the attribute fields — a second pass over a span already in L1 is cheaper
    /// than buffering attributes during the first.
    /// </summary>
    private static LabelSet BuildLabels(ReadOnlySpan<byte> dp, int attrField, ParseState st)
    {
        var labels = st.Labels;
        labels.BeginPoint();

        uint attrTag = ((uint)attrField << 3) | 2;
        var r = new ProtoReader(dp);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag != attrTag) { r.SkipField(tag); continue; }
            if (!TryReadKeyValue(r.ReadLengthDelimited(), out var keyUtf8, out var value)) continue;
            if (value.TryFormat(st, out var sv)) labels.Add(st.Intern(keyUtf8), sv);
        }

        return labels.Build();
    }

    // ── KeyValue / AnyValue ───────────────────────────────────────────────────

    /// <summary>
    /// A decoded OTLP AnyValue, kept as its wire bytes so callers that only need to know
    /// "was it a string?" (the service-name rule) — or that skip the attribute outright — do
    /// not force a conversion, and one that keeps it resolves it through the interner.
    /// </summary>
    private readonly ref struct AnyValue
    {
        public readonly ReadOnlySpan<byte> Utf8;
        public readonly long    Int;
        public readonly double  Double;
        public readonly bool    Bool;
        public readonly byte    Which;   // 0 none, 1 string, 2 bool, 3 int, 4 double

        private AnyValue(ReadOnlySpan<byte> utf8, long i, double d, bool b, byte which)
        { Utf8 = utf8; Int = i; Double = d; Bool = b; Which = which; }

        public static AnyValue Str(ReadOnlySpan<byte> s) => new(s, 0, 0, false, 1);
        public static AnyValue Boolean(bool b) => new(default, 0, 0, b, 2);
        public static AnyValue Integer(long i) => new(default, i, 0, false, 3);
        public static AnyValue Dbl(double d)   => new(default, 0, d, false, 4);

        public bool IsString => Which == 1;

        /// <summary>
        /// Label text through the interner, or false when the value carries nothing usable — same
        /// precedence and the same text as the mapper's FormatLabelValue (string, int, double,
        /// bool). Numbers are formatted as UTF-8 into the stack and interned from there, so a
        /// repeating number allocates nothing either: <c>IUtf8SpanFormattable</c> with the
        /// invariant culture writes exactly the characters <c>ToString(InvariantCulture)</c>
        /// returns.
        /// </summary>
        public bool TryFormat(ParseState st, out InternedText text)
        {
            Span<byte> buf = stackalloc byte[32];   // long ≤ 20, shortest round-trip double ≤ 24
            int n;
            switch (Which)
            {
                case 1:
                    text = st.Intern(Utf8);
                    return true;
                case 3:
                    if (!Int.TryFormat(buf, out n, default, CultureInfo.InvariantCulture)) break;
                    text = st.Intern(buf[..n]);
                    return true;
                case 4:
                    if (!Double.TryFormat(buf, out n, default, CultureInfo.InvariantCulture)) break;
                    text = st.Intern(buf[..n]);
                    return true;
                case 2:
                    text = st.Intern(Bool ? "true"u8 : "false"u8);
                    return true;
                default:
                    text = default;
                    return false;
            }

            // A number whose invariant text outgrows the buffer — not reachable for long or
            // double, kept so a wrong size can only cost an allocation.
            text = new InternedText(Which == 3 ? Int.ToString(CultureInfo.InvariantCulture)
                                               : Double.ToString(CultureInfo.InvariantCulture), -1);
            return true;
        }
    }

    private static bool TryReadKeyValue(
        ReadOnlySpan<byte> bytes, out ReadOnlySpan<byte> key, out AnyValue value)
    {
        key   = default;
        value = default;
        bool haveKey = false;

        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: key = r.ReadLengthDelimited(); haveKey = true; break;   // field 1: key
                case 18: value = ReadAnyValue(r.ReadLengthDelimited()); break;   // field 2: value
                default: r.SkipField(tag); break;
            }
        }
        return haveKey;
    }

    private static AnyValue ReadAnyValue(ReadOnlySpan<byte> bytes)
    {
        AnyValue result = default;
        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: result = AnyValue.Str(r.ReadLengthDelimited());   break; // field 1: string_value
                case 16: result = AnyValue.Boolean(r.ReadVarint() != 0);   break; // field 2: bool_value
                case 24: result = AnyValue.Integer((long)r.ReadVarint());  break; // field 3: int_value
                case 33: result = AnyValue.Dbl(r.ReadDouble());            break; // field 4: double_value
                default: r.SkipField(tag); break;
            }
        }
        return result;
    }

    private static string Hex(ReadOnlySpan<byte> bytes) =>
        bytes.IsEmpty ? string.Empty : Convert.ToHexStringLower(bytes);
}
