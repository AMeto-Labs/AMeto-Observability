using System.Globalization;
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
/// Semantics are identical to <c>OtlpProtoDecoder</c> + <c>OtlpMetricMapper</c>;
/// <c>OtlpMetricProtoParityTests</c> pins that.
/// </summary>
public static class OtlpMetricProtoParser
{
    /// <summary>Per-call scratch state — keeps the recursive readers to two parameters.</summary>
    private sealed class ParseState
    {
        public readonly List<MetricIngestItem> Result = [];
        /// <summary>Refilled per data point; LabelSet copies the pairs into its own array.</summary>
        public readonly List<KeyValuePair<string, string>> Labels = new(16);

        public string? ServiceName;
        public List<KeyValuePair<string, string>>? ResourceLabels;
    }

    public static List<MetricIngestItem> Parse(ReadOnlySpan<byte> payload)
    {
        var st = new ParseState();
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
        st.ServiceName    = null;
        st.ResourceLabels = null;

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
    /// GUID per process start and would fork every series on every restart.
    /// </summary>
    private static void ReadResource(ReadOnlySpan<byte> bytes, ParseState st)
    {
        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag != 10) { r.SkipField(tag); continue; }                       // field 1: attributes
            if (!TryReadKeyValue(r.ReadLengthDelimited(), st, out var key, out var value)) continue;

            if (key == "service.name")
            {
                if (value.IsString) st.ServiceName = value.Text;                 // mapper parity: string only
                continue;
            }
            if (key == "service.instance.id") continue;
            if (key.StartsWith("telemetry.sdk.",    StringComparison.Ordinal) ||
                key.StartsWith("telemetry.distro.", StringComparison.Ordinal)) continue;

            string? sv = value.Format();
            if (sv is not null)
                (st.ResourceLabels ??= []).Add(new KeyValuePair<string, string>(key, sv));
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
        string? name = null;
        string  unit = string.Empty;

        var pass1 = new ProtoReader(bytes);
        uint tag;
        while ((tag = pass1.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: name = pass1.ReadString(); break;   // field 1: name
                case 26: unit = pass1.ReadString(); break;   // field 3: unit
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
            List<long>?           counts    = null;
            List<double>?         bounds    = null;
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
                    case 48: (counts ??= []).Add((long)p.ReadVarint());  break;  // field 6 unpacked varint
                    case 49: (counts ??= []).Add((long)p.ReadFixed64()); break;  // field 6 unpacked fixed64
                    case 50:                                                     // field 6 packed
                    {
                        var packed = p.ReadLengthDelimited();
                        counts ??= [];
                        // N x fixed64 is always a multiple of 8; varints almost never are —
                        // the same auto-detection the DOM decoder used.
                        bool asFixed = packed.Length % 8 == 0;
                        var pr = new ProtoReader(packed);
                        while (!pr.End) counts.Add((long)(asFixed ? pr.ReadFixed64() : pr.ReadVarint()));
                        break;
                    }
                    case 57: (bounds ??= []).Add(p.ReadDouble()); break;         // field 7 unpacked
                    case 58:                                                     // field 7 packed double
                    {
                        var packed = p.ReadLengthDelimited();
                        bounds ??= [];
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
                BucketBounds      = bounds?.ToArray(),
                BucketCounts      = counts?.ToArray(),
                Exemplars         = exemplars?.ToArray(),
            });
        }
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
    /// Builds the point's label set: service name, the point's own attributes, then any
    /// resource label not already present (point attributes win on key collision).
    /// Re-walks the data point for just the attribute fields — a second pass over a span
    /// already in L1 is cheaper than buffering attributes during the first.
    /// </summary>
    private static LabelSet BuildLabels(ReadOnlySpan<byte> dp, int attrField, ParseState st)
    {
        var pairs = st.Labels;
        pairs.Clear();
        if (st.ServiceName is not null)
            pairs.Add(new KeyValuePair<string, string>("service.name", st.ServiceName));

        uint attrTag = ((uint)attrField << 3) | 2;
        var r = new ProtoReader(dp);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            if (tag != attrTag) { r.SkipField(tag); continue; }
            if (!TryReadKeyValue(r.ReadLengthDelimited(), st, out var key, out var value)) continue;
            string? sv = value.Format();
            if (sv is not null) pairs.Add(new KeyValuePair<string, string>(key, sv));
        }

        var res = st.ResourceLabels;
        if (res is not null)
            for (int i = 0; i < res.Count; i++)
            {
                bool exists = false;
                for (int j = 0; j < pairs.Count; j++)
                    if (pairs[j].Key == res[i].Key) { exists = true; break; }
                if (!exists) pairs.Add(res[i]);
            }

        return pairs.Count == 0 ? LabelSet.Empty : new LabelSet(pairs);
    }

    // ── KeyValue / AnyValue ───────────────────────────────────────────────────

    /// <summary>
    /// A decoded OTLP AnyValue, kept unformatted so callers that only need to know
    /// "was it a string?" (the service-name rule) do not force a conversion.
    /// </summary>
    private readonly struct AnyValue
    {
        public readonly string? Text;
        public readonly long    Int;
        public readonly double  Double;
        public readonly bool    Bool;
        public readonly byte    Which;   // 0 none, 1 string, 2 bool, 3 int, 4 double

        private AnyValue(string? text, long i, double d, bool b, byte which)
        { Text = text; Int = i; Double = d; Bool = b; Which = which; }

        public static AnyValue Str(string s)   => new(s, 0, 0, false, 1);
        public static AnyValue Boolean(bool b) => new(null, 0, 0, b, 2);
        public static AnyValue Integer(long i) => new(null, i, 0, false, 3);
        public static AnyValue Dbl(double d)   => new(null, 0, d, false, 4);
        public static AnyValue None            => default;

        public bool IsString => Which == 1;

        /// <summary>Label text, or null when the value carries nothing usable — same
        /// precedence as the mapper's FormatLabelValue (string, int, double, bool).</summary>
        public string? Format() => Which switch
        {
            1 => Text,
            3 => Int.ToString(CultureInfo.InvariantCulture),
            4 => Double.ToString(CultureInfo.InvariantCulture),
            2 => Bool ? "true" : "false",
            _ => null,
        };
    }

    private static bool TryReadKeyValue(
        ReadOnlySpan<byte> bytes, ParseState st, out string key, out AnyValue value)
    {
        key   = string.Empty;
        value = AnyValue.None;
        bool haveKey = false;

        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: key = r.ReadString(); haveKey = true; break;             // field 1: key
                case 18: value = ReadAnyValue(r.ReadLengthDelimited(), st); break; // field 2: value
                default: r.SkipField(tag); break;
            }
        }
        return haveKey;
    }

    private static AnyValue ReadAnyValue(ReadOnlySpan<byte> bytes, ParseState st)
    {
        var result = AnyValue.None;
        var r = new ProtoReader(bytes);
        uint tag;
        while ((tag = r.ReadTag()) != 0)
        {
            switch (tag)
            {
                case 10: result = AnyValue.Str(r.ReadString());            break; // field 1: string_value
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
