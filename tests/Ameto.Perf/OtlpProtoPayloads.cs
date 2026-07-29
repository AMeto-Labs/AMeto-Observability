using Google.Protobuf;

namespace Ameto.Perf;

/// <summary>
/// Hand-built OTLP/protobuf payloads for the parity tests and the throughput probe.
/// Encoded with the field numbers and wire types the OTel .NET SDK emits (notably:
/// histogram count as fixed64 and bucket_counts packed, which is what the exporter
/// actually writes rather than what a naive reading of the proto would suggest).
/// </summary>
internal static class OtlpProtoPayloads
{
    public const int Metrics    = 20;   // distinct instrument names per export
    public const int PointsEach = 25;   // label combinations per instrument
    public const int Buckets    = 15;   // histogram buckets (OTel default boundaries)
    public const int HistoEvery = 3;    // every 3rd instrument is a histogram

    // ── Wire-format helpers ───────────────────────────────────────────────────

    public static byte[] Msg(Action<CodedOutputStream> body)
    {
        using var ms = new MemoryStream();
        var cos = new CodedOutputStream(ms);
        body(cos);
        cos.Flush();
        return ms.ToArray();
    }

    public static void Nested(CodedOutputStream cos, int field, byte[] child)
    {
        cos.WriteTag(field, WireFormat.WireType.LengthDelimited);
        cos.WriteBytes(ByteString.CopyFrom(child));
    }

    private static byte[] StringAttr(string key, string value) => Msg(c =>
    {
        c.WriteTag(1, WireFormat.WireType.LengthDelimited); c.WriteString(key);
        Nested(c, 2, Msg(v => { v.WriteTag(1, WireFormat.WireType.LengthDelimited); v.WriteString(value); }));
    });

    private static byte[] IntAttr(string key, long value) => Msg(c =>
    {
        c.WriteTag(1, WireFormat.WireType.LengthDelimited); c.WriteString(key);
        Nested(c, 2, Msg(v => { v.WriteTag(3, WireFormat.WireType.Varint); v.WriteInt64(value); }));
    });

    private static byte[] BoolAttr(string key, bool value) => Msg(c =>
    {
        c.WriteTag(1, WireFormat.WireType.LengthDelimited); c.WriteString(key);
        Nested(c, 2, Msg(v => { v.WriteTag(2, WireFormat.WireType.Varint); v.WriteBool(value); }));
    });

    private static byte[] DoubleAttr(string key, double value) => Msg(c =>
    {
        c.WriteTag(1, WireFormat.WireType.LengthDelimited); c.WriteString(key);
        Nested(c, 2, Msg(v => { v.WriteTag(4, WireFormat.WireType.Fixed64); v.WriteDouble(value); }));
    });

    /// <summary>Wraps scope-level content into a full ExportMetricsServiceRequest.</summary>
    private static byte[] Request(byte[]? resource, byte[] scopeMetrics) => Msg(c =>
        Nested(c, 1, Msg(rm =>
        {
            if (resource is not null) Nested(rm, 1, resource);
            Nested(rm, 2, scopeMetrics);
        })));

    private static byte[] StandardResource() => Msg(c =>
    {
        Nested(c, 1, StringAttr("service.name", "Etisalat.API"));
        Nested(c, 1, StringAttr("deployment.environment", "Test"));
        Nested(c, 1, StringAttr("host.name", "sandbox-kz02"));
        // Excluded by the mapper — present so the parity test covers the exclusion rules.
        Nested(c, 1, StringAttr("service.instance.id", "c0ffee00-dead-beef-0000-000000000001"));
        Nested(c, 1, StringAttr("telemetry.sdk.name", "opentelemetry"));
        Nested(c, 1, StringAttr("telemetry.distro.version", "1.9.0"));
    });

    // ── Payloads ──────────────────────────────────────────────────────────────

    /// <summary>Realistic export: 20 instruments x 25 series, every 3rd a 15-bucket histogram.</summary>
    public static byte[] Metrics_Realistic() => Request(StandardResource(), Msg(c =>
    {
        for (int m = 0; m < Metrics; m++)
        {
            bool histo = m % HistoEvery == 0;
            int mi = m;
            Nested(c, 2, Msg(metric =>
            {
                metric.WriteTag(1, WireFormat.WireType.LengthDelimited);
                metric.WriteString(histo ? $"http.server.request.duration.{mi}" : $"process.runtime.counter.{mi}");
                metric.WriteTag(3, WireFormat.WireType.LengthDelimited);
                metric.WriteString(histo ? "ms" : "1");

                if (histo) Nested(metric, 9, Msg(h =>
                {
                    for (int p = 0; p < PointsEach; p++) Nested(h, 1, HistogramPoint(p));
                }));
                else Nested(metric, 7, Msg(s =>
                {
                    for (int p = 0; p < PointsEach; p++) Nested(s, 1, NumberPoint(p));
                    s.WriteTag(3, WireFormat.WireType.Varint); s.WriteBool(true);   // is_monotonic
                }));
            }));
        }
    }));

    public static byte[] EmptyMetrics() => Msg(_ => { });

    /// <summary>A single gauge point with no attributes and no resource message at all.</summary>
    public static byte[] BareGauge() => Request(null, Msg(c =>
        Nested(c, 2, Msg(metric =>
        {
            metric.WriteTag(1, WireFormat.WireType.LengthDelimited);
            metric.WriteString("process.cpu.utilization");
            Nested(metric, 5, Msg(g => Nested(g, 1, Msg(dp =>          // field 5: gauge
            {
                dp.WriteTag(3, WireFormat.WireType.Fixed64); dp.WriteFixed64(1_785_300_060_000_000_000UL);
                dp.WriteTag(4, WireFormat.WireType.Fixed64); dp.WriteDouble(0.42);
            }))));
        }))));

    /// <summary>One non-monotonic sum point carrying every AnyValue type as an attribute.</summary>
    public static byte[] MixedAttributeTypes() => Request(StandardResource(), Msg(c =>
        Nested(c, 2, Msg(metric =>
        {
            metric.WriteTag(1, WireFormat.WireType.LengthDelimited);
            metric.WriteString("queue.depth");
            Nested(metric, 7, Msg(s =>                                  // field 7: sum, is_monotonic absent → gauge
            {
                Nested(s, 1, Msg(dp =>
                {
                    dp.WriteTag(3, WireFormat.WireType.Fixed64); dp.WriteFixed64(1_785_300_060_000_000_000UL);
                    dp.WriteTag(6, WireFormat.WireType.Fixed64); dp.WriteSFixed64(-17);      // as_int, negative
                    Nested(dp, 7, IntAttr("int.attr", 42));
                    Nested(dp, 7, BoolAttr("bool.attr", true));
                    Nested(dp, 7, BoolAttr("false.attr", false));
                    Nested(dp, 7, IntAttr("negative.attr", -7));
                    Nested(dp, 7, DoubleAttr("double.attr", 1.5));
                    Nested(dp, 7, StringAttr("empty.attr", ""));
                }));
            }));
        }))));

    /// <summary>Histogram whose exemplars exercise the "trace link required" filter.</summary>
    public static byte[] HistogramWithExemplars() => Request(StandardResource(), Msg(c =>
        Nested(c, 2, Msg(metric =>
        {
            metric.WriteTag(1, WireFormat.WireType.LengthDelimited);
            metric.WriteString("http.server.request.duration");
            Nested(metric, 9, Msg(h => Nested(h, 1, Msg(dp =>
            {
                dp.WriteTag(3, WireFormat.WireType.Fixed64); dp.WriteFixed64(1_785_300_060_000_000_000UL);
                dp.WriteTag(4, WireFormat.WireType.Fixed64); dp.WriteFixed64(7);
                dp.WriteTag(5, WireFormat.WireType.Fixed64); dp.WriteDouble(931.5);

                byte[] counts = Msg(b => { for (int i = 0; i < 4; i++) b.WriteFixed64((ulong)i); });
                dp.WriteTag(6, WireFormat.WireType.LengthDelimited); dp.WriteBytes(ByteString.CopyFrom(counts));
                byte[] bounds = Msg(b => { for (int i = 0; i < 3; i++) b.WriteDouble(5 * Math.Pow(2, i)); });
                dp.WriteTag(7, WireFormat.WireType.LengthDelimited); dp.WriteBytes(ByteString.CopyFrom(bounds));

                // Kept: has a trace id.
                Nested(dp, 8, Msg(ex =>
                {
                    ex.WriteTag(2, WireFormat.WireType.Fixed64); ex.WriteFixed64(1_785_300_059_000_000_000UL);
                    ex.WriteTag(3, WireFormat.WireType.Fixed64); ex.WriteDouble(455.25);
                    ex.WriteTag(4, WireFormat.WireType.LengthDelimited);
                    ex.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("b7ad6b7169203331")));
                    ex.WriteTag(5, WireFormat.WireType.LengthDelimited);
                    ex.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("0af7651916cd43dd8448eb211c80319c")));
                }));
                // Dropped: no trace id.
                Nested(dp, 8, Msg(ex =>
                {
                    ex.WriteTag(2, WireFormat.WireType.Fixed64); ex.WriteFixed64(1_785_300_058_000_000_000UL);
                    ex.WriteTag(6, WireFormat.WireType.Fixed64); ex.WriteSFixed64(12);
                }));

                Nested(dp, 9, StringAttr("http.route", "/api/pay"));
            }))));
        }))));

    /// <summary>Realistic trace export: one resource, <paramref name="spans"/> server spans
    /// with the attribute set ASP.NET Core instrumentation emits.</summary>
    public static byte[] Traces_Realistic(int spans = 200) => Msg(c =>
        Nested(c, 1, Msg(rs =>
        {
            Nested(rs, 1, StandardResource());
            Nested(rs, 2, Msg(ss =>
            {
                for (int i = 0; i < spans; i++) Nested(ss, 2, Span(i));
            }));
        })));

    private static byte[] Span(int i) => Msg(c =>
    {
        c.WriteTag(1, WireFormat.WireType.LengthDelimited);
        c.WriteBytes(ByteString.CopyFrom(Convert.FromHexString($"0af7651916cd43dd8448eb211c80{i % 100:x2}9c")));
        c.WriteTag(2, WireFormat.WireType.LengthDelimited);
        c.WriteBytes(ByteString.CopyFrom(Convert.FromHexString($"b7ad6b71692033{i % 100:x2}")));
        c.WriteTag(4, WireFormat.WireType.LengthDelimited);
        c.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("00f067aa0ba902b7")));
        c.WriteTag(5, WireFormat.WireType.LengthDelimited); c.WriteString($"GET /api/v1/resource/{i % 7}");
        c.WriteTag(6, WireFormat.WireType.Varint);          c.WriteEnum(2);          // SERVER
        c.WriteTag(7, WireFormat.WireType.Fixed64);         c.WriteFixed64(1_785_300_000_000_000_000UL + (ulong)i * 1_000_000UL);
        c.WriteTag(8, WireFormat.WireType.Fixed64);         c.WriteFixed64(1_785_300_000_012_000_000UL + (ulong)i * 1_000_000UL);

        Nested(c, 9, StringAttr("http.request.method", i % 2 == 0 ? "GET" : "POST"));
        Nested(c, 9, StringAttr("url.path", $"/api/v1/resource/{i % 7}"));
        Nested(c, 9, IntAttr("http.response.status_code", i % 5 == 0 ? 500 : 200));
        Nested(c, 9, StringAttr("network.protocol.version", "1.1"));
        Nested(c, 9, StringAttr("server.address", $"node-{i % 3}"));
        Nested(c, 9, StringAttr("user_agent.original", "k6/0.49 (https://k6.io/)"));

        Nested(c, 15, Msg(st =>                                                      // field 15: status
        {
            if (i % 5 == 0)
            {
                st.WriteTag(2, WireFormat.WireType.LengthDelimited); st.WriteString("Internal Server Error");
                st.WriteTag(3, WireFormat.WireType.Varint);          st.WriteEnum(2);
            }
            else
            {
                st.WriteTag(3, WireFormat.WireType.Varint); st.WriteEnum(1);
            }
        }));
    });

    // ── Data points ───────────────────────────────────────────────────────────

    private static void PointAttributes(CodedOutputStream c, int field, int p)
    {
        Nested(c, field, StringAttr("http.route", $"/api/v1/resource/{p % 7}"));
        Nested(c, field, StringAttr("http.request.method", p % 2 == 0 ? "GET" : "POST"));
        Nested(c, field, StringAttr("http.response.status_code", p % 5 == 0 ? "500" : "200"));
        Nested(c, field, StringAttr("network.protocol.version", "1.1"));
        Nested(c, field, StringAttr("server.address", $"node-{p % 3}"));
    }

    private static byte[] NumberPoint(int p) => Msg(c =>
    {
        c.WriteTag(2, WireFormat.WireType.Fixed64); c.WriteFixed64(1_785_300_000_000_000_000UL);  // start_time
        c.WriteTag(3, WireFormat.WireType.Fixed64); c.WriteFixed64(1_785_300_060_000_000_000UL);  // time_unix_nano
        c.WriteTag(6, WireFormat.WireType.Fixed64); c.WriteSFixed64(42_000 + p);                  // as_int
        PointAttributes(c, 7, p);
    });

    private static byte[] HistogramPoint(int p) => Msg(c =>
    {
        c.WriteTag(2, WireFormat.WireType.Fixed64); c.WriteFixed64(1_785_300_000_000_000_000UL);  // start_time
        c.WriteTag(3, WireFormat.WireType.Fixed64); c.WriteFixed64(1_785_300_060_000_000_000UL);  // time_unix_nano
        c.WriteTag(4, WireFormat.WireType.Fixed64); c.WriteFixed64((ulong)(1000 + p));            // count
        c.WriteTag(5, WireFormat.WireType.Fixed64); c.WriteDouble(12345.67 + p);                  // sum

        byte[] counts = Msg(b => { for (int i = 0; i < Buckets; i++) b.WriteFixed64((ulong)(i * 7 + p)); });
        c.WriteTag(6, WireFormat.WireType.LengthDelimited); c.WriteBytes(ByteString.CopyFrom(counts));

        byte[] bounds = Msg(b => { for (int i = 0; i < Buckets - 1; i++) b.WriteDouble(5 * Math.Pow(2, i)); });
        c.WriteTag(7, WireFormat.WireType.LengthDelimited); c.WriteBytes(ByteString.CopyFrom(bounds));

        PointAttributes(c, 9, p);
    });
}
