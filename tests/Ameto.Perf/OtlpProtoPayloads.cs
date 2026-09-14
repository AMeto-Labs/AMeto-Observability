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

    // ── Logs ──────────────────────────────────────────────────────────────────

    /// <summary>Log records per realistic export — one collector batch from a busy service.</summary>
    public const int LogRecords = 1000;

    /// <summary>
    /// Realistic log export: one resource, one scope, <paramref name="records"/> request-completed
    /// records with a trace link and six mixed-type attributes each — what an OTel SDK exporter or
    /// the collector posts to <c>/v1/logs</c> as application/x-protobuf.
    /// </summary>
    public static byte[] Logs_Realistic(int records = LogRecords) => Msg(c =>
        Nested(c, 1, Msg(rl =>
        {
            Nested(rl, 1, StandardResource());
            Nested(rl, 2, Msg(sl =>
            {
                Nested(sl, 1, Msg(scope =>
                {
                    scope.WriteTag(1, WireFormat.WireType.LengthDelimited); scope.WriteString("Ameto.Sink");
                    scope.WriteTag(2, WireFormat.WireType.LengthDelimited); scope.WriteString("1.0.10");
                }));
                for (int i = 0; i < records; i++) Nested(sl, 2, LogRecord(i));
            }));
        })));

    private static byte[] LogRecord(int i) => Msg(c =>
    {
        c.WriteTag(1, WireFormat.WireType.Fixed64);
        c.WriteFixed64(1_785_300_000_000_000_000UL + (ulong)i * 1_000_000UL);          // time_unix_nano
        c.WriteTag(2, WireFormat.WireType.Varint); c.WriteEnum(i % 10 == 0 ? 17 : 9);  // severity_number
        c.WriteTag(3, WireFormat.WireType.LengthDelimited);
        c.WriteString(i % 10 == 0 ? "Error" : "Information");                          // severity_text
        Nested(c, 5, Msg(b =>                                                          // body
        {
            b.WriteTag(1, WireFormat.WireType.LengthDelimited);
            b.WriteString("HTTP {Method} {Path} responded {StatusCode} in {Elapsed} ms");
        }));

        Nested(c, 6, StringAttr("http.request.method", i % 2 == 0 ? "GET" : "POST"));
        Nested(c, 6, StringAttr("url.path",            $"/api/v1/resource/{i % 7}"));
        Nested(c, 6, IntAttr("http.response.status_code", i % 5 == 0 ? 500 : 200));
        Nested(c, 6, DoubleAttr("elapsed_ms", 1.5 + i % 97));
        Nested(c, 6, BoolAttr("cache_hit", i % 3 == 0));
        Nested(c, 6, StringAttr("RequestId", $"0HN7{i:D6}:00000001"));

        c.WriteTag(9, WireFormat.WireType.LengthDelimited);                            // trace_id
        c.WriteBytes(ByteString.CopyFrom(Convert.FromHexString($"0af7651916cd43dd8448eb211c80{i % 100:x2}9c")));
        c.WriteTag(10, WireFormat.WireType.LengthDelimited);                           // span_id
        c.WriteBytes(ByteString.CopyFrom(Convert.FromHexString($"b7ad6b71692033{i % 100:x2}")));
    });

    /// <summary>An empty ExportLogsServiceRequest.</summary>
    public static byte[] EmptyLogs() => Msg(_ => { });

    /// <summary>
    /// One record carrying every scalar AnyValue type, a resource with no service.name at all,
    /// and the three malformed KeyValue shapes: key with no value, value with no key, and an
    /// AnyValue with no field set.
    /// </summary>
    public static byte[] Logs_ScalarAttributeTypes() => Msg(c =>
        Nested(c, 1, Msg(rl =>
        {
            Nested(rl, 1, Msg(res =>
            {
                Nested(res, 1, StringAttr("host.name", "sandbox-kz02"));
                Nested(res, 1, IntAttr("host.cpu.count", 8));
            }));
            Nested(rl, 2, Msg(sl => Nested(sl, 2, Msg(lr =>
            {
                lr.WriteTag(1, WireFormat.WireType.Fixed64); lr.WriteFixed64(1_785_300_060_000_000_000UL);
                lr.WriteTag(2, WireFormat.WireType.Varint);  lr.WriteEnum(13);
                Nested(lr, 5, Msg(b => { b.WriteTag(1, WireFormat.WireType.LengthDelimited); b.WriteString("mixed"); }));
                Nested(lr, 6, IntAttr("int.attr", 42));
                Nested(lr, 6, IntAttr("negative.attr", -7));
                Nested(lr, 6, BoolAttr("bool.attr", true));
                Nested(lr, 6, BoolAttr("false.attr", false));
                Nested(lr, 6, DoubleAttr("double.attr", 1.5));
                Nested(lr, 6, StringAttr("empty.attr", ""));
                Nested(lr, 6, Msg(kv =>                                        // key with no value message
                {
                    kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("no.value");
                }));
                Nested(lr, 6, Msg(kv =>                                        // value with no key — dropped
                    Nested(kv, 2, Msg(v => { v.WriteTag(1, WireFormat.WireType.LengthDelimited); v.WriteString("orphan"); }))));
                Nested(lr, 6, Msg(kv =>                                        // empty AnyValue → nil
                {
                    kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("empty.value");
                    Nested(kv, 2, Msg(_ => { }));
                }));
            }))));
        })));

    /// <summary>
    /// The absent-field record: no severity, no body, no timestamp, no trace id, no attributes,
    /// and a resource message with no attributes.
    /// </summary>
    public static byte[] Logs_BareRecord() => Msg(c =>
        Nested(c, 1, Msg(rl =>
        {
            Nested(rl, 1, Msg(_ => { }));
            Nested(rl, 2, Msg(sl => Nested(sl, 2, Msg(_ => { }))));
        })));

    /// <summary>Severity carried only by severity_text, plus a body that is not a string.</summary>
    public static byte[] Logs_SeverityTextOnly() => Msg(c =>
        Nested(c, 1, Msg(rl =>
        {
            Nested(rl, 1, StandardResource());
            Nested(rl, 2, Msg(sl =>
            {
                foreach (string text in SeverityTexts)
                {
                    string t = text;
                    Nested(sl, 2, Msg(lr =>
                    {
                        lr.WriteTag(1, WireFormat.WireType.Fixed64); lr.WriteFixed64(1_785_300_060_000_000_000UL);
                        lr.WriteTag(3, WireFormat.WireType.LengthDelimited); lr.WriteString(t);
                        Nested(lr, 5, Msg(b => { b.WriteTag(3, WireFormat.WireType.Varint); b.WriteInt64(7); }));
                    }));
                }
            }));
        })));

    public static readonly string[] SeverityTexts =
        ["FATAL", "Error", "WARN", "WARNING", "Information", "INFO", "debug", "TRACE", "nonsense", ""];

    /// <summary>
    /// Attribute values the DOM decoder never modelled: a nested array, a nested kvlist (itself
    /// holding an array) and a bytes value. Also a short — non-conformant — trace id and no
    /// span id at all.
    /// </summary>
    public static byte[] Logs_NestedAndBytes() => Msg(c =>
        Nested(c, 1, Msg(rl =>
        {
            Nested(rl, 1, Msg(res => Nested(res, 1, StringAttr("service.name", "Etisalat.API"))));
            Nested(rl, 2, Msg(sl => Nested(sl, 2, Msg(lr =>
            {
                lr.WriteTag(1, WireFormat.WireType.Fixed64); lr.WriteFixed64(1_785_300_060_000_000_000UL);
                lr.WriteTag(2, WireFormat.WireType.Varint);  lr.WriteEnum(9);
                Nested(lr, 5, Msg(b => { b.WriteTag(1, WireFormat.WireType.LengthDelimited); b.WriteString("nested"); }));

                Nested(lr, 6, Msg(kv =>                                        // array of scalars
                {
                    kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("tags");
                    Nested(kv, 2, Msg(v => Nested(v, 5, Msg(arr =>
                    {
                        Nested(arr, 1, Msg(e => { e.WriteTag(1, WireFormat.WireType.LengthDelimited); e.WriteString("a"); }));
                        Nested(arr, 1, Msg(e => { e.WriteTag(3, WireFormat.WireType.Varint); e.WriteInt64(2); }));
                        Nested(arr, 1, Msg(e => { e.WriteTag(2, WireFormat.WireType.Varint); e.WriteBool(true); }));
                    }))));
                }));
                Nested(lr, 6, Msg(kv =>                                        // kvlist containing an array
                {
                    kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("ctx");
                    Nested(kv, 2, Msg(v => Nested(v, 6, Msg(kvl =>
                    {
                        Nested(kvl, 1, StringAttr("inner", "x"));
                        Nested(kvl, 1, IntAttr("depth", 2));
                        Nested(kvl, 1, Msg(_ => { }));                          // no key — dropped
                        Nested(kvl, 1, Msg(nest =>
                        {
                            nest.WriteTag(1, WireFormat.WireType.LengthDelimited); nest.WriteString("list");
                            Nested(nest, 2, Msg(v2 => Nested(v2, 5, Msg(arr =>
                                Nested(arr, 1, Msg(e => { e.WriteTag(4, WireFormat.WireType.Fixed64); e.WriteDouble(0.5); }))))));
                        }));
                    }))));
                }));
                Nested(lr, 6, Msg(kv =>                                        // bytes value
                {
                    kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("blob");
                    Nested(kv, 2, Msg(v =>
                    {
                        v.WriteTag(7, WireFormat.WireType.LengthDelimited);
                        v.WriteBytes(ByteString.CopyFrom(new byte[] { 1, 2, 3 }));
                    }));
                }));

                lr.WriteTag(9, WireFormat.WireType.LengthDelimited);            // 8-byte "trace id"
                lr.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("0af7651916cd43dd")));
            }))));
        })));

    /// <summary>
    /// One record whose single attribute value is <paramref name="depth"/> nested
    /// <c>array_value</c> levels with a string at the bottom — the shape that recurses through
    /// the parser's value writer, and a stack overflow if nothing bounds it.
    /// </summary>
    public static byte[] Logs_NestedToDepth(int depth) => Msg(c =>
        Nested(c, 1, Msg(rl =>
        {
            Nested(rl, 1, Msg(res => Nested(res, 1, StringAttr("service.name", "Etisalat.API"))));
            Nested(rl, 2, Msg(sl => Nested(sl, 2, Msg(lr =>
            {
                lr.WriteTag(1, WireFormat.WireType.Fixed64); lr.WriteFixed64(1_785_300_060_000_000_000UL);
                lr.WriteTag(2, WireFormat.WireType.Varint);  lr.WriteEnum(9);
                Nested(lr, 5, Msg(b => { b.WriteTag(1, WireFormat.WireType.LengthDelimited); b.WriteString("deep"); }));
                Nested(lr, 6, Msg(kv =>
                {
                    kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("nest");
                    Nested(kv, 2, NestedArrayValue(depth));
                }));
            }))));
        })));

    /// <summary>An AnyValue wrapping itself in <paramref name="depth"/> array_value levels.</summary>
    private static byte[] NestedArrayValue(int depth)
    {
        byte[] value = Msg(v => { v.WriteTag(1, WireFormat.WireType.LengthDelimited); v.WriteString("leaf"); });
        for (int i = 0; i < depth; i++)
        {
            byte[] inner = value;
            value = Msg(v => Nested(v, 5, Msg(arr => Nested(arr, 1, inner))));
        }
        return value;
    }

    /// <summary>
    /// A scope_logs that appears BEFORE its resource, and a second resource_logs whose own
    /// resource must not inherit the first's service name or attributes.
    /// </summary>
    public static byte[] Logs_ScopeBeforeResource() => Msg(c =>
    {
        Nested(c, 1, Msg(rl =>
        {
            // scope_logs (field 2) written first — legal protobuf, and the reason the parser
            // reads the resource in a pass of its own.
            Nested(rl, 2, Msg(sl => Nested(sl, 2, Msg(lr =>
            {
                lr.WriteTag(1, WireFormat.WireType.Fixed64); lr.WriteFixed64(1_785_300_060_000_000_000UL);
                lr.WriteTag(2, WireFormat.WireType.Varint);  lr.WriteEnum(9);
                Nested(lr, 5, Msg(b => { b.WriteTag(1, WireFormat.WireType.LengthDelimited); b.WriteString("first"); }));
            }))));
            Nested(rl, 1, Msg(res =>
            {
                Nested(res, 1, StringAttr("service.name", "First.Service"));
                Nested(res, 1, StringAttr("host.name", "host-a"));
            }));
        }));
        // Second resource: fewer attributes and a different service, so a leaked ResBuf or
        // ServiceSeen from the first shows up as extra keys or the wrong service name.
        Nested(c, 1, Msg(rl =>
        {
            Nested(rl, 1, Msg(res => Nested(res, 1, StringAttr("service.name", "Second.Service"))));
            Nested(rl, 2, Msg(sl => Nested(sl, 2, Msg(lr =>
            {
                lr.WriteTag(1, WireFormat.WireType.Fixed64); lr.WriteFixed64(1_785_300_061_000_000_000UL);
                lr.WriteTag(2, WireFormat.WireType.Varint);  lr.WriteEnum(9);
                Nested(lr, 5, Msg(b => { b.WriteTag(1, WireFormat.WireType.LengthDelimited); b.WriteString("second"); }));
            }))));
        }));
        // Third resource: none at all, so the previous one's attributes must not carry over.
        Nested(c, 1, Msg(rl =>
            Nested(rl, 2, Msg(sl => Nested(sl, 2, Msg(lr =>
            {
                lr.WriteTag(1, WireFormat.WireType.Fixed64); lr.WriteFixed64(1_785_300_062_000_000_000UL);
                lr.WriteTag(2, WireFormat.WireType.Varint);  lr.WriteEnum(9);
                Nested(lr, 5, Msg(b => { b.WriteTag(1, WireFormat.WireType.LengthDelimited); b.WriteString("third"); }));
            }))))));
    });

    /// <summary>
    /// The wire shapes that arrive in an order the msgpack encoding cannot: a KeyValue whose
    /// value precedes its key, an AnyValue with several oneof cases set, a timestamp past
    /// long.MaxValue, a resource whose first service.name is not a string followed by one that
    /// is, and a repeated key where the last occurrence wins.
    /// </summary>
    public static byte[] Logs_OutOfOrderAndAmbiguous() => Msg(c =>
        Nested(c, 1, Msg(rl =>
        {
            Nested(rl, 1, Msg(res =>
            {
                Nested(res, 1, IntAttr("service.name", 7));                 // first, and not a string
                Nested(res, 1, StringAttr("service.name", "Too.Late"));     // second — must NOT win
            }));
            Nested(rl, 2, Msg(sl => Nested(sl, 2, Msg(lr =>
            {
                lr.WriteTag(1, WireFormat.WireType.Fixed64);
                lr.WriteFixed64(0xFFFF_FFFF_FFFF_FFFFUL);                    // past long.MaxValue
                lr.WriteTag(2, WireFormat.WireType.Varint); lr.WriteEnum(9);
                Nested(lr, 5, Msg(b => { b.WriteTag(1, WireFormat.WireType.LengthDelimited); b.WriteString("ambiguous"); }));

                Nested(lr, 6, Msg(kv =>                                      // value BEFORE key
                {
                    Nested(kv, 2, Msg(v => { v.WriteTag(1, WireFormat.WireType.LengthDelimited); v.WriteString("backwards"); }));
                    kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("reversed");
                }));
                Nested(lr, 6, Msg(kv =>                                      // oneof with four cases set
                {
                    kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("oneof");
                    Nested(kv, 2, Msg(v =>
                    {
                        v.WriteTag(3, WireFormat.WireType.Varint);  v.WriteInt64(11);
                        v.WriteTag(4, WireFormat.WireType.Fixed64); v.WriteDouble(2.5);
                        v.WriteTag(2, WireFormat.WireType.Varint);  v.WriteBool(true);
                        v.WriteTag(1, WireFormat.WireType.LengthDelimited); v.WriteString("string wins");
                    }));
                }));
                Nested(lr, 6, Msg(kv =>                                      // key stated twice
                {
                    kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("ignored");
                    kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("repeated");
                    Nested(kv, 2, Msg(v => { v.WriteTag(1, WireFormat.WireType.LengthDelimited); v.WriteString("last key wins"); }));
                }));
            }))));
        })));

    /// <summary>
    /// A resource_logs whose length prefix claims more bytes than the payload holds — a
    /// truncated upload, or a hostile one.
    /// </summary>
    public static byte[] Logs_TruncatedLengthPrefix()
    {
        byte[] whole = Logs_Realistic(records: 2);
        return whole[..(whole.Length - 32)];       // the outer length now overruns the buffer
    }

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
