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

    /// <summary>
    /// Repeated label keys, every way an exporter produces them (#92): a gauge point that sets one
    /// attribute twice, sets <c>service.name</c> although the resource has it, re-sets a resource
    /// label, and sets one key first as an int and then as a string; a histogram point that repeats
    /// a key; and a resource that repeats a key of its own. OTLP forbids all of it; exporters send it.
    /// </summary>
    public static byte[] RepeatedLabelKeys() => Request(Msg(res =>
    {
        Nested(res, 1, StringAttr("service.name", "Svc.Resource"));
        Nested(res, 1, StringAttr("deployment.environment", "Res"));
        Nested(res, 1, StringAttr("region", "eu-1"));
        Nested(res, 1, StringAttr("region", "eu-2"));                       // the resource's own repeat
        Nested(res, 1, StringAttr("service.name", "Svc.Resource.Again"));   // …and of its service name
    }), Msg(c =>
    {
        Nested(c, 2, Msg(metric =>
        {
            metric.WriteTag(1, WireFormat.WireType.LengthDelimited);
            metric.WriteString("dup.gauge");
            Nested(metric, 5, Msg(g => Nested(g, 1, Msg(dp =>                // field 5: gauge
            {
                dp.WriteTag(3, WireFormat.WireType.Fixed64); dp.WriteFixed64(1_785_300_060_000_000_000UL);
                dp.WriteTag(4, WireFormat.WireType.Fixed64); dp.WriteDouble(1.5);
                Nested(dp, 7, StringAttr("http.route", "/first"));
                Nested(dp, 7, StringAttr("service.name", "Svc.Point"));        // the resource has one
                Nested(dp, 7, StringAttr("http.route", "/last"));
                Nested(dp, 7, StringAttr("deployment.environment", "Point"));  // shadows a resource label
                Nested(dp, 7, IntAttr("k", 1));
                Nested(dp, 7, StringAttr("k", "2"));
            }))));
        }));
        Nested(c, 2, Msg(metric =>
        {
            metric.WriteTag(1, WireFormat.WireType.LengthDelimited);
            metric.WriteString("dup.hist");
            Nested(metric, 9, Msg(h => Nested(h, 1, Msg(dp =>                // field 9: histogram
            {
                dp.WriteTag(3, WireFormat.WireType.Fixed64); dp.WriteFixed64(1_785_300_060_000_000_000UL);
                dp.WriteTag(4, WireFormat.WireType.Fixed64); dp.WriteFixed64(1);
                dp.WriteTag(5, WireFormat.WireType.Fixed64); dp.WriteDouble(2.5);
                Nested(dp, 9, StringAttr("a", "x"));
                Nested(dp, 9, StringAttr("a", "y"));
            }))));
        }));
    }));

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

    /// <summary>
    /// Realistic trace export: one resource, <paramref name="spans"/> server spans with the
    /// attribute set ASP.NET Core instrumentation emits — plus what the DOM decoder used to
    /// materialise and throw away, because leaving it out understates the decode cost of real
    /// SDK traffic: a <c>trace_state</c>, the dropped counts, <c>flags</c>, an exception
    /// <c>events[]</c> entry on every error span (field 11, four attributes of its own) and a
    /// <c>links[]</c> entry on every fourth (field 13).
    /// </summary>
    /// <param name="nestedAttr">
    /// Adds an <c>array_value</c> attribute — the one value type on which the two paths
    /// deliberately disagree (the DOM never modelled fields 5 and 6 and wrote nil). On for the
    /// probe, because real exporters send header arrays and the baseline has to include the
    /// cost; off for the byte-parity test, which has a dedicated payload for the divergence.
    /// </param>
    public static byte[] Traces_Realistic(int spans = 200, bool nestedAttr = true) => Msg(c =>
        Nested(c, 1, Msg(rs =>
        {
            Nested(rs, 1, StandardResource());
            Nested(rs, 2, Msg(ss =>
            {
                for (int i = 0; i < spans; i++) Nested(ss, 2, Span(i, nestedAttr));
            }));
        })));

    private static byte[] Span(int i, bool nestedAttr = true) => Msg(c =>
    {
        c.WriteTag(1, WireFormat.WireType.LengthDelimited);
        c.WriteBytes(ByteString.CopyFrom(Convert.FromHexString($"0af7651916cd43dd8448eb211c80{i % 100:x2}9c")));
        c.WriteTag(2, WireFormat.WireType.LengthDelimited);
        c.WriteBytes(ByteString.CopyFrom(Convert.FromHexString($"b7ad6b71692033{i % 100:x2}")));
        c.WriteTag(3, WireFormat.WireType.LengthDelimited);                          // field 3: trace_state
        c.WriteString("congo=t61rcWkgMzE,rojo=00f067aa0ba902b7");
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
        if (nestedAttr)
            Nested(c, 9, ArrayAttr("http.request.header.accept", "application/json", "text/html"));

        c.WriteTag(10, WireFormat.WireType.Varint); c.WriteUInt32(0);                // dropped_attributes_count

        if (i % 5 == 0) Nested(c, 11, ExceptionEvent(i));                            // field 11: events
        c.WriteTag(12, WireFormat.WireType.Varint); c.WriteUInt32(0);                // dropped_events_count
        if (i % 4 == 0) Nested(c, 13, SpanLink(i));                                  // field 13: links
        c.WriteTag(14, WireFormat.WireType.Varint); c.WriteUInt32(0);                // dropped_links_count

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

        c.WriteTag(16, WireFormat.WireType.Fixed32); c.WriteFixed32(0x0000_0101);    // flags
    });

    /// <summary>The event an instrumented error span carries — four attributes the mapper never read.</summary>
    private static byte[] ExceptionEvent(int i) => Msg(e =>
    {
        e.WriteTag(1, WireFormat.WireType.Fixed64);
        e.WriteFixed64(1_785_300_000_006_000_000UL + (ulong)i * 1_000_000UL);
        e.WriteTag(2, WireFormat.WireType.LengthDelimited); e.WriteString("exception");
        Nested(e, 3, StringAttr("exception.type", "System.InvalidOperationException"));
        Nested(e, 3, StringAttr("exception.message", "The connection pool has been exhausted."));
        Nested(e, 3, StringAttr("exception.stacktrace",
            "   at Wallet.Api.PayController.PostAsync(PayRequest r)\n"
          + "   at lambda_method7(Closure, Object, Object[])\n"
          + "   at Microsoft.AspNetCore.Mvc.Infrastructure.ActionMethodExecutor.Execute()"));
        Nested(e, 3, BoolAttr("exception.escaped", false));
        e.WriteTag(4, WireFormat.WireType.Varint); e.WriteUInt32(0);                 // dropped_attributes_count
    });

    /// <summary>One span link — the shape a messaging consumer span carries.</summary>
    private static byte[] SpanLink(int i) => Msg(l =>
    {
        l.WriteTag(1, WireFormat.WireType.LengthDelimited);
        l.WriteBytes(ByteString.CopyFrom(Convert.FromHexString($"4bf92f3577b34da6a3ce929d0e0e47{i % 100:x2}")));
        l.WriteTag(2, WireFormat.WireType.LengthDelimited);
        l.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("00f067aa0ba902b7")));
        l.WriteTag(3, WireFormat.WireType.LengthDelimited); l.WriteString("congo=t61rcWkgMzE");
        Nested(l, 4, StringAttr("messaging.operation", "publish"));
        l.WriteTag(5, WireFormat.WireType.Varint); l.WriteUInt32(0);                 // dropped_attributes_count
    });

    private static byte[] ArrayAttr(string key, params string[] values) => Msg(c =>
    {
        c.WriteTag(1, WireFormat.WireType.LengthDelimited); c.WriteString(key);
        Nested(c, 2, Msg(v => Nested(v, 5, Msg(arr =>
        {
            foreach (string s in values)
            {
                string t = s;
                Nested(arr, 1, Msg(e => { e.WriteTag(1, WireFormat.WireType.LengthDelimited); e.WriteString(t); }));
            }
        }))));
    });

    /// <summary>An empty ExportTraceServiceRequest.</summary>
    public static byte[] EmptyTraces() => Msg(_ => { });

    /// <summary>
    /// One span carrying every scalar AnyValue type and the three malformed KeyValue shapes
    /// (key with no value, value with no key, AnyValue with no field set), under a resource that
    /// has no <c>service.name</c> at all — so the service falls back to the literal "unknown".
    /// No status message and no parent.
    /// </summary>
    public static byte[] Traces_ScalarAttributeTypes() => Msg(c =>
        Nested(c, 1, Msg(rs =>
        {
            Nested(rs, 1, Msg(res =>
            {
                Nested(res, 1, StringAttr("host.name", "sandbox-kz02"));
                Nested(res, 1, IntAttr("host.cpu.count", 8));
            }));
            Nested(rs, 2, Msg(ss => Nested(ss, 2, Msg(sp =>
            {
                sp.WriteTag(1, WireFormat.WireType.LengthDelimited);
                sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("0af7651916cd43dd8448eb211c80319c")));
                sp.WriteTag(2, WireFormat.WireType.LengthDelimited);
                sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("b7ad6b7169203331")));
                sp.WriteTag(5, WireFormat.WireType.LengthDelimited); sp.WriteString("mixed");
                sp.WriteTag(7, WireFormat.WireType.Fixed64); sp.WriteFixed64(1_785_300_060_000_000_000UL);
                sp.WriteTag(8, WireFormat.WireType.Fixed64); sp.WriteFixed64(1_785_300_060_500_000_000UL);

                Nested(sp, 9, IntAttr("int.attr", 42));
                Nested(sp, 9, IntAttr("negative.attr", -7));
                Nested(sp, 9, BoolAttr("bool.attr", true));
                Nested(sp, 9, BoolAttr("false.attr", false));
                Nested(sp, 9, DoubleAttr("double.attr", 1.5));
                Nested(sp, 9, StringAttr("empty.attr", ""));
                Nested(sp, 9, Msg(kv =>                                        // key with no value message
                {
                    kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("no.value");
                }));
                Nested(sp, 9, Msg(kv =>                                        // value with no key — dropped
                    Nested(kv, 2, Msg(v => { v.WriteTag(1, WireFormat.WireType.LengthDelimited); v.WriteString("orphan"); }))));
                Nested(sp, 9, Msg(kv =>                                        // empty AnyValue → nil
                {
                    kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("empty.value");
                    Nested(kv, 2, Msg(_ => { }));
                }));
                Nested(sp, 9, Msg(kv =>                                        // bytes_value → nil on both paths
                {
                    kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("blob");
                    Nested(kv, 2, Msg(v =>
                    {
                        v.WriteTag(7, WireFormat.WireType.LengthDelimited);
                        v.WriteBytes(ByteString.CopyFrom(new byte[] { 1, 2, 3 }));
                    }));
                }));
            }))));
        })));

    /// <summary>
    /// The spans the mapper drops, and the ones it must not: a short trace id, a missing span
    /// id, a short span id, a CLIENT span exporting to this server's own receiver, the same URL
    /// on a SERVER span, a raw kind of 11 (which masks to CLIENT but is not CLIENT), a
    /// non-conformant parent id, a URL on a non-promoted key, and one ordinary survivor.
    /// </summary>
    public static byte[] Traces_DropRules() => Msg(c =>
        Nested(c, 1, Msg(rs =>
        {
            Nested(rs, 1, Msg(res => Nested(res, 1, StringAttr("service.name", "Wallet.API"))));
            Nested(rs, 2, Msg(ss =>
            {
                Nested(ss, 2, DropSpan("short-trace-id", "0af7651916cd43dd", "b7ad6b7169203331", 2, null, null));
                Nested(ss, 2, DropSpan("no-span-id",     "0af7651916cd43dd8448eb211c80319c", null, 2, null, null));
                Nested(ss, 2, DropSpan("short-span-id",  "0af7651916cd43dd8448eb211c80319c", "b7ad6b71", 2, null, null));
                Nested(ss, 2, DropSpan("self-ingest-client", "1af7651916cd43dd8448eb211c80319c", "b7ad6b7169203332", 3,
                                       "http://ameto-host:8555/v1/traces", null));
                Nested(ss, 2, DropSpan("self-ingest-server", "2af7651916cd43dd8448eb211c80319c", "b7ad6b7169203333", 2,
                                       "http://ameto-host:8555/v1/traces", null));
                Nested(ss, 2, DropSpan("kind-eleven", "3af7651916cd43dd8448eb211c80319c", "b7ad6b7169203334", 11,
                                       "http://ameto-host:8555/otlp/v1/traces", null));
                Nested(ss, 2, DropSpan("short-parent", "4af7651916cd43dd8448eb211c80319c", "b7ad6b7169203335", 1,
                                       null, "00f067aa"));
                Nested(ss, 2, DropSpan("ordinary", "5af7651916cd43dd8448eb211c80319c", "b7ad6b7169203336", 1,
                                       null, "00f067aa0ba902b7"));
            }));
        })));

    private static byte[] DropSpan(string name, string traceHex, string? spanHex, int kind,
                                   string? url, string? parentHex) => Msg(sp =>
    {
        sp.WriteTag(1, WireFormat.WireType.LengthDelimited);
        sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString(traceHex)));
        if (spanHex is not null)
        {
            sp.WriteTag(2, WireFormat.WireType.LengthDelimited);
            sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString(spanHex)));
        }
        if (parentHex is not null)
        {
            sp.WriteTag(4, WireFormat.WireType.LengthDelimited);
            sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString(parentHex)));
        }
        sp.WriteTag(5, WireFormat.WireType.LengthDelimited); sp.WriteString(name);
        sp.WriteTag(6, WireFormat.WireType.Varint);          sp.WriteEnum(kind);
        sp.WriteTag(7, WireFormat.WireType.Fixed64);         sp.WriteFixed64(1_785_300_060_000_000_000UL);
        sp.WriteTag(8, WireFormat.WireType.Fixed64);         sp.WriteFixed64(1_785_300_060_250_000_000UL);
        if (url is not null) Nested(sp, 9, StringAttr("url.full", url));
    });

    /// <summary>
    /// A CLIENT span whose <c>url.full</c> is longer than the 512 bytes the UTF-8 endpoint
    /// matcher will look at, and which names this server's own receiver. The DOM path compared
    /// it as chars, with no cap, and dropped the span; the JSON parser has always kept it.
    /// </summary>
    public static byte[] Traces_OversizedSelfIngestUrl() => Msg(c =>
        Nested(c, 1, Msg(rs =>
        {
            Nested(rs, 1, Msg(res => Nested(res, 1, StringAttr("service.name", "Wallet.API"))));
            Nested(rs, 2, Msg(ss => Nested(ss, 2, DropSpan(
                "oversized-self-ingest", "6af7651916cd43dd8448eb211c80319c", "b7ad6b7169203337", 3,
                "http://ameto-host:8555/v1/traces?q=" + new string('x', 520), null))));
        })));

    /// <summary>
    /// The wire shapes that arrive in an order the msgpack encoding cannot: <c>scope_spans</c>
    /// before its <c>resource</c>, a resource whose first <c>service.name</c> is not a string
    /// followed by one that is, a KeyValue whose value precedes its key, an AnyValue with four
    /// oneof cases set, a repeated key field, a start timestamp past <c>long.MaxValue</c>, and
    /// both HTTP status keys with the new one last. A second resourceSpans with no resource at
    /// all follows, so nothing may carry over from the first.
    /// </summary>
    public static byte[] Traces_OutOfOrderAndAmbiguous() => Msg(c =>
    {
        Nested(c, 1, Msg(rs =>
        {
            // scope_spans (field 2) written first — legal protobuf, and the reason the parser
            // reads the resource in a pass of its own.
            Nested(rs, 2, Msg(ss => Nested(ss, 2, Msg(sp =>
            {
                sp.WriteTag(1, WireFormat.WireType.LengthDelimited);
                sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("7af7651916cd43dd8448eb211c80319c")));
                sp.WriteTag(2, WireFormat.WireType.LengthDelimited);
                sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("b7ad6b7169203338")));
                sp.WriteTag(5, WireFormat.WireType.LengthDelimited); sp.WriteString("ambiguous");
                sp.WriteTag(6, WireFormat.WireType.Varint);  sp.WriteEnum(11);        // masks to CLIENT
                sp.WriteTag(7, WireFormat.WireType.Fixed64); sp.WriteFixed64(0xFFFF_FFFF_FFFF_FFFFUL);
                sp.WriteTag(8, WireFormat.WireType.Fixed64); sp.WriteFixed64(1_785_300_060_000_000_000UL);

                Nested(sp, 9, Msg(kv =>                                      // value BEFORE key
                {
                    Nested(kv, 2, Msg(v => { v.WriteTag(1, WireFormat.WireType.LengthDelimited); v.WriteString("backwards"); }));
                    kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("reversed");
                }));
                Nested(sp, 9, Msg(kv =>                                      // oneof with four cases set
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
                Nested(sp, 9, Msg(kv =>                                      // key stated twice
                {
                    kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("ignored");
                    kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("repeated");
                    Nested(kv, 2, Msg(v => { v.WriteTag(1, WireFormat.WireType.LengthDelimited); v.WriteString("last key wins"); }));
                }));
                Nested(sp, 9, IntAttr("http.status_code", 301));             // old key first…
                Nested(sp, 9, StringAttr("http.response.status_code", "200"));  // …new key last, and it wins

                Nested(sp, 15, Msg(stt => { stt.WriteTag(3, WireFormat.WireType.Varint); stt.WriteEnum(7); }));  // unknown code
            }))));
            Nested(rs, 1, Msg(res =>
            {
                Nested(res, 1, IntAttr("service.name", 7));                  // first, and not a string
                Nested(res, 1, StringAttr("service.name", "Wins.Second"));   // the mapper takes THIS one
                Nested(res, 1, StringAttr("service.name", "Loses.Third"));   // …and a later STRING one does not
                Nested(res, 1, StringAttr("deployment.environment", "Test"));
            }));
        }));
        // No resource at all: the previous block's attributes and service must not carry over.
        Nested(c, 1, Msg(rs =>
            Nested(rs, 2, Msg(ss => Nested(ss, 2, Msg(sp =>
            {
                sp.WriteTag(1, WireFormat.WireType.LengthDelimited);
                sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("8af7651916cd43dd8448eb211c80319c")));
                sp.WriteTag(2, WireFormat.WireType.LengthDelimited);
                sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("b7ad6b7169203339")));
                sp.WriteTag(5, WireFormat.WireType.LengthDelimited); sp.WriteString("orphan-resource");
                sp.WriteTag(7, WireFormat.WireType.Fixed64); sp.WriteFixed64(1_785_300_061_000_000_000UL);
                sp.WriteTag(8, WireFormat.WireType.Fixed64); sp.WriteFixed64(1_785_300_061_000_000_000UL);
            }))))));
    });

    /// <summary>
    /// One span whose single attribute value is <paramref name="depth"/> nested levels with a
    /// string at the bottom — the shape that recurses through the parser's value writer, and a
    /// stack overflow if nothing bounds it.
    /// </summary>
    public static byte[] Traces_NestedToDepth(int depth, Nesting nesting = Nesting.Arrays) => Msg(c =>
        Nested(c, 1, Msg(rs =>
        {
            Nested(rs, 1, Msg(res => Nested(res, 1, StringAttr("service.name", "Wallet.API"))));
            Nested(rs, 2, Msg(ss => Nested(ss, 2, Msg(sp =>
            {
                sp.WriteTag(1, WireFormat.WireType.LengthDelimited);
                sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("9af7651916cd43dd8448eb211c80319c")));
                sp.WriteTag(2, WireFormat.WireType.LengthDelimited);
                sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("b7ad6b716920333a")));
                sp.WriteTag(5, WireFormat.WireType.LengthDelimited); sp.WriteString("deep");
                sp.WriteTag(7, WireFormat.WireType.Fixed64); sp.WriteFixed64(1_785_300_060_000_000_000UL);
                sp.WriteTag(8, WireFormat.WireType.Fixed64); sp.WriteFixed64(1_785_300_060_100_000_000UL);
                Nested(sp, 9, Msg(kv =>
                {
                    kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("nest");
                    Nested(kv, 2, NestedValue(depth, nesting));
                }));
            }))));
        })));

    /// <summary>
    /// One span carrying one <paramref name="valueChars"/>-character string attribute: an
    /// in-limits POST (<c>MaxOtlpBatchBytes</c> is 8 MiB) big enough to grow the parser's
    /// per-thread msgpack scratch far past any keep-it ceiling.
    /// </summary>
    public static byte[] Traces_HugeAttribute(int valueChars) => Msg(c =>
        Nested(c, 1, Msg(rs =>
        {
            Nested(rs, 1, Msg(res => Nested(res, 1, StringAttr("service.name", "Wallet.API"))));
            Nested(rs, 2, Msg(ss => Nested(ss, 2, HugeSpan(valueChars))));
        })));

    /// <summary>
    /// The same span, with a value nested one level past the bound after the huge attribute — so
    /// the scratch is grown and THEN the parse throws, which is the shape that leaves it pinned
    /// when the release only runs on the success path.
    /// </summary>
    public static byte[] Traces_HugeAttributeThenOverDeepValue(int valueChars) => Msg(c =>
        Nested(c, 1, Msg(rs =>
        {
            Nested(rs, 1, Msg(res => Nested(res, 1, StringAttr("service.name", "Wallet.API"))));
            Nested(rs, 2, Msg(ss => Nested(ss, 2, HugeSpan(valueChars, overDeep: true))));
        })));

    /// <summary>Mirrors <c>OtlpTraceProtoParser.MaxValueDepth</c>.</summary>
    private const int MaxTraceValueDepth = 64;

    private static byte[] HugeSpan(int valueChars, bool overDeep = false) => Msg(sp =>
    {
        sp.WriteTag(1, WireFormat.WireType.LengthDelimited);
        sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("9af7651916cd43dd8448eb211c80319c")));
        sp.WriteTag(2, WireFormat.WireType.LengthDelimited);
        sp.WriteBytes(ByteString.CopyFrom(Convert.FromHexString("b7ad6b716920333a")));
        sp.WriteTag(5, WireFormat.WireType.LengthDelimited); sp.WriteString("huge");
        sp.WriteTag(7, WireFormat.WireType.Fixed64); sp.WriteFixed64(1_785_300_060_000_000_000UL);
        sp.WriteTag(8, WireFormat.WireType.Fixed64); sp.WriteFixed64(1_785_300_060_100_000_000UL);
        Nested(sp, 9, StringAttr("big", new string('x', valueChars)));
        if (!overDeep) return;
        Nested(sp, 9, Msg(kv =>
        {
            kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("nest");
            Nested(kv, 2, NestedValue(MaxTraceValueDepth + 1, Nesting.Arrays));
        }));
    });

    /// <summary>
    /// A resource_spans whose length prefix claims more bytes than the payload holds — a
    /// truncated upload, or a hostile one.
    /// </summary>
    public static byte[] Traces_TruncatedLengthPrefix()
    {
        byte[] whole = Traces_Realistic(spans: 2);
        return whole[..(whole.Length - 32)];       // the outer length now overruns the buffer
    }

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

    /// <summary>How a nested attribute value wraps itself at each level.</summary>
    public enum Nesting
    {
        /// <summary>Every level an <c>array_value</c>.</summary>
        Arrays,
        /// <summary>Every level a <c>kvlist_value</c> — the other recursive writer.</summary>
        Kvlists,
        /// <summary>Alternating, so the two recurse through each other.</summary>
        Mixed,
    }

    /// <summary>
    /// One record whose single attribute value is <paramref name="depth"/> nested levels with a
    /// string at the bottom — the shape that recurses through the parser's value writer, and a
    /// stack overflow if nothing bounds it.
    /// </summary>
    public static byte[] Logs_NestedToDepth(int depth, Nesting nesting = Nesting.Arrays) => Msg(c =>
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
                    Nested(kv, 2, NestedValue(depth, nesting));
                }));
            }))));
        })));

    /// <summary>
    /// <paramref name="good"/> complete records followed by one whose attribute value nests past
    /// the parser's depth cap: a batch that turns hostile AFTER its prefix is already in the ring.
    ///
    /// <para>Truncating a buffer cannot produce this shape — the outer resource_logs length
    /// prefix then overruns and the reader throws before it has read a single record (see
    /// <see cref="Logs_TruncatedLengthPrefix"/>). The depth guard is what throws mid-walk.</para>
    /// </summary>
    public static byte[] Logs_PrefixThenOverDeepValue(int good, int depth = 65) => Msg(c =>
        Nested(c, 1, Msg(rl =>
        {
            Nested(rl, 1, StandardResource());
            Nested(rl, 2, Msg(sl =>
            {
                for (int i = 0; i < good; i++) Nested(sl, 2, LogRecord(i));
                Nested(sl, 2, Msg(lr =>
                {
                    lr.WriteTag(1, WireFormat.WireType.Fixed64); lr.WriteFixed64(1_785_300_060_000_000_000UL);
                    lr.WriteTag(2, WireFormat.WireType.Varint);  lr.WriteEnum(9);
                    Nested(lr, 5, Msg(b => { b.WriteTag(1, WireFormat.WireType.LengthDelimited); b.WriteString("deep"); }));
                    Nested(lr, 6, Msg(kv =>
                    {
                        kv.WriteTag(1, WireFormat.WireType.LengthDelimited); kv.WriteString("nest");
                        Nested(kv, 2, NestedValue(depth, Nesting.Arrays));
                    }));
                }));
            }));
        })));

    /// <summary>An AnyValue wrapping itself in <paramref name="depth"/> recursive levels.</summary>
    private static byte[] NestedValue(int depth, Nesting nesting)
    {
        byte[] value = Msg(v => { v.WriteTag(1, WireFormat.WireType.LengthDelimited); v.WriteString("leaf"); });
        for (int i = 0; i < depth; i++)
        {
            byte[] inner = value;
            bool kvlist = nesting switch
            {
                Nesting.Kvlists => true,
                Nesting.Mixed   => i % 2 == 0,
                _               => false,
            };

            value = kvlist
                // AnyValue{ kvlist_value(6){ values(1) = KeyValue{ key(1), value(2) = inner } } }
                ? Msg(v => Nested(v, 6, Msg(kvl => Nested(kvl, 1, Msg(e =>
                    {
                        e.WriteTag(1, WireFormat.WireType.LengthDelimited); e.WriteString("k");
                        Nested(e, 2, inner);
                    })))))
                // AnyValue{ array_value(5){ values(1) = inner } }
                : Msg(v => Nested(v, 5, Msg(arr => Nested(arr, 1, inner))));
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
