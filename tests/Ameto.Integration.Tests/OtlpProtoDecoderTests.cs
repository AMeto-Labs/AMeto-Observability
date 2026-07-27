// Asserting on a model whose collections are declared nullable; the ! noise would
// drown the tests. The decoder guarantees these are non-null on the paths exercised.
#nullable disable

using Google.Protobuf;
using Ameto.Otel;
using Xunit;

namespace Ameto.Integration.Tests;

/// <summary>
/// Characterisation tests for <see cref="OtlpProtoDecoder"/>, which had none.
///
/// <para>They exist to make the submessage-parsing strategy replaceable. The decoder used to
/// materialise each nested message into its own buffer and parser; parsing in place against a
/// pushed length limit is far cheaper but moves the boundary bookkeeping into the reader, and
/// getting it wrong does not throw — it silently drops repeated elements or lets a child's
/// bytes bleed into its parent. So every message here carries SEVERAL repeated children and a
/// scalar field positioned AFTER a nested one, which is exactly what a leaked limit corrupts.</para>
/// </summary>
public sealed class OtlpProtoDecoderTests
{
    // ── Minimal protobuf writer (the project has no generated OTLP types) ─────

    private static byte[] Msg(Action<CodedOutputStream> body)
    {
        using var ms = new MemoryStream();
        var o = new CodedOutputStream(ms);
        body(o);
        o.Flush();
        return ms.ToArray();
    }

    /// <summary>Writes a length-delimited submessage under <paramref name="field"/>.</summary>
    private static void Sub(CodedOutputStream o, int field, byte[] payload)
    {
        o.WriteTag(field, WireFormat.WireType.LengthDelimited);
        o.WriteBytes(ByteString.CopyFrom(payload));
    }

    private static void Str(CodedOutputStream o, int field, string v)
    {
        o.WriteTag(field, WireFormat.WireType.LengthDelimited);
        o.WriteString(v);
    }

    private static void Bytes(CodedOutputStream o, int field, byte[] v)
    {
        o.WriteTag(field, WireFormat.WireType.LengthDelimited);
        o.WriteBytes(ByteString.CopyFrom(v));
    }

    private static void Fixed64(CodedOutputStream o, int field, ulong v)
    {
        o.WriteTag(field, WireFormat.WireType.Fixed64);
        o.WriteFixed64(v);
    }

    private static void VarInt(CodedOutputStream o, int field, long v)
    {
        o.WriteTag(field, WireFormat.WireType.Varint);
        o.WriteInt64(v);
    }

    // ── Builders ──────────────────────────────────────────────────────────────

    private static byte[] StringAttr(string key, string value) =>
        Msg(o => { Str(o, 1, key); Sub(o, 2, Msg(v => Str(v, 1, value))); });

    private static byte[] IntAttr(string key, long value) =>
        Msg(o => { Str(o, 1, key); Sub(o, 2, Msg(v => VarInt(v, 3, value))); });

    private static byte[] DoubleAttr(string key, double value) =>
        Msg(o => { Str(o, 1, key); Sub(o, 2, Msg(v => { v.WriteTag(4, WireFormat.WireType.Fixed64); v.WriteDouble(value); })); });

    private static byte[] BoolAttr(string key, bool value) =>
        Msg(o => { Str(o, 1, key); Sub(o, 2, Msg(v => { v.WriteTag(2, WireFormat.WireType.Varint); v.WriteBool(value); })); });

    private static byte[] Span(string name, byte[] traceId, byte[] spanId, int kind,
                               ulong start, ulong end, params byte[][] attrs) =>
        Msg(o =>
        {
            Bytes(o, 1, traceId);
            Bytes(o, 2, spanId);
            Str(o, 5, name);
            VarInt(o, 6, kind);
            Fixed64(o, 7, start);
            Fixed64(o, 8, end);
            foreach (var a in attrs) Sub(o, 9, a);
            // Status LAST, after the repeated attribute submessages: if a child's limit
            // leaks, this is the field that disappears or misreads.
            Sub(o, 15, Msg(s => { Str(s, 2, "boom"); VarInt(s, 3, 2); }));
        });

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public void Decodes_a_nested_trace_request_with_repeated_children()
    {
        var tid1 = Enumerable.Range(1, 16).Select(i => (byte)i).ToArray();
        var sid1 = Enumerable.Range(1, 8).Select(i => (byte)i).ToArray();
        var sid2 = Enumerable.Range(9, 8).Select(i => (byte)i).ToArray();

        byte[] payload = Msg(root =>
        {
            // TWO resource_spans, each with TWO scope_spans, each with TWO spans.
            for (int r = 0; r < 2; r++)
            {
                int rr = r;
                Sub(root, 1, Msg(rs =>
                {
                    Sub(rs, 1, Msg(res => Sub(res, 1, StringAttr("service.name", $"svc-{rr}"))));
                    for (int s = 0; s < 2; s++)
                    {
                        int ss = s;
                        Sub(rs, 2, Msg(sc =>
                        {
                            Sub(sc, 1, Msg(scope => { Str(scope, 1, $"scope-{ss}"); Str(scope, 2, "1.0"); }));
                            Sub(sc, 2, Span($"GET /a/{rr}{ss}", tid1, sid1, 2, 1000, 2000,
                                            StringAttr("http.method", "GET"),
                                            IntAttr("http.status_code", 200),
                                            DoubleAttr("duration", 1.5),
                                            BoolAttr("ok", true)));
                            Sub(sc, 2, Span($"POST /b/{rr}{ss}", tid1, sid2, 3, 3000, 4000,
                                            StringAttr("http.method", "POST")));
                            // Scalar AFTER the repeated span submessages.
                            Str(sc, 3, "schema://scope");
                        }));
                    }
                    Str(rs, 3, "schema://resource");
                }));
            }
        });

        var req = OtlpProtoDecoder.DecodeTraces(payload, payload.Length);

        Assert.Equal(2, req.ResourceSpans.Count);
        foreach (var rs in req.ResourceSpans)
        {
            Assert.Equal(2, rs.ScopeSpans.Count);
            foreach (var sc in rs.ScopeSpans)
                Assert.Equal(2, sc.Spans.Count);
        }

        var first = req.ResourceSpans[0].ScopeSpans[0].Spans[0];
        Assert.Equal("GET /a/00", first.Name);
        Assert.Equal("0102030405060708090a0b0c0d0e0f10", first.TraceId);
        Assert.Equal("0102030405060708", first.SpanId);
        Assert.Equal(2, first.Kind);
        Assert.Equal("1000", first.StartTimeUnixNano);
        Assert.Equal("2000", first.EndTimeUnixNano);

        Assert.Equal(4, first.Attributes!.Count);
        Assert.Equal("GET", first.Attributes[0].Value!.StringValue);
        Assert.Equal("200", first.Attributes[1].Value!.IntValue);
        Assert.Equal(1.5,   first.Attributes[2].Value!.DoubleValue);
        Assert.True(first.Attributes[3].Value!.BoolValue);

        // The field that follows the repeated children — the canary for a leaked limit.
        Assert.NotNull(first.Status);
        Assert.Equal("boom", first.Status!.Message);
        Assert.Equal(2, first.Status.Code);

        var second = req.ResourceSpans[0].ScopeSpans[0].Spans[1];
        Assert.Equal("POST /b/00", second.Name);
        Assert.Equal("090a0b0c0d0e0f10", second.SpanId);
        Assert.Single(second.Attributes!);

        Assert.Equal("svc-1", req.ResourceSpans[1].Resource!.Attributes![0].Value!.StringValue);
    }

    [Fact]
    public void Unknown_fields_are_skipped_without_disturbing_the_rest()
    {
        byte[] payload = Msg(root =>
        {
            VarInt(root, 99, 12345);                       // unknown scalar before
            Sub(root, 1, Msg(rs =>
            {
                Str(rs, 77, "unknown string");             // unknown inside the child
                Sub(rs, 2, Msg(sc => Sub(sc, 2, Span("only", new byte[16], new byte[8], 1, 5, 6))));
            }));
            Str(root, 88, "unknown trailer");              // unknown after
        });

        var req = OtlpProtoDecoder.DecodeTraces(payload, payload.Length);
        Assert.Single(req.ResourceSpans!);
        Assert.Equal("only", req.ResourceSpans[0].ScopeSpans[0].Spans[0].Name);
    }

    [Fact]
    public void An_empty_request_yields_no_resource_spans()
    {
        var req = OtlpProtoDecoder.DecodeTraces([], 0);
        Assert.Empty(req.ResourceSpans!);
    }

    // ── Metrics and logs ──────────────────────────────────────────────────────
    // The three entry points share SubStream but not their field numbers or nesting, so a
    // rewrite of the submessage strategy would land on all three. Guarding only traces would
    // leave two of them uncovered — the exact gap these tests exist to close.

    [Fact]
    public void Decodes_metrics_with_repeated_children_at_every_level()
    {
        byte[] payload = Msg(root =>
        {
            for (int r = 0; r < 2; r++)
            {
                int rr = r;
                Sub(root, 1, Msg(rm =>                                     // resource_metrics
                {
                    Sub(rm, 1, Msg(res => Sub(res, 1, StringAttr("service.name", $"svc-{rr}"))));
                    for (int s = 0; s < 2; s++)
                        Sub(rm, 2, Msg(sm =>                               // scope_metrics
                        {
                            Sub(sm, 2, Msg(m =>                            // metric: gauge
                            {
                                Str(m, 1, $"cpu.{rr}");
                                Str(m, 3, "%");
                                Sub(m, 5, Msg(g =>                         // gauge
                                {
                                    Sub(g, 1, Msg(dp => { Fixed64(dp, 3, 1_700); dp.WriteTag(4, WireFormat.WireType.Fixed64); dp.WriteDouble(42.5); }));
                                    Sub(g, 1, Msg(dp => { Fixed64(dp, 3, 1_800); dp.WriteTag(4, WireFormat.WireType.Fixed64); dp.WriteDouble(43.5); }));
                                }));
                                // Unit AFTER the nested gauge — canary for a leaked boundary.
                                Str(m, 2, "cpu usage");
                            }));
                        }));
                    Str(rm, 3, "schema://resource");
                }));
            }
        });

        var req = OtlpProtoDecoder.DecodeMetrics(payload, payload.Length);

        Assert.Equal(2, req.ResourceMetrics.Count);
        var sm0 = req.ResourceMetrics[0].ScopeMetrics;
        Assert.Equal(2, sm0.Count);
        var metric = sm0[0].Metrics[0];
        Assert.Equal("cpu.0", metric.Name);
        Assert.Equal("%", metric.Unit);
        Assert.Equal("cpu usage", metric.Description);          // the field after the gauge
        Assert.NotNull(metric.Gauge);
        Assert.Equal(2, metric.Gauge!.DataPoints.Count);
        Assert.Equal(42.5, metric.Gauge.DataPoints[0].AsDouble);
        Assert.Equal(43.5, metric.Gauge.DataPoints[1].AsDouble);
        Assert.Equal("svc-1", req.ResourceMetrics[1].Resource!.Attributes![0].Value!.StringValue);
    }

    [Fact]
    public void Decodes_logs_with_repeated_children_at_every_level()
    {
        byte[] payload = Msg(root =>
        {
            for (int r = 0; r < 2; r++)
            {
                int rr = r;
                Sub(root, 1, Msg(rl =>                                     // resource_logs
                {
                    Sub(rl, 1, Msg(res => Sub(res, 1, StringAttr("service.name", $"svc-{rr}"))));
                    Sub(rl, 2, Msg(sl =>                                   // scope_logs
                    {
                        for (int i = 0; i < 3; i++)
                        {
                            int ii = i;
                            Sub(sl, 2, Msg(lr =>                           // log_record
                            {
                                Fixed64(lr, 1, (ulong)(9_000 + ii));
                                VarInt(lr, 2, 9);                          // severity_number
                                Str(lr, 3, "INFO");
                                Sub(lr, 5, Msg(b => Str(b, 1, $"message {rr}{ii}")));
                                Sub(lr, 6, StringAttr("k", $"v{ii}"));
                                // Severity text re-stated AFTER the nested body/attributes.
                                Str(lr, 3, "INFO2");
                            }));
                        }
                        Str(sl, 3, "schema://scope");
                    }));
                }));
            }
        });

        var req = OtlpProtoDecoder.DecodeLogs(payload, payload.Length);

        Assert.Equal(2, req.ResourceLogs.Count);
        var recs = req.ResourceLogs[0].ScopeLogs[0].LogRecords;
        Assert.Equal(3, recs.Count);
        Assert.Equal("message 00", recs[0].Body!.StringValue);
        Assert.Equal("message 02", recs[2].Body!.StringValue);
        Assert.Equal(9, recs[0].SeverityNumber);
        Assert.Equal("INFO2", recs[0].SeverityText);           // the field after the nested ones
        Assert.Equal("v1", recs[1].Attributes![0].Value!.StringValue);
        Assert.Equal("svc-1", req.ResourceLogs[1].Resource!.Attributes![0].Value!.StringValue);
    }
}
