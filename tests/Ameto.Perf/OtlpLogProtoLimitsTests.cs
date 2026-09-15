using Ameto.Core;
using Ameto.Core.Serialization;
using Ameto.Otel;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// What <see cref="OtlpLogProtoParser"/> does with input no conformant exporter sends:
/// hostile nesting, fields in an order the msgpack encoding cannot mirror, a oneof with
/// several cases set, and a truncated upload.
///
/// <para>These are not parity tests — the DOM decoder had no case for nested values and
/// answered nil for all of them, so there is nothing to compare against. They pin the
/// behaviour directly, and the first of them pins the difference between a 400 and a dead
/// process.</para>
/// </summary>
public sealed class OtlpLogProtoLimitsTests
{
    private static OtlpLogProtoParityTests.CapturingSink Parse(byte[] payload, out int ingested)
    {
        var sink = new OtlpLogProtoParityTests.CapturingSink();
        (ingested, _) = OtlpLogProtoParser.Parse(payload, sink);
        return sink;
    }

    private static Dictionary<string, object?> Props(byte[] msgpack)
        => msgpack.Length == 0 ? [] : LogEventSerializer.DeserializePropertiesMap(msgpack) ?? [];

    // ── Nesting depth ─────────────────────────────────────────────────────────

    [Fact]
    public void A_value_nested_to_the_limit_is_accepted()
    {
        var sink = Parse(OtlpProtoPayloads.Logs_NestedToDepth(64), out int ingested);

        Assert.Equal(1, ingested);
        object? value = Props(sink.Records[0].Props)["nest"];
        for (int level = 0; level < 64; level++)
            value = Assert.Single(Assert.IsType<object[]>(value));
        Assert.Equal("leaf", value);
    }

    [Fact]
    public void A_value_nested_one_level_past_the_limit_is_refused()
    {
        var sink = new OtlpLogProtoParityTests.CapturingSink();
        Assert.Throws<InvalidDataException>(
            () => OtlpLogProtoParser.Parse(OtlpProtoPayloads.Logs_NestedToDepth(65), sink));
    }

    /// <summary>
    /// The bound has to hold for the OTHER recursive writer too, and for the two recursing
    /// through each other: array_value and kvlist_value both call back into the value writer,
    /// and counting only one of them would leave the whole hole open to a payload built from
    /// the other.
    /// </summary>
    [Fact]
    public void The_depth_bound_counts_kvlists()
        => AssertBoundHolds(OtlpProtoPayloads.Nesting.Kvlists);

    [Fact]
    public void The_depth_bound_counts_kvlists_and_arrays_recursing_through_each_other()
        => AssertBoundHolds(OtlpProtoPayloads.Nesting.Mixed);

    // Internal, not a public [Theory] parameter: OtlpProtoPayloads is internal, and xUnit only
    // discovers public methods.
    private static void AssertBoundHolds(OtlpProtoPayloads.Nesting nesting)
    {
        var sink = Parse(OtlpProtoPayloads.Logs_NestedToDepth(64, nesting), out int ingested);
        Assert.Equal(1, ingested);

        object? value = Props(sink.Records[0].Props)["nest"];
        for (int level = 0; level < 64; level++) value = Descend(value);
        Assert.Equal("leaf", value);

        var refused = new OtlpLogProtoParityTests.CapturingSink();
        Assert.Throws<InvalidDataException>(
            () => OtlpLogProtoParser.Parse(OtlpProtoPayloads.Logs_NestedToDepth(65, nesting), refused));
    }

    /// <summary>One level down, whichever of the two shapes this level happens to be.</summary>
    private static object? Descend(object? value) => value switch
    {
        object[] array                      => Assert.Single(array),
        Dictionary<string, object?> map      => map["k"],
        _ => throw new Xunit.Sdk.XunitException($"expected an array or a map, got {value?.GetType().Name ?? "null"}"),
    };

    [Fact]
    public void A_value_nested_ten_thousand_deep_is_refused_without_taking_the_process_down()
    {
        // THE ONE THAT MATTERS. array_value{values{array_value{…}}} recurses through the value
        // writer, so before the depth bound this was not an exception — it was a stack
        // overflow, which the CLR does not let anyone catch: the process dies, mid-batch, with
        // no response and nothing in the log. 150 KB of body, one unauthenticated-shaped POST
        // away. The endpoints catch InvalidDataException and answer 400 / INVALID_ARGUMENT.
        byte[] payload = OtlpProtoPayloads.Logs_NestedToDepth(10_000);
        var sink = new OtlpLogProtoParityTests.CapturingSink();

        Assert.Throws<InvalidDataException>(() => OtlpLogProtoParser.Parse(payload, sink));
        Assert.Empty(sink.Records);
    }

    [Fact]
    public void The_parser_recovers_after_a_refused_payload()
    {
        // The scratch is [ThreadStatic] and survives the throw, so the next request on this
        // thread has to be unaffected by the last one's half-written property map.
        var thrown = new OtlpLogProtoParityTests.CapturingSink();
        Assert.Throws<InvalidDataException>(
            () => OtlpLogProtoParser.Parse(OtlpProtoPayloads.Logs_NestedToDepth(5_000), thrown));

        var sink = Parse(OtlpProtoPayloads.Logs_Realistic(records: 3), out int ingested);
        Assert.Equal(3, ingested);
        Assert.Equal("Etisalat.API", sink.Records[0].Svc);
        Assert.Equal(14, Props(sink.Records[0].Props).Count);   // 6 resource + 6 record + @tr + @sp
    }

    // ── Truncated input ───────────────────────────────────────────────────────

    [Fact]
    public void A_length_prefix_that_overruns_the_body_is_refused()
    {
        // ProtoReader throws on a truncated length-delimited field; the endpoints turn that
        // into 400 rather than reading past the end of the request buffer.
        var sink = new OtlpLogProtoParityTests.CapturingSink();
        Assert.Throws<InvalidDataException>(
            () => OtlpLogProtoParser.Parse(OtlpProtoPayloads.Logs_TruncatedLengthPrefix(), sink));
    }

    // ── Field order and ambiguity ─────────────────────────────────────────────

    [Fact]
    public void Scope_logs_before_its_resource_still_gets_the_resource_attributes()
    {
        var sink = Parse(OtlpProtoPayloads.Logs_ScopeBeforeResource(), out int ingested);
        Assert.Equal(3, ingested);

        // First resource_logs: the resource was written AFTER the records it applies to.
        var first = Props(sink.Records[0].Props);
        Assert.Equal("First.Service", sink.Records[0].Svc);
        Assert.Equal("First.Service", first["service.name"]);
        Assert.Equal("host-a", first["host.name"]);

        // Second: its own service, and NOT the first one's host.name.
        var second = Props(sink.Records[1].Props);
        Assert.Equal("Second.Service", sink.Records[1].Svc);
        Assert.Equal("Second.Service", second["service.name"]);
        Assert.False(second.ContainsKey("host.name"));
        Assert.Single(second);

        // Third: no resource at all, so no resource attributes and no service name.
        Assert.Null(sink.Records[2].Svc);
        Assert.Empty(Props(sink.Records[2].Props));
    }

    [Fact]
    public void Ambiguous_wire_shapes_resolve_the_way_the_mapper_did()
    {
        var sink = Parse(OtlpProtoPayloads.Logs_OutOfOrderAndAmbiguous(), out int ingested);
        Assert.Equal(1, ingested);
        var props = Props(sink.Records[0].Props);

        // A KeyValue whose value precedes its key is still written key-then-value.
        Assert.Equal("backwards", props["reversed"]);

        // Several oneof cases set: string first, as OtlpLogMapper.WriteAnyValue ordered them —
        // NOT whichever arrived last on the wire.
        Assert.Equal("string wins", props["oneof"]);

        // A repeated key: the last occurrence is the one the DOM decoder kept.
        Assert.Equal("last key wins", props["repeated"]);
        Assert.False(props.ContainsKey("ignored"));

        // The first service.name is an int, so the mapper captured null and stopped looking;
        // the later string one does not get a second chance at the dedicated column.
        Assert.Null(sink.Records[0].Svc);

        // Both attributes are still IN the map, duplicate key and all — the mapper wrote every
        // resource attribute it was given and so does this. Decoding that map into a dictionary
        // is what collapses them, last one winning; the byte-level agreement is pinned by
        // The_dom_oracle_agrees_about_the_ambiguous_shapes.
        Assert.Equal("Too.Late", props["service.name"]);

        // A fixed64 past long.MaxValue is not a timestamp. The mapper's long.TryParse of the
        // stringified value failed and fell through to "now", so this does too.
        Assert.InRange(sink.Records[0].Ts,
            DateTimeOffset.UtcNow.UtcTicks - TimeSpan.TicksPerMinute,
            DateTimeOffset.UtcNow.UtcTicks + TimeSpan.TicksPerMinute);
    }

    /// <summary>
    /// The same ambiguous payload through the DOM oracle, so the claims above are pinned to
    /// what the mapper actually did rather than to what this test's author believed it did.
    /// </summary>
    [Fact]
    public void The_dom_oracle_agrees_about_the_ambiguous_shapes()
    {
        byte[] payload = OtlpProtoPayloads.Logs_OutOfOrderAndAmbiguous();
        var dom = OtlpLogMapper.Map(
            OtlpProtoDecoder.DecodeLogs(payload, payload.Length), NodeId.Local.Value);

        var sink = Parse(payload, out _);
        Assert.True(dom[0].RawProperties.Span.SequenceEqual(sink.Records[0].Props),
            $"dom: {Convert.ToHexString(dom[0].RawProperties.Span)}\nnew: {Convert.ToHexString(sink.Records[0].Props)}");
        Assert.Equal(dom[0].ServiceName, sink.Records[0].Svc);
    }
}
