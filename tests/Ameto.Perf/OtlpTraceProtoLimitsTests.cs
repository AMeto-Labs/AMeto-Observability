using Ameto.Core.Serialization;
using Ameto.Otel;
using Ameto.Tracing;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// What the trace ingest parsers do with input no conformant exporter sends: hostile nesting,
/// and a truncated upload.
///
/// <para>These are not parity tests — the DOM decoder had no case for nested values and
/// answered nil for all of them, so there is nothing to compare against. They pin the behaviour
/// directly, and the first of them pins the difference between a 400 and a dead process.</para>
/// </summary>
public sealed class OtlpTraceProtoLimitsTests
{
    private static Dictionary<string, object?> Attrs(byte[] msgpack)
        => msgpack.Length == 0 ? [] : LogEventSerializer.DeserializePropertiesMap(msgpack) ?? [];

    private static object? NestValue(byte[] payload)
    {
        var span = Assert.Single(OtlpTraceProtoParser.Parse(payload));
        return Attrs(span.AttributesBytes)["nest"];
    }

    // ── Nesting depth ─────────────────────────────────────────────────────────

    [Fact]
    public void A_value_nested_to_the_limit_is_accepted()
    {
        object? value = NestValue(OtlpProtoPayloads.Traces_NestedToDepth(64));
        for (int level = 0; level < 64; level++)
            value = Assert.Single(Assert.IsType<object[]>(value));
        Assert.Equal("leaf", value);
    }

    [Fact]
    public void A_value_nested_one_level_past_the_limit_is_refused()
        => Assert.Throws<InvalidDataException>(
            () => OtlpTraceProtoParser.Parse(OtlpProtoPayloads.Traces_NestedToDepth(65)));

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
        object? value = NestValue(OtlpProtoPayloads.Traces_NestedToDepth(64, nesting));
        for (int level = 0; level < 64; level++) value = Descend(value);
        Assert.Equal("leaf", value);

        Assert.Throws<InvalidDataException>(
            () => OtlpTraceProtoParser.Parse(OtlpProtoPayloads.Traces_NestedToDepth(65, nesting)));
    }

    /// <summary>One level down, whichever of the two shapes this level happens to be.</summary>
    private static object? Descend(object? value) => value switch
    {
        object[] array                  => Assert.Single(array),
        Dictionary<string, object?> map => map["k"],
        _ => throw new Xunit.Sdk.XunitException($"expected an array or a map, got {value?.GetType().Name ?? "null"}"),
    };

    [Fact]
    public void A_value_nested_ten_thousand_deep_is_refused_without_taking_the_process_down()
    {
        // THE ONE THAT MATTERS. array_value{values{array_value{…}}} recurses through the value
        // writer, so without the depth bound this is not an exception — it is a stack overflow,
        // which the CLR does not let anyone catch: the process dies, mid-batch, with no response
        // and nothing in the log. 150 KB of body, one POST away. The receivers catch
        // InvalidDataException and answer 400 / INVALID_ARGUMENT.
        Assert.Throws<InvalidDataException>(
            () => OtlpTraceProtoParser.Parse(OtlpProtoPayloads.Traces_NestedToDepth(10_000)));
    }

    [Fact]
    public void The_parser_recovers_after_a_refused_payload()
    {
        // The scratch is [ThreadStatic] and survives the throw, so the next request on this
        // thread has to be unaffected by the last one's half-written attribute map.
        Assert.Throws<InvalidDataException>(
            () => OtlpTraceProtoParser.Parse(OtlpProtoPayloads.Traces_NestedToDepth(5_000)));

        var spans = OtlpTraceProtoParser.Parse(OtlpProtoPayloads.Traces_Realistic(3, nestedAttr: false));
        Assert.Equal(3, spans.Count);
        Assert.Equal("Etisalat.API", spans[0].ServiceName);
        Assert.Equal(11, Attrs(spans[0].AttributesBytes).Count);   // 5 resource + 6 span
    }

    // ── Truncated input ───────────────────────────────────────────────────────

    [Fact]
    public void A_length_prefix_that_overruns_the_body_is_refused()
        => Assert.Throws<InvalidDataException>(
            () => OtlpTraceProtoParser.Parse(OtlpProtoPayloads.Traces_TruncatedLengthPrefix()));

    /// <summary>
    /// A span with no attributes anywhere gets no attribute bytes at all, not an empty msgpack
    /// map — the trace mapper returned <c>[]</c> for that case and the JSON parser matched it,
    /// so the storage-side "has attributes" test keeps meaning what it meant.
    /// </summary>
    [Fact]
    public void A_span_with_no_attributes_gets_no_bytes()
    {
        var spans = OtlpTraceProtoParser.Parse(OtlpProtoPayloads.Traces_OutOfOrderAndAmbiguous());
        Assert.Empty(spans[1].AttributesBytes);
        Assert.Equal(SpanKind.Unspecified, spans[1].Kind);
    }
}
