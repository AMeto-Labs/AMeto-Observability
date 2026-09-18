using System.Text;
using System.Text.Json;
using Ameto.Core.Serialization;
using Ameto.Otel;
using Ameto.Otel.Models;
using Ameto.Tracing;
using Xunit;

namespace Ameto.Perf;

/// <summary>
/// What the two trace ingest parsers do at their edges: for the protobuf one, hostile nesting
/// and a truncated upload; for the JSON one, the nested-value scratch and the escaped strings
/// that now share a per-thread buffer instead of renting one each.
///
/// <para>The protobuf half are not parity tests — the DOM decoder had no case for nested values
/// and answered nil for all of them, so there is nothing to compare against. They pin the
/// behaviour directly, and the first of them pins the difference between a 400 and a dead
/// process. The JSON half ARE parity tests: the JSON DOM models nested values, so it is a real
/// oracle for the buffer reuse.</para>
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

    // ── The JSON parser's shared scratch ──────────────────────────────────────

    private static readonly JsonSerializerOptions DomOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        AllowTrailingCommas  = true,
    };

    private static void AssertJsonMatchesDom(string json)
    {
        byte[] utf8 = Encoding.UTF8.GetBytes(json);
        var streamed = OtlpTraceStreamParser.Parse(utf8);
        var dom = OtlpTraceMapper.Map(
            JsonSerializer.Deserialize<ExportTraceServiceRequest>(utf8, DomOptions)!);

        Assert.Equal(dom.Count, streamed.Count);
        for (int i = 0; i < dom.Count; i++)
        {
            Assert.Equal(dom[i].Name, streamed[i].Name);
            Assert.True(dom[i].AttributesBytes.AsSpan().SequenceEqual(streamed[i].AttributesBytes),
                $"span {i} ({dom[i].Name}): attribute bytes differ\n"
              + $"  dom: {Convert.ToHexString(dom[i].AttributesBytes)}\n"
              + $"  new: {Convert.ToHexString(streamed[i].AttributesBytes)}");
        }
    }

    /// <summary>
    /// msgpack needs an element count before the elements and a <c>Utf8JsonReader</c> cannot be
    /// rewound, so a nested value is buffered and spliced. The buffer is now one per NESTING
    /// LEVEL, reused across values and requests instead of allocated per value — which is only
    /// safe while a level is spliced into its parent before the next sibling opens, and a child
    /// always takes a deeper one. So the payload here nests an array inside a kvlist inside an
    /// array, puts scalar siblings on both sides of the nested one, and gives the span a second
    /// attribute that nests to the same depth all over again.
    /// </summary>
    [Fact]
    public void Json_nested_values_splice_correctly_at_every_level()
        => AssertJsonMatchesDom(NestedBatch);

    /// <summary>
    /// Escaped keys and values share one per-thread buffer that grows to the longest seen. A
    /// short value after a long one must write only its own bytes, and a value past the
    /// keep-it threshold must still come out whole.
    /// </summary>
    [Fact]
    public void Json_escaped_keys_and_values_survive_the_shared_scratch()
        => AssertJsonMatchesDom(EscapedBatch(longValueChars: 200_000));

    [Fact]
    public void Json_escaped_values_are_right_whatever_order_the_lengths_arrive_in()
    {
        // The scratch is [ThreadStatic] and survives between calls, so a batch that grew it must
        // not change what the next batch writes.
        AssertJsonMatchesDom(EscapedBatch(longValueChars: 100_000));
        AssertJsonMatchesDom(EscapedBatch(longValueChars: 8));
    }

    /// <summary>
    /// The self-ingest guard reads an escaped URL through its own buffer, and the endpoint
    /// matcher refuses anything over 512 bytes. An escaped URL longer than that can still
    /// unescape to something shorter — a unicode escape is six bytes for one character — so the
    /// long case cannot simply be skipped, and this pins that it is not.
    /// </summary>
    [Fact]
    public void Json_an_escaped_self_ingest_url_is_still_a_dropped_client_span()
    {
        string json = EscapedUrlBatch();

        // Three CLIENT spans go in: a short escaped self-ingest URL, one whose escapes make it
        // longer than 512 bytes but which unescapes to a self-ingest URL, and an ordinary one.
        var kept = Assert.Single(OtlpTraceStreamParser.Parse(Encoding.UTF8.GetBytes(json)));
        Assert.Equal("not-ours", kept.Name);

        // And the DOM drops the same two: these URLs are short enough unescaped that its
        // uncapped char comparison and the capped UTF-8 one agree.
        AssertJsonMatchesDom(json);
    }

    private const string NestedBatch = """
    {"resourceSpans":[{"resource":{"attributes":[
        {"key":"service.name","value":{"stringValue":"Wallet.API"}}
      ]},
      "scopeSpans":[{"spans":[
        {"traceId":"f6f6f098569a7f2ba54f3c734aa563f0","spanId":"a1b2c3d4e5f60718",
         "name":"nested","kind":2,
         "startTimeUnixNano":"1783953780000000000","endTimeUnixNano":"1783953780250000000",
         "attributes":[
           {"key":"deep","value":{"arrayValue":{"values":[
              {"stringValue":"before"},
              {"kvlistValue":{"values":[
                 {"key":"inner","value":{"arrayValue":{"values":[
                    {"intValue":"1"},
                    {"kvlistValue":{"values":[{"key":"leaf","value":{"doubleValue":0.25}}]}},
                    {"intValue":"2"}
                 ]}}},
                 {"key":"flag","value":{"boolValue":true}}
              ]}},
              {"stringValue":"after"}
           ]}}},
           {"key":"again","value":{"arrayValue":{"values":[
              {"kvlistValue":{"values":[{"key":"k","value":{"stringValue":"v"}}]}},
              {"stringValue":"tail"}
           ]}}},
           {"key":"plain","value":{"stringValue":"last"}}
         ]}
      ]}]
    }]}
    """;

    /// <summary>Every character of <paramref name="s"/> written as a JSON \uXXXX escape.</summary>
    private static string JsonEscaped(string s)
    {
        var sb = new StringBuilder(s.Length * 6);
        foreach (char ch in s) sb.Append((char)0x5C).Append('u').Append(((int)ch).ToString("x4"));
        return sb.ToString();
    }

    private static string EscapedBatch(int longValueChars)
        // Every string below is fully \uXXXX-escaped, so every one goes through the unescape
        // buffer, and the long one decides how far that buffer grows.
        => EscapedBatchTemplate
            .Replace("@@RESKEY@@",   JsonEscaped("res.key"))
            .Replace("@@RESVAL@@",   JsonEscaped("res.value"))
            .Replace("@@DBKEY@@",    JsonEscaped("db.statement"))
            .Replace("@@LONG@@",     JsonEscaped(new string('x', longValueChars)))
            .Replace("@@SHORTKEY@@", JsonEscaped("short.key"))
            .Replace("@@SHORTVAL@@", JsonEscaped("aBc"))
            .Replace("@@T1@@",       JsonEscaped("one two"))
            .Replace("@@T2@@",       JsonEscaped("café"));

    private const string EscapedBatchTemplate = """
    {"resourceSpans":[{"resource":{"attributes":[
        {"key":"service.name","value":{"stringValue":"Wallet.API"}},
        {"key":"@@RESKEY@@","value":{"stringValue":"@@RESVAL@@"}}
      ]},
      "scopeSpans":[{"spans":[
        {"traceId":"f6f6f098569a7f2ba54f3c734aa563f0","spanId":"a1b2c3d4e5f60718",
         "name":"escaped","kind":2,
         "startTimeUnixNano":"1783953780000000000","endTimeUnixNano":"1783953780250000000",
         "attributes":[
           {"key":"@@DBKEY@@","value":{"stringValue":"@@LONG@@"}},
           {"key":"@@SHORTKEY@@","value":{"stringValue":"@@SHORTVAL@@"}},
           {"key":"tags","value":{"arrayValue":{"values":[
              {"stringValue":"@@T1@@"},{"stringValue":"@@T2@@"}
           ]}}},
           {"key":"plain","value":{"stringValue":"no escapes here"}}
         ]}
      ]}]
    }]}
    """;

    /// <summary>
    /// Three CLIENT spans: a fully escaped self-ingest URL, one whose ESCAPED form is past the
    /// matcher's 512-byte ceiling but which unescapes to a 99-byte self-ingest URL, and one
    /// pointing somewhere else. The first two are this server's own receiver and must go.
    /// </summary>
    private static string EscapedUrlBatch() =>
        EscapedUrlTemplate
            .Replace("@@URL1@@", JsonEscaped("http://ameto-host:8555/v1/traces"))
            .Replace("@@URL2@@", JsonEscaped("http://ameto-host:8555/v1/traces?trace=" + new string('a', 60)))
            .Replace("@@URL3@@", JsonEscaped("http://elsewhere:8555/api/pay"));

    private const string EscapedUrlTemplate = """
    {"resourceSpans":[{"resource":{"attributes":[
        {"key":"service.name","value":{"stringValue":"Wallet.API"}}
      ]},
      "scopeSpans":[{"spans":[
        {"traceId":"11111111111111111111111111111111","spanId":"1111111111111111",
         "name":"short-escaped","kind":3,
         "startTimeUnixNano":"1783953780000000000","endTimeUnixNano":"1783953780250000000",
         "attributes":[{"key":"url.full","value":{"stringValue":"@@URL1@@"}}]},
        {"traceId":"22222222222222222222222222222222","spanId":"2222222222222222",
         "name":"long-escaped","kind":3,
         "startTimeUnixNano":"1783953780000000000","endTimeUnixNano":"1783953780250000000",
         "attributes":[{"key":"url.full","value":{"stringValue":"@@URL2@@"}}]},
        {"traceId":"33333333333333333333333333333333","spanId":"3333333333333333",
         "name":"not-ours","kind":3,
         "startTimeUnixNano":"1783953780000000000","endTimeUnixNano":"1783953780250000000",
         "attributes":[{"key":"url.full","value":{"stringValue":"@@URL3@@"}}]}
      ]}]
    }]}
    """;
}
