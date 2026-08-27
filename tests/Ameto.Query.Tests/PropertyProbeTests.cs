using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

/// <summary>
/// A property predicate reads ONE value, and it used to pay for the whole map: a
/// dictionary, a string per key and a box per value, recursively, for every event a scan
/// touched. These tests pin both halves of the fix — that the answers are unchanged, and
/// that the map is not built to produce them.
/// </summary>
public sealed class PropertyProbeTests
{
    /// <summary>By ref: MessagePackWriter is a struct, so a by-value delegate writes to a copy.</summary>
    private delegate void WriteProps(ref MessagePackWriter w);

    private static LogEvent Event(WriteProps writeProps)
    {
        var buf = new ArrayBufferWriter<byte>(256);
        var w   = new MessagePackWriter(buf);
        writeProps(ref w);
        w.Flush();

        return new LogEvent
        {
            Id              = new EventId(0u, 1u),
            Timestamp       = new DateTimeOffset(2026, 8, 23, 10, 0, 0, TimeSpan.Zero),
            Level           = LogLevel.Information,
            MessageTemplate = "order {OrderId} shipped",
            RawProperties   = buf.WrittenMemory,
        };
    }

    /// <summary>Flat props: OrderId, Customer, a dotted OTLP-style key, an array and a number.</summary>
    private static LogEvent FlatEvent() => Event(static (ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(5);
        w.Write("OrderId");             w.Write("order-7");
        w.Write("Customer");            w.Write("cust-3");
        w.Write("http.request.method"); w.Write("GET");
        w.Write("Elapsed");             w.Write(125L);
        w.Write("Tags");                w.WriteArrayHeader(2); w.Write("red"); w.Write("blue");
    });

    private static bool Matches(LogEvent ev, string filter) => CompiledFilter.Compile(filter).Matches(ev);

    [Theory]
    [InlineData("Customer = 'cust-3'",            true)]
    [InlineData("Customer = 'nobody'",            false)]
    [InlineData("Missing = 'x'",                  false)]
    [InlineData("Elapsed > 100",                  true)]
    [InlineData("Elapsed > 200",                  false)]
    [InlineData("Tags = 'blue'",                  true)]   // match-any over an array value
    [InlineData("Tags = 'green'",                 false)]
    [InlineData("http.request.method = 'GET'",    true)]   // dotted name read as ONE key
    [InlineData("http.request.method = 'POST'",   false)]
    [InlineData("contains(OrderId, 'der-7')",     true)]
    public void Predicates_answer_the_same_and_never_build_the_map(string filter, bool expected)
    {
        var ev = FlatEvent();
        Assert.Equal(expected, Matches(ev, filter));
        Assert.False(ev.PropertiesMaterialised,
            "the filter read one property — the whole map must not have been deserialised");
    }

    [Fact]
    public void A_genuinely_nested_path_still_resolves()
    {
        var ev = Event(static (ref MessagePackWriter w) =>
        {
            w.WriteMapHeader(1);
            w.Write("user");
            w.WriteMapHeader(2);
            w.Write("name"); w.Write("ann");
            w.Write("id");   w.Write(42L);
        });

        Assert.True(Matches(ev, "user.name = 'ann'"));
        Assert.True(Matches(ev, "user.id = 42"));
        Assert.False(Matches(ev, "user.name = 'bob'"));
    }

    [Fact]
    public void A_missing_first_segment_short_circuits_without_building_the_map()
    {
        var ev = FlatEvent();
        Assert.False(Matches(ev, "user.name = 'ann'"));   // no 'user' key at all
        Assert.False(ev.PropertiesMaterialised);
    }

    [Fact]
    public void Probe_and_dictionary_agree_once_the_map_is_materialised()
    {
        var ev = FlatEvent();
        _ = ev.Properties;                                // delivery materialised it
        Assert.True(ev.PropertiesMaterialised);

        Assert.True(Matches(ev, "Customer = 'cust-3'"));
        Assert.True(Matches(ev, "http.request.method = 'GET'"));
        Assert.False(Matches(ev, "Customer = 'other'"));
    }

    /// <summary>
    /// OTLP concatenates resource attributes and record attributes into ONE flat map with
    /// no dedup, so a key set at both levels is written twice — record last. The dictionary
    /// keeps the last (dict[k] = v), and the probe has to agree, or the same event answers
    /// the same predicate differently depending on whether anything had already
    /// materialised the map.
    /// </summary>
    [Fact]
    public void A_duplicate_key_resolves_to_the_last_occurrence_like_the_dictionary()
    {
        LogEvent Dup() => Event(static (ref MessagePackWriter w) =>
        {
            w.WriteMapHeader(2);
            w.Write("deployment.environment"); w.Write("prod");      // resource attribute
            w.Write("deployment.environment"); w.Write("staging");   // record attribute wins
        });

        var probed = Dup();
        Assert.True(Matches(probed, "deployment.environment = 'staging'"));
        Assert.False(Matches(probed, "deployment.environment = 'prod'"));
        Assert.False(probed.PropertiesMaterialised);

        var materialised = Dup();
        _ = materialised.Properties;
        Assert.True(Matches(materialised, "deployment.environment = 'staging'"));
        Assert.False(Matches(materialised, "deployment.environment = 'prod'"));
    }

    [Fact]
    public void A_nil_first_occurrence_does_not_hide_a_later_nested_map()
    {
        // Same duplicate shape, but the head of a dotted path: OTLP writes nil for an
        // attribute with no value set, and the structured one can come after it. Taking
        // the first occurrence made the walk give up before it started.
        var ev = Event(static (ref MessagePackWriter w) =>
        {
            w.WriteMapHeader(2);
            w.Write("db"); w.WriteNil();
            w.Write("db"); w.WriteMapHeader(1); w.Write("name"); w.Write("orders");
        });

        Assert.True(Matches(ev, "db.name = 'orders'"));
    }

    [Fact]
    public void Keys_compare_ordinally_like_the_dictionary_does()
    {
        var ev = FlatEvent();
        Assert.False(Matches(ev, "customer = 'cust-3'"));   // wrong case is a different key
        Assert.True(Matches(ev, "Customer = 'cust-3'"));
    }
}
