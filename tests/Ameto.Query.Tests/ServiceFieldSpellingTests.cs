using Ameto.Core;
using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

/// <summary>
/// The service is the built-in field <c>@service</c>, like <c>@tr</c> and <c>@sp</c>; its old
/// spellings — <c>service.name</c> (bare, which the parser turns into an encoded path, or
/// bracketed, which it keeps as one key) and <c>ServiceName</c> — stay aliases. All four must
/// resolve to the one header field in <see cref="BuiltinFields"/>, because the evaluator, the
/// index hint, the header pushdown and the aggregation road all ask that table and nothing else.
/// </summary>
public sealed class ServiceFieldSpellingTests
{
    [Theory]
    [InlineData("@service")]
    [InlineData("service.name")]          // ['service.name'] — the bracket escape keeps the dot
    [InlineData("service\u0001name")]     // service.name — the parser's encoded path
    [InlineData("ServiceName")]
    public void Every_spelling_resolves_to_the_header_field(string alias)
    {
        Assert.True(BuiltinFields.TryResolve(alias, out var field));
        Assert.Equal(BuiltinField.ServiceName, field);
    }

    [Theory]
    [InlineData("@services")]
    [InlineData("@Service")]              // aliases are ordinal, like every other built-in's
    [InlineData("service")]
    [InlineData("Service\u0001Name")]
    public void Near_misses_stay_user_properties(string name) =>
        Assert.False(BuiltinFields.TryResolve(name, out _));

    [Fact]
    public void The_hint_names_the_bucket_the_builder_writes()
    {
        Assert.Equal("@service", ClefFields.ServiceName);
        Assert.Equal("service.name", ClefFields.LegacyServiceName);
        Assert.Equal(ClefFields.ServiceName, BuiltinFields.InvertedKey(BuiltinField.ServiceName));
        Assert.True(BuiltinFields.IsBloomIndexed(BuiltinField.ServiceName));
        Assert.False(BuiltinFields.IsTrigramIndexed(BuiltinField.ServiceName));
    }

    [Theory]
    [InlineData("@service = 'Auth.API'",               true)]
    [InlineData("@service = 'auth.api'",               true)]    // strings compare case-insensitively
    [InlineData("service.name = 'Auth.API'",           true)]
    [InlineData("['service.name'] = 'Auth.API'",       true)]
    [InlineData("ServiceName = 'Auth.API'",            true)]
    [InlineData("@service in ['Billing', 'Auth.API']", true)]
    [InlineData("@service not in ['Billing']",         true)]
    [InlineData("contains(@service, 'th.A')",          true)]
    [InlineData("startsWith(@service, 'Auth')",        true)]
    [InlineData("@service like 'Auth%'",               true)]
    [InlineData("has(@service)",                       true)]
    [InlineData("@service <> 'Auth.API'",              false)]
    [InlineData("@service = 'Billing'",                false)]
    [InlineData("@service = null",                     false)]
    public void The_evaluator_reads_the_header_under_every_spelling(string filter, bool expected) =>
        Assert.Equal(expected, CompiledFilter.Compile(filter).Matches(Event("Auth.API")));

    [Theory]
    [InlineData("@service = null",         true)]
    [InlineData("has(@service)",           false)]
    [InlineData("@service = 'Auth.API'",   false)]
    public void An_event_without_a_service_has_no_service(string filter, bool expected) =>
        Assert.Equal(expected, CompiledFilter.Compile(filter).Matches(Event(service: null)));

    [Fact]
    public void A_user_property_of_the_same_name_is_shadowed_by_the_header()
    {
        // A client that sent a literal `@service` before it was a header key stored it as a
        // property. The built-in shadows it, exactly as `service.name` always shadowed a
        // property of that name — the index hint and the evaluator read the same table row.
        var ev = Event("Auth.API", new Dictionary<string, object?> { ["@service"] = "Rogue" });
        Assert.True(CompiledFilter.Compile("@service = 'Auth.API'").Matches(ev));
        Assert.False(CompiledFilter.Compile("@service = 'Rogue'").Matches(ev));
    }

    private static LogEvent Event(string? service, Dictionary<string, object?>? props = null) => new()
    {
        Id              = new EventId(0u, 1u),
        Timestamp       = new DateTimeOffset(2026, 10, 1, 9, 0, 0, TimeSpan.Zero),
        Level           = LogLevel.Information,
        MessageTemplate = "Handled {Request}",
        Properties      = props,
        ServiceName     = service,
    };
}
