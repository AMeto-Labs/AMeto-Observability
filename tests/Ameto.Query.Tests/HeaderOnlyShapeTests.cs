using Ameto.Core;
using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

/// <summary>
/// A filter that only constrains the level and the service can be counted from event
/// headers instead of materialising every event — which is what the alert evaluator does
/// for the rules it re-runs every fifteen seconds. The detection has to be CONSERVATIVE:
/// a wrong "yes" silently changes what an alert fires on, so anything not proven
/// equivalent must answer no and fall back to the scan.
/// </summary>
public sealed class HeaderOnlyShapeTests
{
    private static (bool Ok, HashSet<LogLevel>? Levels, string? Service) Shape(string? filter)
    {
        var ok = CompiledFilter.Compile(filter).TryGetHeaderOnlyShape(out var levels, out var service);
        return (ok, levels, service);
    }

    [Fact]
    public void An_empty_filter_selects_everything()
    {
        var (ok, levels, service) = Shape(null);
        Assert.True(ok);
        Assert.Null(levels);
        Assert.Null(service);
    }

    [Theory]
    [InlineData("@l = 'Error'")]
    [InlineData("Error")]                      // bare level name
    public void A_single_level_is_recognised(string filter)
    {
        var (ok, levels, service) = Shape(filter);
        Assert.True(ok);
        Assert.Equal([LogLevel.Error], levels);
        Assert.Null(service);
    }

    [Fact]
    public void A_level_set_is_recognised()
    {
        var (ok, levels, _) = Shape("@l in ['Error', 'Fatal']");
        Assert.True(ok);
        Assert.Equal([LogLevel.Error, LogLevel.Fatal], levels!.Order().ToArray());
    }

    [Fact]
    public void An_or_of_levels_is_a_union()
    {
        var (ok, levels, _) = Shape("@l = 'Error' or @l = 'Fatal'");
        Assert.True(ok);
        Assert.Equal([LogLevel.Error, LogLevel.Fatal], levels!.Order().ToArray());
    }

    [Fact]
    public void Level_and_service_together_are_recognised()
    {
        var (ok, levels, service) = Shape("@l = 'Error' and service.name = 'checkout'");
        Assert.True(ok);
        Assert.Equal([LogLevel.Error], levels);
        Assert.Equal("checkout", service);
    }

    [Fact]
    public void Anded_level_constraints_intersect()
    {
        var (ok, levels, _) = Shape("@l in ['Error', 'Fatal'] and @l = 'Fatal'");
        Assert.True(ok);
        Assert.Equal([LogLevel.Fatal], levels);
    }

    [Theory]
    // LogLevelExtensions.TryParse accepts these aliases, but the SCAN compares the literal
    // against Level.ToSeqString() — so 'warn' matches nothing at all. Reading it as
    // Warning would turn a rule that has never fired into one counting the whole level.
    [InlineData("@l = 'info'")]
    [InlineData("@l = 'Info'")]
    [InlineData("@l = 'warn'")]
    [InlineData("Level = 'info'")]
    [InlineData("@l in ['info']")]
    [InlineData("@l in ['Error', 'warn']")]
    public void A_level_alias_the_scan_would_not_match_is_refused(string filter)
    {
        Assert.False(Shape(filter).Ok);
    }

    [Fact]
    public void Canonical_spellings_are_still_accepted_in_any_case()
    {
        Assert.True(Shape("@l = 'Information'").Ok);
        Assert.True(Shape("@l = 'ERROR'").Ok);
        Assert.True(Shape("@l = 'error'").Ok);
    }

    [Theory]
    // The aggregator reads an empty service filter as "every service" and invents
    // "(unknown)" for events that carry none — both mean something the scan does not.
    [InlineData("service.name = ''")]
    [InlineData("service.name = '(unknown)'")]
    [InlineData("service.name = '(Unknown)'")]
    public void A_service_literal_the_aggregator_would_misread_is_refused(string filter)
    {
        Assert.False(Shape(filter).Ok);
    }

    [Theory]
    // Everything below reaches past the header, or describes a set this shape cannot.
    [InlineData("Customer = 'x'")]                              // a user property
    [InlineData("contains(@mt, 'boom')")]                       // message text
    [InlineData("@l = 'Error' or Customer = 'x'")]              // OR across fields
    [InlineData("@l = 'Error' or service.name = 'checkout'")]   // OR reaching a service
    [InlineData("not (@l = 'Error')")]                          // negation
    [InlineData("@l != 'Error'")]                               // a comparison that is not equality
    [InlineData("service.name = 'a' and service.name = 'b'")]   // matches nothing; not expressible
    [InlineData("@l = 'Nonsense'")]                             // not a level at all
    [InlineData("@x.type = 'System.Exception'")]                // exception fields
    [InlineData("@t >= '2026-08-01T00:00:00Z'")]                // time is the caller's window, not ours
    public void Anything_else_falls_back_to_the_scan(string filter)
    {
        Assert.False(Shape(filter).Ok);
    }
}
