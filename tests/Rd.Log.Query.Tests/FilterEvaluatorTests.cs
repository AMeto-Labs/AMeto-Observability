using Rd.Log.Core;
using Rd.Log.Query.Filtering;

namespace Rd.Log.Query.Tests;

/// <summary>
/// Tests for FilterEvaluator and CompiledFilter against real LogEvent instances.
/// </summary>
public sealed class FilterEvaluatorTests
{
    // ── Helpers ───────────────────────────────────────────────────────────────

    private static LogEvent MakeEvent(
        LogLevel level              = LogLevel.Information,
        string template             = "Hello {Name}",
        ExceptionInfo? exception    = null,
        Dictionary<string, object?>? props = null) => new()
    {
        Id              = new EventId(0u, 1u),
        Timestamp       = DateTimeOffset.UtcNow,
        Level           = level,
        MessageTemplate = template,
        Exception       = exception,
        Properties      = props,
    };

    private static bool Eval(string? filter, LogEvent ev) =>
        CompiledFilter.Compile(filter).Matches(ev);

    // ── Match-all ─────────────────────────────────────────────────────────────

    [Fact]
    public void Empty_Filter_MatchesEverything()
    {
        Assert.True(Eval(null,  MakeEvent()));
        Assert.True(Eval("",   MakeEvent(LogLevel.Fatal)));
        Assert.True(Eval("  ", MakeEvent(LogLevel.Debug)));
    }

    // ── Level comparisons ─────────────────────────────────────────────────────

    [Fact]
    public void LevelEquals_Error_MatchesErrorOnly()
    {
        var ev = MakeEvent(LogLevel.Error);
        Assert.True(Eval("@l = 'Error'", ev));
        Assert.False(Eval("@l = 'Error'", MakeEvent(LogLevel.Warning)));
    }

    [Fact]
    public void LevelNotEqual_FiltersCorrectly()
    {
        Assert.False(Eval("@l != 'Error'", MakeEvent(LogLevel.Error)));
        Assert.True(Eval("@l != 'Error'",  MakeEvent(LogLevel.Warning)));
    }

    // ── Property comparisons ──────────────────────────────────────────────────

    [Fact]
    public void PropertyEquals_StringValue_Matches()
    {
        var ev = MakeEvent(props: new() { ["UserId"] = "alice" });
        Assert.True(Eval("UserId = 'alice'", ev));
        Assert.False(Eval("UserId = 'bob'", ev));
    }

    [Fact]
    public void PropertyEquals_NumericValue_Matches()
    {
        var ev = MakeEvent(props: new() { ["StatusCode"] = (long)200 });
        Assert.True(Eval("StatusCode = 200", ev));
        Assert.False(Eval("StatusCode = 404", ev));
    }

    [Fact]
    public void PropertyGreaterThan_Matches()
    {
        var ev = MakeEvent(props: new() { ["Elapsed"] = 1000.0 });
        Assert.True(Eval("Elapsed > 500",  ev));
        Assert.False(Eval("Elapsed > 2000", ev));
    }

    [Fact]
    public void PropertyLessThan_Matches()
    {
        var ev = MakeEvent(props: new() { ["Elapsed"] = 100.0 });
        Assert.True(Eval("Elapsed < 500", ev));
        Assert.False(Eval("Elapsed < 50", ev));
    }

    // ── Built-in CLEF fields ──────────────────────────────────────────────────

    [Fact]
    public void MessageTemplate_Comparison_Matches()
    {
        var ev = MakeEvent(template: "User {Name} logged in");
        Assert.True(Eval("@mt = 'User {Name} logged in'", ev));
        Assert.False(Eval("@mt = 'other'", ev));
    }

    [Fact]
    public void Exception_Field_Matches()
    {
        var ev = MakeEvent(exception: new ExceptionInfo { Type = "System.NullReferenceException" });
        Assert.True(Eval("@x = 'System.NullReferenceException'", ev));
        Assert.False(Eval("@x = 'other'", ev));
    }

    // ── Logical connectives ───────────────────────────────────────────────────

    [Fact]
    public void AndFilter_BothMustMatch()
    {
        var ev = MakeEvent(LogLevel.Error, props: new() { ["UserId"] = "alice" });
        Assert.True(Eval("@l = 'Error' and UserId = 'alice'", ev));
        Assert.False(Eval("@l = 'Error' and UserId = 'bob'", ev));
    }

    [Fact]
    public void OrFilter_EitherCanMatch()
    {
        var error = MakeEvent(LogLevel.Error);
        var fatal = MakeEvent(LogLevel.Fatal);
        var info  = MakeEvent(LogLevel.Information);
        Assert.True(Eval("@l = 'Error' or @l = 'Fatal'", error));
        Assert.True(Eval("@l = 'Error' or @l = 'Fatal'", fatal));
        Assert.False(Eval("@l = 'Error' or @l = 'Fatal'", info));
    }

    [Fact]
    public void NotFilter_Inverts()
    {
        var info = MakeEvent(LogLevel.Information);
        Assert.True(Eval("not @l = 'Error'", info));
        Assert.False(Eval("not @l = 'Information'", info));
    }

    // ── has / isDefined ───────────────────────────────────────────────────────

    [Fact]
    public void Has_PropertyPresent_ReturnsTrue()
    {
        var ev = MakeEvent(props: new() { ["UserId"] = "x" });
        Assert.True(Eval("has(UserId)", ev));
        Assert.False(Eval("has(RequestId)", ev));
    }

    [Fact]
    public void IsDefined_PropertyPresent_ReturnsTrue()
    {
        var ev = MakeEvent(props: new() { ["Trace"] = "abc" });
        Assert.True(Eval("isDefined(Trace)", ev));
        Assert.False(Eval("isDefined(Missing)", ev));
    }

    // ── startsWith / contains ────────────────────────────────────────────────

    [Fact]
    public void StartsWith_MatchesPrefix()
    {
        var ev = MakeEvent(template: "User logged in");
        Assert.True(Eval("startsWith(@mt, 'User')", ev));
        Assert.False(Eval("startsWith(@mt, 'Admin')", ev));
    }

    [Fact]
    public void CiStartsWith_CaseInsensitive()
    {
        var ev = MakeEvent(template: "USER logged in");
        Assert.True(Eval("ci_startsWith(@mt, 'user')", ev));
    }

    [Fact]
    public void Contains_MatchesSubstring()
    {
        var ev = MakeEvent(template: "Request failed with error");
        Assert.True(Eval("contains(@mt, 'failed')", ev));
        Assert.False(Eval("contains(@mt, 'succeeded')", ev));
    }

    [Fact]
    public void CiContains_CaseInsensitive()
    {
        var ev = MakeEvent(template: "Request FAILED with error");
        Assert.True(Eval("ci_contains(@mt, 'failed')", ev));
    }

    // ── in operator ───────────────────────────────────────────────────────────

    [Fact]
    public void In_MatchesOneOfValues()
    {
        var error = MakeEvent(LogLevel.Error);
        var debug = MakeEvent(LogLevel.Debug);
        Assert.True(Eval("@l in ['Error', 'Fatal']", error));
        Assert.False(Eval("@l in ['Error', 'Fatal']", debug));
    }

    // ── null / missing properties ─────────────────────────────────────────────

    [Fact]
    public void MissingProperty_Equals_Null_IsFalse()
    {
        var ev = MakeEvent(props: null);
        // No properties at all — comparing missing prop to value should be false
        Assert.False(Eval("StatusCode = 200", ev));
    }

    [Fact]
    public void Has_NullProperties_ReturnsFalse()
    {
        Assert.False(Eval("has(UserId)", MakeEvent(props: null)));
    }
}

public sealed class CompiledFilterTests
{
    // ── IsMatchAll ────────────────────────────────────────────────────────────

    [Fact]
    public void IsMatchAll_EmptyExpression_IsTrue()
    {
        Assert.True(CompiledFilter.Compile(null).IsMatchAll);
        Assert.True(CompiledFilter.Compile("").IsMatchAll);
    }

    [Fact]
    public void IsMatchAll_NonEmptyExpression_IsFalse()
    {
        Assert.False(CompiledFilter.Compile("@l = 'Error'").IsMatchAll);
    }

    // ── TryGetIndexHint ───────────────────────────────────────────────────────

    [Fact]
    public void TryGetIndexHint_EqualityCompare_ReturnsHint()
    {
        var f = CompiledFilter.Compile("UserId = 'alice'");
        bool found = f.TryGetIndexHint(out string prop, out object? val);
        Assert.True(found);
        Assert.Equal("UserId", prop);
        Assert.Equal("alice",  val);
    }

    [Fact]
    public void TryGetIndexHint_MatchAll_ReturnsFalse()
    {
        var f = CompiledFilter.Compile(null);
        Assert.False(f.TryGetIndexHint(out _, out _));
    }
}
