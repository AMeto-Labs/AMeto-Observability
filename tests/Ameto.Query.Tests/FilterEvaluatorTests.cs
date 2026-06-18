using Ameto.Core;
using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

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

    [Fact]
    public void EndsWith_MatchesSuffix()
    {
        var ev = MakeEvent(template: "Job completed");
        Assert.True(Eval("endsWith(@mt, 'pleted')", ev));
        Assert.False(Eval("endsWith(@mt, 'failed')", ev));
    }

    [Fact]
    public void Length_Compare_WorksForString()
    {
        var ev = MakeEvent(props: new() { ["UserId"] = "alice" });
        Assert.True(Eval("length(UserId) = 5", ev));
        Assert.True(Eval("length(UserId) > 3", ev));
        Assert.False(Eval("length(UserId) < 3", ev));
    }

    [Fact]
    public void ToJson_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["StatusCode"] = 200L });
        Assert.True(Eval("toJson(StatusCode) = '200'", ev));
    }

    [Fact]
    public void FromJson_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["JsonValue"] = "42" });
        Assert.True(Eval("fromJson(JsonValue) = 42", ev));
        Assert.False(Eval("fromJson(JsonValue) = 43", ev));
    }

    [Fact]
    public void FromJsonPath_TopLevel_Field_Works()
    {
        var ev = MakeEvent(props: new() { ["Body"] = """{"merchBal":155,"currency":"USD"}""" });
        Assert.True(Eval("fromJson(Body).merchBal = 155", ev));
        Assert.False(Eval("fromJson(Body).merchBal = 999", ev));
    }

    [Fact]
    public void FromJsonPath_Nested_Works()
    {
        var ev = MakeEvent(props: new() { ["Body"] = """{"user":{"balance":{"amount":42}}}""" });
        Assert.True(Eval("fromJson(Body).user.balance.amount = 42", ev));
    }

    [Fact]
    public void FromJsonPath_ArrayIndex_Works()
    {
        var ev = MakeEvent(props: new() { ["Body"] = """{"items":["first","second"]}""" });
        Assert.True(Eval("fromJson(Body).items[0] = 'first'", ev));
        Assert.True(Eval("fromJson(Body).items[1] = 'second'", ev));
    }

    [Fact]
    public void FromJsonPath_BracketNotation_Works()
    {
        var ev = MakeEvent(props: new() { ["Body"] = """{"user":{"name":"alice"}}""" });
        Assert.True(Eval("fromJson(Body)['user']['name'] = 'alice'", ev));
    }

    [Fact]
    public void FromJsonPath_MissingField_ReturnsFalse()
    {
        var ev = MakeEvent(props: new() { ["Body"] = """{"amount":100}""" });
        Assert.False(Eval("fromJson(Body).missing = 100", ev));
    }

    [Fact]
    public void FromJsonPath_InvalidJson_ReturnsFalse()
    {
        var ev = MakeEvent(props: new() { ["Body"] = "not-json" });
        Assert.False(Eval("fromJson(Body).field = 1", ev));
    }

    // ── fromJson path: like, in, has, string predicates ───────────────────────

    [Fact]
    public void FromJsonPath_Like_MatchesPattern()
    {
        // {"formattedBalance":"154.45AED","currency":"AED"}
        var ev = MakeEvent(props: new() { ["Body"] = """{"formattedBalance":"154.45AED","currency":"AED"}""" });
        Assert.True(Eval("fromJson(Body).formattedBalance like '%AED'", ev));
        Assert.True(Eval("fromJson(Body).formattedBalance like '154%'", ev));
        Assert.False(Eval("fromJson(Body).formattedBalance like '%USD'", ev));
    }

    [Fact]
    public void FromJsonPath_In_MatchesList()
    {
        var ev = MakeEvent(props: new() { ["Body"] = """{"currency":"AED"}""" });
        Assert.True(Eval("fromJson(Body).currency in ['AED','USD','EUR']", ev));
        Assert.False(Eval("fromJson(Body).currency in ['USD','EUR']", ev));
    }

    [Fact]
    public void FromJsonPath_In_NumericValues()
    {
        var ev = MakeEvent(props: new() { ["Body"] = """{"ResponseCode":0}""" });
        Assert.True(Eval("fromJson(Body).ResponseCode in [0, 1, 2]", ev));
        Assert.False(Eval("fromJson(Body).ResponseCode in [1, 2, 3]", ev));
    }

    [Fact]
    public void FromJsonPath_Has_FieldExists()
    {
        var ev = MakeEvent(props: new() { ["Body"] = """{"merchBal":155,"currency":"AED"}""" });
        Assert.True(Eval("has(fromJson(Body).merchBal)", ev));
        Assert.False(Eval("has(fromJson(Body).missingField)", ev));
    }

    [Fact]
    public void FromJsonPath_Contains_MatchesSubstring()
    {
        var ev = MakeEvent(props: new() { ["Body"] = """{"ResponseDescription":"Request Processed Successfully"}""" });
        Assert.True(Eval("contains(fromJson(Body).ResponseDescription, 'Processed')", ev));
        Assert.False(Eval("contains(fromJson(Body).ResponseDescription, 'Failed')", ev));
    }

    [Fact]
    public void FromJsonPath_CiContains_CaseInsensitive()
    {
        var ev = MakeEvent(props: new() { ["Body"] = """{"ResponseDescription":"Request Processed Successfully"}""" });
        Assert.True(Eval("ci_contains(fromJson(Body).ResponseDescription, 'successfully')", ev));
    }

    [Fact]
    public void FromJsonPath_StartsWith_MatchesPrefix()
    {
        var ev = MakeEvent(props: new() { ["Body"] = """{"formattedBalance":"154.45AED"}""" });
        Assert.True(Eval("startsWith(fromJson(Body).formattedBalance, '154')", ev));
        Assert.False(Eval("startsWith(fromJson(Body).formattedBalance, '999')", ev));
    }

    [Fact]
    public void FromJsonPath_EndsWith_MatchesSuffix()
    {
        var ev = MakeEvent(props: new() { ["Body"] = """{"formattedBalance":"154.45AED"}""" });
        Assert.True(Eval("endsWith(fromJson(Body).formattedBalance, 'AED')", ev));
        Assert.False(Eval("endsWith(fromJson(Body).formattedBalance, 'USD')", ev));
    }

    [Fact]
    public void FromJsonPath_AllNumericOps_Work()
    {
        // {"merchBal":155,"ResponseCode":0}
        var ev = MakeEvent(props: new() { ["Body"] = """{"merchBal":155,"ResponseCode":0}""" });
        Assert.True(Eval("fromJson(Body).merchBal = 155",  ev));
        Assert.True(Eval("fromJson(Body).merchBal != 100", ev));
        Assert.True(Eval("fromJson(Body).merchBal > 100",  ev));
        Assert.True(Eval("fromJson(Body).merchBal >= 155", ev));
        Assert.True(Eval("fromJson(Body).merchBal < 200",  ev));
        Assert.True(Eval("fromJson(Body).merchBal <= 155", ev));
        Assert.True(Eval("fromJson(Body).ResponseCode = 0", ev));
    }

    [Fact]
    public void FromJsonPath_CombinedAndExpression_Works()
    {
        var ev = MakeEvent(props: new() { ["Body"] = """{"merchBal":155,"currency":"AED","ResponseCode":0}""" });
        Assert.True(Eval("fromJson(Body).ResponseCode = 0 and fromJson(Body).currency = 'AED'", ev));
        Assert.False(Eval("fromJson(Body).ResponseCode = 0 and fromJson(Body).currency = 'USD'", ev));
    }

    [Fact]
    public void Coalesce_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["RequestId"] = "abc" });
        Assert.True(Eval("coalesce(UserId, RequestId, 'none') = 'abc'", ev));
        Assert.False(Eval("coalesce(UserId, RequestId, 'none') = 'none'", ev));
    }

    [Fact]
    public void ToLower_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["UserName"] = "ALIce" });
        Assert.True(Eval("toLower(UserName) = 'alice'", ev));
    }

    [Fact]
    public void ToUpper_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["UserName"] = "alice" });
        Assert.True(Eval("toUpper(UserName) = 'ALICE'", ev));
    }

    [Fact]
    public void ToNumber_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["StatusCode"] = "404" });
        Assert.True(Eval("toNumber(StatusCode) >= 400", ev));
    }

    [Fact]
    public void Substring_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["Path"] = "/api/users" });
        Assert.True(Eval("substring(Path, 1, 3) = 'api'", ev));
    }

    [Fact]
    public void IndexOf_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["Path"] = "/api/users" });
        Assert.True(Eval("indexOf(Path, '/api') = 0", ev));
        Assert.True(Eval("lastIndexOf(Path, '/') = 4", ev));
    }

    [Fact]
    public void Replace_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["Path"] = "/api/users" });
        Assert.True(Eval("replace(Path, '/api', '/v1') = '/v1/users'", ev));
    }

    [Fact]
    public void Concat_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["FirstName"] = "John", ["LastName"] = "Smith" });
        Assert.True(Eval("concat(FirstName, ' ', LastName) = 'John Smith'", ev));
    }

    [Fact]
    public void CiEndsWith_CaseInsensitive()
    {
        var ev = MakeEvent(template: "JOB COMPLETED");
        Assert.True(Eval("ci_endsWith(@mt, 'completed')", ev));
    }

    [Fact]
    public void TypeOf_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["UserId"] = "alice", ["Count"] = 3L });
        Assert.True(Eval("typeOf(UserId) = 'string'", ev));
        Assert.True(Eval("typeOf(Count) = 'number'", ev));
    }

    [Fact]
    public void ElementAt_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["Tags"] = new List<object?> { "first", "second" } });
        Assert.True(Eval("elementAt(Tags, 0) = 'first'", ev));
    }

    [Fact]
    public void KeysAndValues_Compare_Work()
    {
        var ev = MakeEvent(props: new() { ["Context"] = new Dictionary<string, object?> { ["A"] = 1L, ["B"] = "x" } });
        Assert.True(Eval("keys(Context) != null", ev));
        Assert.True(Eval("values(Context) != null", ev));
    }

    [Fact]
    public void Round_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["Duration"] = 3.14159 });
        Assert.True(Eval("round(Duration, 2) = 3.14", ev));
    }

    [Fact]
    public void Now_Compare_Works()
    {
        var ev = MakeEvent();
        Assert.True(Eval("now() > 0", ev));
    }

    [Fact]
    public void DateTime_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["StartAt"] = "2026-06-05T10:00:00Z" });
        Assert.True(Eval("dateTime(StartAt) > 0", ev));
    }

    [Fact]
    public void ToIsoString_Compare_Works()
    {
        var ticks = new DateTimeOffset(2026, 6, 5, 10, 0, 0, TimeSpan.Zero).UtcTicks;
        var ev = MakeEvent(props: new() { ["StartedTicks"] = ticks });
        Assert.True(Eval("toIsoString(StartedTicks, 0) != null", ev));
    }

    [Fact]
    public void DatePart_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["StartedAt"] = "2026-06-05T10:30:00Z" });
        Assert.True(Eval("datePart(StartedAt, 'hour', 0) = 10", ev));
    }

    [Fact]
    public void TimeOfDay_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["StartedAt"] = "2026-06-05T10:30:00Z" });
        Assert.True(Eval("timeOfDay(StartedAt, 0) > 0", ev));
    }

    [Fact]
    public void TimeSpan_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["DurationText"] = "00:00:05" });
        Assert.True(Eval("timeSpan(DurationText) = 50000000", ev));
    }

    [Fact]
    public void TotalMilliseconds_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["DurationTicks"] = 10_000_000L });
        Assert.True(Eval("totalMilliseconds(DurationTicks) = 1000", ev));
    }

    [Fact]
    public void ToTimeString_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["DurationTicks"] = 10_000_000L });
        Assert.True(Eval("toTimeString(DurationTicks) = '00:00:01'", ev));
    }

    [Fact]
    public void ToHexString_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["StatusCode"] = 200L });
        Assert.True(Eval("toHexString(StatusCode) = '0xc8'", ev));
    }

    [Fact]
    public void Bucket_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["Duration"] = 123.4 });
        Assert.True(Eval("bucket(Duration, 0.1) > 0", ev));
    }

    [Fact]
    public void OffsetIn_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["StartedAt"] = "2026-06-05T10:00:00Z" });
        Assert.True(Eval("offsetIn('UTC', StartedAt) = 0", ev));
    }

    [Fact]
    public void FromXml_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["Body"] = "<root><amount>155</amount></root>" });
        Assert.True(Eval("fromXml(Body, 'root/amount') = '155'", ev));
        Assert.False(Eval("fromXml(Body, 'root/amount') = '999'", ev));
    }

    [Fact]
    public void FromBase64_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["Encoded"] = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("hello")) });
        Assert.True(Eval("fromBase64(Encoded) = 'hello'", ev));
    }

    [Fact]
    public void ToBase64_Compare_Works()
    {
        var ev = MakeEvent(props: new() { ["Text"] = "hello" });
        var expected = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("hello"));
        Assert.True(Eval($"toBase64(Text) = '{expected}'", ev));
    }

    [Fact]
    public void RegexMatch_Works()
    {
        var ev = MakeEvent(props: new() { ["Path"] = "/api/users/123" });
        Assert.True(Eval("regexMatch(Path, '/api/users/[0-9]+')", ev));
        Assert.False(Eval("regexMatch(Path, '/api/admin')", ev));
    }

    [Fact]
    public void RegexExtract_Works()
    {
        var ev = MakeEvent(props: new() { ["Path"] = "/api/users/123" });
        Assert.True(Eval("regexExtract(Path, '([0-9]+)') = '123'", ev));
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
