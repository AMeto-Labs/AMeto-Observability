using Ameto.Core;

namespace Ameto.Core.Tests;

public sealed class LogLevelTests
{
    // ── ToSeqString ───────────────────────────────────────────────────────────

    [Theory]
    [InlineData(LogLevel.Verbose,     "Verbose")]
    [InlineData(LogLevel.Debug,       "Debug")]
    [InlineData(LogLevel.Information, "Information")]
    [InlineData(LogLevel.Warning,     "Warning")]
    [InlineData(LogLevel.Error,       "Error")]
    [InlineData(LogLevel.Fatal,       "Fatal")]
    public void ToSeqString_KnownLevel_ReturnsExpectedString(LogLevel level, string expected)
    {
        Assert.Equal(expected, level.ToSeqString());
    }

    [Fact]
    public void ToSeqString_UnknownValue_ReturnsInformation()
    {
        var unknown = (LogLevel)99;
        Assert.Equal("Information", unknown.ToSeqString());
    }

    // ── TryParse ──────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("Verbose",     LogLevel.Verbose)]
    [InlineData("VERBOSE",     LogLevel.Verbose)]
    [InlineData("verbose",     LogLevel.Verbose)]
    [InlineData("Debug",       LogLevel.Debug)]
    [InlineData("DEBUG",       LogLevel.Debug)]
    [InlineData("Information", LogLevel.Information)]
    [InlineData("information", LogLevel.Information)]
    [InlineData("Info",        LogLevel.Information)]
    [InlineData("INFO",        LogLevel.Information)]
    [InlineData("Warning",     LogLevel.Warning)]
    [InlineData("warning",     LogLevel.Warning)]
    [InlineData("Warn",        LogLevel.Warning)]
    [InlineData("WARN",        LogLevel.Warning)]
    [InlineData("Error",       LogLevel.Error)]
    [InlineData("ERROR",       LogLevel.Error)]
    [InlineData("Fatal",       LogLevel.Fatal)]
    [InlineData("FATAL",       LogLevel.Fatal)]
    public void TryParse_KnownStrings_ReturnsTrueAndCorrectLevel(string input, LogLevel expected)
    {
        bool ok = LogLevelExtensions.TryParse(input.AsSpan(), out var level);
        Assert.True(ok);
        Assert.Equal(expected, level);
    }

    [Theory]
    [InlineData("")]
    [InlineData("unknown")]
    [InlineData("trace")]
    [InlineData("critical")]
    public void TryParse_UnknownStrings_ReturnsFalseAndDefaultsToInformation(string input)
    {
        bool ok = LogLevelExtensions.TryParse(input.AsSpan(), out var level);
        Assert.False(ok);
        Assert.Equal(LogLevel.Information, level);
    }

    // ── Round-trip ────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(LogLevel.Verbose)]
    [InlineData(LogLevel.Debug)]
    [InlineData(LogLevel.Information)]
    [InlineData(LogLevel.Warning)]
    [InlineData(LogLevel.Error)]
    [InlineData(LogLevel.Fatal)]
    public void ToSeqString_ThenTryParse_RoundTrips(LogLevel level)
    {
        string s = level.ToSeqString();
        bool ok  = LogLevelExtensions.TryParse(s.AsSpan(), out var parsed);
        Assert.True(ok);
        Assert.Equal(level, parsed);
    }
}
