namespace Ameto.Core;

/// <summary>
/// Severity levels, numerically ordered — matches Serilog/Seq convention.
/// Values are stored as a single byte in segments and ring buffer slots.
/// </summary>
public enum LogLevel : byte
{
    Verbose     = 0,
    Debug       = 1,
    Information = 2,
    Warning     = 3,
    Error       = 4,
    Fatal       = 5,
}

public static class LogLevelExtensions
{
    public static string ToSeqString(this LogLevel level) => level switch
    {
        LogLevel.Verbose     => "Verbose",
        LogLevel.Debug       => "Debug",
        LogLevel.Information => "Information",
        LogLevel.Warning     => "Warning",
        LogLevel.Error       => "Error",
        LogLevel.Fatal       => "Fatal",
        _                    => "Information",
    };

    public static bool TryParse(ReadOnlySpan<char> value, out LogLevel level)
    {
        if (value.Equals("Verbose",     StringComparison.OrdinalIgnoreCase)) { level = LogLevel.Verbose;     return true; }
        if (value.Equals("Debug",       StringComparison.OrdinalIgnoreCase)) { level = LogLevel.Debug;       return true; }
        if (value.Equals("Information", StringComparison.OrdinalIgnoreCase)) { level = LogLevel.Information; return true; }
        if (value.Equals("Info",        StringComparison.OrdinalIgnoreCase)) { level = LogLevel.Information; return true; }
        if (value.Equals("Warning",     StringComparison.OrdinalIgnoreCase)) { level = LogLevel.Warning;     return true; }
        if (value.Equals("Warn",        StringComparison.OrdinalIgnoreCase)) { level = LogLevel.Warning;     return true; }
        if (value.Equals("Error",       StringComparison.OrdinalIgnoreCase)) { level = LogLevel.Error;       return true; }
        if (value.Equals("Fatal",       StringComparison.OrdinalIgnoreCase)) { level = LogLevel.Fatal;       return true; }
        level = LogLevel.Information;
        return false;
    }
}
