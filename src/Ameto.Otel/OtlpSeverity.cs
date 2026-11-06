using System.Text;
using Ameto.Core;

namespace Ameto.Otel;

/// <summary>
/// OTLP severity → Ameto level, ONE rule for every log road (#138 item 3): the protobuf parser, the JSON
/// stream parser and (in its own string form) the JSON DOM mapper. The number decides whenever it is set;
/// the text is the fallback — a case-insensitive prefix for the levels that have no synonyms, the whole
/// word for <c>WARN</c>/<c>WARNING</c> and <c>INFO</c>/<c>INFORMATION</c>, which have one each.
///
/// <para>The JSON stream parser had a rule of its own: two spellings per level (<c>ERROR</c>, <c>error</c>)
/// and a prefix for all six, read off the raw token. A record with no <c>severityNumber</c> and a
/// <c>severityText</c> of <c>"Error"</c> or <c>"Warning"</c> — what a logger that title-cases its levels
/// sends — was stored as Information over JSON and as Error over protobuf, from the same exporter.</para>
/// </summary>
internal static class OtlpSeverity
{
    /// <summary>The level for <paramref name="severityNumber"/>, or for <paramref name="severityText"/> when the number is 0.</summary>
    public static LogLevel Map(int severityNumber, ReadOnlySpan<byte> severityText)
    {
        if (severityNumber >= 21) return LogLevel.Fatal;
        if (severityNumber >= 17) return LogLevel.Error;
        if (severityNumber >= 13) return LogLevel.Warning;
        if (severityNumber >= 9)  return LogLevel.Information;
        if (severityNumber >= 5)  return LogLevel.Debug;
        if (severityNumber >= 1)  return LogLevel.Verbose;

        if (severityText.IsEmpty) return LogLevel.Information;
        if (StartsWithFold(severityText, "FATAL"u8)) return LogLevel.Fatal;
        if (StartsWithFold(severityText, "ERROR"u8)) return LogLevel.Error;
        if (Ascii.EqualsIgnoreCase(severityText, "WARN"u8) ||
            Ascii.EqualsIgnoreCase(severityText, "WARNING"u8)) return LogLevel.Warning;
        if (Ascii.EqualsIgnoreCase(severityText, "INFO"u8) ||
            Ascii.EqualsIgnoreCase(severityText, "INFORMATION"u8)) return LogLevel.Information;
        if (StartsWithFold(severityText, "DEBUG"u8)) return LogLevel.Debug;
        if (StartsWithFold(severityText, "TRACE"u8)) return LogLevel.Verbose;
        return LogLevel.Information;
    }

    private static bool StartsWithFold(ReadOnlySpan<byte> text, ReadOnlySpan<byte> prefix)
        => text.Length >= prefix.Length && Ascii.EqualsIgnoreCase(text[..prefix.Length], prefix);
}
