using System.Globalization;

namespace Ameto.Core;

/// <summary>
/// Utility methods for converting TraceId / SpanId between binary (two ulong) and
/// 32/16-character lowercase hex string representations.
/// </summary>
public static class TraceIdHelper
{
    // ── Parse ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Parses a 32-char hex TraceId string into (hi, lo) ulong pair.
    /// Returns false and sets both to 0 for null / wrong-length / non-hex input.
    /// </summary>
    public static bool TryParseTraceId(string? hex, out ulong hi, out ulong lo)
    {
        hi = lo = 0;
        if (hex is null || hex.Length != 32) return false;
        return ulong.TryParse(hex.AsSpan(0, 16), NumberStyles.HexNumber, null, out hi)
            && ulong.TryParse(hex.AsSpan(16, 16), NumberStyles.HexNumber, null, out lo);
    }

    /// <summary>
    /// Parses a 16-char hex SpanId string into a ulong.
    /// Returns false and sets to 0 for null / wrong-length / non-hex input.
    /// </summary>
    public static bool TryParseSpanId(string? hex, out ulong spanId)
    {
        spanId = 0;
        if (hex is null || hex.Length != 16) return false;
        return ulong.TryParse(hex, NumberStyles.HexNumber, null, out spanId);
    }

    // ── Format ────────────────────────────────────────────────────────────────

    /// <summary>Returns the 32-char lowercase hex representation of a TraceId, or null when both parts are zero.</summary>
    public static string? FormatTraceId(ulong hi, ulong lo)
        => (hi | lo) != 0 ? $"{hi:x16}{lo:x16}" : null;

    /// <summary>Returns the 16-char lowercase hex representation of a SpanId, or null when zero.</summary>
    public static string? FormatSpanId(ulong spanId)
        => spanId != 0 ? $"{spanId:x16}" : null;
}
