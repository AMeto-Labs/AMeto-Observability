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

    /// <summary>UTF-8 span overload — parses a 32-char hex TraceId with no string allocation.</summary>
    public static bool TryParseTraceId(ReadOnlySpan<byte> hex, out ulong hi, out ulong lo)
    {
        hi = lo = 0;
        if (hex.Length != 32) return false;
        return TryParseHexU64(hex[..16], out hi) && TryParseHexU64(hex[16..], out lo);
    }

    /// <summary>UTF-8 span overload — parses a 16-char hex SpanId with no string allocation.</summary>
    public static bool TryParseSpanId(ReadOnlySpan<byte> hex, out ulong spanId)
    {
        spanId = 0;
        return hex.Length == 16 && TryParseHexU64(hex, out spanId);
    }

    private static bool TryParseHexU64(ReadOnlySpan<byte> hex, out ulong value)
    {
        ulong v = 0;
        for (int i = 0; i < hex.Length; i++)
        {
            int d = HexDigit(hex[i]);
            if (d < 0) { value = 0; return false; }
            v = (v << 4) | (uint)d;
        }
        value = v;
        return true;
    }

    private static int HexDigit(byte c) => c switch
    {
        >= (byte)'0' and <= (byte)'9' => c - (byte)'0',
        >= (byte)'a' and <= (byte)'f' => c - (byte)'a' + 10,
        >= (byte)'A' and <= (byte)'F' => c - (byte)'A' + 10,
        _ => -1,
    };

    // ── Format ────────────────────────────────────────────────────────────────

    /// <summary>Returns the 32-char lowercase hex representation of a TraceId, or null when both parts are zero.</summary>
    public static string? FormatTraceId(ulong hi, ulong lo)
        => (hi | lo) != 0 ? $"{hi:x16}{lo:x16}" : null;

    /// <summary>Returns the 16-char lowercase hex representation of a SpanId, or null when zero.</summary>
    public static string? FormatSpanId(ulong spanId)
        => spanId != 0 ? $"{spanId:x16}" : null;
}
