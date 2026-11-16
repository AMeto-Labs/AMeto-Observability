using System.Globalization;
using Ameto.Core;

namespace Ameto.Mcp.Tools;

/// <summary>
/// The time window of a tool call. A model writes "the last fifteen minutes" far more reliably
/// as <c>-15m</c> than as two ISO timestamps it has to compute from a clock it does not have,
/// so both spellings are taken.
/// </summary>
internal static class TimeWindow
{
    /// <summary>The window a call gets when it names none.</summary>
    public static readonly TimeSpan Default = TimeSpan.FromHours(1);

    /// <summary>
    /// Resolves <paramref name="from"/> / <paramref name="to"/> against <paramref name="now"/>.
    /// A call that names no start gets <paramref name="defaultSpan"/> before its end.
    /// A window wider than <paramref name="max"/> is narrowed to its most recent part and
    /// <paramref name="note"/> says so — an answer about less than was asked has to say it.
    /// </summary>
    public static bool TryResolve(
        string? from, string? to, DateTimeOffset now, TimeSpan max, TimeSpan defaultSpan,
        out DateTimeOffset fromUtc, out DateTimeOffset toUtc, out string? note, out string? error)
    {
        fromUtc = toUtc = default;
        note = error = null;

        if (!TryParseInstant(to, now, out var parsedTo))
        {
            error = $"'to' is not a time: '{to}'. Use an ISO-8601 timestamp, 'now', or a relative time such as -15m, -2h, -1d.";
            return false;
        }
        toUtc = parsedTo ?? now;

        if (!TryParseInstant(from, now, out var parsedFrom))
        {
            error = $"'from' is not a time: '{from}'. Use an ISO-8601 timestamp or a relative time such as -15m, -2h, -1d.";
            return false;
        }
        fromUtc = parsedFrom ?? toUtc - (defaultSpan < max ? defaultSpan : max);

        if (fromUtc > toUtc)
        {
            error = "'from' is later than 'to'.";
            return false;
        }
        if (toUtc - fromUtc > max)
        {
            fromUtc = toUtc - max;
            note = $"window narrowed to its last {Describe(max)}, the widest this profile may ask for";
        }
        return true;
    }

    /// <summary>Null for an absent value; false for one that is neither a timestamp nor relative.</summary>
    private static bool TryParseInstant(string? raw, DateTimeOffset now, out DateTimeOffset? value)
    {
        value = null;
        if (string.IsNullOrWhiteSpace(raw)) return true;
        ReadOnlySpan<char> text = raw.AsSpan().Trim();

        if (text.Equals("now", StringComparison.OrdinalIgnoreCase)) { value = now; return true; }

        if (text[0] == '-' && text.Length >= 3)
        {
            char unit = char.ToLowerInvariant(text[^1]);
            if (!double.TryParse(text[1..^1], NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out double amount) || amount < 0)
                return false;
            TimeSpan? span = unit switch
            {
                's' => TimeSpan.FromSeconds(amount),
                'm' => TimeSpan.FromMinutes(amount),
                'h' => TimeSpan.FromHours(amount),
                'd' => TimeSpan.FromDays(amount),
                'w' => TimeSpan.FromDays(amount * 7),
                _   => null,
            };
            if (span is not { } back || back > TimeSpan.FromDays(3660)) return false;
            value = now - back;
            return true;
        }

        if (!DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind | DateTimeStyles.AssumeUniversal, out var parsed))
            return false;
        value = parsed.ToUniversalTime();
        return true;
    }

    public static string Describe(TimeSpan span) =>
        span.TotalDays >= 1 && span.TotalDays == Math.Floor(span.TotalDays)   ? $"{span.TotalDays:0} d"
      : span.TotalHours >= 1 && span.TotalHours == Math.Floor(span.TotalHours) ? $"{span.TotalHours:0} h"
      : span.TotalMinutes >= 1                                                ? $"{span.TotalMinutes:0} min"
      :                                                                         $"{span.TotalSeconds:0} s";
}

/// <summary>
/// The reference of an event in an answer, short enough to copy and enough to find the event
/// again without a scan.
///
/// <para>An event id carries the millisecond of the event's own timestamp — but the id generator
/// keeps ids monotonic, so an event that arrives late is given a later millisecond than its
/// <c>@t</c>. The reference is therefore the id in base 36, and, only when the two differ, the
/// difference after a sign: <c>1z141z3w0c5b</c>, or <c>1z141z3w0c5b-2s7</c> for an event 3 607 ms
/// older than its id says. From either the event's own millisecond is known exactly, and a
/// query one millisecond wide finds it.</para>
/// </summary>
internal static class EventRef
{
    public static string Format(LogEvent ev)
    {
        long delta = EventMs(ev.Timestamp) - (long)ev.Id.TimestampMs;
        Span<char> buffer = stackalloc char[32];
        int n = Base36.Write(ev.Id.RawValue, buffer);
        if (delta != 0)
        {
            buffer[n++] = delta > 0 ? '+' : '-';
            n += Base36.Write((ulong)Math.Abs(delta), buffer[n..]);
        }
        return new string(buffer[..n]);
    }

    /// <summary>The id, and the millisecond (as UTC ticks) the event's own timestamp falls in.</summary>
    public static bool TryParse(ReadOnlySpan<char> text, out ulong id, out long msStartTicks)
    {
        id = 0;
        msStartTicks = 0;
        text = text.Trim();
        int sign = text.IndexOfAny('+', '-');
        if (!Base36.TryRead(sign < 0 ? text : text[..sign], out id)) return false;

        long delta = 0;
        if (sign >= 0)
        {
            if (!Base36.TryRead(text[(sign + 1)..], out ulong magnitude) || magnitude > long.MaxValue / 2) return false;
            delta = text[sign] == '-' ? -(long)magnitude : (long)magnitude;
        }
        long ms = (long)new EventId(id).TimestampMs + delta;
        msStartTicks = EventIdGenerator.Epoch + ms * TimeSpan.TicksPerMillisecond;
        return msStartTicks > 0 && msStartTicks < DateTime.MaxValue.Ticks - TimeSpan.TicksPerDay;
    }

    /// <summary>Milliseconds since the id epoch, rounded down — also for a time before it.</summary>
    private static long EventMs(DateTimeOffset timestamp)
    {
        long ticks = timestamp.UtcTicks - EventIdGenerator.Epoch;
        long ms = Math.DivRem(ticks, TimeSpan.TicksPerMillisecond, out long rest);
        return rest < 0 ? ms - 1 : ms;
    }
}

/// <summary>Numbers in base 36: the shortest spelling a model copies without loss of case.</summary>
internal static class Base36
{
    private const string Alphabet = "0123456789abcdefghijklmnopqrstuvwxyz";

    public static int Write(ulong value, Span<char> dest)
    {
        Span<char> digits = stackalloc char[14];
        int n = 0;
        do
        {
            digits[n++] = Alphabet[(int)(value % 36)];
            value /= 36;
        }
        while (value != 0);
        for (int i = 0; i < n; i++) dest[i] = digits[n - 1 - i];
        return n;
    }

    public static bool TryRead(ReadOnlySpan<char> text, out ulong value)
    {
        value = 0;
        if (text.IsEmpty || text.Length > 13) return false;
        foreach (char raw in text)
        {
            char c = char.ToLowerInvariant(raw);
            int digit = c is >= '0' and <= '9' ? c - '0' : c is >= 'a' and <= 'z' ? c - 'a' + 10 : -1;
            if (digit < 0) return false;
            UInt128 next = (UInt128)value * 36 + (ulong)digit;
            if (next > ulong.MaxValue) return false;
            value = (ulong)next;
        }
        return true;
    }
}

/// <summary>Where a search stopped: the timestamp and id of the last event it returned.</summary>
internal static class SearchCursor
{
    public static string Format(long timestampTicks, ulong id)
    {
        Span<char> buffer = stackalloc char[32];
        int n = Base36.Write((ulong)timestampTicks, buffer);
        buffer[n++] = '.';
        n += Base36.Write(id, buffer[n..]);
        return new string(buffer[..n]);
    }

    public static bool TryParse(ReadOnlySpan<char> text, out long timestampTicks, out ulong id)
    {
        timestampTicks = 0;
        id = 0;
        text = text.Trim();
        int dot = text.IndexOf('.');
        if (dot <= 0 || !Base36.TryRead(text[..dot], out ulong ticks) || ticks > (ulong)DateTime.MaxValue.Ticks) return false;
        timestampTicks = (long)ticks;
        return Base36.TryRead(text[(dot + 1)..], out id);
    }
}
