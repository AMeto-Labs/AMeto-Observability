using System.Globalization;
using System.Text;
using Ameto.Core;
using Ameto.Mcp.Masking;

namespace Ameto.Mcp.Tools;

/// <summary>
/// Writes events the way a model reads them cheapest: grouped by message template, the template
/// printed once, the properties as a table under it.
///
/// <para>A page of log events is mostly repetition — the same template, the same service, the
/// same level, and often the same value in a column. Printed event by event as JSON, every row
/// pays for all of it again, and an answer runs out of its budget after a few dozen events.
/// Here a group pays for its template once, a value every row shares moves up into the group's
/// header line, and a row is what actually differs. The message is never rendered: the
/// template with its placeholders next to the property columns says the same thing, and says
/// it without repeating a hidden value's token inside a sentence.</para>
/// </summary>
internal static class EventTable
{
    private const int MaxColumns   = 14;
    private const int MaxCell      = 80;
    private const int MaxPadding   = 40;
    private const string ExceptionColumn = "@x";

    /// <summary>What the answer header itself costs, before any event.</summary>
    public const int HeaderCost = 200;

    /// <summary>
    /// What one more event adds to an answer, in characters, given the templates already in it.
    /// The caller adds events in the order the query returned them and stops when the budget is
    /// spent, so what is shown is a prefix of the result and the cursor after it is exact — the
    /// grouping below reorders rows, and a cut made after it would not be a prefix of anything.
    /// </summary>
    public static int CostOf(MaskedEvent ev, HashSet<string> seenTemplates)
    {
        int cost = 30;   // reference, time, separators
        if (seenTemplates.Add(ev.Template)) cost += Math.Min(ev.Template.Length, 400) + 60;
        foreach (var (_, value) in ev.Properties) cost += Math.Min(value.Length, MaxCell) + 2;
        if (ev.Exception is { } x) cost += Math.Min(x.Type.Length + (x.Message?.Length ?? 0) + 2, MaxCell) + 2;
        return cost;
    }

    /// <summary>Appends the events, grouped by template in order of first appearance.</summary>
    public static void Write(StringBuilder sb, IReadOnlyList<MaskedEvent> events)
    {
        if (events.Count == 0) return;

        bool oneDay = true;
        var day = events[0].Source.Timestamp.UtcDateTime.Date;
        foreach (var ev in events)
            if (ev.Source.Timestamp.UtcDateTime.Date != day) { oneDay = false; break; }
        if (oneDay) sb.Append("date ").Append(day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)).Append(" (UTC)\n");

        var groups = new List<List<MaskedEvent>>();
        var byTemplate = new Dictionary<string, List<MaskedEvent>>(StringComparer.Ordinal);
        foreach (var ev in events)
        {
            if (!byTemplate.TryGetValue(ev.Template, out var group))
            {
                byTemplate[ev.Template] = group = [];
                groups.Add(group);
            }
            group.Add(ev);
        }

        for (int g = 0; g < groups.Count; g++)
        {
            sb.Append('\n');
            WriteGroup(sb, g + 1, groups[g], oneDay);
        }
    }

    private static void WriteGroup(StringBuilder sb, int number, List<MaskedEvent> group, bool oneDay)
    {
        sb.Append('[').Append(number).Append("] ").Append(OneLine(group[0].Template, 400)).Append('\n');

        // Every row's cells by column, in order of first appearance. Level and service are
        // columns like any other, so that a group all at one level says so once.
        var columns = new List<string> { "@l", "@service" };
        var index   = new Dictionary<string, int>(StringComparer.Ordinal) { ["@l"] = 0, ["@service"] = 1 };
        var rows    = new List<string?[]>(group.Count);
        bool anyException = false;

        foreach (var ev in group)
        {
            foreach (var (key, _) in ev.Properties)
                if (!index.ContainsKey(key)) { index[key] = columns.Count; columns.Add(key); }
            anyException |= ev.Exception is not null;
        }
        if (anyException) { index[ExceptionColumn] = columns.Count; columns.Add(ExceptionColumn); }

        foreach (var ev in group)
        {
            var row = new string?[columns.Count];
            row[0] = ev.Source.Level.ToSeqString();
            row[1] = ev.Source.ServiceName;
            foreach (var (key, value) in ev.Properties) row[index[key]] = Cell(value);
            if (ev.Exception is { } x)
                row[index[ExceptionColumn]] = Cell(x.Message is null ? x.Type : $"{x.Type}: {x.Message}");
            rows.Add(row);
        }

        // A column every row shares moves into the header line. Not for a group of one, where
        // everything is "shared" and the row would be left empty.
        var shown = new List<int>();
        sb.Append('×').Append(group.Count);
        for (int c = 0; c < columns.Count; c++)
        {
            string? first = rows[0][c];
            bool constant = group.Count > 1 || c < 2;
            for (int r = 1; constant && r < rows.Count; r++) constant = string.Equals(rows[r][c], first, StringComparison.Ordinal);

            if (!constant) { shown.Add(c); continue; }
            if (first is null) continue;
            sb.Append(" · ");
            if (c >= 2) sb.Append(columns[c]).Append(" = ");
            sb.Append(first);
        }
        sb.Append('\n');

        int hiddenColumns = Math.Max(0, shown.Count - MaxColumns);
        if (hiddenColumns > 0)
        {
            sb.Append("(+").Append(hiddenColumns).Append(" more properties not shown: ");
            for (int i = MaxColumns; i < shown.Count; i++)
            {
                if (i > MaxColumns) sb.Append(", ");
                sb.Append(columns[shown[i]]);
            }
            sb.Append(" — name the ones you need in 'fields')\n");
            shown.RemoveRange(MaxColumns, hiddenColumns);
        }

        // Column widths: reference, time, then the properties that differ between rows.
        string timeFormat = oneDay ? "HH:mm:ss.fff" : "MM-dd HH:mm:ss.fff";
        var refs  = new string[group.Count];
        int refWidth = "ref".Length;
        for (int r = 0; r < group.Count; r++)
        {
            refs[r]  = EventRef.Format(group[r].Source);
            refWidth = Math.Max(refWidth, refs[r].Length);
        }
        var widths = new int[shown.Count];
        for (int i = 0; i < shown.Count; i++)
        {
            int width = columns[shown[i]].Length;
            foreach (var row in rows) width = Math.Max(width, row[shown[i]]?.Length ?? 0);
            widths[i] = Math.Min(width, MaxPadding);
        }

        Pad(sb, "ref", refWidth);
        Pad(sb, "@t", timeFormat.Length);
        for (int i = 0; i < shown.Count; i++) PadOrEnd(sb, columns[shown[i]], widths[i], last: i == shown.Count - 1);
        TrimLine(sb);

        for (int r = 0; r < group.Count; r++)
        {
            Pad(sb, refs[r], refWidth);
            Pad(sb, group[r].Source.Timestamp.UtcDateTime.ToString(timeFormat, CultureInfo.InvariantCulture), timeFormat.Length);
            for (int i = 0; i < shown.Count; i++) PadOrEnd(sb, rows[r][shown[i]] ?? "", widths[i], last: i == shown.Count - 1);
            TrimLine(sb);
        }
    }

    /// <summary>One event in full: every property on its own line, the whole exception.</summary>
    public static void WriteDetail(StringBuilder sb, MaskedEvent ev, int maxStackChars)
    {
        var e = ev.Source;
        sb.Append("ref       ").Append(EventRef.Format(e)).Append('\n');
        sb.Append("@t        ").Append(e.Timestamp.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", CultureInfo.InvariantCulture)).Append('\n');
        sb.Append("@l        ").Append(e.Level.ToSeqString()).Append('\n');
        if (e.ServiceName is { } service) sb.Append("@service  ").Append(service).Append('\n');
        if ((e.TraceIdHi | e.TraceIdLo) != 0) sb.Append("@tr       ").Append(TraceIdHelper.FormatTraceId(e.TraceIdHi, e.TraceIdLo)).Append('\n');
        if (e.SpanId != 0) sb.Append("@sp       ").Append(TraceIdHelper.FormatSpanId(e.SpanId)).Append('\n');
        sb.Append("@mt       ").Append(ev.Template).Append('\n');

        foreach (var (key, value) in ev.Properties)
            sb.Append(key).Append(" = ").Append(value.Length > 4_000 ? string.Concat(value.AsSpan(0, 4_000), "…") : value).Append('\n');

        int depth = 0;
        for (var x = ev.Exception; x is not null; x = x.Inner, depth++)
        {
            sb.Append(depth == 0 ? "@x        " : "caused by ").Append(x.Type);
            if (x.Message is not null) sb.Append(": ").Append(x.Message);
            sb.Append('\n');
            if (x.Stack is { Length: > 0 } stack)
            {
                if (stack.Length > maxStackChars)
                    sb.Append(stack, 0, maxStackChars).Append("\n… ").Append(stack.Length - maxStackChars).Append(" more characters of stack trace\n");
                else
                    sb.Append(stack).Append('\n');
            }
        }
    }

    /// <summary>
    /// Events one per line in the order given — for what happened around an event, where the
    /// order is the point and grouping by template would lose it.
    /// </summary>
    public static void WriteTimeline(StringBuilder sb, IReadOnlyList<MaskedEvent> events, ulong markId, bool withService)
    {
        foreach (var ev in events)
        {
            int start = sb.Length;
            var e = ev.Source;
            sb.Append(e.Id.RawValue == markId ? "> " : "  ");
            sb.Append(EventRef.Format(e)).Append("  ");
            sb.Append(e.Timestamp.UtcDateTime.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture)).Append("  ");
            sb.Append(ShortLevel(e.Level)).Append("  ");
            if (withService && e.ServiceName is { } service) sb.Append(service).Append("  ");
            sb.Append(OneLine(ev.Template, 140));
            foreach (var (key, value) in ev.Properties)
            {
                if (sb.Length - start > 300) { sb.Append(" …"); break; }
                sb.Append("  ").Append(key).Append('=').Append(OneLine(value, 40));
            }
            if (ev.Exception is { } x) sb.Append("  @x=").Append(OneLine(x.Message is null ? x.Type : x.Type + ": " + x.Message, 100));
            sb.Append('\n');
        }
    }

    private static string ShortLevel(LogLevel level) => level switch
    {
        LogLevel.Verbose => "VRB", LogLevel.Debug => "DBG", LogLevel.Warning => "WRN",
        LogLevel.Error => "ERR", LogLevel.Fatal => "FTL", _ => "INF",
    };

    /// <summary>A plain table: header, rows, cells padded to their column.</summary>
    public static void WriteGrid(StringBuilder sb, IReadOnlyList<string> header, IReadOnlyList<string?[]> rows)
    {
        var widths = new int[header.Count];
        for (int c = 0; c < header.Count; c++)
        {
            int width = header[c].Length;
            foreach (var row in rows) width = Math.Max(width, row[c]?.Length ?? 4);
            widths[c] = Math.Min(width, MaxPadding * 2);
        }
        for (int c = 0; c < header.Count; c++) PadOrEnd(sb, header[c], widths[c], last: c == header.Count - 1);
        TrimLine(sb);
        foreach (var row in rows)
        {
            for (int c = 0; c < header.Count; c++) PadOrEnd(sb, row[c] ?? "null", widths[c], last: c == header.Count - 1);
            TrimLine(sb);
        }
    }

    /// <summary>A value as one table cell: on one line, and no longer than a cell may be.</summary>
    public static string Cell(string value) => OneLine(value, MaxCell);

    private static string OneLine(string value, int max)
    {
        bool clean = value.Length <= max;
        for (int i = 0; clean && i < value.Length; i++) clean = value[i] is not ('\n' or '\r' or '\t');
        if (clean) return value;

        var sb = new StringBuilder(Math.Min(value.Length, max) + 1);
        foreach (char c in value)
        {
            if (sb.Length >= max) { sb.Append('…'); break; }
            sb.Append(c switch { '\n' => '⏎', '\r' or '\t' => ' ', _ => c });
        }
        return sb.ToString();
    }

    private static void Pad(StringBuilder sb, string text, int width)
    {
        sb.Append(text);
        sb.Append(' ', Math.Max(0, width - text.Length) + 2);
    }

    private static void PadOrEnd(StringBuilder sb, string text, int width, bool last)
    {
        sb.Append(text);
        if (!last) sb.Append(' ', Math.Max(0, width - text.Length) + 2);
    }

    /// <summary>Ends the line, dropping the padding after its last cell.</summary>
    private static void TrimLine(StringBuilder sb)
    {
        int end = sb.Length;
        while (end > 0 && sb[end - 1] == ' ') end--;
        sb.Length = end;
        sb.Append('\n');
    }
}
