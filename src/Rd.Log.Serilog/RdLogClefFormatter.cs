using System.Globalization;
using MessagePack;
using Serilog.Events;

namespace Rd.Log.Serilog;

/// <summary>
/// Writes a single Serilog <see cref="LogEvent"/> as a MessagePack CLEF map
/// compatible with Rd.Log's ingestion endpoint.
///
/// Field layout matches <c>Rd.Log.Core.ClefFields</c>:
///   <c>@t</c>  ISO-8601 timestamp
///   <c>@mt</c> message template
///   <c>@l</c>  level string (Verbose|Debug|Information|Warning|Error|Fatal)
///   <c>@x</c>  optional exception (nested map, depth ≤ 3)
///   + arbitrary properties.
/// </summary>
internal static class RdLogClefFormatter
{
    private const int MaxExceptionDepth = 3;

    public static void Write(ref MessagePackWriter writer, LogEvent evt)
    {
        bool hasException = evt.Exception is not null;
        int  fieldCount   = 3 /* @t @mt @l */
                          + (hasException ? 1 : 0)
                          + evt.Properties.Count;

        writer.WriteMapHeader(fieldCount);

        writer.Write("@t");
        writer.Write(evt.Timestamp.ToString("o", CultureInfo.InvariantCulture));

        writer.Write("@mt");
        writer.Write(evt.MessageTemplate.Text);

        writer.Write("@l");
        writer.Write(evt.Level.ToString());

        if (hasException)
        {
            writer.Write("@x");
            WriteException(ref writer, evt.Exception!, 0);
        }

        foreach (var kv in evt.Properties)
        {
            writer.Write(kv.Key);
            WriteValue(ref writer, kv.Value);
        }
    }

    // ── Property values ──────────────────────────────────────────────────────

    private static void WriteValue(ref MessagePackWriter writer, LogEventPropertyValue value)
    {
        switch (value)
        {
            case ScalarValue scalar:
                WriteScalar(ref writer, scalar.Value);
                break;

            case SequenceValue seq:
                writer.WriteArrayHeader(seq.Elements.Count);
                foreach (var item in seq.Elements)
                    WriteValue(ref writer, item);
                break;

            case StructureValue str:
            {
                bool hasType = !string.IsNullOrEmpty(str.TypeTag);
                writer.WriteMapHeader(str.Properties.Count + (hasType ? 1 : 0));
                if (hasType)
                {
                    writer.Write("$type");
                    writer.Write(str.TypeTag);
                }
                foreach (var p in str.Properties)
                {
                    writer.Write(p.Name);
                    WriteValue(ref writer, p.Value);
                }
                break;
            }

            case DictionaryValue dict:
                writer.WriteMapHeader(dict.Elements.Count);
                foreach (var pair in dict.Elements)
                {
                    WriteScalar(ref writer, pair.Key.Value);
                    WriteValue(ref writer, pair.Value);
                }
                break;

            default:
                // Unknown subtype — fall back to its rendered form.
                writer.Write(value.ToString());
                break;
        }
    }

    private static void WriteScalar(ref MessagePackWriter writer, object? v)
    {
        switch (v)
        {
            case null:                writer.WriteNil();                                                  break;
            case string s:            writer.Write(s);                                                    break;
            case bool b:              writer.Write(b);                                                    break;
            case byte by:             writer.Write(by);                                                   break;
            case sbyte sb:            writer.Write(sb);                                                   break;
            case short sh:            writer.Write(sh);                                                   break;
            case ushort us:           writer.Write(us);                                                   break;
            case int i:               writer.Write(i);                                                    break;
            case uint ui:             writer.Write(ui);                                                   break;
            case long l:              writer.Write(l);                                                    break;
            case ulong ul:            writer.Write(ul);                                                   break;
            case float f:             writer.Write(f);                                                    break;
            case double d:            writer.Write(d);                                                    break;
            case decimal dec:         writer.Write(dec.ToString(CultureInfo.InvariantCulture));           break;
            case DateTime dt:         writer.Write(dt.ToString("o", CultureInfo.InvariantCulture));      break;
            case DateTimeOffset dto:  writer.Write(dto.ToString("o", CultureInfo.InvariantCulture));     break;
            case TimeSpan ts:         writer.Write(ts.ToString("c", CultureInfo.InvariantCulture));      break;
            case Guid g:              writer.Write(g.ToString());                                        break;
            case Uri u:               writer.Write(u.ToString());                                        break;
            case Enum e:              writer.Write(e.ToString());                                        break;
            case byte[] bytes:        writer.Write(bytes);                                               break;
            default:                  writer.Write(Convert.ToString(v, CultureInfo.InvariantCulture));   break;
        }
    }

    // ── Exception ────────────────────────────────────────────────────────────

    private static void WriteException(ref MessagePackWriter writer, Exception ex, int depth)
    {
        bool hasMessage = !string.IsNullOrEmpty(ex.Message);
        bool hasStack   = !string.IsNullOrEmpty(ex.StackTrace);
        bool hasInner   = ex.InnerException is not null && depth + 1 < MaxExceptionDepth;

        int fields = 1 /* type */
                   + (hasMessage ? 1 : 0)
                   + (hasStack   ? 1 : 0)
                   + (hasInner   ? 1 : 0);

        writer.WriteMapHeader(fields);

        writer.Write("type");
        writer.Write(ex.GetType().FullName ?? ex.GetType().Name);

        if (hasMessage)
        {
            writer.Write("msg");
            writer.Write(ex.Message);
        }

        if (hasStack)
        {
            writer.Write("stk");
            writer.Write(ex.StackTrace);
        }

        if (hasInner)
        {
            writer.Write("inner");
            WriteException(ref writer, ex.InnerException!, depth + 1);
        }
    }
}
