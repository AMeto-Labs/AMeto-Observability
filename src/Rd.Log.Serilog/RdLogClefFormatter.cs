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

  // Cached level strings — avoids LogEventLevel.ToString() allocation per event.
  private static readonly string[] s_levelStrings =
      ["Verbose", "Debug", "Information", "Warning", "Error", "Fatal"];

  public static void Write(scoped ref MessagePackWriter writer, LogEvent evt, ReadOnlyMemory<byte> serviceNameUtf8)
  {
    bool hasException = evt.Exception is not null;

    bool hasService = !serviceNameUtf8.IsEmpty;
    int extraFields = (evt.TraceId is not null ? 1 : 0) + (evt.SpanId is not null ? 1 : 0) + (hasService ? 1 : 0);
    int fieldCount = 3 
                    + (hasException ? 1 : 0)
                    + extraFields
                    + evt.Properties.Count;

    writer.WriteMapHeader(fieldCount);

    // Timestamp — stackalloc avoids one string allocation per event.
    Span<char> tsCharBuf = stackalloc char[33];
    evt.Timestamp.TryFormat(tsCharBuf, out int tsLen, "o", CultureInfo.InvariantCulture);
    writer.WriteString("@t"u8);
    WriteAsciiString(ref writer, tsCharBuf[..tsLen]);

    writer.WriteString("@mt"u8);
    writer.Write(evt.MessageTemplate.Text);

    writer.WriteString("@l"u8);
    int li = (int)evt.Level;
    writer.Write((uint)li < (uint)s_levelStrings.Length ? s_levelStrings[li] : evt.Level.ToString());

    if (hasException)
    {
      writer.WriteString("@x"u8);
      WriteException(ref writer, evt.Exception!, 0);
    }

    if (evt.TraceId.HasValue)
    {
      writer.WriteString("@tr"u8);
      writer.Write(evt.TraceId.Value.ToHexString());
    }

    if (evt.SpanId.HasValue)
    {
      writer.WriteString("@sp"u8);
      writer.Write(evt.SpanId.Value.ToHexString());
    }
    if (hasService) { writer.WriteString("service.name"u8); writer.WriteString(serviceNameUtf8.Span); }

    foreach (var kv in evt.Properties)
    {
      writer.Write(kv.Key);
      WriteValue(ref writer, kv.Value);
    }
  }

  // ── Property values ──────────────────────────────────────────────────────

  private static void WriteValue(scoped ref MessagePackWriter writer, LogEventPropertyValue value)
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
          writer.WriteString("$type"u8);
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

  private static void WriteScalar(scoped ref MessagePackWriter writer, object? v)
  {
    // Shared stack buffer — 64 chars covers all formatted scalar types (Guid=36, ISO-8601=33, etc.)
    Span<char> charBuf = stackalloc char[64];
    int len;
    switch (v)
    {
      case null: writer.WriteNil(); break;
      case string s: writer.Write(s); break;
      case bool b: writer.Write(b); break;
      case byte by: writer.Write(by); break;
      case sbyte sb: writer.Write(sb); break;
      case short sh: writer.Write(sh); break;
      case ushort us: writer.Write(us); break;
      case int i: writer.Write(i); break;
      case uint ui: writer.Write(ui); break;
      case long l: writer.Write(l); break;
      case ulong ul: writer.Write(ul); break;
      case float f: writer.Write(f); break;
      case double d: writer.Write(d); break;
      case decimal dec: writer.Write(dec.ToString(CultureInfo.InvariantCulture)); break;
      case DateTime dt:
        dt.TryFormat(charBuf, out len, "o", CultureInfo.InvariantCulture);
        WriteAsciiString(ref writer, charBuf[..len]);
        break;
      case DateTimeOffset dto:
        dto.TryFormat(charBuf, out len, "o", CultureInfo.InvariantCulture);
        WriteAsciiString(ref writer, charBuf[..len]);
        break;
      case TimeSpan ts:
        ts.TryFormat(charBuf, out len, "c");
        WriteAsciiString(ref writer, charBuf[..len]);
        break;
      case Guid g:
        g.TryFormat(charBuf, out len, "D");
        WriteAsciiString(ref writer, charBuf[..len]);
        break;
      case Uri u: writer.Write(u.ToString()); break;
      case Enum e: writer.Write(e.ToString()); break;
      case byte[] bytes: writer.Write(bytes); break;
      default: writer.Write(Convert.ToString(v, CultureInfo.InvariantCulture)); break;
    }
  }

  // ── Exception ────────────────────────────────────────────────────────────

  private static void WriteException(scoped ref MessagePackWriter writer, Exception ex, int depth)
  {
    bool hasMessage = !string.IsNullOrEmpty(ex.Message);
    bool hasStack = !string.IsNullOrEmpty(ex.StackTrace);
    bool hasInner = ex.InnerException is not null && depth + 1 < MaxExceptionDepth;

    int fields = 1 /* type */
               + (hasMessage ? 1 : 0)
               + (hasStack ? 1 : 0)
               + (hasInner ? 1 : 0);

    writer.WriteMapHeader(fields);

    writer.WriteString("type"u8);
    writer.Write(ex.GetType().FullName ?? ex.GetType().Name);

    if (hasMessage)
    {
      writer.WriteString("msg"u8);
      writer.Write(ex.Message);
    }

    if (hasStack)
    {
      writer.WriteString("stk"u8);
      writer.Write(ex.StackTrace);
    }

    if (hasInner)
    {
      writer.WriteString("inner"u8);
      WriteException(ref writer, ex.InnerException!, depth + 1);
    }
  }

  /// <summary>
  /// Writes all-ASCII <paramref name="chars"/> as a MessagePack string directly via
  /// <c>GetSpan</c>/<c>Advance</c>, avoiding CS8352 (stackalloc Span passed to a ref-struct method).
  /// Suitable for timestamps, GUIDs, TimeSpan, DateTime — all produce ASCII-only output.
  /// </summary>
  private static void WriteAsciiString(scoped ref MessagePackWriter writer, scoped ReadOnlySpan<char> chars)
  {
    int len = chars.Length;
    int headerSize;
    Span<byte> span;
    if (len <= 31)
    {
      span = writer.GetSpan(1 + len);
      span[0] = (byte)(0xa0 | len); // fixstr
      headerSize = 1;
    }
    else
    {
      span = writer.GetSpan(2 + len); // str8 — all our values are < 255 bytes
      span[0] = 0xd9;
      span[1] = (byte)len;
      headerSize = 2;
    }
    for (int i = 0; i < len; i++)
      span[headerSize + i] = (byte)chars[i]; // safe: all chars are ASCII (0–27f)
    writer.Advance(headerSize + len);
  }
}
