using System.Globalization;
using MessagePack;
using Serilog.Events;

namespace Serilog.Sinks.Ameto;

/// <summary>
/// How much of an exception the sink keeps once its <c>@x</c> overran <c>ExceptionLimitBytes</c>:
/// the type always, the message and the stack trace cut to a budget, inner exceptions dropped.
/// <see cref="Unlimited"/> is the first attempt at every exception.
/// </summary>
internal readonly record struct ExceptionBudget(int MaxMessageChars, int MaxStackChars, bool KeepInner)
{
  public static readonly ExceptionBudget Unlimited = new(int.MaxValue, int.MaxValue, KeepInner: true);

  /// <summary>
  /// The budget for a limit: about half of it for the stack trace, an eighth (128–4096 chars) for the
  /// message, no inner exceptions. Chars, not bytes, so the limit is a target rather than a cap: a
  /// mostly-ASCII stack trace lands the cut form under it, one full of non-ASCII paths and identifiers
  /// can run up to three times over, the type name is never cut, and the floors keep the cut form
  /// above a limit of a few hundred bytes. The server has no cap of its own on <c>@x</c>, and the
  /// request cap still bounds every request, so the sink does not measure a second time.
  /// </summary>
  public static ExceptionBudget For(int limitBytes) => new(
      MaxMessageChars: Math.Clamp(limitBytes / 8, 128, 4096),
      MaxStackChars:   Math.Max(256, limitBytes / 2),
      KeepInner:       false);
}

/// <summary>
/// Writes a single Serilog <see cref="LogEvent"/> as a MessagePack CLEF map
/// compatible with Ameto's ingestion endpoint.
///
/// Field layout matches <c>Ameto.Core.ClefFields</c>:
///   <c>@t</c>  ISO-8601 timestamp
///   <c>@mt</c> message template
///   <c>@l</c>  level string (Verbose|Debug|Information|Warning|Error|Fatal)
///   <c>@x</c>  optional exception (nested map, depth ≤ 3)
///   + arbitrary properties.
///
/// <para>The event is written in three parts — <see cref="WriteHeader"/>, <see cref="WriteException"/>,
/// <see cref="WriteProperties"/> — so that the sink can flush between them and measure the exception
/// and the properties separately: the server caps the PROPERTIES map (<c>Ingestion.MaxEventPayloadBytes</c>)
/// and takes the exception on its own. <see cref="Write"/> is the three in a row for a caller that
/// measures nothing.</para>
/// </summary>
internal static class AmetoClefFormatter
{
  private const int MaxExceptionDepth = 3;

  /// <summary>The most of a dropped event's template its marker keeps, in UTF-16 units (the server's marker keeps the same).</summary>
  internal const int MarkerTemplateChars = 256;

  /// <summary>Marker for an event whose properties map overran <c>PropertiesLimitBytes</c>.</summary>
  internal const string OversizedTemplate =
      "Log event properties of {EventBodyBytes} bytes exceeded the sink limit of {EventBodyLimitBytes} bytes; the event was dropped: {OriginalTemplate}";

  /// <summary>Marker for an event the server refused with 413 when it was sent on its own.</summary>
  internal const string RefusedTemplate =
      "Log event of {EventBodyBytes} bytes was refused by the server as too large; the event was dropped: {OriginalTemplate}";

  /// <summary>Marker for an event whose formatting threw (a property whose <c>ToString()</c> fails, say).</summary>
  internal const string FormatFailureTemplate =
      "Log event could not be serialised by the Ameto sink ({SinkError}); the event was dropped: {OriginalTemplate}";

  // Cached level strings — avoids LogEventLevel.ToString() allocation per event.
  private static readonly string[] s_levelStrings =
      ["Verbose", "Debug", "Information", "Warning", "Error", "Fatal"];

  /// <summary>The whole event, nothing measured: header, exception, properties.</summary>
  public static void Write(scoped ref MessagePackWriter writer, LogEvent evt, ReadOnlyMemory<byte> serviceNameUtf8)
  {
    WriteHeader(ref writer, evt, serviceNameUtf8);
    if (evt.Exception is not null)
    {
      writer.WriteString("@x"u8);
      WriteException(ref writer, evt.Exception, in ExceptionBudget.Unlimited);
    }
    WriteProperties(ref writer, evt);
  }

  /// <summary>
  /// The map header and the fields that need no measuring: <c>@t</c>, <c>@mt</c>, <c>@l</c>, <c>@tr</c>,
  /// <c>@sp</c>, <c>service.name</c>. The header counts the <c>@x</c> (when the event has an exception)
  /// and the properties the caller writes next, so the caller MUST write both — the exception with
  /// <see cref="WriteException"/> under its <c>@x</c> key, the properties with <see cref="WriteProperties"/>.
  /// </summary>
  public static void WriteHeader(scoped ref MessagePackWriter writer, LogEvent evt, ReadOnlyMemory<byte> serviceNameUtf8)
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
    writer.Write(LevelString(evt.Level));

    WriteTraceAndService(ref writer, evt, serviceNameUtf8);
  }

  /// <summary>
  /// The exception as the server's <c>ExceptionInfo</c> reads it: <c>{type, msg, stk, inner}</c>, inner
  /// exceptions to depth 3. The <paramref name="budget"/> cuts the message and the stack and drops the
  /// inner chain when the sink's limit asked for it.
  /// </summary>
  public static void WriteException(scoped ref MessagePackWriter writer, Exception ex, in ExceptionBudget budget)
      => WriteExceptionAtDepth(ref writer, ex, 0, in budget);

  /// <summary>The event's properties, key by key, as the server copies them into the event's payload.</summary>
  public static void WriteProperties(scoped ref MessagePackWriter writer, LogEvent evt)
  {
    // Serilog's LogEvent.Properties is a Dictionary<> behind the IReadOnlyDictionary<>
    // interface. Iterating the concrete type uses its struct enumerator instead of a
    // boxed IEnumerator — one fewer heap allocation per event on the logging hot path.
    if (evt.Properties is Dictionary<string, LogEventPropertyValue> propDict)
    {
      foreach (var kv in propDict)
      {
        writer.Write(kv.Key);
        WriteValue(ref writer, kv.Value);
      }
    }
    else
    {
      foreach (var kv in evt.Properties)
      {
        writer.Write(kv.Key);
        WriteValue(ref writer, kv.Value);
      }
    }
  }

  // ── Markers ──────────────────────────────────────────────────────────────
  //
  // A dropped event leaves a compact Error-level event in its place that keeps the original
  // timestamp, trace context and service and points at the offender through its (cut) template,
  // level and the measured sizes — searchable in Ameto by OriginalTemplate, DroppedBy or
  // EventBodyBytes. The field names are the server's own drop marker's (IngestionEndpoint), so
  // one filter finds both kinds; DroppedBy tells them apart ("sink" here, "server" there).

  /// <summary>Replacement for an event whose properties overran <c>PropertiesLimitBytes</c>.</summary>
  public static void WriteOversizedMarker(
      scoped ref MessagePackWriter writer, LogEvent evt,
      ReadOnlyMemory<byte> serviceNameUtf8, int propertiesBytes, int limitBytes)
  {
    WriteMarkerHeader(ref writer, evt, serviceNameUtf8, OversizedTemplate, extraFields: 5);

    writer.WriteString("DroppedBy"u8);
    writer.Write("sink");
    writer.WriteString("EventBodyBytes"u8);
    writer.Write(propertiesBytes);
    writer.WriteString("EventBodyLimitBytes"u8);
    writer.Write(limitBytes);
    WriteOriginal(ref writer, evt);
  }

  /// <summary>Replacement for an event the server answered 413 to when it was the only event of its request.</summary>
  public static void WriteRefusedMarker(
      scoped ref MessagePackWriter writer, LogEvent evt,
      ReadOnlyMemory<byte> serviceNameUtf8, int eventBytes)
  {
    WriteMarkerHeader(ref writer, evt, serviceNameUtf8, RefusedTemplate, extraFields: 4);

    writer.WriteString("DroppedBy"u8);
    writer.Write("sink");
    writer.WriteString("EventBodyBytes"u8);
    writer.Write(eventBytes);
    WriteOriginal(ref writer, evt);
  }

  /// <summary>Replacement for an event the formatter could not write; <paramref name="error"/> names what threw.</summary>
  public static void WriteFormatFailureMarker(
      scoped ref MessagePackWriter writer, LogEvent evt,
      ReadOnlyMemory<byte> serviceNameUtf8, string error)
  {
    WriteMarkerHeader(ref writer, evt, serviceNameUtf8, FormatFailureTemplate, extraFields: 4);

    writer.WriteString("DroppedBy"u8);
    writer.Write("sink");
    writer.WriteString("SinkError"u8);
    writer.Write(error.Length <= MarkerTemplateChars ? error : error[..MarkerTemplateChars]);
    WriteOriginal(ref writer, evt);
  }

  /// <summary>
  /// Map header plus <c>@t</c> (the original's), <c>@mt</c> (the marker's template), <c>@l</c> Error,
  /// trace context and service; <paramref name="extraFields"/> is how many properties the caller adds.
  /// Reads nothing of the event that can throw — timestamp, level, trace ids and the template text are
  /// plain values — so a marker for an event whose formatting failed cannot fail the same way.
  /// </summary>
  private static void WriteMarkerHeader(
      scoped ref MessagePackWriter writer, LogEvent evt,
      ReadOnlyMemory<byte> serviceNameUtf8, string template, int extraFields)
  {
    bool hasService = !serviceNameUtf8.IsEmpty;
    int fieldCount = 3                                     // @t @mt @l
                   + (evt.TraceId is not null ? 1 : 0)
                   + (evt.SpanId  is not null ? 1 : 0)
                   + (hasService ? 1 : 0)
                   + extraFields;

    writer.WriteMapHeader(fieldCount);

    Span<char> tsCharBuf = stackalloc char[33];
    evt.Timestamp.TryFormat(tsCharBuf, out int tsLen, "o", CultureInfo.InvariantCulture);
    writer.WriteString("@t"u8);
    WriteAsciiString(ref writer, tsCharBuf[..tsLen]);

    writer.WriteString("@mt"u8);
    writer.Write(template);

    writer.WriteString("@l"u8);
    writer.Write("Error");

    WriteTraceAndService(ref writer, evt, serviceNameUtf8);
  }

  /// <summary>The two fields every marker ends with: the original's (cut) template and its level.</summary>
  private static void WriteOriginal(scoped ref MessagePackWriter writer, LogEvent evt)
  {
    var template = evt.MessageTemplate.Text;
    writer.WriteString("OriginalTemplate"u8);
    writer.Write(template.Length <= MarkerTemplateChars ? template : template[..MarkerTemplateChars]);

    writer.WriteString("OriginalLevel"u8);
    writer.Write(LevelString(evt.Level));
  }

  private static void WriteTraceAndService(scoped ref MessagePackWriter writer, LogEvent evt, ReadOnlyMemory<byte> serviceNameUtf8)
  {
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
    if (!serviceNameUtf8.IsEmpty)
    {
      writer.WriteString("service.name"u8);
      writer.WriteString(serviceNameUtf8.Span);
    }
  }

  private static string LevelString(LogEventLevel level)
  {
    int li = (int)level;
    return (uint)li < (uint)s_levelStrings.Length ? s_levelStrings[li] : level.ToString();
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
      case decimal dec:
        dec.TryFormat(charBuf, out len, default, CultureInfo.InvariantCulture);
        WriteAsciiString(ref writer, charBuf[..len]);
        break;
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

  private static void WriteExceptionAtDepth(scoped ref MessagePackWriter writer, Exception ex, int depth, in ExceptionBudget budget)
  {
    string? message = ex.Message;
    string? stack   = ex.StackTrace;
    bool hasMessage = !string.IsNullOrEmpty(message);
    bool hasStack   = !string.IsNullOrEmpty(stack);
    bool hasInner   = budget.KeepInner && ex.InnerException is not null && depth + 1 < MaxExceptionDepth;

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
      writer.Write(Cut(message!, budget.MaxMessageChars));
    }

    if (hasStack)
    {
      writer.WriteString("stk"u8);
      writer.Write(Cut(stack!, budget.MaxStackChars));
    }

    if (hasInner)
    {
      writer.WriteString("inner"u8);
      WriteExceptionAtDepth(ref writer, ex.InnerException!, depth + 1, in budget);
    }
  }

  /// <summary>
  /// The first <paramref name="maxChars"/> of a string that overran its budget, marked as cut; the
  /// string itself otherwise. A cut never splits a surrogate pair: its halves would be written as U+FFFD.
  /// </summary>
  private static string Cut(string s, int maxChars)
  {
    if (s.Length <= maxChars) return s;
    if (maxChars > 0 && char.IsHighSurrogate(s[maxChars - 1])) maxChars--;
    return string.Concat(s.AsSpan(0, maxChars), " … [cut by Serilog.Sinks.Ameto]");
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
