using System.Buffers;
using MessagePack;

namespace Ameto.Core;

/// <summary>
/// Structured representation of an exception attached to a <see cref="LogEvent"/>.
///
/// Replaces the legacy CLEF <c>@x</c> single-string field with a typed tree that
/// the query layer can index on (<c>Exception.Type</c>, <c>Exception.Inner.Type</c>, …)
/// and that the UI can render with collapsible inner frames.
///
/// The wire format (msgpack map) accepts both shapes for backward compatibility:
///   * a plain string  — wrapped as <c>new ExceptionInfo { Type = "Exception", Message = str }</c>;
///   * a msgpack map   — keys: <c>type</c>, <c>msg</c>, <c>stk</c>, <c>inner</c> (recursive).
///
/// Depth is capped at <see cref="MaxDepth"/> during deserialisation to prevent
/// untrusted clients from sending pathological trees.
/// </summary>
public sealed class ExceptionInfo
{
    /// <summary>Hard cap on the depth of the inner-exception chain (root + 2 inner = 3 levels).</summary>
    public const int MaxDepth = 3;

    /// <summary>Fully-qualified type name (e.g. <c>System.InvalidOperationException</c>).</summary>
    public required string         Type       { get; init; }

    /// <summary>Exception message text (may be null when not provided).</summary>
    public          string?        Message    { get; init; }

    /// <summary>Stack trace text (may be null). Not indexed by default — only stored.</summary>
    public          string?        StackTrace { get; init; }

    /// <summary>Nested inner exception, or <c>null</c>. Bounded by <see cref="MaxDepth"/>.</summary>
    public          ExceptionInfo? Inner      { get; init; }

    // ── Field-name constants used in the msgpack wire representation ─────────
    public static class Fields
    {
        public const string Type    = "type";
        public const string Message = "msg";
        public const string Stack   = "stk";
        public const string Inner   = "inner";
    }

    // ── Msgpack read/write ───────────────────────────────────────────────────

    /// <summary>
    /// Reads an <see cref="ExceptionInfo"/> at the current reader position.
    /// Accepts either a string (legacy CLEF) or a msgpack map.
    /// Returns <c>null</c> for nil. Depth is enforced — anything deeper than
    /// <see cref="MaxDepth"/> is silently truncated (the deepest <see cref="Inner"/>
    /// is set to <c>null</c>).
    /// </summary>
    public static ExceptionInfo? Read(ref MessagePackReader reader)
        => ReadAtDepth(ref reader, depth: 1);

    private static ExceptionInfo? ReadAtDepth(ref MessagePackReader reader, int depth)
    {
        if (reader.TryReadNil()) return null;

        // Legacy: @x came in as a plain string.
        if (reader.NextMessagePackType == MessagePackType.String)
        {
            string? str = reader.ReadString();
            if (string.IsNullOrEmpty(str)) return null;
            return new ExceptionInfo { Type = "Exception", Message = str };
        }

        if (reader.NextMessagePackType != MessagePackType.Map)
        {
            // Unknown shape — skip and ignore.
            reader.Skip();
            return null;
        }

        int    fields  = reader.ReadMapHeader();
        string type    = "Exception";
        string? msg    = null;
        string? stack  = null;
        ExceptionInfo? inner = null;

        for (int i = 0; i < fields; i++)
        {
            string key = reader.ReadString() ?? string.Empty;
            switch (key)
            {
                case Fields.Type:    type  = reader.ReadString() ?? "Exception"; break;
                case Fields.Message: msg   = reader.ReadString();                break;
                case Fields.Stack:   stack = reader.ReadString();                break;
                case Fields.Inner:
                    if (depth < MaxDepth)
                        inner = ReadAtDepth(ref reader, depth + 1);
                    else
                        reader.Skip();                                            // truncate deeper levels
                    break;
                default:             reader.Skip();                              break;
            }
        }

        return new ExceptionInfo
        {
            Type       = type,
            Message    = msg,
            StackTrace = stack,
            Inner      = inner,
        };
    }

    /// <summary>Writes this <see cref="ExceptionInfo"/> as a msgpack map.</summary>
    public void Write(ref MessagePackWriter writer) => WriteAtDepth(ref writer, depth: 1);

    private void WriteAtDepth(ref MessagePackWriter writer, int depth)
    {
        bool writeStack = StackTrace is not null;
        bool writeMsg   = Message    is not null;
        bool writeInner = Inner      is not null && depth < MaxDepth;

        int fields = 1
                   + (writeMsg   ? 1 : 0)
                   + (writeStack ? 1 : 0)
                   + (writeInner ? 1 : 0);

        writer.WriteMapHeader(fields);

        writer.Write(Fields.Type);
        writer.Write(Type);

        if (writeMsg)
        {
            writer.Write(Fields.Message);
            writer.Write(Message);
        }
        if (writeStack)
        {
            writer.Write(Fields.Stack);
            writer.Write(StackTrace);
        }
        if (writeInner)
        {
            writer.Write(Fields.Inner);
            Inner!.WriteAtDepth(ref writer, depth + 1);
        }
    }

    /// <summary>Serialises this exception to a fresh byte array (msgpack map).</summary>
    public byte[] ToBytes()
    {
        var buf = new ArrayBufferWriter<byte>(64);
        var w   = new MessagePackWriter(buf);
        Write(ref w);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>
    /// Reads just what the segment index files — <c>type</c>, <c>msg</c> and the inner
    /// exception's <c>type</c> — as UTF-8 spans into <paramref name="payload"/>, skipping the
    /// stack trace and everything else without decoding it.
    ///
    /// <para>For the merge path, where the exception travels as bytes and the index is the only
    /// consumer. <see cref="FromBytes"/> there meant copying the payload, decoding four key
    /// strings and the 1-5 KB stack trace to UTF-16 per row, on a path that level-split flush
    /// makes 100 % exceptions for every Error segment — gigabytes of gen0 garbage per large
    /// merge for three short strings. The same shapes <see cref="Read"/> accepts are accepted
    /// here: nil (false), a legacy plain string (type <c>Exception</c>, the string as message),
    /// or a map; a map deeper than <see cref="MaxDepth"/> is truncated the same way. Malformed
    /// input returns false rather than throwing. The spans alias <paramref name="payload"/>, which
    /// is a <see cref="ReadOnlyMemory{T}"/> only because <see cref="MessagePackReader"/> has no
    /// span constructor — a caller holding a span pins it and wraps it, without a copy.</para>
    /// </summary>
    public static bool TryReadIndexFields(ReadOnlyMemory<byte> payload,
        out ReadOnlySpan<byte> type, out ReadOnlySpan<byte> message, out ReadOnlySpan<byte> innerType)
    {
        type = "Exception"u8; message = default; innerType = default;
        if (payload.IsEmpty) return false;
        try
        {
            var reader = new MessagePackReader(payload);
            if (reader.TryReadNil()) return false;

            if (reader.NextMessagePackType == MessagePackType.String)
            {
                if (!reader.TryReadStringSpan(out var legacy) || legacy.IsEmpty) return false;
                message = legacy;
                return true;
            }
            if (reader.NextMessagePackType != MessagePackType.Map) return false;

            int fields = reader.ReadMapHeader();
            for (int i = 0; i < fields; i++)
            {
                if (!reader.TryReadStringSpan(out var key)) { reader.Skip(); reader.Skip(); continue; }
                if (key.SequenceEqual("type"u8))
                {
                    if (reader.TryReadNil()) type = "Exception"u8;
                    else if (reader.NextMessagePackType == MessagePackType.String && reader.TryReadStringSpan(out var t)) type = t;
                    else reader.Skip();
                }
                else if (key.SequenceEqual("msg"u8))
                {
                    if (reader.TryReadNil()) message = default;
                    else if (reader.NextMessagePackType == MessagePackType.String && reader.TryReadStringSpan(out var m)) message = m;
                    else reader.Skip();
                }
                else if (key.SequenceEqual("inner"u8))
                {
                    // Depth 2 of MaxDepth = 3: the inner's type is indexed, nothing below it is.
                    if (reader.TryReadNil()) continue;
                    if (reader.NextMessagePackType == MessagePackType.String)
                    {
                        // A legacy inner string reads as type "Exception" — unless it is empty,
                        // which ReadAtDepth turns into no inner at all.
                        if (reader.TryReadStringSpan(out var legacyInner)) { if (!legacyInner.IsEmpty) innerType = "Exception"u8; }
                        else reader.Skip();
                        continue;
                    }
                    if (reader.NextMessagePackType != MessagePackType.Map) { reader.Skip(); continue; }
                    innerType = "Exception"u8;
                    int innerFields = reader.ReadMapHeader();
                    for (int j = 0; j < innerFields; j++)
                    {
                        if (!reader.TryReadStringSpan(out var ik)) { reader.Skip(); reader.Skip(); continue; }
                        if (ik.SequenceEqual("type"u8))
                        {
                            if (reader.TryReadNil()) innerType = "Exception"u8;
                            else if (reader.NextMessagePackType == MessagePackType.String && reader.TryReadStringSpan(out var it)) innerType = it;
                            else reader.Skip();
                        }
                        else reader.Skip();
                    }
                }
                else reader.Skip();   // stk, unknown keys
            }
            return true;
        }
        catch (MessagePackSerializationException)
        {
            return false;
        }
        catch (EndOfStreamException)
        {
            return false;
        }
    }

    /// <summary>Reads an <see cref="ExceptionInfo"/> from a previously-written msgpack byte buffer.</summary>
    public static ExceptionInfo? FromBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty) return null;
        var reader = new MessagePackReader(new ReadOnlySequence<byte>(bytes.ToArray()));
        return Read(ref reader);
    }
}
