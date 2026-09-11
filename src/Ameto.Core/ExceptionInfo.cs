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

    /// <summary>The four keys of the wire map — matched as bytes, never built as strings.</summary>
    private enum ExcField : byte { Unknown = 0, Type, Message, Stack, Inner }

    private static ExcField ClassifyKey(ReadOnlySpan<byte> key) =>
        key.SequenceEqual("type"u8)  ? ExcField.Type    :
        key.SequenceEqual("msg"u8)   ? ExcField.Message :
        key.SequenceEqual("stk"u8)   ? ExcField.Stack   :
        key.SequenceEqual("inner"u8) ? ExcField.Inner   :
        ExcField.Unknown;

    /// <summary>Fallback for the rare non-contiguous key.</summary>
    private static ExcField ClassifyKey(string? key) => key switch
    {
        Fields.Type    => ExcField.Type,
        Fields.Message => ExcField.Message,
        Fields.Stack   => ExcField.Stack,
        Fields.Inner   => ExcField.Inner,
        _              => ExcField.Unknown,
    };

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
            // The key is CLASSIFIED from its bytes, not read as a string. There are four of
            // them, they are three or five bytes long, and the map is read once per
            // exception per depth — so the old `ReadString()` per key built four throwaway
            // UTF-16 strings, transcoded, only to switch on them and drop them. Same
            // technique and same reason as LogEventSerializer.ClassifyKey.
            ExcField field = reader.TryReadStringSpan(out ReadOnlySpan<byte> keySpan)
                ? ClassifyKey(keySpan)
                : ClassifyKey(reader.ReadString());   // rare: the key spans buffer segments

            switch (field)
            {
                case ExcField.Type:    type  = reader.ReadString() ?? "Exception"; break;
                case ExcField.Message: msg   = reader.ReadString();                break;
                case ExcField.Stack:   stack = reader.ReadString();                break;
                case ExcField.Inner:
                    if (depth < MaxDepth)
                        inner = ReadAtDepth(ref reader, depth + 1);
                    else
                        reader.Skip();                                             // truncate deeper levels
                    break;
                default:               reader.Skip();                              break;
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
    /// Reads an <see cref="ExceptionInfo"/> from a previously-written msgpack buffer, WITHOUT
    /// copying it. Prefer this overload wherever the bytes are already on the managed heap —
    /// a segment's decoded exception slice, a WAL record — because the copy the span overload
    /// has to make is the payload itself: 1-5 KB of stack trace, per exception-bearing row.
    /// </summary>
    public static ExceptionInfo? FromBytes(ReadOnlyMemory<byte> bytes)
    {
        if (bytes.IsEmpty) return null;
        var reader = new MessagePackReader(new ReadOnlySequence<byte>(bytes));
        return Read(ref reader);
    }

    /// <summary>
    /// Reads an <see cref="ExceptionInfo"/> from a previously-written msgpack byte buffer.
    ///
    /// <para>A <see cref="MessagePackReader"/> needs a <see cref="ReadOnlySequence{T}"/>, which
    /// cannot be built over a span that may live on the stack — so this overload COPIES.
    /// Callers holding heap memory should use the <see cref="ReadOnlyMemory{T}"/> overload.</para>
    /// </summary>
    public static ExceptionInfo? FromBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty) return null;
        var reader = new MessagePackReader(new ReadOnlySequence<byte>(bytes.ToArray()));
        return Read(ref reader);
    }
}
