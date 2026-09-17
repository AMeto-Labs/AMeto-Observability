using System.Buffers;
using System.Buffers.Binary;
using System.Text;
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
                // A non-string, non-nil type or message is what Read throws on (ReadString);
                // it is reported as malformed here rather than indexed under a made-up type.
                if (key.SequenceEqual("type"u8))
                {
                    if (reader.TryReadNil()) type = "Exception"u8;
                    else if (reader.NextMessagePackType == MessagePackType.String && reader.TryReadStringSpan(out var t)) type = t;
                    else return false;
                }
                else if (key.SequenceEqual("msg"u8))
                {
                    if (reader.TryReadNil()) message = default;
                    else if (reader.NextMessagePackType == MessagePackType.String && reader.TryReadStringSpan(out var m)) message = m;
                    else return false;
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
                            else return false;
                        }
                        else reader.Skip();
                    }
                }
                else reader.Skip();   // stk, unknown keys
            }
            return true;
        }
        // Every content-shaped failure, not the two this used to name. A count or length of 2^31
        // or more — a map32 root, an inner map32, an array32 under a skipped key, a str32 — hits
        // MessagePack's checked uint→int conversion and throws OverflowException, which escaped
        // the index builder and failed a merge the engine then retried every pass. The list is
        // FileBounds's, so this and the engine's corruption verdict cannot drift apart.
        catch (Exception ex) when (FileBounds.DescribesContent(ex))
        {
            return false;
        }
    }

    // ── Decode-free questions about a stored payload ─────────────────────────
    //
    // Both of these answer, from the bytes alone, a question a filter asks per SCANNED row —
    // where building the object graph to answer it is the whole cost the lazy Exception was
    // added to avoid. Each one mirrors a specific branch of ReadAtDepth above; if that method
    // changes shape, these change with it, and ExceptionInfoReadTests pins them together.

    /// <summary>
    /// Whether these bytes decode to a NON-NULL <see cref="ExceptionInfo"/> — decided from the
    /// msgpack type header alone, in constant time and with no allocation.
    ///
    /// <para>Exactly <c>FromBytes(bytes) is not null</c> FOR ANY PAYLOAD FROMBYTES ACCEPTS,
    /// and it has to be exact because <c>LogEvent.HasException</c> is what answers
    /// <c>has(@x)</c>. <see cref="ReadAtDepth"/> returns null for three shapes, and all three
    /// are visible in the first bytes: nil, an EMPTY legacy string, and anything that is
    /// neither a string nor a map. Everything else — any map, any non-empty string — produces
    /// an object.</para>
    ///
    /// <para>The qualifier is the TRUNCATED payload, which <see cref="FromBytes"/> never
    /// accepts, and this answers it from the header alone — so it can land on either side:</para>
    /// <list type="bullet">
    ///   <item>ANY map header answers TRUE, whatever follows it: <c>81</c> (a one-entry fixmap
    ///         carrying nothing), and <c>DE 00</c> or <c>DF 00 00 00</c> — map16 and map32
    ///         headers cut short of their own entry count, which the map branch below tests by
    ///         its lead byte alone and never length-checks;</item>
    ///   <item>a string header announcing a non-zero length answers TRUE even when the body it
    ///         announces is cut short: <c>D9 05 61</c> is a str8 of five bytes carrying one;</item>
    ///   <item>a STRING header too short to hold its own length answers FALSE: <c>D9</c>,
    ///         <c>DA 00</c> and <c>DB 00 00 00</c> (a str32 in four bytes) are absent.</item>
    /// </list>
    /// <para><see cref="FromBytes"/> throws <see cref="System.IO.EndOfStreamException"/> for every
    /// one of them. A corrupt row therefore falls OUT of <c>has(@x)</c> when a STRING header is
    /// cut and IN otherwise: what decides is the KIND of header, not how far the cut went — so a
    /// cut map header is present and a cut string header is not.
    /// That is deliberate: this is asked per scanned row of a segment
    /// whose block frame has already been length-checked, and a presence probe is neither the
    /// place to raise corruption nor worth a body walk to detect it. Anything that then READS
    /// the payload still throws, and the caller that hits it sees the same exception it always
    /// did. <c>ExceptionInfoReadTests</c> pins each of these shapes.</para>
    /// </summary>
    public static bool IsPresent(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty) return false;
        byte b = bytes[0];

        // A map is always an exception: fixmap 0x80-0x8F, map16 0xDE, map32 0xDF.
        if ((b & 0xF0) == 0x80 || b == 0xDE || b == 0xDF) return true;

        // Legacy @x as a plain string — present unless it is empty.
        if ((b & 0xE0) == 0xA0) return (b & 0x1F) != 0;                       // fixstr
        if (b == 0xD9) return bytes.Length >= 2 && bytes[1] != 0;             // str8
        if (b == 0xDA) return bytes.Length >= 3 && BinaryPrimitives.ReadUInt16BigEndian(bytes[1..]) != 0;
        if (b == 0xDB) return bytes.Length >= 5 && BinaryPrimitives.ReadUInt32BigEndian(bytes[1..]) != 0;

        // nil, or a shape ReadAtDepth skips and reports as null.
        return false;
    }

    /// <summary>
    /// Whether the ROOT exception's <see cref="Type"/> or <see cref="Message"/> contains
    /// <paramref name="term"/>, case-insensitively — without building the object.
    ///
    /// <para>This is the free-text search path. A term is tested against EVERY row a scan
    /// touches, and on a level-split Error segment every row carries an exception whose stack
    /// trace is 1-5 KB — so decoding the tree to read two of its strings put the whole payload,
    /// the inner chain and the stack trace on the heap per row, per term.</para>
    ///
    /// <para>Root only, and Type defaulting to <c>"Exception"</c> when the key is absent or
    /// nil, because that is what the evaluator matched off the object and the two must not
    /// disagree. The value is transcoded into stack or pooled scratch, so the comparison is the
    /// same <c>OrdinalIgnoreCase</c> substring test over the same chars a full decode would
    /// have produced.</para>
    /// </summary>
    public static bool RootTextContains(ReadOnlyMemory<byte> bytes, string term)
    {
        if (bytes.IsEmpty) return false;
        var reader = new MessagePackReader(new ReadOnlySequence<byte>(bytes));

        if (reader.TryReadNil()) return false;

        // Legacy plain string: Type is the literal "Exception", Message is the string. An
        // empty one decodes to null and matches nothing at all.
        if (reader.NextMessagePackType == MessagePackType.String)
        {
            if (reader.TryReadStringSpan(out var raw))
                return !raw.IsEmpty && (Utf8Contains(raw, term) || Contains(DefaultType, term));

            string? str = reader.ReadString();
            return !string.IsNullOrEmpty(str) && (Contains(str, term) || Contains(DefaultType, term));
        }

        if (reader.NextMessagePackType != MessagePackType.Map) return false;

        int  fields      = reader.ReadMapHeader();
        bool typeIsDefault = true;

        for (int i = 0; i < fields; i++)
        {
            ExcField field = reader.TryReadStringSpan(out ReadOnlySpan<byte> keySpan)
                ? ClassifyKey(keySpan)
                : ClassifyKey(reader.ReadString());

            switch (field)
            {
                case ExcField.Type:
                    if (reader.TryReadNil()) break;                 // nil ⇒ Type stays "Exception"
                    typeIsDefault = false;
                    if (ReadStringContains(ref reader, term)) return true;
                    break;

                case ExcField.Message:
                    if (reader.TryReadNil()) break;
                    if (ReadStringContains(ref reader, term)) return true;
                    break;

                // Stack and Inner are NOT searched, exactly as the object-side check did not
                // search them. Skipping is also the point: the stack trace is the payload.
                default:
                    reader.Skip();
                    break;
            }
        }

        return typeIsDefault && Contains(DefaultType, term);
    }

    /// <summary>The Type a payload takes when it does not carry one — see <see cref="ReadAtDepth"/>.</summary>
    private const string DefaultType = "Exception";

    private static bool Contains(string? haystack, string term) =>
        haystack is not null && haystack.Contains(term, StringComparison.OrdinalIgnoreCase);

    private static bool ReadStringContains(ref MessagePackReader reader, string term)
    {
        if (reader.TryReadStringSpan(out var utf8)) return Utf8Contains(utf8, term);
        return Contains(reader.ReadString(), term);    // rare: the value spans buffer segments
    }

    /// <summary>Ceiling on the scratch a type name or an exception message is decoded into;
    /// longer values borrow from the pool instead. Either way nothing reaches the heap.</summary>
    private const int TermScratch = 512;

    private static bool Utf8Contains(ReadOnlySpan<byte> utf8, string term)
    {
        if (utf8.IsEmpty) return term.Length == 0;

        int needed = Encoding.UTF8.GetMaxCharCount(utf8.Length);
        char[]? rented = null;
        // Sized from the VALUE, not from the ceiling. `stackalloc` zeroes everything it reserves
        // (nothing in this repo sets SkipLocalsInit), and this runs per scanned row per term —
        // twice, for the type and then the message — over values that are usually a short type
        // name. Reserving TermScratch unconditionally also paid that kilobyte on the over-long
        // road, which then rents from the pool anyway and never touches the reservation.
        Span<char> scratch = needed <= TermScratch
            ? stackalloc char[needed]
            : (rented = ArrayPool<char>.Shared.Rent(needed));
        try
        {
            int written = Encoding.UTF8.GetChars(utf8, scratch);
            return scratch[..written].Contains(term, StringComparison.OrdinalIgnoreCase);
        }
        finally { if (rented is not null) ArrayPool<char>.Shared.Return(rented); }
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
