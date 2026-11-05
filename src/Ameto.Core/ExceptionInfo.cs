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
///
/// <para>THE TREE IS A VIEW, NOT THE STORED THING. An event's <c>@x</c> is kept as the bytes it
/// arrived in — in the ingest batch, the hot tier, the WAL entry and the segment column (see
/// <see cref="LogEvent.RawException"/>) — validated at ingest with <see cref="Validate"/> to be a
/// value <see cref="Read"/> reads, and read field by field where a consumer wants one: the index
/// (<see cref="TryReadIndexFields"/>), the free-text filter (<see cref="RootTextContains"/>), the
/// field predicates (<see cref="ReadField"/>) and the JSON writer. <see cref="Read"/> stays the
/// definition every one of them mirrors, and <see cref="FromBytes(ReadOnlyMemory{byte})"/> builds
/// the tree for the caller that wants the whole of it.</para>
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
    internal enum ExcField : byte { Unknown = 0, Type, Message, Stack, Inner }

    internal static ExcField ClassifyKey(ReadOnlySpan<byte> key) =>
        key.SequenceEqual("type"u8)  ? ExcField.Type    :
        key.SequenceEqual("msg"u8)   ? ExcField.Message :
        key.SequenceEqual("stk"u8)   ? ExcField.Stack   :
        key.SequenceEqual("inner"u8) ? ExcField.Inner   :
        ExcField.Unknown;

    /// <summary>Fallback for the rare non-contiguous key.</summary>
    internal static ExcField ClassifyKey(string? key) => key switch
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

    // ── Keeping the bytes instead of the tree ────────────────────────────────
    //
    // An event's @x is kept as it arrived — in the staged batch, the hot tier, the WAL entry and
    // the segment column — and decoded only where a consumer wants a field of it. What makes that
    // safe is that every stored payload is one Read is KNOWN to read: ingest validates it with the
    // walk below, which accepts and refuses exactly what Read did, and every byte-level reader
    // after it (TryReadIndexFields, RootTextContains, ReadField, the JSON writer) mirrors a
    // branch of ReadAtDepth. Change ReadAtDepth and change them with it; ExceptionInfoReadTests
    // and ExceptionJsonWriterTests pin them together.

    /// <summary>
    /// Reads a raw <c>@x</c> value the way <see cref="Read"/> would, building nothing: true where
    /// Read returns an object, false where it returns null (nil, an empty legacy string, a value
    /// that is neither a string nor a map), and a throw exactly where Read throws — a key that is
    /// neither a string nor nil, or a <c>type</c>, <c>msg</c> or <c>stk</c> that is neither a
    /// string nor nil, at any of the <see cref="MaxDepth"/> levels Read decodes. An <c>inner</c>
    /// below them is skipped, as Read skips it, so it is not validated either; unknown keys are
    /// skipped (a torn value still throws out of the skip).
    ///
    /// <para>Allocates nothing: the one road that may is a non-string value where Read called
    /// <c>ReadString</c>, and there the same call is made so that the exception is the same one.</para>
    /// </summary>
    public static bool Validate(ReadOnlyMemory<byte> bytes)
    {
        if (bytes.IsEmpty) return false;
        var reader = new MessagePackReader(bytes);
        return ValidateAtDepth(ref reader, depth: 1);
    }

    private static bool ValidateAtDepth(ref MessagePackReader reader, int depth)
    {
        if (reader.TryReadNil()) return false;

        if (reader.NextMessagePackType == MessagePackType.String)
        {
            // Legacy @x: an empty string is no exception at all (string.IsNullOrEmpty in Read).
            if (reader.TryReadStringSpan(out var legacy)) return !legacy.IsEmpty;
            var seq = reader.ReadStringSequence();
            return seq is { IsEmpty: false };
        }

        if (reader.NextMessagePackType != MessagePackType.Map)
        {
            reader.Skip();
            return false;
        }

        int fields = reader.ReadMapHeader();
        for (int i = 0; i < fields; i++)
        {
            // A nil key reads as null and classifies as unknown; any other non-string key throws
            // out of ReadString — both exactly as in ReadAtDepth.
            ExcField field = reader.TryReadStringSpan(out ReadOnlySpan<byte> keySpan)
                ? ClassifyKey(keySpan)
                : ClassifyKey(reader.ReadString());

            switch (field)
            {
                case ExcField.Type:
                case ExcField.Message:
                case ExcField.Stack:
                    SkipStringOrNil(ref reader);
                    break;
                case ExcField.Inner:
                    if (depth < MaxDepth) ValidateAtDepth(ref reader, depth + 1);
                    else reader.Skip();
                    break;
                default:
                    reader.Skip();
                    break;
            }
        }
        return true;
    }

    /// <summary>
    /// Steps over a string or a nil without building the string. Anything else is handed to
    /// <c>ReadString</c>, which is what Read calls there: it throws for the value's code, and
    /// a value it would accept is accepted here too, at the cost Read paid.
    /// </summary>
    private static void SkipStringOrNil(ref MessagePackReader reader)
    {
        if (reader.TryReadNil()) return;
        if (reader.NextMessagePackType == MessagePackType.String) { reader.Skip(); return; }
        reader.ReadString();
    }

    /// <summary>A field of the exception a byte-level read can answer: see <see cref="ReadField"/>.</summary>
    public enum Field : byte
    {
        /// <summary><see cref="Type"/> of the root.</summary>
        Type,
        /// <summary><see cref="Message"/> of the root.</summary>
        Message,
        /// <summary><see cref="StackTrace"/> of the root.</summary>
        StackTrace,
        /// <summary><see cref="Type"/> of the root's <see cref="Inner"/>.</summary>
        InnerType,
        /// <summary><see cref="Message"/> of the root's <see cref="Inner"/>.</summary>
        InnerMessage,
    }

    /// <summary>
    /// ONE field of a stored payload, as the decoded tree would answer it — <c>FromBytes(bytes)?.Type</c>,
    /// <c>?.Message</c>, <c>?.StackTrace</c>, <c>?.Inner?.Type</c>, <c>?.Inner?.Message</c> — allocating
    /// that one string and nothing else: no node, no inner chain and, unless it is the field asked for,
    /// no stack trace.
    ///
    /// <para>This is what a predicate on <c>@x.type</c>, <c>@x.message</c> or <c>@x.inner.type</c> reads per
    /// candidate row. Going through <see cref="LogEvent.Exception"/> built the whole tree for it — the
    /// stack trace's 1-5 KB of UTF-16 included — on rows the predicate then rejected. Same answers as
    /// the tree, by the same rules: a repeated key keeps its last value, a <c>type</c> that is absent or
    /// nil is <c>"Exception"</c>, a legacy string is type <c>"Exception"</c> with the string for a message,
    /// and a payload that decodes to no exception at all answers null for every field. Malformed bytes
    /// throw what the tree's decode throws.</para>
    /// </summary>
    public static string? ReadField(ReadOnlyMemory<byte> bytes, Field field)
    {
        if (bytes.IsEmpty) return null;
        var reader = new MessagePackReader(bytes);

        if (reader.TryReadNil()) return null;

        if (reader.NextMessagePackType == MessagePackType.String)
            return LegacyField(ref reader, field == Field.Type, field == Field.Message);

        if (reader.NextMessagePackType != MessagePackType.Map) return null;

        ExcField wanted = field switch
        {
            Field.Type       => ExcField.Type,
            Field.Message    => ExcField.Message,
            Field.StackTrace => ExcField.Stack,
            _                => ExcField.Inner,
        };

        // The LAST occurrence of the wanted key wins, as it does in ReadAtDepth, so the whole level
        // is walked and the value is read from a bookmark afterwards.
        if (!FindLast(ref reader, wanted, out MessagePackReader at))
            return wanted == ExcField.Type ? DefaultType : null;

        if (wanted != ExcField.Inner)
            return ReadStringValue(ref at) ?? (wanted == ExcField.Type ? DefaultType : null);

        // The inner (depth 2 of MaxDepth = 3): nil, an empty string and a non-map non-string are no
        // inner at all; a string is type "Exception" with the string for a message.
        if (at.TryReadNil()) return null;
        if (at.NextMessagePackType == MessagePackType.String)
            return LegacyField(ref at, field == Field.InnerType, field == Field.InnerMessage);
        if (at.NextMessagePackType != MessagePackType.Map) return null;

        ExcField innerWanted = field == Field.InnerType ? ExcField.Type : ExcField.Message;
        if (!FindLast(ref at, innerWanted, out MessagePackReader innerAt))
            return innerWanted == ExcField.Type ? DefaultType : null;
        return ReadStringValue(ref innerAt) ?? (innerWanted == ExcField.Type ? DefaultType : null);
    }

    /// <summary>
    /// Walks one map level and leaves <paramref name="at"/> positioned on the LAST value under
    /// <paramref name="wanted"/>; false, and <paramref name="at"/> unset, when the key is absent.
    /// Every value is skipped, read or not, so the reader ends past the map.
    /// </summary>
    private static bool FindLast(ref MessagePackReader reader, ExcField wanted, out MessagePackReader at)
    {
        at = default;
        bool found  = false;
        int  fields = reader.ReadMapHeader();
        for (int i = 0; i < fields; i++)
        {
            ExcField field = reader.TryReadStringSpan(out ReadOnlySpan<byte> keySpan)
                ? ClassifyKey(keySpan)
                : ClassifyKey(reader.ReadString());
            if (field == wanted) { at = reader; found = true; }
            reader.Skip();
        }
        return found;
    }

    /// <summary>A string value, or null for nil; anything else throws as <c>ReadString</c> does for the tree.</summary>
    private static string? ReadStringValue(ref MessagePackReader at) => at.ReadString();

    /// <summary>
    /// A field of a legacy plain-string exception, positioned on the string: type <c>"Exception"</c>,
    /// the string itself for the message, null for the rest — and null for everything when the
    /// string is empty, which is no exception at all. The string is built only when it is the answer.
    /// </summary>
    private static string? LegacyField(ref MessagePackReader at, bool wantType, bool wantMessage)
    {
        if (wantMessage)
        {
            string? legacy = at.ReadString();
            return string.IsNullOrEmpty(legacy) ? null : legacy;
        }
        bool empty = at.TryReadStringSpan(out var span) ? span.IsEmpty : at.ReadStringSequence() is not { IsEmpty: false };
        return !empty && wantType ? DefaultType : null;
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

        // The LAST type and the LAST msg are the ones the tree holds (a repeated key keeps its
        // last value in ReadAtDepth), so the level is walked whole and the two are read from
        // bookmarks afterwards. Stack and Inner are NOT searched, exactly as the object-side check
        // did not search them. Skipping is also the point: the stack trace is the payload.
        int fields = reader.ReadMapHeader();
        MessagePackReader typeAt = default, msgAt = default;
        bool hasType = false, hasMsg = false;
        for (int i = 0; i < fields; i++)
        {
            ExcField field = reader.TryReadStringSpan(out ReadOnlySpan<byte> keySpan)
                ? ClassifyKey(keySpan)
                : ClassifyKey(reader.ReadString());
            switch (field)
            {
                case ExcField.Type:    typeAt = reader; hasType = true; break;
                case ExcField.Message: msgAt  = reader; hasMsg  = true; break;
            }
            reader.Skip();
        }

        bool typeIsDefault = true;
        if (hasType && !typeAt.TryReadNil())                     // nil ⇒ Type stays "Exception"
        {
            typeIsDefault = false;
            if (ReadStringContains(ref typeAt, term)) return true;
        }
        if (hasMsg && !msgAt.TryReadNil() && ReadStringContains(ref msgAt, term)) return true;

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
