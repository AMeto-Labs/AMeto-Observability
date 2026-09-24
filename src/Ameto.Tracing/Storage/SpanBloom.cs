using System.Buffers;
using System.Globalization;
using System.Text;
using MessagePack;

namespace Ameto.Tracing.Storage;

/// <summary>
/// Per-block bloom filter over span attribute keys and values, used by the TraceQL executor to
/// skip blocks that cannot match an attribute predicate.
///
/// <para>Entries inserted per span attribute:</para>
/// <list type="bullet">
///   <item><c>key</c> — the key's UTF-8 bytes, as stored: key presence, a valid necessary condition
///     for EVERY attribute operator, since a span without the key never matches.</item>
///   <item><c>key 0x1F fold(text(value))</c> — equality probes, for every value that has a text
///     (string, integer, double, boolean; not nil, arrays, maps or binary).</item>
/// </list>
///
/// <para>THE BLOOM IS A PROMISE ABOUT THE EVALUATOR, and both halves of that entry are defined by
/// it rather than by anything convenient to hash. <c>AttributePredicate</c> answers
/// <c>{ .k = "q" }</c> with <c>text(value)</c> equal to <c>q</c> under
/// <see cref="StringComparison.OrdinalIgnoreCase"/>, so the bloom must hold, for every value, a
/// hash that every such <c>q</c> reproduces — one missed case is a block skipped that held the
/// answer, and a TraceQL page that silently returns fewer rows.</para>
///
/// <para><b>text(value) is CULTURE-INDEPENDENT</b> (issue #86): a string is itself; an integer its
/// invariant decimal (<c>-3</c>, never sv-SE's <c>−3</c>); a double its invariant shortest
/// round-trip form (<c>0.375</c>, <c>1E+21</c>, <c>-0</c>, <c>NaN</c>, <c>-Infinity</c>, never
/// ru-KZ's <c>0,375</c> or <c>∞</c>); a boolean <c>True</c>/<c>False</c>. The evaluator formats
/// with the same rules, so a segment written under one culture and queried under another agrees
/// with itself. It used to hash <c>value.ToString()</c> in the process culture, and a segment
/// written on a ru-KZ box answered <c>{ .sampling.ratio = "0.375" }</c> with zero blocks on an
/// en-US one.</para>
///
/// <para><b>fold is upper-of-lower per BMP scalar, from a table COMPILED INTO THE BINARY</b>
/// (<see cref="SpanBloomFold"/>), every astral scalar collapsed to U+FFFD (see <see cref="Fold"/>),
/// and NOT the lowercase the pre-#86 bloom used. Lowercase is not the equivalence
/// <c>OrdinalIgnoreCase</c> uses: it keeps <c>ς</c> apart from <c>σ</c>, <c>µ</c> from <c>μ</c>,
/// and the archaic Cyrillic forms U+1C80–U+1C88 from <c>в д о с т ъ ѣ ꙋ</c> — 44 BMP pairs that
/// the evaluator calls equal and the old bloom called different. Upper-of-lower merges every one
/// of them (proved over every code point in <c>SpanBloomCanonicalTests</c>), and it can be
/// computed from the hint's already-lowercased literal. A TABLE, because asking the host made the
/// bits depend on which host wrote them: ICU and invariant globalization (the Docker image)
/// disagree on Unicode 16's pairs, and a segment the container wrote would have been probed wrong
/// by a Windows host.</para>
///
/// <para><b>From the blob's bytes, with nothing decoded to a string</b> (TS#12): a string value is
/// folded straight off its UTF-8, a number formatted into the stack. The flush used to box every
/// value and allocate every key and every number's text to feed the old hash.</para>
///
/// <para><b>ON DISK the canonical blooms follow <see cref="CanonicalMarker"/> and the fold table's
/// <see cref="SpanBloomFold.Fingerprint"/></b>, behind one EMPTY legacy slot per block — see
/// <c>SpanWriter</c>'s layout. An empty slot is "no bloom, never skip" to every older reader, so a
/// build that predates this hash reads a new segment in full rather than probing canonical bits
/// with the old hash, which would be a false-negative source on exactly the values #86 is about. A
/// segment folded by another table is probed by value only for all-ASCII literals
/// (<see cref="CanonicalValueProbeIsExact"/>). A segment written before this change has no marker
/// and is probed with the LEGACY functions below, permissively wherever the legacy text could have
/// differed (<see cref="LegacyValueProbeIsExact"/>).</para>
///
/// <para>k = 3 probes via double hashing over an FNV-1a 64 hash; the bit length is a power of two
/// chosen at ~12 bits/entry (min 512, cap 32768 bits = 4 KB).</para>
/// </summary>
internal static class SpanBloom
{
    private const int BitsPerEntry = 12;
    private const int MinBits      = 512;
    private const int MaxBits      = 32_768;
    private const int Probes       = 3;

    /// <summary>
    /// Written between the legacy slots and the canonical blooms of a <c>.trc</c> bloom index, and
    /// followed by the 8-byte <see cref="SpanBloomFold.Fingerprint"/>: "RDB3". Its presence is what
    /// tells a reader which hash the bits were built with; its absence means the pre-#86
    /// culture-formatted hash. ("RDB2" — the same section without a fingerprint — never left its
    /// branch; a reader meets it as an unknown marker and reads the segment in full.)
    /// </summary>
    internal const uint CanonicalMarker = 0x52_44_42_33; // "RDB3"

    private const ulong FnvOffset = 14695981039346656037UL;
    private const ulong FnvPrime  = 1099511628211UL;
    private const byte  Separator = 0x1F;

    // ── Writer: from the blob ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Feeds one span's msgpack attribute map to the block bloom straight from its bytes — no key
    /// string, no boxed value, no formatted number on the heap — and says whether the blob is safe
    /// to copy through verbatim.
    ///
    /// <para>ACCEPTS AND REJECTS EXACTLY WHAT <see cref="SpanAttributeBlob.TryWalk{TState}"/> DOES,
    /// because the answer decides the span's bytes on disk, not only its bloom: the same reader
    /// calls in the same order (a map header; a string or nil key; <c>ReadInt64</c>, which throws on
    /// a uint64 past <see cref="long.MaxValue"/>; <c>ReadDouble</c>; <c>ReadBoolean</c>;
    /// <c>ReadNil</c>; <c>Skip</c>), and "one map" means the whole blob. Pinned by
    /// <c>SpanBloomCanonicalTests</c> against <c>TryWalk</c> over the rejection shapes, and by
    /// <c>TraceFlushProbe</c>, whose span blocks are still the pre-change bytes.</para>
    ///
    /// <para>A partial walk leaves the hashes it already added. Extra bits only ever make a block
    /// MORE likely to be read, so a blob that dies half way costs a wasted block read and never a
    /// missing row.</para>
    /// </summary>
    public static bool TryAddBlob(HashSet<ulong> hashes, ReadOnlyMemory<byte> blob)
    {
        // Outside the loop: stackalloc in a loop is not freed per iteration (CA2014).
        Span<byte> scratch = stackalloc byte[32];
        try
        {
            var reader = new MessagePackReader(blob);
            int count  = reader.ReadMapHeader();
            for (int i = 0; i < count; i++)
            {
                ulong key = ReadKeyHash(ref reader);
                AddValue(hashes, key, ref reader, scratch);
            }
            return reader.End;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// FNV of the key's bytes as stored. A nil key is the empty key — what <c>ReadString() ?? ""</c>
    /// made of it. <c>TryReadStringSpan</c> declines only nil (and a string split across segments,
    /// which a reader over one <see cref="ReadOnlyMemory{T}"/> cannot meet); anything that is not a
    /// string throws, as <c>ReadString</c> did.
    /// </summary>
    private static ulong ReadKeyHash(ref MessagePackReader reader)
    {
        if (reader.TryReadStringSpan(out var key)) return Fnv(FnvOffset, key);
        if (reader.TryReadNil())                   return FnvOffset;

        var seq = reader.ReadStringSequence();
        ulong h = FnvOffset;
        if (seq is { } s)
            foreach (var segment in s) h = Fnv(h, segment.Span);
        return h;
    }

    private static void AddValue(HashSet<ulong> hashes, ulong key, ref MessagePackReader reader, scoped Span<byte> scratch)
    {
        switch (reader.NextMessagePackType)
        {
            case MessagePackType.String:
            {
                ulong h = ValuePrefix(key);
                if (reader.TryReadStringSpan(out var utf8))
                {
                    h = FoldUtf8(h, utf8);
                }
                else
                {
                    // Unreachable over one ReadOnlyMemory (see ReadKeyHash); a split string is
                    // copied together rather than folded across a rune the split may have cut.
                    var seq = reader.ReadStringSequence()!.Value;
                    byte[] rented = ArrayPool<byte>.Shared.Rent((int)seq.Length);
                    try
                    {
                        seq.CopyTo(rented);
                        h = FoldUtf8(h, rented.AsSpan(0, (int)seq.Length));
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(rented);
                    }
                }
                hashes.Add(key);
                hashes.Add(h);
                return;
            }
            case MessagePackType.Integer:
            {
                long v = reader.ReadInt64();
                hashes.Add(key);
                hashes.Add(HashInteger(ValuePrefix(key), v, scratch));
                return;
            }
            case MessagePackType.Float:
            {
                double v = reader.ReadDouble();
                hashes.Add(key);
                hashes.Add(HashFloat(ValuePrefix(key), v, scratch));
                return;
            }
            case MessagePackType.Boolean:
            {
                bool v = reader.ReadBoolean();
                hashes.Add(key);
                hashes.Add(HashBoolean(ValuePrefix(key), v));
                return;
            }
            case MessagePackType.Nil:
                reader.ReadNil();
                hashes.Add(key);
                return;
            default:
                // Array, map, binary, extension: the decoder boxes these to null, so the evaluator
                // sees an absent value and only the key is a necessary condition.
                reader.Skip();
                hashes.Add(key);
                return;
        }
    }

    // ── Writer: from a dictionary ─────────────────────────────────────────────────────────────

    /// <summary>
    /// The dictionary path — a record with no blob, or one whose blob failed
    /// <see cref="TryAddBlob"/>. Hashes the value AS <c>SpanWriter.WriteAttributes</c> WRITES IT, which
    /// is what a reader of the segment will evaluate: <c>int</c>/<c>short</c>/<c>byte</c> become an
    /// integer, <c>float</c> a double, and any other type the invariant string
    /// (<see cref="SpanAttributeBlob.InvariantText"/>) it wrote.
    /// </summary>
    public static void AddAttr(HashSet<ulong> hashes, string key, object? value)
    {
        ulong k = HashKey(key);
        hashes.Add(k);

        Span<byte> scratch = stackalloc byte[32];
        ulong p = ValuePrefix(k);
        switch (value)
        {
            case null:     return;
            case string s: hashes.Add(FoldUtf16(p, s));                       return;
            case bool b:   hashes.Add(HashBoolean(p, b));                     return;
            case long l:   hashes.Add(HashInteger(p, l, scratch));            return;
            case int n:    hashes.Add(HashInteger(p, n, scratch));            return;
            case short sh: hashes.Add(HashInteger(p, sh, scratch));           return;
            case byte by:  hashes.Add(HashInteger(p, by, scratch));           return;
            case double d: hashes.Add(HashFloat(p, d, scratch));              return;
            case float f:  hashes.Add(HashFloat(p, f, scratch));              return;
            default:       hashes.Add(FoldUtf16(p, SpanAttributeBlob.InvariantText(value))); return;
        }
    }

    /// <summary>Builds the bitset from collected entry hashes.</summary>
    public static byte[] Build(HashSet<ulong> hashes)
    {
        if (hashes.Count == 0) return [];
        int bits = (int)System.Numerics.BitOperations.RoundUpToPowerOf2(
            (uint)Math.Clamp(hashes.Count * BitsPerEntry, MinBits, MaxBits));
        var bitset = new byte[bits / 8];
        foreach (var h in hashes) Insert(bitset, h);
        return bitset;
    }

    // ── Query: canonical ──────────────────────────────────────────────────────────────────────

    /// <summary>Key-presence probe: FNV of the key's UTF-8, the bytes the writer stored it as.</summary>
    public static ulong HashKey(string key)
    {
        ulong h = FnvOffset;
        Span<byte> buf = stackalloc byte[4];
        int i = 0;
        while (i < key.Length)
        {
            char c = key[i];
            if (c < 0x80) { h = (h ^ c) * FnvPrime; i++; continue; }
            // Invalid UTF-16 (a lone surrogate) decodes to U+FFFD, consuming one char — the same
            // EF BF BD that Encoding.UTF8, and so the msgpack writer, puts on disk for it.
            Rune.DecodeFromUtf16(key.AsSpan(i), out var r, out int used);
            int n = r.EncodeToUtf8(buf);
            h = Fnv(h, buf[..n]);
            i += used;
        }
        return h;
    }

    /// <summary>
    /// Equality probe for <c>{ .key = "value" }</c>. <paramref name="lowerValue"/> is the hint's
    /// lowercased literal (<c>AttrHint.LowerValue</c>); the fold makes that and the original literal
    /// the same probe, since upper-of-lower of a lowercase letter is upper-of-lower of the letter.
    /// </summary>
    public static ulong HashKeyValue(string key, string lowerValue) =>
        FoldUtf16(ValuePrefix(HashKey(key)), lowerValue);

    /// <summary>May-contain test; an empty bitset never rejects (unknown blooms are permissive).</summary>
    public static bool MayContain(ReadOnlySpan<byte> bitset, ulong hash)
    {
        if (bitset.IsEmpty) return true;
        int bits = bitset.Length * 8; // power of two
        ulong h1 = hash, h2 = (hash >> 33) | (hash << 31) | 1;
        for (int i = 0; i < Probes; i++)
        {
            int bit = (int)((h1 + (ulong)i * h2) & (ulong)(bits - 1));
            if ((bitset[bit >> 3] & (1 << (bit & 7))) == 0) return false;
        }
        return true;
    }

    private static void Insert(byte[] bitset, ulong hash)
    {
        int bits = bitset.Length * 8;
        ulong h1 = hash, h2 = (hash >> 33) | (hash << 31) | 1;
        for (int i = 0; i < Probes; i++)
        {
            int bit = (int)((h1 + (ulong)i * h2) & (ulong)(bits - 1));
            bitset[bit >> 3] |= (byte)(1 << (bit & 7));
        }
    }

    // ── The canonical text and its fold ───────────────────────────────────────────────────────

    private static ulong ValuePrefix(ulong keyHash) => (keyHash ^ Separator) * FnvPrime;

    /// <summary>
    /// The fold, one scalar at a time: a BMP scalar through <see cref="SpanBloomFold"/>'s compiled
    /// table (upper-of-lower, U+017F kept — see there), never through the host's casing.
    ///
    /// <para>EVERY SCALAR OUTSIDE THE BMP FOLDS TO U+FFFD. <c>OrdinalIgnoreCase</c> folds astral
    /// letters from .NET's own Unicode data while ICU answers from the host's: measured on the dev
    /// box, the 22 Garay case pairs (U+10D50–U+10D65 against U+10D70–U+10D85) are equal under the
    /// comparer and unmapped by ICU. Collapsing the astral planes costs selectivity only (the bloom
    /// cannot tell 👍 from 👎 and reads a block too many), never a row, and needs no table.</para>
    /// </summary>
    internal static Rune Fold(Rune r) =>
        r.IsBmp ? new Rune(SpanBloomFold.Bmp((char)r.Value)) : Rune.ReplacementChar;

    /// <summary>ASCII fast path of <see cref="Fold"/>: a-z to A-Z, everything else as is.</summary>
    private static byte FoldAscii(byte b) => (uint)(b - 'a') <= 'z' - 'a' ? (byte)(b - 0x20) : b;

    /// <summary>
    /// Folds UTF-8 as it is hashed. An ill-formed sequence is one U+FFFD per maximal invalid
    /// subsequence — <see cref="Rune.DecodeFromUtf8"/>'s rule, which is also the one
    /// <c>Encoding.UTF8.GetChars</c> applies when the evaluator decodes the same bytes.
    /// </summary>
    private static ulong FoldUtf8(ulong h, ReadOnlySpan<byte> utf8)
    {
        Span<byte> buf = stackalloc byte[4];
        int i = 0;
        while (i < utf8.Length)
        {
            byte b = utf8[i];
            if (b < 0x80) { h = (h ^ FoldAscii(b)) * FnvPrime; i++; continue; }
            Rune.DecodeFromUtf8(utf8[i..], out var r, out int used);
            int n = Fold(r).EncodeToUtf8(buf);
            h = Fnv(h, buf[..n]);
            i += used;
        }
        return h;
    }

    /// <summary><see cref="FoldUtf8"/> over the UTF-8 the text encodes to, without encoding it.</summary>
    private static ulong FoldUtf16(ulong h, string s)
    {
        Span<byte> buf = stackalloc byte[4];
        int i = 0;
        while (i < s.Length)
        {
            char c = s[i];
            if (c < 0x80) { h = (h ^ FoldAscii((byte)c)) * FnvPrime; i++; continue; }
            Rune.DecodeFromUtf16(s.AsSpan(i), out var r, out int used);
            int n = Fold(r).EncodeToUtf8(buf);
            h = Fnv(h, buf[..n]);
            i += used;
        }
        return h;
    }

    /// <summary>Invariant decimal: digits and an ASCII minus, nothing to fold.</summary>
    private static ulong HashInteger(ulong h, long v, Span<byte> scratch)
    {
        v.TryFormat(scratch, out int n, default, CultureInfo.InvariantCulture);
        return Fnv(h, scratch[..n]);
    }

    /// <summary>
    /// Invariant shortest round-trip (the default format since .NET Core 3.0, the same text
    /// <c>ToString("R")</c> gives) — at most 24 bytes, e.g. <c>-2.2250738585072014E-308</c> — folded,
    /// because <c>NaN</c>, <c>Infinity</c> and the exponent's <c>E</c> are letters a query may spell
    /// in either case.
    /// </summary>
    private static ulong HashFloat(ulong h, double v, Span<byte> scratch)
    {
        v.TryFormat(scratch, out int n, default, CultureInfo.InvariantCulture);
        for (int i = 0; i < n; i++) h = (h ^ FoldAscii(scratch[i])) * FnvPrime;
        return h;
    }

    private static ulong HashBoolean(ulong h, bool v) =>
        Fnv(h, v ? "TRUE"u8 : "FALSE"u8);   // fold("True"), fold("False")

    private static ulong Fnv(ulong h, ReadOnlySpan<byte> bytes)
    {
        foreach (byte b in bytes) h = (h ^ b) * FnvPrime;
        return h;
    }

    // ── Query: legacy blooms (segments written before #86) ────────────────────────────────────

    /// <summary>Key probe for a pre-#86 bloom. Differs from <see cref="HashKey"/> only for a key
    /// holding a character outside the BMP, which the old hash encoded one surrogate at a time.</summary>
    public static ulong LegacyHashKey(string key) => LegacyFnv(key, suffix: null);

    /// <summary>Equality probe for a pre-#86 bloom — only where <see cref="LegacyValueProbeIsExact"/>.</summary>
    public static ulong LegacyHashKeyValue(string key, string lowerValue) => LegacyFnv(key, lowerValue);

    /// <summary>
    /// WHETHER A PRE-#86 BLOOM CAN BE TRUSTED TO HOLD <paramref name="lowerValue"/> FOR EVERY VALUE
    /// THE EVALUATOR WOULD MATCH IT WITH. The old bloom hashed <c>lowercase(value.ToString())</c> in
    /// the WRITER's culture, and two things make that disagree with today's evaluator:
    /// <list type="bullet">
    ///   <item><b>A number's text.</b> A literal that is the invariant text of some number can
    ///     match an integer or a double whose writer-culture text was different: <c>0.375</c> is
    ///     <c>0,375</c> under ru-KZ, <c>-3</c> is <c>−3</c> under sv-SE, <c>NaN</c> and
    ///     <c>Infinity</c> have localised symbols, an exponent carries a sign. Only a run of ASCII
    ///     digits is the same text in every culture, so a literal that parses as a number and is not
    ///     one is not exact.</item>
    ///   <item><b>Any character outside ASCII.</b> The old bloom lowercased with the WRITER host's
    ///     casing, which nobody recorded: ICU of whatever version, or .NET's own data under invariant
    ///     globalization. Lowercase is not <c>OrdinalIgnoreCase</c>'s equivalence either — 44 BMP
    ///     pairs (<c>ς</c>/<c>σ</c>, <c>µ</c>/<c>μ</c>, U+1C80–U+1C88 against <c>в д о с т ъ ѣ ꙋ</c>, …)
    ///     are equal to the comparer and lowercase apart — and hosts disagree about the rest
    ///     (Unicode 16 gave <c>ɤ</c> an uppercase that older ICU does not know). ASCII lowercases the
    ///     same on every host there has ever been, and no non-ASCII character is equal to an ASCII
    ///     one under the comparer, so an all-ASCII literal is the one case the old bits can be
    ///     trusted for.</item>
    /// </list>
    /// For everything else the reader probes the key alone, which the old bloom holds exactly: the
    /// block skip degrades to key presence, and no row is lost. That includes every Cyrillic and
    /// Greek literal, for as long as segments written before #86 are retained. Query-side only,
    /// once per hint.
    /// </summary>
    public static bool LegacyValueProbeIsExact(string lowerValue)
    {
        bool allDigits = true;
        foreach (char c in lowerValue)
        {
            if (c >= 0x80) return false;
            if ((uint)(c - '0') > 9) allDigits = false;
        }
        if (allDigits) return true;
        return !double.TryParse(lowerValue, NumberStyles.Float, CultureInfo.InvariantCulture, out _);
    }

    /// <summary>
    /// WHETHER A CANONICAL BLOOM CAN BE PROBED BY VALUE for <paramref name="lowerValue"/>.
    /// <paramref name="sameFold"/> says the segment carries this build's
    /// <see cref="SpanBloomFold.Fingerprint"/>; when it does not, the segment was folded by another
    /// build's table and only an all-ASCII literal is trusted (ASCII folds identically in every
    /// table). When it does, a literal is trusted unless it holds a character this HOST's comparer
    /// treats differently from the table (<see cref="SpanBloomFold.HostDisagrees"/> — none on the
    /// hosts this build runs on). Anything not trusted probes the key alone.
    /// </summary>
    public static bool CanonicalValueProbeIsExact(string lowerValue, bool sameFold)
    {
        foreach (char c in lowerValue)
        {
            if (c < 0x80) continue;
            if (!sameFold || SpanBloomFold.HostDisagrees(c)) return false;
        }
        return true;
    }

    /// <summary>
    /// THE PRE-#86 HASH, verbatim: FNV-1a 64 over UTF-8 of the key, optionally followed by 0x1F and
    /// the value lowercased one UTF-16 unit at a time (so a surrogate half encodes as U+FFFD).
    /// Kept for reading segments written before the canonical hash; nothing writes it any more.
    /// </summary>
    private static ulong LegacyFnv(string key, string? suffix)
    {
        ulong h = FnvOffset;
        h = HashUtf8(h, key, lower: false);
        if (suffix is not null)
        {
            h = (h ^ Separator) * FnvPrime;
            h = HashUtf8(h, suffix, lower: true);
        }
        return h;

        static ulong HashUtf8(ulong h, string s, bool lower)
        {
            // BOTH buffers must live OUTSIDE the loop. `stackalloc` inside a loop is not
            // freed per iteration — the frame keeps growing until the method returns
            // (CA2014). The non-ASCII branch is only "rare" for English text: for Cyrillic,
            // CJK or any accented input it is taken on EVERY character, so a long message
            // walked the stack down one allocation per char until the thread overflowed.
            Span<byte> buf = stackalloc byte[128];
            Span<char> one = stackalloc char[1];
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (lower) c = char.ToLowerInvariant(c);
                if (c < 0x80)
                {
                    h = (h ^ (byte)c) * FnvPrime;
                }
                else
                {
                    one[0] = c;
                    int n  = Encoding.UTF8.GetBytes(one, buf);
                    for (int j = 0; j < n; j++) h = (h ^ buf[j]) * FnvPrime;
                }
            }
            return h;
        }
    }
}
