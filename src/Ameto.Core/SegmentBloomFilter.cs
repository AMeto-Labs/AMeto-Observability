using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Ameto.Core;

/// <summary>
/// Per-segment XOR/blocked Bloom filter stored on NativeMemory.
///
/// We use a simple blocked Bloom filter (block size = 64 bytes = 512 bits) with
/// 3 independent 32-bit hash functions (MurmurHash3 finaliser mix).
///
/// False-positive rate: ~1% at capacity; false-negative rate: 0% (guaranteed).
///
/// Indexed key: the UTF-8 bytes of a value's text, CASE-FOLDED by <see cref="CaseFold"/> — see
/// <see cref="Add(ReadOnlySpan{char})"/>. A filter made by <see cref="CreateForBinaryKeys"/> holds
/// raw keys instead and takes no text.
///
/// Binary format — two layouts, told apart by the capacity word of the first header:
///   LEGACY (every build before the canonical fold; binary-keyed filters still):
///     uint32 bitCount   (must be a multiple of 512)
///     uint32 capacity   (informational; high bit = <see cref="FoldedMarker"/>)
///     byte[] bits       (bitCount / 8 bytes)
///   CANONICAL (text folded by <see cref="CaseFold"/>):
///     uint32 512, uint32 <see cref="CanonicalMarker"/>, 64 × 0xFF
///                       — a legacy filter of one block with every bit set
///     uint64 the writer's <see cref="CaseFold.Fingerprint"/>
///     uint32 bitCount, uint32 capacity, byte[] bits — the filter itself
/// </summary>
public sealed unsafe class SegmentBloomFilter : IDisposable
{
    // Each block = 64 bytes = 512 bits; we pick a block per hash[0], then set 2 bits inside it.
    private const int BlockBytes = 64;
    private const int BlockBits  = BlockBytes * 8; // 512

    /// <summary>
    /// High bit of the LEGACY header's <c>capacity</c> word: the text in the bits was lowercased
    /// with <c>ToLowerInvariant</c> by whichever host wrote it. Every build from the one that started
    /// folding up to the canonical fold set it; a blob without it was written before, and holds the
    /// ORIGINAL casing.
    ///
    /// <para>Capacity is informational — it is the segment's expected event count, never
    /// within nine orders of magnitude of 2^31 — so the spare bit costs nothing and saved a
    /// whole-file version bump. A binary-keyed filter (<see cref="CreateForBinaryKeys"/>) still
    /// writes it, as every build has, so the files that hold one keep their bytes.</para>
    /// </summary>
    private const uint FoldedMarker = 0x8000_0000u;

    /// <summary>
    /// The capacity word of the one-block filter a CANONICAL section starts with: "LBC1"
    /// little-endian. It is what tells this build's reader that the real filter follows, folded by
    /// <see cref="CaseFold"/>.
    ///
    /// <para>No legacy section carries it. A legacy capacity word is the filter's capacity — at
    /// most 0x0CCC_CCCC (<see cref="MaxExpectedItems"/>) since sizing was clamped, a group's
    /// forecast nowhere near 2^28 before that — with or without <see cref="FoldedMarker"/>'s bit 31;
    /// this is above 2^28 with bit 31 clear. (A section that had it by accident would be one block
    /// long, too short for what the marker promises, and refused rather than misread.) And the
    /// one-block filter in front of it has every bit set, which is what an OLDER build reads: a
    /// filter that answers "might" to every probe, so a segment this build wrote costs an older
    /// one its bloom gate and never a row. It cannot read the canonical bits, and must not — it
    /// would probe them with its own lowercase.</para>
    /// </summary>
    private const uint CanonicalMarker = 0x3143_424Cu;   // 'L' 'B' 'C' '1'

    /// <summary>The one-block legacy filter in front of a canonical one: header and bits.</summary>
    private const int StubBytes = 8 + BlockBytes;

    /// <summary>Everything a canonical section holds before its bits: the stub, the fingerprint
    /// and the filter's own header.</summary>
    private const int CanonicalPreambleBytes = StubBytes + 8 + 8;

    /// <summary>
    /// How the TEXT in a filter's bits was folded — read off the section it came from, never
    /// assumed, because it decides which of a literal's spellings a probe can stand for.
    /// </summary>
    internal enum TextFold : byte
    {
        /// <summary>The original casing: a section written before folding shipped.</summary>
        None,

        /// <summary><c>ToLowerInvariant</c> of the host that wrote it (<see cref="FoldedMarker"/>):
        /// every build from folding up to the canonical fold. Also what a binary-keyed filter's
        /// header says, as it always has.</summary>
        LowerInvariant,

        /// <summary><see cref="CaseFold"/>, by the table whose fingerprint the section records.</summary>
        Canonical,
    }

    private readonly byte*    _bits;
    private readonly uint     _blockCount;
    private readonly uint     _capacity;
    private readonly TextFold _fold;
    private readonly ulong    _foldFingerprint;
    private readonly bool     _binaryKeys;
    private          bool     _disposed;
    private          long     _added;

    private SegmentBloomFilter(byte* bits, uint blockCount, uint capacity, TextFold fold, ulong foldFingerprint, bool binaryKeys)
    {
        _bits            = bits;
        _blockCount      = blockCount;
        _capacity        = capacity;
        _fold            = fold;
        _foldFingerprint = foldFingerprint;
        _binaryKeys      = binaryKeys;
    }

    /// <summary>How the text in these bits was folded.</summary>
    internal TextFold Fold => _fold;

    /// <summary>The <see cref="CaseFold.Fingerprint"/> a <see cref="TextFold.Canonical"/> filter
    /// was folded under; 0 for the others.</summary>
    internal ulong FoldFingerprint => _foldFingerprint;

    /// <summary>Native bytes the bit array occupies — for the index cache's retained-size accounting.</summary>
    public long RetainedBytes => (long)_blockCount * (BlockBits / 8);

    /// <summary>
    /// Largest item count that still sizes honestly. Above it the filter is capped rather
    /// than allowed to wrap: <c>expectedItems * 10</c> in int arithmetic used to overflow
    /// silently and hand back a 64-byte match-everything filter — no crash, just a
    /// prefilter that stops rejecting anything. Callers now size by TERM count, which
    /// reaches this an order of magnitude sooner than event count did.
    /// </summary>
    private const long MaxExpectedItems = (long)int.MaxValue / 10;

    /// <summary>
    /// Hard ceiling on ONE filter's bits, in bytes. A filter is allocated from a FORECAST — the
    /// writer's guess at how many terms a group will hold, made before the group's first event is
    /// indexed — and a forecast has no upper bound of its own. This gives it one.
    ///
    /// <para>16 MiB is ~13.4 M terms at the design's 10 bits/term, which is more terms than a
    /// 64 MB index group can hold. MEASURED on the two ends of event shape: prop-dense events
    /// (trace ids, eight properties, an exception on one in twenty) run 21.1 terms per event over
    /// 329 payload bytes, so a full 64 MB group holds ~4.3 M terms; thin ones run 7.0 terms over
    /// 151 bytes, ~3.1 M terms. Even doubled for headroom the densest of those asks for 10.7 MB,
    /// so the ceiling is a backstop against a wrong forecast rather than a budget anything
    /// realistic bumps into.</para>
    ///
    /// <para>What it backstops: the writer's event forecast divides the group's payload budget by
    /// the file's measured bytes per row, floored at what the block format spends on a row before
    /// any content — 57 bytes (<c>SegmentWriter.FixedRowCostBytes</c>). On the merge path, where
    /// the source's remaining-event hint is the sum of every source and so does not bind, a 64 MB
    /// group can therefore be forecast at up to 1 177 348 events; at the fallback 64 terms/event
    /// that is 75.3 M terms and 89.8 MiB of <see cref="NativeMemory"/> for a SINGLE group's
    /// filter, copied into an equally large managed array at <see cref="Serialise"/> and written
    /// to the file. Capping costs selectivity only in the case that was already pathological, and
    /// costs it gracefully: bits are shared out over more terms rather than the allocation being
    /// let run.</para>
    ///
    /// <para>That 57 is the whole basis of the figure, so it is pinned by
    /// <c>SegmentBlockGeometryTests</c> rather than merely asserted here. The paragraph used to
    /// read 2 097 152 events and 160 MiB, which needs a 32-byte row — a divisor nothing in the
    /// format could produce, since the fixed columns and the four string columns' offsets cost 57
    /// before a template, a service name or a property is written.</para>
    /// </summary>
    private const long MaxFilterBytes = 16L * 1024 * 1024;

    /// <summary>Creates a new empty TEXT filter sized for <paramref name="expectedItems"/>, subject
    /// to <see cref="MaxFilterBytes"/>: text goes in folded by <see cref="CaseFold"/>, and the
    /// section it serialises to is the canonical layout.</summary>
    public static SegmentBloomFilter Create(long expectedItems) =>
        Allocate(expectedItems, TextFold.Canonical, CaseFold.Fingerprint, binaryKeys: false);

    /// <summary>
    /// Creates a filter for keys that are not text — the trace-id index's eight-byte ids. Only the
    /// byte overloads apply (the text ones throw), and it serialises in the legacy layout with the
    /// folded marker, byte for byte what every build has written for it: there is no casing in a
    /// key like that, and so no reason for those files to change.
    /// </summary>
    public static SegmentBloomFilter CreateForBinaryKeys(long expectedItems) =>
        Allocate(expectedItems, TextFold.LowerInvariant, 0, binaryKeys: true);

    private static SegmentBloomFilter Allocate(long expectedItems, TextFold fold, ulong foldFingerprint, bool binaryKeys)
    {
        // ~10 bits per item → ~1% FPR; round up to multiple of 512. Long arithmetic
        // throughout, then clamped — the old int product wrapped instead of saturating.
        long clamped    = Math.Clamp(expectedItems, 1, MaxExpectedItems);
        long bitsWanted = Math.Clamp(clamped * 10, BlockBits, MaxFilterBytes * 8);
        uint totalBits  = (uint)Math.Min(bitsWanted, uint.MaxValue - BlockBits);
        totalBits       = (totalBits + (uint)(BlockBits - 1)) & ~(uint)(BlockBits - 1);
        uint blockCount = totalBits / BlockBits;
        uint byteCount  = totalBits / 8;

        // Capacity describes the FILTER, not the request: when the ceiling bites, saying the
        // filter was sized for items it has no bits for would make every bits-per-term reading
        // taken from a file a lie. Rounding up to a block multiple means this is a no-op for
        // every filter that is not capped.
        long affordable = Math.Min(clamped, totalBits / 10);

        var bits = (byte*)NativeMemory.AllocZeroed(byteCount);
        return new SegmentBloomFilter(bits, blockCount, (uint)affordable, fold, foldFingerprint, binaryKeys);
    }

    // ── Write ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Terms handed to <see cref="Add(ReadOnlySpan{byte})"/> since construction — the count the
    /// filter was ACTUALLY asked to hold, against the <c>expectedItems</c> it was sized for.
    ///
    /// <para>Every overload funnels through the byte one, so this is the whole write side in a
    /// single counter, and it survives <see cref="Dispose"/> (only the bits are native) — the one
    /// deliberate exception to the disposed-state guards on <see cref="Add(ReadOnlySpan{byte})"/>,
    /// <see cref="MightContain(ReadOnlySpan{byte})"/> and <see cref="Serialise"/>, and required to
    /// be so by <c>ISegmentIndexSink.BloomTermsAdded</c>. It
    /// exists because sizing a filter by a guessed terms-per-event is what makes the section
    /// cost what it does; a writer that can read this back sizes the next group from what the
    /// last one measured. Reading it back is also the only way to state bits-per-term, which is
    /// the number that says whether a filter is selective or saturated.</para>
    /// </summary>
    public long AddedTermCount => _added;

    /// <summary>
    /// Terms these bits carry at the design's ~10 bits each — what the filter can actually hold,
    /// which is the request only when <see cref="MaxFilterBytes"/> did not bite.
    ///
    /// <para>The writer reads it off the group's sink and seals the group before
    /// <see cref="AddedTermCount"/> reaches it, so a forecast that was too small costs a smaller
    /// group instead of a saturated filter. That is also what makes the ceiling a real bound
    /// rather than a quality setting: a capped filter reports the capped number, so the group is
    /// cut at the bits it was given.</para>
    /// </summary>
    public long Capacity => _capacity;

    public void Add(ReadOnlySpan<byte> key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _added++;
        var (h0, h1, h2) = Hash3(key);
        uint blockIdx    = h0 % _blockCount;
        byte* block      = _bits + blockIdx * BlockBytes;

        SetBit(block, h1 % BlockBits);
        SetBit(block, h2 % BlockBits);
    }

    public void Add(string value) => Add((ReadOnlySpan<char>)value);

    /// <summary>
    /// Adds <paramref name="value"/> FOLDED BY <see cref="CaseFold"/>, encoding into stack/pooled
    /// scratch instead of allocating a byte[] per add.
    ///
    /// <para>Folding is a correctness requirement, not a nicety: the per-event scan compares
    /// strings <see cref="StringComparison.OrdinalIgnoreCase"/>, so a filter keyed on the raw
    /// casing answers "not here" for a value that differs from the query literal only in case
    /// — <c>@l = 'error'</c> against a stored <c>Error</c> — and the whole segment is dropped
    /// unread. And it has to be THAT comparer's fold: the <c>ToLowerInvariant</c> this used to
    /// store keeps <c>ς</c> apart from <c>σ</c> and <c>µ</c> from <c>μ</c>, which the comparer
    /// calls equal, so <c>Place = 'ΟΔΟΣ'</c> lost a stored <c>οδος</c> the moment it was flushed
    /// (see <see cref="CaseFold"/>). Folding also keeps the filter's occupancy where it was;
    /// storing both casings would have raised the false-positive rate for every segment.</para>
    /// </summary>
    public void Add(ReadOnlySpan<char> value)
    {
        // Guarded HERE as well as in the byte overload, and before the rent: this path takes a
        // pooled buffer and returns it without a finally, so a throw from further in would strand
        // it. The duplicate check is a load of an already-hot bool against three Murmur passes.
        ThrowIfNotCanonicalText();

        char[]? rented = value.Length > 256 ? ArrayPool<char>.Shared.Rent(value.Length) : null;
        Span<char> folded = rented ?? stackalloc char[256];
        int n = CaseFold.FoldUtf16(value, folded);
        AddCanonical(folded[..n]);
        if (rented is not null) ArrayPool<char>.Shared.Return(rented);
    }

    /// <summary>
    /// Adds the folded form of a UTF-8 value — what <see cref="Add(ReadOnlySpan{char})"/> stores,
    /// reached without a UTF-16 round trip when the value is ASCII: <see cref="CaseFold"/> over
    /// ASCII is A-Z upper, which is <c>Ascii.ToUpper</c>, and the re-encoding of ASCII chars is the
    /// identity, so the hashed bytes — and therefore the bits — are the same. Anything with a
    /// non-ASCII byte is folded scalar by scalar straight off its UTF-8.
    /// </summary>
    public void AddUtf8(ReadOnlySpan<byte> value)
    {
        ThrowIfNotCanonicalText();

        byte[]? rented = value.Length > 512 ? ArrayPool<byte>.Shared.Rent(value.Length) : null;
        Span<byte> fold = rented ?? stackalloc byte[512];
        try
        {
            if (Ascii.ToUpper(value, fold, out int n) == OperationStatus.Done)
            {
                Add(fold[..n]);
                return;
            }
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }

        char[]? rentedChars = value.Length > 256 ? ArrayPool<char>.Shared.Rent(value.Length) : null;
        Span<char> chars = rentedChars ?? stackalloc char[256];
        try
        {
            int n = CaseFold.FoldUtf8(value, chars);
            AddCanonical(chars[..n]);
        }
        finally
        {
            if (rentedChars is not null) ArrayPool<char>.Shared.Return(rentedChars);
        }
    }

    /// <summary>
    /// Adds text ALREADY folded by <see cref="CaseFold"/> — one BMP unit per scalar, never a
    /// surrogate — as the UTF-8 its probe will hash. The index builder folds a non-ASCII value
    /// once for this filter and the trigram index together, and comes in here.
    /// </summary>
    internal void AddCanonical(ReadOnlySpan<char> folded)
    {
        ThrowIfNotCanonicalText();
        int max = folded.Length * 3;   // a BMP unit that is not a surrogate is at most three bytes
        byte[]? rented = max > 512 ? ArrayPool<byte>.Shared.Rent(max) : null;
        Span<byte> utf8 = rented ?? stackalloc byte[512];
        int n = Encoding.UTF8.GetBytes(folded, utf8);
        Add(utf8[..n]);
        if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
    }

    /// <summary>
    /// Text goes only into a filter whose bits are canonical: a binary-keyed one serialises in the
    /// legacy layout, and one read from an older section holds another fold — either would put
    /// canonical bits under a header that tells the reader to probe them some other way.
    /// </summary>
    private void ThrowIfNotCanonicalText()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_fold != TextFold.Canonical)
            throw new InvalidOperationException(_binaryKeys
                ? "This bloom filter holds binary keys; text goes into one made by SegmentBloomFilter.Create."
                : $"This bloom filter was read from a section folded {_fold}; canonical text cannot be added to it.");
    }

    // ── Query ─────────────────────────────────────────────────────────────────

    public bool MightContain(ReadOnlySpan<byte> key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var (h0, h1, h2) = Hash3(key);
        uint blockIdx    = h0 % _blockCount;
        byte* block      = _bits + blockIdx * BlockBytes;

        return TestBit(block, h1 % BlockBits) &&
               TestBit(block, h2 % BlockBits);
    }

    public bool MightContain(string value) => MightContain((ReadOnlySpan<char>)value);

    /// <summary>
    /// Whether a value the scan would accept for <paramref name="value"/> might be here. The
    /// answer "no" is a PROOF that drops the segment's group unread, so it is given only where the
    /// section's fold makes it one; everywhere else the answer is "might", which costs the group
    /// this cheap gate and keeps its rows — the inverted index still prunes an equality one phase
    /// later, exactly. What the section can prove depends on how its text was folded:
    /// <list type="bullet">
    ///   <item><b>Canonical, this build's table.</b> The literal is folded the same way and probed.
    ///     Proof for any literal — unless the HOST compares one of its characters differently from
    ///     the table (<see cref="CaseFold.HostDisagrees"/>; no host this build runs on does).</item>
    ///   <item><b>Canonical, another build's table.</b> Only an all-ASCII literal: ASCII folds the
    ///     same in every table there has been, and no other character does necessarily.</item>
    ///   <item><b>Lowercase (<see cref="FoldedMarker"/>), every build before the canonical
    ///     fold.</b> Only an all-ASCII literal, probed lowercased. The writer lowercased with ITS
    ///     host's casing, which nobody recorded (ICU of some version, or .NET's data under invariant
    ///     globalization), and lowercase is not the comparer's equivalence in any case: a stored
    ///     <c>οδος</c> holds <c>ς</c> where <c>ΟΔΟΣ</c> lowercases to <c>σ</c>, a stored <c>ᲀода</c>
    ///     holds <c>ᲀ</c> where <c>ВОДА</c> lowercases to <c>в</c> — and the archaic forms behind
    ///     <c>в д о с т ъ</c> make even ordinary Cyrillic unprovable. ASCII lowercases the same on
    ///     every host there has ever been, and no non-ASCII character is equal to an ASCII one
    ///     under the comparer, so an ASCII literal's lowercase is the one spelling every matching
    ///     value was stored under. Folding on the WRITE side fixes nothing for bytes already on
    ///     disk; segments written before the canonical fold keep this rule until retention or a
    ///     merge retires them.</item>
    ///   <item><b>Original casing, a section from before folding shipped.</b> Only a literal with
    ///     exactly one spelling — ASCII with no letter: a number, an id of digits and dashes. The
    ///     filter holds one arbitrary casing of anything else.</item>
    /// </list>
    /// And on a host whose comparer equates a non-ASCII character with an ASCII one
    /// (<see cref="CaseFold.HostDisagreesOnAscii"/>), none of the ASCII premises hold, and nothing
    /// is proof.
    /// </summary>
    public bool MightContain(ReadOnlySpan<char> value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (CaseFold.HostDisagreesOnAscii) return true;

        switch (_fold)
        {
            case TextFold.Canonical:
                bool trusted = _foldFingerprint == CaseFold.Fingerprint
                    ? !CaseFold.HostDisagreesWithAny(value)
                    : Ascii.IsValid(value);
                return !trusted || MightContainCanonical(value);

            case TextFold.LowerInvariant:
                return !Ascii.IsValid(value) || MightContainAscii(value, lower: true);

            default:
                return !Ascii.IsValid(value)
                    || value.ContainsAnyInRange('A', 'Z') || value.ContainsAnyInRange('a', 'z')
                    || MightContainAscii(value, lower: false);
        }
    }

    /// <summary>The probe for text folded by <see cref="CaseFold"/>: what <see cref="Add(ReadOnlySpan{char})"/> hashed.</summary>
    private bool MightContainCanonical(ReadOnlySpan<char> value)
    {
        char[]? rentedChars = value.Length > 256 ? ArrayPool<char>.Shared.Rent(value.Length) : null;
        Span<char> folded = rentedChars ?? stackalloc char[256];
        int n = CaseFold.FoldUtf16(value, folded);

        int max = n * 3;
        byte[]? rented = max > 512 ? ArrayPool<byte>.Shared.Rent(max) : null;
        Span<byte> utf8 = rented ?? stackalloc byte[512];
        int len = Encoding.UTF8.GetBytes(folded[..n], utf8);
        bool hit = MightContain(utf8[..len]);

        if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        if (rentedChars is not null) ArrayPool<char>.Shared.Return(rentedChars);
        return hit;
    }

    /// <summary>The probe for an all-ASCII literal against a legacy section: its bytes, lowercased
    /// when the section was.</summary>
    private bool MightContainAscii(ReadOnlySpan<char> value, bool lower)
    {
        byte[]? rented = value.Length > 512 ? ArrayPool<byte>.Shared.Rent(value.Length) : null;
        Span<byte> ascii = rented ?? stackalloc byte[512];
        int n;
        if (lower) Ascii.ToLower(value, ascii, out n);
        else       Ascii.FromUtf16(value, ascii, out n);
        bool hit = MightContain(ascii[..n]);
        if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        return hit;
    }

    // ── Serialisation ─────────────────────────────────────────────────────────

    /// <summary>
    /// Copies the filter out as a section blob.
    ///
    /// <para>Guarded, because the copy is <c>new Span&lt;byte&gt;(_bits, byteCount)</c> and
    /// <see cref="Dispose"/> hands those bytes back to the allocator without clearing
    /// <c>_bits</c> — the pointer is <c>readonly</c> and stays valid-looking forever. Calling this
    /// after disposal therefore READ FREED MEMORY and returned whatever had since been allocated
    /// over it, with no exception and no null to trip over: a segment's bloom section written from
    /// another allocation's bytes, which the query prefilter then trusts to reject segments.
    /// Measured before this guard, on a 4096-term filter disposed and then re-serialised with 32
    /// native allocations squatting on the block: 5120 of 5120 payload bytes came back as the
    /// squatter's fill pattern.</para>
    /// </summary>
    public byte[] Serialise()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        uint byteCount = _blockCount * BlockBytes;
        var buf = new byte[SerialisedLength];
        int preamble = WritePreamble(buf);
        new Span<byte>(_bits, (int)byteCount).CopyTo(buf.AsSpan(preamble));
        return buf;
    }

    /// <summary>Bytes <see cref="Serialise"/> / <see cref="WriteTo"/> produce: the bits and, before
    /// them, 8 bytes of header — or, for a canonical filter, the 88 bytes of
    /// <see cref="WritePreamble"/>.</summary>
    public long SerialisedLength =>
        (_fold == TextFold.Canonical ? CanonicalPreambleBytes : 8L) + (long)_blockCount * BlockBytes;

    /// <summary>
    /// Writes the section — the same bytes <see cref="Serialise"/> returns — straight from the
    /// native bits to <paramref name="destination"/>, with no managed copy in between. The
    /// blob path costs a ~5 MB LOH array per group that dies the moment the writer has
    /// written it; this is the production path. Guarded like <see cref="Serialise"/>, for
    /// the same reason.
    /// </summary>
    public void WriteTo(Stream destination)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        Span<byte> preamble = stackalloc byte[CanonicalPreambleBytes];
        int n = WritePreamble(preamble);
        destination.Write(preamble[..n]);
        destination.Write(new ReadOnlySpan<byte>(_bits, (int)(_blockCount * BlockBytes)));
    }

    /// <summary>
    /// Everything before the bits; returns its length. A canonical filter is preceded by the
    /// one-block, all-ones legacy filter and <see cref="CanonicalMarker"/> (what an older build
    /// reads, see there) and by the fingerprint of the table that folded it — this build's for a
    /// filter it created, the recorded one for a filter it read. A legacy filter keeps saying how
    /// its text was folded: one read back from a pre-folding section and re-serialised must not
    /// start claiming a fold its bits never had.
    /// </summary>
    private int WritePreamble(Span<byte> dest)
    {
        int at = 0;
        if (_fold == TextFold.Canonical)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(dest, BlockBits);
            BinaryPrimitives.WriteUInt32LittleEndian(dest[4..], CanonicalMarker);
            dest.Slice(8, BlockBytes).Fill(0xFF);
            BinaryPrimitives.WriteUInt64LittleEndian(dest[StubBytes..], _foldFingerprint);
            at = StubBytes + 8;
        }
        BinaryPrimitives.WriteUInt32LittleEndian(dest[at..], _blockCount * BlockBits);
        uint capacityWord = _fold == TextFold.LowerInvariant ? (_capacity & ~FoldedMarker) | FoldedMarker : _capacity & ~FoldedMarker;
        BinaryPrimitives.WriteUInt32LittleEndian(dest[(at + 4)..], capacityWord);
        return at + 8;
    }

    public static SegmentBloomFilter Deserialise(ReadOnlySpan<byte> data)
    {
        // An absent/empty bloom section means "no index was built" (e.g. a WAL
        // recovery flushed before the index builder was wired) — that is NO
        // information, so the filter must never reject. An all-zero Create(0)
        // would answer "contains nothing" and make the whole segment invisible
        // to equality-hinted queries. All-ones bits ⇒ MightContain always true.
        if (data.Length < 8)
        {
            var matchAll = Create(0);
            new Span<byte>(matchAll._bits, (int)(matchAll._blockCount * BlockBytes)).Fill(0xFF);
            return matchAll;
        }

        // A canonical section starts with a legacy filter of one block whose capacity word no
        // legacy writer produced (CanonicalMarker); the filter itself follows the fingerprint. A
        // section that has the marker and not the room for what it promises is torn, and is
        // refused below like any other — never read as the all-ones block in front of it, which
        // would quietly turn a damaged section into one that proves nothing.
        if (BinaryPrimitives.ReadUInt32LittleEndian(data) == BlockBits
            && BinaryPrimitives.ReadUInt32LittleEndian(data[4..]) == CanonicalMarker)
        {
            if (data.Length < CanonicalPreambleBytes)
                throw new InvalidDataException($"Canonical bloom section ends at byte {data.Length}, inside its {CanonicalPreambleBytes}-byte preamble");
            ulong fingerprint = BinaryPrimitives.ReadUInt64LittleEndian(data[StubBytes..]);
            return Read(data[(StubBytes + 8)..], TextFold.Canonical, fingerprint);
        }

        bool folded = (BinaryPrimitives.ReadUInt32LittleEndian(data[4..]) & FoldedMarker) != 0;
        return Read(data, folded ? TextFold.LowerInvariant : TextFold.None, 0);
    }

    /// <summary>One filter: a legacy section whole, or what follows a canonical one's preamble.</summary>
    private static SegmentBloomFilter Read(ReadOnlySpan<byte> data, TextFold fold, ulong foldFingerprint)
    {
        uint bitCount     = BinaryPrimitives.ReadUInt32LittleEndian(data[0..]);
        uint capacityWord = BinaryPrimitives.ReadUInt32LittleEndian(data[4..]);
        uint capacity     = capacityWord & ~FoldedMarker;

        // CHECKED BEFORE THE ALLOCATION, because the allocation is NativeMemory and the thing
        // that would throw after it is the copy. A section whose header overstates its own length
        // used to allocate first and fail on the Slice, and by then the pointer lived only in a
        // local of a frame that was unwinding — no owner constructed, no finaliser on this class,
        // nothing to free it. One blob claiming 0xFFFFFFFF bits committed 513 MB and lost it.
        //
        // What made that unbounded rather than a one-off is the caller: QueryExecutor's prefilter
        // catches per group, per segment, per query and falls back to a full scan, so the same
        // corrupt section is re-read and re-leaked by every query that touches it, forever, while
        // every managed-heap instrument reads flat.
        //
        // The multiple-of-512 check is the same guard wearing its other face. A bitCount in
        // [8, 512) passes the length test on a tiny blob and yields blockCount == 0, which is a
        // DivideByZeroException inside Add/MightContain rather than at construction — thrown from
        // under the pooled buffers the text overloads take, which they return without a finally.
        // The format has always required the multiple; nothing enforced it.
        if (bitCount == 0 || (bitCount & (uint)(BlockBits - 1)) != 0 || data.Length < 8L + bitCount / 8)
            throw new InvalidDataException(
                $"Bloom section declares {bitCount} bits ({bitCount / 8} bytes) in a {data.Length}-byte section");

        uint blockCount   = bitCount / BlockBits;
        uint byteCount    = bitCount / 8;

        var bits = (byte*)NativeMemory.AllocZeroed(byteCount);
        data.Slice(8, (int)byteCount).CopyTo(new Span<byte>(bits, (int)byteCount));
        return new SegmentBloomFilter(bits, blockCount, capacity, fold, foldFingerprint, binaryKeys: false);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void SetBit(byte* block, uint bit) =>
        block[bit >> 3] |= (byte)(1 << (int)(bit & 7));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TestBit(byte* block, uint bit) =>
        (block[bit >> 3] & (byte)(1 << (int)(bit & 7))) != 0;

    /// <summary>
    /// Three independent 32-bit hashes via MurmurHash3-style mix of two seeds.
    /// h_i(x) = hash(x, seed_i)  where seed_0=0, seed_1=0x9747b28c, seed_2=0xd4bc5a93
    /// </summary>
    private static (uint h0, uint h1, uint h2) Hash3(ReadOnlySpan<byte> data)
    {
        uint h0 = Murmur32(data, 0);
        uint h1 = Murmur32(data, 0x9747b28c);
        uint h2 = Murmur32(data, 0xd4bc5a93);
        return (h0, h1, h2);
    }

    private static uint Murmur32(ReadOnlySpan<byte> data, uint seed)
    {
        const uint c1 = 0xcc9e2d51u;
        const uint c2 = 0x1b873593u;
        uint h = seed;

        int i = 0;
        while (i + 4 <= data.Length)
        {
            uint k = (uint)(data[i] | (data[i+1] << 8) | (data[i+2] << 16) | (data[i+3] << 24));
            k  *= c1; k  = RotL(k, 15); k *= c2;
            h  ^= k;  h  = RotL(h, 13); h = h * 5 + 0xe6546b64u;
            i  += 4;
        }

        uint tail = 0;
        switch (data.Length - i)
        {
            case 3: tail |= (uint)data[i+2] << 16; goto case 2;
            case 2: tail |= (uint)data[i+1] << 8;  goto case 1;
            case 1: tail |= data[i];
                tail *= c1; tail = RotL(tail, 15); tail *= c2; h ^= tail;
                break;
        }

        h ^= (uint)data.Length;
        // Finalise mix
        h ^= h >> 16; h *= 0x85ebca6bu; h ^= h >> 13; h *= 0xc2b2ae35u; h ^= h >> 16;
        return h;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint RotL(uint x, int r) => (x << r) | (x >> (32 - r));

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        NativeMemory.Free(_bits);
    }
}
