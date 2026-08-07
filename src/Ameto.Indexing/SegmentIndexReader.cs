using Ameto.Core;

namespace Ameto.Indexing;

/// <summary>
/// Read-only composite segment index loaded from the bytes embedded in a .seg file.
/// Implements ISegmentIndex for use by the query layer.
/// </summary>
public sealed class SegmentIndexReader : ISegmentIndex
{
    private readonly SegmentInvertedIndex _inverted;
    private readonly SegmentTrigramIndex  _trigram;
    private readonly SegmentBloomFilter   _bloom;

    private SegmentIndexReader(
        SegmentInvertedIndex inverted,
        SegmentTrigramIndex  trigram,
        SegmentBloomFilter   bloom)
    {
        _inverted = inverted;
        _trigram  = trigram;
        _bloom    = bloom;
    }

    /// <summary>Loads indexes from the raw byte sections read out of a .seg file.</summary>
    public static SegmentIndexReader Load(
        ReadOnlySpan<byte> invertedBytes,
        ReadOnlySpan<byte> trigramBytes,
        ReadOnlySpan<byte> bloomBytes)
    {
        return new SegmentIndexReader(
            SegmentInvertedIndex.Deserialise(invertedBytes),
            SegmentTrigramIndex.Deserialise(trigramBytes),
            SegmentBloomFilter.Deserialise(bloomBytes));
    }

    // ── ISegmentIndex ─────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public uint[]? Lookup(string propertyName, object? value) =>
        _inverted.Lookup(propertyName, value);

    /// <inheritdoc/>
    public bool MightContain(string propertyName, object? value)
    {
        // Bloom filter gives a cheap first gate; inverted index is the definitive check.
        return MightContainValue(_bloom, value) && _inverted.MightContain(propertyName, value);
    }

    /// <summary>
    /// Probes the bloom for every PLAIN text the builder could have stored for a value the
    /// scan would accept.
    ///
    /// <para>The bloom is keyless: it answers "did any value here look like this", and the
    /// only text it holds is what <c>SegmentIndexBuilder</c> added. One literal can match
    /// several of those — <c>Count = '5'</c> against a stored integer, and (before the index
    /// went invariant) <c>Ratio = '2.5'</c> against a <c>ru-KZ</c> host's <c>2,5</c>. Probing
    /// a single spelling made the cheap gate drop the segment before the inverted index,
    /// which by then knew better, was ever consulted.</para>
    /// </summary>
    public static bool MightContainValue(SegmentBloomFilter bloom, object? value)
    {
        IndexValueForms.Buffer buf = default;
        Span<string?> forms = buf;
        int n = IndexValueForms.Plain(value, forms);

        for (int i = 0; i < n; i++)
            if (bloom.MightContain(forms[i]!)) return true;
        return false;
    }

    /// <inheritdoc/>
    public uint[]? LookupTrigram(ReadOnlySpan<char> text) =>
        _trigram.Lookup(text);

    /// <inheritdoc/>
    public uint[]? LookupIntersect(IReadOnlyList<(string property, object? value)> predicates) =>
        _inverted.LookupIntersect(predicates);
}
