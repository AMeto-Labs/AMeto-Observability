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
        string valStr = value?.ToString() ?? string.Empty;
        return _bloom.MightContain(valStr) && _inverted.MightContain(propertyName, value);
    }

    /// <inheritdoc/>
    public uint[]? LookupTrigram(ReadOnlySpan<char> text) =>
        _trigram.Lookup(text);
}
