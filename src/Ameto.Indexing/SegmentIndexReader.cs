using Ameto.Core;

namespace Ameto.Indexing;

/// <summary>
/// Read-only composite segment index loaded from the bytes embedded in a .seg file.
/// Implements ISegmentIndex for use by the query layer.
/// </summary>
/// <remarks>
/// Disposable because <see cref="SegmentBloomFilter"/> owns a <c>NativeMemory</c> block and
/// has no finalizer: every prefiltered segment used to leak bitCount/8 native bytes PER
/// QUERY. The leak scales with the filter's size, so it grows in step with segment size —
/// exactly the direction this refactor moves in.
/// </remarks>
public sealed class SegmentIndexReader : ISegmentIndex, IDisposable
{
    private int _disposed;

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

    /// <summary>
    /// Loads indexes from the raw byte sections read out of a .seg file.
    /// <paramref name="trigramBytes"/> may be empty when the query has no substring
    /// predicate — the trigram section is the largest in the file (~43% of it), and
    /// deserialising it for a query that will never consult it is pure waste.
    /// </summary>
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

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0) _bloom.Dispose();
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

    /// <inheritdoc/>
    public uint[]? LookupIntersect(IReadOnlyList<(string property, object? value)> predicates) =>
        _inverted.LookupIntersect(predicates);
}
