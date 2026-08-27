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
        ApproxRetainedBytes = inverted.ApproxRetainedBytes()
                            + trigram.ApproxRetainedBytes()
                            + bloom.RetainedBytes;
    }

    /// <summary>
    /// Approximate bytes this reader keeps alive (decoded postings + dictionaries + the
    /// native bloom bits), computed once at load. This — not the varint-packed section
    /// length it was decoded from, which is 3-8x smaller — is what the
    /// <see cref="SegmentIndexCache"/> budget charges per entry.
    /// </summary>
    public long ApproxRetainedBytes { get; }

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

    /// <summary>
    /// The group's bloom filter, for the executor's phase-1 gates when this reader comes
    /// out of the <see cref="SegmentIndexCache"/> — a cache hit must not re-read and
    /// re-deserialise the bloom section it already holds decoded.
    /// </summary>
    public SegmentBloomFilter Bloom => _bloom;

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
