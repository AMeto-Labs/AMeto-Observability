namespace Ameto.Indexing;

/// <summary>
/// What sealed index groups measured — distinct (property, value) terms and distinct
/// trigrams — for the next builder to size itself by. One instance is shared by every
/// builder the sink factory creates, flushes on several threads and merges alike, and the
/// numbers are hints: a stale or racing read costs a rehash or an over-sized rental, never
/// correctness. Zero means nothing measured yet and the builders use their own floors.
///
/// <para>CLAMPED, not the raw last count. A single huge merge group would otherwise make every
/// builder created before the next small group sealed rent its 90 MB entry array and 16 MB
/// tables, hold them for the group, and park them in the pool. The hint is the smaller of the
/// last group's count and twice a running average, under an absolute ceiling
/// (<see cref="MaxTerms"/>, <see cref="MaxTrigrams"/>) that bounds the pre-size at what a 64 MB
/// group of dense events actually reaches; a group past it grows by doubling from there. So a
/// real step up in shape is followed fully within two or three groups, and a one-off IS followed,
/// at roughly half its size, until the next group seals: steady 10k-term groups then one 400k
/// group give min(400k, 2 x 107.5k) = 215k for every builder created before the next seal.</para>
///
/// <para>One instance serves every flush level and every merge, so interleaved small and large
/// groups pre-size each other imperfectly: a large group rehashes up from a small hint, a small
/// one rents tables sized for a large one. That costs rehashes or oversized rentals, never
/// correctness. Keying the hints by (flush or merge, level) is the fix if it shows in a profile.</para>
/// </summary>
public sealed class IndexBuildHints
{
    /// <summary>Largest pre-size for the term table: ~2^20 entries is a 64 MB group of unique-id events.</summary>
    public const int MaxTerms    = 1 << 20;
    /// <summary>Largest pre-size for the trigram buckets: the 2^21 ASCII key space is the ceiling anyway.</summary>
    public const int MaxTrigrams = 1 << 20;

    private int _terms, _termsAvg;
    private int _trigrams, _trigramsAvg;

    public int LastTerms    => Volatile.Read(ref _terms);
    public int LastTrigrams => Volatile.Read(ref _trigrams);

    /// <summary>Exception payloads no group could read as an exception map, process-wide — see
    /// <c>SegmentIndexBuilder.MalformedExceptionPayloads</c>. Rows are written; their @x.* terms are not.</summary>
    public long MalformedExceptionPayloads => Volatile.Read(ref _malformed);
    private long _malformed;
    internal void NoteMalformedException() => Interlocked.Increment(ref _malformed);

    public void Record(int terms, int trigrams)
    {
        Volatile.Write(ref _terms,    Clamp(terms,    ref _termsAvg,    MaxTerms));
        Volatile.Write(ref _trigrams, Clamp(trigrams, ref _trigramsAvg, MaxTrigrams));
    }

    /// <summary>min(last, 2 × EMA), EMA with a quarter weight on the newest group, capped.</summary>
    private static int Clamp(int last, ref int avg, int max)
    {
        last = Math.Clamp(last, 0, max);
        int prior = Volatile.Read(ref avg);
        int ema   = prior == 0 ? last : (int)(((long)prior * 3 + last) / 4);
        Volatile.Write(ref avg, ema);
        return Math.Min(last, 2 * ema);
    }
}
