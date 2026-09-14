namespace Ameto.Indexing;

/// <summary>
/// What the last sealed index group measured, for the next one to size itself by: distinct
/// (property, value) terms and distinct trigrams. One instance is shared by every builder the
/// sink factory creates — flushes on several threads and merges alike — and the numbers are
/// plain hints: a stale or racing read costs a rehash or an over-sized rental, never
/// correctness. Zero means nothing measured yet and the builders use their own floors.
/// </summary>
public sealed class IndexBuildHints
{
    private int _terms;
    private int _trigrams;

    public int LastTerms    => Volatile.Read(ref _terms);
    public int LastTrigrams => Volatile.Read(ref _trigrams);

    public void Record(int terms, int trigrams)
    {
        Volatile.Write(ref _terms,    terms);
        Volatile.Write(ref _trigrams, trigrams);
    }
}
