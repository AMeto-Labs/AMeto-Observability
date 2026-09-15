using System.Collections.Concurrent;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// Interns message template strings to avoid storing the same string for every event
/// in the hot-tier. The index (int) is stored in LogEventHeader.MessageTemplatePoolIndex.
///
/// Thread-safe. Lock-free for reads; uses a ConcurrentDictionary.
/// Maximum pool size is capped to prevent unbounded growth (eviction is not implemented —
/// templates are typically low-cardinality).
/// </summary>
public sealed class StringInternPool
{
    private const int MaxPoolSize = 65536;

    private readonly ConcurrentDictionary<string, int> _stringToIndex = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<int, string> _indexToString = new();
    private          int                               _nextIndex      = 0;

    public static readonly StringInternPool Shared = new();

    /// <summary>
    /// Raised once, the first time the pool saturates. Past that point every event carries
    /// its own template string in the hot tier instead of a 2-byte pool index, so memory per
    /// event rises permanently and never recovers (there is no eviction). That used to happen
    /// in complete silence; the storage layer subscribes so it reaches the operator.
    /// </summary>
    public event Action<int>? PoolExhausted;

    private int _exhaustedSignalled;

    public int Intern(string template) => Intern(template, out _);

    /// <summary>
    /// The one place an index is claimed. Everything else routes through it, so the
    /// index→string publication order is reasoned about once.
    ///
    /// <para><paramref name="canonical"/> is NEVER resolved through <see cref="Get"/>.
    /// <c>TryAdd</c> publishes the key — and therefore the index — BEFORE the index→string
    /// map is written, so a thread that loses the race and reads the winner's index in that
    /// window would see <see cref="Get"/> return <see cref="string.Empty"/>. The caller
    /// stores that empty string as the event's template, the hot tier prefers an attached
    /// string over the pool (a <c>??</c> does not catch <c>""</c>), and the event is served
    /// with no template at all while the WAL pool row is skipped as empty. Measured at 4 875
    /// events out of 8 threads × 64 fresh names × 200 rounds.</para>
    ///
    /// <para>So: the winner returns the very instance it stored, and a loser re-probes the
    /// dictionary, whose KEY is the winner's instance and is present by the time
    /// <c>TryAdd</c> failed.</para>
    /// </summary>
    private int Claim(string template, out string canonical)
    {
        canonical = template;

        if (_nextIndex >= MaxPoolSize)
        {
            if (Interlocked.Exchange(ref _exhaustedSignalled, 1) == 0)
                PoolExhausted?.Invoke(MaxPoolSize);
            return -1; // pool full — caller stores -1, template resolved differently
        }

        int newIdx = System.Threading.Interlocked.Increment(ref _nextIndex) - 1;

        if (_stringToIndex.TryAdd(template, newIdx))
        {
            _indexToString[newIdx] = template;
            return newIdx;          // canonical is `template` — the instance just stored
        }

        // Another thread beat us: accept their index AND their instance, from the one
        // structure that is guaranteed to hold both.
        var lookup = _stringToIndex.GetAlternateLookup<ReadOnlySpan<char>>();
        if (lookup.TryGetValue(template.AsSpan(), out string? winner, out int winnerIdx))
        {
            canonical = winner;
            return winnerIdx;
        }

        // Only reachable if Clear() ran between the failed TryAdd and this probe. The old
        // code indexed the dictionary here and would have thrown KeyNotFoundException;
        // answering "not pooled" is the same outcome the caller already handles.
        return -1;
    }

    /// <summary>
    /// Interns a UTF-8 template/name without allocating a <see cref="string"/> on a cache
    /// hit (the common case — templates/service names are low-cardinality and repeat). A
    /// string is materialised only the first time a value is seen. Powers the zero-alloc
    /// OTLP streaming ingest path. Returns -1 for empty input or when the pool is full.
    /// </summary>
    public int Intern(ReadOnlySpan<byte> utf8) => Intern(utf8, out _);

    /// <summary>
    /// As <see cref="Intern(ReadOnlySpan{byte})"/>, but also hands back the pool's OWN
    /// instance of the string (<paramref name="canonical"/>) — the same object
    /// <see cref="Get"/> would return for the returned index.
    ///
    /// <para>Folding the <c>Get</c> into the lookup pays twice: it saves the second
    /// dictionary probe per event, and it lets a caller store the shared instance instead
    /// of a fresh per-event duplicate. A tier that keeps one duplicate template string per
    /// event retains ~100 B/event for the life of the tier, all of it gen2 garbage at
    /// flush.</para>
    ///
    /// <para>When the pool is full (index -1) the value is still materialised once and
    /// returned, since there is no pooled instance to share.</para>
    /// </summary>
    public int Intern(ReadOnlySpan<byte> utf8, out string canonical)
    {
        if (utf8.IsEmpty) { canonical = string.Empty; return -1; }

        // Decode UTF-8 → chars for the ordinal lookup. Short by nature ⇒ stack; pool the rare long one.
        int charCount = System.Text.Encoding.UTF8.GetCharCount(utf8);
        char[]? rented = charCount > 512 ? System.Buffers.ArrayPool<char>.Shared.Rent(charCount) : null;
        Span<char> chars = rented ?? stackalloc char[charCount];
        System.Text.Encoding.UTF8.GetChars(utf8, chars);
        var key = chars[..charCount];
        try
        {
            // Alternate lookup matches an existing key by span — no string allocation on hit,
            // and the STORED key is the canonical instance, so no follow-up Get is needed.
            var lookup = _stringToIndex.GetAlternateLookup<ReadOnlySpan<char>>();
            if (lookup.TryGetValue(key, out string? existing, out int idx))
            {
                canonical = existing;
                return idx;
            }

            // Miss: materialise once and claim. Claim answers with the pooled instance —
            // its own on a win, the winner's on a loss — never through Get().
            return Claim(new string(key), out canonical);
        }
        finally
        {
            if (rented is not null) System.Buffers.ArrayPool<char>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// String overload of <see cref="Intern(ReadOnlySpan{byte}, out string)"/>: interns
    /// <paramref name="template"/> and hands back the pool's own instance, so a caller that
    /// keeps the string alive (the hot tier does, for the life of the tier) keeps the shared
    /// one rather than a per-event duplicate. Falls back to the argument itself when the
    /// pool is full.
    /// </summary>
    public int Intern(string template, out string canonical)
    {
        var lookup = _stringToIndex.GetAlternateLookup<ReadOnlySpan<char>>();
        if (lookup.TryGetValue(template.AsSpan(), out string? existing, out int idx))
        {
            canonical = existing;
            return idx;
        }

        return Claim(template, out canonical);
    }

    public string Get(int index)
    {
        if (index < 0) return string.Empty;
        return _indexToString.TryGetValue(index, out var s) ? s : string.Empty;
    }

    /// <summary>Restores a known index→template mapping during WAL recovery.</summary>
    public void ForceIntern(int index, string template)
    {
        _stringToIndex[template] = index;
        _indexToString[index]    = template;
        int expected = _nextIndex;
        while (index + 1 > expected)
        {
            int prev = Interlocked.CompareExchange(ref _nextIndex, index + 1, expected);
            if (prev == expected) break;
            expected = prev;
        }
    }

    public void Clear()
    {
        _stringToIndex.Clear();
        _indexToString.Clear();
        System.Threading.Interlocked.Exchange(ref _nextIndex, 0);
        System.Threading.Interlocked.Exchange(ref _exhaustedSignalled, 0);
    }
}
