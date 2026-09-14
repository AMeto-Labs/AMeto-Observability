using System.Collections.Concurrent;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// Interns message template strings to avoid storing the same string for every event
/// in the hot-tier. The index (int) is stored in LogEventHeader.MessageTemplatePoolIndex.
///
/// Thread-safe. Lock-free for reads: string → index is a ConcurrentDictionary; index →
/// string is a plain array indexed by the id — a bounds check and a load, where the
/// dictionary lookup cost a hash, a bucket walk and a compare per resolved event (twice
/// per materialised hot event, once per header the scan's service memo misses). The
/// array grows under a lock and every slot store happens under that same lock, so a
/// growth can never lose a store; readers take the reference once and index it, and a
/// slot holds either its string or null (not yet interned in that copy).
/// Maximum pool size is capped to prevent unbounded growth (eviction is not implemented —
/// templates are typically low-cardinality).
/// </summary>
public sealed class StringInternPool
{
    private const int MaxPoolSize  = 65536;
    private const int InitialSlots = 1024;

    private readonly ConcurrentDictionary<string, int> _stringToIndex = new(StringComparer.Ordinal);
    private volatile string?[]                         _indexToString  = new string?[InitialSlots];
    private readonly Lock                              _slotLock       = new();
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

    public int Intern(string template)
    {
        if (_stringToIndex.TryGetValue(template, out int idx))
            return idx;

        if (_nextIndex >= MaxPoolSize)
        {
            if (Interlocked.Exchange(ref _exhaustedSignalled, 1) == 0)
                PoolExhausted?.Invoke(MaxPoolSize);
            return -1; // pool full — caller stores -1, template resolved differently
        }

        int newIdx = System.Threading.Interlocked.Increment(ref _nextIndex) - 1;

        // Another thread may have beaten us; accept their index
        if (_stringToIndex.TryAdd(template, newIdx))
        {
            SetSlot(newIdx, template);
            return newIdx;
        }

        return _stringToIndex[template];
    }

    /// <summary>
    /// Interns a UTF-8 template/name without allocating a <see cref="string"/> on a cache
    /// hit (the common case — templates/service names are low-cardinality and repeat). A
    /// string is materialised only the first time a value is seen. Powers the zero-alloc
    /// OTLP streaming ingest path. Returns -1 for empty input or when the pool is full.
    /// </summary>
    public int Intern(ReadOnlySpan<byte> utf8)
    {
        if (utf8.IsEmpty) return -1;

        // Decode UTF-8 → chars for the ordinal lookup. Short by nature ⇒ stack; pool the rare long one.
        int charCount = System.Text.Encoding.UTF8.GetCharCount(utf8);
        char[]? rented = charCount > 512 ? System.Buffers.ArrayPool<char>.Shared.Rent(charCount) : null;
        Span<char> chars = rented ?? stackalloc char[charCount];
        System.Text.Encoding.UTF8.GetChars(utf8, chars);
        var key = chars[..charCount];
        try
        {
            // Alternate lookup matches an existing key by span — no string allocation on hit.
            var lookup = _stringToIndex.GetAlternateLookup<ReadOnlySpan<char>>();
            if (lookup.TryGetValue(key, out int idx))
                return idx;

            return Intern(new string(key)); // miss: materialise once, intern via the string path
        }
        finally
        {
            if (rented is not null) System.Buffers.ArrayPool<char>.Shared.Return(rented);
        }
    }

    public string Get(int index)
    {
        var slots = _indexToString;            // one volatile read; index the copy taken
        return (uint)index < (uint)slots.Length ? slots[index] ?? string.Empty : string.Empty;
    }

    /// <summary>
    /// Stores <paramref name="template"/> at <paramref name="index"/>, growing the array as
    /// needed. Under the lock so that a growth (copy old → new, then publish new) cannot
    /// race a store into the old array and drop it.
    /// </summary>
    private void SetSlot(int index, string template)
    {
        lock (_slotLock)
        {
            var slots = _indexToString;
            if (index >= slots.Length)
            {
                int newLen = slots.Length;
                while (newLen <= index) newLen = Math.Min(newLen * 2, MaxPoolSize);
                var bigger = new string?[newLen];
                slots.AsSpan().CopyTo(bigger);
                bigger[index]  = template;
                _indexToString = bigger;       // volatile store: contents above are visible first
            }
            else
            {
                slots[index] = template;
            }
        }
    }

    /// <summary>Restores a known index→template mapping during WAL recovery.</summary>
    public void ForceIntern(int index, string template)
    {
        // The array is bounded by MaxPoolSize; an id beyond it was never handed out by this
        // pool (Intern stops at the cap), so only the reverse map is kept for it.
        _stringToIndex[template] = index;
        if ((uint)index < MaxPoolSize) SetSlot(index, template);
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
        lock (_slotLock) _indexToString = new string?[InitialSlots];
        System.Threading.Interlocked.Exchange(ref _nextIndex, 0);
        System.Threading.Interlocked.Exchange(ref _exhaustedSignalled, 0);
    }
}
