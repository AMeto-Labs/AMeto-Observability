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

    public int Intern(string template)
    {
        if (_stringToIndex.TryGetValue(template, out int idx))
            return idx;

        if (_nextIndex >= MaxPoolSize)
            return -1; // pool full — caller stores -1, template resolved differently

        int newIdx = System.Threading.Interlocked.Increment(ref _nextIndex) - 1;

        // Another thread may have beaten us; accept their index
        if (_stringToIndex.TryAdd(template, newIdx))
        {
            _indexToString[newIdx] = template;
            return newIdx;
        }

        return _stringToIndex[template];
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
    }
}
