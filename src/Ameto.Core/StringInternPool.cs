using System.Collections.Concurrent;

namespace Ameto.Core;

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
///
/// <para>The cap is per instance. <see cref="Shared"/> keeps the 65 536 the logs tier has
/// always had; a caller interning a different population — metric label keys and values —
/// builds its own pool with its own bound, so a high-cardinality label cannot saturate the
/// pool log templates are indexed in (every event past that point would carry its own
/// template string, permanently).</para>
/// </summary>
public sealed class StringInternPool
{
    /// <summary>The cap <see cref="Shared"/> and the parameterless constructor use.</summary>
    public const int DefaultMaxPoolSize = 65536;
    private const int InitialSlots = 1024;

    private readonly int                               _maxPoolSize;
    private readonly int                               _initialSlots;
    private readonly ConcurrentDictionary<string, int> _stringToIndex = new(StringComparer.Ordinal);
    private volatile string?[]                         _indexToString;
    private readonly Lock                              _slotLock       = new();
    private          int                               _nextIndex      = 0;

    public static readonly StringInternPool Shared = new();

    public StringInternPool() : this(DefaultMaxPoolSize) { }

    /// <param name="maxPoolSize">How many distinct strings this pool will ever hold. Past it
    /// every miss is answered with a fresh, unpooled string (index -1) and
    /// <see cref="PoolExhausted"/> fires once.</param>
    public StringInternPool(int maxPoolSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxPoolSize, 1);
        _maxPoolSize   = maxPoolSize;
        _initialSlots  = Math.Min(InitialSlots, maxPoolSize);
        _indexToString = new string?[_initialSlots];
    }

    /// <summary>The most distinct strings this pool holds before it stops pooling.</summary>
    public int MaxPoolSize => _maxPoolSize;

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
    /// <c>TryAdd</c> publishes the key — and therefore the index — BEFORE
    /// <see cref="SetSlot"/> stores the index→string slot, so a thread that loses the race
    /// and reads the winner's index in that window would see <see cref="Get"/> return
    /// <see cref="string.Empty"/> (the slot is still null). The caller
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

        // Fast reject once saturated, so a saturated pool's misses do not keep incrementing
        // the counter (a stream of new templates could carry it round to negative ids).
        if (_nextIndex >= _maxPoolSize)
            return Exhausted();

        // The cap is checked AGAIN on the index actually claimed: two threads missing
        // together at 65535 both pass the check above and one of them claims 65536, an id
        // beyond every slot the pool can hold. The counter overshoots the cap by at most
        // the number of threads racing at the boundary; each overshooter answers -1 and
        // never touches the map.
        int newIdx = System.Threading.Interlocked.Increment(ref _nextIndex) - 1;
        if (newIdx >= _maxPoolSize)
            return Exhausted();

        if (_stringToIndex.TryAdd(template, newIdx))
        {
            SetSlot(newIdx, template);
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

    /// <summary><c>carriedFrom</c> of <see cref="InternOrCarry(ReadOnlySpan{byte}, out string, StringInternPool, out int)"/>: the text was already here.</summary>
    public const int FoundHere = -1;

    /// <summary><c>carriedFrom</c> of <see cref="InternOrCarry(ReadOnlySpan{byte}, out string, StringInternPool, out int)"/>: the text was in neither pool.</summary>
    public const int FoundNowhere = -2;

    /// <summary>
    /// <see cref="Intern(ReadOnlySpan{byte}, out string)"/> for a pool that REPLACED <paramref name="carryFrom"/>
    /// (the metric label pool's reset): on a miss here, <paramref name="carryFrom"/>'s instance of the text —
    /// when it holds one — is claimed instead of a new string, so a caller still holding that instance keeps
    /// matching it by reference. <paramref name="carriedFrom"/> is the text's index in
    /// <paramref name="carryFrom"/> when it was carried, else <see cref="FoundHere"/> or
    /// <see cref="FoundNowhere"/>.
    ///
    /// <para>A copy of <see cref="Intern(ReadOnlySpan{byte}, out string)"/>'s body, not a layer over it: the
    /// hit — the path every call takes but the first — must cost what that one costs, and that one is the
    /// log ingest path's, which must not gain a parameter or a call to pay for this.</para>
    /// </summary>
    public int InternOrCarry(ReadOnlySpan<byte> utf8, out string canonical, StringInternPool carryFrom, out int carriedFrom)
    {
        carriedFrom = FoundHere;
        if (utf8.IsEmpty) { canonical = string.Empty; return -1; }

        int charCount = System.Text.Encoding.UTF8.GetCharCount(utf8);
        char[]? rented = charCount > 512 ? System.Buffers.ArrayPool<char>.Shared.Rent(charCount) : null;
        Span<char> chars = rented ?? stackalloc char[charCount];
        System.Text.Encoding.UTF8.GetChars(utf8, chars);
        var key = chars[..charCount];
        try
        {
            var lookup = _stringToIndex.GetAlternateLookup<ReadOnlySpan<char>>();
            if (lookup.TryGetValue(key, out string? existing, out int idx))
            {
                canonical = existing;
                return idx;
            }

            if (carryFrom.TryGet(key, out string? carried, out int oldIdx))
            {
                carriedFrom = oldIdx;
                return Claim(carried, out canonical);
            }
            carriedFrom = FoundNowhere;
            return Claim(new string(key), out canonical);
        }
        finally
        {
            if (rented is not null) System.Buffers.ArrayPool<char>.Shared.Return(rented);
        }
    }

    /// <summary>The string overload of <see cref="InternOrCarry(ReadOnlySpan{byte}, out string, StringInternPool, out int)"/>.</summary>
    public int InternOrCarry(string template, out string canonical, StringInternPool carryFrom, out int carriedFrom)
    {
        carriedFrom = FoundHere;
        var lookup = _stringToIndex.GetAlternateLookup<ReadOnlySpan<char>>();
        if (lookup.TryGetValue(template.AsSpan(), out string? existing, out int idx))
        {
            canonical = existing;
            return idx;
        }

        if (carryFrom.TryGet(template.AsSpan(), out string? carried, out int oldIdx))
        {
            carriedFrom = oldIdx;
            return Claim(carried, out canonical);
        }
        carriedFrom = FoundNowhere;
        return Claim(template, out canonical);
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

    /// <summary>
    /// LOOKUP ONLY: the pool's instance of <paramref name="chars"/> and its index when the pool
    /// already holds that text, else false. Nothing is claimed, added or signalled on a miss —
    /// for a reader of text that may not be live (a cold file, where every dead value ever
    /// written would otherwise take a slot this never-evicting pool keeps for the life of the
    /// process). Allocation-free either way.
    /// </summary>
    public bool TryGet(ReadOnlySpan<char> chars, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out string? canonical, out int index)
    {
        var lookup = _stringToIndex.GetAlternateLookup<ReadOnlySpan<char>>();
        if (lookup.TryGetValue(chars, out string? existing, out index))
        {
            canonical = existing;
            return true;
        }
        canonical = null;
        index     = -1;
        return false;
    }

    /// <summary>
    /// As <see cref="TryGet(ReadOnlySpan{char}, out string?, out int)"/>, for UTF-8 — decoded
    /// exactly as <see cref="Intern(ReadOnlySpan{byte}, out string)"/> decodes it. Empty input
    /// is not in the pool (<see cref="Intern(ReadOnlySpan{byte}, out string)"/> never pools it).
    /// </summary>
    public bool TryGet(ReadOnlySpan<byte> utf8, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out string? canonical, out int index)
    {
        if (utf8.IsEmpty) { canonical = null; index = -1; return false; }

        int charCount = System.Text.Encoding.UTF8.GetCharCount(utf8);
        char[]? rented = charCount > 512 ? System.Buffers.ArrayPool<char>.Shared.Rent(charCount) : null;
        Span<char> chars = rented ?? stackalloc char[charCount];
        System.Text.Encoding.UTF8.GetChars(utf8, chars);
        try { return TryGet(chars[..charCount], out canonical, out index); }
        finally
        {
            if (rented is not null) System.Buffers.ArrayPool<char>.Shared.Return(rented);
        }
    }

    /// <summary>Indices claimed so far, capped at <see cref="MaxPoolSize"/> — how full the pool is.</summary>
    public int ClaimedCount => Math.Min(Volatile.Read(ref _nextIndex), _maxPoolSize);

    public string Get(int index)
    {
        var slots = _indexToString;            // one volatile read; index the copy taken
        return (uint)index < (uint)slots.Length ? slots[index] ?? string.Empty : string.Empty;
    }

    /// <summary>
    /// Every miss on a full pool comes here, for the life of the process — and a pool that
    /// never evicts stays full once it gets there (the metric label pool does, on a cluster whose
    /// pods and containers churn their ids into it). The exchange that claims the one
    /// <see cref="PoolExhausted"/> signal is a WRITE, even when it writes the 1 already there: it
    /// takes the cache line exclusive, and the line is the one every other ingest thread reads
    /// <c>_nextIndex</c> and the dictionary reference from on its own way through. So it is
    /// asked only while the answer can still be "first": a plain read once signalled.
    /// </summary>
    private int Exhausted()
    {
        if (Volatile.Read(ref _exhaustedSignalled) == 0
            && Interlocked.Exchange(ref _exhaustedSignalled, 1) == 0)
            PoolExhausted?.Invoke(_maxPoolSize);
        return -1; // pool full — caller stores -1, template resolved differently
    }

    /// <summary>
    /// Stores <paramref name="template"/> at <paramref name="index"/>, growing the array as
    /// needed. Under the lock so that a growth (copy old → new, then publish new) cannot
    /// race a store into the old array and drop it.
    /// </summary>
    private void SetSlot(int index, string template)
    {
        // The growth loop below is bounded by the cap; an index at or past it would
        // spin it for ever — under the lock, with ingest behind it. No such id is ever
        // handed out (Claim re-checks the cap on the claimed index), so this is a guard,
        // not a path.
        if ((uint)index >= (uint)_maxPoolSize) return;

        lock (_slotLock)
        {
            var slots = _indexToString;
            if (index >= slots.Length)
            {
                int newLen = slots.Length;
                while (newLen <= index) newLen = Math.Min(newLen * 2, _maxPoolSize);
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
        // The array is bounded by the cap; an id beyond it was never handed out by this
        // pool (Intern stops at the cap), so only the reverse map is kept for it.
        _stringToIndex[template] = index;
        SetSlot(index, template);   // guarded inside for an id beyond the cap
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
        lock (_slotLock) _indexToString = new string?[_initialSlots];
        System.Threading.Interlocked.Exchange(ref _nextIndex, 0);
        System.Threading.Interlocked.Exchange(ref _exhaustedSignalled, 0);
    }
}
