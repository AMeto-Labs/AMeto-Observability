using System.Text;

namespace Ameto.Tracing.Storage;

/// <summary>
/// Per-block bloom filter over span attribute keys and string values, used by the
/// TraceQL executor to skip blocks that cannot match an attribute predicate.
///
/// <para>Entries inserted per span attribute:</para>
/// <list type="bullet">
///   <item><c>key</c> (ordinal bytes) — key presence; a valid necessary condition
///     for EVERY attribute operator, since a span without the key never matches.</item>
///   <item><c>key 0x1F lowercase(value.ToString())</c> — equality probes. Values are
///     lowercased because TraceQL string comparison is OrdinalIgnoreCase, and
///     non-string values use the same <c>ToString()</c> the evaluator compares with.</item>
/// </list>
///
/// <para>k = 3 probes via double hashing over an FNV-1a 64 hash; the bit length is
/// a power of two chosen at ~12 bits/entry (min 512, cap 32768 bits = 4 KB).</para>
/// </summary>
internal static class SpanBloom
{
    private const int BitsPerEntry = 12;
    private const int MinBits      = 512;
    private const int MaxBits      = 32_768;
    private const int Probes       = 3;

    /// <summary>Collects the two hash entries for one attribute into <paramref name="hashes"/>.</summary>
    public static void AddAttr(HashSet<ulong> hashes, string key, object? value)
    {
        hashes.Add(HashKey(key));
        if (value is not null)
            hashes.Add(HashKeyValue(key, value.ToString() ?? string.Empty));
    }

    /// <summary>Builds the bitset from collected entry hashes.</summary>
    public static byte[] Build(HashSet<ulong> hashes)
    {
        if (hashes.Count == 0) return [];
        int bits = (int)System.Numerics.BitOperations.RoundUpToPowerOf2(
            (uint)Math.Clamp(hashes.Count * BitsPerEntry, MinBits, MaxBits));
        var bitset = new byte[bits / 8];
        foreach (var h in hashes) Insert(bitset, h);
        return bitset;
    }

    public static ulong HashKey(string key) => Fnv1a64(key, suffix: null);

    public static ulong HashKeyValue(string key, string value) => Fnv1a64(key, value);

    /// <summary>May-contain test; an empty bitset never rejects (unknown blooms are permissive).</summary>
    public static bool MayContain(ReadOnlySpan<byte> bitset, ulong hash)
    {
        if (bitset.IsEmpty) return true;
        int bits = bitset.Length * 8; // power of two
        ulong h1 = hash, h2 = (hash >> 33) | (hash << 31) | 1;
        for (int i = 0; i < Probes; i++)
        {
            int bit = (int)((h1 + (ulong)i * h2) & (ulong)(bits - 1));
            if ((bitset[bit >> 3] & (1 << (bit & 7))) == 0) return false;
        }
        return true;
    }

    private static void Insert(byte[] bitset, ulong hash)
    {
        int bits = bitset.Length * 8;
        ulong h1 = hash, h2 = (hash >> 33) | (hash << 31) | 1;
        for (int i = 0; i < Probes; i++)
        {
            int bit = (int)((h1 + (ulong)i * h2) & (ulong)(bits - 1));
            bitset[bit >> 3] |= (byte)(1 << (bit & 7));
        }
    }

    /// <summary>
    /// FNV-1a 64 over UTF-8 of the key, optionally followed by 0x1F and the
    /// LOWERCASED value (TraceQL string equality is case-insensitive).
    /// </summary>
    private static ulong Fnv1a64(string key, string? suffix)
    {
        const ulong Offset = 14695981039346656037UL;
        const ulong Prime  = 1099511628211UL;

        ulong h = Offset;
        h = HashUtf8(h, key, lower: false);
        if (suffix is not null)
        {
            h = (h ^ 0x1F) * Prime;
            h = HashUtf8(h, suffix, lower: true);
        }
        return h;

        static ulong HashUtf8(ulong h, string s, bool lower)
        {
            Span<byte> buf = stackalloc byte[128];
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (lower) c = char.ToLowerInvariant(c);
                if (c < 0x80)
                {
                    h = (h ^ (byte)c) * Prime;
                }
                else
                {
                    // Rare non-ASCII path — encode the single char.
                    int n = Encoding.UTF8.GetBytes(stackalloc char[] { c }, buf);
                    for (int j = 0; j < n; j++) h = (h ^ buf[j]) * Prime;
                }
            }
            return h;
        }
    }
}
