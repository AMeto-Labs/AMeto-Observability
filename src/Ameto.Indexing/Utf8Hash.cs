using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Ameto.Indexing;

/// <summary>
/// Hash of a UTF-8 term for the build-phase tables. Eight bytes a round with a multiply-xorshift
/// mix — terms are 2-40 bytes, so this is a handful of instructions and no call. Not
/// cryptographic and not stable across builds by contract: it never touches disk.
/// </summary>
internal static class Utf8Hash
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Compute(ReadOnlySpan<byte> s)
    {
        ulong h = 0x9E3779B97F4A7C15UL ^ (ulong)s.Length;
        while (s.Length >= 8)
        {
            h  = (h ^ BinaryPrimitives.ReadUInt64LittleEndian(s)) * 0xBF58476D1CE4E5B9UL;
            h ^= h >> 29;
            s  = s[8..];
        }
        if (s.Length >= 4)
        {
            h  = (h ^ BinaryPrimitives.ReadUInt32LittleEndian(s)) * 0xBF58476D1CE4E5B9UL;
            h ^= h >> 29;
            s  = s[4..];
        }
        for (int i = 0; i < s.Length; i++)
            h = (h ^ s[i]) * 0x100000001B3UL;
        h ^= h >> 32;
        h *= 0x94D049BB133111EBUL;
        h ^= h >> 29;
        return (uint)h;
    }
}
