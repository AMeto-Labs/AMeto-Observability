using System.Buffers.Binary;

namespace Ameto.Core;

/// <summary>
/// Incremental CRC32C (Castagnoli). Hardware-accelerated where SSE4.2 / ARM CRC is
/// present; table fallback otherwise. Chosen over a NuGet hashing package to keep the
/// storage core dependency-free, and over FNV because torn mmap write-back produces
/// exactly the structured corruption weak hashes miss.
///
/// <para>IN Ameto.Core BECAUSE IT HAS TWO CALLERS NOW. It was written for the log WAL and lived
/// inside its file; the trace manifest needs the same guarantee over the same class of damage,
/// and a second copy is the shape <see cref="FileBounds"/> exists to argue against — the last one
/// diverged within a single review round. One implementation, two storage engines.</para>
/// </summary>
public static class Crc32c
{
    private static readonly uint[] Table = BuildTable();

    private static uint[] BuildTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint c = i;
            for (int k = 0; k < 8; k++)
                c = (c & 1) != 0 ? 0x82F63B78u ^ (c >> 1) : c >> 1;
            table[i] = c;
        }
        return table;
    }

    /// <summary>Extends a finalized CRC with more data (pass 0 to start).</summary>
    public static uint Append(uint crc, ReadOnlySpan<byte> data)
    {
        uint c = ~crc;
        if (System.Runtime.Intrinsics.X86.Sse42.IsSupported)
        {
            if (System.Runtime.Intrinsics.X86.Sse42.X64.IsSupported)
            {
                while (data.Length >= 8)
                {
                    c = (uint)System.Runtime.Intrinsics.X86.Sse42.X64.Crc32(c, BinaryPrimitives.ReadUInt64LittleEndian(data));
                    data = data[8..];
                }
            }
            for (int i = 0; i < data.Length; i++)
                c = System.Runtime.Intrinsics.X86.Sse42.Crc32(c, data[i]);
        }
        else if (System.Runtime.Intrinsics.Arm.Crc32.IsSupported)
        {
            if (System.Runtime.Intrinsics.Arm.Crc32.Arm64.IsSupported)
            {
                while (data.Length >= 8)
                {
                    c = System.Runtime.Intrinsics.Arm.Crc32.Arm64.ComputeCrc32C(c, BinaryPrimitives.ReadUInt64LittleEndian(data));
                    data = data[8..];
                }
            }
            for (int i = 0; i < data.Length; i++)
                c = System.Runtime.Intrinsics.Arm.Crc32.ComputeCrc32C(c, data[i]);
        }
        else
        {
            for (int i = 0; i < data.Length; i++)
                c = Table[(byte)(c ^ data[i])] ^ (c >> 8);
        }
        return ~c;
    }
}
