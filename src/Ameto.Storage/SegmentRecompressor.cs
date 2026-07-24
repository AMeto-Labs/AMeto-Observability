using System.Buffers;
using System.Buffers.Binary;
using K4os.Compression.LZ4;
using Microsoft.Extensions.Logging;

namespace Ameto.Storage;

/// <summary>
/// One-shot background re-compression of a cold <c>.seg</c> file from the fast
/// LZ4 level the latency-sensitive flush path uses to LZ4-HC (~20-30 % smaller).
///
/// <para>The transform is purely mechanical and provably lossless:</para>
/// <list type="number">
///   <item>each data block is decoded and re-encoded with HC; the new bytes are
///     decoded again and byte-compared against the original payload — any
///     mismatch aborts the whole rewrite;</item>
///   <item>the inverted/trigram/bloom index sections are copied verbatim (their
///     posting lists reference event ordinals, which do not change);</item>
///   <item>the block index is rewritten with remapped block offsets;</item>
///   <item>the header is preserved apart from a "recompressed" flag bit, and the
///     result atomically replaces the original via <see cref="File.Replace(string,string,string?)"/>
///     (retried — concurrent readers hold short-lived shared handles).</item>
/// </list>
/// A block whose HC encoding is not smaller keeps its original bytes, so a
/// rewritten file can never grow.
/// </summary>
internal static class SegmentRecompressor
{
    private const uint   Magic       = 0x52_44_4C_47; // "RDLG"
    private const uint   FooterMagic = 0x52_44_46_54; // "RDFT"
    private const ushort SegVersion  = 5;
    private const int    HeaderSize  = 46;
    private const int    FooterSize  = 44;            // 5 × int64 + magic
    private const int    FlagsOffset = 39;            // Magic4 Ver2 Node4 Seg8 Min8 Max8 Cnt4 Lvl1 → Flags

    /// <summary>Header flag: this segment has already been re-compressed with HC.</summary>
    public const byte FlagRecompressed = 0x02;

    /// <summary>True when the file is a v5 segment not yet carrying the HC flag.</summary>
    public static bool IsCandidate(string segPath)
    {
        try
        {
            using var fs = new FileStream(segPath, FileMode.Open, FileAccess.Read, FileShare.Read, 256);
            Span<byte> hdr = stackalloc byte[HeaderSize];
            if (fs.Read(hdr) != HeaderSize) return false;
            if (BinaryPrimitives.ReadUInt32LittleEndian(hdr) != Magic) return false;
            if (BinaryPrimitives.ReadUInt16LittleEndian(hdr[4..]) != SegVersion) return false;
            return (hdr[FlagsOffset] & FlagRecompressed) == 0;
        }
        catch { return false; }
    }

    /// <summary>
    /// Re-compresses <paramref name="segPath"/> in place. Returns the bytes saved,
    /// or null when the file was skipped (not a candidate, in use, or verification
    /// failed — the original is never touched in those cases).
    /// </summary>
    public static long? Recompress(string segPath, ILogger logger, CancellationToken ct)
    {
        string tmpPath = segPath + ".hctmp";
        try
        {
            long saved = Transform(segPath, tmpPath, ct);
            if (saved < 0) { TryDelete(tmpPath); return null; }

            // Atomic swap; readers hold short-lived shared handles, so retry briefly.
            for (int attempt = 0; ; attempt++)
            {
                try
                {
                    File.Replace(tmpPath, segPath, destinationBackupFileName: null);
                    return saved;
                }
                catch (IOException) when (attempt < 5)
                {
                    Thread.Sleep(200 * (attempt + 1));
                }
            }
        }
        catch (OperationCanceledException) { TryDelete(tmpPath); throw; }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Segment recompression skipped for {File}", Path.GetFileName(segPath));
            TryDelete(tmpPath);
            return null;
        }
    }

    /// <summary>Returns bytes saved, or -1 when the segment should be left as is.</summary>
    private static long Transform(string srcPath, string tmpPath, CancellationToken ct)
    {
        using var src = new FileStream(srcPath, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 16, FileOptions.SequentialScan);
        using var br  = new BinaryReader(src);

        // ── Header ────────────────────────────────────────────────────────────
        var header = br.ReadBytes(HeaderSize);
        if (header.Length != HeaderSize) return -1;
        if (BinaryPrimitives.ReadUInt32LittleEndian(header) != Magic) return -1;
        if (BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(4)) != SegVersion) return -1;
        if ((header[FlagsOffset] & FlagRecompressed) != 0) return -1;

        // ── Footer ────────────────────────────────────────────────────────────
        src.Seek(-FooterSize, SeekOrigin.End);
        long invOff      = br.ReadInt64();
        long triOff      = br.ReadInt64();
        long bloomOff    = br.ReadInt64();
        long blockIdxOff = br.ReadInt64();
        br.ReadInt64(); // footerOffset (recomputed)
        if (br.ReadUInt32() != FooterMagic) return -1;
        if (invOff <= 0 || blockIdxOff <= invOff) return -1;

        using var dst = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None, 1 << 16);
        using var bw  = new BinaryWriter(dst);

        header[FlagsOffset] |= FlagRecompressed;
        bw.Write(header);
        // The writer seeks to HeaderSize before the first block — mirror that.
        dst.Seek(HeaderSize, SeekOrigin.Begin);

        // ── Blocks: decode → HC re-encode → verify → write ────────────────────
        var offsetMap = new Dictionary<long, long>();
        src.Seek(HeaderSize, SeekOrigin.Begin);
        while (src.Position < invOff)
        {
            ct.ThrowIfCancellationRequested();

            long oldOffset = src.Position;
            int uncompLen  = (int)br.ReadUInt32();
            int compLen    = (int)br.ReadUInt32();
            if (uncompLen <= 0 || compLen <= 0 || uncompLen > 64 * 1024 * 1024) return -1;
            var compBytes  = br.ReadBytes(compLen);

            byte[] raw    = ArrayPool<byte>.Shared.Rent(uncompLen);
            byte[] recomp = ArrayPool<byte>.Shared.Rent(LZ4Codec.MaximumOutputSize(uncompLen));
            byte[] check  = ArrayPool<byte>.Shared.Rent(uncompLen);
            try
            {
                if (LZ4Codec.Decode(compBytes, 0, compLen, raw, 0, uncompLen) != uncompLen) return -1;

                int newLen = LZ4Codec.Encode(raw, 0, uncompLen, recomp, 0, recomp.Length, LZ4Level.L09_HC);

                offsetMap[oldOffset] = dst.Position;
                if (newLen > 0 && newLen < compLen)
                {
                    // Paranoia before we commit: the new bytes must decode to the
                    // exact original payload.
                    if (LZ4Codec.Decode(recomp, 0, newLen, check, 0, uncompLen) != uncompLen ||
                        !raw.AsSpan(0, uncompLen).SequenceEqual(check.AsSpan(0, uncompLen)))
                        return -1;

                    bw.Write((uint)uncompLen);
                    bw.Write((uint)newLen);
                    bw.Write(recomp, 0, newLen);
                }
                else
                {
                    bw.Write((uint)uncompLen);
                    bw.Write((uint)compLen);
                    bw.Write(compBytes);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(raw);
                ArrayPool<byte>.Shared.Return(recomp);
                ArrayPool<byte>.Shared.Return(check);
            }
        }

        // ── Index sections (inverted + trigram + bloom): verbatim copy ────────
        long newInvOff   = dst.Position;
        long sectionLen  = blockIdxOff - invOff;
        src.Seek(invOff, SeekOrigin.Begin);
        CopyExactly(src, dst, sectionLen);
        long newTriOff   = newInvOff + (triOff   - invOff);
        long newBloomOff = newInvOff + (bloomOff - invOff);

        // ── Block index: remap offsets ────────────────────────────────────────
        long newBlockIdxOff = dst.Position;
        src.Seek(blockIdxOff, SeekOrigin.Begin);
        uint blockCount = br.ReadUInt32();
        bw.Write(blockCount);
        for (uint i = 0; i < blockCount; i++)
        {
            long  oldBlockOffset = br.ReadInt64();
            ulong firstId        = br.ReadUInt64();
            uint  firstOrdinal   = br.ReadUInt32();
            if (!offsetMap.TryGetValue(oldBlockOffset, out long newBlockOffset)) return -1;
            bw.Write(newBlockOffset);
            bw.Write(firstId);
            bw.Write(firstOrdinal);
        }

        // ── Footer ────────────────────────────────────────────────────────────
        long footerOffset = dst.Position;
        bw.Write(newInvOff);
        bw.Write(newTriOff);
        bw.Write(newBloomOff);
        bw.Write(newBlockIdxOff);
        bw.Write(footerOffset);
        bw.Write(FooterMagic);
        bw.Flush();

        return src.Length - dst.Length;
    }

    private static void CopyExactly(FileStream src, FileStream dst, long count)
    {
        byte[] buf = ArrayPool<byte>.Shared.Rent(1 << 16);
        try
        {
            while (count > 0)
            {
                int n = src.Read(buf, 0, (int)Math.Min(buf.Length, count));
                if (n <= 0) throw new EndOfStreamException();
                dst.Write(buf, 0, n);
                count -= n;
            }
        }
        finally { ArrayPool<byte>.Shared.Return(buf); }
    }

    private static void TryDelete(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); } catch { }
    }
}
