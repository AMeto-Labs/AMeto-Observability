using System.Buffers.Binary;
using System.Text;
using K4os.Compression.LZ4;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// The HC re-compressor is a purely mechanical envelope transform — these tests
/// build a byte-exact v5 envelope, run it, and verify every section survives:
/// blocks decode to identical payloads, index sections are byte-identical,
/// block-index offsets are remapped, the flag bit makes the rewrite one-shot.
/// </summary>
public sealed class SegmentRecompressorTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-seg-" + Guid.NewGuid().ToString("N"));

    public SegmentRecompressorTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private const uint Magic       = 0x52_44_4C_47; // "RDLG"
    private const uint FooterMagic = 0x52_44_46_54; // "RDFT"

    private sealed record Envelope(
        string Path,
        List<byte[]> BlockPayloads,
        byte[] InvBytes, byte[] TriBytes, byte[] BloomBytes,
        List<(long Offset, ulong FirstId, uint FirstOrdinal)> BlockIndex);

    /// <summary>Builds a v4/v5 .seg envelope with FAST-compressed compressible blocks.</summary>
    private Envelope BuildSegment(int blocks, ushort version = 5)
    {
        var rnd  = new Random(7);
        var path = Path.Combine(_dir, $"1-{blocks}{version}-100-200.seg");
        using var fs = new FileStream(path, FileMode.CreateNew, FileAccess.Write);
        using var bw = new BinaryWriter(fs);

        // Header (46 bytes: 42 written + 4 slack, matching SegmentWriter)
        bw.Write(Magic);
        bw.Write(version);
        bw.Write((uint)1);                    // nodeId
        bw.Write((ulong)blocks);              // segId
        bw.Write(100L); bw.Write(200L);       // min/max ts
        bw.Write((uint)(blocks * 10));        // eventCount
        bw.Write((byte)0);                    // minLevel
        bw.Write((byte)0x01);                 // flags = FlagCompressed
        bw.Write((byte)0); bw.Write((byte)0);
        fs.Seek(46, SeekOrigin.Begin);

        var payloads = new List<byte[]>();
        var blockIdx = new List<(long, ulong, uint)>();
        for (int b = 0; b < blocks; b++)
        {
            // Log-like compressible payload with some noise.
            var sb = new StringBuilder();
            for (int i = 0; i < 400; i++)
                sb.Append("Payment accepted for provider MintRoute-AED request ")
                  .Append(rnd.Next(1000)).Append(" route console/api/{provider}/ProviderPayment ");
            var payload = Encoding.UTF8.GetBytes(sb.ToString());
            payloads.Add(payload);

            var comp = new byte[LZ4Codec.MaximumOutputSize(payload.Length)];
            int len  = LZ4Codec.Encode(payload, 0, payload.Length, comp, 0, comp.Length, LZ4Level.L00_FAST);

            blockIdx.Add((fs.Position, (ulong)(b * 1000), (uint)(b * 400)));
            bw.Write((uint)payload.Length);
            bw.Write((uint)len);
            bw.Write(comp, 0, len);
        }

        var inv   = Filled(333, 0xAA);
        var tri   = Filled(217, 0xBB);
        var bloom = Filled(129, 0xCC);

        long invOff = fs.Position;   bw.Write((uint)inv.Length);   bw.Write(inv);
        long triOff = fs.Position;   bw.Write((uint)tri.Length);   bw.Write(tri);
        long bloOff = fs.Position;   bw.Write((uint)bloom.Length); bw.Write(bloom);

        long blockIdxOff = fs.Position;
        bw.Write((uint)blockIdx.Count);
        foreach (var (off, fid, ord) in blockIdx)
        {
            bw.Write(off); bw.Write(fid);
            if (version >= 5) bw.Write(ord); // v4 entries have no FirstOrdinal
        }

        long footerOff = fs.Position;
        bw.Write(invOff); bw.Write(triOff); bw.Write(bloOff); bw.Write(blockIdxOff); bw.Write(footerOff);
        bw.Write(FooterMagic);

        return new Envelope(path, payloads, inv, tri, bloom, blockIdx);

        static byte[] Filled(int n, byte v) { var a = new byte[n]; Array.Fill(a, v); return a; }
    }

    [Fact]
    public void Recompress_ShrinksAndPreservesEverything()
    {
        var env  = BuildSegment(blocks: 5);
        long before = new FileInfo(env.Path).Length;

        Assert.True(SegmentRecompressor.IsCandidate(env.Path));
        long? saved = SegmentRecompressor.Recompress(env.Path, NullLogger.Instance, CancellationToken.None);

        Assert.NotNull(saved);
        Assert.True(saved > 0, $"expected shrink, saved={saved}");
        Assert.Equal(before - saved!.Value, new FileInfo(env.Path).Length);

        // Re-parse the rewritten file and verify all content.
        using var fs = new FileStream(env.Path, FileMode.Open, FileAccess.Read);
        using var br = new BinaryReader(fs);

        var hdr = br.ReadBytes(46);
        Assert.Equal(Magic, BinaryPrimitives.ReadUInt32LittleEndian(hdr));
        Assert.Equal(5, BinaryPrimitives.ReadUInt16LittleEndian(hdr.AsSpan(4)));
        Assert.Equal(0x01 | 0x02, hdr[39]); // FlagCompressed | FlagRecompressed

        fs.Seek(-44, SeekOrigin.End);
        long invOff = br.ReadInt64(), triOff = br.ReadInt64(), bloOff = br.ReadInt64();
        long blockIdxOff = br.ReadInt64(), footerOff = br.ReadInt64();
        Assert.Equal(FooterMagic, br.ReadUInt32());

        // Blocks decode to the exact original payloads, in order.
        fs.Seek(46, SeekOrigin.Begin);
        var newOffsets = new List<long>();
        foreach (var payload in env.BlockPayloads)
        {
            newOffsets.Add(fs.Position);
            int uncomp = (int)br.ReadUInt32();
            int comp   = (int)br.ReadUInt32();
            Assert.Equal(payload.Length, uncomp);
            var raw = new byte[uncomp];
            Assert.Equal(uncomp, LZ4Codec.Decode(br.ReadBytes(comp), 0, comp, raw, 0, uncomp));
            Assert.Equal(payload, raw);
        }
        Assert.Equal(invOff, fs.Position);

        // Index sections verbatim.
        Assert.Equal((uint)env.InvBytes.Length, br.ReadUInt32());
        Assert.Equal(env.InvBytes, br.ReadBytes(env.InvBytes.Length));
        Assert.Equal(triOff, fs.Position);
        Assert.Equal((uint)env.TriBytes.Length, br.ReadUInt32());
        Assert.Equal(env.TriBytes, br.ReadBytes(env.TriBytes.Length));
        Assert.Equal(bloOff, fs.Position);
        Assert.Equal((uint)env.BloomBytes.Length, br.ReadUInt32());
        Assert.Equal(env.BloomBytes, br.ReadBytes(env.BloomBytes.Length));

        // Block index remapped to the new block positions, other fields intact.
        Assert.Equal(blockIdxOff, fs.Position);
        Assert.Equal((uint)env.BlockIndex.Count, br.ReadUInt32());
        for (int i = 0; i < env.BlockIndex.Count; i++)
        {
            Assert.Equal(newOffsets[i], br.ReadInt64());
            Assert.Equal(env.BlockIndex[i].FirstId, br.ReadUInt64());
            Assert.Equal(env.BlockIndex[i].FirstOrdinal, br.ReadUInt32());
        }
        Assert.Equal(footerOff, fs.Position);
    }

    [Fact]
    public void Recompress_IsOneShot()
    {
        var env = BuildSegment(blocks: 2);
        Assert.NotNull(SegmentRecompressor.Recompress(env.Path, NullLogger.Instance, CancellationToken.None));

        // Second pass: flag set → not a candidate, and Recompress refuses.
        Assert.False(SegmentRecompressor.IsCandidate(env.Path));
        Assert.Null(SegmentRecompressor.Recompress(env.Path, NullLogger.Instance, CancellationToken.None));
    }

    /// <summary>v4 segments (no FirstOrdinal in block-index entries) must migrate too — they are the bulk of pre-July data.</summary>
    [Fact]
    public void Recompress_HandlesV4Segments()
    {
        var env = BuildSegment(blocks: 4, version: 4);
        long before = new FileInfo(env.Path).Length;

        Assert.True(SegmentRecompressor.IsCandidate(env.Path));
        long? saved = SegmentRecompressor.Recompress(env.Path, NullLogger.Instance, CancellationToken.None);
        Assert.NotNull(saved);
        Assert.True(saved > 0);

        using var fs = new FileStream(env.Path, FileMode.Open, FileAccess.Read);
        using var br = new BinaryReader(fs);
        var hdr = br.ReadBytes(46);
        Assert.Equal(4, BinaryPrimitives.ReadUInt16LittleEndian(hdr.AsSpan(4))); // version preserved
        Assert.Equal(0x01 | 0x02, hdr[39]);

        fs.Seek(-44, SeekOrigin.End);
        long invOff = br.ReadInt64(); br.ReadInt64(); br.ReadInt64();
        long blockIdxOff = br.ReadInt64(); br.ReadInt64();
        Assert.Equal(FooterMagic, br.ReadUInt32());

        // Blocks roundtrip + block index remapped with 16-byte v4 entries.
        fs.Seek(46, SeekOrigin.Begin);
        var newOffsets = new List<long>();
        foreach (var payload in env.BlockPayloads)
        {
            newOffsets.Add(fs.Position);
            int uncomp = (int)br.ReadUInt32();
            int comp   = (int)br.ReadUInt32();
            var raw = new byte[uncomp];
            Assert.Equal(uncomp, LZ4Codec.Decode(br.ReadBytes(comp), 0, comp, raw, 0, uncomp));
            Assert.Equal(payload, raw);
        }
        Assert.Equal(invOff, fs.Position);

        fs.Seek(blockIdxOff, SeekOrigin.Begin);
        Assert.Equal((uint)env.BlockIndex.Count, br.ReadUInt32());
        for (int i = 0; i < env.BlockIndex.Count; i++)
        {
            Assert.Equal(newOffsets[i], br.ReadInt64());
            Assert.Equal(env.BlockIndex[i].FirstId, br.ReadUInt64());
        }
        Assert.False(SegmentRecompressor.IsCandidate(env.Path)); // one-shot for v4 too
    }
}
