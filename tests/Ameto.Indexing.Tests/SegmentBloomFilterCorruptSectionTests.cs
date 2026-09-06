using Ameto.Core;
using System.Buffers.Binary;
using System.Diagnostics;
using Ameto.Indexing;
using Xunit;

namespace Ameto.Indexing.Tests;

/// <summary>
/// <see cref="SegmentBloomFilter.Deserialise"/> takes every dimension from the blob's own header
/// and its bits are <see cref="System.Runtime.InteropServices.NativeMemory"/>, so a header that
/// disagrees with the section it arrived in decides whether a corrupt file costs a query or costs
/// the process.
///
/// <para>The caller is what makes the size of that decision. QueryExecutor's prefilter catches
/// per group, per segment, per query and falls back to a full scan, so a bloom section that fails
/// to parse is never fatal and never retired — it is re-read on every query that touches its
/// group, indefinitely. Anything the failed parse left behind is therefore left behind again each
/// time, and off-heap, where no managed instrument shows it.</para>
///
/// <para>Corruption of this shape is not exotic: a torn write, a partially copied replica, a
/// truncated restore. The reader hands the section over at its DECLARED length, so the blob's
/// header is the only thing that says how many bits are supposed to be inside it.</para>
/// </summary>
public sealed class SegmentBloomFilterCorruptSectionTests
{
    /// <summary>
    /// The failure is stated by the EXCEPTION TYPE, which is the whole evidence here.
    ///
    /// <para><see cref="InvalidDataException"/> can only come from the length check, which runs
    /// before <c>AllocZeroed</c>. The version this replaces allocated first and died on the copy,
    /// which surfaced as <see cref="ArgumentOutOfRangeException"/> out of <c>Span.Slice</c> — so
    /// the type is not decoration, it is the difference between rejecting the blob and having
    /// already taken the memory that then goes unreachable. Measured on the old code, one blob
    /// declaring 0xFFFFFFFF bits: 513 MB of private bytes on a single call, never freed.</para>
    /// </summary>
    [Fact]
    public void A_section_shorter_than_its_declared_bit_count_is_rejected_before_it_allocates()
    {
        Assert.Throws<InvalidDataException>(() => SegmentBloomFilter.Deserialise(Header(bitCount: 0xFFFF_FE00u)));

        // One block short is still short: the check is the section's real length, not a guess at
        // whether the shortfall looks deliberate.
        byte[] oneBlockShort = new byte[8 + 512 / 8];
        BinaryPrimitives.WriteUInt32LittleEndian(oneBlockShort, 1024u);
        Assert.Throws<InvalidDataException>(() => SegmentBloomFilter.Deserialise(oneBlockShort));
    }

    /// <summary>
    /// The harm itself, weighed rather than inferred — and weighed WITHOUT asserting how the
    /// refusal is signalled, so that this measures the leak instead of re-testing the exception
    /// type above. Sixteen passes over a header claiming 64 MiB of bits inside a twelve-byte
    /// section: a gigabyte of native memory under the old ordering, nothing under this one.
    ///
    /// <para>Sixteen because one is not the failure. The prefilter's catch means a corrupt section
    /// is refused and re-read on every query that touches its group, so what matters is that the
    /// refusal costs the same the sixteenth time as the first.</para>
    /// </summary>
    [Fact]
    public void Rejecting_a_corrupt_section_repeatedly_does_not_grow_native_memory()
    {
        const int passes    = 16;
        const uint bitCount = 64u * 1024 * 1024 * 8;   // 64 MiB of bits, a clean multiple of 512

        byte[] blob = Header(bitCount);
        long before = CommittedBytes();
        for (int i = 0; i < passes; i++)
        {
            try { SegmentBloomFilter.Deserialise(blob).Dispose(); }
            catch (Exception) { /* deliberately untyped: this test weighs the refusal, not its shape */ }
        }
        long grew = CommittedBytes() - before;

        // A quarter of what the leak would be — far above any noise this assembly generates,
        // far below the 1 GiB the old code committed and dropped over these sixteen calls.
        const long bound = (long)passes * (bitCount / 8) / 4;
        Assert.True(grew < bound,
            $"private bytes grew {grew / 1048576.0:F0} MB over {passes} rejected sections, against a " +
            $"{bound / 1048576.0:F0} MB bound — the section is being allocated before it is checked");
    }

    /// <summary>
    /// The other face of the same guard. <c>bitCount</c> in [8, 512) fits inside a small blob, so
    /// a length check alone lets it through — and it yields <c>blockCount == 0</c>, which is not a
    /// construction failure at all but a <see cref="DivideByZeroException"/> raised later, from
    /// inside <c>Add</c> and <c>MightContain</c>, underneath the pooled buffers that
    /// <c>AddRaw</c> and <c>MightContainRaw</c> take and return without a <c>finally</c>. The
    /// binary format has always required the multiple of 512; nothing checked it.
    /// </summary>
    [Fact]
    public void A_bit_count_that_is_not_a_whole_number_of_blocks_is_rejected()
    {
        foreach (uint bitCount in new uint[] { 8, 64, 511, 513, 1023 })
        {
            byte[] blob = new byte[8 + 1024];   // long enough that only the block check can reject it
            BinaryPrimitives.WriteUInt32LittleEndian(blob, bitCount);
            Assert.Throws<InvalidDataException>(() => SegmentBloomFilter.Deserialise(blob));
        }
    }

    /// <summary>
    /// The guard has to reject corruption without rejecting files. A real filter's own blob, and
    /// the empty-section case that means "no index was built" — which is NO information and must
    /// still come back matching everything, or the prefilter would drop every segment written
    /// before the index builder was wired.
    /// </summary>
    [Fact]
    public void A_real_blob_and_an_absent_section_are_both_still_accepted()
    {
        using var written = SegmentBloomFilter.Create(4096);
        written.Add("Wallet.API");
        written.Add("Error");

        using var read = SegmentBloomFilter.Deserialise(written.Serialise());
        Assert.True(read.MightContain("Wallet.API"));
        Assert.True(read.MightContain("Error"));
        Assert.Equal(written.Capacity, read.Capacity);

        using var absent = SegmentBloomFilter.Deserialise([]);
        Assert.True(absent.MightContain("anything at all"));

        // A one-block filter is the smallest legal blob and must survive the multiple-of-512
        // check, which is where an off-by-one in it would show.
        using var smallest = SegmentBloomFilter.Create(1);
        using var reread   = SegmentBloomFilter.Deserialise(smallest.Serialise());
        Assert.False(reread.MightContain("nothing was ever added"));
    }

    /// <summary>An 8-byte header declaring <paramref name="bitCount"/> bits and carrying none.</summary>
    private static byte[] Header(uint bitCount)
    {
        byte[] blob = new byte[12];
        BinaryPrimitives.WriteUInt32LittleEndian(blob, bitCount);
        return blob;
    }

    /// <summary>Commit charge, which counts NativeMemory the moment it is taken — unlike anything
    /// on the managed side, which never sees these bytes at all.</summary>
    private static long CommittedBytes()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        using var self = Process.GetCurrentProcess();
        return self.PrivateMemorySize64;
    }
}
