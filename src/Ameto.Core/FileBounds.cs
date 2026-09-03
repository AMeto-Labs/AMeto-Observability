namespace Ameto.Core;

/// <summary>
/// THE ONE RULE EVERY READER IN THIS NAMESPACE SIZES AN ALLOCATION BY: a number that came out of a
/// file may not describe more entries than the file had bytes to hold them.
///
/// <para>This exists because the rule kept being restated in prose and applied to one site at a
/// time. Four review rounds fixed the reported instance and left its twin one file over: the
/// <c>.stats</c> count was bounded with a measurement written into its comment while
/// <c>ServiceGraphSidecar</c>'s identical shape measured the same 4 GB and went untouched; then the
/// counts were all bounded while every block LENGTH stayed on a 64 MB constant, which a one-byte
/// flip turned into 64 MB rented out of a 3.5 KB file, eight ways in parallel on the 512 MB server
/// this branch exists to keep alive.</para>
///
/// <para>TWO QUESTIONS, TWO ANSWERS, AND CONFLATING THEM BROKE HEALTHY FILES. The first version of
/// this class took a file size and a heap size per element and divided the remaining bytes by the
/// LARGER, on the reasoning that an allocation should never exceed the bytes that described it.
/// That reasoning is wrong at its first step: <c>bytesRemaining</c> counts bytes ON DISK, so a
/// legitimate count of N occupies <c>N * fileBytesPerElement</c> — dividing by a bigger heap figure
/// makes the bound tighter than the format allows. For a <c>HashSet&lt;uint&gt;</c> (four bytes on
/// disk, about sixteen in memory) it was four times too strict, and it REFUSED ORDINARY SEGMENTS:
/// measured against the previous commit, a 20 000-span v3 file with no attributes threw
/// <c>InvalidDataException</c> where the parent returned all 20 000 rows, and so did a 50 000-span
/// one — exactly <c>HotFlushThreshold</c>, the size of an ordinary flush. Worse, that exception is
/// content-shaped, so it classified as permanent damage over data that was entirely intact. The
/// suite stayed green because every fixture writes attributes, and attributes inflate the bloom
/// index that <c>bytesRemaining</c> is measured from.</para>
///
/// <para>So: <see cref="RequireCountFits"/> answers "could the file hold this many?" and nothing
/// else. What an entry costs once BUILT is a separate question with a separate answer —
/// <see cref="PreallocFor"/>, which caps a preallocation without refusing the read, because a large
/// legitimate file legitimately needs its memory and a reader that throws at it is a worse failure
/// than one that grows a list.</para>
/// </summary>
public static class FileBounds
{
    /// <summary>
    /// The most entries a preallocation will reserve up front, whatever a legitimate count says.
    /// Growing into a real one costs a few doublings; reserving for a declared one costs the
    /// process, and this is the difference between the two.
    /// </summary>
    private const int PreallocByteCeiling = 4 * 1024 * 1024;

    /// <summary>
    /// Whether a read failure describes the FILE'S CONTENTS rather than the machine's ability to
    /// read it — the split that decides whether a fault is permanent damage or "ask me again".
    ///
    /// <para>Here, in one place, because it was hand-copied into two files in the change that
    /// introduced it — the same "same shape one file over" this class exists to stop, created
    /// deliberately and then diverging within one round: OverflowException reaches
    /// MessagePackPrimitives from checked arithmetic and was in neither list, so 17 of 2000 measured
    /// bit-flips answered real corruption with "retry", for ever.</para>
    /// </summary>
    public static bool DescribesContent(Exception ex) =>
        ex is InvalidDataException or EndOfStreamException
           or System.Text.DecoderFallbackException
           or IndexOutOfRangeException or ArgumentOutOfRangeException
           or OverflowException
           or MessagePack.MessagePackSerializationException;

    /// <summary>
    /// The largest element count <paramref name="bytesRemaining"/> could hold on disk.
    /// </summary>
    /// <param name="fileBytesPerElement">
    /// Smallest number of FILE bytes one entry occupies. Only the on-disk size belongs here: what
    /// the entry costs in memory cannot make the file smaller, and treating it as if it could is
    /// what refused healthy segments.
    /// </param>
    public static long MaxCountThatFits(long bytesRemaining, int fileBytesPerElement)
    {
        if (bytesRemaining <= 0) return 0;
        return bytesRemaining / Math.Max(1, fileBytesPerElement);
    }

    /// <summary>
    /// Throws unless <paramref name="count"/> is a number this many bytes could describe.
    /// </summary>
    public static void RequireCountFits(
        long count, long bytesRemaining, int fileBytesPerElement, string what, string path)
    {
        long max = MaxCountThatFits(bytesRemaining, fileBytesPerElement);
        if (count < 0 || count > max)
            throw new InvalidDataException(
                $"{what} count too large: declares {count} entries in {path}, but {bytesRemaining} bytes "
              + $"remain and one entry occupies at least {fileBytesPerElement} on disk (at most {max})");
    }

    /// <summary>
    /// A capacity to reserve for <paramref name="count"/> entries — the count itself when that is
    /// modest, and a ceiling when it is not. NOT a limit: the collection still grows to whatever the
    /// file honestly contains.
    ///
    /// <para>This is where the heap size of an entry belongs. A count can pass
    /// <see cref="RequireCountFits"/> honestly and still be expensive to reserve for, because a
    /// <c>HashSet&lt;uint&gt;</c> costs about sixteen bytes for every four it occupies on disk —
    /// measured at 5.17x the file on a 361 KB segment. Capping the reservation answers that without
    /// refusing to read anything.</para>
    /// </summary>
    public static int PreallocFor(long count, int heapBytesPerElement)
    {
        long ceiling = PreallocByteCeiling / Math.Max(1, heapBytesPerElement);
        return (int)Math.Clamp(count, 0, ceiling);
    }

    /// <summary>
    /// Throws unless <paramref name="length"/> is a byte count the remaining file could supply. The
    /// companion to <see cref="RequireCountFits"/> for raw lengths — block sizes, payload sizes —
    /// which were the half of this rule that a constant was doing instead.
    /// </summary>
    public static void RequireLengthFits(long length, long bytesRemaining, string what, string path)
    {
        if (length < 0 || length > bytesRemaining)
            throw new InvalidDataException(
                $"{what} size too large: claims {length} bytes in {path}, but only {bytesRemaining} remain");
    }
}
