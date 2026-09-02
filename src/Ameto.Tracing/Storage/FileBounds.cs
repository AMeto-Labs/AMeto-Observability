namespace Ameto.Tracing.Storage;

/// <summary>
/// THE ONE RULE EVERY READER IN THIS NAMESPACE SIZES AN ALLOCATION BY: a number that came out of a
/// file may not ask for more memory than the file had bytes to ask with.
///
/// <para>This exists because the rule kept being restated in prose and applied to one site at a
/// time. Four review rounds fixed the reported instance and left its twin one file over: the
/// <c>.stats</c> count was bounded with a measurement written into its comment while
/// <c>ServiceGraphSidecar</c>'s identical shape measured the same 4 GB and went untouched; then the
/// counts were all bounded while every block LENGTH stayed on a 64 MB constant, which a one-byte
/// flip turned into 64 MB rented out of a 3.5 KB file, eight ways in parallel on the 512 MB server
/// this whole branch exists to keep alive.</para>
///
/// <para>TWO SIZES PER ELEMENT, AND THAT IS THE POINT OF THE SIGNATURE. Bounding a count by the
/// file bytes it occupies is not enough when the thing built from it is bigger: a
/// <c>HashSet&lt;uint&gt;</c> stores four bytes per element in the file and costs about sixteen on
/// the heap, so "count &lt;= bytes/4" quietly permits four times the file — measured at 5.17x on a
/// 361 KB segment with a torn-but-in-range offset. Asking for both numbers makes that visible at
/// every call site, and the bound uses the LARGER, so an allocation can never exceed the bytes that
/// described it.</para>
/// </summary>
internal static class FileBounds
{
    /// <summary>
    /// Whether a read failure describes the FILE'S CONTENTS rather than the machine's ability to
    /// read it — the split that decides whether a fault is permanent damage or "ask me again".
    ///
    /// <para>Here, in one place, because it was hand-copied into two files in the change that
    /// introduced it — the same "same shape one file over" this whole class exists to stop, created
    /// deliberately and then diverging within one round: OverflowException reaches
    /// MessagePackPrimitives from checked arithmetic and was in neither list, so 17 of 2000 measured
    /// bit-flips answered real corruption with "retry", for ever.</para>
    /// </summary>
    internal static bool DescribesContent(Exception ex) =>
        ex is InvalidDataException or EndOfStreamException
           or System.Text.DecoderFallbackException
           or IndexOutOfRangeException or ArgumentOutOfRangeException
           or OverflowException
           or MessagePack.MessagePackSerializationException;

    /// <summary>
    /// The largest element count <paramref name="bytesRemaining"/> can honestly describe.
    /// </summary>
    /// <param name="fileBytesPerElement">Smallest number of file bytes one element occupies.</param>
    /// <param name="heapBytesPerElement">
    /// Roughly what one element costs once built. Pass the same value as the file size when they
    /// match; the difference is the whole reason this parameter exists.
    /// </param>
    internal static long MaxCountThatFits(long bytesRemaining, int fileBytesPerElement, int heapBytesPerElement)
    {
        if (bytesRemaining <= 0) return 0;
        int per = Math.Max(1, Math.Max(fileBytesPerElement, heapBytesPerElement));
        return bytesRemaining / per;
    }

    /// <summary>
    /// Throws unless <paramref name="count"/> is a number this many bytes could describe. The
    /// message names both sizes, because "the count is too large" without them sends the next
    /// reader to the wrong end of the file.
    /// </summary>
    internal static void RequireCountFits(
        long count, long bytesRemaining, int fileBytesPerElement, int heapBytesPerElement,
        string what, string path)
    {
        long max = MaxCountThatFits(bytesRemaining, fileBytesPerElement, heapBytesPerElement);
        if (count < 0 || count > max)
            throw new InvalidDataException(
                $"{what} count too large: declares {count} entries in {path}, but {bytesRemaining} bytes remain and one "
              + $"entry costs at least {fileBytesPerElement} on disk / {heapBytesPerElement} in memory "
              + $"(at most {max})");
    }

    /// <summary>
    /// Throws unless <paramref name="length"/> is a byte count the remaining file could supply. The
    /// companion to <see cref="RequireCountFits"/> for raw lengths — block sizes, payload sizes —
    /// which were the half of this rule that a constant was doing instead.
    /// </summary>
    internal static void RequireLengthFits(long length, long bytesRemaining, string what, string path)
    {
        if (length < 0 || length > bytesRemaining)
            throw new InvalidDataException(
                $"{what} size too large: claims {length} bytes in {path}, but only {bytesRemaining} remain");
    }
}
