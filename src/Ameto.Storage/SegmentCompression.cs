namespace Ameto.Storage;

/// <summary>
/// Which LZ4 level a <see cref="SegmentWriter"/> encodes its blocks at. A write-side choice
/// only — an LZ4 block decodes identically whichever level produced it, so the reader has
/// no branch and a mixed directory is normal.
/// </summary>
public enum SegmentCompression
{
    /// <summary>
    /// LZ4 fast (L00). The flush path's level: a flush is on the ingest thread's critical
    /// path and its output is short-lived — the first merge rewrites it.
    /// </summary>
    Fast = 0,

    /// <summary>
    /// LZ4-HC (L09). The merge path's level: a merged file is written once, read many times
    /// and kept until retention deletes it, so the encode cost is paid once against a
    /// smaller file for the whole of its life.
    /// </summary>
    High = 1,
}
