namespace Rd.Log.Query.Filtering;

/// <summary>
/// Internal canonical encoding for property paths.
///
/// User-typed paths such as <c>Headers.Foo</c> or <c>Headers['Api-Request-Id'].Bar</c>
/// are decomposed into segments by <see cref="FilterParser"/> and re-joined with
/// <see cref="Separator"/> (U+0001) so that the runtime can split on a delimiter
/// that cannot collide with any in-segment character (idents disallow it, and
/// JSON property names from MessagePack do not contain control bytes).
/// </summary>
public static class PropertyPath
{
    /// <summary>U+0001 START OF HEADING — chosen as separator because it is
    /// excluded from JSON property names produced by MessagePack/CLEF.</summary>
    public const char Separator = '\u0001';

    /// <summary>U+0002 START OF TEXT — segment prefix marking that the segment
    /// is a numeric array index (originating from <c>Foo[0]</c> style paths).
    /// String segments (<c>Foo['0']</c>) never carry this prefix, so the
    /// evaluator can decide between dictionary-key and list-index lookups.</summary>
    public const char IndexMarker = '\u0002';

    /// <summary>True when the path is nested (contains more than one segment).</summary>
    public static bool IsNested(string path) => path.IndexOf(Separator) >= 0;

    /// <summary>True when the segment was produced from <c>[number]</c>.</summary>
    public static bool IsIndexSegment(ReadOnlySpan<char> segment) =>
        segment.Length > 0 && segment[0] == IndexMarker;

    /// <summary>Returns the raw textual form of a segment (strips marker, if any).</summary>
    public static ReadOnlySpan<char> SegmentValue(ReadOnlySpan<char> segment) =>
        IsIndexSegment(segment) ? segment[1..] : segment;
}
