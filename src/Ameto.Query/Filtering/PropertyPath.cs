using Ameto.Core;

namespace Ameto.Query.Filtering;

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
    public const char Separator = ClefFields.PropertyPathSeparator;

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

    /// <summary>
    /// Reads a segment as a position in a list — THE definition of which segments are one.
    /// The evaluator asks it of every segment that meets a list (<c>Foo.0</c> and
    /// <c>Foo['0']</c> reach the first element, as <c>Foo[0]</c> does), and anything that has
    /// to know which paths can reach a list's elements asks the same question here, so the two
    /// cannot come to disagree about a spelling.
    ///
    /// <para>They did once: this accepts whatever <see cref="int.TryParse(ReadOnlySpan{char}, System.Globalization.NumberStyles, IFormatProvider?, out int)"/>
    /// accepts as an integer — white space on either side, a leading sign, a trailing NUL —
    /// while a reader that took "a position is a run of digits" for the rule judged
    /// <c>Items[' 0'].Sku</c> as a path through a property called <c>" 0"</c>, and let it past
    /// a rule on <c>Items.Sku</c>.</para>
    ///
    /// <para>Allocation-free: the evaluator calls it per event.</para>
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public static bool TryReadPosition(ReadOnlySpan<char> segmentValue, out int position) =>
        int.TryParse(segmentValue, System.Globalization.NumberStyles.Integer,
                     System.Globalization.CultureInfo.InvariantCulture, out position);

    /// <summary>
    /// True when the encoded path could ALSO name a single flat property key spelled with
    /// literal dots — i.e. it is nested and carries no array subscript.
    ///
    /// <para><c>FilterParser.ReadPropertyPath</c> splits <c>http.request.method</c> on the dot
    /// and rejoins with <see cref="Separator"/>, but an OTLP attribute of that name is ONE key
    /// with dots in it: <c>OtlpLogMapper</c> copies resource and record attributes verbatim,
    /// <c>LogEventSerializer.ReadMap</c> stores them verbatim, and <c>SegmentIndexBuilder</c>
    /// files them verbatim. So the grammar's only reading of a dotted name was "walk a nested
    /// map", and every semantic-convention attribute filter on an OTLP server matched nothing
    /// unless the user knew to bracket-quote it. Both spellings are now candidates, on the
    /// evaluator side and in the index lookup alike.</para>
    /// </summary>
    public static bool MayBeFlatKey(string path) =>
        path.IndexOf(Separator) >= 0 && path.IndexOf(IndexMarker) < 0;

    /// <summary>
    /// Writes the literal-dot spelling of an encoded path into <paramref name="dest"/> and
    /// returns its length, or -1 when <paramref name="dest"/> is too small. Allocation-free:
    /// the evaluator calls this per event.
    /// </summary>
    public static int WriteFlatKey(ReadOnlySpan<char> path, Span<char> dest)
    {
        if (dest.Length < path.Length) return -1;
        for (int i = 0; i < path.Length; i++)
            dest[i] = path[i] == Separator ? '.' : path[i];
        return path.Length;
    }

    /// <summary>
    /// The path as the person who typed it would write it — <c>Headers.Foo</c>,
    /// <c>Items[0].Name</c>. The encoded form carries control characters, so anything that
    /// puts a property name in front of a user (an aggregation's column heading, a parser
    /// diagnostic) has to come back through here first.
    /// </summary>
    public static string ToDisplay(string path)
    {
        if (path.IndexOf(Separator) < 0 && path.IndexOf(IndexMarker) < 0) return path;

        var sb   = new System.Text.StringBuilder(path.Length + 4);
        var rest = path.AsSpan();
        while (!rest.IsEmpty)
        {
            int sep = rest.IndexOf(Separator);
            var seg = sep < 0 ? rest : rest[..sep];
            rest    = sep < 0 ? default : rest[(sep + 1)..];

            if (IsIndexSegment(seg))      sb.Append('[').Append(SegmentValue(seg)).Append(']');
            else if (sb.Length > 0)       sb.Append('.').Append(seg);
            else                          sb.Append(seg);
        }
        return sb.ToString();
    }

    /// <summary>Number of segments in an encoded path (always at least one).</summary>
    public static int SegmentCount(string path)
    {
        int n = 1;
        for (int i = 0; i < path.Length; i++)
            if (path[i] == Separator) n++;
        return n;
    }

    /// <summary>
    /// Drops array-subscript segments from an encoded path, so the two segments of
    /// <c>Foo[0].Bar</c> survive and the subscript does not.
    ///
    /// <para>The segment index does not record element positions: <c>SegmentIndexBuilder</c>
    /// recurses through a msgpack array without extending the flat key, so every element of
    /// <c>Foo</c> is filed under the bare key <c>Foo</c>. The subscripted path therefore has
    /// no bucket of its own, and the stripped path names the bucket that holds every element
    /// — a superset of what the subscript selects, which is exactly what an index hint may
    /// be. The scan re-checks the subscript per event.</para>
    ///
    /// <para>Returns the argument itself when there is nothing to strip, so the common case
    /// allocates nothing. Only ever called while compiling a filter, never per query.</para>
    /// </summary>
    public static string StripIndexSegments(string path)
    {
        if (path.IndexOf(IndexMarker) < 0) return path;

        char[]? rented = path.Length > 256
            ? System.Buffers.ArrayPool<char>.Shared.Rent(path.Length)
            : null;
        Span<char> buf = rented ?? stackalloc char[256];
        try
        {
            int written = 0;
            var rest    = path.AsSpan();
            while (!rest.IsEmpty)
            {
                int sep = rest.IndexOf(Separator);
                var seg = sep < 0 ? rest : rest[..sep];
                rest    = sep < 0 ? default : rest[(sep + 1)..];

                if (IsIndexSegment(seg)) continue;
                if (written > 0) buf[written++] = Separator;
                seg.CopyTo(buf[written..]);
                written += seg.Length;
            }
            return new string(buf[..written]);
        }
        finally
        {
            if (rented is not null) System.Buffers.ArrayPool<char>.Shared.Return(rented);
        }
    }
}
