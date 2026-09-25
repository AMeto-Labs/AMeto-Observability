using System.Buffers.Binary;
using System.Globalization;
using Ameto.Tracing;
using Ameto.Tracing.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE BLOOM WRITER AS IT STOOD BEFORE #86, kept in the tests so that a segment written by it can
/// still be made: every <c>.trc</c> on a deployed server carries one, and the reader has to keep
/// answering them correctly until retention has removed the last.
///
/// <para>IT IS THE OLD CODE, NOT A MODEL OF IT: the same walk (<c>SpanAttributeBlob.TryWalk</c>
/// with a boxed value per pair, the dictionary as the fallback), the same entry
/// (<c>value.ToString()</c> in the CURRENT culture, lowercased by the legacy hash) and the same
/// block cut (4096 spans of the writer's own start-time permutation). That claim is held by
/// <c>TraceFlushProbe</c>: the new writer's file with this section spliced in is, byte for byte,
/// the file the pre-change writer produced — its original golden SHA-256.</para>
/// </summary>
internal static class LegacySpanBloom
{
    private const int BlockSize  = 4096;
    private const int FooterSize = 28;

    /// <summary>
    /// Replaces the bloom index of <paramref name="trcPath"/> (which must have been written by
    /// <c>SpanWriter</c> from <paramref name="corpus"/>) with the section the pre-#86 writer would
    /// have written for it under <paramref name="culture"/>. Everything before the section and the
    /// footer after it are kept; the footer's offsets do not move, because the section is the last
    /// thing before it.
    /// </summary>
    public static void Rewrite(string trcPath, IList<SpanRecord> corpus, string culture)
    {
        byte[] file   = File.ReadAllBytes(trcPath);
        long   bloomAt = (long)BinaryPrimitives.ReadUInt64LittleEndian(file.AsSpan(file.Length - 12));
        byte[] footer = file.AsSpan(file.Length - FooterSize).ToArray();

        var blooms = UnderCulture(culture, () => Blooms(corpus));

        using var fs = new FileStream(trcPath, FileMode.Create, FileAccess.Write);
        fs.Write(file, 0, (int)bloomAt);
        using var bw = new BinaryWriter(fs);
        bw.Write((uint)blooms.Count);
        foreach (var b in blooms)
        {
            bw.Write((uint)b.Length);
            bw.Write(b);
        }
        bw.Write(footer);
    }

    /// <summary>The pre-#86 <c>SpanWriter.WriteBlock</c> bloom feed, block by block.</summary>
    private static List<byte[]> Blooms(IList<SpanRecord> spans)
    {
        // The writer's permutation, computed the way it computes it — the same Span.Sort over the
        // same comparison, so ties land where they landed in the file.
        int count  = spans.Count;
        var order  = new int[count];
        var keys   = new long[count];
        for (int i = 0; i < count; i++) { order[i] = i; keys[i] = spans[i].StartTimeUnixNano; }
        order.AsSpan().Sort((a, b) => keys[a].CompareTo(keys[b]));

        var blooms = new List<byte[]>();
        var hashes = new HashSet<ulong>();
        for (int start = 0; start < count; start += BlockSize)
        {
            hashes.Clear();
            int end = Math.Min(count, start + BlockSize);
            for (int i = start; i < end; i++)
            {
                var s    = spans[order[i]];
                var blob = s.AttributesBytes;
                if (!blob.IsEmpty && SpanAttributeBlob.TryWalk(blob, hashes, static (h, k, v) => AddAttr(h, k, v)))
                    continue;
                if (s.Attributes is { Count: > 0 } attrs)
                    foreach (var (k, v) in attrs) AddAttr(hashes, k, v);
            }
            blooms.Add(SpanBloom.Build(hashes));
        }
        return blooms;
    }

    /// <summary>The pre-#86 <c>SpanBloom.AddAttr</c>, verbatim.</summary>
    internal static void AddAttr(HashSet<ulong> hashes, string key, object? value)
    {
        hashes.Add(SpanBloom.LegacyHashKey(key));
        if (value is not null)
            hashes.Add(SpanBloom.LegacyHashKeyValue(key, value.ToString() ?? string.Empty));
    }

    private static T UnderCulture<T>(string culture, Func<T> body)
    {
        var saved = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
        try { return body(); }
        finally { CultureInfo.CurrentCulture = saved; }
    }
}
