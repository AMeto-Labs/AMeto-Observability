using Ameto.Indexing;

namespace Ameto.Indexing.Tests;

/// <summary>
/// Regression cover for the trigram index's on-disk key width.
///
/// The V1 codec wrote <c>(byte)c</c> per trigram char, so every code unit above
/// U+00FF was truncated — Cyrillic 'п' (U+043F) was stored as 0x3F '?'. Because
/// <see cref="SegmentTrigramIndex.Lookup"/> builds its key from the real search
/// text and treats a MISSING trigram as proof of absence, substring search over
/// non-ASCII text returned zero rows for every flushed segment while still working
/// against the un-flushed hot tier. These tests pin the widened V2 key.
/// </summary>
public sealed class SegmentTrigramNonAsciiTests
{
    private static SegmentTrigramIndex Build(params string[] messages)
    {
        var idx = new SegmentTrigramIndex();
        for (int i = 0; i < messages.Length; i++)
            idx.Add((uint)i, messages[i]);
        return idx;
    }

    [Theory]
    [InlineData("платёж проведён", "проведён")]
    [InlineData("платёж проведён", "платёж")]
    [InlineData("ошибка подключения к базе", "подключения")]
    [InlineData("用户已登录", "户已登")]
    [InlineData("Ünerwartete Ausnahme", "erwartete")]
    public void NonAscii_SurvivesRoundTrip(string message, string query)
    {
        var built = Build(message, "unrelated ascii line", "другое сообщение");
        var read  = SegmentTrigramIndex.Deserialise(built.Serialise());

        var expected = built.Lookup(query);
        Assert.NotNull(expected);
        Assert.NotEmpty(expected!);
        Assert.Equal(expected, read.Lookup(query));
    }

    /// <summary>
    /// The bug's real bite: two DIFFERENT non-ASCII trigrams collapsed onto the same
    /// '?' key, so a hit on one message resolved to the other's offsets. Distinct
    /// Cyrillic words must stay distinct after a round-trip.
    /// </summary>
    [Fact]
    public void DistinctCyrillicWords_DoNotCollide()
    {
        var built = Build("платёж проведён", "ошибка соединения");
        var read  = SegmentTrigramIndex.Deserialise(built.Serialise());

        Assert.Equal([0u], read.Lookup("платёж")!);
        Assert.Equal([1u], read.Lookup("ошибка")!);
        Assert.Empty(read.Lookup("отсутствует")!);
    }

    /// <summary>ASCII behaviour is unchanged by the widened key.</summary>
    [Fact]
    public void Ascii_Unchanged()
    {
        var built = Build("payment processed", "http handled");
        var read  = SegmentTrigramIndex.Deserialise(built.Serialise());

        Assert.Equal([0u], read.Lookup("processed")!);
        Assert.Equal([1u], read.Lookup("handled")!);
        Assert.Empty(read.Lookup("zzz")!);
    }

    /// <summary>
    /// Offsets repeated within one event (the same trigram in both the template and a
    /// property value) must de-duplicate — the codec requires ascending DISTINCT input.
    /// </summary>
    [Fact]
    public void RepeatedOffset_DeDuplicates()
    {
        var idx = new SegmentTrigramIndex();
        idx.Add(0, "повторяем");
        idx.Add(0, "повторяем");   // same offset, same trigrams
        idx.Add(1, "повторяем");

        var read = SegmentTrigramIndex.Deserialise(idx.Serialise());
        Assert.Equal([0u, 1u], read.Lookup("повторяем")!);
    }
}
