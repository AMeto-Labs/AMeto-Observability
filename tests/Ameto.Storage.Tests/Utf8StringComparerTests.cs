using System.Text;

namespace Ameto.Storage.Tests;

/// <summary>
/// The comparer that lets the merge's dedup table be probed with a segment column's raw bytes.
///
/// <para>Its whole job is that two spellings of one value — the UTF-8 on the block and the UTF-16
/// on the heap — hash and compare alike. Everything here is a place where those two spellings
/// disagree about length: Cyrillic spends two bytes a character, an emoji spends four bytes for
/// two UTF-16 units, and a value past the stack scratch takes the pooled path on both sides.</para>
/// </summary>
public sealed class Utf8StringComparerTests
{
    private static Dictionary<string, string> NewTable() => new(Utf8StringComparer.Instance);

    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>
    /// Hash agreement, which is the one thing a two-sided comparer can get wrong silently: an
    /// entry put in by STRING has to be findable by BYTES. A mismatch would not corrupt anything —
    /// dedup is only an allocation optimisation — it would quietly turn the table into a
    /// write-only structure and put the per-event string back.
    /// </summary>
    [Theory]
    [InlineData("evt {n} round 0")]                                  // ASCII
    [InlineData("Заказ {id} отклонён")]                              // 2 bytes a char
    [InlineData("payment 🙂 settled")]                                // 4 bytes, 2 UTF-16 units
    [InlineData("")]
    public void AnEntryInsertedByString_IsFoundByItsBytes(string value)
    {
        var table = NewTable();
        table[value] = value;

        Assert.True(table.GetAlternateLookup<ReadOnlySpan<byte>>().TryGetValue(Utf8(value), out var found));
        Assert.Same(value, found);
    }

    /// <summary>Past the 512-byte stack scratch both sides rent, and must still agree.</summary>
    [Fact]
    public void AValueLongerThanTheStackScratch_IsFoundByItsBytes()
    {
        var table = NewTable();
        string ascii   = new('x', 900);
        string cyrilic = new('щ', 400);   // 800 bytes of UTF-8 from 400 UTF-16 units
        table[ascii]   = ascii;
        table[cyrilic] = cyrilic;

        var byBytes = table.GetAlternateLookup<ReadOnlySpan<byte>>();
        Assert.Same(ascii,   byBytes[Utf8(ascii)]);
        Assert.Same(cyrilic, byBytes[Utf8(cyrilic)]);
    }

    /// <summary>
    /// The point of the exercise: one string per DISTINCT value, whatever the traffic. Probing
    /// constructs nothing, so every hit hands back the instance already on the heap — which is
    /// what the merge's B/event figure is made of (see
    /// <c>SegmentMergeTests.Merge_AllocationStaysFlatAcrossALargeBacklog</c>).
    /// </summary>
    [Fact]
    public void ProbingByBytes_KeepsHandingBackTheSameInstance()
    {
        var table   = NewTable();
        var byBytes = table.GetAlternateLookup<ReadOnlySpan<byte>>();
        var bytes   = Utf8("Заказ {id} отклонён");

        byBytes[bytes] = Encoding.UTF8.GetString(bytes);
        string first   = byBytes[bytes];

        for (int i = 0; i < 1000; i++) Assert.Same(first, byBytes[bytes]);
        Assert.Single(table);
    }

    /// <summary>
    /// Values that are equal for the first bytes and part after them, in both directions across
    /// the ASCII boundary — the comparison Ascii.Equals cannot answer on its own, since it reports
    /// false both for "different" and for "not ASCII".
    /// </summary>
    [Theory]
    [InlineData("Заказ",  "Заказы")]
    [InlineData("order",  "ordér")]
    [InlineData("ordér",  "order")]
    [InlineData("payment 🙂", "payment 🙃")]
    [InlineData("Svc.1",  "Svc.2")]
    public void ValuesThatDiffer_AreNotConflated(string a, string b)
    {
        var table   = NewTable();
        var byBytes = table.GetAlternateLookup<ReadOnlySpan<byte>>();
        byBytes[Utf8(a)] = a;

        Assert.False(byBytes.TryGetValue(Utf8(b), out _));
        Assert.Same(a, byBytes[Utf8(a)]);

        byBytes[Utf8(b)] = b;
        Assert.Equal(2, table.Count);
        Assert.Same(a, byBytes[Utf8(a)]);
        Assert.Same(b, byBytes[Utf8(b)]);
    }
}
