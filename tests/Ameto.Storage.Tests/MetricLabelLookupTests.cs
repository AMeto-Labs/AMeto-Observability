using System.Text;
using Ameto.Core;
using Ameto.Metrics;

namespace Ameto.Storage.Tests;

/// <summary>
/// The lookup-only half of the interners (WP7 review, F1): <see cref="StringInternPool.TryGet(ReadOnlySpan{byte}, out string?, out int)"/>
/// and <see cref="MetricLabelInterner.Lookup"/> / <see cref="MetricLabelInterner.LookupLabelSet"/>
/// answer what the pool holds and never add, claim or signal — and decode exactly as interning does.
/// </summary>
public sealed class MetricLabelLookupTests
{
    [Fact]
    public void A_lookup_miss_decodes_as_interning_would_and_adds_nothing()
    {
        var interner = new MetricLabelInterner(64, 16);
        byte[][] inputs =
        [
            "plain"u8.ToArray(),
            Encoding.UTF8.GetBytes("Ж中é"),
            [0x61, 0xFF, 0x62, 0xC3],                 // invalid sequences: one U+FFFD each
            [0xED, 0xA0, 0x80],                       // an encoded lone surrogate
        ];
        foreach (var utf8 in inputs)
        {
            Assert.Equal(-1, interner.Lookup(utf8, out string missed));
            Assert.Equal(Encoding.UTF8.GetString(utf8), missed);
        }
        Assert.Equal(0, interner.Strings.ClaimedCount);

        foreach (var utf8 in inputs)
        {
            int id = interner.Intern(utf8, out string pooled);
            Assert.Equal(id, interner.Lookup(utf8, out string found));
            Assert.Same(pooled, found);
        }

        Assert.Equal(MetricLabelInterner.EmptyStringId, interner.Lookup([], out string empty));
        Assert.Same(string.Empty, empty);
    }

    [Fact]
    public void A_lookup_on_a_full_pool_neither_claims_nor_signals()
    {
        var pool = new StringInternPool(2);
        int signals = 0;
        pool.PoolExhausted += _ => signals++;
        pool.Intern("a"u8);
        pool.Intern("b"u8);

        Assert.False(pool.TryGet("c"u8, out _, out int index));
        Assert.Equal(-1, index);
        Assert.Equal(0, signals);                  // Intern("c") would have fired it
        Assert.True(pool.TryGet("a"u8, out string? a, out int aIndex));
        Assert.Equal("a", a);
        Assert.Equal(0, aIndex);
        Assert.False(pool.TryGet(ReadOnlySpan<byte>.Empty, out _, out _));
        Assert.Equal(2, pool.ClaimedCount);
    }

    [Fact]
    public void A_label_set_lookup_publishes_nothing()
    {
        var interner = new MetricLabelInterner(64, 16);
        int k = interner.Intern("k", out string key), v = interner.Intern("v", out string value);
        int z = interner.Intern("z", out string zKey), one = interner.Intern("1", out string oneValue);

        var looked = interner.LookupLabelSet(new[] { zKey, oneValue, key, value }, new[] { z, one, k, v });   // pairs in any order: sorted
        Assert.Equal(new LabelSet([new("k", "v"), new("z", "1")]), looked);
        Assert.Same(key, looked.KeyAt(0));
        Assert.NotSame(interner.LookupLabelSet(new[] { key, value }, new[] { k, v }),
                       interner.LookupLabelSet(new[] { key, value }, new[] { k, v }));          // a miss is not published

        var published = interner.GetLabelSet(new[] { key, value }, new[] { k, v });
        Assert.Same(published, interner.LookupLabelSet(new[] { key, value }, new[] { k, v }));   // a hit is shared
        Assert.Same(LabelSet.Empty, interner.LookupLabelSet([], []));
    }
}
