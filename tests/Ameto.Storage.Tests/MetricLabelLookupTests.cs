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

    /// <summary>
    /// WHAT INGEST PUBLISHES, THE COLD READER FINDS (PR #84 review, #14) — across many shapes, not one.
    /// <see cref="MetricLabelInterner.GetLabelSet"/> and <see cref="MetricLabelInterner.LookupLabelSet"/>
    /// were two copies of one probe; a hash or slot change in either would make every cold read build
    /// fresh label sets with no test failing. They are one probe now; this pins the agreement over a
    /// thousand sets of one to four pairs, each looked up with its pairs in another order, right after
    /// it was published (so no later publish can have displaced it).
    /// </summary>
    [Fact]
    public void Every_set_ingest_publishes_is_the_one_a_lookup_finds()
    {
        var interner = new MetricLabelInterner(4_096, MetricLabelInterner.DefaultLabelSetSlots);
        var rng = new Random(20260924);
        for (int n = 0; n < 1_000; n++)
        {
            int pairs = 1 + n % 4;
            var kv  = new string[2 * pairs];
            var ids = new int[2 * pairs];
            for (int p = 0; p < pairs; p++)
            {
                ids[2 * p]     = interner.Intern($"key-{p}-{rng.Next(8)}", out kv[2 * p]);
                ids[2 * p + 1] = interner.Intern($"value-{rng.Next(512)}", out kv[2 * p + 1]);
            }

            // The lookup gets the pairs reversed: both sort in place, so the order must not matter.
            var lkv  = new string[kv.Length];
            var lids = new int[ids.Length];
            for (int p = 0; p < pairs; p++)
            {
                int q = pairs - 1 - p;
                (lkv[2 * q], lkv[2 * q + 1])   = (kv[2 * p], kv[2 * p + 1]);
                (lids[2 * q], lids[2 * q + 1]) = (ids[2 * p], ids[2 * p + 1]);
            }

            var published = interner.GetLabelSet(kv, ids);
            Assert.Same(published, interner.LookupLabelSet(lkv, lids));
        }
    }
}
