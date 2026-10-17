using System.Text;
using Ameto.Core;

namespace Ameto.Core.Tests;

/// <summary>
/// A request's share of the intern pool (round-6 review R5-M9). The pool keeps every text for the life of
/// the process, within its budget, and any request could spend the whole budget for every source; it adds
/// no more than its share now. A text the pool holds costs nothing of a share; one past it is answered as
/// on a full pool — its own string, index -1 — spending nothing and signalling nothing.
/// </summary>
public sealed class StringInternPoolShareTests
{
    [Fact]
    public void A_miss_within_a_share_is_pooled_and_taken_from_it()
    {
        var pool  = new StringInternPool();
        var share = new InternShare(texts: 2, chars: 10);

        Assert.True(pool.Intern("abc"u8, out _, ref share) >= 0);
        Assert.Equal((1, 7L), (share.Texts, share.Chars));
        Assert.True(pool.Intern("defgh", out _, ref share) >= 0);
        Assert.Equal((0, 2L), (share.Texts, share.Chars));
        Assert.Equal(8, pool.PooledChars);
    }

    [Fact]
    public void A_miss_past_a_share_is_answered_unpooled_and_spends_nothing()
    {
        var pool      = new StringInternPool();
        int signalled = 0;
        pool.PoolExhausted += _ => signalled++;

        var noTexts = new InternShare(texts: 0, chars: 1_000);
        Assert.Equal(-1, pool.Intern("abc"u8, out string kept, ref noTexts));
        Assert.Equal("abc", kept);

        var fewChars = new InternShare(texts: 10, chars: 2);
        Assert.Equal(-1, pool.Intern("abc", out string keptString, ref fewChars));
        Assert.Equal("abc", keptString);
        Assert.Equal((10, 2L), (fewChars.Texts, fewChars.Chars));   // nothing taken from it

        Assert.Equal(0, pool.PooledChars);                          // nothing spent of the pool
        Assert.Equal(0, signalled);                                 // and it has not run out
        Assert.False(pool.TryGet("abc".AsSpan(), out _, out _));
    }

    [Fact]
    public void A_text_the_pool_holds_costs_a_share_nothing()
    {
        var pool  = new StringInternPool();
        int index = pool.Intern("GET /orders {Id}");
        var spent = new InternShare(texts: 0, chars: 0);

        Assert.Equal(index, pool.Intern(Encoding.UTF8.GetBytes("GET /orders {Id}"), out string canonical, ref spent));
        Assert.Same(pool.Get(index), canonical);
        Assert.Equal(index, pool.Intern("GET /orders {Id}", out _, ref spent));
    }

    /// <summary>
    /// The bound: spending the pool's budget takes <see cref="StringInternPool.RequestShareDivisorTexts"/> requests' shares of
    /// new texts, or <see cref="StringInternPool.RequestShareDivisorChars"/> of new text — and a share always holds one text the
    /// pool takes, so a request can still pool its longest one.
    /// </summary>
    [Fact]
    public void A_request_share_is_a_fixed_part_of_the_pool_and_holds_its_longest_text()
    {
        var pool  = new StringInternPool();
        var share = pool.NewRequestShare();
        Assert.Equal(StringInternPool.DefaultMaxPoolSize / StringInternPool.RequestShareDivisorTexts, share.Texts);
        Assert.Equal(StringInternPool.DefaultMaxPooledChars / StringInternPool.RequestShareDivisorChars, share.Chars);
        Assert.True(share.Chars >= StringInternPool.DefaultMaxPooledTextChars, "a request's share cannot hold the longest text the pool takes");

        var small = new StringInternPool(maxPoolSize: 10, maxPooledChars: 100, maxTextChars: 40).NewRequestShare();
        Assert.Equal((1, 40L), (small.Texts, small.Chars));   // at least one text, as long as the pool takes
    }
}
