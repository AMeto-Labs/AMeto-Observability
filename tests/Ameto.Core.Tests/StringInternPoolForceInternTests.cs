using Ameto.Core;

namespace Ameto.Core.Tests;

/// <summary>
/// <see cref="StringInternPool.ForceIntern"/> is how a start puts an orphaned WAL's pool rows back:
/// templates and, since log WAL v5, services. Each WAL's rows are its own process's numbering, so
/// two WALs left by DIFFERENT processes can give one index two strings — the replay restores and
/// flushes one WAL at a time, so each is resolved against its own rows, but the pool the server
/// then runs on keeps what the last of them left.
///
/// <para>What it kept was wrong. Overwriting a slot left the previous string's reverse entry
/// pointing at it, so the first live event naming that string was handed an index that now
/// resolves to the other one. A template survives that — the tier keeps the text it was handed —
/// but a service is the index alone: every live event of the first WAL's service would be stored,
/// shown and counted under the second WAL's, until the next restart.</para>
/// </summary>
public sealed class StringInternPoolForceInternTests
{
    [Fact]
    public void A_string_whose_slot_a_later_row_took_is_given_a_fresh_index()
    {
        var pool = new StringInternPool();
        pool.ForceIntern(5, "Orders.Api");      // one orphaned WAL's row
        pool.ForceIntern(5, "Billing.Worker");  // another's, from another process, same index

        Assert.Equal("Billing.Worker", pool.Get(5));
        Assert.Equal(5, pool.Intern("Billing.Worker"));

        int orders = pool.Intern("Orders.Api");
        Assert.NotEqual(5, orders);
        Assert.Equal("Orders.Api", pool.Get(orders));
        Assert.True(orders > 5, "a fresh index is past every restored one");
    }

    [Fact]
    public void The_same_row_twice_and_a_string_moving_to_another_slot_keep_every_answer_true()
    {
        var pool = new StringInternPool();
        pool.ForceIntern(2, "Orders.Api");
        pool.ForceIntern(2, "Orders.Api");      // a second WAL of the SAME process: same numbering
        Assert.Equal(2, pool.Intern("Orders.Api"));

        pool.ForceIntern(7, "Orders.Api");      // the string at another index too
        Assert.Equal("Orders.Api", pool.Get(2));   // what index 2 said stays true
        Assert.Equal("Orders.Api", pool.Get(7));
        Assert.Equal("Orders.Api", pool.Get(pool.Intern("Orders.Api")));
    }

    [Fact]
    public void The_utf8_lookup_sees_the_same_answers()
    {
        var pool = new StringInternPool();
        pool.ForceIntern(3, "Платежи.Шлюз");
        pool.ForceIntern(3, "Orders.Api");

        int svc = pool.Intern("Платежи.Шлюз"u8);
        Assert.NotEqual(3, svc);
        Assert.Equal("Платежи.Шлюз", pool.Get(svc));
        Assert.False(pool.TryGet("Платежи.Шлюз".AsSpan(), out _, out int stale) && stale == 3);
    }
}
