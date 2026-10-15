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

    /// <summary>
    /// Round-5 L-14: recovery's text was counted against the budget without being refused, so a log an
    /// earlier version wrote — with more text in its pool than today's budget — left the pool past it
    /// after the replay, and every new template and service after the start went unpooled for the life
    /// of the process. Restored text is held outside the budget now; the budget bounds what is claimed.
    /// </summary>
    [Fact]
    public void Text_recovery_restored_past_the_budget_leaves_the_budget_to_new_text()
    {
        var pool = new StringInternPool(1024, maxPooledChars: 10);
        pool.ForceIntern(0, new string('a', 30));
        pool.ForceIntern(1, new string('b', 30));
        pool.ForceIntern(1, new string('c', 20));   // a later WAL's row over the same slot gives the first back

        int fresh = pool.Intern("new");
        Assert.True(fresh >= 0, "new text was refused: the restored text spent the budget");
        Assert.Equal("new", pool.Get(fresh));
        Assert.Equal(-1, pool.Intern("12345678"));  // 3 + 8 is past the budget of 10: it still bounds claims
        Assert.Equal(3, pool.PooledChars);
        Assert.Equal(50, pool.RestoredChars);
    }

    /// <summary>
    /// Round-5 L-14 (found beside it): ForceIntern mapped the string before it stored the slot, so a slot
    /// array's doubling that ran out of memory left the string mapped to a slot that still held another
    /// WAL's string — every live event of it after the start was filed under that one. The slot goes first
    /// now, as a claim's does (#126 review F5); the seam stands for the second step failing.
    /// </summary>
    [Fact]
    public void A_restore_whose_second_step_runs_out_of_memory_maps_no_string_to_another()
    {
        var pool = new StringInternPool();
        pool.ForceIntern(5, "Orders.Api");
        int faults = 1;
        pool.OnRestoredSlotStoredForTest = () => { if (faults-- > 0) throw new OutOfMemoryException("injected"); };

        Assert.Throws<OutOfMemoryException>(() => pool.ForceIntern(5, "Billing.Worker"));

        Assert.Equal("Billing.Worker", pool.Get(pool.Intern("Billing.Worker")));
        Assert.Equal("Orders.Api", pool.Get(pool.Intern("Orders.Api")));
    }
}
