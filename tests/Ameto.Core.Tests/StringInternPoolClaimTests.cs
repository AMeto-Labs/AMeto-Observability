using Ameto.Core;

namespace Ameto.Core.Tests;

/// <summary>
/// A CLAIM THAT RUNS OUT OF MEMORY LEAVES ITS INDEX RESOLVABLE (#126 review F5).
///
/// <para><see cref="StringInternPool"/> published a new string's index (<c>TryAdd</c>, whose
/// <c>GrowTable</c> runs after the key is linked) before it stored the index → string slot. An
/// OutOfMemoryException in between — the table's growth, or the slot array's doubling — left the
/// string mapped to an index whose slot stayed null, and <see cref="StringInternPool.Get"/> answers
/// <c>""</c> for it for the life of the pool. Log events carry their service only as that index:
/// the first batch of a newly deployed service, arriving during a heap peak and answered 503, made
/// every later event of that service unattributed until a restart. The slot is stored first now.</para>
/// </summary>
public sealed class StringInternPoolClaimTests
{
    [Fact]
    public void A_claim_that_runs_out_of_memory_leaves_its_index_resolvable_to_the_retry()
    {
        var pool   = new StringInternPool();
        int faults = 1;
        pool.OnClaimedForTest = () => { if (faults-- > 0) throw new OutOfMemoryException("injected: GrowTable"); };

        Assert.Throws<OutOfMemoryException>(() => pool.Intern("svc-new"u8));    // the first batch: answered 503
        int index = pool.Intern("svc-new"u8);                                     // the exporter's retry

        Assert.True(index >= 0);
        Assert.Equal("svc-new", pool.Get(index));
        Assert.Equal(index, pool.Intern("svc-new"));                              // and the string overload agrees
    }

    [Fact]
    public void A_claim_from_a_string_that_runs_out_of_memory_leaves_its_index_resolvable_too()
    {
        var pool   = new StringInternPool();
        int faults = 1;
        pool.OnClaimedForTest = () => { if (faults-- > 0) throw new OutOfMemoryException("injected: GrowTable"); };

        Assert.Throws<OutOfMemoryException>(() => pool.Intern("GET /orders {Id}"));
        int index = pool.Intern("GET /orders {Id}", out string canonical);

        Assert.Equal("GET /orders {Id}", pool.Get(index));
        Assert.Same(canonical, pool.Get(index));
    }
}
