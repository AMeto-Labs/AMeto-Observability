using System.Reflection;
using Ameto.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// The pool's id → string side is an array indexed by the id (one load, no dictionary),
/// grown under a lock while readers keep indexing whichever copy they hold; and its cap
/// is enforced on the id actually CLAIMED, because two threads missing together at the
/// boundary both pass a pre-increment check and one of them would otherwise be handed
/// an id past the last slot — which the growth loop could never reach, spinning for ever
/// under the lock with every later miss (ingest) queued behind it.
/// </summary>
public sealed class StringInternPoolSlotArrayTests
{
    private const int MaxPoolSize = 65536;   // mirrors the pool's private cap

    [Fact]
    public void Get_resolves_an_interned_id_and_answers_empty_for_anything_else()
    {
        var pool = new StringInternPool();
        int a = pool.Intern("alpha");
        int b = pool.Intern("beta");

        Assert.Equal("alpha", pool.Get(a));
        Assert.Equal("beta",  pool.Get(b));
        Assert.Equal(string.Empty, pool.Get(-1));                 // "no template"
        Assert.Equal(string.Empty, pool.Get(b + 1));              // never handed out, inside the array
        Assert.Equal(string.Empty, pool.Get(1_000_000));          // beyond the array
        Assert.Equal(string.Empty, pool.Get(int.MaxValue));
        Assert.Equal(string.Empty, pool.Get(int.MinValue));
    }

    [Fact]
    public void Growth_does_not_lose_a_store_and_readers_see_every_id_they_are_told_about()
    {
        var pool = new StringInternPool();
        const int n = 20_000;                                     // several doublings past the initial array

        // Readers hammer Get for ids the writer has announced, on whichever array copy they
        // get; a lost store or a torn growth shows up as an empty answer for a known id.
        int  announced = 0;
        bool stop      = false;
        var  failures  = new List<int>();
        var readers = Enumerable.Range(0, 3).Select(_ => Task.Run(() =>
        {
            var rng = new Random();
            while (!Volatile.Read(ref stop))
            {
                int upTo = Volatile.Read(ref announced);
                if (upTo == 0) continue;
                int id = rng.Next(0, upTo);
                if (pool.Get(id) != "t" + id)
                    lock (failures) failures.Add(id);
            }
        })).ToArray();

        for (int i = 0; i < n; i++)
        {
            int id = pool.Intern("t" + i);
            Assert.Equal(i, id);
            Volatile.Write(ref announced, i + 1);
        }
        Volatile.Write(ref stop, true);
        Task.WaitAll(readers);

        Assert.Empty(failures);
        for (int i = 0; i < n; i++) Assert.Equal("t" + i, pool.Get(i));
    }

    /// <summary>
    /// The growth loop's guard, exercised directly: with the cap as the loop's ceiling, an
    /// index at the cap can never be reached by doubling, so without the guard this call
    /// never returns. Reflection because the only production route to it is a two-thread
    /// race at the boundary, which the test below provokes but cannot force.
    /// </summary>
    [Fact]
    public void SetSlot_at_or_past_the_cap_returns_instead_of_growing_for_ever()
    {
        var pool    = new StringInternPool();
        var setSlot = typeof(StringInternPool).GetMethod("SetSlot", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.NotNull(setSlot);

        var done = Task.Run(() =>
        {
            setSlot.Invoke(pool, [MaxPoolSize,     "at the cap"]);
            setSlot.Invoke(pool, [MaxPoolSize + 1, "past the cap"]);
            setSlot.Invoke(pool, [int.MaxValue,    "far past"]);
        });
        Assert.True(done.Wait(TimeSpan.FromSeconds(20)), "SetSlot hung in the slot-array growth loop");
        Assert.Equal(string.Empty, pool.Get(MaxPoolSize));
        Assert.Equal(string.Empty, pool.Get(MaxPoolSize + 1));

        // ForceIntern goes through the same guard (recovery of a corrupt pool file).
        var forced = Task.Run(() => pool.ForceIntern(MaxPoolSize, "beyond"));
        Assert.True(forced.Wait(TimeSpan.FromSeconds(20)), "ForceIntern hung in the slot-array growth loop");
        Assert.Equal(-1, pool.Intern("anything new"));            // the counter is past the cap
    }

    /// <summary>
    /// Provokes the boundary race many times over: a fresh pool one id short of the cap,
    /// sixteen threads released together on distinct strings. Whichever thread claims
    /// past the cap must answer -1 and never touch the slot array; exactly one thread per
    /// round gets the last id. Timeout-guarded so a regression shows as a failure, not a
    /// hung test run.
    /// </summary>
    [Fact]
    public void Concurrent_misses_at_the_cap_never_hang_and_hand_out_exactly_one_last_id()
    {
        const int threads = 16, rounds = 200;
        for (int round = 0; round < rounds; round++)
        {
            var pool = new StringInternPool();
            pool.ForceIntern(MaxPoolSize - 2, "penultimate");   // counter now at 65535: one id left

            var barrier = new Barrier(threads);
            var results = new int[threads];
            var workers = Enumerable.Range(0, threads).Select(t => Task.Factory.StartNew(() =>
            {
                barrier.SignalAndWait();
                results[t] = pool.Intern("last-" + t);
            }, TaskCreationOptions.LongRunning)).ToArray();

            Assert.True(Task.WaitAll(workers, TimeSpan.FromSeconds(20)),
                $"round {round}: Intern hung — a claimant past the cap spun in the slot-array growth loop");

            Assert.Equal(1, results.Count(r => r == MaxPoolSize - 1));
            Assert.Equal(threads - 1, results.Count(r => r == -1));
            Assert.DoesNotContain(results, r => r >= MaxPoolSize);

            int winner = Array.IndexOf(results, MaxPoolSize - 1);
            Assert.Equal("last-" + winner, pool.Get(MaxPoolSize - 1));
            Assert.Equal(-1, pool.Intern("one-more"));            // saturated for good
        }
    }
}
