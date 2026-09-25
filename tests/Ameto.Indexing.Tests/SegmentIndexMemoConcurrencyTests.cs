using System.Collections.Concurrent;
using Ameto.Core;
using Ameto.Indexing;

namespace Ameto.Indexing.Tests;

/// <summary>
/// The query cache's entries are MEMOS shared by every query that touches a group, grown while
/// leased, charged at release, and evicted — by budget, by growth, by a RAM-pressure shed —
/// while other queries may still be answering through them. These tests pin what must survive
/// that: every answer equals the decoded index's, a memo evicted under a lease keeps answering
/// that lease and is never charged again, two queries that miss one bucket at once keep ONE
/// answer and charge it once, the budget's bookkeeping returns to zero, and every use of a
/// group is counted exactly once, as a hit (no section read) or a miss.
///
/// <para>Races are forced through seams (<see cref="SegmentIndexReader.BeforeRemember"/>, events
/// that park a thread inside the window) or run as a stress with a start barrier — never timed
/// with sleeps.</para>
/// </summary>
public sealed class SegmentIndexMemoConcurrencyTests
{
    // ── Fixture: a few groups' sections, and the decoded answers to a fixed set of questions ──

    private sealed class Group
    {
        public required string Path;
        public required byte[] Inverted, Trigram, Bloom;
        public required object?[] Expected;        // per question: uint[]? or bool
    }

    private enum Ask { Lookup, MightContain, Intersect, Trigram, BloomValue }

    private readonly record struct Question(Ask Kind, string Property, object? Value, (string, object?)[]? Predicates = null);

    private static readonly Question[] Questions =
    [
        new(Ask.Lookup,       "Customer", "cust-3"),
        new(Ask.MightContain, "Customer", "cust-3"),
        new(Ask.MightContain, "Customer", "nobody"),
        new(Ask.Intersect,    "",         null, [("Customer", "cust-3"), ("@l", "Error")]),
        new(Ask.Intersect,    "",         null, [("@l", "error")]),
        new(Ask.Intersect,    "",         null, [("Nope", "x")]),
        new(Ask.Lookup,       "k",        17L),
        new(Ask.Trigram,      "",         "timeout"),
        new(Ask.Trigram,      "",         "shipped to cust-1"),
        new(Ask.Trigram,      "",         "zzz"),
        new(Ask.BloomValue,   "",         "cust-7"),
        new(Ask.BloomValue,   "",         "nobody-at-all"),
    ];

    private static Group[] BuildGroups(int count)
    {
        var groups = new Group[count];
        for (int g = 0; g < count; g++)
        {
            var rng   = new Random(80 + g);
            var inv   = new SegmentInvertedIndex();
            var tri   = new SegmentTrigramIndex();
            var bloom = SegmentBloomFilter.Create(4096);
            for (uint o = 0; o < 400; o++)
            {
                string customer = "cust-" + rng.Next(20);
                string level    = o % 5 == 0 ? "Error" : "Information";
                inv.Add(o, "Customer", customer); bloom.Add(customer);
                inv.Add(o, "@l", level);          bloom.Add(level);
                inv.Add(o, "k", (long)o);         bloom.Add(o.ToString(System.Globalization.CultureInfo.InvariantCulture));
                tri.Add(o, $"order {o} shipped to {customer}" + (o % 7 == g ? " after timeout" : ""));
            }
            byte[] invBytes = inv.Serialise(), triBytes = tri.Serialise(), bloomBytes = bloom.Serialise();
            bloom.Dispose();

            // The reference: the decoded index and the bloom itself, asked single-threaded.
            var oracleInv = SegmentInvertedIndex.Deserialise(invBytes);
            var oracleTri = SegmentTrigramIndex.Deserialise(triBytes);
            using var oracleBloom = SegmentBloomFilter.Deserialise(bloomBytes);
            var expected = new object?[Questions.Length];
            for (int q = 0; q < Questions.Length; q++)
            {
                var x = Questions[q];
                expected[q] = x.Kind switch
                {
                    Ask.Lookup       => oracleInv.Lookup(x.Property, x.Value),
                    Ask.MightContain => SegmentIndexReader.MightContainValue(oracleBloom, x.Value) && oracleInv.MightContain(x.Property, x.Value),
                    Ask.Intersect    => oracleInv.LookupIntersect(x.Predicates!),
                    Ask.Trigram      => oracleTri.Lookup((string)x.Value!),
                    _                => SegmentIndexReader.MightContainValue(oracleBloom, x.Value),
                };
            }
            groups[g] = new Group { Path = $"g{g}.seg", Inverted = invBytes, Trigram = triBytes, Bloom = bloomBytes, Expected = expected };
        }
        return groups;
    }

    private static SegmentIndexView Open(SegmentIndexCache? cache, Group g) =>
        SegmentIndexView.OverSections(cache, g.Path, 0, g.Inverted, g.Trigram, g.Bloom);

    private static object? AskIt(SegmentIndexView v, in Question x) => x.Kind switch
    {
        Ask.Lookup       => v.Lookup(x.Property, x.Value),
        Ask.MightContain => v.MightContain(x.Property, x.Value),
        Ask.Intersect    => v.LookupIntersect(x.Predicates!),
        Ask.Trigram      => v.LookupTrigram((string)x.Value!),
        _                => v.MightContainValue(x.Value),
    };

    private static bool Same(object? expected, object? actual) => (expected, actual) switch
    {
        (null, null)              => true,
        (uint[] e, uint[] a)      => e.AsSpan().SequenceEqual(a),
        (bool e, bool a)          => e == a,
        _                         => false,
    };

    // ── The stress: readers, eviction by budget and growth, and a shedder, all at once ──

    /// <summary>
    /// Four readers ask random questions of six groups through a cache whose budget holds only a
    /// couple of memos, so entries are evicted by budget and by their own growth while leased,
    /// and a fifth thread sheds the whole cache in a loop. Every answer must be the decoded
    /// index's; afterwards the bookkeeping must return to zero, and every view must have counted
    /// exactly one hit or miss.
    /// </summary>
    [Fact]
    public void Readers_racing_eviction_and_shed_always_get_the_decoded_answer()
    {
        var groups = BuildGroups(6);
        var cache  = new SegmentIndexCache(8 * 1024);
        const int Readers = 4, ViewsPerReader = 3_000;

        var failures = new ConcurrentQueue<string>();
        using var start = new Barrier(Readers + 1);
        int readersLeft = Readers;
        long shed = 0;

        var threads = new List<Thread>();
        for (int t = 0; t < Readers; t++)
        {
            int seed = t;
            threads.Add(new Thread(() =>
            {
                var rng = new Random(seed);
                start.SignalAndWait();
                try
                {
                    for (int i = 0; i < ViewsPerReader; i++)
                    {
                        var g = groups[rng.Next(groups.Length)];
                        using var v = Open(cache, g);
                        int asks = 1 + rng.Next(3);
                        for (int a = 0; a < asks; a++)
                        {
                            int q = rng.Next(Questions.Length);
                            var got = AskIt(v, Questions[q]);
                            if (!Same(g.Expected[q], got)) failures.Enqueue($"{g.Path} question {q}");
                        }
                    }
                }
                catch (Exception ex) { failures.Enqueue(ex.ToString()); }
                finally { Interlocked.Decrement(ref readersLeft); }
            }));
        }
        threads.Add(new Thread(() =>
        {
            start.SignalAndWait();
            while (Volatile.Read(ref readersLeft) > 0) { cache.Shed(); shed++; }
        }));

        foreach (var th in threads) th.Start();
        foreach (var th in threads) Assert.True(th.Join(TimeSpan.FromMinutes(2)), "a thread never finished");

        Assert.True(failures.IsEmpty, string.Join(Environment.NewLine, failures.Take(10)));
        Assert.True(shed > 0);
        Assert.Equal((long)Readers * ViewsPerReader, cache.HitCount + cache.MissCount);

        // Whatever is still listed accounts for exactly what TotalBytes says: shedding it all
        // must land on zero, not on a residue of growth charged to entries no longer listed.
        cache.Shed();
        Assert.Equal(0, cache.EntryCount);
        Assert.Equal(0, cache.TotalBytes);
        Assert.Equal(0, cache.NativeBytes);
    }

    // ── Sequenced races ───────────────────────────────────────────────────────

    /// <summary>
    /// A memo shed while a query holds it keeps answering that query — from what it knows and
    /// from the sections the query lends it — and what it learns afterwards is not charged to a
    /// cache it is no longer in. The next query gets a fresh memo, and misses.
    /// </summary>
    [Fact]
    public void A_memo_shed_under_a_lease_keeps_answering_it_and_is_never_charged_again()
    {
        var g     = BuildGroups(1)[0];
        var cache = new SegmentIndexCache(1 << 20);

        var v = Open(cache, g);
        Assert.True(Same(g.Expected[0], AskIt(v, Questions[0])));
        var memo = v.Index;

        Assert.Equal(1, cache.EntryCount);
        cache.Shed();
        Assert.Equal(0, cache.TotalBytes);

        // Still answers, the known question and a new one.
        Assert.True(Same(g.Expected[0], AskIt(v, Questions[0])));
        Assert.True(Same(g.Expected[7], AskIt(v, Questions[7])));
        v.Dispose();

        Assert.Equal(0, cache.TotalBytes);                 // the growth went nowhere
        Assert.Equal(0, cache.EntryCount);

        using var next = Open(cache, g);
        Assert.NotSame(memo, next.Index);
        Assert.True(Same(g.Expected[0], AskIt(next, Questions[0])));
        Assert.True(next.ReadSections);                    // a fresh memo: a miss
    }

    /// <summary>
    /// Two queries miss the same bucket at once. The seam parks the first between its scan and
    /// remembering the answer; the second answers and remembers; the first then finds the
    /// bucket taken. Both answers are right, the memo holds one, and it is charged once — the
    /// same bytes a single query asking alone would have cost.
    /// </summary>
    [Theory]
    [InlineData(0)]    // an inverted bucket
    [InlineData(7)]    // trigram postings
    [InlineData(10)]   // a bloom verdict
    public void Two_queries_missing_one_bucket_at_once_keep_one_answer_and_charge_it_once(int question)
    {
        var g = BuildGroups(1)[0];

        // What asking alone costs.
        var solo = new SegmentIndexCache(1 << 20);
        using (var v = Open(solo, g)) Assert.True(Same(g.Expected[question], AskIt(v, Questions[question])));
        long soloBytes = solo.TotalBytes;

        var cache = new SegmentIndexCache(1 << 20);
        using (Open(cache, g)) { }                           // create the (empty) memo
        SegmentIndexReader memo;
        using (var peek = Open(cache, g)) memo = peek.Index;

        using var parked   = new ManualResetEventSlim();
        using var released = new ManualResetEventSlim();
        int calls = 0;
        memo.BeforeRemember = () =>
        {
            if (Interlocked.Increment(ref calls) != 1) return;   // park the first asker only
            parked.Set();
            released.Wait();
        };

        object? first = null;
        var firstThread = new Thread(() =>
        {
            using var v = Open(cache, g);
            first = AskIt(v, Questions[question]);
        });
        firstThread.Start();
        Assert.True(parked.Wait(TimeSpan.FromMinutes(1)), "the first asker never reached the seam");

        object? second;
        using (var v = Open(cache, g)) second = AskIt(v, Questions[question]);
        released.Set();
        Assert.True(firstThread.Join(TimeSpan.FromMinutes(1)));
        memo.BeforeRemember = null;

        Assert.True(Same(g.Expected[question], first));
        Assert.True(Same(g.Expected[question], second));
        Assert.Equal(soloBytes, cache.TotalBytes);
        Assert.Equal(memo.ApproxRetainedBytes, cache.TotalBytes);

        // And the memo now answers without a section.
        using var again = Open(cache, g);
        Assert.True(Same(g.Expected[question], AskIt(again, Questions[question])));
        Assert.False(again.ReadSections);
    }

    /// <summary>
    /// Two leases on one memo; the first to release charges growth that no longer fits, which
    /// unlists the entry while the second lease still holds it. The second keeps answering, and
    /// its release is what lets the memo go.
    /// </summary>
    [Fact]
    public void Growth_that_no_longer_fits_evicts_the_memo_under_its_other_lease()
    {
        var g = BuildGroups(1)[0];
        long empty;
        using (var probe = SegmentIndexView.OverSections(null, "x", 0, g.Inverted, g.Trigram, g.Bloom))
            empty = probe.Index.ApproxRetainedBytes;
        var cache = new SegmentIndexCache(empty + 64);        // room for an empty memo, not a taught one

        var a = Open(cache, g);
        var b = Open(cache, g);
        Assert.Same(a.Index, b.Index);

        Assert.True(Same(g.Expected[3], AskIt(a, Questions[3])));
        a.Dispose();                                          // charges the growth → over budget → unlisted
        Assert.Equal(0, cache.EntryCount);
        Assert.Equal(0, cache.TotalBytes);

        Assert.True(Same(g.Expected[3], AskIt(b, Questions[3])));   // the memo is still whole under b
        Assert.True(Same(g.Expected[8], AskIt(b, Questions[8])));
        b.Dispose();
        Assert.Equal(0, cache.TotalBytes);
    }

    /// <summary>
    /// The same path over different bytes: the entry taught from the old bytes is replaced, not
    /// consulted — while a query that still holds it keeps its answers about the old bytes, which
    /// is what it opened.
    /// </summary>
    [Fact]
    public void Different_bytes_under_one_path_get_a_fresh_memo_and_the_old_lease_keeps_its_own()
    {
        var groups = BuildGroups(2);
        var (oldBytes, newBytes) = (groups[0], groups[1]);
        var cache  = new SegmentIndexCache(1 << 20);
        var oldFp  = new IndexGroupFingerprint(1, default, oldBytes.Inverted.Length, default);
        var newFp  = new IndexGroupFingerprint(1, default, newBytes.Inverted.Length + 1, default);

        var held = SegmentIndexView.OverSections(cache, "same.seg", 0, oldBytes.Inverted, oldBytes.Trigram, oldBytes.Bloom, oldFp);
        Assert.True(Same(oldBytes.Expected[0], AskIt(held, Questions[0])));

        using (var next = SegmentIndexView.OverSections(cache, "same.seg", 0, newBytes.Inverted, newBytes.Trigram, newBytes.Bloom, newFp))
        {
            Assert.NotSame(held.Index, next.Index);
            for (int q = 0; q < Questions.Length; q++)
                Assert.True(Same(newBytes.Expected[q], AskIt(next, Questions[q])), $"new bytes, question {q}");
        }
        Assert.Equal(1, cache.StaleReplacedCount);

        Assert.True(Same(oldBytes.Expected[0], AskIt(held, Questions[0])));   // still the old file's answer
        held.Dispose();

        using var again = SegmentIndexView.OverSections(cache, "same.seg", 0, newBytes.Inverted, newBytes.Trigram, newBytes.Bloom, newFp);
        Assert.True(Same(newBytes.Expected[0], AskIt(again, Questions[0])));
        Assert.False(again.ReadSections);                                      // the new memo, kept
        Assert.Equal(1, cache.EntryCount);
    }

    // ── What a hit is ─────────────────────────────────────────────────────────

    /// <summary>
    /// On the query path a hit is a group answered without reading any of its sections. An entry
    /// that exists but has not been asked this question is a miss — which is the honest reading
    /// of "the cache saved this query nothing" — and so is the first use of a group.
    /// </summary>
    [Fact]
    public void A_view_counts_a_hit_only_when_it_read_no_section()
    {
        var g     = BuildGroups(1)[0];
        var cache = new SegmentIndexCache(1 << 20);

        void Use(int question, bool expectRead)
        {
            using var v = Open(cache, g);
            Assert.True(Same(g.Expected[question], AskIt(v, Questions[question])));
            Assert.Equal(expectRead, v.ReadSections);
        }

        Use(0,  expectRead: true);    // first sight of the group
        Use(0,  expectRead: false);   // the same bucket: from the memo
        Use(7,  expectRead: true);    // a trigram it has not looked for
        Use(7,  expectRead: false);
        Use(10, expectRead: true);    // a bloom verdict it has not given
        Use(10, expectRead: false);
        using (Open(cache, g)) { }    // a view that asks nothing reads nothing: a hit

        Assert.Equal(4, cache.HitCount);
        Assert.Equal(3, cache.MissCount);
    }

    /// <summary>A memo owns no section, so asked directly — not through a view — it has nothing
    /// to answer with, and says so rather than answering "no information".</summary>
    [Fact]
    public void A_memo_asked_outside_a_view_refuses()
    {
        var cache = new SegmentIndexCache(1 << 20);
        var g = BuildGroups(1)[0];
        SegmentIndexReader memo;
        using (var v = Open(cache, g)) memo = v.Index;

        Assert.Throws<InvalidOperationException>(() => memo.Lookup("Customer", "cust-3"));
        Assert.Throws<InvalidOperationException>(() => memo.LookupTrigram("timeout"));
        Assert.Throws<InvalidOperationException>(() => memo.Bloom);
        Assert.Equal(0, memo.ApproxNativeBytes);
    }
}
