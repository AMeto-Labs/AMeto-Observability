using Ameto.Core;

namespace Ameto.Storage.Tests;

public sealed class StringInternPoolTests
{
    [Fact]
    public void Intern_SameString_ReturnsSameIndex()
    {
        var pool = new StringInternPool();
        int a = pool.Intern("Hello {Name}");
        int b = pool.Intern("Hello {Name}");
        Assert.Equal(a, b);
    }

    [Fact]
    public void Intern_DifferentStrings_ReturnsDifferentIndices()
    {
        var pool = new StringInternPool();
        int a = pool.Intern("Template A");
        int b = pool.Intern("Template B");
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Get_ReturnsInternedString()
    {
        var pool = new StringInternPool();
        int idx = pool.Intern("Hello {World}");
        Assert.Equal("Hello {World}", pool.Get(idx));
    }

    [Fact]
    public void Get_NegativeIndex_ReturnsEmpty()
    {
        var pool = new StringInternPool();
        Assert.Equal(string.Empty, pool.Get(-1));
    }

    [Fact]
    public void Get_UnknownIndex_ReturnsEmpty()
    {
        var pool = new StringInternPool();
        Assert.Equal(string.Empty, pool.Get(9999));
    }

    [Fact]
    public void Clear_ResetsPool()
    {
        var pool = new StringInternPool();
        int idx = pool.Intern("Template");
        pool.Clear();
        // After clear, the same string gets a new index (starting from 0)
        int newIdx = pool.Intern("Template");
        Assert.Equal(0, newIdx);
    }

    [Fact]
    public void Intern_OutCanonical_ReturnsThePoolsOwnInstance()
    {
        var pool  = new StringInternPool();
        string first = "Order {OrderId} shipped";
        int idx = pool.Intern(first);

        // A distinct-but-equal instance, exactly as a wire decode produces per event.
        string fresh = new string(first.AsSpan());
        Assert.False(ReferenceEquals(first, fresh));

        int idx2 = pool.Intern(fresh, out string canonical);
        Assert.Equal(idx, idx2);
        Assert.Same(pool.Get(idx), canonical);
        Assert.False(ReferenceEquals(fresh, canonical));
    }

    [Fact]
    public void Intern_Utf8OutCanonical_ReturnsThePoolsOwnInstance()
    {
        var pool = new StringInternPool();
        int idx  = pool.Intern("Hello {Name}");

        int idx2 = pool.Intern("Hello {Name}"u8, out string canonical);
        Assert.Equal(idx, idx2);
        Assert.Same(pool.Get(idx), canonical);

        // First sighting via the UTF-8 overload must also come back as the pooled instance.
        int idx3 = pool.Intern("Fresh {Value}"u8, out string canonical3);
        Assert.Same(pool.Get(idx3), canonical3);
        Assert.Equal("Fresh {Value}", canonical3);
    }

    [Fact]
    public void Intern_OutCanonical_EmptyUtf8_IsEmptyStringAndMinusOne()
    {
        var pool = new StringInternPool();
        int idx  = pool.Intern(ReadOnlySpan<byte>.Empty, out string canonical);
        Assert.Equal(-1, idx);
        Assert.Equal(string.Empty, canonical);
    }

    /// <summary>
    /// The window that made this a blocker: TryAdd publishes the KEY (and so the index)
    /// before the index→string map is written, so a thread that loses the race and resolves
    /// the canonical through Get() sees string.Empty for a perfectly valid index. The caller
    /// then attaches "" to the event; the hot tier prefers an attached string over the pool
    /// and its ?? does not catch an empty one, so the event is served with NO template, and
    /// the WAL pool row is skipped because the template is empty.
    ///
    /// <para>Threads are released together on a barrier and all intern the SAME fresh name,
    /// so exactly one wins and the rest take the lose path — the one this exercises.</para>
    /// </summary>
    [Fact]
    public void Intern_ConcurrentFirstSighting_NeverAnswersAnEmptyCanonical()
    {
        const int threads = 8;
        const int names   = 64;
        const int rounds  = 200;

        int emptyCanonical = 0;
        int wrongCanonical = 0;
        int indexMismatch  = 0;

        for (int round = 0; round < rounds; round++)
        {
            var pool    = new StringInternPool();
            var barrier = new Barrier(threads);

            Parallel.For(0, threads, _ =>
            {
                barrier.SignalAndWait();
                for (int n = 0; n < names; n++)
                {
                    string name = $"Template {{Arg{n}}} round {round}";

                    int idxA = pool.Intern(name, out string canonicalA);
                    if (idxA >= 0)
                    {
                        if (canonicalA.Length == 0)       Interlocked.Increment(ref emptyCanonical);
                        else if (canonicalA != name)      Interlocked.Increment(ref wrongCanonical);
                    }

                    int idxB = pool.Intern(System.Text.Encoding.UTF8.GetBytes(name), out string canonicalB);
                    if (idxB >= 0)
                    {
                        if (canonicalB.Length == 0)       Interlocked.Increment(ref emptyCanonical);
                        else if (canonicalB != name)      Interlocked.Increment(ref wrongCanonical);
                    }

                    if (idxA != idxB) Interlocked.Increment(ref indexMismatch);
                }
            });

            // Once every thread is done, every index must resolve to its template.
            for (int n = 0; n < names; n++)
            {
                string name = $"Template {{Arg{n}}} round {round}";
                int    idx  = pool.Intern(name, out string canonical);
                Assert.True(idx >= 0);
                Assert.Equal(name, canonical);
                Assert.Equal(name, pool.Get(idx));
            }
        }

        Assert.Equal(0, emptyCanonical);
        Assert.Equal(0, wrongCanonical);
        Assert.Equal(0, indexMismatch);
    }

    /// <summary>
    /// Every racing thread must end up sharing ONE instance per name — the point of the
    /// canonical overloads. A loser that returned its own materialised copy would pass the
    /// emptiness check above and still leak one duplicate string per event into the tier.
    /// </summary>
    [Fact]
    public void Intern_ConcurrentFirstSighting_AllThreadsShareOneInstance()
    {
        const int threads = 8;
        var pool     = new StringInternPool();
        var barrier  = new Barrier(threads);
        var observed = new System.Collections.Concurrent.ConcurrentBag<string>();

        Parallel.For(0, threads, _ =>
        {
            barrier.SignalAndWait();
            pool.Intern(new string("shared {Template} instance".AsSpan()), out string canonical);
            observed.Add(canonical);
        });

        string first = observed.First();
        Assert.All(observed, s => Assert.Same(first, s));
        Assert.Same(first, pool.Get(pool.Intern("shared {Template} instance")));
    }

    [Fact]
    public void Intern_IndicesAreSequential()
    {
        var pool = new StringInternPool();
        int a = pool.Intern("A");
        int b = pool.Intern("B");
        int c = pool.Intern("C");
        Assert.Equal(0, a);
        Assert.Equal(1, b);
        Assert.Equal(2, c);
    }

    [Fact]
    public void Intern_ConcurrentCalls_SameStringGetsSameIndex()
    {
        var pool    = new StringInternPool();
        const int N = 100;
        var results = new int[N];

        Parallel.For(0, N, i =>
        {
            results[i] = pool.Intern("SharedTemplate");
        });

        Assert.True(results.All(r => r == results[0]));
    }
}
