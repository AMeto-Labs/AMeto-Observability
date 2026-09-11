using Ameto.Storage;

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
