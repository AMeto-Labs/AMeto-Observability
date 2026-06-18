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
