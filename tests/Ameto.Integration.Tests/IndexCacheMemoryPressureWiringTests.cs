using Microsoft.Extensions.DependencyInjection;
using Ameto.Core;
using Ameto.Indexing;

namespace Ameto.Integration.Tests;

/// <summary>
/// The two halves of the index cache's memory story only work if the real composition root wires
/// them, and neither is visible from inside <c>Ameto.Indexing</c>: the cache takes whatever
/// ceilings it is handed, and it is shed only if someone registered it.
///
/// <para>Both were unreachable before. The native bloom bits were charged against a budget
/// derived from the MANAGED heap limit, and <c>RamPressureService</c> — which flushes the hot
/// tier, forces a collection and trims the working set — never touched the cache at all, because
/// it lives in <c>Ameto.Storage</c> and the cache lives in an assembly that references it. A unit
/// test of the cache passes with the production wiring deleted; this one does not.</para>
/// </summary>
public sealed class IndexCacheMemoryPressureWiringTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;

    public IndexCacheMemoryPressureWiringTests(AmetoWebAppFactory factory)
    {
        _factory = factory;
        _factory.CreateClient().Dispose();   // start the host
    }

    [Fact]
    public void The_hosted_cache_is_built_with_a_native_ceiling_of_its_own()
    {
        var cache = _factory.Services.GetRequiredService<SegmentIndexCache>();

        Assert.True(cache.NativeBudgetBytes > 0,
            "AddAmetoQuery must hand the cache Query.EffectiveIndexCacheNativeBytes — without it "
          + "native bloom bits are bounded only by a budget taken of the managed-heap limit, "
          + "which is not where they live");
        Assert.Equal(new QueryOptions().EffectiveIndexCacheNativeBytes, cache.NativeBudgetBytes);

        // The native ceiling bounds where bytes LIVE, so it is anchored to the PHYSICAL limit and
        // never to the managed budget alone: it is the derived backstop, which an explicitly
        // configured Query.IndexCacheBytes may raise — but never past
        // IndexCacheNativeMaxFraction of that limit, because a budget says how much this
        // component may hold and only the host says how much may be pinned beyond the GC's reach.
        // This fixture configures nothing, so the hosted cache must sit on the backstop itself.
        var budgets = MemoryBudgets.Current();
        Assert.Equal(budgets.IndexCacheNativeBytes, cache.NativeBudgetBytes);

        // And the rule holds for any budget an operator could set — which is what the clamp is for.
        long hostCeiling = Math.Max(
            budgets.IndexCacheNativeBytes,
            (long)(budgets.PhysicalLimitBytes * MemoryBudgets.IndexCacheNativeMaxFraction));
        Assert.True(cache.NativeBudgetBytes <= hostCeiling);
        Assert.True(new QueryOptions { IndexCacheBytes = 64L << 30 }.EffectiveIndexCacheNativeBytes <= hostCeiling,
            "a configured budget may raise the native ceiling, but never past the host's share");
    }

    [Fact]
    public void The_hosted_cache_is_registered_for_memory_pressure()
    {
        var cache = _factory.Services.GetRequiredService<SegmentIndexCache>();

        // A live host must have put its cache where RamPressureService can find it. Asserted as
        // "at least one", not "exactly one": other suites in this assembly run their own hosts in
        // parallel, and each registers its own cache.
        Assert.True(MemoryShedRegistry.RegisteredCount >= 1,
            "the running host's SegmentIndexCache should be registered with MemoryShedRegistry — "
          + "it holds native memory no collection can reclaim, in the one component the RAM "
          + "pressure path cannot reference directly");

        // And the registry's view of it is the cache's own.
        Assert.Equal(cache.TotalBytes,  cache.ShedableBytes);
        Assert.Equal(cache.NativeBytes, cache.ShedableNativeBytes);
        Assert.True(MemoryShedRegistry.ShedableNativeBytes >= cache.ShedableNativeBytes);
    }
}
