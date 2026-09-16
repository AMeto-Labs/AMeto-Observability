using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;

namespace Ameto.Query;

public static class QueryServiceExtensions
{
    /// <summary>
    /// Registers query services. Must be called after AddAmetoStorage and AddAmetoIndexing.
    /// </summary>
    public static IServiceCollection AddAmetoQuery(this IServiceCollection services)
    {
        services.AddSingleton(static sp =>
        {
            var q = sp.GetRequiredService<IOptions<ServerOptions>>().Value.Query;
            // Two ceilings: the total, a share of the GC's hard limit, and the native one (bloom
            // bits), a share of the physical limit — the bytes are not in the same place and are
            // not bounded by the same number. Registered with the shed registry so the
            // RAM-pressure loop can drop the cache: it holds native memory no collection can
            // reclaim, and it lives in an assembly RamPressureService cannot reference. DI
            // disposes this singleton with the host, which gives the registration back.
            return new SegmentIndexCache(
                    q.EffectiveIndexCacheBytes, q.EffectiveIndexCacheNativeBytes, q.IndexCacheIdleEvict)
                .RegisterForMemoryPressure();
        });
        services.AddSingleton<IQueryExecutor, QueryExecutor>();
        return services;
    }
}
