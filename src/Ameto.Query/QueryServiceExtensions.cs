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
            return new SegmentIndexCache(q.EffectiveIndexCacheBytes, q.IndexCacheIdleEvict);
        });
        services.AddSingleton<IQueryExecutor, QueryExecutor>();
        return services;
    }
}
