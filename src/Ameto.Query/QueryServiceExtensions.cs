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
        services.AddSingleton(static sp => new SegmentIndexCache(
            sp.GetRequiredService<IOptions<ServerOptions>>().Value.Query.IndexCacheBytes));
        services.AddSingleton<IQueryExecutor, QueryExecutor>();
        return services;
    }
}
