using Microsoft.Extensions.DependencyInjection;
using Ameto.Core;

namespace Ameto.Query;

public static class QueryServiceExtensions
{
    /// <summary>
    /// Registers query services. Must be called after AddAmetoStorage and AddAmetoIndexing.
    /// </summary>
    public static IServiceCollection AddAmetoQuery(this IServiceCollection services)
    {
        services.AddSingleton<IQueryExecutor, QueryExecutor>();
        return services;
    }
}
