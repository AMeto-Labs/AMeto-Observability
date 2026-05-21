using Microsoft.Extensions.DependencyInjection;
using Rd.Log.Core;

namespace Rd.Log.Query;

public static class QueryServiceExtensions
{
    /// <summary>
    /// Registers query services. Must be called after AddRdLogStorage and AddRdLogIndexing.
    /// </summary>
    public static IServiceCollection AddRdLogQuery(this IServiceCollection services)
    {
        services.AddSingleton<IQueryExecutor, QueryExecutor>();
        return services;
    }
}
