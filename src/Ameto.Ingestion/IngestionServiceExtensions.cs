using Microsoft.Extensions.DependencyInjection;

namespace Ameto.Ingestion;

public static class IngestionServiceExtensions
{
    /// <summary>
    /// Registers the log ingest endpoint. Must be called after <c>AddAmetoStorage</c>: the endpoint
    /// writes straight into the <see cref="Ameto.Storage.StorageEngine"/> it registers — there is no
    /// ring or drainer between them any more.
    /// </summary>
    public static IServiceCollection AddAmetoIngestion(this IServiceCollection services)
    {
        // Endpoint — singleton, mapped as a route handler in Program.cs
        services.AddSingleton<IngestionEndpoint>();
        return services;
    }
}
