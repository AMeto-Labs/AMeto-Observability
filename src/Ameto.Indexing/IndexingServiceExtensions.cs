using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Indexing;

public static class IndexingServiceExtensions
{
    /// <summary>
    /// Registers indexing services and wires the index builder delegate into StorageEngine.
    /// Must be called AFTER <c>AddAmetoStorage</c>.
    /// </summary>
    public static IServiceCollection AddAmetoIndexing(this IServiceCollection services)
    {
        services.AddSingleton<SegmentIndexReaderFactory>();

        // Wire the index-building callback into StorageEngine without a circular project reference.
        services.AddSingleton<IndexingWiring>();
        services.AddHostedService<IndexingWiring>(sp => sp.GetRequiredService<IndexingWiring>());

        return services;
    }
}

/// <summary>
/// Hosted service that runs at startup and sets <see cref="StorageEngine.IndexBuilder"/>
/// so that index bytes are built during every segment flush.
/// </summary>
public sealed class IndexingWiring : Microsoft.Extensions.Hosting.IHostedService
{
    private readonly StorageEngine  _storage;
    private readonly IndexingOptions _opts;

    public IndexingWiring(StorageEngine storage, IOptions<ServerOptions> options)
    {
        _storage = storage;
        _opts    = options.Value.Indexing;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        int maxDepth = _opts.MaxPropertyFlattenDepth;
        _storage.IndexBuilder = (hot, pool, order) =>
        {
            var builder = new SegmentIndexBuilder(hot.Count, maxDepth);
            builder.Build(hot, pool, order);
            return (builder.SerialisedInvertedIndex,
                    builder.SerialisedTrigramIndex,
                    builder.SerialisedBloomFilter);
        };
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

/// <summary>
/// Factory that creates <see cref="SegmentIndexReader"/> instances from raw bytes.
/// Registered as a singleton so the query layer can inject it.
/// </summary>
public sealed class SegmentIndexReaderFactory
{
    public SegmentIndexReader Create(
        ReadOnlySpan<byte> invertedBytes,
        ReadOnlySpan<byte> trigramBytes,
        ReadOnlySpan<byte> bloomBytes)
        => SegmentIndexReader.Load(invertedBytes, trigramBytes, bloomBytes);
}
