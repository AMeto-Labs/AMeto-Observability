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
    private readonly Microsoft.Extensions.Logging.ILogger<IndexingWiring> _log;

    public IndexingWiring(StorageEngine storage, IOptions<ServerOptions> options,
                          Microsoft.Extensions.Logging.ILogger<IndexingWiring> log)
    {
        _storage = storage;
        _opts    = options.Value.Indexing;
        _log     = log;
    }

    /// <summary>The hints (and counters) every group this process builds shares — see <see cref="IndexBuildHints"/>.</summary>
    public IndexBuildHints Hints { get; } = new();

    /// <summary>
    /// Bytes parked right now in the index build pools (term and posting slabs, hash tables,
    /// entry arrays) — for diagnostics. Transient after a gen2 trim, but counted in no memory
    /// budget, so it is the figure to read when the build side's resting memory is in question.
    /// </summary>
    public long IndexBuildPooledBytes => IndexBuildPool.PooledBytes;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        int maxDepth = _opts.MaxPropertyFlattenDepth;
        // One hints object for every group this process builds: each builder pre-sizes its
        // term and trigram tables from what the last sealed group measured (see IndexBuildHints).
        var hints = Hints;
        var log   = _log;
        // A FRESH builder per index group — that is the mechanism, not an accident. The
        // accumulators (trigram especially, ~7.6 B/posting scaling with indexed text bytes)
        // die with the builder as soon as its sections are serialised, so peak index-build
        // memory is O(group) and no longer grows with the segment.
        //
        // Sized on the group, not the segment: the bloom's ~10 bits/term budget is what makes
        // it selective, and one filter stretched over a day's terms prunes nothing. Both counts
        // are the writer's forecast for the group — events from the payload budget, terms per
        // event from what the file's already-sealed groups measured. See SegmentWriter.EnsureSink.
        _storage.IndexSinkFactory = (estimatedEventCount, estimatedTermsPerEvent) =>
            new SegmentIndexBuilder(estimatedEventCount, maxDepth, estimatedTermsPerEvent, hints, log);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

/// <summary>
/// How the query layer gets at a group's index. Registered as a singleton so it can be injected.
/// </summary>
public sealed class SegmentIndexReaderFactory
{
    /// <summary>A self-contained reader over copies of one group's sections.</summary>
    public SegmentIndexReader Create(
        ReadOnlySpan<byte> invertedBytes,
        ReadOnlySpan<byte> trigramBytes,
        ReadOnlySpan<byte> bloomBytes)
        => SegmentIndexReader.Load(invertedBytes, trigramBytes, bloomBytes);

    /// <summary>
    /// One query's view of one group: the cached memo when <paramref name="cache"/> is enabled,
    /// answering through <paramref name="segment"/>, which the query keeps open. See
    /// <see cref="SegmentIndexView"/>.
    /// </summary>
    public SegmentIndexView OpenGroup(SegmentIndexCache? cache, string path, int group, SegmentReader segment)
        => SegmentIndexView.Open(cache, path, group, segment);
}
