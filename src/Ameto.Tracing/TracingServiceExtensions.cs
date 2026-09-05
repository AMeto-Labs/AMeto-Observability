using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Tracing.Ingestion;
using Ameto.Tracing.Storage;

namespace Ameto.Tracing;

/// <summary>
/// Whether segments written before the trace-id index existed are brought into it, and how fast.
///
/// <para>OPTIONAL BY DESIGN, because the work is real. Indexing an existing segment means reading
/// and inflating its whole trace index — 38% of the file, and precisely the read this feature
/// removes from the query path. Paid once per segment it buys every later lookup of every trace in
/// it; paid for a thousand segments back to back on a 512 MB box it competes with ingest for the
/// disk and the thread pool. So the operator chooses, and the default chooses caution.</para>
///
/// <para>Nothing about this changes correctness. A segment that is not backfilled is simply not
/// covered, and an uncovered segment is read exactly the way it was read before the index
/// existed — see <c>TraceManifest</c>'s coverage rule. Turning it off later is
/// <c>ClearCoverage</c> plus deleting the <c>.tix</c> files; no trace data is touched either
/// way.</para>
/// </summary>
public enum TraceIndexBackfillMode
{
    /// <summary>Leave existing segments alone. New segments are still indexed as they are written.</summary>
    Off,

    /// <summary>One segment at a time with a pause between — the default. Finishes a week of hourly
    /// segments in a few minutes without ever being the reason a query is slow.</summary>
    Idle,

    /// <summary>As fast as segments can be read. For an operator who wants the fast path NOW and
    /// has the headroom to pay for it in one go.</summary>
    Eager,
}

/// <summary>The mode, boxed once so the container can hold it — DI has no home for a bare enum.</summary>
internal sealed record TraceIndexOptions(TraceIndexBackfillMode Backfill);

public static class TracingServiceExtensions
{
    /// <summary>
    /// Registers all distributed-tracing services: ring buffer, drainer, storage engine,
    /// and the <see cref="ISpanIngester"/> / <see cref="ITraceProvider"/> singletons.
    /// </summary>
    /// <param name="backfill">
    /// What to do about segments that predate the trace-id index. See
    /// <see cref="TraceIndexBackfillMode"/>; <see cref="TraceIndexBackfillMode.Idle"/> by default.
    /// </param>
    public static IServiceCollection AddAmetoTracing(
        this IServiceCollection services,
        string dataDirectory,
        TraceIndexBackfillMode backfill = TraceIndexBackfillMode.Idle,
        bool writeSegmentFormatV4 = false)
    {
        services.AddSingleton(new TraceIndexOptions(backfill));
        services.AddSingleton(sp =>
            new TraceStorageEngine(
                Path.Combine(dataDirectory, "traces"),
                sp.GetRequiredService<ILogger<TraceStorageEngine>>(),
                writeSegmentFormatV4));

        services.AddSingleton<SpanRingBuffer>();
        services.AddSingleton<SpanIngestionEndpoint>();
        services.AddSingleton<ISpanIngester>(sp => sp.GetRequiredService<SpanIngestionEndpoint>());
        services.AddSingleton<ITraceProvider>(sp => sp.GetRequiredService<TraceStorageEngine>());
        services.AddSingleton<ITraceStatsProvider>(sp => sp.GetRequiredService<TraceStorageEngine>());
        services.AddSingleton<IServiceGraphProvider>(sp => sp.GetRequiredService<TraceStorageEngine>());
        services.AddSingleton<ITraceSummaryProvider>(sp => sp.GetRequiredService<TraceStorageEngine>());
        services.AddSingleton<IRetentionTarget>(sp => sp.GetRequiredService<TraceStorageEngine>());

        services.AddSingleton<SpanDrainer>();
        services.AddHostedService<SpanDrainerService>();
        services.AddHostedService<TraceCompactionWorker>();
        services.AddHostedService<TraceIndexBackfillWorker>();

        return services;
    }
}

/// <summary>
/// Brings segments written before the trace-id index into it, one at a time, in the background.
///
/// <para>Every decision here is about NOT being noticed. It starts only after the cold scan has
/// run, because there is nothing to backfill until segments are known; it does one segment per
/// tick and sleeps between them; and when there is nothing left it goes quiet for minutes rather
/// than spinning — new segments are indexed by the flush that writes them, so the only work that
/// ever appears here is a segment adopted from an older install.</para>
///
/// <para>It cannot fail anything. A segment it cannot index stays uncovered and queryable; the
/// engine's own backfill step swallows and records that. The worker itself only paces.</para>
/// </summary>
internal sealed class TraceIndexBackfillWorker(
    TraceStorageEngine engine,
    TraceIndexOptions options,
    ILogger<TraceIndexBackfillWorker> logger) : BackgroundService
{
    private readonly TraceIndexBackfillMode mode = options.Backfill;

    /// <summary>Between segments. Long enough that the disk is somebody else's most of the time.</summary>
    private static readonly TimeSpan IdlePause  = TimeSpan.FromSeconds(5);
    /// <summary>Between segments when the operator asked for speed — still a yield, not a spin.</summary>
    private static readonly TimeSpan EagerPause = TimeSpan.FromMilliseconds(100);
    /// <summary>When there is nothing to do. Segments only appear here after a restart.</summary>
    private static readonly TimeSpan Quiet      = TimeSpan.FromMinutes(5);
    /// <summary>Gives the cold scan a head start; it is the thing that produces the work.</summary>
    private static readonly TimeSpan StartDelay = TimeSpan.FromSeconds(20);

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        // OFF STOPS THE BACKFILL, NOT THE MERGING, and returning here stopped both. Every flush
        // opens a per-segment run whatever the mode — the engine has never heard of this setting —
        // so with the worker gone nothing ever consolidated them and the run count grew until
        // retention, holding a bloom and a sparse map each. That is exactly the memory the
        // compactor exists to bound, left unbounded by a setting whose documentation says it only
        // affects existing segments.
        if (mode == TraceIndexBackfillMode.Off)
            logger.LogInformation(
                "Trace-id index backfill is off — segments written before the index stay on the "
              + "scanning path. New segments are still indexed as they are flushed, and their runs "
              + "are still merged");

        var pause = mode == TraceIndexBackfillMode.Eager ? EagerPause : IdlePause;
        try { await Task.Delay(StartDelay, ct).ConfigureAwait(false); }
        catch (OperationCanceledException) { return; }

        bool announced = false;
        while (!ct.IsCancellationRequested)
        {
            bool worked;
            try
            {
                // Backfill first, merging second. Backfill is what makes lookups fast and merging
                // only makes them cheap to keep fast — so an install still migrating spends its
                // pauses on coverage, and starts consolidating once there is nothing left to cover.
                // BOTH, NOT EITHER. The `||` short-circuited, so while a single uncovered segment
                // remained — and BackfillNextSegment returns true even for a segment it FAILED
                // on — the merge never ran at all. On an install with two thousand segments to
                // migrate that is hours with every per-segment run open and none consolidated,
                // which is the 10x memory the compactor is there to prevent, at its worst exactly
                // when the backfill is adding to it.
                worked = await Task.Run(() =>
                {
                    bool backfilled = mode != TraceIndexBackfillMode.Off && engine.BackfillNextSegment(ct);
                    bool merged     = engine.CompactIndexOnce(ct);
                    return backfilled || merged;
                }, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { return; }
            catch (Exception ex)
            {
                // The engine already swallows per-segment faults; anything reaching here is the
                // worker's own problem and must not end the service.
                logger.LogWarning(ex, "Trace-id index backfill pass failed");
                worked = false;
            }

            if (worked && !announced)
            {
                var (covered, total) = engine.IndexCoverage;
                logger.LogInformation(
                    "Trace-id index backfill running ({Mode}): {Covered} of {Total} cold segments "
                  + "covered so far", mode, covered, total);
                announced = true;
            }
            else if (!worked && announced)
            {
                var (covered, total) = engine.IndexCoverage;
                logger.LogInformation(
                    "Trace-id index backfill idle: {Covered} of {Total} cold segments covered",
                    covered, total);
                announced = false;
            }

            try { await Task.Delay(worked ? pause : Quiet, ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }
        }
    }
}

internal sealed class SpanDrainerService : IHostedService, IAsyncDisposable
{
    private readonly SpanDrainer _drainer;
    public SpanDrainerService(SpanDrainer d) => _drainer = d;
    public Task StartAsync(CancellationToken ct) => Task.CompletedTask;
    public async Task StopAsync(CancellationToken ct) => await _drainer.DisposeAsync();
    public async ValueTask DisposeAsync() => await _drainer.DisposeAsync();
}

internal sealed class TraceCompactionWorker(TraceStorageEngine engine, ILogger<TraceCompactionWorker> logger)
    : BackgroundService
{
    private static readonly TimeSpan Interval = TimeSpan.FromHours(1);

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        // Cold-segment discovery lives here, OFF the startup path: the server
        // accepts ingest from second zero and cold trace data becomes queryable
        // as soon as this completes.
        try
        {
            await Task.Run(engine.LoadColdSegments, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "TraceCompactionWorker: cold-segment load failed");
        }

        // First compaction shortly after load (drains any accumulated backlog of
        // small segments in bounded passes), then hourly.
        await Task.Delay(TimeSpan.FromMinutes(1), ct).ConfigureAwait(false);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Run(engine.CompactSmallSegments, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "TraceCompactionWorker: unexpected error");
            }

            await Task.Delay(Interval, ct).ConfigureAwait(false);
        }
    }
}
