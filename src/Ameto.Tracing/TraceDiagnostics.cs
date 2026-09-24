using Microsoft.Extensions.Logging;
using Ameto.Core;
using Ameto.Tracing.Ingestion;
using Ameto.Tracing.Storage;

namespace Ameto.Tracing;

/// <summary>
/// The trace side's effective budgets and back-pressure counters, as GET /api/diagnostics and the
/// startup line report them. config.yml and CONFIGURATION.md tell an operator who tunes
/// <c>Traces:HotTierMaxBytes</c> or <c>Traces:RingMaxBytes</c> to read these; before this class
/// the engine's budgets and the ring's refusals were internal and nothing reported them.
///
/// <para>Every figure is what the engine and the ring ENFORCE — read from them, never derived
/// afresh — so the report cannot disagree with the behaviour it describes. A property read is a
/// field or an interlocked read: the endpoint that polls this every 10 s allocates nothing here.</para>
/// </summary>
public sealed partial class TraceDiagnostics
{
    private readonly TraceStorageEngine _engine;
    private readonly SpanRingBuffer     _ring;

    internal TraceDiagnostics(TraceStorageEngine engine, SpanRingBuffer ring)
    {
        _engine = engine;
        _ring   = ring;
    }

    /// <summary>Bytes the hot tier may hold before a flush is forced (<c>Traces:HotTierMaxBytes</c>, effective).</summary>
    public long HotTierBudgetBytes => _engine.HotTierBudgetBytes;

    /// <summary>What one compaction pass may hold (<c>Traces:MergeBudgetBytes</c>, effective).</summary>
    public long MergeBudgetBytes => _engine.MergeBudgetBytes;

    /// <summary>Slots in the ingest ring (<c>Traces:RingCapacity</c>, effective).</summary>
    public int RingCapacity => _ring.Capacity;

    /// <summary>The most the spans waiting in the ring may weigh (<c>Traces:RingMaxBytes</c>, effective).</summary>
    public long RingMaxBytes => _ring.MaxBytes;

    /// <summary>What the spans waiting in the ring weigh right now.</summary>
    public long RingBytesInFlight => _ring.BytesInFlight;

    /// <summary>Spans refused because <see cref="RingMaxBytes"/> was spent, since the process started.</summary>
    public long RingRefusedForBytes => _ring.RefusedForBytes;

    /// <summary>Spans refused because every ring slot was still unread — the drainer is behind.</summary>
    public long RingRefusedNoSlot => _ring.RefusedNoSlot;

    /// <summary>
    /// Spans refused for want of an arena chunk. A 32–64 KB payload takes a 64 KiB chunk to itself,
    /// so a stream of such spans lands here at about half of <see cref="RingMaxBytes"/>.
    /// </summary>
    public long RingRefusedNoArena => _ring.RefusedNoArena;

    /// <summary>Span-name strings built outside the (full) name pool since the process started.</summary>
    public long UnpooledSpanNames => _ring.Pools.UnpooledNames;

    /// <summary>Service strings built outside the (full) service pool since the process started.</summary>
    public long UnpooledServiceNames => _ring.Pools.UnpooledServices;

    /// <summary>How many times an intern pool has filled: the service pool at most once, the name pool at most once per tier.</summary>
    public long InternPoolSaturations => _ring.Pools.Saturations;

    /// <summary>
    /// The startup line: the effective trace budgets, once, as the ring is built — the counterpart
    /// of the logs engine's "Flush budgets:" and of the metric engine's "Metric budgets:".
    /// </summary>
    internal static void LogBudgets(ILogger logger, TraceStorageEngine engine, SpanRingBuffer ring)
    {
        var budgets = MemoryBudgets.Current();
        LogBudgetsCore(logger, engine.HotTierBudgetBytes, engine.MergeBudgetBytes,
                       TraceStorageEngine.CompactionThresholdBytesFor(engine.MergeBudgetBytes),
                       ring.Capacity, ring.MaxBytes, budgets.ManagedLimitBytes / 1048576);
    }

    [LoggerMessage(Level = Microsoft.Extensions.Logging.LogLevel.Information,
        Message = "Trace budgets: hot tier {HotTierBytes} B or 50 000 spans (a flush is forced at either), compaction pass "
                + "{MergeBytes} B (segments under {CandidateBytes} B merge), ingest ring {RingSlots} slots holding at most "
                + "{RingMaxBytes} B of payload; derived from a {ManagedLimitMB} MB managed-heap limit, explicit Ameto:Traces values win")]
    private static partial void LogBudgetsCore(ILogger logger, long hotTierBytes, long mergeBytes, long candidateBytes,
                                               int ringSlots, long ringMaxBytes, long managedLimitMB);
}
