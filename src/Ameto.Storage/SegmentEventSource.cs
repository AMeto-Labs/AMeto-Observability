using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// One event, as the segment writer and the index build see it: everything that ends up in a
/// .seg row, and nothing else.
///
/// <para>A <c>readonly ref struct</c> rather than a class or an interface because
/// <see cref="Properties"/> points STRAIGHT AT the producer's buffer — the hot tier's native
/// arena on the flush path, a decompressed block on the merge path. Materialising a per-event
/// object instead would put the merge back where it started: <c>ReadAllRaw</c> allocated one
/// <c>byte[]</c> per event's properties, which is most of what made a whole-batch merge cost
/// ~3× the batch. The stack-only guarantee is what makes "no copy" safe to promise.</para>
///
/// <para>LIFETIME: valid only until the next <see cref="ISegmentEventSource.TryReadNext"/> on the
/// source that produced it. Consume it — copy the payload, index it — before pulling again.</para>
/// </summary>
public readonly ref struct SegmentEventRef
{
    public readonly ulong              Id;
    public readonly long               TimestampUtcTicks;
    public readonly LogLevel           Level;
    public readonly ulong              TraceIdHi;
    public readonly ulong              TraceIdLo;
    public readonly ulong              SpanId;
    /// <summary>Never null; <see cref="string.Empty"/> when the event carries no template.</summary>
    public readonly string             MessageTemplate;
    public readonly string?            ServiceName;
    /// <summary>
    /// The decoded exception — set only by a producer that already holds one (the hot tier
    /// stores exceptions as objects). On the merge path this is null and
    /// <see cref="ExceptionPayload"/> carries the same value as bytes; call
    /// <see cref="DecodeException"/> rather than reading this field.
    /// </summary>
    public readonly ExceptionInfo?     Exception;
    /// <summary>
    /// The exception exactly as it sits in the source file — empty when there is none, or when
    /// the producer only has the decoded form.
    ///
    /// <para>This is what makes the exception column zero-copy through a merge, like every other
    /// column. Decoding it into an object graph and re-serialising it per row cost 6843 B/event
    /// against 182 B for the same merge without exceptions — 37×, and it lands squarely on the
    /// compaction path, because level-split flush means an Error segment is 100 % exceptions and
    /// compaction is what gathers them.</para>
    /// </summary>
    public readonly ReadOnlySpan<byte> ExceptionPayload;
    /// <summary>Raw msgpack property map — empty when the event has no properties.</summary>
    public readonly ReadOnlySpan<byte> Properties;

    public SegmentEventRef(
        ulong id, long timestampUtcTicks, LogLevel level,
        ulong traceIdHi, ulong traceIdLo, ulong spanId,
        string messageTemplate, string? serviceName, ExceptionInfo? exception,
        ReadOnlySpan<byte> properties, ReadOnlySpan<byte> exceptionPayload = default)
    {
        Id                = id;
        TimestampUtcTicks = timestampUtcTicks;
        Level             = level;
        TraceIdHi         = traceIdHi;
        TraceIdLo         = traceIdLo;
        SpanId            = spanId;
        MessageTemplate   = messageTemplate;
        ServiceName       = serviceName;
        Exception         = exception;
        ExceptionPayload  = exceptionPayload;
        Properties        = properties;
    }

    public bool HasTraceId   => (TraceIdHi | TraceIdLo) != 0;
    public bool HasSpanId    => SpanId != 0;
    public bool HasException => Exception is not null || !ExceptionPayload.IsEmpty;

    /// <summary>
    /// The exception as an object graph, decoding the raw payload if that is all we have.
    ///
    /// <para>Only the INDEX BUILD needs this — it indexes the type, the message and the inner
    /// type as strings. The writer does not, and asking for it there is what put a decode plus a
    /// re-encode on every exception-carrying row of every merge. A merge that runs without an
    /// index sink now never decodes at all.</para>
    /// </summary>
    public ExceptionInfo? DecodeException() =>
        Exception ?? (ExceptionPayload.IsEmpty ? null : ExceptionInfo.FromBytes(ExceptionPayload));
}

/// <summary>
/// A source of events in FILE WRITE ORDER — ascending (timestamp, id).
///
/// <para>This is the seam that lets a segment be written without ever holding it: the flush path
/// supplies a frozen <see cref="HotTierSegment"/> in sort order, compaction supplies a k-way
/// merge over several .seg files, and <see cref="SegmentWriter"/> cannot tell the difference.
/// Before it existed, the writer and the index builder both indexed INTO a hot tier
/// (<c>hot.GetHeader(i)</c>), so a merge had to materialise its whole batch into one just to be
/// writable — which is what made merged-segment size a memory question instead of a policy one.</para>
///
/// <para>Implementations are pull-only and single-pass. One interface call per event (the event
/// comes back whole rather than field by field) so the abstraction costs a virtual dispatch per
/// event, not per column.</para>
/// </summary>
public interface ISegmentEventSource
{
    /// <summary>
    /// Reads the next event. The returned <paramref name="ev"/> is valid until the NEXT call —
    /// its <see cref="SegmentEventRef.Properties"/> may point into a buffer this source reuses.
    /// </summary>
    bool TryReadNext(out SegmentEventRef ev);

    /// <summary>
    /// Upper bound on the events still to come. A SIZING HINT only — never a contract, and a
    /// source that cannot know may return <see cref="long.MaxValue"/>. Used to size the bloom
    /// filter of the next index group, where over-estimating costs bits and under-estimating
    /// costs selectivity.
    /// </summary>
    long RemainingEventHint { get; }
}

/// <summary>
/// Receives every event of ONE INDEX GROUP as the writer emits it, then hands back the group's
/// three index sections.
///
/// <para>Push, not pull, and that is the whole point. The pre-streaming contract had the writer
/// call back at the group boundary with an ordinal range, and the builder re-read those events
/// out of the hot tier — which only works if something still holds them. Pushing each event once,
/// while it is in the writer's hand anyway, means neither side has to retain the group: peak
/// build state is the accumulators alone.</para>
///
/// <para>Posting offsets MUST be the FILE ordinals handed to <see cref="Add"/>, not group-local
/// ones — see the ordinal contract on <see cref="SegmentWriter"/>.</para>
/// </summary>
public interface ISegmentIndexSink : IDisposable
{
    void Add(uint fileOrdinal, in SegmentEventRef ev);

    /// <summary>Serialises the group's sections. Called once, after the last <see cref="Add"/>.</summary>
    (byte[] Inverted, byte[] Trigram, byte[] Bloom) Serialise();
}

/// <summary>
/// Creates the sink for one index group. <paramref name="estimatedEventCount"/> is the writer's
/// forecast for the group (see <see cref="SegmentWriter"/>) — the bloom filter is sized up front
/// and cannot be resized, so it is a hint the sink should respect but must not trust.
/// </summary>
public delegate ISegmentIndexSink SegmentIndexSinkFactory(int estimatedEventCount);

/// <summary>
/// The flush path's <see cref="ISegmentEventSource"/>: a frozen hot tier walked in the order
/// produced by <see cref="SegmentWriter.ComputeSortOrder"/>.
///
/// <para><paramref name="order"/> need not cover the whole tier — a level-split flush writes one
/// level's subsequence per segment. Properties are handed out as spans into the tier's native
/// arena, so nothing is copied on the way to the block writer.</para>
/// </summary>
public sealed class HotTierEventSource : ISegmentEventSource
{
    private readonly HotTierSegment   _hot;
    private readonly StringInternPool _pool;
    private readonly int[]?           _order;
    private readonly int              _end;
    private          int              _pos;

    public HotTierEventSource(HotTierSegment hot, StringInternPool pool, int[]? order = null)
        : this(hot, pool, order, 0, order?.Length ?? hot.Count) { }

    public HotTierEventSource(HotTierSegment hot, StringInternPool pool, int[]? order, int firstOrdinal, int eventCount)
    {
        _hot   = hot;
        _pool  = pool;
        _order = order;
        _pos   = firstOrdinal;
        _end   = Math.Min(firstOrdinal + eventCount, order?.Length ?? hot.Count);
    }

    public long RemainingEventHint => Math.Max(0, _end - _pos);

    public bool TryReadNext(out SegmentEventRef ev)
    {
        if (_pos >= _end) { ev = default; return false; }
        int i = _order is null ? _pos : _order[_pos];
        _pos++;
        ev = EventAt(_hot, _pool, i);
        return true;
    }

    /// <summary>
    /// Builds the event view for tier index <paramref name="i"/>. Static so the index build can
    /// keep its by-index entry point (the parity probes drive it directly) while reading exactly
    /// the same fields the streaming path does — one definition of "what an event is".
    /// </summary>
    public static SegmentEventRef EventAt(HotTierSegment hot, StringInternPool pool, int i)
    {
        ref readonly LogEventHeader h = ref hot.GetHeader(i);
        // The tier-local template wins over the pool: it survives a pool miss (WAL recovery
        // restores the pool separately), which is why the tier stores it at all.
        string template = hot.GetTemplate(i) ?? pool.Get(h.MessageTemplatePoolIndex) ?? string.Empty;
        string? service = h.ServiceNamePoolIndex >= 0 ? pool.Get(h.ServiceNamePoolIndex) : null;
        return new SegmentEventRef(
            h.Id, h.TimestampUtcTicks, h.Level,
            h.TraceIdHi, h.TraceIdLo, h.SpanId,
            template, service, hot.GetException(i),
            hot.GetPropertiesPayload(i));
    }
}
