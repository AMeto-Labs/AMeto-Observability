using Ameto.Core;
using Ameto.Core.Serialization;
using Ameto.Storage;

namespace Ameto.Ingestion;

/// <summary>
/// One ingest request's log events, decoded and waiting to be written: the sink the parsers write
/// into (<see cref="IOtlpLogSink"/> for OTLP, <see cref="LogEventSerializer.IClefBatchSink"/> for
/// CLEF), then <see cref="CommitAsync"/> to put them in the store. From
/// <see cref="IngestionEndpoint.BeginBatch"/>; dispose it when the request is done.
///
/// <para>Every staging rule — the per-event size limit and its drop marker, template and service
/// interning — is the endpoint's (<see cref="IngestionEndpoint.StageRaw"/>,
/// <see cref="IngestionEndpoint.StageClef"/>); this holds what they stage.</para>
/// </summary>
public sealed class LogIngestBatch : IOtlpLogSink, LogEventSerializer.IClefBatchSink, IDisposable
{
    private readonly IngestionEndpoint _owner;
    private readonly LogWriteBatch     _staged;

    /// <summary>
    /// Drop markers, staged apart from the client's events and put into <see cref="_staged"/> after
    /// all of them by <see cref="Seal"/>. Usually null.
    ///
    /// <para><b>Why last.</b> The store writes a batch as a prefix and stops at the first event it
    /// has no room for. A marker staged where its oversized event stood could be written before the
    /// store ran out, so "nothing written" was false of a request answered 503, and its retry wrote
    /// that Error marker again. With the markers behind the client's events, a write that stopped
    /// before any of the client's events stopped before every marker too: a 503 wrote nothing, and
    /// the retry writes each marker once. A marker keeps its event's timestamp, which is what the
    /// store orders by; its place in the batch changes only its event id, as for any event that
    /// arrives late.</para>
    /// </summary>
    private List<StagedMarker>? _markers;

    /// <summary>The markers are in <see cref="_staged"/>, after the client's events.</summary>
    private bool _sealed;

    private readonly record struct StagedMarker(LogEventHeader Header, ReadOnlyMemory<byte> Payload, string Template);

    /// <summary>The decoded weight reserved for this batch (<see cref="IngestionEndpoint.TryBeginBatch"/>), given back on dispose.</summary>
    private long _reserved;

    internal LogIngestBatch(IngestionEndpoint owner, int payloadSizeHint, long reserved = 0)
    {
        _owner    = owner;
        _staged   = new LogWriteBatch(payloadSizeHint);
        _reserved = reserved;
    }

    /// <summary>
    /// The events to write: the client's, then the drop markers. The markers join only when the
    /// batch is committed (<see cref="Seal"/>); before that this holds the client's events alone.
    /// </summary>
    internal LogWriteBatch Staged => _staged;

    /// <summary>
    /// Puts the drop markers into <see cref="Staged"/>, after every one of the client's events, and
    /// returns it. Once: the batch is sealed afterwards, and a second call adds nothing.
    /// </summary>
    internal LogWriteBatch Seal()
    {
        if (_sealed) return _staged;
        if (_markers is not null)
            foreach (var m in _markers)
                _staged.Add(m.Header, m.Payload.Span, m.Template, exception: null);
        _sealed = true;
        return _staged;
    }

    /// <summary>Events refused at the door (over the per-event size limit); a marker stands in for each.</summary>
    public int DroppedAtDoor { get; private set; }

    /// <summary>
    /// The body this batch was decoded from turned out malformed part way: the request is answered
    /// 400 (INVALID_ARGUMENT) whatever the store did with the prefix — an answer the client does not
    /// retry, so its refusals are final. See <see cref="MarkMalformed"/>.
    /// </summary>
    public bool Malformed { get; private set; }

    /// <summary>Said by the receiver that caught the decode fault, before it commits the prefix.</summary>
    public void MarkMalformed() => Malformed = true;

    /// <summary>Client events staged so far, drop markers not counted.</summary>
    public int StagedEvents => _sealed ? _staged.Count - MarkerCount : _staged.Count;

    internal int MarkerCount => _markers?.Count ?? 0;

    /// <summary>
    /// Drop markers among the first <paramref name="count"/> events of the sealed batch: the
    /// markers are its last <see cref="MarkerCount"/> events.
    /// </summary>
    internal int MarkersBefore(int count) => Math.Clamp(count - StagedEvents, 0, MarkerCount);

    /// <summary>Writes the staged events into the store and says what became of them.</summary>
    public ValueTask<LogIngestResult> CommitAsync(CancellationToken ct = default) => _owner.CommitAsync(this, ct);

    public void Dispose()
    {
        try { _staged.Dispose(); }
        finally
        {
            // Once: a batch disposed twice gives back its reservation once.
            long reserved = _reserved;
            _reserved = 0;
            if (reserved != 0) _owner.ReleaseDecoded(reserved);
        }
    }

    // ── Sinks ─────────────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public bool TryIngestRaw(
        long tsTicks, byte level,
        ReadOnlySpan<byte> templateUtf8,
        ReadOnlySpan<byte> msgpackProps,
        ulong traceHi, ulong traceLo, ulong spanId,
        ReadOnlySpan<byte> serviceUtf8)
        => Count(_owner.StageRaw(this, tsTicks, level, templateUtf8, msgpackProps, traceHi, traceLo, spanId, serviceUtf8, serviceIdx: -1));

    /// <inheritdoc/>
    public bool TryIngestRaw(
        long tsTicks, byte level,
        ReadOnlySpan<byte> templateUtf8,
        ReadOnlySpan<byte> msgpackProps,
        ulong traceHi, ulong traceLo, ulong spanId,
        ReadOnlySpan<byte> serviceUtf8,
        int serviceIdx)
        => Count(_owner.StageRaw(this, tsTicks, level, templateUtf8, msgpackProps, traceHi, traceLo, spanId, serviceUtf8, serviceIdx));

    /// <inheritdoc/>
    public int InternService(ReadOnlySpan<byte> serviceUtf8) => _owner.Pool.Intern(serviceUtf8);

    /// <summary>Nothing to wake: the batch is written by <see cref="CommitAsync"/>.</summary>
    public void NotifyBatchEnqueued() { }

    /// <inheritdoc/>
    public bool TryIngestClef(
        long tsTicks,
        byte level,
        ReadOnlySpan<byte> templateUtf8,
        ExceptionInfo? exception,
        ReadOnlySpan<byte> msgpackProps,
        ulong traceHi, ulong traceLo, ulong spanId,
        ReadOnlySpan<byte> serviceUtf8)
        => Count(_owner.StageClef(this, tsTicks, level, templateUtf8, exception, msgpackProps, traceHi, traceLo, spanId, serviceUtf8));

    private bool Count(bool staged)
    {
        if (!staged) DroppedAtDoor++;
        return staged;
    }

    // ── Staging (from the endpoint) ───────────────────────────────────────────

    internal void Add(long tsTicks, byte level, int templateIdx, string? template, ExceptionInfo? exception,
                      ReadOnlySpan<byte> payload, ulong traceHi, ulong traceLo, ulong spanId, int serviceIdx)
    {
        System.Diagnostics.Debug.Assert(!_sealed, "an event staged after the batch was committed");
        _staged.Add(Header(tsTicks, level, templateIdx, traceHi, traceLo, spanId, serviceIdx), payload, template, exception);
    }

    /// <summary>
    /// Stages a drop marker, to be written after the client's events (see <see cref="_markers"/>).
    /// <paramref name="payload"/> is kept, not copied: the caller hands over a buffer of its own.
    /// </summary>
    internal void AddMarker(long tsTicks, byte level, int templateIdx, string template,
                            ReadOnlyMemory<byte> payload, ulong traceHi, ulong traceLo, ulong spanId, int serviceIdx)
    {
        System.Diagnostics.Debug.Assert(!_sealed, "a marker staged after the batch was committed");
        (_markers ??= []).Add(new StagedMarker(
            Header(tsTicks, level, templateIdx, traceHi, traceLo, spanId, serviceIdx), payload, template));
    }

    private static LogEventHeader Header(long tsTicks, byte level, int templateIdx,
                                         ulong traceHi, ulong traceLo, ulong spanId, int serviceIdx)
        => new()
        {
            TimestampUtcTicks        = tsTicks,
            Level                    = (Ameto.Core.LogLevel)level,
            MessageTemplatePoolIndex = templateIdx,
            ServiceNamePoolIndex     = serviceIdx,
            TraceIdHi                = traceHi,
            TraceIdLo                = traceLo,
            SpanId                   = spanId,
        };
}

/// <summary>What became of an ingest request's log events.</summary>
/// <param name="Ingested">The client's events now in the store.</param>
/// <param name="Dropped">The client's events that are not: refused at the door, or given up on for lack of room.</param>
/// <param name="Busy">
/// Nothing of the batch was written — for lack of room, because writing kept failing, or because
/// the store is shutting down — and some of the client's events were waiting: the request is
/// answered "retry later" (503, gRPC UNAVAILABLE), and the retry duplicates nothing. The drop
/// markers are written after the client's events, so none of them was written either. A batch of
/// nothing but drop markers is never busy: no retry could store any of its events, so it is
/// answered for good, with its markers written if the store had room for them.
/// </param>
public readonly record struct LogIngestResult(int Ingested, int Dropped, bool Busy);
