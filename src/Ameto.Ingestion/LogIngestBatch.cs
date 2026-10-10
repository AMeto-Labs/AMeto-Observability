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

    /// <summary>Positions in <see cref="_staged"/> of drop markers, ascending. Usually empty.</summary>
    private List<int>? _markers;

    internal LogIngestBatch(IngestionEndpoint owner, int payloadSizeHint)
    {
        _owner  = owner;
        _staged = new LogWriteBatch(payloadSizeHint);
    }

    /// <summary>The events staged — the client's, and drop markers.</summary>
    internal LogWriteBatch Staged => _staged;

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
    public int StagedEvents => _staged.Count - MarkerCount;

    internal int MarkerCount => _markers?.Count ?? 0;

    /// <summary>Drop markers among the first <paramref name="count"/> staged events.</summary>
    internal int MarkersBefore(int count)
    {
        if (_markers is null) return 0;
        int n = 0;
        foreach (int at in _markers)
        {
            if (at >= count) break;
            n++;
        }
        return n;
    }

    /// <summary>Writes the staged events into the store and says what became of them.</summary>
    public ValueTask<LogIngestResult> CommitAsync(CancellationToken ct = default) => _owner.CommitAsync(this, ct);

    public void Dispose() => _staged.Dispose();

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
        var header = new LogEventHeader
        {
            TimestampUtcTicks        = tsTicks,
            Level                    = (Ameto.Core.LogLevel)level,
            MessageTemplatePoolIndex = templateIdx,
            ServiceNamePoolIndex     = serviceIdx,
            TraceIdHi                = traceHi,
            TraceIdLo                = traceLo,
            SpanId                   = spanId,
        };
        _staged.Add(header, payload, template, exception);
    }

    internal void AddMarker(long tsTicks, byte level, int templateIdx, string template,
                            ReadOnlySpan<byte> payload, ulong traceHi, ulong traceLo, ulong spanId, int serviceIdx)
    {
        (_markers ??= []).Add(_staged.Count);
        Add(tsTicks, level, templateIdx, template, exception: null, payload, traceHi, traceLo, spanId, serviceIdx);
    }
}

/// <summary>What became of an ingest request's log events.</summary>
/// <param name="Ingested">The client's events now in the store.</param>
/// <param name="Dropped">The client's events that are not: refused at the door, or given up on for lack of room.</param>
/// <param name="Busy">
/// None of the client's events was written, for lack of room or because the store is shutting
/// down, and some were waiting: the request is answered "retry later" (503, gRPC UNAVAILABLE),
/// which duplicates none of them. A drop marker — the server's own event standing in for one over
/// the size limit — may have been written; the retry then writes it again.
/// </param>
public readonly record struct LogIngestResult(int Ingested, int Dropped, bool Busy);
