using Ameto.Core;
using Ameto.Core.Serialization;
using Ameto.Storage;

namespace Ameto.Ingestion;

/// <summary>
/// One ingest request's log events, decoded and waiting to be written: the sink the parsers write
/// into (<see cref="IOtlpLogSink"/> for OTLP, <see cref="LogEventSerializer.IClefBatchSink"/> for
/// CLEF), then <see cref="BeginCommit"/>, its write awaited, and <see cref="EndCommit"/> to put them in the store. From
/// <see cref="IngestionEndpoint.TryBeginBatch"/>; dispose it when the request is done.
///
/// <para>Every staging rule — the per-event size limit and its drop marker, template and service
/// interning — is the endpoint's (<see cref="IngestionEndpoint.StageRaw"/>,
/// <see cref="IngestionEndpoint.StageClef"/>); this holds what they stage.</para>
///
/// <para><b>What it holds is counted as it is taken</b> (<see cref="ILogBatchBudget"/>): the arrays
/// its <see cref="LogWriteBatch"/> grows — charged before they are taken — and the strings and
/// exceptions the decode creates for it, which a pool with room shares and a full pool makes once
/// per record. Past the batch's limit (<c>Ingestion.MaxDecodedLogBatchBytes</c>) the decode stops
/// with <see cref="LogBatchTooHeavyException"/> — 413, nothing written; past what the batches held
/// at once may hold (<c>Ingestion.MaxDecodedLogBytesInFlight</c>), with
/// <see cref="LogBatchBudgetFullException"/> — 503, nothing written. The weight a receiver puts on
/// a body before decoding it is only the first reservation and an early refusal: an estimate,
/// which a body built to beat it (bytes that decode three times their size, a resource copied into
/// every record) used to beat.</para>
/// </summary>
public sealed class LogIngestBatch : IOtlpLogSink, LogEventSerializer.IClefBatchSink, ILogBatchBudget, IDisposable
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
    ///
    /// <para>Room for each is kept at the end of <see cref="_staged"/> when it is staged
    /// (<see cref="LogWriteBatch.ReserveTail"/>): <see cref="Seal"/>, which runs in the commit,
    /// outside the receivers' decode, never grows an array — an out-of-memory there was a 500,
    /// which exporters drop, for a batch nothing of which was written.</para>
    /// </summary>
    private List<StagedMarker>? _markers;

    /// <summary>The markers are in <see cref="_staged"/>, after the client's events.</summary>
    private bool _sealed;

    private readonly record struct StagedMarker(LogEventHeader Header, ReadOnlyMemory<byte> Payload, string Template, string? Service,
                                                OversizedDrop Drop);

    /// <summary>An event refused at the door, as the line that names it says: its properties' size, its sender's service and its template.</summary>
    internal readonly record struct OversizedDrop(int PayloadBytes, string Service, string Template);

    /// <summary>The drop the <paramref name="index"/>-th marker stands in for.</summary>
    internal OversizedDrop DropAt(int index) => _markers![index].Drop;

    /// <summary>What a staged marker holds besides its payload: its writer's buffer slack, its list slot.</summary>
    private const int MarkerOverheadBytes = 1024;

    /// <summary>The most this batch may hold (<c>Ingestion.MaxDecodedLogBatchBytes</c>); <see cref="long.MaxValue"/> for a batch with no bound.</summary>
    private readonly long _limit;

    /// <summary>Whether this batch reserves what it holds against the process-wide bound.</summary>
    private readonly bool _bounded;

    /// <summary>What the batch holds now: its arrays, and the strings and exceptions charged to it.</summary>
    private long _charged;

    /// <summary>Reserved for it against the batches held decoded at once; given back when its arrays are.</summary>
    private long _reserved;

    private int _released;

    internal LogIngestBatch(IngestionEndpoint owner, int payloadSizeHint, long reserved, long limit, bool bounded)
    {
        _owner    = owner;
        _reserved = reserved;
        _limit    = limit;
        _bounded  = bounded;
        _staged   = new LogWriteBatch(payloadSizeHint, this);
    }

    /// <summary>
    /// The events to write: the client's, then the drop markers. The markers join only when the
    /// batch is committed (<see cref="Seal"/>); before that this holds the client's events alone.
    /// </summary>
    internal LogWriteBatch Staged => _staged;

    /// <summary>What the batch holds, counted as it was taken (<see cref="ILogBatchBudget"/>).</summary>
    public long ChargedBytes => Volatile.Read(ref _charged);

    /// <summary>What is reserved for it against the batches held decoded at once.</summary>
    internal long ReservedBytes => Volatile.Read(ref _reserved);

    /// <summary>
    /// Puts the drop markers into <see cref="Staged"/>, after every one of the client's events, and
    /// returns it — into the room kept for them, so nothing grows. Once: the batch is sealed
    /// afterwards, and a second call adds nothing.
    /// </summary>
    internal LogWriteBatch Seal()
    {
        if (_sealed) return _staged;
        if (_markers is not null)
            foreach (var m in _markers)
                _staged.AddReserved(m.Header, m.Payload.Span, m.Template, exception: null, m.Service);
        _sealed = true;
        return _staged;
    }

    /// <summary>
    /// The decode is done: what is reserved for the batch shrinks to what it holds, for the wait
    /// for room that may follow. The reservation grows by doubling while the batch is decoded, and
    /// a weight taken before decoding can be more than the decode made of it.
    /// </summary>
    internal void TrimReservation()
    {
        if (!_bounded) return;
        long excess = _reserved - _charged;
        if (excess <= 0) return;
        _reserved = _charged;
        _owner.ReleaseDecoded(excess);
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

    /// <summary>
    /// Begins writing the staged events into the store: what a commit does before the write's wait, and
    /// the write, synchronously. The caller awaits <see cref="LogCommit.Write"/> ITSELF — a receiver on the
    /// state machine it boxed before the batch was begun (round-5 H-G), where that await allocates nothing
    /// — and hands what it came to to <see cref="EndCommit"/>. No method of the commit's own stands between
    /// the request and the write (round-6 review R5-M4): one whose first suspension was its await of the
    /// write could fail to box there with the write going on.
    /// </summary>
    public LogCommit BeginCommit(CancellationToken ct = default) => _owner.BeginCommit(this, ct);

    /// <summary>What became of the staged events, from what <see cref="LogCommit.Write"/> came to. Synchronous.</summary>
    public LogIngestResult EndCommit(in LogCommit commit, LogBatchWriteResult written) => _owner.EndCommit(this, in commit, written);

    /// <summary>
    /// <see cref="BeginCommit"/>, the write awaited HERE, then <see cref="EndCommit"/>: for a caller with
    /// nothing to lose if this method's own box fails at that await — the tests. Not for a receiver (see
    /// <see cref="BeginCommit"/>).
    /// </summary>
    internal async ValueTask<LogIngestResult> CommitAsync(CancellationToken ct = default)
    {
        var commit  = BeginCommit(ct);
        var written = await commit.Write.ConfigureAwait(false);
        return EndCommit(commit, written);
    }

    /// <summary>
    /// Gives the batch's arrays back, and its reservation with them — when the write that holds the
    /// batch ends, if one does (<see cref="LogWriteBatch.Dispose"/>).
    /// </summary>
    public void Dispose() => _staged.Dispose();

    // ── What it holds (ILogBatchBudget) ───────────────────────────────────────

    long ILogBatchBudget.Headroom => _limit - _charged;

    void ILogBatchBudget.Charge(long bytes) => Charge(bytes);

    void ILogBatchBudget.Released()
    {
        if (Interlocked.Exchange(ref _released, 1) != 0) return;
        long reserved = _reserved;
        _reserved = 0;
        if (_bounded && reserved > 0) _owner.ReleaseDecoded(reserved);
    }

    /// <summary>
    /// Counts <paramref name="bytes"/> more as held — or throws, and then they are not:
    /// <see cref="LogBatchTooHeavyException"/> past the batch's limit, <see cref="LogBatchBudgetFullException"/>
    /// when the reservation cannot grow to cover them. Negative undoes an earlier charge.
    /// </summary>
    internal void Charge(long bytes)
    {
        long charged = _charged + bytes;
        if (bytes > 0 && _bounded)
        {
            if (charged > _limit) throw new LogBatchTooHeavyException(charged, _limit);
            if (charged > _reserved) _reserved = _owner.ReserveMore(_reserved, charged, _limit);
        }
        _charged = charged;
    }

    /// <summary>A string the decode made for this batch alone: its size in the heap.</summary>
    internal void ChargeString(string s) => Charge(StringBytes(s));

    /// <summary>An exception tree the decode made for this batch alone (CLEF's <c>@x</c>): its size in the heap.</summary>
    internal void ChargeException(ExceptionInfo exception)
    {
        long bytes = 0;
        for (var e = exception; e is not null; e = e.Inner)
            bytes += ExceptionInfoObjectBytes + StringBytes(e.Type) + StringBytes(e.Message) + StringBytes(e.StackTrace);
        Charge(bytes);
    }

    /// <summary>A string's size in a 64-bit heap: header, method table, length, its UTF-16 and a terminator, 8-aligned.</summary>
    internal static long StringBytes(string? s) => s is null ? 0 : (22L + 2L * s.Length + 7) & ~7L;

    /// <summary>An <see cref="ExceptionInfo"/> object: header, method table and its four references.</summary>
    private const long ExceptionInfoObjectBytes = 48;

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
    public int InternService(ReadOnlySpan<byte> serviceUtf8) => _owner.Pool.Intern(IngestionEndpoint.ServiceName(serviceUtf8));

    /// <inheritdoc cref="IOtlpLogSink.MaxEventPayloadBytes"/>
    public int MaxEventPayloadBytes => _owner.MaxEventPayloadBytes;

    /// <inheritdoc cref="IOtlpLogSink.MaxServiceNameBytes"/>
    public int MaxServiceNameBytes => IngestionEndpoint.MaxServiceNameBytes;

    /// <inheritdoc cref="IOtlpLogSink.ChargeScratch"/>
    public void ChargeScratch(long bytes) => Charge(bytes);

    /// <inheritdoc/>
    public bool TryIngestOversized(
        long tsTicks, byte level,
        ReadOnlySpan<byte> templateUtf8,
        long payloadBytes,
        ulong traceHi, ulong traceLo, ulong spanId,
        ReadOnlySpan<byte> serviceUtf8,
        int serviceIdx)
        => Count(_owner.StageOversized(this, tsTicks, level, templateUtf8, payloadBytes, traceHi, traceLo, spanId, serviceUtf8, serviceIdx));

    /// <inheritdoc/>
    public bool TryIngestOversizedClef(
        long tsTicks, byte level,
        ReadOnlySpan<byte> templateUtf8,
        long payloadBytes,
        ulong traceHi, ulong traceLo, ulong spanId,
        ReadOnlySpan<byte> serviceUtf8)
        => Count(_owner.StageOversized(this, tsTicks, level, templateUtf8, payloadBytes, traceHi, traceLo, spanId, serviceUtf8, serviceIdx: -1));

    /// <summary>Nothing to wake: the batch is written by <see cref="BeginCommit"/>.</summary>
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
                      ReadOnlySpan<byte> payload, ulong traceHi, ulong traceLo, ulong spanId, int serviceIdx,
                      string? service = null)
    {
        System.Diagnostics.Debug.Assert(!_sealed, "an event staged after the batch was committed");
        _staged.Add(Header(tsTicks, level, templateIdx, traceHi, traceLo, spanId, serviceIdx), payload, template, exception, service);
    }

    /// <summary>
    /// Stages a drop marker, to be written after the client's events (see <see cref="_markers"/>),
    /// and keeps room for it at the end of the batch now. <paramref name="payload"/> is kept, not
    /// copied: the caller hands over a buffer of its own.
    /// </summary>
    internal void AddMarker(long tsTicks, byte level, int templateIdx, string template,
                            ReadOnlyMemory<byte> payload, ulong traceHi, ulong traceLo, ulong spanId, int serviceIdx,
                            string? service, OversizedDrop drop)
    {
        System.Diagnostics.Debug.Assert(!_sealed, "a marker staged after the batch was committed");
        Charge(MarkerOverheadBytes + payload.Length + StringBytes(service) + StringBytes(drop.Service) + StringBytes(drop.Template));
        _staged.ReserveTail(payload.Length);
        (_markers ??= []).Add(new StagedMarker(
            Header(tsTicks, level, templateIdx, traceHi, traceLo, spanId, serviceIdx), payload, template, service, drop));
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

/// <summary>
/// A commit begun (<see cref="LogIngestBatch.BeginCommit"/>): the write, for the caller to await, and
/// what the answer is made of, read before the write — whose lease ends inside it, where a dispose asked
/// meanwhile zeroes the batch. Await <see cref="Write"/> once, then hand both to
/// <see cref="LogIngestBatch.EndCommit"/>.
/// </summary>
public readonly struct LogCommit
{
    internal LogCommit(ValueTask<LogBatchWriteResult> write, int staged, int clientEvents, int markers, int droppedAtDoor, bool malformed)
    {
        Write         = write;
        Staged        = staged;
        ClientEvents  = clientEvents;
        Markers       = markers;
        DroppedAtDoor = droppedAtDoor;
        Malformed     = malformed;
    }

    /// <summary>
    /// The write, begun: it waits for room if it must, and never throws for a write that fails
    /// (StorageEngine.WriteBatchAsync). A receiver awaits it on the state machine it boxed before the
    /// batch was begun, where the await allocates nothing (round-6 review R5-M4). Awaited once, as any
    /// <see cref="ValueTask{TResult}"/>; completed at once when nothing was staged.
    /// </summary>
    public ValueTask<LogBatchWriteResult> Write { get; }

    /// <summary>Events written by <see cref="Write"/>: the client's, then the drop markers.</summary>
    internal int Staged { get; }

    /// <summary>The client's events among them.</summary>
    internal int ClientEvents { get; }

    /// <summary>The drop markers among them, after the client's events.</summary>
    internal int Markers { get; }

    /// <summary>Events refused at the door (<see cref="LogIngestBatch.DroppedAtDoor"/>).</summary>
    internal int DroppedAtDoor { get; }

    /// <summary>The body turned out malformed part way (<see cref="LogIngestBatch.Malformed"/>).</summary>
    internal bool Malformed { get; }
}

/// <summary>
/// A log batch whose decode passed <c>Ingestion.MaxDecodedLogBatchBytes</c> — weighed before it was
/// decoded, or counted while it was: refused 413 (gRPC RESOURCE_EXHAUSTED), nothing of it written.
/// </summary>
public sealed class LogBatchTooHeavyException(long weight, long limit)
    : Exception("a log batch decodes to more than the configured limit for one request")
{
    /// <summary>What it weighed, or had been counted at, when it was refused.</summary>
    public long Weight { get; } = weight;

    /// <summary>The limit it passed.</summary>
    public long Limit { get; } = limit;
}

/// <summary>
/// The batches held decoded at once are at their limit (<c>Ingestion.MaxDecodedLogBytesInFlight</c>):
/// answered 503 with Retry-After (gRPC UNAVAILABLE), nothing of the batch written.
/// </summary>
public sealed class LogBatchBudgetFullException() : Exception("the log batches held decoded at once are at their limit");
