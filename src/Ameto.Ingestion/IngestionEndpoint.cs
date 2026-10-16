using System.Buffers;
using MessagePack;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Ameto.Core;
using Ameto.Core.Serialization;
using Ameto.Storage;

namespace Ameto.Ingestion;

/// <summary>
/// Sink for the zero-alloc OTLP streaming parser: one already-msgpack-encoded record at a
/// time. Implemented by <see cref="LogIngestBatch"/>; abstracted so the parser can be
/// unit-tested against a capturing fake.
/// </summary>
public interface IOtlpLogSink
{
    bool TryIngestRaw(
        long tsTicks, byte level,
        ReadOnlySpan<byte> templateUtf8,
        ReadOnlySpan<byte> msgpackProps,
        ulong traceHi, ulong traceLo, ulong spanId,
        ReadOnlySpan<byte> serviceUtf8);

    /// <summary>
    /// Interns a <c>service.name</c> ONCE for a whole resourceLogs block and returns its
    /// pool index, so the parser does not re-hash the same name for every record under it
    /// (the name is a property of the resource, and a block carries hundreds of records).
    /// Returns -1 when there is nothing to intern or the sink has no pool.
    /// </summary>
    int InternService(ReadOnlySpan<byte> serviceUtf8) => -1;

    /// <summary>
    /// As <see cref="TryIngestRaw(long, byte, ReadOnlySpan{byte}, ReadOnlySpan{byte}, ulong, ulong, ulong, ReadOnlySpan{byte})"/>,
    /// with the service already interned by <see cref="InternService"/>. A negative
    /// <paramref name="serviceIdx"/> means "not interned yet — do it from the span".
    /// </summary>
    bool TryIngestRaw(
        long tsTicks, byte level,
        ReadOnlySpan<byte> templateUtf8,
        ReadOnlySpan<byte> msgpackProps,
        ulong traceHi, ulong traceLo, ulong spanId,
        ReadOnlySpan<byte> serviceUtf8,
        int serviceIdx)
        => TryIngestRaw(tsTicks, level, templateUtf8, msgpackProps, traceHi, traceLo, spanId, serviceUtf8);

    /// <summary>Called once after a batch has been parsed. A sink that writes as it goes may ignore it.</summary>
    void NotifyBatchEnqueued();

    /// <summary>
    /// The longest property map a record may have. A parser stops building one that passes it and
    /// hands the record to <see cref="TryIngestOversized"/> instead — so its scratch never holds more
    /// than this for a record, where a resource attribute of bytes that are not UTF-8 grew it to three
    /// times the attribute, in every record under it, before the record was refused anyway.
    /// Unbounded for a sink that keeps everything.
    /// </summary>
    int MaxEventPayloadBytes => int.MaxValue;

    /// <summary>
    /// The longest <c>service.name</c> the sink keeps, in UTF-8 bytes: it cuts a longer one at a
    /// character. A parser that has to copy the name may stop a few bytes past this — what it does
    /// not copy, the sink would cut. Unbounded for a sink that keeps everything.
    /// </summary>
    int MaxServiceNameBytes => int.MaxValue;

    /// <summary>
    /// Counts <paramref name="bytes"/> of scratch the parser is about to take for the batch — the
    /// unescaped copy of a template whose JSON string carries escapes, as long as the body it is in —
    /// against what the batch may hold, before it is taken; a negative count gives it back. Throws as
    /// the batch's own charges do, and then nothing was counted. Nothing for a sink that keeps everything.
    /// </summary>
    void ChargeScratch(long bytes) { }

    /// <summary>
    /// A record whose property map would be <paramref name="payloadBytes"/> long — past
    /// <see cref="MaxEventPayloadBytes"/> — refused before it was built: the sink records the drop
    /// as it would an oversized map's, without the map. Returns false: the record is dropped.
    /// </summary>
    bool TryIngestOversized(
        long tsTicks, byte level,
        ReadOnlySpan<byte> templateUtf8,
        long payloadBytes,
        ulong traceHi, ulong traceLo, ulong spanId,
        ReadOnlySpan<byte> serviceUtf8,
        int serviceIdx) => false;
}

/// <summary>
/// The log ingest path: decodes a request into a <see cref="LogIngestBatch"/>, writes the batch
/// into the store with <see cref="StorageEngine.WriteBatchAsync"/>, and answers once it is there —
/// in the hot tier and its WAL, or in a spill file. A 200 therefore means the events survive the
/// death of this process (see <see cref="StorageEngine"/>, "The write path").
///
/// <para>Handles POST /api/events itself (<see cref="HandleAsync"/>); the OTLP receivers decode
/// into a batch from <see cref="BeginBatch"/> and commit it the same way.</para>
///
/// <para>Wire format of /api/events: MessagePack array of CLEF maps.
///   [ { "@t": "...", "@mt": "...", "@l": "...", "Prop": value, ... }, ... ]</para>
///
/// Returns (every 200, 400 and 503 is application/json with the counts):
///   200 OK          { "ingested": N, "dropped": M }
///   400 Bad Request { "ingested": N, "dropped": M, "failedAtElement": K }
///          The body stopped being a CLEF array at element K. The N events before it are
///          INGESTED and stay so. failedAtElement is omitted when the body failed
///          before any element, at the array header (not an array, or empty).
///   400 Bad Request { "ingested": 0, "dropped": 0 }
///          The body ended short of its Content-Length on a stream that ends rather than
///          throws (see HandleAsync 1b). Kestrel fails that read itself.
///   413 Payload Too Large, no body, above <see cref="IngestionOptions.MaxBatchBytes"/>, or for a
///          batch that would decode to more than <see cref="IngestionOptions.MaxDecodedLogBatchBytes"/>
///          (weighed before decoding, or counted the moment the decode passes it; nothing of it
///          ingested)
///   503 Service Unavailable, Retry-After { "ingested": 0, "dropped": M }
///          The store had no room for any of the client's events within
///          <see cref="IngestionOptions.BackPressureWait"/>, writing into it kept failing for
///          that long, or it is shutting down. Nothing of the batch was written — the drop
///          markers standing in for events over the size limit go in after the client's own
///          events — so a retry duplicates nothing. A batch of nothing but events over the size
///          limit is answered 200, never 503: no retry could store any of them. Also, before or
///          while it is decoded, when the batches held decoded at once are at their limit
///          (<see cref="TryBeginBatch"/>, <see cref="ReserveMore"/>), nothing of it written.
///   500 A fault in the server underneath (intern pool, logger). It leaves the handler and
///          hosting answers it; see IsMalformedPayload.
/// </summary>
public sealed class IngestionEndpoint
{
    private readonly StorageEngine              _storage;
    private readonly StringInternPool           _pool;
    private readonly ILogger<IngestionEndpoint> _logger;

    /// <summary>Max HTTP body bytes for one CLEF batch — 413 above this. From config.</summary>
    private readonly int _maxBatchBytes;

    /// <summary>
    /// Max properties bytes per event: the configured limit, never above one hot-tier chunk — an
    /// event larger than that no tier can hold, and it is better refused here, with its marker,
    /// than counted as a store error.
    /// </summary>
    private readonly int _maxEventPayloadBytes;

    /// <summary>How long a batch waits for room in the store. From config.</summary>
    private readonly TimeSpan _backPressureWait;

    /// <summary>The most one batch may decode to (<c>Ingestion.MaxDecodedLogBatchBytes</c>) — 413 above it.</summary>
    private readonly long _maxDecodedBatchBytes;

    /// <summary>The most the batches held decoded at once may hold together (<c>Ingestion.MaxDecodedLogBytesInFlight</c>).</summary>
    private readonly long _decodedInFlightLimit;

    /// <summary>What the batches held decoded right now have reserved (<see cref="TryBeginBatch"/>, <see cref="ReserveMore"/>).</summary>
    private long _decodedInFlight;

    /// <summary>The seconds a 503 asks a client to wait before retrying.</summary>
    internal const string RetryAfterSeconds = "1";

    public IngestionEndpoint(
        StorageEngine storage,
        StringInternPool pool,
        ServerOptions options,
        ILogger<IngestionEndpoint> logger)
    {
        _storage  = storage;
        _pool     = pool;
        _logger   = logger;
        _maxBatchBytes        = options.Ingestion.MaxBatchBytes;
        _maxEventPayloadBytes = (int)Math.Min(options.Ingestion.MaxEventPayloadBytes, HotTierSegment.ChunkPayloadBytes);
        _backPressureWait     = options.Ingestion.BackPressureWait;
        _maxDecodedBatchBytes = options.Ingestion.EffectiveMaxDecodedLogBatchBytes;
        _decodedInFlightLimit = options.Ingestion.EffectiveMaxDecodedLogBytesInFlight;

        if (options.Ingestion.RingCapacity is not null || options.Ingestion.PayloadPoolBytes is not null)
            _logger.LogWarning(
                "Ingestion.RingCapacity and Ingestion.PayloadPoolBytes no longer have any effect: logs are written " +
                "straight into the store by the request that carries them, and the ingest ring they sized is gone. " +
                "Ingestion.BackPressureWait and HotTier.SpillEnabled are what decide what happens when the flush is behind.");
        if (options.Ingestion.MaxEventPayloadBytes > HotTierSegment.ChunkPayloadBytes)
            _logger.LogWarning(
                "Ingestion.MaxEventPayloadBytes {Configured} B is more than one hot-tier chunk holds; events are refused " +
                "above {Effective} B", options.Ingestion.MaxEventPayloadBytes, _maxEventPayloadBytes);
        if (options.Ingestion.MaxDecodedLogBatchBytes > IngestionOptions.MaxDecodedLogBatchBytesCeiling)
            _logger.LogWarning(
                "Ingestion.MaxDecodedLogBatchBytes {Configured} B is past the most a log batch can hold; {Effective} B is used",
                options.Ingestion.MaxDecodedLogBatchBytes, _maxDecodedBatchBytes);
        if (options.Ingestion.MaxDecodedLogBytesInFlight > IngestionOptions.MaxDecodedLogBytesInFlightCeiling)
            _logger.LogWarning(
                "Ingestion.MaxDecodedLogBytesInFlight {Configured} B is past the most the log batches can hold; {Effective} B is used",
                options.Ingestion.MaxDecodedLogBytesInFlight, _decodedInFlightLimit);
    }

    /// <summary>The intern pool templates and services are interned into — the store's.</summary>
    internal StringInternPool Pool => _pool;

    /// <summary>
    /// A fresh batch, with no weight reserved for it — for a caller that bounds what it stages
    /// itself. A receiver takes its batch from <see cref="TryBeginBatch"/>. Dispose it once the
    /// request is done with it.
    /// </summary>
    /// <param name="payloadSizeHint">
    /// The payload bytes the batch is likely to hold — the body's length is a good one — so its
    /// buffer is rented once at that size rather than doubled up to it. 0: start small and grow.
    /// </param>
    public LogIngestBatch BeginBatch(int payloadSizeHint = 0) => new(this, payloadSizeHint, reserved: 0, limit: long.MaxValue, bounded: false);

    // ── Decoded-batch budgets ─────────────────────────────────────────────────

    /// <summary>
    /// The most one batch may decode to, in bytes (<c>Ingestion.MaxDecodedLogBatchBytes</c>): a
    /// receiver refuses a heavier one — 413, gRPC RESOURCE_EXHAUSTED — with nothing ingested; by its
    /// weight before decoding when that already passes it, else the moment its decode does
    /// (<see cref="LogIngestBatch"/> counts what it takes).
    /// </summary>
    public long MaxDecodedBatchBytes => _maxDecodedBatchBytes;

    /// <summary>The most the batches held decoded at once may hold together (<c>Ingestion.MaxDecodedLogBytesInFlight</c>).</summary>
    public long MaxDecodedBytesInFlight => _decodedInFlightLimit;

    /// <summary>What the batches held decoded right now have reserved together.</summary>
    public long DecodedBytesInFlight => Interlocked.Read(ref _decodedInFlight);

    /// <summary>
    /// A fresh batch for one request, <paramref name="decodedWeight"/> reserved for it against what
    /// the batches held decoded at once may hold (<see cref="MaxDecodedBytesInFlight"/>) — or null
    /// when that would take the total past it: the receiver answers 503 with Retry-After (gRPC:
    /// UNAVAILABLE), having decoded and written nothing. That is the bound on a stalled store's
    /// waiting requests, each holding its decoded batch for up to BackPressureWait. The reservation
    /// grows as the decode takes more (<see cref="ReserveMore"/>), and shrinks to what the batch holds
    /// once it is decoded. A batch is always taken when nothing else is held, so no weight is refused
    /// for good here.
    /// </summary>
    /// <param name="payloadSizeHint">See <see cref="BeginBatch"/>.</param>
    /// <param name="decodedWeight">What the request's body was weighed to decode to: the first reservation.</param>
    public LogIngestBatch? TryBeginBatch(int payloadSizeHint, long decodedWeight)
    {
        long weight = Math.Max(0, decodedWeight);
        long total  = Interlocked.Add(ref _decodedInFlight, weight);
        if (total > _decodedInFlightLimit && total > weight)   // over, and others hold some of it
        {
            Interlocked.Add(ref _decodedInFlight, -weight);
            NoteDecodedBudgetFull(weight);
            return null;
        }
        try { return new LogIngestBatch(this, payloadSizeHint, weight, _maxDecodedBatchBytes, bounded: true); }
        catch
        {
            Interlocked.Add(ref _decodedInFlight, -weight);
            throw;
        }
    }

    /// <summary>
    /// Grows a batch's reservation from <paramref name="reserved"/> to cover <paramref name="charged"/>
    /// bytes it holds: to twice what it had, at most its <paramref name="limit"/>, or to exactly what
    /// it holds when the doubling does not fit. Returns the new reservation; throws
    /// <see cref="LogBatchBudgetFullException"/> when even that would take the total past
    /// <see cref="MaxDecodedBytesInFlight"/> while other batches hold some of it.
    /// </summary>
    internal long ReserveMore(long reserved, long charged, long limit)
    {
        long doubled = reserved > long.MaxValue / 2 ? long.MaxValue : 2 * reserved;
        long want    = Math.Min(limit, Math.Max(charged, doubled));
        if (TryReserve(want - reserved, want)) return want;
        if (want > charged && TryReserve(charged - reserved, charged)) return charged;
        NoteDecodedBudgetFull(charged);
        throw new LogBatchBudgetFullException();
    }

    private bool TryReserve(long delta, long batchTotal)
    {
        long total = Interlocked.Add(ref _decodedInFlight, delta);
        if (total <= _decodedInFlightLimit || total <= batchTotal) return true;   // under, or nothing else held
        Interlocked.Add(ref _decodedInFlight, -delta);
        return false;
    }

    /// <summary>Gives back a batch's reservation (<see cref="LogIngestBatch.Dispose"/>).</summary>
    internal void ReleaseDecoded(long weight) => Interlocked.Add(ref _decodedInFlight, -weight);

    /// <summary>
    /// What a CLEF batch will decode to: <see cref="IngestionOptions.DecodedLogRecordBytes"/> an event,
    /// by the element count its array header claims (never more than its bytes could hold), and its
    /// body — a CLEF event's properties are a part of it. A body that does not start with an array
    /// header weighs its own length; its decode refuses it.
    /// </summary>
    public static long WeighClef(ReadOnlyMemory<byte> body, out long events)
    {
        events = 0;
        try
        {
            var reader = new MessagePackReader(body);
            if (reader.TryReadArrayHeader(out int count)) events = Math.Min(count, body.Length);
        }
        catch (Exception) { /* not an array: the decode says so */ }
        return events * IngestionOptions.DecodedLogRecordBytes + body.Length;
    }

    /// <summary>
    /// Says, at most once a second with the count since, that a batch was refused for weighing more
    /// than <see cref="MaxDecodedBatchBytes"/>: the client does not retry a 413, so this line is the
    /// server's only record of the lost batch.
    /// </summary>
    public void NoteTooHeavy(string? route, long weight)
    {
        if (!TakeOncePerSecond(ref _tooHeavyNextAt, ref _tooHeavySkipped, out int skipped)) return;
        try
        {
            _logger.LogWarning(
                "A log batch on {Route} would decode to about {Weight:N0} B, more than Ingestion.MaxDecodedLogBatchBytes " +
                "({Limit:N0} B): refused (413 / RESOURCE_EXHAUSTED), nothing of it ingested — the sender must split it. " +
                "{Skipped} more refused since the last of these warnings",
                route ?? "(unknown)", weight, _maxDecodedBatchBytes, skipped);
        }
        catch (Exception) { /* the refusal stands */ }
    }

    /// <summary>Says, at most once a second with the count since, that a batch found the decoded budget spent (503).</summary>
    private void NoteDecodedBudgetFull(long weight)
    {
        if (!TakeOncePerSecond(ref _budgetFullNextAt, ref _budgetFullSkipped, out int skipped)) return;
        try
        {
            _logger.LogWarning(
                "A log batch of about {Weight:N0} B decoded was asked to retry (503 / UNAVAILABLE): the batches held " +
                "decoded already hold {InFlight:N0} B of the {Limit:N0} B allowed at once " +
                "(Ingestion.MaxDecodedLogBytesInFlight) — the store is behind and requests are waiting for room. " +
                "{Skipped} more asked to retry since the last of these warnings",
                weight, DecodedBytesInFlight, _decodedInFlightLimit, skipped);
        }
        catch (Exception) { /* the 503 stands */ }
    }

    private long _tooHeavyNextAt   = long.MinValue, _budgetFullNextAt  = long.MinValue;
    private int  _tooHeavySkipped, _budgetFullSkipped;

    /// <summary>The once-a-second gate both lines above share the shape of (see <see cref="TakeOversizedWarning"/>).</summary>
    private bool TakeOncePerSecond(ref long nextAt, ref int skippedSince, out int skipped)
    {
        var  clock = Clock;
        long now   = clock.GetTimestamp();
        long from  = Volatile.Read(ref nextAt);
        if (now >= from && Interlocked.CompareExchange(ref nextAt, now + clock.TimestampFrequency, from) == from)
        {
            skipped = Interlocked.Exchange(ref skippedSince, 0);
            return true;
        }
        Interlocked.Increment(ref skippedSince);
        skipped = 0;
        return false;
    }

    // ── Counters (/api/diagnostics) ───────────────────────────────────────────

    private long _acceptedTotal;
    private long _droppedOversized;

    /// <summary>Events written into the store since start, drop markers included.</summary>
    public long AcceptedTotal => Interlocked.Read(ref _acceptedTotal);

    /// <summary>
    /// Events refused at the door for a properties payload over the per-event limit — counted by
    /// the answer that ends their request, so a batch answered 503 is counted by its retry, once.
    /// </summary>
    public long DroppedOversized => Interlocked.Read(ref _droppedOversized);

    // ── The oversized-drop Warning, rate-limited ──────────────────────────────

    /// <summary>
    /// The oversized-drop Warning is written at most once in this long; the drops in between are
    /// counted into the next one. It is written with the answer that ends a request (a 503 is
    /// staged again by its retry, which is not a new drop), so the count is of drops, not of
    /// attempts; a producer sending oversized events in bulk wrote one line per event. A drop whose
    /// marker the store had no room for is named whatever this says (see LogOversizedDrops).
    /// Internal, so a test that injects its fault through that log call can lift the limit.
    /// </summary>
    internal TimeSpan OversizedWarningInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>The clock <see cref="OversizedWarningInterval"/> is measured by; a test's own moves only when it says.</summary>
    internal TimeProvider Clock { get; set; } = TimeProvider.System;

    /// <summary>
    /// Test seam: runs as each record is staged, before anything of it is; throwing from it is the
    /// decode itself running out of memory — the server's failure, answered 503 (gRPC UNAVAILABLE)
    /// with nothing written. Null in production.
    /// </summary>
    internal Action? BeforeStageForTest { get; set; }

    /// <summary>The <see cref="Clock"/> timestamp from which the next Warning may be written.</summary>
    private long _oversizedWarnFrom = long.MinValue;

    /// <summary>Drops since the last Warning that wrote none of their own.</summary>
    private int _oversizedWarnSkipped;

    /// <summary>
    /// Whether this drop writes its Warning, with <paramref name="skipped"/> the drops since the
    /// last one that did not — the shape of OtlpOutOfMemoryLog's line. Lock-free: of two drops in
    /// the same instant, one writes it.
    /// </summary>
    private bool TakeOversizedWarning(out int skipped)
    {
        var  clock = Clock;
        long now   = clock.GetTimestamp();
        long from  = Volatile.Read(ref _oversizedWarnFrom);
        long step  = (long)(OversizedWarningInterval.TotalSeconds * clock.TimestampFrequency);
        if (now >= from && Interlocked.CompareExchange(ref _oversizedWarnFrom, now + step, from) == from)
        {
            skipped = Interlocked.Exchange(ref _oversizedWarnSkipped, 0);
            return true;
        }
        Interlocked.Increment(ref _oversizedWarnSkipped);
        skipped = 0;
        return false;
    }

    private void LogOversizedDrop(int payloadBytes, string service, string template, int skipped)
    {
        if (skipped == 0)
            _logger.LogWarning(
                "Dropped oversized log event: properties {PayloadBytes} B exceed limit {LimitBytes} B (service={Service}, template=\"{Template}\")",
                payloadBytes, _maxEventPayloadBytes, service, template);
        else
            _logger.LogWarning(
                "Dropped oversized log event: properties {PayloadBytes} B exceed limit {LimitBytes} B (service={Service}, template=\"{Template}\"); " +
                "{Unlogged} more dropped since the last of these warnings, which are written at most once a second",
                payloadBytes, _maxEventPayloadBytes, service, template, skipped);
    }

    // ── Commit ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Begins writing a decoded batch into the store: what a commit does before the write's wait,
    /// and the write itself, synchronously. Called by <see cref="LogIngestBatch.BeginCommit"/>; the
    /// caller awaits <see cref="LogCommit.Write"/> and hands what it came to to <see cref="EndCommit"/>.
    ///
    /// <para><b>No frame of its own between the request and the write</b> (round-6 review R5-M4). The
    /// commit was an async method, and its first real suspension — where its state machine is boxed —
    /// was its await of the write, AFTER the write began. A box that could not be allocated threw there
    /// with the write going on. It was waited out on the request's thread, for the write's wait and a
    /// second more: a pool thread the write's own room-wait continuation needed, so on two cores a burst
    /// of them stalled the whole pool (a trivial work item delayed 8.6–9.7 s at p99 in the review's
    /// probe). Past that the failure went up — a 500 — while the write went on and stored the batch,
    /// which a Seq client's retry then stored a second time (2 rounds in 10). The receivers await the
    /// write themselves now, on the state machine they boxed before the batch was begun (round-5 H-G):
    /// that await allocates nothing, so nothing between the write's start and its end can fail but the
    /// write, which answers for what it did and never throws for a write that fails.</para>
    /// </summary>
    internal LogCommit BeginCommit(LogIngestBatch batch, CancellationToken ct)
    {
        // Decoded: for the wait for room, the batch keeps reserved what it holds and no more.
        batch.TrimReservation();

        // The drop markers join the batch now, AFTER every one of the client's events: a write that
        // stops before the first of the client's events then stops before every marker too. Into
        // the room kept for them as they were staged — this grows nothing, so it cannot run out of
        // memory here, outside the receivers' decode.
        var staged = batch.Seal();

        // What the answer is made of, read BEFORE the write: the write's lease ends inside it, and a
        // dispose asked meanwhile (deferred to that end) zeroes the batch — read after, the client's
        // event count was 0 and every marker counted as written.
        int stagedCount = staged.Count;
        if (stagedCount == 0)   // no marker staged, so nothing was refused: nothing to write
            return new LogCommit(default, 0, 0, 0, batch.DroppedAtDoor, batch.Malformed);
        return new LogCommit(_storage.WriteBatchAsync(staged, _backPressureWait, ct),
                             stagedCount, batch.StagedEvents, batch.MarkerCount, batch.DroppedAtDoor, batch.Malformed);
    }

    /// <summary>
    /// Says what became of a batch whose write (<see cref="LogCommit.Write"/>, begun by
    /// <see cref="BeginCommit"/>) came to <paramref name="r"/>: the counts the answer gives, and the
    /// lines its drops are named in. Called by <see cref="LogIngestBatch.EndCommit"/>. Synchronous; it
    /// allocates nothing but those lines, each guarded — nothing here makes a written batch a failure.
    /// </summary>
    internal LogIngestResult EndCommit(LogIngestBatch batch, in LogCommit commit, LogBatchWriteResult r)
    {
        int droppedAtDoor = commit.DroppedAtDoor;
        if (commit.Staged == 0)
            return new LogIngestResult(0, droppedAtDoor, Busy: false);
        int stagedCount  = commit.Staged;
        int clientEvents = commit.ClientEvents;
        int markerCount  = commit.Markers;
        Interlocked.Add(ref _acceptedTotal, r.Written);

        // Drop markers stand in for events refused at the door: they are written like any other
        // event, but they are not what the client sent, and the client's counts are of its own.
        // The batch was processed up to `processed`; what follows was not written.
        int processed      = r.Written + r.Refused;
        int markersWritten = Math.Clamp(processed - clientEvents, 0, markerCount);
        int markersUnsent  = markerCount - markersWritten;
        int ingested       = r.Written - markersWritten;
        int clientUnsent   = r.NotWritten - markersUnsent;
        int dropped        = droppedAtDoor + r.Refused + clientUnsent;

        // Best-effort, and never the request's failure: this runs after the write, and a log call that
        // ran out of memory here (its arguments box) made a batch the store had partly written a 500,
        // with the count of what it wrote lost and a Seq client's retry writing that part twice.
        if (r.NotWritten > 0 && _logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Debug))
        {
            try
            {
                _logger.LogDebug("Log batch: {NotWritten} of {Staged} event(s) not written — the store had no room within {Wait}",
                    r.NotWritten, stagedCount, _backPressureWait);
            }
            catch (Exception) { /* the line is lost; the answer is not */ }
        }

        // Nothing written, and some of the client's events were waiting: the caller answers "retry
        // later", and the retry duplicates nothing. Nothing at all, markers included: they come
        // after the client's events, so a write that stopped before the first of those wrote none
        // of them. A batch of nothing but markers is never busy. No retry could store any of its
        // events — it could only send them again, each one over the size limit, to write the
        // markers — so it is answered for good, and a marker the store had no room for is not
        // written: the drop is then named in a Warning of its own (LogOversizedDrops, below).
        bool busy = r.Written == 0 && clientUnsent > 0;

        // Events refused at the door are COUNTED by the answer that ends the request: a 503 is
        // staged again by its retry, and one oversized event in a batch retried each second for
        // three minutes read as ~180 drops. A 400 (malformed) is final whatever the store did. The
        // lines that name them are written here too, for the same reason: logged as each attempt was
        // decoded, a retried batch wrote one per attempt, and the count of drops in the next line
        // counted attempts (~170 for 20 drops). Guarded: a line that runs out of memory after the
        // write must not make a written batch a 500.
        if (!busy || commit.Malformed)
        {
            Interlocked.Add(ref _droppedOversized, droppedAtDoor);
            if (markerCount > 0) LogOversizedDrops(batch, markersWritten);
        }
        return new LogIngestResult(ingested, dropped, busy);
    }

    /// <summary>
    /// The unwritten-marker Warnings a request writes on its own before the rest are summed up in one
    /// line: they are written whatever the once-a-second limit says (each is the only record of its
    /// drop), and a body of 8 MiB at a 1 KiB limit holds ~8 000 — a flood of the bounded file-log queue
    /// and the Event Log.
    /// </summary>
    internal const int UnwrittenMarkerWarningsPerRequest = 20;

    /// <summary>
    /// Names the events a finally answered request dropped at the door. A drop whose marker was
    /// written is named at most once a second (its marker names it on the Events page); one whose
    /// marker the store had no room for, after the client's own events, is ALWAYS named — this line
    /// is the only record of it, and suppressing it with the rest left a drop no one could find. A
    /// request names the first <see cref="UnwrittenMarkerWarningsPerRequest"/> of those, and the rest
    /// in one line with their count and their total size. Never throws.
    /// </summary>
    private void LogOversizedDrops(LogIngestBatch batch, int markersWritten)
    {
        try
        {
            int  unnamed = 0;
            long unnamedBytes = 0;
            for (int i = 0; i < batch.MarkerCount; i++)
            {
                var drop = batch.DropAt(i);
                if (i >= markersWritten)
                {
                    if (i - markersWritten >= UnwrittenMarkerWarningsPerRequest)
                    {
                        unnamed++;
                        unnamedBytes += drop.PayloadBytes;
                        continue;
                    }
                    _logger.LogWarning(
                        "Dropped oversized log event: properties {PayloadBytes} B exceed limit {LimitBytes} B (service={Service}, template=\"{Template}\"); " +
                        "the store had no room for its marker after the batch's own events, so this line is the only record of it",
                        drop.PayloadBytes, _maxEventPayloadBytes, drop.Service, drop.Template);
                }
                else if (TakeOversizedWarning(out int skipped))
                    LogOversizedDrop(drop.PayloadBytes, drop.Service, drop.Template, skipped);
            }
            if (unnamed > 0)
                _logger.LogWarning(
                    "Dropped {Count} more oversized log event(s) of the same request ({Bytes} B of properties, each over {LimitBytes} B), " +
                    "whose markers the store had no room for: this line is the only record of them",
                    unnamed, unnamedBytes, _maxEventPayloadBytes);
        }
        catch (Exception) { /* the lines are lost; the answer is not */ }
    }

    // ── POST /api/events (CLEF) ───────────────────────────────────────────────

    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(OwnerTaskMethodBuilder))]
    public async Task HandleAsync(HttpContext ctx)
    {
        // Boxed first, owning nothing (round-5 H-G, M-D). A state machine is boxed at its first real
        // suspension, and a box that cannot be allocated throws there with every finally of the method
        // skipped while what it awaited runs on: a body read writing into a buffer handed back, or —
        // the body read whole without suspending — the commit, which went on to write the batch whose
        // reservation the skipped finally never gave back. The handler waited such a commit out on its
        // thread (ce9e57c), a pool thread the commit itself needed: under a burst on two cores the whole
        // pool stalled for seconds, and the bound then expired into a 500. Here nothing is held yet: a
        // failure is a 500 for a request nothing was taken from, which a Seq client retries.
        await Task.Yield();

        // ── 1. Read body into a pooled buffer ─────────────────────────────────
        long? contentLength = ctx.Request.ContentLength;
        if (contentLength > _maxBatchBytes)
        {
            ctx.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
            return;
        }

        // From IngestBufferPool, the pool the OTLP receivers read into, not ArrayPool.Shared: a
        // 1.4 MB batch rounds up to the 2 MB bucket, and past Shared's shallow per-core depth
        // every concurrent request got a fresh array on the large object heap. Every buffer
        // below goes back to IngestBufferPool and nowhere else — one handed to Shared is not a
        // crash, it is this pool emptying one request at a time.
        //
        // Rented, and straight into the try: a read that THROWS must still give the buffer back.
        // Kestrel raises BadHttpRequestException for a body that ends short of its
        // Content-Length, and RequestAborted cancels a read. Both used to fire before the try
        // opened, so the rented array was never returned. Every exit below, the 413 included,
        // returns it exactly once: in the finally.
        byte[]? bodyBuf = IngestBufferPool.Rent(
            contentLength.HasValue ? Math.Max((int)contentLength.Value, 1) : 64 * 1024);
        int     bodyLen = 0;
        LogIngestBatch? batch = null;
        try
        {
            if (contentLength.HasValue)
            {
                int expected = (int)contentLength.Value;
                while (bodyLen < expected)
                {
                    int n = await ctx.Request.Body.ReadAsync(
                        bodyBuf.AsMemory(bodyLen, expected - bodyLen), ctx.RequestAborted);
                    if (n == 0) break;
                    bodyLen += n;
                }
            }
            else
            {
                while (true)
                {
                    int n = await ctx.Request.Body.ReadAsync(
                        bodyBuf.AsMemory(bodyLen), ctx.RequestAborted);
                    if (n == 0) break;
                    bodyLen += n;

                    if (bodyLen > _maxBatchBytes)
                    {
                        ctx.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
                        return;   // the finally returns the buffer
                    }

                    if (bodyLen == bodyBuf.Length)
                    {
                        byte[] bigger = IngestBufferPool.Rent(bodyBuf.Length * 2);
                        Buffer.BlockCopy(bodyBuf, 0, bigger, 0, bodyLen);
                        // Swap first, return second: from the swap on, the finally owns `bigger`,
                        // so even a Return that threw could not send `smaller` back twice.
                        byte[] smaller = bodyBuf;
                        bodyBuf = bigger;
                        IngestBufferPool.Return(smaller);
                    }
                }
            }

            // ── 1b. A body that never arrived in full is refused whole ────────
            // Defense in depth, NOT the Kestrel path. On Kestrel a body short of its
            // Content-Length never gets here: ReadAsync throws BadHttpRequestException
            // ("Unexpected end of request content") above, whether the client sends FIN or
            // RST, and hosting answers it. This guard is for a body stream that just ends
            // early instead of throwing — TestServer, or another server. On such a stream the
            // prefix parses perfectly up to the cut, so streaming it would ingest the prefix
            // and then answer 400. Seq clients retry a non-2xx (Serilog.Sinks.Seq throws, and
            // its batching sink retries), and the prefix would land again on every retry.
            // A body that arrives IN FULL and is malformed in the middle is the residual case:
            // it still ingests the prefix, see StreamBatch.
            if (contentLength.HasValue && bodyLen != (int)contentLength.Value)
            {
                _logger.LogDebug(
                    "Truncated ingestion body: {Received} of {Expected} bytes — batch refused whole",
                    bodyLen, contentLength.Value);
                // The same counts shape every other /api/events reply has: nothing landed.
                await WriteCountsAsync(ctx, StatusCodes.Status400BadRequest, ingested: 0, dropped: 0);
                return;
            }

            // ── 2+3. Decode the MessagePack array into the batch ──────────────
            // No LogEvent per event: the batch reader hands each event over as spans into
            // bodyBuf, and the batch copies the property bytes it keeps. A body that is not a
            // CLEF array is rejected before any event is seen; one that turns malformed part way
            // through answers 400 with the intact prefix written (see StreamBatch).
            // Weighed first, by its array header: a body of many tiny events decodes to many times
            // its size (a staged event costs ~80 B besides its properties, and an event can be one
            // byte on the wire), and it is decoded whole before any of it is written. Over the
            // limit it is refused as what it is, too large; under it, its weight is reserved
            // against what the batches held decoded at once may weigh, or it is asked to retry.
            long weight = WeighClef(bodyBuf.AsMemory(0, bodyLen), out long events);
            if (weight > _maxDecodedBatchBytes)
            {
                NoteTooHeavy(ctx.Request.Path.Value, weight);
                ctx.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
                return;   // the finally returns the buffer
            }

            // Sized from the body: a CLEF event's properties are a part of its map, so the decoded
            // payloads never outgrow the body that carried them, and the batch is one rent.
            batch = TryBeginBatch(payloadSizeHint: bodyLen, weight);
            if (batch is null)
            {
                ctx.Response.Headers.RetryAfter = RetryAfterSeconds;
                await WriteCountsAsync(ctx, StatusCodes.Status503ServiceUnavailable, ingested: 0,
                                       dropped: (int)Math.Min(events, int.MaxValue));
                return;
            }
            var progress = default(LogEventSerializer.ClefBatchProgress);
            Exception? malformed = null;
            try
            {
                LogEventSerializer.StreamBatch(bodyBuf.AsMemory(0, bodyLen), batch, ref progress);
            }
            // What the decode held passed the batch's limit — counted as it was taken, the weight
            // above being only an estimate: refused as what it is, too large, and nothing of it
            // written (the finally gives the batch back unwritten).
            catch (LogBatchTooHeavyException ex)
            {
                NoteTooHeavy(ctx.Request.Path.Value, ex.Weight);
                ctx.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
                return;
            }
            // Its reservation could not grow: the batches held at once are at their limit. Nothing
            // written, so the retry the 503 asks for duplicates nothing.
            catch (LogBatchBudgetFullException)
            {
                // Given back before the answer is awaited (round-5 review L-7): an await whose
                // registration fails skips the finally below, and with it the batch's dispose — its
                // reservation of the decoded budget lost for good.
                batch.Dispose();
                batch = null;
                ctx.Response.Headers.RetryAfter = RetryAfterSeconds;
                await WriteCountsAsync(ctx, StatusCodes.Status503ServiceUnavailable, ingested: 0,
                                       dropped: (int)Math.Min(events, int.MaxValue));
                return;
            }
            // Classified by WHERE it was thrown, not by its type. This used to be
            // catch(Exception), which also swallowed failures of the sink underneath — an
            // intern-pool fault, or the logger — and reported them to the client as "malformed
            // payload"; those surface as 500. A list of reader exception types was tried and was
            // short on day one: a str32/bin32/ext32 length prefix of 2^31 or more throws
            // OverflowException, and went out as 500.
            catch (Exception ex) when (IsMalformedPayload(ex, progress.InSink))
            {
                malformed       = ex;
                batch.MarkMalformed();   // answered 400 below, whatever the store does
            }

            // The body is decoded into the batch: give it back before the write, which may wait.
            IngestBufferPool.Return(bodyBuf);
            bodyBuf = null;

            // ── 4. Write, then answer ─────────────────────────────────────────
            // The write is awaited HERE, on the box made above (round-6 review R5-M4): that await
            // allocates nothing, so nothing between the write's start and its end can fail but the
            // write, which answers for what it did. The batch is given back as soon as it is written,
            // before the answer, which a slow client can hold up.
            var commit  = batch.BeginCommit(ctx.RequestAborted);
            var written = await commit.Write;
            LogIngestResult result = batch.EndCommit(commit, written);
            batch.Dispose();
            batch = null;

            if (malformed is not null)
            {
                // Warning, not Debug, and with the counts: "some of that batch landed and some of
                // it did not" is an operator's problem, and the element index is what makes it
                // findable in the sender. ElementIndex -1 is the array header itself (the reply
                // then omits failedAtElement).
                _logger.LogWarning(malformed,
                    "Malformed ingestion payload at element {ElementIndex} (-1 = the array header) of {ElementCount}: "
                  + "{Ingested} event(s) already ingested, {Dropped} dropped — batch refused",
                    progress.ElementIndex, progress.ElementCount, result.Ingested, result.Dropped);

                await WriteCountsAsync(ctx, StatusCodes.Status400BadRequest, result.Ingested, result.Dropped,
                                       failedAtElement: progress.ElementIndex);
                return;
            }

            if (result.Busy)
            {
                ctx.Response.Headers.RetryAfter = RetryAfterSeconds;
                await WriteCountsAsync(ctx, StatusCodes.Status503ServiceUnavailable, result.Ingested, result.Dropped);
                return;
            }

            await WriteCountsAsync(ctx, StatusCodes.Status200OK, result.Ingested, result.Dropped);
        }
        finally
        {
            if (bodyBuf is not null) IngestBufferPool.Return(bodyBuf);
            batch?.Dispose();
        }
    }

    private static async Task WriteCountsAsync(HttpContext ctx, int status, int ingested, int dropped, int failedAtElement = -1)
    {
        ctx.Response.StatusCode  = status;
        ctx.Response.ContentType = "application/json";
        WriteCountsJson(ctx.Response.BodyWriter, ingested, dropped, failedAtElement);
        await ctx.Response.BodyWriter.FlushAsync(ctx.RequestAborted);
    }

    /// <summary>
    /// Whether a throw out of StreamBatch is the client's (400) or the server's (500), decided
    /// by where it happened. <paramref name="inSink"/> is
    /// <see cref="LogEventSerializer.ClefBatchProgress.InSink"/> as the throw left it.
    ///
    /// <para>Set: the fault came out of the sink — the intern pool, the logger — and is the
    /// server's, whatever its type.</para>
    ///
    /// <para>Clear: the reader threw while walking the body, and the body is at fault whatever
    /// the TYPE. MessagePackReader throws MessagePackSerializationException for a code it cannot
    /// read, EndOfStreamException for a body that ends inside an element, and OverflowException
    /// for a str32/bin32/ext32 length prefix of 2^31 or more. A list of those types is one
    /// reader upgrade away from being short again. The one exception is
    /// <see cref="OutOfMemoryException"/>, which says nothing about the body.</para>
    /// </summary>
    internal static bool IsMalformedPayload(Exception ex, bool inSink)
        => !inSink && ex is not OutOfMemoryException;

    /// <summary>
    /// Writes <c>{"ingested":N,"dropped":M}</c> into the response buffer with no string in
    /// between — plus <c>,"failedAtElement":K</c> when the batch was refused part way, so a
    /// sender reading the 400 can tell how much of it landed and where it stopped. The
    /// interpolated form allocated the formatted string, a char[] from the handler, and then
    /// a UTF-8 transcode of it — ~300 B per request, for 30-odd bytes of constant-shaped
    /// JSON. Every count is a non-negative int, so the longest possible body is well under
    /// the requested span.
    /// </summary>
    private static void WriteCountsJson(
        System.IO.Pipelines.PipeWriter writer, int ingested, int dropped, int failedAtElement = -1)
    {
        const int MaxLen = 24 + 19 + 11 + 11 + 11; // literals + three int32s at their widest
        Span<byte> span = writer.GetSpan(MaxLen);
        int pos = 0;

        "{\"ingested\":"u8.CopyTo(span);
        pos += 12;
        System.Buffers.Text.Utf8Formatter.TryFormat(ingested, span[pos..], out int written);
        pos += written;

        ",\"dropped\":"u8.CopyTo(span[pos..]);
        pos += 11;
        System.Buffers.Text.Utf8Formatter.TryFormat(dropped, span[pos..], out written);
        pos += written;

        if (failedAtElement >= 0)
        {
            ",\"failedAtElement\":"u8.CopyTo(span[pos..]);
            pos += 19;
            System.Buffers.Text.Utf8Formatter.TryFormat(failedAtElement, span[pos..], out written);
            pos += written;
        }

        span[pos++] = (byte)'}';
        writer.Advance(pos);
    }

    // ── Staging (called by LogIngestBatch) ────────────────────────────────────

    /// <summary>
    /// Stages one already-msgpack-encoded record — the OTLP parsers' road. Interns template and
    /// service straight from UTF-8 (no string allocated on a cache hit). False when the record was
    /// refused at the door (oversized: logged, and a marker staged in its place).
    /// </summary>
    internal bool StageRaw(
        LogIngestBatch batch,
        long tsTicks, byte level,
        ReadOnlySpan<byte> templateUtf8,
        ReadOnlySpan<byte> msgpackProps,
        ulong traceHi, ulong traceLo, ulong spanId,
        ReadOnlySpan<byte> serviceUtf8,
        int serviceIdx)
    {
        BeforeStageForTest?.Invoke();
        serviceUtf8 = ServiceName(serviceUtf8);
        // The service name belongs to the resourceLogs block, not the record: when the
        // parser has already interned it, every record under that block skips the
        // UTF-8 decode + hash that used to run once per record. A full pool answers -1 with the
        // name materialised, and it is then kept beside the event, as a template is (the name
        // used to be lost from every event ingested past saturation); -1 and null when empty.
        string? service = null;
        if (serviceIdx < 0 && !serviceUtf8.IsEmpty)
        {
            serviceIdx = _pool.Intern(serviceUtf8, out string svcText, ref batch.Share);
            if (serviceIdx < 0 && svcText.Length != 0) service = svcText;
        }

        if (msgpackProps.Length > _maxEventPayloadBytes)
            return StageOversizedCore(batch, tsTicks, level, templateUtf8, msgpackProps.Length, traceHi, traceLo, spanId, serviceUtf8, serviceIdx, service);

        // Intern hands back the pool's OWN instance, so the tier stores the shared string
        // rather than a per-event duplicate — and one dictionary probe does the work of two.
        int     tmplIdx = _pool.Intern(templateUtf8, out string canonicalTmpl, ref batch.Share); // -1 when empty, or past the request's share
        // Attach on CONTENT, not on the index. A full pool answers -1 with the template
        // materialised in canonicalTmpl, and past that point the attached string is the only
        // copy of it anywhere: the header says -1 and pool.Get(-1) is "". Keying this on
        // tmplIdx >= 0 dropped @mt from every event ingested after saturation. Only an empty
        // template attaches null — never "", which the tier would prefer over the pool.
        string? tmpl    = canonicalTmpl.Length != 0 ? canonicalTmpl : null;

        // What a full pool made for this record alone is held by the batch: charged to it, as its
        // arrays are. A pooled string is shared and costs the batch nothing.
        if (tmplIdx < 0 && tmpl is not null) batch.ChargeString(tmpl);
        if (service is not null)             batch.ChargeString(service);

        batch.Add(tsTicks, level, tmplIdx, tmpl, exception: null, msgpackProps, traceHi, traceLo, spanId, serviceIdx, service);
        return true;
    }

    /// <summary>The longest property map an event may have (<c>Ingestion.MaxEventPayloadBytes</c>, at most a hot-tier chunk).</summary>
    internal int MaxEventPayloadBytes => _maxEventPayloadBytes;

    /// <summary>
    /// The longest <c>@service</c> an event keeps, in UTF-8 bytes: a longer one is cut (at a whole
    /// character). A service is an identifier, and it rides with every event of its batch: one of
    /// 70 KB, pooled but too long for a WAL pool row, was carried in EVERY entry of the log — 1 000
    /// events made a 70 MB WAL, 3 500 a 230 MB one whose replay ran a 384 MiB heap out at every start.
    /// </summary>
    internal const int MaxServiceNameBytes = 1024;

    /// <summary><paramref name="utf8"/> cut to <see cref="MaxServiceNameBytes"/>, at the start of the character the cut would split.</summary>
    internal static ReadOnlySpan<byte> ServiceName(ReadOnlySpan<byte> utf8)
    {
        if (utf8.Length <= MaxServiceNameBytes) return utf8;
        int cut = MaxServiceNameBytes;
        // On a continuation byte: back to its lead — three at most, the most a character has. A longer
        // run is not UTF-8, and the decode makes U+FFFD of it whatever the cut.
        for (int back = 0; back < 3 && (utf8[cut] & 0xC0) == 0x80; back++) cut--;
        return utf8[..cut];
    }

    /// <summary>
    /// An event the parser refused before it built its property map, which would have been
    /// <paramref name="payloadBytes"/> long (see <see cref="IOtlpLogSink.TryIngestOversized"/>):
    /// staged as <see cref="StageRaw"/> stages an oversized one. Returns false.
    /// </summary>
    internal bool StageOversized(
        LogIngestBatch batch,
        long tsTicks, byte level,
        ReadOnlySpan<byte> templateUtf8,
        long payloadBytes,
        ulong traceHi, ulong traceLo, ulong spanId,
        ReadOnlySpan<byte> serviceUtf8,
        int serviceIdx)
    {
        BeforeStageForTest?.Invoke();
        serviceUtf8 = ServiceName(serviceUtf8);
        string? service = null;
        if (serviceIdx < 0 && !serviceUtf8.IsEmpty)
        {
            serviceIdx = _pool.Intern(serviceUtf8, out string svcText, ref batch.Share);
            if (serviceIdx < 0 && svcText.Length != 0) service = svcText;
        }
        return StageOversizedCore(batch, tsTicks, level, templateUtf8, payloadBytes, traceHi, traceLo, spanId, serviceUtf8, serviceIdx, service);
    }

    /// <summary>
    /// Drops an oversized event, but leaves a compact Error breadcrumb in the stream so the loss is
    /// visible on the Events page — not only in the server's own log. Marked DroppedBy=server to
    /// distinguish it from the client sink's own oversized marker.
    ///
    /// <para>Cold path, one per dropped event: the strings are worth their cost. The log line that
    /// names the producer is written with the answer that ends the request (EndCommit): the drop is
    /// counted there, once, not per attempt, and a drop whose marker the store had no room for is
    /// named there. The service comes from the span when the caller passed one, else from the pool:
    /// a parser that interned the block's service.name once may hand over only the index.</para>
    /// </summary>
    private bool StageOversizedCore(
        LogIngestBatch batch,
        long tsTicks, byte level,
        ReadOnlySpan<byte> templateUtf8,
        long payloadBytes,
        ulong traceHi, ulong traceLo, ulong spanId,
        ReadOnlySpan<byte> serviceUtf8,
        int serviceIdx, string? service)
    {
        int    bytes    = (int)Math.Min(payloadBytes, int.MaxValue);
        string origTmpl = MarkerTemplate(templateUtf8);
        string svcStr   = !serviceUtf8.IsEmpty ? StringInternPool.Decode(serviceUtf8)
                        : serviceIdx >= 0      ? _pool.Get(serviceIdx)
                        : string.Empty;
        StageServerDropMarker(batch, tsTicks, level, origTmpl, bytes, traceHi, traceLo, spanId, serviceIdx, service,
                              new LogIngestBatch.OversizedDrop(bytes, svcStr.Length != 0 ? svcStr : "(none)", Truncate(origTmpl, 120)));
        return false; // original counted as dropped by the caller
    }

    /// <summary>
    /// Stages one CLEF event straight from the request body, with no <see cref="LogEvent"/> in
    /// between: the same duties as <see cref="StageRaw"/> (oversized warning + server drop marker,
    /// template/service interning), with everything arriving as spans over the pooled body buffer.
    /// </summary>
    internal bool StageClef(
        LogIngestBatch batch,
        long tsTicks,
        byte level,
        ReadOnlySpan<byte> templateUtf8,
        ExceptionInfo? exception,
        ReadOnlySpan<byte> msgpackProps,
        ulong traceHi, ulong traceLo, ulong spanId,
        ReadOnlySpan<byte> serviceUtf8)
    {
        BeforeStageForTest?.Invoke();
        serviceUtf8 = ServiceName(serviceUtf8);
        if (msgpackProps.Length > _maxEventPayloadBytes)
        {
            // Cold path: the strings here are worth their cost — this is one marker event per
            // dropped event, not per event. The drop is COUNTED, and the line that names it written,
            // with the answer that ends the request (EndCommit), as in StageRaw.
            string  tmplStr = MarkerTemplate(templateUtf8);
            string? svcStr  = serviceUtf8.IsEmpty  ? null         : StringInternPool.Decode(serviceUtf8);

            int markerSvcIdx = svcStr is not null ? _pool.Intern(svcStr, out _, ref batch.Share) : -1;
            StageServerDropMarker(
                batch, tsTicks, level, tmplStr, msgpackProps.Length, traceHi, traceLo, spanId,
                markerSvcIdx, markerSvcIdx < 0 ? svcStr : null,
                new LogIngestBatch.OversizedDrop(msgpackProps.Length, svcStr ?? "(none)", Truncate(tmplStr, 120)));
            return false;
        }

        // Intern returns the pool's own instance, so the hot tier shares one string per
        // template instead of retaining this event's copy (see StageRaw).
        int     tmplIdx  = _pool.Intern(templateUtf8, out string canonical, ref batch.Share); // -1 when empty, or past the share
        string? tmpl     = canonical.Length != 0 ? canonical : null;   // see StageRaw: kept past saturation, never ""
        int     svcIdx   = _pool.Intern(serviceUtf8, out string svcText, ref batch.Share);   // -1 when empty
        string? service  = svcIdx < 0 && svcText.Length != 0 ? svcText : null;   // see StageRaw: kept past saturation

        // Charged as in StageRaw — and the event's @x, decoded into a tree of strings for it alone.
        if (tmplIdx < 0 && tmpl is not null) batch.ChargeString(tmpl);
        if (service is not null)             batch.ChargeString(service);
        if (exception is not null)           batch.ChargeException(exception);

        batch.Add(tsTicks, level, tmplIdx, tmpl, exception, msgpackProps, traceHi, traceLo, spanId, svcIdx, service);
        return true;
    }

    /// <summary>
    /// Serialised template of the server-side oversized-drop marker. The
    /// placeholders match the property keys below so the message renders with the
    /// numbers filled in. Interned once (low cardinality).
    /// </summary>
    private const string DropMarkerTemplate =
        "Ameto server dropped an oversized log event: properties {EventBodyBytes} B exceed limit {EventBodyLimitBytes} B ({OriginalTemplate})";

    /// <summary>The most of a refused event's template its drop marker keeps, in UTF-16 units.</summary>
    private const int MarkerTemplateChars = 256;

    /// <summary>
    /// The start of a refused event's template — what its drop marker keeps (<see cref="MarkerTemplateChars"/>)
    /// and its drop line names (120 units) — decoded from no more of its UTF-8 than that can take, four bytes
    /// a unit at the most, U+FFFD for what is not UTF-8, allocating nothing but the string (round-5 review
    /// L-13). The whole template was decoded, through Encoding.UTF8's replacement fallback, ~35 B for each
    /// byte that is not UTF-8 — 140 MB for one record of a 4 MB body, every record refused unassembled — and
    /// all but its first 256 units dropped.
    /// </summary>
    private static string MarkerTemplate(ReadOnlySpan<byte> utf8) =>
        utf8.IsEmpty ? string.Empty : StringInternPool.Decode(utf8[..Math.Min(utf8.Length, 4 * MarkerTemplateChars)]);

    /// <summary>
    /// Stages a compact Error event standing in for a dropped oversized one,
    /// preserving its timestamp / trace-span / service. The marker is tiny (the
    /// original template is truncated) so it can never itself be oversized, and it
    /// carries <c>DroppedBy=server</c> to set it apart from the client sink's marker.
    /// </summary>
    private void StageServerDropMarker(
        LogIngestBatch batch,
        long tsTicks, byte originalLevel, string originalTemplate,
        int payloadBytes, ulong traceHi, ulong traceLo, ulong spanId, int serviceIdx, string? service,
        LogIngestBatch.OversizedDrop drop)
    {
        string origTmpl  = originalTemplate.Length <= MarkerTemplateChars ? originalTemplate : originalTemplate[..MarkerTemplateChars];
        string origLevel = ((Ameto.Core.LogLevel)originalLevel).ToString();

        var buf = new ArrayBufferWriter<byte>(384);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(5);
        w.Write("DroppedBy");           w.Write("server");
        w.Write("EventBodyBytes");      w.Write(payloadBytes);
        w.Write("EventBodyLimitBytes"); w.Write(_maxEventPayloadBytes);
        w.Write("OriginalTemplate");    w.Write(origTmpl);
        w.Write("OriginalLevel");       w.Write(origLevel);
        w.Flush();

        int mtIdx = _pool.Intern(DropMarkerTemplate);
        batch.AddMarker(tsTicks, (byte)Ameto.Core.LogLevel.Error, mtIdx, DropMarkerTemplate,
                        buf.WrittenMemory, traceHi, traceLo, spanId, serviceIdx, service, drop);
    }

    /// <summary>
    /// The template as the drop Warning names it: at most <paramref name="max"/> UTF-16 units and
    /// an ellipsis, or "(none)".
    ///
    /// <para>Never cut between the halves of a surrogate pair. A cut after a high surrogate left
    /// half an emoji or CJK extension character at the end of the logged template: an unpaired
    /// surrogate, which turns into U+FFFD (or an encoder error) once a sink writes the line as
    /// UTF-8. Backing off one unit leaves the pair out whole; it is the same one concat, over a
    /// span one shorter, so nothing more is allocated.</para>
    /// </summary>
    internal static string Truncate(string? s, int max)
    {
        if (string.IsNullOrEmpty(s)) return "(none)";
        if (s.Length <= max) return s;
        int cut = max > 0 && char.IsHighSurrogate(s[max - 1]) ? max - 1 : max;
        return string.Concat(s.AsSpan(0, cut), "…");
    }
}
