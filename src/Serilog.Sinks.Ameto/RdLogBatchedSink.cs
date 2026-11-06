using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using MessagePack;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.PeriodicBatching;

namespace Serilog.Sinks.Ameto;

/// <summary>
/// Batched Serilog sink that serialises events as Ameto's MessagePack CLEF array
/// (matching <c>Ameto.Core.Serialization.LogEventSerializer</c>) and POSTs them to
/// <c>{serverUrl}/api/events</c>.
///
/// <para><b>Delivery.</b> A batch goes out as one request, or as several when its serialised form
/// passes <c>BatchBytesLimit</c> (the server refuses a body over <c>Ingestion.MaxBatchBytes</c>, 4 MB
/// by default, with 413). The server's answers (see <c>IngestionEndpoint</c>) are taken apart:
/// <list type="bullet">
///   <item>200: stored.</item>
///   <item>400 with <c>failedAtElement</c> K: the K events before it are stored and stay so, event K
///     is dropped with a SelfLog line, the rest are sent again. A 400 without it is a request the
///     server could not read at all; it is dropped, never retried.</item>
///   <item>413: the request is cut in half and sent again, down to one event; one event the server
///     still refuses is replaced by an Error marker (<see cref="AmetoClefFormatter.WriteRefusedMarker"/>).</item>
///   <item>429 and 503 (the server had no room; nothing of the request was written): one retry inline
///     after <c>Retry-After</c> (one second when absent, ten at most), then the batching layer's turn.</item>
///   <item>500, 502, 504 and a transport failure: the batching layer's turn at once.</item>
///   <item>401, 403, 404 and any other 4xx: configuration, which no retry fixes; the batch is dropped
///     with a SelfLog line.</item>
/// </list>
/// "The batching layer's turn" means <see cref="EmitBatchAsync"/> throws: the batching sink keeps the
/// batch and retries it with growing back-off for its retry time limit. A retried batch RESUMES past
/// the events this sink already stored or dropped (remembered by reference, <see cref="ResumePoint"/>),
/// so a request that went through is never repeated: the server does not de-duplicate.</para>
///
/// <para><b>Limits.</b> The properties map of one event is measured against <c>PropertiesLimitBytes</c>
/// — what the server measures against <c>Ingestion.MaxEventPayloadBytes</c> — and an event over it is
/// replaced by an Error marker that names it. The exception is measured on its own against
/// <c>ExceptionLimitBytes</c>; over it, the stack trace is cut and the inner chain dropped, and the
/// event still arrives. An event whose formatting throws costs only that event: a marker takes its
/// place and the rest of the batch is unaffected.</para>
/// </summary>
internal sealed class AmetoBatchedSink : IBatchedLogEventSink, IDisposable
{
    private static readonly MediaTypeHeaderValue MsgPackMedia = new("application/x-msgpack");

    /// <summary>The wait before the one inline retry of a 429/503 when the reply names none.</summary>
    internal static readonly TimeSpan DefaultRetryAfter = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The longest the sink waits inline on a <c>Retry-After</c>. Past it the batching layer waits
    /// instead, where a shutdown can interrupt the wait; this sink has no cancellation token.
    /// </summary>
    internal static readonly TimeSpan MaxInlineRetryAfter = TimeSpan.FromSeconds(10);

    /// <summary>The array header the sink writes: array32, five bytes, patched with the count at the end.</summary>
    private const int ArrayHeaderBytes = 5;

    /// <summary>How much of a reply body a SelfLog line carries.</summary>
    private const int SelfLogBodyChars = 512;

    private readonly Uri                  _endpoint;
    private readonly string?              _apiKey;
    private readonly ReadOnlyMemory<byte> _serviceNameUtf8;
    private readonly HttpClient           _http;
    private readonly bool                 _ownsHttp;
    private readonly int                  _propertiesLimitBytes;
    private readonly int                  _exceptionLimitBytes;
    private readonly int                  _batchBytesLimit;
    private readonly PooledBufferWriter   _bufWriter = new(65_536);

    /// <summary>
    /// The events of the batch in flight that are done with — stored, or dropped on purpose — in batch
    /// order, kept across an <see cref="EmitBatchAsync"/> that threw so that the retry resumes after them.
    /// Cleared when a batch completes or a different batch arrives.
    /// </summary>
    private readonly List<LogEvent> _handled = new();

    /// <summary>Test seam: the wait before the inline retry. <see cref="Task.Delay(TimeSpan)"/> in production.</summary>
    internal Func<TimeSpan, Task> Delay { get; set; } = static wait => Task.Delay(wait);

    public AmetoBatchedSink(string serverUrl, string? apiKey, string? serviceName, HttpClient? httpClient,
        int propertiesLimitBytes = 65_536, int exceptionLimitBytes = 256 * 1024, int batchBytesLimit = 3 * 1024 * 1024)
    {
        _propertiesLimitBytes = propertiesLimitBytes;
        _exceptionLimitBytes  = exceptionLimitBytes;
        _batchBytesLimit      = batchBytesLimit;
        // The trailing slash is not cosmetic. Uri resolution replaces the last path SEGMENT, so
        // "http://host/ameto" + "api/events" silently becomes "http://host/api/events" — the
        // prefix is dropped and, because Serilog swallows transport failures, the operator sees
        // no logs and no error. Normalising here makes both forms behave the same.
        var baseUri      = new Uri(serverUrl.TrimEnd('/') + "/", UriKind.Absolute);
        _endpoint        = new Uri(baseUri, "api/events");
        _apiKey          = apiKey;
        _serviceNameUtf8 = serviceName is not null
            ? Encoding.UTF8.GetBytes(serviceName)
            : ReadOnlyMemory<byte>.Empty;

        if (httpClient is null)
        {
            _http     = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
            _ownsHttp = true;
        }
        else
        {
            _http     = httpClient;
            _ownsHttp = false;
        }
    }

    public async Task EmitBatchAsync(IEnumerable<LogEvent> batch)
    {
        var list  = batch as IList<LogEvent> ?? batch.ToList();
        int start = ResumePoint(list);
        if (start >= list.Count)
        {
            _handled.Clear();
            return;
        }

        // Detach from any active OTel trace context so that HttpClient instrumentation
        // won't create a child span inside the calling application's traces.
        // The POST to /api/events is infrastructure noise, not business logic.
        var savedActivity = Activity.Current;
        Activity.Current = null;
        try
        {
            await DeliverAsync(list, start).ConfigureAwait(false);
            _handled.Clear();
        }
        finally
        {
            Activity.Current = savedActivity;
            // A batch that grew the buffer far past the request cap (one huge event) does not pin
            // that memory for the life of the logger.
            _bufWriter.Trim(_batchBytesLimit > 0 ? 2 * _batchBytesLimit : 16 * 1024 * 1024);
        }
    }

    /// <summary>
    /// Where to start in <paramref name="events"/>: past the events of <see cref="_handled"/> when the
    /// batch begins with exactly them — the batching layer handing back the batch whose delivery threw —
    /// and at the first event otherwise. The comparison is by reference: a retried batch carries the
    /// same <see cref="LogEvent"/> instances, a new batch never does.
    /// </summary>
    private int ResumePoint(IList<LogEvent> events)
    {
        int n = _handled.Count;
        if (n == 0) return 0;
        if (n <= events.Count)
        {
            int i = 0;
            while (i < n && ReferenceEquals(events[i], _handled[i])) i++;
            if (i == n) return n;
        }
        _handled.Clear();
        return 0;
    }

    private void MarkHandled(IList<LogEvent> events, int start, int count)
    {
        for (int i = 0; i < count; i++)
            _handled.Add(events[start + i]);
    }

    /// <summary>
    /// Sends <paramref name="events"/> from <paramref name="start"/> in requests of at most
    /// <c>BatchBytesLimit</c> bytes, acting on each reply as the class doc says. Returns when every
    /// event is stored or dropped; throws when the batching layer should retry what is left.
    /// </summary>
    private async Task DeliverAsync(IList<LogEvent> events, int start)
    {
        int  maxEvents = int.MaxValue;   // halved on 413, kept for the rest of the batch
        bool retried   = false;          // the one inline retry, per request

        while (start < events.Count)
        {
            int   taken = Serialize(events, start, Math.Min(events.Count - start, maxEvents));
            Reply reply = await PostAsync(_bufWriter.WrittenMemory).ConfigureAwait(false);

            switch (reply.Kind)
            {
                case ReplyKind.Delivered:
                    MarkHandled(events, start, taken);
                    start  += taken;
                    retried = false;
                    break;

                case ReplyKind.BadElement:
                {
                    int k = reply.FailedAtElement;
                    if (k < 0 || k >= taken)
                    {
                        // Not a CLEF array to the server, or a body that ended short: nothing of it was
                        // stored, and sending the same bytes again would get the same answer.
                        SelfLog.WriteLine("Ameto sink: server could not read a request of {0} events (400): {1}; dropped",
                            taken, reply.BodyText);
                        MarkHandled(events, start, taken);
                        start += taken;
                    }
                    else
                    {
                        var template = events[start + k].MessageTemplate.Text;
                        SelfLog.WriteLine("Ameto sink: server could not read event {0} of a request of {1} (400, template: {2}); " +
                                          "dropped, the {0} before it are stored, the rest are sent again",
                            k, taken, Head(template));
                        MarkHandled(events, start, k + 1);
                        start += k + 1;
                    }
                    retried = false;
                    break;
                }

                case ReplyKind.TooLarge:
                    if (taken > 1)
                    {
                        maxEvents = taken / 2;   // the loop serialises the first half next
                        break;
                    }
                    await PostRefusedMarkerAsync(events[start], _bufWriter.Position - ArrayHeaderBytes).ConfigureAwait(false);
                    MarkHandled(events, start, 1);
                    start  += 1;
                    retried = false;
                    break;

                case ReplyKind.Rejected:
                    SelfLog.WriteLine("Ameto sink: server returned {0}: {1}; {2} events dropped",
                        reply.StatusCode, reply.BodyText, events.Count - start);
                    return;

                case ReplyKind.Retry:
                    if (reply.RetryAfter is TimeSpan wait && !retried)
                    {
                        retried = true;
                        await Delay(wait).ConfigureAwait(false);
                        break;
                    }
                    var ex = new AmetoDeliveryException(reply.StatusCode, reply.BodyText);
                    SelfLog.WriteLine("Ameto sink: {0}; {1} events left for the batching layer to retry", ex.Message, events.Count - start);
                    throw ex;
            }
        }
    }

    /// <summary>
    /// Encodes <paramref name="events"/> from <paramref name="start"/> into the reused buffer as one
    /// MessagePack array, stopping before the event that would take the request past
    /// <c>BatchBytesLimit</c> — never before the first, which goes alone however large — or at
    /// <paramref name="maxCount"/>. Returns how many events the request holds.
    /// </summary>
    private int Serialize(IList<LogEvent> events, int start, int maxCount)
    {
        _bufWriter.Reset();
        BeginArray();

        int taken = 0;
        for (int i = start; i < start + maxCount; i++)
        {
            int before = _bufWriter.Position;
            WriteEvent(events[i]);
            if (taken > 0 && _batchBytesLimit > 0 && _bufWriter.Position > _batchBytesLimit)
            {
                _bufWriter.Rewind(before);
                break;
            }
            taken++;
        }

        EndArray(taken);
        return taken;
    }

    /// <summary>
    /// The array header as array32 whatever the count: the count is known only once the byte cap
    /// says where the request ends, and every MessagePack reader takes the wide form for a small count.
    /// </summary>
    private void BeginArray()
    {
        var span = _bufWriter.GetSpan(ArrayHeaderBytes);
        span[0] = 0xdd;
        _bufWriter.Advance(ArrayHeaderBytes);
    }

    private void EndArray(int count)
        => BinaryPrimitives.WriteUInt32BigEndian(_bufWriter.WrittenSpan.Slice(1, 4), (uint)count);

    /// <summary>
    /// One event into the buffer, measured in parts. A new <see cref="MessagePackWriter"/> is made
    /// after every flush and every rewind: the writer caches the span it writes into, so one that
    /// outlived a rewind would write past the position the rewind set.
    /// </summary>
    private void WriteEvent(LogEvent evt)
    {
        int before = _bufWriter.Position;
        try
        {
            var writer = new MessagePackWriter(_bufWriter);
            AmetoClefFormatter.WriteHeader(ref writer, evt, _serviceNameUtf8);
            writer.Flush();

            if (evt.Exception is not null)
                WriteException(evt);

            int propertiesAt = _bufWriter.Position;
            writer = new MessagePackWriter(_bufWriter);
            AmetoClefFormatter.WriteProperties(ref writer, evt);
            writer.Flush();

            int propertiesBytes = _bufWriter.Position - propertiesAt;
            if (_propertiesLimitBytes > 0 && propertiesBytes > _propertiesLimitBytes)
            {
                // Replace with a compact Error-level marker: the oversized event is VISIBLE in Ameto
                // (searchable by OriginalTemplate) instead of being rejected by the server's cap on the
                // properties map, where its marker would say less.
                _bufWriter.Rewind(before);
                var marker = new MessagePackWriter(_bufWriter);
                AmetoClefFormatter.WriteOversizedMarker(ref marker, evt, _serviceNameUtf8, propertiesBytes, _propertiesLimitBytes);
                marker.Flush();

                SelfLog.WriteLine("Ameto sink: {0} B of properties exceeded PropertiesLimitBytes={1}; replaced with an Error marker (template: {2})",
                    propertiesBytes, _propertiesLimitBytes, Head(evt.MessageTemplate.Text));
            }
        }
        catch (Exception ex)
        {
            // A property whose ToString() throws, an exception whose StackTrace getter does: that event
            // alone is lost, as a marker that names the failure; the rest of the batch is unaffected.
            _bufWriter.Rewind(before);
            try
            {
                var marker = new MessagePackWriter(_bufWriter);
                AmetoClefFormatter.WriteFormatFailureMarker(ref marker, evt, _serviceNameUtf8, $"{ex.GetType().Name}: {ex.Message}");
                marker.Flush();
            }
            catch
            {
                _bufWriter.Rewind(before);
            }
            SelfLog.WriteLine("Ameto sink: could not serialise an event (template: {0}); replaced with an Error marker: {1}",
                Head(evt.MessageTemplate.Text), ex);
        }
    }

    /// <summary>
    /// <c>@x</c>, measured against <c>ExceptionLimitBytes</c>: written whole first, and when that overran
    /// the limit, rewound and written again with the stack cut and the inner chain dropped.
    /// </summary>
    private void WriteException(LogEvent evt)
    {
        var writer = new MessagePackWriter(_bufWriter);
        writer.WriteString("@x"u8);
        writer.Flush();

        int at = _bufWriter.Position;
        writer = new MessagePackWriter(_bufWriter);
        AmetoClefFormatter.WriteException(ref writer, evt.Exception!, in ExceptionBudget.Unlimited);
        writer.Flush();

        int bytes = _bufWriter.Position - at;
        if (_exceptionLimitBytes <= 0 || bytes <= _exceptionLimitBytes) return;

        _bufWriter.Rewind(at);
        writer = new MessagePackWriter(_bufWriter);
        AmetoClefFormatter.WriteException(ref writer, evt.Exception!, ExceptionBudget.For(_exceptionLimitBytes));
        writer.Flush();

        SelfLog.WriteLine("Ameto sink: {0} B exception exceeded ExceptionLimitBytes={1}; stack trace cut, inner exceptions dropped (template: {2})",
            bytes, _exceptionLimitBytes, Head(evt.MessageTemplate.Text));
    }

    /// <summary>
    /// The marker for an event the server refused with 413 on its own, sent as a request of one. Best
    /// effort: a reply other than success is logged and the event is given up either way.
    /// </summary>
    private async Task PostRefusedMarkerAsync(LogEvent evt, int eventBytes)
    {
        SelfLog.WriteLine("Ameto sink: server refused a {0} B event as too large (413); replaced with an Error marker (template: {1})",
            eventBytes, Head(evt.MessageTemplate.Text));

        _bufWriter.Reset();
        BeginArray();
        var writer = new MessagePackWriter(_bufWriter);
        AmetoClefFormatter.WriteRefusedMarker(ref writer, evt, _serviceNameUtf8, eventBytes);
        writer.Flush();
        EndArray(1);

        Reply reply = await PostAsync(_bufWriter.WrittenMemory).ConfigureAwait(false);
        if (reply.Kind != ReplyKind.Delivered)
            SelfLog.WriteLine("Ameto sink: the marker for the refused event was not stored ({0})", reply.StatusCode);
    }

    // ── HTTP ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// One POST. A transport failure (connection refused, timeout) propagates: the batching layer
    /// retries the batch, and <see cref="ResumePoint"/> keeps it from repeating the requests that went through.
    /// </summary>
    private async Task<Reply> PostAsync(ReadOnlyMemory<byte> payload)
    {
        try
        {
            using var content = new ReadOnlyMemoryContent(payload);
            content.Headers.ContentType = MsgPackMedia;

            using var request = new HttpRequestMessage(HttpMethod.Post, _endpoint) { Content = content };
            if (!string.IsNullOrEmpty(_apiKey))
                request.Headers.Add("X-Seq-ApiKey", _apiKey);

            using var response = await _http.SendAsync(request).ConfigureAwait(false);

            int status = (int)response.StatusCode;
            if (response.IsSuccessStatusCode) return Reply.Delivered;

            byte[] body = await SafeReadBodyAsync(response).ConfigureAwait(false);
            return status switch
            {
                400        => Reply.BadElement(ReadFailedAtElement(body), body),
                413        => Reply.TooLarge,
                429 or 503 => Reply.Retry(status, body, RetryAfterOf(response)),
                >= 500     => Reply.Retry(status, body, retryAfter: null),
                _          => Reply.Rejected(status, body),
            };
        }
        catch (Exception ex) when (ex is not AmetoDeliveryException)
        {
            SelfLog.WriteLine("Ameto sink: failed to send a request: {0}", ex);
            throw; // the batching layer retries the batch
        }
    }

    /// <summary>
    /// <c>failedAtElement</c> of the server's 400 body — <c>{"ingested":N,"dropped":M,"failedAtElement":K}</c>
    /// — or −1 when the body has none (the server could not read the array header) or is not that JSON.
    /// </summary>
    internal static int ReadFailedAtElement(ReadOnlySpan<byte> json)
    {
        try
        {
            var reader = new Utf8JsonReader(json);
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject) return -1;
            while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
            {
                bool hit = reader.ValueTextEquals("failedAtElement"u8);
                if (!reader.Read()) return -1;
                if (hit)
                    return reader.TokenType == JsonTokenType.Number && reader.TryGetInt32(out int k) ? k : -1;
                reader.Skip();
            }
        }
        catch (JsonException)
        {
        }
        return -1;
    }

    /// <summary>The reply's <c>Retry-After</c> as a wait: delta seconds or a date, clamped to [0, <see cref="MaxInlineRetryAfter"/>]; <see cref="DefaultRetryAfter"/> without one.</summary>
    internal static TimeSpan RetryAfterOf(HttpResponseMessage response)
    {
        var header = response.Headers.RetryAfter;
        TimeSpan wait = DefaultRetryAfter;
        if (header?.Delta is TimeSpan delta)    wait = delta;
        else if (header?.Date is DateTimeOffset at) wait = at - DateTimeOffset.UtcNow;

        if (wait < TimeSpan.Zero)          return TimeSpan.Zero;
        if (wait > MaxInlineRetryAfter)    return MaxInlineRetryAfter;
        return wait;
    }

    private static async Task<byte[]> SafeReadBodyAsync(HttpResponseMessage response)
    {
        try   { return await response.Content.ReadAsByteArrayAsync().ConfigureAwait(false); }
        catch { return []; }
    }

    /// <summary>The first <see cref="AmetoClefFormatter.MarkerTemplateChars"/> of a template, for SelfLog lines.</summary>
    private static string Head(string template)
        => template.Length <= 96 ? template : template[..96];

    public Task OnEmptyBatchAsync() => Task.CompletedTask;

    public void Dispose()
    {
        if (_ownsHttp) _http.Dispose();
        _bufWriter.Dispose();
    }

    // ── Replies ──────────────────────────────────────────────────────────────

    private enum ReplyKind
    {
        /// <summary>2xx: the request is stored.</summary>
        Delivered,
        /// <summary>400: the request stopped being a CLEF array at <see cref="Reply.FailedAtElement"/> (−1: at the start).</summary>
        BadElement,
        /// <summary>413: the request is too large for the server; nothing of it is stored.</summary>
        TooLarge,
        /// <summary>429, 503, 5xx: nothing stored, worth sending again later.</summary>
        Retry,
        /// <summary>Any other status: configuration, which sending again does not fix.</summary>
        Rejected,
    }

    private readonly record struct Reply(ReplyKind Kind, int StatusCode, byte[]? Body, int FailedAtElement, TimeSpan? RetryAfter)
    {
        public static readonly Reply Delivered = new(ReplyKind.Delivered, 200, null, -1, null);
        public static readonly Reply TooLarge  = new(ReplyKind.TooLarge, 413, null, -1, null);

        public static Reply BadElement(int failedAt, byte[] body)              => new(ReplyKind.BadElement, 400, body, failedAt, null);
        public static Reply Retry(int status, byte[] body, TimeSpan? retryAfter) => new(ReplyKind.Retry, status, body, -1, retryAfter);
        public static Reply Rejected(int status, byte[] body)                 => new(ReplyKind.Rejected, status, body, -1, null);

        /// <summary>The body as text for a SelfLog line, cut to <see cref="SelfLogBodyChars"/>.</summary>
        public string BodyText
        {
            get
            {
                if (Body is null || Body.Length == 0) return "<no body>";
                string text = Encoding.UTF8.GetString(Body);
                return text.Length <= SelfLogBodyChars ? text : text[..SelfLogBodyChars];
            }
        }
    }
}

/// <summary>
/// Thrown out of <see cref="AmetoBatchedSink.EmitBatchAsync"/> for a reply worth retrying once the
/// sink's own inline retry is spent: 429 or 503 (the server had no room, nothing was written) and 5xx.
/// The batching layer logs it to SelfLog and sends the batch again with back-off, and the sink resumes
/// past what the server already stored.
/// </summary>
internal sealed class AmetoDeliveryException(int statusCode, string body)
    : Exception($"Ameto server returned {statusCode}: {body}")
{
    public int StatusCode { get; } = statusCode;
}

/// <summary>
/// <see cref="IBufferWriter{T}"/> backed by a rented <see cref="ArrayPool{T}"/> buffer.
/// Resizes automatically (returning the old buffer) and is reused across batches via <see cref="Reset"/>.
/// </summary>
internal sealed class PooledBufferWriter : IBufferWriter<byte>, IDisposable
{
    private readonly int    _initialCapacity;
    private          byte[] _buf;
    private          int    _pos;

    public PooledBufferWriter(int initialCapacity)
    {
        _initialCapacity = initialCapacity;
        _buf = ArrayPool<byte>.Shared.Rent(initialCapacity);
    }

    public void Advance(int count) => _pos += count;

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        Grow(sizeHint == 0 ? 256 : sizeHint);
        return _buf.AsMemory(_pos);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        Grow(sizeHint == 0 ? 256 : sizeHint);
        return _buf.AsSpan(_pos);
    }

    public ReadOnlyMemory<byte> WrittenMemory => _buf.AsMemory(0, _pos);

    /// <summary>The written bytes, writable: the array header's count is patched through it.</summary>
    public Span<byte> WrittenSpan => _buf.AsSpan(0, _pos);

    /// <summary>Current write offset — pairs with <see cref="Rewind"/> to un-write a region.</summary>
    public int Position => _pos;

    /// <summary>The rented buffer's length; what <see cref="Trim"/> looks at.</summary>
    public int Capacity => _buf.Length;

    /// <summary>Rolls the writer back to an earlier <see cref="Position"/> (discards bytes after it).</summary>
    public void Rewind(int position) => _pos = position;

    public void Reset() => _pos = 0;

    /// <summary>
    /// Lets go of a buffer that grew past <paramref name="keepBytes"/>, returning it to the pool and
    /// renting the initial size again; the written bytes are discarded. For the end of a batch, when
    /// one huge event has grown the buffer far beyond what the next batches need.
    /// </summary>
    public void Trim(int keepBytes)
    {
        if (_buf.Length <= keepBytes) return;
        ArrayPool<byte>.Shared.Return(_buf);
        _buf = ArrayPool<byte>.Shared.Rent(_initialCapacity);
        _pos = 0;
    }

    private void Grow(int needed)
    {
        if (_pos + needed <= _buf.Length) return;
        int newSize = Math.Max(_buf.Length * 2, _pos + needed);
        var next = ArrayPool<byte>.Shared.Rent(newSize);
        _buf.AsSpan(0, _pos).CopyTo(next);
        ArrayPool<byte>.Shared.Return(_buf);
        _buf = next;
    }

    public void Dispose()
    {
        var tmp = System.Threading.Interlocked.Exchange(ref _buf, null!);
        if (tmp is not null) ArrayPool<byte>.Shared.Return(tmp);
    }
}
