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
///   <item>200: stored. Its <c>dropped</c> count — events over the server's size limit, each with the
///     server's own marker in its place, and the tail of a request the store had no room for — goes
///     to SelfLog; the sink cannot tell the two apart, so it resends neither.</item>
///   <item>400 with <c>failedAtElement</c> K: the K events before it are stored and stay so, event K
///     is dropped with a SelfLog line, the rest are sent again. A 400 without it is a request the
///     server could not read at all; it is dropped, never retried.</item>
///   <item>413: the request is cut in half and sent again, down to one event. One event the server
///     still refuses is written as an Error marker (<see cref="AmetoClefFormatter.WriteRefusedMarker"/>)
///     in its place from then on, so the marker is delivered, retried and resumed like any other event.</item>
///   <item>429 and 503 (the server had no room; nothing of the request was written): one retry inline
///     after <c>Retry-After</c> (one second when absent, ten at most), then the batching layer's turn.</item>
///   <item>408, 500, 502, 504 and a transport failure: the batching layer's turn at once.</item>
///   <item>401, 403, 404 and any other 4xx: configuration, which no retry fixes; the batch is dropped
///     with a SelfLog line.</item>
/// </list>
/// "The batching layer's turn" means <see cref="EmitBatchAsync"/> throws: the batching sink keeps the
/// batch and retries it with growing back-off for its retry time limit. A retried batch RESUMES past
/// the events this sink already stored or dropped (remembered by reference, <see cref="ResumePoint"/>),
/// so a request the server answered is never repeated. A request whose answer never arrived — a
/// timeout, a connection dropped after the server wrote it — is sent again and can be stored twice:
/// the server does not de-duplicate.</para>
///
/// <para><b>Limits.</b> The properties map of one event is measured against <c>PropertiesLimitBytes</c>
/// — map header and pairs, what the server measures against <c>Ingestion.MaxEventPayloadBytes</c> —
/// and an event over it is replaced by an Error marker that names it. The exception is measured on
/// its own against <c>ExceptionLimitBytes</c>; over it, the stack trace is cut and the inner chain
/// dropped, and the event still arrives. An event whose formatting throws costs only that event: a
/// marker takes its place and the rest of the batch is unaffected.</para>
/// </summary>
internal sealed class AmetoBatchedSink : IBatchedLogEventSink, IDisposable
{
    private static readonly MediaTypeHeaderValue MsgPackMedia = new("application/x-msgpack");

    /// <summary>The wait before the one inline retry of a 429/503 when the reply names none.</summary>
    internal static readonly TimeSpan DefaultRetryAfter = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The longest the sink waits inline on a <c>Retry-After</c>; a longer one is clamped to it. The
    /// batching layer never sees the header (its retries come after its own back-off), and this sink
    /// has no cancellation token, so a shutdown cannot interrupt the wait: ten seconds is the most a
    /// <c>CloseAndFlush</c> is made to wait for it.
    /// </summary>
    internal static readonly TimeSpan MaxInlineRetryAfter = TimeSpan.FromSeconds(10);

    /// <summary>The array header the sink writes: array32, five bytes, patched with the count at the end.</summary>
    private const int ArrayHeaderBytes = 5;

    /// <summary>How much of a reply body the sink reads: the server's counts take ~50 bytes, and a proxy's error page is not worth more.</summary>
    internal const int MaxReplyBodyBytes = 4096;

    /// <summary>How much of a reply body a SelfLog line carries.</summary>
    private const int SelfLogBodyChars = 512;

    /// <summary>How much of a template a SelfLog line carries: a line is read by a person, a marker (<see cref="AmetoClefFormatter.MarkerTemplateChars"/>) is searched.</summary>
    private const int SelfLogTemplateChars = 96;

    private readonly Uri                  _endpoint;
    private readonly string?              _apiKey;
    private readonly ReadOnlyMemory<byte> _serviceNameUtf8;
    private readonly HttpClient           _http;
    private readonly bool                 _ownsHttp;
    private readonly TimeSpan             _replyTimeout;
    private readonly int                  _propertiesLimitBytes;
    private readonly int                  _exceptionLimitBytes;
    private readonly int                  _batchBytesLimit;
    private readonly int                  _keepBufferBytes;
    private readonly PooledBufferWriter   _bufWriter = new(65_536);

    /// <summary>
    /// The events of the batch in flight that are done with — stored, or dropped on purpose — in batch
    /// order, kept across an <see cref="EmitBatchAsync"/> that threw so that the retry resumes after them.
    /// Cleared when a batch completes or a different batch arrives.
    /// </summary>
    private readonly List<LogEvent> _handled = new();

    /// <summary>
    /// The event the server refused alone with 413, and its serialised size: from then on
    /// <see cref="Serialize"/> writes its marker in its place, so the marker rides in the request
    /// stream — delivered, retried and resumed like any other event — instead of going as a
    /// best-effort POST of its own, which a 503 or a dropped connection lost, and whose throw made the
    /// batching layer upload the whole event again. Kept and cleared with <see cref="_handled"/>, so a
    /// retried batch writes the marker too.
    /// </summary>
    private LogEvent? _refused;
    private int       _refusedBytes;

    /// <summary>Test seam: the wait before the inline retry. <see cref="Task.Delay(TimeSpan)"/> in production.</summary>
    internal Func<TimeSpan, Task> Delay { get; set; } = static wait => Task.Delay(wait);

    public AmetoBatchedSink(string serverUrl, string? apiKey, string? serviceName, HttpClient? httpClient,
        int propertiesLimitBytes = 65_536, int exceptionLimitBytes = 256 * 1024, int batchBytesLimit = 3 * 1024 * 1024)
    {
        _propertiesLimitBytes = propertiesLimitBytes;
        _exceptionLimitBytes  = exceptionLimitBytes;
        _batchBytesLimit      = batchBytesLimit;
        // A batch that grew the buffer far past the request cap (one huge event) does not pin that
        // memory for the life of the logger: a buffer over twice the cap is let go at the batch's end.
        // In long arithmetic: twice a cap past 1 GiB overflowed to a negative keep size, which trimmed
        // after every batch.
        _keepBufferBytes      = batchBytesLimit > 0 ? (int)Math.Min(2L * batchBytesLimit, int.MaxValue) : 16 * 1024 * 1024;
        _bufWriter.KeepBytes  = _keepBufferBytes;
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
        // The reply body is read unbuffered (ReadBodyAsync), where HttpClient.Timeout no longer
        // applies: the read gets the same bound, or 30 s under a client whose timeout is infinite.
        _replyTimeout = _http.Timeout == Timeout.InfiniteTimeSpan ? TimeSpan.FromSeconds(30) : _http.Timeout;
    }

    public async Task EmitBatchAsync(IEnumerable<LogEvent> batch)
    {
        var list  = batch as IList<LogEvent> ?? batch.ToList();
        int start = ResumePoint(list);
        if (start >= list.Count)
        {
            Forget();
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
            Forget();
        }
        finally
        {
            Activity.Current = savedActivity;
            _bufWriter.Trim(_keepBufferBytes);
        }
    }

    /// <summary>The batch in flight is done with: nothing to resume past, no marker to write.</summary>
    private void Forget()
    {
        _handled.Clear();
        _refused = null;
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
        if (n == 0)
        {
            // Nothing handled, so a pending refused event can only be the batch's first; one that is not
            // belongs to a batch the layer dropped, and would otherwise be pinned until this one completes.
            if (_refused is not null && (events.Count == 0 || !ReferenceEquals(events[0], _refused))) _refused = null;
            return 0;
        }
        if (n <= events.Count)
        {
            int i = 0;
            while (i < n && ReferenceEquals(events[i], _handled[i])) i++;
            if (i == n) return n;
        }
        Forget();
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
        int  maxEvents = int.MaxValue;   // halved on 413 until the request fits, or is down to one event
        bool retried   = false;          // the one inline retry, per request

        while (start < events.Count)
        {
            int   taken = Serialize(events, start, Math.Min(events.Count - start, maxEvents));
            Reply reply = await PostAsync(_bufWriter.WrittenMemory).ConfigureAwait(false);

            switch (reply.Kind)
            {
                case ReplyKind.Delivered:
                    NoteDropped(in reply, taken);
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
                        NoteDropped(in reply, taken);
                        SelfLog.WriteLine("Ameto sink: server could not read event {0} of a request of {1} (400, template: {2}); " +
                                          "dropped, the {0} before it are stored, the rest are sent again",
                            k, taken, Head(events[start + k].MessageTemplate.Text));
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
                        retried   = false;
                        break;
                    }
                    if (ReferenceEquals(events[start], _refused))
                    {
                        // The marker itself — a few hundred bytes — is refused: nothing this sink can
                        // send would pass whatever answered, so the event is given up with a line.
                        SelfLog.WriteLine("Ameto sink: server refused even the marker for a refused event as too large (413); dropped (template: {0})",
                            Head(events[start].MessageTemplate.Text));
                        MarkHandled(events, start, 1);
                        start += 1;
                    }
                    else
                    {
                        _refused      = events[start];
                        _refusedBytes = _bufWriter.Position - ArrayHeaderBytes;
                        SelfLog.WriteLine("Ameto sink: server refused a {0} B event as too large (413); replaced with an Error marker (template: {1})",
                            _refusedBytes, Head(events[start].MessageTemplate.Text));
                    }
                    // The culprit is out of the way, so the next request is a full one again: a limit
                    // really below the byte cap halves it again, log₂ N requests rather than one per event.
                    maxEvents = int.MaxValue;
                    retried   = false;
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
    /// The <c>dropped</c> count of an answer that stored the rest of the request: events over the
    /// server's size limit, each with the server's own marker in its place, and — when the store was
    /// short of room — the request's unwritten tail. The sink cannot tell the two apart, so it resends
    /// neither; this line is the client's only record of the loss.
    /// </summary>
    private static void NoteDropped(in Reply reply, int taken)
    {
        if (reply.Dropped > 0)
            SelfLog.WriteLine("Ameto sink: server answered {0} but dropped {1} of the request's {2} events (over its size limit, or not written for want of room)",
                reply.StatusCode, reply.Dropped, taken);
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
            var evt    = events[i];
            if (ReferenceEquals(evt, _refused)) WriteRefusedMarker(evt);
            else                               WriteEvent(evt);
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

            int propertiesBytes = MapHeaderBytes(evt.Properties.Count) + (_bufWriter.Position - propertiesAt);
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
            WriteFormatFailureMarker(evt, ex, before);
        }
    }

    /// <summary>
    /// The map header the server counts in front of the properties it keeps
    /// (<c>LogEventSerializer.StreamEvent</c>): without it the sink let through an event the server
    /// then refused, when the pairs alone came within the header's size of the limit.
    /// </summary>
    private static int MapHeaderBytes(int count) => count < 16 ? 1 : count < 65_536 ? 3 : 5;

    /// <summary>
    /// The marker for an event whose formatting threw. Should even the marker fail (an exception whose
    /// <c>Message</c> throws is caught here; the rest is memory running out), a nil takes the element's
    /// place: the request then still holds the number of elements its header declares, and the server
    /// refuses exactly this one (400 with <c>failedAtElement</c>), which the sink drops by name — where
    /// an element short pointed the 400 at the request's last event, which was in fact stored.
    /// </summary>
    private void WriteFormatFailureMarker(LogEvent evt, Exception ex, int before)
    {
        string error, detail;
        try   { error = $"{ex.GetType().Name}: {ex.Message}"; detail = ex.ToString(); }
        catch { error = detail = ex.GetType().Name; }
        try
        {
            var marker = new MessagePackWriter(_bufWriter);
            AmetoClefFormatter.WriteFormatFailureMarker(ref marker, evt, _serviceNameUtf8, error);
            marker.Flush();
        }
        catch
        {
            _bufWriter.Rewind(before);
            _bufWriter.GetSpan(1)[0] = 0xc0;
            _bufWriter.Advance(1);
        }
        SelfLog.WriteLine("Ameto sink: could not serialise an event (template: {0}); replaced with an Error marker: {1}",
            Head(evt.MessageTemplate.Text), detail);
    }

    /// <summary>The marker that stands in for <see cref="_refused"/>; it reads nothing of the event that can throw.</summary>
    private void WriteRefusedMarker(LogEvent evt)
    {
        var writer = new MessagePackWriter(_bufWriter);
        AmetoClefFormatter.WriteRefusedMarker(ref writer, evt, _serviceNameUtf8, _refusedBytes);
        writer.Flush();
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

            // Headers only: the body is read by ReadBodyAsync up to MaxReplyBodyBytes, so a proxy's
            // error page is neither buffered whole by HttpClient nor decoded whole for a SelfLog line.
            using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);

            int    status = (int)response.StatusCode;
            byte[] body   = await ReadBodyAsync(response, _replyTimeout).ConfigureAwait(false);
            Counts counts = ReadCounts(body);
            if (response.IsSuccessStatusCode) return Reply.Delivered(status, counts.Dropped);
            return status switch
            {
                400           => Reply.BadElement(counts.FailedAtElement, counts.Dropped, body),
                413           => Reply.TooLarge,
                429 or 503    => Reply.Retry(status, body, RetryAfterOf(response)),
                408 or >= 500 => Reply.Retry(status, body, retryAfter: null),
                _             => Reply.Rejected(status, body),
            };
        }
        catch (Exception ex) when (ex is not AmetoDeliveryException)
        {
            SelfLog.WriteLine("Ameto sink: failed to send a request: {0}", ex);
            throw; // the batching layer retries the batch
        }
    }

    /// <summary>
    /// The reply body up to <see cref="MaxReplyBodyBytes"/>, or empty when it cannot be read. The read
    /// has a bound of its own: <c>HttpClient.Timeout</c> ends with the headers once the body is left
    /// unbuffered, and a server that sends its headers and then stalls must not hold the batch for good.
    /// </summary>
    private static async Task<byte[]> ReadBodyAsync(HttpResponseMessage response, TimeSpan timeout)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(MaxReplyBodyBytes);
        try
        {
            using var timer  = new CancellationTokenSource(timeout);
            using var stream = await response.Content.ReadAsStreamAsync(timer.Token).ConfigureAwait(false);
            int total = 0;
            while (total < MaxReplyBodyBytes)
            {
                int read = await stream.ReadAsync(buffer.AsMemory(total, MaxReplyBodyBytes - total), timer.Token).ConfigureAwait(false);
                if (read == 0) break;
                total += read;
            }
            if (total == 0) return [];
            var body = new byte[total];
            Array.Copy(buffer, body, total);
            return body;
        }
        catch
        {
            return [];
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>The server's counts as its JSON gives them; −1 for one the body lacks.</summary>
    internal readonly record struct Counts(int Ingested, int Dropped, int FailedAtElement);

    /// <summary>
    /// <c>{"ingested":N,"dropped":M,"failedAtElement":K}</c> in one pass. A count the body lacks, or a
    /// body that is not that JSON (a proxy's page, nothing at all), reads as −1; <c>failedAtElement</c>
    /// is absent when the server could not read the array header, and the 400 is then the whole request's.
    /// </summary>
    internal static Counts ReadCounts(byte[] json)
    {
        int ingested = -1, dropped = -1, failedAt = -1;
        try
        {
            var reader = new Utf8JsonReader(json);
            if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
            {
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    int slot = reader.ValueTextEquals("ingested"u8)        ? 0
                             : reader.ValueTextEquals("dropped"u8)         ? 1
                             : reader.ValueTextEquals("failedAtElement"u8) ? 2 : -1;
                    if (!reader.Read()) break;
                    if (slot >= 0)
                    {
                        int n = reader.TokenType == JsonTokenType.Number && reader.TryGetInt32(out int value) ? value : -1;
                        if (slot == 0)      ingested = n;
                        else if (slot == 1) dropped  = n;
                        else                failedAt = n;
                    }
                    reader.Skip();   // a nested value is passed over whole; a scalar is already read
                }
            }
        }
        catch (JsonException)
        {
        }
        return new Counts(ingested, dropped, failedAt);
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

    /// <summary>The first <see cref="SelfLogTemplateChars"/> of a template, for SelfLog lines.</summary>
    private static string Head(string template)
        => template.Length <= SelfLogTemplateChars ? template : template[..SelfLogTemplateChars];

    public Task OnEmptyBatchAsync() => Task.CompletedTask;

    public void Dispose()
    {
        if (_ownsHttp) _http.Dispose();
        _bufWriter.Dispose();
    }

    // ── Replies ──────────────────────────────────────────────────────────────

    private enum ReplyKind
    {
        /// <summary>2xx: the request is stored, less what <see cref="Reply.Dropped"/> counts.</summary>
        Delivered,
        /// <summary>400: the request stopped being a CLEF array at <see cref="Reply.FailedAtElement"/> (−1: at the start).</summary>
        BadElement,
        /// <summary>413: the request is too large for the server; nothing of it is stored.</summary>
        TooLarge,
        /// <summary>408, 429, 503, 5xx: nothing stored (a 408 never arrived whole), worth sending again later.</summary>
        Retry,
        /// <summary>Any other status: configuration, which sending again does not fix.</summary>
        Rejected,
    }

    private readonly record struct Reply(ReplyKind Kind, int StatusCode, byte[]? Body, int FailedAtElement, int Dropped, TimeSpan? RetryAfter)
    {
        public static readonly Reply TooLarge = new(ReplyKind.TooLarge, 413, null, -1, 0, null);

        /// <summary><paramref name="dropped"/> as the body counts it; a body without the count (−1) counts as none.</summary>
        public static Reply Delivered(int status, int dropped)                   => new(ReplyKind.Delivered, status, null, -1, Math.Max(dropped, 0), null);
        public static Reply BadElement(int failedAt, int dropped, byte[] body)   => new(ReplyKind.BadElement, 400, body, failedAt, Math.Max(dropped, 0), null);
        public static Reply Retry(int status, byte[] body, TimeSpan? retryAfter) => new(ReplyKind.Retry, status, body, -1, 0, retryAfter);
        public static Reply Rejected(int status, byte[] body)                    => new(ReplyKind.Rejected, status, body, -1, 0, null);

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
/// sink's own inline retry is spent: 429 or 503 (the server had no room, nothing was written), 408
/// (the request never arrived whole) and 5xx. The batching layer logs it to SelfLog and sends the
/// batch again with back-off, and the sink resumes past what the server already stored.
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

    /// <summary>
    /// The largest buffer worth handing back to the shared pool when it is outgrown or trimmed: the pool
    /// keeps what it is handed until memory pressure trims it, so returning a buffer of tens of megabytes
    /// only moves the pinned memory from here into the pool. Larger ones are left to the GC. The sink
    /// sets it to its keep size.
    /// </summary>
    public int KeepBytes { get; set; } = int.MaxValue;

    /// <summary>Rolls the writer back to an earlier <see cref="Position"/> (discards bytes after it).</summary>
    public void Rewind(int position) => _pos = position;

    public void Reset() => _pos = 0;

    /// <summary>
    /// Lets go of a buffer that grew past <paramref name="keepBytes"/>, renting the initial size again;
    /// the written bytes are discarded. The old buffer is left to the GC, not returned: the shared pool
    /// keeps what it is handed until memory pressure trims it, so returning a buffer of tens of
    /// megabytes only moved the pinned memory from here into the pool. For the end of a batch, when
    /// one huge event has grown the buffer far beyond what the next batches need.
    /// </summary>
    public void Trim(int keepBytes)
    {
        if (_buf.Length <= keepBytes) return;
        _buf = ArrayPool<byte>.Shared.Rent(_initialCapacity);
        _pos = 0;
    }

    private void Grow(int needed)
    {
        if (_pos + needed <= _buf.Length) return;
        int newSize = Math.Max(_buf.Length * 2, _pos + needed);
        var next = ArrayPool<byte>.Shared.Rent(newSize);
        _buf.AsSpan(0, _pos).CopyTo(next);
        // An outgrown buffer goes back to the pool only while it is worth keeping: on the way to one huge
        // event, 32 + 16 + 8 + … MB of outgrown buffers would otherwise be parked in the pool for good.
        if (_buf.Length <= KeepBytes) ArrayPool<byte>.Shared.Return(_buf);
        _buf = next;
    }

    public void Dispose()
    {
        var tmp = System.Threading.Interlocked.Exchange(ref _buf, null!);
        if (tmp is not null) ArrayPool<byte>.Shared.Return(tmp);
    }
}
