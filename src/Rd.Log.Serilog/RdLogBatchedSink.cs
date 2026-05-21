using System.Buffers;
using System.Net.Http.Headers;
using MessagePack;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.PeriodicBatching;

namespace Rd.Log.Serilog;

/// <summary>
/// Batched Serilog sink that serialises events as Rd.Log's MessagePack CLEF array
/// (matching <c>Rd.Log.Core.Serialization.LogEventSerializer</c>) and POSTs them to
/// <c>{serverUrl}/api/events</c>.
/// </summary>
internal sealed class RdLogBatchedSink : IBatchedLogEventSink, IDisposable
{
    private static readonly MediaTypeHeaderValue MsgPackMedia = new("application/x-msgpack");

    private readonly Uri        _endpoint;
    private readonly string?    _apiKey;
    private readonly HttpClient _http;
    private readonly bool       _ownsHttp;

    public RdLogBatchedSink(string serverUrl, string? apiKey, HttpClient? httpClient)
    {
        // Normalise: allow trailing slash, append the ingestion path.
        var baseUri = new Uri(serverUrl, UriKind.Absolute);
        _endpoint   = new Uri(baseUri, "api/events");
        _apiKey     = apiKey;

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
        // Materialise to a list once so we can both count and re-iterate.
        var list    = batch as IList<LogEvent> ?? batch.ToList();
        if (list.Count == 0) return;

        byte[] payload = Serialize(list);

        try
        {
            using var content = new ByteArrayContent(payload);
            content.Headers.ContentType = MsgPackMedia;

            using var request = new HttpRequestMessage(HttpMethod.Post, _endpoint) { Content = content };
            if (!string.IsNullOrEmpty(_apiKey))
                request.Headers.Add("X-Seq-ApiKey", _apiKey);

            using var response = await _http.SendAsync(request).ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                var body = await SafeReadBodyAsync(response).ConfigureAwait(false);
                SelfLog.WriteLine("Rd.Log sink: server returned {0}: {1}", (int)response.StatusCode, body);
            }
        }
        catch (Exception ex)
        {
            SelfLog.WriteLine("Rd.Log sink: failed to emit batch: {0}", ex);
            throw; // PeriodicBatchingSink handles retry/queue-back-off
        }
    }

    /// <summary>
    /// Encodes the batch as a top-level MessagePack array of CLEF maps.
    /// Kept in a non-async method because <see cref="MessagePackWriter"/> is a
    /// <c>ref struct</c> and may not cross async suspension points.
    /// </summary>
    private static byte[] Serialize(IList<LogEvent> events)
    {
        var buffer = new ArrayBufferWriter<byte>(4096);
        var writer = new MessagePackWriter(buffer);

        writer.WriteArrayHeader(events.Count);
        foreach (var evt in events)
            RdLogClefFormatter.Write(ref writer, evt);
        writer.Flush();

        return buffer.WrittenSpan.ToArray();
    }

    public Task OnEmptyBatchAsync() => Task.CompletedTask;

    private static async Task<string> SafeReadBodyAsync(HttpResponseMessage response)
    {
        try   { return await response.Content.ReadAsStringAsync().ConfigureAwait(false); }
        catch { return "<unreadable>"; }
    }

    public void Dispose()
    {
        if (_ownsHttp) _http.Dispose();
    }
}
