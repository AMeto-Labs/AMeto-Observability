using System.Buffers;
using System.Diagnostics;
using System.Net.Http.Headers;
using MessagePack;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.PeriodicBatching;

namespace Ameto.Serilog;

/// <summary>
/// Batched Serilog sink that serialises events as Ameto's MessagePack CLEF array
/// (matching <c>Ameto.Core.Serialization.LogEventSerializer</c>) and POSTs them to
/// <c>{serverUrl}/api/events</c>.
/// </summary>
internal sealed class AmetoBatchedSink : IBatchedLogEventSink, IDisposable
{
    private static readonly MediaTypeHeaderValue MsgPackMedia = new("application/x-msgpack");

    private readonly Uri                  _endpoint;
    private readonly string?              _apiKey;
    private readonly ReadOnlyMemory<byte> _serviceNameUtf8;
    private readonly HttpClient           _http;
    private readonly bool                 _ownsHttp;
    private readonly PooledBufferWriter   _bufWriter = new(65_536);

    public AmetoBatchedSink(string serverUrl, string? apiKey, string? serviceName, HttpClient? httpClient)
    {
        var baseUri      = new Uri(serverUrl, UriKind.Absolute);
        _endpoint        = new Uri(baseUri, "api/events");
        _apiKey          = apiKey;
        _serviceNameUtf8 = serviceName is not null
            ? System.Text.Encoding.UTF8.GetBytes(serviceName)
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
        var list = batch as IList<LogEvent> ?? batch.ToList();
        if (list.Count == 0) return;

        ReadOnlyMemory<byte> payload = Serialize(list);

        // Detach from any active OTel trace context so that HttpClient instrumentation
        // won't create a child span inside the calling application's traces.
        // The POST to /api/events is infrastructure noise, not business logic.
        var savedActivity = Activity.Current;
        Activity.Current = null;
        try
        {
            using var content = new ReadOnlyMemoryContent(payload);
            content.Headers.ContentType = MsgPackMedia;

            using var request = new HttpRequestMessage(HttpMethod.Post, _endpoint) { Content = content };
            if (!string.IsNullOrEmpty(_apiKey))
                request.Headers.Add("X-Seq-ApiKey", _apiKey);

            using var response = await _http.SendAsync(request).ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                var body = await SafeReadBodyAsync(response).ConfigureAwait(false);
                SelfLog.WriteLine("Ameto sink: server returned {0}: {1}", (int)response.StatusCode, body);
            }
        }
        catch (Exception ex)
        {
            SelfLog.WriteLine("Ameto sink: failed to emit batch: {0}", ex);
            throw; // PeriodicBatchingSink handles retry/queue-back-off
        }
        finally
        {
            Activity.Current = savedActivity;
        }
    }

    /// <summary>
    /// Encodes the batch into the reused pooled buffer. Returns a slice of that
    /// buffer — valid only until the next call to <see cref="Serialize"/>.
    /// </summary>
    private ReadOnlyMemory<byte> Serialize(IList<LogEvent> events)
    {
        _bufWriter.Reset();
        var writer = new MessagePackWriter(_bufWriter);

        writer.WriteArrayHeader(events.Count);
        foreach (var evt in events)
            AmetoClefFormatter.Write(ref writer, evt, _serviceNameUtf8);
        writer.Flush();

        return _bufWriter.WrittenMemory;
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
        _bufWriter.Dispose();
    }
}

/// <summary>
/// <see cref="IBufferWriter{T}"/> backed by a rented <see cref="ArrayPool{T}"/> buffer.
/// Resizes automatically (returning the old buffer) and is reused across batches via <see cref="Reset"/>.
/// </summary>
internal sealed class PooledBufferWriter : IBufferWriter<byte>, IDisposable
{
    private byte[] _buf;
    private int    _pos;

    public PooledBufferWriter(int initialCapacity)
        => _buf = ArrayPool<byte>.Shared.Rent(initialCapacity);

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

    public void Reset() => _pos = 0;

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
