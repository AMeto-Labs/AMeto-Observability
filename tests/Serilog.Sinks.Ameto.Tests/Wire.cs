using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using Ameto.Core;
using Ameto.Core.Serialization;
using MessagePack;
using Serilog.Events;
using Serilog.Parsing;
using AmetoEvent = Ameto.Core.LogEvent;
using SerilogEvent = Serilog.Events.LogEvent;

namespace Serilog.Sinks.Ameto.Tests;

/// <summary>Serilog events for the tests, built the way the logger builds them.</summary>
internal static class Events
{
    private static readonly MessageTemplateParser Parser = new();

    public static readonly DateTimeOffset T0 = new(2026, 11, 6, 10, 0, 0, TimeSpan.Zero);

    public static SerilogEvent Make(
        string template = "Hello {Name}",
        LogEventLevel level = LogEventLevel.Information,
        Exception? exception = null,
        IEnumerable<LogEventProperty>? properties = null,
        ActivityTraceId? traceId = null,
        ActivitySpanId? spanId = null)
    {
        var props = properties ?? [];
        return traceId is null
            ? new SerilogEvent(T0, level, exception, Parser.Parse(template), props)
            : new SerilogEvent(T0, level, exception, Parser.Parse(template), props, traceId.Value, spanId ?? default);
    }

    public static LogEventProperty P(string name, object? value) => new(name, new ScalarValue(value));
    public static LogEventProperty P(string name, LogEventPropertyValue value) => new(name, value);

    /// <summary>
    /// <paramref name="n"/> events "Event {I}" with I = 0 … n−1 and a Filler string of
    /// <paramref name="fillerChars"/> chars, so that request sizes are predictable.
    /// </summary>
    public static List<SerilogEvent> Many(int n, int fillerChars = 0)
    {
        var filler = new string('f', fillerChars);
        var list = new List<SerilogEvent>(n);
        for (int i = 0; i < n; i++)
            list.Add(Make("Event {I}", properties: [P("I", i), P("Filler", filler)]));
        return list;
    }

    /// <summary>An exception with a real stack trace, as a logger sees one.</summary>
    public static Exception Thrown(string message, Exception? inner = null)
    {
        try
        {
            throw new InvalidOperationException(message, inner);
        }
        catch (InvalidOperationException e)
        {
            return e;
        }
    }
}

/// <summary>A property value whose rendering throws: the formatter's failure case.</summary>
internal sealed class Unrenderable
{
    public override string ToString() => throw new InvalidOperationException("ToString failed on purpose");
}

/// <summary>The wire as the server reads it: bodies decoded by <see cref="LogEventSerializer"/>.</summary>
internal static class Wire
{
    public static List<AmetoEvent> Decode(byte[] body)
    {
        var list = new List<AmetoEvent>();
        uint seq = 1;
        LogEventSerializer.DeserializeBatch(new ReadOnlySequence<byte>(body), 0, ref seq, list);
        return list;
    }

    public static AmetoEvent DecodeOne(byte[] body) => Assert.Single(Decode(body));

    /// <summary>The events through the formatter alone (no limits), as one array body.</summary>
    public static byte[] Body(string? service, params SerilogEvent[] events)
    {
        var buffer  = new ArrayBufferWriter<byte>(1024);
        var writer  = new MessagePackWriter(buffer);
        var svcUtf8 = service is null ? ReadOnlyMemory<byte>.Empty : Encoding.UTF8.GetBytes(service);
        writer.WriteArrayHeader(events.Length);
        foreach (var evt in events)
            AmetoClefFormatter.Write(ref writer, evt, svcUtf8);
        writer.Flush();
        return buffer.WrittenSpan.ToArray();
    }

    public static byte[] Body(params SerilogEvent[] events) => Body("svc", events);

    /// <summary>The I property of an event made by <see cref="Events.Many"/>.</summary>
    public static long I(AmetoEvent evt) => Convert.ToInt64(evt.Properties!["I"]);

    public static List<long> Is(IEnumerable<byte[]> bodies) => bodies.SelectMany(Decode).Select(I).ToList();
}

/// <summary>
/// A stand-in for the Ameto server: records every request body (copied out before the sink reuses
/// its buffer) and answers from a script keyed by request number.
/// </summary>
internal sealed class FakeServer : HttpMessageHandler
{
    private readonly Func<int, byte[], HttpResponseMessage> _script;

    public readonly List<byte[]>  Bodies  = new();
    public readonly List<string?> ApiKeys = new();

    public FakeServer(Func<int, byte[], HttpResponseMessage> script) => _script = script;

    public static FakeServer Always(Func<HttpResponseMessage> reply) => new((_, _) => reply());

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        byte[] body = await request.Content!.ReadAsByteArrayAsync(ct);
        Bodies.Add(body);
        ApiKeys.Add(request.Headers.TryGetValues("X-Seq-ApiKey", out var keys) ? string.Join(",", keys) : null);
        return _script(Bodies.Count - 1, body);
    }

    public static HttpResponseMessage Ok(int ingested = 0)
        => Json(HttpStatusCode.OK, $"{{\"ingested\":{ingested},\"dropped\":0}}");

    public static HttpResponseMessage BadElement(int ingested, int failedAt)
        => Json(HttpStatusCode.BadRequest, $"{{\"ingested\":{ingested},\"dropped\":0,\"failedAtElement\":{failedAt}}}");

    public static HttpResponseMessage Unavailable(TimeSpan? retryAfter = null)
    {
        var reply = Json(HttpStatusCode.ServiceUnavailable, "{\"ingested\":0,\"dropped\":0}");
        if (retryAfter is TimeSpan wait)
            reply.Headers.RetryAfter = new RetryConditionHeaderValue(wait);
        return reply;
    }

    public static HttpResponseMessage Status(HttpStatusCode status) => new(status);

    public static HttpResponseMessage Json(HttpStatusCode status, string json)
        => new(status) { Content = new StringContent(json, Encoding.UTF8, "application/json") };
}

internal static class Sinks
{
    /// <summary>A sink over <paramref name="server"/> whose inline waits are recorded in <paramref name="waits"/> instead of slept.</summary>
    public static AmetoBatchedSink Over(FakeServer server,
        int batchBytesLimit = 3 * 1024 * 1024, int propertiesLimit = 65_536, int exceptionLimit = 256 * 1024,
        List<TimeSpan>? waits = null)
    {
        var sink = new AmetoBatchedSink("http://ameto.test", "k", "svc", new HttpClient(server),
            propertiesLimit, exceptionLimit, batchBytesLimit);
        sink.Delay = wait =>
        {
            waits?.Add(wait);
            return Task.CompletedTask;
        };
        return sink;
    }
}
