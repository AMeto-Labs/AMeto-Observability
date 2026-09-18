using System.Text;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Ameto.Integration.Tests;

/// <summary>
/// OTLP protobuf and JSON records reach the ring through <see cref="IngestionEndpoint.TryIngestRaw"/>.
/// An oversized one is dropped there, and the drop must be logged with the producer named, as
/// the CLEF and <c>LogEvent</c> paths log it. The marker event alone shows the drop on the
/// Events page but leaves the server log silent about who sent it.
/// </summary>
public sealed class OversizedRawDropLogTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    public OversizedRawDropLogTests(AmetoWebAppFactory factory) => _factory = factory;

    [Fact]
    public void OversizedRawRecord_LogsOneWarning_NamingSizeLimitAndService()
    {
        var sp       = _factory.Services;
        var pool     = sp.GetRequiredService<StringInternPool>();
        var options  = sp.GetRequiredService<ServerOptions>();
        var logger   = new CapturingLogger();
        var endpoint = new IngestionEndpoint(
            sp.GetRequiredService<IngestionRingBuffer>(), pool,
            sp.GetRequiredService<IngestionDrainer>(), options, logger);

        int limit  = options.Ingestion.MaxEventPayloadBytes;
        var props  = new byte[limit + 1];                     // size is all TryIngestRaw checks
        int svcIdx = pool.Intern("raw-oversize-svc");

        // The protobuf parser interns service.name once per resource block; the index alone
        // must be enough to name the service.
        bool ok = endpoint.TryIngestRaw(
            DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information,
            Encoding.UTF8.GetBytes("raw oversize probe {N}"), props,
            0, 0, 0, serviceUtf8: default, serviceIdx: svcIdx);
        endpoint.NotifyBatchEnqueued();

        Assert.False(ok);
        var entry = Assert.Single(logger.Entries);
        Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Warning, entry.Level);
        Assert.Equal(limit + 1,           entry.Values["PayloadBytes"]);
        Assert.Equal(limit,               entry.Values["LimitBytes"]);
        Assert.Equal("raw-oversize-svc",  entry.Values["Service"]);
        Assert.Equal("raw oversize probe {N}", entry.Values["Template"]);
    }

    private sealed record Entry(Microsoft.Extensions.Logging.LogLevel Level, Dictionary<string, object?> Values);

    private sealed class CapturingLogger : ILogger<IngestionEndpoint>
    {
        private readonly Lock _gate = new();
        private readonly List<Entry> _entries = [];

        public IReadOnlyList<Entry> Entries { get { lock (_gate) return [.. _entries]; } }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(
            Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
            TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var values = new Dictionary<string, object?>();
            if (state is IReadOnlyList<KeyValuePair<string, object?>> kvs)
                foreach (var kv in kvs) values[kv.Key] = kv.Value;
            lock (_gate) _entries.Add(new Entry(logLevel, values));
        }
    }
}
