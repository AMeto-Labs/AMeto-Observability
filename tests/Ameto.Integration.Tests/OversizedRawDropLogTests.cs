using System.Text;
using Ameto.Core;
using Ameto.Ingestion;
using Ameto.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Ameto.Integration.Tests;

/// <summary>
/// OTLP protobuf and JSON records reach the store through <see cref="LogIngestBatch.TryIngestRaw(long, byte, ReadOnlySpan{byte}, ReadOnlySpan{byte}, ulong, ulong, ulong, ReadOnlySpan{byte}, int)"/>.
/// An oversized one is dropped there, and the drop must be logged with the producer named, as
/// the CLEF path logs it. The marker event alone shows the drop on the Events page but leaves the
/// server log silent about who sent it.
/// </summary>
public sealed class OversizedRawDropLogTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    public OversizedRawDropLogTests(AmetoWebAppFactory factory) => _factory = factory;

    [Fact]
    public async Task OversizedRawRecord_LogsOneWarning_NamingSizeLimitAndService()
    {
        var sp       = _factory.Services;
        var pool     = sp.GetRequiredService<StringInternPool>();
        var options  = sp.GetRequiredService<ServerOptions>();
        var logger   = new CapturingLogger();
        var endpoint = new IngestionEndpoint(sp.GetRequiredService<StorageEngine>(), pool, options, logger);

        int limit  = options.Ingestion.MaxEventPayloadBytes;
        var props  = new byte[limit + 1];                     // size is all TryIngestRaw checks
        int svcIdx = pool.Intern("raw-oversize-svc");

        // The protobuf parser interns service.name once per resource block; the index alone
        // must be enough to name the service.
        using var batch = endpoint.BeginBatch();
        bool ok = batch.TryIngestRaw(
            DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information,
            Encoding.UTF8.GetBytes("raw oversize probe {N}"), props,
            0, 0, 0, serviceUtf8: default, serviceIdx: svcIdx);
        var result = await batch.CommitAsync();

        Assert.False(ok);
        Assert.Equal(0, result.Ingested);   // the marker that stands in for it is written, and is not the client's
        Assert.Equal(1, result.Dropped);
        var entry = Assert.Single(logger.Entries);
        Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Warning, entry.Level);
        Assert.Equal(limit + 1,           entry.Values["PayloadBytes"]);
        Assert.Equal(limit,               entry.Values["LimitBytes"]);
        Assert.Equal("raw-oversize-svc",  entry.Values["Service"]);
        Assert.Equal("raw oversize probe {N}", entry.Values["Template"]);
    }

    /// <summary>
    /// The Warning is written at most once a second, and the next one counts the drops in between.
    /// It is logged as each attempt is decoded, and a batch answered 503 is decoded again by every
    /// retry; a producer sending oversized events in bulk wrote one line per event. Every drop is
    /// still counted, and still leaves its marker.
    /// </summary>
    [Fact]
    public async Task The_drop_Warning_is_written_at_most_once_a_second_and_the_next_one_counts_the_rest()
    {
        var sp       = _factory.Services;
        var pool     = sp.GetRequiredService<StringInternPool>();
        var options  = sp.GetRequiredService<ServerOptions>();
        var logger   = new CapturingLogger();
        var clock    = new Ameto.Testing.ManualTimeProvider();
        var endpoint = new IngestionEndpoint(sp.GetRequiredService<StorageEngine>(), pool, options, logger) { Clock = clock };
        var props    = new byte[options.Ingestion.MaxEventPayloadBytes + 1];

        async Task<LogIngestResult> Send(int oversized)
        {
            using var batch = endpoint.BeginBatch();
            for (int i = 0; i < oversized; i++)
                batch.TryIngestRaw(DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information,
                                   "rate probe {N}"u8, props, 0, 0, 0, "rate-probe-svc"u8);
            return await batch.CommitAsync();
        }

        var first = await Send(3);
        Assert.Equal(3, first.Dropped);
        var only = Assert.Single(logger.Entries);                    // the first of the three
        Assert.False(only.Values.ContainsKey("Unlogged"));

        clock.Advance(TimeSpan.FromMilliseconds(999));
        await Send(1);
        Assert.Single(logger.Entries);                               // still inside the second

        clock.Advance(TimeSpan.FromMilliseconds(1));
        await Send(1);
        Assert.Equal(2, logger.Entries.Count);
        Assert.Equal(3, logger.Entries[1].Values["Unlogged"]);       // the two of the first batch, and the one after
        Assert.Equal("rate-probe-svc", logger.Entries[1].Values["Service"]);
        Assert.Equal(5, endpoint.DroppedOversized);                  // every drop counted
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
