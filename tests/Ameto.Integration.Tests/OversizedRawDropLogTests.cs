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

    /// <summary>An endpoint over this host's store whose batches give up on the store after 100 ms.</summary>
    private (IngestionEndpoint Endpoint, StorageEngine Storage, CapturingLogger Logger, Ameto.Testing.ManualTimeProvider Clock, byte[] Props) QuickEndpoint()
    {
        var sp      = _factory.Services;
        var storage = sp.GetRequiredService<StorageEngine>();
        var options = new ServerOptions
        {
            DataDirectory = sp.GetRequiredService<ServerOptions>().DataDirectory,
            Ingestion     = new IngestionOptions { BackPressureWait = TimeSpan.FromMilliseconds(100) },
        };
        var logger   = new CapturingLogger();
        var clock    = new Ameto.Testing.ManualTimeProvider();
        var endpoint = new IngestionEndpoint(storage, sp.GetRequiredService<StringInternPool>(), options, logger) { Clock = clock };
        return (endpoint, storage, logger, clock, new byte[options.Ingestion.MaxEventPayloadBytes + 1]);
    }

    /// <summary>
    /// R1a-6. A drop whose marker the store had no room for — after the client's own events — is
    /// named in the server log whatever the once-a-second limit says: that line is the only record of
    /// it. Suppressed with the rest, such a drop was in no log and on no Events page.
    /// </summary>
    [Fact]
    public async Task A_drop_whose_marker_the_store_had_no_room_for_is_named_even_in_a_second_already_named()
    {
        var (endpoint, storage, logger, _, props) = QuickEndpoint();

        using (var first = endpoint.BeginBatch())   // takes this second's line
        {
            first.TryIngestRaw(DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information, "named probe {N}"u8, props, 0, 0, 0, "named-svc"u8);
            await first.CommitAsync();
        }
        Assert.Single(Warnings(logger));

        int writes = 0;
        storage._afterWriteGate = () =>
        {
            if (Interlocked.Increment(ref writes) > 1) throw new OutOfMemoryException("test: no room after the client's event");
        };
        LogIngestResult result;
        try
        {
            using var batch = endpoint.BeginBatch();
            batch.TryIngestRaw(DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information, "client probe {N}"u8, [0x80], 0, 0, 0, "lost-svc"u8);
            batch.TryIngestRaw(DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information, "lost probe {N}"u8, props, 0, 0, 0, "lost-svc"u8);
            result = await batch.CommitAsync();
        }
        finally { storage._afterWriteGate = null; }

        Assert.Equal(new LogIngestResult(1, 1, Busy: false), result);   // the client's event in; its marker not
        var warnings = Warnings(logger);
        Assert.Equal(2, warnings.Count);
        var lost = warnings[1];
        Assert.Equal("lost-svc", lost.Values["Service"]);
        Assert.Equal("lost probe {N}", lost.Values["Template"]);
        Assert.Contains("only record", (string)lost.Values["{OriginalFormat}"]!);
    }

    /// <summary>
    /// R1a-7. The Warning's count of drops since the last one is a count of drops: of the answers that
    /// ended requests, as <see cref="IngestionEndpoint.DroppedOversized"/> is. It counted decode
    /// attempts, so a batch answered 503 three times read as four drops (~170 for 20 measured), and a
    /// client retrying each second got a line per attempt. A 503 names nothing; its retry does.
    /// </summary>
    [Fact]
    public async Task The_Warnings_count_of_drops_is_of_drops_not_of_the_attempts_a_503_made()
    {
        var (endpoint, storage, logger, clock, props) = QuickEndpoint();

        async Task<LogIngestResult> SendOne(ReadOnlyMemory<byte> service)
        {
            using var batch = endpoint.BeginBatch();
            batch.TryIngestRaw(DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information, "client probe {N}"u8, [0x80], 0, 0, 0, service.Span);
            batch.TryIngestRaw(DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information, "attempt probe {N}"u8, props, 0, 0, 0, service.Span);
            return await batch.CommitAsync();
        }

        Assert.False((await SendOne("first-svc"u8.ToArray())).Busy);   // takes this second's line
        storage._afterWriteGate = () => throw new OutOfMemoryException("test: no room at all");
        try
        {
            for (int i = 0; i < 3; i++) Assert.True((await SendOne("retried-svc"u8.ToArray())).Busy);
        }
        finally { storage._afterWriteGate = null; }
        Assert.False((await SendOne("retried-svc"u8.ToArray())).Busy);   // the retry that ends it

        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.False((await SendOne("next-svc"u8.ToArray())).Busy);

        var warnings = Warnings(logger);
        Assert.Equal(2, warnings.Count);                                // none for the 503s
        Assert.Equal(1, warnings[1].Values["Unlogged"]);                // the one drop since, not its four attempts
    }

    private static List<Entry> Warnings(CapturingLogger logger) =>
        logger.Entries.Where(e => e.Level == Microsoft.Extensions.Logging.LogLevel.Warning).ToList();

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
