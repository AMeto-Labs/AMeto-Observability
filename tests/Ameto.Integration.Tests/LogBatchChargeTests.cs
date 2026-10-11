using Ameto.Core;
using Ameto.Core.Serialization;
using Ameto.Ingestion;
using Ameto.Otel;
using Ameto.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Ameto.Integration.Tests;

/// <summary>
/// The count a log batch keeps of what it holds (<see cref="LogIngestBatch.ChargedBytes"/>) against
/// what it actually holds once decoded — its arrays, and every string and exception tree it references
/// that is its own rather than the pool's (round-3 review R1b-7: the weight used to be checked only
/// against its own formula). And the bound on the batches held at once, now a share of the heap with a
/// setting of its own (R1b-5).
/// </summary>
public sealed class LogBatchChargeTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-charge-" + Guid.NewGuid().ToString("N"));
    private StorageEngine _storage = null!;

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        var opts = new ServerOptions { DataDirectory = _dir };
        _storage = new StorageEngine(Options.Create(opts), new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
                                     NullLogger<StorageEngine>.Instance);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _storage.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    private IngestionEndpoint Endpoint(long limit, long? inFlight = null) => new(
        _storage, _storage.TemplatePool,
        new ServerOptions
        {
            DataDirectory = _dir,
            Ingestion     = new IngestionOptions { MaxDecodedLogBatchBytes = limit, MaxDecodedLogBytesInFlight = inFlight },
        },
        NullLogger<IngestionEndpoint>.Instance);

    /// <summary>
    /// What the batch holds, found by walking it: its two arrays at their length, and every string and
    /// exception tree its records reference that the pool does not — each once.
    /// </summary>
    private static long Held(LogIngestBatch batch)
    {
        var  staged = batch.Staged;
        long held   = (long)staged.ItemCapacity * LogWriteBatch.ItemBytes + staged.PayloadCapacity;
        var  seen   = new HashSet<object>(ReferenceEqualityComparer.Instance);
        for (int i = 0; i < staged.Count; i++)
        {
            if (staged.TemplateAt(i) is { } t && staged.HeaderAt(i).MessageTemplatePoolIndex < 0 && seen.Add(t))
                held += LogIngestBatch.StringBytes(t);
            if (staged.ServiceAt(i) is { } s && seen.Add(s))
                held += LogIngestBatch.StringBytes(s);
            for (var e = staged.ExceptionAt(i); e is not null; e = e.Inner)
                if (seen.Add(e))
                    held += 48 + LogIngestBatch.StringBytes(e.Type) + LogIngestBatch.StringBytes(e.Message)
                               + LogIngestBatch.StringBytes(e.StackTrace);
        }
        return held;
    }

    private void FillPool()
    {
        for (int i = 0; _storage.TemplatePool.Intern("filler " + i) >= 0; i++) { }
    }

    private static void AssertCountsWhatItHolds(LogIngestBatch batch, long atLeast)
    {
        long held = Held(batch);
        Assert.True(held >= atLeast, $"the body was meant to decode to {atLeast:N0} B or more, and holds {held:N0}");
        Assert.True(batch.ChargedBytes >= held, $"the batch holds {held:N0} B and counted {batch.ChargedBytes:N0}");
        Assert.True(batch.ChargedBytes <= held + 64 * 1024, $"the batch holds {held:N0} B and counted {batch.ChargedBytes:N0}");
        Assert.True(batch.ReservedBytes >= batch.ChargedBytes, "the reservation fell behind what the batch holds");
    }

    // ── The count against what is held ────────────────────────────────────────

    [Fact]
    public void A_resource_of_bytes_that_decode_three_times_their_size_is_counted_in_every_record()
    {
        var    endpoint = Endpoint(limit: 64L * 1024 * 1024);
        byte[] body     = LogBatchHeldMemoryTests.InvalidUtf8ResourceBody(attributeBytes: 2_000, records: 1_800);
        using var batch = endpoint.TryBeginBatch(body.Length, OtlpLogBatchWeight.WeighProto(body))!;

        OtlpLogProtoParser.Parse(body, batch);

        AssertCountsWhatItHolds(batch, atLeast: 1_800L * 3 * 2_000);
    }

    [Fact]
    public void Templates_and_services_made_past_a_full_pool_are_counted()
    {
        FillPool();
        var    endpoint = Endpoint(limit: 64L * 1024 * 1024);
        byte[] body     = LogBatchHeldMemoryTests.Request(LogBatchHeldMemoryTests.Block(
                              [LogBatchHeldMemoryTests.Attribute("service.name", "svc.past.full")],
                              Enumerable.Repeat(LogBatchHeldMemoryTests.Record(new string('m', 1_024)), 2_500).ToArray()));
        using var batch = endpoint.TryBeginBatch(body.Length, OtlpLogBatchWeight.WeighProto(body))!;

        OtlpLogProtoParser.Parse(body, batch);

        AssertCountsWhatItHolds(batch, atLeast: 2_500L * 2 * 1_024);
        Assert.Equal(2_500, batch.StagedEvents);
    }

    [Fact]
    public void A_clef_events_exception_tree_is_counted()
    {
        var    endpoint = Endpoint(limit: 64L * 1024 * 1024);
        byte[] body     = LogBatchHeldMemoryTests.ClefEventsWithExceptions(events: 1_000, stackChars: 1_000);
        using var batch = endpoint.TryBeginBatch(body.Length, IngestionEndpoint.WeighClef(body, out _))!;
        var progress    = default(LogEventSerializer.ClefBatchProgress);

        LogEventSerializer.StreamBatch(body, batch, ref progress);

        AssertCountsWhatItHolds(batch, atLeast: 1_000L * 3 * 2 * 1_000);
    }

    /// <summary>
    /// The decode stops the moment the batch would pass its limit: it never held more. The payload array
    /// is sized from what the limit leaves — a doubling used to take it far past (100 MB staged in a
    /// 128 MiB array, under a 32 MiB limit).
    /// </summary>
    [Fact]
    public void A_batch_that_would_pass_its_limit_stops_decoding_holding_no_more_than_the_limit()
    {
        const long Limit = 4L * 1024 * 1024;
        var    endpoint = Endpoint(limit: Limit);
        byte[] body     = LogBatchHeldMemoryTests.InvalidUtf8ResourceBody(attributeBytes: 2_000, records: 1_800);
        using var batch = endpoint.TryBeginBatch(body.Length, OtlpLogBatchWeight.WeighProto(body))!;

        var thrown = Assert.Throws<LogBatchTooHeavyException>(() => OtlpLogProtoParser.Parse(body, batch));

        Assert.Equal(Limit, thrown.Limit);
        Assert.True(batch.ChargedBytes <= Limit, $"counted {batch.ChargedBytes:N0} B past the limit");
        Assert.True(Held(batch) <= Limit, $"holds {Held(batch):N0} B past the limit");
        Assert.True(batch.Staged.PayloadCapacity <= Limit);
    }

    /// <summary>
    /// The decoded batch keeps reserved what it holds, and no more, for its wait for room: the
    /// reservation grows by doubling while the batch is decoded, and a weight taken before decoding can
    /// be more than the decode made of it. Its arrays going back gives the rest back.
    /// </summary>
    [Fact]
    public async Task A_committed_batch_keeps_reserved_what_it_holds_until_it_is_disposed()
    {
        var endpoint = Endpoint(limit: 64L * 1024 * 1024);
        var batch    = endpoint.TryBeginBatch(payloadSizeHint: 0, decodedWeight: 8L * 1024 * 1024)!;   // an overestimate
        try
        {
            for (int i = 0; i < 100; i++)
                Assert.True(batch.TryIngestRaw(DateTimeOffset.UtcNow.UtcTicks, (byte)Ameto.Core.LogLevel.Information,
                                               "reserve probe {N}"u8, [0x80], 0, 0, 0, default));
            Assert.Equal(8L * 1024 * 1024, endpoint.DecodedBytesInFlight);

            var result = await batch.CommitAsync();

            Assert.Equal(100, result.Ingested);
            Assert.Equal(batch.ChargedBytes, endpoint.DecodedBytesInFlight);
        }
        finally { batch.Dispose(); }
        Assert.Equal(0, endpoint.DecodedBytesInFlight);
    }

    // ── R1b-5: the bound on what the batches hold at once ─────────────────────

    [Fact]
    public void The_bound_on_the_batches_held_at_once_is_an_eighth_of_the_heap_and_not_twice_the_batch_limit()
    {
        var stand = MemoryBudgets.Derive(managedLimitBytes: 384L * 1024 * 1024, physicalLimitBytes: 512L * 1024 * 1024);
        Assert.Equal(48L * 1024 * 1024, IngestionOptions.DefaultMaxDecodedLogBytesInFlightFor(stand));

        // Not moved by the OTLP body limit, which moves the per-batch limit's floor.
        var tiny = MemoryBudgets.Derive(managedLimitBytes: 64L * 1024 * 1024, physicalLimitBytes: 64L * 1024 * 1024);
        Assert.Equal(16L * 1024 * 1024, IngestionOptions.DefaultMaxDecodedLogBytesInFlightFor(tiny));   // the floor
        Assert.True(IngestionOptions.DefaultMaxDecodedLogBatchBytesFor(stand, 64 * 1024 * 1024) > 48L * 1024 * 1024);

        var endpoint = Endpoint(limit: 1_000_000, inFlight: 3_000_000);
        Assert.Equal(3_000_000, endpoint.MaxDecodedBytesInFlight);   // its own setting wins

        Assert.Equal(IngestionOptions.MaxDecodedLogBytesInFlightCeiling,
                     new IngestionOptions { MaxDecodedLogBytesInFlight = long.MaxValue }.EffectiveMaxDecodedLogBytesInFlight);
        Assert.Equal(IngestionOptions.MaxDecodedLogBatchBytesCeiling,
                     new IngestionOptions { MaxDecodedLogBatchBytes = long.MaxValue }.EffectiveMaxDecodedLogBatchBytes);
    }

    /// <summary>
    /// A batch's reservation that cannot grow while others hold the rest stops its decode with a 503's
    /// exception — nothing of it written, and nothing left reserved once it is disposed.
    /// </summary>
    [Fact]
    public void A_decode_whose_reservation_cannot_grow_stops_with_nothing_held()
    {
        var    endpoint = Endpoint(limit: 64L * 1024 * 1024, inFlight: 6L * 1024 * 1024);
        var    other    = endpoint.TryBeginBatch(payloadSizeHint: 0, decodedWeight: 5L * 1024 * 1024)!;   // what others hold
        byte[] body     = LogBatchHeldMemoryTests.InvalidUtf8ResourceBody(attributeBytes: 2_000, records: 1_800);
        try
        {
            using (var batch = endpoint.TryBeginBatch(body.Length, decodedWeight: 512 * 1024)!)
                Assert.Throws<LogBatchBudgetFullException>(() => OtlpLogProtoParser.Parse(body, batch));
            Assert.Equal(5L * 1024 * 1024, endpoint.DecodedBytesInFlight);
        }
        finally { other.Dispose(); }
        Assert.Equal(0, endpoint.DecodedBytesInFlight);
    }
}
