using System.Net;
using Serilog.Debugging;
using static Serilog.Sinks.Ameto.Tests.Events;

namespace Serilog.Sinks.Ameto.Tests;

/// <summary>SelfLog is process-wide: the tests that capture it run alone, after the parallel collections.</summary>
[CollectionDefinition(SelfLogCollection.Name, DisableParallelization = true)]
public sealed class SelfLogCollection
{
    public const string Name = "SelfLog";
}

/// <summary>
/// The lines the sink writes to SelfLog — the only record a client keeps of what the server did not
/// store, and of a request that was given up.
/// </summary>
[Collection(SelfLogCollection.Name)]
public class SelfLogTests
{
    private static async Task<string> CaptureAsync(Func<Task> act)
    {
        var log = new StringWriter();
        SelfLog.Enable(log);
        try
        {
            await act();
        }
        finally
        {
            SelfLog.Disable();
        }
        return log.ToString();
    }

    [Fact]
    public async Task A_200_that_counts_dropped_events_is_reported_with_the_counts()
    {
        var server = FakeServer.Always(() => FakeServer.Ok(ingested: 3, dropped: 2));
        var sink   = Sinks.Over(server);

        var log = await CaptureAsync(() => sink.EmitBatchAsync(Many(5)));

        Assert.Contains("server answered 200 but dropped 2 of the request's 5 events", log);
    }

    [Fact]
    public async Task A_200_without_drops_writes_nothing()
    {
        var server = FakeServer.Always(() => FakeServer.Ok(ingested: 5));
        var sink   = Sinks.Over(server);

        var log = await CaptureAsync(() => sink.EmitBatchAsync(Many(5)));

        Assert.Equal("", log);
    }

    [Fact]
    public async Task A_rejected_request_names_the_status_and_what_was_dropped()
    {
        var server = FakeServer.Always(() => FakeServer.Status(HttpStatusCode.Unauthorized));
        var sink   = Sinks.Over(server);

        var log = await CaptureAsync(() => sink.EmitBatchAsync(Many(3)));

        Assert.Contains("server returned 401", log);
        Assert.Contains("3 events dropped", log);
    }

    [Fact]
    public async Task A_400_without_failedAtElement_names_the_request_dropped()
    {
        var server = FakeServer.Always(() => FakeServer.Json(HttpStatusCode.BadRequest, "{\"ingested\":0,\"dropped\":0}"));
        var sink   = Sinks.Over(server);

        var log = await CaptureAsync(() => sink.EmitBatchAsync(Many(5)));

        Assert.Contains("could not read a request of 5 events (400)", log);
        Assert.Contains("dropped", log);
    }

    [Fact]
    public async Task A_400_with_failedAtElement_names_the_event_by_index_and_template()
    {
        var server = new FakeServer((i, _) => i == 0 ? FakeServer.BadElement(ingested: 3, failedAt: 3) : FakeServer.Ok());
        var sink   = Sinks.Over(server);

        var log = await CaptureAsync(() => sink.EmitBatchAsync(Many(10)));

        Assert.Contains("could not read event 3 of a request of 10 (400, template: Event {I})", log);
    }

    [Fact]
    public async Task A_refused_event_names_its_size_and_template()
    {
        var server = new FakeServer((_, body) => Wire.Decode(body).Any(e => e.MessageTemplate == "Big {Blob}")
            ? FakeServer.Status(HttpStatusCode.RequestEntityTooLarge)
            : FakeServer.Ok());
        var sink   = Sinks.Over(server);

        var log = await CaptureAsync(() => sink.EmitBatchAsync([Make("Big {Blob}", properties: [P("Blob", new string('x', 3000))])]));

        Assert.Contains("refused a", log);
        Assert.Contains("B event as too large (413); replaced with an Error marker (template: Big {Blob})", log);
    }

    [Fact]
    public async Task A_batch_handed_to_the_batching_layer_says_how_many_events_are_left()
    {
        var server = FakeServer.Always(() => FakeServer.Status(HttpStatusCode.BadGateway));
        var sink   = Sinks.Over(server);

        var log = await CaptureAsync(async () => await Assert.ThrowsAsync<AmetoDeliveryException>(() => sink.EmitBatchAsync(Many(4))));

        Assert.Contains("Ameto server returned 502", log);
        Assert.Contains("4 events left for the batching layer to retry", log);
    }
}
