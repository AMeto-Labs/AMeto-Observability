using System.Net;
using System.Net.Http.Headers;
using System.Text;
using static Serilog.Sinks.Ameto.Tests.Events;

namespace Serilog.Sinks.Ameto.Tests;

/// <summary>
/// What the sink does with each answer of the server — against a scripted stand-in, with every
/// body it sent read back by the server's own decoder.
/// </summary>
public class DeliveryTests
{
    // ── Requests ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task A_batch_past_the_byte_cap_goes_as_several_requests_in_order()
    {
        const int cap = 16 * 1024;
        var server = FakeServer.Always(() => FakeServer.Ok());
        var sink   = Sinks.Over(server, batchBytesLimit: cap);

        await sink.EmitBatchAsync(Many(40, fillerChars: 1000));

        Assert.True(server.Bodies.Count >= 3, $"{server.Bodies.Count} requests for ~44 KB under a 16 KB cap");
        Assert.All(server.Bodies, body =>
        {
            Assert.True(body.Length <= cap, $"a request of {body.Length} B passed the {cap} B cap");
            Assert.Equal(0xdd, body[0]);   // array32, whatever the count
        });
        Assert.Equal(Enumerable.Range(0, 40).Select(i => (long)i), Wire.Is(server.Bodies));
    }

    [Fact]
    public async Task One_event_larger_than_the_cap_still_goes_alone()
    {
        var server = FakeServer.Always(() => FakeServer.Ok());
        var sink   = Sinks.Over(server, batchBytesLimit: 2 * 1024);
        var events = new[] { Make("Small {I}", properties: [P("I", 0)]),
                             Make("Big {I}", properties: [P("I", 1), P("Blob", new string('x', 5000))]),
                             Make("Small {I}", properties: [P("I", 2)]) };

        await sink.EmitBatchAsync(events);

        Assert.Equal(3, server.Bodies.Count);
        Assert.True(server.Bodies[1].Length > 5000);
        Assert.Equal([0L, 1L, 2L], Wire.Is(server.Bodies));
    }

    [Fact]
    public async Task A_cap_of_zero_sends_the_batch_as_one_request()
    {
        var server = FakeServer.Always(() => FakeServer.Ok());
        var sink   = Sinks.Over(server, batchBytesLimit: 0);

        await sink.EmitBatchAsync(Many(200, fillerChars: 1000));

        Assert.Single(server.Bodies);
        Assert.Equal(200, Wire.Decode(server.Bodies[0]).Count);
    }

    [Fact]
    public async Task The_api_key_rides_in_the_Seq_header()
    {
        var server = FakeServer.Always(() => FakeServer.Ok());
        var sink   = Sinks.Over(server);

        await sink.EmitBatchAsync(Many(1));

        Assert.Equal("k", Assert.Single(server.ApiKeys));
    }

    // ── 400 ───────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A_400_with_failedAtElement_drops_that_event_and_resends_the_rest()
    {
        var server = new FakeServer((i, _) => i == 0 ? FakeServer.BadElement(ingested: 3, failedAt: 3) : FakeServer.Ok());
        var sink   = Sinks.Over(server);

        await sink.EmitBatchAsync(Many(10));

        Assert.Equal(2, server.Bodies.Count);
        // Events 0–2 are stored (the server said so), event 3 is the one it could not read:
        // the second request carries exactly 4–9, nothing before.
        Assert.Equal([4L, 5L, 6L, 7L, 8L, 9L], Wire.Decode(server.Bodies[1]).Select(Wire.I));
    }

    [Fact]
    public async Task A_400_without_failedAtElement_drops_the_request_and_the_next_batch_is_a_fresh_start()
    {
        var server = new FakeServer((i, _) => i == 0
            ? FakeServer.Json(HttpStatusCode.BadRequest, "{\"ingested\":0,\"dropped\":0}")
            : FakeServer.Ok());
        var sink   = Sinks.Over(server);

        await sink.EmitBatchAsync(Many(5));
        Assert.Single(server.Bodies);

        await sink.EmitBatchAsync(Many(3));
        Assert.Equal(2, server.Bodies.Count);
        Assert.Equal([0L, 1L, 2L], Wire.Decode(server.Bodies[1]).Select(Wire.I));
    }

    [Fact]
    public async Task A_400_with_failedAtElement_followed_by_a_502_does_not_resend_the_stored_prefix_on_retry()
    {
        // 400 K=3: events 0–2 stored, 3 dropped, 4–9 sent again → 502 → the batching layer's turn. The
        // retry must start at 4: 0–2 are in the store, which does not de-duplicate, and 3 was given up.
        var server = new FakeServer((i, _) => i switch
        {
            0 => FakeServer.BadElement(ingested: 3, failedAt: 3),
            1 => FakeServer.Status(HttpStatusCode.BadGateway),
            _ => FakeServer.Ok(),
        });
        var sink  = Sinks.Over(server);
        var batch = Many(10);

        await Assert.ThrowsAsync<AmetoDeliveryException>(() => sink.EmitBatchAsync(batch));
        await sink.EmitBatchAsync(batch);   // the retry

        Assert.Equal(3, server.Bodies.Count);
        Assert.Equal([4L, 5L, 6L, 7L, 8L, 9L], Wire.Decode(server.Bodies[2]).Select(Wire.I));
    }

    [Theory]
    [InlineData("{\"ingested\":3,\"dropped\":0,\"failedAtElement\":3}", 3, 0, 3)]
    [InlineData("{\"failedAtElement\":0}", -1, -1, 0)]
    [InlineData("{\"ingested\":1200,\"dropped\":1800}", 1200, 1800, -1)]
    [InlineData("{\"a\":{\"failedAtElement\":5},\"failedAtElement\":2}", -1, -1, 2)]
    [InlineData("{\"failedAtElement\":\"x\",\"dropped\":true}", -1, -1, -1)]
    [InlineData("not json", -1, -1, -1)]
    [InlineData("<html>a proxy's error page</html>", -1, -1, -1)]
    [InlineData("", -1, -1, -1)]
    public void The_servers_counts_are_read_from_its_json(string body, int ingested, int dropped, int failedAt)
        => Assert.Equal(new AmetoBatchedSink.Counts(ingested, dropped, failedAt),
                        AmetoBatchedSink.ReadCounts(Encoding.UTF8.GetBytes(body)));

    // ── 413 ───────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A_413_halves_the_request_until_it_fits()
    {
        var server = new FakeServer((_, body) => Wire.Decode(body).Count > 2
            ? FakeServer.Status(HttpStatusCode.RequestEntityTooLarge)
            : FakeServer.Ok());
        var sink   = Sinks.Over(server);

        await sink.EmitBatchAsync(Many(8));

        // 8 → 413, 4 → 413, then 2 + 2 + 2 + 2.
        Assert.Equal([8, 4, 2, 2, 2, 2], server.Bodies.Select(b => Wire.Decode(b).Count));
        var stored = server.Bodies.Skip(2).SelectMany(Wire.Decode).Select(Wire.I);
        Assert.Equal(Enumerable.Range(0, 8).Select(i => (long)i), stored);
    }

    [Fact]
    public async Task A_413_for_a_single_event_puts_a_marker_in_its_place_in_the_next_request()
    {
        var server = new FakeServer((_, body) => Wire.Decode(body).Any(e => e.MessageTemplate == "Big {Blob}")
            ? FakeServer.Status(HttpStatusCode.RequestEntityTooLarge)
            : FakeServer.Ok());
        var sink   = Sinks.Over(server);
        var events = new[] { Make("A"), Make("Big {Blob}", Serilog.Events.LogEventLevel.Fatal, properties: [P("Blob", new string('x', 3000))]), Make("C") };

        await sink.EmitBatchAsync(events);

        // [A, Big, C] → 413; halved down to one: A → 200, Big → 413; then a full request again, with
        // the marker in Big's place: [marker, C] → 200 — not a POST of the marker alone, nor C alone.
        Assert.Equal([3, 1, 1, 2], server.Bodies.Select(b => Wire.Decode(b).Count));
        var last   = Wire.Decode(server.Bodies[3]);
        var marker = last[0];
        Assert.Equal(AmetoClefFormatter.RefusedTemplate, marker.MessageTemplate);
        Assert.Equal("Error", marker.Level.ToString());
        Assert.Equal("sink", marker.Properties!["DroppedBy"]);
        Assert.True(Convert.ToInt64(marker.Properties["EventBodyBytes"]) > 3000);
        Assert.Equal("Big {Blob}", marker.Properties["OriginalTemplate"]);
        Assert.Equal("Fatal", marker.Properties["OriginalLevel"]);
        Assert.Equal("C", last[1].MessageTemplate);
    }

    [Fact]
    public async Task After_a_single_event_413_the_rest_of_the_batch_is_not_sent_one_event_at_a_time()
    {
        var server = new FakeServer((_, body) => Wire.Decode(body).Any(e => e.MessageTemplate == "Big {I}")
            ? FakeServer.Status(HttpStatusCode.RequestEntityTooLarge)
            : FakeServer.Ok());
        var sink   = Sinks.Over(server);
        var events = Many(10);
        events[2]  = Make("Big {I}", properties: [P("I", 2), P("Blob", new string('x', 3000))]);

        await sink.EmitBatchAsync(events);

        // 10 → 413, 5 → 413, [0,1] → 200, [2,3] → 413, [2] → 413 (its marker from now on); then the
        // rest in ONE request, [marker, 3…9]: the halving stops with the culprit, not for the batch.
        Assert.Equal([10, 5, 2, 2, 1, 8], server.Bodies.Select(b => Wire.Decode(b).Count));
        Assert.Equal([0L, 1L], Wire.Decode(server.Bodies[2]).Select(Wire.I));
        var last = Wire.Decode(server.Bodies[5]);
        Assert.Equal(AmetoClefFormatter.RefusedTemplate, last[0].MessageTemplate);
        Assert.Equal(Enumerable.Range(3, 7).Select(i => (long)i), last.Skip(1).Select(Wire.I));
    }

    /// <summary>
    /// The server — or a proxy answering 413 to everything, a body limit configured absurdly low — refuses
    /// even the ~300 B marker. Without the guard: [event] → 413 → [marker] → 413 → [marker] … for ever, one
    /// request a turn, the batching loop blocked and CloseAndFlush hanging. With it: the event, then its
    /// marker, then the event is given up with a line. Run on the pool, since the fake answers synchronously
    /// and a loop would never yield.
    /// </summary>
    [Fact]
    public async Task A_marker_the_server_refuses_too_is_dropped_not_retried_forever()
    {
        var server = FakeServer.Always(() => FakeServer.Status(HttpStatusCode.RequestEntityTooLarge));
        var sink   = Sinks.Over(server);

        var send = Task.Run(() => sink.EmitBatchAsync([Make("Big {Blob}", properties: [P("Blob", new string('x', 3000))])]));
        Assert.Same(send, await Task.WhenAny(send, Task.Delay(TimeSpan.FromSeconds(10))));
        await send;

        Assert.Equal(2, server.Count);   // the event, then its marker; nothing after
    }

    [Fact]
    public async Task A_refused_events_marker_is_retried_as_the_marker_not_as_the_event()
    {
        // The request carrying the marker fails on the wire. The batching layer hands the same batch
        // back, and the sink must write the marker again — not upload the refused event whole for
        // another 413 and another marker, as a marker POST of its own did.
        int markerRequests = 0;
        var server = new FakeServer((_, body) =>
        {
            var decoded = Wire.Decode(body);
            if (decoded.Any(e => e.MessageTemplate == "Big {Blob}"))
                return FakeServer.Status(HttpStatusCode.RequestEntityTooLarge);
            if (decoded.Any(e => e.MessageTemplate == AmetoClefFormatter.RefusedTemplate) && markerRequests++ == 0)
                throw new HttpRequestException("connection reset");
            return FakeServer.Ok();
        });
        var sink   = Sinks.Over(server);
        var events = new[] { Make("A"), Make("Big {Blob}", properties: [P("Blob", new string('x', 3000))]), Make("C") };

        await Assert.ThrowsAsync<HttpRequestException>(() => sink.EmitBatchAsync(events));
        Assert.Equal([3, 1, 1, 2], server.Bodies.Select(b => Wire.Decode(b).Count));   // the fourth is the one that threw

        await sink.EmitBatchAsync(events);   // the retry

        Assert.Equal(5, server.Bodies.Count);
        Assert.Equal([AmetoClefFormatter.RefusedTemplate, "C"], Wire.Decode(server.Bodies[4]).Select(e => e.MessageTemplate));
        Assert.DoesNotContain(server.Bodies.Skip(3), body => Wire.Decode(body).Any(e => e.MessageTemplate == "Big {Blob}"));
    }

    // ── 429 / 503 / 5xx ───────────────────────────────────────────────────────

    [Fact]
    public async Task A_503_is_retried_once_inline_after_Retry_After()
    {
        var waits  = new List<TimeSpan>();
        var server = new FakeServer((i, _) => i == 0 ? FakeServer.Unavailable(TimeSpan.FromSeconds(2)) : FakeServer.Ok());
        var sink   = Sinks.Over(server, waits: waits);

        await sink.EmitBatchAsync(Many(3));

        Assert.Equal([TimeSpan.FromSeconds(2)], waits);
        Assert.Equal(2, server.Bodies.Count);
        Assert.Equal([0L, 1L, 2L], Wire.Decode(server.Bodies[1]).Select(Wire.I));
    }

    [Fact]
    public async Task A_503_that_persists_goes_to_the_batching_layer_after_the_default_wait()
    {
        var waits  = new List<TimeSpan>();
        var server = FakeServer.Always(() => FakeServer.Unavailable());
        var sink   = Sinks.Over(server, waits: waits);

        var ex = await Assert.ThrowsAsync<AmetoDeliveryException>(() => sink.EmitBatchAsync(Many(3)));

        Assert.Equal(503, ex.StatusCode);
        Assert.Equal([AmetoBatchedSink.DefaultRetryAfter], waits);
        Assert.Equal(2, server.Bodies.Count);
    }

    [Fact]
    public async Task A_Retry_After_past_ten_seconds_is_clamped()
    {
        var waits  = new List<TimeSpan>();
        var server = new FakeServer((i, _) => i == 0 ? FakeServer.Unavailable(TimeSpan.FromMinutes(10)) : FakeServer.Ok());
        var sink   = Sinks.Over(server, waits: waits);

        await sink.EmitBatchAsync(Many(1));

        Assert.Equal([AmetoBatchedSink.MaxInlineRetryAfter], waits);
    }

    [Theory]
    [InlineData(-60,  0)]    // a date already past: no wait
    [InlineData(5,    5)]    // five seconds ahead: about five seconds
    [InlineData(3600, 10)]   // an hour ahead: clamped
    public void A_Retry_After_date_is_the_wait_until_it_clamped(int secondsFromNow, int expectedSeconds)
    {
        using var response = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
        response.Headers.RetryAfter = new RetryConditionHeaderValue(DateTimeOffset.UtcNow.AddSeconds(secondsFromNow));

        var wait = AmetoBatchedSink.RetryAfterOf(response);

        Assert.InRange(wait, TimeSpan.FromSeconds(expectedSeconds - 1), TimeSpan.FromSeconds(expectedSeconds));
    }

    [Fact]
    public async Task Each_request_of_a_batch_gets_an_inline_retry_of_its_own()
    {
        // Request 1: 503, then 200 on the inline retry. Request 2: 503 again — a new request, so it gets
        // its own inline retry rather than the batching layer at once.
        var waits  = new List<TimeSpan>();
        var server = new FakeServer((i, _) => i is 0 or 2 ? FakeServer.Unavailable() : FakeServer.Ok());
        var sink   = Sinks.Over(server, batchBytesLimit: 6 * 1024, waits: waits);

        await sink.EmitBatchAsync(Many(10, fillerChars: 1000));   // ~1.1 KB events, five to a request

        Assert.Equal([AmetoBatchedSink.DefaultRetryAfter, AmetoBatchedSink.DefaultRetryAfter], waits);
        Assert.Equal(4, server.Bodies.Count);
        Assert.Equal(Enumerable.Range(0, 10).Select(i => (long)i),
            server.Bodies.Where((_, i) => i is 1 or 3).SelectMany(Wire.Decode).Select(Wire.I));
    }

    [Fact]
    public async Task A_429_is_handled_like_a_503()
    {
        var waits  = new List<TimeSpan>();
        var server = new FakeServer((i, _) => i == 0 ? FakeServer.Status(HttpStatusCode.TooManyRequests) : FakeServer.Ok());
        var sink   = Sinks.Over(server, waits: waits);

        await sink.EmitBatchAsync(Many(1));

        Assert.Equal([AmetoBatchedSink.DefaultRetryAfter], waits);
        Assert.Equal(2, server.Bodies.Count);
    }

    [Theory]
    [InlineData(HttpStatusCode.InternalServerError)]
    [InlineData(HttpStatusCode.BadGateway)]
    [InlineData(HttpStatusCode.GatewayTimeout)]
    [InlineData(HttpStatusCode.RequestTimeout)]   // 408: the request never arrived whole; sending it again is the cure, not a configuration error
    public async Task A_5xx_or_a_408_goes_straight_to_the_batching_layer(HttpStatusCode status)
    {
        var waits  = new List<TimeSpan>();
        var server = FakeServer.Always(() => FakeServer.Status(status));
        var sink   = Sinks.Over(server, waits: waits);

        var ex = await Assert.ThrowsAsync<AmetoDeliveryException>(() => sink.EmitBatchAsync(Many(2)));

        Assert.Equal((int)status, ex.StatusCode);
        Assert.Empty(waits);
        Assert.Single(server.Bodies);
    }

    [Fact]
    public async Task A_retried_batch_resumes_past_the_requests_that_went_through()
    {
        // ~1.1 KB events under a 6 KB cap: five to a request. The second request fails for good
        // (a 502 goes to the batching layer at once); the batching layer then hands the SAME batch
        // back, and the sink must not repeat the first request — the server stored it and does not
        // de-duplicate.
        var server = new FakeServer((i, _) => i == 1 ? FakeServer.Status(HttpStatusCode.BadGateway) : FakeServer.Ok());
        var sink   = Sinks.Over(server, batchBytesLimit: 6 * 1024);
        var batch  = Many(12, fillerChars: 1000);

        await Assert.ThrowsAsync<AmetoDeliveryException>(() => sink.EmitBatchAsync(batch));
        Assert.Equal(2, server.Bodies.Count);
        var first = Wire.Decode(server.Bodies[0]).Select(Wire.I).ToList();
        var failed = Wire.Decode(server.Bodies[1]).Select(Wire.I).ToList();

        await sink.EmitBatchAsync(batch);   // the retry

        var afterRetry = server.Bodies.Skip(2).ToList();
        Assert.Equal(failed[0], Wire.I(Wire.Decode(afterRetry[0])[0]));   // picks up exactly where it failed
        var stored = first.Concat(afterRetry.SelectMany(Wire.Decode).Select(Wire.I)).ToList();
        Assert.Equal(Enumerable.Range(0, 12).Select(i => (long)i), stored);   // each event once, in order
    }

    [Fact]
    public async Task Through_the_logger_a_retried_batch_stores_each_event_once()
    {
        // The real batching layer (Serilog's BatchingSink, which PeriodicBatching 5.0 wraps): the second
        // request is answered 502, the layer hands the SAME batch back after one Period, and the sink
        // resumes past the first request. (All twelve are logged before the first send, so the layer's
        // top-up of a retried batch with newer events is not what this exercises.)
        var server = new FakeServer((i, _) => i == 1 ? FakeServer.Status(HttpStatusCode.BadGateway) : FakeServer.Ok());
        var logger = new LoggerConfiguration()
            .WriteTo.Ameto("http://ameto.test", "k", "svc", o =>
            {
                o.HttpClient            = new HttpClient(server);
                o.Period                = TimeSpan.FromMilliseconds(10);
                o.BatchBytesLimit       = 6 * 1024;
                o.EagerlyEmitFirstEvent = false;
            })
            .CreateLogger();

        var filler = new string('f', 1000);
        for (int i = 0; i < 12; i++)
            logger.Information("Event {I} {Filler}", i, filler);

        // Three requests' worth of events, one of them answered 502: four bodies once the layer's own
        // retry has gone out — before the shutdown flush, which would also resume, gets its chance.
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (server.Count < 4 && DateTime.UtcNow < deadline)
            await Task.Delay(10);
        logger.Dispose();

        var bodies = server.Snapshot();
        Assert.True(bodies.Count >= 4, $"{bodies.Count} requests");
        var stored = bodies.Where((_, i) => i != 1).SelectMany(Wire.Decode).Select(Wire.I).ToList();
        Assert.Equal(Enumerable.Range(0, 12).Select(i => (long)i), stored);   // each event once, in order
    }

    [Fact]
    public async Task A_different_batch_after_a_failure_starts_from_its_first_event()
    {
        var server = new FakeServer((i, _) => i == 1 ? FakeServer.Status(HttpStatusCode.BadGateway) : FakeServer.Ok());
        var sink   = Sinks.Over(server, batchBytesLimit: 6 * 1024);

        await Assert.ThrowsAsync<AmetoDeliveryException>(() => sink.EmitBatchAsync(Many(12, fillerChars: 1000)));

        await sink.EmitBatchAsync(Many(3));   // a NEW batch (new instances), not the retry

        Assert.Equal([0L, 1L, 2L], Wire.Decode(server.Bodies[2]).Select(Wire.I));
    }

    // ── 401 and friends, transport ────────────────────────────────────────────

    [Fact]
    public async Task A_401_drops_the_batch_without_a_retry_and_the_next_batch_is_tried()
    {
        var waits  = new List<TimeSpan>();
        var server = new FakeServer((i, _) => i == 0 ? FakeServer.Status(HttpStatusCode.Unauthorized) : FakeServer.Ok());
        var sink   = Sinks.Over(server, waits: waits);

        await sink.EmitBatchAsync(Many(3));   // no throw: retrying a bad key cannot help
        Assert.Single(server.Bodies);
        Assert.Empty(waits);

        await sink.EmitBatchAsync(Many(2));
        Assert.Equal(2, server.Bodies.Count);
        Assert.Equal([0L, 1L], Wire.Decode(server.Bodies[1]).Select(Wire.I));
    }

    [Fact]
    public async Task A_transport_failure_propagates_to_the_batching_layer()
    {
        var server = new FakeServer((_, _) => throw new HttpRequestException("connection refused"));
        var sink   = Sinks.Over(server);

        await Assert.ThrowsAsync<HttpRequestException>(() => sink.EmitBatchAsync(Many(1)));
    }

    [Fact]
    public async Task An_empty_batch_sends_nothing()
    {
        var server = FakeServer.Always(() => FakeServer.Ok());
        var sink   = Sinks.Over(server);

        await sink.EmitBatchAsync([]);

        Assert.Empty(server.Bodies);
    }

    // ── Buffer ────────────────────────────────────────────────────────────────

    [Fact]
    public void A_buffer_grown_far_past_the_keep_size_is_let_go_and_a_small_one_stays()
    {
        using var writer = new PooledBufferWriter(1024);
        writer.GetSpan(1 << 20);
        writer.Advance(1 << 20);
        Assert.True(writer.Capacity >= 1 << 20);

        writer.Trim(keepBytes: 2 << 20);
        Assert.True(writer.Capacity >= 1 << 20, "under the keep size the buffer stays");

        writer.Trim(keepBytes: 64 * 1024);
        Assert.True(writer.Capacity < 1 << 20, "over it the buffer goes back to the pool");
        Assert.Equal(0, writer.Position);
    }
}
