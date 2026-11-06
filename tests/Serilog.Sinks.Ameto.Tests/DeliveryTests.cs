using System.Net;
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

    [Theory]
    [InlineData("{\"ingested\":3,\"dropped\":0,\"failedAtElement\":3}", 3)]
    [InlineData("{\"failedAtElement\":0}", 0)]
    [InlineData("{\"ingested\":0,\"dropped\":0}", -1)]
    [InlineData("{\"a\":{\"failedAtElement\":5},\"failedAtElement\":2}", 2)]
    [InlineData("{\"failedAtElement\":\"x\"}", -1)]
    [InlineData("not json", -1)]
    [InlineData("", -1)]
    public void failedAtElement_is_read_from_the_servers_json(string body, int expected)
        => Assert.Equal(expected, AmetoBatchedSink.ReadFailedAtElement(Encoding.UTF8.GetBytes(body)));

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
    public async Task A_413_for_a_single_event_sends_a_marker_in_its_place()
    {
        var server = new FakeServer((_, body) => Wire.Decode(body).Any(e => e.MessageTemplate == "Big {Blob}")
            ? FakeServer.Status(HttpStatusCode.RequestEntityTooLarge)
            : FakeServer.Ok());
        var sink   = Sinks.Over(server);
        var events = new[] { Make("A"), Make("Big {Blob}", Serilog.Events.LogEventLevel.Fatal, properties: [P("Blob", new string('x', 3000))]), Make("C") };

        await sink.EmitBatchAsync(events);

        // [A, Big, C] → 413; one at a time from here: A → 200, Big → 413, its marker → 200, C → 200.
        Assert.Equal(5, server.Bodies.Count);
        var marker = Wire.DecodeOne(server.Bodies[3]);
        Assert.Equal(AmetoClefFormatter.RefusedTemplate, marker.MessageTemplate);
        Assert.Equal("Error", marker.Level.ToString());
        Assert.Equal("sink", marker.Properties!["DroppedBy"]);
        Assert.True(Convert.ToInt64(marker.Properties["EventBodyBytes"]) > 3000);
        Assert.Equal("Big {Blob}", marker.Properties["OriginalTemplate"]);
        Assert.Equal("Fatal", marker.Properties["OriginalLevel"]);
        Assert.Equal("C", Wire.DecodeOne(server.Bodies[4]).MessageTemplate);
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

    [Fact]
    public async Task A_500_goes_straight_to_the_batching_layer()
    {
        var waits  = new List<TimeSpan>();
        var server = FakeServer.Always(() => FakeServer.Status(HttpStatusCode.InternalServerError));
        var sink   = Sinks.Over(server, waits: waits);

        var ex = await Assert.ThrowsAsync<AmetoDeliveryException>(() => sink.EmitBatchAsync(Many(2)));

        Assert.Equal(500, ex.StatusCode);
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
