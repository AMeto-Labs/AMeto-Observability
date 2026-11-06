using Serilog.Events;
using static Serilog.Sinks.Ameto.Tests.Events;
using AmetoEvent = Ameto.Core.LogEvent;
using SerilogEvent = Serilog.Events.LogEvent;

namespace Serilog.Sinks.Ameto.Tests;

/// <summary>
/// The sink's per-event limits, measured the way the server measures: the properties map on its own,
/// the exception on its own — and the marker an event leaves behind when it cannot go as it is.
/// </summary>
public class EventLimitTests
{
    private static async Task<List<AmetoEvent>> SendAsync(AmetoBatchedSink sink, FakeServer server, params SerilogEvent[] events)
    {
        await sink.EmitBatchAsync(events);
        return server.Bodies.SelectMany(Wire.Decode).ToList();
    }

    [Fact]
    public async Task Properties_over_the_limit_become_a_marker_that_names_the_original()
    {
        var server = FakeServer.Always(() => FakeServer.Ok());
        var sink   = Sinks.Over(server, propertiesLimit: 1024);
        var big    = Make("Big {Blob}", LogEventLevel.Warning, properties: [P("Blob", new string('x', 5000))],
                          traceId: System.Diagnostics.ActivityTraceId.CreateRandom());

        var got = await SendAsync(sink, server, Make("Before"), big, Make("After"));

        Assert.Equal(["Before", AmetoClefFormatter.OversizedTemplate, "After"], got.Select(e => e.MessageTemplate));

        var marker = got[1];
        Assert.Equal("Error", marker.Level.ToString());
        Assert.Equal(T0, marker.Timestamp);
        Assert.Equal("svc", marker.ServiceName);
        Assert.NotEqual(0UL, marker.TraceIdHi | marker.TraceIdLo);   // the marker keeps the original's trace context

        var p = marker.Properties!;
        Assert.Equal("sink", p["DroppedBy"]);
        Assert.True(Convert.ToInt64(p["EventBodyBytes"]) > 5000, "EventBodyBytes is the properties map's size");
        Assert.Equal(1024L, Convert.ToInt64(p["EventBodyLimitBytes"]));
        Assert.Equal("Big {Blob}", p["OriginalTemplate"]);
        Assert.Equal("Warning", p["OriginalLevel"]);
        Assert.False(p.ContainsKey("Blob"));
    }

    [Theory]
    [InlineData(65_527, false)]   // 5 B key + 3 B str16 header + 65 527 = 65 535 B of pairs, + the 1 B map header = the limit: through
    [InlineData(65_528, true)]    // 65 536 B of pairs + the map header: one over, as the server counts it
    public async Task The_properties_limit_counts_the_map_header_as_the_server_does(int blobChars, bool overLimit)
    {
        const int limit = 65_536;
        var server = FakeServer.Always(() => FakeServer.Ok());
        var sink   = Sinks.Over(server, propertiesLimit: limit);

        var got = await SendAsync(sink, server, Make("Big {Blob}", properties: [P("Blob", new string('x', blobChars))]));

        var evt = Assert.Single(got);
        Assert.Equal(overLimit ? AmetoClefFormatter.OversizedTemplate : "Big {Blob}", evt.MessageTemplate);
        if (overLimit) Assert.Equal(limit + 1L, Convert.ToInt64(evt.Properties!["EventBodyBytes"]));

        // What the sink let through, the server takes under the same limit: its streaming decode, the
        // one place it measures the map, refuses nothing of the body.
        var streamed = Assert.Single(Wire.Stream(server.Bodies[0], maxEventPayloadBytes: limit));
        Assert.False(streamed.Oversized, $"the server would refuse a {streamed.PropertiesBytes} B map");
        if (!overLimit) Assert.Equal(limit, streamed.PropertiesBytes);
    }

    [Fact]
    public void The_server_refuses_the_event_a_measure_without_the_header_let_through()
    {
        // The pairs alone are exactly the limit; the server counts the map header too and drops the event.
        var body     = Wire.Body(Make("Big {Blob}", properties: [P("Blob", new string('x', 65_528))]));
        var streamed = Assert.Single(Wire.Stream(body, maxEventPayloadBytes: 65_536));

        Assert.True(streamed.Oversized);
        Assert.Equal(65_537, streamed.PropertiesBytes);
    }

    [Fact]
    public async Task The_properties_limit_measures_the_properties_not_the_exception()
    {
        // 4 KB of stack trace, next to no properties: the SERVER would store this event (its cap is on
        // the properties map), so the sink must not replace it. The old whole-event limit did.
        var server = FakeServer.Always(() => FakeServer.Ok());
        var sink   = Sinks.Over(server, propertiesLimit: 1024);
        var ex     = Thrown(new string('m', 4000));

        var got = await SendAsync(sink, server, Make("Boom {Id}", LogEventLevel.Error, exception: ex, properties: [P("Id", 7)]));

        var evt = Assert.Single(got);
        Assert.Equal("Boom {Id}", evt.MessageTemplate);
        Assert.Equal(7L, Convert.ToInt64(evt.Properties!["Id"]));
        Assert.Equal(ex.Message, evt.Exception!.Message);
    }

    [Fact]
    public async Task A_big_exception_is_cut_but_the_event_arrives()
    {
        var server = FakeServer.Always(() => FakeServer.Ok());
        var sink   = Sinks.Over(server, exceptionLimit: 2048);
        var ex     = Thrown(new string('m', 10_000), Thrown("inner"));

        var got = await SendAsync(sink, server, Make("Boom {Id}", LogEventLevel.Error, exception: ex, properties: [P("Id", 7)]));

        var evt = Assert.Single(got);
        Assert.Equal("Boom {Id}", evt.MessageTemplate);
        Assert.Equal(7L, Convert.ToInt64(evt.Properties!["Id"]));

        var x = evt.Exception!;
        Assert.Equal(typeof(InvalidOperationException).FullName, x.Type);
        Assert.EndsWith("[cut by Serilog.Sinks.Ameto]", x.Message);
        Assert.True(x.Message!.Length < 400);
        Assert.Null(x.Inner);
    }

    [Fact]
    public async Task An_exception_under_the_limit_is_untouched()
    {
        var server = FakeServer.Always(() => FakeServer.Ok());
        var sink   = Sinks.Over(server);
        var ex     = Thrown(new string('m', 2000), Thrown("inner"));

        var got = await SendAsync(sink, server, Make("Boom", LogEventLevel.Error, exception: ex));

        var x = Assert.Single(got).Exception!;
        Assert.Equal(ex.Message, x.Message);
        Assert.Equal(ex.StackTrace, x.StackTrace);
        Assert.Equal("inner", x.Inner!.Message);
    }

    [Fact]
    public async Task A_property_whose_rendering_throws_costs_only_that_event()
    {
        var server = FakeServer.Always(() => FakeServer.Ok());
        var sink   = Sinks.Over(server);
        var bad    = Make("Bad {Obj}", LogEventLevel.Debug, properties: [P("Obj", new Unrenderable())]);

        var got = await SendAsync(sink, server, Make("Before"), bad, Make("After"));

        Assert.Equal(["Before", AmetoClefFormatter.FormatFailureTemplate, "After"], got.Select(e => e.MessageTemplate));

        var marker = got[1];
        Assert.Equal("Error", marker.Level.ToString());
        var p = marker.Properties!;
        Assert.Equal("sink", p["DroppedBy"]);
        Assert.Contains("ToString failed on purpose", (string)p["SinkError"]!);
        Assert.Equal("Bad {Obj}", p["OriginalTemplate"]);
        Assert.Equal("Debug", p["OriginalLevel"]);
    }

    [Fact]
    public async Task A_failure_whose_exception_cannot_say_its_message_still_leaves_a_marker_naming_the_type()
    {
        var server = FakeServer.Always(() => FakeServer.Ok());
        var sink   = Sinks.Over(server);
        var bad    = Make("Bad {Obj}", properties: [P("Obj", new UnrenderableWithoutMessage())]);

        var got = await SendAsync(sink, server, Make("Before"), bad, Make("After"));

        Assert.Equal(["Before", AmetoClefFormatter.FormatFailureTemplate, "After"], got.Select(e => e.MessageTemplate));
        Assert.Equal(nameof(MessagelessException), got[1].Properties!["SinkError"]);
    }

    [Fact]
    public async Task A_limit_of_zero_means_no_limit()
    {
        var server = FakeServer.Always(() => FakeServer.Ok());
        var sink   = Sinks.Over(server, propertiesLimit: 0, exceptionLimit: 0);
        var ex     = Thrown(new string('m', 100_000));

        var got = await SendAsync(sink, server,
            Make("Big {Blob}", exception: ex, properties: [P("Blob", new string('x', 200_000))]));

        var evt = Assert.Single(got);
        Assert.Equal("Big {Blob}", evt.MessageTemplate);
        Assert.Equal(200_000, ((string)evt.Properties!["Blob"]!).Length);
        Assert.Equal(100_000, evt.Exception!.Message!.Length);
    }
}
