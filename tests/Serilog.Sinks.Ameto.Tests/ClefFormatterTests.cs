using System.Diagnostics;
using Ameto.Core;
using Serilog.Events;
using static Serilog.Sinks.Ameto.Tests.Events;

namespace Serilog.Sinks.Ameto.Tests;

/// <summary>
/// The wire contract: what <see cref="AmetoClefFormatter"/> writes is what the server's decoder reads,
/// field by field — the only other speaker of this format is the server itself.
/// </summary>
public class ClefFormatterTests
{
    [Fact]
    public void Header_fields_arrive_as_the_server_reads_them()
    {
        var traceId = ActivityTraceId.CreateRandom();
        var spanId  = ActivitySpanId.CreateRandom();
        var evt = Make("User {Name} signed in", LogEventLevel.Warning,
            properties: [P("Name", "ann")], traceId: traceId, spanId: spanId);

        var decoded = Wire.DecodeOne(Wire.Body(evt));

        Assert.Equal(T0, decoded.Timestamp);
        Assert.Equal("User {Name} signed in", decoded.MessageTemplate);
        Assert.Equal("Warning", decoded.Level.ToString());
        Assert.Equal("svc", decoded.ServiceName);
        Assert.Equal("ann", decoded.Properties!["Name"]);

        Assert.True(TraceIdHelper.TryParseTraceId(traceId.ToHexString(), out var hi, out var lo));
        Assert.Equal(hi, decoded.TraceIdHi);
        Assert.Equal(lo, decoded.TraceIdLo);
        Assert.True(TraceIdHelper.TryParseSpanId(spanId.ToHexString(), out var sp));
        Assert.Equal(sp, decoded.SpanId);
    }

    [Fact]
    public void Every_level_maps_to_the_servers_name()
    {
        foreach (var level in Enum.GetValues<LogEventLevel>())
        {
            var decoded = Wire.DecodeOne(Wire.Body(Make("x", level)));
            Assert.Equal(level.ToString(), decoded.Level.ToString());
        }
    }

    [Fact]
    public void Without_a_service_or_trace_the_event_carries_neither()
    {
        var decoded = Wire.DecodeOne(Wire.Body(service: null, Make("x")));

        Assert.Null(decoded.ServiceName);
        Assert.Equal(0UL, decoded.TraceIdHi | decoded.TraceIdLo);
        Assert.Equal(0UL, decoded.SpanId);
        Assert.Null(decoded.Exception);
    }

    [Fact]
    public void Every_scalar_kind_round_trips()
    {
        var guid = Guid.NewGuid();
        var dt   = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var dto  = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.FromHours(3));
        var evt  = Make(properties:
        [
            P("S", "text"), P("B", true), P("I", 42), P("Neg", -7), P("L", 1L << 40), P("U", ulong.MaxValue),
            P("D", 1.5), P("F", 2.5f), P("Dec", 12.34m), P("Dt", dt), P("Dto", dto),
            P("Ts", TimeSpan.FromMinutes(90)), P("G", guid), P("Uri", new Uri("http://x/y")),
            P("E", DayOfWeek.Friday), P("Bytes", new byte[] { 1, 2, 3 }), P("N", (object?)null),
        ]);

        var p = Wire.DecodeOne(Wire.Body(evt)).Properties!;

        Assert.Equal("text", p["S"]);
        Assert.Equal(true, p["B"]);
        Assert.Equal(42L, Convert.ToInt64(p["I"]));
        Assert.Equal(-7L, Convert.ToInt64(p["Neg"]));
        Assert.Equal(1L << 40, Convert.ToInt64(p["L"]));
        Assert.Equal(ulong.MaxValue, Convert.ToUInt64(p["U"]));
        Assert.Equal(1.5, Convert.ToDouble(p["D"]));
        Assert.Equal(2.5, Convert.ToDouble(p["F"]));
        Assert.Equal("12.34", p["Dec"]);
        Assert.Equal("2026-01-02T03:04:05.0000000Z", p["Dt"]);
        Assert.Equal("2026-01-02T03:04:05.0000000+03:00", p["Dto"]);
        Assert.Equal("01:30:00", p["Ts"]);
        Assert.Equal(guid.ToString("D"), p["G"]);
        Assert.Equal("http://x/y", p["Uri"]);
        Assert.Equal("Friday", p["E"]);
        Assert.Equal(new byte[] { 1, 2, 3 }, p["Bytes"]);
        Assert.Null(p["N"]);
    }

    [Fact]
    public void Sequences_structures_and_dictionaries_nest()
    {
        var seq  = new SequenceValue([new ScalarValue(1), new ScalarValue("two"), new SequenceValue([new ScalarValue(3)])]);
        var str  = new StructureValue([new LogEventProperty("X", new ScalarValue(1)), new LogEventProperty("Y", new ScalarValue("y"))], "Point");
        var dict = new DictionaryValue([new KeyValuePair<ScalarValue, LogEventPropertyValue>(new ScalarValue("k"), new ScalarValue("v"))]);

        var p = Wire.DecodeOne(Wire.Body(Make(properties: [P("Seq", seq), P("Str", str), P("Dict", dict)]))).Properties!;

        var arr = Assert.IsType<object[]>(p["Seq"]);
        Assert.Equal(3, arr.Length);
        Assert.Equal(1L, Convert.ToInt64(arr[0]));
        Assert.Equal("two", arr[1]);
        Assert.Equal(3L, Convert.ToInt64(Assert.IsType<object[]>(arr[2])[0]));

        var map = Assert.IsType<Dictionary<string, object?>>(p["Str"]);
        Assert.Equal("Point", map["$type"]);
        Assert.Equal(1L, Convert.ToInt64(map["X"]));
        Assert.Equal("y", map["Y"]);

        var d = Assert.IsType<Dictionary<string, object?>>(p["Dict"]);
        Assert.Equal("v", d["k"]);
    }

    [Fact]
    public void An_exception_keeps_type_message_stack_and_two_inner_levels()
    {
        var ex  = Thrown("outer", Thrown("inner", Thrown("inner2", Thrown("inner3"))));
        var evt = Make("Boom", LogEventLevel.Error, exception: ex);

        var x = Wire.DecodeOne(Wire.Body(evt)).Exception!;

        Assert.Equal(typeof(InvalidOperationException).FullName, x.Type);
        Assert.Equal("outer", x.Message);
        Assert.Contains(nameof(Thrown), x.StackTrace);
        Assert.Equal("inner", x.Inner!.Message);
        Assert.Equal("inner2", x.Inner.Inner!.Message);
        // Depth 3 on both sides: the fourth level is not written, and the server would not read it.
        Assert.Null(x.Inner.Inner.Inner);
    }

    [Fact]
    public void A_budget_cuts_the_message_and_the_stack_and_drops_the_inner_chain()
    {
        var ex  = Thrown(new string('m', 10_000), Thrown("inner"));
        var evt = Make("Boom", LogEventLevel.Error, exception: ex);

        var buffer = new System.Buffers.ArrayBufferWriter<byte>();
        var writer = new MessagePack.MessagePackWriter(buffer);
        writer.WriteArrayHeader(1);
        AmetoClefFormatter.WriteHeader(ref writer, evt, "svc"u8.ToArray());
        writer.WriteString("@x"u8);
        AmetoClefFormatter.WriteException(ref writer, ex, ExceptionBudget.For(2048));
        AmetoClefFormatter.WriteProperties(ref writer, evt);
        writer.Flush();

        var x = Wire.DecodeOne(buffer.WrittenSpan.ToArray()).Exception!;

        Assert.Equal(typeof(InvalidOperationException).FullName, x.Type);
        Assert.StartsWith("mmmm", x.Message);
        Assert.EndsWith("[cut by Serilog.Sinks.Ameto]", x.Message);
        Assert.True(x.Message!.Length < 400, $"message kept {x.Message.Length} chars of 10 000");
        Assert.NotNull(x.StackTrace);
        Assert.Null(x.Inner);
    }

    [Fact]
    public void A_cut_never_splits_a_surrogate_pair()
    {
        // 'a' then 100 emoji of two UTF-16 units each: the 128th unit, where the message budget for a
        // 1 KB limit ends, is the high half of a pair. Split, it would be written as U+FFFD.
        var message = "a" + string.Concat(Enumerable.Repeat("\U0001F600", 100));
        var ex      = Thrown(message);
        var evt     = Make("Boom", LogEventLevel.Error, exception: ex);

        var buffer = new System.Buffers.ArrayBufferWriter<byte>();
        var writer = new MessagePack.MessagePackWriter(buffer);
        writer.WriteArrayHeader(1);
        AmetoClefFormatter.WriteHeader(ref writer, evt, "svc"u8.ToArray());
        writer.WriteString("@x"u8);
        AmetoClefFormatter.WriteException(ref writer, ex, ExceptionBudget.For(1024));
        AmetoClefFormatter.WriteProperties(ref writer, evt);
        writer.Flush();

        var x = Wire.DecodeOne(buffer.WrittenSpan.ToArray()).Exception!;

        Assert.StartsWith("a\U0001F600", x.Message);
        Assert.DoesNotContain("�", x.Message);
        Assert.EndsWith("[cut by Serilog.Sinks.Ameto]", x.Message);
        Assert.Equal(127, x.Message!.IndexOf(" … [cut", StringComparison.Ordinal));   // 127 units kept, not 128
    }

    [Fact]
    public void The_budget_for_a_limit_scales_with_it()
    {
        var small = ExceptionBudget.For(1024);
        Assert.Equal(128, small.MaxMessageChars);
        Assert.Equal(512, small.MaxStackChars);
        Assert.False(small.KeepInner);

        var big = ExceptionBudget.For(256 * 1024);
        Assert.Equal(4096, big.MaxMessageChars);
        Assert.Equal(128 * 1024, big.MaxStackChars);
    }
}
