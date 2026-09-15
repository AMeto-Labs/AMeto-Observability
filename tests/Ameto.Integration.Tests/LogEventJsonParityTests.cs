using System.Buffers;
using System.Text;
using System.Text.Json;
using Ameto.Core;
using Ameto.Server;
using MessagePack;

namespace Ameto.Integration.Tests;

/// <summary>
/// THE BYTES OF AN EVENT FRAME ARE A CONTRACT. The events SSE stream and the span/trace log
/// lists used to go out as <c>LogEventDto</c> through the reflection-based serialiser;
/// <see cref="LogEventJsonWriter"/> now writes them straight from the <see cref="LogEvent"/>.
/// Everything downstream — the Angular client's parser, the transcoder parity suite, the
/// integration tests that read frames back — reads those bytes positionally, so "equivalent
/// JSON" is not the bar: the output has to be identical, byte for byte.
///
/// <para>The page below is built to hit the places where a hand-written writer is likeliest to
/// diverge from the serialiser's defaults: a timestamp whose fraction ends in zeros (the
/// built-in <c>WriteString(name, DateTimeOffset)</c> trims those and <c>"O"</c> does not), a
/// non-UTC offset, every combination of absent optional field, a two-deep exception with a
/// null message and a null stack, properties from raw msgpack AND from an already-built
/// dictionary, and text that forces escaping — quotes, control characters, non-ASCII and a
/// supplementary-plane pair.</para>
/// </summary>
public sealed class LogEventJsonParityTests
{
    [Fact]
    public void Direct_writer_and_dto_serialiser_produce_identical_bytes()
    {
        foreach (var ev in Page())
        {
            string viaDto    = ViaDto(ev);
            string viaDirect = ViaDirect(ev);
            Assert.Equal(viaDto, viaDirect);
        }
    }

    [Fact]
    public void Whole_page_serialises_identically_as_one_array()
    {
        var page = Page();

        var buf = new ArrayBufferWriter<byte>(1 << 16);
        using (var w = new Utf8JsonWriter(buf))
        {
            w.WriteStartArray();
            foreach (var ev in page) JsonSerializer.Serialize(w, LogEventDto.From(ev), EndpointMapper._json);
            w.WriteEndArray();
        }
        byte[] expected = buf.WrittenSpan.ToArray();

        buf.ResetWrittenCount();
        using (var w = new Utf8JsonWriter(buf))
        {
            w.WriteStartArray();
            foreach (var ev in page) LogEventJsonWriter.Write(w, ev);
            w.WriteEndArray();
        }

        Assert.Equal(Encoding.UTF8.GetString(expected), Encoding.UTF8.GetString(buf.WrittenSpan));
    }

    /// <summary>The <c>"O"</c> round-trip format is the contract, NOT the shortest ISO form.</summary>
    [Fact]
    public void Timestamp_keeps_all_seven_fraction_digits()
    {
        // The '+' of the offset arrives ESCAPED — the default JavaScriptEncoder spells it
        // \u002B — and that escaping is part of the bytes the client has always received.
        string json = ViaDirect(Minimal());
        Assert.Contains("\"@t\":\"2026-09-11T12:00:00.0000000\\u002B00:00\"", json);
        Assert.Equal(ViaDto(Minimal()), json);
    }

    // ── the page ──────────────────────────────────────────────────────────────

    private static List<LogEvent> Page()
    {
        var list = new List<LogEvent>();

        // 1. Nothing optional at all: no exception, no trace, no span, no service, no props.
        list.Add(new LogEvent
        {
            Id              = new EventId(0u, 1u),
            Timestamp       = new DateTimeOffset(2026, 9, 11, 12, 0, 0, TimeSpan.Zero),
            Level           = Ameto.Core.LogLevel.Information,
            MessageTemplate = "plain",
        });

        // 2. Everything present, raw msgpack properties, exception two deep.
        list.Add(new LogEvent
        {
            Id              = new EventId(18_446_744_073_709_551_615UL),
            Timestamp       = new DateTimeOffset(2026, 9, 11, 12, 0, 0, 123, TimeSpan.FromHours(5)).AddTicks(4567),
            Level           = Ameto.Core.LogLevel.Error,
            MessageTemplate = "Handled {Command} \"quoted\"  ünïcødé 😀",
            ServiceName     = "Office.API",
            TraceIdHi       = 0x0123456789abcdefUL,
            TraceIdLo       = 0xfedcba9876543210UL,
            SpanId          = 0x00000000000000ffUL,
            Exception       = new ExceptionInfo
            {
                Type       = "System.InvalidOperationException",
                Message    = "outer \"boom\"\n\ttab",
                StackTrace = "   at A.B() in /src/A.cs:line 1\n   at C.D()",
                Inner      = new ExceptionInfo
                {
                    Type    = "System.NullReferenceException",
                    Message = null,                 // omitted, not null-valued
                    Inner   = new ExceptionInfo { Type = "System.Exception" },
                },
            },
            RawProperties   = FatProps(),
        });

        // 3. Trace without span, service without trace, level at each end of the range.
        list.Add(new LogEvent
        {
            Id              = new EventId(1u, 2u),
            Timestamp       = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
            Level           = Ameto.Core.LogLevel.Verbose,
            MessageTemplate = "",
            TraceIdHi       = 0UL,
            TraceIdLo       = 1UL,                  // hi zero, lo set — still a trace id
        });
        list.Add(new LogEvent
        {
            Id              = new EventId(1u, 3u),
            Timestamp       = new DateTimeOffset(2026, 12, 31, 23, 59, 59, TimeSpan.FromHours(-8)).AddTicks(9_999_999),
            Level           = Ameto.Core.LogLevel.Fatal,
            MessageTemplate = "end",
            ServiceName     = "svc/with spaces & \"quotes\"",
            SpanId          = 0xdeadbeefcafef00dUL, // span without trace
        });

        // 4. Properties as an ALREADY MATERIALISED dictionary — the second door in the writer.
        list.Add(new LogEvent
        {
            Id              = new EventId(2u, 9u),
            Timestamp       = new DateTimeOffset(2026, 5, 5, 5, 5, 5, TimeSpan.Zero),
            Level           = Ameto.Core.LogLevel.Warning,
            MessageTemplate = "dictionary props",
            Properties      = new Dictionary<string, object?>
            {
                ["str"]    = "sträng 𝕏",
                ["nul"]    = null,
                ["i64"]    = 9_223_372_036_854_775_807L,
                ["i32"]    = -17,
                ["u64"]    = 18_446_744_073_709_551_615UL,
                ["dbl"]    = 1.5d,
                ["flt"]    = 2.5f,
                ["bool"]   = true,
                ["arr"]    = new object?[] { 1L, null, "x", new Dictionary<string, object?> { ["k"] = "v" } },
                ["nested"] = new Dictionary<string, object?> { ["deep"] = new object?[] { true, 3L } },
            },
        });

        // 5. An empty property map is still a `props: {}` — not an omission.
        list.Add(new LogEvent
        {
            Id              = new EventId(2u, 10u),
            Timestamp       = new DateTimeOffset(2026, 6, 6, 6, 6, 6, TimeSpan.Zero),
            Level           = Ameto.Core.LogLevel.Debug,
            MessageTemplate = "empty props",
            RawProperties   = EmptyMap(),
        });

        return list;
    }

    private static LogEvent Minimal() => new()
    {
        Id              = new EventId(0u, 1u),
        Timestamp       = new DateTimeOffset(2026, 9, 11, 12, 0, 0, TimeSpan.Zero),
        Level           = Ameto.Core.LogLevel.Information,
        MessageTemplate = "plain",
    };

    private static ReadOnlyMemory<byte> FatProps()
    {
        var buf = new ArrayBufferWriter<byte>(1024);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(6);
        w.Write("SourceContext");  w.Write("Common.MediatR.LoggingBehavior");
        w.Write("Unicode");        w.Write("ключ 😀 \"q\"");
        w.Write("Nil");            w.WriteNil();
        w.Write("Number");         w.Write(-1.25d);
        w.Write("Flag");           w.Write(false);
        w.Write("Response");
        w.WriteMapHeader(2);
        w.Write("$type");          w.Write("MergeCreateCommand");
        w.Write("Items");
        w.WriteArrayHeader(3);
        for (int k = 0; k < 3; k++)
        {
            w.WriteMapHeader(1);
            w.Write("Name"); w.Write("Resource" + k);
        }
        w.Flush();
        return buf.WrittenMemory;
    }

    private static ReadOnlyMemory<byte> EmptyMap()
    {
        var buf = new ArrayBufferWriter<byte>(8);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(0);
        w.Flush();
        return buf.WrittenMemory;
    }

    // ── the two roads ─────────────────────────────────────────────────────────

    private static string ViaDto(LogEvent ev)
    {
        var buf = new ArrayBufferWriter<byte>(4096);
        using var w = new Utf8JsonWriter(buf);
        JsonSerializer.Serialize(w, LogEventDto.From(ev), EndpointMapper._json);
        w.Flush();
        return Encoding.UTF8.GetString(buf.WrittenSpan);
    }

    private static string ViaDirect(LogEvent ev)
    {
        var buf = new ArrayBufferWriter<byte>(4096);
        using var w = new Utf8JsonWriter(buf);
        LogEventJsonWriter.Write(w, ev);
        w.Flush();
        return Encoding.UTF8.GetString(buf.WrittenSpan);
    }
}
