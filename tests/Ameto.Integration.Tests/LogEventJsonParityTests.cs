using System.Buffers;
using System.Text;
using System.Text.Json;
using Ameto.Core;
using MessagePack;

namespace Ameto.Integration.Tests;

/// <summary>
/// THE BYTES OF AN EVENT FRAME ARE A CONTRACT. Every log stream — the events search, the span/trace
/// log lists and the live tail — used to send <c>LogEventDto</c> through the reflection-based
/// serialiser; <see cref="LogEventJsonWriter"/> now writes them straight from the
/// <see cref="LogEvent"/>. Everything downstream — the Angular client's parser, the transcoder parity
/// suite, the integration tests that read frames back — reads those bytes positionally, so
/// "equivalent JSON" is not the bar: the output has to be identical, byte for byte.
///
/// <para>The DTO road is gone from the server — the live tail was the last stream on it — so it is
/// compared against as <see cref="LegacyDtoRoad"/>, a frozen copy of the server's code, and that copy
/// is pinned in turn by golden frames spelled out below.</para>
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
            foreach (var ev in page) JsonSerializer.Serialize(w, LegacyDtoRoad.LogEventDto.From(ev), LegacyDtoRoad.Json);
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

    /// <summary>
    /// THE LIVE TAIL'S STREAM, as a client reads it. The tail wrote one DTO frame per row and sent each
    /// the moment it was written; it now hands the poll to the row writer, which coalesces the frames
    /// into few sends, sends while the source waits, and is flushed at the end of the poll. A client
    /// reads a byte stream, not sends, so the bar is the whole stream: identical, frame prefixes and
    /// blank-line terminators included.
    /// </summary>
    [Fact]
    public async Task A_live_tail_poll_sends_the_same_bytes_through_the_row_writer_as_it_did_as_dto_frames()
    {
        var page = Page();

        var before = new MemoryStream();
        using (var sse = new SseJsonWriter(before))
        {
            foreach (var ev in page)
                await sse.WriteEventAsync(LegacyDtoRoad.LogEventDto.From(ev), LegacyDtoRoad.Contract, default);
        }

        var after = new MemoryStream();
        using (var sse = new SseJsonWriter(after))
        {
            await sse.WriteLogEventsAsync(Poll(page), default);
            await sse.FlushFramesAsync(default);
            Assert.Equal(page.Count, sse.RowsWritten);
        }

        Assert.Equal(Encoding.UTF8.GetString(before.ToArray()), Encoding.UTF8.GetString(after.ToArray()));
    }

    /// <summary>
    /// <see cref="LegacyDtoRoad"/> is a COPY, so the contract is also pinned without it: three frames
    /// spelled out byte for byte, taken from that road — the minimal row, the everything-present row
    /// with its three-deep exception and raw msgpack properties, and the already-materialised
    /// dictionary. An edit that moved the copy, or the writer, fails here even if both moved together.
    /// </summary>
    [Fact]
    public async Task Golden_frames_pin_the_contract_independently_of_the_reference_copy()
    {
        var page = Page();

        var body = new MemoryStream();
        using (var sse = new SseJsonWriter(body))
        {
            foreach (int i in GoldenRows) await sse.WriteLogEventAsync(page[i], default);
            await sse.FlushFramesAsync(default);
        }

        Assert.Equal(GoldenFrames, Encoding.UTF8.GetString(body.ToArray()));
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

    // ── the golden frames ─────────────────────────────────────────────────────

    private static readonly int[] GoldenRows = [0, 1, 4];

    private static string Frame(string json) => "data: " + json + "\n\n";

    private static readonly string GoldenFrames =
        Frame("""{"@t":"2026-09-11T12:00:00.0000000\u002B00:00","@mt":"plain","@l":"Information","id":"1"}""") +
        Frame("""{"@t":"2026-09-11T12:00:00.1234567\u002B05:00","@mt":"Handled {Command} \u0022quoted\u0022  \u00FCn\u00EFc\u00F8d\u00E9 \uD83D\uDE00","@l":"Error","@x":{"type":"System.InvalidOperationException","message":"outer \u0022boom\u0022\n\ttab","stack":"   at A.B() in /src/A.cs:line 1\n   at C.D()","inner":{"type":"System.NullReferenceException","inner":{"type":"System.Exception"}}},"id":"18446744073709551615","@tr":"0123456789abcdeffedcba9876543210","@sp":"00000000000000ff","service.name":"Office.API","props":{"SourceContext":"Common.MediatR.LoggingBehavior","Unicode":"\u043A\u043B\u044E\u0447 \uD83D\uDE00 \u0022q\u0022","Nil":null,"Number":-1.25,"Flag":false,"Response":{"$type":"MergeCreateCommand","Items":[{"Name":"Resource0"},{"Name":"Resource1"},{"Name":"Resource2"}]}}}""") +
        Frame("""{"@t":"2026-05-05T05:05:05.0000000\u002B00:00","@mt":"dictionary props","@l":"Warning","id":"8201","props":{"str":"str\u00E4ng \uD835\uDD4F","nul":null,"i64":9223372036854775807,"i32":-17,"u64":18446744073709551615,"dbl":1.5,"flt":2.5,"bool":true,"arr":[1,null,"x",{"k":"v"}],"nested":{"deep":[true,3]}}}""");

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
            MessageTemplate = "Handled {Command} \"quoted\"  ünïcødé 😀",
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
        JsonSerializer.Serialize(w, LegacyDtoRoad.LogEventDto.From(ev), LegacyDtoRoad.Json);
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

    /// <summary>
    /// The page as a tail's poll hands it over: a real wait before every other row, so the row writer
    /// sends some frames while the source waits and coalesces the rest.
    /// </summary>
    private static async IAsyncEnumerable<LogEvent> Poll(IReadOnlyList<LogEvent> page)
    {
        for (int i = 0; i < page.Count; i++)
        {
            if (i % 2 == 1) await Task.Yield();
            yield return page[i];
        }
    }
}
