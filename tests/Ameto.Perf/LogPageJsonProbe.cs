using System.Buffers;
using System.Diagnostics;
using System.Text.Json;
using Ameto.Core;
using Ameto.Core.Serialization;
using Ameto.Storage;
using MessagePack;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

// xUnit1031 (blocking task operations) is off for this probe deliberately. The page is read
// synchronously so that everything after it runs on the test's own thread: the comparison below is
// GC.GetAllocatedBytesForCurrentThread, which is per-THREAD, and an async test method may resume
// its continuation on another thread-pool thread and weigh whatever that one had done. The wait is
// on a completed cold-segment read with no synchronization context, so the deadlock the rule
// guards cannot happen here.
#pragma warning disable xUnit1031

/// <summary>
/// What a page of the log list actually costs on the server: read 50 events out of a
/// cold segment and write their properties as JSON — the work behind one scroll step.
///
/// Both routes run against the same segment in one build, so the comparison is exact:
///   old — materialise LogEvent.Properties (msgpack → Dictionary of boxed values) and
///         hand it to System.Text.Json, which walks it straight back out;
///   new — write the msgpack the decoder carried through directly to the JSON writer.
/// </summary>
public sealed class LogPageJsonProbe
{
    private const int Events = 2_000;   // events in the segment
    private const int Page   = 50;      // page size the events list requests

    private static readonly JsonSerializerOptions Options = new()
    {
        Converters = { new TestDynamicObjectConverter() },
    };

    /// <summary>The server's <c>_json</c>, spelled out here because Ameto.Perf cannot see it.</summary>
    private static readonly JsonSerializerOptions DtoOptions = new()
    {
        PropertyNamingPolicy   = System.Text.Json.JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
        WriteIndented          = false,
        Converters             = { new TestDynamicObjectConverter() },
    };

    private readonly ITestOutputHelper _out;
    public LogPageJsonProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void TranscodingBeatsDictionaryRoundTrip()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-pagejson-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            string path = BuildSegment(dir);

            // Page of events, decoded once — the JSON write is what is being compared.
            async Task<List<LogEvent>> Page50Async()
            {
                using var reader = SegmentReader.Open(path);
                var list = new List<LogEvent>(Page);
                await foreach (var ev in reader.ReadEventsAsync(null, null, null, reversed: false, default))
                {
                    list.Add(ev);
                    if (list.Count >= Page) break;
                }
                return list;
            }

            var page = Page50Async().GetAwaiter().GetResult();
            Assert.Equal(Page, page.Count);
            Assert.All(page, e => Assert.False(e.RawProperties.IsEmpty));

            var buf = new ArrayBufferWriter<byte>(1 << 20);

            // Work from the raw bytes on BOTH sides, decoding afresh every iteration.
            // Going through ev.Properties would measure the wrong thing: it caches the
            // materialised dictionary, so after the first pass the decode — the very cost
            // being removed — would not be in the loop at all.
            var raws = page.Select(e => e.RawProperties).ToArray();

            // One array around the page: a Utf8JsonWriter takes a single root value, and
            // the brackets cost the same on both sides.
            void ViaDictionary()
            {
                buf.ResetWrittenCount();
                using var w = new Utf8JsonWriter(buf);
                w.WriteStartArray();
                foreach (var raw in raws)
                {
                    var map = LogEventSerializer.DeserializePropertiesMap(raw.Span);
                    JsonSerializer.Serialize(w, (object)map!, Options);
                }
                w.WriteEndArray();
            }

            void ViaTranscoder()
            {
                buf.ResetWrittenCount();
                using var w = new Utf8JsonWriter(buf);
                w.WriteStartArray();
                foreach (var raw in raws)
                    MsgPackJsonTranscoder.WriteMap(w, raw);
                w.WriteEndArray();
            }

            // Same bytes out, or the comparison is meaningless.
            ViaDictionary(); string a = System.Text.Encoding.UTF8.GetString(buf.WrittenSpan);
            ViaTranscoder(); string b = System.Text.Encoding.UTF8.GetString(buf.WrittenSpan);
            Assert.Equal(a, b);

            for (int i = 0; i < 20; i++) { ViaDictionary(); ViaTranscoder(); }

            var (dictMs, dictBytes) = Measure(200, ViaDictionary);
            var (tranMs, tranBytes) = Measure(200, ViaTranscoder);

            _out.WriteLine($"page={Page} events, {a.Length / 1024.0:F0} KB of JSON");
            _out.WriteLine($"dictionary round trip : {dictMs * 1000:F0} us/page | {dictBytes / 1024.0:F0} KB allocated");
            _out.WriteLine($"direct transcode      : {tranMs * 1000:F0} us/page | {tranBytes / 1024.0:F0} KB allocated");
            _out.WriteLine($"gain                  : {dictMs / tranMs:F1}x faster, {(double)dictBytes / Math.Max(tranBytes, 1):F0}x less allocated");

            // The win being guarded is the ALLOCATION: a page used to leave ~700 KB of
            // dictionaries, boxes and strings behind, which is what put GC polling at the top
            // of the scroll profile. GC.GetAllocatedBytesForCurrentThread is deterministic —
            // a busy machine does not change it — so this gate is stable wherever it runs,
            // and the 50x margin is orders of magnitude clear of the real ~700x.
            Assert.True(tranBytes * 50 < dictBytes,
                $"transcoder should allocate ~nothing: dict={dictBytes} B, transcode={tranBytes} B");

            // The wall-clock comparison is REPORTED above, not asserted. It used to be
            // `Assert.True(tranMs < dictMs)` — a strict inequality between two timings taken
            // back to back, with no margin at all, guarding a difference the line above calls
            // modest by construction (the transcoder decodes msgpack inline instead of walking
            // pre-decoded values; the JSON writing itself is the same work either way). On an
            // idle machine it passed; run after five other test projects it went red, which is
            // how it behaved for us: 3/3 alone, 53/53 for the whole Perf suite alone, red only
            // in the back-to-back sweep. A gate that reports the machine's load rather than the
            // code's behaviour teaches everyone to re-run until green, which costs more than
            // the regression it was meant to catch. The ratio stays in the output, so a real
            // slowdown is still visible to anyone reading a failing or passing run.
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { }
        }
    }

    /// <summary>
    /// The other half of what a scroll step costs: DECODING the page out of the segment,
    /// before a byte of JSON is written. Reported per event, because that is the number every
    /// change to <c>DecodeColumnarBlock</c> moves — string transcodes, the properties copy and
    /// (until it went lazy) a whole <c>ExceptionInfo</c> tree, per candidate row.
    /// </summary>
    [Fact]
    public async Task PageDecodeCostsAreReportedPerEvent()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-pagedecode-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            string path = BuildSegment(dir);

            async Task<int> PageAsync(int take)
            {
                using var reader = SegmentReader.Open(path);
                int n = 0;
                await foreach (var ev in reader.ReadEventsAsync(null, null, null))
                {
                    _ = ev.MessageTemplate; _ = ev.ServiceName; _ = ev.RawProperties.Length;
                    if (++n >= take) break;
                }
                return n;
            }

            await PageAsync(Page);   // warm

            long b0 = GC.GetAllocatedBytesForCurrentThread();
            int got = await PageAsync(Page);
            long pageBytes = GC.GetAllocatedBytesForCurrentThread() - b0;
            Assert.Equal(Page, got);

            long b1 = GC.GetAllocatedBytesForCurrentThread();
            await PageAsync(Events);
            long allBytes = GC.GetAllocatedBytesForCurrentThread() - b1;

            _out.WriteLine($"decode page of {Page} : {pageBytes / 1024.0:F0} KB ({pageBytes / (double)Page:F0} B/event)");
            _out.WriteLine($"decode all {Events}   : {allBytes / 1024.0:F0} KB ({allBytes / (double)Events:F0} B/event)");

            // A page must not cost the segment — the report above is the number that moves,
            // this is only the floor under it.
            Assert.True(pageBytes * 4 < allBytes,
                $"a {Page}-event page costs like the whole segment: {pageBytes} B vs {allBytes} B");
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    /// <summary>
    /// The WHOLE row, not just its properties: what one page of the events SSE stream costs
    /// to serialise, end to end.
    ///
    /// <para>old — <c>LogEventDto.From(ev)</c> plus reflection-based
    /// <c>JsonSerializer.Serialize</c>: a DTO, an ExceptionInfoDto tree, and four strings per
    /// row (<c>Timestamp.ToString("O")</c>, <c>Id.ToString()</c>, two interpolated hex ids)
    /// that exist only to be copied into the output and dropped;<br/>
    /// new — <c>LogEventJsonWriter</c> formatting straight into the output writer's buffer.</para>
    ///
    /// <para>The DTO below is a LOCAL MIRROR of the one the server used to send, because Ameto.Perf
    /// does not reference the integration tests, where the frozen copy of it lives. That the two
    /// roads emit the same bytes is not asserted here — it is pinned against that copy, and golden
    /// frames, by <c>Ameto.Integration.Tests.LogEventJsonParityTests</c>; this probe only weighs
    /// them.</para>
    /// </summary>
    [Fact]
    public void DirectEventWriterBeatsDtoReflection()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-eventjson-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            string path = BuildSegment(dir);

            async Task<List<LogEvent>> Page50Async()
            {
                using var reader = SegmentReader.Open(path);
                var list = new List<LogEvent>(Page);
                await foreach (var ev in reader.ReadEventsAsync(null, null, null, reversed: false, default))
                {
                    list.Add(ev);
                    if (list.Count >= Page) break;
                }
                return list;
            }

            // Half the page carries an exception, so the DTO's tree copy is in the measurement
            // the way it is in a real Error page.
            var page = Page50Async().GetAwaiter().GetResult();
            var rows = new List<LogEvent>(page.Count);
            for (int i = 0; i < page.Count; i++)
            {
                var e = page[i];
                rows.Add(new LogEvent
                {
                    Id              = e.Id,
                    Timestamp       = e.Timestamp,
                    Level           = e.Level,
                    MessageTemplate = e.MessageTemplate,
                    ServiceName     = e.ServiceName,
                    RawProperties   = e.RawProperties,
                    TraceIdHi       = 0x0123456789abcdefUL,
                    TraceIdLo       = (ulong)(i + 1),
                    SpanId          = (ulong)(i + 1),
                    Exception       = (i % 2) == 0 ? null : new ExceptionInfo
                    {
                        Type       = "System.InvalidOperationException",
                        Message    = "the handler refused the command",
                        StackTrace = "   at Common.MediatR.LoggingBehavior.Handle()\n   at Office.API.Controller.Post()",
                        Inner      = new ExceptionInfo { Type = "System.TimeoutException", Message = "timed out" },
                    },
                });
            }

            var buf = new ArrayBufferWriter<byte>(1 << 20);

            void ViaDto()
            {
                buf.ResetWrittenCount();
                using var w = new Utf8JsonWriter(buf);
                w.WriteStartArray();
                foreach (var ev in rows)
                    JsonSerializer.Serialize(w, ProbeLogEventDto.From(ev), DtoOptions);
                w.WriteEndArray();
            }

            void ViaDirect()
            {
                buf.ResetWrittenCount();
                using var w = new Utf8JsonWriter(buf);
                w.WriteStartArray();
                foreach (var ev in rows)
                    LogEventJsonWriter.Write(w, ev);
                w.WriteEndArray();
            }

            ViaDto();    string a = System.Text.Encoding.UTF8.GetString(buf.WrittenSpan);
            ViaDirect(); string b = System.Text.Encoding.UTF8.GetString(buf.WrittenSpan);
            Assert.Equal(a, b);

            for (int i = 0; i < 20; i++) { ViaDto(); ViaDirect(); }

            var (dtoMs, dtoBytes) = Measure(200, ViaDto);
            var (dirMs, dirBytes) = Measure(200, ViaDirect);

            _out.WriteLine($"page={Page} rows, {a.Length / 1024.0:F0} KB of JSON, half with an exception tree");
            _out.WriteLine($"DTO + reflection STJ : {dtoMs * 1000:F0} us/page | {dtoBytes / 1024.0:F1} KB allocated | {dtoBytes / (double)Page:F0} B/row");
            _out.WriteLine($"direct writer        : {dirMs * 1000:F0} us/page | {dirBytes / 1024.0:F1} KB allocated | {dirBytes / (double)Page:F0} B/row");
            _out.WriteLine($"gain                 : {dtoMs / dirMs:F1}x faster, {(double)dtoBytes / Math.Max(dirBytes, 1):F0}x less allocated");

            // The DTO road allocates ~600-900 B per row and the direct one allocates nothing
            // per row at all (the writer's own buffer is amortised across the page). A 10x
            // margin is orders of magnitude clear of that and does not depend on machine load.
            Assert.True(dirBytes * 10 < dtoBytes,
                $"direct writer should allocate ~nothing: dto={dtoBytes} B, direct={dirBytes} B");
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { }
        }
    }

    private static (double MsPerIter, long Bytes) Measure(int iters, Action body)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        long b0 = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < iters; i++) body();
        sw.Stop();
        return (sw.Elapsed.TotalMilliseconds / iters,
                (GC.GetAllocatedBytesForCurrentThread() - b0) / iters);
    }

    /// <summary>
    /// A segment of fat events — the Office.API shape that hurts on this stand. Internal so the
    /// live-tail frame probe weighs the same rows.
    /// </summary>
    internal static string BuildSegment(string dir)
    {
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(Events + 1, (long)Events * 4096 + (16L << 20));

        int tmplIdx = pool.Intern("----- Command {0} handled; Response: {@1}");
        string tmpl = pool.Get(tmplIdx);
        int svcIdx  = pool.Intern("Office.API");
        long baseTicks = DateTimeOffset.UtcNow.UtcTicks;

        var buf = new ArrayBufferWriter<byte>(4096);
        for (int i = 0; i < Events; i++)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(4);
            w.Write("SourceContext");      w.Write("Common.MediatR.LoggingBehavior");
            w.Write("ApplicationContext"); w.Write("Office.API");
            w.Write("Environment");        w.Write("Test");
            w.Write("1");
            w.WriteMapHeader(3);
            w.Write("$type");   w.Write("MergeCreateCommand");
            w.Write("AppName"); w.Write("KioskAgent");
            w.Write("Permissions");
            w.WriteArrayHeader(30);
            for (int k = 0; k < 30; k++)
            {
                w.WriteMapHeader(2);
                w.Write("PermissionName"); w.Write($"Resource{k}.Action");
                w.Write("DisplayName");    w.Write($"Do action on resource {k}");
            }
            w.Flush();

            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i,
                Level                    = Ameto.Core.LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
            };
            Assert.True(hot.TryWrite(h, buf.WrittenSpan, tmpl));
        }
        hot.Freeze();

        string path = Path.Combine(dir, "0-1-0-0.seg");
        var order = SegmentWriter.ComputeSortOrder(hot);
        using (var sw = new SegmentWriter(path))
        {
            sw.WriteEvents(hot, pool, order);
            sw.Finalise(new NodeId(0), new SegmentId(1UL));
        }
        return path;
    }
}

// ── local mirror of the server's SSE DTO ──────────────────────────────────────
//
// Ameto.Perf does not reference the integration tests, so the road being weighed is rebuilt
// here, field for field and attribute for attribute. It is a MEASUREMENT fixture only (this
// probe and LiveTailFrameProbe): whether the server's old DTO and LogEventJsonWriter agree on
// the bytes is settled in Ameto.Integration.Tests.LogEventJsonParityTests, against the frozen
// copy of it (LegacyDtoRoad) and golden frames.

internal sealed class ProbeLogEventDto
{
    [System.Text.Json.Serialization.JsonPropertyName("@t")]           public string Timestamp       { get; init; } = "";
    [System.Text.Json.Serialization.JsonPropertyName("@mt")]          public string MessageTemplate { get; init; } = "";
    [System.Text.Json.Serialization.JsonPropertyName("@l")]           public string Level           { get; init; } = "";
    [System.Text.Json.Serialization.JsonPropertyName("@x")]           public ProbeExceptionDto? Exception { get; init; }
    [System.Text.Json.Serialization.JsonPropertyName("id")]           public string Id              { get; init; } = "";
    [System.Text.Json.Serialization.JsonPropertyName("@tr")]          public string? TraceId        { get; init; }
    [System.Text.Json.Serialization.JsonPropertyName("@sp")]          public string? SpanId         { get; init; }
    [System.Text.Json.Serialization.JsonPropertyName("service.name")] public string? ServiceName    { get; init; }
    [System.Text.Json.Serialization.JsonPropertyName("props")]        public ProbeEventProps? Properties { get; init; }

    public static ProbeLogEventDto From(LogEvent ev) => new()
    {
        Timestamp       = ev.Timestamp.ToString("O"),
        MessageTemplate = ev.MessageTemplate,
        Level           = ev.Level.ToSeqString(),
        Exception       = ProbeExceptionDto.From(ev.Exception),
        Id              = ev.Id.RawValue.ToString(),
        TraceId         = TraceIdHelper.FormatTraceId(ev.TraceIdHi, ev.TraceIdLo),
        SpanId          = TraceIdHelper.FormatSpanId(ev.SpanId),
        ServiceName     = ev.ServiceName,
        Properties      = !ev.RawProperties.IsEmpty ? new ProbeEventProps(ev.RawProperties)
                        : ev.Properties is { } map  ? new ProbeEventProps(map)
                        : null,
    };
}

[System.Text.Json.Serialization.JsonConverter(typeof(ProbeEventPropsConverter))]
internal readonly struct ProbeEventProps
{
    public readonly ReadOnlyMemory<byte>         Raw;
    public readonly Dictionary<string, object?>? Map;

    public ProbeEventProps(ReadOnlyMemory<byte> raw)        { Raw = raw;     Map = null; }
    public ProbeEventProps(Dictionary<string, object?> map) { Raw = default; Map = map;  }
}

internal sealed class ProbeEventPropsConverter : System.Text.Json.Serialization.JsonConverter<ProbeEventProps>
{
    public override ProbeEventProps Read(ref Utf8JsonReader reader, Type t, JsonSerializerOptions o)
        => throw new NotSupportedException();

    public override void Write(Utf8JsonWriter writer, ProbeEventProps value, JsonSerializerOptions options)
    {
        if (!value.Raw.IsEmpty) { MsgPackJsonTranscoder.WriteMap(writer, value.Raw); return; }
        if (value.Map is { } map) { JsonSerializer.Serialize(writer, (object)map, options); return; }
        writer.WriteStartObject();
        writer.WriteEndObject();
    }
}

internal sealed class ProbeExceptionDto
{
    [System.Text.Json.Serialization.JsonPropertyName("type")]    public string  Type       { get; init; } = "";
    [System.Text.Json.Serialization.JsonPropertyName("message")] public string? Message    { get; init; }
    [System.Text.Json.Serialization.JsonPropertyName("stack")]   public string? StackTrace { get; init; }
    [System.Text.Json.Serialization.JsonPropertyName("inner")]   public ProbeExceptionDto? Inner { get; init; }

    public static ProbeExceptionDto? From(ExceptionInfo? src)
        => src is null ? null : new ProbeExceptionDto
        {
            Type       = src.Type,
            Message    = src.Message,
            StackTrace = src.StackTrace,
            Inner      = From(src.Inner),
        };
}
