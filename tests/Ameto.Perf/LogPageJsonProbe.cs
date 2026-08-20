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

    /// <summary>A segment of fat events — the Office.API shape that hurts on this stand.</summary>
    private static string BuildSegment(string dir)
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
