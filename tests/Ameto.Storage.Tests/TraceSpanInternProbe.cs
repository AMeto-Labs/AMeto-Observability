using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// TI#5: WHAT A SPAN'S NAME AND SERVICE COST THE HOT TIER WHEN THEY ARRIVE THE WAY A PARSER HANDS
/// THEM OVER — a fresh <c>string</c> per span for the name (a route template that repeats across
/// essentially every span of a batch) and a fresh one per resource block for the service, so a tier
/// spanning ten thousand requests holds ten thousand copies of <c>"billing"</c>.
///
/// <para><c>TraceHotTierProbe</c> cannot see this: its corpus hands every span the SAME literal, so
/// the name is shared whether the engine interns it or not. Here each span gets its own instance,
/// built from chars exactly as <c>Encoding.UTF8.GetString</c> would build it, and a new service
/// instance every 200 spans (one OTLP request's worth).</para>
///
/// <para>Printed, not asserted: the live set is a process-wide figure. The gate for the behaviour is
/// in <c>TraceSpanInternTests</c>.</para>
/// </summary>
public sealed class TraceSpanInternProbe : IDisposable
{
    private const int Spans         = 49_000;
    private const int SpansPerTrace = 10;
    private const int SpansPerBlock = 200;

    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly List<string>      _dirs = [];
    private readonly ITestOutputHelper _out;

    public TraceSpanInternProbe(ITestOutputHelper output) => _out = output;

    public void Dispose()
    {
        foreach (var d in _dirs)
            try { Directory.Delete(d, true); } catch { }
    }

    [Fact]
    public void Retained_bytes_per_span_when_every_name_is_its_own_string()
    {
        Measure(2_000, label: null);           // warm
        var r = Measure(Spans, label: "parser-shaped names");

        _out.WriteLine("");
        _out.WriteLine($"at the 50 000-span threshold: {r.RetainedPerSpan * 50_000 / 1048576.0:N1} MB");
    }

    private readonly record struct Result(double MicrosPerSpan, long RetainedPerSpan);

    private Result Measure(int spans, string? label)
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-internprobe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);

        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        long liveBefore = GC.GetTotalMemory(forceFullCollection: true);

        // Built BEFORE the clock and dropped before the sample, as TraceHotTierProbe does — but the
        // blobs are shared (one per shape) so what the tier is charged is the record and its two
        // strings, not the corpus.
        byte[] blob = TraceHotTierProbe.SqlClientBlob(0);
        const string NameText = "SELECT payments";
        const string SvcText  = "billing";

        var items = new SpanIngestItem[spans];
        long baseNano = Base.ToUnixTimeMilliseconds() * 1_000_000L;
        string service = new(SvcText.AsSpan());
        for (int i = 0; i < spans; i++)
        {
            if (i % SpansPerBlock == 0) service = new string(SvcText.AsSpan());
            items[i] = new SpanIngestItem
            {
                TraceId           = new TraceId(0x9E3779B97F4A7C15UL, (ulong)(i / SpansPerTrace + 1)),
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = i % SpansPerTrace == 0 ? default : new SpanId((ulong)(i / SpansPerTrace * SpansPerTrace + 1)),
                StartTimeUnixNano = baseNano + i * 1_000_000L,
                DurationNanos     = 1_000_000L * (1 + i % 2000),
                Name              = new string(NameText.AsSpan()),   // its own instance, as a parser makes it
                ServiceName       = service,
                Kind              = SpanKind.Client,
                AttributesBytes   = blob,
            };
        }

        var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < spans; i++) engine.WriteSpan(items[i]);
        sw.Stop();

        Array.Clear(items);
        long retained = GC.GetTotalMemory(forceFullCollection: true) - liveBefore;
        GC.KeepAlive(engine);
        GC.KeepAlive(blob);

        var r = new Result(sw.Elapsed.TotalMicroseconds / spans, retained / spans);
        if (label is not null)
        {
            _out.WriteLine($"HOT TIER  {spans:N0} spans, a fresh name string per span and a fresh service per {SpansPerBlock}   [{label}]");
            _out.WriteLine($"  wall        {sw.Elapsed.TotalMilliseconds,10:N1} ms   {r.MicrosPerSpan,8:N2} us/span");
            _out.WriteLine($"  RETAINED    {retained / 1048576.0,10:N1} MB   {r.RetainedPerSpan,8:N0} B/span   (the blob is shared: record + strings)");
        }

        engine.Dispose();
        return r;
    }
}
