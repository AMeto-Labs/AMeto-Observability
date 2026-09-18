using System.Diagnostics;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT ONE SPAN COSTS TO INGEST, AND WHAT THE HOT TIER THEN HOLDS — the gate for the traces half
/// of issue #83, because the 512 MB stand did not die of CPU. It died of a `List&lt;SpanRecord&gt;`
/// weighing 1 117 bytes per span at a 50 000-span flush threshold, doubled for the whole of every
/// flush by the detached snapshot the readers keep seeing, plus 199 MB for a compaction pass built
/// out of the same records.
///
/// <para>RETAINED, NOT ALLOCATED, is the number this file exists for. Allocation is gen0 churn and
/// the collector deals with it; what killed the container is the live set, so the measurement is
/// <c>GC.GetTotalMemory(forceFullCollection: true)</c> across an ingest whose engine is still
/// alive and holding everything it took in.</para>
///
/// <para>Two span shapes, because they bracket the cost: an attribute-less span is the floor a
/// <c>SpanRecord</c> cannot go below, and the eight-attribute SqlClient span is what an
/// instrumented service actually emits. The distance between them IS the attribute map.</para>
///
/// <para>Printed, not asserted, except for one bound: a span in the tier must stay well under the
/// kilobyte that the dictionary cost. A printed figure that drifts is information; an asserted one
/// that drifts is a machine-dependent test.</para>
/// </summary>
public sealed class TraceHotTierProbe : IDisposable
{
    /// <summary>Just under <c>HotFlushThreshold</c> (50 000), so the tier is never flushed out from under the measurement.</summary>
    private const int Spans        = 49_000;
    private const int SpansPerTrace = 10;

    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly List<string> _dirs = [];
    private readonly ITestOutputHelper _out;

    public TraceHotTierProbe(ITestOutputHelper output) => _out = output;

    public void Dispose()
    {
        foreach (var d in _dirs)
            try { Directory.Delete(d, true); } catch { }
    }

    [Fact]
    public void Ingest_cost_and_retained_bytes_per_span()
    {
        // Warm: JIT the engine, the WAL and the msgpack paths so the first measured run is not
        // paying for all three.
        Measure(attributes: true, spans: 2_000, label: null);

        var zero = Measure(attributes: false, spans: Spans, label: "0 attrs");
        var eight = Measure(attributes: true, spans: Spans, label: "8 attrs");

        _out.WriteLine("");
        _out.WriteLine($"attribute map costs {eight.RetainedPerSpan - zero.RetainedPerSpan:N0} B/span retained, "
                     + $"{eight.MicrosPerSpan - zero.MicrosPerSpan:N2} us/span");
        _out.WriteLine($"at the 50 000-span flush threshold the tier is "
                     + $"{eight.RetainedPerSpan * 50_000 / 1048576.0:N0} MB, and twice that across a flush");

        // THE GATE. main retained 1 117 B/span for this shape; the dictionary alone was 987 B of
        // it. A tier that holds the blob has no dictionary, no eight key strings and no eight
        // boxes, so it cannot come near that number — if this trips, the ingest path has started
        // decoding attributes again.
        Assert.True(eight.RetainedPerSpan < 700,
            $"an eight-attribute span retains {eight.RetainedPerSpan:N0} B in the hot tier — the "
            + "attribute map is being decoded on the ingest path again");
    }

    private readonly record struct Result(double MicrosPerSpan, long AllocPerSpan, long RetainedPerSpan);

    private Result Measure(bool attributes, int spans, string? label)
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-hotprobe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);

        // THE LIVE-SET BASELINE IS TAKEN BEFORE THE CORPUS EXISTS, and that is not fussiness. The
        // attribute blob the corpus hands over is the SAME ARRAY the tier ends up holding, so a
        // baseline taken after the corpus was built charges the tier nothing for it: the corpus
        // being dropped at the end frees the SpanIngestItem wrappers and nothing else, and the
        // probe reports 36 B/span for a span carrying 375 B of attributes. Measuring from before
        // means the delta is everything ingest made live, blobs included.
        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        long liveBefore = GC.GetTotalMemory(forceFullCollection: true);

        // The whole corpus is built and handed over BEFORE the clock starts: the mapper's work is
        // the ingest lens's (WP1), not this one's, and leaving it inside would put a byte[] per
        // span on this probe's allocation figure.
        var items = new SpanIngestItem[spans];
        long baseNano = Base.ToUnixTimeMilliseconds() * 1_000_000L;
        for (int i = 0; i < spans; i++)
            items[i] = new SpanIngestItem
            {
                TraceId           = new TraceId(0x9E3779B97F4A7C15UL, (ulong)(i / SpansPerTrace + 1)),
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = i % SpansPerTrace == 0 ? default : new SpanId((ulong)(i / SpansPerTrace * SpansPerTrace + 1)),
                StartTimeUnixNano = baseNano + i * 1_000_000L,
                DurationNanos     = 1_000_000L * (1 + i % 2000),
                Name              = "SELECT payments",
                ServiceName       = "billing",
                Kind              = SpanKind.Client,
                Status            = SpanStatusCode.Unset,
                HttpStatusCode    = 0,
                AttributesBytes   = attributes ? SqlClientBlob(i) : [],
            };

        var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);

        long allocBefore = GC.GetTotalAllocatedBytes(precise: true);
        int  g2Before    = GC.CollectionCount(2);

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < spans; i++) engine.WriteSpan(items[i]);
        sw.Stop();

        long allocated = GC.GetTotalAllocatedBytes(precise: true) - allocBefore;
        int  g2        = GC.CollectionCount(2) - g2Before;

        // The corpus is as heavy as the tier; drop it before sampling the live set or it is
        // counted twice.
        Array.Clear(items);
        long retained = GC.GetTotalMemory(forceFullCollection: true) - liveBefore;
        GC.KeepAlive(engine);

        long walBytes = Directory.EnumerateFiles(dir, "*.wal").Sum(static f => new FileInfo(f).Length);

        var r = new Result(sw.Elapsed.TotalMicroseconds / spans, allocated / spans, retained / spans);

        if (label is not null)
        {
            _out.WriteLine("");
            _out.WriteLine($"HOT TIER  {spans:N0} spans through TraceStorageEngine.WriteSpan   [{label}]");
            _out.WriteLine($"  wall        {sw.Elapsed.TotalMilliseconds,10:N1} ms   {r.MicrosPerSpan,8:N2} us/span");
            _out.WriteLine($"  allocated   {allocated / 1048576.0,10:N1} MB   {r.AllocPerSpan,8:N0} B/span");
            _out.WriteLine($"  RETAINED    {retained / 1048576.0,10:N1} MB   {r.RetainedPerSpan,8:N0} B/span");
            _out.WriteLine($"  at 50 000   {r.RetainedPerSpan * 50_000 / 1048576.0,10:N0} MB   (+ the same again while a flush builds)");
            _out.WriteLine($"  g2 during ingest {g2}");
            _out.WriteLine($"  WAL file    {walBytes / 1048576.0,10:N1} MB   {walBytes / spans,8:N0} B/span");
        }

        engine.Dispose();
        return r;
    }

    /// <summary>
    /// The msgpack map an OpenTelemetry SqlClient instrumentation emits — the same eight
    /// attributes <c>ColdSpanSegmentFixture</c> uses, written the way <c>OtlpTraceMapper</c> writes
    /// them, so the blob is the 375-byte one every figure in the recon report is quoted against.
    /// </summary>
    internal static byte[] SqlClientBlob(int i)
    {
        var buf = new System.Buffers.ArrayBufferWriter<byte>(512);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(8);
        w.Write("db.system");         w.Write("mssql");
        w.Write("db.name");           w.Write("payments");
        w.Write("db.statement");      w.Write("SELECT TOP 100 Id, TenantId, Amount, CreatedUtc FROM dbo.Payments "
                                            + "WHERE TenantId = @p0 AND CreatedUtc >= @p1 ORDER BY CreatedUtc DESC");
        w.Write("net.peer.name");     w.Write("sql-prod-03.svc.cluster.local");
        w.Write("net.peer.port");     w.Write(1433L);
        w.Write("http.route");        w.Write("/api/v1/tenants/{tenantId}/payments");
        w.Write("thread.id");         w.Write((long)(i % 64));
        w.Write("otel.library.name"); w.Write("OpenTelemetry.Instrumentation.SqlClient");
        w.Flush();
        return buf.WrittenMemory.ToArray();
    }
}
