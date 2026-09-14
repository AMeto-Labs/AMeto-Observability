using System.Buffers;
using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query;
using Ameto.Query.Filtering;
using Ameto.Storage;
using MessagePack;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// What <c>select count(*) group by ['service.name']</c> costs, on the two roads it can take.
///
/// <para>old — the ordered k-way merge: every event in the window decoded into a LogEvent, its
/// properties copied, its exception rebuilt, then three columns read off it and thrown away;<br/>
/// new — the header scan behind <c>/api/events/counts</c>: timestamp, level and service pool
/// index, in parallel across segments, never materialising an event at all.</para>
///
/// <para>The corpus is deliberately fat — a realistic property map per event — because that is
/// the part the header road does not pay for, and a probe over empty events would report a
/// difference several times smaller than the one a real window shows.</para>
/// </summary>
public sealed class AggregationPathProbe
{
    private const int Events = 60_000;

    private static readonly DateTimeOffset Base = new(2026, 8, 24, 9, 0, 0, TimeSpan.Zero);
    private static readonly string[] Services = ["checkout", "billing", "gateway", "Office.API", "identity"];

    private readonly ITestOutputHelper _out;
    public AggregationPathProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void HeaderScanBeatsTheOrderedEventScan()
    {
        string dir = Path.Combine(Path.GetTempPath(), "ameto-aggpath-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var opts = new ServerOptions { DataDirectory = dir };
        var engine = new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        try
        {
            engine.IndexSinkFactory = static (c, t) => new SegmentIndexBuilder(c, 5, t);
            Fill(engine);

            var query    = new QueryExecutor(engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);
            var scanOnly = new AggregationExecutor(query);
            var withHdr  = new AggregationExecutor(query, headerScan: engine);

            Assert.True(AggregationParser.TryParse("select count(*) group by ['service.name']", out var q));
            var from = Base.AddMinutes(-1);
            var to   = Base.AddDays(2);

            // Same table out of both, or the comparison means nothing.
            var a = scanOnly.ExecuteAsync(q!, from, to).GetAwaiter().GetResult();
            var b = withHdr.ExecuteAsync(q!, from, to).GetAwaiter().GetResult();
            Assert.Equal(a.Rows.Count, b.Rows.Count);
            for (int i = 0; i < a.Rows.Count; i++)
            {
                Assert.Equal(a.Rows[i].Key[0],    b.Rows[i].Key[0]);
                Assert.Equal(a.Rows[i].Values[0], b.Rows[i].Values[0]);
            }
            Assert.Equal((double)Events, a.Rows.Sum(r => r.Values[0] ?? 0));

            var (scanMs, scanBytes) = Measure(3, () => scanOnly.ExecuteAsync(q!, from, to).GetAwaiter().GetResult());
            var (hdrMs,  hdrBytes)  = Measure(3, () => withHdr.ExecuteAsync(q!, from, to).GetAwaiter().GetResult());

            _out.WriteLine($"{Events:N0} events, {a.Rows.Count} services, both tiers");
            _out.WriteLine($"ordered event scan : {scanMs,8:F1} ms | {scanBytes / (1024.0 * 1024.0),7:F1} MB allocated | {scanBytes / (double)Events,6:F0} B/event");
            _out.WriteLine($"parallel header scan: {hdrMs,8:F1} ms | {hdrBytes / (1024.0 * 1024.0),7:F1} MB allocated | {hdrBytes / (double)Events,6:F0} B/event");
            _out.WriteLine($"gain                : {scanMs / Math.Max(hdrMs, 0.001),4:F1}x faster, {scanBytes / (double)Math.Max(hdrBytes, 1),4:F0}x less allocated");

            // Allocation is the deterministic half and the one worth gating: the event scan
            // builds a LogEvent plus a property copy per event and the header scan builds
            // neither, so the ratio is structural rather than a property of this machine.
            Assert.True(hdrBytes * 5 < scanBytes,
                $"header scan should allocate far less: scan={scanBytes} B, header={hdrBytes} B");

            // The road the header scan CANNOT take — a user property as the group key — so the
            // per-event cost of group-key building is visible on its own terms rather than
            // hidden behind the shortcut.
            Assert.True(AggregationParser.TryParse("select count(*) group by ['ApplicationContext']", out var userKey));
            var userRun = withHdr.ExecuteAsync(userKey!, from, to).GetAwaiter().GetResult();
            Assert.Equal((double)Events, Assert.Single(userRun.Rows).Values[0]);

            var (keyMs, keyBytes) = Measure(3, () => withHdr.ExecuteAsync(userKey!, from, to).GetAwaiter().GetResult());
            _out.WriteLine($"scan road, user-property key: {keyMs,8:F1} ms | {keyBytes / (1024.0 * 1024.0),7:F1} MB allocated | {keyBytes / (double)Events,6:F0} B/event");
        }
        finally
        {
            try { engine.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { }
            try { Directory.Delete(dir, true); } catch { }
        }
    }

    private static void Fill(StorageEngine engine)
    {
        var buf = new ArrayBufferWriter<byte>(512);
        for (int i = 0; i < Events; i++)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(4);
            w.Write("SourceContext");      w.Write("Common.MediatR.LoggingBehavior");
            w.Write("ApplicationContext"); w.Write("Office.API");
            w.Write("RequestPath");        w.Write("/api/v1/orders/" + i);
            w.Write("Elapsed");            w.Write((double)(i % 500));
            w.Flush();

            engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = Base.UtcTicks + i * (TimeSpan.TicksPerSecond / 10),
                Level                    = (Ameto.Core.LogLevel)(i % 6),
                MessageTemplatePoolIndex = engine.TemplatePool.Intern("Command {0} handled; Response: {@1}"),
                ServiceNamePoolIndex     = engine.TemplatePool.Intern(Services[i % Services.Length]),
            }, buf.WrittenSpan.ToArray());

            // Two flushes, so the corpus spans several cold segments AND the hot tier — which
            // is what both roads actually meet in production.
            if (i == Events / 3 || i == 2 * Events / 3)
                engine.FlushHotTierAsync().GetAwaiter().GetResult();
        }
    }

    private static (double MsPerIter, long Bytes) Measure(int iters, Action body)
    {
        body();                       // warm the caches both roads share
        GC.Collect();
        GC.WaitForPendingFinalizers();
        long b0 = GC.GetTotalAllocatedBytes(precise: true);
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < iters; i++) body();
        sw.Stop();
        return (sw.Elapsed.TotalMilliseconds / iters,
                (GC.GetTotalAllocatedBytes(precise: true) - b0) / iters);
    }
}
