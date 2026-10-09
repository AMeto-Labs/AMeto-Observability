using System.Diagnostics;
using Ameto.Core;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE STAND, COMPACTED. What one compaction run does to full-size flushes on the 512 MB container's
/// own budget — the production picture behind this change: 2.6k flush-sized trace segments that the
/// materialising merge never touched, because a pair of full flushes is twice the pass budget.
///
/// <para>The sources are what the stand's tier flushes: ~37 600 ordinary eight-attribute SqlClient
/// spans each, ten spans a trace, four services, back to back in time. Printed per mode: segments
/// before and after, passes, wall time, the merge's live-set peak (sampled after its blocks) and the
/// bytes on disk. Release figures are the ones to quote; Debug runs a third of the flushes.</para>
/// </summary>
public sealed class TraceStandCompactionProbe : IDisposable
{
    private const long MB = 1024 * 1024;
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly string[] Services = ["gateway", "billing", "ledger", "auth"];

#if DEBUG
    private const int Flushes = 4;
#else
    private const int Flushes = 12;
#endif

    private readonly string            _root = Path.Combine(Path.GetTempPath(), "ameto-tstand-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;

    public TraceStandCompactionProbe(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_root);
    }

    public void Dispose()
    {
        SpanWriter._afterMergedBlockForTest = null;
        try { Directory.Delete(_root, true); } catch { }
    }

    private static MemoryBudgets Stand => MemoryBudgets.Derive(managedLimitBytes: 384 * MB, physicalLimitBytes: 512 * MB);

    /// <summary>A full stand tier, in ordinary spans — the count its byte budget flushes at.</summary>
    private static int FullTier =>
        (int)(new TracesOptions().HotTierMaxBytesFor(Stand) / TraceStorageEngine.HotSpanBytes(375));

    private void WriteFlushes(string dir)
    {
        long t = Base.ToUnixTimeMilliseconds() * Ms;
        ulong id = 1;
        int tier = FullTier;
        for (int f = 0; f < Flushes; f++)
        {
            var spans = new List<SpanRecord>(tier);
            for (int i = 0; i < tier; i++, id++)
            {
                int k = i % 10;
                ulong trace = id / 10 + 1;
                spans.Add(new SpanRecord
                {
                    TraceId           = new TraceId(0x57A9D000UL, trace),
                    SpanId            = new SpanId(id),
                    ParentSpanId      = k == 0 ? default : new SpanId(id - (ulong)k),
                    StartTimeUnixNano = t,
                    DurationNanos     = (1 + i % 400) * Ms,
                    Name              = k == 0 ? "POST /api/pay" : "SELECT payments",
                    ServiceName       = Services[k % Services.Length],
                    Kind              = k == 0 ? SpanKind.Server : SpanKind.Client,
                    Status            = i % 97 == 0 ? SpanStatusCode.Error : SpanStatusCode.Ok,
                    Attributes        = ColdSpanSegmentFixture.SqlClientAttributes(i),
                });
                t += 6 * Ms;    // ~37 600 spans in ~4 minutes: a busy install's flush cadence
            }
            SpanWriter.Write(dir, spans);
        }
    }

    private static long DiskBytes(string dir) =>
        Directory.EnumerateFiles(dir, "spans-*").Where(static f => !f.EndsWith(".wal", StringComparison.Ordinal))
                 .Sum(static f => new FileInfo(f).Length);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void One_run_over_full_stand_flushes(bool streaming)
    {
        long budget = new TracesOptions().MergeBudgetBytesFor(Stand);
        string dir = Path.Combine(_root, streaming ? "streamed" : "materialised");
        Directory.CreateDirectory(dir);
        WriteFlushes(dir);
        long diskBefore = DiskBytes(dir);

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance,
            options: new TracesOptions { StreamingCompaction = streaming, MergeBudgetBytes = budget, CompactionMinAge = TimeSpan.Zero });
        e.LoadColdSegments();
        int before = e.ColdSegmentCountForTest;

        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        long baseline = GC.GetTotalMemory(forceFullCollection: true);
        long peak = 0;
        SpanWriter._afterMergedBlockForTest = written =>
        {
            if (written % (8 * 4096) != 0) return;
            peak = Math.Max(peak, GC.GetTotalMemory(forceFullCollection: true) - baseline);
        };

        long cpuBefore = (long)Process.GetCurrentProcess().TotalProcessorTime.TotalMilliseconds;
        var sw = Stopwatch.StartNew();
        try { e.CompactSmallSegments(); }
        finally { SpanWriter._afterMergedBlockForTest = null; }
        sw.Stop();
        long cpu = (long)Process.GetCurrentProcess().TotalProcessorTime.TotalMilliseconds - cpuBefore;

        var after = e.ColdSegmentsForTest;
        long spans = after.Sum(static s => (long)s.SpanCount);
        _out.WriteLine($"STAND COMPACTION ({(streaming ? "streaming" : "materialising")}), pass budget {budget / (double)MB:F1} MB, "
                     + $"{Flushes} full flushes of {FullTier:N0} spans");
        _out.WriteLine($"  segments   {before} -> {after.Length}  ({string.Join(", ", after.Select(static s => s.SpanCount.ToString("N0")))})");
        _out.WriteLine($"  passes     {e.LastCompactionPassesForTest}");
        _out.WriteLine($"  wall       {sw.Elapsed.TotalSeconds:F2} s, cpu {cpu / 1000.0:F2} s, {sw.Elapsed.TotalMicroseconds / Math.Max(1, spans):F2} us/span");
        _out.WriteLine($"  live peak  {peak / (double)MB:F1} MB (sampled every 8 blocks written)");
        _out.WriteLine($"  disk       {diskBefore / (double)MB:F1} MB -> {DiskBytes(dir) / (double)MB:F1} MB");

        Assert.Equal((long)Flushes * FullTier, spans);
        if (streaming)
        {
            Assert.True(after.Length * 3 <= before + 2, $"{before} full flushes left {after.Length} segments");
            Assert.True(peak < budget, $"the merge held {peak / (double)MB:F1} MB, past its {budget / (double)MB:F1} MB budget");
        }
        else
        {
            Assert.Equal(before, after.Length);                  // the materialising merge leaves them be
        }
    }
}
