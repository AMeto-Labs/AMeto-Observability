using System.Diagnostics;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT A COMPACTION PASS HOLDS LIVE, which is the largest single number in the traces half of
/// issue #83 and the one that has nothing to do with ingest at all.
///
/// <para><c>CompactOnePass</c> materialises up to <c>MaxSpansPerPass</c> = 120 000
/// <see cref="SpanRecord"/>s into one list and hands them to <c>SpanWriter</c>. At main a record
/// read back out of a segment weighed 1 740 B — the attribute <c>Dictionary</c>, its eight key
/// strings, its eight boxed values — so the pass peaked at 199 MB of spans alone, on a container
/// with a 384 MB heap limit, for a merge that copies the attributes straight back out again
/// without looking at one of them.</para>
///
/// <para>The measurement is <c>SpanReader.ReadAll</c> of one 50 000-span segment, held live, and
/// then multiplied out — the same arithmetic the compaction does, on the same span shape, so the
/// extrapolation is a multiplication and not a guess.</para>
/// </summary>
public sealed class TraceCompactionMemoryProbe : IClassFixture<ColdSpanSegmentFixture>
{
    /// <summary><c>TraceStorageEngine.MaxSpansPerPass</c> — 2 × <c>CompactionThreshold</c>.</summary>
    private const int MaxSpansPerPass = 120_000;

    private readonly ColdSpanSegmentFixture _fx;
    private readonly ITestOutputHelper      _out;

    public TraceCompactionMemoryProbe(ColdSpanSegmentFixture fx, ITestOutputHelper output)
    {
        _fx  = fx;
        _out = output;
    }

    [Fact]
    public void Retained_bytes_of_one_materialised_segment()
    {
        // Warm: ArrayPool block buffers and the JIT, so neither is charged to the measurement.
        _ = SpanReader.ReadAll(_fx.SegmentPath).Count;

        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        long liveBefore  = GC.GetTotalMemory(forceFullCollection: true);
        long allocBefore = GC.GetTotalAllocatedBytes(precise: true);

        var sw    = Stopwatch.StartNew();
        var spans = SpanReader.ReadAll(_fx.SegmentPath);
        sw.Stop();

        long allocated = GC.GetTotalAllocatedBytes(precise: true) - allocBefore;
        long retained  = GC.GetTotalMemory(forceFullCollection: true) - liveBefore;
        GC.KeepAlive(spans);

        int  count       = spans.Count;
        long perSpan     = retained / count;
        double passBytes = (double)perSpan * MaxSpansPerPass;

        _out.WriteLine($"COMPACTION  SpanReader.ReadAll of one {count:N0}-span segment");
        _out.WriteLine($"  wall        {sw.Elapsed.TotalMilliseconds,10:N1} ms   "
                     + $"{sw.Elapsed.TotalMicroseconds / count,6:N2} us/span");
        _out.WriteLine($"  allocated   {allocated / 1048576.0,10:N1} MB   {allocated / count,6:N0} B/span");
        _out.WriteLine($"  RETAINED    {retained / 1048576.0,10:N1} MB   {perSpan,6:N0} B/span");
        _out.WriteLine($"  a MaxSpansPerPass = {MaxSpansPerPass:N0} merge therefore peaks at "
                     + $"{passBytes / 1048576.0:N0} MB of spans alone");

        Assert.Equal(ColdSpanSegmentFixture.Spans, count);

        // THE GATE. main retained 1 740 B/span here, so a pass was 199 MB — over half the stand's
        // whole heap limit. A record that carries the blob is the block's bytes plus the record,
        // and nothing else.
        Assert.True(perSpan < 700,
            $"a span read out of a segment retains {perSpan:N0} B, so a compaction pass peaks at "
            + $"{passBytes / 1048576.0:N0} MB — the reader is building attribute dictionaries again");
    }
}
