using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// A HEALTHY SEGMENT READS BACK IN FULL — asserted, because a bounds check that had never been
/// pointed at one refused two ordinary shapes of file and the whole suite stayed green.
///
/// <para>The bound in question divided the bytes remaining in the file by the larger of an entry's
/// on-disk size and its in-memory size. Bytes remaining are bytes ON DISK, so dividing by a bigger
/// heap figure made the limit tighter than the format allows — four times tighter for a
/// <c>HashSet&lt;uint&gt;</c>. Measured against the previous commit, a 20 000-span segment with no
/// attributes threw where the parent returned all 20 000 rows, as did one of exactly
/// <c>HotFlushThreshold</c> spans, and the throw was content-shaped so it classified as PERMANENT
/// DAMAGE over data that was completely intact.</para>
///
/// <para>Every other fixture in this suite writes attributes, and attributes inflate the per-block
/// bloom index that the remaining-bytes figure is measured from — which is exactly why 430 tests
/// could not see it. So these fixtures write NO attributes on purpose, and they exist to be the
/// boring case nothing else covers: read the file back and get everything.</para>
/// </summary>
public sealed class PlainSegmentReadbackTests : IDisposable
{
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string _root = Path.Combine(Path.GetTempPath(), "ameto-plainseg-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public PlainSegmentReadbackTests(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_root);
    }

    public void Dispose() { try { Directory.Delete(_root, true); } catch { } }

    /// <summary>A span with NO attributes — the shape the broken bound refused.</summary>
    private static void Write(TraceStorageEngine e, ulong id, long startNano) =>
        e.WriteSpan(new SpanIngestItem
        {
            TraceId = new TraceId(0, id), SpanId = new SpanId(id), ParentSpanId = default,
            StartTimeUnixNano = startNano, DurationNanos = 2 * Ms,
            Name = "GET /orders", ServiceName = "billing",
            Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
            AttributesBytes = [],
        });

    [Theory]
    [InlineData(20_000)]      // several blocks, and the size the reviewer measured
    [InlineData(50_000)]      // exactly HotFlushThreshold: an ordinary flushed segment
    public async Task A_segment_with_no_attributes_reads_back_every_span(int spans)
    {
        string dir = Path.Combine(_root, "plain" + spans);
        Directory.CreateDirectory(dir);

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        for (int k = 0; k < spans; k++) Write(e, 1 + (ulong)k, _baseNano + k * Ms);
        e.FlushHotTier();

        var seg = Assert.Single(e.ColdSegmentsForTest);
        _out.WriteLine($"{spans:N0} spans, {new FileInfo(seg.FilePath).Length:N0} B on disk");

        // The service filter is the path that reads the service index, which is where the bound
        // that refused these files lived.
        int found = 0;
        await foreach (var _ in e.SearchSpansAsync(
            from: Base.AddMinutes(-5), to: Base.AddDays(7), serviceName: "billing", limit: spans))
            found++;

        Assert.Equal(spans, found);

        // And the list path, which reads the summary sidecar's own counts.
        var page = await e.GetTraceListAsync(
            Base.AddMinutes(-5), Base.AddDays(7), "billing", null, null, null, null, spans);
        Assert.Equal(spans, page.Rows.Count);
        Assert.False(page.Unreadable, "a completely healthy segment was reported as damaged");
    }

    [Fact]
    public async Task A_plain_segment_survives_a_TraceQL_query_that_uses_the_bloom_index()
    {
        // The bloom path has the widest gap between an entry's four bytes on disk and its sixteen in
        // a HashSet, so it is where a heap-sized bound bites hardest — and a file with no attributes
        // has the smallest bloom index to measure against.
        string dir = Path.Combine(_root, "plainbloom");
        Directory.CreateDirectory(dir);

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        for (int k = 0; k < 20_000; k++) Write(e, 100_000 + (ulong)k, _baseNano + k * Ms);
        e.FlushHotTier();

        int found = 0;
        await foreach (var _ in e.SearchSpansAsync(
            from: Base.AddMinutes(-5), to: Base.AddDays(7),
            attrHints: [new AttrHint("db.system", "mssql")], limit: 20_000))
            found++;

        // Nothing matches — these spans carry no attributes at all — but the READ must succeed, and
        // the segment must not be marked damaged for having a small index.
        _out.WriteLine($"bloom-filtered matches: {found}");
        var page = await e.GetTraceListAsync(
            Base.AddMinutes(-5), Base.AddDays(7), null, null, null, null, null, 100);
        Assert.False(page.Unreadable);
    }
}
