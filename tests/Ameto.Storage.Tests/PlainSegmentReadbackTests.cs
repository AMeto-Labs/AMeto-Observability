using MessagePack;
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

        // ASSERTED, NOT PRINTED — issue #67. The count was written to the output and compared with
        // nothing, so any regression in this path left the test green; the only assertion was that
        // the segment was not flagged as damaged.
        //
        // AND THE EXPECTED NUMBER IS EVERY SPAN, NOT ZERO, which is the part worth reading twice.
        // An AttrHint is a block-SKIPPING hint — "a necessary attribute condition ... used to skip
        // storage blocks via their attribute blooms" — and not a per-span filter; a bloom is
        // one-sided and can only ever rule a block out. These spans carry no attributes, so the
        // attribute bloom is absent, and an absent index section means NO INFORMATION and must
        // answer "might match" (Ameto.Indexing.Tests.EmptyIndexSemanticsTests, whose whole subject
        // is that a match-nothing answer would make the segment permanently invisible to filtered
        // queries). So nothing is skipped and all 20 000 come back.
        //
        // Asserting 0 here would encode exactly the behaviour that convention forbids, and would
        // turn this test into a guard for the bug rather than against it.
        _out.WriteLine($"bloom-filtered matches: {found}");
        Assert.Equal(20_000, found);

        var page = await e.GetTraceListAsync(
            Base.AddMinutes(-5), Base.AddDays(7), null, null, null, null, null, 100);
        Assert.False(page.Unreadable);
    }

    /// <summary>A span carrying exactly one attribute, in the wire form the engine reads back.</summary>
    private static void WriteWithAttr(TraceStorageEngine e, ulong id, long startNano, string dbSystem) =>
        e.WriteSpan(new SpanIngestItem
        {
            TraceId = new TraceId(0, id), SpanId = new SpanId(id), ParentSpanId = default,
            StartTimeUnixNano = startNano, DurationNanos = 2 * Ms,
            Name = "SELECT orders", ServiceName = "billing",
            Kind = SpanKind.Client, Status = SpanStatusCode.Ok,
            AttributesBytes = MessagePackSerializer.Serialize(
                new Dictionary<string, object?> { ["db.system"] = dbSystem }),
        });

    [Fact]
    public async Task A_bloom_hint_never_loses_a_span_that_carries_the_attribute()
    {
        // THE PROPERTY A SKIPPING HINT ACTUALLY HAS, and the one worth defending: it may return
        // spans that do not match, never fewer than the ones that do. A bloom is one-sided, so the
        // only way this path can be wrong is a FALSE NEGATIVE — a block ruled out that held a
        // matching span — and that failure is silent, which is what makes it worth a test.
        //
        // Counting is not enough to see it: today nothing is skipped, so a count assertion passes
        // whatever the bloom decides. So the tagged spans are identified and every one of them has
        // to come back. A regression that starts rejecting blocks fails here by name.
        string dir = Path.Combine(_root, "mixedbloom");
        Directory.CreateDirectory(dir);

        const int total = 20_000;
        const int withAttr = 137;      // not a round number, and not a fraction of the block size

        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        var expected = new HashSet<ulong>(withAttr);
        for (int k = 0; k < total; k++)
        {
            ulong id = 200_000 + (ulong)k;

            // Spread across the file rather than bunched at the front, so surviving depends on
            // every block being consulted and not just the first one.
            if (expected.Count < withAttr && k % 140 == 0)
            {
                WriteWithAttr(e, id, _baseNano + k * Ms, "mssql");
                expected.Add(id);
            }
            else Write(e, id, _baseNano + k * Ms);
        }
        Assert.Equal(withAttr, expected.Count);   // the fixture built what it says it built
        e.FlushHotTier();

        var missing = new HashSet<ulong>(expected);
        int found = 0;
        await foreach (var s in e.SearchSpansAsync(
            from: Base.AddMinutes(-5), to: Base.AddDays(7),
            attrHints: [new AttrHint("db.system", "mssql")], limit: total))
        {
            found++;
            missing.Remove(s.SpanId.RawValue);
        }

        _out.WriteLine($"{withAttr} of {total:N0} spans carry db.system=mssql; the hinted search "
                     + $"returned {found} span(s) and lost {missing.Count} of the {withAttr}");
        Assert.Empty(missing);

        // The other direction, stated so the test cannot be "fixed" by making the hint match
        // everything unconditionally: it is allowed to over-return, but it is answering about THIS
        // segment, so it can never hand back more than the segment holds.
        Assert.InRange(found, withAttr, total);
    }
}
