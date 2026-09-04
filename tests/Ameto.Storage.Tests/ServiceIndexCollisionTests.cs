using Ameto.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// A SERVICE-FILTERED SEARCH MAY NOT ANSWER A CONFIDENT ZERO OUT OF A DAMAGED INDEX.
///
/// <para>Four shapes of torn service-index offset — past the end, at the end, zero, negative — are
/// loud: they throw, the classifier calls them corruption, and the page says so. The fifth shape is
/// not. A single-bit flip that lands the offset INSIDE the file reads back cleanly: a small service
/// count, a walk that matches no name, and an empty set of blocks worth opening. The caller read
/// that as "no block in this segment holds that service" and stopped — a positive claim about the
/// data, assembled from a number the file no longer agrees with.</para>
///
/// <para>Measured on a 90 827-byte segment: of the 40 single-bit flips of this field, 38 were loud
/// and 2 (offset ^16 and ^32) returned zero spans with Unreadable=False, next to an unfiltered
/// search over the same window that returned every one.</para>
///
/// <para>The index now answers null — "I cannot tell you" — and the search reads every block. On
/// healthy data this costs nothing: a segment that genuinely lacks a service never reaches here,
/// because the engine drops it one level up on SpanSegmentInfo.Services.</para>
/// </summary>
public sealed class ServiceIndexCollisionTests : IDisposable
{
    private const long Ms = 1_000_000L;
    private static readonly DateTimeOffset Base = new(2026, 8, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly string _root = Path.Combine(Path.GetTempPath(), "ameto-svcidx-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private readonly long _baseNano = Base.ToUnixTimeMilliseconds() * Ms;

    public ServiceIndexCollisionTests(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_root);
    }

    public void Dispose() { try { Directory.Delete(_root, true); } catch { } }

    private static void Write(TraceStorageEngine e, ulong id, long startNano) =>
        e.WriteSpan(new SpanIngestItem
        {
            TraceId = new TraceId(0, id), SpanId = new SpanId(id), ParentSpanId = default,
            StartTimeUnixNano = startNano, DurationNanos = 2 * Ms,
            Name = "GET /orders", ServiceName = "billing",
            Kind = SpanKind.Server, Status = SpanStatusCode.Ok,
            AttributesBytes = [],
        });

    /// <summary>The v3 footer is 28 bytes: traceIdx, svcIdx, bloomIdx, magic. svcIdx is the second.</summary>
    private const int SvcIdxFromEnd = 28 - 8;

    [Theory]
    [InlineData(32)]   // reads back clean, matches nothing, and used to answer an empty set
    public async Task An_offset_that_slips_INSIDE_the_file_does_not_become_an_empty_answer(long flip)
    {
        string dir = Path.Combine(_root, "flip" + flip);
        Directory.CreateDirectory(dir);

        int total = 100;
        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        for (int k = 0; k < total; k++) Write(e, 200_000 + (ulong)k, _baseNano + k * Ms);
        e.FlushHotTier();

        var seg = Assert.Single(e.ColdSegmentsForTest);

        // Torn AFTER the snapshot was taken, which is the real sequence: the engine read this
        // header when it flushed the file, and the flip happens on the disk underneath it. So the
        // segment is still believed to hold "billing" and the search reaches the block index —
        // the only way to observe what that index answers.
        long before;
        using (var raw = new FileStream(seg.FilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
        using (var br = new BinaryReader(raw))
        using (var bw = new BinaryWriter(raw))
        {
            raw.Seek(-SvcIdxFromEnd, SeekOrigin.End);
            before = (long)br.ReadUInt64();
            long torn = before ^ flip;

            // The premise of the test: still a legal position in the file, so nothing throws.
            Assert.True(torn > 0 && torn < raw.Length,
                $"flip {flip} left the offset outside the file — that is the loud case, not this one");

            raw.Seek(-SvcIdxFromEnd, SeekOrigin.End);
            bw.Write((ulong)torn);
            _out.WriteLine($"svcIdxOffset {before} -> {torn} (file {raw.Length} B)");
        }

        int unfiltered = 0;
        await foreach (var _ in e.SearchSpansAsync(
            from: Base.AddMinutes(-5), to: Base.AddDays(7), limit: total)) unfiltered++;

        int filtered = 0;
        await foreach (var _ in e.SearchSpansAsync(
            from: Base.AddMinutes(-5), to: Base.AddDays(7), serviceName: "billing", limit: total)) filtered++;

        _out.WriteLine($"unfiltered={unfiltered} service-filtered={filtered}");

        Assert.Equal(total, unfiltered);

        // The index is an optimisation; losing it costs speed, never rows.
        Assert.Equal(unfiltered, filtered);
    }

    [Fact]
    public async Task EVERY_single_bit_flip_of_the_offset_is_loud_or_complete_but_never_quiet()
    {
        // The reviewer's own instrument, kept: sweep all 64 single-bit flips of this one field and
        // demand the same thing of each. A torn index may be LOUD — refused by the byte-count
        // bound, thrown, classified as damage, reported — or it may be COMPLETE, having degraded
        // to reading every block. What it may not be is quiet: fewer spans than an unfiltered
        // search over the same window, with nothing raised. Two of these flips used to be exactly
        // that, and which two depends on the bytes the segment happens to contain, so pinning the
        // pair by number would pin the fixture rather than the property.
        string dir = Path.Combine(_root, "sweep");
        Directory.CreateDirectory(dir);

        const int total = 100;
        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance);
        for (int k = 0; k < total; k++) Write(e, 300_000 + (ulong)k, _baseNano + k * Ms);
        e.FlushHotTier();
        string path = Assert.Single(e.ColdSegmentsForTest).FilePath;

        long from = _baseNano - 1000 * Ms, to = _baseNano + 1000 * Ms;
        long honest;
        using (var raw = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        using (var br = new BinaryReader(raw))
        {
            raw.Seek(-SvcIdxFromEnd, SeekOrigin.End);
            honest = (long)br.ReadUInt64();
        }

        int loud = 0, complete = 0, skipped = 0;
        var quiet = new List<string>();

        for (int bit = 0; bit < 64; bit++)
        {
            long torn = honest ^ (1L << bit);
            using (var raw = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
            using (var bw = new BinaryWriter(raw))
            {
                raw.Seek(-SvcIdxFromEnd, SeekOrigin.End);
                bw.Write((ulong)torn);
            }

            int found = 0;
            bool threw = false;
            try
            {
                await foreach (var _ in SpanReader.SearchAsync(
                    path, from, to, serviceName: "billing", spanName: null, status: null,
                    httpStatusCode: null, minDurationNanos: null, maxDurationNanos: null,
                    attrHints: null, ct: CancellationToken.None)) found++;
            }
            catch (Exception ex)
            {
                threw = true;
                Assert.True(FileBounds.DescribesContent(ex),
                    $"bit {bit} reached the classifier as {ex.GetType().Name}, which reads as "
                  + "retryable rather than as damage");
            }

            if (threw) loud++;
            else if (found == total) complete++;
            else if (torn == honest) skipped++;
            else quiet.Add($"bit {bit}: offset {honest} -> {torn}, {found} of {total} spans, no exception");
        }

        // Put the honest offset back so the fixture's own teardown is not reading a torn file.
        using (var raw = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
        using (var bw = new BinaryWriter(raw))
        {
            raw.Seek(-SvcIdxFromEnd, SeekOrigin.End);
            bw.Write((ulong)honest);
        }

        _out.WriteLine($"64 single-bit flips: {loud} loud, {complete} complete, {quiet.Count} quiet");
        Assert.True(quiet.Count == 0,
            "a service-filtered search came back short and said nothing:" + Environment.NewLine
          + string.Join(Environment.NewLine, quiet));

        // The instrument has to be able to fail: if every flip were refused before it reached the
        // service index, this test would pass without exercising the contract it exists for.
        Assert.True(complete > 0, "no flip reached the degrade-to-reading-everything path");
    }
}