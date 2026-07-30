using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// A page query must open only the segments that can actually reach the page.
///
/// <para>It used to open all of them: <c>GetSegments(null, null)</c> returns every segment
/// in the catalog, and the merge primed an iterator for each — memory-mapping the file and
/// decompressing a block — before serving 50 events off the top of the heap. On the sandbox
/// stand that is 291 opens for one page, and it is why paging cost grew with the catalog
/// (10 ms → 379 ms per page as scrolling went deeper).</para>
/// </summary>
public sealed class LazySegmentPrimingTests : IAsyncLifetime
{
    private const int Segments      = 40;
    private const int EventsPerSeg  = 25;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-lazyprime-" + Guid.NewGuid().ToString("N"));
    private StorageEngine _engine = null!;
    private QueryExecutor _query  = null!;
    private readonly ITestOutputHelper _out;

    public LazySegmentPrimingTests(ITestOutputHelper o) => _out = o;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = _dir }),
            new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        _query = new QueryExecutor(_engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);

        // Non-overlapping segments, oldest first: segment k covers minute k.
        long baseTicks = new DateTimeOffset(2026, 7, 30, 0, 0, 0, TimeSpan.Zero).UtcTicks;
        var buf = new ArrayBufferWriter<byte>(128);
        for (int s = 0; s < Segments; s++)
        {
            for (int i = 0; i < EventsPerSeg; i++)
            {
                buf.ResetWrittenCount();
                var w = new MessagePackWriter(buf);
                w.WriteMapHeader(1);
                w.Write("n"); w.Write((long)(s * EventsPerSeg + i));
                w.Flush();

                var h = new LogEventHeader
                {
                    Id                       = new EventId(0u, (uint)(s * EventsPerSeg + i)).RawValue,
                    TimestampUtcTicks        = baseTicks + s * TimeSpan.TicksPerMinute + i * TimeSpan.TicksPerSecond,
                    Level                    = Ameto.Core.LogLevel.Information,
                    MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {n}"),
                    ServiceNamePoolIndex     = _engine.TemplatePool.Intern("Svc.A"),
                };
                Assert.True(_engine.TryWrite(h, buf.WrittenSpan.ToArray()));
            }
            await _engine.FlushHotTierAsync();
        }
        Assert.Equal(Segments, _engine.ListSegments().Count);
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private async Task<List<LogEvent>> PageAsync(int count, bool forward = false)
    {
        var res = new List<LogEvent>(count);
        await foreach (var ev in _query.ExecuteAsync(new QueryRequest
        {
            Count     = count,
            Direction = forward ? QueryDirection.Forward : QueryDirection.Backward,
        }))
        {
            res.Add(ev);
            if (res.Count >= count) break;
        }
        return res;
    }

    /// <summary>
    /// A short page must equal the head of a full read — the oracle is the query itself
    /// reading everything, so the assertion does not depend on how ids are encoded.
    /// </summary>
    [Fact]
    public async Task NewestPageIsCorrectAcrossManySegments()
    {
        var all  = await PageAsync(Segments * EventsPerSeg);
        var page = await PageAsync(5);

        Assert.Equal(5, page.Count);
        Assert.Equal(all.Take(5).Select(e => e.Id.RawValue).ToArray(),
                     page.Select(e => e.Id.RawValue).ToArray());

        for (int i = 1; i < page.Count; i++)
            Assert.True(page[i].Timestamp <= page[i - 1].Timestamp, $"order broken at {i}");
    }

    [Fact]
    public async Task OldestPageIsCorrectWhenReadingForward()
    {
        var all  = await PageAsync(Segments * EventsPerSeg, forward: true);
        var page = await PageAsync(5, forward: true);

        Assert.Equal(5, page.Count);
        Assert.Equal(all.Take(5).Select(e => e.Id.RawValue).ToArray(),
                     page.Select(e => e.Id.RawValue).ToArray());

        for (int i = 1; i < page.Count; i++)
            Assert.True(page[i].Timestamp >= page[i - 1].Timestamp, $"order broken at {i}");
    }

    /// <summary>Forward and backward must be exact reverses of one another.</summary>
    [Fact]
    public async Task ForwardAndBackwardAgreeOnTheWholeSet()
    {
        var desc = await PageAsync(Segments * EventsPerSeg);
        var asc  = await PageAsync(Segments * EventsPerSeg, forward: true);

        Assert.Equal(desc.Count, asc.Count);
        Assert.Equal(desc.Select(e => e.Id.RawValue).ToArray(),
                     asc.Select(e => e.Id.RawValue).Reverse().ToArray());
    }

    [Fact]
    public async Task AFullReadStillReturnsEverything()
    {
        var all = await PageAsync(Segments * EventsPerSeg);
        Assert.Equal(Segments * EventsPerSeg, all.Count);
        Assert.Equal(all.Count, all.Select(e => e.Id.RawValue).Distinct().Count());
    }

    /// <summary>
    /// The point: a small page must not pay for the whole catalog. Priming a segment
    /// memory-maps it and decompresses a block, so allocation tracks segments opened.
    /// </summary>
    [Fact]
    public async Task SmallPageCostsFarLessThanReadingEverything()
    {
        await PageAsync(5);                       // warm
        await PageAsync(Segments * EventsPerSeg);

        long b0 = GC.GetAllocatedBytesForCurrentThread();
        await PageAsync(5);
        long small = GC.GetAllocatedBytesForCurrentThread() - b0;

        long b1 = GC.GetAllocatedBytesForCurrentThread();
        await PageAsync(Segments * EventsPerSeg);
        long full = GC.GetAllocatedBytesForCurrentThread() - b1;

        _out.WriteLine($"{Segments} segments x {EventsPerSeg} events");
        _out.WriteLine($"page of 5 : {small / 1024.0:F0} KB");
        _out.WriteLine($"full read : {full / 1024.0:F0} KB   ({(double)full / small:F1}x)");

        Assert.True(small * 5 < full,
            $"a 5-event page still costs like a full read: {small} B vs {full} B — lazy priming is not working");
    }

    /// <summary>
    /// Breaking out of a page early must still release every mmap. On Windows an open
    /// mapping blocks File.Delete, so deleting the segment files is a definitive check —
    /// and a leak here would silently block merge and retention in production.
    /// </summary>
    [Fact]
    public async Task EarlyBreakReleasesEveryMappedSegment()
    {
        await PageAsync(3);

        var paths = _engine.ListSegments().Select(s => s.FilePath).ToList();
        await _engine.DisposeAsync();

        foreach (var p in paths)
        {
            File.Delete(p);                        // throws if a mapping is still open
            Assert.False(File.Exists(p));
        }

        // Re-create so DisposeAsync in the fixture stays valid.
        _engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = _dir }),
            new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
    }
}
