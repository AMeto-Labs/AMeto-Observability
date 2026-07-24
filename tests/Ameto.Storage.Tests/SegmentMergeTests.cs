using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// Small-segment merge: many tiny flush segments collapse into one large segment
/// through the regular flush pipeline, preserving every event byte-for-byte
/// (ids, timestamps, levels, templates, services, raw property payloads,
/// exceptions, trace correlation), with crash-safe manifest recovery.
/// </summary>
public sealed class SegmentMergeTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-merge-" + Guid.NewGuid().ToString("N"));
    private StorageEngine _engine = null!;

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _engine = NewEngine();
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    private StorageEngine NewEngine() => new(
        Options.Create(new ServerOptions { DataDirectory = _dir }),
        new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
        NullLogger<StorageEngine>.Instance);

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static byte[] Props(int i)
    {
        var buf = new ArrayBufferWriter<byte>(64);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("n");   w.Write((long)i);
        w.Write("key"); w.Write("wallet:" + i);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>Writes <paramref name="count"/> events and flushes them into one small segment.</summary>
    private async Task WriteSegmentAsync(int round, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int n = round * 1000 + i;
            var header = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)(round * 100_000 + i)).RawValue,
                TimestampUtcTicks        = DateTime.UtcNow.Ticks + n * TimeSpan.TicksPerMillisecond,
                Level                    = (LogLevel)(n % 6),
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {n} round " + round % 3),
                ServiceNamePoolIndex     = _engine.TemplatePool.Intern("Svc." + round % 4),
                TraceIdHi                = (ulong)(n + 1),
                TraceIdLo                = (ulong)(n + 2),
                SpanId                   = (ulong)(n + 3),
            };
            var exc = n % 50 == 0
                ? new ExceptionInfo { Type = "System.InvalidOperationException", Message = "boom " + n }
                : null;
            Assert.True(_engine.TryWrite(header, Props(n), exception: exc));
        }
        await _engine.FlushHotTierAsync();
    }

    private List<RawSegmentEvent> ReadEverything()
    {
        var dedup = new Dictionary<string, string>(StringComparer.Ordinal);
        var all = new List<RawSegmentEvent>();
        foreach (var seg in _engine.ListSegments())
        {
            using var r = SegmentReader.Open(seg.FilePath);
            all.AddRange(r.ReadAllRaw(dedup));
        }
        return all.OrderBy(e => e.Id).ToList();
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Merge_CollapsesSmallSegments_Losslessly()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 120);

        Assert.Equal(10, _engine.ListSegments().Count);
        var before = ReadEverything();
        Assert.Equal(1200, before.Count);

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));

        var segs = _engine.ListSegments();
        Assert.Single(segs);
        Assert.Equal(1200u, segs[0].EventCount);
        Assert.Single(Directory.GetFiles(_dir, "segments/*.seg", SearchOption.TopDirectoryOnly)
            .Concat(Directory.GetFiles(Path.Combine(_dir, "segments"), "*.seg")).Distinct());
        Assert.Empty(Directory.GetFiles(Path.Combine(_dir, "segments"), "*.mergemanifest"));

        var after = ReadEverything();
        Assert.Equal(before.Count, after.Count);
        for (int i = 0; i < before.Count; i++)
        {
            Assert.Equal(before[i].Id,        after[i].Id);
            Assert.Equal(before[i].TsTicks,   after[i].TsTicks);
            Assert.Equal(before[i].Level,     after[i].Level);
            Assert.Equal(before[i].Template,  after[i].Template);
            Assert.Equal(before[i].Service,   after[i].Service);
            Assert.Equal(before[i].TraceIdHi, after[i].TraceIdHi);
            Assert.Equal(before[i].TraceIdLo, after[i].TraceIdLo);
            Assert.Equal(before[i].SpanId,    after[i].SpanId);
            Assert.Equal(before[i].Props ?? [], after[i].Props ?? []);
            Assert.Equal(before[i].Exception?.Type,    after[i].Exception?.Type);
            Assert.Equal(before[i].Exception?.Message, after[i].Exception?.Message);
        }
    }

    [Fact]
    public async Task Merge_RefusesTinyBatches()
    {
        for (int round = 0; round < 3; round++) // below MergeMinSources
            await WriteSegmentAsync(round, 50);
        Assert.False(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        Assert.Equal(3, _engine.ListSegments().Count);
    }

    [Fact]
    public async Task InterruptedMerge_IsFinishedOnRestart()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        var mergedPath = _engine.ListSegments().Single().FilePath;

        // Simulate a crash between publishing the merged segment and deleting a
        // source: a stale source file + a manifest naming it.
        var segDir = Path.Combine(_dir, "segments");
        var stale  = Path.Combine(segDir, "9-999-1-2.seg");
        File.WriteAllBytes(stale, [1, 2, 3]);
        File.WriteAllLines(mergedPath + ".mergemanifest", [Path.GetFileName(stale)]);

        await _engine.DisposeAsync();
        _engine = NewEngine();
        await WaitForCatalogAsync();

        Assert.False(File.Exists(stale));                                   // recovery deleted the duplicate
        Assert.Empty(Directory.GetFiles(segDir, "*.mergemanifest"));        // manifest consumed
        Assert.Contains(_engine.ListSegments(), s => s.FilePath == mergedPath); // merged data intact
    }

    /// <summary>
    /// A source held open by an in-flight query cannot be deleted on Windows —
    /// the manifest must survive so the recovery sweep finishes the deletion,
    /// otherwise the file resurrects as duplicate events after a restart.
    /// </summary>
    [Fact]
    public async Task Merge_KeepsManifest_WhileSourceIsHeldOpen()
    {
        for (int round = 0; round < 10; round++)
            await WriteSegmentAsync(round, 60);

        var segDir = Path.Combine(_dir, "segments");
        var victim = _engine.ListSegments().OrderBy(s => s.MinTimestampTicks).First();

        using (var hold = new FileStream(victim.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
            Assert.True(File.Exists(victim.FilePath));                       // delete blocked by the open handle
            Assert.Single(Directory.GetFiles(segDir, "*.mergemanifest"));    // manifest kept for recovery
        }

        // Handle released → restart recovery finishes the interrupted deletion.
        await _engine.DisposeAsync();
        _engine = NewEngine();
        await WaitForCatalogAsync();

        Assert.False(File.Exists(victim.FilePath));
        Assert.Empty(Directory.GetFiles(segDir, "*.mergemanifest"));
        Assert.Equal(600, ReadEverything().Count); // no duplicates, nothing lost
    }

    /// <summary>The catalog loads in the background — poll until segments appear.</summary>
    private async Task WaitForCatalogAsync()
    {
        for (int i = 0; i < 100 && _engine.ListSegments().Count == 0; i++)
            await Task.Delay(50);
    }
}
