using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// The index cache is keyed by (path, group), and a path CAN come back with different bytes: a
/// replicated <c>{node}-{id}</c> segment re-imported after retention unlinked the first one, from
/// a peer wiped and reinstalled under the same NodeId, or from two nodes sharing one. A memo is
/// only right about the bytes it was taught from — its bucket postings are ordinals of that file,
/// its "absent" answers and bloom verdicts are about that file — so one that answers for the new
/// bytes drops rows the new file holds and names rows it does not. A decoded group of the old
/// cache was evicted within seconds; a memo of a few KB can outlive a file by
/// <c>IndexCacheIdleEvict</c>, or for ever with it off.
///
/// <para>So an entry carries the fingerprint of the bytes it learned from — the segment's id,
/// node and size, and the group's directory record (its section offsets, row counts and time
/// bounds) — and a query that opens the same key over different bytes gets a fresh memo.</para>
/// </summary>
public sealed class IndexCacheStaleFileTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "ameto-stalememo-" + Guid.NewGuid().ToString("N"));
    private readonly List<StorageEngine> _engines = [];

    public void Dispose()
    {
        foreach (var e in _engines)
            try { e.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { }
        QuerySegmentFixtures.DeleteDataDirectory(_root);
    }

    /// <summary>Only the one segment it is handed — the file a test swaps under a fixed path.</summary>
    private sealed class OneSegment(StorageEngine hot) : ISegmentProvider
    {
        public SegmentInfo? Current;
        public IReadOnlyList<SegmentInfo> GetSegments(DateTimeOffset? from, DateTimeOffset? to) =>
            Current is null ? [] : [Current];
        public IHotTierReader OpenHotTierReader() => hot.OpenHotTierReader();
    }

    [Fact]
    public async Task A_file_replaced_under_the_same_path_is_not_answered_from_the_old_memo()
    {
        // Two different segments, written by the real flush.
        var a = await WriteSegmentAsync("a", events: 300, customer: i => "cust-" + (i % 10), template: "order {k} shipped to {Customer}");
        var b = await WriteSegmentAsync("b", events: 211, customer: i => i % 17 == 0 ? "only-b" : "cust-" + (i * 3 % 11),
                                        template: "refund {k} issued to {Customer}");

        // One fixed path the provider serves, whichever bytes are behind it.
        string fixedDir = Path.Combine(_root, "served");
        Directory.CreateDirectory(fixedDir);
        string path = Path.Combine(fixedDir, "0-1-segment.seg");

        var provider = new OneSegment(_engines[0]);
        var cache    = new SegmentIndexCache(64 * 1024 * 1024);
        var cached   = new QueryExecutor(provider, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance, cache);
        var plain    = new QueryExecutor(provider, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);

        string[] filters = ["Customer = 'cust-3'", "Customer = 'only-b'", "contains(@mt, 'refund')", "contains(@mt, 'shipped')"];

        // Teach the cache file A.
        File.Copy(a.FilePath, path);
        provider.Current = At(a, path);
        foreach (var f in filters)
        {
            Assert.Equal(await KeysAsync(plain, f), await KeysAsync(cached, f));
            Assert.Equal(await KeysAsync(plain, f), await KeysAsync(cached, f));   // and from the memo
        }
        Assert.True(cache.HitCount > 0);

        // Retention unlinks it; a replica with the same name and different bytes arrives.
        File.Delete(path);
        File.Copy(b.FilePath, path);
        provider.Current = At(b, path);

        foreach (var f in filters)
        {
            var expected = await KeysAsync(plain, f);
            Assert.Equal(expected, await KeysAsync(cached, f));
        }
        // The rows the old memo would have dropped are really there.
        Assert.NotEmpty(await KeysAsync(cached, "Customer = 'only-b'"));
        Assert.NotEmpty(await KeysAsync(cached, "contains(@mt, 'refund')"));
    }

    // ── Fixture ───────────────────────────────────────────────────────────────

    private async Task<SegmentInfo> WriteSegmentAsync(string name, int events, Func<int, string> customer, string template)
    {
        string dir  = Path.Combine(_root, name);
        Directory.CreateDirectory(dir);
        var opts    = new ServerOptions { DataDirectory = dir };
        var engine  = new StorageEngine(Options.Create(opts), new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
                                        NullLogger<StorageEngine>.Instance, Timeout.InfiniteTimeSpan);
        _engines.Add(engine);
        engine.IndexSinkFactory = static (estimatedEventCount, termsPerEvent) =>
            new SegmentIndexBuilder(estimatedEventCount, 5, termsPerEvent);

        long baseTicks = new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero).UtcTicks;
        var buf = new ArrayBufferWriter<byte>(128);
        for (int i = 0; i < events; i++)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(2);
            w.Write("k");        w.Write((long)i);
            w.Write("Customer"); w.Write(customer(i));
            w.Flush();
            Assert.True(engine.TryWrite(new LogEventHeader
            {
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerSecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = engine.TemplatePool.Intern(template),
                ServiceNamePoolIndex     = engine.TemplatePool.Intern("Svc"),
            }, buf.WrittenSpan));
        }
        await engine.FlushHotTierAsync();
        return Assert.Single(engine.ListSegments());
    }

    private static SegmentInfo At(SegmentInfo s, string path) => new()
    {
        Id = s.Id, NodeId = s.NodeId, FilePath = path,
        MinTimestampTicks = s.MinTimestampTicks, MaxTimestampTicks = s.MaxTimestampTicks,
        EventCount = s.EventCount, MinLevel = s.MinLevel,
        CompressedBytes = s.CompressedBytes, UncompressedBytes = s.UncompressedBytes,
    };

    private static async Task<List<long>> KeysAsync(QueryExecutor q, string filter)
    {
        var keys = new List<long>();
        await foreach (var ev in q.ExecuteAsync(new QueryRequest { Filter = filter, Count = 10_000, Direction = QueryDirection.Forward }))
            keys.Add((long)ev.Properties!["k"]!);
        return keys;
    }
}
