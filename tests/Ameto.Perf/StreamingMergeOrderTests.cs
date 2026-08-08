using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query;
using Ameto.Storage;
using Ameto.Storage.Tests;   // MergeBucketGrid, compiled in from the merge suite (see .csproj)
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Two things the merge suite could not see, both of which would destroy data silently
/// because a merge DELETES ITS SOURCES.
///
/// <para>THE HEAP'S TIE-BREAK. Every other merge test writes strictly increasing timestamps
/// (<c>base + n × 1 ms</c>), so <c>MergingSegmentEventSource.Less</c> never reaches its second
/// comparison and the equal-timestamp branch was untested. Real segments tie constantly:
/// a burst of events inside one tick, a batch import, an OTLP push that stamps a whole span
/// tree with one timestamp.</para>
///
/// <para>THE INDEX SINK. <c>SegmentMergeTests</c>, <c>StreamingMergeCrashSafetyTests</c> and
/// <c>LevelSplitFlushTests</c> all set <c>_allowIndexlessMerge</c>, so no test merged with a
/// sink wired — yet a production merge always has one, and a merged file is the one file large
/// enough to span several index groups. A wrong group ordinal there loses rows at QUERY time,
/// where nothing counts them.</para>
/// </summary>
public sealed class StreamingMergeOrderTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mergeorder-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;
    private StorageEngine _engine = null!;

    public StreamingMergeOrderTests(ITestOutputHelper o) => _out = o;

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _engine = new StorageEngine(
            Options.Create(new ServerOptions { DataDirectory = _dir }),
            new RetentionStore(new ServerOptions { DataDirectory = _dir }, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance);
        // Small enough that a few thousand ordinary events span several groups — the shape a
        // day-scale merged file has by default, reached here without writing a day of events.
        _engine._groupPayloadBudgetBytes = 256 * 1024;
        _engine.IndexSinkFactory = (estimatedEventCount, termsPerEvent) =>
            new SegmentIndexBuilder(estimatedEventCount, 5, termsPerEvent);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        try { Directory.Delete(_dir, true); } catch { }
    }

    // ── The index sink, on the merge path ─────────────────────────────────────

    /// <summary>
    /// A merged file built through the REAL wiring — <c>SegmentIndexBuilder</c> per group,
    /// several groups — must serve every row through the index. Posting offsets are file
    /// ordinals while the sections are per group, so an off-by-one at a group boundary would
    /// return the wrong rows for one value and nothing at all for another; both look like a
    /// query miss rather than corruption.
    /// </summary>
    [Fact]
    public async Task AMergedFileServesEveryRowThroughItsGroupIndexes()
    {
        // On the grid, not at an offset from the wall clock. The ten rounds cover 8100 s of event
        // time, and a bucket boundary landing inside that window splits them across two buckets —
        // the older sealed, the newer holding now and therefore open at a fanout of 8. Neither
        // side then consumes all ten and ListSegments().Single() throws. Ten sources is not what
        // saves it: the straddle is the failure mode, and it is reachable on ~1.3 % of instants.
        long old  = MergeBucketGrid.SealedBucketStart(LogLevel.Information);
        int  tmpl = _engine.TemplatePool.Intern("order {OrderId} processed");
        int  svc  = _engine.TemplatePool.Intern("Svc.Orders");

        const int Rounds = 10, PerRound = 900;
        for (int r = 0; r < Rounds; r++)
        {
            for (int i = 0; i < PerRound; i++)
            {
                int n = r * PerRound + i;
                Assert.True(_engine.TryWrite(new LogEventHeader
                {
                    TimestampUtcTicks        = old + n * TimeSpan.TicksPerSecond,
                    Level                    = LogLevel.Information,
                    MessageTemplatePoolIndex = tmpl,
                    ServiceNamePoolIndex     = svc,
                }, Props(n)));
            }
            await _engine.FlushHotTierAsync();
        }
        Assert.Equal(Rounds, _engine.ListSegments().Count);

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        var merged = _engine.ListSegments().Single();
        Assert.Equal((uint)(Rounds * PerRound), merged.EventCount);

        using (var reader = SegmentReader.Open(merged.FilePath))
        {
            _out.WriteLine($"merged {Rounds * PerRound} events into {reader.Groups.Length} index groups");
            Assert.True(reader.Groups.Length >= 3, $"only {reader.Groups.Length} group(s) — this proves nothing");
        }

        var query = new QueryExecutor(_engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);

        Assert.Equal(Rounds * PerRound, (await RunAsync(query, null, Rounds * PerRound + 10)).Count);

        // A unique value from every part of the file: each lives in exactly one group, so this
        // walks the group boundaries rather than the middle of one section.
        for (int n = 0; n < Rounds * PerRound; n += 137)
            Assert.True((await RunAsync(query, $"OrderId = 'order-{n}'", 10)).Count == 1,
                        $"order-{n} is unreachable through the merged file's index");

        // …and a predicate that spans every group.
        var spread = await RunAsync(query, "Customer = 'cust-7'", Rounds * PerRound);
        Assert.Equal(Enumerable.Range(0, Rounds * PerRound).Count(i => i % 40 == 7), spread.Count);
    }

    /// <summary>
    /// The exception column is COPIED through a merge as raw bytes rather than decoded and
    /// re-encoded, so the index build is now the only thing that ever turns it back into an
    /// object graph. That split has to hold on both sides: the merged file must still serve
    /// <c>@x.type</c> from its index, and the payload it stores must still decode to the same
    /// exception a reader hands the query layer.
    ///
    /// <para>Level-split flush makes this the normal shape for Error, not an edge case — an
    /// Error segment is 100 % exceptions and compaction is what gathers them.</para>
    /// </summary>
    [Fact]
    public async Task AMergedFileStillIndexesExceptionsItCopiedThroughAsBytes()
    {
        // Error's own bucket, on the grid — see the sibling test above for why the offset-from-now
        // idiom straddles. Error shares Information's 90-day TTL and so its 7-day width, but the
        // anchor is taken for the level actually written rather than assumed equal to it.
        long old  = MergeBucketGrid.SealedBucketStart(LogLevel.Error);
        int  tmpl = _engine.TemplatePool.Intern("settlement failed for {OrderId}");
        int  svc  = _engine.TemplatePool.Intern("Svc.Payments");

        const int Rounds = 10, PerRound = 900;
        for (int r = 0; r < Rounds; r++)
        {
            for (int i = 0; i < PerRound; i++)
            {
                int n = r * PerRound + i;
                // Two exception types, so a query can select a known subset rather than "all".
                var exc = new ExceptionInfo
                {
                    Type       = n % 3 == 0 ? "System.TimeoutException" : "System.InvalidOperationException",
                    Message    = n % 3 == 0 ? "settlementbarrier not cleared" : "order could not be settled",
                    StackTrace = "   at Ameto.Payments.Settle(Int32 n) in /src/Settle.cs:line " + (100 + n % 50),
                    Inner      = new ExceptionInfo { Type = "System.Net.Sockets.SocketException", Message = "reset" },
                };
                Assert.True(_engine.TryWrite(new LogEventHeader
                {
                    TimestampUtcTicks        = old + n * TimeSpan.TicksPerSecond,
                    Level                    = LogLevel.Error,
                    MessageTemplatePoolIndex = tmpl,
                    ServiceNamePoolIndex     = svc,
                }, Props(n), exception: exc));
            }
            await _engine.FlushHotTierAsync();
        }

        // The same query against the UNMERGED flush segments, so the number the merged file has
        // to reproduce is measured on this build rather than assumed. Free text, because it is
        // routed through the TRIGRAM index the builder fills from the exception's own strings —
        // which is precisely the state that would go missing if the merge stopped handing the
        // sink a decodable exception.
        var query    = new QueryExecutor(_engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance);
        int expected = await CountAsync(query, "settlementbarrier", Rounds * PerRound);
        Assert.Equal(Enumerable.Range(0, Rounds * PerRound).Count(n => n % 3 == 0), expected);

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        var merged = _engine.ListSegments().Single();
        Assert.Equal((uint)(Rounds * PerRound), merged.EventCount);

        // The payload survived the copy: it still decodes, whole, inner exception included.
        using (var reader = SegmentReader.Open(merged.FilePath))
        {
            Assert.True(reader.Groups.Length >= 3, $"only {reader.Groups.Length} group(s) — this proves nothing");
            var rows = reader.ReadAllRaw(new Dictionary<string, string>(StringComparer.Ordinal));
            Assert.Equal(Rounds * PerRound, rows.Count);
            Assert.All(rows, r => Assert.NotNull(r.Exception));
            Assert.Equal("System.Net.Sockets.SocketException", rows[0].Exception!.Inner!.Type);
        }

        // …and the index built from those same bytes still resolves the term, to the same rows.
        Assert.Equal(expected, await CountAsync(query, "settlementbarrier", Rounds * PerRound));
    }

    // ── The heap's tie-break ──────────────────────────────────────────────────

    /// <summary>
    /// Five sources, every event on the SAME tick, ids interleaved across the sources (source
    /// s owns s, s+5, s+10 …). Ordering therefore comes entirely from the id comparison, and a
    /// heap that drained one source before touching the next would show up as a permutation.
    /// </summary>
    [Fact]
    public void IdenticalTimestampsAcrossSourcesStillMergeInIdOrder()
    {
        const int Sources = 5, PerSource = 300;
        long ts = new DateTimeOffset(2026, 7, 30, 0, 0, 0, TimeSpan.Zero).UtcTicks;

        var paths = new List<string>();
        for (int s = 0; s < Sources; s++)
            paths.Add(BuildSegment($"0-{s + 1}-0-0.seg",
                                   Enumerable.Range(0, PerSource).Select(k => (ulong)(k * Sources + s)).ToList(),
                                   _ => ts));

        var seen = Drain(paths);

        Assert.Equal(Sources * PerSource, seen.Count);
        Assert.Equal(Enumerable.Range(0, Sources * PerSource).Select(i => (ulong)i).ToList(),
                     seen.Select(x => x.Id).ToList());
        Assert.All(seen, x => Assert.Equal(ts, x.Ts));
    }

    /// <summary>
    /// Two sources holding the same (ts, id). The merge preserves the multiset — it is not a
    /// deduplicator, and quietly dropping one of a pair would be indistinguishable from a lost
    /// event once the sources are deleted.
    /// </summary>
    [Fact]
    public void ADuplicateKeyInTwoSourcesKeepsBothRows()
    {
        long ts = new DateTimeOffset(2026, 7, 30, 0, 0, 0, TimeSpan.Zero).UtcTicks;
        var seen = Drain([BuildSegment("0-101-0-0.seg", [7UL, 42UL, 99UL], _ => ts),
                          BuildSegment("0-102-0-0.seg", [42UL, 43UL],      _ => ts)]);

        Assert.Equal([7UL, 42UL, 42UL, 43UL, 99UL], seen.Select(x => x.Id).ToList());
    }

    /// <summary>
    /// Ties that straddle BLOCK boundaries: 8,000 rows of ~300 B fill many 64 KB blocks, and
    /// every third event repeats its predecessor's timestamp. A cursor that reloads its block
    /// mid-tie must still hand the heap the right key, and the block zone map must stay
    /// monotonic across the equal timestamps.
    /// </summary>
    [Fact]
    public void TiesStraddlingBlockBoundariesKeepTheStreamSorted()
    {
        const int Sources = 4, PerSource = 2_000;
        long baseTs = new DateTimeOffset(2026, 7, 30, 0, 0, 0, TimeSpan.Zero).UtcTicks;

        var paths    = new List<string>();
        var expected = new List<(long Ts, ulong Id)>();
        for (int s = 0; s < Sources; s++)
        {
            var ids = Enumerable.Range(0, PerSource).Select(k => (ulong)(k * Sources + s)).ToList();
            paths.Add(BuildSegment($"0-{200 + s}-0-0.seg", ids,
                                   id => baseTs + (long)(id / 3) * TimeSpan.TicksPerSecond, pad: 300));
            foreach (var id in ids) expected.Add((baseTs + (long)(id / 3) * TimeSpan.TicksPerSecond, id));
        }

        Assert.Equal(expected.OrderBy(x => x.Ts).ThenBy(x => x.Id).ToList(), Drain(paths));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static List<(long Ts, ulong Id)> Drain(IReadOnlyList<string> paths)
    {
        var seen = new List<(long, ulong)>();
        using var src = MergingSegmentEventSource.Open(paths);
        while (src.TryReadNext(out var ev)) seen.Add((ev.TimestampUtcTicks, ev.Id));
        return seen;
    }

    private static byte[] Props(int i)
    {
        var buf = new ArrayBufferWriter<byte>(384);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(3);
        w.Write("OrderId");  w.Write("order-" + i);          // unique per event
        w.Write("Customer"); w.Write("cust-" + (i % 40));    // spans every group
        w.Write("pad");      w.Write(new string((char)('a' + i % 26), 220));
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    private static async Task<int> CountAsync(QueryExecutor q, string filter, int cap) =>
        (await RunAsync(q, filter, cap)).Count;

    private static async Task<List<LogEvent>> RunAsync(QueryExecutor q, string? filter, int count)
    {
        var res = new List<LogEvent>();
        await foreach (var ev in q.ExecuteAsync(new QueryRequest
        {
            Filter = filter, Count = count, Direction = QueryDirection.Backward,
        }))
        {
            res.Add(ev);
            if (res.Count >= count) break;
        }
        return res;
    }

    /// <summary>A .seg with exactly the ids and timestamps asked for — the engine assigns its
    /// own ids on the write path, so a tie can only be built by writing the file directly.</summary>
    private string BuildSegment(string name, IReadOnlyList<ulong> ids, Func<ulong, long> tsOf, int pad = 64)
    {
        var segDir = Path.Combine(_dir, "handbuilt");
        Directory.CreateDirectory(segDir);

        var pool    = new StringInternPool();
        int tmplIdx = pool.Intern("probe {n}");
        int svcIdx  = pool.Intern("Svc.Probe");

        using var hot = new HotTierSegment(ids.Count + 1, (long)ids.Count * (pad + 256) + (8L << 20));
        var buf = new ArrayBufferWriter<byte>(512);
        foreach (var id in ids)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(2);
            w.Write("id");  w.Write((long)id);
            w.Write("pad"); w.Write(new string('x', pad));
            w.Flush();
            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = id,
                TimestampUtcTicks        = tsOf(id),
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
            }, buf.WrittenSpan, pool.Get(tmplIdx)));
        }
        hot.Freeze();

        string path = Path.Combine(segDir, name);
        using (var sw = new SegmentWriter(path))
        {
            sw.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot));
            sw.Finalise(new NodeId(0), new SegmentId(1UL));
        }
        return path;
    }
}
