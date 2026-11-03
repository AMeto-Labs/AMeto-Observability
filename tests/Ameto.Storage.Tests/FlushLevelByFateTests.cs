using Ameto.Core;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using K4os.Compression.LZ4;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// A flush is compressed at <see cref="SpanWriter.FlushLevel"/> on the premise that a merge will
/// rewrite it at <see cref="SpanWriter.MergeLevel"/>. Where the premise fails — a flush the
/// compaction planner will never take — the file would keep the cheaper level until retention, so
/// the engine asks the planner's own candidate test before it writes and uses the merge level for a
/// flush that settles on its own. These facts pin that decision
/// (<see cref="TraceStorageEngine.FlushLevelFor"/>) and that the engine wires it in.
/// </summary>
public sealed class FlushLevelByFateTests : IDisposable
{
    private const long MB = 1024 * 1024;
    private const long Ms = 1_000_000L;
    private static readonly long BaseNano =
        new DateTimeOffset(2026, 8, 1, 12, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds() * Ms;

    private readonly string _root = Path.Combine(Path.GetTempPath(), "ameto-flushlevel-" + Guid.NewGuid().ToString("N"));

    public FlushLevelByFateTests() => Directory.CreateDirectory(_root);
    public void Dispose() { try { Directory.Delete(_root, true); } catch { } }

    private string Dir(string name)
    {
        string d = Path.Combine(_root, name);
        Directory.CreateDirectory(d);
        return d;
    }

    // ── The decision ────────────────────────────────────────────────────────

    /// <summary>The stand's pass budget, the case the decision exists for.</summary>
    private const long StandBudget = 24 * MB;

    /// <summary>The ordinary span's read-back weight: a 375-byte blob plus the per-span overhead.</summary>
    private const long OrdinarySpanBytes = 608;

    [Fact]
    public void A_flush_the_streaming_planner_can_take_keeps_the_flush_level()
    {
        // 500 spans in 50 traces at the ordinary weight: a sliver of a 24 MB pass.
        Assert.Equal(SpanWriter.FlushLevel,
            TraceStorageEngine.FlushLevelFor(spanCount: 500, traceCount: 50, weightBytes: 500 * OrdinarySpanBytes,
                                             StandBudget, streaming: true));
    }

    [Fact]
    public void A_flush_that_settles_on_its_own_is_written_at_the_merge_level()
    {
        // Every span its own trace: the output-side state alone (36 B a span, 200 B a row) passes
        // half of what a 24 MB pass may spend, so the planner would never take it — on the 512 MB
        // stand a full flush of one-span traces is exactly this file.
        const int spans = 37_631;
        Assert.Equal(SpanWriter.MergeLevel,
            TraceStorageEngine.FlushLevelFor(spans, traceCount: spans, weightBytes: spans * OrdinarySpanBytes,
                                             StandBudget, streaming: true));
    }

    [Fact]
    public void The_decision_is_the_streaming_planners_own_candidate_test()
    {
        // Not a second model of the planner: FlushLevelFor asks the same three functions
        // SelectStreamingBatch asks, with the same halving, so the two cannot disagree. Checked
        // across the shapes a stand produces, including ones that sit near the line.
        long available = StandBudget - TraceStorageEngine.StreamedFixedBytes(StandBudget);
        foreach (int spans in new[] { 500, 4_096, 12_000, 37_631, 50_000 })
        foreach (int perTrace in new[] { 1, 2, 5, 10 })
        {
            int  traces  = Math.Max(1, spans / perTrace);
            long weight  = spans * OrdinarySpanBytes;
            var  seg     = new SpanSegmentInfo { SpanCount = spans }.WithWeight(weight);
            bool settles = TraceStorageEngine.StreamedBlockBytes(seg)
                         + TraceStorageEngine.StreamedStateBytes(seg, traces) >= available / 2;
            Assert.Equal(settles ? SpanWriter.MergeLevel : SpanWriter.FlushLevel,
                TraceStorageEngine.FlushLevelFor(spans, traces, weight, StandBudget, streaming: true));
        }
    }

    [Fact]
    public void A_budget_too_small_for_any_streaming_pass_settles_every_flush()
    {
        // The fixed costs eat the whole budget: the planner returns nothing, ever, so every flush is final.
        long budget = TraceStorageEngine.StreamedWriterFixedBytes;
        Assert.Equal(SpanWriter.MergeLevel,
            TraceStorageEngine.FlushLevelFor(100, 10, 100 * OrdinarySpanBytes, budget, streaming: true));
    }

    [Fact]
    public void A_materialising_pass_takes_a_flush_under_its_threshold_and_refuses_one_at_it()
    {
        long threshold = TraceStorageEngine.CompactionThresholdBytesFor(StandBudget);
        Assert.Equal(SpanWriter.FlushLevel,
            TraceStorageEngine.FlushLevelFor(1_000, 100, threshold - 1, StandBudget, streaming: false));
        Assert.Equal(SpanWriter.MergeLevel,
            TraceStorageEngine.FlushLevelFor(1_000, 100, threshold, StandBudget, streaming: false));
    }

    // ── The wiring ──────────────────────────────────────────────────────────

    /// <summary>
    /// The engine asks before it writes: with a pass budget no streaming merge fits, the flushed
    /// file's blocks are the merge level's bytes; with room to merge, the flush level's. Read off
    /// the file itself — every LZ4 section inflated and re-pickled at both levels — so the fact is
    /// about what reached disk, not about which argument a method received.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void The_engine_writes_a_flush_that_will_never_merge_at_the_merge_level(bool roomToMerge)
    {
        long   budget = roomToMerge ? 256 * MB : TraceStorageEngine.StreamedWriterFixedBytes;
        string dir    = Dir(roomToMerge ? "room" : "settled");
        using var e = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance,
                                             options: new TracesOptions { MergeBudgetBytes = budget, CompactionMinAge = TimeSpan.Zero });
        e.WriteSpans(Corpus(6_000));
        e.FlushHotTier();

        var seg      = Assert.Single(e.ColdSegmentsForTest);
        var sections = ReadCompressedSections(seg.FilePath, out long onDisk);
        long atFlushLevel = 0, atMergeLevel = 0;
        foreach (var raw in sections)
        {
            atFlushLevel += LZ4Pickler.Pickle(raw, SpanWriter.FlushLevel).Length;
            atMergeLevel += LZ4Pickler.Pickle(raw, SpanWriter.MergeLevel).Length;
        }
        Assert.NotEqual(atFlushLevel, atMergeLevel);   // the corpus tells the two levels apart
        Assert.Equal(roomToMerge ? atFlushLevel : atMergeLevel, onDisk);
    }

    private static SpanIngestItem[] Corpus(int spans)
    {
        var items = new SpanIngestItem[spans];
        for (int i = 0; i < spans; i++)
        {
            int trace = i / 10;
            items[i] = new SpanIngestItem
            {
                TraceId           = new TraceId(0x5EED, (ulong)trace + 1),
                SpanId            = new SpanId((ulong)i + 1),
                ParentSpanId      = i % 10 == 0 ? default : new SpanId((ulong)(trace * 10) + 1),
                StartTimeUnixNano = BaseNano + i * Ms,
                DurationNanos     = (i % 7 + 1) * 3 * Ms,
                Name              = i % 10 == 0 ? "POST /api/pay" : "SELECT payments WHERE id = @p0",
                ServiceName       = i % 3 == 0 ? "gateway" : "ledger",
                Kind              = i % 10 == 0 ? SpanKind.Server : SpanKind.Client,
                Status            = i % 11 == 0 ? SpanStatusCode.Error : SpanStatusCode.Ok,
                HttpStatusCode    = (short)(i % 10 == 0 ? 200 : 0),
                AttributesBytes   = Blob(i),
            };
        }
        return items;
    }

    /// <summary>A msgpack map of eight attributes: enough repetition for the HC levels to differ.</summary>
    private static byte[] Blob(int i)
    {
        var buf = new System.Buffers.ArrayBufferWriter<byte>(256);
        var w   = new MessagePack.MessagePackWriter(buf);
        w.WriteMapHeader(8);
        w.Write("db.system");           w.Write("mssql");
        w.Write("db.name");             w.Write("payments");
        w.Write("db.statement");        w.Write("SELECT id, amount, currency FROM payments WHERE merchant_id = @p0 AND created_at > @p1");
        w.Write("net.peer.name");       w.Write("sql-" + (i % 4) + ".internal");
        w.Write("net.peer.port");       w.Write(1433);
        w.Write("http.request.method"); w.Write(i % 2 == 0 ? "GET" : "POST");
        w.Write("url.path");            w.Write("/api/v1/payments/" + (i % 50));
        w.Write("thread.id");           w.Write(i % 64);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>The LZ4 sections of a v3 <c>.trc</c>, inflated: the span blocks and the trace-index block.</summary>
    private static List<byte[]> ReadCompressedSections(string trcPath, out long onDiskBytes)
    {
        byte[] file = File.ReadAllBytes(trcPath);
        var span = file.AsSpan();
        long traceIdxOffset = (long)System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(span[^28..]);

        var sections = new List<byte[]>();
        onDiskBytes = 0;
        int pos = 27;                                      // the fixed header
        while (true)
        {
            int comp = (int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(span[(pos + 4)..]);
            sections.Add(LZ4Pickler.Unpickle(span.Slice(pos + 8, comp)));
            onDiskBytes += comp;
            bool wasIndex = pos == traceIdxOffset;
            pos += 8 + comp;
            if (wasIndex) break;                           // the trace-index block is the last one
        }
        return sections;
    }
}
