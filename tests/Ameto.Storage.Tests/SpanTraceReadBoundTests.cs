using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using K4os.Compression.LZ4;
using MessagePack;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// ONE cold segment holding two traces worth naming, in a geometry that is fixed every run.
///
/// <para>The span shape is <see cref="ColdSpanSegmentFixture.SqlClientAttributes"/> — the same
/// eight-attribute SqlClient span the search bound is measured on — because a record's WEIGHT is
/// the entire subject here: a fixture that writes attribute-less spans defends a shape twenty
/// times lighter than the one that killed the server.</para>
///
/// <list type="bullet">
///   <item><see cref="Wide"/> owns the FIRST span of every block, so a read of it decodes every
///   block in the file and a test can sample the live set between them;</item>
///   <item><see cref="Needle"/> owns exactly ONE span, and it is the LAST span of the file — the
///   worst case for a reader that resolves offsets by walking, and the shape the finding was
///   measured on.</item>
/// </list>
/// Every other span is a trace of its own, so nothing else shares an id by accident.
///
/// <para>ITS SIZE, AND THE NUMBER IT IS NOT. The headline figure the finding was reported in —
/// 159.42 MB allocated to open a one-span trace — was measured on a 100 000-span segment. This
/// fixture is HALF that, and deliberately: 50 000 is exactly <c>HotFlushThreshold</c>, the size of
/// an ordinary flushed segment, so it is the file a user meets rather than a worst case chosen to
/// make a number look bad (a compacted segment reaches <c>MaxSpansPerPass</c> = 200 000, four
/// times this again). Every assertion below is therefore a RATIO against this fixture's own
/// measured whole-file cost, printed beside it, and never a comparison with the reported
/// constant — which would be a test of the machine the finding was taken on.</para>
/// </summary>
public sealed class TraceLookupSegmentFixture : IDisposable
{
    /// <summary>Exactly <c>HotFlushThreshold</c>: the size of an ordinary flushed segment, and
    /// half the 100 000 the reported 159.42 MB was measured on — see the class remarks.</summary>
    public const int Spans     = 50_000;
    /// <summary><c>SpanWriter.BlockSize</c>. The unit a trace read is allowed to hold.</summary>
    public const int BlockSize = 4096;

    public static int Blocks => (Spans + BlockSize - 1) / BlockSize;   // 13

    public static readonly TraceId Wide   = new(0x0A11CE_0000_0000_01UL, 0x1);
    public static readonly TraceId Needle = new(0x0A11CE_0000_0000_01UL, 0x2);

    /// <summary>The one span <see cref="Needle"/> owns — the last of the file.</summary>
    public const int NeedleOffset = Spans - 1;

    public static readonly DateTimeOffset Base = new(2026, 8, 2, 9, 0, 0, TimeSpan.Zero);

    public static long StartNano(int i) => Base.ToUnixTimeMilliseconds() * 1_000_000L + i * 1_000_000L;

    /// <summary>Which trace span <paramref name="i"/> belongs to.</summary>
    public static TraceId TraceOf(int i) =>
        i % BlockSize == 0 ? Wide
      : i == NeedleOffset  ? Needle
      : new TraceId(0xB0B_0000_0000_0000UL, (ulong)(i + 1));

    /// <summary>The global span offsets <see cref="Wide"/> occupies — one per block.</summary>
    public static int[] WideOffsets =>
        [.. Enumerable.Range(0, Blocks).Select(b => b * BlockSize)];

    public string Dir         { get; }
    public string SegmentPath { get; }

    public TraceLookupSegmentFixture()
    {
        Dir = Path.Combine(Path.GetTempPath(), "ameto-trcread-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Dir);

        var corpus = new List<SpanRecord>(Spans);
        for (int i = 0; i < Spans; i++)
            corpus.Add(new SpanRecord
            {
                TraceId           = TraceOf(i),
                SpanId            = new SpanId((ulong)(i + 1)),
                // Every span but the block-leading ones has a parent, so the v3 encoder writes
                // both branches of the parent field and the skip has to step over both.
                ParentSpanId      = i % BlockSize == 0 ? default : new SpanId((ulong)i),
                StartTimeUnixNano = StartNano(i),
                DurationNanos     = 1_000_000L * (1 + i % 2000),
                Name              = "SELECT payments",
                ServiceName       = "billing",
                Kind              = SpanKind.Client,
                Status            = SpanStatusCode.Unset,
                HttpStatusCode    = 0,
                Attributes        = ColdSpanSegmentFixture.SqlClientAttributes(i),
            });

        SegmentPath = SpanWriter.Write(Dir, corpus).FilePath;

        corpus.Clear();
        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
    }

    public void Dispose()
    {
        try { Directory.Delete(Dir, true); } catch { }
    }
}

/// <summary>
/// Opening ONE TRACE must cost the trace, not the segment it lives in.
///
/// <para>The span search was bounded first (see <c>SpanSearchBoundTests</c>) and the trace-detail
/// path was left holding exactly the fault that had just been removed from it: the trace index
/// stores GLOBAL SPAN OFFSETS, and the reader honoured them by materialising every span of the
/// file into a <c>List</c> so it could index into it. <c>GET /api/traces/{id}</c> and
/// <c>/flamegraph</c> therefore did on one click what the search had stopped doing on one
/// query — the stream returned the row and opening the row killed the box.</para>
///
/// <para>WHY THE FIX IS NOT A SEEK, and what these tests are really pinning. Offsets are indices
/// into the span SEQUENCE, and a v3 block's timestamps are a delta chain off the block's first
/// span — so the spans before a wanted one, inside its block, still have to be walked or every
/// start time after them is wrong. They are walked without being BUILT. That is the whole trick,
/// and it has two ways to go wrong that a memory test alone would never see: the Δts chain
/// silently drifting (covered by asserting exact start times against the whole-file reader), and
/// the block-geometry shortcut resolving an offset to another trace's span (covered by files
/// written to a geometry the shortcut does not expect).</para>
///
/// <para>WHERE THE MEASUREMENTS ARE TAKEN. A trace read hands back nothing until its walk is
/// over, so sampling "at the first yielded span" would sample after the peak — the mistake an
/// earlier round of the search tests made and measured 0.08 MB against a reader that materialises
/// whole files. <c>SpanReader._afterTraceBlockForTest</c> fires once per DECODED block, which is
/// inside the walk, and a reader that materialises the file never reaches it at all — so the
/// sample count is itself the assertion that the streamed path ran.</para>
/// </summary>
public sealed class SpanTraceReadBoundTests : IClassFixture<TraceLookupSegmentFixture>
{
    private readonly TraceLookupSegmentFixture _fx;
    private readonly ITestOutputHelper         _out;

    public SpanTraceReadBoundTests(TraceLookupSegmentFixture fx, ITestOutputHelper output)
    {
        _fx  = fx;
        _out = output;
    }

    private async Task<List<SpanRecord>> ReadTrace(TraceId id, string? path = null)
    {
        var got = new List<SpanRecord>();
        await foreach (var s in SpanReader.ReadTraceAsync(path ?? _fx.SegmentPath, id, CancellationToken.None))
            got.Add(s);
        return got;
    }

    // ── The bound ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task The_peak_of_a_trace_read_is_one_block_not_the_segment()
    {
        // Warm-up: JIT, and the ArrayPool block buffers, which are rented on the first decode and
        // stay in the pool afterwards — an unwarmed first pass reports the pool's growth as the
        // reader's live set.
        Assert.Equal(TraceLookupSegmentFixture.Blocks,
                     (await ReadTrace(TraceLookupSegmentFixture.Wide)).Count);

        // The yardstick, measured on this machine and this fixture rather than asserted from a
        // constant: what holding every span of the file costs, which is what the reader this
        // replaced retained for the whole of a trace lookup.
        long materialised = LiveBytesHoldingTheWholeFile();
        long perSpan      = materialised / TraceLookupSegmentFixture.Spans;
        long oneBlock     = perSpan * TraceLookupSegmentFixture.BlockSize;

        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        long baseline = GC.GetTotalMemory(forceFullCollection: true);

        long peak    = 0;
        int  samples = 0;
        SpanReader._afterTraceBlockForTest = () =>
        {
            samples++;
            long live = GC.GetTotalMemory(forceFullCollection: true) - baseline;
            if (live > peak) peak = live;
        };

        List<SpanRecord> spans;
        try     { spans = await ReadTrace(TraceLookupSegmentFixture.Wide); }
        finally { SpanReader._afterTraceBlockForTest = null; }

        _out.WriteLine($"segment       = {TraceLookupSegmentFixture.Spans:N0} spans, "
                     + $"{TraceLookupSegmentFixture.Blocks} blocks, "
                     + $"{new FileInfo(_fx.SegmentPath).Length / 1048576.0:N1} MB on disk");
        _out.WriteLine($"per span      = {perSpan:N0} B");
        _out.WriteLine($"one block     = {oneBlock / 1048576.0,8:N2} MB");
        _out.WriteLine($"whole file    = {materialised / 1048576.0,8:N2} MB   <- peak of the materialising reader");
        _out.WriteLine($"trace read    = {peak / 1048576.0,8:N2} MB   <- peak of this one, over {samples} decoded block(s)");

        Assert.Equal(TraceLookupSegmentFixture.Blocks, spans.Count);

        // A reader that materialises the file never enters the block walk, so it would sail
        // through every threshold below on a peak of zero. The sample count is what stops that.
        Assert.True(samples >= TraceLookupSegmentFixture.Blocks,
            $"the walk decoded {samples} block(s) for a trace with one span in each of "
          + $"{TraceLookupSegmentFixture.Blocks} — the trace read is not walking blocks at all");

        // THE SHAPE ASSERTION, in this fixture's own bytes: under what ONE BLOCK of decoded spans
        // costs, not "under N megabytes". A reader that materialises the file peaks at
        // Spans/BlockSize = 12.2 blocks and fails here.
        //
        // The multiplier was 3 and that was too loose to do its job. The walk legitimately holds
        // the block's raw bytes, which are far cheaper than the same block's SpanRecords — measured
        // here at 2.69 MB against a 6.84 MB block, so 0.39 blocks — while the defect this guards
        // against, materialising the block's spans and selecting from them, adds a whole block and
        // lands at about 1.4. A budget of three blocks passed both, which is how the earlier
        // version of this test stayed green under exactly that mutation. One block sits between
        // them with room on either side: two and a half times the honest peak, and well under the
        // defect's.
        Assert.True(peak < oneBlock,
            $"peak retention was {peak / 1048576.0:N2} MB = {(double)peak / oneBlock:N2} blocks "
          + $"({peak * 100.0 / materialised:N1}% of the whole file) — a trace read is holding "
          + "a whole block's worth of decoded spans, not just the block");

        Assert.True(peak < materialised / 4,
            $"peak retention was {peak * 100.0 / materialised:N1}% of the whole file");
    }

    [Fact]
    public async Task A_one_span_trace_does_not_allocate_the_segment()
    {
        // Allocation counters, not heap sampling: exact, and they see the work a materialising
        // reader does and then drops before any GC could be asked about it. This is the KIND of
        // number the finding was reported in — 159.42 MB to open a one-span trace — but that
        // figure was taken on a 100 000-span segment and this fixture is 50 000, so the assertion
        // is a ratio against the whole-file read measured right here, not against the constant.
        await ReadTrace(TraceLookupSegmentFixture.Needle);        // warm

        long a0 = GC.GetTotalAllocatedBytes(precise: true);
        var  got = await ReadTrace(TraceLookupSegmentFixture.Needle);
        long allocTrace = GC.GetTotalAllocatedBytes(precise: true) - a0;

        long b0 = GC.GetTotalAllocatedBytes(precise: true);
        var  all = SpanReader.ReadAll(_fx.SegmentPath);
        long allocFile = GC.GetTotalAllocatedBytes(precise: true) - b0;
        GC.KeepAlive(all);

        _out.WriteLine($"one-span trace : {allocTrace / 1048576.0,8:N2} MB allocated");
        _out.WriteLine($"whole file     : {allocFile  / 1048576.0,8:N2} MB allocated");
        _out.WriteLine($"                 {100.0 * allocTrace / allocFile:N2} % of reading the file");

        Assert.Single(got);
        Assert.Equal(TraceLookupSegmentFixture.StartNano(TraceLookupSegmentFixture.NeedleOffset),
                     got[0].StartTimeUnixNano);

        // The reader this replaced read the whole file to answer this, so the two numbers were
        // the same number. A tenth is a generous bar for "one block out of thirteen, and the
        // twelve it skipped are not decompressed either".
        Assert.True(allocTrace < allocFile / 10,
            $"opening a ONE-SPAN trace allocated {allocTrace / 1048576.0:N2} MB against the "
          + $"{allocFile / 1048576.0:N2} MB of reading the whole {TraceLookupSegmentFixture.Spans:N0}-span "
          + "segment — the trace read is still materialising the file");
    }

    // ── What the bound must not have cost ─────────────────────────────────────

    [Fact]
    public async Task A_trace_read_returns_exactly_what_the_whole_file_reader_would()
    {
        // The equivalence, checked against the one reader that still materialises whole files.
        // Start times are the load-bearing field: they are a per-block DELTA CHAIN, so a walk
        // that skips a span without carrying its delta returns spans whose timestamps are
        // plausible, ordered, and wrong.
        var all = SpanReader.ReadAll(_fx.SegmentPath);

        foreach (var (id, label) in new[]
                 {
                     (TraceLookupSegmentFixture.Wide,   "wide (one span per block)"),
                     (TraceLookupSegmentFixture.Needle, "needle (one span, last of the file)"),
                     (TraceLookupSegmentFixture.TraceOf(4097), "an ordinary mid-block trace"),
                     (TraceLookupSegmentFixture.TraceOf(1),    "an ordinary first-block trace"),
                 })
        {
            var expected = all.Where(s => s.TraceId.Equals(id))
                              .Select(s => (s.SpanId, s.ParentSpanId, s.StartTimeUnixNano,
                                            s.DurationNanos, s.Name, s.ServiceName, s.Status))
                              .ToList();
            var actual = (await ReadTrace(id))
                              .Select(s => (s.SpanId, s.ParentSpanId, s.StartTimeUnixNano,
                                            s.DurationNanos, s.Name, s.ServiceName, s.Status))
                              .ToList();

            Assert.NotEmpty(expected);
            Assert.Equal(expected, actual);
        }

        // And the attributes survive: the skip must apply to spans the trace does not own.
        var wide = await ReadTrace(TraceLookupSegmentFixture.Wide);
        foreach (var s in wide)
        {
            Assert.NotNull(s.Attributes);
            Assert.Equal(8, s.Attributes!.Count);
            Assert.Equal("mssql", s.Attributes["db.system"]);
            Assert.Equal(1433L,   s.Attributes["net.peer.port"]);
        }
    }

    [Fact]
    public async Task A_trace_the_segment_does_not_hold_yields_nothing()
    {
        Assert.Empty(await ReadTrace(new TraceId(0xDEAD, 0xBEEF)));
    }

    // ── The geometry shortcut, and what happens when it is wrong ──────────────

    [Theory]
    // A block size SMALLER than the reader assumes: the offset lands in a block whose first span
    // sits above it, which the walk can see without decoding anything.
    [InlineData(1_000)]
    // LARGER: the shortcut skips a block that actually held the span and then resolves the offset
    // onto a span belonging to a different trace — only the per-span trace-id check catches this.
    [InlineData(8_000)]
    // And a file whose whole content is one block, where the shortcut happens to be right.
    [InlineData(20_000)]
    public async Task A_v3_segment_written_to_another_block_size_still_resolves_its_offsets(int blockSpans)
    {
        const int Total = 10_000;
        string dir = Path.Combine(Path.GetTempPath(), "ameto-trcgeom-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var corpus = BuildOddCorpus(Total);
            string path = Path.Combine(dir, "odd.trc");
            OddGeometryV3Writer.Write(path, corpus, blockSpans);

            // Offsets chosen to land on both sides of every 4096-boundary the shortcut assumes.
            foreach (int offset in new[] { 0, 1, 3_000, 4_095, 4_096, 5_000, 8_500, Total - 1 })
            {
                var id       = corpus[offset].TraceId;
                var expected = corpus.Where(s => s.TraceId.Equals(id))
                                     .Select(s => (s.SpanId, s.StartTimeUnixNano, s.Name))
                                     .ToList();
                var actual   = (await ReadTrace(id, path))
                                     .Select(s => (s.SpanId, s.StartTimeUnixNano, s.Name))
                                     .ToList();

                Assert.Equal(expected, actual);
            }
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    [Fact]
    public async Task A_legacy_v2_segment_resolves_its_offsets_by_counting_blocks()
    {
        // v2 has no delta chain and no fixed block size the reader can lean on, so it always
        // takes the counting walk. It is also the format the migration path keeps alive, so a
        // trace opened on a not-yet-compacted segment goes through exactly this code.
        const int Total = 9_000;
        string dir = Path.Combine(Path.GetTempPath(), "ameto-trcv2-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var corpus = BuildOddCorpus(Total);
            string path = Path.Combine(dir, "legacy.trc");
            SpanFormatV3Tests.WriteV2File(path, corpus);

            Assert.Equal(2, SpanReader.ReadSegmentInfo(path).FormatVersion);

            foreach (int offset in new[] { 0, 4_095, 4_096, 8_192, Total - 1 })
            {
                var id       = corpus[offset].TraceId;
                var expected = corpus.Where(s => s.TraceId.Equals(id))
                                     .Select(s => (s.SpanId, s.StartTimeUnixNano, s.Name))
                                     .ToList();
                var actual   = (await ReadTrace(id, path))
                                     .Select(s => (s.SpanId, s.StartTimeUnixNano, s.Name))
                                     .ToList();
                Assert.Equal(expected, actual);
            }
        }
        finally { try { Directory.Delete(dir, true); } catch { } }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Spans whose traces straddle block boundaries at every plausible block size: every fifth
    /// span joins a trace 2 500 spans behind it, so no matter where the blocks fall, some trace
    /// has spans on both sides of one.
    /// </summary>
    private static List<SpanRecord> BuildOddCorpus(int total)
    {
        var spans = new List<SpanRecord>(total);
        for (int i = 0; i < total; i++)
            spans.Add(new SpanRecord
            {
                TraceId           = new TraceId(0xC0FFEEUL, (ulong)(i % 5 == 0 && i >= 2500 ? i - 2500 : i)),
                SpanId            = new SpanId((ulong)(i + 1)),
                ParentSpanId      = i % 3 == 0 ? default : new SpanId((ulong)i),
                StartTimeUnixNano = TraceLookupSegmentFixture.StartNano(i),
                DurationNanos     = 1_000_000L * (1 + i % 700),
                Name              = "span-" + i,
                ServiceName       = "billing",
                Kind              = SpanKind.Client,
                Status            = SpanStatusCode.Unset,
                HttpStatusCode    = (short)(i % 7 == 0 ? 500 : 0),
                Attributes        = ColdSpanSegmentFixture.SqlClientAttributes(i),
            });
        return spans;
    }

    private long LiveBytesHoldingTheWholeFile()
    {
        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        long baseline = GC.GetTotalMemory(forceFullCollection: true);
        var  held     = SpanReader.ReadAll(_fx.SegmentPath);
        long live     = GC.GetTotalMemory(forceFullCollection: true) - baseline;
        GC.KeepAlive(held);
        return live;
    }
}

/// <summary>
/// A v3 writer with a SETTABLE block size — the one thing <see cref="SpanWriter"/> fixes and the
/// one thing <c>SpanReader</c>'s offset shortcut assumes. Encodes spans exactly as the real
/// writer does (positional array, per-block Δts chain, inline attributes) and lays out the same
/// four sections, minus the per-block blooms, which are optional by format (a zero-length bitset
/// means "never skip").
/// </summary>
internal static class OddGeometryV3Writer
{
    public static void Write(string filePath, IList<SpanRecord> spans, int blockSpans)
    {
        var ordered = new List<SpanRecord>(spans);
        ordered.Sort(static (a, b) => a.StartTimeUnixNano.CompareTo(b.StartTimeUnixNano));

        using var fs = new FileStream(filePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 65536);
        using var bw = new BinaryWriter(fs);

        bw.Write(0x52_44_54_43u);                        // "RDTC"
        bw.Write((ushort)3);
        bw.Write((uint)ordered.Count);
        bw.Write(ordered[0].StartTimeUnixNano);
        bw.Write(ordered[^1].StartTimeUnixNano);
        bw.Write((byte)0);                               // flags

        var traceIndex  = new Dictionary<TraceId, List<uint>>();
        var svcBlockMap = new Dictionary<string, SortedSet<uint>>(StringComparer.Ordinal);

        int written    = 0;
        int blockCount = 0;
        Span<byte> idBuf = stackalloc byte[16];   // one for the whole walk — not one per block
        while (written < ordered.Count)
        {
            int  count    = Math.Min(blockSpans, ordered.Count - written);
            uint blockIdx = (uint)blockCount;

            var buf = new ArrayBufferWriter<byte>(1024 * 1024);
            var w   = new MessagePackWriter(buf);
            w.WriteArrayHeader(count);

            long prevTs = 0;
            for (int i = 0; i < count; i++)
            {
                var s = ordered[written + i];
                if (!traceIndex.TryGetValue(s.TraceId, out var t))
                    traceIndex[s.TraceId] = t = new List<uint>(4);
                t.Add((uint)(written + i));
                if (!svcBlockMap.TryGetValue(s.ServiceName, out var blk))
                    svcBlockMap[s.ServiceName] = blk = new SortedSet<uint>();
                blk.Add(blockIdx);

                w.WriteArrayHeader(11);
                s.TraceId.WriteTo(idBuf);
                w.Write((ReadOnlySpan<byte>)idBuf);
                BinaryPrimitives.WriteUInt64BigEndian(idBuf, s.SpanId.RawValue);
                w.Write((ReadOnlySpan<byte>)idBuf[..8]);
                if (s.ParentSpanId.IsEmpty) w.WriteNil();
                else
                {
                    BinaryPrimitives.WriteUInt64BigEndian(idBuf, s.ParentSpanId.RawValue);
                    w.Write((ReadOnlySpan<byte>)idBuf[..8]);
                }
                w.Write(i == 0 ? s.StartTimeUnixNano : s.StartTimeUnixNano - prevTs);
                prevTs = s.StartTimeUnixNano;
                w.Write(s.DurationNanos);
                w.Write(s.Name);
                w.Write(s.ServiceName);
                w.Write((byte)s.Kind);
                w.Write((byte)s.Status);
                w.Write(s.HttpStatusCode);
                if (s.Attributes is { Count: > 0 } attrs)
                {
                    w.WriteMapHeader(attrs.Count);
                    foreach (var (k, v) in attrs)
                    {
                        w.Write(k);
                        switch (v)
                        {
                            case null:       w.WriteNil();               break;
                            case string str: w.Write(str);               break;
                            case bool b:     w.Write(b);                 break;
                            case long l:     w.Write(l);                 break;
                            case double d:   w.Write(d);                 break;
                            default:         w.Write(v.ToString() ?? ""); break;
                        }
                    }
                }
                else w.WriteNil();
            }
            w.Flush();

            var raw  = buf.WrittenSpan;
            var comp = LZ4Pickler.Pickle(raw, LZ4Level.L09_HC);
            bw.Write((uint)raw.Length);
            bw.Write((uint)comp.Length);
            bw.Write(comp);

            written += count;
            blockCount++;
        }

        long traceIdxOffset = fs.Position;
        {
            var idxBuf = new MemoryStream(traceIndex.Count * 32);
            var idxBw  = new BinaryWriter(idxBuf);
            Span<byte> tid = stackalloc byte[16];
            idxBw.Write((uint)traceIndex.Count);
            foreach (var (traceId, offsets) in traceIndex)
            {
                traceId.WriteTo(tid);
                idxBw.Write(tid);
                idxBw.Write((uint)offsets.Count);
                foreach (var o in offsets) idxBw.Write(o);
            }
            idxBw.Flush();
            var raw  = idxBuf.GetBuffer().AsSpan(0, (int)idxBuf.Length);
            var comp = LZ4Pickler.Pickle(raw, LZ4Level.L09_HC);
            bw.Write((uint)raw.Length);
            bw.Write((uint)comp.Length);
            bw.Write(comp);
        }

        long svcIdxOffset = fs.Position;
        bw.Write((uint)svcBlockMap.Count);
        foreach (var (svcName, blocks) in svcBlockMap)
        {
            var nameBytes = Encoding.UTF8.GetBytes(svcName);
            bw.Write((ushort)nameBytes.Length);
            bw.Write(nameBytes);
            bw.Write((uint)blocks.Count);
            foreach (var b in blocks) bw.Write(b);
        }

        long bloomIdxOffset = fs.Position;
        bw.Write((uint)blockCount);
        for (int i = 0; i < blockCount; i++) bw.Write(0u);   // no bloom ⇒ never skip

        bw.Write((ulong)traceIdxOffset);
        bw.Write((ulong)svcIdxOffset);
        bw.Write((ulong)bloomIdxOffset);
        bw.Write(0x52_44_54_46u);                            // "RDTF"
    }
}
