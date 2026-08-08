using System.Buffers;
using System.Diagnostics;
using K4os.Compression.LZ4;
using MessagePack;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// What LZ4-HC buys and what it costs, measured on the path that now does it: the merge.
///
/// <para>HC used to be a background sweep that rewrote a cold file in place. It has been dead
/// since v6 (its version gate refused every segment written after it) and it cannot simply be
/// pointed at v7, whose index sections are interleaved between block runs. The replacement is
/// to encode at HC while the merge is writing the file it already rewrites end to end.</para>
///
/// <para>The three questions this answers: how much smaller is a merged file at HC, what does
/// the extra encode cost against everything else a merge does, and is L09 the right rung.</para>
/// </summary>
public sealed class MergeCompressionProbe : IDisposable
{
    private const long GroupBudget = 8L * 1024 * 1024;
    private const int  Sources     = 8;

    private readonly ITestOutputHelper _out;
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-hc-" + Guid.NewGuid().ToString("N"));

    public MergeCompressionProbe(ITestOutputHelper o)
    {
        _out = o;
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, true); } catch { }
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    /// <summary>
    /// The merge, run over byte-identical sources at both levels: what HC costs the pass that
    /// now does it, and what it returns.
    ///
    /// <para>MEASURED, 48 000 trace-carrying prop-dense events, 17.9 MB of uncompressed payload:
    /// the merged file goes from 20.49 MB to 19.92 MB (-2.8 %) while its BLOCKS go from 4.45 MB
    /// to 3.88 MB (-12.8 %), for ~18 % more wall clock on the merge. The gap between the two
    /// percentages is the whole story and <see cref="TheBlockPayloadIsAMinorityOfAMergedFile"/>
    /// is where it is explained.</para>
    ///
    /// <para>The absolute cost is what settles it: at the merge's own throughput (48 MB of
    /// uncompressed payload a second through a full merge with the index build), the extra encode
    /// is ~66 ms per 18 MB. The sandbox stand ingests ~370 MB of stored payload a day and this
    /// branch measures ~3× write amplification, so a day of its compaction spends on the order of
    /// twenty seconds of ONE core more than it did. That is the number that makes 2.8 % worth
    /// having; a percentage of a pass would not have been.</para>
    /// </summary>
    [Fact]
    public void HcCostsAMinorityOfAMergeAndTheFileKeepsIt()
    {
        const double MB = 1048576.0;

        var paths = BuildSources(eventsPerSource: 6_000);

        // Warm: JIT, ArrayPool growth and the page cache for the sources.
        Merge(paths, "warm-fast", SegmentCompression.Fast);
        Merge(paths, "warm-hc",   SegmentCompression.High);

        var fast = Best(paths, "fast", SegmentCompression.Fast);
        var hc   = Best(paths, "hc",   SegmentCompression.High);
        long fastBlocks = Breakdown(paths, "e2e-fast", SegmentCompression.Fast, GroupBudget).Blocks;
        long hcBlocks   = Breakdown(paths, "e2e-hc",   SegmentCompression.High, GroupBudget).Blocks;

        double payloadShrink = 1 - hcBlocks / (double)fastBlocks;
        double fileShrink    = 1 - hc.Bytes / (double)fast.Bytes;
        double slower        = hc.Millis / fast.Millis;

        _out.WriteLine($"  events merged   : {fast.Events:N0}");
        _out.WriteLine($"  fast (L00)      : file {fast.Bytes / MB,6:F2} MB  blocks {fastBlocks / MB,5:F2} MB  merge {fast.Millis,6:F0} ms");
        _out.WriteLine($"  HC   (L06)      : file {hc.Bytes / MB,6:F2} MB  blocks {hcBlocks / MB,5:F2} MB  merge {hc.Millis,6:F0} ms");
        _out.WriteLine($"  HC shrinks the payload {payloadShrink * 100:F1}%, the file {fileShrink * 100:F1}%, " +
                       $"for {(slower - 1) * 100:F0}% more merge time");

        // Same content on both sides — only the encoder changed.
        Assert.Equal(fast.Events, hc.Events);
        Assert.True(hc.Bytes < fast.Bytes, "HC produced a bigger file than the fast level");

        // The guard is on the PAYLOAD, because that is the quantity the codec controls. Set well
        // below the measured 12.8 % so a codec upgrade does not fail the build, and far above 0
        // so a silent revert to the fast level does.
        Assert.True(payloadShrink > 0.08,
            $"HC saved only {payloadShrink * 100:F1}% of the payload — the merge is not writing HC");

        // Wall clock on a shared machine, so the bound is loose on purpose: it catches the SHAPE
        // of the answer (the encode is a minority of a merge, not a multiple of it) — the rung
        // choice itself is guarded by the ladder test, on bytes rather than on time.
        Assert.True(slower < 2.0,
            $"HC merge took {slower:F2}x the fast merge — the encode is no longer a minority of the pass");
    }

    /// <summary>
    /// Which HC rung, measured on the exact block payloads the writer emits — block-level, so the
    /// codec is isolated from the merge's other work.
    ///
    /// <para>MEASURED (uncompressed → compressed, encode rate over uncompressed bytes):</para>
    /// <code>
    ///                 id-dense 17.9 MB          text-dense 24.3 MB
    ///   L00_FAST      4.45 MB        1818 MB/s  3.64 MB        1192 MB/s
    ///   L03_HC        3.98 MB -10.6%  323 MB/s  2.78 MB -23.6%  360 MB/s
    ///   L06_HC        3.88 MB -12.7%  235 MB/s  2.67 MB -26.5%  208 MB/s
    ///   L09_HC        3.88 MB -12.8%  135 MB/s  2.66 MB -26.7%  142 MB/s
    ///   L12_MAX       3.87 MB -13.1%   29 MB/s  2.65 MB -27.1%   31 MB/s
    /// </code>
    ///
    /// <para>L06 is the rung because L09 buys 0.1-0.2 points more for 1.6× the CPU and L12 buys
    /// 0.4 for 7×. Decode is measured too and is not neutral in the usual direction: HC emits
    /// fewer, longer matches, so its blocks read back FASTER (6.5 → 8.2 GB/s id-dense,
    /// 6.5 → 8.6 GB/s text-dense). The query path never pays for the write-side choice.</para>
    /// </summary>
    [Theory]
    [InlineData(Shape.IdDense)]
    [InlineData(Shape.TextDense)]
    public void TheLevelLadderStopsAtL06(Shape shape)
    {
        var blocks = BlockPayloads(eventsPerSource: 6_000, shape);
        long raw = 0;
        foreach (var b in blocks) raw += b.Length;

        _out.WriteLine($"  {blocks.Count} blocks, {raw / 1048576.0:F2} MB uncompressed");
        _out.WriteLine("  level    | compressed |  ratio | vs L00 |   encode MB/s");
        _out.WriteLine("  ---------+------------+--------+--------+--------------");

        var ladder = new (string Name, LZ4Level Level)[]
        {
            ("L00_FAST", LZ4Level.L00_FAST),
            ("L03_HC",   LZ4Level.L03_HC),
            ("L06_HC",   LZ4Level.L06_HC),
            ("L09_HC",   LZ4Level.L09_HC),
            ("L12_MAX",  LZ4Level.L12_MAX),
        };

        long fastBytes = 0;
        var results = new List<(string Name, long Bytes, double MbPerSec)>();
        foreach (var (name, level) in ladder)
        {
            // Three passes, fastest wins: one rung must not pay for the JIT the next one uses.
            var r = EncodeAll(blocks, level);
            for (int i = 0; i < 2; i++)
            {
                var again = EncodeAll(blocks, level);
                Assert.Equal(r.Bytes, again.Bytes);
                if (again.Millis < r.Millis) r = again;
            }
            if (name == "L00_FAST") fastBytes = r.Bytes;
            results.Add((name, r.Bytes, raw / 1048576.0 / (r.Millis / 1000.0)));
            _out.WriteLine($"  {name,-8} | {r.Bytes / 1048576.0,7:F2} MB | {raw / (double)r.Bytes,5:F2}x | " +
                           $"{100 - r.Bytes * 100.0 / fastBytes,5:F1}% | {raw / 1048576.0 / (r.Millis / 1000.0),9:F0}");
        }

        // Decode is the query path, and it is not neutral: HC emits fewer, longer matches, so a
        // block it produced reads back at least as fast as a fast-level one. Whatever the size
        // argument decides, the read side never pays for it.
        foreach (var (name, level) in new[] { ladder[0], ladder[2] })
        {
            var (bytes, millis) = DecodeAll(blocks, level);
            _out.WriteLine($"  decode {name,-8}: {raw / 1048576.0 / (millis / 1000.0),6:F0} MB/s ({bytes / 1048576.0:F2} MB compressed)");
        }

        long l06 = results.Single(static r => r.Name == "L06_HC").Bytes;
        long l09 = results.Single(static r => r.Name == "L09_HC").Bytes;
        long l12 = results.Single(static r => r.Name == "L12_MAX").Bytes;

        Assert.True(l06 < fastBytes * 0.92, "HC did not beat the fast level by enough to bother");

        // The rung choice, as a property rather than as a timing: the two levels ABOVE the one
        // the writer uses have nothing left to give. If a codec change ever makes L09 materially
        // smaller than L06, this fails and the constant deserves another look — which is the only
        // reason the ladder is a test and not a paragraph.
        Assert.True(l09 > l06 * 0.99,
            $"L09 is {(1 - l09 / (double)l06) * 100:F1}% smaller than L06 — revisit the rung");
        Assert.True(l12 > l06 * 0.98,
            $"L12 is {(1 - l12 / (double)l06) * 100:F1}% smaller than L06 — revisit the rung");
    }

    /// <summary>
    /// Where a merged file's bytes actually are — and therefore why a 13-27 % smaller payload is
    /// a 2.5-2.8 % smaller file. HC compresses BLOCKS; the inverted, trigram and bloom sections
    /// go to disk exactly as the index builder serialised them, and they are the majority of a
    /// v7 segment.
    ///
    /// <para>MEASURED, 48 000 events, one index group:</para>
    /// <code>
    ///   IdDense    20.49 MB  blocks 21.7 %  inverted 31.4 %  trigram 29.0 %  bloom 17.9 %
    ///   TextDense  39.55 MB  blocks  9.2 %  inverted 42.9 %  trigram 38.6 %  bloom  9.3 %
    /// </code>
    ///
    /// <para>This is the correction to the retired sweep's own advertised "~20-30 % smaller":
    /// that was always the BLOCK payload, and the block payload was never most of the file. It
    /// also names where the bytes would have to be attacked next — the index sections are not
    /// compressed at all, and they are 78-90 % of the file. That is a reader change (offsets
    /// inside a section are seeked, not streamed) and is deliberately not attempted here.</para>
    /// </summary>
    [Fact]
    public void TheBlockPayloadIsAMinorityOfAMergedFile()
    {
        const double MB = 1048576.0;

        foreach (var shape in new[] { Shape.IdDense, Shape.TextDense })
        {
            var paths  = BuildSources(eventsPerSource: 6_000, shape);
            long budget = SegmentWriter.DefaultGroupPayloadBudgetBytes;
            var b = Breakdown(paths, $"break-{shape}", SegmentCompression.Fast, budget);
            var h = Breakdown(paths, $"break-hc-{shape}", SegmentCompression.High, budget);
            _out.WriteLine(
                $"  {shape}, {b.Groups} group(s), {b.Total / MB:F2} MB file:");
            _out.WriteLine(
                $"    blocks {b.Blocks / MB,6:F2} MB ({b.Blocks * 100.0 / b.Total,4:F1}%) | " +
                $"inverted {b.Inverted / MB,6:F2} MB ({b.Inverted * 100.0 / b.Total,4:F1}%) | " +
                $"trigram {b.Trigram / MB,6:F2} MB ({b.Trigram * 100.0 / b.Total,4:F1}%) | " +
                $"bloom {b.Bloom / MB,5:F2} MB ({b.Bloom * 100.0 / b.Total,4:F1}%)");
            _out.WriteLine(
                $"    HC: blocks {h.Blocks / MB,6:F2} MB (-{(1 - h.Blocks / (double)b.Blocks) * 100:F1}% of the payload) " +
                $"=> file {h.Total / MB:F2} MB (-{(1 - h.Total / (double)b.Total) * 100:F1}% of the file)");

            // The claim that decides how much to invest in the block codec, asserted rather than
            // printed: whatever the event shape, most of a merged file is index. Set at half the
            // file against a measured 9-22 %, so it only fires if the balance genuinely inverts.
            Assert.True(b.Blocks * 2 < b.Total,
                $"{shape}: blocks are {b.Blocks * 100.0 / b.Total:F1}% of the file — the payload is now the majority " +
                "and the block codec is worth more attention than this branch gave it");
            Assert.True(h.Blocks < b.Blocks, $"{shape}: HC did not shrink the payload");
        }
    }

    private readonly record struct Sections(int Groups, long Total, long Blocks, long Inverted, long Trigram, long Bloom);

    private Sections Breakdown(List<string> paths, string name, SegmentCompression c, long budget)
    {
        string p = Path.Combine(_dir, name + ".seg");
        using (var source = MergingSegmentEventSource.Open(paths))
        using (var writer = new SegmentWriter(p, budget, c))
        {
            writer.WriteEvents(source, static (count, termsPerEvent) => new SegmentIndexBuilder(count, 5, termsPerEvent), CancellationToken.None);
            writer.Finalise(new NodeId(0), new SegmentId(3UL));
        }

        using var fs = new FileStream(p, FileMode.Open, FileAccess.Read);
        using var br = new BinaryReader(fs);
        fs.Seek(-44, SeekOrigin.End);
        long dirOff = br.ReadInt64();
        br.ReadInt64(); br.ReadInt64();
        long blockIdxOff = br.ReadInt64();

        fs.Seek(dirOff, SeekOrigin.Begin);
        uint groups = br.ReadUInt32();
        long inv = 0, tri = 0, blo = 0;
        for (uint g = 0; g < groups; g++)
        {
            br.ReadUInt32(); br.ReadUInt32(); br.ReadUInt32(); br.ReadUInt32();
            br.ReadInt64();  br.ReadInt64();
            long invOff = br.ReadInt64(), triOff = br.ReadInt64(), bloOff = br.ReadInt64();
            inv += SectionLength(fs, br, invOff);
            tri += SectionLength(fs, br, triOff);
            blo += SectionLength(fs, br, bloOff);
        }

        long total  = fs.Length;
        long blocks = blockIdxOff - 46 - inv - tri - blo;   // everything before the block index that is not a section
        return new Sections((int)groups, total, blocks, inv, tri, blo);

        static long SectionLength(FileStream fs, BinaryReader br, long off)
        {
            if (off <= 0) return 0;
            long save = fs.Position;
            fs.Seek(off, SeekOrigin.Begin);
            long len = br.ReadUInt32() + 4L;
            fs.Seek(save, SeekOrigin.Begin);
            return len;
        }
    }

    private readonly record struct Result(int Events, long Bytes, double Millis);

    private Result Best(List<string> paths, string name, SegmentCompression c)
    {
        var a = Merge(paths, name + "-1", c);
        var b = Merge(paths, name + "-2", c);
        // Same bytes both times; take the faster run — the slower one is scheduler noise.
        Assert.Equal(a.Bytes, b.Bytes);
        return a.Millis <= b.Millis ? a : b;
    }

    private Result Merge(List<string> paths, string name, SegmentCompression compression)
    {
        string merged = Path.Combine(_dir, name + ".seg");
        var sw = Stopwatch.StartNew();
        SegmentInfo info;
        using (var source = MergingSegmentEventSource.Open(paths))
        using (var writer = new SegmentWriter(merged, GroupBudget, compression))
        {
            writer.WriteEvents(source, static (count, termsPerEvent) => new SegmentIndexBuilder(count, 5, termsPerEvent), CancellationToken.None);
            info = writer.Finalise(new NodeId(0), new SegmentId(1UL));
        }
        sw.Stop();
        return new Result((int)info.EventCount, info.CompressedBytes, sw.Elapsed.TotalMilliseconds);
    }

    private static (long Bytes, double Millis) EncodeAll(List<byte[]> blocks, LZ4Level level)
    {
        long total = 0;
        byte[] buf = ArrayPool<byte>.Shared.Rent(LZ4Codec.MaximumOutputSize(1 << 17));
        try
        {
            // One untimed pass so the timed one measures the codec, not the first-touch faults.
            foreach (var b in blocks) LZ4Codec.Encode(b, 0, b.Length, buf, 0, buf.Length, level);

            var sw = Stopwatch.StartNew();
            foreach (var b in blocks) total += LZ4Codec.Encode(b, 0, b.Length, buf, 0, buf.Length, level);
            sw.Stop();
            return (total, sw.Elapsed.TotalMilliseconds);
        }
        finally { ArrayPool<byte>.Shared.Return(buf); }
    }

    /// <summary>Compresses every block at <paramref name="level"/>, then times decoding them all.</summary>
    private static (long Bytes, double Millis) DecodeAll(List<byte[]> blocks, LZ4Level level)
    {
        var comp = new List<byte[]>(blocks.Count);
        long total = 0;
        foreach (var b in blocks)
        {
            var buf = new byte[LZ4Codec.MaximumOutputSize(b.Length)];
            int n = LZ4Codec.Encode(b, 0, b.Length, buf, 0, buf.Length, level);
            comp.Add(buf[..n]);
            total += n;
        }

        byte[] outBuf = ArrayPool<byte>.Shared.Rent(1 << 18);
        try
        {
            double best = double.MaxValue;
            for (int pass = 0; pass < 4; pass++)
            {
                var sw = Stopwatch.StartNew();
                for (int i = 0; i < comp.Count; i++)
                    LZ4Codec.Decode(comp[i], 0, comp[i].Length, outBuf, 0, blocks[i].Length);
                sw.Stop();
                if (pass > 0) best = Math.Min(best, sw.Elapsed.TotalMilliseconds);
            }
            return (total, best);
        }
        finally { ArrayPool<byte>.Shared.Return(outBuf); }
    }

    /// <summary>The blocks a merged segment actually contains, pulled back out of one.</summary>
    private List<byte[]> BlockPayloads(int eventsPerSource, Shape shape)
    {
        var paths  = BuildSources(eventsPerSource, shape);
        string p   = Path.Combine(_dir, $"ladder-{shape}.seg");
        using (var source = MergingSegmentEventSource.Open(paths))
        using (var writer = new SegmentWriter(p, GroupBudget))
        {
            writer.WriteEvents(source, static (count, termsPerEvent) => new SegmentIndexBuilder(count, 5, termsPerEvent), CancellationToken.None);
            writer.Finalise(new NodeId(0), new SegmentId(2UL));
        }

        var blocks = new List<byte[]>();
        using var fs = new FileStream(p, FileMode.Open, FileAccess.Read);
        using var br = new BinaryReader(fs);
        fs.Seek(-44, SeekOrigin.End);
        br.ReadInt64(); br.ReadInt64(); br.ReadInt64();
        long blockIdxOff = br.ReadInt64();
        fs.Seek(blockIdxOff, SeekOrigin.Begin);
        uint count = br.ReadUInt32();
        var offsets = new long[count];
        for (uint i = 0; i < count; i++) { offsets[i] = br.ReadInt64(); br.ReadInt64(); br.ReadUInt32(); }
        foreach (long off in offsets)
        {
            fs.Seek(off, SeekOrigin.Begin);
            int uncomp = (int)br.ReadUInt32();
            int comp   = (int)br.ReadUInt32();
            var raw    = new byte[uncomp];
            Assert.Equal(uncomp, LZ4Codec.Decode(br.ReadBytes(comp), 0, comp, raw, 0, uncomp));
            blocks.Add(raw);
        }
        return blocks;
    }

    /// <summary>
    /// The two ends of what a segment's bytes are made of. Both are prop-dense; they differ in
    /// how much of each event is INDEXED, which is what decides how much of the file a
    /// block-level codec can even reach.
    /// </summary>
    public enum Shape
    {
        /// <summary>
        /// Trace-carrying, high-cardinality ids: a trace id, a span id, a wallet id and a
        /// request id per event, every one distinct. Each is an inverted-index term and a bloom
        /// entry of its own, so the index sections outweigh the payload.
        /// </summary>
        IdDense,

        /// <summary>
        /// A service that logs bodies, not identifiers: one 600-byte request excerpt per event,
        /// no traces, three low-cardinality props. The trigram index grows with the text, but
        /// the payload grows faster.
        /// </summary>
        TextDense,
    }

    private List<string> BuildSources(int eventsPerSource, Shape shape = Shape.IdDense)
    {
        string sub = Path.Combine(_dir, $"src-{shape}-{eventsPerSource}");
        if (Directory.Exists(sub))
            return [.. Directory.GetFiles(sub, "*.seg").OrderBy(static f => f)];

        Directory.CreateDirectory(sub);
        var paths = new List<string>(Sources);
        for (int s = 0; s < Sources; s++) paths.Add(WriteSource(sub, s, eventsPerSource, shape));
        return paths;
    }

    /// <summary>
    /// Prop-dense events of the shape a payments service actually emits: eight properties, an
    /// id-heavy mix of high-entropy tokens (GUID-ish request ids, wallet ids) and repeated
    /// low-entropy ones (routes, regions, status codes), traces on every row and an exception on
    /// one in twenty. The mix is what decides HC's ratio: pure repetition flatters it, pure
    /// entropy defeats it.
    /// </summary>
    private static string WriteSource(string dir, int sourceIndex, int events, Shape shape)
    {
        if (shape == Shape.TextDense) return WriteTextDenseSource(dir, sourceIndex, events);

        var pool = new StringInternPool();
        using var hot = new HotTierSegment(events + 1, (long)events * 1024 + (32L << 20));
        int tmplIdx = pool.Intern("Settlement {Stage} for wallet {Wallet} finished with {Status} in {Elapsed} ms");
        int svcIdx  = pool.Intern("Etisalat.Payments.Settlement");

        var rng = new Random(4242 + sourceIndex);
        string[] stages   = ["authorize", "capture", "reconcile", "refund"];
        string[] routes   = ["/api/pay", "/api/topup", "/api/status", "/api/balance"];
        string[] regions  = ["ae-dxb", "ae-auh", "sa-ruh"];
        string[] statuses = ["Succeeded", "Retried", "Declined"];

        long baseTicks = new DateTimeOffset(2026, 7, 30, 0, 0, 0, TimeSpan.Zero).UtcTicks
                       + sourceIndex * (events / 2L) * TimeSpan.TicksPerMillisecond;

        var buf = new ArrayBufferWriter<byte>(1024);
        for (int i = 0; i < events; i++)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(8);
            w.Write("Stage");            w.Write(stages[rng.Next(stages.Length)]);
            w.Write("Wallet");           w.Write("wallet-" + rng.Next(0, 2_000_000).ToString("D9"));
            w.Write("Status");           w.Write(statuses[rng.Next(statuses.Length)]);
            w.Write("http.route");       w.Write(routes[rng.Next(routes.Length)]);
            w.Write("http.status_code"); w.Write(new[] { 200, 201, 400, 404, 500 }[rng.Next(5)]);
            w.Write("Elapsed");          w.Write(Math.Round(rng.NextDouble() * 500, 2));
            w.Write("region");           w.Write(regions[rng.Next(regions.Length)]);
            w.Write("RequestId");        w.Write("0HN" + rng.NextInt64().ToString("x16"));
            w.Flush();

            var h = new LogEventHeader
            {
                Id                       = new EventId((uint)sourceIndex, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerMillisecond,
                Level                    = (i % 20) == 0 ? LogLevel.Error : LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
                TraceIdHi                = (ulong)rng.NextInt64(),
                TraceIdLo                = (ulong)rng.NextInt64(),
                SpanId                   = (ulong)rng.NextInt64(),
            };
            var exc = (i % 20) == 0 ? Exception(i) : null;
            if (!hot.TryWrite(h, buf.WrittenSpan, null, exc)) break;
        }
        hot.Freeze();

        string path = Path.Combine(dir, $"src-{sourceIndex:D2}.seg");
        using var writer = new SegmentWriter(path);
        writer.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot));
        writer.Finalise(new NodeId(0), new SegmentId((ulong)sourceIndex + 1));
        return path;
    }

    /// <summary>
    /// A service that logs request bodies: field names repeat across every event (what LZ4 lives
    /// on), values vary, and none of it is a high-cardinality index term.
    /// </summary>
    private static string WriteTextDenseSource(string dir, int sourceIndex, int events)
    {
        var pool = new StringInternPool();
        using var hot = new HotTierSegment(events + 1, (long)events * 2048 + (32L << 20));
        int tmplIdx = pool.Intern("Outbound {Verb} to {Host} returned {Status}");
        int svcIdx  = pool.Intern("Etisalat.Payments.Gateway");

        var rng = new Random(99 + sourceIndex);
        string[] verbs  = ["POST", "PUT"];
        string[] hosts  = ["mintroute.ae", "ledger.internal", "kyc.internal"];
        string[] cities = ["Dubai", "Abu Dhabi", "Sharjah", "Riyadh"];

        long baseTicks = new DateTimeOffset(2026, 7, 30, 0, 0, 0, TimeSpan.Zero).UtcTicks
                       + sourceIndex * (events / 2L) * TimeSpan.TicksPerMillisecond;

        var buf = new ArrayBufferWriter<byte>(2048);
        for (int i = 0; i < events; i++)
        {
            string body =
                $$"""
                {"merchantReference":"MR-{{rng.Next(100_000, 999_999)}}","amount":{{rng.Next(10, 90_000)}},
                 "currency":"AED","payer":{"name":"Customer {{rng.Next(1, 5000)}}","city":"{{cities[rng.Next(cities.Length)]}}",
                 "country":"AE"},"items":[{"sku":"TOPUP-{{rng.Next(1, 40)}}","qty":1,"unitPrice":{{rng.Next(5, 500)}}},
                 {"sku":"FEE","qty":1,"unitPrice":{{rng.Next(1, 9)}}}],"channel":"mobile-app","attempt":{{rng.Next(1, 3)}},
                 "notes":"settlement window closes at midnight local time, retry policy is exponential backoff"}
                """;

            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(4);
            w.Write("Verb");    w.Write(verbs[rng.Next(verbs.Length)]);
            w.Write("Host");    w.Write(hosts[rng.Next(hosts.Length)]);
            w.Write("Status");  w.Write(new[] { 200, 201, 502 }[rng.Next(3)]);
            w.Write("Request"); w.Write(body);
            w.Flush();

            var h = new LogEventHeader
            {
                Id                       = new EventId((uint)sourceIndex, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerMillisecond,
                Level                    = LogLevel.Information,
                MessageTemplatePoolIndex = tmplIdx,
                ServiceNamePoolIndex     = svcIdx,
            };
            if (!hot.TryWrite(h, buf.WrittenSpan)) break;
        }
        hot.Freeze();

        string path = Path.Combine(dir, $"src-{sourceIndex:D2}.seg");
        using var writer = new SegmentWriter(path);
        writer.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot));
        writer.Finalise(new NodeId(0), new SegmentId((ulong)sourceIndex + 1));
        return path;
    }

    private static ExceptionInfo Exception(int n) => new()
    {
        Type       = "System.InvalidOperationException",
        Message    = "wallet " + n + " is not in a state that permits settlement",
        StackTrace = string.Join('\n', Enumerable.Range(0, 12).Select(f =>
            $"   at Ameto.Payments.Settlement.Step{f}(WalletContext ctx, Int32 attempt) in /src/Settlement.cs:line {100 + f}")),
        Inner      = new ExceptionInfo { Type = "System.TimeoutException", Message = "ledger did not answer in 30s" },
    };
}
