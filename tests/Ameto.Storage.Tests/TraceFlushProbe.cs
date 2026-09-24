using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Security.Cryptography;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using K4os.Compression.LZ4;
using MessagePack;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT ONE FLUSH COSTS, AND THE PROOF THAT MAKING IT CHEAPER DID NOT MOVE A BYTE ON DISK.
///
/// <para>Two halves, in one file because they measure the same call. The GOLDEN facts pin
/// <c>SpanWriter.Write</c>'s four output files — <c>.trc</c>, <c>.stats</c>, <c>.svcgraph</c>,
/// <c>.tracesum</c> — to SHA-256 constants recorded from the writer as it stood at
/// <c>cb5780e</c>, i.e. BEFORE work package WP5 opened it. Every item of TS#7(a)–(f) claims to be
/// byte-identical on disk; this is where that claim is a claim rather than a hope. The PROBE
/// prints µs/span, B/span and the LOH delta for the whole flush and for each sidecar separately,
/// so the 599 ms the recon measured is attributable rather than a single number.</para>
///
/// <para>DETERMINISM is the whole value of the golden half, so <see cref="BuildCorpus"/> contains
/// no clock, no <c>Guid</c> and no unseeded randomness: fixed nanos, derived ids, arithmetic
/// payloads. The nonce in the segment's FILE NAME is a <c>Guid</c> and that is fine — the hash is
/// over the file's contents, which carry no name.</para>
///
/// <para>The corpus is built to be hostile to every shortcut this package takes:</para>
/// <list type="bullet">
///   <item><b>500 distinct start times over 12 000 spans</b> — 24-way ties, out of order, so the
///     sort really runs and the permutation it picks for ties is pinned. An index sort that
///     produced a different tie order would move the block bytes and this test would say so.</item>
///   <item><b>three blocks</b> (4096 + 4096 + 3808), so the per-block service map, the per-block
///     bloom and the block-index arithmetic are all exercised more than once.</item>
///   <item><b>twelve services, parents crossing them</b> — a non-trivial <c>.svcgraph</c>, and a
///     service index whose blocks are a set rather than a singleton.</item>
///   <item><b>every OTLP value shape</b> — string, int, double, bool, nil, a DUPLICATE key (the
///     resource-then-span shadowing the OTLP mapper emits), a nested map and an array (both box
///     to null), binary, a dictionary-only record with no blob (the <c>WriteAttributes</c>
///     fallback, and <c>short</c>/<c>byte</c>/<c>float</c> with it), a TRUNCATED blob (the
///     <c>TryWalk</c> rejection path), and a span with no attributes at all.</item>
///   <item><b>HTTP semconv on the roots</b> — <c>http.request.method</c> and <c>url.path</c>, so
///     <c>.tracesum</c>'s method/path resolution is in the hash.</item>
/// </list>
///
/// <para>BOTH FORMAT VERSIONS, because they differ exactly where this package works: v3 writes the
/// full trace-index block out of the writer's <c>Dictionary</c> — in the dictionary's INSERTION
/// order — and v4 writes a four-byte stub. A change to how the trace index is accumulated is
/// invisible in a v4 file and loud in a v3 one, and v3 is what <c>DefaultVersion</c> still is.</para>
///
/// <para>If a golden fact fails after a DELIBERATE format change: re-baseline the constant, say so
/// in the commit body, and expect to bump the format version — an old reader will not understand
/// the new bytes. It must never be edited to make a failure go away.</para>
/// </summary>
public sealed class TraceFlushProbe : IDisposable
{
    private const int  Spans        = 12_000;               // 4096 + 4096 + 3808 = three blocks
    private const int  SpansPerTrace = 10;
    private const long BaseNano     = 1_754_049_600_000_000_000L;   // 2025-08-01T12:00:00Z, a constant
    private const int  DistinctTimestamps = 500;            // 24-way ties on every one of them

    private static readonly string[] ServiceNames =
    [
        "wallet-api", "ledger-worker", "payments-gateway", "fraud-scorer",
        "notification-fanout", "auth-edge", "catalog-api", "search-indexer",
        "billing-cron", "webhook-relay", "session-store", "rate-limiter",
    ];

    private static readonly string[] SpanNames =
    [
        "GET /api/v1/orders", "SqlClient.Execute", "HTTP POST", "Kafka.Produce",
        "redis.GET", "validate-token", "score", "fanout",
    ];

    private readonly List<string> _dirs = [];
    private readonly ITestOutputHelper _out;

    public TraceFlushProbe(ITestOutputHelper output) => _out = output;

    public void Dispose()
    {
        foreach (var d in _dirs)
            try { Directory.Delete(d, recursive: true); } catch { /* best effort */ }
    }

    // ── The golden constants ────────────────────────────────────────────────────
    //
    // THE TWO .trc CONSTANTS WERE RE-RECORDED FOR #86, DELIBERATELY, AND ONLY THOSE TWO. The bloom
    // index changed format: its values are hashed from a culture-invariant, case-folded text taken
    // straight off the blob's bytes (SpanBloom), and the section is now empty legacy slots, the
    // "RDB3" marker, the fold table's fingerprint (SpanBloomFold), then the blooms. Nothing else in
    // the file moved, and that is proved rather than hoped: The_flush_changes_nothing_but_the_bloom_index
    // splices the PRE-#86 bloom section (LegacySpanBloom, the old writer's feed) into today's file
    // and gets the old constants below back, byte for byte. The three sidecars do not carry a bloom
    // and did not move at all. (Recorded twice on the branch: 2196470F…/C0E6962B… under the unshipped
    // "RDB2" layout, then these, +8 bytes each for the fingerprint — the blooms themselves identical.)
    //
    // History of the old constants: SHA-256 of each file SpanWriter.Write produced for
    // BuildCorpus() at cb5780e — the merge of wave 1, the writer as WP5 found it — recomputed at
    // 4cb9686 under the INVARIANT culture; the two .trc constants were first recorded under ru-KZ
    // and read D1BD639A… and FBFA3819…, because the old bloom hashed `value.ToString()` in the
    // CURRENT culture.

    private const string V3Trc      = "8B8066F8B9418B62903432641EADBEEDAAF4FD7F80996BD3236A260FB99E4178";
    private const string V3Stats    = "FAF48AB485397E0F3B65E3ADCE5413D6B8702E8AB8E3A044214D5CD0CB79C423";
    private const string V3SvcGraph = "44BE43D931468E9C74F5177BA89CAA5D252FA87FDF744AA98BA4F848C56C5A9F";
    private const string V3TraceSum = "BB5C2FD272B3D86F3DB50870847F530601D234AE373A33D4AD0FD524FA54C773";
    private const string V4Trc      = "0FEAA5E8BD06A15F738E90580E2EB536E2022B40A83BD7C273007B59DABFAEB4";

    /// <summary>The pre-#86 <c>.trc</c> constants, as recorded under the invariant culture.</summary>
    private const string Pre86V3Trc = "81564E1FC2731519ABB87046EAB30D2EE23DDF5B7C96839261009FFDB992AA11";
    private const string Pre86V4Trc = "7D35320374B43D8BB9F8F3314B2B55A4137583772D5C182C927B9CA5C2868880";

    // THE HASHES NO LONGER DEPEND ON THE MACHINE'S CULTURE, and every golden fact runs under four
    // ambient cultures to say so: ru-KZ and sv-SE (a decimal comma; sv-SE's minus is U+2212), en-US
    // and the invariant one. The corpus has doubles (`sampling.ratio`, `latency.ms`), floats
    // (`backoff.seconds`) and negative integers (`clock.skew.ns`) — every value whose
    // `ToString()` differs between those cultures. Until #86 the flush had to be PINNED to the
    // invariant culture here, and the comment said "this pins the TEST, not the product"; the pin
    // is gone because the product no longer needs it. Put `value.ToString()` back into SpanBloom
    // and the ru-KZ and sv-SE rows go red.

    [Theory]
    [InlineData("ru-KZ")]
    [InlineData("en-US")]
    [InlineData("sv-SE")]
    [InlineData("")]
    public void The_flush_produces_the_recorded_bytes_v3(string ambientCulture)
    {
        // The literal, not SpanWriter.DefaultVersion: this fact pins the v3 file, and a change to
        // the default must not quietly turn it into a second v4 fact.
        var h = UnderCulture(ambientCulture, () => FlushAndHash(3, "golden-v3"));

        _out.WriteLine($".trc      {h.TrcLength,10:N0} B  {h.Trc}");
        _out.WriteLine($".stats    {h.StatsLength,10:N0} B  {h.Stats}");
        _out.WriteLine($".svcgraph {h.SvcGraphLength,10:N0} B  {h.SvcGraph}");
        _out.WriteLine($".tracesum {h.TraceSumLength,10:N0} B  {h.TraceSum}");

        Assert.Equal(V3Trc,      h.Trc);
        Assert.Equal(V3Stats,    h.Stats);
        Assert.Equal(V3SvcGraph, h.SvcGraph);
        Assert.Equal(V3TraceSum, h.TraceSum);
    }

    /// <summary>
    /// The v4 <c>.trc</c> only — the three sidecars do not know the format version and are already
    /// pinned above, so hashing them twice would pin nothing new.
    /// </summary>
    [Theory]
    [InlineData("ru-KZ")]
    [InlineData("en-US")]
    [InlineData("sv-SE")]
    [InlineData("")]
    public void The_flush_produces_the_recorded_bytes_v4(string ambientCulture)
    {
        var h = UnderCulture(ambientCulture, () => FlushAndHash(4, "golden-v4"));
        _out.WriteLine($".trc      {h.TrcLength,10:N0} B  {h.Trc}");
        Assert.Equal(V4Trc, h.Trc);
    }

    /// <summary>
    /// #86 CHANGED THE BLOOM INDEX AND NOTHING ELSE, and this is where that sentence is a fact. Take
    /// today's file, replace its bloom section with the one the pre-#86 writer produced for the same
    /// corpus (<see cref="LegacySpanBloom"/>, under the invariant culture the old constants were
    /// recorded in), and the result is the pre-#86 file — its original SHA-256. So the span blocks,
    /// the trace and service indices and the footer are the old bytes, which means the blob walk
    /// that now feeds the bloom accepted and rejected every blob of the corpus (the truncated one
    /// included) exactly as the one it replaced: a different verdict copies different bytes into the
    /// block. It also certifies <see cref="LegacySpanBloom"/> as the old writer, which is what the
    /// legacy-read tests in <c>SpanBloomCanonicalTests</c> stand on.
    /// </summary>
    [Theory]
    [InlineData(3, Pre86V3Trc)]
    [InlineData(4, Pre86V4Trc)]
    public void The_flush_changes_nothing_but_the_bloom_index(ushort version, string pre86)
    {
        string dir  = NewDir($"pre86-v{version}");
        var corpus  = BuildCorpus();
        string trc  = UnderCulture("ru-KZ", () => SpanWriter.Write(dir, corpus, version: version).FilePath);

        LegacySpanBloom.Rewrite(trc, corpus, culture: "");
        string sha = Sha(trc);
        _out.WriteLine($"v{version} with the pre-#86 bloom section: {sha}");
        Assert.Equal(pre86, sha);
    }

    /// <summary>
    /// THE ONE STRUCTURAL FACT THE HASHES CANNOT STATE: that the segment is sorted by start time,
    /// and that spans which TIE on it keep a stable, reproducible order. The hashes above would
    /// catch a change to it, but they would report "the bytes moved" — this says which bytes and
    /// why, which is the difference between a five-minute diagnosis and an afternoon.
    /// </summary>
    [Fact]
    public void The_segment_is_sorted_by_start_time_and_ties_are_reproducible()
    {
        var corpus = BuildCorpus();
        string dirA = NewDir("order-a");
        string dirB = NewDir("order-b");

        var a = SpanReader.ReadAll(SpanWriter.Write(dirA, corpus).FilePath);
        var b = SpanReader.ReadAll(SpanWriter.Write(dirB, BuildCorpus()).FilePath);

        Assert.Equal(Spans, a.Count);
        for (int i = 1; i < a.Count; i++)
            Assert.True(a[i - 1].StartTimeUnixNano <= a[i].StartTimeUnixNano,
                $"span {i} starts before span {i - 1} — the flush did not sort");

        // Two independent runs over two independently built copies of the same corpus must lay the
        // ties down the same way, or the file is not a function of its input.
        for (int i = 0; i < a.Count; i++)
            Assert.Equal(a[i].SpanId.RawValue, b[i].SpanId.RawValue);
    }

    /// <summary>
    /// THE BATCH A FLUSH IS HANDED IS NOT THE FLUSH'S TO REORDER, and this is the fact that makes
    /// TS#7(a) safe rather than merely cheap.
    ///
    /// <para>The plan says to sort the caller's list in place "because <c>CompleteFlush</c> hands
    /// over a detached snapshot". It is detached from the HOT TIER, and from nothing else:
    /// <c>TraceStorageEngine</c> parks the same reference in <c>_flushingSpans</c> for the whole
    /// of the write and serves the by-trace lookup, the trace list, the volume sparkline, the
    /// per-service stats and the service graph out of it while the flush runs — under the engine
    /// READ lock, which the flush thread does not hold. A <c>List&lt;T&gt;.Sort</c> under those
    /// readers bumps the list's version stamp and faults every live enumerator with "Collection
    /// was modified"; the ones that got their element first would read a half-permuted tier.</para>
    ///
    /// <para>So the writer sorts a permutation instead, and this says so in a way that a later
    /// "optimisation" back to an in-place sort cannot pass.</para>
    /// </summary>
    [Fact]
    public void The_flush_does_not_reorder_the_batch_it_is_given()
    {
        var corpus = BuildCorpus();
        var before = new ulong[corpus.Count];
        for (int i = 0; i < corpus.Count; i++) before[i] = corpus[i].SpanId.RawValue;

        SpanWriter.Write(NewDir("untouched"), corpus);

        for (int i = 0; i < corpus.Count; i++)
            Assert.Equal(before[i], corpus[i].SpanId.RawValue);
    }

    /// <summary>
    /// TS#7(a)'S BYTE-IDENTITY ARGUMENT, AT THE SIZE THE ARGUMENT IS ABOUT — 50 000 spans, the
    /// production flush threshold, where the copy the item removes is a 400 KB LOH array.
    ///
    /// <para>Introsort is UNSTABLE, so a tier full of ties (and this corpus is: 50 000 spans over
    /// 500 start times) has its order decided by the algorithm's swap sequence and not by the
    /// data. The claim is that sorting <c>int[] {0..n-1}</c> with a comparison that reads a
    /// parallel key array performs exactly the comparisons the old <c>List&lt;SpanRecord&gt;.Sort</c>
    /// performed, in the same sequence, over the same entry point — so the permutation is the same
    /// one. That is a claim about the BCL, which is why it is asserted against the BCL rather than
    /// reasoned about: the reference order below is produced by the code this item deleted.</para>
    ///
    /// <para>It prints what that deleted copy allocates, as a same-run control — the figure is
    /// what TS#7(a) buys, measured on the machine that is reading it.</para>
    /// </summary>
    [Fact]
    public void The_index_sort_picks_the_same_order_the_deleted_copy_did()
    {
        const int Large = 50_000;
        var corpus = BuildCorpus(Large);

        // THE DELETED CODE, VERBATIM, as the reference: this is SpanWriter.Write's first three
        // lines at cb5780e. Measured while it runs, because what it costs IS the item.
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        long loh0 = LohAfterLastFullGc();
        long a0   = GC.GetAllocatedBytesForCurrentThread();
        var ordered = new List<SpanRecord>(corpus);
        ordered.Sort(static (a, b) => a.StartTimeUnixNano.CompareTo(b.StartTimeUnixNano));
        long copyAlloc = GC.GetAllocatedBytesForCurrentThread() - a0;
        long copyLoh   = LohAllocatedSince(loh0);

        _out.WriteLine($"the copy TS#7(a) removed: {copyAlloc:N0} B allocated, "
                     + $"{copyLoh:N0} B of it LOH, for {Large:N0} spans "
                     + $"({copyAlloc / (double)Large:N1} B/span)");

        var written = SpanReader.ReadAll(SpanWriter.Write(NewDir("perm"), corpus).FilePath);

        Assert.Equal(ordered.Count, written.Count);
        for (int i = 0; i < ordered.Count; i++)
            Assert.Equal(ordered[i].SpanId.RawValue, written[i].SpanId.RawValue);
    }

    /// <summary>
    /// TS#7(b)'S PREMISE, STATED AS A FACT ABOUT THE FILE: a service's block list is ascending and
    /// carries each block ONCE.
    ///
    /// <para>The <c>SortedSet</c> the item removes gave both properties for free; a
    /// <c>List&lt;uint&gt;</c> with a "did I already record this block" guard gives them only
    /// because block indices are handed to <c>WriteBlock</c> in ascending order and are constant
    /// within a block. That is a property of the WALK, not of the type, so it belongs in a test —
    /// a guard that compared against the FIRST element, or one that was dropped entirely, would
    /// produce a service index with 4 096 copies of every block index in it and no reader would
    /// complain: it would simply read the same block four thousand times per query.</para>
    ///
    /// <para>The corpus is chosen so a service is ABSENT from a middle block: `ghost` is in blocks
    /// 0 and 2 only, which is the case a "last block seen" guard gets right and a "first block
    /// seen" one does not.</para>
    /// </summary>
    [Fact]
    public void Each_service_lists_its_blocks_once_and_in_ascending_order()
    {
        // 9 000 spans → blocks 0 (0..4095), 1 (4096..8191), 2 (8192..8999).
        const int N = 9_000;
        var corpus = new List<SpanRecord>(N);
        for (int i = 0; i < N; i++)
        {
            // Sorted on the way in, so position i is block i / 4096 and the shape below is exact.
            bool ghost = i < 4096 || i >= 8192;
            corpus.Add(new SpanRecord
            {
                TraceId           = new TraceId(0, (ulong)i),
                SpanId            = new SpanId((ulong)i + 1),
                StartTimeUnixNano = BaseNano + i,
                DurationNanos     = 1_000_000,
                Name              = "op",
                ServiceName       = ghost ? "ghost" : "everywhere",
            });
        }
        // "everywhere" must be in all three blocks: give it one span in each.
        corpus[0]     = Respan(corpus[0],     "everywhere");
        corpus[8192]  = Respan(corpus[8192],  "everywhere");

        string path = SpanWriter.Write(NewDir("svcidx"), corpus).FilePath;
        var index = ReadServiceIndex(path);

        Assert.Equal(new uint[] { 0, 1, 2 }, index["everywhere"]);
        Assert.Equal(new uint[] { 0, 2 },    index["ghost"]);
    }

    private static SpanRecord Respan(SpanRecord s, string service) => new()
    {
        TraceId           = s.TraceId,
        SpanId            = s.SpanId,
        ParentSpanId      = s.ParentSpanId,
        StartTimeUnixNano = s.StartTimeUnixNano,
        DurationNanos     = s.DurationNanos,
        Name              = s.Name,
        ServiceName       = service,
        Kind              = s.Kind,
        Status            = s.Status,
        HttpStatusCode    = s.HttpStatusCode,
    };

    /// <summary>
    /// The <c>.trc</c> service index, straight out of the file: the footer's last 28 bytes carry
    /// the section offsets, and the service index runs from its own offset to the bloom index's.
    /// Read here rather than through <c>SpanReader</c> because no reader exposes the raw block
    /// lists, and the block lists are the thing under test.
    /// </summary>
    private static Dictionary<string, uint[]> ReadServiceIndex(string path)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        using var br = new BinaryReader(fs);

        fs.Position = fs.Length - 28;
        br.ReadUInt64();                      // traceIdxOffset
        ulong svcIdxOffset = br.ReadUInt64();

        fs.Position = (long)svcIdxOffset;
        uint serviceCount = br.ReadUInt32();
        var result = new Dictionary<string, uint[]>(StringComparer.Ordinal);
        for (uint s = 0; s < serviceCount; s++)
        {
            string name = System.Text.Encoding.UTF8.GetString(br.ReadBytes(br.ReadUInt16()));
            uint blockCount = br.ReadUInt32();
            var blocks = new uint[blockCount];
            for (uint b = 0; b < blockCount; b++) blocks[b] = br.ReadUInt32();
            result[name] = blocks;
        }
        return result;
    }

    /// <summary>
    /// TS#7(d)'S FAILURE MODE, NAMED: the span→service map the <c>.svcgraph</c> resolves parents
    /// through is now built inside the writer's BLOCK pass, and a block pass is a natural place to
    /// reset per-block state. A map that were reset per block would still produce a plausible
    /// service graph — every edge whose two ends fall in the same block survives — and would
    /// silently drop every edge that crosses a block boundary, which on a 4 096-span block is
    /// every long trace in the segment.
    ///
    /// <para>So: 5 000 spans over two blocks, and every child in block 1 has its parent in block
    /// 0, in a different service. The one edge must carry all of them.</para>
    /// </summary>
    [Fact]
    public void The_service_graph_resolves_parents_from_an_earlier_block()
    {
        const int N = 5_000;                    // block 0 = 0..4095, block 1 = 4096..4999
        const int Children = N - 4096;
        var corpus = new List<SpanRecord>(N);
        for (int i = 0; i < N; i++)
        {
            bool child = i >= 4096;
            corpus.Add(new SpanRecord
            {
                TraceId           = new TraceId(0, (ulong)(i % 400)),
                SpanId            = new SpanId((ulong)i + 1),
                // Every child's parent is span 1 — the very first span of block 0.
                ParentSpanId      = child ? new SpanId(1) : default,
                StartTimeUnixNano = BaseNano + i,
                DurationNanos     = 3_000_000,
                Name              = "op",
                ServiceName       = child ? "downstream" : "upstream",
            });
        }

        string path = SpanWriter.Write(NewDir("svcgraph-blocks"), corpus).FilePath;
        var edges = ServiceGraphSidecar.ReadEdges(path);

        var edge = Assert.Single(edges);
        Assert.Equal("upstream",   edge.From);
        Assert.Equal("downstream", edge.To);
        Assert.Equal((uint)Children, edge.CallCount);
    }

    /// <summary>
    /// TS#7(f): WHAT THE SUMMARY SIDECAR LEAVES BEHIND ON THE RECORDS IT READ — which, before this
    /// item, was a decoded attribute dictionary per ROOT SPAN, and the reason the item is about
    /// resident memory rather than about microseconds.
    ///
    /// <para><c>GetAttr(s.Attributes, …)</c> makes the ask the first touch of the record's blob, so
    /// the lazy decode runs right there — a Dictionary, a key string and a box per attribute, ~987 B
    /// against the blob's 375 B for an eight-attribute span — and <c>SpanRecord</c> MEMOISES it. The
    /// records are the flush snapshot, which <c>TraceStorageEngine._flushingSpans</c> holds and
    /// serves queries from until the flush publishes, so every one of those dictionaries stays live
    /// for the rest of the flush. <c>HttpSemconvKeys.Resolve</c> answers both questions from the
    /// bytes and attaches nothing.</para>
    ///
    /// <para>THE GATE IS THE MEMO ITSELF, NOT A HEAP FIGURE. What the item removes is a decoded
    /// dictionary left attached to a record, so the fact asserted is that no record with a blob
    /// has been decoded — read off <c>SpanRecord</c>'s own memo flag, which is exactly what the
    /// lazy accessor sets and exactly what pins the dictionary. A retained-bytes gate (two full
    /// compacting collects, subtracted) was tried first and is printed below as the figure the
    /// item is about, but it is process-wide: on this machine it read 0 B and 262 KB for the fixed
    /// code in two sessions, and 397 KB and 659 KB for the defect. A gate with that much noise in it
    /// would fail on an unrelated test's finaliser, or pass beside a real regression.</para>
    ///
    /// <para>The corpus is FRESH — a record that has already been asked for its attributes has
    /// already paid, and would hide the whole effect.</para>
    /// </summary>
    [Fact]
    public void The_summary_sidecar_leaves_no_decoded_attributes_on_the_records()
    {
        // Private by design — SpanRecord exposes no "have you decoded" and should not grow one for
        // a test. Found by name and asserted found, so a rename fails HERE, loudly, rather than
        // turning the gate below into a count of nothing.
        var decodedFlag = typeof(SpanRecord).GetField("_decoded", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(decodedFlag);

        // Warm the JIT on a throwaway corpus, so the measured write is not paying for it.
        TraceSummarySidecar.Write(Path.Combine(NewDir("sum-warm"), "w.trc"), BuildCorpus(2_000));

        var fresh = BuildCorpus();
        string path = Path.Combine(NewDir("sum-retained"), "p.trc");

        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        long before = GC.GetTotalMemory(forceFullCollection: true);
        long a0     = GC.GetAllocatedBytesForCurrentThread();

        TraceSummarySidecar.Write(path, fresh);

        long allocated = GC.GetAllocatedBytesForCurrentThread() - a0;
        long retained  = GC.GetTotalMemory(forceFullCollection: true) - before;
        GC.KeepAlive(fresh);   // the point of the measurement: the snapshot is still live

        int roots = 0, rootsWithBlob = 0, decoded = 0;
        foreach (var s in fresh)
        {
            if (s.ParentSpanId.IsEmpty) roots++;
            if (s.AttributesBytes.IsEmpty) continue;   // no blob: its dictionary was handed in, not decoded
            if (s.ParentSpanId.IsEmpty) rootsWithBlob++;
            if ((bool)decodedFlag.GetValue(s)!) decoded++;
        }

        _out.WriteLine($".tracesum over {Spans:N0} fresh spans ({roots:N0} roots, {rootsWithBlob:N0} with a blob): "
                     + $"{allocated:N0} B allocated, {retained:N0} B still attached afterwards, "
                     + $"{decoded:N0} records decoded");

        // Every root with a blob is a record the sidecar reads method and path from; before this
        // item each of them came out of the write carrying a decoded dictionary.
        Assert.True(rootsWithBlob > 0, "the corpus has no root with a blob — the gate below would be vacuous");
        Assert.Equal(0, decoded);
    }

    /// <summary>
    /// SHA-256 of the <c>.tracesum</c> <see cref="BuildOversizedSummaryCorpus"/> produces, recorded
    /// from the writer at <c>76a6a08</c> — two MemoryStreams, a CopyTo, a ToArray — immediately
    /// before TS#7(e) replaced that body writer.
    /// </summary>
    private const string OversizedTraceSum = "116F2A5168E4701CDA311CC1A840033C67F1856A36875653795299AB898CA411";

    /// <summary>
    /// TS#7(e)'S BYTE-IDENTITY WHERE THE GOLDEN CORPUS CANNOT REACH IT. The <c>.tracesum</c> body is
    /// written front to back into one rented buffer sized from the trace count, and the golden
    /// corpus fits the first rental — so the golden hash never sees the buffer GROW, and never sees
    /// the byte cuts the old <c>GetBytes(s)[..max]</c> made, because none of its strings is long
    /// enough to be cut.
    ///
    /// <para>This corpus reaches both: root names and paths of tens of kilobytes (the body outgrows
    /// its first rental about ten times over), a method one byte past the 255-byte cut with a
    /// two-byte character straddling it, a path past the 65 535-byte cut with a three-byte
    /// character straddling THAT, a lone surrogate (UTF-8's replacement bytes), multi-byte service
    /// names, an empty service name (pool index -1), a trace with no root at all — and a first
    /// trace whose children arrive before its root, so the service pool is only right if it is
    /// interned in the ROW's order (root first) rather than the order the set met the names. In the
    /// golden corpus every root is its trace's earliest span and the two orders coincide.</para>
    /// </summary>
    [Fact]
    public void The_summary_body_keeps_its_bytes_past_its_first_buffer_and_at_every_cut()
    {
        string trc = Path.Combine(NewDir("sum-oversized"), "p.trc");
        TraceSummarySidecar.Write(trc, BuildOversizedSummaryCorpus());

        string sum = Path.ChangeExtension(trc, ".tracesum");
        string hash = Sha(sum);
        _out.WriteLine($".tracesum {new FileInfo(sum).Length,10:N0} B  {hash}");

        // Readable end to end — the hash says "the same bytes as before", this says those bytes
        // are still a file: every row, the rootless one included.
        Assert.Equal(OversizedTraces + 1, TraceSummarySidecar.ReadSummaries(trc).Count);
        Assert.Equal(OversizedTraceSum, hash);
    }

    private const int OversizedTraces = 24;

    private static List<SpanRecord> BuildOversizedSummaryCorpus()
    {
        string[] services = ["сервис-платежей", "ledger", "δ-service", ""];
        var spans = new List<SpanRecord>();

        for (int t = 0; t < OversizedTraces; t++)
        {
            var traceId = new TraceId(0xB0B0_0000_0000_0000UL | (uint)t, 0x5EED_0000_0000_0000UL ^ (ulong)t);
            var rootId  = new SpanId(0x1000UL + (ulong)t * 16);

            string method = (t % 3) switch
            {
                0 => new string('M', 254) + "é",        // 256 bytes: the 255-byte cut splits the é
                1 => "GET",
                _ => "\uD800X",                         // lone surrogate: EF BF BD on the way out
            };
            string path = t % 2 == 0
                ? new string('p', 65_534) + "€"         // 65 537 bytes: the cut splits the €
                : "/short/" + t;

            var root = new SpanRecord
            {
                TraceId           = traceId,
                SpanId            = rootId,
                StartTimeUnixNano = BaseNano + t * 1_000L,
                DurationNanos     = 5_000_000L + t,
                Name              = "корень-" + new string('н', 20_000 + t),   // ~40 KB of two-byte text
                ServiceName       = services[t % 3],
                Status            = t % 5 == 0 ? SpanStatusCode.Error : SpanStatusCode.Ok,
                HttpStatusCode    = (short)(200 + t),
                Attributes        = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["http.request.method"] = method,
                    ["url.path"]            = path,
                },
            };

            // THE FIRST TRACE LISTS ITS CHILDREN BEFORE ITS ROOT, and its three service names are
            // new to the pool. Its service set therefore meets them child-first while its row
            // interns root-first — the one ordering the pre-pass must reproduce and the one no
            // other trace here would notice getting wrong.
            if (t != 0) spans.Add(root);
            for (int c = 1; c <= 2; c++)
                spans.Add(new SpanRecord
                {
                    TraceId           = traceId,
                    SpanId            = new SpanId(rootId.RawValue + (ulong)c),
                    ParentSpanId      = rootId,
                    StartTimeUnixNano = BaseNano + t * 1_000L + c,
                    DurationNanos     = 1_000_000L,
                    Name              = "child",
                    ServiceName       = services[(t + c) % services.Length],   // "" on some
                });
            if (t == 0) spans.Add(root);
        }

        // A trace with no root in this segment: its row takes the EARLIEST span's service.
        var orphan = new TraceId(0xDEAD_0000_0000_0000UL, 1);
        for (int c = 0; c < 3; c++)
            spans.Add(new SpanRecord
            {
                TraceId           = orphan,
                SpanId            = new SpanId(0xF000UL + (ulong)c),
                ParentSpanId      = new SpanId(0xEEEEUL),
                StartTimeUnixNano = BaseNano + 900_000L - c,
                DurationNanos     = 2_000_000L,
                Name              = "orphan",
                ServiceName       = services[c],
            });

        return spans;
    }

    // ── The probe ───────────────────────────────────────────────────────────────

    /// <summary>
    /// µs/span, B/span and LOH for one flush, split so the cost is attributable: the whole
    /// <c>SpanWriter.Write</c>, then each sidecar measured on its own call, then the <c>.trc</c>
    /// itself by difference. The v4 run is printed beside the v3 one because their only difference
    /// is the trace-index BLOCK, which isolates what writing that block costs from what
    /// accumulating the map costs.
    ///
    /// <para>ALLOCATION IS PER-THREAD (<c>GC.GetAllocatedBytesForCurrentThread</c>): the flush is
    /// synchronous here, so every byte of it is billed to this thread and xUnit's output drain on
    /// another one cannot get into the figure. The LOH figure is what the window put on the
    /// large-object heap (see <see cref="LohAllocatedSince"/>) and is process-wide — there is no
    /// per-thread equivalent — so it admits whatever else the runtime does in the window; it is
    /// printed as a trend, never asserted.</para>
    /// </summary>
    [Fact]
    public void Flush_cost_per_span()
    {
        // Warm: JIT the writer, the sidecars, msgpack and LZ4 before anything is measured — on a
        // corpus of its own. See Measure for why no measured call may share it.
        var warm = BuildCorpus();
        SpanWriter.Write(NewDir("warm"), warm);
        ServiceGraphSidecar.Write(Path.Combine(NewDir("warm-g"), "w.trc"), warm);
        TraceSummarySidecar.Write(Path.Combine(NewDir("warm-s"), "w.trc"), warm);

        var whole   = Measure("SpanWriter.Write  (v3)", static (d, c) => SpanWriter.Write(d, c));
        var wholeV4 = Measure("SpanWriter.Write  (v4)", static (d, c) => SpanWriter.Write(d, c, version: SpanWriter.NewestVersion));
        var graph   = Measure(".svcgraph", static (d, c) => ServiceGraphSidecar.Write(Path.Combine(d, "p.trc"), c));
        var summary = Measure(".tracesum", static (d, c) => TraceSummarySidecar.Write(Path.Combine(d, "p.trc"), c));

        _out.WriteLine($"FLUSH of {Spans:N0} spans, {SpansPerTrace} spans/trace, {ServiceNames.Length} services");
        _out.WriteLine("");
        Print("total (v3)", whole);
        Print("total (v4)", wholeV4);
        Print("  .svcgraph", graph);
        Print("  .tracesum", summary);
        Print("  .trc+.stats (by difference)", new Sample(
            whole.Micros    - graph.Micros    - summary.Micros,
            whole.Allocated - graph.Allocated - summary.Allocated,
            whole.Loh       - graph.Loh       - summary.Loh));
        _out.WriteLine("");
        _out.WriteLine($"trace-index block (v3 − v4)   {whole.Micros - wholeV4.Micros,10:N0} us   "
                     + $"{(whole.Allocated - wholeV4.Allocated) / (double)Spans,8:N1} B/span");

        // NOT A GATE. Every number above is machine- and configuration-dependent (Debug doubles the
        // wall time), and a probe that fails on a slow agent teaches nothing. The byte-identity
        // facts above are the gate; this is the instrument.
        Assert.True(whole.Allocated > 0);
    }

    /// <summary>
    /// TS#7(g), MEASURED AND NOT ADOPTED. The plan's instruction was "measure the file-size delta
    /// first; do not adopt blind": the flush compresses every span block (and v3's trace-index
    /// block) at <c>LZ4Level.L09_HC</c>, and the proposal was L03_HC for the flush — on the critical
    /// path for releasing tier memory — keeping HC for compaction. This is the measurement, kept so
    /// the decision can be re-taken on numbers rather than re-argued.
    ///
    /// <para>Measured on the writer's OWN bytes rather than a model of them: the segment is written
    /// as a flush writes it, every LZ4 section is read back out of the file and inflated, and each
    /// is re-pickled at every candidate level. So the sizes are exactly what that level puts on disk
    /// for the corpus, section for section, and the times are the compressor's alone (best of three
    /// passes; K4os ships Release-built, so this holds in a Debug test run too).</para>
    ///
    /// <para>THREE CORPORA, because the answer depends on the data. The golden one is synthetic and
    /// repetitive. The varied one gives every span its own statement parameters, a 32-hex-digit URL
    /// id, a user id, a latency and a port — high entropy, where HC's search is most expensive and
    /// finds least. The resource-shaped one is the stored shape <c>SpanFormatV3Tests</c> uses —
    /// resource attributes merged onto EVERY span, a route per root — and it is where the levels
    /// part: long matches repeated across spans are what L09_HC's deeper search exists to find.
    /// MEASURED: L03_HC is +1,9-2,1 % on the golden corpus and +1,3 % on the varied one, but +7,1 %
    /// on the resource-shaped one, which also pushed <c>V3_IsMuchSmallerThanV2</c> over its bound.
    /// Material, so the flush stays at L09_HC.</para>
    ///
    /// <para>A PROBE, NOT A GATE: the only assertion is that the instrument reads the file it
    /// measures (L09_HC re-pickled is what is on disk).</para>
    /// </summary>
    [Fact]
    public void Lz4_level_of_the_flush_blocks_measured()
    {
        MeasureLz4Levels("golden",   BuildCorpus(Spans));
        MeasureLz4Levels("golden",   BuildCorpus(50_000));
        MeasureLz4Levels("varied",   BuildVariedCorpus(50_000));
        MeasureLz4Levels("resource", BuildResourceShapedCorpus(50_000));
    }

    private void MeasureLz4Levels(string corpusName, List<SpanRecord> corpus)
    {
        LZ4Level[] levels = [LZ4Level.L09_HC, LZ4Level.L03_HC, LZ4Level.L00_FAST];
        int n = corpus.Count;

        string trc = SpanWriter.Write(NewDir($"lz4-{corpusName}-{n}"), corpus).FilePath;
        var raws   = ReadCompressedSections(trc, out long onDisk);
        long fileBytes = new FileInfo(trc).Length;

        _out.WriteLine($"{corpusName} {n:N0} spans: {raws.Count} LZ4 sections (span blocks + trace index), "
                     + $"{onDisk:N0} B of a {fileBytes:N0} B .trc");
        long hcBytes = 0;
        foreach (var level in levels)
        {
            long bytes  = 0;
            double best = double.MaxValue;
            for (int pass = 0; pass < 3; pass++)
            {
                long b = 0;
                var sw = Stopwatch.StartNew();
                foreach (var raw in raws) b += LZ4Pickler.Pickle(raw, level).Length;
                sw.Stop();
                bytes = b;
                best  = Math.Min(best, sw.Elapsed.TotalMicroseconds);
            }
            if (level == LZ4Level.L09_HC) hcBytes = bytes;

            long file = fileBytes - hcBytes + bytes;
            _out.WriteLine($"  {level,-9} {bytes,11:N0} B  .trc {file,11:N0} B "
                         + $"({(file - fileBytes) * 100.0 / fileBytes,5:+0.0;-0.0} %)  "
                         + $"{best / 1000.0,7:N1} ms  {best / n,5:N2} us/span");
        }

        Assert.Equal(onDisk, hcBytes);
    }

    /// <summary>
    /// A higher-entropy corpus for the LZ4 measurement only — see
    /// <see cref="Lz4_level_of_the_flush_blocks_measured"/>. Seeded, so the same every run; not part
    /// of any golden fact.
    /// </summary>
    private static List<SpanRecord> BuildVariedCorpus(int howMany)
    {
        var rnd   = new Random(42);
        var spans = new List<SpanRecord>(howMany);
        var buf   = new ArrayBufferWriter<byte>(512);
        string[] ops = ["GET /api/v1/orders/{id}", "POST /api/v1/payments", "SqlClient.Execute", "redis.GET", "Kafka.Produce"];

        for (int i = 0; i < howMany; i++)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(8);
            w.Write("db.system");        w.Write(i % 3 == 0 ? "mssql" : "postgresql");
            w.Write("db.statement");     w.Write($"SELECT * FROM Orders WHERE CustomerId = {rnd.Next(1_000_000)} AND Status = {rnd.Next(5)}");
            w.Write("net.peer.port");    w.Write((long)rnd.Next(1024, 65535));
            w.Write("http.url");         w.Write($"https://api.example.com/v1/orders/{rnd.NextInt64():x16}{rnd.NextInt64():x16}?page={rnd.Next(100)}");
            w.Write("user.id");          w.Write(rnd.NextInt64().ToString("x16"));
            w.Write("latency.ms");       w.Write(rnd.NextDouble() * 1000);
            w.Write("http.status_code"); w.Write((long)(rnd.Next(10) == 0 ? 500 : 200));
            w.Write("deployment.env");   w.Write("production");
            w.Flush();

            spans.Add(new SpanRecord
            {
                TraceId           = new TraceId((ulong)rnd.NextInt64(), (ulong)(i / 8)),
                SpanId            = new SpanId((ulong)rnd.NextInt64() | 1),
                ParentSpanId      = i % 8 == 0 ? default : new SpanId((ulong)rnd.NextInt64() | 1),
                StartTimeUnixNano = BaseNano + (long)i * 1_000_000 + rnd.Next(1_000_000),
                DurationNanos     = rnd.Next(1, 2_000_000_000),
                Name              = ops[rnd.Next(ops.Length)],
                ServiceName       = ServiceNames[rnd.Next(ServiceNames.Length)],
                Kind              = (SpanKind)rnd.Next(6),
                Status            = rnd.Next(20) == 0 ? SpanStatusCode.Error : SpanStatusCode.Ok,
                AttributesBytes   = buf.WrittenSpan.ToArray(),
            });
        }
        return spans;
    }

    /// <summary>
    /// The shape <c>SpanFormatV3Tests</c> calls "the real stored shape", for the LZ4 measurement
    /// only: resource attributes (environment, PR, instance id) merged onto EVERY span, as the
    /// server does at ingest, with HTTP semconv on the roots, a statement on every third span and a
    /// retry counter on the rest. Eight spans per trace, seeded jitter.
    /// </summary>
    private static List<SpanRecord> BuildResourceShapedCorpus(int howMany)
    {
        var rnd   = new Random(42);
        var spans = new List<SpanRecord>(howMany);
        var buf   = new ArrayBufferWriter<byte>(512);
        string[] routes   = ["console/api/{provider}/ProviderPayment/{providerPaymentId}", "api/payments/{id}/status",
                             "api/kiosk/{kioskId}/heartbeat", "api/providers/{providerId}/balance"];
        string[] services = ["MintRoute.API", "KioskAgent.API", "Etisalat.API"];
        string[] ops      = ["GET {route}", "SELECT payments", "publish PaymentAccepted", "HTTP POST"];

        for (int n = 0; n < howMany; n++)
        {
            int t = n / 8, i = n % 8;
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(3 + (i == 0 ? 6 : i % 3 == 0 ? 3 : 2));
            w.Write("Environment");         w.Write("Development");
            w.Write("PR");                  w.Write("pr7170");
            w.Write("service.instance.id"); w.Write(new Guid(t % 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0).ToString());
            if (i == 0)
            {
                w.Write("http.route");                w.Write(routes[t % routes.Length]);
                w.Write("http.request.method");       w.Write("GET");
                w.Write("http.response.status_code"); w.Write(200L);
                w.Write("network.protocol.version");  w.Write("1.1");
                w.Write("url.scheme");                w.Write("http");
                w.Write("url.path");                  w.Write("/" + routes[t % routes.Length].Replace("{provider}", "prov" + t % 9));
            }
            else if (i % 3 == 0)
            {
                w.Write("db.system");    w.Write("mssql");
                w.Write("db.statement"); w.Write("SELECT * FROM payments WHERE provider_id = @p" + t % 5);
                w.Write("db.rows");      w.Write(12.5 + i);
            }
            else
            {
                w.Write("retry.count"); w.Write((long)(i % 3));
                w.Write("cache.hit");   w.Write(i % 4 == 0);
            }
            w.Flush();

            long traceStart = BaseNano + t * 50_000_000L;
            spans.Add(new SpanRecord
            {
                TraceId           = new TraceId((ulong)(t + 1) * 0x9E3779B97F4A7C15UL, (ulong)(t + 17) * 0xC2B2AE3D27D4EB4FUL),
                SpanId            = new SpanId((ulong)(t * 100 + 1 + i)),
                ParentSpanId      = i == 0 ? default : new SpanId((ulong)(t * 100 + 1 + (i - 1) / 2)),
                StartTimeUnixNano = traceStart + i * 2_000_000L + rnd.Next(0, 500) * 1_000L,
                DurationNanos     = 1_000_000L + rnd.Next(0, 60_000) * 1_000L,
                Name              = ops[i % ops.Length],
                ServiceName       = services[(t + i / 3) % services.Length],
                Kind              = i == 0 ? SpanKind.Server : (i % 3 == 0 ? SpanKind.Client : SpanKind.Internal),
                Status            = rnd.Next(0, 50) == 0 ? SpanStatusCode.Error : SpanStatusCode.Unset,
                HttpStatusCode    = (short)(i == 0 ? 200 : 0),
                AttributesBytes   = buf.WrittenSpan.ToArray(),
            });
        }
        return spans;
    }

    /// <summary>
    /// Every LZ4 section of a v3 <c>.trc</c>, inflated: the span blocks (header to the trace-index
    /// offset) and the trace-index block itself. <paramref name="onDiskBytes"/> is their pickled
    /// size as written.
    /// </summary>
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

    private readonly record struct Sample(double Micros, long Allocated, long Loh);

    /// <summary>
    /// One measured call over a FRESH corpus, built before the window opens. A record memoises its
    /// attribute decode, so a corpus that an earlier call already walked arrives pre-paid: until
    /// TS#7(f) the <c>.tracesum</c> figure was taken over the records the warm-up had decoded, and
    /// showed the decode it existed to remove as costing nothing. A real flush sees every record
    /// exactly once, which is what a fresh corpus reproduces.
    /// </summary>
    private Sample Measure(string label, Action<string, List<SpanRecord>> flush)
    {
        string dir    = NewDir(label.Replace(' ', '-').Replace('.', '-').Replace('(', '-').Replace(')', '-'));
        var    corpus = BuildCorpus();

        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);

        long loh0 = LohAfterLastFullGc();
        long a0   = GC.GetAllocatedBytesForCurrentThread();
        var  sw   = Stopwatch.StartNew();
        flush(dir, corpus);
        sw.Stop();
        long allocated = GC.GetAllocatedBytesForCurrentThread() - a0;
        long loh       = LohAllocatedSince(loh0);
        return new Sample(sw.Elapsed.TotalMicroseconds, allocated, loh);
    }

    // ── LOH, measured as allocated rather than as left over ─────────────────────
    //
    // ALL THREE PARTS OF THE OLD FIGURE WERE WRONG, and it read 0,00 MB for every flush —
    // including the 400 KB array TS#7(a) removed, which is as plainly LOH as an allocation gets.
    // It read `GenerationInfo[^1]`, and GenerationInfo is gen0, gen1, gen2, LOH, POH: the LAST
    // entry is the pinned-object heap. It read `SizeAfterBytes` of whatever GC had last run — the
    // one BEFORE the window, since nothing inside it collected — so the delta was a GC's figure
    // subtracted from itself. And a generation's SIZE includes its fragmentation, so an array
    // placed in a free gap the previous collection left moves nothing: the first repair (the right
    // index, a GC after the window) still read 0 B for that same array whenever the test ran after
    // another one, and 400 KB when it ran alone.
    //
    // What is counted instead is OBJECT bytes — size minus fragmentation. The LOH is only ever
    // collected by a gen2 GC, so between two of them nothing leaves it; a forced full collection
    // AFTER the window sees, at entry, the objects the previous full collection left plus every
    // object allocated there since — which is the window, because the probes run serially (the
    // assembly disables test parallelisation) and force a full collection immediately before it.
    // Still process-wide, so still printed and never asserted.

    /// <summary>Index of the large-object heap in <see cref="GCMemoryInfo.GenerationInfo"/>.</summary>
    private const int LohIndex = 3;

    /// <summary>Object bytes on the LOH as the last full blocking collection left it.</summary>
    private static long LohAfterLastFullGc()
    {
        var loh = GC.GetGCMemoryInfo(GCKind.FullBlocking).GenerationInfo[LohIndex];
        return loh.SizeAfterBytes - loh.FragmentationAfterBytes;
    }

    /// <summary>Object bytes put on the LOH since <paramref name="lohObjectsAfterLastFullGc"/> was read.</summary>
    private static long LohAllocatedSince(long lohObjectsAfterLastFullGc)
    {
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        var loh = GC.GetGCMemoryInfo(GCKind.FullBlocking).GenerationInfo[LohIndex];
        return loh.SizeBeforeBytes - loh.FragmentationBeforeBytes - lohObjectsAfterLastFullGc;
    }

    private void Print(string label, in Sample s) =>
        _out.WriteLine($"{label,-30} {s.Micros / 1000.0,8:N1} ms  {s.Micros / Spans,6:N2} us/span  "
                     + $"{s.Allocated / 1048576.0,7:N2} MB  {s.Allocated / (double)Spans,7:N0} B/span  "
                     + $"LOH {s.Loh / 1048576.0,6:N2} MB");

    // ── The corpus ──────────────────────────────────────────────────────────────

    private readonly record struct Hashes(
        string Trc, long TrcLength,
        string Stats, long StatsLength,
        string SvcGraph, long SvcGraphLength,
        string TraceSum, long TraceSumLength);

    /// <summary>
    /// Writes the corpus and hashes the four files UNDER WHATEVER CULTURE THE CALLER SET — no pin
    /// any more; see the note above the golden facts. <c>SpanWriter.Write</c> runs start to finish
    /// on the calling thread (no task, no pool hop), so the caller's culture is the one every
    /// format inside it would see.
    /// </summary>
    private Hashes FlushAndHash(ushort version, string label)
    {
        string dir  = NewDir(label);
        var corpus  = BuildCorpus();
        string trc  = SpanWriter.Write(dir, corpus, version: version).FilePath;
        string bas  = Path.Combine(dir, Path.GetFileNameWithoutExtension(trc));

        return new Hashes(
            Sha(trc),                  new FileInfo(trc).Length,
            Sha(bas + ".stats"),       new FileInfo(bas + ".stats").Length,
            Sha(bas + ".svcgraph"),    new FileInfo(bas + ".svcgraph").Length,
            Sha(bas + ".tracesum"),    new FileInfo(bas + ".tracesum").Length);
    }

    /// <summary>Runs <paramref name="body"/> with this thread's culture set, and puts it back.</summary>
    private static T UnderCulture<T>(string culture, Func<T> body)
    {
        var saved = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
        try { return body(); }
        finally { CultureInfo.CurrentCulture = saved; }
    }

    private static string Sha(string path) =>
        Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(path)));

    private string NewDir(string label)
    {
        string dir = Path.Combine(Path.GetTempPath(), $"Ameto-flush-{label}-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);
        return dir;
    }

    /// <summary>
    /// The fixed corpus. See the type docstring for what each shape is here to catch. Built fresh
    /// on every call — the writer is allowed to reorder what it is handed, and a shared corpus
    /// would let one test's sort decide the next test's input.
    /// </summary>
    private static List<SpanRecord> BuildCorpus(int howMany = Spans)
    {
        var spans = new List<SpanRecord>(howMany);
        var buf   = new ArrayBufferWriter<byte>(1024);

        for (int i = 0; i < howMany; i++)
        {
            int  trace   = i / SpansPerTrace;
            bool isRoot  = i % SpansPerTrace == 0;
            var  traceId = new TraceId(0xA1B2C3D400000000UL | (uint)trace,
                                       0x0F1E2D3C4B5A6978UL ^ ((ulong)(uint)trace * 2654435761UL));
            var  spanId  = new SpanId((ulong)(i + 1) * 0x9E3779B97F4A7C15UL);
            var  parent  = isRoot ? default : new SpanId((ulong)(trace * SpansPerTrace + 1) * 0x9E3779B97F4A7C15UL);

            // 24-way ties, out of order: the sort has real work and real ambiguity.
            long start = BaseNano + (long)(i % DistinctTimestamps) * 1_000_000L;
            long dur   = (long)(i % 23) * 47_000_000L + 1;

            // The parent's service is picked from a different stride than the child's, so most
            // parent→child pairs cross a service boundary and the .svcgraph has real edges.
            string svc = ServiceNames[(i * 7 + i / SpansPerTrace) % ServiceNames.Length];

            var (blob, dict) = Attributes(i, isRoot, buf);

            // THE DICTIONARY IS SET ONLY WHEN THERE IS ONE. `Attributes` is an init accessor that
            // MEMOISES — assigning null to it marks the record decoded-as-nothing and kills the
            // lazy decode of the blob for good. A corpus built with `Attributes = null` beside a
            // real blob would therefore take the no-attributes branch everywhere and quietly stop
            // testing the very paths this file exists for.
            spans.Add(dict is null
                ? new SpanRecord
                {
                    TraceId           = traceId,
                    SpanId            = spanId,
                    ParentSpanId      = parent,
                    StartTimeUnixNano = start,
                    DurationNanos     = dur,
                    Name              = SpanNames[i % SpanNames.Length],
                    ServiceName       = svc,
                    Kind              = (SpanKind)(i % 6),
                    Status            = StatusOf(i),
                    HttpStatusCode    = (short)(200 + i % 5 * 100),
                    AttributesBytes   = blob,
                }
                : new SpanRecord
                {
                    TraceId           = traceId,
                    SpanId            = spanId,
                    ParentSpanId      = parent,
                    StartTimeUnixNano = start,
                    DurationNanos     = dur,
                    Name              = SpanNames[i % SpanNames.Length],
                    ServiceName       = svc,
                    Kind              = (SpanKind)(i % 6),
                    Status            = StatusOf(i),
                    HttpStatusCode    = (short)(200 + i % 5 * 100),
                    Attributes        = dict,
                });
        }

        return spans;
    }

    private static SpanStatusCode StatusOf(int i) =>
        i % 13 == 0 ? SpanStatusCode.Error :
        i % 3  == 0 ? SpanStatusCode.Ok    : SpanStatusCode.Unset;

    /// <summary>
    /// One span's attributes, in one of seven shapes. Returns the blob, the dictionary, or
    /// neither — <c>SpanRecord.Attributes</c> is an <c>init</c> that MEMOISES, so handing it null
    /// explicitly is not the same as not handing it anything, and shape 4 relies on the
    /// difference: a record with a dictionary and no blob is the <c>WriteAttributes</c> fallback.
    /// </summary>
    private static (ReadOnlyMemory<byte> Blob, IReadOnlyDictionary<string, object?>? Dict) Attributes(
        int i, bool isRoot, ArrayBufferWriter<byte> buf)
    {
        switch (i % 7)
        {
            case 0:   // the ordinary instrumented span, plus HTTP semconv on the roots
            {
                buf.ResetWrittenCount();
                var w = new MessagePackWriter(buf);
                w.WriteMapHeader(isRoot ? 10 : 8);
                w.Write("db.system");        w.Write("mssql");
                w.Write("db.statement");     w.Write("SELECT TOP 100 * FROM Orders WHERE CustomerId = @p0");
                w.Write("net.peer.port");    w.Write((long)(1433 + i % 7));
                w.Write("db.rows");          w.Write((long)(i % 4096));
                w.Write("sampling.ratio");   w.Write(0.25 + i % 4 * 0.125);
                w.Write("db.cached");        w.Write(i % 2 == 0);
                w.Write("deployment.env");   w.Write("production");
                // THE DUPLICATE KEY: the OTLP mapper writes resource attributes and then span
                // attributes, so a span attribute shadows a resource one of the same name. Last
                // copy wins, and the writer copies the blob through verbatim — both halves are in
                // the hash.
                w.Write("deployment.env");   w.Write("production-eu-west-1");
                if (isRoot)
                {
                    w.Write("http.request.method"); w.Write(i % 3 == 0 ? "POST" : "GET");
                    w.Write("url.path");            w.Write("/api/v1/orders/" + i % 97);
                }
                w.Flush();
                return (buf.WrittenSpan.ToArray(), null);
            }

            case 1:   // present but empty — a map header and nothing after it
            {
                buf.ResetWrittenCount();
                var w = new MessagePackWriter(buf);
                w.WriteMapHeader(0);
                w.Flush();
                return (buf.WrittenSpan.ToArray(), null);
            }

            case 2:   // shapes the decoder boxes to null: a nested map, an array, and nil
            {
                buf.ResetWrittenCount();
                var w = new MessagePackWriter(buf);
                w.WriteMapHeader(4);
                w.Write("peer.tags");    w.WriteArrayHeader(2); w.Write("a"); w.Write("b");
                w.Write("exception");    w.WriteMapHeader(1); w.Write("type"); w.Write("TimeoutException");
                w.Write("trace.flags");  w.WriteNil();
                w.Write("service.tier"); w.Write("gold");
                w.Flush();
                return (buf.WrittenSpan.ToArray(), null);
            }

            case 3:   // binary, and a negative integer
            {
                buf.ResetWrittenCount();
                var w = new MessagePackWriter(buf);
                w.WriteMapHeader(3);
                w.Write("payload.digest"); w.Write(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF, (byte)i });
                w.Write("clock.skew.ns");  w.Write(-(long)(i % 1000) * 1_000_000L);
                w.Write("queue.depth");    w.Write((long)(i % 31));
                w.Flush();
                return (buf.WrittenSpan.ToArray(), null);
            }

            case 4:   // NO blob, a dictionary — the WriteAttributes fallback, every boxed type it switches on
            {
                var d = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["messaging.system"]  = "kafka",
                    ["messaging.partition"] = (int)(i % 16),
                    ["retry.count"]       = (short)(i % 5),
                    ["priority"]          = (byte)(i % 3),
                    ["backoff.seconds"]   = (float)(i % 9) / 4f,
                    ["latency.ms"]        = (double)(i % 900) / 3.0,
                    ["was.retried"]       = i % 2 == 1,
                    ["upstream"]          = null,
                    ["correlation"]       = (long)i * 31L,
                };
                return (default, d);
            }

            case 5:   // A TRUNCATED BLOB: TryWalk must reject it and the writer must fall back
            {
                buf.ResetWrittenCount();
                var w = new MessagePackWriter(buf);
                w.WriteMapHeader(3);
                w.Write("span.kind.hint"); w.Write("client");
                w.Write("unterminated");   // the value never arrives
                w.Flush();
                return (buf.WrittenSpan.ToArray(), null);
            }

            default:  // no attributes of any kind
                return (default, null);
        }
    }
}
