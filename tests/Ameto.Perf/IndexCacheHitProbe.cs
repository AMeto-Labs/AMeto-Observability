using System.Buffers;
using System.Diagnostics;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query;
using Ameto.Query.Filtering;
using Ameto.Query.Tests;   // QuerySegmentFixtures, compiled in from the query suite (see .csproj)
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Issue #80: the segment-index cache almost never hit, so a filtering query re-parsed whole
/// index sections that a query a moment earlier had already parsed.
///
/// <para>THE STAND. 1.2 M events in 24 segments, written once by the current writer through the
/// production flush (four tier flushes, each split six ways by level), in the event mix
/// <c>tools/loggen</c> sends — the generator the load runs of #79 used, so the sections have the
/// shape the cache met there: request, payment and message ids, trace and span ids on a share of
/// the rows, structured exceptions on the errors. Two filters: <c>@mt like '%timeout%'</c>,
/// which has no equality hint and so consults every group, and <c>Provider = 'UnionPay'</c>,
/// which the bloom gate narrows to the groups that could hold it (the Information segments) and
/// the inverted index to 4 % of their rows.</para>
///
/// <para>FOUR READINGS, because they answer different questions.</para>
/// <list type="bullet">
/// <item><see cref="PhaseBreakdown"/> walks the prefilter's steps one group at a time on the
/// test thread — the same calls the executor makes, in its order, through the same
/// <see cref="SegmentIndexView"/> with a throwaway memo — so every phase gets its own clock and
/// its own per-thread allocation counter: the bloom gate, the index (sections rented on need,
/// lookups, intersection), and the candidate scan. This is the miss path, the one the cache is
/// there to skip. Until #80 its index phase decoded every group's sections whole.</item>
/// <item><see cref="RepeatedFilter"/> runs each filter five times through a
/// <see cref="QueryExecutor"/> with the production cache budget (256 MB, 96 MB native) and
/// reports what <c>/api/diagnostics</c> reports — the <c>indexCache*</c> counters — per run.
/// The executor prefilters eight groups at a time on the pool, so allocation there is the
/// process-wide counter; the assembly runs one probe at a time, which makes it attributable.</item>
/// <item><see cref="StandBudget"/> repeats that at the 512 MB stand's derived budget
/// (46 MB, 25.6 MB native) against the cache switched off — issue option 4.</item>
/// <item><see cref="FixedFilterAmongUniqueLookups"/> is the shape that is not a repeated filter:
/// a fixed LIKE between searches for one-off ids, at the stand's budget.</item>
/// </list>
///
/// <para>Every executor run is checked against the uncached executor's result, row for row: a
/// cache may only ever skip work.</para>
///
/// <para>WHAT IS ASSERTED, with margins wide enough for the Debug build on CI's two-core runner
/// (timings there vary ~40 % run to run): hit rates, allocation and what the cache holds. Time is
/// printed, never asserted. BEFORE (aa58efd, Release) and the bound each
/// assertion sets:</para>
/// <code>
///                                         before      after      asserted
///   LIKE walk, index phase allocation     782 MB     1.4 MB      &lt; 32 MB
///   EQ walk, index phase allocation       218 MB     0.4 MB      &lt; 16 MB
///   LIKE walk, index time / scan time       23x      1.6-3.6x    (printed only)
///   LIKE warm hit rate, 256 MB             2.1 %      100 %      ≥ 95 %
///   EQ warm hit rate, 256 MB              11.5 %      100 %      ≥ 95 %
///   LIKE warm allocation per query        834 MB      33 MB      &lt; 150 MB
///   cache bytes after both filters        255 MB     1.4 MB      &lt; 16 MB, none native
///   hit rates at 46 MB, and alternating     0 %       100 %      ≥ 95 %
///   fixed LIKE among one-off lookups, 46 MB   —        100 %      ≥ 95 %, cache &lt; 16 MB
/// </code>
/// <para>What is left of the miss path's index phase is mostly reading the trigram sections out of
/// the mapped file (<c>SegmentReader</c>'s section rent, a <c>ReadArray</c> at ~1.3 GB/s here):
/// ~65 ms of the LIKE walk's index time for 101 MB of sections, against ~2 ms to find the five
/// trigrams in them.</para>
/// </summary>
public sealed class IndexCacheHitProbe : IClassFixture<IndexCacheHitProbe.Corpus>
{
    public const string Like     = "@mt like '%timeout%'";
    public const string Equality = "Provider = 'UnionPay'";

    private const int  Runs            = 5;
    private const long ProdBudget      = 256L * 1024 * 1024;
    private const long ProdNative      = 96L * 1024 * 1024;
    private const long StandBudgetB    = 46L * 1024 * 1024;
    private const long StandNative     = (long)(512L * 1024 * 1024 * 0.05);

    private readonly Corpus            _corpus;
    private readonly ITestOutputHelper _out;

    public IndexCacheHitProbe(Corpus corpus, ITestOutputHelper o)
    {
        _corpus = corpus;
        _out    = o;
    }

    // ── 1. The miss path, phase by phase ─────────────────────────────────────

    [Fact]
    public void PhaseBreakdown()
    {
        _out.WriteLine(_corpus.Describe());
        Phases like = default;

        foreach (var text in new[] { Like, Equality })
        {
            var filter = CompiledFilter.Compile(text);
            Assert.Null(filter.DerivedLevels);   // the walk below mirrors the executor without level hints

            Walk(filter, printGroups: false);                     // warm: JIT, page cache
            var runs = new Phases[Runs];
            for (int r = 0; r < Runs; r++) runs[r] = Walk(filter, printGroups: r == 0 && text == Like);

            var m = Phases.Median(runs);
            if (text == Like) like = m;
            _out.WriteLine("");
            _out.WriteLine($"── {text}: median of {Runs} single-threaded walks over {m.Groups} groups ({m.GroupsPassed} past the bloom gate) ──");
            _out.WriteLine($"  open segments          {m.OpenMs,9:F1} ms   {Mb(m.OpenBytes),9:F1} MB");
            _out.WriteLine($"  bloom gate             {m.BloomMs,9:F1} ms   {Mb(m.BloomBytes),9:F1} MB");
            _out.WriteLine($"  index (rent on need,   {m.IndexMs,9:F1} ms   {Mb(m.IndexBytes),9:F1} MB");
            _out.WriteLine($"    lookup, intersect)");
            _out.WriteLine($"  candidate scan         {m.ScanMs,9:F1} ms   {Mb(m.ScanBytes),9:F1} MB   ({m.Candidates} candidates, {m.Matches} matches)");
            _out.WriteLine($"  total                  {m.TotalMs,9:F1} ms   {Mb(m.TotalBytes),9:F1} MB");
            _out.WriteLine($"  what a cache would keep: {Mb(m.MemoBytes):F2} MB of memo for {m.Groups} groups, nothing native");

            Assert.True(m.Matches > 0, $"{text} matched nothing — the corpus lost its shape");

            // The index phase is what #80 was about: it decoded every group's sections whole —
            // 782 MB for the LIKE walk, 218 MB for the equality one. It now reads one bucket or a
            // handful of trigrams per group.
            long indexBound = text == Like ? 32L << 20 : 16L << 20;
            Assert.True(m.IndexBytes < indexBound,
                $"{text}: the index phase allocated {Mb(m.IndexBytes):F1} MB — something decodes whole sections again");
        }

        // Time is REPORTED, not asserted: the index phase is mostly reading sections out of the
        // mapped file (I/O) and the scan is CPU, so their ratio on a loaded two-core runner says
        // more about the runner than the code. The allocation bound above is what fails when the
        // index decodes whole sections again.
        _out.WriteLine("");
        _out.WriteLine($"LIKE index / scan time: {like.IndexMs / like.ScanMs:F1}x (23x before #80)");
    }

    // ── 2. The repeated filter at the production budget ──────────────────────

    [Fact]
    public async Task RepeatedFilter()
    {
        var plain = NewExecutor(null);
        var expectLike = await DrainAsync(plain, Like);
        var expectEq   = await DrainAsync(plain, Equality);
        await WarmJitAsync(plain);

        using var cache = new SegmentIndexCache(ProdBudget, ProdNative, TimeSpan.Zero);
        var cached = NewExecutor(cache);

        _out.WriteLine($"production budget: {Mb(ProdBudget):F0} MB total, {Mb(ProdNative):F0} MB native");
        var like = await RepeatAsync(cached, cache, Like, expectLike);
        var eq   = await RepeatAsync(cached, cache, Equality, expectEq);
        var cold   = await RepeatAsync(plain,  null,  Like, expectLike, label: "uncached");
        var coldEq = await RepeatAsync(plain,  null,  Equality, expectEq, label: "uncached");

        _out.WriteLine("");
        _out.WriteLine($"LIKE warm (runs 2..{Runs}): hit {like.WarmHitPct:F1} %, median {like.WarmMedianMs:F1} ms, {Mb(like.WarmMedianBytes):F1} MB   | uncached median {cold.WarmMedianMs:F1} ms, {Mb(cold.WarmMedianBytes):F1} MB");
        _out.WriteLine($"EQ   warm (runs 2..{Runs}): hit {eq.WarmHitPct:F1} %, median {eq.WarmMedianMs:F1} ms, {Mb(eq.WarmMedianBytes):F1} MB   | uncached median {coldEq.WarmMedianMs:F1} ms, {Mb(coldEq.WarmMedianBytes):F1} MB");
        _out.WriteLine($"cache after both: {Mb(cache.TotalBytes):F2} MB in {cache.EntryCount} entries, {Mb(cache.NativeBytes):F2} MB native");

        // Before: 2.1 % and 11.5 %, two entries of 128 MB filling the whole budget.
        Assert.True(like.WarmHitPct >= 95, $"LIKE warm hit rate {like.WarmHitPct:F1} %");
        Assert.True(eq.WarmHitPct   >= 95, $"EQ warm hit rate {eq.WarmHitPct:F1} %");
        Assert.True(like.WarmMedianBytes < 150L << 20,
            $"a repeated LIKE allocated {Mb(like.WarmMedianBytes):F1} MB per query (834 MB before)");
        Assert.True(cache.TotalBytes < 16L << 20,
            $"the cache holds {Mb(cache.TotalBytes):F1} MB for two filters over 24 groups — it is keeping sections, not answers");
        Assert.Equal(0, cache.NativeBytes);
    }

    // ── 3. Option 4: the 512 MB stand's budget, or no cache at all ───────────

    [Fact]
    public async Task StandBudget()
    {
        var plain = NewExecutor(null);
        var expectLike = await DrainAsync(plain, Like);
        var expectEq   = await DrainAsync(plain, Equality);
        await WarmJitAsync(plain);

        using var cache = new SegmentIndexCache(StandBudgetB, StandNative, TimeSpan.Zero);
        var cached = NewExecutor(cache);

        _out.WriteLine($"stand budget: {Mb(StandBudgetB):F0} MB total, {Mb(StandNative):F1} MB native");
        var like = await RepeatAsync(cached, cache, Like, expectLike);
        var eq   = await RepeatAsync(cached, cache, Equality, expectEq);

        // The mixed shape a dashboard produces: the two filters taking turns.
        long h0 = cache.HitCount, m0 = cache.MissCount;
        for (int r = 0; r < Runs; r++)
        {
            Assert.Equal(expectLike, await DrainAsync(cached, Like));
            Assert.Equal(expectEq,   await DrainAsync(cached, Equality));
        }
        long h = cache.HitCount - h0, m = cache.MissCount - m0;
        _out.WriteLine($"alternating LIKE/EQ x{Runs}: {h} hits / {m} misses = {Pct(h, h + m):F1} %, cache {Mb(cache.TotalBytes):F1} MB in {cache.EntryCount} entries");

        var offLike = await RepeatAsync(plain, null, Like, expectLike, label: "cache off");
        var offEq   = await RepeatAsync(plain, null, Equality, expectEq, label: "cache off");

        _out.WriteLine("");
        _out.WriteLine($"LIKE at 46 MB: hit {like.WarmHitPct:F1} %, {like.WarmMedianMs:F1} ms, {Mb(like.WarmMedianBytes):F1} MB   | off: {offLike.WarmMedianMs:F1} ms, {Mb(offLike.WarmMedianBytes):F1} MB");
        _out.WriteLine($"EQ   at 46 MB: hit {eq.WarmHitPct:F1} %, {eq.WarmMedianMs:F1} ms, {Mb(eq.WarmMedianBytes):F1} MB   | off: {offEq.WarmMedianMs:F1} ms, {Mb(offEq.WarmMedianBytes):F1} MB");

        // Before: no entry fit (one Information group decoded is 128 MB), so the stand's cache
        // held nothing and hit nothing. A memo is kilobytes.
        Assert.True(like.WarmHitPct >= 95, $"LIKE warm hit rate at 46 MB {like.WarmHitPct:F1} %");
        Assert.True(eq.WarmHitPct   >= 95, $"EQ warm hit rate at 46 MB {eq.WarmHitPct:F1} %");
        Assert.True(Pct(h, h + m)   >= 95, $"alternating hit rate at 46 MB {Pct(h, h + m):F1} %");
        Assert.True(cache.TotalBytes <= StandBudgetB);
    }

    // ── 4. A fixed filter among one-off lookups ──────────────────────────────

    /// <summary>
    /// The shape that is NOT a repeated filter: a dashboard's fixed LIKE, refreshed between
    /// searches for one-off ids (<c>RequestId = 'req-…'</c>, a new value each time). Every one-off
    /// reads the bloom of every group and leaves a verdict behind in each; those are bounded per
    /// memo (<c>SegmentIndexReader.MaxBoundedAnswers</c>), so they neither grow the cache nor push
    /// out the fixed filter. At the stand's budget, the fixed LIKE must keep hitting while the
    /// one-offs — new questions by definition — miss.
    /// </summary>
    [Fact]
    public async Task FixedFilterAmongUniqueLookups()
    {
        const int Rounds = 60;
        var plain = NewExecutor(null);
        var expectLike = await DrainAsync(plain, Like);
        await WarmJitAsync(plain);

        using var cache = new SegmentIndexCache(StandBudgetB, StandNative, TimeSpan.Zero);
        var cached = NewExecutor(cache);

        long likeHits = 0, likeMisses = 0;
        for (int r = 0; r < Rounds; r++)
        {
            long h0 = cache.HitCount, m0 = cache.MissCount;
            Assert.Equal(expectLike, await DrainAsync(cached, Like));
            if (r > 0) { likeHits += cache.HitCount - h0; likeMisses += cache.MissCount - m0; }

            // Not a value the corpus holds: its generator draws twelve random hex digits.
            Assert.Empty(await DrainAsync(cached, $"RequestId = 'req-zz{r:x10}'"));
        }

        _out.WriteLine($"fixed LIKE among {Rounds} one-off lookups at 46 MB: {likeHits} hits / {likeMisses} misses " +
                       $"= {Pct(likeHits, likeHits + likeMisses):F1} %, cache {Mb(cache.TotalBytes):F2} MB in {cache.EntryCount} entries");

        Assert.True(Pct(likeHits, likeHits + likeMisses) >= 95,
            $"the fixed LIKE hit {Pct(likeHits, likeHits + likeMisses):F1} % among one-off lookups");
        Assert.True(cache.TotalBytes < 16L << 20, $"one-off lookups grew the cache to {Mb(cache.TotalBytes):F1} MB");
    }

    // ── Executor runs ─────────────────────────────────────────────────────────

    private readonly record struct RepeatResult(double WarmHitPct, double WarmMedianMs, long WarmMedianBytes);

    private async Task<RepeatResult> RepeatAsync(
        QueryExecutor q, SegmentIndexCache? cache, string filter, List<(long, ulong)> expected, string label = "cached")
    {
        _out.WriteLine("");
        _out.WriteLine($"── {filter} ({label}) ──");
        var ms    = new double[Runs - 1];
        var bytes = new long[Runs - 1];
        long warmHits = 0, warmMisses = 0;
        for (int r = 0; r < Runs; r++)
        {
            long h0 = cache?.HitCount ?? 0, m0 = cache?.MissCount ?? 0;
            long a0 = GC.GetTotalAllocatedBytes(precise: true);
            long t0 = Stopwatch.GetTimestamp();
            var rows = await DrainAsync(q, filter);
            double elapsed = Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
            long alloc = GC.GetTotalAllocatedBytes(precise: true) - a0;
            long h = (cache?.HitCount ?? 0) - h0, m = (cache?.MissCount ?? 0) - m0;

            Assert.Equal(expected, rows);
            if (r > 0) { ms[r - 1] = elapsed; bytes[r - 1] = alloc; warmHits += h; warmMisses += m; }

            _out.WriteLine(cache is null
                ? $"  run {r + 1}: {elapsed,8:F1} ms  {Mb(alloc),8:F1} MB  {rows.Count} rows"
                : $"  run {r + 1}: {elapsed,8:F1} ms  {Mb(alloc),8:F1} MB  {rows.Count} rows  hits {h,3} misses {m,3}  " +
                  $"| indexCacheEntries {cache.EntryCount} indexCacheBytes {Mb(cache.TotalBytes):F1} MB indexCacheNativeBytes {Mb(cache.NativeBytes):F1} MB " +
                  $"indexCacheNativeEvicted {cache.NativeEvictedCount}");
        }
        Array.Sort(ms);
        Array.Sort(bytes);
        return new RepeatResult(Pct(warmHits, warmHits + warmMisses), ms[ms.Length / 2], bytes[bytes.Length / 2]);
    }

    /// <summary>
    /// Runs both filters through both executor paths until the JIT has promoted them, through a
    /// THROWAWAY cache so the one being measured still starts cold. Without it, whichever side a
    /// fact measures first runs tier-0 code: run alone, the stand fact had its cached runs at
    /// 70-85 ms against 60-66 ms uncached, where the same cached query warm takes ~27 ms.
    /// </summary>
    private async Task WarmJitAsync(QueryExecutor plain)
    {
        using var scratch = new SegmentIndexCache(ProdBudget, ProdNative, TimeSpan.Zero);
        var warm = NewExecutor(scratch);
        for (int i = 0; i < 4; i++)
        {
            await DrainAsync(warm, Like);
            await DrainAsync(warm, Equality);
            await DrainAsync(plain, Like);
            await DrainAsync(plain, Equality);
        }
    }

    private QueryExecutor NewExecutor(SegmentIndexCache? cache) =>
        new(_corpus.Engine, new SegmentIndexReaderFactory(), NullLogger<QueryExecutor>.Instance, cache);

    /// <summary>Every match, oldest first, as (timestamp, id) — the identity a result is compared by.</summary>
    private static async Task<List<(long, ulong)>> DrainAsync(QueryExecutor q, string filter)
    {
        var rows = new List<(long, ulong)>(64 * 1024);
        await foreach (var ev in q.ExecuteAsync(new QueryRequest
        {
            Filter    = filter,
            Count     = int.MaxValue,
            Direction = QueryDirection.Forward,
        }))
        {
            rows.Add((ev.Timestamp.UtcTicks, ev.Id.RawValue));
        }
        return rows;
    }

    // ── The miss path, walked by hand ─────────────────────────────────────────

    private struct Phases
    {
        public double OpenMs, BloomMs, IndexMs, ScanMs;
        public long   OpenBytes, BloomBytes, IndexBytes, ScanBytes;
        public long   MemoBytes, Candidates, Matches;
        public int    Groups, GroupsPassed;

        public readonly double TotalMs    => OpenMs + BloomMs + IndexMs + ScanMs;
        public readonly long   TotalBytes => OpenBytes + BloomBytes + IndexBytes + ScanBytes;

        public static Phases Median(Phases[] runs)
        {
            var m = runs[0];
            m.OpenMs  = Med(runs, static p => p.OpenMs);  m.OpenBytes  = (long)Med(runs, static p => p.OpenBytes);
            m.BloomMs = Med(runs, static p => p.BloomMs); m.BloomBytes = (long)Med(runs, static p => p.BloomBytes);
            m.IndexMs = Med(runs, static p => p.IndexMs); m.IndexBytes = (long)Med(runs, static p => p.IndexBytes);
            m.ScanMs  = Med(runs, static p => p.ScanMs);  m.ScanBytes  = (long)Med(runs, static p => p.ScanBytes);
            return m;
        }

        private static double Med(Phases[] runs, Func<Phases, double> f)
        {
            var v = new double[runs.Length];
            for (int i = 0; i < v.Length; i++) v[i] = f(runs[i]);
            Array.Sort(v);
            return v[v.Length / 2];
        }
    }

    /// <summary>
    /// One prefilter + scan over the whole corpus, the executor's miss path step by step
    /// (<c>QueryExecutor.PrefilterSegmentsAsync</c>, then <c>ScanSegmentAsync</c>), on this thread:
    /// each group through a <see cref="SegmentIndexView"/> with no cache, i.e. a memo that knows
    /// nothing yet and is dropped with the group.
    /// </summary>
    private Phases Walk(CompiledFilter filter, bool printGroups)
    {
        var  factory      = new SegmentIndexReaderFactory();
        bool hasIndexHint = !filter.IsMatchAll && filter.TryGetIndexHint(out _, out _);
        var  p            = new Phases();

        if (printGroups)
            _out.WriteLine($"{"segment id",10} {"g",2} {"level",-11} {"events",8} {"inverted",10} {"trigram",10} {"bloom",9} {"memo kept",10}");

        foreach (var info in _corpus.Segments)
        {
            long a = GC.GetAllocatedBytesForCurrentThread(), t = Stopwatch.GetTimestamp();
            using var reader = SegmentReader.Open(info.FilePath);
            Charge(ref p.OpenMs, ref p.OpenBytes, a, t);

            List<uint>? candidates = null;
            bool unnarrowed = false, anySurvived = false;
            var  groups     = reader.Groups;
            for (int g = 0; g < groups.Length; g++)
            {
                var grp = groups[g];
                if (grp.EventCount == 0) continue;
                p.Groups++;

                using var index = factory.OpenGroup(null, info.FilePath, g, reader);

                a = GC.GetAllocatedBytesForCurrentThread(); t = Stopwatch.GetTimestamp();
                bool bloomPass = !hasIndexHint || QueryExecutor.PassesBloomGate(filter, index);
                Charge(ref p.BloomMs, ref p.BloomBytes, a, t);

                bool keep = false;
                uint[]? groupCandidates = null;
                bool everyRow = false;
                if (bloomPass)
                {
                    p.GroupsPassed++;
                    a = GC.GetAllocatedBytesForCurrentThread(); t = Stopwatch.GetTimestamp();
                    keep = QueryExecutor.TryNarrowWithIndex(filter, index, null, grp.EventCount, out groupCandidates, out everyRow);
                    Charge(ref p.IndexMs, ref p.IndexBytes, a, t);
                }
                p.MemoBytes += index.Index.ApproxRetainedBytes;

                if (printGroups)
                {
                    using var inv = reader.RentInvertedIndexBytes(g);
                    using var tri = reader.RentTrigramIndexBytes(g);
                    using var blo = reader.RentBloomFilterBytes(g);
                    _out.WriteLine($"{info.Id.Value,10} {g,2} {info.MinLevel,-11} {grp.EventCount,8} " +
                                   $"{Mb(inv.Span.Length),7:F1} MB {Mb(tri.Span.Length),7:F1} MB {Mb(blo.Span.Length),6:F1} MB " +
                                   $"{index.Index.ApproxRetainedBytes / 1024.0,7:F1} KB");
                }
                if (!keep) continue;

                anySurvived = true;
                Assert.False(everyRow);                         // no level hints in this walk
                if (groupCandidates is null) unnarrowed = true;
                else (candidates ??= new List<uint>()).AddRange(groupCandidates);
            }
            if (!anySurvived) continue;

            a = GC.GetAllocatedBytesForCurrentThread(); t = Stopwatch.GetTimestamp();
            uint[]? offsets = unnarrowed ? null : candidates?.ToArray();
            p.Candidates += offsets is null ? info.EventCount : offsets.Length;
            var e = reader.ReadEventsAsync(offsets, null, null, reversed: false, CancellationToken.None).GetAsyncEnumerator();
            try
            {
                while (Sync(e.MoveNextAsync()))
                    if (filter.Matches(e.Current)) p.Matches++;
            }
            finally { Sync(e.DisposeAsync()); }
            Charge(ref p.ScanMs, ref p.ScanBytes, a, t);
        }
        return p;
    }

    private static void Charge(ref double ms, ref long bytes, long allocBefore, long tsBefore)
    {
        ms    += Stopwatch.GetElapsedTime(tsBefore).TotalMilliseconds;
        bytes += GC.GetAllocatedBytesForCurrentThread() - allocBefore;
    }

    /// <summary>The segment reader's iterator never awaits anything that does not complete in
    /// line — which is what keeps the per-thread allocation counter honest here.</summary>
    private static bool Sync(ValueTask<bool> vt) => vt.IsCompletedSuccessfully ? vt.Result : vt.AsTask().GetAwaiter().GetResult();
    private static void Sync(ValueTask vt) { if (!vt.IsCompletedSuccessfully) vt.AsTask().GetAwaiter().GetResult(); }

    private static double Mb(long bytes) => bytes / (1024.0 * 1024.0);
    private static double Pct(long part, long whole) => whole == 0 ? 0 : 100.0 * part / whole;

    // ── The corpus ────────────────────────────────────────────────────────────

    /// <summary>
    /// Written once per test class, under TMP, and deleted with it. ~0.5 GB on disk.
    /// </summary>
    public sealed class Corpus : IAsyncLifetime
    {
        public const int Flushes        = 4;
        public const int EventsPerFlush = 300_000;

        private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-idxcachehit-" + Guid.NewGuid().ToString("N"));

        public StorageEngine              Engine   { get; private set; } = null!;
        public IReadOnlyList<SegmentInfo> Segments { get; private set; } = [];
        public long                       FileBytes { get; private set; }
        public double                     BuildSeconds { get; private set; }

        public async Task InitializeAsync()
        {
            long t0 = Stopwatch.GetTimestamp();
            Directory.CreateDirectory(_dir);
            // A tier big enough for one flush's 300 k events, so a segment is cut only where
            // this fixture flushes. The derived group budget stays the host's (64 MB here), so
            // each level segment is one index group — the cache unit the issue measured.
            var opts = new ServerOptions
            {
                DataDirectory = _dir,
                HotTier       = new HotTierOptions { MaxSizeBytes = 160L * 1024 * 1024 },
            };
            // No maintenance loop: its first pass would merge these 24 segments three minutes in —
            // on a slow runner, in the middle of the facts that count this corpus's groups.
            Engine = new StorageEngine(
                Options.Create(opts),
                new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
                NullLogger<StorageEngine>.Instance,
                Timeout.InfiniteTimeSpan);
            Engine.IndexSinkFactory = static (estimatedEventCount, termsPerEvent) =>
                new SegmentIndexBuilder(estimatedEventCount, 5, termsPerEvent);

            var  gen       = new LoggenShape(new Random(80), Engine.TemplatePool);
            var  buf       = new ArrayBufferWriter<byte>(512);
            long baseTicks = DateTimeOffset.UtcNow.UtcTicks - TimeSpan.TicksPerHour;
            for (int f = 0; f < Flushes; f++)
            {
                for (int i = 0; i < EventsPerFlush; i++)
                {
                    long ts = baseTicks + ((long)f * EventsPerFlush + i) * TimeSpan.TicksPerMillisecond;
                    Assert.True(gen.Write(Engine, buf, ts), "the hot tier refused a write");
                }
                await Engine.FlushHotTierAsync();
            }

            var segs = new List<SegmentInfo>(Engine.ListSegments());
            segs.Sort(static (a, b) => a.Id.Value.CompareTo(b.Id.Value));
            Segments = segs;
            long bytes = 0;
            foreach (var s in segs) bytes += new FileInfo(s.FilePath).Length;
            FileBytes    = bytes;
            BuildSeconds = Stopwatch.GetElapsedTime(t0).TotalSeconds;
            Assert.Equal(Flushes * 6, segs.Count);
        }

        public async Task DisposeAsync()
        {
            await Engine.DisposeAsync();
            QuerySegmentFixtures.DeleteDataDirectory(_dir);
        }

        public string Describe()
        {
            long events = 0;
            foreach (var s in Segments) events += s.EventCount;
            return $"corpus: {Segments.Count} segments, {events} events, {FileBytes / (1024.0 * 1024.0):F0} MB on disk, written in {BuildSeconds:F1} s";
        }
    }

    /// <summary>
    /// <c>tools/loggen</c>'s <c>EventGenerator</c>, written straight into the engine: the same
    /// thirteen scenarios, weights, property shapes, trace share and exceptions. The header
    /// carries what the CLEF path would have lifted out of the map (level, template, service,
    /// trace and span id, exception); the payload is the rest.
    /// </summary>
    private sealed class LoggenShape(Random rng, StringInternPool pool)
    {
        private static readonly string[] Services  = ["Wallet.API", "Processing.API", "Etisalat.API", "dealer.Gateway", "Notification.Worker"];
        private static readonly string[] Methods   = ["GET", "POST", "PUT", "DELETE"];
        private static readonly string[] Paths     = ["/api/pay", "/api/topup", "/api/status", "/dealer/api/wallet", "/api/orders", "/api/balance"];
        private static readonly string[] Queues    = ["payments", "notifications", "reconciliation"];
        private static readonly string[] Providers = ["Visa", "Mastercard", "UnionPay"];
        private static readonly string[] Auth      = ["local", "google", "microsoft"];

        private static readonly ExceptionInfo GatewayTimeout = new()
        {
            Type       = "System.TimeoutException",
            Message    = "The payment gateway did not respond within 30000 ms.",
            StackTrace = "   at Wallet.Gateway.PaymentClient.SendAsync(PaymentRequest request)\n   at Wallet.Api.PaymentsController.Process(PaymentDto dto)",
            Inner      = new ExceptionInfo { Type = "System.Net.Sockets.SocketException", Message = "Connection timed out (10060)" },
        };

        private static readonly ExceptionInfo InvalidState = new()
        {
            Type       = "System.InvalidOperationException",
            Message    = "Sequence contains no matching element.",
            StackTrace = "   at System.Linq.ThrowHelper.ThrowNoMatchException()\n   at Processing.Api.Orders.OrderService.Complete(Int64 orderId)",
        };

        private static readonly (LogLevel Level, string Template)[] Scenarios =
        [
            (LogLevel.Information, "HTTP {Method} {Path} responded {StatusCode} in {Elapsed} ms"),
            (LogLevel.Information, "Payment {PaymentId} of {Amount} {Currency} processed via {Provider}"),
            (LogLevel.Information, "User {UserId} signed in from {ClientIp} via {AuthProvider}"),
            (LogLevel.Information, "Order {OrderId} created for customer {CustomerId}: {Order}"),
            (LogLevel.Information, "Wallet {WalletId} balance updated to {Balance} {Currency}"),
            (LogLevel.Debug,       "Executed DbCommand ({Elapsed} ms) [{CommandType}] rows={Rows}"),
            (LogLevel.Debug,       "Consumed message {MessageId} from {Queue} in {Elapsed} ms"),
            (LogLevel.Verbose,     "Cache miss for key {CacheKey}"),
            (LogLevel.Warning,     "Retry {Attempt} for request {RequestId} to {Endpoint} after {DelayMs} ms"),
            (LogLevel.Warning,     "Slow query ({Elapsed} ms) exceeded threshold {Threshold} ms [{CommandType}]"),
            (LogLevel.Error,       "Failed to process payment {PaymentId}: gateway timeout after {Timeout} ms"),
            (LogLevel.Error,       "Unhandled exception handling {Path} (request {RequestId})"),
            (LogLevel.Fatal,       "Unrecoverable error in {Component}; the worker will restart"),
        ];

        // loggen's weights out of 100: Info 58 %, Debug 22 %, Verbose 4 %, Warning 10 %, Error 5 %, Fatal 1 %.
        private static readonly int[] Weights = [22, 12, 8, 8, 8, 14, 8, 4, 7, 3, 4, 1, 1];
        private readonly int[] _cumulative = BuildCumulative();
        private readonly int[] _templates  = InternTemplates(pool);
        private readonly int[] _services   = InternServices(pool);

        private static int[] BuildCumulative()
        {
            var c = new int[100];
            int scenario = 0, used = 0;
            for (int i = 0; i < 100; i++)
            {
                while (scenario < Weights.Length - 1 && used >= Weights[scenario]) { scenario++; used = 0; }
                c[i] = scenario; used++;
            }
            return c;
        }

        private static int[] InternTemplates(StringInternPool pool)
        {
            var r = new int[Scenarios.Length];
            for (int i = 0; i < r.Length; i++) r[i] = pool.Intern(Scenarios[i].Template);
            return r;
        }

        private static int[] InternServices(StringInternPool pool)
        {
            var r = new int[Services.Length];
            for (int i = 0; i < r.Length; i++) r[i] = pool.Intern(Services[i]);
            return r;
        }

        public bool Write(StorageEngine engine, ArrayBufferWriter<byte> buf, long ts)
        {
            int scenario = _cumulative[rng.Next(100)];
            int service  = rng.Next(Services.Length);
            bool traced = scenario switch
            {
                0 => rng.Next(2) == 0,
                1 or 3 or 5 => rng.Next(3) == 0,
                10 or 11 => true,
                _ => false,
            };
            ExceptionInfo? exc = scenario switch { 10 or 12 => GatewayTimeout, 11 => InvalidState, _ => null };

            var header = new LogEventHeader
            {
                TimestampUtcTicks        = ts,
                Level                    = Scenarios[scenario].Level,
                MessageTemplatePoolIndex = _templates[scenario],
                ServiceNamePoolIndex     = _services[service],
            };
            if (traced)
            {
                header.TraceIdHi = (ulong)rng.NextInt64() | 1;
                header.TraceIdLo = (ulong)rng.NextInt64();
                header.SpanId    = (ulong)rng.NextInt64() | 1;
            }

            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            switch (scenario)
            {
                case 0:
                    w.WriteMapHeader(6);
                    w.Write("Method");     w.Write(Methods[rng.Next(Methods.Length)]);
                    w.Write("Path");       w.Write(Paths[rng.Next(Paths.Length)]);
                    w.Write("StatusCode"); w.Write(rng.Next(50) == 0 ? 500 : 200);
                    w.Write("Elapsed");    w.Write(Math.Round(rng.NextDouble() * 240 + 1.5, 2));
                    w.Write("ClientIp");   w.Write($"10.220.{rng.Next(16)}.{rng.Next(250)}");
                    w.Write("RequestId");  w.Write($"req-{rng.NextInt64():x12}");
                    break;
                case 1:
                    w.WriteMapHeader(5);
                    w.Write("PaymentId"); w.Write($"pay_{rng.NextInt64():x12}");
                    w.Write("Amount");    w.Write(Math.Round(rng.NextDouble() * 4900 + 100, 2));
                    w.Write("Currency");  w.Write("AED");
                    w.Write("Provider");  w.Write(Providers[rng.Next(Providers.Length)]);
                    w.Write("WalletId");  w.Write(rng.Next(1_000_000, 9_999_999));
                    break;
                case 2:
                    w.WriteMapHeader(3);
                    w.Write("UserId");       w.Write(rng.Next(1000, 99999));
                    w.Write("ClientIp");     w.Write($"10.220.{rng.Next(16)}.{rng.Next(250)}");
                    w.Write("AuthProvider"); w.Write(Auth[rng.Next(Auth.Length)]);
                    break;
                case 3:
                    w.WriteMapHeader(3);
                    w.Write("OrderId");    w.Write(rng.Next(100_000, 999_999));
                    w.Write("CustomerId"); w.Write(rng.Next(1000, 99999));
                    w.Write("Order");
                    w.WriteMapHeader(3);
                    w.Write("Id");        w.Write(rng.Next(100_000, 999_999));
                    w.Write("ItemCount"); w.Write(rng.Next(1, 9));
                    w.Write("Total");     w.Write(Math.Round(rng.NextDouble() * 900 + 20, 2));
                    break;
                case 4:
                    w.WriteMapHeader(3);
                    w.Write("WalletId"); w.Write(rng.Next(1_000_000, 9_999_999));
                    w.Write("Balance");  w.Write(Math.Round(rng.NextDouble() * 90000, 2));
                    w.Write("Currency"); w.Write("AED");
                    break;
                case 5:
                    w.WriteMapHeader(3);
                    w.Write("Elapsed");     w.Write(Math.Round(rng.NextDouble() * 45 + 0.3, 2));
                    w.Write("CommandType"); w.Write(rng.Next(2) == 0 ? "SELECT" : "UPDATE");
                    w.Write("Rows");        w.Write(rng.Next(0, 500));
                    break;
                case 6:
                    w.WriteMapHeader(3);
                    w.Write("MessageId"); w.Write($"msg-{rng.NextInt64():x12}");
                    w.Write("Queue");     w.Write(Queues[rng.Next(Queues.Length)]);
                    w.Write("Elapsed");   w.Write(Math.Round(rng.NextDouble() * 80 + 0.5, 2));
                    break;
                case 7:
                    w.WriteMapHeader(1);
                    w.Write("CacheKey"); w.Write($"wallet:{rng.Next(1_000_000, 9_999_999)}:balance");
                    break;
                case 8:
                    w.WriteMapHeader(4);
                    w.Write("Attempt");   w.Write(rng.Next(1, 5));
                    w.Write("RequestId"); w.Write($"req-{rng.NextInt64():x12}");
                    w.Write("Endpoint");  w.Write(Paths[rng.Next(Paths.Length)]);
                    w.Write("DelayMs");   w.Write(250 * (1 << rng.Next(4)));
                    break;
                case 9:
                    w.WriteMapHeader(3);
                    w.Write("Elapsed");     w.Write(Math.Round(rng.NextDouble() * 4000 + 1000, 1));
                    w.Write("Threshold");   w.Write(1000);
                    w.Write("CommandType"); w.Write("SELECT");
                    break;
                case 10:
                    w.WriteMapHeader(2);
                    w.Write("PaymentId"); w.Write($"pay_{rng.NextInt64():x12}");
                    w.Write("Timeout");   w.Write(30000);
                    break;
                case 11:
                    w.WriteMapHeader(2);
                    w.Write("Path");      w.Write(Paths[rng.Next(Paths.Length)]);
                    w.Write("RequestId"); w.Write($"req-{rng.NextInt64():x12}");
                    break;
                default:
                    w.WriteMapHeader(1);
                    w.Write("Component"); w.Write("PaymentDispatcher");
                    break;
            }
            w.Flush();
            return engine.TryWrite(header, buf.WrittenSpan, exception: exc);
        }
    }
}
