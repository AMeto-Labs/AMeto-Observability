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
/// Issue #80: the segment-index cache almost never hits, so a filtering query re-parses whole
/// index sections that a query a moment earlier had already parsed.
///
/// <para>THE STAND. 1.2 M events in 24 segments, written once by the current writer through the
/// production flush (four tier flushes, each split six ways by level), in the event mix
/// <c>tools/loggen</c> sends — the generator the load runs of #79 used, so the sections have the
/// shape the cache met there: request, payment and message ids, trace and span ids on a share of
/// the rows, structured exceptions on the errors. Two filters: <c>@mt like '%timeout%'</c>,
/// which has no equality hint and so reads the inverted AND trigram sections of every group, and
/// <c>Provider = 'UnionPay'</c>, which the bloom gate narrows to the groups that could hold it
/// (the Information segments) and the inverted index to 4 % of their rows.</para>
///
/// <para>THREE READINGS, because they answer different questions.</para>
/// <list type="bullet">
/// <item><see cref="PhaseBreakdown"/> walks the prefilter's steps one group at a time on the
/// test thread — the same calls the executor makes, in its order — so every phase gets its own
/// clock and its own per-thread allocation counter: section rent + index load, the search
/// (narrowing and intersection), and the candidate scan. This is the miss path, the one the
/// cache is there to skip.</item>
/// <item><see cref="RepeatedFilter"/> runs each filter five times through a
/// <see cref="QueryExecutor"/> with the production cache budget (256 MB, 96 MB native) and
/// reports what <c>/api/diagnostics</c> reports — the <c>indexCache*</c> counters — per run.
/// The executor prefilters eight groups at a time on the pool, so allocation there is the
/// process-wide counter; the assembly runs one probe at a time, which makes it attributable.</item>
/// <item><see cref="StandBudget"/> repeats that at the 512 MB stand's derived budget
/// (46 MB, 25.6 MB native) against the cache switched off — issue option 4.</item>
/// </list>
///
/// <para>Every executor run is checked against the uncached executor's result, row for row: a
/// cache may only ever skip work.</para>
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

        foreach (var text in new[] { Like, Equality })
        {
            var filter = CompiledFilter.Compile(text);
            Assert.Null(filter.DerivedLevels);   // the walk below mirrors the executor without level hints

            Walk(filter, printGroups: false);                     // warm: JIT, page cache
            var runs = new Phases[Runs];
            for (int r = 0; r < Runs; r++) runs[r] = Walk(filter, printGroups: r == 0 && text == Like);

            var m = Phases.Median(runs);
            _out.WriteLine("");
            _out.WriteLine($"── {text}: median of {Runs} single-threaded walks over {m.Groups} groups ({m.GroupsLoaded} loaded) ──");
            _out.WriteLine($"  open segments          {m.OpenMs,9:F1} ms   {Mb(m.OpenBytes),9:F1} MB");
            _out.WriteLine($"  bloom gate             {m.BloomMs,9:F1} ms   {Mb(m.BloomBytes),9:F1} MB");
            _out.WriteLine($"  rent + index load      {m.LoadMs,9:F1} ms   {Mb(m.LoadBytes),9:F1} MB");
            _out.WriteLine($"  search / intersection  {m.SearchMs,9:F1} ms   {Mb(m.SearchBytes),9:F1} MB");
            _out.WriteLine($"  candidate scan         {m.ScanMs,9:F1} ms   {Mb(m.ScanBytes),9:F1} MB   ({m.Candidates} candidates, {m.Matches} matches)");
            _out.WriteLine($"  total                  {m.TotalMs,9:F1} ms   {Mb(m.TotalBytes),9:F1} MB");
            _out.WriteLine($"  sections read          {Mb(m.PackedBytes),9:F1} MB packed -> {Mb(m.RetainedBytes),9:F1} MB charged to the cache (ApproxRetainedBytes)");

            Assert.True(m.Matches > 0, $"{text} matched nothing — the corpus lost its shape");
        }
    }

    // ── 2. The repeated filter at the production budget ──────────────────────

    [Fact]
    public async Task RepeatedFilter()
    {
        var plain = NewExecutor(null);
        var expectLike = await DrainAsync(plain, Like);
        var expectEq   = await DrainAsync(plain, Equality);

        using var cache = new SegmentIndexCache(ProdBudget, ProdNative, TimeSpan.Zero);
        var cached = NewExecutor(cache);

        _out.WriteLine($"production budget: {Mb(ProdBudget):F0} MB total, {Mb(ProdNative):F0} MB native");
        var like = await RepeatAsync(cached, cache, Like, expectLike);
        var eq   = await RepeatAsync(cached, cache, Equality, expectEq);
        var cold = await RepeatAsync(plain,  null,  Like, expectLike, label: "uncached");

        _out.WriteLine("");
        _out.WriteLine($"LIKE warm (runs 2..{Runs}): hit {like.WarmHitPct:F1} %, median {like.WarmMedianMs:F1} ms, {Mb(like.WarmMedianBytes):F1} MB   | uncached median {cold.WarmMedianMs:F1} ms, {Mb(cold.WarmMedianBytes):F1} MB");
        _out.WriteLine($"EQ   warm (runs 2..{Runs}): hit {eq.WarmHitPct:F1} %, median {eq.WarmMedianMs:F1} ms, {Mb(eq.WarmMedianBytes):F1} MB");
    }

    // ── 3. Option 4: the 512 MB stand's budget, or no cache at all ───────────

    [Fact]
    public async Task StandBudget()
    {
        var plain = NewExecutor(null);
        var expectLike = await DrainAsync(plain, Like);
        var expectEq   = await DrainAsync(plain, Equality);

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
        public double OpenMs, BloomMs, LoadMs, SearchMs, ScanMs;
        public long   OpenBytes, BloomBytes, LoadBytes, SearchBytes, ScanBytes;
        public long   PackedBytes, RetainedBytes, Candidates, Matches;
        public int    Groups, GroupsLoaded;

        public readonly double TotalMs    => OpenMs + BloomMs + LoadMs + SearchMs + ScanMs;
        public readonly long   TotalBytes => OpenBytes + BloomBytes + LoadBytes + SearchBytes + ScanBytes;

        public static Phases Median(Phases[] runs)
        {
            var m = runs[0];
            m.OpenMs   = Med(runs, static p => p.OpenMs);   m.OpenBytes   = (long)Med(runs, static p => p.OpenBytes);
            m.BloomMs  = Med(runs, static p => p.BloomMs);  m.BloomBytes  = (long)Med(runs, static p => p.BloomBytes);
            m.LoadMs   = Med(runs, static p => p.LoadMs);   m.LoadBytes   = (long)Med(runs, static p => p.LoadBytes);
            m.SearchMs = Med(runs, static p => p.SearchMs); m.SearchBytes = (long)Med(runs, static p => p.SearchBytes);
            m.ScanMs   = Med(runs, static p => p.ScanMs);   m.ScanBytes   = (long)Med(runs, static p => p.ScanBytes);
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
    /// (<c>QueryExecutor.PrefilterSegmentsAsync</c>, then <c>ScanSegmentAsync</c>), on this thread.
    /// </summary>
    private Phases Walk(CompiledFilter filter, bool printGroups)
    {
        var  factory      = new SegmentIndexReaderFactory();
        bool needTrigram  = filter.GetTrigramHints().Count > 0;
        bool hasIndexHint = !filter.IsMatchAll && filter.TryGetIndexHint(out _, out _);
        var  p            = new Phases();

        if (printGroups)
            _out.WriteLine($"{"segment id",10} {"g",2} {"level",-11} {"events",8} {"inverted",10} {"trigram",10} {"bloom",9} {"decoded",10}");

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

                a = GC.GetAllocatedBytesForCurrentThread(); t = Stopwatch.GetTimestamp();
                using var bloomSec = reader.RentBloomFilterBytes(g);
                bool bloomPass = true;
                if (hasIndexHint)
                {
                    using var bloom = SegmentBloomFilter.Deserialise(bloomSec.Span);
                    bloomPass = QueryExecutor.PassesBloomGate(filter, bloom);
                }
                Charge(ref p.BloomMs, ref p.BloomBytes, a, t);
                if (!bloomPass) continue;
                p.GroupsLoaded++;

                a = GC.GetAllocatedBytesForCurrentThread(); t = Stopwatch.GetTimestamp();
                using var invSec = reader.RentInvertedIndexBytes(g);
                using var triSec = needTrigram ? reader.RentTrigramIndexBytes(g) : default;
                var idx = factory.Create(invSec.Span, triSec.Span, bloomSec.Span);
                Charge(ref p.LoadMs, ref p.LoadBytes, a, t);

                p.PackedBytes   += invSec.Span.Length + triSec.Span.Length + bloomSec.Span.Length;
                p.RetainedBytes += idx.ApproxRetainedBytes;
                if (printGroups)
                {
                    using var fullTri = reader.RentTrigramIndexBytes(g);
                    _out.WriteLine($"{info.Id.Value,10} {g,2} {info.MinLevel,-11} {grp.EventCount,8} " +
                                   $"{Mb(invSec.Span.Length),7:F1} MB {Mb(fullTri.Span.Length),7:F1} MB {Mb(bloomSec.Span.Length),6:F1} MB {Mb(idx.ApproxRetainedBytes),7:F1} MB");
                }

                a = GC.GetAllocatedBytesForCurrentThread(); t = Stopwatch.GetTimestamp();
                bool keep;
                uint[]? groupCandidates;
                bool everyRow;
                using (idx)
                    keep = QueryExecutor.TryNarrowWithIndex(filter, idx, null, grp.EventCount, out groupCandidates, out everyRow);
                Charge(ref p.SearchMs, ref p.SearchBytes, a, t);
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
            Engine = new StorageEngine(
                Options.Create(opts),
                new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
                NullLogger<StorageEngine>.Instance);
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
