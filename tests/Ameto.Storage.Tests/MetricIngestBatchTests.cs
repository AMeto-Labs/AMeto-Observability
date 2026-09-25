using Ameto.Core;
using Ameto.Metrics;
using Ameto.Metrics.Storage;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// The ingest path's batch shape: the write-ahead log takes its lock once per OTLP batch
/// rather than once per data point, and the engine logs the whole batch before any of it is
/// published to the hot tier.
/// </summary>
public sealed class MetricIngestBatchTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-mbatch-" + Guid.NewGuid().ToString("N"));
    private readonly List<MetricStorageEngine>  _engines = [];
    private readonly List<MetricWriteAheadLog>  _wals    = [];

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        for (int i = _wals.Count - 1; i >= 0; i--)
            try { _wals[i].Dispose(); } catch { }
        for (int i = _engines.Count - 1; i >= 0; i--)
            try { await _engines[i].DisposeAsync(); } catch { }
        try { Directory.Delete(_dir, true); } catch { }
    }

    /// <summary>
    /// One MiB of log — the floor <see cref="MetricsOptions"/> allows — so a batch of 20 000
    /// 52-byte entries has to grow it, which is the fault this class injects. The hot-tier
    /// threshold is left at the budget cap so nothing flushes underneath the test.
    /// </summary>
    private static readonly MetricsOptions SmallLog = new()
    {
        HotTierBytes    = MemoryBudgets.MetricHotTierCapBytes,
        MinFlushBytes   = MemoryBudgets.MetricHotTierCapBytes / 10,
        WalInitialBytes = 1L * 1024 * 1024,
    };

    private MetricStorageEngine NewEngine(string dir, MetricsOptions options)
    {
        var e = new MetricStorageEngine(dir, NullLogger<MetricStorageEngine>.Instance, options);
        _engines.Add(e);
        return e;
    }

    private MetricWriteAheadLog OpenWal(string dir)
    {
        var w = MetricWriteAheadLog.Open(Path.Combine(dir, "metrics.wal"), 1L * 1024 * 1024);
        _wals.Add(w);
        return w;
    }

    private static LabelSet Labels(params (string K, string V)[] pairs)
    {
        var kv = new KeyValuePair<string, string>[pairs.Length];
        for (int i = 0; i < pairs.Length; i++) kv[i] = new(pairs[i].K, pairs[i].V);
        return new LabelSet(kv);
    }

    private static MetricIngestItem Scalar(string name, long nano, double value, LabelSet? labels = null) => new()
    {
        Name              = name,
        Kind              = MetricKind.Gauge,
        Unit              = "ms",
        Labels            = labels ?? LabelSet.Empty,
        TimestampUnixNano = nano,
        ScalarValue       = value,
    };

    private static MetricIngestItem Histo(string name, long nano, long count, double sum) => new()
    {
        Name              = name,
        Kind              = MetricKind.Histogram,
        Unit              = "ms",
        Labels            = Labels(("route", "/x"), ("method", "GET")),
        TimestampUnixNano = nano,
        HistogramCount    = count,
        HistogramSum      = sum,
        BucketBounds      = [1, 5, 10],
        BucketCounts      = [count / 4, count / 4, count / 4, count - 3 * (count / 4)],
    };

    private static MetricDataPoint PointOf(MetricIngestItem i) => new()
    {
        TimestampUnixNano = i.TimestampUnixNano,
        Value             = i.Kind == MetricKind.Histogram
                                ? (i.HistogramCount > 0 ? i.HistogramSum / i.HistogramCount : 0)
                                : i.ScalarValue,
        Count             = i.HistogramCount,
        Sum               = i.HistogramSum,
        BucketCounts      = i.BucketCounts,
    };

    /// <summary>
    /// The batch overload writes the SAME BYTES the point-by-point one does — the entries, the
    /// series pool and the header's claimed end of data, all of it. This is the parity gate for
    /// the change: one lock acquisition instead of N is only allowed to change how long the log
    /// is held, never what it holds.
    /// </summary>
    [Fact]
    public void A_batch_append_writes_exactly_what_point_by_point_appends_write()
    {
        string oneByOne = Path.Combine(_dir, "one-by-one");
        string batched  = Path.Combine(_dir, "batched");
        Directory.CreateDirectory(oneByOne);
        Directory.CreateDirectory(batched);

        long baseNano = 1_785_300_000_000_000_000L;
        var  items    = new MetricIngestItem[64];

        for (int i = 0; i < items.Length; i++)
        {
            items[i]  = i % 3 == 0
                ? Histo("http.server.duration", baseNano + i * 1_000L, 40 + i, 12.5 * i)
                : Scalar("http.server.active", baseNano + i * 1_000L, i * 1.5,
                         Labels(("service.name", "api"), ("node", "n" + (i % 4))));

        }

        var a = OpenWal(oneByOne);
        for (int i = 0; i < items.Length; i++) a.Append(items[i], PointOf(items[i]));
        a.Dispose();

        var b = OpenWal(batched);
        b.Append(items);
        b.Dispose();

        Assert.Equal(File.ReadAllBytes(Path.Combine(oneByOne, "metrics.wal")),
                     File.ReadAllBytes(Path.Combine(batched,  "metrics.wal")));
        Assert.Equal(File.ReadAllBytes(Path.Combine(oneByOne, "metrics.wal.pool")),
                     File.ReadAllBytes(Path.Combine(batched,  "metrics.wal.pool")));
    }

    /// <summary>
    /// The batch resolves its series indices from the concurrent registry WITHOUT the write
    /// lock, which is the point of the change — a hit needs no exclusion. What it must not do is
    /// carry an index across the one event that voids every index at once: a commit that empties
    /// the log truncates the pool file and re-issues indices from zero.
    ///
    /// <para>Driven by the <c>OnSeriesResolvedForTest</c> seam, which IS the window between the
    /// lock-free lookups and the lock, rather than by two threads and a hopeful sleep.</para>
    /// </summary>
    [Fact]
    public void An_index_resolved_before_a_commit_emptied_the_log_is_not_reused()
    {
        string dir = Path.Combine(_dir, "epoch");
        Directory.CreateDirectory(dir);
        var wal = OpenWal(dir);

        long nano = 1_785_300_000_000_000_000L;
        var  s    = Scalar("m", nano,     1.0, Labels(("series", "s")));
        var  t    = Scalar("m", nano + 1, 2.0, Labels(("series", "t")));
        wal.Append(s, PointOf(s));                       // series index 0
        wal.Append(t, PointOf(t));                       // series index 1

        bool fired = false;
        wal.OnSeriesResolvedForTest = () =>
        {
            if (fired) return;
            fired = true;
            // Empties the log: the registry is cleared, the pool file truncated, and the next
            // registration starts again at 0 — so the index just resolved for "t" (1) now names
            // a pool record that does not exist.
            ulong g = wal.BeginFlush();
            Assert.Equal(MetricWalCommit.Committed, wal.CommitFlush(g));
        };

        try
        {
            var again = new[] { Scalar("m", nano + 2, 3.0, Labels(("series", "t"))) };
            wal.Append(again);
        }
        finally { wal.OnSeriesResolvedForTest = null; }

        Assert.True(fired, "the seam never ran; the test proved nothing");

        var recovered = wal.ReadAll(out int unresolved);
        Assert.Equal(0, unresolved);                     // pre-fix: 1 — index 1 resolves to nothing
        var point = Assert.Single(recovered);
        Assert.Equal(3.0, point.Point.Value);
        Assert.Contains(("series", "t"), point.Labels.Pairs);
    }

    /// <summary>
    /// A batch whose log append cannot grow the file publishes NONE of itself.
    ///
    /// <para>The engine logs the whole batch under one lock and only then walks it into the hot
    /// tier, and the log stores its file-header write offset once the batch is down — so the
    /// entries a failed <c>Grow</c> left behind are never claimed, never replayed, and the tier
    /// never saw the point they belong to. Appending point by point made the failure a PREFIX
    /// instead: every point before the one that could not grow was already logged and already
    /// queryable, while the call reported failure and the exporter retried the whole batch.</para>
    /// </summary>
    [Fact]
    public async Task A_batch_whose_log_cannot_grow_publishes_none_of_itself()
    {
        var engine = NewEngine(_dir, SmallLog);
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;

        // Fill most of the 1 MiB log (20 164 entries of room at 52 B) without needing a grow.
        var filler = new MetricIngestItem[15_000];
        for (int i = 0; i < filler.Length; i++)
            filler[i] = Scalar("filler.metric", baseNano + i * 1_000L, 1.0, Labels(("s", (i & 7).ToString())));
        engine.Ingest(filler);

        try
        {
            engine.WalForTest.BeforeResize = static _ => throw new IOException("disk full (test seam)");

            var poison = new MetricIngestItem[20_000];
            for (int i = 0; i < poison.Length; i++)
                poison[i] = Scalar("poison.metric", baseNano + i * 1_000L, 7.0,
                                   Labels(("s", (i & 7).ToString())));

            Assert.ThrowsAny<IOException>(() => engine.Ingest(poison));
        }
        finally { engine.WalForTest.BeforeResize = null; }

        // Nothing of the poison batch is visible. Pre-change this is the whole prefix the log
        // had room for — thousands of points, in the tier and in the log, from a call that threw.
        int visible = 0;
        await foreach (var s in engine.QueryAsync("poison.metric"))
            visible += s.Points.Count;
        Assert.Equal(0, visible);

        // And the batch that DID fit is untouched by the failure beside it.
        int fillerPoints = 0;
        await foreach (var s in engine.QueryAsync("filler.metric"))
            fillerPoints += s.Points.Count;
        Assert.Equal(filler.Length, fillerPoints);
    }

    /// <summary>
    /// The rarer half of the batch shape: a batch carrying points the future-skew guard refuses.
    /// The accepted set is then NOT the batch, so it is compacted into a pooled array and the
    /// original ordinal of each survivor is carried alongside — because the exemplar pass indexes
    /// its handover array by the ordinal in the ORIGINAL batch, not by position in the accepted
    /// set. Drop the ordinal map and the exemplars are filed against the wrong series' labels
    /// (or, at the end of a batch, against no series at all).
    /// </summary>
    [Fact]
    public async Task Refused_points_do_not_shift_the_exemplars_of_the_points_around_them()
    {
        var engine   = NewEngine(Path.Combine(_dir, "skew"), SmallLog);
        long nowNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        long sane    = nowNano - 60_000_000_000L;
        long future  = nowNano + (long)TimeSpan.FromHours(48).TotalSeconds * 1_000_000_000L;

        // Round one establishes the five series and, with them, the CANONICAL LabelSet instance
        // each one is known by. That instance is the discriminator below: a ring entry gets it
        // from the handover array, and gets the point's own (equal but distinct) instance when
        // the handover misses.
        var warm = new MetricIngestItem[5];
        for (int i = 0; i < warm.Length; i++)
            warm[i] = Scalar("skew.metric", sane - 1_000L + i, i, Labels(("series", "s" + i)));
        engine.Ingest(warm);

        var canonical = new LabelSet[5];
        for (int i = 0; i < 5; i++) canonical[i] = warm[i].Labels;

        // Round two: ordinals 0 and 2 are refused by the future guard; 1, 3 and 4 survive, and
        // 3 and 4 carry exemplars — so the accepted POSITIONS of the two exemplar-carrying items
        // (1 and 2) differ from their ORIGINAL ordinals (3 and 4). Fresh LabelSet instances, as
        // the OTLP parser builds per point.
        var batch = new MetricIngestItem[5];
        for (int i = 0; i < batch.Length; i++)
            batch[i] = new MetricIngestItem
            {
                Name              = "skew.metric",
                Unit              = "ms",
                Kind              = MetricKind.Gauge,
                Labels            = Labels(("series", "s" + i)),
                TimestampUnixNano = i is 0 or 2 ? future : sane + i,
                ScalarValue       = i,
                Exemplars         = i is 3 or 4
                    ? [new MetricExemplar
                       {
                           TimestampUnixNano = sane + i,
                           Value             = i,
                           TraceId           = "4bf92f3577b34da6a3ce929d0e0e4736",
                           SpanId            = "00f067aa0ba902b7",
                       }]
                    : null,
            };

        Assert.Equal(2, engine.Ingest(batch));                    // the two refused points

        int stored = 0;
        await foreach (var s in engine.QueryAsync("skew.metric")) stored += s.Points.Count;
        Assert.Equal(5 + 3, stored);                              // five warm-up, three survivors

        var exemplars = engine.GetExemplars("skew.metric", null, null, null);
        Assert.Equal(2, exemplars.Count);

        // Each entry holds the CANONICAL instance of its own series' labels. Reference equality,
        // not value equality: with the ordinals shifted, the handover slot for ordinal 3 or 4 is
        // empty, the pass silently falls back to the point's own label set, and every assertion
        // that only compares VALUES still passes while the ring is holding a per-point LabelSet
        // it is now the sole owner of.
        foreach (var ex in exemplars)
        {
            string series = "";
            foreach (var (k, v) in ex.Labels.Pairs) if (k == "series") series = v;
            Assert.True(series is "s3" or "s4", $"exemplar filed against {series}, not s3/s4");
            Assert.Same(canonical[series[1] - '0'], ex.Labels);
        }
    }

    /// <summary>
    /// THE CROSSING SCHEDULES ITS FLUSH BEFORE IT PAYS FOR ANYTHING ELSE. A batch that takes the
    /// tier over its threshold AND the log past its 3/4 mark owes three things on its way out of
    /// <c>Ingest</c>: the threshold flush, the log's pre-grow (a file extension) and the exemplar
    /// pass. The flush is the one another thread runs, so it is scheduled first and the other two
    /// follow on this thread. Judged at the engine's own scheduling seam, on the ingest thread: at
    /// that instant this thread has not grown the log and no exemplar pass has run. The flush is
    /// parked at its snapshot so its commit cannot empty the log under the pre-grow; afterwards
    /// both have still happened, in this same call. Revert to checking the threshold last and the
    /// seam sees one growth and one exemplar pass already paid.
    /// </summary>
    [Fact]
    public async Task A_crossing_schedules_its_flush_before_the_growth_and_exemplars_it_also_owes()
    {
        string dir = Path.Combine(_dir, "crossing-order");
        var engine = NewEngine(dir, new MetricsOptions
        {
            HotTierBytes    = 100_000,        // 1 563 scalar points at 64 B
            MinFlushBytes   = 100_000,
            WalInitialBytes = 128 * 1024,     // 2 520 entries of 52 B; the 3/4 mark at 1 890
        });

        int me = Environment.CurrentManagedThreadId;
        int grownHere = 0;                                     // this thread's growths only
        engine.WalForTest.OnGrowMappedForTest = () =>
        {
            if (Environment.CurrentManagedThreadId == me) grownHere++;
        };

        using var releaseFlush = new ManualResetEventSlim();
        engine.OnSnapshotTakenForTest = () => releaseFlush.Wait();

        Task? scheduled = null;
        int  grownAtSchedule           = -1;
        long exemplarPassesAtSchedule  = -1;
        engine.OnThresholdFlushScheduledForTest = t =>
        {
            scheduled                = t;
            grownAtSchedule          = grownHere;
            exemplarPassesAtSchedule = engine.ExemplarPasses;
        };

        // 2 100 points: 109 200 B of log (past the 98 304 mark, inside 131 072) and 134 400 B of
        // tier (past 100 000) — one call crosses both. The first carries an exemplar.
        long now   = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        var  batch = new MetricIngestItem[2_100];
        for (int i = 0; i < batch.Length; i++)
            batch[i] = Scalar("crossing.metric", now - batch.Length + i, i, Labels(("s", (i & 7).ToString())));
        batch[0] = new MetricIngestItem
        {
            Name              = batch[0].Name,
            Kind              = batch[0].Kind,
            Unit              = batch[0].Unit,
            Labels            = batch[0].Labels,
            TimestampUnixNano = batch[0].TimestampUnixNano,
            ScalarValue       = batch[0].ScalarValue,
            Exemplars         = [new MetricExemplar { TimestampUnixNano = batch[0].TimestampUnixNano, Value = 1 }],
        };

        try
        {
            engine.Ingest(batch);

            Assert.NotNull(scheduled);
            Assert.True(grownAtSchedule == 0 && exemplarPassesAtSchedule == 0,
                $"the crossing scheduled its flush only after paying for {grownAtSchedule} growth(s) and "
              + $"{exemplarPassesAtSchedule} exemplar pass(es)");
            Assert.Equal(1, grownHere);                        // both still paid, in this call
            Assert.Equal(1, engine.ExemplarPasses);
        }
        finally { releaseFlush.Set(); }

        await scheduled!;
        Assert.Equal(0, engine.HotPointCount);
    }

    /// <summary>
    /// The engine's log replays through <see cref="MetricLabelInterner.Shared"/> — the interner the
    /// OTLP parsers intern into. <c>MetricWriteAheadLog.Open</c> takes an optional interner so the
    /// replay facts can run on a private one; an engine that passed its own would still pass every
    /// replay fact while keeping a second copy of every label the live path already holds.
    /// </summary>
    [Fact]
    public void The_engine_replays_through_the_interner_the_parsers_intern_into()
    {
        var engine = NewEngine(Path.Combine(_dir, "shared-interner"), SmallLog);
        Assert.Same(MetricLabelInterner.Shared, engine.WalForTest.Interner);
    }
}
