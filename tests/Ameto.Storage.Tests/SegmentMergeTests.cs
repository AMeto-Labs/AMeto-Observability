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
        NullLogger<StorageEngine>.Instance)
    {
        // These tests run without the index builder and verify the scan path;
        // production merges wait until the builder is wired.
        _allowIndexlessMerge = true,
    };

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static byte[] Props(int i, int padBytes = 0)
    {
        var buf = new ArrayBufferWriter<byte>(64 + padBytes);
        var w = new MessagePackWriter(buf);
        w.WriteMapHeader(padBytes > 0 ? 3 : 2);
        w.Write("n");   w.Write((long)i);
        w.Write("key"); w.Write("wallet:" + i);
        if (padBytes > 0) { w.Write("pad"); w.Write(new string('x', padBytes)); }
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>
    /// Writes <paramref name="count"/> events and flushes them into one small segment.
    ///
    /// <para>Single-level by default: a flush now writes ONE SEGMENT PER LEVEL, so a
    /// mixed-level round would produce six segments and every "N rounds ⇒ N segments"
    /// assertion below would be counting something else. Tests that care about level
    /// behaviour pass <paramref name="fixedLevel"/> explicitly.</para>
    /// </summary>
    private async Task WriteSegmentAsync(int round, int count, LogLevel? fixedLevel = null, long? baseTicks = null, int padBytes = 0)
    {
        for (int i = 0; i < count; i++)
        {
            int n = round * 1000 + i;
            var header = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)(round * 100_000 + i)).RawValue,
                TimestampUtcTicks        = (baseTicks ?? DateTime.UtcNow.Ticks) + n * TimeSpan.TicksPerMillisecond,
                Level                    = fixedLevel ?? LogLevel.Information,
                MessageTemplatePoolIndex = _engine.TemplatePool.Intern("evt {n} round " + round % 3),
                ServiceNamePoolIndex     = _engine.TemplatePool.Intern("Svc." + round % 4),
                TraceIdHi                = (ulong)(n + 1),
                TraceIdLo                = (ulong)(n + 2),
                SpanId                   = (ulong)(n + 3),
            };
            var exc = n % 50 == 0
                ? new ExceptionInfo { Type = "System.InvalidOperationException", Message = "boom " + n }
                : null;
            Assert.True(_engine.TryWrite(header, Props(n, padBytes), exception: exc));
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

    /// <summary>
    /// The merge cursor deduplicates templates and service names by their UTF-8 bytes, so the
    /// values worth carrying through a real merge are the ones where bytes and characters part
    /// company — Cyrillic at two bytes a character, an emoji at four bytes for two UTF-16 units,
    /// and a pair that agrees for its whole ASCII prefix and differs after it.
    /// </summary>
    [Fact]
    public async Task Merge_PreservesTemplatesThatAreNotAscii()
    {
        string[] templates = ["Заказ {id} отклонён", "order {id} declined", "ordér {id} declinéd", "payment 🙂 {id}"];
        string[] services  = ["Платежи.Ядро", "Payments.Core", "Płatności"];
        long     origin    = MergeBucketGrid.SealedBucketStart(LogLevel.Information);

        for (int round = 0; round < 6; round++)
        {
            for (int i = 0; i < 40; i++)
            {
                int n = round * 1000 + i;
                Assert.True(_engine.TryWrite(new LogEventHeader
                {
                    Id                       = new EventId(0u, (uint)(round * 100_000 + i)).RawValue,
                    TimestampUtcTicks        = origin + n * TimeSpan.TicksPerMillisecond,
                    Level                    = LogLevel.Information,
                    MessageTemplatePoolIndex = _engine.TemplatePool.Intern(templates[n % templates.Length]),
                    ServiceNamePoolIndex     = _engine.TemplatePool.Intern(services[n % services.Length]),
                }, Props(n), exception: null));
            }
            await _engine.FlushHotTierAsync();
        }
        var before = ReadEverything();
        Assert.Equal(240, before.Count);

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        Assert.Single(_engine.ListSegments());

        var after = ReadEverything();
        Assert.Equal(before.Count, after.Count);
        for (int i = 0; i < before.Count; i++)
        {
            Assert.Equal(before[i].Id,       after[i].Id);
            Assert.Equal(before[i].Template, after[i].Template);
            Assert.Equal(before[i].Service,  after[i].Service);
        }
        // Every distinct spelling survived as itself — a comparer that conflated two of them
        // would leave the merged file short of one.
        Assert.Equal(templates.Length, after.Select(e => e.Template).Distinct().Count());
        Assert.Equal(services.Length,  after.Select(e => e.Service).Distinct().Count());
    }

    /// <summary>
    /// ONE STRING PER DISTINCT VALUE, not one per event — the thing the dedup table is for, and
    /// the thing it did not do while the cursor decoded a template and a service name out of the
    /// block before asking the table whether it already held them. The table always answered yes;
    /// the two fresh strings went to Gen0 unread.
    ///
    /// <para>Measured on the merge SOURCE alone, with no writer downstream, because that is the
    /// only resolution at which the answer is visible. Through a whole merge pass the two strings
    /// come to ~88 B an event inside a total of a few megabytes, which is why
    /// <see cref="Merge_AllocationStaysFlatAcrossALargeBacklog"/> — the assertion this claim used
    /// to be written beside — reads 5 MB with the decode and 3 MB without it, against a bound of
    /// 20. Here they are nearly the whole reading: the cursor otherwise hands out spans over the
    /// block it already holds, so a drained source allocates essentially nothing per event.</para>
    ///
    /// <para>The warm-up pass is not ceremony. The block buffers come from
    /// <see cref="System.Buffers.ArrayPool{T}"/>, and a first rent against an empty pool
    /// allocates — over 20 000 events that alone would be larger than the signal.</para>
    /// </summary>
    [Fact]
    public async Task Merge_BuildsAStringPerDistinctValue_NotPerEvent()
    {
        for (int round = 0; round < 8; round++)
            await WriteSegmentAsync(round, 2_500);
        var sources = Directory.GetFiles(Path.Combine(_dir, "segments"), "*.seg").Order().ToList();
        Assert.Equal(8, sources.Count);

        Assert.Equal(20_000, DrainMerge(sources, out _));

        long   events   = DrainMerge(sources, out long bytes);
        double perEvent = bytes / (double)events;

        // Three templates and four service names across the whole 20 000, so the honest cost of
        // this loop is seven strings. 8 B/event allows over a hundred of them and is still an
        // order of magnitude under the ~88 B two throwaway strings a row come to.
        Assert.True(perEvent < 8.0,
            $"the merge source allocated {perEvent:F1} B/event draining {events:N0} events — it is " +
            "building a template and a service name per row again, for values already in the table");
    }

    /// <summary>
    /// One string per distinct value means ONE — the dedup table must not hold a separate key
    /// and value with the same content.
    ///
    /// <para>Inserting through the UTF-8 alternate lookup is what produced two: the alternate
    /// has no key to store, so it asks the comparer to <c>Create</c> one, transcoding a second
    /// time. B/event does not catch it — the duplicate is per DISTINCT value, and these
    /// segments have seven of them between 20 000 events — so the table is inspected directly.
    /// Filled to the merge's own cap with interpolated templates the difference measured 18.49 MB
    /// retained against 10.50 MB.</para>
    /// </summary>
    [Fact]
    public async Task Merge_DedupTableHoldsOneInstancePerDistinctValue()
    {
        for (int round = 0; round < 3; round++)
            await WriteSegmentAsync(round, 400);
        var sources = Directory.GetFiles(Path.Combine(_dir, "segments"), "*.seg").Order().ToList();
        Assert.Equal(3, sources.Count);

        // The cursor's own table, so what the merge put in it can be read back out.
        var dedup = new Dictionary<string, string>(Utf8StringComparer.Instance);
        int rows = 0;
        foreach (var path in sources)
        {
            using var cursor = new SegmentEventCursor(SegmentReader.Open(path), dedup);
            while (cursor.MoveNext())
            {
                var ev = cursor.Current;
                // What the writer is handed must be what the table retains, or dedup is a
                // memory cost with no saving.
                Assert.Same(dedup[ev.MessageTemplate], ev.MessageTemplate);
                if (ev.ServiceName is not null) Assert.Same(dedup[ev.ServiceName], ev.ServiceName);
                rows++;
            }
        }

        Assert.Equal(1_200, rows);
        Assert.NotEmpty(dedup);
        foreach (var (key, value) in dedup)
            Assert.True(ReferenceEquals(key, value),
                $"the dedup entry for \"{key}\" holds two distinct instances of the same content — " +
                "it was inserted through the UTF-8 alternate, which transcodes a second time to " +
                "materialise the key");
    }

    /// <summary>Drains a k-way merge of <paramref name="sources"/> and reports what the DRAIN
    /// allocated — opening and closing the sources sits outside the measurement.</summary>
    private static long DrainMerge(List<string> sources, out long allocatedBytes)
    {
        using var src = MergingSegmentEventSource.Open(sources);

        long before = GC.GetAllocatedBytesForCurrentThread();
        long events = 0;
        while (src.TryReadNext(out _)) events++;
        allocatedBytes = GC.GetAllocatedBytesForCurrentThread() - before;
        return events;
    }

    [Fact]
    public async Task Merge_RefusesTinyBatches_WhileRecent()
    {
        for (int round = 0; round < 3; round++) // below MergeMinSources, timestamps = now
            await WriteSegmentAsync(round, 50);
        Assert.False(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        Assert.Equal(3, _engine.ListSegments().Count);
    }

    /// <summary>
    /// A SETTLED window merges from just 2 sources — quiet days leave a handful of tiny
    /// segments per day, and a "not worth it" threshold would strand them forever (observed
    /// live: ~1,000 files parked that way).
    ///
    /// <para>Anchored on the BUCKET GRID, not at <c>UtcNow - 5d</c>. The planner buckets on an
    /// absolute grid, so a fixed offset back from now lands in a sealed bucket or in the open
    /// one depending on the time of day — and in the open one these 3 sources are below the
    /// fanout of 8 and nothing merges. See <see cref="MergeBucketGrid"/>.</para>
    /// </summary>
    [Fact]
    public async Task Merge_ConsolidatesSparseSettledWindows()
    {
        long old = MergeBucketGrid.SealedBucketStart(LogLevel.Information);
        for (int round = 0; round < 3; round++)
            await WriteSegmentAsync(round, 50, baseTicks: old + round * TimeSpan.TicksPerHour);

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        var segs = _engine.ListSegments();
        Assert.Single(segs);
        Assert.Equal(150u, segs[0].EventCount);
    }

    /// <summary>
    /// Segment expiry is MaxTs + Ttl(MinLevel) — merging across TTL classes would
    /// delete long-lived events early (or keep short-lived ones 30× longer). The
    /// default policy gives Debug 3 days and Error 90, so these must never mix.
    /// </summary>
    [Fact]
    public async Task Merge_NeverMixesRetentionTtlClasses()
    {
        for (int round = 0; round < 8; round++)
            await WriteSegmentAsync(round, 40, fixedLevel: LogLevel.Debug);
        for (int round = 8; round < 16; round++)
            await WriteSegmentAsync(round, 40, fixedLevel: LogLevel.Error);
        Assert.Equal(16, _engine.ListSegments().Count);

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));

        var segs = _engine.ListSegments().OrderBy(s => s.MinLevel).ToList();
        Assert.Equal(2, segs.Count);
        Assert.Equal(LogLevel.Debug, segs[0].MinLevel); // 3-day class, merged alone
        Assert.Equal(LogLevel.Error, segs[1].MinLevel); // 90-day class, merged alone
        Assert.Equal(320u, segs[0].EventCount);
        Assert.Equal(320u, segs[1].EventCount);
    }

    // ── Legacy mixed-level segments ───────────────────────────────────────────

    /// <summary>
    /// Writes ONE segment holding SEVERAL levels, straight through <see cref="SegmentWriter"/>.
    ///
    /// <para>It has to bypass the engine, because the engine can no longer produce this file: a
    /// flush partitions its tier by level and writes one segment per level. That is exactly the
    /// point — this is the on-disk shape every build BEFORE the level split wrote, and an
    /// upgrade inherits those files untouched. <c>LoadSegmentCatalog</c> takes a segment's
    /// metadata from the file's own footer and never from its name, so dropping one into the
    /// segment directory before the engine starts is all it takes to reproduce an upgraded
    /// install exactly.</para>
    ///
    /// <para>Levels cycle in <paramref name="levels"/> order, so <c>MinLevel</c> is the lowest
    /// severity VALUE among them — the field retention and the merge planner both read.</para>
    /// </summary>
    private static SegmentInfo WriteMixedLevelSegmentFile(
        string segDir, ulong segId, long baseTicks, int perLevel, params LogLevel[] levels)
    {
        const string Template = "evt {n} round 0";
        var pool  = new StringInternPool();
        int total = perLevel * levels.Length;
        using var hot = new HotTierSegment(total + 1, (long)total * 1024 + (8L << 20));

        for (int i = 0; i < total; i++)
            Assert.True(hot.TryWrite(new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)(segId * 100_000 + (ulong)i)).RawValue,
                TimestampUtcTicks        = baseTicks + i * TimeSpan.TicksPerMillisecond,
                Level                    = levels[i % levels.Length],
                MessageTemplatePoolIndex = pool.Intern(Template),
                ServiceNamePoolIndex     = pool.Intern("Svc.0"),
            }, Props(i), Template));
        hot.Freeze();

        long minTs = baseTicks;
        long maxTs = baseTicks + (total - 1) * TimeSpan.TicksPerMillisecond;
        string path = Path.Combine(segDir, $"{NodeId.Local.Value}-{segId}-{minTs}-{maxTs}.seg");
        using var writer = new SegmentWriter(path);
        writer.WriteEvents(hot, pool);
        return writer.Finalise(NodeId.Local, new SegmentId(segId));
    }

    /// <summary>
    /// Stages the state an UPGRADE leaves behind: two legacy mixed-level files carrying levels
    /// Debug..Error, beside one genuinely level-pure Debug segment written by the current flush
    /// path — all three in the same sealed Debug bucket, all three in a fresh engine's catalog.
    ///
    /// <para>The pure segment is flushed FIRST and the engine is then restarted, because the
    /// catalog is loaded once at construction: a file dropped into the directory afterwards is
    /// invisible until the next start, which is also the only moment a real upgrade has.</para>
    /// </summary>
    private async Task<(SegmentInfo MixedA, SegmentInfo MixedB, SegmentInfo Pure)> StageUpgradedInstallAsync()
    {
        long bucket = MergeBucketGrid.SealedBucketStart(LogLevel.Debug);

        // The level-pure partner, through the real flush path — same event count and same
        // property shape as the mixed files, so the planner's size-spread rule (a run's largest
        // may be at most MergeRunSizeRatio× its smallest) cannot be what decides this test.
        await WriteSegmentAsync(round: 0, count: 120, fixedLevel: LogLevel.Debug,
                                baseTicks: bucket + 3 * TimeSpan.TicksPerHour);
        var pure = _engine.ListSegments().Single();

        await _engine.DisposeAsync();
        string segDir = Path.Combine(_dir, "segments");
        var mixedA = WriteMixedLevelSegmentFile(segDir, 5000, bucket + 1 * TimeSpan.TicksPerHour, 30,
            LogLevel.Debug, LogLevel.Information, LogLevel.Warning, LogLevel.Error);
        var mixedB = WriteMixedLevelSegmentFile(segDir, 5001, bucket + 2 * TimeSpan.TicksPerHour, 30,
            LogLevel.Debug, LogLevel.Information, LogLevel.Warning, LogLevel.Error);

        _engine = NewEngine();
        for (int i = 0; i < 100 && _engine.ListSegments().Count < 3; i++) await Task.Delay(50);
        Assert.Equal(3, _engine.ListSegments().Count);

        // Both mixed files report the LOWEST level they hold, which is what puts them in the
        // same planner bucket as the pure Debug segment beside them.
        Assert.Equal(LogLevel.Debug, mixedA.MinLevel);
        Assert.Equal(LogLevel.Debug, mixedB.MinLevel);
        return (mixedA, mixedB, pure);
    }

    /// <summary>
    /// ACCEPTED BEHAVIOUR, PINNED ON PURPOSE — this test asserts something the storage engine
    /// gets WRONG, and the assertions below are the ones to change if that is ever fixed.
    ///
    /// <para>WHAT HAPPENS. The merge planner buckets on <c>SegmentInfo.MinLevel</c>, and
    /// MinLevel is the lowest severity VALUE in a file, not proof that the file holds one level.
    /// A segment written before the level split holds every level its flush happened to see, so
    /// it reports the lowest of them and is bucketed as though it were level-pure — then merged
    /// with segments that genuinely are. The output is a NEW file, mixed again, carrying the
    /// lowest level's TTL; and because a merged file takes the NEWEST source's MaxTimestamp,
    /// each pass moves its deadline further out. Confirmed on a live server: the deployed build
    /// wrote fresh v7 files holding levels [2,3,4], [2,3] and [1,2,3,4] — the last 26,386 events
    /// and 11.2 MB, whose Debug-derived 3-day deadline had already passed.</para>
    ///
    /// <para>WHY IT IS ACCEPTED RATHER THAN FIXED. Splitting on the merge side would mean
    /// deciding a segment is level-homogeneous, and MinLevel cannot answer that — only a READ
    /// of every row can, which is a full decode of every candidate on the planning path that
    /// today touches no file at all. It is not worth it, because the condition SELF-HEALS: a
    /// mixed segment carries the shortest TTL of the levels inside it (Debug's 3 days here), so
    /// it expires within days of the upgrade, and once the last one is gone every segment on
    /// disk is level-pure — after which same-MinLevel inputs give level-pure output by
    /// construction and this test's premise can no longer be built by the engine at all.</para>
    ///
    /// <para>TO MAKE MERGES LEVEL-PURE: this test should then produce FOUR segments, one per
    /// level present, and <see cref="Merge_ALegacyMixedSegmentKeepsTheLowestLevelsRetention"/>
    /// should stop finding Error rows on a 3-day deadline. Nothing else here needs to move.</para>
    /// </summary>
    [Fact]
    public async Task Merge_CarriesALegacyMixedLevelSegmentForward_Accepted()
    {
        var (mixedA, _, pure) = await StageUpgradedInstallAsync();

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));

        var segs = _engine.ListSegments();
        Assert.Single(segs);                       // mixed + mixed + pure ⇒ ONE file, not one per level
        var merged = segs[0];
        Assert.Equal(360u, merged.EventCount);     // 120 + 120 + 120, nothing dropped on the way

        // ACCEPTED (1): the output is MIXED again — every level that went in is still in one file.
        var dedup  = new Dictionary<string, string>(StringComparer.Ordinal);
        using (var r = SegmentReader.Open(merged.FilePath))
        {
            var levels = new HashSet<byte>();
            foreach (var ev in r.ReadAllRaw(dedup)) levels.Add(ev.Level);
            Assert.Equal(new[] { LogLevel.Debug, LogLevel.Information, LogLevel.Warning, LogLevel.Error }
                             .Select(static l => (byte)l).OrderBy(static b => b),
                         levels.OrderBy(static b => b));
        }

        // ACCEPTED (2): its MinLevel is the LOWEST level present, so the merged file inherits
        // the mixed sources' shape rather than the pure partner's.
        Assert.Equal(LogLevel.Debug, merged.MinLevel);

        // ACCEPTED (3): the deadline MOVED FORWARD. Expiry is MaxTimestamp + Ttl(MinLevel), and
        // the merged file takes the newest source's MaxTimestamp — so the mixed sources' rows
        // now expire when the pure segment's do, three hours later than they would have. The
        // shift is bounded by one bucket width per pass, but it happens again on every pass.
        Assert.Equal(pure.MaxTimestampTicks, merged.MaxTimestampTicks);
        Assert.True(merged.MaxTimestampTicks > mixedA.MaxTimestampTicks,
            "the merged file should carry the newest source's MaxTimestamp");
    }

    /// <summary>
    /// The retention consequence of the segment above, stated in days.
    ///
    /// <para>Expiry is <c>MaxTimestamp + Ttl(MinLevel)</c> and TTL is NOT monotonic in the
    /// level's value — Debug (1) sits below Information (2) while living 3 days against 90 — so
    /// the single Debug row in a mixed file sets the deadline for every Error row beside it.
    /// That is the same data loss <see cref="LevelSplitFlushTests"/> proves the flush no longer
    /// causes; on the merge path, for files an upgrade inherited, it is ACCEPTED and it is
    /// self-limiting: the short TTL is precisely what makes these files disappear.</para>
    /// </summary>
    [Fact]
    public async Task Merge_ALegacyMixedSegmentKeepsTheLowestLevelsRetention()
    {
        await StageUpgradedInstallAsync();
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        var merged = _engine.ListSegments().Single();

        var policy = RetentionPolicy.Default;      // Debug 3 days, every other level 90
        var actual = merged.MaxTimestamp.Add(policy.GetTtl(merged.MinLevel));
        var earned = merged.MaxTimestamp.Add(policy.GetTtl(LogLevel.Error));

        Assert.False(merged.IsExpired(policy, actual.AddTicks(-1)));
        Assert.True(merged.IsExpired(policy, actual.AddTicks(1)));

        // ACCEPTED: the 90 Error rows in this file are deleted 87 days early, because one Debug
        // row shares it with them. A level-pure Error segment would keep them for 90 days.
        Assert.Equal(TimeSpan.FromDays(87), earned - actual);

        // And they really are in there — this is data being dropped, not an empty guarantee.
        var dedup = new Dictionary<string, string>(StringComparer.Ordinal);
        using var r = SegmentReader.Open(merged.FilePath);
        int errors = 0;
        foreach (var ev in r.ReadAllRaw(dedup)) if (ev.Level == (byte)LogLevel.Error) errors++;
        Assert.Equal(60, errors);                  // 30 per mixed file, two of them
    }

    /// <summary>
    /// The LEVEL COLUMN survives a merge unchanged, row by row.
    ///
    /// <para>This is coverage that was lost rather than never written. <c>WriteSegmentAsync</c>
    /// used to stamp <c>(LogLevel)(n % 6)</c> and the level split changed it to a constant
    /// Information — necessary, because a mixed-level round now flushes to six segments and
    /// every "N rounds ⇒ N segments" assertion in this file would count something else. But it
    /// left <see cref="Merge_CollapsesSmallSegments_Losslessly"/> comparing a column that no
    /// longer varies: a merge that wrote a constant level, or dropped the column and let the
    /// reader default it, would still pass every assertion in this suite. Mixed sources restore
    /// the check without disturbing the segment arithmetic, because these files are staged on
    /// disk instead of flushed.</para>
    /// </summary>
    [Fact]
    public async Task Merge_PreservesTheLevelOfEveryRow()
    {
        await StageUpgradedInstallAsync();

        var dedup  = new Dictionary<string, string>(StringComparer.Ordinal);
        var before = new Dictionary<ulong, byte>(360);
        foreach (var seg in _engine.ListSegments())
        {
            using var r = SegmentReader.Open(seg.FilePath);
            foreach (var ev in r.ReadAllRaw(dedup)) before[ev.Id] = ev.Level;
        }
        Assert.Equal(360, before.Count);
        Assert.Equal(4, before.Values.Distinct().Count());   // the column genuinely varies

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));

        using var merged = SegmentReader.Open(_engine.ListSegments().Single().FilePath);
        int seen = 0;
        foreach (var ev in merged.ReadAllRaw(dedup))
        {
            Assert.Equal(before[ev.Id], ev.Level);
            seen++;
        }
        Assert.Equal(360, seen);
    }

    /// <summary>
    /// Prop-dense events (~2 KB each — the real-service shape that stalled the sandbox sweep)
    /// used to be trimmed out of the batch because a hot tier divides payload into fixed 8 MB
    /// chunks. There is no tier in the pipeline any more, so the whole backlog merges in one
    /// pass and every event survives.
    /// </summary>
    [Fact]
    public async Task Merge_ConsumesDenseBatchesWhole()
    {
        // 12 segments × 700 events × ~2 KB props ≈ 16 MB payload — four times what a tier
        // chunk could hold from slot 0, i.e. exactly the shape the old trim path cut up.
        for (int round = 0; round < 12; round++)
            await WriteSegmentAsync(round, 700, padBytes: 2048);
        Assert.Equal(12, _engine.ListSegments().Count);
        var before = ReadEverything();

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));

        var segs = _engine.ListSegments();
        Assert.Single(segs);
        Assert.Equal(8400u, segs[0].EventCount);

        var after = ReadEverything();
        Assert.Equal(before.Count, after.Count); // nothing lost, nothing duplicated
        Assert.Equal(before[0].Id, after[0].Id);
        Assert.Equal(before[^1].Id, after[^1].Id);
    }

    /// <summary>
    /// The merged segment's size is now bounded by POLICY (MergeTargetPayloadBytes /
    /// MergeMaxEvents), not by how much fits in memory. What must stay flat is the
    /// ALLOCATION: the writer holds one block per source plus one index group, so merging a
    /// backlog several times larger than the old 32 MB batch cap must not cost several times
    /// the garbage.
    /// </summary>
    [Fact]
    public async Task Merge_AllocationStaysFlatAcrossALargeBacklog()
    {
        // 40 segments x 500 events x ~2 KB props ≈ 40 MB payload — comfortably past the old
        // per-batch cap, so the whole lot is consumed in a single pass.
        for (int round = 0; round < 40; round++)
            await WriteSegmentAsync(round, 500, padBytes: 2048);
        Assert.Equal(40, _engine.ListSegments().Count);
        var before = ReadEverything();

        long allocBefore = GC.GetTotalAllocatedBytes(precise: false);
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
        long allocMb = (GC.GetTotalAllocatedBytes(precise: false) - allocBefore) / 1048576;

        var segs = _engine.ListSegments();
        Assert.Single(segs);
        Assert.Equal(20_000u, segs[0].EventCount);
        Assert.Equal(before.Count, ReadEverything().Count);

        // The old pipeline allocated a managed List<RawSegmentEvent> plus a byte[] per event's
        // properties for the whole batch before writing anything. Streaming copies each payload
        // once, straight into the open block: MEASURED 4 MB of managed allocation for 40 MB of
        // payload. (These merges run without an index sink — the index build's own state is
        // bounded by the group budget and measured by IndexGroupMemoryProbe.)
        //
        // This bound is about the STREAM, not about the cursor's strings. Per event these 20k
        // ran 265.8 B while the cursor decoded every template and service name before
        // deduplicating the result and 177.8 B once the table began probing by UTF-8 — 5 MB
        // against 3 MB here, both comfortably inside 20, so nothing in this assertion can see the
        // difference. Merge_BuildsAStringPerDistinctValue_NotPerEvent is where that is measured.
        Assert.True(allocMb < 20, $"merge allocated {allocMb} MB streaming 40 MB of payload");
    }

    /// <summary>
    /// A merged file can only expire whole, so a batch never reaches outside ONE bucket — the
    /// span is exactly how much longer its oldest event outlives its own deadline. Nine
    /// consecutive days of Information straddle two of Information's 7-day buckets, so they
    /// collapse to two files rather than one, each inside the bound.
    /// </summary>
    [Fact]
    public async Task Merge_NeverSpansMoreThanOneBucket()
    {
        long width  = MergeBucketGrid.BucketWidth(LogLevel.Information);
        // Nine days straddle two buckets, and BOTH have to be sealed for the pair of merges
        // below to run on the 2-source threshold.
        long origin = MergeBucketGrid.SealedBucketStart(LogLevel.Information, bucketsSpanned: 2);
        for (int round = 0; round < 9; round++)
            await WriteSegmentAsync(round, 40, baseTicks: origin + round * TimeSpan.TicksPerDay);

        int passes = 0;
        while (await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None)) Assert.True(++passes < 10);

        var segs = _engine.ListSegments();
        Assert.Equal(2, segs.Count);                                    // days 0-6 and days 7-8
        Assert.Equal(360u, (uint)segs.Sum(s => s.EventCount));
        foreach (var s in segs)
            Assert.True(s.MaxTimestampTicks - s.MinTimestampTicks <= width,
                $"segment spans {(s.MaxTimestampTicks - s.MinTimestampTicks) / (double)TimeSpan.TicksPerDay:F2} days");
    }

    /// <summary>
    /// DENSE SEGMENTS MERGE AGAIN. The co-fit gate (UncompressedBytes ≤ 4 MB, EventCount ≤
    /// 8192) existed only because a merge re-packed its batch into a hot tier, whose chunks
    /// are a FIXED division of the event index (idx / 16384) with 8 MB of payload each — two
    /// prop-dense segments could not co-fit chunk 0 however small they were in total. On the
    /// sandbox stand that gate excluded most of the files from compaction.
    ///
    /// <para>There is no tier and no chunk geometry in the pipeline now, so the gate is gone.
    /// This is the proof it can be: the exact shape it used to exclude — 2500 × ~3.1 KB props,
    /// ~7.8 MB uncompressed per file — merges, and every event survives byte for byte.</para>
    ///
    /// <para>The DENSE PAIR merges; the ~900 KB file beside them does not join it, and the
    /// bucket settles at two files. That is the trade, stated deliberately: the third file is
    /// 9× smaller than the pair, so taking it would rewrite 15.6 MB to absorb 900 KB. A policy
    /// that always collapses a bucket to exactly one file is a policy that rewrites whatever
    /// the bucket holds for whatever arrives late — which is how one row came to cost a 1.4 MB
    /// rewrite five times running. Two files is the correct terminal state here.</para>
    /// </summary>
    [Fact]
    public async Task Merge_MergesDenseSegments_Losslessly()
    {
        long old = MergeBucketGrid.SealedBucketStart(LogLevel.Information); // settled → 2 sources suffice
        await WriteSegmentAsync(0, 2500, baseTicks: old,                              padBytes: 3072);
        await WriteSegmentAsync(1, 2500, baseTicks: old + 1 * TimeSpan.TicksPerHour, padBytes: 3072);
        // Slot-dense too: 9000 events in one file, over the old 8192-event half-chunk gate.
        await WriteSegmentAsync(2, 9000, baseTicks: old + 2 * TimeSpan.TicksPerHour);
        Assert.Equal(3, _engine.ListSegments().Count);

        // These are the files the gate used to reject: each reports more uncompressed payload
        // than half a tier chunk.
        var dense = _engine.ListSegments().Where(s => s.EventCount == 2500).ToList();
        Assert.Equal(2, dense.Count);
        Assert.All(dense, s => Assert.True(s.UncompressedBytes > 4L * 1024 * 1024,
            $"test is meaningless: segment reports only {s.UncompressedBytes} B uncompressed"));

        var before = ReadEverything();
        Assert.Equal(14_000, before.Count);

        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));

        var segs = _engine.ListSegments().OrderByDescending(s => s.UncompressedBytes).ToList();
        Assert.Equal(2, segs.Count);
        Assert.Equal(5_000u, segs[0].EventCount);   // the two dense files, merged
        Assert.Equal(9_000u, segs[1].EventCount);   // 9× smaller, left alone
        Assert.Equal(14_000u, (uint)segs.Sum(s => s.EventCount));

        var after = ReadEverything();
        Assert.Equal(before.Count, after.Count);
        for (int i = 0; i < before.Count; i++)
        {
            Assert.Equal(before[i].Id,      after[i].Id);
            Assert.Equal(before[i].TsTicks, after[i].TsTicks);
            Assert.Equal(before[i].Props ?? [], after[i].Props ?? []);
        }

        // Nothing mergeable remains → clean false, no anchor-skip churn.
        Assert.False(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
    }

    /// <summary>
    /// An anchor skip must not burn the whole pass: when a selected bucket yields
    /// no usable batch (here: its segments are unreadable on disk), the pass
    /// re-selects the next bucket instead of returning false for 600 s. Unreadable
    /// segments are skip-listed individually so they are never re-opened.
    ///
    /// <para>The two buckets are computed from the bucket GRID, not from <c>UtcNow</c> plus a
    /// 40 h offset. That offset predates buckets: whether two segments 40 h apart land in one
    /// bucket or two depends on where the wall clock happens to sit relative to a boundary, so
    /// the test passed or failed by time of day (2 failures of 2 runs on the pristine tree).
    /// A test whose result depends on when it is run is worse than no test.</para>
    /// </summary>
    [Fact]
    public async Task Merge_RetriesNextWindow_AfterAnchorSkip()
    {
        long width = MergeBucketGrid.BucketWidth(LogLevel.Information);
        // Two ADJACENT buckets, both far enough back to be sealed. Anchoring on the grid is
        // what makes "these two are in different buckets" true on every run.
        long b0 = MergeBucketGrid.SealedBucketStart(LogLevel.Information, bucketsSpanned: 2);
        // Bucket 0 (oldest, sealed): two segments, both corrupted after flush.
        await WriteSegmentAsync(0, 50, baseTicks: b0 + TimeSpan.TicksPerHour);
        await WriteSegmentAsync(1, 50, baseTicks: b0 + 2 * TimeSpan.TicksPerHour);
        // Bucket 1 (the next window on the grid, still sealed): two healthy segments.
        await WriteSegmentAsync(2, 50, baseTicks: b0 + width + TimeSpan.TicksPerHour);
        await WriteSegmentAsync(3, 50, baseTicks: b0 + width + 2 * TimeSpan.TicksPerHour);

        var byTime  = _engine.ListSegments().OrderBy(s => s.MinTimestampTicks).ToList();
        foreach (var victim in byTime.Take(2))
            File.WriteAllBytes(victim.FilePath, [0xDE, 0xAD, 0xBE, 0xEF]); // magic mismatch

        // One pass: window 1 fails (both sources unreadable → no usable batch),
        // the retry selects window 2 and merges it — the pass still succeeds.
        Assert.True(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));

        var segs = _engine.ListSegments();
        Assert.Contains(segs, s => s.EventCount == 100);            // window 2 merged
        Assert.True(File.Exists(byTime[0].FilePath));               // corrupt files untouched
        Assert.True(File.Exists(byTime[1].FilePath));

        // Corrupt segments are skip-listed → nothing left to select, clean false.
        Assert.False(await _engine.TryMergeSmallSegmentsOnceAsync(CancellationToken.None));
    }

    /// <summary>
    /// Writer-side and reader-side UncompressedBytes must be the SAME quantity
    /// (sum of event-block uncompressed sizes) — if they drifted, a segment near
    /// the 4 MB co-fit gate would flip candidacy across a restart. The default
    /// (cheap) open keeps the legacy file-size fallback: query-path opens never
    /// pay the block walk.
    /// </summary>
    [Fact]
    public async Task UncompressedBytes_IsHonest_AndRestartStable()
    {
        await WriteSegmentAsync(0, 300, padBytes: 512);
        var info = _engine.ListSegments().Single(); // writer-produced catalog entry

        Assert.True(info.UncompressedBytes > info.CompressedBytes,
            $"expected uncompressed {info.UncompressedBytes} > compressed {info.CompressedBytes} for compressible padding");

        using var honest = SegmentReader.Open(info.FilePath, computeUncompressedBytes: true);
        Assert.Equal(info.UncompressedBytes, honest.Info.UncompressedBytes);

        using var cheap = SegmentReader.Open(info.FilePath);
        Assert.Equal(cheap.Info.CompressedBytes, cheap.Info.UncompressedBytes);
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
