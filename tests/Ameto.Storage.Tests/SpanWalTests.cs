using Ameto.Tracing;
using Ameto.Tracing.Storage;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// Span write-ahead log: append/replay fidelity, the post-flush watermark, torn tails,
/// growth past the initial mapping — and the behaviour the log exists to enable, namely
/// that a trickle of spans no longer costs a .trc segment (plus .stats sidecar) per tick.
/// </summary>
public sealed class SpanWalTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-swal-" + Guid.NewGuid().ToString("N"));

    public SpanWalTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    private string WalPath => Path.Combine(_dir, "spans.wal");

    private static SpanIngestItem Item(int i, long startNano, string? name = null, byte[]? attrs = null) => new()
    {
        TraceId           = new TraceId((ulong)(i + 1) * 0x9E3779B97F4A7C15UL, (ulong)(i + 7)),
        SpanId            = new SpanId((ulong)(i + 100)),
        ParentSpanId      = i == 0 ? default : new SpanId((ulong)(i + 99)),
        StartTimeUnixNano = startNano,
        DurationNanos     = 1_000_000L + i,
        Name              = name ?? $"GET /api/thing/{i}",
        ServiceName       = i % 2 == 0 ? "MintRoute.API" : "KioskAgent.API",
        Kind              = i % 3 == 0 ? SpanKind.Server : SpanKind.Client,
        Status            = i % 5 == 0 ? SpanStatusCode.Error : SpanStatusCode.Unset,
        HttpStatusCode    = (short)(i % 3 == 0 ? 200 : 0),
        AttributesBytes   = attrs ?? [],
    };

    // ── Roundtrip ─────────────────────────────────────────────────────────────

    [Fact]
    public void Replays_every_field_after_reopen()
    {
        var attrs = MessagePackSerializer.Serialize(
            new Dictionary<string, object?> { ["http.method"] = "GET", ["retry"] = 3 });

        var written = new List<SpanIngestItem>();
        var wal = SpanWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 25; i++)
        {
            var it = Item(i, 1_784_800_000_000_000_000L + i * 1_000_000L, attrs: i % 4 == 0 ? attrs : null);
            written.Add(it);
            wal.Append(it);
        }
        wal.Dispose();   // unclean stop: nothing flushed a segment, the log is all there is

        var reopened = SpanWriteAheadLog.Open(WalPath);
        var replayed = reopened.ReadAll();
        reopened.Dispose();

        Assert.Equal(written.Count, replayed.Count);
        for (int i = 0; i < written.Count; i++)
        {
            Assert.Equal(written[i].TraceId,           replayed[i].TraceId);
            Assert.Equal(written[i].SpanId.RawValue,   replayed[i].SpanId.RawValue);
            Assert.Equal(written[i].ParentSpanId.RawValue, replayed[i].ParentSpanId.RawValue);
            Assert.Equal(written[i].StartTimeUnixNano, replayed[i].StartTimeUnixNano);
            Assert.Equal(written[i].DurationNanos,     replayed[i].DurationNanos);
            Assert.Equal(written[i].Name,              replayed[i].Name);
            Assert.Equal(written[i].ServiceName,       replayed[i].ServiceName);
            Assert.Equal(written[i].Kind,              replayed[i].Kind);
            Assert.Equal(written[i].Status,            replayed[i].Status);
            Assert.Equal(written[i].HttpStatusCode,    replayed[i].HttpStatusCode);
            Assert.Equal(written[i].AttributesBytes,   replayed[i].AttributesBytes);
        }
    }

    [Fact]
    public void Handles_empty_and_unicode_names()
    {
        var wal = SpanWriteAheadLog.Open(WalPath);
        wal.Append(Item(0, 1_000, name: ""));
        wal.Append(Item(1, 2_000, name: "платёж → провайдер 🚀"));
        wal.Dispose();

        var reopened = SpanWriteAheadLog.Open(WalPath);
        var replayed = reopened.ReadAll();
        reopened.Dispose();

        Assert.Equal(2, replayed.Count);
        Assert.Equal("",                      replayed[0].Name);
        Assert.Equal("платёж → провайдер 🚀", replayed[1].Name);
    }

    // ── Generation ────────────────────────────────────────────────────────────

    [Fact]
    public void Reset_drops_everything_it_logged()
    {
        var wal = SpanWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 10; i++) wal.Append(Item(i, 5_000 + i));
        wal.Reset();
        wal.Dispose();

        var reopened = SpanWriteAheadLog.Open(WalPath);
        Assert.Empty(reopened.ReadAll());
        reopened.Dispose();
    }

    [Fact]
    public void Spans_appended_after_a_reset_survive_however_early_they_started()
    {
        // An OTLP exporter ships a span when it ENDS, so a long-running span reaches the log
        // after a flush while having started before it. Filtering replay on the span's own
        // start time dropped exactly these; the generation is assigned by us, under our lock,
        // and does not care what the client's clock says.
        const long segmentMaxStart = 1_700_000_000_000_000_000L;

        var wal = SpanWriteAheadLog.Open(WalPath);
        wal.Append(Item(0, segmentMaxStart));
        wal.Reset();                                                   // segment flushed

        wal.Append(Item(1, segmentMaxStart - 30_000_000_000L));        // 30 s span, just ended
        wal.Append(Item(2, segmentMaxStart -  1_000_000_000L));        // client clock behind
        wal.Append(Item(3, segmentMaxStart +  5_000_000_000L));        // ordinary
        wal.Dispose();

        var reopened = SpanWriteAheadLog.Open(WalPath);
        var ids = reopened.ReadAll().Select(s => s.SpanId.RawValue).OrderBy(x => x).ToArray();
        reopened.Dispose();

        Assert.Equal([101UL, 102UL, 103UL], ids);
    }

    [Fact]
    public void A_reset_that_never_zeroed_the_offset_still_hides_flushed_spans()
    {
        // The crash window: generation bumped, write offset not yet stored. Entries from the
        // flushed generation are still addressable and must not come back.
        var wal = SpanWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 6; i++) wal.Append(Item(i, 9_000 + i));
        long flushedBytes = wal.WrittenBytes;
        wal.Reset();
        for (int i = 6; i < 9; i++) wal.Append(Item(i, 9_000 + i));     // new generation
        long liveBytes = wal.WrittenBytes;
        wal.Dispose();

        // Rewind the header to cover both generations, as a lost offset store would.
        using (var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Seek(8, SeekOrigin.Begin);
            fs.Write(BitConverter.GetBytes(32 + flushedBytes + liveBytes));
        }

        var reopened = SpanWriteAheadLog.Open(WalPath);
        var kept     = reopened.ReadAll();
        reopened.Dispose();

        Assert.Equal(3, kept.Count);                                    // only the new generation
        Assert.All(kept, s => Assert.True(s.SpanId.RawValue >= 106));
    }

    [Fact]
    public void A_span_without_a_start_time_does_not_truncate_the_log()
    {
        // OtlpTraceStreamParser leaves startTimeUnixNano at 0 when the field is absent and
        // nothing downstream rejects it, so such a span reaches the log. It must cost only
        // itself, not everything written after it.
        var wal = SpanWriteAheadLog.Open(WalPath);
        wal.Append(Item(0, 1_700_000_000_000_000_000L));
        wal.Append(Item(1, 0));
        wal.Append(Item(2, 1_700_000_000_000_000_002L));
        wal.Append(Item(3, 1_700_000_000_000_000_003L));
        wal.Dispose();

        var reopened = SpanWriteAheadLog.Open(WalPath);
        var replayed = reopened.ReadAll();
        reopened.Dispose();

        Assert.Equal(4, replayed.Count);
        Assert.Contains(replayed, s => s.SpanId.RawValue == 102);
        Assert.Contains(replayed, s => s.SpanId.RawValue == 103);
    }

    // ── Durability edges ──────────────────────────────────────────────────────

    [Theory]
    [InlineData(40)]      // offset lands mid-way through the zeroed region
    [InlineData(4096)]    // …and far enough in that whole zero "entries" would fit
    public void A_write_offset_past_the_real_data_replays_only_the_real_data(int overshoot)
    {
        // An append stores the payload first and the offset second, so a crash normally
        // leaves the offset short. The dangerous inverse — header page flushed, payload
        // pages not — is simulated here by inflating the offset over untouched zeroes.
        var wal = SpanWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 8; i++) wal.Append(Item(i, 3_000 + i));
        long real = wal.WrittenBytes;
        wal.Dispose();

        using (var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Seek(8, SeekOrigin.Begin);                   // WalFileHeader.WriteOffset
            fs.Write(BitConverter.GetBytes(32 + real + overshoot));
        }

        var reopened = SpanWriteAheadLog.Open(WalPath);
        var replayed = reopened.ReadAll();
        reopened.Dispose();

        Assert.Equal(8, replayed.Count);                    // no invented zero-filled spans
        Assert.All(replayed, s => Assert.True(s.StartTimeUnixNano >= 3_000));
    }

    [Fact]
    public void Grows_past_the_initial_mapping_without_losing_entries()
    {
        // Tiny initial capacity forces several doublings mid-run.
        var big = MessagePackSerializer.Serialize(
            new Dictionary<string, object?> { ["blob"] = new string('x', 900) });

        var wal = SpanWriteAheadLog.Open(WalPath, initialCapacity: 8 * 1024);
        const int n = 400;
        for (int i = 0; i < n; i++) wal.Append(Item(i, 7_000 + i, attrs: big));
        wal.Dispose();

        var reopened = SpanWriteAheadLog.Open(WalPath);
        var replayed = reopened.ReadAll();
        reopened.Dispose();

        Assert.Equal(n, replayed.Count);
        Assert.Equal(7_000, replayed[0].StartTimeUnixNano);
        Assert.Equal(7_000 + n - 1, replayed[^1].StartTimeUnixNano);
        Assert.Equal(big, replayed[123].AttributesBytes);
    }

    // ── The regression this whole change is about ─────────────────────────────

    private static int TrcCount(string dir) => Directory.GetFiles(dir, "*.trc").Length;

    [Fact]
    public void A_trickle_of_spans_writes_no_segment()
    {
        // The old behaviour: SpanDrainer flushed unconditionally every 30 s, so five spans
        // bought a full .trc plus a .stats sidecar — ~5 800 files/day on a quiet instance.
        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);

        for (int tick = 0; tick < 20; tick++)
        {
            for (int i = 0; i < 5; i++)
                engine.WriteSpan(Item(i, 1_784_800_000_000_000_000L + tick * 1_000_000L + i));
            engine.FlushIfDue();
        }

        Assert.Equal(0, TrcCount(_dir));                    // 100 spans, still not worth a file
        Assert.True(new FileInfo(WalPath).Exists);          // …but all of them are durable
    }

    [Fact]
    public void A_real_batch_still_writes_a_segment_and_clears_the_log()
    {
        var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);

        for (int i = 0; i < 600; i++)                       // over MinSegmentSpans (500)
            engine.WriteSpan(Item(i, 1_784_800_000_000_000_000L + i * 1_000L));
        engine.FlushIfDue();
        engine.WaitForFlushForTest();                       // the segment build runs off the lock now

        Assert.Equal(1, TrcCount(_dir));

        // Release the mapping before reading the log back — the engine holds it exclusively,
        // which is also what stops two engines sharing one data directory.
        engine.Dispose();                                   // hot tier already empty: no second segment

        var wal = SpanWriteAheadLog.Open(WalPath);
        Assert.Empty(wal.ReadAll());                        // gave its spans to the segment and stood down
        wal.Dispose();
        Assert.Equal(1, TrcCount(_dir));
    }

    [Fact]
    public async Task Spans_held_back_from_a_segment_are_still_searchable()
    {
        // The WAL is a durability copy, never a query source: everything in it is also in
        // the hot tier, and every read path merges hot with cold. Holding a trickle back
        // from disk must therefore be invisible to a lookup by id and to the trace list.
        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);

        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        for (int i = 0; i < 40; i++) engine.WriteSpan(Item(i, baseNano + i * 1_000_000L));
        engine.FlushIfDue();
        Assert.Equal(0, TrcCount(_dir));                    // still nothing on disk

        var byId = engine.GetTraceAsync(Item(11, 0).TraceId).ToBlockingEnumerable().ToList();
        Assert.Single(byId);
        Assert.Equal(baseNano + 11 * 1_000_000L, byId[0].StartTimeUnixNano);

        var list = await engine.GetTraceListAsync(
            DateTimeOffset.UtcNow.AddHours(-1), DateTimeOffset.UtcNow.AddHours(1),
            serviceName: null, spanName: null, status: null,
            minDurationNanos: null, maxDurationNanos: null, limit: 100);
        Assert.Equal(40, list.Rows.Count);                  // one trace per span in the corpus
    }

    [Fact]
    public void Existing_segments_survive_the_upgrade_and_stay_queryable()
    {
        // The deploy case: a data directory already holding segments written by the old
        // build, opened for the first time by an engine that now keeps a WAL beside them.
        long oldNano = DateTimeOffset.UtcNow.AddMinutes(-30).ToUnixTimeMilliseconds() * 1_000_000L;
        var legacy = new List<SpanRecord>();
        for (int i = 0; i < 6; i++)                          // the old 30-second flush shape
            legacy.Add(new SpanRecord
            {
                TraceId           = new TraceId(0xAAAA_0000_0000_0001UL, (ulong)(i + 1)),
                SpanId            = new SpanId((ulong)(500 + i)),
                StartTimeUnixNano = oldNano + i * 1_000_000L,
                DurationNanos     = 2_000_000L,
                Name              = "legacy op",
                ServiceName       = "MintRoute.API",
                Kind              = SpanKind.Server,
                Status            = SpanStatusCode.Unset,
            });
        var pre = SpanWriter.Write(_dir, legacy);
        Assert.Equal(1, TrcCount(_dir));

        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        engine.LoadColdSegments();                           // what TraceCompactionWorker does at startup

        // Opening a WAL in the same directory neither deletes nor hides the old segment.
        Assert.True(File.Exists(pre.FilePath));
        Assert.Equal(1, TrcCount(_dir));

        var old = engine.GetTraceAsync(new TraceId(0xAAAA_0000_0000_0001UL, 3)).ToBlockingEnumerable().ToList();
        Assert.Single(old);
        Assert.Equal("legacy op", old[0].Name);

        // New spans land in the WAL + hot tier and coexist with the cold data.
        long freshNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        engine.WriteSpan(Item(1, freshNano));
        var fresh = engine.GetTraceAsync(Item(1, 0).TraceId).ToBlockingEnumerable().ToList();
        Assert.Single(fresh);
        Assert.Equal(1, TrcCount(_dir));                     // still just the pre-existing file
    }

    // ── De-duplication on read ────────────────────────────────────────────────

    /// <summary>
    /// The crash window this design admits: the segment reached disk, the generation bump
    /// did not, so replay puts spans back into the hot tier that the segment also holds.
    /// A waterfall must not show them twice.
    /// </summary>
    [Fact]
    public void A_span_in_both_the_hot_tier_and_a_segment_is_returned_once()
    {
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        var traceId   = new TraceId(0xBBBB_0000_0000_0002UL, 42);

        // The segment written just before the crash.
        SpanWriter.Write(_dir,
        [
            new SpanRecord
            {
                TraceId = traceId, SpanId = new SpanId(900), StartTimeUnixNano = baseNano,
                DurationNanos = 1_000_000L, Name = "dup", ServiceName = "MintRoute.API",
                Kind = SpanKind.Server, Status = SpanStatusCode.Unset,
            },
        ]);

        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        engine.LoadColdSegments();

        // The same span replayed into the hot tier, plus one that only exists there.
        engine.WriteSpan(new SpanIngestItem
        {
            TraceId = traceId, SpanId = new SpanId(900), StartTimeUnixNano = baseNano,
            DurationNanos = 1_000_000L, Name = "dup", ServiceName = "MintRoute.API",
            Kind = SpanKind.Server, Status = SpanStatusCode.Unset,
        });
        engine.WriteSpan(new SpanIngestItem
        {
            TraceId = traceId, SpanId = new SpanId(901), StartTimeUnixNano = baseNano + 1_000L,
            DurationNanos = 1_000_000L, Name = "only-hot", ServiceName = "MintRoute.API",
            Kind = SpanKind.Client, Status = SpanStatusCode.Unset,
        });

        var spans = engine.GetTraceAsync(traceId).ToBlockingEnumerable().ToList();

        Assert.Equal(2, spans.Count);
        Assert.Single(spans, s => s.SpanId.RawValue == 900);
        Assert.Single(spans, s => s.SpanId.RawValue == 901);

        // The search path returns individual spans too, and folds the same repeat.
        var found = engine.SearchSpansAsync(
            from: DateTimeOffset.UtcNow.AddHours(-1), to: DateTimeOffset.UtcNow.AddHours(1))
            .ToBlockingEnumerable().ToList();
        Assert.Equal(2, found.Count);
    }

    [Fact]
    public void Spans_without_an_id_are_never_folded_together()
    {
        // A producer omitting the span id would otherwise collapse every such span into one.
        // Dropping distinct spans is data loss, not de-duplication.
        long baseNano = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        var traceId   = new TraceId(0xCCCC_0000_0000_0003UL, 7);

        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        for (int i = 0; i < 3; i++)
            engine.WriteSpan(new SpanIngestItem
            {
                TraceId = traceId, SpanId = default, StartTimeUnixNano = baseNano + i * 1_000L,
                DurationNanos = 1_000_000L, Name = $"no-id-{i}", ServiceName = "MintRoute.API",
                Kind = SpanKind.Internal, Status = SpanStatusCode.Unset,
            });

        var spans = engine.GetTraceAsync(traceId).ToBlockingEnumerable().ToList();
        Assert.Equal(3, spans.Count);
    }

    [Fact]
    public void Unflushed_spans_come_back_after_a_crash()
    {
        long baseNano = 1_784_800_000_000_000_000L;

        // A log left behind by a process that died before any flush.
        var crashed = SpanWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 12; i++) crashed.Append(Item(i, baseNano + i * 1_000L));
        crashed.Dispose();

        using var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);

        // Recovered into the hot tier and queryable, with no segment on disk.
        var spans = engine.GetTraceAsync(Item(3, 0).TraceId).ToBlockingEnumerable().ToList();
        Assert.Single(spans);
        Assert.Equal(baseNano + 3 * 1_000L, spans[0].StartTimeUnixNano);
        Assert.Equal(0, TrcCount(_dir));
    }

    // ── Two-phase flush (Begin / Commit / Abandon) ───────────────────────────
    //
    // The protocol that lets the engine build a segment OFF its lock: Begin moves
    // appends to the next generation while the header keeps the flushed one, so a crash
    // mid-flush replays BOTH (the segment is not durable yet); Commit relocates the
    // during-flush tail and kills the flushed generation; Abandon keeps everything.

    private const long BaseNano = 1_784_800_000_000_000_000L;

    [Fact]
    public void Crash_between_begin_and_commit_replays_both_generations()
    {
        var wal = SpanWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 5; i++) wal.Append(Item(i, BaseNano + i));
        wal.BeginFlush();
        for (int i = 5; i < 8; i++) wal.Append(Item(i, BaseNano + i));   // arrive mid-flush
        wal.Dispose();                                                   // crash before commit

        var reopened = SpanWriteAheadLog.Open(WalPath);
        var replayed = reopened.ReadAll();
        reopened.Dispose();

        // The segment never landed — losing either generation would be data loss.
        Assert.Equal(8, replayed.Count);
        Assert.Equal(Enumerable.Range(0, 8).Select(i => BaseNano + i),
                     replayed.Select(r => r.StartTimeUnixNano));
    }

    [Fact]
    public void Commit_kills_the_flushed_generation_and_keeps_the_tail()
    {
        var wal = SpanWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 5; i++) wal.Append(Item(i, BaseNano + i));
        wal.BeginFlush();
        for (int i = 5; i < 8; i++) wal.Append(Item(i, BaseNano + i));
        wal.CommitFlush();                                               // segment durable
        wal.Dispose();

        var reopened = SpanWriteAheadLog.Open(WalPath);
        var replayed = reopened.ReadAll();

        Assert.Equal(3, replayed.Count);                                 // only the tail
        Assert.Equal(new[] { BaseNano + 5, BaseNano + 6, BaseNano + 7 },
                     replayed.Select(r => r.StartTimeUnixNano));

        // And the tail keeps accepting + committing normally afterwards.
        reopened.BeginFlush();
        reopened.CommitFlush();
        Assert.Empty(reopened.ReadAll());
        reopened.Dispose();
    }

    [Fact]
    public void Abandon_then_retry_commit_loses_nothing_and_dies_cleanly()
    {
        var wal = SpanWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 4; i++) wal.Append(Item(i, BaseNano + i));
        wal.BeginFlush();
        wal.Append(Item(4, BaseNano + 4));                               // mid first attempt
        wal.AbandonFlush();                                              // write failed

        // Crash here must still replay everything: nothing was committed.
        Assert.Equal(5, wal.ReadAll().Count);

        wal.BeginFlush();                                                // retry covers all 5
        wal.Append(Item(5, BaseNano + 5));                               // arrives mid-retry
        wal.CommitFlush();
        wal.Dispose();

        var reopened = SpanWriteAheadLog.Open(WalPath);
        var replayed = reopened.ReadAll();
        reopened.Dispose();

        Assert.Single(replayed);                                         // only the retry tail
        Assert.Equal(BaseNano + 5, replayed[0].StartTimeUnixNano);
    }

    [Fact]
    public void Recovery_truncates_to_where_replay_stopped_so_later_appends_survive()
    {
        // The state a crash between the commit's data barrier and its header store leaves:
        // the front is relocated and terminated by the generation-0 marker, but the header
        // still carries the OLD generation and the OLD, longer offset. Replay handles that
        // — and must also FIX the offset, or the next append lands past the terminator,
        // where the following recovery stops before reaching it.
        var wal = SpanWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 5; i++) wal.Append(Item(i, BaseNano + i));
        wal.BeginFlush();
        for (int i = 5; i < 8; i++) wal.Append(Item(i, BaseNano + i));

        long oldOffset = ReadHeaderInt64(8);      // covers flushed entries + the tail
        uint oldGen    = ReadHeaderUInt32(16);    // the generation being flushed

        wal.CommitFlush();
        wal.Dispose();
        PatchHeader(offset: 8,  value: BitConverter.GetBytes(oldOffset));   // header store "lost"
        PatchHeader(offset: 16, value: BitConverter.GetBytes(oldGen));

        var reopened = SpanWriteAheadLog.Open(WalPath);
        Assert.Equal(3, reopened.ReadAll().Count);            // the relocated tail, stopping at the marker

        for (int i = 8; i < 10; i++) reopened.Append(Item(i, BaseNano + i));
        reopened.Dispose();

        var again    = SpanWriteAheadLog.Open(WalPath);
        var replayed = again.ReadAll();
        again.Dispose();

        // Without the truncation the two new spans landed beyond the marker and the walk
        // stopped before them: 3 instead of 5, silently.
        Assert.Equal(5, replayed.Count);
        Assert.Equal(new[] { BaseNano + 5, BaseNano + 6, BaseNano + 7, BaseNano + 8, BaseNano + 9 },
                     replayed.Select(r => r.StartTimeUnixNano));
    }

    private long ReadHeaderInt64(int offset)
    {
        using var fs = new FileStream(WalPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        Span<byte> buf = stackalloc byte[8];
        fs.Seek(offset, SeekOrigin.Begin);
        fs.ReadExactly(buf);
        return BitConverter.ToInt64(buf);
    }

    private uint ReadHeaderUInt32(int offset)
    {
        using var fs = new FileStream(WalPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        Span<byte> buf = stackalloc byte[4];
        fs.Seek(offset, SeekOrigin.Begin);
        fs.ReadExactly(buf);
        return BitConverter.ToUInt32(buf);
    }

    private void PatchHeader(int offset, byte[] value)
    {
        using var fs = new FileStream(WalPath, FileMode.Open, FileAccess.ReadWrite);
        fs.Seek(offset, SeekOrigin.Begin);
        fs.Write(value);
    }

    [Fact]
    public void Engine_flush_is_durable_and_wal_holds_only_the_tail()
    {
        // End-to-end through the engine's snapshot/complete path (FlushHotTier runs it
        // synchronously): flushed spans land in a .trc, the WAL commits, and a restart
        // replays nothing that the segment already holds.
        var engine = new TraceStorageEngine(_dir, NullLogger<TraceStorageEngine>.Instance);
        for (int i = 0; i < 600; i++)
            engine.WriteSpan(Item(i, BaseNano + i * 1_000L));
        engine.FlushHotTier();
        Assert.Equal(1, TrcCount(_dir));
        engine.Dispose();

        var reopened = SpanWriteAheadLog.Open(WalPath);
        Assert.Empty(reopened.ReadAll());                                // clean stop: all cold
        reopened.Dispose();
    }
}
