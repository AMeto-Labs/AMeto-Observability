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

    // ── Watermark ─────────────────────────────────────────────────────────────

    [Fact]
    public void Reset_drops_everything_it_logged()
    {
        var wal = SpanWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 10; i++) wal.Append(Item(i, 5_000 + i));
        wal.Reset(flushedThroughNano: 5_009);
        wal.Dispose();

        var reopened = SpanWriteAheadLog.Open(WalPath);
        Assert.Empty(reopened.ReadAll());
        reopened.Dispose();
    }

    [Fact]
    public void Watermark_skips_spans_a_segment_already_holds()
    {
        // Simulates the crash window: the segment reached disk and the watermark landed,
        // but the write offset was never zeroed. Replay must not resurrect cold spans.
        var wal = SpanWriteAheadLog.Open(WalPath);
        for (int i = 0; i < 6; i++) wal.Append(Item(i, 9_000 + i));
        wal.Dispose();

        // Re-open and stamp the watermark WITHOUT clearing the entries, by writing them
        // again after the reset — the first six are now at or below the flush point.
        var mid = SpanWriteAheadLog.Open(WalPath);
        var all = mid.ReadAll();
        Assert.Equal(6, all.Count);
        mid.Dispose();

        var stamped = SpanWriteAheadLog.Open(WalPath);
        stamped.Reset(flushedThroughNano: 9_003);          // covers 9_000..9_003
        for (int i = 0; i < 6; i++) stamped.Append(Item(i, 9_000 + i));   // replay-equivalent tail
        stamped.Dispose();

        var after = SpanWriteAheadLog.Open(WalPath);
        var kept  = after.ReadAll();
        after.Dispose();

        Assert.Equal(2, kept.Count);                        // only 9_004 and 9_005 survive
        Assert.All(kept, s => Assert.True(s.StartTimeUnixNano > 9_003));
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
        Assert.Equal(40, list.Count);                       // one trace per span in the corpus
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
}
