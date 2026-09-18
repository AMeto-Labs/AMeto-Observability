using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Storage.Tests;

public sealed class HotTierSegmentTests : IDisposable
{
    // Small segment for tests: 16 events, 64 KB payload
    private readonly HotTierSegment _segment = new(16, 64 * 1024);

    public void Dispose() => _segment.Dispose();

    // ── TryWrite ──────────────────────────────────────────────────────────────

    [Fact]
    public void TryWrite_EmptyPayload_ReturnsTrueAndIncrementsCount()
    {
        var header = MakeHeader(LogLevel.Information, 1);
        bool ok = _segment.TryWrite(header, ReadOnlySpan<byte>.Empty);
        Assert.True(ok);
        Assert.Equal(1, _segment.Count);
    }

    [Fact]
    public void TryWrite_MultipleEvents_CountMatchesWritten()
    {
        for (int i = 0; i < 5; i++)
            _segment.TryWrite(MakeHeader(LogLevel.Debug, i), ReadOnlySpan<byte>.Empty);

        Assert.Equal(5, _segment.Count);
    }

    [Fact]
    public void TryWrite_WhenFrozen_ReturnsFalse()
    {
        _segment.Freeze();
        bool ok = _segment.TryWrite(MakeHeader(LogLevel.Error, 99), ReadOnlySpan<byte>.Empty);
        Assert.False(ok);
        Assert.Equal(0, _segment.Count);
    }

    [Fact]
    public void TryWrite_WhenAtCapacity_ReturnsFalse()
    {
        // Fill all 16 slots
        for (int i = 0; i < 16; i++)
            _segment.TryWrite(MakeHeader(LogLevel.Verbose, i), ReadOnlySpan<byte>.Empty);

        Assert.Equal(16, _segment.Count);

        bool ok = _segment.TryWrite(MakeHeader(LogLevel.Fatal, 17), ReadOnlySpan<byte>.Empty);
        Assert.False(ok);
        Assert.Equal(16, _segment.Count); // still 16
    }

    // ── IsFull ────────────────────────────────────────────────────────────────

    [Fact]
    public void IsFull_AfterCapacityReached_IsTrue()
    {
        for (int i = 0; i < 16; i++)
            _segment.TryWrite(MakeHeader(LogLevel.Debug, i), ReadOnlySpan<byte>.Empty);

        Assert.True(_segment.IsFull);
    }

    [Fact]
    public void IsFull_BeforeCapacity_IsFalse()
    {
        _segment.TryWrite(MakeHeader(LogLevel.Information, 1), ReadOnlySpan<byte>.Empty);
        Assert.False(_segment.IsFull);
    }

    // ── IsFrozen ──────────────────────────────────────────────────────────────

    [Fact]
    public void IsFrozen_AfterFreeze_IsTrue()
    {
        Assert.False(_segment.IsFrozen);
        _segment.Freeze();
        Assert.True(_segment.IsFrozen);
    }

    // ── ReadAll ───────────────────────────────────────────────────────────────

    [Fact]
    public void ReadAll_ReturnsAllWrittenEvents_InOrder()
    {
        var timestamps = new[]
        {
            DateTimeOffset.UtcNow.AddMinutes(-2),
            DateTimeOffset.UtcNow.AddMinutes(-1),
            DateTimeOffset.UtcNow,
        };

        foreach (var ts in timestamps)
            _segment.TryWrite(MakeHeader(LogLevel.Information, 1, ts), ReadOnlySpan<byte>.Empty);

        var events = _segment.ReadAll().ToList();
        Assert.Equal(3, events.Count);

        // ReadAll should return in insertion order
        for (int i = 0; i < timestamps.Length; i++)
        {
            Assert.Equal(timestamps[i].UtcTicks, events[i].Timestamp.UtcTicks);
        }
    }

    [Fact]
    public void ReadAll_EmptySegment_ReturnsEmpty()
    {
        Assert.Empty(_segment.ReadAll());
    }

    [Fact]
    public void ReadAll_EventLevelPreserved()
    {
        _segment.TryWrite(MakeHeader(LogLevel.Error, 1), ReadOnlySpan<byte>.Empty);
        _segment.TryWrite(MakeHeader(LogLevel.Warning, 2), ReadOnlySpan<byte>.Empty);

        var events = _segment.ReadAll().ToList();
        Assert.Equal(LogLevel.Error,   events[0].Level);
        Assert.Equal(LogLevel.Warning, events[1].Level);
    }

    // ── Chunk geometry (regression: payload area must not wedge the segment) ────

    /// <summary>
    /// Guards the chunk-geometry invariant: with realistic (&gt;128 B) payloads a segment
    /// must fill to its configured event capacity, spanning multiple chunks, instead of
    /// wedging when the first chunk's payload area fills. A regression here re-introduces
    /// the "flush storm" (tiny cold segments → ingest drops under load).
    /// </summary>
    [Fact]
    public void TryWrite_RealisticPayloads_ReachesEventCapacityAcrossChunks()
    {
        const int count   = 50_000;                 // > ChunkEventCapacity ⇒ multiple chunks
        var       payload = new byte[300];          // > 128 B (the old geometry's per-slot budget)
        new Random(7).NextBytes(payload);

        using var seg = new HotTierSegment(count, (long)count * payload.Length + 1024 * 1024);
        int accepted = 0;
        for (int i = 0; i < count; i++)
            if (seg.TryWrite(MakeHeader(LogLevel.Information, i), payload)) accepted++;

        Assert.Equal(count, accepted);
        Assert.Equal(count, seg.Count);

        // Payload of an event well past the first chunk boundary must round-trip intact.
        var read = seg.GetPropertiesPayload(count - 1);
        Assert.Equal(payload.Length, read.Length);
        Assert.True(read.SequenceEqual(payload));
    }

    // ── Zone map ─────────────────────────────────────────────────────────────

    /// <summary>
    /// The per-chunk [min, max] must follow the events actually written, not the order
    /// they arrived in: a late event with an OLD timestamp lands in the newest chunk and
    /// must widen that chunk's zone, or a query for its window would skip the chunk and
    /// lose it.
    /// </summary>
    [Fact]
    public void ChunkMayOverlap_TracksOutOfOrderArrivalsExactly()
    {
        const int perChunk = HotTierSegment.ChunkEventCapacity;
        long      b        = new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero).UtcTicks;
        var       tick     = TimeSpan.TicksPerSecond;

        using var seg = new HotTierSegment(perChunk * 2 + 16, 32L * 1024 * 1024);

        // Chunk 0: monotone [b, b + perChunk).
        for (int i = 0; i < perChunk; i++)
            Assert.True(seg.TryWrite(MakeHeader(LogLevel.Information, i, new DateTimeOffset(b + i * tick, TimeSpan.Zero)), ReadOnlySpan<byte>.Empty));
        // Chunk 1: starts far in the future…
        for (int i = 0; i < 8; i++)
            Assert.True(seg.TryWrite(MakeHeader(LogLevel.Information, perChunk + i, new DateTimeOffset(b + (perChunk + 1000 + i) * tick, TimeSpan.Zero)), ReadOnlySpan<byte>.Empty));

        long late = b - 500 * tick;   // …then one late arrival older than anything in chunk 0
        Assert.False(seg.ChunkMayOverlap(1, late, late));
        Assert.False(seg.ChunkMayOverlap(1, b + 10 * tick, b + 20 * tick));           // chunk 0's band: not chunk 1's, yet
        Assert.True(seg.TryWrite(MakeHeader(LogLevel.Information, perChunk + 8, new DateTimeOffset(late, TimeSpan.Zero)), ReadOnlySpan<byte>.Empty));

        Assert.True (seg.ChunkMayOverlap(1, late, late));                             // widened downward
        Assert.False(seg.ChunkMayOverlap(0, late, late));                             // chunk 0 untouched
        Assert.True (seg.ChunkMayOverlap(0, b + 10 * tick, b + 20 * tick));
        Assert.True (seg.ChunkMayOverlap(1, b + 10 * tick, b + 20 * tick));           // a zone is a range: the late arrival pulled it over chunk 0's band
        Assert.True (seg.ChunkMayOverlap(1, b + (perChunk + 1003) * tick, long.MaxValue));
        Assert.False(seg.ChunkMayOverlap(0, b + (perChunk + 1003) * tick, long.MaxValue));

        // The span handed to the scan covers exactly the published slots of the chunk.
        Assert.Equal(9, seg.ChunkHeaders(1, seg.Count - perChunk).Length);
        Assert.Equal(late, seg.ChunkHeaders(1, 9)[8].TimestampUtcTicks);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static LogEventHeader MakeHeader(
        LogLevel level, int seq,
        DateTimeOffset? timestamp = null) => new()
    {
        Id                      = new EventId(0u, (uint)seq).RawValue,
        TimestampUtcTicks       = (timestamp ?? DateTimeOffset.UtcNow).UtcTicks,
        Level                   = level,
        MessageTemplatePoolIndex = -1,
        PropertiesArenaOffset   = 0,
        PropertiesByteLength    = 0,
        Flags                   = 0,
    };
}

/// <summary>
/// The per-chunk template / exception arrays are pooled. A recycled array must come back
/// clean: a slot the new tier's writer never fills reads as "no template / no exception",
/// never as whatever the previous tier left there.
/// </summary>
public sealed class HotTierSlotArrayRecyclingTests
{
    [Fact]
    public void A_recycled_chunk_array_never_leaks_the_previous_tiers_templates()
    {
        var ex = new ExceptionInfo { Type = "T", Message = "m" };
        for (int round = 0; round < 8; round++)
        {
            using var tier = new HotTierSegment(64, 64 * 1024);
            // Fill slots 0..15 with a template + exception, then dispose (returns the arrays).
            for (int i = 0; i < 16; i++)
                Assert.True(tier.TryWrite(Header(i), ReadOnlySpan<byte>.Empty, "tmpl-" + i, ex));
        }

        using var fresh = new HotTierSegment(64, 64 * 1024);
        // Write slots 0..15 WITHOUT a template or exception; slot 3 with both.
        for (int i = 0; i < 16; i++)
            Assert.True(fresh.TryWrite(Header(i), ReadOnlySpan<byte>.Empty, i == 3 ? "only-three" : null, i == 3 ? ex : null));

        for (int i = 0; i < 16; i++)
        {
            Assert.Equal(i == 3 ? "only-three" : null, fresh.GetTemplate(i));
            Assert.Same(i == 3 ? ex : null, fresh.GetException(i));
        }
        var events = fresh.ReadAll().ToList();
        Assert.Equal(16, events.Count);
        Assert.All(events.Where(e => e.Id.RawValue != new EventId(0u, 3u).RawValue), e => Assert.Null(e.Exception));
    }

    private static LogEventHeader Header(int seq) => new()
    {
        Id                = new EventId(0u, (uint)seq).RawValue,
        TimestampUtcTicks = DateTimeOffset.UtcNow.UtcTicks,
        Level             = LogLevel.Information,
        MessageTemplatePoolIndex = -1,
    };
}
