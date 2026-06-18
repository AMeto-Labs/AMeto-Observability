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
