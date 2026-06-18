using Ameto.Core;

namespace Ameto.Core.Tests;

public sealed class IdentifiersTests
{
    // ── NodeId ────────────────────────────────────────────────────────────────

    [Fact]
    public void NodeId_Local_IsZero()
    {
        Assert.Equal(0u, NodeId.Local.Value);
    }

    [Fact]
    public void NodeId_Equals_SameValue()
    {
        var a = new NodeId(42u);
        var b = new NodeId(42u);
        Assert.True(a.Equals(b));
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void NodeId_Equals_DifferentValue()
    {
        var a = new NodeId(1u);
        var b = new NodeId(2u);
        Assert.False(a.Equals(b));
    }

    [Fact]
    public void NodeId_ToString_ReturnsValueString()
    {
        Assert.Equal("7", new NodeId(7u).ToString());
    }

    // ── SegmentId ─────────────────────────────────────────────────────────────

    [Fact]
    public void SegmentId_Equals_SameValue()
    {
        var a = new SegmentId(100UL);
        var b = new SegmentId(100UL);
        Assert.True(a.Equals(b));
        Assert.True(a == b);
        Assert.False(a != b);
    }

    [Fact]
    public void SegmentId_Equals_DifferentValue()
    {
        var a = new SegmentId(1UL);
        var b = new SegmentId(2UL);
        Assert.False(a.Equals(b));
        Assert.True(a != b);
    }

    [Fact]
    public void SegmentId_ToString_ReturnsValueString()
    {
        Assert.Equal("999", new SegmentId(999UL).ToString());
    }
}

public sealed class RetentionPolicyTests
{
    [Theory]
    [InlineData(LogLevel.Verbose,     90)]
    [InlineData(LogLevel.Debug,        3)]
    [InlineData(LogLevel.Information, 90)]
    [InlineData(LogLevel.Warning,     90)]
    [InlineData(LogLevel.Error,       90)]
    [InlineData(LogLevel.Fatal,       90)]
    public void Default_GetTtl_ReturnsExpectedDays(LogLevel level, int days)
    {
        Assert.Equal(TimeSpan.FromDays(days), RetentionPolicy.Default.GetTtl(level));
    }

    [Fact]
    public void GetTtl_UnknownLevel_ReturnsFallback90Days()
    {
        var policy = RetentionPolicy.Default;
        Assert.Equal(TimeSpan.FromDays(90), policy.GetTtl((LogLevel)99));
    }

    [Fact]
    public void SegmentInfo_IsExpired_OldSegment_ReturnsTrue()
    {
        var now = DateTimeOffset.UtcNow;
        // Debug TTL = 3 days; segment maxTs = 100 days ago → expired
        var info = new SegmentInfo
        {
            Id                = new SegmentId(1),
            NodeId            = NodeId.Local,
            FilePath          = "seg.bin",
            MinTimestampTicks = now.AddDays(-101).UtcTicks,
            MaxTimestampTicks = now.AddDays(-100).UtcTicks,
            EventCount        = 1,
            MinLevel          = LogLevel.Debug,
            CompressedBytes   = 0,
            UncompressedBytes = 0,
        };

        Assert.True(info.IsExpired(RetentionPolicy.Default, now));
    }

    [Fact]
    public void SegmentInfo_IsExpired_FreshSegment_ReturnsFalse()
    {
        var now = DateTimeOffset.UtcNow;
        // Error TTL = 90 days; segment maxTs = 1 day ago → not expired
        var info = new SegmentInfo
        {
            Id                = new SegmentId(2),
            NodeId            = NodeId.Local,
            FilePath          = "seg.bin",
            MinTimestampTicks = now.AddDays(-2).UtcTicks,
            MaxTimestampTicks = now.AddDays(-1).UtcTicks,
            EventCount        = 1,
            MinLevel          = LogLevel.Error,
            CompressedBytes   = 0,
            UncompressedBytes = 0,
        };

        Assert.False(info.IsExpired(RetentionPolicy.Default, now));
    }
}
