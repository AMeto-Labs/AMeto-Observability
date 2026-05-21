using Rd.Log.Core;

namespace Rd.Log.Core.Tests;

public sealed class EventIdTests
{
    // Layout reminder: [42b ms-since-epoch][10b NodeId][12b sequence]
    // Field widths exposed by EventIdGenerator.

    private const ulong MaxMs   = (1UL << EventIdGenerator.TimeBits) - 1;
    private const uint  MaxNode = (1u  << EventIdGenerator.NodeBits) - 1;
    private const uint  MaxSeq  = (1u  << EventIdGenerator.SeqBits)  - 1;

    // ── Construction ──────────────────────────────────────────────────────────

    [Fact]
    public void Ctor_NodeAndSequence_StoresBothParts()
    {
        var id = new EventId(1u, 42u);
        Assert.Equal(0UL, id.TimestampMs);
        Assert.Equal(1u,  id.NodeId);
        Assert.Equal(42u, id.Sequence);
    }

    [Fact]
    public void Ctor_TimeNodeSeq_StoresAllParts()
    {
        var id = new EventId(timestampMsSinceEpoch: 12345UL, nodeId: 3u, sequence: 7u);
        Assert.Equal(12345UL, id.TimestampMs);
        Assert.Equal(3u,      id.NodeId);
        Assert.Equal(7u,      id.Sequence);
    }

    [Fact]
    public void Ctor_RawUlong_RoundTrips()
    {
        var original  = new EventId(timestampMsSinceEpoch: 999UL, nodeId: 5u, sequence: 17u);
        var recreated = new EventId(original.RawValue);
        Assert.Equal(original.TimestampMs, recreated.TimestampMs);
        Assert.Equal(original.NodeId,      recreated.NodeId);
        Assert.Equal(original.Sequence,    recreated.Sequence);
    }

    [Fact]
    public void Empty_IsZero()
    {
        Assert.Equal(0UL, EventId.Empty.TimestampMs);
        Assert.Equal(0u,  EventId.Empty.NodeId);
        Assert.Equal(0u,  EventId.Empty.Sequence);
        Assert.Equal(0UL, EventId.Empty.RawValue);
    }

    // ── RawValue round-trip ───────────────────────────────────────────────────

    public static IEnumerable<object[]> RoundTripCases() => new[]
    {
        new object[] { 0UL,    0u,      0u      },
        new object[] { 1UL,    1u,      1u      },
        new object[] { MaxMs,  MaxNode, MaxSeq  },
        new object[] { 0UL,    MaxNode, MaxSeq  },
        new object[] { MaxMs,  0u,      0u      },
    };

    [Theory]
    [MemberData(nameof(RoundTripCases))]
    public void RawValue_RoundTrips(ulong ms, uint nodeId, uint seq)
    {
        var original  = new EventId(ms, nodeId, seq);
        var recreated = new EventId(original.RawValue);
        Assert.Equal(original.TimestampMs, recreated.TimestampMs);
        Assert.Equal(original.NodeId,      recreated.NodeId);
        Assert.Equal(original.Sequence,    recreated.Sequence);
    }

    // ── Equality ──────────────────────────────────────────────────────────────

    [Fact]
    public void Equals_SameValues_IsTrue()
    {
        var a = new EventId(5u, 10u);
        var b = new EventId(5u, 10u);
        Assert.Equal(a, b);
        Assert.True(a == b);
        Assert.False(a != b);
    }

    [Fact]
    public void Equals_DifferentValues_IsFalse()
    {
        var a = new EventId(5u, 10u);
        var b = new EventId(5u, 11u);
        Assert.NotEqual(a, b);
        Assert.False(a == b);
        Assert.True(a != b);
    }

    [Fact]
    public void GetHashCode_SameValues_SameHash()
    {
        Assert.Equal(new EventId(3u, 7u).GetHashCode(),
                     new EventId(3u, 7u).GetHashCode());
    }

    // ── Ordering ──────────────────────────────────────────────────────────────

    [Fact]
    public void CompareTo_LowerSequence_IsNegative()
    {
        var earlier = new EventId(0u, 1u);
        var later   = new EventId(0u, 2u);
        Assert.True(earlier.CompareTo(later) < 0);
        Assert.True(earlier < later);
        Assert.False(earlier > later);
    }

    [Fact]
    public void CompareTo_HigherTimestamp_IsGreater()
    {
        // Timestamp occupies the high bits, so a later ms beats any node/seq combo.
        var a = new EventId(timestampMsSinceEpoch: 1UL, nodeId: MaxNode, sequence: MaxSeq);
        var b = new EventId(timestampMsSinceEpoch: 2UL, nodeId: 0u,      sequence: 0u);
        Assert.True(a < b);
        Assert.True(b > a);
    }

    [Fact]
    public void CompareTo_SameTimestampHigherNode_IsGreater()
    {
        // Within one ms, NodeId breaks ties (it sits above the sequence).
        var a = new EventId(timestampMsSinceEpoch: 5UL, nodeId: 1u, sequence: MaxSeq);
        var b = new EventId(timestampMsSinceEpoch: 5UL, nodeId: 2u, sequence: 0u);
        Assert.True(a < b);
    }

    // ── ToString ──────────────────────────────────────────────────────────────

    [Fact]
    public void ToString_FormatsAsTimestampNodeSequence()
    {
        var id = new EventId(timestampMsSinceEpoch: 100UL, nodeId: 3u, sequence: 17u);
        Assert.Equal("100:3:17", id.ToString());
    }
}

public sealed class EventIdGeneratorTests
{
    [Fact]
    public void Next_ProducesStrictlyIncreasingIds()
    {
        var gen = new EventIdGenerator(new NodeId(1));
        ulong prev = 0;
        for (int i = 0; i < 10_000; i++)
        {
            var id = gen.Next();
            Assert.True(id > prev, $"id #{i} ({id}) was not greater than previous ({prev})");
            prev = id;
        }
    }

    [Fact]
    public void Next_EncodesNodeId()
    {
        var gen = new EventIdGenerator(new NodeId(7));
        var id  = new EventId(gen.Next());
        Assert.Equal(7u, id.NodeId);
    }

    [Fact]
    public void Next_BorrowsTimeWhenSequenceSaturates()
    {
        // Pin the time component to a fixed past instant so all 4096 sequences
        // have to fit in one ms — generator must roll forward without colliding.
        var gen      = new EventIdGenerator(new NodeId(0));
        long fixedTk = EventIdGenerator.Epoch + TimeSpan.TicksPerMillisecond * 1000;

        var ids = new HashSet<ulong>();
        for (int i = 0; i < 5_000; i++)
            Assert.True(ids.Add(gen.Next(fixedTk)), $"duplicate id at i={i}");

        ulong baselineMs = (ulong)((fixedTk - EventIdGenerator.Epoch) / TimeSpan.TicksPerMillisecond);
        foreach (var raw in ids)
            Assert.True(new EventId(raw).TimestampMs >= baselineMs);
    }

    [Fact]
    public void Next_ConcurrentCallers_ProduceUniqueIds()
    {
        var gen     = new EventIdGenerator(new NodeId(1));
        const int N = 100_000;
        var ids     = new ulong[N];

        Parallel.For(0, N, i => ids[i] = gen.Next());

        Assert.Equal(N, new HashSet<ulong>(ids).Count);
    }
}
