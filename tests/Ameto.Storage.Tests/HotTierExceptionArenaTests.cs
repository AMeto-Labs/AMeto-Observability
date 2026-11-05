using System.Runtime.InteropServices;
using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Storage.Tests;

/// <summary>
/// The exception arena's bounds. The payload limit is HARD for exception bytes, as it is for the text
/// an event brings of its own and unlike the properties, which run on into their chunk's slack: the
/// arena takes a block whenever the last is full, so nothing else would stop a stream of large
/// exceptions short of the chunk's 16 384 slots. An exception longer than a block gets a block of
/// its own (a replay of a log an older release wrote); an allocation that throws stores nothing; and
/// a materialised row's exception is a view into the arena until the row owns it.
/// </summary>
public sealed class HotTierExceptionArenaTests
{
    private const int KB = 1024;

    [Fact]
    public void An_exception_bearing_event_past_the_tiers_size_is_refused()
    {
        const long Limit = 1024 * KB;
        byte[] exc = ExceptionBytes(64 * KB);
        using var tier = new HotTierSegment(64, Limit);
        for (int i = 0; i < 16; i++)
            Assert.True(tier.TryWrite(Header(i), ReadOnlySpan<byte>.Empty, null, exc), $"event {i} ({(i + 1) * 64} KB of the limit's {Limit / KB} KB) was refused");
        Assert.True(tier.IsFull, "a tier holding its limit of exception bytes did not read full");
        Assert.Equal(Limit, tier.ExceptionBytes);

        Assert.False(tier.TryWrite(Header(16), ReadOnlySpan<byte>.Empty, null, exc), "an exception-bearing event past the limit was taken");
        Assert.False(tier.TryWrite(Header(16), ReadOnlySpan<byte>.Empty, null, ExceptionBytes(1)), "one byte of exception past the limit was taken");
        Assert.Equal(16, tier.Count);
        Assert.Equal(Limit, tier.ExceptionBytes);

        // The properties alone run on into the chunk's slack, as they always did: for them the limit is the flush trigger.
        Assert.True(tier.TryWrite(Header(17), new byte[100], null, default), "an event without an exception was refused at the limit");
        Assert.Equal(17, tier.Count);

        using var empty = new HotTierSegment(64, Limit);
        Assert.True(empty.TryWrite(Header(0), ReadOnlySpan<byte>.Empty, null, ExceptionBytes(4 * 1024 * KB)), "an empty tier refused an exception larger than a tier");
        Assert.True(empty.IsFull);
        Assert.False(empty.TryWrite(Header(1), ReadOnlySpan<byte>.Empty, null, ExceptionBytes(1)), "a tier over its limit took one more exception");
    }

    [Fact]
    public void An_exception_longer_than_a_block_gets_a_block_of_its_own()
    {
        using var tier = new HotTierSegment(64, 64L * 1024 * KB);
        byte[] small = ExceptionBytes(1000);
        byte[] large = ExceptionBytes(HotTierSegment.ExceptionBlockBytes + 12_345);
        Assert.True(tier.TryWrite(Header(0), ReadOnlySpan<byte>.Empty, null, small));
        long oneBlock = tier.AllocatedBytes;
        Assert.True(tier.TryWrite(Header(1), ReadOnlySpan<byte>.Empty, null, large), "an exception longer than a block was refused");
        Assert.True(tier.TryWrite(Header(2), ReadOnlySpan<byte>.Empty, null, small));

        Assert.True(tier.GetExceptionPayload(0).SequenceEqual(small));
        Assert.True(tier.GetExceptionPayload(1).SequenceEqual(large), "the large exception did not read back whole");
        Assert.True(tier.GetExceptionPayload(2).SequenceEqual(small));
        Assert.True(tier.GetExceptionMemory(1).Span.SequenceEqual(large));
        Assert.Equal(2 * small.Length + large.Length, tier.ExceptionBytes);
        // A block of its own size for the large one, and the next small one in a fresh block of the usual size.
        Assert.Equal(oneBlock + large.Length + HotTierSegment.ExceptionBlockBytes, tier.AllocatedBytes);
    }

    /// <summary>
    /// A rent of the chunk's exception-positions array, or an exception block's allocation, that runs out of
    /// memory: a write that stored nothing — no count, no bytes, no block half-made — and the arena whole after it.
    /// </summary>
    [Theory]
    [InlineData((byte)HotTierSegment.SlotKind.ExceptionOffsets)]
    [InlineData((byte)HotTierSegment.SlotKind.ExceptionBlock)]
    public void A_write_that_throws_renting_or_allocating_for_its_exception_stores_nothing(byte kind)
    {
        var failing = (HotTierSegment.SlotKind)kind;
        byte[] ex = new ExceptionInfo { Type = "T", Message = "m" }.ToBytes();
        using var tier = new HotTierSegment(64, 64 * KB);
        long allocated = tier.AllocatedBytes;
        tier._beforeSlotRentForTest = k => { if (k == failing) throw new OutOfMemoryException("test: no memory for " + k); };
        Assert.Throws<OutOfMemoryException>(() => tier.TryWrite(Header(0), ReadOnlySpan<byte>.Empty, "the failed write's {n}", ex, "unpooled.svc"));
        Assert.Equal(0, tier.Count);
        Assert.Equal(0, tier.ExceptionBytes);
        Assert.Equal(allocated, tier.AllocatedBytes);

        tier._beforeSlotRentForTest = null;
        Assert.True(tier.TryWrite(Header(1), ReadOnlySpan<byte>.Empty, template: null, exception: default));   // the same slot
        Assert.Null(tier.GetTemplate(0));
        Assert.Null(tier.GetService(0));
        Assert.True(tier.GetExceptionPayload(0).IsEmpty);

        Assert.True(tier.TryWrite(Header(2), ReadOnlySpan<byte>.Empty, "t", ex, "s"));
        Assert.True(tier.GetExceptionPayload(1).SequenceEqual(ex));
        Assert.Equal(ex.Length, tier.ExceptionBytes);
    }

    [Fact]
    public void A_materialised_rows_exception_is_a_view_into_the_arena_until_the_row_owns_it()
    {
        byte[] ex = new ExceptionInfo { Type = "System.TimeoutException", Message = "slow", StackTrace = new string('s', 3000) }.ToBytes();
        var tier = new HotTierSegment(64, 64 * KB);
        Assert.True(tier.TryWrite(Header(0), ReadOnlySpan<byte>.Empty, "t", ex));
        Assert.True(tier.TryWrite(Header(1), ReadOnlySpan<byte>.Empty, "t", ex));

        var view  = tier.Materialise(0, null);
        var owned = tier.Materialise(1, null);
        Assert.False(MemoryMarshal.TryGetArray(view.RawException, out _), "the scan's row copied its exception out of the tier");
        Assert.True(view.RawException.Span.SequenceEqual(ex));
        Assert.Equal("System.TimeoutException", ExceptionInfo.ReadField(view.RawException, ExceptionInfo.Field.Type));   // read where it sits
        Assert.True(view.HasException);

        for (int i = 0; i < 100; i++) _ = tier.Materialise(0, null).RawException;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++) _ = tier.Materialise(0, null).RawException;
        long perRow = (GC.GetAllocatedBytesForCurrentThread() - before) / 1_000;
        Assert.True(perRow < 300, $"materialising a row allocated {perRow} B: the 3 KB exception is being copied");

        owned.OwnRawBytes();
        Assert.True(MemoryMarshal.TryGetArray(owned.RawException, out _), "an owned row still points into the tier");
        Assert.True(owned.RawException.Span.SequenceEqual(ex));
        var all = tier.ReadAll().ToList();
        Assert.All(all, e => Assert.True(MemoryMarshal.TryGetArray(e.RawException, out _), "ReadAll handed out a view"));

        tier.Dispose();
        Assert.True(owned.RawException.Span.SequenceEqual(ex));                        // the copy outlives the tier
        Assert.Equal("slow", all[0].Exception?.Message);
        Assert.Throws<ArgumentOutOfRangeException>(() => _ = view.RawException.Span);   // a view left over fails loudly rather than read freed memory
    }

    private static byte[] ExceptionBytes(int bytes)
    {
        var b = new byte[bytes];
        new Random(bytes).NextBytes(b);
        return b;
    }

    private static LogEventHeader Header(int seq) => new()
    {
        Id                       = new EventId(0u, (uint)seq).RawValue,
        TimestampUtcTicks        = DateTimeOffset.UtcNow.UtcTicks,
        Level                    = LogLevel.Error,
        MessageTemplatePoolIndex = -1,
        ServiceNamePoolIndex     = -1,
    };
}
