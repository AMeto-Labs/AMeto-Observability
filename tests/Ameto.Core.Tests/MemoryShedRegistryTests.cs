using Ameto.Core;

namespace Ameto.Core.Tests;

/// <summary>
/// The inversion that lets the RAM-pressure loop reach memory it cannot reference.
///
/// <para><c>RamPressureService</c> lives in <c>Ameto.Storage</c>; the segment-index cache lives in
/// <c>Ameto.Indexing</c>, which references Storage — so the pressure path cannot call the cache,
/// and the cache holds <c>NativeMemory</c> that no collection the pressure path forces will ever
/// return. This registry is the seam: Core is the one assembly both can see.</para>
///
/// <para>What these pin is the contract the pressure loop depends on — a registration is asked,
/// a disposed one is not, the native share is reported apart from the total so the caller's
/// "is this pressure ours?" test can add it without counting the managed half twice, and one
/// misbehaving shedder cannot stop the others being asked.</para>
/// </summary>
public sealed class MemoryShedRegistryTests
{
    private sealed class FakeShedder : IMemoryShedder
    {
        public long Total;
        public long Native;
        public int  ShedCalls;

        public long ShedableBytes       => Total;
        public long ShedableNativeBytes => Native;

        public long Shed()
        {
            ShedCalls++;
            long released = Total;
            Total  = 0;
            Native = 0;
            return released;
        }
    }

    /// <summary>A shedder that fails the way a real one might: mid-shutdown, mid-dispose.</summary>
    private sealed class ThrowingShedder : IMemoryShedder
    {
        public long ShedableBytes       => throw new InvalidOperationException("boom");
        public long ShedableNativeBytes => throw new InvalidOperationException("boom");
        public long Shed()              => throw new InvalidOperationException("boom");
    }

    [Fact]
    public void A_registered_shedder_is_asked_and_reports_its_bytes()
    {
        var s = new FakeShedder { Total = 1000, Native = 250 };

        long baseTotal  = MemoryShedRegistry.ShedableBytes;
        long baseNative = MemoryShedRegistry.ShedableNativeBytes;
        using (MemoryShedRegistry.Register(s))
        {
            Assert.Equal(baseTotal  + 1000, MemoryShedRegistry.ShedableBytes);
            // Reported apart from the total: the caller already counts the managed half through
            // the GC's own heap figure, so it may only add THIS to its reclaimable estimate.
            Assert.Equal(baseNative + 250,  MemoryShedRegistry.ShedableNativeBytes);

            Assert.Equal(1000L, MemoryShedRegistry.Shed());
            Assert.Equal(1, s.ShedCalls);
            Assert.Equal(baseTotal, MemoryShedRegistry.ShedableBytes);   // it let go
        }
    }

    [Fact]
    public void A_disposed_registration_is_never_asked_again()
    {
        var s = new FakeShedder { Total = 500, Native = 100 };

        int  baseCount = MemoryShedRegistry.RegisteredCount;
        var  token     = MemoryShedRegistry.Register(s);
        Assert.Equal(baseCount + 1, MemoryShedRegistry.RegisteredCount);

        token.Dispose();
        Assert.Equal(baseCount, MemoryShedRegistry.RegisteredCount);

        MemoryShedRegistry.Shed();
        Assert.Equal(0, s.ShedCalls);
        Assert.Equal(500, s.Total);      // untouched

        token.Dispose();                 // idempotent
        Assert.Equal(baseCount, MemoryShedRegistry.RegisteredCount);
    }

    /// <summary>
    /// This runs from the pressure loop, where the flush and the collection that follow matter
    /// more than any one shedder. A throw must not blind the rest or escape into that loop.
    /// </summary>
    [Fact]
    public void One_shedder_that_throws_does_not_stop_the_others()
    {
        var good = new FakeShedder { Total = 700, Native = 300 };

        using (MemoryShedRegistry.Register(new ThrowingShedder()))
        using (MemoryShedRegistry.Register(good))
        {
            long baseTotal = 0;   // the throwing one contributes nothing rather than failing
            Assert.Equal(baseTotal + 700, MemoryShedRegistry.ShedableBytes);
            Assert.Equal(baseTotal + 300, MemoryShedRegistry.ShedableNativeBytes);

            Assert.Equal(700L, MemoryShedRegistry.Shed());
            Assert.Equal(1, good.ShedCalls);
        }
    }

    [Fact]
    public void Nothing_registered_sheds_nothing()
    {
        Assert.Equal(0L, MemoryShedRegistry.Shed());
        Assert.Equal(0L, MemoryShedRegistry.ShedableBytes);
        Assert.Equal(0L, MemoryShedRegistry.ShedableNativeBytes);
        Assert.Equal(0,  MemoryShedRegistry.RegisteredCount);
    }
}
