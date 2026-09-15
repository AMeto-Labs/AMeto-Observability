using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Ameto.Core;
using Ameto.Storage;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// MemoryBudgetTests checks the arithmetic; this checks that the ENGINE uses it. The behaviour
/// that matters on a small host — fewer frozen-tier slots and a narrower flush width — lives in
/// the constructor's flush semaphores, and nothing looked at them: putting the old flat
/// 640 MB / 512 MB constants back left the whole suite green.
///
/// <para>The tier is the console stand's 16 MB: two 9 MB chunks (1 MB of 64-byte headers + 8 MB of
/// payload) = 18 MB native per frozen tier, and 32,768 events x 1,400 B = 43.75 MB of managed
/// index-build state per flush.</para>
/// </summary>
public sealed class StorageEngineBudgetWiringTests : IDisposable
{
    private const long MB = 1024 * 1024;
    private const long GB = 1024 * MB;

    private readonly string _root = Path.Combine(Path.GetTempPath(), "Ameto-budgetwiring-" + Guid.NewGuid().ToString("N"));

    public void Dispose() { try { Directory.Delete(_root, true); } catch { } }

    private StorageEngine NewEngine(MemoryBudgets budgets)
    {
        string dir = Path.Combine(_root, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var opts = new ServerOptions
        {
            DataDirectory = dir,
            HotTier       = new HotTierOptions { MaxSizeBytes = 16 * MB },
        };
        return new StorageEngine(
            Options.Create(opts),
            new RetentionStore(opts, NullLogger<RetentionStore>.Instance),
            NullLogger<StorageEngine>.Instance,
            budgets);
    }

    /// <summary>What the core-count half of the width formula allows, given what memory allows.</summary>
    private static int WidthFor(int widthByMemory) =>
        Math.Clamp(Math.Min(Environment.ProcessorCount / 2, widthByMemory), 1, 8);

    /// <summary>
    /// The stand as its container presents it: 384 MB heap limit, 512 MB physical. Native budget
    /// 128 MB / 18 MB = 7 slots (the flat 512 MB allowed 28 — half a gigabyte of frozen tiers on a
    /// 512 MB host); managed budget 115 MB / 43.75 MB = 2 concurrent builds (the flat 640 MB allowed 14).
    /// </summary>
    [Fact]
    public async Task In_a_512_mb_container_a_16_mb_tier_gets_seven_slots_and_at_most_two_builds()
    {
        await using var engine = NewEngine(MemoryBudgets.Derive(managedLimitBytes: 384 * MB, physicalLimitBytes: 512 * MB));

        Assert.Equal(7, engine.FlushSlots);
        Assert.Equal(WidthFor(widthByMemory: 2), engine.FlushWidth);
        Assert.True(engine.FlushWidth <= 2, $"width {engine.FlushWidth} exceeds what 115 MB of builds affords");
    }

    /// <summary>A host with room keeps the ceilings it always had: 512 MB / 18 MB = 28 slots, 640 MB / 43.75 MB = 14.</summary>
    [Fact]
    public async Task On_a_64_gb_host_the_same_tier_keeps_the_fixed_ceilings()
    {
        await using var engine = NewEngine(MemoryBudgets.Derive(64 * GB));

        Assert.Equal(28, engine.FlushSlots);
        Assert.Equal(WidthFor(widthByMemory: 14), engine.FlushWidth);
    }
}
