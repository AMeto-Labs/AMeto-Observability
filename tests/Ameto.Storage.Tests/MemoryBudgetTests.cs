using System.Diagnostics;
using Ameto.Core;
using Ameto.Storage;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// The flush budgets used to be flat constants — 640 MB of concurrent index builds, 512 MB of
/// frozen tiers, a 256 MB index cache — chosen for a host with room for them and never
/// reconsidered. On the 512 MB console stand that is over a gigabyte of intent, under a runtime
/// whose own managed hard limit is 384 MB, and the failure mode is an OOM kill that reads like
/// a crash. Each ceiling is now min(constant, a share of a limit).
///
/// <para>Two limits, not one. A 512 MB container never shows the GC 512 MB: the GC's heap hard
/// limit is 75 % of it, 384 MB, and that is what TotalAvailableMemoryBytes reports. Managed
/// shares are taken of that; the native frozen-tier share is taken of the container limit,
/// because native memory is not under the GC's hard limit at all. A single base under-provisioned
/// native by a quarter in every container (96 MB instead of 128 MB at 512 MB).</para>
///
/// <para>Derive is a pure function over injected figures so the arithmetic can be checked at
/// 512 MB, 4 GB and 64 GB without three machines; <c>Current()</c> is checked end to end in child
/// processes started under a GC memory setting, because the GC reads its limits once at startup.</para>
/// </summary>
public sealed class MemoryBudgetTests
{
    private const long MB = 1024 * 1024;
    private const long GB = 1024 * MB;

    /// <summary>The stand as a real 512 MB container presents itself: a 384 MB heap limit, 512 MB physical.</summary>
    [Fact]
    public void In_a_512_mb_container_managed_shares_come_from_the_heap_limit_and_native_from_the_container()
    {
        var b = MemoryBudgets.Derive(managedLimitBytes: 384 * MB, physicalLimitBytes: 512 * MB);

        Assert.Equal((long)(384 * MB * 0.30), b.ManagedBuildBytes);   // 115 MB
        Assert.Equal((long)(512 * MB * 0.25), b.NativeTierBytes);     // 128 MB
        Assert.Equal((long)(384 * MB * 0.15), b.IndexCacheBytes);     //  57 MB
        Assert.True(b.IsConstrained);

        // The managed shares must leave the heap room for queries, ASP.NET and the GC itself, and
        // all three must leave the container room for the runtime, the ring and the live tier.
        Assert.True(b.ManagedBuildBytes + b.IndexCacheBytes < 384 * MB / 2);
        Assert.True(b.ManagedBuildBytes + b.NativeTierBytes + b.IndexCacheBytes < 512 * MB * 0.70);
    }

    /// <summary>
    /// The ingest body-buffer pool is the fourth ceiling, and the last one that sized itself from
    /// something other than memory: its depth came from <c>2 x ProcessorCount</c>, so a 512 MB
    /// container on a 16-core host took the 32-deep ceiling and could park more than the whole
    /// container. It is a share of the MANAGED limit, because request bodies are byte arrays on
    /// the large object heap.
    /// </summary>
    [Fact]
    public void The_ingest_buffer_pool_is_a_share_of_the_heap_limit_like_the_other_managed_ceilings()
    {
        var stand = MemoryBudgets.Derive(managedLimitBytes: 384 * MB, physicalLimitBytes: 512 * MB);

        Assert.Equal((long)(384 * MB * 0.10), stand.IngestBufferBytes);            // 38 MB
        Assert.True(stand.IngestBufferBytes < MemoryBudgets.IngestBufferCapBytes);

        // Every managed ceiling together still has to leave the heap room for queries, ASP.NET
        // and the GC itself — the sum the pool used to sit outside of.
        Assert.True(stand.ManagedBuildBytes + stand.IndexCacheBytes + stand.IngestBufferBytes < 384 * MB * 0.60);

        // A host with room keeps the absolute ceiling, and an unknown limit falls back to it
        // rather than strangling a healthy machine.
        Assert.Equal(MemoryBudgets.IngestBufferCapBytes, MemoryBudgets.Derive(64 * GB).IngestBufferBytes);
        Assert.Equal(MemoryBudgets.IngestBufferCapBytes, MemoryBudgets.Derive(0).IngestBufferBytes);
        Assert.True(MemoryBudgets.Derive(32 * MB).IngestBufferBytes >= 8 * MB);    // the floor
    }

    /// <summary>
    /// The ingest payload arena is the largest native consumer of all — 512 MB by default, the
    /// whole of the console stand's container, reserved for one buffer. Its pages are never given
    /// back once touched, so its high-water mark is a resting level; the reserve-and-commit
    /// mitigation is Windows-only, which is not the 512 MB Linux stand. It is a share of the
    /// PHYSICAL limit, like the frozen tiers and for the same reason: it is native memory, not
    /// under the GC's hard limit.
    /// </summary>
    [Fact]
    public void The_ingest_arena_is_a_share_of_the_physical_limit_and_the_ring_takes_it()
    {
        var stand = MemoryBudgets.Derive(managedLimitBytes: 384 * MB, physicalLimitBytes: 512 * MB);

        Assert.Equal((long)(512 * MB * 0.15), stand.IngestArenaBytes);              // 76 MB
        Assert.True(stand.IngestArenaBytes < MemoryBudgets.IngestArenaCapBytes);

        // Native ceilings together have to leave the container room for the managed heap, the
        // live tier and the runtime itself.
        Assert.True(stand.NativeTierBytes + stand.IngestArenaBytes < 512 * MB / 2);

        // A host with room keeps the 512 MB default; an unknown limit falls back to it.
        Assert.Equal(MemoryBudgets.IngestArenaCapBytes, MemoryBudgets.Derive(64 * GB).IngestArenaBytes);
        Assert.Equal(MemoryBudgets.IngestArenaCapBytes, MemoryBudgets.Derive(0).IngestArenaBytes);
        Assert.True(MemoryBudgets.Derive(32 * MB).IngestArenaBytes >= 16 * MB);     // the floor

        // …and the ring is what takes it, unless an operator says otherwise.
        Assert.Equal(MemoryBudgets.Current().IngestArenaBytes, new IngestionOptions().EffectivePayloadPoolBytes);
        Assert.Equal(96 * MB, new IngestionOptions { PayloadPoolBytes = 96 * MB }.EffectivePayloadPoolBytes);
    }

    /// <summary>
    /// A big host that caps its managed heap with GCHeapHardLimit has said nothing about native
    /// memory, so the native budget must not shrink with the heap.
    /// </summary>
    [Fact]
    public void A_low_heap_hard_limit_on_a_big_host_does_not_shrink_the_native_budget()
    {
        var b = MemoryBudgets.Derive(managedLimitBytes: 512 * MB, physicalLimitBytes: 64 * GB);

        Assert.Equal((long)(512 * MB * 0.30), b.ManagedBuildBytes);
        Assert.Equal(MemoryBudgets.NativeTierCapBytes, b.NativeTierBytes);
        Assert.Equal((long)(512 * MB * 0.15), b.IndexCacheBytes);
    }

    [Fact]
    public void At_4_gb_the_shares_are_above_two_ceilings_and_below_none()
    {
        var b = MemoryBudgets.Derive(4 * GB);

        Assert.Equal(MemoryBudgets.ManagedBuildCapBytes, b.ManagedBuildBytes);  // 30 % = 1.2 GB > cap
        Assert.Equal(MemoryBudgets.NativeTierCapBytes,   b.NativeTierBytes);    // 25 % = 1.0 GB > cap
        Assert.Equal(MemoryBudgets.IndexCacheCapBytes,   b.IndexCacheBytes);    // 15 % = 614 MB > cap
        Assert.False(b.IsConstrained);
    }

    [Fact]
    public void At_64_gb_nothing_grows_past_the_fixed_ceilings()
    {
        var b = MemoryBudgets.Derive(64 * GB);

        Assert.Equal(MemoryBudgets.ManagedBuildCapBytes, b.ManagedBuildBytes);
        Assert.Equal(MemoryBudgets.NativeTierCapBytes,   b.NativeTierBytes);
        Assert.Equal(MemoryBudgets.IndexCacheCapBytes,   b.IndexCacheBytes);
        Assert.False(b.IsConstrained);
    }

    /// <summary>
    /// A figure the runtime could not produce must not be read as "almost no memory" — that
    /// would silently strangle a healthy host. Fall back to the constants, which is what the
    /// engine did before any of this existed.
    /// </summary>
    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    public void Unknown_available_memory_falls_back_to_the_constants(long available)
    {
        var b = MemoryBudgets.Derive(available);

        Assert.Equal(MemoryBudgets.ManagedBuildCapBytes, b.ManagedBuildBytes);
        Assert.Equal(MemoryBudgets.NativeTierCapBytes,   b.NativeTierBytes);
        Assert.Equal(MemoryBudgets.IndexCacheCapBytes,   b.IndexCacheBytes);
        Assert.False(b.IsConstrained);
    }

    /// <summary>An unknown physical limit borrows the managed one rather than the constant.</summary>
    [Fact]
    public void Unknown_physical_limit_borrows_the_managed_limit()
    {
        var b = MemoryBudgets.Derive(managedLimitBytes: 384 * MB, physicalLimitBytes: 0);
        Assert.Equal((long)(384 * MB * 0.25), b.NativeTierBytes);
        Assert.Equal(384 * MB, b.PhysicalLimitBytes);
    }

    /// <summary>An absurd limit still has to yield an engine that can flush at all.</summary>
    [Fact]
    public void Floors_keep_a_tiny_limit_workable()
    {
        var b = MemoryBudgets.Derive(32 * MB);

        Assert.True(b.ManagedBuildBytes >= 16 * MB);
        Assert.True(b.NativeTierBytes   >= 16 * MB);
        Assert.True(b.IndexCacheBytes   >=  8 * MB);
    }

    [Fact]
    public void Budgets_rise_monotonically_with_available_memory()
    {
        long prevManaged = 0, prevNative = 0, prevCache = 0;
        for (long available = 64 * MB; available <= 64 * GB; available *= 2)
        {
            var b = MemoryBudgets.Derive(available * 3 / 4, available);
            Assert.True(b.ManagedBuildBytes >= prevManaged);
            Assert.True(b.NativeTierBytes   >= prevNative);
            Assert.True(b.IndexCacheBytes   >= prevCache);
            (prevManaged, prevNative, prevCache) = (b.ManagedBuildBytes, b.NativeTierBytes, b.IndexCacheBytes);
        }
    }

    /// <summary>
    /// The GC computes its high-memory-load threshold as GCHighMemPercent of the physical limit,
    /// truncating; dividing back out has to land on the limit, not a few bytes under it.
    /// </summary>
    [Theory]
    [InlineData(512L,     90)]
    [InlineData(32_541L,  90)]
    [InlineData(512L,     50)]
    [InlineData(100_000L, 97)]
    public void The_physical_limit_is_recovered_from_the_high_memory_load_threshold(long physicalMb, int percent)
    {
        long physical  = physicalMb * MB;
        long threshold = (long)((double)percent / 100 * physical);   // the GC's own arithmetic

        Assert.Equal(physical, MemoryBudgets.PhysicalLimitFrom(threshold, percent, managedLimitBytes: 1));
    }

    [Theory]
    [InlineData(0L,    90)]
    [InlineData(1000L, 0)]
    [InlineData(1000L, 101)]
    public void An_unusable_threshold_falls_back_to_the_managed_limit(long threshold, int percent) =>
        Assert.Equal(384 * MB, MemoryBudgets.PhysicalLimitFrom(threshold, percent, 384 * MB));

    /// <summary>
    /// The reason the native budget had to scale too: the frozen-tier slot count is the native
    /// budget divided by one tier's real footprint, so the stand's 16 MB tier used to clamp at
    /// 512 MB / 18 MB = 28 slots — half a gigabyte of frozen tiers allowed on a 512 MB host.
    /// (StorageEngineBudgetWiringTests checks the engine actually uses these numbers.)
    /// </summary>
    [Fact]
    public void Slot_count_on_the_512_mb_stand_no_longer_allows_half_a_gigabyte_of_tiers()
    {
        long tierFootprint = HotTierSegment.NativeBytesFor(16 * MB);

        int before = (int)Math.Clamp(MemoryBudgets.NativeTierCapBytes / tierFootprint, 1, 64);
        int after  = (int)Math.Clamp(MemoryBudgets.Derive(384 * MB, 512 * MB).NativeTierBytes / tierFootprint, 1, 64);

        Assert.True(after < before, $"slots should shrink on a small host: {before} -> {after}");
        Assert.True(after >= 1,     "at least one flush must always be able to proceed");
        Assert.True((long)after * tierFootprint <= 512 * MB / 4,
            "frozen tiers must stay inside a quarter of the host");
    }

    /// <summary>An explicit configuration value wins over anything derived, in both directions.</summary>
    [Fact]
    public void Configured_index_cache_budget_wins()
    {
        Assert.Equal(48 * MB, new QueryOptions { IndexCacheBytes = 48 * MB }.EffectiveIndexCacheBytes);
        Assert.Equal(0,       new QueryOptions { IndexCacheBytes = 0 }.EffectiveIndexCacheBytes);
        Assert.Equal(4 * GB,  new QueryOptions { IndexCacheBytes = 4 * GB }.EffectiveIndexCacheBytes);

        long derived = new QueryOptions().EffectiveIndexCacheBytes;
        Assert.Equal(MemoryBudgets.Current().IndexCacheBytes, derived);
        Assert.True(derived > 0 && derived <= MemoryBudgets.IndexCacheCapBytes);
    }

    // ── Current(), end to end, in a process started under a GC memory setting ─────────────

    /// <summary>
    /// The stand's shape without a container: DOTNET_GCTotalPhysicalMemory tells the GC the physical
    /// limit, and the GC treats it exactly as a detected cgroup or job-object limit — a 75 % heap
    /// hard limit follows (measured: 384 MB of 512 MB, the same as a 512 MB job object). Works on
    /// every OS. With one base, this child reported native 96 MB.
    /// </summary>
    [Fact]
    public void Current_in_a_512_mb_container_takes_native_from_the_container_not_the_heap_limit()
    {
        var child = RunChild(("DOTNET_GCTotalPhysicalMemory", "0x20000000"));   // 512 MB

        Assert.Equal(384 * MB, child.ManagedLimit);
        Assert.Equal(512 * MB, child.PhysicalLimit);

        var expected = MemoryBudgets.Derive(384 * MB, 512 * MB);
        Assert.Equal(expected.ManagedBuildBytes, child.ManagedBuild);   // 115 MB
        Assert.Equal(expected.NativeTierBytes,   child.NativeTier);     // 128 MB
        Assert.Equal(expected.IndexCacheBytes,   child.IndexCache);     //  57 MB
    }

    /// <summary>
    /// A 512 MB GCHeapHardLimit on this host caps the managed shares and leaves the native one on
    /// the host's physical memory, which this (unlimited) test process reads the same way.
    /// </summary>
    [Fact]
    public void Current_under_a_low_heap_hard_limit_keeps_native_on_the_host()
    {
        long hostPhysical = MemoryBudgets.Current().PhysicalLimitBytes;
        Assert.True(hostPhysical > 1 * GB, $"this test needs a host with more than 1 GB, saw {hostPhysical / MB} MB");

        var child = RunChild(("DOTNET_GCHeapHardLimit", "0x20000000"));        // 512 MB

        Assert.Equal(512 * MB,     child.ManagedLimit);
        Assert.Equal(hostPhysical, child.PhysicalLimit);

        var expected = MemoryBudgets.Derive(512 * MB, hostPhysical);
        Assert.Equal(expected.ManagedBuildBytes, child.ManagedBuild);
        Assert.Equal(expected.NativeTierBytes,   child.NativeTier);
        Assert.True(child.NativeTier > MemoryBudgets.Derive(512 * MB).NativeTierBytes,
            "a heap hard limit must not shrink the native budget");
    }

    /// <summary>
    /// The physical limit is the threshold divided by GCHighMemPercent, so that has to be the
    /// percentage the GC actually used. A 512 MB container that sets GCHighMemPercent=70 has a
    /// 358 MB threshold. Divided by the default 90 %, that reads as a 398 MB container and a 99 MB
    /// native budget instead of 128 MB. The two tests above run at the default, so they cannot
    /// tell the lookup from the constant.
    /// </summary>
    [Fact]
    public void Current_divides_by_a_configured_high_memory_percent_not_the_default()
    {
        var child = RunChild(("DOTNET_GCTotalPhysicalMemory", "0x20000000"),    // 512 MB
                             ("DOTNET_GCHighMemPercent",      "46"));           // hex: 70 %

        Assert.Equal(384 * MB, child.ManagedLimit);
        Assert.Equal(512 * MB, child.PhysicalLimit);
        Assert.Equal(128 * MB, child.NativeTier);
    }

    private readonly record struct ChildBudgets(
        long ManagedLimit, long PhysicalLimit, long ManagedBuild, long NativeTier, long IndexCache);

    /// <summary>Runs this test assembly's own entry point (<see cref="ChildProcessEntry"/>) under the given GC settings.</summary>
    private static ChildBudgets RunChild(params ReadOnlySpan<(string Name, string Value)> gcSettings)
    {
        string? hostPath = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH");
        var psi = new ProcessStartInfo(hostPath is { Length: > 0 } && File.Exists(hostPath) ? hostPath : "dotnet")
        {
            UseShellExecute        = false,
            RedirectStandardOutput = true,
            RedirectStandardError  = true,
        };
        psi.ArgumentList.Add("exec");
        psi.ArgumentList.Add(typeof(MemoryBudgetTests).Assembly.Location);
        psi.ArgumentList.Add(ChildProcessEntry.MemoryBudgetsCommand);

        // Nothing inherited may pre-empt the settings under test.
        foreach (string prefix in (string[])["DOTNET_", "COMPlus_"])
            foreach (string name in (string[])["GCHeapHardLimit", "GCHeapHardLimitPercent", "GCTotalPhysicalMemory",
                                               "GCHeapHardLimitSOH", "GCHeapHardLimitLOH", "GCHeapHardLimitPOH",
                                               "GCHeapHardLimitSOHPercent", "GCHeapHardLimitLOHPercent", "GCHeapHardLimitPOHPercent",
                                               "GCHighMemPercent"])
                psi.Environment.Remove(prefix + name);
        foreach (var (name, value) in gcSettings)
            psi.Environment[name] = value;

        using var proc = Process.Start(psi)!;
        var stderr = proc.StandardError.ReadToEndAsync();
        string stdout = proc.StandardOutput.ReadToEnd();
        Assert.True(proc.WaitForExit(60_000), "child process did not exit");
        Assert.True(proc.ExitCode == 0, $"child exited {proc.ExitCode}: {stderr.Result}");

        var values = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (string line in stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            int eq = line.IndexOf('=');
            if (eq > 0 && long.TryParse(line.AsSpan(eq + 1), out long v)) values[line[..eq]] = v;
        }

        return new ChildBudgets(
            values["managedLimit"], values["physicalLimit"],
            values["managedBuild"], values["nativeTier"], values["indexCache"]);
    }
}
