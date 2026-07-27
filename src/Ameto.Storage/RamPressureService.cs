using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Ameto.Core;

namespace Ameto.Storage;

/// <summary>
/// Monitors system-wide RAM utilisation and flushes the hot tier when the
/// configured <see cref="ServerOptions.RamTargetPercent"/> threshold is exceeded.
///
/// Behaviour:
/// - Polls every <see cref="_checkInterval"/> (30 s by default).
/// - Uses <see cref="GC.GetGCMemoryInfo"/> to obtain the OS-level memory load
///   without taking a P/Invoke dependency.
/// - After a flush the service backs off for <see cref="_cooldown"/> (5 min)
///   to avoid continuous thrashing when pressure is sustained.
/// </summary>
public sealed class RamPressureService : BackgroundService
{
    private readonly StorageEngine                  _storage;
    private readonly ServerOptions                  _options;
    private readonly ILogger<RamPressureService>    _logger;

    private static readonly TimeSpan _checkInterval = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan _cooldown       = TimeSpan.FromMinutes(5);

    public RamPressureService(
        StorageEngine               storage,
        ServerOptions               options,
        ILogger<RamPressureService> logger)
    {
        _storage = storage;
        _options = options;
        _logger  = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Give the process a moment to settle before we start monitoring.
        await Task.Delay(TimeSpan.FromSeconds(15), stoppingToken);

        DateTimeOffset lastFlush = DateTimeOffset.MinValue;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                int pct = GetSystemRamPercent(out bool scopedToThisProcess);

                // Memory attribution snapshot — lets us split process RSS into
                // managed heap vs. native hot tier from the journal without
                // hitting the authorized /api/diagnostics endpoint.
                var gc = GC.GetGCMemoryInfo();
                const long MB = 1024 * 1024;
                _logger.LogInformation(
                    "MEM ws={WS}MB gc_heap={Heap}MB gc_committed={Committed}MB gc_frag={Frag}MB hot_tier={Hot}MB mode={Mode} sys_ram={Pct}%",
                    Environment.WorkingSet      / MB,
                    gc.HeapSizeBytes            / MB,
                    gc.TotalCommittedBytes      / MB,
                    gc.FragmentedBytes          / MB,
                    _storage.HotTierAllocatedBytes / MB,
                    System.Runtime.GCSettings.IsServerGC ? "Server" : "Workstation",
                    pct);

                if (pct > _options.RamTargetPercent)
                {
                    // Only act if flushing would actually free something. GetSystemRamPercent
                    // falls back to HOST memory load whenever there is no cgroup limit — which
                    // is always on Windows and on any container started without --memory. A
                    // busy host therefore kept this branch permanently true, and the server
                    // would flush its tier, force a blocking compacting gen2 and empty its
                    // working set every cooldown forever, no matter how little it held: a
                    // self-inflicted CPU-and-RSS sawtooth caused entirely by OTHER processes.
                    long reclaimable = _storage.HotTierAllocatedBytes + gc.HeapSizeBytes;
                    long floor       = Math.Max(64L * 1024 * 1024, _options.HotTier.MaxSizeBytes);

                    // A tier's worth of reclaimable memory is necessary but nowhere near
                    // sufficient on a host-wide reading: this server's managed heap alone
                    // clears a 64 MB floor almost from startup, so that test passes even when
                    // the process holds a rounding error of the machine's RAM and the flush is
                    // pure waste. When the number we are reacting to describes the WHOLE host,
                    // also require that giving our memory back would move it by at least one
                    // percentage point. Under a cgroup limit the reading is genuinely about
                    // this container, so the floor alone governs.
                    long hostTotal = gc.TotalAvailableMemoryBytes;
                    long needed    = scopedToThisProcess || hostTotal <= 0
                        ? floor
                        : Math.Max(floor, hostTotal / 100);

                    if (reclaimable < needed)
                    {
                        _logger.LogDebug(
                            "RAM pressure at {Pct}% ({Scope}) but only {Reclaimable} MB is reclaimable here " +
                            "(need {Needed} MB to be worth a flush) — the pressure is not ours; skipping.",
                            pct, scopedToThisProcess ? "this container" : "whole host",
                            reclaimable / MB, needed / MB);
                    }
                    else if (DateTimeOffset.UtcNow - lastFlush >= _cooldown)
                    {
                        _logger.LogWarning(
                            "RAM pressure: system memory at {Pct}% (target {Target}%), " +
                            "{Reclaimable} MB reclaimable. Flushing hot tier to release memory.",
                            pct, _options.RamTargetPercent, reclaimable / MB);

                        await _storage.FlushHotTierAsync(stoppingToken);

                        // Ask the GC to reclaim any freed managed memory, then
                        // release the freed resident pages back to the OS
                        // (Windows working set + Linux arenas).
                        GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
                        WorkingSetTrimmer.TryTrim();

                        lastFlush = DateTimeOffset.UtcNow;
                    }
                }

                // Per-cycle upkeep: return free()'d allocator arenas to the OS so
                // the RSS doesn't drift upward between flushes. Linux-only —
                // Windows manages its own working set and is trimmed only under
                // real pressure above (emptying it every 30 s just churns pages).
                WorkingSetTrimmer.TrimAllocator();
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "RAM pressure check failed");
            }

            await Task.Delay(_checkInterval, stoppingToken);
        }
    }

    /// <summary>
    /// Returns current memory utilisation as a percentage (0–100) of the
    /// effective limit. When running under a cgroup memory limit (i.e. in a
    /// container) it reports <c>(memory.current − reclaimable page cache) / memory.max</c>,
    /// so memory-mapped cold segments — which the kernel evicts before OOM — do
    /// not inflate the reading.
    ///
    /// <para>CAVEAT: the cgroup files are Linux-only AND require a finite limit, so on
    /// Windows, on bare metal, and in any container started without <c>--memory</c> /
    /// <c>mem_limit</c>, this falls through to <see cref="GC.GetGCMemoryInfo"/> and reports
    /// the load of the WHOLE MACHINE — including every unrelated process. Callers must not
    /// treat a high reading as evidence that this process is responsible; see the
    /// reclaimable-memory guard in <see cref="ExecuteAsync"/>.</para>
    /// </summary>
    public static int GetSystemRamPercent() => GetSystemRamPercent(out _);

    /// <inheritdoc cref="GetSystemRamPercent()"/>
    /// <param name="scopedToThisProcess">
    /// <c>true</c> when the figure came from a cgroup limit and therefore describes THIS
    /// container's usage against its own ceiling — pressure this process can relieve.
    /// <c>false</c> when it fell back to whole-machine load, where a high reading may be
    /// entirely someone else's doing. Callers that take corrective action must branch on
    /// this; callers that merely display the number (diagnostics) can ignore it.
    /// </param>
    public static int GetSystemRamPercent(out bool scopedToThisProcess)
    {
        if (TryGetCgroupMemoryPercent(out int pct))
        {
            scopedToThisProcess = true;
            return pct;
        }

        // Fallback: GC's view of physical memory (host RAM when uncontained).
        scopedToThisProcess = false;
        var info  = GC.GetGCMemoryInfo();
        long total = info.TotalAvailableMemoryBytes;
        if (total <= 0) return 0;
        long used = info.MemoryLoadBytes;
        return (int)Math.Round(used * 100.0 / total);
    }

    /// <summary>
    /// Reads the cgroup (v2, then v1) memory limit and current usage, subtracting
    /// reclaimable file-backed cache so the percentage reflects pressure the
    /// engine can actually relieve (anon heap + hot tier), not mmap'd segments.
    /// Returns false when no finite limit is configured. Off the hot path (30 s poll).
    /// </summary>
    private static bool TryGetCgroupMemoryPercent(out int pct)
    {
        pct = 0;
        try
        {
            // ── cgroup v2 (unified hierarchy) ───────────────────────────────
            const string v2 = "/sys/fs/cgroup";
            if (File.Exists($"{v2}/memory.max") && File.Exists($"{v2}/memory.current"))
            {
                string maxRaw = File.ReadAllText($"{v2}/memory.max").Trim();
                if (maxRaw == "max") return false;                       // no limit set
                if (!long.TryParse(maxRaw, out long max) || max <= 0) return false;
                if (!long.TryParse(File.ReadAllText($"{v2}/memory.current").Trim(), out long cur))
                    return false;

                long cache = ReadReclaimableCache($"{v2}/memory.stat");
                pct = Percent(cur - cache, max);
                return true;
            }

            // ── cgroup v1 (legacy memory controller) ────────────────────────
            const string v1 = "/sys/fs/cgroup/memory";
            if (File.Exists($"{v1}/memory.limit_in_bytes") && File.Exists($"{v1}/memory.usage_in_bytes"))
            {
                if (!long.TryParse(File.ReadAllText($"{v1}/memory.limit_in_bytes").Trim(), out long limit)
                    || limit <= 0 || limit > (1L << 62))                 // huge sentinel == unlimited
                    return false;
                if (!long.TryParse(File.ReadAllText($"{v1}/memory.usage_in_bytes").Trim(), out long usage))
                    return false;

                long cache = ReadReclaimableCache($"{v1}/memory.stat");
                pct = Percent(usage - cache, limit);
                return true;
            }
        }
        catch { /* unreadable/inaccessible — use the GC fallback */ }
        return false;

        static int Percent(long used, long total)
        {
            if (used < 0) used = 0;
            return total <= 0 ? 0 : (int)Math.Round(used * 100.0 / total);
        }
    }

    /// <summary>
    /// Sums reclaimable file-backed page cache (active_file + inactive_file)
    /// from a cgroup <c>memory.stat</c>. Common to v1 and v2 (both expose these keys).
    /// </summary>
    private static long ReadReclaimableCache(string statPath)
    {
        long sum = 0;
        foreach (var line in File.ReadLines(statPath))
        {
            int sp = line.IndexOf(' ');
            if (sp <= 0) continue;
            ReadOnlySpan<char> key = line.AsSpan(0, sp);
            if ((key.SequenceEqual("active_file") || key.SequenceEqual("inactive_file"))
                && long.TryParse(line.AsSpan(sp + 1).Trim(), out long v))
                sum += v;
        }
        return sum;
    }
}
