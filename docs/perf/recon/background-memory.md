# AMeto logs subsystem — background CPU & memory footprint recon

Worktree `C:/Users/ruslan.akhmetov/Desktop/Processing/AMeto-perf-logs` (perf/logs-cpu-alloc), read-only. Paths below are under `src/`.

## Headline numbers at defaults (derived from code)

| Item | Where | Value |
|---|---|---|
| Hot-tier chunk | `Ameto.Storage/HotTierSegment.cs:63-71`, `Ameto.Core/LogEvent.cs:25` (header = 64 B) | 16384×64 B + 8 MB = **9 MB/chunk**, `NativeMemory.Alloc` (demand-faulted) |
| Live tier | `HotTierOptions.MaxSizeBytes` 64 MB → 8 chunks, 131 072 events | **72 MB native worst case** |
| Frozen-tier slots | `StorageEngine.cs:401-411`: `clamp(512 MB / 72 MB, width, 64)` = **7** | up to **504 MB native** in flight |
| Index build per flush | `IndexBuildBytesPerEvent` 1 400 × 131 072 | **184 MB managed** per concurrent flush; width = min(cores/2, 3) → 2 on 4 cores |
| Ingest ring | `IngestionRingBuffer.cs:104-128` | slots 65536×64 B = 4 MB zeroed (resident); slab arena 8192×64 KB = **512 MB virtual**; 2×512 KB LOH arrays |
| Index cache | `QueryOptions.IndexCacheBytes` | **256 MB** (managed postings + native bloom bits) |
| WAL | `WriteAheadLog.cs:113-126` | 64 MB mmap per live WAL; `bool[65536]` per WAL |
| Catalog | `ConcurrentDictionary<SegmentKey,SegmentInfo>` | ~250 B/segment, no open handles kept |
| GC | `Ameto.Server.csproj` | Workstation, Concurrent, RetainVM=false; `DOTNET_GCConserveMemory=5` only via Dockerfile / systemd unit / Windows installer env |

Steady-state idle (no ingest, no queries): runtime + JIT'd code (~60–90 MB self-contained) + 4 MB ring + touched hot-tier pages + whatever the index cache has accumulated (grows to 256 MB, shrinks only by LRU). Under 100k/s: 72 + up to 504 (frozen) + 2–3×184 (builds) + 256 (cache) ≈ **1.1–1.4 GB peak**. At 512 MB the runtime auto-sets the managed hard limit to 75 % = 384 MB; index cache + one build alone exceed it.

## Findings (ranked by impact)

### 1. `/api/diagnostics` walks the data directory twice and snapshots all threads, polled every 10 s — CONFIRMED
- `Ameto.Server/DiagnosticsEndpointMapper.cs:35-42,183-200`; poller `client/src/app/pages/settings/sections/dashboards-section/dashboards-section.ts:34` (`setInterval(load, 10_000)`).
- Per call: `DirStats(segments)`, `DirStats(metrics)`, `DirStats(traces)`, `DirStats(wal)` **and** `DirStats(dataRoot)` recursively (re-walks all of the above, every file stat'd twice), `FilesSize(Ameto.db*)`, `DriveInfo`, `Process.GetCurrentProcess()` + `proc.Threads.Count` (on Windows `NtQuerySystemInformation` over *all* processes — hundreds of KB allocated), `GetSegments(null,null)` (LINQ OrderByDescending + ToList over the catalog) then two more `Sum` passes.
- Scales with segment count: 5 000 segments → ~10 k `FileInfo` objects (each ~150 B + full-path string) every 10 s while the Settings dashboard is open ≈ 2–3 MB gen0 per 10 s + several ms of syscalls. This is the one *new* item that scales with N segments.
- Fix: cache the directory stats with a 30–60 s TTL (one walk of `dataRoot` classifying by subdir/extension); compute logs bytes from `segs.Sum(CompressedBytes)`; drop `proc.Threads.Count` (`ThreadPool.ThreadCount` or omit); use `Environment.WorkingSet` instead of a `Process` instance. Risk: nil (staleness ≤ TTL).
- Measure: `dotnet-counters` `gen-0-gc-count` / `alloc-rate` with the Settings page open vs closed; endpoint latency.

### 2. WAL msync + `FlushFileBuffers` every 2 s with no dirty flag — CONFIRMED
- `StorageEngine.cs:3195-3206` → `WriteAheadLog.Flush()` (`WriteAheadLog.cs:241-259`): `_accessor.Flush()` over the full 64 MB view + `_fileStream.Flush(flushToDisk:true)` unconditionally; only the `.pool` stream has a `_poolDirty` guard.
- Cost per no-op tick: kernel PTE walk of 16 384 pages (FlushViewOfFile / msync) + a device cache-flush command. When anything was appended the header page is dirty anyway. A 0.5 Hz wake plus an I/O barrier around the clock on an idle server.
- Fix: track `_flushedOffset`; in `Flush()` return immediately if `_writeOffset == _flushedOffset && !_poolDirty`. Optionally msync only `[flushedOffset, writeOffset)` + the header page instead of the whole view. Risk: none (durability window unchanged).
- Measure: procmon / `strace -f -e msync,fsync` on the pid for an idle minute; `threadpool-completed-work-item-count` idle rate.

### 3. Alert log rules re-decompress every segment in their window every 15 s — CONFIRMED (magnitude per deployment SUSPECTED)
- `Ameto.Alerts/AlertEvaluator.cs:208-233,334-352,391-403`. `EvalInterval` 15 s. Level-less rules → `StorageEngine.AggregateLogVolumeAsync` (`StorageEngine.cs:667-724`): opens **every** cold segment overlapping the window via `SegmentReader.Open` and `AggregateHeaders` (LZ4-decodes all blocks) in `Parallel.ForEach(degree ≤ 8)`; bypasses `LogVolumeCountsCache` (20 s TTL). Level-constrained rules → `IQueryExecutor.ExecuteAsync` with `Count = 1_000_000`, materialising every matching `LogEvent` just to count it.
- With a 24 h window and 500 segments/day: 500 mmaps + full decode per rule per 15 s. Zero rules ⇒ trivial (`GetAll()` is an in-memory `volatile` list, `AlertRuleStore.cs:54`).
- Fix: (a) cold segments are immutable and level-pure — memoise `(SegmentKey → per-service count)` per rule/window shape; re-aggregate only the hot tier + segments not yet memoised; (b) a segment fully inside the window contributes `EventCount` with no open at all (`SegmentInfo.MinLevel` is its only level); (c) for level rules use the header aggregator with a level mask instead of materialising events. Risk: low; key-based memo self-invalidates on merge/retention.
- Measure: `processCpuPercent` in `/api/diagnostics` with N rules enabled vs `Ameto__Alerts__Enabled=false`.

### 4. Per-query fixed overhead: whole-catalog LINQ + one mmap per segment per query — CONFIRMED
- `StorageEngine.GetSegments` (`:584-591`): `Where → OrderByDescending → ToList` over all N segments on every query; `QueryExecutor.cs:124-127` adds two more `Where` + `ToList`. Then `QueryExecutor.cs:383` and `:721` `SegmentReader.Open` per segment per query: `FileInfo` + `CreateFileMapping` + `MapViewOfFile` + block-index read (`SegmentReader.cs:72-100,146-167`), then Dispose. No reader cache (only the decoded *index* is cached).
- Each idle live tail (`EndpointMapper.cs:494-616`) re-runs this at least every `LiveTail.MaxWait` = 5 s; K open tabs ⇒ K catalog sorts + K×(segments in window) mmaps per 5 s. O(N) syscalls/allocs per query; on Windows mmap create/close is ~20–50 µs each.
- Fix: keep the catalog as a time-sorted immutable array rebuilt on publish/delete (rare) so `GetSegments` is a binary-search slice; add a small refcounted LRU of open `SegmentReader`s (same shape as `SegmentIndexCache`) so a live tail reopens nothing. Risk: on Windows a cached mapping blocks `File.Delete` by retention/merge — the merge already tolerates "still held open" via the manifest sweep, but the LRU must evict on delete.
- Measure: `dotnet-trace` sample profile with 3 live tails open and no ingest; count `MemoryMappedFile.CreateFromFile` calls/min.

### 5. Memory budgets are constants that ignore the machine — CONFIRMED (dominant knob at 512 MB)
- `StorageEngine.cs:216-232` `FlushManagedBudgetBytes` = 640 MB, `FlushNativeBudgetBytes` = 512 MB; `_flushSlots` = 7 at the default tier. Nothing consults `GC.GetGCMemoryInfo().TotalAvailableMemoryBytes` (cgroup-aware). On the 512 MB console stand the engine may legally hold 504 MB of frozen tiers plus 2×184 MB of builds plus a 256 MB index cache.
- Fix: derive both budgets and the `IndexCacheBytes` default from available memory (e.g. native ≤ 25 %, managed builds ≤ 30 %, index cache ≤ 15 %) and log the derived ceilings. For the stand: `HotTier.MaxSizeBytes = 16 MB` (2 chunks = 18 MB native, build ≈ 46 MB), `FlushConcurrency = 1`, `Query.IndexCacheBytes = 48 MB`. Note `_flushSlots` still clamps to 512 MB / 18 MB = 28 slots, so the native budget must scale too. Risk: fewer slots = earlier ring back-pressure under bursts (drops instead of OOM — the right trade at 512 MB).
- Measure: the "Flush budgets:" startup line; `hotTierNativeBytes`, `gcCommittedBytes` in `/api/diagnostics`; the 30 s `MEM ws=…` line.

### 6. Ingest slab arena: 512 MB committed on Windows, never trimmed after a burst — CONFIRMED (Windows) / SUSPECTED (Linux)
- `IngestionRingBuffer.cs:114` `NativeMemory.Alloc(512 MB)`: on Windows `HeapAlloc` of a large block is `VirtualAlloc(MEM_COMMIT)` → 512 MB commit charge / PrivateBytes immediately (not WS). On Linux/jemalloc pages fault lazily, but the LIFO free-list means a burst that touches all 8192 slabs leaves them resident forever (≥ 32 MB at one 4 KB page per slab, up to 512 MB with large events); jemalloc cannot purge a live allocation.
- Fix: reserve (`VirtualAlloc(MEM_RESERVE)` / `mmap(PROT_NONE)`) and commit on demand, or `DiscardVirtualMemory` / `madvise(MADV_DONTNEED)` slabs above a high-water mark when the ring is idle; or lower `PayloadPoolBytes` to 32–64 MB on small hosts (512–1024 slabs ≈ 5–10 ms of ingest at 100k/s). Risk: page-fault cost on the next burst.
- Measure: `processPrivateBytes` vs `processWorkingSetBytes` at startup; WS after a synthetic burst (`tools/loadtest`).

### 7. Hot tier allocates two 128 KB LOH arrays per chunk per tier — CONFIRMED
- `HotTierSegment.cs:265-273`: `string?[16384]` and `ExceptionInfo?[16384]` (128 KB each, above the 85 KB LOH threshold) lazily per chunk; dropped when the tier retires. Up to 16 LOH allocations (2 MB) per 64 MB tier every ≤ 5 min, plus the recovery path. LOH is only collected in gen2, so this is a slow gen2 driver and a fragmentation source under `GCConserveMemory`.
- Fix: rent from a dedicated pool and clear on return, or drop the per-event string copy (templates are interned — `MessageTemplatePoolIndex` already exists; the copy exists only for pool-miss robustness) and keep exceptions in a sparse dictionary. Risk: low.
- Measure: `dotnet-counters` `loh-size`, `gen-2-gc-count` over an hour of ingest.

### 8. `RamPressureService` tick — SUSPECTED exception-per-tick on Alpine
- `RamPressureService.cs:43-155`: every 30 s `GC.GetGCMemoryInfo()` ×2, three cgroup file reads + `memory.stat` parse, one Information log line (console + file, `AutoFlush=true` → a write syscall), then `WorkingSetTrimmer.TrimAllocator()` → `DllImport("libc") malloc_trim`. The Docker image is Alpine (musl) with jemalloc preloaded: neither exports `malloc_trim`, so this is an `EntryPointNotFoundException` thrown and swallowed every 30 s (`WorkingSetTrimmer.cs:52-59`). Cheap in absolute terms, but a throw + stack capture every tick.
- Fix: probe once with `NativeLibrary.TryGetExport` and cache a flag; on jemalloc use `mallctl("arena.<i>.purge")` or nothing (background_thread already purges). Risk: none.
- Measure: `dotnet-counters` `exception-count` on an idle Alpine container (expect 2/min).

### 9. `PeerProber` runs every 10 s with replication on by default and no peers — CONFIRMED (low)
- `Ameto.Server/config.yml` `Replication.Enabled: true`, `SeedNodes: []`; `PeerProber.cs:64-70` `PeriodicTimer(10 s)` → `ProbeAllAsync` builds LINQ chains + a `PeerPayload` and awaits `Task.WhenAll(empty)`. Also registers two `HttpClientFactory` clients. Fix: skip the timer when `SeedNodes` is empty and the registry holds only the local node (start lazily on first `Upsert`), or default `Enabled: false`. Measure: visible in any idle `dotnet-trace` capture.

### 10. Cold-maintenance planner allocates O(N) per tick — CONFIRMED (low)
- `StorageEngine.cs:524-562,1488-1560`: every 600 s idle (15 s after a merge) `RecoverInterruptedMerges` enumerates `*.mergemanifest`, then `SelectMergeBatch` builds a `Dictionary<(level,bucket), List<SegmentInfo>>` over the whole catalog plus a key list. Fine at 600 s; only matters if the cadence is shortened. Fix (if touched): skip when the catalog count and a publish/delete generation counter are unchanged.

### 11. GC / runtime configuration — CONFIRMED, recommendations
- Set: Workstation + Concurrent, `RetainVM=false`; `DOTNET_GCConserveMemory=5` via `install/docker/Dockerfile`, `install/linux/install.sh:209`, `install/windows/ameto.iss:424` (**not** when run from a console); jemalloc + `background_thread:true,dirty_decay_ms:5000` on Docker (a purge thread wakes on a 5 s decay — acceptable). Not set: `ReadyToRun`, `GCgen0size`, `GCHeapHardLimit`, `ThreadPool.MinThreads`, `AppContext` switches; `TieredPGO` is the default on. Publishes are self-contained single-file (`.github/workflows/release.yml:128-139`, `build-installer.ps1:102`), so all code is JIT'd at startup.
- Trade-offs: `ConserveMemory=5` costs extra gen2 compactions when fragmentation crosses the threshold — keep at 512 MB, consider 0–3 on the 100k/s box. `GCHeapHardLimitPercent=60` on the stand makes the managed limit explicit and leaves room for native tiers (the default 75 % leaves 128 MB for everything native). `GCgen0size=64 MB` on the big box cuts gen0 count under 100k/s at +64 MB RSS — not on 512 MB. `PublishReadyToRun=true` removes the startup JIT burst (~1–2 s CPU) and JIT scratch; TieredPGO still re-JITs hot methods once. Server GC / DATAS is not applicable (workstation is right for 512 MB).
- Measure: `dotnet-counters monitor System.Runtime` (`gen-0/1/2-gc-count`, `time-in-gc`, `gc-committed`, `loh-size`, `alloc-rate`) for an idle minute and a loaded minute; `Process.TotalProcessorTime` delta over an idle minute.

### 12. `SegmentIndexCache` never shrinks and holds native bloom bits — CONFIRMED (design note)
- `Ameto.Indexing/SegmentIndexCache.cs`; entries live until LRU pressure at 256 MB. After one wide dashboard query the cache stays full forever on an otherwise idle server (the "RSS ratchets after the first big query" shape). Fix: idle-age eviction (unused 10 min) or size the budget from available memory (finding 5). Measure: `EntryCount` / `TotalBytes` exist but are not exposed — add to `/api/diagnostics`.

### 13. `StringInternPool` and WAL side tables — CONFIRMED bounded, low
- `StringInternPool.cs`: two `ConcurrentDictionary`s, cap 65 536, no eviction — worst case a few MB; interpolated templates saturate it and then every event carries its own string (warned once). `WriteAheadLog.cs:97` `bool[65536]` per WAL (64 KB, sub-LOH) per rotation. No action beyond the existing warning.

### 14. Self-logging — CONFIRMED not on the hot path
- `FileLogger.cs`: formatting on the caller thread only when `IsEnabled`, then a bounded `BlockingCollection` drained by one task (`AutoFlush=true` → one write per line). Default file level Information; Debug lines on merge/query paths are filtered before formatting. Console provider also enabled (container stdout). Nothing periodic except the 30 s MEM line.

## Every periodic loop found

| Loop | file:line | Cadence | No-op tick cost |
|---|---|---|---|
| Age flush | `StorageEngine.cs:916` | `HotTier.MaxAge` 5 min | `Count==0` early return; ~0 |
| WAL msync | `StorageEngine.cs:3195` | `WalFlushInterval` 2 s | full-view msync + `FlushFileBuffers` (finding 2) |
| Cold maintenance | `StorageEngine.cs:524-561` | 600 s idle / 15 s after a merge; starts after catalog load + 3 min | manifest dir enum + O(N) planner allocs |
| Flush retry | `StorageEngine.cs:1100-1145` | 15 s, only after a failed flush | n/a |
| Retention | `StorageServiceExtensions.cs:35-68` | 1 h (first at 1 min) | LINQ over catalog + metrics/traces prune |
| RAM pressure + CPU sample | `RamPressureService.cs:43-148` | 30 s (first at 15 s) | 2×GCMemoryInfo, cgroup reads, log line, malloc_trim (finding 8) |
| Ingest drainer idle wait | `IngestionDrainer.cs:157` | `SemaphoreSlim.WaitAsync(1000)` → 1 wake/s | timer + TaskNode alloc per second; 1–5 ms `Task.Delay` only while parked on back-pressure (`:148`) |
| Alert evaluator | `AlertEvaluator.cs:206-233` | 15 s | 0 rules: in-memory list read; per log rule: finding 3 |
| Peer prober | `PeerProber.cs:64` | `ProbeInterval` 10 s | LINQ + payload alloc, no I/O without peers |
| Update checker | `UpdateChecker.cs:91-95` | 15 s then 60 min | one conditional HTTPS GET |
| Live tail (per open tab) | `EndpointMapper.cs:494-616` | event-driven; keepalive + re-poll every `MaxWait` 5 s; floor `MinInterval` 100 ms | full query (finding 4) + SSE frame |
| File logger drain | `FileLogger.cs:56` | event-driven | — |
| Cert reload | `HotReloadCertificate.cs:34` | FileSystemWatcher, TLS only | — |
| Metrics flush check / rollup | `MetricStorageEngine.cs:776` / `:1095` | 60 s / 5 min | out of scope; rollup may trigger `AggressiveGcGate` |
| Trace span drainer / backfill / retention | `SpanDrainer.cs:21`, `TracingServiceExtensions.cs:106-112,206` | 30 s / 5 s while working, 5 min quiet, 20 s start / 1 h | out of scope |
| Client pollers | `dashboards-section.ts:34`, `signals.ts:104`, `metrics.ts:155`, `auth.service.ts:38` | 10 s `/api/diagnostics`, 15 s, 30 s, 60 s | server-side cost per finding 1 |

Not periodic and confirmed cheap per request: `ApiKeyCache` (immutable dictionary + SHA-256, no DB hit), `SseTicketStore` (prunes only past 256 entries), `RetentionStore.Get()` (in-memory), `QueryGuard` (semaphore), `LiveEventSignal.Signal()` per event (one `Interlocked.Increment`, a TCS only per wake-up), `ApiKeyAuthenticationHandler` (cache lookup). `[ThreadStatic]` scratch: `LogEventSerializer` (2 `ArrayBufferWriter`s) and `WriteAheadLog._tExc` — grow-only per thread but small (payload-sized). `SegmentIndexBuilder._mp` grows to the largest payload seen and lives only for one index group.

## Suggested order for the implementation agent
1 (diagnostics walk) → 2 (WAL dirty flag) → 5 (memory-derived budgets + stand config) → 4 (sorted catalog + reader LRU) → 3 (alert count memo) → 7 (chunk side-array pooling) → 6 (slab commit-on-demand) → 8/9 (tick hygiene) → 11 (runtime config).
