# Log flush + merge path — CPU / allocation reconnaissance

Worktree `AMeto-perf-logs` @ 677b49f (perf/logs-cpu-alloc). Read-only. Paths relative to `src/`.

## Flush pipeline map (one 64 MB tier, ~200k events)

| # | Phase | Entry | Threading |
|---|-------|-------|-----------|
| 1 | Swap: open successor WAL, `Freeze()`, publish frozen tier, install new `WriteState` | `Ameto.Storage/StorageEngine.cs:948-1006` (`TryFlushAsync`), `HotTierSegment.cs:356` | serial under `_flushLock`; `_flushSlots` back-pressure |
| 2 | Old WAL `Dispose()` = msync + fsync of ≤64 MB | `StorageEngine.cs:1018`, `WriteAheadLog.cs:448-470` | flush thread, before heavy phase |
| 3 | Heavy phase gate | `StorageEngine.cs:1029` `_flushConcurrency` (width = min(cores/2, 640 MB / 1400 B·events) → 3 at 64 MB) | up to 3 tiers in parallel |
| 4 | Sort order + level split | `StorageEngine.cs:2217-2242` (`ComputeSortOrder` = `SegmentWriter.cs:235`) | 1 thread |
| 5 | Per level, **sequentially awaited**: `Task.Run` → `SegmentWriter.WriteEvents(HotTierEventSource)` | `StorageEngine.cs:2244, 2301-2330`; `SegmentWriter.cs:288-327` | 1 thread per tier (index build + LZ4 + IO all on it) |
| 5a | per event: `EnsureSink().Add()` → `SegmentIndexBuilder.Add` (header fields + streaming msgpack walk → inverted/trigram/bloom) | `SegmentWriter.cs:319`; `Ameto.Indexing/SegmentIndexBuilder.cs:84-315` | |
| 5b | per event: `StageEvent` into columnar scratch | `SegmentWriter.cs:682-736` | |
| 5c | per 64 KB block: `EmitBlock` → delta columns → `LZ4Codec.Encode(L00)` → `BinaryWriter` | `SegmentWriter.cs:751-821` | |
| 5d | per group (64 MB payload or bloom full): `SealGroup` → `Serialise()` three `byte[]` → write sections → dispose bloom | `SegmentWriter.cs:493-522` | |
| 6 | `Finalise`: directory, block index, footer, header, `Flush(flushToDisk:true)`, `File.Move` | `SegmentWriter.cs:554-636` | |
| 7 | Completion marker (fsync) | `StorageEngine.cs:2253, 2279` | |
| 8 | Publish: catalog add, unlist frozen, WAL delete, `RetireHotTier` (NativeMemory.Free when no readers) | `StorageEngine.cs:1064-1090, 1174` | |

Merge: `StorageEngine.cs:1735` plan → manifest → `_flushConcurrency` → `MergeToColdAsync` `:2063-2136` = `MergingSegmentEventSource` (k-way heap, one decompressed block per source, `MergingSegmentEventSource.cs:311-420`) → same `SegmentWriter.WriteEvents` with `SegmentCompression.High` (L06) and the same index sink, i.e. **indexes are rebuilt from scratch, events stream as bytes (no LogEvent), peak = ≤512 blocks × 64 KB + one group's accumulators**. After a merge: `ReleaseMaintenanceMemory` `:574` → gated blocking compacting gen2.

Verified still true (not re-reported): streaming msgpack walk (`SegmentIndexBuilder.cs:210-315`), varint posting codec (`SegmentBitmapCodec.cs`), bitmaps encoded straight into the buffer (`SegmentInvertedIndex.cs:358-365`, `SegmentTrigramIndex.cs:191-197`), per-block scratch reuse (`SegmentWriter.cs:650-663`, probe asserts < 8 MB/tier), no trigram ToArray, `NativeMemory.Alloc` (`HotTierSegment.cs:403`), swap/heavy split, `AggressiveGcGate` in front of every `GC.Collect` (`StorageEngine.cs:576`, `RamPressureService.cs:129`), HC sweep gone.

Measured baselines (from probe comments / engine constants): build ≈ 1.3 s for 131k events (`StorageEngine.cs:196-203`) ≈ **10 µs CPU/event**; retained ≈ 1.35 KB/event trace-carrying (147 MB accumulators + 28 MB blobs, `IndexBuildRetentionProbe`); trigram ≈ 7.6 B/posting and was 324 of 400 MB (`TrigramAccumulatorProbe`); merge ≈ 48 MB payload/s end-to-end incl. index (`MergeCompressionProbe`); index sections are 78-90 % of a merged file; L00 encodes at 1.8 GB/s, L06 at 235 MB/s.

## Findings (ranked by expected impact)

### 1. Trigram accumulator: hashed tuple dictionary + `List<int>` per bucket — dominant CPU and allocation sink. CONFIRMED
`SegmentTrigramIndex.cs:29, 42-74`. Per posting: `(char,char,char)` key → `HashCode.Combine`-style hash + probe into a 100-300k-entry dictionary (entries+buckets ≈ 8-10 MB, L2-missing) + `list[^1]` read + `List.Add` (another miss, doubling growth). ~100 postings/event (template ≈ 38, string values, digits) → 20-40M lookups per 200k flush at ~50-80 ns = **1-3 s CPU/flush (≈60 % of the build)**. Allocation: one `List<int>` object + `int[1]` per bucket (64 B × ~300k) and growth garbage ≈ final size again (~150 MB/flush, dense trigrams' arrays > 85 KB land on the LOH). Frequency: per trigram.
Fix: (a) direct-indexed table for folded ASCII trigrams: `key = c0<<14 | c1<<7 | c2` (21 bits) → `int[2M]` slot → bucket id, no hashing; non-ASCII falls back to the dictionary. (b) Replace `List<int>` with a slab arena of fixed 32-int chunks (`int[] slab` list, bucket = {head, tail, count, lastOffset}); append = write + bump, no per-bucket object, no doubling garbage, freed as one unit per group. (c) Memo repeated strings: `Dictionary<string, int[]>` template → distinct bucket ids (by reference first, ordinal fallback) so a repeated template costs one lookup + 38 appends instead of 38 hash probes. Serialiser walks buckets in first-seen order (keeps today's dictionary insertion order → byte-identical sections). Risk: none on disk; parity test compares two builds through the same class. Measure: `TrigramAccumulatorProbe` (add a CPU column), `FlushCpuProbe`.

### 2. Bloom filter re-adds low-cardinality terms once per event. CONFIRMED
`SegmentIndexBuilder.cs:153, 160, 205, 269 (`_bloom.Add(flatKey)` per property per event)`; each add = `ToLowerInvariant` copy + UTF-8 encode + 3 full Murmur passes + `% _blockCount` division + 2 random writes into a ~5 MB filter (`SegmentBloomFilter.cs:151-203, 368-405`). ~21-25 adds/event of which ~13 (10 keys, level, template, service, bool/small ints) are repeats → ~2.6M pointless adds ≈ 0.2-0.3 s/flush. Per event / per property.
Fix: the inverted index already knows first-sight — make `AddSpan` return `bool created` for the property and for the value, and only then `_bloom.Add`; memo template/service by reference. The bloom is a set, so bits are identical → sections byte-identical. Measure: `FlushCpuProbe` (add bloom-only timing), `BloomSizingProbe` unchanged.

### 3. Section serialisation makes two LOH copies per section per group. CONFIRMED
`SegmentInvertedIndex.cs:343, 369` and `SegmentTrigramIndex.cs:179, 200`: `ArrayBufferWriter(estimate)` (LOH) then `WrittenSpan.ToArray()` (second LOH copy); `SegmentBloomFilter.cs:297` `new byte[4+4+byteCount]` (≈5 MB LOH) copied from native; `SegmentWriter.cs:499-502` writes them and drops them. Per group: 2×(inverted ≈ 10-20 MB + trigram ≈ 20-30 MB) + bloom ≈ **70-110 MB of immediately-dead LOH per group**, ×(levels per tier), on a Workstation GC that only compacts LOH on the forced aggressive collect. `IndexBuildRetentionProbe` "+ serialised index blobs" and `IndexBuildAllocProbe` "serialise=" measure exactly this.
Fix: change `ISegmentIndexSink.Serialise` to `void WriteSections(Stream fs, out long invOff, out long triOff, out long bloomOff)`: write a 4-byte placeholder, stream the section (bloom straight from `new ReadOnlySpan<byte>(_bits, n)`), seek back and patch the length (FileStream is seekable; format unchanged). Interim cheaper step: keep one pooled `ArrayBufferWriter`/rented `byte[]` per writer reused across groups and return `ReadOnlyMemory<byte>` instead of `ToArray()`. Risk: test helpers use `SerialisedInvertedIndex` etc. — keep them as thin wrappers.

### 4. Inverted index: a managed string + `List<int>` + `int[4]` + dictionary entry per distinct value; dictionaries never pre-sized. CONFIRMED
`SegmentInvertedIndex.cs:64-86`: `new string(span)` (`:73, :80`), `new List<int>()` (`:79`, first Add → `int[4]`), inner `Dictionary` grows by doubling (entries arrays > 85 KB after ~3.5k entries → LOH churn ≈ 2× final). For trace/span ids + 2 high-card props: ≈ 150 B × 4 × 200k ≈ **120 MB allocated, ~70 MB retained per group**, plus the 32-char hex strings from `TraceIdHelper.cs:75-80` (`SegmentIndexBuilder.cs:190, 196`). `SegmentIndexBuilder` receives `expectedEventCount` but sizes nothing with it (`:69-75`). Per distinct (name,value).
Fix: UTF-8 term arena (`byte[]` slabs) + open-addressing table keyed by (propertyId, hash, span) with an insertion-ordered entry list (preserves today's output order); postings as in #1 with singleton inline (`first` + `chunkHead=-1`); hex ids formatted with `TryFormat` straight into the arena; pre-size per-property tables from the previous group's cardinality (carry a `Dictionary<string,int>` of last distinct counts in the sink factory). Serialise writes terms from the arena (no UTF-16→UTF-8 transcode). Risk: the value dictionary on the read side is `OrdinalIgnoreCase` and merges case collisions — only write side changes, bytes identical if insertion order is kept. Measure: `IndexBuildBreakdownProbe`, `IndexBuildRetentionProbe`.

### 5. Property walk transcodes every key and value twice and copies every payload. CONFIRMED
`SegmentIndexBuilder.cs:213-214` copies each payload into `_mp` (60 MB memcpy/flush, needed only because `MessagePackReader` wants a sequence); `:227-229` `GetCharCount`+`GetChars` per key per event (keys repeat every event); `:264-266` same for values; then bloom re-encodes the chars back to UTF-8 after folding, and the trigram lowers them again. ≈ 6 passes over each value's bytes and ≈ 1-1.5 µs/event. Per property value.
Fix: with #4's UTF-8 tables the walk hashes raw UTF-8 spans; a small per-builder key cache keyed by raw UTF-8 key span → (key id, inner table, bloom-added) turns a key into one probe; ASCII fast path for folding (byte-wise lowering, identical output to `ToLowerInvariant` for ASCII, fallback otherwise); avoid the `_mp` copy with a `MemoryManager<byte>` over the native chunk (flush) / `new ReadOnlySequence<byte>(block, off, len)` (merge). Risk: folding parity for non-ASCII must keep the UTF-16 path.

### 6. Merge path decodes the whole exception — stack trace included — per event for the index. CONFIRMED
`SegmentIndexBuilder.cs:166` → `SegmentEventSource.cs:82` → `ExceptionInfo.cs:157-162` (`bytes.ToArray()`, then `ReadString()` for `type`, `msg`, **`stk`**, `inner` at `:85-90`). The index uses only type, message and inner type. An Error-level merge is 100 % exceptions (level-pure segments): 1-5 KB of gen0 strings per event, i.e. GBs of garbage on a 4M-event merge, and the CPU of decoding UTF-16 stack traces nobody reads. Per exception-carrying event on merges (flush path already holds the object). `MergeExceptionColumnProbe` measures with `_allowIndexlessMerge = true` — i.e. **without** a sink — so this is currently unmeasured.
Fix: `ExceptionInfo.ReadIndexFields(ReadOnlySpan<byte> payload, ref scratch, out typeUtf8, out msgUtf8, out innerTypeUtf8)` using `TryReadStringSpan` and `Skip()` for `stk`, over the builder's `_mp` scratch; feed spans to `AddSpan`/trigram. Measure: run the probe with the real sink factory and compare B/event.

### 7. `ComputeSortOrder`: delegate-comparison introsort over random header reads, plus per-level list growth and copies. CONFIRMED
`SegmentWriter.cs:235-248`: `Array.Sort(int[], Comparison)` with a closure; each comparison does two `GetHeader` (div/mod + `_chunkArenas` load + 64 B header in a 12.5 MB header set) → ~3.5M comparisons ≈ 50-100 ms/flush. `StorageEngine.cs:2219-2242`: `List<int>` per level doubling (LOH for the dominant level) + `idx.ToArray()` — ~3 × 800 KB LOH per flush. Per flush.
Fix: one sequential pass extracting `(long ts, ulong id, int idx)` into a pooled struct array; detect already-sorted (ingest is nearly time-ordered) → identity; else `MemoryExtensions.Sort` with a struct comparer or LSD radix on ticks; count per level first, then fill exact-size pooled arrays. Risk: none.

### 8. Uncontended locks on single-threaded builders. CONFIRMED
`SegmentInvertedIndex.cs:67` and `SegmentTrigramIndex.cs:53` take a lock per Add — ~3-5M lock/unlock per flush (~60-100 ms). The builder is per group, single-threaded (`SegmentWriter.cs:319`); the build-mode `Lookup` is test-only. Fix: drop the locks, document the contract. Trivial.

### 9. Bloom hashing: three independent Murmur passes and a division per add. CONFIRMED
`SegmentBloomFilter.cs:155-159, 368-374`: `Hash3` hashes the bytes three times; `h0 % _blockCount` is a real division. Fix: one 64-bit hash (`XxHash3` from System.IO.Hashing or one Murmur) with h1/h2 derived (Kirsch-Mitzenmacher) and Lemire multiply-shift block selection. **Format change**: bit positions differ → flag it in a spare bit of the capacity word next to `FoldedMarker` (`:37`), reader keeps old hashing for old blobs. Together with #2, ~0.5 µs/event. Measure: `FlushCpuProbe`.

### 10. Writer per-event small change. CONFIRMED
`SegmentWriter.cs:296-297` `GetByteCount` per event for an interned template; `:738-748` `ArrayPool` rent/return + second encode per string ×2 per event; `:830-870` six tiny `MemoryStream.Write` calls per event; `:723` `ev.Exception.ToBytes()` allocates per exception event on the flush path; `SegmentEventSource.cs:232-233` two `ConcurrentDictionary` lookups per event (`StringInternPool.cs:89-93`, the template one only on a tier miss). Total ≈ 0.3-0.5 µs/event and ~0.4M pool ops per flush.
Fix: encode UTF-8 straight into the stream's `GetBuffer()` after `SetLength`; delta-encode into `long[]`/`uint[]` scratch and write once with `MemoryMarshal.AsBytes` (guard `BitConverter.IsLittleEndian`); write the exception with a `MessagePackWriter` over an `IBufferWriter` adapter of `_excBytes`; memo last service pool index → string.

### 11. `MessagePackReader` token overhead in the walk. SUSPECTED
`SegmentIndexBuilder.cs:215-315`: ~25 tokens/event through `SequenceReader`-based `MessagePackReader` (`TryReadStringSpan`, `NextMessagePackType`, `ReadInt64` …) ≈ 15-30 ns each → 0.5-1 µs/event. A hand-rolled span tokenizer (map/array/str/int/float/bool/nil/bin/ext-skip) is 3-5× cheaper and removes the `_mp` copy. Do after #4/#5.

### 12. K4os `LZ4Codec.Encode` allocates and zeroes a native context per call. SUSPECTED
`SegmentWriter.cs:788` (K4os.Compression.LZ4 1.3.8): `LL.LZ4_compress_fast` allocates+zeroes ≈16 KB per block (flush: ~1000 blocks → negligible), `LZ4_compress_HC` ≈ 256 KB per block (merge: 512 MB / 64 KB = 8k blocks → ~2 GB memset ≈ 10 % of HC time). Verify by decompiling `LL.LZ4_compress_fast`/`LZ4_compress_HC`. The only public context-reusing API is the chained encoder, which changes the block format — vendoring the HC entry with a reusable context is the fix, low priority.

### 13. Per-tier flush is one thread; levels serialised. CONFIRMED (wall-clock, not CPU)
`StorageEngine.cs:2244` awaits each level's `Task.Run` in turn; index build, LZ4 and IO share one thread. Option: index the **emitted block** (all columns are in `_blk`) on a second thread via a bounded channel, so 5a overlaps 5b/5c. Costs no CPU, halves wall time; only worth it after 1-5 shrink the build.

### 14. Merge-specific costs are structurally fine; the build cost applies at ~3× write amplification. CONFIRMED
`MergingSegmentEventSource.cs:112-141` re-parses every column offset per row and `:187-210` hashes template+service bytes per event (`HashCode.AddBytes`) — ~0.2 µs/event, acceptable; heap `Less` `:399-404` reads two properties per compare — fine for ≤512 sources. Peak: ≤512 × 64 KB blocks + one group ≈ 190 MB (`StreamingMergeMemoryProbe`). No `LogEvent` materialisation; indexes rebuilt from scratch (inherent to re-grouping). So findings 1-6, 8, 9 are the merge findings too, weighted by `DayBucketCompactionProbe`'s ~3.6 rewrites per event.

## Suggested order
1 + 8 (trigram arena, locks) → 2 (bloom dedupe) → 3 (stream sections) → 4 + 5 (UTF-8 term tables, key cache) → 6 (exception span reader, add a with-sink probe) → 7 → 10 → 9 (format-flagged) → 11/12/13.

Expected: build CPU from ~10 µs/event to ~3-4 µs; per-group allocation from ~300 MB to well under 100 MB with almost no LOH garbage; retained peak per group roughly halved (trigram 7.6 → ~4.2 B/posting, inverted strings gone).
