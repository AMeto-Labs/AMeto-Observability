using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using K4os.Compression.LZ4;
using MessagePack;

namespace Ameto.Tracing.Storage;

/// <summary>
/// Reads span records from <c>.trc</c> files written by <see cref="SpanWriter"/> —
/// both the current v3 format (positional arrays, block-local delta timestamps,
/// inline attributes) and the legacy v2 format (string-keyed maps, absolute
/// timestamps, nested attribute blobs).
/// </summary>
internal static class SpanReader
{
    private const uint Magic       = 0x52_44_54_43; // "RDTC"
    private const uint FooterMagic = 0x52_44_54_46; // "RDTF"

    /// <summary>
    /// Anti-corruption bound on ONE block's compressed and decompressed size. Its only job is
    /// to stop a garbage length prefix from being handed straight to <c>ArrayPool.Rent</c> —
    /// which is its own way to run a server out of memory — so it is set where nothing real
    /// can reach it, not where a typical block sits.
    ///
    /// <para>Deliberately NOT <see cref="ReadAll"/>'s 10 MB. A block is
    /// <see cref="V3BlockSpans"/> = 4096 spans and nothing truncates attribute values on
    /// ingest, so a service that puts a 4 KB SQL statement on every span writes blocks past
    /// 10 MB legitimately. At 64 MB the bound is 16 KB per span — corruption, not data.</para>
    ///
    /// <para>WHAT THE TWO BOUNDS TOGETHER MEAN, said plainly, because "compaction already
    /// refuses those files" is not a justification — it is the problem. <see cref="ReadAll"/>
    /// refuses a &gt;10 MB block by THROWING <see cref="InvalidDataException"/> mid-merge, and
    /// its only caller is <c>TraceStorageEngine.CompactOnePass</c>, which logs the failure and
    /// leaves the file alone. So a segment with one fat block is a segment every compaction pass
    /// keeps failing on, for ever: it never merges, never migrates, and never shrinks. Search
    /// deliberately keeps it QUERYABLE anyway — a file that will not merge is a bad day, a file
    /// that cannot be read is a data-loss report — but the two bounds are not a designed pair,
    /// they are a known asymmetry. <c>SpanSearchFatBlockTests</c> pins both halves so neither can
    /// drift without the other being noticed.</para>
    /// </summary>
    /// <summary>What COMPACTION will buffer for one block — tighter than a search, because a merge
    /// rewrites the whole file rather than streaming past it.</summary>
    private const int MergeBlockBytes = 10_000_000;

    private const int MaxBlockBytes = 64 * 1024 * 1024;

    /// <summary>
    /// Spans per block, as <c>SpanWriter.BlockSize</c> writes them: every block but the last
    /// holds exactly this many. <see cref="ReadTraceAsync"/> uses it to turn a global span
    /// offset into (block, index) arithmetic — and CHECKS the result rather than trusting it,
    /// so a file written to any other geometry still resolves, just more slowly.
    /// </summary>
    private const int V3BlockSpans = 4096;

    /// <summary>
    /// Ceiling on a LIST CAPACITY taken from a count the file supplied, where the exact bound is
    /// known but large — <see cref="ReadAll"/>'s <c>Math.Min(spanCount, 100_000)</c>, applied to
    /// the two other places a count reaches a constructor. It bounds only the PRE-ALLOCATION: the
    /// list still grows if the file honestly holds more, so an honest file is read out in full and
    /// pays one or two doublings for it.
    /// </summary>
    private const int MaxListPrealloc = 100_000;

    /// <summary>
    /// How far past a segment file's own last-write time its header is allowed to claim its
    /// newest span sits before <see cref="ReadSegmentInfo"/> stops believing it. Generous on
    /// purpose: producers backdate and clocks skew, and the only thing this has to catch is a
    /// value no clock produced at all.
    /// </summary>

    /// <summary>
    /// Test seam: called once per block <see cref="ReadTraceAsync"/> actually DECODES, with that
    /// block still being decoded — inside the method that decodes it, before its frame is gone.
    /// A trace read holds no
    /// reference the caller can
    /// suspend on — the walk runs to completion before the first span is yielded — so this is the
    /// only place a test can sample the live set WHILE the walk is in it, which is the whole
    /// difference between a reader that costs a block and one that costs a segment. Never
    /// assigned in production; null-checked on a path that already does file I/O.
    ///
    /// <para>Process-wide, and its one consumer samples <c>GC.GetTotalMemory</c> — so it is only
    /// sound because <c>Ameto.Storage.Tests</c> disables class parallelisation (see that project's
    /// AssemblyInfo, which does it for exactly this hazard). A second suite sampling the heap
    /// beside it would be reading someone else's allocations.</para>
    /// </summary>
    internal static Action? _afterTraceBlockForTest;

    // ── Segment info ───────────────────────────────────────────────────────────

    public static SpanSegmentInfo ReadSegmentInfo(string filePath)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        uint magic = br.ReadUInt32();
        if (magic != Magic) throw new InvalidDataException($"Invalid .trc magic in {filePath}");

        ushort version = br.ReadUInt16();
        if (version is not (2 or 3)) throw new InvalidDataException($"Unsupported .trc version {version} in {filePath}");
        uint spanCount = br.ReadUInt32();
        long minNano   = br.ReadInt64();
        long maxNano   = br.ReadInt64();

        var (_, svcIdxOffset, _) = ReadFooter(fs, br, version);
        var services = ReadServicesFromIndex(fs, br, svcIdxOffset);

        // THE RANGE IS READ RAW, AND THAT IS A DECISION.
        //
        // An earlier version of this branch "repaired" a header whose range looked impossible,
        // clamping Max down to the file's own mtime plus a day of clock slack. It hid readable
        // data. These two fields decide which segments a walk OPENS, so a value invented at load
        // time can only ever close a door the spans are behind — and the walk skips on
        // `seg.MaxStartNano < fromNano` with no fault, no floor and no region, which is a clean,
        // complete, EMPTY answer over a file that reads perfectly. Measured both ways: 20 spans
        // written an hour ago with their mtime set five days back (rsync -at, tar -xp, cp -p, a
        // restored snapshot) returned 0 rows for every range under a week; 20 spans from a
        // producer clocked 25 h ahead returned 20 rows before a restart and 0 after it.
        //
        // A torn header is still a real hazard — it is what makes a file immortal against
        // retention, and what handed `long.MaxValue` to the vanished-region memory and poisoned
        // every window on the install. That hazard is bounded where the damage is done:
        // `VanishedRegionLog.Record` clamps what it accepts. Bounding it HERE, where the value
        // decides what gets read, trades a loud fault for a silent gap.
        return new SpanSegmentInfo
        {
            FilePath       = filePath,
            MinStartNano   = minNano,
            MaxStartNano   = maxNano,
            SpanCount      = (int)spanCount,
            Services       = services,
            FormatVersion  = version,
            // Captured HERE because this is the last moment it exists: once a segment vanishes,
            // its file and its write time are gone together. Read by HeaderRangeSuspect below to
            // tell a header claiming a time later than its own file from an honest one.
            //
            // NOT a ceiling for VanishedRegionLog any more — that took the mtime for three
            // versions and every one of them clamped a recorded region off real loss in some
            // ordinary case. Record bounds by the clock alone now, and the reasoning is in its
            // body rather than here.
            LastWriteNano  = LastWriteNanos(filePath),
            // OBSERVED, never acted on. The range is believed as written because it decides what
            // gets opened; this only lets the engine name the file for an operator.
            //
            // THE THIRD TEST IS THE ONE THAT MATTERS. Negative and inverted were the easy two, and
            // between them they miss the only tear this whole change is about: a maxNano torn to
            // long.MaxValue is neither. Measured before it was added — a segment written an hour
            // ago with the eight bytes at offset 18 overwritten came back
            // max=9223372036854775807 against a write time of 1788324033349000000, a claim 86 054
            // days later than the file itself, and the flag was FALSE. The warning that exists to
            // name the volume never fired for the fault that motivated it.
            //
            // A file is written after the spans in it arrived, so its own last-write time is an
            // upper bound on what it can honestly claim. Compared with slack, because a producer
            // whose clock runs ahead is ordinary and is not what this is looking for.
            HeaderRangeSuspect = minNano < 0
                              || maxNano < minNano
                              || maxNano > LastWriteNanos(filePath) + SuspectFutureSlackNanos,
        };
    }

    /// <summary>
    /// How far past a file's own write time a header may claim before it is called suspect. A day
    /// absorbs an exporter clocked ahead and a host that drifted; it is nowhere near the 86 054
    /// days a torn field produces.
    /// </summary>
    private const long SuspectFutureSlackNanos = 24L * 3600 * 1_000_000_000;

    /// <summary>
    /// The file's last-write time in Unix nanoseconds, never later than now, falling back to now
    /// when it cannot be read. A segment is written after the spans in it arrived, so this is a
    /// real upper bound on what it could have held — used ONLY as the ceiling for the
    /// vanished-region record, never to correct the header range a walk decides on.
    /// </summary>
    private static long LastWriteNanos(string filePath)
    {
        long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        long ms    = nowMs;
        try
        {
            long writeMs = new DateTimeOffset(File.GetLastWriteTimeUtc(filePath)).ToUnixTimeMilliseconds();
            if (writeMs < ms) ms = writeMs;
        }
        catch { /* best effort — now is already a sound ceiling */ }

        // Unix ms → nanos overflows Int64 past the year 2262; an absurd mtime lands here rather
        // than wrapping into a ceiling that would admit anything.
        if (ms < 0 || ms > long.MaxValue / 1_000_000L) ms = nowMs;
        return ms * 1_000_000L;
    }

    // ── Trace lookup ───────────────────────────────────────────────────────────

    /// <summary>
    /// The spans of ONE trace, in file order (which is start-time order — the writer sorts).
    ///
    /// <para>COSTS THE TRACE, NOT THE FILE. This used to open with
    /// <c>ReadSpansFromFile(filePath)</c> — every span of the segment materialised into a
    /// <c>List</c> — purely so the trace index's GLOBAL SPAN OFFSETS could be used as list
    /// indices. Measured on the fixture in <c>SpanTraceReadBoundTests</c>: opening a ONE-SPAN
    /// trace out of a 50 000-span segment allocated the whole segment, and a compacted segment
    /// (<c>MaxSpansPerPass</c> = 200 000) is four times that — so the trace-detail page and the
    /// flamegraph carried exactly the OOM the span search had just had removed. The stream
    /// returned the row and opening the row killed the box.</para>
    ///
    /// <para>WHY IT IS NOT A PLAIN SEEK. The offsets are indices into the file's span sequence,
    /// not byte positions, and a v3 block's timestamps are a DELTA CHAIN off the block's first
    /// span — so the spans before the wanted one inside its block still have to be walked. They
    /// are walked, not BUILT: <see cref="SkipSpanV3"/> reads the Δ that keeps the chain honest
    /// and steps over everything else, so a skipped span costs no strings, no dictionary and no
    /// record. Blocks are independent (the chain restarts), so blocks holding none of the wanted
    /// offsets are seeked past without an LZ4 touch at all.</para>
    ///
    /// <para>THE PEAK OF ONE CALL is one block plus the trace, and a block is
    /// <see cref="V3BlockSpans"/> spans.</para>
    ///
    /// <para>THE PEAK OF ONE LOOKUP IS NOT, and the factor has to be named or the sentence above
    /// is read as a bound on the request. A trace id carries no time bounds, so
    /// <c>TraceStorageEngine.GetTraceAsync</c> must consult EVERY cold segment and fans this
    /// method out across a <c>SemaphoreSlim(Clamp(ProcessorCount / 2, 2, 8))</c> — up to EIGHT of
    /// these walks decoding a block each at the same instant. So the honest figure is up to eight
    /// blocks plus the trace: still O(1) in segment size, still bounded by a constant this file
    /// controls, and still the difference between that and eight copies of the whole-file read
    /// this replaced (measured at 164.97 MB per call on a 100 000-span segment).</para>
    /// </summary>
    public static async IAsyncEnumerable<SpanRecord> ReadTraceAsync(
        string    filePath,
        TraceId   traceId,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var offsets = ReadTraceOffsets(filePath, traceId);
        if (offsets.Count == 0) yield break;

        // Ascending, which is also file order: the writer appends each offset as it writes the
        // span. Sorted anyway — the walk consumes them in one pass and an index written in any
        // other order would silently lose spans rather than merely cost a seek.
        offsets.Sort();

        // The geometry pass first — it skips whole blocks. It returns null the moment anything
        // disagrees with SpanWriter's fixed block size (a span at the computed position carrying
        // the wrong trace id, or an offset it could not place), and then the exact walk, which
        // counts every block's spans instead of assuming them, is authoritative.
        var spans = WalkTraceOffsets(filePath, traceId, offsets, useBlockGeometry: true, ct)
                 ?? WalkTraceOffsets(filePath, traceId, offsets, useBlockGeometry: false, ct)
                 ?? [];

        foreach (var s in spans)
        {
            ct.ThrowIfCancellationRequested();
            yield return s;
        }
    }

    /// <summary>
    /// Materialises the spans at <paramref name="offsets"/> and nothing else.
    ///
    /// <para>Two modes over one walk. With <paramref name="useBlockGeometry"/> the block a global
    /// offset lives in is <c>offset / V3BlockSpans</c>, so blocks with nothing wanted in them are
    /// seeked past unread — and every span it does build is checked against
    /// <paramref name="traceId"/>, so the assumption cannot silently return another trace's
    /// spans. Without it, each block's span count is read from its own array header and carried
    /// forward, which needs the block decompressed but still decodes no span the trace does not
    /// own. v2 files always take the second mode: nothing fixes their block size.</para>
    /// </summary>
    /// <returns>
    /// The spans found, or NULL in geometry mode when the geometry was disproved — which is the
    /// caller's signal to redo the walk exactly. Never null when
    /// <paramref name="useBlockGeometry"/> is false.
    /// </returns>
    private static List<SpanRecord>? WalkTraceOffsets(
        string filePath, TraceId traceId, List<uint> offsets, bool useBlockGeometry,
        CancellationToken ct)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        br.ReadUInt32();                 // magic
        ushort version = br.ReadUInt16();
        br.ReadUInt32();                 // spanCount — deliberately unused: nothing is sized by it
        br.ReadInt64();                  // minNano
        br.ReadInt64();                  // maxNano
        br.ReadByte();                   // flags

        // v2 blocks carry no promise about their size, so the geometry pass has nothing to stand
        // on. Refusing it here rather than discovering it span by span keeps the fallback free.
        if (useBlockGeometry && version < 3) return null;

        var (traceIdxOffset, _, _) = ReadFooter(fs, br, version);
        fs.Seek(27, SeekOrigin.Begin);   // reset to after header

        // Capacity, not a limit: the list still grows past MaxListPrealloc if the trace really is
        // that big. `offsets` is now bounded by the index block it came from (MaxBlockBytes / 4 =
        // 16 777 216 entries, so 134 MB of empty references), which is a bound but not a small
        // one — and a trace with more spans than ReadAll's own 100 000 in ONE segment is not a
        // trace, it is a count that has been believed too far.
        var  result     = new List<SpanRecord>(Math.Min(offsets.Count, MaxListPrealloc));
        int  want       = 0;             // next index into `offsets`
        long blockFirst = 0;             // global offset of this block's first span
        uint blockIdx   = 0;

        while (fs.Position < traceIdxOffset && want < offsets.Count)
        {
            ct.ThrowIfCancellationRequested();

            uint uncompSize = br.ReadUInt32();
            uint compSize   = br.ReadUInt32();

            if (useBlockGeometry)
            {
                blockFirst = (long)blockIdx * V3BlockSpans;
                if (offsets[want] >= blockFirst + V3BlockSpans)
                {
                    fs.Seek(compSize, SeekOrigin.Current);   // nothing wanted here — pure seek
                    blockIdx++;
                    continue;
                }
                if (offsets[want] < blockFirst) return null; // an offset no block can hold
            }

            // AGAINST THE FILE, NOT AGAINST A CONSTANT. MaxBlockBytes stopped a garbage prefix from
            // asking for gigabytes; it did nothing about a 3.5 KB file asking for 64 MB, which one
            // flipped byte inside a real compSize produces. This shape sits on the trace lookup, the
            // detail view and the SSE list, and GetTraceAsync fans it eight ways — half a gigabyte
            // from one click on the 512 MB box this branch exists to keep alive. The bytes actually
            // left in the file are the bound the file cannot forge; the uncompressed size still
            // needs the constant, because nothing on disk bounds what a payload decompresses to.
            FileBounds.RequireLengthFits(compSize, fs.Length - fs.Position, $"Block {blockIdx}", filePath);
            FileBounds.RequireLengthFits(uncompSize, MaxBlockBytes, $"Block {blockIdx}" + " uncompressed", filePath);

            byte[]  comp   = ArrayPool<byte>.Shared.Rent((int)compSize);
            byte[]? rawBuf = null;
            int     spansInBlock;
            try
            {
                fs.ReadExactly(comp, 0, (int)compSize);
                int rawLen = LZ4Pickler.UnpickledSize(comp.AsSpan(0, (int)compSize));
                if (rawLen > MaxBlockBytes)
                    throw new InvalidDataException(
                        $"Block {blockIdx} decompresses to {rawLen} bytes in {filePath}");

                rawBuf = ArrayPool<byte>.Shared.Rent(rawLen);
                LZ4Pickler.Unpickle(comp.AsSpan(0, (int)compSize), rawBuf.AsSpan(0, rawLen));

                spansInBlock = PickTraceSpansFromBlock(
                    rawBuf.AsMemory(0, rawLen), version, traceId, offsets, blockFirst,
                    strict: useBlockGeometry, ref want, result);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(comp);
                if (rawBuf is not null) ArrayPool<byte>.Shared.Return(rawBuf);
            }

            if (spansInBlock < 0) return null;               // a span was not the trace's — geometry wrong
            if (!useBlockGeometry) blockFirst += spansInBlock;
            blockIdx++;
        }

        // Every offset the index promised has to have been placed. Anything less means the block
        // size this pass assumed is not the one the file was written with.
        if (useBlockGeometry && result.Count != offsets.Count) return null;
        return result;
    }

    /// <summary>
    /// Decodes one block far enough to build the spans <paramref name="offsets"/> asks for,
    /// skipping the rest. Returns the block's span count, or -1 when a wanted position held a
    /// span belonging to another trace and <paramref name="strict"/> is set.
    /// </summary>
    private static int PickTraceSpansFromBlock(
        ReadOnlyMemory<byte> raw, ushort version, TraceId traceId,
        List<uint> offsets, long blockFirst, bool strict, ref int want, List<SpanRecord> into)
    {
        var reader = new MessagePackReader(raw);
        int cnt    = reader.ReadArrayHeader();
        if (cnt > 50_000) // same per-block sanity bound ReadAll enforces
            throw new InvalidDataException($"Block contains too many spans: {cnt}");

        long prevTs = 0;
        for (int i = 0; i < cnt; i++)
        {
            // Nothing left in THIS block: stop decoding it. The block's span count came from the
            // array header, not from walking to the end, so the tail costs nothing.
            if (want >= offsets.Count || offsets[want] >= blockFirst + cnt) break;

            if (offsets[want] == blockFirst + i)
            {
                var rec = version >= 3
                    ? DeserializeSpanV3(ref reader, i == 0, ref prevTs, SpanFilter.MatchAll)
                    : DeserializeSpan(ref reader, SpanFilter.MatchAll);

                if (rec is null || !rec.TraceId.Equals(traceId))
                {
                    // The index pointed at a span this trace does not own. In the geometry pass
                    // that disproves the assumed block size; in the exact walk the index itself
                    // is wrong, and returning another trace's span would be worse than a gap.
                    if (strict) return -1;
                }
                else into.Add(rec);

                want++;
                continue;
            }

            // Consumed, never built. The Δts chain belongs to the block, so it still has to run
            // through this span; its strings, its attribute map and its record do not.
            if (version >= 3) SkipSpanV3(ref reader, i == 0, ref prevTs);
            else              SkipSpanV2(ref reader);
        }

        // SAMPLED HERE, AND NOWHERE ELSE WILL DO. This decodes the block, so anything a defective
        // version of this method materialised is alive only while this frame is. Two earlier
        // placements missed by exactly that much: after the pool return, and then inside the
        // caller after this method had already RETURNED. Both reported 0.08 MB — the same number
        // in three review rounds — because a forced full collection at either point reclaims
        // whatever this frame was holding before the probe reads the heap. Measured with the
        // defect present at the correct placement: 9.29 MB.
        _afterTraceBlockForTest?.Invoke();

        return cnt;
    }

    /// <summary>
    /// Steps the reader over one v3 span, carrying the block's Δts chain through it — the one
    /// field a skipped span still has to be read for.
    /// </summary>
    private static void SkipSpanV3(ref MessagePackReader r, bool first, ref long prevTs)
    {
        int n = r.ReadArrayHeader();
        if (n < 4) { for (int i = 0; i < n; i++) r.Skip(); return; }

        r.Skip();  // tid
        r.Skip();  // sid
        r.Skip();  // pid | nil
        prevTs = first ? r.ReadInt64() : prevTs + r.ReadInt64();
        for (int i = 4; i < n; i++) r.Skip();
    }

    /// <summary>Steps the reader over one v2 span. Absolute timestamps — nothing to carry.</summary>
    private static void SkipSpanV2(ref MessagePackReader r)
    {
        int fields = r.ReadMapHeader();
        for (int i = 0; i < fields; i++) { r.Skip(); r.Skip(); }
    }

    // ── Search with service-index block skip ───────────────────────────────────

    /// <summary>
    /// The non-attribute half of a search predicate, in the form the block decoder can apply
    /// to a span it has half-read.
    ///
    /// <para>It exists so that the decision "this span is not in the answer" can be taken
    /// BEFORE the attribute map is turned into a <c>Dictionary</c>. In the v3 positional
    /// layout the attribute map is the last field, so every scalar the predicate needs is
    /// already in hand by the time the reader reaches it: a rejected span costs one
    /// <c>Skip</c> over the map instead of a dictionary, its keys and its boxed values —
    /// which is where the bytes actually are (~1.5 KB of a ~1.7 KB span with eight ordinary
    /// OTel attributes).</para>
    ///
    /// <para>Attribute PREDICATES are not here. TraceQL evaluates those on the records this
    /// reader hands back, and the hints reach the reader only as per-block bloom skips, so a
    /// span that survives the scalars still has to carry its attributes out.</para>
    /// </summary>
    private readonly struct SpanFilter
    {
        public static readonly SpanFilter MatchAll = new(long.MinValue, long.MaxValue,
            null, null, null, null, null, null);

        private readonly long            _fromNano;
        private readonly long            _toNano;
        private readonly string?         _serviceName;
        private readonly string?         _spanName;
        private readonly SpanStatusCode? _status;
        private readonly short?          _httpStatusCode;
        private readonly long?           _minDurationNanos;
        private readonly long?           _maxDurationNanos;

        public SpanFilter(
            long fromNano, long toNano, string? serviceName, string? spanName,
            SpanStatusCode? status, short? httpStatusCode,
            long? minDurationNanos, long? maxDurationNanos)
        {
            _fromNano         = fromNano;
            _toNano           = toNano;
            _serviceName      = serviceName;
            _spanName         = spanName;
            _status           = status;
            _httpStatusCode   = httpStatusCode;
            _minDurationNanos = minDurationNanos;
            _maxDurationNanos = maxDurationNanos;
        }

        /// <remarks>
        /// Predicate-for-predicate the same comparisons the post-materialisation filter loop
        /// in <c>SearchAsync</c> used to run, in the same order — the set of spans a search
        /// returns must not depend on where the test is applied.
        /// </remarks>
        public bool Matches(long ts, long dur, string name, string svc, SpanStatusCode status, short httpSC)
        {
            if (ts < _fromNano || ts > _toNano) return false;
            if (_serviceName      is not null && !svc.Equals(_serviceName, StringComparison.OrdinalIgnoreCase))  return false;
            if (_spanName         is not null && !name.Contains(_spanName, StringComparison.OrdinalIgnoreCase))  return false;
            if (_status           is not null && status != _status.Value)                                        return false;
            if (_httpStatusCode   is not null && httpSC != _httpStatusCode.Value)                                return false;
            if (_minDurationNanos is not null && dur < _minDurationNanos.Value)                                  return false;
            if (_maxDurationNanos is not null && dur > _maxDurationNanos.Value)                                  return false;
            return true;
        }
    }

    public static async IAsyncEnumerable<SpanRecord> SearchAsync(
        string           filePath,
        long             fromNano,
        long             toNano,
        string?          serviceName,
        string?          spanName,
        SpanStatusCode?  status,
        short?           httpStatusCode,
        long?            minDurationNanos,
        long?            maxDurationNanos,
        IReadOnlyList<AttrHint>? attrHints,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        // Use service index to skip blocks that cannot contain the target service.
        HashSet<uint>? allowedBlocks = null;
        if (serviceName is not null)
        {
            allowedBlocks = ReadServiceBlockIndices(filePath, serviceName);
            if (allowedBlocks.Count == 0) yield break;
        }

        // v3 per-block attribute blooms: drop blocks that cannot satisfy every
        // required attribute predicate (key presence / string equality).
        if (attrHints is { Count: > 0 })
        {
            var bloomAllowed = BloomFilterBlocks(filePath, attrHints);
            if (bloomAllowed is not null)
            {
                if (allowedBlocks is null) allowedBlocks = bloomAllowed;
                else allowedBlocks.IntersectWith(bloomAllowed);
                if (allowedBlocks.Count == 0) yield break;
            }
        }

        // ONE BLOCK AT A TIME, NOT ONE FILE AT A TIME.
        //
        // This used to open by materialising every span of every admitted block into a List and
        // filtering that. The block skipping was already here, so the FILTER was cheap — but
        // every span of every ADMITTED block was materialised before the first predicate ran,
        // and the list stayed rooted by this iterator's state machine for the whole
        // admit/drain/yield cycle above it. The caller's bounded heap therefore capped what a
        // search RETAINED ACROSS segments while the peak INSIDE one segment stayed the
        // segment's whole span count.
        //
        // That is not a rounding error at this engine's segment sizes. SpanSearchBoundTests
        // measures 1,749 bytes per span for an ordinary eight-attribute OTel span, so an
        // ordinary flushed segment (HotFlushThreshold = 50,000) is ~83 MB and a compacted one
        // (MaxSpansPerPass = 200,000) ~334 MB, live all at once — the query this bound was
        // written for, { .db.system = "mssql" && duration > 1s } over a month on a 512 MB
        // server, still died on a single compacted segment.
        //
        // A block is 4096 spans, so the peak is now one block plus the caller's page instead of
        // one segment: decode a block, filter it, hand the survivors over, drop it, take the
        // next. Measured on the same fixture: 83.4 MB -> 8.8 MB, and 8.8 MB is the block, not
        // the file. Bloom and service-index skipping are untouched — a block that was seeked
        // past before is still seeked past — and the order is still the file's, which is what
        // the caller's newest-first heap expects.
        var filter = new SpanFilter(fromNano, toNano, serviceName, spanName,
                                    status, httpStatusCode, minDurationNanos, maxDurationNanos);

        foreach (var s in StreamMatchingSpans(filePath, allowedBlocks, filter, ct))
            yield return s;
    }

    // ── Stats sidecar ──────────────────────────────────────────────────────────

    /// <summary>
    /// Reads per-service stats from the companion <c>.stats</c> file.
    /// Returns empty list if sidecar is absent (older segments).
    /// </summary>
    public static List<ServiceSegmentStats> ReadStats(string trcFilePath)
    {
        var statsPath = Path.ChangeExtension(trcFilePath, ".stats");
        if (!File.Exists(statsPath)) return [];

        const uint StatsMagic = 0x52_44_54_53; // "RDTS"

        try
        {
            using var fs = new FileStream(statsPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096);
            using var br = new BinaryReader(fs);

            if (br.ReadUInt32() != StatsMagic) return [];
            br.ReadUInt16(); // version
            uint count = br.ReadUInt32();

            // THE SAME SHAPE ONE FILE OVER: a count straight out of an unvalidated header, handed
            // to a constructor as a capacity. Measured on a 10-byte .stats whose count field says
            // 500 000 000: 4 000 005 640 bytes allocated, and only then an EOF the catch below
            // turns into an empty list — so the sidecar reported "no stats" and the box reported
            // four gigabytes. Every entry costs at least this many bytes, so a count past what the
            // file can hold describes entries that are not there.
            int minEntryBytes = 2 + 4 + 4 + 8 + 8 + 4 * HistogramBuckets.Count;
            FileBounds.RequireCountFits(count, fs.Length - fs.Position,
                fileBytesPerElement: minEntryBytes, heapBytesPerElement: minEntryBytes,
                "Stats sidecar", statsPath);

            var result = new List<ServiceSegmentStats>((int)count);
            for (uint i = 0; i < count; i++)
            {
                ushort nameLen  = br.ReadUInt16();
                string name     = Encoding.UTF8.GetString(br.ReadBytes(nameLen));
                uint   spans    = br.ReadUInt32();
                uint   errors   = br.ReadUInt32();
                long   minDur   = br.ReadInt64();
                long   maxDur   = br.ReadInt64();
                var    buckets  = new uint[HistogramBuckets.Count];
                for (int b = 0; b < HistogramBuckets.Count; b++)
                    buckets[b] = br.ReadUInt32();

                result.Add(new ServiceSegmentStats
                {
                    ServiceName      = name,
                    SpanCount        = spans,
                    ErrorCount       = errors,
                    MinDurationNanos = minDur,
                    MaxDurationNanos = maxDur,
                    Buckets          = buckets,
                });
            }
            return result;
        }
        catch
        {
            return [];
        }
    }

    // ── ReadAll (compaction) ───────────────────────────────────────────────────

    internal static List<SpanRecord> ReadAll(string filePath)
    {
        const long MaxTotalBytes = 500_000_000; // 500MB limit for safety
        var totalBytesRead = 0L;
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        br.ReadUInt32(); // magic
        ushort version = br.ReadUInt16();
        int spanCount = (int)br.ReadUInt32();
        br.ReadInt64();  // minNano
        br.ReadInt64();  // maxNano
        br.ReadByte();   // flags

        var (traceIdxOffset, _, _) = ReadFooter(fs, br, version);
        fs.Seek(27, SeekOrigin.Begin);

        var result = new List<SpanRecord>(Math.Min(spanCount, 100_000));
        uint blockIdx = 0;

        while (fs.Position < traceIdxOffset && totalBytesRead < MaxTotalBytes)
        {
            uint uncompSize = br.ReadUInt32();
            uint compSize = br.ReadUInt32();

            // AGAINST THE FILE, NOT AGAINST A CONSTANT. MaxBlockBytes stopped a garbage prefix from
            // asking for gigabytes; it did nothing about a 3.5 KB file asking for 64 MB, which one
            // flipped byte inside a real compSize produces. This shape sits on the trace lookup, the
            // detail view and the SSE list, and GetTraceAsync fans it eight ways — half a gigabyte
            // from one click on the 512 MB box this branch exists to keep alive. The bytes actually
            // left in the file are the bound the file cannot forge; the uncompressed size still
            // needs the constant, because nothing on disk bounds what a payload decompresses to.
            // ReadAll keeps its OWN, tighter budget on top of the file bound, and the asymmetry is
            // deliberate: compaction rewrites whole files, so a block search will happily stream
            // past is one compaction must refuse rather than buffer. Losing that limit here made a
            // 16 MB block merge silently.
            FileBounds.RequireLengthFits(compSize, Math.Min(fs.Length - fs.Position, MergeBlockBytes),
                                         $"Block {blockIdx}", filePath);
            FileBounds.RequireLengthFits(uncompSize, MergeBlockBytes, $"Block {blockIdx} uncompressed", filePath);

            if (totalBytesRead + compSize > MaxTotalBytes)
                throw new InvalidDataException($"Total data exceeds {MaxTotalBytes} bytes limit");

            totalBytesRead += compSize;

            // Pooled block buffers — see StreamMatchingSpans; compaction reads whole files,
            // so unpooled blocks here were pure LOH churn on every merge pass.
            byte[]  comp   = ArrayPool<byte>.Shared.Rent((int)compSize);
            byte[]? rawBuf = null;
            try
            {
                fs.ReadExactly(comp, 0, (int)compSize);
                // The third of the three places this length is taken, and the one the sweep missed
                // even though the reason was already written down two methods away: the size lives
                // INSIDE the compressed payload, so the compSize test above never saw it and a short,
                // well-formed block can still ask for gigabytes. A negative value is worse than a
                // large one — Rent throws ArgumentOutOfRangeException, which sails straight past the
                // OutOfMemoryException catch this method relies on.
                int rawLen = LZ4Pickler.UnpickledSize(comp.AsSpan(0, (int)compSize));
                if (rawLen is < 0 or > MaxBlockBytes)
                    throw new InvalidDataException(
                        $"A block decompresses to {rawLen} bytes in {filePath}");
                rawBuf = ArrayPool<byte>.Shared.Rent(rawLen);
                LZ4Pickler.Unpickle(comp.AsSpan(0, (int)compSize), rawBuf.AsSpan(0, rawLen));

                var reader = new MessagePackReader(rawBuf.AsMemory(0, rawLen));
                int cnt = reader.ReadArrayHeader();
                if (cnt > 50_000) // Safety limit for spans per block
                    throw new InvalidDataException($"Block {blockIdx} contains too many spans: {cnt}");

                long prevTs = 0;
                for (int i = 0; i < cnt; i++)
                {
                    if (result.Count >= 1_000_000) // Total span limit
                        throw new InvalidDataException($"Total span count exceeds 1,000,000");
                    result.Add((version >= 3
                        ? DeserializeSpanV3(ref reader, i == 0, ref prevTs, SpanFilter.MatchAll)
                        : DeserializeSpan(ref reader, SpanFilter.MatchAll))!);
                }
            }
            catch (OutOfMemoryException ex)
            {
                throw new InvalidDataException($"Out of memory while processing block {blockIdx} (size: {compSize} bytes)", ex);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(comp);
                if (rawBuf is not null) ArrayPool<byte>.Shared.Return(rawBuf);
            }
            blockIdx++;
        }
        return result;
    }

    // ── Streaming block reader (search path) ───────────────────────────────────

    /// <summary>
    /// Walks the span blocks of a <c>.trc</c> in file order, yielding only the records that
    /// satisfy <paramref name="filter"/> and holding at most ONE block's survivors at a time.
    ///
    /// <para>Synchronous on purpose: there is no I/O here that is not a buffered
    /// <c>FileStream</c> read, and a sync iterator is what lets the file handle, the block
    /// buffers and the current block's records live in one scope that is released the moment
    /// the caller stops enumerating — <c>await using</c> on the async iterator above disposes
    /// this one.</para>
    ///
    /// <para>The decode cannot be pushed all the way down to one record at a time:
    /// <c>MessagePackReader</c> is a ref struct and cannot survive a <c>yield return</c>. One
    /// block is the floor, and one block is 4096 spans.</para>
    /// </summary>
    /// <param name="allowedBlocks">
    /// When non-null, only blocks whose 0-based index is in this set are decompressed —
    /// exactly as before; the others are seeked past without an LZ4 or msgpack touch.
    /// </param>
    private static IEnumerable<SpanRecord> StreamMatchingSpans(
        string         filePath,
        HashSet<uint>? allowedBlocks,
        SpanFilter     filter,
        CancellationToken ct)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        br.ReadUInt32(); // magic
        ushort version = br.ReadUInt16();
        br.ReadUInt32(); // spanCount — deliberately unused: nothing here is sized by it any more
        br.ReadInt64();  // minNano
        br.ReadInt64();  // maxNano
        br.ReadByte();   // flags

        var (traceIdxOffset, _, _) = ReadFooter(fs, br, version);
        fs.Seek(27, SeekOrigin.Begin); // reset to after header

        // The one buffer this walk keeps. Cleared — not reallocated — per block, so its
        // backing array settles at the largest single block's match count and stays there.
        var batch = new List<SpanRecord>(1024);

        uint blockIdx = 0;
        while (fs.Position < traceIdxOffset)
        {
            ct.ThrowIfCancellationRequested();

            uint uncompSize = br.ReadUInt32();
            uint compSize   = br.ReadUInt32();

            if (allowedBlocks is not null && !allowedBlocks.Contains(blockIdx))
            {
                // Skip decompression + deserialization — pure seek, O(1).
                // Safe for v3 too: the Δts chain restarts on every block.
                fs.Seek(compSize, SeekOrigin.Current);
                blockIdx++;
                continue;
            }

            // AGAINST THE FILE, NOT AGAINST A CONSTANT. MaxBlockBytes stopped a garbage prefix from
            // asking for gigabytes; it did nothing about a 3.5 KB file asking for 64 MB, which one
            // flipped byte inside a real compSize produces. This shape sits on the trace lookup, the
            // detail view and the SSE list, and GetTraceAsync fans it eight ways — half a gigabyte
            // from one click on the 512 MB box this branch exists to keep alive. The bytes actually
            // left in the file are the bound the file cannot forge; the uncompressed size still
            // needs the constant, because nothing on disk bounds what a payload decompresses to.
            FileBounds.RequireLengthFits(compSize, fs.Length - fs.Position, $"Block {blockIdx}", filePath);
            FileBounds.RequireLengthFits(uncompSize, MaxBlockBytes, $"Block {blockIdx}" + " uncompressed", filePath);

            // Cleared BEFORE the decode, so the previous block's records are unrooted while
            // this one is being built rather than on top of it.
            batch.Clear();

            // Pooled compressed + decompressed block buffers — every decoded value is
            // copied out by the span deserialisers, so nothing outlives the loop turn.
            byte[]  comp   = ArrayPool<byte>.Shared.Rent((int)compSize);
            byte[]? rawBuf = null;
            try
            {
                fs.ReadExactly(comp, 0, (int)compSize);
                int rawLen = LZ4Pickler.UnpickledSize(comp.AsSpan(0, (int)compSize));
                if (rawLen > MaxBlockBytes)
                    throw new InvalidDataException(
                        $"Block {blockIdx} decompresses to {rawLen} bytes in {filePath}");

                rawBuf = ArrayPool<byte>.Shared.Rent(rawLen);
                LZ4Pickler.Unpickle(comp.AsSpan(0, (int)compSize), rawBuf.AsSpan(0, rawLen));
                DecodeBlockInto(rawBuf.AsMemory(0, rawLen), version, filter, batch);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(comp);
                if (rawBuf is not null) ArrayPool<byte>.Shared.Return(rawBuf);
            }

            // Yielded outside the buffer scope: the pooled arrays are back in the pool before
            // the caller is given anything, so a caller that suspends here holds one block's
            // RECORDS and no block BUFFERS.
            foreach (var s in batch)
            {
                ct.ThrowIfCancellationRequested();
                yield return s;
            }

            blockIdx++;
        }
    }

    /// <summary>
    /// Decodes one decompressed block, appending the records that pass <paramref name="filter"/>
    /// to <paramref name="into"/>. Rejected spans are still fully CONSUMED — the Δts chain and
    /// the msgpack cursor both run through them — they are just never built.
    /// </summary>
    private static void DecodeBlockInto(
        ReadOnlyMemory<byte> raw, ushort version, in SpanFilter filter, List<SpanRecord> into)
    {
        var reader = new MessagePackReader(raw);
        int cnt    = reader.ReadArrayHeader();
        if (cnt > 50_000) // same per-block sanity bound ReadAll enforces
            throw new InvalidDataException($"Block contains too many spans: {cnt}");

        long prevTs = 0;
        for (int i = 0; i < cnt; i++)
        {
            var rec = version >= 3
                ? DeserializeSpanV3(ref reader, i == 0, ref prevTs, filter)
                : DeserializeSpan(ref reader, filter);
            if (rec is not null) into.Add(rec);
        }
    }

    // ── Index readers ──────────────────────────────────────────────────────────

    private static List<uint> ReadTraceOffsets(string filePath, TraceId traceId)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        ushort version = ReadVersion(fs, br);
        var (traceIdxOffset, _, _) = ReadFooter(fs, br, version);
        fs.Seek(traceIdxOffset, SeekOrigin.Begin);

        if (version >= 3)
        {
            // v3: the index is one LZ4 block — decompress, then scan.
            uint uncompSize = br.ReadUInt32();
            uint compSize   = br.ReadUInt32();

            // THE SAME LENGTH TEST THE BLOCK READERS DO, at the one call site the rationale for
            // MaxBlockBytes had not been applied to. Both Rents below take a length straight out
            // of the file, and its stated job is to stop a garbage length prefix being handed to
            // ArrayPool.Rent. What made this one easy to miss is that it DEGRADES rather than
            // crashes: a compSize past int.MaxValue casts negative and Rent throws
            // ArgumentOutOfRange, which the engine catches and logs as a skipped segment. The
            // values in between are the problem — a corrupt prefix of a few hundred MB is a
            // few hundred MB actually rented, per lookup, on a box the rest of this file exists
            // to keep inside one block.
            // AGAINST THE FILE, NOT AGAINST A CONSTANT. MaxBlockBytes stopped a garbage prefix from
            // asking for gigabytes; it did nothing about a 3.5 KB file asking for 64 MB, which one
            // flipped byte inside a real compSize produces. This shape sits on the trace lookup, the
            // detail view and the SSE list, and GetTraceAsync fans it eight ways — half a gigabyte
            // from one click on the 512 MB box this branch exists to keep alive. The bytes actually
            // left in the file are the bound the file cannot forge; the uncompressed size still
            // needs the constant, because nothing on disk bounds what a payload decompresses to.
            FileBounds.RequireLengthFits(compSize, fs.Length - fs.Position, $"Trace index", filePath);
            FileBounds.RequireLengthFits(uncompSize, MaxBlockBytes, $"Trace index" + " uncompressed", filePath);

            // Pooled on both sides of the decompress. This runs once per file on EVERY
            // trace lookup, and both arrays are index-of-the-whole-file sized (hundreds
            // of KB — straight into the LOH). The previous ReadBytes+Unpickle pair
            // allocated both fresh each call: ~470 MB of LOH churn in a 7-minute
            // allocation trace, and the fragmentation that kept forcing compacting GCs.
            byte[]  comp   = ArrayPool<byte>.Shared.Rent((int)compSize);
            byte[]? rawBuf = null;
            try
            {
                fs.ReadExactly(comp, 0, (int)compSize);
                int rawLen = LZ4Pickler.UnpickledSize(comp.AsSpan(0, (int)compSize));

                // The length INSIDE the compressed payload, which the test above never saw: LZ4's
                // header carries the decompressed size, so a short, well-formed block can still
                // ask for gigabytes. Same bound, same reason.
                if (rawLen > MaxBlockBytes)
                    throw new InvalidDataException(
                        $"Trace index decompresses to {rawLen} bytes in {filePath}");

                rawBuf = ArrayPool<byte>.Shared.Rent(rawLen);
                LZ4Pickler.Unpickle(comp.AsSpan(0, (int)compSize), rawBuf.AsSpan(0, rawLen));
                ReadOnlySpan<byte> raw = rawBuf.AsSpan(0, rawLen);

                int pos = 0;
                uint traceCount = BinaryPrimitives.ReadUInt32LittleEndian(raw[pos..]); pos += 4;
                for (uint i = 0; i < traceCount; i++)
                {
                    var candidate = TraceId.Parse(raw.Slice(pos, 16)); pos += 16;
                    uint offsetCnt = BinaryPrimitives.ReadUInt32LittleEndian(raw[pos..]); pos += 4;

                    // THE FOURTH LENGTH IN THIS BLOCK, AND THE ONLY ONE WITHOUT A BOUND. The three
                    // above are ArrayPool.Rent arguments and each got a MaxBlockBytes guard and a
                    // test; this one is a List<uint> CAPACITY four lines later, which is not a Rent
                    // and so was not looked at — although it is read out of the same untrusted
                    // block, on the path that runs FIRST on every single trace lookup. Measured on
                    // a 94-byte file with these four bytes overwritten: offsetCnt = 1 073 741 823
                    // allocated 4 295 035 576 bytes before failing, and 50 000 000 allocated
                    // 200 082 712 — on the 512 MB box the rest of this file exists to keep inside
                    // one block, from one click.
                    //
                    // The bound is exact and free, and the guards' own rationale already names it:
                    // every offset is four bytes of a payload that is itself capped at
                    // MaxBlockBytes, so a count past what is LEFT of this block describes a file
                    // that cannot exist. It also removes the int overflow in the skip below, where
                    // `(int)offsetCnt * 4` wraps negative for a large count.
                        FileBounds.RequireCountFits(offsetCnt, raw.Length - pos,
                            fileBytesPerElement: 4, heapBytesPerElement: 4, "Trace index", filePath);

                    if (candidate.Equals(traceId))
                    {
                        var offsets = new List<uint>((int)offsetCnt);
                        for (uint j = 0; j < offsetCnt; j++)
                        {
                            offsets.Add(BinaryPrimitives.ReadUInt32LittleEndian(raw[pos..]));
                            pos += 4;
                        }
                        return offsets;
                    }
                    pos += (int)offsetCnt * 4;
                }
                return [];
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(comp);
                if (rawBuf is not null) ArrayPool<byte>.Shared.Return(rawBuf);
            }
        }

        uint count = br.ReadUInt32();
        var  idBuf = new byte[16];

        for (uint i = 0; i < count; i++)
        {
            br.Read(idBuf, 0, 16);
            var candidate  = TraceId.Parse(idBuf);
            uint offsetCnt = br.ReadUInt32();

            // The same count, in the uncompressed v2 index, bounded against the same evidence —
            // here the bytes left in the FILE rather than in a decompressed block. A v2 segment is
            // read by exactly the same lookup and would otherwise keep the hole open for every
            // install that has not finished migrating.
            long leftInFile = fs.Length - fs.Position;
            FileBounds.RequireCountFits(offsetCnt, leftInFile,
                fileBytesPerElement: 4, heapBytesPerElement: 4, "Trace index", filePath);

            if (candidate.Equals(traceId))
            {
                var offsets = new List<uint>((int)offsetCnt);
                for (uint j = 0; j < offsetCnt; j++) offsets.Add(br.ReadUInt32());
                return offsets;
            }
            fs.Seek(offsetCnt * 4L, SeekOrigin.Current);
        }
        return [];
    }

    /// <summary>
    /// Blocks whose attribute bloom may satisfy every hint, or null when the file
    /// has no usable bloom index (v2 file / empty blooms) — null means "no skip".
    /// </summary>
    private static HashSet<uint>? BloomFilterBlocks(string filePath, IReadOnlyList<AttrHint> hints)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        ushort version = ReadVersion(fs, br);
        if (version < 3) return null;
        var (_, _, bloomIdxOffset) = ReadFooter(fs, br, version);
        // null here means "this file has no usable bloom index", which the caller treats as "skip
        // nothing" — the safe direction, and why this one may answer rather than throw. The offset
        // still has to be inside the file, because every bound below is measured from where it
        // lands: a torn offset makes "the bytes that remain" a number the file never limited.
        if (bloomIdxOffset <= 0 || bloomIdxOffset >= fs.Length) return null;
        fs.Seek(bloomIdxOffset, SeekOrigin.Begin);

        // Pre-hash the hints once.
        Span<ulong> hashes = stackalloc ulong[Math.Min(hints.Count, 16)];
        int nHints = Math.Min(hints.Count, hashes.Length);
        for (int i = 0; i < nHints; i++)
        {
            var h = hints[i];
            hashes[i] = h.LowerValue is null
                ? SpanBloom.HashKey(h.Key)
                : SpanBloom.HashKeyValue(h.Key, h.LowerValue);
        }

        // BOUNDED BY THE BYTES THAT COULD HOLD IT. This runs on every TraceQL query carrying an
        // attribute hint — that is, on { .db.system = "mssql" }, the query this branch exists for —
        // and a HashSet<uint> of capacity N costs 16N bytes, so one flipped high byte turning 12
        // into 0x4000000C asks for about seventeen gigabytes. Each block writes at least its own
        // four-byte length, so more blocks than a quarter of what is left is a torn count.
        // FOUR BYTES ON DISK, ABOUT SIXTEEN IN A HashSet<uint> — which is why the bound takes both
        // and uses the larger. Dividing the remaining bytes by four permitted four times the file:
        // measured at 5.17x on a 361 KB segment whose bloom offset was torn to a value still inside
        // the file, so the offset check above cannot see it.
        uint blockCount = br.ReadUInt32();
        FileBounds.RequireCountFits(blockCount, fs.Length - fs.Position,
            fileBytesPerElement: 4, heapBytesPerElement: 16, "Bloom index", filePath);

        var allowed = new HashSet<uint>((int)blockCount);
        for (uint b = 0; b < blockCount; b++)
        {
            uint len = br.ReadUInt32();
            if (len > fs.Length - fs.Position)
                throw new InvalidDataException(
                    $"Bloom bitset for block {b} claims {len} bytes past the end of {filePath}");
            var bitset = len > 0 ? br.ReadBytes((int)len) : [];
            bool pass = true;
            for (int i = 0; i < nHints && pass; i++)
                pass = SpanBloom.MayContain(bitset, hashes[i]);
            if (pass) allowed.Add(b);
        }
        return allowed;
    }

    /// <returns>0-based block indices containing at least one span from <paramref name="serviceName"/>.</returns>
    private static HashSet<uint> ReadServiceBlockIndices(string filePath, string serviceName)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        ushort version = ReadVersion(fs, br);
        var (_, svcIdxOffset, _) = ReadFooter(fs, br, version);
        // THROWN, NOT ANSWERED WITH AN EMPTY SET, and the difference is the whole finding. An empty
        // set here does NOT mean "no service index"; the caller reads it as the list of blocks worth
        // opening, so [] means SKIP THE WHOLE SEGMENT. Returning it on a torn offset turned four
        // shapes of corruption — past EOF, at EOF, zero, negative — from loud exceptions that
        // reached the classifier and raised a banner into a service-filtered search that quietly
        // answered zero spans while an unfiltered one returned all forty. That is silent data loss
        // manufactured by a bounds check, which is worse than the unbounded read it replaced.
        //
        // The empty return in ReadServicesFromIndex is correct for the opposite reason: there it
        // means "I do not know which services are here" and the caller degrades openly, testing
        // Services.Length > 0 before it skips anything. Same literal, opposite meaning, one file.
        if (svcIdxOffset <= 0 || svcIdxOffset >= fs.Length)
            throw new InvalidDataException(
                $"Service index offset {svcIdxOffset} is outside {filePath} ({fs.Length} bytes)");
        fs.Seek(svcIdxOffset, SeekOrigin.Begin);

        // Same rule as the bloom index: a service costs at least six bytes here (its length prefix
        // and its block count), and a block index costs four.
        uint svcCount = br.ReadUInt32();
        FileBounds.RequireCountFits(svcCount, fs.Length - fs.Position,
            fileBytesPerElement: 6, heapBytesPerElement: 6, "Service index", filePath);

        for (uint i = 0; i < svcCount; i++)
        {
            ushort nameLen = br.ReadUInt16();
            var    name    = Encoding.UTF8.GetString(br.ReadBytes(nameLen));
            uint   blkCnt  = br.ReadUInt32();
            // Same asymmetry as the bloom index: the set costs far more than the four bytes each
            // index occupies on disk.
            FileBounds.RequireCountFits(blkCnt, fs.Length - fs.Position,
                fileBytesPerElement: 4, heapBytesPerElement: 16, $"Service '{name}'", filePath);

            if (name.Equals(serviceName, StringComparison.OrdinalIgnoreCase))
            {
                var set = new HashSet<uint>((int)blkCnt);
                for (uint b = 0; b < blkCnt; b++) set.Add(br.ReadUInt32());
                return set;
            }
            fs.Seek(blkCnt * 4L, SeekOrigin.Current);
        }
        return [];
    }

    /// <summary>
    /// The service names a segment declares, or an EMPTY LIST if the index cannot be believed.
    ///
    /// <para>Empty rather than an exception, and that is the whole point of this method's shape.
    /// It is called from <see cref="ReadSegmentInfo"/>, which runs for every .trc at startup —
    /// and whose caller answers ANY throw with <c>DeleteSegmentFiles(file)</c>. So a torn
    /// four-byte count here used to cost two things at once: 1 600 074 528 bytes allocated by
    /// <c>new string[count]</c> from a 517-byte file, measured, and then the destruction of a
    /// perfectly readable segment along with its .stats, .svcgraph and .tracesum.</para>
    ///
    /// <para>Neither is necessary. This index is an OPTIMISATION — it lets a search skip segments
    /// and blocks that cannot hold the service it wants. Without it every block is read, which is
    /// slower and completely correct. Degrading to "I do not know which services are in here" is
    /// therefore the honest answer to a damaged index, and the spans behind it stay queryable.
    /// <c>SpanSegmentInfo.Services</c> is already documented as advisory: an empty array means no
    /// segment-level service skip, never "this segment has no services".</para>
    /// </summary>
    private static string[] ReadServicesFromIndex(FileStream fs, BinaryReader br, long svcIdxOffset)
    {
        try
        {
            if (svcIdxOffset <= 0 || svcIdxOffset >= fs.Length) return [];
            fs.Seek(svcIdxOffset, SeekOrigin.Begin);

            // A service costs at least six bytes here: a two-byte name length and a four-byte
            // block count. More than that many is a number no writer produced.
            uint count = br.ReadUInt32();
            if (count > FileBounds.MaxCountThatFits(fs.Length - fs.Position, 6, 6)) return [];

            var services = new string[count];
            for (uint i = 0; i < count; i++)
            {
                ushort nameLen = br.ReadUInt16();
                if (nameLen > fs.Length - fs.Position) return [];
                services[i] = Encoding.UTF8.GetString(br.ReadBytes(nameLen));

                uint blkCnt = br.ReadUInt32();
                if (blkCnt > (fs.Length - fs.Position) / 4) return [];
                fs.Seek(blkCnt * 4L, SeekOrigin.Current);
            }
            return services;
        }
        catch (Exception ex) when (ex is EndOfStreamException or IOException or InvalidDataException
                                      or ArgumentException or OutOfMemoryException)
        {
            return [];
        }
    }

    // ── Footer (v2: 20 bytes, v3: 28 bytes — extra bloom-index offset) ────────

    private static (long traceIdxOffset, long svcIdxOffset, long bloomIdxOffset) ReadFooter(
        FileStream fs, BinaryReader br, ushort version)
    {
        int size = version >= 3 ? 28 : 20;
        fs.Seek(-size, SeekOrigin.End);
        long traceIdx = (long)br.ReadUInt64();
        long svcIdx   = (long)br.ReadUInt64();
        long bloomIdx = version >= 3 ? (long)br.ReadUInt64() : 0;
        uint magic    = br.ReadUInt32();
        if (magic != FooterMagic) throw new InvalidDataException($"Invalid .trc footer magic in {fs.Name}");
        return (traceIdx, svcIdx, bloomIdx);
    }

    /// <summary>Reads just the format version from the header of an open stream, restoring position.</summary>
    private static ushort ReadVersion(FileStream fs, BinaryReader br)
    {
        long pos = fs.Position;
        fs.Seek(4, SeekOrigin.Begin);
        ushort v = br.ReadUInt16();
        fs.Seek(pos, SeekOrigin.Begin);
        return v;
    }

    // ── Span deserialisation ───────────────────────────────────────────────────

    /// <summary>
    /// v3 positional span:
    /// [ tid, sid, pid|nil, Δts, dur, name, svc, kind, status, hsc, attrs|nil ].
    /// <paramref name="prevTs"/> carries the Δts chain across the block.
    ///
    /// <para>Returns null when <paramref name="filter"/> rejects the span. The reader is
    /// advanced past it either way — the Δts chain and the msgpack cursor are the block's,
    /// not the span's — but a rejected span's ATTRIBUTE MAP is skipped rather than decoded.
    /// It used to be decoded unconditionally, which is what made every span of an admitted
    /// block cost ~1.5 KB whether or not the search would ever look at it.</para>
    /// </summary>
    private static SpanRecord? DeserializeSpanV3(
        ref MessagePackReader r, bool first, ref long prevTs, in SpanFilter filter)
    {
        int n = r.ReadArrayHeader(); // 11 fields

        TraceId traceId = default;
        SpanId  spanId  = default;
        SpanId  parentId = default;

        var seq = r.ReadBytes();
        if (seq is { } tidSeq) { Span<byte> b = stackalloc byte[16]; CopyFixed(tidSeq, b); traceId = TraceId.Parse(b); }
        seq = r.ReadBytes();
        if (seq is { } sidSeq) { Span<byte> b = stackalloc byte[8]; CopyFixed(sidSeq, b); spanId = SpanId.Parse(b); }
        if (r.TryReadNil())
        {
            // root span — no parent
        }
        else
        {
            seq = r.ReadBytes();
            if (seq is { } pidSeq) { Span<byte> b = stackalloc byte[8]; CopyFixed(pidSeq, b); parentId = SpanId.Parse(b); }
        }

        long ts = first ? r.ReadInt64() : prevTs + r.ReadInt64();
        prevTs = ts;

        long   dur    = r.ReadInt64();
        string name   = r.ReadString() ?? string.Empty;
        string svc    = r.ReadString() ?? string.Empty;
        var    kind   = (SpanKind)r.ReadByte();
        var    status = (SpanStatusCode)r.ReadByte();
        short  httpSC = r.ReadInt16();

        // Decided BEFORE the attribute map, which is the whole reason the map is the last
        // positional field. Everything the scalar predicate needs is already in hand.
        bool keep = filter.Matches(ts, dur, name, svc, status, httpSC);

        IReadOnlyDictionary<string, object?>? attrs = null;
        if (r.TryReadNil())
        {
            // no attributes
        }
        else if (!keep)
        {
            // Rejected: step over the map whole. No dictionary, no keys, no boxed values.
            r.Skip();
        }
        else
        {
            int cnt = r.ReadMapHeader();
            var dict = new Dictionary<string, object?>(cnt, StringComparer.Ordinal);
            for (int i = 0; i < cnt; i++)
            {
                var key = r.ReadString() ?? string.Empty;
                dict[key] = ReadAttrValue(ref r);
            }
            attrs = dict;
        }

        // Consume any fields a future minor revision might append.
        for (int i = 11; i < n; i++) r.Skip();

        if (!keep) return null;

        return new SpanRecord
        {
            TraceId           = traceId,
            SpanId            = spanId,
            ParentSpanId      = parentId,
            StartTimeUnixNano = ts,
            DurationNanos     = dur,
            Name              = name,
            ServiceName       = svc,
            Kind              = kind,
            Status            = status,
            HttpStatusCode    = httpSC,
            Attributes        = attrs,
        };
    }

    private static object? ReadAttrValue(ref MessagePackReader r) =>
        r.NextMessagePackType switch
        {
            MessagePackType.String  => r.ReadString(),
            MessagePackType.Integer => r.ReadInt64(),
            MessagePackType.Float   => r.ReadDouble(),
            MessagePackType.Boolean => r.ReadBoolean(),
            MessagePackType.Nil     => ReadNil(ref r),
            _                       => SkipUnknown(ref r),
        };

    private static object? ReadNil(ref MessagePackReader r) { r.ReadNil(); return null; }
    private static object? SkipUnknown(ref MessagePackReader r) { r.Skip(); return null; }

    private static void CopyFixed(in System.Buffers.ReadOnlySequence<byte> seq, Span<byte> dest)
    {
        int pos = 0;
        foreach (var seg in seq)
        {
            int take = Math.Min(seg.Length, dest.Length - pos);
            if (take <= 0) break;
            seg.Span[..take].CopyTo(dest[pos..]);
            pos += take;
        }
    }

    /// <summary>
    /// Legacy v2 span: a string-keyed map, so nothing can be decided until every field is
    /// read — the attribute BLOB is still copied out for a span the filter rejects, but it is
    /// not turned into a dictionary. Returns null on rejection, as the v3 path does.
    /// </summary>
    private static SpanRecord? DeserializeSpan(ref MessagePackReader r, in SpanFilter filter)
    {
        int fields = r.ReadMapHeader();
        TraceId        traceId   = default;
        SpanId         spanId    = default;
        SpanId         parentId  = default;
        long           ts        = 0;
        long           dur       = 0;
        string         name      = string.Empty;
        string         svc       = string.Empty;
        SpanKind       kind      = SpanKind.Unspecified;
        SpanStatusCode status    = SpanStatusCode.Unset;
        short          httpSC    = 0;
        byte[]?        attrBytes = null;

        for (int i = 0; i < fields; i++)
        {
            var key = r.ReadString();
            switch (key)
            {
                case "tid":  { var b = ReadBytesFixed(ref r, 16); traceId  = TraceId.Parse(b); break; }
                case "sid":  { var b = ReadBytesFixed(ref r,  8); spanId   = SpanId.Parse(b);  break; }
                case "pid":  { var b = ReadBytesFixed(ref r,  8); parentId = SpanId.Parse(b);  break; }
                case "ts":   ts     = r.ReadInt64();                 break;
                case "dur":  dur    = r.ReadInt64();                 break;
                case "n":    name   = r.ReadString() ?? string.Empty; break;
                case "svc":  svc    = r.ReadString() ?? string.Empty; break;
                case "k":    kind   = (SpanKind)r.ReadByte();         break;
                case "st":   status = (SpanStatusCode)r.ReadByte();   break;
                case "hsc":  httpSC = r.ReadInt16();                  break;
                case "attr":
                {
                    var seq = r.ReadBytes();
                    if (seq.HasValue) attrBytes = System.Buffers.BuffersExtensions.ToArray(seq.Value);
                    break;
                }
                default: r.Skip(); break;
            }
        }

        if (!filter.Matches(ts, dur, name, svc, status, httpSC)) return null;

        return new SpanRecord
        {
            TraceId           = traceId,
            SpanId            = spanId,
            ParentSpanId      = parentId,
            StartTimeUnixNano = ts,
            DurationNanos     = dur,
            Name              = name,
            ServiceName       = svc,
            Kind              = kind,
            Status            = status,
            HttpStatusCode    = httpSC,
            Attributes        = attrBytes is { Length: > 0 } ? DeserializeAttributes(attrBytes) : null,
        };
    }

    private static byte[] ReadBytesFixed(ref MessagePackReader r, int expectedLen)
    {
        var seq = r.ReadBytes();
        if (seq is null) return new byte[expectedLen];

        long totalLen = 0;
        foreach (var seg in seq.Value)
            totalLen += seg.Length;

        if (totalLen > 1024) // 1KB limit for trace/span IDs
            throw new InvalidDataException($"Byte array too large: {totalLen} bytes (expected {expectedLen})");

        var arr = new byte[(int)totalLen];
        long pos = 0;
        foreach (var seg in seq.Value)
        {
            seg.Span.CopyTo(arr.AsSpan((int)pos));
            pos += seg.Length;
        }
        return arr;
    }

    private static IReadOnlyDictionary<string, object?>? DeserializeAttributes(byte[] bytes)
    {
        try { return MessagePackSerializer.Deserialize<Dictionary<string, object?>>(bytes); }
        catch { return null; }
    }

    private static FileStream OpenRead(string path) =>
        new(path, FileMode.Open, FileAccess.Read, FileShare.Read, 65536, FileOptions.SequentialScan);
}
