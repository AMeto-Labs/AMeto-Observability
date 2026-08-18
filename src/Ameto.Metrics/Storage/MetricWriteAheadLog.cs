using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Ameto.Metrics.Storage;

/// <summary>
/// Write-ahead log for the metric hot tier, backed by a memory-mapped file.
/// Built along the same lines as <c>Ameto.Storage.WriteAheadLog</c> (logs tier), including
/// its companion-file trick for data that repeats across entries.
///
/// <para>Why it exists: the hot tier had no log, so durability came from flushing every
/// 60 seconds regardless of how little had arrived — and a flush writes one <c>.mts</c> file
/// PER METRIC NAME. A deployment with 40 instrument names therefore produced 40 files a
/// minute, ~57 600 a day, which the rollup then had to chew through at four metric names per
/// five-minute pass. Appending here is a struct store into a mapped page, so the flush is
/// free to wait for a batch worth writing.</para>
///
/// <para>Two files. The log itself holds one fixed-size entry per data point; the series a
/// point belongs to — name, kind, unit, labels, histogram bounds — is registered once in a
/// companion <c>.pool</c> and referenced afterwards by index. Without that, every point
/// would carry its full label set, which is the bulk of a metric's bytes.</para>
///
/// <code>
///   metrics.wal
///     [File Header — 32 bytes]
///       0   Magic               uint32  "RDMW"
///       4   Version             uint16  1
///       6   _pad                uint16
///       8   WriteOffset         int64
///      16   Generation          uint64  stamped on new appends
///      24   CommittedGeneration uint64  everything at or below this is already in files
///     [Entry — 48 bytes, Pack = 1][BucketCounts: BucketCount × int64]
///
///   metrics.wal.pool
///     [index uint32][byteLen uint32][kind, name, unit, labels, bounds]  (repeated)
/// </code>
///
/// <para><b>Flush protocol.</b> A flush is two-phase, because points keep arriving while the
/// files are being written and their log records must survive:</para>
/// <list type="number">
/// <item><see cref="BeginFlush"/>, called while the caller holds whatever lock makes the
/// hot-tier snapshot atomic, closes the current generation and opens the next. Everything
/// snapshotted carries generation G; everything appended from now on carries G+1.</item>
/// <item><see cref="CommitFlush"/>, called once the files are durable, records G as
/// committed and then compacts the log — the generations at or below G form a prefix
/// (generation is non-decreasing in append order), so reclaiming them is one move of the
/// surviving tail to the front.</item>
/// </list>
///
/// <para><b>One flush at a time, and why the log insists on it.</b> The watermark is a single
/// u64 and <see cref="CommitFlush"/> frees everything at or below it, so committing G+1 also
/// frees G. That is only sound if G's files were already written. With two flushes open at
/// once it is not: the later one's commit reclaims the earlier one's records while the earlier
/// one is still inside its file write, and a failure there leaves those points in neither a
/// file nor the log — durable nowhere, with nothing logged and nothing thrown. The class used
/// to leave that to its caller; it now enforces it. <see cref="BeginFlush"/> refuses to open a
/// second flush, and <see cref="CommitFlush"/> refuses a generation that is not the open one,
/// so the watermark can never pass a flush that has not finished writing. A flush that gives
/// its generation back to the tier says so with <see cref="AbandonFlush"/>.</para>
///
/// <para>A flush that fails simply never commits: the snapshot's records still sit in the
/// log below an unchanged watermark, so a crash before the retry replays them. This is what
/// makes "durable before queryable" true for a point that arrives mid-flush — an earlier
/// design zeroed the whole log after writing the files and destroyed exactly those
/// records. Its points are restored to the hot tier and NOT re-logged, so the abandoned
/// generation's records are what keeps them durable until the next flush snapshots them and
/// commits a generation above it. That coupling is why the failure path must abandon rather
/// than simply return.</para>
///
/// <para><b>What "the flush failed" has to mean for that to be true.</b> Leaving a generation
/// replayable while its points are also in a file is a duplicate, not a rescue, so only a
/// failure that put NO file on disk may abandon. That is a condition on the caller, and it was
/// not met: the writer wrote one file per metric name straight to its final path and kept
/// whatever it had finished when a later one threw, and the flush treated every step after the
/// write — publishing the files, logging — as a failed write too. Both are closed at the
/// source: <c>MetricWriter.Write</c> deletes its own output before rethrowing, and
/// <c>MetricStorageEngine.FlushHotTierAsync</c> restores and abandons only around the write
/// itself, committing on every path where a file survives.</para>
///
/// <para><b>Crash recovery.</b> Recovery keeps entries whose generation is ABOVE the
/// committed watermark. The watermark is written before any bytes move, so a crash during
/// compaction cannot resurrect cold points; the relocated tail is terminated with a
/// generation-0 slot before the new write offset is stored, so such a crash cannot return
/// the survivors twice either. The generation is assigned here, under this
/// class's own lock; nothing derived from the data (a point's timestamp, say) would do,
/// because those come from the instrumented client and are not monotonic in append order.
/// Once the caller's side of the abandon contract holds (see above), a crash landing between
/// the file write and the commit is the only thing left that can duplicate points — a window
/// of one <c>CommitFlush</c> call, not a state the code reaches on its own.</para>
///
/// <para>Generation 0 is never written by an append, so it also marks the end of real data —
/// a zero-filled region is otherwise indistinguishable from a valid entry whose point
/// carries a zero timestamp and a zero value, both of which are legal.</para>
///
/// <para>Exemplars are deliberately not logged: they live in a bounded in-memory ring and
/// are not written to cold files either, so replaying them would restore state that a normal
/// flush never persisted.</para>
/// </summary>
internal sealed unsafe class MetricWriteAheadLog : IDisposable
{
    private const uint   MagicNumber     = 0x52_44_4D_57; // "RDMW"
    private const ushort WalVersion      = 1;
    private const int    FileHeaderSize  = 32;
    private const int    EntryHeaderSize = 48;
    private const ulong  FirstGeneration = 1;

    /// <summary>8 MB holds ~150k scalar points; the log is reset on every flush.</summary>
    private const long DefaultCapacity = 8 * 1024 * 1024;

    /// <summary>Bucket counts per histogram point are capped so the 16-bit length field holds.</summary>
    private const int MaxBucketCounts = ushort.MaxValue;

    [StructLayout(LayoutKind.Sequential, Size = FileHeaderSize)]
    private struct WalFileHeader
    {
        public uint   Magic;
        public ushort Version;
        private ushort _pad;
        public long   WriteOffset;
        public ulong  Generation;
        public ulong  CommittedGeneration;
    }

    /// <summary>
    /// Pack = 1 pins the fields at 46 bytes inside the 48-byte stride. Without it the 8-byte
    /// members would align and push the tail past the stride into the payload area.
    /// A 64-bit generation removes any need to reason about wrap-around.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 1, Size = EntryHeaderSize)]
    private struct MetricWalEntryHeader
    {
        public ulong  Generation;        // 0 = unwritten; see the class remarks
        public uint   SeriesIndex;
        public long   TimestampUnixNano;
        public double Value;
        public long   Count;
        public double Sum;
        public ushort BucketCount;       // number of int64 bucket counts that follow
    }

    /// <summary>A point recovered from the log, with the series it belongs to.</summary>
    internal readonly record struct RecoveredPoint(
        string Name, MetricKind Kind, string Unit, LabelSet Labels,
        double[]? Bounds, MetricDataPoint Point);

    private readonly string _filePath;
    private readonly string _poolPath;
    private readonly Lock   _writeLock = new();

    private MemoryMappedFile?         _mmf;
    private MemoryMappedViewAccessor? _accessor;
    private byte*                     _ptr;
    private long                      _capacity;
    private long                      _writeOffset;   // logical, excludes the file header
    private ulong                     _generation;
    private ulong                     _committedGeneration;
    private bool                      _disposed;

    /// <summary>
    /// The generation handed out by <see cref="BeginFlush"/> that has not yet been committed or
    /// abandoned; 0 = none open. A scalar rather than a set, because ONE open flush is the
    /// invariant this class enforces — see the flush-protocol remarks above. In-memory only:
    /// a crash kills every open flush by definition, and every open generation is by
    /// construction ABOVE the persisted watermark, so recovery replays it. Persisting it would
    /// describe flushes that no longer exist.
    /// </summary>
    private ulong _openFlush;

    // Series registry for the CURRENT generation. Cleared on reset together with the pool
    // file, so neither grows across the life of the process.
    private readonly ConcurrentDictionary<SeriesKey, uint> _seriesIndex = new();
    private uint        _nextSeriesIndex;
    private FileStream? _poolStream;

    public string FilePath => _filePath;

    /// <summary>Bytes of point data currently held. Diagnostics and tests only.</summary>
    public long WrittenBytes { get { lock (_writeLock) return _writeOffset; } }

    private MetricWriteAheadLog(string filePath)
    {
        _filePath = filePath;
        _poolPath = filePath + ".pool";
    }

    public static MetricWriteAheadLog Open(string filePath, long initialCapacity = DefaultCapacity)
    {
        var wal = new MetricWriteAheadLog(filePath);
        wal.OpenOrCreate(initialCapacity);
        return wal;
    }

    private void OpenOrCreate(long initialCapacity)
    {
        bool exists   = File.Exists(_filePath);
        long fileSize = FileHeaderSize + initialCapacity;

        using (var fs = new FileStream(_filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
        {
            if (fs.Length < fileSize) fs.SetLength(fileSize);
            else                      fileSize = fs.Length;   // reopen an already-grown log
        }

        _capacity = fileSize - FileHeaderSize;
        Map(fileSize);

        ref var hdr = ref Unsafe.AsRef<WalFileHeader>(_ptr);
        if (!exists || hdr.Magic != MagicNumber || hdr.Version != WalVersion)
        {
            hdr.Magic               = MagicNumber;
            hdr.Version             = WalVersion;
            hdr.WriteOffset         = FileHeaderSize;
            hdr.Generation          = FirstGeneration;
            hdr.CommittedGeneration = 0;
            _writeOffset            = 0;
            _generation             = FirstGeneration;
            _committedGeneration    = 0;
        }
        else
        {
            _writeOffset         = Math.Max(0, hdr.WriteOffset - FileHeaderSize);
            _generation          = hdr.Generation;
            _committedGeneration = hdr.CommittedGeneration;
            if (_writeOffset > _capacity) _writeOffset = _capacity;

            // The counter must LEAD the watermark. BeginFlush only ever hands out _generation
            // and CommitFlush only ever names a generation it was handed, so a header where the
            // two have crossed makes the first commit reclaim nothing and — before CommitFlush
            // was fixed to close the flush first — wedged the log for the life of the process.
            // Both fields sit inside the same 32-byte header, hence the same sector, so a torn
            // write cannot separate them: this is a restored, copied or bit-rotted file. Repair
            // it here rather than carry it, and write the repair back so a crash before the next
            // flush does not re-read the same value. `_committedGeneration + 1` is exactly
            // FirstGeneration when the watermark is 0, which is where the coercion this replaces
            // (`Generation == 0 ? FirstGeneration : …`) landed for the only case it covered.
            if (_generation <= _committedGeneration)
            {
                _generation    = _committedGeneration + 1;
                hdr.Generation = _generation;
            }
        }

        _poolStream = new FileStream(_poolPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        _poolStream.Seek(0, SeekOrigin.End);
    }

    private void Map(long fileSize)
    {
        _mmf      = MemoryMappedFile.CreateFromFile(_filePath, FileMode.Open, null, fileSize, MemoryMappedFileAccess.ReadWrite);
        _accessor = _mmf.CreateViewAccessor(0, fileSize, MemoryMappedFileAccess.ReadWrite);
        _ptr      = null;
        _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _ptr);
    }

    // ── Append ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Logs one data point. The series is registered in the pool on first sight and referred
    /// to by index afterwards, so the per-point cost is a struct store plus the histogram
    /// bucket counts — no managed allocation on the steady-state path.
    /// </summary>
    public void Append(MetricIngestItem item, in MetricDataPoint point)
    {
        var key = new SeriesKey(item.Name ?? string.Empty, item.Kind, item.Unit ?? string.Empty,
                                item.Labels ?? LabelSet.Empty);

        long[]? buckets = point.BucketCounts;
        int bucketCount = buckets is null ? 0 : Math.Min(buckets.Length, MaxBucketCounts);
        int entrySize   = EntryHeaderSize + bucketCount * sizeof(long);

        lock (_writeLock)
        {
            if (_disposed) return;                          // shutdown race — dropping is correct
            if (_ptr is null)
                throw new InvalidOperationException(
                    "Metric WAL has no mapping; the log is not accepting appends.");

            uint seriesIdx = RegisterSeriesLocked(key, item.BucketBounds);

            while (_writeOffset + entrySize > _capacity)
                Grow();

            byte* dest = _ptr + FileHeaderSize + _writeOffset;

            ref var eh = ref Unsafe.AsRef<MetricWalEntryHeader>(dest);
            eh.Generation        = _generation;
            eh.SeriesIndex       = seriesIdx;
            eh.TimestampUnixNano = point.TimestampUnixNano;
            eh.Value             = point.Value;
            eh.Count             = point.Count;
            eh.Sum               = point.Sum;
            eh.BucketCount       = (ushort)bucketCount;

            if (bucketCount > 0)
                buckets.AsSpan(0, bucketCount)
                       .CopyTo(new Span<long>(dest + EntryHeaderSize, bucketCount));

            _writeOffset += entrySize;
            Unsafe.AsRef<WalFileHeader>(_ptr).WriteOffset = FileHeaderSize + _writeOffset;
        }
    }

    /// <summary>Assigns (and persists, once) the pool index for a series. Caller holds the lock.</summary>
    private uint RegisterSeriesLocked(SeriesKey key, double[]? bounds)
    {
        if (_seriesIndex.TryGetValue(key, out uint existing)) return existing;

        uint index = _nextSeriesIndex++;
        _seriesIndex[key] = index;
        WritePoolRecord(index, key, bounds);
        return index;
    }

    private void WritePoolRecord(uint index, SeriesKey key, double[]? bounds)
    {
        if (_poolStream is null) return;

        var body = new ArrayBufferWriterLite();
        body.WriteByte((byte)key.Kind);
        body.WriteString(key.Name);
        body.WriteString(key.Unit);

        var pairs = key.Labels.Pairs;
        body.WriteUInt16((ushort)Math.Min(pairs.Count, ushort.MaxValue));
        for (int i = 0; i < pairs.Count && i < ushort.MaxValue; i++)
        {
            body.WriteString(pairs[i].Key);
            body.WriteString(pairs[i].Value);
        }

        int boundsLen = bounds is null ? 0 : Math.Min(bounds.Length, ushort.MaxValue);
        body.WriteUInt16((ushort)boundsLen);
        for (int i = 0; i < boundsLen; i++) body.WriteDouble(bounds![i]);

        Span<byte> head = stackalloc byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(head, index);
        BinaryPrimitives.WriteUInt32LittleEndian(head[4..], (uint)body.Length);
        _poolStream.Write(head);
        _poolStream.Write(body.Written);
        _poolStream.Flush();
    }

    // ── Reset ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Closes the generation being flushed and opens the next one. Call while holding the
    /// lock that makes the hot-tier snapshot atomic, so that every point in the snapshot has
    /// already been stamped with the returned generation and every point that arrives while
    /// the files are written gets the next one.
    /// </summary>
    /// <returns>The generation the snapshot belongs to — pass it to <see cref="CommitFlush"/>.</returns>
    /// <exception cref="InvalidOperationException">
    /// A flush opened by an earlier <see cref="BeginFlush"/> has neither committed nor been
    /// abandoned. Committing this one would reclaim that one's records while its files are
    /// still being written, which is the loss this guard exists to make impossible; throwing
    /// happens BEFORE the caller's snapshot is drained, so nothing is in flight to lose.
    /// </exception>
    public ulong BeginFlush()
    {
        lock (_writeLock)
        {
            ulong flushing = _generation;
            // Disposed: the generation is handed back WITHOUT being opened or bumped, so two
            // callers racing a shutdown can be given the same value. Registering it would leave
            // an open flush nothing ever closes — CommitFlush returns early here too.
            if (_disposed || _ptr is null) return flushing;

            if (_openFlush != 0)
                throw new InvalidOperationException(
                    $"Metric WAL flush {_openFlush} is still open; a second flush cannot begin " +
                    "until it commits or is abandoned. Committing the later one would reclaim " +
                    "the earlier one's records while its files are still being written.");

            _openFlush  = flushing;
            _generation = flushing + 1;
            Unsafe.AsRef<WalFileHeader>(_ptr).Generation = _generation;
            return flushing;
        }
    }

    /// <summary>
    /// Gives an opened generation back without committing it: the flush failed, its points went
    /// back into the hot tier, and the NEXT flush's snapshot will carry them. Its records stay
    /// in the log below an unchanged watermark, so they are what keeps those points durable
    /// until that later generation commits and covers them.
    ///
    /// <para>The failure path used to signal this by calling nothing at all, which is why the
    /// coupling was invisible. Saying it out loud is also what keeps the one-open-flush guard
    /// from wedging the log after a failed flush.</para>
    /// </summary>
    public void AbandonFlush(ulong flushedGeneration)
    {
        lock (_writeLock)
        {
            // Tolerant of a generation never registered: the disposed path above hands one out
            // without opening it, and callers must be able to abandon unconditionally.
            if (_openFlush == flushedGeneration) _openFlush = 0;
        }
    }

    /// <summary>
    /// Marks everything up to <paramref name="flushedGeneration"/> as durable elsewhere and
    /// reclaims its space. Call only after the files carrying those points are on disk; a
    /// flush that failed must call <see cref="AbandonFlush"/> instead, leaving its records
    /// replayable.
    ///
    /// <para>A generation that is not the currently open flush is REFUSED. That is the direct
    /// statement of the watermark's rule — it may only ever name a generation whose data is
    /// already in files — and it holds even if a caller reintroduces concurrent flushes: the
    /// later one's commit cannot reclaim the earlier one's records.</para>
    ///
    /// <para>Refusing is not the same as leaving the flush open. Exactly one of the two
    /// refusals belongs to somebody else's flush and must not touch it; the other belongs to
    /// the caller's own, whose flush is over whatever the watermark says. Closing the caller's
    /// flush therefore comes FIRST, before any test that can return — see the comment on the
    /// order below for what happened when it did not.</para>
    /// </summary>
    public void CommitFlush(ulong flushedGeneration)
    {
        lock (_writeLock)
        {
            if (_disposed || _ptr is null) return;

            // Not the flush that is writing: reclaims nothing AND leaves the open flush alone,
            // which is the whole point — closing somebody else's flush here is the
            // reclaim-while-writing this class exists to refuse.
            if (flushedGeneration != _openFlush) return;

            // From here the generation is the caller's own, so its flush is over and the flag it
            // set comes down unconditionally. This used to sit BELOW the watermark test, so a
            // commit the watermark already covered returned with the flush still open — and
            // BeginFlush then threw on every later flush, for the life of the process, with the
            // throw landing in a loop that did not catch it. One unusable header cost every
            // metric flush the process would ever do.
            _openFlush = 0;

            // Nothing left to reclaim: the watermark already names this generation or a later
            // one. Only a header that reached this process already crossed can get here
            // (OpenOrCreate normalises what it can see), and the counter climbs past the
            // watermark on the next flush, so the log resumes compacting by itself.
            if (flushedGeneration <= _committedGeneration) return;

            _committedGeneration = flushedGeneration;

            // The watermark lands before a single byte moves: a crash mid-compaction then
            // still replays exactly the survivors, never the points already in files.
            ref var hdr = ref Unsafe.AsRef<WalFileHeader>(_ptr);
            hdr.CommittedGeneration = flushedGeneration;

            Compact(flushedGeneration);
        }
    }

    /// <summary>
    /// Moves entries above the watermark to the front. Generation is non-decreasing in append
    /// order, so the committed entries are a prefix and the survivors one contiguous tail —
    /// a single move, bounded by whatever arrived while the files were being written.
    ///
    /// <para>That physical prefix is a property of the LAYOUT and holds for any watermark
    /// value: the generation is stamped inside <c>_writeLock</c>, is only ever raised inside
    /// the same lock, and the write offset advances under it too, so entries at or below any
    /// threshold occupy a physical prefix whatever order flushes complete in. What the
    /// one-open-flush rule adds is the SEMANTIC half — that the set of generations already in
    /// files has no holes, so a single u64 watermark can express it. Both halves are needed;
    /// this method's single move rests on the first, and its correctness on the second.</para>
    /// </summary>
    private void Compact(ulong committed)
    {
        byte* data = _ptr + FileHeaderSize;

        long firstSurvivor = _writeOffset;
        long pos = 0;
        while (pos + EntryHeaderSize <= _writeOffset)
        {
            ref var eh = ref Unsafe.AsRef<MetricWalEntryHeader>(data + pos);
            long total = (long)EntryHeaderSize + eh.BucketCount * sizeof(long);
            if (total <= 0 || pos + total > _writeOffset) break;
            if (eh.Generation == 0) break;
            if (eh.Generation > committed) { firstSurvivor = pos; break; }
            pos += total;
        }

        long surviving = _writeOffset - firstSurvivor;
        if (surviving > 0 && firstSurvivor > 0)
            Buffer.MemoryCopy(data + firstSurvivor, data, _capacity, surviving);

        _writeOffset = Math.Max(0, surviving);

        // The move does not erase its source. A crash before the offset store below would
        // therefore leave the old, larger offset covering BOTH the relocated survivors and
        // the originals they were copied from, and replay would return each twice. Marking
        // the slot past the new end with generation 0 makes such a scan stop exactly where
        // the data now ends — ReadAll already treats 0 as end-of-data. (No room for the
        // marker means the log is at capacity, where the next append grows it anyway.)
        if (_writeOffset + EntryHeaderSize <= _capacity)
            Unsafe.AsRef<MetricWalEntryHeader>(data + _writeOffset).Generation = 0;

        Unsafe.AsRef<WalFileHeader>(_ptr).WriteOffset = FileHeaderSize + _writeOffset;

        // The pool is only reclaimable once nothing references it. Survivors still carry
        // their series indices, so it is truncated on the flushes that empty the log — which
        // is the normal case — and simply kept otherwise, bounded by the series cardinality.
        if (_writeOffset == 0)
        {
            _seriesIndex.Clear();
            _nextSeriesIndex = 0;
            try
            {
                _poolStream?.SetLength(0);
                _poolStream?.Flush();
            }
            catch { /* best-effort: stale records are tolerated by replay */ }
        }
    }

    // ── Recovery ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Replays every complete point above the committed watermark — that is, everything not
    /// yet known to be in a file, including a snapshot whose flush never completed. Points
    /// whose series is missing from the pool are skipped, since they cannot be reconstructed,
    /// and reported through <paramref name="unresolved"/> rather than silently dropped.
    /// </summary>
    public List<RecoveredPoint> ReadAll(out int unresolved)
    {
        var result = new List<RecoveredPoint>();
        unresolved = 0;

        lock (_writeLock)
        {
            if (_ptr is null) return result;

            var pool = LoadPool();
            long pos = 0;
            long end = _writeOffset;

            while (pos + EntryHeaderSize <= end)
            {
                byte* src = _ptr + FileHeaderSize + pos;
                ref var eh = ref Unsafe.AsRef<MetricWalEntryHeader>(src);

                long total = (long)EntryHeaderSize + eh.BucketCount * sizeof(long);
                if (total <= 0 || pos + total > end) break;   // torn tail
                if (eh.Generation == 0) break;                // end of real data

                if (eh.Generation > _committedGeneration)
                {
                    if (pool.TryGetValue(eh.SeriesIndex, out var series))
                    {
                        long[]? buckets = null;
                        if (eh.BucketCount > 0)
                        {
                            buckets = new long[eh.BucketCount];
                            new ReadOnlySpan<long>(src + EntryHeaderSize, eh.BucketCount).CopyTo(buckets);
                        }

                        result.Add(new RecoveredPoint(
                            series.Name, series.Kind, series.Unit, series.Labels, series.Bounds,
                            new MetricDataPoint
                            {
                                TimestampUnixNano = eh.TimestampUnixNano,
                                Value             = eh.Value,
                                Count             = eh.Count,
                                Sum               = eh.Sum,
                                BucketCounts      = buckets,
                            }));
                    }
                    else unresolved++;
                }

                pos += total;
            }
        }

        return result;
    }

    private readonly record struct PoolEntry(
        string Name, MetricKind Kind, string Unit, LabelSet Labels, double[]? Bounds);

    /// <summary>
    /// Reads the companion pool. Later records win for a given index, which is what makes a
    /// reset that failed to truncate the file harmless.
    /// </summary>
    private Dictionary<uint, PoolEntry> LoadPool()
    {
        var map = new Dictionary<uint, PoolEntry>();
        if (!File.Exists(_poolPath)) return map;

        try
        {
            _poolStream?.Flush();
            using var fs = new FileStream(_poolPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            Span<byte> head = stackalloc byte[8];

            while (fs.Read(head) == 8)
            {
                uint index = BinaryPrimitives.ReadUInt32LittleEndian(head);
                uint len   = BinaryPrimitives.ReadUInt32LittleEndian(head[4..]);
                if (len == 0 || len > 8 * 1024 * 1024) break;    // torn or bogus record

                var body = new byte[len];
                if (fs.Read(body) != len) break;                 // truncated tail

                var r = new SpanCursor(body);
                var kind = (MetricKind)r.ReadByte();
                string name = r.ReadString();
                string unit = r.ReadString();

                int labelCount = r.ReadUInt16();
                var pairs = new KeyValuePair<string, string>[labelCount];
                for (int i = 0; i < labelCount; i++)
                {
                    string k = r.ReadString();
                    string v = r.ReadString();
                    pairs[i] = new KeyValuePair<string, string>(k, v);
                }

                int boundsLen = r.ReadUInt16();
                double[]? bounds = null;
                if (boundsLen > 0)
                {
                    bounds = new double[boundsLen];
                    for (int i = 0; i < boundsLen; i++) bounds[i] = r.ReadDouble();
                }

                map[index] = new PoolEntry(name, kind, unit, new LabelSet(pairs), bounds);
            }
        }
        catch { /* best-effort: whatever resolved stays usable, the rest is reported */ }

        return map;
    }

    // ── Grow ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Doubles the mapped capacity. Windows refuses to resize a mapped file, so the old
    /// mapping must go first — which means a failure here (a full disk, i.e. exactly when a
    /// log grows) could leave the object alive with no mapping, silently refusing every
    /// later append. The old mapping is therefore restored before the exception escapes.
    /// </summary>
    private void Grow()
    {
        long oldFileSize = FileHeaderSize + _capacity;
        long newCapacity = _capacity * 2;
        long newFileSize = FileHeaderSize + newCapacity;

        Unmap();
        try
        {
            using (var fs = new FileStream(_filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                fs.SetLength(newFileSize);

            Map(newFileSize);
            _capacity = newCapacity;
        }
        catch
        {
            try
            {
                using (var fs = new FileStream(_filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                    if (fs.Length < oldFileSize) fs.SetLength(oldFileSize);
                Map(oldFileSize);
            }
            catch { /* nothing left to restore to — the throw below is the honest signal */ }
            throw;
        }
    }

    private void Unmap()
    {
        if (_accessor is not null)
        {
            try { _accessor.SafeMemoryMappedViewHandle.ReleasePointer(); } catch { }
            _accessor.Dispose();
        }
        _mmf?.Dispose();
        _accessor = null;
        _mmf      = null;
        _ptr      = null;
    }

    // ── Dispose ──────────────────────────────────────────────────────────────

    public void Dispose()
    {
        lock (_writeLock)
        {
            if (_disposed) return;
            _disposed = true;
            try { _poolStream?.Flush(); _poolStream?.Dispose(); } catch { }
            _poolStream = null;
            Unmap();
        }
    }

    /// <summary>Closes and removes both files. Used by tests and by a data-directory reset.</summary>
    public void Delete()
    {
        Dispose();
        try { if (File.Exists(_filePath)) File.Delete(_filePath); } catch { }
        try { if (File.Exists(_poolPath)) File.Delete(_poolPath); } catch { }
    }

    // ── Tiny binary helpers ──────────────────────────────────────────────────

    /// <summary>Growable little-endian writer for pool records (cold path, once per series).</summary>
    private sealed class ArrayBufferWriterLite
    {
        private byte[] _buf = new byte[256];
        private int    _len;

        public int                Length  => _len;
        public ReadOnlySpan<byte> Written => _buf.AsSpan(0, _len);

        private Span<byte> Take(int n)
        {
            if (_len + n > _buf.Length) Array.Resize(ref _buf, Math.Max(_buf.Length * 2, _len + n));
            var s = _buf.AsSpan(_len, n);
            _len += n;
            return s;
        }

        public void WriteByte(byte b)      => Take(1)[0] = b;
        public void WriteUInt16(ushort v)  => BinaryPrimitives.WriteUInt16LittleEndian(Take(2), v);
        public void WriteDouble(double v)  => BinaryPrimitives.WriteDoubleLittleEndian(Take(8), v);

        public void WriteString(string? s)
        {
            s ??= string.Empty;
            int n = Math.Min(Encoding.UTF8.GetByteCount(s), ushort.MaxValue);
            WriteUInt16((ushort)n);
            if (n > 0) Encoding.UTF8.GetBytes(s, Take(n));
        }
    }

    /// <summary>Forward-only reader over a pool record body.</summary>
    private ref struct SpanCursor(ReadOnlySpan<byte> data)
    {
        private readonly ReadOnlySpan<byte> _data = data;
        private int _pos;

        public byte ReadByte() => _pos < _data.Length ? _data[_pos++] : (byte)0;

        public ushort ReadUInt16()
        {
            if (_pos + 2 > _data.Length) { _pos = _data.Length; return 0; }
            var v = BinaryPrimitives.ReadUInt16LittleEndian(_data[_pos..]);
            _pos += 2;
            return v;
        }

        public double ReadDouble()
        {
            if (_pos + 8 > _data.Length) { _pos = _data.Length; return 0; }
            var v = BinaryPrimitives.ReadDoubleLittleEndian(_data[_pos..]);
            _pos += 8;
            return v;
        }

        public string ReadString()
        {
            int n = ReadUInt16();
            if (n == 0 || _pos + n > _data.Length) { _pos = Math.Min(_pos + n, _data.Length); return string.Empty; }
            var s = Encoding.UTF8.GetString(_data.Slice(_pos, n));
            _pos += n;
            return s;
        }
    }
}
