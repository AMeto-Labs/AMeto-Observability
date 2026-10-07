using Ameto.Core;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using K4os.Compression.LZ4;
using MessagePack;

namespace Ameto.Metrics.Storage;

/// <summary>
/// Reads <c>.mts</c> files — both the current v3 format (whole-file LZ4-HC
/// section, ms-delta timestamps, kind-aware points) and the legacy v2 format
/// (per-series LZ4 blocks, absolute nanosecond timestamps). v2 files are
/// rewritten to v3 by the background compaction; v1 files are deleted on load.
/// </summary>
internal static class MetricReader
{
    /// <summary>Largest block this reader will decompress. The same ceiling SpanReader uses, and
    /// for the same reason: nothing on disk bounds what an LZ4 payload claims to expand to. The
    /// writer keeps every section it writes at or under it (<c>MetricWriter</c>).</summary>
    internal const int MaxBlockBytes = 64 * 1024 * 1024;

    /// <summary>
    /// THE LARGEST BLOCK BUFFER A READ TAKES FROM, AND GIVES BACK TO, THE SHARED POOL: 8 MiB, the
    /// largest array this process already parks on a routine I/O path (<c>IngestBufferPool</c>'s top
    /// bucket). Above it a buffer is allocated for the read and dropped with it (#125).
    ///
    /// <para>There was no line at all: a block — a WHOLE file's series section, up to
    /// <see cref="MaxBlockBytes"/> — was rented at any size, the pool rounds it up to a power of two
    /// and keeps what it is handed, and on the 512 MB stand a 58 MiB block parked two 64 MiB arrays
    /// there. Why not the writer's megabyte (<see cref="MetricWriter.MaxPooledBytes"/>): the writer's
    /// buffer lives for one flush, a read happens per QUERY — the alert evaluator runs one per rule
    /// every 15 s — and at a megabyte an ordinary 512-series file's block was allocated by every cold
    /// query that met it: <c>MetricQueryAllocProbe</c> measured 93.9 B a stored point against its
    /// 64 B guard. What lies above 8 MiB is a day of a busy histogram's five-minute points or a
    /// legacy file that carried its history forward — read rarely enough to allocate, large enough
    /// that parking it costs the stand real heap.</para>
    /// </summary>
    internal const int MaxPooledBytes = 8 * 1024 * 1024;

    /// <summary>
    /// THE BLOCK BUFFERS ONE REWRITE'S READS SHARE (#125). A rewrite reads its sources once to plan
    /// and again for every chunk, and each read inflates the source's WHOLE block — up to 64 MiB, and
    /// its compressed copy beside it. Allocated afresh per read, a carried-history FiveMin file of
    /// 58 MiB made ~100 MiB of large-object garbage per chunk; under the stand's 384 MiB hard limit
    /// the collector could not hand that back as fast as the next chunk asked for it, and the rewrite
    /// failed with an OutOfMemoryException with its own chunk well inside the budget. With one scratch
    /// the rewrite holds the largest source's two buffers for its whole length — a fixed cost, outside
    /// the chunk budget as a trace pass's read buffers are outside its own — and churns nothing.
    /// Never pooled, never shared between rewrites; grown, never shrunk; dropped with the rewrite.
    /// </summary>
    internal sealed class ReadScratch
    {
        private byte[]? _compressed, _inflated;

        /// <summary>The compressed block's buffer, at least <paramref name="size"/> bytes.</summary>
        public byte[] Compressed(int size) => Fit(ref _compressed, size);

        /// <summary>The inflated block's buffer, at least <paramref name="size"/> bytes.</summary>
        public byte[] Inflated(int size) => Fit(ref _inflated, size);

        private static byte[] Fit(ref byte[]? buffer, int size)
        {
            if (buffer is not null && buffer.Length >= size) return buffer;
            buffer = null;                                  // the smaller one is garbage before the larger is asked for
            return buffer = GC.AllocateUninitializedArray<byte>(size);
        }
    }

    /// <summary>A block buffer back to the shared pool when it came from there — 8 MiB or less; left to the collector above.</summary>
    private static void Give(byte[] buffer)
    {
        if (buffer.Length > MaxPooledBytes) return;
        ArrayPool<byte>.Shared.Return(buffer);
        ReturnedToPoolForTest?.Invoke(buffer.Length);
    }

    /// <summary>
    /// Test seam: the length of every block buffer this reader hands back to the shared pool, on the
    /// reading thread (a read is synchronous on the thread that enumerates it). Null in production.
    /// </summary>
    [ThreadStatic] internal static Action<int>? ReturnedToPoolForTest;

    private const uint   Magic       = 0x52_44_4D_54; // "RDMT"
    private const uint   FooterMagic = 0x52_44_4D_46; // "RDMF"

    /// <summary>
    /// A file's header and name — what the startup scan registers it by. Positioned reads into the
    /// stack (#94): these are 27 bytes at the start, 12 at the end and the name, and they used to
    /// be read through <see cref="OpenRead"/>'s 64 KB buffer, allocated per file — 64 MB for a
    /// thousand files at every start, to read 40 bytes of each. Checked in the order the
    /// <see cref="BinaryReader"/> read them, so a short or foreign file fails where it did; a name
    /// the file ends inside is what <c>ReadBytes</c> gave, the bytes that are there.
    /// </summary>
    public static MetricSegmentInfo ReadSegmentInfo(string filePath)
    {
        using var handle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        long length = RandomAccess.GetLength(handle);

        Span<byte> head = stackalloc byte[27];
        int got = ReadAt(handle, head, 0);
        if (got < 4) throw new EndOfStreamException();
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(head);
        if (magic != Magic) throw new InvalidDataException($"Invalid .mts magic in {filePath}");
        if (got < 6) throw new EndOfStreamException();
        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(head[4..]);
        if (version is not (2 or 3)) throw new InvalidDataException($"Unsupported .mts version {version} in {filePath}");
        if (got < 27) throw new EndOfStreamException();
        var  granularity = (MetricGranularity)head[6];
        long minNano     = BinaryPrimitives.ReadInt64LittleEndian(head[11..]);
        long maxNano     = BinaryPrimitives.ReadInt64LittleEndian(head[19..]);

        // Footer: the name index's offset and the footer magic, the file's last 12 bytes.
        Span<byte> footer = stackalloc byte[12];
        if (length < 12 || ReadAt(handle, footer, length - 12) < 12) throw new EndOfStreamException();
        long nameIdxOffset = (long)BinaryPrimitives.ReadUInt64LittleEndian(footer);
        if (BinaryPrimitives.ReadUInt32LittleEndian(footer[8..]) != FooterMagic) throw new InvalidDataException("Invalid .mts footer magic");

        // Name index: nameCount uint32 | nameLen uint16 | name bytes.
        Span<byte> nameHead = stackalloc byte[6];
        if (nameIdxOffset < 0) throw new IOException($"Name index offset {nameIdxOffset} is before the start of {filePath}");
        if (ReadAt(handle, nameHead, nameIdxOffset) < 6) throw new EndOfStreamException();
        ushort  nameLen = BinaryPrimitives.ReadUInt16LittleEndian(nameHead[4..]);   // 16 bits: the type is the bound
        byte[]? rented  = nameLen > 256 ? ArrayPool<byte>.Shared.Rent(nameLen) : null;
        try
        {
            Span<byte> name = (rented is null ? stackalloc byte[256] : rented)[..nameLen];
            int nameGot = ReadAt(handle, name, nameIdxOffset + 6);
            return new MetricSegmentInfo
            {
                FilePath      = filePath,
                MetricName    = System.Text.Encoding.UTF8.GetString(name[..nameGot]),
                MinNano       = minNano,
                MaxNano       = maxNano,
                Granularity   = granularity,
                FormatVersion = version,
                SizeBytes     = length,
            };
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// A v3 file's series count and the size its series section inflates to, from its header alone
    /// (offsets 7 and 28) — what a rewrite decides, before it reads a point, whether a metric fits
    /// one chunk by (#125). A claim, like every header field: the rewrite only ever uses it to choose
    /// the single-pass path, whose reads are bounded like any other.
    /// </summary>
    internal static (int SeriesCount, long SectionBytes) ReadSectionSize(string filePath)
    {
        using var handle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        Span<byte> head = stackalloc byte[36];
        if (ReadAt(handle, head, 0) < 36) throw new EndOfStreamException($"{filePath} is shorter than a v3 header");
        return ((int)BinaryPrimitives.ReadUInt32LittleEndian(head[7..]), BinaryPrimitives.ReadUInt32LittleEndian(head[28..]));
    }

    /// <summary>Fills <paramref name="buffer"/> from <paramref name="offset"/> until it is full or the file ends; the bytes read.</summary>
    private static int ReadAt(Microsoft.Win32.SafeHandles.SafeFileHandle handle, Span<byte> buffer, long offset)
    {
        int total = 0;
        while (total < buffer.Length)
        {
            int n = RandomAccess.Read(handle, buffer[total..], offset + total);
            if (n == 0) break;
            total += n;
        }
        return total;
    }

    /// <summary>
    /// The series of <paramref name="metricName"/> in one file whose labels pass
    /// <paramref name="labelMatchers"/>, each carrying only its points in
    /// <c>[fromNano, toNano]</c>; a series left with none is not returned.
    ///
    /// <para><b>The range is applied WHILE the points are decoded, not after.</b> This used to
    /// decode every point of every series into a list and then copy the in-range ones into a
    /// second list with <c>Where().ToList()</c> — a full copy of every series even when the whole
    /// series was in range, and a full decode of every point that was not. Now an out-of-range
    /// point is walked (its timestamp delta has to be summed) but never stored, its bucket array is
    /// skipped rather than built (see <see cref="ReadPointsV3"/>), the list the caller gets is the
    /// one the decoder filled, and the label filter is applied as soon as the labels are read —
    /// before the points, which the writer puts after them — so a series the filter rejects costs
    /// its labels and a structural skip, not a decode.</para>
    ///
    /// <para>The name test is made once per file: every series in a <c>.mts</c> carries the one
    /// name in its index, which is what the per-series test compared.</para>
    /// </summary>
    public static IAsyncEnumerable<MetricSeries> ReadAsync(
        string filePath,
        string metricName,
        long   fromNano,
        long   toNano,
        IReadOnlyDictionary<string, string>? labelMatchers,
        CancellationToken ct) =>
        ReadAsync(filePath, metricName, fromNano, toNano, labelMatchers, buckets: true, ct);

    /// <summary>
    /// As the overload above; with <paramref name="buckets"/> false, histogram points come back
    /// without their bucket arrays (<see cref="MetricPointFields.NoBuckets"/>).
    /// </summary>
    public static async IAsyncEnumerable<MetricSeries> ReadAsync(
        string filePath,
        string metricName,
        long   fromNano,
        long   toNano,
        IReadOnlyDictionary<string, string>? labelMatchers,
        bool   buckets,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var series in Read(filePath, metricName, new ReadWindow(fromNano, toNano, labelMatchers, buckets), ct))
            if (series.Points.Count > 0) yield return series;
        await Task.CompletedTask;
    }

    /// <summary>Every series in the file, every point, labels unfiltered.</summary>
    public static IEnumerable<MetricSeries> ReadAllSync(string filePath) =>
        Read(filePath, metricName: null, ReadWindow.All, CancellationToken.None);

    /// <summary>
    /// Every series' IDENTITY — kind, unit and labels — in file order, and nothing else: its bucket
    /// bounds and points are walked past structurally, never decoded, and each series comes back
    /// with no points and no bounds. The catalog seed's read (#94): it decoded every point of every
    /// <c>.mts</c> at startup to learn one timestamp per series, which the file's header already
    /// answers (see <c>MetricStorageEngine.SeedCatalogFromCold</c>). The labels cannot be had
    /// without inflating the block — they live in it — so this is not a header-only read, but it
    /// builds nothing a catalog does not keep.
    /// </summary>
    internal static IEnumerable<MetricSeries> ReadIdentities(string filePath) =>
        Read(filePath, metricName: null,
             new ReadWindow(long.MinValue, long.MaxValue, null, buckets: false, identities: true),
             CancellationToken.None);

    /// <summary>As <see cref="ReadAllSync(string)"/>, with label text looked up in <paramref name="interner"/> — for tests.</summary>
    internal static IEnumerable<MetricSeries> ReadAllSync(string filePath, MetricLabelInterner interner) =>
        Read(filePath, metricName: null, new ReadWindow(long.MinValue, long.MaxValue, null, buckets: true, interner: interner), CancellationToken.None);

    // ── The rewrite's reads (RewriteMetricInChunks) ───────────────────────────

    /// <summary>
    /// The rewrite's single pass over one source when the whole metric fits one chunk: every series in
    /// file order, each with its POSITION (where <see cref="ReadAt"/> finds it again) and its key
    /// index from <paramref name="keyOf"/>, which numbers keys as they are met. Only a series whose key
    /// index is below <paramref name="pointsBelow"/> has its points decoded; every other series is
    /// decoded up to its identity and bucket bounds, and its points are walked past. Points before
    /// <paramref name="fromNano"/> — past the retention cutoff — are walked and never decoded (#125).
    /// </summary>
    internal static IEnumerable<ReadItem> ReadForRewrite(
        string filePath, Func<SeriesKey, int> keyOf, int pointsBelow, long fromNano = long.MinValue,
        ReadScratch? scratch = null) =>
        ReadCore(filePath, metricName: null,
                 new ReadWindow(fromNano, long.MaxValue, null, buckets: true, labels: true, keyOf, pointsBelow),
                 at: null, CancellationToken.None, scratch);

    /// <summary>
    /// The rewrite's PLANNING pass over one source (#125): every series in file order with its
    /// position, its encoded length, its key index, its bucket bounds — and, instead of its points,
    /// their <see cref="ReadItem.Stats"/>: how many there are, how many lie at or after
    /// <paramref name="fromNano"/>, what the bucket arrays a decode of those would build weigh, and
    /// when the first and last of them fall. The points are walked, with every check a decode makes,
    /// and nothing is built: the rewrite plans its chunks by this before it holds a single point.
    /// </summary>
    internal static IEnumerable<ReadItem> PlanForRewrite(string filePath, Func<SeriesKey, int> keyOf, long fromNano = long.MinValue,
                                                         ReadScratch? scratch = null) =>
        ReadCore(filePath, metricName: null,
                 new ReadWindow(fromNano, long.MaxValue, null, buckets: true, labels: true, keyOf, pointsBelow: 0, stats: true),
                 at: null, CancellationToken.None, scratch);

    /// <summary>
    /// The series at <paramref name="positions"/> (as <see cref="PlanForRewrite"/> or
    /// <see cref="ReadForRewrite"/> reported them, ascending), in that order, with their points in
    /// <c>[fromNano, toNano]</c> and their bucket bounds — and NOT their labels or unit, which the
    /// rewrite already holds from its first pass. A v3 file is inflated once and each series decoded
    /// where it starts; a v2 file's other series are not even inflated.
    /// </summary>
    internal static IEnumerable<ReadItem> ReadAt(string filePath, List<long> positions,
                                                  long fromNano = long.MinValue, long toNano = long.MaxValue,
                                                  ReadScratch? scratch = null) =>
        ReadCore(filePath, metricName: null,
                 new ReadWindow(fromNano, toNano, null, buckets: true, labels: false),
                 at: positions, CancellationToken.None, scratch);

    /// <summary>
    /// One series as a rewrite read meets it: where it sits (for v3 its offset in the inflated block,
    /// for v2 its block's offset in the file), how many bytes it is encoded in, its key index (-1
    /// outside a rewrite), the series itself, and — from <see cref="PlanForRewrite"/> — what its
    /// points would weigh decoded.
    /// </summary>
    internal readonly record struct ReadItem(long Position, int Length, int Key, MetricSeries Series, PointStats Stats);

    /// <summary>
    /// What a series' points come to without being built (<see cref="PlanForRewrite"/>). Every read
    /// fills <see cref="Walked"/>; the rest only a planning read.
    /// </summary>
    internal struct PointStats
    {
        /// <summary>Every point the series holds in this file.</summary>
        public int  Walked;

        /// <summary>The points inside the read's window.</summary>
        public int  Kept;

        /// <summary>The bucket arrays a decode of the kept points builds: 24 + 8 × length bytes each.</summary>
        public long BucketBytes;

        /// <summary>The first and last kept timestamp; <see cref="long.MaxValue"/> / <see cref="long.MinValue"/> when none is kept.</summary>
        public long MinKept, MaxKept;
    }

    /// <summary>
    /// What a read keeps: points in <c>[FromNano, ToNano]</c> (inclusive, as the range test always
    /// was), from series whose labels pass <see cref="Matchers"/> when there are any.
    /// </summary>
    internal readonly struct ReadWindow(
        long fromNano, long toNano, IReadOnlyDictionary<string, string>? matchers, bool buckets,
        bool labels = true, Func<SeriesKey, int>? keyOf = null, int pointsBelow = int.MaxValue,
        MetricLabelInterner? interner = null, bool identities = false, bool stats = false)
    {
        public static ReadWindow All => new(long.MinValue, long.MaxValue, null, buckets: true);

        /// <summary>Only kind, unit and labels are decoded; bounds and points are skipped (<see cref="ReadIdentities"/>).</summary>
        public bool Identities { get; } = identities;

        /// <summary>Points are walked into <see cref="PointStats"/> and not decoded (<see cref="PlanForRewrite"/>).</summary>
        public bool Stats { get; } = stats;

        public long FromNano { get; } = fromNano;
        public long ToNano   { get; } = toNano;
        public IReadOnlyDictionary<string, string>? Matchers { get; } = matchers;

        /// <summary>Whether histogram points get their bucket arrays. False for a caller that said it
        /// will not read them (<see cref="MetricPointFields.NoBuckets"/>): the arrays are walked past
        /// with every check a build makes, and nothing is built.</summary>
        public bool Buckets { get; } = buckets;

        /// <summary>Whether labels and unit are decoded. False only for <see cref="ReadAt"/>.</summary>
        public bool Labels { get; } = labels;

        /// <summary>The rewrite's key numbering (<see cref="ReadForRewrite"/>); null for every other read.</summary>
        public Func<SeriesKey, int>? KeyOf { get; } = keyOf;

        /// <summary>With <see cref="KeyOf"/>: points are decoded only for key indices below this.</summary>
        public int PointsBelow { get; } = pointsBelow;

        /// <summary>Where label text is looked up (never added): <see cref="MetricLabelInterner.Shared"/>
        /// unless a test hands its own.</summary>
        public MetricLabelInterner Interner { get; } = interner ?? MetricLabelInterner.Shared;

        public bool Keeps(long ts) => ts >= FromNano && ts <= ToNano;

        /// <summary>
        /// The share of a file spanning [min, max] (its header's range) that the window keeps: 1 when
        /// it covers the file, else the overlapping fraction of the span. A SIZING HINT for a series'
        /// point list and nothing else — a v3 point reads back up to a millisecond below the header's
        /// min, and a series may span less than its file — so no answer depends on it.
        /// </summary>
        public double Share(long minNano, long maxNano)
        {
            if (FromNano == long.MinValue ? ToNano >= maxNano : FromNano <= minNano - 1_000_000 && ToNano >= maxNano) return 1;
            if (maxNano <= minNano) return 1;
            long lo = Math.Max(FromNano, minNano), hi = Math.Min(ToNano, maxNano);
            if (hi < lo) return 0;
            return ((double)hi - lo + 1) / ((double)maxNano - minNano + 1);
        }
    }

    private static IEnumerable<MetricSeries> Read(string filePath, string? metricName, ReadWindow window, CancellationToken ct)
    {
        foreach (var item in ReadCore(filePath, metricName, window, at: null, ct))
            yield return item.Series;
    }

    /// <summary>
    /// The one reading loop: every series of the file in order (<paramref name="at"/> null), or the
    /// series at the given positions — for v3 an offset in the inflated block, for v2 the file
    /// offset of the series' own block. Each series comes as a <see cref="ReadItem"/>: its position,
    /// its encoded length, its rewrite key index (-1 outside a rewrite read) and its point stats.
    /// </summary>
    private static IEnumerable<ReadItem> ReadCore(
        string filePath, string? metricName, ReadWindow window, List<long>? at, CancellationToken ct,
        ReadScratch? scratch = null)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        uint magic = br.ReadUInt32();
        if (magic != Magic) yield break;

        ushort version = br.ReadUInt16();
        if (version is not (2 or 3)) yield break; // v1 — incompatible, skipped (deleted on load)
        br.ReadByte();   // granularity
        int seriesCount = (int)br.ReadUInt32();
        long minNano = br.ReadInt64();
        long maxNano = br.ReadInt64();
        br.ReadByte();   // flags

        long nameIdxOffset = ReadNameIdxOffset(fs, br);

        // Read metric name
        fs.Seek(nameIdxOffset, SeekOrigin.Begin);
        br.ReadUInt32(); // nameCount
        ushort nameLen = br.ReadUInt16();
        string fileMetric = System.Text.Encoding.UTF8.GetString(br.ReadBytes(nameLen));

        // One name per file, so the per-series name test is a per-file one. (The engine only asks
        // for a file its catalog already matched by this name; a direct caller asking for another
        // gets the same nothing, without the file being inflated to find that out.)
        if (metricName is not null && !fileMetric.Equals(metricName, StringComparison.OrdinalIgnoreCase)) yield break;

        double share = window.Share(minNano, maxNano);

        // Reset to after header (28 bytes)
        fs.Seek(28, SeekOrigin.Begin);

        if (version == 3)
        {
            // One LZ4 block holding every series back to back. Series are decoded one
            // at a time — materialising the whole section would make the caller's peak
            // the file's series count, which is unbounded in files written before the
            // 512-series cap (exactly what a high-cardinality deployment has on disk).
            // THE SAME RULE THE TRACE READERS FOLLOW, and this file was outside the scan that
            // enforces it — which is exactly how it kept its unbounded rents while nine sites one
            // project over were being fixed round after round. A compressed size taken from an
            // untrusted .mts header is a rent of that size before anything discovers the file is
            // shorter, and ArrayPool keeps a large bucket committed for the life of the process.
            br.ReadUInt32(); // uncompSize
            uint compSize = br.ReadUInt32();
            FileBounds.RequireLengthFits(compSize, fs.Length - fs.Position, "Series block", filePath);

            // POOLED UP TO 8 MiB AND NO FURTHER (#125, see MaxPooledBytes). A block is a WHOLE file's
            // series section, up to MaxBlockBytes, and ArrayPool rounds it up to a power of two: a
            // 58 MiB block took two 64 MiB arrays from the shared pool, and the pool kept both after
            // the read — 128 MiB of gen2 parked for a caller that may never come, on a box whose whole
            // heap is 384 MiB. A block above the line is allocated for the read and left to the collector.
            // A rewrite brings its own buffers instead (ReadScratch), which this read neither pools nor drops.
            byte[] comp = scratch is not null
                ? scratch.Compressed((int)compSize)
                : compSize <= MaxPooledBytes ? ArrayPool<byte>.Shared.Rent((int)compSize) : GC.AllocateUninitializedArray<byte>((int)compSize);
            bool   compHeld = scratch is null;
            byte[]? raw  = null;
            try
            {
                fs.ReadExactly(comp, 0, (int)compSize);
                // The length INSIDE the payload, which the check above never saw: LZ4 carries the
                // decompressed size in its own header, so a short well-formed block can still ask
                // for gigabytes.
                int rawLen = LZ4Pickler.UnpickledSize(comp.AsSpan(0, (int)compSize));
                FileBounds.RequireLengthFits(rawLen, MaxBlockBytes, "Series block uncompressed", filePath);
                raw = scratch is not null
                    ? scratch.Inflated(rawLen)
                    : rawLen <= MaxPooledBytes ? ArrayPool<byte>.Shared.Rent(rawLen) : GC.AllocateUninitializedArray<byte>(rawLen);
                LZ4Pickler.Unpickle(comp.AsSpan(0, (int)compSize), raw.AsSpan(0, rawLen));

                // The compressed copy has done its work: given back BEFORE the series are walked, so
                // a read holds one block and not two for as long as its caller takes over the series
                // (a rewrite decodes and accumulates a whole chunk while this iterator is suspended).
                if (compHeld)
                {
                    Give(comp);
                    compHeld = false;
                }

                if (at is null)
                {
                    int offset = 0;
                    for (int i = 0; i < seriesCount && offset < rawLen; i++)
                    {
                        ct.ThrowIfCancellationRequested();
                        int start  = offset;
                        var series = DeserializeNext(fileMetric, raw, offset, rawLen, deltaMs: true, in window, share,
                                                     out offset, out int key, out PointStats stats);
                        if (series is not null) yield return new ReadItem(start, offset - start, key, series, stats);
                    }
                }
                else
                {
                    foreach (long position in at)
                    {
                        ct.ThrowIfCancellationRequested();
                        if (position < 0 || position >= rawLen)
                            throw new InvalidDataException($"Series position {position} is outside the block of {filePath}");
                        var series = DeserializeNext(fileMetric, raw, (int)position, rawLen, deltaMs: true, in window, share,
                                                     out int next, out int key, out PointStats stats);
                        if (series is not null) yield return new ReadItem(position, next - (int)position, key, series, stats);
                    }
                }
            }
            finally
            {
                if (compHeld) Give(comp);
                if (raw is not null && scratch is null) Give(raw);
            }
        }
        else
        {
            // v2: per-series LZ4 blocks — walked in order, or sought one by one.
            int visited = 0;
            while (true)
            {
                long position;
                if (at is null)
                {
                    if (visited >= seriesCount || fs.Position >= nameIdxOffset) break;
                    position = fs.Position;
                }
                else
                {
                    if (visited >= at.Count) break;
                    position = at[visited];
                    fs.Seek(position, SeekOrigin.Begin);
                }
                int i = visited++;
                ct.ThrowIfCancellationRequested();

                br.ReadUInt32(); // uncompSize
                uint compSize = br.ReadUInt32();
                FileBounds.RequireLengthFits(compSize, fs.Length - fs.Position, $"Series {i} block", filePath);

                // Pooled up to 8 MiB, allocated above, or the rewrite's own — see the v3 block above.
                byte[] comp = scratch is not null
                    ? scratch.Compressed((int)compSize)
                    : compSize <= MaxPooledBytes ? ArrayPool<byte>.Shared.Rent((int)compSize) : GC.AllocateUninitializedArray<byte>((int)compSize);
                byte[]? raw = null;
                MetricSeries? series;
                int key, length;
                PointStats stats;
                try
                {
                    fs.ReadExactly(comp, 0, (int)compSize);
                    int rawLen = LZ4Pickler.UnpickledSize(comp.AsSpan(0, (int)compSize));
                    FileBounds.RequireLengthFits(rawLen, MaxBlockBytes, $"Series {i} uncompressed", filePath);
                    raw = scratch is not null
                        ? scratch.Inflated(rawLen)
                        : rawLen <= MaxPooledBytes ? ArrayPool<byte>.Shared.Rent(rawLen) : GC.AllocateUninitializedArray<byte>(rawLen);
                    LZ4Pickler.Unpickle(comp.AsSpan(0, (int)compSize), raw.AsSpan(0, rawLen));
                    series = DeserializeNext(fileMetric, raw, 0, rawLen, deltaMs: false, in window, share, out length, out key, out stats);
                }
                finally
                {
                    if (scratch is null)
                    {
                        Give(comp);
                        if (raw is not null) Give(raw);
                    }
                }

                if (series is not null) yield return new ReadItem(position, length, key, series, stats);
            }
        }
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Decodes the series starting at <paramref name="offset"/> and reports where the next
    /// one begins. Non-iterator by necessity: <see cref="MessagePackReader"/> is a ref struct
    /// and cannot live across a <c>yield</c>, so the cursor is carried out as a plain int and
    /// the reader is rebuilt per call (it is a span wrapper — no allocation). Null when the
    /// window's label filter rejects the series.
    /// </summary>
    private static MetricSeries? DeserializeNext(
        string metricName, byte[] raw, int offset, int length, bool deltaMs,
        in ReadWindow window, double share, out int next, out int key, out PointStats stats)
    {
        var r = new MessagePackReader(new ReadOnlyMemory<byte>(raw, offset, length - offset));
        var series = DeserializeSeries(metricName, ref r, deltaMs, in window, share, out key, out stats);
        next = offset + (int)r.Consumed;
        return series;
    }

    private static MetricSeries? DeserializeSeries(
        string metricName, ref MessagePackReader r, bool deltaMs, in ReadWindow window, double share,
        out int keyIndex, out PointStats stats)
    {
        int fields = r.ReadMapHeader();

        MetricKind kind    = MetricKind.Counter;
        string     unit    = string.Empty;
        LabelSet   labels  = LabelSet.Empty;
        double[]?  bounds  = null;
        List<MetricDataPoint>? points = null;
        keyIndex = -1;
        stats    = new PointStats { MinKept = long.MaxValue, MaxKept = long.MinValue };

        // The rewrite's key can be taken once kind, unit and labels are in hand — which, in the
        // order the writer emits a series (k, u, lbs, bnds, pts, cnt), is before its points. A map
        // in any other order is still read correctly: its points are decoded and its key is taken
        // at the end.
        const int HaveKind = 1, HaveUnit = 2, HaveLabels = 4, HaveIdentity = HaveKind | HaveUnit | HaveLabels;
        int have = 0;

        for (int i = 0; i < fields; i++)
        {
            // The map key compared as the UTF-8 bytes it is on disk. ReadString() materialised a
            // string per key — six per series (k, u, lbs, bnds, pts, cnt), every one garbage the
            // moment the switch had looked at it. A nil key read as null there and matched no
            // case; here it is the empty span and matches none either — its value is skipped.
            ReadOnlySpan<byte> key = ReadKey(ref r);
            if (key.SequenceEqual("k"u8))
            {
                kind  = (MetricKind)r.ReadByte();
                have |= HaveKind;
            }
            else if (key.SequenceEqual("u"u8))
            {
                if (window.Labels) unit = ReadPooled(ref r, window.Interner, out _);
                else               r.Skip();
                have |= HaveUnit;
            }
            else if (key.SequenceEqual("lbs"u8))
            {
                if (!window.Labels) { r.Skip(); continue; }
                labels = ReadLabels(ref r, window.Interner);
                have  |= HaveLabels;
                // Rejected here, before the points the writer puts after the labels: the rest of
                // the map is walked, not decoded, so the reader ends where the next series starts.
                if (window.Matchers is not null && !MatchesLabels(labels, window.Matchers))
                {
                    for (int j = i + 1; j < fields; j++) { r.Skip(); r.Skip(); }
                    return null;
                }
            }
            else if (key.SequenceEqual("bnds"u8))
            {
                if (window.Identities) r.Skip();
                else bounds = ReadBounds(ref r);
            }
            else if (key.SequenceEqual("pts"u8))
            {
                if (window.Identities) { r.Skip(); continue; }
                if (window.KeyOf is { } keyOf && (have & HaveIdentity) == HaveIdentity)
                {
                    keyIndex = keyOf(new SeriesKey(metricName, kind, unit, labels));
                    if (!window.Stats && keyIndex >= window.PointsBelow) { r.Skip(); continue; }   // not this pass's
                }
                if (window.Stats)
                {
                    if (deltaMs) StatPointsV3(ref r, in window, ref stats);
                    else         StatPointsV2(ref r, in window, ref stats);
                    continue;
                }
                points = deltaMs ? ReadPointsV3(ref r, in window, share, out stats.Walked)
                                 : ReadPointsV2(ref r, in window, share, out stats.Walked);
                stats.Kept = points.Count;
            }
            else r.Skip();
        }

        // Identity only: no list for the points it never read (Points is the shared empty default).
        if (window.Identities)
            return new MetricSeries { Name = metricName, Kind = kind, Unit = unit, Labels = labels };

        points ??= [];
        if (window.KeyOf is { } lateKeyOf && keyIndex < 0)
            keyIndex = lateKeyOf(new SeriesKey(metricName, kind, unit, labels));

        // v3 stores idle histogram points (count=0, sum=0, all buckets 0) in the
        // slim scalar shape; reconstruct their all-zero bucket arrays here so the
        // roundtrip is lossless and delta chains in the aggregator stay intact.
        // One shared array per series — nothing downstream mutates BucketCounts.
        if (deltaMs && window.Buckets && kind == MetricKind.Histogram && bounds is not null && points.Count > 0)
        {
            long[]? zeros = null;
            for (int i = 0; i < points.Count; i++)
            {
                var p = points[i];
                if (p.BucketCounts is not null || p.Count != 0 || p.Sum != 0) continue;
                zeros   ??= new long[bounds.Length + 1];
                points[i] = new MetricDataPoint
                {
                    TimestampUnixNano = p.TimestampUnixNano,
                    Value             = p.Value,
                    BucketCounts      = zeros,
                };
            }
        }

        return new MetricSeries
        {
            Name         = metricName,
            Kind         = kind,
            Unit         = unit,
            Labels       = labels,
            BucketBounds = bounds,
            Points       = points,
        };
    }

    /// <summary>
    /// A map key's UTF-8 bytes, in place in the decompressed block — no string. Nil answers the
    /// empty span (ReadString's null). A key that is not a string throws, as ReadString did. The
    /// block is one contiguous buffer, so the span read always succeeds; the copy below is only
    /// for a reader over a segmented sequence, which nothing here builds.
    /// </summary>
    private static ReadOnlySpan<byte> ReadKey(ref MessagePackReader r)
    {
        if (r.TryReadNil()) return default;
        if (r.TryReadStringSpan(out ReadOnlySpan<byte> key)) return key;
        return r.ReadStringSequence() is { } seq ? seq.ToArray() : default;
    }

    private static double[]? ReadBounds(ref MessagePackReader r)
    {
        int count = r.ReadArrayHeader();
        if (count == 0) return null;
        // A header is a claim, not a measurement: one byte is the least a double can occupy in
        // MessagePack, so nothing in the block can hold more than its own remaining bytes. The
        // reservation is capped below that and grown as the values actually arrive, because a
        // count that survives the check can still be far larger than anything real.
        FileBounds.RequireCountFits(count, r.Sequence.Length - r.Consumed,
            fileBytesPerElement: 1, "Bucket bounds", "the series block");
        var b = new double[FileBounds.PreallocFor(count, heapBytesPerElement: sizeof(double))];
        for (int i = 0; i < count; i++)
        {
            if (i == b.Length) Array.Resize(ref b, Math.Min(count, Math.Max(4, b.Length * 2)));
            b[i] = r.ReadDouble();
        }
        return b;
    }

    private static LabelSet ReadLabels(ref MessagePackReader r, MetricLabelInterner interner)
    {
        int count = r.ReadMapHeader();
        // THE SAME RULE AS EVERY OTHER READER HERE, and this site went four rounds without it for a
        // reason worth naming: the convention test that enforces the rule could not SEE this line.
        // Its pattern read a generic argument list with `[^>]*`, which stops at the first `>`, so
        // `List<KeyValuePair<string, string>>` matched nothing at all — the file was scanned, the
        // shape was there, and the scanner walked past it. That blindness is fixed in
        // FileBoundsConventionTests; this is the allocation it was hiding.
        //
        // Two bounds, two questions. Could the file hold this many pairs — a pair is two msgpack
        // strings and the shortest legal one is a byte each, so two bytes on disk. And what may be
        // reserved up front for a count that passes: a map header torn to int.MaxValue was believed
        // whole and asked for 2.1 billion slots, 34 GB of references, before one pair was read.
        FileBounds.RequireCountFits(count, r.Sequence.Length - r.Consumed,
            fileBytesPerElement: 2, "Label set", "the series block");
        if (count > MaxInternedLabelStrings / 2) return ReadLabelsUninterned(ref r, count);

        // RESOLVED AGAINST THE SHARED INTERNER, LOOKUP ONLY. The OTLP parsers and the WAL replay
        // put live text into MetricLabelInterner.Shared; the series a query reads back are, almost
        // always, series ingest is still sending, so their strings and label sets are already
        // there, and a read that finds them hands back those instances and allocates nothing for
        // them. A read that does NOT find them builds its own — and adds nothing: the pool never
        // evicts, and a cold read is the one reader that meets every DEAD value of the retention
        // window (the catalog seed reads every .mts at startup), so interning here filled the
        // pool at boot and left every series started afterwards to be ingested uninterned.
        // Equal by value either way.
        int n        = count * 2;
        var kv       = ArrayPool<string>.Shared.Rent(MaxInternedLabelStrings);
        Span<int> ids = stackalloc int[MaxInternedLabelStrings];
        try
        {
            for (int i = 0; i < n; i++) kv[i] = ReadPooled(ref r, interner, out ids[i]);
            return interner.LookupLabelSet(kv.AsSpan(0, n), ids[..n]);
        }
        finally
        {
            kv.AsSpan(0, n).Clear();
            ArrayPool<string>.Shared.Return(kv);
        }
    }

    /// <summary>
    /// Strings in the largest label set this reader resolves through the interner. A set bigger
    /// than that — legal, never seen from a real exporter — is decoded the way every set used to
    /// be, so the scratch the common case rents stays a constant.
    /// </summary>
    private const int MaxInternedLabelStrings = 128;

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static LabelSet ReadLabelsUninterned(ref MessagePackReader r, int count)
    {
        FileBounds.RequireCountFits(count, r.Sequence.Length - r.Consumed,
            fileBytesPerElement: 2, "Label set", "the series block");
        int cap   = FileBounds.PreallocFor(count, heapBytesPerElement: 16);
        var pairs = new List<KeyValuePair<string, string>>(cap);
        for (int i = 0; i < count; i++)
        {
            var k = r.ReadString() ?? string.Empty;
            var v = r.ReadString() ?? string.Empty;
            pairs.Add(new KeyValuePair<string, string>(k, v));
        }
        return new LabelSet(pairs);
    }

    /// <summary>
    /// A msgpack string looked up in <paramref name="interner"/> from its UTF-8 bytes in place —
    /// the pooled instance and its id when the text is live, else a fresh string and -1, and
    /// nothing added (see <see cref="MetricLabelInterner.Lookup"/>). The text is what
    /// <c>ReadString() ?? string.Empty</c> gave; nil is the empty string, as it was.
    /// </summary>
    private static string ReadPooled(ref MessagePackReader r, MetricLabelInterner interner, out int id)
    {
        if (r.TryReadNil()) { id = MetricLabelInterner.EmptyStringId; return string.Empty; }
        string value;
        if (r.TryReadStringSpan(out ReadOnlySpan<byte> utf8)) id = interner.Lookup(utf8, out value);
        else { value = r.ReadString() ?? string.Empty; id = -1; }   // a segmented sequence: never built here
        return value;
    }

    /// <summary>
    /// v3 points: ms-delta timestamps; a 2-element (slim) point inherits the
    /// previous point's histogram state (count / sum / buckets) — for scalar
    /// series that state is always zero, for histograms it run-length-encodes
    /// idle stretches losslessly.
    ///
    /// <para><b>Only the points in the window are stored.</b> Every point is still walked — a
    /// timestamp is a running sum of deltas, and a slim point's state is the last full point's —
    /// but one outside the window is not added, and its bucket array is not BUILT: its position
    /// is remembered instead, and it is materialised only if a later slim point that IS in the
    /// window inherits it. So the kept points see exactly the state they always did, sharing one
    /// array per full point as they always did, and a point nobody asked for costs no
    /// allocation.</para>
    /// </summary>
    private static List<MetricDataPoint> ReadPointsV3(ref MessagePackReader r, in ReadWindow window, double share, out int walked)
    {
        // A MessagePack array header is a number out of the file like any other: the block it
        // lives in is bounded, but the header can still claim far more points than the block
        // holds, and a capacity is reserved before a single one is read. Reserve modestly and
        // let the list grow into whatever is really there — scaled by the share of the file the
        // window keeps (ReadWindow.Share): the header counts every point, and a window that
        // keeps a third of the file reserving for all of it wastes two thirds, while reserving
        // nothing pays the list's doubling copies instead.
        int count  = r.ReadArrayHeader();
        walked     = count;
        var pts    = new List<MetricDataPoint>(FileBounds.PreallocFor(share >= 1 ? count : (long)(count * share) + 1, heapBytesPerElement: 64));
        long ms    = 0;
        long    cnt = 0;
        double  sum = 0;
        long[]? buckets   = null;
        long    pendingAt = -1;   // the state's bucket array, walked past but not yet built
        for (int i = 0; i < count; i++)
        {
            int n = r.ReadArrayHeader(); // 2 = slim (state unchanged), 5 = full
            ms = i == 0 ? r.ReadInt64() : ms + r.ReadInt64();
            long ts   = ms * 1_000_000;
            bool keep = window.Keeps(ts);

            double val = r.ReadDouble(); // transparently accepts msgpack ints
            if (n >= 5)
            {
                cnt = r.ReadInt64();
                sum = r.ReadDouble();
                pendingAt = -1;
                if (r.TryReadNil())
                {
                    buckets = null; // state set but no buckets recorded
                }
                else if (keep && window.Buckets)
                {
                    buckets = ReadBuckets(ref r);
                }
                else
                {
                    buckets   = null;
                    pendingAt = window.Buckets ? r.Consumed : -1;
                    SkipBuckets(ref r);
                }
            }
            if (!keep) continue;

            if (pendingAt >= 0)
            {
                var at  = new MessagePackReader(r.Sequence.Slice(pendingAt));
                buckets = ReadBuckets(ref at);
                pendingAt = -1;
            }
            pts.Add(new MetricDataPoint
            {
                TimestampUnixNano = ts,
                Value             = val,
                Count             = cnt,
                Sum               = sum,
                BucketCounts      = buckets, // shared with the previous point when slim — nothing mutates it
            });
        }
        return pts;
    }

    /// <summary>
    /// <see cref="ReadPointsV3"/>'s walk with nothing built (#125): the same reads, the same checks
    /// on every field and bucket array — so a series a decode would fail on fails here too — and the
    /// same rule for which bucket arrays a decode of the window's points would BUILD: one per full
    /// point in the window, and one more when a point in the window inherits the state of a full
    /// point outside it (the decode's pending array). The rewrite plans its chunks by what this adds up.
    /// </summary>
    private static void StatPointsV3(ref MessagePackReader r, in ReadWindow window, ref PointStats stats)
    {
        int  count      = r.ReadArrayHeader();
        long ms         = 0;
        int  stateLen   = -1;      // the state's bucket array length; -1: the state has none
        bool stateBuilt = false;   // whether a decode has built the state's array already
        stats.Walked = count;
        for (int i = 0; i < count; i++)
        {
            int n = r.ReadArrayHeader(); // 2 = slim (state unchanged), 5 = full
            ms = i == 0 ? r.ReadInt64() : ms + r.ReadInt64();
            long ts   = ms * 1_000_000;
            bool keep = window.Keeps(ts);

            r.ReadDouble();
            if (n >= 5)
            {
                r.ReadInt64();
                r.ReadDouble();
                stateBuilt = false;
                if (r.TryReadNil()) stateLen = -1;
                else
                {
                    stateLen = WalkBuckets(ref r);
                    if (keep && window.Buckets) { stats.BucketBytes += 24 + 8L * stateLen; stateBuilt = true; }
                }
            }
            if (!keep) continue;

            if (stateLen >= 0 && !stateBuilt && window.Buckets)
            {
                stats.BucketBytes += 24 + 8L * stateLen;
                stateBuilt = true;
            }
            stats.Kept++;
            if (ts < stats.MinKept) stats.MinKept = ts;
            if (ts > stats.MaxKept) stats.MaxKept = ts;
        }
    }

    /// <summary><see cref="ReadPointsV2"/>'s walk with nothing built: every kept point with buckets builds its own array.</summary>
    private static void StatPointsV2(ref MessagePackReader r, in ReadWindow window, ref PointStats stats)
    {
        int count = r.ReadArrayHeader();
        stats.Walked = count;
        for (int i = 0; i < count; i++)
        {
            int n = r.ReadArrayHeader();
            long ts = r.ReadInt64();
            r.ReadDouble();
            r.ReadInt64();
            r.ReadDouble();
            bool keep = window.Keeps(ts);
            if (n >= 5 && !r.TryReadNil())
            {
                int bn = WalkBuckets(ref r);
                if (keep && window.Buckets) stats.BucketBytes += 24 + 8L * bn;
            }
            if (!keep) continue;
            stats.Kept++;
            if (ts < stats.MinKept) stats.MinKept = ts;
            if (ts > stats.MaxKept) stats.MaxKept = ts;
        }
    }

    /// <summary>A bucket array walked as <see cref="SkipBuckets"/> walks it; its length.</summary>
    private static int WalkBuckets(ref MessagePackReader r)
    {
        int bn = r.ReadArrayHeader();
        FileBounds.RequireCountFits(bn, r.Sequence.Length - r.Consumed,
            fileBytesPerElement: 1, "Histogram buckets", "the series block");
        for (int j = 0; j < bn; j++) r.ReadInt64();
        return bn;
    }

    /// <summary>v2 points: absolute nanosecond timestamps; always 5 fields. Only the window's are stored.</summary>
    private static List<MetricDataPoint> ReadPointsV2(ref MessagePackReader r, in ReadWindow window, double share, out int walked)
    {
        int count = r.ReadArrayHeader();
        walked    = count;
        var pts   = new List<MetricDataPoint>(FileBounds.PreallocFor(share >= 1 ? count : (long)(count * share) + 1, heapBytesPerElement: 64));
        for (int i = 0; i < count; i++)
        {
            int n = r.ReadArrayHeader(); // 5 fields in v2
            long   ts  = r.ReadInt64();
            double val = r.ReadDouble();
            long   cnt = r.ReadInt64();
            double sum = r.ReadDouble();
            bool   keep = window.Keeps(ts);
            long[]? buckets = null;
            if (n >= 5)
            {
                if (r.TryReadNil())
                {
                    // scalar point — no buckets
                }
                else if (keep && window.Buckets)
                {
                    buckets = ReadBuckets(ref r);
                }
                else
                {
                    SkipBuckets(ref r);
                }
            }
            if (!keep) continue;
            pts.Add(new MetricDataPoint
            {
                TimestampUnixNano = ts,
                Value             = val,
                Count             = cnt,
                Sum               = sum,
                BucketCounts      = buckets,
            });
        }
        return pts;
    }

    /// <summary>A point's bucket-count array, built.</summary>
    private static long[] ReadBuckets(ref MessagePackReader r)
    {
        int bn = r.ReadArrayHeader();
        FileBounds.RequireCountFits(bn, r.Sequence.Length - r.Consumed,
            fileBytesPerElement: 1, "Histogram buckets", "the series block");
        var bk = new long[FileBounds.PreallocFor(bn, heapBytesPerElement: sizeof(long))];
        for (int j = 0; j < bn; j++)
        {
            if (j == bk.Length) Array.Resize(ref bk, Math.Min(bn, Math.Max(4, bk.Length * 2)));
            bk[j] = r.ReadInt64();
        }
        return bk;
    }

    /// <summary>
    /// A point's bucket-count array, walked past with every check <see cref="ReadBuckets"/> makes —
    /// the count against the block, each element read as the integer it must be — and nothing
    /// kept, so a torn array fails a skipped point exactly as it fails a built one.
    /// </summary>
    private static void SkipBuckets(ref MessagePackReader r)
    {
        int bn = r.ReadArrayHeader();
        FileBounds.RequireCountFits(bn, r.Sequence.Length - r.Consumed,
            fileBytesPerElement: 1, "Histogram buckets", "the series block");
        for (int j = 0; j < bn; j++) r.ReadInt64();
    }

    /// <summary>
    /// Whether <paramref name="labels"/> satisfies every matcher: the key present, and its value
    /// equal to the matcher's — or to one of its '|'-separated options (multi-select, e.g.
    /// <c>service.name=A|B|C</c>). Ordinal throughout. The ONE matcher for the cold reader, the hot
    /// tier and the exemplar ring; callers decide, as they always did, whether a null or empty
    /// matcher set reaches it at all.
    ///
    /// <para><b>A scan of the sorted pairs, not a dictionary per series.</b> Both call sites used to
    /// build <c>labels.Pairs.ToDictionary(...)</c> — a dictionary, its buckets and entries, a pair
    /// view and a LINQ iterator, per series per query — to look up two or three keys in a set of
    /// five. The pairs are already sorted by key (ordinal), so a walk that stops at the first key
    /// past the one sought answers the same lookup with no allocation at all.</para>
    ///
    /// <para><b>A repeated key is matched on the ORDINAL-GREATEST of its values</b> (#92) — the last
    /// of its run, since a set is sorted by key and then by value — which is the value the answer
    /// writes for it (<c>MetricSeriesJson.WriteLabels</c>), so a filter selects exactly the series
    /// whose written labels satisfy it. It is NOT the value ingest keeps today (the last one sent, a
    /// point attribute over the resource): the order the values arrived in was never stored. A series
    /// stored with a point <c>service.name=api</c> under a resource <c>service.name=gateway</c> is
    /// answered and filtered as <c>gateway</c>, while the same series ingested now is <c>api</c>.
    /// Ingest no longer builds such a set; one stored before
    /// that fix (the WAL, an <c>.mts</c>) is matched, never refused. It used to throw
    /// <see cref="ArgumentException"/> before any matcher was looked at, as the <c>ToDictionary</c>
    /// this scan replaced did: any filter on /query, /heatmap or /exemplars that touched such a
    /// series was a 500, and a metric alert rule with labels failed every tick while it was in the
    /// window.</para>
    /// </summary>
    internal static bool MatchesLabels(LabelSet labels, IReadOnlyDictionary<string, string> matchers)
    {
        ReadOnlySpan<string> kv = labels.Interleaved;

        // The concrete dictionary's struct enumerator, when that is what came in (it is, from every
        // endpoint and the alert rules): the interface's would be a boxed enumerator per series.
        if (matchers is Dictionary<string, string> dict)
        {
            foreach (var (k, v) in dict)
                if (!Matches(kv, k, v)) return false;
            return true;
        }
        foreach (var (k, v) in matchers)
            if (!Matches(kv, k, v)) return false;
        return true;
    }

    /// <summary>
    /// The value under <paramref name="key"/> in canonically sorted pairs — the last of its run, the
    /// ordinal-greatest, when the key repeats (see <see cref="MatchesLabels"/>) — tested against the matcher.
    /// </summary>
    private static bool Matches(ReadOnlySpan<string> kv, string key, string matcher)
    {
        for (int i = 0; i < kv.Length; i += 2)
        {
            int c = string.CompareOrdinal(kv[i], key);
            if (c < 0) continue;
            if (c > 0) return false;                                   // past it: the key is absent
            while (i + 2 < kv.Length && string.Equals(kv[i + 2], key)) i += 2;
            return LabelValueMatches(kv[i + 1], matcher);
        }
        return false;
    }

    /// <summary>
    /// Exact match, or OR-match when the matcher value is '|'-delimited (e.g.
    /// <c>service.name=A|B|C</c>) — lets the multi-service filter merge several series server-side
    /// so quantiles aggregate over the union. The options are walked in place: <c>Split('|')</c>
    /// allocated an array and a string per option per series, for the same answer — empty options
    /// included (<c>"A||B"</c> and <c>"A|"</c> both accept the empty value).
    /// </summary>
    internal static bool LabelValueMatches(string actual, string matcher)
    {
        if (matcher.IndexOf('|') < 0) return actual == matcher;
        ReadOnlySpan<char> rest = matcher;
        while (true)
        {
            int bar = rest.IndexOf('|');
            if ((bar < 0 ? rest : rest[..bar]).SequenceEqual(actual)) return true;
            if (bar < 0) return false;
            rest = rest[(bar + 1)..];
        }
    }

    private static long ReadNameIdxOffset(FileStream fs, BinaryReader br)
    {
        fs.Seek(-12, SeekOrigin.End);
        long offset = (long)br.ReadUInt64();
        uint magic  = br.ReadUInt32();
        if (magic != FooterMagic) throw new InvalidDataException("Invalid .mts footer magic");
        return offset;
    }

    /// <summary>
    /// Buffered at 64 KB, deliberately, even though the buffer is allocated per open and
    /// <c>RewriteMetricInChunks</c> reopens every source once per series chunk (~6 MB/min of
    /// gen0 in an allocation trace).
    ///
    /// <para>Dropping the buffer looks free because the v3 path reads a dozen header fields
    /// and then pulls whole LZ4 blocks into pooled buffers. The v2 path does not: it reads
    /// two <see cref="BinaryReader.ReadUInt32"/> per SERIES, and v2 files are exactly the
    /// ones with unbounded series counts. Measured on that shape, 5 000 series per file:</para>
    ///
    /// <code>
    ///   bufferSize     ms/open     alloc/open
    ///            0       20.54            238 B
    ///         4096        1.75          4.4 KB
    ///        65536        0.34         65.9 KB
    /// </code>
    ///
    /// <para>Unbuffered is 60x slower here — trading gen0 garbage, which the collector
    /// handles for almost nothing, for syscalls, which it cannot help with at all. The 6 MB
    /// is 2 % of this server's allocation; the syscalls are not 2 % of anything.</para>
    /// </summary>
    private static FileStream OpenRead(string path) =>
        new(path, FileMode.Open, FileAccess.Read, FileShare.Read, 65536, FileOptions.SequentialScan);
}
