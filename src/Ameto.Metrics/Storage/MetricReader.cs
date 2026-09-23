using Ameto.Core;
using System.Buffers;
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
    /// for the same reason: nothing on disk bounds what an LZ4 payload claims to expand to.</summary>
    private const int MaxBlockBytes = 64 * 1024 * 1024;

    private const uint   Magic       = 0x52_44_4D_54; // "RDMT"
    private const uint   FooterMagic = 0x52_44_4D_46; // "RDMF"

    public static MetricSegmentInfo ReadSegmentInfo(string filePath)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        uint magic = br.ReadUInt32();
        if (magic != Magic) throw new InvalidDataException($"Invalid .mts magic in {filePath}");

        ushort version = br.ReadUInt16();
        if (version is not (2 or 3)) throw new InvalidDataException($"Unsupported .mts version {version} in {filePath}");
        var granularity = (MetricGranularity)br.ReadByte();
        br.ReadUInt32();  // seriesCount
        long minNano = br.ReadInt64();
        long maxNano = br.ReadInt64();

        // Read metric name from name index
        long nameIdxOffset = ReadNameIdxOffset(fs, br);
        fs.Seek(nameIdxOffset, SeekOrigin.Begin);
        br.ReadUInt32(); // nameCount
        ushort nameLen = br.ReadUInt16();
        string metricName = System.Text.Encoding.UTF8.GetString(br.ReadBytes(nameLen));

        return new MetricSegmentInfo
        {
            FilePath      = filePath,
            MetricName    = metricName,
            MinNano       = minNano,
            MaxNano       = maxNano,
            Granularity   = granularity,
            FormatVersion = version,
            SizeBytes     = fs.Length,
        };
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

    /// <summary>Every series in the file, every point, labels unfiltered — the rollup's and the catalog seed's read.</summary>
    public static IEnumerable<MetricSeries> ReadAllSync(string filePath) =>
        Read(filePath, metricName: null, ReadWindow.All, CancellationToken.None);

    // ── The rewrite's reads (RewriteMetricInChunks) ───────────────────────────

    /// <summary>
    /// The rewrite's first pass over one source: every series in file order, each with its
    /// POSITION (where <see cref="ReadAt"/> finds it again) and its key index from
    /// <paramref name="keyOf"/>, which numbers keys as they are met. Only a series whose key index
    /// is below <paramref name="pointsBelow"/> has its points decoded — the rewrite's first chunk,
    /// gathered in the same pass; every other series is decoded up to its identity and bucket
    /// bounds, and its points are walked past.
    /// </summary>
    internal static IEnumerable<(long Position, int Key, MetricSeries Series)> ReadForRewrite(
        string filePath, Func<SeriesKey, int> keyOf, int pointsBelow) =>
        ReadCore(filePath, metricName: null,
                 new ReadWindow(long.MinValue, long.MaxValue, null, buckets: true, labels: true, keyOf, pointsBelow),
                 at: null, CancellationToken.None);

    /// <summary>
    /// The series at <paramref name="positions"/> (as <see cref="ReadForRewrite"/> reported them,
    /// ascending), in that order, with their points and bucket bounds — and NOT their labels or
    /// unit, which the rewrite already holds from its first pass. A v3 file is inflated once and
    /// each series decoded where it starts; a v2 file's other series are not even inflated.
    /// </summary>
    internal static IEnumerable<(long Position, int Key, MetricSeries Series)> ReadAt(string filePath, List<long> positions) =>
        ReadCore(filePath, metricName: null,
                 new ReadWindow(long.MinValue, long.MaxValue, null, buckets: true, labels: false),
                 at: positions, CancellationToken.None);

    /// <summary>
    /// What a read keeps: points in <c>[FromNano, ToNano]</c> (inclusive, as the range test always
    /// was), from series whose labels pass <see cref="Matchers"/> when there are any.
    /// </summary>
    internal readonly struct ReadWindow(
        long fromNano, long toNano, IReadOnlyDictionary<string, string>? matchers, bool buckets,
        bool labels = true, Func<SeriesKey, int>? keyOf = null, int pointsBelow = int.MaxValue)
    {
        public static ReadWindow All => new(long.MinValue, long.MaxValue, null, buckets: true);

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

        public bool Keeps(long ts) => ts >= FromNano && ts <= ToNano;

        /// <summary>Whether every point a file whose header spans [min, max] can hold is in range —
        /// a sizing hint only (a v3 point reads back up to a millisecond below the header's min).</summary>
        public bool Covers(long minNano, long maxNano) =>
            FromNano == long.MinValue ? ToNano >= maxNano : FromNano <= minNano - 1_000_000 && ToNano >= maxNano;
    }

    private static IEnumerable<MetricSeries> Read(string filePath, string? metricName, ReadWindow window, CancellationToken ct)
    {
        foreach (var (_, _, series) in ReadCore(filePath, metricName, window, at: null, ct))
            yield return series;
    }

    /// <summary>
    /// The one reading loop: every series of the file in order (<paramref name="at"/> null), or the
    /// series at the given positions — for v3 an offset in the inflated block, for v2 the file
    /// offset of the series' own block. Each series comes with its position and its rewrite key
    /// index (-1 outside a rewrite read).
    /// </summary>
    private static IEnumerable<(long Position, int Key, MetricSeries Series)> ReadCore(
        string filePath, string? metricName, ReadWindow window, List<long>? at, CancellationToken ct)
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

        bool covers = window.Covers(minNano, maxNano);

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

            byte[] comp = ArrayPool<byte>.Shared.Rent((int)compSize);
            byte[]? raw  = null;
            try
            {
                fs.ReadExactly(comp, 0, (int)compSize);
                // The length INSIDE the payload, which the check above never saw: LZ4 carries the
                // decompressed size in its own header, so a short well-formed block can still ask
                // for gigabytes.
                int rawLen = LZ4Pickler.UnpickledSize(comp.AsSpan(0, (int)compSize));
                FileBounds.RequireLengthFits(rawLen, MaxBlockBytes, "Series block uncompressed", filePath);
                raw = ArrayPool<byte>.Shared.Rent(rawLen);
                LZ4Pickler.Unpickle(comp.AsSpan(0, (int)compSize), raw.AsSpan(0, rawLen));

                if (at is null)
                {
                    int offset = 0;
                    for (int i = 0; i < seriesCount && offset < rawLen; i++)
                    {
                        ct.ThrowIfCancellationRequested();
                        int start  = offset;
                        var series = DeserializeNext(fileMetric, raw, offset, rawLen, deltaMs: true, in window, covers, out offset, out int key);
                        if (series is not null) yield return (start, key, series);
                    }
                }
                else
                {
                    foreach (long position in at)
                    {
                        ct.ThrowIfCancellationRequested();
                        if (position < 0 || position >= rawLen)
                            throw new InvalidDataException($"Series position {position} is outside the block of {filePath}");
                        var series = DeserializeNext(fileMetric, raw, (int)position, rawLen, deltaMs: true, in window, covers, out _, out int key);
                        if (series is not null) yield return (position, key, series);
                    }
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(comp);
                if (raw is not null) ArrayPool<byte>.Shared.Return(raw);
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

                byte[] comp = ArrayPool<byte>.Shared.Rent((int)compSize);
                byte[]? raw = null;
                MetricSeries? series;
                int key;
                try
                {
                    fs.ReadExactly(comp, 0, (int)compSize);
                    int rawLen = LZ4Pickler.UnpickledSize(comp.AsSpan(0, (int)compSize));
                    FileBounds.RequireLengthFits(rawLen, MaxBlockBytes, $"Series {i} uncompressed", filePath);
                    raw = ArrayPool<byte>.Shared.Rent(rawLen);
                    LZ4Pickler.Unpickle(comp.AsSpan(0, (int)compSize), raw.AsSpan(0, rawLen));
                    series = DeserializeNext(fileMetric, raw, 0, rawLen, deltaMs: false, in window, covers, out _, out key);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(comp);
                    if (raw is not null) ArrayPool<byte>.Shared.Return(raw);
                }

                if (series is not null) yield return (position, key, series);
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
        in ReadWindow window, bool covers, out int next, out int key)
    {
        var r = new MessagePackReader(new ReadOnlyMemory<byte>(raw, offset, length - offset));
        var series = DeserializeSeries(metricName, ref r, deltaMs, in window, covers, out key);
        next = offset + (int)r.Consumed;
        return series;
    }

    private static MetricSeries? DeserializeSeries(
        string metricName, ref MessagePackReader r, bool deltaMs, in ReadWindow window, bool covers, out int keyIndex)
    {
        int fields = r.ReadMapHeader();

        MetricKind kind    = MetricKind.Counter;
        string     unit    = string.Empty;
        LabelSet   labels  = LabelSet.Empty;
        double[]?  bounds  = null;
        List<MetricDataPoint>? points = null;
        keyIndex = -1;

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
                if (window.Labels) unit = ReadInterned(ref r, MetricLabelInterner.Shared, out _);
                else               r.Skip();
                have |= HaveUnit;
            }
            else if (key.SequenceEqual("lbs"u8))
            {
                if (!window.Labels) { r.Skip(); continue; }
                labels = ReadLabels(ref r);
                have  |= HaveLabels;
                // Rejected here, before the points the writer puts after the labels: the rest of
                // the map is walked, not decoded, so the reader ends where the next series starts.
                if (window.Matchers is not null && !MatchesLabels(labels, window.Matchers))
                {
                    for (int j = i + 1; j < fields; j++) { r.Skip(); r.Skip(); }
                    return null;
                }
            }
            else if (key.SequenceEqual("bnds"u8)) bounds = ReadBounds(ref r);
            else if (key.SequenceEqual("pts"u8))
            {
                if (window.KeyOf is { } keyOf && (have & HaveIdentity) == HaveIdentity)
                {
                    keyIndex = keyOf(new SeriesKey(metricName, kind, unit, labels));
                    if (keyIndex >= window.PointsBelow) { r.Skip(); continue; }   // not this pass's
                }
                points = deltaMs ? ReadPointsV3(ref r, in window, covers) : ReadPointsV2(ref r, in window, covers);
            }
            else r.Skip();
        }
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

    private static LabelSet ReadLabels(ref MessagePackReader r)
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

        // THROUGH THE SHARED INTERNER, as the OTLP parsers and the WAL replay already are (WP6 of
        // issue #83 left this reader out: it was not that package's file). A cold read used to
        // decode a fresh string for every key and value of every series it touched — ten strings
        // and a label set per five-label series, per query, per rollup chunk, per catalog seed —
        // for text the process already holds: the series a query reads back are, almost always,
        // the series ingest is still sending. Resolved from the UTF-8 in place, a pooled string
        // costs nothing, and a label set whose strings are all pooled comes back as the very
        // instance the hot tier and the WAL hold. Bounded as the interner is (see
        // MetricLabelInterner): past its cap a string is simply built, as before, and a label set
        // over an unpooled string is built fresh — equal by value either way.
        var interner = MetricLabelInterner.Shared;
        int n        = count * 2;
        var kv       = ArrayPool<string>.Shared.Rent(MaxInternedLabelStrings);
        Span<int> ids = stackalloc int[MaxInternedLabelStrings];
        try
        {
            for (int i = 0; i < n; i++) kv[i] = ReadInterned(ref r, interner, out ids[i]);
            return interner.GetLabelSet(kv.AsSpan(0, n), ids[..n]);
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
    /// A msgpack string through <paramref name="interner"/>, from its UTF-8 bytes in place — the
    /// text <c>ReadString() ?? string.Empty</c> gave (the interner decodes exactly as
    /// <c>Encoding.UTF8.GetString</c> does), and its interner id for the label-set lookup. Nil is
    /// the empty string, as it was.
    /// </summary>
    private static string ReadInterned(ref MessagePackReader r, MetricLabelInterner interner, out int id)
    {
        if (r.TryReadNil()) { id = MetricLabelInterner.EmptyStringId; return string.Empty; }
        string value;
        if (r.TryReadStringSpan(out ReadOnlySpan<byte> utf8)) id = interner.Intern(utf8, out value);
        else                                                   id = interner.Intern(r.ReadString() ?? string.Empty, out value);
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
    private static List<MetricDataPoint> ReadPointsV3(ref MessagePackReader r, in ReadWindow window, bool covers)
    {
        // A MessagePack array header is a number out of the file like any other: the block it
        // lives in is bounded, but the header can still claim far more points than the block
        // holds, and a capacity is reserved before a single one is read. Reserve modestly and
        // let the list grow into whatever is really there — and reserve at all only when the
        // whole file is inside the window, since otherwise the header says nothing about how
        // many of its points will be kept.
        int count  = r.ReadArrayHeader();
        var pts    = new List<MetricDataPoint>(covers ? FileBounds.PreallocFor(count, heapBytesPerElement: 64) : 0);
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

    /// <summary>v2 points: absolute nanosecond timestamps; always 5 fields. Only the window's are stored.</summary>
    private static List<MetricDataPoint> ReadPointsV2(ref MessagePackReader r, in ReadWindow window, bool covers)
    {
        int count = r.ReadArrayHeader();
        var pts   = new List<MetricDataPoint>(covers ? FileBounds.PreallocFor(count, heapBytesPerElement: 64) : 0);
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
    /// <para><b>A repeated key still throws, deliberately.</b> <c>ToDictionary</c> threw
    /// <see cref="ArgumentException"/> on a label set with a key twice (the OTLP parser can build
    /// one — a point attribute named <c>service.name</c>, a duplicate attribute) before any matcher
    /// was looked at, failing the whole query. That is a latent bug, but this change is a
    /// performance change and the answer is part of what it must keep: a query that failed still
    /// fails, the same way (<c>MetricQueryGoldenTests</c> pins it). Repeats are adjacent in the
    /// sorted pairs, so finding one is a compare per pair.</para>
    /// </summary>
    internal static bool MatchesLabels(LabelSet labels, IReadOnlyDictionary<string, string> matchers)
    {
        ReadOnlySpan<string> kv = labels.Interleaved;
        for (int i = 2; i < kv.Length; i += 2)
            if (string.Equals(kv[i], kv[i - 2])) ThrowRepeatedKey(kv[i]);

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

    /// <summary>The value under <paramref name="key"/> in canonically sorted pairs, tested against the matcher.</summary>
    private static bool Matches(ReadOnlySpan<string> kv, string key, string matcher)
    {
        for (int i = 0; i < kv.Length; i += 2)
        {
            int c = string.CompareOrdinal(kv[i], key);
            if (c < 0) continue;
            return c == 0 && LabelValueMatches(kv[i + 1], matcher);   // past it: the key is absent
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

    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    private static void ThrowRepeatedKey(string key) =>
        throw new ArgumentException($"An item with the same key has already been added. Key: {key}");

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
