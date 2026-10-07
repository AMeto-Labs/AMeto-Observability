using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;
using K4os.Compression.LZ4;
using MessagePack;

namespace Ameto.Metrics.Storage;

/// <summary>
/// Writes metric series to <c>.mts</c> files.
///
/// <para>File format v3 — "RDMT":</para>
/// <code>
///   [Header]
///     0  Magic       : uint32  "RDMT"
///     4  Version     : uint16  3
///     6  Granularity : uint8
///     7  SeriesCount : uint32
///    11  MinNano     : int64
///    19  MaxNano     : int64
///    27  Flags       : byte
///   [Section — ALL series in ONE LZ4-HC block]
///     uncompSize uint32 | compSize uint32 | LZ4 bytes
///     msgpack: SeriesCount × { k, u, lbs, bnds(double[]), pts, cnt }
///       point: scalar     [ Δts_ms, value ]
///               histogram [ Δts_ms, value, count, sum, bucketCounts(long[] | nil) ]
///       Δts_ms: first point = full unix ms; the rest = varint delta to the previous.
///       Integral doubles are written as msgpack ints (varint) — the reader's
///       ReadDouble transparently accepts both.
///   [MetricName index]
///     nameCount uint32
///     per-name: nameLen uint16 | name bytes | blockOffset uint64 (0) | blockCount uint32
///   [Footer]
///     nameIdxOffset uint64
///     footerMagic   uint32  "RDMF"
/// </code>
/// <para>
/// v3 versus v2: the whole series section is compressed as a single LZ4-HC block
/// (v2 compressed each series separately, so label strings repeated across
/// thousands of series were never deduplicated and small blocks barely
/// compressed); timestamps are millisecond deltas instead of 9-byte nanosecond
/// ints; scalar points drop the always-zero count/sum/buckets fields. v2 files
/// remain readable (see <see cref="MetricReader"/>) and are migrated to v3 by the
/// background compaction in <see cref="MetricStorageEngine"/>.
/// </para>
/// </summary>
internal static class MetricWriter
{
    private const uint   Magic       = 0x52_44_4D_54; // "RDMT"
    private const uint   FooterMagic = 0x52_44_4D_46; // "RDMF"
    private const ushort Version     = 3;

    /// <summary>
    /// Upper bound on series packed into one .mts file. The writer serialises a
    /// whole file into a single msgpack buffer before compressing it, so an
    /// unbounded series count turns into an unbounded (doubling) buffer — with
    /// high-cardinality metrics (observed live: 4,098 series for one metric,
    /// 38,741 in total) that alone drove GB-scale allocation during rollup.
    /// Splitting into several files costs nothing: multiple files per metric and
    /// granularity are normal, and the reader already merges across them.
    /// </summary>
    private const int MaxSeriesPerFile = 512;

    /// <summary>Suffix a file carries until its footer is on disk and its handle is closed.</summary>
    internal const string TempSuffix = ".tmp";

    /// <summary>
    /// Writes one <c>.mts</c> file per distinct metric name found in <paramref name="series"/>
    /// (several, if a name carries more than <see cref="MaxSeriesPerFile"/> series, or more than
    /// fits the block a reader opens — <see cref="MetricReader.MaxBlockBytes"/>, see the loop).
    /// Returns metadata for all created files.
    ///
    /// <para><b>All or nothing on disk.</b> The caller reads a throw as "no file carries these
    /// points", puts the whole snapshot back into the hot tier and leaves the log generation
    /// replayable; with a file already at a final path that is not a window but certain
    /// duplication — served twice and replayed twice, every rollup over the range reading
    /// double. Three mechanisms make the caller's reading true, and the order matters:</para>
    /// <list type="number">
    /// <item>Each file is built at <c>.mts.tmp</c> and renamed only once its footer is written
    /// and its handle closed — the shape <c>StorageEngine.FlushToColdAsync</c> already uses.
    /// A failure INSIDE a file therefore leaves nothing at a path the catalog scan reads. No
    /// after-the-fact cleanup could do that job: a file only joins the returned list once it is
    /// written, so the file the disk died on was exactly the one the catch below could not name,
    /// and what it left behind was a footerless <c>.mts</c> that the next start deleted as
    /// "likely format v1".</item>
    /// <item>Every file's bytes reach the PLATTER before it is renamed — see the flush at the end
    /// of <see cref="WriteFile"/> — so no route exists by which a path the scan reads can hold a
    /// file whose contents are still in page cache.</item>
    /// <item>Nothing is renamed until every file is written, and whole files that DID land are
    /// deleted before the exception leaves, since a later file's failure retracts the whole
    /// call.</item>
    /// </list>
    ///
    /// <para>The publish being a separate pass is what shortens the window a crash can duplicate
    /// in. The caller commits the log generation once this returns, so a crash between the first
    /// rename and that commit leaves published points also replayable; while each file was
    /// renamed as it finished, that window was the whole write of every file after the first —
    /// seconds under a large flush. It is now the renames themselves.</para>
    ///
    /// <para><b>What it allocates is its output, not its working set</b> (issue #94). The section is
    /// serialised into one buffer rented from the shared pool and reused for every file of the call
    /// (<see cref="RentedBufferWriter"/>), where each file used to grow a fresh
    /// <c>ArrayBufferWriter</c> by doubling and then copy it out with <c>ToArray()</c> for the
    /// compressor; each series' points are read in place (<see cref="HotSeries.PointsForWrite"/>),
    /// where <c>GetPoints</c> copied every point of every series twice, once for the file's time
    /// range and once to serialise it; the header, index and footer are laid out in a stack buffer
    /// and written through an unbuffered stream, where a <see cref="BinaryWriter"/> sat over a
    /// 64 KB <see cref="FileStream"/> buffer per file; and the grouping by name is a counting sort
    /// over rented indices, where it was <c>GroupBy</c> / <c>ToList</c> / <c>GetRange</c>. What is
    /// left per file is the compressed payload the pickler returns (its size on disk), the names
    /// and the segment info. The bytes are exactly the ones the old code wrote.</para>
    /// </summary>
    /// <param name="afterFileWritten">
    /// Test seam invoked with each completed file's FINAL path, once it is in place. Null in production.
    /// </param>
    /// <param name="duringFileWrite">
    /// Test seam invoked with the temp path while that file is open and half written — the disk
    /// that dies mid-file. Nothing else can produce that state: everything between the open and
    /// the close is I/O, and the names carry a random nonce so the path cannot be booby-trapped
    /// from outside. Null in production.
    /// </param>
    public static List<MetricSegmentInfo> Write(
        string dataDir,
        IList<(SeriesKey Key, HotSeries Series)> series,
        MetricGranularity granularity = MetricGranularity.Raw,
        Action<string>? afterFileWritten = null,
        Action<string>? duringFileWrite  = null)
    {
        var result   = new List<MetricSegmentInfo>();
        var staged   = new List<string>();   // temp paths, index-aligned with result
        int published = 0;

        int   count = series.Count;
        int[] order = TakeInts(Math.Max(count, 1));
        int[] ends  = TakeInts(Math.Max(count, 1));
        using var raw = new RentedBufferWriter();
        Span<char> nonceChars = stackalloc char[32];
        try
        {
            int groups     = GroupByName(series, order.AsSpan(0, count), ends.AsSpan(0, count));
            int start      = 0;
            int maxSection = MaxSectionBytesForTest ?? MetricReader.MaxBlockBytes;
            for (int g = 0; g < groups; g++)
            {
                int    end        = ends[g];
                string metricName = series[order[start]].Key.Name;   // the group's key, as GroupBy's was: its first item's
                string namePart   = FileNamePart(metricName);

                // Serialize ALL a file's series into one msgpack buffer, then compress it as a single
                // LZ4-HC block: repeated label keys/values across series (routes, instance ids, GUIDs)
                // deduplicate inside the shared compression window. The file's time range is taken on
                // the same pass, from each series' first and last point.
                //
                // A FILE ENDS AT MaxSeriesPerFile SERIES, OR BEFORE A SERIES THAT MIGHT NOT FIT THE
                // BLOCK A READER WILL OPEN (#125). The reader refuses a section over
                // MetricReader.MaxBlockBytes, and this writer used to write one anyway: 512 busy
                // histogram series of 1 600 five-minute points made an 87.5 MiB section, written
                // without complaint and refused by every query, merge and rollup that met it. A
                // series is admitted against an upper bound on its encoding (EncodedBound), so a
                // section never passes the line; one series that is larger than a block on its own
                // goes out in time-ordered runs of its points, one file each. Below the line every
                // file is exactly what it was, byte for byte.
                int  fileSeries = 0;
                long minNano = long.MaxValue, maxNano = long.MinValue;
                raw.Reset();
                for (int at = start; at < end; at++)
                {
                    var (key, hs) = series[order[at]];
                    ReadOnlySpan<MetricDataPoint> pts = hs.PointsForWrite();
                    long bound = EncodedBound(key, hs.Bounds, pts);

                    if (fileSeries > 0 && (fileSeries == MaxSeriesPerFile || raw.WrittenSpan.Length + bound > maxSection))
                    {
                        EmitFile(dataDir, metricName, namePart, granularity, fileSeries, minNano, maxNano, raw,
                                 nonceChars, duringFileWrite, staged, result);
                        raw.Reset();
                        fileSeries = 0;
                        minNano    = long.MaxValue;
                        maxNano    = long.MinValue;
                    }

                    if (bound > maxSection)
                    {
                        WriteInRuns(dataDir, metricName, namePart, granularity, key, hs.Bounds, pts, maxSection, raw,
                                    nonceChars, duringFileWrite, staged, result);
                        raw.Reset();
                        continue;
                    }

                    var w = new MessagePackWriter(raw);
                    WriteSeries(ref w, key, hs.Bounds, pts);
                    w.Flush();
                    fileSeries++;
                    if (pts.Length > 0)
                    {
                        minNano = Math.Min(minNano, pts[0].TimestampUnixNano);
                        maxNano = Math.Max(maxNano, pts[^1].TimestampUnixNano);
                    }
                }
                if (fileSeries > 0)
                    EmitFile(dataDir, metricName, namePart, granularity, fileSeries, minNano, maxNano, raw,
                             nonceChars, duringFileWrite, staged, result);
                start = end;
            }

            // ── Publish ───────────────────────────────────────────────────────────────
            // Every file is complete and on the platter; these renames make them visible.
            for (int i = 0; i < result.Count; i++)
            {
                // overwrite: false keeps FileMode.CreateNew's promise across the rename —
                // the nonce makes a collision a bug, and clobbering someone else's segment
                // is not how it should surface.
                File.Move(staged[i], result[i].FilePath, overwrite: false);
                published = i + 1;
                afterFileWritten?.Invoke(result[i].FilePath);
            }
        }
        catch
        {
            // Best effort by necessity — the usual cause is a disk that cannot take another
            // byte, and an unlink needs none. What a failed delete leaves behind is exactly the
            // old behaviour for that one file, not something worse, so the original exception
            // is what the caller must see.
            for (int i = 0; i < published; i++)
                try { File.Delete(result[i].FilePath); } catch { /* leave it; the throw still stands */ }
            for (int i = published; i < staged.Count; i++)
                try { File.Delete(staged[i]); } catch { /* leave it; the throw still stands */ }
            throw;
        }
        finally
        {
            GiveInts(order);
            GiveInts(ends);
        }

        return result;
    }

    /// <summary>
    /// One file from the section in <paramref name="raw"/>: compressed, written at a temp name and
    /// staged for the publish in <see cref="Write"/> — or nothing at all when the section's series
    /// hold not one point (<paramref name="minNano"/> still <see cref="long.MaxValue"/>).
    /// </summary>
    private static void EmitFile(
        string dataDir, string metricName, string namePart, MetricGranularity granularity,
        int seriesCount, long minNano, long maxNano, RentedBufferWriter raw, Span<char> nonceChars,
        Action<string>? duringFileWrite, List<string> staged, List<MetricSegmentInfo> result)
    {
        if (minNano == long.MaxValue) return;   // not one point: no file

        // HC level: writes happen on background flush/rollup threads only, so we trade
        // CPU for the much better ratio. The span overload, not the IBufferWriter one:
        // the two disagree on the header of some sizes (a 64 KB input gets a wider size
        // field), and this is the one whose bytes the files on disk carry.
        byte[] compressed = LZ4Pickler.Pickle(raw.WrittenSpan, LZ4Level.L09_HC);

        // A short nonce keeps the name unique: the (name, min, max, granularity)
        // tuple is NOT — a v2→v3 migration re-writes the same time range, and two
        // files of the same range can legitimately coexist until the next merge
        // dedupes them. Without it, WriteFile's FileMode.CreateNew throws
        // "already exists" and the compaction fails every pass. The name is never
        // parsed back (all metadata is read from the file's own header/index).
        Guid.NewGuid().TryFormat(nonceChars, out _, "N");
        ReadOnlySpan<char> nonce = nonceChars[..8];
        string fileName = $"metrics-{namePart}-{minNano}-{maxNano}-{Suffix(granularity)}-{nonce}.mts";
        string filePath = Path.Combine(dataDir, fileName);
        string tmpPath  = filePath + TempSuffix;

        try
        {
            long size = WriteFile(tmpPath, metricName, granularity, seriesCount, minNano, maxNano,
                                  raw.WrittenSpan.Length, compressed, duringFileWrite);

            // Staged before it is described, so the retraction in Write owns the file from
            // the instant it exists. Sized by what was written to the temp file: reading
            // the length through the final path is one more call that can throw, and it
            // would throw with a complete file sitting at a path nothing has recorded.
            staged.Add(tmpPath);
            result.Add(new MetricSegmentInfo
            {
                FilePath      = filePath,
                MetricName    = metricName,
                MinNano       = minNano,
                MaxNano       = maxNano,
                Granularity   = granularity,
                FormatVersion = Version,
                SizeBytes     = size,
            });
        }
        catch
        {
            // The half-written file. Deleting it here is not redundant with the
            // retraction: it reaches this line before it has been staged, which is
            // exactly the state the retraction cannot name.
            try { File.Delete(tmpPath); } catch { /* leave it; the throw still stands */ }
            throw;
        }
    }

    /// <summary>
    /// ONE SERIES WHOSE ENCODING MAY NOT FIT A BLOCK ON ITS OWN: its points go out in time order, as
    /// many runs as they need, each run a one-series file whose bound fits <paramref name="maxSection"/>.
    /// The points are already in timestamp order (<see cref="HotSeries.PointsForWrite"/>), so the runs
    /// are disjoint in time and a reader merges them back like any two files of one series. Only a
    /// single POINT larger than a block cannot be written — a histogram of millions of buckets, which
    /// nothing ingests — and that throws, which retracts the whole call as any write failure does.
    /// </summary>
    private static void WriteInRuns(
        string dataDir, string metricName, string namePart, MetricGranularity granularity,
        SeriesKey key, double[]? bounds, ReadOnlySpan<MetricDataPoint> pts, int maxSection,
        RentedBufferWriter raw, Span<char> nonceChars, Action<string>? duringFileWrite,
        List<string> staged, List<MetricSegmentInfo> result)
    {
        long identity = IdentityBound(key, bounds);
        if (pts.Length == 0) throw TooLargeForABlock(metricName, maxSection);

        int from = 0;
        while (from < pts.Length)
        {
            long used = identity + PointBound(in pts[from]);
            if (used > maxSection) throw TooLargeForABlock(metricName, maxSection);
            int  to   = from + 1;
            while (to < pts.Length)
            {
                long next = PointBound(in pts[to]);
                if (used + next > maxSection) break;
                used += next;
                to++;
            }

            raw.Reset();
            var w = new MessagePackWriter(raw);
            WriteSeries(ref w, key, bounds, pts[from..to]);
            w.Flush();
            EmitFile(dataDir, metricName, namePart, granularity, seriesCount: 1,
                     pts[from].TimestampUnixNano, pts[to - 1].TimestampUnixNano, raw,
                     nonceChars, duringFileWrite, staged, result);
            from = to;
        }
    }

    private static InvalidDataException TooLargeForABlock(string metricName, int maxSection) =>
        new($"A series of '{metricName}' cannot be written in a block of {maxSection:N0} bytes, the most a reader opens");

    /// <summary>
    /// An upper bound on the bytes <see cref="WriteSeries"/> writes for this series: every string at
    /// its msgpack header's widest plus three UTF-8 bytes a UTF-16 unit, every number at nine bytes,
    /// and every point as a FULL histogram point (<see cref="PointBound"/>). Typically 1.5–2x the
    /// real encoding — which only matters for a section that is already tens of megabytes.
    /// </summary>
    internal static long EncodedBound(SeriesKey key, double[]? bounds, ReadOnlySpan<MetricDataPoint> pts)
    {
        long bytes = IdentityBound(key, bounds);
        for (int i = 0; i < pts.Length; i++) bytes += PointBound(in pts[i]);
        return bytes;
    }

    /// <summary>The series' map, keys, kind, unit, labels, bounds and the two array/count headers.</summary>
    private static long IdentityBound(SeriesKey key, double[]? bounds)
    {
        long bytes = 64 + 3L * key.Unit.Length + 9L * (bounds?.Length ?? 0);
        ReadOnlySpan<string> kv = key.Labels.Interleaved;
        for (int i = 0; i < kv.Length; i++) bytes += 5 + 3L * kv[i].Length;
        return bytes;
    }

    /// <summary>A point at its widest: array header, delta, value, count, sum, and its bucket array.</summary>
    private static long PointBound(in MetricDataPoint p) => 42 + 9L * (p.BucketCounts?.Length ?? 0);

    /// <summary>
    /// Test seam: the most a file's section may hold, in place of <see cref="MetricReader.MaxBlockBytes"/>,
    /// on the writing thread only (the writer is synchronous), so a test can reach the split without
    /// building 64 MiB of points. Null in production.
    /// </summary>
    [ThreadStatic] internal static int? MaxSectionBytesForTest;

    /// <summary>
    /// Fills <paramref name="order"/> with the indices of <paramref name="series"/> grouped by metric
    /// name — names in the order they are first met, items in input order within a name, which is
    /// what <c>GroupBy</c> gave — and <paramref name="ends"/>[g] with where group g ends in it.
    /// Returns the number of groups. One name, the rewrite's case every time, is one pass and no
    /// dictionary; several (a flush) are a counting sort keyed by an ordinal dictionary, the
    /// comparer <c>GroupBy</c> used for strings.
    /// </summary>
    private static int GroupByName(IList<(SeriesKey Key, HotSeries Series)> series, Span<int> order, Span<int> ends)
    {
        int n = order.Length;
        if (n == 0) return 0;

        string first = series[0].Key.Name;
        int    same  = 1;
        while (same < n && string.Equals(series[same].Key.Name, first, StringComparison.Ordinal)) same++;
        if (same == n)
        {
            for (int i = 0; i < n; i++) order[i] = i;
            ends[0] = n;
            return 1;
        }

        var   groupOf = new Dictionary<string, int>(StringComparer.Ordinal);
        int[] groupAt = TakeInts(n);
        try
        {
            int groups = 0;
            ends.Clear();
            for (int i = 0; i < n; i++)
            {
                ref int g = ref CollectionsMarshal.GetValueRefOrAddDefault(groupOf, series[i].Key.Name, out bool known);
                if (!known) g = groups++;
                groupAt[i] = g;
                ends[g]++;
            }

            // Counts -> starts, then each index placed at its group's cursor: the cursor ends where
            // the group does.
            int sum = 0;
            for (int g = 0; g < groups; g++) { int c = ends[g]; ends[g] = sum; sum += c; }
            for (int i = 0; i < n; i++) order[ends[groupAt[i]]++] = i;
            return groups;
        }
        finally { GiveInts(groupAt); }
    }

    /// <summary>
    /// Writes one complete file at <paramref name="filePath"/>, which is a temp name: the caller
    /// renames it. Nothing here may assume the file survives — every throw between the open and
    /// the close leaves a file with no footer, and it is the temp name that keeps the reader from
    /// ever meeting one. Returns the file's length.
    ///
    /// <para>Three writes of whole spans — header with the section's sizes, the payload, then the
    /// name index and footer — through a stream with no buffer of its own (<c>bufferSize: 0</c>):
    /// a buffered one allocated 64 KB per file to coalesce what is already coalesced here. The
    /// layout is the one a <see cref="BinaryWriter"/> produced field by field, little-endian.</para>
    /// </summary>
    private static long WriteFile(
        string filePath,
        string metricName,
        MetricGranularity granularity,
        int seriesCount,
        long minNano,
        long maxNano,
        int rawLength,
        byte[] compressed,
        Action<string>? duringFileWrite)
    {
        using var fs = new FileStream(filePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, bufferSize: 0);

        // Header, then the section's two sizes.
        Span<byte> head = stackalloc byte[HeaderBytes + 8];
        BinaryPrimitives.WriteUInt32LittleEndian(head,        Magic);
        BinaryPrimitives.WriteUInt16LittleEndian(head[4..],   Version);
        head[6] = (byte)granularity;
        BinaryPrimitives.WriteUInt32LittleEndian(head[7..],   (uint)seriesCount);
        BinaryPrimitives.WriteInt64LittleEndian(head[11..],   minNano);
        BinaryPrimitives.WriteInt64LittleEndian(head[19..],   maxNano);
        head[27] = 0; // flags
        BinaryPrimitives.WriteUInt32LittleEndian(head[28..],  (uint)rawLength);
        BinaryPrimitives.WriteUInt32LittleEndian(head[32..],  (uint)compressed.Length);
        fs.Write(head);
        fs.Write(compressed);

        // Header and payload down, index and footer to go: the state a disk dies in. Null in
        // production — one delegate read per file, not per series.
        duringFileWrite?.Invoke(filePath);

        // Name index — one distinct name per file — then the footer. The name's length field is
        // 16 bits and was always written truncated (a (ushort) cast) ahead of all of its bytes.
        long nameIdxOffset = HeaderBytes + 8 + compressed.Length;
        int  nameLen       = Encoding.UTF8.GetByteCount(metricName);
        int  tailLen       = 4 + 2 + nameLen + 8 + 4 + 8 + 4;
        byte[]? rented     = tailLen > StackTailBytes ? ArrayPool<byte>.Shared.Rent(tailLen) : null;
        try
        {
            Span<byte> tail = (rented is null ? stackalloc byte[StackTailBytes] : rented)[..tailLen];
            BinaryPrimitives.WriteUInt32LittleEndian(tail, 1);
            BinaryPrimitives.WriteUInt16LittleEndian(tail[4..], (ushort)nameLen);
            Encoding.UTF8.GetBytes(metricName, tail.Slice(6, nameLen));
            int at = 6 + nameLen;
            BinaryPrimitives.WriteUInt64LittleEndian(tail[at..], 0);                    // block offset — unused in v3 (single section)
            BinaryPrimitives.WriteUInt32LittleEndian(tail[(at + 8)..], (uint)seriesCount);
            BinaryPrimitives.WriteUInt64LittleEndian(tail[(at + 12)..], (ulong)nameIdxOffset);
            BinaryPrimitives.WriteUInt32LittleEndian(tail[(at + 20)..], FooterMagic);
            fs.Write(tail);
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }

        // To the PLATTER, not just to the OS, and before the caller renames this file into
        // place — the same step SegmentWriter.Finalise takes on the log side, for the same
        // reason. What the caller does the moment these files are published is commit the WAL
        // generation carrying their points, which is a store into a memory-mapped header page;
        // that page and these data pages are written back by independent paths with no ordering
        // between them. Without this the two could land in either order across a power loss, and
        // one of the orders is the watermark advancing past points whose file is still a
        // zero-length or truncated .mts at its final path — which the next start deletes as
        // "likely format v1", now for points that no longer exist anywhere else. Closing the
        // handle only hands the bytes to the OS; the temp name and the rename order the
        // PUBLISH, and this orders the DATA.
        fs.Flush(flushToDisk: true);
        return nameIdxOffset + tailLen;
    }

    /// <summary>Bytes of the fixed header: magic, version, granularity, series count, min, max, flags.</summary>
    private const int HeaderBytes = 28;

    /// <summary>A name index and footer up to this size is laid out on the stack; a longer metric name rents.</summary>
    private const int StackTailBytes = 512;

    private static string Suffix(MetricGranularity granularity) => granularity switch
    {
        MetricGranularity.Raw     => "raw",
        MetricGranularity.FiveMin => "fivemin",
        MetricGranularity.OneHour => "onehour",
        _                         => granularity.ToString().ToLowerInvariant(),
    };

    private static void WriteSeries(
        ref MessagePackWriter w,
        SeriesKey key,
        double[]? bounds,
        ReadOnlySpan<MetricDataPoint> points)
    {
        w.WriteMapHeader(6);
        w.Write("k");    w.Write((byte)key.Kind);
        w.Write("u");    w.Write(key.Unit);
        w.Write("lbs");  WriteLabels(ref w, key.Labels);
        w.Write("bnds"); WriteBounds(ref w, bounds);
        w.Write("pts");  WritePoints(ref w, points);
        w.Write("cnt");  w.Write((uint)points.Length);
    }

    /// <summary>The pairs off the interleaved span: <see cref="LabelSet.Pairs"/> built a list view and a boxed iterator per series.</summary>
    private static void WriteLabels(ref MessagePackWriter w, LabelSet labels)
    {
        ReadOnlySpan<string> kv = labels.Interleaved;
        w.WriteMapHeader(kv.Length >> 1);
        for (int i = 0; i + 1 < kv.Length; i += 2)
        {
            w.Write(kv[i]);
            w.Write(kv[i + 1]);
        }
    }

    private static void WriteBounds(ref MessagePackWriter w, double[]? bounds)
    {
        if (bounds is null) { w.WriteArrayHeader(0); return; }
        w.WriteArrayHeader(bounds.Length);
        foreach (var b in bounds) w.Write(b);
    }

    private static void WritePoints(ref MessagePackWriter w, ReadOnlySpan<MetricDataPoint> pts)
    {
        w.WriteArrayHeader(pts.Length);
        long prevMs = 0;
        long prevCount = 0;
        double prevSum = 0;
        long[]? prevBuckets = null;
        for (int i = 0; i < pts.Length; i++)
        {
            ref readonly var p = ref pts[i];
            long ms = p.TimestampUnixNano / 1_000_000;

            // Slim shape when the histogram state (count / sum / cumulative
            // buckets) is unchanged from the previous point — the reader inherits
            // it. Covers scalar kinds (state is always zero) and every idle
            // export of a cumulative histogram, which on quiet services is the
            // overwhelming majority of points.
            bool same = p.Count == prevCount && p.Sum == prevSum && BucketsEqual(p.BucketCounts, prevBuckets);
            w.WriteArrayHeader(same ? 2 : 5);

            // First point: absolute unix ms; the rest: delta to the previous point.
            w.Write(i == 0 ? ms : ms - prevMs);
            prevMs = ms;

            WriteNumber(ref w, p.Value);
            if (same) continue;

            w.Write(p.Count);
            WriteNumber(ref w, p.Sum);
            if (p.BucketCounts is { Length: > 0 } bc)
            {
                w.WriteArrayHeader(bc.Length);
                foreach (var c in bc) w.Write(c);
            }
            else
            {
                w.WriteNil();
            }
            prevCount   = p.Count;
            prevSum     = p.Sum;
            prevBuckets = p.BucketCounts;
        }
    }

    /// <summary>Bucket equality where null and all-zero are equivalent (both mean "nothing observed").</summary>
    private static bool BucketsEqual(long[]? a, long[]? b)
    {
        if (ReferenceEquals(a, b)) return true;
        if (a is null) return !HasAnyCount(b);
        if (b is null) return !HasAnyCount(a);
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++) if (a[i] != b[i]) return false;
        return true;
    }

    private static bool HasAnyCount(long[]? buckets)
    {
        if (buckets is null) return false;
        foreach (var b in buckets) if (b != 0) return true;
        return false;
    }

    /// <summary>Writes an integral double as a msgpack varint (1–9 B instead of a fixed 9 B float64).</summary>
    private static void WriteNumber(ref MessagePackWriter w, double v)
    {
        if (double.IsFinite(v) && Math.Floor(v) == v && Math.Abs(v) <= 9.0e15)
            w.Write((long)v);
        else
            w.Write(v);
    }

    /// <summary>
    /// The UTF-8 bytes a file name gives the metric's name (see <see cref="FileNamePart"/>): the rest
    /// of the name is at most ~75 bytes, so a file name stays far below the 255 that NTFS counts in
    /// UTF-16 units and ext4 in BYTES.
    /// </summary>
    internal const int MaxNamePartBytes = 64;

    /// <summary>
    /// The metric's part of a <c>.mts</c> file name: the sanitized name, when its UTF-8 form fits
    /// <see cref="MaxNamePartBytes"/>; otherwise its longest prefix that leaves room for <c>~</c> and
    /// eight hex digits of a stable hash of the FULL name (FNV-1a over its UTF-16 units), so two long
    /// names that share the prefix still read apart.
    ///
    /// <para><b>Capped, because the whole name used to go in</b> (#106 review, P1). OpenTelemetry
    /// allows 255-character metric names; past ~186 ASCII characters (~90 Cyrillic on Linux, where the
    /// limit is bytes and sanitizing keeps letters) the file name passed 255, <c>File.Create</c> threw,
    /// and the whole <see cref="Write"/> was retracted — the flush failed and put its snapshot back,
    /// every flush after it failed the same way for as long as that series stayed in the tier, and
    /// the other metrics' files of each of those flushes were deleted with it. Nothing reads the
    /// name back — the catalog, queries and compaction take the metric from the file's own index —
    /// so a cap changes the path and not a byte inside the file.</para>
    /// </summary>
    internal static string FileNamePart(string metricName)
    {
        string sanitized = SanitizeName(metricName);
        if (Encoding.UTF8.GetByteCount(sanitized) <= MaxNamePartBytes) return sanitized;

        // Every character left is a BMP non-surrogate (SanitizeName replaces surrogates), so 1-3 bytes.
        int budget = MaxNamePartBytes - 9, bytes = 0, n = 0;
        while (n < sanitized.Length)
        {
            char c = sanitized[n];
            int  b = c < 0x80 ? 1 : c < 0x800 ? 2 : 3;
            if (bytes + b > budget) break;
            bytes += b;
            n++;
        }

        uint hash = 2166136261;
        foreach (char c in metricName) { hash ^= c; hash *= 16777619; }

        return string.Create(n + 9, (sanitized, n, hash), static (span, s) =>
        {
            s.sanitized.AsSpan(0, s.n).CopyTo(span);
            span[s.n] = '~';
            s.hash.TryFormat(span[(s.n + 1)..], out _, "x8", System.Globalization.CultureInfo.InvariantCulture);
        });
    }

    /// <summary>The name with every character but a letter, a digit, '-' and '_' replaced by '_' — the name itself when there is none to replace.</summary>
    private static string SanitizeName(string name)
    {
        int i = 0;
        while (i < name.Length && IsNameChar(name[i])) i++;
        if (i == name.Length) return name;
        return string.Create(name.Length, name, static (span, n) =>
        {
            for (int j = 0; j < span.Length; j++) span[j] = IsNameChar(n[j]) ? n[j] : '_';
        });
    }

    private static bool IsNameChar(char c) => char.IsLetterOrDigit(c) || c == '-' || c == '_';

    /// <summary>
    /// The section's msgpack, built in ONE buffer for a whole <see cref="Write"/> call and reset per
    /// file. It grows by taking twice the size and giving the old one back, and is given back when the
    /// call ends, thrown or not.
    ///
    /// <para><b>Pooled up to <see cref="MaxPooledBytes"/>, and no further</b> (#94). The pool rather
    /// than a buffer kept by the writer: this runs on the flush and on every rollup and compaction
    /// pass, minutes apart, and a buffer held between them is held for nothing. But what the shared
    /// pool is handed it keeps until a full collection under pressure trims it, and a section runs to
    /// ~15 MB (512 series of a busy histogram): returned, that is 16 MB of gen2 held for the next
    /// caller who may never come, on a 512 MB stand. So a buffer over a megabyte — the common
    /// section is a few hundred KB — is allocated for the call and dropped with it, and only the
    /// small ones go back. <see cref="ReturnedToPoolForTest"/> sees every size handed back.</para>
    /// </summary>
    private sealed class RentedBufferWriter : IBufferWriter<byte>, IDisposable
    {
        private const int InitialBytes = 64 * 1024;

        private byte[] _buf = Take(InitialBytes);
        private int    _len;

        public ReadOnlySpan<byte> WrittenSpan => _buf.AsSpan(0, _len);

        public void Reset() => _len = 0;

        public void Advance(int count)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(count);
            if (count > _buf.Length - _len) throw new InvalidOperationException("Advanced past the end of the buffer.");
            _len += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0) { Ensure(sizeHint); return _buf.AsMemory(_len); }

        public Span<byte> GetSpan(int sizeHint = 0) { Ensure(sizeHint); return _buf.AsSpan(_len); }

        private void Ensure(int sizeHint)
        {
            int need = Math.Max(sizeHint, 1);
            if (_buf.Length - _len >= need) return;
            long want = Math.Max((long)_len + need, (long)_buf.Length * 2);
            var  next = Take((int)Math.Min(want, Array.MaxLength));
            _buf.AsSpan(0, _len).CopyTo(next);
            Give(_buf);
            _buf = next;
        }

        public void Dispose()
        {
            var buf = _buf;
            _buf = [];
            _len = 0;
            if (buf.Length > 0) Give(buf);
        }

        /// <summary>From the pool up to <see cref="MaxPooledBytes"/> (whose buckets never exceed it); allocated above.</summary>
        private static byte[] Take(int size) =>
            size <= MaxPooledBytes ? ArrayPool<byte>.Shared.Rent(size) : GC.AllocateUninitializedArray<byte>(size);

        /// <summary>Back to the pool when it is small enough to be worth keeping; left to the collector otherwise.</summary>
        private static void Give(byte[] buf)
        {
            if (buf.Length > MaxPooledBytes) return;
            ArrayPool<byte>.Shared.Return(buf);
            ReturnedToPoolForTest?.Invoke(buf.Length);
        }
    }

    /// <summary>
    /// The largest buffer the writer hands back to the shared pool, in bytes — the section buffer
    /// (see <see cref="RentedBufferWriter"/>) and the grouping's index arrays alike.
    /// </summary>
    internal const int MaxPooledBytes = 1024 * 1024;

    /// <summary>
    /// An index array for the grouping (<c>order</c>, <c>ends</c>, <c>groupAt</c>): pooled up to
    /// <see cref="MaxPooledBytes"/>, allocated above it (#106 review, F2). They are sized by the
    /// whole snapshot, and a flush of more than 262 144 series parked three arrays of a megabyte or
    /// more in <c>ArrayPool&lt;int&gt;.Shared</c> for good — the retention the section buffer's cap
    /// removed, by another door.
    /// </summary>
    private static int[] TakeInts(int count) =>
        (long)count * sizeof(int) <= MaxPooledBytes ? ArrayPool<int>.Shared.Rent(count) : GC.AllocateUninitializedArray<int>(count);

    /// <summary>Back to the pool when it is small enough to be worth keeping; left to the collector otherwise.</summary>
    private static void GiveInts(int[] array)
    {
        long bytes = (long)array.Length * sizeof(int);
        if (bytes > MaxPooledBytes) return;
        ArrayPool<int>.Shared.Return(array);
        ReturnedToPoolForTest?.Invoke((int)bytes);
    }

    /// <summary>
    /// Test seam: the size in bytes of every buffer the writer hands back to the shared pool — section
    /// buffers and index arrays — on the writing thread (the writer is synchronous, so a thread-static
    /// reaches only the setting test's calls). Null in production.
    /// </summary>
    [ThreadStatic] internal static Action<int>? ReturnedToPoolForTest;
}
