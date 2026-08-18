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
    /// (several, if a name carries more than <see cref="MaxSeriesPerFile"/> series). Returns
    /// metadata for all created files.
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
        var byMetric = series.GroupBy(s => s.Key.Name);
        var result   = new List<MetricSegmentInfo>();
        var staged   = new List<string>();   // temp paths, index-aligned with result
        int published = 0;

        try
        {
            foreach (var group in byMetric)
            foreach (var items in Chunk(group.ToList(), MaxSeriesPerFile))
            {
                long minNano = long.MaxValue, maxNano = long.MinValue;
                foreach (var (_, hs) in items)
                {
                    var pts = hs.GetPoints(long.MinValue, long.MaxValue);
                    if (pts.Count > 0)
                    {
                        minNano = Math.Min(minNano, pts[0].TimestampUnixNano);
                        maxNano = Math.Max(maxNano, pts[^1].TimestampUnixNano);
                    }
                }
                if (minNano == long.MaxValue) continue;

                // A short nonce keeps the name unique: the (name, min, max, granularity)
                // tuple is NOT — a v2→v3 migration re-writes the same time range, and two
                // files of the same range can legitimately coexist until the next merge
                // dedupes them. Without it, WriteFile's FileMode.CreateNew throws
                // "already exists" and the compaction fails every pass. The name is never
                // parsed back (all metadata is read from the file's own header/index).
                string suffix   = granularity == MetricGranularity.Raw ? "raw" : granularity.ToString().ToLower();
                string nonce    = Guid.NewGuid().ToString("N").Substring(0, 8);
                string fileName = $"metrics-{SanitizeName(group.Key)}-{minNano}-{maxNano}-{suffix}-{nonce}.mts";
                string filePath = Path.Combine(dataDir, fileName);
                string tmpPath  = filePath + TempSuffix;

                try
                {
                    WriteFile(tmpPath, group.Key, granularity, items, minNano, maxNano, duringFileWrite);

                    // Staged before it is described, so the retraction below owns the file from
                    // the instant it exists. Sized off the temp file too: reading the length
                    // through the final path is one more call that can throw, and it would throw
                    // with a complete file sitting at a path nothing has recorded.
                    staged.Add(tmpPath);
                    result.Add(new MetricSegmentInfo
                    {
                        FilePath      = filePath,
                        MetricName    = group.Key,
                        MinNano       = minNano,
                        MaxNano       = maxNano,
                        Granularity   = granularity,
                        FormatVersion = Version,
                        SizeBytes     = new FileInfo(tmpPath).Length,
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

        return result;
    }

    /// <summary>Splits a metric's series into files of at most <paramref name="size"/> series.</summary>
    private static IEnumerable<List<(SeriesKey Key, HotSeries Series)>> Chunk(
        List<(SeriesKey Key, HotSeries Series)> items, int size)
    {
        if (items.Count <= size) { yield return items; yield break; }
        for (int i = 0; i < items.Count; i += size)
            yield return items.GetRange(i, Math.Min(size, items.Count - i));
    }

    /// <summary>
    /// Builds one complete file at <paramref name="filePath"/>, which is a temp name: the
    /// caller renames it. Nothing here may assume the file survives — every throw between the
    /// open and the close leaves a file with no footer, and it is the temp name that keeps the
    /// reader from ever meeting one.
    /// </summary>
    private static void WriteFile(
        string filePath,
        string metricName,
        MetricGranularity granularity,
        List<(SeriesKey Key, HotSeries Series)> items,
        long minNano,
        long maxNano,
        Action<string>? duringFileWrite = null)
    {
        // Serialize ALL series into one msgpack buffer, then compress it as a
        // single LZ4-HC block: repeated label keys/values across series (routes,
        // instance ids, GUIDs) deduplicate inside the shared compression window.
        var bufWriter = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new MessagePackWriter(bufWriter);
        foreach (var (key, hs) in items)
        {
            var pts = hs.GetPoints(long.MinValue, long.MaxValue);
            WriteSeries(ref w, key, hs.Bounds, pts);
        }
        w.Flush();

        var raw        = bufWriter.WrittenSpan.ToArray();
        // HC level: writes happen on background flush/rollup threads only, so we
        // trade CPU for the much better ratio.
        var compressed = LZ4Pickler.Pickle(raw, LZ4Level.L09_HC);

        using var fs = new FileStream(filePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 65536);
        using var bw = new BinaryWriter(fs);

        // Header
        bw.Write(Magic);
        bw.Write(Version);
        bw.Write((byte)granularity);
        bw.Write((uint)items.Count);
        bw.Write(minNano);
        bw.Write(maxNano);
        bw.Write((byte)0); // flags

        // Section
        bw.Write((uint)raw.Length);
        bw.Write((uint)compressed.Length);
        bw.Write(compressed);

        // Header and payload down, index and footer to go: the state a disk dies in. Null in
        // production — one delegate read per file, not per series.
        duringFileWrite?.Invoke(filePath);

        // Name index
        long nameIdxOffset = fs.Position;
        bw.Write((uint)1); // only one distinct name per file
        var nameBytes = System.Text.Encoding.UTF8.GetBytes(metricName);
        bw.Write((ushort)nameBytes.Length);
        bw.Write(nameBytes);
        bw.Write((ulong)0);                 // block offset — unused in v3 (single section)
        bw.Write((uint)items.Count);

        // Footer
        bw.Write((ulong)nameIdxOffset);
        bw.Write(FooterMagic);

        bw.Flush();
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
    }

    private static void WriteSeries(
        ref MessagePackWriter w,
        SeriesKey key,
        double[]? bounds,
        IReadOnlyList<MetricDataPoint> points)
    {
        w.WriteMapHeader(6);
        w.Write("k");    w.Write((byte)key.Kind);
        w.Write("u");    w.Write(key.Unit);
        w.Write("lbs");  WriteLabels(ref w, key.Labels);
        w.Write("bnds"); WriteBounds(ref w, bounds);
        w.Write("pts");  WritePoints(ref w, points);
        w.Write("cnt");  w.Write((uint)points.Count);
    }

    private static void WriteLabels(ref MessagePackWriter w, LabelSet labels)
    {
        var pairs = labels.Pairs;
        w.WriteMapHeader(pairs.Count);
        foreach (var (k, v) in pairs)
        {
            w.Write(k);
            w.Write(v);
        }
    }

    private static void WriteBounds(ref MessagePackWriter w, double[]? bounds)
    {
        if (bounds is null) { w.WriteArrayHeader(0); return; }
        w.WriteArrayHeader(bounds.Length);
        foreach (var b in bounds) w.Write(b);
    }

    private static void WritePoints(ref MessagePackWriter w, IReadOnlyList<MetricDataPoint> pts)
    {
        w.WriteArrayHeader(pts.Count);
        long prevMs = 0;
        long prevCount = 0;
        double prevSum = 0;
        long[]? prevBuckets = null;
        for (int i = 0; i < pts.Count; i++)
        {
            var p = pts[i];
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

    private static string SanitizeName(string name) =>
        string.Concat(name.Select(c => char.IsLetterOrDigit(c) || c == '-' || c == '_' ? c : '_'));
}
