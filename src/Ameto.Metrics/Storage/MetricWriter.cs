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
        int[] order = ArrayPool<int>.Shared.Rent(Math.Max(count, 1));
        int[] ends  = ArrayPool<int>.Shared.Rent(Math.Max(count, 1));
        using var raw = new RentedBufferWriter();
        Span<char> nonceChars = stackalloc char[32];
        try
        {
            int groups = GroupByName(series, order.AsSpan(0, count), ends.AsSpan(0, count));
            int start  = 0;
            for (int g = 0; g < groups; g++)
            {
                int    end        = ends[g];
                string metricName = series[order[start]].Key.Name;   // the group's key, as GroupBy's was: its first item's
                for (int from = start; from < end; from += MaxSeriesPerFile)
                {
                    ReadOnlySpan<int> items = order.AsSpan(from, Math.Min(MaxSeriesPerFile, end - from));

                    // Serialize ALL the file's series into one msgpack buffer, then compress it as a
                    // single LZ4-HC block: repeated label keys/values across series (routes, instance
                    // ids, GUIDs) deduplicate inside the shared compression window. The file's time
                    // range is taken on the same pass, from each series' first and last point.
                    raw.Reset();
                    var  w       = new MessagePackWriter(raw);
                    long minNano = long.MaxValue, maxNano = long.MinValue;
                    foreach (int i in items)
                    {
                        var (key, hs) = series[i];
                        ReadOnlySpan<MetricDataPoint> pts = hs.PointsForWrite();
                        if (pts.Length > 0)
                        {
                            minNano = Math.Min(minNano, pts[0].TimestampUnixNano);
                            maxNano = Math.Max(maxNano, pts[^1].TimestampUnixNano);
                        }
                        WriteSeries(ref w, key, hs.Bounds, pts);
                    }
                    w.Flush();
                    if (minNano == long.MaxValue) continue;   // not one point: no file

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
                    string fileName = $"metrics-{SanitizeName(metricName)}-{minNano}-{maxNano}-{Suffix(granularity)}-{nonce}.mts";
                    string filePath = Path.Combine(dataDir, fileName);
                    string tmpPath  = filePath + TempSuffix;

                    try
                    {
                        long size = WriteFile(tmpPath, metricName, granularity, items.Length, minNano, maxNano,
                                              raw.WrittenSpan.Length, compressed, duringFileWrite);

                        // Staged before it is described, so the retraction below owns the file from
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
            ArrayPool<int>.Shared.Return(order);
            ArrayPool<int>.Shared.Return(ends);
        }

        return result;
    }

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
        int[] groupAt = ArrayPool<int>.Shared.Rent(n);
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
        finally { ArrayPool<int>.Shared.Return(groupAt); }
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
    /// The section's msgpack, built in ONE buffer rented from the shared pool for a whole
    /// <see cref="Write"/> call and reset per file. It grows by renting twice the size and returning
    /// the old one, and goes back to the pool when the call ends, thrown or not. The pool rather than
    /// a buffer kept by the writer: this runs on the flush and on every rollup and compaction pass,
    /// minutes apart, and a buffer held between them is a buffer held for nothing — the pool trims
    /// what nobody rents.
    /// </summary>
    private sealed class RentedBufferWriter : IBufferWriter<byte>, IDisposable
    {
        private const int InitialBytes = 64 * 1024;

        private byte[] _buf = ArrayPool<byte>.Shared.Rent(InitialBytes);
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
            var  next = ArrayPool<byte>.Shared.Rent((int)Math.Min(want, Array.MaxLength));
            _buf.AsSpan(0, _len).CopyTo(next);
            ArrayPool<byte>.Shared.Return(_buf);
            _buf = next;
        }

        public void Dispose()
        {
            var buf = _buf;
            _buf = [];
            _len = 0;
            if (buf.Length > 0) ArrayPool<byte>.Shared.Return(buf);
        }
    }
}
