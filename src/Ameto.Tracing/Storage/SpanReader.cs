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

        return new SpanSegmentInfo
        {
            FilePath      = filePath,
            MinStartNano  = minNano,
            MaxStartNano  = maxNano,
            SpanCount     = (int)spanCount,
            Services      = services,
            FormatVersion = version,
        };
    }

    // ── Trace lookup ───────────────────────────────────────────────────────────

    public static async IAsyncEnumerable<SpanRecord> ReadTraceAsync(
        string    filePath,
        TraceId   traceId,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var offsets = ReadTraceOffsets(filePath, traceId);
        if (offsets.Count == 0) yield break;

        var all = ReadSpansFromFile(filePath);
        foreach (var o in offsets)
        {
            ct.ThrowIfCancellationRequested();
            if (o < (uint)all.Count) yield return all[(int)o];
        }
    }

    // ── Search with service-index block skip ───────────────────────────────────

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

        var spans = ReadSpansFromFile(filePath, allowedBlocks);
        foreach (var s in spans)
        {
            ct.ThrowIfCancellationRequested();
            if (s.StartTimeUnixNano < fromNano || s.StartTimeUnixNano > toNano) continue;
            if (serviceName      is not null && !s.ServiceName.Equals(serviceName, StringComparison.OrdinalIgnoreCase)) continue;
            if (spanName         is not null && !s.Name.Contains(spanName, StringComparison.OrdinalIgnoreCase)) continue;
            if (status           is not null && s.Status != status.Value) continue;
            if (httpStatusCode   is not null && s.HttpStatusCode != httpStatusCode.Value) continue;
            if (minDurationNanos is not null && s.DurationNanos < minDurationNanos.Value) continue;
            if (maxDurationNanos is not null && s.DurationNanos > maxDurationNanos.Value) continue;
            yield return s;
        }
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

            if (uncompSize > 10_000_000 || compSize > 10_000_000) // 10MB per block limit
                throw new InvalidDataException($"Block {blockIdx} size too large: uncompressed={uncompSize}, compressed={compSize}");

            if (totalBytesRead + compSize > MaxTotalBytes)
                throw new InvalidDataException($"Total data exceeds {MaxTotalBytes} bytes limit");

            var compBytes = br.ReadBytes((int)compSize);
            totalBytesRead += compSize;

            try
            {
                var raw = LZ4Pickler.Unpickle(compBytes);
                var reader = new MessagePackReader(raw);
                int cnt = reader.ReadArrayHeader();
                if (cnt > 50_000) // Safety limit for spans per block
                    throw new InvalidDataException($"Block {blockIdx} contains too many spans: {cnt}");

                long prevTs = 0;
                for (int i = 0; i < cnt; i++)
                {
                    if (result.Count >= 1_000_000) // Total span limit
                        throw new InvalidDataException($"Total span count exceeds 1,000,000");
                    result.Add(version >= 3
                        ? DeserializeSpanV3(ref reader, i == 0, ref prevTs)
                        : DeserializeSpan(ref reader));
                }
            }
            catch (OutOfMemoryException ex)
            {
                throw new InvalidDataException($"Out of memory while processing block {blockIdx} (size: {compSize} bytes)", ex);
            }
            blockIdx++;
        }
        return result;
    }

    // ── Core block reader ──────────────────────────────────────────────────────

    /// <param name="allowedBlocks">
    /// When non-null, only blocks whose 0-based index is in this set are decompressed.
    /// Non-allowed blocks are skipped via <c>Seek</c>, saving LZ4 + msgpack work.
    /// </param>
    private static List<SpanRecord> ReadSpansFromFile(
        string         filePath,
        HashSet<uint>? allowedBlocks = null)
    {
        using var fs = OpenRead(filePath);
        using var br = new BinaryReader(fs);

        br.ReadUInt32(); // magic
        ushort version = br.ReadUInt16();
        int spanCount = (int)br.ReadUInt32();
        br.ReadInt64();  // minNano
        br.ReadInt64();  // maxNano
        br.ReadByte();   // flags

        var (traceIdxOffset, _, _) = ReadFooter(fs, br, version);
        fs.Seek(27, SeekOrigin.Begin); // reset to after header

        int capacity = allowedBlocks is null ? spanCount : spanCount / 2;
        var result   = new List<SpanRecord>(capacity);

        uint blockIdx = 0;
        while (fs.Position < traceIdxOffset)
        {
            uint uncompSize = br.ReadUInt32();
            uint compSize   = br.ReadUInt32();

            if (allowedBlocks is not null && !allowedBlocks.Contains(blockIdx))
            {
                // Skip decompression + deserialization — pure seek, O(1).
                // Safe for v3 too: the Δts chain restarts on every block.
                fs.Seek(compSize, SeekOrigin.Current);
            }
            else
            {
                var compBytes = br.ReadBytes((int)compSize);
                var raw       = LZ4Pickler.Unpickle(compBytes);
                var reader    = new MessagePackReader(raw);
                int cnt       = reader.ReadArrayHeader();
                long prevTs   = 0;
                for (int i = 0; i < cnt; i++)
                    result.Add(version >= 3
                        ? DeserializeSpanV3(ref reader, i == 0, ref prevTs)
                        : DeserializeSpan(ref reader));
            }

            blockIdx++;
        }

        return result;
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
            br.ReadUInt32(); // uncompSize
            uint compSize = br.ReadUInt32();
            var  raw      = LZ4Pickler.Unpickle(br.ReadBytes((int)compSize));

            int pos = 0;
            uint traceCount = BinaryPrimitives.ReadUInt32LittleEndian(raw.AsSpan(pos)); pos += 4;
            for (uint i = 0; i < traceCount; i++)
            {
                var candidate = TraceId.Parse(raw.AsSpan(pos, 16)); pos += 16;
                uint offsetCnt = BinaryPrimitives.ReadUInt32LittleEndian(raw.AsSpan(pos)); pos += 4;
                if (candidate.Equals(traceId))
                {
                    var offsets = new List<uint>((int)offsetCnt);
                    for (uint j = 0; j < offsetCnt; j++)
                    {
                        offsets.Add(BinaryPrimitives.ReadUInt32LittleEndian(raw.AsSpan(pos)));
                        pos += 4;
                    }
                    return offsets;
                }
                pos += (int)offsetCnt * 4;
            }
            return [];
        }

        uint count = br.ReadUInt32();
        var  idBuf = new byte[16];

        for (uint i = 0; i < count; i++)
        {
            br.Read(idBuf, 0, 16);
            var candidate  = TraceId.Parse(idBuf);
            uint offsetCnt = br.ReadUInt32();

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
        if (bloomIdxOffset <= 0) return null;
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

        uint blockCount = br.ReadUInt32();
        var allowed = new HashSet<uint>((int)blockCount);
        for (uint b = 0; b < blockCount; b++)
        {
            uint len = br.ReadUInt32();
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
        fs.Seek(svcIdxOffset, SeekOrigin.Begin);

        uint svcCount = br.ReadUInt32();
        for (uint i = 0; i < svcCount; i++)
        {
            ushort nameLen = br.ReadUInt16();
            var    name    = Encoding.UTF8.GetString(br.ReadBytes(nameLen));
            uint   blkCnt  = br.ReadUInt32();

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

    private static string[] ReadServicesFromIndex(FileStream fs, BinaryReader br, long svcIdxOffset)
    {
        fs.Seek(svcIdxOffset, SeekOrigin.Begin);
        uint count    = br.ReadUInt32();
        var  services = new string[count];
        for (uint i = 0; i < count; i++)
        {
            ushort nameLen = br.ReadUInt16();
            services[i]   = Encoding.UTF8.GetString(br.ReadBytes(nameLen));
            uint blkCnt   = br.ReadUInt32();
            fs.Seek(blkCnt * 4L, SeekOrigin.Current);
        }
        return services;
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
    /// </summary>
    private static SpanRecord DeserializeSpanV3(ref MessagePackReader r, bool first, ref long prevTs)
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

        IReadOnlyDictionary<string, object?>? attrs = null;
        if (r.TryReadNil())
        {
            // no attributes
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

    private static SpanRecord DeserializeSpan(ref MessagePackReader r)
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
