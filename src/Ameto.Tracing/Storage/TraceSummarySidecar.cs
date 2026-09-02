using System.Text;
using K4os.Compression.LZ4;

namespace Ameto.Tracing.Storage;

/// <summary>
/// One pre-aggregated row per trace, derived at flush time. Lets the trace-list and
/// trace-stats endpoints answer without deserialising a single span (analogous to the
/// <c>.stats</c> / <c>.svcgraph</c> sidecars).
/// </summary>
public sealed class TraceSummary
{
    public TraceId        TraceId        { get; init; }
    public SpanId         RootSpanId     { get; init; }
    public long           RootStartNano  { get; init; }
    public long           DurationNanos  { get; init; }
    public uint           SpanCount      { get; init; }
    public bool           HasRoot        { get; init; }
    public bool           HasError       { get; init; }
    public SpanStatusCode RootStatus     { get; init; }
    public short          HttpStatusCode { get; init; }
    public string         Name           { get; init; } = string.Empty;
    public string         ServiceName    { get; init; } = string.Empty;
    public string         HttpMethod     { get; init; } = string.Empty;
    public string         HttpPath       { get; init; } = string.Empty;
    /// <summary>Union of service names across the trace's spans in this segment.</summary>
    public string[]       Services       { get; init; } = [];
}

/// <summary>Sparse trace-volume bucket on a fixed <see cref="TraceSummarySidecar.GridNanos"/> grid.</summary>
public readonly record struct TraceVolumeEntry(long GridIndex, uint TraceCount, uint ErrorCount);

/// <summary>Header-only view of a <c>.tracesum</c> file — enough for volume/sparkline, no body read.</summary>
public sealed class TraceVolumeSegment
{
    public long                    MinStartNano { get; init; }
    public long                    MaxStartNano { get; init; }
    public List<TraceVolumeEntry>  Buckets      { get; init; } = [];
}

/// <summary>
/// Builds and reads the <c>.tracesum</c> companion sidecar.
///
/// <para>Binary format "RDTV":</para>
/// <code>
///   Magic uint32 | Version uint16
///   MinStartNano int64 | MaxStartNano int64
///   [Volume header — uncompressed, tiny]
///     volCount uint32
///     per bucket: gridIndex int64 | traceCount uint32 | errorCount uint32   (16 B each)
///   [Body — LZ4-pickled]
///     bodyUncompSize uint32 | bodyCompSize uint32 | LZ4 bytes of:
///       serviceCount uint32 | per service: nameLen uint16 | UTF-8
///       traceCount   uint32 | per trace: fixed prefix + name/method/path + service indices
/// </code>
/// </summary>
internal static class TraceSummarySidecar
{
    private const uint   Magic     = 0x52_44_54_56; // "RDTV"
    private const ushort Version   = 1;

    /// <summary>
    /// The largest decompressed body this reader will build. Every number below is copied out of a
    /// file, and a length prefix that has been torn is a request for that many bytes BEFORE anything
    /// discovers the file is shorter — <c>BinaryReader.ReadBytes</c> allocates `new byte[count]` and
    /// only then copies whatever it actually found. Measured on this reader before it was bounded: a
    /// 359-byte sidecar with its compressed-size field overwritten allocated 700 294 448 bytes, the
    /// read SUCCEEDED, the page came back rows=20 with no fault of any kind — and the same 668 MB was
    /// paid again on every page of every stream, because this runs once per segment per page.
    ///
    /// <para>256 MB is far above any body this writer produces (a segment's summaries compress to
    /// kilobytes) and far below the point where a torn field costs the process.</para>
    /// </summary>
    private const int MaxBodyBytes = 256 * 1024 * 1024;

    /// <summary>Paths already reported as having an unreadable volume header, so a poll every
    /// fifteen seconds does not become a log every fifteen seconds.</summary>
    private static HashSet<string>? _volumeWarned;

    /// <summary>Volume grid resolution — 10 s. Sparse, so idle gaps cost nothing.</summary>
    public const long GridNanos = 10_000_000_000L;

    private static readonly string[] MethodKeys = { "http.request.method", "http.method" };
    private static readonly string[] PathKeys   = { "url.path", "http.target", "http.route", "url.full", "http.url" };

    // ── Writer ──────────────────────────────────────────────────────────────────

    public static void Write(string baseTrcPath, IList<SpanRecord> spans, string? outputPath = null)
    {
        if (spans.Count == 0) return;

        // One pass: group spans by trace id into per-trace accumulators.
        var traces = new Dictionary<TraceId, Acc>(spans.Count / 2 + 1);
        long segMin = long.MaxValue, segMax = long.MinValue;

        for (int i = 0; i < spans.Count; i++)
        {
            var s = spans[i];
            if (s.StartTimeUnixNano < segMin) segMin = s.StartTimeUnixNano;
            if (s.StartTimeUnixNano > segMax) segMax = s.StartTimeUnixNano;

            if (!traces.TryGetValue(s.TraceId, out var a))
            {
                a = new Acc { TraceId = s.TraceId };
                traces[s.TraceId] = a;
            }

            a.SpanCount++;
            if (s.Status == SpanStatusCode.Error) a.HasError = true;
            (a.Services ??= new HashSet<string>(2, StringComparer.Ordinal)).Add(s.ServiceName);

            if (s.StartTimeUnixNano < a.EarliestNano)
            {
                a.EarliestNano  = s.StartTimeUnixNano;
                a.FirstService  = s.ServiceName;
            }

            // First empty-parent span wins the "root" slot.
            if (s.ParentSpanId.IsEmpty && !a.HasRoot)
            {
                a.HasRoot        = true;
                a.RootSpanId     = s.SpanId;
                a.RootStartNano  = s.StartTimeUnixNano;
                a.RootDurNanos   = s.DurationNanos;
                a.RootStatus     = s.Status;
                a.RootHttpStatus = s.HttpStatusCode;
                a.RootName       = s.Name;
                a.RootService    = s.ServiceName;
                a.RootMethod     = GetAttr(s.Attributes, MethodKeys);
                a.RootPath       = GetAttr(s.Attributes, PathKeys);
            }
        }

        // Volume histogram on the fixed grid (keyed by each trace's representative start).
        var vol = new Dictionary<long, VolCell>(traces.Count);
        foreach (var a in traces.Values)
        {
            long grid = (a.HasRoot ? a.RootStartNano : a.EarliestNano) / GridNanos;
            vol.TryGetValue(grid, out var cell);
            cell.Traces++;
            if (a.HasError) cell.Errors++;
            vol[grid] = cell;
        }

        // Service pool (dedupes repeated service names across trace rows).
        var pool    = new Dictionary<string, int>(StringComparer.Ordinal);
        var poolArr = new List<string>();
        int Intern(string name)
        {
            if (name.Length == 0) return -1;
            if (pool.TryGetValue(name, out var idx)) return idx;
            idx = poolArr.Count;
            pool[name] = idx;
            poolArr.Add(name);
            return idx;
        }

        // Serialise the body first (needs the pool built up).
        byte[] rawBody;
        using (var bodyMs = new MemoryStream(traces.Count * 64))
        using (var bw = new BinaryWriter(bodyMs, Encoding.UTF8, leaveOpen: true))
        {
            // Reserve pool position — write traces into a temp, interning as we go, then
            // write pool + traces. Simpler: iterate twice — intern in first pass already
            // done for services set; do row writing after pool is known. We build rows
            // into a scratch stream while interning, then prepend the pool.
            using var rowsMs = new MemoryStream(traces.Count * 48);
            using (var rw = new BinaryWriter(rowsMs, Encoding.UTF8, leaveOpen: true))
            {
                rw.Write((uint)traces.Count);
                Span<byte> tid = stackalloc byte[16];
                foreach (var a in traces.Values)
                {
                    a.TraceId.WriteTo(tid);
                    rw.Write(tid);
                    rw.Write(a.RootSpanId.RawValue);
                    rw.Write(a.HasRoot ? a.RootStartNano : a.EarliestNano);
                    rw.Write(a.HasRoot ? a.RootDurNanos  : 0L);
                    rw.Write(a.SpanCount);

                    byte flags = 0;
                    if (a.HasRoot)  flags |= 0b01;
                    if (a.HasError) flags |= 0b10;
                    rw.Write(flags);
                    rw.Write((byte)a.RootStatus);
                    rw.Write(a.RootHttpStatus);
                    rw.Write(Intern(a.HasRoot ? a.RootService : a.FirstService));

                    WriteStr16(rw, a.HasRoot ? a.RootName   : string.Empty);
                    WriteStr8 (rw, a.HasRoot ? a.RootMethod : string.Empty);
                    WriteStr16(rw, a.HasRoot ? a.RootPath   : string.Empty);

                    var svcs = a.Services!;
                    rw.Write((ushort)svcs.Count);
                    foreach (var sv in svcs) rw.Write(Intern(sv));
                }
            }

            // Now write pool, then the rows blob.
            bw.Write((uint)poolArr.Count);
            foreach (var name in poolArr)
            {
                var nb = Encoding.UTF8.GetBytes(name);
                bw.Write((ushort)nb.Length);
                bw.Write(nb);
            }
            rowsMs.Position = 0;
            rowsMs.CopyTo(bodyMs);
            bw.Flush();
            rawBody = bodyMs.ToArray();
        }

        var compBody = LZ4Pickler.Pickle(rawBody);

        string path = outputPath ?? Path.ChangeExtension(baseTrcPath, ".tracesum");
        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 65536);
        using var w  = new BinaryWriter(fs);

        w.Write(Magic);
        w.Write(Version);
        w.Write(segMin);
        w.Write(segMax);

        w.Write((uint)vol.Count);
        foreach (var (grid, cell) in vol)
        {
            w.Write(grid);
            w.Write(cell.Traces);
            w.Write(cell.Errors);
        }

        w.Write((uint)rawBody.Length);
        w.Write((uint)compBody.Length);
        w.Write(compBody);

        w.Flush();
        fs.Flush(flushToDisk: true); // durable before the caller renames and resets the WAL
    }

    // ── Reader: volume header only (cheap) ──────────────────────────────────────

    /// <summary>True when the companion <c>.tracesum</c> sidecar exists for this segment.</summary>
    public static bool Exists(string trcFilePath) =>
        File.Exists(Path.ChangeExtension(trcFilePath, ".tracesum"));

    public static TraceVolumeSegment? ReadVolume(string trcFilePath)
    {
        var path = Path.ChangeExtension(trcFilePath, ".tracesum");
        if (!File.Exists(path)) return null;

        try
        {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 16384, FileOptions.SequentialScan);
            using var br = new BinaryReader(fs);

            if (br.ReadUInt32() != Magic) return null;
            br.ReadUInt16(); // version
            long min = br.ReadInt64();
            long max = br.ReadInt64();

            // Bounded like every other count in this file. The same field is read by
            // TryReadSummaries a few methods down, where it was bounded and here it was not —
            // and this is the WORSE path of the two: /api/traces/stats is polled every fifteen
            // seconds independently of the list, so it runs even while the list is frozen.
            // Measured on a real 2532-byte .tracesum with the volume count set to 0x02000000:
            // 512.2 MB allocated on one stats refresh, and 0x40000000 asks for 17.2 GB.
            uint volCount = br.ReadUInt32();
            FileBounds.RequireCountFits(volCount, fs.Length - fs.Position,
                fileBytesPerElement: 16, heapBytesPerElement: 16, "Volume header", path);

            var buckets = new List<TraceVolumeEntry>((int)volCount);
            for (uint i = 0; i < volCount; i++)
            {
                long grid   = br.ReadInt64();
                uint traces = br.ReadUInt32();
                uint errors = br.ReadUInt32();
                buckets.Add(new TraceVolumeEntry(grid, traces, errors));
            }

            return new TraceVolumeSegment { MinStartNano = min, MaxStartNano = max, Buckets = buckets };
        }
        catch (Exception ex) when (FileBounds.DescribesContent(ex))
        {
            // Damaged volume header. null sends the caller down its legacy fallback, which rescans
            // EVERY span of the segment — measured at 6 156 760 bytes against 19 280 for the healthy
            // read, 319x, on a path /api/traces/stats polls every fifteen seconds. Worth saying once
            // rather than paying silently for ever.
            _volumeWarned ??= new();
            if (_volumeWarned.Add(path))
                Console.Error.WriteLine($"[ameto] trace-volume header in {path} will not parse: {ex.Message}");
            return null;
        }
    }

    // ── Reader: full per-trace rows ─────────────────────────────────────────────

    /// <summary>
    /// Every row in the sidecar, unbounded in time, with "could not read" flattened into "no
    /// rows".
    ///
    /// <para>NOT ON ANY SCAN PATH ANY MORE — its only remaining caller is
    /// <c>TraceSummarySidecarTests</c>, which round-trips a file it has just written and for
    /// which the two answers really are the same. Every production reader goes through
    /// <see cref="TryReadSummaries"/>, because flattening them is what let a segment fall out of
    /// a window and the stream above it still report <c>done {"complete":true}</c>. Kept as the
    /// round-trip's front door and nothing else; a new caller here is almost certainly a bug.</para>
    /// </summary>
    public static List<TraceSummary> ReadSummaries(string trcFilePath) =>
        TryReadSummaries(trcFilePath, long.MinValue, long.MaxValue, out var rows) ? rows : [];

    /// <summary>
    /// The rows whose representative start falls in <c>[fromNano, toNano]</c>, or FALSE when the
    /// sidecar could not be read at all.
    ///
    /// <para>THE RETURN VALUE IS THE POINT. This used to end in <c>catch { return []; }</c>, so a
    /// file that vanished between the <see cref="Exists"/> probe and the open — which compaction
    /// produces by design, publishing its merged output before unlinking its sources — or one a
    /// power cut left truncated, merged as an EMPTY LIST. The walk then ran to the end of the
    /// window, recorded no floor, reported itself uncapped, and the stream above it sent
    /// <c>done {"complete":true}</c> over a window a whole segment had just fallen out of.</para>
    ///
    /// <para>THE RANGE IS PUSHED DOWN, and only that far. The body is one LZ4 blob with no index,
    /// so the whole of it is still decompressed to reach any row — what the bound removes is the
    /// per-row cost the caller used to pay and then throw away: the <see cref="TraceSummary"/>
    /// itself, its <c>Services</c> array, and the three UTF-8 strings, for every trace in a
    /// segment that compaction may have grown to 200 000 spans while the caller's whole budget is
    /// 2 500 rows. A segment nested inside a wider one stays below the cursor for up to the 24 h
    /// <c>SelectCompactionBatch</c> groups within, and is reopened on every page of every stream
    /// for as long as it does.</para>
    /// </summary>
    public static bool TryReadSummaries(
        string trcFilePath, long fromNano, long toNano, out List<TraceSummary> rows)
    {
        rows = [];
        var path = Path.ChangeExtension(trcFilePath, ".tracesum");

        try
        {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 65536, FileOptions.SequentialScan);
            using var br = new BinaryReader(fs);

            if (br.ReadUInt32() != Magic) return false;
            br.ReadUInt16(); // version
            br.ReadInt64();  // min
            br.ReadInt64();  // max

            // EVERY LENGTH BELOW IS BOUNDED BY EVIDENCE THE FILE CANNOT FORGE — the bytes actually
            // left in it, and the ceiling above. Returning false rather than throwing keeps the
            // caller's own classification intact: a sidecar that will not parse is a corrupt
            // sidecar, which it already knows how to say.
            uint volCount = br.ReadUInt32();
            long afterVol = fs.Position + volCount * 16L;
            if (volCount > (fs.Length - fs.Position) / 16) { rows = []; return false; }
            fs.Seek(afterVol, SeekOrigin.Begin); // skip volume header

            uint uncompSize = br.ReadUInt32();
            uint compSize   = br.ReadUInt32();
            if (compSize > fs.Length - fs.Position || uncompSize > MaxBodyBytes) { rows = []; return false; }

            byte[] comp = br.ReadBytes((int)compSize);
            if (comp.Length != compSize) { rows = []; return false; }

            // The size INSIDE the payload, which the check above never saw: LZ4 carries the
            // decompressed length in its own header, so a short well-formed block can still ask for
            // gigabytes. Same reasoning as SpanReader's MaxBlockBytes guard, same failure without it.
            if (LZ4Pickler.UnpickledSize(comp) is < 0 or > MaxBodyBytes) { rows = []; return false; }

            byte[] raw = LZ4Pickler.Unpickle(comp);
            if (raw.Length != uncompSize) { /* tolerate — trust actual length */ }

            rows = ParseBody(raw, fromNano, toNano);
            return true;
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            // ABSENCE IS NOT A READ FAILURE, and this method has always answered it with false —
            // the caller re-probes Exists precisely to tell "the file is gone" from "the file will
            // not parse", which is what separates a compaction handover from a loss. Letting it
            // propagate would hand that question to an exception classifier that cannot answer it.
            rows = [];
            return false;
        }
        catch (Exception ex) when (FileBounds.DescribesContent(ex))
        {
            // Only the exceptions that describe CONTENT are answered here, and answered as false —
            // "this sidecar will not parse". Everything else is left to propagate, because the
            // caller is the only place that can tell a damaged file from a locked one and it was
            // reduced to guessing: a bare catch here meant a sharing violation, an SMB blip or a
            // remount arrived at the caller as "the file exists but would not read", which it
            // hardcoded to Corrupt — a permanent claim over a lock that clears in seconds, with
            // nothing in the log, because this branch logged nothing either.
            rows = [];
            return false;
        }
    }

    private static List<TraceSummary> ParseBody(byte[] raw, long fromNano, long toNano)
    {
        var ms = new MemoryStream(raw, writable: false);
        using var br = new BinaryReader(ms);

        // A string here costs at least its two-byte length prefix, so a pool larger than half the
        // body is a torn count and nothing else.
        uint poolCount = br.ReadUInt32();
        if (poolCount > (raw.Length - ms.Position) / 2) throw new InvalidDataException(
            $"Trace-summary pool count {poolCount} cannot fit in {raw.Length - ms.Position} bytes");
        var pool = new string[poolCount];
        for (uint i = 0; i < poolCount; i++)
        {
            ushort len = br.ReadUInt16();
            pool[i] = Encoding.UTF8.GetString(br.ReadBytes(len));
        }

        string PoolAt(int idx) => idx >= 0 && idx < pool.Length ? pool[idx] : string.Empty;

        uint traceCount = br.ReadUInt32();
        // Capacity on the TRACE COUNT would allocate the whole segment's worth of slots for a
        // window that may want none of them, which is half of what the bound is here to stop.
        var  result     = new List<TraceSummary>();
        Span<byte> tidBuf = stackalloc byte[16];

        for (uint i = 0; i < traceCount; i++)
        {
            br.Read(tidBuf);
            var    tid       = TraceId.Parse(tidBuf);
            var    rootSid   = new SpanId(br.ReadUInt64());
            long   startNano = br.ReadInt64();

            // The row is variable-length, so an out-of-range row still has to be WALKED past —
            // but nothing of it needs to be decoded or allocated.
            if (startNano < fromNano || startNano > toNano)
            {
                ms.Position += 8 + 4 + 1 + 1 + 2 + 4;   // dur, spanCount, flags, status, http, svc
                SkipStr16(br, ms);                      // name
                SkipStr8 (br, ms);                      // method
                SkipStr16(br, ms);                      // path
                // The read is its OWN statement, and it has to be. `ms.Position += Read...()`
                // evaluates the Position GETTER before the right-hand side, so the two bytes the
                // read consumes are then written back out of the total — every following row
                // parses two bytes early, and the body desynchronises into an exception the
                // caller reports as an unreadable segment.
                long svcIndexBytes = br.ReadUInt16() * 4L;
                ms.Position += svcIndexBytes;
                continue;
            }

            long   durNanos  = br.ReadInt64();
            uint   spanCount = br.ReadUInt32();
            byte   flags     = br.ReadByte();
            var    status    = (SpanStatusCode)br.ReadByte();
            short  httpSC    = br.ReadInt16();
            int    rootSvc   = br.ReadInt32();
            string name      = ReadStr16(br);
            string method    = ReadStr8(br);
            string httpPath  = ReadStr16(br);

            ushort svcCount  = br.ReadUInt16();
            var    services  = svcCount == 0 ? [] : new string[svcCount];
            for (int j = 0; j < svcCount; j++) services[j] = PoolAt(br.ReadInt32());

            result.Add(new TraceSummary
            {
                TraceId        = tid,
                RootSpanId     = rootSid,
                RootStartNano  = startNano,
                DurationNanos  = durNanos,
                SpanCount      = spanCount,
                HasRoot        = (flags & 0b01) != 0,
                HasError       = (flags & 0b10) != 0,
                RootStatus     = status,
                HttpStatusCode = httpSC,
                Name           = name,
                ServiceName    = PoolAt(rootSvc),
                HttpMethod     = method,
                HttpPath       = httpPath,
                Services       = services,
            });
        }

        return result;
    }

    // Length first, seek second — see the note at the call site: as one expression the Position
    // getter is evaluated before the read, and the length prefix is then un-consumed.
    private static void SkipStr8 (BinaryReader r, MemoryStream ms) { int n = r.ReadByte();   ms.Position += n; }
    private static void SkipStr16(BinaryReader r, MemoryStream ms) { int n = r.ReadUInt16(); ms.Position += n; }

    // ── Helpers ─────────────────────────────────────────────────────────────────

    private static void WriteStr8(BinaryWriter w, string s)
    {
        var b = Encoding.UTF8.GetBytes(s);
        if (b.Length > 255) b = b[..255];
        w.Write((byte)b.Length);
        w.Write(b);
    }

    private static void WriteStr16(BinaryWriter w, string s)
    {
        var b = Encoding.UTF8.GetBytes(s);
        if (b.Length > 65535) b = b[..65535];
        w.Write((ushort)b.Length);
        w.Write(b);
    }

    private static string ReadStr8(BinaryReader r)  => Encoding.UTF8.GetString(r.ReadBytes(r.ReadByte()));
    private static string ReadStr16(BinaryReader r) => Encoding.UTF8.GetString(r.ReadBytes(r.ReadUInt16()));

    private static string GetAttr(IReadOnlyDictionary<string, object?>? attrs, string[] keys)
    {
        if (attrs is null) return string.Empty;
        foreach (var k in keys)
            if (attrs.TryGetValue(k, out var v) && v is not null)
                return v.ToString() ?? string.Empty;
        return string.Empty;
    }

    private struct VolCell { public uint Traces; public uint Errors; }

    private sealed class Acc
    {
        public TraceId        TraceId;
        public uint           SpanCount;
        public bool           HasError;
        public long           EarliestNano = long.MaxValue;
        public string         FirstService = string.Empty;

        public bool           HasRoot;
        public SpanId         RootSpanId;
        public long           RootStartNano;
        public long           RootDurNanos;
        public SpanStatusCode RootStatus;
        public short          RootHttpStatus;
        public string         RootName    = string.Empty;
        public string         RootService = string.Empty;
        public string         RootMethod  = string.Empty;
        public string         RootPath    = string.Empty;

        public HashSet<string>? Services;
    }
}
