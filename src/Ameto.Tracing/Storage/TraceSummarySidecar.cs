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

    /// <summary>Volume grid resolution — 10 s. Sparse, so idle gaps cost nothing.</summary>
    public const long GridNanos = 10_000_000_000L;

    private static readonly string[] MethodKeys = { "http.request.method", "http.method" };
    private static readonly string[] PathKeys   = { "url.path", "http.target", "http.route", "url.full", "http.url" };

    // ── Writer ──────────────────────────────────────────────────────────────────

    public static void Write(string baseTrcPath, IList<SpanRecord> spans)
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

        string path = Path.ChangeExtension(baseTrcPath, ".tracesum");
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

            uint volCount = br.ReadUInt32();
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
        catch { return null; }
    }

    // ── Reader: full per-trace rows ─────────────────────────────────────────────

    public static List<TraceSummary> ReadSummaries(string trcFilePath)
    {
        var path = Path.ChangeExtension(trcFilePath, ".tracesum");
        if (!File.Exists(path)) return [];

        try
        {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 65536, FileOptions.SequentialScan);
            using var br = new BinaryReader(fs);

            if (br.ReadUInt32() != Magic) return [];
            br.ReadUInt16(); // version
            br.ReadInt64();  // min
            br.ReadInt64();  // max

            uint volCount = br.ReadUInt32();
            fs.Seek(volCount * 16L, SeekOrigin.Current); // skip volume header

            uint   uncompSize = br.ReadUInt32();
            uint   compSize   = br.ReadUInt32();
            byte[] comp       = br.ReadBytes((int)compSize);
            byte[] raw        = LZ4Pickler.Unpickle(comp);
            if (raw.Length != uncompSize) { /* tolerate — trust actual length */ }

            return ParseBody(raw);
        }
        catch { return []; }
    }

    private static List<TraceSummary> ParseBody(byte[] raw)
    {
        var ms = new MemoryStream(raw, writable: false);
        using var br = new BinaryReader(ms);

        uint poolCount = br.ReadUInt32();
        var  pool      = new string[poolCount];
        for (uint i = 0; i < poolCount; i++)
        {
            ushort len = br.ReadUInt16();
            pool[i] = Encoding.UTF8.GetString(br.ReadBytes(len));
        }

        string PoolAt(int idx) => idx >= 0 && idx < pool.Length ? pool[idx] : string.Empty;

        uint traceCount = br.ReadUInt32();
        var  result     = new List<TraceSummary>((int)traceCount);
        Span<byte> tidBuf = stackalloc byte[16];

        for (uint i = 0; i < traceCount; i++)
        {
            br.Read(tidBuf);
            var    tid       = TraceId.Parse(tidBuf);
            var    rootSid   = new SpanId(br.ReadUInt64());
            long   startNano = br.ReadInt64();
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
