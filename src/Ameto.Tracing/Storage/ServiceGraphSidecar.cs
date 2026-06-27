using System.Text;

namespace Ameto.Tracing.Storage;

/// <summary>Per-edge stats for a cold-tier segment's service graph.</summary>
public sealed class ServiceEdgeRecord
{
    public string From        { get; init; } = string.Empty;
    public string To          { get; init; } = string.Empty;
    public uint   CallCount   { get; init; }
    public uint   ErrorCount  { get; init; }
    public uint[] Buckets     { get; init; } = new uint[HistogramBuckets.Count];
}

/// <summary>
/// Builds service-call edges from a span batch and writes a <c>.svcgraph</c> sidecar.
///
/// <para>Binary format "RDTG":</para>
/// <code>
///   Magic uint32 | Version uint16 | EdgeCount uint32
///   per edge: fromLen uint16 | from UTF-8 | toLen uint16 | to UTF-8
///             callCount uint32 | errorCount uint32 | buckets uint32[19]
/// </code>
/// </summary>
internal static class ServiceGraphSidecar
{
    private const uint   GraphMagic = 0x52_44_54_47; // "RDTG"
    private const ushort Version    = 1;

    // ── Writer ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Derives cross-service edges from <paramref name="spans"/>, writes them
    /// to <c>{basePath}.svcgraph</c>. Returns nothing — caller uses ReadEdges.
    /// </summary>
    public static void Write(string baseTrcPath, IList<SpanRecord> spans)
    {
        if (spans.Count == 0) return;

        // spanId → serviceName lookup for the entire batch
        var spanSvc = new Dictionary<SpanId, string>(spans.Count);
        for (int i = 0; i < spans.Count; i++)
            spanSvc[spans[i].SpanId] = spans[i].ServiceName;

        // Accumulate edges
        var edges = new Dictionary<(string From, string To), MutableEdge>(16);

        for (int i = 0; i < spans.Count; i++)
        {
            var s = spans[i];
            if (s.ParentSpanId.IsEmpty) continue;
            if (!spanSvc.TryGetValue(s.ParentSpanId, out var parentSvc)) continue;
            if (string.Equals(parentSvc, s.ServiceName, StringComparison.Ordinal)) continue;

            var key = (parentSvc, s.ServiceName);
            if (!edges.TryGetValue(key, out var edge))
            {
                edge = new MutableEdge();
                edges[key] = edge;
            }
            edge.CallCount++;
            if (s.Status == SpanStatusCode.Error) edge.ErrorCount++;
            edge.Buckets[HistogramBuckets.IndexOf(s.DurationNanos)]++;
        }

        if (edges.Count == 0) return;

        string path = Path.ChangeExtension(baseTrcPath, ".svcgraph");
        using var fs = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096);
        using var bw = new BinaryWriter(fs);

        bw.Write(GraphMagic);
        bw.Write(Version);
        bw.Write((uint)edges.Count);

        foreach (var ((from, to), e) in edges)
        {
            var fb = Encoding.UTF8.GetBytes(from);
            var tb = Encoding.UTF8.GetBytes(to);
            bw.Write((ushort)fb.Length); bw.Write(fb);
            bw.Write((ushort)tb.Length); bw.Write(tb);
            bw.Write(e.CallCount);
            bw.Write(e.ErrorCount);
            foreach (var b in e.Buckets) bw.Write(b);
        }
    }

    // ── Reader ────────────────────────────────────────────────────────────────

    /// <summary>Reads edges from the companion <c>.svcgraph</c> sidecar. Empty on missing/corrupt file.</summary>
    public static List<ServiceEdgeRecord> ReadEdges(string trcFilePath)
    {
        var path = Path.ChangeExtension(trcFilePath, ".svcgraph");
        if (!File.Exists(path)) return [];

        try
        {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096);
            using var br = new BinaryReader(fs);

            if (br.ReadUInt32() != GraphMagic) return [];
            br.ReadUInt16(); // version
            uint count = br.ReadUInt32();

            var result = new List<ServiceEdgeRecord>((int)count);
            for (uint i = 0; i < count; i++)
            {
                string from     = Encoding.UTF8.GetString(br.ReadBytes(br.ReadUInt16()));
                string to       = Encoding.UTF8.GetString(br.ReadBytes(br.ReadUInt16()));
                uint   calls    = br.ReadUInt32();
                uint   errors   = br.ReadUInt32();
                var    buckets  = new uint[HistogramBuckets.Count];
                for (int b = 0; b < HistogramBuckets.Count; b++)
                    buckets[b] = br.ReadUInt32();

                result.Add(new ServiceEdgeRecord
                {
                    From = from, To = to,
                    CallCount = calls, ErrorCount = errors,
                    Buckets = buckets,
                });
            }
            return result;
        }
        catch { return []; }
    }

    private sealed class MutableEdge
    {
        public uint   CallCount  = 0;
        public uint   ErrorCount = 0;
        public uint[] Buckets    = new uint[HistogramBuckets.Count];
    }
}
