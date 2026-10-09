using Ameto.Core;
using System.Text;

namespace Ameto.Tracing.Storage;

internal static partial class ServiceGraphSidecar
{
    /// <summary>
    /// The edges of a segment's <c>.svcgraph</c>, or FALSE when the sidecar exists and will not read.
    /// A sidecar that is not there is an answer — the segment had no cross-service call — and comes
    /// back as true with no edges: the writer writes no file for an empty edge set.
    ///
    /// <para><see cref="ReadEdges"/> flattens "damaged" into "no edges", which is right for a query —
    /// a broken sidecar costs that segment's edges, not the request — and wrong for a merge, which
    /// deletes the source afterwards: its edges would be gone for good. A merge asks this instead and
    /// leaves a source whose sidecar will not read to the merge that rebuilds edges from spans.</para>
    /// </summary>
    public static bool TryReadEdges(string trcFilePath, out List<ServiceEdgeRecord> edges)
    {
        edges = [];
        var path = Path.ChangeExtension(trcFilePath, ".svcgraph");
        if (!File.Exists(path)) return true;

        try
        {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096);
            using var br = new BinaryReader(fs);

            if (br.ReadUInt32() != GraphMagic) return false;
            br.ReadUInt16(); // version
            uint count = br.ReadUInt32();
            int perEdge = 2 + 2 + 4 + 4 + 4 * HistogramBuckets.Count;
            if (!FileBounds.CountFits(count, fs.Length - fs.Position, perEdge)) return false;

            edges = new List<ServiceEdgeRecord>((int)count);
            for (uint i = 0; i < count; i++)
            {
                string from    = Encoding.UTF8.GetString(br.ReadBytes(br.ReadUInt16()));
                string to      = Encoding.UTF8.GetString(br.ReadBytes(br.ReadUInt16()));
                uint   calls   = br.ReadUInt32();
                uint   errors  = br.ReadUInt32();
                var    buckets = new uint[HistogramBuckets.Count];
                for (int b = 0; b < HistogramBuckets.Count; b++) buckets[b] = br.ReadUInt32();
                edges.Add(new ServiceEdgeRecord { From = from, To = to, CallCount = calls, ErrorCount = errors, Buckets = buckets });
            }
            return fs.Position == fs.Length;
        }
        catch (Exception ex) when (FileBounds.DescribesContent(ex))
        {
            edges = [];
            return false;
        }
    }

    /// <summary>
    /// THE <c>.svcgraph</c> OF A STREAMING MERGE: every edge its sources recorded, plus the edges that
    /// only exist now that their spans share a file.
    ///
    /// <para><b>WHAT A SOURCE ALREADY SAID IS SUMMED, NOT RE-DERIVED.</b> A flush resolves every parent
    /// across its whole batch, however far apart the two spans started. Re-deriving that over a merged
    /// file needs a span-id map of the whole output — 28 B a span on the heap, a million-span output's
    /// worth, which is what the streaming merge exists not to hold. Each source's own edges are exact
    /// already, so they are added as they are.</para>
    ///
    /// <para><b>WHAT NO SOURCE COULD SAY IS FOUND IN A WINDOW.</b> A child whose parent landed in a
    /// different flush — a trace that straddled the flush boundary — has no edge in either source. The
    /// merge sees both, and counts the pair when the two spans are from DIFFERENT sources and start
    /// within <see cref="WindowNanos"/> of each other (in either order, so a parent from a clock behind
    /// its child still resolves). Pairs from the same source are never counted here: that source's own
    /// sidecar already holds them. A span id seen in two sources (a replayed duplicate) is ambiguous and
    /// draws no edge from this pass, so nothing is counted twice.</para>
    /// </summary>
    internal sealed class MergeEdges
    {
        /// <summary>How far apart in start time two spans of different sources may be and still be paired.</summary>
        public long WindowNanos { get; }

        /// <summary>The most span ids held for pairing; past it the oldest go first.</summary>
        public int MaxSpans { get; }

        private const int Ambiguous = -1;

        private readonly Dictionary<SpanId, (string Service, int Source, long Start)> _spans;
        private readonly Queue<(SpanId Id, long Start)> _spanOrder = new();

        // Children whose parent has not been seen yet, by the parent's id.
        private readonly Dictionary<SpanId, List<Pending>> _pending = new();
        private readonly Queue<(SpanId Parent, long Start)> _pendingOrder = new();
        private int _pendingCount;

        private readonly Dictionary<(string From, string To), MutableEdge> _edges = new(16);

        public MergeEdges(long windowNanos, int maxSpans)
        {
            WindowNanos = windowNanos;
            MaxSpans    = Math.Max(1, maxSpans);
            _spans      = new Dictionary<SpanId, (string, int, long)>(Math.Min(MaxSpans, 16_384));
        }

        /// <summary>Calls counted that no source recorded.</summary>
        public int CrossSourceCalls { get; private set; }

        /// <summary>Adds what a source's own sidecar recorded.</summary>
        public void AddRecorded(List<ServiceEdgeRecord> recorded)
        {
            foreach (var e in recorded)
            {
                var acc = EdgeFor(e.From, e.To);
                acc.CallCount  += e.CallCount;
                acc.ErrorCount += e.ErrorCount;
                var b = e.Buckets;
                for (int i = 0; i < HistogramBuckets.Count && i < b.Length; i++) acc.Buckets[i] += b[i];
            }
        }

        /// <summary>Takes one span of source <paramref name="source"/>, in the merge's start order.</summary>
        public void Add(SpanRecord s, int source)
        {
            long ts = s.StartTimeUnixNano;
            Expire(ts);

            // Children that arrived before this span — a parent from a clock behind them.
            if (!s.SpanId.IsEmpty && _pending.Remove(s.SpanId, out var waiting))
            {
                _pendingCount -= waiting.Count;
                foreach (var p in waiting)
                    if (p.Source != source) Count(s.ServiceName, p.Service, p.Status, p.DurationNanos);
            }

            if (!s.SpanId.IsEmpty)
            {
                // A duplicate across sources (a replayed span) is ambiguous for good: which copy a
                // child "belongs with" is not a question this pass can answer, so it draws no edge.
                int owner = _spans.TryGetValue(s.SpanId, out var seen) && seen.Source != source ? Ambiguous : source;
                _spans[s.SpanId] = (s.ServiceName, owner, ts);
                _spanOrder.Enqueue((s.SpanId, ts));
            }

            if (s.ParentSpanId.IsEmpty) return;
            if (_spans.TryGetValue(s.ParentSpanId, out var parent))
            {
                if (parent.Source != source && parent.Source != Ambiguous)
                    Count(parent.Service, s.ServiceName, s.Status, s.DurationNanos);
                return;
            }

            if (!_pending.TryGetValue(s.ParentSpanId, out var list))
                _pending[s.ParentSpanId] = list = new List<Pending>(1);
            list.Add(new Pending(s.ServiceName, source, s.Status, s.DurationNanos));
            _pendingCount++;
            _pendingOrder.Enqueue((s.ParentSpanId, ts));
        }

        private void Count(string from, string to, SpanStatusCode status, long durationNanos)
        {
            if (string.Equals(from, to, StringComparison.Ordinal)) return;
            var e = EdgeFor(from, to);
            e.CallCount++;
            if (status == SpanStatusCode.Error) e.ErrorCount++;
            e.Buckets[HistogramBuckets.IndexOf(durationNanos)]++;
            CrossSourceCalls++;
        }

        private void Expire(long now)
        {
            while (_spanOrder.TryPeek(out var head)
                && (_spans.Count > MaxSpans || (now > head.Start && now - head.Start > WindowNanos)))
            {
                _spanOrder.Dequeue();
                // Only the entry this queue item put there: a later copy of the id re-enqueued itself.
                if (_spans.TryGetValue(head.Id, out var v) && v.Start == head.Start) _spans.Remove(head.Id);
            }
            while (_pendingOrder.TryPeek(out var head)
                && (_pendingCount > MaxSpans || (now > head.Start && now - head.Start > WindowNanos)))
            {
                _pendingOrder.Dequeue();
                if (_pending.Remove(head.Parent, out var dropped)) _pendingCount -= dropped.Count;
            }
        }

        private MutableEdge EdgeFor(string from, string to)
        {
            if (!_edges.TryGetValue((from, to), out var e)) _edges[(from, to)] = e = new MutableEdge();
            return e;
        }

        /// <summary>Writes the summed edges to <paramref name="path"/>, fsynced — or nothing for an empty set, as the flush does.</summary>
        public void Write(string path)
        {
            if (_edges.Count == 0) return;
            using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 4096);
            using var bw = new BinaryWriter(fs);

            bw.Write(GraphMagic);
            bw.Write(Version);
            bw.Write((uint)_edges.Count);
            foreach (var ((from, to), e) in _edges)
            {
                var fb = Encoding.UTF8.GetBytes(from);
                var tb = Encoding.UTF8.GetBytes(to);
                bw.Write((ushort)fb.Length); bw.Write(fb);
                bw.Write((ushort)tb.Length); bw.Write(tb);
                bw.Write(e.CallCount);
                bw.Write(e.ErrorCount);
                foreach (var b in e.Buckets) bw.Write(b);
            }

            bw.Flush();
            fs.Flush(flushToDisk: true);
        }

        private readonly record struct Pending(string Service, int Source, SpanStatusCode Status, long DurationNanos);
    }
}
