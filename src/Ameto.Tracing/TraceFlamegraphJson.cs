using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;
using Microsoft.AspNetCore.Http;

namespace Ameto.Tracing;

/// <summary>
/// THE FLAME GRAPH'S WIRE FORMAT, WRITTEN BY HAND AND WITHOUT RECURSION — byte for byte what
/// serialising a <see cref="FlamegraphNode"/> tree through the host's JSON options produced, for
/// every tree that serialiser would write at all (issue #91).
///
/// <para>WHY. That serialiser refused a tree deeper than 32 levels: a node is an object plus its
/// <c>children</c> array, two of System.Text.Json's default 64 levels, so the 33rd level threw and
/// the flame graph of an ordinary deep trace — recursion, middleware chains, retries inside
/// retries — was a 500. The tree is now walked with an EXPLICIT stack (one int per open level,
/// from the pool) straight from the index the builder keeps, and written into the response with
/// a <see cref="Utf8JsonWriter"/> over <see cref="TraceDetailJson.WriterOptions(HttpContext)"/> —
/// the host's encoder and layout. There is no node object, no id string and no children array per
/// span any more, and no recursion on the builder's side either: a chain of any length costs one
/// stack slot per level, not a CLR frame.</para>
///
/// <para>THE SAME TREE, rule for rule — <c>TraceDetailShapeTests</c> pins the bytes (including a
/// 32-level golden captured on the old serialiser) and <c>TraceFlamegraphParityTests</c> holds the
/// writer to the old builder over seeded random traces: a span whose parent is empty or names no
/// span of the trace is a root candidate, and the LAST candidate in the provider's order is the
/// root; the children of a node are every span naming its id as parent, in the provider's order,
/// grouped BY ID (two spans sharing an id — only the empty id can, after the provider's dedupe —
/// share one child list); <c>selfMs</c> is the node's total less the sum, left to right, of its
/// children's ROUNDED totals, floored at zero; no root at all is the literal <c>null</c>. Ids are
/// <see cref="TraceDetailJson.FormatHex(ulong, Span{byte})"/>, and kinds and statuses the same
/// <see cref="TraceDetailJson.KindNames"/> / <see cref="TraceDetailJson.StatusNames"/> the detail
/// page writes, so the two views cannot spell one span two ways.</para>
///
/// <para>PAST <see cref="MaxLevels"/> THE TREE IS CUT, NEVER REFUSED. A node on the last level that
/// still has children is written whole — its own <c>totalMs</c> and <c>selfMs</c> count them — with
/// <c>"children":[]</c> followed by <c>"truncated":true</c>, and its subtree is not written.</para>
///
/// <para>THE WALK IS BOUNDED IN SIZE AS WELL AS DEPTH: at most <c>min(n, MaxLevels)</c> levels and at
/// most <c>n</c> nodes for a trace of <c>n</c> spans. Of these only <see cref="MaxLevels"/> can cut a
/// tree the provider hands over: it dedupes non-empty span ids, so every span is reached at most
/// once, on a path of distinct spans — never more than <c>n</c> nodes, never deeper than <c>n</c>
/// levels. Only a REPEATED non-empty id could make the walk revisit a span: a cycle (the old
/// recursive builder overflowed the stack on one) or, with two spans naming the repeated id as
/// parent, a tree that doubles at every level. Past either bound the node whose children are not
/// all written is closed with <c>"truncated":true</c> — after an empty <c>children</c> array at the
/// depth bound, after the children already written at the node bound. The flag appears on no node
/// of a tree that was not cut, so every body the old serialiser wrote is unchanged.</para>
///
/// <para>FLUSHED AS IT GOES, every <see cref="TraceDetailJson.FlushThresholdBytes"/>, as the
/// detail page and the serialiser before it do; nothing is written until the spans are collected
/// and the tree indexed, so a lookup that fails is still a 500 before the response has started.</para>
/// </summary>
internal static class TraceFlamegraphJson
{
    /// <summary>
    /// The deepest level written; a node here with children is cut (see the class remarks). A
    /// sanity bound, not a working limit: 128 times the depth the serialiser used to refuse at,
    /// far past any call graph a person reads, and small enough that its stack is 16 KB.
    /// </summary>
    internal const int MaxLevels = 4096;

    private static readonly JsonEncodedText PSpanId    = JsonEncodedText.Encode("spanId");
    private static readonly JsonEncodedText PName      = JsonEncodedText.Encode("name");
    private static readonly JsonEncodedText PService   = JsonEncodedText.Encode("service");
    private static readonly JsonEncodedText PKind      = JsonEncodedText.Encode("kind");
    private static readonly JsonEncodedText PStatus    = JsonEncodedText.Encode("status");
    private static readonly JsonEncodedText PTotalMs   = JsonEncodedText.Encode("totalMs");
    private static readonly JsonEncodedText PSelfMs    = JsonEncodedText.Encode("selfMs");
    private static readonly JsonEncodedText PChildren  = JsonEncodedText.Encode("children");
    private static readonly JsonEncodedText PTruncated = JsonEncodedText.Encode("truncated");

    /// <summary>
    /// The host's writer settings (<see cref="TraceDetailJson.WriterOptions(HttpContext)"/>) with
    /// room for <see cref="MaxLevels"/> levels: two JSON levels each. <c>MaxDepth</c> changes no
    /// byte — only where the writer would refuse.
    /// </summary>
    internal static JsonWriterOptions WriterOptions(HttpContext ctx) =>
        WithRoom(TraceDetailJson.WriterOptions(ctx));

    /// <summary><paramref name="host"/> with the depth the walk needs.</summary>
    internal static JsonWriterOptions WithRoom(JsonWriterOptions host)
    {
        host.MaxDepth = 2 * MaxLevels;
        return host;
    }

    /// <summary>The whole flame graph of <paramref name="spans"/> (provider order) into the response.</summary>
    internal static async Task WriteAsync(HttpContext ctx, List<SpanRecord> spans)
    {
        ctx.Response.ContentType = TraceDetailJson.ContentType;
        var body = ctx.Response.BodyWriter;
        var walk = new Walk(spans);
        try
        {
            using var json = new Utf8JsonWriter(body, WriterOptions(ctx));
            long flushed = 0;
            while (!walk.WriteSome(json, flushed + TraceDetailJson.FlushThresholdBytes))
            {
                json.Flush();
                flushed = json.BytesCommitted;
                // No token, as WriteAsJsonAsync passed none: a client that has gone completes the
                // pipe, and that is the signal to stop writing.
                if ((await body.FlushAsync()).IsCompleted) return;
            }
            json.Flush();
            await body.FlushAsync();
        }
        finally { walk.Dispose(); }
    }

    /// <summary>The whole flame graph of <paramref name="spans"/> into <paramref name="json"/>, at once.</summary>
    internal static void Write(Utf8JsonWriter json, List<SpanRecord> spans)
    {
        var walk = new Walk(spans);
        try   { walk.WriteSome(json, long.MaxValue); }
        finally { walk.Dispose(); }
    }

    /// <summary>
    /// The whole flame graph, but stopping every <paramref name="stepBytes"/> or so: the writer is
    /// flushed and the walk resumed where it stopped — what <see cref="WriteAsync"/> does at its
    /// flush threshold, at a step small enough to stop between any two nodes. For the parity tests,
    /// which hold the resumed walk to the one-shot bytes. Returns how many times it resumed.
    /// </summary>
    internal static int WriteInSteps(Utf8JsonWriter json, List<SpanRecord> spans, int stepBytes)
    {
        var walk = new Walk(spans);
        try
        {
            int resumes = 0;
            while (!walk.WriteSome(json, json.BytesCommitted + json.BytesPending + stepBytes))
            {
                json.Flush();
                resumes++;
            }
            return resumes;
        }
        finally { walk.Dispose(); }
    }

    /// <summary>
    /// The tree's index and the walk's position in it — pooled arrays and ints, so the walk can
    /// stop at a flush and carry on after it. A struct: it lives in the caller's frame (or the
    /// state machine of <see cref="WriteAsync"/>) and is never copied once built.
    ///
    /// <para>One rented int array: five regions of <c>n</c>, then the stack:</para>
    /// <list type="bullet">
    ///   <item><c>order</c>: sorted position → span index (building only);</item>
    ///   <item><c>groupOf</c>: span index → its id's group, the first sorted position holding it;</item>
    ///   <item><c>head</c>: group → first child's span index, or -1;</item>
    ///   <item><c>tail</c>: group → last child's span index (building only);</item>
    ///   <item><c>next</c>: span index → next sibling's span index, or -1;</item>
    ///   <item><c>stack</c>: open level − 1 → the next child of that level's node still to write, or
    ///   -1 once they are all written, <see cref="Cut"/> once the node budget stopped them.
    ///   <c>min(n, MaxLevels)</c> slots.</item>
    /// </list>
    /// </summary>
    private struct Walk : IDisposable
    {
        private readonly List<SpanRecord> _spans;
        private int[] _scratch;          // empty for an empty trace, and after Dispose
        private readonly int _n;
        private readonly int _root;

        /// <summary>
        /// The deepest level whose node may open its children: <c>min(n, MaxLevels)</c>. A walk
        /// that reached level <c>n</c> in a tree without repeated span ids is standing on a leaf,
        /// so a node there WITH children can only be a span opened twice.
        /// </summary>
        private readonly int _levels;

        private int  _open;      // levels currently open: the stack's height
        private int  _written;   // nodes opened so far — never more than n
        private bool _started;

        /// <summary>A stack slot's value once its node's remaining children were cut; -1 is "all written".</summary>
        private const int Cut = -2;

        public Walk(List<SpanRecord> spans)
        {
            _spans   = spans;
            _n       = spans.Count;
            _levels  = Math.Min(_n, MaxLevels);
            _root    = -1;
            _scratch = [];
            if (_n == 0) return;

            int n = _n;
            _scratch = ArrayPool<int>.Shared.Rent(5 * n + _levels);
            ulong[] ids = ArrayPool<ulong>.Shared.Rent(n);
            try
            {
                var order   = _scratch.AsSpan(0,     n);
                var groupOf = _scratch.AsSpan(n,     n);
                var head    = _scratch.AsSpan(2 * n, n);
                var tail    = _scratch.AsSpan(3 * n, n);
                var next    = _scratch.AsSpan(4 * n, n);

                for (int i = 0; i < n; i++) { ids[i] = spans[i].SpanId.RawValue; order[i] = i; }
                Array.Sort(ids, _scratch, 0, n);      // the keys, carrying their span indexes in `order`

                for (int p = 0, run = 0; p < n; p++)
                {
                    if (p > 0 && ids[p] != ids[p - 1]) run = p;
                    groupOf[order[p]] = run;
                }
                head.Fill(-1);
                next.Fill(-1);

                var sortedIds = new ReadOnlySpan<ulong>(ids, 0, n);
                for (int i = 0; i < n; i++)
                {
                    var parent = spans[i].ParentSpanId;
                    int g = parent.IsEmpty ? -1 : FindGroup(sortedIds, parent.RawValue);
                    if (g < 0) { _root = i; continue; }   // the last candidate wins, as it always did

                    if (head[g] < 0) head[g] = i;
                    else             next[tail[g]] = i;
                    tail[g] = i;
                }
            }
            finally { ArrayPool<ulong>.Shared.Return(ids); }
        }

        /// <summary>
        /// Writes on from where the last call stopped until the tree is finished — true — or the
        /// writer's total output has reached <paramref name="until"/> — false, and the caller
        /// flushes and calls again. Checked between nodes, so a call may overshoot by one node.
        /// </summary>
        public bool WriteSome(Utf8JsonWriter w, long until)
        {
            if (!_started)
            {
                _started = true;
                if (_root < 0) { w.WriteNullValue(); return true; }
                Open(w, _root, level: 1);
            }

            while (_open > 0)
            {
                if (w.BytesCommitted + w.BytesPending >= until) return false;

                ref int cursor = ref _scratch[5 * _n + _open - 1];   // stack[_open - 1]
                int child = cursor;
                if (child < 0)
                {
                    w.WriteEndArray();    // the node's children
                    if (child == Cut) w.WriteBoolean(PTruncated, true);
                    w.WriteEndObject();   // the node
                    _open--;
                    continue;
                }
                if (_written >= _n)
                {
                    // Every span has been written once and this node still has children: each of
                    // them could only be a span written a second time. See the class remarks.
                    cursor = Cut;
                    continue;
                }
                cursor = _scratch[4 * _n + child];                  // next[child]
                Open(w, child, level: _open + 1);
            }
            return true;
        }

        /// <summary>
        /// Writes node <paramref name="index"/> up to and including the <c>[</c> of its children,
        /// and pushes it — or, when it has none or sits on the last level, closes it at once.
        /// </summary>
        private void Open(Utf8JsonWriter w, int index, int level)
        {
            _written++;
            int n       = _n;
            var groupOf = _scratch.AsSpan(n,     n);
            var head    = _scratch.AsSpan(2 * n, n);
            var next    = _scratch.AsSpan(4 * n, n);
            var span    = _spans[index];
            int first   = head[groupOf[index]];

            // The children's ROUNDED totals, summed left to right — what the old builder summed.
            double totalMs = span.DurationNanos / 1_000_000.0;
            double childMs = 0;
            for (int c = first; c >= 0; c = next[c])
                childMs += Math.Round(_spans[c].DurationNanos / 1_000_000.0, 3);
            double selfMs  = Math.Max(0, totalMs - childMs);

            Span<byte> hex = stackalloc byte[16];
            TraceDetailJson.FormatHex(span.SpanId.RawValue, hex);

            w.WriteStartObject();
            w.WriteString(PSpanId,  hex);
            w.WriteString(PName,    span.Name);
            w.WriteString(PService, span.ServiceName);
            TraceDetailJson.WriteEnum(w, PKind,   (byte)span.Kind,   TraceDetailJson.KindNames,   span.Kind);
            TraceDetailJson.WriteEnum(w, PStatus, (byte)span.Status, TraceDetailJson.StatusNames, span.Status);
            w.WriteNumber(PTotalMs, Math.Round(totalMs, 3));
            w.WriteNumber(PSelfMs,  Math.Round(selfMs,  3));
            w.WritePropertyName(PChildren);
            w.WriteStartArray();

            if (first < 0)
            {
                w.WriteEndArray();
                w.WriteEndObject();
                return;
            }
            if (level >= _levels)
            {
                // The cut: this node is drawn, its subtree is not, and the response says so.
                w.WriteEndArray();
                w.WriteBoolean(PTruncated, true);
                w.WriteEndObject();
                return;
            }
            _scratch.AsSpan(5 * n, _levels)[_open++] = first;
        }

        public void Dispose()
        {
            if (_scratch.Length == 0) return;
            ArrayPool<int>.Shared.Return(_scratch);
            _scratch = [];
        }
    }

    /// <summary>The first sorted position holding <paramref name="id"/>, or -1 — the group's key.</summary>
    private static int FindGroup(ReadOnlySpan<ulong> sortedIds, ulong id)
    {
        int lo = 0, hi = sortedIds.Length;
        while (lo < hi)
        {
            int mid = (int)(((uint)lo + (uint)hi) >> 1);
            if (sortedIds[mid] < id) lo = mid + 1;
            else                     hi = mid;
        }
        return lo < sortedIds.Length && sortedIds[lo] == id ? lo : -1;
    }
}
