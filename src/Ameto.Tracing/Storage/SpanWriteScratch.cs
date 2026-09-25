using System.Buffers;
using System.Runtime.InteropServices;

namespace Ameto.Tracing.Storage;

/// <summary>
/// The segment writer's big scratch arrays, kept BY THE ENGINE between flushes (#90): the sort
/// permutation and its keys, the <c>.tracesum</c> body, and the trace-index refs (TS#7(c)).
///
/// <para>WHY NOT JUST <c>ArrayPool.Shared</c>. A flush runs on whatever thread-pool thread its task
/// lands on, and the shared pool keeps a returned array in the RETURNING thread's slot first — so a
/// flush on another thread, or on the same one after a gen-2 trim, found the slot empty and
/// allocated those arrays afresh on the large-object heap: +2.5 MB per 50 000-span flush measured
/// (18.35 MB on the thread that last returned them, 20.85 MB on any other thread, and on most
/// thread-pool flushes). One slot per array here, owned by the engine, is found by whichever
/// thread flushes next.</para>
///
/// <para>BOUNDED, because an engine slot is never trimmed. An array is kept only up to a flush's own
/// size (<see cref="MaxKeptElements"/> spans, <see cref="MaxKeptBodyBytes"/> of body); a compaction
/// pass's larger arrays go back to the shared pool as before. What this holds at rest is what one
/// ordinary flush used — 3.1 MB for 50 000 spans of 10-span traces, at most 6.1 MB.</para>
///
/// <para>Exclusive by exchange: a flush and a compaction pass running together each take a slot
/// with <see cref="Interlocked.Exchange{T}(ref T, T)"/>, so the second finds it empty and rents from
/// the shared pool — never the same array twice. Every array held here came from
/// <c>ArrayPool.Shared.Rent</c>, so any of them may go back there.</para>
/// </summary>
internal sealed class SpanWriteScratch
{
    /// <summary>
    /// The largest per-span array kept: the rent size of a full flush (<c>HotFlushThreshold</c> =
    /// 50 000 spans rounds up to 65 536).
    /// </summary>
    internal const int MaxKeptElements = 1 << 16;

    /// <summary>
    /// The largest <c>.tracesum</c> body kept. Ordinary traces fit (50 000 spans of 10-span traces
    /// rent 1 MB); a full flush of single-span traces rents 8 MB and is not kept.
    /// </summary>
    internal const int MaxKeptBodyBytes = 4 << 20;

    private int[]?          _order;
    private long[]?         _keys;
    private byte[]?         _body;
    private TraceSpanRef[]? _pairs;

    public int[]          RentOrder(int n) => Take(ref _order, n);
    public long[]         RentKeys(int n)  => Take(ref _keys, n);
    public byte[]         RentBody(int n)  => Take(ref _body, n);
    public TraceSpanRef[] RentPairs(int n) => Take(ref _pairs, n);

    public void Return(int[] a)          => Give(ref _order, a, MaxKeptElements);
    public void Return(long[] a)         => Give(ref _keys,  a, MaxKeptElements);
    public void ReturnBody(byte[] a)     => Give(ref _body,  a, MaxKeptBodyBytes);
    public void Return(TraceSpanRef[] a) => Give(ref _pairs, a, MaxKeptElements);

    /// <summary>Test hook: the arrays held right now (null where a slot is empty or taken).</summary>
    internal (int[]? Order, long[]? Keys, byte[]? Body, TraceSpanRef[]? Pairs) HeldForTest =>
        (Volatile.Read(ref _order), Volatile.Read(ref _keys), Volatile.Read(ref _body), Volatile.Read(ref _pairs));

    private static T[] Take<T>(ref T[]? slot, int n)
    {
        var held = Interlocked.Exchange(ref slot, null);
        if (held is not null)
        {
            if (held.Length >= n) return held;
            ArrayPool<T>.Shared.Return(held);     // too small for this one: the shared pool may place it
        }
        return ArrayPool<T>.Shared.Rent(n);
    }

    private static void Give<T>(ref T[]? slot, T[] a, int cap)
    {
        if (a.Length > cap) { ArrayPool<T>.Shared.Return(a); return; }
        var displaced = Interlocked.Exchange(ref slot, a);   // the one just used is the likeliest fit next
        if (displaced is not null) ArrayPool<T>.Shared.Return(displaced);
    }
}

/// <summary>
/// One span's place in a segment, for the trace-id index: its trace, and its position in the
/// segment's span order. 20 bytes, packed — a flush makes one per span.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 4)]
internal readonly struct TraceSpanRef(TraceId traceId, uint offset)
{
    public readonly TraceId TraceId = traceId;
    public readonly uint    Offset  = offset;
}

/// <summary>
/// A segment's trace-id index as the writer built it (TS#7(c)): one <see cref="TraceSpanRef"/> per
/// span, SORTED by trace id and then by offset, so each trace is one contiguous run with its offsets
/// ascending. It replaces a <c>Dictionary&lt;TraceId, List&lt;uint&gt;&gt;</c> that cost a
/// <c>List&lt;uint&gt;</c> — and its growth — per trace.
///
/// <para>OWNS A RENTED ARRAY, handed from <c>SpanWriter.Write</c> to its <c>onTraceIndex</c> callee,
/// which calls <see cref="Release"/> when it is done with it. A class, not a struct, so that is
/// one owner whatever holds a reference: <see cref="Release"/> takes the array out first, and a
/// second call — from any reference — finds nothing to give back rather than pooling the same
/// array twice (two renters would then share it). One small object per flush. Never released
/// costs nothing but the pool's chance to reuse the array.</para>
/// </summary>
internal sealed class TraceIndexPairs
{
    private TraceSpanRef[]?            _refs;
    private readonly SpanWriteScratch? _owner;

    /// <summary>How many spans: the length of <see cref="Refs"/> until <see cref="Release"/>.</summary>
    public int Count { get; }

    /// <summary>How many distinct traces: the runs in <see cref="Refs"/>.</summary>
    public int Traces { get; }

    internal TraceIndexPairs(TraceSpanRef[] refs, int count, int traces, SpanWriteScratch? owner)
    {
        _refs  = refs;
        _owner = owner;
        Count  = count;
        Traces = traces;
    }

    /// <summary>The sorted refs: trace by trace, offsets ascending within each. Empty once released.</summary>
    public ReadOnlySpan<TraceSpanRef> Refs =>
        Volatile.Read(ref _refs) is { } refs ? new(refs, 0, Count) : default;

    /// <summary>
    /// Gives the array back — to the engine's scratch when it came from there, else to the shared
    /// pool. Idempotent: only the first call has an array to give.
    /// </summary>
    public void Release()
    {
        var refs = Interlocked.Exchange(ref _refs, null);
        if (refs is null) return;
        if (_owner is not null) _owner.Return(refs);
        else                    ArrayPool<TraceSpanRef>.Shared.Return(refs);
    }
}

/// <summary>
/// Trace id (the id's own order, <see cref="TraceId.CompareTo"/>), then offset. Total, since an
/// offset is one span's own: any sort gives one result.
/// </summary>
internal readonly struct ByTraceThenOffset : IComparer<TraceSpanRef>
{
    public int Compare(TraceSpanRef a, TraceSpanRef b)
    {
        int c = a.TraceId.CompareTo(b.TraceId);
        return c != 0 ? c : a.Offset.CompareTo(b.Offset);
    }
}
