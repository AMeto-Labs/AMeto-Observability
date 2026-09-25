using System.Buffers;

namespace Ameto.Tracing.Storage;

/// <summary>
/// The segment writer's big scratch arrays, kept BY THE ENGINE between flushes (#90): the sort
/// permutation and its keys, and the <c>.tracesum</c> body.
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
/// ordinary flush used — 1.8 MB for 50 000 spans of 10-span traces, at most 4.8 MB.</para>
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

    private int[]?  _order;
    private long[]? _keys;
    private byte[]? _body;

    public int[]  RentOrder(int n) => Take(ref _order, n);
    public long[] RentKeys(int n)  => Take(ref _keys, n);
    public byte[] RentBody(int n)  => Take(ref _body, n);

    public void Return(int[] a)      => Give(ref _order, a, MaxKeptElements);
    public void Return(long[] a)     => Give(ref _keys,  a, MaxKeptElements);
    public void ReturnBody(byte[] a) => Give(ref _body,  a, MaxKeptBodyBytes);

    /// <summary>Test hook: the arrays held right now (null where a slot is empty or taken).</summary>
    internal (int[]? Order, long[]? Keys, byte[]? Body) HeldForTest =>
        (Volatile.Read(ref _order), Volatile.Read(ref _keys), Volatile.Read(ref _body));

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
