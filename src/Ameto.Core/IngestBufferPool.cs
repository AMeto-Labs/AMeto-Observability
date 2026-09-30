using System.Buffers;
using System.Diagnostics;

namespace Ameto.Core;

/// <summary>
/// The buffer pool the ingest receivers read their request bodies into — CLEF
/// <c>POST /api/events</c>, OTLP/HTTP, OTLP/gRPC, and the OTLP gzip inflate target (both receivers).
///
/// <para>These buffers are large and they are rented one per request.
/// <see cref="ArrayPool{T}.Shared"/> is the wrong shape for that: it keeps one array per
/// thread plus at most eight per core in each size bucket, and it is the same pool storage
/// and indexing rent from. A 1.4 MB batch rounds up to the 2 MB bucket, so past that shallow
/// depth each concurrent request gets a fresh, zeroed 2 MB array straight on the large object
/// heap — which is what an allocation trace of the stand under load showed, and the reason
/// the LOH grew while nothing leaked.</para>
///
/// <h3>What it can hold, and why that is bounded on purpose</h3>
/// <para>A pool that is never trimmed is a memory leak with good manners. The gRPC reader
/// does not get a Content-Length (nor does a chunked CLEF post), so it doubles 64 KB → 2 MB
/// and leaves an array in EVERY bucket on the way; at 32 deep that is about 126 MB pinned for
/// ever by 32 concurrent 2 MB batches, on a stand whose whole budget is 512 MB. So:</para>
/// <list type="bullet">
///   <item>A TOTAL BYTE CAP across every bucket, <see cref="MaxPooledTotalBytes"/>, taken from
///   the same memory model as the flush, tier and index-cache ceilings
///   (<see cref="MemoryBudgets.IngestBufferBytes"/>): a return that would exceed it is dropped
///   and becomes ordinary garbage. This is the bound that binds, and it is the one that was
///   missing — see the arithmetic below for what depth alone allowed.</item>
///   <item>Depth per bucket is still <c>2 x ProcessorCount</c>, clamped to
///   [<see cref="MinArraysPerBucket"/>, <see cref="MaxArraysPerBucket"/>], because request
///   concurrency really is bounded by cores. What it must not be asked to do is bound the
///   MEMORY: that argument assumed the containers which cannot afford it are the ones with few
///   cores, which holds only under a CPU quota — and this project's deployments set a memory
///   limit and no CPU limit, so <c>ProcessorCount</c> reports the host's cores. With the cap
///   above, a deep pool is free to use its depth in whichever bucket the traffic rents from.</item>
///   <item><see cref="Trim"/> runs after a gen2 collection and empties the pool outright when
///   the GC says memory load has passed its high threshold — but at most once every
///   <see cref="MinTrimInterval"/>, or the refill it causes drives the next collection and the
///   pool never gets past one. Dropping the whole pool is the trim: the arrays become garbage
///   in the collection that follows, and a pool that has to refill under pressure is the
///   outcome worth having.</item>
/// </list>
/// <para>The arithmetic this replaces, stated plainly rather than flatteringly: a full set of
/// buckets up to <see cref="MaxPooledBytes"/> is about <c>2 x MaxPooledBytes</c> = 16 MB, so the
/// ceiling between two trims WAS <c>depth x 16 MB</c> — 64 MB at the four-deep floor, 512 MB at
/// the 32-deep one, which is the whole of a 512 MB container and reachable there because the
/// core count is the host's. It is now <c>min(that, the budget)</c>: about 38 MB in a 512 MB
/// container, 128 MB where there is room for it. The trim is what makes even that a peak rather
/// than a resting level, and <see cref="PooledBytes"/> is what makes it visible — the figure
/// <c>/api/diagnostics</c> reports beside the index-build pool's.</para>
///
/// <para>Arrays are rented dirty and must be treated as uninitialised, and — as with any
/// pool — an array rented here must be returned here, in a <c>finally</c>, exactly once.
/// Handing one back to <see cref="ArrayPool{T}.Shared"/> is not a correctness bug but it
/// empties this pool one request at a time, which defeats the whole point of it.</para>
/// </summary>
public static class IngestBufferPool
{
    /// <summary>
    /// Largest array kept. Covers the default ceilings of both receivers —
    /// <c>Ingestion.MaxOtlpBatchBytes</c> (8 MiB) and <c>Ingestion.MaxBatchBytes</c> (4 MiB) —
    /// so a body that is accepted at all is a body this pool can serve.
    ///
    /// <para>An operator who raises either ceiling past this still gets correct behaviour, just
    /// not pooled: <see cref="Rent"/> above it allocates an array of exactly the requested
    /// length, and <see cref="Return"/> drops such an array rather than throwing, because its
    /// length maps past the last bucket.</para>
    /// </summary>
    public const int MaxPooledBytes = 8 * 1024 * 1024;

    /// <summary>
    /// ONE ARRAY IN EVERY BUCKET — the smallest pool that can serve a reader which has to double
    /// its way up to <see cref="MaxPooledBytes"/>, and therefore the floor under
    /// <see cref="MemoryBudgets.IngestBufferBytes"/>.
    ///
    /// <para>The buckets are the powers of two from <c>BoundedByteArrayPool.MinLength</c> (4 KiB)
    /// to <see cref="MaxPooledBytes"/> (8 MiB) inclusive, so a full set weighs
    /// <c>2 x MaxPooledBytes - MinLength</c> = 16 773 120 B, just under 16 MiB. That set is not a
    /// pathological shape — it is the ORDINARY one for the two readers with no Content-Length
    /// (gRPC, and a chunked CLEF post), which start at 64 KiB and double, leaving an array in
    /// every bucket on the way up.</para>
    ///
    /// <para>A budget under this cannot hold even one such ladder: <see cref="Return"/> drops
    /// what would exceed it, so the largest arrays — the 8 MiB OTLP bodies, the ones on the large
    /// object heap this pool exists for — are the ones dropped, and they become LOH garbage on
    /// every request. Derived here rather than written as a number in
    /// <c>MemoryBudgets</c>, so raising <see cref="MaxPooledBytes"/> raises the floor with it.</para>
    /// </summary>
    public const long FullBucketSetBytes = 2L * MaxPooledBytes - BoundedByteArrayPool.MinLength;

    /// <summary>Ceiling on the per-bucket depth, whatever the core count says.</summary>
    public const int MaxArraysPerBucket = 32;

    /// <summary>Floor on it, so a single-core container still pools rather than allocating.</summary>
    public const int MinArraysPerBucket = 4;

    /// <summary>Depth actually used: bounded by cores, because concurrency is.</summary>
    public static int ArraysPerBucket { get; } =
        Math.Clamp(2 * Environment.ProcessorCount, MinArraysPerBucket, MaxArraysPerBucket);

    /// <summary>
    /// Total bytes this pool may park, from <see cref="MemoryBudgets.IngestBufferBytes"/> — a
    /// share of the managed-heap limit, because request bodies are managed arrays on the large
    /// object heap. Read once: the budgets are a startup decision, and this is consulted on a
    /// path that runs per request.
    /// </summary>
    public static long MaxPooledTotalBytes { get; } = MemoryBudgets.Current().IngestBufferBytes;

    private static BoundedByteArrayPool _pool = Create();

    static IngestBufferPool() => PoolTrimPolicy.OnGen2Collection(static () => { TrimIfUnderPressure(); return true; });

    private static BoundedByteArrayPool Create() => new(MaxPooledBytes, ArraysPerBucket, MaxPooledTotalBytes);

    /// <summary>
    /// Bytes parked right now — what a <see cref="Trim"/> would release. Reported by
    /// <c>/api/diagnostics</c>: this pool holds the highest-volume ingest road's buffers, and
    /// until it could be read there was no figure anywhere that attributed them.
    /// </summary>
    public static long PooledBytes => Volatile.Read(ref _pool).PooledBytes;

    /// <summary>Rents an array of at least <paramref name="minimumLength"/> bytes. Contents are undefined.</summary>
    public static byte[] Rent(int minimumLength)
    {
        byte[] array = Volatile.Read(ref _pool).Rent(minimumLength);
        Observer?.Rented(array);
        return array;
    }

    /// <summary>
    /// Returns an array rented from THIS pool. Never call it twice for one array.
    ///
    /// <para>A return that races a <see cref="Trim"/> lands in the new pool instead of the old
    /// one. That is harmless and deliberate: both pools have identical bucket geometry, so the
    /// array is accepted, and the only consequence is that one buffer survives a trim it could
    /// have been dropped by.</para>
    /// </summary>
    public static void Return(byte[] array)
    {
        // Told BEFORE the pool has it back: after, another thread may already have rented it,
        // and an observer would see that rent ahead of this return.
        Observer?.Returning(array);
        Volatile.Read(ref _pool).Return(array);
    }

    /// <summary>
    /// Test hook: sees every rent and every return. Null outside tests, where it costs one static
    /// read and a null check on calls that each move a request body of kilobytes to megabytes.
    ///
    /// <para>It exists because nothing else can tell whether a receiver's buffers came from this
    /// pool and all came back to it. A buffer rented here and returned to
    /// <see cref="ArrayPool{T}.Shared"/>, or the reverse, raises no error anywhere; the pool just
    /// stops pooling. A field rather than a registration API so a test can install it with one
    /// <see cref="Interlocked.CompareExchange{T}(ref T, T, T)"/>.</para>
    /// </summary>
    internal static IIngestBufferPoolObserver? Observer;

    /// <summary>
    /// Drops every pooled array. The next rents allocate and refill.
    ///
    /// <para>Called at the end of a gen2 collection when the GC reports high memory load, and
    /// available directly so the behaviour can be tested without arranging for an
    /// out-of-memory condition.</para>
    /// </summary>
    public static void Trim() => Volatile.Write(ref _pool, Create());

    /// <summary>
    /// Shortest gap between two pressure trims — <see cref="PoolTrimPolicy.MinTrimInterval"/>,
    /// where the rule and its reasoning now live for both pools. Without a gap the trim feeds
    /// itself: every gen2 under sustained load empties the pool, the next gRPC request (which
    /// gets no Content-Length, so it doubles 64 KB → 2 MB) leaves about 4 MB of fresh
    /// large-object garbage refilling it, and that brings the next gen2 forward.
    /// </summary>
    public static TimeSpan MinTrimInterval => PoolTrimPolicy.MinTrimInterval;

    /// <summary><see cref="Stopwatch"/> timestamp of the last pressure trim; 0 = never.</summary>
    private static long _lastPressureTrim;

    /// <summary>
    /// The gen2 hook: <see cref="PoolTrimPolicy.ShouldTrim"/> decides, and a manual
    /// <see cref="Trim"/> deliberately does NOT move the window — this records only the trims
    /// the hysteresis is there to space out.
    ///
    /// <para>What is passed matters as much as the threshold: on a host-wide reading the policy
    /// weighs <see cref="PooledBytes"/> against the machine, so a neighbouring process cannot
    /// empty this pool every thirty seconds for ever while it holds a few megabytes that could
    /// not have relieved anything.</para>
    /// </summary>
    private static void TrimIfUnderPressure()
    {
        var info = GC.GetGCMemoryInfo();
        long now = Stopwatch.GetTimestamp();
        if (!PoolTrimPolicy.ShouldTrim(
                PooledBytes, info.MemoryLoadBytes, info.HighMemoryLoadThresholdBytes,
                info.TotalAvailableMemoryBytes, PoolTrimPolicy.ReadingIsScopedToThisProcess,
                now, Volatile.Read(ref _lastPressureTrim))) return;

        Volatile.Write(ref _lastPressureTrim, now);
        Trim();
    }
}

/// <summary>What <see cref="IngestBufferPool.Observer"/> is told. Called on the renting or returning thread.</summary>
internal interface IIngestBufferPoolObserver
{
    /// <summary>After <paramref name="array"/> has been handed out.</summary>
    void Rented(byte[] array);

    /// <summary>Before <paramref name="array"/> goes back into the pool.</summary>
    void Returning(byte[] array);
}
