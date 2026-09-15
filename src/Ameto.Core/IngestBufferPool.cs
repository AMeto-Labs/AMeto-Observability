using System.Buffers;
using System.Diagnostics;

namespace Ameto.Core;

/// <summary>
/// The buffer pool the OTLP receivers read their request bodies into — OTLP/HTTP, OTLP/gRPC,
/// and the gRPC gzip inflate target. (The CLEF receiver still rents from
/// <see cref="ArrayPool{T}.Shared"/>; moving it needs its rents and its returns changed
/// together, which is a separate change.)
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
/// does not get a Content-Length, so it doubles 64 KB → 2 MB and leaves an array in EVERY
/// bucket on the way; at 32 deep that is about 126 MB pinned for ever by 32 concurrent 2 MB
/// batches, on a stand whose whole budget is 512 MB. So:</para>
/// <list type="bullet">
///   <item>Depth is <c>2 x ProcessorCount</c>, clamped to
///   [<see cref="MinArraysPerBucket"/>, <see cref="MaxArraysPerBucket"/>] — request
///   concurrency is bounded by cores far more tightly than by that ceiling, and the small
///   containers that cannot afford the memory are exactly the ones with few cores.</item>
///   <item><see cref="Trim"/> runs after a gen2 collection and empties the pool outright when
///   the GC says memory load has passed its high threshold — but at most once every
///   <see cref="MinTrimInterval"/>, or the refill it causes drives the next collection and the
///   pool never gets past one. Dropping the whole pool is the trim: the arrays become garbage
///   in the collection that follows, and a pool that has to refill under pressure is the
///   outcome worth having.</item>
/// </list>
/// <para>The arithmetic, stated plainly rather than flatteringly: a full set of buckets up
/// to <see cref="MaxPooledBytes"/> is about <c>2 x MaxPooledBytes</c> = 16 MB, so the
/// absolute ceiling between two trims is <c>depth x 16 MB</c> — 64 MB at the four-deep floor,
/// 512 MB at the 32-deep ceiling. Reaching the top of that needs 32 concurrent EIGHT-MEGABYTE
/// batches to have been in flight at once, which is 256 MB of live request bodies: memory the
/// process had committed at that peak whatever pool it came from. The realistic shape — 2 MB
/// batches through the doubling reader — is <c>depth x ~4 MB</c>, so 16 MB on the two-core
/// stand. The trim is what makes either of those a peak rather than a resting level.</para>
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
    /// </summary>
    public const int MaxPooledBytes = 8 * 1024 * 1024;

    /// <summary>Ceiling on the per-bucket depth, whatever the core count says.</summary>
    public const int MaxArraysPerBucket = 32;

    /// <summary>Floor on it, so a single-core container still pools rather than allocating.</summary>
    public const int MinArraysPerBucket = 4;

    /// <summary>Depth actually used: bounded by cores, because concurrency is.</summary>
    public static int ArraysPerBucket { get; } =
        Math.Clamp(2 * Environment.ProcessorCount, MinArraysPerBucket, MaxArraysPerBucket);

    private static ArrayPool<byte> _pool = Create();

    static IngestBufferPool() => Gen2GcCallback.Register(static () => { TrimIfUnderPressure(); return true; });

    private static ArrayPool<byte> Create() => ArrayPool<byte>.Create(MaxPooledBytes, ArraysPerBucket);

    /// <summary>Rents an array of at least <paramref name="minimumLength"/> bytes. Contents are undefined.</summary>
    public static byte[] Rent(int minimumLength) => Volatile.Read(ref _pool).Rent(minimumLength);

    /// <summary>
    /// Returns an array rented from THIS pool. Never call it twice for one array.
    ///
    /// <para>A return that races a <see cref="Trim"/> lands in the new pool instead of the old
    /// one. That is harmless and deliberate: both pools have identical bucket geometry, so the
    /// array is accepted, and the only consequence is that one buffer survives a trim it could
    /// have been dropped by.</para>
    /// </summary>
    public static void Return(byte[] array) => Volatile.Read(ref _pool).Return(array);

    /// <summary>
    /// Drops every pooled array. The next rents allocate and refill.
    ///
    /// <para>Called at the end of a gen2 collection when the GC reports high memory load, and
    /// available directly so the behaviour can be tested without arranging for an
    /// out-of-memory condition.</para>
    /// </summary>
    public static void Trim() => Volatile.Write(ref _pool, Create());

    /// <summary>
    /// Shortest gap between two pressure trims.
    ///
    /// <para>Without a gap the trim feeds itself. Every gen2 under sustained load empties the
    /// pool; the next gRPC request, which has no Content-Length and so doubles 64 KB → 2 MB,
    /// then leaves about 4 MB of fresh large-object garbage refilling it; that brings the next
    /// gen2 forward, which empties it again. The pool never gets past one refill and the whole
    /// point of it is gone — while the allocation it causes makes the pressure worse. And the
    /// signal is not always ours to believe: on a bare Windows host <c>MemoryLoadBytes</c> is
    /// machine-wide, so one greedy neighbour would switch the pool off permanently.</para>
    ///
    /// <para>Thirty seconds is far longer than the interval between gen2 collections under
    /// load and far shorter than an operator would wait to see memory come back.</para>
    /// </summary>
    public static readonly TimeSpan MinTrimInterval = TimeSpan.FromSeconds(30);

    /// <summary><see cref="Stopwatch"/> timestamp of the last pressure trim; 0 = never.</summary>
    private static long _lastPressureTrim;

    /// <summary>
    /// Whether the pool should be emptied: the GC's own high-memory-load threshold — which on a
    /// container is a fraction of the cgroup limit rather than of the host's RAM — and not
    /// again for <see cref="MinTrimInterval"/> after the last time it was.
    ///
    /// <para>Public, and taking the clock rather than reading it, so the rule can be tested
    /// against numbers instead of against a real memory shortage and a real wait.</para>
    /// </summary>
    /// <param name="nowTimestamp">A <see cref="Stopwatch.GetTimestamp"/> reading.</param>
    /// <param name="lastTrimTimestamp">The reading taken at the last trim, or 0 for never.</param>
    public static bool ShouldTrim(
        long memoryLoadBytes, long highLoadThresholdBytes, long nowTimestamp, long lastTrimTimestamp)
    {
        if (highLoadThresholdBytes <= 0 || memoryLoadBytes < highLoadThresholdBytes) return false;
        if (lastTrimTimestamp == 0) return true;
        return nowTimestamp - lastTrimTimestamp >= (long)(MinTrimInterval.TotalSeconds * Stopwatch.Frequency);
    }

    /// <summary>
    /// The gen2 hook. A manual <see cref="Trim"/> deliberately does NOT move the window — this
    /// records only the trims the hysteresis is there to space out.
    /// </summary>
    private static void TrimIfUnderPressure()
    {
        var info = GC.GetGCMemoryInfo();
        long now = Stopwatch.GetTimestamp();
        if (!ShouldTrim(info.MemoryLoadBytes, info.HighMemoryLoadThresholdBytes,
                        now, Volatile.Read(ref _lastPressureTrim))) return;

        Volatile.Write(ref _lastPressureTrim, now);
        Trim();
    }

    /// <summary>
    /// Runs an action at the end of each gen2 collection, the pattern the BCL's own pools use.
    ///
    /// <para>The object is unreachable from the moment it is constructed, so a collection
    /// finalises it; the finaliser resurrects it with <see cref="GC.ReRegisterForFinalize"/>.
    /// After the first couple of collections it lives in gen2, which is what makes the
    /// callback a gen2 callback rather than a gen0 one.</para>
    /// </summary>
    private sealed class Gen2GcCallback
    {
        private readonly Func<bool> _callback;

        private Gen2GcCallback(Func<bool> callback) => _callback = callback;

        public static void Register(Func<bool> callback) => _ = new Gen2GcCallback(callback);

        ~Gen2GcCallback()
        {
            bool again;
            // An unhandled exception in a finaliser tears the process down, and nothing here is
            // worth that. (This runs on the finaliser thread after the collection, not inside
            // it, which is also why GC.GetGCMemoryInfo below reports the one that just ended.)
            try { again = _callback(); } catch { again = false; }
            if (again && !Environment.HasShutdownStarted) GC.ReRegisterForFinalize(this);
        }
    }
}
