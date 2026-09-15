using System.Numerics;
using System.Runtime.CompilerServices;

namespace Ameto.Indexing;

/// <summary>
/// The pools the index accumulators rent from — dedicated, not <c>ArrayPool.Shared</c>.
///
/// <para>The shared pool trims on every gen2 collection, and under memory pressure it drops
/// large buffers outright, so on a loaded box a builder's 1 MB slabs and multi-MB tables came
/// back as fresh LOH allocations group after group — the churn the pooling exists to remove,
/// reappearing exactly when the box can least afford it. It also hands out from per-core
/// stacks that ingestion shares.</para>
///
/// <h3>What it can hold, and why that is bounded on purpose</h3>
/// <para>A pool that is never trimmed is a memory leak with good manners: one large merge
/// group would leave its 46 MB entry array, 16 MB of tables and twenty slabs parked here for
/// the life of the process, invisible to GC pressure, on a stand whose whole budget is 512 MB.
/// So each pool is bounded three ways:</para>
/// <list type="bullet">
///   <item>a BYTE CAP per pool (<see cref="SlabPool{T}.MaxPooledBytes"/>) and a depth per size
///   bucket: a return that would exceed either is dropped — the array becomes ordinary
///   garbage;</item>
///   <item>a HIGH-WATER TRIM at the end of every gen2 collection: each size bucket keeps at
///   most as many arrays as were simultaneously OUT of it since the previous gen2, and drops
///   the rest. A size nothing rented is emptied; the slab bucket a giant group filled to
///   twenty keeps the three the steady state of small groups actually uses. So a one-off
///   group's arrays outlive it by at most one collection cycle;</item>
///   <item>a FULL TRIM when the GC reports memory load past its high threshold (on a container,
///   a fraction of the cgroup limit): every pool is emptied outright and refills — at most once
///   every <see cref="MinTrimInterval"/>; a gen2 inside that window does the high-water trim, or
///   the refill would drive the next gen2 and the pools would never get past one group.</item>
/// </list>
/// <para>Arrays larger than a pool's maximum are allocated and, on return, dropped — they are
/// live memory a group genuinely needed, accounted to it by the GC like any other.</para>
///
/// <para>MEASURED (<c>IndexBuildPoolProbe</c>), prop-dense trace-carrying events: see the
/// commit that introduced the caps for the resting levels at 16 MB and 64 MB groups.</para>
/// </summary>
internal static class IndexBuildPool
{
    public const int SlabBytes = 1 << 20;

    /// <summary>Term, posting and section-writer slabs: exactly 1 MB each.</summary>
    public static readonly SlabPool<byte> Slabs = new(maxLength: SlabBytes, maxArraysPerBucket: 64, maxPooledBytes: 64L << 20);

    /// <summary>Slot and hash tables; the largest is the trigram's 2^21-entry table (8 MB).</summary>
    public static readonly SlabPool<int> Ints = new(maxLength: 1 << 22, maxArraysPerBucket: 4, maxPooledBytes: 48L << 20);

    /// <summary>Entry arrays of one struct type — a pool per type.</summary>
    public static SlabPool<T> Entries<T>() where T : struct => EntryPool<T>.Instance;

    private static class EntryPool<T> where T : struct
    {
        public static readonly SlabPool<T> Instance = new(maxLength: 1 << 21, maxArraysPerBucket: 4, maxPooledBytes: 96L << 20);
        static EntryPool() => Register(Instance);
    }

    private static readonly List<ITrimmable> _all = new();

    static IndexBuildPool()
    {
        Register(Slabs);
        Register(Ints);
        Gen2GcCallback.Register(static () => { OnGen2(); return true; });
    }

    private static void Register(ITrimmable p) { lock (_all) _all.Add(p); }

    /// <summary>Bytes parked in every pool right now.</summary>
    public static long PooledBytes
    {
        get { long n = 0; lock (_all) foreach (var p in _all) n += p.PooledBytes; return n; }
    }

    /// <summary>
    /// Shortest gap between two pressure trims — the rule <c>IngestBufferPool</c> applies, for
    /// the same reason.
    ///
    /// <para>Without it the full trim feeds itself. Under sustained load past the GC's high
    /// threshold every gen2 empties every pool; the next 16 MB group then re-allocates ~30 MB of
    /// LOH refilling them (1 MB slabs, the 8 MB ASCII trigram table, the term table, the entry
    /// arrays), which spends the LOH budget and brings the next gen2 forward, which empties them
    /// again. The hit rate goes to zero exactly when the box is short of memory — the per-group
    /// LOH churn the pool exists to remove, caused by the pool. And the signal is not always ours
    /// to believe: on a bare Windows host <c>MemoryLoadBytes</c> is machine-wide.</para>
    ///
    /// <para>Between pressure trims a gen2 does the ordinary high-water trim, so the pools still
    /// shed what the last cycle did not use while the window is closed.</para>
    /// </summary>
    public static readonly TimeSpan MinTrimInterval = TimeSpan.FromSeconds(30);

    /// <summary><see cref="System.Diagnostics.Stopwatch"/> timestamp of the last pressure trim; 0 = never.
    /// Moved only by the gen2 hook — a manual <see cref="TrimAll"/> is not a trim the window spaces out.</summary>
    private static long _lastPressureTrim;

    /// <summary>The gen2 hook's action.</summary>
    public static void OnGen2()
    {
        var info = GC.GetGCMemoryInfo();
        OnGen2(info.MemoryLoadBytes, info.HighMemoryLoadThresholdBytes, System.Diagnostics.Stopwatch.GetTimestamp(),
               ref _lastPressureTrim, AllPools.Instance);
    }

    /// <summary>
    /// The gen2 policy with its inputs injected — memory info, clock, the window's state and the
    /// pools — so the branch it takes is testable without a real memory shortage or a real wait.
    /// Returns whether it emptied the pools.
    /// </summary>
    internal static bool OnGen2(long memoryLoadBytes, long highLoadThresholdBytes, long nowTimestamp,
                                ref long lastPressureTrimTimestamp, ITrimmable pools)
    {
        if (ShouldTrimAll(memoryLoadBytes, highLoadThresholdBytes, nowTimestamp, lastPressureTrimTimestamp))
        {
            lastPressureTrimTimestamp = nowTimestamp;
            pools.TrimAll();
            return true;
        }
        pools.TrimIdle();
        return false;
    }

    /// <summary>Each bucket keeps at most what was simultaneously out of it since the last call.</summary>
    public static void TrimIdle() { lock (_all) foreach (var p in _all) p.TrimIdle(); }

    /// <summary>Empties every pool.</summary>
    public static void TrimAll() { lock (_all) foreach (var p in _all) p.TrimAll(); }

    /// <summary>
    /// Whether a gen2 should empty every pool: memory load at or past the GC's own high threshold
    /// (on a container a fraction of the cgroup limit), and not again for
    /// <see cref="MinTrimInterval"/> after the last time it did.
    /// </summary>
    /// <param name="nowTimestamp">A <see cref="System.Diagnostics.Stopwatch.GetTimestamp"/> reading.</param>
    /// <param name="lastTrimTimestamp">The reading taken at the last pressure trim, or 0 for never.</param>
    public static bool ShouldTrimAll(long memoryLoadBytes, long highLoadThresholdBytes, long nowTimestamp, long lastTrimTimestamp)
    {
        if (highLoadThresholdBytes <= 0 || memoryLoadBytes < highLoadThresholdBytes) return false;
        if (lastTrimTimestamp == 0) return true;
        return nowTimestamp - lastTrimTimestamp >= (long)(MinTrimInterval.TotalSeconds * System.Diagnostics.Stopwatch.Frequency);
    }

    /// <summary>Every registered pool as one <see cref="ITrimmable"/> — what the gen2 hook trims.</summary>
    private sealed class AllPools : ITrimmable
    {
        public static readonly AllPools Instance = new();
        public long PooledBytes => IndexBuildPool.PooledBytes;
        public void TrimIdle() => IndexBuildPool.TrimIdle();
        public void TrimAll()  => IndexBuildPool.TrimAll();
    }

    internal interface ITrimmable
    {
        long PooledBytes { get; }
        void TrimIdle();
        void TrimAll();
    }

    /// <summary>
    /// Power-of-two size buckets, each a bounded stack under a lock; byte-accounted so the
    /// caps mean bytes and the probes can read what is parked. Rent hands out the exact bucket
    /// size (the tables use <c>Length</c> as their capacity, so it must be a power of two).
    /// </summary>
    internal sealed class SlabPool<T> : ITrimmable where T : struct
    {
        private const int MinLength = 16;
        private readonly T[][][] _buckets;     // [bucket][depth]
        private readonly int[]   _counts;      // parked per bucket
        private readonly int[]   _out;         // rented THIS cycle and not yet returned, per bucket (never below 0)
        private readonly int[]   _peakOut;     // high-water mark of _out since the last trim
        private readonly int     _maxPerBucket;
        private long             _pooledBytes;
        private readonly object  _lock = new();

        public readonly int  MaxLength;
        public readonly long MaxPooledBytes;

        public SlabPool(int maxLength, int maxArraysPerBucket, long maxPooledBytes)
        {
            MaxLength      = (int)BitOperations.RoundUpToPowerOf2((uint)maxLength);
            MaxPooledBytes = maxPooledBytes;
            _maxPerBucket  = maxArraysPerBucket;
            int buckets    = BitOperations.Log2((uint)MaxLength) - BitOperations.Log2(MinLength) + 1;
            _buckets = new T[buckets][][];
            _counts  = new int[buckets];
            _out     = new int[buckets];
            _peakOut = new int[buckets];
            for (int i = 0; i < buckets; i++) _buckets[i] = new T[maxArraysPerBucket][];
        }

        public long PooledBytes => Volatile.Read(ref _pooledBytes);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int BucketOf(int length) => BitOperations.Log2((uint)length) - BitOperations.Log2(MinLength);

        public T[] Rent(int minimumLength)
        {
            int length = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(minimumLength, MinLength));
            if (length > MaxLength) return new T[length];   // beyond the pool: live memory, GC-owned
            int b = BucketOf(length);
            lock (_lock)
            {
                if (++_out[b] > _peakOut[b]) _peakOut[b] = _out[b];
                if (_counts[b] > 0)
                {
                    var arr = _buckets[b][--_counts[b]];
                    _buckets[b][_counts[b]] = null!;
                    _pooledBytes -= (long)length * Unsafe.SizeOf<T>();
                    return arr;
                }
            }
            return new T[length];
        }

        public void Return(T[] array)
        {
            int length = array.Length;
            if (length > MaxLength || length < MinLength || !BitOperations.IsPow2(length)) return;
            long bytes = (long)length * Unsafe.SizeOf<T>();
            int b = BucketOf(length);
            lock (_lock)
            {
                if (_out[b] > 0) _out[b]--;
                if (_counts[b] == _maxPerBucket || _pooledBytes + bytes > MaxPooledBytes) return;   // dropped
                _buckets[b][_counts[b]++] = array;
                _pooledBytes += bytes;
            }
        }

        public void TrimIdle()
        {
            lock (_lock)
            {
                for (int b = 0; b < _buckets.Length; b++)
                {
                    // Keep what the last cycle needed at once. The count is of THIS cycle's rents
                    // still out, floored at zero, so an array a builder never returns (a leak)
                    // or returns a cycle after renting it neither inflates nor deflates the
                    // next cycle's need for more than one cycle.
                    DropBeyond(b, _peakOut[b]);
                    _peakOut[b] = 0;
                    _out[b]     = 0;
                }
            }
        }

        public void TrimAll()
        {
            lock (_lock)
                for (int b = 0; b < _buckets.Length; b++) { DropBeyond(b, 0); _peakOut[b] = 0; _out[b] = 0; }
        }

        private void DropBeyond(int b, int keep)
        {
            while (_counts[b] > keep)
            {
                _buckets[b][--_counts[b]] = null!;
                _pooledBytes -= (long)(MinLength << b) * Unsafe.SizeOf<T>();
            }
        }
    }

    /// <summary>
    /// Runs an action at the end of each gen2 collection — the pattern the BCL's own pools use.
    /// The object is unreachable from the moment it is constructed, so a collection finalises
    /// it; the finaliser resurrects it with <see cref="GC.ReRegisterForFinalize"/>. After the
    /// first couple of collections it lives in gen2, which makes the callback a gen2 callback.
    /// </summary>
    private sealed class Gen2GcCallback
    {
        private readonly Func<bool> _callback;
        private Gen2GcCallback(Func<bool> callback) => _callback = callback;
        public static void Register(Func<bool> callback) => _ = new Gen2GcCallback(callback);

        ~Gen2GcCallback()
        {
            bool again;
            try { again = _callback(); } catch { again = false; }   // a throwing finaliser tears the process down
            if (again && !Environment.HasShutdownStarted) GC.ReRegisterForFinalize(this);
        }
    }
}
