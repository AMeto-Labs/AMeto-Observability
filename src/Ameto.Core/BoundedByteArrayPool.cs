using System.Numerics;

namespace Ameto.Core;

/// <summary>
/// Power-of-two size buckets of <c>byte[]</c>, each a bounded stack under a lock, with the
/// parked bytes ACCOUNTED — so a total cap means bytes rather than a count, and what is held can
/// be reported.
///
/// <para>It exists because <see cref="System.Buffers.ArrayPool{T}.Create"/> gives neither. Its
/// depth is per bucket, so the real ceiling is <c>depth x (a full set of buckets)</c> — for
/// request bodies that is <c>depth x ~2 x the largest array</c>, because a reader with no
/// Content-Length doubles from 64 KB and leaves an array in every bucket on the way. And nothing
/// can read back what it holds, so the footprint could not be put in <c>/api/diagnostics</c> or
/// subtracted from any budget. This keeps the same bucket geometry and adds the two missing
/// halves: a byte cap enforced on <see cref="Return"/>, and <see cref="PooledBytes"/>.</para>
///
/// <para>The lock is per pool, not per bucket, which the BCL's configurable pool also does. These
/// are request-sized buffers — one rent and one return per HTTP request, against a body of
/// kilobytes to megabytes — so the contention that matters here is memory, not the lock.</para>
///
/// <para>Arrays come back DIRTY, as from any pool. An array returned here must have been rented
/// here, exactly once; a foreign array of the right shape is accepted and simply pools something
/// its owner may still be using, which is why <c>IngestBufferPool.Observer</c> exists.</para>
/// </summary>
internal sealed class BoundedByteArrayPool
{
    /// <summary>Smallest bucket. Below this a rent allocates: the saving is not worth a slot.</summary>
    private const int MinLength = 4096;

    private readonly byte[]?[][] _buckets;      // [bucket][depth]
    private readonly int[]       _counts;       // parked per bucket
    private readonly int         _maxPerBucket;
    private readonly Lock        _lock = new();
    private long                 _pooledBytes;

    /// <summary>Largest array this pool will keep. Above it, rents allocate and returns are dropped.</summary>
    public readonly int MaxLength;

    /// <summary>Total bytes that may be parked across every bucket.</summary>
    public readonly long MaxPooledBytes;

    public BoundedByteArrayPool(int maxLength, int maxArraysPerBucket, long maxPooledBytes)
    {
        MaxLength      = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(maxLength, MinLength));
        MaxPooledBytes = Math.Max(maxPooledBytes, MinLength);
        _maxPerBucket  = Math.Max(1, maxArraysPerBucket);

        int buckets = BitOperations.Log2((uint)MaxLength) - BitOperations.Log2(MinLength) + 1;
        _buckets = new byte[]?[buckets][];
        _counts  = new int[buckets];
        for (int i = 0; i < buckets; i++) _buckets[i] = new byte[]?[_maxPerBucket];
    }

    /// <summary>Bytes parked right now — what a trim would release, and what diagnostics reports.</summary>
    public long PooledBytes { get { lock (_lock) return _pooledBytes; } }

    private static int BucketOf(int length) => BitOperations.Log2((uint)length) - BitOperations.Log2(MinLength);

    /// <summary>
    /// An array of at least <paramref name="minimumLength"/> bytes, contents undefined. Past
    /// <see cref="MaxLength"/> it is allocated at exactly the length asked for — correct, just
    /// not pooled, which is what an operator raising a body limit past this pool gets.
    /// </summary>
    public byte[] Rent(int minimumLength)
    {
        if (minimumLength > MaxLength) return new byte[minimumLength];

        int length = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(minimumLength, MinLength));
        int b      = BucketOf(length);
        lock (_lock)
        {
            if (_counts[b] > 0)
            {
                var array = _buckets[b][--_counts[b]]!;
                _buckets[b][_counts[b]] = null;
                _pooledBytes -= array.Length;
                return array;
            }
        }
        return new byte[length];
    }

    /// <summary>
    /// Takes <paramref name="array"/> back if there is room for it under BOTH bounds — the
    /// bucket's depth and the pool's total bytes. Otherwise it is dropped and becomes ordinary
    /// garbage, which is the behaviour that makes the cap real rather than advisory.
    /// </summary>
    public void Return(byte[]? array)
    {
        if (array is null) return;

        int length = array.Length;
        if (length > MaxLength || length < MinLength || !BitOperations.IsPow2(length)) return;

        int b = BucketOf(length);
        lock (_lock)
        {
            if (_counts[b] == _maxPerBucket || _pooledBytes + length > MaxPooledBytes) return;   // dropped
            _buckets[b][_counts[b]++] = array;
            _pooledBytes += length;
        }
    }

    /// <summary>Drops everything parked. The arrays become garbage in the collection that follows.</summary>
    public void Clear()
    {
        lock (_lock)
        {
            for (int b = 0; b < _buckets.Length; b++)
            {
                var bucket = _buckets[b];
                for (int i = 0; i < _counts[b]; i++) bucket[i] = null;
                _counts[b] = 0;
            }
            _pooledBytes = 0;
        }
    }
}
