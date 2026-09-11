using System.Buffers;

namespace Ameto.Core;

/// <summary>
/// The buffer pool every ingest receiver reads its request body into — CLEF, OTLP/HTTP,
/// OTLP/gRPC, and the gRPC gzip inflate target.
///
/// <para>These buffers are large and they are rented one per request.
/// <see cref="ArrayPool{T}.Shared"/> is the wrong shape for that: it keeps one array per
/// thread plus at most eight per core in each size bucket, and it drops what it holds on
/// every gen2 collection. A 1.4 MB batch rounds up to the 2 MB bucket, so past that shallow
/// depth each concurrent request gets a fresh, zeroed 2 MB array straight on the large
/// object heap — which is what an allocation trace of the stand under load showed, and the
/// reason the LOH grew while nothing leaked.</para>
///
/// <para>This pool is deeper and is never trimmed, so a steady request rate settles into
/// reusing the same arrays. What it holds is bounded by the peak concurrency it has seen —
/// at most <see cref="MaxArraysPerBucket"/> arrays in any one size — which is memory the
/// process had committed at that peak anyway; it simply does not hand it back. Anything
/// larger than <see cref="MaxPooledBytes"/> is allocated per call and ignored on return,
/// exactly as <c>ArrayPool</c>'s own implementation does.</para>
///
/// <para>Arrays are rented dirty and must therefore be treated as uninitialised, and — as
/// with any pool — an array rented here must be returned here, in a <c>finally</c>, exactly
/// once. Handing one back to <see cref="ArrayPool{T}.Shared"/> is not a correctness bug but
/// it empties this pool one request at a time, which defeats the whole point of it.</para>
/// </summary>
public static class IngestBufferPool
{
    /// <summary>
    /// Largest array kept. Covers the default ceilings of both receivers —
    /// <c>Ingestion.MaxOtlpBatchBytes</c> (8 MiB) and <c>Ingestion.MaxBatchBytes</c> (4 MiB) —
    /// so a body that is accepted at all is a body this pool can serve.
    /// </summary>
    public const int MaxPooledBytes = 8 * 1024 * 1024;

    /// <summary>
    /// Depth per size bucket. Deep enough that ordinary concurrency never misses, shallow
    /// enough that the worst case is bounded and small next to the tier and index memory the
    /// same process holds.
    /// </summary>
    public const int MaxArraysPerBucket = 32;

    private static readonly ArrayPool<byte> Pool =
        ArrayPool<byte>.Create(MaxPooledBytes, MaxArraysPerBucket);

    /// <summary>Rents an array of at least <paramref name="minimumLength"/> bytes. Contents are undefined.</summary>
    public static byte[] Rent(int minimumLength) => Pool.Rent(minimumLength);

    /// <summary>Returns an array rented from THIS pool. Never call it twice for one array.</summary>
    public static void Return(byte[] array) => Pool.Return(array);
}
