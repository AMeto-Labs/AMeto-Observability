using System.Buffers;

namespace Ameto.Indexing;

/// <summary>
/// The pools the index accumulators rent from — dedicated, not <see cref="ArrayPool{T}.Shared"/>.
///
/// <para>The shared pool trims on every gen2 collection, and under memory pressure it drops
/// large buffers outright, so on a loaded box a builder's 1 MB slabs and multi-MB tables came
/// back as fresh LOH allocations group after group — the churn the pooling exists to remove,
/// reappearing exactly when the box can least afford it. It also hands out from per-core
/// stacks that ingestion shares. These pools hold what is returned to them, bounded per bucket:
/// at most a few builders' worth (flush width plus a merge), and only sizes that were actually
/// rented, so idle retention is what the busiest recent group needed and nothing more.</para>
///
/// <para>Slabs are exactly <see cref="SlabBytes"/>; anything larger is allocated outside the
/// pool and dropped on return (the <c>Create</c> pool's behaviour for over-length arrays),
/// which only a term longer than a megabyte can ask for.</para>
/// </summary>
internal static class IndexBuildPool
{
    public const int SlabBytes = 1 << 20;

    /// <summary>Term, posting and section-writer slabs.</summary>
    public static readonly ArrayPool<byte> Slabs = ArrayPool<byte>.Create(SlabBytes, maxArraysPerBucket: 256);

    /// <summary>Slot and hash tables; the largest is the trigram's 2^21-entry table.</summary>
    public static readonly ArrayPool<int> Ints = ArrayPool<int>.Create(1 << 22, maxArraysPerBucket: 8);

    /// <summary>Entry arrays of one struct type — a pool per type, sized for a 64 MB group's terms.</summary>
    public static ArrayPool<T> Entries<T>() where T : struct => EntryPool<T>.Instance;

    private static class EntryPool<T> where T : struct
    {
        public static readonly ArrayPool<T> Instance = ArrayPool<T>.Create(1 << 22, maxArraysPerBucket: 8);
    }
}
