using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Indexing;

/// <summary>
/// What identifies the BYTES behind one (path, group) cache key: the segment's id, node and file
/// size, the group's directory record — its section offsets, row range and time bounds — the
/// file's last-write time, and a CRC of its block index.
///
/// <para>A path is not an identity. A replicated <c>{node}-{id}</c> segment can be re-imported
/// under the name retention unlinked — a peer wiped and reinstalled under the same NodeId, or two
/// nodes sharing one — and a memo taught from the old bytes answers for the new ones wrongly: its
/// postings are ordinals of the old file, its "absent" and its bloom verdicts are about the old
/// file, so the group loses rows it holds. An entry remembers the fingerprint it learned under,
/// and a query that opens the key over a different one gets a fresh memo.</para>
///
/// <para>None of the structural fields is content: a peer wiped and reinstalled under the same
/// NodeId can replay the same id, row counts and timestamps with values of the same lengths, and
/// its file then agrees on all of them. The format stores no digest, so two cheap signals that
/// the bytes changed stand in for one. The last-write time comes from the stat the reader makes
/// anyway: a segment is never rewritten in place, so it moves only when the file is replaced. The
/// block-index CRC comes from bytes the reader reads anyway: it moves when the compressed blocks
/// are laid out differently, which also covers a replacement that kept the old timestamp (a
/// time-preserving copy or restore). A replacement that defeats both is not a case the cache
/// tries to separate.</para>
/// </summary>
public readonly record struct IndexGroupFingerprint(
    ulong SegmentId, NodeId Node, long FileBytes, SegmentIndexGroup Group, long LastWriteTicks, uint BlockIndexCrc)
{
    /// <summary>The fingerprint of group <paramref name="group"/> of an open segment.</summary>
    public static IndexGroupFingerprint Of(SegmentReader segment, int group) =>
        new(segment.Info.Id.Value, segment.Info.NodeId, segment.Info.CompressedBytes, segment.Groups[group],
            segment.LastWriteTicks, segment.BlockIndexCrc);
}

/// <summary>
/// One query's use of one index group: the group's memo — the cached one when there is a cache,
/// found or created, otherwise a throwaway — answering through the segment this query already
/// has open.
///
/// <para><b>Why the cache holds memos and not sections.</b> A reader that keeps its group's packed
/// sections (<see cref="SegmentIndexReader.Load"/>) is charged what it copied: 40 MB for an
/// Information group of the #80 stand, 258 MB for the stand's 24 groups against a 256 MB budget,
/// so a slightly larger store thrashes the LRU and the 512 MB stand's 46 MB holds one group. Yet
/// the sections are already in memory for as long as any query can use them — the segment file is
/// mapped by the query that asks, and its pages are the OS's to cache. So the cache keeps only
/// what a query WORKED OUT (<see cref="SegmentIndexReader.CreateMemo"/>): kilobytes per group, and
/// a view lends it the sections of this query's <see cref="SegmentReader"/>, renting one only
/// when a question is new.</para>
///
/// <para><b>Hits.</b> A group this view answered without renting any section was a hit; one that
/// needed a section — a new filter, or a memo the cache had evicted — was a miss. That is what
/// <see cref="SegmentIndexCache.HitCount"/> and <see cref="SegmentIndexCache.MissCount"/> count for
/// the query path, recorded when the view is disposed.</para>
///
/// <para><b>Lifetime.</b> Single-threaded, and scoped to one group of one query: rented sections go
/// back to the pool, a bloom it had to deserialise is freed, and the cache lease is released —
/// charging the memo's growth — on <see cref="Dispose"/>. The memo it borrowed outlives it in the
/// cache; nothing native does.</para>
/// </summary>
public sealed class SegmentIndexView : ISegmentIndex, IIndexSectionSource, IDisposable
{
    private const int InvertedRead = 1, TrigramRead = 2, BloomRead = 4;

    private readonly SegmentIndexReader      _index;
    private readonly SegmentIndexCache.Lease _lease;
    private readonly bool                    _leased;

    // Where sections come from: the query's segment, or — for tests — sections in hand.
    private readonly SegmentReader?       _segment;
    private readonly int                  _group;
    private readonly ReadOnlyMemory<byte> _heldInverted, _heldTrigram, _heldBloom;

    private PooledSection       _inverted, _trigram;
    private SegmentBloomFilter? _bloom;
    private int                 _read;
    private bool                _disposed;

    private SegmentIndexView(SegmentIndexReader index, SegmentIndexCache.Lease lease, bool leased,
                             SegmentReader? segment, int group,
                             ReadOnlyMemory<byte> inverted = default, ReadOnlyMemory<byte> trigram = default,
                             ReadOnlyMemory<byte> bloom = default)
    {
        _index        = index;
        _lease        = lease;
        _leased       = leased;
        _segment      = segment;
        _group        = group;
        _heldInverted = inverted;
        _heldTrigram  = trigram;
        _heldBloom    = bloom;
    }

    /// <summary>
    /// Opens group <paramref name="group"/> of <paramref name="path"/> for one query, reading any
    /// section it needs from <paramref name="segment"/> (which the caller keeps open for longer
    /// than this view). With an enabled cache the memo is the cached one — created empty if the
    /// group has none — and every query that opens the group shares what it learns; without one,
    /// the memo lives and dies with this view.
    /// </summary>
    public static SegmentIndexView Open(SegmentIndexCache? cache, string path, int group, SegmentReader segment)
    {
        if (cache is { Enabled: true })
        {
            var lease = cache.AcquireOrAdd(path, group, IndexGroupFingerprint.Of(segment, group));
            return new SegmentIndexView(lease.Index, lease, leased: true, segment, group);
        }
        return new SegmentIndexView(SegmentIndexReader.CreateMemo(), default, leased: false, segment, group);
    }

    /// <summary>A view over sections already in hand — for tests, which need no segment file to
    /// exercise the memo, the cache and their races. <paramref name="fingerprint"/> stands for the
    /// bytes: a test that swaps them passes a different one.</summary>
    internal static SegmentIndexView OverSections(
        SegmentIndexCache? cache, string path, int group,
        ReadOnlyMemory<byte> inverted, ReadOnlyMemory<byte> trigram, ReadOnlyMemory<byte> bloom,
        IndexGroupFingerprint fingerprint = default)
    {
        if (cache is { Enabled: true })
        {
            var lease = cache.AcquireOrAdd(path, group, fingerprint);
            return new SegmentIndexView(lease.Index, lease, leased: true, null, group, inverted, trigram, bloom);
        }
        return new SegmentIndexView(SegmentIndexReader.CreateMemo(), default, leased: false, null, group,
                                    inverted, trigram, bloom);
    }

    /// <summary>The memo this view answers through.</summary>
    internal SegmentIndexReader Index => _index;

    /// <summary>True once this view has had to read any of the group's sections: a miss.</summary>
    public bool ReadSections => _read != 0;

    // ── Questions ─────────────────────────────────────────────────────────────

    /// <summary>
    /// The bloom gate — <see cref="SegmentIndexReader.MightContainValue(SegmentBloomFilter, object?)"/>
    /// with each probed text's verdict remembered, so the bits are read only for a text this group
    /// has never been asked about.
    /// </summary>
    public bool MightContainValue(object? value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _index.MightContainValue(value, this);
    }

    /// <inheritdoc/>
    public uint[]? Lookup(string propertyName, object? value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _index.Lookup(propertyName, value, this);
    }

    /// <inheritdoc/>
    public bool MightContain(string propertyName, object? value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _index.MightContain(propertyName, value, this);
    }

    /// <inheritdoc/>
    public uint[]? LookupTrigram(ReadOnlySpan<char> text)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _index.LookupTrigram(text, this);
    }

    /// <inheritdoc/>
    public uint[]? LookupIntersect(IReadOnlyList<(string property, object? value)> predicates)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _index.LookupIntersect(predicates, this);
    }

    // ── Sections, rented on first need ────────────────────────────────────────

    ReadOnlySpan<byte> IIndexSectionSource.InvertedSection()
    {
        if (_segment is null) { _read |= InvertedRead; return _heldInverted.Span; }
        if ((_read & InvertedRead) == 0)
        {
            _inverted = _segment.RentInvertedIndexBytes(_group);
            _read    |= InvertedRead;
        }
        return _inverted.Span;
    }

    ReadOnlySpan<byte> IIndexSectionSource.TrigramSection()
    {
        if (_segment is null) { _read |= TrigramRead; return _heldTrigram.Span; }
        if ((_read & TrigramRead) == 0)
        {
            _trigram = _segment.RentTrigramIndexBytes(_group);
            _read   |= TrigramRead;
        }
        return _trigram.Span;
    }

    /// <summary>
    /// Deserialised on first need and freed with the view. The section is returned to the pool
    /// as soon as its bits are copied out: the filter owns its own native copy.
    /// </summary>
    SegmentBloomFilter IIndexSectionSource.BloomFilter()
    {
        if (_bloom is not null) return _bloom;
        _read |= BloomRead;
        if (_segment is null) return _bloom = SegmentBloomFilter.Deserialise(_heldBloom.Span);

        using var section = _segment.RentBloomFilterBytes(_group);
        return _bloom = SegmentBloomFilter.Deserialise(section.Span);
    }

    /// <summary>
    /// Returns what was rented, frees the bloom, and hands the memo back to the cache — recording
    /// a hit or a miss, and charging whatever the memo learned. Idempotent.
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _bloom?.Dispose();
        if (_segment is not null)
        {
            if ((_read & InvertedRead) != 0) _inverted.Dispose();
            if ((_read & TrigramRead)  != 0) _trigram.Dispose();
        }
        if (_leased) _lease.Complete(readSections: _read != 0);
    }
}
