namespace Ameto.Storage;

/// <summary>
/// What a query has decided about the index groups of one segment, asked for a group at a time
/// by the read that walks them (<see cref="SegmentReader.ReadGroupsAsync"/>).
///
/// <para>An interface and not a list, because the answer for a group may not exist yet when the
/// read starts: the caller may leave the costly ones — a posting list a row — until the read
/// reaches the group, and a read that ends first never asks. So <see cref="Read"/> is called at
/// most once per group, in the order the read walks them, from the thread that drives the read,
/// and only for groups it gets to.</para>
/// </summary>
public interface ISegmentGroupPlan
{
    /// <summary>What to read of group <paramref name="group"/> (an index into <see cref="SegmentReader.Groups"/>).</summary>
    GroupRead Read(int group);
}

/// <summary>
/// One group's part in a read: none of it, all of it, or the rows named. The three answers a
/// group's index can give about a filter, carried as what they are — "all of it" is a state here
/// and not a list of every ordinal the group holds.
/// </summary>
public readonly struct GroupRead
{
    private enum Part : byte { None, Whole, Named }

    private readonly Part _part;

    private GroupRead(Part part, ReadOnlyMemory<uint> candidates)
    {
        _part      = part;
        Candidates = candidates;
    }

    /// <summary>Nothing of the group is read. The default value, so an unset slot reads nothing.</summary>
    public static GroupRead Skip => default;

    /// <summary>Every row of the group is read.</summary>
    public static GroupRead Whole => new(Part.Whole, default);

    /// <summary>
    /// Only the rows at <paramref name="candidates"/> — file ordinals, all of them inside the
    /// group — are read; an empty list reads nothing.
    ///
    /// <para>Read-only memory, because the list is often not the caller's to give away: it may be
    /// the array an index cache keeps and answers every later query from, lent for this read
    /// without a copy. The reader cannot write to it, by type.</para>
    /// </summary>
    public static GroupRead Only(ReadOnlyMemory<uint> candidates) => new(Part.Named, candidates);

    /// <summary>True for <see cref="Skip"/>.</summary>
    public bool IsSkip => _part == Part.None;

    /// <summary>True when the group's rows are named (<see cref="Only"/>), even if none is.</summary>
    public bool IsNamed => _part == Part.Named;

    /// <summary>The rows named; empty unless <see cref="IsNamed"/>.</summary>
    public ReadOnlyMemory<uint> Candidates { get; }
}
