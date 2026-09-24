namespace Ameto.Storage;

/// <summary>
/// What one merge call or cold-maintenance pass did (see
/// <see cref="StorageEngine.MergeSmallSegmentsOnceAsync"/>). Three answers, not a bool: a call
/// that lost the merge gate to a running merge has learnt nothing about whether anything is left
/// to merge, and reading it as "nothing to merge" stopped fixpoint loops short and sent the
/// maintenance loop into its long idle pause.
/// </summary>
internal enum MergeOutcome : byte
{
    /// <summary>The planner found no batch worth merging, or none it could open — the steady state.</summary>
    NothingToMerge,

    /// <summary>A batch was merged and committed.</summary>
    Merged,

    /// <summary>Another merge or maintenance pass held the merge gate; this call did nothing.</summary>
    Busy,
}
