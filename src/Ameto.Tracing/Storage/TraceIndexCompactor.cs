using Microsoft.Extensions.Logging;

namespace Ameto.Tracing.Storage;

/// <summary>
/// MERGES INDEX RUNS INTO FEWER, BIGGER ONES — the step that stops the index scaling with the
/// number of segments.
///
/// <para>WHAT IT IS FOR, stated as a number rather than a principle. Every open run keeps its bloom
/// filter and sparse block map in memory: about 12 KB per ten thousand traces. One run per segment
/// is free at a hundred segments (a megabyte or two) and is not free at ten thousand — that is over
/// a hundred megabytes on a box with five hundred, held permanently, to answer a question the disk
/// could answer. Merging ten runs into one replaces ten blooms with one sized for the union, which
/// is roughly a tenth of the memory and one bloom check instead of ten.</para>
///
/// <para>IT NEVER CHANGES WHAT IS COVERED, and that single property is what makes it safe to
/// interrupt. The merged run carries the union of its inputs' covered segments, so exactly the same
/// segments are vouched for before and after; a crash mid-merge leaves a temp file nobody names and
/// the old runs still in the manifest. There is no window in which coverage is claimed by a file
/// that does not exist, because the manifest is only touched once, after the rename.</para>
///
/// <para>DEAD ENTRIES ARE DROPPED HERE, and this is the only place they are. A segment that has
/// been compacted away or expired leaves its entries behind in any merged run that held it —
/// harmless, because the read path matches hits against live segments, but they accumulate. The
/// merge simply does not copy them forward, which is what a tombstone would have been for in a
/// general LSM and is free here because the catalog already says who is alive.</para>
/// </summary>
internal sealed class TraceIndexCompactor
{
    /// <summary>
    /// Runs at one level before they are merged into the next. Ten is the classic levelled ratio
    /// and the arithmetic is unremarkable here: a level holds ten times its predecessor, so three
    /// levels reach a thousand segments and four reach ten thousand.
    /// </summary>
    internal const int RunsPerLevel = 10;

    /// <summary>
    /// The most runs one merge will take at once. A merge reads one block per input run at a time,
    /// so the memory is bounded by this times the block size — not by the size of the runs. Kept
    /// modest anyway: a merge holds the disk for its duration and this is a background chore.
    /// </summary>
    internal const int MaxRunsPerMerge = 32;

    private readonly string  _dir;
    private readonly ILogger _logger;

    public TraceIndexCompactor(string dir, ILogger logger)
    {
        _dir    = dir;
        _logger = logger;
    }

    /// <summary>
    /// Picks the lowest level that has accumulated enough runs, or returns an empty list.
    ///
    /// <para>Lowest first, deliberately: level 1 is where new runs arrive, so draining it is what
    /// keeps the run count from growing, and doing it before the bigger levels means the cheap work
    /// happens first and often.</para>
    /// </summary>
    internal static List<TraceIndexRun> SelectMergeBatch(IReadOnlyList<TraceIndexRun> runs)
    {
        var byLevel = new Dictionary<int, List<TraceIndexRun>>();
        foreach (var r in runs)
        {
            if (!byLevel.TryGetValue(r.Level, out var list)) byLevel[r.Level] = list = new();
            list.Add(r);
        }

        foreach (int level in byLevel.Keys.Order())
        {
            var list = byLevel[level];
            if (list.Count < RunsPerLevel) continue;

            // Smallest first: merging the small ones costs least and reduces the count most per
            // byte rewritten, which is the same reason the segment compactor works in size tiers.
            list.Sort(static (a, b) => a.EntryCount.CompareTo(b.EntryCount));
            return list.Take(MaxRunsPerMerge).ToList();
        }
        return [];
    }

    /// <summary>
    /// Merges <paramref name="batch"/> into one run at the next level, keeping only entries whose
    /// segment is still in <paramref name="liveSegments"/>.
    ///
    /// <para>Returns null when nothing usable came out — an input that will not open, or a batch
    /// whose segments have all gone. The caller leaves the manifest alone in that case, which
    /// leaves coverage exactly as it was.</para>
    /// </summary>
    public TraceIndexRun? Merge(IReadOnlyList<TraceIndexRun> batch, IReadOnlySet<ulong> liveSegments)
    {
        if (batch.Count == 0) return null;

        int nextLevel = batch.Max(r => r.Level) + 1;
        string path   = Path.Combine(_dir, $"tix-L{nextLevel}-{Guid.NewGuid():N}.tix");

        var readers = new List<TraceIndexReader>(batch.Count);
        try
        {
            foreach (var run in batch)
            {
                var r = TraceIndexReader.Open(run.FilePath);
                if (r is null)
                {
                    // ONE UNREADABLE INPUT ABORTS THE WHOLE MERGE. Carrying on would produce a
                    // merged run that claims its inputs' coverage while missing that input's
                    // entries — a covered segment whose traces the index cannot find, which is the
                    // one failure mode this design does not tolerate. The bad run is left for
                    // TraceIndexStore.Sync to notice and withdraw.
                    _logger.LogWarning(
                        "Trace index merge abandoned: {Path} would not open, and merging without it "
                      + "would produce a run that vouches for segments it cannot answer for", run.FilePath);
                    return null;
                }
                readers.Add(r);
            }

            // Enumerating each run in key order and writing through TraceIndexWriter, which sorts
            // again before it writes. Sorting twice is a real cost only when a run is enormous, and
            // buying the k-way merge machinery to avoid it would be paying complexity for a
            // background chore that already holds one block per input.
            var w = new TraceIndexWriter();
            int copied = 0, dropped = 0;
            foreach (var r in readers)
            {
                foreach (var (key, segId, offsets) in r.EnumerateEntries())
                {
                    if (!liveSegments.Contains(segId)) { dropped++; continue; }
                    w.AddRaw(key, segId, offsets);
                    copied++;
                }
            }

            if (copied == 0)
            {
                _logger.LogInformation(
                    "Trace index merge produced nothing: all {Dropped} entries belonged to segments "
                  + "that are gone", dropped);
                return null;
            }

            // The union of the inputs' coverage, minus segments that have since left the catalog.
            var covered = new HashSet<ulong>();
            foreach (var run in batch)
                foreach (ulong sid in run.CoveredSegments)
                    if (liveSegments.Contains(sid)) covered.Add(sid);

            var merged = w.Write(path, nextLevel, [.. covered]);
            _logger.LogInformation(
                "Trace index merged {Inputs} run(s) into L{Level}: {Copied} entries kept, {Dropped} "
              + "dropped as dead, {Segments} segment(s) covered",
                batch.Count, nextLevel, copied, dropped, covered.Count);
            return merged;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Trace index merge failed — the existing runs are untouched");
            try { if (File.Exists(path)) File.Delete(path); } catch { /* best effort */ }
            return null;
        }
        finally
        {
            foreach (var r in readers) r.Dispose();
        }
    }
}
