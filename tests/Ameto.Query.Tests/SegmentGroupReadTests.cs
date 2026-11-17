using Ameto.Core;
using Ameto.Storage;

namespace Ameto.Query.Tests;

/// <summary>
/// <see cref="SegmentReader.ReadGroupsAsync"/>: a segment read one index group at a time, each
/// group by the answer its plan gives when the read gets there — none of it, all of it, or the
/// rows named.
///
/// <para>The query's scan reads every prefiltered segment this way, so what is pinned here is
/// what a query's rows rest on: walking the groups is walking the file (the same rows, in the
/// same order, as <see cref="SegmentReader.ReadEventsAsync"/> gives), each group gets its own
/// answer and no other's, and a group is asked about only when the read reaches it — which is
/// what lets a caller leave an answer unbuilt for the groups a page never gets to. Over the
/// multi-group segment the other suites of this fixture assert on, with plans written by hand:
/// the narrowing that produces a real one is <c>NarrowByCountTests</c>' and
/// <c>LateNamingTests</c>'.</para>
/// </summary>
public sealed class SegmentGroupReadTests : IAsyncLifetime
{
    private const int Events = QuerySegmentFixtures.GroupedEvents;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-groupread-" + Guid.NewGuid().ToString("N"));
    private StorageEngine _engine  = null!;
    private string        _segPath = null!;
    private long          _baseTicks;

    public async Task InitializeAsync() =>
        (_engine, _, _segPath, _baseTicks) = await QuerySegmentFixtures.GroupedSegmentAsync(_dir);

    public async Task DisposeAsync()
    {
        await _engine.DisposeAsync();
        QuerySegmentFixtures.DeleteDataDirectory(_dir);
    }

    /// <summary>A plan given as a function, which notes the groups it was asked about, in order.</summary>
    private sealed class Plan(Func<int, GroupRead> read) : ISegmentGroupPlan
    {
        public List<int> Asked { get; } = [];

        public GroupRead Read(int group)
        {
            Asked.Add(group);
            return read(group);
        }
    }

    /// <summary>The fixture writes event <c>i</c> at second <c>i</c>, so its file ordinal is <c>i</c>.</summary>
    private static int OrdinalOf(LogEvent ev) =>
        int.Parse(QuerySegmentFixtures.OrderIdOf(ev).AsSpan("order-".Length), System.Globalization.CultureInfo.InvariantCulture);

    private static async Task<List<int>> ReadAsync(IAsyncEnumerable<LogEvent> events)
    {
        var ordinals = new List<int>();
        await foreach (var ev in events) ordinals.Add(OrdinalOf(ev));
        return ordinals;
    }

    // ── Walking the groups is walking the file ───────────────────────────────

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EveryGroupReadWhole_IsTheFile_InItsOrder(bool reversed)
    {
        using var reader = SegmentReader.Open(_segPath);
        int groups = reader.Groups.Length;
        Assert.True(groups >= 4, $"fixture produced {groups} group(s)");
        Assert.True(reader.GroupsCoverBlocks);

        var plan   = new Plan(static _ => GroupRead.Whole);
        var byPlan = await ReadAsync(reader.ReadGroupsAsync(plan, null, null, reversed));
        var byFile = await ReadAsync(reader.ReadEventsAsync(null, null, null, reversed));

        Assert.Equal(Events, byFile.Count);
        Assert.Equal(byFile, byPlan);
        Assert.Equal(reversed ? Enumerable.Range(0, groups).Reverse() : Enumerable.Range(0, groups), plan.Asked);
    }

    /// <summary>
    /// Three kinds of group in one read, in turn: skipped, whole, and named by every seventh of
    /// its rows with its first and last. Each group reads what its own answer says, and the rows
    /// come out in file order, or its reverse.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EachGroupReadsWhatItsOwnAnswerSays(bool reversed)
    {
        using var reader = SegmentReader.Open(_segPath);
        var groups = reader.Groups.ToArray();

        var owed = new List<int>();
        var plan = new Plan(g =>
        {
            int first = (int)groups[g].FirstOrdinal, count = (int)groups[g].EventCount;
            switch (g % 3)
            {
                case 0: return GroupRead.Skip;
                case 1: return GroupRead.Whole;
                default:
                    uint[] named = [.. Enumerable.Range(first, count)
                        .Where(o => o == first || o == first + count - 1 || o % 7 == 0).Select(static o => (uint)o)];
                    return GroupRead.Only(named);
            }
        });
        for (int g = 0; g < groups.Length; g++)
        {
            int first = (int)groups[g].FirstOrdinal, count = (int)groups[g].EventCount;
            if (g % 3 == 1) owed.AddRange(Enumerable.Range(first, count));
            if (g % 3 == 2) owed.AddRange(Enumerable.Range(first, count).Where(o => o == first || o == first + count - 1 || o % 7 == 0));
        }
        if (reversed) owed.Reverse();

        Assert.Equal(owed, await ReadAsync(reader.ReadGroupsAsync(plan, null, null, reversed)));
    }

    /// <summary>The window is the reader's to apply, group by group, as it is block by block.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AWindowCutsTheSameRows_WhicheverWayTheFileIsWalked(bool reversed)
    {
        using var reader = SegmentReader.Open(_segPath);
        var from = new DateTimeOffset(_baseTicks + 1_234 * TimeSpan.TicksPerSecond, TimeSpan.Zero);
        var to   = new DateTimeOffset(_baseTicks + 4_321 * TimeSpan.TicksPerSecond, TimeSpan.Zero);

        var byPlan = await ReadAsync(reader.ReadGroupsAsync(new Plan(static _ => GroupRead.Whole), from, to, reversed));
        var byFile = await ReadAsync(reader.ReadEventsAsync(null, from, to, reversed));

        Assert.Equal(4_321 - 1_234 + 1, byFile.Count);
        Assert.Equal(byFile, byPlan);
    }

    // ── A group is asked about when the read reaches it ──────────────────────

    /// <summary>
    /// The reason the plan is an interface and not a list: a read that stops early has asked
    /// only about the groups it got to. One row off the newest end asks about the last group;
    /// one off the oldest, about the first — and a group the plan skips is passed on the way
    /// to the next, having been asked.
    /// </summary>
    [Fact]
    public async Task AReadThatStopsEarly_HasAskedOnlyAboutTheGroupsItReached()
    {
        using var reader = SegmentReader.Open(_segPath);
        int last = reader.Groups.Length - 1;

        var newest = new Plan(static _ => GroupRead.Whole);
        Assert.Equal(Events - 1, await FirstAsync(reader.ReadGroupsAsync(newest, null, null, reversed: true)));
        Assert.Equal([last], newest.Asked);

        var oldest = new Plan(static _ => GroupRead.Whole);
        Assert.Equal(0, await FirstAsync(reader.ReadGroupsAsync(oldest, null, null)));
        Assert.Equal([0], oldest.Asked);

        // The last two groups skipped: the first row comes from the third from the end.
        var skipping = new Plan(g => g >= last - 1 ? GroupRead.Skip : GroupRead.Whole);
        int firstRow = await FirstAsync(reader.ReadGroupsAsync(skipping, null, null, reversed: true));
        Assert.Equal((int)(reader.Groups[last - 1].FirstOrdinal - 1), firstRow);
        Assert.Equal([last, last - 1, last - 2], skipping.Asked);

        static async Task<int> FirstAsync(IAsyncEnumerable<LogEvent> events)
        {
            await foreach (var ev in events) return OrdinalOf(ev);
            throw new InvalidOperationException("the read returned no row");
        }
    }

    // ── The rows named ───────────────────────────────────────────────────────

    /// <summary>
    /// The row walk needs its candidates ascending and the reader makes no caller promise it:
    /// a list out of order is sorted into a copy, and the caller's — which may be an index
    /// cache's own — is left exactly as it was.
    /// </summary>
    [Fact]
    public async Task ALIstOutOfOrder_NamesTheSameRows_AndIsNotTouched()
    {
        using var reader = SegmentReader.Open(_segPath);
        var    group = reader.Groups[1];
        uint[] named = [.. Enumerable.Range((int)group.FirstOrdinal, (int)group.EventCount)
            .Where(static o => o % 5 == 0).Select(static o => (uint)o).Reverse()];
        uint[] asGiven = [.. named];

        var rows = await ReadAsync(reader.ReadGroupsAsync(
            new Plan(g => g == 1 ? GroupRead.Only(named) : GroupRead.Skip), null, null));

        Assert.Equal(asGiven.Reverse().Select(static o => (int)o), rows);
        Assert.Equal(asGiven, named);
    }

    /// <summary>
    /// A group that is named and names no row reads nothing — it does not fall back to
    /// "no list, so all of it", which is what an empty list means to the file-wide read.
    /// </summary>
    [Fact]
    public async Task AGroupNamedByNoRow_ReadsNothing()
    {
        using var reader = SegmentReader.Open(_segPath);

        Assert.Empty(await ReadAsync(reader.ReadGroupsAsync(new Plan(static _ => GroupRead.Only(default)), null, null)));
        Assert.Empty(await ReadAsync(reader.ReadGroupsAsync(new Plan(static _ => GroupRead.Skip), null, null)));
        Assert.True(default(GroupRead).IsSkip);
    }

    // ── The premise ──────────────────────────────────────────────────────────

    /// <summary>
    /// Walking the groups reads the file only where the groups ARE the file: consecutive, from
    /// the first block to the last. The writer lays every file out that way; a directory that
    /// leaves a block out, counts one twice or runs past the end is not read by its groups.
    /// </summary>
    [Fact]
    public void GroupsCoverTheBlocks_OnlyWhenTheyRunFromTheFirstToTheLast_WithoutAGapOrAnOverlap()
    {
        static SegmentIndexGroup G(int firstBlock, int blocks) => new(firstBlock, blocks, 0, 0, 0, 0, 0, 0, 0);

        Assert.True(SegmentReader.CoverBlocks([G(0, 3), G(3, 1), G(4, 6)], 10));
        Assert.True(SegmentReader.CoverBlocks([G(0, 10)], 10));
        Assert.True(SegmentReader.CoverBlocks([G(0, 0)], 0));                    // an empty segment's one entry
        Assert.True(SegmentReader.CoverBlocks([], 0));

        Assert.False(SegmentReader.CoverBlocks([], 10));                         // blocks no group owns
        Assert.False(SegmentReader.CoverBlocks([G(0, 3), G(4, 6)], 10));         // a gap
        Assert.False(SegmentReader.CoverBlocks([G(0, 3), G(2, 8)], 10));         // an overlap
        Assert.False(SegmentReader.CoverBlocks([G(1, 9)], 10));                  // does not start at the first
        Assert.False(SegmentReader.CoverBlocks([G(0, 3), G(3, 6)], 10));         // stops short
        Assert.False(SegmentReader.CoverBlocks([G(0, 3), G(3, 8)], 10));         // runs past the end
        Assert.False(SegmentReader.CoverBlocks([G(0, -1), G(-1, 11)], 10));      // a torn count
    }
}
