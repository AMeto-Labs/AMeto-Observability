using System.Buffers;
using Ameto.Core;
using Ameto.Indexing;
using Ameto.Query.Filtering;
using Ameto.Storage;
using MessagePack;

namespace Ameto.Query.Tests;

/// <summary>
/// What one index group's index says about a filter (<see cref="QueryExecutor.Narrow"/>), asked
/// by the posting lists' COUNTS before any list is built.
///
/// <para>The narrowing used to build every list it had a hint for and then look at what it had:
/// a list as long as the group was recognised as "every row" after it had been decoded and
/// copied, and a list that named half a group was built in every group of every segment a
/// prefilter batch held, for a page that read fifty rows of the newest one. The lists that cost
/// most were the ones that said least.</para>
///
/// <para>Each rule below is pinned over an index the builder really wrote, through a wrapper
/// that notes what the narrowing asked it to COUNT and what to LIST — so a rule that stops being
/// applied shows as a list that was asked for, and not only as a verdict that happens to come
/// out the same. The rows a filter returns are another suite's business
/// (<c>LateNamingTests</c>, through the executor and over real segments).</para>
/// </summary>
public sealed class NarrowByCountTests
{
    private const string Level = ClefFields.Level;

    // ── A list that names nearly all of its group is not built ───────────────

    /// <summary>
    /// 124 rows of 128 carry <c>Shard = 'b'</c>: more than fifteen in sixteen. The list would
    /// name next to everything, so it is counted and left unbuilt, and the group contributes
    /// every row — the four that do not match are the scan's to reject. This was the exception
    /// marker's rule alone; an ordinary property got its list built, copied and handed over.
    /// </summary>
    [Fact]
    public void An_equality_that_names_nearly_all_of_its_group_is_counted_and_never_listed()
    {
        using var group = Group(128, shard: static row => row is >= 8 and < 12 ? "a" : "b");
        var asked = new RecordingIndex(group);

        Assert.Equal(QueryExecutor.GroupVerdict.EveryRow, Narrow(asked, "Shard = 'b'", 128, out var rows));
        Assert.True(rows.IsEmpty);
        Assert.Equal(["Shard"], asked.Counted);
        Assert.Empty(asked.Listed);

        // The other value of the same property is four rows: a list worth having, and short.
        asked = new RecordingIndex(group);
        Assert.Equal(QueryExecutor.GroupVerdict.Named, Narrow(asked, "Shard = 'a'", 128, out rows));
        Assert.Equal(new uint[] { 8, 9, 10, 11 }, rows.ToArray());
        Assert.Equal(["Shard"], asked.Listed);

        // One row short of the line — 119 of 128, where 120 is fifteen in sixteen — is named.
        using var sparser = Group(128, shard: static row => row < 9 ? "a" : "b");
        asked = new RecordingIndex(sparser);
        Assert.Equal(QueryExecutor.GroupVerdict.Named, Narrow(asked, "Shard = 'b'", 128, out rows));
        Assert.Equal(119, rows.Length);

        // And a group whose size is not known has no "nearly all" to be: every list is named.
        asked = new RecordingIndex(group);
        Assert.Equal(QueryExecutor.GroupVerdict.Named, Narrow(asked, "Shard = 'b'", 0, out rows));
        Assert.Equal(124, rows.Length);
    }

    /// <summary>
    /// Beside a hint that does narrow, the one that names nearly the whole group is left out of
    /// the intersection and the other narrows alone. What comes back is a superset of the
    /// answer by the rows the long list would have removed — here none — and the list the index
    /// was asked for is the short one's, by itself.
    /// </summary>
    [Fact]
    public void Beside_a_hint_that_narrows_the_one_that_names_the_group_is_left_out()
    {
        using var group = Group(128,
            shard:  static row => row is >= 8 and < 12 ? "a" : "b",
            tenant: static row => row % 32 == 9 ? "acme" : "other");
        var asked = new RecordingIndex(group);

        Assert.Equal(QueryExecutor.GroupVerdict.Named, Narrow(asked, "Shard = 'b' and Tenant = 'acme'", 128, out var rows));
        Assert.Equal(new uint[] { 9, 41, 73, 105 }, rows.ToArray());        // row 9 is Shard 'a': the scan rejects it
        Assert.Equal(["Shard", "Tenant"], asked.Counted);
        Assert.Equal(["Tenant"], asked.Listed);

        // In the other order the same hint is left out: it is known by its position, not by being first.
        asked = new RecordingIndex(group);
        Assert.Equal(QueryExecutor.GroupVerdict.Named, Narrow(asked, "Tenant = 'acme' and Shard = 'b'", 128, out rows));
        Assert.Equal(new uint[] { 9, 41, 73, 105 }, rows.ToArray());
        Assert.Equal(["Tenant"], asked.Listed);

        // Two hints that both name the group: nothing is listed at all, and the group is whole.
        asked = new RecordingIndex(group);
        Assert.Equal(QueryExecutor.GroupVerdict.EveryRow, Narrow(asked, "Shard = 'b' and @l = 'Error'", 128, out rows));
        Assert.Equal(["Shard", Level], asked.Counted);
        Assert.Empty(asked.Listed);
    }

    /// <summary>
    /// <c>@l = 'Error'</c> typed into the filter, over the Error level of a level-split store:
    /// the list is the whole group. It used to be decoded and copied in order to be compared with
    /// the group's size; it is counted.
    /// </summary>
    [Fact]
    public void The_level_every_row_has_is_counted_and_never_listed()
    {
        using var group = Group(64, shard: static _ => "b");
        var asked = new RecordingIndex(group);

        Assert.Equal(QueryExecutor.GroupVerdict.EveryRow, Narrow(asked, "@l = 'Error'", 64, out _));
        Assert.Equal([Level], asked.Counted);
        Assert.Empty(asked.Listed);
    }

    /// <summary>
    /// A hint whose list holds no row drops the group by the count alone — nothing is listed,
    /// and what the other hints would have named is never asked for.
    /// </summary>
    [Fact]
    public void A_hint_that_lists_no_row_drops_the_group_before_anything_is_listed()
    {
        using var group = Group(128, shard: static row => row % 2 == 0 ? "a" : "b");
        var asked = new RecordingIndex(group);

        Assert.Equal(QueryExecutor.GroupVerdict.Dropped, Narrow(asked, "Shard = 'a' and Shard = 'nobody'", 128, out _));
        Assert.Empty(asked.Listed);

        // A property the index has never seen is not that: it has no bucket to be empty, so it
        // says nothing, and the group is narrowed by the hint that does have one.
        asked = new RecordingIndex(group);
        Assert.Equal(QueryExecutor.GroupVerdict.Named, Narrow(asked, "Shard = 'a' and Region = 'eu'", 128, out var rows));
        Assert.Equal(64, rows.Length);
    }

    // ── The level set ────────────────────────────────────────────────────────

    /// <summary>
    /// The levels PARAMETER — the UI's level menu — is a union of the levels' lists, and over a
    /// level-split store that union is all of a group or none of it. Both are read off the
    /// counts: an event has one level, so the lists are disjoint and their counts add up to the
    /// rows the union names. The union used to be built, a list per level, to be measured.
    /// </summary>
    [Fact]
    public void The_level_set_is_counted_before_any_level_is_listed()
    {
        using var errors = Group(64, shard: static _ => "b");

        var asked = new RecordingIndex(errors);
        Assert.Equal(QueryExecutor.GroupVerdict.EveryRow, Narrow(asked, null, 64, out _, levels: [LogLevel.Error]));
        Assert.Equal([Level], asked.Counted);
        Assert.Empty(asked.Listed);

        asked = new RecordingIndex(errors);
        Assert.Equal(QueryExecutor.GroupVerdict.EveryRow,
            Narrow(asked, null, 64, out _, levels: [LogLevel.Warning, LogLevel.Error, LogLevel.Fatal]));
        Assert.Empty(asked.Listed);

        // None of the allowed levels is in the group: dropped, and nothing was listed to learn it.
        asked = new RecordingIndex(errors);
        Assert.Equal(QueryExecutor.GroupVerdict.Dropped,
            Narrow(asked, null, 64, out _, levels: [LogLevel.Warning, LogLevel.Information]));
        Assert.Empty(asked.Listed);

        // A group that mixes levels is where the union is a real narrowing, and is built.
        using var mixed = Group(64, shard: static _ => "b", level: static row => row % 4 == 0 ? LogLevel.Warning : LogLevel.Error);
        asked = new RecordingIndex(mixed);
        Assert.Equal(QueryExecutor.GroupVerdict.Named, Narrow(asked, null, 64, out var rows, levels: [LogLevel.Warning]));
        Assert.Equal(16, rows.Length);
        Assert.Equal([Level], asked.Listed);

        // Nearly all of it — 62 rows of 64 — is the group, like any other list that long.
        using var nearly = Group(64, shard: static _ => "b", level: static row => row is 5 or 50 ? LogLevel.Warning : LogLevel.Error);
        asked = new RecordingIndex(nearly);
        Assert.Equal(QueryExecutor.GroupVerdict.EveryRow, Narrow(asked, null, 64, out _, levels: [LogLevel.Error]));
        Assert.Empty(asked.Listed);
    }

    /// <summary>
    /// Beside candidates another hint named, a level set that is the whole group changes
    /// nothing and is not built; one that is part of the group is intersected in.
    /// </summary>
    [Fact]
    public void A_level_set_that_is_the_group_leaves_the_other_hints_candidates_as_they_are()
    {
        using var mixed = Group(64,
            shard: static row => row < 6 ? "a" : "b",
            level: static row => row % 2 == 0 ? LogLevel.Warning : LogLevel.Error);

        var asked = new RecordingIndex(mixed);
        Assert.Equal(QueryExecutor.GroupVerdict.Named,
            Narrow(asked, "Shard = 'a'", 64, out var rows, levels: [LogLevel.Warning, LogLevel.Error]));
        Assert.Equal(new uint[] { 0, 1, 2, 3, 4, 5 }, rows.ToArray());
        Assert.Equal(["Shard"], asked.Listed);

        asked = new RecordingIndex(mixed);
        Assert.Equal(QueryExecutor.GroupVerdict.Named, Narrow(asked, "Shard = 'a'", 64, out rows, levels: [LogLevel.Error]));
        Assert.Equal(new uint[] { 1, 3, 5 }, rows.ToArray());
    }

    // ── A long list waits for the scan ───────────────────────────────────────

    /// <summary>
    /// The prefilter names rows only where the list is short. A longer one is left unbuilt and
    /// the group is reported as such, to be asked again — with no limit — if the scan gets to it.
    /// The line is the index's own between a list its counting walk decodes and one it only
    /// counts, so "short" is a list that costs nothing more to name.
    /// </summary>
    [Fact]
    public void A_list_longer_than_the_limit_is_left_for_later_and_named_when_asked_without_one()
    {
        int limit = QueryExecutor.NameUpFront;

        // Rows 0..limit-1 are 'a', the next limit+1 are 'c', the 2·limit after them 'b'; and
        // every three-hundredth row from the eighth is Tenant 'acme'.
        uint total = (uint)(4 * limit + 1);
        using var group = Group((int)total,
            shard:  row => row < limit ? "a" : row < 2 * limit + 1 ? "c" : "b",
            tenant: static row => row % 300 == 7 ? "acme" : "other");

        // Exactly at the limit: named now.
        var asked = new RecordingIndex(group);
        Assert.Equal(QueryExecutor.GroupVerdict.Named, Narrow(asked, "Shard = 'a'", total, out var rows, limit));
        Assert.Equal(limit, rows.Length);

        // One row past it: not now, and nothing was listed to decide that.
        asked = new RecordingIndex(group);
        Assert.Equal(QueryExecutor.GroupVerdict.Deferred, Narrow(asked, "Shard = 'c'", total, out rows, limit));
        Assert.True(rows.IsEmpty);
        Assert.Equal(["Shard"], asked.Counted);
        Assert.Empty(asked.Listed);

        // Asked again as the scan asks: the same rows the list always gave.
        asked = new RecordingIndex(group);
        Assert.Equal(QueryExecutor.GroupVerdict.Named, Narrow(asked, "Shard = 'c'", total, out rows));
        Assert.Equal([.. Enumerable.Range(limit, limit + 1).Select(static row => (uint)row)], rows.ToArray());

        // One long list among the hints is enough to wait: the short one beside it is not
        // listed by itself in the meantime. Asked without the limit, they are intersected.
        asked = new RecordingIndex(group);
        Assert.Equal(QueryExecutor.GroupVerdict.Deferred, Narrow(asked, "Shard = 'c' and Tenant = 'acme'", total, out _, limit));
        Assert.Equal(["Shard", "Tenant"], asked.Counted);
        Assert.Empty(asked.Listed);

        asked = new RecordingIndex(group);
        Assert.Equal(QueryExecutor.GroupVerdict.Named, Narrow(asked, "Shard = 'c' and Tenant = 'acme'", total, out rows));
        Assert.Equal(new uint[] { 307 }, rows.ToArray());

        // Two long lists that share no row: left for later like any other, and found empty
        // only when they are built. "Left for later" settles nothing about the group.
        asked = new RecordingIndex(group);
        Assert.Equal(QueryExecutor.GroupVerdict.Deferred, Narrow(asked, "Shard = 'c' and Shard = 'b'", total, out _, limit));
        Assert.Equal(QueryExecutor.GroupVerdict.Dropped,  Narrow(asked, "Shard = 'c' and Shard = 'b'", total, out _));

        // The level set waits the same way when its union has to be built.
        using var mixed = Group(4 * limit, shard: static _ => "b", level: static row => row % 2 == 0 ? LogLevel.Warning : LogLevel.Error);
        asked = new RecordingIndex(mixed);
        Assert.Equal(QueryExecutor.GroupVerdict.Deferred,
            Narrow(asked, null, (uint)(4 * limit), out _, limit, levels: [LogLevel.Error]));
        Assert.Empty(asked.Listed);
    }

    /// <summary>
    /// What the counts decide, they decide however little may be listed: a group is dropped, or
    /// carried whole, by a prefilter that may build nothing at all. Only naming waits.
    /// </summary>
    [Fact]
    public void What_the_counts_decide_does_not_wait()
    {
        using var group = Group(1024, shard: static row => row % 64 == 0 ? "a" : "b");

        Assert.Equal(QueryExecutor.GroupVerdict.Dropped,  Narrow(group, "Shard = 'nobody'", 1024, out _, limit: 0));
        Assert.Equal(QueryExecutor.GroupVerdict.EveryRow, Narrow(group, "Shard = 'b'",      1024, out _, limit: 0));
        Assert.Equal(QueryExecutor.GroupVerdict.EveryRow, Narrow(group, "@l = 'Error'",     1024, out _, limit: 0));
        Assert.Equal(QueryExecutor.GroupVerdict.Dropped,  Narrow(group, null, 1024, out _, limit: 0, levels: [LogLevel.Warning]));
        Assert.Equal(QueryExecutor.GroupVerdict.EveryRow, Narrow(group, null, 1024, out _, limit: 0, levels: [LogLevel.Error]));
        Assert.Equal(QueryExecutor.GroupVerdict.Deferred, Narrow(group, "Shard = 'a'",      1024, out _, limit: 0));

        // A property the index does not know, asked for a value it holds under another: the
        // index has nothing to say, and says so at once too.
        Assert.Equal(QueryExecutor.GroupVerdict.NoInformation, Narrow(group, "Region = 'b'", 1024, out _, limit: 0));
    }

    /// <summary>
    /// A filter with a substring predicate is narrowed whole, whatever the limit: a trigram
    /// list has no count to ask for, and a group it proves empty has to be dropped by the
    /// prefilter, in its batch — not kept for a scan that would then walk such groups one
    /// segment at a time to find nothing in each.
    /// </summary>
    [Fact]
    public void A_filter_with_a_substring_predicate_is_never_left_for_later()
    {
        using var group = Group(1024, shard: static row => row % 2 == 0 ? "a" : "b");

        Assert.Equal(QueryExecutor.GroupVerdict.Named,
            Narrow(group, "Shard = 'a' and @mt like '%settle%'", 1024, out var rows, limit: 0));
        Assert.Equal(512, rows.Length);

        Assert.Equal(QueryExecutor.GroupVerdict.Dropped,
            Narrow(group, "Shard = 'a' and @mt like '%refund%'", 1024, out _, limit: 0));
    }

    // ── The answer is lent, not copied ───────────────────────────────────────

    /// <summary>
    /// For a single equality the rows named ARE the posting list as the index keeps it: the
    /// same memory every time it is asked, with no copy in between. That copy was four bytes for
    /// every row a query named, in every group it named rows in, and existed to keep a caller
    /// from writing to the index's list — which read-only memory does by type.
    /// </summary>
    [Fact]
    public void The_rows_a_single_equality_names_are_the_index_s_own_list()
    {
        using var group = Group(1024, shard: static row => row % 2 == 0 ? "a" : "b");
        var filter = CompiledFilter.Compile("Shard = 'a'");

        Assert.Equal(QueryExecutor.GroupVerdict.Named, QueryExecutor.Narrow(filter, group, null, 1024, int.MaxValue, out var first));
        Assert.Equal(QueryExecutor.GroupVerdict.Named, QueryExecutor.Narrow(filter, group, null, 1024, int.MaxValue, out var again));

        Assert.Equal(512, first.Length);
        Assert.True(first.Span == again.Span, "the list was copied on its way out of the index");

        // The array-returning lookup still hands every caller an array of its own.
        uint[] mine = group.LookupIntersect([("Shard", "a")])!;
        Assert.False((ReadOnlySpan<uint>)mine == first.Span);
        Assert.Equal(mine, first.ToArray());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// The production narrowing of one group for <paramref name="filter"/> and a level set, with
    /// at most <paramref name="limit"/> rows to a list it may build (none by default, as the
    /// scan asks).
    /// </summary>
    private static QueryExecutor.GroupVerdict Narrow(
        ISegmentIndex index, string? filter, uint groupRows, out ReadOnlyMemory<uint> rows,
        int limit = int.MaxValue, LogLevel[]? levels = null)
    {
        (string, object?)[][]? levelHints = null;
        if (levels is { Length: > 0 })
        {
            levelHints = new (string, object?)[levels.Length][];
            for (int i = 0; i < levels.Length; i++) levelHints[i] = [(Level, levels[i].ToSeqString())];
        }
        return QueryExecutor.Narrow(CompiledFilter.Compile(filter), index, levelHints, groupRows, limit, out rows);
    }

    /// <summary>
    /// One index group of <paramref name="rows"/> rows as a query reads it — the packed reader
    /// over what the production builder wrote. Each row carries <c>Shard</c>, and
    /// <c>Tenant</c> when <paramref name="tenant"/> is given; its level is Error unless
    /// <paramref name="level"/> says otherwise.
    /// </summary>
    private static SegmentIndexReader Group(
        int rows, Func<int, string> shard, Func<int, string>? tenant = null, Func<int, LogLevel>? level = null)
    {
        long ticks = new DateTimeOffset(2026, 11, 16, 9, 0, 0, TimeSpan.Zero).UtcTicks;
        var  buf   = new ArrayBufferWriter<byte>(64);

        using var builder = new SegmentIndexBuilder(rows);
        for (int i = 0; i < rows; i++)
        {
            buf.ResetWrittenCount();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(tenant is null ? 1 : 2);
            w.Write("Shard"); w.Write(shard(i));
            if (tenant is not null) { w.Write("Tenant"); w.Write(tenant(i)); }
            w.Flush();

            var ev = new SegmentEventRef(new EventId(0u, (uint)i).RawValue, ticks + i, level?.Invoke(i) ?? LogLevel.Error,
                                         0, 0, 0, "settle {OrderId}", "Billing.API", buf.WrittenSpan, []);
            builder.Add((uint)i, in ev);
        }

        var (inverted, trigram, bloom) = builder.Serialise();
        return SegmentIndexReader.Load(inverted, trigram, bloom);
    }

    /// <summary>An index that notes which keys it was asked to count and which lists to build.</summary>
    private sealed class RecordingIndex(ISegmentIndex inner) : ISegmentIndex
    {
        public List<string> Counted { get; } = [];
        public List<string> Listed  { get; } = [];

        public uint[]? Lookup(string propertyName, object? value)
        {
            Listed.Add(propertyName);
            return inner.Lookup(propertyName, value);
        }

        public bool MightContain(string propertyName, object? value) => inner.MightContain(propertyName, value);

        public uint[]? LookupTrigram(ReadOnlySpan<char> text) => inner.LookupTrigram(text);

        public uint[]? LookupIntersect(IReadOnlyList<(string property, object? value)> predicates)
        {
            Listed.Add(string.Join(" + ", predicates.Select(static p => p.property)));
            return inner.LookupIntersect(predicates);
        }

        public bool TryLookupIntersect(
            IReadOnlyList<(string property, object? value)> predicates, out ReadOnlyMemory<uint> offsets)
        {
            Listed.Add(string.Join(" + ", predicates.Select(static p => p.property)));
            return inner.TryLookupIntersect(predicates, out offsets);
        }

        public int? CountAtMost(string propertyName, object? value)
        {
            Counted.Add(propertyName);
            return inner.CountAtMost(propertyName, value);
        }
    }
}
