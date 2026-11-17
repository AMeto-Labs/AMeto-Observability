using System.Text;
using Ameto.Core;
using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

/// <summary>
/// The nesting limit bounds what the parser recurses on. A chain — <c>a and b and c …</c> —
/// is read by a loop and is not nesting, yet the parser builds it left-deep, a level per
/// clause, and everything that walks the tree afterwards recursed down it: the hint builders,
/// the header predicate, the evaluator on every event. Measured before
/// <see cref="ChainBalancer"/>: about 650 bytes of stack per clause in a Debug build and 490
/// in a Release one before the JIT has optimised the walkers, so a chain of 2 315 clauses
/// (3 063 in Release) overflowed a 1.5 MB stack — and a request line carries 2 720 of the
/// densest, which a Debug process did not survive.
///
/// <para>A compiled filter now holds a long chain balanced. These pin the two halves of that:
/// a chain of any length compiles and evaluates, and the balanced tree is the same filter in
/// every way a caller can observe.</para>
/// </summary>
public sealed class ChainBalancerTests
{
    private static LogEvent Event(LogLevel level, params (string Key, object Value)[] props)
    {
        var map = new Dictionary<string, object?>(props.Length);
        foreach (var (k, v) in props) map[k] = v;
        return new LogEvent
        {
            Id              = new EventId(0u, 1u),
            Timestamp       = new DateTimeOffset(2026, 8, 23, 12, 0, 0, TimeSpan.Zero),
            Level           = level,
            MessageTemplate = "something happened",
            Properties      = map,
        };
    }

    private static string Chain(string op, int n, Func<int, string> clause)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < n; i++)
        {
            if (i > 0) sb.Append(' ').Append(op).Append(' ');
            sb.Append(clause(i));
        }
        return sb.ToString();
    }

    private static int Depth(FilterNode node) => node switch
    {
        AndNode a => 1 + Math.Max(Depth(a.Left), Depth(a.Right)),
        OrNode o  => 1 + Math.Max(Depth(o.Left), Depth(o.Right)),
        NotNode n => 1 + Depth(n.Operand),
        _         => 0,
    };

    /// <summary>The leaves of a tree in reading order — what every walker visits, in the order it visits them.</summary>
    private static List<FilterNode> Leaves(FilterNode node)
    {
        var leaves = new List<FilterNode>();
        var stack  = new Stack<FilterNode>();
        stack.Push(node);
        while (stack.Count > 0)
        {
            switch (stack.Pop())
            {
                case AndNode a: stack.Push(a.Right); stack.Push(a.Left); break;
                case OrNode o:  stack.Push(o.Right); stack.Push(o.Left); break;
                case NotNode n: stack.Push(n.Operand); break;
                case var leaf:  leaves.Add(leaf); break;
            }
        }
        return leaves;
    }

    // ── A chain of any length ─────────────────────────────────────────────────

    [Fact]
    public void The_longest_chain_a_request_line_can_carry_compiles_and_evaluates()
    {
        // Kestrel refuses a request line over 8 192 bytes, so that is what the search and tail
        // endpoints can be handed. The densest chain that fits is a lone `-` per clause — the
        // lexer reads it as a number, the parser as a word to search for — three characters
        // each: `-or-or-…`, 2 720 of them. Left-deep, a Debug process followed about 2 450 and
        // then ended; a Release one about 3 400. Without the balancer this test does not fail,
        // it takes the test host with it.
        const int clauses = 2_720;
        string filter = string.Join("or", Enumerable.Repeat("-", clauses));
        Assert.Equal(8_192 - "GET /api/events?filter= HTTP/1.1\r\n".Length, filter.Length);

        Assert.Equal(12, Depth(ChainBalancer.Balance(FilterParser.Parse(filter))));     // ⌈log₂ 2 720⌉

        var f = CompiledFilter.Compile(filter);

        Assert.True(f.Matches(Event(LogLevel.Error, ("Id", "a-b"))));
        Assert.False(f.Matches(Event(LogLevel.Error, ("Id", "ab"))));
        Assert.False(f.TryGetHeaderOnlyShape(out _, out _));
        Assert.Null(f.DerivedLevels);
    }

    [Theory]
    [InlineData("and")]
    [InlineData("or")]
    public void A_chain_far_longer_than_any_stack_could_follow_compiles_and_evaluates(string op)
    {
        // A hundred thousand clauses, a megabyte of filter: what the body of an alert rule may
        // carry, since nothing bounds its length short of the 30 MB a request body may be.
        // Left-deep this is a hundred thousand frames in every walker; balanced it is seventeen.
        const int n = 100_000;
        string filter = Chain(op, n, i => $"P{i % 7} = {i % 7}");

        var f = CompiledFilter.Compile(filter);

        var all  = Event(LogLevel.Error, ("P0", 0L), ("P1", 1L), ("P2", 2L), ("P3", 3L), ("P4", 4L), ("P5", 5L), ("P6", 6L));
        var one  = Event(LogLevel.Error, ("P3", 3L));
        var none = Event(LogLevel.Error, ("P3", 9L));

        Assert.True(f.Matches(all));
        Assert.Equal(op == "or", f.Matches(one));
        Assert.False(f.Matches(none));

        // Every clause of an AND-chain is an index hint; an OR-chain offers none.
        Assert.Equal(op == "and" ? n : 0, f.GetInvertedHints().Count);
    }

    [Fact]
    public void A_long_chain_inside_a_group_inside_a_not_is_balanced_where_it_stands()
    {
        string inner  = Chain("or", 5_000, i => $"A = {i}");
        string filter = $"@l = 'Error' and not ({inner}) and B = 1";

        // Two levels of `and`, the `not`, and ⌈log₂ 5 000⌉ = 13 under it — where the parser's
        // tree has 4 999. Asserted on the tree, because on a stack large enough to follow the
        // unbalanced chain the matches below would come out right either way.
        Assert.Equal(2 + 1 + 13, Depth(ChainBalancer.Balance(FilterParser.Parse(filter))));

        var f = CompiledFilter.Compile(filter);

        Assert.True(f.Matches(Event(LogLevel.Error, ("A", 5_000L), ("B", 1L))));
        Assert.False(f.Matches(Event(LogLevel.Error, ("A", 4_999L), ("B", 1L))));
        Assert.False(f.Matches(Event(LogLevel.Debug, ("A", 5_000L), ("B", 1L))));
    }

    [Fact]
    public void A_long_chain_that_ends_in_a_group_of_the_same_operator_keeps_every_clause_in_order()
    {
        // `p and p and … and (q and q and …)`: the group on the right is one clause of the outer
        // chain and a long chain of its own. Both are balanced, and the reading order — which
        // is the order of the hints, and of evaluation — is the order they were written in.
        string filter = Chain("and", 12, i => $"P{i} = 'p{i}'") + " and (" + Chain("and", 12, i => $"Q{i} = 'q{i}'") + ")";

        var parsed   = FilterParser.Parse(filter);
        var balanced = ChainBalancer.Balance(parsed);

        var before = Leaves(parsed);
        var after  = Leaves(balanced);
        Assert.Equal(24, after.Count);
        for (int i = 0; i < 24; i++) Assert.Same(before[i], after[i]);
        Assert.Equal(3 + 4, Depth(balanced));       // the group is the last of 13 clauses, three levels down; four more inside

        var hints = CompiledFilter.Compile(filter).GetInvertedHints();
        Assert.Equal(24, hints.Count);
        for (int i = 0; i < 12; i++)
        {
            Assert.Equal(($"P{i}", (object?)$"p{i}"), hints[i]);
            Assert.Equal(($"Q{i}", (object?)$"q{i}"), hints[12 + i]);
        }
    }

    // ── The balanced tree is the same filter ──────────────────────────────────

    [Fact]
    public void A_chain_no_longer_than_the_threshold_is_the_very_tree_the_parser_built()
    {
        for (int n = 1; n <= ChainBalancer.LongChain; n++)
        {
            foreach (string op in new[] { "and", "or" })
            {
                var parsed = FilterParser.Parse(Chain(op, n, i => $"P{i} = {i}"));
                Assert.Same(parsed, ChainBalancer.Balance(parsed));
            }
        }

        // …including when it is nested and negated.
        var nested = FilterParser.Parse("not (A = 1 or B = 2) and (C = 3 or not D = 4) and E = 5");
        Assert.Same(nested, ChainBalancer.Balance(nested));
    }

    [Fact]
    public void A_filter_with_no_long_chain_costs_the_balancer_no_allocation()
    {
        // Compiling is not per event, but it is per query and per live-tail connection, and
        // the filters that come through it are almost all of this kind.
        var root = FilterParser.Parse(
            "@l = 'Error' and (A = 1 or B = 2 or not C = 3) and has(D) and E like 'x%' and not (F = 4 and G = 5)");

        for (int i = 0; i < 100; i++) ChainBalancer.Balance(root);

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++) ChainBalancer.Balance(root);
        long bytes = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Same(root, ChainBalancer.Balance(root));
        Assert.True(bytes == 0, $"expected zero allocation over 1000 passes, saw {bytes} B");
    }

    [Fact]
    public void A_longer_chain_keeps_its_clauses_in_order_and_loses_its_depth()
    {
        foreach (string op in new[] { "and", "or" })
        {
            for (int n = ChainBalancer.LongChain + 1; n <= 200; n++)
            {
                var parsed   = FilterParser.Parse(Chain(op, n, i => $"P{i} = {i}"));
                var balanced = ChainBalancer.Balance(parsed);

                // The same node objects, left to right: nothing copied, dropped or reordered.
                var before = Leaves(parsed);
                var after  = Leaves(balanced);
                Assert.Equal(n, after.Count);
                for (int i = 0; i < n; i++) Assert.Same(before[i], after[i]);

                // n − 1 levels became ⌈log₂ n⌉.
                int ceilLog2 = 0;
                for (int reach = 1; reach < n; reach <<= 1) ceilLog2++;
                Assert.Equal(n - 1, Depth(parsed));
                Assert.Equal(ceilLog2, Depth(balanced));

                // Still one operator all the way down.
                Assert.All(Inner(balanced), node => Assert.Equal(op == "and", node is AndNode));
            }
        }
    }

    private static IEnumerable<FilterNode> Inner(FilterNode node)
    {
        var stack = new Stack<FilterNode>();
        stack.Push(node);
        while (stack.Count > 0)
        {
            var n = stack.Pop();
            if (n is AndNode a)     { yield return n; stack.Push(a.Left); stack.Push(a.Right); }
            else if (n is OrNode o) { yield return n; stack.Push(o.Left); stack.Push(o.Right); }
        }
    }

    [Fact]
    public void The_parser_still_answers_with_the_left_deep_tree()
    {
        // Balancing belongs to CompiledFilter. A caller of the parser may be comparing the
        // shape of two parses, so the parser's own answer does not change.
        var root = FilterParser.Parse(Chain("and", 40, i => $"P{i} = {i}"));

        int depth = 0;
        for (FilterNode at = root; at is AndNode and; at = and.Left)
        {
            Assert.IsType<CompareNode>(and.Right);
            depth++;
        }
        Assert.Equal(39, depth);
    }

    [Theory]
    [InlineData("and")]
    [InlineData("or")]
    public void A_balanced_chain_matches_exactly_the_events_the_left_deep_one_does(string op)
    {
        // The parser's own tree, evaluated as it stands, is the reference: FilterEvaluator
        // takes any tree. Every chain length on both sides of the threshold, and for each one
        // the events that single out each clause in turn.
        for (int n = 2; n <= 40; n++)
        {
            string filter   = Chain(op, n, i => $"P{i} = {i}");
            var    leftDeep = FilterParser.Parse(filter);
            var    compiled = CompiledFilter.Compile(filter);

            for (int odd = -1; odd < n; odd++)
            {
                // For `and`: every clause true except clause `odd`. For `or`: only it true.
                var props = new (string, object)[n];
                for (int i = 0; i < n; i++)
                {
                    bool holds = op == "and" ? i != odd : i == odd;
                    props[i] = ($"P{i}", holds ? (long)i : -1L - i);
                }
                var ev = Event(LogLevel.Error, props);

                bool expected = op == "and" ? odd < 0 : odd >= 0;
                Assert.Equal(expected, FilterEvaluator.Matches(leftDeep, ev));
                Assert.Equal(expected, compiled.Matches(ev));
            }
        }
    }

    [Fact]
    public void Chains_of_both_operators_nested_in_each_other_match_the_same_events()
    {
        // (P0 = 0 and P1 = 1 and … 12) or (P12 = 12 and … ) or … — twelve groups of twelve,
        // both layers past the threshold, plus a negated chain.
        var groups = new List<string>();
        for (int g = 0; g < 12; g++)
            groups.Add("(" + Chain("and", 12, i => $"P{g * 12 + i} = {g * 12 + i}") + ")");
        string filter = string.Join(" or ", groups) + " or not (" + Chain("or", 12, i => $"N{i} = {i}") + ")";

        var leftDeep = FilterParser.Parse(filter);
        var compiled = CompiledFilter.Compile(filter);

        var rng = new Random(20261116);
        for (int round = 0; round < 400; round++)
        {
            var props = new List<(string, object)>();
            // One group fully satisfied in a third of the rounds, nearly satisfied in the rest.
            int g    = rng.Next(12);
            int miss = rng.Next(3) == 0 ? -1 : rng.Next(12);
            for (int i = 0; i < 12; i++)
                if (i != miss) props.Add(($"P{g * 12 + i}", (long)(g * 12 + i)));
            // A stray true clause from another group.
            int other = rng.Next(144);
            if (other / 12 != g) props.Add(($"P{other}", (long)other));
            // The negated chain: true for the event when none of its clauses holds.
            if (rng.Next(2) == 0) { int k = rng.Next(12); props.Add(($"N{k}", (long)k)); }

            var ev = Event(LogLevel.Error, props.ToArray());
            Assert.Equal(FilterEvaluator.Matches(leftDeep, ev), compiled.Matches(ev));
        }
    }

    // ── …and every walker reads it the same ───────────────────────────────────

    [Fact]
    public void Index_hints_come_out_in_the_order_the_clauses_were_written()
    {
        const int n = 60;
        var f = CompiledFilter.Compile(Chain("and", n, i => $"P{i} = 'v{i}'"));

        var hints = f.GetInvertedHints();
        Assert.Equal(n, hints.Count);
        for (int i = 0; i < n; i++)
            Assert.Equal(($"P{i}", (object?)$"v{i}"), hints[i]);

        // The bloom gate takes the FIRST equality of the chain.
        Assert.True(f.TryGetIndexHint(out string property, out object? value));
        Assert.Equal("P0", property);
        Assert.Equal("v0", value);
    }

    [Fact]
    public void Trigram_hints_come_out_in_the_order_the_clauses_were_written()
    {
        const int n = 60;
        var f = CompiledFilter.Compile(Chain("and", n, i => $"contains(@mt, 'word{i:000}')"));

        var hints = f.GetTrigramHints();
        Assert.Equal(n, hints.Count);
        for (int i = 0; i < n; i++)
            Assert.Equal(("@mt", $"word{i:000}"), hints[i]);

        // An OR-chain still offers none: the consumer intersects hints.
        Assert.Empty(CompiledFilter.Compile(Chain("or", n, i => $"contains(@mt, 'word{i:000}')")).GetTrigramHints());
    }

    [Fact]
    public void A_long_chain_of_levels_still_derives_its_levels_and_its_header_shape()
    {
        // Twenty clauses over three levels.
        string[] names = ["Error", "Fatal", "Warning"];
        var f = CompiledFilter.Compile(Chain("or", 20, i => $"@l = '{names[i % 3]}'"));

        var expected = new HashSet<LogLevel> { LogLevel.Error, LogLevel.Fatal, LogLevel.Warning };
        Assert.Equal(expected, f.DerivedLevels);

        Assert.True(f.TryGetHeaderOnlyShape(out var levels, out string? service));
        Assert.Equal(expected, levels);
        Assert.Null(service);

        Assert.True(f.Matches(Event(LogLevel.Fatal)));
        Assert.False(f.Matches(Event(LogLevel.Information)));
    }

    [Fact]
    public void A_long_and_chain_still_answers_from_the_header()
    {
        // A service and nineteen level constraints that intersect to one level.
        string filter = "@service = 'payments' and " +
                        Chain("and", 19, i => i % 2 == 0 ? "@l in ['Error', 'Fatal']" : "@l in ['Error', 'Warning']");
        var f = CompiledFilter.Compile(filter);

        Assert.True(f.TryGetHeaderOnlyShape(out var levels, out string? service));
        Assert.Equal(new HashSet<LogLevel> { LogLevel.Error }, levels);
        Assert.Equal("payments", service);
        Assert.Equal(new HashSet<LogLevel> { LogLevel.Error }, f.DerivedLevels);
        Assert.NotNull(f.HeaderPredicate);
        Assert.True(f.HeaderPredicate!.HasServicePredicate);
    }

    [Fact]
    public void A_negated_long_chain_of_services_is_still_pushed_down_to_the_header_clause_by_clause()
    {
        // not (s0 or s1 or … s19) is twenty conjuncts by De Morgan, and the header predicate
        // collects each one from under the `or` — whatever shape the `or` has.
        var f = CompiledFilter.Compile("not (" + Chain("or", 20, i => $"@service = 'svc-{i}'") + ")");

        var header = f.HeaderPredicate;
        Assert.NotNull(header);
        Assert.True(header!.HasServicePredicate);

        static LogEvent From(string? service) => new()
        {
            Id              = new EventId(0u, 1u),
            Timestamp       = new DateTimeOffset(2026, 8, 23, 12, 0, 0, TimeSpan.Zero),
            Level           = LogLevel.Error,
            MessageTemplate = "something happened",
            ServiceName     = service,
        };

        for (int i = 0; i < 20; i++)
        {
            Assert.False(header.ServiceMayMatch($"svc-{i}"), $"svc-{i} is excluded by the filter");
            Assert.False(f.Matches(From($"svc-{i}")));
        }
        foreach (string? other in new[] { "svc-20", "payments", null })
        {
            // The header predicate may only say "cannot match"; where the evaluator matches, it must not.
            Assert.True(f.Matches(From(other)));
            Assert.True(header.ServiceMayMatch(other));
        }
    }

    [Fact]
    public void A_long_chain_of_time_bounds_still_narrows_the_window()
    {
        // Thirty lower bounds and thirty upper ones; the tightest of each wins.
        var from = new DateTimeOffset(2026, 3, 1, 0, 0, 0, TimeSpan.Zero);
        string lower = Chain("and", 30, i => $"@t >= '{from.AddDays(i):yyyy-MM-ddTHH:mm:ssZ}'");
        string upper = Chain("and", 30, i => $"@t < '{from.AddDays(90 - i):yyyy-MM-ddTHH:mm:ssZ}'");
        var f = CompiledFilter.Compile(lower + " and " + upper);

        Assert.Equal(from.AddDays(29).UtcTicks, f.MinTimestampTicks);
        Assert.Equal(from.AddDays(61).UtcTicks - 1, f.MaxTimestampTicks);
    }

    // ── The backstop ──────────────────────────────────────────────────────────

    /// <summary>
    /// <paramref name="levels"/> groups, each <c>(next-group and B … or C …)</c>: the one
    /// arrangement that turns nesting into depth. Every group OPENS with the next one, so the
    /// next one sits at the bottom of both of this group's chains; and the operators alternate
    /// on the way down, so no two chains are one chain — <c>(a or b) or c</c> is just
    /// <c>a or b or c</c>, parentheses leave nothing in the tree.
    /// </summary>
    private static string Staircase(int levels, int ands, int ors, int extraOutermostOrs = 0)
    {
        string text = "A = 1";
        for (int i = 0; i < levels; i++)
        {
            int orsHere = ors + (i == levels - 1 ? extraOutermostOrs : 0);
            text = "(" + text
                 + string.Concat(Enumerable.Repeat(" and B = 2", ands))
                 + string.Concat(Enumerable.Repeat(" or C = 3", orsHere))
                 + ")";
        }
        return text;
    }

    [Fact]
    public void A_group_of_the_same_operator_is_part_of_the_chain_it_opens()
    {
        // Sixty-four groups of `or`, each wrapped round the one before — which is one chain of
        // 1 025 clauses, and is balanced as one. This is also the shape the events page builds
        // a filter in, a click at a time: `(current filter) and X`.
        const int groups = 64;
        string text = "A = 1";
        for (int i = 0; i < groups; i++)
            text = "(" + text + string.Concat(Enumerable.Repeat(" or A = 2", 16)) + ")";

        var parsed = FilterParser.Parse(text);
        Assert.Equal(groups * 16, Depth(parsed));
        Assert.Equal(11, Depth(ChainBalancer.Balance(parsed)));     // ⌈log₂ 1 025⌉

        var f = CompiledFilter.Compile(text);
        Assert.True(f.Matches(Event(LogLevel.Error, ("A", 1L))));
        Assert.True(f.Matches(Event(LogLevel.Error, ("A", 2L))));
        Assert.False(f.Matches(Event(LogLevel.Error, ("A", 3L))));
    }

    /// <summary>Groups of four levels each that add up to exactly <see cref="ChainBalancer.MaxDepth"/>.</summary>
    private const int GroupsToTheLimit = ChainBalancer.MaxDepth / 4;

    [Fact]
    public void A_tree_at_the_depth_limit_compiles()
    {
        // Sixty-four groups, four levels each: the innermost clause sits exactly
        // ChainBalancer.MaxDepth levels under the root, well inside the parser's own limit.
        Assert.True(GroupsToTheLimit <= FilterParser.MaxNestingDepth);
        string filter = Staircase(GroupsToTheLimit, ands: 2, ors: 2);
        Assert.Equal(ChainBalancer.MaxDepth, Depth(FilterParser.Parse(filter)));

        var f = CompiledFilter.Compile(filter);
        Assert.True(f.Matches(Event(LogLevel.Error, ("A", 1L), ("B", 2L))));    // true all the way up
        Assert.True(f.Matches(Event(LogLevel.Error, ("C", 3L))));               // the outermost `or`
        Assert.False(f.Matches(Event(LogLevel.Error, ("A", 2L), ("B", 2L))));
        Assert.False(f.Matches(Event(LogLevel.Error, ("A", 1L))));
    }

    [Fact]
    public void A_tree_one_level_deeper_is_refused_with_a_sentence()
    {
        string filter = Staircase(GroupsToTheLimit, ands: 2, ors: 2, extraOutermostOrs: 1);
        Assert.Equal(ChainBalancer.MaxDepth + 1, Depth(FilterParser.Parse(filter)));

        var ex = Assert.Throws<FormatException>(() => CompiledFilter.Compile(filter));
        Assert.Contains("nested too deeply", ex.Message, StringComparison.Ordinal);
        Assert.Contains($"more than {ChainBalancer.MaxDepth} levels deep", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Use fewer groups inside one another", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_worst_a_filter_can_do_inside_both_limits_is_refused_not_followed()
    {
        // As many groups as the parser allows, each holding the longest chain of either
        // operator that is left unbalanced: 100 × (7 + 7) = 1 400 levels, and nothing in it is
        // past the nesting limit.
        int longest = ChainBalancer.LongChain - 1;
        string filter = Staircase(FilterParser.MaxNestingDepth, ands: longest, ors: longest);
        Assert.Equal(FilterParser.MaxNestingDepth * 2 * longest, Depth(FilterParser.Parse(filter)));

        Assert.Throws<FormatException>(() => CompiledFilter.Compile(filter));
    }

    [Fact]
    public void Chains_too_long_to_stay_unbalanced_do_not_buy_more_depth()
    {
        // The same staircase with a hundred clauses of each operator in every group. Unbalanced
        // that is 100 × 200 levels; balanced each chain is seven, and 100 × 14 is still past
        // the limit — so it is refused, by the same sentence, having cost almost nothing to refuse.
        string filter = Staircase(FilterParser.MaxNestingDepth, ands: 100, ors: 100);

        var ex = Assert.Throws<FormatException>(() => CompiledFilter.Compile(filter));
        Assert.Contains("nested too deeply", ex.Message, StringComparison.Ordinal);

        // Sixteen groups of it fit: 16 × (7 + 7) = 224 levels.
        var f = CompiledFilter.Compile(Staircase(16, ands: 100, ors: 100));
        Assert.True(f.Matches(Event(LogLevel.Error, ("A", 1L), ("B", 2L))));
        Assert.False(f.Matches(Event(LogLevel.Error, ("A", 2L), ("B", 2L))));
    }

    [Fact]
    public void A_balanced_chain_counts_for_the_levels_it_has_once_balanced_and_the_limit_is_exact_there_too()
    {
        // Eighteen groups of two hundred-clause chains: 18 × (7 + 7) = 252 levels, every one
        // of them a level of a BALANCED chain. Four negations in front make it 256, which is
        // the limit; a fifth is one too many. (Each group opens with the next, and the first
        // clause of a balanced chain sits at its full depth, so the count is not a ceiling here.)
        string staircase = Staircase(18, ands: 100, ors: 100);

        string atTheLimit = "not not not not " + staircase;
        Assert.Equal(ChainBalancer.MaxDepth, Depth(ChainBalancer.Balance(FilterParser.Parse(atTheLimit))));
        var f = CompiledFilter.Compile(atTheLimit);
        Assert.True(f.Matches(Event(LogLevel.Error, ("A", 1L), ("B", 2L))));    // four negations cancel
        Assert.False(f.Matches(Event(LogLevel.Error, ("A", 2L), ("B", 2L))));

        var ex = Assert.Throws<FormatException>(() => CompiledFilter.Compile("not " + atTheLimit));
        Assert.Contains("nested too deeply", ex.Message, StringComparison.Ordinal);
    }
}
