using Ameto.Core;

namespace Ameto.Query.Filtering;

/// <summary>
/// The part of a filter the hot-tier scan can answer from the 64-byte
/// <see cref="LogEventHeader"/> alone — see <see cref="IHotHeaderPredicate"/> for the contract.
///
/// <para>Built from the filter's AND-chain only. Every leaf verdict here is EXACT (the level
/// mask is computed by running the evaluator's own comparison over each level; trace and
/// span ids compare as integers under the same presence rule the string path has; a service
/// leaf is evaluated by the evaluator on the resolved name), which is what lets <c>not</c>
/// be pushed down as a plain complement. What makes the whole thing three-valued is only
/// that the chain's OTHER conjuncts are not looked at: "may match" means "nothing here
/// rules it out".</para>
///
/// <para>An OR is pushed only when every leaf under it constrains the level — the union of
/// their masks is then exact too. An OR reaching any other field (<c>@l = 'Error' or
/// Foo = 1</c>) is not representable and is ignored, i.e. answers "maybe". Under a
/// <c>not</c>, De Morgan turns an OR into conjuncts, so <c>not (@l = 'Debug' or
/// service.name = 'x')</c> pushes both halves.</para>
/// </summary>
internal sealed class HeaderPredicate : IHotHeaderPredicate
{
    /// <summary>One bit per <see cref="LogLevel"/> value, Verbose..Fatal.</summary>
    private const int  LevelCount = 6;
    private const byte AllLevels  = (1 << LevelCount) - 1;

    private readonly byte         _levelMask;
    private readonly IdTest[]     _idTests;
    private readonly ServiceLeaf[] _serviceLeaves;

    private readonly struct IdTest(bool isSpan, bool isNullTest, bool eq, bool negated, ulong hi, ulong lo)
    {
        public readonly bool  IsSpan     = isSpan;
        public readonly bool  IsNullTest = isNullTest;   // `@tr = null` / `@tr != null`
        public readonly bool  Eq         = eq;
        public readonly bool  Negated    = negated;
        public readonly ulong Hi         = hi;
        public readonly ulong Lo         = lo;
    }

    private readonly struct ServiceLeaf(FilterNode node, bool negated)
    {
        public readonly FilterNode Node    = node;
        public readonly bool       Negated = negated;
    }

    private HeaderPredicate(byte levelMask, IdTest[] idTests, ServiceLeaf[] serviceLeaves)
    {
        _levelMask     = levelMask;
        _idTests       = idTests;
        _serviceLeaves = serviceLeaves;
    }

    /// <summary>Levels the AND-chain admits, or null when it constrains none.</summary>
    public HashSet<LogLevel>? DerivedLevels
    {
        get
        {
            if (_levelMask == AllLevels) return null;
            var set = new HashSet<LogLevel>(LevelCount);
            for (int l = 0; l < LevelCount; l++)
                if (((_levelMask >> l) & 1) != 0) set.Add((LogLevel)l);
            return set;
        }
    }

    // ── IHotHeaderPredicate ───────────────────────────────────────────────────

    public bool MayMatch(in LogEventHeader header)
    {
        // A level byte outside the enum is not one the mask speaks for: leave it to the
        // evaluator rather than guess.
        int lvl = (byte)header.Level;
        if (lvl < LevelCount && ((_levelMask >> lvl) & 1) == 0) return false;

        var tests = _idTests;
        for (int i = 0; i < tests.Length; i++)
        {
            ref readonly var t = ref tests[i];
            bool present = t.IsSpan
                ? header.SpanId != 0
                : (header.TraceIdHi | header.TraceIdLo) != 0;
            bool verdict;
            if (t.IsNullTest)
                verdict = t.Eq ? !present : present;
            else
            {
                bool eq = present && (t.IsSpan
                    ? header.SpanId == t.Lo
                    : header.TraceIdHi == t.Hi && header.TraceIdLo == t.Lo);
                verdict = t.Eq ? eq : !eq;
            }
            if (verdict == t.Negated) return false;
        }
        return true;
    }

    public bool HasServicePredicate => _serviceLeaves.Length > 0;

    public bool ServiceMayMatch(string? serviceName)
    {
        var leaves = _serviceLeaves;
        for (int i = 0; i < leaves.Length; i++)
        {
            ref readonly var leaf = ref leaves[i];
            bool verdict = leaf.Node switch
            {
                CompareNode cmp => FilterEvaluator.CompareValues(serviceName, cmp.Value, cmp.Op),
                InNode inNode   => FilterEvaluator.InValues(serviceName, inNode.Values),
                _               => true,
            };
            if (verdict == leaf.Negated) return false;
        }
        return true;
    }

    // ── Construction ─────────────────────────────────────────────────────────

    /// <summary>Null when the filter constrains nothing the header can see.</summary>
    public static HeaderPredicate? TryBuild(FilterNode root)
    {
        var b = new Builder();
        b.Collect(root, negated: false);
        if (b.LevelMask == AllLevels && b.IdTests.Count == 0 && b.ServiceLeaves.Count == 0)
            return null;
        return new HeaderPredicate(b.LevelMask, b.IdTests.ToArray(), b.ServiceLeaves.ToArray());
    }

    private sealed class Builder
    {
        public byte              LevelMask     = AllLevels;
        public List<IdTest>      IdTests       = new(2);
        public List<ServiceLeaf> ServiceLeaves = new(2);

        public void Collect(FilterNode node, bool negated)
        {
            switch (node)
            {
                case AndNode andNode when !negated:
                    Collect(andNode.Left,  false);
                    Collect(andNode.Right, false);
                    return;

                // not (a or b) ⇔ not a and not b — still a conjunction.
                case OrNode orNode when negated:
                    Collect(orNode.Left,  true);
                    Collect(orNode.Right, true);
                    return;

                case NotNode not:
                    Collect(not.Operand, !negated);
                    return;
            }

            // A pure level expression (a leaf, or an AND/OR/NOT tree of level leaves): its
            // mask is exact, so a negation is the complement.
            if (TryLevelMask(node, out byte mask))
            {
                if (negated) mask = (byte)(~mask & AllLevels);
                LevelMask &= mask;
                return;
            }

            switch (node)
            {
                case TraceIdCompareNode tid:
                    IdTests.Add(new IdTest(tid.IsSpan, isNullTest: false, tid.Op == CompareOp.Eq, negated, tid.Hi, tid.Lo));
                    return;

                case CompareNode { RightProperty: null, Value: null, Op: CompareOp.Eq or CompareOp.Ne } cmp
                    when BuiltinFields.TryResolve(cmp.Property, out var f)
                      && (f is BuiltinField.TraceId or BuiltinField.SpanId):
                    IdTests.Add(new IdTest(f == BuiltinField.SpanId, isNullTest: true, cmp.Op == CompareOp.Eq, negated, 0, 0));
                    return;

                case CompareNode { RightProperty: null } svc when IsBuiltin(svc.Property, BuiltinField.ServiceName):
                    ServiceLeaves.Add(new ServiceLeaf(svc, negated));
                    return;

                case InNode svcIn when IsBuiltin(svcIn.Property, BuiltinField.ServiceName):
                    ServiceLeaves.Add(new ServiceLeaf(svcIn, negated));
                    return;

                // Anything else (an OR over mixed fields, a property predicate, a
                // function...) is "maybe": not looked at here, decided by the evaluator.
            }
        }

        /// <summary>
        /// The set of levels a level-only subtree admits, computed by evaluating it for each
        /// level through the evaluator's own comparison — so an alias the evaluator never
        /// matches (<c>'Info'</c>) yields an empty mask here too, not a guess.
        /// </summary>
        private static bool TryLevelMask(FilterNode node, out byte mask)
        {
            mask = 0;
            for (int l = 0; l < LevelCount; l++)
            {
                bool? v = LevelVerdict(node, (LogLevel)l);
                if (v is null) { mask = 0; return false; }
                if (v.Value) mask |= (byte)(1 << l);
            }
            return true;
        }

        private static bool? LevelVerdict(FilterNode node, LogLevel level)
        {
            switch (node)
            {
                case LevelNode lvl:
                    return level == lvl.Level;

                case CompareNode { RightProperty: null } cmp when IsBuiltin(cmp.Property, BuiltinField.Level):
                    return FilterEvaluator.CompareValues(level.ToSeqString(), cmp.Value, cmp.Op);

                case InNode inNode when IsBuiltin(inNode.Property, BuiltinField.Level):
                    return FilterEvaluator.InValues(level.ToSeqString(), inNode.Values);

                case NotNode not:
                    return LevelVerdict(not.Operand, level) is bool inner ? !inner : null;

                case AndNode and:
                    return LevelVerdict(and.Left, level) is bool al && LevelVerdict(and.Right, level) is bool ar
                        ? al && ar : null;

                case OrNode or:
                    return LevelVerdict(or.Left, level) is bool ol && LevelVerdict(or.Right, level) is bool orr
                        ? ol || orr : null;

                default:
                    return null;
            }
        }

        private static bool IsBuiltin(string property, BuiltinField expected) =>
            BuiltinFields.TryResolve(property, out var f) && f == expected;
    }
}
