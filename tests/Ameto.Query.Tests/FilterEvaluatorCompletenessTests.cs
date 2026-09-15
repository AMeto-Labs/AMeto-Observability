using System.Buffers;
using System.Runtime.CompilerServices;
using MessagePack;
using Ameto.Core;
using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

/// <summary>
/// The evaluator has to know every node the parser can build.
///
/// <para>A node it does not know used to answer false. <c>Debug.Fail</c> meant to make that
/// loud, but it did not do the job anywhere. CI and every deployment build Release, where the
/// assertion is compiled out, so the query quietly matched nothing. A Debug server hit a
/// FailFast and died. The node arms are a hand-kept list (<c>MatchesRare</c>), and a new node
/// type such as Q1's <c>TraceIdCompareNode</c> is exactly the change that forgets to add one.
/// These tests make a missing arm a test failure in the configuration CI runs, and a
/// reportable error rather than a silent empty result in production.</para>
/// </summary>
public sealed class FilterEvaluatorCompletenessTests
{
    /// <summary>A node type nobody taught the evaluator about: the future node this guards against.</summary>
    private sealed class UnregisteredNode : FilterNode { }

    private static LogEvent Sample()
    {
        var buf = new ArrayBufferWriter<byte>(128);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(2);
        w.Write("Region");  w.Write("ae-dxb");
        w.Write("Elapsed"); w.Write(125);
        w.Flush();

        return new LogEvent
        {
            Id              = new EventId(0u, 1u),
            Timestamp       = new DateTimeOffset(2026, 9, 15, 10, 0, 0, TimeSpan.Zero),
            Level           = LogLevel.Information,
            MessageTemplate = "request handled",
            RawProperties   = buf.WrittenMemory,
        };
    }

    [Fact]
    public void A_node_the_evaluator_has_no_arm_for_throws_instead_of_matching_nothing()
    {
        var ex = Assert.Throws<FilterEvaluator.UnhandledFilterNodeException>(
            () => FilterEvaluator.Matches(new UnregisteredNode(), Sample()));

        Assert.IsAssignableFrom<NotSupportedException>(ex);
        Assert.Contains(nameof(UnregisteredNode), ex.Message);
    }

    /// <summary>
    /// …wherever it sits. Under a <c>not</c> it would otherwise be worse than silent: "matches
    /// nothing" negated is "matches everything".
    /// </summary>
    [Fact]
    public void A_node_without_an_arm_is_loud_inside_a_compound_filter_too()
    {
        Assert.Throws<FilterEvaluator.UnhandledFilterNodeException>(
            () => FilterEvaluator.Matches(new NotNode(new UnregisteredNode()), Sample()));
    }

    /// <summary>
    /// Every concrete <see cref="FilterNode"/> in Ameto.Query reaches an arm.
    ///
    /// <para>Each node is created WITHOUT its constructor. The test is about whether an arm
    /// exists, and supplying valid arguments to fifty-odd node shapes would be a second parser.
    /// With no constructor run the fields are unset, so most arms throw once they are reached
    /// (usually a NullReferenceException). That still proves the arm is there, and only
    /// <see cref="FilterEvaluator.UnhandledFilterNodeException"/> counts as missing. An
    /// uninitialised node also carries no dispatch tag, so every type goes through the type
    /// switch. That is the list meant to be complete on its own, and the only one the tag's
    /// fallback can reach.</para>
    /// </summary>
    [Fact]
    public void Every_concrete_filter_node_has_an_arm_in_the_evaluator()
    {
        var ev      = Sample();
        var missing = new List<string>();
        int checkedTypes = 0;

        foreach (var type in typeof(FilterNode).Assembly.GetTypes())
        {
            if (type.IsAbstract || type.ContainsGenericParameters || !type.IsSubclassOf(typeof(FilterNode)))
                continue;
            checkedTypes++;

            var node = (FilterNode)RuntimeHelpers.GetUninitializedObject(type);
            try
            {
                FilterEvaluator.Matches(node, ev);
            }
            catch (FilterEvaluator.UnhandledFilterNodeException)
            {
                missing.Add(type.Name);
            }
            catch (Exception)
            {
                // The arm was found and ran into a field the constructor would have set.
            }
        }

        // A scan that finds nothing proves nothing. There are fifty-odd node types today.
        Assert.True(checkedTypes >= 50,
            $"found only {checkedTypes} FilterNode types; the scan is looking in the wrong assembly");
        Assert.True(missing.Count == 0,
            "FilterEvaluator has no arm for: " + string.Join(", ", missing));
    }
}
