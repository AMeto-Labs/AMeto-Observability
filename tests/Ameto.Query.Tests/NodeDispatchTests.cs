using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

/// <summary>
/// The evaluator dispatches on <see cref="FilterNode.DispatchKind"/>, a tag resolved from the
/// concrete type once per compile.
///
/// <para>The name is the point of the first test. A node already has a <c>Kind</c>:
/// <see cref="FromJsonPathStringPredicateNode.Kind"/> says which string predicate it is. A base
/// member spelled <c>Kind</c> is HIDDEN by it, so <c>switch (node.Kind)</c> written against a
/// derived-typed variable compiles against the wrong member and reads the wrong enum — a
/// warning today, a wrong answer the moment someone writes that switch. The two names have to
/// stay different, and this test does not compile if they stop being.</para>
/// </summary>
public sealed class NodeDispatchTests
{
    [Fact]
    public void The_dispatch_tag_does_not_collide_with_a_node_s_own_Kind()
    {
        var node = new FromJsonPathStringPredicateNode(
            "Payload", ["a", "b"], "x", ci: true, FromJsonPathPredicateKind.Contains);

        // Both readable, from the derived type, meaning different things. If the base field were
        // called Kind again, the line below would read FromJsonPathPredicateKind and this would
        // not build.
        Assert.Equal(FromJsonPathPredicateKind.Contains, node.Kind);
        Assert.Equal(NodeKind.Other, node.DispatchKind);
    }

    [Fact]
    public void The_bulk_shapes_carry_their_tag()
    {
        Assert.Equal(NodeKind.MatchAll,    new MatchAllNode().DispatchKind);
        Assert.Equal(NodeKind.Like,        new LikeNode("p", "%x%").DispatchKind);
        Assert.Equal(NodeKind.Compare,     new CompareNode("p", CompareOp.Eq, "x").DispatchKind);
        Assert.Equal(NodeKind.Contains,    new ContainsNode("p", "x").DispatchKind);
        Assert.Equal(NodeKind.StartsWith,  new StartsWithNode("p", "x").DispatchKind);
        Assert.Equal(NodeKind.EndsWith,    new EndsWithNode("p", "x").DispatchKind);
        Assert.Equal(NodeKind.Not,         new NotNode(new MatchAllNode()).DispatchKind);
    }

    /// <summary>
    /// A node type with no tag is dispatched by the type switch behind the jump table and
    /// answers exactly as it always did. This is the safety property the design rests on: a
    /// node added later and not registered loses the shortcut, never the answer.
    /// </summary>
    [Fact]
    public void An_untagged_node_type_still_evaluates()
    {
        var buf = new ArrayBufferWriter<byte>(128);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("Region"); w.Write("ae-dxb");
        w.Flush();

        var ev = new LogEvent
        {
            Id              = new EventId(0u, 1u),
            Timestamp       = new DateTimeOffset(2026, 8, 23, 10, 0, 0, TimeSpan.Zero),
            Level           = LogLevel.Information,
            MessageTemplate = "request handled",
            RawProperties   = buf.WrittenMemory,
        };

        // length() is a function predicate — no tag, so it goes through the type switch.
        Assert.Equal(NodeKind.Other, new LengthCompareNode("Region", CompareOp.Eq, 6).DispatchKind);
        Assert.True(CompiledFilter.Compile("length(Region) = 6").Matches(ev));
        Assert.False(CompiledFilter.Compile("length(Region) = 7").Matches(ev));
    }
}
