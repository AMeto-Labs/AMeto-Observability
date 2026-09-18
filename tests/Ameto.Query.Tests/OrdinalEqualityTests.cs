using System.Buffers;
using MessagePack;
using Ameto.Core;
using Ameto.Query.Filtering;

namespace Ameto.Query.Tests;

/// <summary>
/// <c>=</c> and <c>&lt;&gt;</c> on strings answer with <c>string.Equals(OrdinalIgnoreCase)</c>
/// instead of asking <c>string.Compare</c> for an ordering and testing it against zero.
///
/// <para>The two are the same answer by construction — over one comparison type,
/// <c>Compare(a, b, cmp) == 0</c> is the definition of <c>Equals(a, b, cmp)</c> — so what
/// these tests guard is that the REST of the block did not move with it: that the ordering
/// operators still order, that the case-insensitivity is intact, and that the pairs where
/// equality and ordering could conceivably diverge (embedded nulls, surrogates, strings equal
/// under folding but not byte-for-byte) still answer identically to what the ordering road
/// says about them. An ordinal comparison is being kept ordinal here, not swapped for a
/// linguistic one, and the last case below is the one that would catch it if it ever were.</para>
/// </summary>
public sealed class OrdinalEqualityTests
{
    private delegate void WriteProps(ref MessagePackWriter w);

    private static LogEvent Event(WriteProps writeProps)
    {
        var buf = new ArrayBufferWriter<byte>(256);
        var w   = new MessagePackWriter(buf);
        writeProps(ref w);
        w.Flush();

        return new LogEvent
        {
            Id              = new EventId(0u, 1u),
            Timestamp       = new DateTimeOffset(2026, 8, 23, 10, 0, 0, TimeSpan.Zero),
            Level           = LogLevel.Error,
            MessageTemplate = "request handled",
            RawProperties   = buf.WrittenMemory,
        };
    }

    private static LogEvent WithRegion(string value) => Event((ref MessagePackWriter w) =>
    {
        w.WriteMapHeader(1);
        w.Write("Region"); w.Write(value);
    });

    private static bool Matches(LogEvent ev, string filter) => CompiledFilter.Compile(filter).Matches(ev);

    [Theory]
    [InlineData("ae-dxb", "Region = 'ae-dxb'",  true)]
    [InlineData("ae-dxb", "Region = 'AE-DXB'",  true)]    // case-insensitive, as before
    [InlineData("ae-dxb", "Region = 'ae-dx'",   false)]   // prefix is not equality
    [InlineData("ae-dxb", "Region = 'ae-dxbb'", false)]
    [InlineData("ae-dxb", "Region <> 'ae-dxb'", false)]
    [InlineData("ae-dxb", "Region <> 'other'",  true)]
    [InlineData("",       "Region = ''",        true)]
    [InlineData("",       "Region = 'x'",       false)]
    [InlineData("ae-dxb", "Region < 'b'",       true)]    // ordering still orders
    [InlineData("ae-dxb", "Region > 'b'",       false)]
    [InlineData("ae-dxb", "Region <= 'ae-dxb'", true)]
    [InlineData("ae-dxb", "Region >= 'ae-dxb'", true)]
    public void Equality_and_ordering_answer_as_they_did(string value, string filter, bool expected)
        => Assert.Equal(expected, Matches(WithRegion(value), filter));

    /// <summary>The commonest predicate in the product, and the one this change is for.</summary>
    [Fact]
    public void The_level_predicate_still_matches()
    {
        var ev = WithRegion("x");
        Assert.True(Matches(ev, "@l = 'Error'"));
        Assert.True(Matches(ev, "@l = 'error'"));
        Assert.False(Matches(ev, "@l = 'Warning'"));
        Assert.True(Matches(ev, "@l <> 'Warning'"));
    }

    /// <summary>
    /// The pairs where an equality shortcut could diverge from the ordering it replaced. Each
    /// is asserted against what <c>string.Compare(…, OrdinalIgnoreCase) == 0</c> says, computed
    /// here rather than assumed, so the test states the equivalence it is actually guarding.
    /// </summary>
    [Theory]
    [InlineData("a\0b", "a\0b")]               // embedded null: not a terminator
    [InlineData("a\0b", "a\0c")]
    [InlineData("\U00010400", "\U00010428")]   // DESERET capital / small: a surrogate pair
    [InlineData("straße", "STRASSE")]          // equal linguistically, NOT ordinally
    [InlineData("K", "k")]                     // KELVIN SIGN vs k: equal linguistically only
    [InlineData("resume", "résumé")]
    [InlineData("ǅ", "ǆ")]                     // title-case digraph
    public void Agrees_with_the_ordering_road_it_replaced(string a, string b)
    {
        bool viaCompare = string.Compare(a, b, StringComparison.OrdinalIgnoreCase) == 0;

        var ev = WithRegion(a);
        Assert.Equal(viaCompare,  Matches(ev, $"Region = '{b}'"));
        Assert.Equal(!viaCompare, Matches(ev, $"Region <> '{b}'"));
    }
}
