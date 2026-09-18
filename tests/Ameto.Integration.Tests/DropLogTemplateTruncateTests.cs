using System.Text;
using Ameto.Ingestion;

namespace Ameto.Integration.Tests;

/// <summary>
/// The oversized-drop Warning names the producer's template cut to 120 UTF-16 units. The cut
/// must not split a surrogate pair: half a pair at the end of a log line is an unpaired
/// surrogate, which a UTF-8 sink turns into U+FFFD or refuses to encode.
/// </summary>
public sealed class DropLogTemplateTruncateTests
{
    /// <summary>Throws on an unpaired surrogate instead of replacing it.</summary>
    private static readonly UTF8Encoding Strict = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    [Fact]
    public void A_pair_straddling_the_cut_is_left_out_whole()
    {
        // "😀" is U+1F600, the pair D83D DE00: its high half is unit 120, its low half unit 121.
        string template = new string('a', 119) + "😀" + " tail";

        string cut = IngestionEndpoint.Truncate(template, 120);

        Assert.Equal(new string('a', 119) + "…", cut);
        Strict.GetBytes(cut);   // no half pair left to encode
    }

    [Fact]
    public void A_pair_that_ends_at_the_cut_is_kept_whole()
    {
        string template = new string('a', 118) + "😀" + " tail";

        string cut = IngestionEndpoint.Truncate(template, 120);

        Assert.Equal(new string('a', 118) + "😀…", cut);
        Strict.GetBytes(cut);
    }

    [Fact]
    public void Empty_short_and_plain_long_templates_are_cut_as_before()
    {
        Assert.Equal("(none)", IngestionEndpoint.Truncate(null, 120));
        Assert.Equal("(none)", IngestionEndpoint.Truncate("", 120));

        string exact = new string('b', 118) + "😀";   // 120 units: nothing is cut, nothing copied
        Assert.Same(exact, IngestionEndpoint.Truncate(exact, 120));

        string plain = new string('c', 200);
        Assert.Equal(new string('c', 120) + "…", IngestionEndpoint.Truncate(plain, 120));
    }
}
