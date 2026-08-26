using Ameto.Core;

namespace Ameto.Core.Tests;

/// <summary>
/// The prefix is written by hand into config.yml, and the two shapes it turns into differ by
/// one trailing slash that nothing would complain about until a browser resolved an asset
/// against the wrong directory. So both shapes are pinned here for every form a hand might
/// plausibly type.
/// </summary>
public sealed class UrlBasePathTests
{
    // ── Root: the default, and every way of writing "nothing" ─────────────────

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("/")]
    [InlineData("//")]
    [InlineData("///")]
    public void Parse_EmptyForms_AreTheRoot(string? configured)
    {
        var basePath = UrlBasePath.Parse(configured);

        Assert.True(basePath.IsRoot);
        // UsePathBase("") is the no-op; UsePathBase("/") is NOT — it would be a prefix.
        Assert.Equal("",  basePath.PathBase);
        Assert.Equal("/", basePath.BaseHref);
    }

    [Fact]
    public void Default_IsTheRoot()
    {
        Assert.True(default(UrlBasePath).IsRoot);
        Assert.Equal("",  default(UrlBasePath).PathBase);
        Assert.Equal("/", default(UrlBasePath).BaseHref);
        Assert.Equal(UrlBasePath.Root, default(UrlBasePath));
    }

    // ── The forms an operator actually types ──────────────────────────────────

    [Theory]
    [InlineData("ameto")]
    [InlineData("/ameto")]
    [InlineData("ameto/")]
    [InlineData("/ameto/")]
    [InlineData("  /ameto/  ")]
    [InlineData("//ameto//")]
    public void Parse_AnyHandWrittenForm_NormalisesToTheSamePair(string configured)
    {
        var basePath = UrlBasePath.Parse(configured);

        Assert.False(basePath.IsRoot);
        Assert.Equal("/ameto",  basePath.PathBase);   // leading slash, no trailing
        Assert.Equal("/ameto/", basePath.BaseHref);   // leading AND trailing
    }

    [Theory]
    [InlineData("tools/ameto",    "/tools/ameto")]
    [InlineData("/tools/ameto/",  "/tools/ameto")]
    [InlineData("/a/b/c",         "/a/b/c")]
    public void Parse_NestedPrefix_IsKeptWhole(string configured, string expectedPathBase)
    {
        var basePath = UrlBasePath.Parse(configured);

        Assert.Equal(expectedPathBase,       basePath.PathBase);
        Assert.Equal(expectedPathBase + "/", basePath.BaseHref);
    }

    [Fact]
    public void Parse_PreservesCase()
    {
        // UsePathBase matches case-insensitively, but the href we hand the browser is the
        // one the operator wrote — rewriting their casing would be a surprise, not a service.
        Assert.Equal("/Ameto",  UrlBasePath.Parse("/Ameto").PathBase);
        Assert.Equal("/Ameto/", UrlBasePath.Parse("/Ameto").BaseHref);
    }

    // ── What is refused, and why it is worth refusing ─────────────────────────

    [Theory]
    [InlineData("https://logs.example.com/ameto")]   // the whole-URL mistake
    [InlineData("http://localhost:5341/")]
    public void Parse_FullUrl_Throws(string configured)
    {
        var ex = Assert.Throws<ArgumentException>(() => UrlBasePath.Parse(configured));
        Assert.Contains("full URL", ex.Message);
    }

    [Theory]
    [InlineData("/ameto?x=1")]
    [InlineData("/ameto#frag")]
    [InlineData("\\ameto")]
    public void Parse_QueryFragmentOrBackslash_Throws(string configured)
    {
        Assert.Throws<ArgumentException>(() => UrlBasePath.Parse(configured));
    }

    [Theory]
    [InlineData("/ameto/../etc")]
    [InlineData("/./ameto")]
    public void Parse_DotSegments_Throw(string configured)
    {
        // UsePathBase compares the literal string, so a prefix containing ".." would simply
        // never match a request — a silent "nothing works" rather than an error.
        Assert.Throws<ArgumentException>(() => UrlBasePath.Parse(configured));
    }

    [Theory]
    [InlineData("/am eto")]
    [InlineData("/ameto\tx")]
    public void Parse_InnerWhitespace_Throws(string configured)
    {
        Assert.Throws<ArgumentException>(() => UrlBasePath.Parse(configured));
    }

    // ── Value semantics ───────────────────────────────────────────────────────

    [Fact]
    public void Equality_IsByNormalisedValue()
    {
        Assert.Equal(UrlBasePath.Parse("ameto"), UrlBasePath.Parse("/ameto/"));
        Assert.True(UrlBasePath.Parse("ameto") == UrlBasePath.Parse("//ameto//"));
        Assert.True(UrlBasePath.Parse("/ameto") != UrlBasePath.Parse("/other"));
        Assert.Equal(UrlBasePath.Parse("/ameto").GetHashCode(), UrlBasePath.Parse("ameto/").GetHashCode());
    }

    [Fact]
    public void ToString_ShowsSomethingAnOperatorRecognises()
    {
        Assert.Equal("/",      UrlBasePath.Root.ToString());
        Assert.Equal("/ameto", UrlBasePath.Parse("ameto").ToString());
    }
}
