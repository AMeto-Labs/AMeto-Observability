using System.Reflection;
using Ameto.Core;

namespace Ameto.Core.Tests;

/// <summary>
/// An option an operator cannot find is an option they do not have. README points at
/// <c>docs/CONFIGURATION.md</c> and nothing else, so a setting absent from that file is absent
/// full stop — and two of them were: <c>Ameto:Query</c> had no section at all, which is where the
/// index cache and its new idle eviction live, including the one tuning knob the memory work
/// prescribes for a 512 MB stand.
///
/// <para>This is a drift guard, not a style check. It asks only that every settable option of the
/// two groups this round changed appears BY NAME in the reference, and that the shipped
/// <c>config.yml</c> and the compose example carry the block an operator would copy from.
/// Computed properties (<c>EffectiveIndexCacheBytes</c>) are not settings and are skipped.</para>
/// </summary>
public sealed class ConfigurationDocsTests
{
    private static string RepoRoot()
    {
        var d = new DirectoryInfo(AppContext.BaseDirectory);
        while (d is not null && !File.Exists(Path.Combine(d.FullName, "docs", "CONFIGURATION.md")))
            d = d.Parent;

        Assert.NotNull(d);
        return d!.FullName;
    }

    private static string Read(params string[] relative) =>
        File.ReadAllText(Path.Combine(RepoRoot(), Path.Combine(relative)));

    [Fact]
    public void Every_settable_query_and_ingestion_option_is_documented()
    {
        string doc = Read("docs", "CONFIGURATION.md");
        var missing = new List<string>();

        foreach (var type in new[] { typeof(QueryOptions), typeof(IngestionOptions) })
            foreach (var p in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (!p.CanWrite) continue;                  // computed, not a setting
                if (!doc.Contains(p.Name, StringComparison.Ordinal)) missing.Add($"{type.Name}.{p.Name}");
            }

        Assert.True(missing.Count == 0,
            "docs/CONFIGURATION.md does not mention " + string.Join(", ", missing));
    }

    [Fact]
    public void The_query_section_exists_and_says_what_unset_derives()
    {
        string doc = Read("docs", "CONFIGURATION.md");

        Assert.Contains("## Query options (`Ameto:Query`)", doc, StringComparison.Ordinal);
        // The two behaviour changes an upgrading operator has to be told about.
        Assert.Contains("IndexCacheIdleEvict", doc, StringComparison.Ordinal);
        Assert.Contains("Upgrading", doc, StringComparison.Ordinal);
    }

    [Fact]
    public void The_shipped_config_and_the_compose_example_carry_the_query_block()
    {
        Assert.Contains("Query:", Read("src", "Ameto.Server", "config.yml"), StringComparison.Ordinal);

        string compose = Read("install", "docker", "docker-compose.example.yml");
        Assert.Contains("Ameto__Query__IndexCacheBytes", compose, StringComparison.Ordinal);
        Assert.Contains("Ameto__Query__IndexCacheIdleEvict", compose, StringComparison.Ordinal);
    }
}
