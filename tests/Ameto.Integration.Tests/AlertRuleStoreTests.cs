using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Alerts;
using Ameto.Core;

namespace Ameto.Integration.Tests;

/// <summary>
/// Alert rules now live in Ameto.db (one row per rule) instead of a single
/// alerts.json. These cover the migration off the legacy file, the row-level
/// isolation that a JSON array never had, and the channel-type case-insensitivity
/// that was silently breaking rule loading on real deployments.
/// </summary>
public sealed class AlertRuleStoreTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-alerts-" + Guid.NewGuid().ToString("N"));

    public AlertRuleStoreTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    /// <summary>Identity protector — keeps the test independent of the AES key file.</summary>
    private sealed class NoopProtector : ISecretProtector
    {
        public string Protect(string? plaintext) => plaintext ?? "";
        public string Unprotect(string? value)   => value ?? "";
        public bool   IsProtected(string? value) => false;
    }

    private AlertRuleStore NewStore() =>
        new(_dir, new NoopProtector(), NullLogger<AlertRuleStore>.Instance);

    private static long RuleRowCount(string dir)
    {
        using var conn = new SqliteConnection($"Data Source={Path.Combine(dir, "Ameto.db")}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM alert_rules";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    // ── Migration ─────────────────────────────────────────────────────────────

    [Fact]
    public void MigratesLegacyJson_WithPascalCaseTelegramChannel()
    {
        // The exact shape observed on the VPS: PascalCase field names, a telegram
        // channel (no Url) — which the old case-sensitive converter mis-routed to
        // WebhookChannel and threw "missing required property Url", taking the whole
        // file down.
        var json = """
        [
          {
            "Id": "f1b3bf89",
            "Name": "ProfileCreated",
            "Enabled": true,
            "Severity": "Info",
            "Source": "Log",
            "Comparator": "GreaterOrEqual",
            "Threshold": 1,
            "WindowSeconds": 600,
            "Filter": "@m like 'Profile created%'",
            "Channels": [
              { "BotToken": "secret-token", "ChatId": "8081209694", "Type": "telegram" }
            ],
            "Template": "new profile"
          }
        ]
        """;
        File.WriteAllText(Path.Combine(_dir, "alerts.json"), json);

        var store = NewStore();

        // Rule loaded, telegram channel intact.
        var rules = store.GetAll();
        Assert.Single(rules);
        var rule = rules[0];
        Assert.Equal("ProfileCreated", rule.Name);
        var ch = Assert.IsType<TelegramChannel>(Assert.Single(rule.Channels));
        Assert.Equal("8081209694", ch.ChatId);
        Assert.Equal("secret-token", ch.BotToken);

        // Persisted into the DB and the legacy file archived away.
        Assert.Equal(1, RuleRowCount(_dir));
        Assert.False(File.Exists(Path.Combine(_dir, "alerts.json")));
        Assert.True(File.Exists(Path.Combine(_dir, "alerts.json.migrated")));

        // A second store reads from the DB (no re-import) and sees the same rule.
        var store2 = NewStore();
        Assert.Single(store2.GetAll());
    }

    // ── CRUD round-trip ───────────────────────────────────────────────────────

    [Fact]
    public void Upsert_Delete_RoundtripThroughDb()
    {
        var store = NewStore();
        var rule = new AlertRule
        {
            Id = "r1", Name = "High errors", Enabled = true,
            Source = AlertSource.Log, Comparator = AlertComparator.GreaterOrEqual, Threshold = 5,
            Filter = "@l = 'Error'",
            Channels = new List<AlertChannel> { new TelegramChannel { ChatId = "123", BotToken = "tok" } },
        };
        store.Upsert(rule);

        // Visible to a fresh store (persisted), secret survives the round-trip.
        var reloaded = NewStore().GetById("r1");
        Assert.NotNull(reloaded);
        Assert.Equal("High errors", reloaded!.Name);
        Assert.Equal("tok", ((TelegramChannel)reloaded.Channels[0]).BotToken);

        Assert.True(store.Delete("r1"));
        Assert.Empty(NewStore().GetAll());
        Assert.Equal(0, RuleRowCount(_dir));
    }

    // ── Row-level isolation ───────────────────────────────────────────────────

    [Fact]
    public void CorruptRow_IsSkipped_OthersStillLoad()
    {
        var store = NewStore();
        store.Upsert(new AlertRule { Id = "good", Name = "OK", Source = AlertSource.Log });

        // Inject a garbage row directly — the single-file JSON store would have
        // thrown for the whole set; the DB store must skip just this one.
        using (var conn = new SqliteConnection($"Data Source={Path.Combine(_dir, "Ameto.db")}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO alert_rules (id, data, updated_at) VALUES ('bad', '{ not json', @ts)";
            cmd.Parameters.AddWithValue("@ts", DateTimeOffset.UtcNow.ToString("O"));
            cmd.ExecuteNonQuery();
        }

        var loaded = NewStore().GetAll();
        Assert.Single(loaded);
        Assert.Equal("good", loaded[0].Id);
    }
}
