using System.Net;
using System.Net.Http.Json;
using Ameto.Server.Auth;
using Microsoft.Data.Sqlite;

namespace Ameto.Integration.Tests;

/// <summary>
/// <c>/api/search-history</c> now keys every entry by page as well as by user
/// (<c>scope</c>: logs | traces | metrics), because Events, Traces and Metrics
/// share one endpoint and one table but must not share one list — a trace filter
/// offered as a log search is applied to the wrong query language.
///
/// These cover the three properties that split is made of: entries stay inside
/// their scope for every verb, the ten-recent prune counts per scope so a busy
/// page cannot empty a quiet one, and a request with no scope at all still means
/// 'logs' — the contract that keeps a client built before scopes existed working.
/// The last test is about the upgrade rather than the endpoint: it is the only
/// place in the repo where an old-schema database is opened by the current code.
/// </summary>
public sealed class SearchHistoryEndpointTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;

    public SearchHistoryEndpointTests(AmetoWebAppFactory factory) => _factory = factory;

    /// <summary>
    /// The fixture is shared by the whole class and <see cref="TestAuthHandler"/> derives the
    /// username from the role header, so a role is how a test gets a history of its own. Tests
    /// that lean on a limit (ten recents, five pins) take a role nothing else in this class
    /// records under; the rest share admin and stay unique by query text.
    /// </summary>
    private HttpClient ClientAs(string role)
    {
        var client = _factory.CreateClient();
        client.DefaultRequestHeaders.Add(TestAuthHandler.RoleHeader, role);
        return client;
    }

    /// <summary>A query no other test in the class can collide with.</summary>
    private static string Unique(string tag) => $"{tag}:{Guid.NewGuid():N}";

    private sealed record HistoryResponse(string[] Pinned, string[] Recent);

    private static async Task<HistoryResponse> GetAsync(HttpClient client, string? scope)
    {
        var url  = scope is null ? "/api/search-history" : "/api/search-history?scope=" + Uri.EscapeDataString(scope);
        var resp = await client.GetAsync(url);
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        return (await resp.Content.ReadFromJsonAsync<HistoryResponse>())!;
    }

    private static async Task RecordAsync(HttpClient client, string query, string? scope)
    {
        // Body without a `scope` member at all when the test is exercising an old client,
        // not `scope: null` — the wire shape is the thing under test.
        var resp = scope is null
            ? await client.PostAsJsonAsync("/api/search-history", new { query })
            : await client.PostAsJsonAsync("/api/search-history", new { query, scope });
        Assert.Equal(HttpStatusCode.NoContent, resp.StatusCode);
    }

    private static async Task PinAsync(HttpClient client, string query, bool pinned, string scope)
    {
        var resp = await client.PutAsJsonAsync("/api/search-history/pin", new { query, pinned, scope });
        Assert.Equal(HttpStatusCode.NoContent, resp.StatusCode);
    }

    private static async Task DeleteAsync(HttpClient client, string query, string scope)
    {
        var resp = await client.DeleteAsync(
            $"/api/search-history?query={Uri.EscapeDataString(query)}&scope={Uri.EscapeDataString(scope)}");
        Assert.Equal(HttpStatusCode.NoContent, resp.StatusCode);
    }

    [Fact]
    public async Task RecordedQuery_ComesBackInItsOwnScopeAndNowhereElse()
    {
        var client   = ClientAs("admin");
        var logsQ    = Unique("roundtrip-logs");
        var tracesQ  = Unique("roundtrip-traces");
        var metricsQ = Unique("roundtrip-metrics");

        await RecordAsync(client, logsQ,    "logs");
        await RecordAsync(client, tracesQ,  "traces");
        await RecordAsync(client, metricsQ, "metrics");

        var logs    = await GetAsync(client, "logs");
        var traces  = await GetAsync(client, "traces");
        var metrics = await GetAsync(client, "metrics");

        Assert.Contains(logsQ,    logs.Recent);
        Assert.Contains(tracesQ,  traces.Recent);
        Assert.Contains(metricsQ, metrics.Recent);

        // The isolation, stated in both directions: a traces search is not a log search.
        Assert.DoesNotContain(tracesQ,  logs.Recent);
        Assert.DoesNotContain(metricsQ, logs.Recent);
        Assert.DoesNotContain(logsQ,    traces.Recent);
        Assert.DoesNotContain(logsQ,    metrics.Recent);
    }

    [Fact]
    public async Task Pinning_MovesTheEntryWithinItsScopeOnly()
    {
        // 'manager' is this test's own user: the pinned list is capped at five, and a
        // shared user could carry five pins from elsewhere and hide the one asserted here.
        var client = ClientAs("manager");
        var query  = Unique("pin-both-scopes");

        await RecordAsync(client, query, "logs");
        await RecordAsync(client, query, "traces");

        await PinAsync(client, query, pinned: true, scope: "traces");

        var traces = await GetAsync(client, "traces");
        var logs   = await GetAsync(client, "logs");

        Assert.Contains(query,       traces.Pinned);
        Assert.DoesNotContain(query, traces.Recent);
        // The same text in another scope is a different entry and did not move.
        Assert.Contains(query,       logs.Recent);
        Assert.DoesNotContain(query, logs.Pinned);

        await PinAsync(client, query, pinned: false, scope: "traces");

        traces = await GetAsync(client, "traces");
        Assert.Contains(query,       traces.Recent);
        Assert.DoesNotContain(query, traces.Pinned);
    }

    /// <summary>
    /// The pinned cap is enforced at the WRITE, by demotion. It used to be only a LIMIT in
    /// the read: a sixth pin was stored pinned=1 forever, shown in neither list (pinned is
    /// capped, recent filters pinned=0) and reachable by no route — invisible and immortal.
    /// Now the pin that falls off the end returns to recent: still visible, still deletable.
    /// </summary>
    [Fact]
    public async Task A_SixthPin_DemotesTheOldestPinToRecent_NothingGoesInvisible()
    {
        var client  = ClientAs("viewer");   // own user, same reasoning as the pin test above
        var queries = Enumerable.Range(0, 6).Select(i => Unique($"cap-{i}")).ToArray();

        foreach (var q in queries)
        {
            await RecordAsync(client, q, "metrics");
            await PinAsync(client, q, pinned: true, scope: "metrics");
        }

        var snap = await GetAsync(client, "metrics");

        Assert.Equal(5, snap.Pinned.Length);
        // The five NEWEST pins survive; the first one pinned was demoted, not hidden.
        Assert.DoesNotContain(queries[0], snap.Pinned);
        Assert.Contains(queries[0],       snap.Recent);
        // Every one of the six is visible SOMEWHERE — the invariant the old code broke.
        foreach (var q in queries)
            Assert.True(snap.Pinned.Contains(q) || snap.Recent.Contains(q),
                $"'{q}' is in neither list — invisible and undeletable");
    }

    [Fact]
    public async Task Delete_RemovesTheEntryFromOneScopeAndLeavesTheOther()
    {
        var client = ClientAs("admin");
        var query  = Unique("delete-both-scopes");

        await RecordAsync(client, query, "logs");
        await RecordAsync(client, query, "metrics");

        await DeleteAsync(client, query, "metrics");

        Assert.DoesNotContain(query, (await GetAsync(client, "metrics")).Recent);
        Assert.Contains(query,       (await GetAsync(client, "logs")).Recent);
    }

    [Fact]
    public async Task EleventhRecent_EvictsWithinItsScopeAndNotAcross()
    {
        // 'viewer' records nowhere else in this class, so the ten-row window below is
        // entirely this test's — the assertion is about the prune, not about neighbours.
        var client   = ClientAs("viewer");
        var tag      = Guid.NewGuid().ToString("N")[..8];
        var survivor = Unique("evict-metrics-survivor");

        await RecordAsync(client, survivor, "metrics");

        var queries = new string[11];
        for (int i = 0; i < queries.Length; i++)
        {
            queries[i] = $"evict-logs-{tag}-{i:D2}";
            await RecordAsync(client, queries[i], "logs");
            // Recency is ordered by an ISO timestamp string. Two records inside the same
            // tick would tie, and the tie-break is whatever SQLite feels like — which
            // would make the eviction assertion fail on an arbitrary run, not a wrong one.
            await Task.Delay(2);
        }

        var logs = await GetAsync(client, "logs");

        Assert.Equal(10, logs.Recent.Length);
        Assert.DoesNotContain(queries[0], logs.Recent);      // the oldest, pruned
        for (int i = 1; i < queries.Length; i++)
            Assert.Contains(queries[i], logs.Recent);

        // Eleven searches on Events must not cost the user their Metrics history.
        Assert.Contains(survivor, (await GetAsync(client, "metrics")).Recent);
    }

    [Fact]
    public async Task RequestWithoutAScope_IsExactlyScopeLogs()
    {
        // The back-compat contract: a client built before scopes existed sends neither a
        // body field nor a query param, and must keep landing in the Events history.
        var client = ClientAs("admin");
        var query  = Unique("no-scope");

        await RecordAsync(client, query, scope: null);

        Assert.Contains(query, (await GetAsync(client, null)).Recent);
        Assert.Contains(query, (await GetAsync(client, "logs")).Recent);
        Assert.DoesNotContain(query, (await GetAsync(client, "traces")).Recent);
    }

    [Theory]
    [InlineData("events")]   // the page's other name — the near miss most likely to be typed
    [InlineData("log")]
    [InlineData("Traces!")]
    public async Task UnknownScope_IsRefusedRatherThanQuietlyFiledUnderLogs(string scope)
    {
        var client = ClientAs("admin");
        var query  = Unique("bad-scope");

        var post = await client.PostAsJsonAsync("/api/search-history", new { query, scope });
        var pin  = await client.PutAsJsonAsync("/api/search-history/pin", new { query, pinned = true, scope });
        var get  = await client.GetAsync("/api/search-history?scope=" + Uri.EscapeDataString(scope));
        var del  = await client.DeleteAsync(
            $"/api/search-history?query={Uri.EscapeDataString(query)}&scope={Uri.EscapeDataString(scope)}");

        Assert.Equal(HttpStatusCode.BadRequest, post.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, pin.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, get.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, del.StatusCode);

        // A refused write is a write that did not happen: falling back to the default would
        // file the entry in a bucket the caller never looks at, and say nothing about it.
        Assert.DoesNotContain(query, (await GetAsync(client, "logs")).Recent);
    }

    [Fact]
    public async Task ScopeIsMatchedCaseInsensitivelyAndTrimmed()
    {
        var client = ClientAs("admin");
        var query  = Unique("mixed-case-scope");

        await RecordAsync(client, query, " TrAcEs ");

        // Stored lowercase, so the canonical spelling finds what the sloppy one wrote.
        Assert.Contains(query, (await GetAsync(client, "traces")).Recent);
        Assert.DoesNotContain(query, (await GetAsync(client, "logs")).Recent);
    }

    [Fact]
    public void OpeningAPreScopeDatabase_CopiesEveryRowInAsLogs()
    {
        // The only test that opens a database written by an older build. Everything the
        // v1 table holds was recorded by the Events page — events.store.ts is the only
        // caller that has ever existed — so 'logs' is the true scope for all of it, and
        // a user's pins must survive the upgrade or they look deleted.
        var dir = Path.Combine(Path.GetTempPath(), "Ameto-searchhistory-v1-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);

        try
        {
            var connectionString = $"Data Source={Path.Combine(dir, "Ameto.db")}";

            using (var seed = new SqliteConnection(connectionString))
            {
                seed.Open();
                using var cmd = seed.CreateCommand();
                // The pre-scope DDL, copied from the shipped schema — no scope column,
                // and the primary key this change could not ALTER.
                cmd.CommandText = """
                    CREATE TABLE search_history (
                        username   TEXT    NOT NULL,
                        query      TEXT    NOT NULL,
                        pinned     INTEGER NOT NULL DEFAULT 0,
                        updated_at TEXT    NOT NULL,
                        PRIMARY KEY (username, query)
                    );
                    INSERT INTO search_history (username, query, pinned, updated_at) VALUES
                        ('alice', 'level = Error',     1, '2026-08-01T10:00:00.0000000+00:00'),
                        ('alice', 'service = billing', 0, '2026-08-02T10:00:00.0000000+00:00'),
                        ('bob',   'timeout',           1, '2026-08-03T10:00:00.0000000+00:00');
                    """;
                cmd.ExecuteNonQuery();
            }

            SqliteConnection.ClearAllPools();

            _ = new AuthDatabase(dir);   // InitSchema creates v2, MigrateSchema fills it

            var copied = new List<(string User, string Scope, string Query, long Pinned)>();
            using (var read = new SqliteConnection(connectionString))
            {
                read.Open();
                using var cmd = read.CreateCommand();
                cmd.CommandText = "SELECT username, scope, query, pinned FROM search_history_v2 ORDER BY username, query";
                using (var r = cmd.ExecuteReader())
                {
                    while (r.Read()) copied.Add((r.GetString(0), r.GetString(1), r.GetString(2), r.GetInt64(3)));
                }

                // Anti-destructive by policy: the old table is left where it was, so a
                // downgrade to the previous build still finds the history it wrote.
                cmd.CommandText = "SELECT COUNT(*) FROM search_history";
                Assert.Equal(3L, Convert.ToInt64(cmd.ExecuteScalar()));
            }

            Assert.Equal(3, copied.Count);
            Assert.All(copied, row => Assert.Equal("logs", row.Scope));
            Assert.Equal(("alice", "logs", "level = Error",     1L), copied[0]);
            Assert.Equal(("alice", "logs", "service = billing", 0L), copied[1]);
            Assert.Equal(("bob",   "logs", "timeout",           1L), copied[2]);

            // The copy is gated on a marker, not on v2 being empty — and the difference is a
            // user's deletion staying deleted. Empty v2 out (a user removing their whole
            // history), reopen the database, and assert the frozen v1 rows do NOT come back:
            // an emptiness-gated copy resurrected them on every restart.
            using (var wipe = new SqliteConnection(connectionString))
            {
                wipe.Open();
                using var cmd = wipe.CreateCommand();
                cmd.CommandText = "DELETE FROM search_history_v2";
                cmd.ExecuteNonQuery();
            }
            SqliteConnection.ClearAllPools();

            _ = new AuthDatabase(dir);   // a restart, as far as the schema code can tell

            using (var recheck = new SqliteConnection(connectionString))
            {
                recheck.Open();
                using var cmd = recheck.CreateCommand();
                cmd.CommandText = "SELECT COUNT(*) FROM search_history_v2";
                Assert.Equal(0L, Convert.ToInt64(cmd.ExecuteScalar()));
            }
        }
        finally
        {
            // Pooled SQLite connections keep the file handle open; without this the
            // directory delete fails on Windows and the test litters %TEMP%.
            SqliteConnection.ClearAllPools();
            try { Directory.Delete(dir, recursive: true); } catch { /* best effort */ }
        }
    }
}
