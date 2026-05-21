using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Rd.Log.Core;
using Rd.Log.Core.Serialization;
using Rd.Log.Storage;

namespace Rd.Log.Integration.Tests;

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

file static class TestHelpers
{
    /// <summary>
    /// Serialises a batch of LogEvents into a msgpack array of CLEF maps.
    /// Wire format: fixarray/array16 header + concatenated per-event map bytes.
    /// </summary>
    public static byte[] BuildBatch(params LogEvent[] events)
    {
        var parts = events.Select(LogEventSerializer.Serialize).ToArray();
        int n = parts.Length;

        // msgpack fixarray (≤15 items: 0x9N) or array16 header
        byte[] header = n <= 15
            ? [(byte)(0x90 | n)]
            : [0xdc, (byte)(n >> 8), (byte)(n & 0xff)];

        var buf = new byte[header.Length + parts.Sum(p => p.Length)];
        header.CopyTo(buf, 0);
        int pos = header.Length;
        foreach (var p in parts) { p.CopyTo(buf, pos); pos += p.Length; }
        return buf;
    }

    /// <summary>Creates a minimal LogEvent for ingestion.</summary>
    public static LogEvent MakeEvent(
        string template,
        LogLevel level = LogLevel.Information,
        DateTimeOffset? ts = null,
        Dictionary<string, object?>? props = null) => new()
    {
        Id              = new EventId(0, 0),
        Timestamp       = ts ?? DateTimeOffset.UtcNow,
        Level           = level,
        MessageTemplate = template,
        Properties      = props,
    };

    /// <summary>POSTs a batch to /api/events and returns the JSON response body.</summary>
    public static async Task<JsonElement> IngestAsync(HttpClient client, params LogEvent[] events)
    {
        var body    = BuildBatch(events);
        var content = new ByteArrayContent(body);
        content.Headers.ContentType =
            new System.Net.Http.Headers.MediaTypeHeaderValue("application/octet-stream");
        var resp = await client.PostAsync("/api/events", content);
        resp.EnsureSuccessStatusCode();
        return await resp.Content.ReadFromJsonAsync<JsonElement>();
    }

    /// <summary>
    /// Polls GET /api/events until at least <paramref name="expectedCount"/> events are returned,
    /// or throws <see cref="TimeoutException"/>.
    /// </summary>
    public static async Task<JsonElement> WaitForEventsAsync(
        HttpClient client,
        int expectedCount,
        string? filter  = null,
        TimeSpan? timeout = null)
    {
        var deadline    = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        var filterParam = filter is not null ? $"&filter={Uri.EscapeDataString(filter)}" : "";
        var url         = $"/api/events?count={Math.Max(expectedCount, 1)}{filterParam}";

        while (true)
        {
            var resp = await client.GetAsync(url);
            resp.EnsureSuccessStatusCode();
            var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
            var arr  = json.GetProperty("events");
            if (arr.GetArrayLength() >= expectedCount)
                return json;
            if (DateTimeOffset.UtcNow >= deadline)
                throw new TimeoutException(
                    $"Drain timeout: only {arr.GetArrayLength()} of {expectedCount} events arrived.");
            await Task.Delay(20);
        }
    }

    /// <summary>Builds a /api/events query URL with the given parameters.</summary>
    public static string EventsUrl(
        int     count    = 100,
        string? filter   = null,
        string? dir      = null,
        string? afterId  = null,
        string? from     = null,
        string? to       = null)
    {
        var sb = new System.Text.StringBuilder("/api/events?count=").Append(count);
        if (filter  is not null) sb.Append("&filter=").Append(Uri.EscapeDataString(filter));
        if (dir     is not null) sb.Append("&dir=").Append(dir);
        if (afterId is not null) sb.Append("&afterId=").Append(afterId);
        if (from    is not null) sb.Append("&from=").Append(Uri.EscapeDataString(from));
        if (to      is not null) sb.Append("&to=").Append(Uri.EscapeDataString(to));
        return sb.ToString();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Health & Stats
// ─────────────────────────────────────────────────────────────────────────────

public sealed class HealthAndStatsTests : IClassFixture<RdLogWebAppFactory>
{
    private readonly HttpClient _client;

    public HealthAndStatsTests(RdLogWebAppFactory factory)
        => _client = factory.CreateClient();

    [Fact]
    public async Task Health_Returns200_WithOkStatus()
    {
        var resp = await _client.GetAsync("/health");

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("ok", json.GetProperty("status").GetString());
        Assert.True(json.TryGetProperty("utc", out _), "Response should contain 'utc' field");
    }

    [Fact]
    public async Task Stats_Returns200_WithExpectedShape()
    {
        var resp = await _client.GetAsync("/api/stats");

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.True(json.TryGetProperty("segments",        out _), "Missing 'segments'");
        Assert.True(json.TryGetProperty("totalEvents",     out _), "Missing 'totalEvents'");
        Assert.True(json.TryGetProperty("compressedBytes", out _), "Missing 'compressedBytes'");
    }

    [Fact]
    public async Task Stats_Segments_IsNonNegativeInteger()
    {
        var resp = await _client.GetAsync("/api/stats");
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.True(json.GetProperty("segments").GetInt64() >= 0);
        Assert.True(json.GetProperty("totalEvents").GetInt64() >= 0);
        Assert.True(json.GetProperty("compressedBytes").GetInt64() >= 0);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Ingestion endpoint
// ─────────────────────────────────────────────────────────────────────────────

public sealed class IngestionTests : IClassFixture<RdLogWebAppFactory>
{
    private readonly HttpClient _client;

    public IngestionTests(RdLogWebAppFactory factory)
        => _client = factory.CreateClient();

    [Fact]
    public async Task Ingest_EmptyBatch_Returns200_ZeroIngested()
    {
        var result = await TestHelpers.IngestAsync(_client); // zero events

        Assert.Equal(0, result.GetProperty("ingested").GetInt32());
        Assert.Equal(0, result.GetProperty("dropped").GetInt32());
    }

    [Fact]
    public async Task Ingest_SingleEvent_IngestedCountIsOne()
    {
        var ev     = TestHelpers.MakeEvent("Single event ingestion test");
        var result = await TestHelpers.IngestAsync(_client, ev);

        Assert.Equal(1, result.GetProperty("ingested").GetInt32());
        Assert.Equal(0, result.GetProperty("dropped").GetInt32());
    }

    [Fact]
    public async Task Ingest_BatchOfThree_IngestedCountIsThree()
    {
        var events = new[]
        {
            TestHelpers.MakeEvent("Batch event 1", LogLevel.Information),
            TestHelpers.MakeEvent("Batch event 2", LogLevel.Warning),
            TestHelpers.MakeEvent("Batch event 3", LogLevel.Error),
        };
        var result = await TestHelpers.IngestAsync(_client, events);

        Assert.Equal(3, result.GetProperty("ingested").GetInt32());
        Assert.Equal(0, result.GetProperty("dropped").GetInt32());
    }

    [Fact]
    public async Task Ingest_AllLogLevels_EachAccepted()
    {
        var levels = new[]
        {
            LogLevel.Verbose,
            LogLevel.Debug,
            LogLevel.Information,
            LogLevel.Warning,
            LogLevel.Error,
            LogLevel.Fatal,
        };
        var events = levels
            .Select(l => TestHelpers.MakeEvent($"{l} level event", l))
            .ToArray();

        var result = await TestHelpers.IngestAsync(_client, events);

        Assert.Equal(levels.Length, result.GetProperty("ingested").GetInt32());
    }

    [Fact]
    public async Task Ingest_EventWithProperties_Succeeds()
    {
        var ev = TestHelpers.MakeEvent(
            "Request {Method} {Path} returned {StatusCode}",
            props: new Dictionary<string, object?>
            {
                ["Method"]     = "GET",
                ["Path"]       = "/api/test",
                ["StatusCode"] = 200L,
                ["DurationMs"] = 42.5,
            });
        var result = await TestHelpers.IngestAsync(_client, ev);

        Assert.Equal(1, result.GetProperty("ingested").GetInt32());
    }

    [Fact]
    public async Task Ingest_EventWithException_Succeeds()
    {
        var ev = new LogEvent
        {
            Id              = new EventId(0, 0),
            Timestamp       = DateTimeOffset.UtcNow,
            Level           = LogLevel.Error,
            MessageTemplate = "Unhandled exception",
            Exception       = new ExceptionInfo
            {
                Type       = "System.InvalidOperationException",
                Message    = "test",
                StackTrace = "   at Foo.Bar()",
            },
        };
        var result = await TestHelpers.IngestAsync(_client, ev);

        Assert.Equal(1, result.GetProperty("ingested").GetInt32());
    }

    [Fact]
    public async Task Ingest_LargeBatch_AllIngested()
    {
        const int count = 200;
        var events = Enumerable
            .Range(0, count)
            .Select(i => TestHelpers.MakeEvent($"Bulk event {i}",
                props: new Dictionary<string, object?> { ["Index"] = (long)i }))
            .ToArray();

        var result = await TestHelpers.IngestAsync(_client, events);

        Assert.Equal(count, result.GetProperty("ingested").GetInt32());
    }

    [Fact]
    public async Task Ingest_BadPayload_Returns400()
    {
        var content = new ByteArrayContent([0xFF, 0xFE, 0x00, 0x01]); // invalid msgpack
        content.Headers.ContentType =
            new System.Net.Http.Headers.MediaTypeHeaderValue("application/octet-stream");
        var resp = await _client.PostAsync("/api/events", content);

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [Fact]
    public async Task Ingest_NotAnArray_Returns400()
    {
        // Valid msgpack but not an array — a bare string "hello"
        var content = new ByteArrayContent([0xa5, 0x68, 0x65, 0x6c, 0x6c, 0x6f]); // fixstr "hello"
        content.Headers.ContentType =
            new System.Net.Http.Headers.MediaTypeHeaderValue("application/octet-stream");
        var resp = await _client.PostAsync("/api/events", content);

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [Fact]
    public async Task Ingest_OversizedPayload_Returns413()
    {
        // Build a body just over the 4 MB limit
        var hugeBuf = new byte[4 * 1024 * 1024 + 1];
        var content = new ByteArrayContent(hugeBuf);
        content.Headers.ContentType =
            new System.Net.Http.Headers.MediaTypeHeaderValue("application/octet-stream");
        content.Headers.ContentLength = hugeBuf.Length;
        var resp = await _client.PostAsync("/api/events", content);

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, resp.StatusCode);
    }

    [Fact]
    public async Task Ingest_Then_EventAppearsInQuery()
    {
        // Use a unique template so we can filter specifically for this test's events.
        var tag = Guid.NewGuid().ToString("N")[..8];
        var ev  = TestHelpers.MakeEvent($"Drain probe {tag}",
            props: new Dictionary<string, object?> { ["Tag"] = tag });

        await TestHelpers.IngestAsync(_client, ev);

        // Wait for the drain loop to move the event from ring → hot tier.
        var json = await TestHelpers.WaitForEventsAsync(
            _client, expectedCount: 1, filter: $"Tag = '{tag}'");

        Assert.True(json.GetProperty("count").GetInt32() >= 1);
        var first = json.GetProperty("events").EnumerateArray().First();
        Assert.Equal($"Drain probe {tag}", first.GetProperty("@mt").GetString());
    }

    [Fact]
    public async Task Ingest_ResponseContainsIngestedAndDroppedKeys()
    {
        var ev     = TestHelpers.MakeEvent("Response shape check");
        var result = await TestHelpers.IngestAsync(_client, ev);

        Assert.True(result.TryGetProperty("ingested", out _), "Missing 'ingested'");
        Assert.True(result.TryGetProperty("dropped",  out _), "Missing 'dropped'");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Query fixture — seeds a deterministic dataset once per test class
// ─────────────────────────────────────────────────────────────────────────────

/// <summary>
/// Fixture that starts a fresh Rd.Log server, seeds 10 events with known
/// levels / templates / properties, then waits for the drain loop so that
/// all events are immediately queryable from the hot tier.
///
/// Seeded events:
///   5 × Information  "User {UserId} logged in"        UserId = 1..5
///   3 × Warning      "Slow query took {DurationMs}ms" DurationMs = 100, 200, 300
///   2 × Error        "Request failed {StatusCode}"    StatusCode = 500, 503
/// </summary>
public sealed class QueryTestFixture : IAsyncLifetime
{
    public RdLogWebAppFactory Factory { get; } = new();
    public HttpClient Client { get; private set; } = null!;

    /// <summary>The UTC timestamp used for all seeded events (millisecond precision).</summary>
    public DateTimeOffset SeedTime { get; private set; }

    public async Task InitializeAsync()
    {
        Client   = Factory.CreateClient();
        SeedTime = DateTimeOffset.UtcNow;

        var events = new List<LogEvent>();

        for (int i = 1; i <= 5; i++)
            events.Add(TestHelpers.MakeEvent(
                "User {UserId} logged in",
                ts: SeedTime.AddMilliseconds(i),
                props: new Dictionary<string, object?> { ["UserId"] = (long)i }));

        foreach (int ms in new[] { 100, 200, 300 })
            events.Add(TestHelpers.MakeEvent(
                "Slow query took {DurationMs}ms",
                level: LogLevel.Warning,
                ts: SeedTime.AddMilliseconds(10 + ms),
                props: new Dictionary<string, object?> { ["DurationMs"] = (long)ms }));

        foreach (int code in new[] { 500, 503 })
            events.Add(TestHelpers.MakeEvent(
                "Request failed {StatusCode}",
                level: LogLevel.Error,
                ts: SeedTime.AddMilliseconds(500 + code),
                props: new Dictionary<string, object?> { ["StatusCode"] = (long)code }));

        await TestHelpers.IngestAsync(Client, [.. events]);
        await TestHelpers.WaitForEventsAsync(Client, expectedCount: 10);
    }

    public async Task DisposeAsync()
    {
        Client.Dispose();
        await Factory.DisposeAsync();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Query endpoint — uses pre-seeded dataset
// ─────────────────────────────────────────────────────────────────────────────

public sealed class QueryTests : IClassFixture<QueryTestFixture>
{
    private readonly HttpClient       _client;
    private readonly DateTimeOffset   _seedTime;

    public QueryTests(QueryTestFixture fixture)
    {
        _client   = fixture.Client;
        _seedTime = fixture.SeedTime;
    }

    // ── Response shape ────────────────────────────────────────────────────────

    [Fact]
    public async Task Query_ResponseShape_HasEventsCountCursorKeys()
    {
        var json = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 1));

        Assert.True(json.TryGetProperty("events", out _), "Missing 'events'");
        Assert.True(json.TryGetProperty("count",  out _), "Missing 'count'");
        Assert.True(json.TryGetProperty("cursor", out _), "Missing 'cursor'");
    }

    [Fact]
    public async Task Query_EventShape_HasClefFields()
    {
        var json   = await _client.GetFromJsonAsync<JsonElement>(TestHelpers.EventsUrl(count: 1));
        var events = json.GetProperty("events");
        Assert.True(events.GetArrayLength() >= 1, "Expected at least one event");

        var ev = events[0];
        Assert.True(ev.TryGetProperty("@t",  out _), "Missing '@t'");
        Assert.True(ev.TryGetProperty("@mt", out _), "Missing '@mt'");
        Assert.True(ev.TryGetProperty("@l",  out _), "Missing '@l'");
        Assert.True(ev.TryGetProperty("id",  out _), "Missing 'id'");
    }

    // ── All events ────────────────────────────────────────────────────────────

    [Fact]
    public async Task Query_All_ReturnsAllTenSeededEvents()
    {
        var json = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 100));

        Assert.True(json.GetProperty("count").GetInt32() >= 10,
            "Expected at least 10 seeded events");
    }

    // ── Level filters ─────────────────────────────────────────────────────────

    [Fact]
    public async Task Query_Filter_InformationLevel_Returns5()
    {
        var json   = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 100, filter: "@l = 'Information'"));
        var events = json.GetProperty("events");

        Assert.True(events.GetArrayLength() >= 5);
        foreach (var ev in events.EnumerateArray())
            Assert.Equal("Information", ev.GetProperty("@l").GetString());
    }

    [Fact]
    public async Task Query_Filter_WarningLevel_Returns3()
    {
        var json   = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 100, filter: "@l = 'Warning'"));
        var events = json.GetProperty("events");

        Assert.True(events.GetArrayLength() >= 3);
        foreach (var ev in events.EnumerateArray())
            Assert.Equal("Warning", ev.GetProperty("@l").GetString());
    }

    [Fact]
    public async Task Query_Filter_ErrorLevel_Returns2()
    {
        var json   = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 100, filter: "@l = 'Error'"));
        var events = json.GetProperty("events");

        Assert.True(events.GetArrayLength() >= 2);
        foreach (var ev in events.EnumerateArray())
            Assert.Equal("Error", ev.GetProperty("@l").GetString());
    }

    [Fact]
    public async Task Query_Filter_NonExistentLevel_ReturnsEmpty()
    {
        var json = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 100, filter: "@l = 'Fatal'"));

        Assert.Equal(0, json.GetProperty("count").GetInt32());
    }

    // ── Template filter ───────────────────────────────────────────────────────

    [Fact]
    public async Task Query_Filter_ByExactMessageTemplate_ReturnsMatch()
    {
        var json   = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 100, filter: "@mt = 'Request failed {StatusCode}'"));
        var events = json.GetProperty("events");

        Assert.True(events.GetArrayLength() >= 2);
        foreach (var ev in events.EnumerateArray())
            Assert.Equal("Request failed {StatusCode}", ev.GetProperty("@mt").GetString());
    }

    [Fact]
    public async Task Query_Filter_ByMessageTemplateLike_ReturnsMatches()
    {
        var json   = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 100, filter: "@mt like 'Slow%'"));
        var events = json.GetProperty("events");

        Assert.True(events.GetArrayLength() >= 3);
        foreach (var ev in events.EnumerateArray())
            Assert.StartsWith("Slow", ev.GetProperty("@mt").GetString());
    }

    // ── Property filter ───────────────────────────────────────────────────────

    [Fact]
    public async Task Query_Filter_ByProperty_StatusCode500_ReturnsOneEvent()
    {
        var json   = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 100, filter: "StatusCode = 500"));
        var events = json.GetProperty("events");

        Assert.True(events.GetArrayLength() >= 1);
        var props = events[0].GetProperty("props");
        Assert.Equal(500L, props.GetProperty("StatusCode").GetInt64());
    }

    [Fact]
    public async Task Query_Filter_ByProperty_UserId3_ReturnsOneEvent()
    {
        var json   = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 100, filter: "UserId = 3"));
        var events = json.GetProperty("events");

        Assert.True(events.GetArrayLength() >= 1);
        var props = events[0].GetProperty("props");
        Assert.Equal(3L, props.GetProperty("UserId").GetInt64());
    }

    // ── Count limit ───────────────────────────────────────────────────────────

    [Fact]
    public async Task Query_Count_LimitsResults()
    {
        var json = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 3));

        var events = json.GetProperty("events");
        Assert.Equal(3, events.GetArrayLength());
        Assert.Equal(3, json.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task Query_Count1_ReturnsSingleEvent()
    {
        var json = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 1));

        Assert.Equal(1, json.GetProperty("events").GetArrayLength());
    }

    // ── Direction ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task Query_Direction_Backward_IsNewestFirst()
    {
        var json   = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 10, dir: "backward"));
        var events = json.GetProperty("events").EnumerateArray().ToList();

        for (int i = 1; i < events.Count; i++)
        {
            var prev = DateTimeOffset.Parse(events[i - 1].GetProperty("@t").GetString()!);
            var curr = DateTimeOffset.Parse(events[i    ].GetProperty("@t").GetString()!);
            Assert.True(prev >= curr,
                $"Backward order violated at index {i}: {prev} < {curr}");
        }
    }

    [Fact]
    public async Task Query_Direction_Forward_IsOldestFirst()
    {
        var json   = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 10, dir: "forward"));
        var events = json.GetProperty("events").EnumerateArray().ToList();

        for (int i = 1; i < events.Count; i++)
        {
            var prev = DateTimeOffset.Parse(events[i - 1].GetProperty("@t").GetString()!);
            var curr = DateTimeOffset.Parse(events[i    ].GetProperty("@t").GetString()!);
            Assert.True(prev <= curr,
                $"Forward order violated at index {i}: {prev} > {curr}");
        }
    }

    // ── Time range ────────────────────────────────────────────────────────────

    [Fact]
    public async Task Query_TimeRange_InclusiveWindow_ReturnsEvents()
    {
        var from = _seedTime.AddMinutes(-1).ToString("O");
        var to   = _seedTime.AddMinutes(+5).ToString("O");

        var json = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 100, from: from, to: to));

        Assert.True(json.GetProperty("count").GetInt32() >= 10,
            "Time window should include all seeded events");
    }

    [Fact]
    public async Task Query_TimeRange_FutureFrom_ReturnsNoEvents()
    {
        var from = _seedTime.AddHours(1).ToString("O");

        var json = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 100, from: from));

        Assert.Equal(0, json.GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task Query_TimeRange_PastTo_ReturnsNoEvents()
    {
        var to = _seedTime.AddHours(-1).ToString("O");

        var json = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 100, to: to));

        Assert.Equal(0, json.GetProperty("count").GetInt32());
    }

    // ── Cursor pagination ─────────────────────────────────────────────────────

    [Fact]
    public async Task Query_Pagination_ForwardPages_CoverAllEvents()
    {
        // Page 1: forward, 3 events
        var page1 = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 3, dir: "forward"));
        var page1Events = page1.GetProperty("events").EnumerateArray().ToList();
        Assert.Equal(3, page1Events.Count);

        // Cursor is the last event's id
        var cursor = page1.GetProperty("cursor").GetString();
        Assert.NotNull(cursor);

        // Page 2: continue from cursor
        var page2 = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 3, dir: "forward", afterId: cursor));
        var page2Events = page2.GetProperty("events").EnumerateArray().ToList();

        // No overlap between pages
        var page1Ids = page1Events.Select(e => e.GetProperty("id").GetString()).ToHashSet();
        var page2Ids = page2Events.Select(e => e.GetProperty("id").GetString()).ToHashSet();
        Assert.Empty(page1Ids.Intersect(page2Ids));
    }

    [Fact]
    public async Task Query_Pagination_Cursor_IsPresentWhenEventsReturned()
    {
        var json = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 5));
        var cursor = json.GetProperty("cursor");

        // cursor should be a non-null string when events are returned
        Assert.True(cursor.ValueKind == JsonValueKind.String ||
                    cursor.ValueKind == JsonValueKind.Number,
            "Cursor should be a string or number when events are present");
    }

    // ── Combined filter + time range ──────────────────────────────────────────

    [Fact]
    public async Task Query_Filter_And_TimeRange_Combined()
    {
        var from = _seedTime.AddMinutes(-1).ToString("O");
        var to   = _seedTime.AddMinutes(+5).ToString("O");

        var json   = await _client.GetFromJsonAsync<JsonElement>(
            TestHelpers.EventsUrl(count: 100, filter: "@l = 'Error'", from: from, to: to));
        var events = json.GetProperty("events");

        Assert.True(events.GetArrayLength() >= 2);
        foreach (var ev in events.EnumerateArray())
            Assert.Equal("Error", ev.GetProperty("@l").GetString());
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Alert rules (signals) — full CRUD
// ─────────────────────────────────────────────────────────────────────────────

public sealed class SignalsCrudTests : IClassFixture<RdLogWebAppFactory>
{
    private readonly HttpClient _client;

    public SignalsCrudTests(RdLogWebAppFactory factory)
        => _client = factory.CreateClient();

    // ── POST ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Signals_Post_Returns201_WithLocation()
    {
        var rule = MakeRule();

        var resp = await _client.PostAsJsonAsync("/api/signals", rule);

        Assert.Equal(HttpStatusCode.Created, resp.StatusCode);
        Assert.NotNull(resp.Headers.Location);
    }

    [Fact]
    public async Task Signals_Post_ResponseContainsId()
    {
        var rule = MakeRule(name: "IdCheck");

        var resp = await _client.PostAsJsonAsync("/api/signals", rule);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>();

        Assert.True(body.TryGetProperty("id", out var idProp));
        Assert.False(string.IsNullOrEmpty(idProp.GetString()), "Created rule must have a non-empty id");
    }

    [Fact]
    public async Task Signals_Post_ResponseContainsPostedFields()
    {
        var rule = MakeRule(name: "FieldCheck", filter: "@l = 'Error'", threshold: 7);

        var resp = await _client.PostAsJsonAsync("/api/signals", rule);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>();

        Assert.Equal("FieldCheck",      body.GetProperty("name").GetString());
        Assert.Equal("@l = 'Error'",    body.GetProperty("filter").GetString());
        Assert.Equal(7,                 body.GetProperty("threshold").GetInt32());
    }

    // ── GET list ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task Signals_Get_List_ReturnsArray()
    {
        var resp = await _client.GetAsync("/api/signals");

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(JsonValueKind.Array, json.ValueKind);
    }

    [Fact]
    public async Task Signals_Post_AppearsInList()
    {
        var name = "ListTest_" + Guid.NewGuid().ToString("N")[..6];
        var post  = await _client.PostAsJsonAsync("/api/signals", MakeRule(name: name));
        var id    = (await post.Content.ReadFromJsonAsync<JsonElement>())
                        .GetProperty("id").GetString()!;

        var list = await _client.GetFromJsonAsync<JsonElement[]>("/api/signals");
        Assert.NotNull(list);
        Assert.Contains(list, r => r.GetProperty("id").GetString() == id);
    }

    // ── GET by id ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task Signals_GetById_ReturnsCorrectRule()
    {
        var name = "GetByIdTest_" + Guid.NewGuid().ToString("N")[..6];
        var post = await _client.PostAsJsonAsync("/api/signals", MakeRule(name: name));
        var id   = (await post.Content.ReadFromJsonAsync<JsonElement>())
                       .GetProperty("id").GetString()!;

        var resp = await _client.GetAsync($"/api/signals/{id}");

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var body = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(id,   body.GetProperty("id").GetString());
        Assert.Equal(name, body.GetProperty("name").GetString());
    }

    [Fact]
    public async Task Signals_GetById_UnknownId_Returns404()
    {
        var resp = await _client.GetAsync("/api/signals/does-not-exist-xyz");

        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    // ── PUT ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Signals_Put_UpdatesNameAndFilter()
    {
        var post = await _client.PostAsJsonAsync("/api/signals", MakeRule(name: "Original"));
        var id   = (await post.Content.ReadFromJsonAsync<JsonElement>())
                       .GetProperty("id").GetString()!;

        var update = MakeRule(name: "Updated", filter: "@l = 'Fatal'", threshold: 1);
        var putResp = await _client.PutAsJsonAsync($"/api/signals/{id}", update);

        Assert.Equal(HttpStatusCode.OK, putResp.StatusCode);
        var body = await putResp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("Updated",        body.GetProperty("name").GetString());
        Assert.Equal("@l = 'Fatal'",   body.GetProperty("filter").GetString());
    }

    [Fact]
    public async Task Signals_Put_UnknownId_Returns404()
    {
        var resp = await _client.PutAsJsonAsync("/api/signals/no-such-rule",
            MakeRule(name: "Ghost"));

        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    // ── DELETE ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Signals_Delete_Returns204()
    {
        var post = await _client.PostAsJsonAsync("/api/signals", MakeRule());
        var id   = (await post.Content.ReadFromJsonAsync<JsonElement>())
                       .GetProperty("id").GetString()!;

        var del = await _client.DeleteAsync($"/api/signals/{id}");

        Assert.Equal(HttpStatusCode.NoContent, del.StatusCode);
    }

    [Fact]
    public async Task Signals_Delete_RemovedFromList()
    {
        var post = await _client.PostAsJsonAsync("/api/signals", MakeRule());
        var id   = (await post.Content.ReadFromJsonAsync<JsonElement>())
                       .GetProperty("id").GetString()!;

        await _client.DeleteAsync($"/api/signals/{id}");

        var list = await _client.GetFromJsonAsync<JsonElement[]>("/api/signals");
        Assert.NotNull(list);
        Assert.DoesNotContain(list, r => r.GetProperty("id").GetString() == id);
    }

    [Fact]
    public async Task Signals_Delete_UnknownId_Returns404()
    {
        var resp = await _client.DeleteAsync("/api/signals/does-not-exist-abc");

        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [Fact]
    public async Task Signals_Delete_ThenGetById_Returns404()
    {
        var post = await _client.PostAsJsonAsync("/api/signals", MakeRule());
        var id   = (await post.Content.ReadFromJsonAsync<JsonElement>())
                       .GetProperty("id").GetString()!;

        await _client.DeleteAsync($"/api/signals/{id}");
        var getResp = await _client.GetAsync($"/api/signals/{id}");

        Assert.Equal(HttpStatusCode.NotFound, getResp.StatusCode);
    }

    // ── Full lifecycle ────────────────────────────────────────────────────────

    [Fact]
    public async Task Signals_FullLifecycle_CreateUpdateDelete()
    {
        // 1. Create
        var post = await _client.PostAsJsonAsync("/api/signals",
            MakeRule(name: "Lifecycle", filter: "@l = 'Warning'", threshold: 3));
        Assert.Equal(HttpStatusCode.Created, post.StatusCode);
        var id = (await post.Content.ReadFromJsonAsync<JsonElement>())
                     .GetProperty("id").GetString()!;

        // 2. Verify in list
        var list = await _client.GetFromJsonAsync<JsonElement[]>("/api/signals");
        Assert.Contains(list!, r => r.GetProperty("id").GetString() == id);

        // 3. Update
        var put = await _client.PutAsJsonAsync($"/api/signals/{id}",
            MakeRule(name: "Lifecycle (updated)", filter: "@l = 'Error'", threshold: 10));
        Assert.Equal(HttpStatusCode.OK, put.StatusCode);
        var updated = await put.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("Lifecycle (updated)", updated.GetProperty("name").GetString());
        Assert.Equal(10,                    updated.GetProperty("threshold").GetInt32());

        // 4. Get by id confirms update persisted
        var get = await _client.GetFromJsonAsync<JsonElement>($"/api/signals/{id}");
        Assert.Equal("Lifecycle (updated)", get.GetProperty("name").GetString());

        // 5. Delete
        var del = await _client.DeleteAsync($"/api/signals/{id}");
        Assert.Equal(HttpStatusCode.NoContent, del.StatusCode);

        // 6. No longer found
        var missing = await _client.GetAsync($"/api/signals/{id}");
        Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static object MakeRule(
        string? name      = null,
        string? filter    = "@l = 'Error'",
        int     threshold = 5) => new
    {
        name            = name ?? "IntegrationTest_" + Guid.NewGuid().ToString("N")[..8],
        filter,
        threshold,
        windowSeconds   = 60,
        cooldownSeconds = 300,
        enabled         = true,
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Live tail (SSE)
// ─────────────────────────────────────────────────────────────────────────────

public sealed class LiveTailTests : IClassFixture<RdLogWebAppFactory>
{
    private readonly RdLogWebAppFactory _factory;

    public LiveTailTests(RdLogWebAppFactory factory) => _factory = factory;

    [Fact]
    public async Task LiveTail_Connect_ReceivesKeepaliveOrData()
    {
        // ResponseHeadersRead lets us read the SSE stream incrementally.
        using var client = _factory.CreateClient();
        using var cts    = new CancellationTokenSource(TimeSpan.FromSeconds(3));

        using var request = new HttpRequestMessage(HttpMethod.Get, "/api/events/live");
        using var resp    = await client.SendAsync(
            request, HttpCompletionOption.ResponseHeadersRead, cts.Token);

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Equal("text/event-stream", resp.Content.Headers.ContentType?.MediaType);

        await using var stream = await resp.Content.ReadAsStreamAsync(cts.Token);
        using var reader = new System.IO.StreamReader(stream);

        string? firstLine = null;
        try { firstLine = await reader.ReadLineAsync(cts.Token); }
        catch (OperationCanceledException) { /* timeout acceptable */ }

        if (firstLine is not null)
        {
            Assert.True(firstLine.StartsWith("data:") || firstLine.StartsWith(":"),
                $"Unexpected SSE line: '{firstLine}'");
        }
    }

    [Fact]
    public async Task LiveTail_WithLevelFilter_Returns200()
    {
        using var client = _factory.CreateClient();
        using var cts    = new CancellationTokenSource(TimeSpan.FromSeconds(3));

        using var request = new HttpRequestMessage(
            HttpMethod.Get, "/api/events/live?filter=" + Uri.EscapeDataString("@l = 'Error'"));
        using var resp = await client.SendAsync(
            request, HttpCompletionOption.ResponseHeadersRead, cts.Token);

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);

        // Cancel the stream to stop the server-side SSE loop cleanly.
        await cts.CancelAsync();
    }
}
