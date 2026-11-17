using System.Buffers;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.RegularExpressions;
using MessagePack;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;

namespace Ameto.Integration.Tests;

/// <summary>What the MCP tests share: profiles, clients, and events with a service of their own.</summary>
internal static class McpTesting
{
    /// <summary>The two generations of the protocol the endpoint serves: stateless, and the initialize handshake.</summary>
    public const string Stateless = "2026-07-28", Handshake = "2025-06-18";

    public static async Task<(string Id, string Key)> CreateProfileAsync(HttpClient admin, object profile)
    {
        using var response = await admin.PostAsJsonAsync("/api/mcp/profiles", profile);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        return (body.GetProperty("profile").GetProperty("id").GetString()!, body.GetProperty("key").GetString()!);
    }

    /// <summary>An MCP client of the SDK's own, speaking <paramref name="protocol"/> to the test host with a profile key.</summary>
    public static async Task<McpClient> ConnectAsync(AmetoWebAppFactory factory, string key, string protocol = Stateless, string path = "/mcp")
    {
        var http = factory.CreateClient();
        var transport = new HttpClientTransport(new HttpClientTransportOptions
        {
            Endpoint          = new Uri(http.BaseAddress!, path),
            TransportMode     = HttpTransportMode.StreamableHttp,
            AdditionalHeaders = new Dictionary<string, string> { ["Authorization"] = "Bearer " + key },
        }, http, loggerFactory: null, ownsHttpClient: true);
        return await McpClient.CreateAsync(transport, new McpClientOptions { ProtocolVersion = protocol });
    }

    public static async Task<(string Text, bool IsError)> CallAsync(McpClient client, string tool, Dictionary<string, object?> arguments)
    {
        var result = await client.CallToolAsync(tool, arguments);
        return (string.Join("\n", result.Content.OfType<TextContentBlock>().Select(static b => b.Text)), result.IsError == true);
    }

    /// <summary>One event of a CLEF batch.</summary>
    public sealed record Event(string Service, string Template, string Level = "Information")
    {
        public Dictionary<string, object?> Properties { get; init; } = [];
        public DateTimeOffset?             At         { get; init; }
        public string?                     Exception  { get; init; }
    }

    /// <summary>Ingests the events over the ordinary CLEF route, each with its <c>@service</c>.</summary>
    public static async Task IngestAsync(HttpClient client, params Event[] events)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var w = new MessagePackWriter(buffer);
        w.WriteArrayHeader(events.Length);
        foreach (var e in events)
        {
            w.WriteMapHeader(4 + e.Properties.Count + (e.Exception is null ? 0 : 1));
            w.Write("@t");       w.Write((e.At ?? DateTimeOffset.UtcNow).ToString("O"));
            w.Write("@mt");      w.Write(e.Template);
            w.Write("@l");       w.Write(e.Level);
            w.Write("@service"); w.Write(e.Service);
            if (e.Exception is not null) { w.Write("@x"); w.Write(e.Exception); }
            foreach (var (key, value) in e.Properties)
            {
                w.Write(key);
                switch (value)
                {
                    case null:     w.WriteNil(); break;
                    case string s: w.Write(s); break;
                    case int i:    w.Write(i); break;
                    case long l:   w.Write(l); break;
                    case bool b:   w.Write(b); break;
                    case double d: w.Write(d); break;
                    case Dictionary<string, object?> nested:
                        w.WriteMapHeader(nested.Count);
                        foreach (var (k, v) in nested) { w.Write(k); w.Write(v?.ToString()); }
                        break;
                    // A list, each element as it is: an object keeps the types of its values.
                    case List<object?> list:
                        w.WriteArrayHeader(list.Count);
                        foreach (object? item in list) WriteItem(ref w, item);
                        break;
                    default: w.Write(value.ToString()); break;
                }
            }
        }
        w.Flush();

        using var content = new ByteArrayContent(buffer.WrittenSpan.ToArray());
        content.Headers.ContentType = new("application/x-msgpack");
        using var response = await client.PostAsync("/api/events", content);
        response.EnsureSuccessStatusCode();
        var answer = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(events.Length, answer.GetProperty("ingested").GetInt32());
    }

    private static void WriteItem(ref MessagePackWriter w, object? item)
    {
        switch (item)
        {
            case null:     w.WriteNil(); break;
            case string s: w.Write(s); break;
            case int i:    w.Write(i); break;
            case long l:   w.Write(l); break;
            case bool b:   w.Write(b); break;
            case double d: w.Write(d); break;
            case Dictionary<string, object?> map:
                w.WriteMapHeader(map.Count);
                foreach (var (k, v) in map) { w.Write(k); WriteItem(ref w, v); }
                break;
            case List<object?> list:
                w.WriteArrayHeader(list.Count);
                foreach (object? inner in list) WriteItem(ref w, inner);
                break;
            default: w.Write(item.ToString()); break;
        }
    }

    public static string NewService() => "svc-" + Guid.NewGuid().ToString("N")[..10];

    public static readonly Regex Token = new("h:[0-9a-f]{12}", RegexOptions.Compiled);
}

/// <summary>
/// The AI endpoint end to end: a profile key, an MCP client, the tools, and what an answer may
/// and may not contain.
/// </summary>
public sealed class McpEndpointTests : IClassFixture<McpEndpointTests.Host>
{
    public sealed class Host : AmetoWebAppFactory
    {
        private readonly SemaphoreSlim _once = new(1, 1);
        private bool _ruled;

        /// <summary>The field rules every test of the class runs under, written once.</summary>
        public async Task<HttpClient> AdminAsync()
        {
            var admin = CreateClient();
            await _once.WaitAsync();
            try
            {
                if (_ruled) return admin;
                foreach (var rule in new object[]
                {
                    new { field = "CardNumber",   action = "hash" },
                    new { field = "Pan",          action = "keepLast4" },
                    new { field = "Customer.Iin", action = "hash" },
                })
                {
                    using var response = await admin.PostAsJsonAsync("/api/mcp/rules", rule);
                    Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                }
                _ruled = true;
                return admin;
            }
            finally { _once.Release(); }
        }
    }

    private readonly Host _host;

    public McpEndpointTests(Host host) => _host = host;

    private static Dictionary<string, object?> Args(params (string Key, object? Value)[] pairs) =>
        pairs.ToDictionary(static p => p.Key, static p => p.Value);

    // ── Who gets in ───────────────────────────────────────────────────────────

    [Fact]
    public async Task WithoutAProfileKey_TheEndpointAnswers401()
    {
        // The test host authenticates every request as an admin by default. The endpoint's
        // policy names its own scheme, so that admin is not enough here.
        using var client = _host.CreateClient();
        using var response = await client.PostAsJsonAsync("/mcp", new { jsonrpc = "2.0", id = 1, method = "tools/list" });
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task AnIngestOrReadApiKey_IsNotAProfileKey()
    {
        using var client = _host.CreateClient();
        using var request = new HttpRequestMessage(HttpMethod.Post, "/mcp")
        {
            Content = JsonContent.Create(new { jsonrpc = "2.0", id = 1, method = "tools/list" }),
        };
        request.Headers.Authorization = new("Bearer", AmetoWebAppFactory.TestApiKey);
        using var response = await client.SendAsync(request);
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Theory]
    [InlineData(McpTesting.Stateless)]
    [InlineData(McpTesting.Handshake)]
    public async Task AProfile_IsOfferedTheBuiltInTools_OnEitherProtocolGeneration(string protocol)
    {
        var admin = await _host.AdminAsync();
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "lists tools " + protocol });

        await using var client = await McpTesting.ConnectAsync(_host, key, protocol);
        var tools = await client.ListToolsAsync();

        // The client really is on the generation the test names: a session for the handshake
        // one, none for the stateless one, and the server's instructions on both.
        Assert.Equal(protocol, client.NegotiatedProtocolVersion);
        Assert.Equal(protocol == McpTesting.Handshake, client.SessionId is not null);
        Assert.Contains("Hidden values", client.ServerInstructions);

        Assert.Equal(
            ["logs_aggregate", "logs_context", "logs_fields", "logs_overview", "logs_search"],
            tools.Select(static t => t.Name).Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task AProfileLimitedToPublishedTools_IsOfferedNoBuiltInOne_AndCannotCallOne()
    {
        var admin = await _host.AdminAsync();
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "published only", toolsMode = "publishedOnly" });

        await using var client = await McpTesting.ConnectAsync(_host, key);
        Assert.Empty(await client.ListToolsAsync());

        var (text, isError) = await McpTesting.CallAsync(client, "logs_search", Args());
        Assert.True(isError);
        Assert.Contains("not offered", text);
    }

    [Fact]
    public async Task WithoutFreeFilters_TheToolsThatTakeOne_AreWithdrawn()
    {
        var admin = await _host.AdminAsync();
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "no free filters", freeFilters = false });

        await using var client = await McpTesting.ConnectAsync(_host, key);
        var names = (await client.ListToolsAsync()).Select(static t => t.Name).ToList();

        Assert.DoesNotContain("logs_search", names);
        Assert.DoesNotContain("logs_aggregate", names);
        Assert.Contains("logs_overview", names);
    }

    // ── What an answer contains ───────────────────────────────────────────────

    [Fact]
    public async Task Search_HidesRuledFieldsAndDetectedValues_AndATokenFindsItsEvents()
    {
        var admin = await _host.AdminAsync();
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new(service, "Payment {PaymentId} for card {CardNumber} declined", "Error")
                { Properties = { ["PaymentId"] = "pay_one", ["CardNumber"] = "4111111111111111", ["Pan"] = "5555555555554444" } },
            new(service, "Payment {PaymentId} for card {CardNumber} declined", "Error")
                { Properties = { ["PaymentId"] = "pay_two", ["CardNumber"] = "5555555555554444", ["Password"] = "hunter2" } },
            new(service, "Callback to a.serikov@example.com failed for {PaymentId}", "Warning")
                { Properties = { ["PaymentId"] = "pay_one", ["Note"] = "card 4111 1111 1111 1111 on file" } });
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "search" });
        await using var client = await McpTesting.ConnectAsync(_host, key);

        var (text, isError) = await McpTesting.CallAsync(client, "logs_search", Args(("service", service)));

        Assert.False(isError, text);
        Assert.Contains("3 events", text);
        Assert.Contains("Payment {PaymentId} for card {CardNumber} declined", text);   // the template, not a rendered message
        Assert.DoesNotContain("4111111111111111", text);                               // a field rule
        Assert.DoesNotContain("4111 1111 1111 1111", text);                            // the card detector, in a free string
        Assert.DoesNotContain("a.serikov@example.com", text);                          // the email detector, in the template
        Assert.DoesNotContain("hunter2", text);                                        // a seeded Remove rule
        Assert.DoesNotContain("Password", text);
        Assert.Contains("****4444", text);                                             // keepLast4

        // One value, one token: the card of pay_one as a field and inside the note.
        string token = Regex.Match(text, @"pay_one\s+(h:[0-9a-f]{12})").Groups[1].Value;
        Assert.NotEmpty(token);
        Assert.Contains("card " + token + " on file", text);

        // The token is a search key although the AI never held the value.
        var (byToken, tokenError) = await McpTesting.CallAsync(client, "logs_search",
            Args(("service", service), ("filter", $"CardNumber = '{token}'")));
        Assert.False(tokenError, byToken);
        Assert.Contains("1 event", byToken);
        Assert.Contains("pay_one", byToken);
        Assert.DoesNotContain("pay_two", byToken);
    }

    [Fact]
    public async Task Search_RefusesAPatternMatchOnAHiddenField_AndAnUnknownToken()
    {
        var admin = await _host.AdminAsync();
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "refusals" });
        await using var client = await McpTesting.ConnectAsync(_host, key);

        var (like, likeError) = await McpTesting.CallAsync(client, "logs_search", Args(("filter", "CardNumber like '4111%'")));
        Assert.True(likeError);
        Assert.Contains("hidden field", like);

        var (nested, nestedError) = await McpTesting.CallAsync(client, "logs_search", Args(("filter", "startsWith(Customer.Iin, '9001')")));
        Assert.True(nestedError);
        Assert.Contains("Customer.Iin", nested);

        var (unknown, unknownError) = await McpTesting.CallAsync(client, "logs_search", Args(("filter", "CardNumber = 'h:000000000000'")));
        Assert.True(unknownError);
        Assert.Contains("not a token this server remembers", unknown);

        var (syntax, syntaxError) = await McpTesting.CallAsync(client, "logs_search", Args(("filter", "Level =")));
        Assert.True(syntaxError);
        Assert.Contains("Invalid filter", syntax);
    }

    [Fact]
    public async Task AProfile_SeesOnlyItsServices_AndNoProfileSeesTheAuditTrail()
    {
        var admin = await _host.AdminAsync();
        string mine = McpTesting.NewService(), other = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new(mine,  "Mine {N}")   { Properties = { ["N"] = 1 } },
            new(other, "Theirs {N}") { Properties = { ["N"] = 2 } });
        var (_, scopedKey) = await McpTesting.CreateProfileAsync(admin, new { name = "scoped", services = new[] { mine } });
        var (_, openKey)   = await McpTesting.CreateProfileAsync(admin, new { name = "open" });

        await using var scoped = await McpTesting.ConnectAsync(_host, scopedKey);
        var (all, _) = await McpTesting.CallAsync(scoped, "logs_search", Args());
        Assert.Contains("Mine {N}", all);
        Assert.DoesNotContain("Theirs {N}", all);

        var (asked, _) = await McpTesting.CallAsync(scoped, "logs_search", Args(("filter", $"@service = '{other}'")));
        Assert.Contains("No events match", asked);
        var (words, _) = await McpTesting.CallAsync(scoped, "logs_search", Args(("filter", "Theirs")));
        Assert.Contains("No events match", words);
        var (named, namedError) = await McpTesting.CallAsync(scoped, "logs_search", Args(("service", other)));
        Assert.True(namedError);
        var (counted, _) = await McpTesting.CallAsync(scoped, "logs_aggregate", Args(("query", "select count(*) group by @service")));
        Assert.DoesNotContain(other, counted);

        // Every call above was audited under the service ameto-mcp, and none of that is an AI's to read.
        await using var open = await McpTesting.ConnectAsync(_host, openKey);
        var (audit, _) = await McpTesting.CallAsync(open, "logs_search", Args(("filter", "@service = 'ameto-mcp'")));
        Assert.Contains("No events match", audit);
        var (auditCount, _) = await McpTesting.CallAsync(open, "logs_aggregate", Args(("query", "select count(*) group by @service")));
        Assert.DoesNotContain("ameto-mcp", auditCount);
    }

    [Fact]
    public async Task Aggregate_HidesGroupKeys_AndRefusesAnAggregateOverAHiddenField()
    {
        var admin = await _host.AdminAsync();
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new(service, "Paid {CardNumber}") { Properties = { ["CardNumber"] = "4111111111111111", ["Amount"] = 100 } },
            new(service, "Paid {CardNumber}") { Properties = { ["CardNumber"] = "4111111111111111", ["Amount"] = 300 } },
            new(service, "Paid {CardNumber}") { Properties = { ["CardNumber"] = "5555555555554444", ["Amount"] = 50 } });
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "aggregate" });
        await using var client = await McpTesting.ConnectAsync(_host, key);

        var (text, isError) = await McpTesting.CallAsync(client, "logs_aggregate",
            Args(("query", $"select count(*), sum(Amount) where @service = '{service}' group by CardNumber")));

        Assert.False(isError, text);
        Assert.DoesNotContain("4111111111111111", text);
        Assert.Equal(2, McpTesting.Token.Matches(text).Select(static m => m.Value).Distinct().Count());
        Assert.Matches(@"h:[0-9a-f]{12}\s+2\s+400", text);

        var (min, minError) = await McpTesting.CallAsync(client, "logs_aggregate",
            Args(("query", $"select min(CardNumber) where @service = '{service}'")));
        Assert.True(minError);
        Assert.Contains("hidden field", min);
    }

    [Fact]
    public async Task Context_GivesTheEventInFull_ByTheRefOfASearchRow()
    {
        var admin = await _host.AdminAsync();
        string service = McpTesting.NewService();
        var at = DateTimeOffset.UtcNow.AddSeconds(-30);
        await McpTesting.IngestAsync(admin,
            new(service, "Before {N}") { Properties = { ["N"] = 1 }, At = at.AddSeconds(-2) },
            new(service, "Declined {PaymentId}", "Error")
            {
                Properties = { ["PaymentId"] = "pay_ctx", ["CardNumber"] = "4111111111111111" },
                Exception  = "System.TimeoutException: gateway did not answer a.serikov@example.com",
                At         = at,
            },
            new(service, "After {N}") { Properties = { ["N"] = 3 }, At = at.AddSeconds(2) });
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "context" });
        await using var client = await McpTesting.ConnectAsync(_host, key);

        var (search, _) = await McpTesting.CallAsync(client, "logs_search", Args(("service", service), ("levels", new[] { "Error" })));
        string reference = Regex.Match(search, @"^([0-9a-z]+(?:[+-][0-9a-z]+)?)\s+\d\d:\d\d:\d\d\.\d{3}", RegexOptions.Multiline).Groups[1].Value;
        Assert.NotEmpty(reference);

        var (text, isError) = await McpTesting.CallAsync(client, "logs_context", Args(("ref", reference)));

        Assert.False(isError, text);
        Assert.Contains("@mt       Declined {PaymentId}", text);
        Assert.Contains("PaymentId = pay_ctx", text);
        Assert.Matches(@"CardNumber = h:[0-9a-f]{12}", text);
        Assert.DoesNotContain("a.serikov@example.com", text);     // the exception text goes through the detectors too
        Assert.Contains("Before {N}", text);
        Assert.Contains("After {N}", text);
        Assert.True(text.IndexOf("Before {N}", StringComparison.Ordinal) < text.IndexOf("After {N}", StringComparison.Ordinal));

        var (missing, missingError) = await McpTesting.CallAsync(client, "logs_context", Args(("ref", "zzzzzzzz")));
        Assert.True(missingError, missing);
    }

    [Fact]
    public async Task Overview_And_Fields_DescribeAService()
    {
        var admin = await _host.AdminAsync();
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new(service, "Paid {Amount}")                { Properties = { ["Amount"] = 10, ["CardNumber"] = "4111111111111111" } },
            new(service, "Paid {Amount}")                { Properties = { ["Amount"] = 20, ["CardNumber"] = "4111111111111111" } },
            new(service, "Declined {Reason}", "Error")   { Properties = { ["Reason"] = "insufficient_funds" } });
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "overview" });
        await using var client = await McpTesting.ConnectAsync(_host, key);

        var (overview, overviewError) = await McpTesting.CallAsync(client, "logs_overview", Args(("service", service)));
        Assert.False(overviewError, overview);
        Assert.Contains("3 events", overview);
        Assert.Contains("Error 1", overview);
        Assert.Contains("Information 2", overview);
        Assert.Contains("Paid {Amount}", overview);

        var (fields, fieldsError) = await McpTesting.CallAsync(client, "logs_fields", Args(("service", service)));
        Assert.False(fieldsError, fields);
        Assert.Matches(@"CardNumber\s+2\s+string\s+= only\s+h:[0-9a-f]{12}", fields);
        Assert.Matches(@"Amount\s+2\s+number", fields);
        Assert.Contains("filter syntax", fields);
    }

    // ── What is kept of a call ────────────────────────────────────────────────

    [Fact]
    public async Task EveryCall_IsRecorded_WithWhatItHid()
    {
        var admin = await _host.AdminAsync();
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new McpTesting.Event(service, "Paid {CardNumber}") { Properties = { ["CardNumber"] = "4111111111111111" } });
        var (profileId, key) = await McpTesting.CreateProfileAsync(admin, new { name = "audited" });
        await using var client = await McpTesting.ConnectAsync(_host, key);

        await McpTesting.CallAsync(client, "logs_search", Args(("service", service)));
        await McpTesting.CallAsync(client, "logs_search", Args(("filter", "CardNumber like '4%'")));

        var activity = await admin.GetFromJsonAsync<JsonElement>($"/api/mcp/activity?profile={profileId}");
        var calls = activity.GetProperty("calls").EnumerateArray().ToList();
        Assert.Equal(2, calls.Count);

        var refused = calls[0];   // newest first
        Assert.Equal("Refused", refused.GetProperty("status").GetString());
        Assert.Contains("hidden field", refused.GetProperty("refusal").GetString());
        Assert.Equal("masking", refused.GetProperty("refusalKind").GetString());
        Assert.Equal("CardNumber like '4%'", refused.GetProperty("filter").GetString());

        var ok = calls[1];
        Assert.Equal("logs_search", ok.GetProperty("tool").GetString());
        Assert.Equal("OK", ok.GetProperty("status").GetString());
        Assert.Equal(JsonValueKind.Null, ok.GetProperty("filter").ValueKind);   // it was asked without one
        Assert.Equal("audited", ok.GetProperty("profile").GetString());
        Assert.Equal(1, ok.GetProperty("events").GetInt64());
        Assert.Equal(1, ok.GetProperty("masked").GetInt64());
        Assert.Equal(1, ok.GetProperty("maskedBy").GetProperty("rules").GetProperty("CardNumber").GetInt64());
        Assert.Equal(JsonValueKind.Null, ok.GetProperty("refusalKind").ValueKind);
        Assert.Contains(service, ok.GetProperty("arguments").GetString());

        var profiles = await admin.GetFromJsonAsync<JsonElement>("/api/mcp/profiles");
        var mine = profiles.EnumerateArray().Single(p => p.GetProperty("id").GetString() == profileId);
        Assert.NotEqual(JsonValueKind.Null, mine.GetProperty("lastCallAt").ValueKind);
    }

    [Theory]
    [InlineData(McpTesting.Stateless)]
    [InlineData(McpTesting.Handshake)]
    public async Task ACallsRecord_SaysWhichProtocolRevisionTheClientSpoke(string protocol)
    {
        var admin = await _host.AdminAsync();
        var (profileId, key) = await McpTesting.CreateProfileAsync(admin, new { name = "client of " + protocol });
        await using var client = await McpTesting.ConnectAsync(_host, key, protocol);
        await McpTesting.CallAsync(client, "logs_overview", Args());

        var activity = await admin.GetFromJsonAsync<JsonElement>($"/api/mcp/activity?profile={profileId}");
        string? recorded = Assert.Single(activity.GetProperty("calls").EnumerateArray()).GetProperty("client").GetString();
        Assert.NotNull(recorded);
        Assert.EndsWith("MCP " + protocol, recorded);
    }

    [Fact]
    public async Task ARefusal_IsRecordedWithItsKind()
    {
        var admin = await _host.AdminAsync();
        string seen = McpTesting.NewService(), unseen = McpTesting.NewService();
        var (profileId, key) = await McpTesting.CreateProfileAsync(admin,
            new { name = "refused four ways", services = new[] { seen }, tools = new[] { "logs_search" }, callsPerMinute = 4 });
        await using var client = await McpTesting.ConnectAsync(_host, key);

        await McpTesting.CallAsync(client, "logs_search", Args(("filter", "@l = ")));                  // does not parse
        await McpTesting.CallAsync(client, "logs_search", Args(("filter", "CardNumber like '41%'")));  // would spell a hidden value out
        await McpTesting.CallAsync(client, "logs_search", Args(("service", unseen)));                  // not this profile's service
        await McpTesting.CallAsync(client, "logs_fields", Args());                                     // not this profile's tool
        var (limited, _) = await McpTesting.CallAsync(client, "logs_search", Args());
        // Five calls in a row straddle a minute boundary at most once: the fifth is the limit's, or it ran.
        bool hitTheLimit = limited.Contains("calls for this minute");

        var activity = await admin.GetFromJsonAsync<JsonElement>($"/api/mcp/activity?profile={profileId}");
        var kinds = activity.GetProperty("calls").EnumerateArray()
                            .Select(static c => c.GetProperty("refusalKind").GetString()).Reverse().ToList();   // oldest first

        // A call the profile is not offered is refused before it is counted against the minute.
        Assert.Equal(["input", "masking", "scope", "scope"], kinds.Take(4));
        Assert.Equal(hitTheLimit ? "limit" : null, kinds[4]);
    }

    [Fact]
    public async Task TheSummary_AddsUpTheCallsOfTheWindow_ByStatusByRuleAndByDetector()
    {
        var admin = await _host.AdminAsync();
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin, new McpTesting.Event(service, "Paid {CardNumber}")
        {
            Properties = { ["CardNumber"] = "4111111111111111", ["Note"] = "receipt to a.serikov@example.com" },
        });
        var rules = await admin.GetFromJsonAsync<JsonElement>("/api/mcp/rules");
        string cardRule = rules.EnumerateArray().Single(static r => r.GetProperty("field").GetString() == "CardNumber")
                               .GetProperty("id").GetString()!;
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "summed up" });
        await using var client = await McpTesting.ConnectAsync(_host, key);

        var before = await admin.GetFromJsonAsync<JsonElement>("/api/mcp/summary");
        await McpTesting.CallAsync(client, "logs_search", Args(("service", service)));                // one event, two values hidden
        await McpTesting.CallAsync(client, "logs_search", Args(("filter", "CardNumber like '4%'")));  // refused by the masking rules
        var after = await admin.GetFromJsonAsync<JsonElement>("/api/mcp/summary");

        // The tests of this class share a host and run one at a time, so the difference is this test's own.
        long Grew(params string[] at)
        {
            static long Read(JsonElement from, string[] at)
            {
                foreach (string name in at)
                    if (from.ValueKind != JsonValueKind.Object || !from.TryGetProperty(name, out from)) return 0;
                return from.GetInt64();
            }
            return Read(after, at) - Read(before, at);
        }

        Assert.Equal(24, after.GetProperty("hours").GetInt32());
        Assert.False(after.GetProperty("partial").GetBoolean());
        Assert.Equal(2, Grew("calls"));
        Assert.Equal(1, Grew("profiles"));
        Assert.Equal(1, Grew("events"));
        Assert.Equal(2, Grew("masked"));
        Assert.Equal(1, Grew("maskedBy", "rules"));
        Assert.Equal(1, Grew("maskedBy", "detectors"));
        Assert.Equal(0, Grew("maskedBy", "strict"));
        Assert.Equal(1, Grew("statuses", "OK"));
        Assert.Equal(1, Grew("statuses", "Refused"));
        Assert.Equal(1, Grew("refusals", "masking"));
        Assert.Equal(1, Grew("rules", cardRule));
        Assert.Equal(1, Grew("detectors", "email"));
        Assert.True(after.GetProperty("medianEvents").GetInt64() >= 1);
    }

    [Fact]
    public async Task AProfile_IsHeldToItsCallsPerMinute()
    {
        var admin = await _host.AdminAsync();
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "two a minute", callsPerMinute = 2 });
        await using var client = await McpTesting.ConnectAsync(_host, key);

        // Three calls in a row straddle a minute boundary at most once, so of four at least one is refused.
        int refused = 0;
        for (int i = 0; i < 4; i++)
        {
            var (text, isError) = await McpTesting.CallAsync(client, "logs_overview", Args());
            if (isError && text.Contains("calls for this minute")) refused++;
        }
        Assert.InRange(refused, 1, 2);
    }

    // ── The switch and the settings API ───────────────────────────────────────

    [Fact]
    public async Task SwitchedOffInSettings_TheEndpointAnswers503_UntilSwitchedOn()
    {
        var admin = await _host.AdminAsync();
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "switch" });
        try
        {
            using var off = await admin.PutAsJsonAsync("/api/mcp/settings", new { enabled = false });
            Assert.Equal(HttpStatusCode.OK, off.StatusCode);

            using var request = new HttpRequestMessage(HttpMethod.Post, "/mcp")
            {
                Content = JsonContent.Create(new { jsonrpc = "2.0", id = 1, method = "tools/list" }),
            };
            request.Headers.Authorization = new("Bearer", key);
            using var response = await admin.SendAsync(request);
            Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        }
        finally
        {
            using var on = await admin.PutAsJsonAsync("/api/mcp/settings", new { enabled = true });
            Assert.Equal(HttpStatusCode.OK, on.StatusCode);
        }

        await using var client = await McpTesting.ConnectAsync(_host, key);
        Assert.NotEmpty(await client.ListToolsAsync());
    }

    [Fact]
    public async Task ABuiltInToolSwitchedOffInSettings_IsOfferedToNoProfile_UntilSwitchedOn()
    {
        var admin = await _host.AdminAsync();
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "tool switch" });
        await using var client = await McpTesting.ConnectAsync(_host, key);
        Assert.Equal(5, (await client.ListToolsAsync()).Count);

        static bool Enabled(JsonElement tools, string name) =>
            tools.EnumerateArray().Single(t => t.GetProperty("name").GetString() == name).GetProperty("enabled").GetBoolean();

        try
        {
            using var off = await admin.PutAsJsonAsync("/api/mcp/settings", new { disabledTools = new[] { "logs_fields" } });
            Assert.Equal(HttpStatusCode.OK, off.StatusCode);
            var section = await off.Content.ReadFromJsonAsync<JsonElement>();
            Assert.False(Enabled(section.GetProperty("tools"), "logs_fields"));
            Assert.True(Enabled(section.GetProperty("tools"), "logs_search"));

            // The profile still has the tool switched on; the server's switch is above it.
            var names = (await client.ListToolsAsync()).Select(static t => t.Name).ToList();
            Assert.DoesNotContain("logs_fields", names);
            Assert.Equal(4, names.Count);

            // Not listed, and not callable by a client that remembers the name.
            var (text, isError) = await McpTesting.CallAsync(client, "logs_fields", Args());
            Assert.True(isError);
            Assert.Contains("not offered", text);

            // A setting that is not sent keeps its value.
            using var other = await admin.PutAsJsonAsync("/api/mcp/settings", new { enabled = true });
            Assert.False(Enabled((await other.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("tools"), "logs_fields"));
            var listed = await admin.GetFromJsonAsync<JsonElement>("/api/mcp/tools");
            Assert.False(Enabled(listed.GetProperty("builtIn"), "logs_fields"));

            using var unknown = await admin.PutAsJsonAsync("/api/mcp/settings", new { disabledTools = new[] { "logs_everything" } });
            Assert.Equal(HttpStatusCode.BadRequest, unknown.StatusCode);
            Assert.Equal("disabledTools", (await unknown.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("field").GetString());
        }
        finally
        {
            using var on = await admin.PutAsJsonAsync("/api/mcp/settings", new { disabledTools = Array.Empty<string>() });
            Assert.Equal(HttpStatusCode.OK, on.StatusCode);
        }

        Assert.Contains("logs_fields", (await client.ListToolsAsync()).Select(static t => t.Name));
    }

    [Fact]
    public async Task ARotatedKey_ReplacesTheOldOne_AndADeletedProfileIsGone()
    {
        var admin = await _host.AdminAsync();
        var (id, oldKey) = await McpTesting.CreateProfileAsync(admin, new { name = "rotated" });

        using var rotate = await admin.PostAsync($"/api/mcp/profiles/{id}/rotate-key", null);
        string newKey = (await rotate.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("key").GetString()!;
        Assert.NotEqual(oldKey, newKey);

        Assert.Equal(HttpStatusCode.Unauthorized, await ListStatusAsync(oldKey));
        await using (var client = await McpTesting.ConnectAsync(_host, newKey))
            Assert.NotEmpty(await client.ListToolsAsync());

        using var delete = await admin.DeleteAsync($"/api/mcp/profiles/{id}");
        Assert.Equal(HttpStatusCode.NoContent, delete.StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, await ListStatusAsync(newKey));

        async Task<HttpStatusCode> ListStatusAsync(string key)
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, "/mcp")
            {
                Content = JsonContent.Create(new { jsonrpc = "2.0", id = 1, method = "tools/list" }),
            };
            request.Headers.Authorization = new("Bearer", key);
            using var response = await admin.SendAsync(request);
            return response.StatusCode;
        }
    }

    [Fact]
    public async Task Preview_ShowsAnEventAsStoredAndAsSent()
    {
        var admin = await _host.AdminAsync();
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new McpTesting.Event(service, "Paid {CardNumber}") { Properties = { ["CardNumber"] = "4111111111111111", ["Amount"] = 10 } });

        using var response = await admin.PostAsJsonAsync("/api/mcp/preview", new { filter = $"@service = '{service}'" });
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();

        Assert.True(body.GetProperty("found").GetBoolean());
        Assert.Equal("4111111111111111", body.GetProperty("stored").GetProperty("props").GetProperty("CardNumber").GetString());
        var sent = body.GetProperty("sent").GetProperty("properties").EnumerateArray()
                       .ToDictionary(static p => p.GetProperty("name").GetString()!, static p => p.GetProperty("value").GetString()!);
        Assert.Matches("^h:[0-9a-f]{12}$", sent["CardNumber"]);
        Assert.Equal("10", sent["Amount"]);
        Assert.Equal(1, body.GetProperty("masked").GetProperty("total").GetInt32());
        Assert.Equal(1, body.GetProperty("masked").GetProperty("rules").GetProperty("CardNumber").GetInt32());
        Assert.Empty(body.GetProperty("masked").GetProperty("strict").EnumerateObject());

        // Strict: everything not on the allow-list is hidden, the amount included.
        using var strict = await admin.PostAsJsonAsync("/api/mcp/preview", new { filter = $"@service = '{service}'", mode = "strict" });
        var strictSent = (await strict.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("sent").GetProperty("properties")
                             .EnumerateArray().ToDictionary(static p => p.GetProperty("name").GetString()!, static p => p.GetProperty("value").GetString()!);
        Assert.Matches("^h:[0-9a-f]{12}$", strictSent["Amount"]);
    }

    // ── A profile's tools, from the profile's side ────────────────────────────

    [Fact]
    public async Task AProfilesPublishedTools_AreSetAsOneList_AndItsToolListIsTheOneItsKeyGets()
    {
        var admin = await _host.AdminAsync();
        async Task<string> SaveToolAsync(string stem, bool published)
        {
            using var response = await admin.PostAsJsonAsync("/api/mcp/tools", new
            {
                name = stem + "_" + Guid.NewGuid().ToString("N")[..8], description = "Errors.", filter = "@l = 'Error'", published,
            });
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            return (await response.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("id").GetString()!;
        }
        string first = await SaveToolAsync("granted_first", published: true),
               second = await SaveToolAsync("granted_second", published: true),
               draft = await SaveToolAsync("granted_draft", published: false);

        // Granted as the profile is created, drafts included: a draft is granted and not yet offered.
        using var created = await admin.PostAsJsonAsync("/api/mcp/profiles", new
        {
            name = "granted from its own side", freeFilters = false, publishedTools = new[] { first, draft },
        });
        var body = await created.Content.ReadFromJsonAsync<JsonElement>();
        string id = body.GetProperty("profile").GetProperty("id").GetString()!, key = body.GetProperty("key").GetString()!;
        Assert.Equal(new[] { first, draft }.Order(StringComparer.Ordinal), Granted(body.GetProperty("profile")));

        async Task<JsonElement> ToolAsync(string toolId) =>
            (await admin.GetFromJsonAsync<JsonElement>("/api/mcp/tools")).GetProperty("published").EnumerateArray()
                .Single(t => t.GetProperty("id").GetString() == toolId);
        // In the order of the ids: the answer lists them in the tools' own order, which is not the request's.
        static List<string> Granted(JsonElement profile) =>
            [.. profile.GetProperty("publishedTools").EnumerateArray().Select(static t => t.GetString()!).Order(StringComparer.Ordinal)];
        static List<string?> Names(JsonElement tools) =>
            [.. tools.EnumerateArray().Select(static t => t.GetProperty("name").GetString())];

        Assert.Contains(id, (await ToolAsync(first)).GetProperty("profiles").EnumerateArray().Select(static p => p.GetString()));
        Assert.Empty((await ToolAsync(second)).GetProperty("profiles").EnumerateArray());

        // The list Settings shows is the list the key gets: three built-in tools (no free filters) and one published.
        await using var client = await McpTesting.ConnectAsync(_host, key);
        var offered = await admin.GetFromJsonAsync<JsonElement>($"/api/mcp/profiles/{id}/tools");
        Assert.Equal((await client.ListToolsAsync()).Select(static t => t.Name), Names(offered.GetProperty("tools")));
        Assert.Equal(4, offered.GetProperty("tools").GetArrayLength());
        var published = offered.GetProperty("tools").EnumerateArray().Single(static t => !t.GetProperty("builtIn").GetBoolean());
        Assert.Equal((await ToolAsync(first)).GetProperty("name").GetString(), published.GetProperty("name").GetString());
        Assert.Equal(JsonValueKind.Object, published.GetProperty("inputSchema").ValueKind);

        // The whole list again: what it leaves out is withdrawn, what it adds is granted.
        using var changed = await admin.PutAsJsonAsync($"/api/mcp/profiles/{id}", new
        {
            name = "granted from its own side", freeFilters = false, publishedTools = new[] { second },
        });
        Assert.Equal([second], Granted(await changed.Content.ReadFromJsonAsync<JsonElement>()));
        Assert.Empty((await ToolAsync(first)).GetProperty("profiles").EnumerateArray());
        Assert.Contains((await ToolAsync(second)).GetProperty("name").GetString(), (await client.ListToolsAsync()).Select(static t => t.Name));

        // A save that says nothing about the tools leaves them as they are.
        using var renamed = await admin.PutAsJsonAsync($"/api/mcp/profiles/{id}", new { name = "renamed", freeFilters = false });
        Assert.Equal([second], Granted(await renamed.Content.ReadFromJsonAsync<JsonElement>()));

        using var unknown = await admin.PutAsJsonAsync($"/api/mcp/profiles/{id}", new { name = "renamed", publishedTools = new[] { "no-such-tool" } });
        Assert.Equal(HttpStatusCode.BadRequest, unknown.StatusCode);
        Assert.Equal("publishedTools", (await unknown.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("field").GetString());

        using var missing = await admin.GetAsync("/api/mcp/profiles/no-such-profile/tools");
        Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
    }
}
