using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Ameto.Mcp.Tools;

namespace Ameto.Integration.Tests;

/// <summary>
/// Published tools: a saved query, defined at run time, that the AI calls by name with
/// parameters — and nothing of the filter is the AI's to write.
/// </summary>
public sealed class McpPublishedToolTests : IClassFixture<McpEndpointTests.Host>
{
    private readonly McpEndpointTests.Host _host;

    public McpPublishedToolTests(McpEndpointTests.Host host) => _host = host;

    private static Dictionary<string, object?> Args(params (string Key, object? Value)[] pairs) =>
        pairs.ToDictionary(static p => p.Key, static p => p.Value);

    private static async Task<JsonElement> SaveAsync(HttpClient admin, object tool, HttpStatusCode expected = HttpStatusCode.OK)
    {
        using var response = await admin.PostAsJsonAsync("/api/mcp/tools", tool);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.True(response.StatusCode == expected, $"{response.StatusCode}: {body}");
        return body;
    }

    private static string NewName(string stem) => stem + "_" + Guid.NewGuid().ToString("N")[..8];

    // ── A tool exists the moment it is saved ──────────────────────────────────

    [Theory]
    [InlineData(McpTesting.Stateless)]
    [InlineData(McpTesting.Handshake)]
    public async Task ASavedTool_IsOfferedToTheProfilesGrantedIt_OnTheirNextList(string protocol)
    {
        var admin = await _host.AdminAsync();
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new(service, "Payment {PaymentId} declined: {Reason}", "Error")
                { Properties = { ["PaymentId"] = "pay_a", ["MerchantId"] = "m-1", ["Reason"] = "insufficient_funds", ["CardNumber"] = "4111111111111111", ["Amount"] = 100 } },
            new(service, "Payment {PaymentId} declined: {Reason}", "Error")
                { Properties = { ["PaymentId"] = "pay_b", ["MerchantId"] = "m-1", ["Reason"] = "expired_card", ["CardNumber"] = "5555555555554444", ["Amount"] = 200 } },
            new(service, "Payment {PaymentId} declined: {Reason}", "Error")
                { Properties = { ["PaymentId"] = "pay_c", ["MerchantId"] = "m-2", ["Reason"] = "insufficient_funds", ["CardNumber"] = "4012888888881881", ["Amount"] = 300 } });
        var (granted, grantedKey) = await McpTesting.CreateProfileAsync(admin, new { name = "granted " + protocol });
        var (_, otherKey)         = await McpTesting.CreateProfileAsync(admin, new { name = "not granted " + protocol });

        await using var client = await McpTesting.ConnectAsync(_host, grantedKey, protocol);
        await using var other  = await McpTesting.ConnectAsync(_host, otherKey, protocol);
        Assert.Equal(5, (await client.ListToolsAsync()).Count);

        string name = NewName("payment_declines");
        await SaveAsync(admin, new
        {
            name,
            description = "Declined payments of one merchant, with the decline reason.",
            parameters  = new object[]
            {
                new { name = "merchantId", type = "string", required = true, description = "Merchant id from the back office" },
                new { name = "reason", type = "enum", values = new[] { "insufficient_funds", "expired_card" } },
            },
            filter    = $"@service = '{service}' and MerchantId = {{merchantId}} and Reason = {{reason}}",
            fields    = new[] { "PaymentId", "Reason", "CardNumber" },
            published = true,
            profiles  = new[] { granted },
        });

        // No restart, no reconnect: the same client's next list has it.
        var tools = await client.ListToolsAsync();
        var tool  = Assert.Single(tools, t => t.Name == name);
        Assert.Equal(6, tools.Count);
        Assert.Equal("Declined payments of one merchant, with the decline reason.", tool.Description);
        var schema = tool.JsonSchema;
        Assert.Equal("merchantId", Assert.Single(schema.GetProperty("required").EnumerateArray()).GetString());
        Assert.Equal(2, schema.GetProperty("properties").GetProperty("reason").GetProperty("enum").GetArrayLength());
        Assert.DoesNotContain(await other.ListToolsAsync(), t => t.Name == name);

        // Called with the required parameter only: the clause of the optional one is dropped.
        var (all, allError) = await McpTesting.CallAsync(client, name, Args(("merchantId", "m-1")));
        Assert.False(allError, all);
        Assert.Contains("2 events", all);
        Assert.Contains("pay_a", all);
        Assert.Contains("pay_b", all);
        Assert.DoesNotContain("pay_c", all);
        Assert.DoesNotContain("Amount", all);                       // not among the returned fields
        Assert.DoesNotContain("4111111111111111", all);             // the masking rules apply to a published tool as to any
        Assert.Matches("h:[0-9a-f]{12}", all);

        var (one, _) = await McpTesting.CallAsync(client, name, Args(("merchantId", "m-1"), ("reason", "expired_card")));
        Assert.Contains("1 event", one);
        Assert.Contains("pay_b", one);

        var (refused, refusedError) = await McpTesting.CallAsync(other, name, Args(("merchantId", "m-1")));
        Assert.True(refusedError);
        Assert.Contains("not offered", refused);
    }

    [Fact]
    public async Task AnArgument_IsAValue_WhateverItContains()
    {
        var admin = await _host.AdminAsync();
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new(service, "Order {OrderId}", "Error") { Properties = { ["OrderId"] = "o-1", ["Owner"] = "alice" } },
            new(service, "Order {OrderId}")          { Properties = { ["OrderId"] = "o-2", ["Owner"] = "o'brien \\ co" } });
        var (profile, key) = await McpTesting.CreateProfileAsync(admin, new { name = "values" });
        string name = NewName("orders_of");
        await SaveAsync(admin, new
        {
            name,
            description = "Orders of one owner.",
            parameters  = new object[] { new { name = "owner", required = true } },
            filter      = $"@service = '{service}' and Owner = {{owner}}",
            published   = true,
            profiles    = new[] { profile },
        });
        await using var client = await McpTesting.ConnectAsync(_host, key);

        // An argument that tries to be a filter is searched for as the text it is.
        foreach (string attempt in (string[])["alice' or @l = 'Error", "x') or (1 = 1", "alice' or 'a' = 'a", "{owner}"])
        {
            var (text, isError) = await McpTesting.CallAsync(client, name, Args(("owner", attempt)));
            Assert.False(isError, text);
            Assert.Contains("No events match", text);
        }

        // And a value with a quote and a backslash in it finds the event that holds exactly that.
        var (found, foundError) = await McpTesting.CallAsync(client, name, Args(("owner", "o'brien \\ co")));
        Assert.False(foundError, found);
        Assert.Contains("o-2", found);
        Assert.DoesNotContain("o-1", found);

        var (missing, missingError) = await McpTesting.CallAsync(client, name, Args());
        Assert.True(missingError);
        Assert.Contains("'owner' is required", missing);

        var (unknown, unknownError) = await McpTesting.CallAsync(client, name, Args(("owner", "alice"), ("filter", "@l = 'Error'")));
        Assert.True(unknownError);
        Assert.Contains("'filter' is not a parameter", unknown);
    }

    [Fact]
    public async Task ATool_TakesATokenForAHiddenValue_AndADraftOrAProfileWithoutItIsNotOffered()
    {
        var admin = await _host.AdminAsync();
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new(service, "Paid {PaymentId}") { Properties = { ["PaymentId"] = "pay_x", ["CardNumber"] = "4111111111111111" } },
            new(service, "Paid {PaymentId}") { Properties = { ["PaymentId"] = "pay_y", ["CardNumber"] = "5555555555554444" } });
        var (profile, key) = await McpTesting.CreateProfileAsync(admin,
            new { name = "published only", toolsMode = "publishedOnly" });

        string byCard = NewName("payments_by_card"), draft = NewName("draft_tool");
        await SaveAsync(admin, new
        {
            name = byCard, description = "Payments made with one card.",
            parameters = new object[] { new { name = "card", required = true, description = "The card's token" } },
            filter = $"@service = '{service}' and CardNumber = {{card}}", published = true, profiles = new[] { profile },
        });
        await SaveAsync(admin, new
        {
            name = draft, description = "Not published yet.", filter = $"@service = '{service}'", published = false, profiles = new[] { profile },
        });
        await using var client = await McpTesting.ConnectAsync(_host, key);

        // A profile limited to published tools is offered exactly the published ones it was granted.
        Assert.Equal([byCard], (await client.ListToolsAsync()).Select(static t => t.Name));
        var (asDraft, draftError) = await McpTesting.CallAsync(client, draft, Args());
        Assert.True(draftError, asDraft);

        // The token comes from an answer another profile got; the value behind it never leaves the server.
        var (_, searchKey) = await McpTesting.CreateProfileAsync(admin, new { name = "searches for the token" });
        await using var searcher = await McpTesting.ConnectAsync(_host, searchKey);
        var (search, _) = await McpTesting.CallAsync(searcher, "logs_search", Args(("service", service)));
        string token = System.Text.RegularExpressions.Regex.Match(search, @"pay_x\s+(h:[0-9a-f]{12})").Groups[1].Value;
        Assert.NotEmpty(token);

        var (text, isError) = await McpTesting.CallAsync(client, byCard, Args(("card", token)));
        Assert.False(isError, text);
        Assert.Contains("pay_x", text);
        Assert.DoesNotContain("pay_y", text);
    }

    [Fact]
    public async Task AClientThatHoldsASession_IsToldWhenItsToolListChanges()
    {
        var admin = await _host.AdminAsync();
        var (profile, key) = await McpTesting.CreateProfileAsync(admin, new { name = "notified" });
        await using var client = await McpTesting.ConnectAsync(_host, key, McpTesting.Handshake);

        var told = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var listening = client.RegisterNotificationHandler(
            ModelContextProtocol.Protocol.NotificationMethods.ToolListChangedNotification,
            (_, _) => { told.TrySetResult(); return default; });
        Assert.Equal(5, (await client.ListToolsAsync()).Count);   // the server meets the session on its first request

        string name = NewName("announced");
        await SaveAsync(admin, new { name, description = "Errors.", filter = "@l = 'Error'", published = true, profiles = new[] { profile } });

        await told.Task.WaitAsync(TimeSpan.FromSeconds(20));
        Assert.Contains(await client.ListToolsAsync(), t => t.Name == name);
    }

    [Fact]
    public async Task ASession_AnswersToTheKeyOnEachRequest_NotToTheKeyThatOpenedIt()
    {
        var admin = await _host.AdminAsync();
        string mine = McpTesting.NewService(), theirs = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new(mine,   "Opened by the first {N}")  { Properties = { ["N"] = 1 } },
            new(theirs, "Seen by the second {N}")   { Properties = { ["N"] = 2 } });
        var (_, firstKey)  = await McpTesting.CreateProfileAsync(admin, new { name = "opens the session", services = new[] { mine } });
        var (_, secondKey) = await McpTesting.CreateProfileAsync(admin, new { name = "borrows the session", services = new[] { theirs } });

        await using var first = await McpTesting.ConnectAsync(_host, firstKey, McpTesting.Handshake);
        Assert.NotNull(first.SessionId);
        var (own, _) = await McpTesting.CallAsync(first, "logs_search", Args());
        Assert.Contains("Opened by the first", own);

        // The first profile's session id with the second profile's key: whatever the answer is —
        // a refusal, or the second profile's own events — it is never the first profile's.
        using var raw = _host.CreateClient();
        using var request = new HttpRequestMessage(HttpMethod.Post, "/mcp")
        {
            Content = JsonContent.Create(new
            {
                jsonrpc = "2.0", id = 7, method = "tools/call",
                @params = new { name = "logs_search", arguments = new { } },
            }),
        };
        request.Headers.Authorization = new("Bearer", secondKey);
        request.Headers.Add("Mcp-Session-Id", first.SessionId);
        request.Headers.Add("MCP-Protocol-Version", McpTesting.Handshake);
        request.Headers.Accept.ParseAdd("application/json");
        request.Headers.Accept.ParseAdd("text/event-stream");
        using var response = await raw.SendAsync(request);
        string body = await response.Content.ReadAsStringAsync();

        Assert.DoesNotContain("Opened by the first", body);
    }

    // ── What may be saved ─────────────────────────────────────────────────────

    [Theory]
    [InlineData("logs_search", "@l = 'Error'", "name", "built-in")]
    [InlineData("Bad Name", "@l = 'Error'", "name", "lowercase")]
    [InlineData("ok_name", "Owner = {nobody}", "filter", "not a declared parameter")]
    [InlineData("ok_name", "@l = 'Error'", "filter", "is not used in the filter")]
    [InlineData("ok_name", "Owner = {owner} or @l = 'Error'", "filter", "'or' at its top level")]
    [InlineData("ok_name", "Owner = {owner} and (", "filter", "does not parse")]
    [InlineData("ok_name", "select count(*) where Owner = {owner}", "filter", "aggregation")]
    [InlineData("ok_name", "Owner = {owner} and startsWith(CardNumber, '41')", "filter", "hidden field")]
    [InlineData("ok_name", "@mt = 'Order {owner} shipped'", "filter", "is not used in the filter")]   // a brace in quotes is text
    public async Task ADefinitionThatCannotWork_IsRefused_WithTheFieldItIsAbout(string name, string filter, string field, string says)
    {
        var admin = await _host.AdminAsync();
        var body = await SaveAsync(admin, new
        {
            name        = name == "ok_name" ? NewName("ok_name") : name,
            description = "A tool.",
            parameters  = new object[] { new { name = "owner" } },
            filter,
        }, HttpStatusCode.BadRequest);

        Assert.Equal(field, body.GetProperty("field").GetString());
        Assert.Contains(says, body.GetProperty("error").GetString());
    }

    [Fact]
    public async Task ATool_IsEdited_Renamed_Deleted_AndItsNameIsUnique()
    {
        var admin = await _host.AdminAsync();
        string name = NewName("editable");
        var saved = await SaveAsync(admin, new { name, description = "First.", filter = "@l = 'Error'" });
        string id = saved.GetProperty("id").GetString()!;
        Assert.False(saved.GetProperty("published").GetBoolean());

        var again = await SaveAsync(admin, new { name, description = "Same name.", filter = "@l = 'Error'" }, HttpStatusCode.BadRequest);
        Assert.Contains("already a tool named", again.GetProperty("error").GetString());

        string renamed = NewName("renamed");
        using var put = await admin.PutAsJsonAsync($"/api/mcp/tools/{id}", new
        {
            name = renamed, description = "Second.", filter = "@l = 'Warning'", order = "oldest", maxEvents = 10, defaultWindowMinutes = 15,
        });
        Assert.Equal(HttpStatusCode.OK, put.StatusCode);

        var list = await admin.GetFromJsonAsync<JsonElement>("/api/mcp/tools");
        Assert.Equal(5, list.GetProperty("builtIn").GetArrayLength());
        var mine = list.GetProperty("published").EnumerateArray().Single(t => t.GetProperty("id").GetString() == id);
        Assert.Equal(renamed, mine.GetProperty("name").GetString());
        Assert.Equal("oldest", mine.GetProperty("order").GetString());
        Assert.Equal(10, mine.GetProperty("maxEvents").GetInt32());
        Assert.Equal(15, mine.GetProperty("defaultWindowMinutes").GetInt32());

        using var delete = await admin.DeleteAsync($"/api/mcp/tools/{id}");
        Assert.Equal(HttpStatusCode.NoContent, delete.StatusCode);
        using var gone = await admin.DeleteAsync($"/api/mcp/tools/{id}");
        Assert.Equal(HttpStatusCode.NotFound, gone.StatusCode);
    }

    [Fact]
    public async Task TheEditorsTestRun_AnswersAsTheProfileWouldBeAnswered_WithoutSavingOrRecordingAnything()
    {
        var admin = await _host.AdminAsync();
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new McpTesting.Event(service, "Paid {PaymentId}") { Properties = { ["PaymentId"] = "pay_t", ["CardNumber"] = "4111111111111111" } });
        var (profileId, _) = await McpTesting.CreateProfileAsync(admin, new { name = "test run" });
        string name = NewName("unsaved");

        using var response = await admin.PostAsJsonAsync("/api/mcp/tools/test", new
        {
            profileId,
            tool = new
            {
                name, description = "Payments by id.",
                parameters = new object[] { new { name = "payment", required = true } },
                filter = $"@service = '{service}' and PaymentId = {{payment}}",
            },
            arguments = new { payment = "pay_t" },
        });
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();

        Assert.False(body.GetProperty("isError").GetBoolean());
        Assert.Equal($"@service = '{service}' and PaymentId = 'pay_t'", body.GetProperty("filter").GetString());
        Assert.Contains("pay_t", body.GetProperty("text").GetString());
        Assert.DoesNotContain("4111111111111111", body.GetProperty("text").GetString());
        Assert.Equal(1, body.GetProperty("events").GetInt32());
        Assert.Equal(1, body.GetProperty("masked").GetInt32());
        Assert.Equal(1, body.GetProperty("maskedBy").GetProperty("rules").GetProperty("CardNumber").GetInt32());
        Assert.Equal(name, body.GetProperty("definition").GetProperty("name").GetString());
        Assert.True(body.GetProperty("definition").GetProperty("inputSchema").GetProperty("properties").TryGetProperty("payment", out _));

        // Nothing was saved, and the run is not in the record of the AI's calls.
        var tools = await admin.GetFromJsonAsync<JsonElement>("/api/mcp/tools");
        Assert.DoesNotContain(tools.GetProperty("published").EnumerateArray(), t => t.GetProperty("name").GetString() == name);
        var activity = await admin.GetFromJsonAsync<JsonElement>($"/api/mcp/activity?profile={profileId}");
        Assert.Equal(0, activity.GetProperty("calls").GetArrayLength());
    }

    [Fact]
    public async Task ACallOfAPublishedTool_IsRecordedWithItsFilterAsBound_AndATokenInItStaysAToken()
    {
        var admin = await _host.AdminAsync();
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new McpTesting.Event(service, "Paid {PaymentId}")
                { Properties = { ["PaymentId"] = "pay_r", ["MerchantId"] = "m-7", ["CardNumber"] = "4111111111111111" } });
        var (profile, key) = await McpTesting.CreateProfileAsync(admin, new { name = "recorded filter" });
        string name = NewName("payments_of");
        await SaveAsync(admin, new
        {
            name,
            description = "Payments of one merchant, optionally made with one card.",
            parameters  = new object[] { new { name = "merchantId", required = true }, new { name = "card" } },
            filter      = $"@service = '{service}' and MerchantId = {{merchantId}} and CardNumber = {{card}}",
            published   = true,
            profiles    = new[] { profile },
        });
        await using var client = await McpTesting.ConnectAsync(_host, key);

        var (first, firstError) = await McpTesting.CallAsync(client, name, Args(("merchantId", "m-7")));
        Assert.False(firstError, first);
        string token = McpTesting.Token.Match(first).Value;
        Assert.NotEmpty(token);
        var (second, secondError) = await McpTesting.CallAsync(client, name, Args(("merchantId", "m-7"), ("card", token)));
        Assert.False(secondError, second);
        Assert.Contains("pay_r", second);

        // The arguments alone do not say what was asked; the record keeps the template with them in place.
        var activity = await admin.GetFromJsonAsync<JsonElement>($"/api/mcp/activity?profile={profile}");
        var calls = activity.GetProperty("calls").EnumerateArray().ToList();   // newest first
        Assert.Equal(2, calls.Count);
        Assert.Equal($"@service = '{service}' and MerchantId = 'm-7' and CardNumber = '{token}'", calls[0].GetProperty("filter").GetString());
        Assert.Equal($"@service = '{service}' and MerchantId = 'm-7'", calls[1].GetProperty("filter").GetString());

        // The value behind the token is in neither call's record.
        Assert.DoesNotContain("4111111111111111", activity.GetRawText());
    }

    [Fact]
    public async Task Usage_CountsCallsByTool_AndADeletedProfileIsWithdrawnFromItsTools()
    {
        var admin = await _host.AdminAsync();
        var (profile, key) = await McpTesting.CreateProfileAsync(admin, new { name = "usage" });
        string name = NewName("counted");
        var saved = await SaveAsync(admin, new { name, description = "Errors.", filter = "@l = 'Error'", published = true, profiles = new[] { profile } });
        await using (var client = await McpTesting.ConnectAsync(_host, key))
        {
            await McpTesting.CallAsync(client, name, Args());
            await McpTesting.CallAsync(client, name, Args());
        }

        var usage = await admin.GetFromJsonAsync<JsonElement>("/api/mcp/tools/usage");
        Assert.Equal(2, usage.GetProperty("calls").GetProperty(name).GetInt64());

        using var delete = await admin.DeleteAsync($"/api/mcp/profiles/{profile}");
        Assert.Equal(HttpStatusCode.NoContent, delete.StatusCode);
        var tools = await admin.GetFromJsonAsync<JsonElement>("/api/mcp/tools");
        var mine = tools.GetProperty("published").EnumerateArray().Single(t => t.GetProperty("id").GetString() == saved.GetProperty("id").GetString());
        Assert.Equal(0, mine.GetProperty("profiles").GetArrayLength());
    }

    // ── The template ──────────────────────────────────────────────────────────

    [Fact]
    public void ATemplate_ReadsPlaceholdersOutsideQuotesOnly_AndSplitsAtTopLevelAnds()
    {
        var template = new FilterTemplate(
            "@mt = 'Order {OrderId} and {more}' and Owner = {owner} and (Region = {region} or Region = 'and') and Tags in ['a and b', {tag}]");

        Assert.Equal(["owner", "region", "tag"], template.Placeholders);
        Assert.False(template.HasTopLevelOr);

        string? all = template.Bind(static name => $"'<{name}>'");
        Assert.Equal(
            "@mt = 'Order {OrderId} and {more}' and Owner = '<owner>' and (Region = '<region>' or Region = 'and') and Tags in ['a and b', '<tag>']",
            all);

        // A placeholder with no value takes its own clause with it, and only that one.
        Assert.Equal(
            "@mt = 'Order {OrderId} and {more}' and Owner = '<owner>' and Tags in ['a and b', '<tag>']",
            template.Bind(static name => name == "region" ? null : $"'<{name}>'"));
        Assert.Equal("@mt = 'Order {OrderId} and {more}'", template.Bind(static _ => null));
        Assert.Null(new FilterTemplate("Owner = {owner}").Bind(static _ => null));

        Assert.True(new FilterTemplate("Owner = {owner} or @l = 'Error'").HasTopLevelOr);
        Assert.False(new FilterTemplate("(Owner = {owner} or @l = 'Error') and Brand = 'android or ios'").HasTopLevelOr);
    }

    [Fact]
    public void Arguments_AreBoundByType_AndAWrongOneIsNamed()
    {
        var tool = new McpPublishedTool
        {
            Id = "t", Name = "typed", Description = "d",
            Filter = "Owner = {owner} and Elapsed > {minMs} and Retried = {retried} and Reason = {reason}",
            Parameters =
            [
                new() { Name = "owner", Required = true },
                new() { Name = "minMs", Type = McpToolParameter.Number, Default = "100" },
                new() { Name = "retried", Type = McpToolParameter.Boolean },
                new() { Name = "reason", Type = McpToolParameter.Enum, Values = ["a", "b"] },
            ],
        };
        Assert.Null(PublishedToolBinder.Validate(tool, []));

        static Dictionary<string, JsonElement> Json(string json) =>
            JsonDocument.Parse(json).RootElement.EnumerateObject().ToDictionary(static p => p.Name, static p => p.Value.Clone());

        // A default stands in for an optional parameter; without one its clause goes.
        Assert.Equal("Owner = 'it''s'".Replace("''", "\\'") + " and Elapsed > 100",
            PublishedToolBinder.Bind(tool, Json("""{"owner":"it's"}""")).Filter);
        Assert.Equal("Owner = 'x' and Elapsed > 2.5 and Retried = true and Reason = 'b'",
            PublishedToolBinder.Bind(tool, Json("""{"owner":"x","minMs":"2.5","retried":true,"reason":"B"}""")).Filter);

        Assert.Contains("takes a number", PublishedToolBinder.Bind(tool, Json("""{"owner":"x","minMs":"fast"}""")).Error);
        Assert.Contains("Its values: a, b", PublishedToolBinder.Bind(tool, Json("""{"owner":"x","reason":"c"}""")).Error);
        Assert.Contains("'owner' is required", PublishedToolBinder.Bind(tool, Json("{}")).Error);
        Assert.Contains("owner, minMs?, retried?, reason?", PublishedToolBinder.Bind(tool, Json("""{"nope":1}""")).Error);
    }
}
