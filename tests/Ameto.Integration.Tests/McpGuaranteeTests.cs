using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.RegularExpressions;
using Ameto.Mcp.Tools;
using ModelContextProtocol.Client;

namespace Ameto.Integration.Tests;

/// <summary>
/// What the AI endpoint promises, tried the way the first review of it broke each promise:
/// through a running host and a real MCP client. A hidden value does not leave; a filter or a
/// table cannot be made to spell one out; a published tool's argument changes no filter; and the
/// record of calls is the server's own and says what was asked.
/// </summary>
public sealed class McpGuaranteeTests(McpEndpointTests.Host host) : IClassFixture<McpEndpointTests.Host>
{
    private static Task<(string Text, bool IsError)> Call(McpClient client, string tool, params (string Key, object? Value)[] args) =>
        McpTesting.CallAsync(client, tool, args.ToDictionary(static a => a.Key, static a => a.Value));

    private static async Task Rule(HttpClient admin, object rule)
    {
        using var response = await admin.PostAsJsonAsync("/api/mcp/rules", rule);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    private static async Task<List<JsonElement>> Calls(HttpClient admin, string profileId, int count = 200)
    {
        var activity = await admin.GetFromJsonAsync<JsonElement>($"/api/mcp/activity?profile={profileId}&count={count}");
        return [.. activity.GetProperty("calls").EnumerateArray()];
    }

    private static string? Text(JsonElement call, string name) =>
        call.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    // ── A token never becomes its value where an answer could show it ─────────

    [Fact]
    public async Task ATokenNamedAsAColumn_StaysAToken()
    {
        var admin = await host.AdminAsync();
        await Rule(admin, new { field = "AccountNo", action = "hash" });
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new(service, "Paid {PaymentId}") { Properties = { ["PaymentId"] = "pay_one", ["CardNumber"] = "4111111111111111", ["AccountNo"] = 6456116363L } },
            new(service, "Paid {PaymentId}") { Properties = { ["PaymentId"] = "pay_two", ["CardNumber"] = "5555555555554444", ["AccountNo"] = 6456116363L } });
        var (profileId, key) = await McpTesting.CreateProfileAsync(admin, new { name = "token as a column", services = new[] { service } });
        await using var client = await McpTesting.ConnectAsync(host, key);

        var (search, _) = await Call(client, "logs_search");
        Assert.DoesNotContain("4111111111111111", search);
        Assert.DoesNotContain("6456116363", search);
        string card    = Regex.Match(search, @"pay_one\s+(h:[0-9a-f]{12})").Groups[1].Value;
        string account = Regex.Match(search, @"AccountNo = (h:[0-9a-f]{12})").Groups[1].Value;
        Assert.NotEmpty(card);
        Assert.NotEmpty(account);

        foreach (string query in (string[])
                 [
                     $"select count(*) as '{card}'",                 // as an alias
                     $"select count(['{card}'])",                    // as a property's name
                     $"select count(*) as 'x\\'{account}'",          // a numeric one, with a quote in front of it
                     $"select count(*) group by ['{card}']",
                 ])
        {
            var (table, isError) = await Call(client, "logs_aggregate", ("query", query));
            Assert.False(isError, table);
            Assert.DoesNotContain("4111111111111111", table);
            Assert.DoesNotContain("6456116363", table);
        }

        // The record keeps each query as it was written: token and all.
        Assert.Contains(await Calls(admin, profileId), c => Text(c, "filter") == $"select count(*) as '{card}'");
    }

    [Fact]
    public async Task ATokenInsideAString_MovesNoQuote_InAFilterOrAQuery()
    {
        var admin = await host.AdminAsync();
        await Rule(admin, new { field = "MerchantId", action = "hash" });
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new(service, "Paid {PaymentId}") { Properties = { ["PaymentId"] = "pay_one", ["CardNumber"] = "4111111111111111", ["MerchantId"] = 6456116363L } },
            new(service, "Paid {PaymentId}") { Properties = { ["PaymentId"] = "pay_two", ["CardNumber"] = "5555555555554444", ["MerchantId"] = 6456116363L } });
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "token in a string", services = new[] { service } });
        await using var client = await McpTesting.ConnectAsync(host, key);

        var (search, _) = await Call(client, "logs_search");
        string token = Regex.Match(search, @"MerchantId = (h:[0-9a-f]{12})").Groups[1].Value;
        Assert.NotEmpty(token);

        var (direct, directError) = await Call(client, "logs_search", ("filter", "CardNumber like '4111%'"));
        Assert.True(directError);
        Assert.Contains("hidden field", direct);

        // Two equalities against two strings, as written — and as run.
        var (smuggled, smuggledError) = await Call(client, "logs_search",
            ("filter", $"A = 'p\\'{token}' or B = ' or CardNumber like \\'4111%'"));
        Assert.False(smuggledError, smuggled);
        Assert.Contains("No events match", smuggled);

        var (min, minError) = await Call(client, "logs_aggregate",
            ("query", $"select count(*) as 'x\\'{token}' where A = ', min(CardNumber) as \\'m'"));
        Assert.False(minError, min);
        Assert.DoesNotContain("411111111111111", min);
        Assert.DoesNotContain("6456116363", min);

        // The token itself, where a token belongs, still finds its events.
        var (found, _) = await Call(client, "logs_search", ("filter", $"MerchantId = '{token}'"));
        Assert.Contains("pay_one", found);
        Assert.Contains("pay_two", found);
    }

    [Fact]
    public async Task ATokenUnderAnOrderingOperator_IsRefused_AndRecordedAsAQuestionAboutAHiddenValue()
    {
        var admin = await host.AdminAsync();
        await Rule(admin, new { field = "Qty", action = "hash" });
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new(service, "Short")                                  { Properties = { ["Qty"] = 20 } },
            new(service, "A considerably longer message template") { Properties = { ["Qty"] = 20 } });
        var (profileId, key) = await McpTesting.CreateProfileAsync(admin, new { name = "token ordered", services = new[] { service }, maskingMode = "strict" });
        await using var client = await McpTesting.ConnectAsync(host, key);

        var (search, _) = await Call(client, "logs_search");
        string token = McpTesting.Token.Match(search).Value;
        Assert.NotEmpty(token);

        foreach (string filter in (string[])[$"length(@mt) > '{token}'", $"@mt > '{token}'", $"startsWith(@mt, '{token}')", $"'{token}'"])
        {
            var (text, isError) = await Call(client, "logs_search", ("filter", filter));
            Assert.True(isError, filter);
            Assert.Contains("token for a hidden value", text);
            Assert.DoesNotContain("considerably", text);
        }

        var refused = (await Calls(admin, profileId)).Where(static c => Text(c, "status") == "Refused").ToList();
        Assert.Equal(4, refused.Count);
        Assert.All(refused, static c => Assert.Equal("masking", Text(c, "refusalKind")));
        Assert.Contains(refused, c => Text(c, "filter") == $"length(@mt) > '{token}'");
    }

    // ── A hidden field is hidden by every road ────────────────────────────────

    [Fact]
    public async Task ARuleOnAnObject_HidesWhatIsUnderIt_FromAGroupByAndAPattern()
    {
        var admin = await host.AdminAsync();
        await Rule(admin, new { field = "Buyer", action = "hash" });
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new(service, "Bought {Sku}") { Properties = { ["Sku"] = "s1", ["Buyer"] = new Dictionary<string, object?> { ["Name"] = "Ivan Petrov", ["City"] = "Almaty" } } },
            new(service, "Bought {Sku}") { Properties = { ["Sku"] = "s2", ["Buyer"] = new Dictionary<string, object?> { ["Name"] = "Aigul Serikova", ["City"] = "Astana" } } });
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "object rule", services = new[] { service } });
        await using var client = await McpTesting.ConnectAsync(host, key);

        var (search, _) = await Call(client, "logs_search");
        Assert.DoesNotContain("Ivan", search);

        var (grouped, groupedError) = await Call(client, "logs_aggregate", ("query", "select count(*) group by Buyer.Name"));
        Assert.False(groupedError, grouped);
        Assert.DoesNotContain("Ivan Petrov", grouped);
        Assert.DoesNotContain("Aigul", grouped);
        Assert.Equal(2, McpTesting.Token.Matches(grouped).Count);        // two buyers, two tokens

        var (like, likeError) = await Call(client, "logs_search", ("filter", "Buyer.Name like 'Iv%'"));
        Assert.True(likeError);
        Assert.Contains("hidden field", like);
    }

    [Fact]
    public async Task AHiddenChild_IsNotReadThroughItsParent()
    {
        var admin = await host.AdminAsync();        // the fixture's own rule: Customer.Iin → hash
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new McpTesting.Event(service, "Checked {Step}") { Properties = { ["Step"] = "kyc", ["Customer"] = new Dictionary<string, object?> { ["Iin"] = "900101300126", ["Tier"] = "gold" } } });
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "through the parent", services = new[] { service } });
        await using var client = await McpTesting.ConnectAsync(host, key);

        var (search, _) = await Call(client, "logs_search");
        Assert.DoesNotContain("900101300126", search);
        Assert.Contains("gold", search);

        foreach (string filter in (string[])
                 [
                     "Customer.Iin > '9001'",
                     "elementAt(Customer, 'Iin') > '9001'",
                     "toJson(Customer) > '{\"Iin\":\"90010130012'",
                     "keys(Customer) = 'x'",
                     "values(Customer) = 'x'",
                 ])
        {
            var (text, isError) = await Call(client, "logs_search", ("filter", filter));
            Assert.True(isError, filter);
            Assert.DoesNotContain("1 event", text);
        }

        // Read as a value, the parent says nothing of what is in it.
        var (contains, _) = await Call(client, "logs_search", ("filter", "ci_contains(Customer, '9001')"));
        Assert.Contains("No events match", contains);
    }

    [Fact]
    public async Task AKeepForAnotherService_OpensNothingInThisOne()
    {
        var admin = await host.AdminAsync();
        string shop = McpTesting.NewService(), billing = McpTesting.NewService();
        await Rule(admin, new { field = "NickName", action = "keep", services = new[] { shop } });
        await McpTesting.IngestAsync(admin,
            new McpTesting.Event(billing, "Invoice {No}") { Properties = { ["No"] = "i-1", ["NickName"] = "Ivan Petrov" } });
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "strict elsewhere", services = new[] { billing }, maskingMode = "strict" });
        await using var client = await McpTesting.ConnectAsync(host, key);

        var (search, _) = await Call(client, "logs_search");
        Assert.DoesNotContain("Ivan", search);

        var (grouped, groupedError) = await Call(client, "logs_aggregate", ("query", "select count(*) group by NickName"));
        Assert.False(groupedError, grouped);
        Assert.DoesNotContain("Ivan Petrov", grouped);

        var (like, likeError) = await Call(client, "logs_search", ("filter", "NickName like 'Iv%'"));
        Assert.True(likeError);
        Assert.Contains("hidden field", like);
    }

    [Fact]
    public async Task AnAggregate_ReturnsNothingASearchHides()
    {
        var admin = await host.AdminAsync();
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            // Identifiers kept as text, as they usually are…
            new(service, "Signed up {User}") { Properties = { ["User"] = "u1", ["ClientIin"] = "900101300126", ["Phone"] = "+77015550123", ["Pan2"] = "4111111111111111", ["Born"] = "050312501234" } },
            new(service, "Signed up {User}") { Properties = { ["User"] = "u2", ["ClientIin"] = "000000000101", ["Phone"] = "+77015550199", ["Pan2"] = "5555555555554444", ["Born"] = "050312501234" } },
            // …and kept as numbers, with a measurement beside them.
            new(service, "Paid {User}") { Properties = { ["User"] = "u3", ["IinNo"] = 900101300126L, ["CardNo"] = 4111111111111111L, ["Amount"] = 1250, ["Elapsed"] = 40.5 } },
            new(service, "Paid {User}") { Properties = { ["User"] = "u3", ["IinNo"] = 900101300126L, ["CardNo"] = 4111111111111111L, ["Amount"] = 750,  ["Elapsed"] = 20.5 } });
        var (profileId, key) = await McpTesting.CreateProfileAsync(admin, new { name = "aggregates", services = new[] { service } });
        await using var client = await McpTesting.ConnectAsync(host, key);

        var (search, _) = await Call(client, "logs_search");
        foreach (string hidden in (string[])["900101300126", "77015550123", "4111111111111111"])
            Assert.DoesNotContain(hidden, search);

        // A string that reads as a number is not one to an aggregate: it is, here, an identifier.
        var (strings, stringsError) = await Call(client, "logs_aggregate",
            ("query", "select max(ClientIin), max(Phone), min(Pan2), max(Born), sum(ClientIin), avg(Phone) group by User"));
        Assert.False(stringsError, strings);
        foreach (string leaked in (string[])["900101300126", "77015550123", "77015550199", "411111111111111", "555555555555444", "50312501234", " 101"])
            Assert.DoesNotContain(leaked, strings);

        // A number that is a card number or an IIN takes no part in an aggregate at all: the
        // answer says how many values were left out, and what is a measurement is added up.
        var (numbers, numbersError) = await Call(client, "logs_aggregate",
            ("query", "select max(IinNo), min(CardNo), sum(CardNo), sum(Amount), avg(Elapsed), count(CardNo) where User = 'u3'"));
        Assert.False(numbersError, numbers);
        Assert.DoesNotContain("900101300126", numbers);
        Assert.DoesNotContain("411111111111111", numbers);
        Assert.DoesNotContain("8222222222222222", numbers);              // the two card numbers added up
        Assert.Empty(McpTesting.Token.Matches(numbers));
        Assert.Contains("4 values were left out", numbers);              // two events, two such fields
        Assert.Contains("2000", numbers);                                // sum(Amount)
        Assert.Contains("30.5", numbers);                                // avg(Elapsed)
        Assert.Matches(@"(?m)^null\s+null\s+null\s+2000\s+30\.5\s+2\s*$", numbers);

        // What a table kept out is counted, as what a search hides is.
        Assert.Equal(4, (await Calls(admin, profileId))[0].GetProperty("masked").GetInt64());
    }

    [Fact]
    public async Task SumsOfHiddenNumbers_AreNotEquationsToSolve()
    {
        // The second review's arithmetic: three IINs kept as numbers in a field no rule names.
        // One at a time an aggregate gave a token; two at a time it gave their sum — and three
        // sums of pairs are three equations in three unknowns.
        var admin = await host.AdminAsync();
        string service = McpTesting.NewService();
        long first = 900101300126, second = 850315400055, third = 780720300233;
        await McpTesting.IngestAsync(admin,
            new(service, "Client {Who}") { Properties = { ["Who"] = "first",  ["ClientRef"] = first,  ["Visits"] = 3 } },
            new(service, "Client {Who}") { Properties = { ["Who"] = "second", ["ClientRef"] = second, ["Visits"] = 5 } },
            new(service, "Client {Who}") { Properties = { ["Who"] = "third",  ["ClientRef"] = third,  ["Visits"] = 8 } });
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "sums", services = new[] { service } });
        await using var client = await McpTesting.ConnectAsync(host, key);

        var (search, _) = await Call(client, "logs_search");
        foreach (long iin in (long[])[first, second, third]) Assert.DoesNotContain(iin.ToString(), search);

        foreach (var (a, b) in ((string, string)[])[("first", "second"), ("second", "third"), ("first", "third")])
        {
            var (table, isError) = await Call(client, "logs_aggregate",
                ("query", $"select sum(ClientRef), avg(ClientRef), min(ClientRef), max(ClientRef), sum(Visits), count(ClientRef) where Who in ['{a}', '{b}']"));
            Assert.False(isError, table);
            Assert.Contains("2 values were left out", table);
            // Nothing of twelve digits or more, and nothing that is half of something of thirteen.
            Assert.DoesNotMatch(@"\d{11,}", table.Split('\n').Last(static line => line.Length > 0));
            Assert.Matches(@"(?m)^null\s+null\s+null\s+null\s+\d+\s+2\s*$", table);
        }
    }

    [Fact]
    public async Task PlainWords_DoNotSpellAHiddenValueOut()
    {
        var admin = await host.AdminAsync();
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new McpTesting.Event(service, "Paid {PaymentId}") { Properties = { ["PaymentId"] = "pay_one", ["CardNumber"] = "4111111111111111", ["Note"] = "quokka" } },
            new McpTesting.Event(service, "Refund failed", "Error") { Exception = "System.TimeoutException: the gateway did not answer" });
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "plain words", services = new[] { service }, maskingMode = "strict" });
        await using var client = await McpTesting.ConnectAsync(host, key);

        // Each of these found the event before: one digit more per search is the whole number.
        foreach (string words in (string[])["'4'", "'41'", "'4111'", "'411111111111111'", "'4111111111111111'", "quokka", "pay_one"])
        {
            var (text, isError) = await Call(client, "logs_search", ("filter", words));
            Assert.False(isError, text);
            Assert.Contains("No events match", text);
        }

        // The text of events is still what plain words find.
        var (paid, _) = await Call(client, "logs_search", ("filter", "paid"));
        Assert.Contains("1 event", paid);
        var (thrown, _) = await Call(client, "logs_search", ("filter", "gateway refund"));
        Assert.Contains("1 event", thrown);
        Assert.Contains("Refund failed", thrown);
    }

    // ── Scope ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ASearch_HandsOutNoCursorToAnEventOfAnotherService()
    {
        var admin = await host.AdminAsync();
        string mine = McpTesting.NewService(), other = McpTesting.NewService();
        var start = DateTimeOffset.UtcNow.AddMinutes(-10);
        await McpTesting.IngestAsync(admin, [.. Enumerable.Range(0, 150)
            .Select(i => new McpTesting.Event(other, "Payroll zebraquokka run {N}") { Properties = { ["N"] = i }, At = start.AddSeconds(i) })]);
        await McpTesting.IngestAsync(admin, new McpTesting.Event(mine, "Mine {N}") { Properties = { ["N"] = 1 } });
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "no foreign cursor", services = new[] { mine } });
        await using var client = await McpTesting.ConnectAsync(host, key);

        // A word that is nowhere and a word that is in 150 events of a service this profile does
        // not see are answered alike: with nothing, and nowhere to go on from.
        var (absent, _)  = await Call(client, "logs_search", ("filter", "wombatnowhere"), ("limit", 1));
        var (foreign, _) = await Call(client, "logs_search", ("filter", "zebraquokka"), ("limit", 1));
        Assert.DoesNotContain("cursor=", absent);
        Assert.DoesNotContain("cursor=", foreign);
        Assert.Contains("No events match", foreign);
        Assert.Equal(Regex.Replace(absent, @"\d", "0"), Regex.Replace(foreign, @"\d", "0"));
    }

    [Theory]
    [InlineData("newest", false)]
    [InlineData("oldest", false)]
    [InlineData("newest", true)]
    [InlineData("oldest", true)]
    public async Task ACursor_WalksEveryEventOnce_AlsoWhenTheyShareATimestamp(string order, bool sameInstant)
    {
        var admin = await host.AdminAsync();
        string service = McpTesting.NewService();
        var at = DateTimeOffset.UtcNow.AddMinutes(-5);
        await McpTesting.IngestAsync(admin, [.. Enumerable.Range(0, 57)
            .Select(i => new McpTesting.Event(service, "Step {N}") { Properties = { ["N"] = "n" + i.ToString("00") }, At = sameInstant ? at : at.AddMilliseconds(i * 7) })]);
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "paging " + order + sameInstant, services = new[] { service } });
        await using var client = await McpTesting.ConnectAsync(host, key);

        var seen = new List<string>();
        string? cursor = null;
        for (int pages = 0; ; pages++)
        {
            Assert.True(pages < 20, "the cursor does not advance");
            var args = new List<(string, object?)> { ("limit", 10), ("order", order) };
            if (cursor is not null) args.Add(("cursor", cursor));
            var (text, isError) = await Call(client, "logs_search", [.. args]);
            Assert.False(isError, text);
            seen.AddRange(Regex.Matches(text, @"\bn\d\d\b").Select(static m => m.Value));
            var next = Regex.Match(text, @"cursor=(\S+)");
            if (!next.Success) { Assert.Contains("end of results", text); break; }
            cursor = next.Groups[1].Value;
        }
        Assert.Equal(57, seen.Count);
        Assert.Equal(57, seen.Distinct().Count());
    }

    [Fact]
    public async Task ARefOfAnotherServicesEvent_IsAnsweredLikeOneThatDoesNotExist()
    {
        var admin = await host.AdminAsync();
        string mine = McpTesting.NewService(), other = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new(mine,  "Mine {N}")   { Properties = { ["N"] = 1 } },
            new(other, "Theirs {N}") { Properties = { ["N"] = 2 } });
        var (_, openKey)   = await McpTesting.CreateProfileAsync(admin, new { name = "sees all" });
        var (_, scopedKey) = await McpTesting.CreateProfileAsync(admin, new { name = "sees one", services = new[] { mine } });
        await using var open   = await McpTesting.ConnectAsync(host, openKey);
        await using var scoped = await McpTesting.ConnectAsync(host, scopedKey);

        var (search, _) = await Call(open, "logs_search", ("service", other));
        string reference = Regex.Match(search, @"^([0-9a-z]+(?:[+-][0-9a-z]+)?)\s+\d\d:\d\d:\d\d\.\d{3}", RegexOptions.Multiline).Groups[1].Value;
        Assert.NotEmpty(reference);

        var (theirs, theirsError)   = await Call(scoped, "logs_context", ("ref", reference));
        var (missing, missingError) = await Call(scoped, "logs_context", ("ref", "2w3ki0jnxips"));
        Assert.True(theirsError);
        Assert.True(missingError);
        Assert.Equal(missing, theirs);
    }

    [Fact]
    public async Task ASessionOpenedWithOneKey_IsNotAnotherKeysToUse()
    {
        var admin = await host.AdminAsync();
        string mine = McpTesting.NewService(), theirs = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new(mine,   "Opened by the first {N}") { Properties = { ["N"] = 1 } },
            new(theirs, "Seen by the second {N}")  { Properties = { ["N"] = 2 } });
        var (_, firstKey)  = await McpTesting.CreateProfileAsync(admin, new { name = "opens the session", services = new[] { mine } });
        var (_, secondKey) = await McpTesting.CreateProfileAsync(admin, new { name = "borrows it", services = new[] { theirs } });
        await using var first = await McpTesting.ConnectAsync(host, firstKey, McpTesting.Handshake);

        using var raw = host.CreateClient();
        using var request = new HttpRequestMessage(HttpMethod.Post, "/mcp")
        {
            Content = JsonContent.Create(new { jsonrpc = "2.0", id = 7, method = "tools/call", @params = new { name = "logs_search", arguments = new { } } }),
        };
        request.Headers.Authorization = new("Bearer", secondKey);
        request.Headers.Add("Mcp-Session-Id", first.SessionId);
        request.Headers.Add("MCP-Protocol-Version", McpTesting.Handshake);
        request.Headers.Accept.ParseAdd("application/json");
        request.Headers.Accept.ParseAdd("text/event-stream");
        using var response = await raw.SendAsync(request);

        // Not "no text of the first profile in the body", which an error of any kind satisfies:
        // the answer is a refusal, by its status.
        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.DoesNotContain("Opened by the first", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task TheRecordOfCalls_IsReadOnlyByAProfileThatNamesIt()
    {
        var admin = await host.AdminAsync();
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin, new McpTesting.Event(service, "Something {N}") { Properties = { ["N"] = 1 } });
        var (_, plainKey)   = await McpTesting.CreateProfileAsync(admin, new { name = "sees everything but the record" });
        var (_, auditorKey) = await McpTesting.CreateProfileAsync(admin, new { name = "reads the record", services = new[] { "ameto-mcp" } });
        await using var plain   = await McpTesting.ConnectAsync(host, plainKey);
        await using var auditor = await McpTesting.ConnectAsync(host, auditorKey);

        await Call(plain, "logs_search", ("service", service));        // a call to be recorded
        var (hidden, _) = await Call(plain, "logs_search", ("filter", "@service = 'ameto-mcp'"));
        Assert.Contains("No events match", hidden);

        var (record, isError) = await Call(auditor, "logs_search", ("filter", "Tool = 'logs_search'"));
        Assert.False(isError, record);
        Assert.Contains("AI call {Tool} by {Profile}: {Status}", record);
    }

    // ── The record of calls is the server's own ───────────────────────────────

    [Fact]
    public async Task AnIngestKey_CannotWriteARecordOfACall()
    {
        var admin = await host.AdminAsync();
        var before = await admin.GetFromJsonAsync<JsonElement>("/api/mcp/summary");
        string marker = "forged-" + Guid.NewGuid().ToString("N")[..8];
        foreach (string service in (string[])["ameto-mcp", "AMETO-MCP"])
            await McpTesting.IngestAsync(admin, new McpTesting.Event(service, "AI call {Tool} by {Profile}: {Status}")
            {
                Properties =
                {
                    ["Tool"] = "logs_search", ["Profile"] = marker, ["ProfileId"] = marker, ["Status"] = "OK",
                    ["Events"] = 3, ["Masked"] = 0, ["Tokens"] = 10, ["ElapsedMs"] = 1L, ["Arguments"] = "{}",
                },
            });

        // The other road in: OTLP, where the service is an attribute of the resource.
        string otlp = """
            {"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"ameto-mcp"}}]},
              "scopeLogs":[{"logRecords":[{"timeUnixNano":"NANOS","body":{"stringValue":"AI call {Tool} by {Profile}: {Status}"},
                "attributes":[{"key":"Tool","value":{"stringValue":"logs_search"}},{"key":"Profile","value":{"stringValue":"MARKER"}},
                              {"key":"ProfileId","value":{"stringValue":"MARKER"}},{"key":"Status","value":{"stringValue":"OK"}}]}]}]}]}
            """.Replace("MARKER", marker).Replace("NANOS", (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000).ToString());
        using (var posted = await admin.PostAsync("/v1/logs", new StringContent(otlp, System.Text.Encoding.UTF8, "application/json")))
            Assert.True(posted.IsSuccessStatusCode, $"OTLP ingest answered {(int)posted.StatusCode}");

        Assert.Empty(await Calls(admin, marker));
        var after = await admin.GetFromJsonAsync<JsonElement>("/api/mcp/summary");
        Assert.Equal(before.GetProperty("calls").GetInt32(), after.GetProperty("calls").GetInt32());

        // Nothing was thrown away: the events are there, under a name that says how they came.
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "finds the ingested ones" });
        await using var client = await McpTesting.ConnectAsync(host, key);
        var (found, _) = await Call(client, "logs_search", ("filter", $"@service = 'ingested:ameto-mcp' and ProfileId = '{marker}'"));
        Assert.Contains("3 events", found);
    }

    [Fact]
    public async Task ACall_IsNoLongerThanItsRecord()
    {
        var admin = await host.AdminAsync();
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin, new McpTesting.Event(service, "Paid {PaymentId}") { Properties = { ["PaymentId"] = "pay_tail" } });
        var (profileId, key) = await McpTesting.CreateProfileAsync(admin, new { name = "long filter", services = new[] { service } });
        await using var client = await McpTesting.ConnectAsync(host, key);

        // The filter the review hid a question in: padding first, the question after it.
        string filter = $"Padding = '{new string('x', 2_100)}' or PaymentId = 'pay_tail'";
        var (text, isError) = await Call(client, "logs_search", ("filter", filter));
        Assert.False(isError, text);
        Assert.Contains("pay_tail", text);
        var call = (await Calls(admin, profileId))[0];
        Assert.Equal(filter, Text(call, "filter"));                     // kept whole
        Assert.Contains("pay_tail", Text(call, "arguments"));

        // Past what the record keeps, a filter is not run.
        string tooLong = $"Padding = '{new string('x', FilterGuard.MaxFilterChars)}' or PaymentId = 'pay_tail'";
        var (refused, refusedError) = await Call(client, "logs_search", ("filter", tooLong));
        Assert.True(refusedError);
        Assert.Contains("at most", refused);
        Assert.DoesNotContain("1 event", refused);

        // And so are arguments, whichever of them carries the length.
        var (padded, paddedError) = await Call(client, "logs_search",
            ("filter", "PaymentId = 'pay_tail'"), ("fields", Enumerable.Range(0, 80).Select(static i => new string('f', 100) + i).ToArray()));
        Assert.True(paddedError);
        Assert.Contains("one call may carry at most", padded);
        Assert.Equal("input", Text((await Calls(admin, profileId))[0], "refusalKind"));
    }

    [Fact]
    public async Task CallsPastTheMinute_AreCounted_NotWrittenOneByOne()
    {
        var admin = await host.AdminAsync();
        var (profileId, key) = await McpTesting.CreateProfileAsync(admin, new { name = "flood", callsPerMinute = 1 });
        await using var client = await McpTesting.ConnectAsync(host, key);

        // A call for a tool nobody has is a call: it takes the minute's one place, and the rest
        // — asked for by any name — are over the limit.
        int overLimit = 0;
        for (int i = 0; i < 40; i++)
            if ((await Call(client, "no_such_tool_" + i)).Text.Contains("calls for this minute")) overLimit++;
        for (int i = 0; i < 40; i++)
            if ((await Call(client, "logs_overview")).Text.Contains("calls for this minute")) overLimit++;
        Assert.InRange(overLimit, 77, 79);

        static long Repeats(JsonElement call) => call.TryGetProperty("repeats", out var r) && r.ValueKind == JsonValueKind.Number ? r.GetInt64() : 0;

        var before = await admin.GetFromJsonAsync<JsonElement>("/api/mcp/summary");
        var calls  = await Calls(admin, profileId, 500);
        Assert.InRange(calls.Count, 7, 18);                             // not eighty
        Assert.InRange(calls.Max(Repeats), 32, 64);                     // and the record says about how many there were
        var pastTheMinute = calls.Where(static c => Text(c, "refusalKind") == "limit").ToList();
        Assert.All(pastTheMinute, static c => Assert.Equal("Refused", Text(c, "status")));
        // Every one of them carries its number, the first one too: "1" is the start of a series
        // or the whole of it, and a call that was not such a refusal carries none.
        Assert.All(pastTheMinute, static c => Assert.True(Repeats(c) >= 1));
        Assert.Contains(pastTheMinute, static c => Repeats(c) == 1);
        Assert.All(calls.Except(pastTheMinute), static c => Assert.Equal(0, Repeats(c)));

        // The summary counts refusals, not the records of them: what the last record of each
        // minute says, with the difference named — the calls that left no record of their own.
        long counted = before.GetProperty("refusals").GetProperty("limit").GetInt64();
        long unrecorded = before.GetProperty("unrecorded").GetInt64();
        Assert.InRange(counted, 64, 79);
        Assert.Equal(counted - pastTheMinute.Count, unrecorded);
        Assert.True(unrecorded >= 50, $"unrecorded = {unrecorded}");
        // And it says that every figure it is part of is a floor.
        Assert.True(before.GetProperty("thinned").GetBoolean());
    }

    [Fact]
    public async Task AName_IsRecordedAndAnsweredAtTheLengthANameHas()
    {
        var admin = await host.AdminAsync();
        var (profileId, key) = await McpTesting.CreateProfileAsync(admin, new { name = "long name" });
        await using var client = await McpTesting.ConnectAsync(host, key);

        var (text, isError) = await Call(client, new string('a', 20_000));
        Assert.True(isError);
        Assert.Contains("not offered", text);
        Assert.True(text.Length < 200, $"the answer is {text.Length} characters");
        Assert.Equal(65, Text((await Calls(admin, profileId))[0], "tool")!.Length);
    }

    [Theory]
    [InlineData("-999999999999999999999d")]
    [InlineData("-NaNd")]
    [InlineData("-Infinityh")]
    [InlineData("0001-01-01")]
    public async Task ATimeThatIsNotOne_IsARefusal_NotAFailure(string from)
    {
        var admin = await host.AdminAsync();
        var (profileId, key) = await McpTesting.CreateProfileAsync(admin, new { name = "time " + from });
        await using var client = await McpTesting.ConnectAsync(host, key);

        var (text, isError) = await Call(client, "logs_search", ("from", from));
        Assert.True(isError);
        Assert.Contains("'from' is not a time", text);
        var call = (await Calls(admin, profileId))[0];
        Assert.Equal("Refused", Text(call, "status"));
        Assert.Equal("input", Text(call, "refusalKind"));
    }

    // ── Published tools ───────────────────────────────────────────────────────

    [Fact]
    public async Task APublishedToolsArgument_IsAValue_WhateverItHolds()
    {
        var admin = await host.AdminAsync();
        await Rule(admin, new { field = "TenantNo", action = "hash" });
        string service = McpTesting.NewService(), elsewhere = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new(service,   "Order {OrderId}") { Properties = { ["OrderId"] = "o-alice", ["Owner"] = "alice", ["Region"] = "eu", ["TenantNo"] = 778899L } },
            new(service,   "Order {OrderId}") { Properties = { ["OrderId"] = "o-bob",   ["Owner"] = "bob",   ["Region"] = "us", ["TenantNo"] = 778899L } },
            new(elsewhere, "Salary {Employee}") { Properties = { ["Employee"] = "e-carol" } });
        var (profile, key) = await McpTesting.CreateProfileAsync(admin, new { name = "published only", toolsMode = "publishedOnly" });
        string name = "orders_of_" + Guid.NewGuid().ToString("N")[..8];
        using (var saved = await admin.PostAsJsonAsync("/api/mcp/tools", new
        {
            name,
            description = "Orders of one owner in one region.",
            parameters  = new object[] { new { name = "owner", required = true }, new { name = "region", required = true } },
            filter      = $"@service = '{service}' and Owner = {{owner}} and Region = {{region}}",
            published   = true,
            profiles    = new[] { profile },
        }))
            Assert.Equal(HttpStatusCode.OK, saved.StatusCode);
        await using var client = await McpTesting.ConnectAsync(host, key);

        var (legit, _) = await Call(client, name, ("owner", "alice"), ("region", "eu"));
        Assert.Contains("o-alice", legit);
        Assert.DoesNotContain("o-bob", legit);
        string token = McpTesting.Token.Match(legit).Value;
        Assert.NotEmpty(token);

        // With a token in the first argument and the rest of a filter in the second, this pair
        // used to return every event the profile could see.
        foreach (var (owner, region) in ((string, string)[])
                 [
                     ("x' or has(@mt) or C = 'c", "eu"),
                     ("x'" + token, "or has(@mt) or C = 'c"),
                     (token, "' or has(@mt) or '"),
                     ("alice' and Region = 'us", "eu"),
                 ])
        {
            var (text, isError) = await Call(client, name, ("owner", owner), ("region", region));
            Assert.False(isError, text);
            Assert.Contains("No events match", text);
            Assert.DoesNotContain("o-bob", text);
            Assert.DoesNotContain("e-carol", text);
        }
    }

    [Fact]
    public async Task AToolThatWouldBeEverything_IsNotSaved()
    {
        var admin = await host.AdminAsync();
        using var saved = await admin.PostAsJsonAsync("/api/mcp/tools", new
        {
            name        = "errors_or_owner_" + Guid.NewGuid().ToString("N")[..8],
            description = "Errors, or the events of one owner.",
            parameters  = new object[] { new { name = "owner" } },
            filter      = "(Owner = {owner} or @l = 'Error')",
        });
        Assert.Equal(HttpStatusCode.BadRequest, saved.StatusCode);
        var problem = await saved.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("filter", problem.GetProperty("field").GetString());
        Assert.Contains("no condition left", problem.GetProperty("error").GetString());
    }

    // ── What the second review found ──────────────────────────────────────────

    [Fact]
    public async Task APositionSpelledAnyWay_IsStillAnElementOfTheList()
    {
        var admin = await host.AdminAsync();
        await Rule(admin, new { field = "Basket.Sku", action = "hash" });
        await Rule(admin, new { field = "Basket.Price", action = "hash" });
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new McpTesting.Event(service, "Ordered {OrderId}")
            {
                Properties =
                {
                    ["OrderId"] = "o-1",
                    ["Basket"] = new List<object?>
                    {
                        new Dictionary<string, object?> { ["Sku"] = "ZX-SECRET-41", ["Price"] = 1250 },
                        new Dictionary<string, object?> { ["Sku"] = "QQ-OTHER-7", ["Price"] = 90 },
                    },
                },
            },
            new McpTesting.Event(service, "Ordered {OrderId}")
            {
                Properties =
                {
                    ["OrderId"] = "o-2",
                    ["Basket"] = new List<object?> { new Dictionary<string, object?> { ["Sku"] = "MM-THIRD-9", ["Price"] = 77 } },
                },
            });
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "positions", services = new[] { service } });
        await using var client = await McpTesting.ConnectAsync(host, key);

        var (search, _) = await Call(client, "logs_search");
        Assert.Contains("o-1", search);
        foreach (string secret in (string[])["ZX-SECRET-41", "QQ-OTHER-7", "MM-THIRD-9", "1250"]) Assert.DoesNotContain(secret, search);

        // The engine reads a position out of any segment that reads as a whole number — a space
        // before it, a sign, a tab. Each of these is the first element's Sku, and is judged as it.
        foreach (string position in (string[])["[0]", ".0", "[' 0']", "['0 ']", "['+0']", "['-0']", "['\t0']"])
        {
            var (text, isError) = await Call(client, "logs_search", ("filter", $"Basket{position}.Sku like 'ZX-S%'"));
            Assert.True(isError, position);
            Assert.Contains("hidden field", text);
        }

        var (grouped, groupedError) = await Call(client, "logs_aggregate",
            ("query", "select count(*) group by Basket[' 0'].Sku, Basket['+1'].Sku"));
        Assert.False(groupedError, grouped);
        foreach (string secret in (string[])["ZX-SECRET-41", "QQ-OTHER-7", "MM-THIRD-9"]) Assert.DoesNotContain(secret, grouped);
        Assert.Equal(3, McpTesting.Token.Matches(grouped).Count);        // three values, each as its token

        var (ranged, rangedError) = await Call(client, "logs_aggregate", ("query", "select max(Basket[' 0'].Price), min(Basket[' 0'].Price)"));
        Assert.True(rangedError);
        Assert.Contains("hidden field", ranged);
    }

    [Fact]
    public async Task ATokenComparedWithAFieldInTheOpen_FindsTheValue_AndDoesNotShowIt()
    {
        var admin = await host.AdminAsync();
        await Rule(admin, new { field = "PatientAge", action = "hash" });
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin,
            new(service, "Admitted {Who}") { Properties = { ["Who"] = "patient", ["PatientAge"] = 7391 } },
            new(service, "Retried {Who}")  { Properties = { ["Who"] = "worker", ["Attempt"] = 7391, ["StatusCode"] = 200 } },
            new(service, "Retried {Who}")  { Properties = { ["Who"] = "other", ["Attempt"] = 2, ["StatusCode"] = 200 } });
        var (profileId, key) = await McpTesting.CreateProfileAsync(admin, new { name = "token in the open", services = new[] { service } });
        await using var client = await McpTesting.ConnectAsync(host, key);

        var (search, _) = await Call(client, "logs_search", ("filter", "has(PatientAge)"));
        Assert.DoesNotContain("7391", search);
        string token = McpTesting.Token.Match(search).Value;
        Assert.NotEmpty(token);

        // The same number lies in the open in another field. The token finds it — that is what
        // a token is for — and the answer says so without saying what the number is.
        var (found, foundError) = await Call(client, "logs_search", ("filter", $"Attempt = '{token}' or StatusCode = '{token}'"));
        Assert.False(foundError, found);
        Assert.Contains("1 event", found);
        Assert.Contains("worker", found);
        Assert.Contains(token, found);
        Assert.DoesNotContain("7391", found);

        var (table, tableError) = await Call(client, "logs_aggregate",
            ("query", $"select count(*), max(Attempt) where Attempt = '{token}' group by Attempt"));
        Assert.False(tableError, table);
        Assert.DoesNotContain("7391", table);
        Assert.Contains(token, table);

        // Counted with what the answer hid, under its own name.
        var call = (await Calls(admin, profileId)).First(c => Text(c, "tool") == "logs_search" && Text(c, "filter")!.StartsWith("Attempt"));
        Assert.Equal(1, call.GetProperty("maskedBy").GetProperty("detectors").GetProperty("named").GetInt64());
    }

    [Fact]
    public async Task ThePreview_SaysWhichStoredPropertyEachSentOneIs()
    {
        var admin = await host.AdminAsync();
        string service = McpTesting.NewService();
        await McpTesting.IngestAsync(admin, new McpTesting.Event(service, "Limits of {Plan}")
        {
            Properties = { ["Plan"] = "gold", ["a.serikov@example.com"] = "active" },
        });

        using var response = await admin.PostAsJsonAsync("/api/mcp/preview", new { mode = "standard", filter = $"@service = '{service}'" });
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var sent = (await response.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("sent").GetProperty("properties").EnumerateArray().ToList();

        // A name that holds a value goes out as a token; the preview still says which property it was.
        var hiddenName = Assert.Single(sent, static p => p.GetProperty("stored").GetString() == "a.serikov@example.com");
        Assert.Matches("^h:[0-9a-f]{12}$", hiddenName.GetProperty("name").GetString());
        Assert.Equal("active", hiddenName.GetProperty("value").GetString());
        var plain = Assert.Single(sent, static p => p.GetProperty("stored").GetString() == "Plan");
        Assert.Equal("Plan", plain.GetProperty("name").GetString());
    }

    [Fact]
    public async Task AToolWithAParameterWhereANameStands_IsNotSaved()
    {
        var admin = await host.AdminAsync();
        using var saved = await admin.PostAsJsonAsync("/api/mcp/tools", new
        {
            name        = "by_header_" + Guid.NewGuid().ToString("N")[..8],
            description = "Requests with one header set to one value.",
            parameters  = new object[] { new { name = "name", required = true }, new { name = "value", required = true } },
            filter      = "@service = 'gateway' and Headers[{name}] = {value}",
        });
        Assert.Equal(HttpStatusCode.BadRequest, saved.StatusCode);
        var problem = await saved.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("filter", problem.GetProperty("field").GetString());
        Assert.Contains("{name} stands where a name stands", problem.GetProperty("error").GetString());
    }

    // ── The settings API ──────────────────────────────────────────────────────

    [Fact]
    public async Task AnUpdateOfAProfile_ChangesWhatItNames_AndNothingElse()
    {
        var admin = await host.AdminAsync();
        using var created = await admin.PostAsJsonAsync("/api/mcp/profiles", new
        {
            name = "narrow", description = "one service, strict", services = new[] { "payments-api" }, maskingMode = "strict",
            tools = new[] { "logs_overview" }, freeFilters = false, toolsMode = "publishedOnly", maxEvents = 10, maxWindowMinutes = 30, callsPerMinute = 3,
        });
        string id = (await created.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("profile").GetProperty("id").GetString()!;

        // The update the review sent: a new name and nothing else.
        using var renamed = await admin.PutAsJsonAsync($"/api/mcp/profiles/{id}", new { name = "narrow, renamed" });
        Assert.Equal(HttpStatusCode.OK, renamed.StatusCode);
        var after = await renamed.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("narrow, renamed", after.GetProperty("name").GetString());
        Assert.Equal("one service, strict", after.GetProperty("description").GetString());
        Assert.Equal("payments-api", Assert.Single(after.GetProperty("services").EnumerateArray()).GetString());
        Assert.Equal("strict", after.GetProperty("maskingMode").GetString());
        Assert.Equal("publishedOnly", after.GetProperty("toolsMode").GetString());
        Assert.False(after.GetProperty("freeFilters").GetBoolean());
        Assert.Equal("logs_overview", Assert.Single(after.GetProperty("tools").EnumerateArray()).GetString());
        Assert.Equal((10, 30, 3), (after.GetProperty("maxEvents").GetInt32(), after.GetProperty("maxWindowMinutes").GetInt32(), after.GetProperty("callsPerMinute").GetInt32()));

        // A member that is named is set — to the empty list too, which here means every service.
        using var widened = await admin.PutAsJsonAsync($"/api/mcp/profiles/{id}", new { services = Array.Empty<string>(), freeFilters = true });
        var wide = await widened.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(0, wide.GetProperty("services").GetArrayLength());
        Assert.True(wide.GetProperty("freeFilters").GetBoolean());
        Assert.Equal("narrow, renamed", wide.GetProperty("name").GetString());
        Assert.Equal("strict", wide.GetProperty("maskingMode").GetString());

        // A mode is a name. "2" is no mode at all, and "1" is not a way to spell strict.
        foreach (object body in new object[]
                 {
                     new { maskingMode = "2" }, new { maskingMode = "1" }, new { maskingMode = "0" }, new { toolsMode = "1" },
                     new { maskingMode = "loose" }, new { name = " " },
                 })
        {
            using var refused = await admin.PutAsJsonAsync($"/api/mcp/profiles/{id}", body);
            Assert.Equal(HttpStatusCode.BadRequest, refused.StatusCode);
        }
        using var rule = await admin.PostAsJsonAsync("/api/mcp/rules", new { field = "Whatever", action = "1" });
        Assert.Equal(HttpStatusCode.BadRequest, rule.StatusCode);

        // And a new profile still has to be given a name.
        using var nameless = await admin.PostAsJsonAsync("/api/mcp/profiles", new { services = new[] { "payments-api" } });
        Assert.Equal(HttpStatusCode.BadRequest, nameless.StatusCode);
    }

    // ── Answers of a size ─────────────────────────────────────────────────────

    [Fact]
    public async Task OneEventInFull_KeepsToTheAnswersBudget()
    {
        var admin = await host.AdminAsync();
        string service = McpTesting.NewService();
        var wide = new McpTesting.Event(service, "Wide {N}") { Properties = { ["N"] = 1 } };
        for (int i = 0; i < 28; i++) wide.Properties["Blob" + i.ToString("00")] = new string((char)('a' + i % 26), 2_000);
        await McpTesting.IngestAsync(admin, wide);
        var (_, key) = await McpTesting.CreateProfileAsync(admin, new { name = "wide event", services = new[] { service } });
        await using var client = await McpTesting.ConnectAsync(host, key);

        var (search, _) = await Call(client, "logs_search", ("fields", new[] { "N" }));
        string reference = Regex.Match(search, @"^([0-9a-z]+(?:[+-][0-9a-z]+)?)\s+\d\d:\d\d:\d\d\.\d{3}", RegexOptions.Multiline).Groups[1].Value;
        Assert.NotEmpty(reference);

        var (detail, isError) = await Call(client, "logs_context", ("ref", reference));
        Assert.False(isError, detail);
        Assert.Contains("more properties left out", detail);
        Assert.InRange(detail.Length, 20_000, 30_000);                  // 8 000 tokens, about three characters each
    }

    [Fact]
    public async Task TheFieldsOfAService_CountAsHiddenOnlyWhatTheAnswerShows()
    {
        var admin = await host.AdminAsync();
        string service = McpTesting.NewService();
        // Five events alike: the answer shows each property's value once.
        await McpTesting.IngestAsync(admin, [.. Enumerable.Range(0, 5).Select(_ =>
            new McpTesting.Event(service, "Paid with {CardNumber} by a.serikov@example.com")
            {
                Properties = { ["CardNumber"] = "4111111111111111", ["Contact"] = "a.serikov@example.com", ["Plain"] = "x" },
            })]);
        var (profileId, key) = await McpTesting.CreateProfileAsync(admin, new { name = "fields", services = new[] { service } });
        await using var client = await McpTesting.ConnectAsync(host, key);

        var (fields, isError) = await Call(client, "logs_fields");
        Assert.False(isError, fields);
        Assert.DoesNotContain("4111111111111111", fields);
        Assert.DoesNotContain("a.serikov@example.com", fields);

        // One card number and one address shown as tokens — not five of each, and not the
        // address in a template this answer never shows.
        Assert.Equal(2, (await Calls(admin, profileId))[0].GetProperty("masked").GetInt64());
    }
}
