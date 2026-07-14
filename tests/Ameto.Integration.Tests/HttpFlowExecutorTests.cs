using System.Net;
using System.Text;
using Ameto.Alerts;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ameto.Integration.Tests;

/// <summary>
/// Exercises the multi-step HTTP-flow engine with a request-capturing mock handler:
/// variable substitution (alert / secret / captured), extraction (JSON / XML / regex),
/// step chaining, and stop-on-error.
/// </summary>
public sealed class HttpFlowExecutorTests
{
    private sealed record Call(string Method, string Url, string? Auth, string Body);

    private sealed class MockHandler(Func<HttpRequestMessage, (HttpStatusCode, string)> responder) : HttpMessageHandler
    {
        public readonly List<Call> Calls = [];
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var body = request.Content is null ? "" : await request.Content.ReadAsStringAsync(ct);
            var auth = request.Headers.TryGetValues("Authorization", out var a) ? string.Join(",", a) : null;
            Calls.Add(new Call(request.Method.Method, request.RequestUri!.ToString(), auth, body));
            var (status, resp) = responder(request);
            return new HttpResponseMessage(status) { Content = new StringContent(resp, Encoding.UTF8, "application/json") };
        }
    }

    private static Task Run(MockHandler h, HttpFlowChannel ch, Dictionary<string, string>? vars = null) =>
        HttpFlowExecutor.RunAsync(ch, vars ?? new(), new HttpClient(h), NullLogger.Instance, default);

    [Fact]
    public async Task Substitutes_Alert_Secret_Captured_AcrossSteps()
    {
        var h = new MockHandler(req => req.RequestUri!.Host == "auth.test"
            ? (HttpStatusCode.OK, "{\"access_token\":\"TOK123\"}")
            : (HttpStatusCode.OK, "{}"));

        var ch = new HttpFlowChannel
        {
            Secrets = new() { ["key"] = "sek" },
            Steps =
            [
                new HttpFlowStep
                {
                    Name = "auth", Method = "POST", Url = "http://auth.test/token",
                    BodyType = "json", Body = "{\"secret\":\"{{secret.key}}\"}",
                    Extracts = [ new HttpExtract { Var = "token", Source = "json", Expr = "$.access_token" } ],
                },
                new HttpFlowStep
                {
                    Name = "notify", Method = "GET", Url = "http://api.test/{{alert.name}}",
                    Headers = [ new HttpHeader { Key = "Authorization", Value = "Bearer {{token}}" } ],
                },
            ],
        };

        await Run(h, ch, new() { ["alert.name"] = "myrule" });

        Assert.Equal(2, h.Calls.Count);
        Assert.Contains("sek", h.Calls[0].Body);                 // {{secret.key}} → decrypted secret
        Assert.Equal("http://api.test/myrule", h.Calls[1].Url);  // {{alert.name}} → alert context
        Assert.Equal("Bearer TOK123", h.Calls[1].Auth);          // {{token}} → captured from step 1
    }

    [Fact]
    public async Task Extracts_Via_Xpath()
    {
        var h = new MockHandler(req => req.RequestUri!.Host == "a.test"
            ? (HttpStatusCode.OK, "<r><token>XT</token></r>")
            : (HttpStatusCode.OK, "{}"));
        var ch = new HttpFlowChannel { Steps =
        [
            new HttpFlowStep { Name = "a", Method = "GET", Url = "http://a.test/",
                Extracts = [ new HttpExtract { Var = "t", Source = "xml", Expr = "/r/token" } ] },
            new HttpFlowStep { Name = "b", Method = "GET", Url = "http://b.test/{{t}}" },
        ] };
        await Run(h, ch);
        Assert.Equal("http://b.test/XT", h.Calls[1].Url);
    }

    [Fact]
    public async Task Extracts_Via_Regex()
    {
        var h = new MockHandler(req => req.RequestUri!.Host == "a.test"
            ? (HttpStatusCode.OK, "session id=ABC123; more")
            : (HttpStatusCode.OK, "{}"));
        var ch = new HttpFlowChannel { Steps =
        [
            new HttpFlowStep { Name = "a", Method = "GET", Url = "http://a.test/",
                Extracts = [ new HttpExtract { Var = "t", Source = "regex", Expr = "id=(\\w+);" } ] },
            new HttpFlowStep { Name = "b", Method = "GET", Url = "http://b.test/{{t}}" },
        ] };
        await Run(h, ch);
        Assert.Equal("http://b.test/ABC123", h.Calls[1].Url);
    }

    [Fact]
    public async Task JsonPath_Handles_ArrayIndex_And_BracketKey()
    {
        var h = new MockHandler(req => req.RequestUri!.Host == "a.test"
            ? (HttpStatusCode.OK, "{\"items\":[{\"id\":\"first\"},{\"id\":\"second\"}]}")
            : (HttpStatusCode.OK, "{}"));
        var ch = new HttpFlowChannel { Steps =
        [
            new HttpFlowStep { Name = "a", Method = "GET", Url = "http://a.test/",
                Extracts = [ new HttpExtract { Var = "t", Source = "json", Expr = "$.items[1].id" } ] },
            new HttpFlowStep { Name = "b", Method = "GET", Url = "http://b.test/{{t}}" },
        ] };
        await Run(h, ch);
        Assert.Equal("http://b.test/second", h.Calls[1].Url);
    }

    [Fact]
    public async Task Stops_On_Non2xx()
    {
        var h = new MockHandler(_ => (HttpStatusCode.InternalServerError, "boom"));
        var ch = new HttpFlowChannel { Steps =
        [
            new HttpFlowStep { Name = "a", Method = "GET", Url = "http://x.test/1" },
            new HttpFlowStep { Name = "b", Method = "GET", Url = "http://x.test/2" },
        ] };
        await Run(h, ch);
        Assert.Single(h.Calls);   // the second step is never sent
    }

    [Fact]
    public async Task Stops_When_Extraction_Fails()
    {
        var h = new MockHandler(_ => (HttpStatusCode.OK, "{\"x\":1}"));
        var ch = new HttpFlowChannel { Steps =
        [
            new HttpFlowStep { Name = "a", Method = "GET", Url = "http://x.test/1",
                Extracts = [ new HttpExtract { Var = "t", Source = "json", Expr = "$.missing" } ] },
            new HttpFlowStep { Name = "b", Method = "GET", Url = "http://x.test/2" },
        ] };
        await Run(h, ch);
        Assert.Single(h.Calls);
    }
}
