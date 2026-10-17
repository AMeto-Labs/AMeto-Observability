using System.Net;
using System.Net.Http.Headers;
using System.Text;
using Ameto.Core;
using Microsoft.Extensions.DependencyInjection;

namespace Ameto.Integration.Tests;

/// <summary>
/// An event ingested once the intern pool is full keeps its <c>@service</c>. The pool holds 65 536
/// templates and services; past that, interning answers -1, and the hot tier, which named an event's
/// service only by its pool index, kept no name for it: the event was served, filtered, counted and
/// flushed with no service. The name is kept beside the event now, as the template is. Its own host:
/// the test fills the host's pool.
/// </summary>
public sealed class ServicePastFullPoolTests : IClassFixture<ServicePastFullPoolTests.Factory>
{
    public sealed class Factory : AmetoWebAppFactory;

    private readonly Factory    _factory;
    private readonly HttpClient _client;

    public ServicePastFullPoolTests(Factory factory)
    {
        _factory = factory;
        _client  = factory.CreateClient();
    }

    [Fact]
    public async Task An_event_ingested_past_a_full_intern_pool_keeps_its_service_and_is_found_by_it()
    {
        var pool = _factory.Services.GetRequiredService<StringInternPool>();
        for (int i = 0; pool.Intern("filler " + i) >= 0; i++) { }
        Assert.Equal(-1, pool.Intern("svc.clef.past.saturation"));

        // CLEF, by @service.
        await TestHelpers.IngestAsync(_client, TestHelpers.MakeEvent("clef past saturation", props: new() { ["@service"] = "svc.clef.past.saturation" }));

        // OTLP, by the resource's service.name.
        long nanos = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000;
        string otlp = "{\"resourceLogs\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"svc.otlp.past.saturation\"}}]},"
                    + "\"scopeLogs\":[{\"logRecords\":[{\"timeUnixNano\":\"" + nanos + "\",\"severityNumber\":9,\"body\":{\"stringValue\":\"otlp past saturation\"}}]}]}]}";
        var content = new StringContent(otlp, Encoding.UTF8);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        using (var response = await _client.PostAsync("/v1/logs", content))
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        foreach (var svc in new[] { "svc.clef.past.saturation", "svc.otlp.past.saturation" })
        {
            var events = await TestHelpers.WaitForEventsAsync(_client, expectedCount: 1, filter: $"@service = '{svc}'");
            var ev     = Assert.Single(events);
            Assert.Equal(svc, ev.GetProperty("@service").GetString());
        }
    }
}
