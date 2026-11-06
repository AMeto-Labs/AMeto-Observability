using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Ameto.Core;

namespace Ameto.Integration.Tests;

/// <summary>
/// #130: <c>PUT /api/retention</c> stored any number of days, and "forever" typed as 200 000 days
/// overflowed the trace cutoff. A window outside 1…36 500 days is now refused with 400 and the
/// field's name, and nothing of the request is kept.
/// </summary>
public sealed class RetentionValidationTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    public RetentionValidationTests(AmetoWebAppFactory factory) => _factory = factory;

    private HttpClient Admin()
    {
        var client = _factory.CreateClient();
        client.DefaultRequestHeaders.Add(TestAuthHandler.RoleHeader, "admin");
        return client;
    }

    [Theory]
    [InlineData("tracesDays", 200_000)]
    [InlineData("metricsDays", 36_501)]
    [InlineData("debugDays", 0)]
    [InlineData("verboseDays", -1)]
    public async Task A_window_outside_the_bounds_is_refused_and_the_field_named(string field, int days)
    {
        var client = Admin();
        var before = await client.GetFromJsonAsync<JsonElement>("/api/retention");

        var body = new Dictionary<string, int> { [field] = days };
        var resp = await client.PutAsJsonAsync("/api/retention", body);

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        var problem = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(field, problem.GetProperty("field").GetString());
        Assert.Contains("36500", problem.GetProperty("error").GetString());

        // Nothing of the refused request was kept.
        var after = await client.GetFromJsonAsync<JsonElement>("/api/retention");
        Assert.Equal(before.GetRawText(), after.GetRawText());
    }

    [Fact]
    public async Task The_bounds_themselves_are_accepted()
    {
        var resp = await Admin().PutAsJsonAsync("/api/retention",
            new { verboseDays = RetentionMath.MinDays, tracesDays = RetentionMath.MaxDays, metricsDays = 30 });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var dto = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(RetentionMath.MaxDays, dto.GetProperty("tracesDays").GetInt32());
        Assert.Equal(RetentionMath.MinDays, dto.GetProperty("verboseDays").GetInt32());
    }
}
