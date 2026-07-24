using System.Text.Json;
using Ameto.Core;

namespace Ameto.Integration.Tests;

/// <summary>
/// When the server drops an event whose properties exceed the per-event limit,
/// it now also records a compact Error marker in the stream (DroppedBy=server),
/// so the loss is visible on the Events page — not only in the server's own log.
/// </summary>
public sealed class OversizedDropMarkerTests : IClassFixture<AmetoWebAppFactory>
{
    private readonly AmetoWebAppFactory _factory;
    public OversizedDropMarkerTests(AmetoWebAppFactory factory) => _factory = factory;

    [Fact]
    public async Task OversizedEvent_IsDropped_ButSurfacesAsServerErrorMarker()
    {
        var client = _factory.CreateClient();

        // One event with ~100 KB of properties — over the 64 KB per-event limit.
        var big = new string('x', 100_000);
        var ev = TestHelpers.MakeEvent(
            "----- Command {0} handled; Response: {@1}",
            LogLevel.Information,
            props: new Dictionary<string, object?>
            {
                ["0"]           = "GetWallet",
                ["1"]           = big,
                ["service.name"] = "Wallet.API",
            });

        var resp = await TestHelpers.IngestAsync(client, ev);
        // The oversized original is dropped, not ingested.
        Assert.Equal(0, resp.GetProperty("ingested").GetInt32());
        Assert.Equal(1, resp.GetProperty("dropped").GetInt32());

        // …but a server drop marker appears in the stream.
        var events = await TestHelpers.WaitForEventsAsync(
            client, expectedCount: 1, filter: "DroppedBy = 'server'");
        var marker = Assert.Single(events);

        Assert.Equal("Error", marker.GetProperty("@l").GetString());
        Assert.Contains("dropped an oversized log event", marker.GetProperty("@mt").GetString());
        Assert.Equal("server", GetProp(marker, "DroppedBy").GetString());
        // Original size + limit + the original template are preserved.
        Assert.True(GetProp(marker, "EventBodyBytes").GetInt32() > 65_536);
        Assert.Equal(65_536, GetProp(marker, "EventBodyLimitBytes").GetInt32());
        Assert.Contains("Command", GetProp(marker, "OriginalTemplate").GetString());
        Assert.Equal("Information", GetProp(marker, "OriginalLevel").GetString());
    }

    /// <summary>Reads a property whether the server nested it under "props" or flattened it.</summary>
    private static JsonElement GetProp(JsonElement ev, string name)
    {
        if (ev.TryGetProperty(name, out var flat)) return flat;
        if (ev.TryGetProperty("props", out var props) && props.TryGetProperty(name, out var nested))
            return nested;
        throw new Xunit.Sdk.XunitException($"property '{name}' not found on the marker: {ev}");
    }
}
