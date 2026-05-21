using Rd.Log.Alerts;

namespace Rd.Log.Server;

public static class AlertEndpointMapper
{
    private static AlertRule BuildRule(string? id, AlertRuleUpsertRequest req) => new()
    {
        Id        = id ?? (string.IsNullOrEmpty(req.Id) ? Guid.NewGuid().ToString("N")[..8] : req.Id!),
        Name      = req.Name ?? "Unnamed",
        Filter    = req.Filter,
        Threshold = req.Threshold > 0 ? req.Threshold : 1,
        Window    = TimeSpan.FromSeconds(req.WindowSeconds > 0 ? req.WindowSeconds : 300),
        Cooldown  = TimeSpan.FromSeconds(req.CooldownSeconds > 0 ? req.CooldownSeconds : 900),
        Enabled   = req.Enabled,
        Channels  = req.Channels ?? [],
    };

    /// <summary>
    /// Maps alert rule CRUD endpoints:
    ///   GET    /api/signals
    ///   GET    /api/signals/{id}
    ///   POST   /api/signals
    ///   PUT    /api/signals/{id}
    ///   DELETE /api/signals/{id}
    /// </summary>
    public static void MapAlertEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api/signals").RequireAuthorization();

        group.MapGet("/", (AlertRuleStore store) =>
            Results.Ok(store.GetAll()));

        group.MapGet("/{id}", (string id, AlertRuleStore store) =>
        {
            var rule = store.GetById(id);
            return rule is null ? Results.NotFound() : Results.Ok(rule);
        });

        group.MapPost("/", (AlertRuleUpsertRequest req, AlertRuleStore store) =>
        {
            var rule = BuildRule(null, req);
            store.Upsert(rule);
            return Results.Created($"/api/signals/{rule.Id}", rule);
        });

        group.MapPut("/{id}", (string id, AlertRuleUpsertRequest req, AlertRuleStore store) =>
        {
            if (store.GetById(id) is null) return Results.NotFound();
            var rule = BuildRule(id, req);
            store.Upsert(rule);
            return Results.Ok(rule);
        });

        group.MapDelete("/{id}", (string id, AlertRuleStore store) =>
            store.Delete(id) ? Results.NoContent() : Results.NotFound());
    }
}
