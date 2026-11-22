using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace Ameto.Mcp;

/// <summary>Maps the AI endpoint, and the switch in front of it.</summary>
public static class McpEndpointMapper
{
    /// <summary>Maps <c>/mcp</c>, open to profile keys only.</summary>
    public static IEndpointConventionBuilder MapAmetoMcp(this IEndpointRouteBuilder endpoints) =>
        endpoints.MapMcp(McpAuth.Path).RequireAuthorization(McpAuth.Policy);

    /// <summary>
    /// Answers 503 on the endpoint while the section is switched off in Settings. Synchronous
    /// for every other request — it adds no frame above the telemetry receivers.
    /// </summary>
    public static IApplicationBuilder UseAmetoMcpSwitch(this IApplicationBuilder app)
    {
        var store = app.ApplicationServices.GetRequiredService<McpStore>();
        return app.Use((context, next) =>
            context.Request.Path.StartsWithSegments(McpAuth.Path) && !store.Current.Settings.Enabled
                ? SwitchedOff(context)
                : next(context));
    }

    private static Task SwitchedOff(HttpContext context)
    {
        context.Response.StatusCode  = StatusCodes.Status503ServiceUnavailable;
        context.Response.ContentType = "application/json; charset=utf-8";
        return context.Response.WriteAsync(
            """{"error":"The AI endpoint is switched off. An admin can switch it on under Settings, AI / MCP."}""");
    }
}
