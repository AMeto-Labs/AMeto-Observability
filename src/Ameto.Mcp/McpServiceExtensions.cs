using System.Reflection;
using System.Security.Claims;
using System.Text.Encodings.Web;
using Ameto.Core;
using Ameto.Mcp.Tools;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ModelContextProtocol.AspNetCore;
using ModelContextProtocol.Protocol;

namespace Ameto.Mcp;

/// <summary>The names the AI endpoint is known by.</summary>
public static class McpAuth
{
    /// <summary>Where the endpoint is mapped, relative to <c>Ameto:BasePath</c>.</summary>
    public const string Path = "/mcp";

    /// <summary>The authentication scheme that reads a profile key, and the policy that requires one.</summary>
    public const string Scheme = "McpProfile";
    public const string Policy = "McpProfile";

    /// <summary>
    /// The role of a profile key's identity. It is no user role — not admin, manager or viewer —
    /// so a profile key satisfies no policy of the REST API: there is no unmasked road for it.
    /// </summary>
    public const string Role = "mcp-profile";
}

/// <summary>
/// Authenticates <c>Authorization: Bearer &lt;profile key&gt;</c>. Named only by the endpoint's own
/// policy, so it never runs for another route; the default scheme (JWT) meets the same header
/// first, finds no JWT in it and says nothing, which is the same arrangement the read-path
/// API-key scheme lives under.
/// </summary>
internal sealed class McpProfileAuthenticationHandler(
    IOptionsMonitor<AuthenticationSchemeOptions> options, ILoggerFactory logger, UrlEncoder encoder, McpStore store)
    : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    private const string Bearer = "Bearer ";

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        string header = Request.Headers.Authorization.ToString();
        if (!header.StartsWith(Bearer, StringComparison.OrdinalIgnoreCase))
            return Task.FromResult(AuthenticateResult.NoResult());

        if (store.Authenticate(header.AsSpan(Bearer.Length).Trim()) is not { } profile)
            return Task.FromResult(AuthenticateResult.Fail("The key does not belong to an AI profile."));

        var identity = new ClaimsIdentity(McpAuth.Scheme);
        identity.AddClaim(new Claim(ClaimTypes.NameIdentifier, profile.Id));
        identity.AddClaim(new Claim(ClaimTypes.Name, profile.Name));
        identity.AddClaim(new Claim(ClaimTypes.Role, McpAuth.Role));
        return Task.FromResult(AuthenticateResult.Success(
            new AuthenticationTicket(new ClaimsPrincipal(identity), McpAuth.Scheme)));
    }

    protected override Task HandleChallengeAsync(AuthenticationProperties properties)
    {
        Response.StatusCode              = StatusCodes.Status401Unauthorized;
        Response.Headers.WWWAuthenticate = "Bearer realm=\"ameto-mcp\"";
        Response.ContentType             = "application/json; charset=utf-8";
        return Response.WriteAsync("""{"error":"A profile key is required: Authorization: Bearer <key>. Create a profile under Settings, AI / MCP."}""");
    }
}

public static class McpServiceExtensions
{
    /// <summary>
    /// Registers the AI endpoint's services. Call after the storage and query services, which
    /// the tools read through; the host supplies an <see cref="ISearchGate"/>.
    /// </summary>
    public static IServiceCollection AddAmetoMcp(this IServiceCollection services, string dataDirectory)
    {
        services.AddSingleton(sp => new McpStore(
            dataDirectory,
            sp.GetRequiredService<ISecretProtector>(),
            sp.GetRequiredService<ILogger<McpStore>>()));
        services.AddSingleton<LogTools>();
        services.AddSingleton<McpAudit>();
        services.AddSingleton<McpCallLimiter>();
        services.AddSingleton<McpSessions>();
        services.AddSingleton<McpToolHost>();

        // The SDK logs every request it handles at Information. One line per AI call is what the
        // audit events are for; the server log keeps the SDK's warnings and errors.
        services.AddLogging(static logging => logging.AddFilter("ModelContextProtocol", Microsoft.Extensions.Logging.LogLevel.Warning));

        services.AddAuthentication()
                .AddScheme<AuthenticationSchemeOptions, McpProfileAuthenticationHandler>(McpAuth.Scheme, static _ => { });
        services.AddAuthorizationBuilder()
                .AddPolicy(McpAuth.Policy, static policy => policy
                    .AddAuthenticationSchemes(McpAuth.Scheme)
                    .RequireAuthenticatedUser()
                    .RequireRole(McpAuth.Role));

        services.AddMcpServer(static options =>
                {
                    options.ServerInfo = new Implementation
                    {
                        Name    = "ameto",
                        Title   = "Ameto logs",
                        Version = ServerVersion(),
                    };
                    options.ServerInstructions = BuiltInTools.Instructions;
                    // The list is built per request and does change at run time; say so, so a
                    // client that holds a session listens for the notification.
                    options.Capabilities = new ServerCapabilities { Tools = new ToolsCapability { ListChanged = true } };
                })
                .WithHttpTransport(static transport =>
                {
                    // Clients on the 2026-07-28 revision are served per request, with no session.
                    // Clients that still open with `initialize` get the session they expect, so
                    // one address serves both; a session is a few kilobytes, and they are kept
                    // few and short because this is a small-host server.
                    transport.SessionMode         = HttpServerSessionMode.StatefulForInitializeClients;
                    transport.IdleTimeout         = TimeSpan.FromMinutes(30);
                    transport.MaxIdleSessionCount = 200;
                })
                .WithListToolsHandler(static (request, ct) =>
                    Host(request).ListAsync(request, ct))
                .WithCallToolHandler(static (request, ct) =>
                    Host(request).CallAsync(request, ct));

        return services;
    }

    private static McpToolHost Host(ModelContextProtocol.Server.MessageContext request) =>
        (request.Services ?? request.Server.Services)!.GetRequiredService<McpToolHost>();

    /// <summary>The build's version without the commit suffix a build may append to it.</summary>
    private static string ServerVersion()
    {
        string version = typeof(McpServiceExtensions).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "1.0.0";
        int plus = version.IndexOf('+');
        return plus < 0 ? version : version[..plus];
    }
}
