using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Authentication.JwtBearer;

namespace Rd.Log.Server.Auth;

internal static class AuthServiceExtensions
{
    /// <summary>
    /// Registers JWT Bearer authentication, authorization, <see cref="AuthDatabase"/>,
    /// <see cref="AuthStore"/>, <see cref="JwtIssuer"/>, and <see cref="ApiKeyCache"/> into DI.
    /// The JWT signing secret is auto-generated once and stored in
    /// <c>{dataDirectory}/jwt-secret.bin</c>.
    /// </summary>
    public static IServiceCollection AddRdLogAuth(
        this IServiceCollection services,
        string                  dataDirectory)
    {
        Directory.CreateDirectory(dataDirectory);

        // ── JWT secret ─────────────────────────────────────────────────────────
        var secretPath = Path.Combine(dataDirectory, "jwt-secret.bin");
        string secret;
        if (File.Exists(secretPath))
        {
            secret = File.ReadAllText(secretPath, Encoding.UTF8);
        }
        else
        {
            secret = Convert.ToBase64String(RandomNumberGenerator.GetBytes(64));
            File.WriteAllText(secretPath, secret, Encoding.UTF8);
        }

        var issuer = new JwtIssuer(secret);
        services.AddSingleton(new AuthDatabase(dataDirectory));
        services.AddSingleton<AuthStore>();
        services.AddSingleton(issuer);
        services.AddSingleton<ApiKeyCache>();

        // ── Standard JWT Bearer ────────────────────────────────────────────────
        services
            .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
            .AddJwtBearer(o =>
            {
                o.TokenValidationParameters = issuer.ValidationParameters;
                // Support ?access_token= query param for SSE (EventSource API
                // cannot set Authorization headers in browsers).
                o.Events = new JwtBearerEvents
                {
                    OnMessageReceived = static ctx =>
                    {
                        if (ctx.Request.Query.TryGetValue("access_token", out var token)
                            && token.Count > 0)
                        {
                            ctx.Token = token[0];
                        }
                        return Task.CompletedTask;
                    },
                };
            });

        services.AddAuthorization();

        return services;
    }
}
