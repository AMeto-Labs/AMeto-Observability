using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using Microsoft.IdentityModel.Tokens;

namespace Ameto.Server.Auth;

internal sealed class JwtIssuer
{
    private readonly SymmetricSecurityKey _key;
    public const int ExpiresInSeconds = 2 * 3600; // 2 hours

    public JwtIssuer(string secret)
    {
        _key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(secret));
    }

    public string Issue(string username, string role, string email = "", string displayName = "",
        ViewPermissions permissions = ViewPermissions.All)
    {
        // Admins always carry the full scope regardless of the stored bitmask.
        var effective = role == "admin" ? ViewPermissions.All : permissions;

        var claims = new List<Claim>
        {
            new(ClaimTypes.Name, username),
            new(ClaimTypes.Role, role),
            new(ClaimsPrincipalExtensions.PermClaim, ((int)effective).ToString()),
        };
        if (!string.IsNullOrEmpty(email))
            claims.Add(new Claim(ClaimTypes.Email, email));
        if (!string.IsNullOrEmpty(displayName))
            claims.Add(new Claim("display_name", displayName));

        var token = new JwtSecurityToken(
            claims:    claims,
            expires:   DateTime.UtcNow.AddSeconds(ExpiresInSeconds),
            signingCredentials: new SigningCredentials(_key, SecurityAlgorithms.HmacSha256));
        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    public TokenValidationParameters ValidationParameters => new()
    {
        ValidateIssuer           = false,
        ValidateAudience         = false,
        ValidateLifetime         = true,
        ClockSkew                = TimeSpan.FromMinutes(1),
        IssuerSigningKey         = _key,
    };
}
