using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using Microsoft.IdentityModel.Tokens;

namespace Ameto.Server.Auth;

internal sealed class JwtIssuer
{
    private readonly SymmetricSecurityKey _key;

    /// <summary>
    /// Lifetime of an access token. Short because the sign-in is not: the page trades its session
    /// cookie for a fresh token (see <see cref="AuthSessions"/>), so this bounds only how long a
    /// token lifted out of the page stays usable. A revoked session's tokens are refused at once
    /// regardless (see <see cref="AuthRevocations"/>).
    /// </summary>
    public const int ExpiresInSeconds = 15 * 60;

    /// <summary>
    /// The claim naming the session a token was issued under. The long URI rather than "sid",
    /// like Name and Role here: a short name is rewritten on the way in by the bearer handler's
    /// claim map, and this way the type read back is the type written.
    /// </summary>
    public const string SessionClaim = ClaimTypes.Sid;

    public JwtIssuer(string secret)
    {
        _key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(secret));
    }

    /// <summary>Issues an access token for <paramref name="user"/> under session <paramref name="sessionId"/>.</summary>
    public string Issue(UserRecord user, string sessionId)
    {
        // Admins always carry the full scope regardless of the stored bitmask.
        var effective = user.Role == "admin" ? ViewPermissions.All : user.Permissions;

        var claims = new List<Claim>
        {
            new(ClaimTypes.Name, user.Username),
            new(ClaimTypes.Role, user.Role),
            new(ClaimsPrincipalExtensions.PermClaim, ((int)effective).ToString()),
            new(SessionClaim, sessionId),
        };
        if (!string.IsNullOrEmpty(user.Email))
            claims.Add(new Claim(ClaimTypes.Email, user.Email));
        if (!string.IsNullOrEmpty(user.DisplayName))
            claims.Add(new Claim("display_name", user.DisplayName));

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
        ClockSkew                = AllowedClockSkew,
        IssuerSigningKey         = _key,
    };

    /// <summary>How far past its expiry a token is still accepted.</summary>
    public static readonly TimeSpan AllowedClockSkew = TimeSpan.FromMinutes(1);
}
