using System.Security.Claims;

namespace Ameto.Server.Auth;

/// <summary>
/// Per-user read scopes on the query API. Bit flags, so one user can be granted
/// any combination. Users created before per-view scoping — and every
/// <c>admin</c> — resolve to <see cref="All"/>.
/// </summary>
[Flags]
public enum ViewPermissions
{
    None    = 0,
    Logs    = 1,
    Metrics = 2,
    Traces  = 4,
    Stats   = 8,
    All     = Logs | Metrics | Traces | Stats, // 15
}

/// <summary>Claim-based checks for the view scopes carried in the JWT.</summary>
internal static class ClaimsPrincipalExtensions
{
    /// <summary>Name of the JWT claim holding the effective <see cref="ViewPermissions"/> bitmask.</summary>
    public const string PermClaim = "perm";

    /// <summary>
    /// True when the principal is allowed the requested scope: <c>admin</c> is always
    /// allowed; otherwise the <c>perm</c> claim must contain every requested bit.
    /// Allocation-free — a claim lookup plus an <see cref="int.TryParse(ReadOnlySpan{char}, out int)"/>.
    /// </summary>
    public static bool HasView(this ClaimsPrincipal user, ViewPermissions required)
    {
        if (user.IsInRole("admin")) return true;

        var raw = user.FindFirstValue(PermClaim);
        if (raw is null || !int.TryParse(raw, out var granted)) return false;

        int req = (int)required;
        return (granted & req) == req;
    }
}
