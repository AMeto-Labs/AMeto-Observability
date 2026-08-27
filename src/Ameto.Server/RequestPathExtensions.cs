namespace Ameto.Server;

/// <summary>
/// The one place a path this server hands back to a client gains the deployment prefix.
///
/// <para>Anything the server <i>receives</i> is already prefix-free by the time it is routed —
/// <c>UsePathBase</c> strips it. Anything the server <i>generates</i> is the opposite problem: a
/// redirect target or a <c>Location</c> header is read by the browser against the origin, so a
/// bare <c>"/login"</c> sends it outside the application. Under a reverse proxy scoped to the
/// prefix that is a 404; after an OAuth sign-in it is a 404 holding a freshly issued token.</para>
///
/// <para>This exists rather than the interpolation it replaces because the bug it prevents comes
/// back one site at a time. A new provider, a new POST that returns a <c>Location</c> — the bare
/// path compiles and reads perfectly well, and nothing points at the omission. The client half of
/// this feature made the same call for the same reason; see <c>appPath()</c> there.</para>
///
/// <para><see cref="HttpRequest.PathBase"/> and not the configured option, deliberately: the
/// prefix is additive, so a request that arrived without it must be answered without it too.</para>
/// </summary>
internal static class RequestPathExtensions
{
    /// <summary>
    /// Prefixes an app-absolute path (<c>"/login?error=x"</c>) with the base path this request
    /// arrived under. At the default root prefix it returns the path unchanged.
    /// </summary>
    public static string AppUrl(this HttpContext ctx, string appAbsolutePath)
    {
        // A missing leading slash would silently glue the path to the prefix — "/ametologin".
        // Cheaper to be forgiving here than to have it reviewed as correct somewhere else.
        var path = appAbsolutePath.StartsWith('/') ? appAbsolutePath : $"/{appAbsolutePath}";

        // ToUriComponent, not Value: this is going into a URL, and the escaping is its job.
        return $"{ctx.Request.PathBase.ToUriComponent()}{path}";
    }
}
