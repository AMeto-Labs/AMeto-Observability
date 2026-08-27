namespace Ameto.Core;

/// <summary>
/// The deployment prefix, in the two shapes that need it — and they are not the same shape,
/// which is the whole reason this type exists.
///
/// <para><see cref="PathBase"/> is what <c>UsePathBase</c> takes: a leading slash and no
/// trailing one (<c>"/ameto"</c>). <see cref="BaseHref"/> is what goes into the SPA's
/// <c>&lt;base href&gt;</c>, and HTML resolves that against the *last slash*, so it needs the
/// trailing one (<c>"/ameto/"</c>) or every relative asset would be fetched from the parent
/// directory instead. Root is <c>""</c> and <c>"/"</c> respectively — not the same string,
/// which is exactly the sort of off-by-one-slash that only shows up in a browser.</para>
///
/// <para>Operators write the value by hand, so the parser is generous about the forms a hand
/// might produce (<c>ameto</c>, <c>/ameto</c>, <c>/ameto/</c>, <c>//ameto//</c>) and refuses
/// only what cannot be a path prefix at all — a whole URL, a query, a fragment. A typo that
/// silently produced the wrong prefix would surface as a blank page with a console full of
/// 404s, so it is worth failing loudly at startup instead.</para>
/// </summary>
public readonly struct UrlBasePath : IEquatable<UrlBasePath>
{
    /// <summary>Serving from the root of the host: no prefix at all. The default.</summary>
    public static UrlBasePath Root => default;

    private readonly string? _pathBase;

    private UrlBasePath(string pathBase) => _pathBase = pathBase;

    /// <summary>
    /// For <c>UsePathBase</c>: <c>""</c> at the root, otherwise <c>"/ameto"</c> — leading
    /// slash, no trailing slash.
    /// </summary>
    public string PathBase => _pathBase ?? "";

    /// <summary>
    /// For <c>&lt;base href&gt;</c>: <c>"/"</c> at the root, otherwise <c>"/ameto/"</c> —
    /// leading <b>and</b> trailing slash.
    /// </summary>
    public string BaseHref => _pathBase is null or "" ? "/" : _pathBase + "/";

    /// <summary>True when nothing is configured and every path is served where it always was.</summary>
    public bool IsRoot => string.IsNullOrEmpty(_pathBase);

    /// <summary>
    /// Parses a configured value. Accepts the empty string, <c>"/"</c>, and any of
    /// <c>ameto</c> / <c>/ameto</c> / <c>ameto/</c> / <c>/ameto/</c>, including nested
    /// prefixes (<c>/tools/ameto</c>). Repeated slashes are collapsed.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// The value cannot be a path prefix: it carries a scheme, a query, a fragment, a
    /// backslash, whitespace, or a <c>.</c>/<c>..</c> segment.
    /// </exception>
    public static UrlBasePath Parse(string? configured)
    {
        if (string.IsNullOrWhiteSpace(configured)) return Root;

        var value = configured.Trim();

        // A whole URL is the mistake an operator is most likely to make here, and the one
        // that would otherwise be normalised into the nonsense prefix "/https:/host/ameto".
        if (value.Contains("://", StringComparison.Ordinal))
            throw new ArgumentException(
                $"BasePath must be a path prefix such as \"/ameto\", not a full URL (\"{configured}\").",
                nameof(configured));

        foreach (var bad in "?#\\")
            if (value.Contains(bad))
                throw new ArgumentException(
                    $"BasePath must not contain '{bad}' (\"{configured}\").", nameof(configured));

        var builder = new System.Text.StringBuilder(value.Length + 1);
        foreach (var segment in value.Split('/', StringSplitOptions.RemoveEmptyEntries))
        {
            // Left in, these would let a prefix walk out of itself — and UsePathBase compares
            // the literal string, so "/a/../b" would simply never match anything.
            if (segment is "." or "..")
                throw new ArgumentException(
                    $"BasePath must not contain a '{segment}' segment (\"{configured}\").", nameof(configured));

            foreach (var c in segment)
                if (char.IsWhiteSpace(c) || char.IsControl(c))
                    throw new ArgumentException(
                        $"BasePath must not contain whitespace or control characters (\"{configured}\").",
                        nameof(configured));

            builder.Append('/').Append(segment);
        }

        // Everything was slashes: "/", "//", "///" all mean the root.
        return builder.Length == 0 ? Root : new UrlBasePath(builder.ToString());
    }

    public override string ToString() => IsRoot ? "/" : PathBase;

    public          bool Equals(UrlBasePath other) => string.Equals(PathBase, other.PathBase, StringComparison.Ordinal);
    public override bool Equals(object? obj)    => obj is UrlBasePath other && Equals(other);
    public override int  GetHashCode()          => PathBase.GetHashCode(StringComparison.Ordinal);

    public static bool operator ==(UrlBasePath left, UrlBasePath right) =>  left.Equals(right);
    public static bool operator !=(UrlBasePath left, UrlBasePath right) => !left.Equals(right);
}
