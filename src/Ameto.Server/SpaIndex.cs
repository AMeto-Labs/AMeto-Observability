using System.Security.Cryptography;
using System.Text;

using Ameto.Core;

namespace Ameto.Server;

/// <summary>
/// Serves the SPA entry document with its <c>&lt;base href&gt;</c> set to the configured
/// deployment prefix.
///
/// <para>This is the half of <see cref="ServerOptions.BasePath"/> that <c>UsePathBase</c>
/// cannot do. <c>UsePathBase</c> teaches the <i>server</i> to answer under a prefix; nothing
/// teaches the <i>browser</i> that it is under one. The Angular build writes a literal
/// <c>&lt;base href="/"&gt;</c> into index.html, and everything downstream of that — the
/// router's idea of the current route, every hashed chunk, every relative asset — is resolved
/// against it. Served unchanged under <c>/ameto/</c>, the browser fetches
/// <c>/main-A1B2C3.js</c> from the host root and the app never boots.</para>
///
/// <para>The prefix used to be an <c>ng build --base-href</c> flag, which is why this exists:
/// baking it in makes the artifact deployment-specific, so the container image and the Windows
/// installer of the same version disagreed about where they were hosted. Rewriting the one tag
/// as the document is served costs a single cached string and makes one build work anywhere.</para>
///
/// <para>The document is read on first use and cached from then on, deliberately: it is the
/// only file whose bytes depend on configuration, configuration is bound once at startup, and
/// re-reading it per request would buy nothing while putting a file read on every page load. A
/// miss is not cached — see <see cref="Load"/> for why that asymmetry matters.</para>
/// </summary>
internal sealed class SpaIndex(IWebHostEnvironment environment, UrlBasePath basePath, ILogger<SpaIndex> logger)
{
    private readonly Lock _gate = new();

    /// <summary>The rewritten document and its validator, published together or not at all.</summary>
    private sealed record Cached(byte[] Bytes, string ETag);

    private Cached? _cached;
    private bool    _warnedMissing;

    /// <summary>
    /// Writes the entry document, or returns false when there is nothing to serve — a source
    /// checkout with no <c>npm run build</c> behind it has no wwwroot at all, and that must
    /// stay a 404 rather than a startup failure.
    /// </summary>
    public async ValueTask<bool> TryWriteAsync(HttpContext ctx)
    {
        var cached = Load();
        if (cached is null) return false;
        var (bytes, etag) = (cached.Bytes, cached.ETag);

        var response = ctx.Response;
        response.Headers.ETag = etag;
        // The entry document names hashed chunk files that disappear on the next deploy, so it
        // must never be served from cache without asking. The ETag keeps that revalidation to
        // a 304 in the common case.
        response.Headers.CacheControl = "no-cache";

        if (ctx.Request.Headers.IfNoneMatch.Count > 0)
        {
            foreach (var candidate in ctx.Request.Headers.IfNoneMatch)
            {
                if (candidate is null) continue;
                if (candidate == "*" || candidate.Contains(etag, StringComparison.Ordinal))
                {
                    response.StatusCode = StatusCodes.Status304NotModified;
                    return true;
                }
            }
        }

        response.StatusCode  = StatusCodes.Status200OK;
        response.ContentType = "text/html; charset=utf-8";
        response.ContentLength = bytes.Length;

        // HEAD gets the headers and nothing else; writing a body would be a protocol error.
        if (!HttpMethods.IsHead(ctx.Request.Method))
            await response.Body.WriteAsync(bytes, ctx.RequestAborted).ConfigureAwait(false);

        return true;
    }

    /// <summary>
    /// The document, read and rewritten on first use and cached from then on. Null while there
    /// is nothing to read.
    ///
    /// <para>A MISS is deliberately not cached, and that asymmetry is the point. wwwroot is
    /// populated by a separate build step, and the request that finds it empty is often earlier
    /// than the step that fills it — a source checkout whose `npm run build` is still running, a
    /// slow volume mount, an installer that starts the service before it finishes copying. Latch
    /// the miss and a single early probe turns the UI off until someone restarts the process.
    /// Retrying costs one File.Exists per request, and only while there is no UI to serve.</para>
    /// </summary>
    private Cached? Load()
    {
        var cached = Volatile.Read(ref _cached);
        if (cached is not null) return cached;

        lock (_gate)
        {
            if (_cached is not null) return _cached;

            var root = environment.WebRootPath;
            var path = string.IsNullOrEmpty(root) ? null : Path.Combine(root, "index.html");

            if (path is null || !File.Exists(path))
            {
                // Once, not on every retry — a missing UI should be one line in the log, not a
                // line per request for the life of the process.
                if (!basePath.IsRoot && !_warnedMissing)
                {
                    _warnedMissing = true;
                    logger.LogWarning("BasePath is set to {BasePath} but no index.html was found under {WebRoot} — " +
                                      "the API answers under the prefix, but there is no UI to serve.",
                                      basePath.PathBase, root);
                }
                return null;
            }

            var html  = Rewrite(File.ReadAllText(path), basePath.BaseHref, logger);
            var bytes = Encoding.UTF8.GetBytes(html);
            var fresh = new Cached(bytes, $"\"{Convert.ToHexStringLower(SHA256.HashData(bytes).AsSpan(0, 16))}\"");

            // One publication of one fully-built object: a reader can never see the bytes
            // without the validator that matches them.
            Volatile.Write(ref _cached, fresh);
            return fresh;
        }
    }

    /// <summary>
    /// Points the document's one <c>&lt;base&gt;</c> tag at <paramref name="baseHref"/>,
    /// adding the tag if the document has none.
    /// </summary>
    /// <remarks>
    /// Deliberately not a regex and deliberately not an HTML parser. The input is one file
    /// emitted by the Angular CLI, whose shape we control; the job is to find one attribute in
    /// one tag and leave every other byte untouched.
    /// </remarks>
    internal static string Rewrite(string html, string baseHref, ILogger? logger = null)
    {
        int tagStart = FindTag(html, "base");
        if (tagStart >= 0)
        {
            int tagEnd = html.IndexOf('>', tagStart);
            if (tagEnd > tagStart)
            {
                var (valueStart, valueLength) = FindAttributeValue(html, tagStart, tagEnd, "href");
                if (valueStart >= 0)
                    return string.Concat(html.AsSpan(0, valueStart), baseHref,
                                         html.AsSpan(valueStart + valueLength));

                // A <base> with no href at all: give it one.
                return string.Concat(html.AsSpan(0, tagStart + "<base".Length), $" href=\"{baseHref}\"",
                                     html.AsSpan(tagStart + "<base".Length));
            }
        }

        // No <base> tag. Angular's router reads it with document.head.querySelector('base'),
        // so it has to go inside <head> or it may as well not exist.
        int headStart = FindTag(html, "head");
        if (headStart >= 0)
        {
            int headEnd = html.IndexOf('>', headStart);
            if (headEnd > headStart)
                return string.Concat(html.AsSpan(0, headEnd + 1), $"<base href=\"{baseHref}\">",
                                     html.AsSpan(headEnd + 1));
        }

        // Nowhere sensible to put it. At the root that is harmless — the browser's default base
        // is already the host root — so only a configured prefix is worth complaining about.
        if (baseHref != "/")
            logger?.LogWarning("index.html has neither a <base> tag nor a <head> to add one to, so the " +
                               "configured base href {BaseHref} could not be applied; the UI will not load " +
                               "under the prefix.", baseHref);
        return html;
    }

    /// <summary>Index of <c>&lt;name</c>, where the name is a whole tag name and not a prefix of one.</summary>
    private static int FindTag(string html, string name)
    {
        int from = 0;
        while (true)
        {
            int at = html.IndexOf($"<{name}", from, StringComparison.OrdinalIgnoreCase);
            if (at < 0) return -1;

            int after = at + 1 + name.Length;
            // "<basefoo" is not "<base"; the name has to end here.
            if (after >= html.Length || html[after] is '>' or '/' || char.IsWhiteSpace(html[after]))
                return at;

            from = at + 1;
        }
    }

    /// <summary>
    /// Locates a quoted attribute value inside one tag. Returns the offset and length of the
    /// value itself, excluding the quotes, or (-1, 0) when the attribute is absent.
    /// </summary>
    private static (int Start, int Length) FindAttributeValue(string html, int tagStart, int tagEnd, string attribute)
    {
        int at = tagStart;
        while (true)
        {
            at = html.IndexOf(attribute, at, tagEnd - at, StringComparison.OrdinalIgnoreCase);
            if (at < 0) return (-1, 0);

            int nameEnd = at + attribute.Length;
            // Must be a whole attribute name: preceded by whitespace, followed by '=' (with
            // whitespace allowed either side). Otherwise "data-href" would match "href".
            bool boundedLeft = at > tagStart && char.IsWhiteSpace(html[at - 1]);
            if (boundedLeft)
            {
                int i = nameEnd;
                while (i < tagEnd && char.IsWhiteSpace(html[i])) i++;
                if (i < tagEnd && html[i] == '=')
                {
                    i++;
                    while (i < tagEnd && char.IsWhiteSpace(html[i])) i++;
                    if (i < tagEnd && (html[i] is '"' or '\''))
                    {
                        char quote = html[i];
                        int  start = i + 1;
                        int  end   = html.IndexOf(quote, start);
                        if (end > 0 && end <= tagEnd) return (start, end - start);
                    }
                }
            }

            at = nameEnd;
            if (at >= tagEnd) return (-1, 0);
        }
    }
}
