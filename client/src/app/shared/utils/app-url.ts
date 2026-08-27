/**
 * Resolves an app-relative path against the deployment prefix.
 *
 * The server may be hosted under a prefix (`BasePath: /ameto`), which it applies at runtime by
 * rewriting `<base href>` as it serves index.html. Angular's router picks that up on its own,
 * but nothing else does: a literal `/api/events` is *root*-absolute, so the browser sends it to
 * the host root and skips the prefix entirely. Everything that builds a URL by hand has to go
 * through here.
 *
 * Returns a root-absolute path (`/ameto/api/events`), not a relative one, because the callers
 * need it to mean the same thing from every route. `EventSource` and `window.open` resolve a
 * relative URL against the *current document*, not against `<base>` — opening `traces?x` from
 * `/ameto/events/123` would land on `/ameto/events/traces?x`. A root-absolute path has no such
 * ambiguity.
 *
 * A URL that already names its own destination is returned untouched, and that decision is left
 * to the URL parser rather than to a pattern. `//host/x` is the obvious case, but so is
 * `/\host/x`: every browser engine reads a leading slash-backslash as an authority too, and no
 * amount of squinting at the string makes that obvious. Silently turning either into a path on
 * our own origin would be a worse answer than passing it through.
 *
 * At the default root prefix this is the identity: `/api/events` in, `/api/events` out.
 */
export function appPath(appRelative: string): string {
  const base = document.baseURI;

  let target: URL;
  let origin: string;
  try {
    target = new URL(appRelative, base);
    origin = new URL(base).origin;
  } catch {
    // Not parseable against our own base. We have no idea what it means, so we do not get to
    // rewrite it — handing it back unchanged at least fails where the caller can see it.
    return appRelative;
  }

  // A scheme names the destination outright (`https:`, `blob:`, `data:`), and so does an
  // authority — which is what a differing origin detects, however it was spelled.
  if (/^[a-z][a-z0-9+.-]*:/i.test(appRelative) || target.origin !== origin) return appRelative;

  // App-relative. The leading slash has to go: with it, resolution ignores the base's directory
  // and we would get the un-prefixed path straight back — the exact bug this function prevents.
  const resolved = new URL(appRelative.replace(/^\/+/, ''), base);
  return resolved.pathname + resolved.search + resolved.hash;
}
