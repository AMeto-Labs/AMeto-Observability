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
 * At the default root prefix this is the identity: `/api/events` in, `/api/events` out.
 */
export function appPath(appRelative: string): string {
  // The leading slash has to go: with it, resolution ignores the base's directory and we would
  // get the un-prefixed path straight back — the exact bug this function exists to prevent.
  const url = new URL(appRelative.replace(/^\/+/, ''), document.baseURI);
  return url.pathname + url.search + url.hash;
}
