import { HttpErrorResponse } from '@angular/common/http';

/** What the page shows when a 503 carries no sentence of its own. */
export const UNAVAILABLE_FALLBACK = 'The server cannot answer this right now (503). Try again shortly.';

/**
 * The server's own sentence when a request was refused with 503 — a store that has shut down or is
 * still loading (#95), or a search limit that is full — and null for any other outcome.
 *
 * <p>Exists because every caller of these endpoints used to turn an error into an EMPTY answer
 * (`catchError(() => of([]))`), so "the store cannot answer" reached the user as "no data in this
 * window" — the very confusion the 503 was introduced to end. A page that shows empty for other
 * errors keeps doing so; a 503 becomes a visible sentence instead.</p>
 */
export function unavailableMessage(err: unknown): string | null {
  if (!(err instanceof HttpErrorResponse) || err.status !== 503) return null;
  return errorSentence(err.error) ?? UNAVAILABLE_FALLBACK;
}

/** `{"error": "…"}` — the body every refusal on this server carries — as an object or as text. */
function errorSentence(body: unknown): string | null {
  if (typeof body === 'string') {
    try { return errorSentence(JSON.parse(body)); } catch { return null; }
  }
  if (body && typeof body === 'object' && typeof (body as { error?: unknown }).error === 'string')
    return (body as { error: string }).error;
  return null;
}
