import { HttpErrorResponse } from '@angular/common/http';
import { UNAVAILABLE_FALLBACK, unavailableMessage } from './unavailable';

describe('unavailableMessage', () => {
  const sentence = 'The metric store has shut down and cannot answer. Metrics are available again once the server has restarted.';

  it("returns the server's sentence from a parsed 503 body", () => {
    const err = new HttpErrorResponse({ status: 503, error: { error: sentence } });
    expect(unavailableMessage(err)).toBe(sentence);
  });

  it('returns the sentence from a 503 body that arrived as text', () => {
    const err = new HttpErrorResponse({ status: 503, error: JSON.stringify({ error: sentence }) });
    expect(unavailableMessage(err)).toBe(sentence);
  });

  it('falls back to a generic sentence for a 503 without one', () => {
    expect(unavailableMessage(new HttpErrorResponse({ status: 503, error: null }))).toBe(UNAVAILABLE_FALLBACK);
    expect(unavailableMessage(new HttpErrorResponse({ status: 503, error: 'Service Unavailable' }))).toBe(UNAVAILABLE_FALLBACK);
  });

  it('is null for every other outcome, so those keep their existing handling', () => {
    expect(unavailableMessage(new HttpErrorResponse({ status: 500, error: { error: 'boom' } }))).toBeNull();
    expect(unavailableMessage(new HttpErrorResponse({ status: 0 }))).toBeNull();
    expect(unavailableMessage(new Error('not http'))).toBeNull();
  });
});
