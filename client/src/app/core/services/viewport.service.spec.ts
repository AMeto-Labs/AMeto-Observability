import { describe, it, expect, afterEach, vi } from 'vitest';

import { ViewportService } from './viewport.service';

/**
 * The service names the same breakpoints as `_breakpoints.scss` and follows the window as it
 * is resized; a host with no `matchMedia` (jsdom as it ships) is a desktop with a mouse.
 */
describe('ViewportService', () => {
  const original = window.matchMedia;

  afterEach(() => {
    window.matchMedia = original;
  });

  it('is a desktop with a mouse where matchMedia does not exist', () => {
    // jsdom has no matchMedia; the delete makes that explicit should a future version add one.
    (window as { matchMedia?: unknown }).matchMedia = undefined;
    const vp = new ViewportService();
    expect(vp.phone()).toBe(false);
    expect(vp.tablet()).toBe(false);
    expect(vp.coarse()).toBe(false);
  });

  it('asks for the stylesheet breakpoints and follows a change', () => {
    const listeners = new Map<string, (e: { matches: boolean }) => void>();
    const queries: string[] = [];
    window.matchMedia = vi.fn((query: string) => {
      queries.push(query);
      return {
        matches: query === '(max-width: 639px)' || query === '(pointer: coarse)',
        addEventListener: (_: string, cb: (e: { matches: boolean }) => void) => listeners.set(query, cb),
      } as unknown as MediaQueryList;
    });

    const vp = new ViewportService();
    expect(queries).toEqual([
      '(max-width: 639px)',
      '(min-width: 640px) and (max-width: 899px)',
      '(pointer: coarse)',
    ]);
    expect(vp.phone()).toBe(true);
    expect(vp.tablet()).toBe(false);
    expect(vp.coarse()).toBe(true);

    // The window grows past 640: the phone layout goes, the tablet one comes.
    listeners.get('(max-width: 639px)')!({ matches: false });
    listeners.get('(min-width: 640px) and (max-width: 899px)')!({ matches: true });
    expect(vp.phone()).toBe(false);
    expect(vp.tablet()).toBe(true);
  });
});
