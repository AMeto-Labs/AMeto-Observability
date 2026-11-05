import { describe, it, expect, vi } from 'vitest';

import { LONG_PRESS_MS, LongPress } from './long-press';

/** A clock driven by hand: one pending callback at a time, run by `fire`. */
function clock() {
  let pending: { cb: () => void; ms: number } | undefined;
  let next = 0;
  return {
    schedule: (cb: () => void, ms: number) => { pending = { cb, ms }; return ++next; },
    unschedule: () => { pending = undefined; },
    pending: () => pending,
    fire: () => { const p = pending; pending = undefined; p?.cb(); },
  };
}

describe('LongPress', () => {
  it('fires after the platform delay when the finger rests, and marks the click that follows', () => {
    const c = clock();
    const onFire = vi.fn();
    const press = new LongPress(onFire, c.schedule, c.unschedule);

    press.start(100, 200);
    expect(c.pending()?.ms).toBe(LONG_PRESS_MS);
    press.move(104, 203);                 // within the slop: still a press
    c.fire();
    expect(onFire).toHaveBeenCalledTimes(1);

    press.end();
    expect(press.consumeFired()).toBe(true);   // the release's click is not a tap…
    expect(press.consumeFired()).toBe(false);  // …and only that one click
  });

  it('is cancelled by a finger that travels, and by a release before the delay', () => {
    const c = clock();
    const onFire = vi.fn();
    const press = new LongPress(onFire, c.schedule, c.unschedule);

    press.start(100, 200);
    press.move(100, 212);                 // 12px: a scroll
    expect(c.pending()).toBeUndefined();

    press.start(100, 200);
    press.end();                          // a tap
    expect(c.pending()).toBeUndefined();
    expect(onFire).not.toHaveBeenCalled();
    expect(press.consumeFired()).toBe(false);
  });

  it('a new press replaces a pending one', () => {
    const c = clock();
    const onFire = vi.fn();
    const press = new LongPress(onFire, c.schedule, c.unschedule);

    press.start(0, 0);
    press.start(50, 50);
    press.move(53, 53);                   // measured from the second press
    expect(c.pending()).toBeDefined();
    c.fire();
    expect(onFire).toHaveBeenCalledTimes(1);
  });
});
