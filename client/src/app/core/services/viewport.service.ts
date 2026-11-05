import { Injectable, Signal, signal } from '@angular/core';

/**
 * The viewport as signals, for the few places where a layout is decided in TypeScript rather
 * than in a stylesheet: a virtual list's row estimate, a sheet in place of a dropdown, an action
 * sheet in place of a cursor-anchored menu. The queries spell out the same numbers as
 * `_breakpoints.scss` ($bp-sm 640, $bp-md 900) — a media query cannot read a CSS variable, and
 * a stylesheet cannot be read from here, so the two files name the breakpoints independently
 * and must move together.
 *
 * A host without `matchMedia` (jsdom) is a desktop with a mouse: every signal stays false.
 */
@Injectable({ providedIn: 'root' })
export class ViewportService {
  /** Narrower than $bp-sm: the bottom bar, two-line log rows, sheets. */
  readonly phone: Signal<boolean> = mediaSignal('(max-width: 639px)');
  /** From $bp-sm up to $bp-md: the rail, the table without its date column, slide-overs. */
  readonly tablet: Signal<boolean> = mediaSignal('(min-width: 640px) and (max-width: 899px)');
  /** A touch screen with no fine pointer: targets of a finger's size, no hover-only controls. */
  readonly coarse: Signal<boolean> = mediaSignal('(pointer: coarse)');
}

function mediaSignal(query: string): Signal<boolean> {
  if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
    return signal(false).asReadonly();
  }
  const mq = window.matchMedia(query);
  const matches = signal(mq.matches);
  mq.addEventListener('change', e => matches.set(e.matches));
  return matches.asReadonly();
}
